from collections import Counter
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import shutil

from pyspark.sql.readwriter import DataFrameWriter
from pyspark.sql.types import StringType
import pytest

from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import IngestionError, run_bronze

pytestmark = pytest.mark.integration


@pytest.fixture
def config(tmp_path):
    raw = tmp_path / "raw"
    shutil.copytree(Path(__file__).parent / "fixtures/synthetic_bdg2", raw)
    return BronzeConfig(
        raw_root=raw, lakehouse_root=tmp_path / "lake",
        electricity=raw / "data/meters/raw/electricity.csv",
        metadata=raw / "data/metadata/metadata.csv", data_kind="synthetic",
        dataset_reference="synthetic-software-fixtures-v1",
    )


def table(spark, config, name):
    return spark.read.format("delta").load(str(config.namespace / "bronze" / name))


def reports(config):
    return [json.loads(path.read_text()) for path in (config.namespace / "_runs").glob("*.json")]


def test_values_columns_lineage_and_delta_recovery(spark, config):
    before = {p: p.read_bytes() for p in (config.electricity, config.metadata)}
    report = run_bronze(spark, config)
    assert report["status"] == "passed"
    electricity = table(spark, config, "electricity")
    columns = ["timestamp", "SYNTHETIC_lodging_A", "SYNTHETIC_lodging_B"]
    assert electricity.columns[:3] == columns
    assert all(isinstance(f.dataType, StringType) for f in electricity.schema)
    expected = [
        ("2016-01-01 00:00:00", "0", "12.50"),
        ("2016-01-01 01:00:00", None, "NaN"),
        ("2016-01-01 02:00:00", "-1.0", "999999.00"),
        ("2016-01-01 02:00:00", "-1.0", "999999.00"),
    ]
    # Spark row order is unspecified; a multiset comparison also checks duplicate counts.
    assert Counter(map(tuple, electricity.select(*columns).collect())) == Counter(expected)
    metadata = table(spark, config, "metadata")
    expected_metadata = [
        ("SYNTHETIC_lodging_A", "SYNTHETIC_SITE_A", "Lodging/residential", "Residence Hall",
         "00100.00", "Europe/London", "  synthetic, not BDG2  "),
        ("SYNTHETIC_lodging_B", "SYNTHETIC_SITE_B", "Lodging/residential", "Dormitory",
         None, "US/Eastern", 'synthetic "quoted" text'),
    ]
    original_metadata = ["building_id", "site_id", "primaryspaceusage", "sub_primaryspaceusage",
                         "sqm", "timezone", "note"]
    assert metadata.columns[:7] == original_metadata
    assert Counter(map(tuple, metadata.select(*original_metadata).collect())) == Counter(expected_metadata)
    for item in report["tables"]:
        assert item["input_rows"] == item["output_rows"]
        assert item["read_back_verified"]
        assert list((Path(item["table_path"]) / "_delta_log").glob("*.json"))
        row = table(spark, config, item["name"]).first()
        source = config.electricity if item["name"] == "electricity" else config.metadata
        assert row["_bronze_source_sha256"] == hashlib.sha256(before[source]).hexdigest()
        assert row["_bronze_source_uri"] == source.as_uri()
        assert row["_bronze_data_kind"] == "synthetic"
    assert before == {p: p.read_bytes() for p in before}
    assert reports(config)[0]["status"] == "passed"
    assert not (config.lakehouse_root / "real").exists()


def test_reload_full_snapshot_does_not_accumulate(spark, config):
    run_bronze(spark, config)
    second = run_bronze(spark, config)
    assert [item["output_rows"] for item in second["tables"]] == [4, 2]
    assert table(spark, config, "electricity").count() == 4
    # Only a temporary synthetic copy is modified to test full replacement.
    config.electricity.write_text("timestamp,SYNTHETIC_lodging_A\n2016-01-01 00:00:00,0\n")
    third = run_bronze(spark, config)
    assert third["tables"][0]["output_rows"] == 1
    assert table(spark, config, "electricity").columns[:2] == ["timestamp", "SYNTHETIC_lodging_A"]
    assert len(reports(config)) == 3


@pytest.mark.parametrize("bad", ["missing", "zero_bytes", "missing_column", "header_only",
                                 "extra_field", "short_row"])
def test_invalid_input_does_not_write_either_table(spark, config, bad):
    # Invalid metadata must be detected before any valid electricity data is written.
    if bad == "missing":
        config = replace(config, metadata=config.raw_root / "absent.csv")
    else:
        contents = {
            "zero_bytes": "",
            "missing_column": "site_id\nSYNTHETIC_SITE\n",
            "header_only": "building_id,site_id\n",
            "extra_field": "building_id,site_id\nSYNTHETIC_A,SYNTHETIC_SITE,extra\n",
            "short_row": "building_id,site_id\nSYNTHETIC_A\n",
        }
        config.metadata.write_text(contents[bad])
    with pytest.raises(IngestionError):
        run_bronze(spark, config)
    assert not (config.namespace / "bronze/electricity").exists()
    assert reports(config)[0]["status"] == "failed"


def test_non_delta_destination_rejected(spark, config):
    target = config.namespace / "bronze/metadata"
    target.mkdir(parents=True)
    sentinel = target / "keep.txt"
    sentinel.write_text("do not replace")
    with pytest.raises(IngestionError, match="no es una tabla Delta"):
        run_bronze(spark, config)
    assert sentinel.read_text() == "do not replace"
    assert not (config.namespace / "bronze/electricity").exists()


def test_failed_reload_keeps_previous_snapshot(spark, config):
    first = run_bronze(spark, config)
    config.metadata.write_text("building_id\n")
    with pytest.raises(IngestionError, match="sin registros"):
        run_bronze(spark, config)
    assert table(spark, config, "electricity").count() == first["tables"][0]["output_rows"]
    assert table(spark, config, "metadata").count() == first["tables"][1]["output_rows"]


def test_persistence_failure_reports_partial_commit(spark, config, monkeypatch):
    original = DataFrameWriter.save

    def fail_metadata(self, path=None, *args, **kwargs):
        if path and str(path).endswith("/bronze/metadata"):
            raise OSError("synthetic simulated persistence failure")
        return original(self, path, *args, **kwargs)

    monkeypatch.setattr(DataFrameWriter, "save", fail_metadata)
    with pytest.raises(IngestionError, match="simulated persistence failure"):
        run_bronze(spark, config)
    report = reports(config)[0]
    assert report["status"] == "failed"
    assert report["cross_table_atomic"] is False
    assert report["tables"][0]["status"] == "verified"
    assert report["tables"][1]["status"] == "validated"
    assert table(spark, config, "electricity").count() == 4
    assert not (config.namespace / "bronze/metadata").exists()
