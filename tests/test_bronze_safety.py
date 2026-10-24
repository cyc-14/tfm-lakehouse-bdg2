from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
import shutil

import pytest
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.readwriter import DataFrameWriter

from tfm_lakehouse.config import BronzeConfig, ConfigurationError
from tfm_lakehouse.ingestion import bronze
from tfm_lakehouse.ingestion.bronze import IngestionError, run_bronze
from tfm_lakehouse.ingestion.csv_source import SourceError, inspect_source, read_source
from tfm_lakehouse.ingestion.snapshot import SnapshotError, read_bronze_snapshot


@pytest.fixture
def config(tmp_path):
    raw = tmp_path / "raw"
    shutil.copytree(Path(__file__).parent / "fixtures/synthetic_bdg2", raw)
    return BronzeConfig(
        raw_root=raw, lakehouse_root=tmp_path / "lake",
        electricity=raw / "data/meters/raw/electricity.csv",
        metadata=raw / "data/metadata/metadata.csv",
        data_kind="synthetic", dataset_reference="synthetic-safety-regression",
    )


@pytest.mark.unit
@pytest.mark.parametrize("alias", ["namespace", "table", "reports", "bronze"])
def test_internal_output_aliases_are_rejected(config, alias):
    namespace = config.namespace
    namespace.mkdir(parents=True)
    if alias == "namespace":
        namespace.rmdir()
        target = config.lakehouse_root / "real"
        target.mkdir()
        namespace.symlink_to(target, target_is_directory=True)
    elif alias == "table":
        parent = namespace / "bronze"
        parent.mkdir()
        target = parent / "electricity"
        target.mkdir()
        (parent / "metadata").symlink_to(target, target_is_directory=True)
    else:
        target = namespace / "another_directory"
        target.mkdir()
        (namespace / ("_runs" if alias == "reports" else "bronze")).symlink_to(
            target, target_is_directory=True
        )
    with pytest.raises(ConfigurationError, match="enlazado"):
        config.validate()


@pytest.mark.unit
def test_weather_cannot_reuse_another_source(config):
    with pytest.raises(ConfigurationError, match="distintos"):
        replace(config, weather=config.metadata).validate()


@pytest.mark.unit
@pytest.mark.parametrize("filename", ["electricity[12].csv", "electricity?.csv", "electricity{1,2}.csv"])
def test_literal_glob_names_are_rejected(config, filename):
    path = config.electricity.with_name(filename)
    path.write_text("timestamp,SYNTHETIC_A\n2016,999\n")
    with pytest.raises(SourceError, match="glob"):
        inspect_source(path, "electricity")


@pytest.mark.unit
@pytest.mark.parametrize("payload", [
    b"timestamp,SYNTHETIC_A\n" + b"2016,12.5\n" * 2000 + b"2017,\xff\n",
    b'timestamp,SYNTHETIC_A\n2016,"123\n2017,456\n',
    b'timestamp,SYNTHETIC_A\n2016,"abc"def"\n',
])
def test_invalid_encoding_or_quoted_records_are_rejected(config, payload):
    config.electricity.write_bytes(payload)
    with pytest.raises(SourceError):
        inspect_source(config.electricity, "electricity")


@pytest.mark.unit
def test_weather_requires_timestamp_and_site_id(config):
    path = config.raw_root / "bad-weather.csv"
    path.write_text("timestamp,airTemperature\n2016,12\n")
    with pytest.raises(SourceError, match="obligatorias"):
        inspect_source(path, "weather")


@pytest.mark.unit
def test_namespace_lock_rejects_another_writer(config):
    import fcntl
    config.namespace.mkdir(parents=True)
    with (config.namespace / "_ingestion.lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(IngestionError, match="otra ingesta"):
            run_bronze(object(), config)


def _manifest(config):
    return {
        "report_contract_version": 1, "run_id": "synthetic-manifest", "status": "passed",
        "data_kind": config.data_kind, "dataset_reference": config.dataset_reference,
        "configured_sources": {name: path.resolve().as_uri() for name, path in config.sources.items()},
        "tables": [
            {"name": name, "source_uri": path.resolve().as_uri(),
             "table_path": str(config.table_path(name)), "status": "verified", "delta_version": 0}
            for name, path in config.sources.items()
        ],
    }


@pytest.mark.unit
@pytest.mark.parametrize("invalid", ["missing", "legacy", "json", "duplicate", "outside", "boolean", "negative"])
def test_invalid_publication_never_falls_back_to_current_tables(config, invalid):
    reports = config.namespace / "_runs"
    reports.mkdir(parents=True)
    report = _manifest(config)
    if invalid == "legacy":
        report.pop("report_contract_version")
    elif invalid == "duplicate":
        report["tables"][1] = report["tables"][0]
    elif invalid == "outside":
        report["tables"][0]["table_path"] = str(config.raw_root)
    elif invalid == "boolean":
        report["tables"][0]["delta_version"] = True
    elif invalid == "negative":
        report["tables"][0]["delta_version"] = -1
    if invalid != "missing":
        (reports / "report.json").write_text("{" if invalid == "json" else json.dumps(report))
    with pytest.raises(SnapshotError):
        read_bronze_snapshot(object(), config)


@pytest.mark.unit
def test_real_fixture_uses_configured_spark_options(config, monkeypatch):
    import conftest
    configured = replace(config, master="local[1]", ivy_dir=config.raw_root / "ivy")
    monkeypatch.setenv("TFM_REAL_CONFIG", "synthetic-config.toml")
    monkeypatch.setattr(conftest, "load_config", lambda path: configured)
    captured = {}

    @contextmanager
    def fake_session(app_name, **options):
        captured.update(options)
        yield "synthetic-session"
        captured["closed"] = True

    monkeypatch.setattr(conftest, "spark_session", fake_session)
    fixture = conftest.spark.__wrapped__()
    assert next(fixture) == "synthetic-session"
    with pytest.raises(StopIteration):
        next(fixture)
    assert captured == {"master": "local[1]", "ivy_dir": configured.ivy_dir, "closed": True}


@pytest.mark.integration
def test_multiline_quotes_empty_fields_and_blank_lines(spark, config):
    config.electricity.write_text(
        'timestamp,SYNTHETIC_A\n\n2016,"a ""quoted""\nvalue"\n2017,\n'
    )
    info = inspect_source(config.electricity, "electricity")
    assert info.rows == 2
    rows = [tuple(row) for row in read_source(spark, info).orderBy("timestamp").collect()]
    assert rows == [("2016", 'a "quoted"\nvalue'), ("2017", None)]


@pytest.mark.integration
def test_spark_input_files_must_match_the_inspected_source(spark, config, monkeypatch):
    info = inspect_source(config.electricity, "electricity")
    monkeypatch.setattr(DataFrame, "inputFiles", lambda self: [config.metadata.as_uri()])
    with pytest.raises(SourceError, match="distintos"):
        read_source(spark, info)


@pytest.mark.integration
def test_independent_record_count_mismatch_prevents_all_writes(spark, config, monkeypatch):
    original = bronze.inspect_source

    def incorrect_count(path, name):
        info = original(path, name)
        return replace(info, rows=info.rows + 1) if name == "electricity" else info

    monkeypatch.setattr(bronze, "inspect_source", incorrect_count)
    with pytest.raises(IngestionError, match="no coinciden"):
        run_bronze(spark, config)
    assert not config.table_path("electricity").exists()
    assert not config.table_path("metadata").exists()


@pytest.mark.integration
def test_weather_roundtrip_and_rerun_preserve_original_values(spark, config):
    config = replace(config, weather=config.raw_root / "data/weather/weather.csv")
    originals = {path: path.read_bytes() for path in config.sources.values()}
    run_bronze(spark, config)
    report = run_bronze(spark, config)
    assert [item["output_rows"] for item in report["tables"]] == [4, 2, 3]
    selection, frames = read_bronze_snapshot(spark, config)
    assert selection["run_id"] == report["run_id"]
    rows = frames["weather"].orderBy("site_id", "timestamp").collect()
    assert [(row["airTemperature"], row["precipDepth1HR"]) for row in rows] == [
        ("0", "-1.0"), (None, "NaN"), ("12.50", "0")
    ]
    assert originals == {path: path.read_bytes() for path in originals}


def _fail_metadata(original):
    def save(self, path=None, *args, **kwargs):
        if path and str(path).endswith("/bronze/metadata"):
            raise OSError("synthetic primary persistence failure")
        return original(self, path, *args, **kwargs)
    return save


@pytest.mark.integration
def test_partial_reload_reads_previous_complete_versions(spark, config, monkeypatch):
    first = run_bronze(spark, config)
    config.electricity.write_text("timestamp,SYNTHETIC_A\n2016,777\n")
    config.metadata.write_text("building_id,site_id\nSYNTHETIC_NEW,SYNTHETIC_SITE\n")
    monkeypatch.setattr(DataFrameWriter, "save", _fail_metadata(DataFrameWriter.save))
    with pytest.raises(IngestionError, match="primary persistence failure"):
        run_bronze(spark, config)
    latest = spark.read.format("delta").load(str(config.table_path("electricity")))
    assert latest.count() == 1
    selection, frames = read_bronze_snapshot(spark, config)
    assert selection["run_id"] == first["run_id"]
    assert selection["using_previous_successful_run"] is True
    assert selection["latest_attempt"]["status"] == "failed"
    assert frames["electricity"].count() == 4
    assert frames["metadata"].count() == 2


@pytest.mark.integration
def test_weather_configuration_requires_a_three_table_publication(spark, config):
    run_bronze(spark, config)
    configured = replace(config, weather=config.raw_root / "data/weather/weather.csv")
    with pytest.raises(SnapshotError, match="completa compatible"):
        read_bronze_snapshot(spark, configured)


@pytest.mark.integration
@pytest.mark.parametrize("primary_failure", [False, True])
def test_cleanup_failure_keeps_report_and_primary_cause(spark, config, monkeypatch, primary_failure):
    with monkeypatch.context() as patch:
        if primary_failure:
            patch.setattr(DataFrameWriter, "save", _fail_metadata(DataFrameWriter.save))
        patch.setattr(DataFrame, "unpersist", lambda self: (_ for _ in ()).throw(
            RuntimeError("synthetic cleanup failure")))
        with pytest.raises(IngestionError) as caught:
            run_bronze(spark, config)
    spark.catalog.clearCache()
    report = json.loads(next((config.namespace / "_runs").glob("*.json")).read_text())
    assert report["status"] == "failed"
    assert report["cleanup_errors"]
    assert type(caught.value.__cause__) is (OSError if primary_failure else RuntimeError)
    if primary_failure:
        assert report["error"] == "synthetic primary persistence failure"
    assert not any((config.namespace / "_runs").glob("*.tmp"))


@pytest.mark.integration
def test_report_failure_does_not_replace_original_failure(spark, config, monkeypatch):
    monkeypatch.setattr(DataFrameWriter, "save", _fail_metadata(DataFrameWriter.save))

    def cannot_report(path, report):
        raise OSError("synthetic report failure")

    monkeypatch.setattr(bronze, "_write_report", cannot_report)
    with pytest.raises(IngestionError, match="primary persistence failure") as caught:
        run_bronze(spark, config)
    assert isinstance(caught.value.__cause__, OSError)
    assert any("synthetic report failure" in note for note in caught.value.__cause__.__notes__)


@pytest.mark.integration
def test_recorded_version_belongs_to_this_commit(spark, config, monkeypatch):
    original = DataFrameWriter.save

    def unrelated_commit(self, path=None, *args, **kwargs):
        result = original(self, path, *args, **kwargs)
        if path and str(path).endswith("/bronze/electricity"):
            external = (spark.read.format("delta").load(str(path))
                        .withColumn("SYNTHETIC_lodging_A", F.lit("SYNTHETIC_EXTERNAL")))
            writer = (external.write.format("delta").mode("overwrite")
                      .option("userMetadata", "synthetic unrelated writer"))
            original(writer, path)
        return result

    monkeypatch.setattr(DataFrameWriter, "save", unrelated_commit)
    report = run_bronze(spark, config)
    assert report["tables"][0]["delta_version"] == 0
    selection, frames = read_bronze_snapshot(spark, config)
    assert selection["run_id"] == report["run_id"]
    values = [row[0] for row in frames["electricity"].select("SYNTHETIC_lodging_A").collect()]
    assert "SYNTHETIC_EXTERNAL" not in values


@pytest.mark.integration
def test_recreated_table_cannot_impersonate_a_published_snapshot(spark, config):
    report = run_bronze(spark, config)
    published = report["tables"][0]
    target = config.table_path("electricity")
    replacement = target.with_name("electricity_replacement")
    original = spark.read.format("delta").load(str(target))
    changed = original.withColumn("SYNTHETIC_lodging_A", F.lit("SYNTHETIC_REPLACEMENT"))
    (changed.write.format("delta").option("userMetadata", "synthetic replacement")
     .save(str(replacement)))
    target.rename(target.with_name("electricity_original"))
    replacement.rename(target)
    recreated = spark.read.format("delta").load(str(target))
    assert recreated.schema.jsonValue() == published["output_schema"]
    assert recreated.count() == published["output_rows"]
    with pytest.raises(SnapshotError, match="no pertenece a la ejecución publicada"):
        read_bronze_snapshot(spark, config)
