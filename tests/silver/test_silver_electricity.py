"""Synthetic-only checks for the provisional, non-persisting Silver transform."""
from collections import Counter
import hashlib
from pathlib import Path
import shutil

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
import pytest

from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.silver import silver_electricity as silver

LINEAGE = (
    "_bronze_source_uri", "_bronze_source_sha256", "_bronze_data_kind",
    "_bronze_dataset_reference",
)
PROVENANCE = (
    "file:///synthetic/electricity.csv", hashlib.sha256(b"synthetic-only").hexdigest(),
    "synthetic", "synthetic-silver-test-v1",
)


def string_schema(*names):
    return StructType([StructField(name, StringType(), True) for name in names])


@pytest.mark.unit
@pytest.mark.parametrize("columns", [[], (), "SYNTHETIC_A", b"SYNTHETIC_A", None,
                                     {"SYNTHETIC_A"}, [None], [1], [""], [" "]])
def test_invalid_selection_without_jvm(columns):
    schema = string_schema("timestamp", "SYNTHETIC_A", *LINEAGE)
    with pytest.raises(ValueError):
        silver._validate_columns(schema, columns)


@pytest.mark.unit
@pytest.mark.parametrize("columns,match", [
    (["absent"], "does not exist"),
    (["synthetic_a"], "does not exist"),
    (["SYNTHETIC_A", "SYNTHETIC_A"], "Duplicate"),
    (["timestamp"], "not electricity"),
    (["TIMESTAMP"], "not electricity"),
    *[([name], "not electricity") for name in LINEAGE],
    (["_BRONZE_extra"], "not electricity"),
])
def test_reject_missing_duplicate_and_technical_names(columns, match):
    with pytest.raises(ValueError, match=match):
        silver._validate_columns(string_schema("timestamp", "SYNTHETIC_A", *LINEAGE), columns)


@pytest.mark.unit
@pytest.mark.parametrize("missing", ["timestamp", *LINEAGE])
def test_require_temporal_and_lineage_fields(missing):
    schema = string_schema(*[name for name in ("timestamp", "SYNTHETIC_A", *LINEAGE)
                             if name != missing])
    with pytest.raises(ValueError, match="Missing"):
        silver._validate_columns(schema, ["SYNTHETIC_A"])


@pytest.mark.unit
@pytest.mark.parametrize("field", ["timestamp", "SYNTHETIC_A", "unselected", *LINEAGE])
def test_reject_non_string_schema(field):
    schema = string_schema("timestamp", "SYNTHETIC_A", "unselected", *LINEAGE)
    schema = StructType([StructField(f.name, IntegerType() if f.name == field else f.dataType)
                         for f in schema])
    with pytest.raises(ValueError, match="StringType"):
        silver._validate_columns(schema, ["SYNTHETIC_A"])


@pytest.mark.unit
@pytest.mark.parametrize("extra", ["SYNTHETIC_A", "synthetic_a", "_BRONZE_SOURCE_URI",
                                   "", " ", "_bronze_unknown"])
def test_reject_ambiguous_or_unsupported_schema(extra):
    schema = string_schema("timestamp", "SYNTHETIC_A", *LINEAGE, extra)
    with pytest.raises(ValueError):
        silver._validate_columns(schema, ["SYNTHETIC_A"])


@pytest.mark.unit
def test_validation_accepts_exact_names_without_mutating_selection():
    selected = ["SYNTHETIC_B", "SYNTHETIC_A"]
    assert silver._validate_columns(
        string_schema("timestamp", "SYNTHETIC_A", "SYNTHETIC_B", *LINEAGE), selected,
    ) == ("SYNTHETIC_B", "SYNTHETIC_A")
    assert selected == ["SYNTHETIC_B", "SYNTHETIC_A"]


@pytest.mark.integration
@pytest.mark.parametrize("selected", [["SYNTHETIC_A"], ["SYNTHETIC_B", "SYNTHETIC_A"]])
def test_values_multiplicity_and_lineage_are_preserved(spark, selected):
    rows = [
        ("2017-10-29 01:00:00", "0", "0.0", *PROVENANCE),
        ("2017-10-29 01:00:00", "0", "0.0", *PROVENANCE),
        ("2017-10-29 01:00:00", "-1.0", "999999999999999999999.00", *PROVENANCE),
        ("2017-03-26 01:00:00", None, "", *PROVENANCE),
        ("invalid timestamp", "NaN", "  0012.50  ", *PROVENANCE),
        (None, "-999", "+Inf", *PROVENANCE),
        ("", "special-code", "-0.0", *[None] * len(LINEAGE)),
    ]
    schema = string_schema("timestamp", "SYNTHETIC_A", "SYNTHETIC_B", *LINEAGE)
    wide = spark.createDataFrame(rows, schema)
    actual = silver.electricity_to_long(wide, selected)
    expected = Counter(
        (name, row[0], row[1 if name == "SYNTHETIC_A" else 2], *row[3:])
        for row in rows for name in selected
    )
    assert actual.columns == ["building_id", "timestamp_original", "electricity_value_original",
                              *LINEAGE]
    assert all(isinstance(field.dataType, StringType) for field in actual.schema)
    assert Counter(map(tuple, actual.collect())) == expected
    assert actual.count() == wide.count() * len(selected)
    assert Counter(map(tuple, wide.collect())) == Counter(rows)


@pytest.mark.integration
def test_names_are_literal_identifiers_and_never_sql(spark):
    names = ["synthetic.dot", "synthetic`tick", "synthetic space", "synthetic'quote",
             "x'); DROP TABLE synthetic; --", "building_id", "timestamp_original", "ñ合成"]
    wide = spark.createDataFrame(
        [("original label", *[f"value-{i}" for i in range(len(names))], *PROVENANCE)],
        string_schema("timestamp", *names, *LINEAGE),
    )
    result = silver.electricity_to_long(wide, names)
    assert Counter(map(tuple, result.collect())) == Counter(
        (name, "original label", f"value-{i}", *PROVENANCE) for i, name in enumerate(names)
    )
    assert result.count() == wide.count() * len(names)


@pytest.mark.integration
def test_empty_bronze_is_a_valid_empty_long_frame(spark):
    wide = spark.createDataFrame([], string_schema("timestamp", "SYNTHETIC_A", *LINEAGE))
    result = silver.electricity_to_long(wide, ["SYNTHETIC_A"])
    assert result.count() == wide.count() * 1 == 0
    assert result.columns[:3] == ["building_id", "timestamp_original", "electricity_value_original"]


@pytest.mark.integration
def test_all_null_measurements_still_produce_every_row(spark):
    rows = [(None, None, None, *PROVENANCE)] * 2
    wide = spark.createDataFrame(rows, string_schema("timestamp", "SYNTHETIC_A", "SYNTHETIC_B",
                                                    *LINEAGE))
    result = silver.electricity_to_long(wide, ["SYNTHETIC_A", "SYNTHETIC_B"])
    assert Counter(map(tuple, result.collect())) == Counter({
        ("SYNTHETIC_A", None, None, *PROVENANCE): 2,
        ("SYNTHETIC_B", None, None, *PROVENANCE): 2,
    })
    assert result.count() == wide.count() * 2 == 4


@pytest.mark.integration
def test_transform_is_lazy_and_rejects_bad_schema_before_building_a_plan(spark, monkeypatch):
    wide = spark.createDataFrame([("label", "0", *PROVENANCE)],
                                 string_schema("timestamp", "SYNTHETIC_A", *LINEAGE))

    def forbidden(*args, **kwargs):
        raise AssertionError("The transform must not collect or count")

    with monkeypatch.context() as patch:
        patch.setattr(DataFrame, "collect", forbidden)
        patch.setattr(DataFrame, "count", forbidden)
        result = silver.electricity_to_long(wide, ["SYNTHETIC_A"])
        with pytest.raises(ValueError, match="does not exist"):
            silver.electricity_to_long(wide, ["absent"])
        with pytest.raises(ValueError, match="StringType"):
            silver.electricity_to_long(wide.withColumn("SYNTHETIC_A", F.lit(0)), ["SYNTHETIC_A"])
    assert result.count() == 1


@pytest.mark.integration
def test_reader_uses_published_version_not_latest_physical(spark, tmp_path, monkeypatch):
    raw = tmp_path / "synthetic-input"
    shutil.copytree(Path(__file__).parent.parent / "fixtures/synthetic_bdg2", raw)
    config = BronzeConfig(
        raw_root=raw, lakehouse_root=tmp_path / "synthetic-output",
        electricity=raw / "data/meters/raw/electricity.csv",
        metadata=raw / "data/metadata/metadata.csv", data_kind="synthetic",
        dataset_reference="synthetic-software-fixtures-v1",
    )
    sources_before = {path: path.read_bytes() for path in config.sources.values()}
    published = run_bronze(spark, config)
    item = next(item for item in published["tables"] if item["name"] == "electricity")
    path = str(config.table_path("electricity"))
    # A later physical commit is not a complete Bronze publication.
    published_frame = (spark.read.format("delta")
                       .option("versionAsOf", item["delta_version"]).load(path))
    later = published_frame.limit(1).withColumn("SYNTHETIC_lodging_A", F.lit("unpublished"))
    (later.write.format("delta").mode("overwrite")
     .option("userMetadata", "unpublished-test").save(path))
    assert DeltaTable.forPath(spark, path).history(1).first()["version"] > item["delta_version"]
    assert spark.read.format("delta").load(path).first()["SYNTHETIC_lodging_A"] == "unpublished"

    reader = silver.read_bronze_snapshot
    selections = []

    def record_reader(session, configuration):
        selection, frames = reader(session, configuration)
        selections.append(selection)
        return selection, frames

    monkeypatch.setattr(silver, "read_bronze_snapshot", record_reader)
    selected = ["SYNTHETIC_lodging_A", "SYNTHETIC_lodging_B"]
    selection, result = silver.read_electricity_long(spark, config, selected)
    assert len(selections) == 1 and selection is selections[0]
    assert selection["run_id"] == published["run_id"]
    chosen = next(table for table in selection["tables"] if table["name"] == "electricity")
    assert chosen["delta_version"] == item["delta_version"]
    provenance = (
        config.electricity.as_uri(),
        hashlib.sha256(sources_before[config.electricity]).hexdigest(),
        "synthetic", config.dataset_reference,
    )
    context = ("electricity", published["run_id"], item["delta_version"], path)
    original = [
        ("2016-01-01 00:00:00", "0", "12.50"),
        ("2016-01-01 01:00:00", None, "NaN"),
        ("2016-01-01 02:00:00", "-1.0", "999999.00"),
        ("2016-01-01 02:00:00", "-1.0", "999999.00"),
    ]
    expected = Counter((name, row[0], row[i + 1], *provenance, *context)
                       for row in original for i, name in enumerate(selected))
    assert Counter(map(tuple, result.collect())) == expected
    assert result.count() == item["output_rows"] * len(selected) == 8
    assert sources_before == {source: source.read_bytes() for source in sources_before}
    assert not (config.lakehouse_root / "real").exists()
    assert not (config.namespace / "silver").exists()
