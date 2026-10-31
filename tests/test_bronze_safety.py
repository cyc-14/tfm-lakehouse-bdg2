from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

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
@pytest.mark.parametrize("invalid", ["missing", "legacy", "json", "duplicate", "outside",
                                    "boolean", "negative", "incomplete"])
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
    elif invalid == "incomplete":
        report["tables"] = []
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

    def cannot_report(path, report, **kwargs):
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


def fake_ingestion(config, monkeypatch, controls=None):
    """Exercise orchestration without starting Spark or creating Delta tables."""
    controls = {} if controls is None else controls
    events = []

    class Schema(list):
        def jsonValue(self):
            return {"type": "struct", "fields": []}

    class Frame:
        schema = Schema()

        def __init__(self, info):
            self.info = info

        def persist(self, level):
            return self

        def count(self):
            return self.info.rows

        def withColumn(self, name, value):
            return self

        @property
        def write(self):
            return self

        def format(self, value):
            return self

        def mode(self, value):
            assert value == "overwrite"
            return self

        def option(self, name, value):
            return self

        def save(self, target):
            events.append(f"save:{Path(target).name}")
            if Path(target).name == "metadata" and controls.get("work_error") is not None:
                raise controls["work_error"]

        def unpersist(self):
            events.append(f"unpersist:{self.info.path.name}")
            if self.info.path == config.electricity and controls.get("cleanup_error") is not None:
                raise controls["cleanup_error"]

    frames = {}

    def read(spark, info):
        frame = Frame(info)
        frames[info.path] = frame
        return frame

    class Reader:
        def format(self, value):
            return self

        def option(self, name, value):
            assert name == "versionAsOf"
            assert value == 0
            return self

        def load(self, path):
            return frames[config.sources[Path(path).name]]

    class Conf:
        def get(self, key):
            if controls.get("before_error") is not None:
                raise controls["before_error"]
            return "true"

        def set(self, key, value):
            events.append(f"pruning:{value}")
            if value == "true" and controls.get("restore_error") is not None:
                raise controls["restore_error"]

    class History:
        def history(self):
            return self

        def where(self, value):
            return self

        def select(self, value):
            return self

        def limit(self, value):
            return self

        def collect(self):
            return [{"version": 0}]

    monkeypatch.setattr(bronze, "read_source", read)
    monkeypatch.setattr(bronze, "F", SimpleNamespace(lit=lambda value: value, col=lambda name: name))
    monkeypatch.setattr(bronze, "DeltaTable", SimpleNamespace(
        isDeltaTable=lambda *args: False, forPath=lambda *args: History()))
    directory = config.namespace / "_runs"
    directory.mkdir(parents=True)
    return SimpleNamespace(conf=Conf(), read=Reader()), directory, events


def saved_report(directory):
    return json.loads(next(directory.glob("*.json")).read_text())


@pytest.mark.unit
@pytest.mark.parametrize("cancel", [KeyboardInterrupt, SystemExit])
def test_cancellation_before_preparation_is_failed_and_propagated(config, monkeypatch, cancel):
    interruption = cancel(23)
    spark, directory, events = fake_ingestion(config, monkeypatch, {"before_error": interruption})
    with pytest.raises(cancel) as caught:
        bronze._run_locked(spark, config, directory)
    assert caught.value is interruption
    if cancel is SystemExit:
        assert caught.value.code == 23
    report = saved_report(directory)
    assert report["status"] == "failed"
    assert report["tables"] == []
    assert report["error_type"] == cancel.__name__
    assert report["interruption"]["phase"] == "work"
    assert events == []


@pytest.mark.unit
@pytest.mark.parametrize("primary_kind", ["ordinary", "cancellation", "none"])
def test_cleanup_cancellation_preserves_primary_cause(config, monkeypatch, primary_kind):
    primary = {"ordinary": OSError("synthetic primary failure"),
               "cancellation": KeyboardInterrupt("synthetic primary cancellation"),
               "none": None}[primary_kind]
    secondary = SystemExit(31)
    spark, directory, events = fake_ingestion(config, monkeypatch, {
        "work_error": primary, "cleanup_error": secondary,
    })
    expected = primary if primary_kind == "cancellation" else secondary
    with pytest.raises(type(expected)) as caught:
        bronze._run_locked(spark, config, directory)
    assert caught.value is expected
    if primary_kind == "ordinary":
        assert caught.value.__cause__ is primary
    report = saved_report(directory)
    assert report["status"] == "failed"
    assert report["error_type"] == type(primary if primary is not None else secondary).__name__
    assert report["cleanup_errors"][0]["error_type"] == "SystemExit"
    assert "unpersist:metadata.csv" in events
    assert events[-1] == "pruning:true"


@pytest.mark.unit
def test_restore_cancellation_prevents_passed_publication(config, monkeypatch):
    interruption = KeyboardInterrupt("synthetic restore cancellation")
    spark, directory, events = fake_ingestion(config, monkeypatch, {"restore_error": interruption})
    with pytest.raises(KeyboardInterrupt) as caught:
        bronze._run_locked(spark, config, directory)
    assert caught.value is interruption
    report = saved_report(directory)
    assert report["status"] == "failed"
    assert all(item["status"] == "verified" for item in report["tables"])
    assert report["interruption"]["phase"] == "restore_csv_pruning"


@pytest.mark.unit
@pytest.mark.parametrize("stage", ["serialize", "close", "before_replace", "after_replace"])
@pytest.mark.parametrize("cancel", [KeyboardInterrupt, SystemExit])
def test_bronze_publication_cancellation_boundary(config, monkeypatch, stage, cancel):
    interruption = cancel(41)
    spark, directory, events = fake_ingestion(config, monkeypatch)
    original_dump, original_open, original_replace = json.dump, Path.open, Path.replace
    triggered = False

    def dump(payload, handle, **kwargs):
        nonlocal triggered
        if stage == "serialize" and not triggered:
            triggered = True
            handle.write("{")
            raise interruption
        return original_dump(payload, handle, **kwargs)

    def open_report(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if stage != "close" or path.suffix != ".tmp":
            return handle
        class Writer:
            def write(self, value):
                return handle.write(value)

            def close(self):
                nonlocal triggered
                handle.close()
                if not triggered:
                    triggered = True
                    raise interruption
        return Writer()

    def replace(path, target):
        nonlocal triggered
        if stage == "before_replace" and not triggered:
            triggered = True
            raise interruption
        result = original_replace(path, target)
        if stage == "after_replace" and not triggered:
            triggered = True
            raise interruption
        return result

    monkeypatch.setattr(json, "dump", dump)
    monkeypatch.setattr(Path, "open", open_report)
    monkeypatch.setattr(Path, "replace", replace)
    with pytest.raises(cancel) as caught:
        bronze._run_locked(spark, config, directory)
    assert caught.value is interruption
    if cancel is SystemExit:
        assert caught.value.code == 41
    report = saved_report(directory)
    assert report["status"] == ("passed" if stage == "after_replace" else "failed")
    assert len(report["tables"]) == 2
    assert all(item["status"] == "verified" for item in report["tables"])
    assert not list(directory.glob("*.tmp"))


@pytest.mark.unit
def test_bronze_report_finalizer_preserves_first_cancellation(tmp_path, monkeypatch):
    target = tmp_path / "report.json"
    first = KeyboardInterrupt("synthetic serialization cancellation")
    second = SystemExit(7)
    original_open = Path.open

    def open_report(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        class Writer:
            def write(self, payload):
                handle.write(payload)

            def close(self):
                handle.close()
                raise second
        return Writer()

    def fail(payload, handle, **kwargs):
        handle.write("{")
        raise first

    monkeypatch.setattr(Path, "open", open_report)
    monkeypatch.setattr(json, "dump", fail)
    with pytest.raises(KeyboardInterrupt) as caught:
        bronze._write_report(target, {"status": "failed"})
    assert caught.value is first
    assert not target.exists()
    assert not target.with_suffix(".tmp").exists()
    assert any("close_report" in note for note in first.__notes__)


@pytest.mark.unit
def test_publication_error_then_cleanup_cancel_retains_diagnostic(config, monkeypatch):
    spark, directory, events = fake_ingestion(config, monkeypatch)
    primary = OSError("synthetic serialization failure")
    interruption = KeyboardInterrupt("synthetic report close cancellation")
    original_open, original_dump = Path.open, json.dump
    write_failed = close_failed = False

    def dump(payload, handle, **kwargs):
        nonlocal write_failed
        if not write_failed:
            write_failed = True
            handle.write("{")
            raise primary
        return original_dump(payload, handle, **kwargs)

    def open_report(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if path.suffix != ".tmp":
            return handle
        class Writer:
            def write(self, payload):
                return handle.write(payload)

            def close(self):
                nonlocal close_failed
                handle.close()
                if not close_failed:
                    close_failed = True
                    raise interruption
        return Writer()

    monkeypatch.setattr(json, "dump", dump)
    monkeypatch.setattr(Path, "open", open_report)
    with pytest.raises(KeyboardInterrupt) as caught:
        bronze._run_locked(spark, config, directory)
    assert caught.value is interruption
    assert caught.value.__cause__ is primary
    report = saved_report(directory)
    assert report["status"] == "failed"
    assert report["error_type"] == "OSError"
    assert report["error"] == str(primary)
    assert report["interruption"]["error_type"] == "KeyboardInterrupt"


@pytest.mark.unit
def test_failed_publication_inspection_never_demotes_published_snapshot(config, monkeypatch):
    spark, directory, events = fake_ingestion(config, monkeypatch)
    interruption = KeyboardInterrupt("synthetic cancellation after replacement")
    original_replace, original_lstat = Path.replace, Path.lstat

    def replace(path, target):
        result = original_replace(path, target)
        raise interruption

    def lstat(path):
        if path.parent == directory and path.suffix == ".json":
            raise OSError("synthetic transient inspection failure")
        return original_lstat(path)

    monkeypatch.setattr(Path, "replace", replace)
    monkeypatch.setattr(Path, "lstat", lstat)
    with pytest.raises(KeyboardInterrupt) as caught:
        bronze._run_locked(spark, config, directory)
    assert caught.value is interruption
    assert saved_report(directory)["status"] == "passed"


@pytest.mark.unit
def test_publication_failure_keeps_cause_and_retry_is_bounded(config, monkeypatch):
    spark, directory, events = fake_ingestion(config, monkeypatch)
    first = OSError("synthetic first publication failure")
    calls = []

    def fail(path, report, **kwargs):
        calls.append(report["status"])
        raise first if len(calls) == 1 else RuntimeError("synthetic retry failure")

    monkeypatch.setattr(bronze, "_write_report", fail)
    with pytest.raises(IngestionError) as caught:
        bronze._run_locked(spark, config, directory)
    assert caught.value.__cause__ is first
    assert calls == ["passed", "failed"]
    assert not list(directory.glob("*.json"))


@pytest.mark.unit
def test_namespace_cleanup_does_not_replace_cancellation(config, monkeypatch):
    import fcntl
    config.namespace.mkdir(parents=True)
    interruption = KeyboardInterrupt("synthetic work cancellation")
    original = fcntl.flock

    def flock(handle, operation):
        if operation == fcntl.LOCK_UN:
            raise OSError("synthetic unlock failure")
        return original(handle, operation)

    monkeypatch.setattr(fcntl, "flock", flock)
    with pytest.raises(KeyboardInterrupt) as caught:
        with bronze._namespace_lock(config.namespace):
            raise interruption
    assert caught.value is interruption
    assert any("unlock_namespace" in note for note in interruption.__notes__)


@pytest.mark.integration
@pytest.mark.parametrize("cancel", [KeyboardInterrupt, SystemExit])
def test_interrupted_reload_recovers_previous_publication(spark, config, monkeypatch, cancel):
    first = run_bronze(spark, config)
    before = {path: path.read_bytes() for path in config.sources.values()}
    interruption = cancel(53)
    original = DataFrameWriter.save

    def interrupt_metadata(self, path=None, *args, **kwargs):
        if path and str(path).endswith("/bronze/metadata"):
            raise interruption
        return original(self, path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(DataFrameWriter, "save", interrupt_metadata)
        with pytest.raises(cancel) as caught:
            run_bronze(spark, config)
    assert caught.value is interruption
    attempts = [json.loads(path.read_text()) for path in (config.namespace / "_runs").glob("*.json")]
    failed = next(report for report in attempts if report["run_id"] != first["run_id"])
    assert failed["status"] == "failed"
    assert failed["tables"][0]["status"] == "verified"
    assert failed["tables"][1]["status"] == "validated"
    assert "delta_version" not in failed["tables"][1]
    selected, frames = read_bronze_snapshot(spark, config)
    assert selected["run_id"] == first["run_id"]
    assert selected["using_previous_successful_run"] is True
    assert selected["latest_attempt"]["status"] == "failed"
    assert frames["electricity"].count() == 4
    assert before == {path: path.read_bytes() for path in before}


@pytest.mark.integration
def test_interrupted_first_ingestion_has_no_published_snapshot(spark, config, monkeypatch):
    original = DataFrameWriter.save

    def interrupt_metadata(self, path=None, *args, **kwargs):
        if path and str(path).endswith("/bronze/metadata"):
            raise KeyboardInterrupt("synthetic cancellation after electricity")
        return original(self, path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(DataFrameWriter, "save", interrupt_metadata)
        with pytest.raises(KeyboardInterrupt):
            run_bronze(spark, config)
    with pytest.raises(SnapshotError, match="completa compatible"):
        read_bronze_snapshot(spark, config)
