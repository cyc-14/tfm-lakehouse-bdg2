"""Invented fixtures for IQR Delta identity, ordering and failure recovery."""
from collections import Counter
from copy import deepcopy
from dataclasses import replace
import json
import os
from pathlib import Path

from delta.tables import DeltaTable
import pytest
from pyspark.sql import functions as F
from pyspark.sql.readwriter import DataFrameWriter

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.ingestion.snapshot import SnapshotError
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver.silver_iqr import IQR_METHOD, summarize_electricity_iqr


def _create_silver(spark, root, name="silver", *, interrupt=False):
    raw = root / "raw"
    raw.mkdir(exist_ok=True)
    electricity, metadata = raw / "electricity.csv", raw / "metadata.csv"
    labels = ["2016-01-01 00:00:00", "2016-01-01 01:00:00",
              "2016-01-01 01:00:00", "2016-02-29 02:00:00", "2017-12-31 11:37:00",
              "", "2018-01-01 00:00:00", "bad"]
    rows = []
    for index, label in enumerate(labels):
        values = ["0" if index < 4 else "100"] * 16
        values[1] = ("99999999999999999999.00000000000000000" + str(index)
                     if index < 4 else "NaN")
        values[2] = (("-99999999999999999999.999999999999999999" if index < 2
                      else "99999999999999999999.999999999999999999") if index < 4 else "NaN")
        values[-1] = "NaN"
        if index == 0:
            values[-2] = ""
        rows.append(label + "," + ",".join(values))
    electricity.write_text("timestamp," + ",".join(APPROVED_BUILDINGS) + "\n"
                           + "\n".join(rows) + "\n")
    metadata.write_text("building_id\n" + "\n".join(APPROVED_BUILDINGS) + "\n")
    config = BronzeConfig(raw, root / "bronze", electricity, metadata, "synthetic", "invented")
    report = run_bronze(spark, config)
    if interrupt:
        original = silver._exclusive_json

        def stop(path, value):
            if path.name == "completed.json":
                raise OSError("invented Silver interruption")
            return original(path, value)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(silver, "_exclusive_json", stop)
            with pytest.raises(OSError, match="interruption"):
                silver.publish_synthetic_silver(spark, config, report["run_id"], name, root)
        return config, report, None
    result = silver.publish_synthetic_silver(spark, config, report["run_id"], name, root)
    return config, report, result


@pytest.fixture
def published(spark, tmp_path):
    config, bronze, result = _create_silver(spark, tmp_path)
    return tmp_path, config, bronze, result


def _log(actions):
    return b"\n".join(iqr._canonical(action).encode() for action in actions)


def _actions():
    return [{"commitInfo": {"timestamp": 1234567890123, "userMetadata": "{}"}},
            {"metaData": {"id": "invented-table", "configuration": {}}},
            {"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}}]


@pytest.mark.unit
@pytest.mark.parametrize("case", ["missing_info", "duplicate_info", "missing_time", "null_time",
                                  "bool_time", "string_time", "negative_time", "large_time",
                                  "wrong_metadata", "missing_id", "ict", "ict_feature",
                                  "duplicate_json_field", "invalid_json", "nonfinite_json"])
def test_commit_timestamp_decoder_rejects_ambiguous_and_unsupported_evidence(case):
    actions = _actions()
    if case == "missing_info":
        actions.pop(0)
    elif case == "duplicate_info":
        actions.append(deepcopy(actions[0]))
    elif case == "missing_time":
        del actions[0]["commitInfo"]["timestamp"]
    elif case.endswith("_time"):
        actions[0]["commitInfo"]["timestamp"] = {
            "null_time": None, "bool_time": True, "string_time": "123",
            "negative_time": -1, "large_time": 2**63,
        }[case]
    elif case == "wrong_metadata":
        actions[0]["commitInfo"]["userMetadata"] = "another intent"
    elif case == "missing_id":
        del actions[1]["metaData"]["id"]
    elif case == "ict":
        actions[0]["commitInfo"]["inCommitTimestamp"] = 123
    elif case == "ict_feature":
        actions[2]["protocol"]["writerFeatures"] = ["inCommitTimestamp"]
    raw = _log(actions)
    if case == "duplicate_json_field":
        raw = b'{"commitInfo":{"timestamp":1,"timestamp":2}}'
    elif case == "invalid_json":
        raw = b'{"commitInfo":'
    elif case == "nonfinite_json":
        raw = b'{"commitInfo":{"timestamp":NaN}}'
    with pytest.raises(iqr.IQRPublicationError):
        iqr._commit_info(raw, {})


@pytest.mark.unit
def test_epoch_and_latest_selection_have_no_naming_or_completion_tiebreak():
    assert iqr._commit_info(_log(_actions()), {}) == ("invented-table", 1234567890123)
    old = {"publication_id": "z-last-name", "commit_timestamp_ms": 10}
    new = {"publication_id": "a-first-name", "commit_timestamp_ms": 20, "iqr_accepted": False}
    old["iqr_accepted"] = True
    assert iqr._latest([new, old]) is new
    with pytest.raises(iqr.IQRPublicationError, match="tied"):
        iqr._latest([new, {**old, "commit_timestamp_ms": 20}])
    with pytest.raises(iqr.IQRPublicationError, match="No finalized"):
        iqr._latest([])


@pytest.mark.unit
def test_workspace_identifiers_method_and_local_writer_exclusion(tmp_path):
    raw = tmp_path / "raw"
    config = BronzeConfig(raw, tmp_path / "bronze", raw / "electricity.csv",
                          raw / "metadata.csv", "synthetic", "invented")
    for change in ({"method_version": "not-approved"}, {"publication_id": "../escape"},
                   {"silver_publication_id": "../escape"}):
        arguments = dict(silver_publication_id="silver", publication_id="iqr", workspace=tmp_path)
        arguments.update(change)
        with pytest.raises(iqr.IQRPublicationError):
            iqr.publish_synthetic_iqr(None, config, **arguments)
    with pytest.raises(silver.SilverPublicationError, match="synthetic"):
        iqr.publish_synthetic_iqr(None, replace(config, data_kind="real"), "silver", "iqr", tmp_path)
    with pytest.raises(silver.SilverPublicationError):
        overlap = tmp_path / "iqr-publications"
        iqr._workspace(tmp_path, replace(config, raw_root=overlap,
                                        electricity=overlap / "electricity.csv",
                                        metadata=overlap / "metadata.csv"))
    link = tmp_path / "link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(silver.SilverPublicationError):
        iqr._workspace(link, config)
    with iqr._lock(tmp_path):
        with pytest.raises(iqr.IQRPublicationError, match="active"):
            with iqr._lock(tmp_path):
                pytest.fail("A second writer acquired the namespace")
        with pytest.raises(iqr.IQRPublicationError, match="active"):
            iqr.publish_synthetic_iqr(None, config, "silver", "blocked", tmp_path)
    lock = tmp_path / "iqr-publications" / ".publication.lock"
    os.link(lock, tmp_path / "hard-link")
    with pytest.raises(silver.SilverPublicationError, match="Linked"):
        with iqr._lock(tmp_path):
            pytest.fail("Hardlinked lock was accepted")


@pytest.mark.integration
def test_actual_delta_replay_persisted_annotation_and_preservation(spark, published, monkeypatch):
    root, config, bronze, silver_result = published
    folder = root / "silver-publications" / "silver"
    before_silver = iqr._fingerprint(folder)
    before_bronze = {name: iqr._fingerprint(config.table_path(name)) for name in config.sources}
    source = spark.read.format("delta").option("versionAsOf", 0).load(silver_result["table_path"])
    source_rows = source.collect()
    original = Counter(map(tuple, source_rows))
    assert any(row.timestamp_original is None for row in source_rows)
    assert any(row.electricity_value_original is None for row in source_rows)
    assert max(original.values()) > 1
    expected = {r.building_id: r.asDict() for r in summarize_electricity_iqr(source).collect()}
    actual_save = DataFrameWriter.save
    writes = []

    def guard(writer, path=None, *args, **kwargs):
        assert path is not None and str(root / "iqr-publications") in str(path)
        writes.append(str(path))
        return actual_save(writer, path, *args, **kwargs)

    monkeypatch.setattr(DataFrameWriter, "save", guard)
    first = iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    assert first["output_rows"] == 16 and first["delta_version"] == 0
    assert first["intent"]["method_version"] == IQR_METHOD
    assert first["intent"]["silver"]["delta_table_id"] == silver_result["delta_table_id"]
    assert first["intent"]["silver"]["source_run_id"] == bronze["run_id"]
    assert first["commit_timestamp_ms"] > 0
    same = iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    assert same == first and len(writes) == 1
    assert DeltaTable.forPath(spark, first["table_path"]).history().count() == 1
    state, reread, limits = iqr.read_synthetic_iqr_pair(spark, config, root)
    assert state["iqr_status"] == "not_accepted" and state["warnings"] == ["iqr_not_accepted"]
    assert len(state["unaccepted_buildings"]) == 16
    rows = limits.collect()
    for row in rows:
        assert row._silver_publication_id == "silver" and row._iqr_publication_id == "limits"
        assert {name: row[name] for name in expected[row.building_id]} == expected[row.building_id]
    first_building = next(row for row in rows if row.building_id == APPROVED_BUILDINGS[0])
    assert first_building.iqr_zero_spread and first_building.iqr_outlier_count == 1
    assert first_building.iqr_outlier_percentage == 20 and first_building.off_grid_observation_count == 1
    assert first_building.valid_observation_count > first_building.covered_nominal_hours
    missing = next(row for row in rows if row.building_id == APPROVED_BUILDINGS[-1])
    assert missing.q1 is None and missing.iqr_outlier_count is None
    _, view = iqr.annotate_synthetic_iqr_pair(spark, config, root)
    annotated = view.collect()
    assert view.schema.fields[:len(source.schema)] == source.schema.fields
    assert Counter(tuple(row[:len(source.columns)]) for row in annotated) == original
    assert Counter(map(tuple, reread.collect())) == original
    assert sum(row.is_iqr_outlier is True for row in annotated) > 0
    assert any(row.is_iqr_outlier is None for row in annotated)
    assert {row.iqr_evaluation_status for row in annotated} >= {
        "numeric_not_eligible", "invalid_nominal_timestamp", "outside_study_period", "evaluated",
    }
    assert iqr._fingerprint(folder) == before_silver
    assert {name: iqr._fingerprint(config.table_path(name)) for name in config.sources} == before_bronze
    first["intent"]["parameters"]["multiplier"] = "999"
    first["intent"]["parameters"]["percentiles"].append("0.5")
    assert iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root) == same
    intent_file = root / "iqr-publications" / "limits" / "intent.json"
    original_intent = intent_file.read_bytes()
    for parent in ({}, {**same["intent"]["silver"], "publication_id": "foreign"}):
        altered = deepcopy(same["intent"])
        altered["silver"] = parent
        intent_file.write_text(json.dumps(altered))
        with pytest.raises(iqr.IQRPublicationError, match="identity|intent"):
            iqr.read_synthetic_iqr_pair(spark, config, root)
    intent_file.write_bytes(original_intent)


@pytest.mark.integration
@pytest.mark.parametrize("stage", ["before_reservation", "after_reservation", "before_delta",
                                   "during_delta", "after_commit", "verification",
                                   "before_final", "after_final"])
def test_injected_failures_reconcile_or_block_without_overwrite(spark, published, monkeypatch, stage):
    root, config, _, _ = published
    original_json, original_save = silver._exclusive_json, DataFrameWriter.save

    def interrupted(*args, **kwargs):
        raise OSError("invented interruption")

    def json_failure(path, value):
        if stage == "after_reservation" and path.name == "intent.json":
            original_json(path, value)
            interrupted()
        if stage == "before_final" and path.name == "completed.json":
            interrupted()
        original_json(path, value)
        if stage == "after_final" and path.name == "completed.json":
            interrupted()

    def save_failure(writer, path=None, *args, **kwargs):
        if stage == "during_delta":
            target = Path(path)
            target.mkdir()
            (target / "partial").write_text("invented partial write")
            interrupted()
        if stage == "before_delta":
            interrupted()
        result = original_save(writer, path, *args, **kwargs)
        if stage == "after_commit":
            interrupted()
        return result

    with monkeypatch.context() as fault:
        fault.setattr(silver, "_exclusive_json", json_failure)
        fault.setattr(DataFrameWriter, "save", save_failure)
        if stage == "before_reservation":
            fault.setattr(iqr, "_intent", interrupted)
        if stage == "verification":
            fault.setattr(iqr, "_verify_output", interrupted)
        with pytest.raises(OSError, match="interruption"):
            iqr.publish_synthetic_iqr(spark, config, "silver", "fault", root)
    folder = root / "iqr-publications" / "fault"
    if stage == "before_final":
        with monkeypatch.context() as guard:
            guard.setattr(iqr, "summarize_electricity_iqr",
                          lambda *_: pytest.fail("Pending reading must not run IQR"))
            state, _, limits = iqr.read_synthetic_iqr_pair(spark, config, root)
            assert state["iqr_status"] == "pending" and limits is None
            with pytest.raises(iqr.IQRPublicationError, match="unavailable"):
                iqr.read_synthetic_iqr_pair(spark, config, root,
                                            silver_publication_id="silver", iqr_publication_id="fault")
    if stage == "during_delta":
        before = iqr._fingerprint(folder)
        with pytest.raises((OSError, silver.SilverPublicationError)):
            iqr.publish_synthetic_iqr(spark, config, "silver", "fault", root)
        assert iqr._fingerprint(folder) == before
    else:
        result = iqr.publish_synthetic_iqr(spark, config, "silver", "fault", root)
        assert result["output_rows"] == 16
        assert DeltaTable.forPath(spark, result["table_path"]).history().count() == 1


@pytest.mark.integration
def test_conflicts_corruption_unknown_destinations_and_source_mutation(spark, published):
    root, config, _, result = published
    completed = iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    folder = root / "iqr-publications" / "limits"
    marker = folder / "completed.json"
    good_bytes = marker.read_bytes()
    before = iqr._fingerprint(folder)
    with pytest.raises(iqr.IQRPublicationError, match="method"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root, method_version="other")
    assert iqr._fingerprint(folder) == before
    marker.write_text("{")
    with pytest.raises(iqr.IQRPublicationError):
        iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    marker.write_bytes(good_bytes)
    intent_file = folder / "intent.json"
    intent_bytes = intent_file.read_bytes()
    intent = json.loads(intent_bytes)
    intent["parameters"]["multiplier"] = "2"
    intent_file.write_text(json.dumps(intent))
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    intent_file.write_bytes(intent_bytes)
    unknown = root / "iqr-publications" / "unknown"
    unknown.mkdir()
    other_intent = deepcopy(completed["intent"])
    other_intent["publication_id"] = "unknown"
    (unknown / "intent.json").write_text(json.dumps(other_intent))
    (unknown / "foreign").write_text("preserve")
    with pytest.raises(iqr.IQRPublicationError, match="Unknown|unavailable"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "unknown", root)
    assert (unknown / "foreign").read_text() == "preserve"
    missing = root / "iqr-publications" / "missing_intent" / "silver_iqr_limits"
    missing.mkdir(parents=True)
    (missing / "partial").write_text("preserve partial evidence")
    with pytest.raises(iqr.IQRPublicationError, match="unavailable"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "missing_intent", root)
    assert (missing / "partial").read_text() == "preserve partial evidence"
    limits = spark.read.format("delta").load(completed["table_path"])
    limits.limit(1).write.format("delta").mode("append").save(completed["table_path"])
    with pytest.raises(iqr.IQRPublicationError, match="history"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    source = spark.read.format("delta").load(result["table_path"])
    source.limit(1).write.format("delta").mode("append").save(result["table_path"])
    with pytest.raises(silver.SilverPublicationError, match="history"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "limits", root)
    assert json.loads(marker.read_text()) == completed


@pytest.mark.integration
@pytest.mark.parametrize("case", ["missing", "incomplete", "bad_json", "wrong_id", "wrong_version",
                                  "wrong_schema", "wrong_count", "missing_bronze"])
def test_silver_identity_verified_before_iqr_reservation(spark, published, case):
    root, config, bronze, result = published
    marker = root / "silver-publications" / "silver" / "completed.json"
    chosen = "silver"
    if case == "missing":
        chosen = "absent"
    elif case == "incomplete":
        marker.unlink()
    elif case == "bad_json":
        marker.write_text("{")
    elif case == "missing_bronze":
        path = config.namespace / "_runs" / (bronze["run_id"] + ".json")
        report = json.loads(path.read_text())
        del report["tables"][0]["source_sha256"]
        path.write_text(json.dumps(report))
    else:
        value = dict(result)
        key, replacement = {"wrong_id": ("delta_table_id", "wrong"),
                            "wrong_version": ("delta_version", 1),
                            "wrong_schema": ("schema", {}), "wrong_count": ("output_rows", 1)}[case]
        value[key] = replacement
        marker.write_text(json.dumps(value))
    with pytest.raises((silver.SilverPublicationError, SnapshotError)):
        iqr.publish_synthetic_iqr(spark, config, chosen, "blocked", root)
    assert not (root / "iqr-publications" / "blocked").exists()


@pytest.mark.integration
def test_forged_silver_with_correct_count_and_missing_building_is_rejected(spark, published):
    root, config, _, result = published
    original_folder = root / "silver-publications" / "silver"
    before = iqr._fingerprint(original_folder)
    source = spark.read.format("delta").option("versionAsOf", 0).load(result["table_path"])
    # Keep count and schema while replacing one approved building with duplicates.
    forged = source.where(F.col("building_id") != APPROVED_BUILDINGS[-1]).unionByName(
        source.where(F.col("building_id") == APPROVED_BUILDINGS[0]),
    )
    folder = root / "silver-publications" / "forged"
    folder.mkdir()
    intent = json.loads((original_folder / "intent.json").read_text())
    intent["publication_id"] = "forged"
    target = folder / "table"
    (forged.write.format("delta").mode("errorifexists")
     .option("userMetadata", iqr._canonical(intent)).save(str(target)))
    table_id = DeltaTable.forPath(spark, str(target)).detail().select("id").first().id
    completed = {**result, "publication_id": "forged", "table_path": str(target),
                 "delta_table_id": table_id}
    silver._exclusive_json(folder / "intent.json", intent)
    silver._exclusive_json(folder / "completed.json", completed)
    with pytest.raises(silver.SilverPublicationError, match="rows differ"):
        iqr.publish_synthetic_iqr(spark, config, "forged", "blocked", root)
    assert not (root / "iqr-publications" / "blocked").exists()
    assert iqr._fingerprint(original_folder) == before


@pytest.mark.integration
def test_commit_recency_recovery_history_pending_and_latest_unaccepted(spark, tmp_path):
    config, bronze_a, _ = _create_silver(spark, tmp_path, "z_older", interrupt=True)
    folder_a = tmp_path / "silver-publications" / "z_older"
    log_a = folder_a / "table" / "_delta_log" / "00000000000000000000.json"
    original_commit = log_a.read_bytes()
    _, _, newer = _create_silver(spark, tmp_path, "a_newer")
    recovered = silver.publish_synthetic_silver(spark, config, bronze_a["run_id"], "z_older", tmp_path)
    assert log_a.read_bytes() == original_commit
    old_limits = iqr.publish_synthetic_iqr(spark, config, "z_older", "older_limits", tmp_path)
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        iqr.publish_synthetic_iqr(spark, config, "a_newer", "older_limits", tmp_path)
    os.utime(log_a, (2000000000, 2000000000))
    native_time = (DeltaTable.forPath(spark, str(folder_a / "table")).history()
                   .select(F.unix_millis("timestamp").alias("epoch_ms")).first().epoch_ms)
    assert native_time == 2000000000000
    state, _, limits = iqr.read_synthetic_iqr_pair(spark, config, tmp_path)
    assert state["silver"]["publication_id"] == "a_newer"
    assert state["silver"]["delta_table_id"] == newer["delta_table_id"]
    assert state["iqr_status"] == "pending" and limits is None
    with pytest.raises(iqr.IQRPublicationError, match="pending"):
        iqr.annotate_synthetic_iqr_pair(spark, config, tmp_path)
    historical, _, _ = iqr.read_synthetic_iqr_pair(
        spark, config, tmp_path, silver_publication_id="z_older", iqr_publication_id="older_limits",
    )
    assert historical["silver"]["delta_table_id"] == recovered["delta_table_id"]
    assert historical["iqr"]["delta_table_id"] == old_limits["delta_table_id"]
    with pytest.raises(iqr.IQRPublicationError, match="different"):
        iqr.read_synthetic_iqr_pair(spark, config, tmp_path,
                                    silver_publication_id="a_newer", iqr_publication_id="older_limits")
    original_finalizer = silver._exclusive_json

    def interrupt_old_limits(path, value):
        if path.parent.name == "z_limits" and path.name == "completed.json":
            raise OSError("invented older IQR interruption")
        return original_finalizer(path, value)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(silver, "_exclusive_json", interrupt_old_limits)
        with pytest.raises(OSError, match="interruption"):
            iqr.publish_synthetic_iqr(spark, config, "a_newer", "z_limits", tmp_path)
    old_iqr_log = (tmp_path / "iqr-publications" / "z_limits" / "silver_iqr_limits"
                   / "_delta_log" / "00000000000000000000.json")
    before_recovery = old_iqr_log.read_bytes()
    latest = iqr.publish_synthetic_iqr(spark, config, "a_newer", "a_limits", tmp_path)
    first = iqr.publish_synthetic_iqr(spark, config, "a_newer", "z_limits", tmp_path)
    assert old_iqr_log.read_bytes() == before_recovery
    assert latest["commit_timestamp_ms"] > first["commit_timestamp_ms"]
    state, _, _ = iqr.read_synthetic_iqr_pair(spark, config, tmp_path)
    assert state["iqr"]["intent"]["publication_id"] == "a_limits"
    assert state["iqr_status"] == "not_accepted" and state["warnings"]
    assert state["silver"]["commit_timestamp_ms"] > historical["silver"]["commit_timestamp_ms"]


@pytest.mark.integration
def test_source_changes_during_calculation_block_finalization(spark, published, monkeypatch):
    root, config, _, _ = published
    original = iqr._verify_output

    def changed(*args):
        result = original(*args)
        (root / "silver-publications" / "silver" / "external_change").write_text("invented change")
        return result

    monkeypatch.setattr(iqr, "_verify_output", changed)
    with pytest.raises(iqr.IQRPublicationError, match="changed"):
        iqr.publish_synthetic_iqr(spark, config, "silver", "changing", root)
    folder = root / "iqr-publications" / "changing"
    assert (folder / "silver_iqr_limits" / "_delta_log").is_dir()
    assert not (folder / "completed.json").exists()

