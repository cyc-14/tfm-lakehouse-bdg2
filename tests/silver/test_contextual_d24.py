"""Synthetic tests for the versioned physical metadata evidence contract."""
import json

import pytest
from pyspark.sql.readwriter import DataFrameWriter

from tfm_lakehouse.silver import contextual_identity as identity
from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from test_silver_iqr_publication import _create_silver


@pytest.mark.integration
@pytest.mark.parametrize("completed", [False, True])
def test_legacy_intent_is_rejected_without_modification(spark, tmp_path, completed):
    config, _, _ = _create_silver(spark, tmp_path)
    intent = identity.build_synthetic_contextual_intent(spark, config, "silver", "context", tmp_path)
    assert intent["contract_version"] == 2
    assert intent["metadata_files_sha256"] == iqr._fingerprint(config.table_path("metadata"))
    legacy = dict(intent)
    legacy["contract_version"] = 1
    legacy.pop("metadata_files_sha256")
    folder = tmp_path / "contextual-publications" / "context"
    folder.mkdir(parents=True)
    (folder / "intent.json").write_text(json.dumps(legacy))
    if completed:
        _, frame = identity.enrich_synthetic_metadata(spark, config, "silver", tmp_path)
        (frame.write.format("delta").mode("errorifexists")
         .option("userMetadata", iqr._canonical(legacy)).save(str(folder / "table")))
        result = publication._verify_output(spark, folder / "table", frame, legacy)
        silver._exclusive_json(folder / "completed.json", result)
    before = iqr._fingerprint(folder)
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert iqr._fingerprint(folder) == before
    with pytest.raises(iqr.IQRPublicationError, match="differs"):
        identity.verify_synthetic_contextual_intent(spark, config, "silver", "context", tmp_path, legacy)


@pytest.mark.integration
@pytest.mark.parametrize("change", ["remove", "modify"])
def test_metadata_file_change_after_reservation_is_rejected(spark, tmp_path, monkeypatch, change):
    config, _, _ = _create_silver(spark, tmp_path)
    evidence = config.table_path("metadata") / "synthetic-evidence"
    evidence.write_text("original")
    original_save = DataFrameWriter.save

    def stop(writer, path=None, *args, **kwargs):
        if "contextual-publications" in str(path):
            raise OSError("reserved")
        return original_save(writer, path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(DataFrameWriter, "save", stop)
        with pytest.raises(OSError, match="reserved"):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    folder = tmp_path / "contextual-publications" / "context"
    before = iqr._fingerprint(folder)
    if change == "remove":
        evidence.unlink()
    else:
        evidence.write_text("changed")
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert iqr._fingerprint(folder) == before


@pytest.mark.integration
def test_metadata_change_during_materialization_is_rejected(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    original = identity.enrich_synthetic_metadata
    calls = 0

    def change_second_view(*args, **kwargs):
        nonlocal calls
        diagnostic, frame = original(*args, **kwargs)
        calls += 1
        if calls == 2:
            (config.table_path("metadata") / "synthetic-evidence").write_text("changed")
        return diagnostic, frame

    monkeypatch.setattr(identity, "enrich_synthetic_metadata", change_second_view)
    with pytest.raises(iqr.IQRPublicationError, match="during materialization"):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert not (tmp_path / "contextual-publications" / "context").exists()


@pytest.mark.integration
def test_metadata_change_after_commit_blocks_finalization_and_can_reconcile(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    original = publication._verify_output
    evidence = config.table_path("metadata") / "synthetic-evidence"

    def change_after_verification(*args, **kwargs):
        result = original(*args, **kwargs)
        evidence.write_text("changed")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(publication, "_verify_output", change_after_verification)
        with pytest.raises(iqr.IQRPublicationError, match="differs"):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    folder = tmp_path / "contextual-publications" / "context"
    log = folder / "table" / "_delta_log" / "00000000000000000000.json"
    before = log.read_bytes()
    assert not (folder / "completed.json").exists()
    evidence.unlink()
    result = publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert result["delta_version"] == 0 and log.read_bytes() == before
    assert len(list(log.parent.glob("*.json"))) == 1


@pytest.mark.integration
def test_metadata_change_after_marker_is_rejected_on_replay(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    original = silver._exclusive_json
    evidence = config.table_path("metadata") / "synthetic-evidence"

    def change_after_marker(path, value):
        original(path, value)
        if path.name == "completed.json" and "contextual-publications" in str(path):
            evidence.write_text("changed")

    with monkeypatch.context() as patch:
        patch.setattr(silver, "_exclusive_json", change_after_marker)
        with pytest.raises(iqr.IQRPublicationError, match="differs"):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    folder = tmp_path / "contextual-publications" / "context"
    before = iqr._fingerprint(folder)
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert iqr._fingerprint(folder) == before
    evidence.unlink()
    result = publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert result["delta_version"] == 0 and iqr._fingerprint(folder) == before
