"""Timezone and existing accepted IQR scenarios for contextual reading."""
from datetime import datetime, timedelta
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.silver import contextual_identity as identity
from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import contextual_reader as reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from test_silver_iqr_publication import _create_silver


@pytest.mark.integration
def test_reader_rejects_changed_timezone_resource(spark, tmp_path, monkeypatch):
    first, second = Path("/usr/share/zoneinfo/UTC"), Path("/usr/share/zoneinfo/Etc/GMT+1")
    if not first.is_file() or not second.is_file():
        pytest.skip("Synthetic timezone resources unavailable")
    config, _, _ = _create_silver(spark, tmp_path)
    config.metadata.write_text("building_id,timezone\n" +
                               "\n".join(f"{name},UTC" for name in APPROVED_BUILDINGS) + "\n")
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "timezone_source", tmp_path)
    resource = tmp_path / "timezone-resource"
    resource.write_bytes(first.read_bytes())

    def controlled(name):
        if name is None:
            return "null", None, None
        data = resource.read_bytes()
        ZoneInfo.from_file(BytesIO(data), key=name)
        return "valid", "file:" + str(resource), sha256(data).hexdigest()

    monkeypatch.setattr(identity, "_controlled_zone", controlled)
    publication.publish_synthetic_contextual(spark, config, "timezone_source", "context", tmp_path)
    namespace = tmp_path / "contextual-publications"
    resource.write_bytes(second.read_bytes())
    before = iqr._fingerprint(namespace)
    with pytest.raises(iqr.IQRPublicationError, match="differs"):
        reader.read_synthetic_contextual(spark, config, "context", tmp_path)
    assert iqr._fingerprint(namespace) == before


@pytest.mark.integration
def test_reader_rejects_complete_legacy_v1_without_repair(spark, tmp_path):
    config, _, _ = _create_silver(spark, tmp_path)
    intent = identity.build_synthetic_contextual_intent(spark, config, "silver", "context", tmp_path)
    intent["contract_version"] = 1
    intent.pop("metadata_files_sha256")
    with publication._lock(tmp_path) as namespace:
        folder = namespace / "context"
        folder.mkdir()
        silver._exclusive_json(folder / "intent.json", intent)
        _, expected = identity.enrich_synthetic_metadata(spark, config, "silver", tmp_path)
        (expected.write.format("delta").mode("errorifexists")
         .option("userMetadata", iqr._canonical(intent)).save(str(folder / "table")))
        result = publication._verify_output(spark, folder / "table", expected, intent)
        silver._exclusive_json(folder / "completed.json", result)
    before = iqr._fingerprint(namespace)
    with pytest.raises(iqr.IQRPublicationError, match="version"):
        reader.read_synthetic_contextual(spark, config, "context", tmp_path)
    assert iqr._fingerprint(namespace) == before


@pytest.mark.integration
def test_reader_does_not_consult_existing_accepted_iqr(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    start = datetime(2016, 1, 1)
    rows = [(start + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S") + "," + ",".join(["1"] * 16)
            for i in range(16667)]
    config.electricity.write_text("timestamp," + ",".join(APPROVED_BUILDINGS) + "\n"
                                  + "\n".join(rows) + "\n")
    bronze = run_bronze(spark, config)
    silver.publish_synthetic_silver(spark, config, bronze["run_id"], "large", tmp_path)
    iqr.publish_synthetic_iqr(spark, config, "large", "accepted", tmp_path)
    state, _, _ = iqr.read_synthetic_iqr_pair(spark, config, tmp_path,
                                              silver_publication_id="large", iqr_publication_id="accepted")
    assert state["iqr_status"] == "available"
    publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    iqr_before = iqr._fingerprint(tmp_path / "iqr-publications")
    namespace = tmp_path / "contextual-publications"
    before = iqr._fingerprint(namespace)
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", lambda *a, **k: pytest.fail("IQR read"))
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", lambda *a, **k: pytest.fail("IQR recalculation"))
    result, frame = reader.read_synthetic_contextual(spark, config, "context", tmp_path)
    assert result["output_rows"] == frame.count() == 128
    assert iqr._fingerprint(namespace) == before
    assert iqr._fingerprint(tmp_path / "iqr-publications") == iqr_before
