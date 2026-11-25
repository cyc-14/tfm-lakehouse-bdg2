"""Synthetic historical contextual reads and fail-closed evidence checks."""
import json

import pytest

from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import contextual_reader as reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from test_silver_iqr_publication import _create_silver


@pytest.mark.unit
def test_reader_rejects_invalid_identifier_and_missing_lock_without_writes(tmp_path):
    from tfm_lakehouse.config import BronzeConfig
    raw = tmp_path / "raw"
    config = BronzeConfig(raw, tmp_path / "bronze", raw / "electricity.csv",
                          raw / "metadata.csv", "synthetic", "invented")
    with pytest.raises(iqr.IQRPublicationError, match="identifier"):
        reader.read_synthetic_contextual(None, config, "../invalid", tmp_path)
    with pytest.raises(iqr.IQRPublicationError, match="lock is unavailable"):
        with reader._read_lock(tmp_path):
            pass
    assert not (tmp_path / "contextual-publications").exists()


@pytest.mark.unit
def test_reader_rejects_real_configuration(tmp_path):
    from tfm_lakehouse.config import BronzeConfig
    raw = tmp_path / "raw"
    config = BronzeConfig(raw, tmp_path / "bronze", raw / "electricity.csv",
                          raw / "metadata.csv", "real", "invented")
    with pytest.raises(silver.SilverPublicationError, match="synthetic"):
        reader.read_synthetic_contextual(None, config, "context", tmp_path)


@pytest.mark.unit
def test_reader_rejects_cooperative_writer_without_creating_evidence(tmp_path):
    with publication._lock(tmp_path):
        namespace = tmp_path / "contextual-publications"
        before = iqr._fingerprint(namespace)
        with pytest.raises(iqr.IQRPublicationError, match="active"):
            with reader._read_lock(tmp_path):
                pass
        assert iqr._fingerprint(namespace) == before


@pytest.mark.integration
def test_finalized_explicit_historical_read_and_no_publication_writes(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    first = publication.publish_synthetic_contextual(spark, config, "silver", "first", tmp_path)
    publication.publish_synthetic_contextual(spark, config, "silver", "newer", tmp_path)
    before = {"context": iqr._fingerprint(tmp_path / "contextual-publications"),
              "silver": iqr._fingerprint(tmp_path / "silver-publications"),
              "bronze": iqr._fingerprint(config.lakehouse_root)}

    def forbidden(*args, **kwargs):
        pytest.fail("Reader must not publish, recover or calculate IQR")

    monkeypatch.setattr(publication, "publish_synthetic_contextual", forbidden)
    monkeypatch.setattr(iqr, "summarize_electricity_iqr", forbidden)
    monkeypatch.setattr(iqr, "read_synthetic_iqr_pair", forbidden)
    evidence, frame = reader.read_synthetic_contextual(spark, config, "first", tmp_path)
    assert evidence == first
    assert frame.count() == first["output_rows"] == 128
    assert evidence["delta_version"] == 0
    assert {"context": iqr._fingerprint(tmp_path / "contextual-publications"),
            "silver": iqr._fingerprint(tmp_path / "silver-publications"),
            "bronze": iqr._fingerprint(config.lakehouse_root)} == before


@pytest.mark.integration
def test_reader_rejects_incomplete_and_incompatible_evidence_without_writes(spark, tmp_path):
    config, _, _ = _create_silver(spark, tmp_path)
    publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    namespace = tmp_path / "contextual-publications"
    folder = namespace / "context"
    before = iqr._fingerprint(folder)
    with pytest.raises(iqr.IQRPublicationError, match="unavailable"):
        reader.read_synthetic_contextual(spark, config, "missing", tmp_path)
    assert iqr._fingerprint(folder) == before
    intent_path = folder / "intent.json"
    original = intent_path.read_bytes()
    for change in (lambda x: x.update(contract_version=1),
                   lambda x: x.update(contract_version=2.0)):
        value = json.loads(original)
        change(value)
        intent_path.write_text(json.dumps(value))
        changed = iqr._fingerprint(folder)
        with pytest.raises(iqr.IQRPublicationError, match="version"):
            reader.read_synthetic_contextual(spark, config, "context", tmp_path)
        assert iqr._fingerprint(folder) == changed
    intent_path.write_bytes(original)
    marker = folder / "completed.json"
    marker_bytes = marker.read_bytes()
    marker.write_text("{")
    changed = iqr._fingerprint(folder)
    with pytest.raises(iqr.IQRPublicationError, match="evidence"):
        reader.read_synthetic_contextual(spark, config, "context", tmp_path)
    assert iqr._fingerprint(folder) == changed
    marker.write_bytes(marker_bytes)
    retained = config.table_path("metadata") / "synthetic-extra"
    retained.write_text("changed")
    before = iqr._fingerprint(folder)
    with pytest.raises(iqr.IQRPublicationError, match="differs"):
        reader.read_synthetic_contextual(spark, config, "context", tmp_path)
    assert iqr._fingerprint(folder) == before
