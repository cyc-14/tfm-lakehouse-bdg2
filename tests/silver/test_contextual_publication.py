"""Synthetic contextual Delta publication and explicit recovery."""
import pytest

from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from test_silver_iqr_publication import _create_silver


@pytest.mark.integration
def test_publication_recovery_and_replay(spark, tmp_path, monkeypatch):
    config, _, _ = _create_silver(spark, tmp_path)
    bronze_before = iqr._fingerprint(config.lakehouse_root)
    silver_before = iqr._fingerprint(tmp_path / "silver-publications")
    original = silver._exclusive_json

    def interrupt(path, value):
        if path.name == "completed.json" and "contextual-publications" in str(path):
            raise OSError("marker interruption")
        return original(path, value)

    with monkeypatch.context() as patch:
        patch.setattr(silver, "_exclusive_json", interrupt)
        with pytest.raises(OSError, match="marker interruption"):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    folder = tmp_path / "contextual-publications" / "context"
    assert (folder / "table" / "_delta_log" / "00000000000000000000.json").is_file()
    result = publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert result["output_rows"] == 128
    assert result["delta_version"] == 0
    assert publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path) == result
    assert len(list((folder / "table" / "_delta_log").glob("*.json"))) == 1
    assert iqr._fingerprint(config.lakehouse_root) == bronze_before
    assert iqr._fingerprint(tmp_path / "silver-publications") == silver_before
    assert not (tmp_path / "iqr-publications").exists()
    with pytest.raises(iqr.IQRPublicationError, match="Unsupported enrichment"):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path,
                                                 enrichment_version="different")


@pytest.mark.unit
def test_rejects_real_config_before_publication(tmp_path):
    from tfm_lakehouse.config import BronzeConfig
    raw = tmp_path / "raw"
    config = BronzeConfig(raw, tmp_path / "lake", raw / "electricity.csv",
                          raw / "metadata.csv", "real", "bdg2")
    with pytest.raises(silver.SilverPublicationError, match="synthetic"):
        publication.publish_synthetic_contextual(None, config, "silver", "context", tmp_path)


@pytest.mark.unit
def test_cooperative_lock_rejects_concurrent_writer(tmp_path):
    with publication._lock(tmp_path):
        with pytest.raises(iqr.IQRPublicationError, match="active"):
            with publication._lock(tmp_path):
                pass


@pytest.mark.integration
def test_reservation_and_partial_table_recovery(spark, tmp_path, monkeypatch):
    from pyspark.sql.readwriter import DataFrameWriter

    config, bronze, _ = _create_silver(spark, tmp_path)
    original = DataFrameWriter.save
    folder = tmp_path / "contextual-publications" / "context"

    def before_commit(writer, path=None, *args, **kwargs):
        if path == str(folder / "table"):
            raise OSError("before commit")
        return original(writer, path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(DataFrameWriter, "save", before_commit)
        with pytest.raises(OSError, match="before commit"):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert (folder / "intent.json").is_file()
    assert not (folder / "table").exists()

    manifest = config.namespace / "_runs" / (bronze["run_id"] + ".json")
    original_manifest = manifest.read_bytes()
    try:
        manifest.write_bytes(original_manifest + b" ")
        with pytest.raises(silver.SilverPublicationError):
            publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    finally:
        manifest.write_bytes(original_manifest)

    # An incomplete Delta destination is uncertain evidence and must survive.
    (folder / "table").mkdir()
    with pytest.raises((OSError, silver.SilverPublicationError)):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert (folder / "table").is_dir()


@pytest.mark.integration
def test_conflicting_and_malformed_reservations(spark, tmp_path):
    config, _, _ = _create_silver(spark, tmp_path)
    folder = tmp_path / "contextual-publications" / "context"
    folder.mkdir(parents=True)
    intent = folder / "intent.json"
    intent.write_text('{"contract_version":1}')
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    intent.write_text('{')
    with pytest.raises(iqr.IQRPublicationError, match="evidence"):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
