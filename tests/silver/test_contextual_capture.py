"""Focused synthetic compatibility checks for durable contextual captures."""
import pytest

from tfm_lakehouse.silver import contextual_capture as capture
from tfm_lakehouse.silver import contextual_identity as identity
from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import contextual_reader as reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from test_silver_iqr_publication import _create_silver


@pytest.mark.integration
def test_capture_preserves_v2_and_historical_reader_after_retirement(spark, tmp_path):
    config, _, _ = _create_silver(spark, tmp_path)
    original = identity.build_synthetic_contextual_intent(spark, config, 'silver', 'context', tmp_path)
    result = publication.publish_synthetic_contextual(
        spark, config, 'silver', 'context', tmp_path, capture_id='explicit', owner_id='owner')
    assert iqr._canonical(result['intent']) == iqr._canonical(original)
    folder = tmp_path / 'contextual-captures' / 'explicit'
    marker = iqr._read(folder / 'completed.json')
    assert marker['delta_version'] == 0 and marker['delta_table_id']
    assert marker['physical_tree_sha256'] == iqr._fingerprint(folder / 'table')
    checked, frame = capture.verify(spark, config, tmp_path, folder, 'explicit', 'owner', original)
    assert checked == marker
    assert identity._multiset_hash(frame) == original['context_multiset_sha256']
    held = tmp_path / 'author-retired-capture'
    folder.rename(held)
    evidence, historical = reader.read_synthetic_contextual(spark, config, 'context', tmp_path)
    assert evidence == result and historical.count() == result['output_rows']
    assert held.is_dir()


@pytest.mark.unit
def test_partial_capture_ids_rejected_before_workspace_inspection(monkeypatch):
    monkeypatch.setattr(publication, '_workspace', lambda *_: pytest.fail('workspace inspected'))
    with pytest.raises(iqr.IQRPublicationError, match='paired'):
        publication.publish_synthetic_contextual(None, None, 'silver', 'context', None, capture_id='id')


@pytest.mark.unit
def test_capture_lock_rejects_cooperating_writer(tmp_path):
    from tfm_lakehouse.silver import weather_capture
    with weather_capture._lock(tmp_path):
        with pytest.raises(iqr.IQRPublicationError, match='active'):
            with weather_capture._lock(tmp_path):
                pass
