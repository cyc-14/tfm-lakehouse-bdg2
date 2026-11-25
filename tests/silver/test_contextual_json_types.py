"""Reject JSON evidence whose types differ despite Python numeric equality."""
import json
from types import SimpleNamespace

import pytest

from tfm_lakehouse.silver import contextual_identity as identity
from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from test_silver_iqr_publication import _create_silver


@pytest.mark.unit
def test_intent_verifier_rejects_json_type_substitution(monkeypatch):
    monkeypatch.setattr(identity, "build_synthetic_contextual_intent",
                        lambda *args, **kwargs: {"contract_version": 2, "version": 0})
    with pytest.raises(iqr.IQRPublicationError, match="differs"):
        identity.verify_synthetic_contextual_intent(None, None, "silver", "context", None,
                                                    {"contract_version": 2.0, "version": False})


@pytest.mark.unit
def test_committed_schema_metadata_types_are_exact(monkeypatch, tmp_path):
    expected_schema = {"type": "struct", "fields": [
        {"name": "value", "type": "string", "nullable": False, "metadata": {"tag": 0}}]}
    actual_schema = {"type": "struct", "fields": [
        {"name": "value", "type": "string", "nullable": True, "metadata": {"tag": False}}]}
    expected = SimpleNamespace(schema=SimpleNamespace(jsonValue=lambda: expected_schema))
    actual = SimpleNamespace(schema=SimpleNamespace(jsonValue=lambda: actual_schema))

    class Reader:
        def format(self, _value):
            return self

        def option(self, *_args):
            return self

        def load(self, _path):
            return actual

    monkeypatch.setattr(iqr, "_commit", lambda *_args: {})
    with pytest.raises(iqr.IQRPublicationError, match="schema differs"):
        publication._verify_output(SimpleNamespace(read=Reader()), tmp_path, expected, {})


@pytest.mark.integration
def test_typed_reservation_and_marker_are_rejected_without_writes(spark, tmp_path):
    config, _, _ = _create_silver(spark, tmp_path)
    result = publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    folder = tmp_path / "contextual-publications" / "context"
    marker = folder / "completed.json"
    valid_marker = marker.read_bytes()
    altered_marker = json.loads(valid_marker)
    assert altered_marker["delta_version"] == 0
    altered_marker["delta_version"] = False
    marker.write_text(json.dumps(altered_marker))
    before = iqr._fingerprint(folder)
    with pytest.raises(iqr.IQRPublicationError, match="completion"):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert iqr._fingerprint(folder) == before
    marker.write_bytes(valid_marker)

    intent_path = folder / "intent.json"
    valid_intent = intent_path.read_bytes()
    altered_intent = json.loads(valid_intent)
    altered_intent["contract_version"] = 2.0
    intent_path.write_text(json.dumps(altered_intent))
    before = iqr._fingerprint(folder)
    with pytest.raises(iqr.IQRPublicationError, match="conflicts"):
        publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    assert iqr._fingerprint(folder) == before
    intent_path.write_bytes(valid_intent)
    assert result["delta_version"] == 0
