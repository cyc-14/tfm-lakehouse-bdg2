"""Changes after commit verification must prevent successful reads."""
import pytest

from tfm_lakehouse.silver import contextual_publication as publication
from tfm_lakehouse.silver import contextual_reader as reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from test_silver_iqr_publication import _create_silver


@pytest.mark.integration
@pytest.mark.parametrize("kind", ["metadata", "marker"])
def test_reader_detects_evidence_change_during_verification(spark, tmp_path, monkeypatch, kind):
    config, _, _ = _create_silver(spark, tmp_path)
    publication.publish_synthetic_contextual(spark, config, "silver", "context", tmp_path)
    namespace = tmp_path / "contextual-publications"
    original = publication._verify_output
    snapshot = []

    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        if kind == "metadata":
            (config.table_path("metadata") / "synthetic-extra").write_text("changed")
        else:
            (namespace / "context" / "completed.json").write_text("{}")
        snapshot.append(iqr._fingerprint(namespace))
        return result

    monkeypatch.setattr(publication, "_verify_output", mutate)
    with pytest.raises(iqr.IQRPublicationError, match="differs|changed"):
        reader.read_synthetic_contextual(spark, config, "context", tmp_path)
    assert len(snapshot) == 1 and iqr._fingerprint(namespace) == snapshot[0]
