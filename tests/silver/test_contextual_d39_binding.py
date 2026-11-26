"""Focused contextual publication in an authorized disk-backed synthetic workspace."""
from pathlib import Path
import os

import pytest

from tfm_lakehouse import scalability_workspace
from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.silver import contextual_identity, contextual_publication, contextual_reader, silver_synthetic_publication


@pytest.mark.integration
def test_contextual_publication_in_authorized_d39_workspace(spark, monkeypatch):
    campaign = scalability_workspace.authorize_campaign(os.environ['H06_D40_CAMPAIGN_ID'], 'synthetic-test')
    root = campaign.root
    electricity, metadata = root / 'raw' / 'electricity.csv', root / 'raw' / 'metadata.csv'
    with electricity.open('x', encoding='utf-8') as handle:
        handle.write('timestamp,' + ','.join(APPROVED_BUILDINGS) + '\n' +
                     '2016-01-01 00:00:00,' + ','.join(['1'] * 16) + '\n')
    with metadata.open('x', encoding='utf-8') as handle:
        handle.write('building_id\n' + '\n'.join(APPROVED_BUILDINGS) + '\n')
    config = BronzeConfig(root / 'raw', root / 'lakehouse', electricity, metadata, 'synthetic', 'invented')
    original_unlink = Path.unlink
    def preserve_partial_bronze_report(path, *args, **kwargs):
        if path.is_relative_to(root) and path.parent.name == '_runs' and path.suffix == '.tmp':
            return None
        return original_unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', preserve_partial_bronze_report)
    bronze = run_bronze(spark, config)
    silver_synthetic_publication.publish_synthetic_silver(spark, config, bronze['run_id'], 'silver', root)
    result = contextual_publication.publish_synthetic_contextual(spark, config, 'silver', 'context', root)
    evidence, frame = contextual_reader.read_synthetic_contextual(spark, config, 'context', root)
    assert evidence == result and frame.count() == 16
    repartitioned = frame.repartition(3).cache()
    repartitioned.count()
    repartitioned.unpersist()
    assert contextual_identity._multiset_hash(repartitioned) == result['intent']['context_multiset_sha256']
    assert (root / 'contextual-captures' / 'context' / 'completed.json').is_file()
