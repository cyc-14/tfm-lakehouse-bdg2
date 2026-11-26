"""Non-destructive synthetic weather compatibility through retained Delta inputs."""
import pytest

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.silver import contextual_publication, silver_synthetic_publication
from tfm_lakehouse.silver import weather_capture, weather_publication, weather_reader
from tfm_lakehouse.silver.weather_integrity import weather_tree_fingerprint


@pytest.mark.integration
def test_weather_reader_with_contextual_capture_retains_evidence(spark, tmp_path):
    raw = tmp_path / 'raw'
    raw.mkdir()
    electricity, metadata, weather = (raw / name for name in
                                      ('electricity.csv', 'metadata.csv', 'weather.csv'))
    electricity.write_text('timestamp,' + ','.join(APPROVED_BUILDINGS) + '\n' +
                           ('2016-01-01 12:30:00,' + ','.join(['1'] * 16) + '\n') * 2)
    metadata.write_text('building_id,site_id\n' +
                        '\n'.join(f'{name},S' for name in APPROVED_BUILDINGS) + '\n')
    weather.write_text('site_id,timestamp,airTemperature\nS,2016-01-01 12:00:00,12\n')
    config = BronzeConfig(raw, tmp_path / 'bronze', electricity, metadata,
                          'synthetic', 'invented', weather=weather)
    bronze = run_bronze(spark, config)
    silver_synthetic_publication.publish_synthetic_silver(spark, config, bronze['run_id'], 'silver', tmp_path)
    contextual_publication.publish_synthetic_contextual(spark, config, 'silver', 'context', tmp_path)
    weather_capture.create_synthetic_weather_capture(spark, config, 'silver', 'context', 'weather',
                                                      'capture', 'owner', tmp_path)
    published = weather_publication.publish_synthetic_weather(spark, config, 'capture', 'owner',
                                                               'weather', tmp_path)
    before = weather_tree_fingerprint(tmp_path)
    evidence, frame = weather_reader.read_synthetic_weather(spark, config, 'weather', tmp_path)
    assert evidence == published and evidence['delta_version'] == 0
    assert frame.schema.jsonValue() == evidence['physical_schema']
    assert frame.count() == 32
    assert weather_tree_fingerprint(tmp_path) == before
