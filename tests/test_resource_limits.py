"""Injected D44 measurements; no resource exhaustion or subprocesses."""
from dataclasses import replace
import pytest
from tfm_lakehouse.resource_limits import ResourceSample, assess

GIB = 1024 ** 3
BASE = ResourceSample(0, 0, 0, 0, 0, 0, 26 * GIB, 4 * GIB)


@pytest.mark.parametrize('field,value,reason', [
    ('rss_bytes', 4 * GIB, 'rss'), ('tmp_bytes', 2 * GIB, 'tmp'),
    ('campaign_bytes', 20 * GIB, 'campaign_storage'),
    ('effective_memory_fraction', .8, 'memory'), ('operation_seconds', 1800, 'operation_time'),
    ('campaign_seconds', 28800, 'campaign_time'), ('campaign_free_bytes', 25 * GIB - 1, 'campaign_free'),
    ('tmp_free_bytes', 3 * GIB - 1, 'tmp_free'),
])
def test_stopping_boundaries(field, value, reason):
    assert reason in assess(replace(BASE, **{field: value})).stops


@pytest.mark.parametrize('field,value,reason', [
    ('rss_bytes', 3.2 * GIB, 'rss'), ('tmp_bytes', 1.5 * GIB, 'tmp'),
    ('campaign_bytes', 15 * GIB, 'campaign_storage'), ('effective_memory_fraction', .7, 'memory'),
    ('operation_seconds', 1200, 'operation_time'),
])
def test_warning_boundaries(field, value, reason):
    assert reason in assess(replace(BASE, **{field: value})).warnings


@pytest.mark.parametrize('value', [None, float('nan'), float('inf'), -1, True, 10**400])
def test_invalid_measurements_fail_closed(value):
    assert assess(replace(BASE, rss_bytes=value)).stops == ('invalid:rss_bytes',)


def test_healthy_sample_and_exact_free_space():
    sample = replace(BASE, campaign_free_bytes=25 * GIB, tmp_free_bytes=3 * GIB)
    assert not assess(sample).warnings and not assess(sample).stops
