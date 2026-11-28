"""D44 threshold evaluation; not a workload launcher or safety supervisor."""
from dataclasses import dataclass
from math import isfinite

_GIB = 1024 ** 3
OPERATION_STOP_SECONDS = 1800
CAMPAIGN_STOP_SECONDS = 28800
RESOURCE_STOPS = frozenset({'rss', 'tmp', 'campaign_storage', 'memory', 'operation_time',
                            'campaign_time', 'campaign_free', 'tmp_free'})


@dataclass(frozen=True)
class ResourceSample:
    rss_bytes: float | None
    tmp_bytes: float | None
    campaign_bytes: float | None
    effective_memory_fraction: float | None
    operation_seconds: float | None
    campaign_seconds: float | None
    campaign_free_bytes: float | None
    tmp_free_bytes: float | None


@dataclass(frozen=True)
class Assessment:
    warnings: tuple[str, ...]
    stops: tuple[str, ...]


STORAGE_FIELDS = frozenset({'tmp_bytes', 'campaign_bytes', 'campaign_free_bytes', 'tmp_free_bytes'})


def assess(sample: ResourceSample, *, storage_only: bool = False) -> Assessment:
    """Fail closed on unavailable or invalid measurements; thresholds are absolute."""
    invalid = set()
    warnings, stops = [], []
    active = STORAGE_FIELDS if storage_only else vars(sample).keys()
    for name, value in vars(sample).items():
        if name not in active:
            continue
        try:
            valid = (not isinstance(value, bool) and isinstance(value, (int, float))
                     and isfinite(value) and value >= 0)
        except OverflowError:
            valid = False
        if not valid or (name == "effective_memory_fraction" and value > 1):
            invalid.add(name)
            stops.append(f"invalid:{name}")
    bounds = (
        ("rss", "rss_bytes", 3.2 * _GIB, 4 * _GIB),
        ("tmp", "tmp_bytes", 1.5 * _GIB, 2 * _GIB),
        ("campaign_storage", "campaign_bytes", 15 * _GIB, 20 * _GIB),
        ("memory", "effective_memory_fraction", 0.7, 0.8),
        ("operation_time", "operation_seconds", 1200, OPERATION_STOP_SECONDS),
        ("campaign_time", "campaign_seconds", None, CAMPAIGN_STOP_SECONDS),
    )
    for name, field, warning, stop in bounds:
        if field not in active or field in invalid:
            continue
        value = getattr(sample, field)
        if value >= stop:
            stops.append(name)
        elif warning is not None and value >= warning:
            warnings.append(name)
    if "campaign_free_bytes" not in invalid and sample.campaign_free_bytes < 25 * _GIB:
        stops.append("campaign_free")
    if "tmp_free_bytes" not in invalid and sample.tmp_free_bytes < 3 * _GIB:
        stops.append("tmp_free")
    return Assessment(tuple(warnings), tuple(stops))
