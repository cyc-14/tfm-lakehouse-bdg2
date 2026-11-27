"""D44 threshold evaluation; not a workload launcher or safety supervisor."""
from dataclasses import dataclass
from math import isfinite

_GIB = 1024 ** 3


@dataclass(frozen=True)
class ResourceSample:
    rss_bytes: float
    tmp_bytes: float
    campaign_bytes: float
    effective_memory_fraction: float
    operation_seconds: float
    campaign_seconds: float
    campaign_free_bytes: float
    tmp_free_bytes: float


@dataclass(frozen=True)
class Assessment:
    warnings: tuple[str, ...]
    stops: tuple[str, ...]


def assess(sample: ResourceSample) -> Assessment:
    """Fail closed on unavailable or invalid measurements; thresholds are absolute."""
    values = vars(sample)
    for name, value in values.items():
        try:
            valid = (not isinstance(value, bool) and isinstance(value, (int, float))
                     and isfinite(value) and value >= 0)
        except OverflowError:
            valid = False
        if not valid:
            return Assessment((), (f"invalid:{name}",))
    if sample.effective_memory_fraction > 1:
        return Assessment((), ("invalid:effective_memory_fraction",))
    bounds = (
        ("rss", sample.rss_bytes, 3.2 * _GIB, 4 * _GIB),
        ("tmp", sample.tmp_bytes, 1.5 * _GIB, 2 * _GIB),
        ("campaign_storage", sample.campaign_bytes, 15 * _GIB, 20 * _GIB),
        ("memory", sample.effective_memory_fraction, 0.7, 0.8),
        ("operation_time", sample.operation_seconds, 1200, 1800),
        ("campaign_time", sample.campaign_seconds, None, 28800),
    )
    warnings, stops = [], []
    for name, value, warning, stop in bounds:
        if value >= stop:
            stops.append(name)
        elif warning is not None and value >= warning:
            warnings.append(name)
    if sample.campaign_free_bytes < 25 * _GIB:
        stops.append("campaign_free")
    if sample.tmp_free_bytes < 3 * _GIB:
        stops.append("tmp_free")
    return Assessment(tuple(warnings), tuple(stops))
