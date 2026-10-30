"""Read-only, batch characterization of weather and local timestamp labels."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
from importlib import metadata as import_metadata, resources
import json
import math
import os
from pathlib import Path
import sys
import tomllib
from zoneinfo import TZPATH, ZoneInfo, ZoneInfoNotFoundError

from tfm_lakehouse import characterization as shared


WeatherError = shared.CharacterizationError
VARIABLES = {
    "airTemperature": "degrees Celsius",
    "cloudCoverage": "oktas; source codes require interpretation",
    "dewTemperature": "degrees Celsius",
    "precipDepth1HR": "mm per one-hour accumulation",
    "precipDepth6HR": "mm per six-hour accumulation",
    "seaLvlPressure": "hPa or mbar",
    "windDirection": "degrees clockwise from true north",
    "windSpeed": "m/s",
}
IANA_ALIASES = {
    "US/Eastern": "America/New_York",
    "US/Central": "America/Chicago",
    "US/Mountain": "America/Denver",
    "US/Pacific": "America/Los_Angeles",
}
WEATHER_REFERENCE = (
    "https://github.com/buds-lab/building-data-genome-project-2/wiki/Weather-features"
)


@dataclass(frozen=True)
class WeatherSettings:
    config_path: Path
    raw_root: Path
    metadata: Path
    electricity: Path
    weather: Path
    output_root: Path
    lakehouse_root: Path
    context_config: Path
    h03_2_output_root: Path
    kind: str
    reference: str
    candidates: tuple[shared.Candidate, ...]
    expected_hashes: dict[str, str]
    config_sha256: str
    context_sha256: str
    nominal_start: datetime
    nominal_end: datetime
    missing_tokens: tuple[str, ...] = ("", "NaN")
    timestamp_format: str = "%Y-%m-%d %H:%M:%S"
    max_grid_slots: int = 1000000


def _text(value, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WeatherError(f"{name} must be a nonempty string")
    return value


def _table(value, name: str) -> dict:
    if not isinstance(value, dict):
        raise WeatherError(f"{name} must be a TOML table")
    return value


def load_weather_settings(path: Path) -> WeatherSettings:
    path = path.expanduser().resolve()
    try:
        content = path.read_bytes()
        data = tomllib.loads(content.decode("utf-8"))
        base, paths = path.parent, _table(data["paths"], "paths")
        dataset = _table(data["dataset"], "dataset")
        sources = _table(data["sources"], "sources")
        context_entry = _table(data["context"], "context")

        def resolve(value, name: str) -> Path:
            return (base / Path(_text(value, name)).expanduser()).resolve()

        raw = resolve(paths["raw_root"], "raw_root")
        output = Path(os.path.abspath(base / Path(
            _text(paths["output_root"], "output_root")
        ).expanduser()))
        shared._reject_symlinks(output)
        context_path = resolve(context_entry["characterization_config"], "context config")
        context_bytes = context_path.read_bytes()
        context = tomllib.loads(context_bytes.decode("utf-8"))
        context_dataset = _table(context["dataset"], "context.dataset")
        context_paths = _table(context["paths"], "context.paths")
        context_candidates = context["candidates"]
        if not isinstance(context_candidates, list):
            raise WeatherError("Context candidates must be an array of tables")
        kind = _text(dataset["kind"], "dataset.kind")
        if context_dataset["kind"] != kind:
            raise WeatherError("Context and weather dataset kinds must match")
        context_output = (
            context_path.parent / Path(_text(
                context_paths["output_root"], "context output_root"
            )).expanduser()
        ).resolve()
        candidates = tuple(shared.Candidate(**row) for row in context_candidates)
        analysis = _table(data.get("analysis", {}), "analysis")
        fmt = _text(analysis.get("timestamp_format", "%Y-%m-%d %H:%M:%S"),
                    "timestamp_format")
        start = shared._parse_time(_text(analysis["nominal_start"], "nominal_start"), fmt)
        end = shared._parse_time(_text(analysis["nominal_end"], "nominal_end"), fmt)
        if start is None or end is None:
            raise WeatherError("Nominal endpoints must be valid naive timestamp labels")
        missing = analysis.get("missing_tokens", ["", "NaN"])
        if not isinstance(missing, list) or not all(isinstance(t, str) for t in missing):
            raise WeatherError("missing_tokens must be a list of strings")
        slots = analysis.get("max_grid_slots", 1000000)
        if type(slots) is not int:
            raise WeatherError("max_grid_slots must be an integer")
        expected = data.get("expected_sha256", {})
        if not isinstance(expected, dict):
            raise WeatherError("expected_sha256 must be a table")
        source_paths = {
            name: (raw / Path(_text(sources[name], name))).resolve()
            for name in ("metadata", "electricity", "weather")
        }
        settings = WeatherSettings(
            config_path=path, raw_root=raw, **source_paths,
            output_root=output.resolve(),
            lakehouse_root=resolve(paths["lakehouse_root"], "lakehouse_root"),
            context_config=context_path, h03_2_output_root=context_output,
            kind=kind, reference=_text(dataset["reference"], "dataset.reference"),
            candidates=candidates, expected_hashes=expected,
            config_sha256=hashlib.sha256(content).hexdigest(),
            context_sha256=hashlib.sha256(context_bytes).hexdigest(),
            nominal_start=start, nominal_end=end, missing_tokens=tuple(missing),
            timestamp_format=fmt, max_grid_slots=slots,
        )
    except (OSError, UnicodeError, KeyError, TypeError, ValueError) as exc:
        raise WeatherError(f"Invalid weather configuration {path}: {exc}") from exc
    validate_settings(settings)
    return settings


def validate_settings(settings: WeatherSettings) -> None:
    if settings.kind not in {"real", "synthetic"}:
        raise WeatherError("dataset.kind must be real or synthetic")
    if type(settings.max_grid_slots) is not int or settings.max_grid_slots <= 0:
        raise WeatherError("max_grid_slots must be positive")
    if not all(isinstance(token, str) for token in settings.missing_tokens):
        raise WeatherError("Missing tokens must be strings")
    if len(set(settings.missing_tokens)) != len(settings.missing_tokens):
        raise WeatherError("Duplicated missing tokens")
    for token in settings.missing_tokens:
        try:
            numeric = float(token)
        except ValueError:
            continue
        if math.isfinite(numeric):
            raise WeatherError("Finite numeric tokens cannot be reclassified as missing")
    if (not isinstance(settings.nominal_start, datetime)
            or not isinstance(settings.nominal_end, datetime)):
        raise WeatherError("Nominal endpoints must be datetime labels")
    if any(
        t.tzinfo is not None or t.minute or t.second or t.microsecond
        for t in (settings.nominal_start, settings.nominal_end)
    ) or settings.nominal_start > settings.nominal_end:
        raise WeatherError("Nominal endpoints must be ordered exact naive hourly labels")
    if _slot_count(settings.nominal_start, settings.nominal_end) > settings.max_grid_slots:
        raise WeatherError("Nominal calendar exceeds max_grid_slots")
    if not settings.candidates:
        raise WeatherError("The H03.2 context must contain candidates")
    for key in ("case_id", "building_id"):
        values = [getattr(c, key) for c in settings.candidates]
        if any(not isinstance(v, str) or not v.strip() for v in values):
            raise WeatherError(f"Invalid candidate {key}")
        if len(values) != len(set(values)):
            raise WeatherError(f"Duplicated candidate {key}")
    if any(not isinstance(c.group, str) or not c.group.strip() for c in settings.candidates):
        raise WeatherError("Candidate group must be a nonempty string")
    shared._reject_symlinks(settings.output_root)
    for protected in (settings.raw_root, settings.lakehouse_root,
                      settings.h03_2_output_root, settings.context_config,
                      settings.config_path):
        if shared._overlap(settings.output_root, protected):
            raise WeatherError("Report output_root overlaps protected inputs or H03.2")
    sources = [getattr(settings, name) for name in ("metadata", "electricity", "weather")]
    if len(set(sources)) != 3:
        raise WeatherError("Source paths must be distinct")
    if set(settings.expected_hashes) - {"metadata", "electricity", "weather"}:
        raise WeatherError("Unknown expected source hash")
    for name, source in zip(("metadata", "electricity", "weather"), sources):
        if settings.raw_root not in source.parents or not source.is_file():
            raise WeatherError(f"Source must be a file inside raw_root: {name}")
        expected = settings.expected_hashes.get(name)
        if settings.kind == "real" and expected is None:
            raise WeatherError(f"Expected SHA-256 required for real source {name}")
        if expected is not None and (
            not isinstance(expected, str) or len(expected) != 64
            or any(c not in "0123456789abcdef" for c in expected)
        ):
            raise WeatherError(f"Invalid expected SHA-256: {name}")


def validate_output(settings: WeatherSettings, output: Path) -> Path:
    validate_settings(settings)
    # H03.2's read-only validator uses these same path attributes.
    output = shared.validate_output(settings, output)
    if any(shared._overlap(output, p) for p in
           (settings.h03_2_output_root, settings.context_config)):
        raise WeatherError("Output overlaps H03.2 or its context configuration")
    return output


def _slot_count(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds() // 3600) + 1


def _grid(start: datetime, end: datetime) -> list[datetime]:
    return [start + timedelta(hours=i) for i in range(_slot_count(start, end))]


def _period(time: datetime | None, scale: str) -> str:
    if time is None:
        return "unassigned"
    return time.strftime("%Y" if scale == "year" else "%Y-%m")


@dataclass
class VariableData:
    readings: shared.Measurements = field(default_factory=lambda: shared.Measurements(False))
    minimum: float | None = None
    maximum: float | None = None
    finite_labels: set[datetime] = field(default_factory=set)
    periods: dict = field(default_factory=lambda: defaultdict(Counter))
    special_tokens: Counter = field(default_factory=Counter)

    def add(self, token: str, time: datetime | None, variable: str,
            missing: tuple[str, ...]) -> None:
        self.readings.add(token, missing)
        state, value = shared.classify_cell(token, missing)
        for scale in ("year", "month"):
            self.periods[(scale, _period(time, scale))][state] += 1
        if state != "finite":
            return
        self.minimum = value if self.minimum is None else min(self.minimum, value)
        self.maximum = value if self.maximum is None else max(self.maximum, value)
        if time is not None:
            self.finite_labels.add(time)
        if (variable in {"precipDepth1HR", "precipDepth6HR"} and value == -1
                or variable == "cloudCoverage" and value == 9):
            self.special_tokens[token] += 1

    def summary(self) -> dict:
        return {
            **self.readings.summary(), "minimum": self.minimum, "maximum": self.maximum,
            "special_code_tokens_preserved": dict(sorted(self.special_tokens.items())),
            "special_code_observations": sum(self.special_tokens.values()),
            "numeric_interpretation": "Finite text parsing does not establish physical validity",
        }


@dataclass
class SiteData:
    texts: list[str] = field(default_factory=list)
    times: list[datetime | None] = field(default_factory=list)
    variables: dict[str, VariableData] = field(
        default_factory=lambda: {name: VariableData() for name in VARIABLES}
    )


def _metadata(settings: WeatherSettings) -> tuple[dict, dict, list[str]]:
    buildings, zones = {}, defaultdict(set)
    with shared.csv_source(settings.metadata, {"building_id", "site_id", "timezone"}) as (header, rows):
        for row in rows:
            record = dict(zip(header, row))
            bid, site = record["building_id"], record["site_id"]
            if not bid.strip() or bid in buildings or not site.strip():
                raise WeatherError("Metadata keys must be nonempty and building_id unique")
            buildings[bid] = record
            zones[site].add(record["timezone"])
    return buildings, zones, header


def _inventory(settings: WeatherSettings) -> dict:
    result = {}
    for name in ("metadata", "electricity", "weather"):
        path = getattr(settings, name)
        digest = shared.sha256(path)
        expected = settings.expected_hashes.get(name)
        if expected is not None and digest != expected:
            raise WeatherError(f"Source SHA-256 mismatch: {name}")
        result[name] = {
            "relative_path": path.relative_to(settings.raw_root).as_posix(),
            "bytes": path.stat().st_size, "sha256": digest,
            "expected_hash_matched": digest == expected if expected is not None else None,
            "format": "Strict UTF-8 CSV; original fields retained as text",
        }
    return result


def _catalog(zones: set[str]) -> tuple[dict, dict[str, str]]:
    paths, availability = {}, {}
    for original in sorted(zones):
        for name in dict.fromkeys((original, IANA_ALIASES.get(original, original))):
            try:
                ZoneInfo.no_cache(name)
                availability[name] = "resolved"
            except (ValueError, ZoneInfoNotFoundError):
                availability[name] = "unavailable_in_environment"
                continue
            for root in TZPATH:
                path = Path(root) / name
                if path.is_file():
                    paths[str(path)] = shared.sha256(path)
                    break
            else:
                try:
                    resource = resources.files("tzdata.zoneinfo").joinpath(*name.split("/"))
                    path = Path(str(resource))
                    if path.is_file():
                        paths[str(path)] = shared.sha256(path)
                except (ModuleNotFoundError, TypeError):
                    pass
    version = None
    for root in TZPATH:
        path = Path(root) / "tzdata.zi"
        if path.is_file():
            paths[str(path)] = shared.sha256(path)
            with path.open(encoding="utf-8") as handle:
                version = handle.readline().strip()
            break
    try:
        package = import_metadata.version("tzdata")
    except import_metadata.PackageNotFoundError:
        package = None
    return {
        "system_catalog_version": version, "python_tzdata_version": package,
        "tzpath": list(TZPATH), "python_tzpath": os.environ.get("PYTHONTZPATH"),
        "availability": availability,
    }, paths


def _runtime_inputs(settings: WeatherSettings, inventory: dict, zones: set[str]) -> dict:
    environment, catalog_paths = _catalog(zones)
    files = {
        str(settings.config_path): settings.config_sha256,
        str(settings.context_config): settings.context_sha256,
        str(Path(__file__).resolve()): shared.sha256(Path(__file__)),
        str(Path(shared.__file__).resolve()): shared.sha256(Path(shared.__file__)),
        **{str(getattr(settings, name)): row["sha256"] for name, row in inventory.items()},
        **catalog_paths,
    }
    return {"file_hashes": files, "timezone_environment": environment,
            "settings_sha256": _settings_digest(settings)}


def _settings_digest(settings: WeatherSettings) -> str:
    content = json.dumps(asdict(settings), sort_keys=True, default=str,
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def _verify_inputs(provenance: dict) -> None:
    for name, expected in provenance["file_hashes"].items():
        if shared.sha256(Path(name)) != expected:
            raise WeatherError(f"Input, configuration, code or timezone catalog changed: {name}")


def _classify_local(time: datetime, zone: ZoneInfo) -> str:
    # These are calendar probes, not assignments to source observations.
    possibilities = set()
    for fold in (0, 1):
        probe = time.replace(tzinfo=zone, fold=fold)
        instant = probe.astimezone(timezone.utc)
        if instant.astimezone(zone).replace(tzinfo=None) == time:
            possibilities.add(instant)
    return "nonexistent" if not possibilities else "ambiguous" if len(possibilities) == 2 else "regular"


def _dst_profile(texts: list[str], times: list[datetime | None], name: str) -> dict:
    diagnostic = IANA_ALIASES.get(name, name)
    try:
        zone = ZoneInfo.no_cache(diagnostic)
    except (ValueError, ZoneInfoNotFoundError):
        return {"status": "timezone_unavailable", "diagnostic_zone": diagnostic,
                "counts_received_rows": None, "examples": []}
    counts, examples = Counter(), []
    cache = {}
    for index, (text, time) in enumerate(zip(texts, times), 1):
        if time is None:
            state = "invalid_timestamp"
        else:
            if time not in cache:
                cache[time] = _classify_local(time, zone)
            state = cache[time]
        counts[state] += 1
        if state in {"ambiguous", "nonexistent"} and len(examples) < 20:
            examples.append({"record_within_axis": index, "original_label": text, "class": state})
    return {
        "status": "calendar_hypothesis_only", "diagnostic_zone": diagnostic,
        "counts_received_rows": dict(sorted(counts.items())), "examples": examples,
        "interpretation": "Civil IANA hypothesis; no source fold, UTC timestamp or alignment assigned",
    }


def audit_timezones(buildings: dict, zones: dict, sites: dict,
                    electricity_texts: list[str], electricity_times: list) -> tuple[list, list]:
    inventory, diagnostics = [], []
    names = sorted({row["timezone"] for row in buildings.values()})
    for name in names:
        inventory.append({
            "original_timezone": name,
            "buildings": sum(row["timezone"] == name for row in buildings.values()),
            "site_ids": sorted(site for site, values in zones.items() if name in values),
            "documented_alias_target": IANA_ALIASES.get(name),
            "alias_reference": "https://data.iana.org/time-zones/tzdb/backward",
            "original_value_preserved": True,
        })
        diagnostics.append({
            "source": "electricity", "site_id": None, "original_timezone": name,
            **_dst_profile(electricity_texts, electricity_times, name),
        })
    for site, data in sorted(sites.items()):
        values = zones.get(site, set())
        if len(values) != 1 or not next(iter(values), ""):
            diagnostics.append({
                "source": "weather", "site_id": site, "original_timezone": sorted(values),
                "status": "missing_or_conflicting_metadata_timezone", "counts_received_rows": None,
            })
        else:
            name = next(iter(values))
            diagnostics.append({
                "source": "weather", "site_id": site, "original_timezone": name,
                **_dst_profile(data.texts, data.times, name),
            })
    return inventory, diagnostics


def _duplicates(first: dict, groups: dict, site: str, text: str,
                row: list[str], number: int) -> None:
    key = (site, text)
    digest = hashlib.sha256(json.dumps(row, ensure_ascii=False).encode("utf-8")).hexdigest()
    if key not in first:
        first[key] = (digest, number)
        return
    if key not in groups:
        initial, record = first[key]
        groups[key] = {"payloads": Counter({initial: 1}), "records": [record]}
    group = groups[key]
    group["payloads"][digest] += 1
    if len(group["records"]) < 20:
        group["records"].append(number)


def _duplicate_rows(groups: dict) -> list[dict]:
    return [{
        "site_id": site, "timestamp": text,
        "received_rows": sum(data["payloads"].values()),
        "additional_rows": sum(data["payloads"].values()) - 1,
        "distinct_parsed_payloads": len(data["payloads"]),
        "kind": "exact" if len(data["payloads"]) == 1 else "conflicting",
        "exact_duplicate_extra_rows": sum(data["payloads"].values()) - len(data["payloads"]),
        "source_record_examples": data["records"],
        "source_record_examples_truncated": sum(data["payloads"].values()) > len(data["records"]),
    } for (site, text), data in sorted(groups.items())]


def _period_rows(site: str, data: SiteData, observed: list, nominal: list) -> list:
    rows = []
    for basis, grid in (("observed_span", observed), ("nominal_calendar", nominal)):
        for scale in ("year", "month"):
            expected = defaultdict(set)
            source = defaultdict(set)
            for time in grid:
                expected[_period(time, scale)].add(time)
            for time in data.times:
                if time is not None:
                    source[_period(time, scale)].add(time)
            for variable, values in data.variables.items():
                finite = defaultdict(set)
                for time in values.finite_labels:
                    finite[_period(time, scale)].add(time)
                periods = {period for s, period in values.periods if s == scale} | set(expected)
                for period in sorted(periods):
                    counts = values.periods.get((scale, period), Counter())
                    total = sum(counts.values())
                    slots = expected[period] if grid and period != "unassigned" else None
                    covered = len(finite[period] & slots) if slots is not None else None
                    rows.append({
                        "site_id": site, "variable": variable, "basis": basis,
                        "scale": scale, "period": period, "received_records": total,
                        "finite_values": counts["finite"], "missing_values": counts["missing"],
                        "invalid_values": counts["nonnumeric"] + counts["nonfinite"],
                        "missing_pct_received": shared.percent(counts["missing"], total),
                        "expected_local_labels": len(slots) if slots is not None else None,
                        "expected_labels_absent_from_source": len(slots - source[period]) if slots is not None else None,
                        "distinct_labels_with_finite_value_in_grid": covered,
                        "finite_pct_expected_labels": shared.percent(covered, len(slots)) if slots else None,
                        "source_labels_outside_grid": len(source[period] - slots) if slots is not None else None,
                    })
    return rows


def analyze_weather(settings: WeatherSettings) -> dict:
    """Compute diagnostics without modifying inputs or publishing a report."""
    validate_settings(settings)
    inventory = _inventory(settings)
    buildings, zones, metadata_header = _metadata(settings)
    missing_candidates = {c.building_id for c in settings.candidates} - buildings.keys()
    if missing_candidates:
        raise WeatherError(f"Candidates absent from metadata: {sorted(missing_candidates)}")
    provenance = _runtime_inputs(settings, inventory, {z for values in zones.values() for z in values})
    _verify_inputs(provenance)
    electricity_texts, electricity_times = [], []
    with shared.csv_source(settings.electricity, {"timestamp"}) as (header, rows):
        if len(header) < 2:
            raise WeatherError("Electricity needs at least one building column")
        if not {c.building_id for c in settings.candidates}.issubset(header):
            raise WeatherError("Candidates absent from electricity columns")
        index = header.index("timestamp")
        for row in rows:
            electricity_texts.append(row[index])
            electricity_times.append(shared._parse_time(row[index], settings.timestamp_format))
        inventory["electricity"].update({"records": len(electricity_texts), "columns": header})
    sites, first, groups = {}, {}, {}
    with shared.csv_source(settings.weather, {"timestamp", "site_id", *VARIABLES}) as (header, rows):
        positions = {name: header.index(name) for name in ("timestamp", "site_id", *VARIABLES)}
        received = 0
        for received, row in enumerate(rows, 1):
            site, text = row[positions["site_id"]], row[positions["timestamp"]]
            if not site.strip():
                raise WeatherError("Weather site_id must be nonempty")
            if site not in sites:
                sites[site] = SiteData()
            data = sites[site]
            time = shared._parse_time(text, settings.timestamp_format)
            data.texts.append(text)
            data.times.append(time)
            _duplicates(first, groups, site, text, row, received)
            for variable in VARIABLES:
                data.variables[variable].add(row[positions[variable]], time, variable,
                                             settings.missing_tokens)
        inventory["weather"].update({"records": received, "columns": header})
    inventory["metadata"].update({"records": len(buildings), "columns": metadata_header})
    duplicate_rows = _duplicate_rows(groups)
    nominal = _grid(settings.nominal_start, settings.nominal_end)
    nominal_set = set(nominal)
    site_rows, quality, coverage = [], [], []
    for site, data in sorted(sites.items()):
        temporal, observed = shared.temporal_profile(data.texts, data.times, settings.max_grid_slots)
        labels = {t for t in data.times if t is not None}
        repeated = [r for r in duplicate_rows if r["site_id"] == site]
        site_rows.append({
            "site_id": site, **temporal, "record_index_basis": "received order within site",
            "duplicate_keys": len(repeated),
            "exact_duplicate_keys": sum(r["kind"] == "exact" for r in repeated),
            "conflicting_duplicate_keys": sum(r["kind"] == "conflicting" for r in repeated),
            "metadata_buildings": sum(r["site_id"] == site for r in buildings.values()),
            "metadata_timezone_values": sorted(zones.get(site, set())),
            "nominal_expected_labels": len(nominal),
            "nominal_labels_absent_from_source": len(nominal_set - labels),
            "source_labels_outside_nominal_calendar": len(labels - nominal_set),
        })
        quality.extend({"site_id": site, "variable": name,
                        "documented_unit": VARIABLES[name], **values.summary()}
                       for name, values in data.variables.items())
        coverage.extend(_period_rows(site, data, observed, nominal))
    timezone_rows, dst_rows = audit_timezones(buildings, zones, sites,
                                              electricity_texts, electricity_times)
    electrical, _ = shared.temporal_profile(electricity_texts, electricity_times,
                                            settings.max_grid_slots)
    candidates = []
    for candidate in settings.candidates:
        metadata = buildings[candidate.building_id]
        site = metadata["site_id"]
        candidates.append({
            "case_id": candidate.case_id, "building_id": candidate.building_id,
            "author_group": candidate.group, "site_id": site,
            "original_timezone": metadata["timezone"], "weather_site_present": site in sites,
            "association": "Exact site_id match; not physical temporal alignment",
            "weather_received_records": len(sites[site].texts) if site in sites else 0,
            "variables": {
                name: {"missing_values": v.readings.counts["missing"],
                       "received_records": len(sites[site].texts),
                       "missing_pct_received": shared.percent(v.readings.counts["missing"], len(sites[site].texts))}
                for name, v in sites[site].variables.items()
            } if site in sites else {},
        })
    metrics = {
        "schema_version": "h03.3-v1", "dataset_kind": settings.kind,
        "dataset_reference": settings.reference, "inventory": inventory,
        "methodology": {
            "missing_tokens_exact": list(settings.missing_tokens),
            "received_denominator": "CSV records per site and variable, including repetitions",
            "finite_denominator": "Finite numeric cells, without physical-validity assertion",
            "coverage_denominator": "Distinct naive local labels in the stated calendar; not elapsed hours",
            "duplicate_key": ["site_id", "original timestamp text"],
            "exact_duplicate": "Identical complete parsed CSV fields; byte serialization may differ",
            "nominal_start": str(settings.nominal_start), "nominal_end": str(settings.nominal_end),
            "timestamp_format": settings.timestamp_format, "max_grid_slots": settings.max_grid_slots,
            "context_sha256": settings.context_sha256,
        },
        "metadata": {"buildings": len(buildings), "sites": len(zones),
                     "sites_without_weather": sorted(zones.keys() - sites.keys()),
                     "weather_sites_without_metadata": sorted(sites.keys() - zones.keys()),
                     "conflicting_site_timezones": sorted(s for s, z in zones.items() if len(z) != 1)},
        "weather": {
            "received_records": inventory["weather"]["records"], "site_ids": sorted(sites),
            "duplicate_keys": len(duplicate_rows),
            "exact_duplicate_keys": sum(r["kind"] == "exact" for r in duplicate_rows),
            "conflicting_duplicate_keys": sum(r["kind"] == "conflicting" for r in duplicate_rows),
            "rows_in_repeated_keys": sum(r["received_rows"] for r in duplicate_rows),
            "additional_rows_in_repeated_keys": sum(r["additional_rows"] for r in duplicate_rows),
            "exact_duplicate_extra_rows": sum(r["exact_duplicate_extra_rows"] for r in duplicate_rows),
        },
        "electricity_labels": electrical, "site_coverage": site_rows,
        "variable_quality": quality, "period_coverage": coverage,
        "duplicate_keys": duplicate_rows, "candidate_weather_availability": candidates,
        "timezone_inventory": timezone_rows, "dst_diagnostics": dst_rows,
        "evidence": {
            "observed": "Counts, original labels, parsed tokens and nominal site_id correspondence",
            "official_documentation": {
                "weather": WEATHER_REFERENCE,
                "weather_time_statement": "Local timezone, DST is ignored",
                "electricity": "https://github.com/buds-lab/building-data-genome-project-2/wiki/Meters-data-features",
                "units": VARIABLES,
            },
            "hypotheses": ["Calendar probes assume civil IANA rules; they do not establish source DST policy",
                           "Precipitation -1 and cloud code 9 may have source-specific semantic meanings"],
            "pending_decisions": ["Source DST policy and interval boundaries", "Physical station representativeness",
                                  "Interpretation of special codes and temporal join cardinality"],
        },
        "integration_readiness": "not_validated_no_hourly_join_performed",
    }
    json.dumps(metrics, allow_nan=False)
    _verify_inputs(provenance)
    return {"metrics": metrics, "provenance": provenance}


def write_weather_reports(settings: WeatherSettings, output: Path, analysis: dict,
                          argv: list[str] | None = None) -> Path:
    output = validate_output(settings, output)
    _verify_inputs(analysis["provenance"])
    if analysis["provenance"]["settings_sha256"] != _settings_digest(settings):
        raise WeatherError("Analysis does not belong to the current settings")
    metrics = analysis["metrics"]
    hashes = analysis["provenance"]["file_hashes"]
    if (metrics["dataset_kind"] != settings.kind
            or metrics["dataset_reference"] != settings.reference
            or hashes.get(str(settings.config_path)) != settings.config_sha256
            or hashes.get(str(settings.context_config)) != settings.context_sha256):
        raise WeatherError("Analysis does not belong to the current settings")
    for name in ("metadata", "electricity", "weather"):
        source = getattr(settings, name)
        source_info = metrics["inventory"][name]
        if (hashes.get(str(source)) != source_info["sha256"]
                or source_info["relative_path"] != source.relative_to(settings.raw_root).as_posix()
                or shared.sha256(source) != source_info["sha256"]):
            raise WeatherError(f"Analysis belongs to different source settings: {name}")
    metrics_bytes = json.dumps(metrics, ensure_ascii=False, sort_keys=True,
                               allow_nan=False).encode("utf-8")
    execution = {
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "argv": list(sys.argv if argv is None else argv), "python": sys.version,
        "git_head": shared._git(["rev-parse", "HEAD"], settings.config_path.parent),
        "git_status_short": shared._git(["status", "--short"], settings.config_path.parent),
        "source_paths": {n: str(getattr(settings, n)) for n in ("metadata", "electricity", "weather")},
        "output_path": str(output), **analysis["provenance"],
        "deterministic_metrics_sha256": hashlib.sha256(metrics_bytes).hexdigest(),
        "status": "completed_diagnostic_not_integration_validation",
    }
    shared._reject_symlinks(output)
    output.mkdir(parents=True, exist_ok=False)
    shared._reject_symlinks(output)
    for name, payload in (("report.json", {"execution": execution, "metrics": metrics}),
                          ("execution.json", execution)):
        with (output / name).open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
            handle.write("\n")
    tables = {"source_inventory": [{"source": n, **r} for n, r in metrics["inventory"].items()]}
    for name in ("site_coverage", "variable_quality", "period_coverage", "duplicate_keys",
                 "candidate_weather_availability", "timezone_inventory", "dst_diagnostics"):
        tables[name] = metrics[name]
    for name, rows in tables.items():
        shared._write_csv(output / f"{name}.csv", rows)
    artifacts = {p.name: shared.sha256(p) for p in sorted(output.iterdir()) if p.is_file()}
    # Publish the final name only after closing JSON and verifying the inputs.
    pending_success = output / ".SUCCESS.json.tmp"
    with pending_success.open("x", encoding="utf-8") as handle:
        json.dump({
            "metrics_sha256": execution["deterministic_metrics_sha256"],
            "source_hashes": {n: r["sha256"] for n, r in metrics["inventory"].items()},
            "artifact_hashes": artifacts,
            "interpretation": "Complete diagnostic publication, not analytical quality approval",
        }, handle, sort_keys=True, indent=2)
        handle.write("\n")
    _verify_inputs(analysis["provenance"])
    pending_success.replace(output / "SUCCESS.json")
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="H03.3: read-only weather and local-label diagnostics")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        settings = load_weather_settings(args.config)
        output = validate_output(settings, args.output)
        analysis = analyze_weather(settings)
        command = [sys.executable, "-m", "tfm_lakehouse.weather_characterization",
                   *(sys.argv[1:] if argv is None else argv)]
        write_weather_reports(settings, output, analysis, argv=command)
    except (WeatherError, OSError, ValueError) as exc:
        print(f"Weather characterization failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"status": "completed", "output": str(output),
                      "integration_readiness": analysis["metrics"]["integration_readiness"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
