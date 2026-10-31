"""Reproducible read-only characterization of local wide BDG2 CSVs."""
from __future__ import annotations

import argparse
import collections
from contextlib import contextmanager
from copy import deepcopy
import csv
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tomllib
import traceback
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class CharacterizationError(ValueError):
    """A configuration, input, or evidence-publication contract failed."""


@dataclass(frozen=True)
class Candidate:
    case_id: str
    building_id: str
    group: str


@dataclass(frozen=True)
class Settings:
    config_path: Path
    raw_root: Path
    metadata: Path
    electricity: Path
    output_root: Path
    lakehouse_root: Path
    kind: str
    reference: str
    unit: str
    unit_reference: str
    expected_hashes: dict[str, str]
    candidates: tuple[Candidate, ...]
    config_sha256: str = ""
    primary_usage: str = "Lodging/residential"
    min_sqm: Decimal = Decimal("3000")
    max_sqm: Decimal = Decimal("7000")
    thresholds: tuple[Decimal, ...] = (Decimal("5"), Decimal("8"), Decimal("10"))
    missing_tokens: tuple[str, ...] = ("", "NaN")
    timestamp_format: str = "%Y-%m-%d %H:%M:%S"
    iqr_multiplier: float = 1.5
    max_grid_slots: int = 1000000


_BINDING_CONTRACT = "h03.2-analysis-binding-v1"
_BINDING_SERIALIZATION = "json-utf8-sorted-compact-v1"
_SETTINGS_FIELDS = (
    "config_path", "raw_root", "metadata", "electricity", "output_root", "lakehouse_root",
    "kind", "reference", "unit", "unit_reference", "expected_hashes", "candidates",
    "config_sha256", "primary_usage", "min_sqm", "max_sqm", "thresholds",
    "missing_tokens", "timestamp_format", "iqr_multiplier", "max_grid_slots",
)


def _canonical_bytes(value) -> bytes:
    """Encode binding data without rounding numbers or normalizing Decimal text."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _binding_digest(value) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _effective_settings(settings: Settings) -> dict:
    """Capture every Settings field as independent, JSON-compatible binding data."""
    if {item.name for item in fields(Settings)} != set(_SETTINGS_FIELDS):
        raise CharacterizationError("Settings fields changed; update the binding contract")

    def convert(value):
        if isinstance(value, Path):
            return value.as_posix()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, dict):
            return {key: convert(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [convert(item) for item in value]
        if value is None or type(value) in (str, int, float, bool):
            return value
        raise CharacterizationError("Unsupported effective-settings value")

    result = convert(asdict(settings))
    _canonical_bytes(result)
    return result


def _analytic_digest(report: dict) -> str:
    # Excluding the binding prevents the payload hash from containing itself.
    return _binding_digest({key: value for key, value in report.items()
                            if key != "analysis_provenance"})


def _verify_configuration(settings: Settings, provenance: dict, phase: str) -> None:
    snapshot = provenance["effective_settings"]
    if (_binding_digest(snapshot) != provenance["effective_settings_sha256"]
            or _binding_digest(_effective_settings(settings))
            != provenance["effective_settings_sha256"]):
        raise CharacterizationError(f"Effective settings changed {phase}")
    configuration = provenance["configuration"]
    if (configuration["path"] != settings.config_path.resolve().as_posix()
            or sha256(settings.config_path) != configuration["sha256"]):
        raise CharacterizationError(f"Configuration changed {phase}")
    if settings.config_sha256 and settings.config_sha256 != configuration["sha256"]:
        raise CharacterizationError("Configuration does not match its loaded SHA-256")


def _verify_analysis_binding(settings: Settings, report: dict, phase: str) -> None:
    """Fail closed on legacy, mixed, or accidentally modified analysis results."""
    try:
        provenance = report["analysis_provenance"]
        if (report["schema_version"] != "h03.2-v2"
                or provenance["contract_version"] != _BINDING_CONTRACT
                or provenance["canonical_serialization"] != _BINDING_SERIALIZATION):
            raise CharacterizationError("Unsupported analysis binding contract")
        if _analytic_digest(report) != provenance["analyzed_payload_sha256"]:
            raise CharacterizationError("Analytical content changed after analysis")
        _verify_configuration(settings, provenance, phase)
        if set(provenance["sources"]) != {"metadata", "electricity"}:
            raise CharacterizationError("Invalid bound source roles")
        for name in ("metadata", "electricity"):
            source = getattr(settings, name)
            bound = provenance["sources"][name]
            inventory = report["inventory"][name]
            if (bound["path"] != source.resolve().as_posix() or inventory["path"] != str(source)
                    or bound["sha256"] != inventory["sha256"]
                    or sha256(source) != bound["sha256"]):
                raise CharacterizationError(f"Source changed {phase}: {name}")
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        if isinstance(exc, CharacterizationError):
            raise
        raise CharacterizationError("Missing or invalid analysis binding") from exc


def sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _reject_symlinks(path: Path) -> None:
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise CharacterizationError(f"Output aliases are not allowed: {parent}")


def load_settings(path: Path) -> Settings:
    path = path.expanduser().resolve()
    try:
        content = path.read_bytes()
        data = tomllib.loads(content.decode("utf-8"))
        paths, dataset, base = data["paths"], data["dataset"], path.parent

        def resolve(value: str) -> Path:
            return (base / Path(value).expanduser()).resolve()

        raw = resolve(paths["raw_root"])
        output = Path(os.path.abspath(base / paths["output_root"]))
        _reject_symlinks(output)
        analysis = data.get("analysis", {})
        settings = Settings(
            config_path=path, config_sha256=hashlib.sha256(content).hexdigest(),
            raw_root=raw,
            metadata=(raw / data["sources"]["metadata"]).resolve(),
            electricity=(raw / data["sources"]["electricity"]).resolve(),
            output_root=output.resolve(), lakehouse_root=resolve(paths["lakehouse_root"]),
            kind=dataset["kind"], reference=dataset["reference"],
            unit=dataset["unit"], unit_reference=dataset["unit_reference"],
            expected_hashes=dict(data.get("expected_sha256", {})),
            candidates=tuple(Candidate(**row) for row in data["candidates"]),
            primary_usage=analysis.get("primary_usage", "Lodging/residential"),
            min_sqm=Decimal(str(analysis.get("min_sqm", 3000))),
            max_sqm=Decimal(str(analysis.get("max_sqm", 7000))),
            thresholds=tuple(Decimal(str(x)) for x in analysis.get("thresholds", [5, 8, 10])),
            missing_tokens=tuple(analysis.get("missing_tokens", ["", "NaN"])),
            timestamp_format=analysis.get("timestamp_format", "%Y-%m-%d %H:%M:%S"),
            iqr_multiplier=float(analysis.get("iqr_multiplier", 1.5)),
            max_grid_slots=int(analysis.get("max_grid_slots", 1000000)),
        )
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise CharacterizationError(f"Invalid configuration {path}: {exc}") from exc
    validate_settings(settings)
    return settings


def validate_settings(settings: Settings) -> None:
    if settings.kind not in {"real", "synthetic"}:
        raise CharacterizationError("dataset.kind must be real or synthetic")
    if not all((settings.reference.strip(), settings.unit.strip(), settings.unit_reference.strip())):
        raise CharacterizationError("Dataset reference, unit and unit_reference are required")
    if not settings.candidates:
        raise CharacterizationError("At least one detailed candidate is required")
    for key in ("case_id", "building_id"):
        values = [getattr(c, key) for c in settings.candidates]
        if any(not v for v in values) or len(values) != len(set(values)):
            raise CharacterizationError(f"Empty or duplicated candidate {key}")
    if not (settings.min_sqm.is_finite() and settings.max_sqm.is_finite()
            and 0 <= settings.min_sqm <= settings.max_sqm):
        raise CharacterizationError("Invalid inclusive area limits")
    if not settings.thresholds or any(not t.is_finite() or not 0 <= t <= 100
                                      for t in settings.thresholds):
        raise CharacterizationError("Thresholds must lie between 0 and 100")
    if not math.isfinite(settings.iqr_multiplier) or settings.iqr_multiplier <= 0:
        raise CharacterizationError("Invalid IQR multiplier")
    if settings.max_grid_slots <= 0:
        raise CharacterizationError("max_grid_slots must be positive")
    if not all(isinstance(t, str) for t in settings.missing_tokens):
        raise CharacterizationError("Missing tokens must be strings")
    if len(set(settings.missing_tokens)) != len(settings.missing_tokens):
        raise CharacterizationError("Duplicated missing tokens")
    _reject_symlinks(settings.output_root)
    if _overlap(settings.raw_root, settings.output_root):
        raise CharacterizationError("Raw inputs and report outputs must be separate")
    if _overlap(settings.lakehouse_root, settings.output_root):
        raise CharacterizationError("Reports and Lakehouse must be separate")
    if settings.metadata == settings.electricity:
        raise CharacterizationError("Source paths must be distinct")
    for name in ("metadata", "electricity"):
        source = getattr(settings, name)
        if settings.raw_root not in source.parents or not source.is_file():
            raise CharacterizationError(f"Source must be a file inside raw_root: {source}")
        expected = settings.expected_hashes.get(name)
        if settings.kind == "real" and expected is None:
            raise CharacterizationError(f"Expected SHA-256 required for real source {name}")
        if expected is not None and (len(expected) != 64 or
                                     any(c not in "0123456789abcdef" for c in expected)):
            raise CharacterizationError(f"Invalid expected SHA-256 for {name}")


def _git(arguments: list[str], cwd: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", *arguments], cwd=cwd, capture_output=True, text=True,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
        )
    except OSError:
        return None
    return result.stdout if result.returncode == 0 else None


def validate_output(settings: Settings, output: Path) -> Path:
    lexical = Path(os.path.abspath(output.expanduser()))
    _reject_symlinks(lexical)
    output = lexical.resolve()
    namespace = settings.output_root / settings.kind
    if output == namespace or namespace not in output.parents:
        raise CharacterizationError(f"Output must be a new run beneath {namespace}")
    if os.path.lexists(output):
        raise CharacterizationError(f"Output already exists; choose a new run: {output}")
    if any(_overlap(output, p) for p in
           (settings.raw_root, settings.lakehouse_root, settings.config_path)):
        raise CharacterizationError("Output overlaps protected inputs or Lakehouse")
    if settings.kind == "real":
        probe = output.parent
        while not probe.exists():
            probe = probe.parent
        if not probe.is_dir():
            probe = probe.parent
        root = _git(["rev-parse", "--show-toplevel"], probe)
        if root:
            git_root = Path(root.strip())
            result = subprocess.run(
                ["git", "check-ignore", "-q", "--", str(output.relative_to(git_root))],
                cwd=git_root, capture_output=True,
                env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
            )
            if result.returncode != 0:
                raise CharacterizationError("Real-data reports must be ignored by Git")
    return output


@contextmanager
def csv_source(path: Path, required: set[str]):
    try:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle, strict=True)
            header = next(reader, None)
            if not header:
                raise CharacterizationError(f"Empty CSV or missing header: {path}")
            if header[0].startswith("version https://git-lfs.github.com/spec/"):
                raise CharacterizationError(f"Git LFS pointer is not CSV content: {path}")
            lower = [s.casefold() for s in header]
            if any(not s.strip() for s in header) or len(set(lower)) != len(header):
                raise CharacterizationError(f"Empty or duplicated CSV headers: {path}")
            if not required.issubset(header):
                raise CharacterizationError(f"Missing columns {sorted(required-set(header))}: {path}")

            def checked():
                count = 0
                for row in reader:
                    if not row:
                        continue
                    if len(row) != len(header):
                        raise CharacterizationError(f"CSV width mismatch at line {reader.line_num}: {path}")
                    count += 1
                    yield row
                if not count:
                    raise CharacterizationError(f"CSV has no data records: {path}")

            yield header, checked()
    except (OSError, UnicodeError, csv.Error) as exc:
        raise CharacterizationError(f"Cannot read strict UTF-8 CSV {path}: {exc}") from exc


def classify_cell(token: str, missing_tokens: tuple[str, ...]) -> tuple[str, float | None]:
    if token in missing_tokens:
        return "missing", None
    try:
        value = float(token)
    except ValueError:
        return "nonnumeric", None
    return ("finite", value) if math.isfinite(value) else ("nonfinite", None)


def percent(numerator: int, denominator: int) -> float | None:
    return 100 * numerator / denominator if denominator else None


def passes_threshold(missing: int, total: int, threshold: Decimal) -> bool:
    return total > 0 and Decimal(missing) * 100 <= Decimal(total) * threshold


def quantile(values: list[float], probability: float) -> float | None:
    if not values:
        return None
    position = (len(values) - 1) * probability
    lower = math.floor(position)
    weight = position - lower
    if weight == 0:
        return values[lower]
    return values[lower] * (1 - weight) + values[lower + 1] * weight


@dataclass
class Measurements:
    detailed: bool
    counts: collections.Counter = field(default_factory=collections.Counter)
    missing_tokens: collections.Counter = field(default_factory=collections.Counter)
    invalid_tokens: collections.Counter = field(default_factory=collections.Counter)
    cells: list[tuple[str, float | None]] = field(default_factory=list)

    def add(self, token: str, missing: tuple[str, ...]) -> None:
        state, value = classify_cell(token, missing)
        self.counts[state] += 1
        if state == "missing":
            self.missing_tokens[token] += 1
        elif state != "finite":
            self.invalid_tokens[token] += 1
        elif value == 0:
            self.counts["zero"] += 1
        elif value < 0:
            self.counts["negative"] += 1
        if self.detailed:
            self.cells.append((state, value))

    def summary(self) -> dict:
        valid, missing = self.counts["finite"], self.counts["missing"]
        invalid = self.counts["nonnumeric"] + self.counts["nonfinite"]
        total = valid + missing + invalid
        return {
            "source_records": total, "finite_readings": valid,
            "missing": missing, "missing_pct_received": percent(missing, total),
            "nonnumeric": self.counts["nonnumeric"], "nonfinite": self.counts["nonfinite"],
            "invalid": invalid, "invalid_pct_received": percent(invalid, total),
            "unusable_pct_received": percent(missing + invalid, total),
            "finite_pct_received": percent(valid, total),
            "zero_readings": self.counts["zero"], "zero_pct_finite": percent(self.counts["zero"], valid),
            "negatives": self.counts["negative"],
            "negative_pct_finite": percent(self.counts["negative"], valid),
            "missing_tokens": dict(sorted(self.missing_tokens.items())),
            "invalid_tokens": dict(sorted(self.invalid_tokens.items())),
        }


def _parse_time(text: str, fmt: str) -> datetime | None:
    try:
        value = datetime.strptime(text, fmt)
        if value.tzinfo is not None or value.strftime(fmt) != text:
            return None
        return value
    except ValueError:
        return None


def _time_text(value: datetime | None) -> str | None:
    return value.isoformat(sep=" ") if value is not None else None


def temporal_profile(texts: list[str], times: list[datetime | None],
                     max_slots: int) -> tuple[dict, list[datetime]]:
    counts = collections.Counter(value for value in times if value is not None)
    ordered = sorted(counts)
    gaps, backwards = [], []
    for index in range(1, len(times)):
        before, after = times[index - 1:index + 1]
        if before is None or after is None:
            continue
        seconds = (after - before).total_seconds()
        if seconds > 3600:
            gaps.append({"previous_record": index, "next_record": index + 1,
                         "previous": _time_text(before), "next": _time_text(after),
                         "label_delta_seconds": seconds})
        if seconds < 0:
            backwards.append({"previous_record": index, "next_record": index + 1,
                              "previous": _time_text(before), "next": _time_text(after)})
    grid, limitation = [], None
    off_hour = sum(count for time, count in counts.items()
                   if time.minute or time.second or time.microsecond)
    if not ordered:
        limitation = "No interpretable local timestamp labels"
    elif off_hour:
        limitation = "Off-hour labels: an hourly expected grid is not asserted"
    else:
        slots = int((ordered[-1] - ordered[0]).total_seconds() // 3600) + 1
        if slots > max_slots:
            limitation = "Observed span exceeds max_grid_slots; no expected grid materialized"
        else:
            grid = [ordered[0] + timedelta(hours=i) for i in range(slots)]
    profile = {
        "representation": "Original naive local labels; no UTC conversion",
        "source_records": len(times), "valid_timestamp_rows": sum(counts.values()),
        "invalid_timestamp_rows": len(times) - sum(counts.values()),
        "invalid_timestamp_examples": [
            {"record": i + 1, "text": text}
            for i, (text, time) in enumerate(zip(texts, times)) if time is None
        ][:20],
        "distinct_timestamp_labels": len(counts),
        "duplicate_timestamp_rows": sum(count - 1 for count in counts.values()),
        "duplicate_timestamp_labels": [
            {"timestamp": _time_text(t), "rows": count}
            for t, count in sorted(counts.items()) if count > 1
        ],
        "first_label": _time_text(ordered[0]) if ordered else None,
        "last_label": _time_text(ordered[-1]) if ordered else None,
        "adjacent_forward_gaps": gaps, "backward_transitions": backwards,
        "off_hour_timestamp_rows": off_hour,
        "expected_hourly_labels_in_observed_span": len(grid) if grid else None,
        "expected_labels_absent_from_source": len(set(grid) - set(counts)) if grid else None,
        "grid_limitation": limitation,
        "dst_interpretation": (
            "Not asserted: local labels do not establish elapsed physical hours or DST policy"
        ),
    }
    return profile, grid


def period_coverage(measurements: Measurements, times: list[datetime | None],
                    grid: list[datetime]) -> list[dict]:
    result = []
    for scale in ("year", "month"):
        def key(time: datetime) -> str:
            return time.strftime("%Y" if scale == "year" else "%Y-%m")

        expected = collections.Counter(key(t) for t in grid)
        source_sets: dict[str, set] = collections.defaultdict(set)
        finite_sets: dict[str, set] = collections.defaultdict(set)
        counters: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
        for time, (state, _) in zip(times, measurements.cells):
            period = key(time) if time is not None else "unassigned"
            counters[period]["source_records"] += 1
            counters[period][state] += 1
            if time is not None:
                source_sets[period].add(time)
                if state == "finite":
                    finite_sets[period].add(time)
        for period in sorted(set(counters) | set(expected)):
            counts = counters[period]
            total = counts["source_records"]
            invalid = counts["nonnumeric"] + counts["nonfinite"]
            slots = expected[period] if grid and period != "unassigned" else None
            result.append({
                "scale": scale, "period": period, "source_records": total,
                "finite_readings": counts["finite"], "missing": counts["missing"],
                "invalid": invalid, "missing_pct_received": percent(counts["missing"], total),
                "finite_pct_received": percent(counts["finite"], total),
                "expected_local_labels": slots,
                "distinct_source_labels": len(source_sets[period]),
                "expected_labels_absent_from_source": (
                    slots - len(source_sets[period]) if slots is not None else None
                ),
                "distinct_labels_with_finite_reading": len(finite_sets[period]),
                "finite_pct_expected_local_labels": (
                    percent(len(finite_sets[period]), slots) if slots else None
                ),
            })
    return result


def _runs(indices: list[int], times: list[datetime | None], texts: list[str],
          *, hourly: bool) -> list[dict]:
    repeated = collections.Counter(time for time in times if time is not None)
    runs: list[list[int]] = []
    for index in indices:
        if hourly and (times[index] is None or repeated[times[index]] != 1):
            continue
        follows = bool(runs and index == runs[-1][-1] + 1)
        if hourly and follows:
            previous = runs[-1][-1]
            follows = times[index] - times[previous] == timedelta(hours=1)
        if follows:
            runs[-1].append(index)
        else:
            runs.append([index])
    return [{
        "start_record": run[0] + 1, "end_record": run[-1] + 1,
        "start": texts[run[0]], "end": texts[run[-1]],
        "observations": len(run),
        "label_span_hours": (
            (times[run[-1]] - times[run[0]]).total_seconds() / 3600
            if times[run[-1]] is not None and times[run[0]] is not None else None
        ),
    } for run in runs]


def zero_profile(measurements: Measurements, times: list[datetime | None],
                 texts: list[str]) -> dict:
    indices = [i for i, (state, value) in enumerate(measurements.cells)
               if state == "finite" and value == 0]
    row_runs = _runs(indices, times, texts, hourly=False)
    hourly_runs = _runs(indices, times, texts, hourly=True)

    def summarize(runs: list[dict]) -> dict:
        return {
            "sequences": len(runs),
            "maximum_observations": max((r["observations"] for r in runs), default=0),
            "sequences_at_least": {
                str(n): sum(r["observations"] >= n for r in runs) for n in (24, 72, 168)
            },
            "longest": sorted(runs, key=lambda r: (-r["observations"], r["start_record"]))[:10],
            "runs": runs,
        }

    return {
        "zero_observations": len(indices),
        "consecutive_records": summarize(row_runs),
        "continuous_local_hour_labels": summarize(hourly_runs),
        "interpretation": (
            "Observation counts and local-label continuity, not elapsed physical hours "
            "or confirmed sensor faults"
        ),
    }


def distribution_profile(measurements: Measurements, times: list[datetime | None],
                         texts: list[str], multiplier: float) -> tuple[dict, dict]:
    values = sorted(value for state, value in measurements.cells if state == "finite")
    descriptive = {
        name: quantile(values, probability) for name, probability in (
            ("p01", .01), ("p25", .25), ("median", .5), ("p75", .75), ("p99", .99)
        )
    }
    descriptive.update({
        "mean": statistics.fmean(values) if values else None,
        "population_stddev": statistics.pstdev(values) if values else None,
        "minimum": values[0] if values else None, "maximum": values[-1] if values else None,
        "finite_denominator": len(values), "quantile_method": "Linear type 7",
    })
    q1, q3 = descriptive["p25"], descriptive["p75"]
    iqr = q3 - q1 if values else None
    lower = q1 - multiplier * iqr if values else None
    upper = q3 + multiplier * iqr if values else None
    degenerate = iqr == 0 if values else None
    indices = [
        i for i, (state, value) in enumerate(measurements.cells)
        if state == "finite" and iqr is not None and iqr > 0
        and (value < lower or value > upper)
    ]
    runs = _runs(indices, times, texts, hourly=True)
    isolated = sum(r["observations"] for r in runs if r["observations"] == 1)
    assigned = sum(r["observations"] for r in runs)
    by_period = collections.Counter(
        times[i].strftime("%Y-%m") if times[i] else "unassigned" for i in indices
    )
    tails = {
        "below_p01": sum(value < descriptive["p01"] for value in values),
        "above_p99": sum(value > descriptive["p99"] for value in values),
    } if values else {"below_p01": 0, "above_p99": 0}
    examples = sorted(
        indices, key=lambda i: (-abs(measurements.cells[i][1] - descriptive["median"]), i)
    )[:20]
    outliers = {
        "method": "Strict Tukey fences; exploratory flags only",
        "iqr": iqr, "iqr_multiplier": multiplier, "lower_fence": lower, "upper_fence": upper,
        "degenerate_iqr": degenerate,
        "iqr_interpretation": (
            "Not conclusive when IQR is zero; no IQR flags assigned"
            if degenerate else "Exploratory, not confirmed errors"
        ),
        "flagged_observations": len(indices) if values and not degenerate else None,
        "flagged_pct_finite": percent(len(indices), len(values)) if values and not degenerate else None,
        "percentile_tail_counts": tails, "flagged_by_month": dict(sorted(by_period.items())),
        "continuous_flag_sequences": len(runs), "isolated_flags": isolated,
        "recurrent_flags": assigned - isolated,
        "flags_without_unique_interpretable_time": len(indices) - assigned,
        "longest_sequences": sorted(
            runs, key=lambda r: (-r["observations"], r["start_record"])
        )[:20],
        "examples": [
            {"record": i + 1, "timestamp": texts[i], "value": measurements.cells[i][1]}
            for i in examples
        ],
    }
    return descriptive, outliers


def _timezone_info(name: str | None) -> dict:
    if not name:
        return {"original": name, "resolves_in_environment": False,
                "status": "missing_field_or_empty"}
    try:
        ZoneInfo(name)
        return {"original": name, "resolves_in_environment": True, "status": "resolved"}
    except (ValueError, ZoneInfoNotFoundError):
        aliases = {
            "US/Pacific": "America/Los_Angeles", "US/Mountain": "America/Denver",
            "US/Eastern": "America/New_York", "US/Central": "America/Chicago",
        }
        return {
            "original": name, "resolves_in_environment": False,
            "status": "environment_resolution_failed_not_a_demonstrated_data_error",
            "documented_iana_alias_target": aliases.get(name),
        }


def _inventory(path: Path, expected: str | None) -> dict:
    stat = path.stat()
    digest = sha256(path)
    if expected is not None and digest != expected:
        raise CharacterizationError(f"Source SHA-256 mismatch: {path}")
    return {
        "path": str(path), "bytes": stat.st_size, "sha256": digest,
        "expected_sha256": expected, "expected_hash_matched": expected == digest if expected else None,
        "encoding": "UTF-8 (optional BOM accepted)", "delimiter": ",",
        "csv_validation": "Complete strict parse and record width validation",
    }


def _metadata(settings: Settings) -> tuple[list[dict], dict]:
    with csv_source(settings.metadata, {"building_id", "primaryspaceusage", "sqm"}) as (header, rows):
        records = [dict(zip(header, row)) for row in rows]
    ids = [row["building_id"] for row in records]
    if any(not bid for bid in ids) or len(set(ids)) != len(ids):
        raise CharacterizationError("Metadata building_id must be nonempty and unique")
    primary = collections.Counter(row["primaryspaceusage"] for row in records)
    lodging = [row for row in records if row["primaryspaceusage"] == settings.primary_usage]
    subgroup = collections.Counter(row.get("sub_primaryspaceusage") for row in lodging)
    area, missing_area, invalid_area = [], [], []
    for row in lodging:
        token = row["sqm"]
        if token in settings.missing_tokens:
            missing_area.append(row["building_id"])
            continue
        try:
            value = Decimal(token)
        except ArithmeticError:
            invalid_area.append(row["building_id"])
            continue
        if not value.is_finite() or value < 0:
            invalid_area.append(row["building_id"])
        elif settings.min_sqm <= value <= settings.max_sqm:
            area.append(row["building_id"])
    return records, {
        "columns": header, "metadata_records": len(records), "unique_building_ids": len(set(ids)),
        "primary_categories": [
            {"value": key, "buildings": value} for key, value in sorted(primary.items())
        ],
        "target_primary_usage": settings.primary_usage, "target_primary_buildings": len(lodging),
        "target_subcategories": [
            {"value": key, "buildings": value}
            for key, value in sorted(subgroup.items(), key=lambda item: str(item[0]))
        ],
        "missing_area_ids": missing_area, "invalid_area_ids": invalid_area,
        "area_cohort_building_ids": area,
        "area_bounds_inclusive_sqm": [str(settings.min_sqm), str(settings.max_sqm)],
        "selection_limitation": (
            "Supplied candidates are author-provided; no undocumented criterion "
            "reducing the area cohort to them is inferred"
        ),
    }


def analyze(settings: Settings) -> dict:
    """Read immutable sources and return deterministic metrics without writing reports."""
    original_settings = settings
    settings = deepcopy(settings)
    validate_settings(settings)
    effective = _effective_settings(settings)
    configuration_hash = sha256(settings.config_path)
    if settings.config_sha256 and settings.config_sha256 != configuration_hash:
        raise CharacterizationError("Configuration changed since loading")
    inventory = {
        name: _inventory(getattr(settings, name), settings.expected_hashes.get(name))
        for name in ("metadata", "electricity")
    }
    provenance = {
        "contract_version": _BINDING_CONTRACT,
        "canonical_serialization": _BINDING_SERIALIZATION,
        "effective_settings": effective,
        "effective_settings_sha256": _binding_digest(effective),
        "configuration": {"path": settings.config_path.resolve().as_posix(),
                          "sha256": configuration_hash},
        "sources": {name: {"path": getattr(settings, name).resolve().as_posix(),
                           "sha256": item["sha256"]} for name, item in inventory.items()},
    }
    records, global_summary = _metadata(settings)
    metadata = {row["building_id"]: row for row in records}
    detailed_ids = {candidate.building_id for candidate in settings.candidates}
    if not detailed_ids.issubset(metadata):
        raise CharacterizationError(
            f"Candidates absent from metadata: {sorted(detailed_ids - metadata.keys())}"
        )
    texts, times = [], []
    with csv_source(settings.electricity, {"timestamp"}) as (header, rows):
        if len(header) < 2:
            raise CharacterizationError("Electricity needs timestamp and at least one building")
        columns = set(header)
        if not detailed_ids.issubset(columns):
            raise CharacterizationError(
                f"Candidates absent from electricity: {sorted(detailed_ids - columns)}"
            )
        area_ids = global_summary["area_cohort_building_ids"]
        cohort = [bid for bid in area_ids if bid in columns]
        monitored = set(cohort) | detailed_ids
        measurements = {bid: Measurements(bid in detailed_ids) for bid in sorted(monitored)}
        positions = [(header.index(bid), bid) for bid in measurements]
        time_position = header.index("timestamp")
        for row in rows:
            text = row[time_position]
            texts.append(text)
            times.append(_parse_time(text, settings.timestamp_format))
            for position, bid in positions:
                measurements[bid].add(row[position], settings.missing_tokens)
    temporal, grid = temporal_profile(texts, times, settings.max_grid_slots)
    inventory["metadata"].update({"columns": global_summary["columns"], "records": len(records)})
    inventory["electricity"].update({"columns": header, "records": len(texts)})
    global_summary.update({
        "area_cohort_with_electricity": cohort,
        "area_cohort_without_electricity": [bid for bid in area_ids if bid not in columns],
        "electricity_building_columns": len(header) - 1,
    })
    summaries = {bid: measurement.summary() for bid, measurement in measurements.items()}
    sensitivity = []
    for population, ids in (
        ("area_cohort", cohort),
        ("supplied_candidates", [c.building_id for c in settings.candidates]),
    ):
        for threshold in settings.thresholds:
            included = [
                bid for bid in ids if passes_threshold(summaries[bid]["missing"], len(texts), threshold)
            ]
            sensitivity.append({
                "population": population, "threshold_missing_pct": str(threshold),
                "denominator_records": len(texts), "population_buildings": len(ids),
                "included_buildings": len(included), "included_ids": included,
                "excluded_ids": [bid for bid in ids if bid not in included],
                "invalids_in_passing_buildings": sum(summaries[bid]["invalid"] for bid in included),
            })
    funnel = [
        {"criterion": "metadata_unique_buildings", "remaining": len(records)},
        {"criterion": "exact_primary_usage", "remaining": global_summary["target_primary_buildings"]},
        {"criterion": "inclusive_area_bounds", "remaining": len(area_ids)},
        {"criterion": "electricity_column_present", "remaining": len(cohort)},
    ]
    funnel.extend(
        {"criterion": f"missing_pct_le_{row['threshold_missing_pct']}_independent_scenario",
         "remaining": row["included_buildings"]}
        for row in sensitivity if row["population"] == "area_cohort"
    )
    global_summary["selection_funnel"] = funnel
    buildings = []
    for candidate in settings.candidates:
        bid = candidate.building_id
        measurement = measurements[bid]
        stats, outliers = distribution_profile(
            measurement, times, texts, settings.iqr_multiplier
        )
        finite_times = [
            time for time, (state, _) in zip(times, measurement.cells)
            if state == "finite" and time is not None
        ]
        attrs = {
            key: metadata[bid].get(key) for key in (
                "building_id", "primaryspaceusage", "sub_primaryspaceusage",
                "site_id", "timezone", "sqm"
            )
        }
        fields_status = {
            key: "missing_field" if key not in metadata[bid]
            else "empty_csv_field" if metadata[bid][key] == "" else "provided" for key in attrs
        }
        buildings.append({
            "case_id": candidate.case_id, "building_id": bid, "author_group": candidate.group,
            "metadata": attrs, "metadata_field_status": fields_status,
            "timezone": _timezone_info(attrs["timezone"]), **summaries[bid],
            "first_finite_local_label": _time_text(min(finite_times)) if finite_times else None,
            "last_finite_local_label": _time_text(max(finite_times)) if finite_times else None,
            "statistics": stats, "coverage": period_coverage(measurement, times, grid),
            "zeros": zero_profile(measurement, times, texts), "outliers": outliers,
            "threshold_scenarios": {
                str(t): passes_threshold(summaries[bid]["missing"], len(texts), t)
                for t in settings.thresholds
            },
        })
    for name, source in inventory.items():
        if getattr(settings, name).resolve().as_posix() != provenance["sources"][name]["path"]:
            raise CharacterizationError(f"Source changed during characterization: {name}")
        post_hash = sha256(getattr(settings, name))
        if post_hash != source["sha256"]:
            raise CharacterizationError(f"Source changed during characterization: {name}")
        source["sha256_after"] = post_hash
        source["unchanged_after_analysis"] = True
    comparison = [{
        "case_id": b["case_id"], "building_id": b["building_id"],
        "author_group": b["author_group"], "missing_pct_received": b["missing_pct_received"],
        "zero_pct_finite": b["zero_pct_finite"], "zero_observations": b["zeros"]["zero_observations"],
        "max_zero_hourly_labels": b["zeros"]["continuous_local_hour_labels"]["maximum_observations"],
        "median": b["statistics"]["median"], "p99": b["statistics"]["p99"],
        "maximum": b["statistics"]["maximum"],
        "iqr_flagged_pct_finite": b["outliers"]["flagged_pct_finite"],
        "interpretation": "Exploratory signals; no automatic exclusion or confirmed sensor error",
    } for b in buildings]
    report = {
        "schema_version": "h03.2-v2", "dataset_kind": settings.kind,
        "dataset_reference": settings.reference, "measurement_unit": settings.unit,
        "unit_reference": settings.unit_reference, "inventory": inventory,
        "methodology": {
            "missing_tokens_exact": list(settings.missing_tokens),
            "received_percentage_denominator": "CSV data records, including repeated timestamps",
            "finite_percentage_denominator": "Finite numeric readings, including zeros and negatives",
            "expected_time_denominator": (
                "Distinct hourly naive local labels between source minimum and maximum; "
                "not physical hours"
            ),
            "statistics": "Finite values; population standard deviation; linear type-7 quantiles",
            "surface_bounds": "Inclusive; original metadata sqm values",
            "sensitivity": "Independent inclusive missing-percentage scenarios; not final selection",
            "candidate_group_provenance": (
                "Author-provided labels; thesis section 2.2.6 not independently read"
            ),
            "temporal_policy": (
                "Original labels retained; no UTC conversion, DST inference, deduplication or imputation"
            ),
            "parameters": {
                "min_sqm": str(settings.min_sqm), "max_sqm": str(settings.max_sqm),
                "thresholds_missing_pct": [str(t) for t in settings.thresholds],
                "timestamp_format": settings.timestamp_format,
                "iqr_multiplier": settings.iqr_multiplier,
                "max_grid_slots": settings.max_grid_slots,
            },
        },
        "global": global_summary, "temporal": temporal,
        "area_cohort_quality": [{"building_id": bid, **summaries[bid]} for bid in cohort],
        "buildings": buildings, "sensitivity": sensitivity, "comparison": comparison,
        "limitations": [
            "Thesis section 2.2.6 is not supplied to this tool; manuscript claims need author review",
            "No empirical optimality of the proposed 8% cutoff is asserted",
            "No cleaning, imputation, final selection, Bronze write, Silver or Gold",
            "Zeros, negatives and extremes are observations or flags, not automatically errors",
            "Cross-site comparability needs later analytical contracts and common observed periods",
        ],
    }
    json.dumps(report, allow_nan=False)
    _verify_configuration(original_settings, provenance, "during analysis")
    provenance["analyzed_payload_sha256"] = _analytic_digest(report)
    report["analysis_provenance"] = provenance
    return report


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    names = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False, allow_nan=False)
                if isinstance(value, (list, dict)) else value for key, value in row.items()
            })


def _academic_summary(report: dict) -> str:
    summary = report["global"]
    return "\n".join([
        "# H03.2 \u2014 Resumen acad\u00e9mico para revisi\u00f3n", "",
        f"Se inspeccionaron {summary['metadata_records']} registros de metadatos. "
        f"La categor\u00eda exacta {summary['target_primary_usage']} contiene "
        f"{summary['target_primary_buildings']} edificios.", "",
        f"El filtro inclusivo de superficie conserva {len(summary['area_cohort_building_ids'])} "
        f"edificios; {len(summary['area_cohort_with_electricity'])} tienen columna el\u00e9ctrica.", "",
        f"Los {len(report['buildings'])} identificadores facilitados se estudian como una cohorte "
        "declarada por los autores. No se ha inferido una regla adicional de selecci\u00f3n.", "",
        "Los porcentajes de ausentes usan los registros recibidos. La rejilla temporal representa "
        "etiquetas locales: no acredita horas f\u00edsicas ni una pol\u00edtica de horario estacional.", "",
        "El 8 % es un criterio metodol\u00f3gico propuesto. La sensibilidad no demuestra su optimalidad.", "",
        "Ceros, negativos y extremos no se consideran errores demostrados. La exclusi\u00f3n definitiva "
        "y la justificaci\u00f3n del caso corresponden a los autores.", "",
        "El apartado 2.2.6 no ha sido consultado por esta herramienta; su contraste queda pendiente.", "",
    ])


def write_reports(settings: Settings, output: Path, report: dict,
                  argv: list[str] | None = None) -> Path:
    output = validate_output(settings, output)
    report = deepcopy(report)
    _verify_analysis_binding(settings, report, "before report publication")
    metrics_bytes = json.dumps(
        report, ensure_ascii=False, sort_keys=True, allow_nan=False
    ).encode("utf-8")
    execution = {
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "argv": list(sys.argv if argv is None else argv), "python": sys.version,
        "git_head": _git(["rev-parse", "HEAD"], settings.config_path.parent),
        "git_status_short": _git(["status", "--short"], settings.config_path.parent),
        "tool_sha256": sha256(Path(__file__)),
        "config_sha256": report["analysis_provenance"]["configuration"]["sha256"],
        "analysis_binding_contract": _BINDING_CONTRACT,
        "effective_settings_sha256": report["analysis_provenance"]["effective_settings_sha256"],
        "analyzed_payload_sha256": report["analysis_provenance"]["analyzed_payload_sha256"],
        "config_path": str(settings.config_path), "output_path": str(output),
        "deterministic_metrics_sha256": hashlib.sha256(metrics_bytes).hexdigest(),
    }
    _reject_symlinks(output)
    output.mkdir(parents=True, exist_ok=False)
    _reject_symlinks(output)
    with (output / "report.json").open("x", encoding="utf-8") as handle:
        json.dump({"execution": execution, "metrics": report}, handle, ensure_ascii=False,
                  indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
    _write_csv(output / "source_inventory.csv", [
        {"source": name, **data} for name, data in report["inventory"].items()
    ])
    _write_csv(output / "primary_categories.csv", report["global"]["primary_categories"])
    _write_csv(output / "lodging_subcategories.csv", report["global"]["target_subcategories"])
    _write_csv(output / "selection_funnel.csv", report["global"]["selection_funnel"])
    _write_csv(output / "area_cohort_quality.csv", report["area_cohort_quality"])
    quality, coverage, zeros, outliers = [], [], [], []
    for building in report["buildings"]:
        prefix = {"case_id": building["case_id"], "building_id": building["building_id"]}
        flat = {key: value for key, value in building.items()
                if not isinstance(value, (dict, list))}
        flat.update(building["metadata"])
        flat.update(building["statistics"])
        quality.append(flat)
        coverage.extend({**prefix, **row} for row in building["coverage"])
        for method in ("consecutive_records", "continuous_local_hour_labels"):
            zeros.extend({**prefix, "sequence_method": method, **row}
                         for row in building["zeros"][method]["runs"])
        outliers.append({**prefix, **building["outliers"]})
    _write_csv(output / "building_quality.csv", quality)
    _write_csv(output / "period_coverage.csv", coverage)
    _write_csv(output / "zero_sequences.csv", zeros)
    _write_csv(output / "exploratory_outliers.csv", outliers)
    _write_csv(output / "threshold_sensitivity.csv", report["sensitivity"])
    _write_csv(output / "candidate_comparison.csv", report["comparison"])
    with (output / "execution.json").open("x", encoding="utf-8") as handle:
        json.dump(execution, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    with (output / "academic_summary.md").open("x", encoding="utf-8") as handle:
        handle.write(_academic_summary(report))
    _publish_success(settings, output, report, execution)
    return output


def _publication_failure(primary: BaseException | None, secondary: BaseException) -> BaseException:
    """Preserve the first cause and let the first cancellation control propagation."""
    if primary is None:
        return secondary
    if isinstance(primary, Exception) and not isinstance(secondary, Exception):
        secondary.__cause__ = primary
        return secondary
    primary.add_note("Secondary success-marker failure:\n"
                     + "".join(traceback.format_exception(secondary)))
    return primary


def _publish_success(settings: Settings, output: Path, report: dict, execution: dict) -> None:
    """Publish only a closed, revalidated marker, never replacing an existing name."""
    temporary = output / ".SUCCESS.json.tmp"
    failure = None
    created = False
    try:
        handle = temporary.open("x", encoding="utf-8")
        created = True
        try:
            json.dump({
                "metrics_sha256": execution["deterministic_metrics_sha256"],
                "source_hashes": {name: data["sha256"]
                                  for name, data in report["inventory"].items()},
            }, handle, sort_keys=True, allow_nan=False)
            handle.write("\n")
        except BaseException as exc:
            failure = exc
        finally:
            try:
                handle.close()
            except BaseException as exc:
                failure = _publication_failure(failure, exc)
        if failure is None:
            _verify_analysis_binding(settings, report, "while writing reports")
            _reject_symlinks(output)
            # link() is the publication boundary; a successful final name is never removed.
            os.link(temporary, output / "SUCCESS.json", follow_symlinks=False)
    except BaseException as exc:
        failure = _publication_failure(failure, exc)
    finally:
        if created:
            try:
                temporary.unlink()
            except BaseException as exc:
                failure = _publication_failure(failure, exc)
    if failure is not None:
        raise failure


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="H03.2: local read-only BDG2 characterization")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True,
                        help="New run beneath output_root/dataset.kind")
    arguments = parser.parse_args(argv)
    try:
        settings = load_settings(arguments.config)
        output = validate_output(settings, arguments.output)
        report = analyze(settings)
        command = [sys.executable, "-m", __spec__.name if __spec__ else __name__,
                   *(sys.argv[1:] if argv is None else argv)]
        write_reports(settings, output, report, argv=command)
    except (CharacterizationError, OSError, ValueError) as exc:
        print(f"Characterization failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({
        "status": "passed", "output": str(output),
        "detailed_buildings": len(report["buildings"]),
        "target_primary_buildings": report["global"]["target_primary_buildings"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
