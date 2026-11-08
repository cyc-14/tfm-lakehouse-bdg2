"""Compare explicitly supplied electricity records without reading source files."""
from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
import re


class ComparisonError(ValueError):
    """Raised for an invalid comparison schema or configuration."""


@dataclass(frozen=True)
class Cell:
    """A value and its representation as supplied by a future source adapter.

    ``exact`` requires an integer or Decimal whose fidelity is established by
    the caller. ``parsed_null`` records a null produced by a parser, for which
    the original serialized CSV token is unavailable. Formula caches and binary
    floats are deliberately never treated as exact evidence.
    """

    kind: str
    value: object


_NUMBER = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z")
_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\Z")


def _classify(cell: Cell) -> tuple[str, Decimal | None]:
    if not isinstance(cell, Cell):
        raise ComparisonError("Every measurement must be a Cell")
    value = cell.value
    if cell.kind in {"null", "parsed_null"} and value is None:
        return "missing", None
    if cell.kind == "text" and isinstance(value, str):
        if value in {"", "NaN"}:
            return "missing", None
        if not _NUMBER.fullmatch(value):
            return "invalid", None
        try:
            number = Decimal(value)
        except InvalidOperation:
            return "invalid", None
        return ("number", number) if number.is_finite() else ("invalid", None)
    if cell.kind == "exact" and (type(value) is int or isinstance(value, Decimal)):
        number = Decimal(value)
        return ("number", number) if number.is_finite() else ("invalid", None)
    if cell.kind == "float" and type(value) is float:
        return "ambiguous", None
    if cell.kind == "formula" and isinstance(value, str):
        return "ambiguous", None
    raise ComparisonError("Invalid Cell kind or value")


def _valid_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not _TIMESTAMP.fullmatch(value):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return False
    return True


def _describe(cell: Cell) -> dict[str, str]:
    # repr records the supplied representation; it is never used as numeric proof.
    return {"kind": cell.kind, "value_repr": repr(cell.value)}


def _validated_rows(records, fields: set[str], label: str) -> list[Mapping]:
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise ComparisonError(f"{label} records must be a sequence")
    rows = []
    for index, row in enumerate(records):
        if not isinstance(row, Mapping) or set(row) != fields:
            raise ComparisonError(f"Invalid {label} columns at row {index}")
        for field in fields - {"timestamp"}:
            _classify(row[field])
        rows.append(row)
    return rows


def compare_electricity(
    csv_records: Sequence[Mapping[str, object]],
    excel_records: Sequence[Mapping[str, object]],
    building_ids: Sequence[str],
    *,
    expected_rows: int = 17_544,
    expected_buildings: int = 16,
    example_limit: int = 20,
) -> dict:
    """Compare in-memory records, preserving multiplicity and source representations.

    Row counts and building counts may be reduced explicitly for synthetic tests.
    A verified result requires matching counts, coverage and order, and no invalid
    cells, float/formula ambiguity, duplicate timestamps, or mismatched values.
    Duplicate groups are compared as multisets of whole rows, never by columns.
    """
    if (not isinstance(building_ids, Sequence) or isinstance(building_ids, (str, bytes))
            or any(not isinstance(bid, str) or not bid or bid.strip() != bid
                   or bid.casefold() == "timestamp" for bid in building_ids)):
        raise ComparisonError("Invalid building identifiers")
    ids = tuple(building_ids)
    if len({bid.casefold() for bid in ids}) != len(ids):
        raise ComparisonError("Duplicate or ambiguous building identifiers")
    if (type(expected_rows) is not int or expected_rows <= 0
            or type(expected_buildings) is not int or expected_buildings <= 0
            or len(ids) != expected_buildings):
        raise ComparisonError("Invalid expected dimensions or building count")
    if type(example_limit) is not int or example_limit < 0:
        raise ComparisonError("example_limit must be a nonnegative integer")

    fields = {"timestamp", *ids}
    sources = {
        "csv": _validated_rows(csv_records, fields, "csv"),
        "excel": _validated_rows(excel_records, fields, "excel"),
    }
    groups: dict[str, dict[str, list[Mapping]]] = {}
    source_stats = {}
    examples: list[dict] = []

    def add_example(example: dict) -> None:
        if len(examples) < example_limit:
            examples.append(example)

    for source, rows in sources.items():
        by_time: dict[str, list[Mapping]] = defaultdict(list)
        classes: Counter = Counter()
        missing_representations: Counter = Counter()
        invalid_timestamps = 0
        non_hourly_transitions = 0
        previous_time = None
        for index, row in enumerate(rows):
            timestamp = row["timestamp"]
            if _valid_timestamp(timestamp):
                by_time[timestamp].append(row)
                current_time = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                if (previous_time is not None
                        and current_time - previous_time != timedelta(hours=1)):
                    non_hourly_transitions += 1
                    add_example({"type": "non_hourly_transition", "source": source,
                                 "row": index, "timestamp": timestamp})
                previous_time = current_time
            else:
                invalid_timestamps += 1
                previous_time = None
                add_example({"type": "invalid_timestamp", "source": source,
                             "row": index, "timestamp_repr": repr(timestamp)})
            for bid in ids:
                category, _ = _classify(row[bid])
                classes[category] += 1
                if category == "missing":
                    cell = row[bid]
                    label = ({"": "empty_text", "NaN": "nan_text"}[cell.value]
                             if cell.kind == "text" else cell.kind)
                    missing_representations[label] += 1
                if category in {"invalid", "ambiguous"}:
                    add_example({"type": category, "source": source, "row": index,
                                 "building_id": bid, "cell": _describe(row[bid])})
        groups[source] = by_time
        source_stats[source] = {
            "rows": len(rows), "expected_rows": expected_rows,
            "invalid_timestamps": invalid_timestamps,
            "non_hourly_transitions": non_hourly_transitions,
            "duplicate_timestamp_groups": sum(len(group) > 1 for group in by_time.values()),
            "cell_classes": {name: classes[name] for name in
                             ("missing", "number", "invalid", "ambiguous")},
            "missing_representations": {name: missing_representations[name] for name in
                                        ("null", "parsed_null", "empty_text", "nan_text")},
        }

    csv_groups, excel_groups = groups["csv"], groups["excel"]
    csv_coverage = Counter({key: len(value) for key, value in csv_groups.items()})
    excel_coverage = Counter({key: len(value) for key, value in excel_groups.items()})
    missing_in_excel = csv_coverage - excel_coverage
    additional_in_excel = excel_coverage - csv_coverage
    for kind, counts in (("missing_in_excel", missing_in_excel),
                         ("additional_in_excel", additional_in_excel)):
        for timestamp, count in sorted(counts.items()):
            add_example({"type": kind, "timestamp": timestamp, "occurrences": count})

    outcomes: Counter = Counter()
    missing_representation_differences = 0
    duplicate_mismatches = 0
    duplicate_alignment_ambiguities = 0

    def compare_pair(timestamp: str, left: Mapping, right: Mapping) -> None:
        nonlocal missing_representation_differences
        for bid in ids:
            a, b = left[bid], right[bid]
            ca, na = _classify(a)
            cb, nb = _classify(b)
            if "ambiguous" in (ca, cb):
                outcome = "representation_ambiguity"
            elif ca == "missing" and cb == "missing":
                outcome = "missing_equivalent"
                if a != b:
                    missing_representation_differences += 1
                    add_example({"type": "missing_representation_difference",
                                 "timestamp": timestamp, "building_id": bid,
                                 "csv": _describe(a), "excel": _describe(b)})
            elif "missing" in (ca, cb):
                outcome = "missingness_mismatch"
            elif "invalid" in (ca, cb):
                outcome = "invalid_non_missing"
            elif na != nb:
                outcome = "numeric_mismatch"
            elif (a.kind == b.kind and type(a.value) is type(b.value)
                  and repr(a.value) == repr(b.value)):
                outcome = "exact_equal"
            else:
                outcome = "numeric_representation_equivalent"
            outcomes[outcome] += 1
            if outcome in {"representation_ambiguity", "missingness_mismatch",
                           "invalid_non_missing", "numeric_mismatch"}:
                add_example({"type": outcome, "timestamp": timestamp, "building_id": bid,
                             "csv": _describe(a), "excel": _describe(b)})

    def signature(row: Mapping) -> tuple | None:
        result = []
        for bid in ids:
            category, number = _classify(row[bid])
            if category not in {"missing", "number"}:
                return None
            result.append((category, number))
        return tuple(result)

    for timestamp in sorted(csv_groups.keys() & excel_groups.keys()):
        left, right = csv_groups[timestamp], excel_groups[timestamp]
        if len(left) == len(right) == 1:
            compare_pair(timestamp, left[0], right[0])
            continue
        duplicate_alignment_ambiguities += 1
        left_keys = [signature(row) for row in left]
        right_keys = [signature(row) for row in right]
        if None in left_keys or None in right_keys:
            add_example({"type": "duplicate_alignment_ambiguity", "timestamp": timestamp,
                         "reason": "invalid_or_ambiguous_cell"})
            continue
        remaining: dict[tuple, list[Mapping]] = defaultdict(list)
        for key, row in zip(right_keys, right):
            remaining[key].append(row)
        matched = 0
        unmatched_left = []
        for key, row in zip(left_keys, left):
            if remaining[key]:
                remaining[key].pop(0)
                matched += 1
            else:
                unmatched_left.append(row)
        if matched != len(left) or matched != len(right):
            duplicate_mismatches += 1
            unmatched_right = [row for rows in remaining.values() for row in rows]
            add_example({"type": "duplicate_row_multiset_mismatch", "timestamp": timestamp,
                         "csv_rows": len(left), "excel_rows": len(right),
                         "matched_complete_rows": matched,
                         "unmatched_csv_rows": len(unmatched_left),
                         "unmatched_excel_rows": len(unmatched_right),
                         "csv_sample": ({bid: _describe(unmatched_left[0][bid]) for bid in ids}
                                        if unmatched_left else None),
                         "excel_sample": ({bid: _describe(unmatched_right[0][bid]) for bid in ids}
                                          if unmatched_right else None)})
        else:
            add_example({"type": "duplicate_alignment_ambiguity", "timestamp": timestamp,
                         "reason": "repeated_nominal_timestamp"})

    order_equal = ([row["timestamp"] for row in sources["csv"]]
                   == [row["timestamp"] for row in sources["excel"]])
    if not order_equal:
        add_example({"type": "timestamp_order_mismatch"})
    outcome_names = ("exact_equal", "numeric_representation_equivalent", "missing_equivalent",
                     "numeric_mismatch", "missingness_mismatch", "invalid_non_missing",
                     "representation_ambiguity")
    outcome_totals = {name: outcomes[name] for name in outcome_names}
    compared = sum(outcomes.values())
    verified = (
        all(stat["rows"] == expected_rows and not stat["invalid_timestamps"]
            and not stat["non_hourly_transitions"]
            and not stat["duplicate_timestamp_groups"]
            and not stat["cell_classes"]["invalid"]
            and not stat["cell_classes"]["ambiguous"] for stat in source_stats.values())
        and not missing_in_excel and not additional_in_excel and order_equal
        and not duplicate_mismatches and not duplicate_alignment_ambiguities
        and not any(outcomes[name] for name in outcome_names[3:])
        and compared == expected_rows * expected_buildings
    )
    return {
        "verified": verified,
        "verified_scope": "supplied_records_only",
        "dimension_profile": ("bdg2" if expected_rows == 17_544 and expected_buildings == 16
                              else "custom"),
        "expected_positions_per_source": expected_rows * expected_buildings,
        "sources": source_stats,
        "timestamp_coverage_equal": (
            None if any(stat["invalid_timestamps"] for stat in source_stats.values())
            else csv_coverage == excel_coverage
        ),
        "timestamp_order_equal": order_equal,
        "missing_timestamp_occurrences_in_excel": sum(missing_in_excel.values()),
        "additional_timestamp_occurrences_in_excel": sum(additional_in_excel.values()),
        "duplicate_row_multiset_mismatches": duplicate_mismatches,
        "duplicate_alignment_ambiguities": duplicate_alignment_ambiguities,
        "compared_missing_representation_differences": missing_representation_differences,
        "compared_positions": compared,
        "uncompared_positions": {name: len(rows) * len(ids) - compared
                                 for name, rows in sources.items()},
        "outcomes": outcome_totals,
        "outcomes_scope": "compared_positions_only",
        "examples": examples,
    }
