"""Verified synthetic IQR publications and commit-ordered historical reading."""
from contextlib import contextmanager
from copy import deepcopy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F

from tfm_lakehouse.comparison.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver.silver_iqr import (
    EXPECTED_NOMINAL_HOURS, IQR_METHOD, MINIMUM_COVERAGE_COUNT,
    _annotate, summarize_electricity_iqr,
)
from tfm_lakehouse.silver.silver_snapshot_binding import transform_published_synthetic_electricity


class IQRPublicationError(silver.SilverPublicationError):
    """Publication, source identity or selection cannot be verified safely."""


_PARAMETERS = {
    "percentiles": ["0.25", "0.75"], "interpolation": "linear",
    "multiplier": "1.5", "strict_limits": True, "minimum_population": 4,
    "nominal_start": "2016-01-01", "nominal_end": "2017-12-31",
    "expected_nominal_hours": EXPECTED_NOMINAL_HOURS,
    "minimum_coverage_count": MINIMUM_COVERAGE_COUNT,
    "include_zero_negative_and_repeated": True,
}
_DECIMAL_FIELDS = ("q1", "q3", "iqr", "lower_limit", "upper_limit")
_DECIMAL_TEXT = re.compile(r"-?[0-9]+(?:\.[0-9]+)?\Z", re.ASCII)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise IQRPublicationError(message)


def _canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value) -> str:
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _bytes(path: Path) -> bytes:
    with silver._open_regular(path, os.O_RDONLY) as handle:
        return handle.read()


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "Duplicate evidence field")
        result[key] = value
    return result


def _parse(content: bytes) -> dict:
    try:
        value = json.loads(content, object_pairs_hook=_unique_object,
                           parse_constant=lambda value: _require(False, "Nonfinite JSON value"))
    except (ValueError, UnicodeError) as exc:
        raise IQRPublicationError("Unreadable or incomplete publication evidence") from exc
    _require(isinstance(value, dict), "Publication evidence must be an object")
    return value


def _read(path: Path) -> dict:
    try:
        return _parse(_bytes(path))
    except OSError as exc:
        raise IQRPublicationError("Required publication evidence is unavailable") from exc


def _identifier(value: str) -> None:
    _require(isinstance(value, str) and silver._ID.fullmatch(value) is not None,
             "Invalid publication identifier")


def _workspace(workspace: Path, config: BronzeConfig) -> Path:
    root = silver._validate_workspace(workspace, config)
    target = root / "iqr-publications"
    silver._no_links(target)
    for other in (config.raw_root.resolve(), config.lakehouse_root.resolve(),
                  root / "silver-publications"):
        _require(not target.is_relative_to(other) and not other.is_relative_to(target),
                 "IQR destination overlaps source paths")
    silver._check_tree(target)
    return root


def _fingerprint(folder: Path) -> str:
    """Hash bytes and relative names, never filesystem times."""
    silver._check_tree(folder)
    _require(folder.is_dir(), "Publication directory is unavailable")
    files = {str(path.relative_to(folder)): hashlib.sha256(_bytes(path)).hexdigest()
             for path in folder.rglob("*") if path.is_file()}
    return _digest(files)


def _commit_info(log: bytes, intent: dict) -> tuple[str, int]:
    """Decode a legacy version-zero commit without filesystem timestamps."""
    actions = [_parse(line) for line in log.splitlines() if line.strip()]
    def one(name):
        values = [action[name] for action in actions if name in action]
        _require(len(values) == 1 and isinstance(values[0], dict),
                 f"Missing or ambiguous Delta {name} action")
        return values[0]
    info, metadata, protocol = one("commitInfo"), one("metaData"), one("protocol")
    _require(isinstance(metadata.get("configuration", {}), dict)
             and isinstance(protocol.get("writerFeatures", []), list), "Invalid Delta feature metadata")
    _require("inCommitTimestamp" not in info
             and metadata.get("configuration", {}).get("delta.enableInCommitTimestamps") != "true"
             and "inCommitTimestamp" not in protocol.get("writerFeatures", []),
             "In-commit-timestamp tables are unsupported by this publication contract")
    timestamp = info.get("timestamp")
    _require(type(timestamp) is int and 0 <= timestamp <= 2**63 - 1,
             "Missing or invalid persisted Delta commit timestamp")
    _require(info.get("userMetadata") == _canonical(intent), "Commit intent metadata differs")
    _require(isinstance(metadata.get("id"), str) and bool(metadata["id"]),
             "Commit table identity is missing")
    return metadata["id"], timestamp


def _commit(spark, target: Path, intent: dict) -> dict:
    """Bind the persisted timestamp to the verified physical table and version.

    Delta 3.3.3 substitutes log mtime in history().timestamp for legacy tables.
    Only the timestamp serialized by the writer in commitInfo is used here.
    ICT-enabled tables are explicitly unsupported by this contract version.
    """
    silver._check_tree(target)
    log = _bytes(target / "_delta_log" / "00000000000000000000.json")
    table_id, timestamp = _commit_info(log, intent)
    table = DeltaTable.forPath(spark, str(target))
    history = table.history().select("version", "userMetadata").limit(2).collect()
    _require(len(history) == 1 and history[0].version == 0
             and history[0].userMetadata == _canonical(intent), "Inconsistent Delta history")
    details = table.detail().select("id").collect()
    _require(len(details) == 1 and isinstance(details[0].id, str)
             and details[0].id == table_id, "Physical Delta identity differs")
    return {"delta_table_id": details[0].id, "delta_version": 0,
            "commit_timestamp_ms": timestamp, "commit_sha256": hashlib.sha256(log).hexdigest()}


def _stable_silver(identity: dict, config: BronzeConfig) -> None:
    _require(_fingerprint(Path(identity["table_path"]).parent) == identity["files_sha256"],
             "Silver source changed during verification")
    _, digest = silver._manifest(config, identity["source_run_id"])
    _require(digest == identity["manifest_sha256"], "Bronze source manifest changed")


def _verify_silver(spark, config: BronzeConfig, root: Path, publication_id: str):
    _identifier(publication_id)
    folder = root / "silver-publications" / publication_id
    before = _fingerprint(folder)
    intent, completed = _read(folder / "intent.json"), _read(folder / "completed.json")
    run_id = intent.get("source_run_id")
    _identifier(run_id)
    for name in config.sources:
        silver._check_tree(config.table_path(name))
    report, manifest_hash = silver._manifest(config, run_id)
    assessment, expected = transform_published_synthetic_electricity(spark, config, run_id)
    electricity = next(item for item in report["tables"] if item["name"] == "electricity")
    rows = electricity.get("output_rows")
    _require(type(rows) is int and rows > 0, "Invalid Bronze electricity count")
    expected_intent = {
        "publication_id": publication_id, "source_run_id": run_id,
        "manifest_sha256": manifest_hash, "source_tables": silver._table_identity(report),
        "transform_version": silver._TRANSFORM, "input_rows": rows, "output_rows": rows * 16,
    }
    _require(intent == expected_intent and assessment.get("manifest_sha256") == manifest_hash
             and assessment.get("source_tables") == intent["source_tables"]
             and assessment.get("snapshot_binding") == "h02_version_metadata_schema_count_checked",
             "Silver intent or Bronze provenance differs")
    target = folder / "table"
    verified = silver._validate_output(spark, target, expected, intent)
    _require(completed == verified, "Silver completion evidence differs from committed output")
    commit = _commit(spark, target, intent)
    _require(commit["delta_table_id"] == completed["delta_table_id"], "Silver table ID differs")
    identity = {**completed, **commit, "files_sha256": before,
                "completed_sha256": hashlib.sha256(_bytes(folder / "completed.json")).hexdigest()}
    _stable_silver(identity, config)
    frame = spark.read.format("delta").option("versionAsOf", 0).load(str(target))
    return identity, frame


def _records(frame: DataFrame) -> list[dict]:
    rows = [row.asDict() for row in frame.limit(17).collect()]
    _require(len(rows) == 16 and {row["building_id"] for row in rows} == set(APPROVED_BUILDINGS),
             "IQR output must contain exactly the approved 16 unique buildings")
    for row in rows:
        for name in _DECIMAL_FIELDS:
            value = row[name]
            _require(value is None or (isinstance(value, str) and _DECIMAL_TEXT.fullmatch(value)),
                     "Invalid exact decimal representation")
        _require(row["iqr_method"] == IQR_METHOD, "Statistical method differs")
    return sorted(rows, key=lambda row: row["building_id"])


@contextmanager
def _calculation(spark, config, root, publication_id, *, calculate=True):
    identity, source = _verify_silver(spark, config, root, publication_id)
    source = source.persist(StorageLevel.DISK_ONLY)
    expected = None
    try:
        _require(source.count() == identity["output_rows"], "Silver snapshot count changed")
        _stable_silver(identity, config)
        records = None
        if calculate:
            expected = summarize_electricity_iqr(source).persist(StorageLevel.DISK_ONLY)
            records = _records(expected)
        _stable_silver(identity, config)
        yield identity, source, expected, records
    finally:
        if expected is not None:
            expected.unpersist()
        source.unpersist()


def _intent(identity, publication_id, records) -> dict:
    return {"manifest_contract_version": 1, "logical_output": "silver_iqr_limits",
            "publication_id": publication_id, "silver": identity, "method_version": IQR_METHOD,
            "parameters": deepcopy(_PARAMETERS), "buildings": list(APPROVED_BUILDINGS),
            "output_rows": 16, "result_sha256": _digest(records)}


def _publication_frame(expected, intent):
    identity = intent["silver"]
    return (expected.withColumn("_iqr_publication_id", F.lit(intent["publication_id"]))
            .withColumn("_silver_publication_id", F.lit(identity["publication_id"]))
            .withColumn("_silver_delta_table_id", F.lit(identity["delta_table_id"]))
            .withColumn("_silver_delta_version", F.lit(identity["delta_version"]).cast("long"))
            .withColumn("_silver_identity_sha256", F.lit(_digest(identity))))


def _folder_layout(folder, table_name):
    _require(all(path.name in {"intent.json", "completed.json", table_name}
                 for path in folder.iterdir()), "Unknown content in publication destination")


def _verify_output(spark, target, expected, intent):
    commit = _commit(spark, target, intent)
    actual = spark.read.format("delta").option("versionAsOf", 0).load(str(target))
    _require(silver._same_schema(expected.schema, actual.schema), "IQR committed schema differs")
    rows = _records(actual)
    _require(actual.count() == 16 and not expected.exceptAll(actual).limit(1).count()
             and not actual.exceptAll(expected).limit(1).count(),
             "IQR committed multiset or counts differ")
    statistical = [{key: value for key, value in row.items()
                    if not key.startswith(("_iqr_", "_silver_"))} for row in rows]
    _require(_digest(statistical) == intent["result_sha256"], "IQR result fingerprint differs")
    return {"manifest_contract_version": 1, "status": "passed", "intent": intent,
            **commit, "table_path": str(target), "schema": actual.schema.jsonValue(),
            "output_rows": 16, "reconciliation": "schema_count_multiset_passed"}, actual


@contextmanager
def _lock(root):
    namespace = root / "iqr-publications"
    silver._no_links(namespace)
    namespace.mkdir(exist_ok=True)
    with silver._open_regular(namespace / ".publication.lock", os.O_RDWR | os.O_CREAT) as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise IQRPublicationError("Another local IQR publication is active") from exc
        try:
            yield namespace
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def publish_synthetic_iqr(spark: SparkSession, config: BronzeConfig, silver_publication_id: str,
                          publication_id: str, workspace: Path, *,
                          method_version: str = IQR_METHOD) -> dict:
    """Publish or reconcile one explicit synthetic Silver/method/IQR identity.

    Sources are pinned, materialized and fingerprinted before finalization.
    Only cooperative local IQR writers are serialized. No uncertain state is
    deleted, overwritten or automatically retried. Existing Silver is read only.
    """
    _identifier(silver_publication_id)
    _identifier(publication_id)
    _require(method_version == IQR_METHOD, "Unsupported IQR method version")
    root = _workspace(workspace, config)
    with _lock(root) as namespace, _calculation(spark, config, root, silver_publication_id) as data:
        identity, _, expected, records = data
        intent = _intent(identity, publication_id, records)
        folder = namespace / publication_id
        silver._no_links(folder)
        if not folder.exists():
            folder.mkdir()
            silver._exclusive_json(folder / "intent.json", intent)
        else:
            silver._check_tree(folder)
            _folder_layout(folder, "silver_iqr_limits")
            _require(_read(folder / "intent.json") == intent, "IQR publication ID conflicts with intent")
        target, completed = folder / "silver_iqr_limits", folder / "completed.json"
        output = _publication_frame(expected, intent)
        if not target.exists():
            _require(not completed.exists(), "IQR evidence exists without a Delta table")
            (output.write.format("delta").mode("errorifexists")
             .option("userMetadata", _canonical(intent)).save(str(target)))
        result, _ = _verify_output(spark, target, output, intent)
        _stable_silver(identity, config)
        if completed.exists():
            _require(_read(completed) == result, "IQR completion evidence conflicts with Delta")
        else:
            silver._exclusive_json(completed, result)
        _stable_silver(identity, config)
        return result


def _candidates(namespace):
    silver._check_tree(namespace)
    if not namespace.exists():
        return []
    result = []
    for folder in namespace.iterdir():
        if folder.name == ".publication.lock":
            _require(folder.is_file(), "Invalid publication lock entry")
            continue
        _require(folder.is_dir() and silver._ID.fullmatch(folder.name), "Unknown publication entry")
        _require((folder / "intent.json").is_file(), "Publication intent is missing")
        if (folder / "completed.json").exists():
            result.append(folder.name)
    return result


def _latest(items):
    _require(bool(items), "No finalized verified publication is available")
    latest = max(item["commit_timestamp_ms"] for item in items)
    selected = [item for item in items if item["commit_timestamp_ms"] == latest]
    _require(len(selected) == 1, "Latest publication commit timestamps are tied")
    return selected[0]


def read_synthetic_iqr_pair(spark: SparkSession, config: BronzeConfig, workspace: Path, *,
                            silver_publication_id: str | None = None,
                            iqr_publication_id: str | None = None,
                            ) -> tuple[dict, DataFrame, DataFrame | None]:
    """Verify and read a current or explicit historical Silver/IQR pair.

    Finalized corrupt entries fail selection rather than hiding a newer result.
    Incomplete entries are ineligible. Returned DataFrames remain lazy and need
    retained, unchanged source files after return; no caches are transferred.
    """
    root = _workspace(workspace, config)
    _require(iqr_publication_id is None or silver_publication_id is not None,
             "Explicit IQR selection requires an explicit Silver identity")
    if silver_publication_id is None:
        verified = [_verify_silver(spark, config, root, name)[0]
                    for name in _candidates(root / "silver-publications")]
        silver_publication_id = _latest(verified)["publication_id"]
    with _calculation(spark, config, root, silver_publication_id, calculate=False) as data:
        identity, cached_source, expected, _ = data
        if iqr_publication_id is not None:
            _identifier(iqr_publication_id)
            names = [iqr_publication_id]
        else:
            names = _candidates(root / "iqr-publications")
        compatible = []
        for name in names:
            folder = root / "iqr-publications" / name
            before = _fingerprint(folder)
            _folder_layout(folder, "silver_iqr_limits")
            intent = _read(folder / "intent.json")
            parent = intent.get("silver")
            _require(isinstance(parent, dict) and isinstance(parent.get("publication_id"), str),
                     "IQR parent identity is missing")
            _identifier(parent["publication_id"])
            _require(parent.get("table_path") == str(
                root / "silver-publications" / parent["publication_id"] / "table")
                and isinstance(parent.get("delta_table_id"), str)
                and parent.get("delta_version") == 0
                and isinstance(parent.get("files_sha256"), str), "IQR parent identity is incomplete")
            completed = _read(folder / "completed.json")
            _require(completed.get("intent") == intent and completed.get("status") == "passed",
                     "IQR finalized intent differs")
            commit = _commit(spark, folder / "silver_iqr_limits", intent)
            _require(all(completed.get(key) == value for key, value in commit.items()),
                     "IQR finalized commit identity differs")
            if parent != identity:
                _require(iqr_publication_id is None
                         and parent.get("publication_id") != silver_publication_id,
                         "IQR belongs to a different or changed Silver publication")
                continue
            if expected is None:
                expected = summarize_electricity_iqr(cached_source)
            records = _records(expected)
            _require(intent == _intent(identity, name, records), "IQR method or manifest differs")
            output = _publication_frame(expected, intent)
            result, _ = _verify_output(spark, folder / "silver_iqr_limits", output, intent)
            _require(completed == result,
                     "IQR completion evidence differs from Delta")
            _require(before == _fingerprint(folder), "IQR changed during verification")
            compatible.append({**result, "files_sha256": before})
        _stable_silver(identity, config)
        selected = _latest(compatible) if compatible else None
        state = {"silver": identity, "iqr": selected, "iqr_status": "pending", "warnings": []}
        limits = None
        if selected is not None:
            limits = spark.read.format("delta").option("versionAsOf", 0).load(selected["table_path"])
            rejected = [row["building_id"] for row in _records(limits) if not row["iqr_accepted"]]
            state.update(iqr_status="not_accepted" if rejected else "available",
                         warnings=["iqr_not_accepted"] if rejected else [],
                         unaccepted_buildings=rejected)
        source = spark.read.format("delta").option("versionAsOf", 0).load(identity["table_path"])
        if selected is not None:
            _require(_fingerprint(Path(selected["table_path"]).parent) == selected["files_sha256"],
                     "IQR changed before returning the selected pair")
        _stable_silver(identity, config)
        return state, source, limits


def annotate_synthetic_iqr_pair(spark: SparkSession, config: BronzeConfig, workspace: Path,
                               **selection) -> tuple[dict, DataFrame]:
    """Annotate a verified pair using persisted limits and the existing engine."""
    state, source, limits = read_synthetic_iqr_pair(spark, config, workspace, **selection)
    _require(limits is not None, "IQR is pending for the selected Silver publication")
    annotated = _annotate(source, limits)
    _require(annotated.count() == state["silver"]["output_rows"], "Annotated cardinality differs")
    original = annotated.select(*source.columns)
    _require(not original.exceptAll(source).limit(1).count()
             and not source.exceptAll(original).limit(1).count(), "Annotated Silver multiset differs")
    _stable_silver(state["silver"], config)
    _require(_fingerprint(Path(state["iqr"]["table_path"]).parent) == state["iqr"]["files_sha256"],
             "Persisted IQR changed during annotation")
    return state, annotated
