"""Isolated synthetic Delta captures with explicit ownership and no recovery."""
from contextlib import contextmanager
import fcntl
import hashlib
import logging
import os
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql.types import StructType

from tfm_lakehouse.silver import contextual_reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_identity
from tfm_lakehouse.silver.weather_integrity import weather_tree_fingerprint

CAPTURE_VERSION = 1
_MAX_COMMIT_LINE = 8 * 1024 * 1024
_LOGGER = logging.getLogger(__name__)


def _record_cleanup_failure(primary, error):
    try:
        failures = list(getattr(primary, "_h06_cleanup_failures", []))
        failures.append(error)
        BaseException.__setattr__(primary, "_h06_cleanup_failures", failures)
    except BaseException:
        pass
    try:
        primary.add_note(f"Capture cleanup failed: {error!r}")
    except BaseException:
        try:
            _LOGGER.error("Capture cleanup failed", exc_info=(type(error), error, error.__traceback__))
        except BaseException:
            pass


def _namespace(workspace, config):
    root = iqr._workspace(workspace, config)
    target = root / "weather-captures"
    silver._no_links(target)
    for other in (config.raw_root.resolve(), config.lakehouse_root.resolve(),
                  root / "silver-publications", root / "contextual-publications",
                  root / "iqr-publications", root / "weather-publications",
                  root / "weather-enriched-publications"):
        silver._no_links(other)
        iqr._require(not target.is_relative_to(other) and not other.is_relative_to(target),
                     "Capture destination overlaps another product")
    silver._check_tree(target)
    return root, target


@contextmanager
def _lock(namespace):
    namespace.mkdir(exist_ok=True)
    silver._check_tree(namespace)
    handle = silver._open_regular(namespace / ".capture.lock", os.O_RDWR | os.O_CREAT)
    primary = None
    acquired = False
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError as exc:
            raise iqr.IQRPublicationError("Another capture operation is active") from exc
        yield
    except BaseException as exc:
        primary = exc
        raise
    finally:
        failures = []
        if acquired:
            try:
                fcntl.flock(handle, fcntl.LOCK_UN)
            except BaseException as exc:
                failures.append(exc)
        try:
            handle.close()
        except BaseException as exc:
            failures.append(exc)
        if primary is not None:
            for exc in failures:
                _record_cleanup_failure(primary, exc)
        elif failures:
            for exc in failures[1:]:
                _record_cleanup_failure(failures[0], exc)
            raise failures[0]


def _commit(spark, table, intent):
    """Verify the only Delta commit while bounding individual log-line reads."""
    log = table / "_delta_log" / "00000000000000000000.json"
    digest = hashlib.sha256()
    records = {}
    with silver._open_regular(log, os.O_RDONLY) as handle:
        while True:
            line = handle.readline(_MAX_COMMIT_LINE + 1)
            if not line:
                break
            iqr._require(len(line) <= _MAX_COMMIT_LINE, "Delta commit line exceeds limit")
            digest.update(line)
            if not line.strip():
                continue
            action = iqr._parse(line)
            for name in ("commitInfo", "metaData", "protocol"):
                if name in action:
                    iqr._require(name not in records and isinstance(action[name], dict), "Invalid Delta commit")
                    records[name] = action[name]
    iqr._require(set(records) == {"commitInfo", "metaData", "protocol"}, "Incomplete Delta commit")
    info, metadata, protocol = (records[n] for n in ("commitInfo", "metaData", "protocol"))
    iqr._require(info.get("userMetadata") == iqr._canonical(intent), "Capture commit identity differs")
    iqr._require(type(info.get("timestamp")) is int and info["timestamp"] >= 0,
                 "Invalid capture commit timestamp")
    configuration, features = metadata.get("configuration", {}), protocol.get("writerFeatures", [])
    iqr._require(isinstance(configuration, dict) and isinstance(features, list), "Invalid Delta protocol")
    iqr._require("inCommitTimestamp" not in info and configuration.get("delta.enableInCommitTimestamps") != "true"
                 and "inCommitTimestamp" not in features, "Unsupported Delta timestamp protocol")
    table_id = metadata.get("id")
    iqr._require(isinstance(table_id, str) and bool(table_id), "Capture table identity missing")
    delta = DeltaTable.forPath(spark, str(table))
    history = delta.history().select("version", "userMetadata").limit(2).collect()
    details = delta.detail().select("id").limit(2).collect()
    iqr._require(len(history) == 1 and history[0].version == 0 and
                 history[0].userMetadata == iqr._canonical(intent), "Capture Delta history differs")
    iqr._require(len(details) == 1 and details[0].id == table_id, "Capture table ID differs")
    return {"delta_table_id": table_id, "delta_version": 0,
            "commit_timestamp_ms": info["timestamp"], "commit_sha256": digest.hexdigest()}


def _logical_frame(spark, physical, schema_json):
    expected = StructType.fromJson(schema_json)
    normal = expected.jsonValue()
    for field in normal["fields"]:
        field["nullable"] = True
    iqr._require(iqr._canonical(physical.schema.jsonValue()) == iqr._canonical(normal),
                 "Capture physical schema differs")
    # Validate the original logical nullability in distributed workers.
    return spark.createDataFrame(physical.rdd, expected, verifySchema=True)


def _same_multiset(left, right):
    iqr._require(not left.exceptAll(right).limit(1).count() and
                 not right.exceptAll(left).limit(1).count(), "Capture projection multiset differs")


def _verify(spark, config, root, folder, intent, marker=None):
    silver._check_tree(folder)
    iqr._require(folder.is_dir(), "Capture is unavailable")
    table = folder / "table"
    before = weather_tree_fingerprint(table)
    if marker is not None:
        iqr._require(marker.get("physical_tree_sha256") == before, "Capture tree changed")
    commit = _commit(spark, table, intent)
    if marker is not None:
        iqr._require(all(iqr._canonical(marker.get(name)) == iqr._canonical(value)
                         for name, value in commit.items()), "Capture commit evidence differs")
    physical = spark.read.format("delta").option("versionAsOf", 0).load(str(table))
    candidate = _logical_frame(spark, physical, intent["weather_identity"]["schema"])
    parent = intent["weather_identity"]
    weather_identity.verify_synthetic_weather_identity(
        spark, config, parent["silver"]["publication_id"], parent["contextual_publication_id"],
        parent["publication_id"], root, parent, candidate)
    context_marker, context = contextual_reader.read_synthetic_contextual(
        spark, config, parent["contextual_publication_id"], root)
    iqr._require(iqr._canonical(context_marker) == iqr._canonical(parent["contextual"]),
                 "Contextual source changed")
    electrical_identity, electrical = iqr._verify_silver(spark, config, root, parent["silver"]["publication_id"])
    iqr._require(iqr._canonical(electrical_identity) == iqr._canonical(parent["silver"]), "Silver source changed")
    _same_multiset(candidate.select(*context.columns), context)
    _same_multiset(candidate.select(*electrical.columns), electrical)
    iqr._stable_silver(electrical_identity, config)
    context_after, _ = contextual_reader.read_synthetic_contextual(
        spark, config, parent["contextual_publication_id"], root)
    iqr._require(iqr._canonical(context_after) == iqr._canonical(parent["contextual"]),
                 "Contextual source changed")
    iqr._require(weather_tree_fingerprint(config.table_path("weather")) == parent["weather_files_sha256"],
                 "Weather source changed")
    iqr._require(weather_tree_fingerprint(table) == before, "Capture tree changed")
    result = {"contract_version": CAPTURE_VERSION, "capture_id": intent["capture_id"],
              "owner_id": intent["owner_id"], "intent": intent, **commit,
              "physical_tree_sha256": before, "physical_schema": physical.schema.jsonValue(),
              "logical_schema": parent["schema"], "reconciliation": "full_context_electric_passed"}
    if marker is not None:
        iqr._require(iqr._canonical(marker) == iqr._canonical(result), "Capture marker differs")
    return result


def create_synthetic_weather_capture(spark, config, silver_id, contextual_id, publication_id,
                                     capture_id, owner_id, workspace):
    """Reserve once and materialize a verified capture; retain all failure evidence."""
    iqr._identifier(capture_id)
    iqr._identifier(owner_id)
    root, namespace = _namespace(workspace, config)
    with _lock(namespace):
        folder = namespace / capture_id
        silver._no_links(folder)
        iqr._require(not folder.exists(), "Capture reservation already exists")
        parent, frame = weather_identity.build_synthetic_weather_identity(
            spark, config, silver_id, contextual_id, publication_id, root)
        intent = {"contract_version": CAPTURE_VERSION, "capture_id": capture_id,
                  "owner_id": owner_id, "weather_identity": parent}
        folder.mkdir()
        silver._exclusive_json(folder / "intent.json", intent)
        table = folder / "table"
        (frame.write.format("delta").mode("errorifexists")
         .option("userMetadata", iqr._canonical(intent)).save(str(table)))
        before = weather_tree_fingerprint(folder)
        result = _verify(spark, config, root, folder, intent)
        iqr._require(iqr._canonical(iqr._read(folder / "intent.json")) == iqr._canonical(intent),
                     "Capture intent changed")
        iqr._require(weather_tree_fingerprint(table) == result["physical_tree_sha256"], "Capture tree changed")
        iqr._require(weather_tree_fingerprint(config.table_path("weather")) == parent["weather_files_sha256"],
                     "Weather source changed")
        iqr._stable_silver(parent["silver"], config)
        iqr._require(weather_tree_fingerprint(folder) == before, "Capture evidence changed")
        silver._exclusive_json(folder / "completed.json", result)
        return result


def verify_synthetic_weather_capture(spark, config, capture_id, owner_id, workspace):
    """Verify a completed capture without recovery, deletion or data access API."""
    iqr._identifier(capture_id)
    iqr._identifier(owner_id)
    root, namespace = _namespace(workspace, config)
    iqr._require(namespace.is_dir(), "Capture namespace unavailable")
    with _lock(namespace):
        folder = namespace / capture_id
        silver._check_tree(folder)
        iqr._require(folder.is_dir(), "Capture is unavailable")
        iqr._require({p.name for p in folder.iterdir()} == {"intent.json", "table", "completed.json"},
                     "Capture is incomplete or has unknown evidence")
        before = weather_tree_fingerprint(folder)
        intent = iqr._read(folder / "intent.json")
        marker = iqr._read(folder / "completed.json")
        iqr._require(type(intent.get("contract_version")) is int and intent["contract_version"] == CAPTURE_VERSION
                     and intent.get("capture_id") == capture_id and intent.get("owner_id") == owner_id,
                     "Capture ownership or contract differs")
        result = _verify(spark, config, root, folder, intent, marker)
        iqr._require(weather_tree_fingerprint(folder) == before, "Capture evidence changed")
        return result
