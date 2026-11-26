"""Explicit synthetic weather publication and conservative recovery."""
from pathlib import Path

from tfm_lakehouse.silver import contextual_reader
from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver
from tfm_lakehouse.silver import weather_capture as capture
from tfm_lakehouse.silver.weather_integrity import weather_tree_fingerprint

PUBLICATION_VERSION = 1


def _namespace(workspace, config):
    root = iqr._workspace(workspace, config)
    target = root / "weather-publications"
    silver._no_links(target)
    for other in (config.raw_root.resolve(), config.lakehouse_root.resolve(),
                  root / "silver-publications", root / "contextual-publications",
                  root / "iqr-publications", root / "weather-captures",
                  root / "weather-enriched-publications"):
        silver._no_links(other)
        iqr._require(not target.is_relative_to(other) and not other.is_relative_to(target),
                     "Weather publication destination overlaps another product")
    silver._check_tree(target)
    return root, target


def _verified_capture(spark, config, root, namespace, capture_id, owner_id):
    folder = namespace / capture_id
    silver._check_tree(folder)
    iqr._require(folder.is_dir(), "Capture is unavailable")
    iqr._require({p.name for p in folder.iterdir()} == {"intent.json", "table", "completed.json"},
                 "Capture is incomplete or has unknown evidence")
    before = weather_tree_fingerprint(folder)
    intent = iqr._read(folder / "intent.json")
    marker = iqr._read(folder / "completed.json")
    iqr._require(type(intent.get("contract_version")) is int and intent["contract_version"] == capture.CAPTURE_VERSION
                 and intent.get("capture_id") == capture_id and intent.get("owner_id") == owner_id,
                 "Capture ownership or contract differs")
    result = capture._verify(spark, config, root, folder, intent, marker)
    iqr._require(weather_tree_fingerprint(folder) == before, "Capture evidence changed")
    return folder, result, before


def _stable_capture(spark, config, root, folder, fingerprint, evidence):
    iqr._require(weather_tree_fingerprint(folder) == fingerprint, "Capture evidence changed")
    parent = evidence["intent"]["weather_identity"]
    iqr._require(weather_tree_fingerprint(config.table_path("weather")) == parent["weather_files_sha256"],
                 "Weather source changed")
    iqr._stable_silver(parent["silver"], config)
    contextual, _ = contextual_reader.read_synthetic_contextual(
        spark, config, parent["contextual_publication_id"], root)
    iqr._require(iqr._canonical(contextual) == iqr._canonical(parent["contextual"]),
                 "Contextual source changed")
    iqr._require(weather_tree_fingerprint(folder) == fingerprint, "Capture evidence changed")
    iqr._require(weather_tree_fingerprint(config.table_path("weather")) == parent["weather_files_sha256"],
                 "Weather source changed")


def _verify_table(spark, config, root, table, intent, capture_folder, capture_evidence):
    before = weather_tree_fingerprint(table)
    commit = capture._commit(spark, table, intent)
    parent = capture_evidence["intent"]["weather_identity"]
    output = spark.read.format("delta").option("versionAsOf", 0).load(str(table))
    captured = spark.read.format("delta").option("versionAsOf", 0).load(str(capture_folder / "table"))
    logical = capture._logical_frame(spark, output, parent["schema"])
    capture_logical = capture._logical_frame(spark, captured, parent["schema"])
    capture._same_multiset(logical, capture_logical)
    context_evidence, context = contextual_reader.read_synthetic_contextual(
        spark, config, parent["contextual_publication_id"], root)
    iqr._require(iqr._canonical(context_evidence) == iqr._canonical(parent["contextual"]),
                 "Contextual source changed")
    silver_evidence, electrical = iqr._verify_silver(spark, config, root, parent["silver"]["publication_id"])
    iqr._require(iqr._canonical(silver_evidence) == iqr._canonical(parent["silver"]), "Silver source changed")
    capture._same_multiset(logical.select(*context.columns), context)
    capture._same_multiset(logical.select(*electrical.columns), electrical)
    iqr._stable_silver(silver_evidence, config)
    iqr._require(weather_tree_fingerprint(table) == before, "Weather publication table changed")
    return {"contract_version": PUBLICATION_VERSION, "publication_id": intent["publication_id"],
            "intent": intent, **commit, "physical_tree_sha256": before,
            "physical_schema": output.schema.jsonValue(), "logical_schema": parent["schema"],
            "reconciliation": "full_capture_context_electric_passed"}


def publish_synthetic_weather(spark, config, capture_id: str, owner_id: str,
                              publication_id: str, workspace: Path):
    """Publish or explicitly reconcile one capture without repair or new commits.

    The capture and all retained sources must remain available and unchanged.
    Locks protect cooperating writers only; no historical reading is provided.
    """
    for identifier in (capture_id, owner_id, publication_id):
        iqr._identifier(identifier)
    root, namespace = _namespace(workspace, config)
    capture_root, capture_namespace = capture._namespace(workspace, config)
    iqr._require(root == capture_root and capture_namespace.is_dir(), "Capture namespace unavailable")
    # A fixed order avoids deadlock with other publication attempts.
    with capture._lock(namespace):
        with capture._lock(capture_namespace):
            capture_folder, captured, capture_hash = _verified_capture(
                spark, config, root, capture_namespace, capture_id, owner_id)
            iqr._require(captured["intent"]["weather_identity"]["publication_id"] == publication_id,
                         "Publication ID differs from capture intention")
            intent = {"contract_version": PUBLICATION_VERSION, "publication_id": publication_id,
                      "capture_id": capture_id, "owner_id": owner_id,
                      "capture_folder_sha256": capture_hash, "capture": captured}
            folder = namespace / publication_id
            silver._no_links(folder)
            if not folder.exists():
                folder.mkdir()
                silver._exclusive_json(folder / "intent.json", intent)
            else:
                silver._check_tree(folder)
                names = {p.name for p in folder.iterdir()}
                iqr._require("intent.json" in names and names <= {"intent.json", "table", "completed.json"},
                             "Publication evidence is incomplete or unknown")
                iqr._require(iqr._canonical(iqr._read(folder / "intent.json")) == iqr._canonical(intent),
                             "Weather publication intent conflicts")
            table, completed = folder / "table", folder / "completed.json"
            _stable_capture(spark, config, root, capture_folder, capture_hash, captured)
            if not table.exists():
                iqr._require(not completed.exists(), "Completion exists without publication table")
                frame = spark.read.format("delta").option("versionAsOf", 0).load(str(capture_folder / "table"))
                (frame.write.format("delta").mode("errorifexists")
                 .option("userMetadata", iqr._canonical(intent)).save(str(table)))
            before = weather_tree_fingerprint(folder)
            result = _verify_table(spark, config, root, table, intent, capture_folder, captured)
            _stable_capture(spark, config, root, capture_folder, capture_hash, captured)
            iqr._require(iqr._canonical(iqr._read(folder / "intent.json")) == iqr._canonical(intent),
                         "Weather publication intent changed")
            iqr._require(weather_tree_fingerprint(folder) == before, "Weather publication evidence changed")
            if completed.exists():
                iqr._require(iqr._canonical(iqr._read(completed)) == iqr._canonical(result),
                             "Weather completion evidence differs")
                iqr._require(weather_tree_fingerprint(folder) == before, "Weather publication evidence changed")
            else:
                silver._exclusive_json(completed, result)
            return result
