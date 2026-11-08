"""Fail-closed Silver publication for isolated, caller-declared synthetic fixtures."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from delta.tables import DeltaTable
from pyspark import StorageLevel

from tfm_lakehouse.config import BronzeConfig, project_protected_roots
from tfm_lakehouse.silver_snapshot_binding import transform_published_synthetic_electricity


class SilverPublicationError(RuntimeError):
    """Publication cannot safely proceed or be reconciled."""


_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_TRANSFORM = "h05.3-v1"


def _no_links(path: Path) -> None:
    for component in reversed((path, *path.parents)):
        if component.is_symlink():
            raise SilverPublicationError("Symbolic links are not supported")


def _check_tree(path: Path) -> None:
    """Reject linked and special entries in a pre-existing local Delta tree."""
    _no_links(path)
    if not path.exists():
        return
    pending = [path]
    while pending:
        directory = pending.pop()
        if not directory.is_dir():
            raise SilverPublicationError("Delta tree must contain directories")
        with os.scandir(directory) as entries:
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    pending.append(Path(entry.path))
                elif not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise SilverPublicationError("Linked or special Delta entry is unsupported")


def _open_regular(path: Path, flags: int):
    """Open an unlinked regular file without following its final component."""
    _no_links(path)
    fd = os.open(path, flags | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or (info.st_dev, info.st_ino) != (os.lstat(path).st_dev, os.lstat(path).st_ino)):
            raise SilverPublicationError("Linked or special publication file is unsupported")
        return os.fdopen(fd, "rb" if flags == os.O_RDONLY else "a+b")
    except BaseException:
        os.close(fd)
        raise


def _validate_workspace(workspace: Path, config: BronzeConfig) -> Path:
    if not isinstance(workspace, Path) or not workspace.is_absolute() or ".." in workspace.parts:
        raise SilverPublicationError("An absolute temporary workspace is required")
    _no_links(workspace)
    root = workspace.resolve(strict=True)
    temp = Path("/tmp").resolve(strict=True)
    if root == temp or not root.is_relative_to(temp) or not root.is_dir():
        raise SilverPublicationError("Workspace must be an isolated directory under the temporary root")
    if config.data_kind != "synthetic":
        raise SilverPublicationError("Only synthetic configurations are supported")
    protected = tuple(path.resolve() for path in project_protected_roots())
    if any(root.is_relative_to(path) or path.is_relative_to(root) for path in protected):
        raise SilverPublicationError("Workspace overlaps protected project data")
    for path in (config.raw_root, config.lakehouse_root, config.namespace,
                 *config.sources.values(), *(config.table_path(name) for name in config.sources)):
        _no_links(path)
        resolved = path.resolve()
        if resolved == root or not resolved.is_relative_to(root):
            raise SilverPublicationError("Bronze paths must be inside the temporary workspace")
    silver = root / "silver-publications"
    for path in (silver, *project_protected_roots()):
        _no_links(path)
    if any(silver.is_relative_to(path) or path.is_relative_to(silver) for path in protected):
        raise SilverPublicationError("Silver destination overlaps protected data")
    for path in (config.lakehouse_root.resolve(), config.raw_root.resolve()):
        if silver.is_relative_to(path) or path.is_relative_to(silver):
            raise SilverPublicationError("Silver destination overlaps Bronze or raw")
    config.validate()
    return root


@contextmanager
def _lock(root: Path):
    namespace = root / "silver-publications"
    _no_links(namespace)
    namespace.mkdir(exist_ok=True)
    lock = namespace / ".publication.lock"
    _no_links(lock)
    with _open_regular(lock, os.O_RDWR | os.O_CREAT) as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SilverPublicationError("Another local publication is active") from exc
        try:
            yield namespace
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _exclusive_json(path: Path, value: dict) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle, sort_keys=True, separators=(",", ":"))
        handle.flush()
        os.fsync(handle.fileno())


def _read_json(path: Path) -> dict:
    _no_links(path)
    try:
        with _open_regular(path, os.O_RDONLY) as handle:
            value = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SilverPublicationError("Publication evidence is unreadable or incomplete") from exc
    if not isinstance(value, dict):
        raise SilverPublicationError("Publication evidence is invalid")
    return value


def _manifest(config: BronzeConfig, run_id: str) -> tuple[dict, str]:
    path = config.namespace / "_runs" / (run_id + ".json")
    _no_links(path)
    try:
        with _open_regular(path, os.O_RDONLY) as handle:
            content = handle.read()
        report = json.loads(content)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SilverPublicationError("Bronze report is unavailable") from exc
    return report, hashlib.sha256(content).hexdigest()


def _table_identity(report: dict) -> list[dict]:
    try:
        tables = [{key: item[key] for key in ("name", "table_path", "delta_version", "source_sha256")}
                  for item in report["tables"]]
        return sorted(tables, key=lambda item: item["name"])
    except (KeyError, TypeError) as exc:
        raise SilverPublicationError("Bronze report identity is incomplete") from exc


def _same_schema(left, right) -> bool:
    return [(f.name, f.dataType) for f in left] == [(f.name, f.dataType) for f in right]


def _validate_output(spark, target: Path, expected, intent: dict) -> dict:
    _check_tree(target)
    if not DeltaTable.isDeltaTable(spark, str(target)):
        raise SilverPublicationError("Target is incomplete or is not a Delta table")
    table = DeltaTable.forPath(spark, str(target))
    history = table.history().select("version", "userMetadata").collect()
    metadata = json.dumps(intent, sort_keys=True, separators=(",", ":"))
    if len(history) != 1 or history[0]["version"] != 0 or history[0]["userMetadata"] != metadata:
        raise SilverPublicationError("Delta history conflicts with the intended publication")
    details = table.detail().select("id").collect()
    if len(details) != 1 or not isinstance(details[0]["id"], str) or not details[0]["id"]:
        raise SilverPublicationError("Delta table identity is unavailable")
    actual = spark.read.format("delta").option("versionAsOf", 0).load(str(target))
    if not _same_schema(expected.schema, actual.schema):
        raise SilverPublicationError("Committed schema differs from the intended output")
    count = actual.count()
    if count != intent["output_rows"]:
        raise SilverPublicationError("Committed row count differs from the intended output")
    if expected.exceptAll(actual).limit(1).count() or actual.exceptAll(expected).limit(1).count():
        raise SilverPublicationError("Committed rows differ from the intended output")
    return {"status": "passed", "publication_id": intent["publication_id"],
            "source_run_id": intent["source_run_id"], "manifest_sha256": intent["manifest_sha256"],
            "source_tables": intent["source_tables"], "transform_version": _TRANSFORM,
            "input_rows": intent["input_rows"], "output_rows": count,
            "delta_version": 0, "delta_table_id": details[0]["id"], "schema": actual.schema.jsonValue(),
            "table_path": str(target), "source_authenticity": "not_established"}


def publish_synthetic_silver(spark, config: BronzeConfig, run_id: str, publication_id: str,
                             workspace: Path, *, transform_version: str = _TRANSFORM) -> dict:
    """Publish or reconcile a local synthetic output; never overwrite unknown state.

    Only cooperating local writers are serialized. Atomicity across the Delta
    commit and evidence file, external authenticity, and later retention are not
    guaranteed. An incomplete target without a valid commit requires review.
    """
    if not isinstance(run_id, str) or not _ID.fullmatch(run_id):
        raise SilverPublicationError("Invalid source run ID")
    if not isinstance(publication_id, str) or not _ID.fullmatch(publication_id):
        raise SilverPublicationError("Invalid Silver publication ID")
    if transform_version != _TRANSFORM:
        raise SilverPublicationError("Unsupported transformation version")
    root = _validate_workspace(workspace, config)
    with _lock(root) as namespace:
        folder = namespace / publication_id
        _no_links(folder)
        for name in config.sources:
            _check_tree(config.table_path(name))
        report, manifest_hash = _manifest(config, run_id)
        assessment, frame = transform_published_synthetic_electricity(spark, config, run_id)
        second, second_hash = _manifest(config, run_id)
        if second_hash != manifest_hash or second != report:
            raise SilverPublicationError("Bronze report changed during validation")
        try:
            rows = next(item["output_rows"] for item in report["tables"] if item["name"] == "electricity")
            source_tables = _table_identity(report)
        except (KeyError, TypeError, StopIteration) as exc:
            raise SilverPublicationError("Bronze report lacks electricity count") from exc
        if (type(rows) is not int or rows <= 0
                or assessment["snapshot_binding"] != "h02_version_metadata_schema_count_checked"
                or assessment.get("manifest_sha256") != manifest_hash
                or assessment.get("source_tables") != source_tables):
            raise SilverPublicationError("Bronze binding is incomplete")
        expected_count = rows * 16
        intent = {"publication_id": publication_id, "source_run_id": run_id,
                  "manifest_sha256": manifest_hash, "source_tables": source_tables,
                  "transform_version": transform_version, "input_rows": rows,
                  "output_rows": expected_count}
        for other in namespace.iterdir():
            if other.name == ".publication.lock" or other == folder:
                continue
            _no_links(other)
            if not other.is_dir():
                raise SilverPublicationError("Unknown entry in publication namespace")
            existing = _read_json(other / "intent.json")
            if existing.get("source_run_id") == run_id and existing.get("transform_version") == transform_version:
                raise SilverPublicationError("Bronze publication already reserved under another Silver ID")
        frame = frame.persist(StorageLevel.DISK_ONLY)
        try:
            if frame.count() != expected_count:
                raise SilverPublicationError("Silver row expansion does not match Bronze")
            if not folder.exists():
                folder.mkdir()
                _exclusive_json(folder / "intent.json", intent)
            elif not folder.is_dir() or _read_json(folder / "intent.json") != intent:
                raise SilverPublicationError("Publication ID conflicts with existing evidence")
            target = folder / "table"
            completed = folder / "completed.json"
            _no_links(target)
            _no_links(completed)
            if not target.exists():
                if completed.exists():
                    raise SilverPublicationError("Evidence exists without a Delta table")
                metadata = json.dumps(intent, sort_keys=True, separators=(",", ":"))
                (frame.write.format("delta").mode("errorifexists")
                 .option("userMetadata", metadata).save(str(target)))
            result = _validate_output(spark, target, frame, intent)
            if completed.exists():
                if _read_json(completed) != result:
                    raise SilverPublicationError("Finalized evidence conflicts with Delta")
            else:
                _exclusive_json(completed, result)
            return result
        finally:
            frame.unpersist()
