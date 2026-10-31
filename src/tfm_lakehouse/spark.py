"""Exclusive local Spark/Delta startup with explicit resource ownership."""
from contextlib import contextmanager
import logging
import os
from pathlib import Path
import re
import shlex
import sys
from typing import Iterable, Iterator

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark import SparkContext
from pyspark.sql import SparkSession

from tfm_lakehouse.config import ConfigurationError, validate_ivy_directory

LOGGER = logging.getLogger(__name__)
_LOCK_TIMEOUT_SECONDS = 10.0
_JAVA_ARGFILE = re.compile(r"""(^|[\s=:'"\\])@""")


def _reject_existing_resources() -> None:
    # Check the context first, avoiding JVM queries or wrappers for foreign contexts.
    if (SparkContext._active_spark_context is not None
            or SparkSession._instantiatedSession is not None
            or SparkSession.getActiveSession() is not None):
        raise RuntimeError("Close the existing Spark session/context before starting the project")


@contextmanager
def _spark_locks():
    # PySpark 3.5.7 uses these RLocks in this order inside Builder.getOrCreate().
    first, second = SparkSession.Builder._lock, SparkContext._lock
    # CPython 3.11 exposes the current thread's recursion depth. A flag assigned
    # after acquire() misses cancellation between acquisition and assignment.
    if not all(callable(getattr(lock, "_recursion_count", None)) for lock in (first, second)):
        raise RuntimeError("Safe Spark startup requires CPython RLock depth inspection")
    first_depth, second_depth = first._recursion_count(), second._recursion_count()
    try:
        if not first.acquire(timeout=_LOCK_TIMEOUT_SECONDS):
            raise RuntimeError("Timed out acquiring the Spark builder lock")
        if not second.acquire(timeout=_LOCK_TIMEOUT_SECONDS):
            raise RuntimeError("Timed out acquiring the Spark context lock")
        yield
    finally:
        try:
            if second._recursion_count() > second_depth:
                second.release()
        finally:
            if first._recursion_count() > first_depth:
                first.release()


def _validate_launcher_settings() -> None:
    """Reject explicit Ivy overrides; custom launch scripts are outside this profile."""
    arguments = shlex.split(os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell"))
    if any("spark.jars.ivy" in arg or arg.startswith("--properties-file")
           or arg.startswith("--ivy-repo-path") or _JAVA_ARGFILE.search(arg)
           for arg in arguments):
        raise ConfigurationError("External Ivy overrides/properties files are unsupported")
    for key in ("SPARK_SUBMIT_OPTS", "JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "JDK_JAVA_OPTIONS"):
        value = os.environ.get(key, "")
        if ("ivy." in value or "spark.jars.ivy" in value
                or _JAVA_ARGFILE.search(value)):
            raise ConfigurationError("External JVM Ivy overrides are unsupported")
    if "PYSPARK_GATEWAY_PORT" in os.environ:
        raise ConfigurationError("An externally supplied Spark gateway is unsupported")
    spark_home = Path(os.environ.get("SPARK_HOME", Path(pyspark.__file__).parent))
    conf = Path(os.environ.get("SPARK_CONF_DIR") or spark_home / "conf")
    for filename in ("spark-env.sh", "java-opts"):
        try:
            (conf / filename).lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ConfigurationError("Cannot inspect external Spark launcher settings") from exc
        raise ConfigurationError(f"Custom {filename} requires a separately audited profile")
    defaults = conf / "spark-defaults.conf"
    try:
        with defaults.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip() and not line.lstrip().startswith(("#", "!")):
                    # Java properties escapes/continuations can conceal Ivy keys.
                    if ("spark.jars.ivy" in line or "\\" in line
                            or _JAVA_ARGFILE.search(line)):
                        raise ConfigurationError(
                            "External Ivy/escaped Spark defaults are unsupported"
                        )
    except FileNotFoundError:
        pass
    except (OSError, UnicodeError) as exc:
        raise ConfigurationError("Cannot inspect Spark defaults before startup") from exc


def _cleanup_failure(primary: BaseException | None, secondary: BaseException) -> BaseException:
    if primary is None:
        return secondary
    if isinstance(primary, Exception) and not isinstance(secondary, Exception):
        secondary.__cause__ = primary
        return secondary
    primary.add_note(f"Secondary Spark cleanup failure: {type(secondary).__name__}: {secondary}")
    return primary


def _stop_owned_context(context: SparkContext, session: SparkSession | None) -> None:
    with _spark_locks():
        # Both stop() implementations clear global state; never call them on a replacement.
        if SparkContext._active_spark_context is not context:
            LOGGER.warning("Spark context was replaced externally; skipping manager cleanup")
            return
        default = SparkSession._instantiatedSession
        if session is None and default is not None and getattr(default, "_sc", None) is context:
            session = default
        if (session is not None and getattr(session, "_sc", None) is context
                and (default is None or default is session)):
            session.stop()
        else:
            context.stop()


@contextmanager
def spark_session(
    app_name: str = "tfm-lakehouse",
    master: str = "local[2]",
    ivy_dir: Path | None = None,
    *,
    protected_roots: Iterable[Path] = (),
) -> Iterator[SparkSession]:
    """Own one local context; reject preexisting resources and unsafe Ivy paths.

    Additional protected roots extend the checkout's data roots. Outside an
    identifiable checkout they are required. Paths must remain stable after
    validation; this is not filesystem isolation against concurrent attackers.
    """
    _reject_existing_resources()
    if master != "local" and not (master.startswith("local[") and master.endswith("]")):
        raise ValueError("Este prototipo admite únicamente Spark local.")
    ivy = validate_ivy_directory(ivy_dir, protected_roots=protected_roots)
    _validate_launcher_settings()
    previous = os.environ.get("PYSPARK_PYTHON")
    environment_changed = False
    session = None
    owned = None
    failure = None
    try:
        with _spark_locks():
            _reject_existing_resources()
            builder = (
                SparkSession.builder.master(master).appName(app_name)
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.databricks.delta.snapshotPartitions", "2")
                .config("spark.ui.enabled", "false")
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.jars.ivy", str(ivy))
                .config("spark.pyspark.python", sys.executable)
            )
            builder = configure_spark_with_delta_pip(builder)
            _reject_existing_resources()
            environment_changed = True
            os.environ["PYSPARK_PYTHON"] = sys.executable
            try:
                try:
                    session = builder.getOrCreate()
                finally:
                    # No standard creator can intervene while both locks are held.
                    owned = SparkContext._active_spark_context
            except BaseException:
                # Also cover cancellation of the ownership-registration line itself.
                # Retry only if it did not finish; a prior startup error must not
                # create an unnecessary second registration boundary.
                if owned is None:
                    owned = SparkContext._active_spark_context
                raise
            if owned is None or session.sparkContext is not owned:
                raise RuntimeError("Spark startup did not return the manager's context")
            session.sparkContext.setLogLevel("WARN")
        yield session
    except BaseException as exc:
        failure = exc
    finally:
        if owned is not None:
            try:
                _stop_owned_context(owned, session)
            except BaseException as exc:
                failure = _cleanup_failure(failure, exc)
        if environment_changed:
            try:
                if previous is None:
                    os.environ.pop("PYSPARK_PYTHON", None)
                else:
                    os.environ["PYSPARK_PYTHON"] = previous
            except BaseException as exc:
                failure = _cleanup_failure(failure, exc)
    if failure is not None:
        raise failure
