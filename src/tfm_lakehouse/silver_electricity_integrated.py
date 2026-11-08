"""Compose the approved electricity profile without reading or publishing data."""
from collections.abc import Mapping

from pyspark.sql import DataFrame, functions as F

from tfm_lakehouse.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.silver_electricity import electricity_to_long
from tfm_lakehouse.silver_electricity_numeric import interpret_electricity_numeric
from tfm_lakehouse.silver_electricity_temporal import interpret_electricity_temporal


def _publication_identity(selection: Mapping) -> tuple[str, int, str]:
    """Validate the supplied snapshot context, without authenticating the frame."""
    if not isinstance(selection, Mapping):
        raise ValueError("Publication context must be a mapping")
    run_id = selection.get("run_id")
    tables = selection.get("tables")
    if not isinstance(run_id, str) or not run_id.strip() or not isinstance(tables, list):
        raise ValueError("Publication context requires a run ID and table list")
    items = [item for item in tables if isinstance(item, Mapping) and item.get("name") == "electricity"]
    if len(items) != 1:
        raise ValueError("Publication context requires exactly one electricity table")
    item = items[0]
    version, path = item.get("delta_version"), item.get("table_path")
    if (type(version) is not int or not 0 <= version <= 2**63 - 1
            or not isinstance(path, str) or not path.strip()):
        raise ValueError("Publication electricity identity is incomplete")
    return run_id, version, path


def transform_selected_electricity(
    bronze: DataFrame, *, selection: Mapping | None = None,
) -> DataFrame:
    """Expand the approved 16 columns and append existing H05.2 diagnostics.

    No publication is chosen or read here. Optional selection is caller-supplied
    context; this function cannot establish that it describes the given frame.
    The projection preserves each input occurrence and original Bronze values.
    """
    identity = _publication_identity(selection) if selection is not None else None
    result = electricity_to_long(bronze, APPROVED_BUILDINGS)
    if identity is not None:
        run_id, version, path = identity
        result = (result.withColumn("_bronze_source_name", F.lit("electricity"))
                  .withColumn("_bronze_snapshot_run_id", F.lit(run_id))
                  .withColumn("_bronze_delta_version", F.lit(version))
                  .withColumn("_bronze_table_path", F.lit(path)))
    return interpret_electricity_temporal(interpret_electricity_numeric(result))
