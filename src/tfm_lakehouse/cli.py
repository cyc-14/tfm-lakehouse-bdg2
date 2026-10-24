"""Command-line entry points that do not require virtual environment activation."""
import argparse
import json
import logging
from pathlib import Path

from tfm_lakehouse.config import load_config
from tfm_lakehouse.environment import verify_delta
from tfm_lakehouse.ingestion.bronze import run_bronze
from tfm_lakehouse.ingestion.snapshot import read_bronze_snapshot
from tfm_lakehouse.spark import spark_session


def main() -> int:
    parser = argparse.ArgumentParser(description="TFM Lakehouse local, hitos 1 y 2")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser("check-environment")
    check.add_argument("--report", type=Path)
    check.add_argument("--ivy-dir", type=Path)
    for name in ("ingest-bronze", "inspect-bronze"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parser.parse_args()
    try:
        if args.command == "check-environment":
            with spark_session(ivy_dir=args.ivy_dir) as spark:
                report = verify_delta(spark)
            if args.report:
                args.report.parent.mkdir(parents=True, exist_ok=True)
                args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        else:
            config = load_config(args.config)
            with spark_session(master=config.master, ivy_dir=config.ivy_dir) as spark:
                if args.command == "ingest-bronze":
                    report = run_bronze(spark, config)
                else:
                    selection, frames = read_bronze_snapshot(spark, config)
                    report = {
                        "data_kind": config.data_kind, "run_id": selection["run_id"],
                        "report_path": selection["report_path"],
                        "latest_attempt": selection["latest_attempt"],
                        "using_previous_successful_run": selection["using_previous_successful_run"],
                        "tables": [],
                    }
                    for item in selection["tables"]:
                        name = item["name"]
                        frame = frames[name]
                        report["tables"].append({
                            "name": name, "path": str(config.table_path(name)),
                            "delta_version": item["delta_version"], "rows": item["output_rows"],
                            "schema": frame.schema.jsonValue(),
                            "sample": [row.asDict() for row in frame.limit(5).collect()],
                        })
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    except Exception:
        logging.getLogger(__name__).exception("Comando %s fallido", args.command)
        return 1
