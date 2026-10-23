"""Run from the project root after installing the package."""
import argparse
import json
from pathlib import Path

from tfm_lakehouse.environment import verify_delta
from tfm_lakehouse.spark import spark_session

parser = argparse.ArgumentParser(description="Verifica escritura/lectura Delta con datos sintéticos.")
parser.add_argument("--report", type=Path)
parser.add_argument("--ivy-dir", type=Path)
args = parser.parse_args()
with spark_session(ivy_dir=args.ivy_dir) as spark:
    report = verify_delta(spark)
if args.report:
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
print(json.dumps(report, indent=2))
