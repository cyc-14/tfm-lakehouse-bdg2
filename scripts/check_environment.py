"""Use the same validation and publication path as the package command."""
import sys

from tfm_lakehouse.cli import main


if __name__ == "__main__":
    raise SystemExit(main(["check-environment", *sys.argv[1:]]))
