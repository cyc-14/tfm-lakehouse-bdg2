"""Compatibility alias for the domain module."""
import importlib
import sys

sys.modules[__name__] = importlib.import_module("tfm_lakehouse.comparison.synthetic_selection_workflow")
