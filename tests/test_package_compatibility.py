"""Check that historical module paths alias the canonical module objects."""
import os
import subprocess
import sys

import pytest


@pytest.mark.unit
@pytest.mark.parametrize("canonical_first", [False, True])
def test_legacy_imports_share_identity_and_monkeypatch_state(canonical_first):
    script = '''
import importlib
names = {
    "building_selection": "selection", "selection_reproduction": "selection",
    "excel_bdg2_comparison": "comparison", "comparison_csv": "comparison",
    "comparison_xlsx": "comparison", "bdg2_comparison": "comparison",
    "synthetic_selection_workflow": "comparison", "controlled_synthetic_verification": "comparison",
    "controlled_real_verification": "comparison", "silver_electricity": "silver",
    "silver_electricity_numeric": "silver", "silver_electricity_temporal": "silver",
    "silver_electricity_integrated": "silver", "silver_snapshot_binding": "silver",
    "silver_synthetic_publication": "silver",
}
for name, group in names.items():
    old = "tfm_lakehouse." + name
    new = "tfm_lakehouse." + group + "." + name
    first, second = (new, old) if CANONICAL_FIRST else (old, new)
    a, b = importlib.import_module(first), importlib.import_module(second)
    assert a is b, (old, new)
    marker = object()
    a._compatibility_test_marker = marker
    assert b._compatibility_test_marker is marker
    del a._compatibility_test_marker
'''.replace('CANONICAL_FIRST', repr(canonical_first))
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    subprocess.run([sys.executable, "-B", "-c", script], env=env, check=True)
