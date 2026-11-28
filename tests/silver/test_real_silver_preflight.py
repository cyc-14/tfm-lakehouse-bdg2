"""Focused preflight checks using only invented local files."""

from dataclasses import replace
import hashlib
import os
from pathlib import Path
import stat

import pytest

from tfm_lakehouse.silver import real_silver_preflight as preflight


@pytest.fixture
def request_scope(tmp_path, monkeypatch):
    monkeypatch.setattr(preflight, '_ROOT', tmp_path)
    monkeypatch.setattr(preflight, '_excluded', lambda _: True)
    monkeypatch.setattr(preflight.subprocess, 'check_output', lambda *a, **k: 'ext4\n')
    tables = tuple(preflight.TableIdentity(name, i % 2, '00000000-0000-0000-0000-000000000001',
                                           'part-invented.parquet')
                   for i, name in enumerate(preflight._SOURCE))
    request = preflight.RealSilverRequest('a' * 32, 'test-publication', tables, ())
    root = Path('data/lakehouse/real')
    paths = {Path('data/raw/bdg2/source_manifest.json'), *preflight._SOURCE.values(),
             root / '_runs' / ('a' * 32 + '.json')}
    for table in tables:
        directory = root / 'bronze' / table.name
        paths.add(directory / 'part-invented.parquet')
        paths.update(directory / '_delta_log' / f'{i:020}.json' for i in range(table.version + 1))
    pins = []
    for path in sorted(paths):
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b'invented')
        pins.append(preflight.FileIdentity(path, 8, hashlib.sha256(b'invented').hexdigest()))
    return replace(request, inputs=tuple(pins))


@pytest.mark.unit
def test_verified_scope_only_reads_inputs(request_scope):
    preflight.verify_real_silver_inputs(request_scope, authorized=request_scope)
    assert not request_scope.output.exists() and not request_scope.evidence.exists()


@pytest.mark.unit
def test_different_grant_fails_before_filesystem_access(request_scope, monkeypatch):
    monkeypatch.setattr(preflight, '_verify_file', lambda *_: pytest.fail('input opened'))
    modified = replace(request_scope, publication_id='other')
    with pytest.raises(preflight.RealSilverPreflightError, match='exact read-only authorization'):
        preflight.verify_real_silver_inputs(modified, authorized=request_scope)


@pytest.mark.unit
@pytest.mark.parametrize('fault', ['hash', 'link', 'missing-pin', 'traversal'])
def test_different_input_or_boundary_fails_closed(tmp_path, request_scope, fault):
    target = tmp_path / request_scope.inputs[0].relative_path
    if fault == 'hash':
        target.write_bytes(b'different'[:8])
    elif fault == 'link':
        sibling = tmp_path / 'stand-in'
        sibling.write_bytes(b'invented')
        target.unlink()
        target.symlink_to(sibling)
    elif fault == 'missing-pin':
        request_scope = replace(request_scope, inputs=request_scope.inputs[1:])
    else:
        changed = replace(request_scope.tables[0], active_file='../part-invented.parquet')
        request_scope = replace(request_scope, tables=(changed, *request_scope.tables[1:]))
    with pytest.raises(preflight.RealSilverPreflightError):
        preflight.verify_real_silver_inputs(request_scope, authorized=request_scope)

@pytest.mark.unit
def test_separate_mount_rejected_even_with_same_owner(request_scope, monkeypatch):
    monkeypatch.setattr(preflight.resource_linux, '_mount_id',
                        lambda fd: 2 if stat.S_ISREG(os.fstat(fd).st_mode) else 1)
    with pytest.raises(preflight.RealSilverPreflightError, match='mount'):
        preflight.verify_real_silver_inputs(request_scope, authorized=request_scope)
