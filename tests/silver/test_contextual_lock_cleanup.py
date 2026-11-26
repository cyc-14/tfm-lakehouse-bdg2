"""D25 cleanup behavior for contextual reader and publisher locks."""
import fcntl

import pytest

from tfm_lakehouse.silver import contextual_publication as writer
from tfm_lakehouse.silver import contextual_reader as reader

pytestmark = pytest.mark.unit


@pytest.mark.parametrize('kind', ['writer', 'reader'])
@pytest.mark.parametrize('body_failure', [False, True])
def test_unlock_and_close_failures_preserve_primary(tmp_path, monkeypatch, kind, body_failure):
    class Handle:
        def close(self):
            events.append('close')
            raise OSError('close failure')
    events = []
    handle = Handle()
    module = writer if kind == 'writer' else reader
    namespace = tmp_path / 'contextual-publications'
    namespace.mkdir()
    monkeypatch.setattr(module.silver, '_open_regular', lambda *_: handle)
    def flock(_, operation):
        if operation == fcntl.LOCK_UN:
            events.append('unlock')
            raise OSError('unlock failure')
        events.append('acquire')
    monkeypatch.setattr(module.fcntl, 'flock', flock)
    primary = ValueError('body')
    cause = RuntimeError('original cause')
    primary.__cause__ = cause
    with pytest.raises(ValueError if body_failure else OSError) as caught:
        with (writer._lock(tmp_path) if kind == 'writer' else reader._read_lock(tmp_path)):
            if body_failure:
                raise primary
    assert events == ['acquire', 'unlock', 'close']
    if body_failure:
        assert caught.value is primary
        assert caught.value.__cause__ is cause
        assert any(frame.name == 'test_unlock_and_close_failures_preserve_primary'
                   for frame in caught.traceback)
        assert len(primary._h06_cleanup_failures) == 2
    else:
        assert str(caught.value) == 'unlock failure'
        assert len(caught.value._h06_cleanup_failures) == 1


@pytest.mark.parametrize('kind', ['writer', 'reader'])
def test_acquisition_failure_closes_without_unlock(tmp_path, monkeypatch, kind):
    class Handle:
        def close(self):
            events.append('close')
    events = []
    module = writer if kind == 'writer' else reader
    (tmp_path / 'contextual-publications').mkdir()
    monkeypatch.setattr(module.silver, '_open_regular', lambda *_: Handle())
    def flock(_, operation):
        events.append(operation)
        raise BlockingIOError('locked')
    monkeypatch.setattr(module.fcntl, 'flock', flock)
    with pytest.raises(writer.iqr.IQRPublicationError, match='active'):
        with (writer._lock(tmp_path) if kind == 'writer' else reader._read_lock(tmp_path)):
            pytest.fail('lock acquired')
    assert len(events) == 2 and events[1] == 'close'


@pytest.mark.parametrize('kind', ['writer', 'reader'])
def test_acquisition_and_close_failure_preserve_acquisition(tmp_path, monkeypatch, kind):
    class Handle:
        def close(self):
            raise OSError('close failure')
    module = writer if kind == 'writer' else reader
    (tmp_path / 'contextual-publications').mkdir()
    monkeypatch.setattr(module.silver, '_open_regular', lambda *_: Handle())
    original = OSError('acquire failure')
    def flock(_, operation):
        assert operation != fcntl.LOCK_UN
        raise original
    monkeypatch.setattr(module.fcntl, 'flock', flock)
    with pytest.raises(OSError) as caught:
        with (writer._lock(tmp_path) if kind == 'writer' else reader._read_lock(tmp_path)):
            pytest.fail('lock acquired')
    assert caught.value is original
    assert len(original._h06_cleanup_failures) == 1


def test_cleanup_reporting_import_failure_cannot_mask_primary(monkeypatch):
    import builtins
    original_import = builtins.__import__
    def fail_import(name, *args, **kwargs):
        if name == 'tfm_lakehouse.silver.weather_capture':
            raise ImportError('invented reporting failure')
        return original_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, '__import__', fail_import)
    primary = ValueError('original')
    secondary = OSError('cleanup')
    writer._record_cleanup_failure(primary, secondary)
    assert any('cleanup' in note for note in primary.__notes__)
    assert primary._h06_cleanup_failures == [secondary]


def test_cleanup_reporting_retains_errors_when_import_and_notes_fail(monkeypatch):
    import builtins
    original_import = builtins.__import__
    def fail_import(name, *args, **kwargs):
        if name == 'tfm_lakehouse.silver.weather_capture':
            raise ImportError('invented reporting failure')
        return original_import(name, *args, **kwargs)
    class NoNotes(Exception):
        def add_note(self, note):
            raise RuntimeError('notes unavailable')
    logged = []
    monkeypatch.setattr(builtins, '__import__', fail_import)
    monkeypatch.setattr(writer._LOGGER, 'error', lambda *args, **kwargs: logged.append(kwargs))
    primary, secondary = NoNotes('original'), OSError('cleanup')
    writer._record_cleanup_failure(primary, secondary)
    assert primary._h06_cleanup_failures == [secondary]
    assert logged[0]['exc_info'][1] is secondary



def test_contextual_writer_lock_blocks_independent_process(tmp_path):
    import os
    import selectors
    import subprocess
    import sys
    program = ("import os,sys; from pathlib import Path; "
               "from tfm_lakehouse.silver import contextual_publication as p; "
               "assert 'TFM_REAL_CONFIG' not in os.environ; "
               "lock=p._lock(Path(sys.argv[1])); lock.__enter__(); "
               "print('READY',flush=True); sys.stdin.readline(); lock.__exit__(None,None,None)")
    child = subprocess.Popen([sys.executable, '-B', '-c', program, str(tmp_path)],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             text=True, env=os.environ.copy())
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=15), 'Synthetic child did not acquire the lock'
        assert child.stdout.readline().strip() == 'READY'
        with pytest.raises(writer.iqr.IQRPublicationError, match='active'):
            with writer._lock(tmp_path):
                pytest.fail('foreign writer was not blocked')
    finally:
        try:
            _, stderr = child.communicate(input='release\n', timeout=15)
        except subprocess.TimeoutExpired:
            child.terminate()
            _, stderr = child.communicate(timeout=15)
    assert child.returncode == 0, stderr
