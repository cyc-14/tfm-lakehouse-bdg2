"""Synthetic resource ownership and pre-launch safety tests; no JVM is started."""
from contextlib import contextmanager
import os
from pathlib import Path
from queue import Queue
from threading import Event, RLock, Thread
from types import SimpleNamespace
import sys

import pytest

from tfm_lakehouse import spark as module
from tfm_lakehouse.config import ConfigurationError

pytestmark = pytest.mark.unit


@pytest.fixture
def harness(tmp_path, monkeypatch):
    h = SimpleNamespace(events=[], options={}, errors={}, contexts=[], sessions=[])
    h.context_api = SimpleNamespace(_active_spark_context=None, _lock=RLock())

    def event(stage):
        h.events.append(stage)
        if stage in h.errors:
            raise h.errors[stage]

    class Context:
        def __init__(self):
            self.stops = 0
            h.contexts.append(self)

        def setLogLevel(self, level):
            event('log')
            assert level == 'WARN'

        def stop(self):
            self.stops += 1
            event('stop')
            h.context_api._active_spark_context = None

    class Session:
        def __init__(self, context):
            self._sc = self.sparkContext = context
            self.stops = 0
            h.sessions.append(self)

        def stop(self):
            self.stops += 1
            self._sc.stop()
            h.api._instantiatedSession = None

    class Builder:
        _lock = RLock()

        def master(self, value):
            h.options['master'] = value
            return self

        def appName(self, value):
            h.options['app'] = value
            return self

        def config(self, key, value):
            event('config')
            h.options[key] = value
            return self

        def getOrCreate(self):
            # Exercise reentrancy with the same lock nesting as installed PySpark.
            with self._lock, h.context_api._lock:
                event('start')
                context = Context()
                h.context_api._active_spark_context = context
                event('after_context')
                session = Session(context)
                h.api._instantiatedSession = session
                event('after_session')
                return session

    class API:
        _instantiatedSession = None
        active = None

        @property
        def builder(self):
            event('builder')
            return Builder()

        def getActiveSession(self):
            return self.active

    h.api = API()
    h.api.Builder = Builder
    h.Context, h.Session = Context, Session
    monkeypatch.setattr(module, 'SparkContext', h.context_api)
    monkeypatch.setattr(module, 'SparkSession', h.api)
    monkeypatch.setattr(module, '_LOCK_TIMEOUT_SECONDS', 0.1)

    def delta(builder):
        event('delta')
        return builder.config('spark.jars.packages', 'synthetic-delta-coordinate')

    monkeypatch.setattr(module, 'configure_spark_with_delta_pip', delta)
    # The real path validator still runs, using only synthetic paths for its default.
    monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path / 'home'))
    for key in ('PYSPARK_SUBMIT_ARGS', 'SPARK_SUBMIT_OPTS', 'JAVA_TOOL_OPTIONS',
                '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS', 'PYSPARK_GATEWAY_PORT',
                'SPARK_CONF_DIR', 'SPARK_HOME'):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv('SPARK_CONF_DIR', str(tmp_path / 'conf'))
    return h


@pytest.mark.parametrize('previous', [None, '', 'synthetic-python'])
def test_owned_context_options_stop_and_environment(harness, monkeypatch, previous):
    if previous is None:
        monkeypatch.delenv('PYSPARK_PYTHON', raising=False)
    else:
        monkeypatch.setenv('PYSPARK_PYTHON', previous)
    before = dict(os.environ)
    for _ in range(2):
        with module.spark_session() as session:
            assert session._sc is harness.context_api._active_spark_context
            assert os.environ['PYSPARK_PYTHON'] == sys.executable
        assert session.stops == session._sc.stops == 1
        assert dict(os.environ) == before
    assert harness.options['master'] == 'local[2]'
    assert harness.options['spark.pyspark.python'] == sys.executable
    assert harness.options['spark.jars.ivy'] == str(Path.home() / '.ivy2')
    assert harness.options['spark.sql.extensions'] == 'io.delta.sql.DeltaSparkSessionExtension'
    assert harness.options['spark.sql.catalog.spark_catalog'] == (
        'org.apache.spark.sql.delta.catalog.DeltaCatalog'
    )
    assert harness.options['spark.sql.shuffle.partitions'] == '2'
    assert harness.options['spark.databricks.delta.snapshotPartitions'] == '2'


@pytest.mark.parametrize('kind', ['context', 'active', 'instantiated'])
def test_foreign_resources_are_rejected_without_side_effects(harness, kind):
    foreign = harness.Context()
    if kind == 'context':
        harness.context_api._active_spark_context = foreign
    elif kind == 'active':
        harness.api.active = harness.Session(foreign)
    else:
        harness.api._instantiatedSession = harness.Session(foreign)
    before = dict(os.environ)
    with pytest.raises(RuntimeError, match='existing'):
        with module.spark_session():
            pytest.fail('foreign resource was yielded')
    assert harness.events == []
    assert foreign.stops == 0
    assert dict(os.environ) == before


@pytest.mark.parametrize('stage', [
    'builder', 'config', 'delta', 'start', 'after_context', 'after_session', 'log',
])
@pytest.mark.parametrize('previous', [None, '', 'synthetic-python'])
def test_startup_failures_preserve_identity_and_cleanup(harness, monkeypatch, stage, previous):
    if previous is None:
        monkeypatch.delenv('PYSPARK_PYTHON', raising=False)
    else:
        monkeypatch.setenv('PYSPARK_PYTHON', previous)
    before = dict(os.environ)
    error = RuntimeError('synthetic ' + stage)
    harness.errors[stage] = error
    with pytest.raises(RuntimeError) as caught:
        with module.spark_session():
            pytest.fail('startup failure was yielded')
    assert caught.value is error
    assert dict(os.environ) == before
    assert all(c.stops == 1 for c in harness.contexts)
    assert harness.context_api._active_spark_context is None
    assert harness.api._instantiatedSession is None


@pytest.mark.parametrize('primary', [RuntimeError('body'), KeyboardInterrupt(), SystemExit(27)])
@pytest.mark.parametrize('cleanup', [False, True])
def test_body_error_and_cancellation_survive_cleanup(harness, primary, cleanup):
    if cleanup:
        harness.errors['stop'] = OSError('synthetic cleanup')
    before = dict(os.environ)
    with pytest.raises(type(primary)) as caught:
        with module.spark_session():
            raise primary
    assert caught.value is primary
    if isinstance(primary, SystemExit):
        assert caught.value.code == 27
    if cleanup:
        assert any('synthetic cleanup' in note for note in primary.__notes__)
    assert dict(os.environ) == before


@pytest.mark.parametrize('stage', ['start', 'after_context', 'after_session', 'log'])
@pytest.mark.parametrize('kind', [KeyboardInterrupt, SystemExit])
def test_startup_cancellation_identity_and_code(harness, stage, kind):
    error = kind(29)
    harness.errors[stage] = error
    before = dict(os.environ)
    with pytest.raises(kind) as caught:
        with module.spark_session():
            pytest.fail('cancelled startup was yielded')
    assert caught.value is error
    if kind is SystemExit:
        assert caught.value.code == 29
    assert dict(os.environ) == before
    assert all(c.stops == 1 for c in harness.contexts)


@pytest.mark.parametrize('primary', [None, RuntimeError('body')])
@pytest.mark.parametrize('secondary', [OSError('stop'), KeyboardInterrupt(), SystemExit(31)])
def test_cleanup_error_propagation(harness, primary, secondary):
    harness.errors['stop'] = secondary
    expected = primary if primary is not None and isinstance(secondary, Exception) else secondary
    before = dict(os.environ)
    with pytest.raises(type(expected)) as caught:
        with module.spark_session():
            if primary is not None:
                raise primary
    assert caught.value is expected
    if expected is secondary and primary is not None:
        assert secondary.__cause__ is primary
    assert dict(os.environ) == before


def test_nested_manager_does_not_modify_outer_context(harness):
    with module.spark_session() as outer:
        before = list(harness.events), dict(os.environ)
        with pytest.raises(RuntimeError, match='existing'):
            with module.spark_session():
                pytest.fail('nested manager was yielded')
        assert (harness.events, dict(os.environ)) == before
        assert outer.stops == 0
    assert outer.stops == 1


def test_external_context_replacement_is_not_stopped_or_cleared(harness):
    with module.spark_session() as owned:
        replacement = harness.Context()
        foreign = harness.Session(replacement)
        harness.context_api._active_spark_context = replacement
        harness.api._instantiatedSession = foreign
    assert replacement.stops == foreign.stops == owned.stops == 0
    assert harness.context_api._active_spark_context is replacement
    assert harness.api._instantiatedSession is foreign


def test_mismatched_returned_session_is_not_stopped(harness, monkeypatch):
    foreign_context = harness.Context()
    foreign = harness.Session(foreign_context)

    def mismatch(self):
        harness.context_api._active_spark_context = harness.Context()
        return foreign

    monkeypatch.setattr(harness.api.Builder, 'getOrCreate', mismatch)
    with pytest.raises(RuntimeError, match="manager's context"):
        with module.spark_session():
            pytest.fail('mismatched session was yielded')
    assert foreign.stops == foreign_context.stops == 0
    assert harness.contexts[-1].stops == 1


def test_context_appearing_before_locked_recheck_is_untouched(harness, monkeypatch):
    validate = module.validate_ivy_directory
    foreign = harness.Context()

    def interleave(*args, **kwargs):
        result = validate(*args, **kwargs)
        harness.context_api._active_spark_context = foreign
        return result

    monkeypatch.setattr(module, 'validate_ivy_directory', interleave)
    with pytest.raises(RuntimeError, match='existing'):
        with module.spark_session():
            pytest.fail('foreign context was yielded')
    assert harness.events == []
    assert foreign.stops == 0


def run_thread(function):
    results = Queue()

    def target():
        try:
            results.put((True, function()))
        except BaseException as exc:
            results.put((False, exc))

    thread = Thread(target=target, daemon=True)
    thread.start()
    return thread, results


def finish_thread(thread, results):
    thread.join(timeout=3)
    assert not thread.is_alive(), 'synthetic lock test exceeded its deadline'
    ok, result = results.get(timeout=1)
    if not ok:
        raise result
    return result


def test_reentrant_startup_and_locks_released_during_user_block(harness):
    def use_manager():
        with module.spark_session():
            def inspect():
                with module._spark_locks():
                    return True
            thread, results = run_thread(inspect)
            assert finish_thread(thread, results)
    thread, results = run_thread(use_manager)
    finish_thread(thread, results)


@pytest.mark.parametrize('which', ['builder', 'context'])
def test_lock_contention_times_out_without_leak(harness, which):
    acquired, release = Event(), Event()
    lock = harness.api.Builder._lock if which == 'builder' else harness.context_api._lock

    def hold():
        with lock:
            acquired.set()
            assert release.wait(timeout=3)

    thread, results = run_thread(hold)
    try:
        assert acquired.wait(timeout=1)
        with pytest.raises(RuntimeError, match='Timed out'):
            with module.spark_session():
                pytest.fail('locked startup was yielded')
        assert harness.events == []
    finally:
        release.set()
        finish_thread(thread, results)
    with module._spark_locks():
        pass


@pytest.mark.parametrize('which', ['builder', 'context'])
def test_lock_acquisition_cancellation_releases_previous_lock(harness, monkeypatch, which):
    class CancelLock:
        def _recursion_count(self):
            return 0

        def acquire(self, **kwargs):
            raise KeyboardInterrupt('synthetic lock cancellation')
    owner = harness.api.Builder if which == 'builder' else harness.context_api
    original = owner._lock
    monkeypatch.setattr(owner, '_lock', CancelLock())
    with pytest.raises(KeyboardInterrupt):
        with module.spark_session():
            pytest.fail('cancelled lock was yielded')
    monkeypatch.setattr(owner, '_lock', original)
    thread, results = run_thread(lambda: acquire_both())
    finish_thread(thread, results)


def acquire_both():
    with module._spark_locks():
        return True


@pytest.mark.parametrize('destination', ['raw', 'lakehouse', 'checks'])
def test_invalid_ivy_rejected_before_builder_and_environment(harness, destination):
    from tfm_lakehouse.config import project_protected_roots
    roots = project_protected_roots()
    before = dict(os.environ)
    candidate = roots[['raw', 'lakehouse', 'checks'].index(destination)] / 'ivy'
    with pytest.raises(ConfigurationError):
        with module.spark_session(ivy_dir=candidate):
            pytest.fail('unsafe Ivy was yielded')
    assert harness.events == []
    assert dict(os.environ) == before


@pytest.mark.parametrize('key,value', [
    ('PYSPARK_SUBMIT_ARGS', '--conf spark.jars.ivy=/unsafe pyspark-shell'),
    ('PYSPARK_SUBMIT_ARGS', '--conf=spark.jars.ivySettings=/custom.xml pyspark-shell'),
    ('PYSPARK_SUBMIT_ARGS', '--properties-file /custom.conf pyspark-shell'),
    ('PYSPARK_SUBMIT_ARGS', '--driver-java-options @/custom-arguments pyspark-shell'),
    ('PYSPARK_SUBMIT_ARGS', '--conf spark.driver.extraJavaOptions=@/custom-arguments pyspark-shell'),
    ('PYSPARK_SUBMIT_ARGS', '--conf \'spark.driver.extraJavaOptions="@/custom-arguments"\' pyspark-shell'),
    ('JAVA_TOOL_OPTIONS', '-Divy.default.ivy.user.dir=/unsafe'),
    ('SPARK_SUBMIT_OPTS', '-Dspark.jars.ivy=/unsafe'),
    ('PYSPARK_GATEWAY_PORT', '12345'),
])
def test_external_ivy_override_rejected_before_builder(harness, monkeypatch, key, value):
    monkeypatch.setenv(key, value)
    before = dict(os.environ)
    with pytest.raises(ConfigurationError):
        with module.spark_session():
            pytest.fail('external override was yielded')
    assert harness.events == []
    assert dict(os.environ) == before


def test_external_ivy_defaults_are_rejected(harness, tmp_path, monkeypatch):
    conf = tmp_path / 'conf'
    conf.mkdir()
    (conf / 'spark-defaults.conf').write_text('spark.jars.ivySettings /custom.xml\n')
    monkeypatch.setenv('SPARK_CONF_DIR', str(conf))
    with pytest.raises(ConfigurationError):
        with module.spark_session():
            pytest.fail('external defaults were yielded')
    assert harness.events == []


def test_real_delta_helper_selects_installed_coordinate_without_starting_jvm():
    from delta import configure_spark_with_delta_pip
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    before = SparkContext._gateway, SparkContext._active_spark_context
    builder = configure_spark_with_delta_pip(SparkSession.builder)
    assert builder._options['spark.jars.packages'] == 'io.delta:delta-spark_2.12:3.3.3'
    assert (SparkContext._gateway, SparkContext._active_spark_context) == before


@pytest.mark.parametrize('contents', [
    r'spark.jars.\u0069vySettings /custom.xml' + '\n',
    'spark.jars.\\\n ivySettings /custom.xml\n',
])
def test_escaped_java_properties_cannot_hide_ivy_override(
        harness, tmp_path, monkeypatch, contents):
    conf = tmp_path / 'conf'
    conf.mkdir()
    (conf / 'spark-defaults.conf').write_text(contents)
    monkeypatch.setenv('SPARK_CONF_DIR', str(conf))
    with pytest.raises(ConfigurationError):
        with module.spark_session():
            pytest.fail('escaped defaults were yielded')
    assert harness.events == []


@pytest.mark.parametrize('filename', ['spark-env.sh', 'java-opts'])
def test_custom_launcher_file_is_rejected_without_execution(
        harness, tmp_path, monkeypatch, filename):
    conf = tmp_path / 'conf'
    conf.mkdir()
    (conf / filename).write_text('exit 99\n')
    monkeypatch.setenv('SPARK_CONF_DIR', str(conf))
    with pytest.raises(ConfigurationError, match=filename):
        with module.spark_session():
            pytest.fail('custom script was yielded')
    assert harness.events == []


def test_non_ivy_launcher_settings_remain_usable(harness, tmp_path, monkeypatch):
    conf = tmp_path / 'conf'
    conf.mkdir()
    (conf / 'spark-defaults.conf').write_text(
        '# spark.jars.ivy ignored comment\nspark.ui.enabled false\n'
    )
    monkeypatch.setenv('SPARK_CONF_DIR', str(conf))
    monkeypatch.setenv('PYSPARK_SUBMIT_ARGS', '--conf spark.ui.enabled=false pyspark-shell')
    with module.spark_session(master='local[1]'):
        pass
    assert harness.options['master'] == 'local[1]'


def test_lock_order_is_builder_then_context_and_release_reversed(harness):
    trace = []

    class Lock:
        def __init__(self, name):
            self.name = name
            self.lock = RLock()

        def acquire(self, **options):
            trace.append('acquire:' + self.name)
            return self.lock.acquire(**options)

        def release(self):
            trace.append('release:' + self.name)
            self.lock.release()

        def _recursion_count(self):
            return self.lock._recursion_count()

    harness.api.Builder._lock = Lock('builder')
    harness.context_api._lock = Lock('context')
    with module._spark_locks():
        pass
    assert trace == ['acquire:builder', 'acquire:context', 'release:context', 'release:builder']


@pytest.mark.parametrize('stage', ['builder', 'delta', 'start', 'after_context', 'log'])
def test_failed_startup_releases_locks_for_other_thread(harness, stage):
    harness.errors[stage] = RuntimeError('synthetic')
    with pytest.raises(RuntimeError):
        with module.spark_session():
            pytest.fail('failed startup was yielded')
    thread, results = run_thread(acquire_both)
    assert finish_thread(thread, results)


def test_foreign_default_session_is_not_cleared_when_own_context_stops(harness):
    with module.spark_session() as owned:
        foreign = harness.Session(harness.Context())
        harness.api._instantiatedSession = foreign
    assert owned._sc.stops == 1
    assert owned.stops == foreign.stops == foreign._sc.stops == 0
    assert harness.api._instantiatedSession is foreign


def test_default_ivy_inside_custom_protected_root_is_rejected(harness):
    before = dict(os.environ)
    with pytest.raises(ConfigurationError):
        with module.spark_session(protected_roots=(Path.home(),)):
            pytest.fail('unsafe default was yielded')
    assert harness.events == []
    assert dict(os.environ) == before


@pytest.mark.parametrize('which', ['builder', 'context'])
@pytest.mark.parametrize('preowned', [False, True])
def test_cancellation_after_acquisition_preserves_original_lock_depth(
        harness, monkeypatch, which, preowned):
    owner = harness.api.Builder if which == 'builder' else harness.context_api
    actual = owner._lock

    class AcquiredThenCancelled:
        def _recursion_count(self):
            return actual._recursion_count()

        def acquire(self, **options):
            assert actual.acquire(**options)
            raise KeyboardInterrupt('synthetic cancellation after acquisition')

        def release(self):
            actual.release()

    if preowned:
        actual.acquire()
    before = actual._recursion_count()
    try:
        monkeypatch.setattr(owner, '_lock', AcquiredThenCancelled())
        with pytest.raises(KeyboardInterrupt):
            with module._spark_locks():
                pytest.fail('cancelled acquisition was yielded')
        assert actual._recursion_count() == before
    finally:
        monkeypatch.setattr(owner, '_lock', actual)
        if preowned:
            actual.release()
    thread, results = run_thread(acquire_both)
    assert finish_thread(thread, results)


@pytest.mark.parametrize('startup_fails', [False, True])
def test_cancellation_at_ownership_registration_cleans_partial_context(harness, startup_fails):
    import inspect
    lines, start = inspect.getsourcelines(module.spark_session)
    target = next(start + offset for offset, line in enumerate(lines)
                  if line.strip() == 'owned = SparkContext._active_spark_context')
    cancellation = KeyboardInterrupt('synthetic registration cancellation')
    if startup_fails:
        harness.errors['after_context'] = RuntimeError('synthetic initial startup failure')
    injected = False

    def trace(frame, event, arg):
        nonlocal injected
        if (not injected and event == 'line' and frame.f_code.co_filename == module.__file__
                and frame.f_lineno == target):
            injected = True
            raise cancellation
        return trace

    previous = sys.gettrace()
    before = dict(os.environ)
    try:
        sys.settrace(trace)
        with pytest.raises(KeyboardInterrupt) as caught:
            with module.spark_session():
                pytest.fail('cancelled registration was yielded')
    finally:
        sys.settrace(previous)
    assert injected and caught.value is cancellation
    assert harness.contexts[0].stops == 1
    assert harness.context_api._active_spark_context is None
    assert dict(os.environ) == before
    thread, results = run_thread(acquire_both)
    assert finish_thread(thread, results)


def test_unsupported_lock_implementation_rejected_before_acquisition(harness, monkeypatch):
    class Unsupported:
        def acquire(self, **options):
            pytest.fail('unsupported lock must not be acquired')
    monkeypatch.setattr(harness.api.Builder, '_lock', Unsupported())
    with pytest.raises(RuntimeError, match='CPython'):
        with module.spark_session():
            pytest.fail('unsupported lock was yielded')
    assert harness.events == []


def test_jvm_argument_file_is_rejected_before_builder(harness, monkeypatch):
    monkeypatch.setenv('JDK_JAVA_OPTIONS', '@/synthetic-unread-arguments')
    with pytest.raises(ConfigurationError):
        with module.spark_session():
            pytest.fail('external argument file was yielded')
    assert harness.events == []


@pytest.mark.parametrize('options', [
    'spark.driver.extraJavaOptions @/custom-arguments',
    'spark.driver.extraJavaOptions:@/custom-arguments',
    'spark.driver.extraJavaOptions "@/custom-arguments"',
    "spark.driver.extraJavaOptions '@/custom-arguments'",
    r'spark.driver.extraJavaOptions \@/custom-arguments',
])
def test_driver_jvm_argument_file_in_defaults_is_rejected(
        harness, tmp_path, monkeypatch, options):
    conf = tmp_path / 'conf'
    conf.mkdir()
    (conf / 'spark-defaults.conf').write_text(options + '\n')
    monkeypatch.setenv('SPARK_CONF_DIR', str(conf))
    with pytest.raises(ConfigurationError):
        with module.spark_session():
            pytest.fail('driver argument file was yielded')
    assert harness.events == []


def test_empty_conf_dir_uses_launcher_default(harness, tmp_path, monkeypatch):
    conf = tmp_path / 'distribution/conf'
    conf.mkdir(parents=True)
    (conf / 'java-opts').write_text('@/custom-arguments\n')
    monkeypatch.setenv('SPARK_HOME', str(conf.parent))
    monkeypatch.setenv('SPARK_CONF_DIR', '')
    with pytest.raises(ConfigurationError, match='java-opts'):
        with module.spark_session():
            pytest.fail('default launcher options were yielded')
    assert harness.events == []
