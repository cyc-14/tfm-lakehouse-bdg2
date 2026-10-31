from contextlib import contextmanager
import json
import os
from pathlib import Path
import runpy
import sys

import pytest

from tfm_lakehouse import cli, environment
from tfm_lakehouse.environment import verify_delta


@pytest.mark.environment
def test_delta_roundtrip(spark):
    report = verify_delta(spark)
    assert report["status"] == "passed"
    assert report["rows_read"] == report["rows_written"] == 3


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/check_environment.py"


@pytest.fixture
def project(tmp_path, monkeypatch):
    root = tmp_path / "project"
    (root / "src/tfm_lakehouse").mkdir(parents=True)
    (root / "pyproject.toml").write_text('[project]\nname = "tfm-lakehouse"\n')
    monkeypatch.chdir(root)
    return root


def invoke(interface, arguments, monkeypatch):
    if interface == "cli":
        return cli.main(["check-environment", *map(str, arguments)])
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), *map(str, arguments)])
    with pytest.raises(SystemExit) as caught:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    return caught.value.code


def mock_environment(monkeypatch, report=None):
    events = []

    @contextmanager
    def session(**options):
        events.append("start")
        try:
            yield object()
        finally:
            events.append("stop")

    monkeypatch.setattr(cli, "spark_session", session)
    monkeypatch.setattr(cli, "verify_delta", lambda spark: report or {"status": "passed"})
    return events


@pytest.mark.unit
@pytest.mark.parametrize("interface", ["cli", "script"])
@pytest.mark.parametrize("absolute", [False, True])
def test_environment_report_is_complete_and_exclusive(project, monkeypatch, interface, absolute):
    target = project / "data/checks/environment/new.json"
    events = mock_environment(monkeypatch)
    original = environment.write_environment_report

    def publish(*args, **kwargs):
        assert events == ["start", "stop"]
        return original(*args, **kwargs)

    monkeypatch.setattr(cli, "write_environment_report", publish)
    argument = target if absolute else target.relative_to(project)
    assert invoke(interface, ["--report", argument], monkeypatch) == 0
    assert json.loads(target.read_text()) == {"status": "passed"}
    assert target.stat().st_mode & 0o777 == 0o600
    assert list(target.parent.iterdir()) == [target]
    assert invoke(interface, ["--report", argument], monkeypatch) == 1
    assert events == ["start", "stop"]


@pytest.mark.unit
@pytest.mark.parametrize("interface", ["cli", "script"])
@pytest.mark.parametrize("case", [
    "existing", "directory", "symlink", "dangling", "hardlink", "ancestor_data",
    "ancestor_checks", "ancestor_environment", "outside", "raw", "history",
    "h03", "lakehouse", "traversal", "extension", "nul",
])
def test_unsafe_environment_destination_rejected_before_spark(
        project, monkeypatch, interface, case):
    target = project / "data/checks/environment/new.json"
    sentinel = project / "sentinel.csv"
    sentinel.write_bytes(b"synthetic sentinel\x00\r\n")
    if case in {"existing", "directory", "symlink", "dangling", "hardlink"}:
        target.parent.mkdir(parents=True)
        if case == "existing":
            target.write_bytes(b"old report\n")
        elif case == "directory":
            target.mkdir()
        elif case == "symlink":
            target.symlink_to(sentinel)
        elif case == "dangling":
            target.symlink_to(project / "absent")
        else:
            os.link(sentinel, target)
    elif case.startswith("ancestor_"):
        level = case.removeprefix("ancestor_")
        parent = {"data": project, "checks": project / "data",
                  "environment": project / "data/checks"}[level]
        parent.mkdir(parents=True, exist_ok=True)
        (project / "elsewhere").mkdir()
        (parent / level).symlink_to(project / "elsewhere", target_is_directory=True)
    elif case == "traversal":
        target = project / "data/checks/environment/../environment/new.json"
    elif case == "extension":
        target = target.with_suffix(".csv")
    elif case == "nul":
        target = target.parent / "invalid\0.json"
    else:
        target = project / {
            "outside": "outside/new.json", "raw": "data/raw/source.json",
            "history": "data/checks/environment.json", "h03": "data/checks/h03_3/real/new.json",
            "lakehouse": "data/lakehouse/real/_runs/new.json",
        }[case]
        if case in {"history", "raw"}:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"protected historical/source sentinel\n")
    before = {str(p): p.read_bytes() for p in project.rglob("*")
              if p.is_file() and not p.is_symlink()}
    events = mock_environment(monkeypatch)
    assert invoke(interface, ["--report", target], monkeypatch) == 1
    assert events == []
    assert sentinel.read_bytes() == b"synthetic sentinel\x00\r\n"
    assert before == {str(p): p.read_bytes() for p in project.rglob("*")
                      if p.is_file() and not p.is_symlink()}


@pytest.mark.unit
@pytest.mark.parametrize("interface", ["cli", "script"])
def test_environment_without_report_does_not_require_project(tmp_path, monkeypatch, interface):
    monkeypatch.chdir(tmp_path)
    events = mock_environment(monkeypatch)
    assert invoke(interface, [], monkeypatch) == 0
    assert events == ["start", "stop"]
    assert list(tmp_path.iterdir()) == []


@pytest.mark.unit
def test_environment_report_requires_project_root_before_spark(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    events = mock_environment(monkeypatch)
    assert cli.main(["check-environment", "--report", "data/checks/environment/new.json"]) == 1
    assert events == []
    assert list(tmp_path.iterdir()) == []


@pytest.mark.unit
def test_environment_report_capabilities_checked_before_spark(project, monkeypatch):
    events = mock_environment(monkeypatch)
    monkeypatch.setattr(os, "supports_dir_fd", set())
    assert cli.main(["check-environment", "--report", "data/checks/environment/new.json"]) == 1
    assert events == []
    assert not (project / "data").exists()


@pytest.mark.unit
def test_environment_report_serialization_failure_creates_nothing(project):
    target = project / "data/checks/environment/new.json"
    with pytest.raises(TypeError):
        environment.write_environment_report(target, {"invalid": object()}, project_root=project)
    assert not (project / "data").exists()


@pytest.mark.unit
@pytest.mark.parametrize("stage", ["write", "publication"])
def test_environment_report_failure_has_no_partial_final(project, monkeypatch, stage):
    target = project / "data/checks/environment/new.json"
    original_fdopen = os.fdopen

    def failing_writer(*args, **kwargs):
        handle = original_fdopen(*args, **kwargs)
        class Writer:
            def write(self, payload):
                handle.write("{")
                raise OSError("synthetic write failure")

            def close(self):
                handle.close()
        return Writer()

    def fail_link(*args, **kwargs):
        raise OSError("synthetic publication failure")

    monkeypatch.setattr(os, "fdopen", failing_writer if stage == "write" else original_fdopen)
    if stage == "publication":
        monkeypatch.setattr(os, "link", fail_link)
    with pytest.raises(OSError, match="synthetic"):
        environment.write_environment_report(target, {"status": "passed"}, project_root=project)
    assert not target.exists()
    assert list(target.parent.iterdir()) == []


@pytest.mark.unit
def test_environment_concurrent_destination_is_not_replaced(project, monkeypatch):
    target = project / "data/checks/environment/new.json"
    original_link = os.link

    def concurrent_link(*args, **kwargs):
        target.write_bytes(b"synthetic concurrent sentinel\n")
        return original_link(*args, **kwargs)

    monkeypatch.setattr(os, "link", concurrent_link)
    with pytest.raises(FileExistsError):
        environment.write_environment_report(target, {"status": "passed"}, project_root=project)
    assert target.read_bytes() == b"synthetic concurrent sentinel\n"
    assert list(target.parent.iterdir()) == [target]


@pytest.mark.unit
def test_environment_final_name_appears_only_after_json_is_closed(project, monkeypatch):
    target = project / "data/checks/environment/new.json"
    original_link = os.link
    report = {"status": "passed", "synthetic": [0, None, 12.5]}

    def inspect_link(source, destination, **options):
        assert not target.exists()
        assert json.loads((target.parent / source).read_text()) == report
        result = original_link(source, destination, **options)
        assert json.loads(target.read_text()) == report
        return result

    monkeypatch.setattr(os, "link", inspect_link)
    environment.write_environment_report(target, report, project_root=project)


@pytest.mark.unit
def test_environment_spark_failure_does_not_create_report_directory(project, monkeypatch):
    mock_environment(monkeypatch)

    def fail(spark):
        raise RuntimeError("synthetic Spark check failure")

    monkeypatch.setattr(cli, "verify_delta", fail)
    assert cli.main(["check-environment", "--report", "data/checks/environment/new.json"]) == 1
    assert not (project / "data").exists()


@pytest.mark.unit
def test_environment_fdopen_failure_closes_descriptor(project, monkeypatch):
    target = project / "data/checks/environment/new.json"
    opened = []

    def fail(descriptor, *args, **kwargs):
        opened.append(descriptor)
        raise OSError("synthetic fdopen failure")

    monkeypatch.setattr(os, "fdopen", fail)
    with pytest.raises(OSError, match="fdopen"):
        environment.write_environment_report(target, {}, project_root=project)
    with pytest.raises(OSError):
        os.fstat(opened[0])
    assert list(target.parent.iterdir()) == []


@pytest.mark.unit
def test_environment_ancestor_changed_after_validation_is_not_followed(project, monkeypatch):
    target = project / "data/checks/environment/new.json"
    target.parent.mkdir(parents=True)
    protected = project / "data/raw"
    protected.mkdir()
    sentinel = protected / "source.csv"
    sentinel.write_bytes(b"synthetic original\n")
    original = environment._open_report_directory

    def exchange(root):
        target.parent.rename(target.parent.with_name("previous_environment"))
        target.parent.symlink_to(protected, target_is_directory=True)
        return original(root)

    monkeypatch.setattr(environment, "_open_report_directory", exchange)
    with pytest.raises(OSError):
        environment.write_environment_report(target, {}, project_root=project)
    assert sentinel.read_bytes() == b"synthetic original\n"
    assert list(protected.iterdir()) == [sentinel]


@pytest.mark.unit
@pytest.mark.parametrize("write_failure", [False, True])
def test_environment_close_failure_cannot_publish_partial_json(
        project, monkeypatch, write_failure):
    target = project / "data/checks/environment/new.json"
    original_fdopen = os.fdopen
    primary = OSError("synthetic write failure")
    secondary = OSError("synthetic close failure")

    def wrap(*args, **kwargs):
        handle = original_fdopen(*args, **kwargs)
        class Writer:
            def write(self, payload):
                handle.write("{" if write_failure else payload)
                if write_failure:
                    raise primary

            def close(self):
                handle.close()
                raise secondary
        return Writer()

    monkeypatch.setattr(os, "fdopen", wrap)
    with pytest.raises(OSError) as caught:
        environment.write_environment_report(target, {}, project_root=project)
    assert caught.value is (primary if write_failure else secondary)
    assert not target.exists()
    assert list(target.parent.iterdir()) == []


@pytest.mark.unit
def test_environment_cleanup_failure_preserves_write_cause(project, monkeypatch):
    target = project / "data/checks/environment/new.json"
    primary = OSError("synthetic publication failure")

    def fail_link(*args, **kwargs):
        raise primary

    def fail_unlink(*args, **kwargs):
        raise OSError("synthetic temporary cleanup failure")

    monkeypatch.setattr(os, "link", fail_link)
    monkeypatch.setattr(os, "unlink", fail_unlink)
    with pytest.raises(OSError) as caught:
        environment.write_environment_report(target, {}, project_root=project)
    assert caught.value is primary
    assert any("cleanup failure" in note for note in primary.__notes__)
    assert not target.exists()
    assert len(list(target.parent.glob(".environment-*.tmp"))) == 1


@pytest.mark.unit
def test_environment_directory_close_failure_does_not_leak_child(project, monkeypatch):
    original_open, original_close = os.open, os.close
    opened, closed = [], []
    first = OSError("synthetic directory close failure")

    def record_open(*args, **kwargs):
        descriptor = original_open(*args, **kwargs)
        opened.append(descriptor)
        return descriptor

    def close(descriptor):
        original_close(descriptor)
        closed.append(descriptor)
        if len(closed) == 1:
            raise first

    monkeypatch.setattr(os, "open", record_open)
    monkeypatch.setattr(os, "close", close)
    with pytest.raises(OSError) as caught:
        environment._open_report_directory(project)
    assert caught.value is first
    assert sorted(closed) == sorted(opened)
    for descriptor in opened:
        with pytest.raises(OSError):
            os.fstat(descriptor)
