from dataclasses import replace
from pathlib import Path
import pytest

from tfm_lakehouse.config import BronzeConfig, ConfigurationError, load_config

pytestmark = pytest.mark.unit


@pytest.fixture
def config(tmp_path):
    return BronzeConfig(
        raw_root=tmp_path / "raw", lakehouse_root=tmp_path / "lake",
        electricity=tmp_path / "raw/e.csv", metadata=tmp_path / "raw/m.csv",
        data_kind="synthetic", dataset_reference="synthetic-test",
    )


def test_namespace_separates_real_and_synthetic(config):
    config.validate()
    assert config.namespace != replace(config, data_kind="real").namespace


@pytest.mark.parametrize("change", ["overlap", "kind", "reference", "outside", "same"])
def test_unsafe_configuration_rejected(config, change):
    options = {
        "overlap": {"lakehouse_root": config.raw_root / "lake"},
        "kind": {"data_kind": "unknown"},
        "reference": {"dataset_reference": ""},
        "outside": {"electricity": config.raw_root.parent / "outside.csv"},
        "same": {"metadata": config.electricity},
    }
    with pytest.raises(ConfigurationError):
        replace(config, **options[change]).validate()


def test_paths_are_relative_to_config_file(tmp_path, monkeypatch):
    project = Path(__file__).resolve().parents[1]
    path = project / "config/bronze.synthetic.toml"
    expected = load_config(path)
    monkeypatch.chdir(tmp_path)
    actual = load_config(path)
    assert actual == expected
    assert actual.electricity.is_file()


def test_missing_configuration(tmp_path):
    with pytest.raises(ConfigurationError):
        load_config(tmp_path / "absent.toml")


def test_symlink_destination_cannot_escape_lakehouse(config):
    config.lakehouse_root.mkdir()
    config.raw_root.mkdir()
    (config.lakehouse_root / "synthetic").symlink_to(config.raw_root, target_is_directory=True)
    with pytest.raises(ConfigurationError, match="enlazado"):
        config.validate()


@pytest.fixture
def ivy_paths(tmp_path, monkeypatch):
    from tfm_lakehouse import config as module
    roots = tuple(tmp_path / name for name in ('raw', 'lakehouse', 'checks'))
    monkeypatch.setattr(module, 'project_protected_roots', lambda: roots)
    monkeypatch.setattr(Path, 'home', classmethod(lambda cls: tmp_path / 'home'))
    return tmp_path, roots


def synthetic_files(root):
    return {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*')
            if p.is_file() and not p.is_symlink()}


@pytest.mark.parametrize('index', [0, 1, 2])
@pytest.mark.parametrize('position', ['equal', 'inside', 'ancestor', 'alias'])
def test_ivy_protected_overlap_is_rejected_without_writes(ivy_paths, index, position):
    from tfm_lakehouse.config import validate_ivy_directory
    base, roots = ivy_paths
    protected = roots[index]
    protected.mkdir()
    (protected / 'sentinel.csv').write_bytes(b'synthetic\x00\r\n')
    candidate = {'equal': protected, 'inside': protected / 'new/ivy',
                 'ancestor': base, 'alias': base / 'alias'}[position]
    if position == 'alias':
        candidate.symlink_to(protected, target_is_directory=True)
    before = synthetic_files(base)
    with pytest.raises(ConfigurationError):
        validate_ivy_directory(candidate)
    assert synthetic_files(base) == before
    assert not (protected / 'new').exists()


@pytest.mark.parametrize('relative', ['external/ivy', '.cache/ivy', 'raw-cache', 'home/.ivy2'])
def test_safe_new_ivy_paths_are_accepted_without_creation(ivy_paths, relative):
    from tfm_lakehouse.config import validate_ivy_directory
    base, _ = ivy_paths
    candidate = base / relative
    assert validate_ivy_directory(candidate) == candidate
    assert not candidate.exists()


def test_existing_ivy_root_alias_to_safe_cache_is_accepted(ivy_paths):
    from tfm_lakehouse.config import validate_ivy_directory
    base, _ = ivy_paths
    safe = base / 'safe'
    safe.mkdir()
    alias = base / 'alias'
    alias.symlink_to(safe, target_is_directory=True)
    assert validate_ivy_directory(alias) == safe


@pytest.mark.parametrize('subtree', ['cache', 'jars'])
@pytest.mark.parametrize('location', ['top', 'nested'])
@pytest.mark.parametrize('target', ['safe', 'protected', 'dangling'])
def test_internal_ivy_links_are_rejected(ivy_paths, subtree, location, target):
    from tfm_lakehouse.config import validate_ivy_directory
    base, roots = ivy_paths
    cache = base / 'ivy'
    link = cache / subtree if location == 'top' else cache / subtree / 'group/artifact/link'
    link.parent.mkdir(parents=True)
    destination = roots[0] if target == 'protected' else base / target
    if target != 'dangling':
        destination.mkdir()
        (destination / 'sentinel').write_bytes(b'synthetic sentinel')
    link.symlink_to(destination, target_is_directory=True)
    before = synthetic_files(base)
    with pytest.raises(ConfigurationError):
        validate_ivy_directory(cache)
    assert synthetic_files(base) == before


@pytest.mark.parametrize('kind', ['dangling', 'cycle', 'file', 'file_ancestor', 'cache_file'])
def test_invalid_ivy_components_fail_closed(ivy_paths, kind):
    from tfm_lakehouse.config import validate_ivy_directory
    base, _ = ivy_paths
    candidate = base / 'ivy'
    if kind == 'dangling':
        candidate.symlink_to(base / 'absent', target_is_directory=True)
    elif kind == 'cycle':
        candidate.symlink_to(candidate)
    elif kind in {'file', 'file_ancestor'}:
        candidate.write_bytes(b'synthetic')
        if kind == 'file_ancestor':
            candidate = candidate / 'new'
    else:
        candidate.mkdir()
        (candidate / 'cache').write_bytes(b'synthetic')
    with pytest.raises(ConfigurationError):
        validate_ivy_directory(candidate)


@pytest.mark.parametrize('stage', ['lstat', 'scandir'])
def test_ivy_inspection_errors_are_not_interpreted_as_absence(ivy_paths, monkeypatch, stage):
    from tfm_lakehouse import config as module
    base, _ = ivy_paths
    candidate = base / 'ivy'
    (candidate / 'cache').mkdir(parents=True)

    def fail(*args, **kwargs):
        raise PermissionError('synthetic inspection denied')

    monkeypatch.setattr(Path if stage == 'lstat' else module.os, stage, fail)
    with pytest.raises(ConfigurationError, match='inspect'):
        module.validate_ivy_directory(candidate)


def test_default_ivy_is_explicit_and_independent_of_cwd(ivy_paths, monkeypatch):
    from tfm_lakehouse.config import validate_ivy_directory
    base, _ = ivy_paths
    expected = base / 'home/.ivy2'
    assert validate_ivy_directory(None) == expected
    elsewhere = base / 'elsewhere'
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    assert validate_ivy_directory(None) == expected
    assert not expected.exists()


def test_relative_api_ivy_uses_invocation_directory(ivy_paths, monkeypatch):
    from tfm_lakehouse.config import validate_ivy_directory
    base, _ = ivy_paths
    monkeypatch.chdir(base)
    assert validate_ivy_directory(Path('.cache/ivy')) == base / '.cache/ivy'


def test_explicit_roots_extend_project_protections(ivy_paths):
    from tfm_lakehouse.config import validate_ivy_directory
    base, roots = ivy_paths
    custom = base / 'custom-source'
    for protected in (*roots, custom):
        with pytest.raises(ConfigurationError):
            validate_ivy_directory(protected / 'ivy', protected_roots=(custom,))


def test_standalone_ivy_requires_explicit_protection_context(tmp_path, monkeypatch):
    from tfm_lakehouse import config as module
    monkeypatch.setattr(module, 'project_protected_roots', lambda: ())
    with pytest.raises(ConfigurationError, match='protected_roots'):
        module.validate_ivy_directory(tmp_path / 'ivy')
    assert module.validate_ivy_directory(
        tmp_path / 'ivy', protected_roots=(tmp_path / 'sources',)
    ) == tmp_path / 'ivy'


def test_bronze_config_rejects_ivy_in_custom_roots(config):
    for root in (config.raw_root, config.lakehouse_root):
        with pytest.raises(ConfigurationError):
            replace(config, ivy_dir=root / 'ivy').validate()


@pytest.mark.parametrize('kind', ['relative', 'safe_alias', 'dangling_alias', 'unsafe_alias'])
def test_toml_ivy_keeps_lexical_safety_and_config_relative_paths(ivy_paths, monkeypatch, kind):
    base, roots = ivy_paths
    directory = base / 'config'
    directory.mkdir()
    safe = base / 'safe'
    safe.mkdir()
    alias = base / 'alias'
    if kind != 'relative':
        alias.symlink_to({'safe_alias': safe, 'dangling_alias': base / 'absent',
                          'unsafe_alias': roots[0]}[kind], target_is_directory=True)
        if kind == 'unsafe_alias':
            roots[0].mkdir()
    target = directory / 'synthetic.toml'
    ivy = '../safe' if kind == 'relative' else '../alias'
    target.write_text(
        '[paths]\nraw_root="../raw"\nlakehouse_root="../lakehouse"\n'
        '[dataset]\nkind="synthetic"\nreference="synthetic"\n'
        '[sources]\nelectricity="e.csv"\nmetadata="m.csv"\n'
        f'[spark]\nivy_dir="{ivy}"\n'
    )
    monkeypatch.chdir(base.parent)
    if kind in {'dangling_alias', 'unsafe_alias'}:
        with pytest.raises(ConfigurationError):
            load_config(target)
    else:
        assert load_config(target).ivy_dir == safe
