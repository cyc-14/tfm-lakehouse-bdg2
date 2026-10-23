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
