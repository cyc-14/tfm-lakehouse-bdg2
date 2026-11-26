"""Focused synthetic tests for the D39 local workspace boundary."""
from pathlib import Path

import pytest

from tfm_lakehouse import scalability_workspace as workspace
from tfm_lakehouse.config import BronzeConfig
from tfm_lakehouse.silver import silver_synthetic_publication as silver

pytestmark = pytest.mark.unit


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    root = tmp_path / 'project' / 'data' / 'lakehouse' / 'synthetic' / 'scalability'
    monkeypatch.setattr(workspace, 'SCALABILITY_ROOT', root)
    monkeypatch.setattr(workspace, '_check_git_exclusion', lambda: None)
    monkeypatch.setattr(workspace, '_check_disk_backed', lambda _: None)
    monkeypatch.delenv('TFM_REAL_CONFIG', raising=False)
    monkeypatch.setattr(workspace, '_AUTHORIZED', {})
    return root


def config(campaign, **kwargs):
    root = campaign.root
    values = dict(raw_root=root / 'raw', lakehouse_root=root / 'lakehouse',
                  electricity=root / 'raw' / 'electricity.csv',
                  metadata=root / 'raw' / 'metadata.csv', data_kind='synthetic',
                  dataset_reference='invented')
    values.update(kwargs)
    return BronzeConfig(**values)


def test_reserve_and_authorize_synthetic_layout(sandbox):
    campaign = workspace.reserve_campaign('campaign-1', 'owner-1')
    assert campaign.root == sandbox / 'campaign-1'
    assert silver._validate_workspace(campaign.root, config(campaign)) == campaign.root
    workspace._AUTHORIZED.clear()
    with pytest.raises(silver.SilverPublicationError):
        silver._validate_workspace(campaign.root, config(campaign))
    assert workspace.authorize_campaign('campaign-1', 'owner-1') == campaign
    with pytest.raises(workspace.WorkspaceError, match='ownership'):
        workspace.authorize_campaign('campaign-1', 'foreign')


@pytest.mark.parametrize('value', ['', '.', '..', 'a/b', 'a b', 'x' * 129])
def test_invalid_identifiers_never_inspect_filesystem(sandbox, monkeypatch, value):
    monkeypatch.setattr(workspace, '_check_git_exclusion', lambda: pytest.fail('inspected'))
    with pytest.raises(workspace.WorkspaceError):
        workspace.reserve_campaign(value, 'owner')
    with pytest.raises(workspace.WorkspaceError):
        workspace.reserve_campaign('campaign', value)


def test_exclusive_reservation_and_failure_preserve_evidence(sandbox, monkeypatch):
    campaign = workspace.reserve_campaign('campaign', 'owner')
    with pytest.raises(workspace.WorkspaceError, match='reserved'):
        workspace.reserve_campaign('campaign', 'owner')
    (campaign.root / 'evidence' / 'partial').write_text('retained')
    original = Path.mkdir
    def fail(self, *args, **kwargs):
        if self.name == 'spark-local':
            raise OSError('simulated failure')
        return original(self, *args, **kwargs)
    monkeypatch.setattr(Path, 'mkdir', fail)
    with pytest.raises(OSError):
        workspace.reserve_campaign('incomplete', 'owner')
    assert (sandbox / 'incomplete' / 'owner.json').exists()
    assert (campaign.root / 'evidence' / 'partial').read_text() == 'retained'
    with pytest.raises(workspace.WorkspaceError):
        workspace.authorize_campaign('incomplete', 'owner')


@pytest.mark.parametrize('location', ['root', 'descendant', 'hardlink'])
def test_linked_campaign_rejected(sandbox, tmp_path, location):
    if location == 'root':
        sandbox.parent.mkdir(parents=True)
        sandbox.symlink_to(tmp_path, target_is_directory=True)
        with pytest.raises(workspace.WorkspaceError):
            workspace.reserve_campaign('campaign', 'owner')
        return
    campaign = workspace.reserve_campaign('campaign', 'owner')
    if location == 'descendant':
        (campaign.root / 'raw' / 'link').symlink_to(tmp_path)
    else:
        source = campaign.root / 'raw' / 'first'
        source.write_text('invented')
        (campaign.root / 'raw' / 'second').hardlink_to(source)
    with pytest.raises(workspace.WorkspaceError):
        workspace.validate_campaign(campaign)


@pytest.mark.parametrize('field', ['raw_root', 'lakehouse_root', 'electricity', 'metadata'])
def test_foreign_config_rejected_before_inspection(sandbox, tmp_path, monkeypatch, field):
    campaign = workspace.reserve_campaign('campaign', 'owner')
    foreign = tmp_path / 'protected' / 'must-not-inspect'
    bad = config(campaign, **{field: foreign})
    monkeypatch.setattr(workspace, 'validate_campaign', lambda *_: pytest.fail('inspected'))
    with pytest.raises(silver.SilverPublicationError):
        silver._validate_workspace(campaign.root, bad)


def test_traversal_and_foreign_workspace_rejected_before_inspection(sandbox, tmp_path, monkeypatch):
    campaign = workspace.reserve_campaign('campaign', 'owner')
    monkeypatch.setattr(silver, '_no_links', lambda *_: pytest.fail('inspected'))
    with pytest.raises(silver.SilverPublicationError):
        silver._validate_workspace(Path('/protected/unapproved'), config(campaign))
    with pytest.raises(silver.SilverPublicationError):
        silver._validate_workspace(sandbox / 'campaign' / '..' / 'other', config(campaign))


def test_overlapping_or_duplicate_sources_rejected(sandbox):
    campaign = workspace.reserve_campaign('campaign', 'owner')
    with pytest.raises(silver.SilverPublicationError):
        silver._validate_workspace(campaign.root, config(campaign, metadata=campaign.root / 'raw' / 'electricity.csv'))
    with pytest.raises(silver.SilverPublicationError):
        silver._validate_workspace(campaign.root, config(campaign, lakehouse_root=campaign.root / 'raw'))


def test_real_environment_rejected_without_reservation(sandbox, monkeypatch):
    monkeypatch.setattr(workspace, '_real_config_present', lambda: True)
    with pytest.raises(workspace.WorkspaceError):
        workspace.reserve_campaign('campaign', 'owner')
    assert not sandbox.exists()


def test_tmp_compatibility_with_synthetic_protected_roots(tmp_path, monkeypatch):
    monkeypatch.setattr(silver, 'project_protected_roots', lambda: (tmp_path.parent / 'other-protected',))
    campaign = workspace.Campaign(tmp_path, 'unused', 'unused')
    assert silver._validate_workspace(tmp_path, config(campaign)) == tmp_path


def test_tmp_foreign_source_rejected_before_inspection(tmp_path, monkeypatch):
    campaign = workspace.Campaign(tmp_path, 'unused', 'unused')
    foreign = tmp_path.parent / 'protected-synthetic' / 'file'
    monkeypatch.setattr(silver, '_no_links', lambda *_: pytest.fail('path inspected'))
    with pytest.raises(silver.SilverPublicationError):
        silver._validate_workspace(tmp_path, config(campaign, electricity=foreign))


def test_exclusion_failure_before_creation(sandbox, monkeypatch):
    def reject():
        raise workspace.WorkspaceError('not excluded')
    monkeypatch.setattr(workspace, '_check_git_exclusion', reject)
    with pytest.raises(workspace.WorkspaceError):
        workspace.reserve_campaign('campaign', 'owner')
    assert not sandbox.exists()


def test_changed_git_exclusion_blocks_retained_campaign(sandbox, monkeypatch):
    campaign = workspace.reserve_campaign('campaign', 'owner')
    def reject():
        raise workspace.WorkspaceError('not excluded')
    monkeypatch.setattr(workspace, '_check_git_exclusion', reject)
    with pytest.raises(workspace.WorkspaceError, match='not excluded'):
        workspace.authorize_campaign('campaign', 'owner')


def test_unapproved_filesystem_blocks_reservation(sandbox, monkeypatch):
    def reject(_):
        raise workspace.WorkspaceError('not a disk filesystem')
    monkeypatch.setattr(workspace, '_check_disk_backed', reject)
    with pytest.raises(workspace.WorkspaceError, match='filesystem'):
        workspace.reserve_campaign('campaign', 'owner')
    assert not (sandbox / 'campaign').exists()


def test_nested_mount_rejected_before_tree_traversal(sandbox, monkeypatch):
    campaign = workspace.reserve_campaign('campaign', 'owner')
    original = Path.read_text
    def fake_mountinfo(path, *args, **kwargs):
        if str(path) == '/proc/self/mountinfo':
            return f'1 0 0:1 / {campaign.root}/raw rw - ext4 /dev/fake rw\n'
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'read_text', fake_mountinfo)
    monkeypatch.setattr(workspace, '_check_tree', lambda *_: pytest.fail('traversed mount'))
    with pytest.raises(workspace.WorkspaceError, match='mount'):
        workspace.validate_campaign(campaign)
