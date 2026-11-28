"""Explicitly reserved local workspaces for synthetic scalability campaigns.

This is a cooperative local boundary, not isolation against a hostile process
that replaces directories after validation. Never use it for real sources.
"""
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import stat
import subprocess


class WorkspaceError(RuntimeError):
    """A campaign cannot be safely reserved or used."""


PROJECT_ROOT = Path(__file__).absolute().parents[2]
SCALABILITY_ROOT = PROJECT_ROOT / "data/lakehouse/synthetic/scalability"
CAMPAIGN_JARS = ("io.delta_delta-spark_2.12-3.3.3.jar", "io.delta_delta-storage-3.3.3.jar",
                 "org.antlr_antlr4-runtime-4.9.3.jar")
_IDENTIFIER = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_LAYOUT = ("raw", "lakehouse", "silver-publications", "iqr-publications",
           "contextual-publications", "weather-captures", "weather-publications",
           "spark-local", "spark-conf", "temp", "ivy", "evidence")
_AUTHORIZED: dict[Path, "Campaign"] = {}


@dataclass(frozen=True)
class Campaign:
    root: Path
    campaign_id: str
    owner_id: str


def _id(value: str) -> None:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise WorkspaceError("Invalid campaign or owner identifier")


def _real_config_present() -> bool:
    return "TFM_REAL_CONFIG" in os.environ


def _lexical_root(path: Path) -> Path:
    if (not isinstance(path, Path) or not path.is_absolute() or ".." in path.parts
            or path.parent != SCALABILITY_ROOT or not _IDENTIFIER.fullmatch(path.name)):
        raise WorkspaceError("Campaign path is outside the authorized namespace")
    return path


def _check_components(path: Path) -> None:
    for component in reversed((path, *path.parents)):
        try:
            info = component.lstat()
        except FileNotFoundError:
            continue
        if not stat.S_ISDIR(info.st_mode):
            raise WorkspaceError("Campaign path contains a link or non-directory")


def _check_tree(root: Path) -> None:
    _check_components(root)
    pending = [root]
    while pending:
        current = pending.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(info.st_mode):
                    pending.append(Path(entry.path))
                elif not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise WorkspaceError("Campaign contains a link or special file")


def _check_git_exclusion() -> None:
    relative = SCALABILITY_ROOT.relative_to(PROJECT_ROOT).as_posix() + "/"
    env = dict(os.environ, GIT_OPTIONAL_LOCKS="0")
    try:
        ignored = subprocess.run(["git", "--no-optional-locks", "check-ignore", "-q", "--", relative],
                                 cwd=PROJECT_ROOT, env=env, check=False)
        tracked = subprocess.run(["git", "--no-optional-locks", "ls-files", "--", relative],
                                 cwd=PROJECT_ROOT, env=env, check=True, capture_output=True)
    except (OSError, subprocess.SubprocessError) as exc:
        raise WorkspaceError("Cannot verify Git exclusion") from exc
    if ignored.returncode != 0 or tracked.stdout:
        raise WorkspaceError("Campaign namespace is not safely excluded from Git")


def _check_disk_backed(path: Path) -> None:
    """Fail closed for unknown mounts; the approved WSL workspace uses ext4."""
    try:
        result = subprocess.run(["findmnt", "-n", "-T", str(path), "-o", "FSTYPE"],
                                check=True, capture_output=True, text=True)
    except (OSError, subprocess.SubprocessError) as exc:
        raise WorkspaceError("Cannot verify campaign filesystem") from exc
    if result.stdout.strip() not in {"ext4", "xfs", "btrfs"}:
        raise WorkspaceError("Campaign filesystem is not an approved disk filesystem")


def _reject_submounts(root: Path) -> None:
    """Reject nested mounts, including bind mounts, before traversing a campaign."""
    try:
        lines = Path('/proc/self/mountinfo').read_text(encoding='utf-8').splitlines()
        for line in lines:
            encoded = line.split()[4]
            decoded = re.sub(r'\\([0-7]{3})', lambda m: chr(int(m.group(1), 8)), encoded)
            mount = Path(decoded)
            if mount == root or mount.is_relative_to(root):
                raise WorkspaceError("Campaign contains a nested filesystem mount")
    except (OSError, UnicodeError, IndexError) as exc:
        raise WorkspaceError("Cannot verify campaign mounts") from exc


def reserve_campaign(campaign_id: str, owner_id: str) -> Campaign:
    """Create a new campaign once; retain incomplete state after any failure."""
    _id(campaign_id)
    _id(owner_id)
    root = _lexical_root(SCALABILITY_ROOT / campaign_id)
    if _real_config_present():
        raise WorkspaceError("Real configuration must be unset")
    _check_git_exclusion()
    _check_components(root)
    SCALABILITY_ROOT.mkdir(parents=True, exist_ok=True)
    _check_components(SCALABILITY_ROOT)
    _check_disk_backed(SCALABILITY_ROOT)
    try:
        root.mkdir(mode=0o700)
    except FileExistsError as exc:
        raise WorkspaceError("Campaign is already reserved") from exc
    marker = {"campaign_id": campaign_id, "owner_id": owner_id, "contract_version": 1}
    # Exclusive marker and directories make partial initialization visible; never clean it up.
    fd = os.open(root / "owner.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump(marker, stream, sort_keys=True, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())
    for name in _LAYOUT:
        (root / name).mkdir(mode=0o700)
    campaign = Campaign(root, campaign_id, owner_id)
    _AUTHORIZED[root] = campaign
    return campaign


def validate_campaign(campaign: Campaign) -> Path:
    if not isinstance(campaign, Campaign):
        raise WorkspaceError("Explicit campaign ownership is required")
    _id(campaign.campaign_id)
    _id(campaign.owner_id)
    root = _lexical_root(campaign.root)
    if root.name != campaign.campaign_id or _real_config_present():
        raise WorkspaceError("Campaign identity or environment differs")
    _check_git_exclusion()
    _check_components(root)
    _check_disk_backed(root)
    _reject_submounts(root)
    _check_tree(root)
    marker = root / "owner.json"
    fd = os.open(marker, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise WorkspaceError("Invalid campaign ownership evidence")
        with os.fdopen(fd, encoding="utf-8") as stream:
            fd = -1
            actual = json.load(stream)
    finally:
        if fd >= 0:
            os.close(fd)
    if actual != {"campaign_id": campaign.campaign_id, "owner_id": campaign.owner_id,
                  "contract_version": 1}:
        raise WorkspaceError("Campaign ownership differs")
    for name in _LAYOUT:
        if not (root / name).is_dir():
            raise WorkspaceError("Campaign layout is incomplete")
        _check_disk_backed(root / name)
    return root


def authorize_campaign(campaign_id: str, owner_id: str) -> Campaign:
    """Explicitly attach to a complete retained campaign owned by the caller."""
    _id(campaign_id)
    _id(owner_id)
    campaign = Campaign(SCALABILITY_ROOT / campaign_id, campaign_id, owner_id)
    validate_campaign(campaign)
    _AUTHORIZED[campaign.root] = campaign
    return campaign


def validate_authorized_campaign(campaign: Campaign) -> Path:
    if not isinstance(campaign, Campaign) or _AUTHORIZED.get(campaign.root) != campaign:
        raise WorkspaceError("Campaign is not authorized in this process")
    return validate_campaign(campaign)


def validate_synthetic_config(root: Path, config) -> Path:
    """Validate paths lexically before any inspection of caller-supplied paths."""
    _lexical_root(root)
    campaign = _AUTHORIZED.get(root)
    if campaign is None or config.data_kind != "synthetic":
        raise WorkspaceError("Campaign is not authorized for synthetic execution")
    if (config.raw_root != root / "raw" or config.lakehouse_root != root / "lakehouse"
            or config.ivy_dir is not None):
        raise WorkspaceError("Synthetic roots do not match the campaign layout")
    sources = list(config.sources.values())
    if (not sources or len(set(sources)) != len(sources)
            or any(not isinstance(p, Path) or not p.is_absolute() or ".." in p.parts
                   or not p.is_relative_to(root / "raw") or p == root / "raw"
                   for p in sources)):
        raise WorkspaceError("Synthetic sources must be distinct campaign files")
    validate_campaign(campaign)
    for source in sources:
        _check_components(source.parent)
        try:
            info = source.lstat()
        except FileNotFoundError:
            continue
        else:
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise WorkspaceError("Synthetic source is not an unlinked regular file")
    config.validate()
    return root
