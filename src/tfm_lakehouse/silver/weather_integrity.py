"""Physical weather tree evidence using D24-compatible bounded file reads."""
import hashlib
import os
from pathlib import Path

from tfm_lakehouse.silver import silver_iqr_publication as iqr
from tfm_lakehouse.silver import silver_synthetic_publication as silver

_BLOCK_SIZE = 1024 * 1024


def weather_tree_fingerprint(folder: Path) -> str:
    """Hash the complete tree with D24's relative-path/content semantics.

    Per-file reads are bounded; the mapping of all paths remains on the driver.
    As in D24 this detects inconsistency, not authenticated external provenance.
    """
    silver._check_tree(folder)
    iqr._require(folder.is_dir(), "Weather source directory is unavailable")
    files = {}
    for path in folder.rglob("*"):
        if path.is_file():
            digest = hashlib.sha256()
            with silver._open_regular(path, os.O_RDONLY) as handle:
                for block in iter(lambda: handle.read(_BLOCK_SIZE), b""):
                    digest.update(block)
            files[str(path.relative_to(folder))] = digest.hexdigest()
    return iqr._digest(files)
