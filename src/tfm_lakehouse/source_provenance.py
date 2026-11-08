"""Bind parsers to explicitly supplied bytes and expected SHA-256 digests."""
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterator
import hashlib
import io
import re
from typing import BinaryIO, Callable, TextIO, TypeVar


class ProvenanceError(ValueError):
    """Raised when source bytes cannot be completely verified and parsed."""


@dataclass(frozen=True)
class SourceIdentity:
    """Identity of bytes consumed by the parser, not proof of origin or path stability."""

    sha256: str
    bytes_read: int


_T = TypeVar("_T")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


def _check_inputs(source: BinaryIO, expected_sha256: str, max_bytes: int) -> None:
    if not isinstance(expected_sha256, str) or not _DIGEST.fullmatch(expected_sha256):
        raise ProvenanceError("Expected SHA-256 must be 64 lowercase hexadecimal characters")
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ProvenanceError("max_bytes must be a positive integer")
    try:
        if not source.seekable() or source.tell() != 0:
            raise ProvenanceError("Source must be seekable and positioned at its beginning")
    except (AttributeError, OSError, ValueError) as exc:
        raise ProvenanceError("Source position cannot be validated") from exc


class _HashingReader(io.RawIOBase):
    """Hash every byte supplied to a text parser without owning its source."""

    def __init__(self, source: BinaryIO, max_bytes: int):
        super().__init__()
        self.source = source
        self.max_bytes = max_bytes
        self.digest = hashlib.sha256()
        self.bytes_read = 0
        self.eof = False

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        try:
            chunk = self.source.read(len(buffer))
        except OSError as exc:
            raise ProvenanceError("Source read failed") from exc
        if not isinstance(chunk, bytes):
            raise ProvenanceError("Source must return binary bytes")
        if not chunk:
            self.eof = True
            return 0
        self.bytes_read += len(chunk)
        if self.bytes_read > self.max_bytes:
            raise ProvenanceError("Source exceeds the configured byte limit")
        self.digest.update(chunk)
        buffer[:len(chunk)] = chunk
        return len(chunk)


def parse_verified_csv(
    source: BinaryIO, expected_sha256: str, parser: Callable[[TextIO], _T], *, max_bytes: int,
) -> tuple[_T, SourceIdentity]:
    """Return parsed data only after parser completion, EOF and matching raw-byte hash.

    The parser must consume the full text stream and must not publish provisional
    results. The caller owns the seekable binary stream. A digest identifies the
    bytes parsed; it does not prove source origin or future pathname stability.
    """
    _check_inputs(source, expected_sha256, max_bytes)
    hashed = _HashingReader(source, max_bytes)
    try:
        with io.TextIOWrapper(io.BufferedReader(hashed), encoding="utf-8-sig", newline="") as text:
            result = parser(text)
            if isinstance(result, Iterator):
                raise ProvenanceError("Parser must not return a lazy iterator")
            if text.read(1) != "" or not hashed.eof:
                raise ProvenanceError("Parser did not consume the complete source")
            digest = hashed.digest.hexdigest()
            if digest != expected_sha256:
                raise ProvenanceError("Source SHA-256 mismatch")
            identity = SourceIdentity(digest, hashed.bytes_read)
    except (UnicodeError, OSError) as exc:
        raise ProvenanceError("Source could not be fully read as UTF-8") from exc
    return result, identity


def read_verified_snapshot(
    source: BinaryIO, expected_sha256: str, *, max_bytes: int,
) -> tuple[bytes, SourceIdentity]:
    """Read a bounded immutable byte snapshot and verify it before returning."""
    _check_inputs(source, expected_sha256, max_bytes)
    digest = hashlib.sha256()
    data = bytearray()
    try:
        while True:
            chunk = source.read(min(65_536, max_bytes + 1 - len(data)))
            if not isinstance(chunk, bytes):
                raise ProvenanceError("Source must return binary bytes")
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > max_bytes:
                raise ProvenanceError("Source exceeds the configured byte limit")
            digest.update(chunk)
    except OSError as exc:
        raise ProvenanceError("Source read failed") from exc
    result = digest.hexdigest()
    if result != expected_sha256:
        raise ProvenanceError("Source SHA-256 mismatch")
    return bytes(data), SourceIdentity(result, len(data))
