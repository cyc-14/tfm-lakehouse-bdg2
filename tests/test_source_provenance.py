"""Synthetic byte-binding and stream-completion tests."""
import csv
import hashlib
import io
import os

import pytest
from openpyxl import Workbook

from tfm_lakehouse.comparison_xlsx import read_xlsx_snapshot
from tfm_lakehouse.source_provenance import (
    ProvenanceError, parse_verified_csv, read_verified_snapshot,
)


def digest(content):
    return hashlib.sha256(content).hexdigest()


def test_csv_hash_is_over_exact_parsed_bytes_and_stream_remains_open():
    content = b'\xef\xbb\xbfa,b\r\n"x,y",\r\n'
    source = io.BytesIO(content)
    parsed, identity = parse_verified_csv(source, digest(content), lambda text: list(csv.reader(text)),
                                          max_bytes=100)
    assert parsed == [["a", "b"], ["x,y", ""]]
    assert identity.sha256 == digest(content) and identity.bytes_read == len(content)
    assert not source.closed and source.getvalue() == content


@pytest.mark.parametrize("actual,expected", [(b"a\n", b"b\n"), (b"a\n", b"a\nb\n")])
def test_hash_mismatch_and_truncation_never_return_identity(actual, expected):
    with pytest.raises(ProvenanceError, match="mismatch"):
        parse_verified_csv(io.BytesIO(actual), digest(expected), lambda text: text.read(), max_bytes=100)


def test_early_parser_termination_and_lazy_results_are_rejected():
    content = b"a\nb\n"
    with pytest.raises(ProvenanceError, match="complete"):
        parse_verified_csv(io.BytesIO(content), digest(content), lambda text: next(iter(text)),
                           max_bytes=100)
    with pytest.raises(ProvenanceError, match="lazy"):
        parse_verified_csv(io.BytesIO(content), digest(content), lambda text: iter(text), max_bytes=100)


def test_invalid_digest_limit_and_nonzero_offset_fail():
    source = io.BytesIO(b"abc")
    with pytest.raises(ProvenanceError):
        read_verified_snapshot(source, "not-a-digest", max_bytes=4)
    with pytest.raises(ProvenanceError, match="limit"):
        read_verified_snapshot(source, digest(b"abc"), max_bytes=2)
    source.seek(1)
    with pytest.raises(ProvenanceError):
        read_verified_snapshot(source, digest(b"abc"), max_bytes=4)


def test_csv_source_read_error_cannot_return_proof():
    class Broken(io.BytesIO):
        def read(self, size=-1):
            raise OSError("synthetic I/O failure")

    with pytest.raises(ProvenanceError, match="read failed"):
        parse_verified_csv(Broken(b"a"), digest(b"a"), lambda text: text.read(), max_bytes=5)


def test_large_csv_is_hashed_in_bounded_reads():
    class Tracked(io.BytesIO):
        largest_request = 0

        def read(self, size=-1):
            self.largest_request = max(self.largest_request, size)
            return super().read(size)

    content = (b"synthetic,0\n" * 100_000)
    source = Tracked(content)
    rows, proof = parse_verified_csv(source, digest(content),
                                     lambda text: sum(1 for _ in csv.reader(text)),
                                     max_bytes=len(content))
    assert rows == 100_000 and proof.bytes_read == len(content)
    assert 0 < source.largest_request <= 65_536


def test_mutation_during_streaming_changes_the_digest():
    class Mutating(io.BytesIO):
        changed = False

        def read(self, size=-1):
            result = super().read(size)
            if not self.changed and result and self.tell() < len(self.getvalue()):
                position = self.tell()
                self.changed = True
                super().write(b"z")
                super().seek(position)
            return result

    content = b"a" * 30_000
    with pytest.raises(ProvenanceError, match="mismatch"):
        parse_verified_csv(Mutating(content), digest(content), lambda text: text.read(),
                           max_bytes=len(content))


def test_late_read_error_never_returns_proof():
    class LateFailure(io.BytesIO):
        def read(self, size=-1):
            if self.tell() >= 8_192:
                raise OSError("synthetic late failure")
            return super().read(size)

    content = b"a" * 30_000
    with pytest.raises(ProvenanceError, match="read failed"):
        parse_verified_csv(LateFailure(content), digest(content), lambda text: text.read(),
                           max_bytes=len(content))


def test_csv_parser_error_cannot_return_proof():
    content = b'"unterminated\n'
    with pytest.raises(csv.Error):
        parse_verified_csv(io.BytesIO(content), digest(content),
                           lambda text: list(csv.reader(text, strict=True)), max_bytes=100)


def test_replaced_path_does_not_change_bytes_read_from_open_descriptor(tmp_path):
    original, replacement = b"old\n", b"new\n"
    path, other = tmp_path / "a.csv", tmp_path / "b.csv"
    path.write_bytes(original)
    other.write_bytes(replacement)
    with path.open("rb") as handle:
        os.replace(other, path)
        value, proof = parse_verified_csv(handle, digest(original), lambda text: text.read(), max_bytes=20)
    assert value == "old\n" and proof.sha256 == digest(original)
    assert path.read_bytes() == replacement


def test_xlsx_snapshot_is_immutable_and_is_the_data_parsed():
    workbook = Workbook()
    workbook.active.title = "Hoja1"
    workbook.active.append(["timestamp", "synthetic"])
    workbook.active.append(["2020-01-01 00:00:00", 1])
    target = io.BytesIO()
    workbook.save(target)
    workbook.close()
    raw = target.getvalue()
    with pytest.raises(ProvenanceError, match="mismatch"):
        read_verified_snapshot(io.BytesIO(raw), "0" * 64, max_bytes=len(raw))
    source = io.BytesIO(raw)
    snapshot, identity = read_verified_snapshot(source, digest(raw), max_bytes=len(raw))
    source.seek(0)
    source.write(b"tampered")
    parsed = read_xlsx_snapshot(snapshot, ["synthetic"], expected_rows=1)
    assert parsed.records[0]["timestamp"] == "2020-01-01 00:00:00"
    assert identity.sha256 == digest(raw) and snapshot == raw
