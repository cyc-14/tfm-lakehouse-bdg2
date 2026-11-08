"""End-to-end synthetic provenance and selection checks."""
from datetime import datetime, timedelta
from decimal import Decimal
import hashlib
import io

import pytest
from openpyxl import Workbook

from tfm_lakehouse.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.comparison_csv import CsvAdapterError
from tfm_lakehouse.comparison_xlsx import XlsxInput, _serial_timestamp
from tfm_lakehouse.excel_bdg2_comparison import Cell
from tfm_lakehouse.source_provenance import ProvenanceError
from tfm_lakehouse.synthetic_selection_workflow import SelectionGridError, run_synthetic_workflow


def digest(content):
    return hashlib.sha256(content).hexdigest()


@pytest.fixture(scope="module")
def synthetic_sources():
    header = "building_id,primaryspaceusage,sub_primaryspaceusage,sqm\n"
    metadata = (header + "".join(
        f"{bid},Lodging/residential,Dormitory,4000\n" for bid in APPROVED_BUILDINGS
    )).encode()
    start = datetime(2016, 1, 1)
    labels = [(start + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S")
              for i in range(17_544)]
    electric = (("timestamp," + ",".join(APPROVED_BUILDINGS) + "\n") + "".join(
        label + "," + ",".join(["0"] * 16) + "\n" for label in labels
    )).encode()
    workbook = Workbook(write_only=True)
    sheet = workbook.create_sheet("Hoja1")
    sheet.append(["timestamp", *APPROVED_BUILDINGS])
    for label in labels:
        sheet.append([label, *([0] * 16)])
    target = io.BytesIO()
    workbook.save(target)
    workbook.close()
    return metadata, electric, target.getvalue()


def run(metadata, electricity, xlsx, *, electricity_stream=None, metadata_hash=None):
    return run_synthetic_workflow(
        io.BytesIO(metadata), electricity_stream or io.BytesIO(electricity), io.BytesIO(xlsx),
        metadata_sha256=metadata_hash or digest(metadata), electricity_sha256=digest(electricity),
        xlsx_sha256=digest(xlsx), max_metadata_bytes=len(metadata) + 1,
        max_electricity_bytes=len(electricity) + 1, max_xlsx_bytes=len(xlsx) + 1,
    )


def test_full_synthetic_sources_are_bound_and_reproduce_approved_selection(synthetic_sources):
    metadata, electricity, xlsx = synthetic_sources
    result = run(metadata, electricity, xlsx)
    assert result["source_byte_identity"]["metadata"].sha256 == digest(metadata)
    assert result["source_byte_identity"]["electricity"].sha256 == digest(electricity)
    assert result["source_byte_identity"]["xlsx"].sha256 == digest(xlsx)
    assert result["source_identity_status"] == {"all_supplied_sha256_match": True,
                                                "source_origin_verified": False,
                                                "path_stability_verified": False}
    selection = result["selection_reproduction"]
    assert selection["approved_selection"]["same_order"]
    assert selection["selection_reproduction_eligible"]
    assert all(item["missing"] == 0 and item["zero"] == 17_544
               for item in selection["building_counts"])
    assert result["funnel"]["observed"] == [16, 16, 16, 16]
    assert not result["funnel"]["matches_historical_reference"]
    assert result["comparison"]["electricity_value_equivalence"] is True
    assert result["comparison"]["comparison"]["compared_positions"] == 280_704
    assert result["synthetic_records_eligible"] and not result["verified"]


def test_wrong_hash_aborts_before_processing_other_sources(synthetic_sources):
    metadata, electricity, xlsx = synthetic_sources
    with pytest.raises(ProvenanceError, match="mismatch"):
        run(metadata, electricity, xlsx, metadata_hash="0" * 64)


def test_grid_failure_is_reported_before_missingness_or_xlsx(synthetic_sources):
    metadata, electricity, xlsx = synthetic_sources
    truncated = electricity.rsplit(b"\n", 2)[0] + b"\n"
    with pytest.raises(SelectionGridError) as caught:
        run(metadata, truncated, xlsx)
    assert caught.value.report["missing_hours"] == 1


def test_mutation_between_grid_and_count_passes_fails_hash(synthetic_sources):
    metadata, electricity, xlsx = synthetic_sources

    class MutatingStream(io.BytesIO):
        changed = False

        def seek(self, offset, whence=0):
            if offset == 0 and whence == 0 and not self.changed and self.tell() > 0:
                self.changed = True
                current = self.getvalue()
                at = current.index(b",0,0,0") + 1
                super().seek(at)
                super().write(b"1")
            return super().seek(offset, whence)

    with pytest.raises(ProvenanceError, match="mismatch"):
        run(metadata, electricity, xlsx, electricity_stream=MutatingStream(electricity))


def test_unknown_electricity_identifier_is_rejected(synthetic_sources):
    metadata, electricity, xlsx = synthetic_sources
    first, rest = electricity.split(b"\n", 1)
    unexpected = first + b",unknown_building\n" + rest
    with pytest.raises(CsvAdapterError, match="metadata"):
        run(metadata, unexpected, xlsx)


def test_selection_order_mismatch_prevents_comparison(synthetic_sources, monkeypatch):
    metadata, electricity, _ = synthetic_sources
    lines = metadata.splitlines(keepends=True)
    reversed_metadata = lines[0] + b"".join(reversed(lines[1:]))
    token = "42370.00000000005"
    decoded, info = _serial_timestamp(token, "1900")
    fake = XlsxInput(({"timestamp": decoded},),
                     ({"timestamp": {"raw": token, **info}},), "1900")
    monkeypatch.setattr("tfm_lakehouse.synthetic_selection_workflow.read_xlsx_snapshot",
                        lambda snapshot, ids: fake)
    result = run(reversed_metadata, electricity, b"synthetic")
    selection = result["selection_reproduction"]
    assert selection["approved_selection"]["same_set"]
    assert not selection["approved_selection"]["same_order"]
    assert result["comparison"] is None and not result["synthetic_records_eligible"]
    assert "selection_order_mismatch" in result["verification_limitations"]
    assert "temporal_adjustments_not_compared" in result["verification_limitations"]
    assert len(result["temporal_adjustment_trace"]) == 1
    assert not result["verified"]


def test_invalid_nonmissing_value_blocks_even_when_approved_selection_matches(
    synthetic_sources, monkeypatch,
):
    metadata, electricity, _ = synthetic_sources
    broken = electricity.replace(b",0,0,0", b",bad,0,0", 1)
    monkeypatch.setattr("tfm_lakehouse.synthetic_selection_workflow.read_xlsx_snapshot",
                        lambda snapshot, ids: XlsxInput((), (), "1900"))
    result = run(metadata, broken, b"synthetic")
    selection = result["selection_reproduction"]
    assert selection["approved_selection"]["same_order"]
    assert selection["invalid_cells"] == 1 and not selection["selection_reproduction_eligible"]
    assert "invalid_non_missing_cells" in result["verification_limitations"]
    assert result["comparison"] is None and not result["verified"]


def test_every_temporal_adjustment_is_retained_beyond_example_limit(synthetic_sources, monkeypatch):
    metadata, electricity, _ = synthetic_sources
    start = datetime(2016, 1, 1)
    labels = [(start + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S")
              for i in range(17_544)]
    records = tuple({"timestamp": label, **dict.fromkeys(APPROVED_BUILDINGS, Cell("text", "0"))}
                    for label in labels)
    observed = [{"timestamp": {"kind": "s", "raw_type": "inlineStr", "raw": None}}
                for _ in labels]
    for index in range(48):
        token = str(Decimal(42370) + Decimal(index) / Decimal(24))
        decoded, info = _serial_timestamp(token, "1900")
        assert decoded == labels[index]
        observed[index] = {"timestamp": {"kind": "d", "raw_type": "n", "raw": token, **info}}
    fake = XlsxInput(records, tuple(observed), "1900", ("timestamp", *APPROVED_BUILDINGS), "Hoja1")
    monkeypatch.setattr("tfm_lakehouse.synthetic_selection_workflow.read_xlsx_snapshot",
                        lambda snapshot, ids: fake)
    result = run(metadata, electricity, b"synthetic")
    trace = result["temporal_adjustment_trace"]
    assert len(trace) > 20
    assert len(result["comparison"]["temporal_adjustments"]["examples"]) == 20
    assert len(trace) == result["comparison"]["temporal_adjustments"]["adjusted_rows"]
    assert all(item["raw"] and item["error_seconds"] and item["bound_seconds"] for item in trace)
    assert result["comparison"]["nominal_temporal_agreement"]
    assert result["comparison"]["electricity_value_equivalence"] is True
    assert "comparison:temporal_adjustments" in result["verification_limitations"]
    assert not result["synthetic_records_eligible"] and not result["verified"]
