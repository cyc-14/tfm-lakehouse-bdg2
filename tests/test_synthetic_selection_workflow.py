"""End-to-end synthetic provenance and selection checks."""
from datetime import datetime, timedelta
from dataclasses import replace
from decimal import Decimal
import hashlib
import io
import json
import warnings
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from openpyxl import Workbook

from tfm_lakehouse.bdg2_comparison import APPROVED_BUILDINGS
from tfm_lakehouse.comparison_csv import CsvAdapterError
from tfm_lakehouse.comparison_xlsx import XlsxInput, _serial_timestamp
from tfm_lakehouse.controlled_synthetic_verification import SyntheticSpec, assess_synthetic
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
    for index, label in enumerate(labels):
        sheet.append([start if index == 0 else label, *([0] * 16)])
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
                     ({"timestamp": {"raw": token, **info}},), "1900",
                     ("timestamp", *APPROVED_BUILDINGS), "Hoja1")
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
                        lambda snapshot, ids: XlsxInput((), (), "1900",
                                                       ("timestamp", *APPROVED_BUILDINGS), "Hoja1"))
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


def controlled_spec(sources):
    return SyntheticSpec(
        source_labels=("metadata_fixture", "electricity_fixture", "xlsx_fixture"),
        expected_sha256=tuple(digest(source) for source in sources),
        max_bytes=tuple(len(source) + 1 for source in sources),
        dataset_variant="synthetic", dataset_version="fixture-v1", worksheet="Hoja1",
        building_ids=APPROVED_BUILDINGS, period_start="2016-01-01 00:00:00",
        period_end="2017-12-31 23:00:00",
    )


def assess(sources, spec=None):
    return assess_synthetic(spec or controlled_spec(sources), *(io.BytesIO(item) for item in sources))


def rewrite_xlsx(content, old, new):
    target = io.BytesIO()
    with ZipFile(io.BytesIO(content)) as source, ZipFile(target, "w", ZIP_DEFLATED) as output:
        for item in source.infolist():
            data = source.read(item)
            if item.filename == "xl/worksheets/sheet1.xml":
                assert old in data
                data = data.replace(old, new, 1)
            output.writestr(item.filename, data)
    return target.getvalue()


def test_controlled_synthetic_full_run_has_explicit_stages_and_no_output(synthetic_sources, capsys):
    result = assess(synthetic_sources)
    assert result["stages"]["configuration"] == "passed"
    for stage in ("metadata_bytes_schema", "selection_filters", "electricity_grid",
                  "selection_counts", "selection_reconciliation", "xlsx_integrity",
                  "xlsx_structure", "comparison", "comparison_assessment"):
        assert result["stages"][stage] == "passed"
    assert result["stages"]["source_provenance"] == "unresolved"
    assert result["stages"]["parser_warnings"] == "passed"
    assert result["stages"]["real_authorization"] == "not_granted"
    assert result["stages"]["evidence_output"] == "disabled"
    report = result["assessment"]
    assert report["timestamp_serialization_identity"] == "not_established"
    assert report["comparison"]["compared_positions"] == 280_704
    assert report["nominal_and_value_agreement"]
    assert report["synthetic_records_eligible"]
    assert not report["complete_verification_eligible"] and not result["verified"]
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("field,value,code", [
    ("expected_sha256", ("", "0" * 64, "0" * 64), "three_expected_digests_required"),
    ("dataset_variant", "raw", "synthetic_variant_and_version_required"),
    ("worksheet", "other", "profile_identity_or_period_mismatch"),
    ("building_ids", tuple(reversed(APPROVED_BUILDINGS)), "profile_identity_or_period_mismatch"),
    ("period_start", "2016-01-01 01:00:00", "profile_identity_or_period_mismatch"),
    ("evidence_destination", "/unauthorized/report", "evidence_output_not_authorized"),
])
def test_controlled_contract_rejects_invalid_config_before_reading(field, value, code):
    sources = (b"a", b"b", b"c")
    spec = replace(controlled_spec(sources), **{field: value})
    streams = tuple(io.BytesIO(source) for source in sources)
    report = assess_synthetic(spec, *streams)
    assert report["stages"]["configuration"] == "failed"
    assert report["blocking_reasons"] == [{"stage": "configuration", "code": code}]
    assert all(stream.tell() == 0 for stream in streams)
    assert not report["verified"] and report["assessment"] is None


def test_controlled_contract_blocks_real_config_and_file_streams_before_reading(tmp_path, monkeypatch):
    sources = (b"a", b"b", b"c")
    spec = controlled_spec(sources)
    monkeypatch.setenv("TFM_REAL_CONFIG", "synthetic-placeholder")
    streams = tuple(io.BytesIO(source) for source in sources)
    report = assess_synthetic(spec, *streams)
    assert report["blocking_reasons"][0]["code"] == "real_configuration_present"
    assert all(stream.tell() == 0 for stream in streams)
    monkeypatch.delenv("TFM_REAL_CONFIG")
    path = tmp_path / "fixture.csv"
    path.write_bytes(b"a")
    with path.open("rb") as source:
        report = assess_synthetic(spec, source, io.BytesIO(b"b"), io.BytesIO(b"c"))
        assert source.tell() == 0
    assert report["blocking_reasons"][0]["code"] == "synthetic_memory_streams_required"


def test_no_evidence_destination_means_no_persistence_or_implicit_authorization():
    sources = (b"bad", b"bad", b"bad")
    report = assess(sources)
    assert report["stages"]["configuration"] == "passed"
    assert report["stages"]["metadata_bytes_schema"] == "failed"
    assert report["stages"]["evidence_output"] == "disabled"
    assert report["stages"]["real_authorization"] == "not_granted"


def test_controlled_digest_mismatch_is_a_sanitized_partial_failure(synthetic_sources):
    spec = controlled_spec(synthetic_sources)
    bad = replace(spec, expected_sha256=("0" * 64, *spec.expected_sha256[1:]))
    report = assess(synthetic_sources, bad)
    assert report["stages"]["metadata_bytes_schema"] == "failed"
    assert report["stages"]["comparison"] == "not_run"
    assert report["blocking_reasons"] == [{"stage": "metadata_bytes_schema", "code": "ProvenanceError"}]
    assert report["assessment"] is None and not report["verified"]


def test_controlled_xlsx_digest_failure_preserves_checked_stages(synthetic_sources):
    spec = controlled_spec(synthetic_sources)
    bad = replace(spec, expected_sha256=(*spec.expected_sha256[:2], "0" * 64))
    report = assess(synthetic_sources, bad)
    assert report["stages"]["metadata_integrity"] == "passed"
    assert report["stages"]["electricity_integrity"] == "passed"
    assert report["stages"]["xlsx_integrity"] == "failed"
    assert report["stages"]["xlsx_structure"] == "not_run"
    assert "metadata" in report["checked_digests"]
    assert "electricity_count_pass" in report["checked_digests"]
    assert "xlsx" not in report["checked_digests"]
    assert report["assessment"] is None and not report["verified"]


@pytest.mark.parametrize("change", ["missing", "duplicate", "unexpected"])
def test_controlled_grid_failure_has_counts_and_stops_before_comparison(synthetic_sources, change):
    metadata, electricity, xlsx = synthetic_sources
    lines = electricity.splitlines(keepends=True)
    if change == "missing":
        lines.pop()
    elif change == "duplicate":
        lines[2] = lines[1]
    else:
        lines[-1] = lines[-1].replace(b"2017-12-31 23:00:00", b"2018-01-01 00:00:00")
    report = assess((metadata, b"".join(lines), xlsx))
    assert report["stages"]["electricity_grid"] == "failed"
    assert report["stages"]["selection_counts"] == report["stages"]["comparison"] == "not_run"
    assert report["grid_failure"]["missing_hours"] >= 1
    assert not report["verified"]


def test_controlled_selection_failure_is_explicit(synthetic_sources):
    metadata, electricity, xlsx = synthetic_sources
    lines = metadata.splitlines(keepends=True)
    report = assess((lines[0] + b"".join(reversed(lines[1:])), electricity, xlsx))
    assert report["stages"]["selection_reconciliation"] == "failed"
    assert report["stages"]["comparison"] == "not_run"
    assert report["assessment"]["comparison"] is None
    assert not report["assessment"]["complete_verification_eligible"]


def test_xlsx_structure_checked_even_when_selection_failed(synthetic_sources, monkeypatch):
    metadata, electricity, _ = synthetic_sources
    lines = metadata.splitlines(keepends=True)
    reordered = lines[0] + b"".join(reversed(lines[1:]))
    fake = XlsxInput((), (), "1900", ("timestamp", *reversed(APPROVED_BUILDINGS)), "Hoja1")
    monkeypatch.setattr("tfm_lakehouse.synthetic_selection_workflow.read_xlsx_snapshot",
                        lambda snapshot, ids: fake)
    report = assess((reordered, electricity, b"fixture"))
    assert report["stages"]["selection_reconciliation"] == "failed"
    assert report["stages"]["xlsx_integrity"] == "passed"
    assert report["stages"]["xlsx_structure"] == "failed"
    assert report["blocking_reasons"] == [
        {"stage": "selection_reconciliation", "code": "selection_not_eligible"},
        {"stage": "xlsx_structure", "code": "ComparisonError"},
    ]
    assert report["stages"]["comparison"] == "not_run" and not report["verified"]


def test_controlled_value_mismatch_never_exposes_measurements(synthetic_sources):
    metadata, electricity, xlsx = synthetic_sources
    marker = b"0.123456789123456789"
    changed = electricity.replace(b",0,0,0", b"," + marker + b",0,0", 1)
    report = assess((metadata, changed, xlsx))
    assert report["assessment"]["comparison"]["electricity_value_equivalence"] is False
    assert report["stages"]["comparison_assessment"] == "failed"
    assert not report["assessment"]["nominal_and_value_agreement"]
    assert marker.decode() not in json.dumps(report)
    assert not report["verified"]


def test_controlled_reader_warning_never_exposes_cell_value(synthetic_sources, capsys):
    metadata, electricity, xlsx = synthetic_sources
    marker = b"999999999999999999999"
    changed = rewrite_xlsx(xlsx, b'<c r="B2" t="n"><v>0</v></c>',
                           b'<c r="B2" s="1" t="n"><v>' + marker + b'</v></c>')
    with warnings.catch_warnings(record=True) as outside:
        warnings.simplefilter("always")
        report = assess((metadata, electricity, changed))
    assert not outside
    assert report["parser_warning_count"] >= 1
    assert report["stages"]["parser_warnings"] == "unresolved"
    assert marker.decode() not in json.dumps(report)
    assert not report["verified"]
    assert capsys.readouterr() == ("", "")


def test_controlled_formula_is_ambiguous_and_not_certified(synthetic_sources):
    metadata, electricity, xlsx = synthetic_sources
    formula = rewrite_xlsx(xlsx, b'<c r="B2" t="n"><v>0</v></c>',
                           b'<c r="B2"><f>1+1</f><v>2</v></c>')
    report = assess((metadata, electricity, formula))
    comparison = report["assessment"]["comparison"]
    assert comparison["electricity_value_equivalence"] is None
    assert report["assessment"]["nominal_and_value_agreement"] is None
    assert report["stages"]["comparison_assessment"] == "unresolved"
    assert comparison["outcomes"]["representation_ambiguity"] == 1
    assert not report["verified"]


def test_controlled_adjusted_timestamp_is_nominally_consistent_but_not_verified(synthetic_sources):
    metadata, electricity, xlsx = synthetic_sources
    adjusted = rewrite_xlsx(xlsx, b'<c r="A2" s="1" t="n"><v>42370</v></c>',
                            b'<c r="A2" s="1" t="n"><v>42370.00000000005</v></c>')
    report = assess((metadata, electricity, adjusted))
    assert report["assessment"]["nominal_and_value_agreement"]
    assert report["stages"]["comparison_assessment"] == "unresolved"
    assert report["assessment"]["comparison"]["temporal_adjustments"]["adjusted_rows"] == 1
    assert not report["assessment"]["synthetic_records_eligible"]
    assert not report["assessment"]["complete_verification_eligible"] and not report["verified"]


@pytest.fixture
def real_entry_fixture(tmp_path, synthetic_sources, monkeypatch):
    from tfm_lakehouse.controlled_real_verification import (
        ExecutionAuthorization, RealVerificationSpec, _OPERATIONS,
    )

    raw = tmp_path / "raw"
    raw.mkdir()
    paths = (raw / "metadata.csv", raw / "electricity.csv", tmp_path / "workbook.xlsx")
    for path, content in zip(paths, synthetic_sources):
        path.write_bytes(content)
    config = tmp_path / "config.toml"
    config.write_text(
        '[paths]\nraw_root = "raw"\nlakehouse_root = "lakehouse"\n'
        '[dataset]\nkind = "real"\nreference = "synthetic test"\n'
        '[sources]\nmetadata = "metadata.csv"\nelectricity = "electricity.csv"\n'
    )
    spec = RealVerificationSpec(
        config, *paths, tuple(digest(item) for item in synthetic_sources),
        tuple(len(item) + 100 for item in synthetic_sources), "BDG2 v1.0", "raw", "Hoja1",
        APPROVED_BUILDINGS, "2016-01-01 00:00:00", "2017-12-31 23:00:00",
    )
    authorization = ExecutionAuthorization(spec, "synthetic test approval", _OPERATIONS)
    monkeypatch.setenv("TFM_REAL_CONFIG", str(config))
    return spec, authorization


def test_real_entry_full_synthetic_files(real_entry_fixture, capsys):
    from tfm_lakehouse.controlled_real_verification import verify_real_sources

    spec, authorization = real_entry_fixture
    before = tuple(path.read_bytes() for path in
                   (spec.metadata_path, spec.electricity_path, spec.xlsx_path))
    report = verify_real_sources(spec, authorization)
    assert report["failure"] is None
    assert report["source_integrity"] == "matched_expected_sha256"
    assert report["path_stability"] == "matched_at_checks_only"
    assert report["authorization"] == "caller_attested_not_independently_verified"
    assert report["evidence_output"] == "disabled"
    assert report["assessment"]["selection_reproduced"]
    assert report["assessment"]["comparison"]["compared_positions"] == 280_704
    assert report["assessment"]["comparison"]["electricity_value_equivalence"] is True
    assert not report["verified"] and not report["assessment"]["complete_verification_eligible"]
    assert before == tuple(path.read_bytes() for path in
                           (spec.metadata_path, spec.electricity_path, spec.xlsx_path))
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("change,expected", [
    ("authorization", "specific_authorization_required"),
    ("different_scope", "specific_authorization_required"),
    ("config_missing", "real_config_required"),
    ("config_invalid", "invalid_real_config"),
    ("input_missing", "absolute_paths_required"),
    ("digest_missing", "three_expected_digests_required"),
    ("variant", "dataset_variant_mismatch"),
    ("output", "evidence_output_disabled"),
])
def test_real_entry_rejects_bad_scope_before_opening_sources(real_entry_fixture, monkeypatch,
                                                            change, expected):
    import tfm_lakehouse.controlled_real_verification as entry

    spec, authorization = real_entry_fixture
    if change == "authorization":
        authorization = None
    elif change == "different_scope":
        spec = replace(spec, electricity_variant="cleaned")
    elif change == "config_missing":
        monkeypatch.delenv("TFM_REAL_CONFIG")
    elif change == "config_invalid":
        spec.config_path.write_text("broken = [")
    elif change == "input_missing":
        spec = replace(spec, metadata_path=None)
    elif change == "digest_missing":
        spec = replace(spec, expected_sha256=("", *spec.expected_sha256[1:]))
    elif change == "variant":
        spec = replace(spec, electricity_variant="cleaned")
    elif change == "output":
        spec = replace(spec, evidence_destination="report.json")
    if authorization is not None and change not in {"different_scope", "authorization"}:
        authorization = replace(authorization, scope=spec)
    original_open = entry._open_source
    monkeypatch.setattr(entry, "_open_source", lambda path: original_open(path)
                        if path == spec.config_path else pytest.fail("opened source"))
    result = entry.verify_real_sources(spec, authorization)
    assert result["failure"] == expected
    assert not result["verified"]


def test_real_entry_wrong_hash_and_read_error(real_entry_fixture):
    from tfm_lakehouse.controlled_real_verification import verify_real_sources

    spec, auth = real_entry_fixture
    spec = replace(spec, expected_sha256=("0" * 64, *spec.expected_sha256[1:]))
    report = verify_real_sources(spec, replace(auth, scope=spec))
    assert report["failure"] == "ProvenanceError"
    assert report["stages"]["metadata_bytes_schema"] == "failed"
    spec.metadata_path.unlink()
    report = verify_real_sources(spec, replace(auth, scope=spec))
    assert report["failure"] == "invalid_real_config"


@pytest.mark.parametrize("change,stage", [
    ("metadata", "metadata_bytes_schema"),
    ("selection", "selection_reconciliation"),
    ("grid", "electricity_grid"),
    ("sheet", "xlsx_structure"),
    ("header", "xlsx_structure"),
    ("formula", "comparison_assessment"),
    ("adjustment", "comparison_assessment"),
    ("mismatch", "comparison_assessment"),
])
def test_real_entry_synthetic_failure_modes(real_entry_fixture, change, stage):
    from tfm_lakehouse.controlled_real_verification import verify_real_sources

    spec, auth = real_entry_fixture
    paths = (spec.metadata_path, spec.electricity_path, spec.xlsx_path)
    index = 0 if change in {"metadata", "selection"} else 1 if change in {"grid", "mismatch"} else 2
    data = paths[index].read_bytes()
    marker = b"0.123456789123456789"
    if change == "metadata":
        data = b"bad_header\ninvalid\n"
    elif change == "selection":
        lines = data.splitlines(keepends=True)
        data = lines[0] + b"".join(reversed(lines[1:]))
    elif change == "grid":
        data = data.rsplit(b"\n", 2)[0] + b"\n"
    elif change == "sheet":
        data = rewrite_xlsx(data, b'<c r="B2" t="n"><v>0</v></c>', b'<c r="B2"><f>2</f></c>')
        # Change the workbook sheet name without opening a real workbook.
        out = io.BytesIO()
        with ZipFile(io.BytesIO(data)) as src, ZipFile(out, "w", ZIP_DEFLATED) as dst:
            for item in src.infolist():
                content = src.read(item)
                if item.filename == "xl/workbook.xml":
                    content = content.replace(b'Hoja1', b'Other')
                dst.writestr(item.filename, content)
        data = out.getvalue()
    elif change == "header":
        data = rewrite_xlsx(data, b'Robin_lodging_Dorthy', b'Robin_lodging_Fake')
    elif change == "formula":
        data = rewrite_xlsx(data, b'<c r="B2" t="n"><v>0</v></c>',
                            b'<c r="B2"><f>1+1</f><v>2</v></c>')
    elif change == "adjustment":
        data = rewrite_xlsx(data, b'<c r="A2" s="1" t="n"><v>42370</v></c>',
                            b'<c r="A2" s="1" t="n"><v>42370.00000000005</v></c>')
    elif change == "mismatch":
        data = data.replace(b",0,0,0", b"," + marker + b",0,0", 1)
    paths[index].write_bytes(data)
    digests = list(spec.expected_sha256)
    digests[index] = digest(data)
    limits = list(spec.max_bytes)
    limits[index] = len(data) + 100
    spec = replace(spec, expected_sha256=tuple(digests), max_bytes=tuple(limits))
    report = verify_real_sources(spec, replace(auth, scope=spec))
    assert report["stages"][stage] in {"failed", "unresolved"}
    assert marker.decode() not in json.dumps(report)
    assert not report["verified"]


def test_real_entry_rejects_config_source_hardlink_and_symlink(real_entry_fixture, monkeypatch):
    import os
    import tfm_lakehouse.controlled_real_verification as entry

    spec, auth = real_entry_fixture
    spec.xlsx_path.unlink()
    os.link(spec.config_path, spec.xlsx_path)
    assert entry.verify_real_sources(spec, auth)["failure"] == "config_source_alias"
    spec.xlsx_path.unlink()
    spec.xlsx_path.symlink_to(spec.config_path)
    assert entry.verify_real_sources(spec, auth)["failure"] == "source_symlink"


def test_real_entry_detects_source_substitution(real_entry_fixture, monkeypatch):
    import tfm_lakehouse.controlled_real_verification as entry

    spec, auth = real_entry_fixture
    original = entry._run_verified_workflow

    def replace_after_read(*args, **kwargs):
        result = original(*args, **kwargs)
        replacement = spec.xlsx_path.with_name("replacement.xlsx")
        replacement.write_bytes(b"different synthetic content")
        replacement.replace(spec.xlsx_path)
        return result

    monkeypatch.setattr(entry, "_run_verified_workflow", replace_after_read)
    report = entry.verify_real_sources(spec, auth)
    assert report["failure"] == "source_changed"
    assert report["assessment"] is None and not report["verified"]


def test_real_entry_read_error_is_sanitized(real_entry_fixture, monkeypatch):
    import tfm_lakehouse.source_provenance as provenance
    from tfm_lakehouse.controlled_real_verification import verify_real_sources

    spec, auth = real_entry_fixture
    def failed_read(self, buffer):
        raise OSError("sensitive measurement 123456789")
    monkeypatch.setattr(provenance._HashingReader, "readinto", failed_read)
    report = verify_real_sources(spec, auth)
    assert report["failure"] in {"OSError", "ProvenanceError"}
    assert "123456789" not in json.dumps(report)
    assert not report["verified"]


@pytest.mark.parametrize("extension", [
    '\nweather = "metadata.csv"\n',
    '\n[spark]\nivy_dir = "raw/ivy"\n',
])
def test_real_entry_enforces_full_project_config(real_entry_fixture, extension):
    from tfm_lakehouse.controlled_real_verification import verify_real_sources

    spec, auth = real_entry_fixture
    with spec.config_path.open("a") as config:
        config.write(extension)
    report = verify_real_sources(spec, auth)
    assert report["failure"] == "invalid_real_config"
    assert all(value == "not_run" for value in report["stages"].values())
