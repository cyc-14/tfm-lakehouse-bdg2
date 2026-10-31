"""Synthetic tests for H03.2; these fixtures are not energy-analysis evidence."""
from copy import deepcopy
from dataclasses import fields, replace
from datetime import datetime, timedelta
from decimal import Decimal
import csv
import hashlib
import json
import os
from pathlib import Path
import shutil

import pytest

from tfm_lakehouse import characterization as characterization

from tfm_lakehouse.characterization import (
    Candidate, CharacterizationError, Settings, analyze, load_settings,
    passes_threshold, sha256, validate_output, write_reports,
)

pytestmark = pytest.mark.unit


def make_fixture(tmp_path, values, timestamps=None, metadata=None):
    raw = tmp_path / "raw"
    raw.mkdir()
    metadata_path, electricity_path = raw / "metadata.csv", raw / "electricity.csv"
    metadata = metadata or [
        ["Site_lodging_A", "Lodging/residential", "Dormitory", "Site", "Europe/London", "4000"]
    ]
    fields = ["building_id", "primaryspaceusage", "sub_primaryspaceusage",
              "site_id", "timezone", "sqm"]
    with metadata_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(fields)
        writer.writerows(metadata)
    if timestamps is None:
        timestamps = [
            (datetime(2020, 1, 1) + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S")
            for i in range(len(values))
        ]
    with electricity_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", *(row[0] for row in metadata)])
        writer.writerows([time, *([value] * len(metadata))]
                         for time, value in zip(timestamps, values))
    config = tmp_path / "config.toml"
    config.write_text("# Synthetic test placeholder\n", encoding="utf-8")
    return Settings(
        config_path=config, raw_root=raw, metadata=metadata_path,
        electricity=electricity_path, output_root=tmp_path / "checks",
        lakehouse_root=tmp_path / "lakehouse", kind="synthetic",
        reference="synthetic software fixture", unit="synthetic-unit",
        unit_reference="synthetic test definition", expected_hashes={},
        candidates=(Candidate("CASE_A", metadata[0][0], "synthetic"),),
    )


def one(report):
    return report["buildings"][0]


def test_partitions_cells_and_keeps_zeros_negatives_and_extremes(tmp_path):
    settings = make_fixture(tmp_path, ["", "NaN", "nan", "Infinity", "bad", "0", "-2", "10", "1000"])
    result = one(analyze(settings))
    assert result["source_records"] == 9
    assert (result["missing"], result["nonnumeric"], result["nonfinite"],
            result["finite_readings"]) == (2, 1, 2, 4)
    assert result["missing_pct_received"] == pytest.approx(200 / 9)
    assert result["unusable_pct_received"] == pytest.approx(500 / 9)
    assert result["zero_readings"] == result["negatives"] == 1
    assert result["zero_pct_finite"] == result["negative_pct_finite"] == 25
    assert result["statistics"]["mean"] == 252
    assert result["statistics"]["median"] == 5
    assert result["statistics"]["p25"] == -.5
    assert result["statistics"]["p75"] == 257.5
    assert result["outliers"]["flagged_observations"] == 1
    assert result["outliers"]["examples"][0]["value"] == 1000


def test_population_standard_deviation_and_linear_quantiles(tmp_path):
    result = one(analyze(make_fixture(tmp_path, ["0", "2", "4", "6"])))
    assert result["statistics"]["population_stddev"] == pytest.approx(5 ** .5)
    assert result["statistics"]["p01"] == pytest.approx(.06)
    assert result["statistics"]["p99"] == pytest.approx(5.94)


@pytest.mark.parametrize("values", [[""], ["NaN", "NaN"], ["0"], ["7"]])
def test_empty_and_singleton_numeric_populations(tmp_path, values):
    result = one(analyze(make_fixture(tmp_path, values)))
    stats = result["statistics"]
    if values[0] in ("", "NaN"):
        assert stats["mean"] is None
        assert stats["population_stddev"] is None
        assert result["outliers"]["flagged_observations"] is None
    else:
        assert stats["population_stddev"] == 0
        assert result["outliers"]["degenerate_iqr"] is True


def test_zero_iqr_is_not_reported_as_no_extremes(tmp_path):
    result = one(analyze(make_fixture(tmp_path, ["0"] * 99 + ["100"])))
    assert result["outliers"]["degenerate_iqr"] is True
    assert result["outliers"]["flagged_observations"] is None
    assert result["outliers"]["percentile_tail_counts"]["above_p99"] == 1


def test_long_zero_runs_have_explicit_local_label_units(tmp_path):
    values = ["0"] * 24 + ["1"] + ["0"] * 72 + ["1"] + ["0"] * 168
    result = one(analyze(make_fixture(tmp_path, values)))
    zeros = result["zeros"]["continuous_local_hour_labels"]
    assert result["zeros"]["zero_observations"] == 264
    assert zeros["sequences"] == 3
    assert zeros["maximum_observations"] == 168
    assert zeros["sequences_at_least"] == {"24": 3, "72": 2, "168": 1}
    assert zeros["longest"][0]["label_span_hours"] == 167


def test_duplicate_gap_invalid_and_backward_labels_are_not_imputed(tmp_path):
    times = ["2020-01-01 00:00:00", "2020-01-01 01:00:00",
             "2020-01-01 01:00:00", "2020-01-01 03:00:00",
             "2020-01-01 04:00:00", "invalid",
             "2020-01-01 06:00:00", "2020-01-01 05:00:00"]
    report = analyze(make_fixture(tmp_path, ["0"] * 8, times))
    temporal, building = report["temporal"], one(report)
    assert temporal["source_records"] == 8
    assert temporal["duplicate_timestamp_rows"] == 1
    assert temporal["invalid_timestamp_rows"] == 1
    assert len(temporal["backward_transitions"]) == 1
    assert temporal["expected_hourly_labels_in_observed_span"] == 7
    assert temporal["expected_labels_absent_from_source"] == 1
    assert building["missing"] == 0
    assert building["zeros"]["consecutive_records"]["maximum_observations"] == 8
    assert building["zeros"]["continuous_local_hour_labels"]["maximum_observations"] == 2
    unassigned = [row for row in building["coverage"]
                  if row["scale"] == "month" and row["period"] == "unassigned"][0]
    assert unassigned["source_records"] == 1
    assert unassigned["expected_local_labels"] is None


def test_nonadjacent_duplicate_breaks_first_occurrence_too(tmp_path):
    times = ["2020-01-01 00:00:00", "2020-01-01 01:00:00",
             "2020-01-01 02:00:00", "2020-01-01 00:00:00"]
    zeros = one(analyze(make_fixture(tmp_path, ["0"] * 4, times)))["zeros"]
    assert zeros["consecutive_records"]["maximum_observations"] == 4
    assert zeros["continuous_local_hour_labels"]["maximum_observations"] == 2


def test_entire_missing_month_remains_in_expected_grid(tmp_path):
    times = ["2021-01-31 23:00:00", "2021-03-01 00:00:00"]
    report = analyze(make_fixture(tmp_path, ["1", "1"], times))
    february = [r for r in one(report)["coverage"]
                if r["scale"] == "month" and r["period"] == "2021-02"][0]
    assert february["source_records"] == 0
    assert february["missing_pct_received"] is None
    assert february["expected_local_labels"] == 672
    assert february["expected_labels_absent_from_source"] == 672
    assert february["finite_pct_expected_local_labels"] == 0


@pytest.mark.parametrize("times,max_slots", [
    (["2020-01-01 00:30:00"], 1000),
    (["2020-01-01 00:00:00", "2020-01-02 00:00:00"], 2),
    (["invalid"], 1000),
])
def test_unreliable_or_excessive_grid_is_explicitly_unavailable(tmp_path, times, max_slots):
    settings = make_fixture(tmp_path, ["1"] * len(times), times)
    result = analyze(replace(settings, max_grid_slots=max_slots))
    assert result["temporal"]["expected_hourly_labels_in_observed_span"] is None
    assert result["temporal"]["grid_limitation"]


@pytest.mark.parametrize("threshold", [5, 8, 10])
def test_exact_thresholds_are_inclusive_and_do_not_round(threshold):
    assert passes_threshold(threshold, 100, Decimal(threshold))
    assert not passes_threshold(threshold + 1, 100, Decimal(threshold))
    assert not passes_threshold(0, 0, Decimal(threshold))


def test_inclusive_area_funnel_preserves_exact_category_strings(tmp_path):
    rows = [
        [f"Site_lodging_{letter}", "Lodging/residential", sub, "Site", "Europe/London", sqm]
        for letter, sqm, sub in [
            ("A", "3000", "Residence Hall"), ("B", "7000", "Residence Hall\t"),
            ("C", "2999", "Dormitory"), ("D", "7001", "Dormitory"),
            ("E", "bad", "Dormitory"), ("F", "", "Dormitory"),
        ]
    ]
    report = analyze(make_fixture(tmp_path, ["1"], metadata=rows))
    assert report["global"]["target_primary_buildings"] == 6
    assert report["global"]["area_cohort_building_ids"] == ["Site_lodging_A", "Site_lodging_B"]
    assert report["global"]["invalid_area_ids"] == ["Site_lodging_E"]
    assert report["global"]["missing_area_ids"] == ["Site_lodging_F"]
    categories = {r["value"] for r in report["global"]["target_subcategories"]}
    assert "Residence Hall" in categories and "Residence Hall\t" in categories


@pytest.mark.parametrize("content", [
    "", "timestamp,Site_lodging_A\n",
    "timestamp,Site_lodging_A\n2020-01-01 00:00:00,1,extra\n",
    "timestamp,Site_lodging_A,SITE_LODGING_A\n2020-01-01 00:00:00,1,1\n",
    "other,Site_lodging_A\n2020-01-01 00:00:00,1\n",
    'timestamp,Site_lodging_A\n2020-01-01 00:00:00,"unterminated\n',
    "version https://git-lfs.github.com/spec/v1\n",
])
def test_malformed_sources_fail_before_reporting(tmp_path, content):
    settings = make_fixture(tmp_path, ["1"])
    settings.electricity.write_text(content)
    with pytest.raises(CharacterizationError):
        analyze(settings)
    assert not settings.output_root.exists()


def test_duplicate_metadata_id_is_rejected(tmp_path):
    settings = make_fixture(tmp_path, ["1"])
    settings.metadata.write_text(settings.metadata.read_text() + settings.metadata.read_text().splitlines()[1] + "\n")
    with pytest.raises(CharacterizationError, match="unique"):
        analyze(settings)


def test_expected_source_hash_mismatch_is_rejected(tmp_path):
    settings = make_fixture(tmp_path, ["1"])
    with pytest.raises(CharacterizationError, match="SHA-256"):
        analyze(replace(settings, expected_hashes={"electricity": "0" * 64}))


def test_report_is_deterministic_and_sources_are_unchanged(tmp_path):
    settings = make_fixture(tmp_path, ["0", "1", "", "-1"])
    before = (sha256(settings.metadata), sha256(settings.electricity))
    first, second = analyze(settings), analyze(settings)
    assert first == second
    output = settings.output_root / "synthetic" / "run"
    write_reports(settings, output, first, argv=["synthetic-test-command"])
    payload = json.loads((output / "report.json").read_text())
    assert payload["metrics"] == first
    assert (output / "SUCCESS.json").is_file()
    assert (output / "building_quality.csv").is_file()
    assert (sha256(settings.metadata), sha256(settings.electricity)) == before
    with pytest.raises(CharacterizationError, match="already exists"):
        write_reports(settings, output, first)


def test_source_change_before_publication_leaves_no_success(tmp_path):
    settings = make_fixture(tmp_path, ["1"])
    report = analyze(settings)
    settings.electricity.write_text(settings.electricity.read_text().replace(",1", ",2"))
    output = settings.output_root / "synthetic" / "changed"
    with pytest.raises(CharacterizationError, match="changed"):
        write_reports(settings, output, report)
    assert not output.exists()


def test_report_rejects_raw_lakehouse_and_wrong_namespace(tmp_path):
    settings = make_fixture(tmp_path, ["1"])
    for path in [settings.raw_root / "report", settings.lakehouse_root / "report",
                 settings.output_root / "real" / "run"]:
        with pytest.raises(CharacterizationError):
            validate_output(settings, path)


def test_output_symlink_alias_is_rejected(tmp_path):
    settings = make_fixture(tmp_path, ["1"])
    settings.output_root.mkdir()
    (settings.output_root / "synthetic").symlink_to(settings.raw_root, target_is_directory=True)
    with pytest.raises(CharacterizationError, match="aliases"):
        validate_output(settings, settings.output_root / "synthetic" / "run")


def test_load_settings_resolves_paths_from_configuration(tmp_path, monkeypatch):
    settings = make_fixture(tmp_path, ["1"])
    config = """[paths]
raw_root = "raw"
output_root = "checks"
lakehouse_root = "lakehouse"
[dataset]
kind = "synthetic"
reference = "synthetic"
unit = "synthetic-unit"
unit_reference = "synthetic-definition"
[sources]
metadata = "metadata.csv"
electricity = "electricity.csv"
[[candidates]]
case_id = "CASE_A"
building_id = "Site_lodging_A"
group = "synthetic"
"""
    settings.config_path.write_text(config)
    monkeypatch.chdir(tmp_path.parent)
    parsed = load_settings(settings.config_path)
    assert parsed.metadata == settings.metadata
    assert parsed.output_root == settings.output_root
    assert parsed.config_sha256 == sha256(settings.config_path)


def test_real_output_ignore_is_checked_from_destination_not_external_config(tmp_path):
    import subprocess
    settings = make_fixture(tmp_path, ["1"])
    repository = tmp_path / "different-repository"
    repository.mkdir()
    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    settings = replace(settings, kind="real", output_root=repository / "checks")
    output = settings.output_root / "real" / "run"
    with pytest.raises(CharacterizationError, match="ignored by Git"):
        validate_output(settings, output)
    (repository / ".gitignore").write_text("/checks/\n")
    assert validate_output(settings, output) == output


def test_invalid_utf8_body_is_rejected(tmp_path):
    settings = make_fixture(tmp_path, ["1"])
    with settings.electricity.open("ab") as handle:
        handle.write(b"2020-01-02 00:00:00,\xff\n")
    with pytest.raises(CharacterizationError, match="UTF-8"):
        analyze(settings)


def test_report_write_failure_preserves_partial_directory_without_success(tmp_path, monkeypatch):
    import tfm_lakehouse.characterization as module
    settings = make_fixture(tmp_path, ["0", "1"])
    report = analyze(settings)
    output = settings.output_root / "synthetic" / "failed-write"

    def fail_write(*args, **kwargs):
        raise OSError("synthetic disk failure")

    monkeypatch.setattr(module, "_write_csv", fail_write)
    with pytest.raises(OSError, match="synthetic disk failure"):
        write_reports(settings, output, report)
    assert (output / "report.json").exists()
    assert not (output / "SUCCESS.json").exists()
    with pytest.raises(CharacterizationError, match="already exists"):
        validate_output(settings, output)


def test_source_mutation_during_report_write_cannot_publish_success(tmp_path, monkeypatch):
    import tfm_lakehouse.characterization as module
    settings = make_fixture(tmp_path, ["0", "1"])
    report = analyze(settings)
    output = settings.output_root / "synthetic" / "changed-during-write"
    original = module._write_csv
    changed = False

    def mutate_source(path, rows):
        nonlocal changed
        original(path, rows)
        if not changed:
            settings.electricity.write_text(settings.electricity.read_text() + "\n")
            changed = True

    monkeypatch.setattr(module, "_write_csv", mutate_source)
    with pytest.raises(CharacterizationError, match="while writing"):
        write_reports(settings, output, report)
    assert output.exists()
    assert not (output / "SUCCESS.json").exists()


def binding_fixture(tmp_path):
    metadata = [[f"Site_lodging_{letter}", "Lodging/residential", "Dormitory",
                 "Site", "Europe/London", "4000"] for letter in ("A", "B")]
    settings = make_fixture(tmp_path, ["0", "1", "", "-2"], metadata=metadata)
    return replace(settings, candidates=(
        Candidate("CASE_A", "Site_lodging_A", "synthetic-first"),
        Candidate("CASE_B", "Site_lodging_B", "synthetic-second"),
    ))


def canonical_digest(value):
    content = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


def test_analysis_binding_covers_all_fields_and_hash_scopes(tmp_path):
    settings = replace(binding_fixture(tmp_path), min_sqm=Decimal("3000.00"),
                       max_sqm=Decimal("7E+3"), thresholds=(Decimal("8.0"), Decimal("5")))
    report = analyze(settings)
    provenance = report["analysis_provenance"]
    effective = provenance["effective_settings"]
    assert report["schema_version"] == "h03.2-v2"
    assert provenance["contract_version"] == "h03.2-analysis-binding-v1"
    assert set(effective) == {item.name for item in fields(Settings)}
    assert effective["min_sqm"] == "3000.00"
    assert effective["max_sqm"] == "7E+3"
    assert effective["thresholds"] == ["8.0", "5"]
    assert effective["candidates"] == [
        {"case_id": item.case_id, "building_id": item.building_id, "group": item.group}
        for item in settings.candidates
    ]
    assert provenance["effective_settings_sha256"] == canonical_digest(effective)
    assert provenance["configuration"]["sha256"] == sha256(settings.config_path)
    assert effective["config_sha256"] == ""
    assert provenance["analyzed_payload_sha256"] == canonical_digest({
        key: value for key, value in report.items() if key != "analysis_provenance"
    })
    output = settings.output_root / "synthetic" / "bound"
    write_reports(settings, output, report)
    execution = json.loads((output / "execution.json").read_text())
    success = json.loads((output / "SUCCESS.json").read_text())
    full_digest = hashlib.sha256(json.dumps(report, ensure_ascii=False,
                                            sort_keys=True, allow_nan=False).encode()).hexdigest()
    assert success["metrics_sha256"] == execution["deterministic_metrics_sha256"] == full_digest
    assert execution["effective_settings_sha256"] == provenance["effective_settings_sha256"]
    assert execution["config_sha256"] == provenance["configuration"]["sha256"]
    assert set(success) == {"metrics_sha256", "source_hashes"}
    assert not (output / ".SUCCESS.json.tmp").exists()


@pytest.mark.parametrize("field_name", [item.name for item in fields(Settings)])
def test_publication_rejects_every_changed_settings_field(tmp_path, field_name):
    settings = binding_fixture(tmp_path)
    report = analyze(settings)
    changes = {
        "config_path": tmp_path / "different.toml", "raw_root": tmp_path / "other-raw",
        "metadata": settings.raw_root / "other-metadata.csv",
        "electricity": settings.raw_root / "other-electricity.csv",
        "output_root": tmp_path / "other-checks", "lakehouse_root": tmp_path / "other-lake",
        "kind": "real", "reference": "different reference", "unit": "different unit",
        "unit_reference": "different definition", "expected_hashes": {"metadata": sha256(settings.metadata)},
        "candidates": tuple(reversed(settings.candidates)), "config_sha256": "0" * 64,
        "primary_usage": "different usage", "min_sqm": Decimal("3001"),
        "max_sqm": Decimal("6999"), "thresholds": (Decimal("99"),),
        "missing_tokens": ("",), "timestamp_format": "%Y-%m-%d",
        "iqr_multiplier": 2.0, "max_grid_slots": 500000,
    }
    different = replace(settings, **{field_name: changes[field_name]})
    output = different.output_root / different.kind / "mixed"
    with pytest.raises(CharacterizationError):
        write_reports(different, output, report)
    assert not output.exists()


@pytest.mark.parametrize("change", [
    {"iqr_multiplier": 1.0}, {"max_grid_slots": 1000.0},
    {"thresholds": (Decimal("5.0"),)},
])
def test_binding_preserves_numeric_representations(tmp_path, change):
    settings = replace(make_fixture(tmp_path, ["0", "1"]), iqr_multiplier=1,
                       max_grid_slots=1000, thresholds=(Decimal("5"),))
    report = analyze(settings)
    output = settings.output_root / "synthetic" / "changed-representation"
    with pytest.raises(CharacterizationError, match="settings"):
        write_reports(replace(settings, **change), output, report)
    assert not output.exists()


def test_expected_hashes_snapshot_is_independent(tmp_path):
    settings = make_fixture(tmp_path, ["1"])
    report = analyze(settings)
    settings.expected_hashes["metadata"] = sha256(settings.metadata)
    assert report["analysis_provenance"]["effective_settings"]["expected_hashes"] == {}
    output = settings.output_root / "synthetic" / "changed-dictionary"
    with pytest.raises(CharacterizationError, match="settings"):
        write_reports(settings, output, report)
    assert not output.exists()


@pytest.mark.parametrize("field_name,value", [
    ("case_id", "OTHER_CASE"), ("building_id", "Site_lodging_B"), ("group", "other-group"),
])
def test_candidate_identity_and_group_are_bound(tmp_path, field_name, value):
    settings = make_fixture(tmp_path, ["1"])
    report = analyze(settings)
    candidate = replace(settings.candidates[0], **{field_name: value})
    output = settings.output_root / "synthetic" / "different-candidate"
    with pytest.raises(CharacterizationError, match="settings"):
        write_reports(replace(settings, candidates=(candidate,)), output, report)
    assert not output.exists()


def test_loaded_analysis_a_cannot_publish_with_reloaded_configuration_b(tmp_path):
    settings = make_fixture(tmp_path, ["0", "1"])
    content = '''[paths]
raw_root = "raw"
output_root = "checks"
lakehouse_root = "lakehouse"
[dataset]
kind = "synthetic"
reference = "synthetic configuration fixture"
unit = "synthetic-unit"
unit_reference = "synthetic-definition"
[sources]
metadata = "metadata.csv"
electricity = "electricity.csv"
[analysis]
thresholds = [5]
[[candidates]]
case_id = "CASE_A"
building_id = "Site_lodging_A"
group = "synthetic"
'''
    settings.config_path.write_text(content)
    first = load_settings(settings.config_path)
    report = analyze(first)
    settings.config_path.write_text(content.replace("thresholds = [5]", "thresholds = [99]"))
    second = load_settings(settings.config_path)
    assert second.config_sha256 == sha256(second.config_path)
    output = second.output_root / "synthetic" / "reloaded"
    with pytest.raises(CharacterizationError, match="settings"):
        write_reports(second, output, report)
    assert not output.exists()


@pytest.mark.parametrize("target", ["configuration", "settings", "source"])
def test_changes_during_analysis_are_rejected(tmp_path, monkeypatch, target):
    settings = make_fixture(tmp_path, ["0", "1"])
    original = characterization.distribution_profile

    def mutate(*args, **kwargs):
        result = original(*args, **kwargs)
        if target == "configuration":
            settings.config_path.write_bytes(settings.config_path.read_bytes() + b"# changed\n")
        elif target == "settings":
            settings.expected_hashes["metadata"] = sha256(settings.metadata)
        else:
            settings.electricity.write_bytes(settings.electricity.read_bytes() + b"\n")
        return result

    monkeypatch.setattr(characterization, "distribution_profile", mutate)
    with pytest.raises(CharacterizationError, match="changed"):
        analyze(settings)
    assert not settings.output_root.exists()


def test_loaded_configuration_change_is_rejected_before_analysis(tmp_path):
    settings = make_fixture(tmp_path, ["1"])
    loaded = replace(settings, config_sha256=sha256(settings.config_path))
    settings.config_path.write_bytes(settings.config_path.read_bytes() + b"# changed\n")
    with pytest.raises(CharacterizationError, match="since loading"):
        analyze(loaded)
    assert not settings.output_root.exists()


@pytest.mark.parametrize("target", ["configuration", "metrics", "legacy", "provenance"])
def test_changed_or_unbound_analysis_is_not_published(tmp_path, target):
    settings = make_fixture(tmp_path, ["0", "1"])
    report = analyze(settings)
    if target == "configuration":
        settings.config_path.write_bytes(settings.config_path.read_bytes() + b"# changed\n")
    elif target == "metrics":
        report["buildings"][0]["statistics"]["mean"] = 12345
    elif target == "legacy":
        report.pop("analysis_provenance")
        report["schema_version"] = "h03.2-v1"
    else:
        report["analysis_provenance"]["effective_settings"]["reference"] = "changed"
    output = settings.output_root / "synthetic" / target
    with pytest.raises(CharacterizationError):
        write_reports(settings, output, report)
    assert not output.exists()


def test_publication_uses_independent_analytical_copy(tmp_path, monkeypatch):
    settings = make_fixture(tmp_path, ["0", "1"])
    report = analyze(settings)
    expected = deepcopy(report)
    original = characterization._write_csv

    def mutate(path, rows):
        original(path, rows)
        report["buildings"][0]["statistics"]["mean"] = 12345

    monkeypatch.setattr(characterization, "_write_csv", mutate)
    output = settings.output_root / "synthetic" / "independent-copy"
    write_reports(settings, output, report)
    assert json.loads((output / "report.json").read_text())["metrics"] == expected
    with (output / "building_quality.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert float(rows[0]["mean"]) == expected["buildings"][0]["statistics"]["mean"]
    assert (output / "SUCCESS.json").is_file()


@pytest.mark.parametrize("relative_config", [False, True])
@pytest.mark.parametrize("when", ["analysis", "publication"])
def test_relative_sources_cannot_rebind_after_working_directory_change(
        tmp_path, monkeypatch, relative_config, when):
    first, second = tmp_path / "first", tmp_path / "second"
    first.mkdir()
    settings = make_fixture(first, ["0", "1"])
    shutil.copytree(first, second)
    settings = replace(settings, raw_root=Path("raw"), metadata=Path("raw/metadata.csv"),
                       electricity=Path("raw/electricity.csv"),
                       config_path=Path("config.toml") if relative_config else settings.config_path)
    monkeypatch.chdir(first)
    if when == "analysis":
        original = characterization.distribution_profile

        def change_directory(*args, **kwargs):
            result = original(*args, **kwargs)
            monkeypatch.chdir(second)
            return result

        monkeypatch.setattr(characterization, "distribution_profile", change_directory)
        with pytest.raises(CharacterizationError, match="changed"):
            analyze(settings)
        assert not settings.output_root.exists()
    else:
        report = analyze(settings)
        monkeypatch.chdir(second)
        output = settings.output_root / "synthetic" / "relocated"
        with pytest.raises(CharacterizationError, match="changed"):
            write_reports(settings, output, report)
        assert not output.exists()


@pytest.mark.parametrize("stage", ["write", "close", "link"])
def test_success_marker_failures_never_leave_partial_final(tmp_path, monkeypatch, stage):
    settings = make_fixture(tmp_path, ["0", "1"])
    report = analyze(settings)
    output = settings.output_root / "synthetic" / stage
    original_dump, original_open = json.dump, Path.open

    def dump(payload, handle, **kwargs):
        if stage == "write" and "metrics_sha256" in payload:
            handle.write("{")
            raise OSError("synthetic marker write failure")
        return original_dump(payload, handle, **kwargs)

    def open_marker(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if stage != "close" or path.name != ".SUCCESS.json.tmp":
            return handle
        class Writer:
            def write(self, payload):
                return handle.write(payload)

            def close(self):
                handle.close()
                raise OSError("synthetic marker close failure")
        return Writer()

    def fail_link(*args, **kwargs):
        raise OSError("synthetic marker link failure")

    monkeypatch.setattr(json, "dump", dump)
    monkeypatch.setattr(Path, "open", open_marker)
    if stage == "link":
        monkeypatch.setattr(os, "link", fail_link)
    with pytest.raises(OSError, match="synthetic marker"):
        write_reports(settings, output, report)
    assert (output / "report.json").is_file()
    assert not (output / "SUCCESS.json").exists()
    assert not (output / ".SUCCESS.json.tmp").exists()
    with pytest.raises(CharacterizationError, match="already exists"):
        write_reports(settings, output, report)


@pytest.mark.parametrize("target", ["configuration", "source", "settings"])
def test_inputs_are_rechecked_after_success_marker_close(tmp_path, monkeypatch, target):
    settings = make_fixture(tmp_path, ["0", "1"])
    report = analyze(settings)
    original_open = Path.open

    def open_marker(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if path.name != ".SUCCESS.json.tmp":
            return handle
        class Writer:
            def write(self, payload):
                return handle.write(payload)

            def close(self):
                handle.close()
                if target == "configuration":
                    settings.config_path.write_bytes(settings.config_path.read_bytes() + b"# changed\n")
                elif target == "source":
                    settings.electricity.write_bytes(settings.electricity.read_bytes() + b"\n")
                else:
                    settings.expected_hashes["metadata"] = sha256(settings.metadata)
        return Writer()

    monkeypatch.setattr(Path, "open", open_marker)
    output = settings.output_root / "synthetic" / "changed-after-close"
    with pytest.raises(CharacterizationError, match="changed"):
        write_reports(settings, output, report)
    assert (output / "report.json").is_file()
    assert not (output / "SUCCESS.json").exists()


@pytest.mark.parametrize("stage", ["close", "link"])
def test_existing_or_concurrent_success_is_never_replaced(tmp_path, monkeypatch, stage):
    settings = make_fixture(tmp_path, ["1"])
    report = analyze(settings)
    output = settings.output_root / "synthetic" / "competing-marker"
    original_open, original_link = Path.open, os.link

    def open_marker(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if stage != "close" or path.name != ".SUCCESS.json.tmp":
            return handle
        class Writer:
            def write(self, payload):
                return handle.write(payload)

            def close(self):
                handle.close()
                (output / "SUCCESS.json").write_bytes(b"synthetic existing marker\n")
        return Writer()

    def concurrent_link(*args, **kwargs):
        (output / "SUCCESS.json").write_bytes(b"synthetic existing marker\n")
        return original_link(*args, **kwargs)

    monkeypatch.setattr(Path, "open", open_marker)
    if stage == "link":
        monkeypatch.setattr(os, "link", concurrent_link)
    with pytest.raises(FileExistsError):
        write_reports(settings, output, report)
    assert (output / "SUCCESS.json").read_bytes() == b"synthetic existing marker\n"


@pytest.mark.parametrize("when", ["before", "after"])
@pytest.mark.parametrize("cancel", [KeyboardInterrupt, SystemExit])
def test_success_publication_cancellation_boundary(tmp_path, monkeypatch, when, cancel):
    settings = make_fixture(tmp_path, ["0", "1"])
    report = analyze(settings)
    output = settings.output_root / "synthetic" / "interrupted"
    interruption = cancel(67)
    original = os.link

    def interrupt(*args, **kwargs):
        if when == "before":
            raise interruption
        original(*args, **kwargs)
        raise interruption

    monkeypatch.setattr(os, "link", interrupt)
    with pytest.raises(cancel) as caught:
        write_reports(settings, output, report)
    assert caught.value is interruption
    if cancel is SystemExit:
        assert caught.value.code == 67
    assert (output / "SUCCESS.json").exists() is (when == "after")
    if when == "after":
        assert json.loads((output / "SUCCESS.json").read_text())["source_hashes"] == {
            name: report["inventory"][name]["sha256"] for name in ("metadata", "electricity")
        }
    assert not (output / ".SUCCESS.json.tmp").exists()


def test_foreign_success_temporary_is_not_removed(tmp_path, monkeypatch):
    settings = make_fixture(tmp_path, ["1"])
    report = analyze(settings)
    output = settings.output_root / "synthetic" / "foreign-temporary"
    original = characterization._write_csv

    def insert(path, rows):
        original(path, rows)
        temporary = output / ".SUCCESS.json.tmp"
        if not temporary.exists():
            temporary.write_bytes(b"synthetic foreign temporary\n")

    monkeypatch.setattr(characterization, "_write_csv", insert)
    with pytest.raises(FileExistsError):
        write_reports(settings, output, report)
    assert (output / ".SUCCESS.json.tmp").read_bytes() == b"synthetic foreign temporary\n"
    assert not (output / "SUCCESS.json").exists()


def test_marker_failure_preserves_primary_and_secondary_traces(tmp_path, monkeypatch):
    settings = make_fixture(tmp_path, ["1"])
    report = analyze(settings)
    output = settings.output_root / "synthetic" / "multiple-failures"
    first = OSError("synthetic primary write failure")
    original_dump, original_open, original_unlink = json.dump, Path.open, Path.unlink

    def dump(payload, handle, **kwargs):
        if "metrics_sha256" in payload:
            handle.write("{")
            raise first
        return original_dump(payload, handle, **kwargs)

    def open_marker(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if path.name != ".SUCCESS.json.tmp":
            return handle
        class Writer:
            def write(self, payload):
                return handle.write(payload)

            def close(self):
                handle.close()
                raise RuntimeError("synthetic secondary close failure") from ValueError("close cause")
        return Writer()

    def unlink(path, *args, **kwargs):
        if path.name == ".SUCCESS.json.tmp":
            raise OSError("synthetic secondary cleanup failure")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(json, "dump", dump)
    monkeypatch.setattr(Path, "open", open_marker)
    monkeypatch.setattr(Path, "unlink", unlink)
    with pytest.raises(OSError) as caught:
        write_reports(settings, output, report)
    assert caught.value is first
    notes = "\n".join(first.__notes__)
    assert "secondary close failure" in notes and "close cause" in notes
    assert "secondary cleanup failure" in notes and "Traceback" in notes
    assert not (output / "SUCCESS.json").exists()


@pytest.mark.parametrize("primary_kind", ["ordinary", "cancellation"])
def test_marker_close_cancellation_preserves_control_flow(tmp_path, monkeypatch, primary_kind):
    settings = make_fixture(tmp_path, ["1"])
    report = analyze(settings)
    output = settings.output_root / "synthetic" / "cancelled-close"
    first = (OSError("synthetic first write failure") if primary_kind == "ordinary"
             else KeyboardInterrupt("synthetic first cancellation"))
    secondary = SystemExit(71)
    original_dump, original_open = json.dump, Path.open

    def dump(payload, handle, **kwargs):
        if "metrics_sha256" in payload:
            raise first
        return original_dump(payload, handle, **kwargs)

    def open_marker(path, *args, **kwargs):
        handle = original_open(path, *args, **kwargs)
        if path.name != ".SUCCESS.json.tmp":
            return handle
        class Writer:
            def write(self, payload):
                return handle.write(payload)

            def close(self):
                handle.close()
                raise secondary
        return Writer()

    monkeypatch.setattr(json, "dump", dump)
    monkeypatch.setattr(Path, "open", open_marker)
    expected = secondary if primary_kind == "ordinary" else first
    with pytest.raises(type(expected)) as caught:
        write_reports(settings, output, report)
    assert caught.value is expected
    if primary_kind == "ordinary":
        assert caught.value.code == 71
        assert caught.value.__cause__ is first
    assert not (output / "SUCCESS.json").exists()
