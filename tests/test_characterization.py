"""Synthetic tests for H03.2; these fixtures are not energy-analysis evidence."""
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
import csv
import json
from pathlib import Path

import pytest

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
