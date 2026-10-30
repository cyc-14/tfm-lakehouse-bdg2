"""Synthetic tests for weather diagnostics; no real data is used."""
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import csv
import hashlib
import json
from pathlib import Path

import pytest

from tfm_lakehouse import weather_characterization as weather


pytestmark = pytest.mark.unit
START = "2020-01-01 00:00:00"
END = "2020-01-01 02:00:00"


def row(time=START, site="Site", **values):
    defaults = {name: "1" for name in weather.VARIABLES}
    defaults.update(values)
    return [time, site, *(defaults[name] for name in weather.VARIABLES)]


def make_fixture(tmp_path, rows=None, metadata=None, start=START, end=END,
                 electricity_times=None):
    raw = tmp_path / "raw"
    raw.mkdir()
    metadata = metadata or [("Site_lodging_A", "Site", "Europe/London")]
    for name, header, values in (
        ("metadata", ["building_id", "site_id", "timezone"], metadata),
        ("electricity", ["timestamp", *(m[0] for m in metadata)],
         [[t, *(["7"] * len(metadata))] for t in (electricity_times or [start, end])]),
        ("weather", ["timestamp", "site_id", *weather.VARIABLES],
         rows if rows is not None else [row(start), row(end)]),
    ):
        with (raw / f"{name}.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(header)
            writer.writerows(values)
    candidates = "\n".join(
        f'[[candidates]]\ncase_id = "CASE_{i}"\nbuilding_id = {json.dumps(m[0])}\ngroup = "synthetic"'
        for i, m in enumerate(metadata)
    )
    context = tmp_path / "context.toml"
    context.write_text(
        '[paths]\noutput_root = "h03_2"\n[dataset]\nkind = "synthetic"\n' + candidates,
        encoding="utf-8",
    )
    config = tmp_path / "weather.toml"
    config.write_text(
        '[paths]\nraw_root = "raw"\noutput_root = "checks"\nlakehouse_root = "lakehouse"\n'
        '[dataset]\nkind = "synthetic"\nreference = "synthetic software fixture"\n'
        '[sources]\nmetadata = "metadata.csv"\nelectricity = "electricity.csv"\nweather = "weather.csv"\n'
        '[context]\ncharacterization_config = "context.toml"\n'
        '[analysis]\n' + f'nominal_start = "{start}"\nnominal_end = "{end}"\n',
        encoding="utf-8",
    )
    return weather.load_weather_settings(config)


def metrics(settings):
    return weather.analyze_weather(settings)["metrics"]


def quality(report, variable="airTemperature", site="Site"):
    return next(r for r in report["variable_quality"]
                if r["variable"] == variable and r["site_id"] == site)


def test_partitions_cells_and_preserves_special_codes(tmp_path):
    tokens = ["", "NaN", "bad", "Infinity", "nan", "0", "-1", "9", "1000"]
    rows = [row(START, airTemperature=t, precipDepth1HR=t, cloudCoverage=t) for t in tokens]
    report = metrics(make_fixture(tmp_path, rows))
    result = quality(report)
    assert (result["missing"], result["nonnumeric"], result["nonfinite"],
            result["finite_readings"]) == (2, 1, 2, 4)
    assert result["source_records"] == 9
    assert result["missing_pct_received"] == pytest.approx(200 / 9)
    assert result["zero_pct_finite"] == result["negative_pct_finite"] == 25
    assert (result["minimum"], result["maximum"]) == (-1, 1000)
    assert quality(report, "precipDepth1HR")["special_code_tokens_preserved"] == {"-1": 1}
    assert quality(report, "cloudCoverage")["special_code_tokens_preserved"] == {"9": 1}


@pytest.mark.parametrize("token", ["-1", "-1.0", "9", "0", " 9 ", "1e3"])
def test_finite_codes_cannot_be_reclassified_as_missing(tmp_path, token):
    settings = make_fixture(tmp_path)
    with pytest.raises(weather.WeatherError, match="cannot be reclassified"):
        weather.validate_settings(replace(settings, missing_tokens=("", token)))


def test_duplicate_exact_conflicting_and_aab_are_distinguished(tmp_path):
    a, b = row(START), row(START, airTemperature="2")
    last = row(END)
    settings = make_fixture(tmp_path, [a, a, b, last, last])
    before = weather.shared.sha256(settings.weather)
    report = metrics(settings)
    assert report["weather"]["received_records"] == 5
    assert report["weather"]["duplicate_keys"] == 2
    assert report["weather"]["exact_duplicate_keys"] == 1
    assert report["weather"]["conflicting_duplicate_keys"] == 1
    assert report["weather"]["additional_rows_in_repeated_keys"] == 3
    assert report["weather"]["exact_duplicate_extra_rows"] == 2
    conflict = next(r for r in report["duplicate_keys"] if r["kind"] == "conflicting")
    assert conflict["source_record_examples"] == [1, 2, 3]
    assert conflict["distinct_parsed_payloads"] == 2
    assert weather.shared.sha256(settings.weather) == before


def test_thirty_seven_conflicting_keys_are_detected_from_synthetic_rows(tmp_path):
    times = [(datetime(2020, 1, 1) + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S")
             for i in range(37)]
    rows = [r for t in times for r in (row(t), row(t, airTemperature="2"))]
    report = metrics(make_fixture(tmp_path, rows, end=times[-1]))
    assert report["weather"]["conflicting_duplicate_keys"] == 37
    assert report["weather"]["rows_in_repeated_keys"] == 74
    assert report["weather"]["exact_duplicate_keys"] == 0


def test_same_label_in_different_sites_is_not_a_duplicate(tmp_path):
    metadata = [("Site_lodging_A", "Site", "Europe/London"),
                ("Other_lodging_B", "Other", "Europe/Dublin")]
    report = metrics(make_fixture(tmp_path, [row(), row(site="Other")], metadata))
    assert report["weather"]["duplicate_keys"] == 0
    assert len(report["site_coverage"]) == 2
    assert all(r["source_records"] == 1 for r in report["site_coverage"])


def test_nominal_coverage_excludes_duplicate_and_outside_labels(tmp_path):
    rows = [row(START), row(START), row(END), row("2020-01-02 00:00:00")]
    report = metrics(make_fixture(tmp_path, rows))
    item = next(r for r in report["period_coverage"] if
                r["variable"] == "airTemperature" and r["basis"] == "nominal_calendar"
                and r["scale"] == "month")
    assert item["received_records"] == 4
    assert item["expected_local_labels"] == 3
    assert item["expected_labels_absent_from_source"] == 1
    assert item["source_labels_outside_grid"] == 1
    assert item["finite_pct_expected_labels"] == pytest.approx(200 / 3)


def test_empty_month_and_invalid_label_have_separate_denominators(tmp_path):
    start, end = "2021-01-31 23:00:00", "2021-03-01 00:00:00"
    settings = make_fixture(tmp_path, [row(start), row(end), row("invalid", airTemperature="")],
                            start=start, end=end)
    report = metrics(settings)
    feb = next(r for r in report["period_coverage"] if
               r["variable"] == "airTemperature" and r["basis"] == "nominal_calendar"
               and r["scale"] == "month" and r["period"] == "2021-02")
    assert feb["received_records"] == 0
    assert feb["expected_local_labels"] == feb["expected_labels_absent_from_source"] == 672
    assert feb["missing_pct_received"] is None
    assert feb["finite_pct_expected_labels"] == 0
    unassigned = next(r for r in report["period_coverage"] if
                      r["variable"] == "airTemperature" and r["basis"] == "nominal_calendar"
                      and r["scale"] == "month" and r["period"] == "unassigned")
    assert unassigned["received_records"] == unassigned["missing_values"] == 1
    assert unassigned["expected_local_labels"] is None


def test_missing_candidate_weather_is_reported_not_invented(tmp_path):
    settings = make_fixture(tmp_path, [row(site="Unmapped")])
    report = metrics(settings)
    candidate = report["candidate_weather_availability"][0]
    assert candidate["weather_site_present"] is False
    assert candidate["weather_received_records"] == 0
    assert candidate["variables"] == {}
    assert report["metadata"]["weather_sites_without_metadata"] == ["Unmapped"]


@pytest.mark.parametrize("source", ["metadata", "electricity", "weather"])
def test_source_hash_mismatch_fails_without_output(tmp_path, source):
    settings = make_fixture(tmp_path)
    with pytest.raises(weather.WeatherError, match="SHA-256 mismatch"):
        weather.analyze_weather(replace(settings, expected_hashes={source: "0" * 64}))
    assert not settings.output_root.exists()


@pytest.mark.parametrize("content", [
    "", "timestamp,site_id\n", "version https://git-lfs.github.com/spec/v1\n",
    "timestamp,site_id,airTemperature\n2020-01-01 00:00:00,Site,1,extra\n",
    'timestamp,site_id\n"unterminated\n',
])
def test_invalid_weather_csv_fails_without_output(tmp_path, content):
    settings = make_fixture(tmp_path)
    settings.weather.write_text(content)
    with pytest.raises(weather.WeatherError):
        weather.analyze_weather(settings)
    assert not settings.output_root.exists()


def test_invalid_utf8_body_is_rejected(tmp_path):
    settings = make_fixture(tmp_path)
    with settings.weather.open("ab") as handle:
        handle.write(b"\xff\n")
    with pytest.raises(weather.WeatherError, match="UTF-8"):
        weather.analyze_weather(settings)


def test_empty_numeric_population_is_not_zero(tmp_path):
    report = metrics(make_fixture(tmp_path, [row(airTemperature="")]))
    q = quality(report)
    assert q["finite_readings"] == 0
    assert q["minimum"] is q["maximum"] is q["zero_pct_finite"] is None


def test_dst_is_a_calendar_probe_and_does_not_assign_observation_fold(tmp_path):
    start, end = "2020-03-29 01:00:00", "2020-10-25 01:00:00"
    settings = make_fixture(tmp_path, [row(start), row(end), row(end)],
                            start=start, end=end)
    report = metrics(settings)
    profile = next(r for r in report["dst_diagnostics"] if r["source"] == "weather")
    assert profile["status"] == "calendar_hypothesis_only"
    assert profile["counts_received_rows"] == {"ambiguous": 2, "nonexistent": 1}
    assert report["weather"]["received_records"] == 3
    assert report["integration_readiness"] == "not_validated_no_hourly_join_performed"
    assert all("fold" not in item for item in profile["examples"])


def test_alias_resolution_uses_documented_target_but_keeps_original(tmp_path, monkeypatch):
    real_zone_info = weather.ZoneInfo

    class WithoutAliases:
        @staticmethod
        def no_cache(name):
            if name.startswith("US/"):
                raise weather.ZoneInfoNotFoundError(name)
            return real_zone_info.no_cache(name)

    monkeypatch.setattr(weather, "ZoneInfo", WithoutAliases)
    settings = make_fixture(tmp_path, metadata=[("Site_lodging_A", "Site", "US/Eastern")])
    analysis = weather.analyze_weather(settings)
    tz = analysis["metrics"]["timezone_inventory"][0]
    assert tz["original_timezone"] == "US/Eastern"
    assert tz["documented_alias_target"] == "America/New_York"
    assert analysis["provenance"]["timezone_environment"]["availability"]["US/Eastern"] == "unavailable_in_environment"
    profile = next(r for r in analysis["metrics"]["dst_diagnostics"] if r["source"] == "weather")
    assert profile["diagnostic_zone"] == "America/New_York"


def test_missing_timezone_database_is_an_explicit_limitation(tmp_path, monkeypatch):
    class NoZones:
        @staticmethod
        def no_cache(name):
            raise weather.ZoneInfoNotFoundError(name)

    monkeypatch.setattr(weather, "ZoneInfo", NoZones)
    report = metrics(make_fixture(tmp_path))
    assert all(r["status"] == "timezone_unavailable" for r in report["dst_diagnostics"])
    assert all(r["counts_received_rows"] is None for r in report["dst_diagnostics"])


def test_conflicting_site_timezones_are_not_chosen_arbitrarily(tmp_path):
    metadata = [("Site_lodging_A", "Site", "Europe/London"),
                ("Site_lodging_B", "Site", "Europe/Dublin")]
    report = metrics(make_fixture(tmp_path, metadata=metadata))
    assert report["metadata"]["conflicting_site_timezones"] == ["Site"]
    profile = next(r for r in report["dst_diagnostics"] if r["source"] == "weather")
    assert profile["status"] == "missing_or_conflicting_metadata_timezone"


@pytest.mark.parametrize("change", [
    {"kind": "invalid"}, {"max_grid_slots": 0}, {"max_grid_slots": 1},
    {"missing_tokens": ("", "")}, {"expected_hashes": {"weather": "ABC"}},
    {"max_grid_slots": True}, {"missing_tokens": (None,)},
    {"nominal_start": "invalid"},
    {"nominal_start": datetime(2020, 1, 1, tzinfo=timezone.utc)},
    {"nominal_end": datetime(2020, 1, 1, 2, tzinfo=timezone.utc)},
])
def test_invalid_settings_are_rejected(tmp_path, change):
    settings = make_fixture(tmp_path)
    with pytest.raises(weather.WeatherError):
        weather.validate_settings(replace(settings, **change))


@pytest.mark.parametrize("table", ["paths", "dataset", "sources", "context", "analysis"])
def test_invalid_toml_table_types_raise_contract_error(tmp_path, table):
    settings = make_fixture(tmp_path)
    content = settings.config_path.read_text()
    start = content.index(f"[{table}]\n")
    following = content.find("\n[", start + 1)
    content = content[:start] + (content[following + 1:] if following >= 0 else "")
    settings.config_path.write_text(f"{table} = 1\n" + content)
    with pytest.raises(weather.WeatherError):
        weather.load_weather_settings(settings.config_path)


def test_context_kind_and_candidate_ids_are_checked(tmp_path):
    settings = make_fixture(tmp_path)
    settings.context_config.write_text(settings.context_config.read_text().replace(
        'kind = "synthetic"', 'kind = "real"'))
    with pytest.raises(weather.WeatherError, match="kinds must match"):
        weather.load_weather_settings(settings.config_path)


@pytest.mark.parametrize("source", ["metadata", "electricity", "weather"])
def test_sources_outside_raw_root_are_rejected(tmp_path, source):
    settings = make_fixture(tmp_path)
    outside = tmp_path / "outside.csv"
    outside.write_text("synthetic\n1\n")
    with pytest.raises(weather.WeatherError, match="inside raw_root"):
        weather.validate_settings(replace(settings, **{source: outside}))


def test_off_hour_source_does_not_assert_observed_hourly_grid(tmp_path):
    report = metrics(make_fixture(tmp_path, [row("2020-01-01 00:30:00")]))
    assert report["site_coverage"][0]["expected_hourly_labels_in_observed_span"] is None
    assert report["site_coverage"][0]["grid_limitation"]
    observed = next(r for r in report["period_coverage"] if
                    r["variable"] == "airTemperature" and r["basis"] == "observed_span")
    assert observed["expected_local_labels"] is None


def test_candidate_missing_from_electricity_is_rejected(tmp_path):
    settings = make_fixture(tmp_path)
    settings.electricity.write_text('timestamp,Other\n2020-01-01 00:00:00,1\n')
    with pytest.raises(weather.WeatherError, match="Candidates absent"):
        weather.analyze_weather(settings)


def test_duplicate_metadata_keys_are_rejected(tmp_path):
    settings = make_fixture(tmp_path)
    with settings.metadata.open("a") as h:
        h.write("Site_lodging_A,Site,Europe/London\n")
    with pytest.raises(weather.WeatherError, match="unique"):
        weather.analyze_weather(settings)


def test_output_protects_raw_lakehouse_context_and_h03_2(tmp_path):
    settings = make_fixture(tmp_path)
    for root in (settings.raw_root, settings.lakehouse_root,
                 settings.h03_2_output_root, settings.context_config):
        modified = replace(settings, output_root=root)
        with pytest.raises(weather.WeatherError, match="overlaps"):
            weather.validate_output(modified, root / "synthetic" / "run")
    with pytest.raises(weather.WeatherError):
        weather.validate_output(settings, settings.output_root / "real" / "run")


def test_output_symlink_alias_is_rejected(tmp_path):
    settings = make_fixture(tmp_path)
    settings.output_root.mkdir()
    (settings.output_root / "synthetic").symlink_to(settings.raw_root, target_is_directory=True)
    with pytest.raises(weather.WeatherError, match="aliases"):
        weather.validate_output(settings, settings.output_root / "synthetic" / "run")


def test_real_output_git_ignore_guard_uses_only_synthetic_files(tmp_path):
    import subprocess
    settings = make_fixture(tmp_path)
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    settings = replace(settings, kind="real", output_root=repository / "checks",
                       expected_hashes={n: weather.shared.sha256(getattr(settings, n))
                                        for n in ("metadata", "electricity", "weather")})
    output = settings.output_root / "real" / "run"
    with pytest.raises(weather.WeatherError, match="ignored by Git"):
        weather.validate_output(settings, output)
    (repository / ".gitignore").write_text("/checks/\n")
    assert weather.validate_output(settings, output) == output
    assert not output.exists()


def test_deterministic_metrics_and_complete_exclusive_publication(tmp_path):
    settings = make_fixture(tmp_path)
    first = weather.analyze_weather(settings)
    assert first == weather.analyze_weather(settings)
    output = settings.output_root / "synthetic" / "run"
    weather.write_weather_reports(settings, output, first, argv=["synthetic-test"])
    payload = json.loads((output / "report.json").read_text())
    assert payload["metrics"] == first["metrics"]
    success = json.loads((output / "SUCCESS.json").read_text())
    for name, digest in success["artifact_hashes"].items():
        assert weather.shared.sha256(output / name) == digest
    computed = hashlib.sha256(json.dumps(first["metrics"], ensure_ascii=False,
                                         sort_keys=True, allow_nan=False).encode()).hexdigest()
    assert success["metrics_sha256"] == computed
    with pytest.raises(weather.WeatherError, match="already exists"):
        weather.write_weather_reports(settings, output, first)


@pytest.mark.parametrize("change", [
    {"reference": "different"}, {"max_grid_slots": 200000},
    {"missing_tokens": ("",)}, {"nominal_end": datetime(2020, 1, 1, 3)},
])
def test_publication_rejects_analysis_from_different_settings(tmp_path, change):
    settings = make_fixture(tmp_path)
    analysis = weather.analyze_weather(settings)
    output = settings.output_root / "synthetic" / "mixed"
    with pytest.raises(weather.WeatherError, match="current settings"):
        weather.write_weather_reports(replace(settings, **change), output, analysis)
    assert not output.exists()


@pytest.mark.parametrize("field", ["weather", "config_path", "context_config"])
def test_input_change_before_publication_leaves_no_directory(tmp_path, field):
    settings = make_fixture(tmp_path)
    analysis = weather.analyze_weather(settings)
    path = getattr(settings, field)
    path.write_bytes(path.read_bytes() + b"\n")
    output = settings.output_root / "synthetic" / "changed"
    with pytest.raises(weather.WeatherError, match="changed"):
        weather.write_weather_reports(settings, output, analysis)
    assert not output.exists()


@pytest.mark.parametrize("field", ["weather", "config_path", "context_config"])
def test_input_change_during_analysis_is_detected(tmp_path, monkeypatch, field):
    settings = make_fixture(tmp_path)
    original = weather.audit_timezones

    def mutate(*args):
        result = original(*args)
        path = getattr(settings, field)
        path.write_bytes(path.read_bytes() + b"\n")
        return result

    monkeypatch.setattr(weather, "audit_timezones", mutate)
    with pytest.raises(weather.WeatherError, match="changed"):
        weather.analyze_weather(settings)
    assert not settings.output_root.exists()


def test_write_failure_retains_partial_directory_without_success(tmp_path, monkeypatch):
    settings = make_fixture(tmp_path)
    analysis = weather.analyze_weather(settings)
    output = settings.output_root / "synthetic" / "partial"

    def fail(*args):
        raise OSError("synthetic storage failure")

    monkeypatch.setattr(weather.shared, "_write_csv", fail)
    with pytest.raises(OSError, match="storage failure"):
        weather.write_weather_reports(settings, output, analysis)
    assert (output / "report.json").exists()
    assert not (output / "SUCCESS.json").exists()
    with pytest.raises(weather.WeatherError, match="already exists"):
        weather.validate_output(settings, output)


def test_source_change_during_publication_prevents_success(tmp_path, monkeypatch):
    settings = make_fixture(tmp_path)
    analysis = weather.analyze_weather(settings)
    original = weather.shared._write_csv
    changed = False

    def mutate(path, rows):
        nonlocal changed
        original(path, rows)
        if not changed:
            settings.weather.write_bytes(settings.weather.read_bytes() + b"\n")
            changed = True

    monkeypatch.setattr(weather.shared, "_write_csv", mutate)
    output = settings.output_root / "synthetic" / "changed"
    with pytest.raises(weather.WeatherError, match="changed"):
        weather.write_weather_reports(settings, output, analysis)
    assert output.exists()
    assert not (output / "SUCCESS.json").exists()


def test_success_marker_write_failure_leaves_no_success(tmp_path, monkeypatch):
    settings = make_fixture(tmp_path)
    analysis = weather.analyze_weather(settings)
    output = settings.output_root / "synthetic" / "failed-marker"
    original = weather.json.dump

    def fail_marker(payload, handle, **kwargs):
        if "artifact_hashes" in payload:
            handle.write("{")
            raise OSError("synthetic success marker failure")
        return original(payload, handle, **kwargs)

    monkeypatch.setattr(weather.json, "dump", fail_marker)
    with pytest.raises(OSError, match="success marker failure"):
        weather.write_weather_reports(settings, output, analysis)
    assert output.exists()
    assert not (output / "SUCCESS.json").exists()


def test_synthetic_cli_success_and_error(tmp_path, capsys):
    settings = make_fixture(tmp_path)
    output = settings.output_root / "synthetic" / "cli"
    arguments = ["--config", str(settings.config_path), "--output", str(output)]
    assert weather.main(arguments) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "completed"
    assert weather.main(arguments) == 1
    assert "already exists" in capsys.readouterr().err
