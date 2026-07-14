"""Tests for the flag-ON empty-operational-schedules robustness guard
(finding #2).

The long-term readers resolve per-horizon operational schedules via
`_operational_schedules_for_horizon_type(htype)`, which returns an EMPTY
dict when the deployment's ``ieasyhydroforecast_ml_long_term_supported_modes``
contains NO mode for that horizon (a typo, or only a legacy/other-horizon
mode). Previously the readers guarded selection with
``if lead_aware and <schedules>:`` -- an empty dict is falsy, so under
flag-ON the operational-issuance selector was SILENTLY SKIPPED and the
reader returned UNFILTERED rows (non-operational re-issues / backfills
leaking in), or -- for the quarterly readers -- crashed on
``quarter_horizon_value()``.

These tests lock the fix: under flag-ON, if a horizon's resolved
operational-schedules dict is EMPTY, the reader logs a clear WARNING and
returns its own established empty shape (no unfiltered rows, no crash).
Flag-OFF behavior is unchanged (schedules are never resolved, rows are
returned unfiltered as before).

NOTE: this is distinct from the "fail loud" contract (a mode config
missing ``operational_issue_day`` still RAISES during resolution, BEFORE
the empty guard is reached -- see
``test_lead_aware_operational_issuance_wiring.py`` /
``test_lead_aware_latest_readers.py``). "No schedules configured" is a
legitimate deployment state (a deployment may not run a given horizon);
"schedule present but malformed" is a misconfiguration that must crash.
"""

import datetime as dt
import json
import logging
import os
import sys
from unittest.mock import patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import data_reader

CODE = "19999"
LOGGER_NAME = "src.data_reader"


def _write_mode_config(config_dir, mode, lead, issue_day=None):
    payload = {"operational_month_lead_time": lead}
    if issue_day is not None:
        payload["operational_issue_day"] = issue_day
    (config_dir / f"{mode}.json").write_text(json.dumps(payload))


def _set_long_term_env(monkeypatch, tmp_path, modes):
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "long_term")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", ",".join(modes))


def _month_row(code, year, month, issue_date, model="LR_Base", horizon_value=99, q50=100.0):
    valid_from = f"{year}-{month:02d}-01"
    return {
        "horizon_type": "month",
        "horizon_value": horizon_value,
        "code": code,
        "date": issue_date,
        "model_type": model,
        "valid_from": valid_from,
        "valid_to": valid_from,
        "q50": q50,
        "q05": q50 - 30.0,
        "q10": q50 - 25.0,
        "q25": q50 - 15.0,
        "q75": q50 + 15.0,
        "q90": q50 + 25.0,
        "q95": q50 + 30.0,
        "id": 1,
        "model_type_description": model,
    }


def _season_row(code, valid_from, issue_date, model="LR_Base", horizon_value=0, q50=100.0):
    return {
        "horizon_type": "season",
        "horizon_value": horizon_value,
        "code": code,
        "date": issue_date,
        "model_type": model,
        "valid_from": valid_from,
        "valid_to": valid_from,
        "q50": q50,
        "q05": q50 - 30.0,
        "q10": q50 - 25.0,
        "q25": q50 - 15.0,
        "q75": q50 + 15.0,
        "q90": q50 + 25.0,
        "q95": q50 + 30.0,
        "id": 1,
        "model_type_description": model,
    }


def _has_empty_schedules_warning(caplog):
    return any(
        "no operational" in r.getMessage().lower()
        and "ieasyhydroforecast_ml_long_term_supported_modes" in r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING
    )


# ---------------------------------------------------------------------------
# Site 1 -- read_monthly_forecasts
# ---------------------------------------------------------------------------


class TestMonthlyEmptySchedulesFlagOn:
    """Flag ON, no month mode configured (only 'quarter'): the month
    schedules dict is empty -> warn + return empty, never an unfiltered read.
    """

    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        # Only a quarter mode -> NO month_N mode -> month schedules empty.
        _set_long_term_env(monkeypatch, tmp_path, ["quarter"])
        _write_mode_config(config_dir, "quarter", lead=1, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_warns_returns_empty_and_does_not_read_api(self, caplog):
        rows = [
            _month_row(CODE, 2024, 6, "2024-05-25", q50=111.0),
            _month_row(CODE, 2024, 6, "2024-05-10", q50=999.0),  # would leak pre-fix
        ]

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            return pd.DataFrame(rows)

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with patch(
                "src.data_reader._read_long_forecasts_api", side_effect=fake_api
            ) as mock_api:
                result = data_reader.read_monthly_forecasts([CODE], 2024, 2024)

        assert result.empty
        assert 999.0 not in set(result.get("q50", pd.Series(dtype=float)).astype(float))
        mock_api.assert_not_called()
        assert _has_empty_schedules_warning(caplog)


# ---------------------------------------------------------------------------
# Site 2 -- read_latest_monthly_forecasts
# ---------------------------------------------------------------------------


class TestLatestMonthlyEmptySchedulesFlagOn:
    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["quarter"])
        _write_mode_config(config_dir, "quarter", lead=1, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_warns_returns_empty_and_does_not_read_api(self, caplog):
        rows = [
            _month_row(CODE, 2024, 6, "2024-05-25", q50=111.0),
            _month_row(CODE, 2024, 6, "2024-05-10", q50=999.0),
        ]

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            return pd.DataFrame(rows)

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with patch(
                "src.data_reader._read_long_forecasts_api", side_effect=fake_api
            ) as mock_api:
                result = data_reader.read_latest_monthly_forecasts(
                    [CODE], forecast_date=dt.date(2024, 6, 30)
                )

        assert result.empty
        mock_api.assert_not_called()
        assert _has_empty_schedules_warning(caplog)


# ---------------------------------------------------------------------------
# Site 3 -- read_quarterly_forecasts
# (pre-fix: quarter_horizon_value() RAISES for an unsupported mode; fixed:
#  warn + empty)
# ---------------------------------------------------------------------------


class TestQuarterlyEmptySchedulesFlagOn:
    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        # A month mode (so Source-1 monthly resolves fine) but NO quarter mode.
        _set_long_term_env(monkeypatch, tmp_path, ["month_0"])
        _write_mode_config(config_dir, "month_0", lead=0, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_warns_returns_empty_no_crash(self, caplog):
        # Source-1 monthly returns data; the quarter guard must still discard
        # everything (no quarter schedule => no operational quarterly output).
        month_rows = [_month_row(CODE, 2024, 6, "2024-06-25", q50=111.0)]

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "month":
                return pd.DataFrame(month_rows)
            return pd.DataFrame()

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api):
                result = data_reader.read_quarterly_forecasts([CODE], 2024, 2024)

        assert result.empty
        assert _has_empty_schedules_warning(caplog)


# ---------------------------------------------------------------------------
# Site 4 -- read_latest_quarterly_forecasts
# ---------------------------------------------------------------------------


class TestLatestQuarterlyEmptySchedulesFlagOn:
    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["month_0"])
        _write_mode_config(config_dir, "month_0", lead=0, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_warns_returns_empty_no_crash(self, caplog):
        month_rows = [_month_row(CODE, 2024, 6, "2024-06-25", q50=111.0)]

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type == "month":
                return pd.DataFrame(month_rows)
            return pd.DataFrame()

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api):
                result = data_reader.read_latest_quarterly_forecasts(
                    [CODE], forecast_date=dt.date(2024, 6, 30)
                )

        assert result.empty
        assert _has_empty_schedules_warning(caplog)


# ---------------------------------------------------------------------------
# Site 5 -- read_seasonal_forecasts
# ---------------------------------------------------------------------------


class TestSeasonalNoSeasonModeFlagOn:
    """Flag ON, no seasonal mode configured at all -> warn + empty."""

    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["quarter"])
        _write_mode_config(config_dir, "quarter", lead=1, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_warns_returns_empty_no_unfiltered_leak(self, caplog):
        rows = [
            _season_row(CODE, "2024-04-01", "2024-04-25", q50=111.0),
            _season_row(CODE, "2024-04-01", "2024-04-10", q50=999.0),  # would leak pre-fix
        ]

        def fake_api(codes, start_year, end_year, horizon_type="season", horizon_value=None):
            return pd.DataFrame(rows)

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api):
                result = data_reader.read_seasonal_forecasts([CODE], 2024, 2024)

        assert result.empty
        assert 999.0 not in set(result.get("q50", pd.Series(dtype=float)).astype(float))
        assert _has_empty_schedules_warning(caplog)


class TestSeasonalNoModeAtRequestedLeadFlagOn:
    """Flag ON, a seasonal mode exists but NOT at the requested lead ->
    candidate schedules empty -> warn + empty (covers the horizon_value branch).
    """

    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["seasonal_april"])
        _write_mode_config(config_dir, "seasonal_april", lead=0, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_warns_returns_empty_for_absent_lead(self, caplog):
        rows = [
            _season_row(CODE, "2024-04-01", "2024-04-25", horizon_value=3, q50=111.0),
            _season_row(CODE, "2024-04-01", "2024-04-10", horizon_value=3, q50=999.0),
        ]

        def fake_api(codes, start_year, end_year, horizon_type="season", horizon_value=None):
            return pd.DataFrame(rows)

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api):
                # Requested lead 3, but only seasonal_april (lead 0) is configured.
                result = data_reader.read_seasonal_forecasts([CODE], 2024, 2024, horizon_value=3)

        assert result.empty
        assert _has_empty_schedules_warning(caplog)


# ---------------------------------------------------------------------------
# Site 6 -- read_latest_seasonal_forecasts
# ---------------------------------------------------------------------------


class TestLatestSeasonalNoSeasonModeFlagOn:
    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["quarter"])
        _write_mode_config(config_dir, "quarter", lead=1, issue_day=25)
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

    def test_warns_returns_empty_no_unfiltered_leak(self, caplog):
        rows = [
            _season_row(CODE, "2024-04-01", "2024-04-25", q50=111.0),
            _season_row(CODE, "2024-04-01", "2024-04-10", q50=999.0),
        ]

        def fake_api(codes, start_year, end_year, horizon_type="season", horizon_value=None):
            return pd.DataFrame(rows)

        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api):
                result = data_reader.read_latest_seasonal_forecasts(
                    [CODE], forecast_date=dt.date(2024, 4, 30)
                )

        assert result.empty
        assert 999.0 not in set(result.get("q50", pd.Series(dtype=float)).astype(float))
        assert _has_empty_schedules_warning(caplog)


# ---------------------------------------------------------------------------
# Flag OFF byte-identity: no month mode configured, flag OFF -> schedules
# are NEVER resolved and the rows are returned unfiltered exactly as before
# (the new guard MUST NOT trigger under flag OFF).
# ---------------------------------------------------------------------------


class TestSeasonalEmptySchedulesFlagOffUnchanged:
    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        # No seasonal mode configured -- but flag OFF must ignore this entirely.
        _set_long_term_env(monkeypatch, tmp_path, ["quarter"])
        _write_mode_config(config_dir, "quarter", lead=1, issue_day=25)
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

    def test_flag_off_returns_unfiltered_and_never_resolves_schedules(self):
        rows = [
            _season_row(CODE, "2024-04-01", "2024-04-25", q50=111.0),
            _season_row(CODE, "2024-04-01", "2024-04-10", q50=999.0),
        ]

        def fake_api(codes, start_year, end_year, horizon_type="season", horizon_value=None):
            return pd.DataFrame(rows)

        with (
            patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api),
            patch.object(data_reader, "_operational_schedules_for_horizon_type") as mock_resolve,
        ):
            result = data_reader.read_seasonal_forecasts([CODE], 2024, 2024)

        # Flag OFF must never resolve schedules and must keep the unfiltered rows.
        mock_resolve.assert_not_called()
        assert not result.empty
        assert 999.0 in set(result["q50"].astype(float))


class TestMonthlyEmptySchedulesFlagOffUnchanged:
    @pytest.fixture(autouse=True)
    def _config(self, monkeypatch, tmp_path):
        config_dir = tmp_path / "long_term"
        config_dir.mkdir()
        _set_long_term_env(monkeypatch, tmp_path, ["quarter"])
        _write_mode_config(config_dir, "quarter", lead=1, issue_day=25)
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

    def test_flag_off_returns_unfiltered_and_never_resolves_schedules(self):
        rows = [
            _month_row(CODE, 2024, 6, "2024-05-25", horizon_value=1, q50=111.0),
            _month_row(CODE, 2024, 6, "2024-05-10", horizon_value=1, q50=999.0),
        ]

        def fake_api(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            return pd.DataFrame(rows)

        with (
            patch("src.data_reader._read_long_forecasts_api", side_effect=fake_api) as mock_api,
            patch.object(data_reader, "_operational_schedules_for_horizon_type") as mock_resolve,
        ):
            result = data_reader.read_monthly_forecasts([CODE], 2024, 2024)

        mock_resolve.assert_not_called()
        assert mock_api.call_args.args[1] == 2024  # read window not expanded
        assert not result.empty
        assert 999.0 in set(result["q50"].astype(float))
