"""Tests for PP-064 Chunk A: calendar-window validation for QUARTER rows.

Locks:
- A non-calendar quarter window (a rolling window) is excluded, never
  relabelled, at the single choke point for every direct quarter read
  (``_normalize_combined_forecasts``) and at the writer.
- A December-issued Q1 survives the operational reader
  (``read_latest_quarterly_forecasts``); a back-dated run cannot pick a
  later issue because of it.
- Under flag OFF, a December-issued Q1 of the first requested year is
  read by ``read_quarterly_forecasts``.

Test IDs (A-1..A-10, A-4 dropped as redundant with A-1 flag ON --
IDs kept stable) follow
``doc/plans/issues/high_prio_gi_draft_pp_quarter_calendar_window_validation.md``
Chunk A. Station code ``19999`` (sentinel, never a real station).

Fakes of ``_read_long_forecasts_api`` filter by the requested issue-date
years, ``horizon_value`` AND ``horizon_type`` -- as the real call does --
because a fake that ignores its arguments cannot make these tests fail
on trunk.
"""

import datetime as dt
import json
import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import api_writer, data_reader
from src.aggregation import filter_calendar_quarter_windows
from src.gap_detector import detect_missing_quarterly_ensembles

CODE = "19999"


# ===========================================================================
# Shared fixtures / helpers
# ===========================================================================


@pytest.fixture(autouse=True)
def kghm_quarter_config(monkeypatch, tmp_path):
    """kghm-shaped quarter config: BOTH lead (1) and issue day (25).

    ``tests/test_quarterly_data_reader.py``'s autouse fixture writes the
    lead only, which degrades ``select_operational_issuances`` resolution
    (no ``operational_issue_day``). This file's tests need the full
    schedule, so they carry their own fixture (this also protects the
    A tests against PP-065 P1b, which reuses this file).
    """
    config_dir = tmp_path / "long_term"
    config_dir.mkdir()
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "long_term")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "quarter")
    (config_dir / "quarter.json").write_text(
        json.dumps({"operational_month_lead_time": 1, "operational_issue_day": 25})
    )
    return config_dir


def _quarter_row(valid_from, valid_to, issue_date, *, code=CODE, model="LR_Base", q=100.0):
    """One raw API-shaped QUARTER row, horizon_value pinned to 1 (kghm lead)."""
    return {
        "horizon_type": "quarter",
        "horizon_value": 1,
        "code": code,
        "date": issue_date,
        "model_type": model,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "q": q,
        "q05": q - 10.0,
        "q10": q - 8.0,
        "q25": q - 5.0,
        "q50": q,
        "q75": q + 5.0,
        "q90": q + 8.0,
        "q95": q + 10.0,
        "id": 1,
        "model_type_description": model,
    }


def _quarter_api_fake(rows):
    """Fake ``_read_long_forecasts_api`` that filters like the real API.

    Filters by requested issue-date year range, ``horizon_value`` AND
    ``horizon_type`` (default "month", used by the monthly source of
    both quarter readers -- returning nothing for it isolates these
    tests to the DIRECT quarter source).
    """

    def fake(codes, start_year, end_year, horizon_type="month", horizon_value=None):
        if horizon_type != "quarter":
            return pd.DataFrame()
        wanted_codes = {str(c) for c in codes}
        out = []
        for r in rows:
            if str(r["code"]) not in wanted_codes:
                continue
            issue_year = int(str(r["date"])[:4])
            if not (start_year <= issue_year <= end_year):
                continue
            if horizon_value is not None and int(r["horizon_value"]) != horizon_value:
                continue
            out.append(dict(r))
        return pd.DataFrame(out) if out else pd.DataFrame()

    return fake


def _mock_combined_client(rows):
    """MagicMock SapphirePostprocessingClient returning `rows` verbatim.

    For read_quarterly_combined_forecasts()'s A-3/A-6 tests, which mock
    the client itself (not `_read_long_forecasts_api`) so the real
    `_normalize_combined_forecasts` runs on the raw response.
    """
    client = MagicMock()
    client.readiness_check.return_value = True
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    client.read_long_term_forecasts.return_value = df
    return client


# ===========================================================================
# A-1. Reader: only the calendar-aligned window survives
# ===========================================================================


class TestA1CalendarWindowReader:
    def _rows(self):
        return [
            _quarter_row("2024-04-01", "2024-06-30", "2024-03-25", q=100.0),  # calendar Q2
            _quarter_row("2024-05-01", "2024-07-31", "2024-04-25", q=200.0),  # rolling
            _quarter_row("2024-06-01", "2024-08-31", "2024-05-25", q=300.0),  # rolling
        ]

    def test_read_quarterly_forecasts_flag_on(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        fake = _quarter_api_fake(self._rows())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2024, 2024)

        assert len(result) == 1
        assert int(result["quarter_in_year"].iloc[0]) == 2
        assert float(result["forecasted_discharge"].iloc[0]) == 100.0
        assert str(result["valid_to"].iloc[0])[:10] == "2024-06-30"

    def test_read_quarterly_forecasts_flag_off(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        fake = _quarter_api_fake(self._rows())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2024, 2024)

        assert len(result) == 1
        assert int(result["quarter_in_year"].iloc[0]) == 2
        assert float(result["forecasted_discharge"].iloc[0]) == 100.0
        assert str(result["valid_to"].iloc[0])[:10] == "2024-06-30"
        # Flag OFF: date/horizon_value are not part of the output contract.

    def test_read_latest_quarterly_forecasts_flag_on(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        fake = _quarter_api_fake(self._rows())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2024, 6, 1)
            )

        assert len(result) == 1
        assert int(result["quarter_in_year"].iloc[0]) == 2
        assert float(result["forecasted_discharge"].iloc[0]) == 100.0
        assert str(result["valid_to"].iloc[0])[:10] == "2024-06-30"

    def test_read_latest_quarterly_forecasts_flag_off(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        fake = _quarter_api_fake(self._rows())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2024, 6, 1)
            )

        assert len(result) == 1
        assert int(result["quarter_in_year"].iloc[0]) == 2
        assert float(result["forecasted_discharge"].iloc[0]) == 100.0
        assert str(result["valid_to"].iloc[0])[:10] == "2024-06-30"


# ===========================================================================
# A-2. valid_to enforcement (mutation check: delete the valid_to
# predicate in filter_calendar_quarter_windows -> this must fail)
# ===========================================================================


class TestA2ValidToEnforcement:
    @pytest.mark.parametrize(
        "valid_from,valid_to",
        [
            ("2024-04-01", "2024-07-31"),  # extra month
            ("2024-04-01", "2025-06-30"),  # wrong year
            ("2024-04-01", None),  # null valid_to
        ],
    )
    def test_helper_drops_non_calendar_or_null_valid_to(self, valid_from, valid_to):
        df = pd.DataFrame(
            {
                "code": [CODE],
                "valid_from": [valid_from],
                "valid_to": [valid_to],
            }
        )
        kept, dropped = filter_calendar_quarter_windows(df)
        assert dropped == 1
        assert kept.empty

    @pytest.mark.parametrize(
        "valid_from,valid_to",
        [
            ("2024-04-01", "2024-07-31"),
            ("2024-04-01", "2025-06-30"),
            ("2024-04-01", None),
        ],
    )
    def test_reader_excludes_it(self, monkeypatch, valid_from, valid_to):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [_quarter_row(valid_from, valid_to, "2024-03-25")]
        fake = _quarter_api_fake(rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2024, 2024)
        assert result.empty


# ===========================================================================
# A-3. Malformed input through the readers
# ===========================================================================


class TestA3MalformedInput:
    def test_mixed_date_formats_through_combined_forecasts(self, monkeypatch):
        """Mixed date-only / timestamp valid_from strings raise on trunk's

        bare pd.to_datetime(); the combined path swallows the crash and
        returns empty. Assert the VALID rows are actually returned (no
        exception proves nothing).
        """
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [
            {
                **_quarter_row("2024-04-01", "2024-06-30", "2024-03-25"),
                "valid_from": "2024-04-01",
                "valid_to": "2024-06-30",
            },
            {
                **_quarter_row("2024-07-01", "2024-09-30", "2024-06-25", model="LR_SM"),
                "valid_from": "2024-07-01 00:00:00",
                "valid_to": "2024-09-30 00:00:00",
            },
        ]
        client = _mock_combined_client(rows)
        with (
            patch.object(data_reader, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch.object(data_reader, "SapphirePostprocessingClient", return_value=client),
        ):
            result = data_reader.read_quarterly_combined_forecasts(codes=[CODE])

        assert len(result) == 2
        assert set(result["quarter_in_year"]) == {2, 3}

    def test_unparseable_date_dropped_valid_row_kept(self):
        df = pd.DataFrame(
            {
                "code": [CODE, CODE],
                "valid_from": ["not-a-date", "2024-04-01"],
                "valid_to": ["also-not-a-date", "2024-06-30"],
            }
        )
        kept, dropped = filter_calendar_quarter_windows(df)
        assert dropped == 1
        assert len(kept) == 1
        assert str(kept["valid_from"].iloc[0])[:10] == "2024-04-01"

    def test_all_invalid_batch_returns_empty_no_crash(self):
        df = pd.DataFrame(
            {
                "code": [CODE, CODE],
                "valid_from": ["not-a-date", "2024-05-01"],
                "valid_to": ["also-not-a-date", "2024-07-31"],
            }
        )
        kept, dropped = filter_calendar_quarter_windows(df)
        assert dropped == 2
        assert kept.empty

    def test_missing_valid_to_column_empty_result_and_logged(self, caplog):
        df = pd.DataFrame({"code": [CODE, CODE], "valid_from": ["2024-04-01", "2024-07-01"]})
        with caplog.at_level(logging.INFO, logger="src.data_reader"):
            result = data_reader._normalize_combined_forecasts(df, "quarter")
        assert result.empty
        assert "Dropped 2" in caplog.text


# ===========================================================================
# A-5. December Q1, flag ON
# ===========================================================================


class TestA5DecemberQ1FlagOn:
    def test_december_issued_q1_survives_latest_reader(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        rows = [
            _quarter_row("2027-01-01", "2027-03-31", "2026-12-25", model="LR_Base"),
            _quarter_row("2027-01-01", "2027-03-31", "2026-12-25", model="LR_SM"),
        ]
        fake = _quarter_api_fake(rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2026, 12, 25)
            )

        assert not result.empty
        assert set(result["year"].astype(int)) == {2027}
        assert set(result["quarter_in_year"].astype(int)) == {1}


# ===========================================================================
# A-6. Gap detector through the reader (maintenance ensemble_models set)
# ===========================================================================


class TestA6GapDetectorThroughReader:
    ENSEMBLE_MODELS = {"EM", "Skilled Mean", "Naive Mean"}

    def test_calendar_quarter_with_only_rolling_em_is_a_gap(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [
            _quarter_row("2024-04-01", "2024-06-30", "2024-03-25", model="LR_Base"),
            # EM's only row for this period is rolling -> dropped by
            # Chunk A -> no EM row survives for calendar Q2 2024.
            _quarter_row("2024-05-01", "2024-07-31", "2024-04-25", model="EM"),
        ]
        client = _mock_combined_client(rows)
        with (
            patch.object(data_reader, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch.object(data_reader, "SapphirePostprocessingClient", return_value=client),
        ):
            combined = data_reader.read_quarterly_combined_forecasts(codes=[CODE])

        gaps = detect_missing_quarterly_ensembles(combined, ensemble_models=self.ENSEMBLE_MODELS)
        em_gaps = gaps[gaps["model_short"] == "EM"]
        assert len(em_gaps) == 1
        assert int(em_gaps["year"].iloc[0]) == 2024
        assert int(em_gaps["quarter_in_year"].iloc[0]) == 2

    def test_quarter_covered_only_by_rolling_rows_is_not_a_gap(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [
            # Only rolling rows would naively map to Q2 2024; after
            # Chunk A they are dropped entirely, so Q2 2024 never enters
            # the gap-detector's unit set at all (not "missing").
            _quarter_row("2024-05-01", "2024-07-31", "2024-04-25", model="LR_Base"),
            # A real calendar Q3 2024 with EM present -> no gap there.
            _quarter_row("2024-07-01", "2024-09-30", "2024-06-25", model="LR_Base"),
            _quarter_row("2024-07-01", "2024-09-30", "2024-06-25", model="EM"),
        ]
        client = _mock_combined_client(rows)
        with (
            patch.object(data_reader, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch.object(data_reader, "SapphirePostprocessingClient", return_value=client),
        ):
            combined = data_reader.read_quarterly_combined_forecasts(codes=[CODE])

        gaps = detect_missing_quarterly_ensembles(combined, ensemble_models=self.ENSEMBLE_MODELS)
        assert gaps[(gaps["year"] == 2024) & (gaps["quarter_in_year"] == 2)].empty


# ===========================================================================
# A-7. Writer guard
# ===========================================================================


def _write_quarter_config(tmp_path, monkeypatch, lead=1):
    config_dir = tmp_path / "long_term"
    config_dir.mkdir(exist_ok=True)
    (config_dir / "quarter.json").write_text(json.dumps({"operational_month_lead_time": lead}))
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_config_file_station_selection", "missing.json")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "long_term")
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "quarter")


class TestA7WriterGuard:
    @pytest.fixture(autouse=True)
    def _mock_api(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")
        _write_quarter_config(tmp_path, monkeypatch, lead=1)
        self.mock_client = MagicMock()
        self.mock_client.readiness_check.return_value = True
        self.mock_client.write_long_forecasts.return_value = 1

    def _write(self, data):
        with (
            patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True),
            patch("src.api_writer._get_postprocessing_client", return_value=self.mock_client),
        ):
            return api_writer._write_quarterly_ensemble_to_api(data)

    def test_rolling_window_dropped(self, caplog):
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
                "valid_from": ["2025-05-01"],
                "valid_to": ["2025-07-31"],
            }
        )
        with caplog.at_level(logging.INFO, logger="src.api_writer"):
            result = self._write(data)
        assert result is False
        self.mock_client.write_long_forecasts.assert_not_called()
        assert "Dropped 1" in caplog.text

    def test_calendar_window_disagreeing_with_year_quarter_dropped(self):
        # A genuine calendar window (Q3), but the row's OWN
        # (year, quarter_in_year) say Q2 -> disagreement, dropped.
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
                "valid_from": ["2025-07-01"],
                "valid_to": ["2025-09-30"],
            }
        )
        result = self._write(data)
        assert result is False
        self.mock_client.write_long_forecasts.assert_not_called()

    def test_calendar_valid_from_with_null_valid_to_dropped(self):
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
                "valid_from": ["2025-04-01"],
                "valid_to": [None],
            }
        )
        result = self._write(data)
        assert result is False
        self.mock_client.write_long_forecasts.assert_not_called()

    def test_both_null_keeps_synthesized_window(self):
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
            }
        )
        result = self._write(data)
        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["valid_from"] == "2025-04-01"
        assert records[0]["valid_to"] == "2025-06-30"

    def test_matching_calendar_row_written_unchanged(self):
        """A record identical to today's -- hv and date per flag state.

        Flag OFF (this fixture's default): hv falls back to
        quarter_horizon_value() (1), date falls back to valid_from.
        """
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
                "valid_from": ["2025-04-01"],
                "valid_to": ["2025-06-30"],
            }
        )
        result = self._write(data)
        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["valid_from"] == "2025-04-01"
        assert records[0]["valid_to"] == "2025-06-30"
        assert records[0]["horizon_value"] == 1
        assert records[0]["date"] == "2025-04-01"

    def test_matching_calendar_row_written_unchanged_flag_on(self, monkeypatch):
        """Same positive case under the flag: the row's OWN horizon_value

        and date are used verbatim (not the config fallback), per
        ``api_writer.py:1172-1175, 1199-1204`` (untouched by this fix).
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
                "valid_from": ["2025-04-01"],
                "valid_to": ["2025-06-30"],
                "horizon_value": [3],
                "date": ["2024-10-25"],
            }
        )
        result = self._write(data)
        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["valid_from"] == "2025-04-01"
        assert records[0]["valid_to"] == "2025-06-30"
        assert records[0]["horizon_value"] == 3
        assert records[0]["date"] == "2024-10-25"

    def test_valid_to_extra_month_dropped_even_when_valid_from_matches(self):
        """R3: valid_from alone matching the synthesized window is not

        enough -- valid_to must match too. A rolling window whose start
        happens to be the correct calendar start (2025-04-01) but whose
        end is an extra month (2025-07-31, not 2025-06-30) must still be
        dropped. (Mutation: deleting the writer guard's valid_to
        comparison must make this test fail.)
        """
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
                "valid_from": ["2025-04-01"],
                "valid_to": ["2025-07-31"],
            }
        )
        result = self._write(data)
        assert result is False
        self.mock_client.write_long_forecasts.assert_not_called()

    def test_valid_to_wrong_year_dropped_even_when_valid_from_matches(self):
        """Same as above, but valid_to's YEAR is wrong (2026 instead of

        2025) while valid_from still matches the synthesized start.
        """
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2025],
                "quarter_in_year": [2],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
                "valid_from": ["2025-04-01"],
                "valid_to": ["2026-06-30"],
            }
        )
        result = self._write(data)
        assert result is False
        self.mock_client.write_long_forecasts.assert_not_called()

    def test_one_aggregated_log_line_for_multiple_dropped_rows(self, caplog):
        data = pd.DataFrame(
            {
                "code": [CODE, CODE],
                "year": [2025, 2025],
                "quarter_in_year": [2, 3],
                "model_short": ["Naive Mean", "Naive Mean"],
                "forecasted_discharge": [100.0, 110.0],
                "valid_from": ["2025-05-01", "2025-08-01"],
                "valid_to": ["2025-07-31", "2025-10-31"],
            }
        )
        with caplog.at_level(logging.INFO, logger="src.api_writer"):
            self._write(data)
        drop_lines = [r for r in caplog.records if "non-calendar quarter window" in r.message]
        assert len(drop_lines) == 1
        assert "Dropped 2" in drop_lines[0].message


# ===========================================================================
# A-8. Season rows unaffected
# ===========================================================================


class TestA8SeasonUnchanged:
    def test_season_rows_not_filtered_by_calendar_quarter_rule(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SEASON_START_MONTH", raising=False)
        monkeypatch.delenv("SAPPHIRE_SEASON_END_MONTH", raising=False)
        df = pd.DataFrame(
            {
                "code": [CODE, CODE],
                # Deliberately "rolling"/non-quarter-boundary windows --
                # would be dropped by the quarter guard, must survive
                # for season.
                "valid_from": ["2024-04-15", "2024-05-20"],
                "valid_to": ["2024-09-10", "2024-10-05"],
                "model_type": ["LR_Base", "LR_Base"],
            }
        )
        result = data_reader._normalize_combined_forecasts(df, "season")
        assert len(result) == 2


# ===========================================================================
# A-9. Back-dated run, both flags
# ===========================================================================


class TestA9BackDatedRun:
    def _rows(self):
        return [
            _quarter_row("2026-10-01", "2026-12-31", "2026-09-25", model="LR_Base"),
            _quarter_row("2027-01-01", "2027-03-31", "2026-12-25", model="LR_Base"),
        ]

    def test_flag_off_picks_q4_2026_not_a_future_issue(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        fake = _quarter_api_fake(self._rows())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2026, 9, 25)
            )
        assert set(result["year"].astype(int)) == {2026}
        assert set(result["quarter_in_year"].astype(int)) == {4}

    def test_flag_on_picks_q4_2026_not_a_future_issue(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        fake = _quarter_api_fake(self._rows())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2026, 9, 25)
            )
        assert set(result["year"].astype(int)) == {2026}
        assert set(result["quarter_in_year"].astype(int)) == {4}


# ===========================================================================
# A-10. First-year Q1, flag OFF
# ===========================================================================


class TestA10FirstYearQ1FlagOff:
    def test_december_issued_q1_of_first_year_is_read(self, monkeypatch):
        """Problem 7: a Dec-issued Q1 of the FIRST requested year is read

        even though its issue date's year is start_year - 1 -- the
        start_year - 1 read-window widening's whole purpose.

        NOTE: an earlier version of this test also asserted that a row
        issued 2025-12-25 targeting Q1 2026 was excluded by this same
        call. That assertion relied on a two-sided (start_year, end_year)
        trim that an out-of-loop review found to be a regression: it also
        trimmed a genuine next-year DIRECT row that must survive to win
        over a same-target monthly-derived (Source 1) row in the later
        drop_duplicates(keep="last") combine -- see
        TestRegressionDirectPrecedenceSurvivesLowerBoundWidening below,
        which now owns that scenario. The fix trims only target years
        BELOW start_year (the actual extra rows the widening admits); it
        does not trim target years above end_year. This test now checks
        only the first-year Q1 read, not any next-year exclusion.
        """
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [_quarter_row("2025-01-01", "2025-03-31", "2024-12-25", model="LR_Base")]
        fake = _quarter_api_fake(rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        q1_2025 = result[(result["year"] == 2025) & (result["quarter_in_year"] == 1)]
        assert len(q1_2025) == 1
        assert float(q1_2025["forecasted_discharge"].iloc[0]) == 100.0

    def test_widened_window_still_trims_target_years_below_start_year(self, monkeypatch):
        """The start_year - 1 widening's LOWER bound IS trimmed: a normal

        (non-cross-year) issue within start_year - 1 that targets
        start_year - 1 itself must not leak through the widening into a
        read for [start_year, end_year].
        """
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [_quarter_row("2024-04-01", "2024-06-30", "2024-03-25", model="LR_Base")]
        fake = _quarter_api_fake(rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        assert result.empty


# ===========================================================================
# Regression (out-of-loop review of commit 899ae20d): the Problem-7 fix's
# lower-bound widening must not ALSO trim next-year DIRECT rows that trunk
# kept. Maintenance calling read_quarterly_forecasts(codes, Y, Y) to fill
# only year-Y gaps must still let a Dec-issued direct row targeting Q1 of
# Y+1 win over a same-target monthly-derived (Source 1) row -- exactly as
# on trunk, where drop_duplicates(keep="last") prefers direct because
# concat puts it last.
# ===========================================================================


def _quarter_and_month_api_fake(quarter_rows, monthly_rows):
    """Fake `_read_long_forecasts_api` serving BOTH sources, filtering by

    requested issue-date years, `horizon_value` AND `horizon_type` (the
    monthly source uses the default horizon_type="month").
    """

    def fake(codes, start_year, end_year, horizon_type="month", horizon_value=None):
        wanted_codes = {str(c) for c in codes}
        src = quarter_rows if horizon_type == "quarter" else monthly_rows
        out = []
        for r in src:
            if str(r["code"]) not in wanted_codes:
                continue
            issue_year = int(str(r["date"])[:4])
            if not (start_year <= issue_year <= end_year):
                continue
            if horizon_value is not None and int(r.get("horizon_value", -1)) != horizon_value:
                continue
            out.append(dict(r))
        return pd.DataFrame(out) if out else pd.DataFrame()

    return fake


class TestRegressionDirectPrecedenceSurvivesLowerBoundWidening:
    def _monthly_rows(self):
        # Two months (Jan, Feb 2026) per model -> QUARTER_MIN_MONTHS (2)
        # satisfied -> aggregate_monthly_fc_to_quarterly synthesizes a
        # Q1 2026 row per model, issued within 2025 so it survives the
        # (unwidened) monthly read window [2025, 2025].
        rows = []
        for model, value in (("LR_Base", 200.0), ("LR_SM", 220.0)):
            for month in (1, 2):
                rows.append(
                    {
                        "code": CODE,
                        "date": "2025-11-25",
                        "model_type": model,
                        "valid_from": f"2026-{month:02d}-01",
                        "valid_to": f"2026-{month:02d}-28",
                        "forecasted_discharge": value,
                        "q50": value,
                        "horizon_value": 1,
                    }
                )
        return rows

    def _direct_rows(self):
        return [
            _quarter_row("2026-01-01", "2026-03-31", "2025-12-25", model="LR_Base", q=100.0),
            _quarter_row("2026-01-01", "2026-03-31", "2025-12-25", model="LR_SM", q=120.0),
        ]

    def test_direct_next_year_q1_wins_over_monthly_derived(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        fake = _quarter_and_month_api_fake(self._direct_rows(), self._monthly_rows())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        q1_2026 = result[(result["year"] == 2026) & (result["quarter_in_year"] == 1)]
        got = dict(zip(q1_2026["model_short"], q1_2026["forecasted_discharge"], strict=False))
        assert got.get("LR_Base") == 100.0
        assert got.get("LR_SM") == 120.0


# ===========================================================================
# Round-2 regression (out-of-loop review of 6a891f7d, pre-existing since
# 899ae20d): the lower-bound-only fix must not drop a BACKFILL direct row
# -- issue year >= start_year, but TARGET year < start_year (e.g. a Q4
# start_year-1 row issued in January of start_year). Trunk's original
# [start_year, end_year] ISSUE-date read had no target-year trim at all,
# so it returned such rows unconditionally; that must still hold here.
# ===========================================================================


class TestRegressionBackfillPrecedenceSurvivesLowerBoundTrim:
    def _direct_rows(self):
        # Issued 2025-01-10 (issue year 2025 == start_year), targeting
        # Q4 2024 (target year 2024 < start_year) -- a backfill.
        return [
            _quarter_row("2024-10-01", "2024-12-31", "2025-01-10", model="LR_Base", q=100.0),
            _quarter_row("2024-10-01", "2024-12-31", "2025-01-10", model="LR_SM", q=120.0),
        ]

    def _monthly_rows(self):
        # Two months (Oct, Nov 2024) per model, also issued 2025-01-10
        # -> aggregate_monthly_fc_to_quarterly synthesizes a competing
        # Q4 2024 row per model.
        rows = []
        for model, value in (("LR_Base", 200.0), ("LR_SM", 220.0)):
            for month in (10, 11):
                rows.append(
                    {
                        "code": CODE,
                        "date": "2025-01-10",
                        "model_type": model,
                        "valid_from": f"2024-{month:02d}-01",
                        "valid_to": f"2024-{month:02d}-28",
                        "forecasted_discharge": value,
                        "q50": value,
                        "horizon_value": 1,
                    }
                )
        return rows

    def test_direct_prior_year_backfill_wins_over_monthly_derived(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        fake = _quarter_and_month_api_fake(self._direct_rows(), self._monthly_rows())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        q4_2024 = result[(result["year"] == 2024) & (result["quarter_in_year"] == 4)]
        got = dict(zip(q4_2024["model_short"], q4_2024["forecasted_discharge"], strict=False))
        assert got.get("LR_Base") == 100.0
        assert got.get("LR_SM") == 120.0

    def test_direct_prior_year_backfill_returned_without_monthly_source(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        fake = _quarter_and_month_api_fake(self._direct_rows(), [])
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        q4_2024 = result[(result["year"] == 2024) & (result["quarter_in_year"] == 4)]
        got = dict(zip(q4_2024["model_short"], q4_2024["forecasted_discharge"], strict=False))
        assert got.get("LR_Base") == 100.0
        assert got.get("LR_SM") == 120.0


# ===========================================================================
# Mutation-check gap (round-3 review of a1da0fb5's drop_mask): a row whose
# issue `date` is null/unparseable must be kept regardless of target year,
# because trunk's API-side year filter could not have excluded it by year
# either. This is NOT covered by the two regression classes above (both
# use well-formed issue dates), so a mutation that treats a null issue
# year as droppable passed the suite unnoticed.
# ===========================================================================


class TestUnparseableIssueDateKeptRegardlessOfTargetYear:
    def test_unparseable_date_backfill_row_is_kept(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        # Calendar Q2 2024 (target year < start_year=2025), issue date
        # unparseable -- the fake returns it unconditionally, as if the
        # real API had served it regardless of our requested year range.
        row = _quarter_row("2024-04-01", "2024-06-30", "not-a-date", model="LR_Base", q=100.0)

        def fake(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type != "quarter":
                return pd.DataFrame()
            return pd.DataFrame([row])

        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        q2_2024 = result[(result["year"] == 2024) & (result["quarter_in_year"] == 2)]
        assert len(q2_2024) == 1
        assert float(q2_2024["forecasted_discharge"].iloc[0]) == 100.0


# ===========================================================================
# F1 (round-3 out-of-loop review of 0e9ef06f): the drop_mask was too
# permissive. It dropped an out-of-window direct row only when its OWN
# target year was also below start_year, so an out-of-window row
# targeting some OTHER calendar quarter of start_year (not Q1) survived
# and could beat a same-target monthly-derived row, or an in-window
# direct row, depending on API order. Invariant: the flag-OFF direct set
# = trunk's set (issue year in [start_year, end_year], any target year)
# + ONLY the December-issued Q1 of start_year.
# ===========================================================================


class TestRegressionIssueYearMaskTooPermissive:
    def _monthly_q2_2025(self):
        rows = []
        for month in (4, 5):
            rows.append(
                {
                    "code": CODE,
                    "date": "2025-03-25",
                    "model_type": "LR_Base",
                    "valid_from": f"2025-{month:02d}-01",
                    "valid_to": f"2025-{month:02d}-28",
                    "forecasted_discharge": 200.0,
                    "q50": 200.0,
                    "horizon_value": 1,
                }
            )
        return rows

    def test_stale_out_of_window_row_does_not_beat_monthly_derived(self, monkeypatch):
        """A direct Q2 2025 row issued 2024-12-25 (out-of-window, and NOT

        the Dec-Q1-of-start_year case) must be dropped -- trunk never read
        it at all -- so the monthly-derived Q2 2025 value wins.
        """
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        direct_rows = [
            _quarter_row("2025-04-01", "2025-06-30", "2024-12-25", model="LR_Base", q=100.0)
        ]
        fake = _quarter_and_month_api_fake(direct_rows, self._monthly_q2_2025())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        q2_2025 = result[(result["year"] == 2025) & (result["quarter_in_year"] == 2)]
        assert len(q2_2025) == 1
        assert float(q2_2025["forecasted_discharge"].iloc[0]) == 200.0

    def test_stale_row_does_not_clobber_in_window_direct_row_regardless_of_api_order(
        self, monkeypatch
    ):
        """Same as above, but an in-window direct Q2 2025 row (value 300,

        issued 2025-05-25) is ALSO present, placed FIRST in API order. The
        stale out-of-window row (100, issued 2024-12-25) must still be
        dropped, so drop_duplicates(keep="last") never sees it and the
        in-window direct value wins over the monthly-derived one too --
        exactly as on trunk, which never read the stale row regardless of
        API order.
        """
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        direct_rows = [
            _quarter_row("2025-04-01", "2025-06-30", "2025-05-25", model="LR_Base", q=300.0),
            _quarter_row("2025-04-01", "2025-06-30", "2024-12-25", model="LR_Base", q=100.0),
        ]
        fake = _quarter_and_month_api_fake(direct_rows, self._monthly_q2_2025())
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        q2_2025 = result[(result["year"] == 2025) & (result["quarter_in_year"] == 2)]
        assert len(q2_2025) == 1
        assert float(q2_2025["forecasted_discharge"].iloc[0]) == 300.0


# ===========================================================================
# F2 (round-3 out-of-loop review of 0e9ef06f): a `date` column mixing
# tz-aware and tz-naive strings makes `pd.to_datetime(..., format="mixed")`
# return an object-dtype Series, so a subsequent `.dt` access raises
# AttributeError -- aborting the whole quarterly read where trunk's plain
# string comparison never would have. `_issue_date_local_calendar_date`
# fixes this by parsing only the first 10 characters (the calendar date).
# ===========================================================================


class TestRegressionMixedTimezoneIssueDate:
    def test_read_quarterly_forecasts_flag_off_no_exception(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [
            _quarter_row("2025-01-01", "2025-03-31", "2025-01-10", model="LR_Base", q=100.0),
            _quarter_row(
                "2025-04-01",
                "2025-06-30",
                "2025-03-25T00:00:00+06:00",
                model="LR_Base",
                q=200.0,
            ),
        ]
        fake = _quarter_api_fake(rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        q1_2025 = result[(result["year"] == 2025) & (result["quarter_in_year"] == 1)]
        q2_2025 = result[(result["year"] == 2025) & (result["quarter_in_year"] == 2)]
        assert float(q1_2025["forecasted_discharge"].iloc[0]) == 100.0
        assert float(q2_2025["forecasted_discharge"].iloc[0]) == 200.0

    def test_read_latest_quarterly_forecasts_flag_off_no_exception(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [
            _quarter_row("2025-01-01", "2025-03-31", "2025-01-10", model="LR_Base", q=100.0),
            _quarter_row(
                "2025-04-01",
                "2025-06-30",
                "2025-03-25T00:00:00+06:00",
                model="LR_Base",
                q=200.0,
            ),
        ]
        fake = _quarter_api_fake(rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2025, 6, 1)
            )

        assert set(result["year"].astype(int)) == {2025}
        assert set(result["quarter_in_year"].astype(int)) == {2}
        assert float(result["forecasted_discharge"].iloc[0]) == 200.0

    def test_read_latest_quarterly_forecasts_flag_on_no_exception(self, monkeypatch):
        """The mixed-tz `date` column must survive the Problem-6 bound

        (this fix's target) without raising. The second row's issue date
        is deliberately AFTER forecast_date, so the bound drops it before
        `select_operational_issuances` runs -- that function has its own,
        separate, out-of-scope `pd.to_datetime(date_col)` (no
        format="mixed") which would raise on a column that still mixed
        tz-aware and naive strings; this test isolates the bound's fix
        from that unrelated pre-existing parse.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        rows = [
            # Dec-issued Q1 2025, tz-aware issue date -- before
            # forecast_date, survives the bound.
            _quarter_row(
                "2025-01-01",
                "2025-03-31",
                "2024-12-25T00:00:00+06:00",
                model="LR_Base",
                q=100.0,
            ),
            # Naive issue date, but AFTER forecast_date -- dropped by the
            # bound, so it never reaches select_operational_issuances.
            _quarter_row("2025-10-01", "2025-12-31", "2025-08-25", model="LR_Base", q=200.0),
        ]
        fake = _quarter_api_fake(rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2025, 6, 1)
            )

        assert set(result["year"].astype(int)) == {2025}
        assert set(result["quarter_in_year"].astype(int)) == {1}
        assert float(result["forecasted_discharge"].iloc[0]) == 100.0


# ===========================================================================
# R1 (final independent review of 275826de): filter_calendar_quarter_windows
# parses valid_from/valid_to with format="mixed". A valid_to column mixing
# tz-aware and tz-naive strings across rows makes that parse return an
# object-dtype Series, so .dt.normalize() raises AttributeError; the
# combined reader's try/except then swallows it and returns ZERO rows
# where trunk returned both. _local_calendar_date fixes this the same way
# data_reader's _issue_date_local_calendar_date does.
# ===========================================================================


class TestRegressionMixedTimezoneValidTo:
    def test_read_quarterly_combined_forecasts_no_exception_both_rows_returned(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [
            {
                **_quarter_row("2024-04-01", "2024-06-30", "2024-03-25", model="LR_Base", q=100.0),
            },
            {
                **_quarter_row(
                    "2024-07-01",
                    "2024-09-30T00:00:00+06:00",
                    "2024-06-25",
                    model="LR_SM",
                    q=200.0,
                ),
            },
        ]
        client = _mock_combined_client(rows)
        with (
            patch.object(data_reader, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch.object(data_reader, "SapphirePostprocessingClient", return_value=client),
        ):
            result = data_reader.read_quarterly_combined_forecasts(codes=[CODE])

        assert len(result) == 2
        assert set(result["quarter_in_year"]) == {2, 3}


# ===========================================================================
# R2 (final independent review of 275826de): _read_long_forecasts_api drops
# all-null columns (dropna(axis=1, how="all")). If every row's valid_from
# is null (valid_to present), that drops the valid_from column entirely,
# and _normalize_combined_forecasts then dereferences the missing column
# -> KeyError, aborting read_quarterly_forecasts (which calls it with no
# try/except, unlike _read_long_combined_forecasts_api).
# ===========================================================================


class TestRegressionAllNullValidFromColumnDropped:
    def test_read_quarterly_forecasts_no_exception_empty_result(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

        def client_side_effect(**kwargs):
            if kwargs.get("horizon_type") != "quarter":
                return pd.DataFrame()
            return pd.DataFrame(
                [
                    {
                        "horizon_type": "quarter",
                        "horizon_value": 1,
                        "code": CODE,
                        "date": "2025-03-25",
                        "model_type": "LR_Base",
                        # valid_from is null for every row of this batch,
                        # so the real API client's dropna(axis=1,
                        # how="all") drops the column entirely below.
                        "valid_from": None,
                        "valid_to": "2025-06-30",
                        "q50": 100.0,
                    }
                ]
            )

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.read_long_term_forecasts.side_effect = client_side_effect

        with (
            patch.object(data_reader, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch.object(data_reader, "SapphirePostprocessingClient", return_value=mock_client),
        ):
            result = data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        assert result.empty

    def test_normalize_combined_forecasts_no_valid_from_column_returns_empty(self):
        """Unit-level fallback per the review note: a frame with no

        valid_from column at all (only valid_to) must not raise.
        """
        df = pd.DataFrame({"code": [CODE], "valid_to": ["2025-06-30"]})
        result = data_reader._normalize_combined_forecasts(df, "quarter")
        assert result.empty


# ===========================================================================
# R5 (final independent review of 275826de): observability. Aggregated
# INFO counts (no station codes) for rows dropped by the flag-OFF
# issue-year mask and by the Problem-6 issue-date bound; a WARNING when
# the flag-OFF mask cannot run because 'quarter_in_year' or 'date' is
# absent. Deferred (per the reviewer, recorded here not fixed): the
# monthly-derived Source 1 in read_latest_quarterly_forecasts has no
# issue-date bound (PP-065).
# ===========================================================================


class TestR5Observability:
    def test_read_quarterly_forecasts_logs_dropped_issue_year_count(self, monkeypatch, caplog):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        direct_rows = [
            _quarter_row("2025-04-01", "2025-06-30", "2024-12-25", model="LR_Base", q=100.0)
        ]
        monthly_rows = []
        for month in (4, 5):
            monthly_rows.append(
                {
                    "code": CODE,
                    "date": "2025-03-25",
                    "model_type": "LR_Base",
                    "valid_from": f"2025-{month:02d}-01",
                    "valid_to": f"2025-{month:02d}-28",
                    "forecasted_discharge": 200.0,
                    "q50": 200.0,
                    "horizon_value": 1,
                }
            )
        fake = _quarter_and_month_api_fake(direct_rows, monthly_rows)
        with (
            caplog.at_level(logging.INFO, logger="src.data_reader"),
            patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake),
        ):
            data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        drop_lines = [
            r for r in caplog.records if "issued before the requested year range" in r.message
        ]
        assert len(drop_lines) == 1
        assert "Dropped 1" in drop_lines[0].message
        assert CODE not in drop_lines[0].message

    def test_read_quarterly_forecasts_warns_when_mask_columns_missing(self, monkeypatch, caplog):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        row = {
            "horizon_type": "quarter",
            "horizon_value": 1,
            "code": CODE,
            "model_type": "LR_Base",
            "valid_from": "2025-04-01",
            "valid_to": "2025-06-30",
            "q50": 100.0,
            # no "date" column at all.
        }

        def fake(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type != "quarter":
                return pd.DataFrame()
            return pd.DataFrame([row])

        with (
            caplog.at_level(logging.WARNING, logger="src.data_reader"),
            patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake),
        ):
            data_reader.read_quarterly_forecasts([CODE], 2025, 2025)

        warn_lines = [r for r in caplog.records if "filter skipped" in r.message]
        assert len(warn_lines) == 1
        assert "date" in warn_lines[0].message

    def test_read_latest_quarterly_forecasts_logs_dropped_future_issue_count(
        self, monkeypatch, caplog
    ):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [
            _quarter_row("2026-10-01", "2026-12-31", "2026-09-25", model="LR_Base", q=100.0),
            _quarter_row("2027-01-01", "2027-03-31", "2026-12-25", model="LR_Base", q=200.0),
        ]
        fake = _quarter_api_fake(rows)
        with (
            caplog.at_level(logging.INFO, logger="src.data_reader"),
            patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake),
        ):
            data_reader.read_latest_quarterly_forecasts([CODE], forecast_date=dt.date(2026, 9, 25))

        drop_lines = [r for r in caplog.records if "issued after forecast_date" in r.message]
        assert len(drop_lines) == 1
        assert "Dropped 1" in drop_lines[0].message
        assert CODE not in drop_lines[0].message
