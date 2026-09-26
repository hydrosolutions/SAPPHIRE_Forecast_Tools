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

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src import aggregation, api_writer, data_reader
from src.aggregation import (
    _parse_local_calendar_date,
    filter_calendar_quarter_windows,
    local_calendar_date,
)
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
        quarter_horizon_value() (1), date falls back to valid_from --
        NOT the row's own horizon_value/date, which are deliberately
        supplied here (3 / 2024-10-25) so this test can actually tell
        the config fallback apart from a bug that honours them despite
        the flag (a row supplying neither could not distinguish the
        two: both would just read as the fallback values). (Mutation:
        removing the flag gates in api_writer.py:1172-1175, 1199-1204
        must make this test fail.)
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
                "horizon_value": [3],
                "date": ["2024-10-25"],
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

    @pytest.mark.parametrize(
        "valid_from",
        [
            "2024-05-01",
            "2024-01-01",
        ],
    )
    def test_mismatching_valid_from_dropped_even_when_valid_to_matches(self, valid_from):
        """S4: the valid_from predicate is not locked by an existing

        test -- deleting `or parsed_valid_from != pd.Timestamp(valid_from)`
        from the writer's mismatch check keeps the suite green, because
        every other case that reaches this row also has a mismatching
        valid_to. Here valid_to matches the synthesized Q2 2024 end
        (2024-06-30) while valid_from does not, so only the valid_from
        clause can catch it.
        """
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2024],
                "quarter_in_year": [2],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
                "valid_from": [valid_from],
                "valid_to": ["2024-06-30"],
            }
        )
        result = self._write(data)
        assert result is False
        self.mock_client.write_long_forecasts.assert_not_called()

    @pytest.mark.parametrize(
        "valid_to",
        [
            "2024-06-30T99:00:00",
            "2024-06-30garbage",
        ],
    )
    def test_unparseable_valid_to_skipped(self, valid_to):
        """Q2: the guard parses the row's own values with

        local_calendar_date, not a str(...)[:10] prefix compare, which
        accepted a garbage time-of-day/suffix like this (its first 10
        chars happen to match the synthesized date) and would have
        written it anyway. (Mutation: restoring the [:10] prefix
        compare makes this test fail.)
        """
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2024],
                "quarter_in_year": [2],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
                "valid_from": ["2024-04-01"],
                "valid_to": [valid_to],
            }
        )
        result = self._write(data)
        assert result is False
        self.mock_client.write_long_forecasts.assert_not_called()

    @pytest.mark.parametrize(
        "valid_from,valid_to",
        [
            ("2024/04/01", "2024/06/30"),
            ("20240401", "20240630"),
        ],
    )
    def test_differently_formatted_same_date_accepted_and_serialized_as_iso(
        self, valid_from, valid_to
    ):
        """Q2: a same-date value in a different format (accepted by the

        reader's own calendar-window check, src.aggregation's
        local_calendar_date) must also be accepted here -- and written
        as the synthesized ISO date, not the row's own differently
        formatted string (which would otherwise be written literally
        and malform the API payload).
        """
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2024],
                "quarter_in_year": [2],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
                "valid_from": [valid_from],
                "valid_to": [valid_to],
            }
        )
        result = self._write(data)
        assert result is True
        records = self.mock_client.write_long_forecasts.call_args[0][0]
        assert records[0]["valid_from"] == "2024-04-01"
        assert records[0]["valid_to"] == "2024-06-30"

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

    def test_year_2262_q1_rejected_even_with_both_null(self):
        """S2: the writer must match the reader's conservative

        year > 2261 cutoff (filter_calendar_quarter_windows) even for
        the "both null, keep synthesized" case -- Q1 2262 alone
        (Jan-Mar) is a safely-representable window, but the reader
        rejects ANY row with valid_from.year > 2261 regardless of
        quarter, so the writer must too, for consistency. Out-of-range
        data is nonsense; the only point is agreement between the two.
        """
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2262],
                "quarter_in_year": [1],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
            }
        )
        result = self._write(data)
        assert result is False
        self.mock_client.write_long_forecasts.assert_not_called()

    def test_year_2262_q1_rejected_with_matching_own_values(self):
        """Same as above, but the row's own valid_from/valid_to are

        present and self-consistent (2262-01-01..2262-03-31) -- still
        rejected, since the target year alone already disqualifies it.
        """
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [2262],
                "quarter_in_year": [1],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
                "valid_from": ["2262-01-01"],
                "valid_to": ["2262-03-31"],
            }
        )
        result = self._write(data)
        assert result is False
        self.mock_client.write_long_forecasts.assert_not_called()

    def test_year_1677_q3_rejected_even_with_both_null(self):
        """T3: the writer must also match the reader's LOWER-bound

        cutoff (local_calendar_date rejects anything before
        1677-09-22). Year 1677 Q3 synthesizes valid_from "1677-07-01",
        below the cutoff, so it must be skipped even with both null
        (the case that otherwise always keeps the synthesized window).
        """
        data = pd.DataFrame(
            {
                "code": [CODE],
                "year": [1677],
                "quarter_in_year": [3],
                "model_short": ["Naive Mean"],
                "forecasted_discharge": [100.0],
            }
        )
        result = self._write(data)
        assert result is False
        self.mock_client.write_long_forecasts.assert_not_called()


class TestS2ReaderWriterYear2262Agreement:
    def test_reader_rejects_year_2262_q1(self):
        df = pd.DataFrame(
            {
                "code": [CODE],
                "valid_from": ["2262-01-01"],
                "valid_to": ["2262-03-31"],
            }
        )
        kept, dropped = filter_calendar_quarter_windows(df)
        assert dropped == 1
        assert kept.empty


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
# string comparison never would have. `local_calendar_date` fixes this by
# parsing each value's LOCAL calendar date (tz dropped).
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
# where trunk returned both. local_calendar_date (shared with
# data_reader, which imports it from here) fixes this by parsing each
# value's LOCAL calendar date (tz dropped).
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

    def test_read_quarterly_forecasts_direct_reader_no_exception(self, monkeypatch):
        """P6: guard for the R1 fix through read_quarterly_forecasts

        directly (not just the combined reader). Passes on 1259efa4
        (the guard, not a base-failing regression test); verified to
        FAIL when local_calendar_date is swapped back to the old
        format="mixed" parse.
        """
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [
            _quarter_row("2024-04-01", "2024-06-30", "2024-03-25", model="LR_Base", q=100.0),
            _quarter_row(
                "2024-07-01",
                "2024-09-30T00:00:00+06:00",
                "2024-06-25",
                model="LR_SM",
                q=200.0,
            ),
        ]
        fake = _quarter_api_fake(rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2024, 2024)

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

    def test_empty_frame_columns_come_from_input_not_a_hard_coded_list(self):
        """P3: the early-return empty frame's columns come from the

        INPUT's own columns plus year/quarter_in_year -- not a
        hard-coded list, which could differ from the pre-fix empty
        frame (it would have silently dropped horizon_value, flag, and
        q-columns that were never in that hard-coded list).
        """
        df = pd.DataFrame(
            {
                "code": [CODE],
                "horizon_value": [1],
                "flag": [0],
                "q05": [10.0],
                "valid_to": ["2025-06-30"],
                # no "valid_from" column at all.
            }
        )
        result = data_reader._normalize_combined_forecasts(df, "quarter")
        assert result.empty
        assert set(result.columns) == {
            "code",
            "horizon_value",
            "flag",
            "q05",
            "valid_to",
            "year",
            "quarter_in_year",
        }

    def test_neither_valid_from_nor_valid_to_present_non_empty_warns(self, caplog):
        """P4: a non-empty frame with NEITHER valid_from nor valid_to

        must log a WARNING with the row count (no station codes) before
        every row is discarded, instead of vanishing silently.
        """
        df = pd.DataFrame({"code": [CODE, CODE], "model_type": ["LR_Base", "LR_SM"]})
        with caplog.at_level(logging.WARNING, logger="src.data_reader"):
            result = data_reader._normalize_combined_forecasts(df, "quarter")
        assert result.empty
        warn_lines = [r for r in caplog.records if "neither valid_from" in r.message]
        assert len(warn_lines) == 1
        assert "Dropped 2" in warn_lines[0].message
        assert CODE not in warn_lines[0].message


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

        drop_lines = [r for r in caplog.records if "dated after forecast_date" in r.message]
        assert len(drop_lines) == 1
        assert "Dropped 1" in drop_lines[0].message
        assert CODE not in drop_lines[0].message


# ===========================================================================
# P1 (final independent review of 1259efa4): the str(...)[:10] +
# format="%Y-%m-%d" helper changed which values parse relative to
# format="mixed", in BOTH directions -- it silently REJECTED values
# format="mixed" always accepted (e.g. "2024/04/01", "20240401", a
# leading space) and silently ACCEPTED values format="mixed" always
# rejected (e.g. "2024-04-01T99:00:00", "2024-04-01garbage"). Replaced
# with one element-wise pd.Timestamp-based helper in aggregation.py,
# imported by data_reader (no cycle: data_reader already imports FROM
# aggregation). The data_reader twin is deleted.
# ===========================================================================


class TestP1LocalCalendarDateParsing:
    @pytest.mark.parametrize(
        "value",
        [
            "2024-04-01",
            "2024-04-01T00:00:00",
            "2024-04-01 00:00:00",
            "2024/04/01",
            "20240401",
            "2024-4-1",
            pd.Timestamp("2024-04-01"),
            dt.date(2024, 4, 1),
        ],
    )
    def test_accepted_values_parse_to_2024_04_01(self, value):
        result = local_calendar_date(pd.Series([value])).iloc[0]
        assert result == pd.Timestamp("2024-04-01")

    @pytest.mark.parametrize(
        "value",
        [
            pd.Timestamp("2024-04-01T00:00:00+06:00"),
            "2024-04-01T00:00:00+06:00",
        ],
    )
    def test_tz_aware_values_keep_local_wall_clock_date(self, value):
        # LOCAL calendar date (2024-04-01), NOT converted to UTC.
        result = local_calendar_date(pd.Series([value])).iloc[0]
        assert result == pd.Timestamp("2024-04-01")

    @pytest.mark.parametrize(
        "value",
        [
            "2024-04-01T99:00:00",
            "2024-04-01garbage",
            "garbage",
            None,
            float("nan"),
            pd.NA,
            pd.NaT,
            # Q1: pd.Timestamp accepts dates outside the datetime64[ns]
            # range (~1677-09-21..2262-04-11) at second resolution, but
            # casting to ns overflows -- these must come back NaT, not
            # raise OutOfBoundsDatetime.
            "9999-12-31",
            "0001-04-01",
            "2500-04-01",
            dt.date(3000, 1, 1),
            pd.Timestamp("2500-01-01"),
            np.datetime64("2500-01-01"),
        ],
    )
    def test_rejected_values_are_nat(self, value):
        result = local_calendar_date(pd.Series([value])).iloc[0]
        assert pd.isna(result)

    def test_mixed_timezone_column_does_not_raise(self):
        s = pd.Series(["2024-06-30", "2024-09-30T00:00:00+06:00"])
        result = local_calendar_date(s)
        assert list(result) == [pd.Timestamp("2024-06-30"), pd.Timestamp("2024-09-30")]

    def test_midnight_normalization(self):
        """Q5: a value with a non-midnight time-of-day is normalized to

        midnight. (Mutation: removing .normalize() in
        _parse_local_calendar_date makes this fail.)
        """
        result = local_calendar_date(pd.Series(["2024-04-01T13:45:00"])).iloc[0]
        assert result == pd.Timestamp("2024-04-01 00:00:00")

    def test_deduped_result_matches_non_deduped_on_repetitive_mixed_input(self):
        """Q3: proves the factorize-based dedup broadcast is equivalent

        to parsing every row individually, on a repetitive input mixing
        NaNs and value types (the case the dedup optimizes for).
        """
        from src.aggregation import _parse_local_calendar_date

        distinct = [
            "2024-04-01",
            "2024-04-01T00:00:00+06:00",
            None,
            float("nan"),
            pd.NaT,
            pd.Timestamp("2024-07-01"),
            dt.date(2024, 8, 1),
            "garbage",
            "9999-12-31",
            "2024/04/01",
        ]
        s = pd.Series(distinct * 50).sample(frac=1.0, random_state=0).reset_index(drop=True)

        non_deduped = pd.to_datetime(
            pd.Series([_parse_local_calendar_date(v) for v in s], dtype=object)
        )
        deduped = local_calendar_date(s)

        pd.testing.assert_series_equal(deduped, non_deduped, check_names=False)

    # =======================================================================
    # T1/T2 (confirm review of f0ca2d94): three review rounds each found a
    # NEW way to break a generic "type + string representation" dedup key
    # (a same-instant tz-aware pair at different offsets; 0/False and
    # 1/True; a date subclass whose __str__ is constant; np.datetime64
    # unit-multiplier aliasing; a __str__ that raises and aborts the whole
    # batch). local_calendar_date now de-duplicates ONLY exact `str`
    # values, keyed on the string itself -- correct BY CONSTRUCTION, since
    # two equal strings are the same input to a pure function, not by
    # enumerating collision classes. Everything else (including an
    # unhashable value) is parsed individually, every time, with no key.
    # =======================================================================

    @staticmethod
    def _assert_matches_naive_parse(s):
        naive = pd.to_datetime(pd.Series([_parse_local_calendar_date(v) for v in s], dtype=object))
        result = local_calendar_date(s)
        pd.testing.assert_series_equal(result, naive, check_names=False)

    def test_collision_pairs_both_orders_no_list(self):
        """Every known collision class the raw-value/generic-key designs

        broke on, each pair included in BOTH orders, WITHOUT an
        unhashable value mixed in (kept separate below) so no fallback
        path can mask a genuine collision. Fails on f0ca2d94 for the
        pairs it still mis-handles.
        """
        t1 = pd.Timestamp("2024-04-01 00:00", tz="+06:00")
        t2 = pd.Timestamp("2024-03-31 18:00", tz="UTC")
        assert t1 == t2  # same instant, different offset -- the setup

        d1 = np.datetime64("2024-04-02", "D")
        d2 = d1.astype("datetime64[2D]")  # unit-multiplier aliasing

        pairs = [
            (t1, t2),
            (0, False),
            (1, True),
            ("20240401", 20240401),
            (d1, d2),
        ]
        for a, b in pairs:
            for ordered in ([a, b, b, a], [b, a, a, b]):
                self._assert_matches_naive_parse(pd.Series(ordered, dtype=object))

    def test_unhashable_list_value_kept_separate_from_collision_pairs(self):
        s = pd.Series(["2024-04-01", [1, 2, 3], "2024-04-01"], dtype=object)
        self._assert_matches_naive_parse(s)

    def test_date_subclass_with_constant_str_not_confused(self):
        """A date subclass is never `type(v) is str`, so its (here

        deliberately useless) __str__ can never collide two distinct
        dates into the same cache entry.
        """

        class ConstantStrDate(dt.date):
            def __str__(self):
                return "CONSTANT"

        a, b = ConstantStrDate(2024, 4, 1), ConstantStrDate(2024, 5, 1)
        s = pd.Series([a, b, b, a], dtype=object)
        self._assert_matches_naive_parse(s)

    def test_object_with_raising_str_returns_nat_not_crash(self):
        """_parse_local_calendar_date's except must be broad enough to

        catch whatever an arbitrary object's __str__ raises (not just
        ValueError/TypeError/OverflowError). Mutation: narrowing it back
        to that tuple makes this fail, since pd.Timestamp lets the
        RuntimeError from __str__ propagate unwrapped.
        """

        class RaisingStr:
            def __str__(self):
                raise RuntimeError("boom")

        s = pd.Series(["2024-04-01", RaisingStr(), "2024-04-01"], dtype=object)
        self._assert_matches_naive_parse(s)
        result = local_calendar_date(s)
        assert pd.isna(result.iloc[1])

    def test_random_shuffled_property_object_category_string_dtypes(self):
        """Differential property test, fixed seed: local_calendar_date

        must equal the naive per-row parse for a randomized, repetitive
        mix, across object/category/string dtype.
        """
        rng = np.random.default_rng(20260926)
        pool = [
            "2024-04-01",
            "2024-05-01",
            "2024-04-01T00:00:00+06:00",
            "20240401",
            "garbage",
            None,
        ]
        values = [pool[i] for i in rng.integers(0, len(pool), size=300)]
        for as_dtype in (None, "category", "string"):
            s = pd.Series(values, dtype=object)
            if as_dtype:
                s = s.astype(as_dtype)
            self._assert_matches_naive_parse(s)

    def test_random_shuffled_property_datetime64_variants(self):
        """Differential property test, fixed seed, across datetime64

        dtype variants: naive ns, naive seconds (non-ns unit -> path
        (c)), and tz-aware -- including values near both the lower and
        upper datetime64[ns] bounds. Verifies path (a) against path (c)
        (a per-value re-implementation) on random data, as specified.
        """
        rng = np.random.default_rng(20260926)
        pool = pd.DatetimeIndex(
            [
                pd.Timestamp("2024-04-01T13:45:00"),
                pd.Timestamp("2024-05-01T00:00:00"),
                pd.Timestamp("1677-09-21T05:00:00"),  # below the cutoff
                pd.Timestamp("1677-09-22T00:00:01"),  # just above it
                pd.Timestamp("2262-04-01T00:00:00"),  # near the upper limit
            ]
        )
        idx = rng.integers(0, len(pool), size=300)

        s_ns = pd.Series(pool.take(idx).values)
        self._assert_matches_naive_parse(s_ns)

        s_seconds = pd.Series(pool.astype("datetime64[s]").take(idx).values)
        self._assert_matches_naive_parse(s_seconds)

        s_tz = pd.Series(pool.tz_localize("Asia/Bishkek").take(idx))
        self._assert_matches_naive_parse(s_tz)

    def test_vectorized_datetime64_path_rejects_lower_bound_before_normalize(self):
        """Targeted, minimal version of the property test above: a

        datetime64[ns] Series containing a value below the cutoff must
        come back NaT, not the bogus wrapped-around date .normalize()
        alone would produce. Mutation (ii): removing the vectorized
        path's `too_low` mask (checked BEFORE .normalize(), not after)
        makes this fail.
        """
        s = pd.Series([pd.Timestamp("2024-04-01"), pd.Timestamp("1677-09-21T05:00:00")])
        assert s.dtype == "datetime64[ns]"
        result = local_calendar_date(s)
        assert result.iloc[0] == pd.Timestamp("2024-04-01")
        assert pd.isna(result.iloc[1])

    def test_parse_once_for_str_series_and_no_calls_for_datetime64(self, monkeypatch):
        """Parse-once contract: on a repetitive PURE-str series, each

        distinct string is parsed exactly once. On a datetime64 series,
        the per-value parser is never called at all -- that path is
        fully vectorized. Mutation (i): dedup keyed on str(v) for every
        value (not gated on type(v) is str) makes the collision-pairs
        test fail instead of this one; this test's own mutation is
        reverting to a plain per-row map, which would make the call
        count equal the row count.
        """
        calls = []
        original = aggregation._parse_local_calendar_date

        def spy(v):
            calls.append(v)
            return original(v)

        monkeypatch.setattr(aggregation, "_parse_local_calendar_date", spy)

        distinct = ["2024-04-01", "2024-05-01", "2024-06-01"]
        s = pd.Series(distinct * 100)
        aggregation.local_calendar_date(s)
        assert len(calls) == len(distinct)

        calls.clear()
        dt_s = pd.to_datetime(pd.Series(["2024-04-01"] * 300))
        aggregation.local_calendar_date(dt_s)
        assert len(calls) == 0

    def test_lower_bound_wrap_rejected(self):
        """S3: a value just above pd.Timestamp.min normalizes DOWN to a

        time-of-day earlier than the representable minimum; pandas does
        not raise for this, it silently wraps around to a bogus date
        near the UPPER limit (observed: 2262-04-11) instead of NaT.
        Tested as a string, a Timestamp, and a np.datetime64.
        """
        from src.aggregation import _parse_local_calendar_date

        assert pd.isna(_parse_local_calendar_date(pd.Timestamp.min))
        assert pd.isna(_parse_local_calendar_date(np.datetime64("1677-09-21T12:00:00", "ns")))
        assert pd.isna(_parse_local_calendar_date("1677-09-21T12:00:00"))
        # Just past the cutoff: parses normally.
        assert _parse_local_calendar_date("1677-09-22T00:00:01") == pd.Timestamp("1677-09-22")

    def test_empty_series_returns_empty_datetime64(self):
        result = local_calendar_date(pd.Series([], dtype=object))
        assert result.empty
        assert result.dtype == "datetime64[ns]"

    def test_empty_tz_aware_series_returns_naive_datetime64(self):
        """Q4: an empty tz-aware input (datetime64[ns, UTC]) must still

        come back naive datetime64[ns] -- not tz-aware, which .kind=="M"
        alone would not catch.
        """
        result = local_calendar_date(pd.Series([], dtype="datetime64[ns, UTC]"))
        assert result.empty
        assert result.dtype == "datetime64[ns]"

    def test_all_null_tz_aware_series_returns_naive_datetime64(self):
        """Contract lock (not a regression test -- this already passed

        on 8c9cfc2a too): a non-empty, entirely-null tz-aware input must
        come back naive datetime64[ns] with NaT rows, not left tz-aware.
        """
        result = local_calendar_date(pd.Series([pd.NaT, pd.NaT], dtype="datetime64[ns, UTC]"))
        assert result.dtype == "datetime64[ns]"
        assert result.isna().all()

    def test_all_null_object_series_returns_naive_datetime64(self):
        """Contract lock (not a regression test -- this already passed

        on 8c9cfc2a too): an all-null object-dtype input must come back
        naive datetime64[ns].
        """
        result = local_calendar_date(pd.Series([None, float("nan"), pd.NaT]))
        assert result.dtype == "datetime64[ns]"
        assert result.isna().all()

    def test_slash_date_accepted_and_garbage_hour_rejected_through_the_filter(self):
        """Fails on 1259efa4: the old helper flips both outcomes -- it

        REJECTS the slash-separated date (index 0) and ACCEPTS the
        garbage-hour valid_to (index 1), the exact opposite of what is
        asserted here.
        """
        df = pd.DataFrame(
            {
                "code": [CODE, CODE],
                "valid_from": ["2024/04/01", "2024-07-01"],
                "valid_to": ["2024/06/30", "2024-09-30T99:00:00"],
            }
        )
        kept, dropped = filter_calendar_quarter_windows(df)
        assert dropped == 1
        assert len(kept) == 1
        assert kept["valid_from"].iloc[0] == pd.Timestamp("2024-04-01")


# ===========================================================================
# Q1 (confirm review of 8c9cfc2a, Important): out-of-range dates crash.
# pd.Timestamp accepts dates outside the datetime64[ns] range at second
# resolution, but the OUTER pd.to_datetime cast (or, separately,
# valid_from + DateOffset(months=3) for a valid_from within ~3 months of
# the ns upper limit) then raises OutOfBoundsDatetime/OverflowError,
# outside the per-value try. Both direct readers abort; the combined
# reader's try/except swallows it and returns empty, losing valid
# companion rows. All of these must fail (raise) on 8c9cfc2a.
# ===========================================================================


class TestRegressionOutOfRangeDatesDoNotCrash:
    def _valid_row(self):
        return _quarter_row("2024-04-01", "2024-06-30", "2024-03-25", model="LR_Base", q=100.0)

    def _all_companions(self):
        return [
            # valid_to outside the datetime64[ns] range.
            _quarter_row("2024-10-01", "9999-12-31", "2024-09-25", model="OOB_VALID_TO"),
            # valid_from outside the range.
            _quarter_row("2500-04-01", "2500-06-30", "2024-03-25", model="OOB_VALID_FROM"),
            # valid_from in range, but + 3 months overflows the ns upper
            # limit (~2262-04-11) even though 2262-02-01 itself parses.
            _quarter_row("2262-02-01", "2262-04-30", "2024-03-25", model="OOB_OFFSET"),
            # valid_from at a quarter-start month, past the safety
            # margin -- excluded conservatively, per spec, without
            # attempting the (safe, in this one case) arithmetic.
            _quarter_row("2262-01-01", "2262-03-31", "2024-03-25", model="NEAR_LIMIT"),
            # issue date outside the range (a genuinely calendar Q3 2024
            # window otherwise).
            _quarter_row("2024-07-01", "2024-09-30", "0001-01-01", model="OOB_ISSUE_DATE"),
        ]

    def _fake_returning_all(self, rows):
        def fake(codes, start_year, end_year, horizon_type="month", horizon_value=None):
            if horizon_type != "quarter":
                return pd.DataFrame()
            return pd.DataFrame(rows)

        return fake

    def test_read_quarterly_forecasts_flag_off_survives(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [self._valid_row(), *self._all_companions()]
        fake = self._fake_returning_all(rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_quarterly_forecasts([CODE], 2024, 2024)

        q2_2024 = result[(result["year"] == 2024) & (result["quarter_in_year"] == 2)]
        assert len(q2_2024) == 1
        assert float(q2_2024["forecasted_discharge"].iloc[0]) == 100.0

    def test_read_latest_quarterly_forecasts_flag_off_survives(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [self._valid_row(), *self._all_companions()]
        fake = self._fake_returning_all(rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2024, 10, 1)
            )

        assert not result.empty
        q2_2024 = result[(result["year"] == 2024) & (result["quarter_in_year"] == 2)]
        assert len(q2_2024) == 1
        assert float(q2_2024["forecasted_discharge"].iloc[0]) == 100.0

    def test_read_latest_quarterly_forecasts_flag_on_survives(self, monkeypatch):
        """OOB_ISSUE_DATE is omitted here: it is a genuine calendar

        window, so (unlike the other companions, which the calendar
        filter excludes before select_operational_issuances ever runs)
        it would reach select_operational_issuances' own unrelated,
        pre-existing, out-of-scope plain pd.to_datetime(date_col) parse,
        which raises on '0001-01-01' regardless of this fix. Covered for
        flag OFF above, where select_operational_issuances is not
        invoked.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        companions = [c for c in self._all_companions() if c["model_type"] != "OOB_ISSUE_DATE"]
        rows = [self._valid_row(), *companions]
        fake = self._fake_returning_all(rows)
        with patch.object(data_reader, "_read_long_forecasts_api", side_effect=fake):
            result = data_reader.read_latest_quarterly_forecasts(
                [CODE], forecast_date=dt.date(2024, 10, 1)
            )

        assert not result.empty
        q2_2024 = result[(result["year"] == 2024) & (result["quarter_in_year"] == 2)]
        assert len(q2_2024) == 1
        assert float(q2_2024["forecasted_discharge"].iloc[0]) == 100.0

    def test_read_quarterly_combined_forecasts_survives(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        rows = [self._valid_row(), *self._all_companions()]
        client = _mock_combined_client(rows)
        with (
            patch.object(data_reader, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}),
            patch.object(data_reader, "SapphirePostprocessingClient", return_value=client),
        ):
            result = data_reader.read_quarterly_combined_forecasts(codes=[CODE])

        q2_2024 = result[(result["year"] == 2024) & (result["quarter_in_year"] == 2)]
        assert len(q2_2024) == 1
        assert float(q2_2024["forecasted_discharge"].iloc[0]) == 100.0
