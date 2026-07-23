"""LOCKED regression tests for PP-045: period-forecast boundary-gap backfill.

TDD tests-first. These lock the contract for a fix that adds a backfill
entrypoint able to heal per-model PENTAD/DECADE period forecasts stranded
when an operational boundary day was missed. The backfill re-aggregates a
date range through the EXISTING operational aggregation + save path, one
year at a time.

The implementation does not exist yet, so tests that touch the new seams
(the ``write_csv`` kwarg on ``save_forecast_data``, the four additive
kwargs on ``_run_short_term_postprocessing``, and the new
``backfill_period_forecasts`` module) are EXPECTED to fail at call/import
time — that is the intended TDD "red" state. The module-level imports that
could break collection (``postprocessing_operational`` and
``backfill_period_forecasts``) are done lazily *inside* the tests so the
file always COLLECTS cleanly; only the not-yet-implemented seams go red.

Contracts locked here (match names precisely):
  1. file_writer.save_forecast_data(config, simulated, write_csv=True)
     — additive kwarg; write_csv=False skips the two atomic_write_csv calls
     but still performs the API write; default True preserves behavior.
  2. postprocessing_operational._run_short_term_postprocessing(
         config, today, errors, timing_stats_,
         start_year=None, end_year=None, dry_run=False, write_csv=True)
     — four additive trailing kwargs; start/end default to today.year;
     dry_run skips save; write_csv forwards to save_forecast_data;
     existing positional call sites keep working.
  3. backfill_period_forecasts.main(argv) -> int (0 ok, non-zero error)
     — CLI over a date range, per (horizon, year) ascending.
"""

import datetime as dt
import logging
import os
import sys
from unittest.mock import patch

import pandas as pd
import pytest

# Path setup mirrors the sibling test modules / conftest.py.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# These imports are known-good at top level (used identically by the
# existing test_data_reader_ml_aggregation.py / test_file_writer.py).
from src import api_writer, file_writer  # noqa: E402
from src.data_reader import _normalize_ml_forecasts  # noqa: E402
from src.postprocessing_tools import TimingStats  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers — lazy imports so a not-yet-existing module never breaks collection.
# ---------------------------------------------------------------------------
def _import_operational():
    """Import the real postprocessing_operational module.

    Kept lazy (not top-level) so its import-time side effects and the
    additive-kwarg contract can't break collection of the whole file.
    """
    import postprocessing_operational as po

    return po


def _import_backfill():
    """Import the not-yet-existing backfill_period_forecasts module.

    Until the implementer creates it, this raises ModuleNotFoundError and
    the calling test errors — the expected TDD red state.
    """
    import backfill_period_forecasts as bf

    return bf


def _raw_missed_boundary_day_frame():
    """Raw whole-slice DAY frame (same shape as T1) for code 19999.

    The Jul 10 2026 pentad boundary carries in-period daily targets
    Jul 11-15 (pentad_in_year 39), one out-of-period target Jul 16
    (pentad 40 -> dropped by _normalize_ml_forecasts), and a non-boundary
    noise issue date Jul 8 (dropped entirely). Feeding this to the REAL
    ``_normalize_ml_forecasts`` yields exactly one healed per-model period
    row (code 19999, TFT, pentad_in_year 39, forecasted_discharge 30.0).
    """
    return pd.DataFrame(
        {
            "code": ["19999"] * 8,
            "date": (["2026-07-10"] * 6 + ["2026-07-08"] * 2),
            "target": [
                "2026-07-11",
                "2026-07-12",
                "2026-07-13",
                "2026-07-14",
                "2026-07-15",  # in pentad 39
                "2026-07-16",  # pentad 40 -> filtered out
                "2026-07-09",
                "2026-07-10",
            ],
            "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 999.0, 777.0, 777.0],
            "q05": [5.0, 10.0, 15.0, 20.0, 25.0, 500.0, 300.0, 300.0],
            "q95": [15.0, 30.0, 45.0, 60.0, 75.0, 1500.0, 900.0, 900.0],
        }
    )


# ===========================================================================
# T1 — Aggregation regression: a "missed" boundary is healed by re-aggregation
# ===========================================================================
class TestAggregationHealsMissedBoundary:
    """Re-running the REAL daily->period aggregation over a range that
    includes a previously-missed boundary produces the per-model period row.
    """

    def test_missed_pentad_boundary_yields_period_row(self):
        # Arrange: a whole-slice DAY frame for one station. The Jul 10 2026
        # boundary (a pentad boundary that could have been missed) carries
        # in-period daily targets Jul 11-15 (pentad_in_year 39), one
        # out-of-period target Jul 16 (pentad 40 -> dropped), and a
        # non-boundary noise issue date Jul 8 (dropped entirely).
        raw = pd.DataFrame(
            {
                "code": ["19999"] * 8,
                "date": (
                    ["2026-07-10"] * 6  # boundary issue day
                    + ["2026-07-08"] * 2  # non-boundary -> dropped
                ),
                "target": [
                    "2026-07-11",
                    "2026-07-12",
                    "2026-07-13",
                    "2026-07-14",
                    "2026-07-15",  # in pentad 39
                    "2026-07-16",  # pentad 40 -> filtered out
                    "2026-07-09",
                    "2026-07-10",
                ],
                "forecasted_discharge": [10.0, 20.0, 30.0, 40.0, 50.0, 999.0, 777.0, 777.0],
                "q05": [5.0, 10.0, 15.0, 20.0, 25.0, 500.0, 300.0, 300.0],
                "q95": [15.0, 30.0, 45.0, 60.0, 75.0, 1500.0, 900.0, 900.0],
            }
        )

        # Act
        result = _normalize_ml_forecasts(raw, "TFT", "pentad")

        # Assert: exactly the healed per-model period row for the boundary day.
        assert len(result) == 1
        assert result["code"].iloc[0] == "19999"
        assert result["model_short"].iloc[0] == "TFT"
        assert "pentad_in_year" in result.columns
        # offset date Jul 11 -> pentad_in_year 39 (string from tag_library).
        assert str(result["pentad_in_year"].iloc[0]) == "39"
        # Mean of the 5 in-period targets: (10+20+30+40+50)/5 = 30.0.
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)
        assert result["q05"].iloc[0] == pytest.approx(15.0)  # mean(5,10,15,20,25)


# ===========================================================================
# T2 — _run_short_term_postprocessing: year-range kwargs + back-compat
# ===========================================================================
class TestRunShortTermYearRange:
    """The operational helper accepts additive start_year/end_year kwargs and
    forwards them to the reader; existing positional call sites still work.
    """

    @staticmethod
    def _patched(po, station_codes=("19999",)):
        """Patch the reader/save/station seams on the operational module."""
        return (
            patch.object(
                po.data_reader,
                "read_observed_and_modelled_data",
                return_value=(pd.DataFrame(), pd.DataFrame()),
            ),
            patch.object(
                po.data_reader,
                "read_skill_metrics",
                return_value=pd.DataFrame(),
            ),
            patch.object(po.file_writer, "save_forecast_data", return_value=None),
            patch.object(po, "_read_station_codes", return_value=list(station_codes)),
        )

    def test_explicit_start_end_year_forwarded_to_reader(self):
        po = _import_operational()
        p_reader, p_skill, p_save, p_codes = self._patched(po)
        with p_reader as m_reader, p_skill, p_save as m_save, p_codes:
            # Act — request a backfill year different from `today`.
            po._run_short_term_postprocessing(
                po.PENTAD,
                dt.date(2026, 7, 20),
                [],
                TimingStats(),
                start_year=2025,
                end_year=2025,
            )

        # Assert — reader received the requested backfill year, save happened.
        assert m_reader.call_count == 1
        kwargs = m_reader.call_args.kwargs
        assert kwargs["start_year"] == 2025
        assert kwargs["end_year"] == 2025
        m_save.assert_called_once()

    def test_default_positional_call_uses_today_year(self):
        """Back-compat: the legacy positional call site (no year kwargs)
        defaults start_year==end_year==today.year."""
        po = _import_operational()
        p_reader, p_skill, p_save, p_codes = self._patched(po)
        with p_reader as m_reader, p_skill, p_save, p_codes:
            # Act — the historical 4-positional-arg call must still work.
            po._run_short_term_postprocessing(
                po.PENTAD,
                dt.date(2026, 7, 20),
                [],
                TimingStats(),
            )

        kwargs = m_reader.call_args.kwargs
        assert kwargs["start_year"] == 2026
        assert kwargs["end_year"] == 2026


# ===========================================================================
# T3 — dry_run skips the save call
# ===========================================================================
class TestRunShortTermDryRun:
    def _run(self, dry_run):
        po = _import_operational()
        with (
            patch.object(
                po.data_reader,
                "read_observed_and_modelled_data",
                return_value=(pd.DataFrame(), pd.DataFrame()),
            ),
            patch.object(po.data_reader, "read_skill_metrics", return_value=pd.DataFrame()),
            patch.object(po.file_writer, "save_forecast_data", return_value=None) as m_save,
            patch.object(po, "_read_station_codes", return_value=["19999"]),
        ):
            po._run_short_term_postprocessing(
                po.PENTAD,
                dt.date(2026, 7, 20),
                [],
                TimingStats(),
                start_year=2025,
                end_year=2025,
                dry_run=dry_run,
            )
            return m_save

    def test_dry_run_true_skips_save(self):
        m_save = self._run(dry_run=True)
        m_save.assert_not_called()

    def test_dry_run_false_calls_save(self):
        m_save = self._run(dry_run=False)
        m_save.assert_called_once()

    def _run_nonempty(self, dry_run, caplog):
        """Run with NON-empty modelled data so the dry-run coverage branch
        actually computes coverage; patches log_most_recent_forecasts so the
        no-filesystem-write contract (F2) can be asserted."""
        po = _import_operational()
        from conftest import PENTAD as PENTAD_CFG

        modelled = pd.DataFrame(
            {
                "code": ["19999", "19999"],
                "date": pd.to_datetime(["2025-07-10", "2025-07-15"]),
                "pentad_in_year": [39, 40],
                "model_short": ["TFT", "TFT"],
                "forecasted_discharge": [30.0, 40.0],
            }
        )
        with (
            patch.object(
                po.data_reader,
                "read_observed_and_modelled_data",
                return_value=(pd.DataFrame(), modelled.copy()),
            ),
            patch.object(po.data_reader, "read_skill_metrics", return_value=pd.DataFrame()),
            patch.object(po.sl, "calculate_virtual_stations_data", side_effect=lambda df: df),
            patch.object(po.file_writer, "save_forecast_data", return_value=None) as m_save,
            patch.object(po.pt, "log_most_recent_forecasts") as m_log,
            patch.object(po, "_read_station_codes", return_value=["19999"]),
        ):
            with caplog.at_level(logging.INFO):
                po._run_short_term_postprocessing(
                    PENTAD_CFG,
                    dt.date(2025, 7, 20),
                    [],
                    TimingStats(),
                    start_year=2025,
                    end_year=2025,
                    dry_run=dry_run,
                )
        return m_save, m_log

    def test_dry_run_true_logs_coverage_and_skips_filesystem(self, caplog):
        """F2 lock: dry_run must emit the coverage log AND must not call
        log_most_recent_forecasts (which would create a dir + write a CSV)."""
        m_save, m_log = self._run_nonempty(dry_run=True, caplog=caplog)
        assert "DRY-RUN" in caplog.text
        m_save.assert_not_called()
        m_log.assert_not_called()

    def test_dry_run_false_logs_most_recent(self, caplog):
        """Operational path (dry_run=False) still calls log_most_recent_forecasts."""
        m_save, m_log = self._run_nonempty(dry_run=False, caplog=caplog)
        m_save.assert_called_once()
        m_log.assert_called_once()


# ===========================================================================
# T3b — write_csv / require_api forwarded to save_forecast_data (G3)
# ===========================================================================
class TestRunShortTermForwarding:
    def test_write_csv_and_require_api_forwarded(self):
        """_run_short_term_postprocessing forwards write_csv and require_api
        through to file_writer.save_forecast_data unchanged."""
        po = _import_operational()
        with (
            patch.object(
                po.data_reader,
                "read_observed_and_modelled_data",
                return_value=(pd.DataFrame(), pd.DataFrame()),
            ),
            patch.object(po.data_reader, "read_skill_metrics", return_value=pd.DataFrame()),
            patch.object(po.file_writer, "save_forecast_data", return_value=None) as m_save,
            patch.object(po, "_read_station_codes", return_value=["19999"]),
        ):
            po._run_short_term_postprocessing(
                po.PENTAD,
                dt.date(2026, 7, 20),
                [],
                TimingStats(),
                start_year=2025,
                end_year=2025,
                write_csv=False,
                require_api=True,
            )

        m_save.assert_called_once()
        kwargs = m_save.call_args.kwargs
        assert kwargs["write_csv"] is False
        assert kwargs["require_api"] is True


# ===========================================================================
# T4 — main(): per (horizon, year) ascending iteration; both horizons
# ===========================================================================
class TestBackfillMainYearIteration:
    def test_both_horizons_each_year_ascending(self):
        bf = _import_backfill()
        with (
            patch.object(bf, "_run_short_term_postprocessing") as m_run,
            patch.object(bf.sl, "load_environment"),
        ):
            rc = bf.main(
                [
                    "--start-date",
                    "2024-03-01",
                    "--end-date",
                    "2026-07-10",
                    "--horizon",
                    "both",
                ]
            )

        assert rc == 0
        # 3 years x 2 horizons = 6 invocations.
        assert m_run.call_count == 6

        # Collect (config, year) pairs in invocation order.
        pairs = []
        for call in m_run.call_args_list:
            config = call.args[0]
            year = call.kwargs["start_year"]
            assert call.kwargs["end_year"] == year  # single-year per call
            # The per-year date anchor is Jan 1 of that year (arg index 1).
            assert call.args[1] == dt.date(year, 1, 1)
            # API-only defaults: no filesystem write, no dry run.
            assert call.kwargs["dry_run"] is False
            assert call.kwargs["write_csv"] is False
            pairs.append((config, year))

        years_in_order = [y for _, y in pairs]
        # Years iterate ascending (non-decreasing across the sequence).
        assert years_in_order == sorted(years_in_order)
        assert set(years_in_order) == {2024, 2025, 2026}

        # Both PENTAD and DECAD configs are exercised for every year.
        for y in (2024, 2025, 2026):
            configs_for_year = {c for c, yy in pairs if yy == y}
            assert bf.PENTAD in configs_for_year
            assert bf.DECAD in configs_for_year


# ===========================================================================
# T5 — CLI validation: bad ranges / malformed dates return non-zero, no raise
# ===========================================================================
class TestBackfillMainValidation:
    def test_end_before_start_returns_nonzero(self):
        bf = _import_backfill()
        with (
            patch.object(bf, "_run_short_term_postprocessing") as m_run,
            patch.object(bf.sl, "load_environment") as m_env,
        ):
            rc = bf.main(
                [
                    "--start-date",
                    "2026-07-10",
                    "--end-date",
                    "2024-03-01",
                    "--horizon",
                    "both",
                ]
            )
        assert isinstance(rc, int)
        assert rc != 0
        m_run.assert_not_called()
        # Validation must happen BEFORE any environment load.
        m_env.assert_not_called()

    def test_malformed_date_returns_nonzero(self):
        bf = _import_backfill()
        with (
            patch.object(bf, "_run_short_term_postprocessing") as m_run,
            patch.object(bf.sl, "load_environment") as m_env,
        ):
            rc = bf.main(
                [
                    "--start-date",
                    "not-a-date",
                    "--end-date",
                    "2026-07-10",
                    "--horizon",
                    "pentad",
                ]
            )
        assert isinstance(rc, int)
        assert rc != 0
        m_run.assert_not_called()
        # Validation must happen BEFORE any environment load.
        m_env.assert_not_called()


# ===========================================================================
# T6 — horizon selection
# ===========================================================================
class TestBackfillHorizonSelection:
    def _run_horizon(self, horizon):
        bf = _import_backfill()
        with (
            patch.object(bf, "_run_short_term_postprocessing") as m_run,
            patch.object(bf.sl, "load_environment"),
        ):
            rc = bf.main(
                [
                    "--start-date",
                    "2025-01-01",
                    "--end-date",
                    "2025-06-30",
                    "--horizon",
                    horizon,
                ]
            )
        return bf, m_run, rc

    def test_pentad_only_invokes_pentad_config(self):
        bf, m_run, rc = self._run_horizon("pentad")
        assert rc == 0
        # Single-year range 2025-01-01..2025-06-30 -> exactly one call, PENTAD.
        assert m_run.call_count == 1
        configs = [c.args[0] for c in m_run.call_args_list]
        assert all(c is bf.PENTAD for c in configs)
        assert bf.DECAD not in configs

    def test_decad_only_invokes_decad_config(self):
        bf, m_run, rc = self._run_horizon("decad")
        assert rc == 0
        # Single-year range 2025-01-01..2025-06-30 -> exactly one call, DECAD.
        assert m_run.call_count == 1
        configs = [c.args[0] for c in m_run.call_args_list]
        assert all(c is bf.DECAD for c in configs)
        assert bf.PENTAD not in configs


# ===========================================================================
# T7 — End-to-end persistence: write_csv toggles CSV writes, API always runs
# ===========================================================================
class TestSaveForecastDataWriteCsvKwarg:
    """save_forecast_data(config, frame, write_csv=...) — the API write of the
    healed per-model period row happens regardless; the CSV writes obey the
    write_csv flag. Only the API boundary is mocked (to capture the payload)
    plus atomic_write_csv (to observe whether it fired)."""

    @pytest.fixture
    def missed_period_frame(self):
        """Minimal valid combined frame with a per-model period row for a
        previously-missed pentad period."""
        return pd.DataFrame(
            {
                "code": ["19999"],
                "date": pd.to_datetime(["2026-07-10"]),
                "pentad_in_month": [3],
                "pentad_in_year": [39],
                "forecasted_discharge": [30.0],
                "model_short": ["TFT"],
            }
        )

    @pytest.fixture(autouse=True)
    def _csv_env(self, tmp_path):
        overrides = {
            "ieasyforecast_intermediate_data_path": str(tmp_path),
            "ieasyforecast_combined_forecast_pentad_file": "combined_pentad.csv",
            "ieasyforecast_combined_forecast_decad_file": "combined_decad.csv",
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_CONSISTENCY_CHECK": "false",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            yield

    def test_write_csv_false_api_yes_csv_no(self, missed_period_frame):
        from conftest import PENTAD

        captured = {}

        def _capture(df, horizon):
            captured["df"] = df.copy()
            captured["horizon"] = horizon
            return True

        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(
                api_writer, "_write_combined_forecast_to_api", side_effect=_capture
            ) as m_api,
            patch.object(file_writer, "atomic_write_csv") as m_atomic,
        ):
            file_writer.save_forecast_data(PENTAD, missed_period_frame, write_csv=False)

        # (a) API received the healed per-model period row.
        m_api.assert_called_once()
        payload = captured["df"]
        assert "19999" in set(payload["code"].astype(str))
        assert "TFT" in set(payload["model_short"])
        # (a') Full payload content: date, period, value, model + horizon arg.
        assert "2026-07-10" in set(payload["date"].astype(str))
        assert 39 in set(payload["pentad_in_year"].astype(int))
        assert payload["forecasted_discharge"].iloc[0] == pytest.approx(30.0)
        assert captured["horizon"] == "pentad"
        # (b) No CSV write happened.
        m_atomic.assert_not_called()

    def test_write_csv_true_writes_csv(self, missed_period_frame):
        from conftest import PENTAD

        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(api_writer, "_write_combined_forecast_to_api", return_value=True),
            patch.object(file_writer, "atomic_write_csv") as m_atomic,
        ):
            file_writer.save_forecast_data(PENTAD, missed_period_frame, write_csv=True)

        # Default/True path performs exactly two CSV writes (combined + latest).
        assert m_atomic.call_count == 2


# ===========================================================================
# T8 — Issue-date vs target Dec31/Jan1 semantics (locked via real helpers)
# ===========================================================================
class TestIssueDateTargetSemantics:
    """A target period starting Jan 1 of year Y derives from the Dec-31
    (year Y-1) issue date. Locks the target=date+1 mapping and period math
    so the year-at-a-time backfill anchors periods in the right year."""

    def test_dec31_issue_maps_to_jan1_target_period(self):
        import tag_library as tl
        from src import postprocessing_tools as pt

        issue_date = dt.date(2025, 12, 31)
        target_start = pt.forecast_target_date(issue_date)

        # The target period starts Jan 1 of the following year.
        assert target_start == dt.date(2026, 1, 1)
        # Jan 1 is pentad-in-year 1 (first pentad of year Y).
        assert str(tl.get_pentad_in_year(target_start)) == "1"
        # Dec 31 is an end-of-month pentad boundary (a valid issue day).
        from postprocessing_operational import is_pentad_boundary

        assert is_pentad_boundary(issue_date) is True


# ===========================================================================
# T9 — Write/processing failure surfaced by main as non-zero exit
# ===========================================================================
class TestBackfillMainFailureSurfaced:
    def test_propagated_error_returns_nonzero(self):
        bf = _import_backfill()

        def _raise_for_2025(config, today, errors, *args, **kwargs):
            # Simulate a fail-loud API-write failure in one backfill year.
            if kwargs.get("start_year") == 2025:
                raise RuntimeError("simulated API write failure")
            return None

        with (
            patch.object(
                bf, "_run_short_term_postprocessing", side_effect=_raise_for_2025
            ) as m_run,
            patch.object(bf.sl, "load_environment"),
        ):
            rc = bf.main(
                [
                    "--start-date",
                    "2024-01-01",
                    "--end-date",
                    "2026-12-31",
                    "--horizon",
                    "pentad",
                ]
            )

        # main must report failure (non-zero) rather than raise or claim success.
        assert isinstance(rc, int)
        assert rc != 0
        # One bad year must NOT abort the rest: a year AFTER the failing 2025
        # is still attempted.
        attempted_years = {c.kwargs["start_year"] for c in m_run.call_args_list}
        assert 2026 in attempted_years


# ===========================================================================
# T10 — Motivating failure: get_latest_forecasts collapses same period across
#       years, which is exactly WHY the backfill must process one year at a
#       time. (Regression lock for the year-at-a-time design.)
# ===========================================================================
class TestGetLatestForecastsCollapsesAcrossYears:
    def test_same_period_two_years_collapses_to_later_year(self):
        # Same (code, pentad_in_year, model_short) on two different calendar
        # years — the yearless dedup key means only one row can survive.
        frame = pd.DataFrame(
            {
                "code": ["19999", "19999"],
                "date": pd.to_datetime(["2025-07-10", "2026-07-10"]),
                "pentad_in_year": [39, 39],
                "model_short": ["TFT", "TFT"],
                "forecasted_discharge": [25.0, 30.0],
            }
        )

        result = file_writer.get_latest_forecasts(frame, horizon_column_name="pentad_in_year")

        # Collapses to a single row; the LATER year (2026) survives.
        assert len(result) == 1
        assert pd.to_datetime(result["date"].iloc[0]).year == 2026
        assert result["forecasted_discharge"].iloc[0] == pytest.approx(30.0)


# ===========================================================================
# A — require_api failure modes on the REAL save_forecast_data
# ===========================================================================
class TestSaveForecastDataRequireApi:
    """The ``require_api`` flag turns a non-performed / failed API write into a
    hard RuntimeError; the best-effort default (require_api=False) never raises.
    Only the api_writer seam is patched; write_csv=False keeps CSV I/O out."""

    @pytest.fixture
    def missed_period_frame(self):
        """Minimal valid combined frame with a per-model period row for a
        previously-missed pentad period (same shape as T7)."""
        return pd.DataFrame(
            {
                "code": ["19999"],
                "date": pd.to_datetime(["2026-07-10"]),
                "pentad_in_month": [3],
                "pentad_in_year": [39],
                "forecasted_discharge": [30.0],
                "model_short": ["TFT"],
            }
        )

    @pytest.fixture(autouse=True)
    def _csv_env(self, tmp_path):
        overrides = {
            "ieasyforecast_intermediate_data_path": str(tmp_path),
            "ieasyforecast_combined_forecast_pentad_file": "combined_pentad.csv",
            "ieasyforecast_combined_forecast_decad_file": "combined_decad.csv",
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_CONSISTENCY_CHECK": "false",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            yield

    def test_a1_api_unavailable_require_api_raises_default_does_not(self, missed_period_frame):
        """A1: API unavailable -> require_api=True raises; require_api=False
        (best-effort default) completes without raising and skips the write."""
        from conftest import PENTAD

        # require_api=True: unavailable API is a hard error.
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            with pytest.raises(RuntimeError):
                file_writer.save_forecast_data(
                    PENTAD, missed_period_frame.copy(), write_csv=False, require_api=True
                )

        # require_api=False: best-effort, no raise (API write simply skipped).
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            ret = file_writer.save_forecast_data(
                PENTAD, missed_period_frame.copy(), write_csv=False, require_api=False
            )
        assert ret is None

    def test_a2_api_write_returns_false_require_api_raises_default_does_not(
        self, missed_period_frame
    ):
        """A2: API available but the write returns False -> require_api=True
        raises; require_api=False swallows the falsy return."""
        from conftest import PENTAD

        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(api_writer, "_write_combined_forecast_to_api", return_value=False),
        ):
            with pytest.raises(RuntimeError):
                file_writer.save_forecast_data(
                    PENTAD, missed_period_frame.copy(), write_csv=False, require_api=True
                )

        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(api_writer, "_write_combined_forecast_to_api", return_value=False),
        ):
            ret = file_writer.save_forecast_data(
                PENTAD, missed_period_frame.copy(), write_csv=False, require_api=False
            )
        assert ret is None

    def test_a3_api_write_success_require_api_does_not_raise(self, missed_period_frame):
        """A3: API available and the write returns True -> require_api=True
        completes and the API write is performed exactly once."""
        from conftest import PENTAD

        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(api_writer, "_write_combined_forecast_to_api", return_value=True) as m_api,
        ):
            ret = file_writer.save_forecast_data(
                PENTAD, missed_period_frame.copy(), write_csv=False, require_api=True
            )

        assert ret is None
        m_api.assert_called_once()


# ===========================================================================
# B — Composed end-to-end: real aggregation -> real operational helper ->
#     real save -> captured API payload (a WIRING regression fails this)
# ===========================================================================
class TestComposedBackfillEndToEnd:
    """The full heal path with nothing faked between aggregation and the API
    boundary: the modelled frame is produced by the REAL
    ``_normalize_ml_forecasts`` (real daily->pentad aggregation), then flows
    through the REAL ``_run_short_term_postprocessing`` and REAL
    ``save_forecast_data`` (write_csv=False, require_api=True). Only the reader,
    station-code, skill, virtual-station, log, and API-write seams are patched."""

    @pytest.fixture(autouse=True)
    def _csv_env(self, tmp_path):
        overrides = {
            "ieasyforecast_intermediate_data_path": str(tmp_path),
            "ieasyforecast_combined_forecast_pentad_file": "combined_pentad.csv",
            "ieasyforecast_combined_forecast_decad_file": "combined_decad.csv",
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_CONSISTENCY_CHECK": "false",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            yield

    def test_missed_boundary_heals_through_operational_save_to_api(self):
        from conftest import PENTAD

        po = _import_operational()

        # REAL aggregation: raw DAY frame -> one healed per-model period row.
        modelled = _normalize_ml_forecasts(_raw_missed_boundary_day_frame(), "TFT", "pentad")
        assert not modelled.empty  # sanity: aggregation produced the healed row

        captured = {}

        def _capture(df, horizon):
            captured["df"] = df.copy()
            captured["horizon"] = horizon
            return True

        with (
            patch.object(
                po.data_reader,
                "read_observed_and_modelled_data",
                return_value=(pd.DataFrame(), modelled.copy()),
            ),
            patch.object(po.data_reader, "read_skill_metrics", return_value=pd.DataFrame()),
            patch.object(po.sl, "calculate_virtual_stations_data", side_effect=lambda df: df),
            patch.object(po, "_read_station_codes", return_value=["19999"]),
            patch.object(po.pt, "log_most_recent_forecasts"),
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(api_writer, "_write_combined_forecast_to_api", side_effect=_capture),
        ):
            po._run_short_term_postprocessing(
                PENTAD,
                dt.date(2026, 7, 20),
                [],
                TimingStats(),
                start_year=2026,
                end_year=2026,
                write_csv=False,
                require_api=True,
            )

        # NOTE: the real api_writer's LR-exclusion / null-drop / dedup run INSIDE
        # _write_combined_forecast_to_api, which is patched here — so those are
        # out of scope for this test. We assert on the payload
        # save_forecast_data hands to the API (post get_latest_forecasts), which
        # is the correct boundary for a composed wiring regression.
        assert "df" in captured
        payload = captured["df"]
        tft = payload[(payload["code"].astype(str) == "19999") & (payload["model_short"] == "TFT")]
        assert len(tft) == 1
        assert str(tft["pentad_in_year"].iloc[0]) == "39"
        assert tft["forecasted_discharge"].iloc[0] == pytest.approx(30.0)
        assert captured["horizon"] == "pentad"


# ===========================================================================
# C — main() surfaces a require_api failure that propagates through the REAL
#     save path (composes main -> real save -> non-zero exit)
# ===========================================================================
class TestBackfillMainRequireApiFailure:
    """A failed API write in the REAL save path (require_api=True is set by
    main) is caught by main's per-year try/except and reported as a non-zero
    exit code — main never raises and never claims success."""

    @pytest.fixture(autouse=True)
    def _csv_env(self, tmp_path):
        overrides = {
            "ieasyforecast_intermediate_data_path": str(tmp_path),
            "ieasyforecast_combined_forecast_pentad_file": "combined_pentad.csv",
            "ieasyforecast_combined_forecast_decad_file": "combined_decad.csv",
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_CONSISTENCY_CHECK": "false",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            yield

    def test_main_returns_nonzero_when_api_write_fails(self):
        bf = _import_backfill()
        po = _import_operational()

        # REAL aggregation feeds real data into the real save path.
        modelled = _normalize_ml_forecasts(_raw_missed_boundary_day_frame(), "TFT", "pentad")

        with (
            patch.object(bf.sl, "load_environment"),
            patch.object(
                po.data_reader,
                "read_observed_and_modelled_data",
                return_value=(pd.DataFrame(), modelled.copy()),
            ),
            patch.object(po.data_reader, "read_skill_metrics", return_value=pd.DataFrame()),
            patch.object(po.sl, "calculate_virtual_stations_data", side_effect=lambda df: df),
            patch.object(po, "_read_station_codes", return_value=["19999"]),
            patch.object(po.pt, "log_most_recent_forecasts"),
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(api_writer, "_write_combined_forecast_to_api", return_value=False),
        ):
            rc = bf.main(
                [
                    "--start-date",
                    "2026-07-01",
                    "--end-date",
                    "2026-07-01",
                    "--horizon",
                    "pentad",
                ]
            )

        # The require_api RuntimeError raised by the real save is caught by
        # main's per-year handler and surfaced as a non-zero exit code.
        assert isinstance(rc, int)
        assert rc != 0
