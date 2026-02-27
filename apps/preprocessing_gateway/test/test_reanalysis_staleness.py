"""
Diagnostic tests for the reanalysis extension pipeline.

These tests investigate why reanalysis files (e.g. 00003_T_reanalysis.csv)
stop receiving new data approximately 6 months behind the current date.

Root cause: The 195-day stability window in select_stable_operational_data()
combined with the control member's date range means the reanalysis always
trails "today" by approximately 180 days (195 minus the forecast horizon).

The tests below document and verify this behavior so operators can
understand when the gap is expected vs. when something is broken.
"""

import os
import sys
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from extend_era5_reanalysis import (
    calculate_daily_norm,
    extend_reanalysis_with_operational,
    select_stable_operational_data,
)

# =====================================================================
# Helper to build realistic DataFrames
# =====================================================================


def make_daily_df(
    start_date: date, end_date: date, code: str, value_col: str = "P", base_value: float = 5.0
):
    """Build a daily DataFrame mimicking reanalysis or control member."""
    dates = pd.date_range(start_date, end_date, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "code": code,
            value_col: [base_value + i * 0.01 for i in range(len(dates))],
        }
    )


# =====================================================================
# 1. Stability window determines reanalysis lag
# =====================================================================


class TestStabilityWindowLag:
    """Verify that the 195-day stability window creates a ~180-day lag
    in the reanalysis when the control member includes forecast days."""

    def test_reanalysis_lag_with_15_day_forecast(self):
        """Control member from (today-365) to (today+15) — the typical
        operational range.  Stable data ends at (today+15) - 195 =
        today - 180.  The reanalysis can never advance beyond that."""
        today = date(2026, 2, 21)
        cm_start = today - timedelta(days=365)
        cm_end = today + timedelta(days=15)  # 15-day ECMWF forecast

        cm = make_daily_df(cm_start, cm_end, "00003")
        stable = select_stable_operational_data(cm)

        latest_stable = stable["date"].max().date()
        expected_threshold = cm_end - timedelta(days=195)
        # strict < means latest stable is threshold - 1 day
        expected_latest = expected_threshold - timedelta(days=1)

        assert latest_stable == expected_latest
        # This works out to approximately today - 181 days
        lag_days = (today - latest_stable).days
        assert 175 <= lag_days <= 185, f"Expected ~180-day lag, got {lag_days} days"

    def test_reanalysis_lag_without_forecast(self):
        """Control member ending exactly at 'today' (no forecast days).
        Stable data ends at today - 195.  Reanalysis trails by ~195."""
        today = date(2026, 2, 21)
        cm_start = today - timedelta(days=365)
        cm_end = today

        cm = make_daily_df(cm_start, cm_end, "00003")
        stable = select_stable_operational_data(cm)

        latest_stable = stable["date"].max().date()
        lag_days = (today - latest_stable).days
        assert lag_days == 196, f"Expected 196-day lag (195 + 1 for strict <), got {lag_days}"

    @pytest.mark.parametrize("forecast_horizon", [0, 5, 10, 15, 20])
    def test_lag_varies_with_forecast_horizon(self, forecast_horizon):
        """The reanalysis lag = 195 - forecast_horizon + 1 (strict <)."""
        today = date(2026, 2, 21)
        cm_start = today - timedelta(days=365)
        cm_end = today + timedelta(days=forecast_horizon)

        cm = make_daily_df(cm_start, cm_end, "00003")
        stable = select_stable_operational_data(cm)

        latest_stable = stable["date"].max().date()
        lag_days = (today - latest_stable).days
        expected_lag = 195 - forecast_horizon + 1
        assert lag_days == expected_lag


# =====================================================================
# 2. Reanalysis does NOT grow when it already covers the stable window
# =====================================================================


class TestNoGrowthWhenAlreadyCovered:
    """When the initial ERA5 download already covers all stable dates,
    the daily extension adds zero new rows."""

    def test_no_new_rows_when_reanalysis_ahead(self):
        """ERA5 reanalysis goes up to Aug 30, 2025.  Today's control
        member (Feb 21 + 15-day forecast) has stable data ending at
        Aug 24, 2025.  Reanalysis already covers all stable dates,
        so no new rows are added."""
        today = date(2026, 2, 21)
        cm_start = today - timedelta(days=365)
        cm_end = today + timedelta(days=15)

        cm = make_daily_df(cm_start, cm_end, "00003")
        stable = select_stable_operational_data(cm)
        stable_end = stable["date"].max()

        # Existing reanalysis covers well beyond the stable window
        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 30), "00003")
        assert reanalysis["date"].max() > stable_end, (
            "Test setup: reanalysis must extend beyond stable window"
        )

        combined = extend_reanalysis_with_operational(reanalysis, stable)

        original_end = reanalysis["date"].max()
        new_end = combined["date"].max()
        assert new_end == original_end, (
            f"Expected reanalysis to stay at {original_end}, but it grew to {new_end}"
        )

    def test_no_new_rows_count(self):
        """Verify row count doesn't increase when CM stable window is
        a subset of existing reanalysis."""
        # CM from Feb 2025 to Mar 2026 — stable portion ends ~Aug 24
        cm = make_daily_df(date(2025, 2, 21), date(2026, 3, 8), "00003")
        stable = select_stable_operational_data(cm)
        stable_end = stable["date"].max().date()

        # Reanalysis must cover beyond stable end for "no growth"
        reanalysis = make_daily_df(date(2020, 1, 1), stable_end + timedelta(days=10), "00003")

        original_len = len(reanalysis)
        combined = extend_reanalysis_with_operational(reanalysis, stable)

        # Values may be updated (operational wins dedup) but no new dates
        assert len(combined) == original_len, (
            f"Expected {original_len} rows, got {len(combined)}. "
            f"Stable CM range: {stable['date'].min()} to {stable['date'].max()}, "
            f"Reanalysis range: {reanalysis['date'].min()} to {reanalysis['date'].max()}"
        )


# =====================================================================
# 3. Reanalysis DOES grow when stable data extends beyond it
# =====================================================================


class TestGrowthWhenStableDataIsNewer:
    """When the stable control member data extends beyond the current
    reanalysis end date, new rows are added."""

    def test_grows_by_expected_days(self):
        """ERA5 ends Dec 31, 2024.  CM stable window ends ~Aug 9, 2025.
        Reanalysis should grow by ~221 days (Jan 1 to Aug 9)."""
        today = date(2026, 2, 21)
        cm_start = today - timedelta(days=365)
        cm_end = today + timedelta(days=15)

        reanalysis = make_daily_df(date(2020, 1, 1), date(2024, 12, 31), "00003")
        cm = make_daily_df(cm_start, cm_end, "00003")

        stable = select_stable_operational_data(cm)
        # Only new data is CM stable data after reanalysis end
        new_data = stable[stable["date"] > reanalysis["date"].max()]

        combined = extend_reanalysis_with_operational(reanalysis, stable)

        expected_new_end = stable["date"].max()
        actual_new_end = combined["date"].max()
        assert actual_new_end == expected_new_end, (
            f"Expected reanalysis to grow to {expected_new_end}, got {actual_new_end}"
        )

        added_rows = len(combined) - len(reanalysis)
        assert added_rows == len(new_data), f"Expected {len(new_data)} new rows, got {added_rows}"

    def test_daily_run_adds_one_day(self):
        """Simulate two consecutive daily runs.  Each run should extend
        the reanalysis by exactly 1 day."""
        # Day 1: reanalysis ends at Aug 8
        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 8), "00003")

        # Day 1 CM: stable ends at Aug 9
        day1_cm = make_daily_df(date(2025, 2, 21), date(2026, 2, 21) + timedelta(days=15), "00003")
        stable1 = select_stable_operational_data(day1_cm)
        reanalysis = extend_reanalysis_with_operational(reanalysis, stable1)
        end_after_day1 = reanalysis["date"].max().date()

        # Day 2 CM: one day later, stable window shifts by 1 day
        day2_cm = make_daily_df(date(2025, 2, 22), date(2026, 2, 22) + timedelta(days=15), "00003")
        stable2 = select_stable_operational_data(day2_cm)
        reanalysis = extend_reanalysis_with_operational(reanalysis, stable2)
        end_after_day2 = reanalysis["date"].max().date()

        assert end_after_day2 == end_after_day1 + timedelta(days=1), (
            f"Expected 1-day growth: {end_after_day1} → "
            f"{end_after_day1 + timedelta(days=1)}, "
            f"got {end_after_day2}"
        )


# =====================================================================
# 4. Stability window parameter sensitivity
# =====================================================================


class TestStabilityWindowTuning:
    """Explore how changing the stability_days parameter affects the
    reanalysis gap.  Helps decide whether 195 days is appropriate."""

    @pytest.mark.parametrize(
        "stability_days,expected_max_lag",
        [
            (195, 196),  # Current default: ~6.5 months lag
            (180, 181),  # 6 months
            (90, 91),  # 3 months
            (30, 31),  # 1 month
            (15, 16),  # Matches forecast horizon
        ],
    )
    def test_lag_with_different_stability_windows(self, stability_days, expected_max_lag):
        """With no forecast horizon, lag = stability_days + 1 (strict <)."""
        today = date(2026, 2, 21)
        cm = make_daily_df(today - timedelta(days=365), today, "00003")
        stable = select_stable_operational_data(cm, stability_days)
        latest_stable = stable["date"].max().date()
        lag = (today - latest_stable).days
        assert lag == expected_max_lag

    def test_stability_30_with_forecast_closes_gap_to_16_days(self):
        """With stability_days=30 and 15-day forecast, the reanalysis
        trails by only 16 days — much more current than the default."""
        today = date(2026, 2, 21)
        cm = make_daily_df(today - timedelta(days=365), today + timedelta(days=15), "00003")
        stable = select_stable_operational_data(cm, stability_days=30)
        latest_stable = stable["date"].max().date()
        lag = (today - latest_stable).days
        assert lag == 16


# =====================================================================
# 5. End-to-end pipeline simulation
# =====================================================================


class TestEndToEndPipelineSimulation:
    """Simulate the full extend_era5_reanalysis main() flow with mock
    files to verify the expected reanalysis growth pattern."""

    def test_pipeline_extends_reanalysis_and_writes_dashboard(self, tmp_path):
        """Full pipeline: read reanalysis + CM → filter stable → extend
        → calculate norm → write CSV + dashboard CSV."""
        code = "00003"
        today = date(2026, 2, 21)

        # Create initial reanalysis (ends well before stable window)
        reanalysis_end = date(2025, 6, 1)
        reanalysis = make_daily_df(date(2020, 1, 1), reanalysis_end, code)
        reanalysis_file = tmp_path / f"{code}_P_reanalysis.csv"
        reanalysis.to_csv(reanalysis_file, index=False)

        # Create control member (365 days back + 15-day forecast)
        cm = make_daily_df(today - timedelta(days=365), today + timedelta(days=15), code)
        cm_file = tmp_path / f"{code}_P_control_member.csv"
        cm.to_csv(cm_file, index=False)

        # Execute pipeline steps
        reanalysis_df = pd.read_csv(reanalysis_file)
        cm_df = pd.read_csv(cm_file)
        reanalysis_df["date"] = pd.to_datetime(reanalysis_df["date"])
        cm_df["date"] = pd.to_datetime(cm_df["date"])

        stable = select_stable_operational_data(cm_df)
        extended = extend_reanalysis_with_operational(reanalysis_df, stable)

        # Verify reanalysis grew
        assert extended["date"].max() > reanalysis_df["date"].max(), (
            "Reanalysis should have grown with new stable data"
        )

        # Verify stable threshold
        expected_threshold = cm_df["date"].max() - timedelta(days=195)
        expected_latest = expected_threshold - timedelta(days=1)
        assert extended["date"].max() == expected_latest

        # Calculate norm and verify dashboard output
        norm = calculate_daily_norm(extended, cm_df, "P", today.year)
        assert "P_norm" in norm.columns
        assert "P" in norm.columns
        assert len(norm) == 365  # 2026 is not a leap year

        # Save and re-read to verify CSV round-trip
        extended.to_csv(reanalysis_file, index=False)
        dashboard_file = tmp_path / f"{code}_P_reanalysis_dashboard.csv"
        norm.to_csv(dashboard_file, index=False)

        reread = pd.read_csv(reanalysis_file)
        assert len(reread) == len(extended)

    def test_pipeline_repeated_runs_are_idempotent_within_same_day(self, tmp_path):
        """Running the pipeline twice on the same day should not
        change the reanalysis (idempotent after first extension)."""
        code = "00003"
        today = date(2026, 2, 21)

        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 6, 1), code)
        cm = make_daily_df(today - timedelta(days=365), today + timedelta(days=15), code)

        # Run 1
        stable = select_stable_operational_data(cm)
        result1 = extend_reanalysis_with_operational(reanalysis, stable)

        # Run 2 (using result1 as the new reanalysis)
        stable2 = select_stable_operational_data(cm)
        result2 = extend_reanalysis_with_operational(result1, stable2)

        pd.testing.assert_frame_equal(
            result1.reset_index(drop=True),
            result2.reset_index(drop=True),
        )


# =====================================================================
# 6. Diagnosing the Aug 21 observation
# =====================================================================


class TestDiagnoseAugust21:
    """The user observed that 00003_P_reanalysis.csv has no data after
    Aug 21, 2025.  These tests calculate what dates are expected given
    various scenarios."""

    def test_aug21_matches_pipeline_running_today(self):
        """If the pipeline ran today (Feb 21, 2026) with a control
        member that includes a 15-day forecast, the latest stable date
        should be approximately Aug 10, 2025.  Aug 21 is 11 days later,
        suggesting the forecast horizon is about 26 days or the pipeline
        last ran around Mar 4."""
        today = date(2026, 2, 21)
        target_latest = date(2025, 8, 21)

        # Calculate: what forecast_horizon produces Aug 21 as latest?
        # threshold = max_date - 195 > Aug 21  =>  max_date > Aug 21 + 195
        # max_date = today + forecast_horizon
        # today + forecast_horizon > Aug 21 + 195 = Mar 4, 2026
        required_max = target_latest + timedelta(days=195 + 1)  # strict <
        forecast_horizon = (required_max - today).days

        # Aug 21 + 196 = Mar 5, 2026.  forecast_horizon = Mar 5 - Feb 21 = 12
        assert forecast_horizon == 12, (
            f"Need {forecast_horizon}-day forecast to reach Aug 21. "
            f"ECMWF IFS provides ~15 days, so Aug 21 is plausible if "
            f"the gateway returns data up to today+12."
        )

    def test_aug21_with_default_15_day_forecast(self):
        """With a 15-day forecast, the pipeline running today (Feb 21)
        produces latest stable date of Aug 24, 2025.  So if the file
        shows Aug 21, the pipeline last ran about 3 days ago (Feb 18)
        with a 15-day forecast, OR it ran today with a 12-day forecast
        horizon (see test_aug21_with_variable_gateway_response)."""
        today = date(2026, 2, 21)
        cm_end = today + timedelta(days=15)
        threshold = cm_end - timedelta(days=195)
        latest_stable = threshold - timedelta(days=1)

        assert latest_stable == date(2025, 8, 24), (
            f"With 15-day forecast running today, latest stable is {latest_stable}"
        )

        # When would pipeline need to have run for Aug 21 to be latest?
        # latest_stable = (run_date + 15) - 195 - 1 = Aug 21
        # run_date = Aug 21 + 195 + 1 - 15 = Aug 21 + 181
        run_date = date(2025, 8, 21) + timedelta(days=195 + 1 - 15)
        assert run_date == date(2026, 2, 18), (
            f"Pipeline would need to have last run on {run_date} "
            f"for Aug 21 to be the latest stable date with 15-day "
            f"forecast horizon"
        )

    def test_aug21_if_no_forecast_horizon(self):
        """If the data gateway returns data up to exactly today (no
        forecast), the pipeline running today yields latest stable
        date of Aug 9, 2025."""
        today = date(2026, 2, 21)
        cm = make_daily_df(today - timedelta(days=365), today, "00003")
        stable = select_stable_operational_data(cm)
        latest = stable["date"].max().date()
        assert latest == date(2025, 8, 9)

    def test_aug21_with_variable_gateway_response(self):
        """The data gateway might return varying forecast horizons
        (not always exactly 15 days).  Test the range of possible
        latest stable dates."""
        today = date(2026, 2, 21)
        results = {}
        for horizon in range(0, 25):
            cm = make_daily_df(
                today - timedelta(days=365), today + timedelta(days=horizon), "00003"
            )
            stable = select_stable_operational_data(cm)
            if not stable.empty:
                results[horizon] = stable["date"].max().date()

        # With 12-day horizon, latest stable = Aug 21
        assert results[12] == date(2025, 8, 21), (
            f"12-day horizon gives {results[12]}, expected Aug 21"
        )

        # With 15-day horizon, latest stable = Aug 24 (wait, let me check)
        # cm_end = Feb 21 + 15 = Mar 8
        # threshold = Mar 8 - 195 = Aug 25
        # latest stable = Aug 24
        # Hmm, let me recalculate:
        # Mar 8 - 195 days:
        # Mar 8 -> Feb 8 (28), Feb 8 -> Jan 8 (31), Jan 8 -> Dec 8 (31),
        # Dec 8 -> Nov 8 (30), Nov 8 -> Oct 8 (31), Oct 8 -> Sep 8 (30),
        # Sep 8 -> Aug 25 (14) = 28+31+31+30+31+30+14 = 195
        # threshold = Aug 25, so latest stable = Aug 24
        assert results[15] == date(2025, 8, 24)

        # Report: Aug 21 requires a 12-day forecast horizon
        for horizon, latest in sorted(results.items()):
            if latest == date(2025, 8, 21):
                assert horizon == 12


# =====================================================================
# 7. Value overwrite behavior during extension
# =====================================================================


class TestValueOverwriteDuringExtension:
    """When the control member overlaps with existing reanalysis dates,
    the operational value wins (keep='last').  This changes existing
    values even when no new dates are added."""

    def test_operational_overwrites_era5_values(self):
        """ERA5 has P=1.0 on Jan 1.  CM has P=99.0 on Jan 1.
        After extension, Jan 1 has P=99.0."""
        reanalysis = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-01-01", "2025-01-02"]),
                "code": ["00003"] * 2,
                "P": [1.0, 2.0],
            }
        )
        cm_stable = pd.DataFrame(
            {
                "date": pd.to_datetime(["2025-01-01"]),
                "code": ["00003"],
                "P": [99.0],
            }
        )
        result = extend_reanalysis_with_operational(reanalysis, cm_stable)

        jan1 = result[result["date"] == pd.Timestamp("2025-01-01")]
        assert jan1["P"].iloc[0] == 99.0, "Operational data should overwrite ERA5 reanalysis values"
        assert len(result) == 2, "Row count should not change"

    def test_silent_overwrite_no_new_rows(self):
        """Even when the reanalysis already covers all stable dates,
        the extension still silently overwrites values (dedup behavior).
        This means the reanalysis file IS being modified each run even
        if its date range doesn't grow."""
        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 30), "00003", base_value=10.0)
        today = date(2026, 2, 21)
        cm = make_daily_df(
            today - timedelta(days=365),
            today + timedelta(days=15),
            "00003",
            base_value=50.0,  # Different values
        )
        stable = select_stable_operational_data(cm)

        original_len = len(reanalysis)
        combined = extend_reanalysis_with_operational(reanalysis, stable)

        # Same length (no new dates)
        assert len(combined) == original_len

        # But overlapping values are now from CM, not ERA5
        overlap_start = stable["date"].min()
        overlap_mask = combined["date"] >= overlap_start
        overlapping_original = reanalysis[reanalysis["date"] >= overlap_start]
        overlapping_combined = combined[overlap_mask]

        # Values in the overlap region should differ from original ERA5
        # (because CM values win dedup)
        if len(overlapping_original) > 0 and len(overlapping_combined) > 0:
            orig_vals = overlapping_original["P"].values[:5]
            new_vals = overlapping_combined["P"].values[:5]
            assert not np.allclose(orig_vals, new_vals), (
                "Expected CM values to overwrite ERA5 values in overlap"
            )


# =====================================================================
# 8. Multiple HRU codes — independent processing
# =====================================================================


class TestMultipleHRUCodes:
    """The pipeline loops over HRU codes independently.  Verify that
    one HRU's data doesn't affect another's."""

    def test_two_hrus_independent_stable_windows(self):
        """HRU 00003 and 00050 may have different control member date
        ranges (unlikely but possible if gateway returns different
        amounts of data).  Verify independent processing."""
        cm_003 = make_daily_df(date(2025, 2, 1), date(2026, 3, 8), "00003")
        cm_050 = make_daily_df(date(2025, 3, 1), date(2026, 3, 5), "00050")

        stable_003 = select_stable_operational_data(cm_003)
        stable_050 = select_stable_operational_data(cm_050)

        # Different max dates → different thresholds → different end
        assert stable_003["date"].max() != stable_050["date"].max()

    def test_extend_preserves_multi_code_reanalysis(self):
        """Reanalysis has both codes A and B.  Extending with stable
        data for code A only should preserve code B unchanged."""
        reanalysis = pd.concat(
            [
                make_daily_df(date(2020, 1, 1), date(2025, 6, 1), "A"),
                make_daily_df(date(2020, 1, 1), date(2025, 6, 1), "B"),
            ],
            ignore_index=True,
        )

        # Only code A has new stable data
        stable_a = make_daily_df(date(2025, 6, 2), date(2025, 8, 1), "A")

        combined = extend_reanalysis_with_operational(reanalysis, stable_a)

        code_b = combined[combined["code"] == "B"]
        original_b = reanalysis[reanalysis["code"] == "B"]

        pd.testing.assert_frame_equal(
            code_b.reset_index(drop=True),
            original_b.reset_index(drop=True),
        )
