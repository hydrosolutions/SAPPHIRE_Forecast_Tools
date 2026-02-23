"""
Tests documenting what data the operational pipeline writes to the API
and where coverage gaps exist.

Pipeline execution order (Dockerfile CMD):
  1. Quantile_Mapping_OP.py   → meteo yesterday+today, norm=None
  2. extend_era5_reanalysis.py → 365 dashboard records (norm + value)
  3. snow_data_operational.py  → snow yesterday+today

These tests verify the exact records each script would produce, so
operators can understand what the API receives vs. what it lacks.
"""
import os
import sys
from datetime import date, timedelta
from unittest.mock import patch, MagicMock, call

import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)

# Mock the sapphire_dg_client module before importing the actual modules.
# sapphire_dg_client is a private package not installed in the test env.
sys.modules['sapphire_dg_client'] = MagicMock()
sys.modules['sapphire_dg_client.SapphireDGClient'] = MagicMock()
sys.modules['sapphire_dg_client.snow_model'] = MagicMock()
sys.modules['sapphire_dg_client.client'] = MagicMock()

from extend_era5_reanalysis import (
    select_stable_operational_data,
    extend_reanalysis_with_operational,
    calculate_daily_norm,
    _write_meteo_to_api as extend_write_meteo,
)
import Quantile_Mapping_OP as qm
import snow_data_operational as sdo


# =====================================================================
# Helper
# =====================================================================

def make_daily_df(start: date, end: date, code: str,
                  col: str = 'P', base: float = 5.0):
    dates = pd.date_range(start, end, freq='D')
    return pd.DataFrame({
        'date': dates,
        'code': code,
        col: [base + i * 0.01 for i in range(len(dates))],
    })


# =====================================================================
# 1. Quantile_Mapping_OP writes yesterday+today with norm=None
# =====================================================================

class TestQMWritesRecentDays:
    """Quantile_Mapping_OP._write_meteo_to_api filters to yesterday+today
    (2-day window to guard against DG data lag) and sets norm=None.
    This means it provides the raw operational value but no climatological
    context."""

    def test_qm_writes_yesterday_and_today(self):
        """QM's _write_meteo_to_api filters to yesterday+today and sets
        norm=None.  We mock the API client to capture what is sent."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        today = pd.Timestamp.today().normalize()
        yesterday = today - pd.Timedelta(days=1)
        two_days_ago = today - pd.Timedelta(days=2)
        # Only 3 days of data — avoids the 381-row CM range issue
        cm_data = pd.DataFrame({
            'date': [two_days_ago, yesterday, today],
            'code': '00003',
            'P': [1.0, 2.0, 3.0],
        })

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_meteo.return_value = 2

        with patch.object(qm, 'SapphirePreprocessingClient',
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 'SAPPHIRE_API_ENABLED': 'true',
                 'SAPPHIRE_API_URL': 'http://test:8000',
             }):
            result = qm._write_meteo_to_api(cm_data, 'P', '00003')

        assert result is True
        records = mock_client.write_meteo.call_args[0][0]
        assert len(records) == 2, (
            f"QM should write 2 records (yesterday+today), "
            f"got {len(records)}"
        )
        dates = {r['date'] for r in records}
        assert dates == {
            yesterday.strftime('%Y-%m-%d'),
            today.strftime('%Y-%m-%d'),
        }
        for r in records:
            assert r['norm'] is None, (
                "QM writes norm=None — it has no climatological data"
            )

    def test_qm_skips_when_recent_days_not_in_data(self):
        """If the CM data doesn't include yesterday or today (e.g.,
        gateway returned only future forecast days), QM writes nothing."""
        if not qm.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        # Data only has tomorrow — outside the yesterday+today window
        day_after = (
            pd.Timestamp.today().normalize() + timedelta(days=1)
        )
        data = pd.DataFrame({
            'date': [day_after],
            'code': ['00003'],
            'P': [5.0],
        })

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True

        with patch.object(qm, 'SapphirePreprocessingClient',
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 'SAPPHIRE_API_ENABLED': 'true',
                 'SAPPHIRE_API_URL': 'http://test:8000',
             }):
            result = qm._write_meteo_to_api(data, 'P', '00003')

        assert result is False
        mock_client.write_meteo.assert_not_called()


# =====================================================================
# 2. extend_era5_reanalysis writes full current year
# =====================================================================

class TestExtendWritesFullYear:
    """extend_era5_reanalysis._write_meteo_to_api writes ALL data
    passed — the dashboard DataFrame for the entire current year."""

    def test_writes_365_records_for_non_leap_year(self):
        """Dashboard data for 2026 (non-leap): 365 records per code."""
        # Build a realistic dashboard DataFrame
        today = date(2026, 2, 21)
        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 1),
                                   '00003')
        cm = make_daily_df(
            today - timedelta(days=365),
            today + timedelta(days=15),
            '00003'
        )
        stable = select_stable_operational_data(cm)
        extended = extend_reanalysis_with_operational(reanalysis, stable)
        dashboard = calculate_daily_norm(extended, cm, 'P', today.year)

        assert len(dashboard) == 365

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_meteo.return_value = 365

        with patch('extend_era5_reanalysis.SAPPHIRE_API_AVAILABLE', True), \
             patch('extend_era5_reanalysis.SapphirePreprocessingClient',
                   return_value=mock_client), \
             patch.dict(os.environ, {
                 'SAPPHIRE_API_ENABLED': 'true',
                 'SAPPHIRE_API_URL': 'http://test:8000',
             }):
            result = extend_write_meteo(dashboard, 'P')

        assert result is True
        records = mock_client.write_meteo.call_args[0][0]
        assert len(records) == 365

    def test_norm_present_for_all_days(self):
        """Every dashboard record should have a norm value (from
        reanalysis climatology spanning 2020-2025)."""
        today = date(2026, 2, 21)
        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 1),
                                   '00003')
        cm = make_daily_df(
            today - timedelta(days=365),
            today + timedelta(days=15),
            '00003'
        )
        stable = select_stable_operational_data(cm)
        extended = extend_reanalysis_with_operational(reanalysis, stable)
        dashboard = calculate_daily_norm(extended, cm, 'P', today.year)

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_meteo.return_value = 365

        with patch('extend_era5_reanalysis.SAPPHIRE_API_AVAILABLE', True), \
             patch('extend_era5_reanalysis.SapphirePreprocessingClient',
                   return_value=mock_client), \
             patch.dict(os.environ, {
                 'SAPPHIRE_API_ENABLED': 'true',
                 'SAPPHIRE_API_URL': 'http://test:8000',
             }):
            extend_write_meteo(dashboard, 'P')

        records = mock_client.write_meteo.call_args[0][0]
        norms = [r['norm'] for r in records]
        none_norms = [n for n in norms if n is None]
        assert len(none_norms) == 0, (
            f"{len(none_norms)} of {len(records)} records have norm=None"
        )

    def test_value_present_for_current_year_dates_in_cm(self):
        """Operational values exist for current-year dates that fall
        within the CM date range (Jan 1 to ~today+15).  Dates after
        that have value=None."""
        today = date(2026, 2, 21)
        cm_start = today - timedelta(days=365)
        cm_end = today + timedelta(days=15)

        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 1),
                                   '00003')
        cm = make_daily_df(cm_start, cm_end, '00003')
        stable = select_stable_operational_data(cm)
        extended = extend_reanalysis_with_operational(reanalysis, stable)
        dashboard = calculate_daily_norm(extended, cm, 'P', today.year)

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_meteo.return_value = 365

        with patch('extend_era5_reanalysis.SAPPHIRE_API_AVAILABLE', True), \
             patch('extend_era5_reanalysis.SapphirePreprocessingClient',
                   return_value=mock_client), \
             patch.dict(os.environ, {
                 'SAPPHIRE_API_ENABLED': 'true',
                 'SAPPHIRE_API_URL': 'http://test:8000',
             }):
            extend_write_meteo(dashboard, 'P')

        records = mock_client.write_meteo.call_args[0][0]
        records_by_date = {r['date']: r for r in records}

        # Jan 1 is within CM range → has value
        jan1 = records_by_date['2026-01-01']
        assert jan1['value'] is not None, (
            "Jan 1 is within CM range, should have operational value"
        )
        assert jan1['norm'] is not None

        # Today is within CM range → has value
        today_rec = records_by_date[today.strftime('%Y-%m-%d')]
        assert today_rec['value'] is not None

        # Dec 31 is beyond CM range → value is None
        dec31 = records_by_date['2026-12-31']
        assert dec31['value'] is None, (
            "Dec 31 is beyond CM range, value should be None"
        )
        assert dec31['norm'] is not None, (
            "Dec 31 should still have a norm from reanalysis"
        )


# =====================================================================
# 3. Coverage gap: no historical data written by operational pipeline
# =====================================================================

class TestHistoricalDataGap:
    """The operational pipeline only writes current-year dashboard
    data.  Historical years are NOT written to the API.  This is
    the main coverage gap."""

    def test_dashboard_contains_only_current_year(self):
        """calculate_daily_norm produces dates only for current_year.
        No historical dates are included."""
        today = date(2026, 2, 21)
        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 1),
                                   '00003')
        cm = make_daily_df(
            today - timedelta(days=365),
            today + timedelta(days=15),
            '00003'
        )
        stable = select_stable_operational_data(cm)
        extended = extend_reanalysis_with_operational(reanalysis, stable)
        dashboard = calculate_daily_norm(extended, cm, 'P', today.year)

        years_in_dashboard = dashboard['date'].dt.year.unique()
        assert list(years_in_dashboard) == [2026], (
            f"Dashboard should contain only 2026, got {list(years_in_dashboard)}"
        )

    def test_raw_reanalysis_never_written_to_api(self):
        """The extended reanalysis (2000-present, ~2000+ records per
        code) is saved to CSV but NOT passed to _write_meteo_to_api.
        Only the dashboard (365 records) is written.  This means
        historical P/T values are only available via data migration."""
        today = date(2026, 2, 21)
        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 1),
                                   '00003')
        cm = make_daily_df(
            today - timedelta(days=365),
            today + timedelta(days=15),
            '00003'
        )
        stable = select_stable_operational_data(cm)
        extended = extend_reanalysis_with_operational(reanalysis, stable)
        dashboard = calculate_daily_norm(extended, cm, 'P', today.year)

        # The extended reanalysis has ~2000 records
        assert len(extended) > 1500
        # But only the 365-row dashboard is written to API
        assert len(dashboard) == 365

        # Quantifying the gap: records in reanalysis but not in API
        gap_records = len(extended) - len(dashboard)
        assert gap_records > 1000, (
            f"At least 1000 historical reanalysis records are NOT "
            f"written to the API. Actual gap: {gap_records} records."
        )


# =====================================================================
# 4. Forecast contamination in dashboard values
# =====================================================================

class TestForecastContamination:
    """The dashboard merges operational data from the FULL CM file,
    which includes ~15 forecast days.  These forecast values are
    written to the API as 'value' alongside norms, indistinguishable
    from observed operational data."""

    def test_forecast_days_appear_as_operational_values(self):
        """Dates beyond today but within CM range have values that
        come from the ECMWF control member forecast, not observations.
        The API record has no flag distinguishing them."""
        today = date(2026, 2, 21)
        forecast_horizon = 15
        cm_end = today + timedelta(days=forecast_horizon)

        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 1),
                                   '00003')
        cm = make_daily_df(
            today - timedelta(days=365), cm_end, '00003'
        )
        stable = select_stable_operational_data(cm)
        extended = extend_reanalysis_with_operational(reanalysis, stable)
        dashboard = calculate_daily_norm(extended, cm, 'P', today.year)

        # Check dates after today but within forecast horizon
        tomorrow = today + timedelta(days=1)
        forecast_end = cm_end
        forecast_rows = dashboard[
            (dashboard['date'] >= pd.Timestamp(tomorrow)) &
            (dashboard['date'] <= pd.Timestamp(forecast_end))
        ]

        assert len(forecast_rows) == forecast_horizon, (
            f"Expected {forecast_horizon} forecast-day rows, "
            f"got {len(forecast_rows)}"
        )

        # These rows have 'P' values (from CM forecast), not NaN
        has_value = forecast_rows['P'].notna().sum()
        assert has_value == forecast_horizon, (
            f"All {forecast_horizon} forecast days have values "
            f"(from ECMWF control member), but {has_value} have values"
        )

    def test_count_forecast_vs_observation_vs_missing(self):
        """Categorize all 365 dashboard records by data source:
        - observation: past dates with CM data
        - forecast: future dates with CM data
        - missing: dates beyond CM range (NaN value)"""
        today = date(2026, 2, 21)
        cm_start = today - timedelta(days=365)
        cm_end = today + timedelta(days=15)

        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 1),
                                   '00003')
        cm = make_daily_df(cm_start, cm_end, '00003')
        stable = select_stable_operational_data(cm)
        extended = extend_reanalysis_with_operational(reanalysis, stable)
        dashboard = calculate_daily_norm(extended, cm, 'P', today.year)

        today_ts = pd.Timestamp(today)
        cm_end_ts = pd.Timestamp(cm_end)
        year_start = pd.Timestamp(f'{today.year}-01-01')

        # Observation days: Jan 1 to today (inclusive)
        obs_days = dashboard[
            (dashboard['date'] >= year_start) &
            (dashboard['date'] <= today_ts) &
            (dashboard['P'].notna())
        ]
        # Forecast days: today+1 to cm_end
        forecast_days = dashboard[
            (dashboard['date'] > today_ts) &
            (dashboard['date'] <= cm_end_ts) &
            (dashboard['P'].notna())
        ]
        # Missing days: after cm_end
        missing_days = dashboard[
            dashboard['P'].isna()
        ]

        total = len(obs_days) + len(forecast_days) + len(missing_days)
        assert total == 365

        # Verify counts
        assert len(obs_days) == (today - date(2026, 1, 1)).days + 1  # 52
        assert len(forecast_days) == 15
        assert len(missing_days) == 365 - len(obs_days) - len(forecast_days)


# =====================================================================
# 5. Snow: yesterday+today operational write
# =====================================================================

class TestSnowWriteCoverage:
    """snow_data_operational._write_snow_to_api filters to yesterday+today
    (2-day window to guard against SnowMapper data lag). Historical snow
    data is NOT accumulated in the API by the operational pipeline."""

    def test_snow_writes_yesterday_and_today(self):
        """Even though the CSV accumulates all historical data, only
        yesterday+today are sent to the API."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        today = pd.Timestamp.today().normalize()
        yesterday = today - pd.Timedelta(days=1)
        # CSV has 365 days of accumulated data
        snow_data = pd.DataFrame({
            'date': pd.date_range(
                today - timedelta(days=364), today, freq='D'
            ),
            'code': '00003',
            'SWE': np.random.uniform(0, 100, 365),
        })

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_snow.return_value = 2

        with patch.object(sdo, 'SapphirePreprocessingClient',
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 'SAPPHIRE_API_ENABLED': 'true',
                 'SAPPHIRE_API_URL': 'http://test:8000',
             }):
            result = sdo._write_snow_to_api(snow_data, 'SWE', '00003')

        assert result is True
        records = mock_client.write_snow.call_args[0][0]
        assert len(records) == 2, (
            f"Snow operational write should be 2 records "
            f"(yesterday+today), got {len(records)}"
        )
        dates = {r['date'] for r in records}
        assert dates == {
            yesterday.strftime('%Y-%m-%d'),
            today.strftime('%Y-%m-%d'),
        }

    def test_snow_csv_has_full_history_but_api_gets_two_days(self):
        """Quantify the gap: CSV accumulates 365+ days, API gets 2."""
        if not sdo.SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        today = pd.Timestamp.today().normalize()
        n_days = 365
        snow_data = pd.DataFrame({
            'date': pd.date_range(
                today - timedelta(days=n_days - 1), today, freq='D'
            ),
            'code': '00003',
            'SWE': np.random.uniform(0, 100, n_days),
        })

        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_snow.return_value = 2

        with patch.object(sdo, 'SapphirePreprocessingClient',
                          return_value=mock_client), \
             patch.dict(os.environ, {
                 'SAPPHIRE_API_ENABLED': 'true',
                 'SAPPHIRE_API_URL': 'http://test:8000',
             }):
            sdo._write_snow_to_api(snow_data, 'SWE', '00003')

        records = mock_client.write_snow.call_args[0][0]
        gap = n_days - len(records)
        assert gap == 363, (
            f"Snow API gap: {gap} of {n_days} CSV days NOT written"
        )


# =====================================================================
# 6. Script execution order and overwrite behavior
# =====================================================================

class TestScriptExecutionOrder:
    """The Dockerfile runs scripts in order:
      1. Quantile_Mapping_OP.py  (meteo yesterday+today, norm=None)
      2. extend_era5_reanalysis.py (meteo full year, norm populated)
    Since the API uses upsert on (meteo_type, code, date), script 2
    overwrites script 1's records — replacing norm=None with the
    actual norm value.  This is correct behavior."""

    def test_extend_overwrites_qm_today_record(self):
        """Verify that the dashboard record for today includes both
        value AND norm, overwriting QM's norm=None."""
        today = date(2026, 2, 21)
        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 1),
                                   '00003')
        cm = make_daily_df(
            today - timedelta(days=365),
            today + timedelta(days=15),
            '00003'
        )
        stable = select_stable_operational_data(cm)
        extended = extend_reanalysis_with_operational(reanalysis, stable)
        dashboard = calculate_daily_norm(extended, cm, 'P', today.year)

        today_row = dashboard[
            dashboard['date'] == pd.Timestamp(today)
        ]
        assert len(today_row) == 1
        assert today_row['P'].notna().iloc[0], (
            "Today should have an operational value from CM"
        )
        assert today_row['P_norm'].notna().iloc[0], (
            "Today should have a norm from reanalysis climatology — "
            "this overwrites QM's norm=None"
        )


# =====================================================================
# 7. Summary: complete gap inventory
# =====================================================================

class TestGapInventory:
    """Summary test documenting all coverage gaps between CSV and API
    after a full operational pipeline run."""

    def test_gap_summary(self):
        """Build realistic data, run the pipeline, and count records
        that would be written to API vs. stored in CSV."""
        today = date(2026, 2, 21)
        code = '00003'
        cm_start = today - timedelta(days=365)
        cm_end = today + timedelta(days=15)

        # Build data
        reanalysis = make_daily_df(date(2020, 1, 1), date(2025, 8, 1),
                                   code)
        cm = make_daily_df(cm_start, cm_end, code)
        stable = select_stable_operational_data(cm)
        extended = extend_reanalysis_with_operational(reanalysis, stable)
        dashboard = calculate_daily_norm(extended, cm, 'P', today.year)

        # CSV state after pipeline
        csv_reanalysis_rows = len(extended)
        csv_dashboard_rows = len(dashboard)

        # API state after pipeline
        # QM: 2 records (yesterday + today)
        qm_api_records = 2
        # extend: 365 records (full year dashboard)
        extend_api_records = len(dashboard)
        # snow: 2 records per variable per HRU (yesterday + today)
        snow_api_records = 2

        # Gaps
        reanalysis_not_in_api = csv_reanalysis_rows - extend_api_records

        # --- Assertions documenting the gaps ---

        # Gap 1: Historical reanalysis not in API
        assert reanalysis_not_in_api > 1500, (
            f"GAP 1: {reanalysis_not_in_api} historical reanalysis "
            f"records exist in CSV but are NOT in the API. "
            f"These require data migration."
        )

        # Gap 2: Meteo norms complete for current year
        norms_present = dashboard['P_norm'].notna().sum()
        assert norms_present == 365, (
            f"NO GAP for norms: all {norms_present}/365 current-year "
            f"days have norms"
        )

        # Gap 3: Operational values only for ~52+15 days
        values_present = dashboard['P'].notna().sum()
        days_with_obs = (today - date(2026, 1, 1)).days + 1  # 52
        days_with_forecast = 15
        expected_values = days_with_obs + days_with_forecast
        assert values_present == expected_values, (
            f"Operational values: {values_present} days have values "
            f"({days_with_obs} observed + {days_with_forecast} forecast). "
            f"Remaining {365 - values_present} days have value=None."
        )

        # Gap 4: Snow accumulation gap
        # CSV accumulates ~365 days, API gets 2 (yesterday + today)
        assert snow_api_records == 2, (
            f"GAP 4: Snow API has {snow_api_records} records vs. "
            f"~365 in CSV. Historical snow requires maintenance script."
        )
