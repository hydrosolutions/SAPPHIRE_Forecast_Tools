"""Tests for src/long_term_range_stats.py.

Synthetic station code '19999'; no real station codes or discharge values.
Reference window: 2000-2025 (26 years, min_eligible=13) unless noted.
"""

from __future__ import annotations

import calendar
import datetime as dt
import importlib.util as _iutil
import math
import os
import sys

import pytest

_ltr_spec = _iutil.spec_from_file_location(
    "_ltr_long_term_range_stats",
    os.path.join(os.path.dirname(__file__), "..", "src", "long_term_range_stats.py"),
)
_ltr = _iutil.module_from_spec(_ltr_spec)
sys.modules["_ltr_long_term_range_stats"] = _ltr
_ltr_spec.loader.exec_module(_ltr)
QUARTER_MONTHS = _ltr.QUARTER_MONTHS
SEASON_MONTHS = _ltr.SEASON_MONTHS
PeriodStats = _ltr.PeriodStats
StationRangeStats = _ltr.StationRangeStats
_accepted_yearly_quarterly_means = _ltr._accepted_yearly_quarterly_means
_accepted_yearly_seasonal_means = _ltr._accepted_yearly_seasonal_means
_get_year_month = _ltr._get_year_month
_iter_rows = _ltr._iter_rows
_monthly_mean_50pct = _ltr._monthly_mean_50pct
compute_period_stats = _ltr.compute_period_stats
compute_station_range_stats = _ltr.compute_station_range_stats
get_reference_end_year = _ltr.get_reference_end_year
get_reference_start_year = _ltr.get_reference_start_year
min_eligible_years = _ltr.min_eligible_years
reference_years = _ltr.reference_years
validate_reference_window = _ltr.validate_reference_window
del _iutil, _ltr_spec, _ltr

CODE = "19999"
_REF_YEARS_26 = list(range(2000, 2026))  # 2000-2025 inclusive


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _daily_rows_full_month(year, month, value):
    """Full-coverage daily rows for one month with a constant discharge value."""
    days = calendar.monthrange(year, month)[1]
    return [
        {
            "code": CODE,
            "date": dt.date(year, month, day).isoformat(),
            "discharge": value,
        }
        for day in range(1, days + 1)
    ]


def _daily_rows_full_year(year, value_by_month):
    """Full-coverage daily rows for one year."""
    rows = []
    for month in range(1, 13):
        rows.extend(_daily_rows_full_month(year, month, value_by_month[month]))
    return rows


def _full_year_data(ref_years, value_per_year_month):
    """Build daily_by_year dict. value_per_year_month: {year: {month: value}}."""
    return {
        year: _daily_rows_full_year(year, value_per_year_month[year])
        for year in ref_years
        if year in value_per_year_month
    }


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def test_get_reference_start_year_default(monkeypatch):
    monkeypatch.delenv("SAPPHIRE_RANGE_REFERENCE_START_YEAR", raising=False)
    assert get_reference_start_year() == 2000


def test_get_reference_end_year_default(monkeypatch):
    monkeypatch.delenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", raising=False)
    assert get_reference_end_year() == 2025


def test_get_reference_start_year_from_env(monkeypatch):
    monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_START_YEAR", "1990")
    assert get_reference_start_year() == 1990


def test_get_reference_end_year_from_env(monkeypatch):
    monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "2024")
    assert get_reference_end_year() == 2024


def test_get_reference_start_year_invalid_raises(monkeypatch):
    monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_START_YEAR", "not_a_year")
    with pytest.raises(ValueError, match="malformed"):
        get_reference_start_year()


def test_get_reference_end_year_invalid_raises(monkeypatch):
    monkeypatch.setenv("SAPPHIRE_RANGE_REFERENCE_END_YEAR", "not_a_year")
    with pytest.raises(ValueError, match="malformed"):
        get_reference_end_year()


def test_validate_reference_window_valid():
    validate_reference_window(2000, 2025, 2026)  # must not raise


def test_validate_reference_window_start_equal_end_valid():
    validate_reference_window(2025, 2025, 2026)  # single-year window is valid


def test_validate_reference_window_raises_when_start_greater_than_end():
    with pytest.raises(ValueError, match="must be <="):
        validate_reference_window(2025, 2000, 2027)


def test_validate_reference_window_raises_when_end_equals_today_year():
    with pytest.raises(ValueError, match="strictly less than"):
        validate_reference_window(2000, 2026, 2026)


def test_validate_reference_window_raises_when_end_exceeds_today_year():
    with pytest.raises(ValueError, match="strictly less than"):
        validate_reference_window(2000, 2027, 2026)


def test_validate_reference_window_raises_on_start_below_min_year():
    # 0 is below dt.MINYEAR (1)
    with pytest.raises(ValueError, match="outside the valid range"):
        validate_reference_window(0, 2025, 2026)


def test_validate_reference_window_raises_on_end_above_max_year():
    # 10000 is above dt.MAXYEAR (9999)
    with pytest.raises(ValueError, match="outside the valid range"):
        validate_reference_window(2000, 10000, 2026)


def test_min_eligible_years_26_years():
    # 26 years: ceil(26/2) = 13 >= 2
    assert min_eligible_years(26) == 13


def test_min_eligible_years_odd_rounds_up():
    # 25 years: ceil(25/2) = ceil(12.5) = 13
    assert min_eligible_years(25) == 13
    # 27 years: ceil(27/2) = ceil(13.5) = 14
    assert min_eligible_years(27) == 14


def test_min_eligible_years_minimum_two():
    # Very short windows: at least 2 regardless
    assert min_eligible_years(1) == 2
    assert min_eligible_years(2) == 2
    assert min_eligible_years(3) == 2


def test_reference_years_inclusive():
    assert reference_years(2000, 2025) == list(range(2000, 2026))
    assert reference_years(2024, 2025) == [2024, 2025]
    assert reference_years(2024, 2024) == [2024]


# ---------------------------------------------------------------------------
# _iter_rows
# ---------------------------------------------------------------------------


def test_iter_rows_none_returns_empty():
    assert _iter_rows(None) == []


def test_iter_rows_list_passes_through():
    rows = [{"date": "2025-01-01", "discharge": 1.0}]
    assert _iter_rows(rows) is rows


def test_iter_rows_non_iterable_returns_empty():
    assert _iter_rows(42) == []


# ---------------------------------------------------------------------------
# _get_year_month
# ---------------------------------------------------------------------------


def test_get_year_month_from_iso_string():
    assert _get_year_month("2025-03-15") == (2025, 3)


def test_get_year_month_from_date_object():
    assert _get_year_month(dt.date(2025, 3, 15)) == (2025, 3)


def test_get_year_month_from_datetime_object():
    assert _get_year_month(dt.datetime(2025, 3, 15, 12, 0)) == (2025, 3)


def test_get_year_month_malformed_returns_none():
    assert _get_year_month("not_a_date") == (None, None)
    assert _get_year_month(None) == (None, None)


# ---------------------------------------------------------------------------
# _monthly_mean_50pct
# ---------------------------------------------------------------------------


def test_monthly_mean_50pct_full_coverage():
    rows = _daily_rows_full_month(2025, 3, 10.0)
    assert _monthly_mean_50pct(rows, 2025, 3) == pytest.approx(10.0)


def test_monthly_mean_50pct_exactly_50pct_passes():
    # 28-day February: 14 days = exactly 50%
    rows = [
        {"date": dt.date(2025, 2, day).isoformat(), "discharge": 5.0}
        for day in range(1, 15)  # 14 days
    ]
    result = _monthly_mean_50pct(rows, 2025, 2)
    assert result == pytest.approx(5.0)


def test_monthly_mean_50pct_below_50pct_returns_none():
    # 28-day February: 13 days < 50%
    rows = [
        {"date": dt.date(2025, 2, day).isoformat(), "discharge": 5.0}
        for day in range(1, 14)  # 13 days
    ]
    assert _monthly_mean_50pct(rows, 2025, 2) is None


def test_monthly_mean_50pct_leap_year_february():
    # Feb 2024 (leap): 29 days, threshold = 14.5; need >= 15 days
    rows_14 = [
        {"date": dt.date(2024, 2, day).isoformat(), "discharge": 3.0}
        for day in range(1, 15)  # 14 days < 14.5
    ]
    rows_15 = [
        {"date": dt.date(2024, 2, day).isoformat(), "discharge": 3.0}
        for day in range(1, 16)  # 15 days >= 14.5
    ]
    assert _monthly_mean_50pct(rows_14, 2024, 2) is None
    assert _monthly_mean_50pct(rows_15, 2024, 2) == pytest.approx(3.0)


def test_monthly_mean_50pct_excludes_nonfinite():
    # 31-day January: mix of finite and nonfinite
    rows = [{"date": dt.date(2025, 1, 1).isoformat(), "discharge": float("nan")}] * 20
    rows += [
        {"date": dt.date(2025, 1, day).isoformat(), "discharge": 5.0} for day in range(21, 32)
    ]  # 11 finite values
    # 11 < 15.5 (50% of 31) → None
    assert _monthly_mean_50pct(rows, 2025, 1) is None


def test_monthly_mean_50pct_excludes_inf_and_neg_inf():
    rows = [
        {"date": dt.date(2025, 6, day).isoformat(), "discharge": float("inf")}
        for day in range(1, 31)  # 30 'infinite' values, only finite ones count
    ]
    # 0 finite values < 15 → None
    assert _monthly_mean_50pct(rows, 2025, 6) is None


def test_monthly_mean_50pct_ignores_wrong_month():
    rows = _daily_rows_full_month(2025, 4, 7.0)  # April data
    assert _monthly_mean_50pct(rows, 2025, 3) is None  # Asked for March


def test_monthly_mean_50pct_empty_returns_none():
    assert _monthly_mean_50pct([], 2025, 1) is None


def test_monthly_mean_50pct_none_returns_none():
    assert _monthly_mean_50pct(None, 2025, 1) is None


def test_monthly_mean_50pct_deduplicates_calendar_dates():
    # 1 unique calendar day repeated 16 times is still only 1 distinct day,
    # which is far below the 50% threshold for January (15.5 days).
    rows = [{"date": dt.date(2025, 1, 1).isoformat(), "discharge": 10.0}] * 16
    assert _monthly_mean_50pct(rows, 2025, 1) is None


def test_monthly_mean_50pct_deduplication_does_not_skew_equal_means():
    # 20 rows all for day 15 with value 7.0: deduplicated → 1 day.
    # Then 15 rows for distinct days 1-15 with value 7.0: 15 unique days >= 15.5? No, 15 < 15.5.
    # Use 16 distinct days to pass, plus duplicates on day 1 → mean must stay 7.0.
    rows = [{"date": dt.date(2025, 1, 1).isoformat(), "discharge": 7.0}] * 20
    rows += [
        {"date": dt.date(2025, 1, day).isoformat(), "discharge": 7.0}
        for day in range(2, 18)  # days 2-17: 16 more unique days
    ]
    # 17 unique days (1 + 16) >= 15.5 → passes; mean = 7.0
    result = _monthly_mean_50pct(rows, 2025, 1)
    assert result == pytest.approx(7.0)


# ---------------------------------------------------------------------------
# compute_period_stats
# ---------------------------------------------------------------------------


def test_compute_period_stats_known_std():
    # 3 values: 10, 20, 30. mean=20, variance=((−10)²+(0)²+(10)²)/2=100, std=10
    accepted = {2000: 10.0, 2001: 20.0, 2002: 30.0}
    result = compute_period_stats(accepted, min_years=2)
    assert result.count == 3
    assert result.std == pytest.approx(10.0)


def test_compute_period_stats_two_values():
    # 2 values: 5 and 15. std = sqrt(((5-10)²+(15-10)²)/1) = sqrt(50) ≈ 7.071
    accepted = {2000: 5.0, 2001: 15.0}
    result = compute_period_stats(accepted, min_years=2)
    assert result.count == 2
    assert result.std == pytest.approx(math.sqrt(50), rel=1e-9)


def test_compute_period_stats_constant_history_std_zero():
    # All values identical: std = 0, not None (genuine zero variability)
    accepted = {year: 42.0 for year in range(2000, 2015)}
    result = compute_period_stats(accepted, min_years=13)
    assert result.count == 15
    assert result.std == pytest.approx(0.0)


def test_compute_period_stats_singleton_returns_none():
    result = compute_period_stats({2025: 100.0}, min_years=2)
    assert result.std is None
    assert result.count == 1


def test_compute_period_stats_empty_returns_none():
    result = compute_period_stats({}, min_years=2)
    assert result.std is None
    assert result.count == 0


def test_compute_period_stats_12_of_26_unavailable():
    # 12 accepted years out of 26-year window → min_eligible=13 → insufficient
    accepted = {year: 50.0 for year in range(2000, 2012)}  # 12 years
    result = compute_period_stats(accepted, min_years=13)
    assert result.std is None
    assert result.count == 12


def test_compute_period_stats_13_of_26_qualifies():
    # 13 accepted years → exactly meets threshold → valid std
    accepted = {year: float(year - 2000) for year in range(2000, 2013)}
    result = compute_period_stats(accepted, min_years=13)
    assert result.std is not None
    assert result.count == 13


def test_compute_period_stats_min_years_boundary_exact():
    # min_years=2, 2 accepted values → valid
    result = compute_period_stats({2000: 1.0, 2001: 3.0}, min_years=2)
    assert result.std == pytest.approx(math.sqrt(2.0))
    assert result.count == 2


# ---------------------------------------------------------------------------
# _accepted_yearly_quarterly_means
# ---------------------------------------------------------------------------


def test_accepted_yearly_quarterly_requires_all_3_months():
    # Year 2025 has Jan/Feb but not March → Q1 not accepted
    monthly_means = {
        2025: {1: 10.0, 2: 20.0},  # missing March
        2024: {1: 10.0, 2: 20.0, 3: 30.0},  # all present
    }
    result = _accepted_yearly_quarterly_means(monthly_means, 1)
    assert 2025 not in result
    assert 2024 in result
    assert result[2024] == pytest.approx((10.0 + 20.0 + 30.0) / 3)


def test_accepted_yearly_quarterly_equal_weight():
    # Q2 = Apr(4), May(5), Jun(6)
    monthly_means = {2020: {4: 100.0, 5: 200.0, 6: 300.0}}
    result = _accepted_yearly_quarterly_means(monthly_means, 2)
    assert result[2020] == pytest.approx(200.0)


# ---------------------------------------------------------------------------
# _accepted_yearly_seasonal_means
# ---------------------------------------------------------------------------


def test_accepted_yearly_seasonal_requires_3_of_6_months():
    # 2 months → rejected
    monthly_means = {2025: {4: 10.0, 5: 20.0}}
    result = _accepted_yearly_seasonal_means(monthly_means)
    assert 2025 not in result


def test_accepted_yearly_seasonal_exactly_3_of_6_accepted():
    monthly_means = {2025: {4: 10.0, 5: 20.0, 6: 30.0}}  # 3 of 6
    result = _accepted_yearly_seasonal_means(monthly_means)
    assert 2025 in result
    assert result[2025] == pytest.approx(20.0)


def test_accepted_yearly_seasonal_equal_weight_non_contiguous():
    # Only Apr, Jul, Sep present (3 of 6, not contiguous)
    monthly_means = {2025: {4: 12.0, 7: 24.0, 9: 36.0}}
    result = _accepted_yearly_seasonal_means(monthly_means)
    assert 2025 in result
    assert result[2025] == pytest.approx((12.0 + 24.0 + 36.0) / 3)


def test_accepted_yearly_seasonal_full_coverage_all_6():
    monthly_means = {2025: {m: float(m) for m in SEASON_MONTHS}}
    result = _accepted_yearly_seasonal_means(monthly_means)
    assert 2025 in result
    expected = sum(float(m) for m in SEASON_MONTHS) / 6
    assert result[2025] == pytest.approx(expected)


# ---------------------------------------------------------------------------
# compute_station_range_stats — integration
# ---------------------------------------------------------------------------


def test_compute_station_range_stats_known_monthly_std():
    # 13 years of data for January, increasing by 1 each year
    # values: 0,1,2,...,12. sample std = sqrt(sum((i-6)^2 for i in 0..12)/12)
    ref_years = list(range(2000, 2013))  # 13 years
    daily_by_year = {
        year: _daily_rows_full_month(year, 1, float(year - 2000)) for year in ref_years
    }
    min_years = min_eligible_years(13)
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    ms = stats.monthly[1]
    assert ms.count == 13
    values = list(range(13))
    mean = sum(values) / 13
    expected_std = math.sqrt(sum((v - mean) ** 2 for v in values) / 12)
    assert ms.std == pytest.approx(expected_std, rel=1e-9)


def test_compute_station_range_stats_insufficient_history_12_of_26():
    # 12 valid years → std=None, count=12 for monthly months with data
    ref_years = _REF_YEARS_26  # 2000-2025
    min_years = min_eligible_years(26)  # = 13
    daily_by_year = {
        year: _daily_rows_full_month(year, 6, float(year))
        for year in ref_years[:12]  # only first 12 years have June data
    }
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    ms_june = stats.monthly[6]
    assert ms_june.std is None
    assert ms_june.count == 12


def test_compute_station_range_stats_exactly_13_of_26_qualifies():
    ref_years = _REF_YEARS_26
    min_years = min_eligible_years(26)  # = 13
    # 13 years with constant value → std=0
    daily_by_year = {year: _daily_rows_full_month(year, 7, 42.0) for year in ref_years[:13]}
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    ms_july = stats.monthly[7]
    assert ms_july.std == pytest.approx(0.0)
    assert ms_july.count == 13


def test_compute_station_range_stats_missing_years_in_denominator():
    # 20 years provided but only 13 have valid data for April
    # Missing years still count against eligibility denominator
    ref_years = list(range(2000, 2026))  # 26 years
    min_years = min_eligible_years(26)  # = 13
    daily_by_year = {
        year: _daily_rows_full_month(year, 4, float(year))
        for year in ref_years[:13]  # 13 years have April data
    }
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    # 13 valid years = exactly meets threshold
    ms_april = stats.monthly[4]
    assert ms_april.std is not None
    assert ms_april.count == 13


def test_compute_station_range_stats_empty_history_all_months():
    ref_years = _REF_YEARS_26
    stats = compute_station_range_stats({}, ref_years, min_eligible_years(26))
    for month in range(1, 13):
        assert stats.monthly[month].std is None
        assert stats.monthly[month].count == 0
    for quarter in range(1, 5):
        assert stats.quarterly[quarter].std is None
        assert stats.quarterly[quarter].count == 0
    assert stats.seasonal is not None
    assert stats.seasonal.std is None
    assert stats.seasonal.count == 0


def test_compute_station_range_stats_quarterly_requires_all_3_months():
    # Only provide Jan and Feb for Q1 — all years have Jan/Feb but not Mar
    ref_years = list(range(2000, 2015))  # 15 years
    min_years = min_eligible_years(15)  # = max(2, ceil(7.5)) = 8
    daily_by_year = {
        year: (
            _daily_rows_full_month(year, 1, float(year))
            + _daily_rows_full_month(year, 2, float(year))
        )
        for year in ref_years
    }
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    # Q1 requires Jan+Feb+Mar all valid — Mar is missing → all Q1 years rejected
    q1 = stats.quarterly[1]
    assert q1.std is None
    assert q1.count == 0


def test_compute_station_range_stats_seasonal_3_vs_2_months():
    # Only provide Apr and May (2 months) for each year → season rejected
    ref_years = list(range(2000, 2015))
    min_years = min_eligible_years(15)
    daily_by_year = {
        year: (
            _daily_rows_full_month(year, 4, float(year))
            + _daily_rows_full_month(year, 5, float(year))
        )
        for year in ref_years
    }
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    # 2 of 6 Apr-Sep months → below SEASON_MIN_ACCEPTED_MONTHS=3 → no seasonal year accepted
    assert stats.seasonal.std is None
    assert stats.seasonal.count == 0

    # Now add Jun → 3 of 6 months → qualifies
    daily_by_year_3mo = {
        year: (
            _daily_rows_full_month(year, 4, float(year))
            + _daily_rows_full_month(year, 5, float(year))
            + _daily_rows_full_month(year, 6, float(year))
        )
        for year in ref_years
    }
    stats3 = compute_station_range_stats(daily_by_year_3mo, ref_years, min_years)
    # All 15 years qualify (3 of 6 Apr-Sep months) and 15 >= min_eligible=8 → valid std
    assert stats3.seasonal.count == len(ref_years)
    assert stats3.seasonal.std is not None


def test_compute_station_range_stats_seasonal_only_apr_sep():
    # Season is strictly Apr-Sep; Oct-Mar data should NOT contribute to seasonal stats
    ref_years = list(range(2000, 2015))
    min_years = min_eligible_years(15)
    daily_by_year = {
        year: (
            # Only Oct-Mar (outside Apr-Sep season)
            _daily_rows_full_month(year, 10, float(year))
            + _daily_rows_full_month(year, 11, float(year))
            + _daily_rows_full_month(year, 12, float(year))
            + _daily_rows_full_month(year, 1, float(year + 1))
        )
        for year in ref_years
    }
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    assert stats.seasonal.count == 0  # no Apr-Sep data


def test_compute_station_range_stats_duplicate_dates_do_not_inflate_count():
    # Duplicate date rows within a year are not deduplicated at this level;
    # monthly mean counts them as multiple measurements, not duplicate years.
    # Duplicate YEARS are impossible (dict keys). The eligible-year count
    # is bounded by len(ref_years).
    ref_years = [2020, 2021]
    min_years = 2
    rows_2020 = _daily_rows_full_month(2020, 3, 10.0)
    rows_2020 += _daily_rows_full_month(2020, 3, 10.0)  # duplicate month rows
    rows_2021 = _daily_rows_full_month(2021, 3, 20.0)
    daily_by_year = {2020: rows_2020, 2021: rows_2021}
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    # Duplicate rows inflate coverage but mean is still correct (all 10.0 for 2020)
    # Year count is 2 (not inflated): {2020: 10.0, 2021: 20.0}
    ms = stats.monthly[3]
    assert ms.count == 2
    assert ms.std == pytest.approx(math.sqrt(50.0))  # std of [10, 20]


def test_compute_station_range_stats_reference_cutoff_exact():
    # Strictly uses ref_years; a year outside the window is not read.
    ref_years = [2000, 2001]
    min_years = 2
    # Provide data for 2002 (outside window) but not in daily_by_year for ref_years
    daily_by_year = {
        2000: _daily_rows_full_month(2000, 1, 5.0),
        2001: _daily_rows_full_month(2001, 1, 15.0),
        2002: _daily_rows_full_month(2002, 1, 999.0),  # outside window
    }
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    ms = stats.monthly[1]
    # Only 2000 and 2001 are in ref_years; 2002 is not iterated
    assert ms.count == 2
    assert ms.std == pytest.approx(math.sqrt(50.0))


def test_compute_station_range_stats_nonfinite_observations_excluded():
    ref_years = [2020]
    min_years = 2
    rows = []
    # January: 20 finite rows + 11 NaN rows — total 31 days
    for day in range(1, 21):
        rows.append(
            {
                "date": dt.date(2020, 1, day).isoformat(),
                "discharge": 5.0,
            }
        )
    for day in range(21, 32):
        rows.append(
            {
                "date": dt.date(2020, 1, day).isoformat(),
                "discharge": float("nan"),
            }
        )
    daily_by_year = {2020: rows}
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    # 20 finite out of 31 days = 64.5% ≥ 50% → valid mean
    # count=1 (only year 2020) < min_years=2 → std=None
    ms = stats.monthly[1]
    assert ms.count == 1
    assert ms.std is None  # only 1 year, below min_years=2


def test_compute_station_range_stats_returns_station_range_stats():
    ref_years = [2020, 2021]
    daily_by_year = {
        2020: _daily_rows_full_year(2020, {m: float(m) for m in range(1, 13)}),
        2021: _daily_rows_full_year(2021, {m: float(m) + 10 for m in range(1, 13)}),
    }
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years=2)
    assert isinstance(stats, StationRangeStats)
    assert set(stats.monthly.keys()) == set(range(1, 13))
    assert set(stats.quarterly.keys()) == {1, 2, 3, 4}
    assert stats.seasonal is not None


def test_compute_station_range_stats_period_stats_type():
    ref_years = [2020, 2021]
    daily_by_year = {
        2020: _daily_rows_full_year(2020, {m: 10.0 for m in range(1, 13)}),
        2021: _daily_rows_full_year(2021, {m: 20.0 for m in range(1, 13)}),
    }
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years=2)
    assert all(isinstance(ps, PeriodStats) for ps in stats.monthly.values())
    assert all(isinstance(ps, PeriodStats) for ps in stats.quarterly.values())
    assert isinstance(stats.seasonal, PeriodStats)


def test_compute_station_range_stats_monthly_std_unrounded():
    # Std should be stored without rounding (the dashboard derives delta from it)
    ref_years = [2000, 2001, 2002]
    min_years = 2
    # March values: 1, 2, 3 — exact std = sqrt(1.0)
    daily_by_year = {
        year: _daily_rows_full_month(year, 3, float(year - 1999)) for year in ref_years
    }
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    ms = stats.monthly[3]
    assert ms.std == pytest.approx(1.0, rel=1e-9)
    # Verify it's a float, not rounded
    assert isinstance(ms.std, float)


def test_compute_station_range_stats_seasonal_all_6_months():
    # Full Apr-Sep data for 13 years, constant 50 m³/s → std=0
    ref_years = list(range(2000, 2013))
    min_years = min_eligible_years(13)
    daily_by_year = {
        year: sum((_daily_rows_full_month(year, m, 50.0) for m in SEASON_MONTHS), [])
        for year in ref_years
    }
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    assert stats.seasonal.std == pytest.approx(0.0)
    assert stats.seasonal.count == 13


def test_compute_station_range_stats_seasonal_unavailable_other_seasons():
    # Stage 1 only publishes Apr-Sep; non-Apr-Sep seasons produce std=None.
    # This is implicit: months outside SEASON_MONTHS don't participate in
    # seasonal std computation regardless of data availability.
    ref_years = list(range(2000, 2015))
    min_years = min_eligible_years(15)
    # Provide only Oct-Mar data (no Apr-Sep data at all)
    daily_by_year = {
        year: sum((_daily_rows_full_month(year, m, 10.0) for m in [1, 2, 3, 10, 11, 12]), [])
        for year in ref_years
    }
    stats = compute_station_range_stats(daily_by_year, ref_years, min_years)
    # Seasonal accepted means = 0 years → std=None
    assert stats.seasonal.std is None
    assert stats.seasonal.count == 0
