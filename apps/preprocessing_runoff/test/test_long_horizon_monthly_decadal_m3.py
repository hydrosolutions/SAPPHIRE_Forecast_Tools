"""M3 locked tests: iEH-HF rounded-aggregation actuals for month/quarter/season.

Fake code "19999"; no real discharge values. Locks in the new
``from_decadal`` monthly aggregation rules (``build_monthly_records``), the
quarter/season round_3sf-on-actuals-only change (``build_quarterly_records``/
``build_seasonal_record``), and the actuals-only invariant: norm/date/
day_of_year/horizon_value/horizon_in_year never move when the aggregation
method changes -- only current/previous do.
"""

import datetime as dt
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
# sync_lhh's own module-level import already inserted the iEasyHydroForecast
# dir onto sys.path, so this resolves without a separate path.insert here.
import forecast_library as fl
import sync_long_horizon_hydrograph as sync_lhh

TEST_CODE = "19999"
TARGET_YEAR = 2026
CLOSED_TODAY = dt.date(2027, 1, 1)


def _norms():
    return [float(month) for month in range(1, 13)]


def _daily_rows_for_month(year, month, values_by_day):
    """One row per day; ``values_by_day`` is 1-indexed by list position."""
    return [
        {"code": TEST_CODE, "date": dt.date(year, month, day).isoformat(), "discharge": value}
        for day, value in enumerate(values_by_day, start=1)
    ]


def _monthly_record(month, *, norm=0.0, previous=None, current=None, year=TARGET_YEAR):
    """A minimal already-built monthly record, for feeding directly into the
    quarter/season builders without going through build_monthly_records."""
    return {
        "horizon_type": "month",
        "code": TEST_CODE,
        "date": f"{year}-{month:02d}-01",
        "day_of_year": sync_lhh.MID_MONTH_DOY[month - 1],
        "horizon_value": month,
        "horizon_in_year": month,
        "norm": norm,
        "previous": previous,
        "current": current,
    }


def _record_for_month(records, month):
    return next(record for record in records if record["horizon_value"] == month)


# ---------------------------------------------------------------------------
# From-decadal mean (S6)
# ---------------------------------------------------------------------------


def test_from_decadal_current_is_round_3sf_of_the_3_decad_mean():
    decad_current = {1: 10.0, 2: 20.0, 3: 30.0}  # month 1 -> decads 1, 2, 3

    records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms(),
        daily_current_year=[],
        daily_previous_year=[],
        target_year=TARGET_YEAR,
        today=CLOSED_TODAY,
        decad_current=decad_current,
        decad_previous=None,
        from_decadal=True,
    )

    january = _record_for_month(records, 1)
    assert january["current"] == fl.round_3sf(sum(decad_current.values()) / 3)
    assert january["current"] == 20.0


def test_from_decadal_current_rounds_a_non_round_decad_mean():
    decad_current = {4: 10.0, 5: 20.0, 6: 25.0}  # month 2 -> decads 4, 5, 6

    records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms(),
        daily_current_year=[],
        daily_previous_year=[],
        target_year=TARGET_YEAR,
        today=CLOSED_TODAY,
        decad_current=decad_current,
        decad_previous=None,
        from_decadal=True,
    )

    february = _record_for_month(records, 2)
    assert february["current"] == fl.round_3sf(sum(decad_current.values()) / 3)
    assert february["current"] == 18.3


# ---------------------------------------------------------------------------
# Missing-decad month (S17): partial coverage is NOT a 2-decad mean
# ---------------------------------------------------------------------------


def test_missing_decad_month_is_none_not_a_two_decad_mean():
    decad_current = {
        7: 10.0,
        8: 20.0,
        # decad 9 (month 3's 3rd decad) is entirely absent
        10: 10.0,
        11: 20.0,
        12: None,  # decad 12 (month 4's 3rd decad) is explicitly None
    }

    records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms(),
        daily_current_year=[],
        daily_previous_year=[],
        target_year=TARGET_YEAR,
        today=CLOSED_TODAY,
        decad_current=decad_current,
        decad_previous=None,
        from_decadal=True,
    )

    march = _record_for_month(records, 3)
    april = _record_for_month(records, 4)
    assert march["current"] is None
    assert april["current"] is None


# ---------------------------------------------------------------------------
# config=false daily (S8)
# ---------------------------------------------------------------------------


def test_from_decadal_false_uses_round_3sf_of_the_80pct_daily_mean():
    daily_values = [7.0] * 25 + [None] * 6  # 25/31 = 80.6% >= 80%
    daily_rows = _daily_rows_for_month(TARGET_YEAR, 1, daily_values)

    records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms(),
        daily_current_year=daily_rows,
        daily_previous_year=[],
        target_year=TARGET_YEAR,
        today=CLOSED_TODAY,
        from_decadal=False,
    )

    january = _record_for_month(records, 1)
    expected = fl.round_3sf(sync_lhh.monthly_mean_threshold_80(daily_values, TARGET_YEAR, 1))
    assert january["current"] == expected
    assert january["current"] == 7.0


def test_from_decadal_false_below_threshold_is_none():
    daily_values = [7.0] * 20 + [None] * 11  # 20/31 = 64.5% < 80%
    daily_rows = _daily_rows_for_month(TARGET_YEAR, 1, daily_values)

    records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms(),
        daily_current_year=daily_rows,
        daily_previous_year=[],
        target_year=TARGET_YEAR,
        today=CLOSED_TODAY,
        from_decadal=False,
    )

    january = _record_for_month(records, 1)
    assert january["current"] is None


def test_from_decadal_flag_actually_switches_the_aggregation_method():
    # Month 1 (31 days): decad 1 (days 1-10) = 10.0, decad 2 (days 11-20) =
    # 20.0, decad 3 (days 21-31, 11 days) = 30.0. The unweighted mean of the 3
    # decadal averages (20.0) differs from the day-weighted daily mean
    # (~20.3) because decad 3 is 11 days, not 10 -- proving the config
    # actually changes the aggregation method, not just its inputs.
    daily_values = [10.0] * 10 + [20.0] * 10 + [30.0] * 11
    daily_rows = _daily_rows_for_month(TARGET_YEAR, 1, daily_values)
    decad_current = {1: 10.0, 2: 20.0, 3: 30.0}

    daily_records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms(),
        daily_current_year=daily_rows,
        daily_previous_year=[],
        target_year=TARGET_YEAR,
        today=CLOSED_TODAY,
        from_decadal=False,
    )
    decadal_records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms(),
        daily_current_year=daily_rows,
        daily_previous_year=[],
        target_year=TARGET_YEAR,
        today=CLOSED_TODAY,
        decad_current=decad_current,
        decad_previous=None,
        from_decadal=True,
    )

    daily_january = _record_for_month(daily_records, 1)
    decadal_january = _record_for_month(decadal_records, 1)

    assert daily_january["current"] == fl.round_3sf(
        sync_lhh.monthly_mean_threshold_80(daily_values, TARGET_YEAR, 1)
    )
    assert daily_january["current"] == 20.3
    assert decadal_january["current"] == fl.round_3sf(sum(decad_current.values()) / 3)
    assert decadal_january["current"] == 20.0
    assert daily_january["current"] != decadal_january["current"]


# ---------------------------------------------------------------------------
# Quarter (S9)
# ---------------------------------------------------------------------------


def test_quarter_current_is_round_3sf_of_the_3_rounded_monthly_means():
    monthly_records = [
        _monthly_record(1, norm=1.0, current=10.0),
        _monthly_record(2, norm=2.0, current=20.0),
        _monthly_record(3, norm=4.0, current=25.0),
    ]

    quarters = sync_lhh.build_quarterly_records(monthly_records, TEST_CODE, TARGET_YEAR)
    q1 = _record_for_month(quarters, 1)

    expected_current = fl.round_3sf(sum(record["current"] for record in monthly_records) / 3)
    assert q1["current"] == expected_current
    assert q1["current"] == 18.3

    # norm stays the EXISTING unrounded mean of the constituent monthly norms.
    expected_norm = sum(record["norm"] for record in monthly_records) / 3
    assert q1["norm"] == expected_norm
    assert q1["norm"] == 2.3333333333333335
    assert q1["norm"] != fl.round_3sf(expected_norm)


def test_quarter_current_is_none_when_any_constituent_monthly_current_is_none():
    monthly_records = [
        _monthly_record(1, norm=1.0, current=10.0),
        _monthly_record(2, norm=2.0, current=None),
        _monthly_record(3, norm=4.0, current=25.0),
    ]

    quarters = sync_lhh.build_quarterly_records(monthly_records, TEST_CODE, TARGET_YEAR)
    q1 = _record_for_month(quarters, 1)

    assert q1["current"] is None


# ---------------------------------------------------------------------------
# Season (S9)
# ---------------------------------------------------------------------------


def test_season_current_is_round_3sf_of_the_6_rounded_monthly_means():
    monthly_records = [
        _monthly_record(4, norm=1.0, current=10.0),
        _monthly_record(5, norm=2.0, current=20.0),
        _monthly_record(6, norm=3.0, current=25.0),
        _monthly_record(7, norm=4.0, current=15.0),
        _monthly_record(8, norm=5.0, current=22.0),
        _monthly_record(9, norm=7.0, current=30.0),
    ]

    season = sync_lhh.build_seasonal_record(monthly_records, TEST_CODE, TARGET_YEAR)

    expected_current = fl.round_3sf(sum(record["current"] for record in monthly_records) / 6)
    assert season["current"] == expected_current
    assert season["current"] == 20.3

    # norm stays the EXISTING unrounded mean of the constituent monthly norms.
    expected_norm = sum(record["norm"] for record in monthly_records) / 6
    assert season["norm"] == expected_norm
    assert season["norm"] == 3.6666666666666665
    assert season["norm"] != fl.round_3sf(expected_norm)


def test_season_current_is_none_when_any_constituent_monthly_current_is_none():
    monthly_records = [
        _monthly_record(month, norm=float(month), current=10.0 * month) for month in range(4, 10)
    ]
    _record_for_month(monthly_records, 6)["current"] = None

    season = sync_lhh.build_seasonal_record(monthly_records, TEST_CODE, TARGET_YEAR)

    assert season["current"] is None


# ---------------------------------------------------------------------------
# Round-of-rounded cascade (intentional relic, not a bug)
# ---------------------------------------------------------------------------


def test_decad_to_month_to_quarter_rounding_cascade_is_the_documented_relic():
    # Month 1: decads round to 10.1 (raw mean 10.0533...).
    # Month 2: decads round to 10.0 (raw mean 10.04 exactly).
    # Month 3: decads round to 10.1 (raw mean 10.05 exactly, half-up).
    decad_values_by_month = {
        1: [10.05, 10.05, 10.06],
        2: [10.04, 10.04, 10.04],
        3: [10.05, 10.05, 10.05],
    }
    decad_current = {}
    for month, decads in decad_values_by_month.items():
        first_decad = (month - 1) * 3 + 1
        for offset, value in enumerate(decads):
            decad_current[first_decad + offset] = value

    monthly_records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms(),
        daily_current_year=[],
        daily_previous_year=[],
        target_year=TARGET_YEAR,
        today=CLOSED_TODAY,
        decad_current=decad_current,
        decad_previous=None,
        from_decadal=True,
    )
    quarters = sync_lhh.build_quarterly_records(monthly_records, TEST_CODE, TARGET_YEAR)
    q1 = _record_for_month(quarters, 1)

    monthly_currents = [_record_for_month(monthly_records, month)["current"] for month in (1, 2, 3)]
    assert monthly_currents == [10.1, 10.0, 10.1]

    cascaded_quarter_current = fl.round_3sf(sum(monthly_currents) / 3)
    assert q1["current"] == cascaded_quarter_current
    assert q1["current"] == 10.1

    # A SINGLE round straight from the 9 source decad values (never rounded
    # per-month first) gives a DIFFERENT number -- this documents that the
    # intermediate month-level round is intentional (iEH-HF parity), not
    # something to "fix" by only rounding once at the end.
    all_source_decads = [value for decads in decad_values_by_month.values() for value in decads]
    single_round_from_source = fl.round_3sf(sum(all_source_decads) / len(all_source_decads))
    assert single_round_from_source == 10.0
    assert q1["current"] != single_round_from_source


# ---------------------------------------------------------------------------
# Null-propagation cascade
# ---------------------------------------------------------------------------


def test_null_propagates_from_thin_decad_through_month_to_quarter_and_season():
    # Month 6 (decads 16, 17, 18): decad 18 is missing -> month 6 current None.
    decad_current = {16: 10.0, 17: 20.0}
    # Every OTHER month gets a real 3-decad value, so a None constituent in
    # a quarter/season can only be attributable to month 6.
    for month in range(1, 13):
        if month == 6:
            continue
        first_decad = (month - 1) * 3 + 1
        decad_current[first_decad] = 10.0
        decad_current[first_decad + 1] = 20.0
        decad_current[first_decad + 2] = 30.0

    monthly_records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms(),
        daily_current_year=[],
        daily_previous_year=[],
        target_year=TARGET_YEAR,
        today=CLOSED_TODAY,
        decad_current=decad_current,
        decad_previous=None,
        from_decadal=True,
    )
    june = _record_for_month(monthly_records, 6)
    assert june["current"] is None

    quarters = sync_lhh.build_quarterly_records(monthly_records, TEST_CODE, TARGET_YEAR)
    q2 = _record_for_month(quarters, 2)  # Apr-Jun, contains month 6
    q1 = _record_for_month(quarters, 1)  # Jan-Mar, unaffected sibling
    assert q2["current"] is None
    assert q1["current"] is not None
    assert q1["current"] == 20.0

    season = sync_lhh.build_seasonal_record(monthly_records, TEST_CODE, TARGET_YEAR)
    assert season["current"] is None


# ---------------------------------------------------------------------------
# Actuals-only invariant
# ---------------------------------------------------------------------------


def test_toggling_from_decadal_leaves_identity_fields_byte_identical():
    daily_values = [10.0] * 10 + [20.0] * 10 + [30.0] * 11
    daily_rows = _daily_rows_for_month(TARGET_YEAR, 1, daily_values)
    decad_current = {1: 10.0, 2: 20.0, 3: 30.0}
    norms = _norms()

    decadal_records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=norms,
        daily_current_year=daily_rows,
        daily_previous_year=[],
        target_year=TARGET_YEAR,
        today=CLOSED_TODAY,
        decad_current=decad_current,
        decad_previous=None,
        from_decadal=True,
    )
    daily_records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=norms,
        daily_current_year=daily_rows,
        daily_previous_year=[],
        target_year=TARGET_YEAR,
        today=CLOSED_TODAY,
        decad_current=decad_current,
        decad_previous=None,
        from_decadal=False,
    )

    identity_fields = (
        "horizon_type",
        "code",
        "date",
        "day_of_year",
        "horizon_value",
        "horizon_in_year",
        "norm",
    )
    for decadal_record, daily_record in zip(decadal_records, daily_records, strict=True):
        for field in identity_fields:
            assert decadal_record[field] == daily_record[field]

    decadal_january = _record_for_month(decadal_records, 1)
    daily_january = _record_for_month(daily_records, 1)
    assert decadal_january["current"] == 20.0
    assert daily_january["current"] == 20.3
    assert decadal_january["current"] != daily_january["current"]


# ---------------------------------------------------------------------------
# In-progress month
# ---------------------------------------------------------------------------


def test_in_progress_month_current_is_none_even_with_all_3_decads_present():
    in_progress_today = dt.date(2026, 6, 15)
    decad_current = {16: 10.0, 17: 20.0, 18: 30.0}  # month 6 -> decads 16, 17, 18
    decad_previous = {16: 5.0, 17: 6.0, 18: 7.0}

    records = sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms(),
        daily_current_year=[],
        daily_previous_year=[],
        target_year=2026,
        today=in_progress_today,
        decad_current=decad_current,
        decad_previous=decad_previous,
        from_decadal=True,
    )

    june = _record_for_month(records, 6)
    assert june["current"] is None
    # previous is unaffected by the in-progress guard (it is always closed).
    assert june["previous"] == fl.round_3sf(sum(decad_previous.values()) / 3)
    assert june["previous"] == 6.0
