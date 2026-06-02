import datetime as dt
import math
import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import sync_long_horizon_hydrograph as sync_lhh

TEST_CODE = "19999"


def _daily_rows(year, month_values):
    rows = []
    for month, values in month_values.items():
        for day, value in enumerate(values, start=1):
            rows.append(
                {
                    "code": TEST_CODE,
                    "date": dt.date(year, month, day).isoformat(),
                    "discharge": value,
                }
            )
    return rows


def _full_year_rows(year, value_by_month):
    rows = []
    for month in range(1, 13):
        days = sync_lhh.calendar.monthrange(year, month)[1]
        value = value_by_month[month]
        rows.extend(_daily_rows(year, {month: [value] * days}))
    return rows


def _norms():
    return [float(month) for month in range(1, 13)]


def _records(
    daily_current_year,
    daily_previous_year,
    target_year=2026,
    today=dt.date(2027, 1, 1),
    norms=None,
):
    return sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms() if norms is None else norms,
        daily_current_year=daily_current_year,
        daily_previous_year=daily_previous_year,
        target_year=target_year,
        today=today,
    )


def _record_for_month(records, month):
    return next(record for record in records if record["horizon_value"] == month)


def test_writes_full_triad_with_complete_data():
    previous_values = {month: month * 10.0 for month in range(1, 13)}
    current_values = {month: month * 20.0 for month in range(1, 13)}

    records = _records(
        daily_current_year=_full_year_rows(2026, current_values),
        daily_previous_year=_full_year_rows(2025, previous_values),
    )

    assert len(records) == 12
    for month, record in enumerate(records, start=1):
        assert record["horizon_type"] == "month"
        assert record["code"] == TEST_CODE
        assert record["date"] == f"2026-{month:02d}-01"
        assert record["horizon_value"] == month
        assert record["horizon_in_year"] == month
        assert record["norm"] == float(month)
        assert record["previous"] == previous_values[month]
        assert record["current"] == current_values[month]


def test_one_year_missing_writes_only_other_field():
    current_values = {month: month * 20.0 for month in range(1, 13)}

    records = _records(
        daily_current_year=_full_year_rows(2026, current_values),
        daily_previous_year=[],
    )

    assert len(records) == 12
    for month, record in enumerate(records, start=1):
        assert record["previous"] is None
        assert record["current"] == current_values[month]


def test_current_is_none_for_in_progress_month():
    today = dt.date(2026, 6, 15)
    current_values = {month: month * 20.0 for month in range(1, 6)}
    current_rows = []
    for month, value in current_values.items():
        days = sync_lhh.calendar.monthrange(2026, month)[1]
        current_rows.extend(_daily_rows(2026, {month: [value] * days}))
    current_rows.extend(_daily_rows(2026, {6: [120.0] * 30}))

    records = _records(
        daily_current_year=current_rows,
        daily_previous_year=[],
        today=today,
    )

    for month in range(1, 6):
        assert _record_for_month(records, month)["current"] == current_values[month]
    assert _record_for_month(records, 6)["current"] is None
    for month in range(7, 13):
        assert _record_for_month(records, month)["current"] is None


def test_monthly_mean_below_threshold_writes_none():
    sparse_january = [10.0] * 24 + [None] * 7
    previous_rows = _daily_rows(2025, {1: sparse_january})
    current_rows = _daily_rows(2026, {1: [20.0] * 31})

    records = _records(
        daily_current_year=current_rows,
        daily_previous_year=previous_rows,
    )

    january = _record_for_month(records, 1)
    assert january["norm"] == 1.0
    assert january["previous"] is None
    assert january["current"] == 20.0


@pytest.mark.parametrize(
    ("year", "month", "finite_count"),
    [
        (2025, 1, 25),
        (2025, 4, 24),
        (2025, 2, 23),
    ],
)
def test_monthly_mean_at_threshold_writes_value(year, month, finite_count):
    values = [float(month)] * finite_count
    previous_rows = _daily_rows(year, {month: values})

    records = _records(
        daily_current_year=[],
        daily_previous_year=previous_rows,
    )

    assert _record_for_month(records, month)["previous"] == float(month)


def test_writes_none_when_daily_series_contains_nan():
    non_finite_values = [5.0] * 5 + [float("nan")] * 9 + [float("inf")] * 8 + [float("-inf")] * 9
    sparse_rows = _daily_rows(2025, {1: non_finite_values})

    sparse_records = _records(
        daily_current_year=[],
        daily_previous_year=sparse_rows,
    )

    sparse_previous = _record_for_month(sparse_records, 1)["previous"]
    assert sparse_previous is None
    assert sparse_previous != "NaN"

    finite_values = [7.0] * 25 + [float("nan")] * 6
    finite_rows = _daily_rows(2025, {1: finite_values})
    finite_records = _records(
        daily_current_year=[],
        daily_previous_year=finite_rows,
    )

    finite_previous = _record_for_month(finite_records, 1)["previous"]
    assert finite_previous == 7.0
    assert math.isfinite(finite_previous)
    assert sync_lhh._json_safe(float("nan")) is None
    assert sync_lhh._json_safe(float("inf")) is None
    assert sync_lhh._json_safe(float("-inf")) is None


def test_idempotent_writes_with_identical_upstream():
    sdk = MagicMock()
    sdk.get_norm_for_site.return_value = _norms()
    client = MagicMock()
    client.read_runoff.side_effect = [
        _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
        _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
    ]

    first = sync_lhh.write_station_monthly_hydrograph(
        TEST_CODE,
        sdk,
        client,
        target_year=2026,
        today=dt.date(2026, 12, 31),
    )
    second = sync_lhh.write_station_monthly_hydrograph(
        TEST_CODE,
        sdk,
        client,
        target_year=2026,
        today=dt.date(2026, 12, 31),
    )

    # Service-side upsert logging is covered by the API service; this unit
    # invariant keeps the posted payload stable for identical upstream inputs.
    assert second == first
    assert client.write_hydrograph.call_count == 2
    assert client.write_hydrograph.call_args_list[0].args[0] == first
    assert client.write_hydrograph.call_args_list[1].args[0] == second


def test_calendar_days_used_for_february_non_leap():
    records_with_coverage = _records(
        daily_current_year=[],
        daily_previous_year=_daily_rows(2025, {2: [2.0] * 23}),
    )
    assert _record_for_month(records_with_coverage, 2)["previous"] == 2.0

    records_below_threshold = _records(
        daily_current_year=[],
        daily_previous_year=_daily_rows(2025, {2: [2.0] * 22}),
    )
    assert _record_for_month(records_below_threshold, 2)["previous"] is None
