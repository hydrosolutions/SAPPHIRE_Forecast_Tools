import datetime as dt
import json
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


_EMPTY_SDK_PAGE = {"count": 0, "next": None, "previous": None, "results": []}


class FakeSDK:
    def __init__(self, *payloads):
        self.payloads = list(payloads)

    def get_norm_for_site(self, code, value_field, norm_period):
        payload = self.payloads.pop(0)
        if isinstance(payload, Exception):
            raise payload
        return payload

    def get_data_values_for_site(self, filters=None, **kwargs):
        # No SDK period actuals: force the local daily-aggregation fallback in
        # sync_short_horizon_hydrograph._fetch_sdk_period_actuals and avoid the
        # code-leaking AttributeError warning that would otherwise fire.
        return dict(_EMPTY_SDK_PAGE)


class FakeHydrographClient:
    def __init__(self, runoff_by_year=None, existing_hydrograph=None):
        self.runoff_by_year = runoff_by_year or {}
        self.records_by_key = {}
        self.write_calls = []
        for record in existing_hydrograph or []:
            self.records_by_key[self._key(record)] = dict(record)

    @staticmethod
    def _key(record):
        return (record["horizon_type"], record["code"], record["date"])

    def read_runoff(self, horizon, code, start_date, end_date, limit):
        year = int(start_date[:4])
        return self.runoff_by_year.get(year, [])

    def read_hydrograph(self, horizon, code, start_date, end_date, limit):
        rows = [
            record
            for record in self.records_by_key.values()
            if record["horizon_type"] == horizon
            and record["code"] == str(code)
            and start_date <= record["date"] <= end_date
        ]
        return sync_lhh.pd.DataFrame(rows)

    def write_hydrograph(self, records):
        records = [dict(record) for record in records]
        self.write_calls.append(records)
        for record in records:
            self.records_by_key[self._key(record)] = record

    def written_records(self):
        return list(self.records_by_key.values())


class StageFailingHydrographClient(FakeHydrographClient):
    def __init__(self, *, fail_code, fail_on_call, runoff_by_year=None):
        super().__init__(runoff_by_year=runoff_by_year)
        self.fail_code = fail_code
        self.fail_on_call = fail_on_call
        self._write_call_count_by_code = {}

    def write_hydrograph(self, records):
        code = str(records[0]["code"])
        self._write_call_count_by_code[code] = self._write_call_count_by_code.get(code, 0) + 1
        if code == self.fail_code and self._write_call_count_by_code[code] == self.fail_on_call:
            raise sync_lhh.SapphireAPIError("stage write rejected", status_code=422)
        super().write_hydrograph(records)


def _records(
    daily_current_year,
    daily_previous_year,
    target_year=2026,
    today=dt.date(2027, 1, 1),
    norms=None,
    from_decadal=None,
):
    return sync_lhh.build_monthly_records(
        code=TEST_CODE,
        norms=_norms() if norms is None else norms,
        daily_current_year=daily_current_year,
        daily_previous_year=daily_previous_year,
        target_year=target_year,
        today=today,
        from_decadal=from_decadal,
    )


def _record_for_month(records, month):
    return next(record for record in records if record["horizon_value"] == month)


def _records_by_horizon(records, horizon_type):
    return [record for record in records if record["horizon_type"] == horizon_type]


def test_writes_full_triad_with_complete_data():
    previous_values = {month: month * 10.0 for month in range(1, 13)}
    current_values = {month: month * 20.0 for month in range(1, 13)}

    records = _records(
        daily_current_year=_full_year_rows(2026, current_values),
        daily_previous_year=_full_year_rows(2025, previous_values),
        from_decadal=False,
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
        from_decadal=False,
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
        from_decadal=False,
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
        from_decadal=False,
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
        from_decadal=False,
    )

    assert _record_for_month(records, month)["previous"] == float(month)


def test_writes_none_when_daily_series_contains_nan():
    non_finite_values = [5.0] * 5 + [float("nan")] * 9 + [float("inf")] * 8 + [float("-inf")] * 9
    sparse_rows = _daily_rows(2025, {1: non_finite_values})

    sparse_records = _records(
        daily_current_year=[],
        daily_previous_year=sparse_rows,
        from_decadal=False,
    )

    sparse_previous = _record_for_month(sparse_records, 1)["previous"]
    assert sparse_previous is None
    assert sparse_previous != "NaN"

    finite_values = [7.0] * 25 + [float("nan")] * 6
    finite_rows = _daily_rows(2025, {1: finite_values})
    finite_records = _records(
        daily_current_year=[],
        daily_previous_year=finite_rows,
        from_decadal=False,
    )

    finite_previous = _record_for_month(finite_records, 1)["previous"]
    assert finite_previous == 7.0
    assert math.isfinite(finite_previous)
    assert sync_lhh._json_safe(float("nan")) is None
    assert sync_lhh._json_safe(float("inf")) is None
    assert sync_lhh._json_safe(float("-inf")) is None


def test_numpy_integer_norms_are_json_safe_python_floats():
    np = pytest.importorskip("numpy")
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []})

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=FakeSDK([np.int64(month) for month in range(1, 13)]),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    monthly_norms = [record["norm"] for record in _records_by_horizon(records, "month")]
    assert monthly_norms == [float(month) for month in range(1, 13)]
    assert all(type(norm) is float for norm in monthly_norms)
    json.dumps(records)


def test_numpy_nan_existing_norm_is_written_as_none():
    np = pytest.importorskip("numpy")
    existing_months = [
        {
            "horizon_type": "month",
            "code": TEST_CODE,
            "date": f"2026-{month:02d}-01",
            "day_of_year": sync_lhh.MID_MONTH_DOY[month - 1],
            "horizon_value": month,
            "horizon_in_year": month,
            "norm": np.float64("nan") if month == 1 else np.float64(month),
            "previous": None,
            "current": None,
        }
        for month in range(1, 13)
    ]
    client = FakeHydrographClient(
        runoff_by_year={2025: [], 2026: []},
        existing_hydrograph=existing_months,
    )

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=FakeSDK([]),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    monthly_records = _records_by_horizon(records, "month")
    assert _record_for_month(monthly_records, 1)["norm"] is None
    assert _record_for_month(monthly_records, 2)["norm"] == 2.0
    json.dumps(records)


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
    assert second.records == first.records
    assert second.status is sync_lhh.LongHorizonStationWriteStatus.WRITTEN
    assert client.write_hydrograph.call_count == 2
    assert client.write_hydrograph.call_args_list[0].args[0] == first.records
    assert client.write_hydrograph.call_args_list[1].args[0] == second.records


def test_calendar_days_used_for_february_non_leap():
    records_with_coverage = _records(
        daily_current_year=[],
        daily_previous_year=_daily_rows(2025, {2: [2.0] * 23}),
        from_decadal=False,
    )
    assert _record_for_month(records_with_coverage, 2)["previous"] == 2.0

    records_below_threshold = _records(
        daily_current_year=[],
        daily_previous_year=_daily_rows(2025, {2: [2.0] * 22}),
        from_decadal=False,
    )
    assert _record_for_month(records_below_threshold, 2)["previous"] is None


def _monthly_records_for_season(target_year=2025, norm=1.0, previous=2.0, current=3.0):
    records = []
    for month in range(1, 13):
        records.append(
            {
                "horizon_type": "month",
                "code": TEST_CODE,
                "date": f"{target_year}-{month:02d}-01",
                "day_of_year": sync_lhh.MID_MONTH_DOY[month - 1],
                "horizon_value": month,
                "horizon_in_year": month,
                "norm": norm + month,
                "previous": previous + month,
                "current": current + month,
            }
        )
    return records


def test_season_writes_full_triad_from_complete_monthly():
    monthly_records = _monthly_records_for_season(target_year=2025)

    season = sync_lhh.build_seasonal_record(monthly_records, TEST_CODE, target_year=2025)
    leap_season = sync_lhh.build_seasonal_record(
        _monthly_records_for_season(target_year=2024),
        TEST_CODE,
        target_year=2024,
    )

    assert season["horizon_type"] == "season"
    assert season["code"] == TEST_CODE
    assert season["date"] == "2025-04-01"
    assert season["horizon_value"] == 1
    assert season["horizon_in_year"] == 1
    assert season["day_of_year"] == 91
    assert leap_season["day_of_year"] == 92
    assert season["norm"] == sum(1.0 + month for month in range(4, 10)) / 6
    assert season["previous"] == sum(2.0 + month for month in range(4, 10)) / 6
    assert season["current"] == sum(3.0 + month for month in range(4, 10)) / 6


@pytest.mark.parametrize("missing_field", ["norm", "previous", "current"])
def test_season_field_is_none_when_any_monthly_value_missing(missing_field):
    monthly_records = _monthly_records_for_season(target_year=2025)
    _record_for_month(monthly_records, 6)[missing_field] = None

    season = sync_lhh.build_seasonal_record(monthly_records, TEST_CODE, target_year=2025)

    assert season[missing_field] is None
    for populated_field in {"norm", "previous", "current"} - {missing_field}:
        assert season[populated_field] is not None


def test_season_writes_none_when_monthly_contains_nan():
    monthly_records = _monthly_records_for_season(target_year=2025)
    _record_for_month(monthly_records, 5)["previous"] = float("nan")

    season = sync_lhh.build_seasonal_record(monthly_records, TEST_CODE, target_year=2025)

    assert season["previous"] is None
    assert season["previous"] != "NaN"
    assert season["norm"] is not None
    assert season["current"] is not None


def test_season_horizon_identity_is_stable():
    target_year = 2025
    season = sync_lhh.build_seasonal_record(
        _monthly_records_for_season(target_year=target_year),
        TEST_CODE,
        target_year=target_year,
    )

    assert (season["horizon_type"], season["code"], season["date"]) == (
        "season",
        TEST_CODE,
        f"{target_year}-04-01",
    )


def test_season_idempotent_with_identical_monthly():
    client = MagicMock()
    monthly_records = _monthly_records_for_season(target_year=2025)

    first = sync_lhh.write_station_seasonal_hydrograph(
        TEST_CODE,
        monthly_records,
        client,
        target_year=2025,
        today=dt.date(2026, 6, 15),
    )
    second = sync_lhh.write_station_seasonal_hydrograph(
        TEST_CODE,
        monthly_records,
        client,
        target_year=2025,
        today=dt.date(2026, 6, 15),
    )

    # Service-side _has_changes=False is covered by the API service; this unit
    # invariant keeps the posted seasonal payload stable for identical inputs.
    assert second == first
    assert client.write_hydrograph.call_count == 2
    assert client.write_hydrograph.call_args_list[0].args[0] == [first]
    assert client.write_hydrograph.call_args_list[1].args[0] == [second]


def test_season_current_is_none_for_in_progress_target_year():
    in_progress_monthly = _monthly_records_for_season(target_year=2026)
    for month in range(6, 10):
        _record_for_month(in_progress_monthly, month)["current"] = None

    in_progress_season = sync_lhh.build_seasonal_record(
        in_progress_monthly,
        TEST_CODE,
        target_year=2026,
    )
    completed_season = sync_lhh.build_seasonal_record(
        _monthly_records_for_season(target_year=2025),
        TEST_CODE,
        target_year=2025,
    )

    assert in_progress_season["current"] is None
    assert completed_season["current"] is not None


def test_season_april_first_day_of_year_in_leap_year():
    season = sync_lhh.build_seasonal_record(
        _monthly_records_for_season(target_year=2024),
        TEST_CODE,
        target_year=2024,
    )

    assert season["day_of_year"] == 92


def test_quarterly_field_mean_returns_constituent_month_mean():
    monthly_records = _monthly_records_for_season(target_year=2025, norm=0.0)

    for quarter, months in sync_lhh.QUARTER_MONTHS.items():
        expected = sum(float(month) for month in months) / 3
        assert sync_lhh._quarterly_field_mean(monthly_records, quarter, "norm") == expected


def test_quarterly_field_mean_returns_none_when_constituent_month_is_none():
    monthly_records = _monthly_records_for_season(target_year=2025)
    _record_for_month(monthly_records, 2)["norm"] = None

    assert sync_lhh._quarterly_field_mean(monthly_records, 1, "norm") is None


@pytest.mark.parametrize("non_finite", [float("nan"), float("inf"), float("-inf")])
def test_quarterly_field_mean_returns_none_when_constituent_month_is_non_finite(non_finite):
    monthly_records = _monthly_records_for_season(target_year=2025)
    _record_for_month(monthly_records, 5)["previous"] = non_finite

    assert sync_lhh._quarterly_field_mean(monthly_records, 2, "previous") is None


def test_quarterly_field_mean_returns_none_when_constituent_month_is_absent():
    monthly_records = [
        record
        for record in _monthly_records_for_season(target_year=2025)
        if record["horizon_value"] != 5
    ]

    assert sync_lhh._quarterly_field_mean(monthly_records, 2, "norm") is None


@pytest.mark.parametrize(
    ("target_year", "expected_days"),
    [
        (2025, (1, 91, 182, 274)),
        (2024, (1, 92, 183, 275)),
    ],
)
def test_build_quarterly_records_dates_leap_aware_days_and_no_stat_fields(
    target_year,
    expected_days,
):
    monthly_records = _monthly_records_for_season(target_year=target_year, norm=0.0)
    stat_fields = {
        "count",
        "mean",
        "std",
        "min",
        "max",
        "q05",
        "q10",
        "q25",
        "q50",
        "q75",
        "q90",
        "q95",
    }

    records = sync_lhh.build_quarterly_records(monthly_records, TEST_CODE, target_year=target_year)

    assert [record["date"] for record in records] == [
        f"{target_year}-01-01",
        f"{target_year}-04-01",
        f"{target_year}-07-01",
        f"{target_year}-10-01",
    ]
    assert [record["day_of_year"] for record in records] == list(expected_days)
    for record in records:
        quarter = record["horizon_value"]
        expected_norm = sum(float(month) for month in sync_lhh.QUARTER_MONTHS[quarter]) / 3
        assert record["horizon_type"] == "quarter"
        assert record["horizon_in_year"] == quarter
        assert record["norm"] == pytest.approx(expected_norm, abs=1e-9)
        assert stat_fields.isdisjoint(record)


def test_quarter_current_is_none_for_in_progress_target_year():
    in_progress_monthly = _monthly_records_for_season(target_year=2026)
    for month in sync_lhh.QUARTER_MONTHS[2]:
        _record_for_month(in_progress_monthly, month)["current"] = None

    in_progress_quarters = sync_lhh.build_quarterly_records(
        in_progress_monthly,
        TEST_CODE,
        target_year=2026,
    )
    completed_quarters = sync_lhh.build_quarterly_records(
        _monthly_records_for_season(target_year=2025),
        TEST_CODE,
        target_year=2025,
    )

    assert in_progress_quarters[1]["current"] is None
    assert completed_quarters[0]["current"] is not None


def test_write_long_horizon_hydrograph_writes_quarterly_records():
    sdk = MagicMock()
    sdk.get_norm_for_site.return_value = _norms()
    client = MagicMock()
    client.read_runoff.side_effect = [
        _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
    ]

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    quarterly_records = client.write_hydrograph.call_args_list[2].args[0]
    assert len(records) == 17
    assert client.write_hydrograph.call_count == 3
    assert len(quarterly_records) == 4
    assert all(record["horizon_type"] == "quarter" for record in quarterly_records)
    for record in quarterly_records:
        quarter = record["horizon_value"]
        expected_norm = sum(float(month) for month in sync_lhh.QUARTER_MONTHS[quarter]) / 3
        assert record["norm"] == pytest.approx(expected_norm, abs=1e-9)


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ([float(month) for month in range(1, 13)], sync_lhh._NormClassification.VALID),
        (tuple(float(month) for month in range(1, 13)), sync_lhh._NormClassification.VALID),
        (None, sync_lhh._NormClassification.NORM_ABSENT),
        ([], sync_lhh._NormClassification.NORM_ABSENT),
        ([1.0] * 11, sync_lhh._NormClassification.NORM_ABSENT),
        ([1.0] * 13, sync_lhh._NormClassification.NORM_ABSENT),
        ([1.0] * 11 + [None], sync_lhh._NormClassification.NORM_ABSENT),
        ([1.0] * 11 + [float("nan")], sync_lhh._NormClassification.NORM_ABSENT),
        ([1.0] * 11 + [float("inf")], sync_lhh._NormClassification.NORM_ABSENT),
        ([1.0] * 11 + [float("-inf")], sync_lhh._NormClassification.NORM_ABSENT),
        ([1.0] * 11 + ["12.0"], sync_lhh._NormClassification.NORM_ABSENT),
        ([1.0] * 11 + [object()], sync_lhh._NormClassification.NORM_ABSENT),
        ("bare string", sync_lhh._NormClassification.NORM_ABSENT),
        ({"month": 1.0}, sync_lhh._NormClassification.NORM_ABSENT),
        (12.0, sync_lhh._NormClassification.NORM_ABSENT),
    ],
)
def test_classifies_monthly_norm_payloads(payload, expected):
    assert sync_lhh._classify_monthly_norms(payload) is expected


def test_classifies_sdk_exception_as_failed():
    result = sync_lhh._lookup_monthly_norms(
        TEST_CODE,
        FakeSDK(ConnectionError("tunnel down")),
    )

    assert result.classification is sync_lhh._NormClassification.SDK_FAILED
    assert isinstance(result.exception, ConnectionError)


def test_norm_absent_without_prior_norms_writes_all_horizons_and_local_values(monkeypatch):
    monkeypatch.setenv("SAPPHIRE_MONTHLY_FROM_DECADAL", "false")
    previous_values = {month: month * 10.0 for month in range(1, 13)}
    current_values = {month: month * 20.0 for month in range(1, 13)}
    client = FakeHydrographClient(
        runoff_by_year={
            2025: _full_year_rows(2025, previous_values),
            2026: _full_year_rows(2026, current_values),
        },
    )

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=FakeSDK([]),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    monthly_records = _records_by_horizon(records, "month")
    season_records = _records_by_horizon(records, "season")
    quarter_records = _records_by_horizon(records, "quarter")
    keys = {(record["horizon_type"], record["code"], record["date"]) for record in records}
    assert len(monthly_records) == 12
    assert len(season_records) == 1
    assert len(quarter_records) == 4
    assert len(keys) == 17
    assert records.station_statuses == [
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT)
    ]
    for month in range(1, 13):
        monthly = _record_for_month(monthly_records, month)
        assert monthly["date"] == f"2026-{month:02d}-01"
        assert monthly["horizon_value"] == month
        assert monthly["horizon_in_year"] == month
        assert monthly["norm"] is None
    assert _record_for_month(monthly_records, 2)["previous"] == sync_lhh.fl.round_3sf(20.0)
    assert _record_for_month(monthly_records, 2)["current"] == sync_lhh.fl.round_3sf(40.0)
    assert _record_for_month(monthly_records, 9)["previous"] == sync_lhh.fl.round_3sf(90.0)
    assert _record_for_month(monthly_records, 9)["current"] == sync_lhh.fl.round_3sf(180.0)
    assert season_records[0]["norm"] is None
    assert all(record["norm"] is None for record in quarter_records)


def test_norm_absent_preserves_existing_month_norms_and_derives_rollups():
    existing_months = [
        {
            "horizon_type": "month",
            "code": TEST_CODE,
            "date": f"2026-{month:02d}-01",
            "day_of_year": sync_lhh.MID_MONTH_DOY[month - 1],
            "horizon_value": month,
            "horizon_in_year": month,
            "norm": 100.0 + month,
            "previous": None,
            "current": None,
        }
        for month in range(1, 13)
    ]
    client = FakeHydrographClient(
        runoff_by_year={
            2025: _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
            2026: _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        },
        existing_hydrograph=existing_months,
    )

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=FakeSDK(None),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    monthly_records = _records_by_horizon(records, "month")
    quarter_records = _records_by_horizon(records, "quarter")
    season = _records_by_horizon(records, "season")[0]
    for month in range(1, 13):
        assert _record_for_month(monthly_records, month)["norm"] == 100.0 + month
    assert season["norm"] == pytest.approx(sum(100.0 + month for month in range(4, 10)) / 6)
    for quarter_record in quarter_records:
        quarter = quarter_record["horizon_value"]
        expected = sum(100.0 + month for month in sync_lhh.QUARTER_MONTHS[quarter]) / 3
        assert quarter_record["norm"] == pytest.approx(expected)


def test_skips_station_when_sdk_raises(caplog):
    client = MagicMock()

    with caplog.at_level(sync_lhh.logging.DEBUG):
        result = sync_lhh.write_station_monthly_hydrograph(
            TEST_CODE,
            FakeSDK(ConnectionError("tunnel down")),
            client,
            target_year=2026,
            today=dt.date(2026, 6, 15),
        )

    assert result.status is sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED
    assert result.records == []
    client.write_hydrograph.assert_not_called()
    assert "ConnectionError" in caplog.text
    assert "tunnel down" in caplog.text


# INFRA-032: the SDK-failure log for write_station_monthly_hydrograph was
# lifted from DEBUG to WARNING because the root logger is configured at
# WARNING in production (iEasyHydroForecast.setup_library), which makes this
# script's own logging.basicConfig(level=logging.INFO) a no-op. Below
# WARNING, the failing station and the reason are invisible in production
# logs. Capture at WARNING (not DEBUG) so this test would fail if the level
# regressed back to DEBUG.
def test_skips_station_when_sdk_raises_logs_at_warning_with_station_and_error(caplog):
    client = MagicMock()

    with caplog.at_level(sync_lhh.logging.WARNING):
        result = sync_lhh.write_station_monthly_hydrograph(
            TEST_CODE,
            FakeSDK(ConnectionError("tunnel down")),
            client,
            target_year=2026,
            today=dt.date(2026, 6, 15),
        )

    assert result.status is sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED
    warning_records = [
        record for record in caplog.records if record.levelno == sync_lhh.logging.WARNING
    ]
    assert len(warning_records) == 1
    message = warning_records[0].getMessage()
    assert TEST_CODE in message
    assert "ConnectionError" in message
    assert "tunnel down" in message


def test_valid_then_norm_absent_preserves_norms_but_updates_local_values_then_sdk_failed(
    monkeypatch,
):
    monkeypatch.setenv("SAPPHIRE_MONTHLY_FROM_DECADAL", "false")
    client = FakeHydrographClient(
        runoff_by_year={
            2025: _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
            2026: _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        },
    )
    valid_records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=FakeSDK(_norms()),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )
    assert valid_records.station_statuses == [
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.WRITTEN)
    ]
    assert _record_for_month(_records_by_horizon(valid_records, "month"), 5)["norm"] == 5.0

    client.runoff_by_year = {
        2025: _full_year_rows(2025, {month: 15.0 for month in range(1, 13)}),
        2026: _full_year_rows(2026, {month: 30.0 for month in range(1, 13)}),
    }
    norm_absent_records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=FakeSDK([]),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )
    month_five = _record_for_month(_records_by_horizon(norm_absent_records, "month"), 5)
    assert norm_absent_records.station_statuses == [
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT)
    ]
    assert month_five["norm"] == 5.0
    assert month_five["previous"] == sync_lhh.fl.round_3sf(15.0)
    assert month_five["current"] == sync_lhh.fl.round_3sf(30.0)

    write_call_count = len(client.write_calls)
    sdk_failed_records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=FakeSDK(ConnectionError("tunnel down")),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )
    assert sdk_failed_records == []
    assert sdk_failed_records.station_statuses == [
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED)
    ]
    assert len(client.write_calls) == write_call_count


def test_mixed_batch_carries_station_statuses():
    client = FakeHydrographClient(
        runoff_by_year={
            2025: _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
            2026: _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        },
    )

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE, TEST_CODE, TEST_CODE],
        iehhf_sdk=FakeSDK(_norms(), [], ConnectionError("tunnel down")),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    assert records.station_statuses == [
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.WRITTEN),
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT),
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED),
    ]


def test_mixed_batch_status_summary_tallies_statuses_and_total():
    records = sync_lhh._LongHorizonWriteResult()
    records.station_statuses = [
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.WRITTEN),
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT),
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED),
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.API_FAILED),
    ]

    summary = sync_lhh._summarize_long_horizon_station_statuses(records)

    assert summary.status_counts == {
        sync_lhh.LongHorizonStationWriteStatus.WRITTEN: 1,
        sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT: 1,
        sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED: 1,
        sync_lhh.LongHorizonStationWriteStatus.API_FAILED: 1,
    }
    assert summary.total_attempted == 4


@pytest.mark.parametrize(
    ("statuses", "expected_exit_code"),
    [
        ([sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED], 4),
        ([sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT], 0),
        ([sync_lhh.LongHorizonStationWriteStatus.WRITTEN], 0),
        ([sync_lhh.LongHorizonStationWriteStatus.API_FAILED], 5),
    ],
)
def test_exit_code_for_station_status_summary(statuses, expected_exit_code):
    records = sync_lhh._LongHorizonWriteResult()
    records.station_statuses = [(TEST_CODE, status) for status in statuses]
    summary = sync_lhh._summarize_long_horizon_station_statuses(records)

    assert sync_lhh._exit_code_for_long_horizon_summary(summary) == expected_exit_code


def test_degraded_summary_logs_exact_counts_only_line(caplog):
    records = sync_lhh._LongHorizonWriteResult()
    records.station_statuses = [
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.WRITTEN),
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT),
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED),
    ]
    summary = sync_lhh._summarize_long_horizon_station_statuses(records)

    with caplog.at_level(sync_lhh.logging.WARNING):
        sync_lhh._log_degraded_long_horizon_summary(summary)

    expected = (
        "DEGRADED: monthly discharge norms unavailable for 1/3 stations; "
        "observed runoff written; norm and percent-of-norm unavailable."
    )
    assert [record.message for record in caplog.records] == [expected]
    assert TEST_CODE not in expected


def test_degraded_summary_not_logged_when_no_norms_absent(caplog):
    records = sync_lhh._LongHorizonWriteResult()
    records.station_statuses = [
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.WRITTEN),
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED),
    ]
    summary = sync_lhh._summarize_long_horizon_station_statuses(records)

    with caplog.at_level(sync_lhh.logging.WARNING):
        sync_lhh._log_degraded_long_horizon_summary(summary)

    assert "DEGRADED:" not in caplog.text


def test_run_summary_artifact_is_counts_only():
    records = sync_lhh._LongHorizonWriteResult()
    records.station_statuses = [
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.WRITTEN),
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT),
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED),
    ]
    summary = sync_lhh._summarize_long_horizon_station_statuses(records)

    artifact = sync_lhh._format_long_horizon_run_summary_artifact(summary)

    assert "LONG-HORIZON RUN SUMMARY" in artifact
    assert (
        "DEGRADED: monthly discharge norms unavailable for 1/3 stations; "
        "observed runoff written; norm and percent-of-norm unavailable."
    ) in artifact
    assert "written=1" in artifact
    assert "norm_absent=1" in artifact
    assert "sdk_failed=1" in artifact
    assert "api_failed=0" in artifact
    assert TEST_CODE not in artifact


def test_api_failed_station_counts_in_summary_denominator_and_artifact():
    client = FakeHydrographClient(
        runoff_by_year={
            2025: _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
            2026: _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        },
    )
    original_write_hydrograph = client.write_hydrograph
    failed_once = False

    def fail_first_write(records):
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise sync_lhh.requests.exceptions.Timeout("hydrograph write timed out")
        original_write_hydrograph(records)

    client.write_hydrograph = fail_first_write

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE, TEST_CODE],
        iehhf_sdk=FakeSDK(_norms(), []),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )
    summary = sync_lhh._summarize_long_horizon_station_statuses(records)
    artifact = sync_lhh._format_long_horizon_run_summary_artifact(summary)

    assert records.station_statuses == [
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.API_FAILED),
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT),
    ]
    assert summary.total_attempted == 2
    assert summary.status_counts[sync_lhh.LongHorizonStationWriteStatus.API_FAILED] == 1
    assert sync_lhh._degraded_long_horizon_summary_line(summary) == (
        "DEGRADED: monthly discharge norms unavailable for 1/2 stations; "
        "observed runoff written; norm and percent-of-norm unavailable."
    )
    assert "api_failed=1" in artifact


def test_norm_absent_with_no_local_data_writes_empty_triad_rows():
    client = FakeHydrographClient(runoff_by_year={2025: [], 2026: []})

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=FakeSDK({}),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    assert len(_records_by_horizon(records, "month")) == 12
    assert len(_records_by_horizon(records, "season")) == 1
    assert len(_records_by_horizon(records, "quarter")) == 4
    for record in records:
        assert record["norm"] is None
        assert record["previous"] is None
        assert record["current"] is None


def test_norm_absent_rerun_is_idempotent_without_duplicate_keys():
    client = FakeHydrographClient(
        runoff_by_year={
            2025: _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
            2026: _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        },
    )

    first = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=FakeSDK([]),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )
    second = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=FakeSDK([]),
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    assert sorted(first, key=lambda record: (record["horizon_type"], record["date"])) == sorted(
        second,
        key=lambda record: (record["horizon_type"], record["date"]),
    )
    assert len(client.written_records()) == 17
    keys = {
        (record["horizon_type"], record["code"], record["date"])
        for record in client.written_records()
    }
    assert len(keys) == 17


def test_leap_year_february_threshold_still_uses_29_days_with_norm_absent(monkeypatch):
    monkeypatch.setenv("SAPPHIRE_MONTHLY_FROM_DECADAL", "false")
    client = FakeHydrographClient(
        runoff_by_year={
            2023: _daily_rows(2023, {2: [2.0] * 23}),
            2024: _daily_rows(2024, {2: [4.0] * 24}),
        },
    )

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[TEST_CODE],
        iehhf_sdk=FakeSDK([]),
        client=client,
        target_year=2024,
        today=dt.date(2025, 1, 1),
    )

    february = _record_for_month(_records_by_horizon(records, "month"), 2)
    assert february["previous"] == sync_lhh.fl.round_3sf(2.0)
    assert february["current"] == sync_lhh.fl.round_3sf(4.0)
    assert february["day_of_year"] == 46


def test_orchestrator_continues_after_skipped_station():
    skipped_code = "A"
    valid_code = "B"
    sdk = MagicMock()
    sdk.get_norm_for_site.side_effect = [ConnectionError("tunnel down"), _norms()]
    client = MagicMock()
    client.read_runoff.side_effect = [
        _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
    ]

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=[skipped_code, valid_code],
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    assert len(records) == 17
    assert {record["code"] for record in records} == {valid_code}
    assert client.write_hydrograph.call_count == 3
    assert len(client.write_hydrograph.call_args_list[0].args[0]) == 12
    assert len(client.write_hydrograph.call_args_list[1].args[0]) == 1
    assert len(client.write_hydrograph.call_args_list[2].args[0]) == 4


def test_orchestrator_continues_after_quarterly_api_write_failure(caplog):
    first_code = "19999"
    second_code = "19998"
    sdk = MagicMock()
    sdk.get_norm_for_site.side_effect = [_norms(), _norms()]
    client = MagicMock()
    client.read_runoff.side_effect = [
        _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
        _full_year_rows(2026, {month: 30.0 for month in range(1, 13)}),
        _full_year_rows(2025, {month: 15.0 for month in range(1, 13)}),
    ]
    write_calls = []

    def fail_third_write(_records):
        write_calls.append(_records)
        if len(write_calls) == 3:
            raise sync_lhh.SapphireAPIError("quarter rejected", status_code=422)

    client.write_hydrograph.side_effect = fail_third_write

    with caplog.at_level(sync_lhh.logging.WARNING):
        records = sync_lhh.write_long_horizon_hydrograph(
            codes=[first_code, second_code],
            iehhf_sdk=sdk,
            client=client,
            target_year=2026,
            today=dt.date(2027, 1, 1),
        )

    assert client.write_hydrograph.call_count == 6
    assert len(records) == 30
    assert "1/2 attempted station(s)" in caplog.text
    assert "19999" not in caplog.text
    assert records.attempted_station_codes == ["19999", "19998"]
    assert records.completed_station_codes == ["19998"]
    assert records.failed_station_codes == ["19999"]


def test_orchestrator_preserves_monthly_when_seasonal_api_write_fails(caplog):
    sdk = MagicMock()
    sdk.get_norm_for_site.return_value = _norms()
    client = MagicMock()
    client.read_runoff.side_effect = [
        _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
    ]
    write_calls = []

    def fail_second_write(_records):
        write_calls.append(_records)
        if len(write_calls) == 2:
            raise sync_lhh.SapphireAPIError("season rejected", status_code=422)

    client.write_hydrograph.side_effect = fail_second_write

    with caplog.at_level(sync_lhh.logging.WARNING):
        records = sync_lhh.write_long_horizon_hydrograph(
            codes=["19999"],
            iehhf_sdk=sdk,
            client=client,
            target_year=2026,
            today=dt.date(2027, 1, 1),
        )

    assert len(records) == 12
    assert client.write_hydrograph.call_count == 2
    assert "1/1 attempted station(s)" in caplog.text
    assert "19999" not in caplog.text
    assert records.attempted_station_codes == ["19999"]
    assert records.completed_station_codes == []
    assert records.failed_station_codes == ["19999"]


@pytest.mark.parametrize(
    ("fail_on_call", "expected_records"),
    [
        (2, 12),
        (3, 13),
    ],
)
def test_norm_absent_later_stage_api_failure_counts_station_once_and_keeps_denominator(
    caplog,
    fail_on_call,
    expected_records,
):
    first_code = "19999"
    second_code = "19998"
    client = StageFailingHydrographClient(
        fail_code=first_code,
        fail_on_call=fail_on_call,
        runoff_by_year={
            2025: _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
            2026: _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        },
    )

    def read_runoff(horizon, code, start_date, end_date, limit):
        year = int(start_date[:4])
        base_rows = client.runoff_by_year.get(year, [])
        return [{**row, "code": str(code)} for row in base_rows]

    client.read_runoff = read_runoff

    with caplog.at_level(sync_lhh.logging.WARNING):
        records = sync_lhh.write_long_horizon_hydrograph(
            codes=[first_code, second_code],
            iehhf_sdk=FakeSDK({}, {}),
            client=client,
            target_year=2026,
            today=dt.date(2027, 1, 1),
        )

    summary = sync_lhh._summarize_long_horizon_station_statuses(records)
    artifact = sync_lhh._format_long_horizon_run_summary_artifact(summary)

    assert len(records) == expected_records + 17
    assert records.station_statuses == [
        (first_code, sync_lhh.LongHorizonStationWriteStatus.API_FAILED),
        (second_code, sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT),
    ]
    assert summary.total_attempted == 2
    assert summary.status_counts[sync_lhh.LongHorizonStationWriteStatus.API_FAILED] == 1
    assert summary.status_counts[sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT] == 1
    assert sync_lhh._degraded_long_horizon_summary_line(summary) == (
        "DEGRADED: monthly discharge norms unavailable for 1/2 stations; "
        "observed runoff written; norm and percent-of-norm unavailable."
    )
    assert "total_attempted=2" in artifact
    assert "api_failed=1" in artifact
    warning_and_error_text = " ".join(
        record.message for record in caplog.records if record.levelno >= sync_lhh.logging.WARNING
    )
    assert first_code not in warning_and_error_text


def test_orchestrator_marks_read_runoff_failure_as_attempted_failed(caplog):
    sdk = MagicMock()
    sdk.get_norm_for_site.return_value = _norms()
    client = MagicMock()
    client.read_runoff.side_effect = sync_lhh.SapphireAPIError("runoff unavailable")

    with caplog.at_level(sync_lhh.logging.WARNING):
        records = sync_lhh.write_long_horizon_hydrograph(
            codes=["19999"],
            iehhf_sdk=sdk,
            client=client,
            target_year=2026,
            today=dt.date(2027, 1, 1),
        )

    assert len(records) == 0
    client.write_hydrograph.assert_not_called()
    assert "1/1 attempted station(s)" in caplog.text
    assert "19999" not in caplog.text
    assert records.attempted_station_codes == ["19999"]
    assert records.completed_station_codes == []
    assert records.failed_station_codes == ["19999"]


def test_orchestrator_skip_has_metadata_but_no_attempt_completion_or_failure():
    sdk = MagicMock()
    sdk.get_norm_for_site.side_effect = ConnectionError("tunnel down")
    client = MagicMock()

    records = sync_lhh.write_long_horizon_hydrograph(
        codes=["19999"],
        iehhf_sdk=sdk,
        client=client,
        target_year=2026,
        today=dt.date(2027, 1, 1),
    )

    assert isinstance(records, sync_lhh._LongHorizonWriteResult)
    assert records == []
    assert records.attempted_station_codes == []
    assert records.completed_station_codes == []
    assert records.failed_station_codes == []


def _patch_main_dependencies(monkeypatch, records):
    monkeypatch.setattr(
        sync_lhh.sys,
        "argv",
        ["sync_long_horizon_hydrograph.py", "--target-year", "2026"],
    )
    monkeypatch.setattr(sync_lhh.sl, "load_environment", MagicMock())
    monkeypatch.setattr(sync_lhh, "IEasyHydroHFSDK", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(sync_lhh, "resolve_sdk_station_codes", MagicMock(return_value=["19999"]))
    monkeypatch.setattr(sync_lhh, "_get_preprocessing_client", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(
        sync_lhh,
        "write_long_horizon_hydrograph",
        MagicMock(return_value=records),
    )


def test_main_exits_five_when_every_attempted_station_has_api_read_write_failure(
    monkeypatch,
    caplog,
):
    records = sync_lhh._LongHorizonWriteResult([{"code": "19999"}])
    records.station_statuses = [("19999", sync_lhh.LongHorizonStationWriteStatus.API_FAILED)]
    records.attempted_station_codes = ["19999"]
    records.completed_station_codes = []
    records.failed_station_codes = ["19999"]
    _patch_main_dependencies(monkeypatch, records)

    with caplog.at_level(sync_lhh.logging.ERROR), pytest.raises(SystemExit) as exc:
        sync_lhh.main()

    assert exc.value.code == 5
    assert (
        "Long-horizon monthly hydrograph ingestion completed with 1 API read/write failure(s)."
        in caplog.text
    )


def test_main_exits_five_when_some_station_completes_after_api_read_write_failure(
    monkeypatch,
):
    records = sync_lhh._LongHorizonWriteResult([{"code": "19999"}, {"code": "19998"}])
    records.station_statuses = [
        ("19999", sync_lhh.LongHorizonStationWriteStatus.API_FAILED),
        ("19998", sync_lhh.LongHorizonStationWriteStatus.WRITTEN),
    ]
    records.attempted_station_codes = ["19999", "19998"]
    records.completed_station_codes = ["19998"]
    records.failed_station_codes = ["19999"]
    _patch_main_dependencies(monkeypatch, records)

    with pytest.raises(SystemExit) as exc:
        sync_lhh.main()

    assert exc.value.code == 5


def test_main_exits_four_when_sdk_norm_lookup_fails(monkeypatch):
    records = sync_lhh._LongHorizonWriteResult([{"code": "19999"}])
    records.station_statuses = [(TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED)]
    _patch_main_dependencies(monkeypatch, records)

    with pytest.raises(SystemExit) as exc:
        sync_lhh.main()

    assert exc.value.code == 4


# INFRA-032: the precedence was deliberately inverted so exit 4 means "SDK
# failures only, no API failures". A later phase treats exit 4 as non-fatal
# degradation; that is only safe if API_FAILED (a real read/write failure,
# not just a missing norm lookup) always wins and keeps the run fatal (5).
# This assertion changing from 4 to 5 is an authorised contract change, not
# a weakened test.
def test_main_exits_five_before_four_when_sdk_and_api_failures_both_present(monkeypatch):
    records = sync_lhh._LongHorizonWriteResult([{"code": "19999"}])
    records.station_statuses = [
        (TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED),
        ("19998", sync_lhh.LongHorizonStationWriteStatus.API_FAILED),
    ]
    _patch_main_dependencies(monkeypatch, records)

    with pytest.raises(SystemExit) as exc:
        sync_lhh.main()

    assert exc.value.code == 5


def test_main_exits_four_when_all_sdk_failed_even_with_zero_records(monkeypatch, caplog):
    records = sync_lhh._LongHorizonWriteResult()
    records.station_statuses = [(TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.SDK_FAILED)]
    records.attempted_station_codes = []
    records.completed_station_codes = []
    records.failed_station_codes = []
    _patch_main_dependencies(monkeypatch, records)

    with caplog.at_level(sync_lhh.logging.ERROR), pytest.raises(SystemExit) as exc:
        sync_lhh.main()

    assert exc.value.code == 4
    assert "SDK norm lookup failure" in caplog.text
    assert "No monthly hydrograph records produced" not in caplog.text


def test_main_norm_absent_no_records_still_exits_two(monkeypatch, caplog):
    records = sync_lhh._LongHorizonWriteResult()
    records.station_statuses = [(TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT)]
    records.attempted_station_codes = []
    records.completed_station_codes = []
    records.failed_station_codes = []
    _patch_main_dependencies(monkeypatch, records)

    with caplog.at_level(sync_lhh.logging.ERROR), pytest.raises(SystemExit) as exc:
        sync_lhh.main()

    assert exc.value.code == 2
    assert "No monthly hydrograph records produced - nothing to write." in caplog.text


def test_main_clean_records_exit_zero(monkeypatch):
    records = sync_lhh._LongHorizonWriteResult([{"code": "19999"}])
    records.station_statuses = [(TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.WRITTEN)]
    records.attempted_station_codes = [TEST_CODE]
    records.completed_station_codes = [TEST_CODE]
    records.failed_station_codes = []
    _patch_main_dependencies(monkeypatch, records)

    with pytest.raises(SystemExit) as exc:
        sync_lhh.main()

    assert exc.value.code == 0


def test_main_prints_counts_only_run_summary_artifact(monkeypatch, capsys):
    records = sync_lhh._LongHorizonWriteResult([{"code": "19999"}])
    records.station_statuses = [(TEST_CODE, sync_lhh.LongHorizonStationWriteStatus.NORM_ABSENT)]
    _patch_main_dependencies(monkeypatch, records)

    with pytest.raises(SystemExit) as exc:
        sync_lhh.main()

    stdout = capsys.readouterr().out
    assert exc.value.code == 0
    assert "LONG-HORIZON RUN SUMMARY" in stdout
    assert (
        "DEGRADED: monthly discharge norms unavailable for 1/1 stations; "
        "observed runoff written; norm and percent-of-norm unavailable."
    ) in stdout
    assert TEST_CODE not in stdout


def test_main_empty_records_path_when_no_station_attempted(monkeypatch, caplog):
    records = sync_lhh._LongHorizonWriteResult()
    records.attempted_station_codes = []
    records.completed_station_codes = []
    records.failed_station_codes = []
    _patch_main_dependencies(monkeypatch, records)

    with caplog.at_level(sync_lhh.logging.ERROR), pytest.raises(SystemExit) as exc:
        sync_lhh.main()

    assert exc.value.code == 2
    assert "No monthly hydrograph records produced - nothing to write." in caplog.text
    assert "All 0 attempted station(s)" not in caplog.text


def test_orchestrator_does_not_catch_programming_errors():
    sdk = MagicMock()
    sdk.get_norm_for_site.return_value = _norms()
    client = MagicMock()
    client.read_runoff.side_effect = [
        _full_year_rows(2026, {month: 20.0 for month in range(1, 13)}),
        _full_year_rows(2025, {month: 10.0 for month in range(1, 13)}),
    ]
    write_calls = []

    def fail_third_write(_records):
        write_calls.append(_records)
        if len(write_calls) == 3:
            raise KeyError("bad record shape")

    client.write_hydrograph.side_effect = fail_third_write

    with pytest.raises(KeyError):
        sync_lhh.write_long_horizon_hydrograph(
            codes=["19999"],
            iehhf_sdk=sdk,
            client=client,
            target_year=2026,
            today=dt.date(2027, 1, 1),
        )


def test_build_quarterly_records_api_contract_fields():
    records = sync_lhh.build_quarterly_records(
        _monthly_records_for_season(target_year=2026),
        TEST_CODE,
        2026,
    )
    expected_fields = {
        "horizon_type",
        "code",
        "date",
        "day_of_year",
        "horizon_value",
        "horizon_in_year",
        "norm",
        "previous",
        "current",
    }

    assert len(records) == 4
    for record in records:
        assert set(record) == expected_fields
        assert record["horizon_type"] == "quarter"
        assert record["code"] == TEST_CODE
        assert record["horizon_value"] == record["horizon_in_year"]
