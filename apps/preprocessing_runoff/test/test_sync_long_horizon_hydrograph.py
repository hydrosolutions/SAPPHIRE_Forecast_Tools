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


@pytest.mark.parametrize(("norms", "actual_count"), [([], 0), ([1.0] * 7, 7)])
def test_skips_station_when_norms_missing(norms, actual_count, caplog):
    sdk = MagicMock()
    sdk.get_norm_for_site.return_value = norms
    client = MagicMock()

    with caplog.at_level(sync_lhh.logging.WARNING):
        records = sync_lhh.write_station_monthly_hydrograph(
            TEST_CODE,
            sdk,
            client,
            target_year=2026,
            today=dt.date(2026, 6, 15),
        )

    assert records == []
    client.write_hydrograph.assert_not_called()
    assert "12" in caplog.text
    assert str(actual_count) in caplog.text


def test_skips_station_when_sdk_raises(caplog):
    sdk = MagicMock()
    sdk.get_norm_for_site.side_effect = ConnectionError("tunnel down")
    client = MagicMock()

    with caplog.at_level(sync_lhh.logging.WARNING):
        records = sync_lhh.write_station_monthly_hydrograph(
            TEST_CODE,
            sdk,
            client,
            target_year=2026,
            today=dt.date(2026, 6, 15),
        )

    assert records == []
    client.write_hydrograph.assert_not_called()
    assert "ConnectionError" in caplog.text
    assert "tunnel down" in caplog.text


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
    assert "Long-horizon hydrograph API read/write failed for station 19999" in caplog.text
    assert "1/2 attempted station(s)" in caplog.text
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
    assert "Long-horizon hydrograph API read/write failed for station 19999" in caplog.text
    assert records.attempted_station_codes == ["19999"]
    assert records.completed_station_codes == []
    assert records.failed_station_codes == ["19999"]


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
    assert "Long-horizon hydrograph API read/write failed for station 19999" in caplog.text
    assert "read/write" in caplog.text
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
    monkeypatch.setattr(sync_lhh.sys, "argv", ["sync_long_horizon_hydrograph.py"])
    monkeypatch.setattr(sync_lhh.sl, "load_environment", MagicMock())
    monkeypatch.setattr(sync_lhh, "IEasyHydroHFSDK", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(sync_lhh, "resolve_sdk_station_codes", MagicMock(return_value=["19999"]))
    monkeypatch.setattr(sync_lhh, "_get_preprocessing_client", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(
        sync_lhh,
        "write_long_horizon_hydrograph",
        MagicMock(return_value=records),
    )


def test_main_exits_two_when_every_attempted_station_has_api_read_write_failure(
    monkeypatch,
    caplog,
):
    records = sync_lhh._LongHorizonWriteResult([{"code": "19999"}])
    records.attempted_station_codes = ["19999"]
    records.completed_station_codes = []
    records.failed_station_codes = ["19999"]
    _patch_main_dependencies(monkeypatch, records)

    with caplog.at_level(sync_lhh.logging.ERROR), pytest.raises(SystemExit) as exc:
        sync_lhh.main()

    assert exc.value.code == 2
    assert (
        "All 1 attempted station(s) had long-horizon hydrograph API read/write failures"
        in caplog.text
    )


def test_main_exits_zero_when_some_station_completes_after_api_read_write_failure(
    monkeypatch,
):
    records = sync_lhh._LongHorizonWriteResult([{"code": "19999"}, {"code": "19998"}])
    records.attempted_station_codes = ["19999", "19998"]
    records.completed_station_codes = ["19998"]
    records.failed_station_codes = ["19999"]
    _patch_main_dependencies(monkeypatch, records)

    with pytest.raises(SystemExit) as exc:
        sync_lhh.main()

    assert exc.value.code == 0


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
