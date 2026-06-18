from __future__ import annotations

from datetime import date, timedelta

import pytest

from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.pairs import build_pairs

STATION_CODE = "19999"


def _daily_rows(
    *,
    year: int,
    month: int,
    count: int | None = None,
    value: float = 10.0,
) -> list[dict[str, object]]:
    rows = []
    day = date(year, month, 1)
    while day.month == month and (count is None or len(rows) < count):
        rows.append(
            {
                "horizon": "day",
                "code": STATION_CODE,
                "date": day.isoformat(),
                "discharge": value,
            }
        )
        day += timedelta(days=1)
    return rows


def _short_forecast(
    period_key: int,
    value: float | None,
    *,
    model_type: str = "model-a",
    year: int = 2024,
    flag: int | None = 0,
    issue_date: str = "2024-01-01",
) -> dict[str, object]:
    row = {
        "horizon": "day",
        "code": STATION_CODE,
        "date": issue_date,
        "target": f"{year}-01-{period_key:02d}",
        "horizon_in_year": period_key,
        "model_type": model_type,
        "forecasted_discharge": value,
    }
    if flag is not None:
        row["flag"] = flag
    return row


def _short_observed(period_key: int, year: int, discharge: float) -> dict[str, object]:
    return {
        "horizon": "day",
        "code": STATION_CODE,
        "horizon_in_year": period_key,
        "year": year,
        "discharge": discharge,
    }


def _day_norm(period_key: int, norm: float = 10.0) -> dict[str, object]:
    return {
        "horizon": "day",
        "code": STATION_CODE,
        "horizon_in_year": period_key,
        "norm": norm,
        "count": 30,
    }


def test_short_term_pairs_emit_all_contingency_cells(fake_client_factory) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            _short_forecast(1, 7.0),
            _short_forecast(2, 7.0),
            _short_forecast(3, 9.0),
            _short_forecast(4, 9.0),
        ],
        runoff_rows=[
            _short_observed(1, 2024, 7.0),
            _short_observed(2, 2024, 9.0),
            _short_observed(3, 2024, 7.0),
            _short_observed(4, 2024, 9.0),
        ],
        hydrograph_rows=[_day_norm(period_key) for period_key in range(1, 5)],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "day")

    contingency_by_period = dict(zip(pairs["period_key"], pairs["contingency"], strict=True))
    assert contingency_by_period == {1: "TP", 2: "FP", 3: "FN", 4: "TN"}
    assert pairs["norm_provenance"].tolist() == ["calculated"] * 4
    assert pairs["regime"].tolist() == ["operational"] * 4
    assert ledger.entries == ()
    assert _call_count(client, "read_hydrograph") == 1


def test_regime_from_flag_and_error_flag_exclusion(fake_client_factory) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            _short_forecast(1, 7.0, flag=0, issue_date="2020-01-01"),
            _short_forecast(2, 7.0, flag=1, issue_date="2025-01-01"),
            _short_forecast(3, 7.0, flag=2, issue_date="2025-01-01"),
        ],
        runoff_rows=[
            _short_observed(1, 2024, 7.0),
            _short_observed(2, 2024, 7.0),
            _short_observed(3, 2024, 7.0),
        ],
        hydrograph_rows=[_day_norm(period_key) for period_key in range(1, 4)],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "day")

    regime_by_period = dict(zip(pairs["period_key"], pairs["regime"], strict=True))
    assert regime_by_period == {1: "operational", 2: "hindcast"}
    assert pairs.attrs["regime_source"] == "flag"
    assert ledger.counts_by_stage_reason() == {("pair", "forecast_error_flag"): 1}


def test_regime_falls_back_to_issue_date_when_flag_does_not_distinguish(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            _short_forecast(1, 7.0, flag=0, issue_date="2023-12-31"),
            _short_forecast(2, 7.0, flag=0, issue_date="2024-01-01"),
            _short_forecast(3, 7.0, flag=2, issue_date="2024-01-02"),
        ],
        runoff_rows=[
            _short_observed(1, 2024, 7.0),
            _short_observed(2, 2024, 7.0),
            _short_observed(3, 2024, 7.0),
        ],
        hydrograph_rows=[_day_norm(period_key) for period_key in range(1, 4)],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "day")

    regime_by_period = dict(zip(pairs["period_key"], pairs["regime"], strict=True))
    assert regime_by_period == {1: "hindcast", 2: "operational"}
    assert pairs.attrs["regime_source"] == "date"
    assert ledger.counts_by_stage_reason() == {("pair", "forecast_error_flag"): 1}


def test_long_term_calendar_join_and_rolling_window_exclusion(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        long_forecasts_rows=[
            {
                "horizon": "month",
                "code": STATION_CODE,
                "date": "2023-12-15",
                "valid_from": "2024-04-01",
                "valid_to": "2024-04-30",
                "horizon_value": 2,
                "model_type": "model-a",
                "q": 7.0,
            },
            {
                "horizon": "month",
                "code": STATION_CODE,
                "date": "2023-12-20",
                "valid_from": "2024-04-10",
                "valid_to": "2024-05-09",
                "horizon_value": 2,
                "model_type": "model-a",
                "q": 7.0,
            },
        ],
        runoff_rows=[
            *_daily_rows(year=2024, month=4, value=7.0),
            *_daily_rows(year=2024, month=5, count=10, value=9.0),
        ],
        hydrograph_rows=[
            {
                "horizon": "month",
                "code": STATION_CODE,
                "horizon_in_year": 4,
                "norm": 10.0,
            }
        ],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "month")

    assert len(pairs) == 1
    row = pairs.iloc[0]
    assert row["period_key"] == 4
    assert row["year"] == 2024
    assert row["lead"] == 2
    assert row["issue_date"] == "2023-12-15"
    assert row["regime"] == "hindcast"
    assert row["norm_provenance"] == "official"
    assert row["contingency"] == "TP"
    assert ledger.counts_by_stage_reason() == {
        ("pair", "forecast_rolling_window"): 1,
        ("observed", "observed_incomplete_month"): 1,
    }


def test_pair_exclusions_loo_and_memoized_norm_readers(fake_client_factory) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            _short_forecast(5, 7.0, model_type="model-a"),
            _short_forecast(5, 6.0, model_type="model-b"),
            _short_forecast(5, 9.0, model_type="model-c"),
            _short_forecast(6, None),
            _short_forecast(7, 7.0),
            _short_forecast(8, 7.0),
        ],
        runoff_rows=[
            _short_observed(5, 2022, 10.0),
            _short_observed(5, 2023, 10.0),
            _short_observed(5, 2024, 1000.0),
            _short_observed(6, 2024, 10.0),
            _short_observed(8, 2023, 10.0),
            _short_observed(8, 2024, 10.0),
        ],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE], min_years=2)

    pairs, ledger = build_pairs(config, client, "day")

    assert len(pairs) == 3
    assert pairs["model"].tolist() == ["model-a", "model-b", "model-c"]
    assert pairs["norm"].tolist() == [10.0, 10.0, 10.0]
    assert pairs["norm"].iloc[0] != pytest.approx((10.0 + 10.0 + 1000.0) / 3)
    assert ledger.counts_by_stage_reason() == {
        ("pair", "forecast_missing"): 1,
        ("pair", "observed_unmatched"): 1,
        ("norm", "norm_unavailable_lt_min_years"): 1,
    }
    assert _call_count(client, "read_hydrograph") == 1
    assert _call_count(client, "read_runoff") == 1


def _call_count(client, method_name: str) -> int:
    return [name for name, _kwargs in client.calls].count(method_name)
