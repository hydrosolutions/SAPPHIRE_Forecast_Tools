from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pytest

from forecast_skill_eval.baselines import build_operational_proxy_baseline
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.pairs import _read_short_forecasts, basin_for_code, build_pairs
from forecast_skill_eval.regimes import RegimePolicy, choose_regime_policy, derive_regime

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
    horizon: str = "day",
    code: str = STATION_CODE,
) -> dict[str, object]:
    row = {
        "horizon": horizon,
        "code": code,
        "date": issue_date,
        "target": f"{year}-01-{period_key:02d}",
        "horizon_in_year": period_key,
        "model_type": model_type,
        "forecasted_discharge": value,
    }
    if flag is not None:
        row["flag"] = flag
    return row


def _short_observed(
    period_key: int,
    year: int,
    discharge: float,
    *,
    code: str = STATION_CODE,
) -> dict[str, object]:
    return {
        "horizon": "day",
        "code": code,
        "horizon_in_year": period_key,
        "year": year,
        "discharge": discharge,
    }


def _day_norm(
    period_key: int,
    norm: float = 10.0,
    *,
    code: str = STATION_CODE,
) -> dict[str, object]:
    return {
        "horizon": "day",
        "code": code,
        "horizon_in_year": period_key,
        "norm": norm,
        "count": 30,
    }


def _lr_forecast(
    period_key: int,
    value: float | None,
    *,
    horizon: str = "pentad",
    issue_date: str = "2024-01-01",
) -> dict[str, object]:
    return {
        "horizon": horizon,
        "code": STATION_CODE,
        "date": issue_date,
        "horizon_in_year": period_key,
        "forecasted_discharge": value,
    }


def _model_forecast(
    period_key: int,
    value: float | None,
    *,
    model: str,
    horizon: str = "pentad",
    year: int = 2024,
    issue_date: str = "2024-01-01",
    flag: int | None = 0,
) -> dict[str, object]:
    row = {
        "horizon": horizon,
        "code": STATION_CODE,
        "date": issue_date,
        "target": f"{year}-01-{period_key:02d}",
        "horizon_in_year": period_key,
        "model": model,
        "forecasted_discharge": value,
    }
    if flag is not None:
        row["flag"] = flag
    return row


def _observed(
    period_key: int,
    year: int,
    discharge: float,
    *,
    horizon: str,
) -> dict[str, object]:
    return {
        "horizon": horizon,
        "code": STATION_CODE,
        "horizon_in_year": period_key,
        "year": year,
        "discharge": discharge,
    }


def _norm(period_key: int, *, horizon: str, norm: float = 10.0) -> dict[str, object]:
    return {
        "horizon": horizon,
        "code": STATION_CODE,
        "horizon_in_year": period_key,
        "norm": norm,
        "count": 30,
    }


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("15999", "chu_kyrgyz"),
        ("16999", "syr_darya"),
        ("17999", "amu_darya"),
        ("19999", "other"),
        (15, "other"),
        ("1", "other"),
    ],
)
def test_basin_for_code_uses_prefix_mapping(code: object, expected: str) -> None:
    assert basin_for_code(code, ForecastSkillEvalConfig().basin_by_prefix) == expected


def _flag_frame(flags: list[int]) -> pd.DataFrame:
    return pd.DataFrame({"code": [STATION_CODE] * len(flags), "flag": flags})


def test_policy_uses_informative_hindcast_and_nan_flags_at_scale() -> None:
    short_policy = choose_regime_policy(_flag_frame(([4] * 1100) + ([0] * 10)))
    long_policy = choose_regime_policy(_flag_frame(([1] * 1100) + ([0] * 10)))
    date_policy = choose_regime_policy(_flag_frame(([0] * 1100) + ([2] * 10)))

    assert short_policy.source == "flag"
    assert long_policy.source == "flag"
    assert (
        derive_regime(
            {"flag": 1},
            issue_date="2025-01-01",
            policy=long_policy,
        ).regime
        == "hindcast"
    )
    assert date_policy.source == "date"


@pytest.mark.parametrize(
    ("flag", "issue_date", "expected_regime", "expected_reason"),
    [
        (0, "2020-01-01", "operational", None),
        (1, "2025-01-01", "hindcast", None),
        (4, "2025-01-01", "hindcast", None),
        (3, "2025-01-01", None, "forecast_actual_nan_flag"),
        (2, "2025-01-01", None, "forecast_error_flag"),
        (None, "2024-01-01", "operational", None),
        (None, "2023-12-31", "hindcast", None),
        (99, "2024-01-01", "operational", None),
    ],
)
def test_flag_regime_derivation_uses_taxonomy_and_date_fallback(
    flag: int | None,
    issue_date: str,
    expected_regime: str | None,
    expected_reason: str | None,
) -> None:
    policy = RegimePolicy(
        source="flag",
        operational_start=date(2024, 1, 1),
        reason="test",
        flag_counts={},
    )
    row = {} if flag is None else {"flag": flag}

    decision = derive_regime(row, issue_date=issue_date, policy=policy)

    assert decision.regime == expected_regime
    assert decision.exclude_reason == expected_reason


def test_flagless_lr_rows_date_fallback_match_flagged_operational_ml(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            _model_forecast(1, 9.0, model="TFT", flag=0, issue_date="2024-01-01"),
            _model_forecast(2, 8.0, model="TFT", flag=4, issue_date="2020-01-01"),
        ],
        lr_forecasts_rows=[
            _lr_forecast(1, 7.0, issue_date="2024-01-01"),
        ],
        runoff_rows=[
            _observed(1, 2024, 7.0, horizon="pentad"),
            _observed(2, 2024, 8.0, horizon="pentad"),
        ],
        hydrograph_rows=[
            _norm(1, horizon="pentad"),
            _norm(2, horizon="pentad"),
        ],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "pentad")
    baseline = build_operational_proxy_baseline(pairs)

    assert ("pair", "forecast_unknown_flag") not in ledger.counts_by_stage_reason()
    operational_pairs = pairs[(pairs["period_key"] == 1) & (pairs["regime"] == "operational")]
    assert set(operational_pairs["model"]) == {"TFT", "LR"}
    assert pairs.attrs["regime_source"] == "flag"
    assert not baseline.empty


@pytest.mark.parametrize("horizon", ["pentad", "decade"])
def test_lr_rows_score_beside_short_term_models(
    fake_client_factory,
    horizon: str,
) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            _model_forecast(1, 9.0, model="TFT", horizon=horizon),
        ],
        lr_forecasts_rows=[
            _lr_forecast(1, 7.0, horizon=horizon),
        ],
        runoff_rows=[
            _observed(1, 2024, 7.0, horizon=horizon),
        ],
        hydrograph_rows=[
            _norm(1, horizon=horizon),
        ],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, horizon)

    assert set(pairs["model"]) == {"TFT", "LR"}
    assert pairs[pairs["model"].eq("LR")]["contingency"].tolist() == ["TP"]
    assert ledger.entries == ()


def test_short_forecast_reader_preserves_ml_rows_without_lr_data(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            _model_forecast(1, 7.0, model="TFT"),
            _model_forecast(2, 9.0, model="XGB"),
        ],
        runoff_rows=[
            _observed(1, 2024, 7.0, horizon="pentad"),
            _observed(2, 2024, 9.0, horizon="pentad"),
        ],
        hydrograph_rows=[
            _norm(1, horizon="pentad"),
            _norm(2, horizon="pentad"),
        ],
    )
    ledger = ExclusionLedger()

    forecasts = _read_short_forecasts(
        client,
        "pentad",
        (STATION_CODE,),
        (None,),
        None,
        None,
        ledger,
    )
    pairs, pair_ledger = build_pairs(
        ForecastSkillEvalConfig(station_filter=[STATION_CODE]),
        client,
        "pentad",
    )

    assert forecasts[["model", "horizon_in_year", "point_value"]].to_dict("records") == [
        {"model": "TFT", "horizon_in_year": 1, "point_value": 7.0},
        {"model": "XGB", "horizon_in_year": 2, "point_value": 9.0},
    ]
    assert pairs[["model", "period_key", "forecast_value"]].to_dict("records") == [
        {"model": "TFT", "period_key": 1, "forecast_value": 7.0},
        {"model": "XGB", "period_key": 2, "forecast_value": 9.0},
    ]
    assert ledger.entries == ()
    assert pair_ledger.entries == ()


def test_lr_null_forecast_is_recorded_as_forecast_missing(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        lr_forecasts_rows=[
            _lr_forecast(1, None),
        ],
        runoff_rows=[
            _observed(1, 2024, 7.0, horizon="pentad"),
        ],
        hydrograph_rows=[
            _norm(1, horizon="pentad"),
        ],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "pentad")

    assert pairs.empty
    assert ledger.counts_by_stage_reason() == {("pair", "forecast_missing"): 1}


@pytest.mark.parametrize(
    ("model_filter", "expect_lr"),
    [
        (None, True),
        (["TFT"], False),
        (["TFT", "LR"], True),
    ],
)
def test_lr_read_respects_model_filter_gating(
    fake_client_factory,
    model_filter: list[str] | None,
    expect_lr: bool,
) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            _model_forecast(1, 9.0, model="TFT"),
        ],
        lr_forecasts_rows=[
            _lr_forecast(1, 7.0),
        ],
        runoff_rows=[
            _observed(1, 2024, 7.0, horizon="pentad"),
        ],
        hydrograph_rows=[
            _norm(1, horizon="pentad"),
        ],
    )
    config = ForecastSkillEvalConfig(
        station_filter=[STATION_CODE],
        model_filter=model_filter,
    )

    pairs, _ledger = build_pairs(config, client, "pentad")

    assert ("LR" in set(pairs["model"])) is expect_lr
    assert _call_count(client, "read_lr_forecasts") == int(expect_lr)


def test_lr_rows_are_not_duplicated_for_multi_model_filter(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            _model_forecast(1, 9.0, model="TFT"),
            _model_forecast(1, 8.0, model="XGB"),
        ],
        lr_forecasts_rows=[
            _lr_forecast(1, 7.0),
        ],
        runoff_rows=[
            _observed(1, 2024, 7.0, horizon="pentad"),
        ],
        hydrograph_rows=[
            _norm(1, horizon="pentad"),
        ],
    )
    config = ForecastSkillEvalConfig(
        station_filter=[STATION_CODE],
        model_filter=["TFT", "XGB", "LR"],
    )

    pairs, _ledger = build_pairs(config, client, "pentad")

    lr_pairs = pairs[pairs["model"].eq("LR")]
    assert len(lr_pairs) == 1
    assert lr_pairs[["code", "period_key", "year"]].to_dict("records") == [
        {"code": STATION_CODE, "period_key": 1, "year": 2024}
    ]
    assert _call_count(client, "read_lr_forecasts") == 1


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


def test_pairs_carry_basin_from_station_code_prefix(fake_client_factory) -> None:
    rows = [
        ("15999", "chu_kyrgyz"),
        ("16999", "syr_darya"),
        ("17999", "amu_darya"),
        ("19999", "other"),
    ]
    client = fake_client_factory(
        forecasts_rows=[
            _short_forecast(index, 7.0, code=code) for index, (code, _basin) in enumerate(rows, 1)
        ],
        runoff_rows=[
            _short_observed(index, 2024, 7.0, code=code)
            for index, (code, _basin) in enumerate(rows, 1)
        ],
        hydrograph_rows=[
            _day_norm(index, code=code) for index, (code, _basin) in enumerate(rows, 1)
        ],
    )

    pairs, ledger = build_pairs(ForecastSkillEvalConfig(), client, "day")

    basin_by_code = dict(zip(pairs["code"], pairs["basin"], strict=True))
    assert basin_by_code == dict(rows)
    assert ledger.entries == ()


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
    # Default issue-day filter is empty (alignment-only), so any calendar-aligned
    # issue date passes; only the rolling-window check drives the exclusion here.
    client = fake_client_factory(
        long_forecasts_rows=[
            {
                "horizon": "month",
                "code": STATION_CODE,
                "date": "2023-12-25",  # day 25: operational, calendar-aligned → pair
                "valid_from": "2024-04-01",
                "valid_to": "2024-04-30",
                "horizon_value": 2,
                "model_type": "model-a",
                "q": 7.0,
            },
            {
                "horizon": "month",
                "code": STATION_CODE,
                "date": "2023-12-10",  # day 10: operational, rolling window → excluded
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
    assert row["issue_date"] == "2023-12-25"
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


# ---------------------------------------------------------------------------
# Helpers for long-term forecast fixtures
# ---------------------------------------------------------------------------


def _long_forecast(
    *,
    horizon: str = "month",
    issue_date: str = "2024-01-25",
    valid_from: str = "2024-04-01",
    valid_to: str = "2024-04-30",
    horizon_value: int = 1,
    model: str = "model-a",
    value: float = 7.0,
) -> dict[str, object]:
    return {
        "horizon": horizon,
        "code": STATION_CODE,
        "date": issue_date,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "horizon_value": horizon_value,
        "model_type": model,
        "q": value,
    }


def _april_hydrograph_norm() -> dict[str, object]:
    return {
        "horizon": "month",
        "code": STATION_CODE,
        "horizon_in_year": 4,
        "norm": 10.0,
    }


# ---------------------------------------------------------------------------
# Issue-day filter tests
# ---------------------------------------------------------------------------


def test_month_forecast_on_non_operational_day_is_excluded(
    fake_client_factory,
) -> None:
    # Opt-in: pass the set explicitly to exercise the issue-day filter.
    # The default is empty (no filtering); this test covers the non-default path.
    client = fake_client_factory(
        long_forecasts_rows=[
            _long_forecast(issue_date="2024-02-05"),  # day 5: not in {1, 10, 25}
        ],
        runoff_rows=_daily_rows(year=2024, month=4, value=7.0),
        hydrograph_rows=[_april_hydrograph_norm()],
    )
    config = ForecastSkillEvalConfig(
        station_filter=[STATION_CODE],
        operational_issue_days=[1, 10, 25],
    )

    pairs, ledger = build_pairs(config, client, "month")

    assert pairs.empty
    assert (
        ledger.counts_by_stage_reason().get(("pair", "forecast_non_operational_issue_day"), 0) == 1
    )


def test_month_forecast_on_operational_day_creates_pair(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        long_forecasts_rows=[
            _long_forecast(issue_date="2024-01-25"),  # day 25: operational
        ],
        runoff_rows=_daily_rows(year=2024, month=4, value=7.0),
        hydrograph_rows=[_april_hydrograph_norm()],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "month")

    assert len(pairs) == 1
    assert ("pair", "forecast_non_operational_issue_day") not in ledger.counts_by_stage_reason()


def test_empty_operational_issue_days_disables_issue_day_filter(
    fake_client_factory,
) -> None:
    client = fake_client_factory(
        long_forecasts_rows=[
            _long_forecast(issue_date="2024-02-05"),  # day 5: not operational, but filter off
        ],
        runoff_rows=_daily_rows(year=2024, month=4, value=7.0),
        hydrograph_rows=[_april_hydrograph_norm()],
    )
    config = ForecastSkillEvalConfig(
        station_filter=[STATION_CODE],
        operational_issue_days=[],
    )

    pairs, ledger = build_pairs(config, client, "month")

    assert len(pairs) == 1
    assert ("pair", "forecast_non_operational_issue_day") not in ledger.counts_by_stage_reason()


def test_issue_day_filter_does_not_apply_to_quarter(
    fake_client_factory,
) -> None:
    # Q2: April–June; issue date on day 5 (not operational) must not trigger the
    # issue-day filter because the filter only applies to the "month" horizon.
    client = fake_client_factory(
        long_forecasts_rows=[
            _long_forecast(
                horizon="quarter",
                issue_date="2024-02-05",  # day 5: not operational
                valid_from="2024-04-01",
                valid_to="2024-06-30",
            ),
        ],
        runoff_rows=[
            *_daily_rows(year=2024, month=4, value=7.0),
            *_daily_rows(year=2024, month=5, value=7.0),
            *_daily_rows(year=2024, month=6, value=7.0),
        ],
        hydrograph_rows=[
            {
                "horizon": "quarter",
                "code": STATION_CODE,
                "horizon_in_year": 2,
                "norm": 10.0,
                "count": 30,
            }
        ],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    _pairs, ledger = build_pairs(config, client, "quarter")

    assert ("pair", "forecast_non_operational_issue_day") not in ledger.counts_by_stage_reason()
