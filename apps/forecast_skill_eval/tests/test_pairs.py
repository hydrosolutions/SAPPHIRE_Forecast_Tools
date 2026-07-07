from __future__ import annotations

import math
from datetime import date, timedelta

import pandas as pd
import pytest

from forecast_skill_eval.baselines import build_operational_proxy_baseline
from forecast_skill_eval.config import ForecastSkillEvalConfig
from forecast_skill_eval.contingency import count_contingencies
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.pairs import (
    _derive_long_lead,
    _read_short_forecasts,
    _target_period_start,
    basin_for_code,
    build_pairs,
)
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


def test_regime_source_auto_matches_default_on_flag_rich_frame() -> None:
    """regime_source='auto' reproduces the no-arg policy on a flag-rich frame."""
    frame = _flag_frame(([4] * 1100) + ([0] * 10))
    default_policy = choose_regime_policy(frame)
    auto_policy = choose_regime_policy(frame, regime_source="auto")

    assert default_policy.source == "flag"
    assert auto_policy.source == default_policy.source
    assert auto_policy.reason == default_policy.reason
    assert auto_policy.flag_counts == default_policy.flag_counts


def test_regime_source_auto_matches_default_on_flag_sparse_frame() -> None:
    """regime_source='auto' reproduces the no-arg policy on a flag-sparse frame."""
    frame = _flag_frame([0] * 20)
    default_policy = choose_regime_policy(frame)
    auto_policy = choose_regime_policy(frame, regime_source="auto")

    assert default_policy.source == "date"
    assert auto_policy.source == default_policy.source
    assert auto_policy.reason == default_policy.reason
    assert auto_policy.flag_counts == default_policy.flag_counts


def test_regime_source_flag_forces_flag_on_sparse_frame() -> None:
    """regime_source='flag' forces flag mode even when auto would pick date."""
    frame = _flag_frame([0] * 20)

    assert choose_regime_policy(frame).source == "date"
    forced = choose_regime_policy(frame, regime_source="flag")
    assert forced.source == "flag"
    assert forced.reason == "regime source forced to flag"


def test_regime_source_date_forces_date_on_flag_rich_frame() -> None:
    """regime_source='date' forces date mode even when auto would pick flag."""
    frame = _flag_frame(([4] * 1100) + ([0] * 10))

    assert choose_regime_policy(frame).source == "flag"
    forced = choose_regime_policy(frame, regime_source="date")
    assert forced.source == "date"
    assert forced.reason == "regime source forced to issue date"


@pytest.mark.parametrize("source", ["flag", "date"])
def test_nan_flag_excluded_in_both_modes(source: str) -> None:
    """REG-5: a flag-3 (nan) row is excluded regardless of regime source."""
    policy = RegimePolicy(
        source=source,
        operational_start=date(2024, 1, 1),
        reason="test",
        flag_counts={},
    )

    decision = derive_regime({"flag": 3}, issue_date="2025-01-01", policy=policy)

    assert decision.regime is None
    assert decision.exclude_reason == "forecast_actual_nan_flag"


@pytest.mark.parametrize("source", ["flag", "date"])
def test_error_flag_excluded_in_both_modes(source: str) -> None:
    """A flag-2 (error) row is excluded regardless of regime source."""
    policy = RegimePolicy(
        source=source,
        operational_start=date(2024, 1, 1),
        reason="test",
        flag_counts={},
    )

    decision = derive_regime({"flag": 2}, issue_date="2025-01-01", policy=policy)

    assert decision.regime is None
    assert decision.exclude_reason == "forecast_error_flag"


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


# ---------------------------------------------------------------------------
# Quantile-band ingestion through pairs (P1 — additive plumbing)
# ---------------------------------------------------------------------------


def _short_forecast_with_quantiles(
    period_key: int,
    value: float,
    *,
    model_type: str = "TFT",
    year: int = 2024,
    issue_date: str = "2024-01-01",
    horizon: str = "pentad",
    code: str = STATION_CODE,
) -> dict[str, object]:
    """Short forecast row that includes the 4-node quantile band."""
    return {
        "horizon": horizon,
        "code": code,
        "date": issue_date,
        "target": f"{year}-01-{period_key:02d}",
        "horizon_in_year": period_key,
        "model_type": model_type,
        "forecasted_discharge": value,
        "q05": value * 0.5,
        "q25": value * 0.8,
        "q75": value * 1.2,
        "q95": value * 1.5,
    }


def _long_forecast_with_quantiles(
    *,
    horizon: str = "month",
    issue_date: str = "2024-01-25",
    valid_from: str = "2024-04-01",
    valid_to: str = "2024-04-30",
    horizon_value: int = 1,
    model: str = "model-a",
    value: float = 7.0,
) -> dict[str, object]:
    """Long forecast row that includes the full 7-node quantile band."""
    return {
        "horizon": horizon,
        "code": STATION_CODE,
        "date": issue_date,
        "valid_from": valid_from,
        "valid_to": valid_to,
        "horizon_value": horizon_value,
        "model_type": model,
        "q": value,
        "q05": value * 0.5,
        "q10": value * 0.65,
        "q25": value * 0.8,
        "q50": value,
        "q75": value * 1.2,
        "q90": value * 1.35,
        "q95": value * 1.5,
    }


def test_short_pairs_carry_five_node_quantile_band(fake_client_factory) -> None:
    """Short-term pairs: fc_q05/q25/q50/q75/q95 populated; q10/q90 NaN; grid_id=short5."""
    # 6.0 < 0.8 * 10.0 (norm) = 8.0 → "below" for both fc and obs → TP
    value = 6.0
    client = fake_client_factory(
        forecasts_rows=[_short_forecast_with_quantiles(1, value)],
        runoff_rows=[_observed(1, 2024, value, horizon="pentad")],
        hydrograph_rows=[_norm(1, horizon="pentad")],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "pentad")

    assert len(pairs) == 1
    row = pairs.iloc[0]
    # Existing columns unchanged
    assert row["forecast_value"] == pytest.approx(value)
    assert row["contingency"] == "TP"
    # Quantile columns present and correct
    assert row["fc_grid_id"] == "short5"
    assert row["fc_q05"] == pytest.approx(value * 0.5)
    assert row["fc_q25"] == pytest.approx(value * 0.8)
    assert row["fc_q50"] == pytest.approx(value)  # from forecasted_discharge
    assert row["fc_q75"] == pytest.approx(value * 1.2)
    assert row["fc_q95"] == pytest.approx(value * 1.5)
    assert math.isnan(row["fc_q10"])
    assert math.isnan(row["fc_q90"])
    assert ledger.entries == ()


def test_long_pairs_carry_seven_node_quantile_band(fake_client_factory) -> None:
    """Long-term pairs: all 7 fc_q* columns populated; grid_id=long7."""
    value = 7.0
    client = fake_client_factory(
        long_forecasts_rows=[_long_forecast_with_quantiles(value=value)],
        runoff_rows=_daily_rows(year=2024, month=4, value=value),
        hydrograph_rows=[_april_hydrograph_norm()],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "month")

    assert len(pairs) == 1
    row = pairs.iloc[0]
    # Existing columns unchanged
    assert row["forecast_value"] == pytest.approx(value)
    # Quantile columns
    assert row["fc_grid_id"] == "long7"
    assert row["fc_q05"] == pytest.approx(value * 0.5)
    assert row["fc_q10"] == pytest.approx(value * 0.65)
    assert row["fc_q25"] == pytest.approx(value * 0.8)
    assert row["fc_q50"] == pytest.approx(value)
    assert row["fc_q75"] == pytest.approx(value * 1.2)
    assert row["fc_q90"] == pytest.approx(value * 1.35)
    assert row["fc_q95"] == pytest.approx(value * 1.5)


def test_lr_pairs_carry_all_nan_quantile_columns(fake_client_factory) -> None:
    """LR pairs: all fc_q* columns NaN; fc_grid_id empty string."""
    client = fake_client_factory(
        lr_forecasts_rows=[_lr_forecast(1, 7.0)],
        runoff_rows=[_observed(1, 2024, 7.0, horizon="pentad")],
        hydrograph_rows=[_norm(1, horizon="pentad")],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "pentad")

    lr_pairs = pairs[pairs["model"].eq("LR")]
    assert len(lr_pairs) == 1
    row = lr_pairs.iloc[0]
    # Existing columns unchanged
    assert row["forecast_value"] == pytest.approx(7.0)
    assert row["contingency"] == "TP"
    # All quantile columns NaN; grid_id empty
    assert row["fc_grid_id"] == ""
    for col in ("fc_q05", "fc_q10", "fc_q25", "fc_q50", "fc_q75", "fc_q90", "fc_q95"):
        assert math.isnan(row[col]), f"Expected NaN for {col} on LR row"


def test_pairs_quantile_columns_do_not_alter_existing_columns(fake_client_factory) -> None:
    """Regression: adding quantile band columns to source rows must not change
    forecast_value, contingency, or any pre-existing pair column.

    value=9.0 ≥ 0.8*10.0=8.0 → fc_class="normal"; obs=7.0 < 8.0 → obs_class="below" → FN.
    """
    value = 9.0
    client = fake_client_factory(
        forecasts_rows=[_short_forecast_with_quantiles(1, value)],
        runoff_rows=[_observed(1, 2024, 7.0, horizon="pentad")],
        hydrograph_rows=[_norm(1, horizon="pentad")],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "pentad")

    assert len(pairs) == 1
    row = pairs.iloc[0]
    assert row["forecast_value"] == pytest.approx(value)
    assert row["observed_value"] == pytest.approx(7.0)
    assert row["norm"] == pytest.approx(10.0)
    assert row["fc_class"] == "normal"  # 9.0 ≥ threshold → normal
    assert row["obs_class"] == "below"  # 7.0 < threshold → below
    assert row["contingency"] == "FN"
    assert ledger.entries == ()


# ---------------------------------------------------------------------------
# Short-term issue-before-target filter + one-per-target dedup (default off)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("horizon", "period_key", "year", "expected"),
    [
        ("decade", 16, 2025, date(2025, 6, 1)),  # 6th decade → month 6, day 1
        ("decade", 1, 2025, date(2025, 1, 1)),
        ("decade", 3, 2025, date(2025, 1, 21)),
        ("pentad", 1, 2025, date(2025, 1, 1)),
        ("pentad", 7, 2025, date(2025, 2, 1)),  # first pentad of month 2
        ("pentad", 6, 2025, date(2025, 1, 26)),
        ("day", 1, 2025, date(2025, 1, 1)),
        ("day", 60, 2024, date(2024, 2, 29)),  # leap-year day-of-year
    ],
)
def test_target_period_start_maps_short_term_periods(
    horizon: str,
    period_key: int,
    year: int,
    expected: date,
) -> None:
    assert _target_period_start(horizon, period_key, year) == expected


@pytest.mark.parametrize(
    ("horizon", "period_key", "year"),
    [
        ("decade", 0, 2025),  # month 0 → invalid
        ("decade", 37, 2025),  # month 13 → invalid
        ("pentad", 0, 2025),
        ("pentad", 73, 2025),
        ("month", 4, 2025),  # long-term → filter N/A
        ("quarter", 2, 2025),
        ("season", 1, 2025),
    ],
)
def test_target_period_start_returns_none_for_invalid_or_long_term(
    horizon: str,
    period_key: int,
    year: int,
) -> None:
    assert _target_period_start(horizon, period_key, year) is None


def _issue_filter_client(fake_client_factory):
    # day period_key 5 → target start 2024-01-05; period_key 6 → 2024-01-06.
    return fake_client_factory(
        forecasts_rows=[
            _short_forecast(5, 7.0, issue_date="2024-01-01"),  # before start → keep
            _short_forecast(6, 7.0, issue_date="2024-01-10"),  # after start → drop
        ],
        runoff_rows=[
            _short_observed(5, 2024, 7.0),
            _short_observed(6, 2024, 7.0),
        ],
        hydrograph_rows=[_day_norm(5), _day_norm(6)],
    )


def test_issue_before_target_filter_drops_leaking_short_term_forecast(
    fake_client_factory,
) -> None:
    client = _issue_filter_client(fake_client_factory)
    config = ForecastSkillEvalConfig(
        station_filter=[STATION_CODE],
        short_term_issue_before_target=True,
    )

    pairs, ledger = build_pairs(config, client, "day")

    assert pairs["period_key"].tolist() == [5]
    assert ledger.counts_by_stage_reason() == {("pair", "forecast_issue_in_target_period"): 1}


def test_issue_before_target_filter_off_by_default_keeps_all(
    fake_client_factory,
) -> None:
    client = _issue_filter_client(fake_client_factory)
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "day")

    assert sorted(pairs["period_key"].tolist()) == [5, 6]
    assert ("pair", "forecast_issue_in_target_period") not in ledger.counts_by_stage_reason()


def test_unparseable_issue_date_is_not_dropped_by_filter(
    fake_client_factory,
) -> None:
    # An unparseable issue date makes the issue-before-target guard a no-op, so
    # turning the filter on must produce the exact same outcome as leaving it off
    # (whatever downstream regime handling does is identical in both runs).
    def _client():
        return fake_client_factory(
            forecasts_rows=[_short_forecast(6, 7.0, issue_date="not-a-date")],
            runoff_rows=[_short_observed(6, 2024, 7.0)],
            hydrograph_rows=[_day_norm(6)],
        )

    off_pairs, off_ledger = build_pairs(
        ForecastSkillEvalConfig(station_filter=[STATION_CODE]),
        _client(),
        "day",
    )
    on_pairs, on_ledger = build_pairs(
        ForecastSkillEvalConfig(
            station_filter=[STATION_CODE],
            short_term_issue_before_target=True,
        ),
        _client(),
        "day",
    )

    pd.testing.assert_frame_equal(on_pairs, off_pairs)
    assert on_ledger.counts_by_stage_reason() == off_ledger.counts_by_stage_reason()
    assert ("pair", "forecast_issue_in_target_period") not in on_ledger.counts_by_stage_reason()


def _reissue_client(fake_client_factory):
    # Three genuine pre-period re-issues for the same (code, pk, year, model).
    return fake_client_factory(
        forecasts_rows=[
            _short_forecast(10, 7.0, model_type="TFT", issue_date="2024-01-01"),
            _short_forecast(10, 7.0, model_type="TFT", issue_date="2024-01-02"),
            _short_forecast(10, 7.0, model_type="TFT", issue_date="2024-01-03"),
        ],
        runoff_rows=[_short_observed(10, 2024, 7.0)],
        hydrograph_rows=[_day_norm(10)],
    )


def test_dedup_one_per_target_keeps_latest_issue(fake_client_factory) -> None:
    client = _reissue_client(fake_client_factory)
    config = ForecastSkillEvalConfig(
        station_filter=[STATION_CODE],
        short_term_dedup_one_per_target=True,
    )

    pairs, _ledger = build_pairs(config, client, "day")

    assert len(pairs) == 1
    assert pairs.iloc[0]["issue_date"] == "2024-01-03"
    assert pairs.iloc[0]["period_key"] == 10


def test_dedup_off_by_default_keeps_all_reissues(fake_client_factory) -> None:
    client = _reissue_client(fake_client_factory)
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, _ledger = build_pairs(config, client, "day")

    assert len(pairs) == 3
    assert sorted(pairs["issue_date"].tolist()) == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
    ]


def test_long_term_horizons_unaffected_by_short_term_gates(
    fake_client_factory,
) -> None:
    # Two month re-issues for the same target period; one issued INSIDE April.
    # Neither short-term gate may touch long-term pairs.
    client = fake_client_factory(
        long_forecasts_rows=[
            _long_forecast(issue_date="2024-01-25"),
            _long_forecast(issue_date="2024-04-15"),  # inside target month
        ],
        runoff_rows=_daily_rows(year=2024, month=4, value=7.0),
        hydrograph_rows=[_april_hydrograph_norm()],
    )
    config = ForecastSkillEvalConfig(
        station_filter=[STATION_CODE],
        short_term_issue_before_target=True,
        short_term_dedup_one_per_target=True,
    )

    pairs, ledger = build_pairs(config, client, "month")

    assert len(pairs) == 2
    assert ("pair", "forecast_issue_in_target_period") not in ledger.counts_by_stage_reason()


def test_default_config_run_is_byte_identical_with_leakage_and_reissues(
    fake_client_factory,
) -> None:
    # Fixture mixes a leaking issue (pk 6 issued after start) with three re-issues
    # (pk 10).  With both gates off (default) every row must survive unchanged.
    client = fake_client_factory(
        forecasts_rows=[
            _short_forecast(5, 7.0, issue_date="2024-01-01"),
            _short_forecast(6, 7.0, issue_date="2024-01-10"),
            _short_forecast(10, 7.0, model_type="TFT", issue_date="2024-01-01"),
            _short_forecast(10, 7.0, model_type="TFT", issue_date="2024-01-02"),
            _short_forecast(10, 7.0, model_type="TFT", issue_date="2024-01-03"),
        ],
        runoff_rows=[
            _short_observed(5, 2024, 7.0),
            _short_observed(6, 2024, 7.0),
            _short_observed(10, 2024, 7.0),
        ],
        hydrograph_rows=[_day_norm(5), _day_norm(6), _day_norm(10)],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE])

    pairs, ledger = build_pairs(config, client, "day")

    assert len(pairs) == 5
    assert sorted(pairs["period_key"].tolist()) == [5, 6, 10, 10, 10]
    assert ("pair", "forecast_issue_in_target_period") not in ledger.counts_by_stage_reason()


# ---------------------------------------------------------------------------
# Long-term derive-lead correctness gate (default off)
# ---------------------------------------------------------------------------


def _quarter_norm(quarter: int, norm: float = 10.0) -> dict[str, object]:
    return {
        "horizon": "quarter",
        "code": STATION_CODE,
        "horizon_in_year": quarter,
        "norm": norm,
    }


def _season_norm(norm: float = 10.0) -> dict[str, object]:
    return {
        "horizon": "season",
        "code": STATION_CODE,
        "horizon_in_year": 1,
        "norm": norm,
    }


# Aligned validity windows for the four calendar quarters.
_QUARTER_WINDOW = {
    1: ("01-01", "03-31", (1, 2, 3)),
    2: ("04-01", "06-30", (4, 5, 6)),
    3: ("07-01", "09-30", (7, 8, 9)),
    4: ("10-01", "12-31", (10, 11, 12)),
}


def _quarter_runoff(year: int, quarter: int, value: float = 7.0) -> list[dict[str, object]]:
    _start, _end, months = _QUARTER_WINDOW[quarter]
    rows: list[dict[str, object]] = []
    for month in months:
        rows.extend(_daily_rows(year=year, month=month, value=value))
    return rows


def _season_runoff(year: int, value: float = 7.0) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for month in range(4, 10):  # Apr–Sep
        rows.extend(_daily_rows(year=year, month=month, value=value))
    return rows


def test_derive_long_lead_quarter_season_and_underivable() -> None:
    # Quarter Q2 (target month 4): lead = 4 - issue_month within the same year.
    assert _derive_long_lead("quarter", 2, "2024-03-25", 2024) == 1
    assert _derive_long_lead("quarter", 2, "2024-04-10", 2024) == 0
    # Quarter Q4 (target month 10): {0, 1} for late-summer / in-quarter issuances.
    assert _derive_long_lead("quarter", 4, "2024-09-25", 2024) == 1
    assert _derive_long_lead("quarter", 4, "2024-10-05", 2024) == 0
    # Season (target month 4 = April): spans leads 0..3 across the issue months.
    assert _derive_long_lead("season", 1, "2024-01-15", 2024) == 3
    assert _derive_long_lead("season", 1, "2024-02-10", 2024) == 2
    assert _derive_long_lead("season", 1, "2024-03-01", 2024) == 1
    assert _derive_long_lead("season", 1, "2024-04-01", 2024) == 0
    # Cross-year issuance (Q1 target month 1, issued the prior December).
    assert _derive_long_lead("quarter", 1, "2023-12-25", 2024) == 1
    # Underivable: no issue date, missing year, or unmappable period key.
    assert _derive_long_lead("quarter", 2, None, 2024) is None
    assert _derive_long_lead("quarter", 2, "2024-03-25", None) is None
    assert _derive_long_lead("quarter", 9, "2024-03-25", 2024) is None


def test_quarter_flag_off_is_byte_identical_to_legacy(fake_client_factory) -> None:
    # Two re-issues of the same Q2/2024 target.  With the flag OFF the legacy
    # behaviour keeps BOTH rows and the "lead" column carries the stored
    # horizon_value (here a deliberately distinct sentinel 9, not the quarter).
    def _client():
        return fake_client_factory(
            long_forecasts_rows=[
                _long_forecast(
                    horizon="quarter",
                    issue_date="2024-03-25",
                    valid_from="2024-04-01",
                    valid_to="2024-06-30",
                    horizon_value=9,
                    value=7.0,
                ),
                _long_forecast(
                    horizon="quarter",
                    issue_date="2024-04-10",
                    valid_from="2024-04-01",
                    valid_to="2024-06-30",
                    horizon_value=9,
                    value=7.0,
                ),
            ],
            runoff_rows=_quarter_runoff(2024, 2),
            hydrograph_rows=[_quarter_norm(2)],
        )

    off_pairs, off_ledger = build_pairs(
        ForecastSkillEvalConfig(station_filter=[STATION_CODE]),
        _client(),
        "quarter",
    )

    assert len(off_pairs) == 2
    assert off_pairs["lead"].tolist() == [9, 9]
    assert ("pair", "long_forecast_lead_underivable") not in off_ledger.counts_by_stage_reason()

    # Flipping the flag ON must change the outcome (dedup to one, lead = quarter).
    on_pairs, _on_ledger = build_pairs(
        ForecastSkillEvalConfig(station_filter=[STATION_CODE], long_term_derive_lead=True),
        _client(),
        "quarter",
    )
    assert len(on_pairs) == 1
    assert on_pairs.iloc[0]["lead"] == 2


def test_quarter_flag_on_dedups_two_sources_to_smallest_lead(fake_client_factory) -> None:
    # Two sources for the SAME Q2/2024 target: one issued in March (lead 1) and
    # one issued in April (lead 0).  Under the flag they collapse to one deduped
    # pair keeping the smallest-lead (April) issuance.
    client = fake_client_factory(
        long_forecasts_rows=[
            _long_forecast(
                horizon="quarter",
                issue_date="2024-03-25",  # lead 1
                valid_from="2024-04-01",
                valid_to="2024-06-30",
                horizon_value=2,
                value=7.0,
            ),
            _long_forecast(
                horizon="quarter",
                issue_date="2024-04-10",  # lead 0 (smallest → wins)
                valid_from="2024-04-01",
                valid_to="2024-06-30",
                horizon_value=2,
                value=7.0,
            ),
        ],
        runoff_rows=_quarter_runoff(2024, 2),
        hydrograph_rows=[_quarter_norm(2)],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE], long_term_derive_lead=True)

    pairs, _ledger = build_pairs(config, client, "quarter")

    assert len(pairs) == 1
    row = pairs.iloc[0]
    assert row["issue_date"] == "2024-04-10"  # smallest derived lead retained
    assert row["period_key"] == 2
    assert row["lead"] == 2  # stratified by target quarter


def test_quarter_flag_on_stratifies_contingency_per_target_quarter(
    fake_client_factory,
) -> None:
    # One aligned forecast per calendar quarter of 2024.  Under the flag the
    # quarter contingency must break out one per-target-quarter row (Q1–Q4),
    # carrying the target quarter in the "lead" column.
    long_rows = [
        _long_forecast(
            horizon="quarter",
            issue_date="2023-12-25" if q == 1 else f"2024-{_QUARTER_WINDOW[q][0]}",
            valid_from=f"2024-{_QUARTER_WINDOW[q][0]}",
            valid_to=f"2024-{_QUARTER_WINDOW[q][1]}",
            horizon_value=q,
            value=7.0,
        )
        for q in (1, 2, 3, 4)
    ]
    runoff_rows: list[dict[str, object]] = []
    for q in (1, 2, 3, 4):
        runoff_rows.extend(_quarter_runoff(2024, q))
    client = fake_client_factory(
        long_forecasts_rows=long_rows,
        runoff_rows=runoff_rows,
        hydrograph_rows=[_quarter_norm(q) for q in (1, 2, 3, 4)],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE], long_term_derive_lead=True)

    pairs, _ledger = build_pairs(config, client, "quarter")

    assert len(pairs) == 4
    assert sorted(pairs["lead"].tolist()) == [1, 2, 3, 4]

    contingency = count_contingencies(pairs)
    pooled = contingency[
        (contingency["code"] == "POOLED")
        & (contingency["basin"] == "all")
        & (contingency["norm_provenance"] == "all")
        & (contingency["regime"] == "all")
        & (contingency["season"] == "all")
    ]
    assert sorted(pooled["lead"].tolist()) == [1, 2, 3, 4]
    assert (pooled["n_pairs"] == 1).all()


def test_season_flag_on_keeps_all_leads_and_dedups_reissues(fake_client_factory) -> None:
    # A single Apr–Sep season (2024) issued at all four leads 0–3.  Unlike quarter,
    # season's period_key is the constant 1, so the derived lead MUST enter the
    # dedup key: all four genuine leads are retained (like month), and only a true
    # re-issue WITHIN a lead is collapsed (here lead 1 has two issuances → keep the
    # latest).  Season keeps the derived lead as the stratifier (never period_key).
    client = fake_client_factory(
        long_forecasts_rows=[
            _long_forecast(
                horizon="season",
                issue_date="2024-01-15",  # lead 3
                valid_from="2024-04-01",
                valid_to="2024-09-30",
                horizon_value=1,
                value=7.0,
            ),
            _long_forecast(
                horizon="season",
                issue_date="2024-02-10",  # lead 2
                valid_from="2024-04-01",
                valid_to="2024-09-30",
                horizon_value=1,
                value=7.0,
            ),
            _long_forecast(
                horizon="season",
                issue_date="2024-03-01",  # lead 1
                valid_from="2024-04-01",
                valid_to="2024-09-30",
                horizon_value=1,
                value=7.0,
            ),
            _long_forecast(
                horizon="season",
                issue_date="2024-03-20",  # lead 1 re-issue (latest → wins)
                valid_from="2024-04-01",
                valid_to="2024-09-30",
                horizon_value=1,
                value=7.0,
            ),
            _long_forecast(
                horizon="season",
                issue_date="2024-04-01",  # lead 0
                valid_from="2024-04-01",
                valid_to="2024-09-30",
                horizon_value=1,
                value=7.0,
            ),
        ],
        runoff_rows=_season_runoff(2024),
        hydrograph_rows=[_season_norm()],
    )
    config = ForecastSkillEvalConfig(station_filter=[STATION_CODE], long_term_derive_lead=True)

    pairs, _ledger = build_pairs(config, client, "season")

    # All four genuine leads retained; the lead-1 re-issue collapsed to one.
    assert sorted(pairs["lead"].tolist()) == [0, 1, 2, 3]
    assert (pairs["period_key"] == 1).all()  # lead NOT overwritten by period_key
    issue_by_lead = dict(zip(pairs["lead"], pairs["issue_date"], strict=True))
    assert issue_by_lead[1] == "2024-03-20"  # latest issue within lead 1

    # Season contingency stratifies per genuine lead 0–3.
    contingency = count_contingencies(pairs)
    pooled = contingency[
        (contingency["code"] == "POOLED")
        & (contingency["basin"] == "all")
        & (contingency["norm_provenance"] == "all")
        & (contingency["regime"] == "all")
        & (contingency["season"] == "all")
    ]
    assert sorted(pooled["lead"].tolist()) == [0, 1, 2, 3]


def test_month_unchanged_under_long_term_derive_lead(fake_client_factory) -> None:
    # Month's stored horizon_value already IS the lead; the flag must not touch it,
    # dedup it, or drop it as underivable.
    def _client():
        return fake_client_factory(
            long_forecasts_rows=[
                _long_forecast(issue_date="2024-01-25", horizon_value=3),
                _long_forecast(issue_date="2024-02-25", horizon_value=2),
            ],
            runoff_rows=_daily_rows(year=2024, month=4, value=7.0),
            hydrograph_rows=[_april_hydrograph_norm()],
        )

    off_pairs, _ = build_pairs(
        ForecastSkillEvalConfig(station_filter=[STATION_CODE]),
        _client(),
        "month",
    )
    on_pairs, on_ledger = build_pairs(
        ForecastSkillEvalConfig(station_filter=[STATION_CODE], long_term_derive_lead=True),
        _client(),
        "month",
    )

    pd.testing.assert_frame_equal(on_pairs, off_pairs)
    assert sorted(on_pairs["lead"].tolist()) == [2, 3]
    assert ("pair", "long_forecast_lead_underivable") not in on_ledger.counts_by_stage_reason()


def test_quarter_underivable_lead_dropped_under_flag(fake_client_factory) -> None:
    # A calendar-aligned quarter forecast with NO issue date: under the flag it
    # cannot yield a lead, so it is dropped with the dedicated ledger reason
    # instead of pooling as an aggregated (lead-less) row.
    dateless_row = {
        "horizon": "quarter",
        "code": STATION_CODE,
        "valid_from": "2024-04-01",
        "valid_to": "2024-06-30",
        "horizon_value": 2,
        "model_type": "model-a",
        "q": 7.0,
    }

    def _client():
        return fake_client_factory(
            long_forecasts_rows=[dateless_row],
            runoff_rows=_quarter_runoff(2024, 2),
            hydrograph_rows=[_quarter_norm(2)],
        )

    # OFF: legacy behaviour never reaches the underivable guard.  (A date-less row
    # cannot form a pair anyway — the regime step drops it — but crucially NOT with
    # the derive-lead reason.)
    _off_pairs, off_ledger = build_pairs(
        ForecastSkillEvalConfig(station_filter=[STATION_CODE]),
        _client(),
        "quarter",
    )
    assert ("pair", "long_forecast_lead_underivable") not in off_ledger.counts_by_stage_reason()

    # ON: dropped early as underivable, before the regime step even runs.
    on_pairs, on_ledger = build_pairs(
        ForecastSkillEvalConfig(station_filter=[STATION_CODE], long_term_derive_lead=True),
        _client(),
        "quarter",
    )
    assert on_pairs.empty
    assert on_ledger.counts_by_stage_reason() == {("pair", "long_forecast_lead_underivable"): 1}
