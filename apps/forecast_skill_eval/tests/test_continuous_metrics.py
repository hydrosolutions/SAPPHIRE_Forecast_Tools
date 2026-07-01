"""Unit tests for continuous/volume accuracy metrics in continuous_metrics.py.

All fixtures use synthetic station codes 19999/29999 and invented discharge.
No real station codes or discharge values appear in this file.
"""

from __future__ import annotations

import math
import warnings

import numpy as np
import pandas as pd
import pytest

from forecast_skill_eval.continuous_metrics import (
    _EXPECTED_PERIODS,
    CONTINUOUS_METRIC_COLUMNS,
    SEASONAL_VOLUME_COLUMNS,
    SEASONAL_VOLUME_SUMMARY_COLUMNS,
    bias,
    compute_continuous_metrics,
    compute_seasonal_volume,
    days_in_period,
    kge_2009,
    mae,
    nse,
    relative_volume_error,
)
from forecast_skill_eval.ledger import ExclusionLedger
from forecast_skill_eval.prob_metrics import compute_probabilistic_metrics

_GROUP_KEY_COLS = (
    "horizon",
    "model",
    "regime",
    "season",
    "code",
    "basin",
    "norm_provenance",
    "lead",
)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _pair(
    *,
    code: str = "19999",
    horizon: str = "pentad",
    period_key: int = 25,
    year: int = 2020,
    model: str = "TFT",
    regime: str = "all",
    season: str = "irrigation",
    basin: str = "basinA",
    norm_provenance: str = "official",
    lead: object = None,
    forecast_value: float = 10.0,
    observed_value: float = 8.0,
    norm: float = 12.0,
    issue_date: str | None = None,
    grid_id: str = "short5",
) -> dict:
    return {
        "horizon": horizon,
        "code": code,
        "basin": basin,
        "period_key": period_key,
        "year": year,
        "model": model,
        "regime": regime,
        "season": season,
        "lead": lead,
        "issue_date": issue_date,
        "forecast_value": forecast_value,
        "observed_value": observed_value,
        "norm": norm,
        "norm_provenance": norm_provenance,
        "obs_class": "normal",
        "fc_class": "normal",
        "contingency": "TN",
        "fc_q05": np.nan,
        "fc_q10": np.nan,
        "fc_q25": np.nan,
        "fc_q50": forecast_value,
        "fc_q75": np.nan,
        "fc_q90": np.nan,
        "fc_q95": np.nan,
        "fc_grid_id": grid_id,
    }


def _pairs_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _pooled_strata_row(frame: pd.DataFrame, *, code: str) -> pd.Series:
    """Return the fully-pooled-strata continuous-metrics row for a code."""
    match = frame[
        (frame["basin"] == "all")
        & (frame["norm_provenance"] == "all")
        & (frame["regime"] == "all")
        & (frame["season"] == "all")
        & (frame["code"] == code)
    ]
    assert len(match) == 1, f"expected exactly one pooled row, got {len(match)}"
    return match.iloc[0]


# ===========================================================================
# Primitives
# ===========================================================================


class TestBias:
    def test_sign(self):
        assert bias(np.array([3.0, 4.0]), np.array([2.0, 2.0])) == pytest.approx(1.5)

    def test_empty_is_nan(self):
        assert math.isnan(bias(np.array([]), np.array([])))


class TestMae:
    def test_value(self):
        assert mae(np.array([3.0, 1.0]), np.array([2.0, 2.0])) == pytest.approx(1.0)

    def test_empty_is_nan(self):
        assert math.isnan(mae(np.array([]), np.array([])))


class TestRelativeVolumeError:
    def test_fraction(self):
        # sum(fc) = 10, sum(obs) = 8 -> (10 - 8) / 8 = 0.25
        assert relative_volume_error(np.array([6.0, 4.0]), np.array([5.0, 3.0])) == pytest.approx(
            0.25
        )

    def test_sum_obs_zero_is_nan(self):
        assert math.isnan(relative_volume_error(np.array([1.0, 2.0]), np.array([0.0, 0.0])))

    def test_empty_is_nan(self):
        assert math.isnan(relative_volume_error(np.array([]), np.array([])))

    def test_equals_kge_beta_minus_one(self):
        fc = np.array([2.0, 4.0, 6.0, 8.0, 10.0])
        obs = np.array([1.0, 3.0, 5.0, 7.0, 9.0])
        rve = relative_volume_error(fc, obs)
        _, _, _, beta = kge_2009(fc, obs)
        assert rve == pytest.approx(beta - 1.0, abs=1e-12)


class TestNse:
    def test_perfect(self):
        vals = np.array([1.0, 2.0, 3.0, 4.0])
        assert nse(vals, vals) == pytest.approx(1.0)

    def test_zero_at_mean_forecast(self):
        obs = np.array([1.0, 2.0, 3.0])
        fc = np.array([2.0, 2.0, 2.0])  # constant at mean(obs)
        assert nse(fc, obs) == pytest.approx(0.0)

    def test_zero_obs_variance_is_nan(self):
        assert math.isnan(nse(np.array([1.0, 2.0, 3.0]), np.array([5.0, 5.0, 5.0])))

    def test_empty_is_nan(self):
        assert math.isnan(nse(np.array([]), np.array([])))

    def test_golden(self):
        fc = np.array([2.0, 4.0, 6.0, 8.0, 10.0])
        obs = np.array([1.0, 3.0, 5.0, 7.0, 9.0])
        assert nse(fc, obs) == pytest.approx(0.875)


class TestKge2009:
    def test_perfect(self):
        vals = np.array([1.0, 2.0, 3.0, 4.0])
        kge, r, alpha, beta = kge_2009(vals, vals)
        assert kge == pytest.approx(1.0)
        assert r == pytest.approx(1.0)
        assert alpha == pytest.approx(1.0)
        assert beta == pytest.approx(1.0)

    def test_components_golden_ddof0(self):
        fc = np.array([2.0, 4.0, 6.0, 8.0, 10.0])
        obs = np.array([1.0, 3.0, 5.0, 7.0, 9.0])
        kge, r, alpha, beta = kge_2009(fc, obs)
        # Independent closed form (ddof=0).
        exp_r = float(np.corrcoef(fc, obs)[0, 1])
        exp_alpha = float(fc.std(ddof=0) / obs.std(ddof=0))
        exp_beta = float(fc.mean() / obs.mean())
        exp_kge = 1.0 - math.sqrt(
            (exp_r - 1.0) ** 2 + (exp_alpha - 1.0) ** 2 + (exp_beta - 1.0) ** 2
        )
        assert r == pytest.approx(exp_r, abs=1e-9)
        assert alpha == pytest.approx(1.0, abs=1e-9)
        assert beta == pytest.approx(1.2, abs=1e-9)
        assert exp_kge == pytest.approx(0.8, abs=1e-9)
        assert kge == pytest.approx(0.8, abs=1e-9)

    def test_constant_forecast_all_nan_no_warning(self):
        fc = np.array([5.0, 5.0, 5.0])
        obs = np.array([1.0, 2.0, 3.0])
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # any RuntimeWarning fails the test
            result = kge_2009(fc, obs)
        assert all(math.isnan(v) for v in result)

    def test_single_point_is_nan(self):
        result = kge_2009(np.array([1.0]), np.array([1.0]))
        assert all(math.isnan(v) for v in result)

    def test_zero_obs_variance_is_nan(self):
        result = kge_2009(np.array([1.0, 2.0, 3.0]), np.array([4.0, 4.0, 4.0]))
        assert all(math.isnan(v) for v in result)

    def test_zero_mean_obs_is_nan(self):
        # mean(obs) == 0 -> beta undefined
        result = kge_2009(np.array([1.0, 2.0, 3.0]), np.array([-2.0, 0.0, 2.0]))
        assert all(math.isnan(v) for v in result)


class TestDaysInPeriod:
    @pytest.mark.parametrize("sub", [1, 2, 3, 4, 5])
    def test_pentad_sub_1_to_5(self, sub):
        # January sub-periods 1..5 -> period_key = sub
        assert days_in_period("pentad", sub, 2021) == 5

    def test_pentad_6_thirtyone_day_month(self):
        # January (31) sub 6 -> period_key 6
        assert days_in_period("pentad", 6, 2021) == 6

    def test_pentad_6_thirty_day_month(self):
        # April (30) sub 6 -> period_key (4-1)*6 + 6 = 24
        assert days_in_period("pentad", 24, 2021) == 5

    def test_pentad_6_feb_non_leap(self):
        # Feb (28) sub 6 -> period_key (2-1)*6 + 6 = 12
        assert days_in_period("pentad", 12, 2021) == 3

    def test_pentad_6_feb_leap(self):
        assert days_in_period("pentad", 12, 2020) == 4

    @pytest.mark.parametrize("sub", [1, 2])
    def test_decade_sub_1_2(self, sub):
        # January sub 1..2 -> period_key = sub
        assert days_in_period("decade", sub, 2021) == 10

    def test_decade_3_thirtyone_day_month(self):
        # January (31) sub 3 -> period_key 3
        assert days_in_period("decade", 3, 2021) == 11

    def test_decade_3_thirty_day_month(self):
        # April (30) sub 3 -> period_key (4-1)*3 + 3 = 12
        assert days_in_period("decade", 12, 2021) == 10

    def test_decade_3_feb_non_leap(self):
        # Feb (28) sub 3 -> period_key (2-1)*3 + 3 = 6
        assert days_in_period("decade", 6, 2021) == 8

    def test_decade_3_feb_leap(self):
        assert days_in_period("decade", 6, 2020) == 9

    def test_month(self):
        assert days_in_period("month", 1, 2021) == 31
        assert days_in_period("month", 2, 2021) == 28
        assert days_in_period("month", 2, 2020) == 29

    @pytest.mark.parametrize("horizon", ["day", "quarter", "season"])
    def test_gated_horizons_return_none(self, horizon):
        assert days_in_period(horizon, 1, 2021) is None

    def test_out_of_range_period_key(self):
        assert days_in_period("month", 13, 2021) is None
        assert days_in_period("pentad", 0, 2021) is None


# ===========================================================================
# compute_continuous_metrics
# ===========================================================================


class TestComputeContinuousMetrics:
    def test_empty_in_empty_out(self):
        result = compute_continuous_metrics(pd.DataFrame())
        assert list(result.columns) == list(CONTINUOUS_METRIC_COLUMNS)
        assert result.empty

    def test_basic_values(self):
        # 10 pairs, fc = obs + 1: bias=1, mae=1, rve=0.2, kge=0.8, nse=0.875
        obs_vals = [1.0, 3.0, 5.0, 7.0, 9.0, 11.0, 13.0, 15.0, 17.0, 19.0]
        rows = [_pair(period_key=25, observed_value=o, forecast_value=o + 1.0) for o in obs_vals]
        result = compute_continuous_metrics(_pairs_df(rows))
        row = _pooled_strata_row(result, code="19999")
        assert row["n_pairs"] == 10
        assert row["bias"] == pytest.approx(1.0)
        assert row["mae"] == pytest.approx(1.0)
        assert row["rve"] == pytest.approx(0.1)  # sum diff 10 / sum obs 100
        assert row["kge_r"] == pytest.approx(1.0)
        assert row["kge_alpha"] == pytest.approx(1.0)
        assert not math.isnan(row["nse"])

    def test_variance_metrics_suppressed_below_min_pairs(self):
        # 5 pairs -> bias/mae/rve finite, kge*/nse NaN
        rows = [_pair(observed_value=float(i + 1), forecast_value=float(i + 2)) for i in range(5)]
        result = compute_continuous_metrics(_pairs_df(rows))
        row = _pooled_strata_row(result, code="19999")
        assert row["n_pairs"] == 5
        assert not math.isnan(row["bias"])
        assert not math.isnan(row["mae"])
        assert not math.isnan(row["rve"])
        assert math.isnan(row["kge"])
        assert math.isnan(row["nse"])

    def test_n_equals_two_still_suppressed(self):
        # n=2: primitive would let r=+-1 through; reducer must suppress.
        rows = [
            _pair(observed_value=1.0, forecast_value=2.0),
            _pair(observed_value=3.0, forecast_value=4.0),
        ]
        result = compute_continuous_metrics(_pairs_df(rows))
        row = _pooled_strata_row(result, code="19999")
        assert row["n_pairs"] == 2
        assert math.isnan(row["kge"])
        assert math.isnan(row["nse"])
        assert not math.isnan(row["bias"])

    def test_group_key_parity_with_prob_metrics(self):
        # 2 codes x 2 models, pentad (lead None) + month (lead 1).
        rows: list[dict] = []
        for code in ("19999", "29999"):
            for model in ("TFT", "LR"):
                for pk in (25, 26, 27):
                    rows.append(
                        _pair(
                            code=code,
                            model=model,
                            horizon="pentad",
                            period_key=pk,
                            lead=None,
                            observed_value=float(pk),
                            forecast_value=float(pk) + 1.0,
                        )
                    )
                for month in (4, 5, 6):
                    rows.append(
                        _pair(
                            code=code,
                            model=model,
                            horizon="month",
                            period_key=month,
                            lead=1,
                            observed_value=float(month) * 2,
                            forecast_value=float(month) * 2 + 1.0,
                        )
                    )
        pairs = _pairs_df(rows)

        cont = compute_continuous_metrics(pairs)
        prob = compute_probabilistic_metrics(pairs, thresholds={}, clim_ref={}, events_filter=())

        cont_keys = {
            tuple(v) for v in cont[list(_GROUP_KEY_COLS)].itertuples(index=False, name=None)
        }
        prob_keys = {
            tuple(v) for v in prob[list(_GROUP_KEY_COLS)].itertuples(index=False, name=None)
        }
        assert cont_keys == prob_keys


# ===========================================================================
# compute_seasonal_volume
# ===========================================================================


class TestComputeSeasonalVolume:
    def test_empty_in_empty_out(self):
        detail, summary = compute_seasonal_volume(pd.DataFrame())
        assert list(detail.columns) == list(SEASONAL_VOLUME_COLUMNS)
        assert list(summary.columns) == list(SEASONAL_VOLUME_SUMMARY_COLUMNS)
        assert detail.empty
        assert summary.empty

    def test_day_weighted_true_volume(self):
        # May pentads 25..30 -> days [5,5,5,5,5,6], sum 31.
        rows = [
            _pair(period_key=pk, forecast_value=10.0, observed_value=8.0) for pk in range(25, 31)
        ]
        detail, _ = compute_seasonal_volume(_pairs_df(rows))
        assert len(detail) == 1
        row = detail.iloc[0]
        assert row["n_periods"] == 6
        assert row["season_volume_m3_fc"] == pytest.approx(10.0 * 86400 * 31)
        assert row["season_volume_m3_obs"] == pytest.approx(8.0 * 86400 * 31)
        assert row["seasonal_volume_error"] == pytest.approx(0.25)
        assert row["mean_flow_fc"] == pytest.approx(10.0)
        assert row["mean_flow_obs"] == pytest.approx(8.0)
        # Only 6 of 36 expected pentads -> incomplete.
        assert not bool(row["season_complete"])
        assert row["expected_periods"] == 36

    def test_v_obs_zero_error_is_nan(self):
        rows = [
            _pair(period_key=pk, forecast_value=10.0, observed_value=0.0) for pk in range(25, 31)
        ]
        detail, _ = compute_seasonal_volume(_pairs_df(rows))
        assert math.isnan(detail.iloc[0]["seasonal_volume_error"])

    @pytest.mark.parametrize(
        ("horizon", "keys", "expected"),
        [
            ("pentad", range(19, 55), 36),  # Apr(19)..Sep(54)
            ("decade", range(10, 28), 18),  # Apr(10)..Sep(27)
            ("month", range(4, 10), 6),  # Apr..Sep
        ],
    )
    def test_expected_periods_and_complete(self, horizon, keys, expected):
        lead = 1 if horizon == "month" else None
        rows = [
            _pair(
                horizon=horizon,
                period_key=pk,
                lead=lead,
                forecast_value=10.0,
                observed_value=8.0,
            )
            for pk in keys
        ]
        detail, _ = compute_seasonal_volume(_pairs_df(rows))
        assert len(detail) == 1
        row = detail.iloc[0]
        assert row["expected_periods"] == expected
        assert _EXPECTED_PERIODS[horizon] == expected
        assert row["n_periods"] == expected
        assert bool(row["season_complete"])

    def test_off_by_one_flips_complete(self):
        # Drop one month -> 5 of 6.
        rows = [_pair(horizon="month", period_key=m, lead=1) for m in range(4, 9)]
        detail, _ = compute_seasonal_volume(_pairs_df(rows))
        row = detail.iloc[0]
        assert row["n_periods"] == 5
        assert not bool(row["season_complete"])

    def test_duplicate_target_period_deduped_and_logged(self):
        ledger = ExclusionLedger()
        rows = [
            _pair(period_key=25, forecast_value=10.0, observed_value=8.0, issue_date="2020-05-01"),
            _pair(
                period_key=25, forecast_value=99.0, observed_value=8.0, issue_date="2020-05-06"
            ),  # re-issue, latest wins
            _pair(period_key=26, forecast_value=10.0, observed_value=8.0),
        ]
        detail, _ = compute_seasonal_volume(_pairs_df(rows), ledger=ledger)
        row = detail.iloc[0]
        # Deduped: period 25 (latest fc=99) + period 26 (fc=10). No double-count.
        assert row["n_periods"] == 2
        # days(pentad 25)=5, days(pentad 26)=5
        assert row["season_volume_m3_fc"] == pytest.approx((99.0 * 5 + 10.0 * 5) * 86400)
        entries = [
            e
            for e in ledger.entries
            if e.stage == "value" and e.reason == "duplicate_target_period"
        ]
        assert len(entries) == 1

    @pytest.mark.parametrize("horizon", ["day", "quarter", "season"])
    def test_horizon_gate_excludes(self, horizon):
        rows = [_pair(horizon=horizon, period_key=5, lead=1, season="irrigation")]
        detail, summary = compute_seasonal_volume(_pairs_df(rows))
        assert detail.empty
        assert summary.empty

    def test_non_irrigation_excluded(self):
        # January pentads (season non_irrigation) -> excluded.
        rows = [_pair(period_key=pk, season="non_irrigation") for pk in range(1, 7)]
        detail, _ = compute_seasonal_volume(_pairs_df(rows))
        assert detail.empty

    def test_cross_year_summary(self):
        rows: list[dict] = []
        # Year 2019: uniform +25% error; Year 2020: uniform +25% error too.
        for year in (2019, 2020):
            for pk in range(25, 31):
                rows.append(
                    _pair(
                        year=year,
                        period_key=pk,
                        forecast_value=10.0,
                        observed_value=8.0,
                    )
                )
        detail, summary = compute_seasonal_volume(_pairs_df(rows))
        assert len(detail) == 2
        assert len(summary) == 1
        srow = summary.iloc[0]
        assert srow["n_years"] == 2
        assert srow["seasonal_volume_error_mean"] == pytest.approx(0.25)
        assert srow["seasonal_volume_error_median"] == pytest.approx(0.25)
