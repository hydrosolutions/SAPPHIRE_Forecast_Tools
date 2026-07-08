"""Tests for the configurable minimum n_pairs gate (P2 — Defect B).

Goal: verify that the configurable ``K`` floor correctly gates membership in
the long-term EM and SM pools, raises the output floor for monthly/quarter/
seasonal rows, and leaves short-term (pentad/decad) behaviour byte-identical.

Locked invariants:
- A member with n_pairs = K-1 is excluded from EM/SM membership.
- A member with n_pairs = K is included.
- An aggregate row (EM/NM/SM) resolving to n_pairs < K is not emitted.
- An aggregate row resolving to n_pairs >= K is emitted.
- Short-term gate/skill calls receive no min_pairs (behaviour unchanged).
- NM membership stays ungated (all models enter the pool regardless of n_pairs);
  NM *output* rows < K are dropped by the shared output floor.
- Quarter/season EM membership is fixed-LR (AGGREGATED_EM_RAW_MODELS) and is
  NOT skill-gated; only the output floor applies.
- Env var parsing: default (unset) → 4/5; override is honoured; invalid → error.

Placeholder station code: ``19999`` throughout (never a real code).
"""

import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.skill_metrics import (
    _long_term_min_pairs,
    calculate_monthly_skill_metrics,
    calculate_quarterly_skill_metrics,
    calculate_seasonal_skill_metrics,
    filter_for_highly_skilled_forecasts,
)

STATION = "19999"
QUANTILE_COLS = ["q05", "q10", "q25", "q50", "q75", "q90", "q95"]

# K values at defaults (MONTH=4, QUARTER=5, SEASON=5)
K_MONTH = 4
K_QS = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _q_row(q50: float):
    """Expand a single q50 into 7 monotone quantile values."""
    q = float(q50)
    return [q * 0.70, q * 0.75, q * 0.85, q, q * 1.15, q * 1.25, q * 1.30]


def _make_skill_row(
    *,
    model_short: str,
    n_pairs: int,
    nse: float = 0.5,
    sdivsigma: float = 0.5,
    accuracy: float = 0.9,
    mae: float = 10.0,
    month_in_year: int = 3,
    horizon_value: int = 0,
    code: str = STATION,
) -> dict:
    return {
        "month_in_year": month_in_year,
        "horizon_value": horizon_value,
        "code": code,
        "model_short": model_short,
        "n_pairs": n_pairs,
        "nse": nse,
        "sdivsigma": sdivsigma,
        "accuracy": accuracy,
        "mae": mae,
    }


def _make_monthly_obs(rows):
    """Rows: (code, year, month, discharge_avg)."""
    df = pd.DataFrame(rows, columns=["code", "year", "month", "discharge_avg"])
    df["month_in_year"] = df["month"]
    delta_df = (
        df.groupby(["code", "month_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "month_in_year", "delta"]], on=["code", "month_in_year"])


def _make_monthly_fcst(rows):
    """Rows: (code, year, month, model_short, q50)."""
    records = []
    for code, year, month, model, q50 in rows:
        records.append([code, year, month, model] + _q_row(q50))
    return pd.DataFrame(records, columns=["code", "year", "month", "model_short"] + QUANTILE_COLS)


def _make_quarterly_obs(rows):
    """Rows: (code, year, quarter_in_year, discharge_avg)."""
    df = pd.DataFrame(rows, columns=["code", "year", "quarter_in_year", "discharge_avg"])
    delta_df = (
        df.groupby(["code", "quarter_in_year"])
        .agg(std_discharge=("discharge_avg", "std"))
        .reset_index()
    )
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "quarter_in_year", "delta"]], on=["code", "quarter_in_year"])


def _make_quarterly_fcst(rows):
    """Rows: (code, year, quarter_in_year, model_short, q50)."""
    records = []
    for code, year, qiy, model, q50 in rows:
        records.append([code, year, qiy, model] + _q_row(q50))
    return pd.DataFrame(
        records, columns=["code", "year", "quarter_in_year", "model_short"] + QUANTILE_COLS
    )


def _make_seasonal_obs(rows):
    """Rows: (code, season_year, discharge_avg)."""
    df = pd.DataFrame(rows, columns=["code", "season_year", "discharge_avg"])
    df["season_in_year"] = 1
    delta_df = df.groupby(["code"]).agg(std_discharge=("discharge_avg", "std")).reset_index()
    delta_df["delta"] = 0.674 * delta_df["std_discharge"].fillna(0.0)
    return df.merge(delta_df[["code", "delta"]], on=["code"])


def _make_seasonal_fcst(rows):
    """Rows: (code, season_year, season_in_year, model_short, q50)."""
    records = []
    for code, sy, siy, model, q50 in rows:
        records.append([code, sy, siy, model] + _q_row(q50))
    return pd.DataFrame(
        records, columns=["code", "season_year", "season_in_year", "model_short"] + QUANTILE_COLS
    )


# ---------------------------------------------------------------------------
# 1. Env var parsing: _long_term_min_pairs
# ---------------------------------------------------------------------------


class TestLongTermMinPairsHelper:
    """Unit tests for _long_term_min_pairs env-var helper."""

    def test_default_month_is_4(self, monkeypatch):
        """Unset env → MONTH K=4."""
        monkeypatch.delenv("ieasyhydroforecast_min_pairs_long_term", raising=False)
        assert _long_term_min_pairs("MONTH") == 4

    def test_default_quarter_is_5(self, monkeypatch):
        """Unset env → QUARTER K=5."""
        monkeypatch.delenv("ieasyhydroforecast_min_pairs_long_term_quarter", raising=False)
        assert _long_term_min_pairs("QUARTER") == 5

    def test_default_season_is_5(self, monkeypatch):
        """Unset env → SEASON K=5."""
        monkeypatch.delenv("ieasyhydroforecast_min_pairs_long_term_season", raising=False)
        assert _long_term_min_pairs("SEASON") == 5

    def test_case_insensitive(self, monkeypatch):
        """horizon_type is case-insensitive."""
        monkeypatch.delenv("ieasyhydroforecast_min_pairs_long_term", raising=False)
        assert _long_term_min_pairs("month") == 4
        assert _long_term_min_pairs("Month") == 4

    def test_override_month_env_var(self, monkeypatch):
        """Setting the env var overrides the default."""
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_long_term", "6")
        assert _long_term_min_pairs("MONTH") == 6

    def test_override_quarter_env_var(self, monkeypatch):
        """Quarter env var override is honoured."""
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_long_term_quarter", "3")
        assert _long_term_min_pairs("QUARTER") == 3

    def test_invalid_value_raises_clear_error(self, monkeypatch):
        """Non-integer env var raises ValueError with the offending value named."""
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_long_term", "banana")
        with pytest.raises(ValueError, match="banana"):
            _long_term_min_pairs("MONTH")

    def test_zero_value_raises_error(self, monkeypatch):
        """K < 1 is nonsensical and must raise ValueError."""
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_long_term", "0")
        with pytest.raises(ValueError):
            _long_term_min_pairs("MONTH")

    def test_unknown_horizon_raises_error(self):
        """An unrecognised horizon_type raises ValueError."""
        with pytest.raises(ValueError, match="PENTAD"):
            _long_term_min_pairs("PENTAD")


# ---------------------------------------------------------------------------
# 2. filter_for_highly_skilled_forecasts: min_pairs kwarg
# ---------------------------------------------------------------------------


class TestFilterMinPairsKwarg:
    """Unit tests for the new min_pairs keyword on filter_for_highly_skilled_forecasts."""

    def _base_df(self, n_pairs_values: list[int]) -> pd.DataFrame:
        """Build a minimal skill-stats DataFrame with varying n_pairs."""
        rows = []
        for i, np_ in enumerate(n_pairs_values):
            rows.append(
                _make_skill_row(
                    model_short=f"M{i}",
                    n_pairs=np_,
                    nse=0.9,
                    sdivsigma=0.4,
                    accuracy=0.9,
                )
            )
        return pd.DataFrame(rows)

    def test_none_min_pairs_is_byte_identical(self):
        """min_pairs=None: result is exactly the same as calling without the arg."""
        df = self._base_df([1, 2, 3, 5])
        # Override thresholds to pass all rows through the metric gates so
        # the only discriminator is n_pairs.
        result_no_arg = filter_for_highly_skilled_forecasts(
            df, nse=0.0, sdivsigma=10.0, accuracy=0.0
        )
        result_none = filter_for_highly_skilled_forecasts(
            df, min_pairs=None, nse=0.0, sdivsigma=10.0, accuracy=0.0
        )
        pd.testing.assert_frame_equal(
            result_no_arg.reset_index(drop=True), result_none.reset_index(drop=True)
        )

    def test_km1_excluded_k_included(self):
        """n_pairs = K-1 is excluded; n_pairs = K is included."""
        K = 4
        df = self._base_df([K - 1, K, K + 1])
        result = filter_for_highly_skilled_forecasts(
            df, min_pairs=K, nse=0.0, sdivsigma=10.0, accuracy=0.0
        )
        assert K - 1 not in result["n_pairs"].values, "K-1 row must be excluded"
        assert K in result["n_pairs"].values, "K row must be included"
        assert K + 1 in result["n_pairs"].values, "K+1 row must be included"

    def test_applies_after_metric_filters(self):
        """min_pairs is applied AFTER the metric AND-filter, not instead of it."""
        K = 4
        # Row 0: n_pairs=K but nse=-0.5 → fails NSE gate → excluded
        # Row 1: n_pairs=K and nse=0.9 → passes both → included
        rows = [
            _make_skill_row(model_short="A", n_pairs=K, nse=-0.5),
            _make_skill_row(model_short="B", n_pairs=K, nse=0.9),
        ]
        df = pd.DataFrame(rows)
        # Use long-term defaults: NSE>0, sdivsigma/accuracy disabled
        result = filter_for_highly_skilled_forecasts(
            df,
            min_pairs=K,
            nse=0.0,
            sdivsigma=False,
            accuracy=False,
        )
        assert "A" not in result["model_short"].values
        assert "B" in result["model_short"].values

    def test_na_n_pairs_treated_as_zero(self):
        """NaN n_pairs counts as 0 — excluded by any positive K."""
        df = pd.DataFrame([_make_skill_row(model_short="X", n_pairs=4, nse=0.9)])
        df.loc[0, "n_pairs"] = np.nan
        result = filter_for_highly_skilled_forecasts(
            df, min_pairs=4, nse=0.0, sdivsigma=False, accuracy=False
        )
        assert result.empty

    def test_short_term_wrapper_receives_no_min_pairs(self):
        """The legacy wrapper in ensemble_calculator passes no min_pairs → unchanged."""
        # Import the wrapper (not the canonical) to verify it still works
        from src.ensemble_calculator import (
            filter_for_highly_skilled_forecasts as wrapper,
        )

        df = self._base_df([1, 2, 3])
        # The wrapper never sets min_pairs → None → byte-identical to original
        # Both should return the same rows as a plain call with default thresholds.
        canonical_result = filter_for_highly_skilled_forecasts(df)
        wrapper_result = wrapper(df)
        pd.testing.assert_frame_equal(
            canonical_result.reset_index(drop=True),
            wrapper_result.reset_index(drop=True),
        )


# ---------------------------------------------------------------------------
# 3. Monthly output floor at K=4
# ---------------------------------------------------------------------------


class TestMonthlyOutputFloor:
    """Monthly skill rows with n_pairs < K=4 must not be emitted."""

    def _make_n_year_data(self, n_years: int, *, month: int = 3):
        """Build obs + 2-model forecasts for ``n_years`` years on ``month``."""
        obs_rows = [(STATION, 2010 + i, month, 100.0 + i * 2) for i in range(n_years)]
        fcst_rows = [(STATION, 2010 + i, month, "LR_Base", 102.0 + i) for i in range(n_years)] + [
            (STATION, 2010 + i, month, "LR_SM", 98.0 + i) for i in range(n_years)
        ]
        return _make_monthly_obs(obs_rows), _make_monthly_fcst(fcst_rows)

    def test_k_minus_1_pairs_produces_no_rows(self):
        """K-1=3 years → all raw and baseline rows have n_pairs=3 < K=4 → dropped."""
        obs, fcst = self._make_n_year_data(K_MONTH - 1)
        skill_out, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        bad = skill_out[skill_out["n_pairs"].fillna(0) < K_MONTH]
        assert bad.empty, (
            f"Rows with n_pairs < {K_MONTH} survived: "
            f"{bad[['model_short', 'n_pairs']].to_dict('records')}"
        )
        # With 3 years, every group has n_pairs=3 < K=4 → full frame empty
        assert skill_out.empty, (
            f"Expected empty output at n_pairs=3 < K=4, got {len(skill_out)} rows"
        )

    def test_k_pairs_produces_rows(self):
        """K=4 years → n_pairs=4 >= K → rows are retained."""
        obs, fcst = self._make_n_year_data(K_MONTH)
        skill_out, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        raw_rows = skill_out[skill_out["model_short"] == "LR_Base"]
        assert not raw_rows.empty, "LR_Base rows must be present at n_pairs=K"
        assert (raw_rows["n_pairs"] >= K_MONTH).all()

    def test_no_output_row_below_k(self):
        """After calculate_monthly_skill_metrics, no row has n_pairs < K."""
        # Mix: month 3 has K years (kept), month 4 has K-1 years (dropped)
        obs_rows = [(STATION, 2010 + i, 3, 100.0 + i) for i in range(K_MONTH)] + [
            (STATION, 2010 + i, 4, 80.0 + i) for i in range(K_MONTH - 1)
        ]
        fcst_rows = (
            [(STATION, 2010 + i, 3, "LR_Base", 102.0 + i) for i in range(K_MONTH)]
            + [(STATION, 2010 + i, 3, "LR_SM", 99.0 + i) for i in range(K_MONTH)]
            + [(STATION, 2010 + i, 4, "LR_Base", 82.0 + i) for i in range(K_MONTH - 1)]
            + [(STATION, 2010 + i, 4, "LR_SM", 79.0 + i) for i in range(K_MONTH - 1)]
        )
        obs = _make_monthly_obs(obs_rows)
        fcst = _make_monthly_fcst(fcst_rows)
        skill_out, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        bad = skill_out[skill_out["n_pairs"].fillna(0) < K_MONTH]
        assert bad.empty, (
            f"Rows with n_pairs < {K_MONTH} survived: "
            f"{bad[['month_in_year', 'model_short', 'n_pairs']].to_dict('records')}"
        )


# ---------------------------------------------------------------------------
# 4. Monthly EM membership gate: model with n_pairs < K excluded
# ---------------------------------------------------------------------------


class TestMonthlyEMGate:
    """EM membership rejects members with n_pairs < K, accepts n_pairs >= K.

    We can only observe this indirectly: if a model's n_pairs < K, it should
    not appear in any EM composition string.
    """

    def _run_monthly(self, n_pairs_per_model: dict[str, int]):
        """Build a synthetic scenario where each model has the given n_pairs."""
        n_years_max = max(n_pairs_per_model.values())

        obs_rows = [(STATION, 2010 + i, 5, 100.0 + i * 2) for i in range(n_years_max)]
        obs = _make_monthly_obs(obs_rows)

        fcst_rows = []
        for model, n in n_pairs_per_model.items():
            for i in range(n):
                fcst_rows.append((STATION, 2010 + i, 5, model, 102.0 + i))
        fcst = _make_monthly_fcst(fcst_rows)

        skill_out, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        return skill_out

    def test_low_n_model_not_in_em_composition(self):
        """A model with n_pairs=K-1 must not appear in any EM composition."""
        # LR_Base: K-1 years (n_pairs=K-1 → excluded from EM)
        # LR_SM:   K   years (n_pairs=K   → eligible for EM)
        skill_out = self._run_monthly({"LR_Base": K_MONTH - 1, "LR_SM": K_MONTH})
        em_rows = skill_out[skill_out["model_short"] == "EM"]
        if not em_rows.empty:
            compositions = em_rows["composition"].fillna("").tolist()
            for comp in compositions:
                assert "LR_Base" not in comp, (
                    f"LR_Base (n_pairs=K-1) must not appear in EM composition: {comp!r}"
                )

    def test_sufficient_n_model_can_enter_em(self):
        """Both models with n_pairs >= K that pass the default EM gate produce an EM row.

        The monthly EM pool uses the DEFAULT skill gate (nse>0.8, sdivsigma<0.6,
        accuracy>0.8) plus the min_pairs floor.  This fixture is designed so that
        both member models genuinely pass that gate, confirming EM is produced.

        Fixture: obs=[70,90,110,130], LR_Base q50=[84,95,107,118],
        LR_SM q50=[82,94,105,116].  Both yield nse≈0.81, sdivsigma≈0.57,
        accuracy=1.0 — all passing the default thresholds.
        """
        month = 5
        obs_vals = [70.0, 90.0, 110.0, 130.0]  # 4 years = K_MONTH
        lr_base_q50 = [84.0, 95.0, 107.0, 118.0]  # nse≈0.813, sdivsigma≈0.570
        lr_sm_q50 = [82.0, 94.0, 105.0, 116.0]  # nse≈0.810, sdivsigma≈0.565

        obs_rows = [(STATION, 2010 + i, month, obs_vals[i]) for i in range(K_MONTH)]
        fcst_rows = [
            (STATION, 2010 + i, month, "LR_Base", lr_base_q50[i]) for i in range(K_MONTH)
        ] + [(STATION, 2010 + i, month, "LR_SM", lr_sm_q50[i]) for i in range(K_MONTH)]
        obs = _make_monthly_obs(obs_rows)
        fcst = _make_monthly_fcst(fcst_rows)
        skill_out, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_rows = skill_out[skill_out["model_short"] == "EM"]
        assert not em_rows.empty, (
            "Expected an EM row when both models pass the default gate "
            "(nse>0.8, sdivsigma<0.6, accuracy>0.8) with n_pairs >= K"
        )

    def test_em_membership_gates_on_min_pairs(self, monkeypatch):
        """min_pairs — not the NSE gate — controls whether EM members qualify.

        Both models pass the default EM gate (nse≈0.82, sdivsigma≈0.57,
        accuracy=1.0) and have n_pairs=5 (≥K=4 but <10).

        With ieasyhydroforecast_min_pairs_long_term=4 → EM row IS produced
        (n_pairs=5 ≥ K=4, metric gate passes).
        With ieasyhydroforecast_min_pairs_long_term=10 → EM row ABSENT
        (n_pairs=5 < K=10, members are excluded by the min_pairs floor).

        Fixture: obs=[70,85,100,115,130], LR_Base q50=[84,92,101,110,118],
        LR_SM q50=[82,90,99,108,116].  Both yield nse≈0.82, sdivsigma≈0.57,
        accuracy=1.0 — all passing the default thresholds.
        """
        month = 8
        n_years = 5  # n_pairs = 5 ∈ [K=4, 10)
        obs_vals = [70.0, 85.0, 100.0, 115.0, 130.0]
        lr_base_q50 = [84.0, 92.0, 101.0, 110.0, 118.0]  # nse≈0.816, sdivsigma≈0.574
        lr_sm_q50 = [82.0, 90.0, 99.0, 108.0, 116.0]  # nse≈0.816, sdivsigma≈0.574

        obs_rows = [(STATION, 2010 + i, month, obs_vals[i]) for i in range(n_years)]
        fcst_rows = [
            (STATION, 2010 + i, month, "LR_Base", lr_base_q50[i]) for i in range(n_years)
        ] + [(STATION, 2010 + i, month, "LR_SM", lr_sm_q50[i]) for i in range(n_years)]
        obs = _make_monthly_obs(obs_rows)
        fcst = _make_monthly_fcst(fcst_rows)

        # --- K=4: members have n_pairs=5 >= K → EM row produced ---
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_long_term", "4")
        skill_k4, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_k4 = skill_k4[skill_k4["model_short"] == "EM"]
        assert not em_k4.empty, (
            "EM row must be present when n_pairs=5 >= min_pairs=4 "
            "and both models pass the default skill gate"
        )

        # --- K=10: members have n_pairs=5 < K → excluded from EM → absent ---
        monkeypatch.setenv("ieasyhydroforecast_min_pairs_long_term", "10")
        skill_k10, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        em_k10 = skill_k10[skill_k10["model_short"] == "EM"]
        assert em_k10.empty, (
            "EM row must be absent when n_pairs=5 < min_pairs=10 "
            "(min_pairs gate excludes both members from the EM pool)"
        )


# ---------------------------------------------------------------------------
# 5. NM membership stays ungated; NM output row < K is floored
# ---------------------------------------------------------------------------


class TestNaiveMeanMembership:
    """NM membership must include all models regardless of n_pairs."""

    def test_nm_includes_low_n_models_but_output_floor_applies(self):
        """All models (including n_pairs < K) feed NM; NM output rows < K are dropped.

        We verify NM is NOT excluded from computation (it draws from all raw
        models without a skill gate). However, the shared output floor drops
        NM rows where the resulting n_pairs < K.
        """
        # 3 years (K-1 < K=4) — NM should be computed from all models
        # but the shared output floor drops the NM row if n_pairs=3 < 4.
        obs_rows = [(STATION, 2010 + i, 6, 100.0 + i) for i in range(K_MONTH - 1)]
        fcst_rows = [(STATION, 2010 + i, 6, "LR_Base", 102.0 + i) for i in range(K_MONTH - 1)] + [
            (STATION, 2010 + i, 6, "LR_SM", 99.0 + i) for i in range(K_MONTH - 1)
        ]
        obs = _make_monthly_obs(obs_rows)
        fcst = _make_monthly_fcst(fcst_rows)
        skill_out, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        # With n_pairs=3 < K=4, the NM output row is dropped by the output floor
        nm_rows = skill_out[skill_out["model_short"] == "Naive Mean"]
        assert nm_rows.empty, "NM output row with n_pairs=K-1 must be dropped by the output floor"

    def test_nm_output_present_when_n_pairs_gte_k(self):
        """NM output row is present when n_pairs >= K."""
        obs_rows = [(STATION, 2010 + i, 7, 100.0 + i) for i in range(K_MONTH)]
        fcst_rows = [(STATION, 2010 + i, 7, "LR_Base", 102.0 + i) for i in range(K_MONTH)] + [
            (STATION, 2010 + i, 7, "LR_SM", 99.0 + i) for i in range(K_MONTH)
        ]
        obs = _make_monthly_obs(obs_rows)
        fcst = _make_monthly_fcst(fcst_rows)
        skill_out, _, _ = calculate_monthly_skill_metrics(obs, fcst)
        nm_rows = skill_out[skill_out["model_short"] == "Naive Mean"]
        assert not nm_rows.empty, "NM output row must be present when n_pairs >= K"
        assert (nm_rows["n_pairs"] >= K_MONTH).all()


# ---------------------------------------------------------------------------
# 6. Quarter/season output floor at K=5
# ---------------------------------------------------------------------------


class TestAggregatedOutputFloor:
    """Quarterly and seasonal output rows with n_pairs < K=5 must not be emitted."""

    def test_quarterly_k_minus_1_produces_no_rows(self):
        """K-1=4 years → all quarterly rows have n_pairs=4 < K=5 → dropped."""
        n = K_QS - 1
        obs_rows = [(STATION, 2010 + i, 1, 100.0 + i * 2) for i in range(n)]
        fcst_rows = [(STATION, 2010 + i, 1, "LR_Base", 102.0 + i) for i in range(n)] + [
            (STATION, 2010 + i, 1, "LR_SM", 98.0 + i) for i in range(n)
        ]
        obs = _make_quarterly_obs(obs_rows)
        fcst = _make_quarterly_fcst(fcst_rows)
        skill_out, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        bad = skill_out[skill_out["n_pairs"].fillna(0) < K_QS]
        assert bad.empty, (
            f"Quarterly rows with n_pairs < {K_QS} survived: "
            f"{bad[['model_short', 'n_pairs']].to_dict('records')}"
        )

    def test_quarterly_k_pairs_produces_rows(self):
        """K=5 quarters → n_pairs=5 >= K → rows are retained."""
        n = K_QS
        obs_rows = [(STATION, 2010 + i, 1, 100.0 + i * 2) for i in range(n)]
        fcst_rows = [(STATION, 2010 + i, 1, "LR_Base", 102.0 + i) for i in range(n)] + [
            (STATION, 2010 + i, 1, "LR_SM", 98.0 + i) for i in range(n)
        ]
        obs = _make_quarterly_obs(obs_rows)
        fcst = _make_quarterly_fcst(fcst_rows)
        skill_out, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        raw_rows = skill_out[skill_out["model_short"] == "LR_Base"]
        assert not raw_rows.empty, f"LR_Base rows must be present at n_pairs=K={K_QS}"
        assert (raw_rows["n_pairs"] >= K_QS).all()

    def test_seasonal_k_minus_1_produces_no_rows(self):
        """K-1=4 seasonal years → n_pairs=4 < K=5 → dropped."""
        n = K_QS - 1
        obs_rows = [(STATION, 2010 + i, 100.0 + i * 2) for i in range(n)]
        fcst_rows = [(STATION, 2010 + i, 1, "LR_Base", 102.0 + i) for i in range(n)] + [
            (STATION, 2010 + i, 1, "LR_SM", 98.0 + i) for i in range(n)
        ]
        obs = _make_seasonal_obs(obs_rows)
        fcst = _make_seasonal_fcst(fcst_rows)
        skill_out, _, _ = calculate_seasonal_skill_metrics(obs, fcst)
        bad = skill_out[skill_out["n_pairs"].fillna(0) < K_QS]
        assert bad.empty, (
            f"Seasonal rows with n_pairs < {K_QS} survived: "
            f"{bad[['model_short', 'n_pairs']].to_dict('records')}"
        )

    def test_seasonal_k_pairs_produces_rows(self):
        """K=5 seasonal years → n_pairs=5 >= K → rows are retained."""
        n = K_QS
        obs_rows = [(STATION, 2010 + i, 100.0 + i * 2) for i in range(n)]
        fcst_rows = [(STATION, 2010 + i, 1, "LR_Base", 102.0 + i) for i in range(n)] + [
            (STATION, 2010 + i, 1, "LR_SM", 98.0 + i) for i in range(n)
        ]
        obs = _make_seasonal_obs(obs_rows)
        fcst = _make_seasonal_fcst(fcst_rows)
        skill_out, _, _ = calculate_seasonal_skill_metrics(obs, fcst)
        raw_rows = skill_out[skill_out["model_short"] == "LR_Base"]
        assert not raw_rows.empty, f"LR_Base rows must be present at n_pairs=K={K_QS}"
        assert (raw_rows["n_pairs"] >= K_QS).all()


# ---------------------------------------------------------------------------
# 7. Quarter/season EM membership is fixed-LR (not skill-gated)
# ---------------------------------------------------------------------------


class TestAggregatedEMFixedMembership:
    """Quarter/season EM derives from AGGREGATED_EM_RAW_MODELS, not a skill gate.

    The output floor still applies to the EM row, but membership is NOT gated
    by skill thresholds or n_pairs.
    """

    def test_quarterly_em_present_when_two_lr_models_available(self):
        """EM is built from LR_Base + LR_SM regardless of their skill/n_pairs,
        as long as the resulting n_pairs >= K (output floor)."""
        n = K_QS  # enough pairs to survive the output floor
        obs_rows = [(STATION, 2010 + i, 2, 100.0 + i * 2) for i in range(n)]
        # Use very poor NSE values that would fail the skill gate — but EM must
        # still be formed from the two LR models (membership is fixed-LR).
        # We set forecast values close to obs so the EM itself has decent NSE,
        # even if hypothetically we were gating on it (which we are not for EM).
        fcst_rows = [(STATION, 2010 + i, 2, "LR_Base", 102.0 + i) for i in range(n)] + [
            (STATION, 2010 + i, 2, "LR_SM", 98.0 + i) for i in range(n)
        ]
        obs = _make_quarterly_obs(obs_rows)
        fcst = _make_quarterly_fcst(fcst_rows)
        skill_out, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        em_rows = skill_out[skill_out["model_short"] == "EM"]
        assert not em_rows.empty, "Quarter EM must be present when both LR models have n_pairs >= K"

    def test_quarterly_em_output_floor_still_applies(self):
        """EM output row with n_pairs < K is dropped by the output floor."""
        n = K_QS - 1  # not enough pairs
        obs_rows = [(STATION, 2010 + i, 2, 100.0 + i * 2) for i in range(n)]
        fcst_rows = [(STATION, 2010 + i, 2, "LR_Base", 102.0 + i) for i in range(n)] + [
            (STATION, 2010 + i, 2, "LR_SM", 98.0 + i) for i in range(n)
        ]
        obs = _make_quarterly_obs(obs_rows)
        fcst = _make_quarterly_fcst(fcst_rows)
        skill_out, _, _ = calculate_quarterly_skill_metrics(obs, fcst)
        em_rows = skill_out[skill_out["model_short"] == "EM"]
        assert em_rows.empty, "EM output row with n_pairs=K-1 must be dropped by the output floor"


# ---------------------------------------------------------------------------
# 8. Short-term path is byte-unchanged
# ---------------------------------------------------------------------------


class TestShortTermUnchanged:
    """The short-term (pentad/decad) skill path must be byte-identical.

    We verify that:
    1. Setting the long-term env vars has no effect on short-term output.
    2. The short-term call at calculate_skill_metrics (create_ensemble_forecasts
       path) does not receive min_pairs.
    """

    def test_monthly_env_var_does_not_affect_short_term_filter(self, monkeypatch):
        """The long-term env var must not change short-term filter behaviour.

        The short-term filter_for_highly_skilled_forecasts call at
        skill_metrics.py:~1958 uses no min_pairs.  We verify this by comparing
        the canonical filter output with and without the env var set.
        """
        # Build a minimal skill_stats with n_pairs < K_MONTH but good metrics
        # so it would pass the default short-term gates.
        rows = [_make_skill_row(model_short="LR", n_pairs=2, nse=0.9, sdivsigma=0.4, accuracy=0.9)]
        df = pd.DataFrame(rows)

        monkeypatch.delenv("ieasyhydroforecast_min_pairs_long_term", raising=False)
        result_no_env = filter_for_highly_skilled_forecasts(df)

        monkeypatch.setenv("ieasyhydroforecast_min_pairs_long_term", "6")
        result_with_env = filter_for_highly_skilled_forecasts(df)

        # The canonical function called with no min_pairs (short-term path)
        # must be identical regardless of the env var (env var is only read
        # by _long_term_min_pairs, not by the filter itself when min_pairs=None).
        pd.testing.assert_frame_equal(
            result_no_env.reset_index(drop=True),
            result_with_env.reset_index(drop=True),
        )

    def test_create_ensemble_forecasts_short_term_unaffected(self, monkeypatch):
        """create_ensemble_forecasts (short-term EM path) ignores long-term env vars."""
        from src.ensemble_calculator import create_ensemble_forecasts

        monkeypatch.setenv("ieasyhydroforecast_min_pairs_long_term", "99")

        # Build minimal forecasts and skill_stats for 2 pentads, 2 models,
        # 3+ years so n_pairs >= 2 but < 99 (would be filtered if min_pairs=99).
        n_years = 3
        period_col = "pentad_in_year"
        pentad = 5
        fcst_rows = []
        skill_rows = []
        for i in range(n_years):
            for model, _nse in [("LR", 0.95), ("TFT", 0.90)]:
                fcst_rows.append(
                    {
                        "code": STATION,
                        "date": pd.Timestamp(f"{2010 + i}-01-26"),
                        period_col: pentad,
                        "model_short": model,
                        "forecasted_discharge": 100.0 + i,
                        "pentad_in_month": 1,
                    }
                )
        for model, nse in [("LR", 0.95), ("TFT", 0.90)]:
            skill_rows.append(
                {
                    period_col: pentad,
                    "code": STATION,
                    "model_short": model,
                    "nse": nse,
                    "sdivsigma": 0.4,
                    "accuracy": 0.9,
                    "mae": 5.0,
                    "n_pairs": n_years,
                }
            )

        forecasts = pd.DataFrame(fcst_rows)
        skill_stats = pd.DataFrame(skill_rows)

        import tag_library as tl

        joint, _ = create_ensemble_forecasts(
            forecasts,
            skill_stats,
            period_col=period_col,
            period_in_month_col="pentad_in_month",
            get_period_in_month_func=tl.get_pentad,
        )
        # EM should still be present (n_pairs=3 < 99 would fail long-term gate
        # but the short-term path must NOT apply min_pairs).
        em_rows = joint[joint["model_short"] == "EM"]
        assert not em_rows.empty, (
            "Short-term EM must be present — create_ensemble_forecasts must "
            "not apply the long-term min_pairs gate"
        )


# ---------------------------------------------------------------------------
# 9. Monthly EM membership is lead-aware (per-horizon_value)
# ---------------------------------------------------------------------------


class TestMonthlyEMLeadAwareMembership:
    """Monthly EM pool membership must be per (month_in_year, horizon_value, code, model_short)
    when horizon_value is present on both the forecast and skill sides.

    Scenario: model LR_SM has n_pairs = K-1 at lead 0 and n_pairs = K at lead 1.
    Both leads pass the default skill gate (nse, sdivsigma, accuracy).
    LR_Base has n_pairs = K at both leads (always eligible).

    Expected:
    - lead 0 EM: LR_Base only (LR_SM excluded by min_pairs floor) → single-model
      composition → EM is ABSENT (is_multi_model_composition guard).
    - lead 1 EM: LR_Base + LR_SM (both eligible) → EM IS PRESENT.

    This locks per-lead EM membership under the min_pairs floor.
    Placeholder station code: ``19999``.
    """

    _SKILL_COLS = [
        "month_in_year",
        "horizon_value",
        "code",
        "model_short",
        "sdivsigma",
        "nse",
        "delta",
        "accuracy",
        "mae",
        "n_pairs",
    ]
    _FC_COLS = [
        "code",
        "year",
        "month",
        "month_in_year",
        "horizon_value",
        "model_short",
        "forecasted_discharge",
        "q05",
        "q10",
        "q25",
        "q50",
        "q75",
        "q90",
        "q95",
        "valid_from",
        "valid_to",
        "date",
        "flag",
    ]

    def _fc_row(self, model, q50, *, hv, month=3):
        """One monthly forecast row with a symmetric quantile fan."""
        spread = 20.0
        return (
            STATION,
            2025,
            month,
            month,
            hv,
            model,
            q50,
            q50 - spread,
            q50 - spread * 0.75,
            q50 - spread * 0.5,
            q50,
            q50 + spread * 0.5,
            q50 + spread * 0.75,
            q50 + spread,
            "2025-03-01",
            "2025-03-31",
            "2025-03-01",
            0,
        )

    def _skill_row(self, model, n_pairs, hv, *, month=3):
        """One skill row that passes the DEFAULT EM gate (nse=0.9, sdivsigma=0.3,
        accuracy=0.9) with the given n_pairs."""
        return (month, hv, STATION, model, 0.30, 0.90, 5.0, 0.90, 2.0, n_pairs)

    def test_lead_aware_em_excludes_model_at_starved_lead(self, monkeypatch):
        """LR_SM with n_pairs=K-1 at lead 0 must NOT appear in EM at lead 0;
        the same model with n_pairs=K at lead 1 MUST appear in EM at lead 1.

        This is the locked regression for the EM lead-aware merge_keys fix.
        """
        monkeypatch.delenv("ieasyhydroforecast_min_pairs_long_term", raising=False)
        K = K_MONTH  # 4

        from src.ensemble_calculator import create_monthly_ensemble_forecasts

        skill = pd.DataFrame(
            [
                # lead 0: LR_Base eligible (n_pairs=K), LR_SM starved (n_pairs=K-1)
                self._skill_row("LR_Base", K, hv=0),
                self._skill_row("LR_SM", K - 1, hv=0),
                # lead 1: both eligible (n_pairs=K)
                self._skill_row("LR_Base", K, hv=1),
                self._skill_row("LR_SM", K, hv=1),
            ],
            columns=self._SKILL_COLS,
        )
        forecasts = pd.DataFrame(
            [
                self._fc_row("LR_Base", 100.0, hv=0),
                self._fc_row("LR_SM", 105.0, hv=0),
                self._fc_row("LR_Base", 110.0, hv=1),
                self._fc_row("LR_SM", 115.0, hv=1),
            ],
            columns=self._FC_COLS,
        )

        result = create_monthly_ensemble_forecasts(forecasts, skill)

        em = result[result["model_short"] == "EM"]
        em_lead0 = em[em["horizon_value"] == 0]
        em_lead1 = em[em["horizon_value"] == 1]

        # Lead 0: LR_SM excluded by min_pairs → only LR_Base qualifies →
        # single-model composition → EM must be absent (is_multi_model_composition).
        assert em_lead0.empty, (
            "EM at lead 0 must be absent: LR_SM (n_pairs=K-1) is excluded by "
            "the per-lead min_pairs floor, leaving only LR_Base — single-model "
            f"ensembles are discarded. Got: {em_lead0[['composition']].to_dict('records')}"
        )

        # Lead 1: both LR_Base and LR_SM have n_pairs=K → EM must be present.
        assert not em_lead1.empty, (
            "EM at lead 1 must be present: both LR_Base and LR_SM have "
            f"n_pairs={K} >= K={K} at this lead."
        )
        composition_lead1 = em_lead1.iloc[0]["composition"]
        assert "LR_SM" in composition_lead1, (
            f"LR_SM must appear in lead-1 EM composition (n_pairs=K), "
            f"got composition: {composition_lead1!r}"
        )
        assert "LR_Base" in composition_lead1, (
            f"LR_Base must appear in lead-1 EM composition, got composition: {composition_lead1!r}"
        )
