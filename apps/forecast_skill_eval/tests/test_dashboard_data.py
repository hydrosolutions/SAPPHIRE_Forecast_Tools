"""Unit tests for forecast_skill_eval.dashboard.data.

Uses only small inline fixtures — no real station codes (uses 19999 / 29999).
No Streamlit dependency: data.py is pure pandas.
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
import pytest

from forecast_skill_eval.dashboard.data import (
    available_options,
    distinct_values,
    filter_metrics,
    filter_prob_by_grid,
    load_metrics,
    load_prob_metrics,
    load_reliability,
    per_station,
    pooled_row,
    rank_stations,
)

# ---------------------------------------------------------------------------
# Helpers / fixture builders
# ---------------------------------------------------------------------------


def _base_row(**overrides) -> dict:
    """Return a minimal valid row with safe defaults.

    Uses synthetic station codes 19999 / 29999 — never real codes.
    """
    row = {
        "horizon": "pentad",
        "model": "LR",
        "regime": "operational",
        "season": "all",
        "code": "19999",
        "basin": "all",
        "norm_provenance": "all",
        "lead": float("nan"),
        "event": "below_norm",
        "TP": 3,
        "FP": 1,
        "FN": 2,
        "TN": 10,
        "n_pairs": 16,
        "base_rate": 0.3125,
        "base_rate_undefined": False,
        "pod": 0.6,
        "pod_undefined": False,
        "far": 0.25,
        "far_undefined": False,
        "pofd": 0.09,
        "pofd_undefined": False,
        "csi": 0.5,
        "csi_undefined": False,
        "frequency_bias": 0.8,
        "frequency_bias_undefined": False,
        "hss": 0.45,
        "hss_undefined": False,
        "pss": 0.5,
        "pss_undefined": False,
        "pod_ci_lower": 0.3,
        "pod_ci_upper": 0.85,
        "pod_ci_undefined": False,
        "far_ci_lower": 0.05,
        "far_ci_upper": 0.55,
        "far_ci_undefined": False,
    }
    row.update(overrides)
    return row


def _make_df(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def _write_csv(tmp_path: Path, *rows: dict) -> Path:
    df = _make_df(*rows)
    path = tmp_path / "contingency_metrics.csv"
    df.to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# load_metrics
# ---------------------------------------------------------------------------


class TestLoadMetrics:
    def test_reads_csv_and_returns_dataframe(self, tmp_path: Path):
        path = _write_csv(tmp_path, _base_row())
        df = load_metrics(path)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1

    def test_event_column_absent_synthesised_as_below_norm(self, tmp_path: Path):
        """When the CSV has no 'event' column, load_metrics adds it as 'below_norm'."""
        row = _base_row()
        del row["event"]
        df_raw = _make_df(row)
        path = tmp_path / "metrics.csv"
        df_raw.to_csv(path, index=False)

        df = load_metrics(path)
        assert "event" in df.columns
        assert (df["event"] == "below_norm").all()

    def test_event_column_present_preserved(self, tmp_path: Path):
        path = _write_csv(tmp_path, _base_row(event="low_p10"))
        df = load_metrics(path)
        assert df["event"].iloc[0] == "low_p10"

    def test_lead_column_numeric(self, tmp_path: Path):
        path = _write_csv(tmp_path, _base_row(), _base_row(lead=2.0))
        df = load_metrics(path)
        assert pd.api.types.is_float_dtype(df["lead"])

    def test_lead_nan_preserved(self, tmp_path: Path):
        path = _write_csv(tmp_path, _base_row())
        df = load_metrics(path)
        assert math.isnan(df["lead"].iloc[0])

    def test_multiple_rows_loaded(self, tmp_path: Path):
        path = _write_csv(
            tmp_path,
            _base_row(code="19999"),
            _base_row(code="29999"),
            _base_row(code="POOLED"),
        )
        df = load_metrics(path)
        assert len(df) == 3


# ---------------------------------------------------------------------------
# per_station / pooled_row
# ---------------------------------------------------------------------------


class TestPerStationAndPooled:
    @pytest.fixture()
    def df_mixed(self) -> pd.DataFrame:
        return _make_df(
            _base_row(code="19999"),
            _base_row(code="29999"),
            _base_row(code="POOLED"),
        )

    def test_per_station_excludes_pooled(self, df_mixed: pd.DataFrame):
        result = per_station(df_mixed)
        assert "POOLED" not in result["code"].values
        assert len(result) == 2

    def test_per_station_includes_both_stations(self, df_mixed: pd.DataFrame):
        result = per_station(df_mixed)
        assert set(result["code"]) == {"19999", "29999"}

    def test_per_station_empty_when_only_pooled(self):
        df = _make_df(_base_row(code="POOLED"))
        assert per_station(df).empty

    def test_pooled_row_returns_series(self, df_mixed: pd.DataFrame):
        result = pooled_row(df_mixed)
        assert result is not None
        assert isinstance(result, pd.Series)
        assert result["code"] == "POOLED"

    def test_pooled_row_returns_none_when_absent(self):
        df = _make_df(_base_row(code="19999"), _base_row(code="29999"))
        assert pooled_row(df) is None

    def test_pooled_row_returns_first_when_multiple(self):
        df = _make_df(
            _base_row(code="POOLED", pod=0.7),
            _base_row(code="POOLED", pod=0.8),
        )
        result = pooled_row(df)
        assert result is not None
        assert result["pod"] == pytest.approx(0.7)

    # ------------------------------------------------------------------
    # basin-column scoping (new in this fix)
    # ------------------------------------------------------------------

    def test_per_station_basin_col_returns_only_all_basin_row(self):
        """Each station has basin='all' and basin='other'; only 'all' returned.

        Without the fix, Altair would SUM both rows → POD≈2.  With the fix,
        per_station returns exactly one row per station (the 'all' aggregate).
        """
        df = _make_df(
            _base_row(code="19999", basin="all", pod=0.25),
            _base_row(code="19999", basin="other", pod=0.25),
        )
        result = per_station(df)
        assert len(result) == 1
        assert result["basin"].iloc[0] == "all"
        assert result["pod"].iloc[0] == pytest.approx(0.25)

    def test_per_station_no_basin_col_returns_all_non_pooled(self):
        """Frames without a basin column work exactly as before."""
        df = _make_df(
            _base_row(code="19999"),
            _base_row(code="29999"),
            _base_row(code="POOLED"),
        )
        df = df.drop(columns=["basin"])
        result = per_station(df)
        assert "basin" not in result.columns
        assert set(result["code"]) == {"19999", "29999"}
        assert len(result) == 2

    def test_pooled_row_prefers_basin_all_when_both_exist(self):
        """POOLED with basin='all' (pod=0.7) beats basin='other' (pod=0.8)."""
        df = _make_df(
            _base_row(code="POOLED", basin="all", pod=0.7),
            _base_row(code="POOLED", basin="other", pod=0.8),
        )
        result = pooled_row(df)
        assert result is not None
        assert result["pod"] == pytest.approx(0.7)
        assert result["basin"] == "all"

    def test_pooled_row_no_basin_col_returns_pooled(self):
        """Frames without basin column still return the single POOLED row."""
        df = _make_df(
            _base_row(code="19999"),
            _base_row(code="POOLED", pod=0.55),
        )
        df = df.drop(columns=["basin"])
        result = pooled_row(df)
        assert result is not None
        assert result["code"] == "POOLED"
        assert result["pod"] == pytest.approx(0.55)

    def test_pooled_row_no_basin_col_returns_none_when_absent(self):
        """No POOLED rows and no basin column → None."""
        df = _make_df(
            _base_row(code="19999"),
            _base_row(code="29999"),
        )
        df = df.drop(columns=["basin"])
        assert pooled_row(df) is None


# ---------------------------------------------------------------------------
# filter_metrics
# ---------------------------------------------------------------------------


class TestFilterMetrics:
    @pytest.fixture()
    def df_multi(self) -> pd.DataFrame:
        return _make_df(
            _base_row(code="19999", model="LR", season="all", regime="operational"),
            _base_row(code="19999", model="LR", season="irrigation", regime="operational"),
            _base_row(code="19999", model="GBT", season="all", regime="operational"),
            _base_row(code="29999", model="LR", season="all", regime="hindcast"),
            _base_row(code="POOLED", model="LR", season="all", regime="operational"),
        )

    def test_basic_filter_horizon_season_regime(self, df_multi: pd.DataFrame):
        result = filter_metrics(
            df_multi,
            horizon="pentad",
            event="below_norm",
            season="all",
            regime="operational",
            norm_provenance="all",
        )
        assert len(result) == 3  # 19999/LR, 19999/GBT, POOLED

    def test_model_string_filter(self, df_multi: pd.DataFrame):
        result = filter_metrics(
            df_multi,
            horizon="pentad",
            event="below_norm",
            season="all",
            regime="operational",
            norm_provenance="all",
            model="LR",
        )
        assert all(result["model"] == "LR")
        assert len(result) == 2  # 19999 + POOLED

    def test_model_list_filter(self, df_multi: pd.DataFrame):
        result = filter_metrics(
            df_multi,
            horizon="pentad",
            event="below_norm",
            season="all",
            regime="operational",
            norm_provenance="all",
            model=["LR", "GBT"],
        )
        assert set(result["model"]) == {"LR", "GBT"}

    def test_model_none_returns_all_models(self, df_multi: pd.DataFrame):
        result = filter_metrics(
            df_multi,
            horizon="pentad",
            event="below_norm",
            season="all",
            regime="operational",
            norm_provenance="all",
            model=None,
        )
        assert set(result["model"]) == {"LR", "GBT"}

    def test_lead_none_matches_nan_rows(self, tmp_path: Path):
        """lead=None must match rows where lead is NaN (short-term)."""
        df = _make_df(
            _base_row(lead=float("nan")),
            _base_row(lead=1.0),
            _base_row(lead=2.0),
        )
        result = filter_metrics(
            df,
            horizon="pentad",
            event="below_norm",
            season="all",
            regime="operational",
            norm_provenance="all",
            lead=None,
        )
        assert len(result) == 1
        assert math.isnan(result["lead"].iloc[0])

    def test_lead_integer_matches_exact(self):
        """lead=2 must match only rows with lead == 2.0."""
        df = _make_df(
            _base_row(lead=float("nan")),
            _base_row(lead=1.0),
            _base_row(lead=2.0),
            _base_row(lead=2.0, code="29999"),
        )
        result = filter_metrics(
            df,
            horizon="pentad",
            event="below_norm",
            season="all",
            regime="operational",
            norm_provenance="all",
            lead=2,
        )
        assert len(result) == 2
        assert (result["lead"] == 2.0).all()

    def test_empty_result_when_no_match(self, df_multi: pd.DataFrame):
        result = filter_metrics(
            df_multi,
            horizon="month",  # not present in fixture
            event="below_norm",
            season="all",
            regime="operational",
            norm_provenance="all",
        )
        assert result.empty

    def test_event_filter_applied(self):
        df = _make_df(
            _base_row(event="below_norm"),
            _base_row(event="low_p10"),
        )
        result = filter_metrics(
            df,
            horizon="pentad",
            event="low_p10",
            season="all",
            regime="operational",
            norm_provenance="all",
        )
        assert len(result) == 1
        assert result["event"].iloc[0] == "low_p10"

    def test_result_is_a_copy(self, df_multi: pd.DataFrame):
        result = filter_metrics(
            df_multi,
            horizon="pentad",
            event="below_norm",
            season="all",
            regime="operational",
            norm_provenance="all",
        )
        result["pod"] = 999.0
        # Original must be unchanged.
        assert (df_multi["pod"] != 999.0).all()


# ---------------------------------------------------------------------------
# rank_stations
# ---------------------------------------------------------------------------


class TestRankStations:
    @pytest.fixture()
    def df_with_pooled(self) -> pd.DataFrame:
        return _make_df(
            _base_row(code="19999", pod=0.7, pod_undefined=False),
            _base_row(code="29999", pod=0.4, pod_undefined=False),
            _base_row(code="POOLED", pod=0.55, pod_undefined=False),
        )

    @pytest.fixture()
    def df_with_undefined(self) -> pd.DataFrame:
        return _make_df(
            _base_row(code="19999", pod=0.7, pod_undefined=False),
            _base_row(code="29999", pod=float("nan"), pod_undefined=True),
            _base_row(code="POOLED", pod=0.55, pod_undefined=False),
        )

    def test_ranks_descending_by_default(self, df_with_pooled: pd.DataFrame):
        result = rank_stations(df_with_pooled, "pod")
        assert result["pod"].iloc[0] == pytest.approx(0.7)
        assert result["pod"].iloc[1] == pytest.approx(0.4)

    def test_ranks_ascending_when_requested(self, df_with_pooled: pd.DataFrame):
        result = rank_stations(df_with_pooled, "pod", ascending=True)
        assert result["pod"].iloc[0] == pytest.approx(0.4)

    def test_excludes_pooled_from_ranking(self, df_with_pooled: pd.DataFrame):
        result = rank_stations(df_with_pooled, "pod")
        assert "POOLED" not in result["code"].values

    def test_drops_undefined_metric_rows(self, df_with_undefined: pd.DataFrame):
        result = rank_stations(df_with_undefined, "pod")
        assert "29999" not in result["code"].values
        assert len(result) == 1

    def test_drops_nan_metric_rows(self):
        df = _make_df(
            _base_row(code="19999", hss=0.5, hss_undefined=False),
            _base_row(code="29999", hss=float("nan"), hss_undefined=False),
        )
        result = rank_stations(df, "hss")
        assert len(result) == 1
        assert result["code"].iloc[0] == "19999"

    def test_empty_result_when_all_undefined(self):
        df = _make_df(
            _base_row(code="19999", pod=float("nan"), pod_undefined=True),
            _base_row(code="29999", pod=float("nan"), pod_undefined=True),
        )
        result = rank_stations(df, "pod")
        assert result.empty


# ---------------------------------------------------------------------------
# distinct_values
# ---------------------------------------------------------------------------


class TestDistinctValues:
    def test_returns_sorted_unique_strings(self):
        df = _make_df(
            _base_row(horizon="pentad"),
            _base_row(horizon="day"),
            _base_row(horizon="pentad"),
        )
        result = distinct_values(df, "horizon")
        assert result == ["day", "pentad"]

    def test_returns_sorted_unique_numbers(self):
        df = _make_df(
            _base_row(n_pairs=10),
            _base_row(n_pairs=3),
            _base_row(n_pairs=10),
        )
        result = distinct_values(df, "n_pairs")
        assert result == [3, 10]

    def test_drops_null_values(self):
        df = _make_df(
            _base_row(lead=float("nan")),
            _base_row(lead=1.0),
        )
        result = distinct_values(df, "lead")
        assert float("nan") not in result
        assert 1.0 in result

    def test_empty_column_returns_empty_list(self):
        df = _make_df(_base_row())
        df["horizon"] = None
        result = distinct_values(df, "horizon")
        assert result == []

    def test_single_value(self):
        df = _make_df(_base_row(model="LR"), _base_row(model="LR"))
        result = distinct_values(df, "model")
        assert result == ["LR"]


# ---------------------------------------------------------------------------
# available_options
# ---------------------------------------------------------------------------


class TestAvailableOptions:
    """Tests for the cascading-filter helper.

    Uses synthetic codes 19999 / 29999 only — no real station codes.
    """

    @pytest.fixture()
    def df_cascade(self) -> pd.DataFrame:
        """Two horizon families with different norm_provenance/lead combos."""
        return _make_df(
            # Short-term family: pentad, norm_provenance in {all, calculated}
            _base_row(
                horizon="pentad",
                norm_provenance="all",
                lead=float("nan"),
                model="LR",
                code="19999",
            ),
            _base_row(
                horizon="pentad",
                norm_provenance="calculated",
                lead=float("nan"),
                model="LR",
                code="19999",
            ),
            # Long-term family: month, norm_provenance in {all, official}
            _base_row(
                horizon="month",
                norm_provenance="all",
                lead=0.0,
                model="LR_Base",
                code="19999",
            ),
            _base_row(
                horizon="month",
                norm_provenance="official",
                lead=1.0,
                model="LR_Base",
                code="29999",
            ),
        )

    # ------------------------------------------------------------------ #
    # Basic behaviour                                                      #
    # ------------------------------------------------------------------ #

    def test_no_selections_returns_all_values(self, df_cascade: pd.DataFrame):
        result = available_options(df_cascade, "horizon", {})
        assert result == ["month", "pentad"]

    def test_single_upstream_filter_applied(self, df_cascade: pd.DataFrame):
        result = available_options(df_cascade, "norm_provenance", {"horizon": "pentad"})
        assert result == ["all", "calculated"]
        assert "official" not in result

    def test_official_only_for_month(self, df_cascade: pd.DataFrame):
        result = available_options(df_cascade, "norm_provenance", {"horizon": "month"})
        assert "official" in result
        assert "calculated" not in result

    def test_multiple_upstream_selections_cascade(self, df_cascade: pd.DataFrame):
        """Narrowing by horizon AND norm_provenance restricts model choices."""
        result = available_options(
            df_cascade,
            "model",
            {"horizon": "month", "norm_provenance": "official"},
        )
        assert result == ["LR_Base"]

    # ------------------------------------------------------------------ #
    # column-in-selections is ignored                                     #
    # ------------------------------------------------------------------ #

    def test_column_itself_is_skipped_in_selections(self, df_cascade: pd.DataFrame):
        """If the queried column appears in selections it must be ignored."""
        # Asking for norm_provenance options WITH norm_provenance already in
        # selections — the entry for the queried column is skipped so the
        # result is the full set given the OTHER constraints.
        result = available_options(
            df_cascade,
            "norm_provenance",
            {"horizon": "pentad", "norm_provenance": "calculated"},
        )
        # Both 'all' and 'calculated' exist for pentad regardless of the
        # stale norm_provenance entry in selections.
        assert "all" in result
        assert "calculated" in result

    # ------------------------------------------------------------------ #
    # None values in selections are skipped                               #
    # ------------------------------------------------------------------ #

    def test_none_value_in_selections_skipped(self, df_cascade: pd.DataFrame):
        """A None value means 'no constraint' — must not filter anything."""
        result = available_options(
            df_cascade,
            "norm_provenance",
            {"horizon": "pentad", "lead": None},
        )
        assert result == ["all", "calculated"]

    # ------------------------------------------------------------------ #
    # Lead (NaN) handling                                                 #
    # ------------------------------------------------------------------ #

    def test_lead_returns_empty_for_short_term_horizon(self, df_cascade: pd.DataFrame):
        """For pentad, all lead values are NaN — available_options returns []."""
        result = available_options(df_cascade, "lead", {"horizon": "pentad"})
        assert result == []

    def test_lead_returns_sorted_ints_for_long_term_horizon(self, df_cascade: pd.DataFrame):
        """For month, non-NaN leads are returned in sorted order."""
        result = available_options(df_cascade, "lead", {"horizon": "month"})
        assert result == [0.0, 1.0]

    # ------------------------------------------------------------------ #
    # Edge cases                                                          #
    # ------------------------------------------------------------------ #

    def test_no_matching_rows_returns_empty_list(self, df_cascade: pd.DataFrame):
        result = available_options(df_cascade, "model", {"horizon": "nonexistent"})
        assert result == []

    def test_result_is_sorted(self, df_cascade: pd.DataFrame):
        result = available_options(df_cascade, "horizon", {})
        assert result == sorted(result)

    def test_result_excludes_nulls(self, df_cascade: pd.DataFrame):
        """NaN in the target column must not appear in the output."""
        result = available_options(df_cascade, "lead", {"horizon": "month"})
        for v in result:
            assert v == v  # NaN != NaN; this passes only for non-NaN values

    def test_empty_df_returns_empty_list(self):
        df = _make_df(_base_row()).iloc[0:0]  # empty frame with correct columns
        result = available_options(df, "horizon", {})
        assert result == []

    def test_returns_list_not_array(self, df_cascade: pd.DataFrame):
        result = available_options(df_cascade, "horizon", {})
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Probabilistic loader helpers
# ---------------------------------------------------------------------------

# Minimal column sets that the loaders must produce on empty/absent files.
_PROB_METRIC_COLS = {
    "horizon",
    "model",
    "regime",
    "season",
    "code",
    "basin",
    "norm_provenance",
    "lead",
    "event",
    "fc_grid_id",
    "n_pairs",
    "crps",
    "crps_clim",
    "crpss",
    "crps_persist",
    "crpss_persist",
    "coverage_50",
    "coverage_80",
    "coverage_90",
    "coverage_ci_lower",
    "coverage_ci_upper",
    "reliability_50",
    "reliability_80",
    "reliability_90",
    "nominal_50",
    "nominal_80",
    "nominal_90",
    "sharpness_iqr",
    "sharpness_width",
    "sharpness_width_norm",
    "rank_mean",
    "rank_var",
    "rank_calibration_error",
    "brier",
    "brier_ss",
}

_PROB_RELIABILITY_COLS = {
    "horizon",
    "model",
    "regime",
    "season",
    "code",
    "basin",
    "norm_provenance",
    "lead",
    "fc_grid_id",
    "nominal_level",
    "observed_frequency",
    "n",
}


def _prob_metric_row(**overrides) -> dict:
    """Minimal valid prob_metrics row.  Uses synthetic codes only."""
    row = {
        "horizon": "pentad",
        "model": "EM",
        "regime": "operational",
        "season": "all",
        "code": "19999",
        "basin": "all",
        "norm_provenance": "all",
        "lead": float("nan"),
        "event": "distribution",
        "fc_grid_id": "short5",
        "n_pairs": 20,
        "crps": 0.12,
        "crps_clim": 0.18,
        "crpss": 0.33,
        "crps_persist": 0.20,
        "crpss_persist": 0.40,
        "coverage_50": 0.55,
        "coverage_80": float("nan"),
        "coverage_90": 0.88,
        "coverage_ci_lower": 0.70,
        "coverage_ci_upper": 0.98,
        "reliability_50": 0.05,
        "reliability_80": float("nan"),
        "reliability_90": 0.02,
        "nominal_50": 0.50,
        "nominal_80": 0.80,
        "nominal_90": 0.90,
        "sharpness_iqr": 10.0,
        "sharpness_width": 25.0,
        "sharpness_width_norm": 0.50,
        "rank_mean": 0.48,
        "rank_var": 0.08,
        "rank_calibration_error": 0.05,
        "brier": float("nan"),
        "brier_ss": float("nan"),
    }
    row.update(overrides)
    return row


def _reliability_row(**overrides) -> dict:
    """Minimal valid prob_reliability row.  Uses synthetic codes only."""
    row = {
        "horizon": "pentad",
        "model": "EM",
        "regime": "operational",
        "season": "all",
        "code": "19999",
        "basin": "all",
        "norm_provenance": "all",
        "lead": float("nan"),
        "fc_grid_id": "short5",
        "nominal_level": 0.50,
        "observed_frequency": 0.52,
        "n": 20,
    }
    row.update(overrides)
    return row


def _write_prob_csv(tmp_path: Path, filename: str, *rows: dict) -> Path:
    """Write rows to filename in tmp_path; return the sibling contingency path."""
    df = pd.DataFrame(list(rows))
    prob_path = tmp_path / filename
    df.to_csv(prob_path, index=False)
    # load_prob_metrics / load_reliability resolve from the parent of the
    # contingency CSV, so we provide a fake sibling path.
    contingency = tmp_path / "contingency_metrics.csv"
    if not contingency.exists():
        pd.DataFrame().to_csv(contingency, index=False)
    return contingency


# ---------------------------------------------------------------------------
# TestLoadProbMetrics
# ---------------------------------------------------------------------------


class TestLoadProbMetrics:
    def test_reads_sibling_prob_metrics_csv(self, tmp_path: Path):
        """load_prob_metrics reads prob_metrics.csv from the same directory."""
        sib = tmp_path / "prob_metrics.csv"
        pd.DataFrame([_prob_metric_row()]).to_csv(sib, index=False)
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_prob_metrics(contingency)
        assert len(df) == 1
        assert df["model"].iloc[0] == "EM"

    def test_tolerates_missing_file_returns_empty_typed_frame(self, tmp_path: Path):
        """When prob_metrics.csv is absent, an empty typed DataFrame is returned."""
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_prob_metrics(contingency)
        assert df.empty
        assert set(_PROB_METRIC_COLS).issubset(set(df.columns))

    def test_synthesises_event_column_when_absent(self, tmp_path: Path):
        """A CSV without 'event' gets event='distribution' for all rows."""
        row = _prob_metric_row()
        del row["event"]
        sib = tmp_path / "prob_metrics.csv"
        pd.DataFrame([row]).to_csv(sib, index=False)
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_prob_metrics(contingency)
        assert "event" in df.columns
        assert (df["event"] == "distribution").all()

    def test_synthesises_fc_grid_id_when_absent(self, tmp_path: Path):
        """A CSV without 'fc_grid_id' gets fc_grid_id='' for all rows."""
        row = _prob_metric_row()
        del row["fc_grid_id"]
        sib = tmp_path / "prob_metrics.csv"
        pd.DataFrame([row]).to_csv(sib, index=False)
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_prob_metrics(contingency)
        assert "fc_grid_id" in df.columns
        assert (df["fc_grid_id"] == "").all()

    def test_lead_is_numeric(self, tmp_path: Path):
        sib = tmp_path / "prob_metrics.csv"
        pd.DataFrame([_prob_metric_row(lead=2.0)]).to_csv(sib, index=False)
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_prob_metrics(contingency)
        assert pd.api.types.is_float_dtype(df["lead"])
        assert df["lead"].iloc[0] == pytest.approx(2.0)

    def test_lead_nan_preserved(self, tmp_path: Path):
        sib = tmp_path / "prob_metrics.csv"
        pd.DataFrame([_prob_metric_row()]).to_csv(sib, index=False)
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_prob_metrics(contingency)
        assert math.isnan(df["lead"].iloc[0])

    def test_multiple_rows_loaded(self, tmp_path: Path):
        sib = tmp_path / "prob_metrics.csv"
        pd.DataFrame([_prob_metric_row(code="19999"), _prob_metric_row(code="29999")]).to_csv(
            sib, index=False
        )
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_prob_metrics(contingency)
        assert len(df) == 2


# ---------------------------------------------------------------------------
# TestLoadReliability
# ---------------------------------------------------------------------------


class TestLoadReliability:
    def test_reads_sibling_prob_reliability_csv(self, tmp_path: Path):
        """load_reliability reads prob_reliability.csv from the same directory."""
        sib = tmp_path / "prob_reliability.csv"
        pd.DataFrame([_reliability_row()]).to_csv(sib, index=False)
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_reliability(contingency)
        assert len(df) == 1
        assert df["nominal_level"].iloc[0] == pytest.approx(0.50)

    def test_tolerates_missing_file_returns_empty_typed_frame(self, tmp_path: Path):
        """When prob_reliability.csv is absent, an empty typed DataFrame is returned."""
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_reliability(contingency)
        assert df.empty
        assert set(_PROB_RELIABILITY_COLS).issubset(set(df.columns))

    def test_synthesises_fc_grid_id_when_absent(self, tmp_path: Path):
        """A CSV without 'fc_grid_id' gets fc_grid_id='' for all rows."""
        row = _reliability_row()
        del row["fc_grid_id"]
        sib = tmp_path / "prob_reliability.csv"
        pd.DataFrame([row]).to_csv(sib, index=False)
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_reliability(contingency)
        assert "fc_grid_id" in df.columns
        assert (df["fc_grid_id"] == "").all()

    def test_lead_is_numeric(self, tmp_path: Path):
        sib = tmp_path / "prob_reliability.csv"
        pd.DataFrame([_reliability_row(lead=1.0)]).to_csv(sib, index=False)
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_reliability(contingency)
        assert pd.api.types.is_float_dtype(df["lead"])
        assert df["lead"].iloc[0] == pytest.approx(1.0)

    def test_lead_nan_preserved(self, tmp_path: Path):
        sib = tmp_path / "prob_reliability.csv"
        pd.DataFrame([_reliability_row()]).to_csv(sib, index=False)
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_reliability(contingency)
        assert math.isnan(df["lead"].iloc[0])

    def test_multiple_rows_loaded(self, tmp_path: Path):
        sib = tmp_path / "prob_reliability.csv"
        pd.DataFrame(
            [
                _reliability_row(nominal_level=0.05),
                _reliability_row(nominal_level=0.25),
                _reliability_row(nominal_level=0.75),
            ]
        ).to_csv(sib, index=False)
        contingency = tmp_path / "contingency_metrics.csv"
        contingency.touch()

        df = load_reliability(contingency)
        assert len(df) == 3


# ---------------------------------------------------------------------------
# TestFilterProbByGrid
# ---------------------------------------------------------------------------


class TestFilterProbByGrid:
    @pytest.fixture()
    def df_two_grids(self) -> pd.DataFrame:
        """Mixed frame with 'short5' and 'long7' rows."""
        return pd.DataFrame(
            [
                _prob_metric_row(code="19999", fc_grid_id="short5"),
                _prob_metric_row(code="29999", fc_grid_id="short5"),
                _prob_metric_row(code="19999", fc_grid_id="long7"),
            ]
        )

    def test_returns_only_matching_grid(self, df_two_grids: pd.DataFrame):
        result = filter_prob_by_grid(df_two_grids, "short5")
        assert len(result) == 2
        assert (result["fc_grid_id"] == "short5").all()

    def test_returns_empty_when_no_match(self, df_two_grids: pd.DataFrame):
        result = filter_prob_by_grid(df_two_grids, "unknown_grid")
        assert result.empty

    def test_returns_copy_not_view(self, df_two_grids: pd.DataFrame):
        result = filter_prob_by_grid(df_two_grids, "short5")
        result["crpss"] = 999.0
        # Original must be unchanged.
        assert (df_two_grids["crpss"] != 999.0).all()

    def test_handles_missing_fc_grid_id_column(self):
        """When the column is absent, returns an empty frame."""
        df = pd.DataFrame([_prob_metric_row()]).drop(columns=["fc_grid_id"])
        result = filter_prob_by_grid(df, "short5")
        assert result.empty

    def test_long7_rows_excluded_from_short5_filter(self, df_two_grids: pd.DataFrame):
        """Design Decision 3: long7 CRPS must never appear in a short5 ranking."""
        result = filter_prob_by_grid(df_two_grids, "short5")
        assert "long7" not in result["fc_grid_id"].values

    def test_empty_input_returns_empty(self):
        df = pd.DataFrame([_prob_metric_row()]).iloc[0:0]
        result = filter_prob_by_grid(df, "short5")
        assert result.empty
