"""Unit tests for forecast_skill_eval.dashboard.aggregates.

Uses only small inline fixtures — no real station codes (uses 19999 / 29999).
No Streamlit or matplotlib dependency: aggregates.py is pure pandas.
"""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd
import pytest

from forecast_skill_eval.dashboard.aggregates import (
    BASELINES,
    CANONICAL_LEADS,
    FIG4_LEADS,
    FIG6_HORIZONS,
    HCOLORS,
    HORIZONS,
    LONG_TERM,
    PROVENANCE,
    SHORT_TERM,
    _add_h_label,
    _baseline_rows,
    base_horizon,
    get_baseline_refs,
    lead_display,
    load_baselines,
    model_family,
    model_sort_key,
    prep_model_comparison_per_horizon,
    prep_op_vs_hindcast,
    prep_performance_diagram,
    prep_pooled,
    prep_seasonal_pod,
    prep_skill_ladder,
)

# ---------------------------------------------------------------------------
# Fixture builder helpers
# ---------------------------------------------------------------------------


def _base_row(**overrides) -> dict:
    """Minimal valid POOLED row. Uses synthetic codes only (19999/29999)."""
    row = {
        "horizon": "pentad",
        "model": "LR",
        "regime": "operational",
        "season": "all",
        "code": "POOLED",
        "basin": "all",
        "norm_provenance": "calculated",
        "lead": float("nan"),
        "event": "below_norm",
        "n_pairs": 20,
        "base_rate": 0.35,
        "base_rate_undefined": False,
        "pod": 0.65,
        "pod_undefined": False,
        "far": 0.20,
        "far_undefined": False,
        "hss": 0.50,
        "hss_undefined": False,
        "csi": 0.55,
        "csi_undefined": False,
        "pod_ci_lower": 0.45,
        "pod_ci_upper": 0.82,
        "pod_ci_undefined": False,
    }
    row.update(overrides)
    return row


def _make_df(*rows: dict) -> pd.DataFrame:
    """Build a DataFrame from dicts; coerce lead to float."""
    df = pd.DataFrame(list(rows))
    if "lead" in df.columns:
        df["lead"] = pd.to_numeric(df["lead"], errors="coerce")
    return df


def _baseline_row(**overrides) -> dict:
    """Minimal valid baseline row."""
    row = {
        "horizon": "pentad",
        "model": "LR",
        "regime": "operational",
        "season": "all",
        "code": "POOLED",
        "basin": "all",
        "norm_provenance": "calculated",
        "lead": float("nan"),
        "pod": 0.60,
        "pod_undefined": False,
        "far": 0.25,
        "far_undefined": False,
        "hss": 0.40,
        "hss_undefined": False,
        "n_pairs": 18,
        "base_rate": 0.33,
        "base_rate_undefined": False,
        "baseline": "persistence",
        "comparison_model": "LR",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# TestModelFamily
# ---------------------------------------------------------------------------


class TestModelFamily:
    """Tests for :func:`model_family`."""

    def test_known_models_return_correct_family(self):
        """LR→lr, TFT→ml, EM→ensemble, Naive Mean→naive."""
        assert model_family("LR") == "lr"
        assert model_family("TFT") == "ml"
        assert model_family("EM") == "ensemble"
        assert model_family("Naive Mean") == "naive"

    def test_unknown_model_returns_other(self):
        """Unrecognised model name should return "other"."""
        assert model_family("UNKNOWN_XYZ") == "other"


# ---------------------------------------------------------------------------
# TestModelSortKey
# ---------------------------------------------------------------------------


class TestModelSortKey:
    """Tests for :func:`model_sort_key`."""

    def test_lr_sorts_before_ml(self):
        """LR family (order 0) should sort before ML family (order 1)."""
        key_lr = model_sort_key("LR")
        key_ml = model_sort_key("TFT")
        assert key_lr < key_ml

    def test_same_family_sorts_alphabetically(self):
        """Within the LR family, alphabetical order by model name applies."""
        key_a = model_sort_key("LR_Base")
        key_b = model_sort_key("LR_SM")
        assert key_a < key_b


# ---------------------------------------------------------------------------
# TestBaseHorizon
# ---------------------------------------------------------------------------


class TestBaseHorizon:
    """Tests for :func:`base_horizon`."""

    def test_short_term_returns_self(self):
        """Bare horizon labels return the same string."""
        assert base_horizon("pentad") == "pentad"
        assert base_horizon("decade") == "decade"
        assert base_horizon("day") == "day"

    def test_long_term_with_lead_returns_base(self):
        """'month L0' should return 'month'."""
        assert base_horizon("month L0") == "month"
        assert base_horizon("quarter L1") == "quarter"
        assert base_horizon("season L0") == "season"

    def test_quarter_with_q_label_returns_base(self):
        """'quarter Q1' (target-quarter form) should still return 'quarter'."""
        assert base_horizon("quarter Q1") == "quarter"
        assert base_horizon("quarter Q4") == "quarter"

    def test_unknown_returns_input(self):
        """Unrecognised labels are passed through unchanged."""
        assert base_horizon("foobar") == "foobar"


# ---------------------------------------------------------------------------
# TestLeadDisplay
# ---------------------------------------------------------------------------


class TestLeadDisplay:
    """Tests for the horizon-aware :func:`lead_display` helper."""

    def test_quarter_uses_q_prefix(self):
        """Quarter's lead value is the target quarter → 'Q{n}'."""
        assert lead_display("quarter", 1) == "Q1"
        assert lead_display("quarter", 4) == "Q4"

    def test_month_uses_l_prefix(self):
        """Month keeps genuine forecast-lead labelling → 'L{n}'."""
        assert lead_display("month", 0) == "L0"
        assert lead_display("month", 3) == "L3"

    def test_season_uses_l_prefix(self):
        """Season keeps genuine forecast-lead labelling → 'L{n}'."""
        assert lead_display("season", 0) == "L0"
        assert lead_display("season", 2) == "L2"

    def test_short_term_sentinel_is_dash(self):
        """The short-term sentinel (-1) renders as an em dash, any horizon."""
        assert lead_display("pentad", -1) == "—"
        assert lead_display("quarter", -1) == "—"


# ---------------------------------------------------------------------------
# TestAddHLabel
# ---------------------------------------------------------------------------


class TestAddHLabel:
    """Tests for the private :func:`_add_h_label` helper."""

    def test_short_term_gets_minus_one_lead_int(self):
        """Short-term rows must have lead_int == -1."""
        df = _make_df(_base_row(horizon="pentad", lead=float("nan")))
        result = _add_h_label(df)
        assert result["lead_int"].iloc[0] == -1

    def test_short_term_gets_horizon_as_h_label(self):
        """Short-term rows must have h_label equal to the horizon string."""
        df = _make_df(_base_row(horizon="decade", lead=float("nan")))
        result = _add_h_label(df)
        assert result["h_label"].iloc[0] == "decade"

    def test_long_term_gets_lead_int(self):
        """Long-term rows must have lead_int equal to the integer lead."""
        df = _make_df(_base_row(horizon="month", lead=2.0))
        result = _add_h_label(df)
        assert result["lead_int"].iloc[0] == 2

    def test_long_term_gets_labelled_h_label(self):
        """Long-term rows must have h_label like 'month L2'."""
        df = _make_df(_base_row(horizon="month", lead=2.0))
        result = _add_h_label(df)
        assert result["h_label"].iloc[0] == "month L2"

    def test_quarter_h_label_uses_q_prefix(self):
        """Quarter rows carry the target quarter → h_label like 'quarter Q1'."""
        df = _make_df(_base_row(horizon="quarter", lead=1.0))
        result = _add_h_label(df)
        assert result["h_label"].iloc[0] == "quarter Q1"

    def test_season_h_label_unchanged_uses_l_prefix(self):
        """Season rows keep the 'L{n}' lead form."""
        df = _make_df(_base_row(horizon="season", lead=0.0))
        result = _add_h_label(df)
        assert result["h_label"].iloc[0] == "season L0"


# ---------------------------------------------------------------------------
# TestLoadBaselines
# ---------------------------------------------------------------------------


class TestLoadBaselines:
    """Tests for :func:`load_baselines`."""

    def test_returns_empty_df_when_file_absent(self, tmp_path: Path):
        """When baselines.csv does not exist, return an empty DataFrame."""
        # Only contingency_metrics.csv exists in tmp_path; baselines.csv does not
        result = load_baselines(tmp_path / "contingency_metrics.csv")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_reads_valid_baselines_csv(self, tmp_path: Path):
        """When baselines.csv exists in the same dir, it should be read."""
        baselines_path = tmp_path / "baselines.csv"
        df = _make_df(_baseline_row())
        df.to_csv(baselines_path, index=False)

        result = load_baselines(tmp_path / "contingency_metrics.csv")
        assert not result.empty
        assert "baseline" in result.columns

    def test_lead_column_parsed_numeric(self, tmp_path: Path):
        """The lead column in baselines must be parsed as numeric."""
        baselines_path = tmp_path / "baselines.csv"
        # Include a NaN row alongside a numeric row to trigger float64 dtype.
        df = _make_df(_baseline_row(lead=float("nan")), _baseline_row(lead=1.0))
        df.to_csv(baselines_path, index=False)

        result = load_baselines(tmp_path / "contingency_metrics.csv")
        assert pd.api.types.is_numeric_dtype(result["lead"])


# ---------------------------------------------------------------------------
# TestPrepPooled
# ---------------------------------------------------------------------------


class TestPrepPooled:
    """Tests for :func:`prep_pooled`."""

    @pytest.fixture()
    def df_mixed(self) -> pd.DataFrame:
        """DataFrame with POOLED and per-station rows across multiple configs."""
        return _make_df(
            # Valid POOLED row
            _base_row(
                code="POOLED",
                horizon="pentad",
                season="all",
                regime="operational",
                norm_provenance="calculated",
                event="below_norm",
            ),
            # Per-station row (should be excluded)
            _base_row(
                code="19999",
                horizon="pentad",
                season="all",
                regime="operational",
                norm_provenance="calculated",
                event="below_norm",
            ),
            # Wrong basin
            _base_row(
                code="POOLED",
                basin="custom_basin",
                horizon="pentad",
                season="all",
                regime="operational",
                norm_provenance="calculated",
            ),
            # Wrong season
            _base_row(
                code="POOLED",
                horizon="pentad",
                season="irrigation",
                regime="operational",
                norm_provenance="calculated",
            ),
            # Wrong regime
            _base_row(
                code="POOLED",
                horizon="pentad",
                season="all",
                regime="hindcast",
                norm_provenance="calculated",
            ),
            # Wrong provenance
            _base_row(
                code="POOLED",
                horizon="pentad",
                season="all",
                regime="operational",
                norm_provenance="official",
            ),
            # Wrong event
            _base_row(
                code="POOLED",
                horizon="pentad",
                season="all",
                regime="operational",
                norm_provenance="calculated",
                event="above_norm",
            ),
            # Valid month POOLED row
            _base_row(
                code="POOLED",
                horizon="month",
                season="all",
                regime="operational",
                norm_provenance="official",
                lead=0.0,
            ),
        )

    def test_keeps_only_pooled_code(self, df_mixed: pd.DataFrame):
        """Only rows with code=='POOLED' should be retained."""
        result = prep_pooled(df_mixed)
        assert (result["code"] == "POOLED").all()

    def test_keeps_only_basin_all(self, df_mixed: pd.DataFrame):
        """Only rows with basin=='all' should be retained."""
        result = prep_pooled(df_mixed)
        assert (result["basin"] == "all").all()

    def test_keeps_canonical_norm_provenance(self, df_mixed: pd.DataFrame):
        """Only rows with canonical norm_provenance per horizon should remain."""
        result = prep_pooled(df_mixed)
        for _, row in result.iterrows():
            assert PROVENANCE.get(row["horizon"]) == row["norm_provenance"]

    def test_filters_by_season(self, df_mixed: pd.DataFrame):
        """Only rows matching the specified season should remain."""
        result = prep_pooled(df_mixed, season="all")
        assert (result["season"] == "all").all()

    def test_filters_by_event(self, df_mixed: pd.DataFrame):
        """Only rows matching the specified event should remain."""
        result = prep_pooled(df_mixed, event="below_norm")
        assert (result["event"] == "below_norm").all()

    def test_adds_h_label_column(self, df_mixed: pd.DataFrame):
        """Result must contain the 'h_label' column."""
        result = prep_pooled(df_mixed)
        assert "h_label" in result.columns

    def test_short_term_h_label_equals_horizon(self, df_mixed: pd.DataFrame):
        """For short-term horizons, h_label should equal the horizon string."""
        result = prep_pooled(df_mixed)
        short_rows = result[result["horizon"].isin(SHORT_TERM)]
        for _, row in short_rows.iterrows():
            assert row["h_label"] == row["horizon"]

    def test_long_term_h_label_includes_lead(self, df_mixed: pd.DataFrame):
        """For long-term horizons, h_label should include 'L<lead_int>'."""
        result = prep_pooled(df_mixed)
        long_rows = result[result["horizon"].isin(LONG_TERM)]
        for _, row in long_rows.iterrows():
            assert "L" in row["h_label"]

    def test_empty_when_no_match(self):
        """Returns empty DataFrame when no rows match the filters."""
        df = _make_df(_base_row(code="POOLED", event="above_norm"))
        result = prep_pooled(df, event="below_norm")
        assert result.empty


# ---------------------------------------------------------------------------
# TestPrepPerformanceDiagram
# ---------------------------------------------------------------------------


class TestPrepPerformanceDiagram:
    """Tests for :func:`prep_performance_diagram`."""

    @pytest.fixture()
    def df_perf(self) -> pd.DataFrame:
        """DataFrame with defined and undefined POD/FAR rows."""
        return _make_df(
            _base_row(pod=0.70, pod_undefined=False, far=0.20, far_undefined=False),
            _base_row(
                model="TFT", pod=float("nan"), pod_undefined=True, far=0.30, far_undefined=False
            ),
            _base_row(
                model="EM", pod=0.60, pod_undefined=False, far=float("nan"), far_undefined=True
            ),
        )

    def test_excludes_undefined_pod_or_far(self, df_perf: pd.DataFrame):
        """Rows with pod_undefined or far_undefined must be dropped."""
        result = prep_performance_diagram(df_perf)
        assert len(result) == 1
        assert result["model"].iloc[0] == "LR"

    def test_adds_sr_column(self, df_perf: pd.DataFrame):
        """Success Ratio (sr = 1 - far) must be added."""
        result = prep_performance_diagram(df_perf)
        assert "sr" in result.columns
        assert result["sr"].iloc[0] == pytest.approx(1.0 - 0.20)

    def test_adds_family_columns(self, df_perf: pd.DataFrame):
        """family, family_color, family_label columns must be present."""
        result = prep_performance_diagram(df_perf)
        assert "family" in result.columns
        assert "family_color" in result.columns
        assert "family_label" in result.columns
        assert result["family"].iloc[0] == "lr"

    def test_adds_horizon_color(self, df_perf: pd.DataFrame):
        """horizon_color must be added and match HCOLORS."""
        result = prep_performance_diagram(df_perf)
        assert "horizon_color" in result.columns
        assert result["horizon_color"].iloc[0] == HCOLORS["pentad"]

    def test_lead_label_is_dash_for_short_term(self, df_perf: pd.DataFrame):
        """Short-term rows must have lead_label == '—'."""
        result = prep_performance_diagram(df_perf)
        assert result["lead_label"].iloc[0] == "—"

    def test_lead_label_includes_lead_int_for_long_term(self):
        """Long-term rows must have lead_label like 'L0'."""
        df = _make_df(
            _base_row(
                horizon="month",
                norm_provenance="official",
                lead=0.0,
                pod=0.70,
                pod_undefined=False,
                far=0.20,
                far_undefined=False,
            )
        )
        result = prep_performance_diagram(df)
        assert not result.empty
        assert result["lead_label"].iloc[0] == "L0"

    def test_lead_label_uses_q_prefix_for_quarter(self):
        """Quarter rows label the lead as the target quarter → 'Q1'."""
        df = _make_df(
            _base_row(
                horizon="quarter",
                norm_provenance="aggregated_from_monthly",
                lead=1.0,
                pod=0.70,
                pod_undefined=False,
                far=0.20,
                far_undefined=False,
            )
        )
        result = prep_performance_diagram(df)
        assert not result.empty
        assert result["lead_label"].iloc[0] == "Q1"


# ---------------------------------------------------------------------------
# TestBaselineRows
# ---------------------------------------------------------------------------


class TestBaselineRows:
    """Tests for the private :func:`_baseline_rows` helper."""

    def test_empty_df_returns_empty(self):
        """Passing an empty baselines DataFrame should return an empty result."""
        empty = pd.DataFrame(
            columns=[
                "code",
                "baseline",
                "regime",
                "basin",
                "horizon",
                "season",
                "norm_provenance",
                "lead",
            ]
        )
        result = _baseline_rows(empty, "pentad", "climatology", None)
        assert result.empty

    def test_filters_by_baseline_name(self):
        """Only rows matching baseline_name should be returned."""
        df = _make_df(
            _baseline_row(baseline="climatology"),
            _baseline_row(baseline="persistence"),
        )
        result = _baseline_rows(df, "pentad", "climatology", None)
        assert (result["baseline"] == "climatology").all()
        assert len(result) == 1

    def test_filters_nan_lead_when_lead_is_none(self):
        """lead=None should keep only rows where lead is NaN."""
        df = _make_df(
            _baseline_row(lead=float("nan")),
            _baseline_row(lead=1.0),
        )
        result = _baseline_rows(df, "pentad", "persistence", None)
        for v in result["lead"]:
            assert math.isnan(v)

    def test_filters_numeric_lead_when_lead_given(self):
        """lead=1 should keep only rows where lead == 1.0."""
        df = _make_df(
            _baseline_row(horizon="month", norm_provenance="official", lead=float("nan")),
            _baseline_row(horizon="month", norm_provenance="official", lead=1.0),
        )
        result = _baseline_rows(df, "month", "persistence", 1)
        assert len(result) == 1
        assert result["lead"].iloc[0] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# TestGetBaselineRefs
# ---------------------------------------------------------------------------


class TestGetBaselineRefs:
    """Tests for :func:`get_baseline_refs`."""

    def test_returns_none_values_when_baselines_empty(self):
        """All values should be None when baselines DataFrame is empty."""
        empty = pd.DataFrame(
            columns=[
                "code",
                "baseline",
                "regime",
                "basin",
                "horizon",
                "season",
                "norm_provenance",
                "lead",
                "base_rate",
                "base_rate_undefined",
                "pod",
                "pod_undefined",
                "hss",
                "hss_undefined",
            ]
        )
        refs = get_baseline_refs(empty, "pentad")
        assert refs["clim_base_rate"] is None
        assert refs["persistence_pod"] is None
        assert refs["persistence_hss"] is None

    def test_returns_clim_base_rate(self):
        """Should extract base_rate from climatology row."""
        df = _make_df(
            _baseline_row(baseline="climatology", base_rate=0.35, base_rate_undefined=False)
        )
        refs = get_baseline_refs(df, "pentad")
        assert refs["clim_base_rate"] == pytest.approx(0.35)

    def test_returns_none_when_undefined(self):
        """base_rate_undefined=True should yield clim_base_rate=None."""
        df = _make_df(
            _baseline_row(baseline="climatology", base_rate=float("nan"), base_rate_undefined=True)
        )
        refs = get_baseline_refs(df, "pentad")
        assert refs["clim_base_rate"] is None

    def test_persistence_uses_canonical_lead(self):
        """For long-term, persistence should use the canonical lead from CANONICAL_LEADS."""
        # Insert persistence at lead 0 (canonical for month) and lead 2
        df = _make_df(
            _baseline_row(
                horizon="month",
                norm_provenance="official",
                lead=float(CANONICAL_LEADS["month"]),
                baseline="persistence",
                pod=0.55,
                pod_undefined=False,
                hss=0.30,
                hss_undefined=False,
            ),
            _baseline_row(
                horizon="month",
                norm_provenance="official",
                lead=2.0,
                baseline="persistence",
                pod=0.45,
                pod_undefined=False,
                hss=0.20,
                hss_undefined=False,
            ),
        )
        # Pass lead=2 but persistence should still use canonical (0 for month)
        refs = get_baseline_refs(df, "month", lead=2)
        assert refs["persistence_pod"] == pytest.approx(0.55)
        assert refs["persistence_hss"] == pytest.approx(0.30)


# ---------------------------------------------------------------------------
# TestPrepModelComparisonPerHorizon
# ---------------------------------------------------------------------------


class TestPrepModelComparisonPerHorizon:
    """Tests for :func:`prep_model_comparison_per_horizon`."""

    @pytest.fixture()
    def df_multi(self) -> pd.DataFrame:
        """POOLED rows across several horizons and leads."""
        return _make_df(
            # Short-term pentad (NaN lead)
            _base_row(
                horizon="pentad", norm_provenance="calculated", lead=float("nan"), model="LR"
            ),
            _base_row(
                horizon="pentad", norm_provenance="calculated", lead=float("nan"), model="TFT"
            ),
            # Long-term month, multiple leads
            _base_row(horizon="month", norm_provenance="official", lead=0.0, model="LR"),
            _base_row(horizon="month", norm_provenance="official", lead=1.0, model="LR"),
            _base_row(
                horizon="month", norm_provenance="official", lead=5.0, model="LR"
            ),  # outside FIG4_LEADS
            # Different horizon that should be excluded
            _base_row(
                horizon="decade", norm_provenance="calculated", lead=float("nan"), model="LR"
            ),
        )

    def test_short_term_keeps_only_nan_lead_rows(self, df_multi: pd.DataFrame):
        """Short-term comparison should only include NaN-lead rows."""
        result = prep_model_comparison_per_horizon(df_multi, "pentad")
        assert result["lead_int"].eq(-1).all()

    def test_long_term_keeps_only_fig4_leads(self, df_multi: pd.DataFrame):
        """Long-term comparison should restrict to FIG4_LEADS[horizon]."""
        result = prep_model_comparison_per_horizon(df_multi, "month")
        allowed = FIG4_LEADS["month"]
        for li in result["lead_int"]:
            assert li in allowed

    def test_excludes_other_horizons(self, df_multi: pd.DataFrame):
        """Only rows matching the requested horizon should be returned."""
        result = prep_model_comparison_per_horizon(df_multi, "pentad")
        assert (result["horizon"] == "pentad").all()

    def test_adds_family_columns(self, df_multi: pd.DataFrame):
        """family, family_color, family_label columns must be present."""
        result = prep_model_comparison_per_horizon(df_multi, "pentad")
        for col in ("family", "family_color", "family_label"):
            assert col in result.columns


# ---------------------------------------------------------------------------
# TestPrepSkillLadder
# ---------------------------------------------------------------------------


class TestPrepSkillLadder:
    """Tests for :func:`prep_skill_ladder`."""

    @pytest.fixture()
    def df_all(self) -> pd.DataFrame:
        """POOLED operational rows for all horizons."""
        rows = []
        for h in ["day", "pentad", "decade"]:
            prov = PROVENANCE[h]
            rows.append(
                _base_row(
                    horizon=h, norm_provenance=prov, model="LR", hss=0.45, hss_undefined=False
                )
            )
            rows.append(
                _base_row(
                    horizon=h, norm_provenance=prov, model="EM", hss=0.55, hss_undefined=False
                )
            )
        # Long-term: only include canonical leads
        for h, canonical_lead in CANONICAL_LEADS.items():
            prov = PROVENANCE[h]
            rows.append(
                _base_row(
                    horizon=h,
                    norm_provenance=prov,
                    lead=float(canonical_lead),
                    model="LR",
                    hss=0.40,
                    hss_undefined=False,
                )
            )
        return _make_df(*rows)

    @pytest.fixture()
    def baselines_empty(self) -> pd.DataFrame:
        """Empty baselines DataFrame (no persistence data)."""
        return pd.DataFrame(
            columns=[
                "code",
                "baseline",
                "regime",
                "basin",
                "horizon",
                "season",
                "norm_provenance",
                "lead",
                "pod",
                "pod_undefined",
                "hss",
                "hss_undefined",
                "base_rate",
                "base_rate_undefined",
            ]
        )

    def test_returns_three_rows_per_horizon(self, df_all, baselines_empty):
        """Skill ladder must have exactly 3 rows per horizon (6 horizons = 18)."""
        result = prep_skill_ladder(df_all, baselines_empty)
        assert len(result) == 3 * len(HORIZONS)

    def test_climatology_hss_is_zero(self, df_all, baselines_empty):
        """Climatology row must always have HSS = 0.0."""
        result = prep_skill_ladder(df_all, baselines_empty)
        clim_rows = result[result["series"] == "Climatology"]
        # Use direct element-wise comparison (0.0 is exact; pytest.approx doesn't
        # compose with pandas .all() correctly).
        assert (clim_rows["hss"] == 0.0).all()

    def test_best_model_excludes_baselines(self, df_all, baselines_empty):
        """Best model series must not select a model from BASELINES."""
        result = prep_skill_ladder(df_all, baselines_empty)
        best_rows = result[result["series"].str.startswith("Best model")]
        for _, row in best_rows.iterrows():
            assert row["model"] not in BASELINES or row["model"] == ""

    def test_nan_hss_for_persistence_when_absent(self, df_all, baselines_empty):
        """When baselines is empty, persistence HSS must be NaN."""
        result = prep_skill_ladder(df_all, baselines_empty)
        pers_rows = result[result["series"] == "Persistence"]
        for v in pers_rows["hss"]:
            assert math.isnan(v)


# ---------------------------------------------------------------------------
# TestPrepSeasonalPod
# ---------------------------------------------------------------------------


class TestPrepSeasonalPod:
    """Tests for :func:`prep_seasonal_pod`."""

    @pytest.fixture()
    def df_em(self) -> pd.DataFrame:
        """POOLED EM rows for fig6 horizons across all seasons."""
        rows = []
        for h, lead_str, _ in FIG6_HORIZONS:
            prov = PROVENANCE[h]
            lead_val = float(lead_str) if lead_str is not None else float("nan")
            for season in ["non_irrigation", "all", "irrigation"]:
                rows.append(
                    _base_row(
                        horizon=h,
                        norm_provenance=prov,
                        lead=lead_val,
                        model="EM",
                        season=season,
                        pod=0.65,
                        pod_undefined=False,
                    )
                )
        return _make_df(*rows)

    def test_returns_three_seasons_per_fig6_horizon(self, df_em: pd.DataFrame):
        """Each fig6 horizon should contribute 3 season rows."""
        result = prep_seasonal_pod(df_em)
        assert len(result) == 3 * len(FIG6_HORIZONS)

    def test_pod_undefined_when_no_data(self):
        """When no EM row is found, pod_undefined should be True."""
        # DataFrame with no EM model
        df = _make_df(_base_row(model="LR"))
        result = prep_seasonal_pod(df)
        assert result["pod_undefined"].all()

    def test_valid_pod_when_em_row_present(self, df_em: pd.DataFrame):
        """When an EM row exists, pod must be populated and pod_undefined=False."""
        result = prep_seasonal_pod(df_em)
        defined_rows = result[~result["pod_undefined"]]
        assert not defined_rows.empty
        # Use abs tolerance comparison (pytest.approx doesn't compose with
        # pandas .all() correctly for element-wise checks).
        assert ((defined_rows["pod"] - 0.65).abs() < 1e-6).all()


# ---------------------------------------------------------------------------
# TestPrepOpVsHindcast
# ---------------------------------------------------------------------------


class TestPrepOpVsHindcast:
    """Tests for :func:`prep_op_vs_hindcast`."""

    @pytest.fixture()
    def df_lt(self) -> pd.DataFrame:
        """POOLED operational + hindcast rows for long-term horizons."""
        rows = []
        for h in LONG_TERM:
            prov = PROVENANCE[h]
            canonical_lead = float(CANONICAL_LEADS.get(h, 0))
            rows.append(
                _base_row(
                    horizon=h,
                    norm_provenance=prov,
                    lead=canonical_lead,
                    model="LR",
                    regime="operational",
                    hss=0.45,
                )
            )
            rows.append(
                _base_row(
                    horizon=h,
                    norm_provenance=prov,
                    lead=canonical_lead,
                    model="LR",
                    regime="hindcast",
                    hss=0.50,
                )
            )
        return _make_df(*rows)

    def test_long_term_horizons_only(self, df_lt: pd.DataFrame):
        """Output should only contain long-term horizons."""
        result = prep_op_vs_hindcast(df_lt)
        for h in result["horizon"].unique():
            assert h in LONG_TERM

    def test_month_limited_to_l0_l3(self):
        """Month leads above 3 must be excluded from the output."""
        rows = []
        prov = PROVENANCE["month"]
        for lead in [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]:
            rows.append(
                _base_row(
                    horizon="month",
                    norm_provenance=prov,
                    lead=lead,
                    model="LR",
                    regime="operational",
                    hss=0.40,
                )
            )
            rows.append(
                _base_row(
                    horizon="month",
                    norm_provenance=prov,
                    lead=lead,
                    model="LR",
                    regime="hindcast",
                    hss=0.35,
                )
            )
        df = _make_df(*rows)
        result = prep_op_vs_hindcast(df)
        month_rows = result[result["horizon"] == "month"]
        assert (month_rows["lead_int"] <= 3).all()

    def test_empty_when_no_hindcast_data(self):
        """When only operational rows exist, the function still returns data."""
        # Just operational rows — function should still work
        rows = []
        for h in LONG_TERM:
            prov = PROVENANCE[h]
            rows.append(
                _base_row(
                    horizon=h,
                    norm_provenance=prov,
                    lead=float(CANONICAL_LEADS.get(h, 0)),
                    model="LR",
                    regime="operational",
                    hss=0.45,
                )
            )
        df = _make_df(*rows)
        result = prep_op_vs_hindcast(df)
        # Should still return something since operational rows exist
        assert isinstance(result, pd.DataFrame)

    def test_returns_both_regimes(self, df_lt: pd.DataFrame):
        """Result should include both 'operational' and 'hindcast' rows."""
        result = prep_op_vs_hindcast(df_lt)
        assert not result.empty
        regimes = set(result["regime"].unique())
        assert "operational" in regimes
        assert "hindcast" in regimes

    def test_lead_label_is_horizon_aware(self, df_lt: pd.DataFrame):
        """Quarter rows label as 'Q{n}'; month/season keep 'L{n}'."""
        result = prep_op_vs_hindcast(df_lt)
        quarter_labels = set(result.loc[result["horizon"] == "quarter", "lead_label"])
        month_labels = set(result.loc[result["horizon"] == "month", "lead_label"])
        season_labels = set(result.loc[result["horizon"] == "season", "lead_label"])
        assert quarter_labels == {"Q1"}
        assert month_labels == {"L0"}
        assert season_labels == {"L0"}
