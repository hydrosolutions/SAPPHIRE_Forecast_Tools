"""
PP-075 E2 — Four-method range consistency tests.

Covers: native / delta / manual / mixed methods with stable and translated keys,
LR rows in native mode, missing inputs → NaN, lower-bound clamp, long-term
range_delta substitution, and unknown method → NaN.

Production-path section: exercises create_forecast_summary_table directly so
that the same bounds appear in both chart and table (the pre-fix mismatch).
"""
import math

import pandas as pd
import pytest
from src import processing

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _gettext(s):
    """No-op translation (identity gettext for tests)."""
    return s


def _make_table(*, q=100.0, delta=10.0, q25=90.0, q75=110.0, model_short="GBT"):
    return pd.DataFrame([{
        "forecasted_discharge": q,
        "delta": delta,
        "Q25": q25,
        "Q75": q75,
        "model_short": model_short,
    }])


# ---------------------------------------------------------------------------
# Delta method
# ---------------------------------------------------------------------------

class TestDeltaMethod:
    def test_stable_key(self):
        result = processing.calculate_forecast_range(_gettext, _make_table(), "delta", 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)

    def test_translated_key(self):
        # translated label = same as stable (identity gettext)
        result = processing.calculate_forecast_range(_gettext, _make_table(), _gettext("delta"), 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)

    def test_clamp_lower_to_zero(self):
        t = _make_table(q=5.0, delta=20.0)
        result = processing.calculate_forecast_range(_gettext, t, "delta", 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(0.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(25.0)

    def test_missing_delta_gives_nan(self):
        t = _make_table(delta=float("nan"))
        result = processing.calculate_forecast_range(_gettext, t, "delta", 0)
        assert math.isnan(result["fc_lower"].iloc[0])
        assert math.isnan(result["fc_upper"].iloc[0])

    def test_zero_delta_gives_zero_width(self):
        t = _make_table(delta=0.0)
        result = processing.calculate_forecast_range(_gettext, t, "delta", 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(100.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Native method
# ---------------------------------------------------------------------------

class TestNativeMethod:
    def test_uses_q25_q75_for_non_lr(self):
        t = _make_table(q25=85.0, q75=115.0, model_short="GBT")
        result = processing.calculate_forecast_range(_gettext, t, "native", 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(85.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(115.0)

    def test_lr_rows_use_delta(self):
        t = _make_table(q=100.0, delta=12.0, q25=float("nan"), q75=float("nan"), model_short="LR")
        result = processing.calculate_forecast_range(_gettext, t, "native", 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(88.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(112.0)

    def test_lr_rows_clamp_lower(self):
        t = _make_table(q=5.0, delta=20.0, q25=float("nan"), q75=float("nan"), model_short="LR")
        result = processing.calculate_forecast_range(_gettext, t, "native", 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(0.0)

    def test_missing_q25_gives_nan_for_non_lr(self):
        t = _make_table(q25=float("nan"), q75=110.0, model_short="GBT")
        result = processing.calculate_forecast_range(_gettext, t, "native", 0)
        assert math.isnan(result["fc_lower"].iloc[0])
        assert math.isnan(result["fc_upper"].iloc[0])

    def test_ordered_check_q25_gt_q75_gives_nan(self):
        t = _make_table(q25=115.0, q75=90.0, model_short="GBT")
        result = processing.calculate_forecast_range(_gettext, t, "native", 0)
        assert math.isnan(result["fc_lower"].iloc[0])
        assert math.isnan(result["fc_upper"].iloc[0])

    def test_translated_key(self):
        t = _make_table(q25=85.0, q75=115.0, model_short="EM")
        result = processing.calculate_forecast_range(_gettext, t, _gettext("Native"), 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(85.0)


# ---------------------------------------------------------------------------
# Manual method
# ---------------------------------------------------------------------------

class TestManualMethod:
    def test_stable_key(self):
        result = processing.calculate_forecast_range(_gettext, _make_table(), "manual", 20)
        assert result["fc_lower"].iloc[0] == pytest.approx(80.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(120.0)

    def test_translated_key(self):
        result = processing.calculate_forecast_range(
            _gettext, _make_table(), "Manual range, select value below", 20
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(80.0)

    def test_clamp_lower_to_zero(self):
        t = _make_table(q=10.0)
        result = processing.calculate_forecast_range(_gettext, t, "manual", 110)
        assert result["fc_lower"].iloc[0] == pytest.approx(0.0)

    def test_missing_q_gives_nan(self):
        t = _make_table(q=float("nan"))
        result = processing.calculate_forecast_range(_gettext, t, "manual", 20)
        assert math.isnan(result["fc_lower"].iloc[0])


# ---------------------------------------------------------------------------
# Mixed method
# ---------------------------------------------------------------------------

class TestMixedMethod:
    def test_delta_wins_when_smaller(self):
        # delta=5, pct=10% of 100 = 10 → halfwidth = min(5,10) = 5
        t = _make_table(q=100.0, delta=5.0)
        result = processing.calculate_forecast_range(_gettext, t, "mixed", 10)
        assert result["fc_lower"].iloc[0] == pytest.approx(95.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(105.0)

    def test_pct_wins_when_smaller(self):
        # delta=20, pct=5% of 100 = 5 → halfwidth = min(20,5) = 5
        t = _make_table(q=100.0, delta=20.0)
        result = processing.calculate_forecast_range(_gettext, t, "mixed", 5)
        assert result["fc_lower"].iloc[0] == pytest.approx(95.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(105.0)

    def test_translated_key(self):
        t = _make_table(q=100.0, delta=5.0)
        result = processing.calculate_forecast_range(_gettext, t, "min[delta, %]", 10)
        assert result["fc_lower"].iloc[0] == pytest.approx(95.0)

    def test_clamp_lower_to_zero(self):
        # q=1, delta=10, pct=200% → p*q=2; halfwidth=min(10,2)=2; fc_lower=1-2=-1 → clamp to 0
        t = _make_table(q=1.0, delta=10.0)
        result = processing.calculate_forecast_range(_gettext, t, "mixed", 200)
        assert result["fc_lower"].iloc[0] == pytest.approx(0.0)

    def test_missing_delta_gives_nan(self):
        t = _make_table(delta=float("nan"))
        result = processing.calculate_forecast_range(_gettext, t, "mixed", 10)
        assert math.isnan(result["fc_lower"].iloc[0])


# ---------------------------------------------------------------------------
# Unknown method
# ---------------------------------------------------------------------------

class TestUnknownMethod:
    def test_unknown_gives_nan_not_fallback(self):
        result = processing.calculate_forecast_range(_gettext, _make_table(), "unknown_xyz", 0)
        assert math.isnan(result["fc_lower"].iloc[0])
        assert math.isnan(result["fc_upper"].iloc[0])


# ---------------------------------------------------------------------------
# No delta column present
# ---------------------------------------------------------------------------

class TestNoDeltaColumn:
    def test_delta_method_no_delta_column_gives_nan(self):
        t = pd.DataFrame([{"forecasted_discharge": 100.0, "Q25": 90.0, "Q75": 110.0, "model_short": "GBT"}])
        result = processing.calculate_forecast_range(_gettext, t, "delta", 0)
        assert math.isnan(result["fc_lower"].iloc[0])
        assert math.isnan(result["fc_upper"].iloc[0])


# ---------------------------------------------------------------------------
# Long-term range_delta substitution
# ---------------------------------------------------------------------------

class TestLongTermRangeDeltaSubstitution:
    """Caller substitutes range_delta into delta before calling — verify contract."""

    def test_range_delta_used_as_delta_in_delta_mode(self):
        t = pd.DataFrame([{
            "forecasted_discharge": 100.0,
            "delta": float("nan"),   # skill delta — should NOT be used
            "range_delta": 8.0,      # observation-derived spread
            "Q25": float("nan"),
            "Q75": float("nan"),
            "model_short": "GBT",
        }])
        # Caller substitutes before calling (as vizualization.py does for LT)
        lt_copy = t.copy()
        lt_copy["delta"] = lt_copy["range_delta"]
        result = processing.calculate_forecast_range(_gettext, lt_copy, "delta", 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(92.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(108.0)

    def test_absent_range_delta_gives_nan(self):
        t = pd.DataFrame([{
            "forecasted_discharge": 100.0,
            "delta": float("nan"),
            "Q25": float("nan"),
            "Q75": float("nan"),
            "model_short": "GBT",
        }])
        # No range_delta column → delta stays NaN
        result = processing.calculate_forecast_range(_gettext, t.copy(), "delta", 0)
        assert math.isnan(result["fc_lower"].iloc[0])


# ---------------------------------------------------------------------------
# Widget value support
# ---------------------------------------------------------------------------

class TestWidgetValue:
    def test_accepts_widget_object_for_slider(self):
        class FakeSlider:
            value = 10

        t = _make_table()
        result = processing.calculate_forecast_range(_gettext, t, "manual", FakeSlider())
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)

    def test_accepts_widget_object_for_selector(self):
        class FakeSelector:
            value = "delta"

        t = _make_table()
        result = processing.calculate_forecast_range(_gettext, t, FakeSelector(), 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)


# ---------------------------------------------------------------------------
# Production path: create_forecast_summary_table
# ---------------------------------------------------------------------------
# These tests import vizualization — guard under SAPPHIRE_TEST_ENV is set by
# conftest._dashboard_env. The import itself is lazy (inside each test class)
# so collection does not fail if heavy deps are absent.

class _FakeModelSelection:
    """Minimal stand-in for the Panel MultiSelect / CheckBoxGroup widget."""

    def __init__(self, model_shorts):
        # options must be a dict {display_key: model_short} as the real widget
        self.options = {m: m for m in model_shorts}
        self.value = list(model_shorts)


def _make_short_term_forecasts(horizon="pentad"):
    """Minimal short-term forecast DataFrame with one LR and one GBT row."""
    horizon_col = "pentad_in_year" if horizon == "pentad" else "decad_in_year"
    dates = pd.to_datetime(["2026-03-06", "2026-03-06"])
    return pd.DataFrame({
        "code": ["19999", "19999"],
        "date": dates,
        "Date": dates + pd.Timedelta(days=1),
        "year": [2026, 2026],
        "station_labels": ["19999 - Test River", "19999 - Test River"],
        horizon_col: [13, 13],
        "model_short": ["GBT", "LR"],
        "model_long": ["Gradient Boosting (GBT)", "Linear regression (LR)"],
        "forecasted_discharge": [100.0, 90.0],
        "Q25": [85.0, float("nan")],
        "Q75": [115.0, float("nan")],
        "delta": [10.0, 8.0],
        "accuracy": [80.0, 75.0],
        "sdivsigma": [0.5, 0.6],
        "mae": [2.0, 2.5],
    })


def _make_long_term_forecasts(horizon="month", range_delta=12.0):
    """Minimal long-term forecast DataFrame. range_delta=None omits the column."""
    dates = pd.to_datetime(["2026-03-01", "2026-03-01"])
    row = {
        "code": "19999",
        "date": dates[0],
        "Date": dates[0] + pd.Timedelta(days=1),
        "year": 2026,
        "station_labels": "19999 - Test River",
        "month_in_year": 3,
        "quarter_in_year": 1,
        "model_short": "LR",
        "model_long": "Linear regression (LR)",
        "forecasted_discharge": 100.0,
        "Q25": float("nan"),
        "Q75": float("nan"),
        "delta": 999.0,  # skill delta — must NOT appear in LT bounds
        "accuracy": 70.0,
        "sdivsigma": 0.7,
        "mae": 3.0,
    }
    if range_delta is not None:
        row["range_delta"] = range_delta
    df = pd.DataFrame([row])
    df["date"] = pd.to_datetime(df["date"])
    df["Date"] = pd.to_datetime(df["Date"])
    return df


class TestCreateForecastSummaryTableShortTerm:
    """create_forecast_summary_table exercises — short-term horizons."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz
        self.viz = viz

    def _call(self, forecasts, horizon, range_type, range_slider=0):
        ms = _FakeModelSelection(forecasts["model_short"].unique().tolist())
        date_picker = forecasts["date"].max()
        return self.viz.create_forecast_summary_table(
            _gettext, horizon, forecasts,
            "19999 - Test River", date_picker, ms,
            range_type, range_slider,
        )

    def test_pentad_delta_method_bounds(self):
        """GBT row: fc_lower = Q-delta, fc_upper = Q+delta."""
        df = _make_short_term_forecasts("pentad")
        result = self._call(df, "pentad", "delta")
        gbt = result[result[_gettext("Model")] == "GBT"]
        assert len(gbt) == 1
        lower = gbt[_gettext("Forecast lower bound")].iloc[0]
        upper = gbt[_gettext("Forecast upper bound")].iloc[0]
        assert lower == pytest.approx(90.0, abs=0.01)
        assert upper == pytest.approx(110.0, abs=0.01)

    def test_pentad_native_method_gbt_uses_q25_q75(self):
        """Native mode: GBT gets Q25/Q75 as bounds."""
        df = _make_short_term_forecasts("pentad")
        result = self._call(df, "pentad", "native")
        gbt = result[result[_gettext("Model")] == "GBT"]
        lower = gbt[_gettext("Forecast lower bound")].iloc[0]
        upper = gbt[_gettext("Forecast upper bound")].iloc[0]
        assert lower == pytest.approx(85.0, abs=0.01)
        assert upper == pytest.approx(115.0, abs=0.01)

    def test_pentad_native_method_lr_uses_delta(self):
        """Native mode: LR row uses Q±delta (no native quantiles)."""
        df = _make_short_term_forecasts("pentad")
        result = self._call(df, "pentad", "native")
        lr = result[result[_gettext("Model")] == "LR"]
        lower = lr[_gettext("Forecast lower bound")].iloc[0]
        upper = lr[_gettext("Forecast upper bound")].iloc[0]
        assert lower == pytest.approx(82.0, abs=0.01)   # 90 - 8
        assert upper == pytest.approx(98.0, abs=0.01)   # 90 + 8

    def test_decad_delta_method_bounds(self):
        """Decade horizon: same delta-method bounds."""
        df = _make_short_term_forecasts("decade")
        result = self._call(df, "decade", "delta")
        gbt = result[result[_gettext("Model")] == "GBT"]
        lower = gbt[_gettext("Forecast lower bound")].iloc[0]
        upper = gbt[_gettext("Forecast upper bound")].iloc[0]
        assert lower == pytest.approx(90.0, abs=0.01)
        assert upper == pytest.approx(110.0, abs=0.01)

    def test_short_term_uses_skill_delta_not_range_delta(self):
        """Short-term path must use skill delta even when range_delta column present."""
        df = _make_short_term_forecasts("pentad")
        df["range_delta"] = 50.0  # should NOT be used for short-term
        result = self._call(df, "pentad", "delta")
        gbt = result[result[_gettext("Model")] == "GBT"]
        # bounds based on delta=10, not range_delta=50
        lower = gbt[_gettext("Forecast lower bound")].iloc[0]
        upper = gbt[_gettext("Forecast upper bound")].iloc[0]
        assert lower == pytest.approx(90.0, abs=0.01)
        assert upper == pytest.approx(110.0, abs=0.01)

    def test_output_columns_present(self):
        """Expected columns present in output after rename."""
        df = _make_short_term_forecasts("pentad")
        result = self._call(df, "pentad", "delta")
        for col in [_gettext("Model"), _gettext("Forecasted discharge"),
                    _gettext("Forecast lower bound"), _gettext("Forecast upper bound"),
                    _gettext("δ")]:
            assert col in result.columns, f"Missing column: {col!r}"


class TestCreateForecastSummaryTableLongTerm:
    """create_forecast_summary_table — long-term horizons use range_delta exclusively."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz
        self.viz = viz

    def _call(self, forecasts, horizon, range_type="delta", range_slider=0):
        ms = _FakeModelSelection(forecasts["model_short"].unique().tolist())
        date_picker = forecasts["date"].max()
        return self.viz.create_forecast_summary_table(
            _gettext, horizon, forecasts,
            "19999 - Test River", date_picker, ms,
            range_type, range_slider,
        )

    # -- range_delta present and finite --

    @pytest.mark.parametrize("horizon", ["month", "quarter", "season"])
    def test_range_delta_used_for_bounds(self, horizon):
        """range_delta=12 → fc_lower=88, fc_upper=112 for Q=100 delta mode."""
        df = _make_long_term_forecasts(horizon, range_delta=12.0)
        result = self._call(df, horizon)
        lower = result[_gettext("Forecast lower bound")].iloc[0]
        upper = result[_gettext("Forecast upper bound")].iloc[0]
        assert lower == pytest.approx(88.0, abs=0.01)
        assert upper == pytest.approx(112.0, abs=0.01)

    @pytest.mark.parametrize("horizon", ["month", "quarter", "season"])
    def test_displayed_delta_is_range_delta_not_skill_delta(self, horizon):
        """Displayed δ column = range_delta (12), not skill delta (999)."""
        df = _make_long_term_forecasts(horizon, range_delta=12.0)
        result = self._call(df, horizon)
        displayed_delta = result[_gettext("δ")].iloc[0]
        assert displayed_delta == pytest.approx(12.0, abs=0.01)

    # -- range_delta column absent --

    @pytest.mark.parametrize("horizon", ["month", "quarter", "season"])
    def test_absent_range_delta_gives_nan_bounds(self, horizon):
        """When range_delta column absent, bounds must be NaN even if skill delta finite."""
        df = _make_long_term_forecasts(horizon, range_delta=None)
        # skill delta=999 is present but must not be used
        result = self._call(df, horizon)
        lower = result[_gettext("Forecast lower bound")].iloc[0]
        upper = result[_gettext("Forecast upper bound")].iloc[0]
        assert math.isnan(float(lower)), f"Expected NaN lower, got {lower}"
        assert math.isnan(float(upper)), f"Expected NaN upper, got {upper}"

    @pytest.mark.parametrize("horizon", ["month", "quarter", "season"])
    def test_absent_range_delta_gives_nan_displayed_delta(self, horizon):
        """When range_delta absent, displayed δ is NaN, not skill delta."""
        df = _make_long_term_forecasts(horizon, range_delta=None)
        result = self._call(df, horizon)
        displayed_delta = result[_gettext("δ")].iloc[0]
        assert math.isnan(float(displayed_delta)), f"Expected NaN δ, got {displayed_delta}"

    # -- range_delta column present but NaN --

    def test_nan_range_delta_gives_nan_bounds(self):
        """range_delta=NaN in a present column → NaN bounds."""
        df = _make_long_term_forecasts("month", range_delta=float("nan"))
        result = self._call(df, "month")
        lower = result[_gettext("Forecast lower bound")].iloc[0]
        upper = result[_gettext("Forecast upper bound")].iloc[0]
        assert math.isnan(float(lower))
        assert math.isnan(float(upper))

    # -- range_delta clamp --

    def test_range_delta_clamps_lower_to_zero(self):
        """Q=5, range_delta=20 → fc_lower clamped to 0."""
        df = _make_long_term_forecasts("month", range_delta=20.0)
        df["forecasted_discharge"] = 5.0
        result = self._call(df, "month")
        lower = result[_gettext("Forecast lower bound")].iloc[0]
        assert float(lower) == pytest.approx(0.0, abs=0.001)

    # -- input frame not mutated --

    def test_input_frame_not_mutated_by_long_term_path(self):
        """create_forecast_summary_table must not change delta column of caller's frame."""
        df = _make_long_term_forecasts("month", range_delta=12.0)
        original_delta = df["delta"].iloc[0]  # 999.0
        self._call(df, "month")
        assert df["delta"].iloc[0] == pytest.approx(original_delta)

    # -- multi-row: mixed NaN and finite range_delta --

    def test_multirow_mixed_range_delta(self):
        """Row with finite range_delta gets bounds; row with NaN range_delta gets NaN."""
        df = pd.DataFrame([
            {
                "code": "19999",
                "date": pd.Timestamp("2026-03-01"),
                "Date": pd.Timestamp("2026-03-02"),
                "year": 2026,
                "station_labels": "19999 - Test River",
                "month_in_year": 3,
                "model_short": "LR",
                "model_long": "Linear regression (LR)",
                "forecasted_discharge": 100.0,
                "Q25": float("nan"),
                "Q75": float("nan"),
                "delta": 999.0,
                "range_delta": 10.0,
                "accuracy": 70.0,
                "sdivsigma": 0.7,
                "mae": 3.0,
            },
            {
                "code": "19999",
                "date": pd.Timestamp("2026-03-01"),
                "Date": pd.Timestamp("2026-03-02"),
                "year": 2026,
                "station_labels": "19999 - Test River",
                "month_in_year": 3,
                "model_short": "GBT",
                "model_long": "Gradient Boosting (GBT)",
                "forecasted_discharge": 80.0,
                "Q25": float("nan"),
                "Q75": float("nan"),
                "delta": 888.0,
                "range_delta": float("nan"),
                "accuracy": 60.0,
                "sdivsigma": 0.8,
                "mae": 4.0,
            },
        ])
        ms = _FakeModelSelection(["LR", "GBT"])
        date_picker = pd.Timestamp("2026-03-01")
        from src import vizualization as viz
        result = viz.create_forecast_summary_table(
            _gettext, "month", df,
            "19999 - Test River", date_picker, ms,
            "delta", 0,
        )
        lr_row = result[result[_gettext("Model")] == "LR"]
        gbt_row = result[result[_gettext("Model")] == "GBT"]
        # LR: range_delta=10 → bounds finite
        assert lr_row[_gettext("Forecast lower bound")].iloc[0] == pytest.approx(90.0, abs=0.01)
        assert lr_row[_gettext("Forecast upper bound")].iloc[0] == pytest.approx(110.0, abs=0.01)
        # GBT: range_delta=NaN → bounds NaN
        assert math.isnan(float(gbt_row[_gettext("Forecast lower bound")].iloc[0]))
        assert math.isnan(float(gbt_row[_gettext("Forecast upper bound")].iloc[0]))

    # -- non-identity gettext renames columns --

    def test_non_identity_gettext_renames_columns(self):
        """Column names follow the gettext function passed in."""
        translations = {
            "Model": "Модель",
            "Forecast lower bound": "Нижняя граница",
            "Forecast upper bound": "Верхняя граница",
            "Forecasted discharge": "Прогноз расхода",
            "δ": "δ",
            "s/σ": "s/σ",
            "MAE": "MAE",
            "Accuracy": "Точность",
        }

        def fake_gt(s):
            return translations.get(s, s)

        df = _make_long_term_forecasts("month", range_delta=12.0)
        ms = _FakeModelSelection(["LR"])
        date_picker = df["date"].max()
        from src import vizualization as viz
        result = viz.create_forecast_summary_table(
            fake_gt, "month", df,
            "19999 - Test River", date_picker, ms,
            "delta", 0,
        )
        assert "Нижняя граница" in result.columns
        assert "Верхняя граница" in result.columns
        assert "Модель" in result.columns


class TestCreateForecastSummaryTableFilterByDate:
    """filter_by_date=False skips max-date reduction."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz
        self.viz = viz

    def test_filter_by_date_false_keeps_all_rows(self):
        """When filter_by_date=False, rows with different dates are preserved."""
        dates = pd.to_datetime(["2026-03-01", "2026-03-06"])
        df = pd.DataFrame({
            "code": ["19999", "19999"],
            "date": dates,
            "Date": dates + pd.Timedelta(days=1),
            "year": [2026, 2026],
            "station_labels": ["19999 - Test River", "19999 - Test River"],
            "model_short": ["LR", "GBT"],
            "model_long": ["Linear regression (LR)", "Gradient Boosting (GBT)"],
            "forecasted_discharge": [90.0, 100.0],
            "Q25": [float("nan"), 85.0],
            "Q75": [float("nan"), 115.0],
            "delta": [8.0, 10.0],
            "accuracy": [75.0, 80.0],
            "sdivsigma": [0.6, 0.5],
            "mae": [2.5, 2.0],
        })
        ms = _FakeModelSelection(["LR", "GBT"])
        date_picker = pd.Timestamp("2026-03-10")
        result = self.viz.create_forecast_summary_table(
            _gettext, "pentad", df,
            "19999 - Test River", date_picker, ms,
            "delta", 0,
            filter_by_date=False,
        )
        assert len(result) == 2


class TestCreateForecastSummaryTableMissingForecast:
    """Rows without a forecast value are excluded from the table."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz
        self.viz = viz

    def test_null_forecasted_discharge_row_excluded(self):
        dates = pd.to_datetime(["2026-03-06", "2026-03-06"])
        df = pd.DataFrame({
            "code": ["19999", "19999"],
            "date": dates,
            "Date": dates + pd.Timedelta(days=1),
            "year": [2026, 2026],
            "station_labels": ["19999 - Test River", "19999 - Test River"],
            "pentad_in_year": [13, 13],
            "model_short": ["GBT", "LR"],
            "model_long": ["Gradient Boosting (GBT)", "Linear regression (LR)"],
            "forecasted_discharge": [100.0, float("nan")],
            "Q25": [85.0, float("nan")],
            "Q75": [115.0, float("nan")],
            "delta": [10.0, 8.0],
            "accuracy": [80.0, 75.0],
            "sdivsigma": [0.5, 0.6],
            "mae": [2.0, 2.5],
        })
        ms = _FakeModelSelection(["GBT", "LR"])
        date_picker = pd.Timestamp("2026-03-06")
        result = self.viz.create_forecast_summary_table(
            _gettext, "pentad", df,
            "19999 - Test River", date_picker, ms,
            "delta", 0,
        )
        # Only GBT with a real forecast value should be in the table
        assert len(result) == 1
        assert result[_gettext("Model")].iloc[0] == "GBT"


class TestCalculateForecastRangeMultiRow:
    """Multi-row frames: NaN gaps preserved, finite rows computed independently."""

    def test_mixed_rows_nan_gap_preserved(self):
        """Middle NaN gap row stays NaN; surrounding rows compute correctly."""
        t = pd.DataFrame({
            "forecasted_discharge": [100.0, float("nan"), 80.0],
            "delta": [10.0, 5.0, 8.0],
            "Q25": [float("nan")] * 3,
            "Q75": [float("nan")] * 3,
            "model_short": ["GBT", "GBT", "GBT"],
        })
        result = processing.calculate_forecast_range(_gettext, t, "delta", 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert math.isnan(result["fc_lower"].iloc[1])  # NaN Q preserved
        assert result["fc_lower"].iloc[2] == pytest.approx(72.0)

    def test_finite_zero_delta_row_preserved(self):
        """Genuine zero delta gives zero-width band; not treated as NaN."""
        t = pd.DataFrame({
            "forecasted_discharge": [100.0, 50.0],
            "delta": [0.0, 5.0],
            "Q25": [float("nan")] * 2,
            "Q75": [float("nan")] * 2,
            "model_short": ["GBT", "GBT"],
        })
        result = processing.calculate_forecast_range(_gettext, t, "delta", 0)
        assert result["fc_lower"].iloc[0] == pytest.approx(100.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(100.0)
        assert result["fc_lower"].iloc[1] == pytest.approx(45.0)


# ---------------------------------------------------------------------------
# PP-075 regression: None (Python None) in range_delta produces object dtype
# ---------------------------------------------------------------------------

class TestNoneRangeDeltaObjectDtypeRegression:
    """Regression for PP-075: None (not NaN) in range_delta creates an object-dtype
    column, which caused TypeError at forecast_table['delta'].round(2).

    The API can return null for missing values; pandas stores a mixed None/float
    column as object dtype.  After the fix, those values must render as NaN
    (unavailable), never raise.
    """

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz
        self.viz = viz

    def _call(self, forecasts, horizon="month", range_type="delta", range_slider=0):
        ms = _FakeModelSelection(forecasts["model_short"].unique().tolist())
        date_picker = forecasts["date"].max()
        return self.viz.create_forecast_summary_table(
            _gettext, horizon, forecasts,
            "19999 - Test River", date_picker, ms,
            range_type, range_slider,
        )

    def test_single_none_range_delta_does_not_raise(self):
        """Single row with range_delta=None (Python None → object dtype) must not
        raise TypeError and must display δ as NaN (unavailable)."""
        df = pd.DataFrame([{
            "code": "19999",
            "date": pd.Timestamp("2026-04-01"),
            "Date": pd.Timestamp("2026-04-02"),
            "year": 2026,
            "station_labels": "19999 - Test River",
            "month_in_year": 4,
            "model_short": "LR",
            "model_long": "Linear regression (LR)",
            "forecasted_discharge": 100.0,
            "Q25": float("nan"),
            "Q75": float("nan"),
            "delta": 5.0,
            "range_delta": None,  # Python None → object dtype
            "accuracy": 70.0,
            "sdivsigma": 0.7,
            "mae": 3.0,
        }])
        assert df["range_delta"].dtype == object, "pre-condition: None makes object dtype"
        result = self._call(df, "month")
        displayed_delta = result[_gettext("δ")].iloc[0]
        assert math.isnan(float(displayed_delta)), (
            f"None range_delta must render as NaN δ, got {displayed_delta!r}"
        )
        # Bounds must also be NaN (no valid range_delta to compute from)
        assert math.isnan(float(result[_gettext("Forecast lower bound")].iloc[0]))
        assert math.isnan(float(result[_gettext("Forecast upper bound")].iloc[0]))

    def test_mixed_none_and_finite_range_delta_object_dtype(self):
        """When range_delta column is object-typed (e.g. API-sourced mixed None/float
        or an all-None station), rows with finite values get correct bounds and None
        rows get NaN — no TypeError.

        pandas 2.x coerces mixed None/float to float64, so we explicitly cast to
        object dtype to match the production case where the column arrives as object.
        """
        df = pd.DataFrame([
            {
                "code": "19999",
                "date": pd.Timestamp("2026-04-01"),
                "Date": pd.Timestamp("2026-04-02"),
                "year": 2026,
                "station_labels": "19999 - Test River",
                "month_in_year": 4,
                "model_short": "LR",
                "model_long": "Linear regression (LR)",
                "forecasted_discharge": 100.0,
                "Q25": float("nan"),
                "Q75": float("nan"),
                "delta": 999.0,
                "range_delta": 10.0,   # finite — bounds must be computable
                "accuracy": 70.0,
                "sdivsigma": 0.7,
                "mae": 3.0,
            },
            {
                "code": "19999",
                "date": pd.Timestamp("2026-04-01"),
                "Date": pd.Timestamp("2026-04-02"),
                "year": 2026,
                "station_labels": "19999 - Test River",
                "month_in_year": 4,
                "model_short": "GBT",
                "model_long": "Gradient Boosting (GBT)",
                "forecasted_discharge": 80.0,
                "Q25": float("nan"),
                "Q75": float("nan"),
                "delta": 888.0,
                "range_delta": float("nan"),   # unavailable
                "accuracy": 60.0,
                "sdivsigma": 0.8,
                "mae": 4.0,
            },
        ])
        # Explicitly force object dtype to simulate the production API-sourced column.
        # pandas 2.x coerces mixed None/float to float64, but the API client can
        # deliver an object-typed column when nulls arrive as Python None.
        df["range_delta"] = df["range_delta"].astype(object)
        assert df["range_delta"].dtype == object, "pre-condition: explicitly cast to object"
        result = self._call(df, "month")
        lr_row = result[result[_gettext("Model")] == "LR"]
        gbt_row = result[result[_gettext("Model")] == "GBT"]
        # LR: range_delta=10 → fc_lower=90, fc_upper=110, δ=10
        assert lr_row[_gettext("Forecast lower bound")].iloc[0] == pytest.approx(90.0, abs=0.01)
        assert lr_row[_gettext("Forecast upper bound")].iloc[0] == pytest.approx(110.0, abs=0.01)
        assert lr_row[_gettext("δ")].iloc[0] == pytest.approx(10.0, abs=0.01)
        # GBT: range_delta=None → NaN bounds and NaN δ
        assert math.isnan(float(gbt_row[_gettext("Forecast lower bound")].iloc[0]))
        assert math.isnan(float(gbt_row[_gettext("δ")].iloc[0]))

    def test_none_sdivsigma_and_mae_do_not_raise(self):
        """None in sdivsigma and mae (API-nullable fields) must also be safe at round()."""
        df = pd.DataFrame([{
            "code": "19999",
            "date": pd.Timestamp("2026-04-01"),
            "Date": pd.Timestamp("2026-04-02"),
            "year": 2026,
            "station_labels": "19999 - Test River",
            "month_in_year": 4,
            "model_short": "LR",
            "model_long": "Linear regression (LR)",
            "forecasted_discharge": 100.0,
            "Q25": float("nan"),
            "Q75": float("nan"),
            "delta": 5.0,
            "range_delta": 12.0,
            "accuracy": None,   # None — object dtype
            "sdivsigma": None,  # None — object dtype
            "mae": None,        # None — object dtype
        }])
        result = self._call(df, "month")
        # Bounds must be finite (range_delta=12 is numeric)
        assert result[_gettext("Forecast lower bound")].iloc[0] == pytest.approx(88.0, abs=0.01)
        assert result[_gettext("Forecast upper bound")].iloc[0] == pytest.approx(112.0, abs=0.01)
        # Skill columns should be NaN (not raise)
        assert math.isnan(float(result[_gettext("s/σ")].iloc[0]))
        assert math.isnan(float(result[_gettext("MAE")].iloc[0]))
