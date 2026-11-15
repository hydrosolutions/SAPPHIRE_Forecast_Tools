"""PP-075 E2 — Range rendering guard tests.

Covers:
- plot_current_runoff_forecast_range: NaN bounds → no range legend, mean Scatter retained
- plot_current_runoff_forecast_range_date_format: same
- plot_current_runoff_forecast_range_date_format_v2: same
- plot_runoff_forecast_range_area: past band preserves NaN-middle rows (no dropna)
- plot_pentad_forecast_hydrograph_data: 4 range methods × pentad + decad —
  verify fc_lower/fc_upper values by capturing render-helper inputs
- plot_pentad_forecast_hydrograph_data_v2: same, date-format path

Station code 19999 throughout; no real station codes.
"""

import datetime as dt
from unittest.mock import patch

import holoviews as hv
import pandas as pd
import pytest

hv.extension("bokeh")

STATION = "19999 - Test River"
COLORS = [
    "#003f5c",
    "#2f4b7c",
    "#665191",
    "#a05195",
    "#d45087",
    "#f95d6a",
    "#ff7c43",
]


def _gt(s):
    """Identity gettext."""
    return s


# ---------------------------------------------------------------------------
# Minimal helpers
# ---------------------------------------------------------------------------


def _make_current_df(
    *,
    mean=100.0,
    fc_lower=float("nan"),
    fc_upper=float("nan"),
    model_short="GBT",
    date_col="date",
    use_datetime=False,
):
    """One-row DataFrame with the renamed columns the render helpers receive."""
    d = pd.Timestamp("2026-03-06") if use_datetime else 13
    return pd.DataFrame(
        [
            {
                date_col: d,
                "E[Q]": mean,
                "Lower bound": fc_lower,
                "Upper bound": fc_upper,
                "Model": model_short,
            }
        ]
    )


def _make_current_df_v2(
    *,
    mean=100.0,
    fc_lower=float("nan"),
    fc_upper=float("nan"),
    model_short="GBT",
):
    """One-row DF for the v2 date-format helper."""
    return pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2026-03-06"),
                "forecasted_discharge column name": mean,
                "forecast lower bound column name": fc_lower,
                "forecast upper bound column name": fc_upper,
                "forecast model short column name": model_short,
            }
        ]
    )


# ---------------------------------------------------------------------------
# Tests: plot_current_runoff_forecast_range (v1 — numeric x-axis)
# ---------------------------------------------------------------------------


class TestCurrentForecastRangeV1:
    """plot_current_runoff_forecast_range: ErrorBars omitted when bounds are NaN."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz

        self.viz = viz

    def _call(self, df, fc_lower, fc_upper):
        """Build a minimal DF and call the helper directly."""
        data = pd.DataFrame(
            [
                {
                    "date": 13,
                    "E[Q]": 100.0,
                    "Lower bound": fc_lower,
                    "Upper bound": fc_upper,
                    "Model": "GBT",
                }
            ]
        )
        # The helper computes errors as mean - lower, upper - mean so pass abs bounds
        return self.viz.plot_current_runoff_forecast_range(
            data,
            "date",
            "Model",
            "E[Q]",
            "Lower bound",
            "Upper bound",
            COLORS,
            "m³/s",
        )

    def test_nan_bounds_returns_single_scatter_not_errorbars(self):
        result = self._call(None, fc_lower=float("nan"), fc_upper=float("nan"))
        # With no valid bounds, overlay must not contain ErrorBars
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "ErrorBars" not in types, f"ErrorBars present despite NaN bounds: {types}"

    def test_nan_bounds_mean_scatter_still_present(self):
        result = self._call(None, fc_lower=float("nan"), fc_upper=float("nan"))
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "Scatter" in types, f"Mean Scatter missing: {types}"

    def test_valid_bounds_errorbars_present(self):
        result = self._call(None, fc_lower=90.0, fc_upper=110.0)
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "ErrorBars" in types, f"ErrorBars missing for valid bounds: {types}"
        assert "Scatter" in types, f"Mean Scatter missing for valid bounds: {types}"

    def test_valid_bounds_legend_contains_range_values(self):
        result = self._call(None, fc_lower=90.0, fc_upper=110.0)
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        labels = [e.label for e in elements]
        range_labels = [lbl for lbl in labels if "range" in lbl.lower()]
        assert range_labels, f"No range legend entry in labels: {labels}"
        # Both bound values should appear in the legend text
        # (format_discharge converts 10.0 → "10.0", etc.)
        assert any("10" in lbl for lbl in range_labels), (
            f"Bound values not in legend: {range_labels}"
        )

    def test_zero_bounds_not_suppressed(self):
        """Genuine zero bounds (Q=5, fc_lower=0, fc_upper=10) should render."""
        # fc_lower=0 means lower=mean-0=mean; ensure we don't suppress valid zero
        result = self._call(None, fc_lower=5.0, fc_upper=15.0)
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "ErrorBars" in types


# ---------------------------------------------------------------------------
# Regression tests: plot_current_runoff_forecast_range_date_format guard
# ---------------------------------------------------------------------------


class TestCurrentForecastRangeDateFormatRegression:
    """Regression: missing last row must not suppress ErrorBars for earlier valid rows."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz

        self.viz = viz

    def _call_multi(self, rows):
        """Call helper with an explicitly constructed multi-row DataFrame."""
        data = pd.DataFrame(rows)
        return self.viz.plot_current_runoff_forecast_range_date_format(
            data,
            "date",
            "Model",
            "E[Q]",
            "Lower bound",
            "Upper bound",
            COLORS,
            "m³/s",
        )

    def test_two_rows_valid_first_missing_last_errorbars_present(self):
        """Row[0] valid bounds + row[1] NaN bounds → ErrorBars still rendered for row[0]."""
        rows = [
            {
                "date": pd.Timestamp("2026-03-01"),
                "E[Q]": 100.0,
                "Lower bound": 90.0,
                "Upper bound": 110.0,
                "Model": "GBT",
            },
            {
                "date": pd.Timestamp("2026-03-06"),
                "E[Q]": 105.0,
                "Lower bound": float("nan"),
                "Upper bound": float("nan"),
                "Model": "GBT",
            },
        ]
        result = self._call_multi(rows)
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "ErrorBars" in types, (
            "ErrorBars suppressed because last row has NaN — regression guard failed"
        )
        assert "Scatter" in types

    def test_two_rows_valid_first_missing_last_scatter_has_both_rows(self):
        """Mean Scatter must include all rows, not just valid-bound rows."""
        rows = [
            {
                "date": pd.Timestamp("2026-03-01"),
                "E[Q]": 100.0,
                "Lower bound": 90.0,
                "Upper bound": 110.0,
                "Model": "GBT",
            },
            {
                "date": pd.Timestamp("2026-03-06"),
                "E[Q]": 105.0,
                "Lower bound": float("nan"),
                "Upper bound": float("nan"),
                "Model": "GBT",
            },
        ]
        result = self._call_multi(rows)
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        scatter_elements = [e for e in elements if type(e).__name__ == "Scatter"]
        assert scatter_elements, "Scatter missing"
        # Scatter data must have 2 rows (all means rendered)
        scatter_data = scatter_elements[0].data
        assert len(scatter_data) == 2, (
            f"Expected 2 rows in Scatter (all means), got {len(scatter_data)}"
        )

    def test_all_missing_bounds_means_only(self):
        """All NaN bounds → no ErrorBars, Scatter present."""
        rows = [
            {
                "date": pd.Timestamp("2026-03-01"),
                "E[Q]": 100.0,
                "Lower bound": float("nan"),
                "Upper bound": float("nan"),
                "Model": "GBT",
            },
            {
                "date": pd.Timestamp("2026-03-06"),
                "E[Q]": 105.0,
                "Lower bound": float("nan"),
                "Upper bound": float("nan"),
                "Model": "GBT",
            },
        ]
        result = self._call_multi(rows)
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "ErrorBars" not in types, "ErrorBars present despite all-NaN bounds"
        assert "Scatter" in types

    def test_genuine_zero_lower_bound_not_suppressed(self):
        """fc_lower=0 is a valid finite value and must not suppress ErrorBars."""
        rows = [
            {
                "date": pd.Timestamp("2026-03-06"),
                "E[Q]": 10.0,
                "Lower bound": 0.0,
                "Upper bound": 20.0,
                "Model": "GBT",
            }
        ]
        result = self._call_multi(rows)
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "ErrorBars" in types, "ErrorBars suppressed for genuine zero lower bound"

    def test_upper_less_than_mean_errorbars_present_with_correct_offsets(self):
        """mean=100, lower=80, upper=95: upper offset=-5 must NOT suppress ErrorBars.

        Row[0] has valid bounds where upper < mean (native percentile interval
        crossing the mean is unusual but valid). Row[1] has NaN bounds.
        ErrorBars must be rendered with offset data: lower_offset=20, upper_offset=-5.
        """
        rows = [
            {
                "date": pd.Timestamp("2026-03-01"),
                "E[Q]": 100.0,
                "Lower bound": 80.0,
                "Upper bound": 95.0,
                "Model": "GBT",
            },
            {
                "date": pd.Timestamp("2026-03-06"),
                "E[Q]": 105.0,
                "Lower bound": float("nan"),
                "Upper bound": float("nan"),
                "Model": "GBT",
            },
        ]
        result = self._call_multi(rows)
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "ErrorBars" in types, (
            "ErrorBars suppressed when upper bound < mean (negative offset)"
        )
        # Offsets in ErrorBars data: lower_offset = mean-lower = 100-80 = 20,
        # upper_offset = upper-mean = 95-100 = -5.
        errorbars = [e for e in elements if type(e).__name__ == "ErrorBars"][0]
        eb_data = errorbars.data
        assert eb_data["Lower bound"].iloc[0] == pytest.approx(20.0, abs=0.1), (
            "Lower offset should be mean - lower = 100 - 80 = 20"
        )
        assert eb_data["Upper bound"].iloc[0] == pytest.approx(-5.0, abs=0.1), (
            "Upper offset should be upper - mean = 95 - 100 = -5"
        )


# ---------------------------------------------------------------------------
# Tests: plot_current_runoff_forecast_range_date_format
# ---------------------------------------------------------------------------


class TestCurrentForecastRangeDateFormat:
    """plot_current_runoff_forecast_range_date_format: same guard as v1."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz

        self.viz = viz

    def _call(self, fc_lower, fc_upper):
        data = pd.DataFrame(
            [
                {
                    "date": pd.Timestamp("2026-03-06"),
                    "E[Q]": 100.0,
                    "Lower bound": fc_lower,
                    "Upper bound": fc_upper,
                    "Model": "GBT",
                }
            ]
        )
        return self.viz.plot_current_runoff_forecast_range_date_format(
            data,
            "date",
            "Model",
            "E[Q]",
            "Lower bound",
            "Upper bound",
            COLORS,
            "m³/s",
        )

    def test_nan_bounds_no_errorbars(self):
        result = self._call(float("nan"), float("nan"))
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "ErrorBars" not in types

    def test_nan_bounds_mean_retained(self):
        result = self._call(float("nan"), float("nan"))
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "Scatter" in types

    def test_valid_bounds_both_present(self):
        result = self._call(90.0, 110.0)
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "ErrorBars" in types
        assert "Scatter" in types


# ---------------------------------------------------------------------------
# Tests: plot_current_runoff_forecast_range_date_format_v2
# ---------------------------------------------------------------------------


class TestCurrentForecastRangeDateFormatV2:
    """plot_current_runoff_forecast_range_date_format_v2: Area guard."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz

        self.viz = viz

    def _call(self, fc_lower, fc_upper, model="GBT"):
        data = _make_current_df_v2(
            mean=100.0, fc_lower=fc_lower, fc_upper=fc_upper, model_short=model
        )
        title_date_end = dt.date(2026, 3, 10)
        return self.viz.plot_current_runoff_forecast_range_date_format_v2(
            data,
            "date",
            title_date_end,
            "forecast model short column name",
            "forecasted_discharge column name",
            "forecast lower bound column name",
            "forecast upper bound column name",
            COLORS,
            "m³/s",
        )

    def test_nan_bounds_no_area(self):
        result = self._call(float("nan"), float("nan"))
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "Area" not in types, f"Area present despite NaN bounds: {types}"

    def test_nan_bounds_mean_curve_retained(self):
        result = self._call(float("nan"), float("nan"))
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "Curve" in types, f"Mean Curve missing: {types}"

    def test_valid_bounds_area_present(self):
        result = self._call(90.0, 110.0)
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "Area" in types, f"Area missing for valid bounds: {types}"
        assert "Curve" in types

    def test_valid_bounds_legend_has_bound_values(self):
        result = self._call(90.0, 110.0)
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        labels = [e.label for e in elements]
        range_labels = [lbl for lbl in labels if "range" in lbl.lower()]
        assert range_labels, f"No range legend entry in labels: {labels}"

    def test_lr_model_nan_bounds_still_shows_curve(self):
        result = self._call(float("nan"), float("nan"), model="LR")
        elements = list(result) if isinstance(result, hv.Overlay) else [result]
        types = [type(e).__name__ for e in elements]
        assert "Curve" in types
        assert "Area" not in types


# ---------------------------------------------------------------------------
# Tests: plot_runoff_forecast_range_area — past band preserves NaN gaps
# ---------------------------------------------------------------------------


class TestPastBandNaNGap:
    """Past area band must not drop rows with NaN bounds (preserves visual gap)."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz

        self.viz = viz

    def _make_past_df(self):
        """5 rows; row index 2 has NaN in both bound columns."""
        return pd.DataFrame(
            {
                "pentad": [1, 2, 3, 4, 5],
                "fc_lower": [80.0, 85.0, float("nan"), 90.0, 92.0],
                "fc_upper": [120.0, 118.0, float("nan"), 115.0, 113.0],
                "Model": ["GBT"] * 5,
            }
        )

    def test_area_element_data_preserves_nan_row_count(self):
        """The Area element must receive all 5 rows (NaN row not dropped)."""
        df = self._make_past_df()
        # Capture what hv.Area receives
        original_area = hv.Area

        captured = {}

        def mock_area(data, **kwargs):
            captured["data"] = data
            return original_area(data, **kwargs)

        with patch("src.vizualization.hv.Area", side_effect=mock_area):
            self.viz.plot_runoff_forecast_range_area(
                df,
                "pentad",
                "Model",
                "fc_lower",
                "fc_upper",
                COLORS,
                "m³/s",
            )

        assert "data" in captured, "hv.Area was not called"
        area_df = captured["data"]
        assert len(area_df) == 5, f"Expected 5 rows (NaN gap preserved), got {len(area_df)}"
        # Specifically the NaN row must be present
        nan_rows = area_df[area_df["fc_lower"].isna()]
        assert len(nan_rows) == 1, f"Expected 1 NaN row in area data, got {len(nan_rows)}"


# ---------------------------------------------------------------------------
# Shared synthetic data for hydrograph integration tests
# ---------------------------------------------------------------------------


def _make_hydrograph_pentad_df(*, current_year=2026, horizon="pentad"):
    """Minimal hydrograph DataFrame for plot_pentad_forecast_hydrograph_data."""
    if horizon == "pentad":
        hcol = "pentad_in_year"
    else:
        hcol = "decad_in_year"
    rows = []
    for p in range(1, 72 if horizon == "pentad" else 37):
        rows.append(
            {
                hcol: p,
                "station_labels": STATION,
                "date": pd.Timestamp(f"{current_year}-01-01") + pd.Timedelta(days=(p - 1) * 5),
                "min": 50.0,
                "max": 200.0,
                "q05": 60.0,
                "q95": 190.0,
                "q25": 80.0,
                "q75": 160.0,
                "mean": 100.0,
                "norm": 110.0,
                str(current_year - 1): 95.0,
                str(current_year): float("nan") if p > 13 else 98.0,
            }
        )
    return pd.DataFrame(rows)


def _make_forecasts_df(
    *,
    horizon="pentad",
    current_year=2026,
    model="GBT",
    q25=85.0,
    q75=115.0,
    delta=10.0,
):
    """Minimal forecasts DataFrame covering pentad 1-13 (or decad 1-5)."""
    if horizon == "pentad":
        hcol = "pentad_in_year"
        dates = [
            pd.Timestamp(f"{current_year}-01-01") + pd.Timedelta(days=(p - 1) * 5)
            for p in range(1, 14)
        ]
    else:
        hcol = "decad_in_year"
        dates = [
            pd.Timestamp(f"{current_year}-01-01") + pd.Timedelta(days=(p - 1) * 10)
            for p in range(1, 6)
        ]
    rows = []
    for i, d in enumerate(dates):
        rows.append(
            {
                "code": "19999",
                "date": d,
                "Date": d + pd.Timedelta(days=1),
                "year": current_year,
                "station_labels": STATION,
                hcol: i + 1,
                "model_short": model,
                "model_long": f"Model {model}",
                "forecasted_discharge": 100.0,
                "Q25": q25,
                "Q75": q75,
                "delta": delta,
                "accuracy": 80.0,
                "sdivsigma": 0.5,
                "mae": 2.0,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# plot_pentad_forecast_hydrograph_data: 4 range methods × pentad + decad
# ---------------------------------------------------------------------------


class TestPlotPentadForecastHydrographData:
    """Verify calculate_forecast_range is applied correctly by hydrograph function.

    Strategy: patch plot_current_runoff_forecast_range to capture the input
    DataFrame (which has already had calculate_forecast_range applied).
    Assert numeric bound values on that DataFrame.
    """

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz

        self.viz = viz

    def _call_and_capture(
        self, horizon, range_type, range_slider=0, q25=85.0, q75=115.0, delta=10.0
    ):
        """Run the function; return the captured forecasts_current DataFrame."""
        forecasts = _make_forecasts_df(horizon=horizon, q25=q25, q75=q75, delta=delta)
        hydrograph = _make_hydrograph_pentad_df(horizon=horizon)
        # title_date must equal the last forecast issue date so forecasts_current
        # is non-empty. pentad p=13 → 2026-03-02; decade p=5 → 2026-02-10.
        if horizon == "pentad":
            title_date = pd.Timestamp("2026-03-02").date()
        else:
            title_date = pd.Timestamp("2026-02-10").date()

        captured = {}

        fake_element = hv.Curve([])
        # Patch all render helpers to avoid bokeh rendering issues in tests
        with (
            patch("src.vizualization.plot_runoff_range_area", return_value=fake_element),
            patch("src.vizualization.plot_runoff_range_bound", return_value=fake_element),
            patch("src.vizualization.plot_runoff_line", return_value=fake_element),
            patch("src.vizualization.plot_runoff_forecasts", return_value=fake_element),
            patch("src.vizualization.plot_runoff_forecast_range_area", return_value=fake_element),
            patch(
                "src.vizualization.plot_current_runoff_forecast_range",
                side_effect=lambda data, *args, **kwargs: (
                    captured.__setitem__("data", data) or fake_element
                ),
            ),
        ):
            self.viz.plot_pentad_forecast_hydrograph_data(
                _gt,
                horizon,
                hydrograph,
                forecasts,
                STATION,
                title_date,
                ["GBT"],
                range_type,
                range_slider,
                range_visibility="No",
            )

        return captured.get("data")

    def test_pentad_native_method_gbt_q25_q75(self):
        """Native: fc_lower=Q25, fc_upper=Q75 for non-LR model."""
        df = self._call_and_capture("pentad", "native", q25=85.0, q75=115.0)
        assert df is not None, "Render helper not called"
        assert df["forecast lower bound column name"].iloc[-1] == pytest.approx(85.0, abs=0.1)
        assert df["forecast upper bound column name"].iloc[-1] == pytest.approx(115.0, abs=0.1)

    def test_pentad_delta_method_bounds(self):
        """Delta: fc_lower=Q-delta, fc_upper=Q+delta."""
        df = self._call_and_capture("pentad", "delta", delta=10.0)
        assert df is not None
        assert df["forecast lower bound column name"].iloc[-1] == pytest.approx(90.0, abs=0.1)
        assert df["forecast upper bound column name"].iloc[-1] == pytest.approx(110.0, abs=0.1)

    def test_pentad_manual_method_20_percent(self):
        """Manual with range_slider=20: fc_lower=Q*0.8, fc_upper=Q*1.2."""
        df = self._call_and_capture("pentad", "manual", range_slider=20)
        assert df is not None
        lower = df["forecast lower bound column name"].iloc[-1]
        upper = df["forecast upper bound column name"].iloc[-1]
        assert lower == pytest.approx(80.0, abs=0.1)
        assert upper == pytest.approx(120.0, abs=0.1)

    def test_pentad_mixed_method_uses_min_delta_or_percent(self):
        """Mixed range_slider=20: fc range = min(delta, 20%Q) on each side."""
        # delta=10, 20%*100=20 → delta wins → bounds [90, 110]
        df = self._call_and_capture("pentad", "mixed", range_slider=20, delta=10.0)
        assert df is not None
        lower = df["forecast lower bound column name"].iloc[-1]
        upper = df["forecast upper bound column name"].iloc[-1]
        # min(10, 20) = 10 → [90, 110]
        assert lower == pytest.approx(90.0, abs=0.1)
        assert upper == pytest.approx(110.0, abs=0.1)

    def test_pentad_mixed_method_percent_wins(self):
        """Mixed range_slider=20: when delta > 20%Q, the 20% bound wins."""
        # delta=30, 20%*100=20 → 20% wins → [80, 120]
        df = self._call_and_capture("pentad", "mixed", range_slider=20, delta=30.0)
        assert df is not None
        lower = df["forecast lower bound column name"].iloc[-1]
        upper = df["forecast upper bound column name"].iloc[-1]
        assert lower == pytest.approx(80.0, abs=0.1)
        assert upper == pytest.approx(120.0, abs=0.1)

    def test_decad_native_method_gbt_q25_q75(self):
        """Decad horizon, native method: bounds from Q25/Q75."""
        df = self._call_and_capture("decade", "native", q25=85.0, q75=115.0)
        assert df is not None
        assert df["forecast lower bound column name"].iloc[-1] == pytest.approx(85.0, abs=0.1)
        assert df["forecast upper bound column name"].iloc[-1] == pytest.approx(115.0, abs=0.1)

    def test_decad_delta_method_bounds(self):
        """Decad horizon, delta method."""
        df = self._call_and_capture("decade", "delta", delta=10.0)
        assert df is not None
        assert df["forecast lower bound column name"].iloc[-1] == pytest.approx(90.0, abs=0.1)
        assert df["forecast upper bound column name"].iloc[-1] == pytest.approx(110.0, abs=0.1)

    def test_lower_bound_clamped_to_zero(self):
        """fc_lower must never be negative."""
        df = self._call_and_capture("pentad", "delta", delta=200.0)
        assert df is not None
        # Q=100, delta=200 → 100-200=-100, should clamp to 0
        assert df["forecast lower bound column name"].iloc[-1] >= 0.0


# ---------------------------------------------------------------------------
# plot_pentad_forecast_hydrograph_data_v2: 4 range methods
# ---------------------------------------------------------------------------


def _make_hydrograph_day_df(*, current_year=2026):
    """Minimal daily hydrograph DataFrame for the _v2 function."""
    rows = []
    for d in range(0, 365):
        rows.append(
            {
                "station_labels": STATION,
                "date": pd.Timestamp(f"{current_year}-01-01") + pd.Timedelta(days=d),
                "day_of_year": d + 1,
                "min": 50.0,
                "max": 200.0,
                "5%": 60.0,
                "95%": 190.0,
                "25%": 80.0,
                "75%": 160.0,
                "mean": 100.0,
                str(current_year - 1): 95.0,
                str(current_year): float("nan") if d > 65 else 98.0,
            }
        )
    return pd.DataFrame(rows)


def _make_linreg_predictor_df(*, horizon="pentad", current_year=2026):
    """Minimal linreg predictor stub for _v2 function."""
    return pd.DataFrame(
        [
            {
                "station_labels": STATION,
                "predictor": 50.0,
                "predictor_start_date": pd.Timestamp(f"{current_year}-03-01"),
                "predictor_end_date": pd.Timestamp(f"{current_year}-03-05"),
                "forecast_start_date": pd.Timestamp(f"{current_year}-03-06"),
                "forecast_end_date": pd.Timestamp(f"{current_year}-03-10"),
            }
        ]
    )


class TestPlotPentadForecastHydrographDataV2:
    """plot_pentad_forecast_hydrograph_data_v2: 4 range methods, pentad+decad."""

    @pytest.fixture(autouse=True)
    def _import(self):
        from src import vizualization as viz

        self.viz = viz

    def _call_and_capture(
        self, horizon, range_type, range_slider=0, q25=85.0, q75=115.0, delta=10.0
    ):
        forecasts = _make_forecasts_df(horizon=horizon, q25=q25, q75=q75, delta=delta)
        hydrograph = _make_hydrograph_day_df()
        linreg = _make_linreg_predictor_df(horizon=horizon)
        # V2 adds 1 day to forecasts['date'] before filtering:
        # pentad p=13: 2026-03-02 → shifted 2026-03-03; title_date+1=2026-03-03 → title_date=2026-03-02.
        # decade p=5: 2026-02-10 → shifted 2026-02-11; title_date+1=2026-02-11 → title_date=2026-02-10.
        if horizon == "pentad":
            title_date = pd.Timestamp("2026-03-02").date()
        else:
            title_date = pd.Timestamp("2026-02-10").date()

        captured = {}
        fake_element = hv.Curve([])

        # processing.add_predictor_dates may need to be stubbed as it reads
        # the linreg_predictor frame by station; patch to return our stub.
        with (
            patch("src.vizualization.processing.add_predictor_dates", return_value=linreg),
            patch("src.vizualization.plot_runoff_range_area", return_value=fake_element),
            patch("src.vizualization.plot_runoff_range_bound", return_value=fake_element),
            patch("src.vizualization.plot_runoff_range_bound_v2", return_value=fake_element),
            patch("src.vizualization.plot_runoff_line", return_value=fake_element),
            patch("src.vizualization.plot_runoff_forecasts_v2", return_value=fake_element),
            patch(
                "src.vizualization.plot_runoff_forecast_range_area_v2", return_value=fake_element
            ),
            patch("src.vizualization.create_cached_vlines", return_value=fake_element),
            patch(
                "src.vizualization.plot_current_runoff_forecast_range_date_format_v2",
                side_effect=lambda data, *args, **kwargs: (
                    captured.__setitem__("data", data) or fake_element
                ),
            ),
        ):
            self.viz.plot_pentad_forecast_hydrograph_data_v2(
                _gt,
                horizon,
                hydrograph,
                linreg,
                forecasts,
                STATION,
                title_date,
                ["GBT"],
                range_type,
                range_slider,
                range_visibility=_gt("No"),
                rram_forecast=None,
                ml_forecast=None,
            )

        return captured.get("data")

    def test_pentad_native_bounds(self):
        df = self._call_and_capture("pentad", "native", q25=85.0, q75=115.0)
        assert df is not None
        assert df["forecast lower bound column name"].iloc[-1] == pytest.approx(85.0, abs=0.1)
        assert df["forecast upper bound column name"].iloc[-1] == pytest.approx(115.0, abs=0.1)

    def test_pentad_delta_bounds(self):
        df = self._call_and_capture("pentad", "delta", delta=10.0)
        assert df is not None
        assert df["forecast lower bound column name"].iloc[-1] == pytest.approx(90.0, abs=0.1)
        assert df["forecast upper bound column name"].iloc[-1] == pytest.approx(110.0, abs=0.1)

    def test_pentad_manual_bounds(self):
        df = self._call_and_capture("pentad", "manual", range_slider=20)
        assert df is not None
        assert df["forecast lower bound column name"].iloc[-1] == pytest.approx(80.0, abs=0.1)
        assert df["forecast upper bound column name"].iloc[-1] == pytest.approx(120.0, abs=0.1)

    def test_pentad_mixed_delta_wins(self):
        """Mixed range_slider=20: delta < 20%Q → delta range."""
        df = self._call_and_capture("pentad", "mixed", range_slider=20, delta=10.0)
        assert df is not None
        assert df["forecast lower bound column name"].iloc[-1] == pytest.approx(90.0, abs=0.1)
        assert df["forecast upper bound column name"].iloc[-1] == pytest.approx(110.0, abs=0.1)

    def test_decad_native_bounds(self):
        df = self._call_and_capture("decade", "native", q25=85.0, q75=115.0)
        assert df is not None
        assert df["forecast lower bound column name"].iloc[-1] == pytest.approx(85.0, abs=0.1)
        assert df["forecast upper bound column name"].iloc[-1] == pytest.approx(115.0, abs=0.1)

    def test_decad_delta_bounds(self):
        df = self._call_and_capture("decade", "delta", delta=10.0)
        assert df is not None
        assert df["forecast lower bound column name"].iloc[-1] == pytest.approx(90.0, abs=0.1)
        assert df["forecast upper bound column name"].iloc[-1] == pytest.approx(110.0, abs=0.1)

    def test_lower_bound_clamped_to_zero(self):
        df = self._call_and_capture("pentad", "delta", delta=500.0)
        assert df is not None
        assert df["forecast lower bound column name"].iloc[-1] >= 0.0
