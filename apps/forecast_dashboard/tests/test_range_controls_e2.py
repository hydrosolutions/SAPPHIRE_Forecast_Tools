"""PP-075 E2 — range-control contract tests.

Covers:
- calculate_forecast_range: slider not parsed for native/delta (None slider valid).
- calculate_forecast_range: manual/mixed with None/NaN/inf slider → both bounds NaN.
- calculate_forecast_range: manual/mixed with valid 0 slider preserved.
- create_range_widgets: four stable keys, default is delta, slider initially hidden.
- create_forecast_card: range_help pane present in returned card column.
"""

import pandas as pd
import panel as pn
import pytest
from src import processing

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def identity_gettext(s):
    return s


def _base_df(q=100.0, delta=10.0):
    return pd.DataFrame({"forecasted_discharge": [q], "delta": [delta]})


def _q_df(q25=90.0, q75=110.0, q=100.0):
    return pd.DataFrame({
        "forecasted_discharge": [q],
        "Q25": [q25],
        "Q75": [q75],
    })


# ---------------------------------------------------------------------------
# slider not parsed for native / delta — None is valid
# ---------------------------------------------------------------------------

class TestNativeDeltaNoneSlider:
    def test_native_none_slider_uses_quantiles(self):
        """Native with None slider must not raise; Q25/Q75 used for bounds."""
        df = _q_df()
        result = processing.calculate_forecast_range(
            identity_gettext, df, "native", None
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)

    def test_delta_none_slider_uses_delta(self):
        """Delta with None slider must not raise; delta column used for bounds."""
        df = _base_df(q=100.0, delta=5.0)
        result = processing.calculate_forecast_range(
            identity_gettext, df, "delta", None
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(95.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(105.0)

    def test_native_nan_slider_uses_quantiles(self):
        """Native with NaN slider must not raise; Q25/Q75 still used."""
        df = _q_df()
        result = processing.calculate_forecast_range(
            identity_gettext, df, "native", float("nan")
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(90.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(110.0)

    def test_delta_nan_slider_uses_delta(self):
        """Delta with NaN slider must not raise; delta column still used."""
        df = _base_df(q=100.0, delta=5.0)
        result = processing.calculate_forecast_range(
            identity_gettext, df, "delta", float("nan")
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(95.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(105.0)


# ---------------------------------------------------------------------------
# manual / mixed with invalid slider → both bounds NaN
# ---------------------------------------------------------------------------

class TestManualMixedInvalidSlider:
    @pytest.mark.parametrize("method", ["manual", "Manual range, select value below"])
    def test_manual_none_slider_produces_nan(self, method):
        df = pd.DataFrame({"forecasted_discharge": [100.0]})
        result = processing.calculate_forecast_range(
            identity_gettext, df, method, None
        )
        assert result["fc_lower"].isna().all()
        assert result["fc_upper"].isna().all()

    @pytest.mark.parametrize("method", ["mixed", "min[delta, %]"])
    def test_mixed_none_slider_produces_nan(self, method):
        df = _base_df()
        result = processing.calculate_forecast_range(
            identity_gettext, df, method, None
        )
        assert result["fc_lower"].isna().all()
        assert result["fc_upper"].isna().all()

    def test_manual_nan_slider_produces_nan(self):
        df = pd.DataFrame({"forecasted_discharge": [100.0]})
        result = processing.calculate_forecast_range(
            identity_gettext, df, "manual", float("nan")
        )
        assert result["fc_lower"].isna().all()
        assert result["fc_upper"].isna().all()

    def test_manual_inf_slider_produces_nan(self):
        df = pd.DataFrame({"forecasted_discharge": [100.0]})
        result = processing.calculate_forecast_range(
            identity_gettext, df, "manual", float("inf")
        )
        assert result["fc_lower"].isna().all()
        assert result["fc_upper"].isna().all()

    def test_mixed_inf_slider_produces_nan(self):
        df = _base_df()
        result = processing.calculate_forecast_range(
            identity_gettext, df, "mixed", float("inf")
        )
        assert result["fc_lower"].isna().all()
        assert result["fc_upper"].isna().all()


# ---------------------------------------------------------------------------
# valid 0 slider preserved for manual / mixed
# ---------------------------------------------------------------------------

class TestZeroSliderPreserved:
    def test_manual_zero_slider_produces_zero_width_range(self):
        """Zero slider → lower == upper == Q (zero-width, not NaN)."""
        df = pd.DataFrame({"forecasted_discharge": [100.0]})
        result = processing.calculate_forecast_range(
            identity_gettext, df, "manual", 0
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(100.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(100.0)

    def test_mixed_zero_slider_uses_min_delta_zero(self):
        """Zero slider → halfwidth = min(delta, 0) = 0, bounds equal Q."""
        df = _base_df(q=100.0, delta=5.0)
        result = processing.calculate_forecast_range(
            identity_gettext, df, "mixed", 0
        )
        assert result["fc_lower"].iloc[0] == pytest.approx(100.0)
        assert result["fc_upper"].iloc[0] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# create_range_widgets — stable keys, default, slider visibility
# ---------------------------------------------------------------------------

class TestCreateRangeWidgets:
    def test_four_stable_keys(self):
        from dashboard.widgets import create_range_widgets
        sel, slider, radio = create_range_widgets()
        values = set(sel.options.values())
        assert values == {"delta", "native", "manual", "mixed"}

    def test_default_is_delta(self):
        from dashboard.widgets import create_range_widgets
        sel, slider, radio = create_range_widgets()
        assert sel.value == "delta"

    def test_slider_initially_hidden(self):
        from dashboard.widgets import create_range_widgets
        sel, slider, radio = create_range_widgets()
        assert slider.visible is False


# ---------------------------------------------------------------------------
# create_forecast_card — range_help pane present
# ---------------------------------------------------------------------------

class TestCreateForecastCard:
    def test_range_help_pane_in_card(self):
        """The range_help Markdown pane must be present inside the card column."""
        from dashboard.widgets import create_forecast_card, create_range_widgets

        sel, slider, radio = create_range_widgets()
        btn = pn.widgets.Button(name="Apply")
        cb = pn.widgets.CheckBoxGroup(name="Models", options={})
        card, model_sec, range_vis_sec, ref_pane = create_forecast_card(
            sel, slider, cb, radio, btn, width=300
        )
        # Walk the card's contents looking for a Markdown pane with "daily"
        inner_col = card[0]
        help_texts = [
            obj.object for obj in inner_col
            if isinstance(obj, pn.pane.Markdown) and "daily" in (obj.object or "")
        ]
        assert help_texts, "No range_help Markdown pane found in forecast_card column"
