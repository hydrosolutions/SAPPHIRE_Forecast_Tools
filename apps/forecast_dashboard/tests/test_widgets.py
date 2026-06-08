"""Unit tests for forecast_dashboard/dashboard/widgets.py and PlotManager.

Covers create_horizon_selector() with ML forecasts enabled, disabled,
and the default (backwards-compatible) argument.

Also covers PlotManager.set_forecast_cards_visibility() card-hiding logic
without requiring a running Panel server.
"""

import sys
import os
import types

# Make the dashboard package importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dashboard import widgets
from dashboard.plot_manager import PlotManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_gettext(monkeypatch):
    """Monkeypatch dashboard.widgets._ to an identity function."""
    monkeypatch.setattr(widgets, "_", lambda s: s)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_create_horizon_selector_with_ml_enabled(monkeypatch):
    """All four horizons are present when ML forecasts are enabled."""
    _patch_gettext(monkeypatch)
    widget = widgets.create_horizon_selector(True)
    assert set(widget.options.values()) == {"pentad", "decade", "month", "season"}
    assert widget.value == "pentad"


def test_create_horizon_selector_with_ml_disabled(monkeypatch):
    """Only short-term horizons are present when ML forecasts are disabled."""
    _patch_gettext(monkeypatch)
    widget = widgets.create_horizon_selector(False)
    assert set(widget.options.values()) == {"pentad", "decade"}
    assert widget.value == "pentad"


def test_create_horizon_selector_default_matches_ml_enabled(monkeypatch):
    """Default call (no arg) exposes all four horizons — backwards compatible."""
    _patch_gettext(monkeypatch)
    widget = widgets.create_horizon_selector()
    assert set(widget.options.values()) == {"pentad", "decade", "month", "season"}
    assert widget.value == "pentad"


# ---------------------------------------------------------------------------
# PlotManager.set_forecast_cards_visibility — quarterly card hide/show logic
# ---------------------------------------------------------------------------

class _FakeCard:
    """Minimal stand-in for a Panel card with a visible attribute."""
    def __init__(self, visible=True):
        self.visible = visible


class _FakeWarning:
    """Minimal stand-in for forecast_warning pane."""
    def __init__(self):
        self.visible = True


def _make_stub_pm():
    """Return a PlotManager instance with its __init__ bypassed.

    Only the attributes accessed by set_forecast_cards_visibility are
    populated — avoids the Panel/server/API dependencies of the real __init__.
    """
    pm = object.__new__(PlotManager)
    # Attributes read by the four-card loop
    pm.linreg_card = _FakeCard()
    pm.hydrograph_card = _FakeCard()
    pm.skill_metrics_card = _FakeCard()
    pm.skill_table_card = _FakeCard()
    # Month-0 summary card (already handled correctly)
    pm.summary_table_m0_card = _FakeCard()
    # Quarterly summary card (the subject of the bug fix)
    pm.summary_table_q_card = _FakeCard()
    # Summary table card (sizing_mode / height branch)
    pm.summary_table_card = _FakeCard()
    pm.summary_table_card.sizing_mode = "stretch_both"
    pm.summary_table_card.height = None
    # WidgetManager stub — only forecast_warning and forecast_summary_table needed
    wm = types.SimpleNamespace(
        forecast_warning=_FakeWarning(),
        forecast_summary_table=types.SimpleNamespace(value=["row1", "row2"]),
    )
    pm._wm = wm
    return pm


class TestSetForecastCardsVisibility:
    """set_forecast_cards_visibility hides the quarterly card on non-month horizons."""

    def test_quarterly_card_hidden_when_not_is_month(self):
        """Switching away from month (is_month=False) must hide summary_table_q_card."""
        pm = _make_stub_pm()
        pm.summary_table_q_card.visible = True  # start visible (simulates month horizon)

        pm.set_forecast_cards_visibility(visible=False, is_month=False)

        assert pm.summary_table_q_card.visible is False, (
            "summary_table_q_card must be hidden when is_month=False"
        )

    def test_quarterly_card_not_force_hidden_when_is_month(self):
        """Calling with is_month=True must NOT force-hide the quarterly card.

        Visibility-on is the responsibility of update_quarterly_summary_tabulator().
        """
        pm = _make_stub_pm()
        pm.summary_table_q_card.visible = True  # pre-set visible

        pm.set_forecast_cards_visibility(visible=False, is_month=True)

        assert pm.summary_table_q_card.visible is True, (
            "set_forecast_cards_visibility must not hide summary_table_q_card "
            "when is_month=True"
        )

    def test_quarterly_card_absent_does_not_raise(self):
        """If summary_table_q_card is absent the method must not raise."""
        pm = _make_stub_pm()
        del pm.summary_table_q_card  # simulate old layout without the card

        # Should complete without AttributeError
        pm.set_forecast_cards_visibility(visible=False, is_month=False)


# ---------------------------------------------------------------------------
# TestGetPredictorsWarning — dynamic year column, no hardcoded "2026"
# ---------------------------------------------------------------------------

import datetime
import numpy as np
import pandas as pd


class _FixedDateTimeClass:
    """Stand-in for the datetime.datetime class, frozen at 2030-06-15."""

    @staticmethod
    def now():
        # Construct the return value via the real datetime.date to avoid
        # any circular reference with the monkeypatched attribute.
        return _FixedDateTimeClass._FIXED_DT

    _FIXED_DT = datetime.datetime(2030, 6, 15, 12, 0, 0)


class _FakeDtModule:
    """Minimal stand-in for the ``datetime`` module as used in widgets.py.

    widgets.py does ``import datetime as dt`` then calls
    ``dt.datetime.now().date()``.  Replacing ``widgets.dt`` with this object
    keeps the real ``datetime`` module untouched.
    """

    datetime = _FixedDateTimeClass


def _fake_station(label="15013 - Test"):
    return types.SimpleNamespace(value=label)


def _hydrograph_df(station_label, date_val, year_col, discharge_val):
    """Build a minimal hydrograph_day_all DataFrame."""
    return pd.DataFrame({
        "station_labels": [station_label],
        "date": [pd.to_datetime(date_val)],
        year_col: [discharge_val],
    })


class TestGetPredictorsWarning:
    """get_predictors_warning uses the current year dynamically (no hardcoded 2026)."""

    def _patch_date(self, monkeypatch):
        # Replace the entire ``dt`` name in widgets so dt.datetime.now()
        # returns 2030-06-15 without mutating the real datetime module.
        monkeypatch.setattr(widgets, "dt", _FakeDtModule)

    # ------------------------------------------------------------------
    # Regression: current-year column present and has a valid value → None
    # ------------------------------------------------------------------
    def test_current_year_value_present_returns_none(self, monkeypatch):
        """REGRESSION: with year_col='2030' having a value, no warning is issued.

        Before the fix this raises KeyError because it tries to read column '2026'.
        After the fix it reads '2030' and returns None.
        """
        self._patch_date(monkeypatch)
        station = _fake_station()
        df = _hydrograph_df(
            station_label=station.value,
            date_val="2030-06-15",
            year_col="2030",
            discharge_val=123.4,
        )
        data = {"hydrograph_day_all": df}
        result = widgets.get_predictors_warning(station, data)
        assert result is None, (
            "Expected no warning when today's discharge is present"
        )

    # ------------------------------------------------------------------
    # Current-year value is NaN → alert returned
    # ------------------------------------------------------------------
    def test_current_year_value_nan_returns_alert(self, monkeypatch):
        """NaN discharge for today in the correct year column → alert pane."""
        self._patch_date(monkeypatch)
        station = _fake_station()
        df = _hydrograph_df(
            station_label=station.value,
            date_val="2030-06-15",
            year_col="2030",
            discharge_val=np.nan,
        )
        data = {"hydrograph_day_all": df}
        result = widgets.get_predictors_warning(station, data)
        assert result is not None, (
            "Expected an alert pane when today's discharge is NaN"
        )

    # ------------------------------------------------------------------
    # No row for today → alert returned
    # ------------------------------------------------------------------
    def test_no_row_for_today_returns_alert(self, monkeypatch):
        """No matching row for today's date → alert pane."""
        self._patch_date(monkeypatch)
        station = _fake_station()
        df = _hydrograph_df(
            station_label=station.value,
            date_val="2030-06-14",  # yesterday, not today
            year_col="2030",
            discharge_val=99.0,
        )
        data = {"hydrograph_day_all": df}
        result = widgets.get_predictors_warning(station, data)
        assert result is not None, (
            "Expected an alert pane when there is no row for today"
        )

    # ------------------------------------------------------------------
    # year_col entirely absent from DataFrame → alert, no KeyError
    # ------------------------------------------------------------------
    def test_year_col_absent_returns_alert_not_keyerror(self, monkeypatch):
        """Missing year column (e.g. data not yet updated) → alert, not KeyError."""
        self._patch_date(monkeypatch)
        station = _fake_station()
        # Build a row for today but with a *different* year column
        df = _hydrograph_df(
            station_label=station.value,
            date_val="2030-06-15",
            year_col="2029",  # '2030' column is absent
            discharge_val=50.0,
        )
        data = {"hydrograph_day_all": df}
        result = widgets.get_predictors_warning(station, data)
        assert result is not None, (
            "Expected an alert pane when the year column is absent"
        )


