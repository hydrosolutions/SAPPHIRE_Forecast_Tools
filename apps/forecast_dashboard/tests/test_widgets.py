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
