"""Unit tests for forecast_dashboard/dashboard/widgets.py.

Covers create_horizon_selector() with ML forecasts enabled, disabled,
and the default (backwards-compatible) argument.
"""

import sys
import os

# Make the dashboard package importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dashboard import widgets


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
