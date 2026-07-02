"""LOCKED acceptance test for milestone M1 — display-path routing.

M1 criterion: the bulletin/dashboard display path must route
currently-stored hydrograph values through the shared 3-significant-figure
helpers (``round_3sf`` / ``format_discharge``), so the displayed value matches
the 3sf contract. This is the PRE-PARITY routing check only — per-horizon
stored==displayed-under-new-parity is deferred to M2/M3.

The dashboard's discharge display formatter is
``src.bulletins.round_discharge_to_comma_separated_string`` (used for every
hydrograph value cell: act_q_this / act_q_last / act_norm / hydrograph_* /
last_year_q_pentad_mean, etc.). Under M1 its numeric content, once the locale
decimal-comma is normalised back to a plain dot, must equal round_3sf(value).

No real station codes or discharge values are used (fake code '19999').

These tests MUST fail while the formatter still uses the legacy banded rounding
(e.g. 2.565 -> '2,56', 99.95 -> '99,9') and pass only once it routes through
the shared 3sf helper. They must not be weakened.

Scope note: the non-finite / None negative path (blank cell) is a property of
the shared helper itself and is locked at the unit level in
``iEasyHydroForecast/tests/test_round_3sf_contract.py`` (``format_discharge`` /
``round_3sf`` return ''/None for NaN/Inf/None). It is deliberately NOT re-asserted
here: the legacy display formatter already special-cases NaN -> '' today, so a
display-layer NaN assertion would be green before any M1 code lands and would
lock nothing. This file locks only the routed-value behaviour that is red now.
"""

import sys
import types
from unittest.mock import MagicMock

import pytest

# round_3sf is the shared source of truth; importable via conftest sys.path.
from iEasyHydroForecast import forecast_library as fl

FAKE_STATION_CODE = "19999"

# ---------------------------------------------------------------------------
# Bootstrap: stub heavy dashboard deps, then freshly import src.bulletins.
# Mirrors test_bulletins_numeric_cells.py; restores sys.modules afterwards.
# ---------------------------------------------------------------------------
_FAKE_KEYS = [
    "panel",
    "panel.viewable",
    "panel.widgets",
    "panel.layout",
    "panel.pane",
    "panel.template",
    "src.gettext_config",
    "dashboard.logger",
    "src.db",
    "src.reports",
    "ieasyreports",
    "ieasyreports.settings",
    "ieasyreports.core",
    "ieasyreports.core.tags",
    "ieasyreports.core.tags.tag",
    "ieasyreports.core.report_generator",
    "tag_library",
    "src.bulletins",
    "src",
]

_saved = {k: sys.modules[k] for k in _FAKE_KEYS if k in sys.modules}

try:
    for _mod in [
        "panel",
        "panel.viewable",
        "panel.widgets",
        "panel.layout",
        "panel.pane",
        "panel.template",
        "tag_library",
    ]:
        if _mod not in sys.modules:
            sys.modules[_mod] = MagicMock()

    if "src.gettext_config" not in sys.modules:
        _gc = types.ModuleType("src.gettext_config")
        _gc._ = lambda x: x
        _gc.translation_manager = MagicMock()
        sys.modules["src.gettext_config"] = _gc

    if "dashboard.logger" not in sys.modules:
        _lg = types.ModuleType("dashboard.logger")
        _lg.setup_logger = MagicMock(return_value=MagicMock())
        sys.modules["dashboard.logger"] = _lg

    if "src.db" not in sys.modules:
        sys.modules["src.db"] = MagicMock()

    if "src.reports" not in sys.modules:
        sys.modules["src.reports"] = MagicMock()

    for _mod in [
        "ieasyreports",
        "ieasyreports.settings",
        "ieasyreports.core",
        "ieasyreports.core.tags",
        "ieasyreports.core.tags.tag",
    ]:
        if _mod not in sys.modules:
            sys.modules[_mod] = MagicMock()

    _rg_mod = types.ModuleType("ieasyreports.core.report_generator")

    class _StubDefaultReportGenerator:
        pass

    _rg_mod.DefaultReportGenerator = _StubDefaultReportGenerator
    sys.modules["ieasyreports.core.report_generator"] = _rg_mod

    for _clear in ("src.bulletins", "src"):
        sys.modules.pop(_clear, None)

    from src import bulletins

    display_discharge = bulletins.round_discharge_to_comma_separated_string

finally:
    for _k in _FAKE_KEYS:
        if _k in _saved:
            sys.modules[_k] = _saved[_k]
        elif _k in sys.modules:
            del sys.modules[_k]
    del _saved, _FAKE_KEYS


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

# Values < 1000 (no thousands-separator ambiguity) whose 3sf result differs
# from the legacy banded rounding, so this fails until routing is in place.
ROUTED_VALUES = [2.565, 99.95, 0.2368, 124.67, 24.67]


@pytest.mark.parametrize("value", ROUTED_VALUES)
def test_display_path_routes_through_round_3sf(value):
    """Displayed numeric content (comma normalised to dot) == round_3sf(value).

    Normalising ',' -> '.' tolerates the layered decimal-comma locale without
    pinning it, while still asserting the 3sf digits.
    """
    displayed = display_discharge(value)
    assert displayed not in (None, "")
    parsed = float(displayed.replace(",", "."))
    assert parsed == fl.round_3sf(value)
