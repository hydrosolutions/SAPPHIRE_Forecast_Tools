"""Unit tests for BulletinManager._month_hydration_params (bulletin_manager.py).

A monthly forecast issued in month N targets month N+1, so the climatological
norm and the number of days in the month must be resolved for the TARGET month
(the month-in-year of the latest forecast row), not the current calendar month.
This contract is exercised here in isolation by calling the helper unbound on a
lightweight fake ``self`` (avoids constructing the heavy BulletinManager).

bulletin_manager.py imports `panel as pn` which is not installed in the test
environment.  We mock the heavy dependencies at import time so that the class
can be imported and the helper tested in isolation. Mirrors the bootstrap in
test_bulletin_header_date.py.
"""

import datetime
import sys
import types
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Bootstrap: mock heavy dashboard dependencies before importing the module.
# The fakes are injected temporarily, the import is performed, then sys.modules
# is restored to its prior state so that other test modules collected later are
# not contaminated by our stubs.
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
]

# Save whatever is already present so we can restore it afterwards.
_saved = {k: sys.modules[k] for k in _FAKE_KEYS if k in sys.modules}

try:
    # Inject fakes only for keys that are not already real modules.
    for _mod in [
        "panel",
        "panel.viewable",
        "panel.widgets",
        "panel.layout",
        "panel.pane",
        "panel.template",
    ]:
        if _mod not in sys.modules:
            sys.modules[_mod] = MagicMock()

    if "src.gettext_config" not in sys.modules:
        _gc = types.ModuleType("src.gettext_config")
        _gc._ = lambda x: x  # no-op translation
        _gc.translation_manager = MagicMock()
        sys.modules["src.gettext_config"] = _gc

    if "dashboard.logger" not in sys.modules:
        _lg = types.ModuleType("dashboard.logger")
        _lg.setup_logger = MagicMock(return_value=MagicMock())
        sys.modules["dashboard.logger"] = _lg

    if "src.db" not in sys.modules:
        sys.modules["src.db"] = MagicMock()

    from dashboard.bulletin_manager import BulletinManager  # noqa: E402

finally:
    # Restore sys.modules: remove keys we injected; put back originals we saved.
    for _k in _FAKE_KEYS:
        if _k in _saved:
            sys.modules[_k] = _saved[_k]
        elif _k in sys.modules:
            del sys.modules[_k]
    del _saved, _FAKE_KEYS


class TestMonthHydrationParams:
    """_month_hydration_params resolves the TARGET month, not the current month."""

    def test_uses_target_month_from_bulletin_metadata(self):
        # get_bulletin_metadata("month") returns (last_date, target_month_in_year, target_year)
        fake = types.SimpleNamespace(
            dm=types.SimpleNamespace(
                get_bulletin_metadata=lambda horizon: (datetime.date(2026, 7, 1), 7, 2026)
            )
        )

        target_month, target_year, days_in_month = BulletinManager._month_hydration_params(fake)

        assert target_month == 7, f"Expected target month 7 (July), got {target_month}"
        assert target_year == 2026, f"Expected target year 2026, got {target_year}"
        assert days_in_month == 31, f"July has 31 days, got {days_in_month}"

    def test_days_in_month_reflects_target_february_leap(self):
        # February 2028 (leap) → 29 days; proves days come from the target, not 'now'.
        fake = types.SimpleNamespace(
            dm=types.SimpleNamespace(
                get_bulletin_metadata=lambda horizon: (datetime.date(2028, 2, 1), 2, 2028)
            )
        )

        target_month, target_year, days_in_month = BulletinManager._month_hydration_params(fake)

        assert (target_month, target_year, days_in_month) == (2, 2028, 29)
