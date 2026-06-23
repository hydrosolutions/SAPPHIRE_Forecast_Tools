"""Unit tests for resolve_bulletin_header_date (bulletin_manager.py).

Tests the pure helper that selects which date's month/year should appear in
the bulletin Excel title.  Monthly forecasts are issued in the month before
their target month, so the issue date (last_date) can still fall in the
previous month.  The helper reads `valid_from` from the latest forecast row
to return the correct target month date.

bulletin_manager.py imports `panel as pn` which is not installed in the test
environment.  We mock the heavy dependencies at import time so that the pure
helper can be imported and tested in isolation.
"""

import sys
import types
from unittest.mock import MagicMock

import pandas as pd

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

    # Import the module under test — will raise AttributeError if the helper
    # function has not been added yet (i.e. before the fix).
    from dashboard import bulletin_manager  # noqa: E402

    resolve_bulletin_header_date = bulletin_manager.resolve_bulletin_header_date

finally:
    # Restore sys.modules: remove keys we injected; put back originals we saved.
    for _k in _FAKE_KEYS:
        if _k in _saved:
            sys.modules[_k] = _saved[_k]
        elif _k in sys.modules:
            del sys.modules[_k]
    del _saved, _FAKE_KEYS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_forecasts_all(issue_date_str, valid_from_str):
    """Build a minimal forecasts_all DataFrame with one row."""
    return pd.DataFrame(
        {
            "date": [pd.Timestamp(issue_date_str)],
            "valid_from": [pd.Timestamp(valid_from_str)],
            "month_in_year": [pd.Timestamp(valid_from_str).month],
        }
    )


def _make_site(code="19999", reservoir=True):
    site = MagicMock()
    site.code = code
    site.station_label = f"{code} - Test"
    site.basin_ru = "Test Basin"
    site.punkt_name_ru = "Тест вдхр" if reservoir else "Тест"
    return site


def _make_bulletin_record(code="19999"):
    return {
        "code": code,
        "model_type": "LR_Base",
        "forecasted_discharge": 100.0,
        "fc_lower": 90.0,
        "fc_upper": 110.0,
        "delta": 1.0,
        "sdivsigma": 2.0,
        "mae": 3.0,
        "accuracy": 90.0,
    }


def _make_long_forecast_record(code="19999", lead=0):
    return pd.DataFrame(
        {
            "code": [code],
            "date": [pd.Timestamp("2026-04-22")],
            "valid_from": [pd.Timestamp("2026-04-01")],
            "valid_to": [pd.Timestamp("2026-09-30")],
            "model_short": ["LR_Base"],
            "forecasted_discharge": [100.0],
            "Q25": [90.0],
            "Q75": [110.0],
            "season_in_year": [lead],
        }
    )


# ---------------------------------------------------------------------------
# Tests: month horizon — core bug scenario
# ---------------------------------------------------------------------------


class TestResolveBulletinHeaderDate:
    """Tests for resolve_bulletin_header_date."""

    # ── Main bug: May issue date, June forecast target ──────────────────────

    def test_month_horizon_returns_valid_from_month(self):
        """Bug scenario: last_date is in May, valid_from is June 1 → June returned."""
        forecasts_all = _make_forecasts_all("2026-05-25", "2026-06-01")
        last_date = pd.Timestamp("2026-05-26")

        result = resolve_bulletin_header_date("month", last_date, forecasts_all)

        assert result.month == 6, f"Expected June (6), got month {result.month}"
        assert result.year == 2026, f"Expected year 2026, got {result.year}"

    def test_month_horizon_returns_timestamp_with_correct_day(self):
        """valid_from day is preserved in the returned timestamp."""
        forecasts_all = _make_forecasts_all("2026-05-25", "2026-06-01")
        last_date = pd.Timestamp("2026-05-26")

        result = resolve_bulletin_header_date("month", last_date, forecasts_all)

        assert result.day == 1

    # ── Non-month horizons: passthrough ─────────────────────────────────────

    def test_pentad_horizon_returns_last_date_unchanged(self):
        forecasts_all = _make_forecasts_all("2026-05-25", "2026-06-01")
        last_date = pd.Timestamp("2026-05-26")

        result = resolve_bulletin_header_date("pentad", last_date, forecasts_all)

        assert result == last_date

    def test_decade_horizon_returns_last_date_unchanged(self):
        forecasts_all = _make_forecasts_all("2026-05-25", "2026-06-01")
        last_date = pd.Timestamp("2026-05-26")

        result = resolve_bulletin_header_date("decade", last_date, forecasts_all)

        assert result == last_date

    def test_season_horizon_returns_last_date_unchanged(self):
        forecasts_all = _make_forecasts_all("2026-05-25", "2026-06-01")
        last_date = pd.Timestamp("2026-05-26")

        result = resolve_bulletin_header_date("season", last_date, forecasts_all)

        assert result == last_date

    # ── Year boundary: Dec issue, Jan target ────────────────────────────────

    def test_month_horizon_year_boundary_returns_jan_next_year(self):
        """Issue date Dec 2026, valid_from Jan 1 2027 → January 2027 returned."""
        forecasts_all = _make_forecasts_all("2026-12-25", "2027-01-01")
        last_date = pd.Timestamp("2026-12-26")

        result = resolve_bulletin_header_date("month", last_date, forecasts_all)

        assert result.month == 1, f"Expected January (1), got month {result.month}"
        assert result.year == 2027, f"Expected year 2027, got {result.year}"

    # ── Edge: missing valid_from column ─────────────────────────────────────

    def test_month_horizon_missing_valid_from_column_falls_back_to_last_date(self):
        """When valid_from column is absent, fall back to last_date."""
        forecasts_all = pd.DataFrame({"date": [pd.Timestamp("2026-05-25")]})
        last_date = pd.Timestamp("2026-05-26")

        result = resolve_bulletin_header_date("month", last_date, forecasts_all)

        assert result == last_date

    def test_month_horizon_nat_valid_from_falls_back_to_last_date(self):
        """When valid_from is NaT, fall back to last_date."""
        forecasts_all = pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-05-25")],
                "valid_from": [pd.NaT],
            }
        )
        last_date = pd.Timestamp("2026-05-26")

        result = resolve_bulletin_header_date("month", last_date, forecasts_all)

        assert result == last_date

    def test_month_horizon_empty_dataframe_falls_back_to_last_date(self):
        """When forecasts_all is empty, fall back to last_date."""
        forecasts_all = pd.DataFrame(columns=["date", "valid_from"])
        last_date = pd.Timestamp("2026-05-26")

        result = resolve_bulletin_header_date("month", last_date, forecasts_all)

        assert result == last_date

    def test_month_horizon_none_dataframe_falls_back_to_last_date(self):
        """When forecasts_all is None, fall back to last_date."""
        last_date = pd.Timestamp("2026-05-26")

        result = resolve_bulletin_header_date("month", last_date, None)

        assert result == last_date

    # ── Multiple rows: latest row is used ───────────────────────────────────

    def test_month_horizon_uses_last_row_valid_from(self):
        """When forecasts_all has multiple rows, the last row's valid_from is used."""
        forecasts_all = pd.DataFrame(
            {
                "date": [
                    pd.Timestamp("2026-04-25"),
                    pd.Timestamp("2026-05-25"),
                ],
                "valid_from": [
                    pd.Timestamp("2026-05-01"),  # older row — May
                    pd.Timestamp("2026-06-01"),  # latest row — June
                ],
                "month_in_year": [5, 6],
            }
        )
        last_date = pd.Timestamp("2026-05-26")

        result = resolve_bulletin_header_date("month", last_date, forecasts_all)

        assert result.month == 6


class TestBulletinLongTermLeads:
    def test_month_load_uses_resolved_quarter_default(self, monkeypatch):
        site = _make_site()
        fake_db = MagicMock()
        fake_db._read_data.return_value = pd.DataFrame([_make_bulletin_record()])
        fake_db.get_long_forecasts_quarter.return_value = _make_long_forecast_record()
        monkeypatch.setattr(bulletin_manager, "db", fake_db)
        monkeypatch.setattr(
            bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock()
        )

        bulletin_manager._load_bulletin_from_api("month", 2026, 4, [site])

        fake_db.get_long_forecasts_quarter.assert_called_once_with(site.code)

    def test_season_load_passes_saved_issue_lead(self, monkeypatch):
        site = _make_site(reservoir=False)
        fake_db = MagicMock()
        fake_db._read_data.return_value = pd.DataFrame([_make_bulletin_record()])
        fake_db.get_long_forecasts_season.return_value = _make_long_forecast_record(
            lead=0
        )
        monkeypatch.setattr(bulletin_manager, "db", fake_db)
        monkeypatch.setattr(
            bulletin_manager, "hydrate_season_hydrograph_stats", MagicMock()
        )

        bulletin_manager._load_bulletin_from_api("season", 2026, 0, [site])

        fake_db.get_long_forecasts_season.assert_called_once_with(
            site.code, horizon_value=0
        )
