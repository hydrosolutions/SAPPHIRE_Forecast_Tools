"""Unit tests for FD-018: per-site bulletin target month/year.

Bug: an m0-card (lead-0, current-month) bulletin site is hydrated with the
CORRECT target month at add-time, but that hydration is silently overwritten
the next time the bulletin is written or reloaded — both `_on_write` and
`_load_bulletin_from_api` re-derive ONE bulletin-wide target month from the
MAIN panel (`dm.get_bulletin_metadata("month")`) and re-run
`_populate_forecast_attributes` over every site, m0 sites included. A July
(lead-0) forecast ends up carrying August's norm and August's day-count in
the Excel bulletin.

Fix: each bulletin site gets its own `(month, year)` target period, captured
from the site's OWN forecast data's `valid_from` at add-time
(`_resolve_month_target_period`, `_on_add` / `_on_add_m0`), stored as
`site.bulletin_target_period`, and honoured (not re-derived) by `_on_write`
and `_load_bulletin_from_api` via the new `_populate_forecast_attributes(...,
target_period=...)` parameter. All of this is gated on
`SAPPHIRE_SKILL_LEAD_AWARE` (default OFF) — flag-OFF must stay byte-identical
to trunk's bulletin-wide behavior.

bulletin_manager.py imports `panel as pn` which is not installed in the test
environment. We mock the heavy dependencies at import time so the module can
be imported and its callables tested in isolation. Mirrors the bootstrap in
test_bulletin_header_date.py / test_bulletin_edit_persistence.py.
"""

import sys
import types
from unittest.mock import MagicMock

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Bootstrap: mock heavy dashboard dependencies before importing the module.
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

_saved = {k: sys.modules[k] for k in _FAKE_KEYS if k in sys.modules}

try:
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

    from dashboard import bulletin_manager  # noqa: E402

    BulletinManager = bulletin_manager.BulletinManager
    _populate_forecast_attributes = bulletin_manager._populate_forecast_attributes
    _resolve_month_target_period = bulletin_manager._resolve_month_target_period
    _load_bulletin_from_api = bulletin_manager._load_bulletin_from_api

finally:
    for _k in _FAKE_KEYS:
        if _k in _saved:
            sys.modules[_k] = _saved[_k]
        elif _k in sys.modules:
            del sys.modules[_k]
    del _saved, _FAKE_KEYS

# src/site.py has no heavy dependencies, so it can be imported normally.
from src.site import SapphireSite  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_site(code="99001", station_label="99001 - Test Site", punkt_name_ru="Test Punkt"):
    site = SapphireSite(code=code)
    site.station_label = station_label
    site.punkt_name_ru = punkt_name_ru
    return site


def _forecast_frame(station_label, valid_from, model="LR", discharge=100.0):
    """A `get_long_forecasts()`-shaped frame for one station/model row."""
    return pd.DataFrame(
        [
            {
                "station_labels": station_label,
                "valid_from": pd.Timestamp(valid_from),
                "model_short": model,
                "forecasted_discharge": discharge,
            }
        ]
    )


def _tabulator_rows(model="LR", discharge=100.0, lower=90.0, upper=110.0):
    return pd.DataFrame(
        [
            {
                "Model": model,
                "Forecasted discharge": discharge,
                "Forecast lower bound": lower,
                "Forecast upper bound": upper,
                "δ": 1.0,
                "s/σ": 2.0,
                "MAE": 3.0,
                "Accuracy": 90.0,
            }
        ]
    )


def _make_add_manager_stub(site, sites_list, dm_forecasts_all, dm_long_forecasts_m0, tabulator_rows):
    """Fake `self` sufficient to call `_on_add` / `_on_add_m0` unbound."""
    tabulator = types.SimpleNamespace(value=tabulator_rows, selection=[])
    wm = types.SimpleNamespace(
        forecast_tabulator=tabulator,
        forecast_tabulator_m0=tabulator,
        station_selector=types.SimpleNamespace(value=site.station_label),
        horizon_selector=types.SimpleNamespace(value="month"),
    )
    dm = types.SimpleNamespace(
        sites_list=sites_list,
        forecasts_all=dm_forecasts_all,
        long_forecasts_m0=dm_long_forecasts_m0,
        get_bulletin_metadata=MagicMock(
            return_value=(
                pd.Timestamp("2026-08-01"),
                int(dm_forecasts_all["valid_from"].iloc[0].month),
                int(dm_forecasts_all["valid_from"].iloc[0].year),
            )
        ),
    )
    cfg = types.SimpleNamespace(viz=types.SimpleNamespace(app_state=types.SimpleNamespace(pipeline_running=False)))
    main_month = int(dm_forecasts_all["valid_from"].iloc[0].month)
    main_year = int(dm_forecasts_all["valid_from"].iloc[0].year)
    import calendar as _calendar

    days_in_month = _calendar.monthrange(main_year, main_month)[1]
    return types.SimpleNamespace(
        wm=wm,
        dm=dm,
        cfg=cfg,
        bulletin_sites=[],
        _horizon_context=lambda: ("month", 2026, main_month),
        _month_hydration_params=lambda: (main_month, main_year, days_in_month),
        _update_bulletin_table=MagicMock(),
        _show_popup=MagicMock(),
        _show_popup_m0=MagicMock(),
    )


def _make_write_popup_stub():
    return types.SimpleNamespace(object=None, alert_type=None, visible=False)


def _make_write_manager_stub(sites, bulletin_wide_month, bulletin_wide_year=2026):
    """Fake `self` sufficient to call `_on_write` unbound."""
    wm = types.SimpleNamespace(
        basin_selector=types.SimpleNamespace(value="All basins"),
        horizon_selector=types.SimpleNamespace(value="month"),
        write_bulletin_popup=_make_write_popup_stub(),
        downloader=types.SimpleNamespace(refresh_file_list=MagicMock()),
    )
    dm = types.SimpleNamespace(
        get_bulletin_metadata=MagicMock(
            return_value=(pd.Timestamp("2026-08-01"), bulletin_wide_month, bulletin_wide_year)
        ),
        forecasts_all=pd.DataFrame(),
        sites_list=sites,
    )
    cfg = types.SimpleNamespace(env_file_path="/tmp/env")
    processing = types.SimpleNamespace(get_bulletin_header_info=MagicMock(return_value={}))
    return types.SimpleNamespace(
        wm=wm,
        dm=dm,
        cfg=cfg,
        _processing=processing,
        bulletin_sites=sites,
        _write_to_excel=MagicMock(),
        _show_write_popup=MagicMock(),
    )


# ---------------------------------------------------------------------------
# _resolve_month_target_period — pure helper
# ---------------------------------------------------------------------------


class TestResolveMonthTargetPeriod:
    def test_resolves_month_and_year_from_valid_from(self):
        site = _make_site(station_label="99001 - Test Site")
        df = _forecast_frame("99001 - Test Site", "2026-07-01")

        result = _resolve_month_target_period(df, site)

        assert result == (7, 2026)

    def test_returns_none_for_empty_frame(self):
        site = _make_site()
        result = _resolve_month_target_period(pd.DataFrame(), site)
        assert result is None

    def test_returns_none_for_missing_valid_from_column(self):
        site = _make_site(station_label="99001 - Test Site")
        df = pd.DataFrame([{"station_labels": "99001 - Test Site"}])
        result = _resolve_month_target_period(df, site)
        assert result is None

    def test_returns_none_when_site_has_no_rows(self):
        site = _make_site(station_label="99001 - Test Site")
        df = _forecast_frame("99002 - Other Site", "2026-07-01")
        result = _resolve_month_target_period(df, site)
        assert result is None

    def test_returns_none_for_nat_valid_from(self):
        site = _make_site(station_label="99001 - Test Site")
        df = pd.DataFrame(
            [{"station_labels": "99001 - Test Site", "valid_from": pd.NaT}]
        )
        result = _resolve_month_target_period(df, site)  # must not raise
        assert result is None


# ---------------------------------------------------------------------------
# _populate_forecast_attributes: target_period override (additive param)
# ---------------------------------------------------------------------------


class TestPopulateForecastAttributesTargetPeriod:
    def test_target_period_overrides_bulletin_wide_args(self, monkeypatch):
        """The July norm/day-count must be used, not August's, when a
        target_period override is passed.

        Discriminating mutation: dropping `target_period` support (i.e.
        always using `(forecast_horizon, forecast_year)`) makes this RED —
        `hydrate_month_hydrograph_stats` would be called with month=8 and
        `forecast_v_min` would reflect August's day-count.
        """
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.append(month),
        )
        site = _make_site()
        site.forecasts = pd.DataFrame(
            {
                "Model": ["LR"],
                "Forecasted discharge": [100.0],
                "Forecast lower bound": [90.0],
                "Forecast upper bound": [110.0],
            }
        )

        # Bulletin-wide args say August (31 days); target_period override
        # says February 2028 (leap, 29 days) — a month-pair whose day counts
        # differ, so a day-count leak is directly observable.
        _populate_forecast_attributes(
            site, "month", forecast_year=2026, forecast_horizon=8,
            target_period=(2, 2028),
        )

        assert hydrate_calls == [2], f"Expected hydrate call for Feb, got {hydrate_calls}"
        # days_in_month(2028, 2) == 29 -> seconds = 29*86400
        expected_v_min = 90.0 * (29 * 86400) / 1_000_000
        assert site.forecast_v_min == pytest.approx(expected_v_min)

    def test_target_period_none_preserves_bulletin_wide_behavior(self, monkeypatch):
        """Omitting target_period (or passing None) must reproduce today's
        behavior exactly — this is the flag-OFF / legacy-caller contract."""
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.append(month),
        )
        site = _make_site()
        site.forecasts = pd.DataFrame(
            {
                "Model": ["LR"],
                "Forecasted discharge": [100.0],
                "Forecast lower bound": [90.0],
                "Forecast upper bound": [110.0],
            }
        )

        _populate_forecast_attributes(site, "month", forecast_year=2026, forecast_horizon=8)

        assert hydrate_calls == [8]
        expected_v_min = 90.0 * (31 * 86400) / 1_000_000  # August has 31 days
        assert site.forecast_v_min == pytest.approx(expected_v_min)


# ---------------------------------------------------------------------------
# _on_add / _on_add_m0: capture the site's own target period at add-time
# ---------------------------------------------------------------------------


class TestOnAddCapturesOwnTargetPeriod:
    def test_on_add_m0_captures_from_m0_frame_not_main(self, monkeypatch):
        """The m0 add path must resolve the target period from the site's
        OWN m0 frame, not the main panel's frame.

        Discriminating mutation: passing `self.dm.forecasts_all` instead of
        `self.dm.long_forecasts_m0` in `_on_add_m0` makes this RED — the
        captured period would be (8, 2026) instead of (7, 2026).
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        site = _make_site(station_label="99001 - Test Site")
        main_df = _forecast_frame("99001 - Test Site", "2026-08-01")
        m0_df = _forecast_frame("99001 - Test Site", "2026-07-01")
        fake_self = _make_add_manager_stub(site, [site], main_df, m0_df, _tabulator_rows())

        BulletinManager._on_add_m0(fake_self, event=None)

        assert site.bulletin_target_period == (7, 2026)

    def test_on_add_captures_from_main_frame_not_m0(self, monkeypatch):
        """Symmetric check: the main-panel add path must use its own frame,
        not the m0 frame.

        Discriminating mutation: swapping `self.dm.forecasts_all` for
        `self.dm.long_forecasts_m0` in `_on_add` makes this RED.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        site = _make_site(station_label="99001 - Test Site")
        main_df = _forecast_frame("99001 - Test Site", "2026-08-01")
        m0_df = _forecast_frame("99001 - Test Site", "2026-07-01")
        fake_self = _make_add_manager_stub(site, [site], main_df, m0_df, _tabulator_rows())

        BulletinManager._on_add(fake_self, event=None)

        assert site.bulletin_target_period == (8, 2026)

    def test_on_add_m0_flag_off_does_not_set_target_period(self, monkeypatch):
        """Flag-OFF kill switch: no new attribute is set at all."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "false")
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        site = _make_site(station_label="99001 - Test Site")
        main_df = _forecast_frame("99001 - Test Site", "2026-08-01")
        m0_df = _forecast_frame("99001 - Test Site", "2026-07-01")
        fake_self = _make_add_manager_stub(site, [site], main_df, m0_df, _tabulator_rows())

        BulletinManager._on_add_m0(fake_self, event=None)

        assert not hasattr(site, "bulletin_target_period")

    def test_on_add_m0_missing_valid_from_falls_back_without_raising(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        site = _make_site(station_label="99001 - Test Site")
        main_df = _forecast_frame("99001 - Test Site", "2026-08-01")
        empty_m0_df = pd.DataFrame()
        fake_self = _make_add_manager_stub(site, [site], main_df, empty_m0_df, _tabulator_rows())

        BulletinManager._on_add_m0(fake_self, event=None)  # must not raise

        assert site.bulletin_target_period is None


# ---------------------------------------------------------------------------
# _on_write: honours each site's own captured target period
# ---------------------------------------------------------------------------


class TestOnWriteHonoursPerSiteTargetPeriod:
    def test_m0_site_keeps_its_own_month_despite_main_panel_drift(self, monkeypatch):
        """The heart of FD-018: an m0 site's July norm/day-count must survive
        `_on_write` even though the bulletin-wide (main panel) period has
        since rolled over to August.

        This must be RED before the fix. Discriminating mutation: in
        `_on_write`, always pass `target_period=None` to
        `_populate_forecast_attributes` (i.e. drop the per-site lookup) —
        the hydrate call then receives month=8 instead of 7.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_hydrograph_stats", MagicMock())
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.append(month),
        )

        site = _make_site(code="99001")
        site.forecasts = pd.DataFrame(
            {
                "Model": ["LR"],
                "Forecasted discharge": [50.0],
                "Forecast lower bound": [40.0],
                "Forecast upper bound": [60.0],
            }
        )
        site.bulletin_target_period = (7, 2026)  # captured earlier via m0 add

        fake_self = _make_write_manager_stub([site], bulletin_wide_month=8)

        BulletinManager._on_write(fake_self, event=None)

        assert hydrate_calls == [7], f"Expected July (7), got {hydrate_calls}"
        # July has 31 days -> volume computed from 31, not August's 31 too —
        # use forecast_v_min/hydrograph_norm-independent proxy: assert the
        # hydrate call is unambiguous evidence of month, already checked above.

    def test_flag_off_ignores_captured_target_period(self, monkeypatch):
        """Kill-switch contract: with the flag OFF, _on_write must reproduce
        today's buggy-but-locked-in behavior — the bulletin-wide month wins
        even for a site carrying its own `bulletin_target_period`.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "false")
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_hydrograph_stats", MagicMock())
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.append(month),
        )

        site = _make_site(code="99001")
        site.forecasts = pd.DataFrame(
            {
                "Model": ["LR"],
                "Forecasted discharge": [50.0],
                "Forecast lower bound": [40.0],
                "Forecast upper bound": [60.0],
            }
        )
        site.bulletin_target_period = (7, 2026)

        fake_self = _make_write_manager_stub([site], bulletin_wide_month=8)

        BulletinManager._on_write(fake_self, event=None)

        assert hydrate_calls == [8], f"Expected bulletin-wide August (8), got {hydrate_calls}"

    def test_two_sites_keep_two_different_target_months(self, monkeypatch):
        """A bulletin with a main-panel site AND an m0 site must not clobber
        one site's target period with the other's — the heart of the bug.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_hydrograph_stats", MagicMock())
        hydrate_calls = {}
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.setdefault(site.code, []).append(month),
        )

        site_main = _make_site(code="99001")
        site_main.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [50.0],
             "Forecast lower bound": [40.0], "Forecast upper bound": [60.0]}
        )
        site_main.bulletin_target_period = (8, 2026)

        site_m0 = _make_site(code="99002")
        site_m0.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [70.0],
             "Forecast lower bound": [60.0], "Forecast upper bound": [80.0]}
        )
        site_m0.bulletin_target_period = (7, 2026)

        fake_self = _make_write_manager_stub([site_main, site_m0], bulletin_wide_month=8)

        BulletinManager._on_write(fake_self, event=None)

        assert hydrate_calls["99001"] == [8]
        assert hydrate_calls["99002"] == [7]

    def test_missing_target_period_falls_back_without_raising(self, monkeypatch):
        """A site with an unresolved (None) target_period must fall back to
        the bulletin-wide period, and _on_write must not raise."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_hydrograph_stats", MagicMock())
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.append(month),
        )

        site = _make_site(code="99001")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [50.0],
             "Forecast lower bound": [40.0], "Forecast upper bound": [60.0]}
        )
        site.bulletin_target_period = None  # resolution failed at add-time

        fake_self = _make_write_manager_stub([site], bulletin_wide_month=8)

        BulletinManager._on_write(fake_self, event=None)  # must not raise

        assert hydrate_calls == [8]


# ---------------------------------------------------------------------------
# _load_bulletin_from_api: honours each site's own captured target period
# ---------------------------------------------------------------------------


class TestLoadBulletinFromApiHonoursPerSiteTargetPeriod:
    def _api_rows_df(self, code, model="LR", discharge=50.0):
        return pd.DataFrame(
            [
                {
                    "code": code,
                    "model_type": model,
                    "forecasted_discharge": discharge,
                    "fc_lower": discharge - 10,
                    "fc_upper": discharge + 10,
                    "delta": 1.0,
                    "sdivsigma": 2.0,
                    "mae": 3.0,
                    "accuracy": 90.0,
                }
            ]
        )

    def test_reload_keeps_m0_site_own_month(self, monkeypatch):
        """A site reloaded via `_load_bulletin_from_api` must keep its own
        captured target period rather than the bulletin-wide (main panel)
        one passed into the function.

        This must be RED before the fix. Discriminating mutation: dropping
        the per-site `getattr(site, "bulletin_target_period", None)` lookup
        (always passing None/using the passed-in forecast_horizon) makes the
        hydrate call receive month=8 instead of 7.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.append(month),
        )
        site = _make_site(code="99001", station_label="99001 - Test Site")
        site.bulletin_target_period = (7, 2026)  # captured at earlier add-time

        monkeypatch.setattr(
            bulletin_manager.db, "_read_data", MagicMock(return_value=self._api_rows_df("99001"))
        )

        result = _load_bulletin_from_api("month", 2026, 8, [site])

        assert len(result) == 1
        assert hydrate_calls == [7], f"Expected July (7), got {hydrate_calls}"

    def test_reload_flag_off_uses_bulletin_wide_month(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "false")
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.append(month),
        )
        site = _make_site(code="99001", station_label="99001 - Test Site")
        site.bulletin_target_period = (7, 2026)

        monkeypatch.setattr(
            bulletin_manager.db, "_read_data", MagicMock(return_value=self._api_rows_df("99001"))
        )

        _load_bulletin_from_api("month", 2026, 8, [site])

        assert hydrate_calls == [8]

    def test_reload_two_sites_keep_different_months(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        hydrate_calls = {}
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.setdefault(site.code, []).append(month),
        )
        site_main = _make_site(code="99001", station_label="99001 - Main Site")
        site_main.bulletin_target_period = (8, 2026)
        site_m0 = _make_site(code="99002", station_label="99002 - M0 Site")
        site_m0.bulletin_target_period = (7, 2026)

        rows = pd.concat(
            [self._api_rows_df("99001"), self._api_rows_df("99002")], ignore_index=True
        )
        monkeypatch.setattr(bulletin_manager.db, "_read_data", MagicMock(return_value=rows))

        _load_bulletin_from_api("month", 2026, 8, [site_main, site_m0])

        assert hydrate_calls["99001"] == [8]
        assert hydrate_calls["99002"] == [7]

    def test_reload_missing_target_period_falls_back_without_raising(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.append(month),
        )
        site = _make_site(code="99001", station_label="99001 - Test Site")
        # No bulletin_target_period attribute at all (legacy/never-added-
        # this-session site).

        monkeypatch.setattr(
            bulletin_manager.db, "_read_data", MagicMock(return_value=self._api_rows_df("99001"))
        )

        result = _load_bulletin_from_api("month", 2026, 8, [site])  # must not raise

        assert len(result) == 1
        assert hydrate_calls == [8]
