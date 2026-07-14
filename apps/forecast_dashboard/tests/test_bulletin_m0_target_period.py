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
    _resolve_reload_month_target_period = bulletin_manager._resolve_reload_month_target_period
    _forecast_value_matches = bulletin_manager._forecast_value_matches
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

    def test_uses_selected_row_not_global_latest_valid_from(self):
        """FD-018 review #2: the resolver must resolve from the operator's
        OWN selected row (`site.forecasts`, set by `_on_add`/`_on_add_m0`
        before this is called), not from `sorted(valid_from)[-1]` over the
        whole undeduplicated `source_df`.

        Scenario: the station's wide history contains an OLDER issue-date
        row for the model the operator actually picked (LR, targeting
        July), AND a NEWER issue-date row for a DIFFERENT model the
        operator did NOT pick (GBT, targeting August). The naive
        "latest valid_from over the whole frame" resolution picks August;
        resolving from the operator's own selection must pick July.

        Discriminating mutation: dropping the model-narrowing step (i.e.
        reverting to `sorted(source_df["valid_from"])[-1]` over the
        unfiltered frame) makes this RED — result becomes (8, 2026).
        """
        site = _make_site(station_label="99001 - Test Site")
        # The operator selected the LR row in the tabulator.
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [100.0]}
        )
        df = pd.DataFrame(
            [
                {
                    "station_labels": "99001 - Test Site",
                    "model_short": "LR",
                    "date": pd.Timestamp("2026-06-30"),
                    "valid_from": pd.Timestamp("2026-07-01"),
                },
                {
                    "station_labels": "99001 - Test Site",
                    "model_short": "GBT",
                    "date": pd.Timestamp("2026-07-31"),
                    "valid_from": pd.Timestamp("2026-08-01"),
                },
            ]
        )

        result = _resolve_month_target_period(df, site)

        assert result == (7, 2026), (
            f"Expected the operator's own selected (LR, July) row, got {result}"
        )

    def test_falls_back_to_whole_frame_when_no_selected_forecasts(self):
        """Without `site.forecasts` (e.g. a legacy call site), the resolver
        must still work exactly as before — no new hard requirement on
        `site.forecasts` being present."""
        site = _make_site(station_label="99001 - Test Site")
        df = _forecast_frame("99001 - Test Site", "2026-07-01")

        result = _resolve_month_target_period(df, site)

        assert result == (7, 2026)


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

    def test_reload_honours_target_period_already_known_in_memory(self, monkeypatch):
        """Mid-session reload (e.g. flipping horizon tabs and back, on the
        SAME site objects) must trust an already-resolved
        `bulletin_target_period` rather than re-deriving it — this is
        `hasattr(site, "bulletin_target_period")` being True, a genuinely
        different case from the cold-reload / fresh-object case covered by
        `TestGenuineReloadRoundTrip` below.

        Discriminating mutation: dropping the per-site
        `getattr(site, "bulletin_target_period", None)` lookup (always
        passing None/using the passed-in forecast_horizon) makes the
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
        site.bulletin_target_period = (7, 2026)  # already resolved earlier this session

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

    def test_reload_two_sites_keep_different_months_already_in_memory(self, monkeypatch):
        """Mid-session counterpart of the round trip in
        `TestGenuineReloadRoundTrip` — both sites already carry a resolved
        `bulletin_target_period` (same-session case), so no re-derivation
        against `dm` is needed or attempted."""
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


# ---------------------------------------------------------------------------
# _resolve_reload_month_target_period / _forecast_value_matches
# (FD-018 review finding #1 — pure-function coverage)
# ---------------------------------------------------------------------------


class TestForecastValueMatches:
    def test_matches_on_station_model_and_discharge(self):
        df = _forecast_frame("99001 - Test Site", "2026-07-01", model="LR", discharge=50.0)
        result = _forecast_value_matches(df, "99001 - Test Site", "LR", 50.0)
        assert result == (7, 2026)

    def test_no_match_when_discharge_differs(self):
        df = _forecast_frame("99001 - Test Site", "2026-07-01", model="LR", discharge=50.0)
        assert _forecast_value_matches(df, "99001 - Test Site", "LR", 51.0) is None

    def test_no_match_when_model_differs(self):
        df = _forecast_frame("99001 - Test Site", "2026-07-01", model="LR", discharge=50.0)
        assert _forecast_value_matches(df, "99001 - Test Site", "GBT", 50.0) is None

    def test_none_for_empty_or_malformed_frame(self):
        assert _forecast_value_matches(pd.DataFrame(), "x", "LR", 1.0) is None
        assert _forecast_value_matches(None, "x", "LR", 1.0) is None


class TestResolveReloadMonthTargetPeriod:
    def test_no_m0_data_means_nothing_attempted(self):
        """Tajik-style deployments: `m0_df` is always empty (no month_0
        mode). Must not be attempted at all — contract used by `_on_write`
        to distinguish 'nothing to report' from 'resolution failed'."""
        site = _make_site(station_label="99001 - Test Site")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [50.0]}
        )
        period, attempted = _resolve_reload_month_target_period(
            site, pd.DataFrame(), pd.DataFrame()
        )
        assert (period, attempted) == (None, False)

    def test_confident_m0_only_match_resolves_to_m0_period(self):
        """The persisted discharge is found ONLY in the m0 frame — this is
        the exact case FD-018 needs to fix: a reloaded m0-card site must
        resolve to its own (July) period, not the bulletin-wide one."""
        site = _make_site(station_label="99001 - Test Site")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [50.0]}
        )
        main_df = _forecast_frame(
            "99001 - Test Site", "2026-08-01", model="LR", discharge=999.0
        )
        m0_df = _forecast_frame(
            "99001 - Test Site", "2026-07-01", model="LR", discharge=50.0
        )
        period, attempted = _resolve_reload_month_target_period(site, main_df, m0_df)
        assert (period, attempted) == ((7, 2026), True)

    def test_main_only_match_leaves_bulletin_wide_default(self):
        """The persisted discharge is found ONLY in the main frame — a
        plain main-panel site. Nothing to override; the caller's existing
        bulletin-wide default is already correct."""
        site = _make_site(station_label="99001 - Test Site")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [100.0]}
        )
        main_df = _forecast_frame(
            "99001 - Test Site", "2026-08-01", model="LR", discharge=100.0
        )
        m0_df = _forecast_frame(
            "99001 - Test Site", "2026-07-01", model="LR", discharge=999.0
        )
        period, attempted = _resolve_reload_month_target_period(site, main_df, m0_df)
        assert (period, attempted) == (None, False)

    def test_ambiguous_match_in_both_frames_reports_attempted_failure(self):
        """The persisted discharge happens to match BOTH frames — cannot
        confidently disambiguate. Must be reported as `attempted=True,
        period=None` (a genuine, surfaced failure), not silently swallowed
        as 'nothing to resolve'.

        Discriminating mutation: collapsing this branch into
        `(None, False)` (treating ambiguous the same as 'not needed') would
        make `_on_write`'s operator alert (see
        TestOnWriteSurfacesUnresolvedTargetPeriod) never fire for a
        genuinely unresolved site.
        """
        site = _make_site(station_label="99001 - Test Site")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [50.0]}
        )
        main_df = _forecast_frame(
            "99001 - Test Site", "2026-08-01", model="LR", discharge=50.0
        )
        m0_df = _forecast_frame(
            "99001 - Test Site", "2026-07-01", model="LR", discharge=50.0
        )
        period, attempted = _resolve_reload_month_target_period(site, main_df, m0_df)
        assert (period, attempted) == (None, True)

    def test_no_match_in_either_frame_means_nothing_attempted(self):
        """The underlying forecast has since changed in both frames (e.g. a
        new pipeline run) — there is genuinely nothing left to compare
        against, so this is 'not needed', not 'failed'."""
        site = _make_site(station_label="99001 - Test Site")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [12345.0]}
        )
        main_df = _forecast_frame(
            "99001 - Test Site", "2026-08-01", model="LR", discharge=100.0
        )
        m0_df = _forecast_frame(
            "99001 - Test Site", "2026-07-01", model="LR", discharge=50.0
        )
        period, attempted = _resolve_reload_month_target_period(site, main_df, m0_df)
        assert (period, attempted) == (None, False)


# ---------------------------------------------------------------------------
# Genuine save -> reload round trip (FD-018 review finding #1)
#
# The tests above (TestLoadBulletinFromApiHonoursPerSiteTargetPeriod) only
# ever exercise the "already resolved this session" branch by manually
# seeding `site.bulletin_target_period` before calling
# `_load_bulletin_from_api` — that never touches the actually-broken path,
# because a genuinely fresh `SapphireSite` (as `dm.sites_list` builds on
# every new session — see `DataManager.__init__` /
# `Site.get_site_attribues_from_iehhf_dataframe`) has no such attribute.
# These tests drive add -> save -> reload through BRAND NEW site objects,
# with `db._save_data` / `db._read_data` backed by a real (in-memory)
# upsert store instead of being replaced by a `MagicMock`.
# ---------------------------------------------------------------------------


class _FakeBulletinStore:
    """In-memory stand-in for the Bulletin API resource.

    Upserts on (horizon_type, year, horizon_value, code) — the same tuple
    as the real unique constraint
    (sapphire/services/postprocessing/app/models.py::Bulletin.__table_args__),
    confirming (per FD-018 review finding #3) that the API itself, not just
    the in-memory `bulletin_sites` list, allows only ONE row per station per
    bulletin: a main-panel add and an m0-card add for the SAME code cannot
    both persist.
    """

    def __init__(self):
        self.rows: list[dict] = []

    def save(self, _service, _resource, records):
        for r in records:
            self.rows = [
                x for x in self.rows
                if not (
                    x["horizon_type"] == r["horizon_type"]
                    and x["year"] == r["year"]
                    and x["horizon_value"] == r["horizon_value"]
                    and x["code"] == r["code"]
                )
            ]
            self.rows.append(dict(r))

    def read(self, _service, _resource, params):
        rows = [
            r for r in self.rows
            if r["horizon_type"] == params.get("horizon")
            and r["year"] == params.get("year")
            and r["horizon_value"] == params.get("horizon_value")
        ]
        return pd.DataFrame(rows)


class TestGenuineReloadRoundTrip:
    def test_m0_site_survives_reload_as_fresh_object(self, monkeypatch):
        """The heart of FD-018 review finding #1: add a station from the m0
        card, save it, then reload into a BRAND NEW `SapphireSite` object
        (as a fresh browser session / `BulletinManager.__init__` really
        does) — the July target month must survive, even though the
        bulletin-wide (main panel) period is August.

        This must be RED before the fix. Discriminating mutation: dropping
        the `_resolve_reload_month_target_period` lookup in
        `_load_bulletin_from_api` (i.e. always passing `target_period=None`
        for a site with no in-memory `bulletin_target_period`) makes the
        hydrate call receive month=8 instead of 7.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        store = _FakeBulletinStore()
        monkeypatch.setattr(bulletin_manager.db, "_save_data", store.save)
        monkeypatch.setattr(bulletin_manager.db, "_read_data", store.read)
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        station_label = "99001 - Test Site"
        main_df = _forecast_frame(station_label, "2026-08-01", model="LR", discharge=999.0)
        m0_df = _forecast_frame(station_label, "2026-07-01", model="LR", discharge=50.0)

        add_site = _make_site(code="99001", station_label=station_label)
        fake_add_self = _make_add_manager_stub(
            add_site, [add_site], main_df, m0_df,
            _tabulator_rows(model="LR", discharge=50.0, lower=40.0, upper=60.0),
        )
        BulletinManager._on_add_m0(fake_add_self, event=None)

        assert store.rows, "expected the m0 add to have saved a bulletin record"
        assert add_site.bulletin_target_period == (7, 2026)

        # --- Reload: brand new site object, never touched by _on_add_m0 ---
        fresh_site = _make_site(code="99001", station_label=station_label)
        assert not hasattr(fresh_site, "bulletin_target_period")

        dm_reload = types.SimpleNamespace(forecasts_all=main_df, long_forecasts_m0=m0_df)
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.append(month),
        )

        result = _load_bulletin_from_api("month", 2026, 8, [fresh_site], dm=dm_reload)

        assert len(result) == 1
        assert hydrate_calls == [7], (
            f"Expected July (7) — the m0 site's OWN target month — even "
            f"though the bulletin-wide (main panel) period is August; "
            f"got {hydrate_calls}"
        )
        assert fresh_site.bulletin_target_period == (7, 2026)

    def test_main_and_m0_sites_keep_different_months_after_fresh_reload(self, monkeypatch):
        """Two DIFFERENT stations in the same bulletin — one added from the
        main panel, one from the m0 card — must each keep their own target
        period after a cold reload into fresh site objects."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        store = _FakeBulletinStore()
        monkeypatch.setattr(bulletin_manager.db, "_save_data", store.save)
        monkeypatch.setattr(bulletin_manager.db, "_read_data", store.read)
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        main_label = "99001 - Main Site"
        m0_label = "99002 - M0 Site"

        main_df_for_main_site = _forecast_frame(main_label, "2026-08-01", model="LR", discharge=100.0)
        m0_df_for_main_site = _forecast_frame(main_label, "2026-07-01", model="LR", discharge=999.0)
        main_site = _make_site(code="99001", station_label=main_label)
        fake_add_main = _make_add_manager_stub(
            main_site, [main_site], main_df_for_main_site, m0_df_for_main_site,
            _tabulator_rows(model="LR", discharge=100.0, lower=90.0, upper=110.0),
        )
        BulletinManager._on_add(fake_add_main, event=None)

        main_df_for_m0_site = _forecast_frame(m0_label, "2026-08-01", model="LR", discharge=999.0)
        m0_df_for_m0_site = _forecast_frame(m0_label, "2026-07-01", model="LR", discharge=70.0)
        m0_site = _make_site(code="99002", station_label=m0_label)
        fake_add_m0 = _make_add_manager_stub(
            m0_site, [m0_site], main_df_for_m0_site, m0_df_for_m0_site,
            _tabulator_rows(model="LR", discharge=70.0, lower=60.0, upper=80.0),
        )
        BulletinManager._on_add_m0(fake_add_m0, event=None)

        assert {r["code"] for r in store.rows} == {"99001", "99002"}

        # --- Reload: fresh site objects, combined source frames (as the
        # real dm.forecasts_all / dm.long_forecasts_m0 would carry both
        # stations at once) ---
        fresh_main = _make_site(code="99001", station_label=main_label)
        fresh_m0 = _make_site(code="99002", station_label=m0_label)

        combined_main_df = pd.concat(
            [main_df_for_main_site, main_df_for_m0_site], ignore_index=True
        )
        combined_m0_df = pd.concat(
            [m0_df_for_main_site, m0_df_for_m0_site], ignore_index=True
        )
        dm_reload = types.SimpleNamespace(
            forecasts_all=combined_main_df, long_forecasts_m0=combined_m0_df
        )

        hydrate_calls = {}
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.setdefault(site.code, []).append(month),
        )

        _load_bulletin_from_api("month", 2026, 8, [fresh_main, fresh_m0], dm=dm_reload)

        assert hydrate_calls["99001"] == [8]
        assert hydrate_calls["99002"] == [7]

    def test_same_station_both_cards_collides_at_the_api_not_just_in_memory(self, monkeypatch):
        """FD-018 review finding #3, documented as a locked-down (not
        redesigned) fact: adding the SAME station from both cards does not
        just overwrite the in-memory `bulletin_sites` entry — the fake
        store's upsert key mirrors the real DB
        `UniqueConstraint("horizon_type", "year", "horizon_value", "code")`
        (models.py::Bulletin), so the second save silently replaces the
        first at the persistence layer too. There is exactly one row for
        this code after both adds; whichever was added SECOND wins.

        This is intentionally NOT a bug this fix redesigns — it pins the
        current (safe: no exception, no duplicate rows) behavior so a
        future fix for finding #3 has a locked contract to change
        deliberately.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        store = _FakeBulletinStore()
        monkeypatch.setattr(bulletin_manager.db, "_save_data", store.save)
        monkeypatch.setattr(bulletin_manager.db, "_read_data", store.read)
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        label = "99001 - Both Cards Site"
        main_df = _forecast_frame(label, "2026-08-01", model="LR", discharge=100.0)
        m0_df = _forecast_frame(label, "2026-07-01", model="LR", discharge=50.0)

        site = _make_site(code="99001", station_label=label)
        fake_add_main = _make_add_manager_stub(
            site, [site], main_df, m0_df,
            _tabulator_rows(model="LR", discharge=100.0, lower=90.0, upper=110.0),
        )
        BulletinManager._on_add(fake_add_main, event=None)
        assert len(store.rows) == 1

        fake_add_m0 = _make_add_manager_stub(
            site, [site], main_df, m0_df,
            _tabulator_rows(model="LR", discharge=50.0, lower=40.0, upper=60.0),
        )
        BulletinManager._on_add_m0(fake_add_m0, event=None)

        # Still exactly one row for this code — the m0 add replaced the
        # main-panel one, not appended alongside it.
        assert len(store.rows) == 1
        assert store.rows[0]["code"] == "99001"
        assert store.rows[0]["forecasted_discharge"] == 50.0  # the m0 (last) add won


# ---------------------------------------------------------------------------
# _on_write: a genuinely unresolved target period surfaces to the operator
# (FD-018 review finding #4)
# ---------------------------------------------------------------------------


class TestOnWriteSurfacesUnresolvedTargetPeriod:
    def test_unresolved_target_period_warns_operator_but_still_writes(self, monkeypatch):
        """A site whose `bulletin_target_period` is explicitly `None`
        (disambiguation was attempted and failed — see
        `_resolve_reload_month_target_period`'s `attempted` contract) must
        not produce a plain 'Bulletin saved successfully'. The write must
        still happen (never block), but the operator must be told this
        site's month could not be confirmed.

        Discriminating mutation: dropping the `unresolved_codes` tracking
        (i.e. always calling
        `_show_write_popup(_("Bulletin saved successfully"))`
        unconditionally) makes this RED — `alert_type` stays `"success"`.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_hydrograph_stats", MagicMock())
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        site = _make_site(code="99001")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [50.0],
             "Forecast lower bound": [40.0], "Forecast upper bound": [60.0]}
        )
        site.bulletin_target_period = None  # attempted and failed at reload

        fake_self = _make_write_manager_stub([site], bulletin_wide_month=8)

        BulletinManager._on_write(fake_self, event=None)

        fake_self._write_to_excel.assert_called_once()  # write still happened
        fake_self._show_write_popup.assert_called_once()
        args, kwargs = fake_self._show_write_popup.call_args
        assert kwargs.get("alert_type") == "warning"
        assert "99001" in args[0]

    def test_resolved_target_period_still_shows_plain_success(self, monkeypatch):
        """Control case: a site with a CONFIRMED target period must not
        trigger the warning path — only genuine resolution failures do."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_hydrograph_stats", MagicMock())
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        site = _make_site(code="99001")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [50.0],
             "Forecast lower bound": [40.0], "Forecast upper bound": [60.0]}
        )
        site.bulletin_target_period = (7, 2026)

        fake_self = _make_write_manager_stub([site], bulletin_wide_month=8)

        BulletinManager._on_write(fake_self, event=None)

        fake_self._show_write_popup.assert_called_once_with("Bulletin saved successfully")

    def test_no_attribute_at_all_does_not_warn(self, monkeypatch):
        """A site that never went through resolution this session at all
        (attribute absent, not `None`) must fall back silently — there is
        nothing to report, this is today's ordinary bulletin-wide path."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_hydrograph_stats", MagicMock())
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        site = _make_site(code="99001")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [50.0],
             "Forecast lower bound": [40.0], "Forecast upper bound": [60.0]}
        )
        # No bulletin_target_period attribute set at all.

        fake_self = _make_write_manager_stub([site], bulletin_wide_month=8)

        BulletinManager._on_write(fake_self, event=None)

        fake_self._show_write_popup.assert_called_once_with("Bulletin saved successfully")
