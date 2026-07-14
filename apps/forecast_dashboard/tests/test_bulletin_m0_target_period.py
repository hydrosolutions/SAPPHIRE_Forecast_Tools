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
via the new `_populate_forecast_attributes(..., target_period=...)`
parameter. All of this is gated on `SAPPHIRE_SKILL_LEAD_AWARE` (default OFF)
— flag-OFF must stay byte-identical to trunk's bulletin-wide behavior.

FD-018 review #3 (owner decision, see `doc/plans/issues/mid_prio_gi_draft_
pp_bulletin_target_period_field.md`, PP-040): an earlier draft also
had `_load_bulletin_from_api` try to GUESS a reloaded site's target period
(`_resolve_reload_month_target_period`, matching persisted `(model,
discharge)` against the main/m0 frames). That heuristic was proven worse
than trunk — it could confidently resolve to the WRONG frame, and a
malformed row could raise and silently discard the whole saved bulletin —
and has been DELETED. Reload is now intentionally IDENTICAL to trunk (always
the bulletin-wide period, for both flag states) until PP-040 (a `Bulletin`
schema field) lands. `_load_bulletin_from_api` additionally guards against a
site-object-reuse hazard: `site` objects come from `dm.sites_list`, which
`DataManager.load_station` reuses across station/horizon/date switches, so a
`bulletin_target_period` cached by an earlier in-session add must be cleared
on reload rather than silently ignored (ignoring alone would still let it
leak into a later `_on_write` for the same site object).

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


def _make_add_manager_stub(
    site, sites_list, dm_forecasts_all, dm_long_forecasts_m0, tabulator_rows,
    date_picker_value=None,
):
    """Fake `self` sufficient to call `_on_add` / `_on_add_m0` unbound."""
    if date_picker_value is None:
        date_picker_value = pd.Timestamp("2099-01-01")
    tabulator = types.SimpleNamespace(value=tabulator_rows, selection=[])
    wm = types.SimpleNamespace(
        forecast_tabulator=tabulator,
        forecast_tabulator_m0=tabulator,
        station_selector=types.SimpleNamespace(value=site.station_label),
        horizon_selector=types.SimpleNamespace(value="month"),
        # FD-018 review #5: `_on_add` reads this to bound the main-panel
        # target-period resolution the same way `wm.date_picker` bounds
        # `create_forecast_summary_tabulator`. None of the fixtures in this
        # module give `dm_forecasts_all` a `date` column, so this value is
        # inert for all EXISTING tests (the bound-filter step is skipped
        # whenever there is no `date` column to bound) — it only matters for
        # the new date-picker-bound tests below, which override it.
        date_picker=types.SimpleNamespace(value=date_picker_value),
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
# _resolve_month_target_period — FD-018 review #5: the surviving defect.
# The resolver must reproduce the SAME date-picker bound
# `create_forecast_summary_table` (vizualization.py ~:3037-3047) applies
# before taking the max-date row, not just narrow by model over the whole
# (undeduplicated) frame.
# ---------------------------------------------------------------------------


class TestResolveMonthTargetPeriodDatePickerBound:
    def test_date_picker_bound_excludes_later_same_model_row(self):
        """The operator wound the date picker BACK to an earlier issue date
        and (at that setting) picked the model's row targeting July. The
        SAME model also has a LATER row elsewhere in the wide frame
        targeting August — the row the operator did NOT see because the
        date picker excluded it at the time. The resolver must honour the
        date-picker bound and resolve July, not the globally-latest row for
        that model.

        This is the exact scenario named in the FD-018 review: model-only
        narrowing (already fixed) is not sufficient when the SAME model has
        multiple issue dates and the operator's bound excludes the later
        one.

        Discriminating mutation: dropping the `date_bound` filtering block
        in `_resolve_month_target_period` (i.e. going back to taking the
        max date over the whole model-narrowed subset, unbounded) makes
        this RED — result becomes (8, 2026).
        """
        site = _make_site(station_label="99001 - Test Site")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [50.0]}
        )
        df = pd.DataFrame(
            [
                {
                    "station_labels": "99001 - Test Site",
                    "model_short": "LR",
                    "date": pd.Timestamp("2026-07-31"),
                    "valid_from": pd.Timestamp("2026-08-01"),
                },
                {
                    "station_labels": "99001 - Test Site",
                    "model_short": "LR",
                    "date": pd.Timestamp("2026-06-30"),
                    "valid_from": pd.Timestamp("2026-07-01"),
                },
            ]
        )

        # Operator wound the date picker back to 2026-06-30.
        result = _resolve_month_target_period(df, site, pd.Timestamp("2026-06-30"))

        assert result == (7, 2026), (
            f"Expected the operator's own (bounded) July selection, got {result}"
        )

    def test_missing_model_column_returns_none_not_whole_frame(self):
        """FD-018 review #5 (kill the silent fallback): if `site.forecasts`
        is present but lacks the localized `Model` column (e.g. renamed or
        misconfigured upstream), the resolver must NOT silently fall back
        to resolving from the whole (model-un-narrowed) station frame —
        that is exactly the "wrong model/row silently wins" hazard this
        function exists to prevent. It must return `None` so the caller
        falls back to the bulletin-wide period and the operator sees the
        existing "could not be confirmed" warning (`_on_add` / `_on_add_m0`
        / `_on_write`).

        Discriminating mutation: relaxing the `model_col not in
        selected_forecasts.columns` guard to instead skip narrowing and
        keep resolving from the un-narrowed `site_rows` (the pre-fix
        shape) makes this RED — the result becomes (8, 2026), the globally
        -latest row's month, instead of `None`.
        """
        site = _make_site(station_label="99001 - Test Site")
        # Present and non-empty, but the wrong column name.
        site.forecasts = pd.DataFrame(
            {"ModelName": ["LR"], "Forecasted discharge": [50.0]}
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

        result = _resolve_month_target_period(df, site, pd.Timestamp("2026-07-31"))

        assert result is None

    def test_ordinary_latest_row_unaffected_by_bound_fix(self):
        """Control: the ordinary case — one model, one row, no date-picker
        games — must resolve exactly as before. The bound fix must not
        change behavior when there is nothing to exclude."""
        site = _make_site(station_label="99001 - Test Site")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR"], "Forecasted discharge": [100.0]}
        )
        df = pd.DataFrame(
            [
                {
                    "station_labels": "99001 - Test Site",
                    "model_short": "LR",
                    "date": pd.Timestamp("2026-07-31"),
                    "valid_from": pd.Timestamp("2026-08-01"),
                },
            ]
        )

        # date_bound == the same (latest) issue date, as it would be when
        # the operator has NOT touched the date picker.
        result = _resolve_month_target_period(df, site, pd.Timestamp("2026-07-31"))

        assert result == (8, 2026)

    def test_multiple_selected_models_disagreeing_on_target_period_is_unresolved(self):
        """If the operator's selection somehow spans models that resolve to
        DIFFERENT target periods within the same bounded/max-dated batch,
        the resolver must not silently pick one — it returns `None`
        (falling back to the bulletin-wide period with the existing
        warning). This is a defensive branch: structurally, one issue-date
        batch is produced by a single monthly forecast run and every model
        in it targets the same calendar month, so this should not be
        reachable via the tabulator itself (`create_forecast_summary_table`
        already collapses to one `date == max(date)` batch before the
        operator ever sees a row) — reaching it indicates a data anomaly,
        not normal two-model selection.
        """
        site = _make_site(station_label="99001 - Test Site")
        site.forecasts = pd.DataFrame(
            {"Model": ["LR", "GBT"], "Forecasted discharge": [50.0, 55.0]}
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
                    "date": pd.Timestamp("2026-06-30"),
                    "valid_from": pd.Timestamp("2026-08-01"),  # anomalous
                },
            ]
        )

        result = _resolve_month_target_period(df, site, pd.Timestamp("2026-06-30"))

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

    def test_on_add_date_picker_wound_back_excludes_later_same_model_row(self, monkeypatch):
        """End-to-end version of the FD-018 review #5 defect: the operator
        winds `wm.date_picker` back to an earlier issue date and — at that
        setting — the tabulator shows (and the operator selects) the row
        targeting July. `dm.forecasts_all` (the wide, undeduplicated
        history) also carries a LATER row for the SAME model targeting
        August, which the operator never saw at that date-picker setting.

        `_on_add` must capture July, and a subsequent `_on_write` must
        hydrate with July's month, not August's.

        This was RED before the fix: `_on_add` called
        `_resolve_month_target_period(self.dm.forecasts_all, selected_site)`
        with no date-picker bound at all, so the model-narrowed subset's
        globally-latest row (August) always won regardless of the date
        picker.

        Discriminating mutation: drop the `self.wm.date_picker.value`
        argument from `_on_add`'s call to `_resolve_month_target_period`
        (reverting to the 2-arg call) — `bulletin_target_period` becomes
        (8, 2026) and the write's hydrate call receives month=8 instead of
        7.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        site = _make_site(station_label="99001 - Test Site")
        main_df = pd.DataFrame(
            [
                {  # the LATER row for the SAME model — must NOT win.
                    "station_labels": "99001 - Test Site",
                    "model_short": "LR",
                    "date": pd.Timestamp("2026-07-31"),
                    "valid_from": pd.Timestamp("2026-08-01"),
                    "forecasted_discharge": 100.0,
                },
                {  # the row the operator actually saw (date picker wound
                   # back) and picked.
                    "station_labels": "99001 - Test Site",
                    "model_short": "LR",
                    "date": pd.Timestamp("2026-06-30"),
                    "valid_from": pd.Timestamp("2026-07-01"),
                    "forecasted_discharge": 50.0,
                },
            ]
        )
        m0_df = pd.DataFrame()  # unused by _on_add

        fake_self = _make_add_manager_stub(
            site, [site], main_df, m0_df,
            _tabulator_rows(model="LR", discharge=50.0, lower=40.0, upper=60.0),
            date_picker_value=pd.Timestamp("2026-06-30"),  # wound back
        )

        BulletinManager._on_add(fake_self, event=None)

        assert site.bulletin_target_period == (7, 2026), (
            f"Expected the operator's own July selection, got "
            f"{site.bulletin_target_period}"
        )

        # And the write itself must use July's hydration, not the
        # bulletin-wide (August) period.
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda s, month, db: hydrate_calls.append(month),
        )
        write_self = _make_write_manager_stub([site], bulletin_wide_month=8)
        BulletinManager._on_write(write_self, event=None)

        assert hydrate_calls == [7], f"Expected July (7), got {hydrate_calls}"


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
# _load_bulletin_from_api: reload matches trunk, ignores/clears any cached
# per-site target period (the reload heuristic is DELETED — see module
# docstring and PP-040)
# ---------------------------------------------------------------------------


class TestLoadBulletinFromApiMatchesTrunk:
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

    @pytest.mark.parametrize("flag", ["true", "false"])
    def test_reload_ignores_and_clears_cached_target_period(self, monkeypatch, flag):
        """Reload must be identical to trunk regardless of the flag: a
        `bulletin_target_period` left on the site object (as if cached by an
        earlier in-session add) must be neither honoured NOR merely
        skipped — it must be actively cleared, so it cannot leak into a
        subsequent `_on_write` call for this same (reused) site object.

        Discriminating mutation: reverting to the deleted heuristic's
        `hasattr(site, "bulletin_target_period"): target_period =
        site.bulletin_target_period` mid-session branch makes this RED
        under flag="true" — hydrate would receive month=7 instead of 8, and
        the attribute would still be present afterwards.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", flag)
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.append(month),
        )
        site = _make_site(code="99001", station_label="99001 - Test Site")
        site.bulletin_target_period = (7, 2026)  # stale/cached from an earlier add

        monkeypatch.setattr(
            bulletin_manager.db, "_read_data", MagicMock(return_value=self._api_rows_df("99001"))
        )

        result = _load_bulletin_from_api("month", 2026, 8, [site])

        assert len(result) == 1
        assert hydrate_calls == [8], f"Expected bulletin-wide August (8), got {hydrate_calls}"
        assert not hasattr(site, "bulletin_target_period"), (
            "a cached target period must be cleared on reload, not just ignored"
        )

    def test_reload_two_sites_both_use_bulletin_wide_month_regardless_of_cache(self, monkeypatch):
        """Two sites, each carrying a DIFFERENT stale cached target period
        (as if one came from an earlier m0 add and the other from a main
        add) — reload must give BOTH the same bulletin-wide month, never
        their individually cached ones."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        hydrate_calls = {}
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.setdefault(site.code, []).append(month),
        )
        site_a = _make_site(code="99001", station_label="99001 - Site A")
        site_a.bulletin_target_period = (8, 2026)
        site_b = _make_site(code="99002", station_label="99002 - Site B")
        site_b.bulletin_target_period = (7, 2026)

        rows = pd.concat(
            [self._api_rows_df("99001"), self._api_rows_df("99002")], ignore_index=True
        )
        monkeypatch.setattr(bulletin_manager.db, "_read_data", MagicMock(return_value=rows))

        _load_bulletin_from_api("month", 2026, 8, [site_a, site_b])

        assert hydrate_calls["99001"] == [8]
        assert hydrate_calls["99002"] == [8]
        assert not hasattr(site_a, "bulletin_target_period")
        assert not hasattr(site_b, "bulletin_target_period")

    def test_reload_missing_target_period_falls_back_without_raising(self, monkeypatch):
        """A site with no `bulletin_target_period` attribute at all (the
        ordinary case — see `DataManager.__init__` /
        `Site.get_site_attribues_from_iehhf_dataframe`) must not raise and
        must use the bulletin-wide period, exactly like trunk."""
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda site, month, db: hydrate_calls.append(month),
        )
        site = _make_site(code="99001", station_label="99001 - Test Site")

        monkeypatch.setattr(
            bulletin_manager.db, "_read_data", MagicMock(return_value=self._api_rows_df("99001"))
        )

        result = _load_bulletin_from_api("month", 2026, 8, [site])  # must not raise

        assert len(result) == 1
        assert hydrate_calls == [8]


# ---------------------------------------------------------------------------
# Stale-cache regression (FD-018 review finding #2): `dm.sites_list` reuses
# site objects across station/horizon/date switches
# (`DataManager.load_station` replaces `_data`, not `_sites_list`). An m0 add
# earlier in the session must never leak its cached target period into a
# LATER reload of a different (e.g. main-panel) bulletin for the SAME
# station code.
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


class TestReloadClearsStaleCachedTargetPeriod:
    def test_m0_add_then_later_main_reload_same_code_uses_main_month(self, monkeypatch):
        """The exact hazard named in the FD-018 review: add station 99001
        from the m0 card (July), then — WITHOUT the site object being
        rebuilt, since `dm.sites_list` persists across station/horizon
        switches — reload a MAIN-panel bulletin for the SAME code that now
        targets August. August's norm/day-count must be used, not July's.

        Discriminating mutation: removing the
        `if hasattr(site, "bulletin_target_period"): del
        site.bulletin_target_period` clear in `_load_bulletin_from_api`
        makes this RED — the hydrate call would receive month=7 (July)
        instead of 8 (August), and 'Bulletin saved successfully' would
        silently carry August's discharge with July's norm.
        """
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        store = _FakeBulletinStore()
        monkeypatch.setattr(bulletin_manager.db, "_save_data", store.save)
        monkeypatch.setattr(bulletin_manager.db, "_read_data", store.read)
        monkeypatch.setattr(bulletin_manager, "hydrate_month_hydrograph_stats", MagicMock())

        station_label = "99001 - Test Site"
        m0_df = _forecast_frame(station_label, "2026-07-01", model="LR", discharge=50.0)
        main_df_at_add_time = _forecast_frame(
            station_label, "2026-07-01", model="LR", discharge=999.0
        )

        site = _make_site(code="99001", station_label=station_label)
        fake_add_self = _make_add_manager_stub(
            site, [site], main_df_at_add_time, m0_df,
            _tabulator_rows(model="LR", discharge=50.0, lower=40.0, upper=60.0),
        )
        BulletinManager._on_add_m0(fake_add_self, event=None)

        assert site.bulletin_target_period == (7, 2026)
        assert store.rows and store.rows[0]["code"] == "99001"

        # --- Simulate: operator switches station/horizon (dm.sites_list
        # reuses this SAME `site` object — see DataManager.load_station),
        # then comes back to a MAIN-panel bulletin for this station that has
        # since rolled over to August. ---
        store.rows = [
            {
                "horizon_type": "month", "year": 2026, "horizon_value": 8,
                "code": "99001", "model_type": "LR",
                "forecasted_discharge": 100.0, "fc_lower": 90.0, "fc_upper": 110.0,
                "delta": 1.0, "sdivsigma": 2.0, "mae": 3.0, "accuracy": 90.0,
            }
        ]

        hydrate_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "hydrate_month_hydrograph_stats",
            lambda s, month, db: hydrate_calls.append(month),
        )

        result = _load_bulletin_from_api("month", 2026, 8, [site])

        assert len(result) == 1
        assert hydrate_calls == [8], (
            f"Expected August (8) for the reloaded MAIN-panel bulletin; "
            f"the stale July m0 cache leaked in: {hydrate_calls}"
        )
        assert not hasattr(site, "bulletin_target_period"), (
            "stale bulletin_target_period must be cleared, or it would also "
            "leak into a later _on_write call for this same site object"
        )

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
        deliberately. Unrelated to the deleted reload heuristic: this test
        never calls `_load_bulletin_from_api` at all.
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
        """A site whose `bulletin_target_period` is explicitly `None` (this
        site's own add-time resolution genuinely failed — see
        `_resolve_month_target_period` returning `None` in `_on_add`/
        `_on_add_m0`) must not produce a plain 'Bulletin saved
        successfully'. The write must still happen (never block), but the
        operator must be told this site's month could not be confirmed.

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
