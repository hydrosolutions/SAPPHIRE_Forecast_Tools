"""PP-075 E2 — UI callback regression tests.

Closes gaps left by helper/widget unit tests: drives actual callbacks and
asserts operator-visible widget state.  Station code 19999 throughout.

Coverage:
1. PlotManager.update_forecast_plots long-term Apply
   - month refreshes primary table (forecast_tabulator) and m0
   - quarter/season refreshes primary table only, never touches shortterm chart
   - month tabulator with range_delta produces finite fc_lower/fc_upper
2. WidgetManager._wire_station_period_change registered callback
   - long-term horizons hide model_section and range_vis_section
   - short-term restores model_section and range_vis_section
   - reference_period_pane shown/hidden from dm._data["range_reference_period"]
   - forecast_card visible on Forecast tab (active=1) for all horizons
3. _wire_range_slider_visibility: native/delta → hidden; manual/mixed → visible
4. update_sidepane_card_visibility: forecast controls per-tab; Forecast tab
   retains forecast_card for month/quarter/season
"""

import types
from datetime import date as _date
from unittest.mock import MagicMock, patch

import pandas as pd
import panel as pn
import pytest
from dashboard.plot_manager import PlotManager
from dashboard.widget_manager import WidgetManager
from src import vizualization

# ---------------------------------------------------------------------------
# Shared synthetic station
# ---------------------------------------------------------------------------

STATION_CODE = "19999"
STATION_LABEL = f"{STATION_CODE} - Test River"


def _identity(s):
    return s


# ---------------------------------------------------------------------------
# Minimal fake PlotManager — bypass __init__ (see test_quarter_calendar_card)
# ---------------------------------------------------------------------------

def _make_stub_pm_for_lt(horizon, tabulator_calls, m0_calls, q_calls):
    """PlotManager with __init__ bypassed and spied update methods."""
    pm = object.__new__(PlotManager)
    pm._ = _identity
    pm._cfg = types.SimpleNamespace(viz=vizualization)
    pm._wm = types.SimpleNamespace(
        horizon_selector=types.SimpleNamespace(value=horizon),
        station_selector=types.SimpleNamespace(value=STATION_LABEL),
        date_picker=types.SimpleNamespace(value=None),
        model_checkbox=types.SimpleNamespace(options={}, value=[]),
        range_selector=types.SimpleNamespace(value="delta"),
        range_slider=types.SimpleNamespace(value=None),
        range_radiobutton=types.SimpleNamespace(value="delta"),
        forecast_tabulator=MagicMock(),
        forecast_tabulator_m0=MagicMock(),
        forecast_info_m1=types.SimpleNamespace(object=""),
        forecast_info_m0=types.SimpleNamespace(object=""),
    )
    pm._dm = types.SimpleNamespace(
        forecasts_all=pd.DataFrame(),
        long_forecasts_m0=None,
    )
    pm.pentad_forecast = types.SimpleNamespace(object=None)
    pm.summary_table_m0_card = types.SimpleNamespace(visible=False)
    pm.summary_table_q_card = types.SimpleNamespace(visible=False)

    # Spy stubs
    def _stub_update_tabulator():
        tabulator_calls.append("update_forecast_tabulator")

    def _stub_update_m0():
        m0_calls.append("update_forecast_tabulator_m0")

    def _stub_update_q():
        q_calls.append("update_quarterly_summary_tabulator")

    pm.update_forecast_tabulator = _stub_update_tabulator
    pm.update_forecast_tabulator_m0 = _stub_update_m0
    pm.update_quarterly_summary_tabulator = _stub_update_q
    return pm


# ===========================================================================
# 1. PlotManager.update_forecast_plots — long-term Apply
# ===========================================================================

class TestUpdateForecastPlotsLongTerm:
    def test_month_calls_all_three_updaters(self):
        """update_forecast_plots with 'month' must call update_forecast_tabulator,
        update_forecast_tabulator_m0, and update_quarterly_summary_tabulator."""
        tab_calls, m0_calls, q_calls = [], [], []
        pm = _make_stub_pm_for_lt("month", tab_calls, m0_calls, q_calls)
        pm._wm.horizon_selector.value = "month"

        PlotManager.update_forecast_plots(pm)

        assert tab_calls == ["update_forecast_tabulator"]
        assert m0_calls == ["update_forecast_tabulator_m0"]
        assert q_calls == ["update_quarterly_summary_tabulator"]

    def test_quarter_calls_only_tabulator(self):
        """update_forecast_plots with 'quarter' must call update_forecast_tabulator
        and nothing else — no m0, no quarterly, no short-term chart update."""
        tab_calls, m0_calls, q_calls = [], [], []
        pm = _make_stub_pm_for_lt("quarter", tab_calls, m0_calls, q_calls)
        pm._wm.horizon_selector.value = "quarter"

        PlotManager.update_forecast_plots(pm)

        assert tab_calls == ["update_forecast_tabulator"]
        assert m0_calls == []
        assert q_calls == []
        # Short-term chart must NOT be touched
        assert pm.pentad_forecast.object is None

    def test_season_calls_only_tabulator(self):
        """update_forecast_plots with 'season' must call update_forecast_tabulator
        and not touch short-term chart or m0 updaters."""
        tab_calls, m0_calls, q_calls = [], [], []
        pm = _make_stub_pm_for_lt("season", tab_calls, m0_calls, q_calls)
        pm._wm.horizon_selector.value = "season"

        PlotManager.update_forecast_plots(pm)

        assert tab_calls == ["update_forecast_tabulator"]
        assert m0_calls == []
        assert q_calls == []
        assert pm.pentad_forecast.object is None

    def test_shortterm_does_not_call_m0_or_quarterly_updaters(self):
        """update_forecast_plots with 'pentad' must NOT call update_forecast_tabulator_m0
        or update_quarterly_summary_tabulator — those are month-only.
        (update_forecast_tabulator IS also called on the short-term path at the end.)"""
        tab_calls, m0_calls, q_calls = [], [], []
        pm = _make_stub_pm_for_lt("pentad", tab_calls, m0_calls, q_calls)
        pm._wm.horizon_selector.value = "pentad"
        # Stub out everything the real short-term path touches.
        pm._dm.hydrograph_pentad_all = None
        pm._dm.hydrograph_day_all = None
        pm._dm.linreg_predictor = None
        pm._dm.rram_forecast = None
        pm._dm.ml_forecast = None
        pm._build_forecast_hydrograph = lambda: None
        pm._cfg = types.SimpleNamespace(
            viz=types.SimpleNamespace(
                plot_forecast_skill=lambda *a, **kw: (
                    types.SimpleNamespace(object=None),
                    types.SimpleNamespace(object=None),
                ),
            )
        )
        pm.effectiveness = types.SimpleNamespace(object=None)
        pm.accuracy = types.SimpleNamespace(object=None)

        PlotManager.update_forecast_plots(pm)

        # Short-term also calls update_forecast_tabulator (at the end) —
        # the key invariant is that m0 and quarterly are never touched.
        assert m0_calls == [], "m0 updater must not run for pentad"
        assert q_calls == [], "quarterly updater must not run for pentad"

    def test_month_range_delta_produces_finite_bounds(self):
        """When forecasts_all has a range_delta column, update_forecast_tabulator
        (the REAL method) must produce finite fc_lower/fc_upper in the tabulator.
        This test drives the REAL update_forecast_tabulator with a minimal df
        carrying range_delta=20.0 and asserts the tabulator.value rows have
        finite lower/upper bounds — proves the range_delta substitution path
        in create_forecast_summary_table actually fires rather than producing NaN."""
        from src import vizualization as viz

        tabulator = pn.widgets.Tabulator(pd.DataFrame())

        forecasts_all = pd.DataFrame([{
            "station_labels": STATION_LABEL,
            "code": STATION_CODE,
            "date": pd.Timestamp("2026-07-01"),
            "model_short": "GBT",
            "forecasted_discharge": 100.0,
            "delta": 5.0,
            "range_delta": 20.0,
            "Q25": 85.0,
            "Q75": 115.0,
            "month_in_year": 7,
        }])

        wm = types.SimpleNamespace(
            station_selector=types.SimpleNamespace(value=STATION_LABEL),
            date_picker=types.SimpleNamespace(value=pd.Timestamp("2026-07-01").date()),
            model_checkbox=types.SimpleNamespace(options={"GBT": "GBT"}, value=["GBT"]),
            range_selector=types.SimpleNamespace(value="delta"),
            range_slider=types.SimpleNamespace(value=None),
            forecast_tabulator=tabulator,
            forecast_info_m1=types.SimpleNamespace(object=""),
            horizon_selector=types.SimpleNamespace(value="month"),
        )

        pm = object.__new__(PlotManager)
        pm._ = _identity
        pm._cfg = types.SimpleNamespace(viz=viz)
        pm._wm = wm
        pm._dm = types.SimpleNamespace(forecasts_all=forecasts_all)

        PlotManager.update_forecast_tabulator(pm)

        df = tabulator.value
        assert df is not None and not df.empty, "tabulator must be populated"
        # range_delta=20 → delta path → fc_lower=80, fc_upper=120 (or any finite pair)
        lower_col = next(
            (c for c in df.columns if "lower" in c.lower() or c == "fc_lower"), None
        )
        upper_col = next(
            (c for c in df.columns if "upper" in c.lower() or c == "fc_upper"), None
        )
        assert lower_col is not None, f"no lower-bound column in {list(df.columns)}"
        assert upper_col is not None, f"no upper-bound column in {list(df.columns)}"
        assert df[lower_col].notna().any(), (
            f"fc_lower must be finite when range_delta=20 is present; got {df[lower_col].tolist()}"
        )
        assert df[upper_col].notna().any(), (
            f"fc_upper must be finite when range_delta=20 is present; got {df[upper_col].tolist()}"
        )


# ===========================================================================
# 2. WidgetManager._wire_station_period_change — real callback wiring
# ===========================================================================

def _make_wm_for_callback(
    initial_horizon="pentad",
    dm_data=None,
    is_forecast_tab=True,
):
    """Minimal fake WidgetManager self for _wire_station_period_change.

    Mirrors the _make_wm pattern from test_monthly_lead_regression.py.
    All Panel widgets are real so pn.depends/watch wiring fires synchronously.
    """
    fake_self = types.SimpleNamespace(
        _gettext=_identity,
        horizon_selector=pn.widgets.Select(
            options=["pentad", "decade", "month", "quarter", "season"],
            value=initial_horizon,
        ),
        station_selector=pn.widgets.Select(
            options=[STATION_LABEL],
            value=STATION_LABEL,
        ),
        pentad_selector=pn.widgets.Select(options=[30], value=30),
        decad_selector=pn.widgets.Select(options=[10], value=10),
        date_picker=types.SimpleNamespace(value=None),
        forecast_card=types.SimpleNamespace(visible=False),
        model_section=types.SimpleNamespace(visible=True),
        range_vis_section=types.SimpleNamespace(visible=True),
        reference_period_pane=types.SimpleNamespace(object="", visible=False),
        horizon_info_pane=types.SimpleNamespace(object=""),
        _dashboard_tabs=types.SimpleNamespace(active=1 if is_forecast_tab else 0),
        _post_load_callbacks=[],
        _metadata_horizon=initial_horizon,
        forecast_horizon=30,
        forecast_year=2026,
        last_date=None,
    )
    fake_self.refresh_model_checkbox = lambda: None
    fake_self.refresh_warnings = lambda: None
    fake_self._refresh_horizon_info_pane = lambda: None

    return fake_self


def _make_fake_dm(ref_period=None, fail_metadata=False):
    """Minimal fake DataManager for _wire_station_period_change."""
    data = {}
    if ref_period is not None:
        data["range_reference_period"] = ref_period

    class _FakeDM:
        forecasts_all = pd.DataFrame()
        _data = data

        def load_station(self, horizon, code):
            pass

        def update_sites_for_pentad(self, _, horizon, pentad, decad):
            pass

        def invalidate_render_cache(self):
            pass

        def get_bulletin_metadata(self, horizon):
            if fail_metadata:
                raise ValueError("no data")
            return (None, 8, 2026)

    return _FakeDM()


class TestWireStationPeriodChangeLongTerm:
    """Real pn.depends callback via _wire_station_period_change."""

    def test_longterm_hides_model_and_range_sections(self):
        """Switching to a long-term horizon must set model_section.visible=False
        and range_vis_section.visible=False — these controls are ST-only."""
        fake_self = _make_wm_for_callback("pentad")
        fake_self.model_section.visible = True
        fake_self.range_vis_section.visible = True
        dm = _make_fake_dm()
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)
        fake_self.horizon_selector.value = "month"

        assert fake_self.model_section.visible is False, (
            "model_section must be hidden on long-term horizon"
        )
        assert fake_self.range_vis_section.visible is False, (
            "range_vis_section must be hidden on long-term horizon"
        )

    def test_shortterm_restores_model_and_range_sections(self):
        """Switching back to a short-term horizon must restore model_section and
        range_vis_section to visible=True."""
        fake_self = _make_wm_for_callback("month")
        fake_self.model_section.visible = False
        fake_self.range_vis_section.visible = False
        dm = _make_fake_dm()
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)
        fake_self.horizon_selector.value = "pentad"

        assert fake_self.model_section.visible is True, (
            "model_section must be restored on short-term horizon"
        )
        assert fake_self.range_vis_section.visible is True, (
            "range_vis_section must be restored on short-term horizon"
        )

    def test_longterm_with_ref_period_shows_label(self):
        """When dm._data has a 'range_reference_period' and horizon is long-term,
        reference_period_pane must become visible with the period text."""
        fake_self = _make_wm_for_callback("pentad")
        dm = _make_fake_dm(ref_period="1991–2020")
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)
        fake_self.horizon_selector.value = "month"

        assert fake_self.reference_period_pane.visible is True, (
            "reference_period_pane must be visible when ref period is available"
        )
        assert "1991" in fake_self.reference_period_pane.object, (
            f"period text must include the ref period; got {fake_self.reference_period_pane.object!r}"
        )

    def test_longterm_without_ref_period_hides_label(self):
        """When dm._data has no 'range_reference_period', reference_period_pane
        must remain hidden even on a long-term horizon."""
        fake_self = _make_wm_for_callback("pentad")
        fake_self.reference_period_pane.visible = True  # start visible
        dm = _make_fake_dm(ref_period=None)
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)
        fake_self.horizon_selector.value = "month"

        assert fake_self.reference_period_pane.visible is False, (
            "reference_period_pane must be hidden when no ref period is available"
        )

    def test_shortterm_hides_ref_period_label(self):
        """Short-term horizons must always hide reference_period_pane regardless
        of what is in dm._data."""
        fake_self = _make_wm_for_callback("month")
        fake_self.reference_period_pane.visible = True  # start visible
        dm = _make_fake_dm(ref_period="1991–2020")
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)
        fake_self.horizon_selector.value = "pentad"

        assert fake_self.reference_period_pane.visible is False, (
            "reference_period_pane must be hidden for short-term horizons"
        )

    def test_forecast_card_visible_on_forecast_tab_longterm(self):
        """forecast_card.visible must be True on Forecast tab (active=1) even
        for long-term horizons — the card houses the Apply button."""
        fake_self = _make_wm_for_callback("pentad", is_forecast_tab=True)
        fake_self.forecast_card.visible = False
        dm = _make_fake_dm()
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)
        fake_self.horizon_selector.value = "month"

        assert fake_self.forecast_card.visible is True, (
            "forecast_card must be visible on Forecast tab for long-term horizons"
        )

    def test_forecast_card_hidden_on_predictors_tab(self):
        """forecast_card must be hidden when the active tab is Predictors (0),
        regardless of horizon."""
        fake_self = _make_wm_for_callback("pentad", is_forecast_tab=False)
        fake_self._dashboard_tabs.active = 0
        fake_self.forecast_card.visible = True
        dm = _make_fake_dm()
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)
        fake_self.horizon_selector.value = "month"

        assert fake_self.forecast_card.visible is False, (
            "forecast_card must be hidden on Predictors tab"
        )

    @pytest.mark.parametrize("horizon", ["quarter", "season"])
    def test_quarter_season_hides_model_range_sections(self, horizon):
        """Quarter and season also qualify as long-term: model/range sections
        must be hidden exactly as for month."""
        fake_self = _make_wm_for_callback("pentad")
        fake_self.model_section.visible = True
        fake_self.range_vis_section.visible = True
        dm = _make_fake_dm()
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)
        fake_self.horizon_selector.value = horizon

        assert fake_self.model_section.visible is False
        assert fake_self.range_vis_section.visible is False


# ===========================================================================
# 3. _wire_range_slider_visibility — real selector watcher
# ===========================================================================

def _make_wm_for_range_visibility():
    """Fake WidgetManager self with real range_selector and range_slider
    for testing _wire_range_slider_visibility."""
    from dashboard.widgets import create_range_widgets

    sel, slider, radio = create_range_widgets()
    fake_self = types.SimpleNamespace(
        range_selector=sel,
        range_slider=slider,
        _gettext=_identity,
    )
    fake_self._cfg = types.SimpleNamespace(
        viz=vizualization,
    )
    return fake_self


class TestRangeSliderVisibilityWatcher:
    """Real .param.watch wiring via _wire_range_slider_visibility."""

    def test_initial_delta_slider_hidden(self):
        """Default selection is 'delta' — slider must start hidden."""
        fake_self = _make_wm_for_range_visibility()

        WidgetManager._wire_range_slider_visibility(fake_self)

        assert fake_self.range_slider.visible is False, (
            "slider must be hidden by default (delta)"
        )

    def test_switch_to_manual_shows_slider(self):
        """Switching range_selector to a 'manual' internal value must show the slider."""
        fake_self = _make_wm_for_range_visibility()
        WidgetManager._wire_range_slider_visibility(fake_self)

        fake_self.range_selector.value = "manual"

        assert fake_self.range_slider.visible is True, (
            "slider must become visible when range_selector is set to 'manual'"
        )

    def test_switch_to_mixed_shows_slider(self):
        """Switching to 'mixed' must show the slider."""
        fake_self = _make_wm_for_range_visibility()
        WidgetManager._wire_range_slider_visibility(fake_self)

        fake_self.range_selector.value = "mixed"

        assert fake_self.range_slider.visible is True

    def test_switch_to_native_hides_slider(self):
        """Switching to 'native' must hide the slider."""
        fake_self = _make_wm_for_range_visibility()
        WidgetManager._wire_range_slider_visibility(fake_self)
        # First make it visible via manual
        fake_self.range_selector.value = "manual"
        assert fake_self.range_slider.visible is True

        fake_self.range_selector.value = "native"

        assert fake_self.range_slider.visible is False, (
            "slider must be hidden again when switching to 'native'"
        )

    def test_switch_back_to_delta_hides_slider(self):
        """Switching back to 'delta' from 'manual' must re-hide the slider."""
        fake_self = _make_wm_for_range_visibility()
        WidgetManager._wire_range_slider_visibility(fake_self)
        fake_self.range_selector.value = "manual"
        assert fake_self.range_slider.visible is True

        fake_self.range_selector.value = "delta"

        assert fake_self.range_slider.visible is False


# ===========================================================================
# 4. update_sidepane_card_visibility — tab callback
# ===========================================================================

def _make_tabs(active):
    return types.SimpleNamespace(active=active)


def _make_cards():
    return {
        "horizon_card": types.SimpleNamespace(visible=None),
        "station_card": types.SimpleNamespace(visible=None),
        "forecast_card": types.SimpleNamespace(visible=None),
        "basin_card": types.SimpleNamespace(visible=None),
        "reload_card": types.SimpleNamespace(visible=None),
    }


class TestUpdateSidepaneCardVisibility:
    """update_sidepane_card_visibility contract per tab."""

    def test_predictors_tab_hides_forecast_and_basin(self):
        tabs = _make_tabs(0)
        cards = _make_cards()
        horizon_sel = types.SimpleNamespace(value="pentad")
        event = MagicMock()

        vizualization.update_sidepane_card_visibility(
            tabs, cards["horizon_card"], cards["station_card"],
            cards["forecast_card"], cards["basin_card"], cards["reload_card"],
            event, horizon_selector=horizon_sel,
        )

        assert cards["forecast_card"].visible is False
        assert cards["basin_card"].visible is False
        assert cards["horizon_card"].visible is True
        assert cards["station_card"].visible is True

    def test_forecast_tab_shows_forecast_card(self):
        tabs = _make_tabs(1)
        cards = _make_cards()
        event = MagicMock()

        vizualization.update_sidepane_card_visibility(
            tabs, cards["horizon_card"], cards["station_card"],
            cards["forecast_card"], cards["basin_card"], cards["reload_card"],
            event, horizon_selector=types.SimpleNamespace(value="pentad"),
        )

        assert cards["forecast_card"].visible is True
        assert cards["basin_card"].visible is False

    @pytest.mark.parametrize("horizon", ["month", "quarter", "season"])
    def test_forecast_tab_forecast_card_shown_for_longterm(self, horizon):
        """Forecast card must stay visible on Forecast tab for all long-term
        horizons — it contains the Apply button."""
        tabs = _make_tabs(1)
        cards = _make_cards()
        event = MagicMock()

        vizualization.update_sidepane_card_visibility(
            tabs, cards["horizon_card"], cards["station_card"],
            cards["forecast_card"], cards["basin_card"], cards["reload_card"],
            event, horizon_selector=types.SimpleNamespace(value=horizon),
        )

        assert cards["forecast_card"].visible is True, (
            f"forecast_card must be visible on Forecast tab for {horizon} horizon"
        )

    def test_bulletin_tab_hides_station_and_forecast(self):
        tabs = _make_tabs(2)
        cards = _make_cards()
        event = MagicMock()

        vizualization.update_sidepane_card_visibility(
            tabs, cards["horizon_card"], cards["station_card"],
            cards["forecast_card"], cards["basin_card"], cards["reload_card"],
            event,
        )

        assert cards["station_card"].visible is False
        assert cards["forecast_card"].visible is False
        assert cards["basin_card"].visible is True

    def test_other_tab_hides_all(self):
        """Disclaimer (or any tab >= 3) must hide all sidebar cards."""
        tabs = _make_tabs(3)
        cards = _make_cards()
        event = MagicMock()

        vizualization.update_sidepane_card_visibility(
            tabs, cards["horizon_card"], cards["station_card"],
            cards["forecast_card"], cards["basin_card"], cards["reload_card"],
            event,
        )

        assert cards["horizon_card"].visible is False
        assert cards["station_card"].visible is False
        assert cards["forecast_card"].visible is False
        assert cards["basin_card"].visible is False
        assert cards["reload_card"].visible is False


# ===========================================================================
# 5. Full PlotManager.update_forecast_plots — all three tables, real updaters
# ===========================================================================

_RESERVOIR_LABEL = f"{STATION_CODE} - Test River вдхр"


def _make_reservoir_site():
    from src.site import SapphireSite
    site = SapphireSite.__new__(SapphireSite)
    site.code = STATION_CODE
    site.punkt_name_ru = "вдхр Test"
    return site


def _make_full_pm(range_selector_value="delta", range_slider_value=None):
    """Real PlotManager via __new__ with all three real update methods.

    Station 19999 is a reservoir (punkt_name_ru contains 'вдхр').
    Callers must patch skill_lead_aware_enabled in the calling scope.
    """
    forecasts_all = pd.DataFrame([{
        "station_labels": _RESERVOIR_LABEL,
        "code": STATION_CODE,
        "date": pd.Timestamp("2026-06-30"),
        "model_short": "GBT",
        "forecasted_discharge": 100.0,
        "delta": 5.0,
        "range_delta": 10.0,
        "Q25": 70.0,
        "Q75": 130.0,
        "month_in_year": 6,
    }])
    long_forecasts_m0 = pd.DataFrame([{
        "station_labels": _RESERVOIR_LABEL,
        "code": STATION_CODE,
        "date": pd.Timestamp("2026-07-01"),
        "model_short": "GBT",
        "forecasted_discharge": 200.0,
        "delta": 5.0,
        "range_delta": 20.0,
        "Q25": 140.0,
        "Q75": 260.0,
        "month_in_year": 7,
    }])
    long_forecasts_quarter = pd.DataFrame([{
        "station_labels": _RESERVOIR_LABEL,
        "code": STATION_CODE,
        "date": pd.Timestamp("2026-06-30"),
        "model_short": "GBT",
        "forecasted_discharge": 300.0,
        "delta": 5.0,
        "range_delta": 30.0,
        "Q25": 210.0,
        "Q75": 390.0,
        "valid_from": pd.Timestamp("2026-07-01"),
        "valid_to": pd.Timestamp("2026-09-30"),
        "quarter_issue_date": pd.Timestamp("2026-06-30"),
    }])

    wm = types.SimpleNamespace(
        horizon_selector=types.SimpleNamespace(value="month"),
        station_selector=pn.widgets.Select(
            options=[_RESERVOIR_LABEL], value=_RESERVOIR_LABEL
        ),
        date_picker=types.SimpleNamespace(value=_date(2026, 6, 30)),
        model_checkbox=types.SimpleNamespace(options={"GBT": "GBT"}, value=["GBT"]),
        range_selector=types.SimpleNamespace(value=range_selector_value),
        range_slider=types.SimpleNamespace(value=range_slider_value),
        range_radiobutton=types.SimpleNamespace(value="delta"),
        forecast_tabulator=pn.widgets.Tabulator(pd.DataFrame()),
        forecast_tabulator_m0=pn.widgets.Tabulator(pd.DataFrame()),
        forecast_tabulator_q=pn.widgets.Tabulator(pd.DataFrame()),
        forecast_info_m1=types.SimpleNamespace(object=""),
        forecast_info_m0=types.SimpleNamespace(object=""),
        forecast_info_q=types.SimpleNamespace(object=""),
    )

    dm = types.SimpleNamespace(
        forecasts_all=forecasts_all,
        long_forecasts_m0=long_forecasts_m0,
        long_forecasts_quarter=long_forecasts_quarter,
        sites_list=[_make_reservoir_site()],
    )

    pm = object.__new__(PlotManager)
    pm._ = _identity
    pm._cfg = types.SimpleNamespace(viz=vizualization)
    pm._wm = wm
    pm._dm = dm
    pm.summary_table_m0_card = types.SimpleNamespace(visible=False)
    pm.summary_table_q_card = types.SimpleNamespace(visible=False)
    return pm


def _get_bounds(tabulator, lower_key="Forecast lower bound", upper_key="Forecast upper bound"):
    df = tabulator.value
    assert df is not None and not df.empty, f"tabulator not populated; got {df!r}"
    return df[lower_key].iloc[0], df[upper_key].iloc[0]


class TestFullApplyE2:
    """PlotManager.update_forecast_plots with REAL updaters — all three rendered tables.

    Proves Apply populates main, m0, and quarterly tabulators with numerically
    correct bounds for delta, manual, and native range modes.  Station 19999
    is a reservoir so the quarterly card's conditions are met.

    Visibility gate for m0: forecasts_all max date 2026-06-30 (month=6) →
    summary_target_month=7; m0 max date 2026-07-01 (month=7) → passes.
    """

    def test_delta_mode_bounds_all_three_tables(self):
        """delta mode: main=90/110, m0=180/220, quarter=270/330."""
        with patch("dashboard.plot_manager.skill_lead_aware_enabled", return_value=False):
            pm = _make_full_pm("delta", None)
            PlotManager.update_forecast_plots(pm)

        lower_m, upper_m = _get_bounds(pm._wm.forecast_tabulator)
        lower_m0, upper_m0 = _get_bounds(pm._wm.forecast_tabulator_m0)
        lower_q, upper_q = _get_bounds(pm._wm.forecast_tabulator_q)

        assert (lower_m, upper_m) == (90.0, 110.0), f"main bounds wrong: {lower_m}/{upper_m}"
        assert (lower_m0, upper_m0) == (180.0, 220.0), f"m0 bounds wrong: {lower_m0}/{upper_m0}"
        assert (lower_q, upper_q) == (270.0, 330.0), f"quarter bounds wrong: {lower_q}/{upper_q}"

    def test_manual_20pct_bounds_all_three_tables(self):
        """manual 20%: main=80/120, m0=160/240, quarter=240/360."""
        with patch("dashboard.plot_manager.skill_lead_aware_enabled", return_value=False):
            pm = _make_full_pm("manual", 20)
            PlotManager.update_forecast_plots(pm)

        lower_m, upper_m = _get_bounds(pm._wm.forecast_tabulator)
        lower_m0, upper_m0 = _get_bounds(pm._wm.forecast_tabulator_m0)
        lower_q, upper_q = _get_bounds(pm._wm.forecast_tabulator_q)

        assert (lower_m, upper_m) == (80.0, 120.0), f"main bounds wrong: {lower_m}/{upper_m}"
        assert (lower_m0, upper_m0) == (160.0, 240.0), f"m0 bounds wrong: {lower_m0}/{upper_m0}"
        assert (lower_q, upper_q) == (240.0, 360.0), f"quarter bounds wrong: {lower_q}/{upper_q}"

    def test_native_mode_bounds_all_three_tables(self):
        """native mode: main=70/130, m0=140/260, quarter=210/390."""
        with patch("dashboard.plot_manager.skill_lead_aware_enabled", return_value=False):
            pm = _make_full_pm("native", None)
            PlotManager.update_forecast_plots(pm)

        lower_m, upper_m = _get_bounds(pm._wm.forecast_tabulator)
        lower_m0, upper_m0 = _get_bounds(pm._wm.forecast_tabulator_m0)
        lower_q, upper_q = _get_bounds(pm._wm.forecast_tabulator_q)

        assert (lower_m, upper_m) == (70.0, 130.0), f"main bounds wrong: {lower_m}/{upper_m}"
        assert (lower_m0, upper_m0) == (140.0, 260.0), f"m0 bounds wrong: {lower_m0}/{upper_m0}"
        assert (lower_q, upper_q) == (210.0, 390.0), f"quarter bounds wrong: {lower_q}/{upper_q}"

    def test_m0_and_quarter_cards_visible_after_apply(self):
        """summary_table_m0_card and summary_table_q_card must be visible=True after Apply."""
        with patch("dashboard.plot_manager.skill_lead_aware_enabled", return_value=False):
            pm = _make_full_pm("delta", None)
            PlotManager.update_forecast_plots(pm)

        assert pm.summary_table_m0_card.visible is True, "m0 card must be visible"
        assert pm.summary_table_q_card.visible is True, "quarter card must be visible"

    def test_input_dataframes_not_mutated(self):
        """Apply must not mutate Q25/Q75 or the skill delta column in forecasts_all."""
        with patch("dashboard.plot_manager.skill_lead_aware_enabled", return_value=False):
            pm = _make_full_pm("delta", None)
            PlotManager.update_forecast_plots(pm)

        fa = pm._dm.forecasts_all
        assert fa.at[0, "Q25"] == 70.0, f"Q25 mutated: {fa.at[0, 'Q25']}"
        assert fa.at[0, "Q75"] == 130.0, f"Q75 mutated: {fa.at[0, 'Q75']}"
        assert fa.at[0, "delta"] == 5.0, f"skill delta mutated: {fa.at[0, 'delta']}"

    def test_shortterm_path_never_called(self):
        """_build_forecast_hydrograph must not be entered for a month-horizon Apply."""
        with patch("dashboard.plot_manager.skill_lead_aware_enabled", return_value=False):
            pm = _make_full_pm("delta", None)

            def _raise():
                raise AssertionError("short-term path was entered for month horizon")

            pm._build_forecast_hydrograph = _raise
            PlotManager.update_forecast_plots(pm)  # must not raise


# ===========================================================================
# 6. Station-change watcher — load_station updates ref label
# ===========================================================================

class TestStationChangeWatcher:
    """Real @pn.depends watcher via _wire_station_period_change.

    Changing station_selector.value triggers load_station; dm._data is
    updated with the new reference period, which causes reference_period_pane
    to show the period text.
    """

    def test_station_change_updates_ref_period_label(self):
        """Switching to a second station (same code 19999) calls load_station,
        which sets dm._data['range_reference_period'], and the callback makes
        reference_period_pane visible with the period text."""

        class _DM:
            forecasts_all = pd.DataFrame()
            _data: dict = {}  # noqa: RUF012

            def __init__(self):
                self._data = {}

            def load_station(self, horizon, code):
                self._data["range_reference_period"] = "1991–2020"

            def update_sites_for_pentad(self, _, horizon, pentad, decad):
                pass

            def invalidate_render_cache(self):
                pass

            def get_bulletin_metadata(self, horizon):
                return (None, 8, 2026)

        fake_self = types.SimpleNamespace(
            _gettext=_identity,
            horizon_selector=pn.widgets.Select(
                options=["month", "pentad"], value="month"
            ),
            station_selector=pn.widgets.Select(
                options=[STATION_LABEL, f"{STATION_CODE} - Other River"],
                value=STATION_LABEL,
            ),
            pentad_selector=pn.widgets.Select(options=[30], value=30),
            decad_selector=pn.widgets.Select(options=[10], value=10),
            date_picker=types.SimpleNamespace(value=None),
            forecast_card=types.SimpleNamespace(visible=False),
            model_section=types.SimpleNamespace(visible=True),
            range_vis_section=types.SimpleNamespace(visible=True),
            reference_period_pane=types.SimpleNamespace(object="", visible=False),
            horizon_info_pane=types.SimpleNamespace(object=""),
            _dashboard_tabs=types.SimpleNamespace(active=1),
            _post_load_callbacks=[],
            _metadata_horizon="month",
            forecast_horizon=30,
            forecast_year=2026,
            last_date=None,
        )
        fake_self.refresh_model_checkbox = lambda: None
        fake_self.refresh_warnings = lambda: None
        fake_self._refresh_horizon_info_pane = lambda: None

        dm = _DM()
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)

        # Trigger by changing to a second label with the same code
        fake_self.station_selector.value = f"{STATION_CODE} - Other River"

        assert fake_self.reference_period_pane.visible is True, (
            "reference_period_pane must be visible after station change that sets ref period"
        )
        assert "1991" in fake_self.reference_period_pane.object, (
            f"period text must contain the year; got {fake_self.reference_period_pane.object!r}"
        )


# ===========================================================================
# 7. Range slider visibility — non-identity gettext proves stable-key routing
# ===========================================================================

class TestRangeSliderVisibilityNonIdentityGettext:
    """_wire_range_slider_visibility uses stable string keys even when _ ≠ identity.

    update_range_slider_visibility checks range_type against both
    _("delta")/_("Native") AND the stable "delta"/"native" literals.
    With a Russian-like gettext, stable-key selects still hide the slider.
    """

    def test_stable_key_delta_hides_slider_with_translated_gettext(self):
        """With gettext that maps 'delta'→'дельта', setting range_selector to the
        stable key 'delta' must still hide the slider (matches the literal fallback)."""
        from dashboard.widgets import create_range_widgets

        def _ru(s):
            return {"delta": "дельта",
                    "Native": "Нативный"}.get(s, s)

        sel, slider, _radio = create_range_widgets()
        fake_self = types.SimpleNamespace(
            range_selector=sel,
            range_slider=slider,
            _gettext=_ru,
            _cfg=types.SimpleNamespace(viz=vizualization),
        )

        WidgetManager._wire_range_slider_visibility(fake_self)

        # Stable key "delta" → hidden even though _ru("delta") ≠ "delta"
        assert slider.visible is False, "slider must start hidden for 'delta'"

        # Stable key "manual" → visible
        sel.value = "manual"
        assert slider.visible is True, "slider must become visible for 'manual'"

        # Back to stable key "delta" → hidden again
        sel.value = "delta"
        assert slider.visible is False, (
            "slider must be hidden again for 'delta' even with translated gettext"
        )
