"""PP-075 — capture identity regression tests.

Covers the four guards added to _capture_quarterly_snapshot and the
metadata-ordering fix in update_quarterly_summary_tabulator.

Coverage:
1. Card visibility guard — hidden card returns None even with source_df set
2. Station identity filter — source_df for wrong station returns None
3. Negative index guard — selection=[-1] returns None
4. Ambiguity guard — model with conflicting periods returns None
5. Duplicate source rows with SAME period — accepted (unambiguous)
6. REAL PlotManager renderer handoff:
   a. _quarterly_source_df assigned AFTER renderer succeeds (nonempty table)
   b. Renderer failure leaves _quarterly_source_df=None
   c. Renderer producing empty table leaves _quarterly_source_df=None
7. Sorted tabulator row-selection picks the correct model by identity, not
   by sorted position
8. Model readonly editor — forecast_tabulator_q.editors['Model'] is None
9. Numeric columns are editable — not set to None in editors

Station code 19999 only (no real station codes or discharge values).
"""

from __future__ import annotations

import types
from datetime import date as _date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import panel as pn
import pytest
from dashboard.plot_manager import PlotManager
from src import vizualization
from src.site import SapphireSite

# ---------------------------------------------------------------------------
# Bootstrap — import bulletin_manager helpers the same way test_quarterly_snapshot
# does, via the existing test infrastructure.
# ---------------------------------------------------------------------------
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Import directly (panel/db/etc are real in this test environment)
from dashboard import bulletin_manager  # noqa: E402
from dashboard.bulletin_manager import _capture_quarterly_snapshot  # noqa: E402

# ---------------------------------------------------------------------------
# Constants / helpers
# ---------------------------------------------------------------------------

STATION_CODE = "19999"
STATION_LABEL = f"{STATION_CODE} - Test River вдхр"
OTHER_STATION = "19998 - Other River"

Q3_VF = "2026-07-01"
Q3_VT = "2026-09-30"
Q4_VF = "2026-10-01"
Q4_VT = "2026-12-31"


def _identity(s):
    return s


def _make_reservoir_site(code=STATION_CODE):
    site = SapphireSite.__new__(SapphireSite)
    site.code = code
    site.punkt_name_ru = "вдхр Test"
    site.station_label = STATION_LABEL
    return site


def _q_source(model="GBT", vf=Q3_VF, vt=Q3_VT, station=STATION_LABEL):
    return pd.DataFrame([{
        "model_short": model,
        "valid_from": pd.Timestamp(vf),
        "valid_to": pd.Timestamp(vt),
        "forecasted_discharge": 300.0,
        "delta": 15.0,
        "range_delta": 30.0,
        "Q25": 210.0,
        "Q75": 390.0,
        "date": pd.Timestamp("2026-06-30"),
        "station_labels": station,
        "quarter_issue_date": pd.Timestamp("2026-06-30"),
    }])


def _q_tabulator(model="GBT", q=300.0, lb=270.0, ub=330.0):
    return pd.DataFrame([{
        "Model": model,
        "Forecasted discharge": q,
        "Forecast lower bound": lb,
        "Forecast upper bound": ub,
        "δ": 30.0,
        "s/σ": 0.5,
        "MAE": 5.0,
        "Accuracy": 90.0,
    }])


def _make_wm_with_pm(source_df, tabulator_df, selection=None, card_visible=True):
    """wm stub with _pm.summary_table_q_card set, for card-visibility tests."""
    tab_q = pn.widgets.Tabulator(tabulator_df if tabulator_df is not None else pd.DataFrame())
    if selection is not None:
        tab_q.selection = selection
    elif tabulator_df is not None and not tabulator_df.empty:
        tab_q.selection = [0]

    q_card = types.SimpleNamespace(visible=card_visible)
    pm_stub = types.SimpleNamespace(summary_table_q_card=q_card)

    wm = types.SimpleNamespace(
        _quarterly_source_df=source_df,
        forecast_tabulator_q=tab_q,
        station_selector=types.SimpleNamespace(value=STATION_LABEL),
        _pm=pm_stub,
    )
    return wm


# ---------------------------------------------------------------------------
# Full PlotManager stub for renderer tests (mirrors _make_full_pm in
# test_range_callbacks_e2.py)
# ---------------------------------------------------------------------------

def _make_pm_for_renderer(quarterly_df, make_tabulator=True):
    """Real PlotManager via __new__ with real vizualization, minimal wm."""
    site = _make_reservoir_site()

    wm = types.SimpleNamespace(
        horizon_selector=types.SimpleNamespace(value="month"),
        station_selector=pn.widgets.Select(
            options=[STATION_LABEL], value=STATION_LABEL
        ),
        date_picker=types.SimpleNamespace(value=_date(2026, 6, 30)),
        model_checkbox=types.SimpleNamespace(options={"GBT": "GBT"}, value=["GBT"]),
        range_selector=types.SimpleNamespace(value="delta"),
        range_slider=types.SimpleNamespace(value=None),
        forecast_tabulator_q=(
            pn.widgets.Tabulator(pd.DataFrame()) if make_tabulator else None
        ),
        forecast_info_q=types.SimpleNamespace(object=""),
        _quarterly_source_df=None,
    )

    dm = types.SimpleNamespace(
        sites_list=[site],
        long_forecasts_quarter=quarterly_df,
    )

    pm = object.__new__(PlotManager)
    pm._ = _identity
    pm._cfg = types.SimpleNamespace(viz=vizualization)
    pm._wm = wm
    pm._dm = dm
    pm.summary_table_q_card = types.SimpleNamespace(visible=False)
    return pm


# ===========================================================================
# 1. Card visibility guard
# ===========================================================================

class TestCardVisibilityGuard:
    def test_hidden_card_returns_none_even_with_source_df(self):
        """When card is hidden, _capture returns None regardless of source_df."""
        wm = _make_wm_with_pm(
            source_df=_q_source(),
            tabulator_df=_q_tabulator(),
            selection=[0],
            card_visible=False,
        )
        assert _capture_quarterly_snapshot(wm, _identity) is None

    def test_visible_card_proceeds_to_capture(self):
        """When card is visible, capture proceeds normally."""
        wm = _make_wm_with_pm(
            source_df=_q_source(),
            tabulator_df=_q_tabulator(),
            selection=[0],
            card_visible=True,
        )
        snap = _capture_quarterly_snapshot(wm, _identity)
        assert snap is not None
        assert snap["model_type"] == "GBT"

    def test_no_pm_attribute_skips_guard(self):
        """When wm has no _pm, the card visibility guard is skipped (safe fallback)."""
        tab_q = pn.widgets.Tabulator(_q_tabulator())
        tab_q.selection = [0]
        wm = types.SimpleNamespace(
            _quarterly_source_df=_q_source(),
            forecast_tabulator_q=tab_q,
            station_selector=types.SimpleNamespace(value=STATION_LABEL),
            # _pm intentionally absent
        )
        snap = _capture_quarterly_snapshot(wm, _identity)
        assert snap is not None


# ===========================================================================
# 2. Station identity filter
# ===========================================================================

class TestStationIdentityFilter:
    def test_source_df_for_wrong_station_returns_none(self):
        """Source df containing ONLY the other station returns None."""
        source_other = _q_source(station=OTHER_STATION)
        wm = _make_wm_with_pm(
            source_df=source_other,
            tabulator_df=_q_tabulator(),
            selection=[0],
            card_visible=True,
        )
        assert _capture_quarterly_snapshot(wm, _identity) is None

    def test_source_df_matching_station_captures(self):
        """Source df for the correct station captures successfully."""
        wm = _make_wm_with_pm(
            source_df=_q_source(station=STATION_LABEL),
            tabulator_df=_q_tabulator(),
            selection=[0],
            card_visible=True,
        )
        snap = _capture_quarterly_snapshot(wm, _identity)
        assert snap is not None
        assert snap["model_type"] == "GBT"

    def test_mixed_stations_filters_to_current(self):
        """When source_df has rows for multiple stations, only current-station
        rows are used.  If no rows remain for current station after filtering,
        returns None."""
        mixed = pd.concat([_q_source(station=OTHER_STATION), _q_source(station=STATION_LABEL)],
                          ignore_index=True)
        wm = _make_wm_with_pm(
            source_df=mixed,
            tabulator_df=_q_tabulator(),
            selection=[0],
            card_visible=True,
        )
        snap = _capture_quarterly_snapshot(wm, _identity)
        assert snap is not None


# ===========================================================================
# 3. Negative index guard
# ===========================================================================

class TestNegativeIndexGuard:
    def test_negative_one_selection_returns_none(self):
        """selection=[-1] must return None, not select the last row."""
        wm = _make_wm_with_pm(
            source_df=_q_source(),
            tabulator_df=_q_tabulator(),
            selection=[-1],
            card_visible=True,
        )
        assert _capture_quarterly_snapshot(wm, _identity) is None

    def test_large_negative_selection_returns_none(self):
        """selection=[-99] must return None. Panel raises on out-of-bounds
        selection assignment, so use SimpleNamespace to bypass Panel's
        validation and test the production guard directly."""
        tab_q = types.SimpleNamespace(
            value=_q_tabulator(),
            selection=[-99],
        )
        q_card = types.SimpleNamespace(visible=True)
        pm_stub = types.SimpleNamespace(summary_table_q_card=q_card)
        wm = types.SimpleNamespace(
            _quarterly_source_df=_q_source(),
            forecast_tabulator_q=tab_q,
            station_selector=types.SimpleNamespace(value=STATION_LABEL),
            _pm=pm_stub,
        )
        assert _capture_quarterly_snapshot(wm, _identity) is None

    def test_zero_selection_captures(self):
        """selection=[0] is valid and must capture normally."""
        wm = _make_wm_with_pm(
            source_df=_q_source(),
            tabulator_df=_q_tabulator(),
            selection=[0],
            card_visible=True,
        )
        snap = _capture_quarterly_snapshot(wm, _identity)
        assert snap is not None


# ===========================================================================
# 4. Ambiguity guard
# ===========================================================================

class TestAmbiguityGuard:
    def test_same_model_different_periods_returns_none(self):
        """Two source rows for the same model with different valid_from → None."""
        source = pd.concat([
            _q_source(model="GBT", vf=Q3_VF, vt=Q3_VT),
            _q_source(model="GBT", vf=Q4_VF, vt=Q4_VT),
        ], ignore_index=True)
        wm = _make_wm_with_pm(
            source_df=source,
            tabulator_df=_q_tabulator("GBT"),
            selection=[0],
            card_visible=True,
        )
        assert _capture_quarterly_snapshot(wm, _identity) is None

    def test_same_model_same_period_duplicate_accepted(self):
        """Duplicate source rows for the same model AND same period are accepted
        (unambiguous — same quarter, possibly two data records)."""
        row = _q_source(model="GBT", vf=Q3_VF, vt=Q3_VT)
        source = pd.concat([row, row], ignore_index=True)
        wm = _make_wm_with_pm(
            source_df=source,
            tabulator_df=_q_tabulator("GBT"),
            selection=[0],
            card_visible=True,
        )
        snap = _capture_quarterly_snapshot(wm, _identity)
        assert snap is not None
        assert snap["model_type"] == "GBT"

    def test_different_models_no_ambiguity(self):
        """Two source rows for DIFFERENT models is not ambiguous — capture
        resolves to the selected model only."""
        source = pd.concat([
            _q_source(model="GBT", vf=Q3_VF, vt=Q3_VT),
            _q_source(model="LR", vf=Q3_VF, vt=Q3_VT),
        ], ignore_index=True)
        wm = _make_wm_with_pm(
            source_df=source,
            tabulator_df=_q_tabulator("GBT"),
            selection=[0],
            card_visible=True,
        )
        snap = _capture_quarterly_snapshot(wm, _identity)
        assert snap is not None
        assert snap["model_type"] == "GBT"


# ===========================================================================
# 5. REAL PlotManager renderer handoff
# ===========================================================================

class TestRendererHandoff:
    """update_quarterly_summary_tabulator assigns _quarterly_source_df only after
    a successful nonempty render."""

    def test_source_df_assigned_after_successful_render(self):
        """_quarterly_source_df must be non-None after a successful render."""
        quarterly_df = _q_source()
        pm = _make_pm_for_renderer(quarterly_df)

        PlotManager.update_quarterly_summary_tabulator(pm)

        assert pm._wm._quarterly_source_df is not None, (
            "_quarterly_source_df must be set after successful render"
        )
        assert pm.summary_table_q_card.visible is True

    def test_source_df_none_before_render_then_set_after(self):
        """Verify the before/after ordering: source_df starts None (cleared at top
        of update) and is None BEFORE the renderer fires, set only after."""
        quarterly_df = _q_source()
        pm = _make_pm_for_renderer(quarterly_df)

        # Intercept the renderer to record _quarterly_source_df state before
        original_renderer = pm._cfg.viz.create_forecast_summary_tabulator
        source_df_during_render = []

        def _capturing_renderer(*args, **kwargs):
            source_df_during_render.append(pm._wm._quarterly_source_df)
            return original_renderer(*args, **kwargs)

        pm._cfg = types.SimpleNamespace(
            viz=types.SimpleNamespace(
                create_forecast_summary_tabulator=_capturing_renderer,
            )
        )

        PlotManager.update_quarterly_summary_tabulator(pm)

        assert len(source_df_during_render) == 1
        assert source_df_during_render[0] is None, (
            "_quarterly_source_df must be None DURING the renderer call "
            "(assigned only after renderer succeeds)"
        )
        # After return it must be set
        assert pm._wm._quarterly_source_df is not None

    def test_renderer_exception_leaves_source_df_none(self):
        """If the renderer raises, _quarterly_source_df stays None."""
        quarterly_df = _q_source()
        pm = _make_pm_for_renderer(quarterly_df)

        def _raising_renderer(*args, **kwargs):
            raise RuntimeError("renderer failed")

        pm._cfg = types.SimpleNamespace(
            viz=types.SimpleNamespace(
                create_forecast_summary_tabulator=_raising_renderer,
            )
        )

        with pytest.raises(RuntimeError, match="renderer failed"):
            PlotManager.update_quarterly_summary_tabulator(pm)

        assert pm._wm._quarterly_source_df is None, (
            "_quarterly_source_df must remain None when renderer raises"
        )

    def test_renderer_empty_table_leaves_source_df_none(self):
        """If the renderer produces an empty table, _quarterly_source_df stays None
        and card stays hidden."""
        quarterly_df = _q_source()
        pm = _make_pm_for_renderer(quarterly_df)

        def _empty_renderer(*args, **kwargs):
            # Find tabulator arg and set empty value
            tab = None
            for a in args:
                if isinstance(a, pn.widgets.Tabulator):
                    tab = a
            if tab is not None:
                tab.value = pd.DataFrame()

        pm._cfg = types.SimpleNamespace(
            viz=types.SimpleNamespace(
                create_forecast_summary_tabulator=_empty_renderer,
            )
        )

        PlotManager.update_quarterly_summary_tabulator(pm)

        assert pm._wm._quarterly_source_df is None, (
            "_quarterly_source_df must be None when renderer produces empty table"
        )
        assert pm.summary_table_q_card.visible is False, (
            "card must be hidden when renderer produces empty table"
        )

    def test_no_quarterly_data_clears_source_df(self):
        """When quarterly_df is empty/None, source_df is None after update."""
        pm = _make_pm_for_renderer(pd.DataFrame())
        pm._wm._quarterly_source_df = "stale"  # pre-set stale value

        PlotManager.update_quarterly_summary_tabulator(pm)

        assert pm._wm._quarterly_source_df is None, (
            "_quarterly_source_df must be cleared when card is hidden"
        )


# ===========================================================================
# 6. Sorted tabulator row-selection picks correct model by identity
# ===========================================================================

class TestSortedRowSelection:
    """When the tabulator is sorted differently from the source df, row 0 of the
    sorted display must still map to the correct model, not to the first source row."""

    def test_sorted_tabulator_row0_captures_correct_model(self):
        """Tabulator sorted GBT-first (row 0 = GBT 300.0); source has LR first.
        selection=[0] must capture GBT with its source-df identity."""
        source = pd.concat([
            _q_source(model="LR", vf=Q3_VF, vt=Q3_VT),
            _q_source(model="GBT", vf=Q3_VF, vt=Q3_VT),
        ], ignore_index=True)
        source.loc[source["model_short"] == "GBT", "forecasted_discharge"] = 300.0
        source.loc[source["model_short"] == "LR", "forecasted_discharge"] = 100.0

        # Tabulator value is sorted GBT first (higher Q)
        sorted_tab = pd.concat([
            _q_tabulator("GBT", q=300.0, lb=270.0, ub=330.0),
            _q_tabulator("LR", q=100.0, lb=90.0, ub=110.0),
        ], ignore_index=True)

        wm = _make_wm_with_pm(
            source_df=source,
            tabulator_df=sorted_tab,
            selection=[0],
            card_visible=True,
        )
        snap = _capture_quarterly_snapshot(wm, _identity)

        assert snap is not None, "capture must succeed for a valid selection"
        assert snap["model_type"] == "GBT", (
            f"row 0 of sorted tabulator is GBT, but got {snap['model_type']!r}"
        )
        assert snap["forecasted_discharge"] == pytest.approx(300.0), (
            "numeric Q must come from the tabulator row, not source df"
        )

    def test_sorted_tabulator_row1_captures_lr(self):
        """selection=[1] must capture LR (the second visible row after sort)."""
        source = pd.concat([
            _q_source(model="LR", vf=Q3_VF, vt=Q3_VT),
            _q_source(model="GBT", vf=Q3_VF, vt=Q3_VT),
        ], ignore_index=True)
        sorted_tab = pd.concat([
            _q_tabulator("GBT", q=300.0),
            _q_tabulator("LR", q=100.0),
        ], ignore_index=True)

        wm = _make_wm_with_pm(
            source_df=source,
            tabulator_df=sorted_tab,
            selection=[1],
            card_visible=True,
        )
        snap = _capture_quarterly_snapshot(wm, _identity)
        assert snap is not None
        assert snap["model_type"] == "LR"


# ===========================================================================
# 7. Model readonly editor
# ===========================================================================

class TestModelReadonlyEditor:
    """forecast_tabulator_q Model column readonly set by production WidgetManager.__init__.

    Initialises a real WidgetManager (with mocked IO/widget collaborators)
    and asserts editors on the actual wm.forecast_tabulator_q.  The test
    does NOT set editors itself, so removing the lock from widget_manager.py
    causes the assertions to fail.
    """

    @pytest.fixture
    def real_wm(self, monkeypatch):
        import datetime as _dt
        from dashboard.widget_manager import WidgetManager

        last_date = _dt.date(2026, 11, 15)
        monkeypatch.setenv("ieasyreports_report_output_path", "/tmp")
        # create_language_buttons accesses pn.state.location (Panel server-only).
        monkeypatch.setattr(
            "dashboard.widgets.create_language_buttons",
            lambda: pn.pane.Markdown(""),
        )

        dm = MagicMock()
        dm.forecasts_all = pd.DataFrame({"date": [pd.Timestamp(last_date)]})
        dm.get_bulletin_metadata.return_value = (last_date, 1, last_date.year)
        # GBT model needed so the quarterly renderer's model-filter passes.
        dm.get_available_models.return_value = {"GBT": "GBT"}
        dm.get_best_models.return_value = []
        dm.resolve_model_values.return_value = []
        dm._data = {
            "hydrograph_day_all": pd.DataFrame({"station_labels": [], "date": []}),
        }

        cfg = MagicMock()
        # Must be True so "month" is a valid option in horizon_selector.
        cfg.display_ML_forecasts = True
        cfg.horizon = "month"
        cfg.viz.create_reload_button.return_value = pn.pane.Str("")

        station_dict = {"Test Basin": [STATION_LABEL]}

        return WidgetManager(dm, cfg, station_dict)

    def test_model_column_readonly_set_by_production_init(self, real_wm):
        """editors['Model'] is None on the real forecast_tabulator_q after __init__."""
        from dashboard import widgets as _widgets
        key = _widgets._("Model")
        editors = real_wm.forecast_tabulator_q.editors
        assert key in editors and editors[key] is None, (
            f"Production __init__ must set editors[{key!r}] = None; "
            "removing the lock from widget_manager.py will fail this test."
        )

    def test_numeric_columns_not_locked(self, real_wm):
        """Numeric forecast columns must not be set to None in editors after __init__."""
        for col in (
            "Forecasted discharge",
            "Forecast lower bound",
            "Forecast upper bound",
        ):
            assert (
                col not in real_wm.forecast_tabulator_q.editors
                or real_wm.forecast_tabulator_q.editors[col] is not None
            ), f"Production __init__ must not lock numeric column '{col}'."

    def test_editors_has_only_model_as_none(self, real_wm):
        """Only the translated Model column should have editor=None after __init__."""
        from dashboard import widgets as _widgets
        key = _widgets._("Model")
        none_cols = [
            k for k, v in real_wm.forecast_tabulator_q.editors.items() if v is None
        ]
        assert none_cols == [key], (
            f"Expected only [{key!r}] to have editor=None, got {none_cols}"
        )

    def test_q_add_help_on_add_buttons(self, real_wm):
        """Add and Add-m0 buttons must carry the quarterly help description after __init__."""
        from dashboard import widgets as _widgets
        expected = _widgets._(
            "For monthly forecasts at reservoir stations, also saves "
            "the displayed quarterly forecast selection."
        )
        assert real_wm.add_to_bulletin_button.description == expected
        assert real_wm.add_to_bulletin_m0_button.description == expected

    def test_editors_survive_quarter_render(self, real_wm):
        """editors['Model'] must remain None after the real quarterly renderer fires."""
        from dashboard import widgets as _widgets
        from dashboard.plot_manager import PlotManager
        from src import vizualization

        key = _widgets._("Model")

        # "month" is a valid option because display_ML_forecasts=True in fixture.
        real_wm.horizon_selector.value = "month"

        site = _make_reservoir_site()
        pm = object.__new__(PlotManager)
        pm._ = _identity
        pm._cfg = types.SimpleNamespace(viz=vizualization)
        pm._wm = real_wm
        pm._dm = types.SimpleNamespace(
            sites_list=[site],
            long_forecasts_quarter=_q_source(station=STATION_LABEL),
        )
        pm.summary_table_q_card = types.SimpleNamespace(visible=True)

        pm.update_quarterly_summary_tabulator()

        editors = real_wm.forecast_tabulator_q.editors
        assert key in editors and editors[key] is None, (
            "editors['Model'] must remain None after renderer updates forecast_tabulator_q."
        )
