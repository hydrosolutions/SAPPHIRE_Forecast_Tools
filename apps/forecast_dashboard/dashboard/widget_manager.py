
# dashboard/widget_manager.py
"""
Centralised owner of every dashboard widget instance.

Responsibilities
────────────────
• Create all widgets once, with correct initial values.
• Expose thin helpers that callbacks use to refresh widget state.
• Wire widget-triggered callbacks (station change, range slider, etc.).
• Keep zero business-logic — that stays in DataManager / config.viz.

Usage in forecast_dashboard.py
──────────────────────────────
    wm = WidgetManager(dm, cfg, station_dict)
    wm.wire(dm, pm, dashboard_tabs, gettext=_)
    # wm.station_selector, wm.model_checkbox, … are ready to use.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import panel as pn

from dashboard import widgets
from src.site import SapphireSite as Site

if TYPE_CHECKING:
    from dashboard.config import DashboardConfig
    from dashboard.data_manager import DataManager
    from dashboard.plot_manager import PlotManager

from dashboard.logger import setup_logger

logger = setup_logger()


class WidgetManager:
    """Single source of truth for every dashboard widget."""

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    def __init__(self, dm: DataManager, cfg: DashboardConfig, station_dict: dict) -> None:
        self._dm = dm
        self._cfg = cfg

        # === SIDEBAR WIDGETS ===
        # ── Invisible ───────────────────────────────────────────────────────
        self.date_picker = widgets.create_date_picker(dm.forecasts_all)

        # ── Hydropost ───────────────────────────────────────────────────────
        self.station_selector = widgets.create_station_selector(station_dict)
        width = self.station_selector.width
        self.station_card = widgets.create_station_card(self.station_selector)

        # ── Horizon ─────────────────────────────────────────────────────────
        self.horizon_selector = widgets.create_horizon_selector()
        self.horizon_card = widgets.create_horizon_card(self.horizon_selector, width)

        # ── Invisible ───────────────────────────────────────────────────────
        last_date, self.forecast_horizon, self.forecast_year = (
            dm.get_bulletin_metadata(self.horizon_selector.value)
        )
        self.pentad_selector = widgets.create_pentad_selector(last_date)
        self.decad_selector = widgets.create_decad_selector(last_date)

        # ── Forecast configuration ──────────────────────────────────────────
        available_models = dm.get_available_models(
            self.station_selector.value, self.date_picker.value
        )
        self.model_checkbox = widgets.create_model_checkbox(available_models)
        self._apply_best_models()

        (
            self.range_selector,
            self.range_slider,
            self.range_radiobutton,
        ) = widgets.create_range_widgets()

        self.apply_changes_button = widgets.create_apply_changes_button()

        self.forecast_card = widgets.create_forecast_card(
            self.range_selector,
            self.range_slider,
            self.model_checkbox,
            self.range_radiobutton,
            self.apply_changes_button,
            width,
        )

        # ── Message ─────────────────────────────────────────────────────────
        self.message_pane = widgets.create_message_pane(dm._data)

        # ── Basin ───────────────────────────────────────────────────────────
        self.basin_selector = widgets.create_basin_selector(station_dict)
        self.basin_card = widgets.create_basin_card(self.basin_selector, width)

        # ── Manual re-run of latest forecasts ───────────────────────────────
        self.reload_card = cfg.viz.create_reload_button(self.horizon_selector.value)

        # === PREDICTORS TAB WIDGETS ===
        # ── Warning ─────────────────────────────────────────────────────────
        self.predictors_warning = widgets.create_predictors_warning(
            self.station_selector, dm._data
        )

        # === FORECAST TAB WIDGETS ===
        # ── Warning ─────────────────────────────────────────────────────────
        self.forecast_warning = widgets.create_forecast_warning(
            self.station_selector, dm._data, self.date_picker.value
        )

        # ── Summary table ───────────────────────────────────────────────────
        self.add_to_bulletin_button = widgets.create_add_to_bulletin_button()
        self.forecast_tabulator = widgets.create_forecast_tabulator()
        self.forecast_summary_table = widgets.create_forecast_summary_table(
            self.forecast_tabulator
        )

        # ── Summary table (month_0) ────────────────────────────────────────
        self.forecast_tabulator_m0 = widgets.create_forecast_tabulator()
        self.forecast_summary_table_m0 = widgets.create_forecast_summary_table(
            self.forecast_tabulator_m0
        )
        self.add_to_bulletin_m0_button = widgets.create_add_to_bulletin_button()
        self.add_to_bulletin_m0_popup = widgets.create_add_to_bulletin_popup()
        self.forecast_info_m1 = widgets.create_forecast_info_pane()
        self.forecast_info_m0 = widgets.create_forecast_info_pane()

        # ── Hydrograph ──────────────────────────────────────────────────────
        self.aggregate_radiobutton = widgets.create_aggregate_radiobutton()

        # === BULLETIN TAB WIDGETS ===
        # ── Forecast bulletin ───────────────────────────────────────────────
        self.remove_bulletin_button, self.write_bulletin_button = (
            widgets.create_bulletin_buttons()
        )
        self.bulletin_tabulator = widgets.create_bulletin_tabulator()
        self.add_to_bulletin_popup = widgets.create_add_to_bulletin_popup()
        self.bulletin_table = widgets.create_bulletin_table(
            self.bulletin_tabulator,
            self.remove_bulletin_button,
            self.add_to_bulletin_popup,
        )

        # ── Download bulletin ───────────────────────────────────────────────
        self.downloader, self.bulletin_download_panel = (
            widgets.create_downloader_and_panel(cfg.horizon)
        )

        # === NAVBAR WIDGETS ===
        self.language_buttons = widgets.create_language_buttons()

        # --- Post-load callbacks (registered by other managers) ---
        self._post_load_callbacks: list = []

    # ------------------------------------------------------------------
    # Post-load callback registration
    # ------------------------------------------------------------------
    def register_post_load_callback(self, fn) -> None:
        """Register a callable to be invoked after each station/period load."""
        self._post_load_callbacks.append(fn)

    # ------------------------------------------------------------------
    # Callback wiring — call once after PlotManager & layout exist
    # ------------------------------------------------------------------
    def wire(self, dm: DataManager, pm: PlotManager, dashboard_tabs, *, gettext) -> None:
        """Register all widget-triggered callbacks."""
        self._pm = pm
        self._gettext = gettext
        self._dashboard_tabs = dashboard_tabs

        self._wire_station_period_change(dm, pm)
        self._wire_range_slider_visibility()
        self._wire_site_object_binding(dm)
        # Skill metrics table depends only on horizon, not on station
        self.horizon_selector.param.watch(pm.update_skill_table, "value")

    def _wire_station_period_change(self, dm: DataManager, pm: PlotManager) -> None:
        @pn.depends(self.horizon_selector, self.station_selector, self.pentad_selector, self.decad_selector, watch=True)
        def _on_change(horizon, station_value, selected_pentad, selected_decad):
            """Reload data for the new station and refresh the model checkbox."""
            _ = self._gettext
            is_month = horizon == "month"
            dm.load_station(horizon, station_value.split()[0])
            dm.update_sites_for_pentad(_, horizon, selected_pentad, selected_decad)
            dm.invalidate_render_cache()

            if not dm.forecasts_all.empty:
                max_date = dm.forecasts_all['date'].max()
                if hasattr(max_date, 'date'):
                    self.date_picker.value = max_date.date()

            self.refresh_model_checkbox()
            if is_month:
                pm.update_forecast_tabulator()
                pm.update_forecast_tabulator_m0()
            else:
                self.refresh_warnings()
                pm.render_active_tab(self._dashboard_tabs)
            pm.set_forecast_cards_visibility(not is_month)
            # Only show forecast_card on Forecast tab and non-month horizon
            is_forecast_tab = self._dashboard_tabs.active == 1
            self.forecast_card.visible = is_forecast_tab and not is_month

            try:
                _last_date, self.forecast_horizon, self.forecast_year = (
                    dm.get_bulletin_metadata(horizon)
                )
            except (KeyError, IndexError, TypeError, ValueError):
                pass  # no data for this horizon yet; bulletin callback handles it

            for cb in self._post_load_callbacks:
                cb()

        # prevent GC
        self._on_station_or_period_changed = _on_change

    def _wire_range_slider_visibility(self) -> None:
        # Update the widgets conditional on the active tab
        self.range_selector.param.watch(
            lambda event: self._cfg.viz.update_range_slider_visibility(
                self._gettext, self.range_slider, event,
            ),
            "value",
        )

    def _wire_site_object_binding(self, dm: DataManager) -> None:
        # Update the site object based on site and forecast selection
        # --- Site object binding ---
        self._site_binding = pn.bind(
            Site.get_site_attributes_from_selected_forecast,
            _=self._gettext,
            sites=dm.sites_list,
            site_selection=self.station_selector,
            tabulator=self.forecast_tabulator,
        )

    # ------------------------------------------------------------------
    # Public helpers called by callbacks
    # ------------------------------------------------------------------
    def refresh_model_checkbox(self) -> dict:
        """Recompute model options & pre-selection for the current station/period."""
        print("\n=== Starting Model Select Update ===")
        print(f"Initial widget state:")
        print(f"  Options: {self.model_checkbox.options}")
        print(f"  Current value: {self.model_checkbox.value}")
        
        # First get the updated model dictionary
        available_models = self._dm.get_available_models(
            self.station_selector.value, self.date_picker.value,
            horizon=self.horizon_selector.value,
        )
        print("\nAfter update_model_dict:")
        print(f"  Updated model dict: {available_models}")

        # Get pre-selected models
        best_models = self._dm.get_best_models(
            self.horizon_selector.value,
            self.station_selector.value,
            self.pentad_selector.value,
            self.decad_selector.value,
        )
        print("\nAfter get_best_models:")
        print(f"  Pre-selected models: {best_models}")

        # Create new values list        
        new_values = self._dm.resolve_model_values(available_models, best_models)

        print("\nBefore widget update:")
        print(f"  New options to set: {available_models}")
        print(f"  New values to set: {new_values}")

        # with pn.io.hold(pn.state.curdoc):
        # Try updating options first, then values
        self.model_checkbox.options = available_models
        self.model_checkbox.value = new_values
        # model_checkbox.param.trigger('options')
        # model_checkbox.param.trigger('value')
        
        print("\nAfter options update:")
        print(f"  Widget options: {self.model_checkbox.options}")
        print(f"  Widget value: {self.model_checkbox.value}")

        print("\nFinal widget state:")
        print(f"  Widget options: {self.model_checkbox.options}")
        print(f"  Widget value: {self.model_checkbox.value}")

        logger.debug(
            "Model checkbox refreshed — options=%s, values=%s",
            list(available_models.keys()),
            new_values,
        )
        return available_models

    def refresh_warnings(self) -> None:
        """Re-evaluate predictor & forecast warnings for the active station."""
        widgets.refresh_predictors_warning(
            self.predictors_warning, self.station_selector, self._dm._data
        )
        widgets.refresh_forecast_warning(
            self.forecast_warning,
            self.station_selector,
            self._dm._data,
            self.date_picker.value,
        )

    # ------------------------------------------------------------------
    # Convenience: all widgets auth needs to track for inactivity
    # ------------------------------------------------------------------
    def trackable_widgets(self) -> list[tuple]:
        return [
            (self.station_selector, "value"),
            (self.model_checkbox, "value"),
            (self.range_selector, "value"),
            (self.range_radiobutton, "value"),
            (self.aggregate_radiobutton, "value"),
            (self.basin_selector, "value"),
            (self.add_to_bulletin_button, "clicks"),
            (self.add_to_bulletin_m0_button, "clicks"),
            (self.write_bulletin_button, "clicks"),
            (self.remove_bulletin_button, "clicks"),
            (self.forecast_tabulator, "selection"),
        ]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _apply_best_models(self) -> None:
        best_models = self._dm.get_best_models(
            self.horizon_selector.value,
            self.station_selector.value,
            self.pentad_selector.value,
            self.decad_selector.value,
        )
        available_models = self.model_checkbox.options
        self.model_checkbox.value = self._dm.resolve_model_values(
            available_models, best_models
        )
