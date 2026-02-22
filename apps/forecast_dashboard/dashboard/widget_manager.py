
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
    # wm.station, wm.model_checkbox, … are ready to use.
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

        # ── Date / period selectors ──────────────────────────────────
        self.date_picker = widgets.create_date_picker(dm.forecasts_all)

        last_date, self.forecast_horizon, self.forecast_year = (
            dm.get_bulletin_metadata()
        )
        self.pentad_selector = widgets.create_pentad_selector(last_date)
        self.decad_selector = widgets.create_decad_selector(last_date)

        # ── Station & basin ──────────────────────────────────────────
        self.station = widgets.create_station(station_dict)
        self.station_card = widgets.create_station_card(self.station)
        self.select_basin = widgets.create_select_basin_widget(station_dict)
        self.basin_card = widgets.create_basin_card(self.select_basin, self.station)

        # ── Model selection ──────────────────────────────────────────
        model_dict = dm.get_filtered_model_dict(
            self.station.value, self.date_picker.value
        )
        self.model_checkbox = widgets.create_model_checkbox(model_dict)
        self._apply_preselected_models()

        # ── Range / display controls ─────────────────────────────────
        (
            self.range_selection,
            self.manual_range,
            self.show_range_button,
        ) = widgets.create_range_widgets()
        self.show_daily_data = widgets.create_show_daily_data_widget()
        self.update_forecast_button = widgets.create_update_forecast_button()

        # ── Forecast card (sidebar) ──────────────────────────────────
        self.forecast_card = widgets.create_forecast_card(
            self.range_selection,
            self.manual_range,
            self.model_checkbox,
            self.show_range_button,
            self.update_forecast_button,
            self.station,
        )
        self.pentad_card = widgets.create_pentad_card(
            self.pentad_selector, self.station
        )

        # ── Forecast summary table ───────────────────────────────────
        self.forecast_tabulator = widgets.create_forecast_tabulator()
        self.forecast_summary_table = widgets.create_forecast_summary_table(
            self.forecast_tabulator
        )

        # ── Warnings ─────────────────────────────────────────────────
        self.predictors_warning = widgets.create_predictors_warning(
            self.station, dm._data
        )
        self.forecast_warning = widgets.create_forecast_warning(
            self.station, dm._data, self.date_picker.value
        )

        # ── Bulletin widgets ─────────────────────────────────────────
        self.add_to_bulletin_button = widgets.create_add_to_bulletin_button()
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

        # ── Download / language / misc ───────────────────────────────
        self.downloader, self.bulletin_download_panel = (
            widgets.create_downloader_and_panel(cfg.horizon)
        )
        self.language_buttons = widgets.create_language_buttons()
        self.message_pane = widgets.create_message_pane(dm._data)
        self.reload_card = cfg.viz.create_reload_button()

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

    def _wire_station_period_change(self, dm: DataManager, pm: PlotManager) -> None:
        @pn.depends(self.station, self.pentad_selector, self.decad_selector, watch=True)
        def _on_change(station_value, selected_pentad, selected_decad):
            """Reload data for the new station and refresh the model checkbox."""
            _ = self._gettext
            dm.load_station(station_value.split()[0])
            dm.update_sites_for_pentad(_, selected_pentad, selected_decad)
            dm.invalidate_render_cache()

            self.refresh_warnings()
            self.refresh_model_checkbox()
            pm.render_active_tab(self._dashboard_tabs)

        # prevent GC
        self._on_station_or_period_changed = _on_change

    def _wire_range_slider_visibility(self) -> None:
        # Update the widgets conditional on the active tab
        self.range_selection.param.watch(
            lambda event: self._cfg.viz.update_range_slider_visibility(
                self._gettext, self.manual_range, event,
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
            site_selection=self.station,
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
        model_dict = self._dm.get_filtered_model_dict(
            self.station.value, self.date_picker.value
        )
        print("\nAfter update_model_dict:")
        print(f"  Updated model dict: {model_dict}")

        # Get pre-selected models
        preselected = self._dm.get_preselected_models(
            self.station.value,
            self.pentad_selector.value,
            self.decad_selector.value,
        )
        print("\nAfter get_best_models:")
        print(f"  Pre-selected models: {preselected}")

        # Create new values list        
        new_values = self._dm.resolve_model_values(model_dict, preselected)

        print("\nBefore widget update:")
        print(f"  New options to set: {model_dict}")
        print(f"  New values to set: {new_values}")

        with pn.io.hold(pn.state.curdoc):
            # Try updating options first, then values
            self.model_checkbox.options = model_dict
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
            list(model_dict.keys()),
            new_values,
        )
        return model_dict

    def refresh_warnings(self) -> None:
        """Re-evaluate predictor & forecast warnings for the active station."""
        widgets.refresh_predictors_warning(
            self.predictors_warning, self.station, self._dm._data
        )
        widgets.refresh_forecast_warning(
            self.forecast_warning,
            self.station,
            self._dm._data,
            self.date_picker.value,
        )

    # ------------------------------------------------------------------
    # Convenience: all widgets auth needs to track for inactivity
    # ------------------------------------------------------------------
    def trackable_widgets(self) -> list[tuple]:
        return [
            (self.station, "value"),
            (self.model_checkbox, "value"),
            (self.range_selection, "value"),
            (self.show_range_button, "value"),
            (self.show_daily_data, "value"),
            (self.select_basin, "value"),
            (self.add_to_bulletin_button, "clicks"),
            (self.write_bulletin_button, "clicks"),
            (self.remove_bulletin_button, "clicks"),
            (self.forecast_tabulator, "selection"),
        ]

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _apply_preselected_models(self) -> None:
        preselected = self._dm.get_preselected_models(
            self.station.value,
            self.pentad_selector.value,
            self.decad_selector.value,
        )
        model_dict = self.model_checkbox.options
        self.model_checkbox.value = self._dm.resolve_model_values(
            model_dict, preselected
        )
