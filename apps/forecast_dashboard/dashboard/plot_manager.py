
# dashboard/plot_manager.py
"""Centralises all plot pane creation, update, and tab-rendering logic."""

import panel as pn
import holoviews as hv

from dashboard.logger import setup_logger

logger = setup_logger()


class PlotManager:
    """Owns every plot pane and knows how to (re-)render them."""

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    def __init__(self, dm, wm, cfg, gettext):
        self._dm = dm
        self._wm = wm
        self._cfg = cfg
        self._ = gettext  # i18n helper

        # --- pane creation ---
        self._init_daily_panes()
        self._init_snow_panes()
        self._init_forecast_panes()
        self._init_skill_table()
    
    # ------------------------------------------------------------------
    # Pane initialisation helpers
    # ------------------------------------------------------------------
    def _init_daily_panes(self):
        self.daily_hydrograph = pn.pane.HoloViews(
            hv.Curve([]), sizing_mode="stretch_both"
        )
        if self._dm.rain is None:
            self.daily_rainfall = pn.pane.Markdown(
                self._(
                    "No precipitation data from SAPPHIRE Data Gateway available."
                )
            )
            self.daily_temperature = pn.pane.Markdown(
                self._(
                    "No temperature data from SAPPHIRE Data Gatway available."
                )
            )
        else:
            self.daily_rainfall = pn.pane.HoloViews(
                hv.Curve([]), sizing_mode="stretch_width"
            )
            self.daily_temperature = pn.pane.HoloViews(
                hv.Curve([]), sizing_mode="stretch_width"
            )

    def _init_snow_panes(self):
        if self._dm.snow_data is None:
            self.snow_plots = pn.pane.Markdown(
                self._("No snow data from SAPPHIRE Data Gateway available.")
            )
        else:
            self.snow_plots = {
                k: pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width")
                for k in ("SWE", "HS", "RoF")
            }

    def _init_forecast_panes(self):
        self.forecast_data_and_plot = pn.Column(sizing_mode="stretch_both")
        self.pentad_forecast = pn.pane.HoloViews(
            hv.Curve([]), sizing_mode="stretch_both"
        )
        self.effectiveness = pn.pane.HoloViews(
            hv.Curve([]), sizing_mode="stretch_both"
        )
        self.accuracy = pn.pane.HoloViews(
            hv.Curve([]), sizing_mode="stretch_both"
        )
        self.forecast_skill = pn.Column(self.effectiveness, self.accuracy)

    def _init_skill_table(self):
        self.skill_table = pn.panel(
            self._cfg.viz.create_skill_table(self._, self._dm.forecast_stats),
            sizing_mode="stretch_width",
        )
        (
            self.skill_download_filename,
            self.skill_download_button,
        ) = self.skill_table.download_menu(
            text_kwargs={
                "name": self._("Enter filename:"),
                "value": "forecast_skill_metrics.csv",
            },
            button_kwargs={
                "name": self._("Download currently visible table"),
            },
        )
    
    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------
    def _common_plot_kwargs(self) -> dict:
        """Keyword bundle consumed by most visualisation helpers."""
        wm = self._wm
        return dict(
            forecasts_all=self._dm.forecasts_all,
            station=wm.station.value,
            title_date=wm.date_picker.value,
            model_selection=wm.model_checkbox.value,
            range_type=wm.range_selection.value,
            range_slider=wm.manual_range.value,
            range_visibility=wm.show_range_button.value,
        )
    
    def _build_forecast_hydrograph(self):
        """Build the forecast hydrograph based on current widget state."""
        kw = self._common_plot_kwargs()
        if self._wm.show_daily_data.value == self._("Yes"):
            return self._cfg.viz.plot_pentad_forecast_hydrograph_data(
                self._,
                hydrograph_pentad_all=self._dm.hydrograph_pentad_all,
                **kw,
            )
        return self._cfg.viz.plot_pentad_forecast_hydrograph_data_v2(
            self._,
            hydrograph_day_all=self._dm.hydrograph_day_all,
            linreg_predictor=self._dm.linreg_predictor,
            rram_forecast=self._dm.rram_forecast,
            ml_forecast=self._dm.ml_forecast,
            **kw,
        )
    
    # ------------------------------------------------------------------
    # Forecast tabulator
    # ------------------------------------------------------------------
    def update_forecast_tabulator(self):
        self._cfg.viz.create_forecast_summary_tabulator(
            self._,
            self._dm.forecasts_all,
            self._wm.station,
            self._wm.date_picker,
            self._wm.model_checkbox,
            self._wm.range_selection,
            self._wm.manual_range,
            self._wm.forecast_tabulator,
        )

    # ------------------------------------------------------------------
    # Forecast-tab plots (2nd, 3rd, 4th panels)
    # ------------------------------------------------------------------
    def update_forecast_plots(self, event=None):
        """Refresh the forecast hydrograph and skill plots."""
        # Updates 2nd, 3rd and 4th plots on Forecast tab
        self.pentad_forecast.object = self._build_forecast_hydrograph()

        eff, acc = self._cfg.viz.plot_forecast_skill(
            self._,
            self._dm.hydrograph_pentad_all,
            self._dm.forecasts_all,
            station_widget=self._wm.station.value,
            date_picker=self._wm.date_picker.value,
            model_checkbox=self._wm.model_checkbox.value,
            range_selection_widget=self._wm.range_selection.value,
            manual_range_widget=self._wm.manual_range.value,
            show_range_button=self._wm.show_range_button.value,
        )
        self.effectiveness.object = eff.object
        self.accuracy.object = acc.object

        self.update_forecast_tabulator()

    # ------------------------------------------------------------------
    # Full visualisation refresh (used after data reload)
    # ------------------------------------------------------------------
    def refresh_all_visualizations(self):
        """Re-render every forecast-related visualisation."""
        # Re-bind the plots to use the updated data
        kw = self._common_plot_kwargs()

        #print('---   ---plot_pentad_forecast_hydrograph_data---   ---')
        self._cfg.viz.plot_pentad_forecast_hydrograph_data(
            self._,
            hydrograph_pentad_all=self._dm.hydrograph_pentad_all,
            **kw,
        )
        #print('---   ---done with plot_pentad_forecast_hydrograph_data---   ---')
        #print('---   ---plot_pentad_forecast_hydrograph_data_v2---   ---')
        self._cfg.viz.plot_pentad_forecast_hydrograph_data_v2(
            self._,
            hydrograph_day_all=self._dm.hydrograph_day_all,
            linreg_predictor=self._dm.linreg_predictor,
            rram_forecast=self._dm.rram_forecast,
            ml_forecast=self._dm.ml_forecast,
            **kw,
        )
        #print('---   ---done with plot_pentad_forecast_hydrograph_data_v2---   ---')
        #print('---   ---update_forecast_tabulator---   ---')
        self.update_forecast_tabulator()
        #print('---   ---done with update_forecast_tabulator---   ---')

    # ------------------------------------------------------------------
    # Tab-activation renderer (lazy rendering per station)
    # ------------------------------------------------------------------
    def render_active_tab(self, dashboard_tabs, event=None):
        """Render plots only when a tab is first activated for a station."""
        active = dashboard_tabs.active  # 0 = Predictors, 1 = Forecast
        wm, dm, viz = self._wm, self._dm, self._cfg.viz

        with pn.io.hold(pn.state.curdoc):
            if active == 0 and dm.should_render_predictors(wm.station.value):
                self._render_predictors_tab(viz, dm, wm)
            elif active == 1 and dm.should_render_forecast(wm.station.value):
                self._render_forecast_tab(viz, dm, wm)

    def _render_predictors_tab(self, viz, dm, wm):
        self.daily_hydrograph.object = viz.plot_daily_hydrograph_data(
            self._, dm.hydrograph_day_all, dm.linreg_predictor,
            wm.station.value, wm.date_picker.value,
        )
        if self._cfg.display_weather_data:
            self.daily_rainfall.object = viz.plot_daily_rainfall_data(
                self._, dm.rain, wm.station.value,
                wm.date_picker.value, dm.linreg_predictor,
            )
            self.daily_temperature.object = viz.plot_daily_temperature_data(
                self._, dm.temp, wm.station.value,
                wm.date_picker.value, dm.linreg_predictor,
            )
        if self._cfg.display_snow_data:
            for var in dm.snow_data.keys():
                if dm.snow_data[var] is not None:
                    self.snow_plots[var].object = viz.plot_daily_snow_data(
                        self._, dm.snow_data, var, wm.station.value,
                        wm.date_picker.value, dm.linreg_predictor,
                    )
                else:
                    self.snow_plots[var].object = pn.pane.Markdown(
                        self._(
                            "No snow data from SAPPHIRE Data Gateway available."
                        )
                    )

    def _render_forecast_tab(self, viz, dm, wm):
        plot = viz.select_and_plot_data(
            self._, dm.linreg_predictor, wm.station.value,
            wm.pentad_selector.value, wm.decad_selector.value,
            self._cfg.save_directory,
        )
        self.forecast_data_and_plot[:] = plot.objects
        self.update_forecast_plots()
