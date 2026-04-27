
# dashboard/plot_manager.py
"""
Centralises all plot pane creation, update, tab-rendering logic,
and rendering-related callback wiring.
"""

import panel as pn
import holoviews as hv
from calendar import month_name

from dashboard.logger import setup_logger

logger = setup_logger()


def _ordinal(n: int) -> str:
    """Return day number with English ordinal suffix (1st, 2nd, 3rd, 4th, ...)."""
    if 11 <= (n % 100) <= 13:
        return f"{n}th"
    return f"{n}{['th', 'st', 'nd', 'rd', 'th'][min(n % 10, 4)]}"


def _format_forecast_info(issue_date, horizon_label: str) -> str:
    """Build the info text for a monthly forecast card.

    Args:
        issue_date: The forecast issue date (datetime-like with .month, .day, .year).
        horizon_label: "month_1" or "month_0".
    """
    if horizon_label == "month_1":
        target_month_num = (issue_date.month % 12) + 1
    else:
        target_month_num = issue_date.month
    target = month_name[target_month_num]
    issue_month = month_name[issue_date.month]
    day = _ordinal(issue_date.day)
    return (
        f"Monthly runoff forecast for {target}  \n"
        f"Forecast issue date: {day} of {issue_month} {issue_date.year} ({horizon_label})"
    )


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
    # Callback wiring — call once after layout exists
    # ------------------------------------------------------------------
    def wire(self, dashboard_tabs) -> None:
        """Register all rendering-related callbacks."""
        self._wm.apply_changes_button.on_click(self.update_forecast_plots)

        # Attach the callback to the tabs and station
        dashboard_tabs.param.watch(
            lambda event: self.render_active_tab(dashboard_tabs, event),
            "active",
        )
        dashboard_tabs.param.watch(
            lambda event: self._cfg.viz.update_sidepane_card_visibility(
                dashboard_tabs, self._wm.horizon_card, self._wm.station_card,
                self._wm.forecast_card, self._wm.basin_card, self._wm.reload_card, event,
                horizon_selector=self._wm.horizon_selector,
            ),
            "active",
        )

    # ------------------------------------------------------------------
    # Forecast-tab card visibility (month horizon hides plots)
    # ------------------------------------------------------------------
    def set_forecast_cards_visibility(self, visible: bool, is_month: bool = False) -> None:
        """Show/hide forecast-tab cards.

        visible: True when short-horizon cards (linreg, hydrograph, skill_*) should
                 be shown — pentad/decade only.
        is_month: True only for the month horizon — drives the m0 summary table card.
        """
        for attr in ("linreg_card", "hydrograph_card",
                     "skill_metrics_card", "skill_table_card"):
            card = getattr(self, attr, None)
            if card is not None:
                card.visible = visible
        m0_card = getattr(self, "summary_table_m0_card", None)
        if m0_card is not None and not is_month:
            m0_card.visible = False
        # Adjust summary table height for month vs pentad/decade
        st_card = getattr(self, "summary_table_card", None)
        if st_card is not None:
            if visible:
                st_card.sizing_mode = "stretch_both"
                st_card.height = None
            else:
                st_card.sizing_mode = "stretch_width"
                st_card.height = 600 if len(self._wm.forecast_summary_table.value) > 1 else 240
        # Also toggle the forecast warning pane
        self._wm.forecast_warning.visible = visible

    # ------------------------------------------------------------------
    # Pane factories and initilisation helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _empty_curve():
        return pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
    
    def _markdown(self, text):
        return pn.pane.Markdown(self._(text))
    
    _NO_PRECIP = "No precipitation data from SAPPHIRE Data Gateway available."
    _NO_TEMP = "No temperature data from SAPPHIRE Data Gateway available."
    _NO_SNOW = "No snow data from SAPPHIRE Data Gateway available."

    def _init_daily_panes(self):
        self.daily_hydrograph = self._empty_curve()
        if self._dm.rain is None:
            self.daily_rainfall = self._markdown(self._NO_PRECIP)
            self.daily_temperature = self._markdown(self._NO_TEMP)
        else:
            self.daily_rainfall = self._empty_curve()
            self.daily_temperature = self._empty_curve()

    def _init_snow_panes(self):
        if self._dm.snow_data is None:
            self.snow_plots = self._markdown(self._NO_SNOW)
        else:
            self.snow_plots = {k: self._empty_curve() for k in ("SWE", "HS", "RoF")}

    def _init_forecast_panes(self):
        # Linear regression
        self.forecast_data_and_plot = pn.Column(sizing_mode="stretch_both")

        # Hydrograph
        self.pentad_forecast = self._empty_curve()

        # Forecast skill metrics
        self.effectiveness = self._empty_curve()
        self.accuracy = self._empty_curve()
        self.forecast_skill = pn.Column(self.effectiveness, self.accuracy)

    def _init_skill_table(self):
        horizon = self._wm.horizon_selector.value
        self.skill_table = pn.panel(
            self._cfg.viz.create_skill_table(
                self._, horizon, self._dm.get_forecast_stats_all(horizon)
            ),
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
            station=wm.station_selector.value,
            title_date=wm.date_picker.value,
            model_selection=wm.model_checkbox.value,
            range_type=wm.range_selector.value,
            range_slider=wm.range_slider.value,
            range_visibility=wm.range_radiobutton.value,
        )

    def _build_forecast_hydrograph(self):
        """Build the forecast hydrograph based on current widget state."""
        kw = self._common_plot_kwargs()
        if self._wm.aggregate_radiobutton.value == self._("Yes"):
            return self._cfg.viz.plot_pentad_forecast_hydrograph_data(
                self._,
                horizon=self._wm.horizon_selector.value,
                hydrograph_pentad_all=self._dm.hydrograph_pentad_all,
                **kw,
            )
        return self._cfg.viz.plot_pentad_forecast_hydrograph_data_v2(
            self._,
            horizon=self._wm.horizon_selector.value,
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
        # Initial tabulator fill
        self._cfg.viz.create_forecast_summary_tabulator(
            self._,
            self._wm,
            self._dm.forecasts_all,
            self._wm.station_selector,
            self._wm.date_picker,
            self._wm.model_checkbox,
            self._wm.range_selector,
            self._wm.range_slider,
            self._wm.forecast_tabulator,
        )
        # Update m1 info text for monthly horizon only
        if self._wm.horizon_selector.value == "month":
            df = self._dm.forecasts_all
            if df is not None and not df.empty:
                issue_date = df['date'].max()
                self._wm.forecast_info_m1.object = _format_forecast_info(
                    issue_date, "month_1")
            else:
                self._wm.forecast_info_m1.object = ""
        else:
            self._wm.forecast_info_m1.object = ""

    def update_forecast_tabulator_m0(self):
        """Update the month_0 summary table."""
        m0 = self._dm.long_forecasts_m0
        if m0 is None or m0.empty:
            self.summary_table_m0_card.visible = False
            return
        self.summary_table_m0_card.visible = True
        # Use m0's own max date instead of the shared date_picker
        m0_max_date = m0['date'].max()
        if hasattr(m0_max_date, 'date'):
            m0_max_date = m0_max_date.date()
        self._cfg.viz.create_forecast_summary_tabulator(
            self._,
            self._wm,
            m0,
            self._wm.station_selector,
            m0_max_date,
            self._wm.model_checkbox,
            self._wm.range_selector,
            self._wm.range_slider,
            self._wm.forecast_tabulator_m0,
        )
        # Update m0 info text
        self._wm.forecast_info_m0.object = _format_forecast_info(
            m0_max_date, "month_0")

    # ------------------------------------------------------------------
    # Forecast-tab plots (2nd, 3rd, 4th panels)
    # ------------------------------------------------------------------
    def update_forecast_plots(self, event=None):
        """Refresh the forecast hydrograph and skill plots."""
        # Updates 2nd, 3rd and 4th plots on Forecast tab
        self.pentad_forecast.object = self._build_forecast_hydrograph()

        eff, acc = self._cfg.viz.plot_forecast_skill(
            self._,
            self._wm,
            self._dm.hydrograph_pentad_all,
            self._dm.forecasts_all,
            station_widget=self._wm.station_selector.value,
            date_picker=self._wm.date_picker.value,
            model_checkbox=self._wm.model_checkbox.value,
            range_selection_widget=self._wm.range_selector.value,
            manual_range_widget=self._wm.range_slider.value,
            show_range_button=self._wm.range_radiobutton.value,
        )
        self.effectiveness.object = eff.object
        self.accuracy.object = acc.object

        self.update_forecast_tabulator()

    def update_skill_table(self, event=None):
        """Refresh the all-stations skill metrics table (horizon-scoped).

        Replaces the Tabulator + download menu widgets inside the skill
        table card because column structure differs between pentad and
        decade horizons and the download menu is tied to a specific
        Tabulator instance.
        """
        horizon = self._wm.horizon_selector.value
        new_table = self._cfg.viz.create_skill_table(
            self._, horizon, self._dm.get_forecast_stats_all(horizon)
        )
        new_filename, new_button = new_table.download_menu(
            text_kwargs={"name": self._("Enter filename:"),
                         "value": "forecast_skill_metrics.csv"},
            button_kwargs={"name": self._("Download currently visible table")},
        )

        self.skill_table = new_table
        self.skill_download_filename = new_filename
        self.skill_download_button = new_button

        # Swap the widgets inside the Card's Column (if the card has been built)
        card = getattr(self, "skill_table_card", None)
        if card is not None:
            card[0][:] = [new_table, new_filename, new_button]

    # ------------------------------------------------------------------
    # Full visualisation refresh (used after data reload)
    # ------------------------------------------------------------------
    def refresh_all_visualizations(self):
        """Re-render every forecast-related visualisation."""
        self.update_forecast_plots()
        self.update_skill_table()

    # ------------------------------------------------------------------
    # Tab-activation renderer (lazy rendering per station)
    # ------------------------------------------------------------------
    def render_active_tab(self, dashboard_tabs, event=None):
        """Render plots only when a tab is first activated for a station."""
        if self._wm.horizon_selector.value in ("month", "quarter", "season"):
            return  # long horizons only show summary table
        active = dashboard_tabs.active  # 0 = Predictors, 1 = Forecast
        wm, dm, viz = self._wm, self._dm, self._cfg.viz

        # with pn.io.hold(pn.state.curdoc):
        if active == 0 and dm.should_render_predictors(wm.station_selector.value):
            self._render_predictors_tab(viz, dm, wm)
        elif active == 1 and dm.should_render_forecast(wm.station_selector.value):
            self._render_forecast_tab(viz, dm, wm)

    def _render_predictors_tab(self, viz, dm, wm):
        self.daily_hydrograph.object = viz.plot_daily_hydrograph_data(
            self._, dm, wm, dm.hydrograph_day_all, dm.linreg_predictor,
            wm.station_selector.value, wm.date_picker.value,
        )
        if self._cfg.display_weather_data:
            self.daily_rainfall.object = viz.plot_daily_rainfall_data(
                self._, wm, dm.rain, wm.station_selector.value,
                wm.date_picker.value, dm.linreg_predictor,
            )
            self.daily_temperature.object = viz.plot_daily_temperature_data(
                self._, wm, dm.temp, wm.station_selector.value,
                wm.date_picker.value, dm.linreg_predictor,
            )
        if self._cfg.display_snow_data:
            for var in dm.snow_data.keys():
                if dm.snow_data[var] is not None:
                    self.snow_plots[var].object = viz.plot_daily_snow_data(
                        self._, wm, dm.snow_data, var, wm.station_selector.value,
                        wm.date_picker.value, dm.linreg_predictor,
                        snow_display_start_month=self._cfg.snow_display_start_month,
                        snow_display_start_day=self._cfg.snow_display_start_day,
                    )
                else:
                    self.snow_plots[var].object = pn.pane.Markdown(
                        self._(
                            "No snow data from SAPPHIRE Data Gateway available."
                        )
                    )


    def _render_forecast_tab(self, viz, dm, wm):
        plot = viz.select_and_plot_data(
            self._, dm, wm, dm.linreg_predictor, wm.station_selector.value,
            wm.pentad_selector.value, wm.decad_selector.value,
            self._cfg.save_directory,
        )
        if plot is None:
            self.forecast_data_and_plot[:] = []
            return
        self.forecast_data_and_plot[:] = plot.objects
        self.update_forecast_plots()
