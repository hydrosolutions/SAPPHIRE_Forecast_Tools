# forecast_dashboard.py
#
# This script creates a dashboard for the pentadal forecast.
#
# Run with the following command:
# ieasyhydroforecast_data_root_dir=/absolute/path/to ieasyhydroforecast_env_file_path=/absolute/path/to/sensitive_data_forecast_tools/config/.env_develop_kghm sapphire_forecast_horizon=pentad SAPPHIRE_OPDEV_ENV=True panel serve forecast_dashboard.py --show --autoreload --port 5055

# =========================
# Standard library imports
# =========================
from functools import partial
from concurrent.futures import ThreadPoolExecutor

# =========================
# Third-party imports
# =========================
import panel as pn
import holoviews as hv

# =========================
# Local application imports
# =========================
from src.gettext_config import _
import src.processing as processing
from src.site import SapphireSite as Site
from src.bulletins import write_to_excel
import src.layout as layout

from dashboard.logger import setup_logger
from dashboard.widget_manager import WidgetManager
from dashboard import widgets
from dashboard.bulletin_manager import load_bulletin_from_csv, add_current_selection_to_bulletin, remove_selected_from_bulletin, handle_bulletin_write, create_bulletin_table
from dashboard import config
from dashboard.data_manager import DataManager


logger = setup_logger()

# =====================================================================
# 1. Configuration & environment
# =====================================================================
cfg = config.init_dashboard(pn)

# =====================================================================
# 2. Station metadata & DataManager initialisation
# =====================================================================
valid_codes = ['16159', '16159', '16159', '16101', '16159', '16159', '15054', '16101', '16134', '16101', '16101', '16134', '16134', '15081', '15054', '16105', '16101', '16105', '16100', '16105', '15054', '16160', '15149', '15149', '15149', '15149', '15149', '16105', '15149', '16100', '16100', '16100', '16160', '16160', '16101', '16159', '16100', '15054', '15034', '16139', '16143', '16143', '15051', '15051', '15051', '15051', '16151', '15051', '15081', '15081', '16135', '16135', '16135', '15034', '15034', '16160', '16134', '16143', '16143', '16143', '16139', '16139', '16139', '16139', '16151', '16151', '16151', '16151', '16151', '15054', '15054', '15051', '16143', '16146', '16146', '16146', '16146', '16139', '16160', '15283', '16105', '15215', '15215', '15215', '15261', '16936', '15216', '15216', '15215', '15216', '15216', '15216', '15256', '15256', '15256', '15256', '16936', '15216', '15215', '15259', '15215', '15034', '15259', '15259', '16936', '16936', '16936', '16936', '15259', '15259', '15259', '15013', '15283', '15013', '15013', '15013', '15013', '15013', '15283', '16160', '15283', '15090', '16127', '16127', '16127', '16127', '16127', '16127', '16158', '16134', '16158', '16158', '16158', '16158', '15090', '15102', '15102', '16105', '16158', '16134', '15090', '15283', '15090', '16121', '16121', '16121', '16121', '16121', '16121', '15030', '15030', '15030', '15030', '15102', '15102', '15102', '15102', '15090', '15090', '15256', '15034', '15287', '15083', '16096', '16096', '16096', '16096', '16096', '15171', '16161', '15025', '15171', '15171', '15171', '15171', '16146', '15189', '15189', '15960', '15171', '16096', '16161', '16161', '15189', '16161', '16161', '16070', '16070', '16070', '16070', '16070', '16070', '16169', '16169', '16169', '16169', '16169', '16169', '15189', '16161', '15189', '15025', '15194', '15194', '16068', '15020', '16059', '15020', '15020', '15020', '16059', '16068', '16059', '15256', '16146', '17462', '17462', '17462', '17462', '17462', '16059', '16068', '16068', '16068', '16055', '16055', '16055', '16055', '16055', '16055', '16176', '16176', '16176', '16176', '16176', '16176', '15194', '16059', '16059', '16068', '15189', '15194', '15034', '15214', '15287', '16153', '16153', '16100', '15025', '15025', '15016', '15954', '16153', '15954', '15212', '15212', '15212', '15212', '15212', '16487', '16487', '15212', '16153', '16153', '16136', '15083', '15083', '15083', '15083', '15083', '16135', '16135', '16135', '16136', '15081', '15081', '15081', '16136', '16136', '16136', '16136', '16153', '16487', '15287', '16487', '16487', '16681', '15283', '15214', '15214', '15214', '15312', '15016', '16510', '15312', '15312', '15312', '15312', '15287', '15214', '15287', '15287', '15312', '16510', '16510', '16510', '15016', '15954', '15954', '15954', '15954', '15016', '15016', '15016', '15214', '15285', '15285', '15285', '15285', '15285', '15285', '16510', '16510', '16487', '17462']
all_stations, station_dict = processing.get_all_stations_from_file(valid_codes)

dm = DataManager(
    all_stations=all_stations,
    valid_codes=valid_codes,
    horizon=cfg.horizon,
    horizon_in_year=cfg.horizon_in_year,
)
dm.load_station('15189')

# =====================================================================
# 3. Widgets
# =====================================================================
wm = WidgetManager(dm, cfg, station_dict)

# =====================================================================
# 4. Initial site attribute computation
# =====================================================================
dm.update_sites_for_pentad(_, wm.pentad_selector.value, wm.decad_selector.value)

# =====================================================================
# 5. Callbacks
# =====================================================================
# Watch for changes in pipeline_running and update the add_to_bulletin_button
cfg.viz.app_state.param.watch(wm.sync_add_button_to_pipeline, 'pipeline_running')

# Set the initial state of the button based on whether the pipeline is running
wm.add_to_bulletin_button.disabled = cfg.viz.app_state.pipeline_running


@pn.depends(wm.station, wm.pentad_selector, wm.decad_selector, watch=True)
def on_station_or_period_changed(station_value, selected_pentad, selected_decad):
    """Reload data for the new station and refresh the model checkbox."""
    dm.load_station(station_value.split()[0]) # Pass the station code
    dm.update_sites_for_pentad(_, selected_pentad, selected_decad)
    dm.invalidate_render_cache()

    wm.refresh_warnings()
    wm.refresh_model_checkbox()

    update_active_tab(None)


# =====================================================================
# 6. Bulletin management
# =====================================================================
bulletin_sites = load_bulletin_from_csv(wm.forecast_year, wm.forecast_horizon, cfg.save_directory, dm.sites_list)

_update_bulletin = partial(wm.update_bulletin_table, bulletin_sites)
_update_bulletin()
wm.select_basin.param.watch(lambda event: _update_bulletin(), 'value')

wm.add_to_bulletin_button.on_click(
    partial(
        add_current_selection_to_bulletin,
        viz=cfg.viz,
        forecast_tabulator=wm.forecast_tabulator,
        station=wm.station,
        sites_list=dm.sites_list,
        add_to_bulletin_popup=wm.add_to_bulletin_popup,
        bulletin_sites=bulletin_sites,
        forecast_year_for_saving_bulletin=wm.forecast_year,
        forecast_horizon_for_saving_bulletin=wm.forecast_horizon,
        save_directory=cfg.save_directory,
        update_bulletin_table=_update_bulletin
    )
)

# Attach the remove function to the remove button click event
wm.remove_bulletin_button.on_click(
    partial(
        remove_selected_from_bulletin,
        bulletin_tabulator=wm.bulletin_tabulator,
        bulletin_sites=bulletin_sites,
        add_to_bulletin_popup=wm.add_to_bulletin_popup,
        forecast_year_for_saving_bulletin=wm.forecast_year,
        forecast_horizon_for_saving_bulletin=wm.forecast_horizon,
        save_directory=cfg.save_directory,
        update_bulletin_table=_update_bulletin
    )
)

# =====================================================================
# 7. Plot panes & forecast update logic
# =====================================================================

# Initial setup: populate the main area with the initial selection
daily_hydrograph_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
if dm.rain is None: 
    daily_rainfall_plot = pn.pane.Markdown(_("No precipitation data from SAPPHIRE Data Gateway available."))
    daily_temperature_plot = pn.pane.Markdown(_("No temperature data from SAPPHIRE Data Gatway available."))
else: 
    daily_rainfall_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width") 
    daily_temperature_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width") 
if dm.snow_data is None:
    snow_plot_panes = pn.pane.Markdown(_("No snow data from SAPPHIRE Data Gateway available."))
else:
    snow_plot_panes = {
        k: pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width")
        for k in ('SWE', 'HS', 'RoF')
    }

forecast_data_and_plot = pn.Column(sizing_mode="stretch_both")
pentad_forecast_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
effectiveness_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
accuracy_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
forecast_skill_plot = pn.Column(effectiveness_plot, accuracy_plot)

def _common_plot_kwargs() -> dict:
    """Shared keyword bundle consumed by most viz helpers."""
    return dict(
        forecasts_all=dm.forecasts_all,
        station=wm.station.value,
        title_date=wm.date_picker.value,
        model_selection=wm.model_checkbox.value,
        range_type=wm.range_selection.value,
        range_slider=wm.manual_range.value,
        range_visibility=wm.show_range_button.value,
    )

def _build_forecast_hydrograph():
    """Build the forecast hydrograph based on current widget state."""
    kw = _common_plot_kwargs()
    if wm.show_daily_data.value == _('Yes'):
        return cfg.viz.plot_pentad_forecast_hydrograph_data(
            _, hydrograph_pentad_all=dm.hydrograph_pentad_all, **kw,
        )
    else:
        return cfg.viz.plot_pentad_forecast_hydrograph_data_v2(
            _,
            hydrograph_day_all=dm.hydrograph_day_all,
            linreg_predictor=dm.linreg_predictor,
            rram_forecast=dm.rram_forecast,
            ml_forecast=dm.ml_forecast,
            **kw,
        )


def update_forecast_plots(event):
    """Updates 2nd, 3rd and 4th plots on Forecast tab"""
    pentad_forecast_plot.object = _build_forecast_hydrograph()
    eff, acc = cfg.viz.plot_forecast_skill(
        _,
        dm.hydrograph_pentad_all,
        dm.forecasts_all,
        station_widget=wm.station.value,
        date_picker=wm.date_picker.value,
        model_checkbox=wm.model_checkbox.value,
        range_selection_widget=wm.range_selection.value,
        manual_range_widget=wm.manual_range.value,
        show_range_button=wm.show_range_button.value
    )
    effectiveness_plot.object = eff.object
    accuracy_plot.object = acc.object

    _update_forecast_tabulator()

wm.update_forecast_button.on_click(update_forecast_plots)

# --- Skill table ---
skill_table = pn.panel(
    cfg.viz.create_skill_table(_, dm.forecast_stats),
    sizing_mode='stretch_width')

skill_metrics_download_filename, skill_metrics_download_button = skill_table.download_menu(
    text_kwargs={'name': _('Enter filename:'), 'value': 'forecast_skill_metrics.csv'},
    button_kwargs={'name': _('Download currently visible table')}
)


def _update_forecast_tabulator():
    cfg.viz.create_forecast_summary_tabulator(
        _, dm.forecasts_all, wm.station, wm.date_picker,
        wm.model_checkbox, wm.range_selection, wm.manual_range,
        wm.forecast_tabulator
    )

# --- Initial tabulator fill ---
_update_forecast_tabulator()

# Update the site object based on site and forecast selection
#print(f"DEBUG: forecast_dashboard.py: forecast_tabulator: {forecast_summary_tabulator}")
# --- Site object binding ---
update_site_object = pn.bind(
    Site.get_site_attributes_from_selected_forecast,
    _=_,
    sites=dm.sites_list,
    site_selection=wm.station,
    tabulator=wm.forecast_tabulator)

# =====================================================================
# 8. Data reload watcher
# =====================================================================

def _update_visualizations():
    # Re-bind the plots to use the updated data
    kw = _common_plot_kwargs()
    #print('---   ---plot_pentad_forecast_hydrograph_data---   ---')
    cfg.viz.plot_pentad_forecast_hydrograph_data(
        _, hydrograph_pentad_all=dm.hydrograph_pentad_all, **kw,
    )
    #print('---   ---done with plot_pentad_forecast_hydrograph_data---   ---')

    #print('---   ---plot_pentad_forecast_hydrograph_data_v2---   ---')
    cfg.viz.plot_pentad_forecast_hydrograph_data_v2(
        _, hydrograph_day_all=dm.hydrograph_day_all,
        linreg_predictor=dm.linreg_predictor,
        rram_forecast=dm.rram_forecast,
        ml_forecast=dm.ml_forecast, **kw,
    )
    #print('---   ---done with plot_pentad_forecast_hydrograph_data_v2---   ---')

    #print('---   ---update_forecast_tabulator---   ---')
    _update_forecast_tabulator()
    #print('---   ---done with update_forecast_tabulator---   ---')


def on_data_needs_reload_changed(event):
    if event.new:
        print("Triggered rerunning of forecasts.")
        try:
            #print("---loading data---")
            # load_data()
            #print("---data loaded---")
            #print("---updating viz---")
            _update_visualizations()
            #print("---viz updated---")
            #print("Forecasts produced and visualizations updated successfully.")
        except Exception as e:
            print(f"Error during forecast rerun: {e}")
        finally:
            processing.data_reloader.data_needs_reload = False  # Reset the flag

# Attach watcher only once
if not hasattr(processing.data_reloader, 'watcher_attached'):
    processing.data_reloader.param.watch(on_data_needs_reload_changed, 'data_needs_reload')
    processing.data_reloader.watcher_attached = True

# =====================================================================
# 9. Bulletin writer
# =====================================================================
# Get information for bulletin headers into a dataframe that can be passed to the bulletin writer.
last_date, forecast_horizon, forecast_year = dm.get_bulletin_metadata()
bulletin_header_info = processing.get_bulletin_header_info(last_date, cfg.horizon)
wm.write_bulletin_button.on_click(
    partial(
        handle_bulletin_write,
        bulletin_sites=bulletin_sites,
        select_basin_widget=wm.select_basin,
        write_to_excel=write_to_excel,
        sites_list=dm.sites_list,
        bulletin_header_info=bulletin_header_info,
        env_file_path=cfg.env_file_path,
        downloader=wm.downloader
    )
)

# =====================================================================
# 10. Layout
# =====================================================================
# Define the disclaimer of the dashboard
disclaimer = layout.define_disclaimer(_, cfg.in_docker)


# Update the widgets conditional on the active tab
wm.range_selection.param.watch(lambda event: cfg.viz.update_range_slider_visibility(
    _, wm.manual_range, event), 'value')

# Create a placeholder for the dashboard content
dashboard_content = layout.define_tabs_2(_, wm.predictors_warning, wm.forecast_warning,
    daily_hydrograph_plot, daily_rainfall_plot, daily_temperature_plot, snow_plot_panes,
    forecast_data_and_plot,  
    wm.forecast_summary_table, pentad_forecast_plot, forecast_skill_plot,
    wm.bulletin_table, wm.write_bulletin_button, wm.bulletin_download_panel, disclaimer,
    wm.add_to_bulletin_button, wm.add_to_bulletin_popup, wm.show_daily_data,
    skill_table, skill_metrics_download_filename, skill_metrics_download_button
)
dashboard_content.param.watch(lambda event: cfg.viz.update_sidepane_card_visibility(dashboard_content, wm.station_card, wm.forecast_card, wm.basin_card, wm.pentad_card, wm.reload_card, event), 'active')


def update_active_tab(event):
    """Render plots only when the tab is first activated for a station."""
    active_tab = dashboard_content.active  # 0: Predictors tab, 1: Forecast tab
    with pn.io.hold(pn.state.curdoc):
        if active_tab == 0 and dm.should_render_predictors(wm.station.value):
            daily_hydrograph_plot.object = cfg.viz.plot_daily_hydrograph_data(_, dm.hydrograph_day_all, dm.linreg_predictor, wm.station.value, wm.date_picker.value)
            if cfg.display_weather_data == True:
                daily_rainfall_plot.object = cfg.viz.plot_daily_rainfall_data(_, dm.rain, wm.station.value, wm.date_picker.value, dm.linreg_predictor)
                daily_temperature_plot.object = cfg.viz.plot_daily_temperature_data(_, dm.temp, wm.station.value, wm.date_picker.value, dm.linreg_predictor)
            if cfg.display_snow_data == True:
                for var in dm.snow_data.keys():
                    if dm.snow_data[var] is not None:
                        snow_plot_panes[var].object = cfg.viz.plot_daily_snow_data(_, dm.snow_data, var, wm.station.value, wm.date_picker.value, dm.linreg_predictor)
                    else:
                        snow_plot_panes[var].object = pn.pane.Markdown(_("No snow data from SAPPHIRE Data Gateway available."))
        elif active_tab == 1 and dm.should_render_forecast(wm.station.value):
            plot = cfg.viz.select_and_plot_data(_, dm.linreg_predictor, wm.station.value, wm.pentad_selector.value, wm.decad_selector.value, cfg.save_directory)
            forecast_data_and_plot[:] = plot.objects
            update_forecast_plots(None)


# Attach the callback to the tabs and station
dashboard_content.param.watch(update_active_tab, 'active')
update_active_tab(None)


# message_pane = widgets.create_message_pane(dm._data)
sidebar_content=layout.define_sidebar(_, wm.station_card, wm.forecast_card, wm.basin_card,
                                  wm.message_pane, wm.reload_card)

# =====================================================================
# 11. Authentication
# =====================================================================
from dashboard.auth_manager import AuthManager

# --- Create the auth manager ---
auth = AuthManager()

# --- Register panels whose visibility auth controls ---
auth.register_panels(
    dashboard_content=dashboard_content,
    sidebar_content=sidebar_content,
    language_buttons=wm.language_buttons,
)

# --- Track widgets for inactivity reset ---
auth.track_widgets(wm.trackable_widgets())

# --- Build the template (use auth's widgets) ---
dashboard = pn.template.BootstrapTemplate(
    title=cfg.dashboard_title,
    logo=cfg.icon_path,
    header=[pn.Row(
        pn.layout.HSpacer(),
        wm.language_buttons,
        auth.logout_button,
        auth.logout_panel
    )],
    sidebar=pn.Column(sidebar_content),
    collapsed_sidebar=False,
    main=pn.Column(auth._js_pane, auth.login_form, dashboard_content),
    favicon=cfg.icon_path
)

# --- Initialize auth (sets visibility, restores session) ---
auth.initialize()

# Make the dashboard servable
dashboard.servable()

# =====================================================================
# 12. Background station loading
# =====================================================================
def on_stations_loaded(fut):
    try:
        new_all_stations, new_station_dict = fut.result()
        print(f"Stations loaded from iehhf: {len(new_all_stations) if new_all_stations is not None else 0}")
        # print(type(new_all_stations))
        if new_all_stations is not None:
            # print("Stations: ", new_all_stations)
            dm.replace_stations(new_all_stations, new_station_dict, wm.station,
                                _, wm.pentad_selector.value, wm.decad_selector.value)
    except Exception as e:
        print(f"Failed to load stations: {e}")

executor = ThreadPoolExecutor(max_workers=1)
future = executor.submit(processing.get_all_stations_from_iehhf, valid_codes)
future.add_done_callback(on_stations_loaded)
