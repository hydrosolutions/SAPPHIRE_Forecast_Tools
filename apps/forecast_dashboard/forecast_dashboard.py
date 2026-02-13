# forecast_dashboard.py
#
# This script creates a dashboard for the pentadal forecast.
#
# Run with the following command:
# ieasyhydroforecast_data_root_dir=/absolute/path/to ieasyhydroforecast_env_file_path=/absolute/path/to/sensitive_data_forecast_tools/config/.env_develop_kghm sapphire_forecast_horizon=pentad SAPPHIRE_OPDEV_ENV=True panel serve forecast_dashboard.py --show --autoreload --port 5055

# =========================
# Standard library imports
# =========================
import os
import sys
import datetime as dt
from datetime import datetime, timedelta
from functools import partial
from concurrent.futures import ThreadPoolExecutor

# =========================
# Third-party imports
# =========================
import panel as pn
import pandas as pd
import holoviews as hv

# =========================
# Local application imports
# =========================
from src.gettext_config import _
import src.processing as processing
from src.site import SapphireSite as Site
from src.bulletins import write_to_excel
import src.layout as layout
from src import db

from dashboard.logger import setup_logger
from dashboard import widgets
from dashboard.bulletin_manager import load_bulletin_from_csv, add_current_selection_to_bulletin, remove_selected_from_bulletin, handle_bulletin_write, create_bulletin_table
from dashboard import config
from dashboard import utils
from dashboard.data_manager import DataManager


logger = setup_logger()

# =====================================================================
# 1. Configuration & environment
# =====================================================================
config.setup_panel(pn)
env_file_path, in_docker_flag, icon_path = config.load_env_and_icons()
SAVE_DIRECTORY = config.setup_directories()
viz = config.setup_localization(pn)
horizon, horizon_in_year, dashboard_title = config.get_horizon()
downloader, bulletin_download_panel = widgets.create_downloader_and_panel(horizon)
display_weather_data, display_snow_data = config.display_weather_and_snow_data()

# =====================================================================
# 2. Station metadata & DataManager initialisation
# =====================================================================
valid_codes = ['16159', '16159', '16159', '16101', '16159', '16159', '15054', '16101', '16134', '16101', '16101', '16134', '16134', '15081', '15054', '16105', '16101', '16105', '16100', '16105', '15054', '16160', '15149', '15149', '15149', '15149', '15149', '16105', '15149', '16100', '16100', '16100', '16160', '16160', '16101', '16159', '16100', '15054', '15034', '16139', '16143', '16143', '15051', '15051', '15051', '15051', '16151', '15051', '15081', '15081', '16135', '16135', '16135', '15034', '15034', '16160', '16134', '16143', '16143', '16143', '16139', '16139', '16139', '16139', '16151', '16151', '16151', '16151', '16151', '15054', '15054', '15051', '16143', '16146', '16146', '16146', '16146', '16139', '16160', '15283', '16105', '15215', '15215', '15215', '15261', '16936', '15216', '15216', '15215', '15216', '15216', '15216', '15256', '15256', '15256', '15256', '16936', '15216', '15215', '15259', '15215', '15034', '15259', '15259', '16936', '16936', '16936', '16936', '15259', '15259', '15259', '15013', '15283', '15013', '15013', '15013', '15013', '15013', '15283', '16160', '15283', '15090', '16127', '16127', '16127', '16127', '16127', '16127', '16158', '16134', '16158', '16158', '16158', '16158', '15090', '15102', '15102', '16105', '16158', '16134', '15090', '15283', '15090', '16121', '16121', '16121', '16121', '16121', '16121', '15030', '15030', '15030', '15030', '15102', '15102', '15102', '15102', '15090', '15090', '15256', '15034', '15287', '15083', '16096', '16096', '16096', '16096', '16096', '15171', '16161', '15025', '15171', '15171', '15171', '15171', '16146', '15189', '15189', '15960', '15171', '16096', '16161', '16161', '15189', '16161', '16161', '16070', '16070', '16070', '16070', '16070', '16070', '16169', '16169', '16169', '16169', '16169', '16169', '15189', '16161', '15189', '15025', '15194', '15194', '16068', '15020', '16059', '15020', '15020', '15020', '16059', '16068', '16059', '15256', '16146', '17462', '17462', '17462', '17462', '17462', '16059', '16068', '16068', '16068', '16055', '16055', '16055', '16055', '16055', '16055', '16176', '16176', '16176', '16176', '16176', '16176', '15194', '16059', '16059', '16068', '15189', '15194', '15034', '15214', '15287', '16153', '16153', '16100', '15025', '15025', '15016', '15954', '16153', '15954', '15212', '15212', '15212', '15212', '15212', '16487', '16487', '15212', '16153', '16153', '16136', '15083', '15083', '15083', '15083', '15083', '16135', '16135', '16135', '16136', '15081', '15081', '15081', '16136', '16136', '16136', '16136', '16153', '16487', '15287', '16487', '16487', '16681', '15283', '15214', '15214', '15214', '15312', '15016', '16510', '15312', '15312', '15312', '15312', '15287', '15214', '15287', '15287', '15312', '16510', '16510', '16510', '15016', '15954', '15954', '15954', '15954', '15016', '15016', '15016', '15214', '15285', '15285', '15285', '15285', '15285', '15285', '16510', '16510', '16487', '17462']
all_stations, station_dict = processing.get_all_stations_from_file(valid_codes)

dm = DataManager(
    all_stations=all_stations,
    valid_codes=valid_codes,
    horizon=horizon,
    horizon_in_year=horizon_in_year,
)
dm.load_station('15189')

# =====================================================================
# 3. Widgets
# =====================================================================

# Widget for date selection, always visible
date_picker = widgets.create_date_picker(dm.forecasts_all)

last_date, forecast_horizon_for_saving_bulletin, forecast_year_for_saving_bulletin = dm.get_bulletin_metadata()

# Get information for bulletin headers into a dataframe that can be passed to the bulletin writer.
bulletin_header_info = processing.get_bulletin_header_info(last_date, horizon)

pentad_selector = widgets.create_pentad_selector(last_date)
decad_selector = widgets.create_decad_selector(last_date)
station = widgets.create_station(station_dict)

# --- Model checkbox with initial pre-selection ---
model_dict = dm.get_filtered_model_dict(station.value, date_picker.value)
model_checkbox = widgets.create_model_checkbox(model_dict)

preselected = dm.get_preselected_models(station.value, pentad_selector.value, decad_selector.value)
model_checkbox.value = dm.resolve_model_values(model_dict, preselected)

allowable_range_selection, manual_range, show_range_button = widgets.create_range_widgets()
show_daily_data_widget = widgets.create_show_daily_data_widget()
remove_bulletin_button, write_bulletin_button = widgets.create_bulletin_buttons()
language_buttons = widgets.create_language_buttons()
forecast_tabulator = widgets.create_forecast_tabulator()
select_basin_widget = widgets.create_select_basin_widget(station_dict)
update_forecast_button = widgets.create_update_forecast_button()

forecast_card = widgets.create_forecast_card(allowable_range_selection, manual_range, model_checkbox, show_range_button, update_forecast_button, station)
pentad_card = widgets.create_pentad_card(pentad_selector, station)
station_card = widgets.create_station_card(station)
basin_card = widgets.create_basin_card(select_basin_widget, station)
add_to_bulletin_button = widgets.create_add_to_bulletin_button()
bulletin_tabulator = widgets.create_bulletin_tabulator()

# =====================================================================
# 4. Initial site attribute computation
# =====================================================================
dm.update_sites_for_pentad(_, pentad_selector.value, decad_selector.value)

# =====================================================================
# 5. Callbacks
# =====================================================================

# Adding the watcher logic for disabling the "Add to Bulletin" button
def update_add_to_bulletin_button(event):
    """Update the state of 'Add to Bulletin' button based on pipeline_running status."""
    add_to_bulletin_button.disabled = event.new

# Watch for changes in pipeline_running and update the add_to_bulletin_button
viz.app_state.param.watch(update_add_to_bulletin_button, 'pipeline_running')

# Set the initial state of the button based on whether the pipeline is running
add_to_bulletin_button.disabled = viz.app_state.pipeline_running


@pn.depends(station, pentad_selector, decad_selector, watch=True)
def update_model_select(station_value, selected_pentad, selected_decad):
    """Reload data for the new station and refresh the model checkbox."""
    station_code = station_value.split()[0]
    dm.load_station(station_code)
    dm.update_sites_for_pentad(_, selected_pentad, selected_decad)
    dm.invalidate_render_cache()

    predictors_warning.objects = []  # clear old content
    warning = widgets.get_predictors_warning(station, dm._data)
    if warning:
        predictors_warning.append(warning)

    forecast_warning.objects = []  # clear old content
    warning = widgets.get_forecast_warning(station, dm._data, date_picker.value)
    if warning:
        forecast_warning.append(warning)
    print("\n=== Starting Model Select Update ===")
    print(f"Initial widget state:")
    print(f"  Options: {model_checkbox.options}")
    print(f"  Current value: {model_checkbox.value}")

    # First get the updated model dictionary
    print(station_value, type(station_value))
    updated_model_dict = dm.get_filtered_model_dict(station_value, date_picker.value)

    print("\nAfter update_model_dict:")
    print(f"  Updated model dict: {updated_model_dict}")

    # Get pre-selected models
    updated_preselected = dm.get_preselected_models(station_value, selected_pentad, selected_decad)

    print("\nAfter get_best_models:")
    print(f"  Pre-selected models: {updated_preselected}")

    # Create new values list
    new_values = dm.resolve_model_values(updated_model_dict, updated_preselected)

    print("\nBefore widget update:")
    print(f"  New options to set: {updated_model_dict}")
    print(f"  New values to set: {new_values}")

    # Try updating options first, then values
    model_checkbox.options = updated_model_dict
    model_checkbox.param.trigger('options')

    print("\nAfter options update:")
    print(f"  Widget options: {model_checkbox.options}")
    print(f"  Widget value: {model_checkbox.value}")

    model_checkbox.value = new_values
    model_checkbox.param.trigger('value')

    print("\nFinal widget state:")
    print(f"  Widget options: {model_checkbox.options}")
    print(f"  Widget value: {model_checkbox.value}")

    return updated_model_dict

# =====================================================================
# 6. Bulletin management
# =====================================================================
add_to_bulletin_popup = widgets.create_add_to_bulletin_popup()
bulletin_sites = load_bulletin_from_csv(forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin, SAVE_DIRECTORY, dm.sites_list)

# Function to update the bulletin table
def update_bulletin_table(event=None):
    create_bulletin_table(bulletin_sites, select_basin_widget, bulletin_tabulator)

bulletin_table = widgets.create_bulletin_table(bulletin_tabulator, remove_bulletin_button, add_to_bulletin_popup)
update_bulletin_table()
select_basin_widget.param.watch(update_bulletin_table, 'value')

# add_to_bulletin_button.on_click(add_current_selection_to_bulletin)
add_to_bulletin_button.on_click(
    partial(
        add_current_selection_to_bulletin,
        viz=viz,
        forecast_tabulator=forecast_tabulator,
        station=station,
        sites_list=dm.sites_list,
        add_to_bulletin_popup=add_to_bulletin_popup,
        bulletin_sites=bulletin_sites,
        forecast_year_for_saving_bulletin=forecast_year_for_saving_bulletin,
        forecast_horizon_for_saving_bulletin=forecast_horizon_for_saving_bulletin,
        save_directory=SAVE_DIRECTORY,
        update_bulletin_table=update_bulletin_table
    )
)

# Attach the remove function to the remove button click event
remove_bulletin_button.on_click(
    partial(
        remove_selected_from_bulletin,
        bulletin_tabulator=bulletin_tabulator,
        bulletin_sites=bulletin_sites,
        add_to_bulletin_popup=add_to_bulletin_popup,
        forecast_year_for_saving_bulletin=forecast_year_for_saving_bulletin,
        forecast_horizon_for_saving_bulletin=forecast_horizon_for_saving_bulletin,
        save_directory=SAVE_DIRECTORY,
        update_bulletin_table=update_bulletin_table
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
        'SWE': pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width"),
        'HS': pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width"),
        'RoF': pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_width")
    }

forecast_data_and_plot = pn.Column(sizing_mode="stretch_both")
pentad_forecast_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
effectiveness_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
accuracy_plot = pn.pane.HoloViews(hv.Curve([]), sizing_mode="stretch_both")
forecast_skill_plot = pn.Column(effectiveness_plot, accuracy_plot)

def update_forecast_hydrograph(selected_option, _, hydrograph_day_all,
                               hydrograph_pentad_all, linreg_predictor,
                               forecasts_all, station, title_date,
                               model_selection, range_type, range_slider,
                               range_visibility, rram_forecast, ml_forecast):
    if selected_option == _('Yes'):
        # Show forecasts aggregated to pentadal values
        return viz.plot_pentad_forecast_hydrograph_data(
            _,
            hydrograph_pentad_all=hydrograph_pentad_all,
            forecasts_all=forecasts_all,
            station=station,
            title_date=title_date,
            model_selection=model_selection,
            range_type=range_type,
            range_slider=range_slider,
            range_visibility=range_visibility
        )
    else:
        # Show daily forecasts
        return viz.plot_pentad_forecast_hydrograph_data_v2(
            _,
            hydrograph_day_all=hydrograph_day_all,
            linreg_predictor=linreg_predictor,
            forecasts_all=forecasts_all,
            station=station,
            title_date=title_date,
            model_selection=model_selection,
            range_type=range_type,
            range_slider=range_slider,
            range_visibility=range_visibility,
            rram_forecast=rram_forecast,
            ml_forecast=ml_forecast
        )


def update_forecast_plots(event):
    """Updates 2nd, 3rd and 4th plots on Forecast tab"""
    pentad_forecast_plot.object = update_forecast_hydrograph(
        show_daily_data_widget.value,
        _,
        hydrograph_day_all=dm.hydrograph_day_all,
        hydrograph_pentad_all=dm.hydrograph_pentad_all,
        linreg_predictor=dm.linreg_predictor,
        forecasts_all=dm.forecasts_all,
        station=station.value,
        title_date=date_picker.value,
        model_selection=model_checkbox.value,
        range_type=allowable_range_selection.value,
        range_slider=manual_range.value,
        range_visibility=show_range_button.value,
        rram_forecast=dm.rram_forecast,
        ml_forecast=dm.ml_forecast
    )
    temp = viz.plot_forecast_skill(
        _,
        dm.hydrograph_pentad_all,
        dm.forecasts_all,
        station_widget=station.value,
        date_picker=date_picker.value,
        model_checkbox=model_checkbox.value,
        range_selection_widget=allowable_range_selection.value,
        manual_range_widget=manual_range.value,
        show_range_button=show_range_button.value
    )
    effectiveness_plot.object = temp[0].object
    accuracy_plot.object = temp[1].object

    forecast_summary_table.object = update_forecast_tabulator(station, model_checkbox, allowable_range_selection, manual_range)

update_forecast_button.on_click(update_forecast_plots)

skill_table = pn.panel(
    viz.create_skill_table(_, dm.forecast_stats),
    sizing_mode='stretch_width')

skill_metrics_download_filename, skill_metrics_download_button = skill_table.download_menu(
    text_kwargs={'name': _('Enter filename:'), 'value': 'forecast_skill_metrics.csv'},
    button_kwargs={'name': _('Download currently visible table')}
)


# @pn.depends(station, model_checkbox, allowable_range_selection, manual_range, watch=True)
def update_forecast_tabulator(station, model_checkbox, allowable_range_selection, manual_range):
    viz.create_forecast_summary_tabulator(
        _, dm.forecasts_all, station, date_picker,
        model_checkbox, allowable_range_selection, manual_range,
        forecast_tabulator
    )

# Initial update
update_forecast_tabulator(station, model_checkbox, allowable_range_selection, manual_range)


def update_visualizations():
    # Re-bind the plots to use the updated data
    #print('---   ---plot_pentad_forecast_hydrograph_data---   ---')
    viz.plot_pentad_forecast_hydrograph_data(
        _,
        hydrograph_pentad_all=dm.hydrograph_pentad_all,
        forecasts_all=dm.forecasts_all,
        station=station.value,
        title_date=date_picker.value,
        model_selection=model_checkbox.value,
        range_type=allowable_range_selection.value,
        range_slider=manual_range.value,
        range_visibility=show_range_button.value
    )
    #print('---   ---done with plot_pentad_forecast_hydrograph_data---   ---')

    #print('---   ---plot_pentad_forecast_hydrograph_data_v2---   ---')
    viz.plot_pentad_forecast_hydrograph_data_v2(
        _,
        hydrograph_day_all=dm.hydrograph_day_all,
        linreg_predictor=dm.linreg_predictor,
        forecasts_all=dm.forecasts_all,
        station=station.value,
        title_date=date_picker.value,
        model_selection=model_checkbox.value,
        range_type=allowable_range_selection.value,
        range_slider=manual_range.value,
        range_visibility=show_range_button.value,
        rram_forecast=dm.rram_forecast,
        ml_forecast=dm.ml_forecast
    )
    #print('---   ---done with plot_pentad_forecast_hydrograph_data_v2---   ---')

    #print('---   ---update_forecast_tabulator---   ---')
    update_forecast_tabulator(station, model_checkbox, allowable_range_selection, manual_range)
    #print('---   ---done with update_forecast_tabulator---   ---')


def on_data_needs_reload_changed(event):
    if event.new:
        print("Triggered rerunning of forecasts.")
        try:
            #print("---loading data---")
            # load_data()
            #print("---data loaded---")
            #print("---updating viz---")
            update_visualizations()
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

# Same Tabulator in both tabs
forecast_summary_table = widgets.create_forecast_summary_table(forecast_tabulator)

# Update the site object based on site and forecast selection
#print(f"DEBUG: forecast_dashboard.py: forecast_tabulator: {forecast_summary_tabulator}")
update_site_object = pn.bind(
    Site.get_site_attributes_from_selected_forecast,
    _=_,
    sites=dm.sites_list,
    site_selection=station,
    tabulator=forecast_summary_table)



# Use the new handler
write_bulletin_button.on_click(
    partial(
        handle_bulletin_write,
        bulletin_sites=bulletin_sites,
        select_basin_widget=select_basin_widget,
        write_to_excel=write_to_excel,
        sites_list=dm.sites_list,
        bulletin_header_info=bulletin_header_info,
        env_file_path=env_file_path,
        downloader=downloader
    )
)

# Define the disclaimer of the dashboard
disclaimer = layout.define_disclaimer(_, in_docker_flag)


# Update the layout

# Update the widgets conditional on the active tab
allowable_range_selection.param.watch(lambda event: viz.update_range_slider_visibility(
    _, manual_range, event), 'value')

reload_card = viz.create_reload_button()

# We don't need to update tabs or UI components dynamically since the page reloads

predictors_warning = pn.Column()
warning = widgets.get_predictors_warning(station, dm._data)
if warning:
    predictors_warning.append(warning)

forecast_warning = pn.Column()
warning = widgets.get_forecast_warning(station, dm._data, date_picker.value)
if warning:
    forecast_warning.append(warning)

# Create a placeholder for the dashboard content
dashboard_content = layout.define_tabs_2(_, predictors_warning, forecast_warning,
    daily_hydrograph_plot, daily_rainfall_plot, daily_temperature_plot, snow_plot_panes,
    forecast_data_and_plot,  
    forecast_summary_table, pentad_forecast_plot, forecast_skill_plot,
    bulletin_table, write_bulletin_button, bulletin_download_panel, disclaimer,
    add_to_bulletin_button, add_to_bulletin_popup, show_daily_data_widget,
    skill_table, skill_metrics_download_filename, skill_metrics_download_button
)
dashboard_content.param.watch(lambda event: viz.update_sidepane_card_visibility(dashboard_content, station_card, forecast_card, basin_card, pentad_card, reload_card, event), 'active')

latest_predictors = None
latest_forecast = None


def update_active_tab(event):
    """Callback function to handle tab and station changes"""
    global latest_predictors
    global latest_forecast
    active_tab = dashboard_content.active  # 0: Predictors tab, 1: Forecast tab
    if active_tab == 0 and latest_predictors != station.value:
        latest_predictors = station.value
        daily_hydrograph_plot.object = viz.plot_daily_hydrograph_data(_, dm.hydrograph_day_all, dm.linreg_predictor, station.value, date_picker.value)
        if display_weather_data == True: 
            daily_rainfall_plot.object = viz.plot_daily_rainfall_data(_, dm.rain, station.value, date_picker.value, dm.linreg_predictor)
            daily_temperature_plot.object = viz.plot_daily_temperature_data(_, dm.temp, station.value, date_picker.value, dm.linreg_predictor)
        if display_snow_data == True:
            for var in dm.snow_data.keys():
                if dm.snow_data[var] is not None:
                    snow_plot_panes[var].object = viz.plot_daily_snow_data(_, dm.snow_data, var, station.value, date_picker.value, dm.linreg_predictor)
                else: 
                    snow_plot_panes[var].object = pn.pane.Markdown(_("No snow data from SAPPHIRE Data Gateway available."))
    elif active_tab == 1 and latest_forecast != station.value:
        latest_forecast = station.value
        plot = viz.select_and_plot_data(_, dm.linreg_predictor, station.value, pentad_selector.value, decad_selector.value, SAVE_DIRECTORY)
        forecast_data_and_plot[:] = plot.objects
        update_forecast_plots(None)


# Attach the callback to the tabs and station
dashboard_content.param.watch(update_active_tab, 'active')
station.param.watch(update_active_tab, 'value')
update_active_tab(None)


message_pane = widgets.create_message_pane(dm._data)
sidebar_content=layout.define_sidebar(_, station_card, forecast_card, basin_card,
                                  message_pane, reload_card)

#------------------AUTHENTICATION-----------------------------
from dashboard.auth_manager import AuthManager

# --- Create the auth manager ---
auth = AuthManager()

# --- Register panels whose visibility auth controls ---
auth.register_panels(
    dashboard_content=dashboard_content,
    sidebar_content=sidebar_content,
    language_buttons=language_buttons,
)

# --- Track widgets for inactivity reset ---
auth.track_widgets([
    (station, "value"),
    (model_checkbox, "value"),
    (allowable_range_selection, "value"),
    (show_range_button, "value"),
    (show_daily_data_widget, "value"),
    (select_basin_widget, "value"),
    (add_to_bulletin_button, "clicks"),
    (write_bulletin_button, "clicks"),
    (remove_bulletin_button, "clicks"),
    (forecast_tabulator, "selection"),
])

# --- Build the template (use auth's widgets) ---
dashboard = pn.template.BootstrapTemplate(
    title=dashboard_title,
    logo=icon_path,
    header=[pn.Row(
        pn.layout.HSpacer(),
        language_buttons,
        auth.logout_button,
        auth.logout_panel
    )],
    sidebar=pn.Column(sidebar_content),
    collapsed_sidebar=False,
    main=pn.Column(auth._js_pane, auth.login_form, dashboard_content),
    favicon=icon_path
)

# --- Initialize auth (sets visibility, restores session) ---
auth.initialize()

# ------------------END OF AUTHENTICATION---------------------

# Make the dashboard servable
dashboard.servable()
# endregion

def on_stations_loaded(fut):
    try:
        global all_stations#, sites_list
        _all_stations, _station_dict = fut.result()
        print(f"Stations loaded from iehhf: {len(_all_stations) if _all_stations is not None else 0}")
        # print(type(_all_stations))
        if _all_stations is not None:
            # print("Stations: ", _all_stations)
            all_stations = _all_stations
            station.groups = _station_dict

            dm._sites_list = Site.get_site_attribues_from_iehhf_dataframe(all_stations)
            # sites_list = utils.update_site_attributes_with_hydrograph_statistics_for_selected_pentad(_=_, sites=sites_list, df=dm.hydrograph_pentad_all, pentad=pentad_selector.value, decad=decad_selector.value, horizon=horizon, horizon_in_year=horizon_in_year)
            # sites_list = utils.update_site_attributes_with_linear_regression_predictor(_, sites=sites_list, df=dm.linreg_predictor, pentad=pentad_selector.value, decad=decad_selector.value, horizon=horizon, horizon_in_year=horizon_in_year)
            dm.update_sites_for_pentad(_, pentad_selector.value, decad_selector.value)

    except Exception as e:
        print(f"Failed to load stations: {e}")

executor = ThreadPoolExecutor(max_workers=1)
future = executor.submit(processing.get_all_stations_from_iehhf, valid_codes)
future.add_done_callback(on_stations_loaded)
