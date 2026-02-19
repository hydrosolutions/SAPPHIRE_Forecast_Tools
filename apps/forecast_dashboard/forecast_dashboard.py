# forecast_dashboard.py
#
# This script creates a dashboard for the pentadal forecast.
#
# Run with the following command:
# ieasyhydroforecast_data_root_dir=/absolute/path/to ieasyhydroforecast_env_file_path=/absolute/path/to/sensitive_data_forecast_tools/config/.env_develop_kghm sapphire_forecast_horizon=pentad SAPPHIRE_OPDEV_ENV=True panel serve forecast_dashboard.py --show --autoreload --port 5055

# =========================
# Standard library imports
# =========================
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
from dashboard.bulletin_manager import BulletinManager
from dashboard.plot_manager import PlotManager
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
# 5. Plot manager
# =====================================================================
pm = PlotManager(dm, wm, cfg, gettext=_)

# Initial tabulator fill
pm.update_forecast_tabulator()

# =====================================================================
# 6. Callbacks
# =====================================================================
@pn.depends(wm.station, wm.pentad_selector, wm.decad_selector, watch=True)
def on_station_or_period_changed(station_value, selected_pentad, selected_decad):
    """Reload data for the new station and refresh the model checkbox."""
    dm.load_station(station_value.split()[0]) # Pass the station code
    dm.update_sites_for_pentad(_, selected_pentad, selected_decad)
    dm.invalidate_render_cache()

    wm.refresh_warnings()
    wm.refresh_model_checkbox()

    # update_active_tab(None)
    pm.render_active_tab(dashboard_content)

wm.update_forecast_button.on_click(pm.update_forecast_plots)

# Update the site object based on site and forecast selection
# --- Site object binding ---
update_site_object = pn.bind(
    Site.get_site_attributes_from_selected_forecast,
    _=_,
    sites=dm.sites_list,
    site_selection=wm.station,
    tabulator=wm.forecast_tabulator)


# =====================================================================
# 7. Bulletin management
# =====================================================================
bulletin = BulletinManager(
    wm=wm,
    cfg=cfg,
    dm=dm,
    processing=processing,
    write_to_excel=write_to_excel,
)

# =====================================================================
# 8. Data reload watcher
# =====================================================================
def on_data_needs_reload_changed(event):
    if event.new:
        print("Triggered rerunning of forecasts.")
        try:
            #print("---loading data---")
            # load_data()
            #print("---data loaded---")
            #print("---updating viz---")
            pm.refresh_all_visualizations()
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
# 9. Layout
# =====================================================================
# Define the disclaimer of the dashboard
disclaimer = layout.define_disclaimer(_, cfg.in_docker)


# Update the widgets conditional on the active tab
wm.range_selection.param.watch(lambda event: cfg.viz.update_range_slider_visibility(
    _, wm.manual_range, event), 'value')

# Create a placeholder for the dashboard content
dashboard_content = layout.define_tabs_2(_, wm.predictors_warning, wm.forecast_warning,
    pm.daily_hydrograph, pm.daily_rainfall, pm.daily_temperature, pm.snow_plots,
    pm.forecast_data_and_plot,  
    wm.forecast_summary_table, pm.pentad_forecast, pm.forecast_skill,
    wm.bulletin_table, wm.write_bulletin_button, wm.bulletin_download_panel, disclaimer,
    wm.add_to_bulletin_button, wm.add_to_bulletin_popup, wm.show_daily_data,
    pm.skill_table, pm.skill_download_filename, pm.skill_download_button
)
dashboard_content.param.watch(lambda event: cfg.viz.update_sidepane_card_visibility(
    dashboard_content, wm.station_card, wm.forecast_card, wm.basin_card, wm.pentad_card, wm.reload_card, event), 'active')

pm.render_active_tab(dashboard_content)

# Attach the callback to the tabs and station
# Attach tab-activation renderer & do the first render
dashboard_content.param.watch(
    lambda event: pm.render_active_tab(dashboard_content, event),
    'active',
)


sidebar_content=layout.define_sidebar(_, wm.station_card, wm.forecast_card, wm.basin_card,
                                  wm.message_pane, wm.reload_card)

# =====================================================================
# 10. Authentication
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
# 11. Background station loading
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
