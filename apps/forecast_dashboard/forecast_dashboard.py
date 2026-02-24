# forecast_dashboard.py
"""
Forecast dashboard — assembly script.

Every concern lives in its own manager; this file only creates the
objects, wires them together, and makes the result servable.

Run:
    ieasyhydroforecast_data_root_dir=/absolute/path/to ieasyhydroforecast_env_file_path=/absolute/path/to/sensitive_data_forecast_tools/config/.env_develop_kghm sapphire_forecast_horizon=pentad SAPPHIRE_OPDEV_ENV=True panel serve forecast_dashboard.py --show --autoreload --port 5055
"""
import panel as pn

from src.gettext_config import _
import src.processing as processing
from src.bulletins import write_to_excel
import src.layout as layout

from dashboard.logger import setup_logger
from dashboard.auth_manager import AuthManager
from dashboard.widget_manager import WidgetManager
from dashboard.bulletin_manager import BulletinManager
from dashboard.plot_manager import PlotManager
from dashboard import config
from dashboard.data_manager import DataManager


logger = setup_logger()

# ─── 1. Configuration & environment ─────────────────────────────────
cfg = config.init_dashboard(pn)

# ─── 2. Station metadata & DataManager ──────────────────────────────
all_stations, station_dict = processing.get_all_stations_from_file()

dm = DataManager(all_stations=all_stations, cfg=cfg)
station_code = station_dict[next(iter(station_dict))][0].split()[0]
dm.load_station('15189')

# ─── 3. Widgets ─────────────────────────────────────────────────────
wm = WidgetManager(dm, cfg, station_dict)
dm.update_sites_for_pentad(_, wm.pentad_selector.value, wm.decad_selector.value)

# ─── 4. Plot manager ────────────────────────────────────────────────
pm = PlotManager(dm, wm, cfg, gettext=_)
pm.update_forecast_tabulator()

# ─── 5. Bulletin management ─────────────────────────────────────────
bulletin = BulletinManager(
    wm=wm, cfg=cfg, dm=dm,
    processing=processing,
    write_to_excel=write_to_excel,
)

# ─── 6. Layout ──────────────────────────────────────────────────────
disclaimer = layout.define_disclaimer(_, cfg.in_docker)

dashboard_tabs = layout.define_tabs_2(_, wm, pm, disclaimer)

sidebar_content=layout.define_sidebar_2(_, wm)

# ─── 7. Wire all callbacks ──────────────────────────────────────────
wm.wire(dm, pm, dashboard_tabs, gettext=_)
pm.wire(dashboard_tabs)
dm.wire_data_reload(pm)

# First render of the active tab
pm.render_active_tab(dashboard_tabs)

# ─── 8. Authentication ──────────────────────────────────────────────
auth = AuthManager()
auth.register_panels(
    dashboard_content=dashboard_tabs,
    sidebar_content=sidebar_content,
    language_buttons=wm.language_buttons,
)
auth.track_widgets(wm.trackable_widgets())

# ─── 9. Template ────────────────────────────────────────────────────
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
    main=pn.Column(auth._js_pane, auth.login_form, dashboard_tabs),
    favicon=cfg.icon_path
)

auth.initialize()
dashboard.servable()

# ─── 10. Background station loading ─────────────────────────────────
dm.start_background_station_load(wm, gettext=_)
