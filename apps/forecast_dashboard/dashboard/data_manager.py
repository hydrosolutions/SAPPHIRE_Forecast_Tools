"""
Centralized data management for the forecast dashboard.

Encapsulates all mutable data state, provides controlled access,
handles reloading/refreshing logic, and owns background data lifecycle
(station loading from iehhf, pipeline-reload watcher).
"""

from __future__ import annotations

import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

import pandas as pd
import param

from src.site import SapphireSite as Site
from src import db
import src.processing as processing
from dashboard import utils
from dashboard.logger import setup_logger

if TYPE_CHECKING:
    from dashboard.plot_manager import PlotManager
    from dashboard.widget_manager import WidgetManager

logger = setup_logger()


class DataManager(param.Parameterized):
    """
    Single source of truth for all dashboard data.

    Replaces scattered global variables (`data`, `rram_forecast`,
    `latest_predictors`, `latest_forecast`, `sites_list`, etc.)
    with a cohesive, observable object.

    Usage:
        dm = DataManager(all_stations=..., ...)
        dm.load_station('pentad', '15189')
        forecasts = dm.forecasts_all
    """

    # --- Observable parameters (widgets can watch these) ---
    # current_station = param.String(default='', doc="Currently selected station code")
    # data_version = param.Integer(default=0, doc="Incremented on every data reload to notify dependents")

    def __init__(self, all_stations, **kwargs):
        super().__init__(**kwargs)

        # Immutable configuration
        # self._horizon = horizon
        # if horizon == "pentad":
        #     self._horizon_in_year = "pentad_in_year"
        # elif horizon == "decade":
        #     self._horizon_in_year = "decad_in_year"

        # Station metadata (may be replaced by async iehhf load)
        self._all_stations = all_stations
        self._sites_list = Site.get_site_attribues_from_iehhf_dataframe(all_stations)

        # Core data dict from db.get_data()
        self._data: dict = {}

        # Derived / cached values
        self._all_models: dict = {}
        self._rram_forecast = None

        # Track what's already been rendered to avoid redundant plot updates
        self._last_rendered_predictors_station: str | None = None
        self._last_rendered_forecast_station: str | None = None

        # Background executor (kept alive to prevent GC)
        self._executor: ThreadPoolExecutor | None = None
    
    # ------------------------------------------------------------------
    # Properties – read-only access to internal state
    # ------------------------------------------------------------------
    @property
    def all_stations(self):
        return self._all_stations

    @property
    def sites_list(self):
        return self._sites_list

    @property
    def rram_forecast(self):
        return self._rram_forecast

    def horizon_in_year(self, horizon):
        horizon_in_year = None
        if horizon == "pentad":
            horizon_in_year = "pentad_in_year"
        elif horizon == "decade":
            horizon_in_year = "decad_in_year"
        return horizon_in_year

    # @property
    # def all_models(self) -> dict:
    #     return dict(self._all_models)

    # --- Convenience accessors for common data keys ---

    @property
    def hydrograph_day_all(self):
        return self._data.get("hydrograph_day_all")

    @property
    def hydrograph_pentad_all(self):
        return self._data.get("hydrograph_pentad_all")

    @property
    def rain(self):
        return self._data.get("rain")

    @property
    def temp(self):
        return self._data.get("temp")

    @property
    def snow_data(self):
        return self._data.get("snow_data")

    @property
    def ml_forecast(self):
        return self._data.get("ml_forecast")

    @property
    def linreg_predictor(self):
        return self._data.get("linreg_predictor")

    @property
    def forecasts_all(self):
        return self._data.get("forecasts_all")

    @property
    def forecast_stats(self):
        return self._data.get("forecast_stats")

    @property
    def long_forecasts_m0(self):
        return self._data.get("long_forecasts_m0", pd.DataFrame())

    def get(self, key, default=None):
        """Generic access for less-common keys."""
        return self._data.get(key, default)

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load_station(self, horizon: str, station_code: str) -> None:
        """
        Fetch data for a station and rebuild derived structures.
        This is the *only* place `db.get_data` should be called.
        """
        logger.info(f"Loading data for station {station_code}")
        self._data = db.get_data(horizon, station_code, self._all_stations)
        # self.current_station = station_code
        self._rebuild_all_models()
        # self.data_version += 1  # notify watchers

    def _rebuild_all_models(self) -> None:
        """Rebuild the full model dictionary from freshly loaded forecasts."""
        df = self.forecasts_all
        if df is None or df.empty:
            self._all_models = {}
            return
        # Create a dictionary of the model names and the corresponding model labels
        self._all_models = (
            df[['model_short', 'model_long']]
            .drop_duplicates()
            .set_index('model_long')['model_short']
            .to_dict()
        )

    # ------------------------------------------------------------------
    # Model filtering helpers
    # ------------------------------------------------------------------

    def get_available_models(self, station_code: str, selected_date, horizon: str = "") -> dict:
        """Return models available for a given station + date."""
        if horizon == "month":
            return dict(self._all_models)
        # Update the model_dict with the models we have results for for the selected station
        # Model dict can be empty if no forecasts at all are available for the selected station
        return processing.update_model_dict_date(
            self._all_models, self.forecasts_all,
            station_code, selected_date,
        )

    def get_best_models(self, horizon, station_code: str, pentad, decad) -> list:
        """Return the best models for a station/pentad combination."""
        if horizon == "month":
            # No pentad/decad-level skill data for monthly; return all models
            df = self.forecasts_all
            if df is None or df.empty:
                return []
            return df["model_short"].unique().tolist()
        horizon_value = ""
        if horizon == "pentad":
            horizon_value = pentad
        elif horizon == "decade":
            horizon_value = decad
        return processing.get_best_models_for_station_and_pentad(
            horizon, self.horizon_in_year(horizon), horizon_value, self.forecasts_all, station_code
        )

    def resolve_model_values(self, available_models: dict, preselected: list) -> list:
        """
        Map pre-selected model keys to widget values, handling
        ensemble name mismatches gracefully.
        """
        # Add models to value list safely
        values = []
        for model in preselected:
            if model in available_models:
                values.append(available_models[model])
            elif "Ens. Mean" in model:
                # Find any Neural Ensemble model in the dictionary
                match = next(
                    (available_models[k] for k in available_models if "Ens. Mean" in k),
                    None,
                )
                if match:
                    values.append(match)
            # Skip models that can't be found
            ## silently skip unresolvable models
        return values

    # ------------------------------------------------------------------
    # Site attribute updates
    # ------------------------------------------------------------------

    def update_sites_for_pentad(self, _, horizon, pentad, decad) -> None:
        """Refresh hydrograph statistics + linear regression predictor on sites."""
        if horizon == "month":
            return  # no pentad/decad-level stats for monthly horizon
        # Initial site attribute computation
        self._sites_list = utils.update_site_attributes_with_hydrograph_statistics_for_selected_pentad(
            _=_, sites=self._sites_list,
            df=self.hydrograph_pentad_all,
            pentad=pentad, decad=decad,
            horizon=horizon,
            horizon_in_year=self.horizon_in_year(horizon),
        )
        self._sites_list = utils.update_site_attributes_with_linear_regression_predictor(
            _, sites=self._sites_list,
            df=self.linreg_predictor,
            pentad=pentad, decad=decad,
            horizon=horizon,
            horizon_in_year=self.horizon_in_year(horizon),
        )

    # ------------------------------------------------------------------
    # Station metadata replacement (async iehhf callback)
    # ------------------------------------------------------------------

    def replace_stations(self, horizon, new_all_stations, new_station_dict, station_widget,
                         _, pentad, decad) -> None:
        """
        Called from the background thread callback when iehhf stations finish
        loading.  Updates internal state and refreshes site attributes.
        """
        logger.info(f"Replacing stations: {len(new_all_stations)} loaded from iehhf")
        self._all_stations = new_all_stations
        station_widget.groups = new_station_dict

        self._sites_list = Site.get_site_attribues_from_iehhf_dataframe(new_all_stations)
        self.update_sites_for_pentad(_, horizon, pentad, decad)

    # ------------------------------------------------------------------
    # Render tracking – avoids redundant expensive plot updates
    # ------------------------------------------------------------------

    def should_render_predictors(self, station_code: str) -> bool:
        if self._last_rendered_predictors_station == station_code:
            return False
        self._last_rendered_predictors_station = station_code
        return True

    def should_render_forecast(self, station_code: str) -> bool:
        if self._last_rendered_forecast_station == station_code:
            return False
        self._last_rendered_forecast_station = station_code
        return True

    def invalidate_render_cache(self) -> None:
        """Force both tabs to re-render on next activation."""
        self._last_rendered_predictors_station = None
        self._last_rendered_forecast_station = None

    # ------------------------------------------------------------------
    # Bulletin helpers
    # ------------------------------------------------------------------

    def get_bulletin_metadata(self, horizon):
        """Return (last_date, forecast_horizon, forecast_year) for bulletin saving."""
        # Get the last available date in the data
        max_date = self.forecasts_all['date'].max()
        if not isinstance(max_date, (dt.date, dt.datetime)) or pd.isna(max_date):
            raise ValueError("No valid forecast dates available")
        last_date = max_date + dt.timedelta(days=1)

        if horizon == "month":
            forecast_horizon = dt.datetime.now().month
        else:
            # The forecast is produced on the day before the first day of the forecast
            # pentad, therefore we add 1 to the forecast pentad in linreg_predictor to get
            # the pentad of the forecast period.
            forecast_horizon = int(
                self.forecasts_all[self.horizon_in_year(horizon)].tail(1).values[0]
            )
        return last_date, forecast_horizon, last_date.year

    # @property
    # def linreg_datatable(self):
    #     """Shifted linreg_predictor for display (1-day shift)."""
    #     return processing.shift_date_by_n_days(self.linreg_predictor, 1)

    # ------------------------------------------------------------------
    # Data lifecycle: reload watcher + background station loading
    # ------------------------------------------------------------------

    def wire_data_reload(self, pm: PlotManager) -> None:
        """Watch the data_reloader flag and refresh plots when it fires."""
        def _on_data_needs_reload(event):
            if not event.new:
                return
            print("Triggered rerunning of forecasts.")
            logger.info("Data reload triggered — reloading data and refreshing visualisations.")
            _fa = self.forecasts_all
            logger.debug(
                "D5 Before reload — forecasts_all: %d rows, max date=%s",
                len(_fa) if _fa is not None else 0,
                _fa["date"].max() if _fa is not None and not _fa.empty else "N/A",
            )
            try:
                # Reload data from the API so visualisations use fresh results
                horizon = pm._wm.horizon_selector.value
                station_code = pm._wm.station_selector.value.split()[0]
                self.load_station(horizon, station_code)
                self.invalidate_render_cache()

                # Update date picker to reflect newly available data
                if not self.forecasts_all.empty:
                    max_date = self.forecasts_all['date'].max()
                    if hasattr(max_date, 'date'):
                        pm._wm.date_picker.value = max_date.date()

                pm._wm.refresh_model_checkbox()
                pm.refresh_all_visualizations()
                _fa2 = self.forecasts_all
                logger.debug(
                    "D6 After reload — forecasts_all: %d rows, max date=%s",
                    len(_fa2) if _fa2 is not None else 0,
                    _fa2["date"].max() if _fa2 is not None and not _fa2.empty else "N/A",
                )
            except Exception as e:
                logger.error("Error during forecast rerun: %s", e)
                print(f"Error during forecast rerun: {e}")
            finally:
                processing.data_reloader.data_needs_reload = False

        # Attach watcher only once
        if not getattr(processing.data_reloader, "watcher_attached", False):
            processing.data_reloader.param.watch(
                _on_data_needs_reload, "data_needs_reload"
            )
            processing.data_reloader.watcher_attached = True

    # Background station loading
    def start_background_station_load(self, wm: WidgetManager, gettext) -> None:
        """Kick off the async iehhf station fetch (fire-and-forget)."""
        def _on_done(future):
            try:
                new_all_stations, new_station_dict = future.result()
                count = len(new_all_stations) if new_all_stations is not None else 0
                logger.info("Stations loaded from iehhf: %d", count)
                # print(type(new_all_stations))
                if new_all_stations is not None:
                    # print("Stations: ", new_all_stations)
                    self.replace_stations(wm.horizon_selector.value,
                        new_all_stations, new_station_dict, wm.station_selector,
                        gettext, wm.pentad_selector.value, wm.decad_selector.value,
                    )
            except Exception as e:
                logger.error("Failed to load stations: %s", e)

        self._executor = ThreadPoolExecutor(max_workers=1)
        future = self._executor.submit(processing.get_all_stations_from_iehhf)
        future.add_done_callback(_on_done)
