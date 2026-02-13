"""
Centralized data management for the forecast dashboard.

Encapsulates all mutable data state, provides controlled access,
and handles reloading/refreshing logic in one place.
"""

import param
import datetime as dt

from src.site import SapphireSite as Site
from src import db
import src.processing as processing
from dashboard import utils
from dashboard.logger import setup_logger

logger = setup_logger()


class DataManager(param.Parameterized):
    """
    Single source of truth for all dashboard data.

    Replaces scattered global variables (`data`, `rram_forecast`,
    `latest_predictors`, `latest_forecast`, `sites_list`, etc.)
    with a cohesive, observable object.

    Usage:
        dm = DataManager(all_stations=..., valid_codes=..., ...)
        dm.load_station('15189')
        forecasts = dm.forecasts_all
    """

    # --- Observable parameters (widgets can watch these) ---
    current_station = param.String(default='', doc="Currently selected station code")
    data_version = param.Integer(default=0, doc="Incremented on every data reload to notify dependents")

    def __init__(self, all_stations, valid_codes, horizon, horizon_in_year, **kwargs):
        super().__init__(**kwargs)

        # Immutable configuration
        self._horizon = horizon
        self._horizon_in_year = horizon_in_year
        self._valid_codes = valid_codes

        # Station metadata (may be replaced by async iehhf load)
        self._all_stations = all_stations
        self._sites_list = Site.get_site_attribues_from_iehhf_dataframe(all_stations)

        # Core data dict from db.get_data()
        self._data: dict = {}

        # Derived / cached values
        self._model_dict_all: dict = {}
        self._rram_forecast = None

        # Track what's already been rendered to avoid redundant plot updates
        self._last_rendered_predictors_station: str | None = None
        self._last_rendered_forecast_station: str | None = None
    
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

    @property
    def model_dict_all(self) -> dict:
        return dict(self._model_dict_all)
    
    # --- Convenience accessors for common data keys ---

    @property
    def forecasts_all(self):
        return self._data.get("forecasts_all")

    @property
    def hydrograph_day_all(self):
        return self._data.get("hydrograph_day_all")

    @property
    def hydrograph_pentad_all(self):
        return self._data.get("hydrograph_pentad_all")

    @property
    def linreg_predictor(self):
        return self._data.get("linreg_predictor")

    @property
    def forecast_stats(self):
        return self._data.get("forecast_stats")

    @property
    def ml_forecast(self):
        return self._data.get("ml_forecast")

    @property
    def rain(self):
        return self._data.get("rain")

    @property
    def temp(self):
        return self._data.get("temp")

    @property
    def snow_data(self):
        return self._data.get("snow_data")

    def get(self, key, default=None):
        """Generic access for less-common keys."""
        return self._data.get(key, default)
    
    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load_station(self, station_code: str) -> None:
        """
        Fetch data for a station and rebuild derived structures.
        This is the *only* place `db.get_data` should be called.
        """
        logger.info(f"Loading data for station {station_code}")
        self._data = db.get_data(station_code, self._all_stations)
        self.current_station = station_code
        self._rebuild_model_dict()
        self.data_version += 1  # notify watchers

    def _rebuild_model_dict(self) -> None:
        """Rebuild the full model dictionary from freshly loaded forecasts."""
        df = self.forecasts_all
        if df is None or df.empty:
            self._model_dict_all = {}
            return
        # Create a dictionary of the model names and the corresponding model labels
        self._model_dict_all = (
            df[['model_short', 'model_long']]
            .drop_duplicates()
            .set_index('model_long')['model_short']
            .to_dict()
        )
    
    # ------------------------------------------------------------------
    # Model filtering helpers
    # ------------------------------------------------------------------

    def get_filtered_model_dict(self, station_code: str, selected_date) -> dict:
        """Return models available for a given station + date."""
        # Update the model_dict with the models we have results for for the selected station
        # Model dict can be empty if no forecasts at all are available for the selected station
        return processing.update_model_dict_date(
            self._model_dict_all, self.forecasts_all,
            station_code, selected_date,
        )

    def get_preselected_models(self, station_code: str, pentad, decad) -> list:
        """Return the best models for a station/pentad combination."""
        return processing.get_best_models_for_station_and_pentad(
            self.forecasts_all, station_code, pentad, decad,
        )

    def resolve_model_values(self, model_dict: dict, preselected: list) -> list:
        """
        Map pre-selected model keys to widget values, handling
        ensemble name mismatches gracefully.
        """
        # Add models to value list safely
        values = []
        for model in preselected:
            if model in model_dict:
                values.append(model_dict[model])
            elif "Ens. Mean" in model:
                # Find any Neural Ensemble model in the dictionary
                match = next(
                    (model_dict[k] for k in model_dict if "Ens. Mean" in k),
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

    def update_sites_for_pentad(self, _, pentad, decad) -> None:
        """Refresh hydrograph statistics + linear regression predictor on sites."""
        self._sites_list = utils.update_site_attributes_with_hydrograph_statistics_for_selected_pentad(
            _=_, sites=self._sites_list,
            df=self.hydrograph_pentad_all,
            pentad=pentad, decad=decad,
            horizon=self._horizon,
            horizon_in_year=self._horizon_in_year,
        )
        self._sites_list = utils.update_site_attributes_with_linear_regression_predictor(
            _, sites=self._sites_list,
            df=self.linreg_predictor,
            pentad=pentad, decad=decad,
            horizon=self._horizon,
            horizon_in_year=self._horizon_in_year,
        )

    # ------------------------------------------------------------------
    # Station metadata replacement (async iehhf callback)
    # ------------------------------------------------------------------

    def replace_stations(self, new_all_stations, new_station_dict, station_widget,
                         _, pentad, decad) -> None:
        """
        Called from the background thread callback when iehhf stations finish
        loading.  Updates internal state and refreshes site attributes.
        """
        logger.info(f"Replacing stations: {len(new_all_stations)} loaded from iehhf")
        self._all_stations = new_all_stations
        station_widget.groups = new_station_dict

        self._sites_list = Site.get_site_attribues_from_iehhf_dataframe(new_all_stations)
        self.update_sites_for_pentad(_, pentad, decad)

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

    def get_bulletin_metadata(self):
        """Return (last_date, forecast_horizon, forecast_year) for bulletin saving."""
        # Get the last available date in the data
        last_date = self.forecasts_all['date'].max() + dt.timedelta(days=1)
        # The forecast is produced on the day before the first day of the forecast
        # pentad, therefore we add 1 to the forecast pentad in linreg_predictor to get
        # the pentad of the forecast period.
        forecast_horizon = int(
            self.linreg_predictor[self._horizon_in_year].tail(1).values[0]
        ) + 1
        # return last_date, forecast_horizon, last_date.year
        return last_date, 9, last_date.year

    # @property
    # def linreg_datatable(self):
    #     """Shifted linreg_predictor for display (1-day shift)."""
    #     return processing.shift_date_by_n_days(self.linreg_predictor, 1)
