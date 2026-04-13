import pandas as pd
import panel as pn

from src.gettext_config import _
from dashboard.logger import setup_logger
from src import db  # _read_data, _save_data, _delete_data live here

logger = setup_logger()

_SERVICE  = "postprocessing"
_RESOURCE = "bulletin"

_HYDROGRAPH_DEFAULTS = {
    "hydrograph_mean":         float('nan'),
    "hydrograph_norm":         float('nan'),
    "hydrograph_max":          float('nan'),
    "hydrograph_min":          float('nan'),
    "last_year_q_pentad_mean": float('nan'),
    "linreg_predictor":        float('nan'),
}

def _ensure_site_defaults(site) -> None:
    """Set hydrograph/predictor attributes to None if not yet hydrated.

    `get_forecast_attributes_for_site` reads these; at bulletin-load time
    the site objects have not yet been through the statistics update, so
    we guard against AttributeError by initialising any missing ones here.
    """
    for attr, default in _HYDROGRAPH_DEFAULTS.items():
        if not hasattr(site, attr):
            setattr(site, attr, default)

# ---------------------------------------------------------------------------
# Bulletin-specific API helpers  (thin wrappers around db primitives)
# ---------------------------------------------------------------------------

def _site_to_records(horizon_type: str, year: int, horizon_value: int, site) -> list[dict]:
    """Flatten one site's forecasts DataFrame into API-ready bulletin dicts."""
    records = []
    for _idx, row in site.forecasts.iterrows():
        model = row.get(_('Model'), '')
        if not model:
            continue
        records.append({
            "horizon_type":        horizon_type,
            "year":                year,
            "horizon_value":       horizon_value,
            "code":                site.code,
            "model_type":          model,
            "basin_name":          getattr(site, 'basin_ru', ''),
            "station_label":       site.station_label,
            "forecasted_discharge": row.get(_('Forecasted discharge')),
            "fc_lower":            row.get(_('Forecast lower bound')),
            "fc_upper":            row.get(_('Forecast upper bound')),
            "delta":               row.get(_('δ')),
            "sdivsigma":           row.get(_('s/σ')),
            "mae":                 row.get(_('MAE')),
            "accuracy":            row.get(_('Accuracy')),
        })
    return records


def _load_bulletin_from_api(horizon_type: str, forecast_year: int, forecast_horizon: int, sites_list) -> list:
    """Fetch bulletin records from the API and reconstruct site objects."""
    try:
        df = db._read_data(_SERVICE, _RESOURCE, {
            "horizon":       horizon_type,
            "year":          forecast_year,
            "horizon_value": forecast_horizon,
            "limit":         1000,
        })

        if df.empty:
            logger.info("No bulletin records found for %s %s value=%s",
                        horizon_type, forecast_year, forecast_horizon)
            return []

        bulletin_sites = []
        for code in df["code"].unique():
            site_df = df[df["code"] == code]
            site = next((s for s in sites_list if s.code == str(code)), None)
            if site is None:
                logger.warning("Bulletin record references unknown site code '%s', skipping.", code)
                continue

            site.forecasts = pd.DataFrame([
                {
                    _('Model'):                row["model_type"],
                    _('Forecasted discharge'): row.get("forecasted_discharge"),
                    _('Forecast lower bound'): row.get("fc_lower"),
                    _('Forecast upper bound'): row.get("fc_upper"),
                    _('δ'):                    row.get("delta"),
                    _('s/σ'):                  row.get("sdivsigma"),
                    _('MAE'):                  row.get("mae"),
                    _('Accuracy'):             row.get("accuracy"),
                }
                for _idx, row in site_df.iterrows()
            ])
            site.forecasts = site.forecasts.where(site.forecasts.notna(), other=float('nan'))
            _ensure_site_defaults(site)
            site.get_forecast_attributes_for_site(_, site.forecasts)
            bulletin_sites.append(site)

        logger.info("Loaded %d bulletin sites from API", len(bulletin_sites))
        return bulletin_sites

    except Exception as e:
        logger.error("Error loading bulletin from API: %s", e)
        return []


def _save_bulletin_to_api(horizon_type: str, forecast_year: int, forecast_horizon: int, bulletin_sites: list) -> None:
    """Upsert bulletin site records to the API."""
    records = []
    for site in bulletin_sites:
        records.extend(_site_to_records(horizon_type, forecast_year, forecast_horizon, site))

    if not records:
        logger.info("No bulletin records to save.")
        return

    db._save_data(_SERVICE, _RESOURCE, records)


def _delete_site_from_api(horizon_type: str, forecast_year: int, forecast_horizon: int, site) -> None:
    """Delete the bulletin record for one site from the API bulletin table."""
    db._delete_data(_SERVICE, _RESOURCE, {
        "horizon":       horizon_type,
        "year":          forecast_year,
        "horizon_value": forecast_horizon,
        "code":          site.code,
    })


# ---------------------------------------------------------------------------
# BulletinManager
# ---------------------------------------------------------------------------

class BulletinManager:
    """Encapsulates all bulletin state and wiring for the forecast dashboard."""

    def __init__(self, *, wm, cfg, dm, processing, write_to_excel):
        self.wm = wm
        self.cfg = cfg
        self.dm = dm
        self._processing = processing
        self._write_to_excel = write_to_excel

        # --- Load persisted bulletin sites ---
        self.bulletin_sites = _load_bulletin_from_api(
            wm.horizon_selector.value,
            wm.forecast_year,
            wm.forecast_horizon,
            dm.sites_list,
        )

        # --- Disable "Add to Bulletin" while pipeline runs ---
        # Set the initial state of the button based on whether the pipeline is running
        wm.add_to_bulletin_button.disabled = cfg.viz.app_state.pipeline_running
        # Watch for changes in pipeline_running and update the add_to_bulletin_button
        cfg.viz.app_state.param.watch(self._sync_add_button_to_pipeline, 'pipeline_running')

        # --- Initial table render & basin filter watcher ---
        self._update_bulletin_table()
        wm.basin_selector.param.watch(lambda event: self._update_bulletin_table(), 'value')
        wm.register_post_load_callback(self._on_horizon_change)

        # --- Button callbacks ---
        wm.add_to_bulletin_button.on_click(self._on_add)
        wm.remove_bulletin_button.on_click(self._on_remove)
        wm.write_bulletin_button.on_click(self._on_write)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _horizon_context(self) -> tuple[str, int, int]:
        return (
            self.wm.horizon_selector.value,
            self.wm.forecast_year,
            self.wm.forecast_horizon,
        )

    def _on_horizon_change(self) -> None:
        """Reload bulletin after station/period data has finished loading.

        Called via wm._post_load_callbacks so it always runs AFTER
        dm.load_station() and dm.get_bulletin_metadata() have both
        completed with the current horizon.
        """
        horizon = self.wm.horizon_selector.value
        try:
            _last_date, forecast_horizon, forecast_year = (
                self.dm.get_bulletin_metadata(horizon)
            )
        except (KeyError, IndexError, TypeError, ValueError):
            logger.info("No %s data available yet, clearing bulletin.", horizon)
            self.bulletin_sites = []
            self._update_bulletin_table()
            return

        self.wm.forecast_horizon = forecast_horizon
        self.wm.forecast_year = forecast_year

        self.bulletin_sites = _load_bulletin_from_api(
            horizon, forecast_year, forecast_horizon,
            self.dm.sites_list,
        )
        self._update_bulletin_table()

    def _update_bulletin_table(self) -> None:
        # Function to update the bulletin table
        create_bulletin_table(
            self.bulletin_sites, self.wm.basin_selector, self.wm.bulletin_tabulator
        )
    
    # Adding the watcher logic for disabling the "Add to Bulletin" button
    def _sync_add_button_to_pipeline(self, event) -> None:
        """Disable 'Add to Bulletin' while the pipeline is running."""
        # Update the state of 'Add to Bulletin' button based on pipeline_running status.
        self.wm.add_to_bulletin_button.disabled = event.new

    def _show_popup(self, message: str, alert_type: str = "success") -> None:
        self.wm.add_to_bulletin_popup.object = message
        self.wm.add_to_bulletin_popup.alert_type = alert_type
        self.wm.add_to_bulletin_popup.visible = True
        pn.state.add_periodic_callback(
            lambda: setattr(self.wm.add_to_bulletin_popup, 'visible', False),
            2000, count=1,
        )

    # ------------------------------------------------------------------
    # Button handlers
    # ------------------------------------------------------------------
    # Function to handle adding the current selection to the bulletin
    def _on_add(self, event=None) -> None:
        if self.cfg.viz.app_state.pipeline_running:
            print("Cannot add to bulletin while containers are running.")
            return

        forecast_df = self.wm.forecast_tabulator.value
        if forecast_df is None or forecast_df.empty:
            print("Forecast summary table is empty.")
            return

        selected_indices = self.wm.forecast_tabulator.selection or ([0] if len(forecast_df) > 0 else [])
        selected_rows = forecast_df.iloc[selected_indices]
        selected_station = self.wm.station_selector.value
        selected_site = next(
            (s for s in self.dm.sites_list if s.station_label == selected_station), None
        )
        if selected_site is None:
            print(f"Site '{selected_station}' not found in sites_list.")
            return
        
        # Assign forecasts to selected site object        
        selected_site.forecasts = selected_rows.reset_index(drop=True)
        # Add forecast attributes to site object
        selected_site.get_forecast_attributes_for_site(_, selected_rows)
        # Debugging: Print site details
        print(f"DEBUG: Adding site '{selected_site.code}' to bulletin: {selected_site.forecasts}")

        existing = next((s for s in self.bulletin_sites if s.code == selected_site.code), None)
        if existing is None:
            self.bulletin_sites.append(selected_site)
            print(f"DEBUG: Added new site '{selected_site.station_label}' to bulletin_sites.")
        else:
            self.bulletin_sites[self.bulletin_sites.index(existing)] = selected_site
            print(f"DEBUG: Updated existing site '{selected_site.station_label}' in bulletin_sites.")

        _save_bulletin_to_api(*self._horizon_context(), [selected_site])
        # Update bulletin table
        self._update_bulletin_table()
        self._show_popup(_("Added to bulletin table"))
    
    # Function to remove selected forecasts from the bulletin    
    def _on_remove(self, event=None) -> None:
        """Handle removing selected forecasts from the bulletin."""
        # List of selected row indices
        selected = self.wm.bulletin_tabulator.selection
        if not selected:
            print("No forecasts selected for removal.")
            logger.warning("Remove action triggered, but no forecasts were selected.")
            return
        
        # Get the bulletin DataFrame from the tabulator
        bulletin_df = self.wm.bulletin_tabulator.value
        # Get the hydroposts of the selected rows
        selected_hydroposts = bulletin_df.iloc[selected][_('Hydropost')].unique()
        horizon_ctx = self._horizon_context()

        # Remove the selected sites from bulletin_sites and API
        for hydropost in selected_hydroposts:
            site = next((s for s in self.bulletin_sites if s.station_label == hydropost), None)
            if site is None:
                continue
            _delete_site_from_api(*horizon_ctx, site)
            self.bulletin_sites.remove(site)
            logger.info("Removed site from bulletin: %s", hydropost)
        
        # Update the bulletin table to reflect the changes
        self._update_bulletin_table()

        # Show a success message
        print("Selected forecasts have been removed from the bulletin.")
        self._show_popup(_("Selected forecasts have been removed from the bulletin."))
    
    # Function to handle writing bulletin to Excel
    def _on_write(self, event=None) -> None:
        """Handle writing the bulletin to Excel."""
        try:
            if not self.bulletin_sites:
                print("DEBUG: No sites in bulletin to write.")
                return

            selected_basin = self.wm.basin_selector.value
            filtered = (
                self.bulletin_sites.copy()
                if selected_basin == _("All basins")
                else [s for s in self.bulletin_sites if getattr(s, 'basin_ru', '') == selected_basin]
            )

            if not filtered:
                print("DEBUG: No sites in bulletin for the selected basin.")
                return

            # Debugging: print the site details being written            
            for site in filtered:
                print(f"DEBUG: Writing site '{site.code}' with forecasts: {site.forecasts}")

            last_date, forecast_horizon, forecast_year = self.dm.get_bulletin_metadata(
                self.wm.horizon_selector.value
            )
            bulletin_header_info = self._processing.get_bulletin_header_info(last_date, self.cfg.horizon)
            self._write_to_excel(self.dm.sites_list, filtered, bulletin_header_info, self.cfg.env_file_path)
            print("DEBUG: Bulletin written to Excel successfully.")
            # Refresh the file downloader panel
            self.wm.downloader.refresh_file_list()
        except Exception as e:
            logger.error("Error writing bulletin to Excel: %s", e)


# ---------------------------------------------------------------------------
# Table renderer
# ---------------------------------------------------------------------------

# Function to create the bulletin table
def create_bulletin_table(bulletin_sites, select_basin_widget, bulletin_tabulator):
    print("Creating/updating bulletin table...")

    if bulletin_sites:
        data = []
        for site in bulletin_sites:
            for _idx, forecast_row in site.forecasts.iterrows():
                data.append({
                    _('Hydropost'):            site.station_label,
                    _('Model'):                forecast_row.get(_('Model'), ''),
                    _('Basin'):                getattr(site, 'basin_ru', ''),
                    _('Forecasted discharge'): forecast_row.get(_('Forecasted discharge'), ''),
                    _('Forecast lower bound'): forecast_row.get(_('Forecast lower bound'), ''),
                    _('Forecast upper bound'): forecast_row.get(_('Forecast upper bound'), ''),
                    _('δ'):                    forecast_row.get('δ', ''),
                    _('s/σ'):                  forecast_row.get('s/σ', ''),
                    _('MAE'):                  forecast_row.get('MAE', ''),
                    _('Accuracy'):             forecast_row.get(_('Accuracy'), ''),
                })
        bulletin_df = pd.DataFrame(data)

        # Apply 'Select Basin' filter if applicable
        selected_basin = select_basin_widget.value
        if selected_basin != _("All basins"):
            bulletin_df = bulletin_df[bulletin_df[_('Basin')] == selected_basin]

        bulletin_tabulator.value = bulletin_df
    else:
        # Empty DataFrame with predefined columns
        bulletin_tabulator.value = pd.DataFrame(columns=[
            _('Hydropost'), _('Model'), _('Basin'),
            _('Forecasted discharge'), _('Forecast lower bound'), _('Forecast upper bound'),
            _('δ'), _('s/σ'), _('MAE'), _('Accuracy'),
        ])

    print("Bulletin table updated.")
