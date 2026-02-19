import os
import pandas as pd
import panel as pn
from src.gettext_config import _
from dashboard.logger import setup_logger

logger = setup_logger()


def _get_bulletin_csv_path(year, horizon_value, save_directory):
    """Generate CSV path with pentad information"""
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    horizon_string = f"{horizon_value:02d}"
    bulletin_filename = f'bulletin_{horizon}_{year}_{horizon_string}.csv'
    return os.path.join(save_directory, bulletin_filename)


# Function to load bulletin data from CSV
def _load_bulletin_from_csv(forecast_year, forecast_horizon, save_directory, sites_list):
    """Load bulletin data from CSV file for current pentad"""
    current_bulletin_path = _get_bulletin_csv_path(forecast_year, forecast_horizon, save_directory)
    print(f"DEBUG: bulletin_manager.py: current_bulletin_path: {current_bulletin_path}")

    if not os.path.exists(current_bulletin_path):
        logger.info(f"No bulletin data found for current pentad {forecast_horizon}")
        return []
    
    print(f"DEBUG: bulletin_manager.py: Bulletin path exists: {current_bulletin_path}")
    try:
        bulletin_df = pd.read_csv(current_bulletin_path, encoding='utf-8-sig')

        # Rename columns from original English to localized names for UI consistency
        bulletin_df_display = bulletin_df.rename(columns={
            'station_label': _('Hydropost'),
            'model_short': _('Model'),
            'basin_ru': _('Basin'),
            'forecasted_discharge': _('Forecasted discharge'),
            'fc_lower': _('Forecast lower bound'),
            'fc_upper': _('Forecast upper bound'),
            'delta': _('δ'),
            'sdivsigma': _('s/σ'),
            'mae': _('MAE'),
            'accuracy': _('Accuracy')
        })

        bulletin_sites = []
        for code in bulletin_df['code'].unique():
            site_data = bulletin_df_display[bulletin_df['code'] == code].copy()
            site = next((s for s in sites_list if s.code == str(code)), None)
            if site:
                # Assign forecasts to the site
                site.forecasts = site_data.drop(columns=['code', _('Hydropost'), _('Basin')])
                # Update site attributes
                site.get_forecast_attributes_for_site(_, site.forecasts)
                bulletin_sites.append(site)

        print(f"DEBUG: Loaded bulletin_sites from CSV for pentad {forecast_horizon}:")
        for site in bulletin_sites:
            print(f"Site '{site.code}' with forecasts: {site.forecasts}")

        logger.info(f"Loaded bulletin data for pentad {forecast_horizon}")
        return bulletin_sites
    except Exception as e:
        logger.error(f"Error loading bulletin CSV: {e}")
        return []


# Function to save bulletin data to CSV
def _save_bulletin_to_csv(forecast_year, forecast_horizon, save_directory, bulletin_sites):
    """Save bulletin data to CSV file."""

    current_bulletin_path = _get_bulletin_csv_path(forecast_year, forecast_horizon, save_directory)

    data = []
    for site in bulletin_sites:
        # We need to extract the forecast data and site information
        for idx, forecast_row in site.forecasts.iterrows():
            row_data = forecast_row.to_dict()
            row_data['code'] = site.code
            row_data['station_label'] = site.station_label
            row_data['basin_ru'] = getattr(site, 'basin_ru', '')
            data.append(row_data)

    if data:
        bulletin_df_display = pd.DataFrame(data)

        # Translate the localized columns back to their original names
        bulletin_df = bulletin_df_display.rename(columns={
            _('Hydropost'): 'station_label',
            _('Model'): 'model_short',
            _('Basin'): 'basin_ru',
            _('Forecasted discharge'): 'forecasted_discharge',
            _('Forecast lower bound'): 'fc_lower',
            _('Forecast upper bound'): 'fc_upper',
            _('δ'): 'delta',
            _('s/σ'): 'sdivsigma',
            _('MAE'): 'mae',
            _('Accuracy'): 'accuracy'
        })

        try:
            bulletin_df.to_csv(current_bulletin_path, index=False, encoding='utf-8-sig')
            print(f"Bulletin saved to CSV for pentad {forecast_horizon}")
            logger.info(f"Bulletin saved to CSV for pentad {forecast_horizon}")
        except Exception as e:
            logger.error(f"Error writing bulletin CSV: {e}")
    else:
        # If data is empty, remove the CSV file
        # If data is empty, remove the current pentad's CSV file
        if os.path.exists(current_bulletin_path):
            os.remove(current_bulletin_path)
            print(f"Bulletin CSV file removed for pentad {forecast_horizon} because bulletin is empty")
            logger.info(f"Bulletin CSV file removed for pentad {forecast_horizon} because bulletin is empty")


class BulletinManager:
    """Encapsulates all bulletin state and wiring for the forecast dashboard."""

    def __init__(self, *, wm, cfg, dm, processing, write_to_excel):
        self.wm = wm
        self.cfg = cfg
        self.dm = dm
        self._processing = processing
        self._write_to_excel = write_to_excel

        # --- Load persisted bulletin sites ---
        self.bulletin_sites = _load_bulletin_from_csv(
            wm.forecast_year, wm.forecast_horizon,
            cfg.save_directory, dm.sites_list,
        )

        # --- Disable "Add to Bulletin" while pipeline runs ---
        # Set the initial state of the button based on whether the pipeline is running
        wm.add_to_bulletin_button.disabled = cfg.viz.app_state.pipeline_running
        # Watch for changes in pipeline_running and update the add_to_bulletin_button
        cfg.viz.app_state.param.watch(self._sync_add_button_to_pipeline, 'pipeline_running')

        # --- Initial table render & basin filter watcher ---
        self._update_bulletin_table()
        wm.select_basin.param.watch(lambda event: self._update_bulletin_table(), 'value')

        # --- Button callbacks ---
        wm.add_to_bulletin_button.on_click(self._on_add)
        wm.remove_bulletin_button.on_click(self._on_remove)
        wm.write_bulletin_button.on_click(self._on_write)
    
    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _save(self):
        _save_bulletin_to_csv(
            self.wm.forecast_year, self.wm.forecast_horizon,
            self.cfg.save_directory, self.bulletin_sites,
        )
    
    def _update_bulletin_table(self):
        # Function to update the bulletin table
        create_bulletin_table(
            self.bulletin_sites, self.wm.select_basin, self.wm.bulletin_tabulator
        )
    
    # Adding the watcher logic for disabling the "Add to Bulletin" button
    def _sync_add_button_to_pipeline(self, event) -> None:
        """Disable 'Add to Bulletin' while the pipeline is running."""
        # Update the state of 'Add to Bulletin' button based on pipeline_running status.
        self.wm.add_to_bulletin_button.disabled = event.new
    
    # ------------------------------------------------------------------
    # Button handlers
    # ------------------------------------------------------------------
    # Function to handle adding the current selection to the bulletin
    def _on_add(self, event=None):
        """Handle adding the current selection to the bulletin."""
         # Ensure pipeline is not running
        if self.cfg.viz.app_state.pipeline_running:
            print("Cannot add to bulletin while containers are running.")
            return

        forecast_df = self.wm.forecast_tabulator.value
        if forecast_df is None or forecast_df.empty:
            print("Forecast summary table is empty.")
            return
        
        selected_indices = self.wm.forecast_tabulator.selection
        if not selected_indices and len(forecast_df) > 0:
            selected_indices = [0]

        selected_rows = forecast_df.iloc[selected_indices]
        selected_station = self.wm.station.value
        selected_site = next(
            (s for s in self.dm.sites_list if s.station_label == selected_station),
            None,
        )
        if selected_site is None:
            print(f"Site '{selected_station}' not found in sites_list.")
            return
        
        # Assign forecasts to selected site object        
        selected_site.forecasts = selected_rows.reset_index(drop=True)
        # Add forecast attributes to site object
        selected_site.get_forecast_attributes_for_site(_, selected_rows)
        # Debugging: Print site details
        print(f"DEBUG: Added site '{selected_site.code}' to bulletin with forecasts: {selected_site.forecasts}")

        # Flash success popup
        self.wm.add_to_bulletin_popup.object = _("Added to bulletin table")
        self.wm.add_to_bulletin_popup.alert_type = "success"
        self.wm.add_to_bulletin_popup.visible = True
        pn.state.add_periodic_callback(
            lambda: setattr(self.wm.add_to_bulletin_popup, 'visible', False),
            2000, count=1,
        )

        # Upsert into bulletin_sites
        existing = next(
            (s for s in self.bulletin_sites if s.code == selected_site.code), None
        )
        if existing is None:
            self.bulletin_sites.append(selected_site)
            print(f"DEBUG: Added new site '{selected_site.station_label}' to bulletin_sites.")
        else:
            self.bulletin_sites[self.bulletin_sites.index(existing)] = selected_site
            print(f"DEBUG: Updated existing site '{selected_site.station_label}' in bulletin_sites.")

        # Save updated data to CSV for persistence
        self._save()
        # Update bulletin table
        self._update_bulletin_table()
    
    # Function to remove selected forecasts from the bulletin    
    def _on_remove(self, event=None):
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

        # Remove the selected sites from bulletin_sites
        for hydropost in selected_hydroposts:
            site = next(
                (s for s in self.bulletin_sites if s.station_label == hydropost), None
            )
            if site:
                self.bulletin_sites.remove(site)
                logger.info(f"Removed site from bulletin: {hydropost}")
        
        # Save the updated bulletin to CSV
        self._save()
        # Update the bulletin table to reflect the changes
        self._update_bulletin_table()

        # Show a success message
        print("Selected forecasts have been removed from the bulletin.")
        self.wm.add_to_bulletin_popup.object = _("Selected forecasts have been removed from the bulletin.")
        self.wm.add_to_bulletin_popup.alert_type = "success"
        self.wm.add_to_bulletin_popup.visible = True
        pn.state.add_periodic_callback(
            lambda: setattr(self.wm.add_to_bulletin_popup, 'visible', False),
            2000, count=1,
        )
    
    # Function to handle writing bulletin to Excel
    def _on_write(self, event=None):
        """Handle writing the bulletin to Excel."""
        try:
            if not self.bulletin_sites:
                print("DEBUG: No sites in bulletin to write.")
                return

            selected_basin = self.wm.select_basin.value
            if selected_basin == _("All basins"):
                filtered = self.bulletin_sites.copy()
            else:
                filtered = [
                    s for s in self.bulletin_sites
                    if getattr(s, 'basin_ru', '') == selected_basin
                ]

            if not filtered:
                print("DEBUG: No sites in bulletin for the selected basin.")
                return
            
            # Debugging: print the site details being written            
            for site in filtered:
                print(f"DEBUG: Writing site '{site.code}' with forecasts: {site.forecasts}")

            last_date, forecast_horizon, forecast_year = self.dm.get_bulletin_metadata()
            bulletin_header_info = self._processing.get_bulletin_header_info(
                last_date, self.cfg.horizon,
            )

            self._write_to_excel(
                self.dm.sites_list, filtered, bulletin_header_info,
                self.cfg.env_file_path,
            )
            print("DEBUG: Bulletin written to Excel successfully.")
            # Refresh the file downloader panel
            self.wm.downloader.refresh_file_list()
        except Exception as e:
            logger.error(f"Error writing bulletin to Excel: {e}")

        
# Function to create the bulletin table
def create_bulletin_table(bulletin_sites, select_basin_widget, bulletin_tabulator):
    # global bulletin_tabulator  # Declare as global to modify the global variable
    print("Creating/updating bulletin table...")

    if bulletin_sites:
        data = []
        for site in bulletin_sites:
            for idx, forecast_row in site.forecasts.iterrows():
                data.append({
                    _('Hydropost'): site.station_label,
                    _('Model'): forecast_row.get(_('Model'), ''),
                    _('Basin'): getattr(site, 'basin_ru', ''),
                    _('Forecasted discharge'): forecast_row.get(_('Forecasted discharge'), ''),
                    _('Forecast lower bound'): forecast_row.get(_('Forecast lower bound'), ''),
                    _('Forecast upper bound'): forecast_row.get(_('Forecast upper bound'), ''),
                    _('δ'): forecast_row.get('δ', ''),
                    _('s/σ'): forecast_row.get('s/σ', ''),
                    _('MAE'): forecast_row.get('MAE', ''),
                    _('Accuracy'): forecast_row.get(_('Accuracy'), ''),
                    # Add other fields as needed
                })
        bulletin_df = pd.DataFrame(data)

        # Apply 'Select Basin' filter if applicable
        selected_basin = select_basin_widget.value
        if selected_basin != _("All basins"):
            bulletin_df = bulletin_df[bulletin_df['Basin'] == selected_basin]

        bulletin_tabulator.value = bulletin_df
    else:
        # Empty DataFrame with predefined columns
        bulletin_df = pd.DataFrame(columns=[
            _('Hydropost'), _('Model'), _('Basin'),
            _('Forecasted discharge'), _('Forecast lower bound'), _('Forecast upper bound'),
            _('δ'), _('s/σ'), _('MAE'), _('Accuracy')
        ])

        # Update the Tabulator's value
        bulletin_tabulator.value = bulletin_df

    print("Bulletin table updated.")
