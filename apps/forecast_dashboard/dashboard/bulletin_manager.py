import os
import pandas as pd
import panel as pn
from src.gettext_config import _
from dashboard.logger import setup_logger

logger = setup_logger()


def get_bulletin_csv_path(year, horizon_value, save_directory):
    """Generate CSV path with pentad information"""
    horizon = os.getenv("sapphire_forecast_horizon", "pentad")
    if horizon == "pentad":
        horizon_string = f"{horizon_value:02d}"
    else:
        horizon_string = f"{horizon_value:02d}"
    bulletin_filename = f'bulletin_{horizon}_{year}_{horizon_string}.csv'
    return os.path.join(save_directory, bulletin_filename)


# Function to load bulletin data from CSV
def load_bulletin_from_csv(forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin, save_directory, sites_list):
    """Load bulletin data from CSV file for current pentad"""
    # global bulletin_sites

    # Get current pentad
    #current_date = pd.Timestamp.now()
    #current_year = current_date.year
    #current_pentad = tl.get_pentad_for_date(current_date)
    #current_bulletin_path = get_bulletin_csv_path(current_year, current_pentad)
    current_bulletin_path = get_bulletin_csv_path(forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin, save_directory)
    # Print bulletin path
    print(f"DEBUG: forecast_dashboard.py: current_bulletin_path: {current_bulletin_path}")

    if os.path.exists(current_bulletin_path):
        # Print that bulletin path exists
        print(f"DEBUG: forecast_dashboard.py: Bulletin path exists: {current_bulletin_path}")
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

            print(f"DEBUG: Loaded bulletin_sites from CSV for pentad {forecast_horizon_for_saving_bulletin}:")
            for site in bulletin_sites:
                print(f"Site '{site.code}' with forecasts: {site.forecasts}")

            logger.info(f"Loaded bulletin data for pentad {forecast_horizon_for_saving_bulletin}")
        except Exception as e:
            logger.error(f"Error loading bulletin CSV: {e}")
            bulletin_sites = []
    else:
        logger.info(f"No bulletin data found for current pentad {forecast_horizon_for_saving_bulletin}")
        bulletin_sites = []
    
    return bulletin_sites


# Function to save bulletin data to CSV
def save_bulletin_to_csv(forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin, save_directory, bulletin_sites):
    # Get current pentad
    #current_date = pd.Timestamp.now()
    #current_year = current_date.year
    #current_pentad = tl.get_pentad_for_date(current_date)

    # Generate path for current pentad's bulletin
    #current_bulletin_path = get_bulletin_csv_path(current_year, current_pentad)
    current_bulletin_path = get_bulletin_csv_path(forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin, save_directory)

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
            print(f"Bulletin saved to CSV for pentad {forecast_horizon_for_saving_bulletin}")
            logger.info(f"Bulletin saved to CSV for pentad {forecast_horizon_for_saving_bulletin}")
        except Exception as e:
            logger.error(f"Error writing bulletin CSV: {e}")
    else:
        # If data is empty, remove the CSV file
        # If data is empty, remove the current pentad's CSV file
        if os.path.exists(current_bulletin_path):
            os.remove(current_bulletin_path)
            print(f"Bulletin CSV file removed for pentad {forecast_horizon_for_saving_bulletin} because bulletin is empty")
            logger.info(f"Bulletin CSV file removed for pentad {forecast_horizon_for_saving_bulletin} because bulletin is empty")


# Function to handle adding the current selection to the bulletin
def add_current_selection_to_bulletin(event=None, *, viz, forecast_tabulator, station, sites_list, add_to_bulletin_popup, bulletin_sites, forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin,
                                      save_directory, update_bulletin_table):
    # Ensure pipeline is not running
    if viz.app_state.pipeline_running:
        print("Cannot add to bulletin while containers are running.")
        return

    selected_indices = forecast_tabulator.selection
    forecast_df = forecast_tabulator.value

    if forecast_df is None or forecast_df.empty:
        print("Forecast summary table is empty.")
        return

    if not selected_indices and len(forecast_df) > 0:
        selected_indices = [0]  # Default to the first row

    selected_rows = forecast_df.iloc[selected_indices]
    selected_station = station.value
    selected_site = next(
        (site for site in sites_list if site.station_label == selected_station),
        None
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

    add_to_bulletin_popup.object = _("Added to bulletin table")
    add_to_bulletin_popup.alert_type = "success"
    add_to_bulletin_popup.visible = True
    pn.state.add_periodic_callback(
        lambda: setattr(add_to_bulletin_popup, 'visible', False),
        2000, count=1)

    # Update or add to bulletin_sites
    existing_site = next(
        (site for site in bulletin_sites if site.code == selected_site.code),
        None
    )
    if existing_site is None:
        bulletin_sites.append(selected_site)
        print(f"DEBUG: Added new site '{selected_site.station_label}' to bulletin_sites.")
    else:
        index = bulletin_sites.index(existing_site)
        bulletin_sites[index] = selected_site
        print(f"DEBUG: Updated existing site '{selected_site.station_label}' in bulletin_sites.")

    # Save updated data to CSV for persistence
    # save_bulletin_to_csv()
    save_bulletin_to_csv(forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin, save_directory, bulletin_sites)

    # Update bulletin table
    update_bulletin_table()


# Function to handle writing bulletin to Excel
def handle_bulletin_write(event, *, bulletin_sites, select_basin_widget, write_to_excel, sites_list, bulletin_header_info, env_file_path, downloader):
    try:
        if not bulletin_sites:
            print("DEBUG: No sites in bulletin to write.")
            return

        selected_basin = select_basin_widget.value
        if selected_basin == _("All basins"):
            filtered_bulletin_sites = bulletin_sites.copy()
        else:
            filtered_bulletin_sites = [
                site for site in bulletin_sites if getattr(site, 'basin_ru', '') == selected_basin
            ]

        if not filtered_bulletin_sites:
            print("DEBUG: No sites in bulletin for the selected basin.")
            return

        # Debugging: print the site details being written
        for site in filtered_bulletin_sites:
            print(f"DEBUG: Writing site '{site.code}' with forecasts: {site.forecasts}")

        write_to_excel(
            sites_list, filtered_bulletin_sites, bulletin_header_info,
            env_file_path)
        print("DEBUG: Bulletin written to Excel successfully.")

        # Refresh the file downloader panel
        downloader.refresh_file_list()

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


# Function to remove selected forecasts from the bulletin
def remove_selected_from_bulletin(event=None, *, bulletin_tabulator, bulletin_sites, add_to_bulletin_popup, forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin, save_directory, update_bulletin_table):
    # global bulletin_tabulator
    selected = bulletin_tabulator.selection  # List of selected row indices

    if not selected:
        print("No forecasts selected for removal.")
        logger.warning("Remove action triggered, but no forecasts were selected.")
        return

    # Get the indices of the selected rows
    selected_indices = selected

    # Get the bulletin DataFrame from the tabulator
    bulletin_df = bulletin_tabulator.value

    # Get the hydroposts of the selected rows
    selected_rows = bulletin_df.iloc[selected_indices]
    selected_hydroposts = selected_rows[_('Hydropost')].unique()

    # Remove the selected sites from bulletin_sites
    for hydropost in selected_hydroposts:
        site_to_remove = next(
            (site for site in bulletin_sites if site.station_label == hydropost),
            None)
        if site_to_remove:
            bulletin_sites.remove(site_to_remove)
            logger.info(f"Removed site from bulletin: {hydropost}")

    # Save the updated bulletin to CSV
    # save_bulletin_to_csv()
    save_bulletin_to_csv(forecast_year_for_saving_bulletin, forecast_horizon_for_saving_bulletin, save_directory, bulletin_sites)

    # Update the bulletin table to reflect the changes
    update_bulletin_table()

    # Show a success message
    print("Selected forecasts have been removed from the bulletin.")
    add_to_bulletin_popup.object = _("Selected forecasts have been removed from the bulletin.")
    add_to_bulletin_popup.alert_type = "success"
    add_to_bulletin_popup.visible = True
    pn.state.add_periodic_callback(
        lambda: setattr(add_to_bulletin_popup, 'visible', False),
        2000, count=1)

