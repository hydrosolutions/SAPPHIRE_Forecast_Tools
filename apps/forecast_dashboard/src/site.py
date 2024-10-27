# A class representing a site (hydropost or meteo station in the context of this project).
#
# A site is identified by a unique ID and has a name, location, and other attributes.
# It further has forecasts for different models and forecast horizons.

import pandas as pd

# Logging
import logging
logger = logging.getLogger(__name__)


class SapphireSite:

    def __init__(self,
                 code: str = None,
                 name_ru: str = None,
                 name_nat: str = None,
                 river_name_ru: str = None,
                 punkt_name_ru: str = None,
                 river_name_nat: str = None,
                 punkt_name_nat: str = None,
                 station_label: str = None,
                 lat: float = None,
                 lon: float = None,
                 region_ru: str = None,
                 region_nat: str = None,
                 basin_ru: str = None,
                 basin_nat: str = None,
                 qdanger: float = None,
                 histqmin: float = None,
                 histqmax: float = None,
                 bulletin_order: int = None,
                 linreg_predictor: float = None,):
        """
        Constructs a Site object with the given attributes.

        Site attributes should be associated with data tags in ieasyreports.
        """
        self.code = code if code else None
        self.name_ru = name_ru if name_ru else None
        self.name_nat = name_nat if name_nat else None
        self.river_name_ru = river_name_ru if river_name_ru else None
        self.punkt_name_ru = punkt_name_ru if punkt_name_ru else None
        self.river_name_nat = river_name_nat if river_name_nat else None
        self.punkt_name_nat = punkt_name_nat if punkt_name_nat else None
        self.station_label = f"{self.code} - {self.river_name_ru} {self.punkt_name_ru}" if station_label else None
        self.lat = lat if lat else None
        self.lon = lon if lon else None
        self.region_ru = region_ru if region_ru else None
        self.region_nat = region_nat if region_nat else None
        self.basin_ru = basin_ru if basin_ru else None
        self.basin_nat = basin_nat if basin_nat else None
        self.qdanger = qdanger if qdanger else None
        self.histqmin = histqmin if histqmin else None
        self.histqmax = histqmax if histqmax else None
        self.bulletin_order = bulletin_order if bulletin_order else None
        self.linreg_predictor = linreg_predictor if linreg_predictor else None

    def __str__(self):
        return f"Site {self.code} ({self.river_name_ru} - {self.punkt_name_ru})"

    @classmethod
    def get_site_attribues_from_iehhf_dataframe(cls, df: pd.DataFrame,
        code_col: str = 'code',
        name_ru_col: str = 'station_labels',
        name_nat_col: str = 'name_nat',
        river_name_ru_col: str = 'river_name',
        river_name_nat_col: str = 'river_name_nat',
        punkt_name_ru_col: str = 'punkt_name',
        punkt_name_nat_col: str = 'punkt_name_nat',
        latitude_col: str = 'lat',
        longitude_col: str = 'lon',
        region_ru_col: str = 'region',
        region_nat_col: str = 'region_nat',
        basin_ru_col: str = 'basin',
        basin_nat_col: str = 'basin_nat',
        qdanger_col: str = 'qdanger',
        histqmin_col: str = 'histqmin',
        histqmax_col: str = 'histqmax',
        bulletin_order_col: str = 'bulletin_order',):
        """
        Creates a list of Site objects from a DataFrame.

        Returns:
            list: A list of Site objects.
        """

        try:
            # Create a list of Site objects from the DataFrame
            sites = []
            for index, row in df.iterrows():
                logger.debug(f"Creating Site object from row {index} ...")
                #print(f"row.columns: {df.columns}")
                #print(f"row['bulletin_order']: {row['bulletin_order']}")
                site = cls(
                    code=row[code_col] if code_col in df.columns else None,
                    river_name_ru=row[river_name_ru_col] if river_name_ru_col in df.columns else None,
                    punkt_name_ru=row[punkt_name_ru_col] if punkt_name_ru_col in df.columns else None,
                    river_name_nat=row[river_name_nat_col] if river_name_nat_col in df.columns else None,
                    punkt_name_nat=row[punkt_name_nat_col] if punkt_name_nat_col in df.columns else None,
                    name_ru=row[name_ru_col] if name_ru_col in df.columns else None,
                    name_nat=row[name_nat_col] if name_nat_col in df.columns else None,
                    lat=row[latitude_col] if latitude_col in df.columns else 0.0,
                    lon=row[longitude_col] if longitude_col in df.columns else 0.0,
                    region_ru=row[region_ru_col] if region_ru_col in df.columns else None,
                    region_nat=row[region_nat_col] if region_nat_col in df.columns else None,
                    basin_ru=row[basin_ru_col] if basin_ru_col in df.columns else None,
                    basin_nat=row[basin_nat_col] if basin_nat_col in df.columns else None,
                    qdanger=row[qdanger_col] if qdanger_col in df.columns else None,
                    histqmin=row[histqmin_col] if histqmin_col in df.columns else None,
                    histqmax=row[histqmax_col] if histqmax_col in df.columns else None,
                    bulletin_order=row[bulletin_order_col] if bulletin_order_col in df.columns else None
                )
                site.station_label = f"{site.code} - {site.name_ru}"
                #print(f"Created site {site.code} with {site.station_label}.")
                sites.append(site)

            # order the sites by basin and bulletin order
            # Get the basin and bulletin order for each site
            df = pd.DataFrame({
                'codes': [site.code for site in sites],
                'basins': [site.basin_ru for site in sites],
                'bulletin_order': [site.bulletin_order for site in sites]
            })
            # Sort the sites_list according to the basin and bulletin order
            df = df.sort_values(by=['basins', 'bulletin_order'])
            print(f"Ordered sites: {df}")
            # Get the ordered list of codes
            ordered_codes = df['codes'].tolist()
            # Get site where site.code == ordered_codes[0]
            ordered_sites_list = []
            # Create a new list of sites in the order of the ordered_codes
            for code in ordered_codes:
                temp_site = next((site for site in sites if site.code == code), None)
                #print(f"temp_site: {temp_site}")
                # Test if temp_site is None
                if temp_site is None:
                    print(f"Site with code {code} not found.")
                    continue
                # Add the site to the ordered_sites_list
                # Test if ordered_sits_list is 'NoneType'
                if ordered_sites_list is None:
                    print(f"ordered_sites_list is NoneType")
                    ordered_sites_list = [temp_site]
                else: # ordered_sites_list is not 'NoneType'
                    ordered_sites_list.append(temp_site)
            print(f"Ordered sites: {[site.code for site in ordered_sites_list]}")

            return ordered_sites_list
        except Exception as e:
            print(f'Error creating Site objects from DataFrame: {e}')
            return []

    @classmethod
    def get_site_attributes_from_stations_dataframe(cls, df: pd.DataFrame,
                       site_code_col: str = 'site_code',
                       river_ru_col: str = 'river_ru',
                       punkt_ru_col: str = 'punkt_ru',
                       river_nat_col: str = 'river_nat',
                       punkt_nat_col: str = 'punkt_nat',
                       latitude_col: str = 'latitude',
                       longitude_col: str = 'longitude',
                       region_col: str = 'region',
                       basin_col: str = 'basin') -> list:
        """
        Creates a list of Site objects from a DataFrame.

        Returns:
            list: A list of Site objects.
        """

        try:
            # Create a list of Site objects from the DataFrame
            sites = []
            for index, row in df.iterrows():
                logger.debug(f"Creating Site object from row {index} ...")
                print(f"row: {row}")
                site = cls(
                    code=row[site_code_col] if site_code_col in df.columns else None,
                    river_name_ru=row[river_ru_col] if river_ru_col in df.columns else None,
                    punkt_name_ru=row[punkt_ru_col] if punkt_ru_col in df.columns else None,
                    river_name_nat=row[river_nat_col] if river_nat_col in df.columns else None,
                    punkt_name_nat=row[punkt_nat_col] if punkt_nat_col in df.columns else None,
                    station_label=f"{row[site_code_col]} - {row[river_ru_col]} {row[punkt_ru_col]}" if site_code_col in df.columns else None,
                    lat=row[latitude_col] if latitude_col in df.columns else 0.0,
                    lon=row[longitude_col] if longitude_col in df.columns else 0.0,
                    region=row[region_col] if region_col in df.columns else None,
                    basin=row[basin_col] if basin_col in df.columns else None
                )
                sites.append(site)
            return sites
        except Exception as e:
            print(f'Error creating Site objects from DataFrame: {e}')
            return []

    def get_forecast_attributes_for_site(self, _, df: pd.DataFrame,):
        """
        Fills the Site object with forecast attributes from a DataFrame.

        Returns:
            Site: A SapphireSite object.
        """
        print(f"\n\nget_forecast_attributes_for_site: dataframe: {df}")
        self.forecast_pentad = df['Forecasted discharge'].values[0]
        self.forecast_lower_bound = df['Forecast lower bound'].values[0]
        self.forecast_upper_bound = df['Forecast upper bound'].values[0]
        self.forecast_delta = df['δ'].values[0]
        self.forecast_sdivsigma = df['s/σ'].values[0]
        self.forecast_mae = df['MAE'].values[0]
        self.forecast_accuracy = df['Accuracy'].values[0]
        #self.forecast_nse = df['NSE']  # Not available yet
        self.forecast_model = df['Model'].values[0]
        # Calculate percentage of norm
        self.perc_norm = round((self.forecast_pentad / self.hydrograph_mean) *200, 2)
        print(f"Updated site {self.code} with forecast attributes from DataFrame.")

    def get_site_attributes_from_selected_forecast(cls,
            _,
            sites: list,
            site_selection, # Selected site from the dropdown menu
            tabulator  # Tabulator widget that is displayed in the forecast tab
            ):
        """
        Fills the Site object with selected forecast attributes from a DataFrame.

        Returns:
            list: A list of Site objects.
        """

        print("debug: site_selection: ", site_selection)
        # Selected site
        selected_site_label = site_selection
        selected_site = next((site for site in sites if site.station_label == selected_site_label), None)

        if selected_site and tabulator.selection:
            selected_row = tabulator.value
            print("\n---\n---\nSelected row: ", selected_row, "\n---\n---\n")
            selected_site.forecast_model = selected_row[_('Model')]
            selected_site.forecast_pentad = selected_row[_('Forecasted discharge')]
            selected_site.forecast_lower_bound = selected_row[_('Forecast lower bound')]
            selected_site.forecast_upper_bound = selected_row[_('Forecast upper bound')]
            selected_site.forecast_delta = selected_row[_('δ')]
            selected_site.forecast_sdivsigma = selected_row[_('s/σ')]
            selected_site.forecast_mae = selected_row[_('MAE')]
            selected_site.forecast_accuracy = selected_row[_('Accuracy')]
            print(f"Updated site {selected_site.station_label} with forecast attributes from row {selected_row}.")
        else:
            print(f"No site or row selected.")

    def oder_sites_list_according_to_bulletin_order(cls, sites_list):
        """Order the sites_list according to the order in the attribute bulletin_order of each site"""
        # Get the basin and bulletin order for each site
        df = pd.DataFrame({
            'codes': [site.code for site in sites_list],
            'basins': [site.basin_ru for site in sites_list],
            'bulletin_order': [site.bulletin_order for site in sites_list]
        })
        # Sort the sites_list according to the basin and bulletin order
        df = df.sort_values(by=['basins', 'bulletin_order'])
        print(f"Ordered sites: {df}")
        # Get the ordered list of codes
        ordered_codes = df['codes'].tolist()
        # Create a new list of sites in the order of the ordered_codes
        ordered_sites_list = [site for site in sites_list if site.code in ordered_codes]
        return ordered_sites_list