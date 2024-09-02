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
                 river_name_ru: str = None,
                 punkt_name_ru: str = None,
                 river_name_nat: str = None,
                 punkt_name_nat: str = None,
                 lat: float = None,
                 lon: float = None,
                 region: str = None,
                 basin: str = None):
        """
        Constructs a Site object with the given attributes.
        """
        self.code = code if code else None
        self.river_name_ru = river_name_ru if river_name_ru else None
        self.punkt_name_ru = punkt_name_ru if punkt_name_ru else None
        self.river_name_nat = river_name_nat if river_name_nat else None
        self.punkt_name_nat = punkt_name_nat if punkt_name_nat else None
        self.lat = lat if lat else None
        self.lon = lon if lon else None
        self.region = region if region else None
        self.basin = basin if basin else None

    def __str__(self):
        return f"Site {self.code} ({self.river_name_ru} - {self.punkt_name_ru})"

    @classmethod
    def get_site_attributes_from_dataframe(cls, df: pd.DataFrame,
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
                site = cls(
                    code=row[site_code_col] if site_code_col in df.columns else None,
                    river_name_ru=row[river_ru_col] if river_ru_col in df.columns else None,
                    punkt_name_ru=row[punkt_ru_col] if punkt_ru_col in df.columns else None,
                    river_name_nat=row[river_nat_col] if river_nat_col in df.columns else None,
                    punkt_name_nat=row[punkt_nat_col] if punkt_nat_col in df.columns else None,
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