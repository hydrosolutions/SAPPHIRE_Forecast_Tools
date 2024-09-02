# Description: This file contains the TagManager class which initialises and updates tags for a report.
from ieasyreports.core.tags.tag import Tag
from ieasyreports.settings import TagSettings

# Logging
import logging
logger = logging.getLogger(__name__)


# Define settings
tag_settings = TagSettings()

class SapphireTagManager:
    """Initialises and updates tags for a report."""
    def __init__(self, station_df, tag_settings = None):
        self.station_df = station_df
        self.tag_settings = tag_settings if tag_settings else TagSettings()
        self.initialize_tags()

    def initialize_tags(self):
        self.pentad_tag = Tag("PENTAD", "9", self.tag_settings, "Pentad of the month")
        self.forecast_tag = Tag("FORECAST", "Forecast Data", self.tag_settings, "Forecast related tag")
        self.river_ru_tag = Tag(
            name="RIVER_NAME_RU",
            get_value_fn=self.get_river_name_ru,
            tag_settings=self.tag_settings,
            description="River name in Russian",
            value_fn_args={"station_df": self.station_df},
            header=False,
            data=True)

    def get_river_name_ru(site):
        return site.river_name_ru

    def get_tags(self):
        return [self.pentad_tag, self.forecast_tag, self.river_ru_tag]