# ! Currently not in use !

# Description: This file contains the TagManager class which initialises and updates tags for a report.
from ieasyreports.core.tags.tag import Tag
from ieasyreports.settings import TagSettings

# Logging
import logging
logger = logging.getLogger(__name__)

# local imports
from .site import SapphireSite

# Define settings
tag_settings = TagSettings()

class SapphireTagManager:

    def __init__(self, tag_settings = None):
        self.tag_settings = tag_settings if tag_settings else TagSettings()

    def get_river_name_ru(self, site):
        return site.river_name_ru

    def get_pentad_dummy_fun(self):
        return '0'

    def get_tags(self):

        pentad_tag = Tag(
            name='PENTAD',
            get_value_fn=lambda obj, **kwargs: obj.pentad,
            tag_settings=self.tag_settings)

        forecast_tag = Tag(
            name='FORECAST',
            get_value_fn='0.3',
            tag_settings=self.tag_settings)

        river_ru_tag = Tag(
            name='RIVER_NAME_RU',
            get_value_fn='test river',  # lambda obj, **kwargs: obj.name_ru,
            tag_settings=self.tag_settings,
            data=False
            )

        # Debugging output for get_value_fn
        for tag in [pentad_tag, forecast_tag, river_ru_tag]:
            if callable(tag.get_value_fn):
                print(f"DEBUG: TagManager.get_tags: {tag.name} get_value_fn is callable")
            else:
                print(f"DEBUG: TagManager.get_tags: {tag.name} get_value_fn: {tag.get_value_fn}")

        return [pentad_tag, forecast_tag, river_ru_tag]