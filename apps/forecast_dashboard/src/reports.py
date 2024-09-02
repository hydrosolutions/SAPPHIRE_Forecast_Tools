# Description: This file contains the code for generating reports.
import os
import time


# ieasyreports
from ieasyreports.core.report_generator import DefaultReportGenerator
from ieasyreports.settings import TagSettings, ReportGeneratorSettings

# Logging
import logging
logger = logging.getLogger(__name__)

# Tags
from .tags import SapphireTagManager


class SapphireReport():
    """
    A class to represent a report with its tags.
    """

    def __init__(self, name: str, env_file_path: str, station_df, tag_settings = None):
        """
        Constructs a report with the given name, report type, and tags.

        Args:
            name (str): The name of the report.
            env_file_name (str): The path to the environment file.
        """
        self.name = name
        self.report_settings = self.define_settings(env_file_path)
        self.tag_settings = tag_settings if tag_settings else TagSettings()
        self.tag_manager = SapphireTagManager(station_df, self.tag_settings)
        self.tags = self.tag_manager.get_tags()


    def define_settings(self, env_file_path: str):
        """Define settings based on the report type."""
        # Test if env_file_path is valid (not NoneType and existing)
        if env_file_path is None:
            raise ValueError("Environment file path is required.")
        if not os.path.exists(env_file_path):
            raise FileNotFoundError(f"File not found: {env_file_path}")

        # Define settings
        self.report_settings = ReportGeneratorSettings(
            env_file=env_file_path,
            templates_directory_path = os.getenv("ieasyreports_templates_directory_path")
        )
        return self.report_settings

    def generate_report(self):
        """Generate the report."""
        try:
            logger.debug(f"Generating report for {self.name} ...")
            logger.debug(f"tags: {self.tags}")
            logger.debug(f"river_ru_tag: {self.tags[2].get_value_fn}")
            report_generator = DefaultReportGenerator(
                tags=self.tags,
                template='test_template.xlsx',
                reports_directory_path=self.report_settings.report_output_path,
                templates_directory_path=self.report_settings.templates_directory_path,
                tag_settings=self.tag_settings)
            report_generator.validate()
            report_generator.generate_report()
            return 0
        except Exception as e:
            print(f"Error generating report: {e}")
            raise e

    @classmethod
    def get_allowed_report_types(cls):
        """Return a list of allowed report types."""
        return list(cls.REPORT_TYPE_TAGS.keys())

