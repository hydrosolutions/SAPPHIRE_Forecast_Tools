# Description: This file contains the code for generating reports.
import os
import time


# ieasyreports
from ieasyreports.core.report_generator import DefaultReportGenerator
from ieasyreports.settings import TagSettings, ReportGeneratorSettings
from ieasyreports.core.tags.tag import Tag

# Logging
import logging
logger = logging.getLogger(__name__)

# Tags
from .tags import SapphireTagManager

class SapphireReport():
    """
    A class to represent a report with its tags.
    """

    def __init__(self, name: str, env_file_path: str, tag_settings = None):
        """
        Constructs a report with the given name, report type, and tags.

        Args:
            name (str): The name of the report.
            env_file_name (str): The path to the environment file.
        """
        self.name = name
        self.report_settings = self.define_settings(env_file_path)
        #self.tag_settings = tag_settings if tag_settings else TagSettings()
        #self.tag_manager = SapphireTagManager(self.tag_settings)
        #self.tags = self.tag_manager.get_tags()
        #print("DEBUG: Report.__init__: tags: ", self.tags)


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

    def generate_report(self, sites_list):
        """Generate the report."""
        print(f"DEBUG: Report.generate_report: Generating report for {self.name} ...")
        #print(f"DEBUG: Report.generate_report: tags: {self.tags}")
        #print(f"DEBUG: Report.generate_report: river_ru_tag: {self.tags[2].get_value_fn}")
        try:
            # Configure the report generator
            report_generator = DefaultReportGenerator(
                tags=self.tags,
                template='test_template.xlsx',
                reports_directory_path=self.report_settings.report_output_path,
                templates_directory_path=self.report_settings.templates_directory_path,
                tag_settings=self.tag_settings)

            # Validate and write the report
            report_generator.validate()
            report_generator.generate_report(list_objects=sites_list)

            return 0

        except Exception as e:
            print(f"Error generating report: {e}")
            raise e


