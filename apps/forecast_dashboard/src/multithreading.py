# ! Currently not in use !
# ieasyreports does not seem compatible with multi-threading.


# Multi-threading
from concurrent.futures import ThreadPoolExecutor

# ieassyreport
from ieasyreports.settings import TagSettings, ReportGeneratorSettings
from ieasyreports.core.tags.tag import Tag
from ieasyreports.core.report_generator import DefaultReportGenerator


# Logging
import traceback
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Local imports
from .reports import SapphireReport


# Function to write data to Excel
def write_to_excel(sites_list, env_file_path,
                   tag_settings=None):
    print('DEBUG: Multithreading write_to_excel: Initializing report generator ...')

    # Define tag & report settings
    tag_settings = TagSettings() if tag_settings is None else tag_settings
    report = SapphireReport(name="Test report", env_file_path=env_file_path)
    report_settings = report.define_settings(env_file_path)

    # Define Tags
    pentad_tag = Tag(
            name='PENTAD',
            get_value_fn='0',  # sth like get_pentad_from_date or some such
            tag_settings=tag_settings)

    forecast_tag = Tag(
            name='FORECAST',
            get_value_fn='0.3',
            tag_settings=tag_settings)

    header_tag = Tag(
            name='BASIN',
            get_value_fn=lambda obj, **kwargs: obj.basin,
            tag_settings=tag_settings,
            header=True)

    river_ru_tag = Tag(
            name='RIVER_NAME_RU',
            get_value_fn=lambda obj, **kwargs: obj.river_name_ru,
            tag_settings=tag_settings,
            data=True)

    report_generator = DefaultReportGenerator(
                tags=[pentad_tag, forecast_tag, header_tag, river_ru_tag],
                template='test_template.xlsx',
                reports_directory_path=report_settings.report_output_path,
                templates_directory_path=report_settings.templates_directory_path,
                tag_settings=tag_settings)

    report_generator.validate()
    report_generator.generate_report(list_objects=sites_list)
    # Note all objects that are passed to generate_report through list_obsjects
    # should be 'data' tags. 'data' tags are listed below a 'header' tag.

    #report = SapphireReport(name="Test report", env_file_path=env_file_path)
    #print('DEBUG: Multithreading write_to_excel: Writing bulletin ...')
    #report.generate_report(sites_list=sites_list)


# def write_forecast_bulletin_in_background(site_list, env_file_path, status):
#     status.object = 'Status: Writing...'
#     print('DEBUG: Multithreading write_forecast_b...: Writing forecast bulletin in background ...')
#     with ThreadPoolExecutor() as executor:
#         future = executor.submit(write_to_excel, site_list, env_file_path)
#         future.add_done_callback(lambda f: status_update(status, f))

# # Function to update status after background task is done
# def status_update(status, future):
#     try:
#         result = future.result()
#         status.object = 'Status: Done'
#     except Exception as e:
#         # Print the error and the traceback
#         logger.error(f'Error: {e}')
#         logger.error(traceback.format_exc())
#         status.object = f'Status: Failed with error {e}'
