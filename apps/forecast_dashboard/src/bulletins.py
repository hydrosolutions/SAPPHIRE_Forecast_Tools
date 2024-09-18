import time

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
def write_to_excel(sites_list, header_df, env_file_path, indicator,
                   tag_settings=None):

    # Show the loading spinner
    indicator.value = True

    print('DEBUG: Multithreading write_to_excel: Initializing report generator ...')

    # Define tag & report settings
    tag_settings = TagSettings() if tag_settings is None else tag_settings
    report = SapphireReport(name="Test report", env_file_path=env_file_path)
    report_settings = report.define_settings(env_file_path)

    # Define Tags
    pentad_tag = Tag(
        name='PENTAD',
        get_value_fn=header_df['pentad'].values[0],
        tag_settings=tag_settings)

    month_string_nom_ru_tag = Tag(
        name='MONTH_STR_NOM_RU',
        get_value_fn=header_df['month_str_nom_ru'].values[0],
        tag_settings=tag_settings)

    month_string_gen_ru_tag = Tag(
        name='MONTH_STR_GEN_RU',
        get_value_fn=header_df['month_str_gen_ru'].values[0],
        tag_settings=tag_settings)

    year_tag = Tag(
        name='YEAR',
        get_value_fn=header_df['year'].values[0],
        tag_settings=tag_settings)

    day_start_pentad_tag = Tag(
        name='DAY_START_PENTAD',
        get_value_fn=header_df['day_start_pentad'].values[0],
        tag_settings=tag_settings)

    day_end_pentad_tag = Tag(
        name='DAY_END_PENTAD',
        get_value_fn=header_df['day_end_pentad'].values[0],
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

    punkt_ru_tag = Tag(
        name='PUNKT_NAME_RU',
        get_value_fn=lambda obj, **kwargs: obj.punkt_name_ru,
        tag_settings=tag_settings,
        data=True)

    model_tag = Tag(
        name='MODEL',
        get_value_fn=lambda obj, **kwargs: obj.model,
        tag_settings=tag_settings,
        data=True)

    linreg_predictor_tag = Tag(
        name='LINREG_PREDICTOR',
        get_value_fn=lambda obj, **kwargs: obj.linreg_predictor,
        tag_settings=tag_settings,
        data=True)

    report_generator = DefaultReportGenerator(
                tags=[pentad_tag, forecast_tag, header_tag, river_ru_tag,
                      month_string_nom_ru_tag, month_string_gen_ru_tag, year_tag,
                      day_start_pentad_tag, day_end_pentad_tag],
                template='test_template.xlsx',
                reports_directory_path=report_settings.report_output_path,
                templates_directory_path=report_settings.templates_directory_path,
                tag_settings=tag_settings)

    report_generator.validate()
    report_generator.generate_report(list_objects=sites_list)
    # Note all objects that are passed to generate_report through list_obsjects
    # should be 'data' tags. 'data' tags are listed below a 'header' tag.

    # Hide the loading spinner
    indicator.value = False

    #report = SapphireReport(name="Test report", env_file_path=env_file_path)
    #print('DEBUG: Multithreading write_to_excel: Writing bulletin ...')
    #report.generate_report(sites_list=sites_list)

