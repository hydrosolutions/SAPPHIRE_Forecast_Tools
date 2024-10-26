import os
import math
import openpyxl

# ieassyreport
from ieasyreports.settings import TagSettings, ReportGeneratorSettings
from ieasyreports.core.tags.tag import Tag
from ieasyreports.core.report_generator import DefaultReportGenerator

# Logging
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Local imports
from .reports import SapphireReport


def round_percentage_to_comma_separated_string(value: float) -> str:
    '''
    Round percentage to 0 decimals for values ge  100, to 1 decimal for values
    ge  10 and to 2 decimals for values ge  0.

    Args:
        value (str): The percentage value to round.

    Returns:
        str: The rounded percentage value. An empty string is returned in case of
            a negative input value.
    '''
    try:
        if not isinstance(value, float):
            raise TypeError('Input value must be a float')

        if math.isclose(value, 100.0):
            string = "100"
        elif abs(value) > 0.0 and abs(value) < 10.0:
            string = "{:.2f}".format(round(value, 2))
        elif abs(value) >= 10.0 and abs(value) < 100.0:
            string = "{:.1f}".format(round(value, 1))
        else:
            string = "{:.0f}".format(round(value, 0))
        # Replace . in string with ,
        string = string.replace('.', ',')
        return string
    except TypeError as e:
        print(f'Error in round_percentage: {e}')
        return None
    except Exception as e:
        print(f'Error in round_percentage: {e}')
        return None

def round_discharge_to_comma_separated_string(value: float) -> str:
    '''
    Round discharge to 0 decimals for values ge 100, to 1 decimal for values
    ge 10 and to 2 decimals for values ge 0.

    Args:
        value (str): The discharge value to round.

    Returns:
        str: The rounded discharge value. An empty string is returned in case of
            a negative input value.

    Examples:
        >>> round_discharge(0.0)
        '0'
        >>> round_discharge(0.123)
        '0.1'
        >>> round_discharge(0.0123)
        '0.01'
        >>> round_discharge(0.00623)
        '0.01'
        >>> round_discharge(1.0)
        '1'
        >>> round_discharge(1.23)
        '1.2'
        >>> round_discharge(1.0123)
        '1.01'
        >>> round_discharge(10.123)
        '10.1'
        >>> round_discharge(100.123)
        '100'
        >>> round_discharge(1000.123)
        '1000'
    '''
    try:
        if not isinstance(value, float):
            raise TypeError('Input value must be a float')

        # Return an empty string if the input value is negative
        if value < 0.0:
            string = " "
        # Test if the input value is close to zero
        elif math.isclose(value, 0.0):
            string = "0"
        elif value > 0.0 and value < 10.0:
            string = "{:.2f}".format(round(value, 2))
        elif value >= 10.0 and value < 100.0:
            string = "{:.1f}".format(round(value, 1))
        else:
            string = "{:.0f}".format(round(value, 0))
        # Replace . in string with ,
        string = string.replace('.', ',')
        return string
    except TypeError as e:
        print(f'Error in round_discharge: {e}')
        return None
    except Exception as e:
        print(f'Error in round_discharge: {e}')
        return None

# Function to write data to Excel
def write_to_excel(sites_list, bulletin_sites, header_df, env_file_path,
                   tag_settings=None):

    # Show the loading spinner
    #indicator.value = True

    print('DEBUG: write_to_excel: Initializing report generator ...')

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

    header_tag = Tag(
            name='BASIN_RU',
            get_value_fn=lambda obj, **kwargs: obj.basin_ru,
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
        get_value_fn=lambda obj, **kwargs: obj.forecast_model,
        tag_settings=tag_settings,
        data=True)

    linreg_predictor_tag = Tag(
        name='LINREG_PREDICTOR',
        get_value_fn=lambda obj, **kwargs: round_discharge_to_comma_separated_string(obj.linreg_predictor),
        tag_settings=tag_settings,
        data=True)

    forecast_tag = Tag(
        name='QEXP',
        get_value_fn=lambda obj, **kwargs: round_discharge_to_comma_separated_string(obj.forecast_pentad),
        tag_settings=tag_settings,
        data=True
    )

    delta_tag = Tag(
        name='DELTA',
        get_value_fn=lambda obj, **kwargs: f"{round(obj.forecast_delta, 2)}".replace('.', ','),
        tag_settings=tag_settings,
        data=True
    )

    sdivsigma_tag = Tag(
        name='SDIVSIGMA',
        get_value_fn=lambda obj, **kwargs: f"{round(obj.forecast_sdivsigma, 2)}".replace('.', ','),
        tag_settings=tag_settings,
        data=True
    )

    forecast_lower_bound_tag = Tag(
        name='FORECAST_LOWER_BOUND',
        get_value_fn=lambda obj, **kwargs: round_discharge_to_comma_separated_string(obj.forecast_lower_bound),
        tag_settings=tag_settings,
        data=True
    )

    forecast_upper_bound_tag = Tag(
        name='FORECAST_UPPER_BOUND',
        get_value_fn=lambda obj, **kwargs: round_discharge_to_comma_separated_string(obj.forecast_upper_bound),
        tag_settings=tag_settings,
        data=True
    )

    dash_tag = Tag(
        name='DASH',
        get_value_fn='—',
        tag_settings=tag_settings,
        data=True
    )

    hydrograph_max_tag = Tag(
        name='HYDROGRAPH_MAX',
        get_value_fn=lambda obj, **kwargs: round_discharge_to_comma_separated_string(obj.hydrograph_max),
        tag_settings=tag_settings,
        data=True
    )

    hydrograph_min_tag = Tag(
        name='HYDROGRAPH_MIN',
        get_value_fn=lambda obj, **kwargs: round_discharge_to_comma_separated_string(obj.hydrograph_min),
        tag_settings=tag_settings,
        data=True
    )

    hydrograph_norm_tag = Tag(
        name='QNORM',
        get_value_fn=lambda obj, **kwargs: round_discharge_to_comma_separated_string(obj.hydrograph_mean),
        tag_settings=tag_settings,
        data=True
    )

    qdanger_tag = Tag(
        name='QDANGER',
        get_value_fn=lambda obj, **kwargs: round_discharge_to_comma_separated_string(obj.qdanger),
        tag_settings=tag_settings,
        data=True
    )

    perc_norm_tag = Tag(
        name='PERC_NORM',
        get_value_fn=lambda obj, **kwargs: round_percentage_to_comma_separated_string(obj.perc_norm),
        tag_settings=tag_settings,
        data=True
    )

    tag_list = [pentad_tag, forecast_tag, header_tag, river_ru_tag, punkt_ru_tag,
                model_tag, forecast_tag, dash_tag, linreg_predictor_tag,
                hydrograph_max_tag, hydrograph_min_tag, hydrograph_norm_tag,
                month_string_nom_ru_tag, month_string_gen_ru_tag, year_tag,
                day_start_pentad_tag, day_end_pentad_tag,
                delta_tag, sdivsigma_tag,
                forecast_lower_bound_tag, forecast_upper_bound_tag,
                qdanger_tag, perc_norm_tag]

    report_settings.report_output_path = os.getenv("ieasyreports_report_output_path")
    report_settings.report_output_path = os.path.join(
        report_settings.report_output_path,
        "bulletins",
        "pentad",
        str(header_df['year'].values[0]))#,
        #start_date_month_num + "_" + start_date_month)

    # Modify the bulletin file name to include the basin (or all basins)
    bulletin_file_name = f"{str(header_df['year'].values[0])}_{header_df['month_number'].values[0]:02}_{header_df['month_str_nom_ru'].values[0]}_пентада_{header_df['pentad'].values[0]}_all_basins_short_term_forecast_bulletin.xlsx"

    # If we are not in the first pentad of the month, we want to use the
    # existing bulletin as template to append the new data to it.
    # Test if we are in the first pentad of the month.
    if int(header_df['day_start_pentad'].values[0]) == 1:
        # We can use the default template for the first pentad of the month
        bulletin_template_file = os.getenv("ieasyforecast_template_pentad_bulletin_file")
        report_settings.templates_directory_path = os.getenv("ieasyreports_templates_directory_path")
    else:
        # Test if the file exists and revert to the default template if it does not exist
        if os.path.exists(os.path.join(report_settings.report_output_path, bulletin_file_name)):
            # Overwrite the settings for the templates directory path.
            pass
            #bulletin_template_file = bulletin_file_name
            #report_settings.templates_directory_path = report_settings.report_output_path
        else:
            bulletin_template_file = os.getenv("ieasyforecast_template_pentad_bulletin_file")
            report_settings.templates_directory_path = os.getenv("ieasyreports_templates_directory_path")

    print("DEBUG: write_to_excel: bulletin_template_file: ", bulletin_template_file)
    print("DEBUG: write_to_excel: report_settings.templates_directory_path: ", report_settings.templates_directory_path)
    # Create the report generator
    report_generator = DefaultReportGenerator(
        tags=tag_list,
        template=bulletin_template_file,
        templates_directory_path=report_settings.templates_directory_path,
        reports_directory_path=report_settings.report_output_path,
        tag_settings=tag_settings
        )

    # Set the sheet number in the report generator
    # TODO: This needs to be fixed. Ask Davor for support
    #workbook = openpyxl.load_workbook(os.path.join(report_settings.templates_directory_path, bulletin_template_file))
    #sheet_names = workbook.sheetnames
    #current_sheet = sheet_names[int(int(header_df['pentad'].values[0]) - 1)]
    #report_generator.sheet = workbook[current_sheet]
    print("DEBUG: write_to_excel: report_generator.sheet: ", report_generator.sheet)

    report_generator.validate()
    #report_generator.generate_report(list_objects=sites_list)
    report_generator.generate_report(
        list_objects=bulletin_sites,
        output_filename=bulletin_file_name
        )
    print('DEBUG: write_to_excel: Report generated.')
    # Note all objects that are passed to generate_report through list_obsjects
    # should be 'data' tags. 'data' tags are listed below a 'header' tag.

    # Hide the loading spinner
    #indicator.value = False

    #report = SapphireReport(name="Test report", env_file_path=env_file_path)
    #print('DEBUG: Multithreading write_to_excel: Writing bulletin ...')
    #report.generate_report(sites_list=sites_list)

