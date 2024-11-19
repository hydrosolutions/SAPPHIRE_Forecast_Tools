import os
import math
import openpyxl
import panel as pn
from typing import List

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


# Custom class
# Overwrite the default report generator to add functinality to write to
# specific sheets
class MultiSheetReportGenerator(DefaultReportGenerator):
    def __init__(
        self,
        tags: List[Tag],
        template: str,
        templates_directory_path: str,
        reports_directory_path: str,
        tag_settings: TagSettings,
        requires_header: bool = False,
        sheet: int = 0
    ):
        self.tags = {tag.name: tag for tag in tags}
        self.template_filename = template
        self.templates_directory_path = templates_directory_path
        self.reports_directory_path = reports_directory_path
        self.template = self.open_template_file()
        self.tag_settings = tag_settings
        self.sheet = self.template.worksheets[sheet]

        self.validated = False

        self.requires_header_tag = requires_header
        self.header_tag_info = {}
        self.data_tags_info = []
        self.general_tags = {}


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

def copy_worksheet(report_settings, temp_bulletin_file_name, bulletin_file_name, header_df):
        # Now copy the sheet 1 of the generated report to the appropriate sheet in the final bulletin
    # Load the generated report
    try:
        generated_report = openpyxl.load_workbook(os.path.join(report_settings.report_output_path, temp_bulletin_file_name))
    except Exception as e:
        raise Exception(f"Error loading the generated report: {e}")

    # If the file bulletin_file_name exists, do the following:
    if os.path.exists(os.path.join(report_settings.report_output_path, bulletin_file_name)):
        # Load the final bulletin
        try:
            final_bulletin = openpyxl.load_workbook(os.path.join(report_settings.report_output_path, bulletin_file_name))
        except Exception as e:
            raise Exception(f"Error loading the final bulletin: {e}")

        print(f"DEBUG: write_to_excel: initial final_bulletin.sheetnames: {final_bulletin.sheetnames}")

        # Test if we have a sheet already for the current pentad & remove if it exists
        if f"{int(header_df['pentad'].values[0])} пентада" in final_bulletin.sheetnames:
            print(f"DEBUG: write_to_excel: Removing sheet for pentad {int(header_df['pentad'].values[0])}")
            # Remove the sheet for the current pentad
            final_bulletin.remove(final_bulletin[f"{int(header_df['pentad'].values[0])} пентада"])

        # Get the sheet 1 of the generated report
        generated_sheet = generated_report.active

        # Rename the sheet to the pentad number
        generated_sheet.title = f"{int(header_df['pentad'].values[0])} пентада"

        # Set parent workbook of the generated report to the final bulletin to allow copying
        generated_sheet._parent = final_bulletin

        # Add final_sheet to final_bulletin
        final_bulletin._add_sheet(generated_sheet)

        # Save the final bulletin
        final_bulletin.save(os.path.join(report_settings.report_output_path, bulletin_file_name))

        # Close the workbooks
        generated_report.close()
        final_bulletin.close()

        # Delete the generated report
        os.remove(os.path.join(report_settings.report_output_path, temp_bulletin_file_name))

    else:
        # If the file does not exist, rename the temp_bulletin_file_name to bulletin_file_name
        os.rename(os.path.join(report_settings.report_output_path, temp_bulletin_file_name),
                  os.path.join(report_settings.report_output_path, bulletin_file_name))
        # Test if the sheet name is correct (it should be the pentad number)
        # Load the final bulletin
        final_bulletin = openpyxl.load_workbook(os.path.join(report_settings.report_output_path, bulletin_file_name))
        # Rename the sheet to the pentad number
        final_bulletin.active.title = f"{int(header_df['pentad'].values[0])} пентада"
        # Save the final bulletin
        final_bulletin.save(os.path.join(report_settings.report_output_path, bulletin_file_name))
        # Close the workbook
        final_bulletin.close()

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
    # region tags
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
    # endregion tags

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

    # From bulletin_sites get site lists for each unique basin
    # Create a list of unique basins
    basins = [site.basin_ru for site in bulletin_sites]
    unique_basins = list(set(basins))

    # Create a list of sites for each unique basin
    sites_by_basin = {basin: [site for site in bulletin_sites if site.basin_ru == basin] for basin in unique_basins}

    # Add bulletin_sitest to sites_by_basin under basin 'all_basins'
    sites_by_basin['all_basins'] = bulletin_sites

    # Print the keys in object sites_by_basin
    print(f"DEBUG: write_to_excel: sites_by_basin keys: {sites_by_basin.keys()}")

    # Iterate over the unique basins and generate a report for each basin
    for basin in sites_by_basin.keys():
        print(f"DEBUG: write_to_excel: Generating report for basin {basin} ...")
        # Get the sites for the current basin
        sites = sites_by_basin[basin]

        # Define the bulletin file name
        bulletin_file_name = f"{str(header_df['year'].values[0])}_{header_df['month_number'].values[0]:02}_{header_df['month_str_nom_ru'].values[0]}_{basin}_short_term_forecast_bulletin.xlsx"
        temp_bulletin_file_name = f"_temp_{bulletin_file_name}"

        # Generate the report
        report_generator = DefaultReportGenerator(
            tags=tag_list,
            template=os.getenv("ieasyforecast_template_pentad_bulletin_file"),
            templates_directory_path=os.getenv("ieasyreports_templates_directory_path"),
            reports_directory_path=report_settings.report_output_path,
            tag_settings=tag_settings,
            requires_header=True
        )

        report_generator.validate()

        report_generator.generate_report(
            list_objects=sites,
            output_filename=temp_bulletin_file_name
        )

        copy_worksheet(report_settings, temp_bulletin_file_name, bulletin_file_name, header_df)


    # Done with the report generation

    # Note all objects that are passed to generate_report through list_obsjects
    # should be 'data' tags. 'data' tags are listed below a 'header' tag.



