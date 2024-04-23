import logging
import pandas as pd
import os
import datetime as dt
from . import config
from itertools import groupby

import forecast_library as fl
import tag_library as tl

from typing import Any, List, Optional

from ieasyreports.core.tags.tag import Tag
from ieasyreports.utils import import_from_string
from ieasyreports.core.report_generator import DefaultReportGenerator
from ieasyreports.exceptions import TemplateNotValidatedException, InvalidSettingsException
from ieasyreports.settings import Settings
logger = logging.getLogger(__name__)

class WriteForecastToMultipleSheets(DefaultReportGenerator):
    def __init__(self, tags: List[Tag], template: str, requires_header: bool = False, custom_settings: Settings = None, sheet: int = 0):
        if custom_settings and not isinstance(custom_settings, Settings):
            raise InvalidSettingsException(
                f"`custom_settings` must be a {type(Settings)} instance, got {type(custom_settings)} instead."
            )
        self.settings = custom_settings or Settings()
        self.tags = {tag.name: tag for tag in tags}
        self.template_filename = template
        self.template = self.open_template_file()
        self.sheet = self.template.worksheets[sheet]

        self.validated = False

        self.requires_header_tag = requires_header
        self.header_tag_info = {}
        self.data_tags_info = []
        self.general_tags = {}

class FakeHeaderTemplateGenerator(DefaultReportGenerator):
    # This class is a subclass of the DefaultReportGenerator class and provides a
    # method for generating a report based on a template. The
    # FakeHeaderTemplateGenerator class overrides the generate_report() method of
    # the DefaultReportGenerator class to provide custom functionality for
    # generating reports.
    def generate_report(
        self, list_objects: Optional[List[Any]] = None,
        output_path: Optional[str] = None, output_filename: Optional[str] = None
    ):
        # Generates a report based on a template.
        # Requires a statement settings = Settings() before calling the method.
        # Args:
        #     list_objects (list): A list of objects to be used to generate the
        #         report. The objects in the list should be of the same type as
        #         the objects used to validate the template.
        #     output_path (str): The path to the directory where the report will
        #         be saved. If no output_path is provided, the report will be
        #         saved in the current working directory.
        #     output_filename (str): The name of the report file. If no
        #         output_filename is provided, the report will be saved with the
        #         same name as the template file.

        if not self.validated:
            raise TemplateNotValidatedException(
                "Template must be validated first. Did you forget to call the `.validate()` method?"
            )
        for tag, cells in self.general_tags.items():
            for cell in cells:
                cell.value = tag.replace(cell.value)

        if self.header_tag_info:
            grouped_data = {}
            list_objects = list_objects if list_objects else []
            for list_obj in list_objects:
                header_value = self.header_tag_info["tag"].replace(
                    self.header_tag_info["cell"].value,
                    special="HEADER",
                    obj=list_obj
                )

                if header_value not in grouped_data:
                    grouped_data[header_value] = []
                grouped_data[header_value].append(list_obj)

            original_header_row = self.header_tag_info["cell"].row
            header_style = self.header_tag_info["cell"].font.copy()
            data_styles = [data_tag["cell"].font.copy() for data_tag in self.data_tags_info]

            for header_value, item_group in sorted(grouped_data.items()):
                # write the header value
                cell = self.sheet.cell(row=original_header_row, column=self.header_tag_info["cell"].column,
                                       value=header_value)
                cell.font = header_style

                self.sheet.delete_rows(original_header_row + 3)
                for item in item_group:
                    for idx, data_tag in enumerate(self.data_tags_info):
                        tag = data_tag["tag"]
                        data = tag.replace(data_tag["cell"].value, obj=item, special=self.settings.data_tag)
                        cell = self.sheet.cell(row=original_header_row + 3, column=data_tag["cell"].column,
                                               value=data)
                        cell.font = data_styles[idx]

                    original_header_row += 1

        self.save_report(output_filename, output_path)

def validate_hydrograph_data(hydrograph_data):
    """
    Validates the columns of the hydrograph data.

    Parameters:
    hydrograph_data (DataFrame): The hydrograph data in the format required for
        the display in the forecast dashboard.

    Returns:
    DataFrame: The reformatted hydrograph data in the format required for the
        display in the forecast dashboard.

    Raises:
    TypeError: If the hydrograph_data is not a DataFrame.
    ValueError: If the hydrograph_data is missing the required columns.
    """

    # Check if the hydrograph_data is a DataFrame
    if not isinstance(hydrograph_data, pd.DataFrame):
        raise TypeError("The hydrograph_data must be a DataFrame.")

    # Check if the hydrograph_data has the required columns
    required_columns = ['Date', 'Year', 'Code', 'Q_m3s', 'discharge_avg', 'pentad']
    for column in required_columns:
        if column not in hydrograph_data.columns:
            raise ValueError(f"The hydrograph_data is missing the '{column}' column.")

    # Convert the Date column to a datetime object
    hydrograph_data['Date'] = pd.to_datetime(hydrograph_data['Date'])
    # Make sure the Year column is of type string.
    hydrograph_data['Year'] = hydrograph_data['Year'].astype(str)

    # We do not filter February 29 in leap years here but in the dashboard.

    # Round to 3 digits as is usual in operational hydrology in Kyrgyzstan
    hydrograph_data['Q_m3s'] = hydrograph_data['Q_m3s'].apply(fl.round_discharge_to_float)
    hydrograph_data['discharge_avg'] = hydrograph_data['discharge_avg'].apply(fl.round_discharge_to_float)

    # Overwrite pentad in a month with pentad in a year
    hydrograph_data = tl.add_pentad_in_year_column(hydrograph_data)
    # print(hydrograph_data.head())
    hydrograph_data['day_of_year'] = hydrograph_data['Date'].dt.dayofyear

    return hydrograph_data

def validate_hydrograph_data_decad(hydrograph_data):
    """
    Validates the columns of the hydrograph data.

    Parameters:
    hydrograph_data (DataFrame): The hydrograph data in the format required for
        the display in the forecast dashboard.

    Returns:
    DataFrame: The reformatted hydrograph data in the format required for the
        display in the forecast dashboard.

    Raises:
    TypeError: If the hydrograph_data is not a DataFrame.
    ValueError: If the hydrograph_data is missing the required columns.
    """

    # Check if the hydrograph_data is a DataFrame
    if not isinstance(hydrograph_data, pd.DataFrame):
        raise TypeError("The hydrograph_data must be a DataFrame.")

    # Check if the hydrograph_data has the required columns
    required_columns = ['Date', 'Year', 'Code', 'Q_m3s', 'discharge_avg', 'decad_in_year']
    for column in required_columns:
        if column not in hydrograph_data.columns:
            raise ValueError(f"The hydrograph_data is missing the '{column}' column.")

    # Convert the Date column to a datetime object
    hydrograph_data['Date'] = pd.to_datetime(hydrograph_data['Date'])
    # Make sure the Year column is of type string.
    hydrograph_data['Year'] = hydrograph_data['Year'].astype(str)

    # We do not filter February 29 in leap years here but in the dashboard.

    # Round to 3 digits as is usual in operational hydrology in Kyrgyzstan
    hydrograph_data['Q_m3s'] = hydrograph_data['Q_m3s'].apply(fl.round_discharge_to_float)
    hydrograph_data['discharge_avg'] = hydrograph_data['discharge_avg'].apply(fl.round_discharge_to_float)

    # print(hydrograph_data.head())
    hydrograph_data['day_of_year'] = hydrograph_data['Date'].dt.dayofyear

    return hydrograph_data

def save_hydrograph_data_to_csv(hydrograph_pentad, hydrograph_day):
    # Write this data to csv for subsequent visualization
    # in the forecast dashboard.
    hydrograph_pentad_file_csv = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_pentad_file"))

    # Write the hydrograph_pentad to csv
    ret = hydrograph_pentad.to_csv(hydrograph_pentad_file_csv)
    if ret is None:
        logger.info("Hydrograph pentad data saved to csv file")
    else:
        logger.error("Hydrograph pentad data not saved to csv file")

    hydrograph_day_file_csv = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_day_file"))

    # Write the hydrograph_day to csv. Do not print the index.
    ret = hydrograph_day.to_csv(hydrograph_day_file_csv)
    if ret is None:
        logger.info("Hydrograph day data saved to csv file")
    else:
        logger.error("Hydrograph day data not saved to csv file")

def save_hydrograph_data_to_csv_decad(hydrograph_decad):
    # Write this data to csv for subsequent visualization
    # in the forecast dashboard.
    hydrograph_decad_file_csv = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_decad_file"))

    # Write the hydrograph_pentad to csv
    ret = hydrograph_decad.to_csv(hydrograph_decad_file_csv)
    if ret is None:
        logger.info("Hydrograph decad data saved to csv file")
    else:
        logger.error("Hydrograph decad data not saved to csv file")

def reformat_hydrograph_data(hydrograph_data):
    """
    Reformats the hydrograph data for daily and pentadal output.

    This function selects the necessary columns from the input DataFrame and
    reformats the data in a wide format where the 'Code' and 'day_of_year'/'pentad'
    are the index, the columns are the years, and the values are the 'Q_m3s'/'discharge_avg'.

    Parameters:
    hydrograph_data (DataFrame): The input hydrograph data. This DataFrame should
        contain columns for 'Code', 'Year', 'day_of_year', 'Q_m3s', 'pentad',
        and 'discharge_avg'.

    Returns:
    tuple: A tuple containing two DataFrames. The first DataFrame contains the
        reformatted data for pentadal output, and the second DataFrame contains
        the reformatted data for daily output. Both DataFrames have 'Code' and
        'day_of_year'/'pentad' as the index, 'Year' as the columns, and 'Q_m3s'/'discharge_avg'
        as the values.
    """
    # Select the columns that are needed for the hydrograph data for daily and
    # pentadal output
    hydrograph_data_day = hydrograph_data[['Code', 'Year', 'day_of_year', 'Q_m3s']]
    hydrograph_data_pentad = hydrograph_data[['Code', 'Year', 'pentad', 'discharge_avg']]

    # Reset the index of the hydrograph_data_pentad DataFrame
    hydrograph_data_pentad = hydrograph_data_pentad.reset_index(drop=True)
    hydrograph_data_day = hydrograph_data_day.reset_index(drop=True)

    # Reformat the data in the wide format. The day of the year, Code and pentad
    # are the index. The columns are the years. The values are the discharge_avg.
    hydrograph_pentad = hydrograph_data_pentad.pivot_table(index=['Code', 'pentad'], columns='Year',
                                                           values='discharge_avg')
    hydrograph_day = hydrograph_data_day.pivot_table(index=['Code', 'day_of_year'], columns='Year', values='Q_m3s')

    # Reset the index of the hydrograph_pentad DataFrame
    hydrograph_pentad = hydrograph_pentad.reset_index()
    hydrograph_day = hydrograph_day.reset_index()

    # Convert pentad column to integer
    hydrograph_pentad['pentad'] = hydrograph_pentad['pentad'].astype(int)
    hydrograph_day['day_of_year'] = hydrograph_day['day_of_year'].astype(int)

    # We want to have the data sorted by 'Code' and 'pentad'/'day_of_year'
    hydrograph_pentad.sort_values(by=['Code', 'pentad'], inplace=True)
    hydrograph_day.sort_values(by=['Code', 'day_of_year'], inplace=True)

    # Set 'Code' and 'pentad'/'day_of_year' as index again
    hydrograph_pentad.set_index(['Code', 'pentad'], inplace=True)
    hydrograph_day.set_index(['Code', 'day_of_year'], inplace=True)

    # Sort index
    hydrograph_pentad.sort_index(inplace=True)
    hydrograph_day.sort_index(inplace=True)

    return hydrograph_pentad, hydrograph_day

def reformat_hydrograph_data_decad(hydrograph_data):
    """
    Reformats the hydrograph data for decadal output.

    This function selects the necessary columns from the input DataFrame and
    reformats the data in a wide format where the 'Code' and 'decad_in_year'
    are the index, the columns are the years, and the values is 'discharge_avg'.

    Parameters:
    hydrograph_data (DataFrame): The input hydrograph data. This DataFrame should
        contain columns for 'Code', 'Year', 'day_of_year', 'Q_m3s', 'decad_in_year',
        and 'discharge_avg'.

    Returns:
    tuple: A tuple containing two DataFrames. The first DataFrame contains the
        reformatted data for pentadal output, and the second DataFrame contains
        the reformatted data for daily output. Both DataFrames have 'Code' and
        'decad_in_year' as the index, 'Year' as the columns, and 'discharge_avg'
        as the values.
    """
    # Select the columns that are needed for the hydrograph data for decadal output
    hydrograph_data_decad = hydrograph_data[['Code', 'Year', 'decad_in_year',
                                             'discharge_avg']]

    # Reset the index of the hydrograph_data_decad DataFrame
    hydrograph_data_decad = hydrograph_data_decad.reset_index(drop=True)

    # Reformat the data in the wide format. The day of the year, Code and decad
    # are the index. The columns are the years. The values are the discharge_avg.
    hydrograph_decad = hydrograph_data_decad.pivot_table(
        index=['Code', 'decad_in_year'],
        columns='Year',
        values='discharge_avg')

    # Reset the index of the hydrograph_decad DataFrame
    hydrograph_decad = hydrograph_decad.reset_index()

    # Convert decad column to integer
    hydrograph_decad['decad_in_year'] = hydrograph_decad['decad_in_year'].astype(int)

    # We want to have the data sorted by 'Code' and 'decad_in_year'
    hydrograph_decad.sort_values(by=['Code', 'decad_in_year'], inplace=True)

    # Set 'Code' and 'decad_in_year' as index again
    hydrograph_decad.set_index(['Code', 'decad_in_year'], inplace=True)

    # Sort index
    hydrograph_decad.sort_index(inplace=True)

    return hydrograph_decad

def write_hydrograph_data(modified_data):
    # === Write hydrograph data ===
    logger.info("Writing hydrograph data for pentadal forecasts ...")

    # Validate the columns of the hydrograph data.
    # Write the day of the year into a new column.
    hydrograph_data = validate_hydrograph_data(modified_data)

    # Pivot the hydrograph data to the wide format with daily and pentadal data.
    hydrograph_pentad, hydrograph_day = reformat_hydrograph_data(hydrograph_data)

    # Save the hydrograph data to csv for subsequent visualization in the
    # forecast dashboard.
    save_hydrograph_data_to_csv(hydrograph_pentad, hydrograph_day)

    logger.info("   ... done")

def write_hydrograph_data_decad(modified_data):
    # === Write hydrograph data ===
    logger.info("Writing hydrograph data for decadal forecasts ...")

    # Validate the columns of the hydrograph data.
    # Write the day of the year into a new column.
    hydrograph_data = validate_hydrograph_data_decad(modified_data)

    # Pivot the hydrograph data to the wide format with daily and pentadal data.
    hydrograph_decad = reformat_hydrograph_data_decad(hydrograph_data)

    # Save the hydrograph data to csv for subsequent visualization in the
    # forecast dashboard.
    save_hydrograph_data_to_csv_decad(hydrograph_decad)

def create_tag(name, get_value_fn, description):
    return Tag(name=name, get_value_fn=get_value_fn, description=description)

def bulletin_tags_pentadal_forecast(bulletin_date):
    # === Define tags ===
    bulletin_tag_details = [
        ("PENTAD", tl.get_pentad(bulletin_date), "Pentad of the month"),
        ("PERC_NORM", lambda obj: obj.perc_norm, "Percentage of norm discharge in current pentad"),
        ("QDANGER", lambda obj: obj.qdanger, "Threshold for dangerous discharge"),
        ("QMAX", lambda obj: obj.fc_qmax, "Maximum forecasted discharge range"),
        ("QMIN", lambda obj: obj.fc_qmin, "Minimum forecasted discharge range"),
        ("QEXP", lambda obj: obj.fc_qexp, "Expected discharge in current pentad"),
        ("PREDICTOR", lambda obj: obj.predictor, "Predictor for the forecast"),
        ("DELTA", lambda obj: obj.delta, "Difference between the expected and norm discharge"),
        ("SDIVSIGMA", lambda obj: obj.sdivsigma, "s/sigma"),
        ("DAY_END", tl.get_pentad_last_day(bulletin_date), "End day of the pentadal forecast"),
        ("DAY_START", tl.get_pentad_first_day(bulletin_date), "Start day of the pentadal forecast"),
        ("QNORM", lambda obj: obj.qnorm, "Norm discharge in current pentad"),
        ("HYDROGRAPHMIN", lambda obj: obj.qmin, "Minimum value of the hydrograph"),
        ("HYDROGRAPHMAX", lambda obj: obj.qmax, "Maximum value of the hydrograph"),
        ("BASIN", lambda obj: tl.get_basin_name_short_term_forecast(obj), "Basin of the gauge sites"),
        ("RIVER_NAME", lambda obj: obj.river_name, "Name of the river"),
        ("PUNKT_NAME", lambda obj: obj.punkt_name, "Name of the gauge site"),
        ("MONTH_STR_CASE1", tl.get_month_str_case1(bulletin_date), "Name of the month in a string in the first case"),
        ("MONTH_STR_CASE2", tl.get_month_str_case2(bulletin_date), "Name of the month in a string in the second case"),
        ("YEAR", tl.get_year(bulletin_date), "Name of the month in a string in the second case"),
        ("DAY_START", tl.get_pentad_first_day(bulletin_date), "Start day of the pentadal forecast"),
        ("DAY_END", tl.get_pentad_last_day(bulletin_date), "End day of the pentadal forecast"),
        ("DECAD", tl.get_decad_in_month(bulletin_date), "Decad of the month"),
        ("DASH", "-", "Dash"),
    ]

    return [create_tag(name, fn, desc) for name, fn, desc in bulletin_tag_details]

def sheet_tags(bulletin_date):
    # Tags
    sheet_tag_details = [
        ("FSHEETS_RIVER_NAME", lambda obj: obj.get("river_name"), "Name of the river"),
        ("MONTH_STR_CASE1", tl.get_month_str_case1(bulletin_date), "Name of the month in a string in the first case"),
        ("PENTAD", tl.get_pentad(bulletin_date), "Pentad of the month"),
        ("YEARFSHEETS", lambda obj: obj.get("year"), "Year for which the forecast is produced"),
        ("QPAVG", lambda obj: obj.get("qpavg"), "Average discharge for the current pentad and year"),
        ("QPSUM", lambda obj: obj.get("qpsum"), "3-day discharge sum for the current pentad and year"),
    ]
    return [create_tag(name, fn, desc) for name, fn, desc in sheet_tag_details]

def sheet_tags_decad(bulletin_date):
    # Tags
    sheet_tag_details = [
        ("FSHEETS_RIVER_NAME", lambda obj: obj.get("river_name"), "Name of the river"),
        ("MONTH_STR_CASE1", tl.get_month_str_case1(bulletin_date), "Name of the month in a string in the first case"),
        ("MONTH_LATIN", tl.get_month_str_latin(bulletin_date), "Name of the month in latin numbers"),
        ("PREDICTOR_MONTH_LATIN", tl.get_predcitor_month_latin(bulletin_date), "Name of the month in latin numbers for the predictor"),
        ("DEKAD", tl.get_decad_in_month(bulletin_date), "Decad of the month"),
        ("PREDICTOR_DEKAD", tl.get_predictor_decad(bulletin_date), "Decad of the month for the predictor"),
        ("YEARFSHEETS", lambda obj: obj.get("year"), "Year for which the forecast is produced"),
        ("QPAVG", lambda obj: obj.get("qpavg"), "Average discharge for the current decad and year"),
        ("PREDICTOR", lambda obj: obj.get("predictor"), "Average discharge for the previous decad and year"),
    ]
    return [create_tag(name, fn, desc) for name, fn, desc in sheet_tag_details]

def split_sites_by_basin(sites):
    # Sort the sites by basin name
    sorted_sites = sorted(sites, key=lambda site: site.basin)

    # Group the sites by basin name and sort them by code in descending order
    sites_by_basin = {k: sorted(list(g), key=lambda site: site.code, reverse=True)
                      for k, g in groupby(sorted_sites, key=lambda site: site.basin)}

    return sites_by_basin

def special_sort(fc_sites):
    """
    This function defines special sorting for the sites in the forecast bulletin.

    Args:
    fc_sites (list): A list of Site objects. Should contain sites for one single
    basin only.

    Returns:
    list: A list of Site objects sorted according to the special sorting rules.
    """
    chu_site_order = ['15102', '15149', '15171', '15189', '15194', '15214', '15212','15215', '15216']
    chu_site_order.reverse()
    naryn_site_order = ['16059', '16100', '16096', '16936']
    naryn_site_order.reverse()

    # The sites in fc_sites should be in order of chu_site_order and naryn_site_order
    sorted_sites = []
    for code in chu_site_order:
        for fc_site in fc_sites:
            if fc_site.code == code:
                sorted_sites.append(fc_site)
    for site in naryn_site_order:
        for fc_site in fc_sites:
            if fc_site.code == site:
                sorted_sites.append(fc_site)

    return sorted_sites

def write_forecast_bulletin(settings, start_date, bulletin_date, fc_sites):
    # === Write forecast bulletin ===
    # region Write forecast bulletin
    logger.info("Writing forecast bulletins for pentadal forecasts ...")

    # Format the date as a string in the format "YYYY_MM_DD"
    today_str = start_date.strftime("%Y-%m-%d")
    start_date_year = str(dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().year)
    start_date_month_num = dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().strftime("%m")
    start_date_month = assign_month_string_to_number(dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().month)
    start_date_pentad = tl.get_pentad(bulletin_date)

    # We want a separate bulletin for each basin in fc_sites.
    sites_by_basins = split_sites_by_basin(fc_sites)

    # Overwrite settings for the bulletin folder. In this way we can sort the
    # bulletins in a separate folder.
    settings.report_output_path = os.getenv("ieasyreports_report_output_path")
    settings.report_output_path = os.path.join(
        settings.report_output_path,
        "bulletins",
        "pentad",
        start_date_year)#,
        #start_date_month_num + "_" + start_date_month)

    for basin, sites in sites_by_basins.items():
        # If we are not in the first pentad of the month, we want to use the
        # existing bulletin as template to append the new data to it.
        # Construct the output filename using the formatted date
        bulletin_output_file = os.getenv("ieasyforecast_bulletin_file_name")
        filename = f"{start_date_year}_{start_date_month_num}_{start_date_month}_{basin}_{bulletin_output_file}"

        # We want to write several bulletin sheets into one excel file.
        # Test if we are in the first pentad of the month.
        if int(start_date_pentad) == 1:
            # We can use the default template for the first pentad of the month
            bulletin_template_file = os.getenv("ieasyforecast_template_pentad_bulletin_file")
            settings.templates_directory_path = os.getenv("ieasyreports_templates_directory_path")
        else:
            # Test if the file exists and revert to the default template if it does not exist
            if os.path.exists(os.path.join(settings.report_output_path, filename)):
                # Overwrite the settings for the templates directory path.
                bulletin_template_file = filename
                settings.templates_directory_path = settings.report_output_path
            else:
                bulletin_template_file = os.getenv("ieasyforecast_template_pentad_bulletin_file")
                settings.templates_directory_path = os.getenv("ieasyreports_templates_directory_path")

        # Make sure that all strings in fc_sites are using comma as the decimal
        # separator for writing the report.
        fc_sites_report = sites

        # Make sure the sites are sorted in the sequence as requested by KGHM
        fc_sites_report = special_sort(fc_sites_report)

        for site in fc_sites_report:
            #print("DEBUG: site:", site)
            site.fc_qmin = site.fc_qmin.replace('.', ',')
            site.fc_qmax = site.fc_qmax.replace('.', ',')
            site.fc_qexp = site.fc_qexp.replace('.', ',')
            site.predictor = fl.round_discharge(site.predictor).replace('.', ',')
            site.qnorm = site.qnorm.replace('.', ',')
            site.perc_norm = site.perc_norm.replace('.', ',')
            site.qdanger = site.qdanger.replace('.', ',')
            site.delta = fl.round_discharge(site.delta).replace('.', ',')
            site.qmin = site.qmin.replace('.', ',')
            site.qmax = site.qmax.replace('.', ',')
            site.sdivsigma = site.sdivsigma.replace('.', ',')

        #report_generator = import_from_string(settings.template_generator_class)(
        report_generator = WriteForecastToMultipleSheets(
            tags=bulletin_tags_pentadal_forecast(bulletin_date),
            template=bulletin_template_file,
            requires_header=False,
            custom_settings=settings,
            sheet = (int(start_date_pentad) - 1)
        )

        report_generator.validate()
        report_generator.generate_report(list_objects=fc_sites_report, output_filename=filename)

        # Reset the template directory path in settings
        settings.templates_directory_path = os.getenv("ieasyreports_templates_directory_path")

    logger.info("   ... done")

def write_forecast_bulletin_decad(settings, start_date, bulletin_date, fc_sites):
    # === Write forecast bulletin ===
    # region Write forecast bulletin
    logger.info("Writing forecast bulletins for decadal forecasts ...")

    # Format the date as a string in the format "YYYY_MM_DD"
    today_str = start_date.strftime("%Y-%m-%d")
    start_date_year = str(dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().year)
    start_date_month_num = dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().strftime("%m")
    start_date_month = assign_month_string_to_number(dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().month)
    start_date_pentad = tl.get_decad_in_month(bulletin_date)

    # We want a separate bulletin for each basin in fc_sites.
    sites_by_basins = split_sites_by_basin(fc_sites)

    # Overwrite settings for the bulletin folder. In this way we can sort the
    # bulletins in a separate folder.
    settings.report_output_path = os.getenv("ieasyreports_report_output_path")
    settings.report_output_path = os.path.join(
        settings.report_output_path,
        "bulletins",
        "decad",
        start_date_year)#,
        #start_date_month_num + "_" + start_date_month)

    for basin, sites in sites_by_basins.items():
        # If we are not in the first decad of the month, we want to use the
        # existing bulletin as template to append the new data to it.
        # Construct the output filename using the formatted date
        bulletin_output_file = os.getenv("ieasyforecast_bulletin_file_name")
        filename = f"{start_date_year}_{start_date_month_num}_{start_date_month}_{basin}_{bulletin_output_file}"

        print("DEBUG: settings.templates_directory_path prior", settings.templates_directory_path)
        print("       bulletin output file:", filename)
        # We want to write several bulletin sheets into one excel file.
        # Test if we are in the first decad of the month.
        if int(start_date_pentad) == 1:
            print("       first decad of the month")
            # We can use the default template for the first decad of the month
            bulletin_template_file = os.getenv("ieasyforecast_template_decad_bulletin_file")
            settings.templates_directory_path = os.getenv("ieasyreports_templates_directory_path")
        else:
            print("       not the first decad of the month")
            # Test if the file exists and revert to the default template if it does not exist
            if os.path.exists(os.path.join(settings.report_output_path, filename)):
                print("       bulletin output file already exists")
                # Overwrite the settings for the templates directory path.
                bulletin_template_file = filename
                settings.templates_directory_path = settings.report_output_path
            else:
                print("       bulletin output file does not exist")
                bulletin_template_file = os.getenv("ieasyforecast_template_pentad_bulletin_file")
                settings.templates_directory_path = os.getenv("ieasyreports_templates_directory_path")
        print("DEBUG: settings.templates_directory_path", settings.templates_directory_path)
        print("       bulletin template file:", bulletin_template_file)
        print("       bulletin output file:", filename)

        # Make sure that all strings in fc_sites are using comma as the decimal
        # separator for writing the report.
        fc_sites_report = sites

        # Make sure the sites are sorted in the sequence as requested by KGHM
        fc_sites_report = special_sort(fc_sites_report)

        for site in fc_sites_report:
            #print("DEBUG: site:", site)
            site.fc_qmin = site.fc_qmin.replace('.', ',')
            site.fc_qmax = site.fc_qmax.replace('.', ',')
            site.fc_qexp = site.fc_qexp.replace('.', ',')
            # If there is no predictor, we want to write an empty string.
            # Otherwise, we use fl.round_discharge to round the value and
            # replace the decimal point.
            site.predictor = "" if fl.round_discharge(site.predictor) is None else fl.round_discharge(site.predictor).replace('.', ',')
            #site.predictor = fl.round_discharge(site.predictor).replace('.', ',')
            site.qnorm = site.qnorm.replace('.', ',')
            site.perc_norm = site.perc_norm.replace('.', ',')
            site.qdanger = site.qdanger.replace('.', ',')
            site.delta = "" if fl.round_discharge(site.delta) is None else fl.round_discharge(site.delta).replace('.', ',')
            site.qmin = site.qmin.replace('.', ',')
            site.qmax = site.qmax.replace('.', ',')
            site.sdivsigma = site.sdivsigma.replace('.', ',')

        #report_generator = import_from_string(settings.template_generator_class)(
        report_generator = WriteForecastToMultipleSheets(
            tags=bulletin_tags_pentadal_forecast(bulletin_date),
            template=bulletin_template_file,
            requires_header=False,
            custom_settings=settings,
            sheet = (int(start_date_pentad) - 1)
        )

        report_generator.validate()
        report_generator.generate_report(list_objects=fc_sites_report, output_filename=filename)

        # Reset the template directory path in settings
        settings.templates_directory_path = os.getenv("ieasyreports_templates_directory_path")

    logger.info("   ... done")

def assign_month_string_to_number(month_number):
    """
    Converts a month number to its corresponding month name in Russian.

    This function takes an integer from 1 to 12 that represents a month number
    (where 1 is January and 12 is December) and returns the corresponding month
    name in Russian.

    Parameters:
    month_number (int): An integer from 1 to 12 representing the month number.

    Returns:
    str: The name of the corresponding month in Russian. If the month_number
    is not in the range 1-12, the function will return None.

    Example:
    >>> assign_month_string_to_number(1)
    'Январь'
    >>> assign_month_string_to_number(12)
    'Декабрь'
    """
    if month_number == 1:
        return "Январь"
    elif month_number == 2:
        return "Февраль"
    elif month_number == 3:
        return "Март"
    elif month_number == 4:
        return "Апрель"
    elif month_number == 5:
        return "Май"
    elif month_number == 6:
        return "Июнь"
    elif month_number == 7:
        return "Июль"
    elif month_number == 8:
        return "Август"
    elif month_number == 9:
        return "Сентябрь"
    elif month_number == 10:
        return "Октябрь"
    elif month_number == 11:
        return "Ноябрь"
    elif month_number == 12:
        return "Декабрь"
    elif month_number < 1 or month_number > 12:
        return None

def write_forecast_sheets(settings, start_date, bulletin_date, fc_sites, result2_df):
    # Format the date as a string in the format "YYYY_MM_DD"
    today_str = start_date.strftime("%Y-%m-%d")
    start_date_year = str(dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().year)
    start_date_month_num = dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().strftime("%m")
    start_date_month = assign_month_string_to_number(dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().month)
    start_date_pentad = tl.get_pentad(bulletin_date)

    # If forecast sheets are written
    if config.excel_output():
        logger.info("Writing forecast sheets for pentadal forecasts ...")

        # Get the name of the template file from the environment variables
        forecast_template_file = os.getenv("ieasyforecast_template_pentad_sheet_file")

        # Get the name of the output file from the environment variables
        bulletin_output_file = os.getenv("ieasyforecast_sheet_file_name")

        for site in fc_sites:

            # Construct the output filename using the formatted date
            filename = f"{start_date_year}_{start_date_month_num}_{start_date_month}_{start_date_pentad}_{site.code}_{bulletin_output_file}"

            # This tag is defined here because it's a general tag, and it can't
            # receive a lambda function as a replacement value, it needs to get a
            # concrete value, so we create a new tag for each site

            # We need to use a trick here because we can use the ieasyreports
            # library only for printing one line per site. However, here we want
            # it to print several lines per site. Therefore, we create a dummy
            # Site object for each year in the data and print it.
            # Filter result2_df for the current site
            temp_df = result2_df[result2_df['Code'] == site.code].reset_index(drop=True)
            # Select columns from temp_df
            temp_df = temp_df[['Year', 'discharge_avg', 'discharge_sum', 'forecasted_discharge']]
            # the data frame is already filtered to the current pentad of the year
            temp_df = temp_df.dropna(subset=['forecasted_discharge'])

            site_data = []
            # iterate through all the years for the current site
            for year in temp_df['Year'].unique():
                df_year = temp_df[temp_df['Year'] == year]

                site_data.append({
                    'river_name': site.river_name + " " + site.punkt_name,
                    'year': str(year),
                    'qpavg': fl.round_discharge_trad_bulletin_3numbers(df_year['discharge_avg'].mean()).replace('.', ','),
                    'qpsum': fl.round_discharge_trad_bulletin_3numbers(df_year['discharge_sum'].mean()).replace('.', ',')
                })

            # Add current year and current predictor to site_data
            # Test if site.predictor is nan. If it is, assign ""
            if pd.isna(site.predictor):
                temp_predictor = ""
            else:
                temp_predictor = site.predictor
            site_data.append({
                'river_name': site.river_name + " " + site.punkt_name,
                'year': str(start_date.year),
                'qpavg': "",
                'qpsum': temp_predictor
            })

            # Overwrite settings for theh bulletin folder. In this way we can sort the
            # bulletins in a separate folder.
            settings.report_output_path = os.getenv("ieasyreports_report_output_path")
            settings.report_output_path = os.path.join(
                settings.report_output_path,
                "forecast_sheets",
                "pentad",
                start_date_year,
                start_date_month_num + "_" + start_date_month,
                site.code)


            # directly instantiate the new generator
            report_generator = FakeHeaderTemplateGenerator(
                tags=sheet_tags(bulletin_date),
                template=forecast_template_file,
                requires_header=True,
                custom_settings=settings
            )
            report_generator.validate()
            report_generator.generate_report(
                list_objects=site_data,
                output_filename=filename
            )

        logger.info("   ... done")

    # Write other output
    logger.info("Writing other output ...")

    # Write the forecasted discharge to a csv file. Print code, predictor,
    # fc_qmin, fc_qmax, fc_qexp, qnorm, perc_norm, qdanger for each site in
    # fc_sites
    # Write a file header if the file does not yet exist
    offline_forecast_results_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_pentad_results_file"))

    if not os.path.exists(offline_forecast_results_file):
        with open(offline_forecast_results_file, "w") as f:
            f.write("date,code,predictor,slope,intercept,delta,fc_qmin,fc_qmax,fc_qexp,qnorm,perc_norm,qdanger,sdivsigma,accuracy\n")
            f.flush()

    # Write the data to a csv file
    with open(offline_forecast_results_file, "a") as f:

        # Make sure that all strings in fc_sites are using point as the decimal
        fc_sites_report = fc_sites
        for site in fc_sites_report:
            site.fc_qmin = site.fc_qmin.replace(',', '.')
            site.fc_qmax = site.fc_qmax.replace(',', '.')
            site.fc_qexp = site.fc_qexp.replace(',', '.')
            site.qnorm = site.qnorm.replace(',', '.')
            site.predictor = site.predictor.replace(',', '.')
            site.perc_norm = site.perc_norm.replace(',', '.')
            site.qdanger = site.qdanger.replace(',', '.')
            site.delta = site.delta.replace(',', '.')
            site.sdivsigma = site.sdivsigma.replace(',', '.')
            site.accuracy = site.accuracy.replace(',', '.')
        # Write the data
        for site in fc_sites_report:
            f.write(
                f"{today_str},{site.code},{site.predictor},{site.slope},{site.intercept},{site.delta},{site.fc_qmin}"
                f",{site.fc_qmax},{site.fc_qexp},{site.qnorm},{site.perc_norm},{site.qdanger},{site.sdivsigma},{site.accuracy}\n"
            )
            f.flush()

    # endregion
    logger.info("   ... done")

def write_forecast_sheets_decad(settings, start_date, bulletin_date, fc_sites, result2_df):
    # Format the date as a string in the format "YYYY_MM_DD"
    today_str = start_date.strftime("%Y-%m-%d")
    start_date_year = str(dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().year)
    start_date_month_num = dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().strftime("%m")
    start_date_month = assign_month_string_to_number(dt.datetime.strptime(bulletin_date, '%Y-%m-%d').date().month)
    start_date_decad = tl.get_pentad(bulletin_date)
    start_date_decad = tl.get_decad_in_month(bulletin_date)

    # If forecast sheets are written
    if config.excel_output():
        logger.info("Writing forecast sheets for decadal forecasts ...")

        # Get the name of the template file from the environment variables
        forecast_template_file = os.getenv("ieasyforecast_template_decad_sheet_file")
        print("\n\n\nDEBUG: forecast_template_file:", forecast_template_file)

        # Get the name of the output file from the environment variables
        bulletin_output_file = os.getenv("ieasyforecast_sheet_file_name")

        for site in fc_sites:

            # Construct the output filename using the formatted date
            filename = f"{start_date_year}_{start_date_month_num}_{start_date_month}_{start_date_decad}_{site.code}_{bulletin_output_file}"

            # This tag is defined here because it's a general tag, and it can't
            # receive a lambda function as a replacement value, it needs to get a
            # concrete value, so we create a new tag for each site

            # We need to use a trick here because we can use the ieasyreports
            # library only for printing one line per site. However, here we want
            # it to print several lines per site. Therefore, we create a dummy
            # Site object for each year in the data and print it.
            # Filter result2_df for the current site
            temp_df = result2_df[result2_df['Code'] == site.code].reset_index(drop=True)
            # Select columns from temp_df
            temp_df = temp_df[['Year', 'discharge_avg', 'predictor', 'forecasted_discharge']]
            # the data frame is already filtered to the current pentad of the year
            temp_df = temp_df.dropna(subset=['forecasted_discharge'])

            site_data = []
            # iterate through all the years for the current site
            for year in temp_df['Year'].unique():
                df_year = temp_df[temp_df['Year'] == year]

                site_data.append({
                    'river_name': site.river_name + " " + site.punkt_name,
                    'year': str(year),
                    'qpavg': fl.round_discharge_trad_bulletin_3numbers(df_year['discharge_avg'].mean()).replace('.', ','),
                    'predictor': fl.round_discharge_trad_bulletin_3numbers(df_year['predictor'].mean()).replace('.', ',')
                })

            # Add current year and current predictor to site_data
            # Test if site.predictor is nan. If it is, assign ""
            if pd.isna(site.predictor):
                temp_predictor = ""
            else:
                temp_predictor = site.predictor
            site_data.append({
                'river_name': site.river_name + " " + site.punkt_name,
                'year': str(start_date.year),
                'qpavg': "",
                'predictor': temp_predictor
            })

            # Overwrite settings for theh bulletin folder. In this way we can sort the
            # bulletins in a separate folder.
            settings.report_output_path = os.getenv("ieasyreports_report_output_path")
            settings.report_output_path = os.path.join(
                settings.report_output_path,
                "forecast_sheets",
                "decad",
                start_date_year,
                start_date_month_num + "_" + start_date_month,
                site.code)


            # directly instantiate the new generator
            report_generator = FakeHeaderTemplateGenerator(
                tags=sheet_tags_decad(bulletin_date),
                template=forecast_template_file,
                requires_header=True,
                custom_settings=settings
            )
            report_generator.validate()
            report_generator.generate_report(
                list_objects=site_data,
                output_filename=filename
            )

        logger.info("   ... done")

    # Write other output
    logger.info("Writing other output ...")

    # Write the forecasted discharge to a csv file. Print code, predictor,
    # fc_qmin, fc_qmax, fc_qexp, qnorm, perc_norm, qdanger for each site in
    # fc_sites
    # Write a file header if the file does not yet exist
    offline_forecast_results_file = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_decad_results_file"))

    if not os.path.exists(offline_forecast_results_file):
        with open(offline_forecast_results_file, "w") as f:
            f.write("date,code,predictor,slope,intercept,delta,fc_qmin,fc_qmax,fc_qexp,qnorm,perc_norm,qdanger\n")
            f.flush()

    # Write the data to a csv file
    with open(offline_forecast_results_file, "a") as f:

        # Make sure that all strings in fc_sites are using point as the decimal
        fc_sites_report = fc_sites
        for site in fc_sites_report:
            site.fc_qmin = site.fc_qmin.replace(',', '.')
            site.fc_qmax = site.fc_qmax.replace(',', '.')
            site.fc_qexp = site.fc_qexp.replace(',', '.')
            site.qnorm = site.qnorm.replace(',', '.')
            site.delta = site.delta.replace(',', '.')
            site.predictor = site.predictor.replace(',', '.')
            site.perc_norm = site.perc_norm.replace(',', '.')
            site.qdanger = site.qdanger.replace(',', '.')
            site.sdivsigma = site.sdivsigma.replace(',', '.')
            site.accuracy = site.accuracy.replace(',', '.')
        # Write the data
        for site in fc_sites_report:
            f.write(
                f"{today_str},{site.code},{site.predictor},{site.slope},{site.intercept},{site.delta},{site.fc_qmin}"
                f",{site.fc_qmax},{site.fc_qexp},{site.qnorm},{site.perc_norm},{site.qdanger}\n"
            )
            f.flush()

    # endregion
    logger.info("   ... done")

def write_pentadal_forecast_data(data: pd.DataFrame):
    """
    Writes the data to a csv file for later reating into the forecast dashboard.

    Args:
    data (pd.DataFrame): The data to be written to a csv file.

    Returns:
    None
    """

    # Get the path to the intermediate data folder from the environmental
    # variables and the name of the ieasyforecast_analysis_pentad_file.
    # Concatenate them to the output file path.
    try:
       output_file_path = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_analysis_pentad_file"))
    except Exception as e:
        logger.error("Could not get the output file path.")
        print(os.getenv("ieasyforecast_intermediate_data_path"))
        print(os.getenv("ieasyforecast_analysis_pentad_file"))
        raise e

    # From data dataframe, drop all rows where column issue_date is False.
    # This is done to remove all rows that are not forecasts.
    data = data[data['issue_date'] == True]

    # Write the data to a csv file. Raise an error if this does not work.
    # If the data is written to the csv file, log a message that the data
    # has been written.
    try:
        ret = data.reset_index(drop=True).to_csv(output_file_path, index=False)
        if ret is None:
            logger.info(f"Data written to {output_file_path}.")
        else:
            logger.error(f"Could not write the data to {output_file_path}.")
    except Exception as e:
        logger.error(f"Could not write the data to {output_file_path}.")
        raise e

    return None
