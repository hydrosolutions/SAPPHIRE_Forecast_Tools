import logging
import pandas as pd
import os
from . import config

import forecast_library as fl
import tag_library as tl

from typing import Any, List, Optional

from ieasyreports.core.tags.tag import Tag
from ieasyreports.utils import import_from_string
from ieasyreports.core.report_generator import DefaultReportGenerator
from ieasyreports.exceptions import TemplateNotValidatedException
logger = logging.getLogger(__name__)


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


def write_hydrograph_data(modified_data):
    # === Write hydrograph data ===
    logger.info("Writing hydrograph data ...")
    # Reformat modified_data. Keep columns Q_m3s, Year, Code, discharge_avg, pentad.
    # Write the day of the year into a new column.
    hydrograph_data = modified_data
    # Convert the Date column to a datetime object
    hydrograph_data['Date'] = pd.to_datetime(hydrograph_data['Date'])

    # We do not filter February 29 in leap years here but in the dashboard.

    # Overwrite pentad in a month with pentad in a year
    hydrograph_data = tl.add_pentad_in_year_column(hydrograph_data)
    # print(hydrograph_data.head())
    hydrograph_data['day_of_year'] = hydrograph_data['Date'].dt.dayofyear
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

    # Set 'Code' and 'pentad'/'day_of_year' as index again
    hydrograph_pentad.set_index(['Code', 'pentad'], inplace=True)
    hydrograph_day.set_index(['Code', 'day_of_year'], inplace=True)

    # Write this data to a dump (pickle the data) for subsequent visualization
    # in the forecast dashboard.
    hydrograph_pentad_file_csv = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_pentad_file"))

    # Write the hydrograph_pentad to csv
    hydrograph_pentad.to_csv(hydrograph_pentad_file_csv)

    hydrograph_day_file_csv = os.path.join(
        os.getenv("ieasyforecast_intermediate_data_path"),
        os.getenv("ieasyforecast_hydrograph_day_file"))

    # Write the hydrograph_day to csv
    hydrograph_day.to_csv(hydrograph_day_file_csv)

    logger.info("   ... done")


def create_tag(name, get_value_fn, description):
    return Tag(name=name, get_value_fn=get_value_fn, description=description)


def bulletin_tags(bulletin_date):
    # === Define tags ===
    logger.info("Defining bulletin tags ...")

    bulletin_tag_details = [
        ("PENTAD", tl.get_pentad(bulletin_date), "Pentad of the month"),
        ("PERC_NORM", lambda obj: obj.perc_norm, "Percentage of norm discharge in current pentad"),
        ("QDANGER", lambda obj: obj.qdanger, "Threshold for dangerous discharge"),
        ("QMAX", lambda obj: obj.fc_qmax, "Maximum forecasted discharge range"),
        ("QMIN", lambda obj: obj.fc_qmin, "Minimum forecasted discharge range"),
        ("DAY_END", tl.get_pentad_last_day(bulletin_date), "End day of the pentadal forecast"),
        ("DAY_START", tl.get_pentad_first_day(bulletin_date), "Start day of the pentadal forecast"),
        ("QNORM", lambda obj: obj.qnorm, "Norm discharge in current pentad"),
        ("BASIN", lambda obj: obj.basin, "Basin of the gauge sites"),
        ("RIVER_NAME", lambda obj: obj.river_name, "Name of the river"),
        ("PUNKT_NAME", lambda obj: obj.punkt_name, "Name of the gauge site"),
        ("MONTH_STR_CASE1", tl.get_month_str_case1(bulletin_date), "Name of the month in a string in the first case"),
        ("MONTH_STR_CASE2", tl.get_month_str_case2(bulletin_date), "Name of the month in a string in the second case"),
        ("YEAR", tl.get_year(bulletin_date), "Name of the month in a string in the second case"),
        ("DAY_START", tl.get_pentad_first_day(bulletin_date), "Start day of the pentadal forecast"),
        ("DAY_END", tl.get_pentad_last_day(bulletin_date), "End day of the pentadal forecast"),
        ("PENTAD", tl.get_pentad(bulletin_date), "Pentad of the month"),
        ("DASH", "-", "Dash"),
    ]

    logger.info("   ... done")
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


def write_forecast_bulletin(settings, start_date, bulletin_date, fc_sites):
    # === Write forecast bulletin ===
    # region Write forecast bulletin
    logger.info("Writing forecast outputs ...")

    # Option to turn off bulletin writing, may be used during development only.
    write_bulletin = True

    # Format the date as a string in the format "YYYY_MM_DD"
    today_str = start_date.strftime("%Y-%m-%d")

    if write_bulletin:
        # Get the name of the template file from the environment variables
        bulletin_template_file = os.getenv("ieasyforecast_template_pentad_bulletin_file")

        # Construct the output filename using the formatted date
        bulletin_output_file = os.getenv("ieasyforecast_bulletin_file_name")
        filename = f"{today_str}_{bulletin_output_file}"

        report_generator = import_from_string(settings.template_generator_class)(
            tags=bulletin_tags(bulletin_date),
            template=bulletin_template_file,
            requires_header=False,
            custom_settings=settings
        )

        report_generator.validate()
        report_generator.generate_report(list_objects=fc_sites, output_filename=filename)
        logger.info("   ... done")


def write_forecast_sheets(settings, start_date, bulletin_date, fc_sites, result2_df):
    # Format the date as a string in the format "YYYY_MM_DD"
    today_str = start_date.strftime("%Y-%m-%d")
    # If forecast sheets are written
    if config.excel_output():
        logger.info("Writing forecast sheets ...")

        # Get the name of the template file from the environment variables
        forecast_template_file = os.getenv("ieasyforecast_template_pentad_sheet_file")

        # Get the name of the output file from the environment variables
        bulletin_output_file = os.getenv("ieasyforecast_bulletin_file_name")

        for site in fc_sites:

            # Construct the output filename using the formatted date
            filename = f"{today_str}_{site.code}_{bulletin_output_file}"

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
                    'qpavg': fl.round_discharge(df_year['discharge_avg'].mean()),
                    'qpsum': fl.round_discharge(df_year['discharge_sum'].mean())
                })

            # Add current year and current predictor to site_data
            site_data.append({
                'river_name': site.river_name + " " + site.punkt_name,
                'year': str(start_date.year),
                'qpavg': "",
                'qpsum': site.predictor
            })

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
        os.getenv("ieasyforecast_results_file"))

    if not os.path.exists(offline_forecast_results_file):
        with open(offline_forecast_results_file, "w") as f:
            f.write("date,code,predictor,slope,intercept,delta,fc_qmin,fc_qmax,fc_qexp,qnorm,perc_norm,qdanger\n")
            f.flush()

    # Write the data to a csv file
    with open(offline_forecast_results_file, "a") as f:
        # Write the data
        for site in fc_sites:
            f.write(
                f"{today_str},{site.code},{site.predictor},{site.slope},{site.intercept},{site.delta},{site.fc_qmin}"
                f",{site.fc_qmax},{site.fc_qexp},{site.qnorm},{site.perc_norm},{site.qdanger}\n"
            )
            f.flush()

    # endregion
    logger.info("   ... done")

    # === Store last successful run date ===
    config.store_last_successful_run_date(start_date)
