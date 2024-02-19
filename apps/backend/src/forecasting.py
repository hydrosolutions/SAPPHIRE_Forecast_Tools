import logging
import datetime as dt
import forecast_library as fl

logger = logging.getLogger(__name__)


def perform_linear_regression(modified_data, forecast_pentad_of_year):
    # === Perform linear regression ===
    # region Perform linear regression
    logger.info("Perform linear regression ...")

    # Returns a df filtered to the current pentad of the year
    result_df = fl.perform_linear_regression(modified_data, 'Code', 'pentad_in_year', 'discharge_sum', 'discharge_avg',
                                             int(forecast_pentad_of_year))

    logger.info("   ... done")
    # endregion
    return result_df


def get_predictor(modified_data, offline_mode, start_date, fc_sites, ieh_sdk, predictor_dates):
    # === Calculate forecast ===
    logger.info("Getting predictor ...")
    # region Calculate forecast
    # Get the sum of the last 3 days discharge from the database
    # We get the predictor discharge from the DB only if we are not in
    # operational mode or if the date is after 2020-01-01 in offline mode. This
    # saves runtime.
    if not offline_mode or start_date.date() > dt.datetime.strptime('2020-01-01', '%Y-%m-%d').date():
        logger.info("Getting predictor discharge from DB ...")
        # For each site in fc_sites, get the predictor discharge from the DB
        for site in fc_sites:
            fl.Site.from_DB_get_predictor(ieh_sdk, site, predictor_dates, lagdays=20)

        # Special case for virtual stations
        # For reservoir some inflows there is no data in the DB
        # Check if code 16936 is in fc_sites
        # If it is, then we need to get the predictor from the sum of other sites,
        # namely Naryn - Uch-Terek 16059 and lateral tributaries Chychkan 16096,
        # Uzun-Akmat 16100 and Torkent (no code in DB...)
        logger.info('       ... from the DB ...')
        if '16936' in [site.code for site in fc_sites]:
            tok_contrib_sites = ["16059", "16096", "16100"]
            tok_sites = []
            for site in tok_contrib_sites:
                tok_site = fl.Site(site)
                tok_sites.append(tok_site)
            for site in tok_sites:
                fl.Site.from_DB_get_predictor(ieh_sdk, site, predictor_dates)
            # Sum the predictors of the contributing sites
            tok_predictor = sum([site.predictor for site in tok_sites])
            # Assign the sum to the predictor of the reservoir
            for site in fc_sites:
                if site.code == '16936':
                    site.predictor = tok_predictor

        logger.info(f'   {len(fc_sites)} Predictor discharge gotten from DB, namely:'
                    f'\n{[[site.code, site.predictor] for site in fc_sites]}')

    # If we don't find predictors in the database, we can check the data from
    # the Excel sheets in the result_df DataFrame.
    # Iterate through sites in fc_sites and see if we can find the predictor
    # in result_df.
    # This is mostly relevant for the offline mode.
    logger.info('       ... from the df ...')
    for site in fc_sites:
        if site.predictor is None or float(site.predictor) < 0.0:
            logger.info(f'    No predictor found in DB for site {site.code}. Getting predictor from df ...')
            # print(site.code)
            # Get the data for the site
            # Here we need to use modified_data as result_df is filtered to the
            # current pentad of the year and does not contain predictor data.
            site_data1 = modified_data[modified_data['Code'] == site.code]
            # Get the predictor from the data for today
            fl.Site.from_df_get_predictor(site, site_data1, [start_date])
            # print(fl.Site.from_df_get_predictor(site, site_data, predictor_dates))

    logger.info(f'   {len(fc_sites)} Predictor discharge gotten from df, namely:\n'
                f'{[[site.code, site.predictor] for site in fc_sites]}')
    # print result_df from December 24, 2009 to January 6, 2010
    # print(result_df[(result_df['Date'] >= dt.datetime.strptime('2009-12-24', '%Y-%m-%d').date()) &
    # (result_df['Date'] <= dt.datetime.strptime('2010-01-06', '%Y-%m-%d').date())])
    logger.info("   ... done")


def perform_forecast(fc_sites, forecast_pentad_of_year, result_df):
    # Perform forecast
    logger.info("Performing forecast ...")

    # For each site, calculate the forecasted discharge
    for site in fc_sites:
        fl.Site.from_df_calculate_forecast(site, forecast_pentad_of_year, result_df)

    logger.info(f'   {len(fc_sites)} Forecasts calculated, namely:\n'
                f'{[[site.code, site.fc_qexp] for site in fc_sites]}')

    logger.info("   ... done")
    # endregion


def calculate_forecast_boundaries(result_df, fc_sites, forecast_pentad_of_year):
    # === Get boundaries of forecast ===
    # region Calculate forecast boundaries
    logger.info("Calculating forecast boundaries ...")
    result2_df1 = fl.calculate_forecast_skill(result_df, 'Code', 'pentad_in_year', 'discharge_avg',
                                              'forecasted_discharge')

    # Select columns in dataframe
    # print(result2_df.head(60)[['Date','Code','issue_date','discharge_sum','discharge_avg','pentad_in_year']])
    # print(result2_df.tail(60)[['Date','Code','issue_date','discharge_sum','discharge_avg','pentad_in_year']])
    # print(result2_df.columns)

    for site in fc_sites:
        fl.Site.from_df_get_qrange_discharge(site, forecast_pentad_of_year, result2_df1)
        fl.Site.calculate_percentages_norm(site)

    logger.info("  ... done")
    return result2_df1
