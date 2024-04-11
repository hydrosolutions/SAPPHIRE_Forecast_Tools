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

def get_predictor_from_datetimes(modified_data, start_date, fc_sites, ieh_sdk, backend_has_access_to_db, predictor_dates):

    """
    Get predictors (aggregated river runoff) from the iEasyHydro database or
    from the Excel data. The predictors are written to the site objects in
    fc_sites.

    Args:
        modified_data: DataFrame, the data from the Excel files and from the
            iEasyHydro database (if available).
        start_date: datetime, the date on which the forecast is made.
        fc_sites: list, the list of sites for which the forecast is made.
        ieh_sdk: iEasyHydroSDK, the iEasyHydro SDK object.
        backend_has_access_to_db: bool, True if the backend has access to the
            iEasyHydro database, False otherwise.
        predictor_dates: list, the datetimes for which the predictors are
            aggregated.

    Returns:
        fc_sites: list, the list of sites for which the forecast is made with
            the predictor attribute filled in according to data availability.
    """

    logger.info("Getting predictor with get_predictor_from_datetimes ...")
    # region Calculate forecast
    # Get the sum of the last 3 days discharge from the database
    # We get the predictor discharge from the DB only if we are not in
    # operational mode or if the date is after 2020-01-01 in offline mode. This
    # saves runtime.
    if not backend_has_access_to_db:
        logger.info("No access to DB. Looking for predictor from in df (excel data) ...")
    if backend_has_access_to_db and start_date.date() > dt.datetime.strptime('2020-01-01', '%Y-%m-%d').date():
        logger.info("Looking for predictor in iEasyHydro DB ...")
        # For each site in fc_sites, get the predictor discharge from the DB
        for site in fc_sites:
            fl.Site.from_DB_get_predictor_for_pentadal_forecast(
                sdk=ieh_sdk, site=site, dates=predictor_dates, lagdays=20)

        # Special case for virtual stations
        # For reservoir some inflows there is no data in the DB
        # Check if code 16936 is in fc_sites
        # If it is, then we need to get the predictor from the sum of other sites,
        # namely Naryn - Uch-Terek 16059 and lateral tributaries Chychkan 16096,
        # Uzun-Akmat 16100 and Torkent (no code in DB...)
        if '16936' in [site.code for site in fc_sites]:
            # Test if station 16093 (Torkent) is in the list of stations
            # (This station is under construction at the time of writing this code)
            if '16093' in [site.code for site in fc_sites]:
                tok_contrib_sites = ["16059", "16096", "16100", "16093"]
                torkent_data = True
            else:
                tok_contrib_sites = ["16059", "16096", "16100"]
                torkent_data = False
            tok_sites = []
            for site in tok_contrib_sites:
                tok_site = fl.Site(site)
                tok_sites.append(tok_site)
            for site in tok_sites:
                fl.Site.from_DB_get_predictor_for_pentadal_forecast(ieh_sdk, site, predictor_dates, lagdays=20)

            # Test if there is data in site.predictor for site 16093. If there
            # is no data, assign 0.6*site.predictor for site 16096 to site 16093.
            if torkent_data:
                if tok_sites[3].predictor is None:
                    tok_sites[3].predictor = 0.6 * tok_sites[1].predictor
                    # Sum the predictors of the contributing sites
                    tok_predictor = sum([site.predictor for site in tok_sites])
            else:
                 tok_predictor = sum([site.predictor for site in tok_sites]) + \
                 0.6 * tok_sites[1].predictor



            # Assign the sum to the predictor of the reservoir
            for site in fc_sites:
                if site.code == '16936':
                    site.predictor = tok_predictor

        logger.info(f'   {len(fc_sites)} predictor discharge gotten from DB, namely:'
                    f'\n{[[site.code, site.predictor] for site in fc_sites]}')

    # If we don't find predictors in the database, we can check the data from
    # the Excel sheets in the result_df DataFrame.
    # Iterate through sites in fc_sites and see if we can find the predictor
    # in result_df.
    # This is mostly relevant for the offline mode.
    for site in fc_sites:
        if site.predictor is None or float(site.predictor) < 0.0 or start_date.date() < dt.datetime.strptime('2023-01-01', '%Y-%m-%d').date():
            logger.info(f'    No predictor found in DB for site {site.code}. Getting predictor from df ...')
            # print(site.code)
            # Get the data for the site
            # Here we need to use modified_data as result_df is filtered to the
            # current pentad of the year and does not contain predictor data.
            site_data1 = modified_data[modified_data['Code'] == site.code]
            #print("DEBUG: forecasting:get_predictor: site_data1: \n",
            #      site_data1[['Date', 'Code', 'issue_date', 'discharge_sum']].tail(10))
            #print("DEBUG: forecasting:get_predictor: [start_date]: ", [start_date])
            #print("DEBUG: forecasting:get_predictor: predictor_dates: ", predictor_dates)
            # Get the predictor from the data for today
            fl.Site.from_df_get_predictor(site, site_data1, [start_date])
            # print(fl.Site.from_df_get_predictor(site, site_data, predictor_dates))
            #print("DEBUG: forecasting:get_predictor: site.predictor: ", site.predictor)

    logger.info(f'   {len(fc_sites)} Predictor discharge gotten from df, namely:\n'
                f'{[[site.code, site.predictor] for site in fc_sites]}')
    # print result_df from December 24, 2009 to January 6, 2010
    # print(result_df[(result_df['Date'] >= dt.datetime.strptime('2009-12-24', '%Y-%m-%d').date()) &
    # (result_df['Date'] <= dt.datetime.strptime('2010-01-06', '%Y-%m-%d').date())])
    logger.info("   ... done")


def get_predictor(modified_data, start_date, fc_sites, ieh_sdk, backend_has_access_to_db, predictor_dates):

    logger.info("Getting predictor ...")
    # region Calculate forecast
    # Get the sum of the last 3 days discharge from the database
    # We get the predictor discharge from the DB only if we are not in
    # operational mode or if the date is after 2020-01-01 in offline mode. This
    # saves runtime.
    if not backend_has_access_to_db:
        logger.info("No access to DB. Looking for predictor from in df (excel data) ...")
    if backend_has_access_to_db and start_date.date() > dt.datetime.strptime('2020-01-01', '%Y-%m-%d').date():
        logger.info("Looking for predictor in iEasyHydro DB ...")
        # For each site in fc_sites, get the predictor discharge from the DB
        for site in fc_sites:
            fl.Site.from_DB_get_predictor(ieh_sdk, site, predictor_dates, lagdays=20)

        # Special case for virtual stations
        # For reservoir some inflows there is no data in the DB
        # Check if code 16936 is in fc_sites
        # If it is, then we need to get the predictor from the sum of other sites,
        # namely Naryn - Uch-Terek 16059 and lateral tributaries Chychkan 16096,
        # Uzun-Akmat 16100 and Torkent (no code in DB...)
        if '16936' in [site.code for site in fc_sites]:
            # Test if station 16093 (Torkent) is in the list of stations
            # (This station is under construction at the time of writing this code)
            if '16093' in [site.code for site in fc_sites]:
                tok_contrib_sites = ["16059", "16096", "16100", "16093"]
                torkent_data = True
            else:
                tok_contrib_sites = ["16059", "16096", "16100"]
                torkent_data = False
            tok_sites = []
            for site in tok_contrib_sites:
                tok_site = fl.Site(site)
                tok_sites.append(tok_site)
            for site in tok_sites:
                fl.Site.from_DB_get_predictor(ieh_sdk, site, predictor_dates)

            # Test if there is data in site.predictor for site 16093. If there
            # is no data, assign 0.6*site.predictor for site 16096 to site 16093.
            if torkent_data:
                if tok_sites[3].predictor is None:
                    tok_sites[3].predictor = 0.6 * tok_sites[1].predictor
                    # Sum the predictors of the contributing sites
                    tok_predictor = sum([site.predictor for site in tok_sites])
            else:
                 tok_predictor = sum([site.predictor for site in tok_sites]) + \
                 0.6 * tok_sites[1].predictor



            # Assign the sum to the predictor of the reservoir
            for site in fc_sites:
                if site.code == '16936':
                    site.predictor = fl.round_discharge_to_float(tok_predictor)

        logger.info(f'   {len(fc_sites)} predictor discharge gotten from DB, namely:'
                    f'\n{[[site.code, site.predictor] for site in fc_sites]}')

    # If we don't find predictors in the database, we can check the data from
    # the Excel sheets in the result_df DataFrame.
    # Iterate through sites in fc_sites and see if we can find the predictor
    # in result_df.
    # This is mostly relevant for the offline mode.
    for site in fc_sites:
        if site.predictor is None or float(site.predictor) < 0.0 or start_date.date() < dt.datetime.strptime('2023-01-01', '%Y-%m-%d').date():
            logger.info(f'    No predictor found in DB for site {site.code}. Getting predictor from df ...')
            # print(site.code)
            # Get the data for the site
            # Here we need to use modified_data as result_df is filtered to the
            # current pentad of the year and does not contain predictor data.
            site_data1 = modified_data[modified_data['Code'] == site.code]
            #print("DEBUG: forecasting:get_predictor: site_data1: \n",
            #      site_data1[['Date', 'Code', 'issue_date', 'discharge_sum']].tail(10))
            #print("DEBUG: forecasting:get_predictor: [start_date]: ", [start_date])
            #print("DEBUG: forecasting:get_predictor: predictor_dates: ", predictor_dates)
            # Get the predictor from the data for today
            fl.Site.from_df_get_predictor(site, site_data1, [start_date])
            # print(fl.Site.from_df_get_predictor(site, site_data, predictor_dates))
            #print("DEBUG: forecasting:get_predictor: site.predictor: ", site.predictor)

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
