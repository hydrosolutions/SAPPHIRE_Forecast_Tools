import datetime as dt
from dashboard.logger import setup_logger
logger = setup_logger()


# @pn.depends(pentad_selector, decad_selector, watch=True)
def update_site_attributes_with_hydrograph_statistics_for_selected_pentad(_, sites, df, pentad, decad, horizon, horizon_in_year):
    """Update site attributes with hydrograph statistics for selected pentad"""
    #print(f"\n\n\nDEBUG update_site_attributes_with_hydrograph_statistics_for_selected_pentad: pentad: {pentad}")
    #print(f"column names: {df.columns}")
    # Based on column names and date, figure out which column indicates the
    # last year's Q for the selected pentad
    current_year = dt.datetime.now().year
    #print(f"current year: {current_year}")
    if str(current_year) in df.columns:
        last_year_column = str(current_year - 1)
    else:
        logger.info(f"Column for current year not found. Trying previous year.")
        current_year -= 1
        if str(current_year) in df.columns:
            last_year_column = str(current_year - 1)
        else:
            current_year -= 1
            if str(current_year) in df.columns:
                last_year_column = str(current_year - 1)
            else:
                raise ValueError("No column found for last year's Q.")
    #print(f"\n\nupdate site attributes hydrograph stats: dataframe: {df}")
    # Filter the df for the selected pentad
    if horizon == "pentad":
        horizon_value = pentad
    else:
        horizon_value = decad
    df = df[df[horizon_in_year] == horizon_value].copy()
    # Add a column with the site code
    df['site_code'] = df['station_labels'].str.split(' - ').str[0]
    for site in sites:
        #print(f"site: {site.code}")
        # Test if site.code is in df['site_code']
        if site.code not in df['site_code'].values:
            site.hydrograph_mean = None
            continue
        # Get the hydrograph statistics for each site
        row = df[df['site_code'] == site.code]
        site.hydrograph_mean = row['mean'].values[0]
        site.hydrograph_norm = row['norm'].values[0]
        site.hydrograph_max = row['max'].values[0]
        site.hydrograph_min = row['min'].values[0]
        site.last_year_q_pentad_mean = row[last_year_column].values[0]
        #print(f"site: {site.code}, mean: {site.hydrograph_mean}, max: {site.hydrograph_max}, min: {site.hydrograph_min}, last year mean: {site.last_year_q_pentad_mean}")

    #print(f"Updated sites with hydrograph statistics from DataFrame.")
    return sites


# @pn.depends(pentad_selector, watch=True)
def update_site_attributes_with_linear_regression_predictor(_, sites, df, pentad, decad, horizon, horizon_in_year):
    """Update site attributes with linear regression predictor"""
    # Print pentad
    #print(f"\n\nDEBUGGING update_site_attributes_with_linear_regression_predictor: pentad: {pentad}")
    if horizon == "pentad":
        horizon_value = pentad
    else:
        horizon_value = decad
    # Get row in def for selected pentad
    df = df[df[horizon_in_year] == (horizon_value - 1)].copy()
    #print("\n\nDEBUGGING update_site_attributes_with_linear_regression_predictor")
    #print(f"linreg_predictor: \n{df[df['code'] == '15149']}.tail()")
    # Only keep the last row for each site
    #df = df.drop_duplicates(subset='code', keep='last')
    df = df.sort_values('date').groupby('code').last().reset_index()
    for site in sites:
        #print(f"site: {site.code}")
        # Test if site.code is in df['code']
        if site.code not in df['code'].values:
            site.linreg_predictor = None
            continue
        # Get the linear regression predictor for each site
        row = df[df['code'] == site.code]
        site.linreg_predictor = row['predictor'].values[0]
        print(f"site: {site.code}, linreg predictor: {site.linreg_predictor}")

    #print(f"Updated sites with linear regression predictor from DataFrame.")
    return sites
