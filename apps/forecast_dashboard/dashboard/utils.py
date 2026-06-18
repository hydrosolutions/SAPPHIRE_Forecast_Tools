import datetime as dt
from dashboard.logger import setup_logger
logger = setup_logger()


def hydrate_month_hydrograph_stats(site, month_number: int, db) -> None:
    """Fetch monthly hydrograph stats for *site* and populate norm + last-year Q.

    Fetches ``db.get_hydrograph_pentad_all("month", site.code)``, selects the
    row whose ``month_in_year`` column equals *month_number*, then sets:

    * ``site.hydrograph_norm`` — climatological monthly mean discharge (norm)
    * ``site.month_last_year_q`` — previous-year monthly discharge

    Uses the same year-column resolution logic as
    ``rehydrate_sites_hydrograph_stats``.  The function is NaN/empty/exception-
    safe: on any failure the existing attribute values are left unchanged.

    Args:
        site: Site object with a ``.code`` attribute.
        month_number: Calendar month (1–12) to look up in the hydrograph data.
        db: The ``src.db`` module (injected so the function is testable).
    """
    try:
        df = db.get_hydrograph_pentad_all("month", site.code)
        if df is None or df.empty:
            return

        hin = "month_in_year"
        if hin not in df.columns:
            logger.warning(
                "hydrate_month_hydrograph_stats: column '%s' missing "
                "for station %s — skipping", hin, site.code,
            )
            return

        row_df = df[df[hin] == month_number]
        if row_df.empty:
            logger.warning(
                "hydrate_month_hydrograph_stats: no row for month %s "
                "in station %s — skipping", month_number, site.code,
            )
            return

        row = row_df.iloc[0]

        # Norm
        if "norm" in row_df.columns:
            setattr(site, "hydrograph_norm", row["norm"])

        # Last-year discharge — same year-column fallback as rehydrate_sites_hydrograph_stats
        current_year = dt.datetime.now().year
        yr = current_year
        if str(yr) in df.columns:
            last_yr_col = str(yr - 1)
        elif str(yr - 1) in df.columns:
            yr -= 1
            last_yr_col = str(yr - 1)
        elif str(yr - 2) in df.columns:
            yr -= 2
            last_yr_col = str(yr - 1)
        else:
            logger.warning(
                "hydrate_month_hydrograph_stats: no current-year column "
                "found for station %s — skipping month_last_year_q", site.code,
            )
            return

        if last_yr_col in df.columns:
            setattr(site, "month_last_year_q", row[last_yr_col])

    except Exception:  # noqa: BLE001
        logger.warning(
            "hydrate_month_hydrograph_stats: unexpected error for "
            "station %s — skipping", getattr(site, "code", "?"),
            exc_info=True,
        )


def rehydrate_sites_hydrograph_stats(sites, horizon: str, period_value: int, db) -> None:
    """Re-hydrate hydrograph statistics on each site at bulletin write-time.

    For each site, fetches the per-station hydrograph data from the API and
    sets ``hydrograph_mean``, ``hydrograph_norm``, ``hydrograph_max``,
    ``hydrograph_min``, and ``last_year_q_pentad_mean`` for the given
    period-in-year value. This ensures the attributes are populated even when
    a site was never opened interactively in the dashboard.

    Also sets the just-completed period's observed discharge actuals on each
    site: ``act_q_this`` (current-year discharge in the previous period),
    ``act_q_last`` (previous-year discharge in the previous period), and
    ``act_norm`` (climatological norm for the previous period).

    The function is NaN/empty-safe: if the API returns no data or the
    requested period row or year column is missing, the site's existing value
    is left unchanged and no exception is raised.

    Args:
        sites: Iterable of site objects with a ``.code`` attribute.
        horizon: Forecast horizon string passed to the API — either
            ``"pentad"`` or ``"decade"`` (NOT the legacy ``"decad"``).
        period_value: Period-in-year integer (pentad 1–72 / decad 1–36)
            used to filter the hydrograph row.
        db: The ``src.db`` module (passed in to keep the helper testable via
            injection — callers pass ``src.db`` or a test double).
    """
    # Determine the period-in-year column name the same way db.py does.
    hin = "decad_in_year" if horizon == "decade" else "pentad_in_year"

    # Wrap-around limit depends on horizon: pentad has 72, decade has 36.
    max_period = 36 if horizon == "decade" else 72

    # Resolve current and previous year columns (same fallback logic as
    # update_site_attributes_with_hydrograph_statistics_for_selected_pentad).
    current_year = dt.datetime.now().year

    for site in sites:
        try:
            df = db.get_hydrograph_pentad_all(horizon, site.code)
            if df is None or df.empty:
                continue

            if hin not in df.columns:
                logger.warning(
                    "rehydrate_sites_hydrograph_stats: column '%s' missing "
                    "for station %s — skipping", hin, site.code,
                )
                continue

            row_df = df[df[hin] == period_value]
            if row_df.empty:
                logger.warning(
                    "rehydrate_sites_hydrograph_stats: no row for period %s "
                    "in station %s — skipping", period_value, site.code,
                )
                continue

            row = row_df.iloc[0]

            # Hydrograph envelope statistics
            for attr, col in (
                ("hydrograph_mean", "mean"),
                ("hydrograph_norm", "norm"),
                ("hydrograph_max",  "max"),
                ("hydrograph_min",  "min"),
            ):
                if col in row_df.columns:
                    setattr(site, attr, row[col])

            # Last-year discharge — apply same year-column fallback as utils
            yr = current_year
            if str(yr) in df.columns:
                last_yr_col = str(yr - 1)
            elif str(yr - 1) in df.columns:
                yr -= 1
                last_yr_col = str(yr - 1)
            elif str(yr - 2) in df.columns:
                yr -= 2
                last_yr_col = str(yr - 1)
            else:
                logger.warning(
                    "rehydrate_sites_hydrograph_stats: no current-year "
                    "column found for station %s — skipping last_year_q",
                    site.code,
                )
                continue

            if last_yr_col in df.columns:
                site.last_year_q_pentad_mean = row[last_yr_col]

            # ------------------------------------------------------------------
            # Previous-period actuals (for the ACTUALS table in the bulletin)
            # ------------------------------------------------------------------
            # Determine which period-in-year value is "just completed".
            if period_value == 1:
                # Year wrap: the previous period fell in the previous calendar year.
                prev_period = max_period
                _wrap = True
            else:
                prev_period = period_value - 1
                _wrap = False

            prev_row_df = df[df[hin] == prev_period]
            if prev_row_df.empty:
                # No data for previous period — set actuals to NaN and continue.
                site.act_q_this = float('nan')
                site.act_q_last = float('nan')
                site.act_norm = float('nan')
                continue

            prev_row = prev_row_df.iloc[0]

            if _wrap:
                # Previous period was in the previous calendar year.
                # act_q_this = discharge in that year for the previous period.
                # act_q_last = year before that — not in the df range, so None.
                _this_col = str(current_year - 1)
                site.act_q_this = prev_row[_this_col] if _this_col in prev_row_df.columns else float('nan')
                site.act_q_last = float('nan')
            else:
                # Normal case: previous period is in the current calendar year.
                _this_col = str(yr)
                _last_col = str(yr - 1)
                site.act_q_this = prev_row[_this_col] if _this_col in prev_row_df.columns else float('nan')
                site.act_q_last = prev_row[_last_col] if _last_col in prev_row_df.columns else float('nan')

            site.act_norm = prev_row['norm'] if 'norm' in prev_row_df.columns else float('nan')

        except Exception:  # noqa: BLE001
            logger.warning(
                "rehydrate_sites_hydrograph_stats: unexpected error for "
                "station %s — skipping", getattr(site, "code", "?"),
                exc_info=True,
            )


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
