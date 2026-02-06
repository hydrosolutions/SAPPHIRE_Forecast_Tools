"""
Post-processing module for long-term forecasts.

WHY?
----
Forecasts are generated over periods [t+k-H+1, t+k] that don't align with calendar months.

Example (issue date: April 25, lead_time=2, horizon=30):
  - Forecast period: May 31 - June 29
  - Target calendar month: June 1-30

Hydromets require calendar month values,
so we adjust forecasts using ratio-based scaling with long-term climatology.

HOW?
----
1. Calculate long-term mean for the forecast period (day-month range) across all years
2. Calculate long-term mean for the target calendar month
3. Compute ratio: Q_forecast / fc_period_lt_mean
4. Apply ratio to calendar month mean: Q_adjusted = calendar_month_lt_mean * ratio

The ratio is clipped to prevent extreme adjustments:
  - N < 5 years: use min/max * 2 bounds
  - N >= 5 years: use Student's t 95% CI bounds

LEAVE-ONE-OUT
-------------
To prevent data leakage in hindcast mode, climatology statistics exclude the prediction year.
Each forecast gets statistics computed from all other years only.
"""
import pandas as pd
import numpy as np
from scipy import stats
from config_forecast import ForecastConfig
from __init__ import LT_FORECAST_BASE_COLUMNS
from lt_utils import infer_q_columns



def calculate_lt_statistics_fc_period(discharge_data: pd.DataFrame,
                                       prediction_data: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate long-term statistics for each forecast period (day-month range).

    For each unique (code, valid_from, valid_to, year) combination:
    1. Get day-month range from valid_from to valid_to
    2. Filter historical discharge to this day-month range (excluding prediction year)
    3. Aggregate each year's mean, then compute overall mean/std
    4. Require >= 50% non-missing days for valid year

    Parameters
    ----------
    discharge_data : pd.DataFrame
        Historical discharge data with columns: date, code, discharge
    prediction_data : pd.DataFrame
        Forecast data with columns: date, code, valid_from, valid_to

    Returns
    -------
    pd.DataFrame
        Statistics with columns: code, valid_from, valid_to, year,
        fc_period_lt_mean, fc_period_lt_std, fc_period_lt_n
    """
    # Handle empty prediction data
    if prediction_data.empty:
        return pd.DataFrame(columns=['code', 'valid_from', 'valid_to', 'year',
                                     'fc_period_lt_mean', 'fc_period_lt_std', 'fc_period_lt_n'])

    # Ensure date columns are datetime
    discharge_data = discharge_data.copy()
    prediction_data = prediction_data.copy()
    discharge_data['date'] = pd.to_datetime(discharge_data['date'])
    prediction_data['valid_from'] = pd.to_datetime(prediction_data['valid_from'])
    prediction_data['valid_to'] = pd.to_datetime(prediction_data['valid_to'])
    prediction_data['date'] = pd.to_datetime(prediction_data['date'])

    # Pre-compute day-month and year for discharge data (done once)
    discharge_data['day_month'] = discharge_data['date'].dt.strftime('%m-%d')
    discharge_data['year'] = discharge_data['date'].dt.year

    # Get unique forecast periods with their day-month representation
    unique_periods = prediction_data[['code', 'valid_from', 'valid_to', 'date']].drop_duplicates()
    unique_periods['prediction_year'] = unique_periods['date'].dt.year
    unique_periods['from_day_month'] = unique_periods['valid_from'].dt.strftime('%m-%d')
    unique_periods['to_day_month'] = unique_periods['valid_to'].dt.strftime('%m-%d')
    unique_periods['expected_days'] = (unique_periods['valid_to'] - unique_periods['valid_from']).dt.days + 1

    # Create unique keys for grouping: (code, from_day_month, to_day_month)
    # This allows us to reuse calculations for periods with same day-month range
    unique_periods['period_key'] = (
        unique_periods['code'].astype(str) + '_' +
        unique_periods['from_day_month'] + '_' +
        unique_periods['to_day_month']
    )

    # Pre-compute yearly means for each (code, period_key, year) combination
    # This avoids redundant filtering of discharge data
    results = []

    # Group by period_key to process similar periods together
    for _, period_group in unique_periods.groupby('period_key'):
        # All rows in this group have same code, from_day_month, to_day_month
        first_row = period_group.iloc[0]
        code = first_row['code']
        from_day_month = first_row['from_day_month']
        to_day_month = first_row['to_day_month']
        expected_days = first_row['expected_days']
        min_required_days = expected_days * 0.5

        # Filter discharge data for this code once
        code_discharge = discharge_data[discharge_data['code'] == code]

        # Apply day-month filter once for this period
        if from_day_month <= to_day_month:
            period_mask = (code_discharge['day_month'] >= from_day_month) & \
                          (code_discharge['day_month'] <= to_day_month)
        else:
            # Crosses year boundary
            period_mask = (code_discharge['day_month'] >= from_day_month) | \
                          (code_discharge['day_month'] <= to_day_month)

        period_discharge = code_discharge[period_mask]

        # Calculate yearly statistics once using groupby
        yearly_stats = period_discharge.groupby('year').agg(
            discharge_mean=('discharge', 'mean'),
            non_missing=('discharge', 'count')
        ).reset_index()

        # Filter years with sufficient data
        yearly_stats_valid = yearly_stats[yearly_stats['non_missing'] >= min_required_days]

        # For each prediction year in this period group, calculate statistics
        # excluding that year
        for _, pred_row in period_group.iterrows():
            prediction_year = pred_row['prediction_year']
            valid_from = pred_row['valid_from']
            valid_to = pred_row['valid_to']

            # Exclude prediction year
            yearly_means = yearly_stats_valid[
                yearly_stats_valid['year'] != prediction_year
            ]['discharge_mean'].values

            # Calculate overall statistics
            if len(yearly_means) > 0:
                fc_period_lt_mean = np.mean(yearly_means)
                fc_period_lt_std = np.std(yearly_means, ddof=1) if len(yearly_means) > 1 else np.nan
                fc_period_lt_n = len(yearly_means)
            else:
                fc_period_lt_mean = np.nan
                fc_period_lt_std = np.nan
                fc_period_lt_n = 0

            results.append({
                'code': code,
                'valid_from': valid_from,
                'valid_to': valid_to,
                'year': prediction_year,
                'fc_period_lt_mean': fc_period_lt_mean,
                'fc_period_lt_std': fc_period_lt_std,
                'fc_period_lt_n': fc_period_lt_n
            })

    return pd.DataFrame(results)


def calculate_lt_statistics_calendar_month(discharge_data: pd.DataFrame,
                                            prediction_years: list) -> pd.DataFrame:
    """
    Calculate long-term statistics for each calendar month with leave-one-out.

    For each code, month, and prediction year:
    1. Exclude only that specific prediction year (leave-one-out)
    2. First aggregate to monthly means per year (>= 50% days required)
    3. Then calculate across-year statistics

    Parameters
    ----------
    discharge_data : pd.DataFrame
        Historical discharge data with columns: date, code, discharge
    prediction_years : list
        Years to calculate leave-one-out statistics for

    Returns
    -------
    pd.DataFrame
        Statistics with columns: code, month, year (excluded year),
        calendar_month_lt_mean, calendar_month_lt_std, calendar_month_lt_n
    """
    discharge_data = discharge_data.copy()
    discharge_data['date'] = pd.to_datetime(discharge_data['date'])
    discharge_data['year'] = discharge_data['date'].dt.year
    discharge_data['month'] = discharge_data['date'].dt.month
    discharge_data['days_in_month'] = discharge_data['date'].dt.days_in_month

    # Aggregate to monthly means per (code, year, month)
    # Count non-missing days and compute mean
    monthly_stats = discharge_data.groupby(['code', 'year', 'month']).agg(
        discharge_mean=('discharge', 'mean'),
        non_missing_days=('discharge', 'count'),
        days_in_month=('days_in_month', 'first')
    ).reset_index()

    # Filter: require >= 50% non-missing days
    monthly_stats = monthly_stats[
        monthly_stats['non_missing_days'] >= monthly_stats['days_in_month'] * 0.5
    ]

    if monthly_stats.empty:
        return pd.DataFrame(columns=['code', 'month', 'year',
                                     'calendar_month_lt_mean', 'calendar_month_lt_std',
                                     'calendar_month_lt_n'])

    # For leave-one-out: create results for each prediction year
    results = []

    for prediction_year in prediction_years:
        # Exclude the prediction year
        filtered = monthly_stats[monthly_stats['year'] != prediction_year]

        if filtered.empty:
            continue

        # Aggregate across years for each (code, month)
        stats = filtered.groupby(['code', 'month']).agg(
            calendar_month_lt_mean=('discharge_mean', 'mean'),
            calendar_month_lt_std=('discharge_mean', 'std'),
            calendar_month_lt_n=('discharge_mean', 'count')
        ).reset_index()

        # Add the excluded year column
        stats['year'] = prediction_year

        results.append(stats)

    if not results:
        return pd.DataFrame(columns=['code', 'month', 'year',
                                     'calendar_month_lt_mean', 'calendar_month_lt_std',
                                     'calendar_month_lt_n'])

    return pd.concat(results, ignore_index=True)


def map_forecasted_period_to_calendar_month(prediction_data: pd.DataFrame,
                                             fc_period_stats: pd.DataFrame,
                                             calendar_month_stats: pd.DataFrame,
                                             operational_month_lead_time: int) -> pd.DataFrame:
    """
    Create mapping table joining forecast period stats with calendar month stats.

    1. Extract target month from date + operational_month_lead_time
    2. Merge fc_period_stats on (code, valid_from, valid_to, year)
    3. Merge calendar_month_stats on (code, target_month, year) for leave-one-out

    Parameters
    ----------
    prediction_data : pd.DataFrame
        Raw forecast data with Q columns and metadata
    fc_period_stats : pd.DataFrame
        Forecast period statistics from calculate_lt_statistics_fc_period
    calendar_month_stats : pd.DataFrame
        Calendar month statistics from calculate_lt_statistics_calendar_month
    operational_month_lead_time : int
        Number of months ahead for the forecast

    Returns
    -------
    pd.DataFrame
        Mapped data with all Q columns + statistics columns
    """
    # Handle empty prediction data
    if prediction_data.empty:
        return prediction_data.copy()

    mapped_data = prediction_data.copy()
    mapped_data['date'] = pd.to_datetime(mapped_data['date'])
    mapped_data['valid_from'] = pd.to_datetime(mapped_data['valid_from'])
    mapped_data['valid_to'] = pd.to_datetime(mapped_data['valid_to'])

    # Extract prediction year for merging
    mapped_data['year'] = mapped_data['date'].dt.year

    # Calculate target month: issue month + operational_month_lead_time
    # Calculate target month and handle year rollover
    issue_month = mapped_data['date'].dt.month
    mapped_data['target_month'] = (issue_month + operational_month_lead_time - 1) % 12 + 1

    # Merge forecast period statistics
    fc_period_stats = fc_period_stats.copy()
    fc_period_stats['valid_from'] = pd.to_datetime(fc_period_stats['valid_from'])
    fc_period_stats['valid_to'] = pd.to_datetime(fc_period_stats['valid_to'])

    mapped_data = mapped_data.merge(
        fc_period_stats,
        on=['code', 'valid_from', 'valid_to', 'year'],
        how='left'
    )

    # Calculate the target year: if target_month < issue_month, we crossed into next year
    # e.g., issue_month=11 (Nov), target_month=1 (Jan) → target_year = issue_year + 1
    # Re-extract issue_month from current mapped_data to ensure index alignment after merge
    issue_month_current = mapped_data['date'].dt.month
    issue_year = mapped_data['date'].dt.year
    mapped_data['target_year'] = issue_year + (mapped_data['target_month'] < issue_month_current).astype(int)

    # Merge calendar month statistics (including target_year for leave-one-out)
    # Use target_year instead of issue year to exclude the correct year from climatology
    mapped_data = mapped_data.merge(
        calendar_month_stats,
        left_on=['code', 'target_month', 'target_year'],
        right_on=['code', 'month', 'year'],
        how='left',
        suffixes=('', '_cal')
    )

    # Adjust the valid_from and valid_to to exactly match the start and end of the target month
    # Use target_year (not original valid_from year) to handle year boundary correctly
    mapped_data['valid_from'] = pd.to_datetime({
        'year': mapped_data['target_year'],
        'month': mapped_data['target_month'],
        'day': 1
    })

    # Set valid_to to last day of target month
    mapped_data['valid_to'] = mapped_data['valid_from'] + pd.offsets.MonthEnd(0)

    return mapped_data


def adjust_forecast_to_calendar_month(mapped_data: pd.DataFrame,
                                       q_columns: list) -> pd.DataFrame:
    """
    Apply ratio adjustment to all Q columns (vectorized implementation).

    For each Q column:
    - Case A: N = 0 → keep raw forecast
    - Case B: N < 5 → use no clipping, just apply ratio (don't trust climatology bounds)
    - Case C: N >= 5 → use Student's t 95% CI

    Adjustment formula:
    log_ratio = log(Q / fc_period_lt_mean)
    log_ratio_clipped = clip(log_ratio, bounds)
    Q_adjusted = calendar_month_lt_mean * exp(log_ratio_clipped)
    Q_adjusted = max(Q_adjusted, 0)  # Non-negative only

    Parameters
    ----------
    mapped_data : pd.DataFrame
        Data with Q columns and statistics
    q_columns : list
        List of Q column names to adjust

    Returns
    -------
    pd.DataFrame
        Adjusted forecast data
    """
    # Handle empty data
    if mapped_data.empty:
        return mapped_data.copy()

    adjusted_data = mapped_data.copy()

    # Pre-compute bounds for all rows (vectorized)
    fc_mean = adjusted_data['fc_period_lt_mean'].values
    fc_std = adjusted_data['fc_period_lt_std'].values
    fc_n = adjusted_data['fc_period_lt_n'].values
    cal_mean = adjusted_data['calendar_month_lt_mean'].values
    cal_n = adjusted_data['calendar_month_lt_n'].values

    # Initialize bounds arrays
    n_rows = len(adjusted_data)
    lower_bounds = np.full(n_rows, -np.inf)
    upper_bounds = np.full(n_rows, np.inf)

    # Case B: N < 5 → use no clipping

    # Case C: N >= 5 → use Student's t 95% CI
    case_c_mask = (fc_n >= 5) & ~np.isnan(fc_std) & (fc_std > 0) & (fc_mean > 0)
    if case_c_mask.any():
        t_critical = stats.t.ppf(0.975, df=fc_n[case_c_mask] - 1)
        
        ci_lower = fc_mean[case_c_mask] - t_critical * fc_std[case_c_mask]
        ci_upper = fc_mean[case_c_mask] + t_critical * fc_std[case_c_mask]

        # Lower bound: only compute log where ci_lower > 0
        lower_bounds_c = np.full_like(ci_lower, -np.inf)
        valid_lower = ci_lower > 0
        lower_bounds_c[valid_lower] = np.log(ci_lower[valid_lower] / fc_mean[case_c_mask][valid_lower])
        
        # Upper bound: always valid since ci_upper > 0
        upper_bounds_c = np.log(ci_upper / fc_mean[case_c_mask])

        lower_bounds[case_c_mask] = lower_bounds_c
        upper_bounds[case_c_mask] = upper_bounds_c

    # Identify rows where we can apply adjustment vs keep raw
    # Case A: N = 0 or missing statistics → keep raw forecast
    can_adjust_mask = (fc_n > 0) & ~np.isnan(fc_mean) & (cal_n > 0) & ~np.isnan(cal_mean)
    valid_for_log_mask = can_adjust_mask & (fc_mean > 0)

    for q_col in q_columns:
        if q_col not in adjusted_data.columns:
            continue

        q_raw = adjusted_data[q_col].values.copy()
        adjusted_col = f'{q_col}'

        # Initialize with NaN
        q_adjusted = np.full(n_rows, np.nan)

        # Case A: keep raw forecast where we can't adjust
        case_a_mask = ~can_adjust_mask & ~np.isnan(q_raw)
        q_adjusted[case_a_mask] = q_raw[case_a_mask]

        # For rows that can be adjusted
        adjustable_mask = valid_for_log_mask & ~np.isnan(q_raw) & (q_raw > 0)

        if adjustable_mask.any():
            # Calculate log ratio
            log_ratio = np.log(q_raw[adjustable_mask] / fc_mean[adjustable_mask])

            # Clip to bounds
            log_ratio_clipped = np.clip(
                log_ratio,
                lower_bounds[adjustable_mask],
                upper_bounds[adjustable_mask]
            )

            # Calculate adjusted forecast
            q_adjusted[adjustable_mask] = cal_mean[adjustable_mask] * np.exp(log_ratio_clipped)

        # Handle non-positive raw values that can be adjusted (set to max(0, raw))
        nonpos_mask = valid_for_log_mask & ~np.isnan(q_raw) & (q_raw <= 0)
        q_adjusted[nonpos_mask] = np.maximum(0, q_raw[nonpos_mask])

        # Ensure non-negative
        q_adjusted = np.where(q_adjusted < 0, 0, q_adjusted)

        adjusted_data[adjusted_col] = q_adjusted

    return adjusted_data


def post_process_lt_forecast(forecast_config: ForecastConfig,
                             observed_discharge_data: pd.DataFrame,
                             raw_forecast: pd.DataFrame) -> pd.DataFrame:
    """
    Main orchestration function for post-processing long-term forecasts.

    Adjusts forecasted values from the forecast period (day-month range)
    to calendar month values using ratio-based adjustment with long-term climatology.

    Parameters
    ----------
    forecast_config : ForecastConfig
        Configuration object with operational_issue_day and operational_month_lead_time
    observed_discharge_data : pd.DataFrame
        Historical discharge data with columns: date, code, discharge
    raw_forecast : pd.DataFrame
        Raw forecast data with Q columns and metadata

    Returns
    -------
    pd.DataFrame
        Adjusted forecast data with original and adjusted Q columns
    """
    # Handle empty forecast data
    if raw_forecast.empty:
        return raw_forecast.copy()

    # Access the necessary parameters from the forecast_config
    operational_month_lead_time = forecast_config.get_operational_month_lead_time()

    # Get TARGET years (to exclude from climatology calculation for leave-one-out)
    # We need to exclude the target year, not the issue year, to prevent data leakage
    # e.g., forecast issued Nov 2020 for Jan 2021 should exclude 2021 data
    raw_forecast_copy = raw_forecast.copy()
    raw_forecast_copy['date'] = pd.to_datetime(raw_forecast_copy['date'])
    issue_month = raw_forecast_copy['date'].dt.month
    issue_year = raw_forecast_copy['date'].dt.year
    target_month = (issue_month + operational_month_lead_time - 1) % 12 + 1
    # If target_month < issue_month, we crossed into the next year
    target_year = issue_year + (target_month < issue_month).astype(int)
    prediction_years = target_year.unique().tolist()

    # Step 1: Calculate long term statistics for the forecasted period (day-month)
    fc_period_lt_stats = calculate_lt_statistics_fc_period(
        discharge_data=observed_discharge_data,
        prediction_data=raw_forecast
    )

    # Step 2: Calculate long term statistics for the calendar month
    calendar_month_lt_stats = calculate_lt_statistics_calendar_month(
        discharge_data=observed_discharge_data,
        prediction_years=prediction_years
    )

    # Step 3: Map forecasted period to calendar month
    mapping_df = map_forecasted_period_to_calendar_month(
        prediction_data=raw_forecast,
        fc_period_stats=fc_period_lt_stats,
        calendar_month_stats=calendar_month_lt_stats,
        operational_month_lead_time=operational_month_lead_time
    )

    # Infer Q columns from the raw forecast data
    q_columns = infer_q_columns(raw_forecast)

    # Step 4: Adjust forecast to calendar month
    adjusted_forecast = adjust_forecast_to_calendar_month(
        mapped_data=mapping_df,
        q_columns=q_columns
    )

    # Retain only relevant columns
    columns_to_retain = LT_FORECAST_BASE_COLUMNS + q_columns
    adjusted_forecast = adjusted_forecast[columns_to_retain]

    return adjusted_forecast
