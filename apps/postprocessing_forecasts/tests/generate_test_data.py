#!/usr/bin/env python
"""Generate deterministic test data for workflow integration tests.

Station design
--------------
Code    Skill profile                        Expected EM
99001   LR + TFT + TiDE all pass thresholds  3-model EM
99002   LR + TFT pass, TiDE fails            2-model EM
99003   Only LR passes, TFT + TiDE fail      No EM (single model)

Discharge values (hand-calculable)
----------------------------------
Station  Observed base  LR bias  TFT bias  TiDE bias  TSMixer bias
99001    ~10 m^3/s      +0.5     -0.3      +0.8       -0.2
99002    ~50 m^3/s      +1.0     -0.5      +25.0      +20.0
99003    ~100 m^3/s     +2.0     +40.0     +50.0      +35.0

Observed year-to-year variation
---------------------------------
Discharge varies ±40% across years (2022-2026) to give large inter-annual
variance for reliable skill metric computation:
  obs(station, date) = base * (1 + 0.4 * (year - 2024) / 2)

Station 99001 across years: [6.0, 8.0, 10.0, 12.0, 14.0]
Station 99002 across years: [30.0, 40.0, 50.0, 60.0, 70.0]
Station 99003 across years: [60.0, 80.0, 100.0, 120.0, 140.0]

Expected EM calculations (at year 2024, i.e. base values):
  99001: mean(10.5, 9.7, 10.8) = 10.333
  99002: mean(51.0, 49.5) = 50.25 (TiDE excluded by skill)
  99003: No EM (only LR passes => single model, no ensemble)

Date ranges
-----------
Pentad: 5 pentad dates/year x 5 years (2022-2026) = 25 dates/station
  Dates: Jan 5, Jan 10, Jan 15, Jan 20, Jan 25 each year
  pentad_in_year values: 1, 2, 3, 4, 5

Decad: 3 decad dates/year x 5 years (2022-2026) = 15 dates/station
  Dates: Jan 10, Jan 20, Jan 31 each year
  decad_in_year values: 1, 2, 3

Re-run this script to regenerate all test_data/ files.
"""

import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
STATIONS = ['99001', '99002', '99003']

# Observed discharge base values per station (m^3/s)
OBS_BASE = {'99001': 10.0, '99002': 50.0, '99003': 100.0}

# Model biases per station (added to observed)
BIASES = {
    'LR': {'99001': 0.5, '99002': 1.0, '99003': 2.0},
    'TFT': {'99001': -0.3, '99002': -0.5, '99003': 40.0},
    'TiDE': {'99001': 0.8, '99002': 25.0, '99003': 50.0},
    'TSMixer': {'99001': -0.2, '99002': 20.0, '99003': 35.0},
}

YEARS = range(2022, 2027)

# Pentad dates: Jan 5, 10, 15, 20, 25 each year
PENTAD_DAYS = [5, 10, 15, 20, 25]
# Decad dates: Jan 10, 20, 31 each year
DECAD_DAYS = [10, 20, 31]

# Skill metrics thresholds (matching env vars)
EFFICIENCY_THRESHOLD = 0.6   # sdivsigma < this
ACCURACY_THRESHOLD = 0.8     # accuracy > this
NSE_THRESHOLD = 0.8          # nse > this

# Delta (measurement uncertainty) per station
DELTA = {'99001': 5.0, '99002': 8.0, '99003': 10.0}

OUTPUT_DIR = Path(__file__).parent / 'test_data'

# ---------------------------------------------------------------------------
# Date generation
# ---------------------------------------------------------------------------


def pentad_dates():
    """Generate pentad dates: Jan 5,10,15,20,25 for years 2022-2026."""
    dates = []
    for year in YEARS:
        for day in PENTAD_DAYS:
            dates.append(pd.Timestamp(year, 1, day))
    return sorted(dates)


def decad_dates():
    """Generate decad dates: Jan 10,20,31 for years 2022-2026."""
    dates = []
    for year in YEARS:
        for day in DECAD_DAYS:
            dates.append(pd.Timestamp(year, 1, day))
    return sorted(dates)


def get_pentad_in_month(date):
    """Pentad within month (1-6)."""
    return min((date.day - 1) // 5 + 1, 6)


def get_pentad_in_year(date):
    """Pentad within year (1-72)."""
    day_of_year = date.timetuple().tm_yday
    return min((day_of_year - 1) // 5 + 1, 72)


def get_decad_in_month(date):
    """Decad within month (1-3)."""
    return min((date.day - 1) // 10 + 1, 3)


def get_decad_in_year(date):
    """Decad within year (1-36)."""
    month_offset = (date.month - 1) * 3
    return month_offset + get_decad_in_month(date)


# ---------------------------------------------------------------------------
# Discharge functions — year-based variation for reliable skill metrics
# ---------------------------------------------------------------------------


def observed_discharge(station, date):
    """Return observed discharge for a station at a given date.

    Uses year-to-year variation (±40%) so that within each
    pentad_in_year group (5 dates, one per year), the observed
    variance is large enough for meaningful NSE/accuracy/sdivsigma.

    obs = base * (1 + 0.4 * (year - 2024) / 2)

    99001: [6.0, 8.0, 10.0, 12.0, 14.0] across 2022-2026
    99002: [30.0, 40.0, 50.0, 60.0, 70.0]
    99003: [60.0, 80.0, 100.0, 120.0, 140.0]
    """
    base = OBS_BASE[station]
    year_offset = date.year - 2024  # -2, -1, 0, 1, 2
    return round(base * (1 + 0.4 * year_offset / 2), 3)


def forecasted_discharge(station, date, model):
    """Return forecasted discharge = observed + constant bias."""
    obs = observed_discharge(station, date)
    bias = BIASES[model][station]
    return round(obs + bias, 3)


# ---------------------------------------------------------------------------
# File generators
# ---------------------------------------------------------------------------


def generate_runoff_pentad():
    """runoff_pentad.csv — observed pentadal discharge."""
    dates = pentad_dates()
    rows = []
    for date in dates:
        for station in STATIONS:
            rows.append({
                'date': date.strftime('%Y-%m-%d'),
                'code': station,
                'predictor': '',
                'discharge_avg': observed_discharge(station, date),
                'pentad': get_pentad_in_month(date),
                'pentad_in_year': get_pentad_in_year(date),
            })
    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / 'runoff_pentad.csv', index=False)
    print(f"  runoff_pentad.csv: {len(df)} rows")
    return df


def generate_runoff_decad():
    """runoff_decad.csv — observed decadal discharge."""
    dates = decad_dates()
    rows = []
    for date in dates:
        for station in STATIONS:
            rows.append({
                'date': date.strftime('%Y-%m-%d'),
                'code': station,
                'predictor': '',
                'discharge_avg': observed_discharge(station, date),
                'decad': get_decad_in_month(date),
                'decad_in_year': get_decad_in_year(date),
            })
    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / 'runoff_decad.csv', index=False)
    print(f"  runoff_decad.csv: {len(df)} rows")
    return df


def generate_linreg_pentad():
    """forecast_pentad_linreg.csv — LR pentadal forecasts.

    Includes stats columns (q_mean, q_std_sigma, delta) that
    setup_library splits into a separate stats DataFrame.
    """
    dates = pentad_dates()
    rows = []
    for date in dates:
        for station in STATIONS:
            obs = observed_discharge(station, date)
            fc = forecasted_discharge(station, date, 'LR')
            rows.append({
                'date': date.strftime('%Y-%m-%d'),
                'code': station,
                'predictor': '',
                'discharge_avg': obs,
                'pentad_in_month': get_pentad_in_month(date),
                'pentad_in_year': get_pentad_in_year(date),
                'slope': 1.0,
                'intercept': BIASES['LR'][station],
                'forecasted_discharge': fc,
                'q_mean': OBS_BASE[station],
                'q_std_sigma': OBS_BASE[station] * 0.1,
                'delta': DELTA[station],
                'rsquared': 0.95,
            })
    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / 'forecast_pentad_linreg.csv', index=False)
    print(f"  forecast_pentad_linreg.csv: {len(df)} rows")
    return df


def generate_linreg_decad():
    """forecast_decad_linreg.csv — LR decadal forecasts."""
    dates = decad_dates()
    rows = []
    for date in dates:
        for station in STATIONS:
            obs = observed_discharge(station, date)
            fc = forecasted_discharge(station, date, 'LR')
            rows.append({
                'date': date.strftime('%Y-%m-%d'),
                'code': station,
                'predictor': '',
                'discharge_avg': obs,
                'decad_in_month': get_decad_in_month(date),
                'decad_in_year': get_decad_in_year(date),
                'slope': 1.0,
                'intercept': BIASES['LR'][station],
                'forecasted_discharge': fc,
                'q_mean': OBS_BASE[station],
                'q_std_sigma': OBS_BASE[station] * 0.1,
                'delta': DELTA[station],
                'rsquared': 0.95,
            })
    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / 'forecast_decad_linreg.csv', index=False)
    print(f"  forecast_decad_linreg.csv: {len(df)} rows")
    return df


def generate_ml_forecast(model, horizon_type):
    """Generate ML forecast CSV for one model and horizon type.

    ML CSVs use a different format with quantile columns.
    """
    model_upper = model.upper()
    dates = pentad_dates() if horizon_type == 'pentad' else decad_dates()
    rows = []
    for date in dates:
        for station in STATIONS:
            fc = forecasted_discharge(station, date, model)
            row = {}
            # Quantile columns (Q5..Q95) — all set to fc with small spread
            for q in range(5, 100, 5):
                offset = (q - 50) / 100.0 * fc * 0.1
                row[f'Q{q}'] = round(fc + offset, 3)
            row['date'] = date.strftime('%Y-%m-%d')
            row['code'] = station
            row['forecast_date'] = date.strftime('%Y-%m-%d')
            row['flag'] = 0
            rows.append(row)
    df = pd.DataFrame(rows)

    subdir = OUTPUT_DIR / 'predictions' / model_upper
    subdir.mkdir(parents=True, exist_ok=True)
    filename = f'{horizon_type}_{model_upper}_forecast.csv'
    df.to_csv(subdir / filename, index=False)
    print(f"  predictions/{model_upper}/{filename}: {len(df)} rows")
    return df


def _compute_skill_metrics(station, model, dates, horizon_type):
    """Compute skill metrics from test data for a (station, model) pair.

    Returns dict with sdivsigma, nse, delta, accuracy, mae, n_pairs.
    Uses the same formulas as skill_metrics.py.
    """
    n = len(dates)
    obs_vals = []
    fc_vals = []
    for date in dates:
        obs_vals.append(observed_discharge(station, date))
        fc_vals.append(forecasted_discharge(station, date, model))

    obs = np.array(obs_vals)
    fc = np.array(fc_vals)

    # NSE
    obs_mean = obs.mean()
    ss_res = np.sum((obs - fc) ** 2)
    ss_obs = np.sum((obs - obs_mean) ** 2)
    nse = 1.0 - ss_res / max(ss_obs, 1e-10)

    # sdivsigma (sd of errors / sd of observed)
    errors = obs - fc
    sd_errors = np.std(errors, ddof=0)
    sd_obs = np.std(obs, ddof=0)
    sdivsigma = sd_errors / max(sd_obs, 1e-10)

    # MAE
    mae_val = np.mean(np.abs(errors))

    # Accuracy (fraction within delta)
    delta_val = DELTA[station]
    within_delta = np.abs(errors) <= delta_val
    accuracy = np.mean(within_delta)

    return {
        'sdivsigma': round(sdivsigma, 6),
        'nse': round(nse, 6),
        'delta': round(delta_val, 3),
        'accuracy': round(accuracy, 6),
        'mae': round(mae_val, 6),
        'n_pairs': n,
    }


def generate_skill_metrics_pentad():
    """skill_metrics_pentad.csv — pre-calculated skill metrics.

    One row per (pentad_in_year, code, model).
    Metrics computed from the test data biases to ensure consistency.
    """
    dates = pentad_dates()
    models = ['LR', 'TFT', 'TiDE']
    rows = []

    # Group dates by pentad_in_year
    dates_by_piy = {}
    for date in dates:
        piy = get_pentad_in_year(date)
        dates_by_piy.setdefault(piy, []).append(date)

    for piy, piy_dates in sorted(dates_by_piy.items()):
        for station in STATIONS:
            for model in models:
                metrics = _compute_skill_metrics(
                    station, model, piy_dates, 'pentad'
                )
                rows.append({
                    'pentad_in_year': piy,
                    'code': station,
                    'model_short': model,
                    **metrics,
                })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / 'skill_metrics_pentad.csv', index=False)
    print(f"  skill_metrics_pentad.csv: {len(df)} rows")
    _print_skill_summary(df, 'pentad')
    return df


def generate_skill_metrics_decad():
    """skill_metrics_decad.csv — pre-calculated skill metrics for decad."""
    dates = decad_dates()
    models = ['LR', 'TFT', 'TiDE']
    rows = []

    # Group dates by decad_in_year
    dates_by_diy = {}
    for date in dates:
        diy = get_decad_in_year(date)
        dates_by_diy.setdefault(diy, []).append(date)

    for diy, diy_dates in sorted(dates_by_diy.items()):
        for station in STATIONS:
            for model in models:
                metrics = _compute_skill_metrics(
                    station, model, diy_dates, 'decad'
                )
                rows.append({
                    'decad_in_year': diy,
                    'code': station,
                    'model_short': model,
                    **metrics,
                })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / 'skill_metrics_decad.csv', index=False)
    print(f"  skill_metrics_decad.csv: {len(df)} rows")
    _print_skill_summary(df, 'decad')
    return df


def _print_skill_summary(df, horizon_type):
    """Print which models pass thresholds per station."""
    period_col = (
        'pentad_in_year' if horizon_type == 'pentad' else 'decad_in_year'
    )
    for station in STATIONS:
        sdf = df[df['code'] == station]
        passing = sdf[
            (sdf['sdivsigma'] < EFFICIENCY_THRESHOLD) &
            (sdf['accuracy'] > ACCURACY_THRESHOLD) &
            (sdf['nse'] > NSE_THRESHOLD)
        ]
        models_pass = sorted(passing['model_short'].unique())
        n_periods = sdf[period_col].nunique()
        print(
            f"    {station}: passing models = {models_pass} "
            f"({n_periods} periods)"
        )


def generate_combined_forecasts_pentad():
    """combined_forecasts_pentad.csv — for maintenance gap detection.

    Contains LR+TFT+TiDE rows for all dates/stations,
    plus EM rows for most dates — with deliberate gaps:
    - 99001 at 2026-01-05 (pentad_in_year=1): has models but no EM
    - 99002 at 2026-01-10 (pentad_in_year=2): has models but no EM
    """
    dates = pentad_dates()
    gap_entries = {
        ('99001', '2026-01-05'),
        ('99002', '2026-01-10'),
    }
    models = ['LR', 'TFT', 'TiDE']
    rows = []
    for date in dates:
        datestr = date.strftime('%Y-%m-%d')
        piy = get_pentad_in_year(date)
        pim = get_pentad_in_month(date)
        for station in STATIONS:
            # Individual model rows always present
            for model in models:
                fc = forecasted_discharge(station, date, model)
                rows.append({
                    'horizon_type': 'pentad',
                    'code': station,
                    'date': datestr,
                    'horizon_value': pim,
                    'horizon_in_year': piy,
                    'predictor': '',
                    'slope': 1.0,
                    'intercept': 0.0,
                    'forecasted_discharge': fc,
                    'rsquared': 0.95,
                    'id': '',
                    'model_short': model,
                    'pentad_in_month': pim,
                    'pentad_in_year': piy,
                    'model_type': model,
                    'target': datestr,
                    'flag': 0,
                    'composition': '',
                    'q05': fc * 0.9,
                    'q25': fc * 0.95,
                    'q50': fc,
                    'q75': fc * 1.05,
                    'q95': fc * 1.1,
                    'discharge': fc,
                })

            # EM row — skip for gap entries
            if (station, datestr) not in gap_entries:
                # Simple EM as mean of all models for this station
                em_val = np.mean([
                    forecasted_discharge(station, date, m)
                    for m in models
                ])
                rows.append({
                    'horizon_type': 'pentad',
                    'code': station,
                    'date': datestr,
                    'horizon_value': pim,
                    'horizon_in_year': piy,
                    'predictor': '',
                    'slope': '',
                    'intercept': '',
                    'forecasted_discharge': round(em_val, 3),
                    'rsquared': '',
                    'id': '',
                    'model_short': 'EM',
                    'pentad_in_month': pim,
                    'pentad_in_year': piy,
                    'model_type': 'EM',
                    'target': datestr,
                    'flag': 0,
                    'composition': 'LR, TFT, TiDE',
                    'q05': '',
                    'q25': '',
                    'q50': '',
                    'q75': '',
                    'q95': '',
                    'discharge': round(em_val, 3),
                })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / 'combined_forecasts_pentad.csv', index=False)
    n_em = len(df[df['model_short'] == 'EM'])
    n_gaps = len(gap_entries)
    print(
        f"  combined_forecasts_pentad.csv: {len(df)} rows "
        f"({n_em} EM, {n_gaps} gaps)"
    )
    return df


def generate_combined_forecasts_decad():
    """combined_forecasts_decad.csv — for maintenance gap detection.

    Gaps:
    - 99001 at 2026-01-10 (decad_in_year=1): has models but no EM
    """
    dates = decad_dates()
    gap_entries = {
        ('99001', '2026-01-10'),
    }
    models = ['LR', 'TFT', 'TiDE']
    rows = []
    for date in dates:
        datestr = date.strftime('%Y-%m-%d')
        diy = get_decad_in_year(date)
        dim = get_decad_in_month(date)
        for station in STATIONS:
            for model in models:
                fc = forecasted_discharge(station, date, model)
                rows.append({
                    'horizon_type': 'decad',
                    'code': station,
                    'date': datestr,
                    'horizon_value': dim,
                    'horizon_in_year': diy,
                    'predictor': '',
                    'slope': 1.0,
                    'intercept': 0.0,
                    'forecasted_discharge': fc,
                    'rsquared': 0.95,
                    'id': '',
                    'model_short': model,
                    'decad_in_month': dim,
                    'decad_in_year': diy,
                    'model_type': model,
                    'target': datestr,
                    'flag': 0,
                    'composition': '',
                    'q05': fc * 0.9,
                    'q25': fc * 0.95,
                    'q50': fc,
                    'q75': fc * 1.05,
                    'q95': fc * 1.1,
                    'discharge': fc,
                })

            if (station, datestr) not in gap_entries:
                em_val = np.mean([
                    forecasted_discharge(station, date, m)
                    for m in models
                ])
                rows.append({
                    'horizon_type': 'decad',
                    'code': station,
                    'date': datestr,
                    'horizon_value': dim,
                    'horizon_in_year': diy,
                    'predictor': '',
                    'slope': '',
                    'intercept': '',
                    'forecasted_discharge': round(em_val, 3),
                    'rsquared': '',
                    'id': '',
                    'model_short': 'EM',
                    'decad_in_month': dim,
                    'decad_in_year': diy,
                    'model_type': 'EM',
                    'target': datestr,
                    'flag': 0,
                    'composition': 'LR, TFT, TiDE',
                    'q05': '',
                    'q25': '',
                    'q50': '',
                    'q75': '',
                    'q95': '',
                    'discharge': round(em_val, 3),
                })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / 'combined_forecasts_decad.csv', index=False)
    n_em = len(df[df['model_short'] == 'EM'])
    n_gaps = len(gap_entries)
    print(
        f"  combined_forecasts_decad.csv: {len(df)} rows "
        f"({n_em} EM, {n_gaps} gaps)"
    )
    return df


def generate_config_files():
    """Generate config JSON files for test stations."""
    config_dir = OUTPUT_DIR / 'config'
    config_dir.mkdir(parents=True, exist_ok=True)

    # config_all_stations_library.json
    stations_config = {"stations_available_for_forecast": {}}
    for i, station in enumerate(STATIONS):
        stations_config["stations_available_for_forecast"][station] = {
            "id": [float(i + 1)],
            "basin": ["Test Basin"],
            "lat": [42.0 + i * 0.1],
            "long": [74.0 + i * 0.1],
            "country": ["Testland"],
            "is_virtual": [False],
            "region": ["Test Region"],
            "site_type": ["automatic-discharge"],
            "name_ru": [f"Test Station {station}"],
            "organization_id": [1],
            "elevation": [1000.0 + i * 100],
            "river_ru": [f"Test River {station}"],
            "punkt_ru": [f"Test Point {station}"],
            "code": [int(station)],
            "header": [station],
        }
    with open(config_dir / 'config_all_stations_library.json', 'w') as f:
        json.dump(stations_config, f, indent=2)
    print("  config_all_stations_library.json")

    # config_station_selection.json
    selection = {"stationsID": STATIONS}
    with open(config_dir / 'config_station_selection.json', 'w') as f:
        json.dump(selection, f, indent=2)
    print("  config_station_selection.json")

    # config_output.json
    output_config = {"write_excel": False}
    with open(config_dir / 'config_output.json', 'w') as f:
        json.dump(output_config, f, indent=2)
    print("  config_output.json")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Generating test data files...")
    print()

    print("Input files for setup_library reads:")
    generate_runoff_pentad()
    generate_runoff_decad()
    generate_linreg_pentad()
    generate_linreg_decad()
    for model in ['TFT', 'TiDE', 'TSMixer']:
        generate_ml_forecast(model, 'pentad')
        generate_ml_forecast(model, 'decad')
    print()

    print("Input files for postprocessing src reads:")
    generate_skill_metrics_pentad()
    generate_skill_metrics_decad()
    generate_combined_forecasts_pentad()
    generate_combined_forecasts_decad()
    print()

    print("Config files:")
    generate_config_files()
    print()

    print("Done! All files written to:", OUTPUT_DIR)


if __name__ == '__main__':
    main()
