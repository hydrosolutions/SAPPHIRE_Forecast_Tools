# Post-processing of Hydrological Forecasts

This module is the final stage of the SAPPHIRE forecast pipeline. It takes
raw model outputs, creates ensemble forecasts, and calculates skill metrics
for quality assessment.

## Forecast Horizons

| Horizon | Period | Boundary days |
|---------|--------|---------------|
| **Pentadal** | 5-day | 5th, 10th, 15th, 20th, 25th, last of month |
| **Decadal** | 10-day | 10th, 20th, last of month |
| **Monthly** | 1-month | 1st of month |
| **Quarterly** | 3-month | 1st of quarter (aggregated from monthly) |
| **Seasonal** | Configurable | Default Apr-Sep, cross-year supported (e.g. Oct-Mar) |
| **Daily** | 1-day | Every day for the next 10 days |

Quarterly and seasonal values are aggregated from monthly forecasts
(`src/aggregation.py`), giving all 9 models ensemble candidate coverage.

## Supported Models

Individual forecast models (created upstream):

- **LR** -- Linear Regression (statistical baseline)
- **TFT** -- Temporal Fusion Transformer
- **TiDE** -- Temporal Information Decomposition Encoder
- **TSMixer** -- Time Series Mixer

Ensemble models (created by this module):

- **NE** -- Neural Ensemble (TFT + TiDE + TSMixer, created upstream in
  `setup_library`). Short-term only, excluded from EM candidates.
- **EM** -- Ensemble Mean (variable composition, only models passing skill
  thresholds). All horizons.
- **Skilled Mean** -- Weighted by 1/MAE. Long-term (monthly, quarterly,
  seasonal).
- **Naive Mean** -- Unweighted average of all models. Long-term (monthly,
  quarterly, seasonal).

## Pipeline Overview

The module has three operational modes:

```
Recalculation (yearly, on-demand)
    |
    +-- produces skill metrics
    |
    v
Operational (daily, boundary days)     Maintenance (nightly)
    |                                      |
    +-- reads skill metrics                +-- reads skill metrics
    +-- creates ensembles for today        +-- detects gaps in lookback window
    +-- writes forecasts                   +-- creates ensembles for gap dates
                                           +-- merges + writes forecasts
```

### 1. Operational

**Entry points:**
- `postprocessing_operational.py` (pentad/decad)
- `postprocessing_operational_long_term.py` (monthly + quarterly + seasonal)

**When:** Boundary days only. Skips otherwise.

**Steps:**
1. Check if today is a boundary day
2. Read today's observed + modelled data from API
3. Read pre-calculated skill metrics from API
4. Create ensemble forecasts (filter by skill thresholds, require >= 2
   qualifying models, compute mean discharge)
5. Write to API (upsert) + CSV (deprecated backup)

**Runtime:** Seconds.

### 2. Maintenance

**Entry points:**
- `postprocessing_maintenance.py` (pentad/decad)
- `postprocessing_maintenance_long_term.py` (monthly + quarterly + seasonal)

**When:** Every night.

**Steps:**
1. Read existing combined forecasts from API
2. Detect (date, code) pairs missing EM rows within lookback window
3. Read data + skill metrics for gap dates
4. Create ensembles for gap dates only
5. Merge new rows with existing data, deduplicate
6. Write to API (upsert) + CSV (deprecated backup)

**Runtime:** Minutes.

### 3. Recalculation

**Entry point:** `recalculate_skill_metrics.py`

**When:** Manually triggered, typically end-of-year or after model changes.

**Steps (pentad/decad):**
1. Read all historical observations + forecasts
2. Calculate skill metrics per (period, code, model)
3. Create all ensembles from scratch
4. Calculate ensemble skill metrics
5. Save forecasts + skill metrics to API + CSV

**Steps (monthly/quarterly/seasonal):**
1. Read daily runoff from API, aggregate to monthly observations
2. Read monthly forecasts, aggregate to target horizon
3. Calculate skill metrics (including CRPS)
4. Create ensembles (EM, Skilled Mean, Naive Mean)
5. Save to API + CSV

**Steps (daily):**
1. Read daily observations + forecasts from API
2. Calculate Tier 1 + Tier 2 metrics
3. Save daily skill metrics to API + CSV

**Runtime:** Hours.

**Note:** `postprocessing_forecasts.py` is deprecated. Use
`recalculate_skill_metrics.py` instead.

## Skill Metrics

### Metric tiers

| Tier | Metrics | Purpose |
|------|---------|---------|
| **Tier 1 (threshold)** | sdivsigma, NSE, accuracy | Filter models for EM inclusion |
| **Tier 1 (always)** | MAE, n_pairs, delta | Calculated for all models |
| **Tier 2 (informational)** | PBIAS, KGE-LF, NSE-log | Additional quality indicators |
| **Tier 2 (daily)** | FDC-FHV, FDC-FLV | Flow duration curve metrics |

### Ensemble filtering thresholds

A model qualifies for EM inclusion only if **all three** pass:

| Metric | Threshold | Env var |
|--------|-----------|---------|
| sdivsigma | < 0.6 | `ieasyhydroforecast_efficiency_threshold` |
| NSE | > 0.8 | `ieasyhydroforecast_nse_threshold` |
| accuracy | > 0.8 | `ieasyhydroforecast_accuracy_threshold` |

Setting a threshold to `'False'` disables that filter.

## Source Modules

```
postprocessing_forecasts/
|-- postprocessing_operational.py          Entry point: daily (pentad/decad)
|-- postprocessing_operational_long_term.py Entry point: daily (monthly/quarterly/seasonal)
|-- postprocessing_maintenance.py          Entry point: nightly gap-fill (pentad/decad)
|-- postprocessing_maintenance_long_term.py Entry point: nightly gap-fill (monthly/quarterly/seasonal)
|-- recalculate_skill_metrics.py           Entry point: yearly recalculation (all horizons)
|-- src/
|   |-- aggregation.py         Monthly -> quarterly/seasonal aggregation
|   |-- api_writer.py          Write forecasts + metrics to SAPPHIRE API
|   |-- data_reader.py         Read skill metrics, forecasts, observations (API primary, CSV fallback)
|   |-- ensemble_calculator.py Create ensemble forecasts (EM, Skilled Mean, Naive Mean)
|   |-- file_writer.py         Write forecasts + metrics to CSV (deprecated)
|   |-- gap_detector.py        Detect missing ensemble rows for maintenance
|   |-- skill_metrics.py       Calculate all skill metrics (METRIC_REGISTRY)
|   |-- write_diagnostics.py   Diagnostic/summary logging
|   +-- postprocessing_tools.py Timing, logging, utilities
+-- tests/                     40 test files, ~1100 tests
```

## Configuration

### Deployment variables (`.env`)

| Variable | Description | Default |
|----------|-------------|---------|
| `ieasyhydroforecast_env_file_path` | Path to environment config file | -- |
| `ieasyforecast_intermediate_data_path` | Root directory for CSV storage | -- |
| `SAPPHIRE_API_ENABLED` | Enable/disable API writes | `true` |
| `SAPPHIRE_API_URL` | API base URL | `http://localhost:8000` |

### Prediction mode

`SAPPHIRE_PREDICTION_MODE` controls which horizons to process:

| Value | Processes |
|-------|-----------|
| `PENTAD` | Pentadal only |
| `DECAD` | Decadal only |
| `BOTH` | Pentad + decad (default for short-term entry points) |
| `MONTHLY` | Monthly only |
| `QUARTERLY` | Quarterly only |
| `SEASONAL` | Seasonal only |
| `DAILY` | Daily skill metrics only |
| `ALL` | All horizons (recalculation) |

Short-term entry points use `BOTH` by default. Long-term entry points
process monthly + quarterly + seasonal regardless of this variable.

### Recalculation parameters

| Variable | Description | Default |
|----------|-------------|---------|
| `SAPPHIRE_SKILL_METRICS_YEAR` | Year tag for CSV filenames and API date computation | Current year |
| `SAPPHIRE_SKILL_METRICS_START_YEAR` | Earliest year for pentad/decad calculation | 20 years ago |
| `SAPPHIRE_RECALC_START_YEAR` | First year for monthly/daily recalculation | Required |
| `SAPPHIRE_RECALC_END_YEAR` | Last year for monthly/daily recalculation | Required |

### Maintenance parameters

| Variable | Description | Default |
|----------|-------------|---------|
| `POSTPROCESSING_GAPFILL_WINDOW_DAYS` | Lookback window for gap detection (increase after outages) | 7 |

### Season configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SAPPHIRE_SEASON_START_MONTH` | First month of season (1-12) | 4 (April) |
| `SAPPHIRE_SEASON_END_MONTH` | Last month of season (1-12) | 9 (September) |

Cross-year wrapping is supported: `start=10, end=3` defines an Oct-Mar
season. The `season_year` is the year of the start month.

### Ensemble thresholds

| Variable | Description | Default |
|----------|-------------|---------|
| `ieasyhydroforecast_efficiency_threshold` | sdivsigma threshold | 0.6 |
| `ieasyhydroforecast_nse_threshold` | NSE threshold | 0.8 |
| `ieasyhydroforecast_accuracy_threshold` | Accuracy threshold | 0.8 |

### CSV output paths (deprecated)

| Variable | Description |
|----------|-------------|
| `ieasyforecast_pentadal_combined_forecast_file` | Pentadal forecast CSV |
| `ieasyforecast_decadal_combined_forecast_file` | Decadal forecast CSV |
| `ieasyforecast_pentadal_skill_metrics_file` | Pentadal skill metrics CSV |
| `ieasyforecast_decadal_skill_metrics_file` | Decadal skill metrics CSV |
| `ieasyforecast_monthly_combined_forecast_file` | Monthly forecast CSV |
| `ieasyforecast_monthly_skill_metrics_file` | Monthly skill metrics CSV |

## Testing

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

Or directly:

```bash
cd apps
SAPPHIRE_TEST_ENV=True pytest postprocessing_forecasts/tests/ -v
```
