# Postprocessing Forecasts Refactoring Plan

## Forecast Aggregation Schedule

### 1. Daily to Pentadal/Decadal Aggregation
**Data Sources:** Machine Learning + Conceptual Model daily forecasts

| Aggregation Type | Trigger Date | Description |
|------------------|--------------|-------------|
| Pentadal | Last day of each pentad (days 5, 10, 15, 20, 25, last day of month) | Aggregate daily forecasts to 5-day periods |
| Decadal | Last day of each decade (days 10, 20, last day of month) | Aggregate daily forecasts to 10-day periods |

### 2. Monthly to Quarterly/Seasonal Aggregation

#### Monthly Forecast Database Schema
Monthly forecasts use the following columns in the database (amongh others, there are more):
- `date` - Forecast issue date
- `valid_from` - Start of validity period
- `valid_to` - End of validity period
- `horizon_value` - Lead time (0 for 10th of month forecast, 1 for next month)

#### Quarterly Forecasts (Average runoff over next 3 months)
| Forecast Date | Quarter Covered |
|---------------|-----------------|
| Dec 25 | Jan-Feb-Mar (Q1) |
| Mar 25 | Apr-May-Jun (Q2) |
| Jun 25 | Jul-Aug-Sep (Q3) |
| Sep 25 | Oct-Nov-Dec (Q4) |

#### Seasonal Forecasts (Average runoff April-September)
| Forecast Date | Period Covered | Type |
|---------------|----------------|------|
| Jan 10 | Apr-Sep | Update |
| Feb 10 | Apr-Sep | Update |
| Mar 10 | Apr-Sep | Update |
| Apr 10 | Apr-Sep | Update |
| May 10 | Apr-Sep | Update |

#### Monthly Update Schedule
| Date | Task |
|------|------|
| 25th of each month | Initial quarterly/seasonal forecast |
| 10th of each month | Update of forecast for same period |

---

## Current Architecture Summary

**Note:** The `apps/backend/` module is deprecated and will not be used for this refactoring.

### Key Entry Points

| File | Purpose |
|------|---------|
| `apps/postprocessing_forecasts/postprocessing_forecasts.py` | Main postprocessing orchestrator |
| `apps/pipeline/pipeline_docker.py` | Luigi-based pipeline |
| `apps/iEasyHydroForecast/forecast_library.py` | Core forecast functions |
| `apps/iEasyHydroForecast/setup_library.py` | Configuration and data loading |
| `apps/machine_learning_monthly/` | Monthly ML forecasts module |

### Temporal Resolutions Supported

| Resolution | Periods/Year | Current Status |
|------------|--------------|----------------|
| Daily | 365 | Implemented |
| Pentadal (5-day) | 72 | Implemented |
| Decadal (10-day) | 36 | Implemented |
| Monthly | 12 | Implemented in `machine_learning_monthly/` |
| Quarterly | 4 | Not yet implemented |
| Seasonal | 1 | Not yet implemented |

### Current Postprocessing Functions

#### Pentadal (`forecast_library.py`)
- `calculate_skill_metrics_pentad()` - Line 1752
- `save_forecast_data_pentad()` - Line 3998
- `save_pentadal_skill_metrics()` - Line 3833

#### Decadal (`forecast_library.py`)
- `calculate_skill_metrics_decade()` - Line 2072
- `save_forecast_data_decade()` - Line 4041
- `save_decadal_skill_metrics()` - Line 3882

#### Monthly (`machine_learning_monthly/`)
- Quarterly and seasonal forecasting not yet implemented in this module

### Date Utilities (`tag_library.py`)

| Function | Purpose |
|----------|---------|
| `get_pentad(date)` | Returns pentad of month (1-6) |
| `get_pentad_in_year(date)` | Returns pentad of year (1-72) |
| `get_decad_in_month(date)` | Returns decad of month (1-3) |
| `get_date_for_pentad(pentad, year)` | Converts pentad number to date |
| `get_date_for_decad(decad, year)` | Converts decad number to date |

### Data Models

#### ForecastFlags Class (`iEasyHydroForecast/setup_library.py`)
```python
class ForecastFlags:
    pentad: bool    # 5-day forecast
    decad: bool     # 10-day forecast
    month: bool     # Monthly forecast (exists but not fully used)
    season: bool    # Seasonal forecast (exists but not fully used)
```

#### PredictorDates Class (`iEasyHydroForecast/forecast_library.py`)
```python
class PredictorDates:
    pentad: list    # Pentad predictor dates
    decad: list     # Decad predictor dates
    month: list     # Monthly predictor dates (exists)
    season: list    # Seasonal predictor dates (exists)
```

### Aggregation Functions

| Function | Location | Purpose |
|----------|----------|---------|
| `calculate_pentadaldischargeavg()` | forecast_library.py:576 | Daily to 5-day average |
| `calculate_decadaldischargeavg()` | forecast_library.py:651 | Daily to 10-day average |
| `aggregate_decadal_to_monthly()` | preprocessing_station_forcing/src.py:100 | Decadal to Monthly |
| `create_monthly_df()` | machine_learning_monthly/data_utils.py:369 | Daily to Monthly |

### Output Files

| File | Purpose |
|------|---------|
| `combined_forecasts_pentad.csv` | Combined pentadal forecasts |
| `combined_forecasts_decad.csv` | Combined decadal forecasts |
| `skill_metrics_pentad.csv` | Pentadal skill metrics |
| `skill_metrics_decad.csv` | Decadal skill metrics |

---

## Gaps Identified for Refactoring

### 1. Missing Quarterly Aggregation
- No `calculate_quarterly_avg()` function exists
- No quarterly skill metrics calculation
- No quarterly output files defined
- Not yet implemented in `machine_learning_monthly/` module

### 2. Missing Seasonal (Apr-Sep) Aggregation
- Seasonal flag exists in `ForecastFlags` but not fully implemented
- No seasonal aggregation function
- No seasonal forecast issuance logic
- Not yet implemented in `machine_learning_monthly/` module

### 3. Date Scheduling for New Forecasts
Need to implement:
- Quarterly forecast dates: Dec 25, Mar 25, Jun 25, Sep 25
- Seasonal forecast dates: Jan 10, Feb 10, Mar 10, Apr 10, May 10
- Update logic for 10th of month updates (using `horizon_value` in database)

### 4. Monthly to Quarterly/Seasonal Pipeline
- `machine_learning_monthly/` exists but not integrated with quarterly aggregation
- Need pipeline tasks for quarterly and seasonal forecasts

---

## Recommended Refactoring Steps

### Phase 1: Add Date Utilities
1. Add `get_quarter(date)` function to `tag_library.py`
2. Add `is_quarterly_forecast_date(date)` function
3. Add `is_seasonal_forecast_date(date)` function
4. Add `get_season_months()` helper (returns Apr-Sep months)

### Phase 2: Add Aggregation Functions
1. Create `calculate_quarterly_discharge_avg()` in `forecast_library.py`
2. Create `calculate_seasonal_discharge_avg()` for Apr-Sep period
3. Create `aggregate_monthly_to_quarterly()` function

### Phase 3: Add Skill Metrics
1. Create `calculate_skill_metrics_quarterly()`
2. Create `calculate_skill_metrics_seasonal()`
3. Add corresponding save functions

### Phase 4: Update Pipeline
1. Add `RunQuarterlyWorkflow` Luigi task
2. Add `RunSeasonalWorkflow` Luigi task
3. Add new marker files for tracking
4. Update `ForecastFlags` handling

### Phase 5: Update Configuration
1. Add quarterly/seasonal output file paths to `.env`
2. Add quarterly/seasonal templates if needed
3. Update configuration schemas as needed

---

## File Locations Summary

```
apps/
├── postprocessing_forecasts/
│   ├── postprocessing_forecasts.py    # Main entry point
│   └── src/postprocessing_tools.py    # Helper tools
├── iEasyHydroForecast/
│   ├── forecast_library.py            # Core functions (aggregation, skill metrics)
│   ├── setup_library.py               # Config, data loading, ForecastFlags
│   └── tag_library.py                 # Date utilities
├── machine_learning_monthly/          # Monthly ML forecasts (to be extended for quarterly/seasonal)
├── long_term_forecasting/             # Long-term forecast system
└── pipeline/
    └── pipeline_docker.py             # Luigi orchestration
```

**Note:** `apps/backend/` is deprecated and excluded from this refactoring.

---

## Skill Metrics Recalculation Optimization

### Current Problem

The postprocessing module currently recalculates forecast skill metrics **every day**, but this is unnecessary. Skill metrics only need to be recalculated **once per year** (or on demand for maintenance).

### Current Execution Flow

**File:** `apps/postprocessing_forecasts/postprocessing_forecasts.py` (Lines 117-222)

```
1. Load environment (setup)
2. IF mode includes PENTAD:
   - Read ALL pentadal observed & modelled data (since 2010)
   - Calculate skill metrics for ALL data
   - Create ensemble from highly-skilled models
   - Save forecast data
   - Save skill metrics
3. IF mode includes DECAD:
   - Read ALL decadal observed & modelled data (since 2010)
   - Calculate skill metrics for ALL data
   - Create ensemble from highly-skilled models
   - Save forecast data
   - Save skill metrics
```

**Key Issue:** Steps 2 and 3 process ALL historical data (2010-present) every run, even though only new forecasts need to be saved daily.

### What Needs to Run Daily vs Yearly

| Operation | Current Frequency | Required Frequency |
|-----------|-------------------|-------------------|
| Save new forecast data | Daily | Daily |
| Calculate skill metrics | Daily | Yearly (or on-demand) |
| Create ensemble forecasts | Daily | Yearly (uses skill metrics) |
| Filter highly-skilled models | Daily | Yearly |

### Proposed Solution: Separate Maintenance Script

Since skill metrics recalculation only happens **once per year**, a dedicated maintenance script is the recommended approach.

#### Option A: `--maintenance` Argument (NOT RECOMMENDED)

```bash
# Daily
python postprocessing_forecasts.py

# Yearly maintenance
python postprocessing_forecasts.py --maintenance
```

| Pros | Cons |
|------|------|
| Single entry point | Risk of accidentally running maintenance daily |
| Shared code | More complex conditional logic in main script |
| | Harder to understand script behavior |
| | Docker container needs different invocation |

#### Option B: Separate Script (RECOMMENDED)

```bash
# Daily operations
python postprocessing_forecasts.py

# Yearly maintenance (separate script)
python recalculate_skill_metrics.py
```

| Pros | Cons |
|------|------|
| Clear separation of concerns | Two scripts to maintain |
| No risk of accidental execution | Some code duplication (can be minimized with shared module) |
| Can be scheduled independently in cron/pipeline | |
| Easier to understand each script's purpose | |
| Can have different Docker containers | |
| Simpler main postprocessing script | |
| Easier testing (test each script independently) | |

#### Recommended Implementation

**1. Create new script:** `apps/postprocessing_forecasts/recalculate_skill_metrics.py`

```python
"""
Yearly maintenance script to recalculate forecast skill metrics.

Run this script once per year (recommended: November/December) to:
- Recalculate skill metrics for all models, stations, and pentads/decads
- Update skill_metrics_pentad.csv and skill_metrics_decad.csv
- These files are used for ensemble selection throughout the next year

Usage:
    SAPPHIRE_PREDICTION_MODE=BOTH python recalculate_skill_metrics.py
"""
```

**2. Refactor shared code** into `apps/postprocessing_forecasts/src/skill_metrics.py`:
- `calculate_skill_metrics_pentad()`
- `calculate_skill_metrics_decade()`
- `save_pentadal_skill_metrics()`
- `save_decadal_skill_metrics()`

**3. Simplify daily script** (`postprocessing_forecasts.py`):
- Remove skill metrics calculation
- Only save new forecast data
- Read existing skill metrics for ensemble creation

#### File Structure After Refactoring

```
apps/postprocessing_forecasts/
├── postprocessing_forecasts.py      # Daily: save forecasts, create ensemble
├── recalculate_skill_metrics.py     # Yearly: recalculate all skill metrics
├── src/
│   ├── skill_metrics.py             # Shared skill metrics functions
│   └── postprocessing_tools.py      # Existing helper tools
└── tests/
    ├── test_postprocessing.py
    └── test_skill_metrics.py
```

#### Pipeline Integration

**Daily Pipeline (existing):**
```python
class PostProcessingForecasts(DockerTaskBase):
    # Runs postprocessing_forecasts.py
    # Fast execution, no skill recalculation
```

**Yearly Maintenance (new Luigi task or manual):**
```python
class RecalculateSkillMetrics(DockerTaskBase):
    # Runs recalculate_skill_metrics.py
    # Only triggered manually or scheduled for November/December
```

**Or simply run manually:**
```bash
# SSH to server in November/December
cd /path/to/SAPPHIRE_forecast_tools
SAPPHIRE_PREDICTION_MODE=BOTH python apps/postprocessing_forecasts/recalculate_skill_metrics.py
```

---

## Performance Bottlenecks Identified

### Critical Bottlenecks

#### 1. Triple GroupBy-Apply for Skill Metrics
**Location:** `forecast_library.py` Lines 1909-1957 (pentad), 2214-2264 (decad)

**Problem:** Three separate groupby-apply operations on the same data:
```python
# First groupby for sdivsigma_nse
skill_stats = skill_metrics_df.groupby([...]).apply(sdivsigma_nse, ...)
# Second groupby for MAE
mae_stats = skill_metrics_df.groupby([...]).apply(mae, ...)
# Third groupby for accuracy
accuracy_stats = skill_metrics_df.groupby([...]).apply(forecast_accuracy_hydromet, ...)
# Then two merges to combine
```

**Impact:** Same groupby done 3 times. For 73 pentads × 50 stations × 4 models = 14,600 groups.

**Optimization:** Combine all three metric calculations into a single groupby-apply.

#### 2. Processes ALL Historical Data Every Run
**Location:** `setup_library.py` Lines 2691-2859 (pentad), 2861-3014 (decad)

**Problem:** Reads and processes ALL data from 2010-present on every run:
```python
observed = read_observed_pentadal_data()  # ALL historical data
linreg = read_linreg_forecasts_pentad()   # ALL historical forecasts
# ... plus 4 ML models + conceptual model
```

**Impact:** Unnecessarily loads and processes years of historical data for daily runs.

**Optimization:** Add date filtering to only load recent data for daily runs.

#### 3. Concatenation Inside Loops
**Location:** `setup_library.py` Lines 1961-2009 (conceptual model reading)

**Problem:**
```python
for file in files:
    forecast = read_conceptual_model_forecast_pentad(file)
    forecasts = pd.concat([forecasts, forecast])  # O(N²) complexity!
```

**Impact:** Each concat creates a new DataFrame. For N files, this is O(N²).

**Optimization:** Collect all DataFrames in a list, then concat once at the end.

#### 4. Nested Loops in Virtual Station Calculation
**Location:** `setup_library.py` Lines 2490-2585

**Problem:** Triple-nested loops with DataFrame operations inside:
```python
for station in virtual_stations:
    for contributing_station, weight in weights.items():
        data = data_df[data_df['code'] == contributing_station]  # Filter in loop
        for model in models:
            data_df = pd.concat([data_df, data_virtual_station])  # Concat in innermost loop!
```

**Impact:** For 10 virtual stations with 5 contributors and 4 models = 200 concat operations.

#### 5. Multiple `.isin()` Filters for Ensemble
**Location:** `forecast_library.py` Lines 1968-1972

**Problem:**
```python
skill_metrics_df_ensemble = skill_metrics_df[
    skill_metrics_df['pentad_in_year'].isin(skill_stats_ensemble['pentad_in_year']) &
    skill_metrics_df['code'].isin(skill_stats_ensemble['code']) &
    skill_metrics_df['model_long'].isin(skill_stats_ensemble['model_long']) &
    skill_metrics_df['model_short'].isin(skill_stats_ensemble['model_short'])
]
```

**Impact:** Four separate `.isin()` operations scanning the full DataFrame.

**Optimization:** Use merge instead of multiple `.isin()` calls.

### Performance Bottleneck Summary

| Bottleneck | Location | Severity | Type |
|-----------|----------|----------|------|
| Triple groupby-apply | forecast_library.py:1909-1957 | High | Redundant computation |
| All historical data loaded | setup_library.py:2691+ | High | Unnecessary I/O |
| Concat in loops | setup_library.py:1961-2009 | High | O(N²) complexity |
| Nested loops with concat | setup_library.py:2490-2585 | Critical | O(N³) complexity |
| Multiple .isin() filters | forecast_library.py:1968-1972 | Medium | Redundant scans |

### Existing Timing Infrastructure

**Good news:** Timing stats are already implemented in `postprocessing_forecasts.py` (Lines 38-85):
```python
class TimingStats:
    def start(self, section): ...
    def end(self, section): ...
    def summary(self): ...
```

Sections tracked:
- `'reading pentadal data'`
- `'calculating skill metrics pentads'`
- `'saving pentad results'`
- (Same for decadal)

---

## Skill Metrics and Ensemble Creation

### Skill Metrics Structure

**Important:** Skill metrics are calculated **per model, per station, per pentad/decad of the year** - NOT a single value per model for the entire year.

**Grouping Keys:**
- Pentadal: `['pentad_in_year', 'code', 'model_long', 'model_short']`
- Decadal: `['decad_in_year', 'code', 'model_long', 'model_short']`

**Example:** For pentadal forecasts with 3 models and 50 stations:
- 72 pentads × 50 stations × 3 models = **10,800 skill metric records**

Each record contains:

| Metric | Function | Description | Threshold |
|--------|----------|-------------|-----------|
| sdivsigma | `sdivsigma_nse()` | RMSE / StdDev of observations | < 0.6 (lower is better) |
| NSE | `sdivsigma_nse()` | Nash-Sutcliffe Efficiency | > 0.8 (higher is better) |
| MAE | `mae()` | Mean Absolute Error | (no threshold) |
| Accuracy | `forecast_accuracy_hydromet()` | Fraction within ±delta | > 0.8 (higher is better) |

### Why Different Skills per Pentad/Decad?

Models may perform differently depending on the time of year:
- **Snowmelt periods:** Some models capture spring dynamics better
- **Low-flow periods:** Different models may excel during baseflow
- **Monsoon/wet seasons:** Model performance varies with precipitation patterns

**Example:**
```
Station 15102, Model TFT:
  Pentad 15 (mid-March): sdivsigma=0.45, accuracy=0.85, nse=0.82 → INCLUDED in ensemble
  Pentad 45 (early August): sdivsigma=0.72, accuracy=0.65, nse=0.55 → EXCLUDED from ensemble
```

### Ensemble Creation Logic

**Location:** `forecast_library.py` Lines 1961-2042 (pentad), 2266-2345 (decad)

**Step-by-step process:**

1. **Filter by pentad/decad-specific skill:** For each (pentad_in_year, code) combination, select only models where:
   - `sdivsigma < 0.6` (for that specific pentad/decad)
   - `accuracy > 0.8` (for that specific pentad/decad)
   - `nse > 0.8` (for that specific pentad/decad)

2. **Exclude Neural Ensemble (NE)** from constituent models

3. **Calculate ensemble mean:** Simple arithmetic mean of `forecasted_discharge` from qualifying models

4. **Result:** Different models may be included in the ensemble for different pentads/decads

**Example Ensemble Composition:**
```
Station 15102:
  Pentad 15: Ensemble = mean(TFT, TIDE, LR)     # 3 models qualified
  Pentad 45: Ensemble = mean(LR)                # Only LR qualified (essentially no ensemble)
  Pentad 60: Ensemble = mean(TFT, TIDE, TSMixer, LR)  # All 4 models qualified
```

**Threshold Environment Variables:**
```
ieasyhydroforecast_efficiency_threshold=0.6   # sdivsigma
ieasyhydroforecast_accuracy_threshold=0.8     # accuracy
ieasyhydroforecast_nse_threshold=0.8          # NSE
```

### Skill Metrics Update Schedule

**Recommended:** Recalculate skill metrics **once per year in November or December**

**Rationale:**
- By November/December, you have a full year of forecast-observation pairs
- New skill metrics will be ready for the upcoming year
- Models don't change frequently, so skill is relatively stable

**Workflow:**
```
November/December (yearly maintenance):
  1. Run --maintenance to recalculate ALL skill metrics
  2. New skill_metrics_pentad.csv and skill_metrics_decad.csv are generated
  3. These files are used for ensemble selection throughout the next year

Daily operations:
  1. Generate new forecasts
  2. Read existing skill metrics from CSV files
  3. Use pentad/decad-specific skill to select models for ensemble
  4. Save forecast data (including ensemble)
```

### Dependency: Skill Metrics → Ensemble

```
Skill metrics (pre-calculated, per pentad/decad/station/model)
        ↓
For each new forecast date:
  - Determine pentad_in_year (e.g., pentad 15)
  - Look up skill metrics for that specific pentad
  - Filter models passing thresholds for THAT pentad
        ↓
Calculate ensemble mean from qualifying models
        ↓
Save forecast (individual models + ensemble)
```

**Key Point:** The ensemble composition can vary by pentad/decad because different models qualify based on their pentad/decad-specific skill metrics.

---

## Refactoring Recommendations

### Phase 0: Separate Daily vs Maintenance Tasks (Priority: HIGH)

**Approach:** Create a separate maintenance script (recommended over --maintenance argument)

1. **Create `recalculate_skill_metrics.py`** for yearly maintenance:
   - Full skill metrics recalculation from 2010-present
   - Updates `skill_metrics_pentad.csv` and `skill_metrics_decad.csv`
   - Run manually in November/December each year

2. **Simplify `postprocessing_forecasts.py`** for daily operations:
   - Only save new forecast data
   - Read existing skill metrics from CSV files
   - Create ensemble using pre-calculated pentad/decad-specific skill metrics
   - Fast execution

3. **Extract shared code** into `src/skill_metrics.py`:
   - Skill metrics calculation functions
   - Ensemble creation logic
   - Minimizes code duplication between the two scripts

### Phase 6: Performance Optimization

1. **Combine triple groupby into single operation**
2. **Add date range filtering for daily runs**
3. **Fix concat-in-loop patterns**
4. **Replace multiple .isin() with merge**
5. **Optimize virtual station calculation**

### Configuration Bug Fix

**Issue:** `ieasyforecast_decadal_skill_metrics_file` is not defined in `.env`

**Fix:** Add to `.env`:
```
ieasyforecast_decadal_skill_metrics_file=skill_metrics_decad.csv
```
