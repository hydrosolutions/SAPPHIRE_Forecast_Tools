# Post-processing of Hydrological Forecasts

This module is the final stage of the SAPPHIRE forecast pipeline. It takes raw
model outputs, creates ensemble forecasts, and calculates skill metrics for
quality assessment.

**I/O transition:** The module is transitioning from CSV-based I/O to
API-first I/O via the SAPPHIRE postprocessing API. The target state is:
- **Reads:** API as primary source, CSV as deprecated fallback only.
- **Writes:** API as primary destination, CSV as deprecated backup only.

CSV I/O will be removed once API integration is fully validated.
PP-007, PP-009, PP-010, PP-011, PP-013, and PP-014 are in review.

## Forecast Horizons

| Horizon | Period | Boundary days | Status |
|---------|--------|---------------|--------|
| **Pentadal** | 5-day | 5th, 10th, 15th, 20th, 25th, last of month | Implemented |
| **Decadal** | 10-day | 10th, 20th, last of month | Implemented |
| **Monthly** | 1-month | 1st of month (long-term forecasts) | Implemented |
| **Daily** | 1-day | Every day for the next 10 days (skill metrics + ensembles, see PP-012) | Implemented |
| **Quarterly** | 3-month | 1st of quarter (long-term forecasts) | Planned (PP-017) |
| **Seasonal** | Multi-month | Configurable start/end months (long-term forecasts) | Planned (PP-018) |

**Quarterly and seasonal horizons:** The API schema (`HorizonType.QUARTER`,
`HorizonType.SEASON`) is ready. The `long_forecasts` table already contains
monthly records from 9 models that can be aggregated into quarterly and
seasonal values by this module. Postprocessing support (aggregation,
ensembles, skill metrics, operational/maintenance) is not yet implemented --
see PP-017 and PP-018.

## Supported Models

Individual forecast models (created upstream):

- **LR** - Linear Regression (statistical baseline)
- **TFT** - Temporal Fusion Transformer
- **TiDE** - Temporal Information Decomposition Encoder
- **TSMixer** - Time Series Mixer

Ensemble models (created by this module):

- **NE** - Neural Ensemble (fixed composition: TFT + TiDE + TSMixer, created
  upstream in `setup_library`) - for short-term forecasts only, not included in EM candidates
- **EM** - Ensemble Mean (variable composition, only models passing skill
  thresholds) - for all forecast horizons
- **Skilled Mean** - Weighted by 1/MAE (monthly only) - for long-term forecasts only
- **Naive Mean** - Unweighted average of all models (monthly only) - for long-term forecasts only

## Pipeline Overview

The module has three operational modes, each with a different trigger and scope.

### Relationship between modes

```
Recalculation (yearly, on-demand)
    |
    +-- produces skill metrics (CSV + API)
    |
    v
Operational (daily, boundary days)     Maintenance (nightly)
    |                                      |
    +-- reads skill metrics                +-- reads skill metrics
    +-- creates ensembles for today        +-- detects gaps in lookback window
    +-- writes forecasts                   +-- creates ensembles for gap dates
                                           +-- merges + writes forecasts
```

The recalculation run is the prerequisite -- without fresh skill metrics,
operational and maintenance have no basis for filtering ensemble members.

### 1. Operational -- daily on boundary days

**Entry points:**
- `postprocessing_operational.py` (pentad/decad)
- `postprocessing_operational_long_term.py` (monthly)

**When:** Only on boundary days for the configured horizon. Skips entirely
otherwise.

**Pipeline stages:**

1. **Boundary check** -- Is today a pentad/decad boundary day?
2. **Read current forecasts** -- Today's observed + modelled data.
   - **Target state:** Read from API as the primary source.
   - **Current state (pentad/decad):** Read from API via
     `data_reader.read_observed_and_modelled_data()`, with CSV fallback.
   - **Current state (monthly):** Read from API. Correct.
3. **Read pre-calculated skill metrics** -- via `data_reader.read_skill_metrics`.
   - API is the primary source, CSV is a deprecated fallback only.
4. **Create ensemble forecasts** -- Separately for each horizon:
   - Filter skill metrics for highly skilled models (sdivsigma < 0.6,
     NSE > 0.8, accuracy > 0.8)
   - Merge with forecasts, exclude NE, require >= 2 qualifying models
   - Group by (date, code), compute mean discharge -> EM rows
   - Calculate skill metrics for the new EM rows
5. **Save** -- Write to API (`api_writer`, upsert) as primary + CSV
   (`file_writer`) as deprecated backup. CSV writes will be removed once
   API integration is fully validated.
6. **Log** -- Print most recent forecasts

**Why EM skill metrics are calculated on the fly:** The EM composition changes
per (date, code) pair. On one date, station 15001's EM might be "LR, TFT"; on
another it might be "LR, TFT, TiDE". Since the composition varies, last year's
EM skill metric (calculated for whatever compositions appeared last year) does
not directly apply. See open issue PP-009 below.

**NE handling:** NE (Neural Ensemble) is created upstream in `setup_library`
with a fixed composition (TFT + TiDE + TSMixer). NE skill metrics come from the
annual recalculation. NE is explicitly excluded from EM candidates to avoid
"ensemble of ensembles."

> **TODO (PP-015):** Many ensemble-related methods currently live in
> `setup_library` (in `iEasyHydroForecast`), including NE creation. These
> should be moved into the `postprocessing_forecasts` module where they
> logically belong. Additionally, verify whether NE calculation currently
> happens anywhere in `postprocessing_forecasts` or exclusively in
> `setup_library` -- if only in `setup_library`, the migration is
> straightforward but must be planned.

**Runtime:** Seconds.

### 2. Maintenance -- nightly gap-fill

**Entry points:**
- `postprocessing_maintenance.py` (pentad/decad)
- `postprocessing_maintenance_long_term.py` (monthly)

**When:** Every night, no time gate.

**Pipeline stages:**

1. **Read existing combined forecasts** -- API-first with CSV fallback
   (`data_reader.read_combined_forecasts`).
2. **Detect gaps** -- Find (date, code) pairs missing EM rows within lookback
   window (`POSTPROCESSING_GAPFILL_WINDOW_DAYS`, default 7 days, configurable
   up to e.g. 30 days for longer server outages). See PP-006 for moving this
   to `config.yaml`.
3. **Read data for gap dates** -- Observed + modelled + pre-calculated skill
   metrics
4. **Create ensembles** for gap dates only:
   - **Short-term (pentad/decad):** EM and NE
   - **Long-term (monthly):** EM, Naive Mean, and Skilled Mean
5. **Merge** -- `pd.concat` new EM rows with full existing data, deduplicate on
   (date, code, model_short) with `keep='last'`
6. **Save** -- Write to API (upsert) as primary + rewrite CSV as deprecated
   backup.

**Write behavior:**
- API: Upsert on unique key `(horizon_type, code, model_type, date, target)`.
  Only affected rows are updated; no full-table replace.
- CSV (deprecated): Entire file is rewritten (concat + dedup + save).
  Historical rows are preserved through the merge.

**Runtime:** Minutes.

### 3. Recalculation -- yearly / on-demand

**Entry point:** `recalculate_skill_metrics.py`

**When:** Manually triggered, typically end-of-year or after model changes.

**Purpose:** Re-create all ensembles from scratch and calculate skill metrics
for all individual models and ensembles over the full historical range. This is
also the bootstrap path for new sites — when a site is added to the pipeline,
running this script produces its initial ensembles and skill metrics. See
PP-016.

**Pipeline stages (pentad/decad):**

1. **Read ALL historical data** -- Observations + individual model forecasts
   across the full date range
2. **Calculate individual model skill metrics** --
   `skill_metrics.calculate_skill_metrics_pentad/decade()`:
   - Merge observed & simulated on (code, date)
   - Group by (period_in_year, code, model_short)
   - Calculate all metrics per group: sdivsigma, NSE, MAE, accuracy, delta,
     n_pairs, PBIAS, KGE-LF, NSE-log
3. **Create all ensembles from scratch** -- Filter models by skill thresholds,
   create EM (and Skilled Mean, Naive Mean for monthly). This re-derives
   ensembles even if they already exist from operational runs.
4. **Calculate ensemble skill metrics** -- Same metrics as step 2, applied to
   the newly created ensemble rows.
5. **Save forecasts** (individual + ensemble) to API (upsert) + CSV
   (deprecated backup)
6. **Save skill metrics** to API (upsert) + CSV (deprecated backup)

**Pipeline stages (monthly):**

1. **Read all daily runoff from API** -> aggregate to monthly observations
2. **Read all monthly long-term forecasts from API**
3. **Calculate skill metrics** for individual models
4. **Create ensembles** (EM, Skilled Mean, Naive Mean) + calculate their
   skill metrics (including CRPS for probabilistic evaluation)
5. **Save** to API + CSV (deprecated backup)

**Pipeline stages (daily):**

1. Read daily observations + forecasts from API
2. Calculate all metrics: Tier 1 (sdivsigma, NSE, MAE, accuracy, delta,
   n_pairs) + Tier 2 (PBIAS, KGE-LF, NSE-log, FDC-FHV, FDC-FLV)
3. Save daily skill metrics to API + CSV (deprecated backup)

**Runtime:** Hours.

**Note:** `postprocessing_forecasts.py` is **deprecated** -- it duplicates the
recalculation logic. Use `recalculate_skill_metrics.py` instead.

## Data Sources by Horizon and Mode

| Horizon | Operational reads | Recalculation reads | Status |
|---------|------------------|--------------------|-|
| Pentad/decad forecasts | API-first, CSV-fallback | API-first, CSV-fallback | Correct (PP-010 resolved) |
| Pentad/decad skill metrics | API-first, CSV-fallback | N/A (produces them) | Correct |
| Monthly | API (with CSV fallback) | API (with CSV fallback) | Correct |
| Daily | API | API | Correct |

| Horizon | Writes | Status |
|---------|--------|--------|
| All | API (upsert) + CSV (atomic write) | CSV to be deprecated after validation |

All read paths now use the API as primary source with CSV as deprecated
fallback. NE and virtual station calculations remain in `setup_library`
but these compute derived values, not raw data reads.

## Skill Metrics

### Metric tiers

| Tier | Metrics | Purpose |
|------|---------|---------|
| **Tier 1 (threshold)** | sdivsigma, NSE, accuracy | Used to filter models for EM inclusion |
| **Tier 1 (always)** | MAE, n_pairs, delta | Calculated for all models |
| **Tier 2 (informational)** | PBIAS, KGE-LF, NSE-log | Additional quality indicators |

### Ensemble filtering thresholds

A model qualifies for EM inclusion only if **all three** threshold metrics pass:

| Metric | Threshold | Env var |
|--------|-----------|---------|
| sdivsigma | < 0.6 | `ieasyhydroforecast_efficiency_threshold` |
| NSE | > 0.8 | `ieasyhydroforecast_nse_threshold` |
| accuracy | > 0.8 | `ieasyhydroforecast_accuracy_threshold` |

Setting a threshold to `'False'` disables that filter.

### Skill metrics versioning

Skill metrics CSVs are saved with a year tag (e.g.,
`skill_metrics_pentad_2025.csv`). In the API, each skill metric record has a
`date` attribute that stores the **forecast target date** (not a year) and the
unique key is `(horizon_type, code, model_type, date, horizon_in_year)`.
This means skill metrics are naturally versioned per forecast date --
recalculations for different periods do not overwrite each other (PP-011
resolved).

## Source Modules

```
postprocessing_forecasts/
|-- postprocessing_operational.py          Entry point: daily operational
|-- postprocessing_maintenance.py          Entry point: nightly gap-fill
|-- recalculate_skill_metrics.py           Entry point: yearly recalculation
|-- postprocessing_operational_long_term.py Entry point: monthly operational
|-- postprocessing_maintenance_long_term.py Entry point: monthly gap-fill
|-- postprocessing_forecasts.py            DEPRECATED
|-- src/
|   |-- data_reader.py         Read skill metrics + monthly data (CSV/API)
|   |-- ensemble_calculator.py Create ensemble forecasts (EM, Skilled/Naive Mean)
|   |-- skill_metrics.py       Calculate all skill metrics
|   |-- api_writer.py          Write forecasts + metrics to SAPPHIRE API
|   |-- file_writer.py         Write forecasts + metrics to CSV
|   |-- gap_detector.py        Detect missing ensemble rows for maintenance
|   +-- postprocessing_tools.py Timing, logging, utilities
+-- tests/
```

## Configuration

### Sensitive variables (`.env` file)

These contain deployment-specific paths, URLs, or credentials and must stay in
`.env` (never committed to git).

| Variable | Description |
|----------|-------------|
| `ieasyhydroforecast_env_file_path` | Path to environment configuration file |
| `ieasyforecast_intermediate_data_path` | Root directory for CSV data storage |
| `SAPPHIRE_API_ENABLED` | Enable/disable API writes (default: `true`) |
| `SAPPHIRE_API_URL` | API base URL (default: `http://localhost:8000`) |

### Module configuration (candidates for `config.yaml`)

These are non-sensitive tuning parameters for the postprocessing module. They
currently live in environment variables but should be moved to a `config.yaml`
file inside `postprocessing_forecasts/`. See PP-006.

#### Prediction mode

`SAPPHIRE_PREDICTION_MODE` controls which forecast horizons to process. The
valid values depend on the entry point:

| Value | What it processes | Used by |
|-------|-------------------|---------|
| `PENTAD` | Pentadal forecasts only | Operational, maintenance, recalculation |
| `DECAD` | Decadal forecasts only | Operational, maintenance, recalculation |
| `BOTH` | Pentad + decad (default) | Operational, maintenance, recalculation |
| `MONTHLY` | Monthly long-term forecasts only | Recalculation only (operational/maintenance for monthly have separate entry points) |
| `DAILY` | Daily skill metrics only | Recalculation only (no daily operational/maintenance entry point yet) |
| `QUARTERLY` | Quarterly long-term forecasts only | Planned (PP-017) |
| `SEASONAL` | Seasonal long-term forecasts only | Planned (PP-018) |
| `ALL` | All of the above | Recalculation (runs all horizons in one invocation) |

**Typical usage:** Short-term operational and maintenance runs use `BOTH`
(default). `MONTHLY` and `DAILY` are only relevant for the recalculation
entry point, which is the only script that handles all four horizons. The
monthly and daily operational/maintenance modes have their own dedicated entry
points (`postprocessing_operational_long_term.py`,
`postprocessing_maintenance_long_term.py`) that do not read this variable.

#### Recalculation parameters

| Variable | Description | Default |
|----------|-------------|---------|
| `SAPPHIRE_SKILL_METRICS_YEAR` | Year tag used when saving skill metrics CSV files (e.g., `skill_metrics_pentad_2025.csv`). Also used to compute the forecast target date for API writes. | Current year |
| `SAPPHIRE_SKILL_METRICS_START_YEAR` | Earliest year of historical data to include in pentad/decad skill metric calculation. Controls how many years of observed vs. modelled pairs are considered. | 20 years before current year |
| `SAPPHIRE_RECALC_START_YEAR` | First year of the recalculation range for monthly and daily horizons. Monthly recalculation reads all daily runoff from this year onward. | (required for monthly/daily) |
| `SAPPHIRE_RECALC_END_YEAR` | Last year of the recalculation range for monthly and daily horizons. | (required for monthly/daily) |

#### Maintenance parameters

| Variable | Description | Default |
|----------|-------------|---------|
| `POSTPROCESSING_GAPFILL_WINDOW_DAYS` | Lookback window (in days) for gap detection. Set to a larger value (e.g., 30) after prolonged server outages to backfill missed forecasts. | 7 |

#### Ensemble thresholds

| Variable | Description | Default |
|----------|-------------|---------|
| `ieasyhydroforecast_efficiency_threshold` | sdivsigma threshold for EM inclusion | 0.6 |
| `ieasyhydroforecast_nse_threshold` | NSE threshold for EM inclusion | 0.8 |
| `ieasyhydroforecast_accuracy_threshold` | Accuracy threshold for EM inclusion | 0.8 |

Setting any threshold to `'False'` disables that filter.

### Output file paths (candidates for `config.yaml`)

These define CSV output filenames and are non-sensitive. Currently environment
variables; candidates for `config.yaml`.

| Variable | Description |
|----------|-------------|
| `ieasyforecast_pentadal_combined_forecast_file` | Pentadal forecast CSV |
| `ieasyforecast_decadal_combined_forecast_file` | Decadal forecast CSV |
| `ieasyforecast_pentadal_skill_metrics_file` | Pentadal skill metrics CSV |
| `ieasyforecast_decadal_skill_metrics_file` | Decadal skill metrics CSV |
| `ieasyforecast_monthly_combined_forecast_file` | Monthly forecast CSV |
| `ieasyforecast_monthly_skill_metrics_file` | Monthly skill metrics CSV |

## Testing

Run tests via the project test runner from the `apps/` directory:

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

Or directly:

```bash
cd apps
SAPPHIRE_TEST_ENV=True pytest postprocessing_forecasts/tests/ -v
```

## Open Issues

Issues identified during pipeline review (2026-02-26). These need detailed
planning before implementation.

### PP-006: Move non-sensitive config from env vars to config.yaml

**Status:** Draft plan in `doc/plans/issues/gi_draft_pp_config_yaml.md`.

**Current:** Module tuning parameters (`POSTPROCESSING_GAPFILL_WINDOW_DAYS`,
`SAPPHIRE_PREDICTION_MODE`, recalculation year ranges, ensemble thresholds,
output file paths) are environment variables, mixed in with sensitive
deployment variables.

**Target:** Create a `postprocessing_forecasts/config.yaml` for non-sensitive
module configuration. Keep only deployment-specific variables (API URL, API
enabled, env file path, data root path) in `.env`.

**Benefits:** Easier to adjust parameters (e.g., increasing gapfill window to
30 days after a server outage) without touching `.env`. Config can be version-
controlled.

**Affects:** All entry points, `gap_detector.py`, `ensemble_calculator.py`,
`file_writer.py`

### PP-007: Maintenance should read from API, not CSV — **Review**

**Resolved:** `data_reader.read_combined_forecasts()` now reads from the
postprocessing API (via `read_short_term_forecasts`) with CSV fallback.
`gap_detector.read_combined_forecasts()` is deprecated and delegates to
`data_reader`. `postprocessing_maintenance.py` calls `data_reader` directly.

**Affects:** `data_reader.py`, `gap_detector.py`, `postprocessing_maintenance.py`

**Related:** PP-013 is the monthly-specific variant of this issue.

### PP-008: No audit trail for gap-filled rows

**Current:** Maintenance upserts gap-filled rows silently. There is no way to
distinguish a forecast that was available in real-time from one reconstructed
later by the maintenance pipeline.

**Target:** Add a flag (`is_backfilled: bool` or `backfilled_at: timestamp`) to
the API schema so consumers can distinguish operational from maintenance data.

**Affects:** API schema in `sapphire/services/postprocessing/` (colleague's
domain). Coordinate before implementing.

### PP-009: Stop calculating EM skill metrics on the fly in operational mode — **Review**

**Resolved:** On-the-fly EM skill metric calculation has been removed from
operational mode. The ensemble calculator now creates EM forecast rows without
calculating skill metrics for them. EM skill metrics come from the annual
recalculation instead.

**Affects:** `ensemble_calculator.create_ensemble_forecasts()`,
`postprocessing_operational.py`

### PP-010: Pentad/decad reads should use API (operational + recalculation) -- **Review**

**Resolved:** Pentad/decad reads migrated to API-first via
`data_reader.read_observed_and_modelled_data()`. All three entry points
(operational, recalculation, maintenance) now use the new readers.
NE and virtual station calculations remain in `setup_library`, called
explicitly from entry points.

### PP-011: Skill metrics API unique key should include date -- **Review**

**Resolved:** The API schema uses `UniqueConstraint("horizon_type", "code",
"model_type", "date", "horizon_in_year")` and the CRUD upsert matches.
Client-side (`api_writer._write_skill_metrics_to_api`) computes a per-row
`date` from `horizon_in_year` + target `year` via `tl.get_date_for_pentad()`,
`tl.get_date_for_decad()`, or `date(year, month, 1)`. Different
recalculation years produce different `date` values, so skill metrics are
naturally versioned and do not overwrite across periods.

### PP-012: Daily ensemble creation

**Current:** Daily skill metrics exist (Tier 2: FDC, thresholds) but there are
no daily ensemble forecasts (no EM/Skilled Mean/Naive Mean for daily horizon).

**Decision:** Yes, implement daily ensembles. The ensemble machinery is already
parameterized and can be extended to the daily horizon. The forecast dashboard
consumes these.

**Affects:** `ensemble_calculator.py`, `recalculate_skill_metrics.py`,
potentially new `postprocessing_operational_daily.py`

### PP-013: Monthly maintenance uses CSV-first gap detection — **Review**

**Resolved:** `data_reader.read_monthly_combined_forecasts()` now reads from
the postprocessing API (via `read_long_term_forecasts`) with CSV fallback.
Normalization adds `month_in_year` and `forecasted_discharge` (from `q50`)
for compatibility with the gap detector and merge-back logic.

**Affects:** `data_reader.py`

### PP-015: Move NE creation from setup_library to postprocessing_forecasts

**Current:** NE (Neural Ensemble) creation and many ensemble-related methods
live in `setup_library` (`iEasyHydroForecast`), not in `postprocessing_forecasts`
where they logically belong. This module only receives NE rows and excludes them
from EM candidates.

**Target:** Move NE creation and related ensemble logic into
`postprocessing_forecasts`. First step: audit `setup_library` to identify all
NE-related code and confirm that `postprocessing_forecasts` does not currently
calculate NE at all.

**Affects:** `iEasyHydroForecast/setup_library.py`, `postprocessing_forecasts/src/ensemble_calculator.py`

### PP-016: Recalculation is the bootstrap path for new sites

**Current behavior (keep as-is):** `recalculate_skill_metrics.py` creates all
ensembles (EM, Skilled Mean, Naive Mean) from scratch and calculates skill
metrics for both individual models and ensembles in one pass. For existing
sites this re-derives ensembles that already exist from operational runs; for
new sites it is the only way to produce initial ensembles and skill metrics.

**Decision:** Keep the current behavior. The annual recalculation always
re-creates all ensembles from scratch for all sites, including any newly added
ones.

**Automatic new-site detection:** Rather than requiring the operator to
manually trigger recalculation when a new site is added, the maintenance
pipeline should detect new sites automatically:

1. Maintenance already reads existing forecasts and skill metrics nightly.
2. Compare site codes: `sites_in_forecasts - sites_in_skill_metrics` =
   new sites.
3. If new sites are detected:
   - Log: "New sites detected: {new_sites}, triggering recalculation"
   - Trigger `recalculate_skill_metrics.py` as a **separate background
     process** (not inline — recalculation takes hours and must not block
     the nightly gap-fill).
   - Maintenance continues with its normal gap-fill for existing sites.
4. On the next nightly run, the new sites will have skill metrics and
   can participate in normal gap-fill.

This approach is self-contained (no cross-module coupling with
`preprocessing_runoff`), catches new sites regardless of how they were added,
and reuses infrastructure that maintenance already has (forecast + skill
metric reads).

**Affects:** `postprocessing_maintenance.py`,
`postprocessing_maintenance_long_term.py` (new-site detection logic),
`recalculate_skill_metrics.py` (must support being invoked as a background
process)

### PP-017: Quarterly forecast postprocessing

**Status:** Ready to plan. Detailed checklist in
`doc/plans/postprocessing_unified_plan.md` (Phase 4b).

**Current:** The API schema supports `HorizonType.QUARTER` and the
`long_forecasts` table contains ~18k direct quarterly records (90-91 day
span, rolling 3-month windows) from `LR_BASE`/`LR_SM`. However, only 2 of 9
models produce these direct records. `postprocessing_forecasts` has no
quarterly support.

**Approach:** Aggregate from single-month forecasts (all 9 models) rather
than consuming the direct quarterly records (2 models only). This ensures
full ensemble candidate coverage:
1. Read monthly forecasts from API (already implemented)
2. Average quantiles (Q5-Q95) across 3-month quarters
3. Create ensembles (EM, Skilled Mean, Naive Mean)
4. Calculate skill metrics (point + CRPS) against quarterly observations
5. Write with `horizon_type='quarter'` to API + CSV

**Entry points:** Extend existing long-term entry points
(`postprocessing_operational_long_term.py`,
`postprocessing_maintenance_long_term.py`, `recalculate_skill_metrics.py`)
to process quarterly alongside monthly, rather than creating new scripts.

**Affects:** `data_reader.py`, `ensemble_calculator.py`, `skill_metrics.py`,
`api_writer.py`, `file_writer.py`, `gap_detector.py`,
`recalculate_skill_metrics.py`, `postprocessing_operational_long_term.py`,
`postprocessing_maintenance_long_term.py`

### PP-018: Seasonal forecast postprocessing

**Status:** Ready to plan. Detailed checklist in
`doc/plans/postprocessing_unified_plan.md` (Phase 4b).

**Current:** The API schema supports `HorizonType.SEASON` and the
`long_forecasts` table contains ~11k direct seasonal records (182-day span,
Apr 1 - Sep 30) from `LR_BASE`/`LR_SM`. However, only 2 of 9 models
produce these direct records. `postprocessing_forecasts` has no seasonal
support.

**Approach:** Same as quarterly — aggregate from single-month forecasts:
1. Read monthly forecasts from API
2. Average quantiles across configurable season months (default Apr-Sep)
3. Create ensembles (EM, Skilled Mean, Naive Mean)
4. Calculate skill metrics (point + CRPS) against seasonal observations
5. Write with `horizon_type='season'` to API + CSV

**Season configuration:** Start/end months via env vars
(`SAPPHIRE_SEASON_START_MONTH`, `SAPPHIRE_SEASON_END_MONTH`) with Central
Asia defaults (4=April, 9=September). Will migrate to `config.yaml` with
PP-006.

**Entry points:** Same as PP-017 — extend existing long-term entry points.

**Affects:** Same files as PP-017, plus seasonal config handling.
