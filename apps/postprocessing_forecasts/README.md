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
- **EM** -- Ensemble Mean (variable composition). **Short-term (pentad/decad)
  only**: equal-weight mean of the Skilled-Mean pool (NSE>0 and n_pairs≥4, own
  env vars). Long-term (monthly, quarterly, seasonal) produces no EM (removed
  in PP-072 P3; quarterly EM was already removed in PP-065 P1c). Old monthly/seasonal EM forecast and
  skill rows stay in the database.
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
4. Create ensemble forecasts (short-term: NSE>0 and n_pairs≥4 filter, minimum 2 point
   models with at least one non-LR band contributor; long-term horizons: no EM, Naive Mean + Skilled Mean only;
   compute mean discharge)
5. Write to API (upsert) + CSV (deprecated backup)

**Runtime:** Seconds.

### 2. Maintenance

**Entry points:**
- `postprocessing_maintenance.py` (pentad/decad)
- `postprocessing_maintenance_long_term.py` (monthly + quarterly + seasonal)

**When:** Every night.

**Steps (PP-021 efficient flow):**
1. Read existing combined forecasts from API (cheap)
2. Detect (date, code) pairs missing ensemble rows (short-term: EM/NE; long-term: Naive Mean/Skilled Mean) within lookback window (cheap, in-memory)
3. Detect records with NULL quantiles (stale individual-model/NE rows and stale short-term EM rows)
4. **Early exit if no gaps and no stale records** — completes in <30 seconds
5. Read individual-model forecasts scoped to affected dates only (not full history)
6. Create NE + EM rows (short-term) or Naive Mean + Skilled Mean rows (long-term) for affected dates
7. Merge new rows with existing data, deduplicate
8. Write to API (upsert) + CSV (deprecated backup)

**Runtime:** Seconds (no-gap nights) to minutes (gap/stale-record nights).

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
4. Create ensembles (Skilled Mean, Naive Mean; no long-term EM)
5. Save to API + CSV

**Steps (daily):**
1. Read daily observations + forecasts from API
2. Calculate Tier 1 + Tier 2 metrics
3. Save daily skill metrics to API + CSV

**Runtime:** Hours.

**Note:** `postprocessing_forecasts.py` is deprecated. Use
`recalculate_skill_metrics.py` instead.

## Recovering Stranded Period Forecasts

If short-term per-model PENTAD/DECADE rows are missing for a boundary date, the
recovery tool is `backfill_period_forecasts.py`. It re-aggregates a range through the
existing operational aggregation and save path, one calendar year per internal call.

**Check first whether it applies.** The CLI re-aggregates inputs that already exist —
it does not generate forecasts. When the inputs for the affected dates were never
produced it cannot recreate them: if the rest of the year still has coverage the run
exits 0 having re-upserted everything *else* and nothing new for your dates, and if the
whole horizon-year is empty it exits 1. Neither outcome means "repaired". In the field
cases examined so far the cause was that the pipeline had not run, and the fix for that
is upstream in `machine_learning` — though how representative those cases are is
inferred, not measured.

Two further traps worth knowing before reading the full procedure: only the *years* of
`--start-date`/`--end-date` are used, so a narrow date range does not narrow the work;
and the short-term Ensemble Mean is recomputed against current skill metrics, so a historical
backfill is not a replay.

Full procedure, including the write-safety requirements:
[`doc/prod/backfill_period_forecasts_runbook.md`](../../doc/prod/backfill_period_forecasts_runbook.md).

## Skill Metrics

### Metric tiers

| Tier | Metrics | Purpose |
|------|---------|---------|
| **Tier 1 (threshold)** | sdivsigma, NSE, accuracy | Skill gate. The un-suffixed env vars are the strict gate of the backward-compat wrapper `ensemble_calculator.filter_for_highly_skilled_forecasts` and also serve as the fallback for the short-term EM gate (see below). Long-term has no EM |
| **Tier 1 (always)** | MAE, n_pairs, delta | Calculated for all models |
| **Tier 2 (informational)** | PBIAS, KGE-LF, NSE-log | Additional quality indicators |
| **Tier 2 (daily)** | FDC-FHV, FDC-FLV | Flow duration curve metrics |

### Shared filter settings (strict gate)

Long-term horizons no longer produce EM (PP-072 P3). The strict gate below uses
the un-suffixed env vars. Called without overrides (the backward-compat wrapper
`ensemble_calculator.filter_for_highly_skilled_forecasts`) a model passes only if
**all three** pass. The same variables also act as the **fallback** for the
short-term EM gate (next section):

| Metric | Default threshold (inclusive) | Env var |
|--------|-----------|---------|
| sdivsigma | ≤ 0.6 | `ieasyhydroforecast_efficiency_threshold` |
| NSE | ≥ 0.8 | `ieasyhydroforecast_nse_threshold` |
| accuracy | ≥ 0.8 | `ieasyhydroforecast_accuracy_threshold` |

Setting a threshold to `'False'` disables that filter.

### Short-term EM eligibility (pentad/decad)

Short-term EM has its own gate, independent of the long-term gate. For each of
the three metrics the threshold is resolved in this order: (1) the
`ieasyhydroforecast_*_threshold_short_term` variable if set (an empty string
counts as unset); (2) else the existing un-suffixed deployed variable
(`ieasyhydroforecast_nse_threshold`, `..._efficiency_threshold`,
`..._accuracy_threshold`); (3) else the default below. Per-variable, so a mix is
possible. `ieasyhydroforecast_min_pairs_short_term` has no fallback (default 4).
An NSE fallback that resolves to a disable token logs a warning and uses the
default (NSE > 0) instead of failing; an explicit `_short_term` NSE disable
token raises an error. Comparisons are inclusive (a fallback NSE of `0.0`
admits NSE == 0).

- **NSE filter**: finite NSE > 0 by default (NaN and Inf excluded; the NSE gate
  cannot be disabled). sdivsigma and accuracy gates are off by default but can
  be enabled via their `_short_term` variables.
- **Minimum pairs**: `n_pairs` ≥ 4 by default.
- NE and existing EM rows are always excluded from candidates.
- **Point value**: finite `forecasted_discharge` required for all candidates.
- **LR model**: contributes its point value plus a tolerance band
  q25 = point − delta, q75 = point + delta, where `delta` is the hydromet delta
  of the station and period, taken from the LR row of `skill_stats` only
  (latest skill `date` if several). PP-075 normalizes the reported delta
  across models as described below. LR has no q05/q95. LR is
  dropped from the EM entirely (point and bands) when delta is missing,
  non-finite or negative. Any quantile values supplied for LR are ignored.
- **Non-LR models**: must supply all four finite bands (q05, q25, q75, q95).
  Rows with any missing or non-finite band are dropped before averaging.
- **Minimum composition**: at least 2 distinct point models in the composition
  string (e.g. `"LR, TFT"`). Single-model groups are discarded.
- **Non-LR band contributor**: at least one non-LR model must supply band
  values; groups with only LR are dropped.
- **Aggregation**: unweighted (equal-weight) arithmetic mean of
  `forecasted_discharge` (point); q25/q75 are averaged over LR and non-LR
  members, q05/q95 over non-LR members only.
- **Monotonicity guard**: applied after averaging (corrects crossings).

Caveats: the skill used to select members (NSE, `n_pairs`) is computed on the
same history, so it is in-sample and optimistic. NSE > 0 only means "better
than the climatological mean", a weak filter at short lead; tighten it with
`ieasyhydroforecast_nse_threshold_short_term` (e.g. 0.8). The LR band is a
tolerance band, not a prediction interval.

Long-term EM is no longer produced (PP-072 P3): monthly and seasonal output
Naive Mean + Skilled Mean only, and quarterly EM was already removed in PP-065 P1c. Existing
monthly/seasonal EM forecast rows and EM skill rows remain in the database (skill
rows are exempt from stale tombstoning; quarter EM skill rows are still
tombstoned), so the dashboard may keep showing the last stored EM until it is
superseded. The legacy EM re-derivation inside `calculate_skill_metrics`
(`skill_metrics.py`) was removed (PP-072 P2); the function scores supplied
model rows, and `exclude_models` is accepted but has no effect.

**Example** (synthetic): LR issues point 10 m³/s with delta 2 (q25=8,
q75=12); TFT issues point 20 m³/s with bands q05=10, q25=15, q75=25,
q95=30. EM point = 15 m³/s, q25=11.5 and q75=18.5; q05=10 and q95=30
come from TFT alone. The point can lie outside the bands — no clamping is applied.

### EM hindcast skills and shared delta (PP-075)

**Release status:** implemented on the branch; module tests and independent
review passed. The repository-wide zero-skip gate remains unmet; deployment
is pending. See [PP-075 verification](../../doc/plans/issues/mid_prio_gi_draft_pp_short_term_em_skill_and_delta_ranges.md#verification).

Recalculation builds EM hindcasts from stored pentad/decad member forecasts
for the same station, horizon, target period and boundary issue date, using
the existing EM calculator and member gates above. This EM input path does
not use DAY forecasts; other readers retain their existing default behavior.
The generated EM rows are saved for all eligible stored boundary issues.
Member and EM scoring use the configured `SAPPHIRE_SKILL_METRICS_START_YEAR`
as a **target-year** boundary, retaining a 31 December issue for the first
period of the following year. This scoring-window rule does not narrow the
generated forecast history. EM is scored as one series per
station and calendar period, across changing compositions. A skill row's
composition label is the union of historical members, not a claim that every
forecast used that whole set.

EM skills require **four distinct target years** with finite matched forecast
and observation values. This reporting floor is separate from the configurable
member-admission floor. Duplicate rows do not add years, and mathematically
undefined metrics remain NaN. Recalculation replaces the affected current
reporting-year EM skill keys; insufficient or absent results overwrite old
values with unavailable/tombstone results through the existing write path.
It does not perform a general historical cleanup or remove old long-term EM.

The reported `delta` is selected once from the latest applicable finite,
non-negative LR/observation delta for each station, horizon and calendar
period, independently of individual-model forecast coverage. All output
models receive that same value. Historical accuracy still compares each
error with **that historical date's delta**; reported delta is not a replacement
for those historical tolerances.

Member selection and EM scoring use the same history, so the reported skills
are **in-sample** and may be optimistic. Historical stored inputs mix hindcasts
and operational forecasts; this release neither separates them nor establishes
independent out-of-sample performance. No eligible multi-model EM or too few
matched years means no reportable EM skill, even if member forecasts exist.
Dashboard range-option changes remain deferred to the dashboard maintainer.

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
|   |-- ensemble_calculator.py Create ensemble forecasts (short-term EM; long-term Skilled Mean, Naive Mean)
|   |-- file_writer.py         Write forecasts + metrics to CSV (deprecated)
|   |-- gap_detector.py        Detect missing/stale ensemble rows for maintenance
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
| `POSTPROCESSING_GAPFILL_MAX_MONTHS` | Lookback window in months for gap detection (increase after outages) | 13 |

### Season configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `SAPPHIRE_SEASON_START_MONTH` | First month of season (1-12) | 4 (April) |
| `SAPPHIRE_SEASON_END_MONTH` | Last month of season (1-12) | 9 (September) |

Cross-year wrapping is supported: `start=10, end=3` defines an Oct-Mar
season. The `season_year` is the year of the start month.

### Ensemble thresholds (short-term EM: `_short_term` variable, else the un-suffixed variable, else default)

| Variable | Description | Default |
|----------|-------------|---------|
| `ieasyhydroforecast_efficiency_threshold` | sdivsigma threshold (strict gate); fallback for short-term EM | 0.6 (strict gate) / False (EM) |
| `ieasyhydroforecast_nse_threshold` | NSE threshold (strict gate); fallback for short-term EM (a disable token falls back to default) | 0.8 (strict gate) / 1e-9 (EM) |
| `ieasyhydroforecast_accuracy_threshold` | Accuracy threshold (strict gate); fallback for short-term EM | 0.8 (strict gate) / False (EM) |
| `ieasyhydroforecast_nse_threshold_short_term` | Short-term EM NSE gate (cannot be disabled) | 1e-9 (NSE > 0) |
| `ieasyhydroforecast_efficiency_threshold_short_term` | Short-term EM sdivsigma gate (`False` = off) | False |
| `ieasyhydroforecast_accuracy_threshold_short_term` | Short-term EM accuracy gate (`False` = off) | False |
| `ieasyhydroforecast_min_pairs_short_term` | Short-term EM minimum `n_pairs` (no fallback) | 4 |

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

## Quantile Averaging & Known Limitations

This module averages quantile bands in two distinct ways. They have
different mathematical properties and different correctness guarantees.

### Vincentization (cross-model averaging)

Vincentization averages corresponding quantiles from **different
models** for the same station and target period. For example, if TFT
forecasts q10 = 12.5 m³/s and GBT forecasts q10 = 14.3 m³/s, the
ensemble q10 is approximately 13.4 m³/s.

- Used when creating ensemble forecasts: short-term EM; long-term Skilled Mean, Naive Mean.
- For short-term EM, LR contributes only q25/q75 = point -/+ delta (no q05/q95;
  see Short-term EM eligibility).
- Valid for mixture distributions (Vincent 1912, Ratcliff 1979).
- A quantile monotonicity guard (`enforce_quantile_monotonicity` in
  `postprocessing_tools.py`) is applied after every multi-model
  vincentization step to ensure q05 ≤ q10 ≤ ... ≤ q95.

**Where vincentization occurs (14 sites, crossing guard applied):**

- 7 sites in `src/ensemble_calculator.py`
- 7 sites in `src/skill_metrics.py`

### Temporal aggregation (within-model averaging)

Temporal aggregation averages **one model's quantiles across time
steps** to produce a coarser-horizon forecast. For example, averaging
monthly q05 values to produce a quarterly q05.

- Used in `src/aggregation.py` (monthly to quarterly/seasonal) and
  `src/data_reader.py` (daily to pentad/decad).
- This is **not valid vincentization**. The identity
  `mean(q05_jan, q05_feb, q05_mar) = q05(mean_discharge_Q1)` does
  not hold. The true q05 of the aggregate depends on the correlation
  structure between time steps, which simple averaging cannot capture.
- Crossings at temporal aggregation sites are **detected and logged**
  at WARNING level but **not corrected** — they indicate model quality
  issues that cannot be fixed at the postprocessing stage.
- The correct approach would require joint distribution modelling or
  copula methods (Schefzik et al. 2013).

**Where temporal aggregation occurs (3 sites, log-only detector):**

- 2 sites in `src/aggregation.py`
- 1 site in `src/data_reader.py`

### Summary

| Type | Files | Sites | Crossing guard |
|------|-------|-------|---------------|
| Vincentization (cross-model) | `ensemble_calculator.py`, `skill_metrics.py` | 14 | Applied (corrects) |
| Temporal aggregation (within-model) | `aggregation.py`, `data_reader.py` | 3 | Logged only |

### References

- Vincent, S.B. (1912). The function of the vibrissae in the
  behavior of the white rat. Behavioral Monographs 1(5).
- Ratcliff, R. (1979). Group reaction time distributions and an
  analysis of distribution statistics. Psychological Bulletin
  86(3), 446–461.
- Schefzik, R., Thorarinsdottir, T.L. & Gneiting, T. (2013).
  Uncertainty quantification in complex simulation models using
  ensemble copula coupling. Statistical Science 28(4), 616–640.
- Gneiting, T. & Raftery, A.E. (2007). Strictly proper scoring
  rules, prediction, and estimation. JASA 102(477), 359–378.

