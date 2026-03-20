# Clean Null-Discharge Forecast Records and Re-Fill Gaps

**Status**: In Progress
**Modules**: `postprocessing_forecasts`, `machine_learning`
**Impact**: All ML models (TFT, TiDE, TSMixer), ensembles (EM, NE),
skill metrics for all stations

---

## Problem Statement

The `forecasts` table contains thousands of records with
`forecasted_discharge IS NULL`. These originate from two sources:

1. **Historical pentad/decade records (Jan-Dec 2025)**: The ML pipeline
   wrote records even when the model failed to produce predictions
   (Q50 = NaN).
2. **Recent day-level records (Feb 27-28, 2026)**: The ML pipeline ran
   but the model produced no output on those dates (all 605 records
   per model per date are null).

### Current DB State (2026-03-05)

| Horizon | Total | Null Discharge | % Null |
|---------|------:|---------------:|-------:|
| DAY     | 14,520 | 4,741 | 33% |
| PENTAD  | 18,146 | 4,585 | 25% |
| DECADE  |  9,698 | 2,364 | 24% |

Null records per model at PENTAD level:

| Model   | Total | Null | Valid |
|---------|------:|-----:|------:|
| TFT     | 3,816 | 1,272 | 2,544 |
| TiDE    | 3,834 | 1,139 | 2,695 |
| TSMixer | 3,834 | 1,134 | 2,700 |
| NE      | 3,834 | 1,040 | 2,794 |
| EM      | 2,828 |     0 | 2,828 |

### Why This Blocks the Pipelines

Both `fill_ml_gaps.py` and `gap_detector.detect_missing_ensembles()`
check for **missing rows**, not **null-valued rows**:

- `fill_ml_gaps.py` (line 233): looks for gaps in consecutive
  `forecast_date` — a null-discharge row still counts as "present"
- `gap_detector.py` (line 108): checks `model_short == model` row
  existence — doesn't filter on discharge value

Result: null records mask real gaps, preventing automatic gap-filling.

---

## Plan

### Phase 1: Prevent Recurrence (Code fixes)

**Goal**: Fix the write boundary and gap detectors **before** cleanup,
so the next pipeline run cannot re-introduce null records.

#### 1a. Filter nulls at write boundary

Two locations need a null-discharge filter:

1. **ML writer** (`machine_learning/scr/utils_ml_forecast.py`):
   `_write_ml_daily_forecast_to_api()` line ~779 — skip records where
   Q50 is NaN instead of writing `forecasted_discharge: None`.

2. **Postprocessing writer** (`postprocessing_forecasts/src/api_writer.py`):
   `_write_combined_forecast_to_api()` line ~284 — drop rows where
   `forecasted_discharge` is NaN before building the records list.

#### 1b. Treat null-discharge rows as gaps in detectors

Even with write-side filters, partial failures could slip through.
Defense-in-depth: make gap detectors treat null discharge as missing.

1. **`fill_ml_gaps.py`** (line ~233): when checking for consecutive
   `forecast_date`, also exclude rows where `forecasted_discharge` is
   null (or filter them out before gap detection).

2. **`gap_detector.py`** (line ~108): when checking row existence for
   `model_short == model`, add a filter on
   `forecasted_discharge.notna()`.

### Phase 2: Backup and Delete Null-Discharge Records (DB cleanup)

**Goal**: Remove useless records so gap detectors see real gaps.

**Action**: Run SQL directly on the postprocessing database.

```bash
docker exec sapphire-postprocessing-db psql -U postgres -d postprocessing_db
```

```sql
-- 1. Verify counts before deletion
SELECT
    horizon_type,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE forecasted_discharge IS NULL) AS null_count
FROM forecasts
GROUP BY horizon_type
ORDER BY horizon_type;

-- 2. Backup null records before deletion
COPY (SELECT * FROM forecasts WHERE forecasted_discharge IS NULL)
TO '/tmp/null_forecasts_backup.csv' CSV HEADER;

-- 3. Delete null-discharge records (all horizons)
BEGIN;

DELETE FROM forecasts WHERE forecasted_discharge IS NULL;

-- 4. Verify counts after deletion
SELECT
    horizon_type,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE forecasted_discharge IS NULL) AS null_count
FROM forecasts
GROUP BY horizon_type
ORDER BY horizon_type;

-- If counts look correct:
COMMIT;
-- Otherwise: ROLLBACK;
```

**Expected result**: ~11,690 records deleted across all horizons.

**Risk**: Low. The deleted records contain no forecast information
(discharge = NULL). Valid records are untouched. Backup CSV preserved
in container at `/tmp/null_forecasts_backup.csv`.

### Phase 3: Re-run ML Gap-Fill

**Goal**: Fill day-level gaps via hindcast. After Phase 2, the ML gap
detector will see missing forecast dates.

**Action**: For each model and horizon, run gap-fill:

```bash
# From the ML module directory, with the correct .env loaded:
SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD \
    python fill_ml_gaps.py

SAPPHIRE_MODEL_TO_USE=TIDE SAPPHIRE_PREDICTION_MODE=PENTAD \
    python fill_ml_gaps.py

SAPPHIRE_MODEL_TO_USE=TSMIXER SAPPHIRE_PREDICTION_MODE=PENTAD \
    python fill_ml_gaps.py

# Repeat with PREDICTION_MODE=DECAD for decade horizon
SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=DECAD \
    python fill_ml_gaps.py

SAPPHIRE_MODEL_TO_USE=TIDE SAPPHIRE_PREDICTION_MODE=DECAD \
    python fill_ml_gaps.py

SAPPHIRE_MODEL_TO_USE=TSMIXER SAPPHIRE_PREDICTION_MODE=DECAD \
    python fill_ml_gaps.py
```

**Limitation**: `fill_ml_gaps.py` reads day-level records from the API
(past 730 days). Day records only exist from 2026-02-26 onward. For
the historical period (Jan-Dec 2025), there are no day records to
detect gaps in. Hindcast will only fill the recent gap (Feb 27-28).

To fill historical gaps (pre-Feb 2026), either:

- (a) Run a full hindcast for the historical date range manually, or
- (b) Accept that those dates were periods when the model genuinely
  failed and the valid pentad records that remain (2,544-2,700 per
  model) are sufficient for skill metric calculation.

Option (b) is recommended unless skill metrics show poor coverage.

**Note on NE**: Neural ensemble (NE) is produced by the ML module.
If NE gap-fill is needed, it requires a separate ML pipeline run —
`fill_ml_gaps.py` does not support NE directly. Assess NE coverage
after Phase 3 completes.

### Phase 4: Re-run ML NaN Recalculation

**Goal**: Replace any remaining flag=1/2 (NaN marker) records with
valid hindcast values.

```bash
SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD \
    python recalculate_nan_forecasts.py

SAPPHIRE_MODEL_TO_USE=TIDE SAPPHIRE_PREDICTION_MODE=PENTAD \
    python recalculate_nan_forecasts.py

SAPPHIRE_MODEL_TO_USE=TSMIXER SAPPHIRE_PREDICTION_MODE=PENTAD \
    python recalculate_nan_forecasts.py

# Repeat for DECAD
SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=DECAD \
    python recalculate_nan_forecasts.py

SAPPHIRE_MODEL_TO_USE=TIDE SAPPHIRE_PREDICTION_MODE=DECAD \
    python recalculate_nan_forecasts.py

SAPPHIRE_MODEL_TO_USE=TSMIXER SAPPHIRE_PREDICTION_MODE=DECAD \
    python recalculate_nan_forecasts.py
```

### Phase 5: Re-run Postprocessing Maintenance

**Goal**: Aggregate day-level records to pentad/decade and create
missing ensembles (EM).

```bash
# Runs postprocessing_maintenance.py in the Docker container
bash bin/daily_postprc_maintenance.sh /path/to/.env
```

This will:

1. Read day-level ML forecasts from API, aggregate to pentad/decade
2. Read existing combined forecasts from API
3. Detect missing EM rows via `gap_detector.detect_missing_ensembles()`
4. Create EM ensembles using pre-calculated skill metrics
5. Write merged combined forecasts to API

**Note**: NE (neural ensemble) gaps cannot be filled by maintenance
(requires the ML module). Only EM gaps are filled here.

### Phase 6: Recalculate Skill Metrics (All Years)

**Goal**: Recompute all skill metrics with the now-complete forecast
data. The `SAPPHIRE_SKILL_METRICS_YEAR` env var controls which year's
metrics are saved to the API. Since null records affected data from
2025 onward, run for each affected year.

The script reads **all** observed and modelled data regardless of the
year parameter — the year only controls which period's metrics are
written via `save_skill_metrics()`.

```bash
# Run for 2025 (historical period affected by null records)
SAPPHIRE_SKILL_METRICS_YEAR=2025 \
SAPPHIRE_PREDICTION_MODE=BOTH \
    python recalculate_skill_metrics.py

# Run for 2026 (current year)
SAPPHIRE_SKILL_METRICS_YEAR=2026 \
SAPPHIRE_PREDICTION_MODE=BOTH \
    python recalculate_skill_metrics.py
```

If ML data exists for earlier years (e.g. 2024), add a run for those
years too. Check coverage first:

```sql
SELECT EXTRACT(YEAR FROM date) AS yr, COUNT(*)
FROM forecasts
WHERE horizon_type = 'PENTAD'
GROUP BY yr ORDER BY yr;
```

This recalculates sdivsigma, NSE, MAE, delta, accuracy, PBIAS, KGE
for every (period, code, model) combination.

---

## Verification

After all phases complete:

```sql
-- No null discharge records should remain
SELECT horizon_type, COUNT(*)
FROM forecasts
WHERE forecasted_discharge IS NULL
GROUP BY horizon_type;
-- Expected: 0 rows

-- Check coverage for station 15102
SELECT
    model_type,
    COUNT(*) as total,
    MIN(date) as min_date,
    MAX(date) as max_date
FROM forecasts
WHERE horizon_type = 'PENTAD' AND code = '15102'
GROUP BY model_type
ORDER BY model_type;
```

Dashboard check:

```python
# In explore_forecasts.py, update end_date to 2026-03-10
# All models should show continuous lines (with NaN-gap markers
# only where data legitimately doesn't exist)
```

---

## Execution Order

```
Phase 1 (Code fixes - prevent recurrence)
    |
    v
Phase 2 (DB cleanup - backup + delete nulls)
    |
    v
Phase 3 (ML gap-fill) ---> Phase 4 (ML NaN recalc)
    |                              |
    v                              v
Phase 5 (Postprocessing maintenance)
    |
    v
Phase 6 (Skill metrics - all years: 2025, 2026)
```

Phase 1 must be done first to prevent re-introducing nulls.
Phases 3+4 must complete before Phase 5 (postprocessing needs the
filled ML forecasts).
