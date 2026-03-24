# Clean Null-Discharge Forecast Records and Re-Fill Gaps

**Status**: Complete (Phase 1 code fixes done; Phase 2 DB cleanup skipped — all consumers now filter null-discharge rows, tombstones needed for ML-008b; Phases 3-6 are routine operational tasks, not code issues)
**Modules**: `postprocessing_forecasts`, `machine_learning`
**Impact**: All ML models (TFT, TiDE, TSMixer), ensembles (EM, NE),
skill metrics for all stations

---

## Problem Statement

The `forecasts` table contains thousands of records with
`forecasted_discharge IS NULL`. These originate from two sources:

1. **Historical pentad/decade records (Jan-Dec 2025)**: The ML pipeline
   wrote records when the model failed to produce predictions
   (Q50 = NaN). Many of these carry a flag value indicating the reason
   for failure (transient gap, code error, or permanent failure).
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

The original analysis assumed all null-discharge records were useless
and should be deleted. That was wrong — see "Design Rationale" below.
The actual problem is that **consumers** (gap detectors, skill metrics,
dashboard) were not flag-aware and treated all rows identically
regardless of discharge value or flag.

- **`gap_detector.py`** (line ~108): checked `model_short == model` row
  existence without filtering on `forecasted_discharge.notna()` —
  flag=3 tombstone rows would mask the fact that a date had no valid
  forecast, causing gap detection to skip dates that genuinely needed
  re-hindcasting.
- **`fill_ml_gaps.py`**: similarly treated any row as "present",
  so flag=3 rows would prevent the gap-filler from requesting a
  hindcast for that date.
- **Skill metrics**: including null-discharge rows in metric
  calculations produces meaningless results.

---

## Design Rationale: The ML Flag State Machine

Null-discharge records in the ML module are **intentional** and carry
operational meaning. The flag column encodes the reason:

| Flag | Q50 | Meaning | Retry? |
|------|-----|---------|--------|
| 0 | value | Valid operational forecast | No |
| 1 | NaN | Operational NaN — transient data gap | Yes (`recalculate_nan`) |
| 2 | NaN | Code error — model crashed | Yes (`recalculate_nan`) |
| 3 | NaN | Permanent failure — no input data | **No — tombstone** |
| 4 | value | Valid hindcast replacement | No |

**Flag=3 rows are tombstones.** They were introduced by ML-008b to
prevent the infinite hindcast loop: without a tombstone, the gap
detector would repeatedly detect that date as missing and trigger
another hindcast attempt, which would fail again, looping forever.
Deleting flag=3 rows would re-introduce the ML-008b infinite loop.

**Flag=1/2 rows are retry signals** for `recalculate_nan_forecasts.py`.
They indicate transient failures that may be resolvable with a
hindcast attempt.

**Consequence for this plan**: Phase 2 must NOT be a blanket DELETE of
all null-discharge records. It must be flag-aware.

---

## Plan

### Phase 1: Prevent Recurrence (Code fixes)

**Goal**: Fix write boundaries and gap detectors **before** cleanup,
so the next pipeline run cannot re-introduce the problem.

#### 1a. Remove dead code `_write_ml_daily_forecast_to_api`

`_write_ml_daily_forecast_to_api()` in
`machine_learning/scr/utils_ml_forecast.py` (around line 875) is dead
code — it is never called anywhere in the module. All ML writes go
through `_write_ml_forecast_to_api`. The dead function also contains
a conceptually wrong null-discharge filter (added during an earlier
revision of this plan) and a docstring that incorrectly claims it is
"used by the decad pipeline."

**Action**: Delete `_write_ml_daily_forecast_to_api` entirely from
`utils_ml_forecast.py`.

**Keep the postprocessing writer filter** in
`postprocessing_forecasts/src/api_writer.py` —
`_write_combined_forecast_to_api()` dropping rows where
`forecasted_discharge` is NaN before building the records list is
correct. Combined/ensemble forecasts with null discharge carry no
useful information (they are downstream aggregates, not raw model
state), so filtering them at write time is appropriate and does not
interfere with the flag state machine.

#### 1b. Make gap detectors flag-aware

The gap detector fixes ensure that flag=3 tombstone rows correctly
satisfy "this date is covered" while still allowing `recalculate_nan`
to retry flag=1/2 rows.

1. **`gap_detector.py`** filter on `forecasted_discharge.notna()` —
   **Done** ✓ (already implemented as part of ML-008b / this branch).

2. **`fill_ml_gaps.py`** — **No longer needed.** ML-008b resolved this
   by removing the null-Q50 exclusion filter from the gap detection
   path. Flag=3 rows now count as "present" dates in `fill_ml_gaps.py`,
   which is correct — they should NOT trigger re-hindcasting because
   the failure was permanent (no input data available for that date).

### Phase 2: Flag-Aware DB Cleanup

**Goal**: Resolve or remove only the null-discharge records that are
genuinely problematic, while preserving tombstones that prevent
infinite loops.

**Action**: Run SQL directly on the postprocessing database.

```bash
docker exec sapphire-postprocessing-db psql -U postgres -d postprocessing_db
```

**Step 1 — Understand the flag distribution of null records:**

```sql
SELECT flag, horizon_type, COUNT(*)
FROM forecasts
WHERE forecasted_discharge IS NULL
GROUP BY flag, horizon_type
ORDER BY flag, horizon_type;
```

**Step 2 — Decide per flag value:**

- **Flag=3 (tombstone)**: Keep as-is. These are permanent-failure
  markers that prevent infinite hindcast loops. Do NOT delete.
- **Flag=4 (valid hindcast)**: These should have a discharge value.
  Investigate any flag=4 null rows individually — they indicate a write
  bug. Count:
  ```sql
  SELECT COUNT(*) FROM forecasts
  WHERE forecasted_discharge IS NULL AND flag = 4;
  ```
- **Flag=1/2 (retry signals)**: These should be resolved by running
  `recalculate_nan_forecasts.py` (Phase 4). Do NOT delete — let the
  recalculate script attempt hindcasts and either fill them (flag → 4)
  or mark them permanent (flag → 3). Rows older than 30 days that
  persist as flag=1/2 indicate `recalculate_nan` never ran or
  failed silently.
- **Flag=NULL (if any)**: Records written before the flag column
  existed. Investigate individually before deciding whether to keep
  or delete.

**Step 3 — Backup all null records before any action:**

```sql
COPY (SELECT * FROM forecasts WHERE forecasted_discharge IS NULL)
TO '/tmp/null_forecasts_backup.csv' CSV HEADER;
```

**Step 4 — Remove only flag=NULL records that predate the flag
column** (if confirmed safe after investigation):

```sql
BEGIN;

-- Only delete records with no flag and no discharge
-- (pre-flag-column era, truly useless records)
DELETE FROM forecasts
WHERE forecasted_discharge IS NULL AND flag IS NULL;

-- Verify tombstones are intact
SELECT flag, horizon_type, COUNT(*)
FROM forecasts
WHERE forecasted_discharge IS NULL
GROUP BY flag, horizon_type
ORDER BY flag, horizon_type;
-- Expected: only flag=3 rows remain (tombstones)

-- If correct:
COMMIT;
-- Otherwise: ROLLBACK;
```

**Expected result**: Flag=3 null rows remain (correct). Flag=1/2 rows
remain until Phase 4 resolves them. Only genuinely useless rows (no
flag, no discharge) are removed.

### Phase 3: Re-run ML Gap-Fill

**Goal**: Fill genuinely missing dates via hindcast. After Phase 2,
the ML gap detector will see dates with no record at all (not even a
tombstone).

**Note**: With the ML-008b fix in place, `fill_ml_gaps.py` will now
correctly skip flag=3 dates (tombstones count as "present"). Only
dates with no record at all will be hindcast-attempted. This means
the gap-filler will not loop on permanent-failure dates.

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

**Goal**: Replace flag=1/2 (transient-failure) records with valid
hindcast values. This is the correct resolution path for retry-signal
rows — do not delete them, let `recalculate_nan_forecasts` either
fill them (flag → 4) or permanently mark them (flag → 3).

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

After this phase completes, all flag=1/2 rows should be resolved.
Any that remain as null-discharge should now be flag=3 (permanent
failure confirmed by the recalculate script). Verify:

```sql
SELECT flag, horizon_type, COUNT(*)
FROM forecasts
WHERE forecasted_discharge IS NULL AND flag IN (1, 2)
GROUP BY flag, horizon_type;
-- Expected: 0 rows
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

After all phases complete, the expected state is:

- Flag=3 null-discharge rows remain (tombstones — this is correct)
- No flag=1/2 null-discharge rows (all resolved by Phase 4)
- No flag=NULL null-discharge rows (removed in Phase 2 or confirmed safe)

```sql
-- Flag=3 null-discharge rows are expected (permanent failures / tombstones)
-- Flag=1/2 null-discharge rows should be 0 after recalculate_nan runs
SELECT flag, horizon_type, COUNT(*)
FROM forecasts
WHERE forecasted_discharge IS NULL AND flag NOT IN (3)
GROUP BY flag, horizon_type;
-- Expected: 0 rows (all remaining nulls should be flag=3 tombstones)

-- Verify tombstones are present and accounted for
SELECT flag, horizon_type, COUNT(*)
FROM forecasts
WHERE forecasted_discharge IS NULL AND flag = 3
GROUP BY flag, horizon_type
ORDER BY horizon_type;
-- These rows are correct — do not treat as a problem

-- Check coverage for station 15102
SELECT
    model_type,
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE forecasted_discharge IS NOT NULL) AS valid,
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
Phase 1 (Code fixes - dead code removal, gap detector fixes)
    |
    v
Phase 2 (DB cleanup - flag-aware, backup + selective removal)
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

---

*Revised 2026-03-23: Null-discharge ML records are intentional (flag
state machine). Removed ML write-boundary filter — would break ML-008b
infinite loop prevention. Reframed Phase 2 as flag-aware cleanup.*
