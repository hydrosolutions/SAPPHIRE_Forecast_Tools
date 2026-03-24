# PP-028: Skill metrics — model=None in original report, missing RMSE, and empty decad/monthly metrics

**Priority**: Medium
**Module**: postprocessing\_forecasts (`pp`)
**Status**: Draft — investigation complete, pending verification steps
**Branch**: TBD

## Problem

Three related bugs in the skill metrics write/recalculation path, discovered during the
2026-03-20 local pipeline review.

### Bug 1: `model=None` across all skill metric records (all horizons)

All skill metric API records (`/api/postprocessing/skill-metric/`) were reported as
returning `model=None` regardless of horizon (pentad, decad, monthly). This affects both
stations tested (15189 and 16059).

**Investigation finding (2026-03-24)**: The writer in `api_writer.py:602-612` **does** set
`model_type` correctly via `MODEL_TYPE_MAP` (lines 508-515). The service schema uses
`model_type` (an `SQLEnum(ModelType)`) — not a field called `model`. The API response
schema (`SkillMetricResponse` in `schemas.py:191-200`) exposes `model_type` plus a
computed `model_type_description`. There is **no field called `model`** in the schema.

`MODEL_TYPE_MAP` covers all 17 `ModelType` enum values. The fallback path (unmapped
`model_short`) would produce an invalid enum string that PostgreSQL rejects loudly — so
silent nulls from mapping failure are unlikely.

**Conclusion**: The original observation of `model=None` was almost certainly reading a
non-existent `model` key from the JSON response (which defaults to `None` in Python),
while `model_type` was correctly populated all along.

**Action required**: Query the API directly (see Verification Step 1 below) to confirm.
If `model_type` is populated, close Bug 1 — it is not a bug.

### Bug 2: `rmse=None` in pentad skill metrics

Pentad skill metrics for both stations have valid `mae` and `nse` but `rmse=None` across
all records.

**Investigation finding (2026-03-24)**: Confirmed — RMSE was **never implemented**:

| Layer | Has RMSE? |
|-------|-----------|
| `METRIC_REGISTRY` in `skill_metrics.py:23-78` | No |
| `calculate_all_skill_metrics()` return Series | No |
| `_write_skill_metrics_to_api()` metric columns list | No |
| DB schema `SkillMetric` model in `models.py:196-236` | **No column** |
| Pydantic schema `SkillMetricBase` in `schemas.py:161-180` | **No field** |

The DB schema includes newer metric columns not yet computed by the writer:

| Metric | In DB? | In `METRIC_REGISTRY`? | In writer? | Computed? |
|--------|--------|----------------------|------------|-----------|
| `crps` | Yes | No (pentad/decad); Yes (monthly) | Yes | Monthly only |
| `pbias` | Yes | Yes | Yes | Yes |
| `kgelf` | Yes | Yes | Yes | Yes |
| `nse_log` | Yes | Yes | Yes | Yes |
| `fhv` | Yes | No | Yes | No |
| `flv` | Yes | No | Yes | No |
| `rmse` | **No** | No | No | No |

**Conclusion**: Adding RMSE is a **cross-boundary change** requiring a service schema
migration (new column + Alembic migration). This violates the ownership boundary — it
must be split into a separate follow-up issue and coordinated with the service owner.
It should **not** be bundled with any `apps/`-only fixes in this issue.

### Bug 3: Decad and monthly skill metrics all empty (n\_pairs=0)

Both decad and monthly skill metrics return 10 records per station but ALL have
`n_pairs=0` and null metrics. Pentad works correctly (n\_pairs=9-17).

**Investigation finding (2026-03-24)**: The recalculation code **does** iterate all three
horizons correctly (`recalculate_skill_metrics.py:185-207`). The pair-matching logic is
sound — merges on `["code", "date"]` for pentad/decad and `["code", "year", "month"]` for
monthly. No horizon-specific skipping was found.

**Aggregation finding (2026-03-24)**: The ML module writes all forecasts as
`horizon_type="day"` (6 daily targets for pentad, 11 for decad). Postprocessing **does**
aggregate these to pentad/decad level via `_normalize_ml_forecasts()` in
`data_reader.py:1775-1901`:
1. Filters daily targets to those within the current period boundary using
   `get_pentad_in_year` / `get_decad_in_year` (lines 1805-1837)
2. Groups by `["code", "date"]` and averages `forecasted_discharge`, quantiles (lines
   1839-1860)
3. Returns one aggregated row per (code, forecast\_date)

This aggregation is **proven working for pentad** (n\_pairs=14-17). LR forecasts are
written directly at pentad/decad level (`horizon_type="pentad"` or `"decade"`), so they
do not need aggregation.

**The decad n\_pairs=0 is therefore NOT caused by missing aggregation logic.** Possible
remaining causes — deeper investigation needed:
- **Data availability**: No decad forecast records in the API (LR may not have produced
  decad forecasts for the test stations; ML daily records may not have been written on
  decad boundary dates)
- **Boundary filtering**: The period boundary filter in `_normalize_ml_forecasts()` (lines
  1805-1829) computes `expected_period = (date+1).get_decad_in_year()`. If the +1 day
  offset produces wrong decad assignments at month boundaries, valid targets get filtered
  out. This needs targeted verification with actual date values.
- **Historical data mix**: The transition from writing `horizon_type="pentad"/"decade"` to
  `"day"` means historical data may have mixed horizons. The reader tries `horizon='day'`
  first, then falls back to the original horizon type — but the fallback path may not
  normalize dates the same way.
- **Missing decad observations**: The preprocessing API may not have decad-level
  observation records for the test stations.

**TODO**: Dig deeper into the decad-specific boundary filtering and data availability.
Run targeted API queries comparing pentad vs decad record counts for the test stations,
and trace the `_normalize_ml_forecasts()` boundary filter with concrete decad boundary
dates to check for off-by-one errors.

**Monthly forecasts** (separate path): The reader at `data_reader.py:1046-1047` extracts
`year`/`month` from `valid_from`. If `valid_from` is the forecast issue date rather than
the target month start, the merge on `["code", "year", "month"]` produces zero pairs
silently. Verification must inspect actual `valid_from` values.

**Observed data summary**:

| Horizon | Station | Records | n\_pairs | mae | nse |
|---------|---------|---------|---------|-----|-----|
| Pentad | S1 (15189) | 10 | 14-17 | 0.04-0.12 | 0.94-0.99 |
| Pentad | S2 (16059) | 10 | 9-14 | 2.96-5.22 | -0.31-0.63 |
| Decad | S1 | 10 | **0** | None | None |
| Decad | S2 | 10 | **0** | None | None |
| Monthly | S1 | 10 | **0** | None | None |
| Monthly | S2 | 10 | **0** | None | None |

## Verification Steps (before any code changes)

These must be performed against a running local instance to confirm or reject each bug.

### Step 1: Verify `model_type` in existing skill metric records

```bash
# Check whether model_type is populated (not null) in pentad skill metrics
curl -s "http://localhost:8000/api/postprocessing/skill-metric/?horizon=pentad&code=15189" \
  | python3 -m json.tool | head -40
```

- If `model_type` shows a valid enum value (e.g., `"LR"`, `"TFT"`): Bug 1 is **not a
  bug** — the original report misread `model` instead of `model_type`. Close it.
- If `model_type` is null: the writer's `MODEL_TYPE_MAP` mapping is failing silently.
  Check what `model_short` values are being passed and whether they match `ModelType` enum
  members.

### Step 2: Check decad forecast data availability and aggregation

```bash
# 2a. Check LR forecasts at decad level (written directly as horizon="decade")
curl -s "http://localhost:8000/api/postprocessing/lr-forecast/?horizon=decade&code=15189&limit=5" \
  | python3 -m json.tool

# 2b. Check ML daily forecasts (written as horizon="day", aggregated by postprocessing)
curl -s "http://localhost:8000/api/postprocessing/forecast/?horizon=day&code=15189&limit=10" \
  | python3 -m json.tool

# 2c. Check combined forecasts at decad level (should exist if aggregation ran)
curl -s "http://localhost:8000/api/postprocessing/forecast/?horizon=decade&code=15189&limit=5" \
  | python3 -m json.tool

# 2d. Compare record counts: pentad vs decad (pentad works, decad doesn't)
echo "--- Pentad LR ---"
curl -s "http://localhost:8000/api/postprocessing/lr-forecast/?horizon=pentad&code=15189&limit=1" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'count={len(d)}')"
echo "--- Decad LR ---"
curl -s "http://localhost:8000/api/postprocessing/lr-forecast/?horizon=decade&code=15189&limit=1" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'count={len(d)}')"

# 2e. Check decad observations exist in preprocessing API
curl -s "http://localhost:8000/api/preprocessing/runoff/?horizon=decade&code=15189&limit=5" \
  | python3 -m json.tool
```

**Interpretation guide:**
- If 2a returns records: LR decad forecasts exist. Compare `date` field against decad
  observation dates to check for alignment (off-by-one on decad boundaries).
- If 2b returns daily ML records but 2c is empty: daily→decad aggregation has not run (no
  decad postprocessing on boundary days), or the combined forecast writer doesn't store
  decad-level records.
- If 2d shows pentad LR records but zero decad LR records: LR module may not have been
  configured to produce decad forecasts for these stations — pure data gap.
- If 2e returns no decad observations: even if forecasts exist, the merge on
  `["code", "date"]` will produce zero pairs because the observation side is empty.

### Step 2f: Trace decad boundary filtering (deeper investigation)

If decad forecast and observation records both exist but n\_pairs is still 0, the boundary
filtering in `_normalize_ml_forecasts()` needs targeted verification:

```python
# Run in a Python session with the postprocessing venv active.
# Pick a concrete decad boundary date and trace the filter logic.
import datetime as dt
from iEasyHydroForecast import tag_library as tl

# Example: Jan 10 is a decad boundary
boundary = dt.date(2025, 1, 10)
expected_period = tl.get_decad_in_year(boundary + dt.timedelta(days=1))
print(f"Boundary={boundary}, expected_period={expected_period}")

# For each daily target (Jan 1-10), check which ones pass the filter
for day in range(1, 12):
    target = dt.date(2025, 1, day)
    target_period = tl.get_decad_in_year(target)
    passes = target_period == expected_period
    print(f"  target={target}, target_period={target_period}, passes={passes}")
```

This will reveal whether the +1 day offset in `expected_period = (date+1).get_decad_in_year()`
produces correct or incorrect decad assignments at period boundaries.

### Step 3: Check monthly long-term forecast data availability

```bash
# Check whether monthly long-term forecasts exist
curl -s "http://localhost:8000/api/postprocessing/long-term-forecast/?horizon_type=month&code=15189&limit=5" \
  | python3 -m json.tool
```

- If records exist with non-null discharge values: **inspect `valid_from` values** — do
  they represent the target month (e.g., `2025-06-01` for June forecast) or the issue date
  (e.g., `2025-05-15`)? The reader extracts `year`/`month` from `valid_from`, so a
  mismatch here causes silent zero pairs.
- If records are null or missing: Bug 3 (monthly) is a **data gap** — blocked by the
  known "6/8 model outputs null" issue.

### Step 4: Verify round-trip consistency (write → read → ensemble use)

**Critical safety check.** Skill metrics are consumed by `postprocessing_operational.py`
to build ensembles. Verify that:

```bash
# Read skill metrics as the operational pipeline would
curl -s "http://localhost:8000/api/postprocessing/skill-metric/?horizon=pentad&code=15189" \
  | python3 -c "
import json, sys
data = json.load(sys.stdin)
for r in data[:3]:
    print(f\"model_type={r.get('model_type')}, sdivsigma={r.get('sdivsigma')}, accuracy={r.get('accuracy')}, n_pairs={r.get('n_pairs')}\")
"
```

- The `model_type` values returned by the API must match `model_short` values that the
  skill metric reader maps back when loading into DataFrames. If the operational reader
  expects `model_short="LR"` but the API returns `model_type="LR"`, confirm the reader
  performs the reverse mapping correctly.
- The `sdivsigma` and `accuracy` values must be in ranges that the ensemble threshold
  filter uses (env vars `ieasyhydroforecast_efficiency_threshold` and
  `ieasyhydroforecast_accuracy_threshold`). If any proposed change alters how these are
  computed, it will change which models qualify for EM — silently altering operational
  forecasts.

## Root Cause Summary (post-investigation)

| Bug | Root Cause | Status |
|-----|-----------|--------|
| Bug 1 (model=None) | Almost certainly a misread of API response (`model` vs `model_type`). Writer and `MODEL_TYPE_MAP` (17/17 enum values covered) are correct. | **Needs API query verification (Step 1)** |
| Bug 2 (rmse=None) | Never implemented. No column in DB, no field in schema, no computation. **Cross-boundary fix — split to separate issue.** | **Confirmed gap — out of scope for this issue** |
| Bug 3 (n\_pairs=0) | Recalculation logic correct. Likely data availability (no decad/monthly forecasts) or `valid_from` date semantics for monthly. | **Needs API query verification (Steps 2-4)** |

## Proposed Changes

### If Bug 1 is confirmed as misread (expected outcome)

No code changes needed. Update the original observation notes to use the correct field
name `model_type`. Close Bug 1.

### If Bug 1 is confirmed as real (model\_type actually null in DB)

**File**: `apps/postprocessing_forecasts/src/api_writer.py` (lines 508-515)

Debug the `MODEL_TYPE_MAP` lookup. Since the map covers all 17 enum values and the
fallback would cause a PostgreSQL enum error (not a silent null), this outcome is unlikely
unless the API silently drops the field.

### Bug 2: RMSE — split to separate issue

**Out of scope for PP-028.** RMSE requires a service schema migration (new DB column +
Alembic migration + Pydantic field) which is a cross-boundary change. Create a separate
issue (e.g., PP-029) scoped to:

1. Coordinate with service owner to add `rmse = Column(Float)` to `SkillMetric`
2. Add `rmse: float | None = None` to `SkillMetricBase`
3. Run Alembic migration
4. Add RMSE to `METRIC_REGISTRY` and `calculate_all_skill_metrics()`
5. Add `"rmse"` to metric columns list in `_write_skill_metrics_to_api()`

The calculation is trivial: `np.sqrt(np.mean((obs - sim) ** 2))`. The `sdivsigma`
computation already has the numerator (`sqrt(sum(differences^2) / (n-1))`) at
`skill_metrics.py:905`.

### If Bug 3 is confirmed as data gap (expected outcome)

No code changes to the pair-matching logic. Add observability:

**File**: `apps/postprocessing_forecasts/src/skill_metrics.py`

Add a WARNING log after the merge step when zero pairs are found, so the gap is surfaced
rather than silently written as n\_pairs=0:

```python
if n_pairs == 0:
    logger.warning(
        "Zero forecast-observation pairs for %s/%s/%s — skill metrics will be empty",
        horizon_type, code, model_short,
    )
```

This is additive-only and does not alter data flow.

### If Bug 3 is confirmed as date misalignment

**File**: `apps/postprocessing_forecasts/src/data_reader.py`

Fix the date alignment in the forecast reader for the affected horizon. The pair-matching
merge is at `skill_metrics.py:1678-1680` (short-term) and `skill_metrics.py:1085-1090`
(monthly).

**Safety constraint**: Any date alignment fix must be verified against the ensemble
creation path (see Downstream Impact section below) to ensure it does not change which
models qualify for EM.

## Downstream Impact Analysis

**Skill metrics feed directly into ensemble creation.** This is the critical data flow
constraint that any change must preserve:

```
recalculate_skill_metrics → writes skill_stats to API
    ↓
postprocessing_operational reads skill_stats from API
    ↓
ensemble_calculator.create_ensemble_forecasts() filters models by:
    - sdivsigma < ieasyhydroforecast_efficiency_threshold
    - accuracy > ieasyhydroforecast_accuracy_threshold
    ↓
Qualifying models → EM (Ensemble Mean) or NE (Neural Ensemble)
    ↓
EM/NE written to forecast API → consumed by forecast_dashboard
```

**For long-term (monthly):**
```
recalculate_skill_metrics → writes monthly skill_stats to API
    ↓
Monthly postprocessing reads skill_stats
    ↓
Three ensembles created:
    - EM: threshold-filtered average (sdivsigma, accuracy, nse)
    - Skilled Mean: 1/MAE weighted average of qualifying models
    - Naive Mean: unweighted average of ALL models
```

**Any change that alters `sdivsigma`, `accuracy`, `nse`, or `mae` values — or changes
which (horizon, code, model) combinations have n\_pairs > 0 — will change which models
qualify for ensembles and how they are weighted.** This means:

1. The n\_pairs=0 warning log (proposed for Bug 3) is safe — purely additive.
2. A date alignment fix that increases n\_pairs from 0 to >0 would **add new skill
   metrics where none existed**, potentially enabling EM creation for decad/monthly where
   it currently doesn't happen. This is the desired outcome, but must be verified against
   the threshold configuration.
3. Any change to `calculate_all_skill_metrics()` (e.g., adding RMSE) must not alter the
   existing return values for `sdivsigma`, `nse`, `mae`, `accuracy` — these are the
   ensemble-deciding metrics.

## Acceptance Criteria

- [ ] Verification Steps 1-4 completed and results documented in this plan
- [ ] Bug 1 either closed (misread) or root-caused and fixed
- [ ] Bug 2 split to a separate issue (PP-029) for service schema coordination
- [ ] Bug 3 root cause confirmed (data gap vs date alignment) and appropriate action taken
- [ ] A WARNING log message is emitted when n\_pairs=0 for any horizon/station/model
  combination after recalculation
- [ ] If any code changes made: all existing tests pass (`SAPPHIRE_TEST_ENV=True bash
  run_tests.sh`) with zero new skips
- [ ] If date alignment fixed: verify ensemble thresholds still produce expected EM
  composition for affected stations (round-trip test)

## Existing Test Coverage (audit 2026-03-24)

**Well covered:**
- `calculate_all_skill_metrics()` — 18 tests including edge cases (NaN, single point,
  constant obs, inf) in `test_calculate_all_skill_metrics.py`
- `calculate_skill_metrics()` (pentad + decad) — 14+ tests per horizon including ensemble
  creation, NaN exclusion, threshold filtering in `test_skill_metrics.py`
- `calculate_monthly_skill_metrics()` — 40+ tests including EM, Skilled Mean, Naive Mean,
  CRPS, edge cases in `test_monthly_skill_metrics.py`
- `_write_skill_metrics_to_api()` — model\_type mapping tested for LR, TFT, TiDE,
  TSMixer, EM, NE, GBT, LR\_Base, SM\_GBT, MC\_ALD, SM\_GBT\_Norm, Naive Mean in
  `test_api_integration.py`
- Integration workflow — full recalculation → save → verify in
  `test_integration_postprocessing.py`

**Gaps (relevant to this issue):**
- `pbias`, `kgelf`, `nse_log` are sent in API payloads but **not asserted** in any test —
  changes to the metric columns list could silently drop these
- No test for the `MODEL_TYPE_MAP` fallback path (unmapped model\_short → raw string →
  PostgreSQL enum rejection)
- No test for the write → read → ensemble round-trip (model\_type written correctly but
  read back as model\_short for threshold filtering)
- API write failure/retry scenarios not tested

**Note**: These gaps are pre-existing and not introduced by this issue. They should be
addressed incrementally but are not blockers for PP-028.

## Key Files (from investigation)

| File | Role |
|------|------|
| `apps/postprocessing_forecasts/src/api_writer.py:412-636` | `_write_skill_metrics_to_api()` — builds and posts payload |
| `apps/postprocessing_forecasts/src/api_writer.py:21-43` | `MODEL_TYPE_MAP` — 17 entries, covers all `ModelType` enum values |
| `apps/postprocessing_forecasts/src/api_writer.py:508-515` | `model_type` mapping from `model_short` |
| `apps/postprocessing_forecasts/src/api_writer.py:577-612` | Metric columns list (includes `crps`, `pbias`, `kgelf`, `nse_log`, `fhv`, `flv`; no RMSE) |
| `apps/postprocessing_forecasts/src/skill_metrics.py:23-78` | `METRIC_REGISTRY` — 9 metrics (no RMSE, no CRPS for pentad/decad) |
| `apps/postprocessing_forecasts/src/skill_metrics.py:795-944` | `calculate_all_skill_metrics()` — point metric computation |
| `apps/postprocessing_forecasts/src/skill_metrics.py:1588-1809` | `calculate_skill_metrics()` — pentad/decad flow including ensemble |
| `apps/postprocessing_forecasts/src/skill_metrics.py:1048-1250` | `calculate_monthly_skill_metrics()` — monthly flow including 3 ensembles |
| `apps/postprocessing_forecasts/src/skill_metrics.py:1678-1680` | Pentad/decad pair merge on `["code", "date"]` |
| `apps/postprocessing_forecasts/src/skill_metrics.py:1085-1090` | Monthly pair merge on `["code", "year", "month"]` |
| `apps/postprocessing_forecasts/src/data_reader.py:1046-1047` | Monthly forecast `year`/`month` extraction from `valid_from` |
| `apps/postprocessing_forecasts/src/data_reader.py:1422-1504` | Decad LR forecast read with `api_horizon="decade"` |
| `apps/postprocessing_forecasts/src/data_reader.py:1506-1595` | Decad ML forecast read — tries `horizon='day'` first |
| `apps/postprocessing_forecasts/recalculate_skill_metrics.py:185-207` | Horizon iteration loop (pentad, decad, monthly) |
| `apps/postprocessing_forecasts/postprocessing_operational.py:110-163` | Operational pipeline — reads (not writes) skill metrics for ensemble creation |
| `sapphire/services/postprocessing/app/models.py:196-236` | `SkillMetric` ORM — no `rmse`; has `crps`, `fhv`, `flv` placeholders |
| `sapphire/services/postprocessing/app/schemas.py:161-200` | Pydantic schemas — `model_type` (not `model`), no `rmse` |

## Risks

- **Ensemble stability**: Any change affecting `sdivsigma`, `accuracy`, `nse`, or `mae`
  values alters which models qualify for ensembles. See Downstream Impact Analysis.
- **RMSE schema change**: Cross-boundary (service code). Must be coordinated with service
  owner per ownership boundaries. Split to PP-029.
- **Decad date alignment**: If a fix is needed, changing alignment could alter historical
  n\_pairs values and enable decad EM where it didn't exist before. Run recalculation on a
  test DB first and verify ensemble composition.
- **Monthly `valid_from` semantics**: If a date fix changes which `year`/`month` pairs
  match, it could change monthly skill metric values and alter Skilled Mean / Naive Mean
  membership. Verify against threshold configuration.
- **Test gap for `pbias`/`kgelf`/`nse_log` in API payloads**: These are not asserted in
  tests. Any accidental change to the metric columns list could drop them silently.

## Related Issues

- **PP-027** — EM silent skip observability: fewer EM records for S2 reduce the pool of
  pairs available for skill evaluation
- **PP-029 (new, to be created)** — Add RMSE to service schema and computation pipeline
- **Long-term model null output** — 6/8 model outputs null for monthly forecasts means no
  valid pairs for monthly skill (tracked separately)
- **INFRA-005** — model\_long removal: `model` field semantics here should use
  `model_short` consistently, aligned with the model registry plan

## Source

Discovered during local pipeline review on 2026-03-20. Documented in:

- `doc/dev/review_checklist_local_2026-03-20.md` Section 7.3
- `doc/plans/observations.md` entries "Skill Metrics Have model=None and n\_pairs=0" and
  "Monthly Skill Metrics All Empty"

## Dependency Graph

```json
{
  "phases": [
    {
      "id": "P0",
      "name": "Verification: query API to confirm/reject each bug (Steps 1-4)",
      "depends_on": [],
      "files": []
    },
    {
      "id": "P1",
      "name": "Close Bug 1 if misread confirmed; add n_pairs=0 warning log for Bug 3",
      "depends_on": ["P0"],
      "files": ["src/skill_metrics.py"]
    },
    {
      "id": "P2",
      "name": "Create PP-029 issue for RMSE (service schema coordination)",
      "depends_on": ["P0"],
      "files": []
    },
    {
      "id": "P3",
      "name": "Fix date alignment if Bug 3 confirmed as code issue; verify ensemble stability",
      "depends_on": ["P0"],
      "files": ["src/data_reader.py", "src/skill_metrics.py", "tests/"]
    }
  ]
}
```
