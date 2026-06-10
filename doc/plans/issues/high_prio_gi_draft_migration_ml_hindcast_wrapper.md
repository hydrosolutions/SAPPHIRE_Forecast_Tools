## Build full-history ML hindcast wrapper — toolkit P4b ships export/import but no data generator (MIG-004)

**Status**: Draft (2026-06-10)
**Module**: `machine_learning` + `migration-toolkit`
**Priority**: **High** (gap blocks first-time ML backfill on any dev or production deployment; same class as MIG-003)
**Labels**: `migration-toolkit`, `machine-learning`, `hindcast`, `data-generation`, `high-priority`
**Discovered**: 2026-06-10 during reviewer rounds 5-8 of `bin/dev_local_backfill.sh` v3.x (the v3.5 script's Phase 4b is currently a documented-not-implemented scaffold pointing here)
**Related**:
- `bin/dev_local_backfill.sh` v3.5 Phase 4b (consumer)
- `bin/initialize_site_backfill.sh` (LR sibling reference)
- `apps/long_term_forecasting/calibrate_and_hindcast.py` (LT sibling reference)
- `apps/machine_learning/hindcast_ML_models.py` (the underlying script to wrap)
- `apps/machine_learning/daily_ml_maintenance.sh` (gap-fill, NOT initial backfill — what NOT to use here)

---

## Summary

The migration toolkit (P0–P7, merged) is intentionally scoped as data-migration: it moves existing data between systems. It does not generate new forecast data via model execution. For LR + LT, hindcast wrappers existed before the toolkit work and were not re-built. For ML, no equivalent wrapper exists — only `apps/machine_learning/hindcast_ML_models.py` which requires env vars and processes one (model, horizon) per invocation. This MIG-004 gi_draft specifies a wrapper that iterates models × horizons × stations safely.

---

## Authority citations

- `apps/machine_learning/hindcast_ML_models.py:162` — `forecast_horizon` constants (PENTAD=6, DECAD=11; daily steps)
- `apps/machine_learning/hindcast_ML_models.py:178` — static features file load
- `apps/machine_learning/hindcast_ML_models.py:187` — `PATH_TO_SCALER` env var resolution + `_Decad` value suffix
- `apps/machine_learning/hindcast_ML_models.py:228` — `ieasyhydroforecast_NEW_STATIONS` env var read (station scope)
- `apps/machine_learning/hindcast_ML_models.py:267` — ERA5 coverage requirement: `start_date - 60d` through `end_date + forecast_horizon`
- `apps/machine_learning/hindcast_ML_models.py:284` — static features check before model execution
- `apps/machine_learning/.../BaseDartsDLPredictor.py:323,441` — daily TimeSeries + `last_points_only=False` + `stride=1`
- `apps/machine_learning/scr/utils_ml_forecast.py:655,718` — writer stores `horizon_type='DAY'` (L6 user-lock); payload has `(date, target)` keys
- `bin/utils/migration_py/ml_forecast.py:33-43,267` — confirms ML payload row has both `date` (issue) and `target` (forecast target)
- `sapphire/services/postprocessing/app/models.py:9-25,66` — dual-representation rule (uppercase enum labels, lowercase API wire); `date`=issue date, `target`=forecast target date
- `apps/long_term_forecasting/data_interface.py:865` — SQL precedent: `UPPER(model_type::text) = UPPER(:bind)`
- `apps/machine_learning/reaggregate_day_to_periods.py:24,325` — global reaggregation, no filters, hardcoded model list and DB/container
- `apps/machine_learning/fill_ml_gaps.py:203` — confirms 730-day lookback in maintenance path (NOT suitable for first-time backfill)

---

## Wrapper goal

`bin/initialize_ml_hindcast.sh` — full-history ML hindcast wrapper. Sibling-in-spirit to `bin/initialize_site_backfill.sh` (LR) and `apps/long_term_forecasting/calibrate_and_hindcast.py` (LT). Iterates models × horizons × stations, validates assets + forcing coverage, invokes `hindcast_ML_models.py` per (model, horizon, station), verifies writes via DB query, supports skip-if-already-complete idempotency.

---

## Proposed CLI signature

```bash
bash bin/initialize_ml_hindcast.sh <env_file_path> \
    [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] \
    [--models TFT,TIDE,TSMIXER] \
    [--horizons PENTAD,DECAD] \
    [--station-filter <CODE>] \
    [--reaggregate] [--dry-run] [--continue-on-error] \
    [-h | --help]
```

`<env_file_path>` positional matches `initialize_site_backfill.sh` convention.

---

## Date policy (precedence-ordered)

| Source | Precedence | Behavior if absent |
|---|---|---|
| `--start-date YYYY-MM-DD` (CLI) | 1 | fall through to env var |
| `$ieasyhydroforecast_ml_hindcast_start_date` (env var) | 2 | hard-fail with explicit error if both absent |
| `--end-date YYYY-MM-DD` (CLI) | 1 | fall through to env var |
| `$ieasyhydroforecast_ml_hindcast_end_date` (env var) | 2 | fall through to lookback calc |
| (computed) `today_UTC - $ieasyhydroforecast_ml_hindcast_end_lookback_days` | 3 | env var defaults to 30 if absent |

`today_UTC = date -u +%Y-%m-%d`. Both `start_date` and `end_date` are **INCLUSIVE**.

---

## Station iteration (triple-nested loop)

```bash
for model in $MODELS; do
  for horizon in $HORIZONS; do
    for code in $STATIONS; do
      ieasyhydroforecast_NEW_STATIONS="$code" \
      SAPPHIRE_MODEL_TO_USE="$model" \
      SAPPHIRE_HINDCAST_MODE="$horizon" \
      ieasyhydroforecast_START_DATE="$start_date" \
      ieasyhydroforecast_END_DATE="$end_date" \
      ieasyhydroforecast_env_file_path="$ENV_FILE" \
      uv run python apps/machine_learning/hindcast_ML_models.py
    done
  done
done
```

Each invocation explicitly sets `ieasyhydroforecast_NEW_STATIONS=<code>` so `hindcast_ML_models.py:228` scopes to a single station. Without this, the script would re-run the full configured station list per invocation.

---

## Asset verification preflight (per model × horizon)

**Step 1: Resolve scaler directory path.**

Per-model env var (constant across horizons):
- `$ieasyhydroforecast_PATH_TO_SCALER_TFT`
- `$ieasyhydroforecast_PATH_TO_SCALER_TIDE`
- `$ieasyhydroforecast_PATH_TO_SCALER_TSMIXER`

Horizon modifier applied to the resolved VALUE (not the variable name), per `hindcast_ML_models.py:187`:

```python
PATH_TO_SCALER = os.getenv("ieasyhydroforecast_PATH_TO_SCALER_" + MODEL)
if HORIZON == "DECAD":
    PATH_TO_SCALER = f"{PATH_TO_SCALER}_Decad"
```

Example:
- `$ieasyhydroforecast_PATH_TO_SCALER_TFT = "/data/scalers/TFT"`
- → PENTAD path: `/data/scalers/TFT/`
- → DECAD path: `/data/scalers/TFT_Decad/`

**Step 2: Confirm 5 files exist (4 in scaler dir + 1 static features).**

In the resolved scaler directory (non-ARIMA models):
- `scaler_stats_discharge.csv`
- `scaler_stats_era5.csv`
- `scaler_stats_static.csv`
- `<something>.pt` (any *.pt model checkpoint)

PLUS static features file (per env config; constant for all model × horizon combinations):
- `$ieasyhydroforecast_models_and_scalers_path/$ieasyhydroforecast_PATH_TO_STATIC_FEATURES`

Per `hindcast_ML_models.py:178,284`: the script loads static features BEFORE model execution. Preflight check must occur first.

Refuse to invoke if any of the 5 files is missing; fail-fast with explicit path + missing-file list.

---

## Forcing coverage validation preflight (per station × horizon)

Window: `start_date - 60d` through `end_date + forecast_horizon_days`, where:
- PENTAD: `+ 6 days`
- DECAD: `+ 11 days`

(Per `hindcast_ML_models.py:267`. The 60d lookback is the model's input context window; the forward extension covers daily target dates.)

**PENTAD query**:

```sql
SELECT meteo_type,
       MIN(date) AS coverage_min,
       MAX(date) AS coverage_max,
       COUNT(DISTINCT date) AS coverage_days
  FROM meteo
 WHERE meteo_type IN ('T','P')
   AND code = '<station_code>'
   AND date >= '<start_date>'::date - INTERVAL '60 days'
   AND date <= '<end_date>'::date   + INTERVAL '6 days'
 GROUP BY meteo_type;
```

**DECAD query**:

```sql
SELECT meteo_type,
       MIN(date) AS coverage_min,
       MAX(date) AS coverage_max,
       COUNT(DISTINCT date) AS coverage_days
  FROM meteo
 WHERE meteo_type IN ('T','P')
   AND code = '<station_code>'
   AND date >= '<start_date>'::date - INTERVAL '60 days'
   AND date <= '<end_date>'::date   + INTERVAL '11 days'
 GROUP BY meteo_type;
```

Per-station-per-meteo-type gap fraction: `1 - (coverage_days / total_days_in_window)`. If any station-meteo_type gap exceeds `$ieasyhydroforecast_ml_hindcast_max_gap_fraction` (env var, default `0.05`), refuse to invoke for that station; warn-and-continue if `--continue-on-error`, otherwise fatal.

---

## Write verification per (model × horizon × station)

Run this query BEFORE and AFTER each `hindcast_ML_models.py` invocation:

```sql
SELECT COUNT(DISTINCT date)     AS issue_dates,
       COUNT(*)                 AS rows,
       MIN(date)                AS min_issue_d,
       MAX(date)                AS max_issue_d,
       MIN(target)              AS min_target_d,
       MAX(target)              AS max_target_d
  FROM forecasts
 WHERE UPPER(model_type::text) = UPPER('<MODEL>')
   AND horizon_type::text = 'DAY'
   AND code = '<station_code>'
   AND date >= '<start_date>'
   AND date <= '<end_date>';
```

`horizon_type::text = 'DAY'` filter is critical: ML writer always stores hindcast output as `horizon_type='DAY'` (L6 user-lock per `utils_ml_forecast.py:655,718`). Without filter, legacy PENTAD/DECADE rows can cause false completeness signals.

Column meanings (per `models.py:66`): `date` = issue date (when the forecast was made), `target` = forecast target date (the future date being predicted).

**Acceptance (ALL must hold post-hindcast; otherwise refuse to advance to next iteration):**

1. **PRIMARY (row-count, horizon-aware)**:
   ```
   rows_after >= floor(days_in_range × forecast_horizon × (1 - max_gap_fraction))
   ```
   where `forecast_horizon = 6` (PENTAD) or `11` (DECAD), and `days_in_range = (end_date - start_date + 1)`.

2. **SECONDARY (write-detection)**:
   ```
   rows_after > rows_before
   ```

3. **EDGE (issue date coverage at both ends)**:
   ```
   min_issue_d <= start_date + edge_tolerance_days
   max_issue_d >= end_date - edge_tolerance_days
   ```

4. **EDGE (target date coverage at end)**:
   ```
   max_target_d >= end_date + forecast_horizon_days - edge_tolerance_days
   ```
   where `forecast_horizon_days = 6` (PENTAD) or `11` (DECAD).

`edge_tolerance_days` = `$ieasyhydroforecast_ml_hindcast_edge_tolerance_days` (env var, default `2`). The 2d default is small enough to keep PENTAD's `max_target = end_date + 6d` distinct from DECAD's required `end_date + 11d - 2d = end_date + 9d`, so the DECAD pre-check correctly refuses to skip after a PENTAD-only run.

---

## Worked example — PENTAD then DECAD on same (model, station)

For a 1-year hindcast on 1 station:
- After PENTAD: `6 × 365 = 2,190` rows; 365 distinct issue dates; max_target ≈ `end_date + 6d`
- After DECAD: `11 × 365 = 4,015` rows total; still 365 distinct issue dates (DECAD's first 6 target offsets upsert against PENTAD's); only the 5 additional offsets 7–11 produce new rows → **+5 × 365 = +1,825 new rows**; max_target ≈ `end_date + 11d`

`COUNT(*)` catches this increase (2,190 → 4,015). `COUNT(DISTINCT date)` does NOT (365 → 365). The PRIMARY check is row-count, which scales correctly with horizon.

---

## Idempotency pre-check (skip already-complete invocations)

BEFORE each invocation, run the verification SQL. Skip the invocation (do not call `hindcast_ML_models.py`) IF ALL hold:

```
rows_before >= floor(days_in_range × forecast_horizon × (1 - max_gap_fraction))
AND min_issue_d <= start_date + edge_tolerance_days
AND max_target_d >= end_date + forecast_horizon_days - edge_tolerance_days
```

This is horizon-aware: PENTAD's `max_target ≈ end_date + 6d` correctly fails the DECAD pre-check's `>= end_date + 11d - 2d = end_date + 9d`, so DECAD will not be incorrectly skipped after a PENTAD-only run.

Log "already complete; no work needed" when skipping; continue to next (model, horizon, station).

---

## Optional global reaggregation (`--reaggregate`)

Default OFF. If set, after the triple-loop completes, invoke `apps/machine_learning/reaggregate_day_to_periods.py`.

**WARNING** — to be repeated in both the `--help` and the gi_draft body:

> `--reaggregate` invokes a GLOBAL reaggregation script that:
> - Has NO filters by date, model, or station (`reaggregate_day_to_periods.py:24`)
> - Has a HARDCODED model list and DB/container path (`reaggregate_day_to_periods.py:325`)
> - Reaggregates the ENTIRE `forecasts` table, not just what this wrapper just hindcasted
>
> Use only when a full-table reaggregation is intended. Otherwise leave off
> and invoke `reaggregate_day_to_periods.py` manually with awareness of its
> side effects.
>
> Scoped reaggregation (date/model/station filters) is a potential
> follow-up — call it MIG-006 if it materializes.

---

## Failure modes

| Failure | Severity | Behavior |
|---|---|---|
| Missing scaler dir or any of the 5 expected files | Fatal | Refuse to start; clear error + path + missing-file list |
| Forcing data gap > `max_gap_fraction` for a station | Per-station fatal (warn+continue if `--continue-on-error`) | Skip station; do not invoke `hindcast_ML_models.py` for it |
| `hindcast_ML_models.py` exits non-zero | Per-(model, horizon, station) fatal | Refuse to advance; report exit code |
| PRIMARY row-count check fails post-invocation | Per-iteration fatal | Refuse to advance; report actual vs expected counts |
| SECONDARY write-detection fails | Per-iteration fatal | Refuse to advance; suggests silent API write failure |
| EDGE issue-date or target-date check fails | Per-iteration fatal | Refuse to advance; report which edge and how far off |
| Idempotency pre-check passes (already complete) | Skip silently | Log "already complete"; continue to next iteration |

---

## Wall-clock expectation

- Per (model × horizon × station) hindcast: minutes to hours (Darts model time, depends on station's meteo coverage)
- For 3 models × 2 horizons × 50 stations = 300 iterations: typically **several hours** to **a day** total
- `--reaggregate`: additional ~30 min (global table-wide reaggregation)

---

## Acceptance criteria (for the future implementation PR)

- [ ] `bin/initialize_ml_hindcast.sh` exists with the CLI signature above
- [ ] All three nested loops iterate correctly with explicit `ieasyhydroforecast_NEW_STATIONS=<code>` per invocation
- [ ] Date policy precedence works as specified; fail-fast on missing start_date
- [ ] Asset preflight checks 5 files per (model, horizon) plus the static features file
- [ ] Forcing coverage preflight uses the correct widened window per horizon (PENTAD: end+6d, DECAD: end+11d)
- [ ] Write verification SQL uses `horizon_type::text='DAY'` filter
- [ ] PRIMARY (row count), SECONDARY (write-detection), and both EDGE checks (issue + target) all pass on a clean invocation
- [ ] Idempotency pre-check correctly distinguishes PENTAD-only from PENTAD+DECAD
- [ ] `--reaggregate` is OFF by default; help text + docs include the global-side-effect warning
- [ ] No real station codes in code, comments, or tests
- [ ] No edits to `sapphire/services/`
- [ ] Tests follow the project pattern (sentinel `19999`, fake psql via PATH injection where helpful)

---

## Rollout

Small PR off `maxat_sapphire_2`. Branch: `feature_mig004_ml_hindcast_wrapper`. Implementation effort estimate: 2-3 days for the wrapper + tests + a first dry-run validation against a dev stack.

---

## Out of scope

- Scoped reaggregation (filters by date/model/station) — separate follow-up, possibly MIG-006
- Ensemble models beyond TFT/TiDE/TSMixer — separate concern (existing maintenance scripts handle ENSEMBLE_MEAN etc.)
- Migration of legacy ML hindcast output formats — operator can clean those manually with awareness
- Modifications to `hindcast_ML_models.py` itself — wrapper should treat it as an immutable dependency

---

## Process note

This gi_draft went through 3 reviewer rounds (2026-06-10) before filing. Round-1 found 7 NEEDS REVISION items (station iteration, idempotency, SQL filter, asset path, forcing window, CLI, reaggregation scope). Round-2 narrowed to 3 (cadence, asset DECAD modifier, static features missing). Round-3 returned APPROVE WITH REVISIONS — single wording cleanup on the forcing-window SQL ambiguity. The forcing-window SQL is now split into two explicit per-horizon variants (above).

Recommended follow-up for future spec work: when designing a wrapper around an existing script, read the actual script's env-var contract + asset load paths BEFORE writing the spec — round-1 and round-2's findings were both factual mismatches with the source.
