# PP-073 — Short-term EM gap-fill leaves stale and unwritten EM rows

**Status:** draft, found 2026-11-12 while verifying PP-072 P1 on the dev DB. **Module:** `apps/postprocessing_forecasts`.
Not caused by PP-072; pre-existing gap-fill behaviour. No code changed. Findings come from read-only agent runs; items marked *inferred* were not verified.

## Observation (dev DB, after a `maintenance:postprocessing_forecasts` run, mode BOTH)

A read-only replay of `create_ensemble_forecasts` found missing stored EM rows and
composition differences between replayed and existing rows. Some existing rows lacked
LR although replay included it. This suggests those rows may have been written before
LR became available; that explanation remains inferred.

## Why it persists (verified by reading code)

- `gap_detector.detect_missing_ensembles` treats a (date, code) as a gap only if no **non-null** EM row exists.
- Stale refresh (`postprocessing_maintenance.py:~2199-2209`) flags EM rows only when **q05 is null**. An old EM row with all bands filled is never refreshed on its own.
- Existing rows are rewritten only when their code and date fall in the gap-code × affected-date cross product (`:~2267`); upsert key code, model_type, date, in-year index, horizon_value (`api_writer.py:~682`).
- The write sink drops rows with null discharge. Whether this explains the missing
  rows has not been established.

## Not established

The maintenance run reads a narrower, scoped slice than the replay (affected dates,
gap codes). Suggested read-only reproduction: inspect `_fill_gaps_for_horizon` through
the `create_ensemble_forecasts` call and check whether missing pairs have finite discharge.

## Finding 2026-11-12: old EM rows are re-upserted on every run (found testing PP-072 with run_locally)

A before/after comparison found that existing monthly EM rows can be re-upserted
with their stored `q` normalized to `q50`, while bands and composition remain unchanged.
The relevant code paths are:

- `q` is a stored column, independent of `q50`; `forecasted_discharge` is derived from
  `q50` at read time.
- `postprocessing_operational_long_term.py` reads existing monthly combined rows,
  concatenates new rows, and re-upserts them. `recalculate_skill_metrics.py` also saves
  monthly forecast data. The writer allow-list contains EM and sets `q = q50`, so a
  missing `q50` can replace a previously populated `q` with null.
- This path predates PP-072; retaining old EM rows also permits their re-emission.
- Short-term maintenance can likewise re-write legacy rows without composition.
- Whether downstream consumers still read the legacy `q` column was not checked.

Options if wanted: filter EM out of the read-back before the merge, or stop writing EM in the monthly writer (design call).

## Legacy EM rows (context, not a defect of PP-072)

Legacy rows without composition include non-boundary dates and incomplete quantile
sets. The current creation logic cannot produce all of those historical shapes.
Their presence must not be interpreted as evidence of current creation behavior.

## Proposed scope (decide before work)

1. Read-only repro above; classify the cause.
2. Decide whether stale EM rows (no LR, or pre-PP-072) should be refreshed by the maintenance (e.g. flag EM rows older than the rule version) or by a one-off recalculation; weigh against the PP-072 rule "merge = deploy".
3. Decide whether legacy composition-None EM rows are cleaned up.

Out of scope here: PP-072 gating and LR bands.

**Priority note (2026-11-13):** not on the critical path of the EM goal (PP-075); deferred. For docs only: stale EM rows are not removed by deploy.
