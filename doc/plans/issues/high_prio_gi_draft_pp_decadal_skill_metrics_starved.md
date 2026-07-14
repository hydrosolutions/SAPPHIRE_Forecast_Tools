# High priority: DECADE skill metrics under-paired (n_pairs starved) despite full forecast history

**Status:** Draft — observed during the Tajik (tjhm) historical backfill on
2026-06-17. Captured for later investigation and refinement; root cause not yet
confirmed.

## Problem

After backfilling the full historical ML pentad/decade forecast archive into the
postprocessing DB (TFT/TiDE/TSMixer/EM/NE, 2010→2026) and recalculating skill
metrics, **DECADE skill metrics are starved**: ML decade `n_pairs` maxes out at
**1**, even though the decade forecasts and the observed decade runoff both span
the full historical range. PENTAD skill recovered correctly on the same run, so
this is decade-specific.

## Evidence (Tajik, 2026-06-17, aggregate only)

Skill recalc was run two independent ways — `bin/initialize_site_backfill.sh
--skip-preprunoff --skip-linreg` **and** the operational
`bin/run_periodic_maintenance.sh skill_recalc` — with identical results:

```
horizon | model_type        | rows | min_np | max_np
--------+-------------------+------+--------+-------
PENTAD  | LINEAR_REGRESSION | 1224 |   8    |  21     <- healthy
PENTAD  | NEURAL_ENSEMBLE   | 1080 |   4    |  16     <- recovered (was 0 at P0)
PENTAD  | TFT/TIDE/TSMIXER  | ~1080|   0-4  |  16     <- recovered
DECADE  | LINEAR_REGRESSION |  577 |   0    |   6     <- low even for LR
DECADE  | NEURAL_ENSEMBLE   |  540 |   0    |   1     <- STARVED
DECADE  | TFT/TIDE/TSMIXER  | ~540 |   0    |   1     <- STARVED
```

Confirmed the underlying data IS present (so this is a pairing/compute issue, not
missing data):

- `forecasts` DECADE archive: TFT/TIDE/TSMIXER/ENSEMBLE_MEAN/NEURAL_ENSEMBLE,
  ~8,550 rows/model, spanning 2010→2026.
- `runoffs` DECADE observations: present across the full historical range.

## Key observations / scope

- **Decade-specific.** PENTAD ML skill recovered to `n_pairs=16` on the same
  recalc; only DECADE under-pairs.
- **Not wrapper-specific.** Two different recalc entrypoints give the same
  `max_np=1` → points to the skill computation/read path, not the invocation.
- **LR decade is also weak** (`max_np=6` vs LR pentad `21`), so there may be a
  general decade-pairing limitation, with ML decade hit hardest (`max_np=1`).
- **Not a data gap.** Decade forecasts + observations both exist for the full
  range; the dashboard still shows decade ML *forecasts* — only the decade skill
  *metric* is starved.

## Hypotheses to investigate (unconfirmed)

1. **ML archive read short-circuit** — the `ml_forecast_horizon_archive_split`
   class issue (DAY archive short-circuits the pentad/decade read). The
   read-both-archives fix evidently works for PENTAD but the DECADE path may still
   under-read or short-circuit.
2. **Target/period alignment** — the combined-forecast migrator sets
   `target = date + 1` for all horizons; decade skill pairing may expect the
   10-day decad boundary, matching only a single period and yielding `n_pairs=1`.
3. **`decad_in_year` / `horizon_in_year` grouping** — if the skill metric groups
   by `horizon_in_year` and the migrated decade rows carry inconsistent
   `decad_in_year`, the per-period cross-year pairing would collapse to 1.
4. **Decade observation join** — how decade skill joins observed decade discharge
   vs how pentad does; LR decade also being low suggests a shared decade-join
   weakness.

## Investigation pointers

- Skill computation: `apps/postprocessing_forecasts/.../recalculate_skill_metrics.py`
  and `skill_metrics.py` (compare PENTAD vs DECADE code paths and the pairing/
  group-by keys).
- The ML forecast archive reader (`_read_ml_forecasts_pp_api`) — verify the DECADE
  branch reads the full archive, not just DAY.
- Inspect a sample of migrated DECADE forecast rows: `date`, `target`,
  `horizon_value`, `horizon_in_year` — confirm they are consistent across years
  for a given decad-in-year (use sentinel/redacted codes; never real station
  codes).

## Acceptance criteria (for the eventual fix)

1. DECADE ML skill `n_pairs` reaches parity with available history (comparable to
   PENTAD, ~10–16), for TFT/TIDE/TSMIXER/NEURAL_ENSEMBLE.
2. DECADE LR skill `n_pairs` improves to reflect the available history (root-cause
   the LR decade `max=6` vs pentad `21`).
3. Verified via the acceptance SQL above on a deployment with a populated decade
   archive (filter `horizon_type::text='DECADE'`, stored enum **names**).

## Related

- `ml_forecast_horizon_archive_split` (memory) — DAY vs PENTAD/DECADE archive
  read short-circuit.
- `review_gi_draft_pp_skill_metric_dedup.md` (PP-035), `review_gi_draft_pp_monthly_skill_q50_regression.md`
  (PP-028b) — other skill-metric correctness issues.
- Surfaced during `project_tajik_historical_backfill_2026-06`; the ML archive that
  exposed it was migrated per `ml_fromfile_combinedforecast_migration` (memory).

## Non-goals

- Not blocking the Tajik deployment: decade ML *forecasts* are present and shown;
  only the decade skill tile is weak (and LR decade skill was already weak).
