# High priority: DECADE skill metrics under-paired (n_pairs starved) despite full forecast history

**Status:** Draft — observed during the Tajik (tjhm) historical backfill on
2026-06-17. **VERIFIED 2026-07-14: this is a DATA-COMPLETENESS problem, not a code defect.**

> ## ⚠️ Do NOT "fix the pairing". A proposed root cause was investigated and REFUTED.
>
> An out-of-loop review proposed: *"skill pairing merges on raw `[code, date]`, but a forecast's
> `date` is the ISSUE date while the observation sits on the TARGET period, so pairs only survive
> where they coincide."* This was checked against both code and data and is **WRONG**.
> **Implementing it would have BROKEN the pentad skill that currently works.**
>
> **Refutation 1 — the code. Both sides of the merge are ISSUE-date indexed, so the join is correct.**
> `runoffs.discharge` (= `discharge_avg`) is built in
> `apps/iEasyHydroForecast/forecast_library.py:790-855`: the frame is **reversed**
> (`data_df.iloc[::-1]`), shifted, then rolled — so `discharge_avg` at date D is the mean of the
> **NEXT** 5 days (the forward-looking upcoming-period average). The final line,
> `data_df.loc[~data_df["issue_date"], "discharge_avg"] = np.nan`, **nulls it on every non-issue
> date**. So an observed row is keyed by ISSUE date and *already carries the TARGET period's mean*.
> The forecast is keyed by issue date too. `pd.merge(..., on=["code","date"])`
> (`skill_metrics.py:2030`) is therefore **correct by construction** — there is no issue/target
> misalignment to fix.
>
> **Refutation 2 — the data. The DECADE code path pairs fine; the starvation does not reproduce.**
> In the local DB, DECADE skill is as healthy as PENTAD (`NEURAL_ENSEMBLE`: DECADE `max_n_pairs`=17,
> `avg`≈13; PENTAD `max`=17, `avg`≈13). Day-of-month distributions agree on both sides
> (10 / 20 / end-of-month). So this is **not** a decade-specific algorithm defect.
>
> ## Actual cause (high confidence): the TAJIK ML FORECAST ARCHIVE IS SPARSE
>
> The observations are abundant; the **forecasts** are missing. Local DB, aggregate only:
>
> | Org prefix | Horizon | Stations | Forecast rows (ML) |
> |------------|---------|----------|--------------------|
> | 15 (kyg)   | PENTAD  | 27       | 103,877            |
> | 16 (kyg)   | PENTAD  | 27       | 125,297            |
> | **17 (taj)** | PENTAD | 16      | **4,984**          |
> | **17 (taj)** | DECADE | 16      | **2,566**          |
>
> Tajik is **~12× thinner per station** than Kyrgyz. Meanwhile Tajik *observed* DECADE runoff is
> healthy (35,900 non-null rows, 87 years). `n_pairs` starves because **there are barely any Tajik
> forecasts to pair against** — not because pairing is broken.
>
> This matches the operational history: the observation was made **mid-backfill** on the Tajik server
> (2026-06-17), that backfill was **interrupted** by a server restart, and pentad ML skill
> "recovered" while decade did not — i.e. the decade half of the ML archive migration never
> completed. See [[ml_fromfile_combinedforecast_migration]].
>
> ## What to do instead
>
> 1. **Do NOT change `calculate_skill_metrics`.** The merge is correct.
> 2. **Complete the Tajik ML decade forecast archive** (the from-file combined-CSV migration), then
>    re-run the skill recalc and re-measure `n_pairs`.
> 3. **Re-verify on the Tajik SERVER, not locally** — local does not reproduce. Measure decade
>    forecast rows/station on tjhm before and after.
> 4. Until the archive is complete, the PR #411 min-n gate will correctly suppress the thin decade
>    rows. That is the gate working as designed, not a second bug.
>
> **Remaining uncertainty (be honest):** the local DB does not reproduce the server symptom, so the
> sparse-archive explanation is inferred, not directly observed on tjhm. The server measurement in
> step 3 is what would close this out.

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
