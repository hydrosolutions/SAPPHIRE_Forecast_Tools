# PP-048 — DECADE `ENSEMBLE_MEAN` can freeze far behind its own per-model decade

**Status**: Draft (investigation — root cause not fully pinned)
**Module**: postprocessing_forecasts
**Priority**: Low (an observed instance was healed by the PP-045 backfill)
**Labels**: `postprocessing`, `investigation`

---

## Summary

On the Tajik dev DB, DECADE `ENSEMBLE_MEAN` (EM) was observed frozen at
`2026-04-10` while the per-model DECADE rows (TFT/TIDE/TSMIXER) and
`NEURAL_ENSEMBLE` had advanced to `2026-06-30`. The exact mechanism of the EM
freeze is **not confirmed** and needs investigation.

## Evidence / observations

- BEFORE PP-045 backfill (2026-07-23): DECADE per-model max `2026-06-30`, DECADE
  `ENSEMBLE_MEAN` max `2026-04-10`.
- Running the PP-045 backfill for 2026 **healed** it: DECADE `ENSEMBLE_MEAN`
  advanced to `2026-07-10` alongside the per-model rows — i.e. decade skill
  metrics *were* present at run time, so a naive "decade skill metrics are empty"
  explanation is **not** sufficient on its own.
- EM creation is gated on non-empty skill metrics in both paths
  (`postprocessing_operational.py:142-148`, `postprocessing_maintenance.py:348-353`);
  `read_skill_metrics` returns empty (not error) when API+CSV both lack rows
  (`src/data_reader.py:432-457`).

## Open questions (the investigation)

1. Why did per-model DECADE advance (to 06-30) while EM lagged (04-10), if decade
   skill was present? Candidates: (a) EM was skipped for the 05-xx/06-xx decade
   boundaries at the time they were first produced (transient skill-empty window),
   and maintenance never backfilled EM because its universe is bounded by
   `combined` (the same class as PP-045); (b) an EM-specific creation/skill-join
   gap distinct from the boundary mechanism.
2. Is this the same boundary-gap mechanism as PP-045 (in which case the PP-045
   backfill is the recovery tool and no new code is needed), or a separate
   EM/skill interaction that will re-freeze operationally?

## Proposed direction

Investigate read-only on a DB where the freeze is reproduced (confirm whether
decade `read_skill_metrics` was empty during the frozen window). If it is purely
the PP-045 boundary mechanism, close this as "recovered by the backfill" and add
a note to the operational runbook. If EM has an independent skip path, scope a
targeted fix.

## Out of scope / notes

- No `sapphire/services/` change anticipated.
- Do not commit real station codes / discharge values (placeholder `19999`).

## References

- Split from PP-045 (review_gi_draft_pp_missed_boundary_period_gap.md, secondary
  anomaly section). Related project memory:
  `taj_monthly_bulletin_empty_lastyear_runoff` / snow/skill notes.
