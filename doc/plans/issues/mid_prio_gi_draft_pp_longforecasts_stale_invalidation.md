# PP: stale-aggregate invalidation for long_forecasts (forecast-side) — deferred P1b

**Priority:** mid (lower impact than the skill-side fix already shipped).
**Module:** `apps/postprocessing_forecasts` (+ forecast/dashboard read consumers).
**Status:** deferred follow-up to the min-n + stale-aggregate effort (branch
`feature_lt_skill_min_n_stale`). The skill-side staleness (P1) is done; this is the
forecast-side analogue (plan phase **P1b**), consciously deferred 2026-07-08 with owner
sign-off to land the high-impact P2+P1 fixes first.

## Problem
`long_forecasts` ensemble rows (EM / Skilled Mean / Naive Mean) are written upsert-only
(`api_writer.py` `write_long_forecasts` → service CRUD `crud.py:105-152`, unique key
`(horizon_type, code, model_type, date, target)`, `models.py:147-156`). When
`ensemble_calculator` discards a single-model/empty ensemble at a given `(code, date, target)`
(`ensemble_calculator.py:183-195` monthly; aggregated analogue), the prior run's ensemble
**forecast** row survives — a stale forecast value that the dashboard/bulletins still display,
even though the skill-side is now correctly tombstoned.

## Why deferred
- Impact is lower: P1 already removed the catastrophic *skill* values from the dashboard;
  this is stale *forecast values* for historical dates whose ensemble membership later changed.
- Effort is ~2× the skill side: `long_forecasts` is **date-indexed**, so invalidation needs
  its own write-side tombstone-diff over the full forecast date range *and* its own read-side
  suppression in the forecast/dashboard display (a forecast tombstone = NULL discharge/quantiles,
  which the forecast readers must suppress — distinct from the skill readers already handled).

## Proposed approach (mirror P1)
1. Write-side: after regenerating ensemble forecasts per horizon, diff emitted
   `(code, date, target, model)` keys against existing `long_forecasts` keys and upsert a
   forecast tombstone (NULL forecast/quantile values) for those no longer emitted. Reuse the
   canonical model-name normalization from P1.
2. Read-side: suppress forecast tombstones in the forecast readers and dashboard forecast
   display (NULL discharge → not plotted / not selected).
3. Tests: stale forecast key → tombstoned + suppressed; idempotency; short-term untouched.

## Reference
Plan: `doc/plans/working/skill_min_n_and_stale_aggregate_plan.md` (§P1b). Skill-side
implementation to mirror: `src/stale_tombstones.py`, the `recalculate_skill_metrics.py`
wiring, and `data_reader._drop_tombstone_rows`. Placeholder code `19999` in tests.
