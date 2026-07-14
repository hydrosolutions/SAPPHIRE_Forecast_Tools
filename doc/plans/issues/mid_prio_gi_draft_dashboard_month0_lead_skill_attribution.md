# Dashboard: month_0 forecasts display lead-1 skill (wrong lead attribution)

**Priority:** mid (display-correctness; pre-existing, not a regression).
**Module:** `apps/forecast_dashboard` (fair game).
**Found:** 2026-07-08, during the P2/P1 min-n + stale-aggregate review (branch
`feature_lt_skill_min_n_stale`). Line numbers from `f6a9c040`/`b77fd8c7` — indicative,
confirm at implementation. Present on `maxat_sapphire_2` too (this code path is untouched
by the min-n/stale branch).

## Summary

In the monthly dashboard view, the **month_0 (in-month, lead 0)** forecasts are annotated
with **lead-1** skill metrics, not the lead-0 skill for the same model/month.

Mechanism:
- `get_long_forecasts(..., horizon_value=0)` fetches lead-0 forecast rows but **drops
  `horizon_value`** before returning (`db.py:~637`, `~665`).
- `_get_data_monthly` filters `forecast_stats` to `_op_lead == 1` (`db.py:~880`), then
  **reuses that same lead-1-filtered frame for the month_0 block** (`db.py:~943`), merging
  only on `["code", "month_in_year", "model_short"]` (`db.py:~951`) — with no
  `horizon_value` in the key.
- Net: a month_0 forecast row is joined to the lead-1 skill row for that model/month.

This is independent of the min-n / stale-tombstone work: month_0 has always consumed the
lead-1 skill frame, so it never used lead-0 skill regardless of tombstoning. (The reviewer
noted a suppressed lead-0 tombstone + a real lead-1 row would surface lead-1 skill on the
lead-0 forecast — but that attribution predates and is orthogonal to tombstones.)

## Impact
Operational monthly bulletins/dashboard may present a lead-0 (in-month) forecast alongside a
skill number computed for a *different* lead (lead 1). Misleading, though not a data-write
corruption.

## Proposed fix
Either:
1. Preserve `horizon_value` through `get_long_forecasts` and include it in the monthly merge
   keys, so month_0 (hv=0) matches hv=0 skill; or
2. Build separate `forecast_stats_hv0` / `forecast_stats_hv1` frames and merge the month_0
   block only against `horizon_value == 0` skill.

Add a regression test: a model/month with a lead-0 real skill row and a *different* lead-1
skill row → the month_0 forecast is annotated with the lead-0 value, not lead-1. Use
placeholder code `19999`.

## Notes
Relates to the `get_forecast_stats_all` `horizon_value`-dedup fix already landed on
`feature_lt_skill_min_n_stale` (that fixed collapsing distinct leads in the *all-stats*
reader; this issue is the *month_0 merge-key* attribution, a separate site).
