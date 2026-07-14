# Dashboard: month_0 forecasts display lead-1 skill (wrong lead attribution) — flag-OFF path

**Priority:** mid (display-correctness; pre-existing, not a regression).

> **Scope correction 2026-07-14 (out-of-loop review): this bug is real, but only on the
> `SAPPHIRE_SKILL_LEAD_AWARE`-OFF path — which is the DEFAULT, i.e. every deployment that has not
> opted in.**
>
> A lead-aware path already exists in the dashboard: it preserves `horizon_value` and merges skill
> on it (`apps/forecast_dashboard/src/db.py:696`, `:720`, `:977`, `:1063`) — but it is gated on the
> `SAPPHIRE_SKILL_LEAD_AWARE` flag, which **defaults to OFF**
> (`apps/iEasyHydroForecast/skill_lead_aware_flag.py:29-60`: *"Default (unset) is OFF"*). With the
> flag off, the wrong-lead merge described below is the live behavior, and tests pin that as golden
> (`apps/forecast_dashboard/tests/test_db.py:1404`).
>
> **So there are two ways to close this, and it is an owner decision — do NOT just patch the
> flag-off path:**
> 1. **Enable `SAPPHIRE_SKILL_LEAD_AWARE` per deployment** (requires a full skill recalc — see
>    `doc/prod/long_term_deploy_runbook.md:57`). This makes the issue moot without a code change.
> 2. **Fix the flag-off path** — but that changes golden behavior locked by tests, so it needs
>    explicit sign-off.
>
> **Test scope:** the month_0 regression this draft asks for is still missing (existing tests prove
> lead-aware merging and lead-0 *month_1*, but never a month_0 row with distinct lead-0 vs lead-1
> skill values — `tests/test_db.py:1059`, `:1298`). Add it, scoped to the **flag-on** behavior.
>
> Related: the lead-aware machinery itself is described in
> `doc/plans/working/review_skill_lead_aware_plan_revised.md` (implemented; rollout pending).
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
