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
> ## ✅ OWNER DECISION 2026-07-14 — close this by ENABLING the flag, not by patching the flag-off path.
>
> The product semantics decide it: **for MONTHLY forecasts, Kyrgyz Hydromet chooses which lead goes
> into the bulletin.** (Quarterly is different — we publish only the lowest available lead, so it
> needs no per-lead skill.) If the operator can *choose* the monthly lead, then a single collapsed
> skill number is structurally wrong: when they publish lead 1, the skill shown must be **lead 1's**
> skill. Skill must be computed **per lead**, with the display selecting to match the published
> forecast — which is exactly what the lead-aware path already does.
>
> So: **enable `SAPPHIRE_SKILL_LEAD_AWARE`** (requires a full skill recalc —
> `doc/prod/long_term_deploy_runbook.md:57`). Do **not** patch the flag-off path: it would change
> golden behavior locked by tests, and it would still deliver one number for a product whose lead is
> operator-selectable. The flag-on path *is* the requirement, not a nice-to-have.
>
> **Sequencing — CORRECTED 2026-07-14.** An earlier revision of this file claimed **PP-043 must be
> fixed before enabling the flag**. **That was wrong, and is retracted.** PP-043 concerns
> *short-term* (pentad/decade) skill, whereas this flag governs *long-term monthly* skill — largely
> independent code paths. And PP-043 turned out **not to be a code defect at all** (the pairing is
> correct; the Tajik ML forecast archive is simply sparse). **PP-043 does not block this.**
>
> What *does* still apply: enabling the flag requires a **full long-term skill recalc**, and the
> min-n gate (PR #411) will suppress any lead whose sample is too thin — so confirm the monthly
> archive is adequately populated for the deployment **before** enabling, or tiles will render empty
> for legitimate (gate-working-as-designed) reasons.
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

> **RE-VERIFIED 2026-07-15 against the merged `maxat_sapphire_2` tree (post PR #416 lead-aware
> convergence). Conclusion holds; mechanism/line-refs below updated — the original `_op_lead`
> description was pre-#416 and is stale.** Current state, `apps/forecast_dashboard/src/db.py`:
> - **Flag OFF (`SAPPHIRE_SKILL_LEAD_AWARE`, the default):** `_get_data_monthly` filters
>   `forecast_stats` to `horizon_value == 1` (`db.py:1001-1004`), the `month_0` block fetches the
>   forecast at `m0_lead = 0` (`db.py:1064-1065`), then merges it against that **lead-1-filtered**
>   `forecast_stats` on `merge_keys = ["code", "month_in_year", "model_short"]` (`db.py:973`,
>   `:1073-1078`) — **no lead in the key.** Net: the lead-0 month_0 forecast is annotated with lead-1
>   skill. **Bug confirmed present on the default path.**
> - **Flag ON:** `merge_keys` gains `horizon_value` (`db.py:992`) and `m0_lead` resolves to month_0's
>   own lead, so the merge is per-lead and correct. **Origin's M1 P3 work already fixes this under the
>   flag.**
>
> **So FD-021 is another symptom cured by the already-decided "enable `SAPPHIRE_SKILL_LEAD_AWARE`"**
> (see the OWNER DECISION banner above). No separate flag-OFF patch is needed if the flag is enabled;
> if any deployment must stay flag-OFF, then the fix below applies to that path only.

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
