# PP-062 — a clean monthly tier aborts the whole long-term maintenance run before quarterly/seasonal execute

**Status**: Draft
**Module**: postprocessing_forecasts (`postprocessing_maintenance_long_term.py`)
**Priority**: High
**Labels**: `reliability`, `long-term`, `control-flow`, `silent-success`
**Found**: 2026-09-10, during the long-term recovery runbook review; filed 2026-09-17. Source-only —
no database measurements are used in this issue.
**Related**: **PP-061** (the aggregated writer's `flag=0` clobber — a downstream write-correctness
concern, not this one), **PP-063** (refreshing an aggregate whose membership changed), **PP-041**
(invalidating an aggregate regeneration no longer emits). This issue is upstream of all three: they
describe what happens to quarterly/seasonal rows once written; this one is about whether the
quarterly/seasonal code runs at all in a given invocation.

---

## Summary

`postprocessing_maintenance_long_term()` — the entry point, defined at
`apps/postprocessing_forecasts/postprocessing_maintenance_long_term.py:89` and called directly from
the `__main__` block (`:559-560`; **there is no `main()` function**) — processes the monthly gap-fill
tier first. Six separate conditions inside that monthly tier call `sys.exit(0)`, ending the whole
Python process, before the function ever reaches the quarterly block (starts `:291`) or the seasonal
block (starts `:390`). In a single invocation where the monthly tier hits any one of the six, the
quarterly and seasonal gap-fill code is never reached — not skipped-and-logged, simply not executed.

## The six monthly-tier early exits (verified at `89a6ffc7`, still accurate at the branch's current base `4fe3e545` — the cited source file is unchanged between the two commits)

All six exit via `sys.exit(0)` after only writing a timing summary (`_print_timing()`), and all six
occur strictly before the quarterly block begins at `:291`:

1. `combined.empty` — no monthly combined forecasts read at all: `:110-113`, exit at `:113`.
2. `gaps.empty` — gap detector found no missing monthly ensemble rows: `:123-126`, exit at `:126`.
3. `skill_stats.empty` — no monthly skill metrics available: `:138-141`, exit at `:141`.
4. `all_forecasts.empty` — no monthly forecasts for the detected gap years: `:156-159`, exit at
   `:159`.
5. `filtered.empty` — none of the read forecasts match the specific gap tuples: `:181-184`, exit at
   `:184`.
6. `new_ensemble.empty` — the ensemble calculation produced no row that maps back to an actual gap
   key: `:254-257`, exit at `:257`.

Condition 2 is the "clean monthly tier" case named in this issue's title — nothing was wrong,
monthly simply had no gaps — but all six have the identical effect on the tiers below them.

**The final exit is not evidence against this.** At the end of the function (`:535-542`): if
`errors` (populated only by a failed monthly-save at `:282-287`) is non-empty, `sys.exit(1)`
(`:539`); otherwise `sys.exit(0)` (`:542`). That path is reached only when none of the six early
exits fired — i.e., only on a run where quarterly and seasonal have *already* executed. It says
nothing about runs that took an early exit.

**The zero-exit claim is scoped to these six conditions, not universal.** The function has no
enclosing `try`/`except`; an uncaught exception from any reader, gap-detector, ensemble-calculator,
or writer call (monthly, quarterly, or seasonal) propagates and the process exits non-zero via
Python's default unhandled-exception behavior. Do not read this issue as "the script exits 0 no
matter what" — only the six enumerated conditions, plus the graceful-completion path with no
collected errors, exit 0.

## Production invocation path (verified)

- `apps/pipeline/pipeline_docker.py`: `LongTermPostProcessingMaintenance` (class at `:1946`) runs a
  `sapphire-postprocessing` Docker container with
  `command=["uv", "run", "postprocessing_maintenance_long_term.py"]` (`:1972`).
- `RunPeriodicMaintenanceWorkflow` (`:2050`)'s `task_map` (`:2076-2079`) maps
  `task_type == "long_term"` to `LongTermPostProcessingMaintenance()`.
- `bin/run_periodic_maintenance.sh` lists `long_term` in `VALID_TASK_TYPES` (`:58`) and documents it
  as "Bimonthly long-term postprocessing (1st of odd months)" (`:9`).
- `doc/deployment.md` cron entry (6) (`:1001`):
  `0 22 1 1,3,5,7,9,11 * cd ... && bash bin/run_periodic_maintenance.sh long_term ...` — 22:00 on the
  1st of every odd month. This is the **scheduled production path**.

**A second invocation path exists.** `bin/bimonthly_long_term_postprocessing.sh` also runs
`postprocessing_maintenance_long_term.py` directly: its `run_container` call for the maintenance
branch (`MODE` defaults to `both`, which includes maintenance) launches the same script in a
`postprc-lt-maintenance` container. `bin/README.md` marks this script `[Legacy]` — "superseded by
... `run_periodic_maintenance.sh` for automated cron scheduling" but "still functional for manual
invocation and debugging" — and it is not wired into any tracked cron entry (see the cross-referenced
`doc/plans/observations.md` note on this wrapper's own exit-status and container-naming issues). It
does not weaken this defect: it only broadens reachability beyond the single scheduled path, since a
manual run through either script hits the identical six early exits in the same underlying function.

Confirmed reachable defect: a run on either path whose monthly tier hits any of the six conditions
above completes that run without executing quarterly or seasonal gap-fill.

## What this issue does not claim

- **Not "every deployment".** INFRA-047's archived crontab survey
  (`doc/plans/issues/archive/review_gi_draft_infra_canonical_cron_wrappers_exit_zero.md`) records
  that **uzhm has no long-term cron row at all** — uzhm runs linear regression only. The exposure
  described here applies to deployments that install this cron entry (kghm and tjhm, per that
  survey), not to every deployment of the codebase.
- **Not "quarterly/seasonal have very likely never run".** This is a code-reachability defect
  established from the source; it makes no claim about execution history on any server, and no log
  evidence is attached to support one. The defect stands on the control flow alone.
- **Not "the script exits 0 throughout".** See the six enumerated conditions above and the final
  `sys.exit(1)`/raise paths. The claim is scoped to those six conditions, not stated as a blanket
  property of the script.

## Why this is structurally worse than a wrapper exit-code bug

The `sys.exit(0)` at each of the six conditions is not a lie — the process genuinely completed
without error, because the monthly tier genuinely had nothing to do (or, for a real gap, could not
proceed). That is exactly what makes it undetectable from outside: there is no incorrect exit code
to propagate correctly. A cron wrapper that faithfully reports Luigi's own retcode (the fix shape
already shipped for other wrappers, e.g. INFRA-023/INFRA-047) still reports success here, because
the run *is* a success by the process's own accounting — it just did strictly less work than the
tier boundaries below monthly suggest it should have attempted. Exit status is structurally
incapable of distinguishing "quarterly/seasonal ran and found nothing to fill" from "quarterly/
seasonal never got a chance to run this invocation." Only comparing the specific rows a tier should
hold against what is actually present — row-level verification, not process-level status — can catch
this.

## No per-tier entry point

The script takes no mode or date argument: there is no `argparse`/`ArgumentParser`/`sys.argv`
handling anywhere in the file (confirmed by inspection). Its only external inputs are the three
gap-fill lookback window env vars and the two station-config path env vars used in
`_read_station_codes()` (`:76-86`). There is no way — short of editing the script or hand-invoking
its internals — to ask it to run only the quarterly or seasonal tier, to repair one recovered mode
in isolation, or to work around a monthly-tier abort by re-running just the skipped tiers.

## Configuration referenced (defaults, all independently overridable)

- `POSTPROCESSING_GAPFILL_WINDOW_MONTHS` — monthly lookback, default `3` (`:96`).
- `POSTPROCESSING_GAPFILL_WINDOW_QUARTERS` — quarterly lookback, default `2` (`:294`).
- `POSTPROCESSING_GAPFILL_WINDOW_SEASONS` — seasonal lookback, default `1` (`:393`).

All three are listed as optional environment variables for `postprocessing_forecasts` in
`doc/configuration.md` (around `:268-271`); none has its numeric default documented there, so the
defaults above are read from the code, not the doc. The three windows are independent quantities in
different units (months vs. quarters vs. seasons) — a fixture must construct quarterly/seasonal gaps
inside their own windows, not reuse the monthly window's boundary.

## Existing behavior this issue does not own

**Incidental refresh already happens today, independently of this defect and of PP-061.** When the
quarterly block *is* reached and produces `q_new` rows, the merge step
(`q_merged.drop_duplicates(subset=q_dedup_subset, keep="last")`, `:372-375`) dedupes on
`(year, quarter_in_year, code, model_short)` — plus `horizon_value` only when the lead-aware flag is
on (`:370-371`) — with `keep="last"` favoring the freshly generated row. So an existing row sharing
that key with a regenerated gap-fill row is already overwritten today, with no dependency on this
issue's fix. Do not read PP-062 as implying the quarterly/seasonal tiers are inert or broken when
they do execute — they run and write correctly when reached; the defect here is purely that the
monthly early exits can prevent them from being reached at all in a given invocation.

**Any requirement that pre-existing rows be preserved once the tiers become reliably reachable
belongs to PP-061 and PP-063, not to this issue.** PP-061 owns the `flag=0` clobber on collision;
PP-063 owns refreshing an aggregate whose membership changed. This issue is fixed when quarterly and
seasonal execute in the same invocation as monthly; it does not extend to auditing what they write
once they do.

## Acceptance criteria — scoped to tier reachability only

Write correctness (whether existing rows are preserved or correctly refreshed) is explicitly out of
scope here and assigned to PP-061/PP-063 above. This issue's acceptance is only:

- A deterministic fixture set with one fixture per early-exit condition (all six from the list
  above), each independently constructing:
  - the specific monthly-tier emptiness needed to trigger that exact condition, and
  - genuinely fillable quarterly and seasonal gaps, inside their own lookback windows (2 quarters /
    1 season by default, both independently configurable, and both distinct from the monthly
    window used to trigger the fixture's early exit).
- "Genuinely fillable" means the fixture supplies: usable member forecast values for the targeted
  quarterly/seasonal gap periods, skill metrics for the quarterly and seasonal tiers
  (`data_reader.read_skill_metrics("quarter"/"season", ...)`), and — for the seasonal fixtures — at
  least one seasonal issue lead present in `_supported_seasonal_issue_leads()` (`:64-73`) for the
  fixture's configured long-term modes.
- After the fix, for each of the six fixtures: assert the quarterly block (`:291` onward) and
  seasonal block (`:390` onward) both execute in the same run, and that the specific missing
  `(year, quarter_in_year/season_in_year, code, model_short[, horizon_value])` keys constructed by
  the fixture are filled with usable values. Assert on the specific keys filled, not on an aggregate
  row count (a count can rise through unrelated duplication without proving the targeted gap was
  actually closed).
- A control fixture confirms current behavior is preserved for the non-defective path: when the
  monthly tier has genuine work and completes normally (no early exit), quarterly and seasonal
  already execute today — the fix must not change that path's outcome.

## Proposed direction (not decided here)

Replace each of the six `sys.exit(0)` calls in the monthly tier with an early `return`/`continue`
of the monthly section only, letting execution fall through to the quarterly and seasonal blocks
regardless of the monthly outcome. Consolidate the process-level exit decision (0 vs. 1) to one
place at the end of the function, based on accumulated errors across all three tiers rather than
only the monthly-save error list. Adding a mode/date parameter for per-tier invocation (see "No
per-tier entry point" above) is a related but separate enhancement — this issue records the absence
and leaves the decision of whether to add one to the owner.
