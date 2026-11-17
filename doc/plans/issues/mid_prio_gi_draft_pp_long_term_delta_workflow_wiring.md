# PP-075: Publish long-term delta through existing workflows

**Status:** Review — wiring implemented, independently reviewed and locally verified (2026-11-16). Full operational run is not green due to the pre-existing scheduling mismatch documented below; server deployment pending.
**Parent:** [Shared long-term observation delta](mid_prio_gi_draft_pp_shared_long_term_observation_delta.md).
**Scope:** orchestration only; reuse `sync_long_horizon_hydrograph.py`. No service, schema,
calculation, historical-accuracy, forecasting-model or dashboard changes.

## Evidence and intended behavior

The owner supplied the installed KGHM crontab on 2026-11-16. Its daily entry invokes
`bin/run_daily_maintenance.sh` at `0 19 * * *`; its yearly entry invokes
`bin/yearly_runoff_hydrograph_aggregation.sh` at `0 9 1 1 *`. Keep both commands and
numeric schedules unchanged. Do not copy deployment paths, credentials or operational data into Git.
Several comments in that crontab disagree with numeric times; interpreting them requires the
server timezone/CRON_TZ. This plan does not retime jobs or claim a live server inspection.

| Existing entry point | Current behavior | Planned behavior |
|---|---|---|
| Local `maintenance:preprocessing_runoff` | Runoff maintenance then long-horizon publication | Preserve |
| Local `maintenance`, `daily`, `initialize` | Reach the same runoff-maintenance function | Preserve, no extra publication at those call sites |
| Local `long-term-operational` | Operational runoff/weather; no long-horizon publication | Publish after successful runoff preprocessing, before weather/forecast processing and before the no-active-mode return |
| Server daily wrapper | Luigi maintenance, then unconditional frontend refresh | Luigi, long-horizon publication, then unconditional frontend refresh |
| Server yearly wrapper | Runs the same long-horizon writer | Preserve standalone behavior and January 1 job |

Local publication and long-term postprocessing maintenance succeeded on 2026-11-16.
The actual dashboard API reader verified eligible month/quarter/season delta inputs without
read errors. This is local evidence, not evidence of server deployment.

## Minimal design and contracts

1. Reuse the existing complete-payload writer: month, quarter, April–September season;
   `std/count` computed from deployment-configured 2000–2025 history, minimum 13 accepted years.
   Do not add a delta-only writer, LR carrier, endpoint, cache or new Luigi task.
2. In `apps/run_locally.sh`, narrowly extract the existing long-horizon invocation/status block
   from `run_maintenance_preprocessing_runoff` into one helper. Existing maintenance keeps its
   current logging, summary-row ownership and exit handling exactly. Reuse the helper from
   `run_long_term_operational_pipeline` after runoff succeeds. Preserve organization gates,
   dry-run behavior and `--continue-on-error`; do not publish when runoff failed, even when
   later pipeline work is permitted to continue.
3. The helper retains `RUNOFF_LONG_HORIZON_TARGET_YEAR` support. Without that explicit override,
   the writer uses its current publication year. Never derive that year from `LT_FORECAST_TODAY`:
   historical forecast reruns still need statistics in the dashboard's current-year publication.
   Existing local exits remain: 0 success; 2 no records warning; 4 informational partial norm
   failure; 6 records the existing failed sub-step without aborting maintenance; 1/3/5 fail.
   Preserve the runner's final nonzero outcome for the exit-6 failed sub-step.
   The helper saves `CURRENT_MODULE_LOG` on entry and restores that saved value on non-fatal
   paths, not a hard-coded maintenance log. On fatal paths retain the publication log until
   the owning caller records its failed row, then restore the saved context. Existing
   maintenance retains ownership of its fatal summary row; the new operational caller must
   record a publication failure row for 1/3/5 before aborting or continuing. Exit 6 keeps the
   helper-owned failed sub-step; the operational caller must not add a duplicate failure row. Later success or a no-active-mode return must never erase
   those failures. Keep org gating in the operational caller, not the shared helper.
4. Server wrapper: after capturing Luigi status, call the existing yearly wrapper once with the
   same quoted env-file argument. Attempt publication even if Luigi failed: it can use existing
   stored observations, but must not claim upstream data were refreshed. Capture publication
   status immediately. This is aggregation of returned statuses, not proof that the existing
   frontend child successfully recreated its container; inspect that outcome at rollout. Always run the existing frontend update afterward. Overall success
   requires all three stages to return zero; report each stage separately. Preserve server
   writer exits as failures (including 2/4/6), matching the existing standalone wrapper; do not
   copy local tolerated-exit semantics into cron silently.
5. Nested tunnel ownership needs a small explicit boundary. Add an optional
   `--reuse-existing-tunnel` flag to the yearly wrapper; the daily parent passes it. In that mode
   the child neither establishes a tunnel nor installs tunnel cleanup, avoiding a second setup attempt and leaving tunnel lifecycle ownership with
   the parent. Parse the flag in the existing pre-source argument loop. Private tunnel scripts
   may export PIDs or reuse existing connections; do not assume their behavior from this repo. Without the flag, standalone/yearly lifecycle is unchanged.
   No change to shared `common_functions.sh`, SSH settings, credentials or tunnel scripts.
6. Use the shared deployment env file for `SAPPHIRE_RANGE_REFERENCE_START_YEAR=2000` and
   `SAPPHIRE_RANGE_REFERENCE_END_YEAR=2025` in both producer and dashboard. Their existing env-file
   loading/mounts suffice; no Compose environment expansion. Keep reference changes coordinated
   with a complete publication; no automatic fallback or zero-filled missing statistics.

Putting the writer inside `PrepRunoffMaintenance` would be fewer lines but would make norm-lookup
failures retry runoff and block dependent LR/ML/postprocessing. Keep publication outside the Luigi
DAG to avoid that new coupling. Reusing the yearly wrapper costs one short-lived container and
retains its existing logging, mounts and exit handling. Do not add a second daily cron line.

## Implementation phases

All implementation is delegated to Sonnet 4.6 agents under CLAUDE.md. Every agent prompt includes:
“Do NOT change any existing function signatures, data flow logic, or control flow. Your changes
must be purely additive or modify only the specific behavior described.”

Every bullet must help an agent know what to inspect, what contract not to break, or what verification proves safety — otherwise cut it.

### W1 — Local runner (depends on: none; one implementation agent)

**Allowed files:** `apps/run_locally.sh`,
`apps/pipeline/tests/test_run_locally_orchestration.py`,
`apps/preprocessing_runoff/test/test_run_locally_long_horizon_wiring.py`.

**Acceptance:** real shell harness verifies runoff → one publication → weather/forecast order;
publication still occurs on a no-active-mode day, followed by the existing skip row and
success when no stage failed; org-skipped LT does not publish; failed runoff
never publishes (with and without continue-on-error); dry-run performs no writes; explicit target
publication year is forwarded. With `LT_FORECAST_TODAY` set but no publication-year override,
assert the writer receives neither `--today` nor `--target-year`. Exercise publication exits 0/1/2/3/4/5/6 through the operational path too, including
continue-on-error and no-active-mode returns. Existing maintenance routing tests
for 0/1/2/3/4/5/6, failed-step log attribution and final exit remain unchanged in meaning.
No changes to simulation/calibration targets, model scheduling or historical ensemble recomputation.

### W2 — Existing server wrapper (depends on: none; one implementation agent)

**Allowed files:** `bin/run_daily_maintenance.sh`,
`bin/yearly_runoff_hydrograph_aggregation.sh`,
`apps/pipeline/tests/test_cron_wrapper_exit_codes.py`,
one narrowly scoped test file under `apps/pipeline/tests/` if needed for yearly-wrapper CLI tests.
Extend the existing daily-maintenance cases and shared stub fixture as needed; retain other wrappers
' behavior assertions unchanged.

**Acceptance:** stubbed shell execution proves Luigi → publication → frontend ordering and exact
quoted env argument forwarding (including spaces). Scope the spaced-path check to the new
child invocation using a configuration stub; this is not a claim of end-to-end spaced-path support
in the pre-existing parent/frontend configuration readers. Test stage success/failure combinations,
publication exits 2/4/5/6 (all fail the server aggregate; no local exit-2 tolerance), and frontend execution after either earlier failure. Assert one publication
attempt per wrapper invocation. Test standalone tunnel setup/cleanup unchanged, nested mode does
neither (also on writer failure and with a synthetic inherited tunnel PID), and yearly
`--target-year` parsing still works. Fixtures must stub the publication wrapper,
not execute Docker/SSH or depend on real operational files. No cron installation or remote writes.

### W3 — Documentation and integrated verification (depends on: W1, W2; one verification agent)

**Allowed docs:** `doc/deployment.md`, `apps/preprocessing_runoff/README.md`,
`doc/plans/pp075_range_deployment_handoff.md`, this plan, parent plan and issue index.
Document daily publication in the existing maintenance cron comment; update the yearly wrapper's
scope description in W2 to include reusable publication of delta inputs. Preserve all numeric cron
fields and unrelated jobs. Describe local commands and fixed-reference/current-publication-year rules.

**Acceptance:** run affected suites through `apps/run_tests.sh` (`pipeline`, `preprocessing_runoff`),
then full `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh`; accepted baseline is exactly 12
existing skips and 2 existing expected failures: compare their identities and reasons, not just
counts, and investigate every new skip/xfail or failure. Independent fresh
read-only adversarial review of final diff, then confirm fixes. For cross-workflow changes run
`bash apps/run_docker_tests.sh --skip-ml` and the local LT operational path under normal scheduling;
inspect output/skip reasons, not just exit zero. Existing long-term scheduling may select modes
that models then skip; no scheduling fix belongs here. Verify current-year API `std/count` and the
real dashboard reader after publication. Keep operational logs/configuration out of Git.

## Rollout and deferred work

Deploy updated scripts and preprocessing/dashboard images together with consistent reference env
settings; the yearly wrapper pulls an absent image but does not refresh a cached tag, so verify the
deployed revision/image digest rather than accepting cache presence as readiness. No service migration is needed for this wiring change.
Follow the existing [handoff's observation-statistics rollout](../pp075_range_deployment_handoff.md#observation-statistics-rollout):
when changing the reference period, pause affected dashboard consumption until a complete intended
station/horizon publication is verified. A partial write is not readiness; no reference-version field
exists. Run one explicit publication using the existing yearly wrapper, verify intended station and
month/quarter/season coverage through the API (including legitimate insufficient-history counts),
then refresh/restart the dashboard. Independently inspect the actual dashboard container/image and
reader, because the inherited frontend wrapper can mask Docker failures with a final successful echo
(already documented in `high_prio_gi_draft_pp_quarter_calendar_window_validation.md`); fixing that child
is deferred, not included in W2. Do not wait for the next nightly cron to repair an empty publication. Existing cron
rows stay installed; document the owner-supplied snapshot as evidence, not a new crontab template.
Avoid overlapping manual/yearly/daily writer executions (existing fixed container name); a new
locking framework is deferred. Fixed reference makes daily repetition useful for corrected history,
while yearly publication supplies the new year's rows. Earlier saved bulletin bounds stay frozen.

Deferred: short-term/backend harmonization remains tracked by PP-075; historical LT date propagation
and existing-ensemble recomputation, scheduler tolerance mismatch, cron timezone/comment cleanup,
generic observability, retries/locking redesign and broader script refactoring are separate work.

## Review record

2026-11-16: independent open-ended Codex and Sonnet 4.6 reviews against a clean source snapshot
of implementation commit `a87a66c2` (unrelated uncommitted dashboard fix excluded). Incorporated
operational failure/log ownership, changed-reference rollout gate, precise date/skip tests, nested
tunnel ownership, inherited frontend-status limitation and bounded test scope. Codex confirm-fixes
and final confirmation found no remaining blockers or contradictory contracts. That review covered the
plan only; implementation evidence is recorded below. Server deployment remains pending. The first Sonnet attempt
was interrupted by host sleep and was replaced by a completed fresh review. The owner subsequently authorized implementation on 2026-11-16.

## Implementation verification (2026-11-16)

- W1/W2 implemented by scoped Sonnet 4.6 agents; no service or cron schedule edits.
- Affected suites: pipeline 507 passed; preprocessing 639 passed with its two accepted skips.
  Full suite: 9,382 passed, 12 accepted skips and 2 accepted expected failures. Skip/xfail
  identities and reasons exactly match the earlier accepted baseline.
- Fresh independent Codex review found two minor test gaps (actual stage ordering and quoted
  spaced-path forwarding), with no implementation defects. Both tests were corrected; all
  28 wrapper tests passed afterward. Independent confirm-fixes review returned no remaining findings.
- Docker: all eight non-ML images built and passed import smoke tests. Initial worktree build
  copied local venv symlinks and failed; rerunning the unchanged source from a clean archive
  with the current patch overlaid passed. ML was intentionally excluded by `--skip-ml`.
- Normal-schedule local LT operational run initially stopped at runoff preprocessing because
  the HF tunnel was unavailable, correctly preventing publication. After the owner reopened
  the tunnel, runoff succeeded and the new publication step ran with no SDK/API failures.
  The actual dashboard reader verified month/quarter/season delta inputs after publication,
  with no read errors. Known missing monthly norms were reported separately. Exactly one
  publication invocation and summary occurred before weather preprocessing, which also succeeded.
- The complete normal-schedule command exited 1 after 35m26s: `month_0` through `month_3`
  were admitted by the schedule query (10-day tolerance), but their models rejected today
  under the 5-day issue-date gate. `month_0` was six days after its issue date; the others
  were nine days before theirs. Date skips returned unsuccessful model results; dependent
  models then skipped, all four modes failed, and postprocessing was not reached. No new
  raw forecasts were produced. This pre-existing mismatch is visible in
  `lt_schedule_query.ISSUE_DAY_TOLERANCE`, `lt_utils.check_valid_forecast_issue_date`, and
  `run_forecast.run_forecast`; none changed in this patch. Do not report this as a green
  end-to-end forecast run or broaden W1/W2 into a scheduler fix.
- No server deployment or cron installation performed. Logs and runtime configuration remain
  outside Git. Source remains uncommitted.

```json
{
  "phases": {
    "W1": {"depends_on": [], "parallel_agents": 1},
    "W2": {"depends_on": [], "parallel_agents": 1},
    "W3": {"depends_on": ["W1", "W2"], "parallel_agents": 1}
  }
}
```
