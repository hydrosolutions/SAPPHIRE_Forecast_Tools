# INFRA-053: Pipeline completion email reports a hard-coded task list, not what the run actually did

**Status**: Draft (2026-09-10)
**Module**: `apps/pipeline` (`pipeline_docker.py`)
**Priority**: Medium — it cannot corrupt data and `requires()` correctly gates it to
otherwise-successful runs only (see "Scope boundary" below), so the blast radius is
misinformation, not breakage. Against that: `doc/configuration.md:212` marks
`SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS` "Required-if: email alerts", and
`doc/prod/first_deploy_checklist.md:431`, `doc/monitoring/forecast_tools_monitoring.md:50/75`
and `doc/plans/deployment_new_hydromet_aws.md:712` all instruct operators to set it as part of
bringing up pipeline alerting — this is documented as expected operational infrastructure, not a
dev-only knob. `bin/monitoring/docker_log_watcher.sh:39` and `bin/monitoring/docker.sh:66` read
the **identical** variable name for a separate shell-based failure-alert mechanism, so an operator
who follows the docs and sets it for that purpose also activates this task's misleading success
email as a side effect, without touching pipeline code. Sharper still: the documentation frames
this variable as **failure** alerting specifically — `doc/configuration.md:322` heads its block
"Email alerts on pipeline failure", and `doc/prod/first_deploy_checklist.md:431` calls it
"pipeline failure notifications". An operator who sets it is opting into failure alerts and
receives these success emails unasked, which is precisely the audience least primed to doubt one. **Not proven live from tracked config**:
`apps/config/.env_develop:115` carries only a commented-out placeholder
(`#SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS=<recipient1@example.com,recipient2@example.com>`), and no
real per-deployment env file is (or should be) tracked, per this repo's sensitive-data policy — so
whether any live deployment has actually set it cannot be established from the repository, only
from an operator confirming it. That gap is what keeps this Medium rather than High.
**Labels**: `infra`, `pipeline`, `notification`, `false-success`, `silent-failure`
**Found**: 2026-09-10, extending a pending decision flagged (not resolved) in INFRA-051's file:
"`RunAllMLModels` also appears, uninstantiated, in the kghm completion-notification email
(`:1393-1400`) — flagged as a separate pending decision, not fixed here."
**Filed separately from INFRA-051** (owner decision 2026-09-10): most of this issue's wrongness
— the hard-coded per-org list, the empty tjhm branch, the maintenance/long-term mismatch — is
independent of INFRA-051's case-sensitivity defect. INFRA-051's case-sensitivity bug is only one
of several ways the ML line can be wrong (see point 1 below).

---

## Defect

`SendPipelineCompletionNotification` (`apps/pipeline/pipeline_docker.py:1320`) emails operators
when a pipeline finishes. Its `requires()` (`:1341-1342`) returns `self._depends_on`, so Luigi
only runs it after those dependencies succeed — it fires on success, correctly. The defect is in
`run()` (`:1347`): at `:1387-1407` it builds `"Tasks completed for {ORGANIZATION.upper()}:\n"`
followed by a **hard-coded literal list chosen by an `if`/`elif` chain on `ORGANIZATION`**
(`:1388` `demo`, `:1393` `kghm`, `:1402` `uzhm`). Nothing in this block reads what the current
Luigi run actually scheduled, executed, or the `custom_message` parameter already available on
`self`. The message is emailed at `:1415` via `NotificationManager.send_email`, gated at `:1411`
on `email_recipients` being non-empty (built from `SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS`, read at
`:1360`). When no recipients are configured, the (still wrong) message is composed and logged to
the task's own log file but never sent — the defect is dormant until an operator sets the var.

Verified each of the six points below by reading `apps/pipeline/pipeline_docker.py` at HEAD,
`apps/pipeline/Dockerfile`, `bin/docker-compose-luigi.yml`, and `doc/configuration.md`.

### (1) Claims ML ran when no ML was scheduled — confirmed, and broader than one cause

`kghm`'s branch unconditionally appends `"- RunAllMLModels\n"` (`:1397`). ML scheduling is gated
at four sites by `RUN_ML_MODELS == "True"` (`:816`, `:1483`, `:1545`, `:1870`), where
`RUN_ML_MODELS = env.get("ieasyhydroforecast_run_ML_models")` (`:40`) with no default (absent
resolves to `None`). Any of the following makes every gate evaluate `False` and schedules zero ML
tasks, while the email still lists ML as done: the operator deliberately sets `false`; the
documented-but-lowercase `true` fails the exact-`"True"` comparison (**INFRA-051**, filed
separately and still open); or the variable is simply absent. This finding is independent of
INFRA-051 — it misreports whenever ML is off for *any* reason, not only the case-sensitivity one.

### (2) Names a task with no repository-wired entrypoint — confirmed, with the appropriate hedge

`RunAllMLModels` (`:776-799`) is a `luigi.WrapperTask` that yields `RunMLModel` for every
configured model x prediction mode, ignoring `RUN_ML_MODELS` entirely. Its only repository
references outside its own class body are two test files
(`apps/pipeline/tests/test_marker_files.py`, `test_task_implementations.py`) and this
notification string. Neither `apps/pipeline/Dockerfile:38`'s `CMD` (invokes `PreprocessingRunoff`)
nor any `command:` in `bin/docker-compose-luigi.yml` (which name
`RunPreprocessingGatewayWorkflow`, `RunPreprocessingRunoffWorkflow`, `RunPentadalWorkflow`,
`RunDecadalWorkflow`, `RunDailyMaintenanceWorkflow`, `RunPeriodicMaintenanceWorkflow`, or
`RunLongTermWorkflow`) names `RunAllMLModels` — the real workflows schedule individual `RunMLModel`
tasks directly (`:1483`, `:1545`) instead of going through this wrapper. So the email can name a
task that never runs through any repository-wired path, even when ML is enabled and working.
**Hedge, as the class name says**: external or manual Luigi invocation
(`luigi --module ... RunAllMLModels`) cannot be excluded from repository evidence alone — this
finding is scoped to "no wired production entrypoint," not "unreachable under all circumstances."

### (3) `ConceptualModel` — same shape, confirmed

`ConceptualModel` (`:679`) is appended unconditionally in the `kghm` branch (`:1396`). Its actual
scheduling is gated on `RUN_CM_MODELS == "True"` at `:827` (`RUN_CM_MODELS = env.get(
"ieasyhydroforecast_run_CM_models")`, `:41`) — same unconditional-claim-vs-gated-reality shape as
the ML line, and the same case-sensitivity exposure INFRA-051 notes for the sibling variable.

### (4) `DeleteOldGatewayFiles` — same shape, confirmed

`DeleteOldGatewayFiles` (`:869`) is appended unconditionally in the `kghm` branch (`:1399`), while
its actual scheduling in `RunPentadalWorkflow`/`RunDecadalWorkflow` is
`if RUN_ML_MODELS == "True" or RUN_CM_MODELS == "True":` at `:1500` and `:1562` respectively —
confirmed by reading both sites. If both flags evaluate false, cleanup is skipped but still
reported as done.

### (5) tjhm gets an empty list — confirmed, org string verified

The `if`/`elif` chain covers only `demo` (`:1388`), `kghm` (`:1393`), `uzhm` (`:1402`) — there is
no `tjhm` arm and no final `else`. `grep -c tjhm apps/pipeline/pipeline_docker.py` returns `0`.
`ORGANIZATION = env.get("ieasyhydroforecast_organization")` (`:37`), and `tjhm` is confirmed as
the literal value for the Tajik deployment by `doc/configuration.md:136`
(`` `ieasyhydroforecast_organization` | Required | all | Deployment identifier (`demo`, `kghm`,
`tjhm`, `uzhm`) ``) and by `apps/run_locally.sh:2334`'s help text listing the same four values —
this is a documented, not inferred, org literal. A tjhm deployment's completion email reads
`"Tasks completed for TJHM:\n"` followed immediately by `"\nThis is an automated notification."`
— no task lines at all, for every tjhm run that reaches this task.

### (6) Same forecast-task list follows a maintenance run — confirmed, and applies to all four call sites, not only maintenance

`SendPipelineCompletionNotification` is instantiated at exactly four sites (verified: the class
definition at `:1320` plus four `SendPipelineCompletionNotification(` call sites, no more):
`:1505` (`RunPentadalWorkflow`, `custom_message=f"PENTAD {self.custom_message}"`), `:1567`
(`RunDecadalWorkflow`, `"DECAD {self.custom_message}"`), `:1923` (`RunDailyMaintenanceWorkflow`,
`custom_message="Daily maintenance completed"`), and `:2502` (`RunLongTermWorkflow`,
`f"LONG_TERM {self.custom_message}"`). `custom_message` only ever *adds* a
`"Message: {custom_message}\n\n"` line (`:1380-1381`); the `"Tasks completed for {ORG}:"` block
that follows is built purely from `ORGANIZATION`, with no reference to `custom_message`, to which
of the four call sites fired, or to what `self._depends_on` actually contained. So **all four are
affected, not only maintenance**: a `RunDailyMaintenanceWorkflow` run — which only runs
`PostProcessingMaintenance` (`:1908`) — gets the same forecast-pipeline list as a pentad/decade
run; and a `RunLongTermWorkflow` run — which runs `RunLongTermForecast`, `LongTermPostProcessing`,
`LogFileCleanup`, `DeleteOldMarkerFiles` (`:2481-2488`), none of which appear anywhere in the
hard-coded list — still gets `"PreprocessingRunoff / LinearRegression / PostProcessingForecasts
[/ RunAllMLModels / ConceptualModel] / LogFileCleanup [/ DeleteOldGatewayFiles]"` for its org,
none of which is what that run did.

## Why this matters — the one artefact that reaches a human unprompted

Every other issue in this family is **silent**: PP-051 (recalc reports success on a swallowed API
write), ML-021 (`make_forecast.py` exits 0 having written nothing), PREPG-026 (a failed snow API
write doesn't fail the task), INFRA-047 (canonical cron wrappers exit 0 on failure) — an operator
has to go looking (logs, exit codes, dashboards) to find any of those. This one is the opposite
failure mode in the same family: it **actively asserts** a specific list of completed work to a
human who did not go looking for it and who therefore has no independent reason to doubt it. An
operator who trusts this email needs no exit code, no log, and no alert to be fooled — the email
itself is the false signal. See `high_prio_gi_draft_pp_recalc_silent_api_write_failure.md` (PP-051),
`review_gi_draft_ml_forecast_api_write_silent_success.md` (ML-021),
`archive/review_gi_draft_prepg_snow_api_write_failure_not_reported.md` (PREPG-026), and
`archive/review_gi_draft_infra_canonical_cron_wrappers_exit_zero.md` (INFRA-047).

## Fix direction (not designed here)

Derive the "Tasks completed" list from what the run actually scheduled and completed, not from a
literal per-org table — a corrected hard-coded list would only drift again the next time
scheduling logic changes (a new gate, a new org, a reordered workflow). Luigi already knows the
scheduled task graph for a given run (`self._depends_on`, or the task tree Luigi built to satisfy
`requires()`); the information needed to make this accurate already exists inside the process that
sends the email. How exactly to surface it (walk `_depends_on`, pass an explicit list from each
caller, introspect the scheduler) is an implementation decision for whoever picks this up, not
fixed here.

## Scope boundary

Do **not** change `SendPipelineCompletionNotification`'s success/failure semantics or its
`requires()` gating — it correctly fires only after a successful run, and that is out of scope.
Only the body's accuracy (the "Tasks completed" list) is in scope for a fix.

## Out of scope

- INFRA-051's case-sensitivity defect itself — referenced for point (1), not re-argued or fixed
  here.
- `RunAllMLModels`'s missing production entrypoint as a scheduling gap — noted for point (2); this
  issue only covers the email accurately reflecting whatever is actually wired, not adding an
  entrypoint for it.
- Any change to `SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS` gating, SMTP configuration, or the separate
  `bin/monitoring/docker_log_watcher.sh` / `docker.sh` alerting path — those are a different
  mechanism that happens to share the same env var name.
- Adding a `tjhm` (or generic `else`) branch to the current hard-coded structure — that would only
  extend the pattern this issue says should be replaced, not fix it.

## Acceptance criteria

- [ ] The "Tasks completed" section of the email reflects what the specific run actually
      scheduled/completed, for all four call sites (`:1505`, `:1567`, `:1923`, `:2502`), not a
      static per-`ORGANIZATION` literal.
- [ ] A tjhm deployment's email lists its actual completed tasks instead of an empty section.
- [ ] A `RunDailyMaintenanceWorkflow` or `RunLongTermWorkflow` run's email no longer lists
      unrelated forecast-pipeline task names.
- [ ] `RunAllMLModels`/`ConceptualModel`/`DeleteOldGatewayFiles` appear in a kghm email only when
      the run's actual gates (`RUN_ML_MODELS`, `RUN_CM_MODELS`) caused them to be scheduled.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected
      skips.

---

## Related

| ID | Relation |
|---|---|
| INFRA-051 | Source of one contributing cause to point (1); filed separately per owner decision — most of this issue's wrongness is independent of INFRA-051 |
| PP-051 / ML-021 / PREPG-026 / INFRA-047 | Same failure-visibility family — those are silent-success bugs; this is an active false-success report, the inverse shape |
