# Latest-forecast recovery — minimal maintenance fix

Status: In Progress — M1/M2 (ML recovery and validator fixes) implemented, independently reviewed and verified on remote target `77c0c071`. Full integrated suite, Docker checks and local pipeline passed with the existing accepted test skips. After the tunnel interruption, a targeted live runoff rerun also passed. L1/L2 (latest-only LT recovery) remain separate, not yet implemented. Not deployed.
Date: 2026-11-17
Baseline: `security/maxat-dependency-upgrades` at `4c8899f6` plus its existing uncommitted recovery work. Refetched target `origin/maxat_sapphire_2`: `77c0c071`. Server revision/timezone unverified.

## Implementation checkpoint — ML patch

Implemented only the first ML slice: latest pentad/decade selectors with horizon-specific station scope; strict absent-unit opt-in; protected writes and owned restart handling; latest-only period consumer; dedicated daily invocation that continues to postprocessing after producer errors. The new recovery invocation does not run historical gap-fill/add-station/ordinary postprocessing/skills. The prior newly-added ordinary entry hooks were removed to avoid duplicate recovery. Separately existing general maintenance remains unchanged.

A small configuration-gating mismatch found by the independent reviewer was fixed and rechecked. Latest target publication/tunnel behavior was retained; the yearly wrapper and its supporting test are exact existing upstream code, not a new recovery behavior.

Validation executed for this patch:
- ML: 530 passed, zero skips/failures.
- Postprocessing: 3203 passed, zero skips/failures; one pre-existing expected failure for deferred tombstone behavior.
- Pipeline: 495 passed in the final full-suite validation, zero skips/failures.
- Full pre-commit suite: 8,995 passed across all 16 scopes, 12 pre-existing skips explicitly accepted by the owner for this commit, and two pre-existing expected failures. No new skips or failures; changed recovery test scopes have zero skips.
- `bash apps/run_docker_tests.sh --skip-ml`: eight builds and eight smokes passed; ML image intentionally excluded by that command.
- Live-configuration read-only preview selected the latest separate pentad/decade dates and exited successfully without executing repairs.
- Shell syntax, Ruff for touched Python files, and diff whitespace checks passed.
- Full local `all --continue-on-error` rerun: 37m 6s, 11 execution stages passed, zero execution-stage failures. Short-term validation passed. Long-term and combined validation repeat the same three presence failures (month, quarter, season); overall exit 1. The runner simulates month_0 for January 2024 but does not pass that forecast date or generated horizon scope to the validator, which defaults to the current date and requires all long-term horizons. This existing mismatch is tracked in INFRA-021/028. These are failures, not covered by the owner’s existing-skips exception. No validator behavior was changed.

The first concurrent worktree pipeline test run was invalidated by its source-mutation guard and blocked registry reads; the isolated rerun with read-only network access passed. No test guard was weakened. The full pre-commit suite subsequently passed in an isolated copy of the exact staged source candidate. The required full local pipeline rerun completed with the validation failures above. This is not a claim of a green full pipeline, live recovery-write validation, ML Docker validation, deployment or completion of LT work. No commit/push was made.

## Validator follow-up — 2026-11-18

The owner requested correcting the existing simulation-validation mismatch before proceeding. A narrow `--lt-simulation MODE:YEAR:MONTH_COUNT` handoff now checks the simulated issue dates, configured horizon and lead; short-term validation retains its current date, ordinary operational checks remain unchanged, and all three long-term skill checks remain active. No persistent manifest, service change or forecast-model change was introduced.

Fresh independent review found configuration-root, numeric-string and multiline-input parity issues; these were corrected and the reviewer confirmed no remaining material findings. Final affected scopes: validator 192 passed and pipeline 501 passed, zero skips/failures. The full 16-scope suite passed 9,025 tests with 12 owner-accepted existing skips and two existing expected failures; the final multiline shell correction followed that snapshot and was verified by the full 501-test pipeline scope.

A read-only check with the real configuration found the expected historical outputs and exposed one existing EM row with q75 greater than q90. The validator retains that failure. The requested full local rerun completed in 38m 27s: 10 execution stages passed, one failed (gateway meteo download raised ValueError; snow succeeded). Both long-term validation invocations resolved 2024-01-10:month:0 and found the expected forecasts; the three previous false presence failures are gone. Combined validation reported 39 passed, one failed (the EM quantile crossing), nine warnings and four existing/non-applicable skips. Short-term validation passed. Operational and maintenance long-term postprocessing plus all three skill recalculations passed. Overall exit 1; no green-run claim, commit or deployment. Final Docker checks: eight builds and eight smoke checks passed, with ML intentionally excluded by --skip-ml.

## Retired long-term EM validation follow-up — 2026-11-18

The owner approved treating retired long-term EM quantile crossings as explicit legacy-data warnings. The change is confined to long-term validation; active-product and short-term EM quantile failures remain strict, other data-quality checks retain their original inputs, and retired-only responses cannot satisfy active forecast presence. No historical data was altered, clipped or deleted. Full validator scope: 215 passed, zero skips/failures; fresh independent review found no material issues. Existing ordinary quarter/season correctness routing was not expanded in this patch.

Read-only combined API validation of current short-term outputs and the historical simulation: 40 passed, zero failed, 10 warnings, four existing/non-applicable skips, exit 0. The legacy warning explicitly retains the one EM crossing. This is a validator rerun, not a new execution of the forecast pipeline; the prior meteo-download execution failure remains recorded above. Final full-suite validation passed: 9,051 tests, 12 owner-accepted existing skips and 2 existing expected failures; zero failures. No new skips. The owner subsequently accepted the separate meteo-download execution failure. No deployment was performed.

## PR preparation checkpoint — 2026-11-18

The owner accepted the meteo pipeline failure. The reviewed ML recovery and validator changes are ready for a scoped local commit, excluding the separately uncommitted dependency upgrades. The target was fetched again and remains `77c0c071`; the working branch base `4c8899f6` is seven commits behind it. Reviews used target overlays, but the 9,051-test full suite ran against the feature candidate based on `4c8899f6`. That is not proof of the integrated target result. Reconcile overlapping runner/maintenance changes and verify the integrated branch before calling the PR ready. Long-term latest-only recovery (L1/L2) is not implemented and must not be claimed in this PR.

## Remote-target integration checkpoint — 2026-11-18

The original ML/validator slice was committed as `42ffc479`, excluding pending dependency updates. It was then applied onto fetched `origin/maxat_sapphire_2` at `77c0c071` in the isolated `fix/latest-forecast-recovery` branch. Conflicts preserve unconditional Luigi → latest recovery → observation-statistics publication → frontend execution and aggregate failure reporting. The upstream yearly wrapper remains unchanged.

Integrated-source verification: 9,552 tests passed across all 16 scopes, with the same 12 owner-accepted existing skips and two existing expected failures. Eight Docker builds and eight smoke checks passed with `--skip-ml`; the successful run used a clean source snapshot because local virtual-environment symlinks are unsuitable Docker build-context inputs. Unit tests reused the existing local environments. A fresh independent read-only review found no material integration defects.

The integrated local `all --continue-on-error` run completed in 56m 8s with 11 execution stages and all three validation invocations passing (exit 0). The legacy EM warning remains explicit; existing data-quality warnings also remain. The iEasyHydro HF tunnel at localhost:5555 stopped accepting connections during runoff processing; that stage exited successfully but logged failed live reads and reuse of stored data. The owner reopened the tunnel and both local API ports were verified reachable. A targeted `run_locally.sh preprocessing_runoff` rerun passed in 6m 4s with no connection errors; its validator reported 10 passed, zero failed/warned and 10 non-applicable checks skipped. Existing source-data warnings remain, including one station with no usable daily history whose null hydrograph batch was refused. This closes the tunnel-related verification gap. No PR, deployment or LT implementation is claimed at this checkpoint.

## Minimal LT implementation handoff

Keep the API as the durable source of recovery state. Each invocation selects the latest configured issue per mode/member, retains the current/previous-month guard, and protects healthy raw keys. New writes must pass immediate exact key/value/flag read-back. Previously healthy flag-1 recovery rows may trigger a selected derived rebuild; ordinary flag-0 rows may contribute but do not trigger one alone.

Pass a fresh, private per-invocation scope file from producer to recovery-only postprocessing under the existing wrapper lock. It contains the captured reference date, destination identity, exact issue/horizon/lead/validity scopes, allowed source identities and verified recovery triggers. Postprocessing rereads those sources, uses existing computation helpers, preserves issue/target identities and verifies submitted derived results. Publish independently verified scopes even if another producer unit fails, retaining aggregate failure status. No old handoff may be reused after a failed invocation.

No durable per-key manifest, acknowledgment queue or separate run-token framework is needed: the next invocation rediscovers current usable flag-1 API results after postprocessing failure. Superseded or out-of-window issues are excluded anew. After an interrupted raw write, a later invocation trusts the API's current usable flag-1 result; it does not claim proof of equivalence to the interrupted computation's lost in-memory payload. Quarterly derivation requires its own latest-issue authorization. Missing declared mode files must be diagnosed, not fabricated or silently treated as successful coverage.

The independent design review found no architecture blocker and identified these adapter boundaries: invoke ensemble helpers separately for each exact issue/lead/validity scope; persist only authorized derived outputs, never raw passthrough rows; require a recovery trigger to survive the existing native-versus-monthly-derived quarter source selection and contribute to the result; preserve stored nonforecast fields such as `q_obs` when upserting; and create the fresh handoff path in the wrapper before producer startup, so even import/image failures cannot consume an old handoff. Regression tests must cover these boundaries, including both quarter lead-aware flag states. Existing compute helpers and manual recovery remain unchanged.

## Goal and scope

Regularly retry missing/failed results for **only the latest due forecast per configured stream**, after late inputs arrive. Preserve healthy results and original issue/target identities. Do not drain historical gaps, retrain models, recalculate skills, or redesign maintenance.

The maintenance-mode review supersedes the earlier broad phases and release requirements retained in the evidence appendix. Findings about existing code remain useful; they are not all obligations for this bug fix.

## Smallest implementation sequence

1. **ML fix:** replace the new recovery worker's 30-day automatic selection with latest pentad/decade issues only. Reuse existing compute, eligibility, protected-write, audit/resume and exact read-back helpers. Rebuild only the applicable selected period outputs. Wire a dedicated recovery invocation under the existing daily trigger; do not call a full historical maintenance command just to run recovery. Remove/disable duplicate automatic invocation of the new hook if the dedicated path owns it.
2. **LT fix, separately:** select the latest due issue per configured mode/lead/member schedule. Reuse date validation and model execution, but filter persistence to the exact missing/failed keys and verify the submitted results. The current whole-issue refusal and positive-row-count success cannot deliver this behavior. Prefer a narrow recovery-only adapter/internal write filter; compute whole model output if necessary and safely filter its persistence, rather than redesigning all compute APIs.

Each patch includes its minimal invocation wiring, relevant regression tests, and a short operator note. Do not build a shared recovery framework first. Do not modify colleague-owned `sapphire/services/` or introduce an API contract change.

## Essential now

| Requirement | Minimal solution / reason |
|---|---|
| Latest only | Resolve the latest scheduled issue already due, then test missing/failed results there. If latest is healthy, do nothing even when older gaps exist. Never select the newest historical gap repeatedly. |
| Separate product schedules | Pentad and decade can have different latest dates; LT modes/leads/member schedules are independent. Reuse a shared daily ML computation when its issue serves both products. |
| Correct rollover | Evaluate each horizon consumer independently. An issue superseded for pentad can remain current for decade. Filter saved pending work against the current selected dates; no new supersession state-machine framework is needed. |
| One date per invocation | Capture the domain reference once at entry and pass it through selection, write checks and rebuilds. Restart captures a new reference and rechecks pending eligibility. Avoid a repository-wide clock refactor. |
| Healthy-row protection | Reuse strict eligibility and fresh pre-write checks. Write only approved failed/missing identities. Keep existing owned partial-write reconciliation and read-back; do not weaken proven safeguards to save code. |
| LT partial failure | A failed station/model must be recoverable even when other keys succeeded. Verify every submitted key/value/flag; record unresolved eligible keys as partial, not complete. Existing tests that accept any positive row count do not establish this. |
| Regular bounded retries | Start with existing caps/cooldown and existing durable state. Keep daily scheduling; a recovery-specific invocation must not be permanently suppressed by an old completion marker. A general run-generation framework or intraday scheduler is unnecessary. |
| Verified repairs reach postprocessing | Use a small recovery coordinator/finalization step that processes already-verified units even when another unit failed, then returns the aggregate failure status. Do not redesign the ordinary Luigi dependency graph or hide errors. |
| No accidental historical work | The dedicated recovery invocation runs only latest-key compute/write/rebuild helpers. It must not launch legacy historical recalc, gap-fill, add-station or broad PP as a side effect. |
| Practical non-overlap | Retain recovery unit locks and prevent duplicate recovery invocations. Reuse existing serialization and the maintenance window where applicable. Do not promise atomic protection against foreign API writers; do not silently run over a known conflicting writer. |

Latest-only tests must cover latest healthy with older missing, two different current short-term dates, shared issue deduplication, only one horizon rolling over, future dates excluded, healthy-key preservation, partial write/restart, exact read-back, repeated next-day invocation, and proof that the dedicated path does not call historical routines. Test actual affected LT modes/features; no broad predictor refactor.

## Important scope boundary: existing maintenance versus the new repair

`apps/machine_learning/Dockerfile:40` runs `recalculate_nan_forecasts.py`, `fill_ml_gaps.py` and `add_new_station.py` under `RUN_MODE=maintenance`; the pipeline passes that mode. Ordinary postprocessing also performs historical ensemble gap filling. These are real existing paths, not hypothetical loopholes.

**The new automatic recovery invocation must be latest-only and must not invoke those historical routines.** The minimal proposal leaves separately existing general maintenance behavior unchanged. Consequently it does **not** promise that every write made by the entire existing daily maintenance workflow is latest-only.

The earlier plan expanded the owner's rule into changing all automatic maintenance writes. That interpretation increases scope substantially and is not assumed here. If the owner intends a global ban on all older automatic maintenance writes, the affected legacy entry points need a separate, narrowly scoped gate/bypass change; it cannot honestly be deferred while claiming that global guarantee. This boundary must be stated in the implementation description, not hidden behind a new mode name.

## Defer from this bug fix

- Model/scaler/forcing fingerprints, version inventories and exact as-issued replay infrastructure. Retain existing recovered flags/audit identities and timestamps; describe the result as retrospective reconstruction. Do not change skill inclusion rules.
- Configurable-budget machinery, a 24–48-hour performance guarantee, backlog dashboards, historical gap ageing and generalized telemetry. Use existing bounded limits and concise attempted/recovered/remaining/error counts. Tune only if observed runs demonstrate inadequate capacity.
- Cross-application/global locking retrofit, database advisory locks or compare-and-swap APIs. Keep the known residual foreign-writer race explicit; coordinate separately if a concrete collision requires stronger protection.
- Broad DAG failure-isolation redesign, global completion-marker redesign or additional cron cadence. Fix only the recovery invocation and its finalization behavior.
- Reworking existing historical initialization, ordinary gap filling or station onboarding; see the scope boundary above.
- Extending the LT current/previous-month safety window. If a latest quarterly/seasonal issue is outside it, report blocked; broadening that guard is a distinct change if operationally needed.
- Dashboard ranges, EM eligibility/threshold changes and automatic skill recalculation.

## Data and rollout limits

Late inputs are the reason for recovery. Existing short-term compute uses discharge through the issue date and current meteorological/reanalysis covariates and model artifacts. It is a flagged retrospective reconstruction, not an exact reproduction of the original information set. Preserve current model semantics; add only tests needed for the touched date-selection/persistence behavior. For LT, broader reads alone do not prove later observations affect prediction: configured feature lags matter.

Use the same existing private persisted recovery directory, outside Git, shared with the selected period rebuild helpers. No sensitive operational values or identifiers in commits. Preserve the latest target wrapper's unconditional Luigi → long-horizon observation publication → frontend sequence and aggregate failure behavior. Verify actual deployment timezone/config/image before scheduling; no crontab changes based solely on mismatched comments.

## Small sequential work plan

| Phase | Goal | Allowed files / areas | Depends on | Agents | Acceptance |
|---|---|---|---|---|---|
| M1 | Narrow the existing ML repair to latest issues and selected period rebuilds | `apps/machine_learning/recover_failed_forecasts.py`, its hook/entrypoint only as needed; `apps/postprocessing_forecasts/src/recovered_periods.py`; minimal recovery-only invocation adapter/wiring; focused tests and operator note | Owner accepts minimal scope/boundary | One implementation agent, then independent reviewer | Latest-only writes, healthy preservation, restart and both horizon behavior; no legacy historical routine called by recovery |
| M2 | Validate/review ML patch before opening LT implementation | Relevant tests via `apps/run_tests.sh`, required local/Docker validation and documentation | M1 | One verifier/reviewer | Required checks pass; existing skips/failures reported; actual counts/errors visible; no deployment inferred |
| L1 | Add latest-only selective LT repair | `apps/long_term_forecasting/lt_recovery.py`, narrow internal persistence boundary/adapter near `run_forecast.py`/`lt_utils.py`, minimal invocation/rebuild wiring, focused tests and note | M2 | One implementation agent, then independent reviewer | Exact eligible key writes, no healthy overwrite, full submitted-key read-back, latest-only rebuild and repeated retry |
| L2 | Validate LT patch and prepare concrete rollout | Affected test scopes, required local/Docker validation and runbook | L1 | One verifier/reviewer | Correct configured issue/lead identities and residual limitations recorded; deployment separately confirmed |

Before each implementation patch, enumerate the exact allowed files/functions. Do NOT change existing signatures, data flow or control flow except the specifically scoped fix; avoid unrelated refactoring. The orchestration file may need a narrow recovery-specific command/step; that is not authority to restructure all tasks. New bugs require regression tests, using `apps/run_tests.sh`; required cross-module pipeline and Docker checks still apply. No test execution was needed or performed for this documentation-only scope review.

## Maintenance-mode review conclusion

A lower-cost `gpt-6-luna` agent challenged the expanded scope. It recommended keeping latest selection, healthy protection, LT selective persistence/read-back and narrow retry/finalization wiring, while deferring the broader infrastructure work listed above. The primary agent corrected one source finding: `add_new_station.py` is indeed invoked by the production maintenance Docker entrypoint. The reviewer verified the correction and recommended isolating recovery rather than changing every legacy routine.

The four earlier latest-only corrections are warranted as **behaviors**. They do not justify four broad refactors: horizon/date filtering is small, one captured date is local, and historical routines can be excluded from the dedicated recovery invocation. The remaining scope boundary is explicit above. This plan is a minimal proposal, not authorization to implement or deploy unrelated improvements.

## Earlier source-review evidence (reference, not mandatory scope)

The following evidence is retained for traceability. Earlier statements about broad cross-writer locks, marker frameworks, whole-maintenance restrictions or implementation phases are superseded by the maintenance-mode scope above.

## Independent review findings

Three independent `gpt-6-luna` reviewers were used at the user’s request for lower-cost source checking: short-term recovery, long-term recovery, and orchestration/deployment. No reviewer changed code or wrote live API data. Findings below were checked against the source by the primary agent. This is static source review, not a fresh execution of tests or server verification.

### Evidence matrix

All paths are relative to the repository; line numbers refer to the reviewed working tree.

| Source chain / tests | Evidence and conclusion |
|---|---|
| ML daily entry | `apps/pipeline/pipeline_docker.py:1809–1900`; `apps/machine_learning/Dockerfile:36–40`; `recalculate_nan_forecasts.py:1364–1374`: input dependencies precede ML; recovery follows ordinary NaN recalc but precedes the later fill-gaps/add-station commands. Recovery error currently fails the container and can block downstream PP. |
| ML scope and dates | `apps/machine_learning/scr/utils_ml_forecast.py:574–599`; `recover_failed_forecasts.py:445–517`; `hindcast_ML_models.py:684–695`: both station configurations are unioned, the prior-30-day issue/station units are enumerated and scanned across successive bounded invocations with pentad boundaries prioritized, and compute intersects model-capable stations. Canonical DECAD is valid for the 11-day recovery generation. |
| ML compute semantics | `hindcast_ML_models.py:202–319,657–679,743–753`: observed discharge ends at issue date; current covariates extend through issue plus max(11, output chunk length); current model/scaler are loaded. Original model/input vintage equivalence is not established. |
| ML eligibility and delivery | `recalculate_nan_forecasts.py:688–812,1189–1200`; `recover_failed_forecasts.py:115–170,253–335,365–417`: supported eligible shape is wholly null flag-3 11-target units or six null targets plus absent tail; healthy/mixed units are protected. Exact read-back and owned partial-write restart exist. Wholly absent issues are not thereby proven covered; existing gap-fill must be restricted under automatic latest-only mode; classify unhandled shapes in reporting. |
| Period rebuilds | `apps/postprocessing_forecasts/src/recovered_periods.py:109–353`; `postprocessing_maintenance.py:2109–2142`: exact boundaries, separate configured station lists, shared durable pending queue, per-unit locks and independent pentad/decade iteration. BOTH is explicitly supported here. Current consumer caps three dates/horizon and five minutes; producer budget increases must not merely move the backlog downstream. |
| ML/PP tests | `apps/machine_learning/test/test_failed_issue_recovery_execution.py`; `apps/postprocessing_forecasts/tests/test_recovered_periods.py`; `apps/pipeline/tests/test_maintenance_tasks.py`, `test_task_implementations.py`: existing tests cover routing, protection, restart/cursor and boundary cases; add all-writer contention, pending-aware same-day rerun and partial-failure delivery tests. |
| LT guard and schedules | `apps/long_term_forecasting/lt_recovery.py:282–356,584–690`; `run_forecast.py:455–515,588–617`: exact issue dates and current/previous-month window exist. Any existing member row, including failed/null, blocks the entire issue. Computed scheduled-member list is not used as a selective write set. |
| LT persistence and completeness | `lt_recovery.py:722–775`; `lt_utils.py:333–452,555–595`; read-only `sapphire/services/postprocessing/app/crud.py:107–154`: writes use full identity including horizon, lead, station, model, issue and validity interval; service upsert updates submitted fields. Existing read-back accepts any positive count. `tests/test_lt_recovery.py` explicitly tests partial coverage as success. Replace that acceptance criterion for the new automatic path. |
| LT cutoff and shared state | `data_interface.py:214–297,634–713`; `config_forecast.py:57–111,157–180`: runoff has issue-date clipping; forcing/snow and dependency products need model-feature-level cutoff/provenance tests. Config load writes shared member configuration even before refusal; recovery must isolate or serialize these side effects. Legacy file side effects must not become a new CSV-supported recovery path; API remains authoritative. |
| LT modes and derived outputs | `config_forecast.py:227–276`; `lt_schedule_query.py:78–130`; `postprocessing_operational_long_term.py:206–283`; `postprocessing_maintenance_long_term.py:390–435,641–765`: horizon/lead are config-derived and structurally support month/quarter/season. Deployed mode configurations require validation. Existing PP is not raw member recovery and needs an exact-date rebuild path for repaired results. |
| Orchestration / concurrency | `apps/pipeline/pipeline_docker.py:162–175,364–400,1832–1900,2068–2099,2213–2293`: ordinary daily markers suppress same-day reruns; dated LT markers do not express a resumable partial-key queue. Luigi resources and recovery-only flocks do not cover all manual/external writers. Implement common application-writer serialization or isolated artifacts, plus fresh read/write verification; API races with foreign writers remain explicitly unresolved without colleague coordination. |
| Latest target compatibility | `origin/maxat_sapphire_2` `77c0c071`, `bin/run_daily_maintenance.sh` and associated `test_cron_wrapper_exit_codes.py`: newer target adds long-horizon observation-statistics publication between Luigi and frontend, unconditionally, and aggregates all three failures. Preserve this behavior; do not base deployment on the older local wrapper alone. |

### Follow-up: long-term model-feature and retry verification

The reviewer also traced the locally installed `lt_forecasting` 2.2.0 package: `LINEAR_REGRESSION.py:385–468` and `SciRegressor.py:690–843` select the issue-date row for prediction. `scr/FeatureExtractor.py:217–330` supports trailing features and configured negative shifts, which can bring future-valid-date inputs into that row. Thus broader input reads are **not proof of future-observation use in the deployed models**. Verify the actual per-model feature configurations and period-adjustment code. Late-arriving values with valid dates on/before the issue remain allowed. Tests must distinguish legitimately forecast future covariates from later observations/reanalysis and follow the accepted reconstruction policy rather than demand unavailable historical arrival timestamps.

The installed model package is an additional code source, not proof of the server wheel/version. Include its version and feature configuration fingerprints in deployment verification/provenance.

A misleading existing comment says LT retries skip the guard. Source tracing shows each retry starts a new container running the same recovery CLI and therefore rechecks the guard. Partial writes cause refusal on the next attempt; partial-positive read-back can instead return success and create a completion marker. Those are the actual retry/completion behaviors to test. Do not carry the guard-skip explanation into implementation documentation.

### Earlier implementation recommendations (superseded where broader than minimal scope)

- Long-term planning must enumerate exact expected keys, classify absent/failed/healthy rows, retain dependency computation as needed, and filter the persistence boundary to only approved failed/missing keys. A whole-mode run with an unchanged writer is insufficient. Key equality and value/provenance validation must cover every approved result; partial success is durable and resumable, not a terminal completion marker.
- P1 must specify the per-model cutoff test: late-arriving observations valid on/before the original issue are usable; legitimate forecast covariates can extend through the configured horizon. Later observations/reanalysis must either be excluded from original-information-style products or explicitly permitted and recorded under the accepted retrospective reconstruction policy. Test actual negative-lag features and `post_process_lt_forecast`, not just reader end dates.
- Use the current long-term date/schedule validation and model execution helpers, but introduce an explicit compute-versus-persist seam. Existing `run_forecast.py` writes complete output frames and has no selective station persistence contract. Prefer a new recovery-only adapter/internal persistence filter that leaves public runner signatures unchanged. P1 must settle that mechanism and enumerate exact function changes/files; any unavoidable existing-signature change must be explicitly scoped there, not smuggled in as a general refactor. P3 is not merely a cron change.
- Automatic coverage must distinguish wholly absent issues, wholly failed issues, mixed/partial units and unconfigured/unsupported modes. Existing short-term recovery handles a particular null shape; do not claim universal missing-forecast recovery.
- Budget both compute and downstream rebuilding for the latest issues only; count unresolved latest keys and age before and after each stage. Report superseded/out-of-window gaps separately without retrying them, and diagnose data readiness before expensive retries where feasible.
- Serialize all coordinated application writers that share mutable configurations/model-output paths; preserve the known limitation for foreign API writers. If stronger atomic protection is required, pause that subtask for service-owner coordination rather than editing services.
- Keep recovery failures visible in the final aggregate while ensuring already-verified units can still be postprocessed. Test this ordering, same-day pending work, marker behavior and restart after uncertain writes.

### Targeted latest-only review

A fourth lower-cost `gpt-6-luna` review checked the subsequent owner constraint. Four concrete corrections were incorporated:

1. `apps/machine_learning/add_new_station.py:177–345` automatically generates/writes historical hindcasts via the ML Docker maintenance command. It is now explicitly in P2/P4 scope and the latest-only acceptance tests.
2. `apps/postprocessing_forecasts/postprocessing_maintenance.py:1936,2138–2142,2166–2215,2383–2388` normally scans up to 13 months and writes historical derived forecasts. Automatic latest-only mode must constrain this route too; it is not an implicit historical-maintenance exception.
3. Shared daily fan eligibility is the union of its still-current horizon consumers; supersession and derived writes are horizon-specific.
4. One captured pipeline domain reference controls all due-date checks within a run; a restarted run re-evaluates current consumers against its new captured reference.

The targeted reviewer reread the corrections and found no remaining material contradiction: ready for P1 implementation design, not a claim of implementation or deployment readiness. These are documentation changes and future implementation/test obligations, not claims that existing code already enforces them. Separately requested historical initialization/manual repairs and scheduled skill calculations remain distinct from this latest-only automatic repair workflow.

### Earlier source-review verdict (before scope reduction)

High confidence in the existing short-term routing, station/date coverage, exact read-back foundation, and the identified long-term limitations. The daily-maintenance entry point and reuse of the existing compute helpers are the correct foundation. **Do not accept or deploy the initial proposal unchanged.** The revised plan makes the missing selective-write, provenance, concurrency, marker and partial-failure work explicit.

All three reviewers reread the revised plan and found their material source findings accurately represented. Final wording was refined for bounded cursor scans, configured future covariates versus observations, and the P1 persistence-seam decision. Their endorsement is of the architectural foundation, not implementation or deployment readiness. The subsequent owner latest-only amendment received the targeted fourth review above; it still requires the listed P1/P2/P3/P4 implementation and tests.

The source review does not prove live station/config completeness, server model/forcing availability, archive vintages, cron timezone, installed SDK/API versions, runtime capacity or deployed image revisions. These are P1/P5 checks, not reasons to claim the current deployment is ready. Acceptance remains with the user, including the reconstruction policy above.

```json
{"phases":{"M1":{"depends_on":[],"parallel_agents":1},"M2":{"depends_on":["M1"],"parallel_agents":1},"L1":{"depends_on":["M2"],"parallel_agents":1},"L2":{"depends_on":["L1"],"parallel_agents":1}}}
```
