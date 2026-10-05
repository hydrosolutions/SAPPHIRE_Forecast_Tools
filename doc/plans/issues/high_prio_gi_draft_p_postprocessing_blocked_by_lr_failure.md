## An LR failure blocks postprocessing for every model, so successful ML pentad forecasts are never published; ML/ensemble pentad rows also stop at 2026-09-25 for an undetermined reason (P-062)

**Status**: Draft (2026-10-05)
**Module**: `apps/pipeline` (+ `apps/postprocessing_forecasts`)
**Priority**: **High** — on production kghm the dashboard has no ML/ensemble pentad
forecast since 2026-09-25.
**Labels**: `pipeline`, `luigi`, `blast-radius`, `postprocessing`, `investigation`
**Discovered**: 2026-10-05, production kghm pentad issue day (`maxat_sapphire_2`).
**Related**: **LR-010** (LR exit policy, the lever for Part 1), **PREPQ-024** (upstream
cause of the 2026-10-05 failure), **PP-051** (silent-success family; shipped enum: only
`FAILED` fails, `SKIPPED_NO_RECORDS` benign), **ML-020** (all-null ML rows, same family
as Part 2 candidate c), **ML-021** (ML write silent success), **INFRA-031** (nothing
verifies a run produced the data it owed), **INFRA-046**
(`mid_prio_gi_draft_infra_gateway_failure_blocks_non_consumers.md`; same task-graph
class: one upstream failure blocks tasks that do not need it. Its recorded owner
decision, 2026-09-04: "failed ensemble should not stop models that don't require
ensembles from running"; it is parked at Low pending PREPG-023).

Not to be confused with PP-062 (`high_prio_gi_draft_pp_maintenance_monthly_exit_blocks_tiers.md`).

Two parts: Part 1 is established; Part 2 is an investigation with unknown cause.

---

## Part 1 — LR failure blocks PostProcessingForecasts (established)

### Symptom (2026-10-05)

- `linear_regression` (pentad): all 62 sites `site.predictor: nan`; "Skipping LR pentad
  write: no data for forecast year 2026 ..."; then "CRITICAL: API write failed for
  write_linreg_pentad_forecast_data ..." and exit 1 (LR-010). The retry is the pipeline's
  own `execute_with_retries` (default `max_retries=2`,
  `apps/pipeline/src/timeout_manager.py:102`), not Luigi's retry -> `LinearRegression`
  FAILED.
- `RunMLModel` TFT, TIDE, TSMIXER (PENTAD) all DONE (exit 0).
- Luigi summary: `PostProcessingForecasts(PENTAD)`, `RunPentadalWorkflow`,
  `SendPipelineCompletionNotification` "left pending ... had failed dependencies".
  Wrapper printed "Pentadal forecasting: FAILED (exit 1)."
- A manual re-run the same afternoon failed identically (PREPQ-024: input still absent).

### Root cause (verified by reading the code 2026-10-05)

`apps/pipeline/pipeline_docker.py`:

- `LinearRegression.requires()` returns `PreprocessingRunoff()` (`:643-645`).
- `RunMLModel.requires()` returns `[PreprocessingRunoff(), get_gateway_dependency()]`
  (`:740-743`) — independent of LR.
- `PostProcessingForecasts.requires()` (`:811-835`) starts with
  `[LinearRegression(prediction_mode=...)]`, then appends `RunMLModel` per model and the
  conceptual model if enabled. LR is therefore a hard requirement for postprocessing of
  every model.
- `RunPentadalWorkflow.requires()` (`:1475-1500`) lists LR, the ML models, then
  `PostProcessingForecasts` and cleanup tasks.

Luigi semantics: if any requirement of a task fails, the dependent task never runs. So
the independent, successful ML outputs are never turned into PENTAD rows.

Where PENTAD/DECADE ML and ensemble rows come from (verified):

- The ML module writes only DAY rows: writer `_write_ml_forecast_to_api`
  (`apps/machine_learning/scr/utils_ml_forecast.py:790`) stores `"horizon_type": "day"`
  (`:994`, `:1101`).
- PENTAD/DECADE rows for TFT/TIDE/TSMIXER/ENSEMBLE_MEAN/NEURAL_ENSEMBLE are written only
  by `postprocessing_forecasts`: `src/file_writer.py:252` ->
  `src/api_writer.py` `_write_combined_forecast_to_api` (`:210`; LR rows excluded at
  `:230-244`; date = issue date).
- LR writes `lr_forecasts` itself. The dashboard reads ML from `forecasts` and LR from
  `lr_forecasts` (`apps/forecast_dashboard/src/db.py`, line range not re-verified), so the two can drift independently.

### Design question for the owner (escalate; not decided here)

Should `PostProcessingForecasts` run on whatever upstream tasks succeeded, or is
all-or-nothing intentional?

- EM interaction (verified): `create_ensemble_forecasts`
  (`apps/postprocessing_forecasts/src/ensemble_calculator.py:112`) builds EM from
  skill-qualified candidates including LR (docstring composition example "LR, TFT",
  `:131`; comment `:219`), excluding NE. Single-model compositions are discarded at
  `:223-235`: rows are filtered by `is_multi_model_composition`, stations left with only
  one qualifying model are logged at INFO ("EM: %d station(s) dropped — only 1 qualifying
  model (need 2+)"), and if nothing remains the function logs "No multi-model ensembles
  after filtering" and returns the inputs unchanged. Without LR, EM would be built from
  the ML models alone or omitted for stations where only one model qualifies. Not
  verified: whether EM-without-LR is acceptable to forecasters.
- Options (cross-reference LR-010, do not duplicate its analysis):
  1. A wrapper/sentinel task that always completes (e.g. wraps `LinearRegression`,
     records failure, returns success to Luigi) so PP runs on the remainder. Incomplete
     on its own: `RunPentadalWorkflow.requires()` also lists `LinearRegression` directly
     (`pipeline_docker.py:1480`), and `SendPipelineCompletionNotification` depends on the
     workflow's `base_tasks` (`:1341`, `:1506`); any decoupling must cover those too.
  2. LR exits 0 on a deliberate skip (LR-010's lever); a genuine LR failure still
     blocks PP.
  3. Keep all-or-nothing (intentional) and accept ML being withheld when LR fails.
- Constraints on any option: a genuinely failed LR must still be visible in the
  pipeline outcome. Today the completion notification never fires on a failure (it stays
  pending, see Symptom); the visible failure channel is the per-task
  `send_failure_notification` (called at `pipeline_docker.py:405`/`:425`, defined at `:226`), which is the contract to keep.
  PP must not write partial results that look complete.
- Hazard for any option that runs PP without today's LR: `_read_lr_forecasts_pp_api`
  reads by year range (`apps/postprocessing_forecasts/src/data_reader.py:1943`, called at
  `:2655`). Not verified: whether a stale prior-issue LR row could enter the current
  ensemble mean. A test is required.
- Owner decision needed: keep P-062 separate, or fold Part 1 into INFRA-046's
  task-graph mechanism.

### Acceptance criteria (Part 1, once the owner decides)

- With `LinearRegression` failing and `RunMLModel` succeeding (mocked tasks), the
  outcome matches the chosen option: PP either runs on the ML outputs or is
  deliberately withheld with an explicit message naming LR.
- The LR failure remains visible through the per-task `send_failure_notification` and
  the workflow outcome (the completion notification does not fire on failure today).
- With all tasks succeeding, task graph and outputs are unchanged.
- `ENSEMBLE_MEAN` behaviour without LR is covered by a test whichever option is chosen.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` and `postprocessing_forecasts`
  green, zero skips.

## Part 2 — INVESTIGATION: ML/ensemble PENTAD rows stop at 2026-09-25 (cause unknown)

### Observation (postprocessing DB, 2026-10-05)

- `forecasts` PENTAD `max(date)` for TFT/TIDE/TSMIXER/ENSEMBLE_MEAN/NEURAL_ENSEMBLE =
  2026-09-25.
- `lr_forecasts` PENTAD `max(date)` = 2026-09-30.

So the 2026-09-30 pentad LR succeeded but the ML/ensemble PENTAD rows for 2026-09-30 are
absent. The 2026-10-05 failure explains 10-05 only; it does not explain 09-30. The
09-30 logs should still exist, so read them first: `LogFileCleanup`
(`pipeline_docker.py:1216-1220`) deletes `<intermediate_data>/docker_logs/log_*.txt` older
than 15 days, and the server cron deletes `/home/sapphire/logs/sapphire_*.log` older than
7 days; 09-30 is 5 days before 10-05.

Discriminator: `execute_with_retries` writes the `docker_logs` file only when the
container exits 0 (`pipeline_docker.py:384-391`). On failure it calls
`send_failure_notification` (`:405`, `:425`) and raises; the logs go to
`failure_log_<epoch>.txt` in the same directory (`:239-246`, a name `log_*.txt` does not
match). So absence of `log_postproc_20260930_*` means PP did not succeed that day, which
supports (a).

### Candidate explanations (all unverified)

- (a) `PostProcessingForecasts(PENTAD)` did not complete on 09-30 (an ML task failure or
  timeout, or a PP failure).
- (b) PP ran but its best-effort API write failed or returned `False` and was ignored.
  Context: `file_writer.py:250-257` calls `_write_combined_forecast_to_api` and only
  raises when `require_api` is set; PP-051 documents the swallowed-failure family.
- (c) PP's own combined records for 09-30 had null discharge and were dropped before the
  write (`api_writer.py:410-424`, "Dropped %d null-discharge forecast records before API
  write" at `:420`). That filter acts on PP's combined records, not on ML DAY rows;
  ML-020 (all-null ML rows) is the same family upstream.
- (d) ML exited 0 having written no DAY rows, so `RunMLModel` DONE does not prove DAY rows
  exist (sibling **ML-021**; module_issues.md lists it as Review, implemented 2026-09-09).
  Not verified: whether the kghm deployment runs that code.

### Read-only server checks to discriminate

Run via `docker exec` (mind the quoting):

```bash
docker exec sapphire-postprocessing-db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT model_type, max(date) FROM forecasts
WHERE horizon_type::text ILIKE '\''day'\'' GROUP BY 1;"'

docker exec sapphire-postprocessing-db sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
SELECT horizon_type, model_type, date, count(*), count(forecasted_discharge)
FROM forecasts WHERE date >= DATE '\''2026-09-24'\''
GROUP BY 1,2,3 ORDER BY 3,1,2;"'
```

Log greps in `docker_logs/log_postproc_*` (strings verified in
`apps/postprocessing_forecasts/src/api_writer.py`):

- `Successfully wrote .* combined forecast records` (`:458`)
- `is not ready, skipping combined forecast write` (`:279`)
- `Dropped .* null-discharge` (`:420`)
- `No non-LR forecast records` (`:243`)
- `No combined forecast records to write to API` (`:465`)

Also read the 09-30 task history first (server):

```bash
ls <data_dir>/intermediate_data/docker_logs/ | grep 20260930
grep -A25 "Luigi Execution Summary" /home/sapphire/logs/sapphire_pentadal_forecast_20260930.log
```

How to read the results: DAY rows present with non-null counts for 09-30 and no PENTAD
rows points to (a) or (b); DAY rows missing or all-null points to (d) (ML-021) or (c)
(and ML-020); a "Successfully wrote" line for 09-30 would contradict (a)/(b) and
implicate a query/dashboard problem. Not verified: that these 09-30 files exist on the
server.

### Observability gap

`bin/handover_healthcheck.sh` judges pentad/decad freshness only from the LR endpoint
(`/lr-forecast/` fetch at `:162`, freshness check `:253-281`) and never checks the
`forecasts` table. That is why ML/ensemble PENTAD stopping at 09-25 raised no alert for 10
days. Cross-reference INFRA-031. Proposed (not decided): also check per-model freshness of
the `forecasts` table.

### Close or split rule

Once the cause is known: if it is the same LR-blocks-PP mechanism (a), fold into Part 1;
otherwise split into its own issue (owner module per cause: pipeline, PP, or ML) and
reduce this file to Part 1.

## Not in scope

LR's exit policy (LR-010) and the runoff coverage signal and back-fill (PREPQ-024).
No code is changed by this file.
