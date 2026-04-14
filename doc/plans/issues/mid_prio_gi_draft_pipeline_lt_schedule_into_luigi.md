# Pipeline: Move long-term schedule query into Luigi pipeline

| Field       | Value                                                       |
|-------------|-------------------------------------------------------------|
| **Module**  | `pipeline`, `bin/`                                          |
| **Priority**| Mid                                                         |
| **Status**  | Draft                                                       |
| **Branch**  | TBD (branch from `maxat_sapphire_2`)                        |

## Problem

`bin/run_long_term_forecasts.sh` uses a bare `docker run` to execute
`lt_schedule_query.py` before starting the Luigi pipeline. This is
architecturally inconsistent with the pentadal/decadal scripts and causes
two operational failures:

1. **Image not pulled.** The bare `docker run` does not check whether
   `mabesa/sapphire-lt-forecasting` exists locally. If the image is missing
   (e.g. after cleanup or first deploy), the command fails silently because
   stderr is redirected to `/dev/null`.

2. **Opaque errors.** The `2>/dev/null` suppresses all diagnostic output from
   `lt_schedule_query.py`, making it impossible to diagnose config or
   environment issues on the server.

The pentadal and decadal scripts avoid both problems by delegating all
Docker work to `docker compose run` → Luigi, where `DockerTaskBase` handles
image pulling, retries, timeouts, and log capture.

## Goal

Move the schedule query inside the Luigi pipeline so that
`run_long_term_forecasts.sh` matches the pentadal pattern:
start Luigi daemon → submit workflow → done.

## Architecture: Before vs After

**Before:**

```
Shell script
  └─ docker run lt-forecasting (schedule query)  ← bare docker run, no pull
  └─ parse JSON, export LT_ACTIVE_MODES
  └─ docker compose run long-term
       └─ Luigi: RunLongTermWorkflow(active_modes="month_0,quarter")
            ├─ RunLongTermForecast(forecast_mode="month_0")
            ├─ RunLongTermForecast(forecast_mode="quarter")
            └─ LongTermPostProcessing
```

**After:**

```
Shell script  (same structure as run_pentadal_forecasts.sh)
  └─ docker compose run long-term
       └─ Luigi: RunLongTermWorkflow()
            ├─ LTScheduleQuery  ← DockerTaskBase, lt-forecasting container
            ├─ (reads JSON from shared volume)
            ├─ PreprocessingRunoff + Gateway  ← unchanged, via RunLongTermForecast.requires()
            ├─ RunLongTermForecast(forecast_mode="month_0")
            ├─ RunLongTermForecast(forecast_mode="quarter")
            └─ LongTermPostProcessing
```

## Design Decisions

### D1: `LTScheduleQuery` extends `DockerTaskBase`

Reuses the existing image-pull logic (`there_is_a_newer_image_on_docker_hub`
in `pipeline_utils.py`), retry handling, timeout management, log capture, and
notification infrastructure. This solves the original bug (image not being
pulled) when network is available — `DockerTaskBase.run_docker_container()`
checks Docker Hub for the image and pulls it automatically if missing or
outdated. On air-gapped deployments, both the old bare `docker run` and
`DockerTaskBase` silently skip the pull; this is a pre-existing limitation.

### D2: Container writes JSON to shared volume via stdout redirect

Command: `["sh", "-c", "uv run python lt_schedule_query.py > {output_path}"]`

No changes to `lt_schedule_query.py` are needed. The script already sends JSON
to stdout and logs to stderr. The shell redirect writes the JSON to a file on
the shared `intermediate_data` volume, which is mounted in both the
lt-forecasting child container and the pipeline container.

**Note:** `lt_schedule_query.py` calls `setup_library.load_environment()` at
startup, which reads the `.env` file and may set additional env vars. The
container environment (`ieasyhydroforecast_env_file_path`, `IN_DOCKER=True`)
must be sufficient for this call to succeed — same as the current bare
`docker run` provides.

### D3: `RunLongTermWorkflow.run()` uses Luigi dynamic dependencies

Luigi's `yield` in `run()` is the standard pattern for dynamic DAGs (supported
since Luigi 2.x; this codebase uses >= 3.5.0). Verified in Luigi source:
`worker.py` `_run_get_new_deps()` explicitly handles generator tasks.

The workflow:

1. `requires()` returns `LTScheduleQuery` → runs and completes first
2. `run()` reads the schedule JSON from the shared volume
3. `run()` yields the forecast + postprocessing + cleanup tasks

**Critical Luigi semantics** (verified against Luigi source + docs):

- `run()` **restarts from the top** on every re-execution cycle. After yielded
  tasks complete, Luigi calls `run()` fresh — it does NOT resume the generator.
  All code before `yield` must be idempotent. Reading a JSON file and
  constructing task objects are both idempotent, so this is safe.
- A **failed dynamic dep** does NOT raise an exception in `run()`. The parent
  task is silently set to `UPSTREAM_FAILED`. Failures are visible in the Luigi
  web UI but cannot be caught with try/except inside the generator.
- **Yielding a list works.** `yield base_tasks` where `base_tasks` is a list
  is handled by Luigi's `DynamicRequirements` wrapper + `flatten()` (verified
  in `worker.py` line 154 and `task.py` line 966). Lists, nested lists, and
  single tasks are all flattened correctly. No need for `yield from`.
- Every yielded task **must have a valid `output()`**, or Luigi enters an
  infinite loop (issue #814). Verified: `LogFileCleanup`,
  `DeleteOldMarkerFiles`, `SendPipelineCompletionNotification`, and all forecast
  tasks have output targets.
- The Luigi **web UI** will not show the full DAG upfront — dynamic deps only
  appear after the parent task starts running. This is cosmetic, not functional.

This is a **new pattern** in this codebase — no existing task uses `yield` in
`run()` or reads upstream output. It requires careful testing.

### D4: `active_modes` becomes an optional override parameter

Default: `""` (empty — means "determine from schedule query"). When provided
explicitly (e.g. `--active-modes month_0`), the schedule query is skipped and
the given modes are used directly. This preserves backward compatibility for
debugging and manual invocations.

## Known Pre-Existing Issues (NOT fixed here)

### Exit code ignored

`DockerTaskBase.run_docker_container()` sets `exit_status = 0` after
`container.wait()` without reading the container's actual exit code
(`pipeline_docker.py` line 331). A container that exits non-zero is reported as
success. This affects ALL DockerTaskBase tasks.

**Impact on `LTScheduleQuery`:** If the schedule query container fails,
`execute_with_retries()` still writes the output marker and reports success.
`RunLongTermWorkflow.run()` will then hit `_read_schedule_result()` validation
and fail with a clear `RuntimeError`. The error message must mention possible
container failure — not just "malformed JSON" — so operators know to check the
container logs file.

Mitigation: `_read_schedule_result()` validates the schedule JSON file contents
(not just the marker) and includes the schedule query log path in the error
message for diagnostics.

### Timeout notification dead code

`execute_with_retries()` has unreachable code after the timeout branch
(`pipeline_docker.py` lines 396–405). The `break` at line 399 exits the loop
before `send_failure_notification()` is called. If any DockerTaskBase task
(including `LTScheduleQuery`) times out, no failure email is sent. Operators
must monitor Luigi's web UI for timeout status.

## Implementation Plan

### Phase 1: Add `LTScheduleQuery` task

**Goal:** DockerTaskBase task that runs `lt_schedule_query.py` in the
lt-forecasting container and writes JSON to the shared volume.

**Files:** `apps/pipeline/pipeline_docker.py` (additive only)

**Details:**

- Add class `LTScheduleQuery(DockerTaskBase)` near the other long-term tasks
  (~line 2055)
- `SCHEDULE_RESULT_PATH` — **class attribute**, evaluated at import time using
  the module-level `env` dict (line 31 of `pipeline_docker.py`), same pattern
  as `LongTermPostProcessing.docker_logs_file_path` (lines 2124–2128).
  Value: `get_bind_path(env.get('ieasyforecast_intermediate_data_path'))` +
  `/lt_schedule_result.json` — both the lt-forecasting child container and the
  pipeline container see this path (same volume mount, same bind path)
- `today = luigi.Parameter(default="")` — optional date override for testing.
  When non-empty, appended as `--today {self.today}` to the container command
- `output()` → `luigi.LocalTarget("/app/log_schedule_query.txt")` (standard
  ephemeral marker, re-runs every session)
- `requires()` → empty list (reads config files, not pipeline outputs)
- `docker_logs_file_path` — use a **class attribute** (not `@property`),
  following the `LongTermPostProcessing` pattern. `LTScheduleQuery` has no
  per-instance parameters that vary the path (unless `today` is set, but log
  paths don't need to encode the date override)
- Do **NOT** add `resources = {"lt_memory": 1}`. The `lt_memory` resource lock
  is only for the 12 GB forecast tasks. Adding it to the schedule query would
  cause a deadlock: the workflow waits for the query, but the query can't
  acquire `lt_memory` until a forecast finishes — and no forecast has started
- `run()`:
  - **Delete stale JSON before launch:** `os.remove(SCHEDULE_RESULT_PATH)` if
    the file exists. This prevents reading a valid-but-stale result from a
    previous pipeline run if the current container fails to start (see
    "Known Pre-Existing Issues — Exit code ignored"). The stdout redirect
    (`> file`) truncates on container exec, but if the container never starts
    (Docker daemon error, OOM before exec), no truncation occurs and the old
    file would be silently reused.
  - Volumes: `setup_docker_volumes(env, ["ieasyforecast_configuration_path",
    "ieasyforecast_intermediate_data_path"])` — same as `RunLongTermForecast`
  - Environment: `ieasyhydroforecast_env_file_path`, `IN_DOCKER=True`
  - Do **NOT** include `RUN_MODE=forecast` — this is a schedule query, not a
    forecast run. `RunLongTermForecast` passes it; `LTScheduleQuery` must not.
  - Do **NOT** call `get_docker_host_env_overrides()`. The schedule query reads
    local config files only — it does not connect to remote Docker or APIs.
    (If a future setup requires remote Docker for the schedule query container
    itself, add it then.)
  - Build command: base is `uv run python lt_schedule_query.py`; if
    `self.today` is non-empty, append `--today {self.today}`; then wrap as
    `["sh", "-c", f"{base_cmd} > {SCHEDULE_RESULT_PATH}"]`
  - Container name: `f"lt_schedule_query_{attempt}"` — must match the
    `lt_schedule_query` pattern used by `stop_and_remove_container` in
    Phase 3's cleanup addition. Note: uses underscores (matching
    `lt_forecast_*`), not hyphens (as in `lt-postprocessing_*`). The Docker
    `name` filter does substring matching, so both conventions work with
    `stop_and_remove_container`
  - `execute_with_retries` with `image_name="sapphire-lt-forecasting"`,
    `mem_limit="2g"`, `network="host"`

**Acceptance criteria:**

- Class compiles, no import errors
- Container parameters match `RunLongTermForecast` pattern (same volumes,
  env file, image) — but without `RUN_MODE` or `get_docker_host_env_overrides`
- Container name follows `lt_schedule_query_{attempt}` pattern
- `SCHEDULE_RESULT_PATH` is a class attribute in the intermediate_data directory
- No `resources` attribute on the class
- `run()` deletes `SCHEDULE_RESULT_PATH` before calling `execute_with_retries`

### Phase 2: Modify `RunLongTermWorkflow`

**Goal:** Make the workflow self-contained by reading modes from
`LTScheduleQuery` output.

**Files:** `apps/pipeline/pipeline_docker.py`

**Depends on:** Phase 1

**Details:**

1. Change `active_modes` parameter: `luigi.Parameter(default="")`

2. Replace `requires()`:
   ```python
   def requires(self):
       if not self.active_modes:
           return LTScheduleQuery()
       return []
   ```

3. Add module-level helper `_read_schedule_result()`:
   - Reads `LTScheduleQuery.SCHEDULE_RESULT_PATH`
   - Validates JSON structure: must have `active_modes` (list of strings)
     and `skill_metric_types` (list of strings). Validate element types —
     `all(isinstance(m, str) for m in result["active_modes"])` — to prevent
     a `None` or integer element from causing a confusing error deep in
     Docker container setup
   - Raises clear `RuntimeError` if file missing, empty, or malformed.
     **The error message must include the path to the schedule query's
     `docker_logs_file_path`** so operators can diagnose whether the
     container failed (see "Known Pre-Existing Issues — Exit code ignored").
     Example: `"Schedule result file is empty or malformed at {path}. Check
     schedule query logs at {LTScheduleQuery.docker_logs_file_path}"`
   - This function is pure and idempotent (required by Luigi's re-execution
     semantics — see D3)

4. Rewrite `run()` — full pseudocode of the new body:

   ```python
   def run(self):
       # --- Step 1: Determine active modes ---
       if self.active_modes:
           # Override path: modes provided directly, no schedule query
           modes = [m.strip() for m in self.active_modes.split(",")
                    if m.strip()]
           skill_types = self.skill_metric_types
           if skill_types == "MONTHLY" and any(
                   m not in ("month_0",) for m in modes):
               print(
                   "Warning: active_modes provided manually but "
                   "skill_metric_types defaults to MONTHLY. "
                   "Pass --skill-metric-types if needed.")
       else:
           # Schedule query path: read result from shared volume
           schedule = _read_schedule_result()
           modes = schedule["active_modes"]
           skill_types = ",".join(
               schedule.get("skill_metric_types", ["MONTHLY"]))

       # --- Step 1b: Log schedule decisions for operator visibility ---
       # (Replaces the shell script's "Schedule query result: ..." line)
       if not self.active_modes and modes:
           print(f"Schedule query: {len(modes)} active mode(s): {modes}")
           for mode, reason in schedule.get("skipped_modes", {}).items():
               print(f"  Skipped {mode}: {reason}")
       elif self.active_modes:
           print(f"Override: using manually provided modes: {modes}")

       # --- Step 2: Early exit if nothing to do ---
       if not modes:
           print("No long-term forecast modes active today.")
           if not self.active_modes:
               for mode, reason in schedule.get(
                       "skipped_modes", {}).items():
                   print(f"  Skipped {mode}: {reason}")
           # Write completion markers (same pattern as current run())
           os.makedirs(
               os.path.dirname(self.docker_logs_file_path), exist_ok=True)
           with open(self.docker_logs_file_path, "w") as f:
               f.write("No active modes today")
           with self.output().open("w") as f:
               f.write("No active modes today")
           return

       # --- Step 3: Build task list (same logic as old requires()) ---
       modes_str = ",".join(modes)
       base_tasks = []
       for mode in modes:
           base_tasks.append(
               RunLongTermForecast(forecast_mode=mode))
       base_tasks.append(LongTermPostProcessing(
           active_modes=modes_str,
           skill_metric_types=skill_types))
       base_tasks.append(LogFileCleanup())
       base_tasks.append(DeleteOldMarkerFiles())

       # --- Step 4: Yield dynamic dependencies ---
       if self.send_notifications:
           yield SendPipelineCompletionNotification(
               custom_message=f"LONG_TERM {self.custom_message}",
               depends_on=base_tasks)
       else:
           yield base_tasks

       # --- Step 5: Guard + write completion markers ---
       # Post-yield code may execute before dynamic deps complete
       # (depends on Luigi's generator iteration strategy — see D3).
       # Guard: verify forecast markers exist before writing our own
       # output, to prevent a false "complete" signal if a forecast
       # task failed silently (exit code bug) or never started.
       for mode in modes:
           marker = f"/app/log_lt_forecast_{mode}.txt"
           if not os.path.exists(marker):
               raise RuntimeError(
                   f"Forecast task for mode '{mode}' did not produce "
                   f"marker {marker}. Check Luigi web UI for "
                   f"UPSTREAM_FAILED status.")

       os.makedirs(
           os.path.dirname(self.docker_logs_file_path), exist_ok=True)
       with open(self.docker_logs_file_path, "w") as f:
           f.write(
               f"Long-term workflow for {ORGANIZATION} completed at "
               f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
       with self.output().open("w") as f:
           f.write("Long-term workflow completed")
   ```

   **Idempotency note:** Steps 1–3 re-execute on every Luigi re-entry cycle
   (see D3). Reading the JSON file and constructing task objects are both
   idempotent. Luigi deduplicates tasks by `task_id` (family + params), so
   re-creating the same `RunLongTermForecast(forecast_mode="month_0")` on
   the second cycle yields the already-completed task and fast-forwards.

   **Post-yield guard (Step 5):** Python generator semantics mean code after
   `yield` executes when Luigi iterates past the yield point. Depending on
   whether the Luigi version in use iterates the generator fully in one pass
   or stops at each yield, Step 5 may run before dynamic deps complete.
   The marker-existence guard prevents writing the output target prematurely:
   on the first cycle (before forecasts run), the guard raises, so the output
   is never created; on the final cycle (after forecasts complete), the guard
   passes and the output is written correctly. The pipeline container is
   ephemeral (`--rm`), so the raised exception on a premature cycle is
   harmless — Luigi's in-memory scheduler state is authoritative during the
   run.

**Acceptance criteria:**

- `active_modes=""` (default): `LTScheduleQuery` runs, JSON read, correct
  forecast tasks created
- `active_modes="month_0"` (override): no schedule query, tasks created
  directly
- Empty `active_modes` in schedule JSON: workflow completes with "no active
  modes" message, marker written
- Missing/malformed JSON: workflow fails with clear `RuntimeError`

### Phase 3: Simplify compose and shell script

**Goal:** Remove schedule query from shell, match pentadal structure.

**Files:** `bin/docker-compose-luigi.yml`, `bin/run_long_term_forecasts.sh`,
`bin/utils/common_functions.sh`

**Depends on:** Phase 2

**Changes to `bin/docker-compose-luigi.yml`:**

Remove `--active-modes` and `--skill-metric-types` from the `long-term`
service command:

```yaml
command: ['uv', 'run', 'luigi', '--scheduler-host', 'luigi-daemon',
         '--scheduler-port', '8082',
         '--module', 'apps.pipeline.pipeline_docker',
         'RunLongTermWorkflow']
```

**Changes to `bin/run_long_term_forecasts.sh`:**

Remove:
- The schedule query block: starts at `# --- Schedule query: determine which
  long-term modes are active today ---` and ends at the `fi` after
  `"No long-term forecast modes active today. Exiting."` — includes the bare
  `docker run`, the `python3 -c` JSON parsing, and the early exit on no modes
- The environment variable exports: `export LT_ACTIVE_MODES` and
  `export LT_SKILL_METRIC_TYPES` (two lines before `temp_luigi.cfg` creation)

Replace the dry-run block (starts at `if $DRY_RUN;` with the schedule query
echo lines, ends at `exit 0` / `fi`) with a simplified version that no
longer references the bare `docker run`. The new block should still show enough
detail for operators to debug:

```bash
if $DRY_RUN; then
    echo "|"
    echo "| [DRY RUN] Would submit RunLongTermWorkflow to Luigi."
    echo "| Luigi will run LTScheduleQuery internally to determine active modes."
    echo "|   Schedule query image: mabesa/sapphire-lt-forecasting:${ieasyhydroforecast_backend_docker_image_tag}"
    echo "|   Config: ${ieasyhydroforecast_env_file_path}"
    echo "|   Pipeline submission:"
    echo "|     docker compose -f bin/docker-compose-luigi.yml run --rm long-term"
    echo "|"
    echo "| [DRY RUN] Validation complete. Exiting without starting containers."
    exit 0
fi
```

Resulting script flow (matches `run_pentadal_forecasts.sh`):

1. Source common_functions, parse args, print banner
2. Read configuration, validate compose file
3. Set Luigi scheduler vars
4. Dry-run check (simplified — see above)
5. Establish SSH tunnel, set trap, set COMPOSE_PROJECT_NAME
6. Start Luigi daemon if not running, wait for ready
7. Write `temp_luigi.cfg` (preserve unchanged), submit workflow via
   `docker compose run`
8. Print completion

**Changes to `bin/utils/common_functions.sh`:**

Add `stop_and_remove_container lt_schedule_query` to the
`cleanup_long_term_forecasting_containers` function. `DockerTaskBase`
self-cleans containers on normal exit (`container.remove()` in
`run_docker_container`), but if the pipeline process is killed with SIGKILL
between container start and removal, the container is left dangling. The
cleanup trap exists for exactly this case.

**Acceptance criteria:**

- No `docker run` commands in `run_long_term_forecasts.sh`
- `LT_ACTIVE_MODES` and `LT_SKILL_METRIC_TYPES` do not appear in the script
  or compose file
- Dry-run path prints the simplified plan and exits
- `cleanup_long_term_forecasting_containers` includes `lt_schedule_query`
- Script structure matches `run_pentadal_forecasts.sh`

### Phase 4: Tests

**Goal:** Verify data flows with meaningful tests.

**Files:** `apps/pipeline/tests/test_lt_schedule_workflow.py` (new)

**Depends on:** Phases 1–3

**Test 1 — `LTScheduleQuery` container parameters:**
- Verify `image_name` is `"sapphire-lt-forecasting"`
- Verify `command` includes `lt_schedule_query.py` and stdout redirect to the
  correct path
- Verify `container_name` matches `lt_schedule_query_{attempt}` pattern
- Verify volumes include `ieasyforecast_configuration_path` and
  `ieasyforecast_intermediate_data_path`
- Verify environment includes `ieasyhydroforecast_env_file_path` and
  `IN_DOCKER=True`
- Verify environment does NOT include `RUN_MODE`

**Test 2 — `_read_schedule_result()` validation:**
- Valid JSON with active modes → returns parsed dict
- Valid JSON with empty `active_modes` → returns dict with empty list
- Missing file → raises `RuntimeError` mentioning the log file path
- Empty file → raises `RuntimeError` mentioning the log file path
- Malformed JSON → raises `RuntimeError` mentioning the log file path
- JSON missing `active_modes` key → raises clear error
- `active_modes` contains non-string element (e.g. `[null]`, `[123]`) →
  raises clear error about element types

**Test 2b — `LTScheduleQuery` stale file cleanup:**
- Write a valid JSON file to `SCHEDULE_RESULT_PATH` before `run()` executes
- Verify `run()` deletes the old file before launching the container
- If `execute_with_retries` is mocked to succeed without writing a new file,
  `_read_schedule_result()` should raise (file missing), NOT silently reuse
  the stale data

**Test 3 — `RunLongTermWorkflow` with schedule-driven modes:**
- Write fake schedule JSON:
  `{"active_modes": ["month_0", "quarter"], "skill_metric_types": ["MONTHLY"]}`
- Verify `run()` yields 2× `RunLongTermForecast` + `LongTermPostProcessing`
  + cleanup tasks
- Verify `LongTermPostProcessing` receives `active_modes="month_0,quarter"`
  and `skill_metric_types="MONTHLY"`

**Test 4 — `RunLongTermWorkflow` with explicit override:**
- Set `active_modes="month_0"` directly
- Verify `LTScheduleQuery` is NOT in `requires()`
- Verify single `RunLongTermForecast(forecast_mode="month_0")` task

**Test 5 — `RunLongTermWorkflow` with empty schedule:**
- Write schedule JSON: `{"active_modes": [], "skill_metric_types": []}`
- Verify workflow completes cleanly, marker written, no forecast tasks yielded

**Test 6 — Shell script structural test:**
- Verify `run_long_term_forecasts.sh` contains no `docker run` commands
  (only `docker compose`)
- Verify `LT_ACTIVE_MODES` does not appear in the script
- Verify `lt_schedule_query.py` does not appear in the script
- Verify `cleanup_long_term_forecasting_containers` in `common_functions.sh`
  includes `lt_schedule_query`

**Test 7 — `LTScheduleQuery` with `--today` override:**
- Set `today="2026-03-15"` on the task
- Verify the container command includes `--today 2026-03-15`
- Set `today=""` (default) → verify command does NOT include `--today`

**Test 8 — `RunLongTermWorkflow` completion guard (post-yield safety):**
- Write valid schedule JSON with `active_modes: ["month_0"]`
- Iterate the generator returned by `run()` to collect yielded tasks
- Verify that continuing the generator (calling `next()` again to trigger
  post-yield code) raises `RuntimeError` when the forecast marker file
  `/app/log_lt_forecast_month_0.txt` does NOT exist
- Create the marker file, re-run `run()`, iterate past yield → verify
  output marker IS written and no exception raised
- This test validates the Step 5 guard against premature output marker
  writing, independent of Luigi's generator iteration strategy

**Test 9 — Luigi `LocalScheduler` integration test:**
- Use `luigi.build()` with `local_scheduler=True` (no daemon needed)
- Mock `DockerTaskBase.run_docker_container` to:
  - For `LTScheduleQuery`: write valid schedule JSON to `SCHEDULE_RESULT_PATH`
    and return success
  - For `RunLongTermForecast`: write the forecast marker and return success
  - For other DockerTaskBase tasks: return success
- Run `RunLongTermWorkflow` with `active_modes=""` (schedule query path)
- Verify: `LTScheduleQuery` ran, forecast tasks ran, postprocessing ran,
  workflow output marker exists
- This tests the actual Luigi dynamic dependency cycle with the project's
  Luigi version, catching any mismatch between the plan's generator
  semantics assumptions and reality

**Acceptance criteria:** All tests pass with
`SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline`

### Phase 5: Rebuild pipeline image

**Goal:** Ensure the modified `pipeline_docker.py` is baked into the Docker
image before the shell script changes go live.

**Files:** No code changes — build/push step only.

**Depends on:** Phases 1–3 (all code changes merged)

**Details:**

After merging, rebuild the pipeline image:

```bash
cd /path/to/SAPPHIRE_forecast_tools
docker build -f apps/pipeline/Dockerfile \
    -t mabesa/sapphire-pipeline:${ieasyhydroforecast_backend_docker_image_tag} .
```

If using a remote registry, push with the appropriate tag.

**Atomic deployment required** — all three components (pipeline image, compose
file, shell script) must be updated together in a single deployment window.
Partial deploys fail as follows:

| Scenario | Result |
|---|---|
| New script + old compose + old image | `LT_ACTIVE_MODES` not exported → `--active-modes ''` → old code has required `active_modes` param → **Luigi error** |
| New script + new compose + old image | No `--active-modes` arg → old code missing required param → **Luigi error** |
| Old script + old compose + new image | `LT_ACTIVE_MODES` still exported → override path used → **works** (backward compat) |
| New script + new compose + new image | Schedule query path → **works** |

**Deployment steps on server:**

1. Pull/build the new `mabesa/sapphire-pipeline` image
2. Pull the updated `bin/` scripts and compose file (`git pull`)
3. Verify: steps 1 and 2 complete before the next cron invocation
4. Run `bash bin/run_long_term_forecasts.sh` — now works with the new flow

**Acceptance criteria:**

- `docker run --rm mabesa/sapphire-pipeline:TAG python -c
  "from apps.pipeline.pipeline_docker import LTScheduleQuery"` exits 0

## Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1, "goal": "Add LTScheduleQuery task" },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1, "goal": "Modify RunLongTermWorkflow" },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1, "goal": "Simplify compose + shell" },
    "P4": { "depends_on": ["P1", "P2", "P3"], "parallel_agents": 1, "goal": "Tests" },
    "P5": { "depends_on": ["P1", "P2", "P3"], "parallel_agents": 0, "goal": "Rebuild pipeline image (manual)" }
  }
}
```

## Risk Summary

| Risk | Severity | Mitigation |
|---|---|---|
| `yield` in `run()` is new to this codebase | Medium | Thorough tests (Tests 3–5, 7–9), documented comments, full pseudocode in plan. Test 9 uses `luigi.build()` with `LocalScheduler` to validate actual generator behavior |
| Post-yield code may execute before dynamic deps complete | Medium | Step 5 guard checks forecast marker files exist before writing output marker. If markers absent, raises `RuntimeError` instead of writing a false completion signal. Test 8 validates this guard |
| `run()` restarts from top on re-execution | Medium | All code before `yield` is idempotent (file read + task construction). Documented in D3 |
| Failed dynamic dep → silent `UPSTREAM_FAILED` | Medium | Visible in Luigi web UI; no in-code catch possible. Document for operators |
| Stale schedule JSON from previous run | Medium | `LTScheduleQuery.run()` deletes old file before container launch; if container fails to start, file is absent and `_read_schedule_result()` raises clear error |
| Schedule JSON file empty after container failure | Medium | `_read_schedule_result()` validates contents and includes log file path in error message for diagnostics |
| `lt_memory` resource cargo-culted onto `LTScheduleQuery` | Medium | Explicit prohibition in Phase 1; would cause deadlock if added |
| `RUN_MODE` or host env overrides cargo-culted onto `LTScheduleQuery` | Low | Explicit exclusion in Phase 1 details; schedule query reads config only |
| Concurrent pipeline runs corrupt shared JSON | Low | Concurrent LT invocations are operationally forbidden (cron ensures single). Document this assumption |
| Image tag `local` + image missing | Low | `DockerTaskBase` handles: `ImageNotFound` → pull attempted → pull fails → `execute_with_retries` retries → fails with RuntimeError |
| `container.wait()` exit code not checked (pre-existing) | Medium | `_read_schedule_result()` validates JSON and includes log path in error. See "Known Pre-Existing Issues" |
| Timeout notification dead code (pre-existing) | Low | Documented in "Known Pre-Existing Issues". Operators must monitor Luigi web UI for timeouts |
| Volume path mismatch between containers | Low | Verified: both use `get_bind_path()` → same container-internal path |
| Backward compat: manual `--active-modes` | Low | Parameter kept as optional; when provided, skips schedule query |
| Manual `--active-modes` with default `skill_metric_types` | Low | `run()` prints warning when override path uses default `"MONTHLY"` with non-month_0 modes. Operator must pass `--skill-metric-types` explicitly for quarterly/seasonal |
| Operator visibility regression (schedule decisions) | Low | `run()` prints active modes, skipped modes, and skip reasons — replaces the shell script's `Schedule query result: ...` output |
| `SendPipelineCompletionNotification` task_id ignores `depends_on` | Low | `depends_on` kwarg bypasses Luigi Parameter (stored as `self._depends_on`); task_id is based on `custom_message` only. Safe because only one notification per workflow, but add code comment for future maintainers |
| Partial deployment (image/script/compose out of sync) | **Blocker** | All three components must deploy atomically; partial deploy failure modes documented in Phase 5 |

## Operational Assumptions

- **Single invocation:** Concurrent long-term pipeline runs are not supported.
  The cron schedule must ensure only one invocation at a time. Two simultaneous
  runs would race on `lt_schedule_result.json`.
- **Network for image pull:** `DockerTaskBase` pulls images from Docker Hub
  when they are missing or outdated. On air-gapped servers, images must be
  pre-loaded manually — this is a pre-existing limitation, not introduced here.

## What Is NOT Changed

- `lt_schedule_query.py` — no modifications
- `config_forecast.py`, `lt_utils.py` — no modifications
- `RunLongTermForecast` — unchanged
- `LongTermPostProcessing` — unchanged
- `run_pentadal_forecasts.sh`, `run_decadal_forecasts.sh` — not affected
- Any code in `sapphire/services/` — not touched

## What IS Changed (complete file list)

| File | Change type |
|---|---|
| `apps/pipeline/pipeline_docker.py` | Add `LTScheduleQuery`, modify `RunLongTermWorkflow`, add `_read_schedule_result()` |
| `bin/docker-compose-luigi.yml` | Remove `--active-modes`, `--skill-metric-types` from `long-term` command |
| `bin/run_long_term_forecasts.sh` | Remove schedule query block, simplify to pentadal pattern |
| `bin/utils/common_functions.sh` | Add `lt_schedule_query` to cleanup trap |
| `apps/pipeline/tests/test_lt_schedule_workflow.py` | New test file |
