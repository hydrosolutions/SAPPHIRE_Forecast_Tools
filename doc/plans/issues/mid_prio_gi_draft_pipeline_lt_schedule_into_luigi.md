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

**Note:** `lt_schedule_query.py` calls `setup_library.load_environment()`
inside `query_schedule()` (not at module top-level), which reads the `.env`
file and may set additional env vars. The container environment
(`ieasyhydroforecast_env_file_path`, `IN_DOCKER=True`) must be sufficient
for this call to succeed — same as the current bare `docker run` provides.

**JSON output contract** (verified against `lt_schedule_query.py`):
- `active_modes`: `list[str]` — always present
- `skipped_modes`: `dict[str, str]` — always present (may be `{}`)
- `skill_metric_types`: `list[str]`, sorted — always present
- stdout is clean JSON (all logging goes to stderr via explicit
  `logging.StreamHandler(sys.stderr)`)
- `--today` is the only CLI argument (type `str`, parsed as
  `pd.Timestamp(args.today)`)

### D3: `RunLongTermWorkflow.run()` uses Luigi dynamic dependencies

Luigi's `yield` in `run()` is the standard pattern for dynamic DAGs (supported
since Luigi 2.x; this codebase uses Luigi 3.6.0). Verified in Luigi 3.6.0
source: `worker.py` `_run_get_new_deps()` explicitly handles generator tasks.

**Why `requires()` cannot be used instead:** `requires()` is evaluated once at
DAG construction time (`worker.py:884`, `_add()` method). It is NOT
re-evaluated after upstream tasks complete. Since `LTScheduleQuery` has not
yet run when the DAG is built, the JSON file does not exist, and `requires()`
cannot read it. Dynamic deps via `yield` in `run()` are the only correct
Luigi mechanism for this pattern.

The workflow:

1. `requires()` returns `LTScheduleQuery` → runs and completes first
2. `run()` reads the schedule JSON from the shared volume
3. `run()` yields the forecast + postprocessing + cleanup tasks

**Critical Luigi semantics** (verified against Luigi source + docs):

- `run()` **restarts from the top** on every re-execution cycle. Luigi calls
  `task.run()` fresh each cycle (`worker.py:138`). If yielded deps are not
  yet complete, the generator is discarded and `run()` is called again from
  line 1. When all deps are complete, Luigi calls `task_gen.send(paths)` to
  resume the generator past the yield point and execute post-yield code.
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

- Add `import json` to the stdlib imports block (lines 13–18) — needed by
  `_read_schedule_result()` in Phase 2
- Add class `LTScheduleQuery(DockerTaskBase)` near the other long-term tasks
  (~line 2055)
- `SCHEDULE_RESULT_PATH` — **class attribute**, evaluated at import time using
  the module-level `env` object (line 32 of `pipeline_docker.py`; `env` is an
  `Environment` instance, not a dict — accessed via `env.get(key)`, which
  delegates to `os.getenv(key)`). Same pattern as
  `LongTermPostProcessing.docker_logs_file_path` (lines 2124–2128).
  Value: `get_bind_path(env.get('ieasyforecast_intermediate_data_path'))` +
  `'/lt_schedule_result.json'` — both the lt-forecasting child container and
  the pipeline container see this path (same volume mount, same bind path).
  **Safety:** If `ieasyforecast_intermediate_data_path` is unset,
  `get_bind_path(None)` raises `TypeError` at import time — same failure mode
  as the existing `MARKER_DIR` module-level expression. Tests are protected
  by `conftest.py` `setdefault` (line 36). Production `.env` always contains
  the key.
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
  - **Delete stale JSON before launch:**
    ```python
    if os.path.exists(SCHEDULE_RESULT_PATH):
        os.remove(SCHEDULE_RESULT_PATH)
    ```
    The `os.path.exists` guard is required — on first run, the file does not
    exist and bare `os.remove()` would raise `FileNotFoundError`. This
    prevents reading a valid-but-stale result from a previous pipeline run
    if the current container fails to start (see "Known Pre-Existing Issues —
    Exit code ignored"). The stdout redirect (`> file`) truncates on
    container exec, but if the container never starts (Docker daemon error,
    OOM before exec), no truncation occurs and the old file would be
    silently reused.
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

2. Add a helper method to centralize the override-vs-schedule decision:
   ```python
   def _parse_override_modes(self):
       """Parse active_modes parameter into a clean list.

       Returns empty list if active_modes is empty, whitespace-only,
       or contains only separators (e.g. ","). Both requires() and
       run() use this to make a consistent override-vs-schedule
       decision — they must never diverge.
       """
       if not self.active_modes.strip():
           return []
       return [m.strip() for m in self.active_modes.split(",")
               if m.strip()]
   ```

3. Replace `requires()`:
   ```python
   def requires(self):
       if not self._parse_override_modes():
           return LTScheduleQuery()
       return []
   ```

4. Add module-level helper `_read_schedule_result()`:
   - Reads `LTScheduleQuery.SCHEDULE_RESULT_PATH`
   - Validates JSON structure: must have `active_modes` (list of strings),
     `skill_metric_types` (list of strings), and `skipped_modes` (dict of
     str→str, always present but may be `{}`). Validate element types —
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
   - Requires `import json` (added in Phase 1)

5. Rewrite `run()` — full pseudocode of the new body:

   ```python
   def run(self):
       # --- Step 1: Determine active modes ---
       # _parse_override_modes() is shared with requires() to ensure
       # the override-vs-schedule decision is always consistent.
       override_modes = self._parse_override_modes()

       if override_modes:
           # Override path: modes provided directly, no schedule query
           modes = override_modes
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
               schedule["skill_metric_types"])  # always present in JSON

       # --- Step 1b: Log schedule decisions for operator visibility ---
       # (Replaces the shell script's "Schedule query result: ..." line)
       if not override_modes and modes:
           print(f"Schedule query: {len(modes)} active mode(s): {modes}")
           for mode, reason in schedule.get("skipped_modes", {}).items():
               print(f"  Skipped {mode}: {reason}")
       elif override_modes:
           print(f"Override: using manually provided modes: {modes}")

       # --- Step 2: Early exit if nothing to do ---
       if not modes:
           print("No long-term forecast modes active today.")
           if not override_modes:
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
       # Luigi only resumes the generator past yield after confirming
       # all yielded deps are complete (worker.py:157). So this code
       # runs AFTER deps complete, not before. However, the pre-existing
       # exit-code-not-checked bug means a failed container can write
       # its marker (DockerTaskBase reports exit_status=0 unconditionally),
       # causing complete() to return True on a failed task. This guard
       # provides defense-in-depth: if a forecast marker is absent despite
       # Luigi thinking the task completed, we fail loudly.
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

   **Post-yield guard (Step 5):** Verified in Luigi 3.6.0 source
   (`worker.py:137–164`): Luigi only resumes the generator (via
   `task_gen.send(paths)`) after confirming all yielded deps are complete
   via `requires.complete()`. If deps are incomplete, the generator is
   discarded and `run()` is called fresh on the next cycle. Post-yield code
   therefore runs only after all deps complete — Luigi does NOT resume the
   generator prematurely.

   The guard's real value is defense against the **pre-existing exit-code
   bug**: `run_docker_container()` sets `exit_status = 0` unconditionally
   (line 331), so a failed forecast container may still write its marker.
   Luigi then considers it complete and resumes the generator. The guard
   catches the case where a marker file is genuinely absent (e.g. container
   never started). If the guard raises `RuntimeError`, Luigi marks
   `RunLongTermWorkflow` as FAILED — recovery happens on the next pipeline
   invocation.

**Acceptance criteria:**

- `active_modes=""` (default): `LTScheduleQuery` runs, JSON read, correct
  forecast tasks created
- `active_modes="month_0"` (override): no schedule query, tasks created
  directly
- `active_modes=","` or `" "` (truthy-but-empty): `_parse_override_modes()`
  returns `[]`, so `requires()` runs `LTScheduleQuery` and `run()` reads
  the schedule JSON — same behavior as `active_modes=""`
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
- The schedule query block (lines 89–121): starts at `# --- Schedule query:
  determine which long-term modes are active today ---` (line 89), bare
  `docker run` at lines 93–100, JSON parsing at lines 111–112 (`python3 -c`),
  early exit on no modes at lines 118–121. This is three separate `if`
  statements, not a single block.
- The environment variable exports: `export LT_ACTIVE_MODES` (line 147) and
  `export LT_SKILL_METRIC_TYPES` (line 148), immediately before the compose
  invocation

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

**Goal:** Verify data flows with meaningful tests, and update existing tests
broken by the Phase 2 rewrite.

**Files:**
- `apps/pipeline/tests/test_lt_schedule_workflow.py` (new)
- `apps/pipeline/tests/test_long_term_tasks.py` (update `TestRunLongTermWorkflow`)

**Depends on:** Phases 1–3

**CRITICAL — existing tests that break:** 6 of 7 tests in
`TestRunLongTermWorkflow` (lines 106–188 of `test_long_term_tasks.py`) test
`requires()` returning a task list. After Phase 2, `requires()` returns
`LTScheduleQuery()` or `[]`, and task construction moves into `run()`. These
tests must be rewritten:

| Test (line) | Current assertion | Why it breaks |
|---|---|---|
| `test_requires_forecasts_and_postproc` (109) | `requires()` has 2 forecast + 1 postproc | `requires()` → `[]` |
| `test_includes_cleanup_tasks` (127) | `requires()` has LogFileCleanup + DeleteOldMarkerFiles | Same |
| `test_with_notifications` (139) | `requires()` is SendPipelineCompletionNotification | Same |
| `test_task_count_single_mode` (157) | `len(requires()) == 4` | `len([]) != 4` |
| `test_task_count_multiple_modes` (165) | `len(requires()) == 5` | `len([]) != 5` |
| `test_passes_skill_metric_types_to_postproc` (173) | postproc in `requires()` has correct param | `requires()` → `[]`, IndexError |

Only `test_output_path` (150) survives — `output()` is unchanged.

**Replacement test strategy:** The test intent (given modes, correct tasks are
produced) now applies to `run()` as a generator, not `requires()`. New tests
should:
- Test `requires()` contract: returns `LTScheduleQuery()` when
  `active_modes=""`, returns `[]` when modes provided
- Test `run()` generator: write fake schedule JSON, iterate generator,
  verify yielded tasks match expected forecast + postproc + cleanup tasks
- Follow existing repo pattern: test graph structure, avoid `luigi.build()`
  unless strictly necessary (see Test 9 note below)

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
- Verify `run()` does NOT raise `FileNotFoundError` when `SCHEDULE_RESULT_PATH`
  does not exist (first-run case — the `os.path.exists` guard must work)

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

**Test 4b — `RunLongTermWorkflow` with truthy-but-empty override:**
- Set `active_modes=","` → verify `_parse_override_modes()` returns `[]`
- Verify `requires()` returns `LTScheduleQuery()` (schedule query path,
  NOT the override path)
- Set `active_modes=" "` → same assertions
- This tests that `requires()` and `run()` agree on the override-vs-schedule
  decision via the shared `_parse_override_modes()` helper

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

**Test 8 — `RunLongTermWorkflow` completion guard (defense-in-depth):**
- Write valid schedule JSON with `active_modes: ["month_0"]`
- Iterate the generator returned by `run()` to collect yielded tasks
- Use `gen.send(None)` to resume past yield (simulating Luigi's
  `task_gen.send(paths)` after deps complete) — verify `RuntimeError`
  raised when forecast marker `/app/log_lt_forecast_month_0.txt` is absent
- Create the marker file, re-run `run()`, send past yield → verify
  output marker IS written and no exception raised
- This test validates the Step 5 guard against the pre-existing exit-code
  bug (failed containers that write markers), not against premature
  generator resumption (which Luigi prevents at the framework level)

**Test 9 — Luigi `LocalScheduler` integration test (optional, higher effort):**

**Note on repo testing patterns:** All existing pipeline tests (including
`TestRunLongTermWorkflow`) test graph structure only — `requires()`,
`output()`, parameters. None call `luigi.build()` or invoke `.run()`. Test 9
requires mocking the entire Docker execution chain (`docker.from_env()`,
`run_docker_container`, container wait, etc.), which is significantly more
complex than existing tests. Consider deferring this test to a follow-up if
Phase 4 is already large.

If implemented:
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
| `yield` in `run()` is new to this codebase | Medium | Verified viable against Luigi 3.6.0 source (`_run_get_new_deps`). `requires()` alternative is not possible (evaluated once at DAG construction). Thorough tests (Tests 3–5, 7–9). Test 9 optional (higher effort) |
| Post-yield guard purpose | Low | Luigi 3.6.0 only resumes generator after deps confirmed complete — the guard does NOT prevent premature resumption. Its real value is defense-in-depth against the exit-code-not-checked pre-existing bug. Test 8 validates the guard |
| 6 existing `RunLongTermWorkflow` tests break | Medium | Phase 4 includes rewriting these tests: `requires()` contract tests + `run()` generator tests. Only `test_output_path` survives unchanged |
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
| Truthy-but-empty `active_modes` (e.g. `","`, `" "`) | Medium | `_parse_override_modes()` normalizes before branching; shared by `requires()` and `run()` to prevent divergence. Test 4b validates edge cases |
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

## Code Review Findings (2026-04-15)

### RESOLVED — Incorporated into plan above

#### C1: `active_modes` empty-string safety → VERIFIED SAFE + EDGE CASE FIXED
`"".split(",")` returns `['']`, but the list comprehension `if m.strip()`
filters it to `[]`. No `RunLongTermForecast(forecast_mode="")` is ever
created. The shell script also exits before empty modes reach Luigi.

**Edge case found during review (2026-04-15):** Truthy-but-empty values
like `","` or `" "` would cause `requires()` and `run()` to diverge —
`requires()` (testing `if not self.active_modes`) would skip the schedule
query, but `run()` (testing the parsed list) would try to read the
schedule JSON that was never created. Fixed by extracting
`_parse_override_modes()` as a shared helper used by both methods. This
also ensures operators see skip-reason diagnostics instead of silent
"No active modes today" for malformed override inputs.

#### C2: `env` is `Environment` object, not dict → FIXED
Plan corrected to reference `env = Environment(env_file_path)` at line 32.
`env.get(key)` delegates to `os.getenv(key)`. `SCHEDULE_RESULT_PATH` class
attribute follows the same pattern as `MARKER_DIR` — safe in production and
tests.

#### C3: `import json` missing → FIXED
Added to Phase 1 as an explicit required change.

#### C5: `yield` in `run()` confirmed viable → VERIFIED
Verified against Luigi 3.6.0 source. `requires()` is not viable (called once
at DAG construction). `yield` is the only correct mechanism. Plan's D3
section updated with source-level verification.

#### C6: Post-yield guard reframed → FIXED
Guard does NOT prevent premature generator resumption (Luigi prevents that at
framework level). Guard is defense-in-depth against the exit-code-not-checked
pre-existing bug. Plan updated with correct explanation.

#### C7: `skipped_modes` always present → FIXED
Plan corrected: `skipped_modes` is always emitted (possibly `{}`), not
optional. `_read_schedule_result()` validation updated.

#### C8: 6 existing tests break → FIXED
Phase 4 now includes rewriting 6 of 7 `TestRunLongTermWorkflow` tests.
Replacement strategy documented.

#### M1: `os.remove()` guard → FIXED
Explicit `if os.path.exists(...)` guard added to Phase 1 `run()` description
with code snippet. Test 2b includes first-run case.

#### M2: Test 9 contradicts repo patterns → NOTED
Test 9 marked as optional/higher-effort. All existing pipeline tests avoid
`luigi.build()` and test graph structure only.

## What Is NOT Changed

- `lt_schedule_query.py` — no modifications
- `config_forecast.py`, `lt_utils.py` — no modifications
- `RunLongTermForecast` — unchanged
- `LongTermPostProcessing` — unchanged
- `run_pentadal_forecasts.sh`, `run_decadal_forecasts.sh` — not affected
- `apps/run_locally.sh` — independent code path (no Luigi, no Docker),
  uses its own `LT_ACTIVE_MODES` shell variable; unaffected
- Any code in `sapphire/services/` — not touched

## What IS Changed (complete file list)

| File | Change type |
|---|---|
| `apps/pipeline/pipeline_docker.py` | Add `import json`, add `LTScheduleQuery`, modify `RunLongTermWorkflow` (add `_parse_override_modes()`, rewrite `requires()` + `run()`), add `_read_schedule_result()` |
| `bin/docker-compose-luigi.yml` | Remove `--active-modes`, `--skill-metric-types` from `long-term` command |
| `bin/run_long_term_forecasts.sh` | Remove schedule query block, simplify to pentadal pattern |
| `bin/utils/common_functions.sh` | Add `lt_schedule_query` to cleanup trap |
| `apps/pipeline/tests/test_lt_schedule_workflow.py` | New test file |
| `apps/pipeline/tests/test_long_term_tasks.py` | Rewrite 6 of 7 `TestRunLongTermWorkflow` tests for new `requires()` + `run()` contract |
