# FD-007: Update dashboard Docker dataflows to operational mode with logging

**Status**: Implemented
**Module**: forecast_dashboard
**Priority**: Medium
**Labels**: `forecast_dashboard`, `docker`, `operational-mode`

---

## Summary

Update the "Save Changes" and "Trigger Forecasts" dataflows in the forecast dashboard to align with the current operational pipeline: remove deprecated `sapphire-rerun`, fix the hardcoded env file path, and add persistent container logging. Existing environment variables (`IN_DOCKER_CONTAINER`, `SAPPHIRE_OPDEV_ENV`) are preserved — they are load-bearing for `.env` file selection and host routing inside spawned containers.

## Context

The forecast dashboard (`apps/forecast_dashboard/src/vizualization.py`) has two interactive dataflows that run Docker containers directly (not via Luigi):

1. **Save Changes** (`save_to_database`, line 3711) — after the user edits LR visibility checkboxes and saves, re-runs linreg + postprocessing to recompute forecasts with the updated point set.
2. **Trigger Forecasts** (`create_reload_button` → `run_pipeline`, line 3963) — manually re-runs the full operational pipeline (preprunoff → linreg → ML models → postprocessing).

Both dataflows were written before the operational/maintenance separation and have accumulated drift from the Luigi pipeline.

## Problem

1. **Deprecated container**: Both flows call `mabesa/sapphire-rerun:latest` (reset_rundate) which has been deprecated.
2. **Hardcoded env file path**: Lines 3800 and 4002 hardcode `.env_develop_kghm` instead of using the module-level `env_file_path` variable (which correctly reads from `os.getenv('ieasyhydroforecast_env_file_path')`).
3. **No persistent logging**: Container output goes to stdout only. Luigi tasks write to timestamped files in `docker_logs/`. Dashboard container runs leave no audit trail.

## Desired Outcome

- `sapphire-rerun` calls removed from both dataflows
- Env file path read from the existing `env_file_path` module variable
- Container logs written to `{intermediate_data_path}/docker_logs/log_dashboard_{name}_{timestamp}.txt`
- No changes to SSH tunnel handling, Docker networking, or volume setup
- No changes to the UI behavior (buttons, progress bars, popups)

---

## Technical Analysis

### Current Implementation

**Two `run_docker_container` functions exist:**

1. **Inner function** (line 3597-3705) — defined inside `select_and_plot_data`, used by "Save Changes". Uses `establish_ssh_tunnel` context manager, SSH tunnel network, `network_mode='host'`, and accepts a `progress_bar` parameter.

2. **Module-level function** (line 4137-4220) — used by "Trigger Forecasts" (`create_reload_button`). Uses `subprocess.run` for SSH tunnel, `network='host'`, no progress bar, removes container after completion.

Both are kept as-is (SSH tunnel handling is out of scope). Changes are limited to their **callers**.

**Save Changes caller** (`save_to_database`, line 3711-3857):
- Line 3797-3801: builds `environment` with 3 vars including hardcoded env path
- Line 3815: calls `sapphire-rerun` → **remove**
- Line 3822: calls `sapphire-linreg` → **keep, fix env**
- Line 3828: calls `sapphire-postprocessing` → **keep, fix env**

**Trigger Forecasts caller** (`run_docker_pipeline`, line 3981-4114):
- Line 3997-4003: builds `environment` with 4 vars including hardcoded env path
- Line 4036: calls `sapphire-preprunoff` → **keep, fix env**
- Line 4042: calls `sapphire-rerun` → **remove**
- Line 4048: calls `sapphire-linreg` → **keep, fix env**
- Line 4067: calls ML models in loop → **keep, fix env**
- Line 4080: calls `sapphire-postprocessing` → **keep, fix env**

**Key files:**
- `apps/forecast_dashboard/src/vizualization.py:3597-3705` — inner `run_docker_container` (Save Changes)
- `apps/forecast_dashboard/src/vizualization.py:3711-3857` — `save_to_database`
- `apps/forecast_dashboard/src/vizualization.py:3893-4134` — `create_reload_button` + `run_pipeline` + `run_docker_pipeline`
- `apps/forecast_dashboard/src/vizualization.py:4137-4220` — module-level `run_docker_container` (Trigger Forecasts)
- `apps/forecast_dashboard/src/vizualization.py:3076` — `env_file_path` module variable (already correct)

### Reference: Luigi Operational Tasks

`LinearRegression` (pipeline_docker.py:646-648):
```python
environment = [
    f"ieasyhydroforecast_env_file_path={env_file_path}",
    f"SAPPHIRE_PREDICTION_MODE={self.prediction_mode}",
]
```

`PostProcessingForecasts` (pipeline_docker.py:835-838):
```python
environment = [
    f"ieasyhydroforecast_env_file_path={env_file_path}",
    f"SAPPHIRE_PREDICTION_MODE={self.prediction_mode}",
]
```

`PreprocessingRunoff` (pipeline_docker.py:483-517): same 2-var pattern.

`RunMLModel` (pipeline_docker.py:714-757): adds `SAPPHIRE_MODEL_TO_USE` and `RUN_MODE`.

---

## Implementation Plan

### Approach

Minimal, targeted edits to the two caller functions and small additions to both `run_docker_container` functions (logging + container cleanup). No refactoring of SSH tunnel handling or UI components. Add a small logging helper near the module-level constants.

### Files to Modify

| File | Changes |
|------|---------|
| `apps/forecast_dashboard/src/vizualization.py:3131+` | Add `DOCKER_LOG_DIR` constant and `_write_container_log` helper (after `get_bind_path`) |
| `apps/forecast_dashboard/src/vizualization.py:3597-3705` | Inner `run_docker_container`: add logging + container removal |
| `apps/forecast_dashboard/src/vizualization.py:3711-3857` | Fix `save_to_database`: remove rerun, fix env |
| `apps/forecast_dashboard/src/vizualization.py:3963-4114` | Fix `run_docker_pipeline`: remove rerun, fix env |
| `apps/forecast_dashboard/src/vizualization.py:4137-4220` | Module-level `run_docker_container`: add logging before removal |

### Implementation Steps

#### Phase 1: Add logging helper (no behavioral change)

- [x] **Step 1.1**: Add a `_write_container_log` helper function **after** `get_bind_path` (after line 3131). The log directory path is computed lazily inside the helper (not as a module-level constant) to avoid a `TypeError` crash at import time if `ieasyforecast_intermediate_data_path` is not set:
  ```python
  def _write_container_log(container_name: str, container) -> None:
      """Write container logs to a timestamped file in docker_logs/."""
      try:
          log_dir = os.path.join(
              get_absolute_path(env.get('ieasyforecast_intermediate_data_path')),
              'docker_logs',
          )
          timestamp = dt.datetime.now().strftime('%Y%m%d_%H%M%S')
          log_path = os.path.join(
              log_dir,
              f"log_dashboard_{container_name}_{timestamp}.txt",
          )
          os.makedirs(log_dir, exist_ok=True)
          logs = container.logs().decode('utf-8', errors='replace')
          with open(log_path, 'w') as f:
              f.write(logs)
          logger.info("Container logs written to %s", log_path)
      except Exception as e:
          logger.warning("Failed to write container log: %s", e)
  ```
  Note: `datetime` is imported as `dt` (line 9: `import datetime as dt`), so use `dt.datetime.now()`.

#### Phase 2: Update Save Changes dataflow

- [x] **Step 2.1**: In `save_to_database` (line 3797-3801), fix the hardcoded env file path. Spawned containers always run in Docker, so the env file path must be a bind-mount-internal path. Extract the filename from the host-side `env_file_path` and combine with `bind_volume_path_config` (already computed at line 3777):
  ```python
  # Before:
  environment = [
      'IN_DOCKER_CONTAINER=True',
      'SAPPHIRE_PREDICTION_MODE=' + horizon.upper(),
      f'ieasyhydroforecast_env_file_path={bind_volume_path_config}/.env_develop_kghm'
  ]

  # After:
  env_file_name = os.path.basename(env_file_path)
  environment = [
      'IN_DOCKER_CONTAINER=True',
      f'SAPPHIRE_PREDICTION_MODE={horizon.upper()}',
      f'ieasyhydroforecast_env_file_path={bind_volume_path_config}/{env_file_name}',
  ]
  ```
  This works because the env file always lives in the configuration directory, which is bind-mounted into spawned containers at `bind_volume_path_config`. The filename varies by organization (e.g., `.env_develop_kghm`, `.env_develop_uzhm`).

- [x] **Step 2.2**: Remove the `sapphire-rerun` call and its timing code (lines 3815-3819), and add `temp_docker_start` initialization before the linreg call to avoid `NameError` (the removed block contained the first assignment of `temp_docker_start`):
  ```python
  # Remove these lines (3814-3819):
  # Run the reset rundate module to update the rundate for the linear regression module
  run_docker_container(client, "mabesa/sapphire-rerun:latest", volumes, environment, "reset_rundate",
                       progress_bar)
  temp_docker_end = time.time()
  print(f"Time taken to run reset_rundate: {temp_docker_end - start_docker_runs:.2f} seconds")
  temp_docker_start = time.time()

  # Replace with:
  temp_docker_start = time.time()
  ```

- [x] **Step 2.3**: Add logging and container cleanup to the inner `run_docker_container` (line 3597). Currently this function does NOT remove the container after `container.wait()`, unlike the module-level version. Add `_write_container_log` and `container.remove(force=True)` after the container finishes (after the progress bar reaches 100%, around line 3695):
  ```python
  # After the existing success print (line 3695):
  #     print(f"Container '{container_name}' has stopped successfully.")
  # Add:
  _write_container_log(container_name, container)
  try:
      container.remove(force=True)
  except docker.errors.APIError as e:
      print(f"Warning: Failed to remove container '{container_name}': {e}")
  ```
  Also add logging + removal in the error path (before the raise, around line 3688):
  ```python
  # Before the existing raise docker.errors.ContainerError(...):
  _write_container_log(container_name, container)
  try:
      container.remove(force=True)
  except docker.errors.APIError as e:
      print(f"Warning: Failed to remove container '{container_name}': {e}")
  ```
  The try/except around `container.remove()` matches the pattern used in the module-level `run_docker_container` (line 4212–4216).

#### Phase 3: Update Trigger Forecasts dataflow

- [x] **Step 3.1**: In `run_docker_pipeline` (line 3997-4003), fix the hardcoded env file path. Same pattern as Step 2.1 — use bind path + extracted filename:
  ```python
  # Before:
  environment = [
      'SAPPHIRE_OPDEV_ENV=True',
      'IN_DOCKER_CONTAINER=True',
      f'SAPPHIRE_PREDICTION_MODE={horizon.upper()}',
      f'ieasyhydroforecast_env_file_path={get_bind_path(env.get("ieasyforecast_configuration_path"))}/.env_develop_kghm'
  ]

  # After:
  env_file_name = os.path.basename(env_file_path)
  environment = [
      'SAPPHIRE_OPDEV_ENV=True',
      'IN_DOCKER_CONTAINER=True',
      f'SAPPHIRE_PREDICTION_MODE={horizon.upper()}',
      f'ieasyhydroforecast_env_file_path={get_bind_path(env.get("ieasyforecast_configuration_path"))}/{env_file_name}',
  ]
  ```

- [x] **Step 3.2**: Remove the `sapphire-rerun` call and timing code (lines 4042-4045). No `temp_docker_start` fix needed here — it is already initialized at line 4039 by the preprunoff block. After removal, the linreg timing (line 4050) will measure from end-of-preprunoff, which is acceptable:
  ```python
  # Remove these lines (4041-4045):
  # Run the reset_rundate container
  run_docker_container(client, "mabesa/sapphire-rerun:latest", volumes, environment, "reset_rundate")
  temp_docker_end = time.time()
  print(f"Time taken to run reset_rundate: {temp_docker_end - temp_docker_start:.2f} seconds")
  temp_docker_start = time.time()
  ```

- [x] **Step 3.3**: The module-level `run_docker_container` (line 4137) already removes the container after completion (`container.remove(force=True)` at line 4213). Add `_write_container_log(container_name, container)` at line 4210 — after the success/failure print, before `container.remove()`. This is a 1-line addition.

- [x] **Step 3.4**: For the ML model loop (line 4065-4070), the environment already appends `SAPPHIRE_MODEL_TO_USE` and `RUN_MODE=forecast` — this is correct and matches Luigi's `RunMLModel`. No change needed to the loop, only the base `environment` list (done in Step 3.1).

#### Phase 4: Verify

- [x] **Step 4.1**: Grep the file for any remaining references to `.env_develop_kghm` — there should be none in the two dataflows.
- [x] **Step 4.2**: Grep for `sapphire-rerun` — should be zero matches in the file.
- [x] **Step 4.3**: Run `ruff check apps/forecast_dashboard/src/vizualization.py` and `ruff format` to ensure no lint issues.

---

## Testing

### Test Cases

There are no automated tests for the Docker dataflows (they require a running Docker daemon and SSH tunnel). Testing is manual.

### Manual Verification

1. Start the dashboard locally with a valid `ieasyhydroforecast_env_file_path` set
2. Navigate to the regression tab, select a station/pentad
3. Toggle a visibility checkbox, click "Save Changes"
4. Verify: no `reset_rundate` container appears in `docker ps`
5. Verify: `linreg` and `postprocessing` containers run with correct env vars (check `docker inspect <container>` for `Env`)
6. Verify: log files appear in `{intermediate_data_path}/docker_logs/log_dashboard_linreg_*.txt`
7. Test the "Trigger Forecasts" button — same checks for all containers
8. Verify no `.env_develop_kghm` appears in container env vars

---

## Documentation Impact

- [ ] No documentation impact — changes are internal to the dashboard module, no new env vars, no new user-facing behavior. The logging addition follows the existing `docker_logs/` convention already documented in the pipeline.

---

## Out of Scope

- SSH tunnel handling (both inner and module-level `run_docker_container`)
- Docker networking configuration
- UI changes (buttons, progress bars, popups, spinner)
- Refactoring the two `run_docker_container` functions into one
- Adding retry logic or notifications (Luigi territory)
- Volume configuration changes
- The `plot_manager.py` reference to `select_and_plot_data`
- Error handling bugs in inner `run_docker_container` (tracked separately in FD-008)

## Dependencies

None.

## Pre-existing Issues Discovered During Review

The following bugs were found in the code during FD-007 review. They are **not introduced by FD-007** and are tracked separately:

1. **Inner `run_docker_container`: `raise ContainerError` silently caught** — The `raise` at line ~3714 is inside a `try` whose `except Exception` at line ~3728 catches it. Container failures are swallowed and the pipeline continues (e.g., postprocessing runs after a failed linreg). → **FD-008**
2. **Inner `run_docker_container`: `ContainerError` missing `stderr` argument** — The constructor call omits the required `stderr` positional argument, causing a `TypeError` instead of `ContainerError`, which is also caught by the same `except`. → **FD-008**
3. **`reset_rundate` in container monitoring list** — The `container_names` list in `check_containers_running` (line ~3944) included `"reset_rundate"`. Cleaned up as part of FD-007 since it was directly related to the rerun removal.

## Additional Cleanup (beyond original plan)

- Removed `"reset_rundate"` from the `container_names` monitoring list in `check_containers_running` (~line 3944). This list is used to disable the reload button when pipeline containers are running. Since `reset_rundate` is no longer spawned, including it was dead code.

## Acceptance Criteria

- [x] `sapphire-rerun` removed from both Save Changes and Trigger Forecasts
- [x] Env file path uses `env_file_path` module variable in both dataflows
- [x] Existing environment variables (`IN_DOCKER_CONTAINER`, `SAPPHIRE_OPDEV_ENV`) preserved
- [x] Containers removed after completion in both dataflows (inner `run_docker_container` aligned with module-level one)
- [x] Container logs written to `docker_logs/log_dashboard_{name}_{timestamp}.txt`
- [x] No references to `.env_develop_kghm` in either dataflow
- [x] No references to `sapphire-rerun` or `reset_rundate` in the file
- [x] `ruff check` shows no new issues (pre-existing lint issues remain)
- [x] File parses without syntax errors (`ast.parse`)
- [x] Existing UI behavior unchanged (buttons, progress, popups)

---

## References

- Luigi operational tasks: `apps/pipeline/pipeline_docker.py:619-660` (LinearRegression), `786-851` (PostProcessingForecasts)
- Dashboard dataflows: `apps/forecast_dashboard/src/vizualization.py:3711-3857` (Save), `3893-4134` (Trigger)
- Env resolution: `apps/forecast_dashboard/src/vizualization.py:3076`
