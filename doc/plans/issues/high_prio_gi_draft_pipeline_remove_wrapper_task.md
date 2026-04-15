# Remove RunAllMLMaintenance WrapperTask to fix unfulfilled dependencies

**Priority**: High
**Module**: pipeline
**Status**: Draft

## Problem

`RunAllMLMaintenance` is a `luigi.WrapperTask` that aggregates 6
`MLMaintenance` tasks. All 6 tasks complete successfully and write their
marker files. But when Luigi's worker picks up the WrapperTask, it re-checks
all dependency outputs from a different subprocess (`worker.py:195`). In the
Docker multi-worker environment (`workers=6`), the marker files written by
other worker subprocesses are not visible to this check, causing:

```
RuntimeError: Unfulfilled dependencies at run time:
  MLMaintenance_None_TFT_PENTAD_... (/kyg_data_forecast_tools/.../maintenance_ml_TFT_PENTAD_2026-04-15.marker),
  MLMaintenance_None_TFT_DECAD_... (/kyg_data_forecast_tools/.../maintenance_ml_TFT_DECAD_2026-04-15.marker),
  ... (all 6 markers)
```

This is systematic — all 6 markers are reported missing, not just 1-2. The
issue reproduces on every maintenance run.

**Impact**: `PostProcessingMaintenance` and `RunDailyMaintenanceWorkflow`
never run because they depend on `RunAllMLMaintenance`.

## Root Cause

Luigi's `WrapperTask` is a no-op task whose only purpose is dependency
grouping. But the Luigi worker runs a safety check (`TaskProcess.run()` in
`worker.py:195`) that verifies all dependency outputs exist before calling
`run()`. This check runs in a NEW subprocess (forked via `multiprocessing`),
and in the Docker volume environment, marker files written by other worker
subprocesses are not visible to `os.path.exists()` at check time.

## Fix

Remove the `WrapperTask` indirection. Inline the ML dependency logic directly
into `PostProcessingMaintenance.requires()`. This eliminates the intermediate
subprocess dependency check entirely.

**Before** (current DAG):
```
PostProcessingMaintenance
  ├── LinRegMaintenance(PENTAD)
  ├── LinRegMaintenance(DECAD)
  └── RunAllMLMaintenance  <-- WrapperTask, no-op run(), dependency check fails
        ├── MLMaintenance(TFT, PENTAD)
        ├── MLMaintenance(TFT, DECAD)
        ├── MLMaintenance(TIDE, PENTAD)
        ├── MLMaintenance(TIDE, DECAD)
        ├── MLMaintenance(TSMIXER, PENTAD)
        └── MLMaintenance(TSMIXER, DECAD)
```

**After** (fixed DAG):
```
PostProcessingMaintenance
  ├── LinRegMaintenance(PENTAD)
  ├── LinRegMaintenance(DECAD)
  ├── MLMaintenance(TFT, PENTAD)      <-- direct dependency
  ├── MLMaintenance(TFT, DECAD)
  ├── MLMaintenance(TIDE, PENTAD)
  ├── MLMaintenance(TIDE, DECAD)
  ├── MLMaintenance(TSMIXER, PENTAD)
  └── MLMaintenance(TSMIXER, DECAD)
```

Same DAG edges, same execution order. Only the intermediate empty task is removed.

## Implementation

### `apps/pipeline/pipeline_docker.py`

1. **Delete** `RunAllMLMaintenance` class (lines 1831-1839)

2. **Modify** `PostProcessingMaintenance.requires()` — replace
   `deps.append(RunAllMLMaintenance())` with direct ML task construction:

   ```python
   def requires(self):
       deps = [
           LinRegMaintenance(prediction_mode="PENTAD"),
           LinRegMaintenance(prediction_mode="DECAD"),
       ]
       if RUN_ML_MODELS == "True":
           models = env.get("ieasyhydroforecast_available_ML_models").split(",")
           for model in models:
               for mode in ["PENTAD", "DECAD"]:
                   deps.append(MLMaintenance(model_type=model, prediction_mode=mode))
       return deps
   ```

### `apps/pipeline/tests/test_maintenance_tasks.py`

1. **Update** module docstring (line 5) — remove `RunAllMLMaintenance` mention

2. **Delete** `TestRunAllMLMaintenance` class (lines 161-181)

3. **Update** `test_includes_ml_when_enabled` (line 202) — check for
   `MLMaintenance` instances directly instead of `RunAllMLMaintenance`.
   Expect 4 ML tasks (mock env has `TFT,TIDE` x `PENTAD,DECAD`).

4. **Update** `test_excludes_ml_when_disabled` (line 213) — check that no
   `MLMaintenance` instances are in the dependency list (not
   `RunAllMLMaintenance`)

### `apps/pipeline/README` (line 81)

Replace `RunAllMLMaintenance` wrapper with direct `MLMaintenance` tasks in
the DAG diagram.

## Verification

1. `SAPPHIRE_TEST_ENV=True bash apps/run_tests.sh pipeline` — all tests pass
2. `grep -r "RunAllMLMaintenance" apps/pipeline/` — zero matches
3. Deploy to server and run daily maintenance — `PostProcessingMaintenance`
   should complete without unfulfilled dependencies error

## Risk Assessment

**Safe**: The WrapperTask's `run()` was a no-op. Inlining the same
`MLMaintenance` tasks directly into `PostProcessingMaintenance.requires()`
produces the exact same DAG edges. Luigi still serializes ML tasks via
`resources = {"ml_memory": 1}` and waits for all of them before running
postprocessing.

**No behavioral change**: Same tasks, same order, same concurrency. Only the
empty intermediate task is removed.

## Note: Operational pipeline `RunAllMLModels`

`RunAllMLModels` (line 761) is also a `luigi.WrapperTask` with the same
pattern. It is defined but NOT used as a dependency anywhere in the current
pipeline code (only referenced in a message string and tests). If it were
used as a dependency, it could have the same issue. Out of scope for this
fix — the operational pipeline uses direct task yields, not the wrapper.
