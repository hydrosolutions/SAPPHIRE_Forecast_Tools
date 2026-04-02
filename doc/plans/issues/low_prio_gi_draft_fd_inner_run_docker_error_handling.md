# FD-008: Fix error handling in inner `run_docker_container` (Save Changes)

**Status**: Draft
**Module**: forecast_dashboard
**Priority**: Low
**Labels**: `forecast_dashboard`, `docker`, `error-handling`

---

## Summary

The inner `run_docker_container` function (used by the "Save Changes" dataflow) has two pre-existing error handling bugs that silently swallow container failures and allow the pipeline to continue after a failed step.

## Context

These bugs were discovered during review of FD-007. They are **pre-existing** — present before FD-007 and not introduced by it. They are documented here for separate tracking.

The inner `run_docker_container` is defined inside `select_and_plot_data` at `apps/forecast_dashboard/src/vizualization.py:3618`. It is used by `save_to_database` to run `linreg` and `postprocessing` containers sequentially.

## Problem

### Bug 1: `ContainerError` raise is silently caught

The `raise docker.errors.ContainerError(...)` at line ~3714 is inside a `try` block whose `except Exception as e` at line ~3728 catches it:

```python
try:
    with establish_ssh_tunnel(...):
        ...
        container = client.containers.run(...)
        ...
        if result['StatusCode'] != 0:
            ...
            raise docker.errors.ContainerError(...)  # caught below!
        ...
except Exception as e:
    print(f"Error running container '{container_name}': {e}")  # swallowed
finally:
    ...
```

**Impact**: When a container fails (e.g., linreg exits non-zero), the exception is caught and printed, but `save_to_database` never sees it. Postprocessing runs unconditionally after a failed linreg, producing stale or incorrect results without any user-visible error.

### Bug 2: `ContainerError` missing required `stderr` argument

The `docker.errors.ContainerError.__init__` signature requires a `stderr` positional argument. The call omits it:

```python
raise docker.errors.ContainerError(
    container=container,
    exit_status=result['StatusCode'],
    command=None,
    image=full_image_name
    # stderr= is missing
)
```

This raises a `TypeError` instead of `ContainerError`, which is then caught by the same `except Exception` — making Bug 1 doubly silent.

## Desired Outcome

- Container failures propagate to `save_to_database` so the pipeline stops on first failure
- `ContainerError` is constructed with all required arguments
- User sees a meaningful error message when a container fails

---

## Technical Analysis

### Option A: Re-raise from except block

Minimal change — add `raise` to the except block so the exception propagates:

```python
except docker.errors.ContainerError:
    raise  # let container failures propagate
except Exception as e:
    print(f"Error running container '{container_name}': {e}")
```

And fix the missing `stderr`:

```python
raise docker.errors.ContainerError(
    container=container,
    exit_status=result['StatusCode'],
    command=None,
    image=full_image_name,
    stderr=container.logs(tail=50).decode('utf-8', errors='replace'),
)
```

### Option B: Restructure try/except

Move the container run logic out of the SSH tunnel try/except, so only SSH tunnel errors are caught there. This is more invasive but cleaner.

### Recommendation

Option A — minimal and targeted.

---

## Out of Scope

- Module-level `run_docker_container` (Trigger Forecasts) — does NOT have this bug; it does not re-raise on failure but also does not silently continue a pipeline
- SSH tunnel handling
- UI changes

## Dependencies

None.

## Acceptance Criteria

- [ ] `ContainerError` constructed with all required arguments including `stderr`
- [ ] Container failure in linreg prevents postprocessing from running in the Save Changes flow
- [ ] User sees an error message in the dashboard when a container fails
- [ ] Existing success path unchanged
