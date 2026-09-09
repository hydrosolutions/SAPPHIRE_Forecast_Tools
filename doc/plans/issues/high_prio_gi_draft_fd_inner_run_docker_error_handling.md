# FD-008: forecast dashboard swallows container failures instead of surfacing them (both "Save Changes" and "Trigger forecasts")

**Status**: Draft
**Module**: forecast_dashboard
**Priority**: High (raised from Low 2026-09-09 — see "Priority reconsidered" below; owner
confirmed High). Filename renamed 2026-09-09 from `low_prio_gi_draft_fd_inner_run_docker_error_handling.md`
to `high_prio_gi_draft_fd_inner_run_docker_error_handling.md` to match this field, per
CLAUDE.md's `<priority>_` filename convention; all repo cross-references updated in the same
change.
**Labels**: `forecast_dashboard`, `docker`, `error-handling`, `silent-noop`
**Found**: originally during review of FD-007 (pre-existing then, not introduced by it). Reprised
and corrected 2026-09-09 during an owner-directed investigation into why the forecast
dashboard's decade-horizon actions have not been visibly failing despite at least three of the
four containers in that chain rejecting the value they were given (see LR-012 and FD-026).
**Related**: **LR-012** and **FD-026** — this defect is *why* those two issues' documented
failures (`postprocessing_operational.py` and `make_forecast.py` both `sys.exit`/`raise` on the
dashboard's `DECADE` value) have not been visible to an operator; it is not a dependency of
either — normalizing every module's domain (LR-012 + FD-026) means none of them fail on
`DECADE` any more, independent of whether this issue ships. **P-007**
(`archive/high_prio_gi_draft_pipeline_container_exit_status_discarded.md`, Complete) is the same
defect shape — a container's exit status discarded so failure reads as success — already fixed
once in `pipeline_docker.py`'s own `run_docker_container`; this is that class recurring in the
dashboard's two **separate, independent** implementations of the same idea.

---

## Summary

The forecast dashboard has **two** independent `run_docker_container` implementations, used by
its two manual pipeline-trigger buttons. Both swallow a failed container's exit status instead of
propagating it, so a container that crashes reads, to the calling function and to the operator,
exactly like one that succeeded — the progress bar completes, the button re-enables, and no error
is shown.

## Context

Originally filed covering only the "Save Changes" flow's `run_docker_container`. Re-verified and
**corrected** 2026-09-09: its own "Out of Scope" section previously claimed the "Trigger
forecasts" flow's `run_docker_container` "does NOT have this bug" — that claim is wrong. There
are genuinely two separate function definitions in `apps/forecast_dashboard/src/vizualization.py`
(confirmed via `grep -n "^    def run_docker_container\|^def run_docker_container"` — exactly two
matches), and both have the same defect shape, in slightly different forms.

## Problem

### Bug 1 ("Save Changes"): `ContainerError` is raised, then immediately caught by its own enclosing `except`

The nested `run_docker_container`, defined inside `select_and_plot_data` at
`apps/forecast_dashboard/src/vizualization.py:3858` and used by `save_to_database` ("Save
Changes") to run the `linreg`, `postprocessing`, and `skill_recalc` containers in sequence:

```python
try:
    with establish_ssh_tunnel(...):
        ...
        container = client.containers.run(...)
        ...
        result = container.wait()
        if result['StatusCode'] != 0:                    # :3949
            ...
            raise docker.errors.ContainerError(...)        # :3956-3961, missing `stderr` — Bug 2
        else:
            ...
except Exception as e:                                     # :3970
    print(f"Error running container '{container_name}': {e}")   # :3971 — swallowed
finally:
    ...
```

**Impact**: when a container fails (e.g. `linreg` exits non-zero, or, per LR-012/FD-026,
`postprocessing` or `skill_recalc` reject an unrecognised `SAPPHIRE_PREDICTION_MODE`), the
exception is caught and printed to the server console only. `save_to_database` never sees it and
proceeds to the next container in the sequence regardless, then unconditionally sets the progress
bar to 100% in its own `finally` block.

### Bug 2 ("Save Changes"): `ContainerError` is constructed without the required `stderr` argument

The `docker.errors.ContainerError.__init__` signature requires a `stderr` positional argument.
The call at `:3956-3961` omits it:

```python
raise docker.errors.ContainerError(
    container=container,
    exit_status=result['StatusCode'],
    command=None,
    image=full_image_name
    # stderr= is missing
)
```

This raises a `TypeError` instead of `ContainerError` — still caught by the same `except
Exception` at `:3970`, making Bug 1 doubly silent (the *wrong* exception type is what actually
gets swallowed).

### Bug 3 ("Trigger forecasts"): the module-level `run_docker_container` never raises anything at all — **corrected scope, previously claimed not to have this bug**

`run_pipeline` ("Trigger forecasts") calls a **separate**, module-level `run_docker_container`
defined later in the same file at `:4491` (confirmed by checking which containers `run_pipeline`
launches and which function definition is in scope for it — `run_pipeline` is nested inside
`create_reload_button`, a top-level function with no local `run_docker_container` of its own, so
the reference resolves to the module-level one). On a non-zero exit code, this version does not
even attempt to raise:

```python
result = container.wait()
if result['StatusCode'] != 0:
    print(f"Container '{container_name}' exited with status code {result['StatusCode']}.")  # :4560
    # Optionally log the error or add to a list of failed containers
else:
    print(f"Container '{container_name}' has stopped successfully.")                         # :4563
_write_container_log(container_name, container)                                               # :4566
... container.remove(force=True) ...
```

There is no `raise` anywhere in this branch. The function returns normally either way, so
`run_pipeline` — which runs `linreg`, then conditionally a loop of ML containers (one per
configured model), then `postprocessing`, all via this same function — has **no way** to learn
that any of them failed, and continues to the next one unconditionally every time.

**The function's own docstring is wrong, not just its behaviour.** Its `Raises` section
(`:4491-4503`) reads:

```python
def run_docker_container(client, full_image_name, volumes, environment, container_name):
    """
    Runs a Docker container and blocks until it completes.
    ...
    Raises:
        docker.errors.ContainerError: If the container exits with a non-zero status.
    """
```

That is a documented contract the body does not honour — verified above, the non-zero-exit
branch only prints, with the leftover comment `# Optionally log the error or add to a list of
failed containers` (`:4560-4562`) marking where the intended handling was apparently never
finished. A reader auditing this module by its call sites and docstrings alone — rather than
reading the full body — would reasonably conclude that `run_pipeline` already stops on a failed
container (since the function it calls documents that it raises), and would not think to look
here for why decade failures (LR-012/FD-026) have gone unnoticed. The fix must correct the
docstring together with the behaviour, not leave a `Raises` section that is still untrue after
the fix changes *how* it raises.

**This directly contradicts this issue's own original "Out of Scope" note**, which read: "Module-
level `run_docker_container` (Trigger Forecasts) — does NOT have this bug; it does not re-raise
on failure but also does not silently continue a pipeline." Verified directly against trunk: it
does silently continue — there is nothing else in `run_pipeline` that inspects the outcome of any
`run_docker_container` call, so a failed `linreg` container does not stop the ML or
`postprocessing` containers from running afterward, exactly the failure class Bug 1 describes for
"Save Changes." The original claim was wrong; this revision corrects it rather than repeating it.

## Why this matters now (2026-09-09)

This defect is the reason **LR-012** and **FD-026**'s documented failures have gone unnoticed:
per those two issues, when an operator uses the decade horizon in either dashboard button,
`postprocessing_operational.py` and (in the "Trigger forecasts" flow) `make_forecast.py` both
already reject the dashboard's `SAPPHIRE_PREDICTION_MODE=DECADE` value today, loudly
(`sys.exit(1)` / `raise ValueError`) — but neither failure has ever reached the dashboard's UI,
because of Bug 1/Bug 3 above. This is the same defect shape as **P-007**
(`pipeline_docker.py`'s `run_docker_container` discarding `container.wait()`'s exit code across
20 Luigi call sites, fixed in PR #478): a container that fails is indistinguishable, at the
calling layer, from one that succeeded.

## Priority reconsidered: Low → High (2026-09-09)

Originally filed Low, reasoned narrowly from "Save Changes" alone and treated as a
code-cleanliness issue (propagate the exception properly). That does not hold once its actual
blast radius is understood:

- It affects **both** manual dashboard flows, not one (the original's own "Out of Scope" claim to
  the contrary was wrong — see Bug 3).
- It is the reason a *known-live, already-loudly-failing* defect (LR-012/FD-026's decade
  mismatch, and any other unrelated container crash — OOM, network, code bug — in either flow)
  produces no operator-visible signal at all. This is the same "wrong data reads as success, not
  as a failure" hazard CLAUDE.md's Data I/O Transition section calls out, and the same shape that
  made **P-007** worth fixing project-wide in the Luigi pipeline.
- Not priced at P-007's own historical tier exactly, because P-007's blast radius was the fully
  automated Luigi cron path (20 call sites, 8 images); this is confined to two manual,
  operator-initiated dashboard buttons. That confinement is why this is **High**, not higher —
  but it is no longer a Low-priority cleanup, since it is the concrete reason two other Draft
  issues' failures have been invisible.

## Desired Outcome

- Container failures propagate to the calling function (`save_to_database` / `run_pipeline`) in
  both flows, so a failed step stops the remaining steps in that run rather than continuing
  unconditionally.
- `ContainerError` (or an equivalent signal) is constructed/raised correctly, with all required
  arguments, in both implementations.
- The operator sees a meaningful error message in the dashboard UI when a container fails, not
  only a server-console `print`.

---

## Technical Analysis

### Option A: Re-raise from the `except` block (Bug 1/2, "Save Changes")

Minimal change — let `ContainerError` propagate and fix the missing `stderr`:

```python
except docker.errors.ContainerError:
    raise  # let container failures propagate
except Exception as e:
    print(f"Error running container '{container_name}': {e}")
```

```python
raise docker.errors.ContainerError(
    container=container,
    exit_status=result['StatusCode'],
    command=None,
    image=full_image_name,
    stderr=container.logs(tail=50).decode('utf-8', errors='replace'),
)
```

### Option B: Restructure try/except (Bug 1, "Save Changes")

Move the container-run logic out of the SSH-tunnel `try`/`except`, so only SSH tunnel errors are
caught there. More invasive but cleaner.

### For Bug 3 ("Trigger forecasts"): make the module-level function raise, and make `run_pipeline` check it

The module-level `run_docker_container` (`:4491`) needs the same `raise
docker.errors.ContainerError(..., stderr=...)` (with `stderr`, unlike Bug 2's version) added at
its own `if result['StatusCode'] != 0:` branch (`:4559`), and `run_pipeline`'s three call sites
need to stop assuming success — at minimum, stop launching the next container in the sequence
after a failure, mirroring what Option A achieves for `save_to_database`.

### Recommendation

Option A for Bug 1/2, plus the equivalent raise-and-check fix for Bug 3 — both minimal and
targeted; do them together since they are the same defect shape in two call sites, not two
unrelated changes.

---

## Out of Scope

- LR-012's and FD-026's own fixes (normalizing `SAPPHIRE_PREDICTION_MODE` handling so the
  containers this issue is about stop failing on `DECADE` in the first place) — this issue is
  about **surfacing** a failure when one occurs, not about which values should or should not be
  failures.
- SSH tunnel handling.
- UI changes beyond "show the operator that a container failed" (specific UI/UX design not
  specified here).
- `skill_recalc`'s own `try`/`except` in `save_to_database` (`:4145-4160`) — that one is
  **deliberately** non-fatal by its own docstring comment ("Non-fatal: if this fails, the data
  reload still proceeds") and is a different, intentional design choice, not this bug.

## Dependencies

None. Independent of LR-012 and FD-026 (see "Related" above).

## Acceptance Criteria

- [ ] `ContainerError` constructed with all required arguments including `stderr`, in **both**
  `run_docker_container` implementations.
- [ ] A container failure in `linreg` prevents `postprocessing`/`skill_recalc` from running in
  the "Save Changes" flow.
- [ ] A container failure in `linreg` prevents the ML loop and `postprocessing` from running in
  the "Trigger forecasts" flow.
- [ ] The operator sees an error message in the dashboard UI when a container fails, in both
  flows.
- [ ] Existing success path unchanged in both flows.
- [ ] The module-level `run_docker_container`'s docstring (`:4491-4503`) is corrected to match
  its actual (fixed) behaviour — its `Raises` section must not describe a contract the body does
  not honour, before or after this fix.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` — zero failures,
  zero new skips.

## References

- `apps/forecast_dashboard/src/vizualization.py:3858` (nested `run_docker_container` def, "Save
  Changes"), `:3949` (exit-status check), `:3956-3961` (raise, missing `stderr` — Bug 2),
  `:3970-3971` (the swallow — Bug 1)
- `apps/forecast_dashboard/src/vizualization.py:4491` (module-level `run_docker_container` def,
  "Trigger forecasts"), `:4559-4560` (exit-status check and print, no raise — Bug 3), `:4573`
  (its own `except Exception`, catches only genuinely unexpected errors, not the StatusCode
  branch, since that branch never raises)
- `apps/forecast_dashboard/src/vizualization.py:3984` (`save_to_database`), `:4317`
  (`run_pipeline`), `:4248` (`create_reload_button`, `run_pipeline`'s enclosing scope, confirming
  it has no local `run_docker_container` of its own and therefore resolves to the module-level
  one)
- `doc/plans/issues/mid_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md` (LR-012),
  `doc/plans/issues/high_prio_gi_draft_fd_decade_horizon_prediction_mode_spelling.md` (FD-026) —
  the concrete, already-live failures this defect has been hiding
- `doc/plans/issues/archive/high_prio_gi_draft_pipeline_container_exit_status_discarded.md`
  (P-007 — same defect shape, fixed once already in the Luigi pipeline)
