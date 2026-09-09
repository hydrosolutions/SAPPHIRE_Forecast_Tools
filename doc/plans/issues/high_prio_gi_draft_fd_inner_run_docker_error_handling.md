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
four containers in that chain rejecting the value they were given (see LR-013 and FD-027).
**Related**: **LR-013** and **FD-027** — this defect is *why* those two issues' documented
failures (`postprocessing_operational.py` and `make_forecast.py` both `sys.exit`/`raise` on the
dashboard's `DECADE` value) have not been visible to an operator; it is not a dependency of
either — normalizing every module's domain (LR-013 + FD-027) means none of them fail on
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

**Impact**: when a container fails (e.g. `linreg` exits non-zero, or, per LR-013/FD-027,
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
if result['StatusCode'] != 0:                                                                 # :4559
    print(f"Container '{container_name}' exited with status code {result['StatusCode']}.")   # :4560
    # Optionally log the error or add to a list of failed containers                          # :4561
else:
    print(f"Container '{container_name}' has stopped successfully.")
_write_container_log(container_name, container)
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
here for why decade failures (LR-013/FD-027) have gone unnoticed. The fix must correct the
docstring together with the behaviour, not leave a `Raises` section that is still untrue after
the fix changes *how* it raises.

**This directly contradicts this issue's own original "Out of Scope" note**, which read: "Module-
level `run_docker_container` (Trigger Forecasts) — does NOT have this bug; it does not re-raise
on failure but also does not silently continue a pipeline." Verified directly against trunk: it
does silently continue — there is nothing else in `run_pipeline` that inspects the outcome of any
`run_docker_container` call, so a failed `linreg` container does not stop the ML or
`postprocessing` containers from running afterward, exactly the failure class Bug 1 describes for
"Save Changes." The original claim was wrong; this revision corrects it rather than repeating it.

### Bug 4 (module-level function, both flows in principle): a missing or non-executable SSH tunnel script returns success-like `None`, no signal at all

Before ever running a container, the module-level `run_docker_container` checks for its SSH
tunnel script:

```python
if not os.path.isfile(SSH_TUNNEL_SCRIPT_ABSOLUTE):
    ...
    if not os.path.isfile(SSH_TUNNEL_SCRIPT_ABSOLUTE):  # second candidate path
        print(f"SSH tunnel script not found at: {SSH_TUNNEL_SCRIPT_ABSOLUTE}")
        return                                            # :4522 — silent None, no container run
if not os.access(SSH_TUNNEL_SCRIPT_ABSOLUTE, os.X_OK):
    print(f"SSH tunnel script is not executable: {SSH_TUNNEL_SCRIPT_ABSOLUTE}")
    return                                                 # :4527 — silent None, no container run
```

Either branch returns `None` with no exception and no distinguishing signal — indistinguishable,
to `run_pipeline`, from a container that ran and succeeded. This is a **third** way this specific
function can look successful while doing nothing, on top of Bug 3's silent status-ignore.

## Why re-raising alone does not meet this issue's own acceptance criteria — verify the whole call chain, not just the inner function

A prior draft of this issue's fix (see "Technical Analysis" below) assumed that making the inner
functions raise on failure would be sufficient. Verified against trunk, it is not — at every
layer above the inner function, something currently erases or hides the failure signal:

- **The module-level function's own re-raise would be caught by its own generic handler.** Any
  `raise` added inside the `try` block (including a new `raise ContainerError` at the status
  check) is still inside the same `try` whose `except Exception as e: print(...)` (`:4573-4574`)
  catches everything not specifically excluded. A naive "just add `raise`" fix does not propagate
  anything until that `except` is also changed to let `ContainerError` through specifically (the
  same pattern the nested function's own Option A needs — see below).
- **"Save Changes" would still show a completed progress bar.** Even if `save_to_database` sees
  the exception, its own `except docker.errors.DockerException as e: print(...)` (`:4175`)
  swallows it at that layer too, and its `finally` block unconditionally sets
  `progress_bar.value = 100` (`:4181`) regardless of outcome.
- **"Trigger forecasts" already tries to show an error — and then hides it.** `run_pipeline`'s own
  `except docker.errors.ContainerError as ce: progress_message.object = f"Container Error: {ce}"`
  (`:4443-4444`) *does* set a visible error message — but the enclosing `finally` block
  (`:4452`) sets `progress_message.visible = False` (`:4458`) immediately after, hiding the
  message it just set, on every code path including the error one.

**Conclusion**: a correct fix must change the inner functions (raise/return meaningfully), the
outer `except`/`finally` blocks in both `save_to_database` and `run_pipeline` (stop
unconditionally resetting the UI to a "done" appearance), and Bug 4's silent SSH-script returns —
not the inner functions alone. "Technical Analysis" below is revised accordingly.

## Why this matters now (2026-09-09)

This defect is the reason **LR-013** and **FD-027**'s documented "Save Changes" failures have
gone unnoticed: per those two issues, when an operator uses the decade horizon and clicks "Save
Changes," `postprocessing_operational.py` already rejects the dashboard's
`SAPPHIRE_PREDICTION_MODE=DECADE` value today, loudly (`sys.exit(1)`) — but that failure has
never reached the dashboard's UI, because of Bug 1 above. (Correction: `make_forecast.py` is
**not** implicated here — FD-027's out-of-loop review found "Trigger forecasts" has its own
stale-closure defect, FD-028, that makes it always send `PENTAD`, never `DECADE`, so ML never
sees the bad value via that path. Bug 3/Bug 4 above remain real and independently worth fixing —
they are why *any other* container failure in the "Trigger forecasts" flow, including FD-028's
own, goes unnoticed — just not the specific `DECADE` failure this paragraph originally described
on that flow.) This is the same defect shape as **P-007**
(`pipeline_docker.py`'s `run_docker_container` discarding `container.wait()`'s exit code across
20 Luigi call sites, fixed in PR #478): a container that fails is indistinguishable, at the
calling layer, from one that succeeded.

## Priority reconsidered: Low → High (2026-09-09)

Originally filed Low, reasoned narrowly from "Save Changes" alone and treated as a
code-cleanliness issue (propagate the exception properly). That does not hold once its actual
blast radius is understood:

- It affects **both** manual dashboard flows, not one (the original's own "Out of Scope" claim to
  the contrary was wrong — see Bug 3).
- It is the reason a *known-live, already-loudly-failing* defect (LR-013/FD-027's decade
  mismatch, confined to "Save Changes" — see the correction above) produces no operator-visible
  signal there, and the reason any *other*, unrelated container crash — OOM, network, code bug,
  or FD-028's own stale-horizon defect — produces none in **either** flow. This is the same
  "wrong data reads as success, not as a failure" hazard CLAUDE.md's Data I/O Transition section
  calls out, and the same shape that made **P-007** worth fixing project-wide in the Luigi
  pipeline.
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

### Option A: Re-raise from the `except` block, with `stderr` captured before removal (Bug 1/2, "Save Changes")

Let `ContainerError` propagate, and fix the missing `stderr`:

```python
except docker.errors.ContainerError:
    raise  # let container failures propagate
except Exception as e:
    print(f"Error running container '{container_name}': {e}")
```

```python
stderr_text = container.logs(tail=50).decode('utf-8', errors='replace')  # BEFORE remove()
try:
    container.remove(force=True)
except docker.errors.APIError as e:
    print(f"Warning: Failed to remove container '{container_name}': {e}")
raise docker.errors.ContainerError(
    container=container,
    exit_status=result['StatusCode'],
    command=None,
    image=full_image_name,
    stderr=stderr_text,
)
```

`stderr` is required by the pinned `docker` client library (`apps/forecast_dashboard/pyproject.toml:24`,
`"docker>=7.1.0"`) — confirmed against the library's `ContainerError.__init__` signature.
**Ordering matters**: the logs must be captured *before* `container.remove(force=True)` runs (the
current code removes the container, then would-be-raise afterward) — a removed container's logs
are not guaranteed to still be retrievable, so capturing `stderr` after removal risks constructing
`ContainerError` with an empty or failing `.logs()` call.

### Option B: Restructure try/except (Bug 1, "Save Changes")

Move the container-run logic out of the SSH-tunnel `try`/`except`, so only SSH tunnel errors are
caught there. More invasive but cleaner.

### For Bug 3 (module-level function, used by "Trigger forecasts"): raise past the function's own generic handler, and make the caller check it

Per "Why re-raising alone does not meet this issue's own acceptance criteria" above, this needs
more than adding a `raise`:

1. The module-level `run_docker_container` needs the same `raise
   docker.errors.ContainerError(..., stderr=...)` (stderr captured before removal, as above)
   added at its `if result['StatusCode'] != 0:` branch (`:4559`).
2. Its own outer `except Exception as e:` (`:4573`) must specifically let `ContainerError`
   through first (`except docker.errors.ContainerError: raise` before the generic handler),
   mirroring Bug 1/2's fix — otherwise the newly-added raise is caught right there and nothing
   changes.
3. `run_pipeline`'s own `finally` block (`:4452`) must stop unconditionally hiding
   `progress_message` (`:4458`) when an error occurred — it already sets a `Container Error:`
   message on the right exception (`:4443-4444`); the fix is narrower than it looks; and
   `save_to_database`'s `finally` block (`:4178`) must stop unconditionally forcing
   `progress_bar.value = 100` (`:4181`) on a failure path.
4. Bug 4's two silent `return` statements on a missing/non-executable SSH tunnel script (`:4522`,
   `:4527`) must also signal failure (e.g. raise, or return a sentinel the caller checks) rather
   than returning `None` indistinguishably from success.

### Recommendation

Option A for Bug 1/2, the four-part fix above for Bug 3/4, applied together — they are the same
defect shape (a failure signal manufactured, then discarded somewhere between the inner function
and the UI) recurring at every layer of both flows, not independent changes.

---

## Out of Scope

- LR-013's and FD-027's own fixes (normalizing `SAPPHIRE_PREDICTION_MODE` handling so the
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

None. Independent of LR-013 and FD-027 (see "Related" above).

## Acceptance Criteria

- [ ] `ContainerError` constructed with all required arguments including `stderr` (captured
  *before* `container.remove()`), in **both** `run_docker_container` implementations, and each
  implementation's own generic `except Exception` lets `ContainerError` through rather than
  re-swallowing it.
- [ ] A container failure in `linreg` prevents `postprocessing` from running in the "Save
  Changes" flow, **except** the deliberately-non-fatal `skill_recalc` step (`:4145-4161`), which
  must remain non-fatal by design — this criterion does **not** require the operator to see every
  container failure; it explicitly excludes `skill_recalc`'s own intentional catch. (This
  reconciles the prior draft's self-contradiction between "operator sees every failure" and the
  "Out of Scope" carve-out for `skill_recalc` below.)
- [ ] A container failure in `linreg` prevents the ML loop and `postprocessing` from running in
  the "Trigger forecasts" flow.
- [ ] A missing or non-executable SSH tunnel script (Bug 4, `:4522`/`:4527`) is signalled as a
  failure to the caller, not returned as a silent `None`.
- [ ] The operator sees an error message in the dashboard UI when a **non-excluded** container
  fails, in both flows — and that message is not immediately hidden by the enclosing `finally`
  block (`save_to_database`'s `:4178-4181`, `run_pipeline`'s `:4452-4458`), which must stop
  unconditionally resetting the progress indicator/message to a "done" appearance on a failure
  path.
- [ ] Existing success path unchanged in both flows.
- [ ] The module-level `run_docker_container`'s docstring (`:4491-4503`) is corrected to match
  its actual (fixed) behaviour — its `Raises` section must not describe a contract the body does
  not honour, before or after this fix.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` — zero failures,
  zero new skips.

## Fifth-pass corrections (2026-09-09, out-of-loop review of PR #506)

- Added **Bug 4**: missing/non-executable SSH tunnel script returns silent `None` at `:4522`/
  `:4527`, a third way the module-level function can look successful while doing nothing.
- Added the "Why re-raising alone does not meet this issue's own acceptance criteria" section —
  verified the whole call chain, not just the inner functions: the module-level function's own
  generic `except Exception` (`:4573-4574`) would still catch a newly-added raise;
  `save_to_database`'s `finally` (`:4178-4181`) unconditionally forces the progress bar to 100%
  regardless of outcome; `run_pipeline`'s own `finally` (`:4452-4458`) hides the
  `Container Error:` message its own `except` block (`:4443-4444`) already sets. Revised
  "Technical Analysis" and "Acceptance Criteria" to cover all three layers, not the inner
  functions alone.
- Reconciled a self-contradiction: the acceptance criteria demanded the operator see every
  container failure while "Out of Scope" preserved `skill_recalc`'s deliberately non-fatal catch
  — the relevant criterion now explicitly excludes `skill_recalc`.
- Added: `stderr` must be captured **before** `container.remove(force=True)` runs, not after —
  the current code order would otherwise construct `ContainerError` from a container that may
  already be gone. Confirmed the `docker` client library pin
  (`apps/forecast_dashboard/pyproject.toml:24`, `"docker>=7.1.0"`) requires the `stderr` argument.
- Citation fix: the exit-status check is at `:4559`, not `:4560` (the `print` is at `:4560`, the
  leftover comment at `:4561`).
- **Process note, not a content defect**: this file's 2026-09-09 rename (from the `low_prio_`
  prefix) is recorded by `git` as a delete-plus-add in the diff at default rename-detection
  thresholds, not as a detected rename — a reviewer skimming the diff stat could misread it as a
  deletion. Harmless (the content carried over verified-correct), but worth a one-line callout in
  the PR description.

## References

- `apps/forecast_dashboard/src/vizualization.py:3858` (nested `run_docker_container` def, "Save
  Changes"), `:3949` (exit-status check), `:3956-3961` (raise, missing `stderr` — Bug 2),
  `:3970-3971` (the swallow — Bug 1)
- `apps/forecast_dashboard/src/vizualization.py:4491` (module-level `run_docker_container` def,
  "Trigger forecasts"), `:4522, :4527` (silent SSH-script `return None` — Bug 4), `:4559`
  (exit-status check, no raise — Bug 3), `:4560` (print), `:4561` (leftover comment), `:4573-4574`
  (its own `except Exception`, catches only genuinely unexpected errors and would also catch a
  naively-added raise — must let `ContainerError` through specifically)
- `apps/forecast_dashboard/src/vizualization.py:3984` (`save_to_database`), `:4175`
  (`except docker.errors.DockerException`), `:4178-4181` (`finally`, unconditionally sets
  `progress_bar.value = 100`), `:4317` (`run_pipeline`), `:4443-4444`
  (`except docker.errors.ContainerError`, sets a visible message), `:4452-4458` (`finally`, hides
  that same message), `:4248` (`create_reload_button`, `run_pipeline`'s enclosing scope,
  confirming it has no local `run_docker_container` of its own and therefore resolves to the
  module-level one)
- `apps/forecast_dashboard/pyproject.toml:24` (`"docker>=7.1.0"` — the pinned client library
  version whose `ContainerError` requires `stderr`)
- `doc/plans/issues/high_prio_gi_draft_lr_unrecognised_mode_silent_exit_zero.md` (LR-013),
  `doc/plans/issues/high_prio_gi_draft_fd_decade_horizon_prediction_mode_spelling.md` (FD-027) —
  the concrete, already-live "Save Changes" failure this defect has been hiding
- `doc/plans/issues/high_prio_gi_draft_fd_trigger_forecasts_stale_horizon_closure.md` (FD-028 —
  a different defect this same swallow also hides, on the "Trigger forecasts" side)
- `doc/plans/issues/archive/high_prio_gi_draft_pipeline_container_exit_status_discarded.md`
  (P-007 — same defect shape, fixed once already in the Luigi pipeline)
