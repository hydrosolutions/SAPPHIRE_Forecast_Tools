# P-007: `run_docker_container` discards the container's exit code — every Luigi pipeline task reports success

**Status**: **Complete** (2026-08-24) — fixed in **PR #478**, merged to `maxat_sapphire_2`
**Module**: `apps/pipeline`
**Priority**: **High** — a container that crashes, is missing its entrypoint script, is OOM-killed
or exits non-zero for any reason is reported to Luigi as a **success**. This affects **every**
containerised pipeline task, not one module.
**Labels**: `pipeline`, `luigi`, `docker`, `silent-failure`, `error-handling`
**Found**: 2026-08-21, by the (late) post-implementation review gate for PREPG-007.
**Related**: **P-004** — fixes the *timeout* branch of this same function; see § Why P-004 does not
cover this. **INFRA-023** — the *shell wrapper* around it also exits 0 regardless. **FD-008** — the
same-named function in `forecast_dashboard`, a different code path that *does* read `StatusCode`.

---

> **Completed 2026-08-24 (PR #478).** `container.wait()`'s `StatusCode` is now read, accepting only
> a genuine `int` via `type(raw) is int` — **not** `isinstance`, because `bool` subclasses `int` and
> `False == 0`, so `{"StatusCode": False}` would otherwise have read as success inside the fix for
> reading-as-success. Anything else (`None`, `"0"`, `0.0`, missing key, non-dict) becomes `1`.
> Timeout still returns `124`, unchanged. 286 tests pass.
>
> **TWO THINGS TO EXPECT ON ROLLOUT — neither introduced by the fix, both consequences of it:**
>
> 1. **Tasks that were failing silently will start failing visibly.**
>    `YearlySnowNormRecalculation` is first: it launches the `sapphire-pipeline` image, which does
>    not contain `recalculate_snow_norms.py`. That is a real pre-existing defect this fix reveals,
>    **not** a regression — it needs its own one-line correction
>    (`sapphire-prepgateway` + the preprocessing-gateway working directory, matching
>    `bin/yearly_snow_norm_recalculation.sh`).
> 2. **Success markers written by the buggy version are not invalidated.** Luigi may therefore skip
>    a task on the first run after deploy, because a marker from a run that "succeeded" while
>    actually failing is still on disk.
>
> **Do NOT remove the guard at `pipeline_docker.py:2417-2421`** — commented *"defense-in-depth
> against the pre-existing exit-code bug"*. It is now redundant for this defect specifically, but it
> also catches a missing marker for any other reason. It was left in place deliberately.
>
> **Still open in the same family:** **INFRA-023** — `run_periodic_maintenance.sh` has no `set -e`,
> never captures its `docker compose run` status, and ends on two `echo`s, so the *shell wrapper*
> still exits 0 regardless. Fixing this issue does not make a failing periodic cron visible to an
> operator; both layers must close. **P-004** covers the timeout branch and remains valid.
>
> **Noted, not fixed:** `pipeline_docker.py:428` embeds the full container `logs` in the raised
> exception. Same disclosure class as PREPG-015/017 — container logs can carry the Data Gateway API
> key. Pre-existing; deliberately out of scope here.

## The defect

`apps/pipeline/pipeline_docker.py:329-347`:

```python
try:
    self.run_with_timeout(container.wait)
    exit_status = 0                     # <-- container.wait()'s return value is DISCARDED
except TimeoutError:
    ...
    exit_status = 124
```

`container.wait()` returns `{"StatusCode": N}`. That return value is thrown away and
`exit_status` is hard-coded to `0`. The **only** way this function reports failure is a timeout.

A container that starts and exits `1` — crash, traceback, missing script, bad arguments, OOM —
is indistinguishable from one that succeeded.

**Blast radius: 20 `run_docker_container(...)` call sites across 8 images** —
`sapphire-postprocessing` (6), `sapphire-preprunoff` (3), `sapphire-linreg` (3),
`sapphire-prepgateway` (2), `sapphire-ml` (2), `sapphire-lt-forecasting` (2),
`sapphire-pipeline` (1), `sapphire-conceptmod` (1).

## A live instance, which is how this was found

The scheduled annual snow-norm task launches the **wrong image**
(`pipeline_docker.py:2024-2034`):

```python
image_name="sapphire-pipeline",
command=["uv", "run", "recalculate_snow_norms.py"],
```

but `apps/pipeline/Dockerfile:19-24` copies only `apps/iEasyHydroForecast` and `apps/pipeline`.
`recalculate_snow_norms.py` lives in **`apps/preprocessing_gateway/`** and is therefore **not in
that image**. The container cannot run the script.

Because of the defect above, that failure is reported as success. The standalone wrapper
`bin/yearly_snow_norm_recalculation.sh:70,121-130` correctly uses `sapphire-prepgateway` — but the
documented cron calls `run_periodic_maintenance.sh snow_norms`, which routes to the broken Luigi
task, not the working script.

**Both halves are needed for the observed outcome:** the wrong image makes it fail; the discarded
exit code makes the failure invisible.

## Why P-004 does not cover this — and why P-004 alone is insufficient

P-004 ("silent timeout failure in `execute_with_retries`") is scoped **exclusively to the timeout
branch**: its own text says *"When a Docker container times out (exit_status == 124)"*, and its fix
is to raise when `exit_status == 124`.

That fix is correct and should still land — but it **cannot catch this**, because a crashed
container never reaches `124`. It reaches `0`. P-004 hardens one branch of a function whose other
branch is unconditionally `0`.

Similarly **INFRA-023** documents that `run_periodic_maintenance.sh` never captures its
`docker compose run` status and exits 0 — that is the *shell wrapper* one layer out. Fixing it
alone would surface nothing, because the Python layer it wraps already reported success.

**Three layers, three separate silences.** All three must be closed for a failed pipeline task to
become visible.

## The fix

- Capture the status: `result = container.wait()`, then `exit_status = result.get("StatusCode", 1)`.
  **Default to non-zero on a malformed/absent result**, not to zero.
- Keep `124` for the timeout path so P-004's fix continues to work unchanged.
- Read the logs **before** removing the container (already done) and include the tail in the
  failure detail, so a non-zero status is actionable rather than just a number.

## Acceptance criteria

- A container exiting non-zero yields a **non-zero** `exit_status` from `run_docker_container`, and
  the calling Luigi task **fails** rather than completing. Pin with a fake docker client whose
  `wait()` returns `{"StatusCode": 1}`.
- A container exiting `0` still yields `0` — the happy path is unchanged.
- A `wait()` result lacking `StatusCode` yields **non-zero**, not zero.
- The timeout path still yields `124`, so P-004's behaviour is preserved.
- **A deliberately-broken case is included**: a task pointed at an image lacking its script must
  fail the test. Without this the suite cannot distinguish "verified" from "never exercised" —
  the existing task tests stop at routing (`apps/pipeline/tests/test_maintenance_tasks.py:294-337`)
  and would pass unchanged if this defect were reintroduced.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` green, zero unexpected skips.

## Contract not to break

- **Do not change what a timeout returns.** `124` is load-bearing for P-004 and for
  `execute_with_retries`' retry decisions.
- Tasks that currently pass must keep passing — this makes *failures* visible, it must not
  invent them. Expect previously-green scheduled tasks to start failing once this lands; that is
  the point, but it should be announced rather than discovered.

## Separately worth fixing (do not fold in silently)

The snow-norms task's image/command mismatch is its own one-line correction
(`sapphire-prepgateway` + the preprocessing-gateway working directory, matching
`bin/yearly_snow_norm_recalculation.sh`). It is listed here because it is the instance that
exposed the defect — but fixing only it would leave the other 19 call sites silent.
