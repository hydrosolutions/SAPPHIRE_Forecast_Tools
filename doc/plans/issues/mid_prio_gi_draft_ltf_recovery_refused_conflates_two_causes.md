# LTF-011: the recovery reports "REFUSED" for both "already done" and "something is broken"

**Status**: Draft (2026-09-04)
**Module**: `apps/long_term_forecasting/lt_recovery.py`
**Priority**: **Medium** — nothing is lost or corrupted; the recovery correctly declines to run in
every case it reports. But the operator cannot tell "there was nothing to do" from "I could not
reach the database", and both are reported with the same words and the same status.
**Labels**: `ltf`, `recovery`, `exit-contract`, `operator-experience`
**Found**: 2026-09-04, while drafting **LTF-010** (the missing `run_locally.sh` target). Filed rather
than fixed there, because LTF-010 is explicitly forbidden from changing the recovery implementation.
**Related**: **LTF-009** (shipped Stage A), **LTF-010** (blocked from rendering a refusal as anything
but a failure until this lands), **INFRA-044** (the `DEGRADED` state a benign refusal would map to).

---

## What happens now

`run_recovery` ends stage 1 with two handlers that return the same code
(`lt_recovery.py:622-627`):

```python
    except RecoveryError as exc:
        logger.error("Long-term recovery REFUSED (nothing was run): %s", exc)
        return EXIT_REFUSED
    except Exception as exc:
        logger.exception("Long-term recovery REFUSED (nothing was run): %s", exc)
        return EXIT_REFUSED
```

So `EXIT_REFUSED` (2) covers **two categories that mean opposite things to the operator**:

| Category | Example | What the operator should do |
|---|---|---|
| **Benign refusal** — the guard did its job | member rows already exist for that key; the issue date is outside the permitted window | Nothing. The month is already populated, or the date was wrong. |
| **Something is broken** | configuration failed to load, the mode name is invalid, the station scope came back empty, the API is unavailable or disabled, a readiness check or query failed | Investigate and retry. The month is still missing. |

`bin/run_periodic_maintenance.sh:185` documents exit 2 as *"'REFUSED' (child exit 2) - nothing ran,
no rows were written."* That is true of the rows in both categories, and actively misleading about
the second: it reads as "there was nothing to do".

## Why it matters beyond tidiness

- **An outage looks like success-adjacent.** An operator recovering a missed month while the
  postprocessing API happens to be down is told "REFUSED — nothing was run". The natural reading is
  "already fine". The month stays missing.
- **It blocks a correct local target.** LTF-010 must render exit 2 as a failure precisely because it
  cannot distinguish the two. Once split, the benign half can become a warning
  (INFRA-044's `DEGRADED`) while the broken half stays red. **This issue is the prerequisite for
  that**, and LTF-010 says so.
- **The information already exists.** `RecoveryRefused` is a distinct exception class. The code
  already knows which category it is in; it just discards the distinction at the return.

## The real taxonomy — enumerated, because it is not what the class names suggest

An earlier revision of this issue said "keep exit 2 for `RecoveryRefused`, everything else 1". **That
is wrong**, because `RecoveryRefused` is raised for three different kinds of thing, not one. Every
raise site, read from the code:

| Site | Condition | What it really means |
|---|---|---|
| `:204`, `:208`, `:212` | issue date missing, not zero-padded, not a calendar date | operator typed it wrong |
| `:240` | issue date in the future | operator typed it wrong |
| `:247` | outside the current/previous calendar month window | operator asked for something out of scope |
| `:285`, `:295` | not a scheduled issue date for any member model of this mode | operator asked for something out of scope |
| `:565` | no forecast mode supplied | operator did not set `lt_forecast_mode` |
| `:320` | **station list is empty** | **the deployment is misconfigured** |
| `:563`-area | missing member-model configuration | **the deployment is misconfigured** |
| `:608` | member rows already exist for the key | **already done** |

And `RecoveryQueryError` — API unreachable, not ready, query failed (`:345`, `:350`, `:359`,
`:361`, `:471`) — **also inherits `RecoveryError`**, so any `except RecoveryError` conflates it with
all of the above.

## The contract

**C1 — split on meaning, not on the existing class names.** Two outcomes, mapped from three
meanings:

- **`EXIT_REFUSED` (2) — "declined, and nothing is wrong":** member rows already exist, **and** the
  operator-input refusals (bad/missing date, missing mode, future date, outside the window, not a
  scheduled issue date). In all of these the system is healthy and the answer is "I am not doing
  that, and here is why". They belong together because none of them warrants investigating the
  deployment.
- **`EXIT_FAILED` (1) — "could not be attempted":** empty station list, missing member-model
  configuration, every `RecoveryQueryError`, and every unexpected exception. In all of these the
  month is still missing and something needs fixing.

**This requires reclassifying two raise sites**, because they are currently `RecoveryRefused` and
belong in the failure bucket: the empty station list (`:320`) and the missing member-model
configuration. Introduce a `RecoveryMisconfigured(RecoveryError)` subclass and raise it there,
rather than widening the handler — the handler must stay readable, and the classification belongs at
the raise site where the condition is known.

Then stage 1's handlers become, in order: `except RecoveryRefused` → 2; `except RecoveryError` → 1
(this now catches `RecoveryMisconfigured` and `RecoveryQueryError`); `except Exception` → 1.

Do not add a fourth exit code. Three meanings, two codes, and the message carries the detail.

**C2 — the log lines must stop being identical.** The refusal path keeps "REFUSED (nothing was
run)". The failure path must say the recovery could not be *attempted* and name the exception type,
so a log reader can tell them apart without the exit code.

**C3 — the wrapper's documentation must follow.** `bin/run_periodic_maintenance.sh:185` describes
exit 2 as "nothing ran, no rows were written". After C1 that is accurate for exit 2 and needs a
matching line for exit 1; the exit handling itself is at `:177`. Update both.

**C4 — do not touch the guard's semantics.** Which conditions refuse, the check/write race, the
`--today` window and the read-back acceptance are unchanged. This issue changes how the outcome is
*classified and reported*, and moves two raises between classes. Nothing else.

**C5 — a REFUSED run is not side-effect-free, and the docs should stop implying it is.** Stage 1
loads and synchronises configuration before the guard can decline, writing each model's
`general_config.json` (`config_forecast.py:168`). "Nothing ran" refers to the forecast and the
database rows, not to the process. Say so.

## Files that may be modified

- `apps/long_term_forecasting/lt_recovery.py`
- `apps/long_term_forecasting/tests/test_lt_recovery.py`
- `bin/run_periodic_maintenance.sh` (C3, documentation lines only)

**Do not** change `run_forecast.py`, `pipeline_docker.py`, or the guard's conditions.

## Tests

Use `19999` as a station code if one is needed.

1. **Rows already exist** → exit **2**, message says nothing was run.
2. **Issue date outside the permitted window** → exit **2**.
2b. **Malformed / missing / future issue date**, and **missing forecast mode** → exit **2** each.
   These are operator-input refusals and must not become failures.
3. **Configuration load fails** → exit **1**, message names the exception type and says the recovery
   could not be attempted.
4. **API unreachable / not ready** (`RecoveryQueryError`) → exit **1**.
5. **Empty station scope** → exit **1** — this is a reclassification, so the test must fail against
   today's code, where it exits 2. Same for **missing member-model configuration**.
6. **Unexpected exception in stage 1** (raise something the code does not anticipate) → exit **1**,
   not 2. Regression guard: it must fail if someone re-widens the bare `except` back over the
   refusal code.
6b. **Handler order**: a `RecoveryQueryError` must not be caught as a refusal — pins that
   `except RecoveryRefused` precedes `except RecoveryError`.
7. **The existing Luigi and wrapper behaviour is unchanged for exit 1** — a failed recovery is still
   reported as unsuccessful (`run_periodic_maintenance.sh:174`).

Check by hand that each new test fails if C1 is reverted, and say so in the report.

## Acceptance criteria

- [ ] Exit 2 is returned **only** for "already done" and operator-input refusals; empty station
      list, missing member-model config, query errors and unexpected exceptions all return 1.
- [ ] The two log messages are distinguishable without the exit code.
- [ ] `bin/run_periodic_maintenance.sh`'s exit-code documentation matches the new behaviour, and no
      longer implies a refusal is side-effect-free.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.
- [ ] `ruff check` / `ruff format --check` clean; `bash -n bin/run_periodic_maintenance.sh` clean.

## Phases

- **P1 — split the outcome (C1, C2).** Files: `lt_recovery.py`, `test_lt_recovery.py`.
  Depends on: none. Agents: 1. Accept: tests 1-7 pass.
- **P2 — documentation (C3, C5).** Files: `bin/run_periodic_maintenance.sh`, and
  `lt_recovery.py`'s module docstring. Depends on: P1. Agents: 1.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 }
  }
}
```

## Out of scope

- The check/write race in the guard (documented and accepted in Stage A; closing it needs a
  conditional insert or an advisory lock service-side).
- LTF-010's `run_locally.sh` target — it ships independently, treating exit 2 as a failure until
  this lands.
- LTF-009 Stage B.
