# LTF-010: the long-term recovery has no `run_locally.sh` target, so every local rehearsal is hand-assembled

**Status**: Draft (2026-09-04)
**Module**: `apps/run_locally.sh` (+ `apps/long_term_forecasting/lt_recovery.py`, read-only)
**Priority**: **Medium** — no production path is broken; `run_locally.sh` is a developer gate, not an
operational one. It is not Low because the one task with no local target is the one that writes
forecast rows on an operator's say-so, outside the normal schedule, with a documented check/write
race — the action you least want someone assembling by hand for the first time against a live
deployment.
**Labels**: `ltf`, `run_locally`, `developer-experience`, `rehearsal-gap`
**Found**: 2026-09-04, by the owner, after `bash apps/run_locally.sh long_term_forecasting` was
observed not to cover the recovery path at all.
**Related**: **LTF-009** (the issue that specified Stage A; shipped as PR #485), **INFRA-043** (the
dead `temp_luigi.cfg` mount found while implementing it), **INFRA-044** (a `DEGRADED` result state —
**not** a dependency, see "The exit taxonomy"), and a **follow-up to be filed**: `EXIT_REFUSED`
conflates a benign refusal with an infrastructure failure (see the same section).

> Verified on `docs_ltf_recovery_local_target`, branched from `origin/maxat_sapphire_2` at
> `e367e430`. All citations below are against that tree. **PR #485 is on trunk but was not in the
> branch this was first investigated from** — check you are on a branch that contains
> `apps/long_term_forecasting/lt_recovery.py` before reading further.

---

## The gap

LTF-009 Stage A shipped (PR #485, merge `eb18d932`) as a complete operator-invoked recovery:

| Piece | Where |
|---|---|
| Guard → run → read-back in one process | `apps/long_term_forecasting/lt_recovery.py` (new, 682 lines) |
| `--recover` flag, requires `--today` | `apps/long_term_forecasting/run_forecast.py:565-571`, `:586-590` |
| Dated Luigi task (`max_retries = 1`) | `apps/pipeline/pipeline_docker.py:2203-2233` |
| `task_type="lt_recovery"` wiring + per-(mode,date) marker | `pipeline_docker.py:2057-2091` |
| Operator wrapper | `bin/run_periodic_maintenance.sh` (`lt_recovery <env_file> <mode> <YYYY-MM-DD>`) |
| Tests | `apps/long_term_forecasting/tests/test_lt_recovery.py`, `apps/pipeline/tests/test_lt_dated_recovery.py` |

**`apps/run_locally.sh` was not touched, and contains zero occurrences of `lt_recovery`** (verified:
`grep -c lt_recovery apps/run_locally.sh` → `0`). Every other maintenance task has a local target —
`maintenance:preprocessing_runoff`, `maintenance:preprocessing_gateway`,
`maintenance:linear_regression`, `maintenance:machine_learning`,
`maintenance:postprocessing_forecasts`, `maintenance:postprocessing_long_term`,
`recalculate_snow_norms`, `recalculate_skill_metrics`, `calibrate_long_term`
(`run_locally.sh:60-68`). Of the four `RunPeriodicMaintenanceWorkflow` task types, the recovery is
the only one with no corresponding local target.

**To be precise about what is and is not missing**: `run_forecast.py --today <ISO> --recover` is a
perfectly valid direct CLI invocation and the long-term venv has everything it needs
(`run_forecast.py:553`, `:603` call `run_recovery()` directly). Luigi supplies orchestration,
`max_retries = 1` and a marker — none of which is required to *run* a recovery. So this issue is
**not** "recovery cannot be run locally". It is: there is no standardised, documented, tested entry
point, so every local rehearsal is hand-assembled from the module's internals, and the operator
learns the argument shape by reading source.

> **Do not check this with a naive grep** — the wrapper's `task_type` names and `run_locally.sh`'s
> target names differ, so three of the four look missing when they are not. Map them by function:
>
> | `run_periodic_maintenance.sh` task_type | `run_locally.sh` target |
> |---|---|
> | `long_term` | `maintenance:postprocessing_long_term` |
> | `skill_recalc` | `recalculate_skill_metrics` (impl `:1048`, dispatch `:2253`) |
> | `snow_norms` | `recalculate_snow_norms` (impl `:1069`, dispatch `:2253`) |
> | `lt_recovery` | **none** |
>
> `grep -c skill_recalc apps/run_locally.sh` returns `0` and proves nothing.

## Why this matters more than a missing convenience target

`lt_recovery.py`'s own module docstring states the hazard plainly:

> the guard is not a lock … A concurrent operational run, another recovery, or a manual writer that
> inserts rows for the same `(horizon_type, horizon_value, effective_date)` during that window will
> be silently overwritten by this run's upsert … **Run a recovery when no long-term forecast is in
> flight.**

That is an operational precondition a human must satisfy by judgement. An operator who has never
run the command cannot practise satisfying it anywhere except a live deployment. The local runner is
where every other maintenance action is rehearsed before it is trusted.

## The exit taxonomy — and why `REFUSED` must stay non-zero

`lt_recovery` defines a three-valued outcome (`lt_recovery.py:69-73`):

```
EXIT_OK      = 0
EXIT_FAILED  = 1
EXIT_REFUSED = 2
```

`bin/run_periodic_maintenance.sh:185` describes exit 2 as *"'REFUSED' (child exit 2) - nothing ran,
no rows were written."*

**That description is true of the rows and misleading about everything else, and an earlier revision
of this issue built its whole design on it.** Read the code:

```python
    except RecoveryError as exc:
        logger.error("Long-term recovery REFUSED (nothing was run): %s", exc)
        return EXIT_REFUSED
    except Exception as exc:                      # <-- lt_recovery.py:625-627
        logger.exception("Long-term recovery REFUSED (nothing was run): %s", exc)
        return EXIT_REFUSED
```

Stage 1 ends in a bare `except Exception`, so **exit 2 also covers a configuration-loading error, an
invalid mode name, an empty station scope, an unavailable or disabled API client, a readiness
failure and a query error** — not only the benign `RecoveryRefused` ("rows already exist" / date
outside the window).

Therefore:

- **`EXIT_REFUSED` must remain a non-zero result in `run_locally.sh`.** Rendering it as a passing or
  merely-degraded outcome would report an unreachable API or a typo'd mode as "nothing to do" — the
  exact silent-failure shape INFRA-044, INFRA-030 and ML-022 all exist to prevent. Report it with
  its own wording (`REFUSED — nothing was run`) so it is distinguishable from `FAILED`, but keep the
  process exit non-zero.
- **This issue does NOT depend on INFRA-044.** An earlier revision claimed it did, on the assumption
  that REFUSED was benign. It is not, so the target can ship against today's binary PASS/FAIL.
- Splitting benign refusal from infrastructure failure would need a change inside `lt_recovery.py`
  (it already has a distinct `RecoveryRefused` class to key on), which **C5 forbids**. That split is
  worth its own issue, and is the *prerequisite* for ever mapping a refusal to `DEGRADED`. File it;
  do not smuggle it in here.

| Recovery exit | `run_locally.sh` row | Process exit |
|---|---|---|
| 0 — recovered, and the read-back found at least one row | `PASS` | 0 |
| 2 — REFUSED: guard declined **or** stage 1 errored | `FAIL`, labelled `REFUSED — nothing was run` | non-zero |

*(Rendered as PASS / REFUSED-FAIL / FAIL — there is no `DEGRADED` outcome here until **LTF-011**
splits the two causes of exit 2.)*
| 1 — the forecast ran and failed | `FAIL` | non-zero |
| anything else (parser error, signal) | `FAIL` | non-zero |

**Exit 0 is a partial-success criterion, not proof of complete coverage.** The read-back accepts the
run once at least one finite row carries `forecast_run_flag=1` (`lt_recovery.py:641`, `:673`); it
does not verify every station × model. Do not describe a `PASS` row as "the month is fully
recovered".

## The contract

**C1 — a new `maintenance:long_term_forecasting` target.** Name it for the module, consistent with
the existing `maintenance:<module>` family. It runs, in the `long_term_forecasting` venv:

```
run_forecast.py --today <ISO date> --recover
```

with the forecast mode supplied through the module's existing `lt_forecast_mode` environment
variable — that is how `run_forecast.py` already selects a mode (`:447`,
`os.getenv("lt_forecast_mode")`), and how `run_locally.sh` already passes it for the simulate and
operational targets (`:786`, `:796`, `:850`). Do **not** invent a second mode variable.

It does **not** go through Luigi or Docker. Everything `run_locally.sh` runs is a direct venv
invocation; the Luigi task's `max_retries = 1` and its per-(mode,date) marker are orchestration
concerns with no local equivalent.

**"Local" does not mean "self-contained".** The target still runs against whatever the supplied env
file points at. Document these prerequisites at the target, because a rehearsal that silently lacks
one of them will surface as a confusing `REFUSED`:

- the env file plus its configuration / model / static / intermediate paths;
- `SAPPHIRE_API_ENABLED=true` and a host-reachable postprocessing API at `SAPPHIRE_API_URL`
  (`lt_recovery.py:333`);
- a host-reachable preprocessing database and its credentials (`data_interface.py:35`);
- historical inputs already present for the requested issue date;
- `IN_DOCKER` **explicitly false** — the target must pass `IN_DOCKER=False` to the child rather than
  rely on it being unset, because `run_in_venv` inherits the ambient environment (`run_locally.sh:623`)
  and the database host is chosen from that variable (`data_interface.py:65`); credentials and host
  selection are at `data_interface.py:48`.

**Clock caveat.** Eligibility uses the process-local clock, deliberately matching
`lt_utils.check_valid_forecast_issue_date` (`lt_recovery.py:35`, `:123`). A rehearsal in a local
timezone and a container running UTC can briefly disagree about which issue dates are permitted near
a month boundary. Note it in the usage text; do not claim the local run is behaviourally identical
to the deployment.

**C2 — both parameters are required, and a missing one is a loud refusal.** The date comes from a
new, **mandatory** `LT_RECOVERY_DATE` variable. It is stylistically like
`RUNOFF_LONG_HORIZON_TARGET_YEAR` (`run_locally.sh:894`) but semantically the opposite: that one is
*optional* and omitting it deliberately preserves the underlying script's default. There is no
sensible default issue date for a recovery, so omission is an error, not a fallback.
If either `lt_forecast_mode` or `LT_RECOVERY_DATE` is unset or empty, the target must print what is
missing and exit non-zero **without running anything**.

It must not fall back to a default date, must not infer the mode, and must not run "all modes".
The CLI itself rejects a recovery without `--today` (via `parser.error` at `:588` when `--recover`
is combined with a selection flag, and via the required mutually-exclusive group otherwise) — but
the shell target must fail *before* spawning the process, so the operator gets a message about
`run_locally.sh`'s own interface rather than an argparse error about flags they never typed.

> **This is the ML-022 / INFRA-030 shape and the reason C2 exists.** A `maintenance:` target that
> resolves its parameters from unset environment variables and quietly does nothing, reporting
> success, has already happened twice in this file. A recovery target that silently recovers
> *nothing* would be worse: the operator concludes the month is repaired.

**C3 — it must NOT be part of any aggregate target.** A dated recovery is a deliberate,
argument-bearing, one-month action. Wiring it into a run-everything target would either abort that
run via C2 or, worse, run it with stale parameters. Aggregates here are hard-coded lists
(`run_locally.sh:1400`, `:1485`), so this is achieved by *not* adding the name to them — but the
test must cover **every** aggregate, not just the obvious three: `maintenance`, `daily`, `all`,
`long-term`, `long-term-operational` and `yearly`. Add the new name through explicit valid-target
and dispatch handling only, and state this in `print_usage`.

**C4 — the run's side effects must be documented at the target, precisely.** They are conditional,
and some of them happen *before* a refusal:

- a successful model run overwrites `{model}_forecast.csv` (`lt_utils.py:465`);
- the hindcast CSV is rewritten **only if it already exists**; if absent the code warns and does not
  create one (`lt_utils.py:529`);
- configuration synchronisation rewrites each model's `general_config.json`
  (`config_forecast.py:111`, `:157`) — this runs in stage 1, so **it can happen and then the run can
  still be REFUSED**. A refusal is therefore not side-effect-free;
- acceptance is defined on database rows only, and only partially (see the exit-taxonomy section).

**Do not describe the target as writing to "a dev database".** It writes wherever the supplied env
file points. `print_usage` and the target's banner must state the env file's role prominently enough
that nobody rehearses against production by reflex.

**C5 — do not modify the recovery implementation.** `lt_recovery.py`, `run_forecast.py`,
`pipeline_docker.py` and `bin/run_periodic_maintenance.sh` are all out of scope. This issue adds a
local entry point to code that already works; if the rehearsal reveals a defect in the recovery
itself, that is a new issue, not a widening of this one.

## Files that may be modified

- `apps/run_locally.sh`
- `apps/pipeline/tests/test_run_locally_orchestration.py` (the established harness for driving
  `run_locally.sh` from tests with fake venv stubs)
- `doc/dev/testing_workflow.md` and/or `apps/run_locally.sh`'s own usage block, for C4
- `doc/plans/module_issues.md` — register the LTF-010 row. The number is free on this branch (which
  has LTF-001..007) and avoids LTF-008/009, claimed on the unmerged `docs_fd024_fd025_doc008`
  branch; it is not reserved until that row is committed.

**Do not** change any file listed in C5.

## Tests

Follow the existing pattern in `test_run_locally_orchestration.py`: fake `.venv/bin/<exe>` stubs
that record their argv and exit with a chosen code.

**The harness needs a change first.** The fake venv Python currently records
`SAPPHIRE_PREDICTION_MODE` but **not** `lt_forecast_mode` (`test_run_locally_orchestration.py:121`),
so test 6 cannot be written against it as-is. Extend the stub to record `lt_forecast_mode` as part of
this issue's work.

1. **Happy path**: `lt_forecast_mode=month_0 LT_RECOVERY_DATE=2026-08-01`, stub exits 0 → the stub
   was invoked with `run_forecast.py --today 2026-08-01 --recover`, the summary row is `PASS`,
   process exit 0.
2. **REFUSED**: stub exits 2 → the row is a failure labelled `REFUSED — nothing was run`, and the
   process exits **non-zero**. This is the test that pins the "exit 2 is not benign" decision; it
   must fail if someone later maps REFUSED to a passing or DEGRADED result without first splitting
   the refusal classes inside `lt_recovery.py`.
3. **Failed**: stub exits 1 → `FAIL`, process exit 1.
4. **Missing mode** and **missing date**, separately: the target exits non-zero, names the missing
   variable, and **the stub is never invoked** (assert zero recorded invocations — that is the
   assertion that catches a silent no-op).
5. **Not in the aggregates**: running `maintenance`, `daily`, `all`, `long-term`,
   `long-term-operational` and `yearly` never invokes the recovery stub, whatever
   `lt_forecast_mode` / `LT_RECOVERY_DATE` are set to.
5b. **`--dry-run` with missing parameters** still fails, i.e. the check sits ahead of the dry-run
   early return (`run_locally.sh:2166`).
6. **Mode is passed through, not defaulted**: `lt_forecast_mode=quarter` reaches the child process
   as `quarter`.
7. **Skipped organisations (C2)**: with the organisation set to `demo`, and again `uzhm`, and both
   parameters supplied — the target prints that long-term recovery is not available for this
   deployment, names the organisation, exits **non-zero**, and **the recovery stub is never
   invoked**. `long_term_forecasting` is in both skip lists (`run_locally.sh:211`) via
   `should_skip_module` (`:482`).
8. **Docker hostname cannot leak in (C1)**: with `IN_DOCKER=True` exported ambiently, the child is
   still invoked with `IN_DOCKER=False`, so `data_interface.py:65` selects the host-side database
   name. `run_in_venv` inherits the ambient environment (`run_locally.sh:623`), so this must be set
   explicitly, not assumed.

## Acceptance criteria

- [ ] `EXIT_REFUSED` is reported distinctly **and** keeps the process exit non-zero; test 2 passes.
- [ ] `bash apps/run_locally.sh maintenance:long_term_forecasting` with both variables set performs
      a real recovery against a local deployment and reports the correct one of PASS / DEGRADED /
      FAIL for each of the three exit codes.
- [ ] With either variable unset it exits non-zero, names the variable, and runs nothing.
- [ ] On `demo` and `uzhm` it says long-term recovery is not available for this deployment and exits
      non-zero, without invoking the recovery.
- [ ] The child is invoked with `IN_DOCKER=False` regardless of the ambient value.
- [ ] `maintenance`, `daily` and `all` are unchanged — verified by test, not by inspection.
- [ ] `print_usage` documents the target, both variables, and the CSV/database side effects.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.
- [ ] `bash -n apps/run_locally.sh` clean; `shellcheck` clean if it is in the pre-commit set.

## Phases

- **P1 — the target (C1, C2, C3).** Files: `run_locally.sh`, `test_run_locally_orchestration.py`
  (including the stub change for `lt_forecast_mode`). Depends on: none. Agents: 1.
  Accept: tests 1-6 and 5b pass.
- **P2 — documentation (C4).** Files: `run_locally.sh` usage block, `doc/dev/testing_workflow.md`.
  Depends on: P1. Agents: 1. Accept: the side effects and both variables are documented.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 }
  }
}
```

*(No cross-issue dependency: an earlier revision made P1 depend on INFRA-044, on the mistaken premise
that a REFUSED recovery was benign. It is not — see the exit-taxonomy section — so this issue ships
independently.)*

## Open decision for the owner

**Should the local target also cover LTF-009 Stage B when that lands?** Stage B drives the same task
from `task_map` monthly, unattended. A local target for an unattended job is a different thing from
a local target for an operator command — it would want a "what would you do today?" dry-run rather
than a dated single-month recovery. Recommend keeping this issue to Stage A and revisiting when
Stage B is designed; noting it here so the target's name (`maintenance:long_term_forecasting`) is
not later found to have been claimed by the wrong one of the two.

## Corrections applied after out-of-loop review (2026-09-04)

- **Blocker.** The whole design rested on `EXIT_REFUSED` being benign ("nothing ran, no rows
  written", per `run_periodic_maintenance.sh:185`). The code tells a different story: stage 1 ends in
  a bare `except Exception` (`lt_recovery.py:625-627`), so exit 2 also covers config errors, invalid
  modes, an unavailable API and query failures. Mapping it to `DEGRADED`/exit 0 would have reported
  an outage as "nothing to do". REFUSED now stays non-zero, and the **INFRA-044 dependency is gone**
  — the target ships independently.
- The framing "can only be rehearsed on a deployment" was **false**:
  `run_forecast.py --today <ISO> --recover` is directly runnable in the module venv. The defect is
  the absence of a standardised, documented, tested target — not the impossibility of local
  invocation.
- Exit 0 is a **partial**-success criterion (at least one row with `forecast_run_flag=1`), not proof
  of full station × model coverage.
- Added the real prerequisites for a direct run (API reachable, DB reachable, `IN_DOCKER` false,
  historical inputs present) and the local-vs-container clock caveat near month boundaries.
- Side effects corrected: the hindcast rewrite is **conditional** on the CSV already existing, and
  `general_config.json` is rewritten in stage 1 — so a REFUSED run is **not** side-effect-free. Also
  dropped the assumption that the env file points at a dev database.
- C2's check must sit ahead of the `--dry-run` early return (`:2161`), and the organisation-skip
  behaviour for `demo`/`uzhm` needs an explicit answer rather than an inherited default.
- The aggregate-exclusion test must cover all six aggregates, not three.
- The test harness records `SAPPHIRE_PREDICTION_MODE` but not `lt_forecast_mode`, so the stub needs
  extending before test 6 can exist.
- The "only maintenance task without a local target" claim is narrowed to the four
  `RunPeriodicMaintenanceWorkflow` task types; other Luigi housekeeping tasks
  (`DeleteOldGatewayFiles`, `DeleteOldMarkerFiles`, `LogFileCleanup`) also have none.
- Citations corrected for the `recalculate_*` target implementations and dispatch, the `--recover`
  argparse path, and the runoff-year analogy (optional there, mandatory here).

## Corrections applied 2026-09-04

- The "not available here" decision for `demo`/`uzhm` existed only in prose; it now has a contract
  clause, a test and an acceptance criterion, with the skip mechanism cited (`run_locally.sh:211`,
  `should_skip_module` at `:482`).
- `IN_DOCKER` was merely documented as "should be false". `run_in_venv` inherits the ambient
  environment, so the target must pass `IN_DOCKER=False` explicitly, with a test that an ambient
  true value cannot select the Docker database hostname (`data_interface.py:65`).
- Result wording corrected to PASS / REFUSED-FAIL / FAIL — there is no `DEGRADED` outcome until
  LTF-011 lands.
- Citations: dry-run return `:2166`; recovery marker block ends `:2097`; database host/credentials
  `data_interface.py:48`; `general_config.json` write `config_forecast.py:168`.

## Out of scope

- Any change to the recovery implementation itself (C5).
- LTF-009 Stage B, and its prerequisites (LTF-008's outcome contract, backfill provenance).
- INFRA-043's dead `temp_luigi.cfg` mount, found while implementing Stage A.
- The check/write race in the guard — documented and accepted in Stage A; closing it needs a
  conditional insert or an advisory lock on the service side.
