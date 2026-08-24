# INFRA-039: `validate_env` never validates `SAPPHIRE_PREDICTION_MODE` / `ML_MODE` for the outer-loop targets, so a mode that is valid for one consumer silently produces no forecasts in another

**Status**: Review (2026-08-21). Plan reviewed out-of-loop (codex, open-ended pass) before commit;
that pass overturned three claims in the first draft — see § Review corrections.
**Module**: `apps/run_locally.sh` (`validate_env`)
**Priority**: Medium — no crash, no error, exit 0. Reachable not only by typo but by an operator
carrying a **legitimate** mode (`ALL`, `MONTHLY`) from one target to another. Not confirmed to
have bitten a deployment.
**Labels**: `run_locally`, `machine_learning`, `linear_regression`, `silent-noop`, `dx`
**Found**: 2026-08-21, out-of-loop triage of a second-hand report ("run_locally machine_learning
with prediction mode ALL does not work").
**Related**: **ML-016** (bare `machine_learning`; fixed in PR #468 via
`resolve_ml_bare_target_modes` — the only mode validation in the script today). **ML-022**
(`maintenance:machine_learning` silent no-op — this closes one of its two halves, not its
headline). **INFRA-030** (a module that ran nothing leaves no summary line).

---

## Provenance and the core insight

The triggering invocation, `SAPPHIRE_PREDICTION_MODE=ALL bash apps/run_locally.sh machine_learning`,
is **correctly handled on trunk** — `resolve_ml_bare_target_modes` (`:517-524`) rejects it and
`exit 1`s before any module runs (verified live). The report is not that defect. But it is not an
invented mode either, and that is the point:

**`SAPPHIRE_PREDICTION_MODE` has no single valid domain. It is per-consumer:**

| Consumer | Accepted values | On an unaccepted value |
|---|---|---|
| `make_forecast.py:526` / `recalculate_nan_forecasts.py:163` | `PENTAD`, `DECAD` | raises `ValueError` |
| `linear_regression.py:646-647` | `PENTAD`, `DECAD`, `BOTH` | **silent** — both horizons disabled, no forecast written, reaches `sys.exit(0)` (`:1096`) |
| `postprocessing_maintenance.py:125` | `PENTAD`, `DECAD`, `BOTH` | logs + `sys.exit(1)` |
| `postprocessing_operational.py:245` | `PENTAD`, `DECAD`, `BOTH`, **`MONTHLY`**, **`ALL`** | logs + `sys.exit(1)` |
| `recalculate_skill_metrics.py:94-103` (`VALID_MODES`) | the above plus **`DAILY`**, **`QUARTERLY`**, **`SEASONAL`** | logs + `sys.exit(1)` |
| `validate_pipeline.py:1305-1306` (`resolve_horizons`) | `MODE_TO_HORIZONS` keys | **silent** — `.get(mode, ["pentad"])` falls back to pentad |
| `run_locally.sh` bare `machine_learning` (`:517`) | unset/empty, `PENTAD`, `DECAD`, `BOTH` | logs + `exit 1` |

(`hindcast_ML_models.py:154` reads a *different* variable, `SAPPHIRE_HINDCAST_MODE`, and is not
part of this issue.)

`ALL` is real and operationally used: `bin/yearly_skill_metrics_recalculation.sh:103` forwards an
operator-supplied `${SAPPHIRE_PREDICTION_MODE:-BOTH}` into a consumer that accepts it. An operator
who learns `ALL` there and carries it to `short-term` gets a green run that wrote no forecasts.

**The dangerous input is therefore not a typo — it is a mode that is valid somewhere else.** A
typo is usually caught (late and expensively, but caught); a valid-elsewhere mode is not.

## Defect

`validate_env` (`:1623`) is the universal pre-dispatch choke point: called for every target at
`:2085`, before the dry-run exit (`:2090-2092`) and before dispatch (`:2102`), with a failing
`errors` counter returning non-zero at `:1691`. It logs the prediction mode but never checks its
domain (`:1647`), and it does not validate `ML_MODE` for any outer-loop target either
(`main():2152` logs it, which reads as confirmation) — bare `machine_learning` is the one
exception, already owning its own validation via `resolve_ml_bare_target_modes` (PR #468, see
§ Related above).

### Failure A — `SAPPHIRE_PREDICTION_MODE` accepted, then silently dropped

Mechanism: a non-`BOTH` value that is not `PENTAD`/`DECAD` makes `should_skip_ml_for_mode`
(`:496-501`) filter ML out with one `INFO` line, and makes `linear_regression.py:646-647` disable
both horizons.

**Which targets actually end silent depends on whether a postprocessing step re-validates:**

| Target | `ALL` / `MONTHLY` | a typo (`PENTAAD`) |
|---|---|---|
| `linear_regression` (bare) | **silent, exit 0** | **silent, exit 0** |
| `maintenance:linear_regression` | **silent, exit 0** | **silent, exit 0** |
| `maintenance:machine_learning` | **silent, exit 0** | **silent, exit 0** |
| `short-term`, `all` | **silent, exit 0** — `postprocessing_operational.py:245` accepts `ALL`/`MONTHLY` | loud: PP exits 1, propagated at `:1391` |
| `maintenance` | loud: `postprocessing_maintenance.py:125` accepts neither | loud, same |
| `daily` | immune to Failure A (overwrites the mode) — but see Failure C | immune |

### Failure B — `ML_MODE` accepted, ML silently skipped

`should_skip_ml_for_mode` compares two strings and validates neither. With `ML_MODE=DEACD`,
`"PENTAD" != "DEACD"` and `"DECAD" != "DEACD"` are both true, so **every** mode is filtered
regardless of what the caller resolved.

`ML_MODE=DEACD bash apps/run_locally.sh daily` runs both horizons, skips ML at all four gate
positions (`:1558`, `:1574`) with an `INFO` line each, runs LR and postprocessing normally, and
exits 0 — **no ML forecasts, everything else green.** `daily` is immune to Failure A but not to
this. Same for `short-term` (`:1385`), `all`, `maintenance` (`:1514`),
`maintenance:machine_learning` (`:2155`).

### Failure C — `daily`'s restored mode silently narrows its own validation

`daily` overwrites the mode only for Phases 3-4, then restores the operator's original value at
`:1584`, before Phase 5 and `run_api_validation "daily"` (`:1606`). `run_api_validation` calls
`run_in_venv` (`:1283`), which injects `SAPPHIRE_PREDICTION_MODE` into the child (`:627`), and
`validate_pipeline.resolve_horizons` reads it (`:1305`) with a silent `.get(mode, ["pentad"])`
fallback (`:1306`). So `daily` under a stale/invalid mode validates **pentad only**, having run
both horizons.

Recorded here because it defeats the obvious "just exclude daily" rationale. **Fixing it is out of
scope** (see below) — but the exclusion of `daily` from Block 1 must not be justified by the false
claim that nothing downstream reads the variable.

## Why this survived PR #468

#468 fixed the bare `machine_learning` target, which has no outer mode loop and so needed its own
resolver; it validated both variables there (`:517-531`) and deliberately scoped itself to that
target. Every other target resolves its mode through a per-pipeline loop that was already
"working", so nothing forced a domain check onto them. The script's only mode validation ended up
on the one target hardest to reach by accident.

## Desired outcome

Fail loudly in `validate_env`, **only where a bad value is otherwise silent**. Where the
downstream Python already logs and `sys.exit(1)`, do not duplicate its domain in bash — that is a
drift hazard, not a safety net.

Two **separate** case blocks with deliberately different target lists.

### Block 1 — `SAPPHIRE_PREDICTION_MODE`, domain `PENTAD|DECAD|BOTH`

Applies to the targets that dispatch `linear_regression` or `machine_learning` — the two silent
consumers:

`short-term|all|maintenance|linear_regression|maintenance:linear_regression|maintenance:machine_learning`

Note `linear_regression` (bare) and `maintenance:linear_regression` are in the list; the first
draft omitted the bare target. `maintenance` is included even though it currently fails loudly at
its postprocessing step — failing at entry is cheaper than failing after a full preprocessing run,
and it keeps the rule "targets that dispatch LR or ML" honest.

**`maintenance:machine_learning` is gated on `should_skip_module machine_learning`; the other five
targets are not.** It is the only Block 1 target that dispatches ONLY `machine_learning`, never
`linear_regression`. `demo` and `uzhm` skip `machine_learning` entirely (`:211`), so for those orgs
`maintenance:machine_learning` already does nothing today regardless of
`SAPPHIRE_PREDICTION_MODE`; validating it ungated would newly reject a currently-harmless
invocation. The implementation therefore splits Block 1 into two `case` arms: the five LR-bearing
targets (`short-term`, `all`, `maintenance`, `linear_regression`, `maintenance:linear_regression`)
stay ungated, since they all still dispatch `linear_regression`, which is not org-skippable; the
`maintenance:machine_learning` arm only rejects an out-of-domain mode when
`! should_skip_module machine_learning`. For `demo`/`uzhm`,
`SAPPHIRE_PREDICTION_MODE=ALL bash apps/run_locally.sh maintenance:machine_learning` exits 0 and
invokes nothing — this is intentional, not a gap.

Deliberately **excluded**, each for a stated reason:

- `daily` — overwrites the variable in Phases 3-4. Adding it would newly reject a stale exported
  mode that works today. Its residual under-validation is **Failure C**, filed as out of scope,
  *not* "nothing reads it".
- `recalculate_skill_metrics` — consumer accepts **eight** modes and already exits 1. Duplicating
  that list in bash rejects `ALL`/`MONTHLY`/`SEASONAL` today and drifts tomorrow.
- `maintenance:postprocessing_forecasts` — `postprocessing_maintenance.py:125-130` already exits 1.
- bare `machine_learning` — `resolve_ml_bare_target_modes` owns it.

**Preserve the existing unset behaviour.** The current branch also serves as the "not set → WARN"
notice for targets now excluded from the domain check. Keep the existing `if [ -z ... ]`
WARN / `log OK` for the full current target list and apply the new domain check only to the
narrower list. Note the WARN's wording ("will default to PENTAD") is already inaccurate for the
bare `linear_regression` target, which defaults unset/empty to `BOTH`
(`linear_regression.py:634`) — do not make that worse; leave the message alone or correct it, but
do not extend the PENTAD claim to LR targets.

### Block 2 — `ML_MODE`, domain `PENTAD|DECAD|BOTH`

Applies to `daily|short-term|all|maintenance|maintenance:machine_learning` — `daily` included,
since it is vulnerable to Failure B.

**Gate on `! should_skip_module machine_learning`.** `demo` and `uzhm` skip `machine_learning`
entirely (`:211`), and `resolve_org` runs before `validate_env` (`:2067` before `:2085`), so
`ML_MODE` is irrelevant for those orgs today. An ungated Block 2 would reject runs that currently
work. Excludes `linear_regression`, `maintenance:linear_regression`,
`maintenance:postprocessing_forecasts` and `recalculate_skill_metrics` (never dispatch ML), and
bare `machine_learning` (resolver owns it).

`ML_MODE` is always non-empty — `:194` defaults it to `DECAD` — so there is no empty case.

### Both blocks

Increment `validate_env`'s existing `errors` counter rather than calling `exit`, so a run with two
bad variables reports both. Do not add a `log OK` for `ML_MODE`; `main():2152` already logs it.

## Interaction with ML-022

Closes **one** of ML-022's halves — its observation that `should_skip_ml_for_mode` validates no
domain, so `ML_MODE=JUNK` filters every mode identically.

Does **not** close its headline: `maintenance:machine_learning` with `SAPPHIRE_PREDICTION_MODE`
unset defaults to `PENTAD`, and `ML_MODE`'s default `DECAD` filters it out — a silent no-op built
from two **valid** values that no domain check can catch. ML-022 carries a scope note saying so.

## Out of scope

- **Failure C** (`daily`'s restored mode narrowing `validate_pipeline` to pentad). Real, verified,
  and a different fix — `run_api_validation` should resolve horizons from the target, not from a
  restored env var. File separately.
- **`linear_regression.py`'s silent path itself.** Note it is *not* a no-work path: it loads
  config (`:631`), does connection setup (`:649-670`), unconditionally loads and processes
  discharge data (`:765-769`, reaching `forecast_library.py:1314` even with empty site lists), and
  enters the date loop (`:816-826`) before reaching `sys.exit(0)`. So it burns real I/O and can
  fail earlier for unrelated reasons. Reachable from entry points other than `run_locally.sh`;
  fix at module level, separately.
- **`validate_pipeline.resolve_horizons`'s silent pentad fallback** (`:1305-1306`) — same family,
  separate fix.
- **`bin/locally_run_forecast_tools.sh:33-34`**, which forwards an operator-supplied mode
  unvalidated. The production cron path is *not* exposed: `bin/run_pentadal_forecasts.sh:68` and
  `bin/run_decadal_forecasts.sh:68` hardcode the mode; `bin/daily_ml_maintenance.sh:56` iterates a
  hardcoded `("PENTAD" "DECAD")` array.
- ML-022's unset-default no-op.
- Any change to `resolve_ml_bare_target_modes` or the bare `machine_learning` target.

## Acceptance criteria

1. **Block 1 rejections.** For each of `short-term`, `all`, `maintenance`, `linear_regression`,
   `maintenance:linear_regression`: both `SAPPHIRE_PREDICTION_MODE=ALL` and
   `SAPPHIRE_PREDICTION_MODE=PENTAAD` exit non-zero, name the variable and the offending value, and
   invoke no module. `maintenance:machine_learning` gets the same rejection for orgs that do not
   skip `machine_learning` — but for `demo`/`uzhm` (which skip `machine_learning` entirely, so this
   one Block 1 arm is gated on `should_skip_module machine_learning`) the same out-of-domain value
   must exit **0** and invoke nothing, since the target already no-ops for those orgs regardless of
   the mode. Both halves are tested explicitly.
2. **Block 2 rejections.** For each of `daily`, `short-term`, `all`, `maintenance`,
   `maintenance:machine_learning`: `ML_MODE=DEACD` exits non-zero, names `ML_MODE` and the value,
   and invokes no module.
3. **Regression guards — must still succeed.** Each maps to a live usage or a stated exclusion;
   these are the load-bearing half of the change.
   - `SAPPHIRE_PREDICTION_MODE=ALL bash apps/run_locally.sh recalculate_skill_metrics`, and the
     same for `MONTHLY` and `SEASONAL` — all in `VALID_MODES`. A global whitelist would break
     these; the first draft of this plan did.
   - `SAPPHIRE_PREDICTION_MODE=PENTAAD` on `daily` and on `long-term` still runs.
   - `ML_MODE=DEACD` on `long-term`, `recalculate_skill_metrics`,
     `maintenance:linear_regression`, and bare `linear_regression` still runs.
   - **`ML_MODE=DEACD` on `daily` still runs when `ORG=demo` and when `ORG=uzhm`** (ML is skipped
     for those orgs, so Block 2 must be gated). Both orgs tested explicitly.
   - `SAPPHIRE_PREDICTION_MODE` unset on `short-term` still WARNs and defaults to PENTAD.
   - `SAPPHIRE_PREDICTION_MODE` unset on `recalculate_skill_metrics` still WARNs — the existing
     notice must survive the narrowing.
   - `SAPPHIRE_PREDICTION_MODE` unset on bare `linear_regression` still runs and LR still resolves
     its own `BOTH` default.
4. **Both blocks fire under `--dry-run`** (validation precedes the dry-run exit at `:2090-2092`).
   Tested, not merely asserted in prose.
5. **Every existing test in `apps/pipeline/tests/test_run_locally_orchestration.py` passes
   unchanged.** The seven `ML_MODE` tests (`:885, 895, 908, 916, 932, 981, 1020`) all target bare
   `machine_learning`, which neither block touches. If any needs editing, **stop and escalate** —
   it means a block's target list is wrong.
6. New tests covering 1, 2, 3 and 4 in `apps/pipeline/tests/test_run_locally_orchestration.py`.
7. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero new skips.

## Review corrections

Recorded so the same ground is not re-litigated. The out-of-loop pass overturned:

1. **"`ALL` is not a supported mode."** False — it is valid for `postprocessing_operational.py`
   and `recalculate_skill_metrics.py`. A global whitelist would have broken a live invocation.
2. **"No Phase-5 consumer reads `SAPPHIRE_PREDICTION_MODE`, so `daily` is immune."** False —
   `run_api_validation` → `run_in_venv:627` → `validate_pipeline.py:1305`. Now Failure C.
3. **"An unrecognised mode makes `short-term` report success."** Only for `ALL`/`MONTHLY`; a typo
   is caught late by `postprocessing_operational.py:245` and propagated at `:1391`.

Also corrected: the bare `linear_regression` target was missing from Block 1; Block 2 needed the
`should_skip_module` gate for demo/uzhm; `hindcast_ML_models.py` reads `SAPPHIRE_HINDCAST_MODE`,
not `SAPPHIRE_PREDICTION_MODE`; `daily`'s restore is `:1584` (`:1583` is its comment); the dry-run
exit is `:2090-2092`.
