# INFRA-054: Local runs cover fewer horizons than production when the mode is unset

**Status**: Draft (2026-09-10)
**Module**: `apps/run_locally.sh` + `apps/pipeline/pipeline_docker.py`
**Priority**: Medium — this is a developer-experience / rehearsal-fidelity gap, not a
production defect: production's Luigi tasks never consult a local-only default, so no
deployment is affected. Against that: the only signal a developer gets is one WARN line
per invocation, the miss is a whole horizon's worth of coverage (not a partial gap), and
the mitigation is a one-line env var — cheap enough to justify fixing rather than only
documenting.
**Labels**: `infra`, `run_locally`, `dev-experience`, `mode-resolution`
**Found**: 2026-09-10, surfaced while filing INFRA-055 during review of PR #507 (`ML_MODE`
removal) — both issues were explicitly deferred there as out of scope.

---

## Defect

`apps/run_locally.sh`'s `run_short_term_pipeline` resolves an unset
`SAPPHIRE_PREDICTION_MODE` to `PENTAD` with a WARN:

```
1549    if [ "$original_mode" = "BOTH" ]; then
1550        modes_to_run=(PENTAD DECAD)
...
1554    else
1555        modes_to_run=(PENTAD)
1556        log WARN "SAPPHIRE_PREDICTION_MODE not set, defaulting to PENTAD"
1557    fi
```

`run_maintenance_pipeline` resolves the same variable the same way, independently
(`:1702-1710`, WARN at `:1709`). `run_all` (`:1676-1694`) delegates to
`run_short_term_pipeline` at `:1678` and inherits its resolution.

**Not specific to `machine_learning`.** Both per-mode loops dispatch three modules per
iteration, not one: `run_short_term_pipeline`'s loop (`:1570-1598`) calls
`run_machine_learning` (`:1577`), `run_linear_regression` (`:1596`), and
`run_postprocessing_forecasts` (`:1597`); `run_maintenance_pipeline`'s loop
(`:1721-1743`) calls the maintenance equivalents of the same three. Whichever horizon
the mode resolves to, all three run only for that horizon — `linear_regression` and
`postprocessing_forecasts` are exposed to this gap exactly as `machine_learning` is.

Production does not share this default. `RunWorkflow` in `apps/pipeline/pipeline_docker.py`
has `mode = luigi.Parameter(default="ALL")` (`:1599`), and for `ALL` (or any value other
than `"PENTAD"`/`"DECAD"`) its `requires()` builds **both** workflows unconditionally:

```
1618        else:  # ALL or default
1619            # Run both workflows
1620            return [
1621                RunPentadalWorkflow(...),
1622                RunDecadalWorkflow(...),
1623            ]
```

Each of `RunPentadalWorkflow`/`RunDecadalWorkflow` dispatches `LinearRegression`,
`RunMLModel` (per configured model), and `PostProcessingForecasts` for its own mode
(confirmed for `RunPentadalWorkflow` at `:1479-1495`) — so `ALL` runs the full
LR+ML+postprocessing set for both horizons. Production maintenance is the same:
`PostProcessingMaintenance.requires()` (`:1865-1875`) unconditionally appends
`LinRegMaintenance(prediction_mode="PENTAD")` **and** `LinRegMaintenance(prediction_mode="DECAD")`,
plus both modes' `MLMaintenance` when `RUN_ML_MODELS == "True"`. There is no
`SAPPHIRE_PREDICTION_MODE`-unset case in production at all — the Luigi parameters carry
their own defaults, set independently of any shell environment variable.

So a developer running `bash apps/run_locally.sh all` or `... short-term` (or
`maintenance`) with no mode set rehearses **half** of what production does for the same
target, and the WARN line is the only signal that happened.

## Pre-existing — not caused by PR #507

The PENTAD-default WARN in both loops predates PR #507 by roughly six months: it was
added in commit `dd01b9b5` (2026-03-11), confirmed via `git log -S'not set, defaulting to
PENTAD' -- apps/run_locally.sh`; PR #507 (`refactor_run_locally_drop_ml_mode`, merged
2026-09-09 as `dac4cbbb`/`779e20cc`) is far downstream of that.

Before PR #507, `machine_learning` had an *additional*, independent horizon filter on
top of this: `ML_MODE` defaulted to `DECAD`, and `should_skip_ml_for_mode` skipped any
loop iteration whose mode didn't match it — so under an unset `SAPPHIRE_PREDICTION_MODE`,
`machine_learning` in these loops actually ran for `DECAD` (via `ML_MODE`'s own default),
while `linear_regression`/`postprocessing_forecasts` ran for `PENTAD` (via the shared
default above) — two different modules disagreeing about which single horizon to run.
PR #507 deleted `ML_MODE`, `should_skip_ml_for_mode`, and the associated resolution logic
(per its commit message: *"the instrument used to rehearse production deliberately
rehearsed something production does not do"*), leaving `machine_learning` governed by the
same `SAPPHIRE_PREDICTION_MODE` default as the other two modules. That change fixed the
three-modules-disagreeing defect and, in doing so, made the shared
default-to-PENTAD-only gap described here the **only** remaining horizon divergence
between these local targets and production — which is how it surfaced during that
review. Nobody should read this issue as PR #507 introducing new risk; it removed one
divergence and left a pre-existing one newly visible.

## `daily` is the exception — and a usable workaround

`run_daily_pipeline` does not consult `SAPPHIRE_PREDICTION_MODE` for its own dispatch at
all. Its forecast phase hard-codes `for mode in PENTAD DECAD` (`:1780`) and its
maintenance phase does the same (`:1805`); the ambient variable is only captured
(`:1759`) and restored (`:1830`) around those loops, for
`emit_continue_on_error_hint`'s retry command — never read for horizon selection. So a
developer who runs `bash apps/run_locally.sh daily` gets production-equivalent horizon
coverage regardless of whether `SAPPHIRE_PREDICTION_MODE` is set, and can use `daily` as
a workaround for this gap on the other targets today.

## Existing precedent: refuse rather than assume

Since PR #507, the bare `machine_learning` target no longer defaults at all:
`resolve_ml_bare_target_modes` (`:524-549`) errors and exits 1 when
`SAPPHIRE_PREDICTION_MODE` is unset (`:544-547`, message at `:545`), rather than picking
a horizon. The owner has already chosen "refuse rather than assume" for that one target;
the same question — default to both, keep the current PENTAD default, or refuse — applies
to `run_short_term_pipeline`, `run_maintenance_pipeline`, and (transitively) `run_all`.

## Out of scope

- Which resolution the aggregate targets should adopt — that is the open question below,
  for the owner to decide, not resolved here.
- `run_daily_pipeline`'s own dispatch — already production-equivalent; not touched.
- `machine_learning`'s per-target resolution (`resolve_ml_bare_target_modes`) — already
  fixed by PR #507; referenced only as precedent.

## Open question for the owner (not resolved here)

Should `run_short_term_pipeline`/`run_maintenance_pipeline` (and transitively `run_all`)
default an unset `SAPPHIRE_PREDICTION_MODE` to:
1. both horizons (production-equivalent coverage; roughly doubles local run time), or
2. keep the current PENTAD-only default with its WARN (cheaper, but rehearses half of
   what production does), or
3. refuse and require an explicit mode, matching the bare `machine_learning` target's
   precedent?

## Acceptance criteria

- [ ] The owner's chosen resolution (above) is implemented consistently across
      `run_short_term_pipeline` and `run_maintenance_pipeline`.
- [ ] Whatever is chosen is documented in `run_locally.sh`'s own header comment block
      (`:94-111`), which already documents the `daily`/bare-`machine_learning` divergence
      and should be extended rather than left inconsistent with the new behavior.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero
      unexpected skips.

---

## Related

| ID | Relation |
|---|---|
| INFRA-055 | Found in the same review pass; a distinct defect — aggregate `daily` validation checks only one horizon after running both, rather than this issue's "local runs cover fewer horizons than production" |
| ML-022 | Superseded by PR #507; was one symptom of the now-removed `ML_MODE` divergence this issue's "Pre-existing" section describes |
