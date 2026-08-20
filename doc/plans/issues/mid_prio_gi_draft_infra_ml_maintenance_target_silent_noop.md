# ML-022: `maintenance:machine_learning` silently runs nothing under default env vars

**Status**: Draft (2026-08-20)
**Module**: `apps/run_locally.sh` (`maintenance:machine_learning` case branch and its mode loop)
**Priority**: Medium — a default-env invocation reports success having done no ML maintenance
work; not confirmed to have bitten a real deployment yet (unlike INFRA-037/ML-016).
**Labels**: `run_locally`, `machine_learning`, `silent-noop`
**Found**: 2026-08-20, filed as a follow-up while implementing INFRA-037. Deliberately **not**
fixed in that same patch, to keep INFRA-037 scoped to `daily`'s fail-fast behavior.
**Related**: **ML-016** (the bare `machine_learning` target had the same class of defect —
crashed on an empty `SAPPHIRE_PREDICTION_MODE`; fixed via `resolve_ml_bare_target_modes`, which
validates both `SAPPHIRE_PREDICTION_MODE` and `ML_MODE` against an explicit domain and errors on
an inconsistent explicit request). **INFRA-030** (skipped modules leave no summary line — same
family: a module that ran nothing reports as if it succeeded).

---

## Defect

`maintenance:machine_learning` (`apps/run_locally.sh`, case branch starting at the
`maintenance:machine_learning)` label) resolves a list of modes to run, then filters that list
through `should_skip_ml_for_mode` before calling `run_maintenance_machine_learning`. With
`SAPPHIRE_PREDICTION_MODE` unset:

```bash
local original_mode="${SAPPHIRE_PREDICTION_MODE:-}"
local modes_to_run=()
if [ "$original_mode" = "BOTH" ]; then
    modes_to_run=(PENTAD DECAD)
    ...
elif [ -n "$original_mode" ]; then
    modes_to_run=("$original_mode")
else
    modes_to_run=(PENTAD)
    log WARN "SAPPHIRE_PREDICTION_MODE not set, defaulting to PENTAD"
fi
for mode in "${modes_to_run[@]}"; do
    if should_skip_ml_for_mode "$mode"; then
        log INFO "Skipping machine_learning maintenance for ${mode} (ML_MODE=${ML_MODE})"
        continue
    fi
    export SAPPHIRE_PREDICTION_MODE="$mode"
    log INFO "Running ML maintenance for mode: ${mode}"
    run_maintenance_machine_learning || { exit_code=$?; break; }
done
export SAPPHIRE_PREDICTION_MODE="$original_mode"
```

`ML_MODE` defaults to `DECAD` (`ML_MODE="${ML_MODE:-DECAD}"`, module-level default). With
`SAPPHIRE_PREDICTION_MODE` unset, `modes_to_run=(PENTAD)` (the WARN-logged default). Then
`should_skip_ml_for_mode PENTAD`:

```bash
should_skip_ml_for_mode() {
    local current_mode="$1"
    [ "$ML_MODE" = "BOTH" ] && return 1
    [ "$current_mode" != "$ML_MODE" ]
}
```

`"PENTAD" != "DECAD"` is true, so the function returns 0 (true) — skip. The loop's only element
is filtered out, `run_maintenance_machine_learning` is never called, and the `for` loop simply
ends. The case branch falls through with no further statement, `exit_code` is left at whatever it
was before this branch ran (0, for a standalone invocation) — so the target reports success having
run no ML maintenance at all.

**No validation catches this.** `should_skip_ml_for_mode` compares two strings and returns a
boolean; it validates neither variable's domain. Passing `ML_MODE=JUNK` produces the identical
outcome for *either* mode: `"PENTAD" != "JUNK"` is true and `"DECAD" != "JUNK"` is true, so both
modes are filtered out regardless of which one `modes_to_run` resolved to — a typo in `ML_MODE`
silently empties the run the same way the unset-env default does.

## Why this is latent, not yet confirmed operationally

`daily` and `run_maintenance_pipeline` (the `maintenance` target's own internal implementation)
each run their own per-mode loop that sets `SAPPHIRE_PREDICTION_MODE` explicitly for every
iteration before calling `should_skip_ml_for_mode`, so those call sites never hit the "unset
defaults to PENTAD, then gets filtered by ML_MODE=DECAD" combination — see the memory note that
`ML_MODE=DECAD` does skip ML for PENTAD, but not on `daily`, which runs both horizons explicitly.
The standalone `maintenance:machine_learning` target is the one call site that can still resolve
`SAPPHIRE_PREDICTION_MODE` to the WARN-logged PENTAD default *and* then immediately filter that
single resolved mode away — an operator running it by hand, exactly as INFRA-037 documents
becoming necessary after a `daily` abort, is the realistic trigger.

## Desired outcome

Same shape as ML-016's fix for the bare `machine_learning` target: validate `SAPPHIRE_PREDICTION_MODE`
and `ML_MODE` against their explicit domains (unset/`PENTAD`/`DECAD`/`BOTH` for the former,
`PENTAD`/`DECAD`/`BOTH` for the latter) before resolving `modes_to_run`, and treat "the resolved
mode set is empty after filtering" as a loud condition — either an error (if the emptiness stems
from an inconsistent explicit request, mirroring `resolve_ml_bare_target_modes`'s
`SAPPHIRE_PREDICTION_MODE` vs `ML_MODE` mismatch check) or an explicit "nothing to do, this is
expected" INFO line distinguishable in the summary from "ran and passed" — not silence.

## Out of scope

- Fixing `should_skip_ml_for_mode` itself to validate its input — any fix belongs at the call
  site that resolves the mode (mirroring where ML-016's fix landed), not inside the shared
  predicate, which is also used correctly by call sites that already validate before calling it.
- `run_maintenance_pipeline`'s per-mode loop (`:1354-1368` in this session's working copy) — not
  shown to reach the same empty-modes-to-run state; only the standalone
  `maintenance:machine_learning` case branch resolves a *single* mode from an unset env var and
  then filters it.
- INFRA-030's general "skipped modules leave no summary line" reporting gap — this issue is about
  the specific unset-env-var + wrong-default interaction that empties the run, not about summary
  line formatting in general.

## Acceptance criteria

- [ ] With `SAPPHIRE_PREDICTION_MODE` unset and `ML_MODE` at its default (`DECAD`),
      `maintenance:machine_learning` either runs ML maintenance for `DECAD` (the mode `ML_MODE`
      actually selects) or fails loudly — it must not exit 0 having run nothing.
- [ ] `ML_MODE` set to a value outside `PENTAD`/`DECAD`/`BOTH` is rejected before the mode loop
      runs, not silently treated as "skip everything."
- [ ] A test pins the current confusing combination (`SAPPHIRE_PREDICTION_MODE` unset,
      `ML_MODE=DECAD`) no longer exits 0 with zero ML maintenance invocations.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected
      skips.
