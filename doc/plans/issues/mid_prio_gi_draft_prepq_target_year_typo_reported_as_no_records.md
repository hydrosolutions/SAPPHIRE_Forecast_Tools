# PREPQ-021: a malformed target year is reported as "produced no records" and the run continues

**Status**: Draft (2026-09-22)
**Module**: `preprocessing_runoff`, `apps/run_locally.sh`
**Priority**: **Medium** — a misconfiguration is classified as a benign outcome; no data is lost,
but the operator is told the wrong thing and the run reports success
**Labels**: `bug`, `prepq`, `exit-codes`, `configuration`
**Found**: 2026-09-22, while checking an exit-code claim for INFRA-024

---

## Problem

`sync_long_horizon_hydrograph.py`'s argparse declares `--target-year` as `type=int`
(`_build_parser`, `:865-876`, the `type=int` at `:876`). **argparse exits 2 on a bad argument.**

`run_locally.sh` passes the operator's environment value straight into it (`:959-965`):

```bash
if run_in_venv preprocessing_runoff sync_long_horizon_hydrograph.py -- \
    --target-year "${RUNOFF_LONG_HORIZON_TARGET_YEAR}"; then
```

and then maps exit 2 to a **non-fatal** outcome (`:977-982`):

```bash
if [ $lt_rc -eq 2 ]; then
    log WARN "Long-horizon hydrograph sync produced no records; continuing maintenance"
```

So a typo'd `RUNOFF_LONG_HORIZON_TARGET_YEAR` — anything non-numeric — produces exit 2, which the
wrapper reports as "produced no records", records no `FAIL` row, and continues. The module's `rc`
stays 0.

## What this is and is not

**Not a silent failure.** argparse writes its own error to stderr and `run_in_venv` tees it
(`run_locally.sh:622-634`), so the real cause is in the log. The defect is the **classification**:
a configuration error is presented as a benign data outcome, with a misleading WARN over the top of
it and no row in the summary.

**Only non-integer values are caught this way.** An integer-valued typo — `2025` for `2026` — passes
argument validation, is a perfectly valid target year, and the run processes the wrong year and
exits 0. That is a quieter failure than this one and is not addressed here.

## Why exit 2 is overloaded

Exit 2 already carries two meanings on this path, which is what makes the collision possible:

- from the long-horizon sync: "produced no records; continuing maintenance", non-fatal
  (`run_locally.sh:977-978`)
- from the LT recovery: REFUSED, recorded as `FAIL (REFUSED)` (`:1416-1418`)

argparse's exit 2 is a third, and neither existing meaning fits it.

## Proposed fix — pick one, minimal

1. **Validate before invoking.** Check `RUNOFF_LONG_HORIZON_TARGET_YEAR` is a four-digit integer in
   `run_locally.sh` before the call, and fail the module with a clear message if not. Smallest
   change, keeps the exit-code mapping untouched.
2. **Distinguish the cause in the script.** Have the script validate the year itself and exit with a
   code that is not 2, leaving argparse's 2 unreachable in normal operation.

Option 1 is preferred: it does not renumber anything, and `run_locally.sh` already knows the value.

## Acceptance criteria

- A non-numeric `RUNOFF_LONG_HORIZON_TARGET_YEAR` produces an operator-visible configuration error,
  not "produced no records".
- The run records the failure rather than continuing as if the sub-step had merely found nothing.
- The genuine "no records" case at exit 2 keeps its current non-fatal behaviour.
- A test covering a malformed value; demonstrated to fail before the fix.

## Deliberately out of scope

The integer-valued typo (wrong-but-valid year). Worth its own issue if it matters — it needs a
plausibility check on the year, not argument validation.
