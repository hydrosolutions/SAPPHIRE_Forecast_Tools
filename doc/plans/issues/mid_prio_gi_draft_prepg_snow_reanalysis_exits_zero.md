## `snow_data_renalysis.py` always exits 0 — PREPG-009 was never applied to it (PREPG-027)

**Status**: Draft (2026-09-08)
**Module**: `apps/preprocessing_gateway` (`snow_data_renalysis.py`)
**Priority**: **Medium** — it is a maintenance/backfill script, not the daily operational run, so a
silent failure does not corrupt the operational path. But it is the script an operator runs
*precisely when snow data is already known to be wrong*, and it cannot report that it failed.
**Labels**: `preprocessing_gateway`, `snow`, `silent-success`, `error-handling`
**Found**: 2026-09-08, during the out-of-loop plan review of **PREPG-026**. Verified at trunk, not
inferred.
**Related**: **PREPG-009** (the identical defect, fixed in the operational script, PR #491),
**PREPG-026** (the API-delivery half, fixed in the operational script, PR #500). This issue is the
reanalysis script's share of both.

---

## Problem

`snow_data_renalysis.py` is the operational script as it stood *before* PREPG-009 and PREPG-026.
Two defects, both verified:

**1. No aggregate exit status (the PREPG-009 defect, verbatim).**

- `main()` calls `get_snow_data_reanalysis` per HRU/variable and logs a failure (`:499`), but builds
  no failure list.
- It ends with `logger.info("Snow reanalysis processing complete (%d tasks)", total)` (`:501`) —
  counting tasks **attempted**, which is the exact wording PREPG-009 identified as the defect.
- `main()` returns nothing, and the entry point is a bare `main()` with **no `sys.exit()`**.

So the process exits 0 whatever happens. Every task can fail and the run reports success.

**2. A failed API delivery is discarded (the PREPG-026 defect).**

At `:366-387`: the `write_snow_to_api` return is assigned to `written` and only gates the
consistency check; the consistency result is discarded; and a broad `except Exception` logs and
continues. The function then returns `True`.

That broad catch is also why PREPG-026 did not change this script's behaviour: it absorbs the new
`SapphireAPIError` from the readiness check and still returns `True`.

## Why it matters despite being maintenance-only

This is the recovery tool. When PREPG-025's fallback cannot cover a window, or a gap needs
backfilling, this is what an operator runs — and it will report success having written nothing to
the API. The failure mode is "the fix silently did not work", discovered later by the same absence
that prompted the run.

## The fix — mirror what already shipped, do not redesign

PREPG-009 and PREPG-026 are both merged; copy their shape rather than inventing a third.

1. **Aggregate exit status**, exactly as `snow_data_operational.py:850-890`: collect failed
   `HRU/variable` pairs, log succeeded-vs-attempted, `return 1` when any failed, and call
   `sys.exit(main())`.
2. **Fail the task on a genuine delivery failure**, exactly as `snow_data_operational.py:773-779`:
   narrow the broad `except Exception` to `except SapphireAPIError`, log, and `return False`.

## Contract not to break

- **Do not change the maintenance sync window.** This script calls `write_snow_to_api` with
  `mode="maintenance"` and `reference_date=df_combined["date"].max()` — deliberate, so the window
  is relative to the data rather than the wall clock. Leave both.
- **`SnowPreservationReadError` must keep propagating uncaught** (PREPG-020) — it is caught and
  re-raised at `:378-383` and that block stays.
- **Narrowing the broad `except Exception` is a behaviour change beyond the API case**: anything
  else it currently swallows will start propagating. Check what else can raise in that block before
  narrowing, and keep the failure a falsey task result rather than an escaping exception — this
  script's `main()` has no exception boundary either.
- **The benign no-write cases must stay benign**: `SAPPHIRE_API_ENABLED=false`, an absent client,
  empty input and an empty maintenance window must all still exit 0. PREPG-026's review found this
  is the easy thing to get wrong — `write_snow_to_api` returns `False` for eight conditions and only
  the readiness failure raises.

## Acceptance criteria

- With every task failing, the process exits non-zero and the completion line reports
  succeeded-vs-attempted, not tasks attempted.
- With the API unreachable and `SAPPHIRE_API_ENABLED=true`, the run exits non-zero.
- With `SAPPHIRE_API_ENABLED=false`, the run exits 0 and writes CSV exactly as today.
- One task's failure does not prevent the others from running.
- A genuine `SnowPreservationReadError` still propagates uncaught.
- Tests use a client whose `read_snow` **raises** where an unreachable API would raise — a mock that
  returns a `MagicMock` hid exactly this class of bug for a whole review round in PREPG-026.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.
