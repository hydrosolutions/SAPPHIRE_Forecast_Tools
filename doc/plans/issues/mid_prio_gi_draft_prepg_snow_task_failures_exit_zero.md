## `preprocessing_gateway` reports PASS with all six snow tasks errored (PREPG-009)

**Status**: Draft (2026-08-14)
**Module**: `apps/preprocessing_gateway` (`snow_data_operational.py`), surfaced via
`apps/run_locally.sh`
**Priority**: **Medium** — silent-success on a fully failed sub-task. Not data loss, but the
operator signal is wrong, and the companion validation defect (INFRA-026) means nothing
downstream catches it either.
**Labels**: `preprocessing_gateway`, `snow`, `error-handling`, `silent-success`
**Found**: 2026-08-14, local kghm review on `maxat_sapphire_2` @ `8e3fc1bc`.
**Related**: **INFRA-026** (validation passes on the resulting norm-only rows — the two
together turn a run that wrote no snow at all into an entirely green one). Same silent-success family
as PP-051 / PP-054 / LR-010.

> **Provenance correction (2026-08-16).** The checkout moved from `maxat_sapphire_2` to
> `fix_lr010_lr011_write_contract` at **2026-08-14 16:00** (git reflog), so every run from the
> full-history recalc onward executed on that branch (now `849c8736`), **not** on trunk as the
> line above states. That branch's diff vs trunk touches only
> `apps/linear_regression/linear_regression.py`, `apps/iEasyHydroForecast/forecast_library.py`,
> their tests, and docs — **none of the files this issue concerns** — so the finding holds
> identically on trunk. Recorded for accuracy of the audit trail, not because the conclusion changes.

---

## Observation

`snow_data_operational.py` logged **six ERROR pairs** — every HRU × variable combination
(SWE, HS, RoF for two HRUs) — then finished normally:

```
ERROR - Error getting snow data from Data Gateway for HRU <X>, SWE: Failed to get data from
        api/calculations/snow-operational/… : {"message": "Operational data for HRU <X> is not
        available for date 2026-08-09 00:00:00!", "success": false}
ERROR - Failed to get snow data for HRU <X>, SWE
… x6 …
INFO  - Snow data processing complete (6 tasks)
```

Result: `preprocessing_gateway completed in 1m 42s` → **PASS**, exit 0. Zero snow values
were written; the stored SWE series still ends at its previous non-null date.

The **upstream cause is not ours** — the SAPPHIRE Data Gateway reports operational snow data
unavailable from the requested date. That is a data-availability condition to report, not a
bug to fix here. The defect is that it is reported as success.

> **Recurrence, 2026-09-04 — what an implementer needs from it.** The condition recurred and is
> **still active**: a one-day upstream hole at 2026-09-01 that makes `get_operational` unusable for
> every deployment, because it is all-or-nothing over its range and a spin-up precondition blocks
> recent start dates, so no `start_date` works. **Two consequences for this fix.** (1) It is
> testable against a live failure today — 6/6 tasks error and the process still exits 0, re-verified
> the same day. (2) The condition is recurring, not a one-off (an earlier instance on 2026-08-09
> resolved by 08-17), so an exit contract must not assume "upstream has no data yet" is short-lived.
>
> **Probe trap if you re-measure:** `_call_api` raises a bare `ValueError` on *any* non-200
> (`client_base.py:59-60`), so recording exception types cannot separate "no data" from a 4xx/5xx —
> capture status codes. Same shape as PREPQ-014.
>
> Full evidence, the cross-product picture and the upstream escalation live in
> [`doc/prod/dg_data_gaps_report_2026-09.md`](../../prod/dg_data_gaps_report_2026-09.md); they are
> not restated here.

## Why "6 tasks complete" is the wrong summary

`Snow data processing complete (6 tasks)` counts tasks *attempted*, not *succeeded*. An
operator reading the tail of the log sees a completion line and a PASS. The six ERROR lines
are 15 lines above it and are not reflected in any exit code, summary, or downstream check.

## Proposed fix

1. Track succeeded/failed counts and emit a summary that states both
   (`Snow data processing complete: 0/6 succeeded`).
2. Exit non-zero when any task failed — **one aggregate status, not a graded code**
   (decided 2026-09-04; see the correction below for why graded codes were rejected).
3. No caller changes. `run_locally.sh` and `pipeline_docker.py` already surface a non-zero
   status correctly; nothing needs to learn a new code.

> **Why graded codes were rejected (2026-09-04, out-of-loop review).**
> No current caller can act on a graded code. `pipeline_docker.py:384-428` treats every non-zero
> status except `124` identically — retry, notify, raise, write no marker — and
> `run_locally.sh:1831-1904,2345-2350` reduces any recorded failure to a final exit 1. A
> "warn-level" code for *upstream has no data yet* would therefore still page and still fail
> Luigi, buying nothing over a plain non-zero. The original text proposing that distinct codes
> are "the important one" has been removed as unimplementable without a separate, justified
> orchestration change. **DECIDED (owner, 2026-09-04): single non-zero aggregate** — emit one
> non-zero status and name the failed stages and succeeded/failed counts in the log
> (`Snow data processing complete: 0/6 succeeded`). Luigi behaviour and the marker contract stay
> as they are. Changing caller semantics to distinguish partial from total failure is explicitly
> **out of scope**.

> **Scope of "succeeded/failed" (2026-09-04).** Define a task failure as the existing `False`
> return from the fetch/read/local-CSV path. `get_snow_data_operational()` currently returns
> `True` even when `write_snow_to_api()` returns `False`, when the consistency check fails, or
> after a caught `SapphireAPIError` (`snow_data_operational.py:381-400`). Counting API delivery
> as failure is a **larger** change that would alter what a green run means — out of scope here;
> file separately if wanted.

## Acceptance criteria

- A run in which all snow tasks fail does **not** report PASS.
- A partial failure is distinguishable from full success and from total failure **in the log**
  (named stages plus succeeded/failed counts) — **not** in the exit status, which is a single
  aggregate by decision. Do not add a criterion requiring distinct exit codes.
- Data outputs and the exit behaviour of a fully-successful run are unchanged — **except** the
  completion summary line, which necessarily changes (so "byte-identical" cannot be the bar).
- One **parameterised entry-point** test over three cases — all succeed, **partial failure**, all
  fail — asserting the **actual process status** (`SystemExit` / return code), not `main()`'s return
  value. Two traps this closes: a regression where 1 of 6 tasks fails but the process exits 0 would
  satisfy a total-failure-only test, and `if __name__ == "__main__": main()` currently **discards**
  whatever `main()` returns, so asserting `main() == 1` can pass while the process still exits 0.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.

## Contract not to break

- **Do not discard completed meteo output.** `Quantile_Mapping_OP.py` and
  `extend_era5_reanalysis.py` may have succeeded in the same invocation; their written output must
  stand. But — following the decided single-aggregate contract — the gateway task itself
  **intentionally fails and withholds its Luigi marker** when snow failed. That is the point of the
  fix, not a side effect: with a 6/6 snow outage, a run whose meteo half worked now goes red, and
  dependent tasks that do not consume snow can be blocked by the missing marker. Accept that
  deliberately, or reopen the contract — do not write a criterion that requires both.

## Incidental

DG `api_key` cleartext logging: **ACTIONED, do not re-file.** Related: PREPG-015 (shipped), PREPG-017, PREPG-014.
