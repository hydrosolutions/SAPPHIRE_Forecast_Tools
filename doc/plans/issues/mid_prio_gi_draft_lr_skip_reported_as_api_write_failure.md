## LR's "no data for forecast year" skip is indistinguishable from a genuine API write failure (LR-010)

**Status**: Draft (2026-07-23)
**Module**: `apps/iEasyHydroForecast/forecast_library.py` + `apps/linear_regression/linear_regression.py`
**Priority**: **Low–Medium** — see "Priority is conditional" below. Firmly established:
the diagnosis is misleading. Not yet established: whether exiting non-zero on missing
upstream input is itself wrong.
**Labels**: `linear_regression`, `error-classification`, `false-fail`, `maintenance`, `observability`
**Discovered**: 2026-07-23, local pipeline health review (taj, `maxat_sapphire_2` @ `16fb9a9b`).
**Replaces the archived tracking entry for LR-005 Issue B.** LR-005 (Archived, Low)
already identified this same guard, its early return, and that its message is
misleading, and explicitly noted Issue B "remains". This entry is **not a distinct root
cause** — it is the same guard with the previously-unrecognised downstream effect
(truthiness handling → CRITICAL "database behind CSV" → non-zero exit), plus the
log-wording fix LR-005 asked for. LR-005's NaN-vs-no-row distinction is carried
forward below. Reopening LR-005 instead of keeping this file is an acceptable
alternative — **decide before implementing; do not track both.**
**Related**: **LR-007** (Complete) — silent API write failures; this is the opposite
direction, over-reporting. **INFRA-022** — same class: correct behaviour reported as failure.

---

## Symptom

During `maintenance` on taj, `linear_regression (hindcast)` FAILED in both modes,
immediately after the run logged an iteration as successful:

```
Iteration for 2026-07-20 completed successfully.
[ERROR] linear_regression hindcast failed (exit 1) after 15s
```

The module log gives the sequence:

```
WARNING - Skipping LR decad write: no data for forecast year 2026
          (last_line date_max=2025-07-20 00:00:00). Daily discharge data may be missing.
ERROR   - CRITICAL: API write failed for write_linreg_decad_forecast_data on 2026-07-20.
          Data written to CSV only. API database is now behind CSV.
ERROR   - Pipeline completed but one or more API writes failed. ... Exiting with error.
```

## Root cause

`write_linreg_pentad_forecast_data` and `write_linreg_decad_forecast_data` use a bare
`return` (→ `None`) on their skip paths, but `return api_ok` (`True`/`False`) on the
real write path:

| Function | lines | skip: **empty input** (`if data.empty`) | skip: **no current-year row** | write path |
|----------|-------|------------------------------------------|-------------------------------|------------|
| `write_linreg_pentad_forecast_data` | 3853–4094 | `return` @ **3890** | `return` @ **3922** | `return api_ok` @ 4092 |
| `write_linreg_decad_forecast_data` | 4207–4422 | `return` @ **4244** | `return` @ **4271** | `return api_ok` @ 4420 |

The four skip paths are **two distinct conditions**, not one — both return `None` and
both are therefore misreported as API write failures:
**(a) empty input frame** (`if data.empty: return`), and
**(b) no grouped last row in the forecast year** (the guard analysed below).

Callers test truthiness, so `None` (skip) takes the same branch as `False` (genuine
failure) — `linear_regression.py:954` (pentad) and `:1066` (decad):

```python
if not fl.write_linreg_pentad_forecast_data(linreg_pentad, forecast_date=current_day):
    logger.error("CRITICAL: API write failed for %s on %s. Data written to CSV only. "
                 "API database is now behind CSV.", ...)
    api_write_failures = True
```

`api_write_failures` then drives `sys.exit(1)` at `linear_regression.py:1090–1095`.
Four skip paths are affected across the two writers.

### The guard's actual predicate (correcting LR-005's characterisation)

The guard does **not** inspect forecast values for NaN. It is:

```python
has_current_year = (last_line["date"].dt.year == fd.year).any()
```

i.e. **no grouped last row has a date whose year equals `forecast_date.year`**.
Consequences worth stating, because they bound what any fix may assume:

- Every forecast value can be NaN while dates are current-year → guard does **not** fire.
- A single current-year station makes `.any()` true → the whole batch passes even if
  other stations are stale.

So "missing upstream discharge" is a *likely* cause of the guard firing (and is what
the warning text asserts), not the only possible one.

## Observed reproduction

One before/after observation, not a controlled experiment — other variables were not
held constant, so this establishes coincidence plus a plausible mechanism, not
exclusivity:

| Run | Precondition | LR hindcast result |
|-----|--------------|--------------------|
| 17:49 | `maintenance:preprocessing_runoff` had FAILED (iEH HF tunnel down) | **FAIL (exit 1)**, both modes, with the CRITICAL "database behind CSV" message |
| 18:12 | tunnel restored; `maintenance:preprocessing_runoff` **PASS** (2m 23s) | **PASS**, both modes; guard silent; "Successfully wrote 17 LR forecast records" |

Run logs: `run_locally_20260723_174939.log` and the 18:12 maintenance logs, plus
`apps/linear_regression/logs/log`.

> An earlier draft of this issue also claimed "the pentad path had already written
> 5,979 rows successfully in the same run". **That was wrong and is withdrawn**:
> `linear_regression.py:943` logs `[linreg] pentad write rows=…` as a *pre-write
> diagnostic* (frame size), so it does not evidence a successful write.

## Why it matters

- **It misreports a data gap as data loss.** "API database is now behind CSV" asserts a
  divergence that did not occur — nothing was written CSV-only; the write was skipped.
  An operator would hunt a non-existent reconciliation problem.
- **It hides the actionable fact.** The real signal (upstream discharge missing) is a
  `WARNING` above a `CRITICAL` pointing elsewhere.
- **It fails the run**, which matters more or less depending on the policy question below.

### Priority is conditional

**Medium** is justified only if a legitimate, non-actionable skip should not abort the
run. If the project's position is that missing upstream input *should* fail LR, then
the defect is purely the misleading diagnosis and **Low** (LR-005's original rating) is
right. Resolve this before implementing.

Not verified: the claim that a non-zero LR exit aborts modules queued behind it under
`maintenance`. `run_maintenance_pipeline` continues or aborts depending on
`CONTINUE_ON_ERROR`; confirm the real operational invocation before citing blast radius.

## Proposed fix (to be planned)

1. Make the writers' outcome explicit instead of overloading falsey. **Four outcomes,
   because the skip paths are not one condition:**
   `WRITTEN` · `SKIPPED_EMPTY_INPUT` (`data.empty`) · `SKIPPED_NO_CURRENT_YEAR_ROW` ·
   `WRITE_FAILED`. Collapsing the two skips is acceptable **only** if the caller
   genuinely treats them identically — but they are diagnostically different (nothing
   to write vs. stale upstream data), so keep them distinct in the log even if the
   caller's branch is shared.
2. **Every caller must use explicit comparisons** (`result is WRITE_FAILED`,
   `result is SKIPPED`), never truthiness. An enum member or result object is truthy by
   default, so a leftover `if not result` would silently reclassify a real failure as
   success. Update docstrings and the existing boolean-contract tests.
   *Simpler alternative if callers only need failure-vs-not:* keep `False` exclusively
   for API failure, return `True` for a deliberate no-op, and convey the skip reason
   separately.
3. Treat the skip as a **data-gap warning** naming the missing input; do not set
   `api_write_failures`.
4. **Contract not to break:** the genuine-failure path must be untouched — API error →
   `_handle_api_write_error` → `api_ok = False` → CSV still written → `False` returned →
   CRITICAL + non-zero exit; and in `fail` mode the original exception must still
   propagate. Do not "fix" this by making callers ignore falsey returns.
5. Reword the CRITICAL message so it is emitted only on a real CSV/DB divergence
   (this is LR-005 Issue B's original ask).

## Acceptance criteria

- **Empty input** (`data.empty`): LR logs "nothing to write", does **not** claim CSV/DB
  divergence, and exits 0 (this is an unambiguous successful no-op).
- **No current-year grouped last row**: LR logs a data-gap warning naming the stale
  input, does **not** claim CSV/DB divergence, and exits per the policy decided above.
- Genuine API write failure (mocked client error, warn/ignore mode): CSV still written,
  failure result returned, CRITICAL logged, exit non-zero.
- `SAPPHIRE_API_FAILURE_MODE=fail`: the original exception still propagates.
- Empty `api_data` continues to behave as a successful no-op (existing behaviour). Note
  this is distinct from the `data.empty` early return above, which happens earlier.
- With data present, these invariants hold versus today: identical API payload,
  identical CSV rows/order/format, identical exit status. *(Replaces an earlier
  unverifiable "byte-identical" claim.)*
- Unit tests cover **all four outcomes** for **both** writers — i.e. each of the four
  skip paths (two per writer) plus the write and failure paths — and a `.any()` case
  where one current-year station coexists with stale ones. Placeholder station codes only.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression` green.

## Reproduction

```bash
# With no discharge rows whose year == the forecast year (e.g. iEH HF unreachable so
# maintenance:preprocessing_runoff cannot refresh):
ieasyhydroforecast_env_file_path=<env> SAPPHIRE_PREDICTION_MODE=DECAD \
  bash apps/run_locally.sh maintenance:linear_regression
# -> "Skipping LR decad write: no data for forecast year ..." then
#    "CRITICAL: API write failed ... API database is now behind CSV" and exit 1
```
