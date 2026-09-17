# LTF-012: a model reports SUCCESS having produced no forecast

**Status**: Draft
**Module**: `apps/long_term_forecasting/run_forecast.py`, `apps/long_term_forecasting/lt_utils.py`,
`apps/long_term_forecasting/lt_recovery.py`
**Priority**: High
**Labels**: `ltf`, `data-integrity`, `null-forecast`, `execution-status`, `recovery`
**Found**: 2026-09-10, during the LT recovery runbook review. Reframed as the unfulfilled remainder
of archived **LTF-003** — see "Relationship to LTF-003" below, which this issue must not restate as
new territory.
**Related**: **LTF-003** (archived, Complete — the `flag=2` fix this issue builds on, not
contradicts), **LTF-011** (a different `run_recovery`/exit-code conflation, Draft), **LTF-013**
(the non-operational-mode gap in the same recovery entry point, filed separately because it is an
unrelated code path).

---

## Relationship to LTF-003 — read this before the rest

`doc/plans/issues/archive/high_prio_gi_draft_ltf_flag_zero_on_null.md` is **Complete**. Its defect
was that `run_forecast.py` set `flag=0` unconditionally after `predict_operational()` returned,
so an all-NaN model output was written to the database indistinguishable from a valid forecast.
Its accepted fix, per its own acceptance criteria, was:

- `flag=2` set (not `flag=0`) when the forecast is all-NaN
- `flag=0` set only when at least one Q value is non-NaN
- `success = False` set alongside `flag=2`, so `execution_is_success` propagates the failure
- `prepare_long_forecast_records()` does not append all-`None`-Q records
- `MC_ALD`'s dependency check correctly reads `flag=2` output as failure

**What shipped, verified at `89a6ffc7`:** the flag half. `apply_success_flag`
(`apps/long_term_forecasting/lt_recovery.py:547-570`) stamps every row whose main Q value is NaN
with `MISSING_VALUE_FLAG = 2` (`lt_recovery.py:101`) and every row with a value with
`OPERATIONAL_FLAG = 0` (`lt_recovery.py:99-102`), or `recovery_flag` when a recovery run supplies
one. **`flag=2` on an all-NaN row is the LTF-003 fix working correctly. It is not the defect in
this issue, and nothing here should be read as proposing to change it.**

**What did not ship:** the `success = False` half, in the one branch LTF-003's own fix targeted.
`run_single_model` (`apps/long_term_forecasting/run_forecast.py`, in the block starting after
`forecast = model_instance.predict_operational(today=today)`) reads today:

```python
main_q_col = f"Q_{model_name}"
if main_q_col not in forecast.columns:
    logger.error(...)
    forecast["flag"] = 2
    success = False
else:
    forecast = apply_success_flag(forecast, main_q_col, recovery_flag)
    success = True
```

`apply_success_flag` is called and correctly stamps `flag=2` on every NaN row — but `success = True`
is set unconditionally in that branch, regardless of what `apply_success_flag` just did. A model
whose `Q_<model_name>` column exists but is entirely NaN takes this branch, gets `flag=2` rows (the
LTF-003 fix, working), and returns `success = True` (the LTF-003 gap, still open). The **reported**
status was never brought into line with the flag it sits next to.

## Scope: precisely which branch this is

`success` (renamed `execution_is_success` at the caller) is set to `False` in several places in
this module. Only one of them is the defect:

| Site | Condition | `success` value | In scope here? |
|---|---|---|---|
| `run_single_model`, `main_q_col not in forecast.columns` | the model's own output frame is missing its expected column entirely | `False` | No — already correct |
| `run_single_model`, `today is None` (`check_valid_forecast_issue_date` returns `None`) | model not scheduled for today | `False` (function returns `False` directly — "skip is a failure") | No — already correct, and out of scope for this fix |
| `run_single_model`, `can_be_run == False` (a `SnowMapper`/`Discharge`/`EMCWF_Forecast` freshness or dependency check failed) | stale or missing input data | `False` | No — already correct |
| `run_single_model`, **the branch after `predict_operational` returns and `main_q_col` is present** | `apply_success_flag` may have flagged every row `MISSING_VALUE_FLAG = 2` | `True`, always | **Yes — this is the defect** |
| `run_forecast`, the `try/except` around `run_single_model` | an exception propagated out of the model run | `execution_is_success[model_name] = False` | No — already correct |

The fix must touch only the fourth row. It must not weaken the first three, and must not turn
`can_be_run == False` or the "not scheduled" skip into something other than a failure — both are
already correct today.

## Why this is not cosmetic: the boolean gates execution and is printed as status

`run_forecast` stores the same value keyed by model name:

```python
execution_is_success[model_name] = sucess  # apps/long_term_forecasting/run_forecast.py
```

and reads it back before every downstream model:

```python
deps_success = all(execution_is_success.get(dep, False) for dep in dependencies)
if not deps_success and not ignore_initial_dependencies:
    logger.error(f"Skipping model {model_name} due to failed dependencies: {dependencies}")
    execution_is_success[model_name] = False
    continue
```

So a dependent model — e.g. an `MC_ALD`-style UncertaintyMixture model, or any `SM_GBT_LR`-style
model that consumes another model's hindcast/forecast as a feature — sees an all-NaN upstream
model as having succeeded, and runs on NaN inputs rather than being skipped. This is the exact
cascading-failure mechanism LTF-003 named for `MC_ALD`; LTF-003's flag fix did not close it,
because `execution_is_success` reads `success`, not `flag`.

The same value is also what the end-of-run summary prints:

```python
for model_name, success in execution_is_success.items():
    status = "SUCCESS" if success else "FAILED"
    logger.info(f"{model_name}: {status}")
```

so an operator or log-scraping monitor reading `<model>: SUCCESS` for a model that wrote only
`flag=2` rows has no textual signal that anything is wrong. Both effects — the dependency gate and
the printed status — come from one boolean, which is why this cannot be fixed as a display-only
change.

## A second fix site: `save_forecast`'s return value is not evidence of persistence either

`run_single_model` also does not distinguish "the model produced a value" from "that value reached
the database". After the save call:

```python
save_success = save_forecast(
    forecast_df=forecast,
    model_name=model_name,
    ...
)

if not save_success:
    logger.warning(f"Forecast save had issues for model {model_name}")

# Return success
return success
```

`save_forecast` (`apps/long_term_forecasting/lt_utils.py:555-621`) returns
`success_db or success_csv` — true if *either* track wrote, by its own docstring contract ("`bool`:
True if at least one save operation succeeded"). A `save_forecast` failure (both DB and CSV writes
failing) is only logged as a warning; the function still `return`s the earlier `success` value from
the prediction branch, unchanged. So today, `success`/`execution_is_success` answers only "did
`predict_operational` produce a column with at least one non-NaN value", never "did any row reach
the database". A model can predict successfully, fail to save anywhere, and still report
`SUCCESS`, gate dependents open, and leave the DB with nothing new (rows unchanged from a prior
run, if any).

**This issue therefore names three things currently conflated by one boolean**, and requires a fix
to distinguish them:

1. **Prediction success** — `predict_operational` produced at least one finite `Q_<model_name>`
   value.
2. **Persistence success** — those rows (or a recovery-run's replacements) reached the database.
   `save_forecast`'s false-return contract (point above) is a second fix site, not a restatement
   of the first.
3. **The recovery's read-back criterion** — a deliberately partial, different definition of
   success, used only by `run_recovery`'s stage 3
   (`lt_recovery.py`, `count_member_rows(..., flags={RECOVERY_FLAG}, require_value=True)`): a
   recovery is reported `EXIT_OK` if even **one** row across all members and stations has
   `flag=RECOVERY_FLAG` and a usable value. This criterion is correct for what it does (a recovery
   is not required to achieve full station×model coverage to be worth keeping) and is **not** to be
   changed by this issue — see the acceptance requirement below.

## Acceptance criteria

- [ ] In `run_single_model`'s post-prediction branch, `success` reflects whether
      `apply_success_flag` actually found at least one non-NaN value in `main_q_col` — not merely
      that the column exists. An all-NaN `Q_<model_name>` column must yield `success = False` in
      this branch, matching the `flag=2` rows `apply_success_flag` already writes.
- [ ] An **empty** forecast frame (zero rows) reaching this branch is also treated as failure, not
      only an all-NaN one — do not leave a frame with no rows as an untested edge of the fix.
- [ ] The three already-correct `False`-setting sites (missing column, not-scheduled skip,
      `can_be_run == False`) are unchanged in behaviour.
- [ ] `save_forecast`'s false return is surfaced into the returned `success` value (or a separate
      persistence-success signal `run_forecast` also consults for `execution_is_success`) instead of
      being logged and discarded. Acceptance must include a case where prediction succeeds
      (non-NaN value) but persistence fails (both DB and CSV writes fail) — the reported status
      for that model must not be `SUCCESS`.
- [ ] On the normal `forecast_all=True` path in `run_forecast`, a dependent model is correctly
      skipped (`execution_is_success[dependent] = False`, "Skipping model ... due to failed
      dependencies" logged) when its upstream dependency produced an all-NaN or empty output. Cover
      both:
      - the full `forecast_all` run (`ignore_initial_dependencies = False`), and
      - an explicitly-selected-model run (`forecast_all=False`, `models_to_run` given), where
        `ignore_initial_dependencies = True` is set and dependencies are re-checked inside
        `run_single_model` itself — confirm the fixed `success` value still propagates correctly
        in that mode, since it takes a different path to the same `execution_is_success` dict.
      Also cover the **partial-output** case: some stations get a value, others NaN, for the same
      model — confirm this still reports `success = True` (partial output remains success, per
      LTF-003's own accepted decision that partial-NaN is not a failure) and is not accidentally
      swept into the "all-NaN" fix.
- [ ] The fix does not change `run_recovery`'s stage 3 read-back criterion
      (`count_member_rows(flags={RECOVERY_FLAG}, require_value=True)`, satisfied by one usable
      row). Recovery success stays deliberately partial; this issue must not be implemented as "a
      recovery now requires full coverage to report `EXIT_OK`". Add or update a test asserting the
      read-back threshold is unchanged.
- [ ] `apps/long_term_forecasting/test/` gains unit tests for: all-NaN output → `success = False`;
      empty-frame output → `success = False`; mixed NaN/non-NaN (partial) output → `success = True`;
      prediction success + persistence failure → not reported `SUCCESS`; a dependent model skipped
      when its dependency's output was all-NaN, on both the `forecast_all` and explicitly-selected
      paths.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting` passes with zero
      failures and zero unexpected skips.
- [ ] No changes to `sapphire/services/` (ownership boundary).

## Behaviour to record, not to fix in this issue: the pre-count guard counts `flag=2` rows

`run_recovery`'s stage-1 guard (`lt_recovery.py`, the `count_member_rows` call immediately before
"Guard passed: no member rows exist for...") is called with **no `flags=`** and **no
`require_value=`**:

```python
pre_count = count_member_rows(
    client,
    horizon_type=horizon_type,
    horizon_value=horizon_value,
    effective_date=effective_date,
    model_types=model_types,
    station_codes=station_codes,
)
if pre_count > 0:
    raise RecoveryRefused(...)
```

Because it passes no `flags` filter, it counts rows of **any** flag — including `MISSING_VALUE_FLAG
= 2` rows with `q IS NULL`. So once a recovery run writes all-`flag=2` rows for a month (which is
exactly what happens today, before this issue's fix, for a model whose recovery attempt also
produced all-NaN output — or even after the fix, for any model that genuinely cannot produce a
value on retry), a **second** recovery attempt for that same key is refused by the guard: `pre_count
> 0` because the `flag=2` skeleton rows are still there. The month is then locked until someone
deletes those rows by hand; the recovery runbook's own retry guidance ("check the database before
retrying") does not by itself unblock this, since the rows it finds are exactly what triggers the
refusal.

This is recorded here as behaviour to consider, not a defect this issue fixes, for two reasons a
future fix must respect:

- **Whether an all-NaN model should write `flag=2` rows to the database at all** is a design
  question distinct from the `success` reporting bug above, and changing it interacts with the
  guard above. A fix must **distinguish a wholly empty-valued recovery from a mixed-success one**:
  suppressing all-`flag=2` skeleton rows for a recovery that produced nothing at all is not the
  same change as allowing a **retry of individually failed members** inside an otherwise
  populated (mixed-success) recovery request, and neither should be implemented as if it were the
  other.
- **Any change here must not weaken the guard that protects rows already written.** The guard's
  purpose — refuse to silently overwrite a partially populated month — is correct and is not in
  question; the open question is only whether `flag=2`, no-value rows should count toward "already
  exist" the same way rows carrying a value do.

## Observation (illustrative, not proof — the defect above is established from source alone)

Deployment: kghm development database. Date: 2026-09-10. Query: the same call
`count_member_rows` uses — `client.read_long_term_forecasts(horizon_type="month",
horizon_value=0, start_date="2026-09-10", end_date="2026-09-10")` (equivalently
`GET /api/postprocessing/long-forecast/?horizon_type=month&horizon_value=0&start_date=2026-09-10&end_date=2026-09-10`),
filtered client-side to the deployment's configured station codes and grouped by `flag`.

That query returned 428 member rows for the key: 244 at `flag=1` carrying a value, 184 at `flag=2`
with `q` null. Three of the models holding all-`flag=2` rows for every station in that set are
models whose only `success`-setting branch on this code path is the post-prediction branch cited
above — i.e. this DB state is consistent with the code producing `success = True` on an all-NaN
`Q_<model>` column, which is the defect this issue files. The run's own printed
`<model>: SUCCESS` summary line for those models is not attached here as a separately preserved
log artifact; the SUCCESS-label pairing is established from the source code cited above, per the
evidence-discipline rule for same-run reporting claims, not from an independently reproduced log
excerpt. Station codes are omitted; no discharge values are reproduced beyond the aggregate counts
above.

## Out of scope

- Filling the NaN output itself (a data-availability/model problem, already out of scope per
  LTF-003).
- Changing `flag=2` semantics or the flag values themselves (already correct, per LTF-003;
  unrelated to this issue).
- `run_recovery`'s pre-count guard counting `flag=2` rows (recorded above as behaviour to weigh,
  not fixed here).
- `LTF-013`'s non-operational-mode gap in `run_recovery` — unrelated code path, filed separately.
- Backfilling or deleting existing `{flag: 2, q: None}` rows already in any database (operational
  cleanup, not a code fix).
