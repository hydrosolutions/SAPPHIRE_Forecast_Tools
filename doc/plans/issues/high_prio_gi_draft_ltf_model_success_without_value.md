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
contradicts), **LTF-011** (a different `run_recovery`/exit-code conflation; status Review, fixed in
PR #493), **LTF-013**
(the non-operational-mode gap in the same recovery entry point, filed separately because it is an
unrelated code path).

---

## Relationship to LTF-003 — read this before the rest

`doc/plans/issues/archive/high_prio_gi_draft_ltf_flag_zero_on_null.md` is recorded **Complete** in
`doc/plans/module_issues.md` — the archived file's own header still says **Draft**, stale from
before the tracker was updated at close; an implementer opening that file directly should not read
its header as current status. Its defect was that `run_forecast.py` set `flag=0` unconditionally
after `predict_operational()` returned,
so an all-NaN model output was written to the database indistinguishable from a valid forecast.
Its accepted fix, per its own acceptance criteria, was:

- `flag=2` set (not `flag=0`) when the forecast is all-NaN
- `flag=0` set only when at least one Q value is non-NaN
- `success = False` set alongside `flag=2`, so `execution_is_success` propagates the failure
- `prepare_long_forecast_records()` does not append all-`None`-Q records
- `MC_ALD`'s dependency check correctly reads `flag=2` output as failure

**What shipped, verified at `89a6ffc7` and still accurate at the branch's current base `4fe3e545`
(the cited source is unchanged between the two commits):** the flag half. `apply_success_flag`
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
the prediction branch, unchanged. So today, `success`/`execution_is_success` answers only "does
`forecast` have a `Q_<model_name>` column at all" — not whether that column contains any finite
value, which is the fourth-row defect established above — and never "did any row reach the
database". A model can predict successfully, fail to save anywhere, and still report
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

- [ ] In `run_single_model`'s post-prediction branch, `success` reflects whether `main_q_col`
      contains at least one **finite** value — the same `math.isfinite` contract as
      `lt_recovery._is_usable_value`, not `apply_success_flag`'s `isna()`-based flag assignment.
      The two checks are deliberately different and must stay that way: `flag=2` stays NaN-only
      (see "Leave flag semantics ... unchanged" below); only the `success` value gets the stricter,
      finite-only test. Merely checking that the column exists is not enough. An all-NaN
      `Q_<model_name>` column must yield `success = False`, matching the `flag=2` rows
      `apply_success_flag` already writes. An **infinity-only** `Q_<model_name>` column (every
      value `+inf`/`-inf`, no NaN) must also yield `success = False` — `apply_success_flag`'s
      `isna()` check does not catch infinities and would stamp those rows `flag=OPERATIONAL_FLAG`
      (or `recovery_flag`) rather than `flag=2`, so an implementation that reuses
      `notna().any()` for the success check would pass a NaN-only test suite while still reporting
      `SUCCESS` for a forecast with no usable value.
- [ ] An **empty** forecast frame (zero rows) reaching this branch is also treated as failure, not
      only an all-NaN one — do not leave a frame with no rows as an untested edge of the fix.
- [ ] The three already-correct `False`-setting sites (missing column, not-scheduled skip,
      `can_be_run == False`) are unchanged in behaviour.
- [ ] Persistence success must be scoped to this issue's own definition — "those rows ... reached
      the database" (above), i.e. the database write specifically — not to `save_forecast`'s
      `success_db or success_csv` return, which is `True` whenever the CSV write alone succeeds
      even if the DB write fails. **Consulting `success_db` directly is not sufficient on its
      own**: `save_forecast_to_db` (`lt_utils.py:385-462`) returns `True` when
      `prepare_long_forecast_records` produces zero records (`if not records: ... return True`,
      `lt_utils.py:429-431`), and `prepare_long_forecast_records` silently skips any row with a
      missing `date`, `valid_from`, or `valid_to` (`lt_utils.py:339-345`). The row count
      `write_long_forecasts` returns is logged but never checked against what was submitted
      (`lt_utils.py:435-439`). So a non-empty frame of finite predictions can produce **zero**
      submitted records and `save_forecast_to_db` still returns `True`. The fix must add a
      database-persistence signal that establishes a **non-empty, successful write** — e.g.
      `save_forecast_to_db` (or its caller) treating a zero-record conversion as failure before
      calling the API, and/or checking the returned row count against the records submitted — not
      merely `success_db`'s current `True`/`False` return, and not `run_single_model` consulting
      `success_db` as-is today. Acceptance must include three persistence-failure cases: prediction
      succeeds but both DB and CSV writes fail; prediction succeeds but only the DB write fails
      while the CSV write succeeds; and prediction succeeds with a finite `Q_<model_name>` value
      but every row is discarded during conversion (e.g. missing `date`, `valid_from`, or
      `valid_to`), leaving zero records to submit — in all three cases the reported status for that
      model must not be `SUCCESS`.
- [ ] On the normal `forecast_all=True` path in `run_forecast`, a dependent model is correctly
      skipped (`execution_is_success[dependent] = False`, "Skipping model ... due to failed
      dependencies" logged) when its upstream dependency produced an all-NaN or empty output. Cover
      both:
      - the full `forecast_all` run (`ignore_initial_dependencies = False`), where the
        `deps_success` gate in `run_forecast` is real and already correctly skips a dependent
        whose upstream failed.
      - **the explicitly-selected-model path is NOT covered by this fix, and this issue must say
        so rather than claim it verifies dependency propagation there.** `run_forecast` sets
        `ignore_initial_dependencies = True` for this path with the comment "we check dependencies
        again in the run_single_model function" (`run_forecast.py:486`) — **that comment is
        wrong**. The dependency loop it refers to (`run_single_model`, `for dep in
        model_dependencies.get(model_name, [])`, `run_forecast.py:248-259`) only calls
        `os.path.exists()` on each dependency's forecast/hindcast CSV path; on a missing file it
        calls `logger.error(...)` and then **appends the path anyway** — it never sets
        `can_be_run = False`, never returns early, and never reads `execution_is_success` or the
        upstream `success`/flag value. So on this path a dependent model runs regardless of
        whether its upstream dependency actually succeeded; only a missing *file* is logged, not a
        failed *dependency*. Fixing the post-prediction `success` value (this issue's scope) does
        not close this path, because nothing on this path reads that value. A genuine
        dependency-success check on the explicit-selection path — e.g. having `run_single_model`
        consult `execution_is_success` for each dependency the way `run_forecast`'s own loop does
        — is **larger than this issue's stated scope**; this issue explicitly does not require it.
        Either implement it as a separate, scoped follow-up or record it here as out of scope —
        do not fold it into this issue's acceptance.
      - **Secondary observation, same code path:** because the check is existence-only, a
        **stale** `<model>_forecast.csv`/`<model>_hindcast.csv` left over from an earlier
        (possibly failed) run satisfies it just as well as a fresh one. A dependent model on the
        explicit-selection path can silently consume stale upstream data with nothing more than a
        `logger.error` line to show for it — no exception, no skip, no non-`SUCCESS` status.
      - The misleading source comment at `run_forecast.py:486` should be corrected (to state what
        the loop actually does — a file-existence check, not a dependency-success check) as part
        of whatever fix addresses this.
      Also cover the **partial-output** case: some stations get a value, others NaN, for the same
      model — confirm this still reports `success = True` (partial output remains success, per
      LTF-003's own accepted decision that partial-NaN is not a failure) and is not accidentally
      swept into the "all-NaN" fix. Cover the same partial shape when the unusable rows are
      **infinite** rather than NaN: some stations finite, others `+inf`/`-inf` — this
      finite-plus-non-finite mix must also report `success = True`, using the same finite-value
      criterion as the all-or-nothing cases above.
- [ ] The fix does not change `run_recovery`'s stage 3 read-back criterion
      (`count_member_rows(flags={RECOVERY_FLAG}, require_value=True)`, satisfied by one usable
      row). Recovery success stays deliberately partial; this issue must not be implemented as "a
      recovery now requires full coverage to report `EXIT_OK`". Add or update a test asserting the
      read-back threshold is unchanged.
- [ ] `apps/long_term_forecasting/test/` gains unit tests for: all-NaN output → `success = False`;
      empty-frame output → `success = False`; **infinity-only** output (every value `+inf`/`-inf`,
      no NaN) → `success = False`; mixed NaN/non-NaN (partial) output → `success = True`; mixed
      finite/non-finite (partial, a `+inf`/`-inf` value alongside a finite one) output →
      `success = True`; prediction success + persistence failure (both tracks failing, DB-only
      failing with CSV succeeding, and finite predictions whose records are all discarded during
      conversion due to missing `date`/`valid_from`/`valid_to`) → not reported `SUCCESS`; a
      dependent model skipped when its dependency's output was all-NaN, on the `forecast_all` path
      only (the real `deps_success` gate). Do **not** add a skipping test for the
      explicitly-selected-model path — this fix does not cover it (see the acceptance bullet
      above); a genuine dependency-success check there is separate, larger work.
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
