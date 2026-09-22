# ML-027: hindcast rows are written to the API unfiltered and can overwrite operational forecasts

**Status**: Draft (2026-09-22, revised after out-of-loop review)
**Module**: `machine_learning`
**Priority**: **High** — can replace operational forecasts in the database the dashboard reads, with
no error raised
**Labels**: `bug`, `ml`, `api`, `data-integrity`
**Found**: 2026-09-22, while reviewing a plan for the ML CSV overwrite (GH #515) and unordered
pagination (GH #516)
**Related**: **ML-013** (protected the *parent* write against this same defect), ML-002 (other
failure paths in the hindcast script)

---

## Problem

Hindcast rows reach the API as a complete, unfiltered frame, and the service upserts, so a hindcast
row for a key that already holds an operational forecast **replaces that row's quantiles and flag**.

- `crud.create_forecast` is an upsert — "Create or update … based on horizon_type, code, model_type,
  date, target" — and applies every incoming field with `setattr`
  (`sapphire/services/postprocessing/app/crud.py:18-56`).
- The unique constraint is `(horizon_type, code, model_type, date, target)`; **`flag` is not in the
  key** (`models.py:140-146`).
- Hindcast sets `flag=3` when *any* quantile is null (`hindcast_ML_models.py:470-478`), so a failed
  hindcast can null out a good operational forecast.

**Scope of the claim:** this is not "every maintenance run". It requires a run that actually
launches hindcast — `recalculate_nan_forecasts` returns early with no flag=1/2 candidates
(`:283-295`), `fill_ml_gaps` only runs when gaps exist (`:304-322`) — plus API availability,
successful generation and delivery, and a key collision. When those hold, the overwrite happens and
nothing reports it as data loss.

### Why one write site is not the whole story

The subprocess write (`hindcast_ML_models.py:502`) is the most visible path, but the same unfiltered
frame is also the subprocess's **return channel**, and the parents write it again:

| write site | what it writes |
|---|---|
| `hindcast_ML_models.py:502` | the complete hindcast frame |
| `initialize_ml_tool.py:168`, `:189` | the returned frame again, both horizons |
| `fill_ml_gaps.py:381` | masked gap rows, **including inclusive interval endpoints** (`:278-283`) |
| `add_new_station.py:304`, `:345` | the new station's hindcast rows, both horizons |

So a left endpoint that already holds a `flag=0` row can be overwritten by the parent *after* the
subprocess correctly skipped it. A fix confined to the subprocess does not hold.

### Relationship to ML-013

ML-013 (`e7b32475`, 2026-03-20) narrowed the parent write in `recalculate_nan_forecasts.py` to
`replaced_rows` (`:430-434`) for exactly this reason. That commit did touch
`hindcast_ML_models.py` — its failure logging — but **the hindcast payload itself stayed
unfiltered**, and ML-013's file does not discuss it.

## Why nobody noticed

`apps/machine_learning/test/test_recalculate_nan_api_write.py:688-707` asserts the selective-write
contract while **mocking `call_hindcast_script`**. The mock removes the write that breaks the
contract, so the test passes while the property it names is violated end to end.

## Proposed fix — one guard at the shared boundary

**Guard `_write_ml_forecast_to_api` (`scr/utils_ml_forecast.py:713`): an incoming *hindcast* row must
not overwrite a key that already holds a `flag=0` operational forecast.**

This is smaller than per-site filtering and strictly more complete — every path above goes through
this one function, so subprocess and parents are covered together. Operational writes stay unchanged.

Three things the implementation must get right:

1. **Fail closed.** The protection read must be a *successful, complete* read. `_read_ml_forecasts_from_api`
   returns an empty DataFrame for no-records, readiness failure **and** exceptions alike
   (`:628-629`, `:645-679`), so naively reusing it would permit the overwrite after a transient read
   failure. If protection cannot be established, do not write.
2. **Match the serialised key.** `horizon_type` is always `day`; model names go through
   `ML_MODEL_TYPE_MAP`; codes are `str(int(code))` (`:817-823`); CSV `forecast_date`/`date`
   correspond to API `date`/`target` (`:692-704`).
3. **Bound the guarantee.** This is read-then-write, so it holds **absent a concurrent operational
   writer**. An operational write landing between the protection read and the upsert can still be
   overwritten. State the assumption; do not claim an unconditional guarantee.

**Narrow goal, stated so scope does not creep:** protect operational `flag=0` rows. This deliberately
still allows a `flag=3` hindcast to replace a `flag=4` hindcast, and still writes unrelated new keys —
both broader than ML-013's parent-payload contract. Reproducing ML-013's exact selectivity, or
protecting `flag=4`, is additional scope and not proposed here.

**Not proposed, and why** — recorded so they are not re-derived:
- *Delete the subprocess write.* `hindcast_ML_models.py:511-515` warns that without it, hindcast rows
  may not be visible to `fill_ml_gaps` next run. That is a real consideration but not decisive: the
  parents also persist results. It is set aside because it would leave the other write sites
  unprotected, not because it is unworkable.
- *Change the service-side upsert.* `sapphire/services/` is colleague-owned (CLAUDE.md), and this is
  a client-side mistake.

## Acceptance criteria

- A hindcast row does not replace an existing `flag=0` row for the same serialised key, via **any**
  of the four write sites above — absent a concurrent operational writer.
- A hindcast row still lands where no row exists or the existing row is not `flag=0`, so
  `fill_ml_gaps` continues to see it and does not re-detect the same gap.
- If the protection read fails or is incomplete, no write occurs.
- A regression test **demonstrated to fail against current code**: seed an operational `flag=0` row,
  run a hindcast covering that exact serialised key with differing values, assert the operational
  values survive. Use a deterministic generated frame and a stateful API stub — no model run or
  deployed service needed. Cover the parent re-write paths, not only the subprocess.
- The existing selective-write test stops mocking away the subprocess write, or gains a companion
  that exercises it — otherwise this gap reopens.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning` green.

## What to inspect

1. `scr/utils_ml_forecast.py:713-850` — the shared write boundary and its key serialisation.
2. `hindcast_ML_models.py:493-527`; `initialize_ml_tool.py:156-189`; `fill_ml_gaps.py:278-283`,
   `:347-381`; `add_new_station.py:304`, `:345` — the four write sites.
3. `crud.create_forecast` (`crud.py:18-56`) and the constraint (`models.py:140-146`).
4. `test_recalculate_nan_api_write.py:688-707` — the mock that hides it.
5. `archive/review_gi_draft_ml_recalc_api_overwrite.md` — ML-013's pattern.

## Deliberately out of scope

- The CSV writes alongside these API writes. Those belong to the separate CSV-overwrite work (GH
  #515); that allocation is a decision recorded here, not a fact verified from this repository.
- Any change to `sapphire/services/`. Note one consequence: the guard is a boundary for
  `apps/machine_learning`, not for the repository. `ForecastDataMigrator.send_batch`
  (`sapphire/services/postprocessing/app/data_migrator.py:359-377`) can upload hindcast rows from
  CSV directly, bypassing it. That is a service-side migration path, out of scope here, and does not
  weaken the guard for the write sites listed above.
- The other hindcast failure paths — ML-002.
