## Snow ingestion cannot tolerate a single missing upstream day (PREPG-025)

**Status**: Draft (2026-09-04)
**Module**: `apps/preprocessing_gateway` (`snow_data_operational.py`)
**Priority**: **Medium** — recurring, and each occurrence stops snow ingestion on every deployment
until the gateway backfills the day.
**Labels**: `preprocessing_gateway`, `snow`, `robustness`
**Found**: 2026-09-04, while diagnosing the 2026-09-01 gap.
**Related**: **PREPG-009** (the run reports success while fetching nothing — fix that first, it is
what makes this visible). Escalation for the underlying gap:
[`doc/prod/dg_data_gaps_report_2026-09.md`](../../prod/dg_data_gaps_report_2026-09.md).

---

## Problem

`get_operational` takes only a `start_date` and returns all-or-nothing from there to the forecast
horizon, so **one missing interior day voids the entire response**. The window cannot be shortened
past the hole either, because a spin-up precondition rejects recent start dates. Measured on
kghm/SWE while 2026-09-01 was absent:

```
start_date <= 2026-08-28   ->  "Operational data for HRU ... is not available for date 2026-09-01"
start_date >= 2026-08-29   ->  "No reanalysis data available ..."   (spin-up)
```

No start date works. One absent upstream run therefore stops snow on every deployment for as long
as it takes the gateway to backfill it — days, in the observed case.

## Why a client-side fix is cheap — the data is not actually missing

Each issue-date's forecast covers a **10-day window**, so consecutive days overlap heavily.
Measured 2026-09-04 while 09-01 was absent as an *issue date*:

```
issued 2026-08-30  ->  covers 2026-08-30 … 2026-09-08   contains 2026-09-01: YES
issued 2026-08-31  ->  covers 2026-08-31 … 2026-09-09   contains 2026-09-01: YES
issued 2026-09-02  ->  covers 2026-09-02 … 2026-09-11   contains 2026-09-01: no
```

**The missing day's data exists in its neighbours' forecasts.** A single absent issue date is not a
data gap at all; it would take roughly ten consecutive absent dates to create one.

**This changes how urgent the upstream request is — but only once this issue ships.** Today, with
no fallback, we genuinely are blocked until the gateway backfills, which is what the sent report
says. Once this lands, a single absent run degrades to a logged substitution and the backfill
becomes a nice-to-have rather than a blocker. That is the main argument for doing this at all.

The brittleness is specific to `get_operational`. The sibling endpoints already behave better:
`snow-forecast` is per-issue-date, and `snow-reanalysis` is **hole-tolerant** — asked for
2026-08-20 → 2026-09-04, a range spanning the hole, it returned HTTP 200 with the days it had
rather than refusing — and it accepts an `end_date`. (Reanalysis lags a few days, so it covers
history, not the current window; it is noted here as evidence that tolerating holes is normal for
this API, not as the proposed source.)

## Proposed fix — keep it small

1. Catch **only** the `not available for date` response from `get_operational`. Any other failure
   keeps today's behaviour.
2. On that error, fetch the recent window as per-issue-date `snow-forecast` calls, skipping issue
   dates that return the "No data found" 400. The 10-day overlap fills the hole.
3. Log every substituted date, per HRU and variable.

Explicitly **not** in scope: replacing `get_operational` as the primary source; a general retry or
caching layer; using `snow-reanalysis`; changing the schedule; back-filling history.

**Blocked on a client bug**: `sapphire_dg_client.get_snow_forecast` sends `param=` where that
endpoint requires `parameter=<UPPER>`, so it returns **HS for every variable** with HTTP 200
(reported — see the gateway report, item 3). Until that ships, call the endpoint directly with the
correct parameter name rather than through the broken method; do not work around it by accepting
whatever the method returns.

## The substitution is not the same quantity — say so

A value for 2026-09-01 taken from the 31 August run is a **one-day-ahead forecast**, not the
operational value. At one day's lead for snow the difference is small, but it is a different
number. Do not write it silently as if it were operational: either record the substitution or, at
minimum, log it clearly enough that someone reading the DB can find out. This is the one design
question in the issue and it needs an owner decision before implementation.

## Acceptance criteria

- With the upstream day present, behaviour is unchanged — the fallback never runs, and the written
  rows are identical to today's.
- With one issue date absent, the run completes and writes rows covering that date.
- Each substituted date is logged with its HRU and variable.
- A non-`not available for date` failure still fails; the fallback does not swallow it.
- The fallback cannot mask a *total* outage — if the recent window cannot be assembled, the run
  still fails (this is PREPG-009's contract, and must not be weakened here).
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.

## Contract not to break

- **Do not weaken PREPG-009.** That issue makes a fetch failure fail the run; this one must reduce
  how often that happens, never hide it when it does.
- **Do not change the happy path.** When `get_operational` succeeds, nothing new should execute.
- The forecast endpoint returns a **different shape** — wide, four metadata rows (`Sensor`,
  `Category`, `Unit`, `Interpolation`), dates as `DD.MM.YYYY` descending — so it needs its own
  transform. Do not feed it to `transform_snow_data`, which expects the operational shape.
- `write_snow_to_api`'s existing preservation behaviour (PREPG-020, fail-closed) must be untouched.
