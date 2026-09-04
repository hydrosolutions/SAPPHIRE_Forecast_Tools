## Snow ingestion cannot tolerate a single missing upstream day (PREPG-025)

**Status**: Draft (2026-09-04) — **decision settled; blocked on the dg-client re-pin**
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
3. **Pick a winner deterministically — this is the part that can corrupt data.** Issue dates
   overlap: both the 08-30 and 08-31 forecasts contain 09-01. For each target date take the
   **newest issuance that is not after it** (shortest lead), and de-duplicate *before* handing rows
   to the existing merge. Do not rely on the existing `(date, code)` sort with `keep="last"`
   (`snow_data_operational.py:352-356`) — it has no issue-date to sort on, because
   `transform_snow_data` drops `Source`, so a stale two-day-ahead value can beat a fresh
   one-day-ahead value. Keep the issue date only while assembling; it is not written.
4. Log every substituted date with its HRU, variable, source issue date and lead.

Explicitly **not** in scope: replacing `get_operational` as the primary source; a general retry or
caching layer; using `snow-reanalysis`; changing the schedule; back-filling history.

**NOT blocked on the client fix — use a small local wrapper (revised 2026-09-04).**
`sapphire_dg_client.get_snow_forecast` sends `param=` where the endpoint requires
`parameter=<UPPER>`, so it returns **HS for every variable** with HTTP 200 (reported — gateway
report item 3). That is dangerous here, because `transform_snow_data(df, variable)` labels values
with the **caller's requested variable** and never checks the source (`dg_utils.py:432`), so HS
would be written as SWE and RoF silently.

The response is **not** to wait. Call the endpoint through the client's existing request path with
the correct parameter name — about five lines in `dg_utils`, doing what the broken method should do.
This was verified working on 2026-09-04 against both orgs: all three variables returned distinct,
correct files (different checksums and sizes), whereas the broken `param=` form returned an
identical HS payload for all three.

Why not wait for upstream: the entire purpose of this issue is to stop an outage depending on
somebody else's schedule. The client fix is on a private repo with no committed timeline (as of
2026-09-04 `main` is still `bd9cc905`, unchanged), while a production deployment is currently five
days without snow. Blocking the robustness fix on the external fix reproduces the very dependency
it exists to remove.

**Keep it reversible.** Mark the wrapper with the upstream issue, and pin the behaviour with the
variable-identity test below — that test is what matters, and it passes either way. When the client
ships the fix, delete the wrapper and call the public method; the test proves the swap is safe. The
re-pin mechanics below still apply at that point, and are no longer urgent.

## The substitution is not the same quantity — say so

A value for 2026-09-01 taken from the 31 August run is a **one-day-ahead forecast**, not the
operational value. At one day's lead for snow the difference is small, but it is a different number,
and **it does not stay confined to that row**: norm/statistics calculations consume every non-null
stored `value` (`dg_utils.py:656`), and the yearly recalculation derives next year's `previous`
band from this year's `value` (`recalculate_snow_norms.py:353`). A substitution therefore
participates in derived values for a year afterwards.

**Decision — log, do not record in the database.** `SnowBase` has no provenance field, and adding
one would be a schema change in colleague-owned service code for a rare event. Log the target date,
source issue date and lead. **ACCEPTED by the owner 2026-09-04: fill and log.** The rationale is explicit — a single-day
upstream gap must not cause a system outage, and that is worth more than keeping substituted values
out of the derived numbers. So substituted values *will* feed norms, statistics and the following
year's `previous` band, and the log is what makes that auditable. Do not re-open this as a defect
later: it is a deliberate trade. The rejected alternative was leaving the day absent.

## Acceptance criteria

- With the upstream day present, behaviour is unchanged — the fallback never runs, and the written
  rows are identical to today's.
- With one issue date absent, the run completes; the assembled window reaches the **CSV**, and the
  **API receives the complete current operational window**. Note the API write filters to
  `date >= yesterday` (`dg_utils.py:1053`), so a target date older than yesterday reaches the CSV
  only — this issue does **not** promise historical database backfill.
- **Completeness is defined and enforced**: name the required (target date x code) coverage of the
  window, and fail the task when any required key is missing. A non-empty but partial window must
  **not** count as success — otherwise PREPG-009 is weakened, and note that
  `get_snow_data_operational` already returns `True` even when the API write fails
  (`snow_data_operational.py:381`), which PREPG-009 deliberately does not change.
- **Overlap resolution is pinned**: with both an N-1 and an N-2 day issuance available for a target
  date, the N-1 value is the one written.
- **Variable identity is pinned**: one fallback test with distinct HS/SWE/RoF values asserting the
  request carried `parameter=<UPPER>` and that the written values are the requested variable's.
- A `SnowPreservationReadError` still escapes on the fallback path (PREPG-020) — the existing test
  covers primary-fetch success only (`test_api_integration.py:941`).
- A non-`not available for date` failure still fails; the fallback does not swallow it.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.

## Contract not to break

- **Do not weaken PREPG-009.** That issue makes a fetch failure fail the run; this one must reduce
  how often that happens, never hide it when it does.
- **Do not change the happy path.** When `get_operational` succeeds, nothing new should execute.
- **Reuse `transform_snow_data`; do not write a second one.** It already handles this shape —
  it renames column 0 to `date`, drops the four metadata rows (`df.iloc[4:]`), parses `dayfirst`
  and ignores `Source` (`dg_utils.py:432-444`). Descending dates are harmless: the caller sorts
  afterwards.
- `write_snow_to_api`'s existing preservation behaviour (PREPG-020, fail-closed) must be untouched.
