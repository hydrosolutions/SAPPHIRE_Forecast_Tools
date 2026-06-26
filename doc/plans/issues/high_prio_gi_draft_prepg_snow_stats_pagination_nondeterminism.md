# PREPG-009: Snow climatology API pagination nondeterminism corrupts norms and stats

**Status**: Draft (2026-06-26)
**Module**: `apps/preprocessing_gateway` + `sapphire/services/preprocessing`
**Priority**: **High** (silent climatology corruption on a deployed dashboard)
**Labels**: `preprocessing_gateway`, `preprocessing-service`, `snow-data`, `climatology`, `api-pagination`, `data-integrity`
**Related**: PREPG-001, PREPG-006, PREPG-007, PREPG-008, FD-014, ML-007

> Sanitized: use placeholder station code `19999` in tests and examples. Do not commit real
> station codes, real SWE/HS values, runoff values, or deployment secrets.

---

## Summary

Snow climatology recomputation reads historical snow records from the preprocessing API through
`OFFSET`/`LIMIT` pagination. The service query has no stable `ORDER BY`, while
`calculate_snow_norms_from_api()` and `calculate_snow_stats_from_api()` walk the entire snow table per
variable with no `code` filter. Without an ordered result set, page boundaries can drift between API
calls and runs, so the app receives an incomplete, nondeterministic subset before `drop_duplicates()`.

The correct fix is service-side ordering in `crud.get_snow()`: add
`ORDER BY (snow_type, code, date, id)` before `offset(skip).limit(limit)`. This is not an API contract
change: endpoint, parameters, and response schema stay the same; only result ordering becomes stable.
Because `sapphire/services/**` is colleague-owned, the service change is drafted by us for colleague
review and must not be merged without their approval.

This is the true root cause behind the intermittent/missing/jumping snow bands observed during
PREPG-007. PREPG-007's P3 band verification is unreliable until this lands. The bug is distinct from
PREPG-008, which tracks leap-year `dayofyear` alignment.

## Problem

`recalculate_snow_norms.py` computes yearly snow climatology by calling:

- `dg_utils.calculate_snow_norms_from_api(client, variables)` for daily norms.
- `dg_utils.calculate_snow_stats_from_api(client, variables, n_years_min=5)` for counts and bands.

Both helpers currently paginate all rows for a snow variable in 10,000-row pages, without filtering by
station code. The archive is large enough that each variable spans many pages. The app-side read is
valid only if each service page is drawn from one stable ordered result set.

After the read, both helpers call `drop_duplicates(subset=["snow_type", "code", "date"])`. When the
underlying pages are unstable, this does not repair coverage; it only masks duplicate rows while
leaving omitted rows omitted. In `calculate_snow_stats_from_api()`, the distinct-year count then gates
band population. If `count < n_years_min` (`5`), the row keeps `count` but `mean`, `min`, `max`, and
percentiles remain null.

Result: climatology envelopes flicker or disappear unpredictably even though the archive contains
enough years.

## Evidence

- `apps/preprocessing_gateway/dg_utils.py:407-418`:
  `calculate_snow_norms_from_api()` reads pages with
  `client.read_snow(snow_type=variable.upper(), skip=skip, limit=10000)` and no `code` filter.
- `apps/preprocessing_gateway/dg_utils.py:431-432`:
  norms drop duplicates after the paginated read.
- `apps/preprocessing_gateway/dg_utils.py:547-558`:
  `calculate_snow_stats_from_api()` uses the same no-code-filter pagination pattern.
- `apps/preprocessing_gateway/dg_utils.py:571-572`:
  stats drop duplicates after the paginated read.
- `apps/preprocessing_gateway/dg_utils.py:600`:
  stats count distinct years per `(snow_type, code, dayofyear)`.
- `apps/preprocessing_gateway/dg_utils.py:617`:
  stats populate bands only when `count >= n_years_min`.
- `sapphire/services/preprocessing/app/crud.py:300`:
  `get_snow()` executes `query.offset(skip).limit(limit).all()` with no `ORDER BY`.
- `sapphire/services/preprocessing/app/main.py` exposes only the existing `/snow/` read endpoint for
  snow data in this service; there is no local `/snow/coverage` route to enumerate station codes.
- `apps/preprocessing_gateway/recalculate_snow_norms.py:111` and `:128`:
  the yearly recalc consumes both helpers and writes the resulting norms/stats into per-calendar-year
  snow rows.

Operational evidence: the archive has SWE records for placeholder code `19999` on Jan 1 across many
distinct years, but a recalc stored `count=2`, so early-January bands were nulled. SWE and HS have
comparable table sizes (about 788k rows, about 80 codes each), so this is not data sparsity or
variable-specific volume. It is nondeterministic pagination selecting different subsets.

## Root Cause

The preprocessing service exposes snow reads through `OFFSET`/`LIMIT` pagination without a stable
sort. Application-side climatology code requests the entire variable table, page by page, and assumes
those pages form a complete deterministic snapshot. They do not.

The primary defect is in:

- `sapphire/services/preprocessing/app/crud.py:get_snow()`

The affected consumers include:

- `calculate_snow_norms_from_api()`
- `calculate_snow_stats_from_api()`
- any other `get_snow()` caller that paginates results

Once `get_snow()` orders results by `(snow_type, code, date, id)`, the existing app-side whole-table
pagination becomes deterministic and complete. No app-side rewrite is required for the climatology
helpers in this issue.

## Pivot From Prior Draft

The earlier app-side per-code plan is dropped because review found it is unworkable and dangerous:

- `ieasyhydroforecast_HRU_SNOW_DATA` is the wrong namespace for API snow station codes. HRU codes and
  stored snow station codes have zero overlap in reviewed deployments, so a per-code loop keyed on
  HRUs would match zero rows and zero out climatology.
- There is no local `/snow/coverage` service route to enumerate `(snow_type, code)` pairs.
- `crud.get_snow()` itself has no stable ordering, so any app-side discovery that depends on
  paginated `get_snow()` inherits the same bug.
- The old `limit=50_000` per-code read, discovery-completeness test, HRU discovery fallback, and
  `/snow/coverage` fallback are therefore moot once the service read is ordered.

Optional app-side logging can be considered later, for example warning if a variable returns zero
paginated rows, but that is non-functional observability and must not reintroduce per-code logic.

## Executor Constraints

- `sapphire/services/**` is colleague-owned. P1 is drafted for colleague review; do not merge it
  without the service owner's approval.
- P1 is non-contract-changing: same endpoint, same parameters, same response schema, stable result
  ordering only.
- Agents may modify only the files listed in each phase's allow-list.
- Do **NOT** change `calculate_snow_norms_from_api()` or `calculate_snow_stats_from_api()` in this
  issue; their whole-table pagination is correct once `get_snow()` orders results.
- Do **NOT** change existing function signatures or downstream data flow.
- Changes must be additive/minimal and behavior-focused.
- New logic gets tests. Test behavior, not implementation details.
- Use placeholder station code `19999` and dummy values in tests and examples. Do not use real
  station codes or real operational data values.
- Keep PREPG-008 out of scope: do not change the `dayofyear` leap-year convention in this issue.
- Keep PREPG-007 out of scope except for cross-reference/remediation verification.

---

## Phases

### P1 - Service stable ordering for snow pagination

**Goal**

Make `get_snow()` pagination deterministic and complete for all consumers by adding a stable service
ordering before `OFFSET`/`LIMIT`.

**Files**

Allowed to modify:

- `sapphire/services/preprocessing/app/crud.py`
- `sapphire/services/preprocessing/tests/test_crud.py`
- `sapphire/services/preprocessing/tests/factories.py`

No other files.

**Depends on**

None.

**Agents**

1 Sonnet 4.6 general-purpose agent, `isolation: "worktree"`.

Scope:

- Draft the service change for colleague review; do not merge without their approval.
- In `crud.get_snow()`, add stable ordering before `query.offset(skip).limit(limit).all()`.
- Use the ordered key `(Snow.snow_type, Snow.code, Snow.date, Snow.id)`. This is preferred over
  `Snow.id` alone because it gives deterministic domain ordering while retaining `id` as a final
  tie-breaker.
- Do not change endpoint parameters, response schema, filters, function signature, or write behavior.
- Add a service regression test that seeds more than one page of snow rows spanning multiple codes and
  years. Insert rows in deliberately shuffled order so unordered retrieval does not accidentally match
  the expected domain order.
- In the regression test, paginate via `offset`/`limit` across pages and assert:
  - every seeded row is returned exactly once across the page union;
  - no row appears in more than one page;
  - the concatenated paginated result is ordered by `(snow_type, code, date, id)`;
  - repeated paginated calls return the same ordered row sequence.
- The test must fail without the `ORDER BY` and pass with it.
- Keep the consumer-wide note in code review/PR description: this fixes all paginating `get_snow()`
  consumers, not just snow climatology.

**Acceptance criteria**

- [ ] `crud.get_snow()` applies `ORDER BY (snow_type, code, date, id)` before `offset(skip).limit(limit)`.
- [ ] The service endpoint contract is unchanged: same route, parameters, response model, and response
      fields.
- [ ] A regression test proves paginated reads over a multi-code, multi-year fixture are complete,
      non-overlapping, and stable across repeated calls.
- [ ] The regression test fails against the old unordered query and passes after the ordering change.
- [ ] The change is marked as drafted for colleague review and is not merged without the service
      owner's approval.
- [ ] No real station codes or real data values appear in new/changed tests; use `19999` and other
      dummy placeholder codes only.
- [ ] Relevant service tests pass, for example
      `uv run pytest sapphire/services/preprocessing/tests/test_crud.py`.

### P2 - Remediation and deterministic verification

**Goal**

After P1 is available locally or deployed, recompute affected snow climatology rows for the calendar
years spanning the current dashboard display window and verify the stored counts/bands are complete
and stable.

**Files**

No implementation files. This is an operator/orchestrator runbook phase.

**Depends on**

P1.

**Agents**

No implementation agent required. Operator/orchestrator action after P1 is locally available and
after colleague-approved deploys.

Scope:

- Before remediation, take a snow-table backup or deployment-appropriate DB snapshot.
- Local verification: rebuild/restart the preprocessing service image/container so the ordered
  `get_snow()` query is active before running recalc.
- Server verification: run only after the colleague has approved, merged, and deployed the service
  ordering fix.
- Identify the calendar years that overlap the current hydrological display window. For a window like
  `2025-09-01 ... 2026-08-31`, run the recalc for both `2025` and `2026` if both calendar years are
  displayed and need stored bands.
- Run the existing snow norm/stat recalc for those years using deployment-approved commands. Use
  `ieasyhydroforecast_SNOW_RECALC_YEAR=<year>` when invoking `snow_norms`.
- Run the stats computation or full recalc twice against the same API state and compare the resulting
  counts and band columns for equality before accepting the remediation.
- Verify placeholder SWE code `19999`, `dayofyear=1`, has a stored count equal to the archive's full
  `count(DISTINCT year)` for that `(snow_type, code, dayofyear)`. Do not hardcode `26`; the correct
  value depends on the archive and may differ by code.
- Verify every `(snow_type, code, dayofyear)` with at least `n_years_min` distinct years has populated
  `mean`, `min`, `max`, and percentile fields after recalc.
- Verify the dashboard no longer shows early-January SWE/HS band gaps caused by missing stats. Any
  remaining leap-year calendar-date shift belongs to PREPG-008.
- The corruption is archive-wide. Other calendar years may also contain stale bad stats and should be
  rerun when they enter an active display window or when operators choose a broader cleanup.

Example sanitized SQL shape:

```sql
WITH archive_count AS (
  SELECT
    snow_type,
    code,
    EXTRACT(DOY FROM date)::int AS dayofyear,
    count(DISTINCT EXTRACT(YEAR FROM date)::int) AS archive_year_count
  FROM snow
  WHERE code = '19999'
    AND snow_type = 'SWE'
    AND EXTRACT(DOY FROM date)::int = 1
    AND value IS NOT NULL
  GROUP BY snow_type, code, EXTRACT(DOY FROM date)::int
)
SELECT
  s.snow_type,
  s.code,
  EXTRACT(DOY FROM s.date)::int AS dayofyear,
  s.count AS stored_count,
  a.archive_year_count,
  s.mean,
  s.min,
  s.max,
  s.q05,
  s.q50,
  s.q95
FROM snow s
JOIN archive_count a
  ON a.snow_type = s.snow_type
 AND a.code = s.code
 AND a.dayofyear = EXTRACT(DOY FROM s.date)::int
WHERE s.code = '19999'
  AND s.snow_type = 'SWE'
  AND s.date = DATE '2026-01-01';
```

**Acceptance criteria**

- [ ] A snow-table backup or deployment-appropriate DB snapshot exists before remediation.
- [ ] The ordered service query is active before recalc: locally via rebuilt preprocessing service
      image/container, or server-side after colleague-approved merge/deploy.
- [ ] The snow recalc has been rerun for each calendar year spanning the current dashboard display
      window that needs corrected stored bands.
- [ ] Repeated stats computation or full recalc over the same API state produces identical counts and
      band columns.
- [ ] Placeholder SWE code `19999`, `dayofyear=1`, stores a count equal to the archive's full
      distinct-year count for that code/day after recalc, not a random low count.
- [ ] Bands populate for every day-of-year with at least `n_years_min` distinct years in the archive.
- [ ] No flicker or run-to-run disappearance of climatology bands remains after P1 plus recalc.
- [ ] Any remaining leap-year date alignment issue is triaged to PREPG-008, not this issue.
- [ ] From repo root or `apps/`, `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway`
      passes with zero unexpected skips. The only acceptable skip is the explicit
      `sapphire-api-client` dependency gate.

## Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 0 }
  }
}
```
