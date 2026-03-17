# ML-007: Non-Deterministic API Pagination Causes Inconsistent ML Gap Detection

**Status**: Draft
**Module**: `machine_learning` / infra (cross-cutting — involves
`sapphire/services/` coordination)
**Priority**: Medium
**Labels**: `bug`, `api-integration`, `reliability`
**Blocked By**: Coordination with `sapphire/services/` owner for Option 1

---

## Summary

`_read_ml_forecasts_from_api()` in
`apps/machine_learning/scr/utils_ml_forecast.py` reads all station codes
in a single paginated query. The underlying PostgreSQL endpoint in
`sapphire/services/postprocessing/` has no explicit `ORDER BY`, so DB
pages are returned in arbitrary order. With ~55 codes × 730 days of
data, two consecutive runs of the same query for the same code can
return different date ranges. Gap detection in `fill_ml_gaps.py` is
therefore unreliable: some runs miss recent operational records, gaps
appear to end early, and the full gap window is not detected.

---

## Root Cause Analysis

### Observed behaviour

Two consecutive invocations of `_read_ml_forecasts_from_api()` for the
same station code returned different date ranges — one included 2026
data, the other did not. The query parameters were identical.

### Why pagination produces non-determinism

PostgreSQL does not guarantee row order unless `ORDER BY` is specified.
When a paginated query uses `LIMIT`/`OFFSET` (or keyset pagination)
without an explicit ordering clause, the set of rows falling on each
page is arbitrary and may vary between requests as a result of:

- Planner choosing different index scans
- Buffer pool state
- Concurrent writes filling gaps in heap pages

With ~55 codes × 730 days ≈ 40 000 rows split across multiple pages,
any single page boundary can land at a different point in the dataset
on each call. The caller receives a structurally valid response (correct
number of rows) but with a silently different date range.

### Where the bug manifests

| File | Location | Effect |
|------|----------|--------|
| `apps/machine_learning/scr/utils_ml_forecast.py` | `_read_ml_forecasts_from_api()` | Paginated read with no ordering guarantee |
| `apps/machine_learning/scr/fill_ml_gaps.py` | Gap detection logic | Consumes the non-deterministic read; detects different gap windows on each run |

### Ownership note

The PostgreSQL query lives in
`sapphire/services/postprocessing/` (colleague-managed). Per CLAUDE.md,
this code must not be modified without coordination. Option 1 below
requires an explicit coordination step before implementation.

---

## Implementation Plan

Three options are available with different tradeoffs:

### Option 1 — API-side fix (preferred)

**What**: Ask the `sapphire/services/` owner to add
`ORDER BY id` (or `ORDER BY code, target_date`) to the forecast list
endpoint in `sapphire/services/postprocessing/`. No client-side changes
are required.

**Tradeoffs**:

| Pro | Con |
|-----|-----|
| Single change, fixes all callers | Requires coordination with colleague |
| No increase in API call count | Small query plan change on the server side |
| Correct by construction | Blocked until colleague reviews and merges |
| Cheapest long-term maintenance | |

**Steps**:
1. Open a coordination discussion with the `sapphire/services/` owner,
   referencing this issue.
2. Ask them to add `ORDER BY` to the forecast endpoint (they own the
   fix; we own the issue description and test).
3. After server-side merge, write a regression test in
   `apps/machine_learning/test/test_api_integration.py` that verifies
   consecutive reads for the same code return the same date range.

**Implementation checklist**:
- [ ] Open coordination discussion; share this file as context
- [ ] Confirm which column(s) the ORDER BY should use
  (`id` for stable insert order, or `(code, target_date)` for logical
  order — the latter is more useful to callers)
- [ ] Regression test: two consecutive reads return identical date
  ranges for a fixed code and forecast date
- [ ] Verify `fill_ml_gaps.py` gap detection is stable after fix

---

### Option 2 — Client-side workaround A: per-code reads

**What**: Change `_read_ml_forecasts_from_api()` to issue one API call
per station code instead of a single all-codes query. Each single-code
query returns far fewer rows (≤730) and is less likely to hit a
problematic page boundary that varies between runs. Optionally batch
codes (e.g., 5–10 per call) to reduce the call count.

**Tradeoffs**:

| Pro | Con |
|-----|-----|
| No server-side changes required | 55+ API calls per pipeline run (or N/batch_size with batching) |
| Reliable regardless of server ordering | Higher latency; more network overhead |
| Can be implemented immediately | Batching adds complexity |
| Reduces page-boundary problem per call | Does not fully eliminate non-determinism if batch size leaves many rows per call |

**Implementation checklist**:
- [ ] Refactor `_read_ml_forecasts_from_api()` to accept an optional
  `codes: list[str]` parameter; loop per code (or per batch) rather than
  querying all at once
- [ ] Choose batch size based on typical rows-per-code (~730): a batch
  of 5 codes × 730 rows = 3 650 rows — well within a single page for
  most reasonable page sizes
- [ ] Update callers in `fill_ml_gaps.py` if the function signature
  changes
- [ ] Unit test: mock API returns different orderings per call; assert
  that the final DataFrame contains the expected rows regardless of
  page order
- [ ] Benchmark: measure wall-clock time for 55 codes with per-code vs.
  batched reads and document in PR description

---

### Option 3 — Client-side workaround B: oversized page

**What**: Increase the `page_size` parameter passed to the API to a
value larger than the total expected row count (~40 000 rows), so the
entire result set is returned in a single page and pagination is
effectively disabled.

**Tradeoffs**:

| Pro | Con |
|-----|-----|
| Trivial code change | Fragile: breaks as data grows beyond the page size |
| No server-side changes required | May time out or trigger server-side limits |
| No extra API calls | Transfers far more data per call than needed |
| Immediate stopgap | Not a sustainable solution; silently re-breaks |

This option is **not recommended** except as a short-term stopgap while
waiting for Option 1 coordination.

---

### Recommendation

Implement **Option 1** as the primary fix. While waiting for the
server-side change, apply **Option 2** (per-code reads with a small
batch size) as an interim client-side workaround so that `fill_ml_gaps.py`
is reliable in the meantime. Do not implement Option 3.

---

## Acceptance Criteria

- [ ] Two consecutive calls to `_read_ml_forecasts_from_api()` for the
  same station code and forecast date return identical date ranges
- [ ] `fill_ml_gaps.py` gap detection produces the same gap window on
  repeated runs for the same inputs
- [ ] Regression test added to `apps/machine_learning/test/` that
  verifies stable ordering across calls
- [ ] No increase in false-negative gap detections (gaps that exist but
  are not reported)
- [ ] All existing ML tests continue to pass with zero skips

---

## Out of Scope

- **Fixing other API endpoints**: Only the forecast list endpoint used by
  `_read_ml_forecasts_from_api()` is in scope. Other endpoints may also
  lack `ORDER BY` but are not the subject of this issue.
- **Refactoring `fill_ml_gaps.py` gap logic**: The gap detection
  algorithm itself is correct given stable input. This issue only
  addresses the input stability problem.
- **Long-term forecasting reads**: The long-term module uses a separate
  API client path; any similar issue there is tracked separately.
- **Performance optimisation of bulk reads**: Option 2 introduces more
  API calls; optimising this further (e.g., parallel async calls) is a
  future enhancement, not part of this fix.

---

## Dependency Graph

```json
{
  "options": {
    "option_1_server_fix": {
      "name": "API-side ORDER BY fix (preferred)",
      "depends_on": ["sapphire_services_owner_coordination"],
      "blocked_by": "Coordination with sapphire/services/ owner",
      "files": [
        "sapphire/services/postprocessing/ (colleague-owned, not edited directly)",
        "apps/machine_learning/test/test_api_integration.py"
      ]
    },
    "option_2_per_code_reads": {
      "name": "Client-side per-code reads (interim workaround)",
      "depends_on": [],
      "note": "Can be implemented immediately while Option 1 is pending. Refactor _read_ml_forecasts_from_api() to loop per code or per batch.",
      "files": [
        "apps/machine_learning/scr/utils_ml_forecast.py",
        "apps/machine_learning/scr/fill_ml_gaps.py",
        "apps/machine_learning/test/test_api_integration.py"
      ]
    },
    "option_3_oversized_page": {
      "name": "Oversized page size (not recommended)",
      "depends_on": [],
      "note": "Stopgap only. Fragile and not sustainable as data grows.",
      "files": [
        "apps/machine_learning/scr/utils_ml_forecast.py"
      ]
    },
    "regression_test": {
      "name": "Regression test for stable pagination",
      "depends_on": ["option_1_server_fix"],
      "note": "Verifies that consecutive reads for the same code return identical date ranges after the server-side fix.",
      "files": [
        "apps/machine_learning/test/test_api_integration.py"
      ]
    }
  }
}
```

---

## Related Issues

- **ML-004** — Root issue investigation (Bug E) where non-deterministic
  pagination was first observed. This issue was identified during
  debugging of `_read_ml_forecasts_from_api()` when two runs returned
  different date ranges for the same code.
- **ML-003** — ML gap filling and API read alignment; upstream context
  for how `fill_ml_gaps.py` uses the forecast reader and why stable
  date ranges are required for correct gap detection.
