# Support flag field on long-term forecasts for operational vs hindcast differentiation

- **Status**: Draft
- **Module**: infra (cross-module, requires service coordination)
- **Priority**: Medium
- **Labels**: `api`, `long-term-forecasting`, `data-integrity`, `service-coordination`

## Summary

The `flag` column on `long_forecasts` exists but is not queryable, not
constrained, and has inconsistent semantics with the short-term `forecasts`
table. This prevents consumers from distinguishing operational forecasts from
backfilled/hindcast data, which blocks the planned hindcast gap-filling
workflow.

## Problem

### 1. No read-side filter

`GET /long-forecast/` has no `flag` query parameter
(`postprocessing/app/main.py:136-165`). Consumers receive all records mixed
together with no way to request only operational or only hindcast data.

### 2. Silent overwrite on upsert

The unique constraint does not include `flag`
(`postprocessing/app/models.py:147-156`). When `_bulk_upsert` resolves a
conflict, `flag` is in `update_cols` and gets silently overwritten. A backfill
after an operational run replaces `flag=0` with `flag=0` — no data loss, but
the record's provenance is lost.

### 3. Inconsistent flag semantics across tables

| Value | `long_forecasts` | `forecasts` (short-term ML) |
|-------|-------------------|---------------------------|
| 0 | Operational forecast | Valid forecast |
| 1 | Hindcast | Contains NaN |
| 2 | Error/missing data | Error |
| 3 | — | NaN after hindcast recalc |
| 4 | — | Valid after hindcast recalc |

Long-term semantics: `lt_utils.py:285`. Short-term semantics:
`utils_ml_forecast.py:644`, `recalculate_nan_forecasts.py:6-10`.

## Current flag assignments in code

| Location | Flag | Context |
|----------|------|---------|
| `run_forecast.py:230` | 0 | Successful operational forecast |
| `run_forecast.py:236` | 2 | Failed forecast (missing data) |
| `calibrate_and_hindcast.py:214` | 1 | Hindcast output |
| `simulate_forecasts.py` → `run_forecast` | 0 | Backfill — **same as operational** |
| `api_writer.py:778` | 0 | Monthly ensemble |

`simulate_forecasts.py` calls `run_forecast()` which unconditionally sets
`flag=0`. There is no mechanism to pass a different flag for backfilled data.

## Desired behavior

1. `GET /long-forecast/` accepts optional `flag` query parameter
2. `simulate_forecasts.py` writes `flag=1` for backfilled data
3. Flag semantics documented in a canonical location
4. Dashboard consumers can filter by `flag=0` to get operational-only data

## Proposed changes

### App-side (we do ourselves)

| File | Change |
|------|--------|
| `run_forecast.py` | Accept optional `flag_override` parameter; default to current behavior |
| `simulate_forecasts.py` | Pass `flag_override=1` when calling `run_forecast()` |

### Service-side (need your input)

| File | Change | Effort |
|------|--------|--------|
| `main.py` | Add `flag: int = None` param to `read_long_forecast()` | Small |
| `crud.py` | Add `flag` filter to `get_long_forecast()` query builder | Small |
| `models.py` | Optional: `CheckConstraint` limiting flag to 0,1,2 | Small |
| `models.py` | Docstring or enum documenting flag semantics | Small |

## Flag semantics: standardize or document?

**Option A — Standardize both tables.** Risky. Short-term ML code interprets
`flag=1` as "NaN value" throughout. Large refactor, high risk, low payoff.

**Option B — Document and accept divergence.** The two tables serve different
domains. Long-term produces monthly aggregates (whole forecast succeeds or
fails). Short-term produces daily values where individual days can be NaN.
Document both in `models.py` with clear docstrings.

**Recommendation: Option B.** The cost of standardization outweighs the benefit.

## Migration considerations

- Existing rows from `simulate_forecasts.py` have `flag=0` but are actually
  backfills. No way to retroactively distinguish them (no `created_at` column).
- Future backfills after this fix will have `flag=1` — clean boundary going
  forward.
- `CheckConstraint(flag.in_([0, 1, 2]))` is safe — existing data only
  contains these values.

## Questions for discussion

1. OK to add `flag` as a query parameter to `GET /long-forecast/`?
2. Should we also add it to `GET /forecast/` (short-term) for consistency?
3. Prefer a `FlagType` enum in `models.py` or just a docstring?
4. Appetite for adding a `created_at` timestamp to `long_forecasts`? Would
   solve retroactive identification and help with general auditing.
5. Should `flag` be part of the unique constraint? If yes, the same
   station+date+model could have both an operational and hindcast row. Cleaner
   semantics but changes upsert behavior significantly.
