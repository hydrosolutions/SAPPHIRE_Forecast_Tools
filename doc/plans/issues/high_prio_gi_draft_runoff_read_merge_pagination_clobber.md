# Runoff read-merge-write silently clobbers non-null `discharge`/`predictor` beyond the first 100 rows

**Priority:** high (silent data loss in `runoffs`; no error, no warning).
**Module:** `apps/iEasyHydroForecast/forecast_library.py` (`_write_runoff_to_api`) — the shared
runoff write path used by `preprocessing_runoff` and `linear_regression`.
**Status:** Draft — mechanism confirmed read-only against `origin/maxat_sapphire_2` on
2026-07-14 (found by an out-of-loop Codex review of the runoff backfill plan; the pagination
hole was flagged in shipped code, not merely in the plan).
**Blocks:** `doc/plans/working/runoff_pentad_decad_discharge_backfill_plan.md` — that plan's
whole premise is that read-merge-write prevents null-clobber. It does not, past row 100.

## Summary

`_write_runoff_to_api` guards `maintenance`/`initial` writes with a read-merge-write: read the
rows that already exist, and for any outgoing field that is `None`, keep the stored value rather
than overwriting it with `NULL`. The merge is correct — but the **read it depends on is silently
truncated to 100 rows per station**, so for every existing row past the 100th the merge behaves
as if no row existed, writes the outgoing `None`, and the service upsert overwrites a good stored
value with `NULL`.

## Evidence (all on `origin/maxat_sapphire_2`)

1. **The read is unpaginated.** `apps/iEasyHydroForecast/forecast_library.py:3761` calls
   `client.read_runoff(horizon=..., code=..., start_date=..., end_date=...)` with **no `skip` /
   `limit`**.
2. **The client default is 100.** `sapphire_api_client/preprocessing.py:50-58` —
   `def read_runoff(..., skip: int = 0, limit: int = 100)`. The service agrees:
   `sapphire/services/preprocessing/app/main.py:79` and `crud.py:65` both default `limit: int = 100`.
3. **Truncated rows read as absent.** The merge builds `existing_by_key` only from what came back,
   then `existing_record = existing_by_key.get((code, date))` / `if not existing_record: continue`
   (`forecast_library.py:3789-3791`) — the outgoing record keeps its `None` fields.
4. **The upsert then blindly overwrites.** `sapphire/services/preprocessing/app/crud.py:33-36` —
   `for k, v in data.items(): setattr(existing, k, v)`. An incoming `None` becomes `NULL`.
5. **The surviving 100 are the OLDEST.** `crud.py:80` orders by `(Runoff.code, Runoff.date)` before
   `.offset().limit()`. So the merge protects the *earliest* rows in the window and leaves the
   *newest* — the ones a refresh is most likely to touch — unprotected.

Net: **no exception, no warning, no failed-write count.** The run reports success.

## Blast radius

Protection covers only the oldest 100 rows per `(code, window)`:

| Horizon | Rows/year | Rows protected | Exposed when… |
|---------|-----------|----------------|----------------|
| pentad  | 72        | ~1.4 years     | window > ~1.4 yr |
| decade  | 36        | ~2.8 years     | window > ~2.8 yr |

- **`initial` mode writes the full archive.** Over a ~16-year archive (~1,150 pentad rows/station)
  only ~9% of rows are protected; **~91% are exposed** to null-clobber.
- **Routine `maintenance` is probably safe today** — a short (e.g. 90-day) window is well under 100
  rows — which is exactly why this has not surfaced. The hazard is latent until the next backfill.

## Fix

Paginate the existing-row read. The repo already has the house pattern at
`apps/iEasyHydroForecast/setup_library.py:1948-1962` (loop `skip` by `page_size`, stop on a short
page). Apply it to the `read_runoff` call in `_write_runoff_to_api`.

**No service change required** — `skip`/`limit` already exist on both client and endpoint. This
stays inside `apps/` and does not touch colleague-managed `sapphire/services/`.

Defensive extra: if a read returns exactly `limit` rows, that is the truncation signature — log a
warning if pagination is ever removed or capped again.

## Acceptance criteria

Tests use the placeholder station code `19999`; no real codes or discharge values.

1. **Regression (the bug):** `maintenance`/`initial` write for `19999` where **>100** rows already
   exist in the window, and an outgoing record beyond the 100th has `discharge=None` while the
   stored row has a non-null `discharge`. Assert the written payload **preserves** the stored
   value. This test must FAIL against current code.
2. **Ordering:** because the service returns the oldest rows first, the regression must place the
   clobber-victim among the **newest** rows — a test that only checks row 1 passes today and proves
   nothing.
3. **Pagination boundary:** exercise exactly 100, 101, and >2 pages of existing rows.
4. **No behavior change under 100 rows:** existing merge behavior stays byte-identical (guards the
   current operational path).
5. **Operational mode untouched:** operational writes still write today's row with
   `discharge=None` — no merge, no read.

## Open question

`_write_runoff_to_api` reads **per station code** over the full `[min(date), max(date)]` span of the
batch. If a backfill batch spans many stations and many years, the paginated read is
`n_codes × n_pages` requests. If that is too slow, the alternative is one paginated sweep of the
whole window keyed by `(code, date)` — same merge semantics, fewer round-trips. Measure before
optimizing.

## Related

- `doc/plans/working/runoff_pentad_decad_discharge_backfill_plan.md` (blocked by this)
- Same *class* of defect as PREPQ-009's norm-clobber: a blind full-column upsert
  (`crud.py` `setattr` loop) means any writer that sends `None` destroys stored data. PREPQ-009 fixed
  it for `hydrographs.norm` with a writer-side read-merge; this issue is that same read-merge being
  silently truncated for `runoffs`. **A service-side partial-update/COALESCE would remove the whole
  class** — worth raising with the service owner rather than patching each writer.
