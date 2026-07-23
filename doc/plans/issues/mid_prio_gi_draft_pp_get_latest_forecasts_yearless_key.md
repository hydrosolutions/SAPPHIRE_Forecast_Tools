# PP-046 — `get_latest_forecasts` dedups on a yearless key, collapsing same-period rows across years

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: Medium (latent; worked around by PP-045, but affects any multi-year write)
**Labels**: `postprocessing`, `data-integrity`, `latent`

---

## Summary

`file_writer.get_latest_forecasts` de-duplicates on the **yearless** key
`(code, period_in_year, model_short)` **before** applying its `year >=
latest_year - 1` filter, so the same period-in-year in two different years
collapses to the later year's row — despite a comment promising "the last two
years".

## Evidence

- `apps/postprocessing_forecasts/src/file_writer.py:118-129`:
  ```python
  sorted_df = simulated_df.sort_values("date", ascending=False)
  latest_forecasts = sorted_df.drop_duplicates(
      subset=["code", horizon_column_name, "model_short"], keep="first"
  ).copy()
  latest_year = simulated_df["date"].max().year
  latest_forecasts = latest_forecasts[latest_forecasts["year"] >= (latest_year - 1)]
  ```
  The dedup key omits year, and the year filter runs *after* the dedup — so for
  any period present in `latest_year`, the prior year's same-period row is
  already dropped. The "keep two years" comment
  (`file_writer.py:124-125`) is therefore misleading.

## Impact

- Anything that feeds `save_forecast_data` more than one calendar year at once
  writes only the latest year's rows per (code, period-in-year, model) to the
  API — silently dropping older years.
- PP-045's backfill **works around** this by processing one year at a time
  (`start_year == end_year`), so PP-045 is not blocked. But the underlying
  writer remains a footgun for any future multi-year caller and defeats the
  stated two-year retention.

## Proposed direction (owner to confirm)

Either (a) include `year` in the dedup key so both years' same-period rows
survive, or (b) drop the misleading "two years" comment and document the
one-year-per-write contract explicitly. Decide which retention behavior is
actually intended before changing the key — the two-year filter suggests (a).

## Out of scope / notes

- No `sapphire/services/` change.
- Add a regression test (a same-`(code, period_in_year, model_short)` frame
  across two years) — PP-045 already added a lock that *demonstrates* the
  collapse (`test_backfill_period_forecasts.py::TestGetLatestForecastsCollapsesAcrossYears`);
  a fix would flip that expectation.

## References

- Found during PP-045 (review_gi_draft_pp_missed_boundary_period_gap.md); the
  per-year backfill loop exists precisely because of this behavior.
