# PP-046 — `get_latest_forecasts` dedups on a yearless key, collapsing same-period rows across years

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: Medium — **flagged for owner re-rating 2026-08-18**: "latent" is wrong.
Two entry points already reach `save_forecast_data` with a frame that is not year-bounded
by construction (see "Existing multi-year callers"), so the collapse needs no new caller
to manifest. How often it *does* manifest depends on how many years the archive holds for
a given period — runtime data, not probed here. Not re-rated.
**Labels**: `postprocessing`, `data-integrity`

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
  (`start_year == end_year`), so PP-045 is not blocked. The other two callers do
  not work around it — see below — so no *future* caller is needed for the writer to
  defeat its stated two-year retention.

## Existing multi-year callers (added 2026-08-18)

An earlier draft of this issue called the collapse latent and spoke of a "future"
multi-year caller. Both statements were wrong: two callers already exist. Neither is
year-bounded on the read side. Note what this does **not** claim: that either writes on
every invocation, or that any particular archive currently holds several years of the
same period.

1. **`postprocessing_maintenance.py`** — reads combined forecasts with **no date
   bounds** (`data_reader.read_combined_forecasts(config.name, codes=codes)`,
   `postprocessing_maintenance.py:183`; the function's signature takes only
   `horizon_type` and `codes`, `src/data_reader.py:791-794`) and later writes the merged
   `combined + joint` frame back through the same sink
   (`file_writer.save_forecast_data(config, merged)`, `:403-430`). It is the **persistent**
   caller — nothing decided for PP-060 removes it — but it is **not** unconditional: four
   guards return before the save (empty combined `:185-187`; no affected dates `:227-247`;
   no modelled data for the affected dates `:268-289`; no refresh rows generated
   `:367-369`), and the read is scoped to configured codes (`:133-139`). Gap detection is
   further bounded to a configured lookback window — `POSTPROCESSING_GAPFILL_MAX_MONTHS`,
   **13 months by default** (`postprocessing_maintenance.py:116`), applied at
   `gap_detector.py:80-85`, `:208-210` — though the stale-EM scan is not
   (`postprocessing_maintenance.py:214-225`). So: whatever span the
   archive holds for the configured codes is what reaches the dedup **when the guards
   pass**.

2. **`recalculate_skill_metrics.py::_run_short_term_recalc`** — as of trunk
   `a304ffb0`; **removed by PP-060 option (a)** (the recalc stops writing forecast
   rows), so treat this caller as time-limited. It reads with no year bounds
   (`read_observed_and_modelled_data(config.name, codes=codes)`, `:191-194`), then
   `calculate_skill_metrics` reassigns `modelled` to its own filtered frame, then
   `file_writer.save_forecast_data(config, modelled)` (`:233`) — a bare call that
   inherits `write_csv=True` and `require_api=False`.

**Interaction with `SAPPHIRE_SKILL_METRICS_START_YEAR`.** The recalc's saved frame is
not the raw read: `calculate_skill_metrics` filters both `observed` and `simulated`
to `date.year >= min_year`, where `min_year` defaults to `today.year - 20` and is
overridden by that env var (`src/skill_metrics.py:2165-2169`). The filter is a **lower
cutoff only** and inclusive, so by default it admits the current year plus the preceding
20 calendar years (21 year-buckets) — how many years are actually
present is a property of the archive, not of this code. **Raising** the env var narrows
the admitted span (and so can hide the collapse); **lowering** it widens the span.
Neither changes the dedup itself. Note the maintenance caller applies no such cutoff at
all, so the recalc's frame is not the widest one the yearless key sees.

**CSV and API do not receive the same frame.** `save_forecast_data` writes the **full**
`simulated` frame to the combined CSV and only the deduped `simulated_latest` to the API
(`src/file_writer.py:212-256`). A multi-year run therefore leaves the CSV holding rows
this call never **sent** to the API. Do **not** treat a CSV-vs-API gap as a signature of
this bug: with the default `require_api=False` an unavailable API or a failed write
leaves the same gap (`:244-256`), the API writer separately drops LR, null-discharge and
invalid-horizon rows (`src/api_writer.py:230-244`, `:337-429`), and the service upserts
without deleting omitted rows (`sapphire/services/postprocessing/app/crud.py:18-59`), so
rows dropped by the dedup may still be in the database from an earlier write. Attribute a
discrepancy only after an API read-back.

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
- **PP-060** (`high_prio_gi_draft_pp_recalc_backfill_write_divergence.md`) — the
  divergence between the recalc and the operational/backfill path across the same
  sink; its option (a) removes caller 2 above. The yearless key is one of its eight
  divergence axes, so a fix here must be sequenced against it.
