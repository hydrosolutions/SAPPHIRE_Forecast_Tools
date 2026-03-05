# ML-003: Migrate ML Maintenance Scripts to API-Primary Reads

**Status**: Ready for Review
**Module**: machine_learning
**Priority**: High
**Labels**: `api-migration`, `gap-detection`, `maintenance`, `reliability`

---

## Summary

Switch `fill_ml_gaps.py`, `recalculate_nan_forecasts.py`, and `add_new_station.py`
from CSV-primary to API-primary forecast reads, with CSV as fallback — eliminating
the blind spot where failed forecasts are invisible to all gap-detection mechanisms.

## Context

The machine_learning module is transitioning from CSV-based I/O to the SAPPHIRE
postprocessing API (tracked broadly in INFRA-007). The ML module already **writes**
forecasts to the API (`_write_ml_forecast_to_api()` in `scr/utils_ml_forecast.py:521`),
but the three maintenance scripts still **read exclusively from CSV** to detect and
fill gaps.

This creates a critical blind spot:

```
make_forecast.py fails before CSV write
    → CSV has no row for that forecast_date
    → fill_ml_gaps.py reads CSV, sees no gap (dates look continuous)
    → recalculate_nan_forecasts.py reads CSV, finds no flagged rows
    → postprocessing_maintenance.py reads API — but API is also empty
    → EM ensemble never created for that pentad
    → Dashboard shows LR only; ML/EM silently missing
```

LR is unaffected because it does not depend on these CSV files.

**Related issues:**
- INFRA-007: ML write/read architecture alignment (Phases 1-3 done, focused on
  postprocessing reader side) — **this issue completes the ML module side**
- ML-001: Maintenance mode hindcast FileNotFoundError (separate but related)
- ML-002: Root cause investigation of hindcast subprocess failures (open)

## Problem

Three scripts detect gaps and NaN values by reading from local CSV files:

| Script | CSV read | Purpose |
|--------|----------|---------|
| `fill_ml_gaps.py:194` | `pentad_{MODEL}_forecast.csv` | Find missing forecast_dates |
| `recalculate_nan_forecasts.py:188` | same CSV | Find rows with flag=1/2 |
| `add_new_station.py:159,164` | both pentad and decad CSVs | Determine hindcast date range for new stations |

All three scripts **write** results back to the API after gap-filling
(`fill_ml_gaps.py:287-294`, `recalculate_nan_forecasts.py:308-311`), but their
gap **detection** is CSV-only. If `make_forecast.py` crashes before writing to CSV,
none of these scripts can detect the missed forecast date.

Additionally, there is no diagnostic logging when gap detection finds nothing — a
silent "no gaps found" when the CSV is simply missing dates is indistinguishable from
a genuine all-good state.

## Desired Outcome

- All three maintenance scripts read forecast data from the API first, falling back
  to CSV only when the API is unavailable
- Gap detection in `fill_ml_gaps.py` operates on API data, so dates missing from
  CSV but absent from the API are also detected
- `recalculate_nan_forecasts.py` detects flag=1/2 rows from API records
- `add_new_station.py` derives the hindcast date range from API records
- Diagnostic `WARNING` log when API returns no records (so operators know gap
  detection had nothing to work with)
- Existing CSV fallback preserved so scripts continue to work in offline/dev environments

---

## Technical Analysis

### Current Implementation

**`fill_ml_gaps.py` (main gap-detection loop):**
```
Line 194: forecast = pd.read_csv(forecast_path)        ← CSV-only read
Lines 203-218: iterate forecast.code.unique()
    Line 207: forecast_dates = forecast_code['forecast_date'].unique()
    Line 214: if (forecast_dates[i] - forecast_dates[i-1]).days > 1:  ← gap trigger
Lines 287-294: _write_ml_forecast_to_api(filled_df, ...)  ← API write (already there)
```

**`recalculate_nan_forecasts.py` (NaN/flag detection):**
```
Line 188: forecast = pd.read_csv(forecast_path)        ← CSV-only read
Lines 199-215: parse dates, drop duplicates
Line 222: nan_values = forecast_code[forecast_code['flag'].isin([1,2])]
Lines 308-311: _write_ml_forecast_to_api(hindcast, ...)  ← API write (already there)
```

**`add_new_station.py` (date range for new-station hindcast):**
```
Line 159: decad_forecast = pd.read_csv(...)            ← CSV-only read
Line 164: pentad_forecast = pd.read_csv(...)           ← CSV-only read
Lines 170-171: start_date = min(min(decad_forecast['forecast_date']), ...)
               end_date   = max(max(decad_forecast['forecast_date']), ...)
```

**Existing API write infrastructure (reference):**
- `scr/utils_ml_forecast.py:521` — `_write_ml_forecast_to_api()` — full write pattern
- `scr/utils_ml_forecast.py:689` — `_check_ml_forecast_consistency()` — reads back
  from API after write using `SapphirePostprocessingClient`; use as reference for
  the new read function

**What the API stores:**
- ML forecasts stored with `horizon_type="day"`, one row per daily target date
- Columns: `code`, `date` (forecast_date/issue date), `target`, `model_type`, `flag`,
  `q05`, `q25`, `q75`, `q95`, `forecasted_discharge` (= Q50)
- To reconstruct gap-detection data: group by `(code, date)` to get unique
  `forecast_date` values per model

**Key files:**
- `apps/machine_learning/fill_ml_gaps.py:194` — CSV read to replace
- `apps/machine_learning/recalculate_nan_forecasts.py:188` — CSV read to replace
- `apps/machine_learning/add_new_station.py:159,164` — CSV reads to replace
- `apps/machine_learning/scr/utils_ml_forecast.py:521` — write pattern (reference)
- `apps/machine_learning/scr/utils_ml_forecast.py:689` — read-back pattern (reference)

---

## Implementation Plan

### Approach

Add a single `_read_ml_forecasts_from_api()` function to `utils_ml_forecast.py`,
then update each of the three scripts to call it first and fall back to CSV only
when the API is unavailable or returns empty. This keeps changes local to the ML
module and follows the existing write-side pattern.

**Rejected alternative — rewrite gap detection to use postprocessing reader:**
The postprocessing module's `data_reader.py` already reads ML forecasts from API,
but importing it into the ML module would create a cross-module dependency against
the project's architecture conventions. Keep the ML module self-contained.

### Files to Modify

| File | Changes |
|------|---------|
| `apps/machine_learning/scr/utils_ml_forecast.py` | Add `ML_MODEL_TYPE_MAP`, `_read_ml_forecasts_from_api()`; update `_write_ml_forecast_to_api()` to use shared map |
| `apps/machine_learning/fill_ml_gaps.py` | API-primary read + diagnostic logging |
| `apps/machine_learning/recalculate_nan_forecasts.py` | API-primary read + diagnostic logging |
| `apps/machine_learning/add_new_station.py` | API-primary read for date range |

### Implementation Steps

- [ ] **Step 1: Add `_read_ml_forecasts_from_api()` to `utils_ml_forecast.py`**

  New function that reads raw daily ML forecasts from the API and returns a
  DataFrame in the same schema as the CSV files (columns: `code`, `date`,
  `forecast_date`, `flag`, `Q5`, `Q25`, `Q50`, `Q75`, `Q95`).

  Use `SapphirePostprocessingClient` (already imported at line 49). The client's
  `read_short_term_forecasts()` method (in `short_term.py:40-96`) already supports
  all required filters: `horizon`, `model`, `code`, `start_date`, `end_date`, `limit`.

  **Verified (2026-03-05)**: API-002 identified missing parameters, but the
  installed `sapphire-api-client` now has all of them. No blocker.

  ```python
  # Module-level constant shared by _read and _write functions.
  # Maps internal model names (uppercase) to API ModelType values.
  ML_MODEL_TYPE_MAP = {"TFT": "TFT", "TIDE": "TiDE", "TSMIXER": "TSMixer"}

  _API_PAGE_SIZE = 5000  # rows per page; balances request count vs. payload size

  def _read_ml_forecasts_from_api(
      model_type: str,
      horizon_type: str,
      start_date: str | None = None,
      end_date: str | None = None,
      code: str | None = None,
  ) -> pd.DataFrame:
      """Read ML forecasts from the SAPPHIRE postprocessing API.

      Paginates automatically — safe for large result sets.

      Returns a DataFrame with columns matching the CSV schema:
          code, date (target), forecast_date, flag, Q5, Q25, Q50, Q75, Q95.
      Returns an empty DataFrame if the API is unavailable or returns no records.

      Args:
          model_type: "TFT", "TIDE", or "TSMIXER"
          horizon_type: "pentad" or "decade" — used only for log messages
              (API query always uses horizon="day" since ML stores daily targets)
          start_date: ISO date string for forecast_date (issue date) filter
          end_date: ISO date string for forecast_date (issue date) filter
          code: station code to filter; None means all codes

      Returns:
          DataFrame or empty DataFrame on failure.
      """
      if not SAPPHIRE_API_AVAILABLE:
          logger.warning(
              "sapphire-api-client not installed; cannot read ML forecasts from API"
          )
          return pd.DataFrame()

      api_enabled = os.getenv("SAPPHIRE_API_ENABLED", "true").lower() == "true"
      if not api_enabled:
          return pd.DataFrame()

      api_url = os.getenv("SAPPHIRE_API_URL", "http://localhost:8000")
      client = SapphirePostprocessingClient(base_url=api_url)

      try:
          if not client.readiness_check():
              logger.warning(
                  "SAPPHIRE API at %s is not ready; cannot read ML forecasts",
                  api_url,
              )
              return pd.DataFrame()
      except Exception as exc:
          logger.warning("SAPPHIRE API readiness check failed: %s", exc)
          return pd.DataFrame()

      api_model_type = ML_MODEL_TYPE_MAP.get(model_type.upper(), model_type)

      # Paginate to avoid silent truncation
      pages: list[pd.DataFrame] = []
      skip = 0
      try:
          while True:
              page = client.read_short_term_forecasts(
                  horizon="day",
                  model=api_model_type,
                  code=code,
                  start_date=start_date,
                  end_date=end_date,
                  skip=skip,
                  limit=_API_PAGE_SIZE,
              )
              if page.empty:
                  break
              pages.append(page)
              if len(page) < _API_PAGE_SIZE:
                  break  # last page
              skip += _API_PAGE_SIZE
      except Exception as exc:
          logger.warning("Failed to read ML forecasts from API: %s", exc)
          return pd.DataFrame()

      if not pages:
          return pd.DataFrame()

      df = pd.concat(pages, ignore_index=True)
      logger.info(
          "Read %d %s %s forecast rows from API",
          len(df), model_type, horizon_type,
      )

      # Rename API columns → CSV schema.
      # CSV convention: "forecast_date" = issue date, "date" = target date.
      # API convention: "date" = issue date, "target" = target date.
      df = df.rename(columns={
          "date": "forecast_date",
          "target": "date",
          "q05": "Q5",
          "q25": "Q25",
          "forecasted_discharge": "Q50",
          "q75": "Q75",
          "q95": "Q95",
      })
      df["forecast_date"] = pd.to_datetime(df["forecast_date"])
      df["date"] = pd.to_datetime(df["date"])
      return df
  ```

  **Verified (2026-03-05)**: `SapphirePostprocessingClient.read_short_term_forecasts()`
  accepts `horizon`, `model`, `code`, `start_date`, `end_date`, `skip`, `limit`
  (see `short_term.py:40-96`). All required parameters are available.

- [ ] **Step 2: Update `fill_ml_gaps.py` — API-primary read with CSV fallback**

  Replace the CSV read at line 194 with API-primary + fallback. Add `WARNING`
  logging when both sources return empty.

  ```python
  # --- Read existing forecasts (API-primary, CSV fallback) ---
  # Bound the query to the last 2 years to keep the request fast.
  # Gap detection only needs recent history; older gaps are stale.
  from datetime import timedelta
  api_start = (date.today() - timedelta(days=730)).isoformat()

  forecast = _read_ml_forecasts_from_api(
      model_type=MODEL_TO_USE,
      horizon_type=prefix,  # "pentad" or "decad"
      start_date=api_start,
  )

  if forecast.empty:
      logger.warning(
          "fill_ml_gaps: API returned no %s %s forecasts — falling back to CSV",
          MODEL_TO_USE, prefix,
      )
      try:
          forecast = pd.read_csv(forecast_path)
          forecast["forecast_date"] = pd.to_datetime(forecast["forecast_date"])
          forecast["date"] = pd.to_datetime(forecast["date"])
      except FileNotFoundError:
          logger.warning(
              "fill_ml_gaps: No CSV found at %s either — no gap detection possible",
              forecast_path,
          )
          return

  if forecast.empty:
      logger.warning(
          "fill_ml_gaps: Both API and CSV returned no data for %s %s. "
          "Gap detection skipped — forecasts may be missing.",
          MODEL_TO_USE, prefix,
      )
      return
  ```

  The gap-detection loop (lines 203-218) and hindcast trigger (line 257) remain
  unchanged — they operate on the DataFrame regardless of its source.

- [ ] **Step 3: Update `recalculate_nan_forecasts.py` — API-primary read with fallback**

  Same pattern at line 188. The flag detection loop (lines 217-230) is unchanged.

  Add `WARNING` when API returns no records — a silent "no NaN forecasts to
  recalculate" when the API is simply empty is a diagnostic gap.

  ```python
  from datetime import timedelta
  api_start = (date.today() - timedelta(days=730)).isoformat()

  forecast = _read_ml_forecasts_from_api(
      model_type=MODEL_TO_USE,
      horizon_type=prefix,
      start_date=api_start,
  )

  if forecast.empty:
      logger.warning(
          "recalculate_nan_forecasts: API returned no %s %s forecasts — "
          "falling back to CSV",
          MODEL_TO_USE, prefix,
      )
      try:
          forecast = pd.read_csv(forecast_path)
      except FileNotFoundError:
          logger.error("No forecast file found (API and CSV both empty)")
          return

  if forecast.empty:
      logger.warning(
          "recalculate_nan_forecasts: Both API and CSV empty for %s %s. "
          "NaN recalculation skipped.",
          MODEL_TO_USE, prefix,
      )
      return
  ```

- [ ] **Step 4: Update `add_new_station.py` — API-primary date range**

  `add_new_station.py` reads both CSVs (lines 159, 164) only to compute
  `start_date` and `end_date` for the hindcast (lines 170-171). Replace with
  API queries that return the min/max `forecast_date` across pentad and decad.

  ```python
  def _get_forecast_date_range_from_api(
      model_type: str,
  ) -> tuple[str | None, str | None]:
      """Return (start_date, end_date) as ISO strings from API records.

      Uses two small queries (limit=1 each, sorted by date) per horizon to
      find the oldest and newest forecast_date without fetching all rows.

      Returns (None, None) if the API is unavailable or has no records.
      """
      from scr.utils_ml_forecast import (
          ML_MODEL_TYPE_MAP,
          _read_ml_forecasts_from_api,
      )

      # Fetch a small sample (oldest + newest) per horizon to find the range.
      # _read_ml_forecasts_from_api paginates, but we bound with limit=1-like
      # queries. Since the API doesn't support order_by, we use a broad
      # start_date (earliest plausible) and a bounded end_date (today).
      all_dates = []
      for horizon in ("pentad", "decade"):
          df = _read_ml_forecasts_from_api(
              model_type=model_type,
              horizon_type=horizon,
              # Fetch a manageable window — the first page gives us the oldest
              # dates returned by the API's default ordering.
          )
          if not df.empty and "forecast_date" in df.columns:
              all_dates.append(df["forecast_date"].min())
              all_dates.append(df["forecast_date"].max())

      if not all_dates:
          return None, None
      return (
          pd.Timestamp(min(all_dates)).strftime("%Y-%m-%d"),
          pd.Timestamp(max(all_dates)).strftime("%Y-%m-%d"),
      )
  ```

  **Note**: This fetches all records (paginated) because the API does not
  support `order_by`. For `add_new_station.py` this is acceptable — it runs
  rarely (only when onboarding a station) and the total record count per model
  is bounded. If performance becomes an issue, add `order_by` support to the
  API client (tracked separately).

  Then in `main()`, replace lines 159-171 with:

  ```python
  start_date, end_date = _get_forecast_date_range_from_api(MODEL_TO_USE)

  if start_date is None:
      logger.warning(
          "add_new_station: No existing forecasts in API for %s — "
          "falling back to CSV for date range",
          MODEL_TO_USE,
      )
      # CSV fallback (original logic)
      file_name = f'decad_{MODEL_TO_USE}_forecast.csv'
      decad_forecast = pd.read_csv(
          os.path.join(OUTPUT_PATH_DISCHARGE, file_name), parse_dates=['date']
      )
      decad_forecast['forecast_date'] = pd.to_datetime(decad_forecast['forecast_date'])
      file_name = f'pentad_{MODEL_TO_USE}_forecast.csv'
      pentad_forecast = pd.read_csv(
          os.path.join(OUTPUT_PATH_DISCHARGE, file_name), parse_dates=['date']
      )
      pentad_forecast['forecast_date'] = pd.to_datetime(pentad_forecast['forecast_date'])
      start_date = min(
          min(decad_forecast['forecast_date']), min(pentad_forecast['forecast_date'])
      ).strftime('%Y-%m-%d')
      end_date = max(
          max(decad_forecast['forecast_date']), max(pentad_forecast['forecast_date'])
      ).strftime('%Y-%m-%d')
  ```

- [ ] **Step 5: Write tests**

  Add to `apps/machine_learning/test/test_api_integration.py` (follow the
  existing class structure in that file).

  New test class `TestReadMLForecastsFromAPI`:

  - `test_returns_empty_when_api_unavailable` — mock `SAPPHIRE_API_AVAILABLE=False`,
    verify returns empty DataFrame
  - `test_returns_empty_when_api_disabled` — `SAPPHIRE_API_ENABLED=false`,
    verify returns empty DataFrame
  - `test_returns_dataframe_on_success` — mock `client.get_forecasts()` returning
    sample records, verify column renaming (forecast_date, date, Q5..Q95)
  - `test_returns_empty_on_api_exception` — mock `client.get_forecasts()` raising
    exception, verify returns empty DataFrame (not raises)

  Update `fill_ml_gaps` and `recalculate_nan_forecasts` unit tests to verify:
  - `test_gap_detection_uses_api_first` — mock API returning records, verify
    CSV is not read
  - `test_gap_detection_falls_back_to_csv` — mock API returning empty, verify
    CSV is read
  - `test_gap_detection_logs_warning_when_both_empty` — mock both empty, verify
    `WARNING` log emitted and function returns early

### Code Examples

See the code blocks in Steps 1-4 above. Each step includes the complete
replacement snippet with the exact location (line number) of the code it replaces.

---

## Testing

### Test Cases

- [ ] `test_returns_empty_when_api_unavailable` — no sapphire-api-client installed
- [ ] `test_returns_empty_when_api_disabled` — `SAPPHIRE_API_ENABLED=false`
- [ ] `test_returns_dataframe_on_success` — mocked API, verify column renaming
- [ ] `test_returns_empty_on_api_exception` — API raises, returns empty (no crash)
- [ ] `test_paginates_large_result_sets` — mock returns `_API_PAGE_SIZE` rows on
  first call, fewer on second; verify both pages concatenated
- [ ] `test_readiness_check_exception_returns_empty` — `readiness_check()` raises,
  returns empty (no crash)
- [ ] `test_fill_ml_gaps_uses_api_first` — API mock returns data, CSV not read
- [ ] `test_fill_ml_gaps_falls_back_to_csv` — API empty, CSV read
- [ ] `test_fill_ml_gaps_warns_when_both_empty` — both empty, WARNING logged
- [ ] `test_recalculate_nan_uses_api_first` — same pattern as fill_ml_gaps
- [ ] `test_recalculate_nan_falls_back_to_csv` — fallback works
- [ ] `test_add_new_station_date_range_from_api` — correct min/max dates returned
- [ ] `test_add_new_station_date_range_falls_back_to_csv` — fallback works
- [ ] Existing tests in `test_api_integration.py` all pass (backward compatible)

### Testing Commands

```bash
# Full module test suite (zero skips expected except SAPPHIRE_API_AVAILABLE guard)
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning

# Focused run during development (not for CI)
cd apps/machine_learning
SAPPHIRE_TEST_ENV=True uv run pytest test/ -v -k "api"
```

### Manual Verification

1. Start the SAPPHIRE services: `cd sapphire && docker-compose up -d`
2. Run the ML operational pipeline for one model/horizon to populate API:
   ```bash
   SAPPHIRE_MODEL_TO_USE=TFT SAPPHIRE_PREDICTION_MODE=PENTAD \
     ieasyhydroforecast_env_file_path=~/config/.env \
     bash apps/run_locally.sh machine_learning
   ```
3. Run `fill_ml_gaps.py` and verify logs show "Reading ML forecasts from API"
   (not "falling back to CSV")
4. Delete a date from the API (or insert a gap manually) and verify
   `fill_ml_gaps.py` detects and fills it
5. With API down (`docker-compose stop api-gateway`), verify CSV fallback is used
   and a WARNING is logged

---

## Documentation Impact

- [ ] `apps/machine_learning/README.md` — update data flow description to note
  API-primary reads for maintenance scripts
- [ ] No other documentation impact — this is an internal reliability fix with
  no user-facing behavior change

---

## Out of Scope

- **Migrating `make_forecast.py` reads**: `make_forecast.py` reads old forecasts
  from CSV only to check for existing data before appending (lines 181, 224, 645,
  650). This is a separate concern from gap detection and can be addressed later.
- **Migrating `hindcast_ML_models.py` input reads**: ERA5/temperature/static CSV
  reads (lines 281-357) are model inputs, not forecast outputs. These come from
  the preprocessing pipeline, not the postprocessing API. Out of scope.
- **Sentinel rows in `make_forecast.py` on failure**: Writing `flag=2` rows when
  `make_forecast.py` crashes was discussed as a complementary fix. Track as ML-004
  if needed after this issue is complete.
- **Postprocessing pipeline silent-drop logging** (Gates 1-5 in
  `data_reader.py` and `ensemble_calculator.py`): Separate issue for the
  postprocessing module. Track as PP-021 or similar.
- **`sapphire-api-client` param gaps**: API-002 is resolved — all required
  filter parameters exist in the installed client (verified 2026-03-05).

## Design Notes (from review, 2026-03-05)

1. **Pagination over fixed limit**: `_read_ml_forecasts_from_api()` paginates
   with `skip`/`limit` instead of a single `limit=10000` call. This prevents
   silent truncation as the forecast database grows.
2. **Bounded queries for gap detection**: `fill_ml_gaps.py` and
   `recalculate_nan_forecasts.py` pass `start_date` (last ~2 years) to avoid
   fetching the entire forecast history. Older gaps are stale and not actionable.
3. **Shared model type map**: `ML_MODEL_TYPE_MAP` is a module-level constant
   used by both `_read_ml_forecasts_from_api()` and `_write_ml_forecast_to_api()`
   to avoid duplicate mappings that could drift.
4. **Readiness check wrapped in try/except**: Network failures during the
   readiness check return an empty DataFrame instead of raising.
5. **`add_new_station.py` fetches all records**: Since this script runs rarely
   (new station onboarding only) and the API lacks `order_by`, it fetches all
   paginated records to find min/max dates. Acceptable tradeoff.
6. **Column rename convention**: CSV uses `forecast_date` = issue date and
   `date` = target date. API uses `date` = issue date and `target` = target
   date. The rename block in Step 1 includes inline comments documenting this.

## Dependencies

- **API-002** (resolved): `SapphirePostprocessingClient.read_short_term_forecasts()`
  now supports all required filters: `horizon`, `model`, `code`, `start_date`,
  `end_date`, `limit`. Verified 2026-03-05 in `short_term.py:40-96`. **No blocker.**
- **INFRA-007** (informational): The target architecture (ML writes `horizon_type="day"`,
  postprocessing aggregates) is already implemented. This issue reads from that
  same `horizon_type="day"` data.

## Acceptance Criteria

- [ ] `_read_ml_forecasts_from_api()` in `utils_ml_forecast.py` reads daily ML
  forecasts from the API and returns a DataFrame in CSV-compatible schema
- [ ] API reads paginate automatically — no silent truncation at `limit`
- [ ] `ML_MODEL_TYPE_MAP` constant shared between read and write functions
- [ ] `fill_ml_gaps.py` and `recalculate_nan_forecasts.py` bound API queries
  with `start_date` (last ~2 years) to keep requests fast
- [ ] `fill_ml_gaps.py` reads from API first; only reads CSV when API empty/unavailable
- [ ] `recalculate_nan_forecasts.py` reads from API first; CSV fallback preserved
- [ ] `add_new_station.py` derives hindcast date range from API; CSV fallback preserved
- [ ] `WARNING` logged (not silently ignored) when API returns no records
- [ ] All existing tests in `test_api_integration.py` pass
- [ ] New tests cover API-primary, fallback, and empty-both-sources paths
- [ ] Manual verification: gap detected via API when CSV is missing a date
- [ ] Manual verification: CSV fallback works when API is down

---

## References

- INFRA-007 (Review): `doc/plans/issues/gi_draft_fix_ml_forecast_api_reader.md`
- ML-001 (Draft): `doc/plans/issues/gi_draft_ml_maintenance_hindcast_file_not_found.md`
- API-002 (Draft): `doc/plans/issues/gi_draft_api_client_missing_params.md`
- Existing write pattern: `apps/machine_learning/scr/utils_ml_forecast.py:521`
- Read-back reference: `apps/machine_learning/scr/utils_ml_forecast.py:689`
- Gap detection root cause analysis: investigation in conversation 2026-03-05
