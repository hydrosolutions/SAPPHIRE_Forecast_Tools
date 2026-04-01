# GitHub Issue: FD-003

**Title**: `fix(forecast_dashboard): Duplicate forecast rows caused by skill metric merge fan-out`

**Labels**: `bug`, `forecast_dashboard`, `high-priority`

**Assignee**: @maxatp

**Status**: Draft

---

## Summary

The forecast dashboard displays duplicate forecast rows in the summary forecast panel (and the underlying forecasts data table). Each forecast row for a given (pentad, code, model) appears N times, where N is the number of skill metric recalculation dates stored for that combination in the database.

## Root Cause

The duplication happens during the LEFT merge of `forecasts_all` with `forecast_stats` in `src/db.py:get_data()` (line 436-440):

```python
data["forecasts_all"] = forecasts_all.merge(
    forecast_stats,
    on=["code", hin, "model_short", "model_long"],
    how="left",
    suffixes=("", "_stats"),
)
```

The problem is in `get_forecast_stats()` (line 386-401):

1. It fetches skill metrics spanning two years (`start_date=PREVIOUS_YEAR-12-31` to `end_date=CURRENT_YEAR-12-31`)
2. The skill_metrics table stores **one row per recalculation date** per `(horizon_type, code, model_type, horizon_in_year)` — this is by design, since skill metrics are recalculated periodically and the `date` field tracks when
3. `get_forecast_stats()` **drops the `date` column** at line 400 before returning
4. After dropping `date`, multiple skill metric rows become identical on the merge key `["code", pentad_in_year, "model_short", "model_long"]`
5. The LEFT merge multiplies each forecast row by the number of matching skill metric rows

**Example**: If pentad 18 for station 15013 / model TFT has 3 skill metric entries (recalculated on 2026-01-15, 2026-02-15, 2026-03-15), every forecast row for that combination appears 3 times.

### Verification

The postprocessing API write layer is **not** the source of duplicates:
- The `forecasts` table has a unique constraint on `(horizon_type, code, model_type, date, target)` (see `models.py:94-101`)
- The API uses upsert logic in `crud.py:create_forecast()` (lines 16-62) — it queries for existing records by unique key and updates rather than inserting duplicates
- The client-side writer in `api_writer.py:368-380` also deduplicates before sending

## Affected Components

| Component | File | Lines | Impact |
|-----------|------|-------|--------|
| Skill metric fetch | `src/db.py` | `get_forecast_stats()`, 386-401 | Returns multiple rows per merge key after dropping `date` |
| Merge in orchestrator | `src/db.py` | `get_data()`, 436-440 | Fan-out: 1 forecast x N skill rows = N duplicate rows |
| Summary table | `src/vizualization.py` | `create_forecast_summary_table()`, 2906-2996 | Displays all duplicates — no dedup before rendering |
| Hydrograph plots | `src/vizualization.py` | `plot_pentad_forecast_hydrograph_data()`, 2651-2800 | Overlapping plot lines (visual but harmless) |

Note: `src/processing.py` has commented-out `drop_duplicates()` calls at lines 518, 609, 625 — these were a previous attempt to handle this that was removed.

## Proposed Fix

In `get_forecast_stats()`, keep only the **latest** skill metric per `(code, pentad_in_year, model_short)` before dropping the `date` column:

```python
@_timed
def get_forecast_stats(horizon, station) -> pd.DataFrame:
    code = _resolve_station(station)
    df = _read_data("postprocessing", "skill-metric", {
        "horizon": horizon,
        "code": code,
        "start_date": f"{PREVIOUS_YEAR}-12-31",
        "end_date": f"{CURRENT_YEAR}-12-31",
        "limit": 1000,
    })
    hin = _horizon_in_year_col(horizon)
    df.rename(columns={
        "horizon_in_year": hin,
        "model_type": "model_short",
        "model_type_description": "model_long",
    }, inplace=True)

    # Keep only the most recent skill metric per (code, period, model)
    # to prevent fan-out when merging with forecasts_all in get_data().
    if "date" in df.columns and not df.empty:
        df = df.sort_values("date").drop_duplicates(
            subset=["code", hin, "model_short"], keep="last"
        )

    df.drop(columns=["horizon_type", "date", "id"], inplace=True, errors="ignore")
    return _convert_na_to_nan(df)
```

This ensures a 1:1 relationship on the merge key, eliminating row multiplication.

## Alternative Considered

Adding `drop_duplicates()` in `create_forecast_summary_table()` after the merge — but this treats the symptom rather than the cause. The fan-out in `get_data()` would still inflate memory and slow down all downstream consumers of `forecasts_all`.

## Tasks

- [ ] Add `sort_values("date").drop_duplicates(subset=..., keep="last")` in `get_forecast_stats()` before dropping `date`
- [ ] Verify fix locally: run `panel serve` and confirm summary table shows one row per model
- [ ] Check that skill metric values shown match the latest recalculation (not an older one)
- [ ] Run tests: `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard`
