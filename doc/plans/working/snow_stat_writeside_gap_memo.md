# Snow Stat Write-Side Gap Memo

**Audience:** preprocessing-gateway maintainer  
**Date:** 2026-06-01  
**Context:** Phase 0 of the dashboard snow percentile display plan returned
`DECISION: STOP` after confirming that every stat field in the `/snow/` API
endpoint is NULL across all snow types and all date windows. This memo explains
why those fields are never written, identifies the template to follow, and
frames what needs to be built.

---

## 1. TL;DR

- The `Snow` model carries ten stat columns (`mean`, `min`, `max`, `q05`,
  `q25`, `q50`, `q75`, `q95`, `previous`, `current`) that are fully defined in
  the schema but **have never been written by any code path**.
- The only snow writers — `write_snow_to_api` in
  `apps/preprocessing_gateway/dg_utils.py` and
  `recalculate_norms` in
  `apps/preprocessing_gateway/recalculate_snow_norms.py` — write
  `value`, `norm`, and `value1..value14` only. Stat fields are absent from both
  write paths.
- The hydrograph pipeline (`apps/iEasyHydroForecast/forecast_library.py`,
  `write_pentad_hydrograph_data`) computes identical stats inline during the
  forecast run and writes them via `_write_hydrograph_to_api`. A standalone
  periodic recalc script (analogous to `recalculate_snow_norms.py`) is the
  recommended shape for the snow side.

---

## 2. The Gap

### Schema / model — fields exist

All ten stat fields are declared on the `Snow` SQLAlchemy model at
`sapphire/services/preprocessing/app/models.py:144–160`:

```
count  (Integer, line 145)   mean  (Float, line 146)   std  (Float, line 147)
min    (Float, line 148)      max   (Float, line 149)
q05    (Float, line 152)      q25   (Float, line 153)   q50  (Float, line 154)
q75    (Float, line 155)      q95   (Float, line 156)
previous (Float, line 159)   current (Float, line 160)
```

All are `Optional` in `SnowBase` at
`sapphire/services/preprocessing/app/schemas.py:120–131`, which means the API
happily returns NULL rows without complaint.

### Write path 1 — `dg_utils.write_snow_to_api`

The record dict assembled at
`apps/preprocessing_gateway/dg_utils.py:696–715` contains only:

```python
record = {
    "snow_type": ...,
    "code": ...,
    "date": ...,
    "value": ...,
    "norm": ...,
}
# + value1..value14 added at lines 709-713
```

`mean`, `min`, `max`, `q05`, `q25`, `q50`, `q75`, `q95`, `previous`,
`current`, `count`, and `std` are **absent from this dict**. The upsert in
`sapphire/services/preprocessing/app/crud.py:254–258` writes whatever keys
are in `data.items()` and leaves the rest untouched (i.e., NULL), so these
fields remain NULL after every operational and reanalysis ingest.

### Write path 2 — `recalculate_snow_norms.recalculate_norms`

The record dict built at
`apps/preprocessing_gateway/recalculate_snow_norms.py:187–194` contains only:

```python
record = {
    "snow_type": ...,
    "code": ...,
    "date": ...,
    "value": ...,
    "norm": ...,
}
# + band_values (value1..value14) added at line 194
```

No stat fields. This script's sole purpose is to write the `norm` column;
it was never extended to also write percentiles or comparison values.

### Write path 3 — `SnowDataMigrator` in `data_migrator.py`

`sapphire/services/preprocessing/app/data_migrator.py:536–561` (class
`SnowDataMigrator`, method `prepare_day_data`) builds records with `value` and
`value1..value14` only — no `norm`, no stat fields. The migrator was used
during the initial CSV-to-DB migration and has the same gap.

### Write path 4 — `backfill_new_stations.backfill_snow_from_csv`

`apps/preprocessing_gateway/backfill_new_stations.py:256–272` writes
`value`, `norm`, and `value1..value14`. No stat fields.

**Conclusion:** There is no code path anywhere in the codebase that writes
`mean`, `min`, `max`, `q05`, `q25`, `q50`, `q75`, `q95`, `previous`,
`current`, `count`, or `std` to the `snow` table. These columns have been NULL
since the table was created.

---

## 3. What Writes Snow Rows Today

Two active paths:

1. **Operational/reanalysis ingest** — `write_snow_to_api` at
   `apps/preprocessing_gateway/dg_utils.py:524–727`. Called by
   `snow_data_operational.py` and `snow_data_renalysis.py` during the daily
   pipeline run. Writes `value`, `norm` (preserved from existing API rows if
   not incoming), and `value1..value14`.

2. **Yearly norm recalculation** — `recalculate_norms` at
   `apps/preprocessing_gateway/recalculate_snow_norms.py:39–217`. Designed to
   run once a year (end of August, after reanalysis refresh). Reads all
   historical `value` data from the API, computes the climatological mean per
   `(code, dayofyear)` via `calculate_snow_norms_from_api`
   (`dg_utils.py:379–476`), and writes back only `norm`. Stat fields are not
   computed and not written.

The service-layer upsert lives in
`sapphire/services/preprocessing/app/crud.py:235–281` (`create_snow`).
It is a standard fetch-then-update pattern: load existing rows by
`(snow_type, code, date)`, update changed fields, insert new rows. There is no
computation layer here — the service receives whatever the client sends.

---

## 4. What Writes Hydrograph Stat Rows Today (the Template)

The hydrograph stat population is **inline in the forecast run**, not in a
separate periodic job. The function `write_pentad_hydrograph_data` at
`apps/iEasyHydroForecast/forecast_library.py:4516` is called on every forecast
day by the linear-regression and machine-learning pipelines. It:

1. Filters the full historical discharge dataset to exclude the current year.
2. Groups by `(code, pentad_in_year)` and computes `mean`, `min`, `max`,
   `q05`, `q25`, `q75`, `q95` via `pd.NamedAgg` (lines 4588–4602).
3. Merges in `norm` from iEH HF or leaves NaN.
4. Merges the previous year and current year values as columns named by their
   year (e.g., `"2025"`, `"2026"`).
5. Calls `_write_hydrograph_to_api(runoff_stats, "pentad")` at line 4755.
6. `_write_hydrograph_to_api` (lines 3376–3535) maps the year columns to
   `previous` and `current` fields, rounds, and bulk-writes.

The equivalent decade path is `write_decad_hydrograph_data` (line 4807) and
the monthly path is `write_month_hydrograph_data` (line 5361).

**Key structural difference from snow:** hydrograph stats are re-computed and
re-written on every forecast run, from the full discharge history resident in
the `runoffs` table. Snow data does not go through a corresponding forecast-run
path — it is only ingested from external files. There is therefore no existing
hook to attach snow stat computation to; a standalone periodic script is
required.

The norm computation pattern in `calculate_snow_norms_from_api`
(`dg_utils.py:379–476`) — paginate all history, group by `(code, dayofyear)`,
aggregate — is the exact template to extend for percentiles and comparison
values.

---

## 5. Proposed Change Shape

- **Where:** A new standalone recalculation script in
  `apps/preprocessing_gateway/`, sibling to `recalculate_snow_norms.py`.
  Alternatively, extend `recalculate_snow_norms.py` to also compute and write
  stat fields in the same pass (avoids a second full API read per variable).
  The new computation helper should live in `dg_utils.py` alongside
  `calculate_snow_norms_from_api`.

- **Input data:** All historical `value` rows for each `(snow_type, code)`
  from the preprocessing API, paginated the same way
  `calculate_snow_norms_from_api` already does. Group by `dayofyear`. The
  `previous` field = value for `(code, dayofyear)` in the year before the
  target year; `current` = value in the target year.

- **Output:** For each `(snow_type, code, date)` row in the target year, write
  back `mean`, `min`, `max`, `count`, `std`, `q05`, `q25`, `q50`, `q75`,
  `q95`, `previous`, and `current`. Preserve `value`, `norm`, and band values
  already in the row (same preserve-then-overwrite pattern as
  `recalculate_snow_norms.py:174–194`).

- **Idempotency:** Must be safe to re-run. Upsert on `(snow_type, code, date)`
  — same contract as the existing snow write paths via `crud.create_snow`.
  Running it twice should produce identical database state.

---

## 6. Scope Estimate

**Medium.** The pagination, groupby, and upsert scaffolding already exists in
`dg_utils.calculate_snow_norms_from_api` and `recalculate_snow_norms.py`.
What is missing is: (a) a new aggregation step that computes percentiles and
comparison values from the same input, and (b) an extension of the record
builder to include those fields. No architectural change is required, but the
comparison-value logic (`previous` and `current`) needs care — it differs from
norm computation and must handle leap-year alignment and partial-year data.

---

## 7. Likely Owner(s)

Based on `git log --format="%an"`:

| File | Top contributor |
|------|----------------|
| `apps/preprocessing_gateway/dg_utils.py` | Beatrice Marti (5 commits), Sandro Hunziker (2) |
| `apps/preprocessing_gateway/recalculate_snow_norms.py` | Beatrice Marti (3 commits) |
| `apps/iEasyHydroForecast/forecast_library.py` (hydrograph template) | Beatrice Marti (136 commits) |
| `sapphire/services/preprocessing/app/models.py` | Maxat Pernebayev (9 commits) |

The gateway code (`dg_utils.py`, `recalculate_snow_norms.py`) is owned by
**Beatrice Marti**. The service model and schema (`models.py`, `schemas.py`,
`crud.py`) are owned by **Maxat Pernebayev**. This work crosses an ownership
boundary only if the stat fields need to be added to the service layer — but
they are already present in `models.py:144–160` and `schemas.py:120–131`, so
no service-side changes are expected. The implementation work falls entirely in
`apps/preprocessing_gateway/`.

---

## 8. Open Questions

1. **`previous` and `current` at write time vs. read time.** The hydrograph
   path computes `previous` and `current` at write time (during the forecast
   run) from year-named columns in the DataFrame. For snow, should these also be
   written once a year by a recalc script, or computed dynamically at query
   time in the dashboard? Write-time is simpler to implement and matches the
   hydrograph pattern, but means the fields go stale between recalc runs.

2. **Partial-year handling.** Should the recalc script compute stats for the
   current (incomplete) year, or only for complete climatological years? If the
   script runs in August, the current year has 8 months of data. Writing
   partial-year percentiles may mislead the dashboard's "current" line.

3. **Minimum years threshold.** Is there a floor on how many years of data are
   required before percentile columns should be written (vs. left NULL)? The
   hydrograph path has no explicit threshold; `calculate_snow_norms_from_api`
   logs `n_years` but does not gate on it. Snow data may have stations with
   only 1–2 years of history.

4. **Leap-year alignment for `dayofyear` grouping.** The hydrograph stat
   computation handles leap/non-leap year boundaries explicitly
   (`forecast_library.py:4561–4586`). Snow's `value` column is a daily
   observation; the same DOY-alignment question applies. Does the new
   computation need the same adjustment, or is a simpler `dt.dayofyear` group
   sufficient given that snow norms are already computed without this
   adjustment?

5. **Scope of initial backfill.** After implementing the recalc script, should
   a one-time backfill be run against all stations and all historical years, or
   only against the current year? The `recalculate_snow_norms.py` model writes
   only the target year per invocation; a full-history backfill would require
   looping over years.

---

## 9. References

- Phase 0 evidence:
  `doc/plans/working/snow_field_population_check.md`
- Dashboard plan (Phase 0 context and future phases):
  `doc/plans/issues/high_prio_gi_draft_dashboard_snow_percentile_display.md`
- `Snow` model:
  `sapphire/services/preprocessing/app/models.py:117–166`
- `SnowBase` / `SnowResponse` schema:
  `sapphire/services/preprocessing/app/schemas.py:100–143`
- Primary snow writer:
  `apps/preprocessing_gateway/dg_utils.py:524–727`
- Yearly norm recalc (template for the new script):
  `apps/preprocessing_gateway/recalculate_snow_norms.py`
- Norm computation helper (extend this for percentiles):
  `apps/preprocessing_gateway/dg_utils.py:379–476`
- Hydrograph stat computation (the template to mirror):
  `apps/iEasyHydroForecast/forecast_library.py:4516–4755`
- Hydrograph API write helper:
  `apps/iEasyHydroForecast/forecast_library.py:3376–3535`
