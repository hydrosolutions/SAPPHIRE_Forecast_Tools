# PREPG ERA5 Interior Gap Memo

Date: 2026-06-19

Scope: empirical investigation of missing ERA5 T/P days inside the ML
covariate lookback window for the tjhm deployment. Real station and HRU codes
are intentionally omitted; examples use sentinel code `19999`.

## Findings

### Q1: Where The Missing Days Enter

The bad dates are absent in the raw Data Gateway ERA5 response, not introduced
by the local transform, quantile mapping, or DB write.

Evidence:

- A fresh DG ERA5 probe for `19999`, `2026-05-07..2026-05-09`, returned
  metadata plus rows for `2026-05-07` and `2026-05-09`, but no
  `2026-05-08`.
- A fresh DG ERA5 probe for `19999`, `2026-05-27..2026-05-29`, returned
  rows for `2026-05-27` and `2026-05-29`, but no `2026-05-28`.
- Exact one-day DG ERA5 probes for `2026-05-08` and `2026-05-28` returned
  zero data rows after metadata.
- The local raw operational gateway CSV has the same interior skips: neighboring
  dates are present, the two gap dates are not.
- The quantile-mapped control-member T/P CSVs also skip those dates.
- The preprocessing API contains current-year dashboard records for those
  dates, but with `value = null`, because `extend_era5_reanalysis.py` creates a
  full current-year daily norm frame and left-merges the operational values.

Interpretation: this is not a station-specific transform problem, not a
date-range off-by-one, and not a post-DB-write nulling bug. The most likely
cause is an upstream DG/CDS ERA5 ingestion/cache gap that was never backfilled.
As of this investigation, the missing dates were still unavailable through the
configured DG ERA5 endpoint, so operator-side upstream repair is required if
the real ERA5 values are desired.

### Q2: Operational Gateway Backfill Behavior

Operational gateway does not actively backfill interior missing days.

Relevant code:

- `apps/preprocessing_gateway/Quantile_Mapping_OP.py:673-679` builds a rolling
  one-year operational request window.
- `apps/preprocessing_gateway/Quantile_Mapping_OP.py:713-715` downloads the
  operational spinup/forecast CSV.
- `apps/preprocessing_gateway/dg_utils.py:187-215` transforms only rows present
  in that CSV. It does not reindex to daily continuity.
- `apps/preprocessing_gateway/Quantile_Mapping_OP.py:760-768` forward-fills
  NaN cells that already exist, but cannot fill a date row that was never
  emitted.
- `apps/preprocessing_gateway/Quantile_Mapping_OP.py:770-771` writes the
  resulting sparse control-member CSVs.
- `apps/preprocessing_gateway/extend_era5_reanalysis.py:201-206` skips raw
  reanalysis API writes in operational mode.
- `apps/preprocessing_gateway/extend_era5_reanalysis.py:448-465` keeps only
  operational rows older than the 195-day stability cutoff before extending the
  reanalysis CSV.
- `apps/preprocessing_gateway/extend_era5_reanalysis.py:482-484` lets stable
  operational rows overwrite overlapping reanalysis rows by `date, code`, but
  only for rows that exist.

Operational can extend or overwrite stable overlap rows, but it does not detect
interior gaps, does not request missing exact dates, and does not synthesize
missing rows.

### Q3: Maintenance Design And Observed Run

There are two maintenance paths with different behavior:

- `apps/run_locally.sh:738-746` runs only `extend_era5_reanalysis.py`.
- `apps/run_locally.sh:440-444` passes the env file and prediction mode, but
  not `SAPPHIRE_SYNC_MODE=maintenance`.
- `apps/run_locally.sh:1845-1849` dispatches `maintenance:preprocessing_gateway`
  to that local function.
- `bin/daily_gateway_maintenance.sh:97-111` does set
  `SAPPHIRE_SYNC_MODE=maintenance` for the Docker maintenance container.
- `apps/pipeline/pipeline_docker.py:1679-1699` also sets
  `SAPPHIRE_SYNC_MODE=maintenance` for `GatewayMaintenance`.

Observed tjhm local run:

- Command: local `maintenance:preprocessing_gateway` with the tjhm env file.
- Result: passed.
- It wrote 6,205 P dashboard meteo records and 6,205 T dashboard meteo records
  to the preprocessing API. That is 365 current-year days times 17 stations.
- It did not write raw reanalysis records, consistent with the local runner not
  setting `SAPPHIRE_SYNC_MODE=maintenance`.
- Rechecking the sampled API records after the run still showed the same two
  null dates.
- Rechecking the control-member forcing CSV still showed neighboring dates but
  no rows for the two gap dates.
- The reanalysis CSV still ended at the prior stable frontier. The 195-day
  stability filter means current May 2026 operational rows are not eligible for
  reanalysis extension on 2026-06-19.

Conclusion: the local maintenance target did not close the holes. The Docker
maintenance path is better wired for maintenance sync, but it still has no
explicit interior-gap backfill policy. If the DG response remains missing these
dates, production maintenance also cannot recover the real values.

### Q4: Why Quantile Mapping Drops The Days

`Quantile_Mapping_OP.py` does not explicitly drop those dates. It receives a
raw DG CSV in which the dates are absent, then transforms and quantile-maps only
the rows present.

The NaN handling at `apps/preprocessing_gateway/Quantile_Mapping_OP.py:760-768`
is cell-level forward fill after quantile mapping. It only handles existing rows
with missing values. There is no per-code daily reindex step, so absent dates
are never emitted for forcing.

The dashboard/API null rows come later from
`apps/preprocessing_gateway/extend_era5_reanalysis.py:514-533`, where daily
norms are mapped onto the full current year and current-year operational data
is left-merged. Missing operational dates therefore become rows with norm
present and value null.

## Durable Fix Candidates

### Gateway Interior-Null Backfill

Owner: `apps/preprocessing_gateway`.

Smallest robust change:

- Add a per-code daily continuity check for control-member T/P after the DG
  transform or after quantile mapping.
- For interior missing dates, first retry exact-day DG ERA5 fetches.
- If the exact-day fetch still returns no rows, either fail loudly with an
  actionable gap report or apply an explicit bounded fill policy.
- If filling operationally, group by code and fill only interior gaps. Do not
  use the current whole-DataFrame `ffill` across station boundaries.

This is the right layer to prevent sparse forcing CSVs/API records.

### Quantile Mapping No-Drop / Visibility

Owner: `apps/preprocessing_gateway`.

Smallest change:

- Reindex each code to a complete daily range and emit missing dates explicitly
  before writing forcing CSVs.
- If no fill policy is accepted, emit NaN rows and make the gap loud in logs and
  validation instead of silently omitting dates.

This improves observability, but by itself still leaves ML with NaN unless a
fill or retry policy also runs.

### ML Covariate Guard

Owner: `apps/machine_learning`, tracked with ML-002 Vector 9 / ML-015.

Smallest change:

- Before calling the Darts predictors, assert that target and covariate windows
  are finite across the full required lookback and forecast horizon.
- Either apply an ML-owned covariate fill policy or skip prediction with a
  clear remediation flag instead of letting model inference emit all-NaN output.

This is a defensive guard, not the primary data repair. Gateway owns forcing
completeness; ML owns not passing NaN covariates into models silently.

## DG Re-probe Confirmation (2026-06-19, follow-up)

Explicit re-probe of the configured DG ERA5 endpoint (control-member HRU; sentinel
`19999` used in examples), via `sapphire_dg_client … era5_land.get_era5_land(date, end_date)`:

- control range `2026-06-10..2026-06-12` → 3 data dates returned (client healthy).
- `2026-05-07..2026-05-09` → only `07.05` and `09.05` (08 absent).
- exact `2026-05-08` → 0 data rows (metadata header only).
- `2026-05-27..2026-05-29` → only `27.05` and `29.05` (28 absent).
- exact `2026-05-28` → 0 data rows (metadata header only).

(The DG CSV uses `DD.MM.YYYY` dates and carries ~7 metadata header rows —
`Category/Interpolat/Sensor/Unit/X/Y/Z` — which are not data dates.)

Conclusion: as of 2026-06-19 the two days remain **genuinely absent in DG/CDS** — confirmed
not a fetch-window or parse artifact (neighbors and a recent control range return fine). No
downstream code can synthesize them; upstream/operator backfill is required (WS-4).

## Operator Action

The two dates were still unavailable through the configured DG ERA5 endpoint on
2026-06-19. To recover true ERA5 values, repair or backfill the upstream DG/CDS
cache for those exact dates, then rerun the full gateway path that includes
`Quantile_Mapping_OP.py` followed by `extend_era5_reanalysis.py`.

Tracked as issue **ML-017** (`doc/plans/issues/high_prio_gi_draft_prepg_ml_era5_interior_gap_cascade.md`).
