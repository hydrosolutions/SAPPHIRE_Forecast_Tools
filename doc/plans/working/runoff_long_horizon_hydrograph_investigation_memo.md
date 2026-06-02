# Long-Horizon Runoff Hydrograph Investigation Memo

## TL;DR

- Monthly hydrograph norm ingest already exists, but it writes **norm-only** rows: `forecast_library.write_month_hydrograph_data` builds 12 rows per site with `norm` and no year columns, so `_write_hydrograph_to_api` persists `previous=None` and `current=None` for month records.
- Quarterly and seasonal hydrograph triads are not implemented. The preprocessing API has a shared `/hydrograph/` endpoint and shared `norm` / `previous` / `current` fields, but `quarter` is not in the preprocessing `HorizonType` enum, and the iEasyHydroForecast hydrograph writer only accepts `pentad`, `decade`, or `month`.
- Scope estimate: **Medium-Large**. Month needs extension from norm-only to full triad; season can probably reuse the shared schema after writer/client updates; quarter needs coordination with the preprocessing service owner because the enum currently lacks `quarter`.

## Current state of long-horizon hydrograph endpoints

The preprocessing service exposes one shared hydrograph API, not separate per-horizon endpoints:

- `POST /hydrograph/` creates or updates bulk hydrograph rows via `crud.create_hydrograph` (`sapphire/services/preprocessing/app/main.py:101-108`).
- `GET /hydrograph/` accepts optional `horizon`, `code`, `start_date`, and `end_date` filters and delegates to `crud.get_hydrograph` (`sapphire/services/preprocessing/app/main.py:116-136`).
- `crud.get_hydrograph` converts the `horizon` string into `HorizonType(horizon)` before filtering (`sapphire/services/preprocessing/app/crud.py:136-145`), so the enum decides which horizons the API accepts.

The shared preprocessing enum supports:

- `day`
- `pentad`
- `decade`
- `month`
- `season`
- `year`

It does **not** support `quarter` (`sapphire/services/preprocessing/app/models.py:6-13`). That means:

- Month: endpoint exists and enum accepts the horizon.
- Quarter: endpoint exists generically, but `horizon=quarter` will fail enum conversion unless the service owner extends `HorizonType`.
- Season: endpoint exists and enum accepts the horizon.

The `Hydrograph` SQL model is a single shared table. Horizon is discriminated by `horizon_type`; rows are keyed by `(horizon_type, code, date)` via the unique constraint (`sapphire/services/preprocessing/app/models.py:41-78`). The long-horizon triad fields already exist on that shared model:

- `norm`
- `previous`
- `current`

Those fields are defined at `sapphire/services/preprocessing/app/models.py:70-73` and mirrored in the Pydantic schema at `sapphire/services/preprocessing/app/schemas.py:41-60`.

The dashboard does not currently read long-horizon hydrograph overlays for month/quarter/season:

- The generic short-horizon reader `get_hydrograph_pentad_all(horizon, station)` reads `/preprocessing/hydrograph/` for the supplied horizon and renames `previous` / `current` to current calendar-year labels (`apps/forecast_dashboard/src/db.py:190-217`).
- `get_data()` branches away for `month`, `quarter`, and `season` before the short-horizon block is used (`apps/forecast_dashboard/src/db.py:692-729`).
- `_get_data_monthly()` returns daily hydrograph data and an empty `hydrograph_pentad_all` DataFrame (`apps/forecast_dashboard/src/db.py:767-814`).
- `_get_data_quarter()` and `_get_data_season()` also return daily hydrograph data and empty `hydrograph_pentad_all` DataFrames (`apps/forecast_dashboard/src/db.py:834-856`, `apps/forecast_dashboard/src/db.py:859-880`).

So the current dashboard data loader is not yet consuming month/quarter/season hydrograph triad rows even where the backend can store them.

## Existing monthly-norm ingest

Monthly norm ingest is **present but partial**.

`apps/preprocessing_runoff/sync_monthly_norms.py` is explicitly a yearly monthly discharge norm ingestion script. Its docstring says it fetches monthly discharge norms from the iEasyHydro HF SDK and writes them to SAPPHIRE `hydrographs` with `horizon_type='month'`, 12 rows per site (`apps/preprocessing_runoff/sync_monthly_norms.py:1-12`).

The integration pattern is:

- import `write_month_hydrograph_data` from `forecast_library` (`apps/preprocessing_runoff/sync_monthly_norms.py:55-57`);
- initialize `IEasyHydroHFSDK()` (`apps/preprocessing_runoff/sync_monthly_norms.py:149-152`);
- load forecast-enabled sites from the HF SDK (`apps/preprocessing_runoff/sync_monthly_norms.py:155-160`);
- filter manual sites out before calling the norm endpoint (`apps/preprocessing_runoff/sync_monthly_norms.py:163-180`);
- delegate to `write_month_hydrograph_data(sdk_only_codes, sdk, current_year=args.current_year)` (`apps/preprocessing_runoff/sync_monthly_norms.py:216-229`).

The exact SDK call for monthly norms is:

`iehhf_sdk.get_norm_for_site(code, "discharge", norm_period="m")`

That call is documented in the function arguments (`apps/iEasyHydroForecast/forecast_library.py:5375-5381`) and executed at `apps/iEasyHydroForecast/forecast_library.py:5411-5415`.

However, `write_month_hydrograph_data` only creates rows with:

- `code`
- `date`
- `month`
- `month_in_year`
- `day_of_year`
- `norm`

The DataFrame is built at `apps/iEasyHydroForecast/forecast_library.py:5429-5438`. There are no year columns such as `2025` / `2026`, no `previous`, and no `current`.

Because `_write_hydrograph_to_api` derives `current` and `previous` only from 4-digit year columns (`apps/iEasyHydroForecast/forecast_library.py:3446-3456`, `apps/iEasyHydroForecast/forecast_library.py:3518-3526`), monthly rows written by this function persist `current=None` and `previous=None`.

Daily discharge history exists in `apps/preprocessing_runoff`:

- `preprocessing_runoff.py` obtains an iEasyHydro SDK object through `get_ieh_sdk()` (`apps/preprocessing_runoff/preprocessing_runoff.py:100-126`).
- The pipeline converts filtered daily discharge into hydrograph rows through `src.from_daily_time_series_to_hydrograph(...)` (`apps/preprocessing_runoff/preprocessing_runoff.py:427-433`).
- It then writes daily hydrograph data via `src.write_daily_hydrograph_data_to_csv(...)`, which also posts to the preprocessing API (`apps/preprocessing_runoff/preprocessing_runoff.py:471-479`).

The daily hydrograph generator groups historical discharge by normalized day-of-year and computes describe/percentile statistics (`apps/preprocessing_runoff/src/src.py:4125-4173`). It then merges current-year and last-year discharge into the hydrograph output (`apps/preprocessing_runoff/src/src.py:4175-4201`).

There is no equivalent monthly/quarterly/seasonal discharge-history aggregation in `apps/preprocessing_runoff` source, based on the source search. The only monthly long-horizon hydrograph source path found is the norm-only `sync_monthly_norms.py` + `write_month_hydrograph_data` path.

## Pentad hydrograph triad as template

`write_pentad_hydrograph_data` computes the short-horizon triad in `apps/iEasyHydroForecast/forecast_library.py`.

Statistics:

- It keeps only issue-date rows and drops raw `issue_date` / `discharge` columns (`apps/iEasyHydroForecast/forecast_library.py:4531-4535`).
- It recalculates `pentad` and `pentad_in_year` from `date + 1 day` (`apps/iEasyHydroForecast/forecast_library.py:4545-4553`).
- It excludes the current year for climatology statistics, groups by `(code, pentad_in_year)`, and computes mean/min/max and percentiles (`apps/iEasyHydroForecast/forecast_library.py:4588-4602`).

Norm:

- If iEH HF norm reading is enabled and an SDK is passed, it calls `iehhf_sdk.get_norm_for_site(code, "discharge", norm_period="p")` for each code (`apps/iEasyHydroForecast/forecast_library.py:4603-4625`).
- It expects 72 pentadal norm values and merges them onto `runoff_stats` by `(pentad_in_year, code)` (`apps/iEasyHydroForecast/forecast_library.py:4618-4642`).
- Otherwise it explicitly creates a `norm` column filled with `NaN` (`apps/iEasyHydroForecast/forecast_library.py:4643-4645`).

Previous/current:

- It identifies `last_year` and `current_year` from the latest date in the data (`apps/iEasyHydroForecast/forecast_library.py:4647-4652`).
- It shifts last-year dates forward one year and renames `discharge_avg` to the last-year column name (`apps/iEasyHydroForecast/forecast_library.py:4653-4666`).
- It renames current-year `discharge_avg` to the current-year column name (`apps/iEasyHydroForecast/forecast_library.py:4667-4669`).
- It merges both year columns into `runoff_stats` by `(code, pentad_in_year)` (`apps/iEasyHydroForecast/forecast_library.py:4681-4687`).

Dispatch:

- `_write_hydrograph_to_api` accepts `pentad`, `decade`, and `month` only, mapping each to its horizon-value columns (`apps/iEasyHydroForecast/forecast_library.py:3376-3444`).
- It detects 4-digit year columns and maps the latest to `current` and the second-latest to `previous` (`apps/iEasyHydroForecast/forecast_library.py:3446-3456`, `apps/iEasyHydroForecast/forecast_library.py:3518-3526`).
- It sends records through `client.write_hydrograph(records)` (`apps/iEasyHydroForecast/forecast_library.py:3530-3537`).
- The pentad writer calls `_write_hydrograph_to_api(runoff_stats, "pentad")` before writing the CSV fallback (`apps/iEasyHydroForecast/forecast_library.py:4752-4758`).

The daily preprocessing writer follows the same conceptual shape: it maps year columns to `current` / `previous` and sets `norm` from the 50th percentile (`apps/preprocessing_runoff/src/src.py:4514-4569`).

## Quarterly / seasonal aggregation

No quarterly or seasonal hydrograph aggregation implementation was found in `apps/preprocessing_runoff`, `apps/iEasyHydroForecast`, or `apps/backend` source files.

Known facts from code:

- Monthly norms come directly from the iEH HF SDK call `get_norm_for_site(code, "discharge", norm_period="m")` (`apps/iEasyHydroForecast/forecast_library.py:5375-5381`, `apps/iEasyHydroForecast/forecast_library.py:5411-5415`).
- The monthly writer stamps one row per calendar month using `date=YYYY-MM-01`, `month=1..12`, `month_in_year=1..12`, mid-month `day_of_year`, and `norm` (`apps/iEasyHydroForecast/forecast_library.py:5402-5438`).
- `_write_hydrograph_to_api` has no branch for `quarter` or `season`; it rejects anything outside `pentad`, `decade`, or `month` (`apps/iEasyHydroForecast/forecast_library.py:3431-3444`).
- The preprocessing enum has `season` but no `quarter` (`sapphire/services/preprocessing/app/models.py:6-13`).

The operator-confirmed target says quarterly and seasonal norms should be calculated from monthly norms. I did not find code that defines the aggregation formula. The planner should treat the exact formula as an open question: arithmetic mean, month-length-weighted mean, sum/volume, or another hydrologic convention.

## The gap

The break is at the long-horizon write side:

- Month has an ingestion job, but it writes **only norms**. The monthly writer states that it writes "norm-only rows per site" (`apps/iEasyHydroForecast/forecast_library.py:5370-5373`), builds only a `norm` column without year columns (`apps/iEasyHydroForecast/forecast_library.py:5429-5438`), and then the shared writer can only derive `previous` / `current` from year columns (`apps/iEasyHydroForecast/forecast_library.py:3446-3456`, `apps/iEasyHydroForecast/forecast_library.py:3518-3526`).
- Quarter has no accepted preprocessing `HorizonType` value. The enum omits `quarter` (`sapphire/services/preprocessing/app/models.py:6-13`), while CRUD converts any `horizon` query parameter via `HorizonType(horizon)` (`sapphire/services/preprocessing/app/crud.py:142-145`).
- Season is accepted by the preprocessing enum, but there is no season writer and the existing iEasyHydroForecast helper rejects `season` (`apps/iEasyHydroForecast/forecast_library.py:3431-3444`).
- The dashboard long-horizon data loaders do not request month/quarter/season hydrograph overlays; they return daily hydrograph data and empty `hydrograph_pentad_all` placeholders (`apps/forecast_dashboard/src/db.py:767-814`, `apps/forecast_dashboard/src/db.py:834-880`).

## Proposed shape

- Extend the existing monthly norm path rather than starting from scratch: keep `apps/preprocessing_runoff/sync_monthly_norms.py` as the yearly iEH HF norm-fetch entry point, and extend its delegated write-side function or add a sibling that produces full monthly hydrograph rows with `norm`, `previous`, and `current`.
- Derive monthly `previous` / `current` from discharge history by aggregating daily discharge by `(code, year, month)` and joining prior/current year values to the 12 monthly norm rows. This mirrors the pentad template's year-column merge, but the aggregation unit is month.
- Add quarterly and seasonal norm aggregation after monthly norms are available. The formula must be confirmed before implementation; the codebase does not currently define whether this is arithmetic mean, month-length-weighted mean, sum/volume, or another convention.
- Coordinate with the preprocessing service owner for `quarter`: add a `quarter` enum value and API/client support if the dashboard must fetch/write `horizon=quarter`. `season` already exists in the enum, but the write helper still needs a `season` branch.
- Update the dashboard data loader after backend population exists so month/quarter/season horizons read the matching hydrograph rows instead of returning empty placeholders.

## Scope estimate

**Medium-Large.**

Justification:

- **Medium** for month alone: the norm fetch path already exists, the shared hydrograph model has `norm` / `previous` / `current`, and the pentad/daily templates show how to derive the overlay. The missing piece is monthly discharge-history aggregation plus joining the year values into the existing monthly rows.
- **Large-ish when quarter is included**: quarter is used by the dashboard's long-forecast branch, but the preprocessing enum does not accept `quarter`. That needs service-owner coordination, schema/client changes, and likely dashboard changes.
- **Medium for season after formula confirmation**: the preprocessing enum already accepts `season`, but there is no aggregation/writer branch and no dashboard reader for season hydrograph overlays.

## Open questions for the user

1. What exact aggregation formula should be used for quarterly norms from monthly norms: arithmetic mean, month-length-weighted mean, sum/volume, or another hydrologic convention?
2. What exact aggregation formula should be used for seasonal norms, and which months define each season?
3. Should monthly `previous` / `current` be monthly mean discharge, monthly total/volume, or another statistic derived from daily discharge?
4. Should `quarter` be added to the preprocessing `HorizonType` enum, or should quarterly hydrograph rows be represented as `season` / `month` records with another convention?
5. Should the yearly `sync_monthly_norms.py` job also compute quarter/season rows, or should there be a separate long-horizon hydrograph sync task?
6. Should manual sites receive long-horizon norms from an alternate source, or remain excluded as the current monthly norm script does?

## References

- `sapphire/services/preprocessing/app/main.py:101-108` — shared hydrograph POST endpoint.
- `sapphire/services/preprocessing/app/main.py:116-136` — shared hydrograph GET endpoint with `horizon` filter.
- `sapphire/services/preprocessing/app/crud.py:88-129` — hydrograph upsert keyed by `(horizon_type, code, date)`.
- `sapphire/services/preprocessing/app/crud.py:136-153` — hydrograph read path converts `horizon` through `HorizonType`.
- `sapphire/services/preprocessing/app/models.py:6-13` — preprocessing horizon enum; no `quarter`.
- `sapphire/services/preprocessing/app/models.py:41-78` — shared `Hydrograph` table with `norm`, `previous`, `current`.
- `sapphire/services/preprocessing/app/schemas.py:41-60` — hydrograph API schema includes `norm`, `previous`, `current`.
- `apps/forecast_dashboard/src/db.py:190-217` — short-horizon hydrograph reader maps `previous` / `current`.
- `apps/forecast_dashboard/src/db.py:692-729` — dashboard branches long horizons away from the short-horizon hydrograph reader.
- `apps/forecast_dashboard/src/db.py:767-814` — monthly loader returns daily hydrograph plus empty `hydrograph_pentad_all`.
- `apps/forecast_dashboard/src/db.py:834-880` — quarter/season loaders return daily hydrograph plus empty `hydrograph_pentad_all`.
- `apps/preprocessing_runoff/sync_monthly_norms.py:1-12` — yearly monthly norm ingest intent.
- `apps/preprocessing_runoff/sync_monthly_norms.py:149-180` — iEH HF SDK initialization and manual-site filtering.
- `apps/preprocessing_runoff/sync_monthly_norms.py:216-229` — delegation to `write_month_hydrograph_data`.
- `apps/preprocessing_runoff/preprocessing_runoff.py:427-479` — daily runoff hydrograph generation and write path.
- `apps/preprocessing_runoff/src/src.py:4125-4201` — daily hydrograph statistics plus last/current year merge.
- `apps/preprocessing_runoff/src/src.py:4514-4569` — daily API writer maps year columns to `previous` / `current` and `q50` to `norm`.
- `apps/iEasyHydroForecast/forecast_library.py:3376-3540` — shared iEasyHydroForecast hydrograph API writer.
- `apps/iEasyHydroForecast/forecast_library.py:4516-4758` — pentad hydrograph triad computation and dispatch.
- `apps/iEasyHydroForecast/forecast_library.py:5361-5463` — monthly norm-only writer.
