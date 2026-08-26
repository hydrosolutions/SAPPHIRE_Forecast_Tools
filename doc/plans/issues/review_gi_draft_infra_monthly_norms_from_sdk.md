# Ingest monthly discharge norms from iEH HF SDK into the hydrographs table

> ## ⚠️ SUPERSEDED — do not follow the rollout commands in this document
>
> The `monthly_norms` Luigi task described here was **retired**. It is no longer in
> `RunPeriodicMaintenanceWorkflow`'s task map, and `bin/run_periodic_maintenance.sh` still
> *accepts* the name while exiting 0 without running anything (INFRA-023) — so every cron
> line and manual command below is a silent no-op.
>
> **The replacement is `bin/yearly_runoff_hydrograph_aggregation.sh`.** See DOC-007
> (`review_gi_draft_doc_deployment_cron_block_stale_authority.md`) and INFRA-023
> (`mid_prio_gi_draft_infra_yearly_monthly_norms_cron_unmapped.md`).
>
> Retained as the historical design record only.

## Status — implemented 2026-04-23

Implemented on branch `develop_infra_monthly_norms_from_sdk` (based on
`maxat_sapphire_2` @ `b74871b`). Awaiting review + merge.

Commits (P1 → P4):

| Phase | Commit | Summary |
|-------|--------|---------|
| P1 TDD | `f69bfb8` | Tests for monthly hydrograph norm ingestion |
| P1 | `b18d119` | `write_month_hydrograph_data` + `horizon_type="month"` in `forecast_library.py` (read + write paths) |
| P2 TDD | `80b02ff` | Tests for `sync_monthly_norms` entry point |
| P2 | `fb4ed2e` | `apps/preprocessing_runoff/sync_monthly_norms.py` CLI entry point |
| P3 TDD | `24ac7f4` | Tests for `YearlyMonthlyNormsRecalculation` + task routing |
| P3 | `8826829` | Register `monthly_norms` in `RunPeriodicMaintenanceWorkflow`; update shell + compose comments |
| P4 | `fee4107` | Yearly cron line in `doc/deployment.md` and `doc/plans/deployment_new_hydromet_aws.md` |

Cron line shipped (Jan 1, 03:00 UTC):

```cron
0 3 1 1 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh monthly_norms /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_periodic_monthlynorms_$(date +\%Y\%m\%d).log 2>&1
```

Server rollout instructions: see "Server rollout" section below.

## Server rollout

Pre-requisite: the branch must be merged (or the server checkout switched to
`develop_infra_monthly_norms_from_sdk`) and the `sapphire-preprunoff` Docker
image rebuilt/pulled so it contains `sync_monthly_norms.py`.

1. **Merge or switch branch** on the server:
   ```bash
   cd /data/SAPPHIRE_Forecast_Tools
   git fetch origin
   git checkout maxat_sapphire_2 && git pull   # once the PR is merged
   # — or, for a pre-merge trial run —
   # git checkout develop_infra_monthly_norms_from_sdk && git pull
   ```

2. **Rebuild / pull the preprunoff image** so it contains `sync_monthly_norms.py`:
   ```bash
   # Pull the published tag (recommended once the branch is merged + CI pushed):
   docker pull mabesa/sapphire-preprunoff:latest
   # — or rebuild locally from the checkout:
   bash bin/utils/build_docker_images.sh latest
   ```

3. **Optional first-time manual run** to populate monthly norms immediately
   instead of waiting until next Jan 1:
   ```bash
   bash bin/run_periodic_maintenance.sh monthly_norms /data/<data_folder>/config/<env_file>
   ```
   Verify rows appear in `hydrographs` with `horizon_type='month'` and
   `date` set to first-of-month of the current year.

4. **Add the cron line** (`crontab -e`) — append under the existing periodic
   maintenance section:
   ```cron
   # Yearly monthly discharge norm recalculation from iEH HF SDK at 03:00 UTC on January 1
   0 3 1 1 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh monthly_norms /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_periodic_monthlynorms_$(date +\%Y\%m\%d).log 2>&1
   ```
   Replace `<data_folder>` and `<env_file>` with the deployment's values.

5. **Verify** with `crontab -l` and (on next Jan 1 or after the manual step)
   check `/home/ubuntu/logs/sapphire_periodic_monthlynorms_*.log`.

---

## Problem

SAPPHIRE currently ingests pentadal (`'p'`, 72 values/site) and decadal (`'d'`,
36 values/site) discharge norms from the iEasyHydro HF SDK and writes them to
the `hydrographs` table. The SDK also exposes monthly norms (`'m'`, 12
values/site) via the same `get_norm_for_site(site_code, "discharge",
norm_period="m")` API, but no code path in this repo fetches or persists them.

Operational consequences:
- The `hydrographs` table has no rows with `horizon_type='month'` for any
  deployment, so dashboards and downstream modules cannot show monthly
  climatology baselines.
- Hydromets that may later enable monthly (long-term) forecasts would need the
  norms as a prerequisite; today the norm table is unpopulated for that horizon.

## Decision

Add a **yearly** periodic-maintenance task that fetches monthly norms from the
iEH HF SDK for every forecast-enabled site and writes them to the `hydrographs`
table with `horizon_type='month'`. Ship it as an opt-in cron line so any
hydromet can enable it.

**Decisions (locked in):**
- **Scope of write:** only the `norm` column is populated from the SDK. Other
  columns (`count`, `mean`, `std`, `min`, `max`, `q05`..`q95`, `previous`,
  `current`) stay NULL on monthly rows. Full-stat computation from local runoff
  history is a separate follow-up if/when a consumer needs it.
- **Trigger:** new `monthly_norms` task in `bin/run_periodic_maintenance.sh`,
  run yearly via cron (norms are long-term averages — daily re-fetch is
  wasteful). Runs via the existing `periodic-maintenance` Luigi service,
  consistent with `skill_recalc` and `snow_norms`.
- **Row shape per site:** 12 rows, one per calendar month. `horizon_value` and
  `horizon_in_year` = 1..12. `day_of_year` = representative mid-month day
  (`[15, 46, 74, 105, 135, 166, 196, 227, 258, 288, 319, 349]`, non-leap
  reference — this is a climatology marker, not a calendar record). Consumers
  should treat `day_of_year` as a month-centroid label; in leap years calendar
  `day_of_year` for months March-December would be off by one vs these
  non-leap values. That's acceptable for climatology.
- **`date` column convention:** mirror the existing pentad/decad norm
  writer. `forecast_library.py:4541` sets
  `current_year = data["date"].dt.year.max()` (most recent year in the
  runoff history), and `forecast_library.py:4694-4696` assigns
  `date = get_issue_date_from_pentad(p, current_year)` to each pentad row.
  For monthly: `date` = first-of-month of `current_year`
  (`2025-01-01, 2025-02-01, …, 2025-12-01` for a run where the most recent
  runoff year is 2025). Each yearly re-run therefore writes 12 *new* rows per
  site under a fresh date — the `hydrographs` table accumulates year-by-year
  snapshots. The server-side upsert (`sapphire/services/preprocessing/app/crud.py:89`,
  keyed on `(horizon_type, code, date)`) handles within-year re-runs
  idempotently (the daily LR pipeline already relies on this for
  pentad/decad). Note: the SDK norm is a fixed climatology with no year
  dimension (confirmed by reading `ieasyhydro_sdk/sdk_endpoint_definitions.py`
  — the endpoint accepts no year filter), so the values under different dates
  will typically be identical; the year-indexed dates reflect *when the
  snapshot was taken*, not a change in the underlying norm. **Empirically
  confirmed** on the uzhm preprocessing DB 2026-04-23: `hydrographs`
  contains only 2026 dates (deployment started in 2026), matching the
  "new rows per year" behaviour — mature deployments (e.g. kghm running
  since 2023) would show multiple years in the date range.
- **Column names for month (new):** `horizon_value` DataFrame column is
  `month` (1..12); the "horizon in year" column is `month_in_year` (also
  1..12). Matches the `pentad`/`pentad_in_year` and `decad`/`decad_in_year`
  naming used by existing writers.
- **Site enumeration:** reuse `get_all_forecast_sites_from_HF_SDK()` in
  `setup_library.py` for the full forecast-enabled list, **then filter out
  manual sites** at the entry-point script using `_get_manual_site_codes()`
  (`setup_library.py:712`). Reason: manual sites (e.g. Google-Sheets-backed
  Zarafshan 10001) are appended unconditionally at `setup_library.py:1567-1571`;
  the SDK cannot return norms for them. Passing them in would generate a
  warning per site on every yearly run — proportional to the number of
  manual sites in the deployment. Filtering at the entry point keeps the
  library function general and emits exactly one `info` log per skipped
  manual site rather than N `warning` logs.
- **Failure policy:** per-site log-and-continue **for SDK sites**. An SDK
  site where the SDK returns empty, errors, or returns a length ≠ 12 is
  skipped with a warning; the remaining sites still get written. Manual
  sites are not passed in at all (see Site enumeration), so they don't
  count against the success/failure tally.
- **Failure must be loud at the API layer.** `_write_hydrograph_to_api`
  returns `False` (not raises) when the API is disabled
  (`SAPPHIRE_API_ENABLED=false`), unavailable (`SAPPHIRE_API_AVAILABLE=False`),
  or the readiness check fails (`forecast_library.py:3402-3418`). The
  pentad/decad callers (`forecast_library.py:4739-4744`) discard that
  return value and only set `api_ok=False` on an *exception* — they get
  away with it because they also write a CSV fallback (line 4762). The
  monthly path has **no CSV fallback**, so a naïve mirror would exit 0
  with zero rows persisted and the Luigi marker written, masking the
  outage for a full year. The new `write_month_hydrograph_data` must
  **check the return value of `_write_hydrograph_to_api` and raise** if
  it is `False`. The Luigi task therefore fails, the marker is not
  written, and the operator sees the failure in Luigi's status UI /
  cron log on the next retry. Do not change the pentad/decad caller
  behaviour — they have their CSV safety net and we are not in scope to
  fix that here.
- **No schema change:** `HorizonType.MONTH` already exists in
  `sapphire/services/preprocessing/app/models.py:11`.

## Approach

Mirror the pentad/decad library functions
(`apps/iEasyHydroForecast/forecast_library.py:4502` and `:4793`) with a
monthly-only variant, scoped to just the norm overlay (no by-month runoff
aggregation). Wire it through the same `periodic-maintenance` orchestration
that `skill_recalc` and `snow_norms` already use.

**Why not extend `write_pentad_hydrograph_data`:** those functions expect a
runoff DataFrame as primary input and compute stats from it. Monthly-only
would require ugly conditionals. Cleaner to add a sibling function that takes
`code_list + sdk` and writes norm-only rows.

**Why yearly, not bimonthly:** SDK monthly norms are multi-year climatologies
computed on the iEH HF side. They change when the iEH HF backend recomputes
its long-term averages, which is infrequent. Yearly is enough; operators can
dispatch the task manually between runs if they want a fresher snapshot.

## Scope

**In scope:**
- Extend `_write_hydrograph_to_api` in
  `apps/iEasyHydroForecast/forecast_library.py` so that
  `horizon_type="month"` is accepted alongside the existing `"pentad"` /
  `"decade"` branches.
- Extend `read_hydrograph_data` in the same file (the read-path validator
  at `forecast_library.py:2992-2993` currently raises
  `ValueError(f"horizon_type must be 'pentad' or 'decade', got: {horizon_type}")`)
  to accept `"month"`. Symmetric to the write-path extension — without it,
  no consumer can query the rows we are about to insert, and the plan's
  rollout promise ("dashboards can later query `horizon_type='month'`") is
  structurally blocked. Dashboard rewire is still out of scope; the read
  *capability* just needs to land with the write.
- New library function
  `write_month_hydrograph_data(code_list, iehhf_sdk, current_year=None)` in
  the same file. When `current_year` is None the implementation resolves it
  to `date.today().year` (no runoff DataFrame is passed in, so we can't
  mirror the exact pentad/decad expression
  `data["date"].dt.year.max()`; calling the entry point from the yearly
  cron makes `date.today().year` a faithful substitute).
- New entry-point script (CLI-invokable inside the periodic-maintenance
  container) that initialises the SDK, loads site codes, and calls the library
  function. Location: **`apps/preprocessing_runoff/sync_monthly_norms.py`**
  (monthly norms are discharge-related, so they fit `preprocessing_runoff`
  alongside existing discharge-ingest logic; compare `snow_norms` → lives in
  `preprocessing_gateway`, `skill_recalc` → lives in
  `postprocessing_forecasts`).
- Add a `YearlyMonthlyNormsRecalculation` Luigi task in
  `apps/pipeline/pipeline_docker.py` and register it in
  `RunPeriodicMaintenanceWorkflow.task_map` (per the Files-touched table).
  Update help-text / comments in `bin/run_periodic_maintenance.sh`,
  `bin/docker-compose-luigi.yml`, `bin/README.md` — no dispatch-logic
  changes in the shell layer (validation happens in Luigi).
- Pytest tests (TDD — write first):
  - Unit: `write_month_hydrograph_data` with a mocked SDK returning 12 values
    → 12 rows/site written with correct `horizon_type`, `horizon_value`,
    `day_of_year`, `norm`.
  - Unit: SDK returning empty / raising / wrong-length → site skipped, logged,
    other sites still processed.
  - Unit: empty `code_list` → no-op, returns True.
  - Smoke: entry-point script `main()` invokable with mocked SDK + fake site
    list; exits 0.
- Cron template line + docs: `bin/README.md` cron table,
  `doc/deployment.md`, `doc/plans/deployment_new_hydromet_aws.md` Phase 10
  crontab template.

**Out of scope:**
- Populating `count`, `mean`, `std`, `min`, `max`, `q05`..`q95` for monthly
  rows (separate follow-up if dashboard ever needs monthly-resolution
  quantiles).
- A fallback path that computes monthly norms from local history (runoffs
  table or Google Sheets) when the SDK returns no result for a manual
  (Sheets-backed) site. The expectation — to be verified during the Phase 3
  integration test, not assumed — is that the SDK will not return norms for
  sites whose history lives only in Google Sheets. If that's confirmed, a
  local-history fallback is a separate follow-up issue.
- Migrating existing deployments: this is additive. Rolling out means
  enabling the new cron line.
- Any consumer changes (dashboard, long_term_forecasting) — they can start
  reading the new rows once populated, but their code is not touched here.
- `sapphire/services/` — no service changes.

## Files touched

| File | Change |
|------|--------|
| `apps/iEasyHydroForecast/forecast_library.py` | **Three changes:** (a) extend `_write_hydrograph_to_api` (currently at line 3374) to accept `horizon_type="month"` — add a third branch in the if/elif/else at line ~3425 that sets `horizon_value_col="month"`, `horizon_in_year_col="month_in_year"` and extend the docstring. Keep the `ValueError` default branch so unknown horizon types still raise. (b) Extend `read_hydrograph_data` horizon validator at line 2992-2993 the same way — accept `"month"` alongside `"pentad"` / `"decade"`. (c) Add `write_month_hydrograph_data(code_list, iehhf_sdk, current_year=None)` alongside `write_pentad_hydrograph_data` (line 4502) / `write_decad_hydrograph_data` (line 4793). When `current_year is None`, default to `date.today().year`. Per-site try/except mirroring line 4598; length-check against 12; build 12-row DataFrame per site with `code, date (first-of-month of current_year: YYYY-01-01 .. YYYY-12-01), month (1..12), month_in_year (1..12), day_of_year (mid-month list, non-leap), norm`; concatenate and call `_write_hydrograph_to_api(df, "month")`. **Must check the return value of `_write_hydrograph_to_api` and raise if `False`** (see Decisions > "Failure must be loud"). Mirrors the pentad/decad path which uses `current_year = data["date"].dt.year.max()` to stamp `date = get_issue_date_from_pentad(p, current_year)` per-row (line 4541, 4694-4696). |
| `apps/iEasyHydroForecast/tests/test_forecast_library.py` | Add test class `TestWriteMonthHydrographData` covering: 12-row write, SDK empty, SDK raises, wrong length, empty `code_list`, mixed success/failure across sites. Also add at least one test for `_write_hydrograph_to_api` accepting `"month"` and rejecting a bogus horizon type (keeps the "invalid horizon_type" error path covered). |
| `apps/preprocessing_runoff/sync_monthly_norms.py` *(new)* | CLI entry point: read env, init `IEasyHydroHFSDK`, load forecast-enabled site codes via `get_all_forecast_sites_from_HF_SDK()`, **filter out manual sites using `_get_manual_site_codes()`** (emit one `info` log per skipped manual site), call `write_month_hydrograph_data(sdk_only_codes, sdk)`, exit 0 on partial/full success, non-zero if zero SDK sites succeeded or if the library raises due to API unavailability. Mirrors `apps/preprocessing_gateway/recalculate_snow_norms.py` in argparse setup, SDK init, and logging. |
| `apps/preprocessing_runoff/test/test_sync_monthly_norms.py` *(new)* | Smoke test: `main()` with mocked SDK + stub code list → exits 0, library function called with the right args. Directory is `test/` (singular) per preprocessing_runoff convention; iEasyHydroForecast uses `tests/` (plural) — both are intentional. |
| `apps/pipeline/pipeline_docker.py` | **Two changes:** (a) Add new class `YearlyMonthlyNormsRecalculation(DockerTaskBase)` mirroring `YearlySnowNormRecalculation` at line 1992, but with `image_name="sapphire-preprunoff"` (gspread already installed in that image per commit `13898f7`), `container_name="maintenance-monthly-norms"`, `command=["uv", "run", "sync_monthly_norms.py"]`, `marker = get_maintenance_marker_filepath("monthly_norms")` in `output()`. Mem limits: 4g/6g same as snow_norms unless benchmarking says otherwise. (b) Add `"monthly_norms": YearlyMonthlyNormsRecalculation()` to the `task_map` dict in `RunPeriodicMaintenanceWorkflow.requires` at line ~2037. |
| `apps/pipeline/tests/test_maintenance_tasks.py` | Add test mirroring the existing `YearlySnowNormRecalculation` test (pipeline_docker.py:285 pattern): asserts marker path, image_name, command. |
| `bin/run_periodic_maintenance.sh` | Update the valid-task-types list at lines 10 (doc comment) and 26 (error message) to include `monthly_norms`. No dispatch-logic change needed (the shell script is a pass-through; validation happens in `RunPeriodicMaintenanceWorkflow`). |
| `bin/docker-compose-luigi.yml` | Update the comment at lines 114-115 listing valid `MAINTENANCE_TASK_TYPE` values. `docker-compose` config itself already supports any value via `${MAINTENANCE_TASK_TYPE:-long_term}` at line 127 — no structural change. |
| `bin/README.md` | Update the `monthly_norms` task mention in the periodic-maintenance section around line 123; add a row to the cron-schedule table around line 221. |
| `apps/pipeline/README` | Update the ASCII routing tree at lines 99-102 to add `monthly_norms → YearlyMonthlyNormsRecalculation` alongside `long_term`, `skill_recalc`, `snow_norms`. Keeps the developer-facing routing map in sync with `bin/README.md` and `pipeline_docker.py`. |
| `doc/deployment.md` | Add yearly cron line in the `Set up cron job` section, with comment explaining the task. |
| `doc/plans/deployment_new_hydromet_aws.md` | Add the same line to the Phase 10 crontab template around line 637-650. |

## Phases

### Phase 1 — Library function + tests
- **Goal:** `_write_hydrograph_to_api` and `read_hydrograph_data` both accept
  `"month"`; `write_month_hydrograph_data` exists, fails loudly on API
  unavailability; all three tested, tests green across iEasyHydroForecast
  AND linear_regression.
- **Files:**
  - `apps/iEasyHydroForecast/forecast_library.py` — extend
    `_write_hydrograph_to_api` (line ~3425 if/elif chain) to handle `"month"`;
    extend `read_hydrograph_data` horizon validator at line 2992-2993 to
    accept `"month"`; add the new `write_month_hydrograph_data` function
    with explicit `False`-return check that raises.
  - `apps/iEasyHydroForecast/tests/test_forecast_library.py` — tests per the
    Scope section, plus a regression test that `_write_hydrograph_to_api`
    and `read_hydrograph_data` still raise on unknown horizon types.
    **Must include a test that `write_month_hydrograph_data` raises when
    `_write_hydrograph_to_api` returns `False`** (parameterize with the
    three conditions: `SAPPHIRE_API_ENABLED=false`,
    `SAPPHIRE_API_AVAILABLE=False`, and readiness-check failure).
- **Explicitly not allowed:** edits to `sapphire/services/`, other modules, or
  any Dockerfile. No changes to existing pentad/decad behaviour — in
  particular, do not modify the `api_ok`-flag pattern at
  `forecast_library.py:4739-4744` in the pentad/decad callers.
- **Agents:** 1 (Sonnet 4.6, worktree isolation).
- **Acceptance:** the following two commands each return zero failures
  and zero unexpected skips (run sequentially — `run_tests.sh` accepts
  exactly one module argument, multi-arg invocation silently drops the
  rest per `run_tests.sh:154-192`):
  - `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast`
  - `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression`
    (covers `test_forecast_library_api.py::TestWriteHydrographToApi`, which
    exercises the shared `_write_hydrograph_to_api` helper — a regression
    here would otherwise slip past the iEasyHydroForecast run).
  The pre-existing pentad/decad test cases must continue to pass unchanged.

### Phase 2 — Entry-point script + smoke test
- **Goal:** CLI script that Phase 3's Luigi task can invoke.
- **Files:** `apps/preprocessing_runoff/sync_monthly_norms.py`,
  `apps/preprocessing_runoff/test/test_sync_monthly_norms.py`.
- **Depends on:** P1.
- **Agents:** 1.
- **Acceptance:** smoke test passes; `python sync_monthly_norms.py --help`
  runs without import errors. Structure should mirror `preprocessing_gateway`'s
  snow-norm entry point (same arg parsing, same SDK init, same logging setup).

### Phase 3 — Periodic-maintenance + Luigi wiring
- **Goal:** `bash bin/run_periodic_maintenance.sh monthly_norms <env_file>`
  runs the new task end-to-end (against a test deployment).
- **Files:**
  - `apps/pipeline/pipeline_docker.py` — new class
    `YearlyMonthlyNormsRecalculation(DockerTaskBase)` modelled line-for-line
    on `YearlySnowNormRecalculation` (line 1992) with the three swaps listed
    in the Files-touched table (image, container name, command, marker). And
    add the `"monthly_norms": …` entry to `RunPeriodicMaintenanceWorkflow`'s
    `task_map` at line ~2037 — without this, the Luigi workflow raises
    `ValueError` for unknown task_type.
  - `apps/pipeline/tests/test_maintenance_tasks.py` — new test mirroring the
    `YearlySnowNormRecalculation` test at line ~285.
  - `bin/run_periodic_maintenance.sh` — lines 10 and 26 (doc-comment + error
    message): add `monthly_norms` to the valid-task-types list.
  - `bin/docker-compose-luigi.yml` — comment at lines 114-115 only; no
    structural change.
  - `bin/README.md` — lines ~123 (prose) and ~221 (cron-schedule table).
  - `apps/pipeline/README` — ASCII routing tree at lines 99-102; add
    `monthly_norms → YearlyMonthlyNormsRecalculation` row.
- **Explicitly not allowed:** changes to the snow_norms or skill_recalc task
  definitions, or to `_standard_maintenance_volumes` / `_common_maintenance_env`
  helpers (shared utilities — accidentally touching them would bleed into
  other tasks).
- **Depends on:** P2.
- **Agents:** 1.
- **Acceptance:**
  - `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` — zero
    failures, zero unexpected skips.
  - Against a test `.env` with a live iEH HF connection, running
    `bash bin/run_periodic_maintenance.sh monthly_norms <env>` results in rows
    in `hydrographs` with `horizon_type='month'`, `date` = first-of-month
    of the run's current year (e.g., `2026-01-01 .. 2026-12-01` when run in
    2026), and non-null `norm` for every forecast-enabled site whose norm
    the SDK can return. The marker file at
    `get_maintenance_marker_filepath("monthly_norms")` exists afterwards.
  - Re-running the same task immediately should be a no-op (Luigi sees the
    marker and skips) unless the marker file is removed first.

### Phase 4 — Cron + docs
- **Goal:** Operators can enable the task via crontab with a documented line.
- **Files:** `bin/README.md`, `doc/deployment.md`,
  `doc/plans/deployment_new_hydromet_aws.md`.
- **Depends on:** P3.
- **Agents:** 1.
- **Acceptance:** Docs include the new cron line (e.g. `0 3 1 1 *` — Jan 1 at
  03:00 UTC) with a one-liner explaining what it does and that it is yearly.

### Dependency graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 }
  }
}
```

## Testing plan

- Unit tests per phase (see `Scope > In scope` and phase acceptance criteria).
- Manual integration test after Phase 3: run
  `bash bin/run_periodic_maintenance.sh monthly_norms <uzhm-env-file>` on the
  uzhm staging host and verify that `hydrographs` contains up to ~48 rows
  (4 iEH-HF-sourced uzhm sites × 12 months) with `horizon_type='month'`,
  `date` = first-of-month in the run's current year (e.g.,
  `2026-01-01..2026-12-01`), and non-null `norm`.
- **Empirical check of manual-site behaviour.** The Google-Sheets-backed
  Zarafshan station (10001) is *expected* to be absent from the SDK result,
  but that's an inference, not a tested fact. During the Phase 3 manual
  integration test, explicitly confirm the run log shows a log-and-continue
  warning for 10001 (or equivalent) rather than an unhandled exception. If
  the SDK *does* return norms for 10001 (e.g., because iEH HF has an
  internal norm record for it), note the values and update the
  out-of-scope section accordingly.

## Rollout

- Strictly additive: writes new rows, doesn't modify existing ones.
- No data migration.
- No consumer changes today; dashboards can later query `horizon_type='month'`
  if they want to surface climatology.
- Deployment change: operators opt in by adding one cron line. Not enabled
  automatically.
- **Table-size growth:** mirrors pentad/decad. After N years of yearly runs,
  the `hydrographs` table holds ~`12 × N × (number of forecast-enabled sites)`
  monthly rows, plus the existing `72 × N` pentad and `36 × N` decad rows
  per site. For a 10-station hydromet over 10 years: ~1 200 monthly rows
  total — negligible vs the ~10 800 pentad + ~3 600 decad rows already
  written under the same convention. No pruning needed.

## Orchestration note (per CLAUDE.md)

Each phase is delegated to a Sonnet 4.6 general-purpose agent with
`isolation: "worktree"` and an explicit file-allowlist in the prompt. Orchestrator
reviews the diff after every phase and runs the full `SAPPHIRE_TEST_ENV=True
bash run_tests.sh` before moving on. This work is **unrelated to the
`fix_pipeline_uzhm_timeout_support` branch** and should land on its own
feature branch (suggested name: `develop_infra_monthly_norms_from_sdk`).
