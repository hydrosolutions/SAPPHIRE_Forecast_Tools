# PP-036: ML skill metrics starved by DAY horizon short-circuit

| Field | Value |
|---|---|
| Module | `postprocessing_forecasts` |
| Priority | High |
| Status | **Completed (2026-05-28)** |
| Branch | `fix_postprocessing_skill_metrics` (pushed) |
| Labels | `bug`, `skill-metrics`, `api-integration`, `tajik-deployment` |

## Completion Summary

Implemented on branch `fix_postprocessing_skill_metrics` and pushed. Three commits:

| Commit | Phase | Content |
|---|---|---|
| `a32a11f` | P1 | Archive-union regression tests (replaces DAY-first assertions in `test_data_reader.py`; adds `test_ml_horizon_archive_split.py`; adds direct `_normalize_ml_forecasts` mixed-frame test) |
| `c228122` | P2 | Cutover merge implementation in `apps/postprocessing_forecasts/src/data_reader.py` (+147/-63 lines; signature preserved) |
| `755addf` | P3 | Integration test exercising the recalc path end-to-end, plus NEURAL_ENSEMBLE mixed-coverage numeric-mean assertions |

**Final test run:** 1330 passed, 0 failed, 0 unexpected skips, 2 warnings.

**Red-phase verification:** confirmed 2026-05-28 that the new tests genuinely guard the fix — reverting P2 alone produces 10 failures (8 P1 + 2 P3), all PP-036-related, no Type-II errors, no scope bleed. See "Red-Phase Verification Log" at the bottom of this document for procedure and counts.

**Deferred to deployment time (P4):** operator runs the archive probe, staging recalc, and API/SQL `n_pairs >= 15` assertions from the P4 section against a real postgres-backed deployment before treating PP-036 as deployed on Tajik. Not a release blocker for this branch; tracked as part of the Tajik pre-deploy validation.

**Decisions worth preserving:**
- The LR byte-identical baseline assertion was dropped in favour of relying on the P2 file allow-list and orchestrator deliberation. The "LR path regression" Risks bullet documents the rationale.
- The `first_day_date` cutover is computed per `(code, model_type)` after `_clean_code_column()` to keep `_normalize_ml_forecasts()` archive-agnostic.

## Problem Statement

ML pentad/decade skill metrics are recalculated from too little history because `_read_ml_forecasts_pp_api` stops at the first non-empty archive. The current code says it will try `horizon="day"` first and "then falls back" to the requested period horizon (`apps/postprocessing_forecasts/src/data_reader.py:1582-1586`), but the control flow returns immediately when DAY has any rows:

```python
# apps/postprocessing_forecasts/src/data_reader.py:1621-1674
for try_horizon in ["day", api_horizon]:
    ...
    if all_records:
        logger.debug(
            "Read ML forecasts for %s with horizon=%s",
            model,
            try_horizon,
        )
        return pd.concat(all_records, ignore_index=True)
```

Because operational DAY rows exist for TFT, TiDE, and TSMixer while the longer migrated PENTAD/DECADE archive also exists, recalculation reads only the short DAY-era subset. `calculate_skill_metrics()` then merges simulated and observed rows on `["code", "date"]` (`apps/postprocessing_forecasts/src/skill_metrics.py:1802-1806`) and groups by period, code, and model (`apps/postprocessing_forecasts/src/skill_metrics.py:1813-1824`), so each period-level group can end up with only the recent DAY-era pairs instead of the full migrated history.

## Investigation Findings

The bug still lives in `apps/postprocessing_forecasts/src/data_reader.py`. `_read_ml_forecasts_pp_api()` is defined at `apps/postprocessing_forecasts/src/data_reader.py:1575`, maps `horizon_type="decad"` to API `horizon="decade"` at `apps/postprocessing_forecasts/src/data_reader.py:1614`, builds optional `start_date`/`end_date` at `apps/postprocessing_forecasts/src/data_reader.py:1616-1617`, then loops over `["day", api_horizon]` at `apps/postprocessing_forecasts/src/data_reader.py:1621`. The early return at `apps/postprocessing_forecasts/src/data_reader.py:1668-1674` confirms the draft's root-cause description.

Production callers are confined to `apps/postprocessing_forecasts/src/data_reader.py`: `read_individual_model_forecasts()` calls `_read_ml_forecasts_pp_api()` for each configured ML model at `apps/postprocessing_forecasts/src/data_reader.py:2149-2152`; `read_observed_and_modelled_data()` calls `read_individual_model_forecasts()` at `apps/postprocessing_forecasts/src/data_reader.py:2270-2274`; and `read_individual_model_forecasts_for_dates()` delegates back to `read_individual_model_forecasts()` with year bounds at `apps/postprocessing_forecasts/src/data_reader.py:2204-2213`. Tests import or patch the private reader at `apps/postprocessing_forecasts/tests/test_data_reader.py:14-28` and `apps/postprocessing_forecasts/tests/test_data_reader.py:2919-2966`.

The postprocessing API can filter forecasts by one horizon per request. `GET /forecast/` accepts a single `horizon` query parameter (`sapphire/services/postprocessing/app/main.py:71-83`) and passes it to `crud.get_forecast()` (`sapphire/services/postprocessing/app/main.py:87-99`), where `Forecast.horizon_type == horizon` is applied when present (`sapphire/services/postprocessing/app/crud.py:79-82`). The schema and model field are both `horizon_type` (`sapphire/services/postprocessing/app/schemas.py:8-16`, `sapphire/services/postprocessing/app/models.py:57-70`), and the enum includes `DAY`, `PENTAD`, and `DECADE` (`sapphire/services/postprocessing/app/models.py:9-17`). The client should make two explicit requests (`day` plus `pentad` or `decade`) rather than relying on an unfiltered API call. Do not change anything under `sapphire/services/`.

The short-term recalculation path starts in `bin/yearly_skill_metrics_recalculation.sh`, which delegates to `run_skill_metrics_recalc_once` at `bin/yearly_skill_metrics_recalculation.sh:96-100`; the helper runs `uv run recalculate_skill_metrics.py` in the postprocessing container at `bin/utils/run_skill_metrics_recalc.sh:71-87`. Luigi has the same annual path in `YearlySkillRecalculation`, setting `SAPPHIRE_PREDICTION_MODE=BOTH` and running the same entry point at `apps/pipeline/pipeline_docker.py:1974-2003`. New-site backfill also invokes the same helper in mode `BOTH` at `bin/initialize_site_backfill.sh:419-459`.

Inside the Python entry point, `_run_short_term_recalc()` calls `data_reader.read_observed_and_modelled_data(config.name, codes=codes)` at `apps/postprocessing_forecasts/recalculate_skill_metrics.py:122-131`, derives the Neural Ensemble from whatever ML rows were loaded at `apps/postprocessing_forecasts/recalculate_skill_metrics.py:146-151`, and calls `skill_metrics.calculate_skill_metrics()` at `apps/postprocessing_forecasts/recalculate_skill_metrics.py:153-163`. Scoped dashboard-triggered recalculation also runs `uv run recalculate_skill_metrics.py` with `SAPPHIRE_RECALC_STATION_CODE` at `apps/forecast_dashboard/src/vizualization.py:4035-4050`; `_read_station_codes()` honors that override at `apps/postprocessing_forecasts/recalculate_skill_metrics.py:97-109`.

Operational short-term postprocessing reads only the current year for forecast creation (`apps/postprocessing_forecasts/postprocessing_operational.py:121-128`) and reads already-calculated skill metrics (`apps/postprocessing_forecasts/postprocessing_operational.py:132-140`), so the destructive write happens during recalculation, not normal daily forecast display. Maintenance gap filling uses `read_individual_model_forecasts_for_dates()` at `apps/postprocessing_forecasts/postprocessing_maintenance.py:255-267`; it benefits from the same reader fix but does not recalculate historical skill metrics.

The repository confirms the structural archive split but not the deployment-specific DAY start date. The short-term data-flow document says ML writes `forecasts` rows with `horizon_type=day` and daily targets (`doc/data_flow_short_term.md:99-104`, `doc/data_flow_short_term.md:131-135`). The postprocessing migrator maps ML forecast CSV rows into `horizon_type="day"` for the current `forecast` migrator path (`sapphire/services/postprocessing/app/data_migrator.py:359-378`, `sapphire/services/postprocessing/app/data_migrator.py:1020-1037`), and the bulk migration utility transforms historical ML CSVs into `horizon_type="DAY"` (`apps/machine_learning/bulk_migrate_forecasts.py:90-118`). None of the checked code or docs proves "DAY starts around April 2024" for the running Kyrgyz deployment; that remains an operator-confirmed data fact.

Forecast-date rule audit: the PP-036 fix surface in `_read_ml_forecasts_pp_api()` does not call `date.today()` today and must not add one. There are existing date-today usages elsewhere in the recalc path: `recalculate_skill_metrics.py` defaults the skill-metric year from `dt.date.today().year` at `apps/postprocessing_forecasts/recalculate_skill_metrics.py:211`, long-term/default ranges use it at `apps/postprocessing_forecasts/recalculate_skill_metrics.py:239-246`, `286-293`, `333-340`, and `380-387`, and `calculate_skill_metrics()` uses `dt.date.today().year - 20` as the default filter start at `apps/postprocessing_forecasts/src/skill_metrics.py:1796-1800`. These are pre-existing cleanup candidates; PP-036 should avoid expanding the date surface and should set date/range environment explicitly in tests.

Existing test coverage is partial and currently encodes the bad fallback contract. `TestReadMlForecastsPpApi` asserts "tries DAY first" and "falls back to horizon_type" at `apps/postprocessing_forecasts/tests/test_data_reader.py:2656-2754`, but it does not assert reading both archives. `_normalize_ml_forecasts()` has extensive period-target aggregation coverage in `apps/postprocessing_forecasts/tests/test_data_reader_ml_aggregation.py:1-17`, including pentad target filtering (`apps/postprocessing_forecasts/tests/test_data_reader_ml_aggregation.py:23-51`) and dual pentad/decade boundary behavior (`apps/postprocessing_forecasts/tests/test_data_reader_ml_aggregation.py:438-455`). Recalc workflow tests cover that the entry point calls `calculate_skill_metrics()` and saves results (`apps/postprocessing_forecasts/tests/test_recalc_workflow.py:167-198`), while scoped recalc already uses placeholder station code `19999` in tests (`apps/postprocessing_forecasts/tests/test_scoped_skill_recalc.py:121-124`, `apps/postprocessing_forecasts/tests/test_scoped_skill_recalc.py:181-218`).

The dashboard does not need a PP-036 code change. It fetches persisted skill metrics from `/skill-metric/` in `get_forecast_stats()` (`apps/forecast_dashboard/src/db.py:420-444`) and all-station skill metrics in `get_forecast_stats_all()` (`apps/forecast_dashboard/src/db.py:447-489`). `get_data()` merges those persisted values into forecasts on `["code", horizon_in_year, "model_short"]` at `apps/forecast_dashboard/src/db.py:633-675`. The all-station skill table also uses the same API-backed cache path (`apps/forecast_dashboard/dashboard/data_manager.py:148-156`, `apps/forecast_dashboard/dashboard/plot_manager.py:415-434`). Once recalculation writes correct `n_pairs` and metric values, the dashboard will read them.

Related dashboard plans touch neighboring display bugs, not this recalc root cause. `high_prio_gi_draft_dashboard_skill_metrics_model_long_mismatch.md` proposes removing `model_long` from merge keys (`doc/plans/issues/high_prio_gi_draft_dashboard_skill_metrics_model_long_mismatch.md:68-84`), which current code already reflects at `apps/forecast_dashboard/src/db.py:658-675`. `high_prio_gi_draft_dashboard_duplicate_forecasts_skill_merge.md` proposes keeping the latest skill metric per key (`doc/plans/issues/high_prio_gi_draft_dashboard_duplicate_forecasts_skill_merge.md:58-91`), which current code reflects at `apps/forecast_dashboard/src/db.py:441-443` and `apps/forecast_dashboard/src/db.py:486-488`. `high_prio_gi_draft_dashboard_daily_ml_forecast_limit_truncation.md` concerns raw DAY forecast display truncation (`doc/plans/issues/high_prio_gi_draft_dashboard_daily_ml_forecast_limit_truncation.md:13-31`) and is independent of skill-metric recalculation.

Long-term month/quarter/season skill metrics are not affected by PP-036. The bimonthly long-term wrapper only runs `MONTHLY`, `QUARTERLY`, and `SEASONAL` (`bin/bimonthly_long_term_skill_metrics_recalculation.sh:100-108`). In `recalculate_skill_metrics.py`, those modes call `read_monthly_forecasts()`, `read_quarterly_forecasts()`, and `read_seasonal_forecasts()` at `apps/postprocessing_forecasts/recalculate_skill_metrics.py:238-346`; those readers use `_read_long_forecasts_api()` and the `long_forecasts` endpoint/table (`apps/postprocessing_forecasts/src/data_reader.py:994-1090`, `apps/postprocessing_forecasts/src/data_reader.py:2528-2647`), not `_read_ml_forecasts_pp_api()`.

## Desired Behaviour

For `horizon_type in {"pentad", "decad"}`, `_read_ml_forecasts_pp_api(model, horizon_type, codes, start_year, end_year)` must:

1. Query both archives for the same model, station scope, and date range: `horizon="day"` and `horizon="pentad"` or `horizon="decade"`.
2. Preserve pagination and station scoping for both archives.
3. Compute `first_day_date` from raw DAY rows after `_clean_code_column()`, per `(code, model_type)` pair. This is structurally feasible because `_clean_code_column()` is a local helper (`apps/postprocessing_forecasts/src/data_reader.py:1394-1398`) and `_normalize_ml_forecasts()` is called only after `_read_ml_forecasts_pp_api()` returns (`apps/postprocessing_forecasts/src/data_reader.py:2149-2152`).
4. Log a WARNING if any `(code, model_type)` has DAY rows older than its earliest period-archive row. That should not happen and signals upstream data inconsistency, but it must not abort recalculation.
5. Keep all DAY rows from each pair's `first_day_date` forward.
6. Append PENTAD/DECADE rows only for issue dates older than that pair's `first_day_date`.
7. If a station/model has no DAY rows at all, return the PENTAD/DECADE rows exactly as the current fallback does.
8. If a station/model has only DAY rows and the period-archive query is empty, return DAY rows and continue normally.
9. If a station/model has DAY rows but a missing DAY issue date inside the DAY era, do not fill that gap from the PENTAD/DECADE archive.
10. Preserve the daily target fan in raw DAY rows so `_normalize_ml_forecasts()` can average the in-period targets correctly.
11. Do not raw-deduplicate on `(code, date, model_type)`, because DAY rows intentionally contain multiple targets for the same issue date. The smaller safe implementation is to filter the period archive by DAY-era cutover before normalization, keeping `_normalize_ml_forecasts()` archive-agnostic. Computing the cutover after normalization would force `_normalize_ml_forecasts()` to understand archive provenance, breaking its current contract as a pure raw-API normalizer.
12. Ensure the normalized downstream forecast output has at most one row per `(code, date, model_short)`; DAY wins any overlap and no period is double-counted.

## Edge Cases Requiring Tests

- Station `19999` has only PENTAD/DECADE history and no DAY rows: return period rows and normalize to period forecasts.
- Station `19999` has only DAY history and no PENTAD/DECADE rows: function returns the DAY-derived normalized rows unchanged and does not crash when the period-archive query returns empty.
- Station `19999` has DAY and PENTAD/DECADE rows on the same issue date: normalized output uses DAY-derived values and has no duplicate `(code, date, model_short)`.
- Station `19999` has DAY rows, then a DAY-era outage date where period rows exist: do not use period rows for that in-era gap.
- Station `19999` has no ML history in either archive: preserve current `None`/empty behavior.
- Recalc input straddles the DAY cutover: older PENTAD/DECADE rows plus newer DAY rows both contribute.
- Full-history synthetic recalc produces `n_pairs >= 15` for at least one `(code, model_short, horizon_in_year)` after the fix; the same test should fail on `maxat_sapphire_2` because the early return leaves only the DAY-era pairs.

## Phase P1 - Tests First

**Goal:** Add failing tests that lock the archive-union contract before implementation.

**Files allowed:**

- `apps/postprocessing_forecasts/tests/test_data_reader.py`
- `apps/postprocessing_forecasts/tests/test_ml_horizon_archive_split.py` (new, preferred if the existing file is too large)
- `apps/postprocessing_forecasts/tests/test_data_reader_ml_aggregation.py` only if an existing normalization assertion needs extension

**Forbidden:** code changes under `apps/postprocessing_forecasts/src/` and all changes under `sapphire/services/`.

**Depends on:** none.

**Agents:** 1 Sonnet 4.6 general-purpose agent, `isolation: "worktree"`.

**Agent instructions:**

- Add unit tests around `_read_ml_forecasts_pp_api()` using a fake `SapphirePostprocessingClient`.
- Use station code `19999` only.
- Do not call `date.today()` or `datetime.now()` in tests; use fixed dates and explicit `start_year`/`end_year`.
- Cover the seven edge cases above, including the DAY-only branch where the period archive is empty.
- Update or replace the current assertions at `apps/postprocessing_forecasts/tests/test_data_reader.py:2659-2725` that encode "return DAY when present" as the desired behavior.
- Assert the sequence of API calls includes both `horizon="day"` and the period horizon for pentad and decad.
- Include pagination coverage for both archives if the fake client can do so without brittle mock ordering.
- Do NOT change any existing function signatures, data flow logic, or control flow.

**Acceptance criteria:**

- New tests fail against the current code because PENTAD/DECADE is never read when DAY is non-empty.
- Existing normalization tests still pass.
- No test uses a real station code, operational discharge value, external API, service DB, or wall-clock date.

## Phase P2 - Reader Implementation

**Goal:** Fix the archive read/merge behavior with the smallest safe change.

**Files allowed:**

- `apps/postprocessing_forecasts/src/data_reader.py`

**Forbidden:** all files under `sapphire/services/`; public API helper signature changes; changes to `calculate_skill_metrics()`; changes to long-term readers.

**Depends on:** P1.

**Agents:** 1 Sonnet 4.6 general-purpose agent, `isolation: "worktree"`.

**Agent instructions:**

- Do NOT change any existing function signatures, data flow logic, or control flow outside the specific behavior described here.
- Keep `_read_ml_forecasts_pp_api(model, horizon_type, codes=None, start_year=None, end_year=None)` signature unchanged.
- Extract the existing per-horizon paginated fetch loop into a private helper in the same file, or otherwise remove duplication without changing callers.
- Query `day` and `api_horizon` unconditionally when the API is available and ready.
- Add a helper that merges raw archives by DAY-era cutover:
  - Clean `code` with `_clean_code_column()` and parse `date` before computing cutovers.
  - Compute `first_day_date` from raw DAY rows per `(code, model_type)` pair.
  - Log a WARNING, but do not abort, if a pair has DAY rows older than its earliest period-archive row.
  - For each `(code, model_type)` pair found in DAY rows, keep period rows with `date < first_day_date`.
  - For pairs with no DAY rows, keep all period rows.
  - Concatenate all DAY rows with retained period rows.
  - Do not raw-deduplicate DAY rows on `(code, date, model_type)`.
  - Drop any temporary columns before returning.
- Log separate row counts for DAY, period, retained period, and final rows.
- Preserve current behavior for API unavailable, API disabled, readiness failure, and both archives empty.

**Acceptance criteria:**

- P1 tests pass.
- `_read_ml_forecasts_pp_api()` makes both API requests when DAY rows exist.
- The function returns `None` when both archives are empty, matching current contract.
- No `date.today()` or `datetime.now()` is added.
- `read_individual_model_forecasts()` at `apps/postprocessing_forecasts/src/data_reader.py:2090-2173` does not need a signature change.

## Phase P3 - Recalc Integration Test

**Goal:** Prove the full short-term recalc path sees the longer ML history and produces realistic `n_pairs`.

**Files allowed:**

- `apps/postprocessing_forecasts/tests/test_ml_horizon_archive_split.py`
- `apps/postprocessing_forecasts/tests/test_wiring_integration.py` only if that suite already has the right entry-point fixtures
- `apps/postprocessing_forecasts/tests/test_recalc_workflow.py` only for narrow entry-point assertions

**Forbidden:** real API/DB writes; all changes under `sapphire/services/`.

**Depends on:** P2.

**Agents:** 1 Sonnet 4.6 general-purpose agent, `isolation: "worktree"`.

**Agent instructions:**

- Build an in-memory fake for observed runoff and ML forecast API responses.
- Do NOT change any existing function signatures, data flow logic, or control flow.
- Use fixed years and set `SAPPHIRE_SKILL_METRICS_START_YEAR` explicitly so the test is independent of the machine date.
- Exercise the pipeline through `read_observed_and_modelled_data()` plus `calculate_skill_metrics()`, or through `_run_short_term_recalc()` with `file_writer.save_skill_metrics()` monkeypatched to capture the DataFrame.
- Construct data so the current early return would yield only one or two DAY-era pairs for a period group, while the fixed reader yields at least fifteen pairs.
- Assert `n_pairs >= 15` for station `19999` and at least one ML model on both pentad and decad paths if runtime is acceptable; otherwise make decad a separate unit-level regression and pentad the full integration case.
- Construct a mixed-coverage Neural Ensemble fixture where TFT and TiDE have full PENTAD/DECADE history but TSMixer has only DAY-era rows. The existing `calculate_neural_ensemble_forecast()` and decad variant build NE from whatever target-model rows are available per date by grouping and averaging (`apps/iEasyHydroForecast/setup_library.py:3822-3877`, `apps/iEasyHydroForecast/setup_library.py:3892-3940`), so assert NE exists on historical dates as a partial average of TFT/TiDE and is not silently NaN-filled or absent. Also assert DAY-era NE includes TSMixer once TSMixer rows exist.

**Acceptance criteria:**

- Test fails on the current early-return implementation and passes after P2.
- No test writes to PostgreSQL, SQLite service tables, local production CSVs, or `sapphire/services/`.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` passes with zero unexpected skips.

## Phase P4 - Operational Verification

**Goal:** Verify the fix on staging before Tajik deployment cutover.

**Files allowed:** none; operator commands only.

**Depends on:** P3.

**Agents:** 0; operator runs commands, orchestrator reviews results.

**Operator commands:**

```bash
BASE_URL="http://localhost:8000/api/postprocessing"

# Confirm the archive split for a placeholder station code. Substitute the
# real station code only in the operator shell; do not commit it to docs.
for H in day pentad decade; do
  curl -s "${BASE_URL}/forecast/?horizon=${H}&code=19999&model=TFT&start_date=2000-01-01&end_date=2026-12-31&limit=100000" \
    | jq -r --arg h "$H" '[.[].date] | {horizon:$h, rows:length, first:(min // null), last:(max // null)}'
done
```

Run one staging recalculation after deployment. If running locally inside the postprocessing app environment:

```bash
cd apps/postprocessing_forecasts
SAPPHIRE_PREDICTION_MODE=BOTH \
SAPPHIRE_RECALC_STATION_CODE=19999 \
SAPPHIRE_SKILL_METRICS_START_YEAR=2009 \
SAPPHIRE_SKILL_METRICS_YEAR=2026 \
uv run python recalculate_skill_metrics.py
```

If running the deployed Docker job, use the existing annual or initialization path and verify the same station after completion.

API-level assertion. The `date` filter below is intentional: `_write_skill_metrics_to_api()` maps each `horizon_in_year` to a target-year period date from `SAPPHIRE_SKILL_METRICS_YEAR`/`skill_metrics_year`, not to the recalc runtime (`apps/postprocessing_forecasts/src/api_writer.py:438-440`, `apps/postprocessing_forecasts/src/api_writer.py:543-555`).

```bash
for H in pentad decade; do
  curl -s "${BASE_URL}/skill-metric/?horizon=${H}&code=19999&start_date=2026-01-01&end_date=2026-12-31&limit=10000" \
    | jq -r '
      group_by(.model_type)[] |
      {model: .[0].model_type,
       rows: length,
       min_n_pairs: ([.[].n_pairs] | min),
       max_n_pairs: ([.[].n_pairs] | max),
       avg_n_pairs: (([.[].n_pairs] | add) / length)}'
done
```

Optional SQL assertion inside the postprocessing DB:

```sql
SELECT horizon_type,
       code,
       model_type,
       COUNT(*) AS rows,
       MIN(n_pairs) AS min_n_pairs,
       ROUND(AVG(n_pairs)::numeric, 2) AS avg_n_pairs,
       MAX(n_pairs) AS max_n_pairs
FROM skill_metrics
WHERE code = '19999'
  AND horizon_type IN ('PENTAD', 'DECADE')
  AND date >= DATE '2026-01-01'
  AND date <= DATE '2026-12-31'
GROUP BY horizon_type, code, model_type
ORDER BY horizon_type, model_type;
```

**Acceptance criteria:**

- Archive probe shows DAY and period archives are both queryable for the chosen station/model, or documents that a given model has only period rows.
- After one recalc, at least one `(code, model_type)` pair per short-term horizon has `n_pairs >= 15`.
- No model regresses to duplicate skill rows per `(code, horizon_in_year, model_type)` in the latest recalculation year.
- Dashboard skill tables and plots consume updated API values without dashboard code changes.

## Risks And Mitigations

- **Tests writing to DB:** avoid service clients and monkeypatch all writers. Use in-memory fakes and captured DataFrames.
- **PostgreSQL-only upsert behavior vs SQLite tests:** PP-036 should not test upsert semantics. If any integration test uses SQLite, keep it read-only or capture writes before they reach the service layer.
- **Long-term regression:** `_read_long_forecasts_api()`, `read_monthly_forecasts()`, `read_quarterly_forecasts()`, and `read_seasonal_forecasts()` are separate (`apps/postprocessing_forecasts/src/data_reader.py:994-1090`, `apps/postprocessing_forecasts/src/data_reader.py:2528-2647`). P2 must not touch them; run existing monthly/quarterly/seasonal tests as a guard if the agent edits shared helpers.
- **LR path regression:** LR pentad/decad reads go through `_read_lr_forecasts_pp_api()`, which P2 must not modify. The structural isolation is enforced by the P2 file allow-list (only `apps/postprocessing_forecasts/src/data_reader.py`) and the literal "no signature/data-flow/control-flow change" constraint. Orchestrator deliberation after P2 must explicitly confirm `_read_lr_forecasts_pp_api()` and its callers are unchanged in the diff; this replaces an earlier draft requirement for a byte-identical LR skill baseline fixture, which would have added significant fixture/capture complexity for a contract the file allow-list already enforces.
- **Raw dedupe collapsing DAY fan:** do not raw-deduplicate on `(code, date, model_type)`. Use DAY-era period filtering before normalization, or normalize separately and deduplicate normalized rows.
- **Silent fill of DAY-era outages:** period rows after first DAY issue date must be discarded for that station/model, even if DAY has a missing issue date.
- **Existing NE partial-average behavior:** Neural Ensemble is currently computed from whatever base ML models are available for each date, so historical dates where TSMixer is absent can still get NE from TFT/TiDE. PP-036 preserves this behavior; changing it to require all three base models is a separate modelling decision.
- **Unverified DAY cutoff:** repository evidence does not prove the running deployment's first DAY dates. P4 includes operator archive probes before and after recalculation.
- **Runtime and API volume:** reading both archives increases API calls. Preserve pagination, station scoping, and date bounds; log separate row counts for observability.

## Whole-Fix Acceptance Criteria

- `_read_ml_forecasts_pp_api()` reads both DAY and PENTAD/DECADE archives for ML models and returns a non-double-counted union according to the DAY-era cutover rule.
- Existing public function signatures remain unchanged unless an implementer documents why a private helper cannot preserve them.
- New tests fail on `maxat_sapphire_2` without the fix and pass with the fix.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` passes with zero unexpected skips.
- On staging, after one recalculation, `n_pairs >= 15` for at least one `(code, model_type)` pair per short-term horizon using the API/SQL checks above.
- No code under `sapphire/services/` is modified.
- No dashboard code change is required for PP-036.
- No regression in long-term month/quarter/season skill metrics or forecast readers.

## Assumptions For Operator Confirmation

- The running Kyrgyz deployment's DAY archive starts around April 2024 for TFT/TiDE and later for TSMixer.
- The PENTAD/DECADE archive contains the migrated long history needed for each Tajik station before the first recalc.
- The placeholder station code `19999` in examples will be replaced only in operator shells, not committed to repo docs.
- API enum query parameters continue accepting lowercase `day`, `pentad`, and `decade` even though the PostgreSQL enum stores uppercase names.

## Open Questions

- Should `calculate_skill_metrics()` receive an explicit forecast/reference date in a follow-up to remove the current `dt.date.today().year - 20` default?
- Should `bin/utils/run_skill_metrics_recalc.sh` pass through `SAPPHIRE_RECALC_STATION_CODE` for easier staging verification, or should scoped recalc remain a local/dashboard-only path?

## Dependency Graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 0 }
  }
}
```

## Red-Phase Verification Log

**2026-05-28** — Verified the P1 and P3 test commits genuinely guard the P2
fix. Procedure: created an isolated worktree at HEAD (`a32a11f` P1 +
`c228122` P2 + `755addf` P3), reverted only `c228122` with
`git revert --no-commit`, and ran the postprocessing_forecasts test suite.

Revert scope was clean: only `apps/postprocessing_forecasts/src/data_reader.py`
changed (210 lines, +63/-147), no test files touched.

**Result:** 1320 passed / 10 failed / 0 skipped. All 10 failures came from
`postprocessing_forecasts/tests/test_ml_horizon_archive_split.py`:

- 8 P1 unit failures in `TestMlHorizonArchiveSplit` covering DAY-only
  history preservation, DAY/PERIOD dedup on same issue date, DAY-era outage
  guard, cutover merge, API call sequencing for both pentad and decad
  parametrizations, and paginated union.
- 2 P3 integration failures in `TestMlHorizonArchiveSplitIntegration`
  (pentad and decad) — both reproduced the PP-036 starvation end-to-end:
  `tft_skill["n_pairs"].max() == 1`, expected `>= 15`.

No PP-036 test passed without the fix (no Type-II errors), and no
non-PP-036 test failed (revert did not bleed scope). Worktree removed
afterwards. The fix is verified.
