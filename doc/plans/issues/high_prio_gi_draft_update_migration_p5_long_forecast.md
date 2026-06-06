# P5 — Long-term forecasts CSV-to-API wrapper (configured-mode hindcasts)

**Phase:** P5 of the update-time migration toolkit.
**Status:** Implementation complete; under review.
**Branch:** `feature_p5_long_forecast_history` → `develop_migration_toolkit`.
**Depends on:** P0 (helpers + manifest contract).
**Blocks:** P7 (final runbook stitching).

## Goal

Backfill `long_forecasts` table rows on a deployment server's
postprocessing DB by walking the operator's local
`long_term_predictions/` archive, mapping discovered (mode, model)
hindcast CSVs to API payloads, and POSTing them via the same Dockerised
stdlib-only pattern established by P1a / P1b / P3.

`long_forecasts` lives in the postprocessing service (NOT preprocessing).
The wrapper queries the postprocessing DB for MODE detection and POSTs to
`http://localhost:8003/long-forecast/`.

## Scope (file-level)

```
bin/initialize_long_forecast_history.sh                                   (NEW, 545 lines)
bin/utils/migration_py/long_forecast.py                                   (NEW, 804 lines)
apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py            (NEW, 935 lines, 33 tests)
apps/iEasyHydroForecast/tests/fixtures/migration_csv/long_forecast/month_1.json
apps/iEasyHydroForecast/tests/fixtures/migration_csv/long_forecast/LR_Base_hindcast.csv
apps/iEasyHydroForecast/tests/fixtures/migration_csv/long_forecast/GBT_hindcast.csv
doc/prod/update_data_migration_runbook.md                                 (MODIFY — §5.5 added)
doc/plans/issues/high_prio_gi_draft_update_migration_p5_long_forecast.md  (this file)
```

No edits to `sapphire/services/`, no edits to sibling-phase runbook
sections (§5.1–§5.4, §6.x).

## Distinctive shape vs sibling CSV-source wrappers

The P1a (runoff DAY) / P1b (meteo DAY) / P3 (hydrograph DAY) wrappers
each consume one fixed CSV file at a known path. P5 is structurally
different: the source is a tree of CSVs whose composition is data-driven.
Three discovery layers run before any POST:

1. **`_discover_modes(config_dir)`** — globs
   `config/long_term_configs/*.json` and returns the mode names with
   `monthly` hard-stripped (non-operational per
   `apps/long_term_forecasting/lt_schedule_query.py:54-91`).
2. **`_load_mode_config(config_dir, mode)`** — parses the mode's JSON
   and extracts `models_to_use` (per-family lists of models),
   `horizon_value` (from `operational_month_lead_time`), and
   `horizon_type` (defaults to `"month"` if absent). Raises typed errors
   on missing file / malformed JSON / missing `models_to_use` rather
   than silently producing degenerate state.
3. **`_discover_hindcast_csvs(data_root, mode, models)`** — checks
   `intermediate_data/long_term_predictions/<mode>/<model>/<model>_hindcast.csv`
   for each model in the config and returns only the pairs whose CSV
   exists on disk. Models declared in the JSON without a hindcast CSV
   are silently skipped (logged) — the wrapper does not POST empty
   payloads.

The mode-without-config case is the inverse: a directory at
`long_term_predictions/<mode>/` without a matching
`long_term_configs/<mode>.json` is hard-skipped at the
`_discover_modes` layer because there is no JSON to declare which models
to read. Architecture §Q3 locks this behaviour: no synthetic configs
from directory layout.

## UZB no-op acceptance (Stage E item #12)

When zero modes survive the discover-and-filter pipeline (e.g. a
deployment with no `long_term_configs/` JSON files, or one with only the
hard-skipped `monthly` config), the wrapper exits 0 with a logged
`[no source data for this deployment]` message. Returning a non-zero
exit code in this case would break the canonical SAPPHIRE deployment
workflow on minimal profiles (UZB demo, TAJ pre-deployment) where some
data families are not yet configured.

`test_main_dry_run_zero_modes_returns_no_source_message` and
`test_discover_modes_returns_empty_on_missing_directory` encode this
acceptance into the test suite.

## Per-model payload variance

Models within a single mode write different column sets to their
hindcast CSVs depending on family:

| Family | Point forecast | Quantiles | Ensemble | Special |
|--------|----------------|-----------|----------|---------|
| LR (`LR_Base`, `LR_SM`, …) | `Q_<model>` → `q` | `Q5/Q10/…/Q95` → `q05/q10/…/q95` | — | — |
| GBT (`GBT`, `SM_GBT`) | `Q_<model>` → `q` | as above | `Q_<model>_xgb/_lgbm/_catboost` → `q_xgb/q_lgbm/q_catboost` | — |
| MC_ALD | `Q_MC_ALD` → `q` | as above | — | `Q_loc` → `q_loc` |

The `_build_record` helper assembles each payload from whatever columns
are present in the row. The **universal safe-write rule** (architecture
§Q2 layer 2) applies: empty / NaN / unparseable cells are OMITTED from
the payload — never sent as null. The service-side `_has_changes` +
`setattr` upsert path means that omitting a key preserves the existing
DB value, while sending null would overwrite a populated cell with NULL.

The fixture set `LR_Base_hindcast.csv` (LR family minimum payload),
`GBT_hindcast.csv` (LR set + ensemble), and the unit tests
`test_build_record_LR_model_minimal_quantiles`,
`test_build_record_GBT_model_includes_ensemble_fields`,
`test_build_record_MC_ALD_includes_q_loc`, and
`test_build_record_excludes_null_fields` cover the four payload shapes.

## MODE detection (per mode, not per wrapper invocation)

Unlike P1a / P1b / P3 which detect MODE once per invocation (one CSV →
one target horizon → one query), P5 runs an independent MODE-detection
query per discovered mode because each mode has its own
`horizon_value`. The SQL is:

```sql
SELECT COUNT(*), MIN(date)::text
FROM long_forecasts
WHERE horizon_type = 'month' AND horizon_value = $1;
```

An empty result for one mode (e.g. `month_3`) but populated result for
another (`month_1`) results in full-import for the empty mode and
pre-cutoff (cutoff = the populated mode's `MIN(date)`) for the
populated one — within the same `bash` invocation. This avoids the
operator having to re-invoke the wrapper per mode.

## Forward interface contract (P0 lock)

`--station-filter <code>` is honoured per the P0-locked binding contract.
A station filter applied to long-term forecasts is uncommon in
production but is required by the canary single-station workflow.

`--mode <name>` and `--model <name>` restrict the run to a single mode
or single model across all modes. `--skip-mode <list>` lets the
operator add modes to the always-skipped `monthly` set.

`--dry-run` produces a per-(mode, model) inventory block before any
POSTs.

## Acceptance criteria

- [x] `bash bin/initialize_long_forecast_history.sh --help` returns 0 and
      documents all CLI flags including `--station-filter`.
- [x] `shellcheck -x bin/initialize_long_forecast_history.sh` passes
      (via pre-commit hook).
- [x] 33 tests in `test_initialize_long_forecast.py` all pass.
- [x] Module import audit: stdlib-only + intra-package allowed
      (`test_long_forecast_module_imports_only_stdlib_and_intra_package`).
- [x] `monthly` is hard-skipped at the discovery layer
      (`test_discover_modes_skips_monthly`).
- [x] Modes without a config JSON are hard-skipped
      (`test_load_mode_config_raises_on_missing_json`).
- [x] UZB no-op behaviour: zero modes → exit 0 with `no source data` log
      line.
- [x] Per-family payload shape encoded: LR (q + quantiles), GBT (LR +
      ensemble), MC_ALD (LR + q_loc).
- [x] Universal safe-write rule: empty / NaN / unparseable cells are
      omitted from payloads.
- [x] Runbook §5.5 added (operator procedure: dry-run inventory →
      canary → full population → acceptance SQL).
- [x] No real station codes in fixtures (sentinel 19999 only).
- [x] No edits to `sapphire/services/` or sibling-phase runbook
      sections.

## Known limitations & follow-ups

- The wrapper does NOT currently validate that the source CSV's
  `code` values appear in the org's configured station list — it trusts
  the discovery archive layout. If a deployment's
  `long_term_predictions/` archive contains rows for a different org's
  stations (e.g. mixed TAJ+KGHM data), those rows will be POSTed without
  warning. Mitigation: use `--station-filter` for canary runs to
  validate the target station set before committing to a full run.
- No explicit handling for the case where a JSON config declares a
  model that has a hindcast CSV but with a different column structure
  (e.g. an LR-family model whose CSV is missing the `Q5/Q25/…` quantile
  columns). The `_build_record` helper would emit a payload with just
  `q` and let the service decide. This is intentional per the safe-write
  rule but operators should verify post-run row counts per model
  against expectations.
- `valid_from` / `valid_to` are read directly from the source CSV;
  there is no synthesis from the mode's `horizon_value` if the CSV is
  missing those columns. The `_build_record_returns_none_for_missing_required`
  test ensures rows missing any required key are dropped rather than
  POSTed with synthetic values.

## Reviewer checklist

- [ ] Per-model payload shape matches `LongForecastDataMigrator` in
      `sapphire/services/postprocessing/app/data_migrator.py` (read-only
      reference).
- [ ] Per-mode MODE detection query is sound (independent cutoffs per
      `horizon_value`).
- [ ] `monthly` skip + missing-config skip are surfaced in operator
      logs so the operator notices the hard-skip rather than expecting
      silent inclusion.
- [ ] UZB no-op acceptance behaviour is intentional, not a fallthrough
      bug.

## Test summary

```
SAPPHIRE_TEST_ENV=True bash apps/run_tests.sh iEasyHydroForecast
# 33 new long-forecast tests pass; full suite green.
```

## Charter compliance

Implemented under Sub-Orchestrator Charter v2. File-scope discipline
hard-enforced (no edits outside the listed files). No `sapphire/services/`
edits. No real station codes anywhere. No external review escalation
(authorised by user as part of the parallel-spawn batch).
