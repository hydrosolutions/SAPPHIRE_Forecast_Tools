# MIG-007: Long-term from-file importer needs multihorizon support and code-scoped gap-fill

**Status**: Review - implemented on `fix_migration_long_forecast_multihorizon`; awaiting owner review.
**Implementation commits**: `71c1141` (P1-P3: horizon allow-list, wrapper enum derivation, code-scoped cutoff map), `767138d` (P4: multihorizon and canary gap-fill tests).
**Module**: migration-toolkit (`bin/initialize_long_forecast_history.sh` + `bin/utils/migration_py/long_forecast.py`)
**Priority**: **High** - deployment data-completeness. Quarter and season long-term forecast history can be blocked or under-imported by the from-file migration path.
**Labels**: `migration-toolkit`, `long-term`, `from-file-backfill`, `horizon_type`, `gap-fill`, `data-completeness`
**Discovered**: 2026-06-19, local long-term DB verification during the `doc/prod/backfill_ml_fromfile.md` process.
**ID note**: This is MIG-007, not MIG-006. MIG-006 remains reserved for a possible scoped ML reaggregation follow-up and has no committed index row.
**Related**:
- `doc/prod/backfill_ml_fromfile.md` - documents the backfill workflow where this importer limitation surfaced.
- `doc/plans/archive/longforecast_importer_multihorizon_gapfill_plan.md` - implementation plan and phase evidence for this issue.
- `high_prio_gi_draft_update_migration_p5_long_forecast.md` - original month-focused long-forecast importer.
- **MIG-003** (`high_prio_gi_draft_migration_horizon_type_case_coercion.md`) - dependency: merged uppercase PG enum-label fix (`450ad7f`, PR #362). MIG-007 must preserve the `horizon_type::text='MONTH|QUARTER|SEASON'` lesson while removing month-only assumptions.
- **LTF-006** (`high_prio_gi_draft_ltf_gbt_base_modeltype_gap.md`) - adjacent `GBT_Base` model-type 422, explicitly out of scope here.

> Sentinel station code `19999` only; no real station codes or discharge values in this file.

---

## Symptom

The failure is not simply "season has 0 rows." The real class is:

1. the importer rejected `quarter` / `season` config values before it could write those horizons; and
2. once a horizon was partially populated, the wrapper/Python cutoff path was code-blind.

That means a canary-populated horizon can look partly successful: the canary station is present, while the remaining intended stations are starved by the same horizon-level cutoff and never backfilled from file.

This is a toolkit issue. The postprocessing service contract already has `MONTH`, `QUARTER`, and `SEASON` horizon enum values, and this issue does not require edits under `sapphire/services/**`.

## Verified Pre-Fix State

The target branch state before MIG-007 showed three independent blockers:

1. `bin/utils/migration_py/long_forecast.py` read `horizon_type` from each config and lowercased it, but `_ALLOWED_HORIZON_TYPES` allowed only `{"month"}`. `quarter` and `season` configs failed closed before import.
2. `bin/initialize_long_forecast_history.sh` hard-coded `horizon_type::text='MONTH'` in both target-state cutoff query paths. These sites must be located by symbol because line numbers drifted during P1-P3; the relevant code is `query_cutoff_map_state` and `query_legacy_scalar_target_state`, plus the `MODE_HORIZON_ENUM` derivation in the mode-detection block.
3. `bin/utils/migration_py/long_forecast.py` applied one scalar cutoff to every row. One populated station in a horizon could cutoff-filter rows for stations that had no target rows yet.

MIG-003 is a required dependency: SQL against the PG enum must use uppercase enum labels via `horizon_type::text`, not lowercase enum literals.

## Root Cause

### 1. Importer horizon allow-list was month-only

The Python importer already parsed `horizon_type` from config, but the validation allow-list rejected anything except `month`. This made the config value visible but unusable for quarter/season history imports.

### 2. Wrapper cutoff SQL assumed MONTH

The wrapper decided `full-import` vs `pre-cutoff` from the target `long_forecasts` table. Both the global target-state path and the per-mode path were month-specific. Fixing only the Python allow-list would still leave quarter/season modes using month target state for cutoff decisions.

The fix must derive the PG enum label from an internal allow-list:

- `month -> MONTH`
- `quarter -> QUARTER`
- `season -> SEASON`

The raw config string must never be interpolated into SQL.

### 3. Cutoff was horizon-scoped, not code-scoped

The old scalar cutoff was effectively one `MIN(date)` for the selected target state. In a partially populated horizon, a canary station with recent rows could make every source row for missing stations appear "already covered" and get filtered out.

The correct default is a cutoff map keyed by `(horizon_type, horizon_value, code)`:

- code present in the map: keep existing pre-cutoff behavior for that code; import rows strictly older than that code's target `MIN(date)`.
- code absent from the map: treat as an empty target for that code; import its source rows.

## Required Fix Scope

1. **Accept quarter/season in the importer.** Extend the internal allow-list from `month` to `month|quarter|season`; keep fail-closed behavior for unsupported values such as `week` or `pentad`. The config's normalized `horizon_type` must carry into the POST payload.
2. **Use the mode's horizon enum in cutoff queries.** The wrapper must derive `MODE_HORIZON_ENUM` from the mode config via an explicit allow-list and use it in both target-state query paths. The primary grouped query and the legacy scalar fallback must not hard-code `MONTH`.
3. **Add code-scoped gap-fill.** Replace the single global cutoff filter with a per-row lookup in a cutoff map keyed by normalized `(horizon_type, horizon_value, code)`. The map is generated by the wrapper, mounted read-only into the helper container, and consumed through `--cutoff-map`.

## Gap-Fill Contracts

### S1: cutoff-map key normalization

The wrapper builds the map from `psql`, which emits uppercase `horizon_type::text`, textual `horizon_value`, and raw `code`. Python lookups use lowercase `horizon_type`, integer `horizon_value`, and `_parse_code`-normalized `code`.

Without normalization, the map silently no-ops: every row misses the key and behaves like an empty target. That recreates the MIG-003 case trap at the wrapper-to-Python boundary.

MIG-007 therefore requires normalized map keys on both generation/load and lookup:

- lowercase horizon type;
- integer horizon value;
- `_parse_code`-normalized code.

### S2: known residual

This is not a general gap-filler. Per-code cutoff uses each code's target `MIN(date)` and imports only rows strictly older than that date.

It fills:

- zero-row codes, because there is no cutoff entry;
- recent contiguous-block codes, because a recent `MIN(date)` allows older history to backfill.

It does not fill:

- interior gaps between two populated spans;
- codes whose only existing target rows are old, because the old `MIN(date)` cuts off the rest of history.

### S5: `--allow-global-cutoff` gate

The primary cutoff-map path needs no operator opt-in because it is code-scoped. `--allow-global-cutoff` applies only to the retained legacy scalar `--cutoff` fallback. If cutoff-map generation fails while target rows exist, the wrapper must fail closed unless the operator explicitly accepts the legacy scalar fallback.

## Implementation Status

Implemented on `fix_migration_long_forecast_multihorizon`:

- `71c1141`: P1-P3 implementation:
  - allowed `month`, `quarter`, and `season` in `long_forecast.py`;
  - derived wrapper enum labels from an allow-list;
  - replaced month-only target-state cutoff logic with a grouped cutoff-map path;
  - added `--cutoff-map` and per-row cutoff lookup, with missing entries treated as no cutoff.
- `767138d`: P4 tests:
  - canary gap-fill behavior covered with sentinel-only dry-run/unit tests;
  - quarter and season covered through the dry-run import path;
  - MIG-003 wrapper SQL tests kept green for `MONTH`, `QUARTER`, and `SEASON`.

## Acceptance Criteria

- [x] Importer accepts `horizon_type` values `month`, `quarter`, and `season`; unsupported values still fail closed.
- [x] Quarter and season configs carry their normalized `horizon_type` into generated records.
- [x] Wrapper target-state SQL derives the PG enum label from `MODE_HORIZON_ENUM`, preserving the MIG-003 uppercase enum-label pattern.
- [x] Both target-state paths no longer assume hard-coded `MONTH`.
- [x] Cutoff map is keyed by normalized `(horizon_type, horizon_value, code)`.
- [x] A code missing from the cutoff map imports as an empty target instead of being skipped.
- [x] Canary gap-fill test proves one populated station does not starve missing stations in the same season horizon.
- [x] S1 key-normalization regression test covers uppercase/text/raw map input matching lowercase/int/normalized Python lookup.
- [x] `--allow-global-cutoff` applies only to the legacy scalar fallback, not the primary map path.
- [x] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh iEasyHydroForecast` passed on the implementation branch.
- [x] No `sapphire/services/**` edits.
- [x] Sentinel codes only in committed tests/docs; no real station codes or discharge values.

## Out of Scope

- **LTF-006**: `GBT_Base` is not a valid service `ModelType` and can still produce 422 responses. That is separate service/model-contract work.
- **ML sibling learnings from section 13**: the ML from-file backfill sibling's broader full-import/replay considerations are not part of this long-forecast importer fix.
- **Taj monthly lead-3 config/data**: missing source config or data for a deployment is separate from this importer accepting and gap-filling configured horizons.
- Generic GBT ensemble-column naming changes. MIG-007 does not broaden into model-output schema work.

## Rollback

Toolkit-only rollback is to revert the implementation commits on this branch. The service schema is unchanged. If a bad real import is ever run, data rollback should use the operator's pre-run database dump, not a service migration.
