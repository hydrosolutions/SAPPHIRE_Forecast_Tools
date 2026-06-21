# MIG-008: Long-forecast from-file importer collapses quarter/season to horizon_value=0

**Status**: Draft
**Priority**: Mid
**Module**: migration-toolkit (`bin/initialize_long_forecast_history.sh`, `bin/utils/migration_py/long_forecast.py`)
**Depends on**: MIG-007 (the importer must first accept `quarter`/`season` before this limitation is reachable)

## Problem

The long-term from-file importer derives a record's `horizon_value` from the mode
config's `operational_month_lead_time`. For `month_N` modes this is correct: each
`month_1.json` ... `month_N.json` carries a distinct lead, so the importer
reproduces the operational `MONTH` buckets (hv 0..3).

`quarter` and `season` have only a **single config file each**
(`quarter.json`, `seasonal_april.json`), both with `operational_month_lead_time: 0`.
So the importer can only ever write **`horizon_value=0`** for those horizon types.
The operational long-term pipeline, by contrast, populates a multi-bucket structure:

| horizon_type | operational buckets in DB | from-file importer can write |
|---|---|---|
| MONTH | hv 0..3 (per `month_N` config) | hv 0..3 (correct) |
| QUARTER | hv 1, 2, 3, 4 | hv 0 only |
| SEASON | hv 0, 1, 2, 3 | hv 0 only |

## Impact

- **QUARTER**: a from-file import lands in `QUARTER hv=0`, an **orphan bucket
  disjoint from the operational `hv 1..4`**. Skill metrics and the dashboard that
  read the operational quarter buckets do not see it. The from-file path therefore
  cannot backfill the operational quarter forecasts. (For this reason the local
  quarter write was deliberately **held**.)
- **SEASON**: a from-file import lands in `SEASON hv=0` only. The `hv 0` write is
  itself valid (the April-issued seasonal forecast), and was applied locally as a
  clean additive gap-fill (sentinel verification: 62 -> 79 stations at `hv 0`,
  +731 rows, no overwrite of `hv 1..3`). But the operational `hv 1..3` seasons
  cannot be reconstructed from the single `seasonal_april` config.

## Root cause

`horizon_value` is derived from a **lead-time** concept (`operational_month_lead_time`)
rather than from the **forecast target index** (which quarter / which seasonal issue
point). The from-file config set lacks per-quarter / per-season config files, so
there is no input from which the importer could derive hv 1..4.

This is a configuration / derivation limitation, not a defect in the MIG-007 import
logic (which faithfully writes whatever the config specifies).

## Options

1. **Per-target config files** (mirrors the `month_N` pattern): add
   `quarter_1.json` ... `quarter_4.json` and the seasonal variants, each carrying the
   correct target index, and let the existing derivation produce the right hv.
   Requires authoring the configs and confirming how the operational pipeline assigns
   quarter/season hv.
2. **Derive hv from the source CSV target** (e.g. map the forecast date to its
   quarter / seasonal index) instead of the config lead. More invasive in
   `long_forecast.py`; must match the operational convention exactly.
3. **Document the limitation**: from-file backfill supports MONTH (multi-lead) plus
   the single-bucket `QUARTER hv0` / `SEASON hv0`; operational `QUARTER hv1..4` /
   `SEASON hv1..3` must come from operational reruns, not the from-file importer.

## Recommendation

Confirm first **how the operational `long_term_forecasting` pipeline assigns
`horizon_value` for quarter and season** (target index vs lead). That answer decides
between option 1 (config-only) and option 2 (importer change). Until then, treat
from-file quarter/season backfill as `hv=0`-only and rely on operational reruns for
the other buckets (option 3 as the interim contract).

## Scope

- `bin/utils/migration_py/long_forecast.py` (hv derivation), the wrapper, and the
  `long_term_configs/` config set. **No `sapphire/services/**` changes.**
- Sentinel station codes only in any tests/examples (`19999`); no real codes or
  discharge values.

## Evidence (local, sentinel-safe aggregates)

- `quarter` dry-run derived `horizon_type_enum=QUARTER horizon_value=0`, target
  cutoff map empty (entries=0) -> would create a fresh `QUARTER hv=0` of 17 stations
  / 4876 rows, disjoint from operational `QUARTER hv 1..4` (78-79 stations each).
- `seasonal_april` write derived `SEASON hv=0`, additive: `SEASON hv0` 62 -> 79
  stations, 2940 -> 3671 rows; `hv 1..3` unchanged.
