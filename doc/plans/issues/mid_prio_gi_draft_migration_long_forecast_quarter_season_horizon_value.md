# MIG-008: Quarter/season horizon_value needs a per-bucket config convention (the service is hv-agnostic)

**Status**: Draft (blocked on a producer-side convention decision)
**Priority**: Mid
**Module**: migration-toolkit + `apps/long_term_forecasting` (hindcast/config production); the
postprocessing **service stores hv but does not define it**
**Depends on**: MIG-007 (the importer must accept `quarter`/`season` before this is reachable)
**See also**: `doc/prod/longforecast_quarter_season_hv_convention.md` (the question posed to the
service owner / modeller)

## Summary

The from-file backfill writes `horizon_value=0` for every quarter/season row. Investigation
shows this is **not an importer bug** and **not fixable in the importer alone**: `horizon_value`
has no derivation anywhere in the stack and no meaning imposed by the service. It is a
**producer-side convention** carried by the config field `operational_month_lead_time`, and the
quarter/season configs needed to express the desired buckets do not exist in this repo.

## What the service does (postprocessing) - it is hv-agnostic

- `app/models.py`: `LongForecast.horizon_value = Column(Integer, nullable=False)` - a plain int in
  the natural key `(horizon_type, horizon_value, code, date, model_type, valid_from, valid_to)`.
- `app/schemas.py:50`: `horizon_value: int` - **no validator, no range, no quarter/season rule.**
  The API accepts any integer.
- `app/data_migrator.py:769`: stamps `"horizon_value": self.horizon_value` on every record;
  `:669` sets that from `config["operational_month_lead_time"]`; `--type longforecast` loops
  configs (`:877`), one config -> one constant hv for all its rows.

The service stores whatever int it is sent. It derives nothing and validates no semantic.

## All three writers are identical

Every writer stamps `horizon_value = operational_month_lead_time` (a hand-set constant per config),
and **none derives hv from a date**:

| Writer | Where |
|---|---|
| Service migrator (colleague-owned) | `sapphire/services/postprocessing/app/data_migrator.py:669,769` |
| Operational pipeline | `apps/long_term_forecasting/run_forecast.py:269,409` -> `config_forecast.py:231` |
| From-file importer (MIG-007) | `bin/utils/migration_py/long_forecast.py:251,272` |

The established convention is therefore **one config file per hv bucket**. Months follow it
exactly: `month_1/2/3`, each with its own `operational_month_lead_time` (0/1/2) and its own
hindcast CSV, producing `MONTH hv 0..2`.

## Why quarter/season collapse to hv0

This repo ships a **single** config for each: `quarter.json` and `seasonal_april.json`, both
`operational_month_lead_time: 0`. So each writes exactly one bucket (hv0). Meanwhile each hindcast
CSV actually contains **many** issue points:

- `quarter` = a **3-month forecast issued monthly Mar-Sep** (7 windows/yr): `valid_from -> valid_to`
  = 03->05, 04->06, 05->07, 06->08, 07->09, 08->10, 09->11. All stamped hv0.
- `seasonal_april` = the **Apr-Sep (6-month) forecast, issued in April only**. Stamped hv0, which
  is correct for the April issue.

## Desired semantics (per the operational data + modeller intent)

- **Quarter**: hv should reflect the target quarter (e.g. Mar=Q1, Apr/May/Jun=Q2, Jul/Aug/Sep=Q3).
- **Season**: hv should reflect the **issue date**. The DB convention is
  `hv = target_start_month(April) - issue_month`: April=0, March=1, Feb=2, Jan=3.

Caveats discovered:
- The `quarter` hindcast is a rolling-3-month monthly product (7 windows), which does **not** map
  cleanly onto 4 calendar quarters.
- The existing operational `QUARTER hv1..4` is **internally inconsistent** (e.g. hv1 mixes issue
  months Jan + Mar-Sep; hv2 mostly April; hv3 mostly July; hv4 October) -> likely migrated from a
  config set that has since changed. Its provenance should be confirmed before trying to match it.
- The repo has no `seasonal_january/february/march` configs or CSVs, so `SEASON hv1..3` cannot be
  backfilled from here regardless of the rule.

## Options

1. **Config-per-bucket (architecturally consistent; recommended).** Produce per-bucket configs +
   hindcast CSVs upstream (in `apps/long_term_forecasting` hindcast production), each with the
   correct `operational_month_lead_time`. **No migrator code changes** -- all three writers already
   loop configs and stamp the constant. MIG-007's importer is already correct under this model.
2. **Date-derived hv in the importer.** Compute hv per row from `valid_from` (quarter) / issue date
   (season). **Diverges** from the service migrator and the operational pipeline (which keep
   stamping the constant), so hv would depend on which writer ran -- inconsistent unless all three
   change, including the **colleague-owned service**. Discouraged.
3. **Document the limitation (interim).** From-file backfill supports MONTH (multi-config) plus the
   single-bucket `QUARTER hv0` / `SEASON hv0`; other buckets come from operational reruns. This is
   the current de-facto contract.

## Recommendation

This is a **producer-side convention decision owned by the service owner (colleague) + modeller**,
not a code fix. Do **not** add date-derivation to the importer (option 2) -- it would diverge from
the canonical config-per-bucket design. Take the question in
`doc/prod/longforecast_quarter_season_hv_convention.md` to the owner/modeller; if the answer is
config-per-bucket (option 1), it becomes a hindcast-production + config task with the migrators
unchanged.

## Scope (once the convention is settled)

- Most likely `apps/long_term_forecasting` (hindcast/config production) + the `long_term_configs/`
  set. The migrators (`bin/utils/migration_py/long_forecast.py`, the service `data_migrator.py`)
  likely need **no change** under option 1. **No `sapphire/services/**` edits without coordination.**
- Sentinel station codes only (`19999`); no real codes or discharge values.

## Evidence (local, sentinel-safe aggregates)

- `quarter` dry-run: `horizon_type_enum=QUARTER horizon_value=0`, empty target map -> would create a
  fresh `QUARTER hv=0` (17 stations / 4876 rows), disjoint from operational `QUARTER hv1..4`
  (78-79 stations each). Write **held**.
- `seasonal_april` write: `SEASON hv=0`, additive -> `SEASON hv0` 62 -> 79 stations, 2940 -> 3671
  rows; `hv1..3` unchanged. Correct for the April issue.
- Quarter source CSV (`quarter/<model>/<model>_hindcast.csv`) `valid_from->valid_to` months:
  03->05, 04->06, 05->07, 06->08, 07->09, 08->10, 09->11 (7 monthly 3-month windows).
- Season source CSV (`seasonal_april/...`): single issue month 04, `valid_from->valid_to` 04->09.
