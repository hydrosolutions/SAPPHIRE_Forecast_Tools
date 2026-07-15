# MIG-008: Quarter/season horizon_value convention (RESOLVED) - config audit + DB reconciliation

**Status**: Draft - convention resolved 2026-06-22; awaiting a planner pass to scope the changes
**Priority**: Mid
**Module**: `long_term_configs` (per-deployment) + `apps/long_term_forecasting` (hindcast/config
production) + **`apps/postprocessing_forecasts` (ensemble pipeline -- NEEDS an hv change, see below)**;
the postprocessing *service* (sapphire/services) needs no change

> **Scope expansion (2026-06-22):** an audit found the `apps/postprocessing_forecasts`
> quarterly/seasonal **ensemble** pipeline writes `long_forecasts` with `horizon_value = quarter_in_year`
> (1-4) for quarter and hardcoded `1` for season (`api_writer.py:1043-1067`), contradicting the
> config-lead convention. This is the live source of the `QUARTER hv1-4` / `SEASON hv1` rows. Decision:
> **cover the ensemble pipeline (option a)** -- fix it to emit the config-lead hv. This is a hard
> prerequisite (phase P-PIPE) for the data cleanup, which would otherwise be regenerated. P-PIPE gets
> its own planner+reviewer pass.
**Depends on**: MIG-007 (importer accepts `quarter`/`season`)
**See also**: `doc/prod/longforecast_quarter_season_hv_convention.md` (question + service-owner answer);
`doc/plans/archive/longforecast_hv_convention_plan.md` (phased plan, reviewed -> NO-GO on destructive
cleanup as written); `doc/prod/longforecast_historical_data_decision_request.md` (owner/modeller
decision needed before any `long_forecasts` mutation)

## Resolved convention (2026-06-22, from the service owner)

`horizon_value = operational_month_lead_time` from the config. The existing config-per-bucket
mechanism is correct as-is: there is **no date-derivation** and **no 4-calendar-quarter mapping**.
"Quarter" is a single quarterly product whose hv is just the config lead.

- **Month**: hv = month lead. `month_0->0, month_1->1, month_2->2, month_3->3`. (Tajik filenames are
  off by one -- `month_1.json` carries lead 0 -- but the `operational_month_lead_time` value inside
  each config is authoritative, not the filename.)
- **Quarter**: single quarterly forecast per deployment. lead `= 1` for **Kyrgyz** (hv1), `= 0` for
  **Tajik** (hv0).
- **Season**: one config per issue month, hv = months before the April target start. **Kyrgyz**:
  Jan->hv3, Feb->hv2, Mar->hv1, Apr->hv0. **Tajik**: April only -> hv0.

## What this corrects from the earlier draft

- The "quarter is 7 rolling windows / should map to calendar quarters Q1..Q3" reading was **wrong**.
  Quarter is one product; the 7 monthly issue windows in the hindcast CSV all share the deployment's
  single quarter hv, distinguished by `date`/`valid_from`/`valid_to` in the natural key.
- "Tajik `QUARTER hv0` is an orphan bucket" was **wrong**: for Tajik, hv0 is the **correct** quarter
  bucket. The held Tajik quarter write should be reconsidered (likely proceed).
- The Tajik `seasonal_april -> SEASON hv0` write already applied was **correct**.
- The from-file importer (MIG-007) and the service migrator need **no hv code change** -- both
  already stamp `operational_month_lead_time`.

> **Addendum (2026-07-13):** a separate dashboard-side investigation surfaced a **third dataset —
> MONTH aggregate rows with `horizon_value = calendar month`** (prod, ~1,781 rows, 2016–2023, same
> pathology as `QUARTER hv1-4`), plus a Tajik MONTH coverage gap (empty 2024–2025) and a possibly
> unhealthy 2026-07 operational run. Summarized for the owner in
> `doc/prod/longforecast_historical_data_decision_request.md` (ADDENDUM). Fold "month" into the
> reconciliation scope below alongside quarter/season. P-PIPE for month/quarter appears already landed
> on `maxat_sapphire_2`.

## What still needs adapting (for the planner to investigate + scope)

1. **Config audit, per deployment.** Confirm every needed config exists with the correct
   `operational_month_lead_time`:
   - Kyrgyz: `month_0..3` (leads 0..3), `quarter` (lead 1), and **all four** seasonal issues
     `seasonal_january/february/march/april` (leads 3/2/1/0). Check whether the Jan/Feb/Mar
     seasonal configs **and their hindcast CSVs** exist; if not, that is a hindcast-production gap.
   - Tajik: `month_1..3` (lead values, not filenames), `quarter` (lead 0), `seasonal_april` (lead 0).
2. **Existing DB reconciliation.** Investigate the provenance of the local DB's `QUARTER hv1..4` and
   `SEASON hv1..3` rows (78-79 / 62-73 stations) vs the convention. Determine which deployment they
   belong to (the local stack has carried both Tajik and Kyrgyz), whether any are mis-migrated under
   an old convention, and whether cleanup / re-migration is required.
3. **Held Tajik quarter write.** Re-evaluate: under the convention Tajik quarter -> `QUARTER hv0`, so
   plan whether to proceed with the from-file quarter backfill to hv0 (and how it interacts with any
   existing rows).
4. **Importer verification (no code change expected).** Confirm via a dry-run / test that quarter and
   season configs produce the intended hv; add a regression test if useful.
5. **Server parity.** Ensure the convention and any config additions / data cleanup propagate to the
   deployment server DBs, not just local.

## Out of scope

- No `sapphire/services/**` edits (service is already hv-agnostic and correct).
- No date-derived hv in the importer (explicitly rejected by the resolution).

## Evidence (local, sentinel-safe aggregates)

- All three writers stamp `horizon_value = operational_month_lead_time`: service migrator
  `data_migrator.py:669,769`; operational `run_forecast.py:269,409` (`config_forecast.py:231`);
  from-file `long_forecast.py:251,272`. None derives hv from a date.
- Tajik configs present: `month_1/2/3`, `quarter` (lead 0), `seasonal_april` (lead 0). No Tajik
  Jan/Feb/Mar seasonal configs (expected -- Tajik season is April-only).
- `seasonal_april` write: `SEASON hv0` 62 -> 79 stations, additive (correct for the April issue).
- `quarter` dry-run: would write `QUARTER hv0` (17 stations / 4876 rows) -- correct bucket for Tajik;
  write currently held pending this plan.
