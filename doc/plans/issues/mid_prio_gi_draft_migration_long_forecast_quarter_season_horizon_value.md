# MIG-008: Quarter/season horizon_value convention (RESOLVED) - config audit + DB reconciliation

**Status**: Draft - convention resolved 2026-06-22; awaiting a planner pass to scope the changes
**Priority**: Mid
**Module**: `long_term_configs` (per-deployment) + `apps/long_term_forecasting` (hindcast/config
production) + **`apps/postprocessing_forecasts` (ensemble pipeline -- NEEDS an hv change, see below)**;
the postprocessing *service* (sapphire/services) needs no change

> **Scope expansion (2026-06-22):** an audit found the `apps/postprocessing_forecasts`
> quarterly/seasonal **ensemble** pipeline writes `long_forecasts` with `horizon_value = quarter_in_year`
> (1-4) for quarter and hardcoded `1` for season (`api_writer.py:1043-1067`, at the time of this audit),
> contradicting the config-lead convention. This **was** the source of the `QUARTER hv1-4` / `SEASON hv1`
> rows -- P-PIPE (below) has since fixed the writer, so existing `QUARTER hv1-4` rows in the DB are
> legacy, not currently being added to. **Qualify "legacy":** under flag OFF the writer emits
> `hv = quarter_horizon_value()` (the configured lead); under flag ON it passes each row's own
> `horizon_value` through (`api_writer.py:1173-1174`), and the flag-ON reader can legitimately emit
> several leads for the same quarter (`tests/test_quarterly_data_reader.py`
> `test_monthly_aggregated_two_leads_survive`). So "legacy" means the pre-P-PIPE quarter-number rows
> specifically -- it does **not** mean every row with `hv != config-lead` is legacy; a flag-ON multi-lead
> row can have `hv != config-lead` and still be current. Decision: **cover the ensemble pipeline (option a)** -- fix it to emit the config-lead hv. This is
> a hard prerequisite (phase P-PIPE) for the data cleanup, which would otherwise be regenerated. P-PIPE
> gets its own planner+reviewer pass.
>
> **P-PIPE has landed (verified 2026-09-28).** The writer now emits `horizon_value` = the configured
> lead **for quarter**: `apps/postprocessing_forecasts/src/api_writer.py:1174`
> (`horizon_value = int(row["horizon_value"])` when the flag is on and a per-row value is present) /
> `:1176` (else `horizon_value = quarter_horizon_value()`). **Season is different**: `:1192`
> (`horizon_value = int(row[period_col])`) passes through the stored row's own `horizon_value`, with a
> fallback of `1` only if that column is absent (`src/data_reader.py:3977-3981` --
> `df["season_in_year"] = lead.astype(...)` when `"horizon_value"` is present, else `df["season_in_year"]
> = 1`), rather than computing it from config the way the quarter branch does. The `:1043-1067` citation
> above is historical -- the file has grown since this audit and that range no longer holds the
> quarter/season branch.
**Depends on**: MIG-007 (importer accepts `quarter`/`season`)
**See also**: `doc/prod/longforecast_quarter_season_hv_convention.md` (question + service-owner answer);
`doc/plans/archive/longforecast_hv_convention_plan.md` (phased plan, reviewed -> NO-GO on destructive
cleanup as written); `doc/prod/longforecast_historical_data_decision_request.md` (owner/modeller
decision needed before any `long_forecasts` mutation)

## Resolved convention (2026-06-22, from the service owner)

`horizon_value = operational_month_lead_time` from the config. The existing config-per-bucket
mechanism is correct as-is: there is **no date-derivation** and **no 4-calendar-quarter mapping**.
"Quarter" is a single quarterly product whose hv is just the config lead. (This is about `horizon_value`
only; the target-window contract, since owner-superseded to calendar quarters, is a separate question --
see the DOC-009 row-3 note below, "What this corrects from the earlier draft".)

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
  - **DOC-009 row-3 note (owner decision, 2026-09-25; added 2026-09-28).** Distinguish the two
    meanings this bullet conflates: the **hv conclusion above stands** — mapping `horizon_value` to
    the quarter number was, and remains, wrong; `hv` is the config lead. But **calendar-quarter
    target windows** (`valid_from`/`valid_to` = the calendar quarter's own bounds) are now the
    product contract (owner, 2026-09-25 — see `quarter_calendar_product_plan.md`), which this
    bullet's "one product, distinguished by date/valid_from/valid_to" framing predates. Rolling-window
    rows (the 7 monthly issue windows) are not product under that contract; they are excluded at read
    and, since #527, at write too (PP-064 Chunk A) — not deleted, only excluded.
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
   - **Do NOT run this held backfill now (2026-09-28).** Since PP-064/PP-065 (owner, 2026-09-25/26),
     calendar-quarter target windows are the product contract and the tjhm QUARTER population needs a
     provenance-filtered cleanup first (PP-064 Chunk C decision F, by provenance -- native rows vs.
     postprocessing aggregates holding LR values). Only the **reviewed decision-F re-import** (PP-064
     Chunk C step 3, calendar-issue CSV rows only, 04-01/07-01) may write tjhm QUARTER rows in the
     interim. Any broader from-file backfill (this item) waits for LTF-014 P2 (the hindcast write set,
     D2) and must itself be calendar-window-filtered, or it would re-introduce the rolling-window rows
     PP-064/FD-029 now exclude at read.
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
  write currently held pending this plan. **Held means held**: do not run it now -- see item 3's
  2026-09-28 note above (PP-064 Chunk C decision F only, until LTF-014 P2).
