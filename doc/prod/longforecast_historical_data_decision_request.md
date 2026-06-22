# Decision request: what to do with pre-convention historical long_forecast rows

**For**: postprocessing-service owner + long-term modeller
**From**: forecast-tools side (MIG-008, after the 2026-06-22 hv-convention resolution + a plan review)
**Decision needed before**: any cleanup of `long_forecasts`, and before the Tajik `QUARTER hv0`
backfill (both touch your service DB).
**TL;DR**: The local `long_forecasts` table holds two blocks of **real, non-reproducible** historical
forecasts that do **not** fit the resolved convention. A plan review proved they are **not**
duplicates and **cannot** be regenerated from current configs, so hard-deleting them (as an earlier
draft proposed) would be permanent data loss. We are holding all cleanup until you decide, per
dataset, between **keep / quarantine / re-stamp / delete**.

## Background (settled)

- Convention resolved: `horizon_value = operational_month_lead_time` (config-per-bucket; no
  date-derivation, no calendar-quarter mapping). See
  `doc/prod/longforecast_quarter_season_hv_convention.md`.
- The importer and your service migrator are **correct** and need no change -- both already stamp the
  config lead. This is purely a **data-governance** question about existing rows, not a code issue.

## Dataset A -- `SEASON hv1` April block (orphan April-1 series)

| | Detail |
|---|---|
| Size | ~2,890 rows, ~61 sites, 2000-2026, 2 models (`LR_BASE`/`LR_SM`) |
| What it is | Kyrgyz seasonal forecasts **issued on April 1** under an older `operational_issue_day` |
| Why it's here | Stamped `hv1`; under the convention the April issue is `hv0` |
| Critical finding | **Zero overlap with `SEASON hv0`** on the natural-content key `(code, date, model_type, valid_from, valid_to)` -- and even on `(code, date, model)`. `hv0`'s Kyrgyz April rows are issued **April 25** (current `operational_issue_day=25`); the only April-1 rows in `hv0` are the small Tajik set. So this is a **distinct historical series with no counterpart in hv0**. |
| Reproducible? | **No** -- current configs produce April-25 rows only |

The earlier plan deleted this block "because hv0 already has it." That premise is **false** (overlap
= 0). Deleting it is data loss, not de-duplication.

## Dataset B -- `QUARTER hv1`(January) + `hv2/hv3/hv4` (old calendar-quarter, 12-model ensemble)

| hv | Issue months | Rows / sites / models |
|---|---|---|
| hv1 (stale part) | January | subset of 32,486 |
| hv2 | Apr / May / Jun | 13,849 / 78 / 12 |
| hv3 | Jul / Aug / Sep | 13,675 / 78 / 12 |
| hv4 | October | 13,529 / 78 / 12 |

- This is a coherent **old 4-calendar-quarter scheme** (hv1=Q1 Jan, hv2=Q2, hv3=Q3, hv4=Q4) -- the
  exact mapping the convention rejected -- carrying a **richer 12-model ensemble** (e.g. GBT, SM_GBT*,
  MC_ALD, NAIVE_MEAN, ENSEMBLE_MEAN, SKILLED_MEAN, ...).
- The **current 2-model config (`LR_Base`/`LR_SM`) will never regenerate** these ~55k rows.
- This is genuine operational history from a model set you no longer run -- "stale/unexplained" is
  inaccurate; it is explainable (old convention) but **irreversible if deleted**.

### What is correct and must be kept regardless

- `QUARTER hv1`, the **Mar-Sep `LR_BASE`/`LR_SM`** rows = the current Kyrgyz quarterly product (`hv1`).
- `SEASON hv0` (Tajik + Kyrgyz April-25), `SEASON hv2` (Feb), `SEASON hv3` (Jan) = correct Kyrgyz
  seasonal buckets.
- `QUARTER hv0` does not exist yet; it is the correct home for the Tajik quarterly backfill.

## The decision (per dataset: A and B)

Please choose for each:

1. **Keep as-is** -- leave the historical rows; consumers ignore non-current buckets.
2. **Quarantine** -- move to a side table / add a tag so they are excluded from current consumers but
   not destroyed.
3. **Re-stamp / re-migrate** -- if they are genuinely superseded, map them to the correct
   `(horizon_value, date)` rather than dropping them.
4. **Delete** -- only with your explicit sign-off; we will first capture a **per-row scoped dump** so
   any deletion is individually reversible.

## What we are doing meanwhile

- **Holding** all `long_forecasts` cleanup and the Tajik `QUARTER hv0` backfill until you decide.
- Landing a code-only **importer regression test** that locks the convention (no DB writes).
- All investigation used **aggregate-only** reads; no station codes or discharge values were recorded.

Please reply with a choice for Dataset A and Dataset B (and, for B, whether the 12-model ensemble
should be preserved). We will then revise the execution plan accordingly.

---

## DECISION (2026-06-22, service owner / modeller)

- **Dataset A**: **re-stamp/re-migrate** -> move the Kyrgyz April-1 seasonal series to the correct
  April bucket `SEASON hv0`.
- **Dataset B**: **delete the deprecated models**; **re-stamp the currently-configured models**
  (`LR_BASE`/`LR_SM`) into the correct quarter bucket `QUARTER hv1`. The 10-model ensemble (GBT,
  SM_GBT*, MC_ALD, NAIVE_MEAN, ENSEMBLE_MEAN, SKILLED_MEAN, LR_SM_DT, LR_SM_ROF) is **not** preserved.

Collision verification (aggregate-only, before any write):

- A re-stamp `SEASON hv1`-April -> `hv0`: 2,890 rows, **0** unique-key collisions with `hv0`.
- B re-stamp `QUARTER hv2/3/4` `LR_BASE`/`LR_SM` -> `hv1`: 10,665 rows, **0** collisions with `hv1`.
- B delete deprecated models: ~41,225 rows; the `hv1` deprecated-model rows are **100% January**, so
  the current Mar-Sep rolling product is untouched.

Exact operations are in `doc/plans/working/longforecast_hv_convention_plan.md` (P3). Execution pending
final SQL review + per-step scoped backups.
