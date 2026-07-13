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

Exact operations are in `doc/plans/archive/longforecast_hv_convention_plan.md` (P3). Execution pending
final SQL review + per-step scoped backups.

---

## BLOCKER found during execution review (2026-06-22) -> scope expanded

The `QUARTER hv1-4` and `SEASON hv1` rows are **not** deprecated history: they are the live output of
the `apps/postprocessing_forecasts` quarterly/seasonal **ensemble** pipeline, which writes
`horizon_value = quarter_in_year` (1-4) for quarter and hardcoded `1` for season
(`api_writer.py:1043-1067`, invoked operationally via `file_writer.py:686/718`). Cleaning them without
changing that pipeline would be undone by the next operational run.

**Decision: cover the postprocessing ensemble pipeline (option a).** Fix the ensemble writers to emit
the config-lead `horizon_value` (quarter: kyg 1 / taj 0; season per issue: kyg 3/2/1/0, taj 0). This
becomes phase **P-PIPE**, a hard prerequisite for the data cleanup (P3/P4). All `long_forecasts`
mutation remains held until P-PIPE ships. P-PIPE gets its own planner + reviewer pass.

---

## ADDENDUM (2026-07-13, forecast-tools side) — additional findings from the dashboard investigation

Surfaced while diagnosing a separate Tajik dashboard symptom ("wrong monthly target month", branch
`develop_ltf_monthly_horizon_value`). These are **prod-observed once, aggregate-only, and need prod
re-verification** — the local dev DB shows a different month pattern. They add to the datasets above,
they do not change the resolved convention. **P-PIPE appears to have LANDED on `maxat_sapphire_2`**
(monthly writer `api_writer.py:896` now falls back to a `0` sentinel, quarter writer `:1093` stamps
`quarter_horizon_value()`), so no further ensemble-writer change is needed for those two paths.

### Dataset C (NEW) — MONTH aggregate rows carrying the calendar month as `horizon_value` (prod)
The datasets above cover quarter and season; **month has the same pathology and is not yet listed.**
On the Tajik prod `long_forecasts`, the `ENSEMBLE_MEAN` / `SKILLED_MEAN` / `NAIVE_MEAN` month rows
were written with `horizon_value = calendar month (1..12)` instead of the config lead — **1,781 rows**,
all issued 2016–2023 (they stop where the pre-fix writer stopped). This is the month analogue of the
`QUARTER hv1-4` block and warrants the **same keep / quarantine / re-stamp / delete decision**. The
current (fixed) writer emits `0`, so this is bounded historical data, not an ongoing leak. A naive
"re-stamp `horizon_value := derived lead`" is **unsafe for these** the same way it is for quarter (see
below) — re-derive from the target period, not from a `date`-based lead.

### Tajik MONTH coverage gap (relevant to any month backfill decision)
- The Tajik prod month series runs 2016–2023, then **2024 and 2025 are entirely empty** (a ~32-month
  gap), then a single operational issue date **2026-07-01**. If continuous monthly skill/history is
  wanted for Tajik, that gap needs a backfill decision.
- The 2026-07-01 operational run wrote base-model leads 0/1/2 but **no `ENSEMBLE_MEAN` / `SKILLED_MEAN`
  / `NAIVE_MEAN` aggregates at all**, and `MC_ALD` only at lead 0 (no quantile bands for leads 1–2).
  Worth checking whether the operational aggregation step and `MC_ALD` multi-lead path are healthy on
  the server.

### Note on re-stamping vs the collision-safe approach above
A blanket "`horizon_value := lead derived from the row" collides heavily — on local `QUARTER`, 15,935
of 97,462 rows collapse onto the same unique key, because the quarter writer sets `date = valid_from`,
so a `date`-based lead is 0 for every row and the true issue lead is unrecoverable from the data. This
**confirms** the per-dataset, per-model re-stamp approach already chosen above (which reported 0
collisions) is the right one; a naive derive-and-flip is not viable. Any month re-stamp (Dataset C)
must likewise target a specific model/issue subset with a verified 0-collision scope, not a formula.

All figures aggregate-only; no station codes or discharge values recorded. Prod numbers observed once
and should be re-run on the server before acting.
