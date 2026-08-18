## Quarter skill metrics land at `horizon_value=0` for models whose quarter forecasts start at lead 1 (PP-056)

**Status**: Draft (2026-08-14)
**Module**: `apps/postprocessing_forecasts` (quarter skill path — `recalculate_skill_metrics.py`,
`src/aggregation.py`, `src/skill_metrics.py`)
**Priority**: **High** — manifests only with `SAPPHIRE_SKILL_LEAD_AWARE=true`, which is
being adopted as the new deployment default. Under the flag the operational quarter
reader selects the **configured** quarter lead, so the affected models' skill is
unreachable exactly when it is needed.
**Labels**: `postprocessing_forecasts`, `skill-metrics`, `long-term`, `lead-aware`
**Found**: 2026-08-14, local kghm review on `maxat_sapphire_2` @ `8e3fc1bc`, **after** a
clean full-history recalc (`SAPPHIRE_PREDICTION_MODE=ALL`,
`SAPPHIRE_RECALC_START_YEAR=2000`, flag ON, exit 0, 10m 5s, zero errors).
**Related**: PP-042 (ensemble-exclusion form mismatch, quarter/season exposed) — different
defect, same horizon. PP-049 (quarter/month recalc non-determinism) — if that reproduces,
re-confirm the counts below before acting.

> **Provenance correction (2026-08-16).** The checkout moved from `maxat_sapphire_2` to
> `fix_lr010_lr011_write_contract` at **2026-08-14 16:00** (git reflog), so every run from the
> full-history recalc onward executed on that branch (now `849c8736`), **not** on trunk as the
> line above states. That branch's diff vs trunk touches only
> `apps/linear_regression/linear_regression.py`, `apps/iEasyHydroForecast/forecast_library.py`,
> their tests, and docs — **none of the files this issue concerns** — so the finding holds
> identically on trunk. Recorded for accuracy of the audit trail, not because the conclusion changes.

---

## Observation

For the kghm quarter horizon, **which models have skill rows is inconsistent with which
models have forecast rows, and the two disagree on `horizon_value`.**

Quarter **forecasts** (`/api/postprocessing/long-forecast/?horizon_type=quarter`), one
station, by model → lead:

| Model group | Leads present |
|---|---|
| LR_Base, LR_SM, EM, Naive Mean, Skilled Mean | `hv=0,1,2,3,4` |
| **GBT, LR_SM_DT, LR_SM_ROF, MC_ALD, SM_GBT, SM_GBT_LR, SM_GBT_Norm** | **`hv=1,2,3,4` — no `hv=0` at all** |

Quarter **skill metrics** (`/api/postprocessing/skill-metric/?horizon=quarter`), same
station pair, by model → lead:

| Model group | Leads present |
|---|---|
| LR_Base, LR_SM, EM, Naive Mean, Skilled Mean | `hv=0,1,2,3` |
| **the same seven models** | **`hv=0` ONLY** |

So for those seven models the skill row sits at the one lead where they have **no
forecasts**, and is missing at every lead where they **do**.

`hv=0` skill rows carry `horizon_in_year=[1,2,3,4]` and dates spanning
2026-01-01..2026-10-01, i.e. they are not a single stale artefact — they are the model's
entire quarter skill population.

## Why this matters now

kghm's only `quarter` config declares `operational_month_lead_time = 1`. With
`SAPPHIRE_SKILL_LEAD_AWARE=true` the operational readers and the ensemble/Skilled-Mean
pool select the **configured** lead. At `hv=1` only five of twelve models have skill, so
the seven above are silently outside the pool despite having forecasts there. Flag OFF the
collapse hides this, which is why it has not been seen before.

## Not the same as the neighbouring horizons

- **Season is clean** — only five models produce season forecasts at all, and their skill
  covers the same models and the same leads. No mismatch.
- **Month is clean** — every model spans `hv=0..3`, matching `month_0..month_3` leads 0..3.

The defect is **quarter-specific**, which is the useful narrowing: whatever assigns
`horizon_value` on the quarter skill path differs from the month/season paths.


## Cross-organisation evidence 2026-08-17 (tjhm) — `horizon_value` is NOT config-derived

The kghm data alone could not distinguish *"config-derived but computed wrong"* from
*"never derived from config at all"*. tjhm settles it, because **the two deployments configure
a different quarter lead**:

| | kghm | tjhm |
|---|---|---|
| `quarter` configured lead | **1** | **0** |
| GBT, LR_SM_DT, LR_SM_ROF, MC_ALD, SM_GBT, SM_GBT_LR, SM_GBT_Norm | `hv=0` only | `hv=0` only |
| LR_Base, LR_SM, EM, Naive Mean, Skilled Mean | span `hv=0..3` | span `hv=0..2` |

**The same seven models write `horizon_value=0` on both deployments despite different
configured leads.** On kghm that lands on the wrong lead and is visibly broken; on tjhm it
coincidentally equals the configured lead and looks correct. So the value is **defaulted, not
resolved from configuration**.

This also inverts which population looks anomalous per org — on tjhm (`quarter` lead 0) it is
the *five* models spanning `hv=0,1,2` that deviate from a single configured lead. Any fix must
therefore state which of the two populations is intended **before** changing either.

## A full-history recalc with the flag ON does not change it

tjhm, 2026-08-17, `SAPPHIRE_SKILL_LEAD_AWARE=true` (freshly enabled),
`SAPPHIRE_PREDICTION_MODE=ALL`, `SAPPHIRE_RECALC_START_YEAR=2021`, exit 0, 1m 1s, zero errors.
Records written: month 5360, quarter 382, season 66, pentad 5544, decad 2772, day 45.

**Every model's `horizon_value` distribution was byte-identical before and after** — month,
quarter and season alike. The recalc rewrote metric *values* and changed no lead stratification
whatsoever.

Combined with the identical outcome on kghm (2026-08-14, full history from 2000), this rules
out "stale rows awaiting migration" as an explanation on **two** deployments. The write path
simply does not assign quarter `horizon_value` from the mode's configured lead for those seven
models.

**Narrowed target for whoever picks this up:** find where quarter skill rows obtain
`horizon_value`, and why that path is reached only by the LR-family and the ensemble aggregates.
The seven affected models are exactly the GBT/MC/SM families.

## Side observation — enabling the flag on tjhm restratified nothing

Worth recording so it is not mistaken for a failed migration: tjhm month rows already spanned
`hv=0,1,2` **before** the flag was enabled, so the § 0.5d "half-migrated mix" risk did not
materialise here. Enabling `SAPPHIRE_SKILL_LEAD_AWARE` on tjhm produced **no visible change in
skill-metric lead distribution**. The flag governs per-lead ensemble/skill computation, reader
selection and dashboard lead resolution — not the mere presence of multiple `horizon_value`s in
`skill_metrics`.


## What to inspect

1. `src/aggregation.py` quarter synthesis (`# Synthesize valid_from/valid_to from quarter
   boundaries`, ~`:273`) — does the quarter aggregate carry the source forecast's
   `horizon_value`, or default it?
2. `recalculate_skill_metrics.py` quarter branch (~`:421`, `quarterly_skill = pd.concat(...)`)
   — is `horizon_value` grouped on, or re-derived after the concat?
3. Whether the seven affected models share a code path the other five do not. They are
   exactly the GBT/MC/SM families; LR_Base/LR_SM/EM/Naive Mean/Skilled Mean are the ones
   that behave.
4. `doc/prod/longforecast_quarter_season_hv_convention.md` — confirm the intended quarter
   `horizon_value` convention before assuming `hv=0` is wrong rather than under-specified.

## Open question for the owner

Existing guidance says quarter **publishes the lowest lead** (smallest-lead dedup is
correct for *forecasts*). If that convention is also meant to apply to quarter **skill**,
then `hv=0` may be intentional for models that have no lead-0 forecast — but then the
five well-behaved models writing `hv=0..3` is the deviation, and the operational reader
selecting the configured lead is reading the wrong rows for everyone. **One of the two
populations is wrong; decide which before any code change.**

## Acceptance criteria

- Quarter skill `horizon_value` is derived from one rule, applied to all models, stated in
  the issue and matching the documented convention.
- For every (model, lead) with quarter **forecasts** and enough pairs to clear the min-n
  floor, a quarter **skill** row exists at that same lead.
- No quarter skill row exists at a lead where the model has no forecasts.
- Regression test with a synthetic station (`19999`) carrying a model with forecasts at
  leads 1..4 only, asserting its skill rows land at 1..4 and not at 0.
- Re-run of the full-history recalc reproduces the corrected distribution.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` green.

## Contract not to break

- Do not "fix" this by dropping the `hv=0` rows — if the lowest-lead convention is the
  intended one, deleting them destroys the only quarter skill those models have.
- Month and season distributions are currently correct; any shared-helper change must
  leave them byte-identical.
- Flag OFF must stay a true no-op.
