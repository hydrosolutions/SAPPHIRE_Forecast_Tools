# High prio — postprocessing long-term skill (month/quarter/season) pools forecast issuances across leads

**Status:** ARCHIVED / largely resolved (2026-06-29). The finding below was written
against `develop_forecast_skill_eval`, which was *behind* `origin/maxat_sapphire_2`
and did not yet contain the merged **P-PIPE** series. P-PIPE already implements
per-lead **season** skill (the high-severity case) — `long_term_horizon_resolver`,
lead-aware readers/writers, and `calculate_seasonal_skill_metrics` grouping by
`season_in_year` (= the lead), persisting distinct rows via the existing
`horizon_in_year` unique-key slot, **with no schema change**. The line-number
evidence below reflects the pre-P-PIPE develop code and is stale.

**Resolution:**
- maxat→develop merged (commit `7d0e7f47`); develop now has P-PIPE.
- Season per-lead skill: **done** (P-PIPE). Quarter: single configured lead, left
  as-is (operational decision). Min-`n_pairs` floor added — drop skill rows with
  `n_pairs < 2` (commit `0f62c1ad`), fixing the n=1 pathology in the shipped season
  path. Per-lead baselines: already per-lead (no change). Dashboard headline: shows
  the single operationally-current lead (no change needed).
- **Month** remains lead-pooled and is the one genuine remaining gap. Unlike season
  it has no free unique-key slot (`horizon_in_year` = calendar month), so it needs
  the `SkillMetric.horizon_value` schema column + service coordination. Tracked in a
  **separate new issue draft** (month skill schema change). NOT low priority.

Original finding follows (historical; line numbers pre-P-PIPE).

---

**Status (original):** draft — needs coordination with the postprocessing_forecasts /
services owner before any code change. `apps/postprocessing_forecasts/` and
`sapphire/services/` are colleague-managed (see CLAUDE.md Ownership Boundaries).
This file documents the finding and the proposed fix; it does **not** change any
colleague-owned code.

## Summary

The operational long-term skill metrics in `postprocessing_forecasts` pool
multiple forecast **issuances** (different leads / issue dates) for the same
target period into one skill number. This affects **month, quarter, and season**
— each is grouped by `[period, code, model_short]` with no lead dimension, so
distinct-lead products are averaged together.

For Kyrgyz hydromet a monthly forecast is issued at several leads (e.g. the 10th
in-month update at lead 0 and the 25th prior-month forecast at lead 1+); quarter
and season targets are likewise forecast from multiple issue dates (the local
daily pipeline runs modes month_1/month_2/month_3/quarter). Each lead is a
distinct product with materially different skill.

Surfaced while auditing `apps/forecast_skill_eval` against the canonical
postprocessing implementation. The eval app stratifies long-term skill by lead
(branch `develop_forecast_skill_eval`); postprocessing does not, and should be
aligned for parity. A follow-up read-only audit confirmed quarter and season are
affected too (this file's open question 3 — now answered: **yes**).

## Evidence (read-only)

Grouping keys, all lead-free:

| horizon | skill groupby key | includes lead? | evidence |
| --- | --- | --- | --- |
| month | `["month_in_year", "code", "model_short"]` | no | `skill_metrics.py:1194`, `:1208`; merge on code/year/month at `:1169` |
| quarter | `["quarter_in_year", "code", "model_short"]` | no | `skill_metrics.py:2045`, `:2113`, `:2155` |
| season | `["season_in_year", "code", "model_short"]` | no | `skill_metrics.py:2080`, `:2113`, `:2155` |

`horizon_value` (= lead, months ahead from issue to target start) is carried in
the merged monthly frame (`skill_metrics.py:1086`) but never in the group key.

**Where the lead is lost (quarter/season):** monthly forecasts are aggregated to
quarter/season **before** skill calc (`recalculate_skill_metrics.py:296`, `:343`).
The quarter aggregation groups by `["code","year","quarter_in_year","model_short"]`,
dropping `date`/`horizon_value` (`aggregation.py:251`); direct quarter rows are
deduped on the same lead-free key (`data_reader.py:2677`); the canonical
quarter/season output columns omit `date` and `horizon_value` (`data_reader.py:37`)
and the normalizer explicitly drops `horizon_value` (`data_reader.py:3073`). So a
late per-lead grouping is impossible — the lead must be preserved **upstream**.

**DB severity (read-only aggregates, local `long_forecasts`, 2006–2026, ensembles
excluded):**

| horizon | target×model periods | periods with >=2 issue dates | periods with >=2 leads | leads min/median/max |
| --- | ---: | ---: | ---: | --- |
| quarter | 54,232 | 6,509 | 9,376 | 1 / 1 / 2 |
| season | 2,944 | 2,324 | 2,310 | 1 / 4 / 4 |

Raw current-key pooling estimate:

| horizon | current raw keys | keys pooling >=2 leads | median raw pairs (current) | median per-lead raw pairs |
| --- | ---: | ---: | ---: | ---: |
| quarter | 2,574 | 1,575 | 21 | 18 |
| season | 180 | 146 | 93.5 | 21 |

Quarter is moderate-to-high severity; **season is high** — most current keys pool
multiple leads, often four. (Saved recalc medians: quarter `n_pairs`≈17 over 2,928
rows; season ≈3 over 955 rows.)

## Why it matters

- Published long-term skill conflates different-lead products; the headline is
  not interpretable as the skill of any single issuance.
- Biases effective sample size / base rate (target periods counted multiple times).
- Diverges from the corrected `forecast_skill_eval`, which stores `lead` from
  `horizon_value` (`forecast_skill_eval/pairs.py:340`) and emits long-term
  contingency rows per lead (`forecast_skill_eval/contingency.py:94`).

## Proposed change (to discuss with the owner)

**Apps-side (`postprocessing_forecasts`, colleague-managed — coordinate):**
1. Preserve `horizon_value` through the quarter/season readers and forecast
   aggregation (stop dropping it at `aggregation.py:251`, `data_reader.py:37/3073`).
   For monthly-derived quarter/season forecasts, aggregate by issue/lead too, not
   only target period.
2. Extend the skill group keys to include the lead:
   `[period_col, "horizon_value", "code", "model_short"]` for month/quarter/season
   (`skill_metrics.py:1194/1208`, `:2045/2080/2113/2155`).
3. Keep `date` as diagnostic/provenance; add it to the key only if same-lead
   multiple issue dates should be distinct products.

**Service/API contract (`sapphire/services/postprocessing`, colleague-owned —
required for persistence):** per-lead skill rows cannot be stored today — the
`SkillMetric` model has `date` and `horizon_in_year` but **no `horizon_value`**
(`models.py:201`, `schemas.py:161`), and the unique constraint lacks it
(`models.py:228`); `_write_skill_metrics_to_api` writes `horizon_in_year`, not the
lead (`api_writer.py:629`). Without the schema change, per-lead rows collide on
the unique key and overwrite each other on write. So persisting per-lead skill
needs: add `horizon_value` to the skill-metric model, schema, write/read API, and
unique constraint.

## Decision needed from the owner

1. Expose per-lead skill rows, or report one canonical issuance (e.g.
   smallest-lead / latest) per target period for the operational headline?
2. Approve the `SkillMetric` schema + unique-constraint change to carry
   `horizon_value` (gates the apps-side fix for all three long-term horizons).

## Cross-references

- Quarter/season audit prompt + findings:
  `doc/plans/working/forecast_skill_eval_quarter_season_issue_date_prompt.md`.
- Eval-side fix + audit:
  `doc/plans/working/forecast_skill_eval_postproc_consistency_prompt.md`.
- Coverage-threshold parity already aligned eval → postproc
  (`QUARTER_MIN_MONTHS=2`, `SEASON_MIN_COVERAGE=0.5`) in the eval commit `5c3fa045`.
