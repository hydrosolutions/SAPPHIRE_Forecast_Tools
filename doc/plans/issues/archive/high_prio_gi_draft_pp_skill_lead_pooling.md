# High prio — postprocessing monthly skill pools forecast issuances across leads

**Status:** draft — needs coordination with the postprocessing_forecasts /
services owner before any code change. `apps/postprocessing_forecasts/` and
`sapphire/services/` are colleague-managed (see CLAUDE.md Ownership Boundaries).
This file documents the finding and the proposed fix; it does **not** change any
colleague-owned code.

## Summary

The operational monthly skill metric in `postprocessing_forecasts` pools
multiple forecast **issuances** for the same target month into one skill number.
For Kyrgyz hydromet a monthly forecast is issued **twice** per target month — on
the 25th of the previous month (lead 1) and again as an in-month update on the
10th of the target month (lead 0) — and these two distinct products are scored
together. Tajik hydromet issues once per target month and is unaffected.

This was surfaced while auditing the new `apps/forecast_skill_eval` app against
the canonical postprocessing implementation. The eval app has been fixed to
stratify long-term skill by lead (commit on branch
`develop_forecast_skill_eval`); the same pooling remains in the postprocessing
skill path and should be addressed for parity.

## Evidence (read-only)

- `apps/postprocessing_forecasts/src/skill_metrics.py:1169` — monthly forecasts
  are merged to observations on `code` / `year` / `month` only. Two issuances for
  one target month both survive the merge as separate rows.
- `apps/postprocessing_forecasts/src/skill_metrics.py:1192` and `:1208` — point
  metrics are then grouped by `["month_in_year", "code", "model_short"]`. `lead`
  / `horizon_value` is **not** in the grouping key, even though `horizon_value`
  is carried in the merged frame (`:1086`).
- Net effect: for a (month, station, model) with two issuances, both
  contingency outcomes land in the same group and are averaged into a single
  HSS/PSS — mixing a ~5-day-lead update with a ~5-week-lead prior-month forecast,
  which have materially different skill.

`horizon_value` currently means lead time (months ahead from issue to target
period start) — see `apps/long_term_forecasting/readme.md` and the writer in
`apps/long_term_forecasting/lt_utils.py` / `run_forecast.py`. So it is the
correct field to stratify on.

## Why it matters

- The published operational monthly skill conflates two forecast products; the
  headline number is not interpretable as the skill of either issuance.
- It biases the base rate and effective sample size (kyg months counted twice).
- It diverges from the corrected `forecast_skill_eval` behavior, so the two
  skill products will disagree until postprocessing is aligned.

## Proposed change (to discuss with the owner)

Add `horizon_value` (lead) to the monthly skill grouping key so each issuance is
scored as its own product:

- `skill_metrics.py:1192` / `:1208` — group by
  `["month_in_year", "code", "model_short", "horizon_value"]`.
- Decide the downstream contract: does the postprocessing API skill-metric
  schema carry a lead/horizon_value dimension, or is one canonical issuance
  (e.g. the latest / smallest-lead) selected for the operational headline? This
  is an API-contract question and must be agreed before implementation.
- Mirror the same treatment for quarter/season if they share the pooling path.

## Open questions for the owner

1. Should the operational monthly skill expose per-lead rows, or report a single
   canonical issuance (latest available) per target month?
2. Does the skill-metric DB schema / API response need a lead dimension to carry
   this, or is it resolved before write?
3. Are quarter/season skill computations affected by the same pooling?

## Cross-references

- Eval-side fix + audit: `doc/plans/working/forecast_skill_eval_postproc_consistency_prompt.md`
  and the audit verdict table (kyg two-issuance / taj one-issuance cadence).
- Coverage-threshold parity already aligned eval → postproc
  (`QUARTER_MIN_MONTHS=2`, `SEASON_MIN_COVERAGE=0.5`) in the same eval commit.
