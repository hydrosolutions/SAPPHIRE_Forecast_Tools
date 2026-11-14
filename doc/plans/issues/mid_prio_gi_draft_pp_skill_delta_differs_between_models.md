# PP-074 — Short-term `skill_stats.delta` differs between models for the same station and period

**Status:** In Progress via PP-075 chunk A (owner accepted 2026-11-13). **Module:** `apps/postprocessing_forecasts`.
Cause identified below; implementation and verification are tracked in
[PP-075](mid_prio_gi_draft_pp_short_term_em_skill_and_delta_ranges.md).

## Expected contract (owner, 2026-11-12)

`delta` (= 0.674·σ of the observations, the hydromet tolerance) should depend only on **station, horizon type and
horizon of the year**. It must be identical for every model with the same (code, horizon type, period, date). A
difference between models is a bug.

## Observation (dev/kghm API via `read_skill_metrics`, read-only audit, 2026)

- Stored `delta` values can differ between models for the same station, period and
  date, and between different dated skill rows. This contradicts the shared-delta contract.
- Apparent LR-first ordering in an inspected response is not a reliable selection rule:
  stored order is not guaranteed to be date-sorted.
- Latent edge cases in PP-072's LR delta pick (`src/ensemble_calculator.py` ~:213-232), both latent because the API
  returns date-only values: (a) if the skill `date` column exists but is all NaT, `drop_duplicates(keep='last')` picks
  the LAST LR row, whereas with no `date` column the FIRST is used; (b) `pd.to_datetime(errors='coerce')` without
  `format`/`utc` silently coerces mixed-format or tz-mixed dates to NaT, so an older delta can win (if ever needed,
  use `utc=True, format='ISO8601'`).
- The skill gate (`drop_duplicates` without `date`) and the delta pick can use different dated rows.

## Why it matters

- PP-072 EM builds LR q25/q75 as point ∓ delta. Until this is fixed, PP-072 takes the delta from the **LR skill
  row only** (latest date when several exist) as a quick fix. The other models' deltas are not used by EM.
- The code comment in `create_ensemble_forecasts` and the PP-072 plan claimed "delta is constant per (code, period),
  shared by all models" — the live data contradicts this.

## Cause found by code reading (2026-11-13, verified in code)

Delta is not recomputed per model. `observed["delta"]` is the per-date LR delta merged by (date, code)
(`data_reader.py:~3084-3100`); `forecast_accuracy_hydromet` takes the FIRST non-NaN value in each model's own row
order (`skill_metrics.py:~484-495`) and logs "Delta values vary within group". Models with different date coverage
get different values. Fix is chunk A of PP-075.

## Accepted minimal fix (PP-075 A)

Select the latest applicable finite, non-negative LR/observation delta once per station, horizon and period,
independently of each model's forecast coverage, and broadcast it to the reported skill rows. Keep historical
accuracy calculated against each matched historical date's delta; do not recompute the tolerance from a model's
own pairs. The existing variation warning can still occur during scoring: output normalization alone does not
silence it.

Tests must show equal reported deltas for models with different coverage, deterministic selection under shuffled
input order, and unchanged per-date accuracy. Keep PP-072's LR-only delta lookup; simplifying it and the latent
date-format cases above is not required for this fix.
