# PP skill: ensemble-exclusion form inconsistency (latent, defensive fix)

**Priority:** mid (latent — not currently manifesting; see "Operational evidence").
**Module:** `apps/postprocessing_forecasts` (fair game).
**Found:** 2026-07-07, during LT Skilled-Mean NSE>0 validation (branch
`feature_lt_skilled_mean_nse_threshold`). Line numbers from that branch's tree.

## Summary

When `recalculate_skill_metrics` rebuilds the aggregate rows (EM / Naive Mean /
Skilled Mean), it re-reads forecasts from the DB via `read_monthly_forecasts` etc.
Those forecasts include the *previously written* aggregate rows, stored in **DB-form**
(`ENSEMBLE_MEAN`, `NAIVE_MEAN`, `SKILLED_MEAN` — confirmed in `long_forecasts.model_type`).
`_normalize_monthly_forecasts` (`data_reader.py:~1046`) only renames `model_type`→
`model_short`; it does **not** map to display-form.

The aggregate-pool builders then try to exclude the prior aggregates using a
**display-form** set:

```python
excluded = {"EM", "Naive Mean", "Skilled Mean"}
pool = merged[~merged["model_short"].isin(excluded)].copy()
```

DB-form values (`ENSEMBLE_MEAN`, …) do **not** match this display-form set, so on its
own this filter would let a prior run's aggregates re-enter the member pool — a
history-dependent (non-idempotent) recalc.

### Sites using the fragile display-form exclusion
- `skill_metrics.py:1370` — `skilled_merged["model_short"].isin(["EM","Naive Mean","Skilled Mean"])`
- `skill_metrics.py:1507` — monthly EM/aggregate pool
- `skill_metrics.py:1645` — monthly Skilled-Mean pool
- `skill_metrics.py:2489` — quarter/season EM/aggregate pool
- `skill_metrics.py:2604` — quarter/season Skilled-Mean pool
- (`_AGGREGATED_BASELINES = ("EM","Naive Mean","Skilled Mean")` at `:1157`)

### The correct pattern already exists
`calculate_monthly_skill_metrics` filters `forecasts` at the top (`:1279-1283`) with a
canonical, DB-form-aware exclusion:

```python
forecasts = forecasts[
    ~canonical_model_short_series(forecasts["model_short"]).isin(AGGREGATED_ENSEMBLE_MODELS)
].copy()
```

`AGGREGATED_ENSEMBLE_MODELS = {"ENSEMBLE_MEAN","NAIVE_MEAN","SKILLED_MEAN"}` and
`canonical_model_short_series` upper-cases + underscores + alias-maps, so it catches
**both** DB-form and display-form. This upstream filter is what actually protects the
**monthly** path today — the display-form exclusions downstream are redundant.

## Operational evidence (why this is latent, not active)

Verified on the local `postprocessing_db` after a clean recalc from the fixed branch:
- **0** aggregate rows (EM/NM/SM) list an ensemble name as a member — checked across
  `MONTH`, `QUARTER`, `SEASON` (`(', '||composition||', ') LIKE '%, ENSEMBLE_MEAN, %'`
  etc., all forms). No leak manifests anywhere.
- Monthly path is provably protected by the `:1279-1283` canonical filter.

So there is **no current correctness impact**. The risk is a **silent regression**:
if the upstream canonical filter at `:1279-1283` is ever moved/refactored, or a new
horizon path is added without it, the downstream display-form exclusions will fail
silently and reintroduce a non-idempotent feedback loop.

## Proposed fix (purely defensive; no behavior change today)

1. Replace the four display-form `excluded = {"EM","Naive Mean","Skilled Mean"}`
   filters (`:1370`, `:1507`, `:1645`, `:2489`, `:2604`) with the canonical form:
   `~canonical_model_short_series(df["model_short"]).isin(AGGREGATED_ENSEMBLE_MODELS)`.
2. Confirm the **quarter/season** aggregation has an equivalent upstream canonical
   filter to `:1279-1283`; if absent, add one (that path relies solely on the
   fragile downstream exclusion — the highest-value part of this issue to verify).
3. Add a regression test: feed a forecasts frame that includes DB-form
   `ENSEMBLE_MEAN`/`NAIVE_MEAN`/`SKILLED_MEAN` rows and assert none appear in any
   rebuilt aggregate composition (placeholder code `19999`).

## Acceptance criteria
- All aggregate compositions exclude ensemble names regardless of stored form,
  proven by a test that injects DB-form aggregate rows.
- No change to existing aggregate compositions/skill for clean inputs (byte-identical).
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` — 0 fail / 0 unexpected skip.

## Notes
Do **not** conflate with `feature_lt_skilled_mean_nse_threshold` (LT Skilled-Mean
NSE>0 relaxation) — that change is validated and unrelated. This issue is a separate,
lower-priority hardening of the ensemble-exclusion logic.
