# PP skill: ensemble-exclusion form inconsistency (monthly latent; quarter/season EXPOSED)

**Priority:** mid.
**Module:** `apps/postprocessing_forecasts` (fair game).
**Found:** 2026-07-07, during LT Skilled-Mean NSE>0 validation (branch
`feature_lt_skilled_mean_nse_threshold`). Line numbers from that branch's tree.

> **Correction 2026-07-14 (out-of-loop review) — "latent" is only true for MONTHLY.**
> The original framing ("latent — not currently manifesting") is right for the monthly path and
> **wrong for quarter/season**:
> - **Monthly is protected.** There is an upstream *canonical* exclusion that catches aggregate
>   names in BOTH display-form and DB-form (`skill_metrics.py:1356`, helper
>   `apps/postprocessing_forecasts/src/model_names.py:7`). For monthly this is defensive cleanup only.
> - **Quarter/season are NOT.** They use the display-form `_AGGREGATED_BASELINES`
>   (`skill_metrics.py:1235`) and display-form exclusions downstream (`:2477`, `:2674`, `:2777`), so
>   DB-form `ENSEMBLE_MEAN` / `NAIVE_MEAN` / `SKILLED_MEAN` values **pass straight through the
>   filter** if they reach the function — the prior run's aggregates re-enter the aggregate pool and
>   an ensemble gets computed over ensembles.
>
> **Fix accordingly:** apply the canonical exclusion at the top of
> `_calculate_aggregated_skill_metrics` and use the canonical helper in the downstream filters —
> do not just harden monthly. **Tests:** existing coverage only injects *display-form* stale rows
> (`tests/test_quarterly_skill_metrics.py:380`); add **DB-form** injected rows for quarter AND
> season, which is the case that currently slips through.

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

## Operational evidence — structurally exposed, not currently manifesting

Reconcile with the correction banner above: **monthly is latent (protected); quarter/season are
structurally exposed** (their filters are display-form only, `:2477/:2674/:2777`, so a DB-form
`ENSEMBLE_MEAN`/`NAIVE_MEAN`/`SKILLED_MEAN` is NOT excluded there). "Exposed" is a **code-path**
statement — it does not assert a leak is happening now.

Empirically, no leak currently manifests:
- **0** aggregate rows (EM/NM/SM) list an ensemble name as a member — checked across
  `MONTH`, `QUARTER`, `SEASON` (`(', '||composition||', ') LIKE '%, ENSEMBLE_MEAN, %'`
  etc., all forms) on the local `postprocessing_db` after a clean recalc.
- Monthly is provably protected by the `:1279-1283` canonical filter; quarter/season happen not to
  receive DB-form aggregate rows at the vulnerable filter in the local data, so nothing leaks
  **today** — but that is an upstream-conditions accident, not a guarantee from the code.

So the impact is **latent for monthly and a live structural exposure for quarter/season that is not
currently triggering**. Priority stays **mid**: no observed corruption, but the quarter/season fix
is a real correctness hardening (not purely cosmetic), and the risk is a **silent regression** if
upstream conditions change or a new horizon path is added.

## Proposed fix (quarter/season = real hardening; monthly = defensive cleanup)

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
