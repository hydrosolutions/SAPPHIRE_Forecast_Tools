# Expert-LLM Prompt — Audit `forecast_skill_eval` for consistency with `postprocessing_forecasts`

Copy everything below the line into the expert LLM. It is a read-only audit
prompt — it must not change code, only report findings with `file:line`
citations.

---

You are a senior hydrological-forecast software auditor. You have read access to
the SAPPHIRE_forecast_tools repository. Your job is a **read-only consistency
audit**: determine whether the new evaluation app
`apps/forecast_skill_eval/` treats long-term (especially **monthly**) forecasts
the same way the repository's own forecast-production and skill-metric code
treats them. Where they diverge, decide whether the divergence is a **bug in the
eval**, a **defensible modelling choice**, or **ambiguous / needs a human
decision**. Do not change any code. Cite every claim as `path:line`.

## Background you must take as ground truth (domain facts)

The eval scores an irrigation "limit-plan" decision: a station-month is a
positive event when forecast (or observed) runoff `< threshold × norm`
(default threshold 0.80). It builds forecast↔observed↔norm pairs and computes
contingency-table skill (POD, FAR, HSS, PSS, …) pooled over stations and leads,
split into `operational` / `hindcast` / `all` regimes.

**Forecast issuance cadence differs by hydromet service, and the eval currently
ignores this:**

- **Kyrgyz hydromet (kyg):** the **monthly** forecast for a target month is
  produced **twice** — once on the **25th of the *previous* month** (the initial
  forecast) and then **updated on the 10th of the target month** (an in-month
  revision). So for one (station, target-month, model) there are **two distinct
  operational forecast instances** with different issue dates and different lead
  times.
- **Tajik hydromet (taj):** only **one** monthly forecast per target month,
  produced once.

These two issuances are **different forecast products** (different lead time,
different information set). A correct skill evaluation must NOT silently pool or
double-count them, and must NOT silently discard the legitimate in-month update.

**`horizon_value` was recently changed** in the production/write path. Treat its
*current* meaning as authoritative and verify the eval matches it — do not trust
any older comment or plan doc that says "horizon_value is just a within-month
index and never a join key." Re-derive its real meaning from the code.

## The two sides to compare

### A. The eval under audit — `apps/forecast_skill_eval/`

Read these in full:

- `src/forecast_skill_eval/pairs.py` — pair construction. Note especially
  `_long_instance` (keys long-term pairs as `(code, calendar_period,
  valid_from.year)`, maps `horizon_value` → a passive `lead` column, and **drops
  any row whose `is_calendar_aligned` is not True** as
  `forecast_rolling_window`). Note `build_pairs` builds **one pair per forecast
  row** with no de-duplication across issuances.
- `src/forecast_skill_eval/periods.py` — `long_term_calendar_period` /
  `_month_period`. A month forecast is "aligned" only if `valid_from.day == 1`
  AND `valid_to` is the last day of that month; otherwise it is flagged
  non-aligned (→ excluded as rolling-window upstream).
- `src/forecast_skill_eval/regimes.py` — operational/hindcast split via flags or
  issue-date fallback (`DEFAULT_OPERATIONAL_START = 2024-01-01`).
- `src/forecast_skill_eval/api_readers.py` — `read_long_forecasts` and the
  long-term point-value selection (`q → q50 → q_loc`) and any
  `is_calendar_aligned` / `calendar_period` derivation.
- `src/forecast_skill_eval/config.py` and `norms.py` / `observed_truth.py` only
  as needed to understand keys.

### B. The canonical reference — `apps/postprocessing_forecasts/`

This module *produces and writes* the forecasts and computes the repo's own
operational skill metrics. It is the source of truth for how a forecast instance
is identified. Read:

- `src/api_writer.py` — how long-term records are written: `horizon_value`,
  `valid_from`, `valid_to`, `target`, `horizon_in_year`, model fields. (Search
  for `horizon_value`, `valid_from`, `valid_to`, the monthly/quarter/season
  branches around lines ~830–1080.)
- `src/skill_metrics.py` — **how this module pairs a forecast with its observed
  value and norm when computing its own skill**: what key identifies a unique
  forecast instance, how it handles multiple issuances per target period, what it
  treats as the point value, and how (if at all) it stratifies by lead /
  issuance.
- `src/horizon_config.py` — the canonical horizon definitions and any per-horizon
  issuance/lead semantics.
- `src/aggregation.py` and `recalculate_skill_metrics.py` (module root) — the
  operational skill-recalc entry point and any monthly aggregation logic.
- Also skim how `long_term_forecasting` (the LT producer) and the postprocessing
  API actually populate `date` (issue date), `valid_from`, `valid_to`, and
  `horizon_value` for monthly forecasts, so you can state what a kyg "25th-of-
  prior-month" vs "10th-of-target-month" record literally looks like in the DB.

## Questions to answer (each with `file:line` evidence and a verdict)

1. **Issue cadence / double-counting.** When kyg has two monthly forecast
   instances for the same (station, target-month, model), how many pairs does
   `build_pairs` emit, and are both pooled into the same contingency table? Does
   `postprocessing_forecasts` skill computation instead pick one canonical
   issuance, or stratify by issuance/lead? Is the eval double-counting the kyg
   month relative to the canonical behaviour? Is taj (single issuance) handled
   correctly by the same code path?

2. **The rolling-window exclusion.** In the P8 run ~50% of monthly forecasts
   were dropped as `forecast_rolling_window` (`_long_instance` →
   `is_calendar_aligned is not True`). Determine, from `api_writer.py` /
   `skill_metrics.py`, what `valid_from`/`valid_to` the **in-month (10th) update**
   actually carries. If the update still spans the full calendar month
   (`valid_from = 1st`, `valid_to = month end`) it is being **kept and
   double-counted**; if it spans only the remainder of the month it is being
   **silently discarded** as rolling-window. State which it is, and whether
   either matches how `postprocessing_forecasts` scores it. This is the
   highest-priority open question — resolve it concretely.

3. **`horizon_value` semantics.** Re-derive the *current* meaning of
   `horizon_value` from `api_writer.py` / `horizon_config.py`. The eval maps it
   to a passive `lead` column (`_long_instance`, pairs.py). Is that mapping
   correct under the new definition, or does `horizon_value` now encode the
   issuance/revision (so it should be part of the instance key or used to select
   the canonical forecast)? Flag any place the eval relies on the stale "never a
   join key" assumption.

3.5. **Norm and observed-truth alignment.** The eval derives monthly observed
   truth by aggregating daily runoff and assigns norm provenance from a horizon
   mapping. Confirm the eval's norm key and observed-period definition match the
   period boundaries that `postprocessing_forecasts` uses for the same target
   month (no off-by-one in month/quarter/season windowing).

4. **Regime / operational labelling.** The eval falls back to "issue date ≥
   2024-01-01 ⇒ operational" when flags are not meaningful. Given the real issue
   dates of the two kyg issuances and the single taj issuance, does this
   correctly classify both kyg issuances and the taj issuance? Could the date
   fallback mislabel an in-month update?

5. **Point value & model identity.** Confirm the eval selects the same point
   value (`q → q50 → q_loc`) and the same model identifier (`model_type` /
   `model_short`) that `postprocessing_forecasts` writes and scores against — no
   silent model-name mismatch that would split or merge series.

## Output format

Produce a markdown report with:

- **Verdict table:** one row per question above — `consistent` /
  `eval-bug` / `defensible-choice` / `needs-human-decision`, with the single
  most important `file:line` citation.
- **Findings:** for each divergence, a short paragraph: what the eval does (cite),
  what `postprocessing_forecasts` does (cite), why it matters for the kyg
  two-issuance vs taj one-issuance case, and the concrete consequence on the
  skill numbers (e.g. "double-counts kyg months → inflates n_pairs ~2× and
  biases base rate").
- **Recommended changes to the eval** (described, not coded), ordered by impact,
  each tied to a finding. Distinguish "required for correctness" from "optional
  refinement."
- **Open questions for the human** that the code alone cannot resolve (e.g.
  "should the in-month update be scored as a separate shorter-lead product, or
  should only the latest issuance per target month be scored?").

Constraints: read-only; do not edit code; cite `path:line` for every factual
claim; if the code is genuinely ambiguous, say so rather than guessing; do not
invent DB column names — verify them in `api_writer.py` / the API client.
