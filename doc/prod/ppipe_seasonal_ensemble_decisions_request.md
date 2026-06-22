# Decision request (PP0): seasonal ensemble semantics, per-lead skill, historical regeneration

**For**: long-term modeller + postprocessing-service owner
**From**: forecast-tools side (MIG-008 / P-PIPE -- extending the config-lead `horizon_value` convention
to the `apps/postprocessing_forecasts` quarterly/seasonal ensemble pipeline)
**Blocks**: all P-PIPE implementation and the downstream data cleanup (P3/P4). Nothing is coded yet.
**Why**: a plan + review found that the answers below **change the size and shape** of the work
(especially the seasonal half). We need your decisions before writing any code.

## Background (settled)

`horizon_value = operational_month_lead_time` (config lead). Quarter: kyg `hv1`, taj `hv0`. Season per
issue: kyg Jan/Feb/Mar/Apr = `hv3/2/1/0`, taj Apr = `hv0`. The raw forecast writers already follow
this. The **ensemble** pipeline (`api_writer._write_quarterly/seasonal_ensemble_to_api`) does not -- it
writes quarter `hv = calendar quarter (1-4)` and season `hv = 1` (hardcoded). We will fix it; the
questions are about *what the correct product is*.

## Decision 1 -- Seasonal ensemble: one product per run, or four issue products? (the big one)

Today the ensemble pipeline **collapses** all seasonal issues for a target year into a single row:
the reader drops the issue date, and the ensemble/dedup key is `(season_year, code, model_short)`. So
a Kyrgyz target season currently yields **one** ensemble row regardless of the Jan/Feb/Mar/Apr issue.

Which is correct?

- **(A) One ensemble product per target season** (e.g. the latest/current issue only). Then the
  ensemble pipeline writes a single seasonal `hv` (the current issue's lead), the collapse is fine, and
  only the **raw** forecast rows populate all four `hv3/2/1/0` buckets. **Small change.**
- **(B) Four distinct seasonal ensemble products**, one per issue (Jan/Feb/Mar/Apr), each at its own
  lead `hv3/2/1/0`. Then we must carry the **issue date** through the reader, ensemble grouping, dedup,
  and writer (a per-issue ensemble). **Materially larger change.**

Please choose A or B. If B, confirm the ensemble should be computed **independently per issue**.

## Decision 2 -- Per-lead seasonal skill (only if Decision 1 = B)

Seasonal skill is currently a single value per target season (no lead dimension). If the four issues
become distinct products at different leads, should each lead get its **own skill** (a January
3-month-ahead forecast is normally less skilful than an April 0-month-ahead one), or is the **same
season skill applied to all leads**? This affects the ensemble weighting (EM / Skilled Mean).

## Decision 3 -- Historical quarter/season ensemble: regenerate, re-stamp, or drop?

The DB holds ~41k historical QUARTER **ensemble** rows (EM / Naive Mean / Skilled Mean, 2000-2026) plus
the seasonal `hv1` ensembles. Under the new convention these need to move to the correct hv. The only
full-history rebuild path is `recalculate_skill_metrics.py` with `SAPPHIRE_RECALC_START_YEAR=2000`
(a **manual** run; the cron never does it). Options:

- **(i) Re-stamp** the historical ensemble rows' `horizon_value` in place (cheap, preserves history,
  same approach we used for the raw LR rows). No recalc needed.
- **(ii) Delete + full-history regenerate** via the manual recalc (expensive, and a delete that depends
  on a non-cron manual step is risky).
- **(iii) Drop** the historical ensembles entirely (only keep forward-generated ones).

Which do you want, and should the EM / Naive Mean / Skilled Mean historical ensembles be preserved at
all?

## Decision 4 (technical confirmation) -- config source for postprocessing

Confirm postprocessing may read the same long-term config JSONs as `apps/long_term_forecasting` to
resolve the lead (env/path contract), and the issue-month -> `seasonal_january/february/march/april`
mapping. We can propose a concrete contract if you prefer; just confirm the config root is shared.

## What is on hold until you answer

- All P-PIPE code (writer/reader/ensemble/dashboard changes).
- The data cleanup (P3/P4) -- gated behind P-PIPE + your Decision 3.

Once you answer Decision 1 (and 2 if B) and Decision 3, we will rewrite the P-PIPE phase scope +
acceptance and proceed via the usual plan -> review -> implement loop. Aggregate-only DB verification;
sentinel station codes in all artifacts.

---

## ANSWERS (2026-06-22, modeller / owner)

- **Decision 1 = B**: **four distinct seasonal ensemble products**, one per issue (Jan/Feb/Mar/Apr ->
  `hv3/2/1/0`), computed independently per issue. This is the larger change: issue identity must be
  threaded through the reader projection (`_SEASONAL_FC_COLS`), the ensemble groupby, the dedup keys,
  and a per-row issue->lead mapping in the writer.
- **Decision 2 = yes**: **each lead gets its own skill** -- seasonal skill gains an issue/lead
  dimension; the skill->ensemble join must key on lead (a 3-month-ahead January ensemble is weighted
  with January-lead skill, not the April-lead skill).
- **Decision 3 = regenerate** ("re-run them with the proper settings"): rebuild the historical
  ensembles via `recalculate_skill_metrics.py`, then clean up the obsolete old-convention rows.
  **No new cron job needed** -- the recurring recalc is already scheduled
  (`bin/bimonthly_long_term_skill_metrics_recalculation.sh` covers QUARTERLY/SEASONAL via
  `run_skill_metrics_recalc.sh`; plus `bin/yearly_skill_metrics_recalculation.sh`). The one-time deep
  history (back to 2000, since the cron default window is ~`current_year-20` = 2006) is a **single
  manual recalc run** with `SAPPHIRE_RECALC_START_YEAR=2000` per deployment. Sequence: P-PIPE code ->
  deploy -> recalc (start 2000) writes new-convention rows -> aggregate verify -> then clean up the
  obsolete old-hv rows.
- **Decision 4 = confirmed**: postprocessing may read the long-term config JSONs to resolve the lead +
  issue-month -> seasonal-config mapping.

Scope is therefore the **B branch** (the larger one). P-PIPE will be re-planned in detail (expanded
seasonal issue-threading + per-lead skill + the regenerate-then-clean sequence) before implementation.
