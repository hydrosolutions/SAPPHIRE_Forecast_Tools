# Investigator-LLM Prompt — Should quarter & season skill metrics be grouped by forecast issue date?

Copy everything below the line into an investigator LLM with access to the
SAPPHIRE_forecast_tools repo and the postprocessing DB (local dev is fine).
Read-only screening. Cite `path:line`. Report aggregate counts only — never raw
station codes or discharge values.

---

You are auditing how **quarterly** and **seasonal** forecast skill metrics are
grouped in SAPPHIRE. The hypothesis to test: like monthly forecasts, quarter and
season forecasts are issued **multiple times per target period at different
leads / issue dates** (a distinct forecast product per issuance), so their skill
metrics must be grouped by **forecast issue date (or lead / `horizon_value`)**.
If the current computation pools all issuances of a target quarter/season into
one skill number, it conflates different-lead products and the headline skill is
not interpretable. Confirm or refute this, and recommend the concrete grouping
change.

> ## ✅ OWNER DECISION 2026-07-14 — the QUARTER half of this prompt is ANSWERED. Do not investigate it.
>
> The product semantics (from the owner) settle what no code reading could:
>
> | Horizon | What we actually publish | Consequence for skill |
> |---------|--------------------------|------------------------|
> | **month** | **Kyrgyz Hydromet CHOOSES the lead** to put in the bulletin | Skill **must be per-lead**; the display must show the skill of the lead the operator selected. Collapsing leads at compute time destroys what the bulletin needs. → see **FD-018** + `SAPPHIRE_SKILL_LEAD_AWARE`. |
> | **quarter** | We provide only the **lowest available lead** (no operator choice) | One issuance per target quarter is ever published, so collapsing to the smallest lead is **CORRECT** — it scores the product we actually ship. |
>
> **So the quarter dedup in `pairs.py:292-307` is right by design, not by accident.** It picks the
> smallest lead *present* = lead 0 when it exists, lead 1 when it does not — exactly the published
> product. **Do not "fix" it to per-lead.** What it needs is *documentation* (the rule currently
> lives only in a code comment) and *enforcement*: `DEFAULT_OPERATIONAL_ISSUE_DAYS = ()`
> (`config.py:44`) is **empty**, so nothing actually pins the operational issuance — it is assumed,
> not checked.
>
> **The hypothesis in the paragraph above is therefore REFUTED for quarter** ("must be grouped by
> issue date / lead" — no; we publish one lead). It stands only for **month**, which is already
> tracked as FD-018 / the lead-aware work.
>
> **SEASON — RESOLVED 2026-07-14: keep current behavior, no code change.** Season *can* in principle
> be published at several leads, but hydromets always pick the shortest. Verified against the code:
>
> - **Dashboard (what hydromets see): already single-lead = the shortest.**
>   `_default_seasonal_issue_month()` picks the LATEST supported seasonal issue month ≤ the current
>   month (`apps/forecast_dashboard/src/db.py:97-104`), and the latest issue month is the one closest
>   to the season — i.e. the shortest lead. `_resolve_seasonal_horizon_value` then resolves that
>   mode's configured `operational_month_lead_time`
>   (`apps/iEasyHydroForecast/long_term_horizon_resolver.py:73-81`). Quarter behaves the same way via
>   `quarter_horizon_value()`. So the display already shows only the shortest lead.
> - **Evaluator: season RETAINS leads 0–3** (`pairs.py:292-307`), unlike quarter (which collapses to
>   the smallest lead present). **This is INTENDED, not a defect.**
>
> **Why the two systems differ on season — and why that is correct.** `forecast_skill_eval` is an
> **analysis tool, not an operational forecasting tool** (owner, 2026-07-14). It *should* evaluate
> every lead, including leads that are never published: that is what makes it useful for assessing
> forecast quality across the lead spectrum. The dashboard, by contrast, serves the operational
> bulletin and therefore shows only the lead that is actually published (the shortest). **The
> divergence is by design — do not "reconcile" them.** The only thing to be explicit about is that a
> seasonal figure quoted from the evaluator is *per lead*, so any single number lifted out of it must
> say which lead it refers to.
>
> **"Availability" / no runtime fallback — ACCEPTED 2026-07-14, no issue to file.** "Available" in the
> owner rule ("lead 0 if available, else lead 1") means **configured** (the deployment runs that
> mode), not **present in the data**. There is deliberately no runtime data-availability fallback in
> `long_term_horizon_resolver.py`: the dashboard resolves the configured
> `operational_month_lead_time` and queries exactly that `horizon_value`. **Owner decision: current
> behavior stands for quarterly; leave it as-is.**
>
> ## ⚠️ CORRECTIONS 2026-07-14 — do NOT hand this prompt to an investigator unchanged
>
> An out-of-loop review found the "take as given" block below contains **false premises**. That is
> the most damaging place for an error, because the investigator is explicitly told not to
> re-derive it. Fix these before reuse:
>
> - **`horizon_value` is NOT the lead for quarter/season.** The evaluator states the stored
>   `horizon_value` is not a lead for these horizons and derives the lead instead
>   (`apps/forecast_skill_eval/src/forecast_skill_eval/pairs.py:30-35`;
>   `config.py:92-100`). The bullet below asserting `horizon_value` = lead is wrong for exactly the
>   two horizons this prompt is about.
> - **The evaluator does NOT uniformly "already stratify by lead".** The semantics are *split*:
>   **quarter** dedups to the smallest-lead issuance and reports by target quarter, while **season**
>   keeps genuine leads (`pairs.py:292-307`; tests `test_pairs.py:1320-1366`, `:1368-1403`,
>   `:1449-1520`). So it cannot be used as a clean "reference for intended behavior".
>
> **This makes the prompt's core question an OWNER DECISION, not an investigation:** should quarter
> keep the *smallest-lead headline* (one interpretable number per target quarter) or expose
> *per-genuine-lead* detail like season? Decide that first — an investigator cannot derive it from
> the code, and the current split may well be deliberate.

## Established context (take as given — do not re-derive)

- ~~`horizon_value` = **lead**~~ — **FALSE for quarter/season; see corrections above.** For
  month, `horizon_value` carries the lead; for quarter/season the stored value is not a lead and the
  evaluator derives one. Long-term modes do run at multiple leads: the local daily pipeline logged
  "active modes: month_1 month_2 month_3 quarter", i.e. quarter/season targets
  are forecast from several issue dates.
- The **monthly** skill computation already has this latent pooling problem:
  `apps/postprocessing_forecasts/src/skill_metrics.py` groups point metrics by
  `["month_in_year", "code", "model_short"]` (`:1194`, `:1208`, `:1235`) — the
  merged frame carries `valid_from`/`valid_to`/`date`/`flag`/`horizon_value`
  (`:1086`) but **none of these is in the group key**. Filed as a coordination
  item: `doc/plans/issues/high_prio_gi_draft_pp_skill_lead_pooling.md`.
- ~~The new evaluator `apps/forecast_skill_eval` already stratifies long-term skill
  by lead (its month/quarter/season contingency rows are per-`lead`). Use it as
  the contrast/reference for the intended behavior.~~ — **FALSE as stated; see corrections above.**
  The evaluator's long-term semantics are split: **quarter** dedups to the smallest-lead issuance
  (reported by target quarter), **season** keeps genuine leads (`pairs.py:292-307`). It is therefore
  *not* a clean reference for "intended behavior" — which of those two is intended is precisely the
  open question.
- `apps/postprocessing_forecasts/` and `sapphire/services/` are colleague-managed
  — this is read-only; propose changes, do not implement.

## Questions to answer (each with `path:line` evidence)

1. **Where are quarter and season skill metrics computed and grouped?** Find the
   quarterly/seasonal skill functions invoked by
   `apps/postprocessing_forecasts/recalculate_skill_metrics.py` (QUARTERLY /
   SEASONAL modes) and the aggregation in `src/aggregation.py`
   (`aggregate_monthly_obs_to_quarterly` `:96`, `aggregate_monthly_obs_to_seasonal`
   `:148`) and `src/skill_metrics.py`. State the exact groupby keys used to
   produce each quarter/season skill row.
2. **Is forecast issue date / lead in the grouping key?** Do the quarter/season
   skill rows key on `[quarter_in_year, code, model_short]` /
   `[season_in_year, code, model_short]` only (analogous to monthly), or do they
   include `horizon_value`/`date`/issue-date? Quote it.
3. **Do quarter/season forecasts actually have multiple issuances per target
   period?** From the DB `long_forecasts` (`horizon_type` in {quarter, season}),
   tabulate, per target period, the number of distinct issue dates / `horizon_value`
   values per `(code, model_type, target period)`. Report counts (no codes). If
   >1, pooling is real and conflates leads.
4. **What is the consequence of the current grouping?** If issue date/lead is
   absent from the key, quantify: how many quarter/season skill rows pool ≥2
   distinct leads, and what is the implied inflation of `n_pairs` / distortion of
   the metric. Relate to the just-run recalc (quarter median `n_pairs`≈17, season
   median≈3) — would per-lead grouping change those?
5. **Aggregation order.** Quarter/season are aggregated from monthly. Determine
   WHEN issue date would need to be preserved: is monthly aggregated to
   quarter/season **before** or **after** the forecast↔obs pairing, and does the
   aggregation (`aggregation.py`) drop `horizon_value`/`date` so that a later
   per-lead grouping becomes impossible? If the lead is lost during aggregation,
   the fix must preserve it upstream.
6. **Recommendation.** Should quarter and season skill be grouped by issue
   date/lead? If yes, give the concrete minimal change: which groupby key(s) to
   extend (e.g. add `horizon_value`), where in the aggregation pipeline the lead
   must be carried through, and whether the API/DB skill-metric schema already
   has a column to hold the lead dimension (check
   `sapphire/services/postprocessing/app/models.py` / `schemas.py` for a
   `horizon_value`-like field on the skill-metric model). Flag any API-contract
   change needed (these are colleague-owned).

## Output

- A table: horizon (monthly / quarter / season) → current skill groupby key →
  includes issue-date/lead? (yes/no) → `path:line`.
- The per-target-period issuance counts from question 3 (aggregates only).
- A verdict per horizon: pooling problem present (yes/no) and severity.
- A concrete, minimal recommended grouping change for quarter and season,
  consistent with the monthly fix, distinguishing apps-side vs colleague-owned
  service/API changes.
- Any check you could not perform (e.g. needs a DB query you lack access to) —
  state it as an open risk; do not guess.

Constraints: read-only; cite `path:line` and config/DB column names you verify;
report counts not codes; if the lead is genuinely lost before pairing, say so
plainly rather than assuming it can be grouped late.
