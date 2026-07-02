# Investigator-LLM Prompt — Verify operational monthly issue days for the skill-eval filter

Copy everything below the line into an investigator LLM that has access to BOTH
(a) the SAPPHIRE_forecast_tools repo and (b) the live deployment config + the
postprocessing DB (local dev is fine). Read-only screening; cite `path:line` and
report numbers, never raw station codes or discharge values.

---

You are auditing one filter in `apps/forecast_skill_eval`. The eval scores an
irrigation limit-plan decision per forecast horizon. For the **month** horizon it
keeps only forecasts issued on the *operational* issue days and discards the
rest. The filter currently defaults to the day-of-month set **{1, 10, 25}**
(`operational_issue_days` in `config.py`; applied in `pairs.py:_long_instance`).
Those values were derived **empirically from the DB**, NOT confirmed against the
operational forecast configuration. Your job: confirm or correct them against the
*actual working*, and recommend the right filter form.

## Established context (take as given — do not re-derive)

- `long_forecasts` month rows are two populations: (1) operational, calendar-
  aligned (`valid_from`=1st, `valid_to`=month end); (2) an erroneous bulk-migrated
  "rolling 31-day" product (`valid_from`=issue date, `valid_to`=+31d) issued on a
  5-day grid. Population 2 is being discarded (confirmed migration error). The
  eval already drops it via the `is_calendar_aligned` check.
- Empirically, calendar-aligned month forecasts cluster on issue day **1** (almost
  entirely Tajik / amu_darya, code prefix 17), **10** and **25** (Kyrgyz /
  chu_kyrgyz + syr_darya, prefixes 15/16). `horizon_value` = lead: the 10th is
  dominated by lead 0 (in-month update of the current month); the 25th carries
  leads 1–12.

## What is unverified and why

- `config_monthly.json:9` sets `operational_issue_day: 25`, BUT the "monthly"
  mode is in `NON_OPERATIONAL_MODES` (`apps/long_term_forecasting/lt_schedule_query.py`)
  — it is a calibration mode, not the operational one. So `25` here is NOT proof
  of an operational value.
- The real operational modes are `month_0`, `month_1`, … Each has its own config
  with `operational_issue_day` (read by `config_forecast.py:227`
  `get_operational_issue_day`). Those per-mode config JSONs are NOT in the repo;
  they live in the deployment config directory (env `ieasyhydroforecast_*`,
  `LT_CONFIG_PATH`). They differ per organization (Kyrgyz vs Tajik vs Uzbek).
- `ISSUE_DAY_TOLERANCE = 10` (commented: must be `5` for operational use) in
  `lt_schedule_query.py` — an operational run may fire within ±N days of the
  nominal issue day, so issuances may NOT land on exactly one day-of-month.

## Questions to answer (each with evidence)

1. **Per-mode, per-org operational issue day.** From the actual deployment config
   dir (the `month_0.json`, `month_1.json`, … per org), list `operational_issue_day`
   and `operational_month_lead_time` for every operational monthly mode, for each
   organization present in the dev DB (at least Kyrgyz and Tajik). Confirm or
   correct: is the kyg in-month update (lead 0) issued on the **10th**? The kyg
   prior-month / multi-lead forecast on the **25th**? The Tajik forecast on the
   **1st**? Give the exact configured days.
2. **Tolerance.** Given `ISSUE_DAY_TOLERANCE` (and its operational target of 5),
   do real operational issuances land on day-of-month values OTHER than the
   nominal issue day? Quantify from the DB: for calendar-aligned month forecasts,
   the distribution of issue-date day-of-month, split by organization (or code
   prefix) and by `horizon_value`/lead. Report counts (no codes).
3. **Filter-form recommendation.** Decide which is correct for the eval:
   (a) an exact day-of-month set like {1,10,25};
   (b) a per-org / per-lead nominal day ± tolerance window;
   (c) rely on `is_calendar_aligned` alone (since `calendar_month_adjustment:true`
       already separates operational from the rolling product) and treat issue
       day only as a lead label, not a filter.
   Justify against questions 1–2. If (a), give the definitive day set per org; if
   (b), give nominal day + window per org/lead.
4. **False-drop check.** The eval's current exact-{1,10,25} filter drops a large
   number of month rows as `forecast_non_operational_issue_day`. Determine how
   many of those are genuinely the rolling/erroneous product (acceptable) vs
   legitimate operational forecasts issued within tolerance on an adjacent day
   (would be wrongly discarded). Quantify the wrongly-dropped count, if any.
5. **Cross-org generality.** Will the chosen filter still be correct for the Uzbek
   deployment and any future org, or is it Kyrgyz/Tajik-specific? Note what would
   break.

## How to look

- Repo anchors: `apps/long_term_forecasting/config_forecast.py:227`,
  `lt_schedule_query.py` (`ISSUE_DAY_TOLERANCE`, `NON_OPERATIONAL_MODES`,
  `query_schedule`), `config_monthly.json`, `post_process_lt_forecast.py`
  (calendar snap), and the eval `apps/forecast_skill_eval/src/forecast_skill_eval/`
  (`config.py` `operational_issue_days`, `pairs.py` `_long_instance`,
  `ISSUE_DAY_FILTER_HORIZONS`).
- Deployment: the per-mode config JSONs in the org config dir; the
  `ieasyhydroforecast_organization` env value; the supported-modes env
  (`ieasyhydroforecast_ml_long_term_supported_modes`).
- DB: query `long_forecasts` for `horizon_type='month'`; group issue-date
  day-of-month × calendar-alignment × org/prefix × `horizon_value`. Report
  aggregates only.

## Output

- A table: organization × operational monthly mode → `operational_issue_day`,
  `operational_month_lead_time`, source `path:line` or config file.
- A verdict on the eval filter: exact set / tolerance window / alignment-only,
  with the concrete parameters to set `operational_issue_days` (or the
  replacement) to.
- The wrongly-dropped count from question 4, and any org-generality caveat.
- Any discrepancy between configured issue days and the DB cadence.

Constraints: read-only; cite `path:line` and config file names; report counts not
codes; if a config is not present in the environment you are given, say so and
state what you still could not verify rather than guessing.
