# Quarter forecast = calendar Q1–Q4: overview plan

**Status**: Not started. Plans rev 4, 2026-09-26, after four review rounds: nine independent reviewers, then five on rev 3, including codex and a combined decision-fidelity review.
**This file owns the decisions and the dependency graph.** Child plans refer to it.
**Trigger**: GitHub #521 (quarterly target-window matching). The bug is real. This plan set replaces the code
approach of branch `sandro_sapphire_2_quaterly_agg`.

## Owner decisions

**2026-09-25**
- **Calendar quarters.** Kyrgyz and Tajik Hydromet issue the quarterly forecast for **calendar quarters
  Q1–Q4**, once per quarter, **Q1 included**.
  - kghm: day 25 of the preceding month, lead 1.
  - tjhm: day 1, lead 0.
- **`horizon_value` = config lead** (kghm 1, tjhm 0).
- **Seven models re-enabled for quarter:** GBT, LR_SM_DT, LR_SM_ROF, MC_ALD, SM_GBT, SM_GBT_LR, SM_GBT_NORM,
  as same-issue averages of their monthly forecasts. LR_Base/LR_SM stay native. Season is unchanged.
- **The schedule change (LTF-014 P0) is gated** on modeller confirmation, a server-state read and owner
  approval.

**2026-09-26**
- **A. Derived quarters** use the monthly forecasts produced **on the quarter's issue date**, for the **next
  three months** (leads `L`, `L+1`, `L+2`).
  - They are identified by (issue date, `horizon_value`), not by the stored window.
  - Fixing the labels at source is a follow-up: **LTF-016**.
- **B. Quarterly ensembles are computed like monthly ensembles** (refined in round 2, decision 1). This
  replaces the 2026-06-23 fixed-LR EM (M1) and the 2026-09-25 "D10" rule, **for quarter only**.
  - **Naive Mean:** all models, no gate.
  - **Skilled Mean:** long-term gate (NSE > 0) plus min-pairs, 1/MAE-weighted.
  - **No quarterly Ensemble Mean.** This follows monthly, where EM is being removed (PP-059, owner
    2026-08-18).
  - Quarterly gap detection keys on Naive Mean.
- **C. Quarter min-pairs K = 10.**
- **D. Bounds for rows without quantiles** = forecast ∓ δ (the delta method, as for short-term), applied at
  display time.
- **E. Bulletin:** publish whatever is available until FD-030. No interim guard.
- **F. tjhm LR quarter rows holding postprocessing aggregates:** re-import the native hindcast values before
  the recalc.
- **G. Temporary LR fallback.** Until LTF-014 P0/P2 are deployed, LR quarters are derived from monthly
  forecasts where no native LR row exists. PP-065 P3 removes the fallback afterwards.
- **H. LTF-015:** refuse early runs that fall in a different calendar month than the scheduled issue date.

**2026-09-26, round 3**
- **LTF-014 P0 is deferred; the configs stay at `[3..9]`.** The reason for `[3..9]` cannot be answered
  without the modeller. This does not block the code plans:
  - PP-064 excludes the rolling issues;
  - PP-065's derived models and the decision-G LR fallback supply Q1 and tjhm Q4.
  - Deferred with P0: P0b (moot), LTF-014 P2, PP-065 P3, and DOC-009 row 11.
- **Winter issues are valid for LR_Base/LR_SM** (owner).
- **D1:** accept the offset target.
- **D2:** the P2 procedure is approved.

**2026-09-26, round 2**
1. No quarterly EM (see B).
2. **Old persisted ensemble rows are accepted for now.** This covers the fixed-LR EM and the old Naive and
   Skilled Mean rows. They stay in `long_forecasts` (D8 / PP-041).
3. **Fallback LR rows are not persisted and are accepted as invisible.** The dashboard card and the bulletin
   show no LR row for fallback quarters until LTF-014 P0/P2.
4. **Decision F is by provenance.** Remove tjhm LR QUARTER rows with `date = valid_from` that have no
   counterpart in the LT module CSV, in any quarter, 2026 included.
   - **Superseded for 2026-10-01:** the owner chose option (a). A standalone clear-and-recover step
     (LTF-014 P0b) runs before 2026-11-30, independent of the postprocessing deploy. F preserves its
     recovered rows.
   - Take a preserve manifest, a dry run and a backup first.
5. **PP-065 is on the 2026-12-25 critical path.** Its fallback guarantees a kghm Q1 even without LTF-014 P0.
6. **Early kghm runs** on the 20th–24th are accepted but produce no quarter product. They log a WARNING.

## What is wrong today

| Layer | Today | Plan |
|---|---|---|
| Schedule (data-repo model configs) | `forecast_months [3..9]` in both orgs: a rolling window is issued monthly Mar–Sep, and there is **no Q1** | LTF-014 |
| Postprocessing | The quarter label comes from the `valid_from` month only, and skill joins on the calendar quarter, so rolling windows are scored against another quarter (#521). Flag ON: a Dec-issued Q1 is trimmed out. Quarter rows come in three date populations | PP-064 |
| Quarter model set and ensembles | LR-only since 2026-06-23. The old monthly-derived path averages across issue dates and accepts 2 of 3 months | PP-065 |
| Monthly labels (stored data) | Pre-Feb-2026 hindcast rows carry offset windows (both orgs, mostly tjhm) and wrong January years (kghm GBT family). The producer is already fixed on trunk; the DB rows are stale and mis-score monthly skill | LTF-016 |
| GBT-family climatological bounds | `_add_climatological_quantile_bounds` takes the σ month from the raw `valid_from` and leave-one-out from `today.year`, so the Q25/Q75 of GBT-family monthly models use the wrong month's σ for many issue months: kghm month_1–3 (month-dependent) and tjhm month_3 (Jul and Dec issues). **Live since 2026-04-14** | LTF-017 |
| Dashboard quarterly card | The fetch window is set at import time. The latest issue wins whatever window it covers. The renderer keeps only rows at the max date. The caption relies on lead-1 arithmetic | FD-029 |
| Bulletin quarterly section | Uses the monthly norm, picks a model with `head(1)`, and shows a range only | FD-030 |
| Docs | Describe quarter as "rolling, not calendar". EM-parity and "0 deprecated rows" gates would false-fail. Cleanup predicates would delete product rows | DOC-009 |
| Early manual runs of issue-day-1 modes | The value is ratio-adjusted to the wrong month. This is live for the tjhm month modes | LTF-015 |

## The plans

| ID | File (`issues/`) | Priority / target |
|---|---|---|
| LTF-014 | `high_prio_gi_draft_ltf_quarter_calendar_schedule.md` | High. P0 is gated. Soft targets: tjhm Q4 2026-10-01 (recoverable until 2026-11-30), kghm Q1 2026-12-25 |
| PP-064 | `high_prio_gi_draft_pp_quarter_calendar_window_validation.md` | High. **A deployed by 2026-12-25** |
| PP-065 | `high_prio_gi_draft_pp_quarter_derived_models.md` | High. **Deployed by 2026-12-25.** Four agent phases (P1a–P1d), then rollout. P3 removes the LR fallback |
| LTF-016 | `high_prio_gi_draft_ltf_monthly_window_labels.md` | High. A data fix (re-import and delete) plus verification; no producer change. Independent of the quarter chain |
| LTF-017 | `high_prio_gi_draft_ltf_climatological_bounds_raw_month.md` | High (live). Independent |
| FD-029 | `mid_prio_gi_draft_fd_quarter_card_calendar_window.md` | Medium. **Deployed and restarted by 2026-12-25** |
| DOC-009 | `mid_prio_gi_draft_doc_quarter_calendar_contract_amendments.md` | P1a (safety warnings) before PP-065 deploys; P1b after D4 |
| FD-030 | `mid_prio_gi_draft_fd_quarter_bulletin_norms_and_model.md` | Medium. Blocked on D6 |
| LTF-015 | `mid_prio_gi_draft_ltf_day1_early_run_refusal.md` | Medium |

## Open decisions

| # | Decision | Who | Blocks |
|---|---|---|---|
| D1 | ~~90-day offset target vs calendar-exact~~ **Decided 2026-09-26: accept the offset target** (measured median bias ≤ 2 %) | Owner | — |
| D2 | ~~Hindcast write set~~ **Decided 2026-09-26: the LTF-014 P2 procedure** (scratch config copy, scratch output, CSV only, filtered import, durable publication). Applies when P0 resumes | Owner | — |
| D3 | PP-064 B5: empty quarter skill is handled as for monthly, i.e. no ensembles at all, Naive Mean included. Confirm. B2, B4 and B6 moved into PP-065 | Owner | PP-064 B → recalc |
| D4 | The service owner acknowledges the convention-doc amendment; the owner signs off the decision-request amendment | Service owner, owner | DOC-009 P1b only |
| D6 | Bulletin quarterly section (FD-030's D6a–D6c): the product published (Skilled Mean / Naive Mean / a model), presentation in months 2–3 of a quarter, the norm reference year, and an optional point-value/model tag (a template change) | Owner | FD-030 |
| D8 | Delete legacy rows in `long_forecasts` (one-way SQL; colleague-owned service) | Owner + service owner | Nothing |

Resolved since rev 2:
- **D5** (caption issue date): resolved by the schedule-based native-row rule.
- **D7** (early runs): refuse.
- **D9 and D10**: superseded by decisions B and C.

## Rollout and communication

1. **Server reads, per org, before any change.**
   - The live env used by the postprocessing and dashboard containers: `SAPPHIRE_SKILL_LEAD_AWARE` and
     `ieasyhydroforecast_ml_long_term_supported_modes`. The local `.env_kghm_server` has no
     `SAPPHIRE_SKILL_LEAD_AWARE` line, so it defaults to OFF.
   - The quarter configs (the LTF-014 gate).
   - The crontab LT line.
   - `ieasyhydroforecast_min_pairs_long_term_quarter`. An explicit 5 on a server would cancel decision C.
2. **Who executes.** Each PR names who runs the server steps (the owner or hydromet IT). Image pull and
   restart per module follow `doc/prod/update_deployment_checklist.md`.
3. **Order.** Steps 1–3 are all deployed before 2026-12-25.
   1. PP-064 A and FD-029. Deploy both and restart the dashboard.
   2. DOC-009 P1a.
   3. PP-065 and PP-064 B (the B5 check). Then, **in one window between LT cron days** (kghm 10/25,
      tjhm 1): deploy, run the tjhm decision-F step, and run **one recalc per org** after a DB export.
   4. LTF-014 P0 when its gate clears.
   5. LTF-014 P2.
   6. PP-065 P3, deployed.
   7. The second recalc.
4. **Notice to the Kyrgyz and Tajik hydromets, before the recalc.**
   - Quarterly forecasts are issued four times a year: kghm has no more monthly Mar–Sep issues; tjhm gets
     new Jan and Oct issues.
   - Seven more models appear in the quarterly outputs.
   - The quarterly ensembles are now Naive Mean and Skilled Mean, as for monthly. There is no quarterly
     Ensemble Mean.
   - Bounds for models without quantiles are ±δ on the dashboard. Until FD-030 lands, the bulletin range
     can be blank.
   - Until the schedule change lands, kghm Q1 and tjhm Q1/Q4 show no LR model row. LR is still included in
     the ensembles.
   - Stored quarterly skill values change, some of them sharply. This is a corrected verification method,
     not a model change; include before/after distributions per org.
   - The `validate_pipeline` quarter checks report FAIL on admitted-but-inactive days (INFRA-022).
5. **#521.** Tell Sandro which parts of his branch the plans adopt. His PR stays open until PP-064 and
   PP-065 land.

## Why quarter diverged (history, researched 2026-09-25)

- **The calendar intent was documented and implemented.** The 2026-02-06 plan maps Dec 25 → Q1 … Sep 25 → Q4
  from all nine monthly models; `aggregation.py` implemented it on 2026-03-04.
- **The long-term module built a rolling mode instead.** Its "next 3 months" mode came with
  `forecast_months [3..9]`, set in the data repos around 2026-02-21 with no recorded reason.
- **PR #295 merged the two** by relabelling rolling rows. That is the #521 bug.
- **The CSVs were then read as the specification.** A 2026-06-21 investigation took the hindcast CSVs as the
  product definition. The 2026-06-22 service-owner answer concerned only `horizon_value`, but was later cited
  as settling the product.

## Deferred findings (not filed)

- `apps/forecast_skill_eval` reads quarter `horizon_value` as the quarter number (`dashboard/app.py:385-386`,
  `cli.py:195`). It is an analysis tool; confirm before filing.
- `apps/forecast_dashboard/src/db.py:29-30` fixes `CURRENT_YEAR`/`PREVIOUS_YEAR` at import for every horizon.
  FD-029 fixes only the quarter fetch.
- The hydrology review suggested empirical error quantiles from hindcast residuals as a better band than ±δ.
  Not pursued; decision D chose δ.
- NSE > 0 on small samples passes no-skill models often (40–45 % at n = 5–10). K = 10 mitigates this.
  Leave-one-year-out member selection would remove the in-sample selection bias; it is not planned.

## Dependency graph

Stages:
- **merge**: PR merged;
- **deploy**: on the servers;
- **ops**: manual step.

```json
{
  "phases": {
    "DOC-009.P1a":     { "stage": "merge",  "depends_on": [] },
    "DOC-009.P1b":     { "stage": "merge",  "depends_on": ["D4"] },
    "DOC-009.P2":      { "stage": "merge",  "depends_on": ["LTF-014.P0.kghm", "LTF-014.P0.tjhm", "PP-065.P1d"] },
    "LTF-014.P0.tjhm": { "stage": "ops",    "depends_on": ["LTF-014 P0 gate"], "status": "DEFERRED by owner 2026-09-26 (configs stay [3..9])" },
    "LTF-014.P0b":     { "stage": "ops",    "depends_on": ["LTF-014.P0.tjhm"], "deadline": "2026-11-30", "note": "only if the Oct-1 run was missed: pause crons, export, delete the tjhm 2026-10-01 QUARTER rows (service owner), lt_recovery, resume, verify" },
    "LTF-014.P0.kghm": { "stage": "ops",    "depends_on": ["LTF-014 P0 gate"], "status": "DEFERRED by owner 2026-09-26 (configs stay [3..9])" },
    "LTF-014.P1":      { "stage": "merge",  "depends_on": [], "parallel_agents": 1 },
    "LTF-014.P2":      { "stage": "ops",    "depends_on": ["LTF-014.P0.kghm", "LTF-014.P0.tjhm", "D2"] },
    "LTF-015":         { "stage": "merge",  "depends_on": ["LTF-014.P1"], "parallel_agents": 1 },
    "LTF-016.P0":      { "stage": "ops",    "depends_on": [] },
    "LTF-016.P1":      { "stage": "ops",    "depends_on": ["LTF-016.P0"], "note": "verification: deployed LT image >= 99c5a552, server CSV labels" },
    "LTF-016.P2":      { "stage": "ops",    "depends_on": ["LTF-016.P1"], "note": "re-import and delete with the service owner, then the monthly recalc" },
    "LTF-017":         { "stage": "merge",  "depends_on": [], "parallel_agents": 1 },
    "PP-064.A":        { "stage": "merge",  "depends_on": [], "parallel_agents": 1 },
    "PP-064.A.deploy": { "stage": "deploy", "depends_on": ["PP-064.A"], "deadline": "2026-12-25" },
    "FD-029":          { "stage": "merge",  "depends_on": [], "parallel_agents": 1 },
    "FD-029.deploy":   { "stage": "deploy", "depends_on": ["FD-029"], "deadline": "2026-12-25", "note": "restart the dashboard container" },
    "PP-065.P1a":      { "stage": "merge",  "depends_on": ["PP-064.A"], "parallel_agents": 1 },
    "PP-065.P1b":      { "stage": "merge",  "depends_on": ["PP-065.P1a"], "parallel_agents": 1 },
    "PP-065.P1c":      { "stage": "merge",  "depends_on": ["PP-065.P1a"], "parallel_agents": 1 },
    "PP-065.P1d":      { "stage": "merge",  "depends_on": ["PP-065.P1b", "PP-065.P1c"], "parallel_agents": 1 },
    "PP-064.B":        { "stage": "merge",  "depends_on": ["PP-065.P1d", "D3"], "parallel_agents": 1, "note": "the B5 check only" },
    "deploy.pp":       { "stage": "deploy", "depends_on": ["PP-065.P1d", "PP-064.B", "DOC-009.P1a"], "deadline": "2026-12-25" },
    "tjhm.reimport":   { "stage": "ops",    "depends_on": ["deploy.pp"], "note": "decision F, with the service owner" },
    "recalc.1":        { "stage": "ops",    "depends_on": ["deploy.pp", "tjhm.reimport"], "note": "PP-064 C + PP-065 P2, per org, after an export" },
    "PP-065.P3":       { "stage": "merge",  "depends_on": ["LTF-014.P2", "recalc.1"] },
    "PP-065.P3.deploy":{ "stage": "deploy", "depends_on": ["PP-065.P3"] },
    "recalc.2":        { "stage": "ops",    "depends_on": ["LTF-014.P2", "PP-065.P3.deploy"] },
    "FD-030":          { "stage": "merge",  "depends_on": ["FD-029", "D6"], "parallel_agents": 1 }
  }
}
```

**Shared files, strictly sequential:**
- `src/data_reader.py` quarter readers: PP-064 A → PP-065 P1b → PP-065 P3. PP-064 B no longer edits them.
- `apps/long_term_forecasting/tests/test_lt_utils.py` and `test_post_process_lt_forecast.py`: LTF-014 P1 →
  LTF-015.
- `doc/plans/module_issues.md`: DOC-009 and the index updates.

Everything else touches disjoint files and can run in parallel, including LTF-016 and FD-029.
