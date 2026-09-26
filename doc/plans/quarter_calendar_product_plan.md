# Quarter forecast = calendar Q1–Q4: overview plan

**Status**: Not started. Plans rev 3, 2026-09-26, after three review rounds; the latest had nine independent
reviewers (six on individual plans, plus coordination, hydrology and codex over the whole set).
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
- **B. Quarterly ensembles are computed like monthly ensembles.** This replaces the 2026-06-23 fixed-LR EM
  (M1) and the 2026-09-25 "D10" rule, **for quarter only**.
  - **Naive Mean:** all models, no gate.
  - **Skilled Mean:** long-term gate (NSE > 0) plus min-pairs, 1/MAE-weighted.
  - **Ensemble Mean:** the monthly default gate.
- **C. Quarter min-pairs K = 10.**
- **D. Bounds for rows without quantiles** = forecast ∓ δ (the delta method, as for short-term), applied at
  display time.
- **E. Bulletin:** publish whatever is available until FD-030. No interim guard.
- **F. tjhm LR quarter rows holding postprocessing aggregates:** re-import the native hindcast values before
  the recalc.
- **G. Temporary LR fallback.** Until LTF-014 P0/P2 are deployed, LR quarters are derived from monthly
  forecasts where no native LR row exists. PP-065 P3 removes the fallback afterwards.
- **H. LTF-015:** refuse early runs that fall in a different calendar month than the scheduled issue date.

## What is wrong today

| Layer | Today | Plan |
|---|---|---|
| Schedule (data-repo model configs) | `forecast_months [3..9]` in both orgs: a rolling window is issued monthly Mar–Sep, and there is **no Q1** | LTF-014 |
| Postprocessing | The quarter label comes from the `valid_from` month only, and skill joins on the calendar quarter, so rolling windows are scored against another quarter (#521). Flag ON: a Dec-issued Q1 is trimmed out. Quarter rows come in three date populations | PP-064 |
| Quarter model set and ensembles | LR-only since 2026-06-23. The old monthly-derived path averages across issue dates and accepts 2 of 3 months | PP-065 |
| Monthly labels | tjhm hindcast windows are offset. The kghm GBT family labels January targets in the wrong year, which is a **live monthly skill bug** | LTF-016 |
| Dashboard quarterly card | The fetch window is set at import time. The latest issue wins whatever window it covers. The renderer keeps only rows at the max date. The caption relies on lead-1 arithmetic | FD-029 |
| Bulletin quarterly section | Uses the monthly norm, picks a model with `head(1)`, and shows a range only | FD-030 |
| Docs | Describe quarter as "rolling, not calendar". EM-parity and "0 deprecated rows" gates would false-fail. Cleanup predicates would delete product rows | DOC-009 |
| Early manual runs of issue-day-1 modes | The value is ratio-adjusted to the wrong month. This is live for the tjhm month modes | LTF-015 |

## The plans

| ID | File (`issues/`) | Priority / target |
|---|---|---|
| LTF-014 | `high_prio_gi_draft_ltf_quarter_calendar_schedule.md` | High. P0 is gated. Soft targets: tjhm Q4 2026-10-01 (recoverable until 2026-11-30), kghm Q1 2026-12-25 |
| PP-064 | `high_prio_gi_draft_pp_quarter_calendar_window_validation.md` | High. **A deployed by 2026-12-25** |
| PP-065 | `high_prio_gi_draft_pp_quarter_derived_models.md` | High. Four agent phases (P1a–P1d), then rollout. P3 removes the LR fallback |
| LTF-016 | `high_prio_gi_draft_ltf_monthly_window_labels.md` | High (live monthly bug). Independent of the quarter chain |
| FD-029 | `mid_prio_gi_draft_fd_quarter_card_calendar_window.md` | Medium. **Deployed and restarted by 2026-12-25** |
| DOC-009 | `mid_prio_gi_draft_doc_quarter_calendar_contract_amendments.md` | P1a (safety warnings) before PP-065 deploys; P1b after D4 |
| FD-030 | `mid_prio_gi_draft_fd_quarter_bulletin_norms_and_model.md` | Medium. Blocked on D6 |
| LTF-015 | `mid_prio_gi_draft_ltf_day1_early_run_refusal.md` | Medium |

## Open decisions

| # | Decision | Who | Blocks |
|---|---|---|---|
| D1 | Keep the 90-day offset target relabelled as the quarter (measured median bias ≤ 2 %, except kghm Q2 −1.7 %), or add the monthly-style ratio adjustment in quarter mode | Owner + modeller | Nothing |
| D2 | The hindcast write set for the new issue months (LTF-014 P2) | Modeller + owner | LTF-014 P2 → PP-065 P3 |
| D3 | PP-064 B5: empty quarter skill is handled as for monthly. The check shows monthly produces no ensembles on an empty skill frame, so there is no code change; confirm. B2 and B4 follow from decisions A and G and the 3-of-3 rule. B6 (the writer skips raw LR rows) moved into PP-065 as **required**, because otherwise persisted fallback LR rows would masquerade as native | Owner | PP-064 B → recalc |
| D4 | The service owner acknowledges the convention-doc amendment; the owner signs off the decision-request amendment | Service owner, owner | DOC-009 P1b only |
| D6 | Bulletin quarterly section: product published, presentation in months 2–3 of a quarter, norm reference year, and an optional point-value/model tag (a template change) | Owner | FD-030 |
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
2. **Who executes.** Each PR names who runs the server steps (the owner or hydromet IT). Image pull and
   restart per module follow `doc/prod/update_deployment_checklist.md`.
3. **Order.**
   1. DOC-009 P1a.
   2. PP-064 A and FD-029, both deployed (dashboard restarted) before 2026-12-25.
   3. PP-065.
   4. PP-064 B.
   5. The tjhm re-import (decision F).
   6. **One recalc per org** (PP-064 C together with PP-065 P2), after a DB export.
   7. LTF-014 P0 when its gate clears.
   8. LTF-014 P2, then a second recalc.
   9. PP-065 P3.
4. **Notice to the Kyrgyz and Tajik hydromets, before the recalc.**
   - Quarterly forecasts are issued four times a year: kghm has no more monthly Mar–Sep issues; tjhm gets
     new Jan and Oct issues.
   - Seven more models appear in the quarterly outputs, and the ensemble definitions now match monthly.
   - Bounds for models without quantiles are ±δ.
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
    "LTF-014.P0.tjhm": { "stage": "ops",    "depends_on": ["LTF-014 P0 gate"], "target": "2026-10-01, recoverable until 2026-11-30" },
    "LTF-014.P0.kghm": { "stage": "ops",    "depends_on": ["LTF-014 P0 gate"], "target": "2026-12-25" },
    "LTF-014.P1":      { "stage": "merge",  "depends_on": [], "parallel_agents": 1 },
    "LTF-014.P2":      { "stage": "ops",    "depends_on": ["LTF-014.P0.kghm", "LTF-014.P0.tjhm", "D2"] },
    "LTF-015":         { "stage": "merge",  "depends_on": ["LTF-014.P1"], "parallel_agents": 1 },
    "LTF-016.P0":      { "stage": "ops",    "depends_on": [] },
    "LTF-016.P1":      { "stage": "merge",  "depends_on": ["LTF-016.P0"], "parallel_agents": 1 },
    "PP-064.A":        { "stage": "merge",  "depends_on": ["DOC-009.P1a"], "parallel_agents": 1 },
    "PP-064.A.deploy": { "stage": "deploy", "depends_on": ["PP-064.A"], "deadline": "2026-12-25" },
    "FD-029":          { "stage": "merge",  "depends_on": [], "parallel_agents": 1 },
    "FD-029.deploy":   { "stage": "deploy", "depends_on": ["FD-029"], "deadline": "2026-12-25", "note": "restart the dashboard container" },
    "PP-065.P1a":      { "stage": "merge",  "depends_on": ["PP-064.A"], "parallel_agents": 1 },
    "PP-065.P1b":      { "stage": "merge",  "depends_on": ["PP-065.P1a"], "parallel_agents": 1 },
    "PP-065.P1c":      { "stage": "merge",  "depends_on": ["PP-065.P1a"], "parallel_agents": 1 },
    "PP-065.P1d":      { "stage": "merge",  "depends_on": ["PP-065.P1b", "PP-065.P1c"], "parallel_agents": 1 },
    "PP-064.B":        { "stage": "merge",  "depends_on": ["PP-065.P1d", "D3"], "parallel_agents": 1, "note": "after PP-065: same readers" },
    "deploy.pp":       { "stage": "deploy", "depends_on": ["PP-065.P1d", "PP-064.B", "DOC-009.P1a"] },
    "tjhm.reimport":   { "stage": "ops",    "depends_on": ["deploy.pp"], "note": "decision F, with the service owner" },
    "recalc.1":        { "stage": "ops",    "depends_on": ["deploy.pp", "tjhm.reimport"], "note": "PP-064 C + PP-065 P2, per org, after an export" },
    "recalc.2":        { "stage": "ops",    "depends_on": ["LTF-014.P2", "recalc.1"] },
    "PP-065.P3":       { "stage": "merge",  "depends_on": ["LTF-014.P2", "recalc.2"] },
    "FD-030":          { "stage": "merge",  "depends_on": ["FD-029", "D6"], "parallel_agents": 1 }
  }
}
```

**Shared files, strictly sequential:**
- `src/data_reader.py` quarter readers: PP-064 A → PP-065 P1b → PP-064 B.
- `apps/long_term_forecasting/tests/test_lt_utils.py` and `test_post_process_lt_forecast.py`: LTF-014 P1 →
  LTF-015.
- `doc/plans/module_issues.md`: DOC-009 and the index updates.

Everything else touches disjoint files and can run in parallel, including LTF-016 and FD-029.
