# Quarter forecast = calendar Q1–Q4: overview plan

**Status**: Not started (plans drafted 2026-09-25; rev 2 after out-of-loop review by two `codex exec`
passes and one fresh-context Claude reviewer)
**Owner decisions (2026-09-25)**:
- Kyrgyz Hydromet and Tajik Hydromet both issue the quarterly forecast for **calendar quarters Q1–Q4**,
  once per quarter. **Q1 is required.**
- GBT, LR_SM_DT, LR_SM_ROF, MC_ALD, SM_GBT, SM_GBT_LR and SM_GBT_NORM are **re-enabled for quarter**, as
  same-issue averages of their monthly forecasts for the quarter's three months. This reverses that part
  of the 2026-06-23 decision for quarter only; season stays LR-only.
**Trigger**: GitHub issue #521 (quarterly target-window matching). The bug it reports is real. This plan set
replaces the code approach of its branch `sandro_sapphire_2_quaterly_agg` (see § Relationship to #521).
**This file owns the dependency graph.** The child plans refer to it and do not repeat it.

## What is wrong today

| Layer | Implemented today | Required |
|---|---|---|
| Schedule (data-repo model configs) | `forecast_months: [3..9]` for quarter LR_Base/LR_SM in **both** orgs: a rolling 3-month window is issued monthly Mar–Sep, and **Q1 is never issued** | kghm (day 25, lead 1) `[3,6,9,12]`; tjhm (day 1, lead 0) `[1,4,7,10]` |
| Model target | A fixed 90-day mean shifted by `offset` (kghm 95, tjhm 90), **relabelled** as the quarter (e.g. kghm 25 Dec learns 31 Dec – 30 Mar). The same approximation already applies to today's Q2–Q4 | Owner/modeller decision D1 |
| Quarter model set | LR_Base/LR_SM only since 2026-06-23. The existing monthly-derived path averages across issue dates and 2-of-3 months | PP-065: seven models re-enabled as same-issue derived quarters; LR native-only |
| Postprocessing | The quarter label is the `valid_from` month only; skill joins on `[code, year, quarter_in_year]`. Rolling windows are scored against another quarter's observations (#521). With the flag ON, a December-issued Q1 is **trimmed out** of the operational reader. Three date populations of quarter rows exist (native, flag-OFF rewrites dated `valid_from`, persisted monthly-derived) | PP-064 |
| Dashboard quarterly card | The fetch window is keyed on issue date and frozen at import (misses 2027-dated Q1 rows on Dec 25–31). Latest-by-issue-date per model, whatever the window. The renderer keeps the max-date rows only. The caption trusts stale site attributes / lead-1 arithmetic | FD-029 |
| Bulletin quarterly section | Monthly norm used as the quarter norm; `head(1)` model pick | FD-030 (blocked on decisions) |
| Docs / data governance | The convention doc and MIG-008 say "7 rolling windows, not calendar quarters". The runbook cleanup signature "calendar hv1, `date` = quarter start" matches kghm flag-OFF rewrites and regenerated ensembles, and any date-equality-only variant would match **tjhm native rows** (hv0, issued on the quarter start) | DOC-009 |

**Works already:**
- The producer's window **label** handles the Dec 25 → next-year Jan–Mar rollover.
- Observations, hydrograph quarter norms and stored quarter skill are keyed by calendar quarter.
- The models are refitted on every run, so the schedule change needs no retraining (only new hindcasts).

## The plans

| ID | File | Scope | Priority / deadline |
|---|---|---|---|
| LTF-014 | `issues/high_prio_gi_draft_ltf_quarter_calendar_schedule.md` | P0 data-repo schedule (ops); P1 lock tests; P2 hindcasts for the new issue months | High. P0 is gated on the modeller (Sandro: why `[3..9]`, winter validity), the server state and owner approval. Soft targets: tjhm 2026-10-01 (recoverable until 2026-11-30), kghm 2026-12-25 |
| PP-064 | `issues/high_prio_gi_draft_pp_quarter_calendar_window_validation.md` | A: calendar-window validation, December Q1 through the operational reader (direct rows). B: direct-row dedup, observation coverage, empty-skill EM (decisions). C: recalc rollout | High. **A deployed by 2026-12-25** |
| PP-065 | `issues/high_prio_gi_draft_pp_quarter_derived_models.md` | Re-enable seven models for quarter as same-issue, unweighted monthly averages (null quantiles), incl. the operational December Q1 monthly read. LR native-only. Legacy direct rows of those models ignored on every input read. Quarter-only ensemble quantile rule in both ensemble paths | High. After PP-064 A |
| FD-029 | `issues/mid_prio_gi_draft_fd_quarter_card_calendar_window.md` | Year-safe fetch, per-target-quarter dedup, renderer, caption | Medium. **Deployed by 2026-12-25** |
| DOC-009 | `issues/mid_prio_gi_draft_doc_quarter_calendar_contract_amendments.md` | P1: contract amendments and cleanup warnings. P2: readmes | Medium. P1 merges before or with PP-064 A |
| FD-030 | `issues/mid_prio_gi_draft_fd_quarter_bulletin_norms_and_model.md` | Quarter norms/last-year value, model and period selection in the bulletin | Medium. Blocked on D6 |
| LTF-015 | `issues/low_prio_gi_draft_ltf_day1_lead0_early_run_window.md` | Early manual run of a day-1/lead-0 mode gets the previous month's window label | Low. Deferred (D7) |

## Target dates

- **tjhm Q4, issue 2026-10-01.** The current config skips it. After LTF-014 P0 it can be recovered with
  `lt_recovery` until 2026-11-30, so it does not justify bypassing P0's gate:
  - the modeller confirms why `[3..9]` and whether winter issues are valid;
  - the server config is read;
  - the owner approves.
- **2026-12-25, kghm Q1.** Needs LTF-014 P0 on kghm, PP-064 A (flag-ON trim), FD-029 (fetch window), and
  therefore DOC-009 P1. PP-065 should land by then too, so that the seven models' Q1 appears.

## Why quarter diverged (history, researched 2026-09-25)

- **The calendar intent was documented** in the 2026-02-06 postprocessing plan (Dec 25→Q1 … Sep 25→Q4,
  averaged from all 9 monthly models) and implemented in `aggregation.py` on 2026-03-04.
- **The long-term module built a rolling "next 3 months" quarter mode.** `forecast_months [3..9]` was set in
  the data-repo configs around 2026-02-21, with no recorded reason.
- **PR #295 (2026-03-27) merged the two** by relabelling rolling rows with the quarter of their first month.
  That is the #521 bug.
- **A 2026-06-21 investigation took the hindcast CSVs as the product specification** ("7 rolling windows,
  not calendar"). The 2026-06-22 service-owner answer spoke only to `horizon_value`, but was later cited as
  settling the product.
- **PP-065 restores the original design**, calendar quarters from monthly models, in a same-issue form.

## Decisions needed

| # | Decision | Who | Blocks |
|---|---|---|---|
| D1 | Keep the 90-day offset target relabelled as the quarter, or make the training target calendar-exact (`lt_forecasting` library change, incl. leap-year Q1) | Owner + modeller | Nothing here; declaring the product "exact" |
| D2 | Hindcast write set for the new issue months (new months only / CSV + filtered import / historical cutoff). The plain command overwrites operational rows of every configured month and flips them to flag 1 | Modeller + owner | LTF-014 P2 |
| D3 | PP-064 B2 (deterministic direct-row dedup rule, required), B3 (accept or delete persisted LR rewrites / old derived LR rows), B4 (3-of-3 observation months; unweighted recommended; if day-weighting is chosen it changes observations, PP-065's derived forecasts and preprocessing norms together), B5 (fixed-LR EM also when no quarter skill exists; changes a locked test) | Owner | PP-064 B, C |
| D4 | Service owner acknowledges the amendment to their RESOLUTION (hv rule unchanged; windows are calendar). Owner signs off the decision-request amendment | Service owner, owner | DOC-009 P1 merge |
| D5 | Dashboard caption issue date: accept "latest date before `valid_from`" (can pick a persisted derived row's date), or read the configured issue day in the dashboard | Owner | FD-029 detail only |
| D6 | Bulletin quarterly section: eligible quarter and issuance cutoff, published model and fallback, reopen semantics, in-progress quarter display, shared vs per-reservoir period, norm reference year for a December-issued Q1 | Owner | FD-030; DOC-009 #10 wording |
| D7 | Early-run fix: relabel from the scheduled date (recommended) or refuse | Owner | LTF-015 |
| D8 | Delete stale rows from `long_forecasts` (one-way SQL; no API path; colleague-owned service) | Owner + service owner | Nothing hard. Rolling-window rows are inert after PP-064 A and FD-029. **Calendar-shaped** rewrites and persisted derived rows are **not** inert: a window filter cannot tell them apart. They are handled by the PP-064 B2 dedup and the FD-029 selection rule until D8 deletes them (MIG-008 / PP-041) |
| D9 | **Decided 2026-09-25: yes.** Re-enable the seven models for quarter as same-issue monthly averages → PP-065 | Owner | — |
| D10 | **Decided 2026-09-25.** Quarterly EM includes the seven models. With more than 2 candidate models it is skill-gated with the **long-term gate** (NSE > 0, `_long_term_threshold_overrides`); with 2 or fewer it stays mean(LR_Base, LR_SM) (M1). Ensemble quantiles are null when any contributing member lacks quantiles. **Open assumption:** fewer than 2 qualifying → fall back to mean(LR_Base, LR_SM) | Owner | — (confirm the fallback in the PP-065 PR) |

## Relationship to #521

- **Kept**, re-implemented narrowly:
  - exact calendar-window validation before labelling (PP-064);
  - re-enabling the seven models as same-issue, complete-quarter monthly averages with null quantiles,
    with LR native-only (PP-065). This differs from the branch: the configured quarter lead and issue day
    only, and no date-derived lead.
- **Not kept**:
  - short-term-threshold EM gating (D10 uses the long-term NSE > 0 gate instead);
  - overwriting stored `horizon_value` with a date-derived lead;
  - flag-OFF behaviour and write-key changes;
  - its dashboard edits.
- Its "1,847 passed, 1 xfail" was reproduced. The xfail is the pre-existing `test_min_n_stale_integration.py:405`.
- **The #521 body names a real station code with its skill values on a public repo.** Ask the author to
  redact it to `19999`.

## Implemented drift noted while planning

- M1 says quarterly EM is not skill-gated. The operational run nevertheless skips all quarterly ensembles
  on an **entirely empty** skill frame (`postprocessing_operational_long_term.py:210`,
  `ensemble_calculator.py:632`). A test locks this (`tests/test_quarterly_ensemble_creation.py:329`).
  Decision D3/B5.

## Deferred findings (not filed)

- `apps/forecast_skill_eval` reads quarter `horizon_value` as the quarter number (`dashboard/app.py:385-386`,
  `cli.py:195`). It is an analysis tool; confirm before filing.
- `apps/forecast_dashboard/src/db.py:29-30` fixes `CURRENT_YEAR`/`PREVIOUS_YEAR` at import for every
  horizon. FD-029 fixes only the quarter fetch.
- `validate_pipeline` quarter check (`validate_pipeline.py:554-590`) FAILs on admitted-but-inactive days,
  in 8 of 12 months after LTF-014 instead of 5. This widens INFRA-022; no new issue.

## Dependency graph

Stages: **merge** = code/doc PR merged; **deploy** = on the servers; **ops** = manual step.

```json
{
  "phases": {
    "LTF-014.P0.tjhm": { "stage": "ops",    "depends_on": ["P0-gate: modeller + server state + owner"], "target": "2026-10-01, recoverable until 2026-11-30" },
    "LTF-014.P0.kghm": { "stage": "ops",    "depends_on": ["P0-gate: modeller + server state + owner"], "target": "2026-12-25" },
    "LTF-014.P1":      { "stage": "merge",  "depends_on": [], "parallel_agents": 1 },
    "LTF-014.P2":      { "stage": "ops",    "depends_on": ["LTF-014.P0.tjhm", "LTF-014.P0.kghm", "D2"] },
    "DOC-009.P1":      { "stage": "merge",  "depends_on": ["D4"], "parallel_agents": 1 },
    "DOC-009.P2":      { "stage": "merge",  "depends_on": [], "parallel_agents": 1 },
    "PP-064.A":        { "stage": "merge",  "depends_on": ["DOC-009.P1"], "parallel_agents": 1 },
    "PP-064.A.deploy": { "stage": "deploy", "depends_on": ["PP-064.A"], "deadline": "2026-12-25" },
    "PP-065":          { "stage": "merge",  "depends_on": ["PP-064.A"], "parallel_agents": 1 },
    "PP-065.deploy":   { "stage": "deploy", "depends_on": ["PP-065"] },
    "PP-064.B":        { "stage": "merge",  "depends_on": ["PP-065", "D3", "LTF-014.P0.tjhm"], "parallel_agents": 1, "note": "after PP-065: both edit the two quarter readers" },
    "PP-064.B.deploy": { "stage": "deploy", "depends_on": ["PP-064.B"] },
    "PP-064.C":        { "stage": "ops",    "depends_on": ["PP-064.A.deploy", "PP-065.deploy", "PP-064.B.deploy | D3-deferral-approved"], "note": "recalc; if B was deferred, a repeat recalc after PP-064.B.deploy is mandatory; rerun after LTF-014.P2 if Q1 history is wanted" },
    "FD-029":          { "stage": "merge",  "depends_on": [], "parallel_agents": 1 },
    "FD-029.deploy":   { "stage": "deploy", "depends_on": ["FD-029"], "deadline": "2026-12-25" },
    "FD-030":          { "stage": "merge",  "depends_on": ["FD-029", "D6"], "parallel_agents": 1 },
    "LTF-015":         { "stage": "merge",  "depends_on": ["D7"], "parallel_agents": 1 }
  }
}
```

LTF-014.P1, DOC-009, PP-064.A and FD-029 touch disjoint code files and can run in parallel. DOC-009 and
the index both edit `doc/plans/module_issues.md` (MIG-008 row vs the new rows), so merge those
sequentially.
