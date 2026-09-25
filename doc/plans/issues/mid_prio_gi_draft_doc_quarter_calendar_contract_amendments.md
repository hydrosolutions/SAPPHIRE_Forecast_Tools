# DOC-009: Amend the contract docs that describe quarter as a rolling, non-calendar product

**Status**: Draft (2026-09-25, rev 2 after out-of-loop review)
**Module**: docs
**Priority**: Medium.
- **P1 (contract + cleanup warnings) merges before or together with PP-064 Chunk A**, otherwise PP-064's
  reviewers will cite these docs against the fix.
- P2 (readmes) is independent.

**Labels**: `documentation`, `long-term`, `quarter`, `data-governance`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only.
**Related**: MIG-008, LTF-014, PP-064, FD-029

## Problem

Owner decision (2026-09-25): quarter = calendar Q1–Q4, issued once per quarter (kghm on the 25th of
Dec/Mar/Jun/Sep, lead 1, hv1; tjhm on the 1st of Jan/Apr/Jul/Oct, lead 0, hv0), and Q1 is required.
A read-only sweep on 2026-09-25 of `doc/`, `apps/`, `bin/`, `sapphire/` and `CLAUDE.md` classified 46
passages. The ones below contradict the decision or would lead an operator into a harmful cleanup.
`bin/`, `sapphire/` and `CLAUDE.md` contain no quarter-window statement.

**Still correct, not amended:**
- `horizon_value = operational_month_lead_time` for long forecasts (kghm 1, tjhm 0).
- Hydrograph quarter rows keyed by quarter number 1–4 (`doc/data_flow_long_term.md:248-251`).
- Already-calendar statements such as `apps/postprocessing_forecasts/src/aggregation.py:8` and
  `doc/plans/issues/high_prio_gi_draft_pp_lead_aware_skill.md:22-25`.

## Edit rules

- Append a dated **"Amendment 2026-09-25 (calendar-quarter decision)"** block, or add a short inline note
  next to the passage it corrects.
- **Never delete or reword decision text, historical records or counts.**
- Link to `doc/plans/quarter_calendar_product_plan.md`.
- No station codes, no discharge values.

## P1 — contract and cleanup safety

| # | File:line | Current text (abridged) | Edit |
|---|---|---|---|
| 1 | `doc/prod/longforecast_quarter_season_hv_convention.md:29-30, 40-41` | "3-month forecast issued monthly Mar-Sep (7 windows/yr) … Not 4 calendar quarters" | Amendment at the top of the file: the product is calendar Q1–Q4 with the schedule above; the Mar–Sep monthly issues were a config error (LTF-014). The model target is a fixed 90-day window relabelled as the quarter (LTF-014 § "Not fixed"). |
| 2 | same, RESOLUTION `:76-84` | "no date-derivation and no 4-calendar-quarter mapping; 'quarter' is a single quarterly product" | **Service owner's text: do not edit.** In the amendment state that "hv = config lead" and "no date-derivation" **still hold**; that "no 4-calendar-quarter mapping" holds for `horizon_value`; and that the **windows** are calendar quarters. **Send it to the service owner before merging** (overview decision D4). |
| 3 | MIG-008 `doc/plans/issues/mid_prio_gi_draft_migration_long_forecast_quarter_season_horizon_value.md:24-26, 38-40` | "the 'quarter is 7 rolling windows / should map to calendar quarters' reading was **wrong**" | Amendment: the calendar reading is correct for **windows**; the hv conclusion stands. Re-scope "what still needs adapting" (see list below) |
| 4 | `doc/plans/module_issues.md` MIG-008 row | "no date-derivation, no calendar-quarter mapping. Quarter is a single product" | Append "; windows = calendar Q1–Q4 (amended 2026-09-25, DOC-009)" |
| 5 | `doc/prod/longforecast_historical_data_decision_request.md:15-16, 34, 43-44, 52, 92-93` | Dataset B is "the exact mapping the convention rejected"; keep-set "QUARTER hv1, the Mar-Sep LR rows = the current Kyrgyz quarterly product" | Amendment: Dataset B's calendar **windows** are valid product windows; only its hv = quarter number is off-convention. In the keep-set only the 25 Mar/Jun/Sep issues are product; 25 Apr/May/Jul/Aug are not. The counts are from 2026-06-22 and must be re-measured. **Owner sign-off** (data governance) |
| 6 | `doc/prod/long_term_deploy_runbook.md:392-400` | cleanup (ii) deletes "old calendar-hv1" rows; the quarter raw delete is "typically a 0-row no-op" | Warning box: **do not run cleanup (ii) until it is re-scoped.** The signature "calendar `hv1`, `date == quarter start`" matches kghm flag-OFF rewrites **and regenerated ensembles** (writer: hv = config lead, `date = valid_from` under flag OFF, `apps/postprocessing_forecasts/src/api_writer.py:1172, 1199`). Any variant keyed on date equality alone (without `hv1`) also matches **tjhm native rows** (hv0, issued on the quarter start). Also correct the "0-row no-op" (the local DB holds ~37k deprecated-model QUARTER rows) |
| 7 | `doc/prod/ppipe_ensemble_hv_deploy_runbook.md:279-292` | names the archived P-PIPE reconciliation as authoritative; "old calendar `horizon_value=1` rows" | The same warning box as #6, placed next to the "authoritative" sentence at `:279` |
| 8 | `doc/plans/archive/ppipe_postprocessing_ensemble_hv_plan.md:140-150` (authoritative per #7) | quarter cleanup: "old calendar-`hv1` rows whose `date == quarter start`" | One-line warning right above item 1: "Superseded for quarter by DOC-009 (2026-09-25): the calendar-hv1 / date == quarter-start signature matches kghm regenerated rows, and a date-only variant matches tjhm native rows — do not run." Archive text otherwise untouched |
| 9 | `doc/prod/long_term_recovery_runbook.md:960-975` | the issue-day-1/lead-0 collision described as conditional | State plainly: for tjhm quarter the recovered issue date always equals `valid_from`, so the collision is certain (PP-061) |
| 10 | `doc/data_flow_long_term.md:240-242, 270-273` | "QUARTER rows use the same period keys as postprocessing `long_forecasts`"; join "must use period keys (code, horizon_type, horizon_value)" | Correct it: for long forecasts `horizon_value` is the **lead**, not the quarter number. **Year-specific actuals** (`previous`/`current`): join on `code` + calendar quarter from `valid_from` + target year. **Climatology** (`norm`): join on `code` + quarter; the reference snapshot for a December-issued Q1 is overview decision D6/FD-030 D3 — say so, do not decide it here |

**MIG-008 re-scope list (edit #3):**
- (a) Rolling-window rows are not product.
- (b) A Tajik quarter backfill keeps only calendar windows (Jan/Apr/Jul/Oct issues) before writing hv0.
- (c) Q1 hindcasts are missing (LTF-014 P2).
- (d) Three date populations exist for quarter rows (PP-064 § Mechanism 5). For tjhm the flag-OFF
  rewrites share the native key (PP-061).

## P2 — readmes (independent)

| # | File:line | Edit |
|---|---|---|
| 11 | `apps/long_term_forecasting/readme.md:41, 89-104, 193, 206-207` | Quarter targets calendar quarters. The issue months are set per model by `forecast_months` in `models_and_scalers/long_term_forecasting/quarter/*/*/general_config.json` (kghm `[3,6,9,12]`, tjhm `[1,4,7,10]`). `forecast_days` there is overwritten from the mode JSON. The target is a 90-day window shifted by `offset`, labelled as the quarter |
| 12 | `apps/postprocessing_forecasts/README.md:14, 17-18` | Window = calendar quarter. Quarterly raw models are LR_Base and LR_SM only (M1), not "all 9 models". There are direct (native) and monthly-derived sources, with direct precedence; the derived source's future is pending PP-064 Chunk B. Stored `date` has three populations (native issue date; `valid_from` for flag-OFF rewrites; `valid_from − monthly lead` for flag-ON derived rows) |

**Not edited** (dated observations or out of scope):
- `doc/plans/postprocessing_unified_plan.md:469`
- `doc/dev/review_checklist_server_2026-06-24_kyg_lt_deploy.md:442-443`
- the other archive files (`longforecast_hv_convention_plan.md`,
  `issues/archive/high_prio_gi_draft_ltf_monthly_horizon_value_semantics.md`)
- `apps/forecast_skill_eval` (overview, deferred findings)

## Agent constraints

**Files**: exactly those in the tables, plus `doc/plans/module_issues.md` (MIG-008 row only). No code,
no tests, nothing under `sapphire/services/`.

**Instruction**: *"Do NOT change any existing function signatures, data flow logic, or control flow.
Your changes must be purely additive or modify only the specific behavior described."* For docs, this
means: append amendments, warnings and notes; do not delete or reword existing text, tables or counts.

## Acceptance criteria

- **Per-claim check.** For each row, the reviewer confirms that the specific passage cited is now
  qualified by an adjacent note or by an amendment that names it. "An amendment exists somewhere in the
  file" is not enough.
- `git diff` shows only additions, apart from the MIG-008 index row's appended clause.
- **Sweep, run per directory** (a `grep -r` from the repo root under-reports):
  ```bash
  for d in doc/prod doc/plans doc apps/long_term_forecasting apps/postprocessing_forecasts; do
    grep -rn -i -E "7 windows|rolling (monthly|3-month|Mar-Sep)|not 4 calendar|calendar-quarter mapping|Mar-Sep|calendar-.?hv1" --include='*.md' "$d"
  done
  ```
  Every hit is either qualified per the rule above or on the "not edited" list.
- The service owner has acknowledged #2, and the owner has signed off #5 (linked in the PR).
