# DOC-009: Amend the contract docs that describe quarter as a rolling, non-calendar product

**Status**: Draft (2026-09-26, rev 4 after the second review round)
**Module**: docs
**Priority**: Medium.
- **P1a (safety warnings)** needs no service-owner acknowledgement. It merges **before PP-064 Chunk A
  and before PP-065 deploys**; after PP-065 the warned-about gates false-fail and the warned-about
  deletes remove product rows.
- **P1b (contract amendments)** needs overview decision D4 (service owner) and the owner's sign-off. It
  does not gate code; until it merges, PP-064/PP-065 reviewers use the overview decisions, not rows 1–5.
- **P2 (readmes, data-flow doc)** merges after LTF-014 P0 (both orgs) and PP-065 P1d, per the overview
  graph. The P2 table repeats this per row.

**Labels**: `documentation`, `long-term`, `quarter`, `data-governance`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only.
**Related**: MIG-008, PP-056, PP-058, PP-059, LTF-014, LTF-016, PP-064, PP-065, FD-029, FD-030

## Problem

Owner decisions (2026-09-25; A–H and round 2 of 2026-09-26):
- quarter = calendar Q1–Q4, issued once per quarter: kghm on the 25th of Dec/Mar/Jun/Sep, lead 1, hv1;
  tjhm on the 1st of Jan/Apr/Jul/Oct, lead 0, hv0. Q1 is required.
- The seven models GBT, LR_SM_DT, LR_SM_ROF, MC_ALD, SM_GBT, SM_GBT_LR, SM_GBT_NORM are re-enabled for
  quarter as same-issue averages of their monthly forecasts (PP-065).
- **Derived quarter (decision A)**: for (station, model, Q of year Y) use the monthly forecasts issued on
  the quarter issue date at monthly leads `L`, `L+1`, `L+2`. Months are identified by (issue date,
  `horizon_value`), not by `valid_from`. Mislabelled stored MONTH windows still count; LTF-016 fixes the
  labels upstream.
- **Quarter ensembles (round-2 decision 1)**, referred to below as **Q-ENS**:
  - Naive Mean = mean of all nine quarter models, no skill gate;
  - Skilled Mean = long-term gate (NSE > 0, `_long_term_threshold_overrides()`,
    `apps/postprocessing_forecasts/src/skill_metrics.py:168`; monthly pool at
    `src/ensemble_calculator.py:305-310`) + quarter min-pairs K = 10, 1/MAE-weighted;
  - **no quarterly Ensemble Mean** from PP-065 on (operational and recalc); the recalc tombstones quarter
    EM skill rows. Old persisted quarter EM / Naive Mean / Skilled Mean forecast rows stay in
    `long_forecasts` (round-2 decision 2; deleting them is D8 / PP-041).
  - Season is unchanged: `EM = mean(LR_Base, LR_SM)`.
- Rows without Q25/Q75 show forecast ∓ δ at display time (decision D). Until LTF-014 P0 and P2 are
  deployed, LR_Base/LR_SM quarters missing a native row are derived from monthly (decision G); those
  fallback LR rows are not persisted (round-2 decision 3).

A read-only sweep of `doc/`, `apps/`, `bin/`, `sapphire/` and `CLAUDE.md` (2026-09-25, extended
2026-09-26 with model-set, EM, K and delete patterns) found the passages below. They contradict the
decisions, or they would make an operator run a harmful cleanup or a gate that false-fails.
`bin/`, `sapphire/` and `CLAUDE.md` contain no such statement.

**Still correct, not amended:**
- `horizon_value = operational_month_lead_time` for long forecasts (kghm 1, tjhm 0).
- Hydrograph quarter rows keyed by quarter number 1–4 (`doc/data_flow_long_term.md:248-251`).
- Already-calendar statements: `apps/postprocessing_forecasts/src/aggregation.py:7`,
  `doc/plans/issues/high_prio_gi_draft_pp_lead_aware_skill.md:22-25`.
- Season: LR-only, `EM = mean(LR_Base, LR_SM)`, unchanged.

## Edit rules

- **Decision records** (the convention doc, MIG-008, the decision request, the PP-056/PP-058/PP-059
  plans, archived plans, `module_issues.md` rows): **add an adjacent note that …**. Never delete or
  reword the decision text, historical records or counts. Head each note **"Amendment 2026-09-26
  (calendar-quarter decision)"**.
- **Runbooks, checklists and `doc/configuration.md`** (P1a): add a warning box next to the cited
  passage. A one-line suffix on a table row is allowed.
- **READMEs and `doc/data_flow_long_term.md`** (P2): rewrite the passage, so the doc does not
  contradict itself.
- Link to `doc/plans/quarter_calendar_product_plan.md`. No station codes, no discharge values.
- **Index rows** in `doc/plans/module_issues.md`: find each by `grep -n '^| \*\*<ID>\*\*'`, never by line
  number.

## P1a — safety warnings (no D4 needed)

"After PP-065" below means: from the PP-065 deploy on.

| # | File:line | Current text (abridged) | Edit |
|---|---|---|---|
| 6 | `doc/prod/long_term_deploy_runbook.md:377-400` | Phase 5: cleanup (i) deletes the seven "deprecated" models from QUARTER and SEASON; cleanup (ii) deletes "old calendar-hv1" rows; the quarter raw delete is "typically a 0-row no-op" | Warning box above (i) at `:388`. **(i) must be restricted to SEASON.** The seven models are re-enabled for QUARTER (PP-065) and their derived rows are persisted, so a QUARTER delete removes product rows and their skill. **Do not run cleanup (ii) until it is re-scoped.** The signature "calendar `hv1`, `date == quarter start`" matches kghm flag-OFF derived rows **and regenerated Naive/Skilled Mean rows**: the writer sets hv = config lead and `date = valid_from` under flag OFF (`apps/postprocessing_forecasts/src/api_writer.py:1175, 1199`). Any variant keyed on date equality alone (without `hv1`) also matches **tjhm native rows** (hv0, issued on the quarter start). The "0-row no-op" is wrong: the local DB held ~37k deprecated-model QUARTER rows on 2026-09-25 |
| 6a | same, Phase 4 item 1 `:339-349` | "`MAX(date) ≥ <current operational issue date>` (latest issue date from the deployment's most recent successful operational long-term run)" | Warning box at `:339`: after LTF-014 P0, quarter is issued once per quarter while the month modes run monthly, so in the 2nd and 3rd month of a quarter the QUARTER bucket false-fails this gate. For QUARTER, compare with the latest **quarter** issue date (the last configured quarter issue day on or before today); other buckets unchanged |
| 6b | `doc/prod/ppipe_ensemble_hv_deploy_runbook.md:231-237` | the same per-bucket `MAX(date) >= <current operational issue date>` gate | The same warning box as #6a, at `:231` |
| 7 | `doc/prod/ppipe_ensemble_hv_deploy_runbook.md:279-293` | names the archived P-PIPE reconciliation as authoritative; "old calendar `horizon_value=1` rows" | The same warning box as #6, next to the "authoritative" sentence at `:279` |
| 8 | `doc/plans/archive/ppipe_postprocessing_ensemble_hv_plan.md:140-147` (authoritative per #7) | quarter cleanup: "old calendar-`hv1` rows whose `date == quarter start`" | One-line note above item 1: "Superseded for quarter by DOC-009 (2026-09-26): the calendar-hv1 / date == quarter-start signature matches kghm regenerated rows, and a date-only variant matches tjhm native rows — do not run." |
| 8a | `doc/plans/archive/two_model_ensemble_plan.md` (whole file) | quarter/season EM = mean(LR_Base, LR_SM); seven models dropped for QUARTER | **Top-of-file banner** (not only §4): "Superseded for QUARTER on 2026-09-26 (PP-065): the seven models are re-enabled for quarter as same-issue monthly averages; quarter ensembles are Naive Mean + Skilled Mean, and there is no quarter EM. Model drops, deletes and EM = mean(LR) checks here apply to SEASON only." The banner lists the affected passages: `:29-33`, `:40-42`, §4 `:154-193`, §5 `:228-231`, §6 `:247-250`, P5 `:386-394`, Final Acceptance `:413-420` |
| 9 | `doc/prod/long_term_recovery_runbook.md:960-980` | the issue-day-1/lead-0 collision is described as conditional | Add a note: for tjhm quarter (issue day 1, lead 0) the recovered issue date always equals `valid_from`, so the collision is certain (PP-061) **until PP-065 deploys**. After PP-065 the postprocessing writer no longer writes raw LR quarter rows, so recovered LR rows are not rewritten |
| 13 | `doc/prod/long_term_deploy_runbook.md:20` | change (b): "quarter/season `EM = mean(LR_Base, LR_SM)` … reader drops 7 deprecated quarter models" | Note under the table: for QUARTER, (b) is superseded by PP-065 (the seven models are read again; quarter has no EM since PP-065; Q-ENS). (b) still holds for SEASON |
| 14 | same, phase overview `:144-149` and Phase 4 item 2 `:350-355` | "`EM = mean(LR_BASE, LR_SM)` join check returns `em_pairs > 0 AND mismatch = 0`" | Warning box at `:350`: quarter has no EM since PP-065; restrict the EM check to SEASON. Old quarter EM rows may still join and are not evidence of a stale image. Quarter ensembles are verified per PP-065 § P2 |
| 15 | same, Phase 6 `:406-412` | "The 7 deprecated models return **0 rows** … for `horizon_type IN ('QUARTER','SEASON')`" | Warning box: SEASON only. For QUARTER, the seven models **must** have rows after PP-065 |
| 16 | `doc/prod/ppipe_ensemble_hv_deploy_runbook.md:239-272` | EM parity SQL over `IN ('QUARTER', 'SEASON')`; the `em_pairs > 0 AND mismatch = 0` gate | Warning box at `:239`: quarter has no EM since PP-065; apply the SQL and the gate to SEASON only |
| 17 | `doc/dev/review_checklist_local_template.md` §9.6.4 `:1754-1800`, §9.6.5 `:1802-1841`, summary rows `:2092`, `:2095`, troubleshooting row `:2127` | §9.6.4 loops `EM` over `month quarter season`; §9.6.5 "For quarter and season, `EM` is defined as the plain mean of `LR_Base` and `LR_SM`"; `:2092` "LT EM present, all 3 horizons" | Warning below each heading: quarter has no EM since PP-065; restrict EM checks to SEASON (§9.6.4: expect no new quarter EM row; §9.6.5: `for H in season`). Suffix on `:2092`, `:2095`, `:2127`: "(quarter N/A after PP-065, DOC-009)" |
| 17a | same, §9.6.2 `:1673-1720`; `:241`; `:2125` | §9.6.2 "defaults MONTH=4, QUARTER=5, SEASON=5" and `K=5` for quarter in both loops (`:1683`, `:1703`); "defaults 4/5/5" | Warning below the §9.6.2 heading: after PP-065 the quarter default is K = 10 (decision C); run the quarter loop with `K=10` (or the value in effect), otherwise rows with 5 ≤ n_pairs < 10 carrying metrics pass unnoticed. Suffix on `:241` and `:2125`: "(4/10/5 after PP-065)" |
| 17b | `doc/configuration.md:259`, `:336`, `:357` | quarter min-pairs default `5` (twice); "EM \| quarter / season \| no skill gate — fixed `LR_Base` + `LR_SM` aggregate" | Suffix on `:259` and `:336`: "(10 from PP-065 on; an explicit 5 in a server `.env` cancels it)". Suffix on `:357`: "(season only from PP-065 on; quarter has no EM)" |
| 18 | `doc/prod/update_deployment_checklist.md:1511` | "Phase 4 queries `long_forecasts` (… `EM = mean(LR_BASE, LR_SM)` composition)" | Add a note: quarter has no EM since PP-065; the EM composition check is SEASON-only (see row 14) |
| 19 | `doc/prod/update_data_migration_runbook.md:611-735` (§5.5) | "Full population (all configured modes, all models)" | Warning box after the skip rules (`:632-633`): run `bin/initialize_long_forecast_history.sh` with `--skip-mode quarter` (flag verified: `bin/initialize_long_forecast_history.sh:74, 268-273`) until LTF-014 P2 / MIG-008 (b), because the quarter hindcast CSVs still hold rolling windows. Exception: the reviewed decision-F re-import in PP-064 Chunk C. Also: raw tjhm hindcast MONTH CSVs carry offset windows (LTF-016 defect 1); do not import them on tjhm until LTF-016 has regenerated them |
| 20 | `doc/prod/longforecast_historical_data_decision_request.md:55, 71` | "`QUARTER hv0` … is the correct home for the Tajik quarterly backfill"; "Holding … the Tajik `QUARTER hv0` backfill" | Add a note: the backfill may write only calendar windows (Jan/Apr/Jul/Oct issues), and only after LTF-014 P2 / MIG-008 (b) |
| 21 | same, `:46` and DECISION `:84-86` | "current 2-model config … will never regenerate these rows"; Dataset B: "delete the deprecated models" | Add a note that D8 stands for the **legacy population only**. PP-065 writes quarter rows for the seven models again, so a predicate on `model_type` alone now also matches freshly derived rows. Under flag OFF, kghm derived Q1 rows share the Dataset B hv1-January natural key |
| 22 | `doc/plans/archive/longforecast_hv_convention_plan.md` (top of file) | P3 predicates, still cited as the ones to use by `doc/prod/long_term_deploy_runbook.md:400, 435` and the decision request `:95` | Top-of-file banner: "P3 predicates are superseded for QUARTER (DOC-009, 2026-09-26). The deprecated-model delete matches PP-065 derived rows, and the calendar-hv1 signature matches regenerated rows. Re-derive before any delete (D8)." |
| 23 | `doc/plans/issues/high_prio_gi_draft_pp_quarter_skill_lead_mismatch.md:139-144, 150-154, 160-161` + its PP-056 index row | open question "one of the two populations is wrong"; acceptance "skill rows land at 1..4 and not at 0"; "Do not 'fix' this by dropping the `hv=0` rows" | Add a note: quarter hv = config lead (convention RESOLUTION). PP-065 supersedes PP-056 for the seven models. The kghm hv0 quarter skill rows of those models are **meant** to be replaced by hv1 rows and tombstoned by the recalc (PP-065 § P2). Suffix on the index row: "; superseded for the seven models by PP-065 (DOC-009)" |
| 24 | PP-059 `doc/plans/issues/mid_prio_gi_draft_pp_remove_monthly_em.md:23`, `:140-141`, `:159-160`; PP-058 `doc/plans/issues/low_prio_gi_draft_pp_vestigial_long_term_em.md:115-117`; their PP-058 and PP-059 index rows | PP-059 KEEP row "EM \| quarter / season \| fixed `LR_Base` + `LR_SM` aggregate"; acceptance "Quarter and season EM unchanged"; "Do not touch quarter/season EM"; PP-058 "Quarter/season EM is a different thing again"; both index rows "quarter/season EM … untouched / separate" | Add a note next to each passage: "Superseded for QUARTER by PP-065 (owner, 2026-09-26): quarter has no EM; quarter ensembles are Naive Mean + Skilled Mean. Still holds for SEASON." Suffix on each index row: "; quarter EM removed by PP-065 (DOC-009)" |

## P1b — contract amendments (need D4 + owner sign-off)

| # | File:line | Current text (abridged) | Edit |
|---|---|---|---|
| 1 | `doc/prod/longforecast_quarter_season_hv_convention.md:29-30, 40-41` | "3-month forecast issued monthly Mar-Sep (7 windows/yr) … Not 4 calendar quarters" | Add a note next to `:29-30` and `:40-41`. The product is calendar Q1–Q4 on the schedule above; the Mar–Sep monthly issues were a config error (LTF-014). The model target is a fixed 90-day window labelled as the quarter: ≈ the calendar quarter; the median bias of the 90-day target is ≤ 2% (kghm Q2 −1.7%). The seven models' quarters are derived from monthly forecasts per decision A |
| 2 | same, RESOLUTION `:76-84` | "no date-derivation and no 4-calendar-quarter mapping; 'quarter' is a single quarterly product" | **Service owner's text: do not edit.** Add a note after `:84`: "hv = config lead" and "no date-derivation" **still hold**; "no 4-calendar-quarter mapping" holds for `horizon_value`; the **windows** are calendar quarters. **Send it to the service owner before merging** (D4) |
| 3 | MIG-008 `doc/plans/issues/mid_prio_gi_draft_migration_long_forecast_quarter_season_horizon_value.md:24-26, 38-40` | "the 'quarter is 7 rolling windows / should map to calendar quarters' reading was **wrong**" | Add a note after `:38-40`: the calendar reading is correct for **windows**; the hv conclusion stands. The note carries the "still needs adapting" list below |
| 4 | the MIG-008 index row in `doc/plans/module_issues.md` | "no date-derivation, no calendar-quarter mapping. Quarter is a single product" | Suffix: "; windows = calendar Q1–Q4 (amended 2026-09-26, DOC-009)" |
| 5 | `doc/prod/longforecast_historical_data_decision_request.md:15-16, 34, 43-44, 52, 92-93` | Dataset B is "the exact mapping the convention rejected"; keep-set "QUARTER hv1, the Mar-Sep LR rows = the current Kyrgyz quarterly product" | Add adjacent notes. Dataset B's calendar **windows** are valid product windows; only its hv = quarter number is off-convention. In the keep-set, only the 25 Mar/Jun/Sep issues are product; 25 Apr/May/Jul/Aug are not. The counts are from 2026-06-22 and must be re-measured. **Owner sign-off** (data governance) |

**MIG-008 "still needs adapting" list (row 3):**
- (a) Rolling-window rows are not product.
- (b) A Tajik quarter backfill keeps only calendar windows (Jan/Apr/Jul/Oct issues) before writing hv0.
- (c) Q1 hindcasts are missing (LTF-014 P2).
- (d) Three date populations exist for quarter rows (PP-064 § Mechanism and problems, item 5). Only the
  flag-OFF rewrites and the hv0 legacy rows share the native key; tjhm legacy rows at hv ≥ 1 are
  dropped by selection.
- (e) Stored MONTH windows can be offset or mislabelled (tjhm hindcast rows; the kghm GBT-family
  January year). LTF-016 fixes them.

## P2 — readmes and data-flow doc (rewrites allowed)

| # | File:line | Depends on | Edit |
|---|---|---|---|
| 10 | `doc/data_flow_long_term.md:240-242, 270-275` | after LTF-014 P0 and PP-065 (overview) | For long forecasts `horizon_value` is the **lead**, not the quarter number. **Year-specific actuals** (`previous`/`current`): join on `code` + calendar quarter from `valid_from` + target year. **Climatology** (`norm`): join on `code` + quarter; the reference snapshot for a December-issued Q1 is overview D6 (FD-030's norm-reference-year decision). Say so; do not decide it here |
| 11 | `apps/long_term_forecasting/readme.md:41, 89-103, 193, 206-207` | after LTF-014 P0; currently `[3..9]` | Quarter targets calendar quarters. The issue months are set per model by `forecast_months` in the data-repo `models_and_scalers/long_term_forecasting/quarter/*/*/general_config.json` (kghm `[3,6,9,12]`, tjhm `[1,4,7,10]`). `forecast_days` there is overwritten from the mode JSON (`apps/long_term_forecasting/config_forecast.py:157-179`). The target is a 90-day window shifted by `offset`, labelled as the quarter |
| 12 | `apps/postprocessing_forecasts/README.md:14, 18-19, 305-320`; `doc/data_flow_long_term.md:259-262` | after PP-065 | Window = calendar quarter. Quarterly raw models: LR_Base and LR_SM from their native quarter mode, plus the seven other models as same-issue, unweighted averages of their monthly forecasts at leads L..L+2 (decision A). While decision G's fallback is active, LR quarters without a native row are derived from monthly but **not persisted**. Derived quarters need 3 of 3 months; quarterly observations need 3 of 3 months too (PP-065 P1a, the former PP-064 B4). Replace the `:259-262` "2-of-3-month tolerance" contrast with what trunk does at the time of writing. Derived rows have null quantiles; bounds are forecast ∓ δ at display time (decision D; `apps/forecast_dashboard/src/processing.py:1244`). Ensembles: Naive Mean + Skilled Mean, no quarter EM (Q-ENS). Season: LR_Base/LR_SM only, EM = mean(LR). Stored `date` of quarter rows: LR rows are no longer rewritten by postprocessing, so they keep the native issue date; under flag OFF, derived and ensemble rows are dated `valid_from`. Legacy rows of other shapes may exist (PP-064 § Mechanism and problems, item 5) |

**Not edited** (dated observations, historical records, or out of scope):
- `doc/plans/postprocessing_unified_plan.md:469`
- `doc/dev/review_checklist_server_2026-06-24_kyg_lt_deploy.md:442-443` (gitignored, local only)
- `doc/plans/issues/archive/high_prio_gi_draft_ltf_monthly_horizon_value_semantics.md`
- `doc/prod/ppipe_ensemble_hv_deploy_runbook.md:15-45` (the 2026-06-23 PR #383 update record)
- `doc/plans/working/*` (dated working notes)
- `apps/forecast_skill_eval` (overview, deferred findings)

## Agent constraints

**Files**: exactly those in the three tables, plus `doc/plans/module_issues.md` (the MIG-008, PP-056,
PP-058 and PP-059 index rows only). No code, no tests, nothing under `sapphire/services/`.

**Instruction**: *"Do NOT change any existing function signatures, data flow logic, or control flow.
Your changes must be purely additive or modify only the specific behavior described."* For docs, this
means the edit rules above: adjacent notes and warnings, except the P2 rewrites.

## Acceptance criteria

- **Per-claim check.** For each row, the reviewer confirms that the specific passage cited is now
  qualified by an adjacent note or banner that names it. "An amendment exists somewhere in the file" is
  not enough.
- **Additions only for decision records, runbooks and `doc/configuration.md`**: `git diff --numstat`
  shows ≤ 1 deletion per P1a/P1b file, ≤ 3 for `doc/configuration.md` and
  `doc/dev/review_checklist_local_template.md` (table-row suffixes), ≤ 4 for `module_issues.md`. P2
  files are exempt.
- **Inventory table** in the PR: one row per hit of the sweep below, each with a verdict: *qualified by
  row N*, *not edited (list above)*, *archived/historical record* (any hit under
  `doc/plans/issues/archive/` or `doc/plans/archive/` not already qualified by rows 8, 8a or 22),
  *season-only / short-term / unrelated*, or *allowlisted*.
  Allowlisted: the plan-set files (the overview, PP-064, PP-065, LTF-014, LTF-015, LTF-016, FD-029,
  FD-030, this file) and their `module_issues.md` rows.
  ```bash
  P="7 windows|rolling (monthly|3-month|Mar-Sep)|not 4 calendar|calendar-quarter mapping|Mar-Sep|calendar-.?hv1|deprecated (quarter|model)|7 deprecated|seven (deprecated|models)|mean\(LR|LR-only|\bEM = |two-model|NOT IN \(|period keys|aggregated from monthly|all 9 models|min_pairs_long_term_quarter|K=5|4/5/5|LR_Base.{0,3}\+.{0,3}LR_SM"
  for d in doc apps bin; do   # per directory: grep -r from the repo root under-reports
    grep -rn -i -E "$P" --include='*.md' --include='*.sh' "$d"
  done
  ```
- The service owner has acknowledged #2 and the owner has signed off #5 before P1b merges (both linked
  in the PR).
