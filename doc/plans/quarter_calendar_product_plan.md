# Quarter forecast = calendar Q1–Q4: overview plan

**Status**: In progress. Plans rev 7, 2026-09-28, after five review rounds plus confirm passes and a round-4 owner-decision pass (decisions R4-native-lr-precedence, R4-merge-is-deploy, R4-integration-branch, R4-recalc-runs). **Code status 2026-09-28**: PP-064.A (#527), FD-029 P1 (#528) and PP-065.P1a (#530) are **merged to trunk** and **presumed live on both servers** (owner decision R4-merge-is-deploy — merge is deploy; verify per org, `PP-064.C.step0`). PP-065 P1b–P1d and PP-064 B are **held on the integration branch `integ_quarter_p1b_p2`** (owner decision R4-integration-branch) and merge to trunk only in the P2 window, which is the postprocessing deploy. The automatic bimonthly quarterly recalc is allowed to run in the meantime (owner decision R4-recalc-runs). Remaining: PP-065 P1b–P1d, PP-064 B/C, FD-030, DOC-009, LTF-014/015/016/017.
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
  - Deferred with P0: P0b (moot; expires 2026-11-30), LTF-014 P1 and P2, PP-065 P3, and DOC-009 P2b (row 11).
- **Winter issues are valid for LR_Base/LR_SM** (owner).
- **D1:** accept the offset target.
- **D2:** the P2 procedure is approved.

**2026-09-26, round 2**
1. No quarterly EM (see B).
2. **Old persisted ensemble rows are accepted for now.** This covers the fixed-LR EM and the old Naive and
   Skilled Mean rows. They stay in `long_forecasts` (D8 / PP-041).
3. **Fallback LR rows are not persisted and are accepted as invisible.** The dashboard card and the bulletin
   show no LR row for fallback quarters until LTF-014 P0/P2 (tjhm exception until PP-065 P1b + decision F:
   see the round-4 tjhm interim decision).
4. **Decision F is by provenance.** Remove tjhm LR QUARTER rows with `date = valid_from` that have no
   counterpart in the LT module CSV, in any quarter, 2026 included.
   - Option (a), the standalone clear-and-recover step LTF-014 P0b, is **moot** while P0 is deferred.
     F's provenance predicate therefore **again covers the tjhm rows dated 2026-10-01**, and any
     trunk-written 2027-01-01 rows, in any year. If P0b ever runs, F preserves its recovered rows.
   - Take a preserve manifest, a dry run and a backup first.
5. **PP-065 is on the 2026-12-25 critical path.** Its fallback guarantees a kghm Q1 even without LTF-014 P0.
6. **Early kghm runs** on the 20th–24th are accepted but produce no quarter product. They log a WARNING.

**2026-09-26, round 4**
1. **tjhm interim: accept and document.** Until PP-065 P1b (the writer stops writing raw LR rows) and the
   decision-F cleanup land, the monthly-derived LR_Base/LR_SM quarter rows that postprocessing still
   writes on tjhm (lead 0, issue day 1) carry the native date (flag OFF: `valid_from`; flag ON:
   `valid_from` minus 0 months), so the dashboard card and the bulletin show them as native LR. kghm is
   unaffected — its lead of 1 dates those same rows differently, so FD-029's native-only filter already
   hides them there. No code change; goes into the hydromet notice (§ Rollout and communication).
2. **[MOOT since 2026-09-28, kept for history only.]** This bullet originally warned that deploying FD-029
   before PP-064 A on a flag-ON org would show missing quarter ensembles (FD-029 hides rolling-window
   rows; PP-064 A is what makes the writer emit calendar-quarter-shaped ones). Both are presumed live via
   auto-pull since 2026-09-27/28, under decision R4-merge-is-deploy (merge = deploy) — this is a presumption, not a
   verified fact: the relative deploy order between PP-064 A and FD-029 P1, and any gap window on a
   flag-ON org, are unverified (see `PP-064.C.step0`). Separately, calendar-quarter-shaped ensemble rows
   only exist once the next quarterly postprocessing run (operational or recalc) has produced them — the
   image pull alone does not write them — so a flag-ON org shows no quarter ensembles for a given quarter
   until that run has happened, regardless of the PP-064 A / FD-029 deploy order.

**2026-09-28**
1. **[SUPERSEDED same day by round 4's decisions 5-7, below — kept for history only.]** The first-pass rule
   was "do NOT deploy trunk before PP-065 P2 is ready" (block any server pull of a trunk-built
   postprocessing/dashboard image until P2). It was narrowed the same day once it was confirmed that PP-064
   A and PP-065 P1a were already live on servers via Luigi's auto-pull; round 4 then established (decision
   5) that FD-029 P1 was independently already live too, via the dashboard's own daily auto-pull — so no
   merged PR can in fact be held back from servers this way. Round 4 (decisions 5-7) replaces this gate
   entirely: see below.
2. **PP-065 P1b replaces "Source 1".** In both quarter readers (`read_quarterly_forecasts`,
   `read_latest_quarterly_forecasts`, `apps/postprocessing_forecasts/src/data_reader.py:3105` and `:3453`),
   the old mixed-issue, 2-of-3 `aggregate_monthly_fc_to_quarterly` path is replaced with
   `derive_quarterly_from_monthly_same_issue`: `QUARTERLY_DERIVED_MODELS` always,
   `QUARTER_NATIVE_RAW_MODELS` only as the decision-G fallback (2026-09-26; the temporary LR fallback —
   see decision R4-native-lr-precedence below for direct-row precedence over that fallback). The season path is unchanged.
   `derive_quarterly_from_monthly_same_issue` already exists on trunk since P1a (#530,
   `src/aggregation.py:786`, unit-tested), but neither reader calls it yet — both still call the old
   `aggregate_monthly_fc_to_quarterly` (`src/data_reader.py:3145, 3499, 3508`). Wiring the helper into the
   readers is P1b's job.
3. **The missing-quarter-config split is intended.** FD-031: the dashboard degrades. PP-065 P1b: the
   postprocessing derivation/read FAILS (propagates `FileNotFoundError`), unlike PP-064's
   `_quarter_native_q1_issue_date` (`src/data_reader.py:3045-3083`), which warns and continues on the
   same `(LongTermHorizonResolverError, FileNotFoundError)` exception (Problem-7 exception, warn-and-continue).
   That warn-and-continue branch is not the whole flag-OFF story: on a missing `quarter.json`,
   postprocessing already fails today regardless. Under flag OFF, `quarter_horizon_value()`
   (`src/data_reader.py:3198`) raises `FileNotFoundError` before the helper's only call site (`:3251`)
   is ever reached, so a fully-missing config already propagates uncaught, upstream of the helper. The
   helper's own `operational_schedule_for_mode("quarter")` lookup (`:3073`) loads the same file and
   re-requires the same `operational_month_lead_time` field `quarter_horizon_value()` already
   validated, so by the time the helper runs the only new failure it can hit is a missing/non-integer
   `operational_issue_day` — a `LongTermHorizonResolverError`. The warn branch is therefore effective
   only for that case, never for `FileNotFoundError`.

**2026-09-28, round 4 (supersedes the narrowed gate above; the current rules — decisions
R4-native-lr-precedence, R4-merge-is-deploy, R4-integration-branch and R4-recalc-runs. Labelled, not
lettered, because letters E/F/G were already used by the 2026-09-26 decisions above (bulletin, tjhm
provenance cleanup, temporary LR fallback); those keep their original letters and are unaffected by this
relabelling. A bare number was tried and rejected: PP-065's own "Owner decisions this plan implements"
list is independently numbered 1-8, so "decision 4" there already and unambiguously means "quarter
min-pairs K = 10" — a numeric 2026-09-28 decision 4 would collide with it.)**

- **R4-native-lr-precedence. Native LR precedence wins over PP-064's Problem-7 trunk set.** For direct (non-derived) LR rows,
  PP-065 P1b's native-row rule (the shared helper; PP-065 § "Direct rows, native-row selection") applies
  under **both** flags: a direct LR row survives only if it passes native-row selection, not PP-064's
  broader "any issue year in `[start_year, end_year]`, any target year" trunk set. Non-native,
  backfill-shaped, and null/unparseable-date direct LR rows are **dropped**, and counted, under both
  flags. PP-064's precedence tests (`TestRegressionDirectPrecedenceSurvivesLowerBoundWidening`,
  `TestRegressionBackfillPrecedenceSurvivesLowerBoundTrim`, `tests/test_quarter_calendar_window.py:995,
  1046`) are rewritten with native-shaped fixtures per PP-065's list (PP-065 § "Owner decisions this plan
  implements", item 9). PP-064's own Problem-7 section carries a pointer to this decision.
- **R4-merge-is-deploy. Merge = deploy (verified fact).** Every merge to `maxat_sapphire_2` reaches every org whose image
  tags are `latest` within about a day: CI pushes `:latest` (`.github/workflows/deploy_production.yml`);
  Luigi auto-pulls the backend images whenever the digest differs
  (`apps/pipeline/pipeline_docker.py:296-304`); and the canonical 19:00 UTC cron entry
  (`bin/run_daily_maintenance.sh:118-126`) always runs `bin/daily_update_sapphire_frontend.sh`, which
  (`:66-80`) pulls `mabesa/sapphire-dashboard:$ieasyhydroforecast_frontend_docker_image_tag` and re-creates
  the dashboard containers (`doc/prod/update_deployment_checklist.md:835-842`). **Consequence: #527
  (PP-064 A), #528 (FD-029 P1) and #530 (PP-065 P1a) are all presumed LIVE on the servers since
  2026-09-27/28** — this covers FD-029 P1 too, via the daily frontend pull, not only PP-064 A/PP-065 P1a
  via Luigi. Verify per org: the creation dates of the postprocessing and dashboard images (`docker image
  inspect ... --format '{{.Created}}'`) and the image tags in the server `.env`. This verification is
  `PP-064.C.step0` in the dependency graph.
- **R4-integration-branch. Owner: HOLD the merges.** Because nothing merged to trunk can be kept off servers (R4-merge-is-deploy), the
  remaining work — PP-065 P1b, P1c, P1d and PP-064 B — is reviewed and merged as PRs into an
  **integration branch**, named `integ_quarter_p1b_p2`, not into `maxat_sapphire_2`. That integration
  branch merges into `maxat_sapphire_2` **only** in the P2 window — writers paused, right after the
  export — and that merge **is** the postprocessing deploy (`deploy.pp`).
  - The integration branch must be kept current with trunk: merge trunk INTO it periodically; never
    rebase a shared branch; never `git stash`.
  - Unrelated trunk merges (anything outside this quarter chain) keep deploying as normal — the hold
    applies only to PP-065 P1b–P1d and PP-064 B.
  - CI does not test these modules anyway (INFRA-059), so holding them off trunk costs no CI coverage.
  - Any other quarter-plan phase that touches postprocessing or the dashboard (e.g. FD-030) must state in
    its own plan whether it merges to trunk directly (live at once, so it must be safe live) or goes via
    the integration branch. This plan does not decide FD-030's route — it is an item for FD-030's own
    readiness review.
- **R4-recalc-runs. Owner: let the automatic QUARTERLY recalc run.**
  `bin/bimonthly_long_term_skill_metrics_recalculation.sh` runs via cron `0 10 ${LT_ISSUE_DAY} * *`
  (checklist ~:834); on servers it now runs with P1a's 3-of-3 observation rule and no prior export, so old
  quarter skill rows get overwritten. The owner accepts this. **[LIFTED 2026-09-28 by owner]** N7's ban on
  running a quarter skill recalc between P1a's deploy and P2's export (PP-065, "Rollout note (N7)") is
  lifted, and so is every other passage in this plan set that banned or mandated pausing that recalc
  before P2 — reason: the recalc applying P1a's 3-of-3 rule early is a correctness fix, not a hazard, and
  the ban was only ever protecting an export that can simply run later instead. P2's own export still
  precedes P2's own recalc; it will capture **3-of-3-era** quarter skill (whatever the automatic cron has
  already produced under P1a's rule by then), not pre-P1a skill — P2's runbook states this rather than
  assuming it captures the original baseline.

**User-visible consequence (supersedes the "after the joint deploy" framing elsewhere in this plan).**
FD-029 (which hides every quarter `EM` row and every non-native LR row) is live now, while PP-065 P1b
(which stops writing fresh `EM` rows and starts writing the derived seven-model / Naive Mean / Skilled
Mean rows) is not — it is held on the integration branch (decision R4-integration-branch). So **on servers now**, any quarter
whose DB rows are only a fresh `EM` row plus a non-native LR row shows a **blank card** — this is the
current state, not a transient one that begins "once the joint deploy completes". **The true recovery
point is not the branch merge itself**: the integration-branch merge (`deploy.pp`) only puts the new
derivation code on the servers — it writes no rows. The blank card clears only once the **first successful
quarterly postprocessing run on the new image** (the in-window recalc, PP-064 Chunk C step 4 / PP-065 P2)
has actually produced the derived seven-model and Naive/Skilled Mean rows, **and that run's output has been
verified** (PP-065 P2's post-checks). See PP-065 § "P2 — rollout" for the runbook step that runs this
promptly after the merge, so the blank-card interval does not extend to the next natural LT cron day.

**Status.** #527 (PP-064 A), #528 (FD-029 P1) and #530 (PP-065 P1a) are presumed live on both servers
(verify per org, `PP-064.C.step0`). PP-065 P1b onward and PP-064 B are held on the integration branch
`integ_quarter_p1b_p2` until the P2 window.

## What is wrong today

| Layer | Today | Plan |
|---|---|---|
| Schedule (data-repo model configs) | `forecast_months [3..9]` in both orgs: a rolling window is issued monthly Mar–Sep, and there is **no Q1** | LTF-014 |
| Postprocessing | [historical: pre-#527, fixed on trunk] The quarter label came from the `valid_from` month only, and skill joined on the calendar quarter, so rolling windows were scored against another quarter (#521); flag ON, a Dec-issued Q1 was trimmed out. Both are fixed since #527 (`read_quarterly_forecasts`/`read_latest_quarterly_forecasts` now exclude rolling-window rows at read; the flag-ON Dec-issued Q1 survives the latest reader — see `TestA5DecemberQ1FlagOn`, `apps/postprocessing_forecasts/tests/test_quarter_calendar_window.py:322`; the flag-OFF equivalent is `TestA10FirstYearQ1FlagOff` (`:913`) / `TestPP064aNativeQ1IssuanceRestriction` (`:2098`) — `TestRegressionDirectPrecedenceSurvivesLowerBoundWidening`/`TestRegressionBackfillPrecedenceSurvivesLowerBoundTrim` (`:995, 1046`) are also flag OFF, and cover a different precedence regression, not the flag-ON Dec-Q1 claim). Rows are not deleted, only excluded at read (see LTF-019). Quarter rows still come in three date populations | PP-064 |
| Quarter model set and ensembles | LR-only since 2026-06-23. The old monthly-derived path averages across issue dates and accepts 2 of 3 months. [Still live on trunk — PP-065 P1b not yet merged; `derive_quarterly_from_monthly_same_issue` and the `QUARTER_SUPPORTED_MODELS`/`QUARTERLY_DERIVED_MODELS`/`QUARTER_NATIVE_RAW_MODELS` constants already exist on trunk since P1a (#530), but nothing calls the helper yet and the readers still filter on `AGGREGATED_SUPPORTED_MODELS` (LR + ensembles only), not `QUARTER_SUPPORTED_MODELS`] | PP-065 |
| Monthly labels (stored data) | Pre-Feb-2026 hindcast rows carry offset windows (both orgs, mostly tjhm) and wrong January years (kghm GBT family). The producer is already fixed on trunk; the DB rows are stale and mis-score monthly skill | LTF-016 |
| GBT-family climatological bounds | `_add_climatological_quantile_bounds` takes the σ month from the raw `valid_from` and leave-one-out from `today.year`, so the Q25/Q75 of GBT-family monthly models use the wrong month's σ for many issue months: kghm month_1–3 (month-dependent) and tjhm month_3 (Jul and Dec issues). **Live since 2026-04-14** | LTF-017 |
| Dashboard quarterly card | [historical: pre-#528, fixed on trunk] The fetch window was set at import time and the latest issue won whatever window it covered. The renderer kept only rows at the max date; the caption relied on lead-1 arithmetic | FD-029 |
| Bulletin quarterly section | Uses the monthly norm, picks a model with `head(1)`, and shows a range only | FD-030 |
| Docs | Describe quarter as "rolling, not calendar". EM-parity and "0 deprecated rows" gates would false-fail. Cleanup predicates would delete product rows | DOC-009 |
| Early manual runs of issue-day-1 modes | The value is ratio-adjusted to the wrong month. This is live for the tjhm month modes | LTF-015 |

## The plans

| ID | File (`issues/`) | Priority / target |
|---|---|---|
| LTF-014 | `high_prio_gi_draft_ltf_quarter_calendar_schedule.md` | **Deferred** (P0, P0b, P1, P2). Configs stay `[3..9]`. P0b expires 2026-11-30 |
| PP-064 | `high_prio_gi_draft_pp_quarter_calendar_window_validation.md` | High. **Chunk A merged (#527), presumed already live via auto-pull (owner decision R4-merge-is-deploy, verify per org).** Chunk B merges into the integration branch (owner decision R4-integration-branch) |
| PP-065 | `high_prio_gi_draft_pp_quarter_derived_models.md` | High. **P1a merged (#530), presumed already live via auto-pull (owner decision R4-merge-is-deploy, verify per org).** P1b–P1d merge into the integration branch `integ_quarter_p1b_p2` (owner decision R4-integration-branch), which merges to trunk only in the P2 window — that merge is the postprocessing deploy, due by 2026-12-25. P3 removes the LR fallback |
| LTF-016 | `high_prio_gi_draft_ltf_monthly_window_labels.md` | High. A data fix (re-import and delete) plus verification; no producer change. Independent of the quarter chain |
| LTF-017 | `high_prio_gi_draft_ltf_climatological_bounds_raw_month.md` | High (live). Independent |
| FD-029 | `mid_prio_gi_draft_fd_quarter_card_calendar_window.md` | Medium. **P1 merged (#528), presumed already live via the dashboard's own daily auto-pull (owner decision R4-merge-is-deploy, verify per org)** |
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
3. **Order.** Steps 1–3 are all deployed before 2026-12-25. **Rollout mechanism (owner decisions R4-merge-is-deploy, R4-integration-branch, R4-recalc-runs,
   2026-09-28, current):** merge = deploy (decision R4-merge-is-deploy), so PP-064 A, FD-029 P1 and PP-065 P1a are already
   live on both servers (verify per org, `PP-064.C.step0`). The remaining work (PP-065 P1b–P1d, PP-064 B)
   is held on the integration branch `integ_quarter_p1b_p2` (decision R4-integration-branch) instead of being gated at the
   deploy step — there is no server-side gate to hold, since anything merged to trunk auto-pulls. The
   automatic bimonthly quarterly recalc is allowed to run in the meantime (decision R4-recalc-runs); it is not paused.
   1. PP-064 A, FD-029 P1 and PP-065 P1a: **presumed live already** (verify per org, `PP-064.C.step0`).
   2. DOC-009 P1a.
   3. PP-065 P1b–P1d and PP-064 B (the B5 check): reviewed and merged into `integ_quarter_p1b_p2`, kept
      current with trunk in the meantime.
   4. **In one writer-paused window between LT cron days** (kghm 10/25, tjhm 1):
      1. Merge `integ_quarter_p1b_p2` into trunk — this merge **is** `deploy.pp`.
      2. **Wait for the CI run on the merge commit to succeed**
         (`.github/workflows/deploy_production.yml`) — the merge only builds and pushes the `:latest`
         image; it does not by itself put anything on a server.
      3. **On each server, pull the new image**: `docker pull mabesa/sapphire-postprocessing:latest` (and
         `mabesa/sapphire-dashboard:latest` if this window also carries a dashboard-affecting change).
         Nothing else pulls it inside this window — Luigi only pulls when a task starts
         (`apps/pipeline/pipeline_docker.py:296-304`) and the recalc wrapper only pulls when no image
         exists locally at all (`bin/bimonthly_long_term_skill_metrics_recalculation.sh:77-85`), so with
         writers paused neither one will fetch the merge commit's image on its own.
      4. **Verify the pulled image**, per org, before any recalc or verification step below: `docker image
         inspect mabesa/sapphire-postprocessing:latest --format '{{.Created}}'` (and digest) matches the
         new build from step 4.2, not a stale local image.
      5. Run the tjhm decision-F step, and run **one recalc per org** after a DB export (the export now
         captures 3-of-3-era quarter skill, not pre-P1a skill — decision R4-recalc-runs).
   5. LTF-014 P0 when its gate clears.
   6. LTF-014 P2.
   7. PP-065 P3, deployed.
   8. The second recalc.
4. **Notice to the Kyrgyz and Tajik hydromets, before the recalc.**
   - The long-term quarter mode still runs **monthly Mar–Sep**. Only the calendar issues are published:
     kghm Mar/Jun/Sep 25 → Q2/Q3/Q4, and tjhm Apr/Jul 1 → Q2/Q3. The rolling issues are ignored.
   - **Q1 (both orgs) and tjhm Q4 now appear**, once PP-065 P1b (still held on the integration branch) has
     merged and produced them. They are built from monthly forecasts (the seven models plus Naive/Skilled
     Mean). **On kghm**, no LR row is shown for these quarters from that point on (the fallback LR is not
     persisted, round-2 decision 3). **On tjhm**, until PP-065 P1b and the decision-F cleanup land (see the
     tjhm interim below), a monthly-derived LR_Base/LR_SM row may still be shown for these quarters — as
     native LR, indistinguishable from a genuine issuance (round 4, decision 1). Only once PP-065 P1b +
     decision F land does tjhm also show no LR row for these quarters, like kghm. The caption still shows
     the scheduled issue date throughout, on both orgs.
   - Seven more models appear in the quarterly outputs once PP-065 P1b lands.
   - The quarterly ensembles become Naive Mean and Skilled Mean, as for monthly, once PP-065 P1b lands.
     **No quarterly Ensemble Mean is shown** — FD-029 is **live now** (owner decision R4-merge-is-deploy) and hides every
     quarter `EM` row on the dashboard and in the bulletin input already (both old, persisted rows and any
     fresh ones). Postprocessing itself still **writes** fresh quarter `EM` rows today, since PP-065 P1b
     (the change that stops this write) is held on the integration branch, not yet merged to trunk (owner
     decision R4-integration-branch): `ensemble_calculator.py:765` sets `model_short = "EM"` directly (in
     `_create_aggregated_ensemble_forecasts`), and `api_writer.py:1157-1158` (on trunk since #527, unchanged
     from the pre-#527 branch) resolves that through `MODEL_TYPE_MAP`'s identity `"EM": "EM"` entry
     (`api_writer.py:30`) — not an `"ENSEMBLE_MEAN"`-to-`"EM"` mapping, which is a separate entry used only
     by the skill-metrics write path.
   - **Current, ongoing consequence (not a transient one): a blank card.** Because FD-029 is live and hides
     both the fresh `EM` rows above and every non-native LR row, while PP-065 P1b (which would start
     writing the derived seven-model rows and the Naive Mean / Skilled Mean rows that replace them) has not
     yet merged, **any quarter whose only DB rows are a fresh `EM` row plus a non-native LR row shows a
     blank card on the dashboard right now**. The true recovery point is **not** the `deploy.pp` merge
     itself — merging only puts the new code on the servers, it writes no rows — but the **first successful
     quarterly postprocessing run on the new image**, verified (PP-064 Chunk C step 4 / PP-065 P2). See the
     PP-065 P2 runbook (`high_prio_gi_draft_pp_quarter_derived_models.md` § "P2 — rollout"), which has an
     explicit step to run quarterly postprocessing promptly after that merge and verify it, so the
     blank-card interval does not extend to the next natural LT cron day.
   - **tjhm interim, accepted:** until PP-065 P1b and the decision-F cleanup land, tjhm's monthly-derived
     LR_Base/LR_SM rows are shown as native LR (round 4, decision 1 above) — kghm is unaffected.
   - Bounds for models without quantiles are ±δ on the dashboard. Until FD-030 lands, the bulletin range
     can be blank.
   - **For fallback-derived quarters, LR is included in the ensembles but not shown as its own row** — on
     kghm, unconditionally, since the fallback is never persisted; on tjhm, only once PP-065 P1b + decision
     F land (before that, see the tjhm interim above — a monthly-derived LR row may be shown there). This
     holds for as long as the schedule change stays deferred, which has no date yet.
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
    "DOC-009.P2a":     { "stage": "merge",  "depends_on": ["PP-065.P1d"], "note": "rows 10 and 12; released with the PP-065 deploy. The PP-065 P1a 3-of-3 observation content it documents is on trunk since #530 AND presumed already LIVE on servers via auto-pull (owner decision R4-merge-is-deploy, verify per org)" },
    "DOC-009.P2b":     { "stage": "merge",  "depends_on": ["LTF-014.P0.kghm", "LTF-014.P0.tjhm"], "note": "row 11 (LT readme schedule); deferred with P0" },
    "LTF-014.P0.tjhm": { "stage": "ops",    "depends_on": ["LTF-014 P0 gate"], "status": "DEFERRED by owner 2026-09-26 (configs stay [3..9])" },
    "LTF-014.P0b":     { "stage": "ops",    "depends_on": ["LTF-014.P0.tjhm"], "status": "MOOT while P0 is deferred; expires 2026-11-30 (recovery window)" },
    "LTF-014.P0.kghm": { "stage": "ops",    "depends_on": ["LTF-014 P0 gate"], "status": "DEFERRED by owner 2026-09-26 (configs stay [3..9])" },
    "LTF-014.P1":      { "stage": "merge",  "depends_on": ["LTF-014.P0 resumes"], "status": "DEFERRED with P0 (lock tests for configs not deployed)" },
    "LTF-014.P2":      { "stage": "ops",    "depends_on": ["LTF-014.P0.kghm", "LTF-014.P0.tjhm", "D2"] },
    "LTF-015":         { "stage": "merge",  "depends_on": [], "parallel_agents": 1, "note": "shares test files with LTF-014 P1; whichever lands second rebases" },
    "LTF-016.P0":      { "stage": "ops",    "depends_on": [] },
    "LTF-016.P1":      { "stage": "ops",    "depends_on": ["LTF-016.P0"], "note": "verification: deployed LT image >= 99c5a552, server CSV labels" },
    "LTF-016.P2":      { "stage": "ops",    "depends_on": ["LTF-016.P1"], "note": "re-import and delete with the service owner, then the monthly recalc" },
    "LTF-017":         { "stage": "merge",  "depends_on": [], "parallel_agents": 1 },
    "PP-064.A":        { "stage": "merge",  "depends_on": [], "status": "MERGED #527", "parallel_agents": 1 },
    "PP-064.A.deploy": { "stage": "deploy", "depends_on": ["PP-064.A"], "deadline": "2026-12-25", "status": "presumed live via auto-pull since 2026-09-27/28 (owner decision R4-merge-is-deploy); verify per org (PP-064.C.step0)", "note": "not gated on the integration-branch merge (deploy.pp): its own deploy already happened via auto-pull, independent of PP-065/FD-029" },
    "PP-064.C.step0":  { "stage": "ops",    "depends_on": [], "note": "per-org read: SAPPHIRE_SKILL_LEAD_AWARE, ml_long_term_supported_modes, min_pairs, and confirm the quarter config carries operational_issue_day (PP-064 Chunk C step 0). Also record the image tag and the postprocessing/dashboard image creation dates (docker image inspect mabesa/sapphire-postprocessing:latest --format '{{.Created}}', same for sapphire-dashboard) -- this is decision R4-merge-is-deploy's per-org verification. The automatic bimonthly QUARTERLY recalc is allowed to run and is NOT paused (owner decision R4-recalc-runs, 2026-09-28)." },
    "FD-029":          { "stage": "merge",  "depends_on": [], "status": "MERGED #528 (P1)", "parallel_agents": 1 },
    "FD-029.deploy":   { "stage": "deploy", "depends_on": ["FD-029"], "deadline": "2026-12-25", "status": "presumed live via auto-pull since 2026-09-27/28 (owner decision R4-merge-is-deploy: the dashboard's own daily frontend cron pull, independent of Luigi); verify per org (PP-064.C.step0)", "note": "not gated on the integration-branch merge (deploy.pp): its own deploy already happened, independent of PP-065 P1b-P1d" },
    "PP-065.P1a":      { "stage": "merge",  "depends_on": ["PP-064.A"], "status": "MERGED #530", "parallel_agents": 1 },
    "PP-065.P1a.deploy": { "stage": "deploy", "depends_on": ["PP-065.P1a"], "deadline": "2026-12-25", "status": "presumed live via auto-pull since 2026-09-27/28 (owner decision R4-merge-is-deploy); verify per org (PP-064.C.step0)", "note": "the 3-of-3 observation rule takes effect on any quarter skill recalc against a live-auto-pulled image; owner decision R4-recalc-runs accepts this and does not pause the automatic recalc" },
    "PP-065.P1b":      { "stage": "merge",  "depends_on": ["PP-065.P1a"], "parallel_agents": 1, "note": "merges into the integration branch integ_quarter_p1b_p2 (owner decision R4-integration-branch), not directly into maxat_sapphire_2" },
    "PP-065.P1c":      { "stage": "merge",  "depends_on": ["PP-065.P1a"], "parallel_agents": 1, "note": "merges into the integration branch integ_quarter_p1b_p2 (owner decision R4-integration-branch), not directly into maxat_sapphire_2" },
    "PP-065.P1d":      { "stage": "merge",  "depends_on": ["PP-065.P1b", "PP-065.P1c"], "parallel_agents": 1, "note": "merges into the integration branch integ_quarter_p1b_p2 (owner decision R4-integration-branch), not directly into maxat_sapphire_2" },
    "PP-064.B":        { "stage": "merge",  "depends_on": ["PP-065.P1d", "D3"], "parallel_agents": 1, "note": "the B5 check only; merges into the integration branch integ_quarter_p1b_p2 (owner decision R4-integration-branch), not directly into maxat_sapphire_2" },
    "PP-065.P2.ready": { "stage": "ops",    "depends_on": ["PP-065.P1d", "PP-064.B", "DOC-009.P1a", "PP-064.C.step0"], "note": "P2's code and runbook reviewed on the integration branch, and the writer-paused window scheduled. This is the readiness precondition for deploy.pp (the integration-branch merge to trunk)." },
    "deploy.pp":       { "stage": "deploy", "depends_on": ["PP-065.P2.ready"], "deadline": "2026-12-25", "note": "owner decision R4-integration-branch: this node IS the merge of integ_quarter_p1b_p2 into maxat_sapphire_2, inside the writer-paused window, right after the DB export -- that merge is the postprocessing deploy. PP-064.A.deploy, FD-029.deploy and PP-065.P1a.deploy are independent, already presumed live (see their own nodes) and are not part of this merge. It does not by itself put the new image on any server -- CI still has to build and push :latest, and each server still has to docker pull it; see PP-064 Chunk C step 4 for the explicit wait/pull/verify sub-steps." },
    "tjhm.reimport":   { "stage": "ops",    "depends_on": ["deploy.pp"], "note": "decision F (tjhm provenance cleanup), with the service owner, inside the same writer-paused window as deploy.pp" },
    "recalc.1":        { "stage": "ops",    "depends_on": ["deploy.pp", "tjhm.reimport"], "note": "PP-064 C + PP-065 P2, per org, after an export taken at the start of this same writer-paused window; the export captures 3-of-3-era quarter skill (owner decision R4-recalc-runs), not pre-P1a skill. Requires the per-org image pull and creation-date/digest verification (PP-064 C step 4) to have completed first." },
    "PP-065.P3":       { "stage": "merge",  "depends_on": ["LTF-014.P2", "recalc.1"] },
    "PP-065.P3.deploy":{ "stage": "deploy", "depends_on": ["PP-065.P3"] },
    "recalc.2":        { "stage": "ops",    "depends_on": ["LTF-014.P2", "PP-065.P3.deploy"] },
    "FD-030":          { "stage": "merge",  "depends_on": ["FD-029", "D6"], "parallel_agents": 1 }
  }
}
```

**Shared files, strictly sequential:**
- `src/data_reader.py` quarter readers: PP-064 A → PP-065 P1b → PP-065 P3. PP-064 B no longer edits them.
- `apps/long_term_forecasting/tests/test_lt_utils.py`: LTF-015, and LTF-014 P1 if it resumes. Whichever
  lands second rebases.
- `doc/plans/module_issues.md`: DOC-009 and the index updates.

Everything else touches disjoint files and can run in parallel, including LTF-016 and FD-029.
