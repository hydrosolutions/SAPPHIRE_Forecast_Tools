# PP-064: Score and ensemble only exact calendar-quarter windows, and carry a December-issued Q1 through

**Status**: Draft (2026-09-25, rev 2 after out-of-loop review)
**Module**: `apps/postprocessing_forecasts`
**Priority**: High.
- Stored quarterly skill is wrong today: a rolling window is scored against a different quarter's
  observations. With `SAPPHIRE_SKILL_LEAD_AWARE=true` the wrong window is picked deterministically
  (#521: NSE ≈ −75 → +0.1 after correction).
- Chunk A must be deployed **before 2026-12-25**. With the flag ON, the operational reader drops the
  first kghm Q1, issued Dec 25 for the next year (Problem 6).

**Labels**: `postprocessing_forecasts`, `skill-metrics`, `long-term`, `quarter`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only.
**Supersedes the code approach of**: GitHub #521 / branch `sandro_sapphire_2_quaterly_agg` (f0a83352)
**Related**:
- PP-056 (quarter skill at hv=0)
- PP-041 (no forecast-side invalidation)
- PP-063 (gap detector presence-only)
- PP-049 (flag-OFF `keep="last"` API-order dependence)
- PP-042 (display-form ensemble exclusion)
- PP-061 (aggregated writer `date=valid_from` key collisions)
- PP-020 (quantile averaging)
- MIG-008, DOC-009

Citations are to trunk `82946683`. Its postprocessing tree is identical to the #521 branch base
`559ec9e4`. The local deployment env for kghm (`kyg_data_forecast_tools/config/.env_kghm:502`) enables
`SAPPHIRE_SKILL_LEAD_AWARE`, so **both flag states are live somewhere** and both are in scope.

## Contract (settled decisions this plan must not break)

- **Quarter = calendar Q1–Q4** (owner, 2026-09-25). A valid quarter window has `valid_from` = the 1st of
  Jan/Apr/Jul/Oct **and** `valid_to` = the last day of Mar/Jun/Sep/Dec of the same year.
- **`horizon_value` = config `operational_month_lead_time`** (kghm 1, tjhm 0;
  `doc/prod/longforecast_quarter_season_hv_convention.md` RESOLUTION). Never overwrite a stored hv with a
  date-derived lead.
- **Quarterly Ensemble Mean**:
  - **Today:** mean(LR_Base, LR_SM), not skill-gated (owner, 2026-06-23,
    `doc/plans/archive/two_model_ensemble_plan.md` M1).
  - **Chunk A** preserves today's membership. **Chunks B and C**, which follow PP-065, preserve PP-065's
    rule. No chunk of this plan changes EM membership itself.
  - **PP-065** changes it, per owner decision D10 (2026-09-25): with more than 2 candidates it is
    long-term-gated.

  **Raw quarter models:**
  - LR_Base and LR_SM are native-only.
  - GBT, LR_SM_DT, LR_SM_ROF, MC_ALD, SM_GBT, SM_GBT_LR and SM_GBT_NORM are re-enabled as same-issue
    averages of their monthly forecasts (owner, 2026-09-25). That is implemented by **PP-065**, not here.
  - Until PP-065 lands, the trunk filter (`src/model_names.py:14-16`) keeps quarter LR-only; this plan
    does not change it.
- **Flag OFF stays byte-identical for calendar-aligned input** (PP-056 `:164`; flag-OFF golden
  `tests/test_skill_lead_aware_golden_baseline.py:93`), except where Chunk B, with owner approval,
  changes it deliberately.
- `select_operational_issuances` (`data_reader.py:225-398`) is not modified. It only has to receive
  calendar-only rows.

## Mechanism and problems (verified on trunk)

1. **Label.** `_normalize_combined_forecasts` parses `valid_from` (`src/data_reader.py:3748`), then sets
   `quarter_in_year = MONTH_TO_QUARTER[valid_from.month]` (`:3756-3759`). It never checks `valid_to`, so
   Apr–Jun, May–Jul and Jun–Aug all become Q2. It is the single choke point for every **direct** quarter
   read:
   - `read_quarterly_forecasts` `:3129`
   - `read_latest_quarterly_forecasts` `:3405`
   - `_read_long_combined_forecasts_api` `:3723` (→ `read_quarterly_combined_forecasts` `:3611`, which
     feeds the gap detector, the operational `existing_q` and maintenance `q_combined`)

   The monthly-derived source never passes through it; its windows are synthesized as calendar windows
   (`src/aggregation.py:274-281`).
2. **Select, flag ON.** `select_operational_issuances` keeps the latest issue per
   `(code, model, year, quarter_in_year, lead)` (`:339-341, 346-348, 390-391`). kghm Q2 therefore keeps
   the 25 May Jun–Aug window; tjhm Q1 keeps the 1 Mar Mar–May window.
3. **Select, flag OFF.** The only dedup of direct rows is `drop_duplicates(keep="last")` on
   `[code, year, quarter, model]` (`:3142-3157`). It runs only when the monthly-derived source is
   non-empty, and its order is API order (PP-049). The skill path does not dedup.
4. **Pair.** `calculate_quarterly_skill_metrics` merges on `["code","year","quarter_in_year"]`
   (`src/skill_metrics.py:2452`); the aggregated path merges at `:2682-2688`.
5. **Persist: three date populations exist for QUARTER LR rows** (local DB, 2026-09-25; counts are
   aggregate only).
   - **(a) Native producer rows**, `date` = the real issue date.
   - **(b) Flag-OFF rewrites.** `_write_aggregated_forecasts_to_api` writes **every** row of the joint
     frame, raw LR rows included. It uses `flag: 0` and `record_date = valid_from`
     (`src/api_writer.py:1193-1214`), from the operational run (`postprocessing_operational_long_term.py:216-226`),
     the recalc (`recalculate_skill_metrics.py:391-406`) and maintenance
     (`postprocessing_maintenance_long_term.py:357-376`). Locally, 3,007 LR_BASE hv1 calendar rows are
     rewrites next to 3,635 native ones.
   - **(c) Persisted monthly-derived rows.** Flag ON: `date = valid_from − monthly hv`
     (`aggregation.py:294-300`), with the writer keeping the monthly hv. Example: 1,505 LR_BASE hv1 Q1
     rows are dated **Dec 1**. Flag OFF: `date = valid_from`.

   Populations (b) and (c) have calendar windows, so **no window filter distinguishes them from (a)**.
   The writer also re-emits rolling windows, because it overrides the synthesized calendar window with
   the row's own `valid_from`/`valid_to` (`:1193-1197`).
6. **December-issued Q1 is dropped by the operational reader (flag ON).** `read_latest_quarterly_forecasts`
   sets `end_year = today.year` (`:3331-3334`) and, with the flag on, trims direct rows to target years
   `[start_year, end_year]` (`:3413`). On 2026-12-25 the new Q1 2027 row (`year = 2027`) is removed, so no
   operational Q1 ensemble is built (`postprocessing_operational_long_term.py:211`).
7. **Flag-OFF historical read misses a December-issued Q1 of the first year.** `read_quarterly_forecasts`
   flag OFF reads issue years from `start_year` (`:3121-3126`), so Q1 of `start_year` (issued in December
   of `start_year − 1`) is not read. Only the first recalc year is affected.
8. **Empty skill → no quarterly EM, and this is locked by a test.** The operational run skips all
   quarterly ensembles when the skill frame is empty (`postprocessing_operational_long_term.py:210`), and
   `_create_aggregated_ensemble_forecasts` returns without ensembles on empty skill
   (`src/ensemble_calculator.py:632`). `tests/test_quarterly_ensemble_creation.py:329`
   (`test_empty_skill_returns_forecasts_only`) asserts exactly that.
   - This sits uneasily with M1's "EM reliably = mean(LR_Base, LR_SM)". It only bites when **no** quarter
     skill exists at all (e.g. a fresh deployment).
   - Changing it is decision B5 (Chunk B), not part of Chunk A.

## Chunk A — calendar-window validation + December Q1 (both flag states)

**Goal**:
- A non-calendar quarter row is excluded (never relabelled) at the direct-read choke point and at the writer.
- A December-issued Q1 survives the operational reader.

**Files (only these may be modified)**:
- `apps/postprocessing_forecasts/src/aggregation.py`: new pure helper
  `filter_calendar_quarter_windows(df) -> tuple[pd.DataFrame, int]` next to `QUARTER_MONTHS`
- `apps/postprocessing_forecasts/src/data_reader.py`:
  - `_normalize_combined_forecasts`: apply the helper **for `horizon == "quarter"` only, before the
    existing `valid_from` parse** (`:3748`); season behaviour unchanged
  - `read_latest_quarterly_forecasts`: the target-year upper bound of the flag-ON trim (`:3331-3334, 3413`)
- `apps/postprocessing_forecasts/src/api_writer.py`: quarter branch of `_write_aggregated_forecasts_to_api`
  (`:1160-1204`)
- Tests: new `apps/postprocessing_forecasts/tests/test_quarter_calendar_window.py`; new tests may be
  appended to existing quarter test files, but existing tests are not edited

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* Do not
change:
- the writer's `horizon_value` and `record_date` logic (`api_writer.py:1172-1175, 1199-1204`)
- EM membership
- `select_operational_issuances`
- `model_names.py`
- aggregation grouping
- the season branch

Do not cherry-pick from `sandro_sapphire_2_quaterly_agg`.

**Helper semantics**
- Parse with `pd.to_datetime(..., format="mixed", errors="coerce").dt.normalize()`. Reader output mixes
  date-only strings and timestamps (`astype(str)` at `:3170-3172, 3445-3447`).
- Both `valid_from` and `valid_to` absent → return unchanged. One absent, null or unparseable → invalid,
  dropped.
- Keep a row iff `valid_from` is day 1 of month 1/4/7/10 **and** `valid_to` is the last day of month
  `valid_from.month + 2` of the **same year**.
- Log the dropped count **at INFO** (stale rolling rows stay in the DB, so this fires on every read), with
  the horizon and no station codes.

**Year bound (Problem 6).** For the flag-ON trim in `read_latest_quarterly_forecasts`, the upper target
year must admit the next year's Q1 (e.g. `end_year + 1`, or no upper bound on target year beyond what the
issue-date read returns). The implementer states which in the PR. The issue-date API read (`:3405-3411`)
is unchanged.

**Problem 7** is accepted as a documented limitation (first recalc year only). Add a code comment at
`:3121` and do not change it.

**Tests (Arrange → Act → Assert, station `19999`)**
- **A-1. Reader** (flag ON/OFF × `read_quarterly_forecasts` / `read_latest_quarterly_forecasts`).
  - Input: kghm-like direct rows issued 2024-03-25 (Apr–Jun, 100), 04-25 (May–Jul, 200) and 05-25
    (Jun–Aug, 300).
  - Expect one Q2 row: value 100, `valid_to` 2024-06-30. For flag OFF, do not assert `date`/`horizon_value`
    (`data_reader.py:83-94`).
- **A-2. `valid_to` enforcement.** Direct rows `2024-04-01..2024-07-31`, `2024-04-01..2025-06-30` and
  `2024-04-01..` with null `valid_to` are excluded by both readers and by the writer.
  (Mutation check: deleting the `valid_to` predicate must make A-2 fail.)
- **A-3. Malformed input through the reader.** Frame with a missing `valid_to` column; frame with
  unparseable dates; mixed `"2024-04-01"` / `"2024-04-01 00:00:00"`; all-invalid batch → no exception, only
  valid rows returned.
- **A-4. Placement.** (Mutation check: moving the helper call after `select_operational_issuances` must
  make A-1 flag ON fail.)
- **A-5. December Q1, flag ON.** `forecast_date = 2026-12-25`, direct LR_Base/LR_SM rows issued 2026-12-25
  for 2027-01-01..2027-03-31 at hv1 → `read_latest_quarterly_forecasts` returns them with `year == 2027`,
  `quarter_in_year == 1`. It fails on trunk.
- **A-6. Gap detector, both directions** (`src/gap_detector.py:370-496`; do not edit it).
  - (i) A calendar quarter whose only EM row is rolling-windowed is reported as a gap after Chunk A.
  - (ii) A quarter whose only raw rows are rolling is not reported as a gap for a calendar quarter it
    does not cover.
- **A-7. Writer.** A non-calendar window, or one disagreeing with `(year, quarter_in_year)` → no API record,
  one log line. A calendar row → a record identical to today's (hv and date per flag).
- **A-8. Season rows** through `_normalize_combined_forecasts` are unchanged.
- **Must stay green without edits:**
  - `test_skill_lead_aware_golden_baseline.py:93`
  - `test_lt_min_pairs_gate.py:592-617`
  - `test_quarterly_ensemble_creation.py:203,458`
  - `test_quarterly_skill_metrics.py:265,529`
  - `test_lead_aware_aggregated_skill_ensemble.py:252-263`
  - `test_lead_aware_writer_reader_round_trip.py:419`
  - `test_quarterly_api_writer.py:233,254,312`
  - `test_quarterly_data_reader.py:899,1369`
  - `test_min_n_stale_integration.py:405` (stays xfail)

**Acceptance**:
- Record the full module suite counts before editing.
- After: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` gives zero
  failures and zero unexpected skips; the only xfail is the pre-existing one.
- A-1, A-2 and A-5 fail on trunk.
- `git diff --stat` touches only the listed files.
- `ruff check` / `ruff format --check` are clean on the touched files.

## Chunk B — duplicate direct rows, observation coverage, empty skill (owner decision gate)

**Sequencing.** The monthly-derived source is rebuilt by **PP-065**: same issue only, the seven re-enabled
models only, LR native-only, and legacy direct rows of the seven models ignored. That removes trunk's
mixed-issue derived rows and the "latest quarter" hijack. Chunk B covers what PP-065 does not. It runs
**after** PP-065, because both edit the two quarter readers.

**What remains (verified on trunk):**
- **Duplicate direct LR rows.** Under flag OFF, the only dedup of direct rows is the `keep="last"` over
  the **whole concatenated frame** at `:3142-3157` (and `:3422-3432`). It runs whenever both sources are
  non-empty, so any derived row of an unrelated model triggers it. The result is:
  - with no derived rows, direct duplicates survive;
  - with any derived rows, duplicates are collapsed arbitrarily, in API order.
  - Native rows (a), rewrites (b) and persisted old derived LR rows (c, e.g. dated Dec 1) would all pair
    with the same observation.
  - Under flag ON, `select_operational_issuances` drops (b) and (c) for kghm, where the issue day (25) or
    lead does not match. It cannot drop them for tjhm: day 1, lead 0 means `date == valid_from` matches
    the schedule.
- **Observations.** `aggregate_monthly_obs_to_quarterly` (`aggregation.py:97-146`) accepts 2 of 3 months,
  with an unweighted mean. Preprocessing's quarter norms are also unweighted (`sync_long_horizon_hydrograph.py:638`).
  PP-065's derived forecasts use the **same** weighting rule as the observations.

**Decisions needed (overview D3):**
- **B2. Direct-row dedup (required).** A deterministic dedup of direct quarter rows per
  `(code, year, quarter, model)` in both readers, under both flags.
  - Recommended rule: prefer the row whose derived lead (`valid_from` month − `date` month, in months)
    and issue day match the configured quarter schedule (population (a)).
  - Otherwise prefer the earliest `date` that is not equal to `valid_from`.
  - Otherwise the earliest row.
  - This brings PP-049's quarter part into scope.
- **B3. Persisted LR rewrites (b) and old derived LR rows (c).** Either accept them (B2 makes them lose to
  native rows where native exists), or remove them in a reviewed DB step (D8).
  - For tjhm (day 1, lead 0), rewrites share the native key (PP-061), so they are the same rows.
- **B4. Observation coverage and weighting.** Recommended: require 3 of 3 months and keep the unweighted
  mean (consistent with preprocessing norms).
  - Day-weighting is an owner option. If chosen, it applies to observations, PP-065's derived forecasts
    and preprocessing's quarter norms together (regenerated).
- **B5. Empty skill.** Should a fixed-LR quarterly EM be produced when no quarter skill rows exist at all
  (Problem 8)? If yes:
  - change `postprocessing_operational_long_term.py:207-212` and `ensemble_calculator.py:632` for the
    quarter EM only;
  - update `test_quarterly_ensemble_creation.py:329` to require the fixed-LR EM (the season empty-skill
    test stays unchanged);
  - add an operational-orchestration test under both flags.

**Files (after the decisions)**:
- `src/data_reader.py`: the direct-row dedup in the two quarter readers
- `src/aggregation.py`: observation coverage
- `postprocessing_operational_long_term.py`, `src/ensemble_calculator.py`: B5 only
- tests
- `doc/data_flow_long_term.md`: the 2-of-3 statement at `:259-262`

**Tests that legitimately change** (each edit states its reason in the PR):
- the observation-coverage tests in `test_aggregation.py` (e.g. `:41` `QUARTER_MIN_MONTHS == 2`, `:168`,
  if they concern observations)
- `test_quarterly_ensemble_creation.py:329` (B5 only)

PP-065 owns the derived-forecast test changes.

**New tests:**
- Native row + rewrite + persisted old derived Dec-1 row for the same LR Q1 → one row, the native one:
  - under both flags and both org shapes (kghm day 25 / lead 1, tjhm day 1 / lead 0);
  - with and without an unrelated derived-model row present;
  - with shuffled direct-row order.
- Observations with 2 of 3 months → no quarterly observation (if B4 = 3 of 3).

M1 EM tests must stay unchanged.

## Chunk C — rollout and verification (ops; mostly no code)

**Order** (as in the overview graph):
- Chunk A and PP-065 deployed.
- Then **either** B deployed, **or** the owner approves deferring B, in which case a repeat recalc after B
  deploys is mandatory.
- Then recalc.

1. **Pre-deploy DB audit per org** (read-only SQL, aggregate counts only, no station codes).
   - Classify QUARTER `long_forecasts` rows by flag, model family, window class (calendar/rolling/other),
     `horizon_value`, and **date population**:
     - (a) schedule match: derived lead = config lead and issue day = config day;
     - (b) `date = valid_from`;
     - (c) other, e.g. Dec 1.
   - Also count QUARTER `skill_metrics` rows by `date` year and `horizon_in_year`.
   - Local baseline (2026-09-25, both orgs mixed): kyg LR flag 0 = 45,581 calendar / 15,922 rolling; all
     6,860 skill rows are calendar-keyed and dated 2026.
2. **Recalc** with each deployment's actual flag state (`doc/prod/long_term_deploy_runbook.md`
   § Lead-aware skill).
3. **Post-checks.**
   - Pair counts: before the recalc, derive the expected `n_pairs` per `(model, quarter, lead)` and flag
     state from the audit. That is the number of unique target years with an eligible calendar forecast
     row, **after** the chosen B2 dedup and B4 observation coverage, that also have eligible observations.
   - Compare with the result. Explain every difference; there is no blanket "unchanged" or "reduced"
     expectation.
   - Freshly written QUARTER rows contain no rolling windows.
   - A persisted-derived-row round trip (write → read) yields the B2-selected row.
   - Spot check the #521 station privately (its code is never written to the repo). A plausible value is
     a spot check, not proof.
4. **What the recalc leaves behind:**
   - Rolling-windowed rows (raw and EM), which are inert after Chunk A: owned by PP-041 / the stale-rows
     decision.
   - Under flag OFF, calendar-window EM rows mixed from rolling inputs are overwritten, because they have
     the same key. Rows the recalc no longer emits survive (PP-041/PP-063).

## Out of scope

- Re-enabling the seven models for quarter, and the long-term-gated EM (both PP-065). Short-term-threshold EM gating (#521's proposal) is rejected.
- Dashboard (FD-029/FD-030). Schedule and target construction (LTF-014).
- Deleting DB rows (the stale-rows decision).
- Skill/ensemble-level duplicate window guards. Their inputs come only from the guarded readers or from
  derived rows that are calendar by construction, so they would add no protection.
