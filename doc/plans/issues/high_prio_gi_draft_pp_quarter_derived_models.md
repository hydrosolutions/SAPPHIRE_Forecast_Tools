# PP-065: Re-enable seven models for quarter as same-issue averages of their monthly forecasts

**Status**: Draft (2026-09-25, rev 2 after out-of-loop review)
**Module**: `apps/postprocessing_forecasts`
**Priority**: High (owner request, 2026-09-25)
**Labels**: `postprocessing_forecasts`, `long-term`, `quarter`, `ensembles`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only.
**Related**:
- PP-064 (calendar-window validation; this plan comes after its Chunk A)
- PP-056 (seven models' quarter skill at hv=0)
- PP-020 (averaging quantiles is not valid)
- PP-061 (the aggregated writer hardcodes `flag=0`)
- GitHub #521 (proposed this; branch `sandro_sapphire_2_quaterly_agg` is a reference implementation)

Paths are relative to `apps/postprocessing_forecasts/`; citations are to trunk `82946683`.

## Owner decision (2026-09-25) — reverses the quarter part of the 2026-06-23 decision

For **quarter**, re-enable `GBT`, `LR_SM_DT`, `LR_SM_ROF`, `MC_ALD`, `SM_GBT`, `SM_GBT_LR`, `SM_GBT_NORM`.
Each model's quarterly forecast is **the average of its monthly forecasts for the three months of the
calendar quarter, all from the same issue**. This is deliberate: these models have no native quarter
configuration.

**Unchanged:**
- `LR_Base` and `LR_SM` stay **native-quarterly only** (their own quarter mode, LTF-014).
- **Season stays LR-only** (`AGGREGATED_SUPPORTED_MODELS`, `src/model_names.py:14-16`, applied by the
  season readers at `src/data_reader.py:3284, 3561`).
- `horizon_value` = the configured quarter lead (kghm 1, tjhm 0).
- The quarterly Ensemble Mean rule changes (owner, D10, 2026-09-25): see Target behaviour item 6.
- **Averaging weights.** The derived quarter uses an **unweighted** mean of the three monthly values, the
  same rule quarterly observations and preprocessing's quarter norms use today. If PP-064 B4 later adopts
  day-weighting, it changes all three together.

## Why it is feasible (verified 2026-09-25)

- **Both orgs run all nine models in `month_1`/`month_2`/`month_3`**, on the quarter's issue day, with no
  month restriction:
  - kghm: issue day 25, leads 1/2/3. The 25 Dec issue covers Jan/Feb/Mar.
  - tjhm: issue day 1, leads 0/1/2. The 1 Jan issue covers Jan/Feb/Mar.

  Source: `config/long_term_configs/month_*.json` and `models_and_scalers/long_term_forecasting/month_*/*/*/general_config.json`
  in each data repo. kghm `month_0` (issue day 10) is a different issue and never takes part.
- **Readers already carry what the derivation needs.** `_normalize_monthly_forecasts` keeps `date` and
  `horizon_value`. Flag-ON operational selection keeps every **configured** monthly lead, which includes
  L, L+1 and L+2 in both deployments.
- **History exists for skill, Q1 included.** In the local dev DB (both orgs mixed; re-measure per
  server), each of the seven models has same-issue, non-null MONTH triplets at the calendar-quarter issue
  dates for **all four quarters, 2000–2025/26**:
  - kghm: ~3,800–5,000 triplets per model, 45–53 stations;
  - tjhm: ~650–1,200 per model, 13–15 stations.

  No new hindcasts are needed, unlike native LR Q1 (LTF-014 P2).

## What trunk does today

- **Model filter.** `_filter_supported_aggregated_forecast_models` keeps LR_BASE, LR_SM and the ensembles
  (`src/data_reader.py:97-103`). It is applied **only** in the two raw quarter readers (`:3162`, `:3437`)
  and the season readers.
  - **The combined reader `read_quarterly_combined_forecasts` (`:3591-3618`) applies no model filter.**
    It feeds the gap detector (`src/gap_detector.py:467-479`), the operational `existing_q`
    (`postprocessing_operational_long_term.py:220-227`) and maintenance's merge-back
    (`postprocessing_maintenance_long_term.py:357-376`).
- **The derived path is wrong for this purpose.** `aggregate_monthly_fc_to_quarterly`
  (`src/aggregation.py:218-308`):
  - groups by `[code, year, quarter, model]`, plus the monthly hv under flag ON, but **never by issue date**;
  - accepts 2 of 3 months (`QUARTER_MIN_MONTHS`, `:38`, also used by observations);
  - averages quantiles.

  It is also imported directly by `tests/test_quarterly_api_writer.py:337-378`.
- **Point value.** Skill treats `q` as authoritative, with `q50` as fallback (`src/skill_metrics.py:1369-1384`).
  The latest reader instead pre-fills `forecasted_discharge` from `q50` (`src/data_reader.py:3348-3349, 3357-3358`).
- **Operational December Q1 is trimmed twice.**
  - The latest reader passes `end_year = today.year` to `read_monthly_forecasts` (`:3334, 3346`).
  - That reader trims by **target** year under flag ON (`:1401`), so Jan–Mar of next year are dropped
    before any derivation.
  - PP-064 A fixes only the direct-row trim (`:3413`).
- **Ensemble quantiles.** EM and Naive Mean use column-wise pandas means, which skip NaN members.
  Skilled Mean uses `np.average` (`src/ensemble_calculator.py:846-862`), which propagates NaN.
  - The **recalc builds its own ensembles** in `src/skill_metrics.py` (`:2756-2758`, `:2889-2891`,
    Skilled Mean `:3010-3028`), and they are persisted (`recalculate_skill_metrics.py:403`).
- **Skill with null quantiles already works.**
  - CRPS skips unusable distributions (`src/skill_metrics.py:1191-1198`).
  - PIT returns NaN (`:1219-1224`).
  - Point scoring uses `forecasted_discharge` (`:2721-2739`).
  - Groups below K=5 are dropped (`:2834-2849`).
- **Legacy QUARTER rows of the seven models exist** ("Dataset B": `date = valid_from`, `horizon_value` =
  quarter number; ~37k rows locally). They are calendar-shaped, so PP-064's window filter cannot reject them.

## Target behaviour

1. **Eligibility sets** (`src/model_names.py`). Add, keeping the season sets unchanged:
   - `QUARTERLY_NATIVE_MODELS` = LR_BASE, LR_SM
   - `QUARTERLY_DERIVED_MODELS` = the seven models
   - `QUARTERLY_SUPPORTED_MODELS` = both sets plus the ensembles
2. **Source rules** for every quarter **input** read: the two raw readers **and**
   `read_quarterly_combined_forecasts`.
   - Direct QUARTER rows are kept for `QUARTERLY_NATIVE_MODELS` and for the ensemble models.
   - Direct rows of `QUARTERLY_DERIVED_MODELS` are dropped: Dataset B, and the models' own persisted
     derived rows.
   - Derived rows are built from monthly forecasts only for `QUARTERLY_DERIVED_MODELS`.
   - Consequences:
     - the gap detector and the operational/maintenance merge-back see LR + ensembles + fresh derived rows;
     - legacy seven-model rows are never merged back or re-saved;
     - freshly derived rows are written through the unchanged writer.
3. **Derivation predicate.** A derived quarterly forecast for (station, model m, calendar quarter Q of
   year Y) exists iff there is **one issue date `d`** such that:
   - `d.day` = the configured quarter issue day;
   - the first month of Q is exactly `L` months after `d`'s month, where `L` = `quarter_horizon_value()`.
     Example: kghm L=1 → `d` in Dec/Mar/Jun/Sep. tjhm L=0 → `d` in Jan/Apr/Jul/Oct;
   - for each month k = 0, 1, 2 of Q, there is exactly one MONTH row of m for the station, issued on `d`,
     with `horizon_value = L + k` and `valid_from` = the 1st of Q's month k;
   - each row has a finite point value, resolved as `q` if finite, else `q50`, **before** the completeness
     check.

   The derived row has:
   - value = the unweighted mean of the three resolved values, written to `forecasted_discharge` (and `q`
     if that column is present);
   - **every quantile column null** (`q05`…`q95`, including `q50`);
   - `date = d`, `horizon_value = L`;
   - `valid_from`/`valid_to` = Q's calendar bounds;
   - `year = Y`, `quarter_in_year = Q`.

   Anything else is not derived; log the count at INFO.
4. **Read windows.**
   - **Operational (`read_latest_quarterly_forecasts`).** Next-year target months must survive, so a
     25 Dec issue yields Q1 of next year. The monthly read and the target-year trim at `:1401` must admit
     target year `today.year + 1` **for this caller**, while issue dates stay ≤ `forecast_date`.
     - Implement it either with an optional keyword on `read_monthly_forecasts` (default = current
       behaviour; other callers unaffected), or by reading and trimming locally.
     - Do not let future issues into "latest".
   - **Historical (`read_quarterly_forecasts`).** Read monthly issues from `start_year − 1`, derive, then
     trim derived rows to the requested target years.
5. **Output metadata.** Flag-OFF reader output keeps dropping `date`/`horizon_value`
   (`_quarterly_fc_output_cols`, `src/data_reader.py:83-94`), and flag-OFF skill keeps the hv sentinel 0.
   The derivation helper itself always returns `date` and `horizon_value = L`; flag-ON readers expose them.
6. **Quarterly Ensemble Mean and ensemble quantiles** (owner decision D10, 2026-09-25).
   - **EM membership**, per ensemble group, i.e. (station, year, quarter), plus `horizon_value` under
     flag ON:
     - **Candidates** = the raw quarter models with a finite point value in the group (LR_Base, LR_SM,
       and the seven derived models).
     - **More than 2 candidates → skill-gated with the long-term gate.** EM = the unweighted mean of the
       candidates that pass
       `filter_for_highly_skilled_forecasts(..., min_pairs=_long_term_min_pairs("QUARTER"), **_long_term_threshold_overrides())`
       (NSE > 0 only, **plus the quarter min-pairs floor**). This is the gate the long-term Skilled Mean
       already uses (`src/skill_metrics.py:108`; `src/ensemble_calculator.py:303-308`, `:672-675`). The
       operational and recalc paths must use the identical call, so that the same models qualify in both.
       - This requires at least 2 qualifying models, the existing multi-model rule.
       - **Assumption to confirm with the owner in the PR:** if fewer than 2 qualify, EM falls back to
         mean(LR_Base, LR_SM) when both are present, so EM still forms reliably (the goal of M1).
     - **2 or fewer candidates → unchanged M1 rule:** EM = mean(LR_Base, LR_SM), not skill-gated. Both
       must be present.
     - **Per group.** The candidate count and the qualification are evaluated **per group**, not globally
       (today's EM code counts models globally: `src/ensemble_calculator.py:744`, `src/skill_metrics.py:2748`).
     - Under flag ON, qualification uses the candidate's skill row **at the same lead**. Do not reuse the
       existing leadless fallback join (`src/ensemble_calculator.py:722-738`) for this rule. A candidate
       without a matching skill row does not qualify.
     - **Empty skill frame (interim).** Until PP-064 B5 is decided and implemented, an **entirely empty**
       quarter skill frame keeps today's behaviour: no quarterly ensembles at all
       (`postprocessing_operational_long_term.py:210`, `src/ensemble_calculator.py:632-634`). The fallback
       above applies only when the skill frame is non-empty but candidates lack qualifying rows.
     - **EM skill (recalc).** EM skill is computed per `(code, quarter_in_year[, horizon_value])` across
       years, **not per composition**. Today `src/skill_metrics.py:2784` groups EM skill by `composition`,
       which with varying membership would split the years and fail K=5, while the writer
       (`src/api_writer.py:680-683`) persists one row per key without composition.
       - Keep `composition` on the forecast rows.
       - Change the grouping for quarter only; season stays as is.
   - **Ensemble quantiles** (D10): quarter only, an ensemble's quantile columns are null **when any member
     of that ensemble's actual contributing pool has null quantiles**.
     - EM: its actual members. The derived models have no quantiles, so an EM that includes any of them
       has null quantiles; an EM that is only the LR pair keeps its quantiles.
     - Naive Mean: all raw models present.
     - Skilled Mean: the skill-qualified members only. A derived model excluded by the gate must not null
       an otherwise complete Skilled Mean.
   - Point values are unchanged.
   - Apply the rule in **both** the operational ensemble path (`src/ensemble_calculator.py`) and the recalc
     ensemble path (`src/skill_metrics.py`) before probabilistic scoring. Gate it on
     `period_col == "quarter_in_year"`, so season and monthly are untouched.

## Plan

### P1 — one code agent (after PP-064 Chunk A has merged)

**Files (only these may be modified)**:
- `src/model_names.py`: the new quarter sets
- `src/aggregation.py`: a new function, e.g.
  `derive_quarterly_from_monthly_same_issue(monthly_fc, quarter_lead, issue_day)`.
  **Keep `aggregate_monthly_fc_to_quarterly` and `QUARTER_MIN_MONTHS` unchanged**; observations and
  existing tests use them. If the old function ends up unused, leave its removal to a later cleanup.
- `src/data_reader.py`:
  - Source 1 of the two quarter readers
  - the quarter model filter in the two raw readers and in `read_quarterly_combined_forecasts`
  - the read-window handling from item 4 (an optional keyword on `read_monthly_forecasts` is allowed)
  - point-value resolution
- `src/ensemble_calculator.py` and `src/skill_metrics.py`: the quarterly EM membership and the quarter-only
  quantile rule (item 6). Both the operational path (`_create_aggregated_ensemble_forecasts`, EM block
  ~`:740-771`) and the recalc path (`_calculate_aggregated_skill_metrics`, EM block ~`:2744-2805`) must
  apply the same rule. Gate it on `period_col == "quarter_in_year"`; the season EM stays M1.
- Tests: new `tests/test_quarter_derived_models.py`, and the existing-test updates listed below

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* The only
permitted signature changes are new optional keywords with defaults equal to current behaviour. Do not
change:
- season behaviour, including the season EM
- EM membership beyond item 6
- `api_writer.py`
- `select_operational_issuances`
- PP-064's window validation
- the observation aggregation

`sandro_sapphire_2_quaterly_agg` may be consulted (`src/model_names.py:18-27`, `src/aggregation.py:247-304`),
but re-implement narrowly. In this plan, unlike the branch:
- the lead and issue day are the configured ones, not derived from dates;
- there is no date-derived hv overwrite;
- the EM gate is the long-term gate (NSE > 0), not the short-term thresholds, and applies only with more than 2 candidates;
- there is no flag-OFF lead stratification.

**Tests** (station `19999`; Arrange → Act → Assert; mock only the API boundary, never
`read_monthly_forecasts`):
1. **kghm positive.** GBT MONTH rows issued 2026-12-25 at hv 1/2/3 for Jan/Feb/Mar 2027 → one derived
   Q1 2027 row with:
   - value = the mean;
   - `date` 2026-12-25, hv 1 (checked on the helper and on the flag-ON reader);
   - window 2027-01-01..03-31;
   - all quantiles null, `forecasted_discharge` populated.
2. **tjhm positive.** Issue 2027-01-01 at hv 0/1/2 → Q1 2027, hv 0.
3. **Negative cases, each paired with a positive control in the same frame** (one eligible triplet next
   to the ineligible one; the control must survive):
   - (a) mixed issues (Jan+Feb from 12-25, Mar from 11-25);
   - (b) right months but wrong issue month (Jan/Feb/Mar dated 2026-11-25 carrying hv 1/2/3);
   - (c) wrong issue day (Jan/Feb/Mar dated 2026-12-20);
   - (d) only 2 of 3 months;
   - (e) one month with null `q` and null `q50`.
4. **Point value.** Finite `q` with null `q50` → derived from `q`. `q` ≠ `q50` → derived from `q`. Both
   readers, both flags.
5. **Operational December Q1**, through the real `read_latest_quarterly_forecasts` → `read_monthly_forecasts`
   chain at `forecast_date` 2026-12-25, flag ON and OFF: the Q1 2027 GBT row is present, and no row issued
   after 2026-12-25 is used. It fails on trunk.
6. **LR native-only.** LR_BASE MONTH triplets → no derived LR row; a native LR QUARTER row is still read.
7. **Persisted seven-model rows are ignored everywhere.** Use persisted GBT QUARTER rows **at the
   configured lead** (kghm hv1 / tjhm hv0, `date = valid_from`); the flag-OFF combined read already filters
   other hvs at the API (`src/data_reader.py:3610-3615`). Under both flags:
   - they are excluded by both raw readers (**preservation test**: this already passes on trunk);
   - they are excluded by `read_quarterly_combined_forecasts` (fails on trunk);
   - in `detect_missing_quarterly_ensembles` (`src/gap_detector.py:467-479`):
     - they create no phantom gap keys;
     - genuine ensemble presence is preserved;
     - a fresh monthly-derived row creates a genuine gap even when no direct QUARTER rows exist;
   - they are not merged back by the operational/maintenance paths.
8. **Ensembles, operational and recalc paths, both flags.** Q1 with LR_Base, LR_SM (quantiles present),
   GBT and MC_ALD (derived, null quantiles):
   - (i) All four have NSE > 0 → EM = mean of all four, quantiles **null**.
   - (ii) GBT NSE ≤ 0, the rest > 0 → EM = mean(LR_Base, LR_SM, MC_ALD), quantiles null.
   - (iii) Only LR_SM has NSE > 0 → fewer than 2 qualify → EM = mean(LR_Base, LR_SM), quantiles kept
     (the fallback assumption).
   - (iv) Only LR_Base and LR_SM present (2 candidates), both NSE < 0 → EM = their mean (M1, unchanged).
   - (v) A candidate with no skill row does not qualify.
   - Naive Mean = mean of all raw models present, quantiles **null**.
   - Skilled Mean with GBT gated out → quantiles kept if the remaining members all have quantiles; with
     GBT qualifying → null.
   - The same inputs through the recalc path give the same EM values as the operational path.
   - (vi) **Group isolation:** two groups in one frame, one with 2 candidates and one with 4, and a third
     at a different lead where the qualification is reversed. Each group's EM follows its own count and
     its own lead's skill.
   - (vii) **Min-pairs floor:** a candidate with NSE > 0 but `n_pairs` = K−1 does not qualify; at K it does.
     Operational and recalc agree.
   - (viii) **Empty vs partial skill:** an entirely empty skill frame → no quarterly ensembles (interim).
     A non-empty frame missing one candidate's row → that candidate does not qualify.
   - (ix) **EM skill across compositions (recalc):** nine target years whose EM compositions are 4/1/4
     years → one persisted EM skill row per `(code, quarter[, hv])` with `n_pairs` 9, and no erroneous
     tombstone.
9. **Season unchanged.** A GBT season row is still excluded; the season goldens pass.
10. **Skill.** A derived GBT Q2 row for ≥ 5 target years plus calendar observations → a GBT Q2 skill row
    with the expected `n_pairs` and point metrics, CRPS/PIT/sharpness null, and
    `sharpness_n_90 = sharpness_n_50 = 0`. With 4 years the group is suppressed (K=5).
    Keep the quarterly **skill** reader unfiltered by model (`src/data_reader.py:2814-2835`), so the stale
    comparison still sees the old seven-model skill.
11. **Mutation checks** (recorded in the PR): removing the issue-date grouping fails 3(a); removing the
    issue-month check fails 3(b); relaxing to 2 of 3 months fails 3(d).

**Existing tests** (each change states its reason in the PR):
- **Unchanged:**
  - the direct-row exclusion assertions for the seven models in `tests/test_quarterly_data_reader.py`
    (e.g. `:293-337`, `:765-810`); they stay valid;
  - `tests/test_aggregation.py` observation tests and `:41` (`QUARTER_MIN_MONTHS` is not changed here);
  - `tests/test_quarterly_api_writer.py:337-378` (the old aggregator stays);
  - `tests/test_lead_aware_empty_schedules.py:207, 239` (unsupported-quarter schedule behaviour);
  - the LR-only M1 EM tests: `test_lt_min_pairs_gate.py:592-617`, `test_quarterly_skill_metrics.py:265,529`.
  - the season EM tests, including the three-model **season** test at `test_quarterly_ensemble_creation.py:458`.
- **Changes meaning, keep the assertion:** `test_quarterly_ensemble_creation.py:203-227` has LR_Base, LR_SM
  and GBT with only GBT qualifying. Under the new rule, fewer than 2 qualify, so its expected LR mean holds
  **via the fallback**. Keep the assertion, relabel the test to say so, and mark it as depending on the
  fallback assumption (D10).
- **Expected to change:** tests whose fixtures build quarterly rows from monthly rows through the reader's
  Source 1 and assert the old mixed-issue / 2-of-3 / quantile-averaging results. The candidates are
  `tests/test_quarterly_data_reader.py:134, 407, 443, 654, 985, 1015` and
  `tests/test_quarterly_workflow_integration.py`. The agent lists each one it changes, with before/after.

**Acceptance**:
- Record the suite counts before editing.
- The new tests pass. Tests 5, 7 (combined reader), 8 and 3(a)/(b) fail on trunk.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`: zero failures, zero
  unexpected skips, only the pre-existing xfail.
- `git diff --stat` touches only the listed files.
- `ruff check` / `ruff format --check` are clean.

### P2 — rollout (part of PP-064 Chunk C)

- **Before the recalc:** per org, record QUARTER `skill_metrics` rows for the seven models and for EM by
  `horizon_value` and `horizon_in_year`, with a private value snapshot.
- **EM forecast rows after the recalc.** Verify persisted quarterly EM values, composition and null
  quantiles.
  - The gap detector treats any existing EM as complete regardless of membership
    (`src/gap_detector.py:467-479`).
  - Stale tombstones invalidate **skill**, not forecast rows (`src/stale_tombstones.py:221-238`).
  - So a group that can no longer emit EM keeps its old EM forecast row. That is PP-041/PP-063's
    territory; record the count, do not claim it is repaired.
- **After the recalc, verify the four outcomes** (`src/stale_tombstones.py:29-41, 167-179, 221-228`):
  - kghm flag ON: old hv0 rows tombstoned, new hv1 rows emitted;
  - tjhm flag ON: hv0 rows replaced;
  - flag OFF: sentinel hv0 rows replaced;
  - groups failing eligibility or K are tombstoned.

  Compare values, not only counts.
- **Operational:** on the next quarter issue day, derived rows for the seven models appear for the new
  quarter.
- **DB rows:** the recalc writes derived raw rows with `flag=0` (PP-061), even though their inputs are
  hindcasts; this is known and not fixed here.
- **Dashboard:** the dashboard reads persisted QUARTER rows directly (`apps/forecast_dashboard/src/db.py:817-873`),
  so legacy seven-model rows can still appear there for older quarters.
  - FD-029's per-target-quarter selection prefers rows with `date < valid_from`. That separates fresh
    derived rows from Dataset B **only for kghm under flag ON**.
  - Under flag OFF the writer dates every quarter row `valid_from` (`src/api_writer.py:1199-1204`), and
    tjhm's day-1 / lead-0 issue equals `valid_from` anyway.
  - It is not a general provenance discriminator. Deleting Dataset B (D8) is the real fix.

## Out of scope

- Season re-enablement.
- Short-term-threshold gating of the quarterly EM; any change to the season EM; averaging quantiles into derived quarters.
- Day-weighting (PP-064 B4).
- Deleting Dataset B rows (D8).
