# PP-065: Seven models for quarter as same-issue monthly averages; quarterly ensembles as for monthly

**Status**: Draft (2026-09-26, rev 3 after the 9-reviewer round)
**Module**: `apps/postprocessing_forecasts`
**Priority**: High
**Labels**: `postprocessing_forecasts`, `long-term`, `quarter`, `ensembles`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only.
**Related**:
- PP-064: calendar-window validation, native-row rule, B2 dedup. This plan comes after its Chunk A.
- LTF-016: fix the monthly window labels upstream.
- PP-056: superseded for the seven models by this plan (see DOC-009).
- PP-020 (averaging quantiles), PP-061 (writer `flag=0`), GitHub #521.

Paths are relative to `apps/postprocessing_forecasts/`. Citations are to trunk `82946683`.

## Owner decisions this plan implements (2026-09-25/26)

1. **Seven models re-enabled for quarter.** `GBT`, `LR_SM_DT`, `LR_SM_ROF`, `MC_ALD`, `SM_GBT`, `SM_GBT_LR`
   and `SM_GBT_NORM` come back for **quarter**.
   - Each quarterly forecast is the average of the model's own monthly forecasts **produced on the same
     issue date as the quarter forecast**, for the **next three months**: monthly leads `L`, `L+1`, `L+2`,
     where `L` is the configured quarter lead.
   - Months are identified by (issue date, `horizon_value`), not by the stored `valid_from`. Stored windows
     can be offset (tjhm hindcasts) or mislabelled (the kghm GBT family's January year). LTF-016 fixes
     those labels upstream.
2. **LR native-only, with a temporary fallback.** LR_Base and LR_SM are native quarter models.
   - **Until LTF-014 P0 and P2 are deployed on both orgs**, a (code, year, quarter) with **no native LR row**
     still gets LR_Base/LR_SM derived from their monthly forecasts by the same rule.
   - The fallback is removed in P3.
3. **Quarterly ensembles are computed the same way as monthly ensembles.** This replaces the fixed-LR "M1"
   EM, for quarter only.
   - **Naive Mean** = the average of all raw quarter models, with no skill gate.
   - **Skilled Mean** = the models that beat climatology. It uses the long-term gate (NSE > 0,
     `_long_term_threshold_overrides()`) plus the quarter min-pairs, **inverse-MAE weighted**, exactly as
     monthly (`src/ensemble_calculator.py:303-310`, `:358-...`).
   - **Ensemble Mean** = as monthly: the **default** threshold gate plus min-pairs (`:289-292`), an
     unweighted mean of the qualifying models, requiring more than one. It may not form when fewer than two
     models pass; that is accepted.
4. **Quarter min-pairs K = 10.**
   - The default of `ieasyhydroforecast_min_pairs_long_term_quarter` goes from 5 to 10
     (`src/skill_metrics.py:201-204`).
   - K is shared by the ensemble gates and by skill-row suppression (`:2834-2849`).
5. **Bounds for rows without quantiles.**
   - Derived rows have null quantiles.
   - An ensemble quantile column is null when any contributing member lacks that column: a per-column,
     NaN-propagating mean.
   - The **displayed** bounds for such rows use the delta method (forecast ∓ δ from the quarter skill row),
     as short-term does. That is done in FD-029/FD-030, not here.
6. **Unchanged:**
   - season (model set and ensembles);
   - `horizon_value` = the configured quarter lead (kghm 1, tjhm 0).

## Feasibility (verified; re-measure per server)

**Monthly modes.**
- Both orgs run all nine models in `month_1`/`month_2`/`month_3`, with no `forecast_months` restriction, on
  the quarter's issue day:
  - kghm: day 25, leads 1/2/3;
  - tjhm: day 1, leads 0/1/2.
- kghm `month_0` (day 10) never takes part.
- Compare model names **canonically, upper-case**. The data repos spell it `SM_GBT_Norm`.

**History for skill under decision 1** (local dev DB, both orgs mixed), matching by (issue date, hv) at the
calendar-quarter issue dates:

| Org | Triplets across the seven models |
|---|---|
| kghm | ~27.8k |
| tjhm | ~5.2k |

An exact-`valid_from` predicate would have left tjhm with ~26, and the kghm GBT family with 39 for Q1.

**Derived vs native (kghm Q2–Q4, same stations and years).**
- Derived LR_Base median NSE: 0.25 / 0.50 / 0.44.
- Native LR_Base median NSE: 0.19 / 0.43 / 0.32.
- GBT, SM_GBT and SM_GBT_NORM are weak in Q2 (median ≈ 0).

**Quarterly n_pairs today.**
- kghm median 12–19; tjhm median 5–6.
- With K = 10, many tjhm quarter skill rows may be suppressed until more history is scored. P2 measures
  this.

## What trunk does today (verified)

**Monthly input.**
- Under flag ON it goes through `select_operational_issuances`. That **collapses duplicates and overwrites
  the stored `horizon_value` with a date-derived lead** (`src/data_reader.py:385-395`).
- Normalization turns a null lead into 0 (`:1503`).
- So the derivation must see the **raw** monthly rows.

**Quarter readers.**
- The model filter runs **after** the sources are concatenated and `drop_duplicates(keep="last")` is
  applied (`:3144-3162`, `:3425-3437`). A persisted direct GBT row can therefore replace a fresh derived
  row.
- The combined reader `read_quarterly_combined_forecasts` (`:3591-3618`) applies no model filter.
- Maintenance takes its **whole gap universe** from that reader (`postprocessing_maintenance_long_term.py:295-309`).

**Ensembles.**
- EM counts models globally (`src/ensemble_calculator.py:744`, `src/skill_metrics.py:2748`).
- The recalc groups EM, Naive Mean and Skilled Mean skill **by composition** (`src/skill_metrics.py:2784`,
  `~2922`, `~3056`).
- The writer persists one row per key without composition (`src/api_writer.py:680-683`). A null
  composition triggers a warning (`:587-594`).

**Quantiles.**
- LR QUARTER rows have `q50` null in every row, but q05–q95 present.
- Six of the seven models have no monthly quantiles; MC_ALD has them.

**Config.**
- `quarter_horizon_value()` raises if `quarter` is not a supported mode
  (`apps/iEasyHydroForecast/long_term_horizon_resolver.py:68-80`).
- The issue day comes from `operational_schedule_for_mode("quarter").issue_day` (`:112-142`).

## Target behaviour

1. **Derivation helper.** A pure function in `src/aggregation.py`, e.g.
   `derive_quarterly_from_monthly_same_issue(monthly_raw, lead, issue_day, models)`.
   - **Input:** raw monthly rows with the stored `date` and `horizon_value`, taken **before** any
     operational selection or lead rewriting.
   - **Excluded (and counted):**
     - rows with a null or non-integer stored `horizon_value`;
     - rows whose `date.day` ≠ `issue_day`.
   - **Target month** = issue month + `horizon_value`, year-aware.
   - **Triplet:** for each (code, model, issue date `d`) whose month is Q's first month − `L`, the rows with
     `horizon_value` = `L`, `L+1`, `L+2` must all exist.
   - **Duplicates** at the same (code, model, `d`, hv): prefer the row whose `valid_from` month equals the
     target month; otherwise skip the triplet and count it.
   - **Point value** per month = `q` if finite, else `q50`. All three must be finite.
   - **Output row:**
     - value = the unweighted mean, the same weighting as quarterly observations (PP-064 B4). Write it to
       `forecasted_discharge`, and to `q` if that column exists;
     - all quantile columns null;
     - `date = d`, `horizon_value = L`;
     - `valid_from`/`valid_to` = Q's calendar bounds;
     - `year`, `quarter_in_year`.
   - **Logging:** INFO counts per exclusion reason; no station codes.
2. **Readers** (`read_quarterly_forecasts`, `read_latest_quarterly_forecasts`).
   - Read raw monthly rows via `_read_long_forecasts_api` for issue years `start_year − 1 … end_year`. In
     the latest reader, also require issue date ≤ `forecast_date`.
   - Derive for `QUARTERLY_DERIVED_MODELS`. While the fallback is active, also derive LR where no native LR
     row exists.
   - Trim to the requested **target** years. In the latest reader, target year `today.year + 1` is allowed,
     so a 25 Dec issue yields next year's Q1.
   - **Drop direct rows of `QUARTERLY_DERIVED_MODELS` before the sources are combined.** Direct LR rows go
     through PP-064's native-row rule.
   - Do this **only after the existing schedule guard** (`:3100-3108`). With no quarter mode configured
     (e.g. uzb), do not derive and do not raise.
3. **Writer: stop writing raw LR rows (PP-064 "B6", required here).**
   - The quarter branch of `_write_aggregated_forecasts_to_api` skips `LR_BASE`/`LR_SM` rows. It keeps
     writing the seven derived models and the ensembles.
   - **Why required:** fallback-derived LR rows are native-shaped (`date = d`, hv = `L`). Once persisted, they
     would pass the native-row rule forever. Native LR rows are owned by the LT module.
   - **Consequences:** this changes flag-OFF output (fewer rows written), and flag-OFF rewrites of LR rows
     (population b) stop.
   - **Log** one aggregated skip count per call.
4. **Combined reader and maintenance.**
   - `read_quarterly_combined_forecasts` drops direct rows of the seven models. It stays **filter only**,
     with no monthly read.
   - Before calling `detect_missing_quarterly_ensembles`, maintenance adds to the gap universe the
     **freshly derived keys** for the years it already reads for gap-fill
     (`postprocessing_maintenance_long_term.py:305-309`). A quarter that exists only as monthly-derived rows
     is then detected.
   - Quarters older than that window are covered by the recalc, not by maintenance. This is accepted.
5. **Ensembles** (decision 3), quarter only, identical in the operational path
   (`_create_aggregated_ensemble_forecasts`) and the recalc path (`_calculate_aggregated_skill_metrics`).
   - Use **one shared pure helper** for membership and quantile nulling, imported by both paths.
   - Evaluate the gates **per group**, (code, year, quarter[, `horizon_value`]), and per lead under flag ON.
     Do not use the leadless fallback join (`src/ensemble_calculator.py:722-738`).
   - Skill source: the operational path uses the stored skill. The recalc path uses its own step-2
     `skill_stats` before the K filter, as Skilled Mean already does.
   - Season keeps its current rules.
6. **Ensemble skill.** For quarter only, EM, Naive Mean and Skilled Mean skill are grouped by
   (code, quarter_in_year[, hv]) across years, **not by composition**.
   - The skill row carries `composition` as a stable, non-null label, e.g. the sorted union of the members
     seen.
   - Forecast rows keep their per-year composition.
7. **K = 10** for quarter (decision 4).

## Plan: four agent phases, then rollout

**Agent instruction (every phase):** *"Do NOT change any existing function signatures, data flow logic, or
control flow. Your changes must be purely additive or modify only the specific behavior described."*
- The only permitted signature changes are new optional keywords whose defaults equal current behaviour.
- Do not change season behaviour, `select_operational_issuances` or PP-064's window validation.
- In `api_writer.py`, change only the LR skip (item 3).
- `sandro_sapphire_2_quaterly_agg` may be consulted but not cherry-picked.

### P1a — model sets and derivation helper

**Files:**
- `src/model_names.py`: the quarter sets. The season sets are unchanged.
- `src/aggregation.py`: the new helper. Leave `aggregate_monthly_fc_to_quarterly` and `QUARTER_MIN_MONTHS`
  unchanged.
- New `tests/test_quarter_derived_models.py`.

**Tests on the helper** (station `19999`):
1. kghm, `d` = 2026-12-25, hv 1/2/3 → Q1 2027, `date` = `d`, hv 1, window 2027-01-01..03-31, null quantiles.
2. tjhm, `d` = 2027-01-01, hv 0/1/2 → Q1 2027, hv 0.
3. These are still derived:
   - offset windows (`valid_from` on 01-02, 02-01 and 03-03);
   - a GBT January row labelled with the issue year.
4. Negatives, each with an eligible control in the same frame:
   - wrong issue day;
   - issue month ≠ Q's start − `L`;
   - missing lead;
   - null or NaN point value;
   - null stored hv;
   - ambiguous duplicate.
5. `q` is preferred over `q50`.

**Mutations to record in the PR:**
- drop the issue-day check → the wrong-day negative fails;
- drop the issue-month check → the wrong-month negative fails;
- accept 2 of 3 leads → the missing-lead negative fails.

### P1b — reader wiring

Depends on P1a. It can run in parallel with P1c; the two touch disjoint files.

**Files:**
- `src/data_reader.py`: the two quarter readers, the combined-reader filter, and the raw monthly read.
- `postprocessing_maintenance_long_term.py`: the gap universe.
- `src/api_writer.py`: the LR skip (item 3) only.
- Tests.

**Tests** (mock only the API boundary; both flags):
- December Q1 at `forecast_date` 2026-12-25, through the real `read_latest_quarterly_forecasts`. It fails on
  trunk. Mutation: removing the target-year extension makes the flag-ON case fail.
- `forecast_date` 2026-09-25 with Dec-25 rows present → Q4, not Q1.
- A fresh derived GBT row **survives** next to a persisted GBT QUARTER row with the same key.
- Rows shaped like Dataset B for GBT are dropped by all three readers.
- Maintenance orchestration, with complete monthly triplets and no persisted QUARTER rows → the gap is
  detected and the ensembles are written.
- LR fallback: with no native row, a derived LR row appears. With a native row present, the native row wins;
  the fallback never overrides it.
- uzb-like, with no quarter mode: no derivation, no exception, and
  `tests/test_lead_aware_empty_schedules.py:207,239` unchanged.
- **Writer:**
  - a raw LR row reaches the quarter writer → no record, plus the skip count;
  - a derived GBT row or an ensemble row → a record;
  - under flag OFF, a fallback-derived LR row is never persisted, so the next read can never treat it as
    native.

**Existing tests expected to change** (list each in the PR, with before/after):
- `tests/test_quarterly_data_reader.py:134, 245, 407, 443, 654, 718, 985, 1015`, where they assert the old
  mixed-issue or 2-of-3 monthly aggregation.
- Keep the direct-row exclusion assertions for the seven models (e.g. `:293-337`, `:765-810`).
- `tests/test_quarterly_workflow_integration.py` calls the old aggregator directly and stays unchanged.
- Writer tests that write raw LR quarter rows change: `tests/test_quarterly_api_writer.py:64, 89, 285`.

### P1c — ensembles, ensemble skill, K

Depends on P1a.

**Files:** `src/ensemble_calculator.py`, `src/skill_metrics.py` (the shared helper, the composition-free
quarter grouping, the K default), tests.

**Tests:**
- On the shared helper, with a skill frame:
  - Naive Mean = all members;
  - Skilled Mean = the NSE > 0 members with n_pairs ≥ K, 1/MAE-weighted;
  - EM = the default-gate members, requiring more than one;
  - per-group and per-lead isolation;
  - K−1 / K boundaries.
- Quantile nulling is per column. An LR-only EM with `q50` null keeps q05–q95. Adding a derived member nulls
  all of its quantile columns.
- Recalc path: build observations that give the intended NSE signs. Nine target years with compositions
  4/1/4 → one persisted row per ensemble per (code, quarter[, hv]), `n_pairs` 9, and no erroneous tombstone.

**Existing tests expected to change** (owner decision, "same as monthly"):
- The fixed-LR EM tests: `test_lt_min_pairs_gate.py:592-617`, `test_quarterly_ensemble_creation.py:203-227`,
  `test_quarterly_skill_metrics.py:265, 529`.
- Any test asserting the quarter K default of 5.
- Season EM tests stay unchanged, including `test_quarterly_ensemble_creation.py:458`.

### P1d — end-to-end

Depends on P1b and P1c. Tests only.
- One December-Q1 chain (raw monthly rows → reader → ensembles → skill), both flags.
- Full suite: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` gives zero
  failures and zero unexpected skips; only the pre-existing xfail remains.
- `ruff check` and `ruff format --check` are clean.

### P2 — rollout (with PP-064 Chunk C)

**Deploy PP-065 and run the recalc together.** In between, the gates would run on the old, contaminated
quarter skill.

**Before the recalc**, per org:
- a private export of the QUARTER `long_forecasts` and `skill_metrics` rows;
- a count of rule-A triplets per model × quarter.

**After the recalc**, check:
- the four tombstone outcomes:
  - kghm flag ON: hv0 → hv1;
  - tjhm: hv0 replaced;
  - flag OFF: sentinel replaced;
  - sub-K groups tombstoned;
- the quarter skill rows suppressed by K = 10, per org;
- the persisted ensemble values and compositions;
- the Dataset B rows overwritten by fresh derived rows with the same key under flag OFF. This upsert is
  irreversible, which is why the export is taken first.

**Operationally:** on the next quarter issue day, derived rows appear on the dashboard with δ bounds
(FD-029).

### P3 — remove the LR fallback

Only after LTF-014 P0 and P2 are deployed on both orgs.

**Files:** `src/data_reader.py` (drop the LR fallback branch), tests.

**Acceptance:** before removing it, count per org the quarters that would lose LR rows. For the calendar
quarters of scored years, expect none.

## Out of scope

- Season.
- Fixing the labels upstream (LTF-016).
- Displaying δ bounds (FD-029/FD-030).
- Deleting legacy rows (D8).
- Any writer change beyond the LR skip.
