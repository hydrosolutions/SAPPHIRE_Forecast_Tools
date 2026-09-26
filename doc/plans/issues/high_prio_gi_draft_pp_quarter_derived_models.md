# PP-065: Seven models for quarter as same-issue monthly averages; quarterly Naive/Skilled Mean as for monthly

**Status**: Draft (2026-09-26, rev 4 after the second review round)
**Module**: `apps/postprocessing_forecasts`
**Priority**: High. On the 2026-12-25 critical path (round-2 decision 5): the LR fallback guarantees a kghm
Q1 even without LTF-014 P0.
**Labels**: `postprocessing_forecasts`, `long-term`, `quarter`, `ensembles`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only.
**Related**:
- PP-064: calendar-window validation and the native-row rule (its Contract). This plan comes after its
  Chunk A and absorbs its rev-3 rules B2, B4 and B6.
- LTF-016: fix the monthly window labels upstream.
- PP-056: superseded for the seven models by this plan (see DOC-009).
- PP-059 (its "KEEP quarter EM" is superseded; DOC-009 adds the note).
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
   - **Until LTF-014 P0 and P2 are deployed on both orgs**, a (code, year, quarter, model) with **no native
     LR row** still gets that LR model derived from its monthly forecasts by the same rule.
   - Fallback-derived LR rows are **not persisted**; they feed the ensembles and skill only (round-2
     decision 3).
   - The fallback is removed in P3.
3. **Quarterly ensembles = Naive Mean + Skilled Mean only; no quarterly Ensemble Mean** (round-2 decision
   1). This replaces the fixed-LR "M1" EM, for quarter only, and matches monthly (PP-059).
   - **Naive Mean** = the unweighted average of all raw quarter models, with no skill gate.
   - **Skilled Mean** = the models that beat climatology: the long-term gate (NSE > 0,
     `_long_term_threshold_overrides()`) plus the quarter min-pairs K, **inverse-MAE weighted**, as monthly
     (`src/ensemble_calculator.py:303-310`, `_add_skilled_mean_monthly` `:378-481`).
   - The quarterly EM is **no longer produced** on the operational path or the recalc path. Existing
     quarter EM skill rows are tombstoned by the recalc (`recalculate_skill_metrics.py:410-430`). Old
     persisted EM / Naive / Skilled Mean forecast rows stay (round-2 decision 2).
   - Gap detection keys on Naive Mean. A Skilled Mean that does not form is not a gap.
4. **Quarter min-pairs K = 10.**
   - The default of `ieasyhydroforecast_min_pairs_long_term_quarter` goes from 5 to 10
     (`src/skill_metrics.py:201-204`).
   - K is shared by the Skilled Mean gate and by skill-row suppression (`:2834-2849`).
5. **Bounds for rows without quantiles.**
   - Derived rows have null quantiles.
   - An ensemble quantile column is null when any contributing member lacks that column: a per-column,
     NaN-propagating mean.
   - The **displayed** bounds for such rows use the delta method (forecast ∓ δ from the quarter skill row),
     as short-term does. That is done in FD-029/FD-030, not here.
6. **Unchanged:**
   - season (model set, ensembles including season EM);
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
- `_read_long_forecasts_api` drops every all-null column per batch (`:1468`), so `horizon_value`, `q` or
  `q50` can be missing from the frame.
- So the derivation must see the **raw** monthly rows and tolerate missing columns.

**Quarter readers.**
- `read_quarterly_forecasts` (`:3044`) and `read_latest_quarterly_forecasts` (`:3305`).
- Under flag ON they call `select_operational_issuances` (`:3131`, `:3407`) with the default
  `lead_output_cols=("horizon_value",)` (`:231`), which overwrites the stored hv with the derived lead
  (`:346-349`, `:393-395`).
- Under flag OFF the direct read is API-filtered to `horizon_value = quarter_horizon_value()` (`:3126`,
  `:3402`); that call raises if `quarter` is not a supported mode. The flag-ON schedule guard (`:3100-3109`,
  `:3376-3385`) returns empty with a WARNING instead.
- `_normalize_combined_forecasts` leaves quarter `date` unparsed (it parses `date` for season only,
  `:3770-3771`) and drops `horizon_value` under flag OFF (`:3786-3791`).
- `_quarterly_fc_output_cols` drops `date` and `horizon_value` from the output under flag OFF (`:83-94`;
  projections at `:3167`, `:3442`).
- The model filter runs **after** the sources are concatenated and `drop_duplicates(keep="last")` is
  applied (`:3144-3162`, `:3425-3437`). A persisted direct GBT row can therefore replace a fresh derived
  row.
- The combined reader `read_quarterly_combined_forecasts` (`:3591-3620`) applies no model filter.

**Shared quarter/season constants.** `AGGREGATED_EM_RAW_MODELS` and `AGGREGATED_SUPPORTED_MODELS`
(`src/model_names.py:14-16`) are used by season too: `src/data_reader.py:103` (season readers `:3284`,
`:3561`), `src/ensemble_calculator.py:741`, `src/skill_metrics.py:2745`.

**Ensembles.**
- Quarter EM = fixed LR pair (`src/ensemble_calculator.py:741-744`; recalc `src/skill_metrics.py:2745-2748`).
- The recalc groups EM, Naive Mean and Skilled Mean skill **by composition** (`src/skill_metrics.py:2784`,
  `:2922`, `:3056`).
- Flag-OFF quarter skill is grouped without hv (`src/skill_metrics.py:2642-2647`) and written at hv 0
  (`src/api_writer.py:666-669`); `read_quarterly_skill_metrics` passes it through unchanged
  (`src/data_reader.py:2814-2837`, `:2943-2967`). Monthly keys the Skilled Mean on hv whenever both frames
  carry it (`src/ensemble_calculator.py:403`).
- The writer persists one skill row per key without composition (`src/api_writer.py:680-683`).

**Maintenance** (`postprocessing_maintenance_long_term.py`).
- The quarterly gap-fill is reached only if the monthly block did not exit first (`sys.exit(0)` after the
  checks at `:110`, `:123`, `:138`, `:156`, `:181`, `:254`).
- It is skipped when `q_combined` is empty (`:296`).
- The gap universe is `q_combined` only, and `q_years` comes from `q_gaps` (`:305-310`): a quarter that
  exists only as monthly-derived rows is never detected (circular).
- The detector is called with `{"EM", "Skilled Mean", "Naive Mean"}` (`:297-301`); its default is `{"EM"}`
  (`src/gap_detector.py:370-389`). Stored ensemble names are the service enum values `"Naive Mean"` /
  `"Skilled Mean"` (`api_writer.py:47-48`).
- There is no `forecast_date`; nothing in the quarterly block depends on today's date.

**Quantiles.**
- LR QUARTER rows have `q50` null in every row, but q05–q95 present.
- In the local DB, MC_ALD MONTH rows carry quantiles, and `LR_SM_DT` / `LR_SM_ROF` MONTH rows carry q25 in
  about 50% of rows. Derived rows null their quantiles regardless.

**Config.** `quarter_horizon_value()` and `operational_schedule_for_mode("quarter")` raise
`UnsupportedLongTermModeError` or `LongTermHorizonResolverError`
(`apps/iEasyHydroForecast/long_term_horizon_resolver.py:25-31, 68-80, 112-142, 158-178`).

## Target behaviour

1. **Derivation helper.** A pure function in `src/aggregation.py`, e.g.
   `derive_quarterly_from_monthly_same_issue(monthly_raw, lead, issue_day, models)`.
   - **Input:** raw monthly rows with the stored `date` and `horizon_value`, taken **before** any
     operational selection or lead rewriting. `horizon_value`, `q` and `q50` columns may be absent.
   - **Excluded (and counted):**
     - rows with a null or non-integer stored `horizon_value`, or no `horizon_value` column;
     - rows whose `date.day` ≠ `issue_day`.
   - **Target month** = issue month + `horizon_value`, year-aware.
   - **Triplet:** for each (code, model, issue date `d`) whose month is Q's first month − `L`, the rows with
     `horizon_value` = `L`, `L+1`, `L+2` must all exist.
   - **Duplicates** at the same (code, model, `d`, hv): prefer the row whose `valid_from` (year, month)
     equals the target (year, month); otherwise skip the triplet and count it.
   - **Point value** per month = `q` if the column exists and the value is finite, else `q50`. All three
     must be finite.
   - **Output row:**
     - value = the unweighted mean, the same weighting as quarterly observations. Write it to
       `forecasted_discharge`, and to `q` if that column exists;
     - all quantile columns null;
     - `date = d`, `horizon_value = L`;
     - `valid_from`/`valid_to` = Q's calendar bounds;
     - `year`, `quarter_in_year`.
   - **Logging:** INFO counts per exclusion reason; no station codes.
2. **Readers** (`read_quarterly_forecasts`, `read_latest_quarterly_forecasts`).
   - **Direct rows, native-row selection.** One shared helper, used by both readers under **both** flags:
     it parses `date` (quarter `date` arrives unparsed) and applies PP-064's Contract rule (`date.day` ==
     quarter `issue_day` and year-aware lead == `lead_time`) **to LR rows only**. Non-native LR rows
     (rewrites, persisted monthly-derived rows) are never selected.
   - **Stored leads (flag ON).** Before `select_operational_issuances`, drop and count direct rows whose
     stored `horizon_value` differs from the derived lead, then call it with `lead_output_cols=()` so the
     stored value is preserved. `select_operational_issuances` itself is not modified.
   - **Derived rows.** Read raw monthly rows via `_read_long_forecasts_api` for issue years
     `start_year − 1 … end_year`. In the latest reader, also require issue date ≤ `forecast_date`. Derive
     for the seven models; while the fallback is active, also derive each LR model for (code, year,
     quarter) keys with no selected native row of that model.
   - Trim to the requested **target** years. In the latest reader, target year `today.year + 1` is allowed,
     so a 25 Dec issue yields next year's Q1.
   - **Drop direct rows of the seven models before the sources are combined.** After this, the readers'
     `keep="last"` dedup has no LR or derived-model collision left to resolve.
   - **Output schema.** Keep `date` and `horizon_value` in the reader output under **both** flags (an
     intended change to `_quarterly_fc_output_cols`' flag-OFF branch). `horizon_value` may be null on
     flag-OFF direct rows; no flag-OFF consumer keys on it. The writer's flag-OFF convention
     (`record_date = valid_from`, hv = `quarter_horizon_value()`, `api_writer.py:1172-1175, 1199-1204`) is
     unchanged.
   - **Missing quarter config, both flags.** On `UnsupportedLongTermModeError` or
     `LongTermHorizonResolverError` from the derivation's own config lookups, skip the derivation with a
     WARNING (e.g. uzb). Leave the existing direct-path behaviour unchanged: the flag-ON guard still returns
     empty, and the flag-OFF `quarter_horizon_value()` call still raises. A missing config file
     (`FileNotFoundError`, `long_term_horizon_resolver.py:184`) is a misconfiguration and propagates.
3. **Writer: stop writing raw LR rows (rev-3 PP-064 "B6").**
   - The quarter branch of `_write_aggregated_forecasts_to_api` skips `LR_BASE`/`LR_SM` rows. It keeps
     writing the seven derived models and the ensembles.
   - **Why required:** fallback-derived LR rows are native-shaped (`date = d`, hv = `L`). Once persisted, they
     would pass the native-row rule forever. Native LR rows are owned by the LT module.
   - **Consequences:**
     - flag-OFF output changes (fewer rows written), and flag-OFF rewrites of LR rows (population b) stop;
     - **fallback LR is invisible (accepted, round-2 decision 3):** the dashboard card and the bulletin
       read the DB, so they show no LR row for fallback quarters until LTF-014 P0/P2. LR still enters the
       ensembles and skill.
   - **Log** one aggregated skip count per call.
4. **Combined reader and maintenance.**
   - `read_quarterly_combined_forecasts` drops direct rows of the seven models. It stays **filter only**,
     with no monthly read.
   - **Gap detection** (caller only; `src/gap_detector.py` is not edited): pass
     `ensemble_models={"Naive Mean"}` at `postprocessing_maintenance_long_term.py:297-301`. Before the call,
     drop keys with fewer than two distinct raw models: a single-model group never forms a Naive Mean
     (`src/ensemble_calculator.py:915`), so it would be a perpetual gap.
   - **Gap universe.** Capture `forecast_date = dt.date.today()` **once** at the maintenance entry point
     (Forecast Date Rule). Add to the universe the keys of `data_reader.read_quarterly_forecasts(codes,
     Y − 1, Y + 1)` with `Y = forecast_date.year` (the `+ 1` covers a December-issued Q1). Use that existing
     reader function, not a new `data_reader` entry point.
   - **Allowed restructuring:** the `q_combined.empty` guard (`:296`) becomes "the universe is empty", and
     the quarterly block is reached when the monthly block has nothing to do (the monthly early exits no
     longer end the run before it). Keep the `q_skill` guard (`:304`). Season gap-fill reachability stays
     as today (it runs only when the monthly block completed); changing that is out of scope.
   - Quarters older than the window are covered by the recalc, not by maintenance. Maintenance writes only
     ensemble rows (`:316-321`); derived raw rows of a missed operational run come from the next recalc.
5. **Ensembles** (decision 3), quarter only, identical in the operational path
   (`_create_aggregated_ensemble_forecasts`) and the recalc path (`_calculate_aggregated_skill_metrics`).
   - Use **one shared pure helper** for membership and quantile nulling, imported by both paths. Branch on
     `period_col == "quarter_in_year"`; the season branch keeps today's code, including its EM.
   - Naive Mean over all raw quarter models; Skilled Mean over the NSE > 0 members with `n_pairs` ≥ K,
     1/MAE-weighted. Both keep the existing "more than one member" rule. No EM.
   - Evaluate per group, (code, year, quarter), plus `horizon_value` **only under flag ON**. Gate the hv key
     on the flag, never on column presence: flag-OFF frames now carry hv (item 2) while flag-OFF skill sits
     at hv 0. Do not use the leadless fallback join (`src/ensemble_calculator.py:722-738`).
   - Keep the empty-skill early return (`:632-634`) and the operational skip (`postprocessing_operational_long_term.py:210`):
     no ensembles at all on empty skill, as monthly (PP-064 B5).
   - Skill source: the operational path uses the stored skill. The recalc path uses its own step-2
     `skill_stats` before the K filter, as Skilled Mean already does.
6. **Ensemble skill.** For quarter only, Naive Mean and Skilled Mean skill are grouped by
   (code, quarter_in_year[, hv under flag ON]) across years, **not by composition**.
   - The skill row carries `composition` as a stable, non-null label, e.g. the sorted union of the members
     seen.
   - Forecast rows keep their per-year composition.
7. **K = 10** for quarter (decision 4).
8. **Observations: 3 of 3 months** (rev-3 PP-064 "B4"). Add `QUARTER_OBS_MIN_MONTHS = 3`, used only at
   `src/aggregation.py:125-126`, unweighted. This also changes δ (`:132-141`), which is computed from the
   surviving years. `QUARTER_MIN_MONTHS` (`:38`) is unchanged.

## Plan: four agent phases, then rollout

**Agent instruction (every phase):** *"Do NOT change any existing function signatures, data flow logic, or
control flow. Your changes must be purely additive or modify only the specific behavior described."*
- The only permitted signature changes are new optional keywords whose defaults equal current behaviour.
- The permitted control-flow changes are the ones this plan names: the maintenance restructuring (item 4)
  and the reader output schema (item 2).
- Do not change season behaviour, `select_operational_issuances`, `src/gap_detector.py` or PP-064's window
  validation.
- Add **new quarter-only constants** in `src/model_names.py` (e.g. `QUARTER_NATIVE_RAW_MODELS`,
  `QUARTERLY_DERIVED_MODELS`, `QUARTER_SUPPORTED_MODELS`). Do not modify `AGGREGATED_EM_RAW_MODELS` or
  `AGGREGATED_SUPPORTED_MODELS`; code shared with season branches on `period_col` or the horizon.
- In `api_writer.py`, change only the LR skip (item 3).
- `sandro_sapphire_2_quaterly_agg` may be consulted but not cherry-picked. Never `git stash`.
- Every edited existing test is listed in the PR with its reason (before/after).

### P1a — constants, derivation helper, observation coverage

**Files:**
- `src/model_names.py`: the new quarter-only constants.
- `src/aggregation.py`: the new helper and `QUARTER_OBS_MIN_MONTHS` (item 8). Leave
  `aggregate_monthly_fc_to_quarterly` and `QUARTER_MIN_MONTHS` unchanged.
- New `tests/test_quarter_derived_models.py`; `tests/test_aggregation.py` (observation-coverage tests only).

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
   - ambiguous duplicate (neither row's `valid_from` (year, month) matches the target).
5. `q` is preferred over `q50`.
6. Missing columns: no `horizon_value` column → nothing derived, counted, no exception; no `q` column → `q50`
   used; neither `q` nor `q50` → nothing derived.
7. A duplicate pair where only one row's `valid_from` (year, month) matches the target → that row wins; a
   pair matching the month but not the year does not count as a match.

**Observation tests:** 2 of 3 months → no quarterly observation; 3 of 3 → one. Existing tests that expect
2 of 3 to pass change (e.g. `tests/test_aggregation.py:168`); `tests/test_aggregation.py:41-42` is unchanged.

**Mutations to record in the PR:**
- drop the issue-day check → the wrong-day negative fails;
- drop the issue-month check → the wrong-month negative fails;
- accept 2 of 3 leads → the missing-lead negative fails.

### P1b — readers, native-row selection, maintenance, writer

Depends on P1a. It can run in parallel with P1c; the two touch disjoint source files.

**Files:**
- `src/data_reader.py`: the two quarter readers (item 2), the combined-reader filter, the shared native-row
  helper, `_quarterly_fc_output_cols`.
- `postprocessing_maintenance_long_term.py`: the gap-detector call, the gap universe and the allowed
  restructuring (item 4).
- `src/api_writer.py`: the LR skip (item 3) only.
- Tests.

**Tests** (mock only the API boundary; both flags; both org shapes, kghm day 25 / lead 1 and tjhm day 1 /
lead 0):
- **Native-row selection (kghm shape).** A native row, a rewrite (`date = valid_from`) and a persisted
  derived Dec-1 row for the same LR Q1 → the native row, in both readers; with and without an unrelated
  derived-model row; with shuffled row order. (For tjhm, rewrites and hv0 derived rows share the native
  key, PP-061; decision F handles them, so no tjhm variant of this case.)
- **Fallback (both shapes).** No native row, fallback active → the derived LR row. With a native row
  present, the fallback never overrides it.
- **Stored leads (flag ON).** A direct LR row with the matching date and window but a wrong stored hv, next
  to a valid control → the bad row is dropped and counted; the control keeps its stored hv.
- December Q1 at `forecast_date` 2026-12-25, through the real `read_latest_quarterly_forecasts`. It fails on
  trunk. Mutation: removing the target-year extension makes the flag-ON case fail.
- `forecast_date` 2026-09-25 with Dec-25 rows present → Q4, not Q1.
- A fresh derived GBT row **survives** next to a persisted GBT QUARTER row with the same key.
- **Dataset B** — legacy QUARTER rows of the seven models at hv 1–4 with `date = valid_from` — is dropped
  by all three readers.
- **Output schema.** Under flag OFF, both readers return `date` and `horizon_value`.
- **Missing quarter config (uzb-like), both flags:** the derivation logs a WARNING and is skipped; flag ON
  returns empty as today, with `tests/test_lead_aware_empty_schedules.py:207, 239` unchanged; flag OFF
  behaves exactly as trunk (the direct path's `quarter_horizon_value()` raise is unchanged).
- **Maintenance, through the real entry point** `postprocessing_maintenance_long_term()` with the real
  `data_reader`, `gap_detector` and `ensemble_calculator` (mock only the API client, the environment setup
  and the station-code read; capture `file_writer`): complete monthly triplets, monthly ensembles already
  present (no monthly gaps), no persisted QUARTER rows → the quarter gap is detected and a Naive Mean is
  written. Fails on trunk (the run exits at `:126`).
- **Gap detection:** raw rows with a Naive Mean and no Skilled Mean → no gap; raw rows (≥ 2 models)
  without a Naive Mean → a gap; a key with a single raw model → no gap.
- **Writer:**
  - a raw LR row reaches the quarter writer → no record, plus the skip count;
  - a derived GBT row or an ensemble row → a record;
  - a fallback-derived LR row is never persisted, so the next read cannot treat it as native: test **flag
    ON** (the row's own `date` = `d` would be native) and **tjhm-shaped under flag OFF**
    (`record_date = valid_from` = the issue date, also native-shaped).

**Existing tests expected to change** (list each in the PR, with before/after):
- `tests/test_quarterly_data_reader.py:134, 245, 407, 443, 654, 718, 985, 1015`, where they assert the old
  mixed-issue or 2-of-3 monthly aggregation, or the flag-OFF output columns.
- Keep the direct-row exclusion assertions for the seven models (e.g. `:293-337`, `:765-810`).
- `tests/test_quarterly_workflow_integration.py` calls the old aggregator directly and stays unchanged.
- In `tests/test_quarterly_api_writer.py`, only `:285` writes a raw LR row through the quarter forecast
  writer; `:64` and `:89` are skill-writer tests and are unaffected. Grep the other files that call the
  quarter forecast writer (`test_aggregated_nan_guard.py`, `test_lead_aware_writer_reader_round_trip.py`,
  `test_recalc_workflow.py`, `test_wiring_integration.py`) and list any raw-LR write they assert.
- `tests/test_maintenance_long_term.py:501-625` (quarterly dedup, lead-aware) is expected to be unaffected:
  it mocks `read_quarterly_forecasts`, which the new gap universe reuses. Any other maintenance test that
  asserts the run ends before the quarterly block (e.g. the early-exit tests at `:217`, `:245`) is listed.

### P1c — ensembles, ensemble skill, K

Depends on P1a.

**Files:** `src/ensemble_calculator.py`, `src/skill_metrics.py` (the shared helper, removal of quarter EM,
the composition-free quarter grouping, the K default), tests, and the `quarter_*` keys of the flag-OFF
golden file under `tests/golden/`.

**Tests:**
- On the shared helper, with a skill frame:
  - Naive Mean = all members;
  - Skilled Mean = the NSE > 0 members with `n_pairs` ≥ K, 1/MAE-weighted;
  - **no quarter EM** from `create_quarterly_ensemble_forecasts` or `calculate_quarterly_skill_metrics`;
  - per-group and per-lead isolation (flag ON);
  - K−1 / K boundaries.
- **Flag OFF, skill hv 0, forecast hv 1** → Naive Mean and Skilled Mean form. Fails if the hv key follows
  column presence instead of the flag.
- Quantile nulling is per column. An LR-only Naive Mean with `q50` null keeps q05–q95. Adding a derived
  member nulls all of its quantile columns.
- **Recalc path, composition-free.** Build observations that give the intended NSE signs. **12** target
  years with compositions 5/2/5 (each subset below K) → one persisted row per ensemble per (code,
  quarter[, hv]), `n_pairs` 12, and no erroneous tombstone. With **9** target years → the group is
  suppressed.
- **Tombstone.** A stored quarter EM skill row → the recalc emits a tombstone for it
  (`build_stale_tombstones`).
- Season: EM, Naive Mean and Skilled Mean unchanged.

**Existing tests expected to change** (owner decision; list each in the PR):
- The fixed-LR quarter EM tests now assert that there is **no quarter EM**:
  `tests/test_lt_min_pairs_gate.py:592-635` (both quarter tests), `tests/test_quarterly_ensemble_creation.py:203-227`,
  `tests/test_quarterly_skill_metrics.py:265`. Grep the other quarter test files for `"EM"` and list every
  quarter EM assertion changed.
- `tests/test_quarterly_skill_metrics.py:529` and `tests/test_quarterly_ensemble_creation.py:458` are
  **season** tests and stay unchanged.
- K from 5 to 10: `tests/test_lt_min_pairs_gate.py:44` and `tests/test_n_pairs_floor.py:49` share
  `K_QS = 5` between quarter and season; split them into quarter 10 / season 5 without changing the season
  assertions. `tests/test_lt_min_pairs_gate.py:161` asserts the quarter default of 5. Fixtures with 5–9
  quarter years that relied on the default may now be suppressed; list each. The golden fixture sets K = 5
  explicitly (`tests/_skill_lead_aware_golden_fixtures.py:37`), so it is not affected by the default.
- **Flag-OFF golden** (`tests/test_skill_lead_aware_golden_baseline.py:93`): `quarter_skill`,
  `quarter_joint` and `quarter_ensembles` change (no EM, composition-free grouping, per-column quantiles).
  Regenerate **only those keys** with `tests/generate_skill_lead_aware_golden.py` and show in the PR that
  the `month_*` and `season_*` keys are byte-identical.

### P1d — end-to-end

Depends on P1b and P1c. Tests only.
- One December-Q1 chain (raw monthly rows → reader → ensembles → skill), both flags.
- **Flag OFF chain:** reader → ensembles → operational merge with persisted non-native LR rows present →
  Naive/Skilled Mean are built from native plus derived rows only, and the writer emits no LR row.
- Full suite: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` gives zero
  failures and zero unexpected skips; only the pre-existing xfail remains.
- `ruff check` and `ruff format --check` are clean on the touched files.
- `git diff --stat` touches only the files allowed by P1a–P1d.

### P2 — rollout (with PP-064 Chunk C)

**One window between LT cron days** (kghm 10 and 25; tjhm 1): deploy PP-065, run PP-064 Chunk C step 3
(decision F, tjhm), then the recalc per org. Between deploy and recalc the gates would run on the old,
contaminated quarter skill.

**Before the recalc**, per org:
- a private export of the QUARTER `long_forecasts` and `skill_metrics` rows;
- a count of rule-A triplets per model × quarter.

**After the recalc**, check and report per org (aggregate counts only):
- the four tombstone outcomes:
  - kghm flag ON: hv0 → hv1;
  - tjhm: hv0 replaced;
  - flag OFF: sentinel replaced;
  - sub-K groups and the old quarter EM skill rows tombstoned;
- the quarter skill rows suppressed by K = 10 (PP-064 C step 5);
- persisted Naive Mean / Skilled Mean / EM forecast rows at keys the recalc did not emit (accepted,
  round-2 decision 2; report the count);
- unfillable gaps: quarter keys the maintenance detector reports that the gap-fill cannot fill (e.g. no
  skill row for the key);
- the persisted ensemble values and compositions;
- the Dataset B rows overwritten by fresh derived rows with the same key under flag OFF. This upsert is
  irreversible, which is why the export is taken first.

**Operationally:** on the next quarter issue day, derived rows appear on the dashboard with δ bounds
(FD-029). Fallback quarters show no LR row (round-2 decision 3; hydromet notice).

### P3 — remove the LR fallback

Only after LTF-014 P0 and P2 are deployed on both orgs.

**Files:** `src/data_reader.py` (drop the LR fallback branch), tests.

**Acceptance:** before removing it, count per org the quarters that would lose LR rows. For the calendar
quarters of scored years, expect none.

## Out of scope

- Season.
- Fixing the labels upstream (LTF-016).
- Displaying δ bounds (FD-029/FD-030).
- Deleting legacy rows, including old ensemble rows (D8 / PP-041).
- Any writer change beyond the LR skip.
- Making season gap-fill reachable when the monthly block has nothing to do.
