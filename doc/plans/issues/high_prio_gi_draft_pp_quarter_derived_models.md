# PP-065: Seven models for quarter as same-issue monthly averages; quarterly Naive/Skilled Mean as for monthly

**Status**: Draft (2026-09-26, rev 6 after the fourth review round)
**Module**: `apps/postprocessing_forecasts`
**Priority**: High. On the 2026-12-25 critical path (round-2 decision 5): the LR fallback guarantees a kghm
Q1 even without LTF-014 P0.
**Labels**: `postprocessing_forecasts`, `long-term`, `quarter`, `ensembles`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only.
**Related**:
- PP-064: calendar-window validation and the native-row rule (its Contract). This plan comes after its
  Chunk A and absorbs its rev-3 rules B2, B4 and B6.
- PP-066: owns `select_operational_issuances`' own unclamped issue-day match (`data_reader.py:349-353`).
  This plan's native-row helper (item 2) applies the clamp on its own comparison; it does not touch, and
  does not need, the selector PP-066 fixes.
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
   - P0 is deferred (configs stay `forecast_months [3..9]`), so the fallback has no end date. It covers
     kghm Q1 and tjhm Q1/Q4 (the latter after decision F clears the aggregate-only LR rows).
   - Fallback-derived LR rows are **not persisted**; they feed the ensembles and skill only (round-2
     decision 3).
   - The fallback is removed in P3, which stays blocked while P0 is deferred.
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
   - **Input/output contract.** The rows come from `_read_long_forecasts_api` (`src/data_reader.py:1406`):
     `model_type` in API spellings (`LR_Base`, `SM_GBT_Norm`), an unnormalised `code`, a string `date`.
     - The **caller** renames `model_type` → `model_short` and normalises `code` as the readers do
       (`src/data_reader.py:1493-1498`). It does not call `_normalize_monthly_forecasts`, which fills a
       null hv with 0 (`:1503`) and parses `valid_from` without `format="mixed"` (`:1488`).
     - The **helper** parses `date` (and `valid_from`) with `pd.to_datetime(..., format="mixed")`, and
       matches model names canonically, upper-case (`canonical_model_short_series`,
       `src/model_names.py`).
     - The output `model_short` keeps the **stored spelling**, so downstream name handling is unchanged.
   - **Scope:** it derives only (code, model, `d`) whose `d.month + L` (year-aware) is a quarter start
     month (1/4/7/10); other issue dates produce nothing.
   - **Excluded (and counted):**
     - rows with a null or non-integer stored `horizon_value`, or no `horizon_value` column;
     - rows whose `date.day` ≠ `issue_day`, **clamped to the length of the issue month** — the same
       clamp the shared native-row rule applies below (item 2) and the producer schedules
       (`lt_utils.py:170-172`). Without this clamp, a genuine on-schedule monthly issuance in a short
       month (e.g. `issue_day = 31` in a 30-day June) would be wrongly excluded here, even though the
       same row would pass the (already-clamped) native check everywhere else. Add a test: `issue_day`
       configured as 31, an issue in a 30-day month → the row is NOT excluded, and both the derived-model
       output and the LR-fallback output (item 2's "Derived rows") include it.
   - **Target month** = issue month + `horizon_value`, year-aware.
   - **Triplet:** for each (code, model, issue date `d`) whose month is Q's first month − `L`, the rows with
     `horizon_value` = `L`, `L+1`, `L+2` must all exist.
   - **Duplicates** at the same (code, model, `d`, hv): **exactly one** row whose `valid_from` (year, month)
     equals the target (year, month) wins; zero or ≥ 2 matching rows → skip the triplet and count it.
     Evidence: the local DB has 519 tjhm and 38 kghm MONTH groups where two rows match (one calendar and
     one offset window); today these are all ensembles.
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
     quarter `issue_day`, **clamped to the length of the issue month** — the same clamp the producer
     applies, `apps/long_term_forecasting/lt_utils.py:170-172 nearest_scheduled_issue_date`, and the same
     rule FD-029 already implements — and year-aware lead == `lead_time`) **to LR rows only**. Non-native
     LR rows (rewrites, persisted monthly-derived rows) are never selected. Add a test: issue day
     configured as 31, a native row dated on the 30th of a 30-day issue month → selected as native, at
     the helper level directly, and through both readers **under flag OFF** (which never calls
     `select_operational_issuances`, below). Do **not** assert this end-to-end through the readers under
     flag ON in P1b — `select_operational_issuances` still matches unclamped there and would drop the
     row; that end-to-end proof is gated on PP-066 (its Tests list owns it).
   - **Stored leads (flag ON).** Before `select_operational_issuances`, drop and count direct rows whose
     stored `horizon_value` differs from the derived lead, then call it with `lead_output_cols=()` so the
     stored value is preserved. `select_operational_issuances` itself is not modified — it keeps matching
     the **unclamped** issue day (PP-066, which owns that gap), so under flag ON a clamped-day-only-valid
     row is dropped here even though the native-row helper above would have classified it correctly.
   - **Derived rows.** Read raw monthly rows via `_read_long_forecasts_api` for issue years
     `start_year − 1 … end_year`. In the latest reader, also require issue date ≤ `forecast_date`. Derive
     for the seven models; while the fallback is active, also derive each LR model for (code, year,
     quarter) keys with no selected native row of that model.
   - **Existing Source 1 (LR aggregation) forecast_date bound, latest reader, both flags — IN scope.**
     `read_latest_quarterly_forecasts`' pre-existing monthly-derived path for LR_Base/LR_SM (unrelated to
     the "Derived rows" step above, which is this plan's new seven-model/fallback mechanism) has no bound
     against `forecast_date` today: flag ON calls `read_monthly_forecasts(codes, start_year, end_year)`
     (`data_reader.py:3415`), flag OFF calls raw `_read_long_forecasts_api(codes, start_year, end_year)`
     (`:3423`) — neither takes `today`/`forecast_date`, so a back-dated run could aggregate a monthly row
     issued after it into a quarter that should not be visible yet. This plan rewrites this reader (this
     item, above), so it owns bounding it: filter to issue `date <= forecast_date` (null kept) on the
     rows this reader itself receives — `read_monthly_forecasts`' output under flag ON, the raw rows
     from `_read_long_forecasts_api` under flag OFF — any time **before** they reach
     `aggregate_monthly_fc_to_quarterly` (unchanged, P1a). `read_monthly_forecasts` itself is **not**
     modified: filtering before or after its internal `select_operational_issuances` call is equivalent
     here, because that call derives the lead from (issue month, target month) and additionally requires
     the configured issue *day* (`data_reader.py:346-354`) — so a fixed (target month, lead) pins the
     candidate issue date to one exact calendar date, leaving no same-unit "earlier eligible vs. later
     ineligible reissue on a different day" case for filter placement to matter for.
     - **Test:** `forecast_date = 2026-06-25`; monthly LR rows issued 2026-09-25 and 2026-10-25 (both
       after `forecast_date`) must not produce a Q4 aggregate — under both flags.
     - **Test:** no row dated after `forecast_date` reaches `aggregate_monthly_fc_to_quarterly` (assert
       on a spy, or on the rows actually passed to it) — under both flags.
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
   - **Degraded native rule, flag OFF.** The native-row rule needs `operational_issue_day`;
     `operational_schedule_for_mode` raises `LongTermHorizonResolverError` when it is missing
     (`long_term_horizon_resolver.py:138-142`), and the autouse fixture in
     `tests/test_quarterly_data_reader.py:30-49` writes `quarter.json` with the lead only.
     - Flag OFF, on `LongTermHorizonResolverError` or `UnsupportedLongTermModeError`: log **one** WARNING
       (covering the skipped derivation too) and keep today's direct-LR selection, with **no** native
       filter. This mirrors FD-029's degraded mode. (With `quarter` unsupported, the direct read's
       `quarter_horizon_value()` still raises first, as today.)
     - Flag ON: unchanged. An unsupported `quarter` makes the guard return empty (`src/data_reader.py:3100-3109`,
       `:3376-3385`); a missing issue day already raises from `_operational_schedules_for_horizon_type`
       (`:179`) before any native filter could run.
3. **Writer: stop writing raw LR rows (rev-3 PP-064 "B6").**
   - The quarter branch of `_write_aggregated_forecasts_to_api` skips `LR_BASE`/`LR_SM` rows **and
     `EM`/`ENSEMBLE_MEAN` rows** (compare canonically). It keeps writing the seven derived models, Naive Mean
     and Skilled Mean.
   - **Why the EM skip:** stored rows are re-written through the operational `existing_q` concat
     (`postprocessing_operational_long_term.py:220-227`) and the maintenance merge-back (`:357-376`). Under
     flag OFF the writer re-dates them to `valid_from` (`src/api_writer.py:1199-1204`), and `date` is part of
     the unique key (`sapphire/services/postprocessing/app/models.py:193-201`), so a stored issue-dated EM
     would create a **new** EM key. Existing DB rows are untouched (round-2 decision 2).
   - **Why required:** fallback-derived LR rows are native-shaped (`date = d`, hv = `L`). Once persisted, they
     would pass the native-row rule forever. Native LR rows are owned by the LT module.
   - **Consequences:**
     - flag-OFF output changes (fewer rows written), and flag-OFF rewrites of LR rows (population b) stop;
     - **fallback LR is invisible (accepted, round-2 decision 3) — on kghm.** The dashboard card and the
       bulletin read the DB, so they show no LR row for fallback quarters until LTF-014 P0/P2. **On tjhm**,
       monthly-derived LR may remain visible as native until **both** this item (P1b) **and** decision F
       have landed (decision F runs after `deploy.pp` in the overview's dependency graph, i.e. inside the
       same writer-paused window, not automatically the moment P1b merges) — see the round-4 tjhm interim
       decision. LR still enters the ensembles and skill regardless of visibility.
     - **EM interim, until this item ships.** PP-064 A (already deployable/deployed independently of this
       plan) still writes fresh quarterly EM rows today: `ensemble_calculator.py` sets
       `model_short = "EM"` directly in the quarter aggregation path
       (`_create_aggregated_ensemble_forecasts:765`), and `api_writer.py`'s quarter-write loop
       (`:1157-1158`) resolves that through `MODEL_TYPE_MAP`'s identity `"EM": "EM"` entry (line ~27), not
       the `"ENSEMBLE_MEAN": "EM"` entry (line 50, which serves the skill-metrics write path only).
       FD-029 already hides every quarter EM row it reads on the dashboard side, consistent with the
       owner decision of no quarterly EM, but this item is what stops the *write*. Between PP-064 A's
       deploy and this item's own deploy, a quarter whose only rows are a fresh EM row plus a non-native
       LR row shows nothing on the card or the bulletin (FD-029 drops the EM row; its native-only rule
       drops the non-native LR row).
   - **Log** one aggregated skip count per call.
4. **Combined reader and maintenance.**
   - `read_quarterly_combined_forecasts` drops direct rows of the seven models. It stays **filter only**,
     with no monthly read.
   - **Gap detection** (caller only; `src/gap_detector.py` is not edited): pass
     `ensemble_models={"Naive Mean"}` at `postprocessing_maintenance_long_term.py:297-301`. Before the call,
     drop keys with fewer than two distinct raw models: a single-model group never forms a Naive Mean
     (`src/ensemble_calculator.py:915`), so it would be a perpetual gap.
   - **Gap-key filter (flag ON).** A gap detected via Naive Mean is carried with
     `model_short = "Naive Mean"` (`src/gap_detector.py:481`). The flag-ON filter of newly generated rows
     against the gap keys matches on `model_short` too (`postprocessing_maintenance_long_term.py:331-355`),
     so it would discard every newly computed Skilled Mean. Change it: a Naive Mean gap admits **both**
     newly formed ensembles (Naive Mean and Skilled Mean) for that (code, year, quarter[, hv]). Existing
     non-gap rows are still preserved. Flag OFF has no such filter (unchanged).
   - **Gap universe.** Capture `forecast_date = dt.date.today()` **once** at the maintenance entry point
     (Forecast Date Rule). Concatenate the raw rows of `data_reader.read_quarterly_forecasts(codes,
     Y − 1, Y + 1)`, with `Y = forecast_date.year`, into the gap universe (the `+ 1` covers a
     December-issued Q1). Rows, not just keys: the two-raw-model prefilter counts models per key. Use that
     existing reader function, not a new `data_reader` entry point.
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
   - Keep the empty-skill early return (`:632-634`) and the operational skip (`postprocessing_operational_long_term.py:209`):
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
- The permitted control-flow changes are the ones this plan names: the maintenance restructuring and
  gap-key filter (item 4), the reader output schema and the degraded native rule (item 2).
- Do not change season behaviour, `select_operational_issuances`, `src/gap_detector.py` or PP-064's window
  validation.
- Add **new quarter-only constants** in `src/model_names.py`, in canonical form (the output of
  `canonical_model_short`, `src/model_names.py:19-24`):
  - `QUARTER_NATIVE_RAW_MODELS = frozenset({"LR_BASE", "LR_SM"})`;
  - `QUARTERLY_DERIVED_MODELS = frozenset({"GBT", "LR_SM_DT", "LR_SM_ROF", "MC_ALD", "SM_GBT",
    "SM_GBT_LR", "SM_GBT_NORM"})`;
  - `QUARTER_SUPPORTED_MODELS = QUARTER_NATIVE_RAW_MODELS | QUARTERLY_DERIVED_MODELS |
    AGGREGATED_ENSEMBLE_MODELS`. `ENSEMBLE_MEAN` stays in it **for reading old rows only**: it is never an
    ensemble member (members come from the two raw sets), never produced, and never written (item 3).
- Do not modify `AGGREGATED_EM_RAW_MODELS` or `AGGREGATED_SUPPORTED_MODELS` (`src/model_names.py:14-16`);
  code shared with season branches on `period_col` or the horizon.
- In `api_writer.py`, change only the LR and EM skip (item 3).
- `sandro_sapphire_2_quaterly_agg` may be consulted but not cherry-picked. Never `git stash`.
- Every edited existing test is listed in the PR with its reason (before/after).

### P1a — constants, derivation helper, observation coverage

**Files:**
- `src/model_names.py`: the new quarter-only constants.
- `src/aggregation.py`: the new helper and `QUARTER_OBS_MIN_MONTHS` (item 8). Leave
  `aggregate_monthly_fc_to_quarterly` and `QUARTER_MIN_MONTHS` unchanged.
- New `tests/test_quarter_derived_models.py`; `tests/test_aggregation.py` (observation-coverage tests only;
  the edits are listed below).

**Tests on the helper** (station `19999`):
1. kghm, `d` = 2026-12-25, hv 1/2/3 → Q1 2027, `date` = `d`, hv 1, window 2027-01-01..03-31, null quantiles.
2. tjhm, `d` = 2027-01-01, hv 0/1/2 → Q1 2027, hv 0.
3. These are still derived:
   - offset windows (`valid_from` on 01-02, 02-01 and 03-03);
   - a GBT January row labelled with the issue year;
   - `issue_day` configured as 31, `d` issued on the 30th of a 30-day month (e.g. June) — the producer's
     own clamp; excluded only by the unclamped check this fix removes.
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
8. **Two matching rows** (item 1, uniqueness): a raw/corrected pair at the same (code, model, `d`, hv) with
   the same target month, different endpoints (e.g. 01-01..01-31 and 01-02..02-01) and different values,
   in shuffled order, next to an unambiguous control → the pair's triplet is skipped and counted; the
   control derives.
9. Model-name contract: input `model_type` `SM_GBT_Norm` (after the caller's rename) derives, and the
   output `model_short` is `SM_GBT_Norm`; an issue date whose `d.month + L` is not a quarter start derives
   nothing.

**Observation tests:** 2 of 3 months → no quarterly observation; 3 of 3 → one. The existing test edits
were **exactly** `test_two_months_passes` (renamed `test_three_months_passes`,
`tests/test_aggregation.py:172`), `test_multiple_stations` (`:240`) and `test_multiple_quarters`
(`:298`), each of which relied on 2 of 3 months (measured; line numbers as implemented, this branch's
HEAD). `tests/test_aggregation.py:42-43` (`QUARTER_MIN_MONTHS == 2`) is unchanged.

**Mutations to record in the PR:**
- drop the issue-day check → the wrong-day negative fails;
- drop the issue-month check → the wrong-month negative fails;
- accept 2 of 3 leads → the missing-lead negative fails.

**Acceptance (P1a):**
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`: the full module suite is
  green apart from the three listed `tests/test_aggregation.py` edits; zero unexpected skips; only the
  pre-existing xfail.
- `ruff check` / `ruff format --check` clean on the touched files.
- `git diff --stat` within the P1a file list.

#### P1a amendments (2026-09-27, readiness review; extended 2026-09-27 after dev-DB validation)

Two independent readiness reviews, and later a real-dev-DB validation run plus a second pair of
independent reviews (codex + a fresh Claude reviewer), found gaps in the P1a spec above. This
subsection IS the amended spec (the implementing brief it was drafted from was a scratch file that no
longer exists; nothing here depends on it):

- **Date parsing.** `date` and `valid_from` are parsed with `local_calendar_date`
  (`src/aggregation.py:102-201`), not `pd.to_datetime(..., format="mixed")`: the latter raises on
  mixed tz-aware/naive strings and shifts the local date under `utc=True`.
- **Return shape.** `derive_quarterly_from_monthly_same_issue` returns `(frame, counts)`, where
  `counts` is a `dict[str, int]` (a `Counter`) of exclusion counts by reason, not merely a diagnostic
  log. Ignored-without-counting reasons (model out of scope, no quarter-start, hv outside
  `{lead, lead+1, lead+2}`) are distinct from counted reasons (`bad_key`, `bad_horizon_value`,
  `bad_date`, `wrong_issue_day`, `missing_lead`, `non_finite_value`, `ambiguous_duplicate`, plus
  `missing_column:*` and `invalid_config`).
- **Exclusion order (2026-09-27 fix, restated 2026-09-27 after round-2 review).** The actual
  pipeline order is: `bad_key` (G4, below) -> model scope (silent) -> `bad_horizon_value` (counted)
  -> hv-range (silent) -> `bad_date` (counted) -> quarter-start (silent) -> `wrong_issue_day`
  (counted) -> the dedup/uniqueness/triplet steps. `bad_horizon_value` and `bad_date` are NOT
  "ignored-without-counting" checks and are NOT deferred past the scope checks they gate -- each
  necessarily runs immediately before the scope check that depends on its own column (hv-range needs
  a valid `horizon_value`; quarter-start needs a valid `date`) and could not run any earlier. What
  changed is narrower: hv validity+range now run BEFORE the date-based checks (date parse,
  quarter-start, `wrong_issue_day`), where originally `wrong_issue_day` ran before the hv-range
  check. That gap let an in-scope-model row from a DIFFERENT monthly mode with a genuinely
  irrelevant lead (e.g. kghm's day-10 `month_0`, hv 0, when this call's lead is 1) satisfy the
  quarter-start check by coincidence and then be wrongly counted as `wrong_issue_day` instead of
  silently ignored. `bad_horizon_value` counts only in-model rows with a non-finite or non-integer
  `horizon_value`; a valid but out-of-range `horizon_value` is silently ignored, never counted
  (`src/aggregation.py:941-962` for the reordered hv checks, before the date parse at `:967`).
- **`horizon_value` rule.** `hv = pd.to_numeric(col, errors="coerce")`; a row is eligible only if `hv`
  is finite and `hv == round(hv)`; cast to `int` only AFTER that filter (real API frames carry `hv` as
  float64 with NaN).
- **Output column set.** A FIXED list, never built by copying input rows: `code`, `model_short`,
  `year`, `quarter_in_year`, `date`, `horizon_value`, `valid_from`, `valid_to`,
  `forecasted_discharge`, `q` (present only if the input had a `q` column), and every column of
  `_FC_QUANTILE_COLS` (NaN). Columns like `id`, `flag`, `composition`, `q_obs`,
  `model_type_description` and `horizon_type` never leak into the output.
- **Exact-duplicate pre-step and singleton rule (2026-09-27 fix; extended 2026-09-27 after round-2
  and round-3 review, `src/aggregation.py:1017-1089`).** Before the uniqueness rule, a row is an
  exact duplicate of another only if its identity (code, canonical model, `d`, hv, `valid_from`,
  `valid_to`) AND its point-value inputs (`q` and `q50`, NaN-equal) BOTH match -- a same-window pair
  with a DIFFERENT value is never silently collapsed by whichever row happens to sort first; it is
  left for the uniqueness rule below, where a missing `valid_from` column makes the group
  unresolvable (ambiguous) and a null `valid_from` never matches. The original version keyed only on
  identity, so two rows with different values but a null or identical window (or no `valid_from`
  column at all) silently collapsed to one -- the "no-`valid_from`-column implies ambiguous" branch
  was consequently unreachable.
  - **The window is ALWAYS part of this identity** (round-3 finding H2) -- BOTH ends, `valid_from`
    AND `valid_to` independently, each pinned in ALL THREE dedup partitions: no `id` column at all,
    `id` present but null on this row, and `id` present and non-null -- not just the no-`id` case. A
    null-`id` or no-`id` frame drops window from the key exactly as easily as an `id`-present frame
    would, so the SAME same-window/different-value and different-window/same-value negatives apply
    everywhere (round-3 finding H1: the "without-`id`" partition must also apply the value check, not
    only the fully-`id`-less path; round-3 finding J2: BOTH the `id`-present and the
    `id`-present-but-null partitions needed their own dedicated test for `valid_from`, since each uses
    its own `drop_duplicates` call with its own subset list -- a mutant dropping window from just one
    of the three calls passed everything until each partition had its own pin). **This claim was
    ITSELF wrong for `valid_to` until a follow-up round (finding K2)**: H1/H2/J2's tests all varied
    `valid_from` only, so `valid_to`'s presence in the key was unpinned in EVERY partition -- four
    mutants (J1's raw-fallback revert applied to `valid_to` only; dropping `valid_to`'s dedup column
    from the `id`-present, `id`-present-but-null, and no-`id` subsets) all passed every existing test.
    Each partition now has its own `valid_to`-only variant (same `valid_from`, different `valid_to`,
    same value) alongside the existing `valid_from`-only ones, and its own unparseable-`valid_to`
    variant alongside the unparseable-`valid_from` one (finding K2, below).
  - **`id`, when present, does NOT make a pair authoritative on its own** (round-3 finding H5 --
    avoid that word; restated plainly): `id` never merges rows that key + window + value would not.
    Its only effect is to keep rows APART whose ids differ -- specifically, two null ids are never
    treated as evidence of a repeat (a genuine risk with a naive `id`-only key), so null-`id` rows
    fall back to the plain key+window+value rule, exactly as when `id` is absent entirely. A
    same-`id` pair with a DIFFERENT value or window is a genuine conflict, not a repeated read, so
    both rows are kept and reach the uniqueness rule as a group of >= 2 (round-2 finding G1) -- the
    first version of this fix still deduped on `id` alone (ignoring value and window), so the OUTPUT
    depended on row order, and `_reference_derive` carried the identical bug (the differential test
    could not catch it, since both sides agreed). The `id`-scoped dedup is additionally keyed on the
    natural (code, canonical model, `d`, hv, window) key, not `id` alone, so an accidental `id`
    collision across two UNRELATED triplets (found by the G3 shuffled/duplicated-index invariance
    check, not by hand) can never merge them. The original version's `drop_duplicates(subset=["id"])`
    treated every null `id` as equal to every other, so an all-null `id` column silently discarded 2
    of a triplet's 3 rows regardless of model or lead.
  - **Windows are compared as PARSED local calendar dates where they parse** (round-3 finding H3),
    via `local_calendar_date` -- the SAME parsing rule the amendment applies everywhere else -- not
    as raw strings: `"2027-01-01"` and `"2027-01-01T00:00:00+06:00"` for the same row are the same
    window and do not block the collapse. The original version compared raw `valid_from`/`valid_to`
    strings, so two representations of the identical instant were wrongly treated as different
    windows. **Where a value does NOT parse, the key falls back to a hashable, type-qualified
    string built from the RAW value** (`f"{type(v).__name__}|{v!s}"`, via the shared
    `_window_dedup_key` helper, `src/aggregation.py:680-718`), **with an actual null kept null**
    (round-3 findings J1 and K1): H3's first version keyed purely on the parsed value, so two
    DIFFERENT unparseable strings (e.g. `"garbage"` vs `"xx"`) both became `NaT` and therefore
    compared EQUAL to each other -- silently collapsing a pair the pre-H3 code correctly left
    ambiguous (J1). J1's own first version then kept the raw value itself as the fallback, which
    raised `TypeError: unhashable type` the first time an UNHASHABLE raw value (e.g. a list or dict
    from malformed upstream data) reached `drop_duplicates`, which requires every key value to be
    hashable (K1); the type-qualified STRING fallback is always hashable, and the type prefix means
    an unhashable value never collides with a same-content hashable one either. A genuinely null
    `valid_from`/`valid_to` still matches another null.
  - **A deterministic spelling tiebreak** (round-3 finding H4): when two rows are exact duplicates in
    every respect except the stored `model_short` spelling (they share one canonical model), the rows
    are sorted by that raw spelling (a stable sort) before the drop, so the lexicographically smallest
    spelling wins regardless of input row order. The original version left this to whichever row
    `drop_duplicates(keep="first")` happened to see first.
  - A singleton at (code, canonical model, `d`, hv) is used whatever its `valid_from`, including
    missing/NaT.
- **`bad_key` (2026-09-27, round-2 finding G4).** A null `code` or `model_short` is excluded and
  counted as `bad_key` BEFORE anything groups on `code` (`src/aggregation.py:917-930`, the first
  per-row filter in the function, before even the model-scope check). Pandas `groupby` drops a null
  group key by default, so a null-`code` row's boolean `.transform()` result came back as `NaN`
  instead of `True`/`False`, and `~NaN` raised `TypeError` at the ambiguity check further down --
  this made the ENTIRE call crash, not merely mis-handle the one bad row.
- **`code` (and date/window) output dtype (2026-09-27, round-2 finding G5).** The non-empty result's
  `code`, `date`, `valid_from` and `valid_to` columns are explicitly cast to `object`
  (`src/aggregation.py:864-878`, in `typed()`), matching `empty_result()`'s hardcoded object dtype
  regardless of the input `code` column's own dtype (numeric, pandas `StringDtype`, etc.) -- the
  original version left `code` at whatever dtype it inherited from the input, so the empty and
  non-empty schemas could disagree.
- **Ambiguous-duplicate WARNING.** `ambiguous_duplicate > 0` logs at WARNING (count only, no station
  codes; it signals mislabelled upstream data, LTF-016). Every other count logs at INFO, except
  `invalid_config`, which also logs at WARNING.
- **Invalid-config handling.** `issue_day < 1` or `lead < 0` returns the empty schema with ONE
  WARNING and `invalid_config` counted -- no exception raised.
- **Flag independence.** The helper never reads `SAPPHIRE_SKILL_LEAD_AWARE`; its output is byte-identical
  under both flag states (verified by a parametrized test).
- **Clamp helper.** `clamp_issue_day(year, month, issue_day)` = `min(issue_day,
  calendar.monthrange(year, month)[1])`, the same rule as the producer
  (`apps/long_term_forecasting/lt_utils.py:170-172`) and PP-064's `data_reader.py:3097-3101`. P1b
  reuses this helper for the native-row rule.
- **Distinct-month observation counting.** `aggregate_monthly_obs_to_quarterly` first averages per
  (code, year, quarter, month) skipping NaN, then aggregates those monthly means to the quarter
  (unweighted mean); `n_months` counts DISTINCT months with a non-null monthly mean, not non-null
  rows. With the normal one-row-per-month input this is unchanged from before; only the coverage
  threshold (`QUARTER_MIN_MONTHS` -> `QUARTER_OBS_MIN_MONTHS = 3`) changed. This equivalence is EXACT
  only for input already sorted by (code, year, month), which is what the only caller produces --
  differently-ordered input can differ by roughly 1e-14 from floating-point summation
  reassociation, not by anything a caller should observe in practice.
- **Typed empty schema (2026-09-27 fix).** `empty_result()` returns a schema typed identically to a
  non-empty result: `year`, `quarter_in_year` and `horizon_value` are int64; `forecasted_discharge`,
  `q` and the quantile columns are float64; the rest are object. The original version left every
  empty-schema column as plain object dtype, so e.g. concatenating an empty derived frame with a
  typed direct-read frame could silently upcast `year` away from int64 (and, once pandas removes the
  deprecated empty/all-NA exclusion it currently warns about, change the concatenated dtype outright).
- **Updated line citations** (this branch's HEAD, re-verified 2026-09-27 after round-3 review (H1-H5),
  the J1-J3 follow-up AND the K1-K2 follow-up -- re-verify again after any further edit to
  `src/aggregation.py`, since these drift with every change above them in the file):
  `local_calendar_date` is at `:102-201`; the observation coverage filter is at
  `src/aggregation.py:397`; the delta computation is at `:403-409`; `QUARTER_MIN_MONTHS` is defined
  at `:284` and remains used only by `aggregate_monthly_fc_to_quarterly` (forecast aggregation,
  unchanged by this phase); the shared `_window_dedup_key` helper (K1's hashable, type-qualified
  fallback) is at `:680-718`; the `typed()` function (object-dtype cast included) is at `:864-878`;
  `bad_key` is at `:917-930`; the derivation helper's hv-check reorder is at `:941-962`; the date
  parse itself is the single line at `:967`; the exact-duplicate pre-step (window-parsing via
  `_window_dedup_key` for BOTH `valid_from` and `valid_to`, the spelling-tiebreak sort, and the
  value- and natural-key-scoped `id` dedup, across all three partitions) is at `:1017-1089`; the
  vectorized uniqueness/triplet-assembly rewrite (see "Performance" below) spans `:1110-1226`
  (the end of the file).
- **Performance (2026-09-27, PP-065 F3).** The original P1a implementation grouped rows with Python
  `for key, group in df.groupby(...)` loops for both the uniqueness rule and the final triplet
  assembly -- correct, but O(rows) in Python, measured at 8.5-38s for a ~200k-row synthetic monthly
  frame depending on the reviewing environment. It was rewritten to be fully vectorized: group size
  and match count via `groupby().transform()`, the triplet check and mean via a pivot/unstack on
  `horizon_value`, and month arithmetic/clamping via integer "months since epoch" arithmetic and
  `Series.dt.days_in_month` instead of per-row Python calls. Measured on the same 200k-row synthetic
  frame (same machine): 30.4s -> ~0.65s (about 47x) after the round-2 fixes, ~0.75s after the round-3
  fixes below (H3's window parsing and H4's sort add a little back), still comfortably under the 2s
  target. A real-data re-run (8 org/mode combinations) landed the same conclusion independently:
  byte-identical output to the pre-vectorization commit in all 8 runs, 4-13x faster, counts changing
  only by the intended out-of-scope-row exclusion (F5's month_0-style fix).

  A frozen, deliberately non-vectorized reference copy of the pre-rewrite logic (`_reference_derive`
  in `tests/test_quarter_derived_models.py`) is checked against the production function by a
  randomized differential test (300 generated frames, fixed seed). The generator draws 1-3 fake
  station codes (19999/19998/19997, so `code` is exercised in the grouping key -- round-2 finding
  G2), leads 0-11 (not just 0-2), issue days including 29/30/31 so the day-of-month clamp is
  exercised end to end in Feb and 30-day months, multiple models, wrong days, exact and ambiguous
  duplicates, a deliberate "exactly one match in a group of >= 2" case, NaN/float/out-of-range
  `horizon_value`, a null `code`/`model_short` (`bad_key`), offset windows, mixed-tz date strings and
  missing `q`/`q50` columns; it asserts the sorted output (dtypes included) and the counts match
  exactly. Every frame is ALSO re-run shuffled with a non-default, duplicated index, asserting
  production's own output is unchanged (order/index invariance) -- this caught a real gap round-1
  testing missed: `id`-based dedup was scoped by `id` alone, so an (unrealistic but not impossible)
  `id` collision across two UNRELATED triplets could merge them; the fix scopes it by the natural key
  too. A small set of hand-computed expected results (the Dec-Jan rollover at leads 0/1/2/11, the Feb
  29 and June 30 clamps, a same-`id` conflict, and a single-match-in-a-group win), computed directly
  from the spec rather than via either implementation, lives in
  `TestHandComputedSpecDerivedResults` in the same test file. Keep the reference frozen -- fix
  production, the reference and the differential test together when they are found to disagree,
  never edit the reference alone to match a production change.

  **Round-3 review (2026-09-27).** An independent spec-derived oracle (codex) matched production on
  every frame; a real-data re-run was identical in all 8 runs; a second, spec-only Claude oracle
  found no production defect beyond one interpretation question (resolved as the plain-language `id`
  restatement above). It DID find two real test-coverage gaps that a targeted mutant could slip
  through unnoticed (H1: the value check in the null-`id` partition was unpinned, because
  `_reference_derive` shared that exact line so the differential test agreed with the bug; H2: the
  window's presence in the exact-duplicate identity was unpinned in the no-`id` partition) -- both
  are now covered by a dedicated unit test AND mutation-verified. It also found two real, if minor,
  production gaps (H3, H4) fixed above.

  **Round-3 follow-up (2026-09-27, J1-J3).** A further confirmation pass (codex's independent
  spec-derived oracle matched production on every frame again; the real-data re-run was identical in
  all 8 runs, unchanged order and spellings; a second reviewer found no regression in the H4
  tiebreak) found three more items. J1 (code, both implementations): H3's parsed-date comparison had
  its own gap -- two DIFFERENT unparseable `valid_from`/`valid_to` strings (e.g. `"garbage"` vs
  `"xx"`) both parse to `NaT` and, compared as parsed values only, wrongly counted as the SAME
  window. Fixed above (the raw-value fallback). J2 (test pins): H2's coverage was, despite the
  amendment's wording, NOT actually independent of `id` -- it covered only the no-`id` partition; the
  `id`-present and `id`-present-but-null-on-this-row partitions each have their OWN
  `drop_duplicates` call with its own subset list, so a mutant dropping window from either one alone
  passed every existing test. Both now have a dedicated test and are mutation-verified (see the
  bullet above, now corrected to say "across all three partitions"). J3 (docs): this "Updated line
  citations" bullet, the exact-duplicate bullet above, the stale inline code comment this bullet
  used to still carry the pre-G1 "authoritative" wording, and the P1a section body's own test
  citations below decision 8 were all re-verified/corrected against this branch's HEAD.
- **Dev-DB validation (2026-09-27).** On the local dev DB, complete same-issue triplets across the
  seven derived models (2000-2026) were 30.96k (kghm) / 7.13k (tjhm), and 14.1k / 3.4k over
  2015-2026; the feasibility section above (~27.8k / ~5.2k) did not state its year window, so these
  are not directly comparable, just a more precise re-measurement. Separately: kghm currently has no
  native Q1 LR rows on the 25 Dec issue date, which is expected while LTF-014 P0 is deferred (decision
  2's forecast_months restriction) -- this is exactly the gap the LR fallback (decision 2) exists to
  cover.

### P1b — readers, native-row selection, maintenance, writer

Depends on P1a. It can run in parallel with P1c; the two touch disjoint source files.

**Files:**
- `src/data_reader.py`: the two quarter readers (item 2), the combined-reader filter, the shared native-row
  helper, `_quarterly_fc_output_cols`.
- `postprocessing_maintenance_long_term.py`: the gap-detector call, the gap universe, the gap-key filter
  and the allowed restructuring (item 4).
- `src/api_writer.py`: the LR and EM skip (item 3) only.
- Tests.

**Tests** (mock only the API boundary; both flags; both org shapes, kghm day 25 / lead 1 and tjhm day 1 /
lead 0):
- **Native-row selection (kghm shape).** A native row, a rewrite (`date = valid_from`) and a persisted
  derived Dec-1 row for the same LR Q1 → the native row, in both readers; with and without an unrelated
  derived-model row; with shuffled row order. (For tjhm, rewrites and hv0 derived rows share the native
  key, PP-061; decision F handles them, so no tjhm variant of this case.)
- **Native-row selection, clamped issue day.** `operational_issue_day` configured as 31, issue month a
  30-day month (e.g. June): a native row dated on the 30th (the producer's own clamp,
  `lt_utils.py:170-172`) → selected as native by the shared helper directly, and, **flag OFF only**,
  through both readers. **Not** asserted end-to-end through the readers under flag ON here —
  `select_operational_issuances` still matches unclamped and would drop the row regardless of the
  helper's own classification; PP-066's Tests list carries that end-to-end case once its fix lands.
- **Existing Source 1 forecast_date bound, latest reader, both flags.** `forecast_date = 2026-06-25`;
  monthly LR rows issued 2026-09-25 and 2026-10-25 (both after `forecast_date`) → no Q4 aggregate is
  produced, under both flags. Fails on the pre-P1b base (neither flag bounds this path today).
- **Existing Source 1 forecast_date bound, inputs to the aggregation.** No row dated after
  `forecast_date` reaches `aggregate_monthly_fc_to_quarterly` (assert on a spy, or on the rows actually
  passed to it), under both flags.
- **Fallback (both shapes).** No native row, fallback active → the derived LR row. With a native row
  present, the fallback never overrides it.
- **Stored leads (flag ON).** A direct LR row with the matching date and window but a wrong stored hv, next
  to a valid control → the bad row is dropped and counted; the control keeps its stored hv.
- December Q1 at `forecast_date` 2026-12-25, through the real `read_latest_quarterly_forecasts`, from
  monthly triplets only (derived path). It fails on the pre-P1b base, which already contains PP-064 A.
  Mutation: removing the target-year extension makes the flag-ON case fail.
- `forecast_date` 2026-09-25 with Dec-25 rows present → Q4, not Q1.
- A fresh derived GBT row **survives** next to a persisted GBT QUARTER row with the same key.
- **Dataset B** — legacy QUARTER rows of the seven models at hv 1–4 with `date = valid_from` — is dropped
  by all three readers.
- **Output schema.** Under flag OFF, both readers return `date` and `horizon_value`.
- **Missing quarter config (uzb-like), both flags:** the derivation logs a WARNING and is skipped; flag ON
  returns empty as today, with `tests/test_lead_aware_empty_schedules.py:207, 239` unchanged; flag OFF
  behaves exactly as trunk (the direct path's `quarter_horizon_value()` raise is unchanged).
- **Degraded native rule.** `quarter.json` with the lead only (as the autouse fixture writes it): flag OFF
  → one WARNING, the direct LR rows are returned as on trunk (a non-native rewrite row included), no
  derived rows; flag ON → raises as on trunk.
- **Maintenance, through the real entry point** `postprocessing_maintenance_long_term()` with the real
  `data_reader`, `gap_detector` and `ensemble_calculator` (mock only the API client, the environment setup
  and the station-code read; capture `file_writer`): complete monthly triplets, monthly ensembles already
  present (no monthly gaps), no persisted QUARTER rows → the quarter gap is detected and a Naive Mean is
  written. Fails on the pre-P1b base (derived path; the run exits at `:126`).
- **Maintenance gap-key filter, through the real entry point, both flags:**
  - eligible raw models, sufficient skill, neither ensemble persisted → **both** Naive Mean and Skilled
    Mean are saved;
  - only Skilled Mean absent (it does not form) → no recurring gap.
- **Gap detection:** raw rows with a Naive Mean and no Skilled Mean → no gap; raw rows (≥ 2 models)
  without a Naive Mean → a gap; a key with a single raw model → no gap.
- **Writer:**
  - a raw LR row or an `EM` / `ENSEMBLE_MEAN` row reaches the quarter writer → no record, plus the skip
    count;
  - a derived GBT row, a Naive Mean or a Skilled Mean row → a record;
  - **entry point, flag OFF:** a stored issue-dated quarter EM row returned by
    `read_quarterly_combined_forecasts` through the real `postprocessing_operational_long_term()` (the
    `existing_q` concat) and through the maintenance merge-back → no EM record written, so no new EM key;
  - a fallback-derived LR row is never persisted, so the next read cannot treat it as native: test **flag
    ON** (the row's own `date` = `d` would be native) and **tjhm-shaped under flag OFF**
    (`record_date = valid_from` = the issue date, also native-shaped).

**Existing tests expected to change** (list each in the PR, with before/after):
- `tests/test_quarterly_data_reader.py:134, 245, 407, 443, 654, 718, 985, 1015`, where they assert the old
  mixed-issue or 2-of-3 monthly aggregation, or the flag-OFF output columns.
- Keep the direct-row exclusion assertions for the seven models (e.g. `:293-337`, `:765-810`).
- PP-064's `tests/test_quarter_calendar_window.py` (e.g. A-6 once the maintenance caller keys on Naive
  Mean).
- In `tests/test_quarterly_api_writer.py`, only `:285` writes a raw LR row through the quarter forecast
  writer; `:64` and `:89` are skill-writer tests and are unaffected. Grep the other files that call the
  quarter forecast writer (`test_aggregated_nan_guard.py`, `test_lead_aware_writer_reader_round_trip.py`,
  `test_recalc_workflow.py`, `test_wiring_integration.py`) and list any raw-LR write they assert.
- `tests/test_maintenance_long_term.py:541-574`, `:616` (quarterly dedup, lead-aware) **changes**: its
  `q_combined` and `q_fc` hold one raw model, so the new two-model prefilter excludes the key and nothing
  is saved. Give the fixture two eligible raw models. Any other maintenance test that asserts the run ends
  before the quarterly block (e.g. the early-exit tests at `:217`, `:245`) is listed.

**Acceptance (P1b):**
- The full module suite via `run_tests.sh` (as P1a) is green apart from the test edits listed above; zero
  unexpected skips; only the pre-existing xfail.
- `read_latest_quarterly_forecasts`' existing Source 1 (LR aggregation) is bounded by `forecast_date`
  under **both** flags on the rows it filters itself — both tests above pass, `read_monthly_forecasts`
  is **not** modified (`git diff` shows no change to it), and `aggregate_monthly_fc_to_quarterly` /
  `read_quarterly_forecasts` are otherwise untouched (`git diff` shows no change to either).
- `ruff check` / `ruff format --check` clean on the touched files.
- `git diff --stat` within the P1b file list.

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
  `tests/test_quarterly_skill_metrics.py:265`.
- `tests/test_quarterly_workflow_integration.py` **changes**:
  - `:110-124`: five years and a non-empty quarter skill frame; K = 10 suppresses it. Extend to ≥ 10 years.
  - `:141-157`: ensembles from that skill; same fix.
  - `:301`: `"EM" in result_models` for quarter. Remove the quarter EM expectation. (`:345` is a **season**
    test, `create_seasonal_ensemble_forecasts`, and stays.)
- `tests/test_quarterly_skill_metrics.py:191-204`: five-year assertions (`n_pairs == 5`) → ≥ 10 years.
  `:319`: quarter EM (DB-form LR names).
- `tests/test_quarterly_ensemble_creation.py:194`, `:229`: EM composition and DB-form names.
- `tests/test_lead_aware_aggregated_skill_ensemble.py`: `:168-230`, and the quarter EM loops at `:511`,
  `:567`, `:721`. Its quarter fixtures use five years (`_YEARS_5`, `:44`), so check them against K = 10
  too. The loops at `:583`, `:735` assert empty results and pass as is; the season loops (`:402`, `:538`,
  `:599`, `:614`, `:749`, `:762`) stay; the shared `_ensemble_leads` (`:704`) needs no change.
- For the rest, run `grep -ln '"EM"' tests/*.py`, classify each hit as quarter or season, change only
  quarter, and list every quarter EM assertion changed.
- Keep all season assertions. `tests/test_quarterly_skill_metrics.py:529` and
  `tests/test_quarterly_ensemble_creation.py:458` are **season** tests and stay unchanged.
- K from 5 to 10: `tests/test_lt_min_pairs_gate.py:44` and `tests/test_n_pairs_floor.py:49` share
  `K_QS = 5` between quarter and season; split them into quarter 10 / season 5 without changing the season
  assertions. `tests/test_lt_min_pairs_gate.py:161` asserts the quarter default of 5. Fixtures with 5–9
  quarter years that relied on the default may now be suppressed; list each. The golden fixture sets K = 5
  explicitly (`tests/_skill_lead_aware_golden_fixtures.py:37`), so it is not affected by the default.
- **Flag-OFF golden** (`tests/test_skill_lead_aware_golden_baseline.py:93`): `quarter_skill`,
  `quarter_joint` and `quarter_ensembles` change (no EM, composition-free grouping, per-column quantiles).
  Regenerate **only those keys** with `tests/generate_skill_lead_aware_golden.py` and show in the PR that
  the `month_*` and `season_*` keys are byte-identical.

**Acceptance (P1c):**
- The full module suite via `run_tests.sh` (as P1a) is green apart from the test edits listed above; zero
  unexpected skips; only the pre-existing xfail.
- `ruff check` / `ruff format --check` clean on the touched files.
- `git diff --stat` within the P1c file list.

### P1d — end-to-end

Depends on P1b and P1c. Tests only.
- One December-Q1 chain (raw monthly rows → reader → ensembles → skill), both flags.
- **Flag OFF chain:** reader → ensembles → operational merge with persisted non-native LR rows present →
  Naive/Skilled Mean are built from native plus derived rows only, and the writer emits no LR row.
- **tjhm Q4 after decision F** (tjhm shape, day 1 / lead 0). With aggregate-only LR rows dated 2026-10-01
  at hv0 present, they pass the native rule and suppress the fallback (documents why F must clear them).
  With that population cleared (the post-F state), fallback-derived LR feeds the Q4 2026 Naive Mean and
  Skilled Mean, and no LR row is written, so none is displayed.
- Full suite: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` gives zero
  failures and zero unexpected skips; only the pre-existing xfail remains.
- `ruff check` and `ruff format --check` are clean on the touched files.
- `git diff --stat` touches only the files allowed by P1a–P1d.

### P2 — rollout (with PP-064 Chunk C)

**One writer-paused window** (ops instruction, no code): deploy PP-065, run PP-064 Chunk C step 3
(decision F, tjhm), then the recalc per org. Between deploy and recalc the gates would run on the old,
contaminated quarter skill.
- Pause **every** writer, not just the LT cron days (kghm 10 and 25; tjhm 1): operational runs, the
  maintenance runs (`apps/pipeline/pipeline_docker.py:1946-1972`; `apps/run_locally.sh:1745-1748`), any
  other recalc, and manual runs.
- Wait for running jobs to finish. Then export, mutate, recalc and verify; only then resume.

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
(FD-029). Fallback quarters show no LR row on kghm (round-2 decision 3; hydromet notice); on tjhm a
monthly-derived LR row may still appear as native until decision F, run inside this same window, has
also completed (round-4 tjhm interim).

### P3 — remove the LR fallback

Only after LTF-014 P0 and P2 are deployed on both orgs. **Blocked** while P0 is deferred.

**Files:**
- `src/data_reader.py` (drop the LR fallback branch), tests.
- `README.md`: the passage that documents the active LR fallback, added by DOC-009 row 12 (at trunk the
  row targets `:14, 18-19, 305-320`). Grep `fallback` there and in the repo-root `doc/data_flow_long_term.md`,
  the other row-12 file.

**Acceptance:** before removing it, count per org the quarters that would lose LR rows. For the calendar
quarters of scored years, expect none.

## Out of scope

- Season.
- Fixing the labels upstream (LTF-016).
- Displaying δ bounds (FD-029/FD-030).
- Deleting legacy rows, including old ensemble rows (D8 / PP-041).
- Any writer change beyond the LR and EM skip.
- Making season gap-fill reachable when the monthly block has nothing to do.
