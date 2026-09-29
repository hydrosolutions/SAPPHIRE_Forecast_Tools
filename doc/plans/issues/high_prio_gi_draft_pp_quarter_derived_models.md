# PP-065: Seven models for quarter as same-issue monthly averages; quarterly Naive/Skilled Mean as for monthly

**Status**: P1a **merged to trunk (#530, 2026-09-28)**, presumed already live on both servers via auto-pull
(owner decision R4-merge-is-deploy, 2026-09-28; verify per org, `PP-064.C.step0`). P1b and P1c are next
(parallel, both depend on P1a), then P1d, then rollout (P2). **P1b–P1d merge into the integration branch
`integ_quarter_p1b_p2` (owner decision R4-integration-branch, 2026-09-28), not directly into
`maxat_sapphire_2`** — that branch
merges to trunk only in the P2 window, and that merge is the postprocessing deploy. P1b/P1c must
**reference** the symbols P1a already put on trunk, not
re-create them: the `QUARTER_NATIVE_RAW_MODELS` / `QUARTERLY_DERIVED_MODELS` / `QUARTER_SUPPORTED_MODELS`
constants in `apps/postprocessing_forecasts/src/model_names.py:21-30`, and `clamp_issue_day` /
`clamp_issue_days` (`src/aggregation.py:627, 646`) and `QUARTER_OBS_MIN_MONTHS` (`src/aggregation.py:290`).
`derive_quarterly_from_monthly_same_issue` itself **already exists on trunk** since P1a
(`src/aggregation.py:786-...`, unit-tested in `tests/test_quarter_derived_models.py`), but nothing calls
it yet: `src/data_reader.py`, `postprocessing_maintenance_long_term.py` and `src/api_writer.py` have no
reference to it (measured by grep). The two quarter readers still call the old
`aggregate_monthly_fc_to_quarterly` (`src/data_reader.py:3145, 3499, 3508`). **Wiring the helper into the
readers/maintenance/writer is P1b's job**, not writing the helper itself. Plan drafted 2026-09-26 (rev 6,
after the fourth review round).
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

Paths are relative to `apps/postprocessing_forecasts/`. Citations are to trunk `82946683`; citations
added or corrected on 2026-09-28 are to trunk `6a4ecfae`.

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
7. **P1a amendments approved by the owner (2026-09-28).** A codex plan-conformance check found four
   P1a amendments (below, in the P1a section) that change the ORIGINAL "Target behaviour" spec's
   semantics without an owner decision recorded. The owner has approved all four:
   1. **Parser.** `date`/`valid_from` are parsed with `local_calendar_date`, not
      `pd.to_datetime(..., format="mixed")` (Target behaviour item 1, marked SUPERSEDED below).
   2. **Exact duplicates collapse, not ambiguous.** A row that is an exact duplicate of another --
      same identity key, AND both window ends, AND the point value(s) all match; OR the same `id`
      plus matching values -- collapses to ONE row, instead of making the whole triplet ambiguous
      (Target behaviour item 1's original "Duplicates" sentence, marked SUPERSEDED below).
   3. **Singleton rule scoped to groups of >= 2.** A singleton row at (code, canonical model, `d`,
      hv) is used WHATEVER its `valid_from` -- including an offset or mislabelled window (decision 1
      above) -- with no window match required; the uniqueness rule (the original "exactly one
      matching row" check) applies only within a group of >= 2 rows at that same key (also
      superseding Target behaviour item 1's original sentence).
   4. **Observations: average within the month, then require 3 distinct months.** Duplicate rows of
      one calendar month are averaged together FIRST (skipping NaN); the resulting per-month means
      are what the `QUARTER_OBS_MIN_MONTHS = 3` distinct-month coverage check counts (Target
      behaviour item 8, marked as refined below).
8. **The missing-quarter-config split is intended** (owner decision, first recorded 2026-09-27, restated
   2026-09-28). **P1b: missing quarter config -> FAIL.** Unlike PP-064's `_quarter_native_q1_issue_date`
   (`src/data_reader.py:3072-3083`), which warns and continues (`except
   (LongTermHorizonResolverError, FileNotFoundError)`) because disabling its one narrow admit rule is
   safe, PP-065 P1b's own derivation/read path uses a **narrower** exception tuple that deliberately
   excludes `FileNotFoundError` (`except (UnsupportedLongTermModeError, LongTermHorizonResolverError)`,
   item 2's "Schedule resolution" bullet): a missing `quarter.json` (`FileNotFoundError`,
   `long_term_horizon_resolver.py:184`) is a misconfiguration and **propagates** (FAILS the run) under
   both flags, rather than degrading. This deliberately differs from FD-031's fix on the dashboard side,
   which degrades on the same missing-file case instead of raising -- the two layers own different
   failure modes for the same root cause and are not meant to converge (see FD-031's own note).
   **This FAIL behaviour already exists on trunk today, independent of P1b** -- P1b's job is to
   PRESERVE it, not introduce it: flag OFF already raises via `quarter_horizon_value()`
   (`src/data_reader.py:3198`), reached before any native-row logic; flag ON already propagates via
   `_operational_schedules_for_horizon_type("quarter")` (`:3162`, which calls
   `operational_schedule_for_mode`, itself propagating `FileNotFoundError` from
   `_load_long_term_config`, `long_term_horizon_resolver.py:184`). The warn-and-disable branch on
   `_quarter_native_q1_issue_date`'s own `except (LongTermHorizonResolverError, FileNotFoundError)`
   (`:3072-3083`) is effective only when a plain `LongTermHorizonResolverError` is raised (e.g. a
   lead-only config missing `operational_issue_day`) -- for `FileNotFoundError` specifically, the
   flag-OFF direct read's own `quarter_horizon_value()` call (`:3198`) already raises earlier, before
   `_quarter_native_q1_issue_date` is even reached (its call site is `:3251`), so that branch's
   `FileNotFoundError` case is effectively unreachable via this path.
9. **Native LR precedence wins over PP-064's Problem-7 trunk set** (owner decision
   R4-native-lr-precedence, 2026-09-28; overview § "2026-09-28, round 4"). For direct (non-derived) LR
   rows, the native-row rule (this item's "Direct rows, native-row selection" helper) applies under
   **both** flags: a direct LR row survives only if it passes native-row selection, not PP-064's broader
   "any issue year in `[start_year, end_year]`, any target year" trunk set. Non-native, backfill-shaped,
   and null/unparseable-date direct LR rows are **dropped**, and counted, under both flags. PP-064's own
   Problem-7 widened read (the `start_year − 1` issue-year window and the native-Q1 exception) is
   unaffected in what it **admits** — every row it admits must still pass this rule to survive. PP-064's
   precedence tests (`TestRegressionDirectPrecedenceSurvivesLowerBoundWidening`,
   `TestRegressionBackfillPrecedenceSurvivesLowerBoundTrim`, `tests/test_quarter_calendar_window.py:995,
   1046`) are rewritten with native-shaped fixtures under this decision — see "Owner decision
   R4-native-lr-precedence — native LR precedence, full enumeration" below for the complete list of
   affected/unaffected tests. PP-064's own Problem-7 section (`../high_prio_gi_draft_pp_quarter_calendar_window_validation.md`)
   carries a pointer to this decision.
   - **Degraded LR carve-out (owner). Flag OFF only.** When `quarter.json` lacks `operational_issue_day`,
     no LR row can be
     classified as native or not — the native-row rule cannot run at all. P1b then keeps today's unfiltered
     direct LR selection, with **one** WARNING, instead of dropping every LR row. This is an explicit
     exception to native-row precedence: see "Degraded native rule, flag OFF" below (~:436-443). **Under
     flag ON a missing `operational_issue_day` raises `LongTermHorizonResolverError`, uncaught, as on
     trunk today** (`_operational_schedules_for_horizon_type("quarter")`, `src/data_reader.py:3162`, with
     no try/except around it in the flag-ON branch) — there is no carve-out for it.

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
     - The **helper** parses `date` (and `valid_from`) with `pd.to_datetime(..., format="mixed")`
       **[SUPERSEDED by the amendment below (N7): implemented with `local_calendar_date` instead --
       `format="mixed"` raises `AttributeError` on a subsequent `.dt` access when a column mixes
       tz-aware and tz-naive strings across rows, which the amendment's "Date parsing" bullet and
       `src/aggregation.py:102-201`'s own docstring cover in full; this original bullet is kept for
       history, not as the current contract]**, and matches model names canonically, upper-case
       (`canonical_model_short_series`, `src/model_names.py`).
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
     **[SUPERSEDED by the approved amendments (2) and (3) in "Owner decisions this plan implements"
     above: an EXACT duplicate (same key, both window ends, and values) collapses to one row instead
     of making the triplet ambiguous; a SINGLETON at this key is used whatever its `valid_from`, so
     "zero matching rows" is no longer itself a reason to skip; the uniqueness rule (this sentence's
     "exactly one row ... wins") applies only within a group of >= 2 rows. This original sentence is
     kept for history, not as the current contract -- see the P1a section's "Exact-duplicate
     pre-step and singleton rule" and "A singleton ... is used whatever its `valid_from`" bullets
     below for the current rule.]**
   - **Point value** per month = `q` if the column exists and the value is finite, else `q50`. All three
     must be finite.
   - **Output row:**
     - value = the unweighted mean, the same weighting as quarterly observations. Write it to
       `forecasted_discharge`, and to `q` if that column exists;
     - all quantile columns null;
     - `date = d`, `horizon_value = L`;
     - `valid_from`/`valid_to` = Q's calendar bounds;
     - `year`, `quarter_in_year`.
   - **Logging:** INFO counts per exclusion reason, except `ambiguous_duplicate` and `invalid_config` at
     WARNING (P1a amendment; matches trunk `src/aggregation.py:936-956`, `log_counts()`); counts are also
     returned as a dict (the function's own return value); no station codes. The helper logs its own
     counts internally — **callers must not re-log them.**
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
   - **Order (flag ON).** The full pipeline for a direct LR row, in order: (1) the native-row helper above
     — parses `date`, drops and counts non-native rows AND null/unparseable-date rows. **A `date` column
     entirely absent from the `direct` frame is treated the same as every row's `date` being null.**
     `long_forecasts.date` is `nullable=False`
     (`sapphire/services/postprocessing/app/models.py:159`), so a batch of real API rows can never have
     `date` all-null — this absent-column case is **defensive; it arises only in test fixtures and
     non-API callers** (e.g. a hand-built frame, or a CSV-fed path), not from `_read_long_forecasts_api`
     against the live service. Regardless, the helper must check for the column's presence before parsing
     it, so every LR row is dropped and counted (same reason as an unparseable/null date), with **no
     `KeyError`**. The regression test for this shape,
     `TestR5Observability::test_read_quarterly_forecasts_warns_when_mask_columns_missing`, exercises
     `read_quarterly_forecasts` under **flag OFF**, not this flag-ON pipeline — see the "Order (flag OFF)"
     bullet below for how PP-064's own missing-column guard and this helper interact on that test's row;
     (2) the "Stored
     leads" pre-filter below; (3) `select_operational_issuances`. This matters because
     `select_operational_issuances` calls `pd.to_datetime(candidates[date_col])` **without**
     `errors="coerce"` (`src/data_reader.py:336`; verified) — an unparseable date string reaching that call
     would raise. The native-row helper's own null/unparseable-date drop (step 1) runs first, so an
     unparseable date never reaches `select_operational_issuances` under this pipeline. **Add a flag-ON
     reader test:** a direct LR row with an unparseable `date` (e.g. `"not-a-date"`) is dropped by
     `read_quarterly_forecasts`/`read_latest_quarterly_forecasts` under flag ON, with **no exception**
     raised — this is a regression guard for the ordering, not just the drop itself.
   - **Order (flag OFF).** Under flag OFF, PP-064's Problem-7 issue-year mask and its own
     missing-mask-column guard run FIRST (`src/data_reader.py` ~:3212-3292 — the `elif not lead_aware …`
     drop-mask branch and its sibling `elif not lead_aware … missing_cols` branch), **before** this item's
     native-row helper (decision R4-native-lr-precedence applies the helper under both flags). This
     ordering is required to keep `TestR5Observability::test_read_quarterly_forecasts_logs_dropped_issue_year_count`
     (`tests/test_quarter_calendar_window.py:1477-1508`, one INFO `"Dropped %d quarterly direct forecast
     row(s) issued before the requested year range"`) and
     `TestR5Observability::test_read_quarterly_forecasts_warns_when_mask_columns_missing` (`:1510-1536`, one
     WARNING `"Flag-OFF quarterly issue-year filter skipped: direct rows missing column(s) %s"`) green: if
     the native-row helper ran first and dropped these same rows on its own, the mask's own
     drop-count/missing-column log line would never fire, and both tests would see zero matching log
     records instead of one. **`test_read_quarterly_forecasts_warns_when_mask_columns_missing`'s own row
     is not dropped by PP-064's guard** — that guard only warns and skips the mask when a required column
     is absent, it does not filter `direct` itself — so after it fires, the row still reaches this item's
     native-row helper (step 2), which drops it as unclassifiable for the unrelated, defensive reason
     described under "Order (flag ON)" above (an absent `date` column, treated like an all-null one). Both
     things happen on this row: the WARNING this test locks, then the drop. See "Order (flag ON)" above
     for that rule; it is cited here, not there, because this is the test that actually exercises it — the
     test itself only runs `read_quarterly_forecasts` under flag OFF, never `select_operational_issuances`.
     **`read_latest_quarterly_forecasts` has no Problem-7 mask to order against**
     (`src/data_reader.py:3545-3552`, no such branch) — it has its own, unrelated Problem-6 `forecast_date`
     bound (~:3555-3571). Put this item's native-row helper **after** that bound in this reader too, so its
     existing INFO drop-count (`TestR5Observability::test_read_latest_quarterly_forecasts_logs_dropped_future_issue_count`)
     is unchanged by this item.
   - **Stored leads (flag ON).** Before `select_operational_issuances`, drop and count direct rows whose
     stored `horizon_value` differs from the derived lead, then call it with `lead_output_cols=()` so the
     stored value is preserved. `select_operational_issuances` itself is not modified — it keeps matching
     the **unclamped** issue day (PP-066, which owns that gap), so under flag ON a clamped-day-only-valid
     row is dropped here even though the native-row helper above would have classified it correctly.
   - **Derived rows.** Read raw monthly rows via `_read_long_forecasts_api` for issue years
     `start_year − 1 … end_year`. In the latest reader, also require issue date ≤ `forecast_date`. Derive
     for the seven models; while the fallback is active, also derive each LR model for (code, year,
     quarter) keys with no selected native row of that model.
     - **Interfaces (reuse trunk's own symbols, do not re-create them).** Resolve the quarter schedule once
       per reader call via `operational_schedule_for_mode("quarter")` (shared with the native-row rule and
       `_quarter_native_q1_issue_date`, see "Schedule resolution" below); parse `date`/`valid_from` with
       `local_calendar_date` (`src/aggregation.py:102`), not `pd.to_datetime`; clamp issue days with
       `clamp_issue_days` (`src/aggregation.py:646`), not a hand-rolled clamp; call
       `derived, _counts = derive_quarterly_from_monthly_same_issue(renamed_monthly, schedule.lead_time,
       schedule.issue_day, QUARTERLY_DERIVED_MODELS)` for the seven models, and the same call with
       `QUARTER_NATIVE_RAW_MODELS` in place of `QUARTERLY_DERIVED_MODELS` for the decision-G LR fallback.
       Do **not** re-log `_counts` — the helper already logs its own counts internally (see the Logging
       bullet above). Treat a resolved `schedule.issue_day < 1` as an additional degraded-native-rule
       trigger, the same way PP-064 A's `_quarter_native_q1_issue_date` (`src/data_reader.py:3085-3093`)
       and FD-029 already do — on top of the helper's own internal `invalid_config` handling for that same
       condition (`src/aggregation.py:952-956`), which only covers calls already reached; the reader-level
       trigger is what decides whether to call the helper (and the native-row rule) at all.
     - **Target-year trim scope.** Trim **only** the derived rows (this item's output, both the seven
       models and the decision-G LR fallback) to `[start_year, end_year]` (the latest reader:
       `[start_year, end_year + 1]`, matching its existing next-year-Q1 allowance). Do **not** add a new
       target-year trim to direct rows. **Owner decision R4-native-lr-precedence (2026-09-28): the native rule wins over
       PP-064's Problem-7 "trunk set" for LR direct rows.** Flag OFF: direct LR rows follow the
       **native-row rule** (the shared helper above, "Direct rows, native-row selection") under **both**
       flags, not PP-064's broader "any issue year in `[start_year, end_year]`, any target year" set.
       Non-native LR rows — including backfill-shaped rows (e.g. issued 2025-01-10 targeting Q4 2024) —
       are **dropped**, not kept. The Problem-7 `start_year - 1` widened read and the native-Q1 exception
       (`_quarter_native_q1_issue_date`) **stay in `read_quarterly_forecasts` only, unchanged**: they
       widen which **years** of direct rows are read, but every row they admit must still pass the
       native-row rule to survive. `read_latest_quarterly_forecasts` under flag OFF reads from
       `start_year` with no Problem-7 branch (`src/data_reader.py:3545-3552`) and none is added by P1b.
       Flag ON: the direct-row target-year trim already exists (`_trim_to_target_year_range`,
       `src/data_reader.py:3209`) and is unaffected by this item; that `:3209` trim **predates #527** (it
       is from the earlier M1 P1 config-driven operational-issuance selection work), and #527 added the
       latest reader's own `end_year + 1` variant (`:3582`). `TestRegressionDirectPrecedenceSurvivesLowerBoundWidening`
       and `TestRegressionBackfillPrecedenceSurvivesLowerBoundTrim`
       (`tests/test_quarter_calendar_window.py:995, 1046`) are **rewritten with native-shaped fixtures**
       under this decision — see "Owner decision R4-native-lr-precedence — native LR precedence, full
       enumeration" below (`TestRegressionBackfillPrecedenceSurvivesLowerBoundTrim` splits into two tests
       there) — so they lock the **combine precedence** this plan actually needs (a native direct row
       suppresses the decision-G fallback derivation for its key), not the old backfill/trunk-set
       precedent.
   - **Existing Source 1 (LR aggregation), latest reader, both flags — SUPERSEDED (owner decision
     2026-09-28, "PP-065 P1b replaces Source 1").** There is no longer a separate, bounded-but-otherwise-
     unchanged old LR aggregation path to maintain: `read_latest_quarterly_forecasts`' pre-existing
     monthly-derived path for LR_Base/LR_SM (`aggregate_monthly_fc_to_quarterly`) is **replaced**, not
     bounded, by the same "Derived rows" mechanism above — `derive_quarterly_from_monthly_same_issue`
     called with `QUARTER_NATIVE_RAW_MODELS` as the decision-G fallback. The `forecast_date` bound this
     bullet originally added to the old path is achieved for free by that unification: "Derived rows"
     above already reads via `_read_long_forecasts_api` and, in the latest reader, already requires issue
     `date <= forecast_date`. The two tests below still apply, now to the unified derive-based path, not
     to `aggregate_monthly_fc_to_quarterly`. **[historical text below, kept for context, not the current
     contract]**
     `read_latest_quarterly_forecasts`' pre-existing monthly-derived path for LR_Base/LR_SM (unrelated to
     the "Derived rows" step above, which is this plan's new seven-model/fallback mechanism) has no bound
     against `forecast_date` today: flag ON calls `read_monthly_forecasts(codes, start_year, end_year)`
     (`data_reader.py:3415`), flag OFF calls raw `_read_long_forecasts_api(codes, start_year, end_year)`
     (`:3423`) — neither takes `today`/`forecast_date`, so a back-dated run could aggregate a monthly row
     issued after it into a quarter that should not be visible yet. This plan rewrites this reader (this
     item, above), so it owns bounding it: filter to issue `date <= forecast_date` (null kept) on the
     rows this reader itself receives — `read_monthly_forecasts`' output under flag ON, the raw rows
     from `_read_long_forecasts_api` under flag OFF — any time **before** they reach
     `aggregate_monthly_fc_to_quarterly`. `read_monthly_forecasts` itself is **not**
     modified: filtering before or after its internal `select_operational_issuances` call is equivalent
     here, because that call derives the lead from (issue month, target month) and additionally requires
     the configured issue *day* (`data_reader.py:346-354`) — so a fixed (target month, lead) pins the
     candidate issue date to one exact calendar date, leaving no same-unit "earlier eligible vs. later
     ineligible reissue on a different day" case for filter placement to matter for.
     - **Test:** `forecast_date = 2026-06-25`; monthly LR rows issued 2026-09-25 and 2026-10-25 (both
       after `forecast_date`) must not produce a Q4 aggregate — under both flags.
     - **Test:** no row dated after `forecast_date` reaches the derivation input (assert on a spy, or on
       the rows actually passed to `derive_quarterly_from_monthly_same_issue`) — under both flags.
   - **[SUPERSEDED by the "Target-year trim scope" bullet above.]** ~~Trim to the requested **target**
     years. In the latest reader, target year `today.year + 1` is allowed, so a 25 Dec issue yields next
     year's Q1.~~ As originally written this applied the trim to ALL rows; the current contract restricts
     the target-year trim to DERIVED rows only (this item's own new mechanism). **Flag OFF direct rows do
     NOT keep PP-064's Problem-7 invariant unchanged — this sentence, as originally written, is corrected
     by owner decision R4-native-lr-precedence (above, "Target-year trim scope", ~:341) and no longer
     describes the current contract.** Only the specific claim about the **target-year bound** stays true
     without qualification: this item does not add a NEW target-year trim to direct rows (Problem-7's own
     `start_year − 1` widened read is untouched, and flag ON's existing `_trim_to_target_year_range` trim,
     `src/data_reader.py:3209`, is unaffected by this item). But which direct rows **survive at all** is a
     separate question, and for LR rows specifically it changed under R4-native-lr-precedence: flag OFF
     direct LR rows are no longer PP-064's full "any issue year in `[start_year, end_year]`, any target
     year" trunk set — they must also pass the native-row rule, under **both** flags. Non-LR direct rows
     (the seven derived-eligible models) are unaffected here because they are dropped from the direct
     source entirely by a different bullet ("Drop direct rows of the seven models before the sources are
     combined", below) — the native-row rule specifically concerns LR precedence. Kept for history, not as
     the current contract.
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
   - **Schedule resolution, flag OFF.** Resolve the quarter operational schedule (`operational_schedule_for_mode
     ("quarter")`) **once per reader call** and share the same resolved `schedule` object with the native-row
     rule, the derivation calls above, and `_quarter_native_q1_issue_date`'s own Problem-7 exception
     (`src/data_reader.py:3072-3083`) — so the degraded case (schedule unresolvable) logs exactly **one
     schedule-resolution WARNING** per reader call, not one per call site. **Scope of "exactly one":** this
     invariant covers only the schedule-resolution WARNING this bullet consolidates (the two sub-cases
     below). It says nothing about, and does not reduce, any other pre-existing WARNING in the same reader
     call — in particular PP-064's Problem-7 missing-column "filter skipped" guard
     (`../high_prio_gi_draft_pp_quarter_calendar_window_validation.md`, `src/data_reader.py:3280-3292`,
     `logger.warning("Flag-OFF quarterly issue-year filter skipped: …")`) is unchanged and additional: a
     call missing `quarter_in_year`/`date` on `direct` AND hitting an unresolvable schedule logs both
     warnings, not one. The two locked tests below already scope their own assertion to "one warning
     matching that substring", not "one warning total in the call" — keep that scoping when writing or
     reviewing any test that counts warnings here. `_quarter_native_q1_issue_date(start_year)`
     (`src/data_reader.py:3045`) currently resolves the schedule itself (`:3073`), and its only call site
     is `:3251` in `read_quarterly_forecasts`. Sharing the resolved schedule with it therefore needs one
     additive change to its signature, and only this one: an optional keyword-only parameter (e.g.
     `schedule=None`) — when passed, the helper uses it instead of re-resolving; when omitted, behaviour
     is unchanged and the existing tests keep calling it without the parameter. This is the one permitted
     signature change in P1b. **Do not** reuse
     `_quarter_native_q1_issue_date`'s own `except (LongTermHorizonResolverError, FileNotFoundError)` for
     the derivation calls: that tuple's `FileNotFoundError` branch is specific to the Problem-7
     native-Q1-date exception (warn-and-continue is safe there because it only disables one admit rule),
     and does **not** apply to the derivation/read path, where a missing config file FAILS (item 8,
     "the missing-quarter-config split is intended" — see the overview's owner decisions). The derivation's
     own `except (UnsupportedLongTermModeError, LongTermHorizonResolverError)` (above) is a **different,
     narrower** tuple that deliberately excludes `FileNotFoundError`.
     - **When the reader's own shared resolution fails.** The optional `schedule=None` parameter has no way
       to signal "already tried, and it failed" — passing `schedule=None` back to
       `_quarter_native_q1_issue_date` after the reader's own `operational_schedule_for_mode("quarter")`
       call already raised would make the helper **re-resolve** the same schedule, hit the same
       `LongTermHorizonResolverError` (or the invalid-`issue_day < 1` case) a second time, and log a
       **second** WARNING — breaking the "exactly one WARNING per reader call" invariant this bullet
       requires. Instead: when the reader's single shared resolution fails (a caught
       `LongTermHorizonResolverError`, or a resolved schedule with `issue_day < 1`), the reader logs its
       own one WARNING there and **skips the `_quarter_native_q1_issue_date` call entirely** — equivalent
       to the helper itself having returned "no admit", so no Problem-7 exception is applied. There are
       **two distinct sub-cases**, and the reader's replacement warning must keep BOTH locked substrings,
       one per sub-case — a single generic message covering both would break one of the two tests below.
       On current trunk (`src/data_reader.py:3075-3093`) `_quarter_native_q1_issue_date` already logs two
       differently-worded messages for these two sub-cases, and the reader's own single-WARNING logging
       must preserve both wordings, branched the same way:
       - **Unresolvable schedule** (`operational_schedule_for_mode` raises `LongTermHorizonResolverError`):
         the reader's message must contain the substring `"quarter operational schedule"` (lower-case,
         exact — the test's filter is a plain `in` on `r.message`, case-sensitive). Locked test:
         `tests/test_quarter_calendar_window.py:2178-2207`
         (`test_unresolvable_schedule_drops_prior_year_q1_and_logs_one_warning`, exactly one warning
         matching that substring) must stay green.
       - **Resolved schedule with `issue_day < 1`:** the reader's message must contain the substring
         `"invalid issue_day"` (lower-case, exact — same case-sensitive `in` filter). Locked test:
         `tests/test_quarter_calendar_window.py:2210-2237`
         (`test_invalid_issue_day_drops_prior_year_q1_and_logs_one_warning`, parametrized `issue_day in
         [0, -1]`, exactly one warning matching that substring) must stay green. This sub-case and its
         substring requirement are **not called out by the "Owner decision R4-native-lr-precedence — native LR precedence" test
         enumeration below**, which correctly notes this test is unaffected by decision R4-native-lr-precedence itself — that
         classification is about *decision R4-native-lr-precedence*, not about *this* item's shared-resolution refactor, which
         is what actually determines whether `:2210` keeps passing.
   - **Model filter.** Both quarter readers currently call `_filter_supported_aggregated_forecast_models`
     (`src/data_reader.py:98-104`) after combining sources. The **quarter** call sites are `:3310`
     (`read_quarterly_forecasts`) and `:3606` (`read_latest_quarterly_forecasts`) only; `:3432`
     (`read_seasonal_forecasts`) and `:3730` (`read_latest_seasonal_forecasts`) are the SEASON readers and
     are unrelated to this item, named here only to avoid ambiguity. The call keeps
     only rows in `AGGREGATED_SUPPORTED_MODELS` (LR + the three ensemble aggregates,
     `src/model_names.py:14-16`) — that would silently discard every derived seven-model row this item
     produces. Change the two **quarter** readers' post-combine filter to keep `QUARTER_SUPPORTED_MODELS`
     (`src/model_names.py:28-30`) instead of `AGGREGATED_SUPPORTED_MODELS`. The **season** readers keep
     calling `_filter_supported_aggregated_forecast_models` with `AGGREGATED_SUPPORTED_MODELS` unchanged
     (season is out of scope, item 6).
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
     - **EM interim (current state, owner decisions R4-merge-is-deploy/R4-integration-branch,
       2026-09-28).** The EM write this bullet describes
       **predates PP-064 A** (`api_writer.py:1157-1158` is unchanged from the pre-#527 branch), so it is not
       trunk-only: `ensemble_calculator.py` sets `model_short = "EM"` directly in the quarter aggregation
       path (`_create_aggregated_ensemble_forecasts:765`), and `api_writer.py`'s quarter-write loop
       (`:1157-1158`) resolves that through `MODEL_TYPE_MAP`'s identity `"EM": "EM"` entry
       (`api_writer.py:30`), not the `"ENSEMBLE_MEAN": "EM"` entry (line 50, which serves the skill-metrics
       write path only). This write is live on servers today, and will keep running until this item (P1b)
       merges. **FD-029, which hides every quarter EM row it reads on the dashboard side, is also live on
       servers today** (owner decision R4-merge-is-deploy: it deploys via the dashboard's own daily
       frontend auto-pull, not only with PP-065 P2). **Corrected 2026-09-28 (factual, not a decision
       change): "a fresh EM row plus a non-native LR row" cannot be the whole story.** EM's per-key gate is
       `em_avg[em_avg["composition"].apply(is_multi_model_composition)]` (`ensemble_calculator.py:767`) —
       the same `is_multi_model_composition` predicate as Naive Mean's gate (`:915`), not the coarser
       `n_models > 1` whole-frame precondition (`:744`, which only counts distinct qualifying models
       across the entire frame before the per-key groupby runs). Pre-P1b these are, in effect, the
       identical condition — both require
       `LR_Base` **and** `LR_SM` present with a non-null `forecasted_discharge` at the key, because those
       two are the only non-baseline models the pipeline reads for quarter today. So any key with a
       *fresh* EM row from this pipeline also got a fresh Naive Mean row in the same run, and FD-029 does
       not hide Naive Mean — that key is not blank. The actual blank-card population is narrower: a key
       where **at most one** of `LR_Base`/`LR_SM` has a non-null target-quarter forecast (neither gate
       fires, so there is no EM and no Naive Mean), and the one row that does exist, if any, is
       non-native. (A search for keys with an EM row but no paired Naive Mean row from a stale/historical
       write found none: `_add_naive_mean_aggregated_ens` has existed since the same commit that first
       added quarter aggregation at all, `abfa78fe`, 2026-03-04 — Naive Mean has never been absent from
       this pipeline for quarter.) Confirm the precise count at the pre-deploy DB audit (PP-064 Chunk C
       detail 2), not by assumption. **The
       recovery point is not the branch merge itself** — merging `integ_quarter_p1b_p2` to trunk
       (`deploy.pp`) only puts this item's code on the servers, it writes no rows. The blank card clears
       only once the **first successful quarterly postprocessing run on the new image**, in the P2 window
       (§ "P2 — rollout" below), has actually produced the derived/ensemble rows, and that run's output is
       verified — at which point this item's stopped EM write and the derived/ensemble rows take
       over.
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
8. **Observations: 3 of 3 months** (rev-3 PP-064 "B4"). **Merged to trunk (P1a, #530).**
   `QUARTER_OBS_MIN_MONTHS = 3` is on trunk at `src/aggregation.py:290`, used at `:371, 397`, unweighted
   (line citations corrected from the original `:125-126`, drifted since this item shipped). This also
   changes δ (`aggregate_monthly_obs_to_quarterly`, `:403-409`, corrected from the original `:132-141`),
   which is `0.674 * std(discharge_avg)` computed only from the years that survive the
   `QUARTER_OBS_MIN_MONTHS` filter. `QUARTER_MIN_MONTHS` (`src/aggregation.py:284`, corrected from the
   original `:38`) is unchanged, `= 2`.
   **[Refined by the approved amendment (4) in "Owner decisions this plan implements" above: a
   DUPLICATE row for one calendar month is averaged together with its own month's other row(s)
   FIRST, skipping NaN, and it is that per-month mean the "3 of 3" DISTINCT-month count is over --
   see the P1a section's "Distinct-month observation counting" bullet below for the current rule.]**

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
`tests/test_aggregation.py:172`), `test_multiple_stations` (`:260`) and `test_multiple_quarters`
(`:318`), each of which relied on 2 of 3 months (re-measured on this branch's HEAD after the N3
`test_duplicated_month_is_averaged_not_first_or_max` addition shifted both down from their
original `:240`/`:298`). `tests/test_aggregation.py:42-43` (`QUARTER_MIN_MONTHS == 2`) is unchanged.

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

- **Date parsing** (owner-approved amendment (1), "Owner decisions this plan implements" above,
  2026-09-28). `date` and `valid_from` are parsed with `local_calendar_date`
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
  (`src/aggregation.py:996-1017` for the reordered hv checks, before the date parse at `:1022`).
- **`horizon_value` rule.** `hv = pd.to_numeric(col, errors="coerce")`; a row is eligible only if `hv`
  is finite and `hv == round(hv)`; cast to `int` only AFTER that filter (real API frames carry `hv` as
  float64 with NaN).
- **Output column set.** A FIXED list, never built by copying input rows: `code`, `model_short`,
  `year`, `quarter_in_year`, `date`, `horizon_value`, `valid_from`, `valid_to`,
  `forecasted_discharge`, `q` (present only if the input had a `q` column), and every column of
  `_FC_QUANTILE_COLS` (NaN). Columns like `id`, `flag`, `composition`, `q_obs`,
  `model_type_description` and `horizon_type` never leak into the output.
- **Exact-duplicate pre-step and singleton rule (2026-09-27 fix; extended 2026-09-27 after round-2
  and round-3 review, `src/aggregation.py:1082-1150`; owner-approved amendments (2) and (3), "Owner
  decisions this plan implements" above, 2026-09-28).** Before the uniqueness rule, a row is an
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
    Each of the three PARTITION-dependent tests (H2/J2's own `drop_duplicates`-subset-per-partition
    concern) now has its own `valid_to`-only variant (same `valid_from`, different `valid_to`, same
    value) alongside its existing `valid_from`-only one. The UNPARSEABLE-string variant is different:
    there is exactly one `valid_from` version and one `valid_to` version, both in the no-`id`
    partition -- that is sufficient, because `_window_dedup_key` (the fallback rule itself) runs
    ONCE per window column, on the whole frame, BEFORE the `id`/no-`id` split; a bug in the fallback
    rule is not partition-dependent the way the with-`id`/without-`id`/no-`id` `drop_duplicates`
    SUBSET LISTS are, so one test per column end is enough to pin it (finding K2, below).
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
    windows.
  - **INPUT CONTRACT.** A well-formed `valid_from`/`valid_to` value is an ISO date/datetime string, a
    `date`/`datetime`/`Timestamp`, or null. Any OTHER value is out of contract; `_window_dedup_key`
    (below) itself still classifies it best-effort and never raises, but the specific key it assigns
    (a key unique to that one row) is a deliberate refusal to reason about it, not a guarantee about
    its meaning. Separately, `id` (when present) must be a hashable scalar, as the API guarantees;
    an `id` value that is itself unhashable is out of scope for this helper (it is not a documented
    input shape, unlike an out-of-contract window value, which upstream data quality issues make a
    real possibility). `derive_quarterly_from_monthly_same_issue` as a whole is guaranteed not to
    raise only when its inputs meet this contract (also `horizon_value`/`q`/`q50` numeric-or-null,
    per the P1 fix below): an out-of-contract window value, e.g. `Decimal("sNaN")`, is still
    classified safely by `_window_dedup_key` and does not corrupt the result, but MAY still raise
    later -- confirmed for `Decimal("sNaN")` in `valid_from`/`valid_to` (inside the `id`-present
    branch's `pd.concat`, `src/aggregation.py:1147`), in `horizon_value` or `q`/`q50` (inside
    `pd.to_numeric` itself), and for an unhashable `id` (also inside that same `pd.concat`) -- this
    downstream risk is out of scope, not a gap in `_window_dedup_key` (see the P1 follow-up below).
  - **Where a window value does NOT parse, the fallback rule is the FINAL, SIMPLE and conservative
    one in `_window_dedup_key`** (the shared helper, `src/aggregation.py:709-783`; findings J1, K1,
    L1, M1), applied per value in this exact order, with the WHOLE classification wrapped in one
    `try`/`except Exception` so that NO step -- including the null check itself -- can ever raise
    out of `_window_dedup_key` ITSELF (M1; a guarantee about this helper, not about
    `derive_quarterly_from_monthly_same_issue` as a whole -- see the INPUT CONTRACT bullet above): a
    genuinely null SCALAR (`pd.api.types.is_scalar(v) and pd.isna(v)` --
    `None`, NaN of any float width, `pd.NA`, `NaT` of any flavour) -> `None`, so two nulls always
    match, however differently spelled; else, if the value is EXACTLY a `str` (`type(v) is str`,
    never `isinstance`, which would also admit a `str` SUBCLASS whose own `__hash__`/`__eq__` can
    raise) -> the string itself, so `"garbage" == "garbage"` still collapses and `"garbage" != "xx"`
    never does; else (any other non-null, unparseable value -- a list, dict, ndarray, a `str`
    subclass, or any value that raises merely from being classified, e.g. `Decimal("sNaN")`, whose
    whole point is to raise on inspection) -> a key unique to that row's OWN POSITION, so it is never
    equal to any other row's key and neither `str()` nor `hash()` is ever called on the value. This
    rule went through four revisions: H3's first version keyed purely on the parsed value, so two
    DIFFERENT unparseable strings (e.g. `"garbage"` vs `"xx"`) both became `NaT` and therefore
    compared EQUAL to each other -- silently collapsing a pair the pre-H3 code correctly left
    ambiguous (J1). J1's fix kept the raw value itself as the fallback, which raised `TypeError:
    unhashable type` the first time an UNHASHABLE raw value (e.g. a list or dict from malformed
    upstream data) reached `drop_duplicates` (K1). K1's fix built a type-qualified string
    (`f"{type(v).__name__}|{v!s}"`), which had its OWN holes: `pd.NA`/`np.datetime64("NaT")`/
    `np.float32("nan")` are not `None` and not `isinstance(v, float)`, so two copies of an
    otherwise-identical row differing only in WHICH null flavour they used were wrongly ambiguous;
    `str(v)` can itself raise on an exotic object; and two different same-type objects can collide if
    their `str()` representations happen to be equal (e.g. two long `ndarray`s whose default,
    truncated repr is identical) (L1). L1's own version -- `pd.isna(v)` with no `try`/`except` around
    it, and `isinstance(v, str)` -- still raised on `Decimal("sNaN")` (a signalling NaN, which raises
    `decimal.InvalidOperation` merely from being tested for null) and would have let a `str` subclass
    with a raising `__hash__` reach `drop_duplicates` (M1). The row-position key is deliberately NOT
    a general normal form for arbitrary objects -- it forces every such case apart (a false
    "ambiguous", never a wrong collapse), which is the safe failure mode. A genuinely null
    `valid_from`/`valid_to` still matches another null.
  - **A deterministic spelling tiebreak** (round-3 finding H4): when two rows are exact duplicates in
    every respect except the stored `model_short` spelling (they share one canonical model), the rows
    are sorted by that raw spelling (a stable sort) before the drop, so the lexicographically smallest
    spelling wins regardless of input row order. The original version left this to whichever row
    `drop_duplicates(keep="first")` happened to see first.
  - A singleton at (code, canonical model, `d`, hv) is used whatever its `valid_from`, including
    missing/NaT.
- **`bad_key` (2026-09-27, round-2 finding G4).** A null `code` or `model_short` is excluded and
  counted as `bad_key` BEFORE anything groups on `code` (`src/aggregation.py:972-986`, the first
  per-row filter in the function, before even the model-scope check). Pandas `groupby` drops a null
  group key by default, so a null-`code` row's boolean `.transform()` result came back as `NaN`
  instead of `True`/`False`, and `~NaN` raised `TypeError` at the ambiguity check further down --
  this made the ENTIRE call crash, not merely mis-handle the one bad row.
- **`code` (and date/window) output dtype (2026-09-27, round-2 finding G5).** The non-empty result's
  `code`, `date`, `valid_from` and `valid_to` columns are explicitly cast to `object`
  (`src/aggregation.py:919-934`, in `typed()`), matching `empty_result()`'s hardcoded object dtype
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
  (`apps/long_term_forecasting/lt_utils.py:170-172`) and PP-064's `data_reader.py:3097-3101`.
  **PP-065 N5:** a public vectorized twin, `clamp_issue_days(dates, issue_day)` (same rule via
  `Series.dt.days_in_month`, `src/aggregation.py:646-672`), replaced `derive`'s own inline
  `np.minimum(issue_day, df["_d"].dt.days_in_month)` call and is unit-tested against the scalar
  helper across mixed month lengths (leap/non-leap Feb, a 30-day month, `NaT`). P1b's native-row
  rule reuses THIS vectorized helper, not the scalar one, for its own per-row clamp. **P3 fix:** its
  return dtype is NOT fixed -- `int32` when `dates` has no `NaT`, `float64` when it does (since
  `Series.dt.days_in_month` itself upcasts to hold NaN for a `NaT` row) -- documented rather than
  forced to one dtype via an extra cast, since a `Series.dt.day != this` comparison (the only
  caller) works correctly either way and a float64 cast would needlessly change that comparison's
  dtype in the common no-`NaT` case.
- **Distinct-month observation counting** (owner-approved amendment (4), "Owner decisions this plan
  implements" above, 2026-09-28). `aggregate_monthly_obs_to_quarterly` first averages per
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
  the J1-J3 follow-up, the K1-K2/L1-L2 follow-up, the M1-M3 follow-up, the N1-N7 follow-up, AND the
  P1-P3 follow-up (below) -- re-verify again after any further edit to `src/aggregation.py`, since
  these drift with every change above them in the file): `local_calendar_date` is at `:102-201`; the
  observation coverage filter is at `src/aggregation.py:397`; the delta computation is at
  `:403-409`; `QUARTER_MIN_MONTHS` is defined at `:284` and remains used only by
  `aggregate_monthly_fc_to_quarterly` (forecast aggregation, unchanged by this phase);
  `clamp_issue_days` (PP-065 N5's public vectorized clamp) is at `:646-672`; the shared
  `_window_dedup_key` helper (the SIMPLE, conservative, exception-safe-by-construction
  unparseable-value fallback, finding M1) is at `:709-783`; the `typed()` function (object-dtype
  cast included) is at `:919-934`; `bad_key` is at `:972-986` (its own early return, one line after
  `log_counts()`, not on the same line as the filter itself); the derivation helper's hv-check
  reorder is at `:996-1017`; the date parse itself is the single line at `:1022`; the
  exact-duplicate pre-step (window-parsing via `_window_dedup_key` for BOTH `valid_from` and
  `valid_to`, the spelling-tiebreak sort, and the value- and natural-key-scoped `id` dedup, across
  all three partitions) is at `:1082-1150` (the `id`-present branch's own `pd.concat` is at
  `:1147`); the vectorized uniqueness/triplet-assembly rewrite (see "Performance" below) spans
  `:1152-1287` (the end of the file). Every one of these moved again from the N1-N7 HEAD
  (`8aff8cc6`): the P1 INPUT CONTRACT rewrite and the P3 `clamp_issue_days` docstring fix both grew
  the file (net +16 lines by end of file) -- each citation above was re-measured directly on this
  round's HEAD.
- **Performance (2026-09-27, PP-065 F3).** The original P1a implementation grouped rows with Python
  `for key, group in df.groupby(...)` loops for both the uniqueness rule and the final triplet
  assembly -- correct, but O(rows) in Python, measured at 8.5-38s for a ~200k-row synthetic monthly
  frame depending on the reviewing environment. It was rewritten to be fully vectorized: group size
  and match count via `groupby().transform()`, the triplet check and mean via a pivot/unstack on
  `horizon_value`, and month arithmetic/clamping via integer "months since epoch" arithmetic and
  `Series.dt.days_in_month` instead of per-row Python calls. Measured on the same 200k-row synthetic
  frame (same machine): 30.4s -> ~0.65s (about 47x) after the round-2 fixes, ~0.75s after the round-3
  fixes below (H3's window parsing and H4's sort add a little back), ~0.83-0.84s from the K1/L1
  rewrite of `_window_dedup_key` onward (unchanged again after M1's try/except wrap, since it only
  guards checks that were already cheap), still comfortably under the 2s target. A real-data re-run
  (8 org/mode combinations) landed the same conclusion independently:
  byte-identical output to the pre-vectorization commit in all 8 runs, 4-13x faster, counts changing
  only by the intended out-of-scope-row exclusion (F5's month_0-style fix).

  A same-author, deliberately non-vectorized reference copy of the pre-rewrite logic
  (`_reference_derive` in `tests/test_quarter_derived_models.py`) is checked against the production
  function by a randomized differential test (300 generated frames, fixed seed). This is an
  equivalence check for the vectorization, NOT an independent oracle -- both sides were written by
  the same author from the same spec understanding, so agreement proves the rewrite didn't change
  behaviour, not that either implementation is correct. The generator draws 1-3 fake
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
  29 and June 30 clamps, a same-`id` conflict, a single-match-in-a-group win, and -- PP-065 N2 -- two
  NON-progression-value cases, since every one of the others uses an arithmetic progression where
  mean, median and the positional middle column all coincide), computed directly from the spec
  rather than via either implementation, lives in `TestHandComputedSpecDerivedResults` in the same
  test file. THIS is the actual spec oracle, independent of both implementations -- the differential
  test above is not. Keep `_reference_derive` UNEDITED except in lockstep with production -- fix
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

  **Round-4/5 follow-up (2026-09-27, K1-K2, L1).** Two further confirmation passes (real-data re-run
  identical each time; independent codex and Claude reviewers) found three more items, all on the
  unparseable-window fallback introduced by J1. K1 (codex, code): an UNHASHABLE unparseable value
  (e.g. a list or dict) was kept as J1's raw fallback key, and `drop_duplicates` requires every key
  value to be hashable, so it raised `TypeError: unhashable type` the first time such a row appeared.
  K1's own first fix replaced the raw value with a type-qualified string
  (`f"{type(v).__name__}|{v!s}"`), which is what the exact-duplicate bullet above described until
  this round. K2 (Claude, test pins): `valid_to`'s presence in the window key was completely
  unpinned -- see the corrected coverage claim above. L1 (codex + Claude, code): K1's type-qualified
  string fallback still had three holes -- `pd.NA`/`np.datetime64("NaT")`/`np.float32("nan")` are
  not caught by `v is None` or `isinstance(v, float)`, so two copies of the same row differing only
  in null flavour were wrongly ambiguous; `str(v)` can itself raise on an exotic object (the
  contract `local_calendar_date` promises never to violate for ANY input); and two different
  same-type objects can collide if their truncated `str()` happens to match. `_window_dedup_key` was
  rewritten to be SIMPLE and conservative instead of patched further: null via
  `pd.api.types.is_scalar(v) and pd.isna(v)` (catches every flavour), an unparseable string compared
  as itself, and everything else non-null and unparseable given a key unique to its own row position
  -- never `str()`'d or `hash()`'d. See the exact-duplicate bullet above for the current rule and the
  "Updated line citations" bullet for its location.

  **Round M1-M3 follow-up (2026-09-27).** Final reviews of L1 (real-data re-run identical again --
  seven versions of the derivation now agree byte-for-byte; codex and Claude both agreed the L1 rule
  was right in spirit) still found two gaps, both in exotic-input handling rather than in the
  documented input contract. M1 (code, both implementations): L1's per-value classification was not
  wrapped in `try`/`except`, so it could still raise for pathological inputs outside the documented
  contract -- `pd.isna(Decimal("sNaN"))` raises `decimal.InvalidOperation` (a signaling NaN is
  designed to raise on inspection), and `isinstance(v, str)` admits a `str` subclass with a
  deliberately raising `__hash__`, which `drop_duplicates`'s internal hashing then triggers.
  `_window_dedup_key` now wraps the whole per-value classification in `try`/`except Exception`, uses
  `type(v) is str` (not `isinstance`, which is exactly what let the raising-`__hash__` subclass
  through) for the exact-string branch, and on any exception falls back to the same
  row-position-unique key L1 already used for other unparseable objects -- so the function is
  exception-safe *by construction*, not by enumerating exotic types. Two new tests pin this:
  `Decimal("sNaN")` in `valid_from` and a `str` subclass with a raising `__hash__`, in each case
  asserting the control derives without an exception; mutation-verified (dropping the `try`/`except`
  makes the `sNaN` test raise `decimal.InvalidOperation`; reverting to `isinstance` makes the
  str-subclass test raise `RuntimeError` from inside `drop_duplicates`'s hashing). M2 (test pins):
  two of the three parametrizations of the null-flavour-equivalence test were hollow --
  constructing a `pd.DataFrame` from a list of tuples silently coerces an all-null-ish column to a
  uniform `datetime64[ns]` or `float64` dtype, erasing the exact `np.datetime64("NaT")` /
  `np.float32("nan")` scalar types the test claimed to exercise before `_window_dedup_key` ever saw
  them, so a mutant that narrowed the null check still passed. Fixed by adding a co-located,
  non-conflicting row with a parseable string `valid_from` (forcing the whole column to `object`
  dtype) and asserting the actual per-cell types survive construction before calling derive;
  mutation-verified (narrowing the null check to `None`/`NaT`/`NA`/`float` makes exactly the
  `np.datetime64`/`np.float32` parametrizations fail, leaving `pd.NA` passing, which is the precise
  discriminating signature). Separately, the exact-string fallback branch had no positive test at
  all -- two new tests assert that two rows sharing the same unparseable string (`valid_from` and
  `valid_to` variants) collapse to one derived row with no `ambiguous_duplicate`, mutation-verified
  (replacing the `type(v) is str` branch with `False` makes both fail). M3 (docs): the inline
  comment above the `_window_dedup_key` call sites and this bullet's own fallback-rule text were
  rewritten for the final M1 rule (they still described K1's retired type-qualified-string rule); an
  explicit INPUT CONTRACT was added below to state what a well-formed `valid_from`/`valid_to` value
  is and that an out-of-contract value is still handled, never raised on by `_window_dedup_key`
  itself, but its assigned key carries no meaning beyond "don't merge this with anything"; and the
  `bad_key` citation in the "Updated line citations" bullet was corrected (its own early return is
  one line after `log_counts()`, not on the same line as the count-only branch above it).

  **Post-M1 final review (2026-09-27, docs-only).** codex found nothing further, the real-data
  re-run was identical again (eight versions agree), and a Claude reviewer verified the M1 rule,
  order/index independence, and every M1/M2 mutation -- but flagged that the "never crashes on
  anything" wording above overreached: `Decimal("sNaN")` in `valid_from`/`valid_to` is still
  classified safely by `_window_dedup_key` itself (that guarantee holds, and is unchanged), but
  `derive_quarterly_from_monthly_same_issue` as a whole can still raise on it downstream, inside
  pandas' own `pd.concat` of the `id`-present/`id`-absent partitions (`src/aggregation.py:1143`),
  when the frame has an `id` column. This is outside the documented INPUT CONTRACT, so it was never
  a regression, just imprecise wording. No code or test changed; the derive docstring
  (`src/aggregation.py` ~:834-850), the call-site comment (~:1077-1089), and the INPUT CONTRACT /
  fallback-rule bullets above were reworded to scope the "never raises" guarantee to
  `_window_dedup_key` itself, and to state plainly that `derive`'s own crash-free guarantee holds
  only for the documented contract (ISO date/datetime `valid_from`/`valid_to` or null; a hashable
  scalar `id`).

  **P1-P3 follow-up (2026-09-27, confirm-fixes pass on 8aff8cc6).** A fresh, no-history full-branch
  review (codex: would merge as is; a fresh Claude reviewer: three items) found that the real-data
  run was identical again and the only logic changes across the whole N1-N7 commit were the
  intended ones -- but P1 (both reviewers) caught that the N6 trim had, while shortening the
  docstring, PUT BACK the exact overclaim the Post-M1 fix above had just removed: "An out-of-contract
  value never crashes this function." Verified false again, now for FOUR inputs, not just the
  window value: `Decimal("sNaN")` in `valid_from`/`valid_to` (`pd.concat` in the `id`-present
  branch), in `horizon_value` or `q`/`q50` (inside `pd.to_numeric` -- confirmed directly:
  `decimal.InvalidOperation` for `horizon_value`, `ValueError: cannot convert signaling NaN to
  float` for `q50`), and an unhashable `id` (the same `pd.concat`). Restored the narrowed wording
  with the SAME rule Post-M1 established: `_window_dedup_key` itself never raises for any window
  value; `derive` as a whole is crash-free only within the documented contract (this docstring's own
  INPUT CONTRACT text, `src/aggregation.py:803-822`, and the call-site comment's closing pointer at
  `:1089-1094`). P2 (test): the N4 float64 cast was only
  ever pinned for `q50` -- added a dedicated `q`-column test (Float64 `pd.NA` in `q`, plain NaN in
  `q50` for the same month, so there is no finite fallback), mutation-verified (removing only the
  `q` cast reproduces the exact N4 silent-average bug, from the `q` side this time). P3 (lockstep):
  applied the identical float64 cast to `_reference_derive`'s own `q`/`q50` handling -- without it,
  the reference does not silently agree with a stale production bug the way K1/L1's reference gaps
  once did; it CRASHES instead (`_reference_derive`'s point-value check is a plain Python
  `all(np.isfinite(v) for v in ...)`, and `bool(pd.NA)` raises `TypeError`, confirmed directly), so
  the cast is required for the differential test to run at all on such frames, not merely for
  parity. Extended the differential generator to store `q`/`q50` as pandas nullable `Float64` on
  ~25-30% of generated frames (confirmed by direct measurement: 53/300 for `q`, 80/300 for `q50`
  across the fixed-seed run); all 300 frames still pass. Also fixed `clamp_issue_days`'s own
  docstring, which claimed a fixed float64 return dtype -- confirmed by direct measurement it is
  `int32` when the input has no `NaT` and `float64` only when it does; documented rather than
  forced to one dtype via an extra cast, since the one caller's comparison works correctly either
  way and a cast would needlessly change that comparison's dtype in the common case.
- **Dev-DB validation (2026-09-27).** On the local dev DB, complete same-issue triplets across the
  seven derived models (2000-2026) were 30.96k (kghm) / 7.13k (tjhm), and 14.1k / 3.4k over
  2015-2026; the feasibility section above (~27.8k / ~5.2k) did not state its year window, so these
  are not directly comparable, just a more precise re-measurement. Separately: kghm currently has no
  native Q1 LR rows on the 25 Dec issue date, which is expected while LTF-014 P0 is deferred (decision
  2's forecast_months restriction) -- this is exactly the gap the LR fallback (decision 2) exists to
  cover.
- **Rollout note (N7): the 3-of-3 observation rule is the ONLY part of P1a that takes effect at
  merge**, not gated behind P2's writer changes -- everything else in P1a (the derivation helper,
  the exact-duplicate/uniqueness rules, the vectorization) is a pure function nothing calls yet.
  `aggregate_monthly_obs_to_quarterly`'s `QUARTER_OBS_MIN_MONTHS = 3` threshold IS live on merge,
  reached via `recalculate_skill_metrics.py:385` -> `data_reader.read_quarterly_observations`
  (`:2976`) -> `aggregate_monthly_obs_to_quarterly`, and, per owner decision R4-merge-is-deploy (merge = deploy), it is
  presumed already live on both servers since 2026-09-27/28 via auto-pull. Separately, "3 of 3 months" is
  NOT "a fully observed quarter": a month itself counts as observed at >= 50% of its days
  (`data_reader.py` ~:1302, `monthly[monthly["non_missing_days"] >= monthly["days_in_month"] * 0.5]`), so a
  quarter that passes the 3-of-3 threshold can still be built from three half-empty months.
  - **N7's original ban is [LIFTED 2026-09-28 by owner decision R4-recalc-runs].** N7 originally read: "do NOT run a
    quarter skill recalc on any server between deploying P1a and P2's writer-paused export window" --
    the concern was that the automatic bimonthly recalc
    (`bin/bimonthly_long_term_skill_metrics_recalculation.sh`, which runs a QUARTERLY recalc
    unconditionally) would apply the new 3-of-3 rule to a comparison baseline P2's export never captured.
    The owner accepts this: the automatic recalc is **allowed to run** before P2, with no per-org pause
    decision needed. Consequence for P2 (see "P2 -- rollout" below): P2's own export, taken at the start
    of its writer-paused window, now captures **3-of-3-era** quarter skill -- whatever the automatic cron
    has already produced under P1a's rule by then -- not the original pre-P1a skill; P2's runbook states
    this rather than treating the export as the pre-P1a baseline.

### P1b — readers, native-row selection, maintenance, writer

Depends on P1a. It can run in parallel with P1c; the two touch disjoint source files.

**Files:**
- `src/data_reader.py`: the two quarter readers (item 2) — wiring in the already-on-trunk
  `derive_quarterly_from_monthly_same_issue` (P1a, `src/aggregation.py:786`) in place of
  `aggregate_monthly_fc_to_quarterly`, the model filter (switch the two quarter readers' post-combine
  `_filter_supported_aggregated_forecast_models` call to `QUARTER_SUPPORTED_MODELS`, season unchanged),
  the target-year trim scope (derived rows only), the combined-reader filter, the shared native-row
  helper, `_quarterly_fc_output_cols`. Reference the existing `QUARTERLY_DERIVED_MODELS` /
  `QUARTER_NATIVE_RAW_MODELS` / `QUARTER_SUPPORTED_MODELS` constants (`src/model_names.py:21-30`) and
  `clamp_issue_days` (`src/aggregation.py:646`) — do not re-create them. One permitted signature change:
  `_quarter_native_q1_issue_date(start_year)` (`:3045`) gains an optional keyword-only `schedule=None`
  parameter so the shared-schedule resolution above can pass its already-resolved `OperationalSchedule`
  in; omitted, behaviour and the existing call without it are unchanged.
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
- **Existing Source 1 (now unified) forecast_date bound, latest reader, both flags.**
  `forecast_date = 2026-06-25`; **full hv 1/2/3 monthly triplets (q50 set), one issued 2026-09-25 and
  another issued 2026-10-25** (both dates after `forecast_date`) → no Q4 aggregate is produced from
  either, under both flags. A single monthly row per issue date is not a sufficient fixture here: with
  only one of the three required leads present, `derive_quarterly_from_monthly_same_issue` already
  produces no aggregate for the incomplete-triplet reason alone, so the test would pass even with the
  `forecast_date` bound entirely missing — it would not detect the bug it names. Giving each issue date a
  full, otherwise-derivable triplet, and asserting it is absent, means the bound is the only thing that
  can explain the absence. Fails on the pre-P1b base (neither flag bounds this path today).
- **Existing Source 1 (now unified) forecast_date bound, inputs to the derivation.** No row dated after
  `forecast_date` reaches `derive_quarterly_from_monthly_same_issue` (assert on a spy, or on the rows
  actually passed to it), under both flags.
- **Model filter keeps derived-model rows.** A derived row for one of the seven models (e.g. GBT) survives
  the quarter readers' post-combine filter; a season reader's output for the same model is unaffected
  (still filtered to `AGGREGATED_SUPPORTED_MODELS`).
- **Target-year trim scope.** Flag OFF: a direct row with target year outside `[start_year, end_year]` but
  issue year inside it is still returned **only if it is a NATIVE row** (owner decision R4-native-lr-precedence, above) — the
  Problem-7 invariant widens which issue years are read, not which rows survive the native-row rule; a
  non-native row in that same widened range is dropped. Assert this only under flag OFF. Flag ON: assert
  instead that the existing direct-row target-year trim is
  preserved, unaffected by this item — `read_quarterly_forecasts` trims direct rows to
  `[start_year, end_year]` (`_trim_to_target_year_range`, `src/data_reader.py:3209`);
  `read_latest_quarterly_forecasts` trims them to `[start_year, end_year + 1]` (`:3582`, from #527). A
  derived row with target year outside `[start_year, end_year]` (latest reader: `[start_year, end_year +
  1]`) is trimmed, under both flags.
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
- **Mode unsupported (uzb-like: `quarter` not in `ieasyhydroforecast_ml_long_term_supported_modes`), both
  flags:** `operational_schedule_for_mode("quarter")` raises `UnsupportedLongTermModeError` (a
  `LongTermHorizonResolverError` subclass) — the derivation logs **one** WARNING and is skipped; flag ON
  returns empty as today, with `tests/test_lead_aware_empty_schedules.py:207, 239` unchanged; flag OFF
  behaves exactly as trunk (the direct path's `quarter_horizon_value()` raise is unchanged). This is
  distinct from the case below.
- **Missing quarter config FILE (the mode IS supported, but its config file is absent — e.g. deleted or a
  bad deployment), both flags — split per item 8 ("the missing-quarter-config split is intended",
  overview owner decisions 2026-09-28):** `_load_long_term_config` raises `FileNotFoundError`
  (`apps/iEasyHydroForecast/long_term_horizon_resolver.py:184`) for the derivation/read path, and this
  **propagates** (FAILS the run) under **both** flags — it is not caught by the derivation's own
  `except (UnsupportedLongTermModeError, LongTermHorizonResolverError)` (a narrower tuple that
  deliberately excludes `FileNotFoundError`, item 2's "Schedule resolution" bullet). The existing
  warn-and-disable behaviour for a `FileNotFoundError` stays **only** on `_quarter_native_q1_issue_date`'s
  own Problem-7 exception (`src/data_reader.py:3072-3083`, flag OFF only) — that narrow admit rule
  degrades gracefully because disabling it only drops one exception case, not the whole read. **This
  propagation is existing trunk behaviour**, not new: flag OFF already raises via `quarter_horizon_value()`
  (`:3198`), before reaching any native-row logic; flag ON already propagates via
  `_operational_schedules_for_horizon_type("quarter")` (`:3162`). P1b's job here is to PRESERVE this in
  the new derivation path, not introduce it. Do **not** write a reader-level "warn on a missing
  `quarter.json`" test for either reader — that behaviour does not exist and would be wrong. The
  propagate test above is therefore a regression guard: it passes on the pre-P1b base too — say so in
  the PR.
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
- `tests/test_quarterly_data_reader.py:134, 245, 407, 654, 718, 985, 1015`, where they assert the old
  mixed-issue or 2-of-3 monthly aggregation, or the flag-OFF output columns. (`:443` moved to its own
  bullet below — it needs more than a one-line note.) **Per-test verification (all seven read against the
  actual fixture code, not restated from memory) — new expected result for each. Five of the seven (`:134`,
  `:407`, `:654`, `:985`, `:1015`) are rewritten to a full, non-degraded config with real derivation
  fixtures — none of them are flipped to "empty", and none of the five classes are deleted; only `:245` and
  `:718` keep the degraded-mode framing (they legitimately test that case, kept separate from derivation
  coverage below):**
  - **Point-value contract for every monthly-row fixture this plan adds (shared template below and each
    "same-issue monthly LR triplet" added elsewhere in this section, e.g. the native-LR-precedence fixtures
    and `_monthly_q2_2025`).** `derive_quarterly_from_monthly_same_issue` reads its per-row value as
    `_point_value = q if finite else q50` (`src/aggregation.py:1070-1080`); a row with neither carries a
    finite value fails the all-three-finite check and the whole triplet is excluded as `non_finite_value`
    (`src/aggregation.py:1229-1233`). Every mocked monthly row a test wants the derivation to actually
    average must therefore set `q50` (existing monthly fixtures in this file, e.g. `_monthly_rows` at `:996`
    and `_monthly_q2_2025` at `:1143`, already follow this — only `forecasted_discharge` and `q50` are set,
    never `q`, since real raw LR/GBT monthly API rows do not reliably carry `q`). `forecasted_discharge`
    alone is **not** read by the helper; stating a monthly row's "value" only in `forecasted_discharge`
    silently produces zero derived rows, not the intended mean.
  - **Shared derivation fixture template, used by `:134`, `:407`, `:654`, `:985` and `:1015` below (do NOT
    flip these to "empty" — they must exercise the real derivation, not the degraded-mode skip).** Override
    the class-level autouse config per test to a **full, non-degraded** `quarter.json` (`lead=1,
    issue_day=25`, kghm shape). Mock `_read_long_forecasts_api` so `horizon_type="quarter"` (the
    direct/native source) returns empty (isolating the derivation) and `horizon_type="month"` returns, for
    a target quarter issued on date `d` (day 25): an `LR_Base` **day-25** triplet at hv 1/2/3 (target
    months `L`/`L+1`/`L+2`), `q50` (and `forecasted_discharge`, matching the existing fixture convention)
    100.0 / 105.0 / 110.0 → derived (decision-G fallback) mean **105.0**; a `GBT` **day-25** triplet, same
    issue date and target months, `q50` (and `forecasted_discharge`) 200.0 / 205.0 / 210.0 → derived
    (unconditional, `QUARTERLY_DERIVED_MODELS`) mean **205.0**; and an `LR_Base` **day-10** "backfill"
    triplet, otherwise identical but issued on the 10th of the same issue month, `q50` (and
    `forecasted_discharge`) 900.0 / 910.0 / 920.0 — `issue_day=25` means this triplet fails
    `derive_quarterly_from_monthly_same_issue`'s own `wrong_issue_day` check (`src/aggregation.py`, the
    function's documented counted exclusions) and **contributes nothing**: no row at 910.0 (its mean) or
    any blend with the day-25 values. No native/direct row exists for either model, so `LR_Base` is derived
    only via the decision-G fallback and `GBT` only via the unconditional seven-model derivation — both
    from the day-25 triplet exclusively. The derived rows' own OUTPUT carries these values in
    `forecasted_discharge` (the helper's output column, `src/aggregation.py` item 1 "Output row" above) —
    the assertions below correctly read `result["forecasted_discharge"]`; it is only the MOCKED MONTHLY
    INPUT rows that must additionally carry `q50` for the helper to see them at all.
  - **`:134` `test_aggregated_from_monthly`** (`read_quarterly_forecasts`, flag OFF). Instantiate the
    template for `d = 2024-03-25`, target quarter **Q2 2024** (Apr/May/Jun 2024, hv 1/2/3) — issue year and
    target year both 2024, deliberately avoiding a year-boundary Q1 case so the derived-rows' own
    target-year trim (`[start_year, end_year]` for this reader, item 2's "Target-year trim scope") cannot
    accidentally exclude the row. Call `read_quarterly_forecasts([CODE], 2024, 2024)`. **New assertions:**
    the Q2-2024 slice has exactly two rows — `LR_Base` at `forecasted_discharge == 105.0` and `GBT` at
    `forecasted_discharge == 205.0`; assert none of `900.0`/`910.0`/`920.0` appear anywhere in
    `result["forecasted_discharge"]`, i.e. the day-10 triplet contributes nothing, not even blended into
    the mean. **Fix the test's existing `read_api.call_args.kwargs` assertions too**
    (`tests/test_quarterly_data_reader.py:161-163`: `kwargs = read_api.call_args.kwargs`;
    `kwargs["horizon_type"] == "quarter"`; `kwargs["horizon_value"] == 1`) — `.call_args` is the LAST call
    only, which is safe today because this test patches `read_monthly_forecasts` directly so
    `_read_long_forecasts_api` is invoked once, for the quarter direct source. Under the shared fixture
    template above, `_read_long_forecasts_api` is mocked for **both** `horizon_type="quarter"` (direct) and
    `horizon_type="month"` (derivation input) — the month mock the template requires — so P1b calls it at
    least twice and the LAST call may well be the month one, making `.call_args.kwargs["horizon_type"] ==
    "quarter"` fail. Select the quarter call from `call_args_list` instead, the same pattern
    `test_returns_most_recent_quarter` already uses (`tests/test_quarterly_data_reader.py:705-708`:
    `quarter_call = [call for call in read_api.call_args_list if call.kwargs.get("horizon_type") ==
    "quarter"][0]`; `quarter_call.kwargs["horizon_value"] == 1`).
  - **`:176` `test_quarter_read_uses_resolved_lead_zero`** (`read_quarterly_forecasts`, flag OFF,
    lead-only/degraded `quarter.json` — `operational_month_lead_time` only, no `operational_issue_day`).
    **Unaffected by the `:134` fix above; state this in the inventory.** In degraded mode P1b's derivation
    is skipped entirely (it needs `operational_issue_day`, which this config lacks), so P1b performs **no
    monthly read at all** here — `_read_long_forecasts_api` is still called exactly once, for the direct
    quarter read, and `read_api.call_args.kwargs` (`:192-194`) stays unambiguous. No fixture or assertion
    change needed.
  - **The general rule (from the `:134` note above): under a FULL config (derivation active), any test
    asserting on `read_api.call_args` instead of `call_args_list` must be checked**, because
    `_read_long_forecasts_api` is then called at least twice (once for month derivation, once for the
    direct quarter read) and a bare `.call_args` only captures the LAST one. **Swept
    `tests/test_quarterly_data_reader.py` and `tests/test_quarter_calendar_window.py` for `.call_args` (not
    `.call_args_list`) on a call that could go through `read_quarterly_forecasts`'s derivation path** —
    besides `:134` (fixed above) and `:176` (unaffected, previous bullet), every other `.call_args` use in
    those two files is out of scope: a season reader
    (`test_preserves_four_issue_dates_and_leads` `test_quarterly_data_reader.py:606`,
    `test_returns_latest_season` `:1043`, `test_returns_empty_when_api_unavailable` (season combined)
    `:1267`); `read_quarterly_combined_forecasts`/`read_seasonal_combined_forecasts`, which mock
    `_read_long_combined_forecasts_api` — a separate, filter-only function per item 4 above, unaffected by
    this item (`TestReadQuarterlyCombinedForecasts::test_returns_data_when_api_available` `:1224`,
    `TestReadQuarterlyCombinedForecastsLeadAware`'s two tests `:1245`, `:1255`); the low-level API-wrapper
    tests that call `_read_long_forecasts_api`/`_read_long_combined_forecasts_api` directly, not through a
    reader (`:1279`, `:1306`, `:1324`, `:1349`); and `write_long_forecasts.call_args` assertions in
    `test_quarter_calendar_window.py` (`:495`, `:527`, `:555`, `:699`, `:833`), which are writer-side, not
    reader-side. `tests/test_lead_aware_operational_issuance_wiring.py:143` reads `mock_api.call_args.args[1]`
    but for `read_monthly_forecasts`, not `read_quarterly_forecasts` — out of scope; its own quarter-reader
    test in the same file (`TestReadQuarterlyForecastsQ1Boundary::test_expands_window_and_selects_prior_year_issuance`,
    `:213`) already uses the safe `call_args_list` pattern (`:244-248`).
  - **`:245` `test_filters_deprecated_models_after_combining_sources`.** Same degraded config (no
    override). `direct_api` (LR_SM, SM_GBT_Norm, EM; no `date`/issue-date column) is returned for
    `horizon_type="quarter"`; the `monthly` mock (LR_Base, GBT) is only reachable through the now-dead
    `read_monthly_forecasts` path. **Consequence: `LR_Base` no longer appears at all** (it depended
    entirely on the dead old-aggregation path; degraded mode also blocks the new derivation from
    supplying it as a fallback). `LR_SM` still survives (degraded mode disables the native-row *filter*,
    not the direct read itself). `SM_GBT_Norm` is still absent, now via item 2's unconditional "drop
    direct rows of the seven models before the sources are combined" step rather than the old
    `AGGREGATED_SUPPORTED_MODELS` filter (same outcome, different mechanism — this part needs no fixture
    change). `GBT` is still absent (it was never in the "quarter" direct source, and the monthly mock that
    used to carry it is dead code either way). **New expected result: `set(result["model_short"]) ==
    {"LR_SM", "EM"}`** (drop `"LR_Base"` from the expected set); the second assertion (`GBT`/`SM_GBT_Norm`
    absent) is unchanged.
  - **`:407` `test_monthly_aggregated_two_leads_survive`** (`read_quarterly_forecasts`, flag ON). Instantiate
    the same template as `:134` (`d = 2024-03-25`, Q2 2024, read window `(2024, 2024)`), with
    `SAPPHIRE_SKILL_LEAD_AWARE=true` and the test's own full config. Per decision A's own contract
    (`src/aggregation.py` ~:798-801, "independent of `SAPPHIRE_SKILL_LEAD_AWARE`: the output is identical
    under both flag states"), the derived rows are **flag-independent** — the flag only changes how OTHER
    code groups DIRECT rows, and this fixture supplies no direct rows for `LR_Base`/`GBT` for the flag-ON
    "Stored leads"/`select_operational_issuances` machinery to touch. **New assertions: identical to
    `:134`** — `LR_Base == 105.0`, `GBT == 205.0`, day-10 triplet absent. The test's renamed purpose:
    demonstrate that the derivation contract is unaffected by the lead-aware flag (this replaces its old
    "two leads from one aggregated source" premise, which the same-issue-triplet derivation does not have —
    only one native lead is admitted per key, decision R4-native-lr-precedence).
  - **`:654` `test_returns_most_recent_quarter`** (`read_latest_quarterly_forecasts`, flag OFF). Apply the
    shared fixture's day-25/day-10 triplets for Q1 2025 (issued `2024-12-25`), and additionally an OLDER
    same-issue-day-25 triplet for Q4 2024 (issued `2024-09-25`): `LR_Base` `q50` 90.0/95.0/100.0 → mean
    **95.0**, `GBT` `q50` 190.0/195.0/200.0 → mean **195.0** (both also carry `forecasted_discharge` at the
    same values, per the point-value contract above — `q50` is what the helper actually reads). Set
    `forecast_date = 2025-01-05` (after both issue dates).
    **New assertions:** `read_latest_quarterly_forecasts` returns **only** the most recent quarter's rows —
    `LR_Base == 105.0` and `GBT == 205.0` for Q1 2025 — and no Q4-2024 row (`95.0`/`195.0`) is present; the
    day-10 backfill triplet still contributes nothing (no `910.0`/`920.0`-derived value anywhere in the
    result). This keeps the test's original "most recent quarter" purpose, now against real derived data
    instead of a degraded-mode empty frame.
  - **`:718` `test_latest_filters_deprecated_models_after_combining_sources`.** No config override ->
    degraded; no `SAPPHIRE_SKILL_LEAD_AWARE` set -> flag OFF (this repo's documented OFF default).
    `raw_quarter` (LR_SM, LR_SM_DT; no `date` column) is returned for `horizon_type="quarter"`;
    `raw_monthly` (LR_Base x3, GBT x3; no `date` column) for everything else. Under P1b flag OFF, the old
    raw-path LR aggregation this test relies on for `LR_Base` is superseded by the same unified derivation
    mechanism as flag ON (item 2, "Existing Source 1... SUPERSEDED — applies under both flags"), which
    needs a `date` column to identify same-issue triplets; this fixture has none, and degraded mode skips
    derivation outright regardless. **Consequence: `LR_Base` no longer appears.** `LR_SM` still survives
    (direct, native filter disabled in degraded mode). `LR_SM_DT` is still absent, now via the
    unconditional seven-model direct-row drop (same outcome, different mechanism). `GBT` is still absent
    (never in the direct "quarter" source). **New expected result: `set(result["model_short"]) ==
    {"LR_SM"}`** (drop `"LR_Base"`); the second assertion is unchanged.
  - **`:985` `test_flag_on_source1_aggregates_only_operational_monthly`** (`read_latest_quarterly_forecasts`,
    flag ON). The old fixture (`_rows()`, `:974-983`: two months at two different issue dates, no
    same-issue triplet) is retired — it cannot express the same-issue-triplet contract at all, and rewriting
    its now-obsolete "operationally-selected vs. raw" premise is not useful. **Replace `_rows()` with the
    same fixture as `:654`** (both quarters: Q1 2025 day-25/day-10, plus the older Q4-2024 day-25 triplet),
    `SAPPHIRE_SKILL_LEAD_AWARE=true`, `forecast_date = 2025-01-05`. **New assertions: identical to `:654`**
    — `LR_Base == 105.0`, `GBT == 205.0` for Q1 2025 only; no Q4-2024 row; day-10 triplet absent. This
    demonstrates the same flag-independence as `:407`: the derivation is identical whether or not
    `SAPPHIRE_SKILL_LEAD_AWARE` is set, which **replaces** the class's retired "FINDING 1: Source 1 must
    aggregate OPERATIONALLY-SELECTED monthly rows... rather than RAW monthly rows" premise (`:930-939`) with
    the actual current contract, rather than deleting the class's coverage.
  - **`:1015` `test_flag_off_source1_keeps_raw_path_backfill_retained`** (`read_latest_quarterly_forecasts`,
    flag OFF). Rename to reflect the CONTRACT CHANGE this test now demonstrates (e.g.
    `test_flag_off_backfill_triplet_excluded_by_wrong_issue_day`). Same shared fixture as `:654`/`:985`, flag
    OFF, `forecast_date = 2025-01-05`. Pre-P1b, the old flag-OFF "raw path" retained the day-10 backfill
    triplet's mean (`forecasted_discharge == 300.0` in the old fixture) — that is exactly what this test used
    to lock. **New assertions:** the day-10 backfill mean is **no longer present** anywhere in the result
    (assert no row equals the day-10 triplet's mean); only the day-25 `LR_Base == 105.0` / `GBT == 205.0`
    rows for Q1 2025 survive (same result as `:654`/`:985`). Update the docstring to say the backfill is now
    **excluded** (`wrong_issue_day`), not retained — this is the explicit before/after contract change the
    PR should call out.
  - **Degraded-mode derivation, stated once, and now scoped to `:245`/`:718` only** (the plan's own
    "Degraded native rule, flag OFF" rule, item 2 above; `:134`, `:407`, `:654`, `:985` and `:1015` above
    are deliberately rewritten to use a FULL, non-degraded config instead, so degraded-mode coverage does
    not disappear — it stays live on `:245` and `:718`, which are unaffected by this rewrite): a lead-only
    `quarter.json` (no `operational_issue_day`) makes `operational_schedule_for_mode("quarter")` raise, and
    P1b's response is to log one WARNING **and skip the derivation too** ("log one WARNING (covering the
    skipped derivation too) and keep today's direct-LR selection, with no native filter") — so in degraded
    mode P1b produces **no** seven-model rows and **no** LR fallback rows, only whatever the direct/native
    read already returns unfiltered. This is why `:245` and `:718` (not `:134`/`:654` any more, per the
    rewrite above) lose their previously-aggregated `LR_Base` row with no replacement.
- Keep the direct-row exclusion assertions for the seven models (e.g. `:293-337`, `:765-810`).
- **`tests/test_quarterly_data_reader.py:196, 293, 765` — corrected: flag OFF, LEAD-ONLY (degraded)
  config, mostly NOT a fix.** All three rely on the class-level autouse fixture (`:30-49`), which writes
  `quarter.json` with `operational_month_lead_time` only, no `operational_issue_day` — neither test
  overrides it. `operational_schedule_for_mode("quarter")` therefore raises
  `LongTermHorizonResolverError`, and the plan's own "Degraded native rule, flag OFF" rule (above) applies:
  log one WARNING and keep today's direct-LR selection, with **no native filter**. So a missing `date`
  column does not matter here, unlike a resolvable-schedule case.
  - `:293` (`test_filter_accepts_db_form_lr_and_ensemble_names`) and `:765`
    (`test_latest_accepts_db_form_lr_and_ensemble_names`) are **unchanged**. Their direct rows of the seven
    models (e.g. `GBT`, `SM_GBT_NORM`, `MC_ALD`) are still absent from the output, but now because P1b
    unconditionally drops DIRECT rows of the seven models before combining sources (item 2 above), not
    because of the old `AGGREGATED_SUPPORTED_MODELS` filter — the assertion is the same, the reason
    changed.
  - `:196` (`test_direct_preferred_over_aggregated`) still numerically passes (its direct LR row, q50=99,
    still wins) but becomes **vacuous**: under P1b, `read_quarterly_forecasts`' old Source 1 call site
    (`read_monthly_forecasts` -> `aggregate_monthly_fc_to_quarterly`) is removed, so this test's
    "aggregated" monthly mock is dead code, and the degraded config also skips the new derivation (same
    shared-resolution failure) — there is nothing left to "win over". **Retarget it** (keep the class, do
    not remove it): assert "direct LR is kept unconditionally in degraded mode, no aggregated/derived row is
    produced" — rename the test and rewrite its docstring to say so; drop the now-dead "aggregated" monthly
    mock from its fixture.

**Owner decision R4-native-lr-precedence — native LR precedence, full enumeration** (`tests/test_quarter_calendar_window.py`;
line numbers verified against this branch's HEAD):
- `TestRegressionDirectPrecedenceSurvivesLowerBoundWidening::test_direct_next_year_q1_wins_over_monthly_derived`
  (class `:995`, test `:1024`). **Why the old fixture proved nothing:** its direct row (issued
  `2025-12-25`, target Q1 2026) is already kghm's native Q1 issue date, but with read window
  `read_quarterly_forecasts([CODE], 2025, 2025)` its issue year (2025) simply equals `start_year` — no
  widening exception is even reached. Its `_monthly_rows` fixture (two months, `horizon_value` fixed at 1
  for both, issued `2025-11-25`) additionally produces **no** derived row — **not** because of the
  `missing_lead` exclusion (an earlier draft of this bullet said so incorrectly): with kghm's lead 1, issue
  date `2025-11-25` (November) makes the quarter-start scope filter's own target month November + 1 =
  December, which is not a quarter-start month (1/4/7/10), so **both** rows are dropped there, silently and
  uncounted, before the hv/pivot logic that `missing_lead` belongs to ever runs
  (`aggregation.py` ~:1033-1041, "Quarter-start scope filter... Out of scope, routine -- never counted").
  Either way there was never a competitor to "win" against — the old test only locked native-row selection
  in isolation, and did not exercise "lower bound widening" at all. **Fix:
  re-date to a genuine lower-bound-widening case, and add a real competitor.** Change the direct row to
  issue date `2024-12-25` targeting **Q1 2025** (`valid_from = 2025-01-01`, `valid_to = 2025-03-31`),
  keeping the read window `read_quarterly_forecasts([CODE], 2025, 2025)` (`start_year = end_year = 2025`)
  unchanged. Now issue year (2024) `< start_year` (2025), so the row survives only via PP-064's own
  December-Q1-of-`start_year` exception (`src/data_reader.py` ~:3251-3260,
  `_quarter_native_q1_issue_date(2025)` resolves to `2024-12-25`, the genuine widening case this class is
  named for) — and it is also native (day 25, lead 1 → Jan 2025). Add a same-issue monthly LR triplet:
  `LR_Base`, hv 1/2/3, target months Jan/Feb/Mar 2025, all issued `2024-12-25` (the same date), with
  DIFFERENT values in `q50` (e.g. 200.0/210.0/220.0, mean 210.0 — per the point-value contract above, `q50`
  is what the helper reads, not `forecasted_discharge`) so the direct row's value (kept at 100.0) is
  distinguishable from the derived mean. This satisfies `derive_quarterly_from_monthly_same_issue`'s 3-lead
  requirement, and its target year (2025) is within the reader's own `[start_year, end_year] = [2025,
  2025]` derived-rows trim (item 2's "Target-year trim scope"), so decision-G's LR fallback genuinely
  produces a competing derived Q1-2025 row — unlike a same-fixture attempt targeting Q1 2026, which the
  trim would exclude regardless of precedence. **New assertions:** `q1_2025 =
  result[(result["year"]==2025)&(result["quarter_in_year"]==1)]`; the direct row's value wins
  (`forecasted_discharge == 100.0`, not `210.0`), and no separate fallback-derived row appears at the same
  key — `len(q1_2025[q1_2025["model_short"] == "LR_Base"]) == 1`. This now locks the actual invariant: **a
  native direct row suppresses the decision-G fallback derivation for its key**, in a case that genuinely
  exercises the lower-bound-widening exception, not merely "no competitor happened to exist."
  - **The LR_SM direct row (`q=120.0`, the class's `_direct_rows()` returns both LR_Base and LR_SM).** Only
    the LR_Base row is re-dated to `2024-12-25`/Q1-2025 above. **State explicitly: the LR_SM row is left
    at its original `2025-12-25`, targeting Q1 2026 — outside this reader's `[start_year, end_year] =
    [2025, 2025]` target-year trim, so it is dropped by that trim regardless of native-row status, and
    does not appear in the `q1_2025` slice this test asserts on at all.** The rewritten test therefore
    makes no claim about LR_SM (dropping the original `got.get("LR_SM") == 120.0` assertion is
    intentional, not an oversight); LR_SM coverage of this same invariant would need its own re-dated row
    and is out of scope for this test.
- `TestRegressionBackfillPrecedenceSurvivesLowerBoundTrim` (class `:1046`). **Split into two tests — one
  fixed with a new read window, one kept as the original regression, per the corrected analysis below.**
  **Test A and Test B currently share a single `_direct_rows()` fixture method on the class (both LR_Base
  `q=100.0` and LR_SM `q=120.0`, dated `2025-01-10`) — Test A's rewrite below needs a re-dated row that
  Test B must NOT get, so split it into two fixture methods: keep `_direct_rows()` returning exactly what
  it returns today (both rows, `2025-01-10`) for Test B, and add a new `_direct_rows_native()` for Test A
  returning both rows re-dated to `2024-09-25`.**
  - **Test A — native precedence over the fallback (rewritten; replaces
    `test_direct_prior_year_backfill_wins_over_monthly_derived`, `:1076`).** The earlier draft tried
    re-dating the direct row to `2024-09-25` while keeping the original `read_quarterly_forecasts([CODE],
    2025, 2025)` call — that does not work: the flag-OFF issue-year mask (`src/data_reader.py`
    ~:3247-3261) drops any row with `issue_year < start_year` unless it is the December-Q1-of-`start_year`
    exception, and `2024-09-25` (issue year 2024 `< 2025`, targeting Q4) is neither native-Q1-shaped nor
    admitted by that exception, so it is dropped by the mask **before native-row selection ever runs** —
    changing only the date does not produce a native-precedence test. **Fix: change the read window
    instead.** Call `read_quarterly_forecasts([CODE], 2024, 2024)` (`start_year = end_year = 2024`), using
    the new `_direct_rows_native()` fixture above. Now `2024-09-25` has `issue_year == start_year`, so the
    mask does not drop it, and it reaches native-row selection as a genuine kghm-shape native Q4-2024
    issue date (day 25, lead 1 → Oct/Nov/Dec 2024). Keep both rows' values unchanged (`LR_Base` `100.0`,
    `LR_SM` `120.0`) — **the LR_SM row is re-dated to `2024-09-25` too, mirroring `LR_Base`**, so it is
    also native and also survives, at its original value `120.0`; no monthly LR_SM triplet is added, so it
    has no fallback competitor to suppress (unlike `LR_Base` below) — its presence at `120.0` is a trivial
    "kept, unchanged" case, not evidence of suppressing anything. Add a same-issue monthly LR
    triplet — `LR_Base`, hv 1/2/3, target months Oct/Nov/Dec 2024, all issued `2024-09-25`, with DIFFERENT
    values in `q50` (`300.0`/`310.0`/`320.0`, mean `310.0` — per the point-value contract above) — so
    decision-G's fallback has a real, numerically distinguishable competing derived row to produce for
    `LR_Base` specifically. **New assertions:**
    `q4_2024 = result[(result["year"]==2024)&(result["quarter_in_year"]==4)]`; the native direct row's
    value wins for `LR_Base` (`forecasted_discharge == 100.0`, not `310.0`), no fallback-derived row
    appears at the same key (`len(q4_2024[q4_2024["model_short"] == "LR_Base"]) == 1`), **and** `LR_SM`'s
    native row is present unchanged (`len(q4_2024[q4_2024["model_short"] == "LR_SM"]) == 1`,
    `forecasted_discharge == 120.0`). This test now belongs conceptually
    with the
    widening test above (native beats fallback), not with the backfill-drop test below; rename it (e.g.
    `test_native_direct_row_suppresses_monthly_derived_fallback`) and move its docstring out of the
    "lower-bound trim" framing.
  - **Test B — backfill-shaped rows are dropped (kept, using the ORIGINAL `_direct_rows()` fixture
    unchanged — NOT `_direct_rows_native()`; was
    `test_direct_prior_year_backfill_returned_without_monthly_source`, `:1087`).** Keep the original
    `read_quarterly_forecasts([CODE], 2025, 2025)` call and the original direct rows dated `2025-01-10`
    (issue year 2025, survives the mask, but neither is native — native Q4-2024 is `2024-09-25`). **Both
    `LR_Base` (`100.0`) and `LR_SM` (`120.0`) are backfill-shaped here, unchanged from the original
    fixture, and both are dropped by decision R4-native-lr-precedence for the same reason (non-native,
    per row, regardless of model) — this test's own fixture is not model-specific.** **Corrected
    fixture description (an earlier draft of this bullet was wrong): the test passes NO monthly source at
    all** — `fake = _quarter_and_month_api_fake(self._direct_rows(), [])`
    (`tests/test_quarter_calendar_window.py:1089`, the empty list `:1087-1090`). There is no "2-month,
    hv-fixed-at-1" monthly fixture here and no quarter-start-scope-filter mechanism at play (that
    description belongs to a different test); with an empty monthly source,
    `aggregate_monthly_fc_to_quarterly` has nothing to derive from, so there is trivially no competing
    derived row for either model. **New expected result (unchanged from the prior "corrected" analysis,
    confirmed still
    accurate for this window): the Q4-2024 slice is empty** — decision R4-native-lr-precedence drops both
    non-native direct rows (`LR_Base` and `LR_SM` alike), and there is no fallback row to fall back to for
    either. Rename to reflect the actual
    invariant it locks (e.g. `test_backfill_shaped_direct_row_is_dropped`), and drop the "wins"/"survives"
    framing from its docstring — it now demonstrates the drop, not a win.
  - Rewrite the class-level docstring to state both invariants separately: Test A (native beats fallback)
    and Test B (non-native, backfill-shaped rows are dropped, not returned).
- `TestRegressionIssueYearMaskTooPermissive` (class `:1142`):
  - `test_stale_out_of_window_row_does_not_beat_monthly_derived` (`:1160`). **Corrected — the "assertion
    unchanged" claim in an earlier draft of this list is wrong.** Its `_monthly_q2_2025` fixture
    (`:1143-1157`) is 2 months (April, May) at hv fixed = 1 — this is `missing_lead` under
    `derive_quarterly_from_monthly_same_issue` (no lead 3), so it produces **no** derived row; with the
    direct row also dropped (non-native, issued `2024-12-25` for a Q2-2025 target whose native issue date
    is `2025-03-25`), the result would be **empty**, not the monthly-derived `200.0` currently asserted.
    **Fix:** extend `_monthly_q2_2025` to a full same-issue triplet — months 4, 5, 6 at hv 1, 2, 3, all
    issued `2025-03-25` (kghm's native Q2-2025 issue date). Verified against `derive`'s semantics: with all
    three leads present at the same issue date, decision G's LR fallback derives Q2 2025 = 200.0 (average
    of 200/200/200), so the assertion (200.0 wins) still holds — now via the fallback derivation, not the
    old `aggregate_monthly_fc_to_quarterly` aggregation. Also rewrite the class-level comment block
    (`:1130-1139`), which states the now-superseded invariant "the flag-OFF direct set = trunk's set ... +
    ONLY the December-issued Q1 of `start_year`", to state the native-row invariant instead.
  - `test_stale_row_does_not_clobber_in_window_direct_row_regardless_of_api_order` (`:1178`). **Why:** its
    "in-window" direct row is issued `2025-05-25`, also targeting Q2 2025 (native issue date
    `2025-03-25`). Under the OLD rule it won simply for having an issue year in range; under decision R4-native-lr-precedence it
    is **also non-native** and would now be dropped too. **Change:** re-date this row to the true native
    issue date `2025-03-25` so the test keeps demonstrating its stated intent (the correct direct row beats
    the stale one, regardless of API order) with the same assertion (300.0 wins). **Corrected — an earlier
    draft of this bullet said "with no derived row involved"; that is wrong.** This test calls
    `self._monthly_q2_2025()` for its monthly source, same as `:1160` above, and per that bullet's own fix
    `_monthly_q2_2025` now returns a full same-issue triplet issued `2025-03-25` — the SAME date the direct
    row is re-dated to. Decision-G's LR fallback therefore genuinely derives a competing Q2-2025 value
    (200.0, per `:1160`'s computation) here too; it is not absent, it is **suppressed** by the now-native
    direct row at the same key, exactly as `TestRegressionBackfillPrecedenceSurvivesLowerBoundTrim` Test A
    demonstrates. **New assertions:** `forecasted_discharge == 300.0` (the native direct row, not the
    100.0 stale row or the 200.0 fallback mean) **and** `len(q2_2025[q2_2025["model_short"] == "LR_Base"])
    == 1` (no separate fallback-derived row survives alongside it).
- `TestPP064aNativeQ1IssuanceRestriction` (class `:2098`; `_monthly_q1_2026_rows` fixture function
  `:2070`). **Correcting an earlier draft of this list: the fixture function's own docstring claim needs
  fixing too, not just the two affected tests'.** `_monthly_q1_2026_rows` is 2 months (Jan, Feb 2026),
  issued `2026-01-05`, at hv fixed = 1 — under P1b this produces **no** derived row, but **not** for the
  `missing_lead` reason (an earlier draft of this bullet attributed it there incorrectly). With kghm's
  lead 1, the quarter-start scope filter runs first and computes this issue's target quarter-start month
  as issue month (January, 1) + lead (1) = February (2), which is not a quarter-start month (1/4/7/10) —
  so both rows are dropped there, silently and uncounted, before the hv/pivot logic that `missing_lead`
  belongs to ever runs (same mechanism as `TestRegressionDirectPrecedenceSurvivesLowerBoundWidening`'s
  `_monthly_rows` fixture above). Either way it no longer "exercises the
  concat + drop_duplicates(keep='last') combine" the way its docstring (`:2070-2079`) claims; the combine
  it feeds has nothing on the derived side to combine against. Remove that claim from the docstring when
  P1b touches this file. Two of its five tests change:
  - `test_persisted_monthly_derived_dec1_q1_row_does_not_clobber_jan1_rewrite` (`:2101`) and
    `test_persisted_monthly_derived_dec1_q1_row_with_real_value_still_loses` (`:2124`). **Why:** both
    fixtures pit a "Jan-1 rewrite" row (issued `2026-01-01`) against a Dec-1 backdated row (issued
    `2025-12-01`) — **neither is native** (kghm's native Q1 issue date is `2025-12-25`). Under the OLD
    rule the Jan-1 rewrite won simply for being an ordinary in-year direct row; under decision R4-native-lr-precedence, with no
    native row present, **both would now be dropped**, and (per the fixture correction above) no derived
    row exists either, so the result would be **empty**. This is also exactly the case PP-065 P1b's own
    Tests list (item 2, "Native-row selection (kghm shape)") already specifies: "A native row, a rewrite
    (`date = valid_from`) and a persisted derived Dec-1 row for the same LR Q1 → the native row wins."
    **Change:** add a genuine native-dated direct row (issued `2025-12-25`) to each fixture; the new
    expected winner is that native row's value, not the Jan-1 rewrite's — decided purely among the three
    direct rows, with no derived row in play.
  - The other three tests in the class —
    `test_native_issue_day_clamped_to_short_month_is_still_admitted` (`:2142`),
    `test_unresolvable_schedule_drops_prior_year_q1_and_logs_one_warning` (`:2178`) and
    `test_invalid_issue_day_drops_prior_year_q1_and_logs_one_warning` (`:2210`) — exercise the Problem-7
    exception's own admission logic (clamping, degraded-schedule handling), which decision R4-native-lr-precedence leaves
    **unchanged** (it "stays in `read_quarterly_forecasts` only, unchanged"). **Not affected.**
- **A-6** = `TestA6GapDetectorThroughReader` (`:345-391`). **Not affected — corrected, an earlier draft of
  this list was wrong.** Its two tests,
  `test_calendar_quarter_with_only_rolling_em_is_a_gap` (`:348`) and
  `test_quarter_covered_only_by_rolling_rows_is_not_a_gap` (`:370`), call `detect_missing_quarterly_ensembles`
  **directly** (`:364`, `:389`) with the class's own hardcoded `ENSEMBLE_MODELS = {"EM", "Skilled Mean",
  "Naive Mean"}` (`:346`), bypassing `postprocessing_maintenance_long_term.py` entirely. PP-065 item 4
  narrows the `ensemble_models` argument **only at the maintenance module's own call site**
  (`postprocessing_maintenance_long_term.py:297-301`); `gap_detector.py` itself is not edited (item 4: "the
  caller only; `src/gap_detector.py` is not edited"). Since this test class supplies its own
  `ensemble_models` and never goes through the maintenance caller, item 4's narrowing has no effect on it.
  Its fixtures also carry only `LR_Base`/`EM` rows — no seven-model direct rows for item 2's "drop direct
  rows of the seven models" change to touch either. **Not affected; no fixture or assertion change.**
- **Missing from earlier drafts of this list — add these:**
  - `TestUnparseableIssueDateKeptRegardlessOfTargetYear::test_unparseable_date_backfill_row_is_kept`
    (class `:1109`, test `:1110`). **Why:** its LR_Base direct row has an unparseable issue date
    (`"not-a-date"`), targeting Q2 2024, read window `(2025, 2025)`. PP-064's own year-mask keeps a
    null/unparseable issue date unconditionally (`src/data_reader.py` ~:3242-3243) — but decision R4-native-lr-precedence's
    native-row classification for LR rows requires a schedule-computed issue date to match exactly, which
    an unparseable date can never do. **Change:** the row is now **dropped** (counted under a named
    exclusion reason, e.g. `unparseable_issue_date`), flipping the assertion from `len(q2_2024) == 1` /
    value `100.0` to `len(q2_2024) == 0`. Note for the code: the `src/data_reader.py` ~:3242-3243
    "a null/unparseable issue date is kept" comment still correctly describes PP-064's year-mask, but it no
    longer describes the final outcome for LR rows once decision R4-native-lr-precedence's native-row filter runs afterward —
    replace/annotate it so it does not read as the whole story for LR models.
  - `TestRegressionMixedTimezoneIssueDate::test_read_quarterly_forecasts_flag_off_no_exception` (class
    `:1214`, test `:1215`). **Why:** its Q1-2025 row is issued `2025-01-10` — not native (kghm's native Q1
    issue date is `2024-12-25`) — so under decision R4-native-lr-precedence it is dropped and
    `q1_2025["forecasted_discharge"].iloc[0]` raises `IndexError` on an empty frame. **Change:** re-date it
    to `2024-12-25`. That issue year (2024) is `< start_year` (2025), but it targets Q1 of `start_year`, so
    it is admitted by the Problem-7 December-Q1-of-`start_year` exception (`src/data_reader.py`
    ~:3253-3258) AND matches the native schedule date exactly, so it is also classified native. **New
    expected result:** Q1 2025 = `100.0` (same value, now via the re-dated row). The Q2 2025 row (issued
    `2025-03-25T00:00:00+06:00`) is already native for Q2 2025 (March 25) — no change needed there.
  - `tests/test_quarterly_data_reader.py:361` (`test_direct_quarter_horizon_value_survives`,
    `TestReadQuarterlyForecastsLeadAware`) and `:852`
    (`test_prior_year_operational_issuance_selected_and_backfill_dropped`,
    `TestReadLatestQuarterlyForecastsLeadAware`) — flag ON, stored `horizon_value = 99`. **Correcting an
    earlier draft's "degraded mode" framing: it does not apply to either test.** Both write their OWN full
    `quarter.json` (`operational_month_lead_time: 1, operational_issue_day: 25`) before running, overriding
    the class's lead-only autouse default — so neither is in degraded mode; `operational_schedule_for_mode`
    resolves cleanly (lead = 1) for both. The real cause is P1b's new "Stored leads (flag ON)" rule (item 2
    above): "before `select_operational_issuances`, drop and count direct rows whose stored
    `horizon_value` differs from the derived lead." Both rows are native-dated (`2023-12-25`, the native Q1
    2024 issue date) but carry a stale stored `horizon_value = 99 != 1`, so P1b drops them **before**
    `select_operational_issuances` runs, regardless of the native-row rule or the degraded-mode question.
    - `:361`: this test's whole purpose was demonstrating that a genuine operational row's stored
      `horizon_value` (however garbled) is normalized to the derived lead in the output, not stripped.
      Under P1b it is dropped instead. **Chosen fix:** flip the assertions to expect the row **dropped**
      (`len(result) == 0`) — more honest than setting the stored hv to `1`, which would stop testing the
      mismatched-hv scenario the test was written for.
    - `:852`: **corrected rationale.** The class's real purpose is that the operational issuance (day 25)
      beats a same-lead backfill (day 10). Under P1b's order (see item 2's "Order (flag ON)" bullet above),
      the day-10 row is dropped by the **native-row helper** — it is not the scheduled issue day, so it
      fails native-row selection outright, before either the stored-leads pre-filter or
      `select_operational_issuances` ever runs. `select_operational_issuances`'s own issue-day matching is
      therefore **not** what distinguishes the two rows here; it never gets a competing candidate to choose
      between. **Chosen fix (unchanged in outcome):** change BOTH fixture rows' stored `horizon_value` from
      `99` to `1` (matching the derived lead) — needed so the surviving day-25 native row itself passes the
      stored-leads pre-filter (at hv 99 it would be dropped there even after winning native-row selection);
      the day-10 row's own stored hv no longer matters, since it is already excluded upstream. Assertions
      are unchanged (operational row wins, hv = 1, value = 100.0).
    These two different fixes (drop vs. correct-the-stored-lead) are deliberate, not arbitrary: `:361`'s
    test purpose is specifically about a mismatched stored lead, while `:852`'s is about issue-day
    selection — dropping both rows in `:852` would silently remove its entire reason to exist.
  - `tests/test_quarterly_data_reader.py:443` (`test_dedup_keeps_distinct_leads_after_combining_sources`,
    same `TestReadQuarterlyForecastsLeadAware` class). Its "monthly" fixture (hv 0, two months) is dead
    code under P1b: `read_monthly_forecasts`'s call site inside `read_quarterly_forecasts` is removed, so
    this mock is never invoked, and the raw-API mock it also supplies returns empty for
    `horizon_type="month"`, so the new derivation produces nothing either. Its one direct row (native-dated
    `2023-12-25`, stored hv 99) is dropped by the Stored-leads filter (same mechanism as `:361`/`:852`
    above). **New expected result: empty (`len(result) == 0`)**, not the two-row `{0, 1}` hv set currently
    asserted. The test's premise — two rows differing only by `horizon_value` both surviving the combine —
    is **obsolete** under decision R4-native-lr-precedence: only one native lead is admitted per (code, year, quarter, model) key
    now. Repurpose it (e.g. to demonstrate the Stored-leads drop directly) or remove it.
  - **Other test files that call the quarter readers — swept and re-counted** (`grep -rln
    "read_quarterly_forecasts\|read_latest_quarterly_forecasts" apps/postprocessing_forecasts/tests`, 10
    files total; excluding `test_quarter_calendar_window.py` and `test_quarterly_data_reader.py` themselves,
    the other **8** break down as follows, not uniformly "unaffected because they mock":
    - **Six** are unaffected because they mock `data_reader.read_quarterly_forecasts`/
      `read_latest_quarterly_forecasts` itself (or the whole `data_reader` module) and never exercise the
      real reader body: `test_maintenance_long_term.py:594, 724`, `test_recalc_supported_modes_gate.py:212,
      226, 233`, `test_monthly_workflow_integration.py:610, 680`, `test_wiring_integration.py:2273`,
      `test_recalc_workflow.py:123` (all mocked returns), and `test_lead_aware_latest_readers.py` (its one
      hit is a docstring comment, not a call).
    - **One** (`test_lead_aware_empty_schedules.py:207` `TestQuarterlyEmptySchedulesFlagOn` and `:239`
      `TestLatestQuarterlyEmptySchedulesFlagOn`) DOES call the real readers, but both configure `quarter` as
      an **unsupported** mode — already covered above ("Mode unsupported (uzb-like...)"): flag ON
      unchanged, already cited correctly as unchanged by this plan.
    - **One** (`test_lead_aware_operational_issuance_wiring.py`) DOES call the real reader and **IS
      affected** — see the entry below ("add to this list; not previously enumerated"). It is not
      "unaffected because it mocks."
  - **`test_lead_aware_operational_issuance_wiring.py::TestReadQuarterlyForecastsQ1Boundary::test_expands_window_and_selects_prior_year_issuance`
    (`:199-253`) — add to this list; not previously enumerated.** Flag ON, kghm shape (lead 1, day 25). Its
    one LR_Base row is dated `2023-12-25` — the genuine native Q1-2024 issue date — but carries a stale
    stored `horizon_value = 99` (derived lead = 1). This fixture has no competing day-10 backfill row, so it
    is not exercising the native-vs-backfill distinction `:852` is about — this is purely the "Stored leads
    (flag ON)" pre-filter mechanism (item 2 above, same as `:361`/`:852`): P1b drops the row **before**
    `select_operational_issuances` runs, so `len(result) == 1` and the trailing
    `result.iloc[0]["horizon_value"] == 1` assertion both fail on an empty frame.
    **Chosen fix — correct the stored lead, not drop-and-assert-empty (matching `:852`, not `:361`):** the
    test's actual purpose, per its own assertions on `mock_api.call_args_list` and
    `quarter_calls[0].args[1] <= target_year - 1`, is to verify the flag-ON Q1 boundary expansion picks up
    the prior-year issuance — dropping the row on stored-hv mismatch would make that purpose untestable
    here (same reasoning as `:852`, not `:361`, whose purpose is specifically about the mismatched-hv
    scenario). Change the fixture's `"horizon_value": 99` to `"horizon_value": 1`; assertions are otherwise
    unchanged (`len(result) == 1`, `horizon_value == 1`).
- PP-064's `tests/test_quarter_calendar_window.py` — **completed enumeration** (every class in the file
  checked for direct `LR_Base`/`LR_SM`/`LR_BASE` rows, replacing the earlier "grep before P1b starts"
  placeholder). Besides the classes listed above, `TestA1CalendarWindowReader` (`:145`),
  `TestA2ValidToEnforcement` (`:209`), `TestA3MalformedInput` (`:252`), `TestA5DecemberQ1FlagOn` (`:322`),
  `TestA7WriterGuard` (`:408`), `TestS2ReaderWriterYear2262Agreement` (`:837`), `TestA8SeasonUnchanged`
  (`:856`), `TestA9BackDatedRun` (`:880`), `TestA10FirstYearQ1FlagOff` (`:913`),
  `TestRegressionMixedTimezoneValidTo` (`:1307`), `TestRegressionAllNullValidFromColumnDropped` (`:1372`),
  `TestP1LocalCalendarDateParsing` (`:1572`) and
  `TestRegressionOutOfRangeDatesDoNotCrash` (`:1941`) are **not affected** by decision R4-native-lr-precedence, because their
  direct LR rows already carry native-shaped issue dates (e.g. `TestA1`'s kept row, `2024-03-25` for Q2
  2024; the valid rows in `TestA5`/`TestA9`/`TestRegressionMixedTimezoneValidTo`/
  `TestRegressionOutOfRangeDatesDoNotCrash`), or they exercise a code path decision R4-native-lr-precedence's native-row rule
  does not touch (`TestA7`'s writer guard has no LR rows; `TestA3`/`TestA6`/
  `TestRegressionMixedTimezoneValidTo`'s combined-client tests use `read_quarterly_combined_forecasts`,
  which stays filter-only per item 4 above; `TestA8` is season; `TestP1LocalCalendarDateParsing` is a
  unit-level `local_calendar_date` test with no reader involved), or their non-native rows are already
  dropped today for an independent reason whose outcome coincides with decision R4-native-lr-precedence
  (`TestA10FirstYearQ1FlagOff::test_widened_window_still_trims_target_years_below_start_year`,
  `TestR5Observability`'s first and third tests — two different readers, two different reasons, corrected
  2026-09-28 (the earlier version wrongly gave both the same Problem-7 justification):
  - `test_read_quarterly_forecasts_logs_dropped_issue_year_count` (`read_quarterly_forecasts`) —
    **unaffected precisely because item 2's "Order (flag OFF)" bullet above requires the Problem-7 mask to
    run BEFORE the native-row helper**, so the mask's own drop-count log line still fires on this row
    regardless of what the native-row helper would separately do to it.
  - `test_read_latest_quarterly_forecasts_logs_dropped_future_issue_count`
    (`read_latest_quarterly_forecasts`) — **unaffected for an unrelated reason: this reader has no
    Problem-7 mask at all.** Its dropped row comes from the Problem-6 `forecast_date` bound
    (`src/data_reader.py` ~:3555-3571, `logger.info("Dropped %d quarterly direct forecast row(s) dated
    after forecast_date …")`, matched by the test's `"dated after forecast_date"` substring — not the
    Problem-7 "issued before the requested year range" message the other test above matches), which runs
    regardless of the native-row helper's position. The dropped row is also native anyway (issued
    2026-12-25 for kghm's lead-1/day-25 schedule, the correct native issuance for Q1 2027) — so even if the
    native-row helper ran on it, it would not drop it either. See the "Order (flag OFF)" bullet's own note
    on this reader's ordering.
  Re-verify this list against the actual P1b base before merging — tests may be added to the file between
  this enumeration and implementation.
  - **`TestR5Observability::test_read_quarterly_forecasts_warns_when_mask_columns_missing` (`:1510-1536`)
    — missing from the enumeration above; added here.** Its one row is `model_type: "LR_Base"` with `q50`
    set but **no `"date"` key at all**, so the `direct` frame built from it has no `date` column. This is
    a **defensive** shape, not one that arises from the live API in practice: `long_forecasts.date` is
    `nullable=False` (`sapphire/services/postprocessing/app/models.py:159`), so a real API batch can never
    have every row's `date` null, and `_read_long_forecasts_api`'s own all-null-column drop
    (`:1468`) therefore never removes this column against the live service either — the shape can only
    arise from a test fixture or a non-API caller. This is still a direct LR row, so decision
    R4-native-lr-precedence's native-row rule applies to it, in addition to PP-064's own missing-column
    guard (the general flag-OFF issue-year mask this test currently locks). Per item 2's "Order (flag
    OFF)" bullet above, PP-064's guard runs FIRST under this flag and is the one that fires the WARNING
    this test asserts on — but that guard only warns and skips its own mask on a missing column, it does
    not drop the row itself, so the row still reaches this item's native-row helper afterward. **The
    native-row helper DOES get a chance to run on this test's row, and drops it**: this case (a `date`
    column entirely absent from the `direct` frame, treated the same as every row's `date` being null) is
    already stated under item 2's "Order (flag ON)" bullet above, next to the null/unparseable-date rule,
    with a cross-reference from "Order (flag OFF)" back to it — both bullets now describe the same
    outcome for this row: the WARNING fires first, then the drop. Add a test for exactly this (LR frame
    with no `date` column at all → every LR row dropped and counted, no exception), at the helper level
    and through both readers under both flags.
    **This test's own existing WARNING assertion (`len(warn_lines) == 1` where `warn_lines` filters on the
    substring `"filter skipped"`, and `"date"` appears in that message) still holds under P1b**: it locks
    PP-064's own missing-column guard, which P1b does not modify, and P1b's own missing-`date`-column drop
    (this bullet's new rule) is a routine, counted exclusion — logged at INFO, like every other native-row
    drop reason, not at WARNING — so it adds no second line the `caplog.at_level(logging.WARNING, ...)`
    capture would see, and the substring filter would not match it even if it did. **New expected result
    to add:** the returned frame has **no** `LR_Base` row for this key (the row is dropped as
    unclassifiable, not merely unfiltered-by-year as PP-064's own "skip the mask" wording might suggest in
    isolation) — assert `len(result) == 0`, in addition to the existing WARNING assertions.
- In `tests/test_quarterly_api_writer.py`, only `:285` writes a raw LR row through the quarter forecast
  writer; `:64` and `:89` are skill-writer tests and are unaffected. **Grep of the other files completed
  (2026-09-28):** `test_aggregated_nan_guard.py`, `test_recalc_workflow.py` and `test_wiring_integration.py`
  assert no raw-LR write through the quarter forecast writer. `test_lead_aware_writer_reader_round_trip.py`
  does use `model_short: ["LR_Base", ...]` at `:262` and `:306`, but through `_write_skill_metrics_to_api`
  (the **skill** writer, in `TestAggregatedSkillPerLeadRoundTrip`) — a different function from this item's
  quarter **forecast** writer change, so those two are unaffected and the "only `:285`" claim stands as
  scoped (this item touches the forecast writer only).
- **Writer EM tests (this item skips `EM`/`ENSEMBLE_MEAN` rows; add to the enumeration above).** Every test
  below currently writes an `"EM"` (or, for the round-trip class, no second model) row through
  `_write_quarterly_ensemble_to_api` and asserts on the resulting record count/fields — after this item,
  an all-`EM` input writes **zero** records (same as `test_empty_data_returns_false`). For every test whose
  purpose is not EM itself, the fix is to switch the fixture's `model_short` from `"EM"` to `"Naive Mean"`
  (a generic groupby key with no EM-specific handling anywhere in the write path), which keeps testing what
  each test actually exists to test:
  - `test_quarterly_api_writer.py::TestQuarterlyEnsembleWriter`:
    - `test_writes_ensemble_rows` (`:212`): input is `["EM", "Naive Mean"]` — **new expected result:**
      `len(records) == 1` (the EM row is skipped; only Naive Mean survives). This test's own purpose (that
      both baseline aggregates are written) changes with the writer's behaviour, so its assertion changes
      too, not just its fixture.
    - `test_valid_from_valid_to` (`:233`): single `"EM"` row — switch to `"Naive Mean"`; date/horizon
      assertions unchanged (purpose is the date/lead computation, not EM).
    - `test_horizon_value_uses_resolver_config_lead` (`:254`): `["EM", "EM"]` — switch both to
      `"Naive Mean"`; assertions unchanged (purpose is lead resolution across two quarters).
    - `test_flag_on_row_without_own_horizon_value_falls_back` (`:312`): single `"EM"` row — switch to
      `"Naive Mean"`; assertions unchanged (purpose is the flag-ON fallback-to-config-lead behaviour).
    - `test_flag_on_aggregation_computed_date_round_trips_to_lead` (`:337`): monthly input
      `["EM", "EM"]` fed through `aggregate_monthly_fc_to_quarterly` — switch to `"Naive Mean"`;
      `aggregate_monthly_fc_to_quarterly`'s `groupby` (`src/aggregation.py` group_cols) has no model-specific
      logic, so this preserves the test's purpose (the FIX-6 date/lead round trip).
  - `test_aggregated_nan_guard.py::TestAggregatedNanGuardQuarterly` (all four rows built from `_make_quarterly_data`
    or an inline frame with `model_short: ["EM", ...]`) — switch every row to `"Naive Mean"`; each test's
    NaN-guard assertion (row count survives/drops on NaN `year`/`quarter_in_year`, WARNING presence) is
    otherwise unchanged:
    - `test_nan_year_only` (`:71`): unchanged assertions (`len(records) == 2`).
    - `test_nan_quarter_only` (`:85`): unchanged assertions (`len(records) == 2`, "Dropped" in log).
    - `test_all_valid_no_warning` (`:99`): unchanged assertions (`len(records) == 3`, no "Dropped").
    - `test_nan_forecasted_discharge_not_dropped` (`:140`): unchanged assertions (`len(records) == 3`).
    - (`test_all_nan_returns_false`, `:121`, is **not** listed: every row is dropped by the NaN-year guard
      before the model filter runs, so `result is False` either way — no fixture change needed.)
  - `test_lead_aware_writer_reader_round_trip.py::TestQuarterEnsembleFlagBehaviour` (`:387-434`,
    `test_flag_on_uses_row_horizon_value` and `test_flag_off_uses_configured_quarter_lead`): both build
    `self._quarter_df(...)` with `model_short: ["EM"]` (`:398`) and assert `len(fake_api.long_records) == 1`
    — switch to `"Naive Mean"`; horizon_value assertions (flag ON: the row's own lead; flag OFF: the
    configured lead) are otherwise unchanged, since this class's purpose is the flag-gated horizon_value
    seam, not EM.
- `tests/test_maintenance_long_term.py:541-574`, `:616` (quarterly dedup, lead-aware) **changes**: its
  `q_combined` and `q_fc` hold one raw model, so the new two-model prefilter excludes the key and nothing
  is saved. Give the fixture two eligible raw models. Any other maintenance test that asserts the run ends
  before the quarterly block (e.g. the early-exit tests at `:217`, `:245`) is listed.

**Acceptance (P1b):**
- The full module suite via `run_tests.sh` (as P1a) is green apart from the test edits listed above; zero
  unexpected skips; only the pre-existing xfail.
- `read_latest_quarterly_forecasts`' former Source 1 (LR aggregation) is now the unified
  `derive_quarterly_from_monthly_same_issue`-with-`QUARTER_NATIVE_RAW_MODELS` fallback, bounded by
  `forecast_date` under **both** flags on the rows it filters itself — both tests above pass,
  `read_monthly_forecasts` is **not** modified (`git diff` shows no change to it). Both readers'
  `aggregate_monthly_fc_to_quarterly` call sites are **removed**; `git diff` shows them gone from
  `read_quarterly_forecasts` and `read_latest_quarterly_forecasts` (`src/data_reader.py:3145, 3499, 3508`
  on the pre-P1b base).
- Both quarter readers' post-combine model filter keeps `QUARTER_SUPPORTED_MODELS`, not
  `AGGREGATED_SUPPORTED_MODELS`; the season readers' filter is unchanged (`git diff` shows no change to
  their call sites).
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

**One writer-paused window** (ops instruction, no code): merge the integration branch
`integ_quarter_p1b_p2` (PP-065 P1b–P1d, PP-064 B) into `maxat_sapphire_2` — that merge **is** the
postprocessing deploy trigger (owner decision R4-integration-branch; `deploy.pp` in the overview's
dependency graph). PP-064 A, FD-029 P1 and PP-065 P1a are already live separately (owner decision
R4-merge-is-deploy; verify per org, `PP-064.C.step0`) and are not part of this merge. **The automatic
bimonthly QUARTERLY recalc is allowed to run before this window and does not need to be paused** (owner
decision R4-recalc-runs, 2026-09-28 — see N7 above): it already applies P1a's 3-of-3 rule, on calendar
windows only, on servers today.

**This window follows PP-064 Chunk C's canonical "Order" sequence exactly**
(`../high_prio_gi_draft_pp_quarter_calendar_window_validation.md` § "Chunk C — rollout and verification");
it is stated once there, including the abort path if CI or the pull/verify step fails, and referenced here:
pause writers → the pre-deploy DB audit and **PP-065's own count of rule-A (same-issue monthly triplet)
rows per model × quarter** (done here, at the audit step, i.e. **before** the merge and the export — not
"before the recalc") → export → merge (`deploy.pp`) → wait for CI → pull and verify the image on each
server (using the configured tag, not a hardcoded `:latest`) → decision F (tjhm, PP-064 Chunk C detail 3)
→ recalc per org (detail 4 + this section's "After the recalc" checks below) → post-recalc checks →
resume writers → **run the first operational quarterly postprocessing run promptly** (canonical step 11) —
do not wait for the next scheduled LT cron day (kghm 10/25; tjhm 1). Exact command, per org: `bash
bin/bimonthly_long_term_postprocessing.sh <env_file_path> operational` (this also processes monthly and
seasonal ensembles — there is no quarter-only mode).

**Success criteria for this run** are stated once, in PP-064 Chunk C canonical step 11
(`../high_prio_gi_draft_pp_quarter_calendar_window_validation.md` § "Chunk C — rollout and verification") —
point there, do not restate the checks here. In particular, do **not** require the presence or absence of
any INFO-level log line (the quarterly-block/save/skip messages): canonical step 11 establishes they never
reach this entry point's WARNING-capped log (INFRA-029) regardless of what actually happened.

**Why the operational run, not the recalc, is the blank-card recovery point.** The in-window recalc (step
8) writes the derived seven-model rows for every quarter, the current one included
(`src/skill_metrics.py` ~:2741, `joint_forecasts = forecasts.copy()`), but EM/Skilled Mean/Naive Mean are
built from `merged`, an **inner join with observations** (~:2682-2690, ~:2806-2831) — so the recalc writes
ensembles for the current quarter only where that join happens to match, which is usually not the current
quarter but is not guaranteed never: a quarter's last month counts as observed at ≥50% of its days
(`src/data_reader.py` ~:1301-1302), so a writer-paused window that falls late in that month (e.g. kghm
Dec 17–24) can make the current quarter "observed" before this recalc runs (see PP-064 Chunk C canonical
step 11's PASS criterion 2 loophole note for the resulting read-back caveat). Only the quarterly block of
`postprocessing_operational_long_term.py` (~:207-232) writes them unconditionally, from existing skill plus
the latest derived forecasts, with no observation requirement. Until that operational run executes, the narrower
blank-card population the overview's "User-visible consequence" paragraph now defines (a key where at
most one of `LR_Base`/`LR_SM` has a non-null target-quarter forecast, so neither EM nor Naive Mean forms,
and its one surviving row, if any, is non-native — **not** simply "a fresh EM row plus a non-native LR
row", which also gets a fresh, visible Naive Mean row today) keeps showing a **blank card**. This manual run,
verified, is the actual recovery point for the blank card — not the merge, and not the recalc alone — and
closes the gap instead of leaving it open until the next natural cron day.

The export (canonical step 3) now captures **calendar-window-era, 3-of-3-era** quarter skill (whatever the
automatic bimonthly recalc has already produced under P1a's rule since it merged, owner decision
R4-recalc-runs — see the overview's R4-recalc-runs decision for the full list of what that recalc already
does), not the original pre-P1a skill; treat it as such when comparing before/after.

**After the recalc**, check and report per org (aggregate counts only):
- the four tombstone outcomes:
  - kghm flag ON: hv0 → hv1;
  - tjhm: hv0 replaced;
  - flag OFF: sentinel replaced;
  - sub-K groups and the old quarter EM skill rows tombstoned;
- the quarter skill rows suppressed by K = 10 (PP-064 C detail 5);
- persisted Naive Mean / Skilled Mean / EM forecast rows at keys the recalc did not emit (accepted,
  round-2 decision 2; report the count);
- unfillable gaps: quarter keys the maintenance detector reports that the gap-fill cannot fill (e.g. no
  skill row for the key);
- the persisted ensemble values and compositions;
- the Dataset B rows overwritten by fresh derived rows with the same key under flag OFF. This upsert is
  irreversible, which is why the export is taken first.

**Operationally:** these two things become visible at different points, and only one of them waits for
step 11. **The derived seven-model raw rows are written by the step-8 recalc itself** (see step 9's own
check, above: `joint_forecasts = forecasts.copy()` in `_calculate_aggregated_skill_metrics` passes every
raw forecast row through regardless of whether it joined an observation) — they are visible, with δ
bounds (FD-029), after step 8/9, before step 11 has even run. **Only the target quarter's own Naive Mean
/ Skilled Mean ensemble rows wait for step 11** to be guaranteed: step 8's recalc forms ensembles only
where its own inner join with observations matches, usually not the current quarter (see PP-064 Chunk C
canonical step 11's PASS criterion 2 and its pre-satisfied-branch note above for the exception). Do
**not** frame either as appearing "on the next quarter issue day": that framing is what the step-11
prompt-run requirement exists to avoid (the blank-card interval must not extend
to the next natural LT cron day). Fallback quarters show no LR row on kghm (round-2 decision 3; hydromet
notice); on tjhm a monthly-derived LR row may still appear as native until decision F, run inside this same
window, has also completed (round-4 tjhm interim).

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
