# PP-064: Score and ensemble only exact calendar-quarter windows, and carry a December-issued Q1 through

**Status**: Draft (2026-09-26, rev 5 after the third review round)
**Module**: `apps/postprocessing_forecasts`
**Priority**: High.
- Stored quarterly skill is wrong today: a rolling window is scored against a different quarter's
  observations. With `SAPPHIRE_SKILL_LEAD_AWARE=true` the wrong window is picked deterministically
  (#521: NSE ≈ −75 → +0.1 after correction).
- Chunk A must be deployed **before 2026-12-25**. With the flag ON, the operational reader drops the
  first kghm Q1, issued Dec 25 for the next year (Problem 6).

**Labels**: `postprocessing_forecasts`, `skill-metrics`, `long-term`, `quarter`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only. Owner decisions of 2026-09-26 are cited by letter (A–H), round 2 by number
("round-2 decision 1–6").
**Supersedes the code approach of**: GitHub #521 / branch `sandro_sapphire_2_quaterly_agg` (f0a83352)
**Related**:
- PP-065 (derived models, native-row selection, quarterly Naive/Skilled Mean, LR fallback), FD-029
  (card), LTF-014, LTF-016
- PP-056 (quarter skill at hv=0), PP-041 (no forecast-side invalidation), PP-063 (gap detector
  presence-only), PP-049 (flag-OFF `keep="last"` API-order dependence), PP-061 (aggregated writer
  `date=valid_from` key collisions), PP-020 (quantile averaging)
- MIG-008, DOC-009

Citations are to trunk `82946683`. Its postprocessing tree is identical to the #521 branch base
`559ec9e4`.

**The deployed flag state is unknown.** The local `kyg_data_forecast_tools/config/.env_kghm:502` sets
`SAPPHIRE_SKILL_LEAD_AWARE=true`; the local `.env_kghm_server` has **no** such line (default OFF). Chunk C
step 0 reads the real state per org. Both flag states are in scope.

## Contract (settled decisions this plan must not break)

- **Quarter = calendar Q1–Q4.** A valid quarter window has `valid_from` = the 1st of Jan/Apr/Jul/Oct
  **and** `valid_to` = the last day of Mar/Jun/Sep/Dec of the same year.
- **`horizon_value` = config `operational_month_lead_time`** (kghm 1, tjhm 0;
  `doc/prod/longforecast_quarter_season_hv_convention.md` RESOLUTION). Never overwrite a stored hv with a
  date-derived lead.
- **Native quarter row (one rule, shared with PP-065 and FD-029).** A QUARTER row is native iff
  - `date.day` == the configured quarter `issue_day`, **and**
  - the year-aware lead `(valid_from.year − date.year)·12 + (valid_from.month − date.month)` == the
    configured `lead_time`, as `select_operational_issuances` computes it (`src/data_reader.py:346-349`),
    not "month minus month".
  - Both values come from `operational_schedule_for_mode("quarter")`
    (`apps/iEasyHydroForecast/long_term_horizon_resolver.py:112-142`), which `data_reader` reaches via
    `_operational_schedules_for_horizon_type("quarter")` (`src/data_reader.py:141`).
- **Quarterly ensembles.** Today EM = mean(LR_Base, LR_SM), not skill-gated (2026-06-23 M1). Chunk A
  preserves today's membership; PP-065 removes quarterly EM (owner, 2026-09-26). No chunk of this plan
  changes ensemble membership.
- **Raw quarter models.**
  - LR_Base and LR_SM are native quarter models. While decision G's temporary fallback is active (until
    LTF-014 P0 and P2 are deployed on both orgs), a (code, year, quarter) with no native LR row gets LR
    derived from its monthly forecasts by PP-065 (rule A).
  - The seven re-enabled models are PP-065's. Until PP-065 lands, the trunk filter
    (`src/model_names.py:14-16`) keeps quarter LR-only; this plan does not change it.
- **Flag OFF stays byte-identical for calendar-aligned input** (PP-056 `:164`; flag-OFF golden
  `tests/test_skill_lead_aware_golden_baseline.py:93`), except for these intended changes:
  - Chunk A: a December-issued Q1 of the first requested year is read (Problem 7);
  - Chunk A: direct rows dated after `forecast_date` are ignored by the latest reader (Problem 6);
  - Chunk A: the writer drops a calendar `valid_from` with a null `valid_to`.
  - Chunk B makes no code change. PP-065 changes flag-OFF quarter output (owner-approved), including the
    `quarter_*` keys of that golden; its `month_*` and `season_*` keys stay byte-identical.
- `select_operational_issuances` (`src/data_reader.py:225-398`) is not modified. It only has to receive
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
   - **Mixed date strings raise.** The bare `pd.to_datetime(df["valid_from"])` at `:3748` raises
     `ValueError` on `"2024-04-01"` mixed with `"2024-04-01 00:00:00"` (measured, pandas 2.3.3).
   - **On the combined path that crash is silent.** `_read_long_combined_forecasts_api` catches every
     exception and returns None (`:3725-3731`); `read_quarterly_combined_forecasts` then returns an empty
     frame (`:3616-3620`).
2. **Select, flag ON.** `select_operational_issuances` keeps the latest issue per
   `(code, model, year, quarter_in_year, lead)` (`:339-341, 346-349, 390-391`). kghm Q2 therefore keeps
   the 25 May Jun–Aug window; tjhm Q1 keeps the 1 Mar Mar–May window.
3. **Select, flag OFF.** The only dedup of direct rows is `drop_duplicates(keep="last")` on
   `[code, year, quarter, model]` (`:3150-3157`). It runs only when the monthly-derived source is
   non-empty, and its order is API order (PP-049). The skill path does not dedup.
4. **Pair.** `calculate_quarterly_skill_metrics` merges on `["code","year","quarter_in_year"]`
   (`src/skill_metrics.py:2452`); the aggregated path merges at `:2682-2688`.
5. **Persist: three date populations exist for QUARTER LR rows** (local DB, 2026-09-25; aggregate counts
   only).
   - **(a) Native rows** (Contract rule), `date` = the real issue date.
   - **(b) Rewrites, regenerated on every run.** `_write_aggregated_forecasts_to_api` writes **every** row
     of the frame it gets, raw LR rows included, with `flag: 0` and, under flag OFF,
     `record_date = valid_from` (`src/api_writer.py:1193-1204`). Sources:
     - operational: `existing_q` concat, `_dedup_quarterly_joint`, save
       (`postprocessing_operational_long_term.py:72-91, 220-227`);
     - maintenance gap-fill merge-back (`postprocessing_maintenance_long_term.py:357-376`);
     - recalc raw rows (`recalculate_skill_metrics.py:386, 403`).

     Locally, 3,007 LR_BASE hv1 calendar rows are rewrites next to 3,635 native ones.
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
   - Widening that bound alone lets a **back-dated** run pick a later issue: the reader keeps only the
     maximum (year, quarter) (`:3448-3453`). The flag-OFF read already admits any row dated in
     `today.year`, so this is live under flag OFF today.
7. **Flag-OFF reads miss a December-issued Q1 of the first requested year.** `read_quarterly_forecasts`
   flag OFF reads issue years from `start_year` (`:3120-3127`); flag ON widens the read
   (`:3110-3111`) and trims by target year (`:3137`).
   - Recalc: Q1 of the first recalc year is lost.
   - **Maintenance, every run:** gap-fill calls `read_quarterly_forecasts(codes, q_years.min, q_years.max)`
     (`postprocessing_maintenance_long_term.py:305-310`), so under flag OFF a kghm Q1 gap is never
     fillable.
8. **Empty skill → no quarterly ensembles, locked by a test.** The operational run skips all quarterly
   ensembles on an empty skill frame (`postprocessing_operational_long_term.py:209`), and
   `_create_aggregated_ensemble_forecasts` returns without ensembles (`src/ensemble_calculator.py:632-634`);
   `tests/test_quarterly_ensemble_creation.py:329` asserts it. Monthly behaves alike: an empty monthly
   skill frame exits the run with no monthly ensembles (`postprocessing_operational_long_term.py:145-151`).

## Chunk A — calendar-window validation, December Q1, first-year Q1 (both flag states)

**Goal**:
- A non-calendar quarter row is excluded (never relabelled) at the direct-read choke point and at the writer.
- A December-issued Q1 survives the operational reader; a back-dated run cannot pick a later issue.
- Under flag OFF, a December-issued Q1 of the first requested year is read.

**Files (only these may be modified)**:
- `apps/postprocessing_forecasts/src/aggregation.py`: new pure helper
  `filter_calendar_quarter_windows(df) -> tuple[pd.DataFrame, int]` next to `QUARTER_MONTHS`
- `apps/postprocessing_forecasts/src/data_reader.py`:
  - `_normalize_combined_forecasts`: apply the helper **for `horizon == "quarter"` only, before the
    existing `valid_from` parse** (`:3748`); season behaviour unchanged
  - `read_quarterly_forecasts`: the flag-OFF direct read (`:3120-3127`) only
  - `read_latest_quarterly_forecasts`: the target-year upper bound (`:3331-3334, 3413`) and a direct-row
    date bound
- `apps/postprocessing_forecasts/src/api_writer.py`: quarter branch of `_write_aggregated_forecasts_to_api`
  (`:1160-1204`)
- Tests: new `apps/postprocessing_forecasts/tests/test_quarter_calendar_window.py`; new tests may be
  appended to existing quarter test files, but existing tests are not edited

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* Do not
change:
- the writer's `horizon_value` and `record_date` logic (`api_writer.py:1172-1175, 1199-1204`)
- ensemble membership
- `select_operational_issuances`
- `model_names.py`
- aggregation grouping
- the season branch

Do not cherry-pick from `sandro_sapphire_2_quaterly_agg`. Never `git stash`.

**Helper semantics**
- Parse with `pd.to_datetime(..., format="mixed", errors="coerce").dt.normalize()`. Reader output mixes
  date-only strings and timestamps (`astype(str)` at `:3170-3172, 3445-3447`).
- **Write the normalized `valid_from` back** into the returned frame, so the existing parse at `:3748`
  cannot raise. Leave `valid_to` with the dtype it came in with.
- Both `valid_from` and `valid_to` **columns** absent → return unchanged. Only the `valid_to` column
  absent → every row is invalid (empty result, logged). For a row, either value null or unparseable →
  invalid, dropped.
- Keep a row iff `valid_from` is day 1 of month 1/4/7/10 **and** `valid_to` is the last day of month
  `valid_from.month + 2` of the **same year**.
- Log the dropped count **at INFO** (stale rolling rows stay in the DB, so this fires on every read), with
  the horizon and no station codes.

**Writer guard (quarter branch only)**
- A row whose `valid_from` **and** `valid_to` are both null keeps the synthesized calendar window
  (today's behaviour).
- Otherwise a record is written only if the window is calendar **and** matches `(year, quarter_in_year)`.
- A calendar `valid_from` with a null `valid_to` is **dropped**. This is an intended change from trunk,
  which writes the row's `valid_from` with a synthesized `valid_to` (`:1193-1197`).
- One aggregated count per call (not per row), no station codes.

**Year and date bounds in `read_latest_quarterly_forecasts` (Problem 6)**
- The flag-ON target-year trim admits `end_year + 1`, so a 25 Dec issue yields next year's Q1. The
  issue-date API read (`:3389-3403`) is unchanged.
- Under **both** flags, drop direct rows whose `date` is after `forecast_date` before the sources are
  combined. PP-065 applies the same bound to the monthly source.

**First-year Q1 (Problem 7), flag OFF only**
- Read issue years from `start_year − 1`, then `_trim_to_target_year_range(direct, "year", start_year,
  end_year)`, mirroring flag ON (`:3110-3111, 3137`). Keep the `horizon_value` filter.

**Tests (Arrange → Act → Assert, station `19999`)**

Fakes of `_read_long_forecasts_api` (`:1406`) must filter by the requested issue-date years and
`horizon_value`, as the real call does; a fake that ignores its arguments cannot make A-5, A-9 or A-10
fail on trunk. (A-4 was dropped in rev 3 as redundant with A-1 flag ON; IDs are kept stable.)

**The A tests must survive PP-065 P1b**, which adds the native-row filter to both readers:
- Every A test writes a kghm-shaped `quarter.json` with **both** `operational_month_lead_time` (1) and
  `operational_issue_day` (25). The autouse fixture in `tests/test_quarterly_data_reader.py:30-49` writes
  the lead only, which puts P1b into its degraded mode.
- PP-065 P1b lists `tests/test_quarter_calendar_window.py` among the tests it may change.
- **A-1. Reader** (flag ON/OFF × `read_quarterly_forecasts` / `read_latest_quarterly_forecasts`).
  - Input: kghm-like direct rows issued 2024-03-25 (Apr–Jun, 100), 04-25 (May–Jul, 200) and 05-25
    (Jun–Aug, 300).
  - Expect one Q2 row: value 100, `valid_to` 2024-06-30. For flag OFF, do not assert `date`/`horizon_value`
    (`data_reader.py:83-94`).
- **A-2. `valid_to` enforcement.** Direct rows `2024-04-01..2024-07-31`, `2024-04-01..2025-06-30` and
  `2024-04-01..` with null `valid_to` are excluded by both readers and by the writer.
  (Mutation check: deleting the `valid_to` predicate must make A-2 fail.)
- **A-3. Malformed input through the readers**, including `read_quarterly_combined_forecasts`:
  unparseable dates; mixed `"2024-04-01"` / `"2024-04-01 00:00:00"`; an all-invalid batch. Assert that the
  **valid rows are returned**: the combined path turns a crash into an empty frame, so "no exception"
  proves nothing. The mixed-format case fails on trunk. Separately, a frame **without a `valid_to`
  column** → an empty result plus the dropped-count log line.
- **A-5. December Q1, flag ON.** `forecast_date = 2026-12-25`, direct LR_Base/LR_SM rows issued 2026-12-25
  for 2027-01-01..2027-03-31 at hv1 → `read_latest_quarterly_forecasts` returns them with `year == 2027`,
  `quarter_in_year == 1`. It fails on trunk.
- **A-6. Gap detector through the reader, both directions.** Mock only the API client
  (`data_reader.SapphirePostprocessingClient`) → `read_quarterly_combined_forecasts` →
  `detect_missing_quarterly_ensembles` (`src/gap_detector.py:370-496`; do not edit it), the maintenance
  path (`postprocessing_maintenance_long_term.py:295-297`).
  - (i) A calendar quarter whose only EM row is rolling-windowed is reported as a gap after Chunk A
    (the detector's default `{"EM"}`; PP-065 later switches the maintenance caller to Naive Mean).
  - (ii) A quarter whose only raw rows are rolling is not reported as a gap for a calendar quarter it
    does not cover.
- **A-7. Writer.** A non-calendar window, one disagreeing with `(year, quarter_in_year)`, or a calendar
  `valid_from` with null `valid_to` → no API record, one aggregated log line. Both windows null →
  synthesized window, as today. A calendar row → a record identical to today's (hv and date per flag).
  The positive case uses a **non-LR** model (e.g. `Naive Mean`): PP-065 P1b makes the writer skip LR rows.
- **A-8. Season rows** through `_normalize_combined_forecasts` are unchanged.
- **A-9. Back-dated run, both flags.** `forecast_date = 2026-09-25`, kghm-like rows issued 2026-09-25
  (Oct–Dec) and 2026-12-25 (Jan–Mar 2027) → `read_latest_quarterly_forecasts` returns Q4 2026. Flag OFF
  fails on trunk; flag ON must fail if the date bound is removed after the year bound is widened.
- **A-10. First-year Q1, flag OFF.** A direct row issued 2024-12-25 for 2025-01-01..03-31 at hv1:
  `read_quarterly_forecasts(codes, 2025, 2025)` returns it as Q1 2025, and a row issued 2025-12-25 for
  Q1 2026 is trimmed. Fails on trunk.

**Acceptance**:
- Record the full module suite counts before editing (a reviewer's simulated Chunk A gave 1832 passed /
  1 xfail).
- After: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` passes with **no
  existing test edited**, zero unexpected skips, and only the pre-existing xfail. If an existing test
  fails because of an intended change listed in the Contract, stop and report; do not edit it.
- A-1, A-2, A-3 (mixed), A-5, A-9 (flag OFF) and A-10 fail on trunk.
- `git diff --stat` touches only the listed files.
- `ruff check` / `ruff format --check` are clean on the touched files.

## Chunk B — empty-skill check (no code change expected)

**Sequencing.** A → PP-065 → B. Rev 3's B rules moved into PP-065, which already edits the same files:
- **B2 (native-row selection)** → PP-065 P1b. B2 inside `_dedup_quarterly_joint` and the maintenance
  `q_merged` dedup is dropped: ensembles are computed before those concats, and after PP-065 the writer no
  longer writes LR rows, so those dedups have nothing left to select.
- **B4 (3-of-3 observation months)** → PP-065 P1a.
- **B6 (stop writing raw LR rows)** → PP-065 P1b.
- **B3 (persisted (b)/(c) rows):** accepted. PP-065's native-row rule never selects them. Deleting them is
  D8; tjhm is handled by decision F (Chunk C step 3).

Chunk B no longer edits `data_reader.py` or any other file.

**B5. Empty skill: quarter behaves like monthly** (overview D3).
- Monthly: an empty monthly skill frame exits the run with no monthly ensembles
  (`postprocessing_operational_long_term.py:145-151`).
- Quarter: the operational run skips all quarterly ensembles on an empty skill frame (`:210`), and
  `_create_aggregated_ensemble_forecasts` returns without ensembles (`src/ensemble_calculator.py:632-634`).
- **This still holds under round-2 decision 1.** Naive Mean has no skill gate on membership, but the
  run-level empty-skill skip applies to every ensemble, Naive Mean included, as for monthly.
- Expected result: no code change, and `tests/test_quarterly_ensemble_creation.py:329` unchanged after
  PP-065. Report if the check shows otherwise.

## Chunk C — rollout and verification (ops; mostly no code)

**Order** (as in the overview graph):
- Chunk A and PP-065 deployed (PP-065 includes the 3-of-3 observation rule; 2-of-3 observations against
  3-of-3 derived forecasts would bias the scores).
- **One window between LT cron days** (kghm 10 and 25; tjhm 1): deploy PP-065, run the decision-F step
  (tjhm), then the recalc per org. No LT run may fall inside the window.

0. **Server state read per org** (read-only): `SAPPHIRE_SKILL_LEAD_AWARE`,
   `ieasyhydroforecast_ml_long_term_supported_modes` and `ieasyhydroforecast_min_pairs_long_term_quarter`
   in the env the postprocessing **and** dashboard containers actually load. Record them in the PR; the
   recalc runs with that state. Also confirm the `quarter` config carries both
   `operational_month_lead_time` and `operational_issue_day`: without the issue day,
   `operational_schedule_for_mode("quarter")` raises (`long_term_horizon_resolver.py:138-142`). Under flag
   OFF, PP-065 then skips the derivation and the native-row filter with one WARNING; under flag ON the
   quarter readers raise, as on trunk (`long_term_horizon_resolver.py:84-111` notes taj-style configs that
   omit it).
1. **Pre-recalc backup per org:** `pg_dump`/`COPY` of the QUARTER `skill_metrics` and `long_forecasts`
   rows, kept out of the repo.
2. **Pre-deploy DB audit per org** (read-only SQL, aggregate counts only, no station codes).
   - Classify QUARTER `long_forecasts` rows by flag, model family, window class (calendar/rolling/other),
     `horizon_value` and date population: (a) native (Contract rule); (b) `date = valid_from`; (c) other,
     e.g. Dec 1.
   - Count QUARTER `skill_metrics` rows by `date` year and `horizon_in_year`.
   - Local baseline (2026-09-25, both orgs mixed): kyg LR flag 0 = 45,581 calendar / 15,922 rolling; all
     6,860 skill rows are calendar-keyed and dated 2026.
3. **Decision F (tjhm only), inside the window, before the recalc** (round-2 decision 4). Locally only 16
   of 536 tjhm LR_BASE hv0 Q2/Q3 rows match the hindcast CSV within 1%, and the rows span Q1–Q4: they hold
   postprocessing aggregates.
   - **Predicate, by provenance:** tjhm LR_Base/LR_SM QUARTER rows with `date = valid_from` that have **no
     counterpart** in the LT module's CSV (hindcast plus operational appends), same (code, model, `date`,
     `horizon_value`), in **any** quarter, 2026 included. tjhm native rows also have `date = valid_from`
     (day 1, lead 0), so provenance, not the date, separates them.
   - **Preserve manifest:** the genuine operational and recovered rows (counterpart present). Build a
     **dry-run manifest** of rows to delete and rows to preserve, aggregate counts only in the PR.
   - **Re-import** the native Q2/Q3 hindcast values: the migrator
     (`bin/utils/migration_py/long_forecast.py`) full-import, **no `--cutoff`**, on a CSV filtered to the
     calendar issues (04-01 and 07-01), for LR_Base and LR_SM (`--mode quarter --model <m>`), placed under a
     scratch `--data-dir` as `long_term_predictions/quarter/<model>/<model>_hindcast.csv` (`:8`, `:297`).
     Run with `--dry-run` first, then for real, then read the rows back.
     - Do **not** use `bin/initialize_long_forecast_history.sh`: its per-key cutoff map (MIN(`date`) per
       key) drops every row dated on or after the earliest stored row (`long_forecast.py:513-515`), so on
       a populated DB it imports nothing for these keys.
     - A counterpart key outside 04-01/07-01 whose DB value differs from its CSV value is listed in the
       manifest and escalated, not silently kept.
   - **Delete** the manifest's non-counterpart rows in a reviewed step with the service owner, after the
     backup. Decision G's fallback then derives LR for those quarters (not persisted; round-2 decision 3).
   - F also owns **clearing the pre-existing tjhm rows dated 2026-10-01** before any `lt_recovery` of that
     date (LTF-014 points here).
   - Private before/after DB-vs-CSV value check afterwards.
4. **Recalc** with each deployment's actual flag state (`doc/prod/long_term_deploy_runbook.md`
   § Lead-aware skill).
5. **Post-checks.**
   - Pair counts: before the recalc, derive the expected `n_pairs` per `(model, quarter, lead)` and flag
     state from the audit: unique target years with a native-rule-selected (or derived) calendar forecast
     row **and** a 3-of-3 observation. Explain every difference from the result.
   - Tombstone count per `(model, quarter, hv)` per org, including the old quarter EM skill rows.
   - Suppressed quarter skill rows per org at K = 10 (decision C; `src/skill_metrics.py:2834-2849`).
     Locally the tjhm median `n_pairs` was 5–6 before the fix.
   - The per-org quarter skill frame, read as the pipeline reads it (tombstones dropped,
     `src/data_reader.py:106, 2833`), is **non-empty**. Otherwise every quarterly ensemble is skipped
     (Problem 8). If K = 10 leaves an org with no quarter skill at all, B5 means no Naive Mean either:
     **escalate to the owner before the hydromet notice** goes out.
   - Freshly written QUARTER rows contain no rolling windows and no LR rows.
   - A persisted-derived-row round trip (write → read) yields the native-rule-selected row.
   - Spot check the #521 station privately (its code is never written to the repo). A plausible value is
     a spot check, not proof.
   - **Skill values change sharply** (e.g. flag-OFF kghm median ~0.8–0.9 → ~0.2–0.5). The user-facing
     note is the overview's release-note step; it is not written here.
   - **Before LTF-014 P2**, kghm LR Q1 skill comes from decision G's derived LR rows, because no native Q1
     hindcast exists yet. It changes again once native hindcasts land and the fallback is removed.
   - PP-065 P2 adds its own counts (stale ensemble rows, unfillable gaps).
6. **What the recalc leaves behind:**
   - Rolling-windowed rows (raw and EM), inert after Chunk A: owned by PP-041 / the stale-rows decision.
   - Old EM, Naive Mean and Skilled Mean rows at keys the recalc no longer emits (accepted, round-2
     decision 2; D8 / PP-041). Under flag OFF, rows with the same key as a fresh row are overwritten.

## Out of scope

- Re-enabling the seven models for quarter, native-row selection, and quarterly Naive/Skilled Mean
  without EM (all PP-065, decision B and round-2 decision 1).
- Dashboard (FD-029/FD-030). Schedule and target construction (LTF-014). Monthly label fixes (LTF-016).
- Deleting DB rows beyond decision F (the stale-rows decision).
- Skill/ensemble-level duplicate window guards. Their inputs come only from the guarded readers or from
  derived rows that are calendar by construction, so they would add no protection.
