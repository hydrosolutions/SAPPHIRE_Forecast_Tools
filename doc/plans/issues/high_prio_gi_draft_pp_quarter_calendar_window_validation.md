# PP-064: Score and ensemble only exact calendar-quarter windows, and carry a prior-year-issued Q1 through

**Status**: In Progress. **Chunk A merged to trunk (#527, `955bd384`, 2026-09-27), presumed already live on
both servers** via auto-pull (owner decision R4-merge-is-deploy, 2026-09-28; verify per org, Chunk C step 0). Remaining:
Chunk B (the B5 check only, after PP-065 P1d) merges into the integration branch `integ_quarter_p1b_p2`
(owner decision R4-integration-branch, 2026-09-28), alongside PP-065 P1b–P1d; that branch merges to trunk only in the P2
window, which is the postprocessing deploy. Chunk C (ops, rollout and verification) follows. Draft
(2026-09-26, rev 6 after the fourth review round; updated 2026-09-27 for the owner-approved native-Q1
restriction found by an end-to-end dev-DB cross-check, `115eb886`/`18efd261`, both now inside the merged
`955bd384`)
**Module**: `apps/postprocessing_forecasts`
**Priority**: High.
- Stored quarterly skill is wrong today: a rolling window is scored against a different quarter's
  observations. With `SAPPHIRE_SKILL_LEAD_AWARE=true` the wrong window is picked deterministically
  (#521: NSE ≈ −75 → +0.1 after correction).
- Chunk A must be deployed **before 2026-12-25**, as the prerequisite of PP-065 — **presumed already
  satisfied** via auto-pull (owner decision R4-merge-is-deploy; verify per org). LTF-014 P0 is deferred
  (configs stay `forecast_months [3..9]`), so no native kghm LR row is issued on Dec 25. The first kghm
  Q1 comes from PP-065's derived path (the seven models plus decision G's LR fallback), which needs the
  same next-year target (PP-065 item 2); under flag OFF its derived and ensemble rows are persisted
  dated 2027-01-01. Problem 6 and A-5 remain as the mechanism for direct Dec-25 rows.

**Labels**: `postprocessing_forecasts`, `skill-metrics`, `long-term`, `quarter`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only. Owner decisions of 2026-09-26 are cited by letter (A–H), round 2 by number
("round-2 decision 1–6"), and 2026-09-28 decisions by the `R4-*` labels (R4-native-lr-precedence,
R4-merge-is-deploy, R4-integration-branch, R4-recalc-runs — labelled, not lettered or bare-numbered, to
avoid colliding with the 2026-09-26 letters E/F/G and with PP-065's own independently-numbered decision
list; see the overview's "2026-09-28, round 4" heading for the full reasoning).
**Supersedes the code approach of**: GitHub #521 / branch `sandro_sapphire_2_quaterly_agg` (f0a83352)
**Related**:
- PP-065 (derived models, native-row selection, quarterly Naive/Skilled Mean, LR fallback), PP-066
  (`select_operational_issuances`'s own unclamped issue-day match and mixed-tz crash — neither fixed
  here, Contract below), FD-029 (card), LTF-014, LTF-016
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
  - `date.day` == the configured quarter `issue_day`, **clamped to the length of the issue month** —
    exactly as the producer schedules it (`apps/long_term_forecasting/lt_utils.py:170-172`,
    `nearest_scheduled_issue_date`: `min(issue_day, calendar.monthrange(year, month)[1])`; e.g.
    `issue_day` 31 in June → June 30 is native), **and**
  - the year-aware lead `(valid_from.year − date.year)·12 + (valid_from.month − date.month)` == the
    configured `lead_time`, as `select_operational_issuances` computes it (`src/data_reader.py:346-349`),
    not "month minus month".
  - Both values come from `operational_schedule_for_mode("quarter")`
    (`apps/iEasyHydroForecast/long_term_horizon_resolver.py:112-142`), which `data_reader` reaches via
    `_operational_schedules_for_horizon_type("quarter")` (`src/data_reader.py:141`).
  - **Caveat:** `select_operational_issuances`'s own match (`:349-353`) compares `date.day` to the
    configured `issue_day` **unclamped** — it does not itself apply the clamp above. This plan does not
    touch that function (Contract, below); the unclamped match is PP-066's scope
    ("`select_operational_issuances` robustness: mixed-tz dates and unclamped issue day"), not
    acknowledged-but-unowned here.
- **Quarterly ensembles.** Today EM = mean(LR_Base, LR_SM), not skill-gated (2026-06-23 M1). Chunk A
  preserves today's membership; PP-065 removes quarterly EM (owner, 2026-09-26). No chunk of this plan
  changes ensemble membership.
- **Raw quarter models.**
  - LR_Base and LR_SM are native quarter models. While decision G's temporary fallback is active (until
    LTF-014 P0 and P2 are deployed on both orgs; no end date while P0 is deferred), a (code, year,
    quarter) with no native LR row gets LR derived from its monthly forecasts by PP-065 (rule A).
  - The seven re-enabled models are PP-065's. Until PP-065 lands, the trunk filter
    (`src/model_names.py:14-16`) keeps quarter LR-only; this plan does not change it.
- **Flag OFF stays byte-identical for calendar-aligned input** (PP-056 `:164`; flag-OFF golden
  `tests/test_skill_lead_aware_golden_baseline.py:93`), except for these intended changes:
  - Chunk A: **only** the schedule-dated native issuance targeting Q1 of `start_year` is read as the
    prior-year exception — its `date` must equal the quarter mode's own schedule issue date (`valid_from`
    minus `lead_time` months, on `issue_day`, clamped to that month's length; Dec 25 for kghm with
    on-schedule data). If the schedule cannot be resolved, or resolves with `issue_day < 1`, there is no
    exception at all (owner decision 2026-09-27, `115eb886`, now merged to trunk in `955bd384` / #527;
    see the "First-year Q1 (Problem 7)" section below);
  - Chunk A: direct rows dated after `forecast_date` are ignored by the latest reader (Problem 6);
  - Chunk A: the writer drops a calendar `valid_from` with a null `valid_to`.
  - Chunk B makes no code change. PP-065 changes flag-OFF quarter output (owner-approved), including the
    `quarter_*` keys of that golden; its `month_*` and `season_*` keys stay byte-identical.
- `select_operational_issuances` (`src/data_reader.py:225-398`) is not modified. It only has to receive
  calendar-only rows.

## Mechanism and problems (verified on pre-#527 trunk `82946683`)

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
     (`aggregation.py:293-299`), with the writer keeping the monthly hv. Example: 1,505 LR_BASE hv1 Q1
     rows are dated **Dec 1**. Flag OFF: `date = valid_from`.

   Populations (b) and (c) have calendar windows, so **no window filter distinguishes them from (a)**.
   The writer also re-emits rolling windows, because it overrides the synthesized calendar window with
   the row's own `valid_from`/`valid_to` (`:1193-1197`).
6. **The prior-year-issued Q1 is dropped by the operational reader (flag ON).** `read_latest_quarterly_forecasts`
   sets `end_year = today.year` (`:3331-3334`) and, with the flag on, trims direct rows to target years
   `[start_year, end_year]` (`:3413`). On 2026-12-25 a direct Q1 2027 row (`year = 2027`) is removed, so no
   operational Q1 ensemble is built from it (`postprocessing_operational_long_term.py:211`). While LTF-014
   P0 is deferred no native Dec-25 LR row exists; PP-065's derived source needs the same next-year target
   (its item 2).
   - Widening that bound alone lets a **back-dated** run pick a later issue: the reader keeps only the
     maximum (year, quarter) (`:3448-3453`). The flag-OFF read already admits any row dated in
     `today.year`, so this is live under flag OFF today.
7. **Flag-OFF reads miss a prior-year row targeting Q1 of the first requested year** (Dec 25 for kghm
   with on-schedule data; justification below). `read_quarterly_forecasts`
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

## Chunk A — calendar-window validation, prior-year-issued Q1, first-year Q1 (both flag states)

**Goal**:
- A non-calendar quarter row is excluded (never relabelled) at the direct-read choke point and at the writer.
- The prior-year-issued Q1 survives the operational reader's **direct source**; a back-dated run cannot
  pick a later direct issue. The operational reader's **monthly-derived source** has no such bound under
  either flag (`read_latest_quarterly_forecasts` calls `read_monthly_forecasts(codes, start_year,
  end_year)` flag ON, `:3415`, or raw `_read_long_forecasts_api(codes, start_year, end_year)` flag OFF,
  `:3423` — neither takes `today`/`forecast_date`) — that gap is out of this chunk's scope; PP-065
  rewrites this source and owns bounding it (PP-065 item 2).
- Under flag OFF, **only** the schedule-dated native issuance targeting Q1 of `start_year` is read as the
  prior-year exception (Dec 25 for kghm with on-schedule data) — not any other prior-year row that merely
  shares the target quarter (owner decision 2026-09-27, `115eb886`, after a dev-DB read found the
  broader match also admitted a persisted monthly-derived Q1 row that nulled real values; see "First-year
  Q1 (Problem 7)" below).

**Files (only these may be modified; final shape merged to trunk as `955bd384` / #527, incl. the
2026-09-27 native-Q1 restriction `115eb886`/`18efd261` below)**:
- `apps/postprocessing_forecasts/src/aggregation.py`:
  - new date-parsing core: `_LOCAL_CALENDAR_DATE_LOWER_BOUND = pd.Timestamp("1677-09-22")` (`:38`),
    `_parse_local_calendar_date` (element-wise, `:41-86`), `_local_calendar_date_per_value` (the
    per-value fallback, `:89-97`) and the public `local_calendar_date(s) -> pd.Series` (`:100-199`) —
    this is the ONE date-parsing helper for the whole PP-064 fix; the reader no longer has its own
    (see "Helper semantics" below for its exact semantics)
  - new pure helper `filter_calendar_quarter_windows(df) -> tuple[pd.DataFrame, int]` (`:202-278`),
    immediately below `local_calendar_date`
- `apps/postprocessing_forecasts/src/data_reader.py`:
  - `_normalize_combined_forecasts` (`:3823-3921`): apply `filter_calendar_quarter_windows` **for
    `horizon == "quarter"` only, before the existing `valid_from` parse** (now `:3880`); season
    behaviour unchanged. No local helper of its own — the module has none any more.
  - `read_quarterly_forecasts` (`:3044-3243`): the flag-OFF direct read (`:3120-3127`) only, using
    `local_calendar_date` (imported from `src.aggregation` at `:3072`) for the issue-year mask
  - `read_latest_quarterly_forecasts` (`:3373-3556`): the target-year upper bound and a direct-row date
    bound, also using `local_calendar_date` (imported at `:3397`, used at `:3480`)
- `apps/postprocessing_forecasts/src/api_writer.py`: quarter branch of `_write_aggregated_forecasts_to_api`
  (`:1070-1325`, guard logic at `:1194-1249`)
- Tests: new `apps/postprocessing_forecasts/tests/test_quarter_calendar_window.py`; new tests may be
  appended to existing quarter test files, but existing tests are not edited

**Agent instruction**: *"Do NOT change any existing function signatures, data flow logic, or control
flow. Your changes must be purely additive or modify only the specific behavior described."* Do not
change:
- the writer's `horizon_value` and `record_date` logic (`api_writer.py:1169-1176, 1264-1269`)
- ensemble membership
- `select_operational_issuances`
- `model_names.py`
- aggregation grouping
- the season branch

Do not cherry-pick from `sandro_sapphire_2_quaterly_agg`. Never `git stash`.

**Helper semantics — `local_calendar_date` (`aggregation.py:100-199`)**

The single date-parsing helper behind `filter_calendar_quarter_windows`, both quarter readers' issue-date
masks, and the writer guard. Not `_issue_date_local_calendar_date` in `data_reader.py` — that name/location
was an earlier round; it was deleted, and every call site now imports `local_calendar_date` from
`src.aggregation`.

- Equivalent, per value, to parsing with `pd.Timestamp(v)` (`_parse_local_calendar_date`,
  `aggregation.py:41-86`) — not a bare `pd.to_datetime(s, format="mixed", errors="coerce")`, which raises
  `AttributeError` on a subsequent `.dt` access when `s` mixes tz-aware and tz-naive strings across rows.
- A tz-aware value is dropped to its LOCAL wall-clock date (`tz_localize(None)`, never `tz_convert`,
  which would shift the underlying instant to UTC first).
- Out-of-range → `NaT`, never raises: a value before `1677-09-22` (just past `pd.Timestamp.min`) or not
  representable at ns resolution (e.g. `pd.Timestamp` accepts `"9999-12-31"` at second resolution, but
  casting it to ns overflows) both return `NaT`; the lower-bound check runs on the value BEFORE
  `.normalize()`, because normalizing a value already close to the minimum can itself silently wrap to a
  bogus date near the upper limit instead of raising (observed: `2262-04-11`). The whole per-value parse
  is wrapped in a broad `except Exception`, since `pd.Timestamp(v)` can invoke arbitrary methods on an
  arbitrary `v` (e.g. `__str__`) that can raise anything.
- **Vectorised fast path.** A `datetime64` column (naive or tz-aware, any unit) is handled fully
  vectorised when it casts to `datetime64[ns]`; a non-ns unit falls back to per-value parsing (casting to
  ns to normalize it could itself overflow for an extreme value).
- **De-duplication is restricted to exact `str` values, keyed on the string itself** — a pure function of
  its argument, so this cannot collide with any other type or value. Every other value (`Timestamp`,
  `datetime`, `date`, `np.datetime64`, a number, a bool, `None`/`NaN`/`NA`/`NaT`, or anything unhashable)
  is parsed individually every time, with no cache key at all. An earlier version de-duplicated by a
  generic type+repr key; three review rounds each found a new way to break it (a same-instant tz-aware
  value at two different UTC offsets; `0`/`False` and `1`/`True`/`1.0`, all `==` but parsed differently;
  `str()`/`repr()` collisions between unrelated types; a `__str__` that raises) — so this is restricted to
  the one case correct BY CONSTRUCTION, not by enumerating collision classes.
- Empty input, and input that is entirely null regardless of its own dtype, both return naive
  `datetime64[ns]`.

**`filter_calendar_quarter_windows` (`aggregation.py:202-278`)**
- **Write the normalized `valid_from` back** into the returned frame (via `local_calendar_date(...).dt.normalize()`),
  so the existing parse further downstream (`data_reader.py:3880`) cannot raise on the mixed formats this
  helper already resolved. Leave `valid_to` with the dtype it came in with.
- Both `valid_from` and `valid_to` **columns** absent → return unchanged. Only one of the two columns
  present → every row is invalid (empty result). Both present → a row with either value null or
  unparseable is invalid and dropped.
- Keep a row iff `valid_from` is day 1 of month 1/4/7/10 **and** `valid_to` equals `valid_from + 3
  months − 1 day` — but the expected `valid_to` is **only computed for rows whose `valid_from` year is
  `<= 2261`** (`aggregation.py:256-271`): `datetime64[ns]` tops out at `2262-04-11`, so the `+3 months`
  arithmetic can itself overflow for an otherwise-valid `valid_from` within ~3 months of that limit; any
  row above the cutoff cannot be verified this way and is simply not a calendar quarter.

**`_normalize_combined_forecasts` (`data_reader.py:3823-3921`), quarter branch**
- Log the dropped count **at INFO** whenever it is nonzero (`:3843-3848`) — stale rolling rows stay in
  the DB, so this fires on every read — with the horizon and no station codes.
- **Early empty return** (`:3849-3877`) when the filter leaves zero rows with `valid_from` absent from
  the result, OR when neither `valid_from` nor `valid_to` was present in the input at all. The latter
  case additionally logs its own **WARNING** (`:3862-3871`) naming the dropped row count and horizon,
  because `filter_calendar_quarter_windows` itself logged nothing for a "neither column present" input
  (it returns such a frame unchanged, 0 dropped) — without this, those rows would be silently discarded.
  The returned empty frame's columns are taken from the **input frame's own columns** (plus `year`/
  `quarter_in_year` if absent), not a fixed list — this avoids a `KeyError` on the subsequent `valid_from`
  parse (`:3880`) for callers with no try/except of their own (e.g. `read_quarterly_forecasts`, unlike
  `_read_long_combined_forecasts_api`'s try/except at `:3812-3819`).

**Writer guard (quarter branch only, `api_writer.py:1194-1249`)**
- A row whose `valid_from` **and** `valid_to` are both null keeps the synthesized calendar window
  (today's behaviour).
- Otherwise a record is written only if the row's own window, parsed with `local_calendar_date`, matches
  the synthesized calendar window for `(year, quarter_in_year)` exactly — an unparseable but *present*
  value counts as a mismatch (dropped), not as "absent" (`:1231-1247`: `both_present` gates the
  `local_calendar_date` comparison; anything else, including one-sided presence, is `mismatch = True`).
  Parsed via `local_calendar_date`, not a `str(...)[:10]` prefix compare — the latter accepted a garbage
  time-of-day like `"2024-06-30T99:00:00"` and rejected a same-date value in a different format like
  `"2024/06/30"` or `"20240630"` that the reader's own calendar-window check already accepts.
- A calendar `valid_from` with a null `valid_to` is **dropped**. This is an intended change from trunk,
  which writes the row's `valid_from` with a synthesized `valid_to`.
- A record that passes still **writes the synthesized ISO-format dates**, not the row's own (possibly
  differently-formatted) string — the row's own values were only used to verify agreement.
- **Target-year range guard, numeric, both ends** (`:1219-1230`): a row is skipped outright — before any
  parse comparison — if `year > 2261` (matching `filter_calendar_quarter_windows`' own upper cutoff,
  `aggregation.py:256-271`) or if `(year, quarter start month) < (1677, October)` (matching
  `local_calendar_date`'s own `1677-09-22` lower bound: Q1–Q3 of 1677 start before that date; only Q4's
  Oct 1 start clears it). This comparison is **numeric**, not a string comparison — `"999-01-01" <
  "1677-09-22"` is `False` lexicographically (every year with fewer digits than 1677 would otherwise
  misclassify as in-range).
- One aggregated count per call (not per row) at **WARNING**, no station codes (`:1297-1309`). Not
  INFO, unlike the reader-side calendar-window drop counts (which stay at INFO, correctly, since a
  rolling-window drop there is expected on every run by design): a row reaching the writer with a
  non-calendar window means an upstream invariant broke — the readers already filter those out — so it
  is treated as WARNING-worthy. This also matters for visibility: `setup_library` caps the root logger
  at WARNING on import (INFRA-029), so an INFO-level line here would never reach production logs at all.

**Year and date bounds in `read_latest_quarterly_forecasts` (Problem 6)**
- The flag-ON target-year trim admits `end_year + 1`, so a 25 Dec issue yields next year's Q1. The
  issue-date API read (`:3432-3469`) is unchanged.
- Under **both** flags, drop direct rows whose `date` is after `forecast_date`. PP-065 applies the same
  bound to the monthly source.
  - **Placement:** immediately after `direct = _normalize_combined_forecasts(raw_q, "quarter")` (`:3474`)
    and **before** `select_operational_issuances` (`:3493`).
  - Parse quarter `date` with `local_calendar_date` (`src.aggregation`, imported at `:3395-3398`) for the
    mask only, not a bare `pd.to_datetime(..., format="mixed", errors="coerce")`; do not write it back
    (quarter `date` arrives unparsed, `:3902-3903` parses it for season only). A bare mixed-format parse
    raises `AttributeError` on the subsequent `.dt` access when `date` mixes tz-aware and tz-naive
    strings — see `TestRegressionMixedTimezoneIssueDate` below.
  - Rows with a null or unparseable `date` are **kept** and not subject to the bound (today's behaviour).
  - No `date` column → skip the bound.
  - **Drop count logged at INFO** when nonzero (`:3484-3490`), naming whether it may be a back-dated run
    or a flag-OFF row dated at the quarter start.

**First-year Q1 (Problem 7), flag OFF only** (line numbers re-measured on current trunk `6a4ecfae`)
- Read issue years from `start_year − 1` (`:3193-3199`). Keep the `horizon_value` filter unchanged. Do
  **not** mirror flag ON's `_trim_to_target_year_range(..., end_year)` (`:3209`) here — an earlier version
  of this fix did, and an out-of-loop review found it silently reversed direct-source precedence (below).
- **Invariant, revised 2026-09-27 (owner decision, `115eb886`, now merged to trunk in `955bd384` / #527
  — see "Native-only restriction" below for why).** The flag-OFF direct set = trunk's set (every row
  with issue year in `[start_year, end_year]`, any target year) **plus only rows from the schedule-dated
  native issuance**: any prior-year row targeting Q1 of `start_year` whose `date` exactly matches the
  quarter mode's own schedule-computed native issue date (`valid_from` — Jan 1 of `start_year` — minus
  `lead_time` months, on `issue_day`, clamped to that month's length) — the mask (`data_reader.py:3247-3265`,
  re-measured on current trunk `6a4ecfae` — unchanged since `955bd384`; the branch-era citation was
  `:3251-3264`) is evaluated per row
  across the whole `direct` frame, so it admits every matching row,
  across stations and models, not a single row overall. If the schedule cannot be resolved, or resolves
  with `issue_day < 1`, there is **no** exception at all — trunk's set only — and one WARNING is logged.
  Nothing else is added, nothing else is removed.
- **Superseded for LR rows by PP-065 P1b (owner decision R4-native-lr-precedence, 2026-09-28).** The "trunk's set" described
  above (every row with issue year in `[start_year, end_year]`, any target year, widened by this Q1
  exception) is this chunk's own contract for direct rows in general. For **LR rows specifically**,
  PP-065 P1b's native-row rule wins instead, under both flags: only a direct LR row that passes native-row
  selection survives; non-native, backfill-shaped, and null/unparseable-date direct LR rows are dropped
  and counted. See the overview's owner decisions (2026-09-28, decision R4-native-lr-precedence) and PP-065 § "Owner decisions
  this plan implements" (item 9) for the full rule; PP-065's "Target-year trim scope" note narrates the
  same override from PP-065's side. This chunk's own Problem-7 widening (the `start_year − 1` read and the
  native-Q1 exception above) is unaffected in what it **admits** — it still widens which issue years are
  read — but every row it admits must still pass PP-065's native-row rule to survive, for LR models.
- **Native-only restriction (`_quarter_native_q1_issue_date`, `data_reader.py`, `115eb886`).** Before
  2026-09-27 the mask checked only target year `== start_year` and `quarter_in_year == 1` — it did not
  check the issue date at all, so it also admitted any OTHER prior-year row sharing that target quarter,
  whatever its actual issue month or day. A read-only run against the real kghm dev DB found this also
  admitted a **persisted monthly-derived Q1 row backdated to Dec 1** (population (c), "Mechanism" item 5)
  — not the genuine Dec-25 issuance — which shares the `(code, model, year, quarter)` dedup key with the
  real Jan-1 rewrite and, carrying a higher API id, won `drop_duplicates(keep="last")` over it: **21 real
  LR values were nulled and 7 stations lost their Q1-2026 ensembles.** The mask now computes the native
  issue date via `_quarter_native_q1_issue_date(start_year)` and requires an exact match on `date`, not
  merely on target year and quarter. `horizon_value` is still a stored data attribute the API filters on
  (`:3193-3199`), unchanged; `select_operational_issuances`, which validates `date` against the schedule
  under flag ON, still runs only under flag ON (`:3203`), never in this branch — the new date check
  replicates just enough of that validation for the flag-OFF exception. With **on-schedule data** — the
  only kind LTF-015 allows to exist — this still reduces in practice to: kghm lead 1 → the Dec 25 issue of
  `start_year − 1`; tjhm lead 0 → issued Jan 1 of `start_year` itself, already inside
  `[start_year, end_year]`, so the exception is never even exercised for it. Off-schedule issue dates are
  now rejected by the date check itself, not left to the producer's concern (LTF-015) alone.
- **Known limitation (accepted): the widened first-year Q1 can duplicate against its own rewrite.**
  The native Dec-25 Q1 row this widening admits for the *first* requested year, and that same
  quarter's flag-OFF rewrite (population (b), dated at `valid_from` = Jan 1 — see "Mechanism", item 5),
  are two distinct DB rows sharing the same `(code, year, quarter_in_year, model_short)` key. When the
  monthly-derived source (`aggregated`) is empty for that key, `combined = direct` runs with **no**
  dedup at all (the `drop_duplicates(keep="last")` only runs when concatenating with a non-empty
  `aggregated`), so **both rows survive** into the reader's output and are both paired against the same
  observation downstream. This is not a new class of bug: on trunk, without the widening, the exact same
  pair (native row from year `Y − 1`, rewrite from year `Y`) already coexists for every year *after* the
  first one in any multi-year read range — the widening only extends it to the first year too. This is
  the pre-existing **PP-049** (flag-OFF `keep="last"` API-order dependence) / **PP-061** (aggregated
  writer `date = valid_from` key collisions) duplicate class; accepted here, not fixed by this chunk.
- **Known limitation (accepted): the Dec-1 monthly-derived Q1 row still competes with the Jan-1 rewrite
  for target years > `start_year`.** `115eb886`'s date check above only tightens the **first requested
  year's** Q1 exception — the one path that used to admit the Dec-1 row by mistake. It does nothing for
  a target year `Y > start_year`: that year's issue year (`Y − 1`) already satisfies
  `>= start_year` on its own, so the row is kept **unconditionally** by the "every row with issue year
  `>= start_year` is kept" rule below, with no date check at all — the same **PP-049**/**PP-061**
  duplicate class as the bullet above, just not gated behind the widened exception this time. This is
  trunk behaviour, not introduced by this chunk. **PP-065 P1b's native-only LR selection closes it
  properly**, in both readers, under both flags, by selecting the native row directly instead of relying
  on which duplicate happens to win a dedup — see PP-065's Tests list, "Native-row selection (kghm
  shape)" entry (`../high_prio_gi_draft_pp_quarter_derived_models.md`, ~:1170-1173 — re-measured
  2026-09-29 after this round's own edits (round 12c) shifted the file again; earlier drafts' `~:434-436`,
  `~:1117-1119`, `~:1132-1134`, `~:1162-1164`, `~:1165-1168` and `~:1169-1172` had each drifted): "a native row, a
  rewrite (`date = valid_from`) and a persisted derived Dec-1 row for the same LR Q1 → the native row, in
  both readers".
- Drop a row when its issue year is `< start_year` **unless** it is that Q1-of-`start_year` row —
  checked via **all three** of target year `== start_year`, `quarter_in_year == 1`, **and** (since
  2026-09-27, `115eb886`) `date` equalling the schedule-computed native issue date — not target year and
  quarter alone. Checking target year alone (an earlier, round-2 version of this fix) was still too
  permissive: it also kept an out-of-window row targeting some *other* calendar quarter of `start_year`
  (e.g. issued 2024-12-25 targeting Q2 2025, not Q1), which could then beat a same-target monthly-derived
  row — or even an in-window direct row, depending on API order — via `drop_duplicates(keep="last")`
  (round-3 out-of-loop review of the round-2 fix). Checking target year **and** quarter, but not `date`
  (the fix that stood until 2026-09-27), was *also* too permissive: it admitted a persisted
  monthly-derived Q1 row backdated to Dec 1 as well as the genuine native issuance — see "Native-only
  restriction" above.
- Every row with issue year `>= start_year` is kept unconditionally, regardless of target year (trunk's
  own set): a **backfill** row (target year `< start_year`, e.g. a Q4 `start_year − 1` row issued in
  `start_year`, #521-style) and a row whose target year is `> end_year` (e.g. a Dec-`end_year`-issued Q1
  of `end_year + 1`, which must survive so it keeps precedence over a same-target monthly-derived row in
  the later `drop_duplicates(keep="last")` combine).
- A row whose issue `date` is null or unparseable is kept — trunk's API-side year filter could not have
  excluded it by year either.
- Parse the issue year with `local_calendar_date` (`src.aggregation`, imported at `:3072`; see "Helper
  semantics" above for its exact behaviour — a per-value `pd.Timestamp` parse, tz-aware → local
  wall-clock date, out-of-range → `NaT`, never raises — **not** a `str(...)[:10]` prefix slice, an
  earlier round's approach that this replaced because it changed which values parse in both directions
  relative to a `format="mixed"` parse), not a bare `pd.to_datetime(..., format="mixed")`. A `date`
  column mixing tz-aware and tz-naive strings (e.g. `"2025-01-10"` next to
  `"2025-03-25T00:00:00+06:00"`) would make the latter fall back to an object-dtype Series and raise
  `AttributeError` on the subsequent `.dt` access — trunk's plain string comparison never had this
  failure mode. The same helper is used for the Problem-6 issue-date bound in
  `read_latest_quarterly_forecasts`. No other pre-existing date parse (e.g.
  `select_operational_issuances`' own) is touched.
- **Drop count logged at INFO** when nonzero (`:3186-3191`).
- **Missing-column guard.** When `quarter_in_year` or `date` is absent from `direct` (so the mask above
  cannot even run — a year-only check was already shown insufficient, see
  `TestRegressionIssueYearMaskTooPermissive`), skip the mask and log a **WARNING** naming the missing
  column(s) (`:3199-3205`), rather than silently doing nothing.
- Locked by `TestA10FirstYearQ1FlagOff` (first-year Q1 read; lower-bound trim),
  `TestRegressionDirectPrecedenceSurvivesLowerBoundWidening` (next-year Q1 direct row wins over
  monthly-derived), `TestRegressionBackfillPrecedenceSurvivesLowerBoundTrim` (prior-year backfill row
  survives, with and without a competing monthly-derived row),
  `TestUnparseableIssueDateKeptRegardlessOfTargetYear` (null/unparseable issue date kept),
  `TestRegressionIssueYearMaskTooPermissive` (an out-of-window row targeting a *different* quarter of
  `start_year` is dropped, both alone and alongside an in-window direct row, regardless of API order),
  `TestPP064aNativeQ1IssuanceRestriction` (added 2026-09-27, `115eb886`/`18efd261` — the persisted
  monthly-derived Dec-1 row does not clobber the Jan-1 rewrite whether it is null- or real-valued; the
  native issue day is clamped to a short issue month exactly as the producer clamps it, with a
  one-day-off distractor that must lose; an unresolvable schedule, and a resolvable one with
  `issue_day < 1`, each disable the exception entirely and log exactly one WARNING)
  and `TestRegressionMixedTimezoneIssueDate` — precisely: (i) `read_quarterly_forecasts` flag OFF, (ii)
  `read_latest_quarterly_forecasts` flag OFF, and (iii) `read_latest_quarterly_forecasts` flag ON where
  the second, problematic row is dropped by the Problem-6 date bound *before* it reaches
  `select_operational_issuances` — in `tests/test_quarter_calendar_window.py`. A mixed-format batch that
  reaches `select_operational_issuances` itself (either quarterly reader's flag-ON branch, once past the
  Problem-6 bound) still raises there: that function is deliberately unmodified (Contract, above) and is
  PP-066's scope, not this one's.

**Additional test classes added across later review rounds** (all in
`tests/test_quarter_calendar_window.py`, beyond the ones named above and the A-1..A-10 list below):
- `TestS2ReaderWriterYear2262Agreement` — the reader and the writer must agree at the shared
  `datetime64[ns]` upper cutoff (year 2262 Q1 rejected by both).
- `TestRegressionMixedTimezoneValidTo` — a mixed tz-aware/naive **`valid_to`** (not `date`) column, both
  through `read_quarterly_combined_forecasts` and directly through `read_quarterly_forecasts`; verified
  to fail if `local_calendar_date` is swapped back to the deleted `format="mixed"`-based parse.
- `TestRegressionAllNullValidFromColumnDropped` — when every row's `valid_from` is null, the real API
  client's own `dropna(axis=1, how="all")` drops the column entirely before `_normalize_combined_forecasts`
  ever sees it; must return empty, not raise `KeyError`, through `read_quarterly_forecasts` (no
  try/except of its own, unlike the combined path).
- `TestR5Observability` — the three drop-count log lines this plan adds (the flag-OFF issue-year mask's
  INFO count, its missing-column WARNING, and the Problem-6 date bound's INFO count in the latest
  reader) fire exactly once per call, name the count, and never a station code.
- `TestP1LocalCalendarDateParsing` — direct unit coverage of `local_calendar_date` itself: accepted vs.
  rejected values, tz-aware local-wall-clock semantics, the lower-bound cutoff (including the
  normalize-before-cutoff wrap-around guard), the exact-`str` dedup cache (including adversarial
  collision pairs, an unhashable value, a `str` subclass, and an object whose `__str__` raises),
  vectorised-vs-per-value dispatch, and empty/all-null input for every column shape.
- `TestRegressionOutOfRangeDatesDoNotCrash` — every one of the four call sites (`read_quarterly_forecasts`
  and `read_latest_quarterly_forecasts`, both flag states where applicable, plus
  `read_quarterly_combined_forecasts`) survives an out-of-range date without raising.
- `TestA7WriterGuard` grew substantially beyond its original A-7 scope (below): differently-formatted
  same-date values (e.g. `"2024/04/01"` vs. `"2024-04-01"`) are accepted and re-serialized as ISO, not
  rejected as a mismatch; a `valid_to` one month or one year off is dropped even when `valid_from`
  matches; year 2262 Q1 and year 1677 Q3 are rejected even with both stored values null (the numeric
  out-of-range guard fires before any parse comparison); an unpadded small year (e.g. `"999"`) is
  rejected, proving the comparison is numeric, not lexicographic; year 1677 **Q4** (the one quarter at
  the exact lower boundary that clears it) is written.

**Tests (Arrange → Act → Assert, station `19999`)**

Fakes of `_read_long_forecasts_api` (`:1406`) must filter by the requested issue-date years,
`horizon_value` **and `horizon_type`**, as the real call does. The monthly source of both quarter readers
calls the same function with the default `horizon_type="month"` (e.g. `:3423`), and existing fakes ignore
all arguments (`tests/test_lead_aware_latest_readers.py:135`). A fake that ignores its arguments cannot
make A-5, A-9 or A-10 fail on trunk. (A-4 was dropped in rev 3 as redundant with A-1 flag ON; IDs are kept
stable.)

**The A tests must survive PP-065 P1b**, which adds the native-row filter to both readers:
- The new test file has its own config fixture, following `tests/test_quarterly_data_reader.py:30-49`:
  set `ieasyforecast_configuration_path`, `ieasyhydroforecast_ml_long_term_configuration` and
  `ieasyhydroforecast_ml_long_term_supported_modes` (including `quarter`), and write a kghm-shaped
  `quarter.json` with **both** `operational_month_lead_time` (1) and `operational_issue_day` (25). The
  autouse fixture there writes the lead only, which puts P1b into its degraded mode.
- PP-065 P1b lists `tests/test_quarter_calendar_window.py` among the tests it may change.
- **A-1. Reader** (flag ON/OFF × `read_quarterly_forecasts` / `read_latest_quarterly_forecasts`).
  - Input: kghm-like direct rows issued 2024-03-25 (Apr–Jun, 100), 04-25 (May–Jul, 200) and 05-25
    (Jun–Aug, 300).
  - Expect one Q2 row: value 100, `valid_to` 2024-06-30. For flag OFF, do not assert `date`/`horizon_value`
    (`data_reader.py:83-94`).
  - The `read_latest_quarterly_forecasts` variant uses `forecast_date` 2024-06-01 (all three issues lie
    before it).
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
  - Pass the maintenance caller's set, `ensemble_models={"EM", "Skilled Mean", "Naive Mean"}`
    (`postprocessing_maintenance_long_term.py:297-301`); the detector reports gaps per model
    (`src/gap_detector.py:470-482`). This is PP-064 A, before PP-065 changes the set.
  - (i) A calendar quarter whose only EM row is rolling-windowed is reported as a gap after Chunk A:
    assert the row with `model_short == "EM"`.
  - (ii) A quarter whose only raw rows are rolling is not reported as a gap for a calendar quarter it
    does not cover.
- **A-7. Writer.** A non-calendar window, one disagreeing with `(year, quarter_in_year)`, or a calendar
  `valid_from` with null `valid_to` → no API record, one aggregated log line. Both windows null →
  synthesized window, as today. A calendar row → a record identical to today's (hv and date per flag).
  The positive case uses a **non-LR, non-EM** model (e.g. `Naive Mean`): PP-065 P1b makes the writer skip
  LR and EM rows.
- **A-8. Season rows** through `_normalize_combined_forecasts` are unchanged.
- **A-9. Back-dated run, both flags, direct source only.** `forecast_date = 2026-09-25`, kghm-like
  **direct** rows issued 2026-09-25 (Oct–Dec) and 2026-12-25 (Jan–Mar 2027) → `read_latest_quarterly_forecasts`
  returns Q4 2026. Flag OFF fails on trunk; flag ON must fail if the date bound is removed after the year
  bound is widened. `_quarter_api_fake` returns nothing for the monthly source
  (`horizon_type != "quarter"`) by construction, so this test proves the bound only for the direct
  source — it says nothing about the monthly-derived source, which has no such bound under either flag
  (see the Chunk A Goal above); that gap is PP-065's to close, not tested here.
- **A-10. First-year Q1, flag OFF.** A direct row issued 2024-12-25 for 2025-01-01..03-31 at hv1:
  `read_quarterly_forecasts(codes, 2025, 2025)` returns it as Q1 2025
  (`test_december_issued_q1_of_first_year_is_read`). Fails on trunk. A row issued and targeting inside
  2024 (issue year `< start_year`, target year `2024 != start_year`, so not the Q1-of-`start_year`
  exception) is dropped (`test_widened_window_still_trims_target_years_below_start_year`). A row issued
  2025-12-25 for Q1 2026 is **kept, not trimmed** — see the next-year-precedence regression classes
  above, which own that scenario: it must keep precedence over a same-target monthly-derived row. An
  out-of-window row issued 2024-12-25 but targeting a *different* quarter of `start_year` (e.g. Q2 2025,
  not Q1) is dropped even though its target year alone would pass — locked by
  `TestRegressionIssueYearMaskTooPermissive` (both alone and alongside an in-window direct row,
  regardless of API order); checking target year without also checking `quarter_in_year == 1` was
  itself a regression. A `date` column mixing tz-aware and tz-naive issue-date strings must not raise
  through `read_quarterly_forecasts` flag OFF, or through `read_latest_quarterly_forecasts` in either
  flag state (flag ON only once the Problem-6 bound has dropped the problematic row before it would
  reach `select_operational_issuances`) — locked by `TestRegressionMixedTimezoneIssueDate`. A mixed batch
  that reaches `select_operational_issuances` still raises there today; that is PP-066's scope.
  A-10's own 2024-12-25 issue date is the kghm fixture's schedule-computed native issue date (lead 1,
  issue day 25), so A-10 exercises the case where the exception's date check (`115eb886`) matches; the
  case where a target-year+quarter match exists but the date does **not** — the persisted
  monthly-derived Dec-1 row, and the unresolvable/invalid-schedule fallback — is locked separately by
  `TestPP064aNativeQ1IssuanceRestriction` above, not folded into A-10 (added 2026-09-27).

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
  D8; tjhm is handled by decision F (Chunk C detail 3, canonical step 7).

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
- **Rollout mechanism (owner decisions R4-merge-is-deploy, R4-integration-branch, R4-recalc-runs, 2026-09-28 — see the overview's "Rollout and
  communication").** Chunk A (this plan) and FD-029 are both merged to trunk (#527, #528), and **both are
  presumed already deployed** — owner decision R4-merge-is-deploy: Chunk A via Luigi's automatic `:latest` pull whenever the
  Docker Hub digest differs (`apps/pipeline/pipeline_docker.py:296-304`), and FD-029 via the dashboard's own
  daily frontend auto-pull (`bin/daily_update_sapphire_frontend.sh`, run from the 19:00 UTC cron entry) —
  verify per org (image tags, postprocessing/dashboard image creation dates; step 0 below). Because merge
  already means deploy, the remaining work (PP-065 P1b–P1d and Chunk B) is **held on the integration branch
  `integ_quarter_p1b_p2`** (owner decision R4-integration-branch) instead of being gated at a deploy step — there is nothing
  left to gate at deploy time once something is merged. The automatic bimonthly quarterly recalc is
  **allowed to run** in the meantime and is not paused (owner decision R4-recalc-runs). **Open, unverified:** the
  interaction between live Chunk A (postprocessing) and the live FD-029 dashboard, while PP-065 P1b has not
  merged, is exactly the current-state blank-card consequence documented in the overview's EM-interim
  paragraph — check it is understood correctly in the P1b readiness review.
- **Chunk A and PP-065 P1a live (presumed, verify at step 0); P1b–P1d reach servers only at canonical steps
  4-6.** (An earlier draft of this bullet said "Chunk A and PP-065 deployed", which contradicts the
  canonical sequence below — P1b–P1d are not deployed at this point in the runbook, they merge into trunk
  at step 4 and are pulled/verified on each server at step 6.) The precondition this bullet actually needs
  — that the recalc scores against 3-of-3 observations, not the old 2-of-3 rule — is already satisfied by
  P1a alone, which is **presumed live now, per org, conditional on that org's resolved image tag being
  `latest`** (owner decision R4-merge-is-deploy, verify per org at step 0): 2-of-3
  observations against 3-of-3 derived forecasts would bias the scores, and P1a's 3-of-3 observation rule
  prevents that regardless of whether P1b–P1d have reached this server yet.
- **Pre-window step** (before the writer-paused window opens, no code): merge trunk into
  `integ_quarter_p1b_p2`, **record the trunk commit hash just merged**, and run `cd apps &&
  SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` (`run_tests.sh` lives in `apps/`; and
  `forecast_dashboard` if a dashboard-affecting change is also in this window) on that exact tree. CI does
  not test these modules (INFRA-059: `.github/workflows/build_test.yml`'s `test_postprocessing` job,
  `:452-472`, only `uv sync`s and verifies imports, no pytest), so this local run is the only test gate
  before the merge below — and step 4's guard checks trunk `HEAD` against the commit hash recorded here.
- **One writer-paused window** (ops instruction, no code). This is the **canonical rollout sequence: steps
  1-10 inside the writer-paused window, step 11 after it** —
  PP-065 § "P2 — rollout" and the overview's rollout step 3.4 reference this list rather than restating it:
  **Both orgs, one window.** Step 4 below (the `integ_quarter_p1b_p2` → trunk merge, `deploy.pp`) deploys
  P1b to **every** org whose image tag is `latest` at once (R4-merge-is-deploy) — there is no per-org merge
  to stagger. **An org whose tag resolves to `local`** (detail 0's per-org read) is a BLOCKER to resolve
  before this window opens — set that org's `.env` tag to `latest` so step 4's merge reaches it, or plan an
  explicit manual deploy for it; step 4's single merge does not by itself put anything on such an org. So
  steps 1-3 (pause every writer, the pre-deploy DB audit plus PP-065's rule-A triplet count,
  and the export) must each be completed on **both** kghm and tjhm before step 4 runs: pausing and
  auditing/exporting only one org before merging would leave the other org's writers active, and its
  pre-change state uncaptured, the moment the new image auto-pulls there too. Steps 6-10 (pull/verify,
  decision F, the recalc, post-recalc checks, resuming writers) are already written per-org below, run
  inside this same writer-paused window, and none of them may be deferred to a later window for either
  org — **except step 7 (decision F), which is tjhm-only** (kghm has no equivalent provenance-cleanup
  problem; see "B3" above), so kghm has no step-7 action to perform, not a deferred one. **Step 11 (the
  first operational run) runs AFTER step 10 resumes the writers, so it is after the writer-paused window
  closes, not inside it** — it must still happen promptly, on both orgs, in this same rollout pass, just
  not under the pause. **Window placement.** The window must fall between both orgs' LT cron
  days — kghm's 10th and 25th, tjhm's 1st — e.g. days 2-9 or 11-24 of a month; step 0 (detail 0 below)
  confirms each org's configured `operational_issue_day`, and the overview's own rollout step 1 ("crontab
  LT line") confirms the actual cron entry still matches, before relying on either.
  1. **Pause every writer**, not just the LT cron days (kghm 10 and 25; tjhm 1): operational runs, the
     maintenance runs (`apps/pipeline/pipeline_docker.py:1946-1972`; `apps/run_locally.sh:1745-1748`), any
     recalc other than the one at step 8, and manual runs — the automatic bimonthly recalc does not need
     pausing beforehand (owner decision R4-recalc-runs). Wait for running jobs to finish before continuing.
  2. **The read-only pre-deploy DB audit** (detail 2 below) and **PP-065's count of rule-A (same-issue
     monthly triplet) rows per model × quarter** (PP-065 § "P2 — rollout", the "This window follows PP-064
     Chunk C's canonical 'Order' sequence exactly" paragraph, ~:2122-2126 (the rule-A sentence is the
     "pause writers → the pre-deploy DB audit and PP-065's own count of rule-A ..." line, ~:2125) —
     re-measured 2026-09-29 (round 12g), done here, at the audit step,
     not "before the recalc": that heading no longer exists in PP-065 P2).
  3. **Export** (detail 1 below: `pg_dump`/`COPY` of the QUARTER `skill_metrics` and `long_forecasts`
     rows, kept out of the repo) — this is the SAME export PP-065 P2 refers to; state it once here.
  4. **Merge** `integ_quarter_p1b_p2` into trunk — this merge **is** the postprocessing deploy trigger
     (`deploy.pp` in the overview's dependency graph). **Guard: trunk must not have moved since the
     pre-window step's test run.** Before merging, confirm trunk `HEAD` still equals the trunk commit
     recorded when it was merged into `integ_quarter_p1b_p2` at the pre-window step above. If trunk has
     advanced (e.g. another PR landed on `maxat_sapphire_2` in the meantime), merge trunk into
     `integ_quarter_p1b_p2` again and re-run `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh
     postprocessing_forecasts` (and `forecast_dashboard` if applicable) on the updated tree before
     proceeding — this merge gets no CI pytest gate either (see the pre-window step above), so this
     re-run is the only test gate against the newly-merged trunk commits.
  5. **Wait for the CI run on the merge commit to succeed** (`.github/workflows/deploy_production.yml`) —
     the merge builds and pushes the image. **Production CI pushes only the `:latest` tag**
     (`.github/workflows/deploy_production.yml:4`, `env.IMAGE_TAG: latest`, unconditional) — it does not
     build or push any org's separately configured tag. This step does not by itself put anything on a
     server. **If CI fails: keep writers paused, and revert the merge or fix forward before resuming** — see
     "Abort path" below; do not proceed to step 6.
  6. **On each server, pull the new image and verify it**: use the tag as the org's `.env` actually
     resolves it — `read_configuration` (`bin/utils/common_functions.sh:102-112`) sets an unset
     `ieasyhydroforecast_backend_docker_image_tag` / `..._frontend_docker_image_tag` to `local` (with a
     WARNING), never to `latest`, before exporting it — so read the raw `.env` value (or the exported
     shell value after `read_configuration` has run) rather than typing `:latest` or relying on a bare
     shell fallback. `docker pull
     mabesa/sapphire-postprocessing:${ieasyhydroforecast_backend_docker_image_tag:-local}` (and
     `mabesa/sapphire-dashboard:${ieasyhydroforecast_frontend_docker_image_tag:-local}` if this window
     also carries a dashboard-affecting change) — **use the org's configured tag, not a hardcoded
     `:latest`; if it is unset it resolves to `local`, meaning this org does not auto-deploy from CI at
     all** (R4-merge-is-deploy condition 1). Then `docker image inspect
     mabesa/sapphire-postprocessing:${ieasyhydroforecast_backend_docker_image_tag:-local} --format
     '{{.Created}}'` (and digest) and confirm it matches the new build from step 5, not a stale local image
     (detail 0/`PP-064.C.step0`'s own reading may predate this merge — re-verify here). Nothing else pulls
     it inside this window: Luigi only pulls when a task starts
     (`apps/pipeline/pipeline_docker.py:296-304`), and the recalc wrapper
     (`bin/bimonthly_long_term_skill_metrics_recalculation.sh:77-85`) only pulls when no image exists
     locally at all — so without this explicit pull, steps 7-9 below would silently run against the OLD
     image. **If the pull or the verification fails or does not match: keep writers paused, do not
     proceed** — see "Abort path" below.
     - **Pinned tags (both images).** This bullet covers an org whose tag is **explicitly pinned** to a
       real, non-`local` value (e.g. a version pin) — an org whose tag resolves to `local` is handled at
       detail 0 above as a BLOCKER to fix before the window opens, not by retagging: never push a `:local`
       tag to Docker Hub (Luigi would auto-pull it on every `local` deployment,
       `apps/pipeline/pipeline_docker.py:298-304`). Since step 5 only builds and pushes `:latest`, an org whose configured
       tag (`ieasyhydroforecast_backend_docker_image_tag` / `..._frontend_docker_image_tag`) is **not**
       `latest` (e.g. a version pin) has **no new build matching that tag** — the pull in this step returns
       the same old image, and the creation-date/digest check correctly reports no match, but for a
       different reason than a failed or still-propagating pull. **Before proceeding to step 7**, the
       operator must either promote/retag this run's `:latest` build to the org's pinned tag (and push it),
       or abort per the "Abort path" below — do not proceed on a pinned org with an unmatched image.
       **Presumed: the local copies of the org env files set `latest`; to be recorded per org at step 0
       (`PP-064.C.step0`).** **Corrected 2026-09-29 (plan-sync round 12): the repo's own template env file
       DOES carry this override** — `apps/config/.env:136-137` sets both
       `ieasyhydroforecast_backend_docker_image_tag` and `ieasyhydroforecast_frontend_docker_image_tag` to
       `latest` (the earlier "carries no override" claim here was wrong). That file is a demo/template
       config (`ieasyhydroforecast_organization=demo`), not a real kghm/tjhm server `.env`, so it shows
       intent, not what either org's actual deployed `.env` contains — no per-org read confirming the real
       servers match it has been recorded yet, so this is still not verified fact for kghm/tjhm. It is not
       expected to trigger, but confirm at step 0 rather than assume it; it is a guard for the next org
       whose tag is pinned.
     - **The dashboard needs a re-create, not just a pull.** `docker pull` on its own does not restart or
       re-create the running dashboard containers — a pulled image with no re-create keeps serving the OLD
       code. If this window carries a dashboard-affecting change, follow the pull with the same
       stop/pull/recreate sequence `bin/daily_update_sapphire_frontend.sh` uses
       (`:59` `docker compose -f bin/docker-compose-dashboards.yml down`, `:68` the pull already covered
       above, `:76` `start_docker_compose_dashboards`, which runs `docker compose -f sapphire/docker-compose.yml
       up -d` — `bin/utils/common_functions.sh:607-616`) — or run that script directly instead of a bare
       `docker pull`, then still perform this step's own creation-date/digest verification.
  7. **Decision F (tjhm)**, inside the window, before the recalc — detail 3 below.
  8. **Recalc** per org — detail 4 below.
  9. **Post-recalc checks** — detail 5 below.
  10. **Resume the paused writers.**
  11. **Run the first operational quarterly postprocessing run promptly**, and verify it — do not wait for
      the next scheduled LT cron day. Exact command, per org: `bash
      bin/bimonthly_long_term_postprocessing.sh <env_file_path> operational`. This invokes
      `postprocessing_operational_long_term.py`, which has no quarter-only mode — the same run also
      processes monthly and seasonal ensembles. This is what writes the derived seven-model rows'
      Naive Mean ensemble row for the CURRENT quarter, whether or not that quarter is observed, wherever a
      target-quarter key can form a derived-composition Naive Mean — two or more non-null contributors,
      **one of them derived** (Naive Mean itself needs only two or more distinct non-null raw
      contributors, which a plain `LR_Base`+`LR_SM` key already satisfies,
      `ensemble_calculator.py` ~:890-915; a *derived*-composition Naive Mean additionally requires one of
      the seven re-enabled models among those contributors)
      (see the INVESTIGATE outcome below for when none can). **Skilled Mean is a separate, narrower
      condition, not implied by Naive Mean forming:** the same run writes a Skilled Mean row for that key
      only where its own skill gate also passes — see "Skilled Mean is not a required row" below for the
      two conditions. A target-quarter key can form a Naive Mean without forming a Skilled Mean, and gap
      detection keys on Naive Mean only (PP-065 § "Quarterly ensembles = Naive Mean + Skilled Mean only"
      ~:67, "A Skilled Mean that does not form is not a gap"). The in-window
      recalc (step 8) only writes ensembles where its own inner join against observations matches
      (`src/skill_metrics.py` ~:2682-2690, ~:2806-2831) — usually not the current
      quarter, **but not guaranteed never**: a quarter's last month counts as observed at ≥50% of its
      days (`data_reader.py` ~:1301-1302), so a writer-paused window that falls late in that month (e.g.
      kghm Dec 17–24) can make the current quarter "observed" before step 8 runs, and step 8 then writes
      its ensembles too — see PASS criterion 2's loophole note below. Only this operational run's
      quarterly block (`postprocessing_operational_long_term.py` ~:207-232) writes Naive Mean, from
      existing skill plus the latest derived forecasts, with no observation requirement, whenever a
      target-quarter key can form it — and Skilled Mean alongside it wherever that key's own skill gate
      also passes. See the
      overview's "User-visible consequence" paragraph and PP-065 § "P2 — rollout" for the blank-card
      framing this closes.

      **Success criteria (PP-065 § "P2 — rollout" points here rather than restating this; simplified
      2026-09-28, replacing the earlier "rewritten 2026-09-28" version — two review rounds found that its
      per-key read-back rules kept diverging from the real ensemble-formation rules in
      `ensemble_calculator.py`; this version states only what the formation rules actually guarantee).**

      **Scope: "the target quarter"** = the single `(year, quarter_in_year)` that
      `read_latest_quarterly_forecasts` returns for this run's `forecast_date` — operationally, the latest
      quarter for which the API holds forecast rows (or, in the step-3 export, the latest quarter present
      there), not a calendar computation the operator does separately: the reader combines the aggregated
      and direct sources, then keeps only the max `year`/`quarter_in_year` pair (`data_reader.py`
      ~:3618-3623).

      The wrapper's own exit status proves nothing: `run_container`
      (`bin/bimonthly_long_term_postprocessing.sh:102-148`) discards the container's exit code at its own
      two call sites (`:151-162` — the operational block at `:158-161` never captures or checks the
      function's return value); the Python entry point `sys.exit(0)`s successfully before the quarterly
      block whenever there is no monthly skill or no recent monthly forecasts
      (`postprocessing_operational_long_term.py:145-163`); and a failed quarterly API write only logs a
      WARNING, with the caller's return value unchecked (`file_writer.py:853-861`, called from
      `postprocessing_operational_long_term.py:227`).

      **INFO lines from this entry point are not a usable check — do not require their presence or their
      absence.** `postprocessing_operational_long_term.py` imports `setup_library` (`:24`) before its own
      `logging.basicConfig(level=logging.DEBUG)` (`:36`); `setup_library.py:44` already calls
      `logging.basicConfig(level=logging.WARNING)` at import time, and `basicConfig()` is a no-op once the
      root logger already has handlers, so the later call never raises the level — the root logger for this
      entry point is capped at WARNING (INFRA-029,
      [`high_prio_gi_draft_infra_setup_library_root_logger_caps_info.md`](high_prio_gi_draft_infra_setup_library_root_logger_caps_info.md),
      still Draft). `"Quarterly ensembles saved."` (`:228`), `"Quarterly forecasts written to API
      successfully."` (`file_writer.py:855`) and the INFO skip messages (`:230`, `:232`) are all
      `logger.info` and so **never reach the log** for this entry point today; neither their presence nor
      their absence proves anything until INFRA-029 lands.

      **Skilled Mean is not a required row.** It legitimately does not form unless at least two models
      both (a) pass `filter_for_highly_skilled_forecasts` with the long-term NSE>0 override and the
      quarter min-pairs floor K (`skill_metrics.py` `_long_term_threshold_overrides` ~:168-193,
      `_long_term_min_pairs("QUARTER")` ~:208-227, default K = 5, overridable via
      `ieasyhydroforecast_min_pairs_long_term_quarter`; decision C sets K = 10) and (b) survive the
      subsequent inner merge against non-null current-quarter forecasts (`ensemble_calculator.py`
      `_add_skilled_mean_aggregated_ens` ~:802-879, `how="inner"` at `:834` then
      `dropna(subset=["forecasted_discharge"])` at `:836`). Requiring it unconditionally would fail valid
      runs — its absence is informational, not a failure (see below).

      **Step 11 outcomes, per org (round 12c: replaces the earlier three-outcome "criterion 2 not
      applicable" framing with a simpler rule: the recalc and step 11 use different readers and trims, so
      a per-key applicability test cannot be made exact; the operator investigates instead).**
      - **PASS** = criterion 1 (below) and criterion 2 (below) both hold.
      - **PASS (pre-satisfied)** = the existing late-quarter branch below: the derived-composition Naive
        Mean already exists after step 9 → criterion 1 alone. See the "When the target quarter already
        counts as observed …" bullet under criterion 2 for the rule and its documented limitation.
      - **INVESTIGATE** = criterion 1 holds but criterion 2 is not met. This is NOT an automatic FAIL. The
        operator determines and records either a defect, or the reason no key could form a
        derived-composition Naive Mean (e.g. no target-quarter key with two or more contributors, one of
        them derived; Naive Mean needs two or more distinct raw `model_short` values,
        `ensemble_calculator.py` ~:887-915). The rollout is complete only after the investigation is
        recorded.
      - **FAIL** = criterion 1 fails.

      1. **Criterion 1: log checks, WARNING level, across two named files.**
         - **Wrapper log** (`${LOG_DIR}/run_${TIMESTAMP}.log` — `log_file`,
           `bin/bimonthly_long_term_postprocessing.sh:57`; every `log_message` line is `tee -a`'d there,
           `:61`): the wrapper's own `log_message` lines are shell output, not Python logging, and are
           always present regardless of the root logger's level: require `"postprc-lt-operational
           completed successfully"` (`:138`); require that `"WARNING: postprc-lt-operational completed
           with exit code: "` (`:140`) is absent.
         - **Per-container service log** (`${LOG_DIR}/postprc-lt-operational_${TIMESTAMP}.log` —
           `SERVICE_LOG`, `:108`; the container's stdout/stderr is `tee`'d there, `:133`; the wrapper
           prints its path as `"  Service log: $SERVICE_LOG"`, `:111`): require that the WARNING-level
           early-exit and failure strings are absent — each verified below as `logger.warning` in the
           source, so each WOULD reach this WARNING-capped file if it fired:
           `"No monthly skill metrics available. Run recalculate_skill_metrics.py or maintenance first.
           Exiting."` (`postprocessing_operational_long_term.py` `logger.warning` at `:146-150`, followed
           by `sys.exit(0)` at `:151`); `"No recent monthly forecasts available. Exiting."`
           (`logger.warning` at `:162`, `sys.exit(0)` at `:163`); `"Quarterly forecasts API write returned
           False (disabled, unavailable, or failed)."` (`file_writer.py:858`, `logger.warning`, called
           from `:227` — capital "Quarterly", not "quarterly"); `"No quarterly skill metrics available"`
           (`data_reader.py:2837`, `logger.warning`, `read_quarterly_skill_metrics`); `"No quarterly
           forecast data available"` (`data_reader.py:3588`, `logger.warning`,
           `read_quarterly_forecasts`/`read_latest_quarterly_forecasts`). Each is quoted verbatim from its
           source, including case; the match against this log file is case-sensitive, so none of these
           five strings, in this exact case, may appear anywhere in it.
      2. **Criterion 2: at least one Naive Mean row for the target quarter whose `composition` includes at least one
         `QUARTERLY_DERIVED_MODELS` member** (`src/model_names.py:22-24`), per org (aggregate counts only,
         no station codes in the plan or the PR).
         - **Why this is the one required data check.** Naive Mean's formation rule
           (`ensemble_calculator.py:33-41`, applied at `:915` for quarter/season) requires only **two
           distinct raw `model_short` values** contributing a non-null `forecasted_discharge` at a
           `(year, quarter_in_year, code[, horizon_value])` key (`is_multi_model_composition` — the
           composition string contains a comma) — no skill gate, unlike Skilled Mean. Pre-P1b,
           `AGGREGATED_EM_RAW_MODELS = {LR_BASE, LR_SM}` (`src/model_names.py:14`) are the only two
           non-baseline models the pipeline reads for quarter, so a Naive Mean row with composition
           `"LR_Base, LR_SM"` can already exist today, independent of P1b. What P1b changes is *which*
           models the composition can draw from: the seven `QUARTERLY_DERIVED_MODELS`
           (`src/model_names.py:22-24`, landed in P1a) become readable contributors once P1b's reader
           change lands, so a target-quarter Naive Mean whose composition includes one of them is
           observable proof that this run actually used the new derivation path, not an inference from
           logs.
         - `composition` is a stored, API-readable column on `long_forecasts`
           (`sapphire/services/postprocessing/app/models.py:165`, `LongForecast.composition`), returned by
           `client.read_long_term_forecasts()` (`sapphire_api_client`'s `long_term.py:75`) and passed
           through unmodified by this app's own normalizers (`data_reader.py`'s `_read_long_forecasts_api`
           and `_normalize_combined_forecasts` neither drop nor rename it) — read it directly at the
           target-quarter keys, not via a row-count substitute.
         - **When the target quarter already counts as observed before step 11 runs, the blank-card
           recovery goal is already met by the recalc — criterion 2 is pre-satisfied.** A quarter's last
           month counts as observed at ≥50% of its days (`data_reader.py` ~:1301-1302); a writer-paused
           window that falls late in that month (e.g. kghm Dec 17–24) can make the target quarter
           "observed" before step 11 runs, so step 8's own recalc can already write a target-quarter
           Naive Mean row with a derived composition (its empty-return paths log no WARNING either:
           `read_latest_quarterly_forecasts`'s two silent `if combined.empty: return ...` branches,
           `data_reader.py` ~:3603-3604 and ~:3607-3608, and the resulting skip at
           `postprocessing_operational_long_term.py:230`/`:232` is `logger.info`, not `logger.warning`,
           so criterion 1 above would not catch a silently-skipped step 11 either). **In this branch, PASS
           proves only that the blank-card recovery goal is already met — it does NOT prove that step 11's
           own quarterly block executed on this run.** The block can no-op end to end this run (e.g. its
           quarter skill frame is tombstone-only: an INFO-level `"Read 0 quarterly skill metric rows from
           API"` followed by an INFO-only skip, nothing at WARNING) and PASS would still be granted from
           the pre-existing row alone. Step 9's precondition checks (i) and (ii) (detail 5 below) confirm
           only that the block's two required inputs exist in the DB after the recalc — by themselves they
           do not distinguish this pre-satisfied case from step 11's own quarterly block actually running
           (detail 5 says so explicitly) — and PP-064 B's B5 is the standing contract for the block's
           behaviour when they fail — see detail 5 below for both.
           **Step 9 (detail 5 below) must record whether this row already exists before step 11 runs.**
           If it does, PASS requires only criterion 1 (the wrapper and WARNING-level log checks) above;
           criterion 2 is recorded as "pre-satisfied at step 9" rather than re-evaluated against step
           11's own run. The postprocessing service's own log (read-only, colleague-managed) showing a
           fresh `"Created long forecast: …"` / `"Updated long forecast: …"` line
           (`sapphire/services/postprocessing/app/crud.py:137,146`, `logger.info`, `create_long_forecast`)
           for a Naive Mean row at the target quarter's key, timestamped **after** step 10 (resume
           writers), is OPTIONAL corroboration only that step 11 itself also touched that row — an
           unchanged row logs only `"Skipped unchanged long forecast: …"` at DEBUG (`crud.py:139`),
           invisible at the service's default INFO level (`app/logger.py:8`, `settings.log_level`), so
           its absence does not fail a valid run.
           If no such row exists after step 9, the outcome is determined at step 11 as stated in "Step 11
           outcomes, per org" above: **PASS** if criteria 1 and 2 both hold; **INVESTIGATE** if criterion 1
           holds but criterion 2 does not (not an automatic FAIL — see that outcome's own text for what the
           operator records); **FAIL** if criterion 1 fails.

      **Informational, not pass/fail** (a lead for investigation, not a required outcome — do not gate the
      run's success on either of these):
      - The count of target-quarter keys where the derived seven-model rows are present (step 9, detail
        5's own check) but the key's Naive Mean `composition` is still `LR_Base`/`LR_SM`-only. A non-zero
        count is worth investigating (e.g. a derived model's forecast was null at that key), not a failure.
      - Skilled Mean's own presence and `composition` at the target quarter. Its absence is not a
        failure — see "Skilled Mean is not a required row" above; where it is present, the same
        composition check applies (does it include a `QUARTERLY_DERIVED_MODELS` member).

      Aside from these two checks, this success criteria list intentionally drops the earlier per-key
      "recalc (step 8) writes no current-quarter ensemble rows" cross-check: it restated
      `_calculate_aggregated_skill_metrics`'s inner-join behaviour (`skill_metrics.py` ~:2682-2690,
      ~:2807-2832) rather than checking this run's own output, and added a second query without changing
      the pass/fail outcome. **Not the same as** criterion 2's step-9 recording above, which is a single
      aggregate boolean (recorded once at step 9, before step 11 runs) that determines whether the
      recovery goal is already met — the step-9 record only decides whether the pre-satisfied branch
      applies; PASS / INVESTIGATE / FAIL are decided by step 11's own criteria (four outcomes in total,
      listed above) precisely in the case the dropped cross-check never distinguished (a pre-existing
      row from step 8 vs. a fresh one from step 11), so it stays.

      **Supporting context, not a separate check:**
      - **No EM row is written for the target quarter by this run.** PP-065 P1b's writer change stops the
        quarter EM write (`high_prio_gi_draft_pp_quarter_derived_models.md` item 3, "Writer: stop writing
        raw LR rows" — the same change also skips `EM`/`ENSEMBLE_MEAN` rows). Any quarter `EM` row a
        read-back turns up after this run predates P1b's deploy (or is from an old image still running
        somewhere), not something this run wrote. FD-029 hides these rows on the dashboard card, so their
        presence is only visible via a direct DB/API read-back, never the card.
      - **Optional supporting evidence, read-only:** the postprocessing service's own `"Created long
        forecast: …"` / `"Updated long forecast: …"` log lines (`sapphire/services/postprocessing/app/
        crud.py:137, 146`) around the time of this run. Corroborating only, not required — the service is
        colleague-managed and this plan does not gate on its log format.

      **The derived seven-model rows for the target quarter are not this step's check — move it to step
      9.** They are written by the in-window recalc itself (step 8): `joint_forecasts = forecasts.copy()`
      in `_calculate_aggregated_skill_metrics` (`skill_metrics.py` ~:2741-2742) passes every raw forecast
      row through regardless of whether it joined an observation, so the target quarter's raw rows survive
      that pass-through even though no ensemble is computed for them there. See detail 5 below (canonical
      step 9), not this step — same scope, "the target quarter" as defined above, for consistency.

      **Never run this command concurrently** — `run_container` (`bin/bimonthly_long_term_postprocessing.sh:113-117`)
      removes any existing container with the same fixed name (`docker rm -f postprc-lt-operational`) before
      starting; a second, overlapping invocation (another operator, or an overlapping cron/maintenance run)
      would kill this run's still-in-progress container. Note also that this wrapper is marked "kept on
      origin for manual / debugging use only" in the deployment checklist
      (`doc/prod/update_deployment_checklist.md:846-847`), superseded for the normal schedule by
      `run_periodic_maintenance.sh long_term`; using it here, for a one-off out-of-band run right after the
      writer-paused window, is deliberate, not an oversight.

  **Abort path.** If CI (step 5) or the pull/verify step (step 6) fails after the merge: keep writers
  paused, and revert the merge or fix forward before resuming — do not leave a merged-but-unverified
  `integ_quarter_p1b_p2` sitting on trunk across the window boundary. Otherwise the next unrelated trunk
  merge, or the monthly `scheduled_security_rebuild` workflow
  (`.github/workflows/scheduled_security_rebuild.yml`, cron `0 0 1 * *`; verify), rebuilds and publishes a
  P1b image at the configured tag regardless of this window's own CI/pull outcome, which Luigi then
  auto-pulls **outside any writer-paused window**.

  **The revert-of-a-merge trap (default abort action: FIX FORWARD, not revert).** Reverting the
  `integ_quarter_p1b_p2` → trunk merge on trunk is not a neutral undo. The overview's own
  R4-integration-branch rule requires periodically merging trunk INTO `integ_quarter_p1b_p2` to keep it
  current; the next such sync after a revert on trunk carries that revert commit into
  `integ_quarter_p1b_p2` too, and since the branch's own tip still has P1b–P1d/PP-064 B unreverted, the
  sync applies the revert's diff there as well — **silently removing P1b–P1d/PP-064 B from the
  integration branch**, with its tests staying green because the code under test was removed along with
  everything else. Re-merging `integ_quarter_p1b_p2` into trunk afterwards does not undo this: trunk
  already contains those commits as ancestors (from the original merge), so git treats them as already
  merged and the second merge applies no new diff — the content stays gone from both branches. **FIX
  FORWARD is therefore the default abort action** (writers stay paused; do not revert). If a revert of the
  `integ_quarter_p1b_p2` → trunk merge (commit R on trunk) is genuinely unavoidable, it carries a
  mandatory follow-up, in this order — R only enters `integ_quarter_p1b_p2` through the next
  trunk → integ sync, so the revert-of-R cannot happen before that sync:
  1. Do the trunk → integ sync (the one the overview's R4-integration-branch rule requires periodically),
     which brings R into `integ_quarter_p1b_p2` as an ancestor.
  2. **Immediately** `git revert R` on `integ_quarter_p1b_p2`, before running or trusting any test on that
     branch. This restores the P1b–P1d/PP-064 B content, and because R is now an ancestor of
     `integ_quarter_p1b_p2`, later syncs do not re-apply it.
  3. Verify the feature content is back: `git diff <pre-merge-integ-tip> integ_quarter_p1b_p2 --
     apps/postprocessing_forecasts` shows only trunk changes made since that tip, and no removal of the
     P1b–P1d changes — name concrete files or symbols to spot-check (e.g. the reader's use of
     `derive_quarterly_from_monthly_same_issue`).

  There is no "re-create the branch from its pre-merge tip" alternative: every later trunk → integ sync
  brings R in again regardless, so a re-created branch would need this same revert-of-R follow-up anyway.
  This is the canonical text for this hazard; the overview's R4-integration-branch decision points here
  rather than restating it.

**Details, keyed to the canonical steps above by number** (these are reference detail, not a second,
competing order — "step N" above is the canonical sequence; "detail N" below is this list):

0. **Server state read per org** (read-only): `SAPPHIRE_SKILL_LEAD_AWARE`,
   `ieasyhydroforecast_ml_long_term_supported_modes` and `ieasyhydroforecast_min_pairs_long_term_quarter`
   in the env the postprocessing **and** dashboard containers actually load. Record them in the PR; the
   recalc runs with that state. Also confirm the `quarter` config carries both
   `operational_month_lead_time` and `operational_issue_day`: without the issue day,
   `operational_schedule_for_mode("quarter")` raises (`long_term_horizon_resolver.py:138-142`). Under flag
   OFF, PP-065 then skips the derivation and the native-row filter with one WARNING; under flag ON the
   quarter readers raise, as on trunk (`long_term_horizon_resolver.py:84-111` notes taj-style configs that
   omit it). **R4-merge-is-deploy's per-org verification (2026-09-28):** record all three conditions that
   decision depends on (overview § R4-merge-is-deploy, ~:164-185), not only the image dates: (1) **the
   tag value** — read the org's actual configured tag from its `.env` file (or via `read_configuration`,
   `bin/utils/common_functions.sh:102-112`, which resolves an unset tag to `local` with a WARNING, never
   to `latest`); an org whose `.env` never sets these variables auto-pulls nothing from trunk and this
   whole verification reduces to "still running `local`". **If it resolves to `local` for an org, record
   this as a BLOCKER to resolve before the writer-paused window opens**: either set that org's `.env` tag
   to `latest`, or plan an explicit manual deploy for that org. Never push a `:local` tag to Docker Hub to
   work around this — Luigi on every `local`-tagged deployment would auto-pull it as soon as one exists
   (`apps/pipeline/pipeline_docker.py:298-304`), turning every such org into an unintended auto-deploy
   target; the pinned-tag retag guidance at canonical step 6 below applies only to explicitly pinned,
   non-`local` tags, not to this case; (2) **whether `validate_dashboard_origins`
   passes** for this org (`bin/daily_update_sapphire_frontend.sh:55`) — e.g. the last
   `daily_update_sapphire_frontend` log shows the pull actually ran, not an early `exit 1` — **and inspect
   the RUNNING dashboard container itself** (`docker ps` / `docker inspect <container> --format
   '{{.Image}}'`, compared against the pulled image ID): `daily_update_sapphire_frontend.sh` backgrounds
   the compose recreate and never checks its result (`:68-80`, `start_docker_compose_dashboards`,
   `bin/utils/common_functions.sh:606-616`, only `wait`s on the PID without checking its exit status), so a
   passing log and a fresh pull do not by themselves prove the running container is the new image; and (3) the
   postprocessing/dashboard image creation dates (`docker image inspect
   mabesa/sapphire-postprocessing:${ieasyhydroforecast_backend_docker_image_tag:-local} --format
   '{{.Created}}'`, same for `sapphire-dashboard` with `ieasyhydroforecast_frontend_docker_image_tag` —
   use the org's *configured* tag, not a hardcoded `:latest`), to confirm Chunk A and FD-029 are actually
   live. The automatic bimonthly QUARTERLY recalc is allowed to run and does not need pausing (owner
   decision R4-recalc-runs, 2026-09-28).
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
   - **The predicate covers any year.** LTF-014 P0b is moot while P0 is deferred, so F again owns the
     pre-existing tjhm rows dated 2026-10-01 and any 2027-01-01 rows trunk writes before `deploy.pp`.
     - Evidence: the local DB has tjhm QUARTER rows dated 2026-10-01 at hv0, flag 0 (LR_BASE 4, LR_SM 4,
       plus EM / Naive Mean / Skilled Mean). They pass the native rule and would suppress the fallback.
     - Test (PP-065 P1d): once the aggregate-only Oct-1 LR population is cleared, fallback LR feeds the
       ensembles and no LR row is written or displayed.
     - If P0b ever runs, its recovered flag-1 LR rows are genuine: put them in F's preserve manifest.
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
   - **Two precondition checks, run per org as part of this recalc's own post-checks.** (i) and (ii)
     establish that the quarterly block's inputs exist in the DB after this recalc — they do not, by
     themselves, prove step 11's block ran on a later operational run: canonical step 11's pre-satisfied
     branch (its PASS criterion 2 note) can pass from an already-existing row without step 11's own
     quarterly block ever having executed on that run; (i) and (ii) make the two required inputs
     observable, not stand in for step 11's own PASS criteria.
     - **(i) At least one NON-tombstone quarter skill row per org.** A tombstone is `n_pairs == 0` (or
       NULL) with every metric column NULL, upserted by the write-side to mark a stale long-horizon skill
       key; a legitimate row always has `n_pairs >= K` (`_drop_tombstone_rows`, `src/data_reader.py:107-120`
       — `n_pairs.notna() & (n_pairs > 0)` is the exact separator). The per-org quarter skill frame, read
       as the operational run itself reads it (tombstones dropped by `read_quarterly_skill_metrics`,
       `src/data_reader.py:2832-2838`), must be **non-empty**. If it is not — including the tombstone-only
       case, where the API returns rows but every one is a tombstone, `_drop_tombstone_rows` strips all of
       them, and the read logs `"Read 0 quarterly skill metric rows from API"` at INFO
       (`data_reader.py:2835`) — the operational run's own `if not quarterly_skill.empty:` gate
       (`postprocessing_operational_long_term.py:210`) takes the INFO-only skip branch (`:232`): every
       quarterly ensemble is skipped (Problem 8), silently, with nothing at WARNING level to catch it. If
       K = 10 leaves an org with no quarter skill at all, B5 means no Naive Mean either: **escalate to the
       owner before the hydromet notice** goes out.
     - **(ii) Target-quarter forecast rows are present.** This check is a post-save read-back — per org,
       via the API/DB — of the target-quarter rows this recalc just persisted
       (`file_writer.save_quarterly_forecast_data(quarterly_joint)`, `recalculate_skill_metrics.py:403`).
       It is NOT the `read_quarterly_forecasts` call at `recalculate_skill_metrics.py:386`: that call is
       this recalc's own INPUT read, made before the save, against the DB's prior state, not this run's
       output. It is also a different reader than the one step 11's block calls at runtime
       (`read_latest_quarterly_forecasts`, `postprocessing_operational_long_term.py:211`); that reader has
       its own silent empty-return branches (`data_reader.py` ~:3603-3608). So (ii) confirms the rows exist
       in the DB after this recalc's write, not that step 11's own read of them would succeed. See "The
       derived seven-model rows for the target quarter are present, per org" two bullets below for how
       this recalc writes them and what "the target quarter" means; the block's other required input,
       alongside (i).
   - Freshly written QUARTER rows contain no rolling windows, no LR rows and no EM rows.
   - **The derived seven-model rows for the target quarter are present, per org** (moved here from step
     11, 2026-09-28: this recalc, not the operational run, writes them; "the target quarter" is the same
     scope canonical step 11 defines — the single `(year, quarter_in_year)` `read_latest_quarterly_forecasts`
     returns for this run's `forecast_date`). `joint_forecasts = forecasts.copy()` in
     `_calculate_aggregated_skill_metrics` (`skill_metrics.py` ~:2741-2742) passes every raw forecast row
     through regardless of whether it joined an observation, so the target quarter's raw rows survive this
     recalc's pass-through — independently of whether a target-quarter ensemble also happens to be
     computed here (usually not, but see the next bullet). This is precondition check (ii) above.
   - **B5 documents the mechanism to check first when the quarterly block goes quiet.**
     `validate_pipeline.py`'s "Quarterly skill metrics" presence check (`~:584-590`, via
     `check_presence`'s empty-check at `~:356-358`, default `warn_if_empty=False`) FAILs when the quarter
     skill API returns literally zero rows — that one case is caught. It does NOT catch the tombstone-only
     / all-suppressed case (the API returns rows, but every one is a tombstone, so the operational run's
     own `_drop_tombstone_rows` still empties the frame downstream) or the "No recent quarterly forecasts"
     INFO-only skip (`postprocessing_operational_long_term.py:230`/`:232`) — for those, and for the
     tombstone-only case once it reaches the operational run, no runtime detector exists. B5 is the
     standing contract for what the block does when input (i) fails — an empty (post-tombstone-drop)
     quarter skill frame skips every quarterly ensemble, exactly as an empty monthly skill frame skips
     every monthly one, logged at INFO and not surfaced anywhere else. A quarterly block silently going
     quiet in some later, unrelated run is this documented failure mode recurring, not a new class of bug;
     look there first rather than re-deriving the mechanism — but nothing in the code raises an alert on
     its own.
   - **Record whether a target-quarter Naive Mean row with a `QUARTERLY_DERIVED_MODELS` composition
     already exists after this recalc.** This happens whenever the target quarter already counts as
     observed before step 11 runs — the ≥50%-days-per-month rule (`data_reader.py` ~:1301-1302) can make
     a quarter's last month, and so the quarter, "observed" if the writer-paused window falls late in
     that month (e.g. kghm Dec 17–24), and the recalc's own observation join (`skill_metrics.py`
     ~:2682-2690, ~:2806-2831) then writes its ensembles here rather than never. If this row is already
     present, canonical step 11's PASS criterion 2 is already pre-satisfied and criterion 1 (the wrapper
     and WARNING-level log checks) alone suffices — this is the record that determination reads. The
     service-log Created/Updated evidence canonical step 11 describes is optional corroboration only, not
     required for PASS.
   - A persisted-derived-row round trip (write → read) yields the native-rule-selected row.
   - Spot check the #521 station privately (its code is never written to the repo). A plausible value is
     a spot check, not proof.
   - **Skill values change sharply** (e.g. flag-OFF kghm median ~0.8–0.9 → ~0.2–0.5). The user-facing
     note is the overview's release-note step; it is not written here.
   - **While LTF-014 P0 is deferred (no end date)**, LR for kghm Q1 **and** tjhm Q1/Q4 (after decision F)
     comes from decision G's fallback-derived rows, because neither native issues nor native hindcasts
     exist for those quarters. It changes again once P0/P2 land and the fallback is removed.
   - PP-065 P2 adds its own counts (stale ensemble rows, unfillable gaps).
6. **What the recalc leaves behind:**
   - **Rolling-windowed rows (raw and EM), inert after Chunk A.** Local count (dev DB, 2026-09-27):
     31,282 kghm rolling-window quarter rows. Chunk A (this plan) and FD-029 stop *reading and writing*
     them from their own deploy onward (Mechanism item 1; FD-029 "Problem 2") — neither deletes any row.
     Their removal is owned by decision F (the tjhm-specific provenance predicate, detail 3 above,
     canonical step 7) for the
     population it covers, and by D8 / PP-041 (the stale-rows decision) for the rest; it is not automatic
     on deploy.
   - Old EM, Naive Mean and Skilled Mean rows at keys the recalc no longer emits (accepted, round-2
     decision 2; D8 / PP-041). Under flag OFF, rows with the same key as a fresh row are overwritten.
7. **Rollback (flag ON → OFF) must remove the ensemble twins too, not only LR's.** FD-029's dedup
   (`../mid_prio_gi_draft_fd_quarter_card_calendar_window.md`, item 5, "Known limitation (accepted):
   rollback from flag ON to OFF") documents that a flag-ON native-shaped row keeps outranking a
   flag-OFF rewrite at the same `(code, model_short, year, quarter_in_year)` key until the old row is
   deleted, because `date` is part of the natural key so the two rows never collide — and that this
   applies to EM/Naive Mean/Skilled Mean rows exactly as it does to LR, since `ensemble_calculator.py`
   carries the member's `date` through as a per-column `"first"` aggregation spec — not a bare
   `agg("first")` call, which does not exist: EM and Naive Mean pass a dict entry
   (`em_agg[dcol] = "first"` / `naive_agg[dcol] = "first"`) into `.agg(dict)` in
   `_create_aggregated_ensemble_forecasts` (`:758`) and `_add_naive_mean_aggregated_ens` (`:906`)
   respectively, while Skilled Mean passes a named-aggregation tuple (`sm_agg[dcol] = (dcol, "first")`)
   into `.agg(**dict)` in `_add_skilled_mean_aggregated_ens` (`:865`) — and `api_writer.py`'s
   `record_date` logic (`:1264-1269`) stamps that `date` for any `model_short` under flag ON. If a flag
   is ever rolled back to OFF on a deployed org, the rollback runbook must delete the stale flag-ON
   native-shaped LR **and** ensemble rows for the affected quarters (not just LR's), or they keep
   winning the dashboard's dedup indefinitely. No code change here — this is a rollout/runbook step, not
   a Chunk A/B behaviour.
   - **Skill needs the same rollback step (added 2026-09-27).** FD-029's rollback caveat also covers
     quarter skill (`../mid_prio_gi_draft_fd_quarter_card_calendar_window.md`, ~:298-307, "The rollback
     caveat extends to skill, not only to forecast rows"): a quarter skill row written during the flag-ON
     era at the hv-0 sentinel holds genuine lead-0 skill, not a rewrite, so it keeps winning the
     dashboard's flag-OFF selection's first preference (hv-0 over the configured-lead fallback) after the
     rollback. On a **kghm** (lead 1) org this shows lead-0 accuracy against a lead-1 forecast until it is
     overwritten. **The rollback runbook must therefore also run a flag-OFF quarter skill recalc per
     org**, so hv-0 is rewritten with genuine flag-OFF (configured-lead) skill — until that recalc runs,
     the affected org's card shows the flag-ON-era lead-0 skill. No code change here either.

## Out of scope

- Re-enabling the seven models for quarter, native-row selection, and quarterly Naive/Skilled Mean
  without EM (all PP-065, decision B and round-2 decision 1).
- Dashboard (FD-029/FD-030). Schedule and target construction (LTF-014). Monthly label fixes (LTF-016).
- Deleting DB rows beyond decision F (the stale-rows decision).
- Skill/ensemble-level duplicate window guards. Their inputs come only from the guarded readers or from
  derived rows that are calendar by construction, so they would add no protection.
