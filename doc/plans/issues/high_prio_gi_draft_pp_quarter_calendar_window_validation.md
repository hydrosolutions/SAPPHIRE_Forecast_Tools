# PP-064: Score and ensemble only exact calendar-quarter windows, and carry a prior-year-issued Q1 through

**Status**: Draft (2026-09-26, rev 6 after the fourth review round)
**Module**: `apps/postprocessing_forecasts`
**Priority**: High.
- Stored quarterly skill is wrong today: a rolling window is scored against a different quarter's
  observations. With `SAPPHIRE_SKILL_LEAD_AWARE=true` the wrong window is picked deterministically
  (#521: NSE ≈ −75 → +0.1 after correction).
- Chunk A must be deployed **before 2026-12-25**, as the prerequisite of PP-065. LTF-014 P0 is deferred
  (configs stay `forecast_months [3..9]`), so no native kghm LR row is issued on Dec 25. The first kghm
  Q1 comes from PP-065's derived path (the seven models plus decision G's LR fallback), which needs the
  same next-year target (PP-065 item 2); under flag OFF its derived and ensemble rows are persisted
  dated 2027-01-01. Problem 6 and A-5 remain as the mechanism for direct Dec-25 rows.

**Labels**: `postprocessing_forecasts`, `skill-metrics`, `long-term`, `quarter`
**Overview**: [`../quarter_calendar_product_plan.md`](../quarter_calendar_product_plan.md). The dependency
graph lives there only. Owner decisions of 2026-09-26 are cited by letter (A–H), round 2 by number
("round-2 decision 1–6").
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
  - Chunk A: any prior-year row targeting Q1 of `start_year` at the configured `horizon_value` is read,
    whatever its issue month or day (Dec 25 for kghm with on-schedule data; see the "First-year Q1
    (Problem 7)" section below for why the mask does not check the issue date);
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
- Under flag OFF, a prior-year row targeting Q1 of `start_year` at the configured `horizon_value` is
  read, whatever its issue month or day (Dec 25 for kghm with on-schedule data).

**Files (only these may be modified; final shape, branch `fix_pp_quarter_calendar_window`)**:
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

**First-year Q1 (Problem 7), flag OFF only**
- Read issue years from `start_year − 1` (`:3132-3138`). Keep the `horizon_value` filter unchanged. Do
  **not** mirror flag ON's `_trim_to_target_year_range(..., end_year)` (`:3148`) here — an earlier version
  of this fix did, and an out-of-loop review found it silently reversed direct-source precedence (below).
- **Invariant:** the flag-OFF direct set = trunk's set (every row with issue year in
  `[start_year, end_year]`, any target year) **plus** any prior-year row targeting Q1 of `start_year`
  that is stored at the configured `horizon_value` (`quarter_horizon_value()`), **whatever its actual
  issue month or day**. Nothing else is added, nothing else is removed.
- **The mask does not check the issue date at all, by design.** `horizon_value` is a stored data
  attribute the API filters on (`:3132-3138`) — not a schedule check against the row's own `date`; the
  code never verifies that a prior-year Q1 row was genuinely issued on the org's configured issue day.
  The mask itself (`:3173-3181`) checks only target year `== start_year` and `quarter_in_year == 1`;
  `select_operational_issuances`, which *would* validate `date` against the schedule, runs only under
  flag ON (`:3141`), never in this branch. With **on-schedule data** — the only kind LTF-015 (a
  producer-side change, refuses an early run that falls in a different calendar month than its
  scheduled issue date) allows to exist — this reduces in practice to: kghm lead 1 → the Dec 25 issue of
  `start_year − 1`; tjhm lead 0 → issued Jan 1 of `start_year` itself, already inside
  `[start_year, end_year]`, so the widening exception is never even exercised for it. Off-schedule
  issue dates reaching this mask are the **producer's** concern (LTF-015), not this reader's — the
  reader's own contract is exactly what it admits above, not what a well-behaved producer happens to
  send it.
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
- Drop a row when its issue year is `< start_year` **unless** it is that Q1-of-`start_year` row —
  checked via **both** target year `== start_year` **and** `quarter_in_year == 1`, not target year
  alone. Checking target year alone (an earlier, round-2 version of this fix) was still too permissive:
  it also kept an out-of-window row targeting some *other* calendar quarter of `start_year` (e.g. issued
  2024-12-25 targeting Q2 2025, not Q1), which could then beat a same-target monthly-derived row — or
  even an in-window direct row, depending on API order — via `drop_duplicates(keep="last")` (round-3
  out-of-loop review of the round-2 fix).
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
  `start_year` is dropped, both alone and alongside an in-window direct row, regardless of API order)
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
- **One writer-paused window** (ops instruction, no code): deploy PP-065, run the decision-F step (tjhm),
  then the recalc per org.
  - Pause **every** writer, not just the LT cron days (kghm 10 and 25; tjhm 1): operational runs, the
    maintenance runs (`apps/pipeline/pipeline_docker.py:1946-1972`; `apps/run_locally.sh:1745-1748`),
    any recalc other than the one below, and manual runs.
  - Wait for running jobs to finish. Then export, mutate, recalc and verify; only then resume.

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
   - The per-org quarter skill frame, read as the pipeline reads it (tombstones dropped,
     `src/data_reader.py:106, 2833`), is **non-empty**. Otherwise every quarterly ensemble is skipped
     (Problem 8). If K = 10 leaves an org with no quarter skill at all, B5 means no Naive Mean either:
     **escalate to the owner before the hydromet notice** goes out.
   - Freshly written QUARTER rows contain no rolling windows, no LR rows and no EM rows.
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
