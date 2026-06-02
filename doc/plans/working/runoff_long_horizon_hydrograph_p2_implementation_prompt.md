# Runoff long-horizon hydrograph P2 implementation prompt — season aggregation writer

> Paste the section between "--- BEGIN PROMPT ---" and "--- END PROMPT ---"
> to the implementation agent. P2 is additive on top of P1
> (`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`,
> committed at `93a1d36`). Work continues on
> `develop_dashboard_snow_display`. Plan at commit `ec03c44`.

--- BEGIN PROMPT ---

You are an implementation agent on the SAPPHIRE forecast tools project.
Your role is **Phase 2 only** of the long-horizon runoff hydrograph
plan at
`doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md`.
Phase 1 (commit `93a1d36`) added the monthly writer and tests. Phase 2
adds seasonal aggregation on top of the existing monthly records.

## What you are doing

**Goal**: Extend the existing P1 writer with a seasonal aggregator that
produces one `horizon_type="season"` record per `(station, target_year)`
with `(norm, previous, current)` as arithmetic means of the six April-
September monthly values from the Phase 1 record builder. The season
covers April-September (D3, the vegetation / high-flow window).

**Files you may modify (exhaustive)**

- `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` (EXTEND)
- `apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`
  (EXTEND)

You may NOT modify any other file. In particular:

- **No edits to `apps/iEasyHydroForecast/forecast_library.py`.** That
  helper rejects `season` at `forecast_library.py:3431-3444` by design;
  P2 uses the API client directly per D-Q3.
- **No edits to `apps/preprocessing_runoff/src/src.py`**, the existing
  `sync_monthly_norms.py`, or any other existing file.
- **No edits to `sapphire/services/`.** The preprocessing enum already
  accepts `season` at `sapphire/services/preprocessing/app/models.py:6-13`
  and the shared `Hydrograph` table already exposes `norm`, `previous`,
  `current`.
- **No edits to plan documents, decisions artifact, or other planning
  files.**

You may NOT change the existing P1 functions in
`sync_long_horizon_hydrograph.py`. Your work is purely additive:
add new helpers + tests alongside the existing ones. If a P1 function
needs refactoring to make P2 cleaner, escalate to the orchestrator.

## Source-of-truth references

- **Plan**: `doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md`
  (commit `ec03c44`). See §Phase 2 for the full contract.
- **Decisions artifact**: `doc/plans/working/runoff_long_horizon_hydrograph_decisions.md`
  (commit `4c49a4c`). D1 (aggregation formula), D3 (season window),
  and D-Q6 (per-month threshold) are the relevant ones.
- **P1 implementation**: `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
  (commit `93a1d36`). Read `build_monthly_records`,
  `write_station_monthly_hydrograph`, and `write_long_horizon_hydrograph`
  to understand the pattern your additions should mirror.

## Behaviour before this change

- P1 writes 12 monthly records per station per run, each with
  `horizon_type="month"` and the `(norm, previous, current)` triad
  subject to the per-month threshold (D-Q6) and the in-progress-month
  rule (D2).
- No seasonal records are written from `apps/preprocessing_runoff/`.
- The shared `Hydrograph` table already supports `horizon_type="season"`
  through the existing FastAPI service; no service work is required.
- The `iEasyHydroForecast/forecast_library.py` helper
  `_write_hydrograph_to_api` rejects `season` at lines 3431-3444 — that
  rejection is by design; the new writer goes through
  `SapphirePreprocessingClient.write_hydrograph` directly.

## Behaviour after this change

The writer also produces **one** seasonal record per station per target
year:

- `horizon_type = "season"`
- `code = <station>`
- `date = "{target_year}-04-01"` (April 1 of the target year)
- `day_of_year = 91 if target_year is not a leap year, else 92` — this
  is April 1's day-of-year. Use Python's
  `datetime.date(target_year, 4, 1).timetuple().tm_yday` so leap years
  are handled automatically. Do NOT hard-code 91.
- `horizon_value = 1` (sentinel)
- `horizon_in_year = 1` (sentinel — means "the one-and-only
  April-September season"; MUST NOT be interpreted ordinally by any
  future code)
- `norm`, `previous`, `current` computed independently per field, each
  as the arithmetic mean of the six monthly values for months 4-9 of
  the same target year.

### Aggregation rule (D1, the strict-completeness rule)

For each seasonal field `f ∈ {norm, previous, current}`:

```python
monthly_values_for_f = [
    monthly_record_for_month[f]
    for month in (4, 5, 6, 7, 8, 9)
]
if any(v is None for v in monthly_values_for_f):
    seasonal_f = None
else:
    seasonal_f = sum(monthly_values_for_f) / 6
```

If **any one** of the six monthly values for that field is `None`, the
seasonal field is `None`. Do NOT compute a partial mean over the
non-None subset; do NOT fall back to monthly norms; do NOT skip the row.
The other two seasonal fields can still be written if their six monthly
values are all populated. This matches the plan's §Phase 2 "Expected
behavior after" bullet and the round-2 reviewer-approved wording.

### JSON-safe (defence in depth)

Pass each seasonal field through the existing `_json_safe` helper
already defined in P1 before posting. The helper at line 57 of the
P1 file is the canonical one; reuse it. Do not create a duplicate.

Strictly speaking, an arithmetic mean of six finite floats is itself
finite, so this is a belt-and-braces step — but it keeps the contract
uniform across monthly and seasonal records, and makes the test
`test_season_writes_none_when_monthly_contains_nan` express the same
invariant as its monthly counterpart.

### In-progress / future months and the seasonal `current`

By D2, the in-progress month of `target_year` has `current = None`, and
months beyond `today.month` have nothing aggregated so their `current`
is also `None`. As a direct consequence of the strict-completeness rule,
the seasonal `current` is `None` whenever any of April-September in
`target_year` has not yet completed. This means for a 2026 target year
run on 2026-06-02:

- Seasonal `norm`: populated (norms are always present from the SDK).
- Seasonal `previous`: populated if all six monthly `previous` (which
  reference 2025) cleared the per-month threshold.
- Seasonal `current`: `None` (because June 2026 is in-progress and
  July-September 2026 haven't started).

A 2025-target rerun (operator backfill) on the same date would
populate the seasonal `current` if all six 2025 monthly `current`
values were above threshold, because 2025 is fully complete.

### Record identity (idempotency)

The service-side upsert key is
`(horizon_type, code, date)`
(`sapphire/services/preprocessing/app/crud.py:88-129`,
`sapphire/services/preprocessing/app/models.py:75-79`). A rerun with
identical mocked monthly inputs must produce a byte-equal seasonal
record dict — same `date`, same numeric fields, same sentinels. The
upsert key guarantees the service overwrites in place; the rerun test
asserts that the writer posts the same record both times.

## Implementation requirements

- **New function `build_seasonal_record(monthly_records, code,
  target_year)`** (or equivalent name). Receives the list of 12
  monthly record dicts P1 builds for the station and returns a single
  seasonal record dict (or `None` if none of the three seasonal
  fields can be populated — actually still return the dict with all
  three `None`, so the row is always written when there are monthly
  records to aggregate over; the dashboard semantics are "we tried,
  here's the seasonal slot, fields are None where data didn't
  qualify". Match the Q-4 spirit: never silently drop a station.
- **New function `write_station_seasonal_hydrograph(...)` or
  equivalent**. Mirrors `write_station_monthly_hydrograph` shape: takes
  the station code, monthly records, client, target_year, today;
  builds the seasonal record; posts via `client.write_hydrograph`.
- **Update `write_long_horizon_hydrograph(...)` (the top-level
  orchestrator) to call the seasonal builder after the monthly builder
  for each station, posting both monthly and seasonal records.**
  Pass the existing monthly record list to avoid recomputation.
  Keep the existing P1 monthly behaviour byte-identical.
- **No new env vars; no new dependencies.**
- **Use `today` parameter-passing**, not `dt.date.today()` inside
  helpers, matching the P1 convention.

## Tests (all required)

Append these to
`apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`.
Test names follow the P1 naming style.

1. **`test_season_writes_full_triad_from_complete_monthly`** — all six
   Apr-Sep monthly records have non-None `norm`, `previous`, `current`.
   Assert:
   - One seasonal record per station.
   - `horizon_type == "season"`, `code == TEST_CODE`,
     `date == f"{target_year}-04-01"`, `horizon_value == 1`,
     `horizon_in_year == 1`.
   - `day_of_year == 91` for non-leap target year (e.g. 2025) and
     `day_of_year == 92` for a leap target year (e.g. 2024).
   - Each seasonal field equals the arithmetic mean of the six
     corresponding monthly field values.

2. **`test_season_field_is_none_when_any_monthly_value_missing`** —
   for each of the three fields, test that if ONE of the six monthly
   values is None, the seasonal field is None and the other two
   seasonal fields are still computed correctly. Three sub-cases (one
   per field), or three separate tests.

3. **`test_season_writes_none_when_monthly_contains_nan`** — the
   monthly record builder should already null out non-finite values
   via `_json_safe` in P1, so the seasonal aggregator should not
   actually see NaN/inf in its inputs. As a defence-in-depth guard,
   construct monthly records that bypass `_json_safe` (e.g. inject
   NaN directly into a monthly dict) and assert the seasonal field
   for that month is treated as None by the aggregator (i.e. the
   strict-completeness rule fires because the value is `None` or
   non-finite). The posted seasonal field is None, not NaN.

4. **`test_season_horizon_identity_is_stable`** — assert the posted
   seasonal record's `(horizon_type, code, date)` triple is exactly
   `("season", TEST_CODE, f"{target_year}-04-01")`. This is the upsert
   key per `crud.py:88-129`; locking it in catches accidental drift.

5. **`test_season_idempotent_with_identical_monthly`** — run the
   seasonal writer twice with byte-identical monthly inputs. Assert:
   - The two posted seasonal records are element-wise equal.
   - The writer made two calls (no caching guess).
   - As in P1's idempotency test, the service-side
     `_has_changes=False` guarantee is referenced in a comment but
     not directly asserted (it lives in the service code).

6. **`test_season_current_is_none_for_in_progress_target_year`** —
   pin `today = date(2026, 6, 15)` and `target_year = 2026`. Build
   monthly records where April-May have populated `current` (complete
   months) and June onwards have `current = None` (in-progress or
   future per D2). Assert the seasonal `current` is `None`. As a
   companion: also build monthly records for a completed target year
   (e.g. 2025 with all six months populated) and assert the seasonal
   `current` IS populated. This locks in the interaction between D2
   and D1.

7. **`test_season_april_first_day_of_year_in_leap_year`** — explicit
   regression guard: assert `day_of_year == 92` when `target_year`
   is a leap year (2024 is the most recent). Reuses the seasonal
   builder with target_year=2024.

All tests use mocked SDK + mocked client; no live network. Follow
Arrange → Act → Assert. Use descriptive fixture names. Reuse the P1
fixture helpers where natural (e.g. `_norms`, `_full_year_rows`,
`_daily_rows`, `_record_for_month`).

## Self-review before returning

1. **Scope check**: `git diff --stat` shows exactly the two existing
   files modified (additive only). No new files. No edits to
   `forecast_library.py`, `src/src.py`, `sync_monthly_norms.py`,
   `apps/sapphire_api_client/`, or `sapphire/services/`.

2. **P1 behaviour preserved**: the existing P1 functions
   (`_json_safe`, `monthly_mean_threshold_80`, `_iter_daily_rows`,
   `_month_values`, `build_monthly_records`,
   `write_station_monthly_hydrograph`, `resolve_sdk_station_codes`,
   `main`) are byte-identical to commit `93a1d36`. Confirm with
   `git diff -U0 apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
   — every hunk should be a pure addition; no modifications to
   existing lines except as required to wire seasonal calls into
   `write_long_horizon_hydrograph`.

3. **No real station codes**: `grep -nE '\b[0-9]{4,5}\b'
   apps/preprocessing_runoff/sync_long_horizon_hydrograph.py
   apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`
   — any 4-5 digit number that isn't `19999`, a year, a port, a
   calendar value, or a line number reference needs inline
   justification.

4. **Test runner**: `cd apps && SAPPHIRE_TEST_ENV=True bash
   run_tests.sh preprocessing_runoff` passes. P1's 10 test cases
   continue to pass; the 7 new seasonal tests pass; total passes
   ≥ 317 (310 from P1 baseline + 7 new). The 2 pre-existing skips
   in `test_src.py` remain (unrelated to this phase).

5. **No date.today() in business logic**: `grep -nE
   'date\.today|datetime\.now' apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
   shows zero matches outside `main()` / argparse defaults / logging
   timestamps.

6. **`_json_safe` is not duplicated**: `grep -c 'def _json_safe'
   apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` returns 1.

## Hard constraints (non-negotiable)

1. **Do NOT modify any file outside the two file paths above.**
2. **Do NOT modify P1's existing functions** other than to wire
   the seasonal call into `write_long_horizon_hydrograph`.
3. **Do NOT edit `apps/iEasyHydroForecast/forecast_library.py`** or
   `sapphire/services/`.
4. **Do NOT add `# noqa` comments**, new dependencies, or new env
   vars.
5. **Do NOT use real station codes** anywhere. `TEST_CODE = "19999"`
   only.
6. **Do NOT commit, push, branch, stage, or stash.** The orchestrator
   commits after deliberation.
7. **Do NOT use `date.today()` or `datetime.now()` inside helpers.**
   Pass `today` as a parameter.
8. **Do NOT relax the strict-completeness rule.** If any one of the
   six monthly values for a field is None, the seasonal field is
   None.
9. **Do NOT skip or `xfail` tests.** All seven new tests must pass.

## Deliverable format

Return a single short Markdown report (under ~150 lines):

1. **Summary** — 2-3 sentences: seasonal builder + writer + 7 tests
   added; all tests pass; scope check clean; P1 behaviour preserved.
2. **Files modified** — two paths with line counts before/after.
3. **Scope check** — confirm no new files; only the two P1 files
   touched; specifically confirm no edits to
   `forecast_library.py`, `src/src.py`, `sync_monthly_norms.py`,
   `sapphire_api_client/`, or `sapphire/services/`.
4. **P1 preservation** — confirm the existing P1 functions are
   byte-identical (cite the `git diff -U0` summary).
5. **Test run** — paste the tail of `SAPPHIRE_TEST_ENV=True bash
   run_tests.sh preprocessing_runoff` showing pass/fail/skip totals.
6. **Strict-completeness compliance** — confirm the `any(v is None
   for v in monthly_values)` check is in place (cite line number).
7. **Day-of-year compliance** — confirm
   `datetime.date(target_year, 4, 1).timetuple().tm_yday` is used,
   not a hard-coded constant (cite line number).
8. **Sensitive-data check** — confirm no real station codes in code
   or tests.
9. **Coordination items** (optional) — anything the orchestrator
   should know (e.g. a name you chose that differs from the prompt's
   suggested name, or a place where reusing P1 helpers required a
   tiny adjustment).

## What success looks like

- Two existing files extended with new functions and tests.
- All seven new tests pass; all P1 tests still pass; no regressions.
- The seasonal record satisfies the strict-completeness rule (any
  missing monthly → that seasonal field is None).
- `day_of_year` is leap-aware (91 non-leap, 92 leap).
- `horizon_type="season"`, `(horizon_type, code, date)` upsert key
  is stable.
- No real station codes. No edits to shared / retired modules.
- Phase 3 (local end-to-end verification) can dispatch immediately
  after Phase 2's commit.

If something is ambiguous (e.g. how to wire the seasonal call into
the orchestrator without changing the monthly behaviour, or whether
to factor out a shared "compute mean of six values with strict
completeness" helper), STOP and escalate to the orchestrator with a
specific question. Do NOT guess.

--- END PROMPT ---
