# Runoff long-horizon hydrograph P1 implementation prompt — monthly previous/current writer

> Paste the section between "--- BEGIN PROMPT ---" and "--- END PROMPT ---"
> to the implementation agent. This is the **first code-bearing phase** of
> the long-horizon runoff hydrograph plan. Work continues on
> `develop_dashboard_snow_display`. Plan at commit `4c49a4c`;
> P0a decisions at commit `28ba979`; P0b PROCEED at commit `4c49a4c`.

--- BEGIN PROMPT ---

You are an implementation agent on the SAPPHIRE forecast tools project.
Your role is **Phase 1 only** of the long-horizon runoff hydrograph
plan at
`doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md`.
This is the first code-bearing phase. P0a (decisions) and P0b (sanity
gates → PROCEED) are complete.

## What you are doing

**Goal**: Add a new monthly long-horizon writer at
`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` and its
test file at
`apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`.
The writer produces 12 records per station per run, each with
`horizon_type="month"` and the full triad `(norm, previous, current)`.
Norms come from the iEH HF SDK monthly norm call already used by the
existing code; `previous` and `current` come from local daily SAPPHIRE
runoff aggregation, with a per-month threshold rule (D-Q6) that writes
`None` when the cell is too sparse to be trustworthy.

**Files you may modify (exhaustive)**

- `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` (CREATE)
- `apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`
  (CREATE)

You may NOT modify any other file. In particular:

- **No edits to `apps/iEasyHydroForecast/forecast_library.py`.** If the
  direct client is somehow insufficient, escalate to the orchestrator
  rather than touching `forecast_library.py`. The old norm-only writer
  there is retired in Phase 4 — do not patch it now.
- **No edits to `apps/preprocessing_runoff/src/src.py`**, the existing
  `sync_monthly_norms.py`, `preprocessing_runoff.py`, or any other
  existing file.
- **No edits to `sapphire/services/`.** The shared `/hydrograph/`
  endpoint and the `HorizonType` enum already accept `month`.
- **No edits to plan documents, decisions artifact, or other planning
  files.**

## Source-of-truth references

- **Plan**: `doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md`
  (commit `4c49a4c`). See §Phase 1 for the full contract; §Decisions
  Committed for D1-D4, Q-1 through Q-6.
- **Decisions artifact**: `doc/plans/working/runoff_long_horizon_hydrograph_decisions.md`
  (commit `4c49a4c`). D-Q6 — the per-month threshold — is new and
  governs `previous`/`current` writing.

The plan and decisions artifact are the canonical sources for behaviour.
This prompt summarises them but does not replace them.

## Behaviour before this change

- `apps/preprocessing_runoff/sync_monthly_norms.py` delegates to
  `write_month_hydrograph_data` in `forecast_library.py:5367-5373`,
  which writes 12 monthly **norm-only** rows. `previous` and `current`
  remain `None` because the shared helper only derives them from
  4-digit year columns
  (`apps/iEasyHydroForecast/forecast_library.py:3446-3456`,
  `apps/iEasyHydroForecast/forecast_library.py:3517-3526`).
- `SapphirePreprocessingClient.write_hydrograph` is a thin pass-through
  to the FastAPI service
  (`apps/sapphire_api_client/sapphire_api_client/preprocessing.py:198-208`,
  `apps/sapphire_api_client/sapphire_api_client/client.py:142-152`). No
  NaN/inf sanitization happens upstream — that's why the JSON-safe
  helper must live in the new writer.
- The local preprocessing API exposes daily runoff at
  `GET /preprocessing/runoff/?horizon=day&code={code}&start_date=YYYY-01-01&end_date=YYYY-12-31&limit=10000`.
  This is the data source for `previous`/`current` aggregation.

## Behaviour after this change

The new writer at `sync_long_horizon_hydrograph.py`:

1. Loads station codes from the env-configured station set (use the
   same lookup that `sync_monthly_norms.py` uses — read it; do not
   re-implement station resolution).
2. For each station, fetches monthly norms via
   `iehhf_sdk.get_norm_for_site(code, "discharge", norm_period="m")`
   (12 values) per
   `apps/iEasyHydroForecast/forecast_library.py:5379-5381`.
3. For each station, fetches daily runoff for `Y` and `Y-1` via the
   preprocessing API endpoint above (one call per year per station).
4. Builds 12 records, one per `target_month ∈ 1..12`:
   - `horizon_type = "month"`
   - `code = <station>`
   - `date = "{Y}-{target_month:02d}-01"` (the target year is `Y`,
     not `Y-1`; `previous` references `Y-1` via field, not via date)
   - `day_of_year` = the mid-month day-of-year matching the existing
     monthly norm helper at
     `apps/iEasyHydroForecast/forecast_library.py:5402-5438`.
   - `horizon_value = target_month`, `horizon_in_year = target_month`
   - `norm = sdk_norms[target_month - 1]` (passed through the JSON-safe
     helper)
   - `previous = monthly_mean(daily_Y_minus_1, Y-1, target_month)`
     applying the per-month threshold rule
   - `current = monthly_mean(daily_Y, Y, target_month)` applying the
     per-month threshold rule AND the in-progress-month rule
5. Calls `client.write_hydrograph(records)` once per station (or once
   per batch; either is fine as long as the per-record contract is met).

### Per-month threshold rule (D-Q6, the core data-quality gate)

For each `(station, year, month)` cell:

```python
import calendar, math

def monthly_mean_threshold_80(daily_values_for_month, year, month):
    """
    Return arithmetic mean of non-null finite values, or None if
    fewer than 80% of calendar days in the month have a non-null
    finite value.
    """
    days_in_month = calendar.monthrange(year, month)[1]  # 28/29/30/31
    non_null_finite = [
        v for v in daily_values_for_month
        if v is not None and isinstance(v, (int, float)) and math.isfinite(v)
    ]
    if len(non_null_finite) / days_in_month < 0.80:
        return None
    return sum(non_null_finite) / len(non_null_finite)
```

The denominator is `calendar.monthrange(year, month)[1]`, NOT
`len(daily_values_for_month)` and NOT a hard-coded 30 or 31. Get this
right for February in both leap and non-leap years.

### In-progress month rule (D2)

For `current` specifically, even if the threshold passes for the
current calendar month, `current = None`. A month is in-progress when
`(target_year, target_month) == (today.year, today.month)`. Locked in
by `test_current_is_none_for_in_progress_month`.

Capture `today` at the entry point (the `main()` or the CLI argparse
boundary), then pass it as a parameter to all downstream functions per
CLAUDE.md's "forecast date rule". Do NOT use `date.today()` inside
the threshold helper or the record builder — that would make tests
non-deterministic.

### JSON-safe helper (BLOCKER-2 from review rounds)

Every numeric record field (`norm`, `previous`, `current`) MUST pass
through a local `_json_safe` helper before
`client.write_hydrograph`:

```python
import math

def _json_safe(value):
    """NaN, +inf, -inf, and None all map to None. Finite floats pass through."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not math.isfinite(value):
        return None
    return value
```

This helper lives in `sync_long_horizon_hydrograph.py`, NOT in
`forecast_library.py`, NOT in `apps/sapphire_api_client/`, NOT in
`apps/preprocessing_runoff/src/src.py`. Rationale: the API client is a
thin pass-through; FastAPI rejects NaN/inf with a 422 that aborts the
whole batch. The snow `_json_safe` precedent (commit `2793b62`) is the
template.

### Missing-year rule (Q-4)

If `daily_Y_minus_1` is empty (e.g. the station has no `Y-1` data at
all): every record's `previous` is `None`, but every record's `current`
follows its own threshold rule and the row is still written. Likewise
if `daily_Y` is empty.

Never skip a record purely because one or both of `previous`/`current`
is `None`. The norm row is the anchor; previous/current decorate it.

## Implementation requirements

- **Station set resolution**: read the same env var(s) that
  `sync_monthly_norms.py` reads. Do not introduce new env vars.
- **API base URL**: use the same `SAPPHIRE_API_URL` /
  `SAPPHIRE_API_ENABLED` pattern that the rest of preprocessing_runoff
  uses. Do not hard-code `http://localhost:8000`.
- **iEH HF SDK access**: use the same SDK construction the existing
  monthly-norms path uses (`forecast_library.py:5411-5415` for the call
  shape; the SDK client itself is already constructed upstream in
  `sync_monthly_norms.py`). If the existing pattern is to receive an
  `iehhf_sdk` instance via dependency injection, mirror that — your
  unit tests will mock it.
- **CLI**: provide an argparse entry point with at minimum
  `--target-year` (defaults to current year) so operators can backfill
  a prior year. Mirror the option style of `sync_monthly_norms.py:1-50`
  for consistency.
- **Logging**: use the standard logging module already in
  preprocessing_runoff. Log per-station progress at INFO; threshold
  decisions at DEBUG. Do not log raw daily values.
- **No new dependencies.** Use stdlib `calendar`, `math`, `datetime`,
  `argparse`, `logging` + already-installed `pandas` /
  `sapphire_api_client` / `requests`. Check
  `apps/preprocessing_runoff/pyproject.toml` if uncertain.
- **No real station codes in test fixtures.** Use `19999` or
  `<station-N>` aliases per
  `[[feedback-no-real-station-codes]]`.

## Tests (all required)

Create
`apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`
with at minimum these tests. All tests use mocked SDK + mocked
preprocessing API; no live network.

1. **`test_writes_full_triad_with_complete_data`** — a station with
   100% daily coverage for both `Y` and `Y-1`. Asserts:
   - 12 records returned.
   - Each has `horizon_type="month"`, `code=<station>`,
     `date="{Y}-{month:02d}-01"`, `horizon_value=month`,
     `horizon_in_year=month`.
   - Each has `norm` = the SDK norm for that month.
   - Each has `previous` = the arithmetic mean of the mocked Y-1 daily
     values for that month.
   - Each has `current` = the arithmetic mean of the mocked Y daily
     values for that month (for completed months).

2. **`test_one_year_missing_writes_only_other_field`** — `daily_Y` is
   non-empty, `daily_Y_minus_1` is empty. Asserts every record has
   `current` populated (subject to in-progress rule) and `previous =
   None`. Row count unchanged.

3. **`test_current_is_none_for_in_progress_month`** — D2 rule. Pin
   `today = date(2026, 6, 15)`. Mock `daily_Y` so June 2026 has 100%
   coverage. Assert the June record has `current = None` regardless;
   May and earlier completed months get their means; July-December
   also get `current = None` (because they're future, no data yet).

4. **`test_monthly_mean_below_threshold_writes_none`** — D-Q6 below
   threshold. Mock January 2025 daily data with 24/31 non-null days
   (77.4%, below 80%). Assert the January record has `previous = None`
   even though norm and `current` (Jan 2026) may be populated.

5. **`test_monthly_mean_at_threshold_writes_value`** — D-Q6 at the
   80% boundary. Test three cases in one parametric or three separate
   tests:
   - 31-day month (e.g. January): 25/31 non-null days (80.6%) → mean
     written.
   - 30-day month (e.g. April): 24/30 non-null days (80.0%) → mean
     written.
   - 28-day February in a non-leap year (2025): 23/28 non-null days
     (82.1%) → mean written. Verify the divisor is 28 not 29.
   Each assertion: the written value equals the arithmetic mean of the
   non-null finite values supplied.

6. **`test_writes_none_when_daily_series_contains_nan`** — JSON-safe
   helper. Mock `daily_Y_minus_1` for one month with values including
   `float("nan")`, `float("inf")`, `float("-inf")`. Two assertions:
   - The non-finite values are treated as non-null-finite-missing for
     threshold counting (so a month with 5 valid days + 26 NaN/inf
     days has effective coverage 5/31 = 16% → below threshold →
     `previous = None`).
   - When the threshold IS met by finite values only, the resulting
     mean is finite (no NaN propagation). And as a separate guard:
     when somehow a NaN/inf reaches the per-record dict (e.g. via a
     test that bypasses the mean computation and feeds NaN directly
     into the record), the JSON-safe helper still returns `None`.
   Include explicit assertions that the posted record contains
   `None`, not the string `"NaN"`, not `float("nan")`, not the
   numeric mean of a NaN-containing series.

7. **`test_idempotent_writes_with_identical_upstream`** — determinism
   invariant. Run the writer twice with byte-identical mocked SDK and
   API responses. Two assertions:
   - The list of records posted on the second run is element-wise
     equal to the first run (same `(norm, previous, current)` per
     date).
   - The second run triggers zero `Updated hydrograph` log lines from
     the service-side upsert. (Mock the service log; or, more simply,
     assert the writer call count and the record shape — the
     service-side log claim is verified via `_has_changes` returning
     False, which depends on the service's `crud.create_hydrograph`
     at `sapphire/services/preprocessing/app/crud.py:88-129` and is
     out of scope for a unit test. Acceptable simplification:
     assert that the writer posts the SAME records both times, and
     reference the service-side guarantee in a comment.)

8. **`test_calendar_days_used_for_february_non_leap`** — explicit
   guard against off-by-one in the divisor. 2025 is non-leap. Mock
   23 non-null days for February 2025. Assert `previous` is written
   (23/28 = 82.1% ≥ 80%). Then mock 22 non-null days; assert
   `previous = None` (22/28 = 78.6% < 80%).

Each test follows Arrange → Act → Assert. Use descriptive fixture
names (`mocked_full_daily_Y`, `mocked_sparse_january`), not generic
(`data`, `fixture1`). Assert on outputs and public API surface only;
do not probe private attributes.

## Self-review before returning

Run all of these:

1. **Scope check**: `git diff --stat` should show exactly two new
   files at the paths above. No other files touched. No edits to
   `forecast_library.py`, `src/src.py`, `sync_monthly_norms.py`,
   `apps/sapphire_api_client/`, or `sapphire/services/`.

2. **No real station codes**: `grep -nE '\b[0-9]{4,5}\b'
   apps/preprocessing_runoff/sync_long_horizon_hydrograph.py
   apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`
   — any 4-5 digit number that isn't `19999`, a year, a port, a line
   number reference, or a calendar value (28, 29, 30, 31, 80) needs
   inline justification.

3. **JSON-safe helper location**: `grep -nE '_json_safe|isfinite'
   apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` returns
   matches. Same grep over `apps/iEasyHydroForecast/forecast_library.py`
   and `apps/sapphire_api_client/` shows you didn't add it elsewhere.

4. **Test runner**: `cd apps && SAPPHIRE_TEST_ENV=True bash
   run_tests.sh preprocessing_runoff` passes, zero failures, zero
   unexpected skips. (Dependency-gated skips on
   `SAPPHIRE_API_AVAILABLE` remain acceptable per CLAUDE.md.)

5. **No date.today() in business logic**: `grep -nE
   'date\.today|datetime\.now' apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
   shows zero matches outside of `main()` / argparse defaults /
   logging timestamps.

## Hard constraints (non-negotiable)

1. **Do NOT modify any file outside the two new file paths.**
2. **Do NOT edit `apps/iEasyHydroForecast/forecast_library.py`.**
   Escalate if you think you need to.
3. **Do NOT add `# noqa` comments.** Fix the code instead, or escalate
   if the rule is overly strict project-wide.
4. **Do NOT use real station codes** anywhere in code, tests, or
   reports. Aliases / `19999` only.
5. **Do NOT add new dependencies.** Stdlib + existing
   pandas/requests/sapphire_api_client/iehhf_sdk only.
6. **Do NOT commit, push, branch, stage, or stash.** The orchestrator
   commits after deliberation.
7. **Do NOT use `date.today()` or `datetime.now()` inside the
   threshold helper, record builder, or any function called from
   them.** Pass `today` as a parameter.
8. **Do NOT skip or `xfail` tests.** All eight tests must pass.

## Deliverable format

Return a single short Markdown report (under ~150 lines):

1. **Summary** — 2-3 sentences: writer + tests created; all tests
   pass; scope check clean.
2. **Files created** — full paths, line counts.
3. **Scope check** — confirm exactly two new files; no other files
   touched; specifically confirm no edits to `forecast_library.py`,
   `src/src.py`, `sync_monthly_norms.py`, `sapphire_api_client/`, or
   `sapphire/services/`.
4. **Test run** — paste the tail of `SAPPHIRE_TEST_ENV=True bash
   run_tests.sh preprocessing_runoff` showing pass/fail/skip totals.
5. **Threshold compliance** — confirm
   `calendar.monthrange(year, month)[1]` is the divisor everywhere
   (cite line numbers).
6. **JSON-safe compliance** — confirm `_json_safe` lives in
   `sync_long_horizon_hydrograph.py` and not elsewhere (one-line
   grep evidence).
7. **In-progress month compliance** — confirm `current = None` for
   the (today.year, today.month) cell (cite test name + line).
8. **Sensitive-data check** — confirm no real station codes in code
   or tests.
9. **Coordination items** (optional) — anything the orchestrator
   should know (e.g. an SDK shape you had to clarify, a station
   set lookup that differed from the plan's expectation, etc.).

## What success looks like

- Two new files at the specified paths.
- All eight tests pass under `SAPPHIRE_TEST_ENV=True bash
  run_tests.sh preprocessing_runoff`.
- The full preprocessing_runoff test suite still passes (no
  regression).
- The writer's behaviour matches the plan and decisions artifact
  exactly. Phase 2 (season aggregation) can dispatch immediately
  after Phase 1's commit.
- No real station codes. No edits to retired or shared modules.
- The orchestrator can read the deliverable and verify scope without
  a deep diff.

If something is ambiguous (e.g. how the existing station-set lookup
works, whether the SDK returns 12 floats or a dict), STOP and escalate
to the orchestrator with a specific question. Do NOT guess. The
orchestrator has read the relevant code and can clarify in one
round-trip.

--- END PROMPT ---
