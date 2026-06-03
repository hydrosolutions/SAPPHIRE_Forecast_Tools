# Runoff long-horizon hydrograph P1-fix implementation prompt — tolerate stations without monthly norms

> Paste the section between "--- BEGIN PROMPT ---" and "--- END PROMPT ---"
> to the implementation agent. P3 (local e2e verification at evidence file
> `runoff_long_horizon_hydrograph_e2e_evidence.md`) returned `BLOCKED —
> writer exit code 3` because one station's `get_norm_for_site` call
> returned zero norms and the P1 writer raised `ValueError`. The
> existing `forecast_library.py:5413-5427` pattern handles this case via
> log-warning + skip. This fix makes the P1 writer match that precedent.

--- BEGIN PROMPT ---

You are an implementation agent on the SAPPHIRE forecast tools project.
Your role is **P1-fix only**: a small, targeted patch to make
`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` tolerant of
stations whose iEH HF SDK monthly-norm call either raises an
exception or returns a non-12 result. The current code raises
`ValueError`, aborting the entire run on one bad station. The fix
makes the writer log a warning and skip the station, matching the
established `write_month_hydrograph_data` pattern.

## Context — why this fix is needed

P3 (local end-to-end verification) ran the writer against the
operator's live stack on 2026-06-02 and got:

```
ValueError: Expected 12 monthly norms for station <station-1>; got 0
```

The writer aborted with exit code 3 before processing the remaining
62 stations or writing any records. The plan's Q-4 explicitly states
"write the station/month row **when norm data exists**", implying
skip-when-absent. The canonical precedent at
`apps/iEasyHydroForecast/forecast_library.py:5413-5427` already
implements skip-and-continue for this exact data shape:

```python
try:
    norms = iehhf_sdk.get_norm_for_site(code, "discharge", norm_period="m")
except Exception as exc:
    logger.warning(
        f"write_month_hydrograph_data: SDK call failed for site {code}, skipping. "
        f"Error: {exc}"
    )
    continue

if len(norms) != 12:
    logger.warning(
        f"write_month_hydrograph_data: expected 12 norm values for site {code}, "
        f"got {len(norms)} — skipping this site."
    )
    continue
```

Match this pattern in the new writer.

## What you are doing

**Goal**: Make
`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` skip
stations whose SDK norm call either raises or returns a non-12
list. Other stations in the same run must continue to be
processed. Add two tests in
`apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`
that lock in the skip behaviour.

**Files you may modify (exhaustive)**

- `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
  (PATCH, additive + one logic change)
- `apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`
  (PATCH, additive only)

You may NOT modify any other file. In particular:

- **No edits to `apps/iEasyHydroForecast/forecast_library.py`**.
- **No edits to other preprocessing_runoff source files**
  (`src/src.py`, `sync_monthly_norms.py`, etc.).
- **No edits to `sapphire/services/`** or `apps/sapphire_api_client/`.
- **No edits to plan documents, decisions artifact, or other planning
  files.**

## Behaviour before this change

In `sync_long_horizon_hydrograph.py` (commit `785528a`):

1. `write_station_monthly_hydrograph` at line ~170 calls
   `iehhf_sdk.get_norm_for_site(code, "discharge", norm_period="m")`
   without exception handling. If the SDK raises, the exception
   propagates up to `main()`'s generic `except Exception` and the
   process exits with code 3.
2. `build_monthly_records` at line ~131 raises `ValueError` when
   `len(norms) != 12`. Same effect: the process exits with code 3
   on the first bad station.
3. The orchestrator `write_long_horizon_hydrograph` (line ~256)
   does not skip stations; it assumes every station produces 12
   monthly records.

Net effect: one operationally-imperfect station blocks all 62 other
stations.

## Behaviour after this change

1. `write_station_monthly_hydrograph` wraps the SDK call in
   `try / except Exception`. On exception: log a warning at
   `WARNING` level (NOT `ERROR`) naming the station and the
   exception type/message, then return an empty list.
2. The length check moves into `write_station_monthly_hydrograph`
   BEFORE calling `build_monthly_records`. If
   `len(norms) != 12`, log a warning naming the station and the
   actual count, then return an empty list.
3. `build_monthly_records` keeps its current contract: it assumes
   it receives exactly 12 norm values. Remove the internal
   `raise ValueError` (or convert it to an `assert` for
   developer-debugging, your call — but the runtime path no
   longer reaches it because the caller validates first).
4. `write_long_horizon_hydrograph` checks the monthly result and:
   - If empty (station skipped due to norm issue): do NOT call
     the seasonal builder for that station. Do NOT extend
     `all_records`. Continue to the next station.
   - If non-empty (12 records): existing behaviour — extend
     `all_records` with monthly records and append the seasonal
     record.
5. After the loop, if `all_records` is empty (zero stations
   produced norms), log an `ERROR` and let `main()` exit with
   the existing exit-code-2 ("no SDK sites remain after
   filtering") — or pick a distinct sentinel return (your call;
   document it). The CURRENT behaviour for an empty
   `code_list` is exit 2 at `main()` line ~273; matching that
   for an empty-after-filtering case is the most consistent.

The new writer is a strict superset of the old when norms are
clean (no behavioural change for stations with valid norms). It
becomes tolerant for the missing-norm case.

## Implementation guidance

Mirror the canonical pattern at
`apps/iEasyHydroForecast/forecast_library.py:5413-5427` verbatim
for the log message shape, severity, and `continue` semantics
(in your code, the `continue` becomes a `return []` since each
station is processed inside `write_station_monthly_hydrograph`,
and the orchestrator decides whether to extend or skip based on
the empty return).

Use the existing module `logger` (`logger = logging.getLogger(__name__)`).
Keep log messages free of real station codes; log the code as
passed (callers responsibility) — your test fixtures use
`TEST_CODE = "19999"`.

Do NOT introduce a new module-level constant, env var, or
configuration knob for this behaviour. It's not configurable; it's
the only correct semantics.

Do NOT change the `_json_safe` helper, the per-month threshold
helper, the seasonal aggregator, the JSON-safe wrapper application,
or any of the existing tests' expected behaviour. P1+P2's contracts
for stations with valid norms are unchanged.

## Tests (both required)

Append these to
`apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py`:

1. **`test_skips_station_when_norms_missing`** — mock
   `iehhf_sdk.get_norm_for_site` to return an empty list `[]` for
   the station code under test. Assertions:
   - `write_station_monthly_hydrograph(...)` returns `[]` (or
     equivalent empty), not raising.
   - `client.write_hydrograph` is NOT called for that station.
   - A `WARNING` log line is emitted that includes "12" (the
     expected count) and "0" (the actual count). Use `caplog`
     or equivalent to capture.
   - Test a second mocked call where the SDK returns 7 values
     (partial response) and assert the same skip behaviour. The
     warning message should reflect "got 7".

2. **`test_skips_station_when_sdk_raises`** — mock
   `iehhf_sdk.get_norm_for_site` to raise an exception (e.g.
   `ConnectionError("tunnel down")`). Assertions:
   - `write_station_monthly_hydrograph(...)` returns `[]`, not
     raising.
   - `client.write_hydrograph` is NOT called for that station.
   - A `WARNING` log line is emitted naming the exception type
     and message.

3. **`test_orchestrator_continues_after_skipped_station`** —
   build a fixture where station A's SDK call raises and station
   B's SDK call returns valid 12 norms. Call
   `write_long_horizon_hydrograph(codes=["A", "B"], ...)` (or
   whichever signature is current). Assertions:
   - Station A produces zero records.
   - Station B produces 12 monthly + 1 seasonal = 13 records.
   - `client.write_hydrograph` is called once for B's monthly
     batch and once for B's seasonal batch (or merged — match
     the actual code path).
   - The function returns 13 records total (or whatever the
     orchestrator's return shape is — match existing behaviour).
   - No `ValueError` raised; the function completes normally.

(The third test is a critical integration check — it's what
proves the bug is actually fixed end-to-end. The orchestrator
must NOT abort on station A.)

All tests use mocked SDK + mocked client; no live network.

## Self-review before returning

1. **Scope check**: `git diff --stat` shows exactly two existing
   files modified, no new files. No edits to
   `forecast_library.py`, `src/src.py`, `sync_monthly_norms.py`,
   `sapphire_api_client/`, or `sapphire/services/`.

2. **Existing behaviour preserved**: re-run the full P1+P2 test
   suite. All 319 previous tests still pass. No regressions in
   the per-month threshold, in-progress-month rule, JSON-safe
   helper, or seasonal aggregation.

3. **New tests pass**: the three new tests above pass.

4. **No `raise ValueError` remains in `build_monthly_records`
   for the count case**. `grep -n 'raise ValueError' apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
   should return zero matches (or document any remaining raise
   inline as still-needed).

5. **Log severity is WARNING, not ERROR**. `grep -nE
   'logger\.warning' apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
   shows the two new warning lines (SDK failure + count mismatch).

6. **No real station codes**: same sniff as P1 / P2.

7. **Test runner**: `cd apps && SAPPHIRE_TEST_ENV=True bash
   run_tests.sh preprocessing_runoff` passes. Expected: at
   least 322 passed (319 + 3 new), 2 skipped.

## Hard constraints (non-negotiable)

1. **Do NOT modify any file outside the two file paths above.**
2. **Do NOT change the per-month threshold helper, the seasonal
   aggregator, the `_json_safe` helper, or any existing test's
   expected behaviour.** Stations with valid norms must produce
   byte-identical output to commit `785528a`.
3. **Do NOT add new env vars, new dependencies, or new
   configuration knobs.**
4. **Do NOT log at `ERROR` for the per-station skip case.**
   `WARNING` is the right severity — this is expected
   operational data heterogeneity, not a fatal error. Match
   the precedent.
5. **Do NOT silently catch other exceptions** that aren't
   SDK-related. The try/except wraps ONLY the
   `get_norm_for_site` call, not the rest of the per-station
   work.
6. **Do NOT commit, push, branch, stage, or stash.** The
   orchestrator commits after deliberation.
7. **Do NOT use real station codes** anywhere.

## Deliverable format

Return a single short Markdown report (under ~100 lines):

1. **Summary** — 2 sentences: skip-and-continue handling added;
   3 new tests pass; existing 319 tests still pass.
2. **Files modified** — two paths with line counts before/after.
3. **Scope check** — confirm only the two files were touched;
   specifically confirm no edits to `forecast_library.py`,
   `src/src.py`, `sync_monthly_norms.py`, `sapphire_api_client/`,
   `sapphire/services/`.
4. **Test run** — paste the tail of
   `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff`
   showing pass/fail/skip totals.
5. **Behaviour compliance** — confirm `try/except` wraps only
   the SDK call; the length-check downgrade is in
   `write_station_monthly_hydrograph` not in
   `build_monthly_records`; the orchestrator skips empty-monthly
   stations cleanly (cite line numbers).
6. **No regressions** — confirm the existing P1+P2 tests (10 +
   9 = 19 long-horizon tests) all still pass.
7. **Sensitive-data check** — confirm no real station codes.
8. **Coordination items** (optional) — anything the orchestrator
   should know.

## What success looks like

- Two existing files patched additively.
- All previous 319 tests still pass; 3 new tests pass; total
  ≥322 passed, 2 skipped.
- A station whose SDK norm call raises or returns non-12 is
  skipped with a `WARNING` log; remaining stations process
  normally.
- The orchestrator does not abort on a single bad station.
- Phase 3 (e2e verification) can re-dispatch and proceed past
  the previously-blocking station.

If you encounter an ambiguity (e.g. the orchestrator's return
shape doesn't match what the new test expects, or the existing
test fixtures already cover this case in a way that conflicts),
STOP and escalate to the orchestrator with a specific question.
Do NOT guess.

--- END PROMPT ---
