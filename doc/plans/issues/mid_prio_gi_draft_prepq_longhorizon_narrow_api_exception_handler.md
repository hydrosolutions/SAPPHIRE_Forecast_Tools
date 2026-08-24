# PREPQ-018: Long-horizon hydrograph write loop's exception tuple misses `JSONDecodeError`, exits 3 instead of 5

**Status**: Draft (2026-08-21)
**Module**: `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
**Priority**: **Mid** — pre-existing, fails loudly (fatal exit code, pipeline aborts), no data loss.
The defect is a wrong exit code and one missing station status in the run summary, not silent
success. Raised to Mid rather than Low because PREPQ-015 (Implemented, 2026-08-21) makes the gap
more travelled: stations that used to exit the per-station loop early via the norm-lookup raise
now proceed to the write calls, so more attempted stations per run pass through the narrow
handler.
**Labels**: `preprocessing_runoff`, `long-horizon`, `error-handling`, `exit-code`
**Found**: 2026-08-21, out-of-loop review of the PREPQ-015 implementation diff.
**Related**: **PREPQ-015** (`issues/review_gi_draft_prepq_longhorizon_sdk_failure_drops_station.md`,
Implemented) widens exposure to this pre-existing gap but does not introduce it — see
"Pre-existing, not introduced by PREPQ-015" below.

---

## The defect

`write_long_horizon_hydrograph`'s per-station write loop
(`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py:559-604`) wraps the three per-station
writer calls (monthly, seasonal, quarterly) in a single `try`/`except`:

```python
except _API_READ_WRITE_ERRORS as exc:
```

at `:592`. `_API_READ_WRITE_ERRORS` is defined at `:51-56`:

```python
_API_READ_WRITE_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)
if SapphireAPIError is not None:
    _API_READ_WRITE_ERRORS = (SapphireAPIError, *_API_READ_WRITE_ERRORS)
```

so the tuple contains at most `(SapphireAPIError, requests.exceptions.ConnectionError,
requests.exceptions.Timeout)`. The pinned `sapphire-api-client` calls `response.json()` after a
successful HTTP response; that can raise `requests.exceptions.JSONDecodeError`. Verified via
`JSONDecodeError.__mro__`: `JSONDecodeError -> InvalidJSONError -> RequestException -> OSError,
json.JSONDecodeError -> ValueError -> Exception`. It is a `RequestException` subclass, not a
`ConnectionError` or `Timeout` subclass, and is not `SapphireAPIError` — so it is not caught by
`_API_READ_WRITE_ERRORS` and is not caught anywhere inside the loop.

**Consequence**, traced through the code: the uncaught exception exits `write_station_monthly_
hydrograph`/`write_station_seasonal_hydrograph`/`write_station_quarterly_hydrograph` and
`write_long_horizon_hydrograph` entirely, propagating up through `main()`'s call at `:762-768`.
The affected station is left in `attempted_station_codes` (appended at `:561`, before the `try`)
but in none of `completed_station_codes`, `failed_station_codes`, or `station_statuses` — those
are only appended inside the `try` block's success path (`:590-591`) or the `except` block
(`:593-596`), and this exception hits neither. `main()`'s generic handler
(`:798-803`) then catches it as `Exception` and calls `sys.exit(3)` — the "unexpected exception"
code documented at `:734` — instead of the correct `sys.exit(5)` ("API read/write failure",
`:736-739`) that `_exit_code_for_long_horizon_summary` (`:629-641`) would have produced had the
station reached a terminal `API_FAILED` status.

## Pre-existing, not introduced by PREPQ-015

`git diff origin/maxat_sapphire_2 -- apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
shows the PREPQ-015 diff touches only `write_station_monthly_hydrograph` (`:337-417`) and the
`SDK_FAILED` skip/pop removal in `write_long_horizon_hydrograph` (`:567-575`ish) — it does not
touch `_API_READ_WRITE_ERRORS` (`:51-56`) or the `except` clause (`:592`) at all. The tuple is
byte-identical to trunk. The gap affects `WRITTEN` and `NORM_ABSENT` stations today, exactly as it
would a station now landing on the `SDK_FAILED` path.

**What PREPQ-015 changes is exposure, not existence.** Before PREPQ-015, a station whose norm
lookup raised (`SDK_FAILED`) returned early with `records=[]` and never reached the seasonal/
quarterly write calls in the same iteration — so the narrow handler's blind spot on those write
calls never mattered for that station. After PREPQ-015, `SDK_FAILED` stations fall through the
same write path as `WRITTEN`/`NORM_ABSENT` stations, so more attempted stations per run traverse
the gap this issue describes. State that plainly: this is the reason the issue was found now, not
a claim that PREPQ-015 created the gap.

## Not silent, not data loss — get the framing right

Exit 3 is fatal. `run_locally.sh`'s generic `elif [ $lt_rc -ne 0 ]` branch sets `rc`, the module is
recorded FAIL, and the pipeline aborts — the same abort behavior INFRA-037 documents for exit 4/5.
The cost here is narrower than a silent-success defect: a **wrong exit code** (3 instead of 5,
losing the specific "API failure" signal in favor of a generic "unexpected exception" one) and
**one missing station status** in `station_statuses` relative to `attempted_station_codes` (so the
run summary undercounts by one and `len(station_statuses) == len(attempted_station_codes)` does
not hold for that run). This is why the fix is filed as a Mid-priority issue rather than corrected
inline in the PREPQ-015 patch.

## Proposed fix (not implemented here)

Widen the exception coverage at the client boundary rather than special-casing one more
`requests.exceptions.*` member. Two shapes, either acceptable:

- Add the relevant `requests.exceptions.RequestException` subtypes (at minimum
  `JSONDecodeError`/`InvalidJSONError`) to `_API_READ_WRITE_ERRORS` at `:51-56`.
- Or catch `requests.exceptions.RequestException` itself (the common base class covering
  `ConnectionError`, `Timeout`, and `JSONDecodeError` alike) instead of enumerating subtypes,
  keeping `SapphireAPIError` as a separate arm if it is not itself a `RequestException` subclass.

**Constraint the fix must preserve**: programming errors — `KeyError`, `TypeError`, etc. — raised
by application code (not the HTTP/JSON transport layer) must keep propagating uncaught out of
`write_long_horizon_hydrograph`, exiting via `main()`'s `sys.exit(3)` path as today.
`test_orchestrator_does_not_catch_programming_errors`
(`apps/preprocessing_runoff/test/test_sync_long_horizon_hydrograph.py:1606-1630`) asserts exactly
this — it makes `client.write_hydrograph` raise `KeyError("bad record shape")` on the third call
and asserts `pytest.raises(KeyError)` around `write_long_horizon_hydrograph`. Whatever widened
tuple or base class is chosen, it must not become broad enough to swallow that `KeyError`.

## Acceptance criteria

- A `requests.exceptions.JSONDecodeError` raised by any of the three per-station writer calls
  (monthly, seasonal, quarterly) inside `write_long_horizon_hydrograph`'s loop is caught by the
  `except` clause at `:592`, not propagated to `main()`.
- The affected station's sole terminal status is `LongHorizonStationWriteStatus.API_FAILED` — it
  appears in `failed_station_codes`, not in `completed_station_codes`.
- `main()` exits **5**, not 3, for a run where this is the only failure.
- `len(station_statuses) == len(attempted_station_codes)` holds after the run (no station is left
  with zero terminal statuses).
- `test_orchestrator_does_not_catch_programming_errors` continues to pass unmodified — a `KeyError`
  raised by a writer call still propagates out of `write_long_horizon_hydrograph` uncaught.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — zero failures, zero
  unexpected skips.
