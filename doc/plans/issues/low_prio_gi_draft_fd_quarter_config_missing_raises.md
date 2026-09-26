# FD-031: quarter enabled but `quarter.json` missing raises before FD-029's degraded mode

**Status**: Draft (2026-09-26)
**Module**: `apps/forecast_dashboard`
**Priority**: Low — pre-existing on trunk; not introduced by FD-029.
**Related**: FD-029 (`mid_prio_gi_draft_fd_quarter_card_calendar_window.md`, its degraded-mode
handler wraps `operational_schedule_for_mode` only); PP-064 (native-row Contract rule shared
with the quarter reader)

## Problem

`get_long_forecasts_quarter` resolves the request's `horizon_value` before doing anything
else. When `"quarter"` is a deployment-supported mode
(`ieasyhydroforecast_ml_long_term_supported_modes` includes `quarter`) but its `quarter.json`
config file is absent, that first call raises `FileNotFoundError` unconditionally — on every
call that omits an explicit `horizon_value`, which is every live caller. FD-029 adds a
degraded fallback (no native preference, no LR strictness, eligibility `date <= today`), but
it only wraps the later `operational_schedule_for_mode("quarter")` call. The earlier,
unguarded resolve still raises first, so the reservoir quarterly card
(`_get_data_monthly`, via `get_long_forecasts_quarter`) and all three bulletin blocks
(`bulletin_manager.py:395, 761, 891`) fail to load instead of degrading. Reproduced (not
guessed) on both trunk and the FD-029 branch.

## Evidence

- Trunk `82946683`: `apps/forecast_dashboard/src/db.py:816-819` —
  `get_long_forecasts_quarter(station=None, horizon_value=None)`'s first statement is
  `resolved_horizon_value = _resolve_quarter_horizon_value(horizon_value)` (`:819`).
- FD-029 branch (`fix_fd_quarter_card_calendar`, worktree `sapphire-fd029`,
  HEAD `5fe3c02c`): same call is still the function's first statement, now
  `apps/forecast_dashboard/src/db.py:851`, and still runs well before the degraded-mode
  `try`/`except (LongTermHorizonResolverError, FileNotFoundError)` at `:929-937`, which guards
  only `operational_schedule_for_mode("quarter")`.
- `_resolve_quarter_horizon_value` (`db.py:81-84`, unchanged by FD-029) calls
  `quarter_horizon_value()` whenever no explicit `horizon_value` is passed — true for the card
  and for all three bulletin call sites.
- `quarter_horizon_value()` → `_horizon_value_for_mode("quarter")`
  (`apps/iEasyHydroForecast/long_term_horizon_resolver.py:68-70, 78-81`) →
  `_ensure_supported_mode("quarter")` (passes, since `quarter` is supported) →
  `_load_long_term_config("quarter")` (`:181-198`), which raises `FileNotFoundError` at
  `:184` when `quarter.json` does not exist on
  `ieasyhydroforecast_ml_long_term_configuration`.
- Exception type: `FileNotFoundError`, uncaught at every call site above.

## Proposed fix

Degrade the same way as FD-029's existing handler: wrap the
`_resolve_quarter_horizon_value(horizon_value)` call (trunk `:819`, FD-029 branch `:851`) in a
`try`/`except (LongTermHorizonResolverError, FileNotFoundError)`, log one WARNING naming the
missing config, and fall back so the function still reaches its own degraded path instead of
raising (e.g. proceed without a `horizon_value` request filter under the flag-OFF request
shape, mirroring how the later handler falls back to `schedule = None`). Do **not** change
`_resolve_quarter_horizon_value`'s contract or signature for callers that pass an explicit
`horizon_value` — this fix touches only the one unguarded call inside
`get_long_forecasts_quarter`.

## Tests

- Quarter enabled, `quarter.json` absent, no explicit `horizon_value` →
  `get_long_forecasts_quarter()` returns (empty or degraded) rows, no exception; one WARNING
  logged naming the missing config.
- Same setup through `_get_data_monthly` (card path) and through one
  `bulletin_manager` block (e.g. `_populate_forecast_attributes`) → both complete without
  raising.
- Existing degraded-mode tests (a lead-only `quarter.json`, FD-029 test 11) are unaffected —
  this fix only widens which config problem degrades; it does not change behaviour when the
  file exists.

## Acceptance

- `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes, zero unexpected skips.
- No existing test edited; the new tests fail on both trunk and the FD-029 branch before the
  fix.
- `git diff --stat` limited to `apps/forecast_dashboard/src/db.py` and its tests.
- Station code `19999` in any fixture; no real station codes.
