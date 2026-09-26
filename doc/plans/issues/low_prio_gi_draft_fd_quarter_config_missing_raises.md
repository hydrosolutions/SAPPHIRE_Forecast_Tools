# FD-031: quarter enabled but its config cannot be resolved raises before FD-029's degraded mode

**Status**: Draft (2026-09-26)
**Module**: `apps/forecast_dashboard`
**Priority**: Low — pre-existing on trunk; not introduced by FD-029.
**Related**: FD-029 (`mid_prio_gi_draft_fd_quarter_card_calendar_window.md`, its degraded-mode
handler wraps `operational_schedule_for_mode` only); PP-064 (native-row Contract rule shared
with the quarter reader)

## Problem

`get_long_forecasts_quarter`'s second statement — after resolving the station code, and before
fetching data and before FD-029's schedule-resolution/degraded-mode handler — resolves the
request's `horizon_value` via `_resolve_quarter_horizon_value`. That call can raise for any of
several config problems on a deployment where `"quarter"` is a supported mode: `quarter.json`
absent (`FileNotFoundError`), invalid JSON in it, a missing/non-integer
`operational_month_lead_time` field, or (in principle, if reached with `quarter` not actually
listed as supported) `UnsupportedLongTermModeError` — all four raise from the same resolver
chain and none is caught here. FD-029 adds a degraded fallback (no native preference, no LR
strictness, eligibility `date <= today`), but it only wraps the *later*
`operational_schedule_for_mode("quarter")` call. The earlier, unguarded resolve still raises
first for any of the above, so the reservoir quarterly card (`_get_data_monthly`, via
`get_long_forecasts_quarter`) and all three bulletin blocks (`bulletin_manager.py:395, 761,
891`) fail to load instead of degrading. Reproduced (not guessed) on both trunk and the FD-029
branch, for the missing-file case.

## Evidence

- Trunk `28ee535d`: `apps/forecast_dashboard/src/db.py:819` —
  `code = _resolve_station(station) if station else None` (the function's first statement);
  `:820` — `resolved_horizon_value = _resolve_quarter_horizon_value(horizon_value)` (the
  **second** statement, not the first).
- FD-029 branch (`fix_fd_quarter_card_calendar`, worktree `sapphire-fd029`,
  HEAD `5fe3c02c`): the same two statements are now at `:851`/`:852` (station resolve, then the
  `horizon_value` resolve) — still both well ahead of the degraded-mode
  `try`/`except (LongTermHorizonResolverError, FileNotFoundError)` at `:929-938`, which guards
  only `operational_schedule_for_mode("quarter")`.
- `_resolve_quarter_horizon_value` (trunk `db.py:81-84`; FD-029 branch `db.py:82-85`, one line
  shifted, unchanged by FD-029) calls `quarter_horizon_value()` whenever no explicit
  `horizon_value` is passed — true for the card and for all three bulletin call sites.
- `quarter_horizon_value()` → `_horizon_value_for_mode("quarter")`
  (`apps/iEasyHydroForecast/long_term_horizon_resolver.py:68-70, 78-81`) →
  `_ensure_supported_mode("quarter")` (`:172-178`, raises `UnsupportedLongTermModeError` at
  `:175` — a `LongTermHorizonResolverError` subclass — if `quarter` is not supported) →
  `_load_long_term_config("quarter")` (`:181-198`), which raises `FileNotFoundError` at `:184`
  when `quarter.json` is absent, or `LongTermHorizonResolverError` at `:190` (invalid JSON) or
  `:195` (not a JSON object) → `_require_int_field` (`:158-169`), which raises
  `LongTermHorizonResolverError` at `:160` (field missing) or `:167` (field not an integer) for
  `operational_month_lead_time`.
- Exception types reaching `get_long_forecasts_quarter` uncaught: `FileNotFoundError` and every
  `LongTermHorizonResolverError` (including its `UnsupportedLongTermModeError` subclass) raised
  by the chain above.

## Proposed fix

Wrap the early `_resolve_quarter_horizon_value(horizon_value)` call (trunk `:820`, FD-029
branch `:852`) in `try`/`except (LongTermHorizonResolverError, FileNotFoundError)` — the same
exception set FD-029's later handler already catches, so both handlers agree on what
"degraded" means. On catch: fall back so the function still reaches its own degraded path
(e.g. proceed without a `horizon_value` request filter in `params`, rather than guessing a
value) instead of raising. State explicitly: this also degrades on invalid-config errors
(missing/invalid `operational_month_lead_time`, invalid JSON, an unsupported mode reached this
way) — not only a missing file. There is no promise that behaviour is unchanged whenever
`quarter.json` merely *exists*; it is unchanged only when the config resolves successfully.

**One-WARNING coordination (do not duplicate FD-029's later handler).** If the early resolve
fails, the function must not go on to call `operational_schedule_for_mode("quarter")` a second
time at the later handler (FD-029 branch `:930`) — that call would hit the *same* underlying
config problem and log a *second* WARNING for one root cause. Carry the early failure into the
degraded decision directly: when the early resolve already failed, skip the later
`operational_schedule_for_mode` call entirely, set `schedule = None` / `degraded = True` from
the early exception, and log exactly one WARNING (from the early catch). Only attempt
`operational_schedule_for_mode` when the early resolve succeeded. Do **not** change
`_resolve_quarter_horizon_value`'s contract or signature for callers that pass an explicit
`horizon_value` — this fix touches only the one unguarded call inside
`get_long_forecasts_quarter` and how its failure is threaded into the existing degraded-mode
decision.

## Tests

All fixtures below use non-empty API response data (an empty response short-circuits before
either handler runs, per FD-029's own empty-`df` branch, and would not exercise this fix).
Assert, for each case: no exception; exactly one WARNING logged; date eligibility behaves as
degraded (`date <= today`); LR rows are retained (degraded skips the LR-strictness filter);
`is_native` is False on every row; and the `horizon_value`/dedup output shape matches the
flag's expectations (with `horizon_value` in the dedup key under flag ON, without it under flag
OFF).

- `quarter.json` absent, no explicit `horizon_value`, non-empty API rows, flag OFF →
  `get_long_forecasts_quarter()` returns degraded rows, exactly one WARNING.
- Same, flag ON.
- Invalid config content instead of a missing file (e.g. `quarter.json` present but missing
  `operational_month_lead_time`, or containing invalid JSON), non-empty API rows, one flag
  state → same degraded outcome, exactly one WARNING (proves the exception-set widening did not
  introduce a second warning path either).
- Same setup through `_get_data_monthly` (card path) and through one `bulletin_manager` block
  (e.g. `_populate_forecast_attributes`) → both complete without raising.
- Existing degraded-mode tests (a lead-only `quarter.json`, FD-029 test 11) are unaffected —
  this fix only widens which config problem degrades at this one call site; behaviour when the
  config resolves successfully is unchanged.

## Acceptance

- `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes, zero unexpected skips.
- No existing test edited; the new tests fail on both trunk and the FD-029 branch before the
  fix.
- `git diff --stat` limited to `apps/forecast_dashboard/src/db.py` and its tests.
- Station code `19999` in any fixture; no real station codes.
