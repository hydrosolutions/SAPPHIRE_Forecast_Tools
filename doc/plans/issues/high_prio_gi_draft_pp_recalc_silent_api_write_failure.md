# PP-051 — Long-term skill-metrics recalc reports success when the API write fails

**Status**: Draft
**Module**: postprocessing_forecasts (`file_writer.py`, `recalculate_skill_metrics.py`)
**Priority**: High
**Labels**: `reliability`, `api-integration`, `data-integrity`, `false-success`

---

## Summary

`recalculate_skill_metrics.py` can complete every configured mode and `sys.exit(0)`
while zero rows reached Postgres — for example when the API is unreachable or
`SAPPHIRE_API_ENABLED` is false. The failure mode responsible
(`SAPPHIRE_API_FAILURE_MODE`) defaults to `"warn"`, not `"raise"`
(`apps/iEasyHydroForecast/forecast_library.py:88`), and every `save_*` function
in `file_writer.py` that performs the API write wraps it in a bare
`try/except Exception` that routes to `fl._handle_api_write_error(...)` and then
unconditionally `return None` — regardless of whether the write succeeded.

The caller (`recalculate_skill_metrics.py`) treats `ret is None` as **success**:

```python
ret = file_writer.save_monthly_skill_metrics(monthly_skill, year=skill_metrics_year)
if ret is None:
    logger.info(f"{label} skill metrics saved successfully.")
else:
    logger.error(f"Error saving {label} skill metrics: {ret}")
```

Because every `save_*` function returns `None` on **both** the success path and
the swallowed-exception path (under the default `warn` mode), this check cannot
distinguish them. The same pattern repeats at the pentad/decad, monthly,
quarterly, seasonal, and daily call sites in `recalculate_skill_metrics.py`
(lines ~240, ~361, ~430, ~513, and the daily block ~549-566 — the daily block's
own `try/except` around the call is equally defeated, since the exception it's
waiting for is already swallowed one level down).

## Evidence

- `apps/iEasyHydroForecast/forecast_library.py:88` — default failure mode is
  `"warn"`.
- `apps/iEasyHydroForecast/forecast_library.py:95-116` — `_handle_api_write_error`
  only re-raises when mode is `"fail"`; `"warn"` logs and returns (no exception
  propagates to the caller).
- `apps/postprocessing_forecasts/src/file_writer.py:665-671`
  (`save_quarterly_skill_metrics`) and `:701-705` (`save_seasonal_skill_metrics`)
  — **API-only, no CSV fallback at all**. If the API write fails under `warn`
  mode, nothing is persisted anywhere and the function still returns `None`.
- `apps/postprocessing_forecasts/src/file_writer.py:429-433`
  (`save_monthly_skill_metrics`) — CSV write is conditional on
  `ieasyforecast_intermediate_data_path`/`ieasyforecast_monthly_skill_metrics_file`
  being set (`:418-427`); if either is unset, CSV is skipped with only a
  `logger.warning`, no exception. Combined with a swallowed API failure, monthly
  skill metrics can be dropped entirely with **zero raised exceptions** anywhere
  in the call chain.
- `apps/postprocessing_forecasts/src/file_writer.py:338-345`
  (`save_skill_metrics`, pentad/decad) — CSV write is unconditional and *does*
  raise on failure (`:334-336`), so pentad/decad always has at least the CSV as
  a fallback record. Quarterly/seasonal have no equivalent fallback.
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py:240-245, 361-365,
  430-436, 513-519, 549-566` — every save call is gated on `ret is None`, and
  `errors.append(...)` only fires on the (unreachable, under `warn`) non-`None`
  branch.
- `SAPPHIRE_CONSISTENCY_CHECK` (`file_writer.py:348-378`, `:435-461`) does **not**
  close this gap: `_verify_preprocessing_write_consistency`
  (`apps/iEasyHydroForecast/forecast_library.py:3166-3206`) compares the
  in-memory `written_data` DataFrame against the **CSV file it just wrote** —
  it never reads back from the API/DB, so it cannot detect an API write
  failure. It is also opt-in (default `false`) and only wired for pentad/decad
  and monthly — quarterly and seasonal have no CSV to check against in the
  first place.

## Impact

- The deployment runbook's Phase 3 (`doc/prod/long_term_deploy_runbook.md`
  "Regenerate (full-history recalc)") and the bimonthly cron
  (`bin/bimonthly_long_term_skill_metrics_recalculation.sh`) both treat a clean
  exit as evidence the recalc landed in the DB. `bimonthly_long_term_skill_metrics_recalculation.sh`
  itself only inspects the wrapped script's **exit status** per mode
  (`rc=$?` at line 108) — it has no independent signal and cannot see through
  this gap even in principle.
- A transient API outage, a misconfigured `SAPPHIRE_API_URL`, or
  `SAPPHIRE_API_ENABLED=false` during a scheduled or manual recalc produces a
  green log (`"Script finished successfully"`, exit 0) with the DB unchanged.
- The runbook's own Phase 4 verification queries (see PP/DOC companion issue on
  Phase 4 coverage) read `long_forecasts`, not `skill_metrics` — so this failure
  mode is invisible to the documented stop-gate as well as to the wrapper's exit
  code.
- Quarterly and seasonal skill metrics are the most exposed: no CSV fallback
  exists, so a swallowed API failure there is a **total** loss of that recalc's
  output, not a degraded one.

## Why the `warn` default may be intentional (read before proposing "just raise")

`_handle_api_write_error`'s `warn` default plausibly exists so that a
**transient** API blip does not abort a long-running cron job partway through —
LR-007 (`archive/review_gi_draft_lr_api_write_loud_failure.md`, Complete) hit
the identical tension for `linear_regression.py` and explicitly preserved
"complete everything, then report" as the desired default behavior, only adding
a **post-completion** failure signal (non-`None`/`bool` return value checked by
the caller) rather than changing the exception-raising default. Any fix here
should preserve "the recalc still processes every mode/horizon" — the actual
defect is that the **existing** post-completion check (`ret is None`) is
structurally incapable of seeing a swallowed failure, not that failures are
swallowed per se.

## Investigation still needed (do not implement without this)

1. Confirm whether any `save_*` function in `file_writer.py` has a viable
   "API write attempted and failed" signal available at its return boundary
   today (none currently does — all six long-term/short-term `save_*` skill
   and forecast functions return bare `None`).
2. Confirm the full list of `save_*` functions affected — this draft has
   checked `save_skill_metrics`, `save_monthly_skill_metrics`,
   `save_quarterly_skill_metrics`, `save_seasonal_skill_metrics`, and the daily
   skill metrics block; `save_forecast_data`, `save_monthly_forecast_data`,
   `save_quarterly_forecast_data`, and `save_seasonal_forecast_data` follow the
   same `try/except -> _handle_api_write_error -> return None` shape by
   inspection of the surrounding code and should be re-checked line-by-line
   before any fix is scoped.
3. Confirm whether `bin/bimonthly_long_term_skill_metrics_recalculation.sh`
   and/or the direct-form invocation in the deploy runbook should also gain an
   independent post-hoc DB check (e.g. a lightweight row-count probe) as a
   second line of defense, separate from the Python-side fix.

## Proposed options (owner to choose — not decided here)

**Option A — Mirror LR-007's return-value pattern.** Change the affected
`save_*` functions to return a `bool` (`api_ok`), set `False` only when
`_handle_api_write_error` is reached (i.e., an exception was actually caught),
and update `recalculate_skill_metrics.py`'s `if ret is None:` checks to
`if ret is True:` / equivalent. This is the smallest, most precedented change
(same mechanism already shipped and verified for `linear_regression.py`) and
requires no change to the default failure mode.

**Option B — Change the default `SAPPHIRE_API_FAILURE_MODE` to `"fail"` for
this entry point only.** Riskier: an operator-visible behavior change (a
transient API blip now aborts the whole recalc mid-run, per the LR-007
precedent's own Phase-1 analysis of `fail` mode's consequences), and it changes
shared, cross-module default behavior (`_handle_api_write_error` is used by
LR, ML-adjacent, and other postprocessing call sites) rather than scoping the
fix to the recalc entry point.

**Option C — Add an independent post-recalc verification step** (row-count or
`MAX(date)` probe against the DB) inside
`bin/bimonthly_long_term_skill_metrics_recalculation.sh` or as a follow-up to
Phase 3 in the deploy runbook, without changing the Python return contract.
Cheaper to ship but does not fix the root cause (the exit code is still wrong
for ad-hoc / non-wrapped invocations of `recalculate_skill_metrics.py`).

**Recommendation for the owner to confirm, not a decision made here:** Option A
mirrors a shipped, reviewed precedent (LR-007) and preserves the "complete
every mode, then report" behavior the `warn` default protects. Option C is a
reasonable defense-in-depth addition regardless of which of A/B is chosen.
Changing the default failure mode (Option B) is explicitly **out of scope**
for this draft — it is a cross-module, operator-visible behavior change and
needs owner sign-off, not a unilateral pick.

## Out of scope / notes

- No `sapphire/services/` change — this is entirely `apps/` behavior.
- Any implementation must preserve the "process every mode, then exit"
  contract that `warn` mode currently provides — do not turn this into a
  mid-run abort without explicit owner sign-off (see LR-007 Phase 2 notes on
  why `fail` mode was kept operator-opt-in, not default).
- A fix should add regression tests asserting: (a) a mocked API failure under
  default `warn` mode makes the affected `save_*` function return a
  false-y/failure signal, (b) `recalculate_skill_metrics.py` exits non-zero
  when any such signal is seen, (c) CSV write behavior (where CSV exists) is
  unchanged in `warn` mode.

## References

- `apps/iEasyHydroForecast/forecast_library.py:82-116`
- `apps/postprocessing_forecasts/src/file_writer.py:291-707` (all `save_*`
  skill-metrics functions)
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py:228-587`
- `bin/bimonthly_long_term_skill_metrics_recalculation.sh:104-115`
- `doc/prod/long_term_deploy_runbook.md` § "Phase 3 — Regenerate"
- Precedent: `doc/plans/issues/archive/review_gi_draft_lr_api_write_loud_failure.md`
  (LR-007, Complete) — same defect class, already fixed for `linear_regression.py`
  via the return-`bool` pattern (Option A here).
