# ML-024: `SAPPHIRE_CONSISTENCY_CHECK` verifies nothing for pentad/decad ML forecasts

**Status**: Draft (2026-09-09)
**Module**: `apps/machine_learning` (`scr/utils_ml_forecast.py`)
**Priority**: Medium — the check is opt-in (`SAPPHIRE_CONSISTENCY_CHECK`
defaults to `false`, `scr/utils_ml_forecast.py:976`), and the only place in this
repository that sets it to `true` is `apps/machine_learning/locally_run_ml_forecasts.sh:49`
— itself a non-operational, developer-only script (see ML-025's Reachability
section, which reaches the same conclusion independently). Its check's Boolean
result is also discarded at the caller (`make_forecast.py:201`/`:284` never use
the return of `_check_ml_forecast_consistency`), so a mismatch only produces a
log line, never a failed run. No repository evidence shows a deployment
enabling and monitoring this flag. **What would justify raising this back to
High**: a deployment shown (crontab, `.env_develop_<org>`, or a documented
runbook) to set `SAPPHIRE_CONSISTENCY_CHECK=true` operationally and to alert on
its log output — until then, the worst-case blast radius is a misleading log
line, not a masked production failure.
**Labels**: `ml`, `api`, `silent-success`, `consistency-check`
**Found**: 2026-09-08/09, out-of-loop review of ML-021 (PR #503). Listed as
"deliberately not addressed" defect 1 in
[`review_gi_draft_ml_forecast_api_write_silent_success.md`](review_gi_draft_ml_forecast_api_write_silent_success.md)
(`## Deliberately not addressed`); this file expands it to a standalone issue.
**Pre-existing**: yes — confirmed present before and unchanged by ML-021. ML-021 gave
the consistency-check `try` its own exception boundary (commit `1bded0cb`) but did
not touch what it reads or compares.

---

## Defect

`_write_ml_forecast_to_api` (`scr/utils_ml_forecast.py:713`) always stores ML
forecast rows with a literal `"horizon_type": "day"` (`:818`). A sibling
function, `_write_ml_daily_forecast_to_api` (`:866`), does the same at `:925`,
but a repo-wide search finds **no call site for it anywhere** — it appears to be
dead code. It is mentioned here only because its docstring and behavior are
otherwise identical, not because it has a live caller; it is out of scope for
this issue. This is **deliberate, not a bug** — `_write_ml_forecast_to_api`'s
own docstring says so:

```
All daily forecasts are stored with horizon_type="day" regardless of
the caller's horizon_type. The horizon_type parameter is retained for
backward compatibility but is informational only — it indicates whether
the caller is producing pentad or decade forecasts, but storage always
uses "day" with day-of-year horizon values.
```

(`scr/utils_ml_forecast.py:717-721`.)

`_check_ml_forecast_consistency` (`:958`) reads back with the caller's raw
`horizon_type` string instead:

```python
page = client.read_forecasts(
    horizon=horizon_type,   # :1003 — inside the per-code loop
    code=code,
    ...
)
```

and the no-codes fallback branch does the same (`:1017`). `make_forecast.py`
calls the check with `horizon_type="pentad"` or `"decade"`
(`write_pentad_forecast`/`write_decad_forecast`, `make_forecast.py:201`/`:284`).
Since every row from `_write_ml_forecast_to_api` is written with
`horizon_type="day"`, a read filtered on `horizon="pentad"` or `horizon="decade"`
**will usually return empty**, regardless of whether the write succeeded,
partially succeeded, or wrote corrupted values. When it is empty, `api_data.empty`
is true, the function logs `"No API data found for consistency check"` (`:1026`)
and **returns `True`** (`:1027`) — the same value it returns for a genuine
successful comparison.

**"Usually empty" is not "always empty" — a second, worse failure mode exists.**
Two other write paths in this repo create ML rows with a literal `horizon_type`
of `"pentad"`/`"decade"` instead of `"day"`:
`sapphire/services/postprocessing/app/data_migrator.py`'s
`ForecastDataMigrator.prepare_pentad_data`/`prepare_decade_data` (`:334-378`,
one-time CSV-to-API migration tooling for the `--type forecast` migration path),
and `bin/utils/migration_py/ml_forecast.py`'s `_build_record` under its opt-in
`--preserve-legacy-ml-horizons` flag (`:208-275`, an active repository script).
Both are confirmed by
`apps/iEasyHydroForecast/tests/test_initialize_ml_forecast.py::test_build_record_preserves_legacy_pentad_horizon_with_flag`
and `..._decade_horizon_with_flag` (`:415-444`), which pin the flag preserving
`horizon_type="pentad"`/`"decade"` in the built API payload. Where such
legacy-horizon rows exist for the same station code, `_check_ml_forecast_consistency`'s
read finds them instead of nothing — the comparison then runs against
**unrelated rows from a different write path**, producing a false "consistent"
verdict built from comparing today's write against a stale or unrelated legacy
row, rather than from comparing nothing at all. The empty-read /
returns-`True`-unconditionally behavior above is what happens only when no such
legacy rows exist for that code.

## Reachability — confirmed live whenever the flag is set, in every deployment

This is not a developer-only edge case, but the real trigger is narrower than
"whenever the write succeeded": `write_pentad_forecast`/`write_decad_forecast`
(`make_forecast.py:183-202`, `:266-285`) set a local `write_succeeded = True`
after `_write_ml_forecast_to_api(...)` returns **without raising** —
they do not check its returned Boolean. `_write_ml_forecast_to_api` returns
`False` (not an exception) for several benign no-ops, including an empty input
DataFrame, `SAPPHIRE_API_ENABLED=false`, and the client library being absent
(`scr/utils_ml_forecast.py:750-773`); none of those raise, so `write_succeeded`
is `True` and `_check_ml_forecast_consistency` runs in all of them too, not only
after a genuine write. The real trigger is: **any non-raising invocation of
`_write_ml_forecast_to_api` while `SAPPHIRE_API_AVAILABLE` is `True`** — call it
succeeded, no-op, or wrote zero rows, so long as it did not raise. Conversely,
when the client library is absent (`SAPPHIRE_API_AVAILABLE=False`) the whole
block, including the consistency check, is skipped by the outer
`if SAPPHIRE_API_AVAILABLE:` guard (`make_forecast.py:182`/`:265`). This is any
deployment or ad hoc run that sets `SAPPHIRE_CONSISTENCY_CHECK=true` while the
client library is importable — for example
`apps/machine_learning/locally_run_ml_forecasts.sh:49` sets it unconditionally.
No org-specific gating, no mode restriction: this is a plain string mismatch
between the write path and the read path, live whenever the flag is on and the
check is reached.

**Open question for the fix (not decided here):** should
`_check_ml_forecast_consistency` be gated on `_write_ml_forecast_to_api`'s
returned Boolean (skip the check on a benign no-op, since there is nothing to
verify), or should it keep running unconditionally on any non-raising call? The
current code effectively does the latter by discarding the return value; a fix
to the horizon key alone (see Desired outcome) does not resolve this on its own.

**Not caught by the existing test suite.** `test_api_integration.py`'s
`TestCheckMLForecastConsistency` (tests
`test_consistency_check_passes_on_match`,
`test_consistency_check_fails_on_row_count_mismatch`,
`test_consistency_check_fails_on_value_mismatch`; verified by reading the file)
mocks `client.read_forecasts` directly and has it return matching or
mismatching data **regardless of the `horizon` argument passed** — none of those
tests assert on what `horizon=` value the function calls `read_forecasts` with,
so they cannot catch a horizon-key mismatch between the write and the read. This
is the actual gap: the tests exercise the comparison logic once data is
returned, not whether the read is scoped to find any data at all.

## Not to be confused with ML-005 (Complete)

ML-005 (`archive/review_gi_draft_pp_org_scoped_data_readers.md`, Phase 4) fixed a
**different** defect in this same function: the read had no station-code filter
at all. The current code already loops per station code (`:1000-1012`) — that
fix is in place and working. This issue is about the **horizon** key, not the
code filter; do not treat one as covering the other.

## Desired outcome

`_check_ml_forecast_consistency` reads back using the same `horizon_type` value
the write actually used (`"day"`), not the caller-supplied `"pentad"`/`"decade"`
label, so a real write failure or corruption produces a real mismatch instead of
a read that either finds nothing or finds an unrelated legacy row. The fix must
state explicitly which horizon the read-back should use and why.
`_write_ml_daily_forecast_to_api` (`:925`) writes the same `"day"` value but, as
noted above, has no confirmed caller and no tests reference it — a fix here has
no compatibility obligation toward it.

## Out of scope

- Whether a genuine consistency *mismatch* should fail the run — recorded as an
  open owner question in ML-021's file, not decided or touched here.
- `_write_ml_forecast_to_api`'s write behavior — unchanged and correct per its
  own docstring; only the read-back key is wrong.
- ML-005's station-code filtering — already fixed, not reopened here.

## Acceptance criteria

- [ ] `_check_ml_forecast_consistency` reads back with `horizon="day"`
      regardless of the caller's `horizon_type` argument, matching what
      `_write_ml_forecast_to_api` actually stored.
- [ ] A test pins that, with a real (non-empty) API-side row for the written
      station/date, `_check_ml_forecast_consistency("pentad", ...)` reaches
      the comparison branch instead of the empty-result `"No API data found"`
      branch — i.e. the read is proven to be capable of finding data at all,
      not just that the comparison logic is correct once data is handed to it.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures,
      zero unexpected skips.

---

## Related

| ID | Relation |
|---|---|
| ML-021 | Source issue; this is "deliberately not addressed" defect 1 there |
| ML-005 | Complete — fixed the station-code filter on the same function; different defect |
