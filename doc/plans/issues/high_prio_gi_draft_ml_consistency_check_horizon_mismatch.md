# ML-024: `SAPPHIRE_CONSISTENCY_CHECK` verifies nothing for pentad/decad ML forecasts

**Status**: Draft (2026-09-09)
**Module**: `apps/machine_learning` (`scr/utils_ml_forecast.py`)
**Priority**: High — the check is opt-in, but for every deployment that opts in
(`SAPPHIRE_CONSISTENCY_CHECK=true`) it reports "consistent" while reading back
zero rows, on every pentad/decad run, unconditionally. A verification mechanism
that always passes is worse than no mechanism, because it is trusted.
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
forecast rows with a literal `"horizon_type": "day"` (`:818`; the sibling
`_write_ml_daily_forecast_to_api` does the same at `:925`). This is **deliberate,
not a bug** — the function's own docstring says so:

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
Since every row was written with `horizon_type="day"`, a read filtered on
`horizon="pentad"` or `horizon="decade"` **always returns empty**, regardless of
whether the write succeeded, partially succeeded, or wrote corrupted values.
`api_data.empty` is then true, the function logs `"No API data found for
consistency check"` (`:1026`) and **returns `True`** (`:1027`) — the same value
it returns for a genuine successful comparison.

## Reachability — confirmed live whenever the flag is set, in every deployment

This is not a developer-only edge case: `_check_ml_forecast_consistency` is
called on **every** pentad/decad write when `SAPPHIRE_CONSISTENCY_CHECK=true`
(`make_forecast.py:200`/`:283`, unconditional whenever the write itself
succeeded), which is any deployment or ad hoc run that sets that flag — for
example `apps/machine_learning/locally_run_ml_forecasts.sh:49` sets it
unconditionally. No org-specific gating, no mode restriction: this is a plain
string mismatch between the write path and the read path, always live when the
flag is on.

**Not caught by the existing test suite.** `test_api_integration.py`'s
`TestCheckMLForecastConsistency` (`:618`, `:653`, `:687`; verified by reading the
file) mocks `client.read_forecasts` directly and has it return matching or
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
an always-empty read. The fix must state explicitly which horizon the read-back
should use and why, since `_write_ml_daily_forecast_to_api` (`:925`) writes the
same `"day"` value from a different caller — any fix here should keep that
caller working too.

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
