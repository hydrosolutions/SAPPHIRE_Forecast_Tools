# LR-008: Align LR `horizon_in_year` metadata with target-period convention

**Status**: Draft
**Module**: `linear_regression`
**Priority**: High
**Labels**: `data-integrity`, `api-integration`

---

## Summary

The LR pipeline writes `horizon_in_year` to the API using the **issue-date** pentad/decad (e.g., 17 on March 25), while the ML pipeline and downstream consumers expect the **target-period** pentad/decad (18 on March 25). The forecast values, training data, norm discharge, and visibility lookups are all correct — only the DB metadata tag is misaligned.

---

## Misdiagnosis Note

The original LR-008 plan (drafted 2026-03-25) concluded that `forecast_pentad_of_year = tl.get_pentad_in_year(current_day)` was wrong and should use `current_day + 1`. Investigation on 2026-03-26 proved this was **incorrect**:

The LR DataFrame convention stores rows under the **issue date**. `get_pentadal_and_decadal_data()` (`forecast_library.py:1013`) assigns `pentad_in_year` from each row's own date. A row dated March 25 has `pentad_in_year = 17` and carries:
- `discharge_sum` (predictor X): 3-day sum ending on March 25 (the issue date)
- `discharge_avg` (predictand y): 5-day average of March 26–31 (the target period, pentad 18)

Therefore `forecast_horizon_int = 17` is the **correct** key for:
- `save_discharge_avg(group_id="17")` — selects historical March 25 rows whose `discharge_avg` is the March 26–31 norm
- `perform_linear_regression(forecast_horizon_int=17)` — filters training data to all historical pentad-17 issue dates
- `perform_forecast(group_id="17")` — looks up regression results by issue pentad

Changing `forecast_horizon_int` to 18 would select rows from March 26–31 issue dates, whose `discharge_avg` covers April 1–5 — the wrong training data entirely.

The `+1 day` offset in the visibility lookup (`forecast_library.py:1679`) is also **correct** for the current code: `forecast_date_str` = last day of the issue pentad (March 25), `+1 day` = first day of the target pentad (March 26), and the derived `(month, pentad_in_month)` correctly queries the target period's visibility key.

---

## Problem Description

A forecast produced on March 25 targets pentad 18 (March 26–31).

- **ML writes**: `horizon_in_year = 18` (target-period convention)
- **LR writes**: `horizon_in_year = 17` (issue-date convention)

The postprocessing normalizers (`_normalize_lr_forecasts` at `data_reader.py:1751`) discard `horizon_in_year` and recompute `pentad_in_year` from `date + 1 day`, so EM/NE joins are unaffected.

However, the **forecast dashboard** reads `horizon_in_year` directly from the API without recomputing. Affected locations:
- `db.get_data()` — skill metrics merge on `pentad_in_year`
- `vizualization.py` — skill plot grouping, predictor date display
- `utils.py` — station attribute filter
- `data_manager.py` — current-period detection

---

## Root Cause

In `linear_regression.py`, `linreg_pentad["pentad_in_year"]` retains the issue-date value (17) all the way through to `_write_lr_forecast_to_api`, which reads it as `horizon_in_year`. No override to the target-period value exists.

---

## Implementation Plan

### Phase 1: Metadata override before API/CSV write

**Goal**: Override `pentad_in_year` / `decad_in_year` in the output DataFrame to the target period immediately before the write call, without changing any upstream computation.

**Files allowed to modify**:
- `apps/linear_regression/linear_regression.py`

**Changes**:

1. Pentad path — insert after `perform_forecast` / rename block and before `write_linreg_pentad_forecast_data` call:

```python
# Override pentad_in_year and pentad_in_month for API/CSV: the DB fields
# horizon_in_year and horizon_value must reflect the TARGET period (the
# period being forecast), not the issue date's period. The LR DataFrame
# convention stores rows under the issue date, so pentad_in_year =
# issue_pentad. We convert to target_pentad here, only for the write.
# forecast_pentad_of_year (used for training, norms, visibility) is
# unchanged.
# Dec 31 edge case: issue pentad 72 wraps to target pentad 1.
# Note: the 4 hydrograph/timeseries write functions already recompute
# pentad_in_year from date + 1 day internally, so only the forecast
# write needs this override.
_issue_pentad = int(forecast_pentad_of_year)
_target_pentad = 1 if _issue_pentad == 72 else _issue_pentad + 1
linreg_pentad["pentad_in_year"] = str(_target_pentad)
linreg_pentad["pentad_in_month"] = str(((_target_pentad - 1) % 6) + 1)
```

2. Decad path — insert before `write_linreg_decad_forecast_data` call:

```python
# Same override for decad. Issue decad 36 wraps to target decad 1.
_issue_decad = int(forecast_decad_of_year)
_target_decad = 1 if _issue_decad == 36 else _issue_decad + 1
linreg_decad["decad_in_year"] = str(_target_decad)
linreg_decad["decad_in_month"] = str(((_target_decad - 1) % 3) + 1)
```

**CRITICAL CONSTRAINT**: Do NOT change `forecast_pentad_of_year`, `forecast_decad_of_year`, or any call to `save_discharge_avg`, `perform_linear_regression`, `perform_forecast`. The override is purely a pre-write column mutation.

**Acceptance criteria**:
- On March 25 (issue pentad 17): `horizon_in_year` = 18, `horizon_value` = 6
- On March 10 (issue decad 7): `horizon_in_year` = 8, `horizon_value` = 2
- On Dec 31 (issue pentad 72): `horizon_in_year` = 1, `horizon_value` = 1
- On Dec 31 (issue decad 36): `horizon_in_year` = 1, `horizon_value` = 1
- `save_discharge_avg`, `perform_linear_regression`, `perform_forecast` still receive issue-date key
- All existing tests pass

---

### Phase 2: Protective tests

**Goal**: Verify and protect both the upstream issue-date convention and the new target-date metadata.

**Files allowed to modify**:
- `apps/linear_regression/test/test_horizon_metadata.py` (new file)

**Test matrix**:

| # | Category | Description | Assertion |
|---|----------|-------------|-----------|
| 1 | Convention | `pentad_in_year` on historical rows = issue date's pentad | Row dated Mar 25 has `pentad_in_year == "17"` |
| 2 | Convention | Training filter uses issue pentad | `perform_linear_regression(forecast_horizon_int=17)` filters to `pentad_in_year == 17` rows |
| 3 | Upstream isolation | `save_discharge_avg` receives issue pentad as `group_id` | Mock: `group_id == "17"` on March 25 |
| 4 | Upstream isolation | `perform_linear_regression` receives `forecast_horizon_int == 17` on March 25 | Mock: `forecast_horizon_int == 17` |
| 5 | Metadata fix (pentad) | March 25: write receives target metadata | `pentad_in_year == "18"`, `pentad_in_month == "6"` |
| 6 | Metadata fix (pentad) | March 5: write receives target metadata | `pentad_in_year == "14"`, `pentad_in_month == "2"` |
| 7 | Metadata fix (pentad non-boundary) | March 12: write receives target metadata | `pentad_in_year == "16"`, `pentad_in_month == "4"` |
| 8 | Metadata fix (decad) | March 10: write receives target metadata | `decad_in_year == "8"`, `decad_in_month == "2"` |
| 9 | Metadata fix (decad) | March 20: write receives target metadata | `decad_in_year == "9"`, `decad_in_month == "3"` |
| 10 | Dec 31 wrap (pentad) | Issue pentad 72 → target pentad 1 | `pentad_in_year == "1"`, `pentad_in_month == "1"` |
| 11 | Dec 31 wrap (decad) | Issue decad 36 → target decad 1 | `decad_in_year == "1"`, `decad_in_month == "1"` |
| 12 | Month boundary (pentad) | Jan 31 (issue pentad 6) → target pentad 7 | `pentad_in_year == "7"`, `pentad_in_month == "1"` (Feb) |
| 13 | Month boundary (pentad) | May 31 (issue pentad 30) → target pentad 31 | `pentad_in_year == "31"`, `pentad_in_month == "1"` (Jun) |
| 14 | Leap year | Feb 29 (issue pentad 12) → target pentad 13 | `pentad_in_year == "13"`, `pentad_in_month == "1"` (Mar) |
| 15 | Internal consistency | `horizon_in_year` and `horizon_value` are consistent | For all test cases: `(pentad_in_year - 1) // 6 + 1` = month, `(pentad_in_year - 1) % 6 + 1` = `pentad_in_month` |

**Integration smoke test**: Mock `fl.save_discharge_avg` and `fl.perform_linear_regression`. Call the pipeline pentad path with `forecast_date=date(2026, 3, 25)`. Assert: (a) `save_discharge_avg` received `group_id="17"`, (b) `perform_linear_regression` received `forecast_horizon_int=17`, (c) the DataFrame passed to `write_linreg_pentad_forecast_data` has `pentad_in_year == "18"` and `pentad_in_month == "6"`.

**CRITICAL CONSTRAINT**: Tests use `SAPPHIRE_TEST_ENV=True`, mock API clients, no live filesystem writes outside `tmp_path`.

**Acceptance criteria**:
```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh linear_regression
```
All tests pass. Zero skips (except valid `SAPPHIRE_API_AVAILABLE` dependency-gate skips).

---

### Phase 3: Documentation

**Goal**: Record the issue-date indexing convention and the misdiagnosis so future developers don't re-open the wrong fix.

**Files allowed to modify**:
- `apps/iEasyHydroForecast/src/iEasyHydroForecast/forecast_library.py` — docstring addition only
- `doc/data_flow_short_term.md` — new section

**Changes**:

1. **`perform_linear_regression` docstring** — append to the existing docstring:

```
    Note on issue-date indexing convention:
        The input DataFrame (data_df) carries ``pentad_in_year`` values computed
        from each row's own date — the **issue date** of that historical forecast,
        not the target period. A row dated 2026-03-25 has ``pentad_in_year = 17``
        and ``discharge_avg`` equal to the mean of March 26–31 (the target, pentad
        18). Therefore ``forecast_horizon_int = 17`` is correct when forecasting on
        March 25: it selects all historical March 21–25 issue dates, each carrying
        the correct target's discharge average.

        The ``horizon_in_year`` metadata written to the API is overridden to the
        target pentad (18) in ``linear_regression.py`` after this function returns.
        This function must NOT receive the target pentad as ``forecast_horizon_int``.
```

2. **`doc/data_flow_short_term.md`** — add a new section "LR Issue-Date Indexing Convention" in the Linear Regression section:

```markdown
#### LR Issue-Date Indexing Convention

The LR module indexes training data by the **issue date** (the date the forecast
is produced), not the target period. `get_pentadal_and_decadal_data()` assigns
`pentad_in_year` from each row's own date. A row dated March 25 has
`pentad_in_year = 17`, while its `discharge_avg` column holds the mean discharge
of March 26–31 (the target period, pentad 18).

This means `forecast_horizon_int = 17` (the issue pentad) is the correct filter
key for training data and norm discharge on March 25. **Do not change it to 18.**

The ML pipeline uses a different convention: `horizon_in_year = 18` (the target
pentad). The discrepancy is resolved by a metadata override in
`linear_regression.py` that increments `pentad_in_year` by 1 immediately before
the API write, without changing any upstream computation.

| Layer | LR value | ML value | Convention |
|-------|----------|----------|------------|
| Training data filter | 17 | n/a | Issue-date |
| Norm discharge lookup | 17 | n/a | Issue-date |
| Visibility query | Correct (uses +1 day from issue pentad's last day) | n/a | Issue-date |
| API `horizon_in_year` | 18 (after override) | 18 | Target-date |
| API `horizon_value` | 6 (after override) | 6 | Target-date |
```

**Acceptance criteria**:
- Docstring is added without changing function signature or behavior
- `data_flow_short_term.md` section explains the convention clearly

---

## Acceptance Criteria (overall)

- [ ] `horizon_in_year` written to API = target pentad (18 on March 25, not 17)
- [ ] `horizon_value` written to API = target pentad-in-month (6 on March 25, not 5)
- [ ] `horizon_in_year` and `horizon_value` are internally consistent for all cases
- [ ] `forecast_pentad_of_year` passed to upstream functions is unchanged (17 on March 25)
- [ ] Dec 31 edge case handled: pentad 72 → 1, decad 36 → 1
- [ ] 14 protective tests pass, including upstream isolation guards
- [ ] Full linear_regression test suite passes with zero skips
- [ ] Convention documented in `perform_linear_regression` docstring and `data_flow_short_term.md`
- [ ] No changes to `forecast_library.py` logic (docstring only)
- [ ] No changes to `sapphire/services/` without coordination

---

## Historical Data

The API upsert key is `(horizon_type, code, date)` — `horizon_in_year` is NOT part of the unique key. Historical rows have `horizon_in_year` = issue pentad (off by one from the target convention). Options:

- **Option A (recommended)**: Let future runs overwrite via upsert. After deploying the fix, any re-run (operational or hindcast) will write the correct target pentad. No immediate migration needed.
- **Option B (deferred)**: One-time SQL UPDATE: `SET horizon_in_year = CASE WHEN horizon_in_year = 72 THEN 1 ELSE horizon_in_year + 1 END` for pentad LR rows on boundary dates. Safe because `horizon_in_year` is not part of the upsert key.

Document the chosen approach in the PR.

---

## Related Issues

- **LR-009**: Closed as invalid — was predicated on the original (wrong) LR-008 fix that would have changed `forecast_horizon_int` to the target pentad. Since the rescoped fix does not change `forecast_horizon_int`, the Dec 31 `_year` corruption scenario cannot occur.
- **INFRA-015**: Pentad/decade boundary audit — this was the only finding; downgraded to medium priority.

---

## Dependency Graph

```json
{
  "phases": {
    "P1": {
      "title": "Metadata override before API/CSV write",
      "file": "apps/linear_regression/linear_regression.py",
      "depends_on": [],
      "parallel_agents": 1
    },
    "P2": {
      "title": "Protective tests (14 tests + integration smoke test)",
      "file": "apps/linear_regression/test/test_horizon_metadata.py",
      "depends_on": ["P1"],
      "parallel_agents": 1
    },
    "P3": {
      "title": "Documentation: docstring + data_flow_short_term.md",
      "files": ["apps/iEasyHydroForecast/forecast_library.py", "doc/data_flow_short_term.md"],
      "depends_on": ["P1"],
      "parallel_agents": 1
    }
  },
  "execution_groups": [
    { "group": 1, "phases": ["P1"] },
    { "group": 2, "phases": ["P2", "P3"], "note": "Can run in parallel after P1" }
  ]
}
```

---

*Rescoped 2026-03-26: Original diagnosis was wrong — training data and norms are correct under the issue-date convention. Fix narrowed to metadata-only override before API write.*
