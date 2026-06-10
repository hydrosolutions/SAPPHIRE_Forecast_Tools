## Wrapper SQL uses lowercase horizon_type literals against UPPERCASE PG enum (MIG-003)

**Status**: Draft (2026-06-10)
**Module**: `bin/` (migration toolkit — ML export wrapper + long-term initialize wrapper)
**Priority**: **High** (deployment-blocking — every affected query hard-fails against any operational deployment, same severity class as MIG-001/Finding 11)
**Labels**: `migration-toolkit`, `sql`, `enum-coercion`, `deployment-blocker`, `audit-fix`
**Discovered**: 2026-06-10 during P3 verification of the hardening batch (PR #360 + #361 merged earlier today). Flagged in advance by the P1 agent (task #56) and confirmed live during the same session.
**Related**:
- Finding 11 / PR #361 — same bug class for `model_type`. The fix pattern + tests there are the model for this one.
- Authority: `sapphire/services/postprocessing/app/models.py:9-17` — `HorizonType` Python enum where NAMES (`DAY`/`PENTAD`/`DECADE`/...) differ from `.value` strings (`day`/`pentad`/`decade`/...). SQLAlchemy stores the NAME as the PG enum label.

---

## Summary

Four executable SQL queries in the migration toolkit's `.sh` wrappers compare `horizon_type` against lowercase string literals (`'day'`, `'pentad'`, `'decade'`, `'month'`). The live `sapphire-postprocessing-db` `horizontype` PG enum stores UPPERCASE labels (`DAY`, `PENTAD`, `DECADE`, `MONTH`, `QUARTER`, `SEASON`). Every affected query hard-fails on any operational deployment with:

```
ERROR: invalid input value for enum horizontype: "day"
```

This blocks both the ML laptop export and the long-term forecast import wrappers on any real deployment. Same bug class as Finding 11 (which P1 fixed for `model_type`); the same `::text` cast pattern is the right fix here.

---

## Live evidence (reproduced 2026-06-10)

```bash
$ POSTGRES_USER=$(docker exec sapphire-postprocessing-db printenv POSTGRES_USER)

# Probe the enum:
$ docker exec sapphire-postprocessing-db psql -U "$POSTGRES_USER" -d postprocessing_db -tAc \
    "SELECT enumlabel FROM pg_enum WHERE enumtypid='horizontype'::regtype ORDER BY enumsortorder"
DAY
PENTAD
DECADE
MONTH
QUARTER
SEASON

# Reproduce the bug:
$ docker exec sapphire-postprocessing-db psql -U "$POSTGRES_USER" -d postprocessing_db -tAc \
    "SELECT COUNT(*) FROM forecasts WHERE horizon_type = 'day'"
ERROR:  invalid input value for enum horizontype: "day"
LINE 1: SELECT COUNT(*) FROM forecasts WHERE horizon_type = 'day'
                                                            ^

# Confirm fix pattern works (model_type::text cast — same as Finding 11 fix):
$ docker exec sapphire-postprocessing-db psql -U "$POSTGRES_USER" -d postprocessing_db -tAc \
    "SELECT COUNT(*) FROM forecasts WHERE horizon_type::text = 'DAY'"
1144764
```

The existing live row already stores `horizon_type='PENTAD'` (uppercase). Confirmed by:
```
$ ... -tAc "SELECT model_type::text, horizon_type::text FROM forecasts LIMIT 1"
TSMIXER|PENTAD
```

---

## Audit — exact bug sites (file:line)

`grep -rnE "horizon_type[ =]*[=]?[ ]*'(day|pentad|decade|month|quarter|season|year)'" bin/ apps/`
filtered to executable SQL contexts (excluding docstrings, comments, and tests):

| File | Line | Wrong | Correct |
|---|---|---|---|
| `bin/export_ml_forecast_history.sh` | :312 | `where+=" AND horizon_type IN ('day','pentad','decade')"` | `where+=" AND horizon_type::text IN ('DAY','PENTAD','DECADE')"` |
| `bin/export_ml_forecast_history.sh` | :314 | `where+=" AND horizon_type = 'day'"` | `where+=" AND horizon_type::text = 'DAY'"` |
| `bin/initialize_long_forecast_history.sh` | :320 | `... WHERE horizon_type='month'` | `... WHERE horizon_type::text='MONTH'` |
| `bin/initialize_long_forecast_history.sh` | :469 | `... WHERE horizon_type='month' AND horizon_value=${MODE_HORIZON_VALUE}` | `... WHERE horizon_type::text='MONTH' AND horizon_value=${MODE_HORIZON_VALUE}` |

**Authority** for the case mismatch: `sapphire/services/postprocessing/app/models.py:9-17` — `HorizonType` Python enum class where the Python NAMES (`DAY`/`PENTAD`/...) become the PG enum labels and the `.value` strings (`'day'`/`'pentad'`/...) are the API wire format. Compare to `ModelType` at `:23` which has the same pattern and is explicitly documented in the comment.

---

## Audit — what's NOT in scope (not a bug)

The `grep` also surfaced lots of non-bug mentions of `horizon_type='day'` etc. These should NOT be changed:

- **Docstrings + comments** (`bin/export_ml_forecast_history.sh:25,58,124`; `bin/initialize_ml_forecast_history.sh:40,346`; `bin/utils/migration_py/ml_forecast.py:35,645`; `apps/machine_learning/scr/utils_ml_forecast.py:762`; `apps/iEasyHydroForecast/forecast_library.py:5368`). Comments are not executed.
- **API wire format references** (any Python code that passes `'day'` to a Pydantic model or REST API). Pydantic's `HorizonType` enum maps the `.value` string to the enum member, then SQLAlchemy stores the NAME — same dual-representation rule as `model_type`. **Do NOT change these**.
- **Tests asserting the wire form** (e.g. `apps/iEasyHydroForecast/tests/test_forecast_library.py:3498,3539,3794,3870` — these check the API call uses lowercase `'decade'`/`'month'`; correct, do not change). Same for `apps/postprocessing_forecasts/tests/test_quarterly_api_writer.py` etc.
- **Test that asserts the buggy SQL pattern**: `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py:800` literally asserts `"horizon_type='month'" in src`. This test was wrong — it was enshrining the bug. **Update it** to assert the new pattern `"horizon_type::text='MONTH'"`.

The dual-representation rule (PG enum NAMES uppercase vs API wire VALUES lowercase) is the same rule P1 documented for `model_type`. The runbook §6.4 representation note from PR #361 already explains the pattern — extend it to cover `horizon_type`.

---

## Fix

Single-PR fix following the P1 pattern:

### 1. SQL changes (4 sites)

Apply the table above. Use `horizon_type::text` cast (NOT `UPPER(horizon_type::text)`) — same rationale as P1 Revision #8: the cast sidesteps enum-literal coercion without the non-sargable function wrapper.

### 2. Test changes

- Update `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py:800`: change the assertion from `"horizon_type='month'"` to `"horizon_type::text='MONTH'"`. The neighbouring docstring at `:793` should also be updated.
- Similar assertion at `:1043` ("per-mode query exists and references both horizon_type='month' AND ...") should be updated.
- Add a behavioral regression test (mirror the P1 pattern at `apps/iEasyHydroForecast/tests/test_export_ml_forecast.py::test_export_dry_run_sql_uses_uppercase_pg_enum_labels`): capture the dry-run SQL emitted by the affected wrappers, assert the WHERE clause contains uppercase `'DAY'`/`'MONTH'` and does NOT contain lowercase `'day'`/`'month'`.

### 3. Runbook §6 representation note

Extend the §6.4 "Representation note" callout (added by PR #361) to also cover `horizon_type`. Same two-representation rule: PG enum NAMES uppercase for SQL; API wire VALUES lowercase for JSON payloads. Cite `models.py:9-17`.

### 4. Audit (no change expected)

Confirm no other wrapper or helper has the same pattern:
- `git grep -nE "horizon_type[ =]*=[ ]*'(day|pentad|decade|month|quarter|season|year)'" bin/` should return zero matches after the fix
- `git grep -nE "horizon_type IN \('(day|pentad|decade)" bin/` should also return zero

If new matches appear, fix them in the same PR.

---

## Acceptance criteria

- [ ] All four bug sites use `horizon_type::text` with UPPERCASE literals.
- [ ] `git grep -nE "horizon_type[ =]*=[ ]*'(day|pentad|decade|month|quarter|season|year)'" bin/` returns no matches in executable SQL.
- [ ] `git grep -nE "horizon_type IN \('(day|pentad|decade|month|quarter|season|year)" bin/` returns no matches.
- [ ] `test_initialize_long_forecast.py` assertions updated to the new pattern; tests pass.
- [ ] Behavioral regression test captures dry-run SQL and would FAIL on the old form.
- [ ] Existing tests asserting API wire format (`apps/iEasyHydroForecast/tests/test_forecast_library.py:3498-3539,3794-3870` etc.) remain UNCHANGED and green — they validate the Pydantic side which still uses lowercase.
- [ ] Runbook §6.4 representation note extended to cover `horizon_type` with `models.py:9-17` authority.
- [ ] `SAPPHIRE_TEST_ENV=True bash apps/run_tests.sh iEasyHydroForecast` passes, zero failures, no unexpected skips.
- [ ] Manual live-DB verification: run `psql -c "SELECT COUNT(*) FROM forecasts WHERE horizon_type::text = 'DAY'"` against the local stack, confirm returns a count (not enum error).

---

## Rollout

1. Single PR off `maxat_sapphire_2`. Branch: `fix_horizon_type_enum_sql`. Doc + 2 wrappers + 1 test file + 1 runbook section. Roughly 15-20 lines of executable code change.
2. **No image rebuild required** — the wrappers are read fresh from the operator's checkout on each invocation. Sapphire-services not affected.
3. **No DB migration required** — DB schema already has uppercase enum labels; this PR aligns the wrappers to the schema.
4. **Operators with existing log files from prior runs**: if they ran the ML export or long-term initialize wrappers before this fix, those runs would have hard-errored — there's no silent-corruption risk. They should re-run after merge.

---

## Process note

This bug was flagged in advance by the P1 implementing agent (logged as task #56 from the 2026-06-08 walkthrough) and confirmed live during the P3 verification on 2026-06-10. The pattern is identical to Finding 11 (model_type) and was visible in `models.py:9-17` for anyone reading the schema, but slipped through the original migration-toolkit sprint because the test suite stubs psql and doesn't validate against a real enum at integration time.

**Recommended follow-up for future migration-toolkit work**: any wrapper that constructs SQL against a column of PG enum type MUST either (a) use the `::text` cast pattern, or (b) include an integration test that runs the constructed SQL against an actual enum. Without one of those, this class of bug is invisible until live deployment.

---

## Out of scope

- Other case-sensitivity audits beyond `horizon_type` (e.g. `meteo_type`, `snow_type`). Worth a separate audit but not blocking this PR.
- Restructuring the Pydantic dual-representation pattern at the services layer (`sapphire/services/` — colleague-managed).
- Adding integration tests that stand up a real Postgres instance with the enum (process improvement; separate scope).
