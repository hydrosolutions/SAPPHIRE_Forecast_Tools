## LT config raw `.split(',')` doesn't strip whitespace — operator env-var edits with spaces cause membership-check failures

**Status**: Draft (2026-06-10)
**Module**: `apps/long_term_forecasting/config_forecast.py`
**Priority**: **Low** (operator-facing footgun, not a runtime regression — no current deployment is known to set the env var with spaces, but the bash workaround in `bin/dev_local_backfill.sh` v3.x demonstrates that operators have already hit it in development)
**Labels**: `low-prio`, `config-parsing`, `long-term-forecasting`, `migration-toolkit-companion`
**Discovered**: 2026-06-10 during reviewer round-5 of `bin/dev_local_backfill.sh` v3.x — found while tracing why a forecast mode passed in by the wrapper failed the assertion at `config_forecast.py:58` even though the env var "looked right" when echoed.
**Related**:
- PR #XXX (this PR — pending merge)
- `bin/dev_local_backfill.sh` v3.x — currently works around this bug with bash-side env-var whitespace stripping; once this PR merges, the bash workaround can be retired in a follow-up cleanup
- Sibling Tier-1 MIG issues (MIG-001, MIG-002) — discovered during the same Tajik deployment work cycle

---

## Summary

`config_forecast.py:46` parses `ieasyhydroforecast_ml_long_term_supported_modes` with a raw `.split(',')` — no whitespace stripping, no empty-entry filtering. If an operator writes the env var with a space after commas (e.g., `"monthly, seasonal"`) the resulting `LT_supported_modes` list contains `["monthly", " seasonal"]` (note the leading space on the second entry). The membership check at `config_forecast.py:58` (`forecast_mode in self.LT_supported_modes`) then fails when an upstream caller passes the trimmed mode name `"seasonal"`:

```python
assert forecast_mode in self.LT_supported_modes, \
    f"Forecast config {forecast_mode} not supported. Supported configs: {self.LT_supported_modes}"
```

The assertion error message displays `[' monthly', ' seasonal']` (with leading spaces visible) but the visual cue is subtle enough that operators miss it on first reading.

This was surfaced during reviewer round-5 of a sibling dev script (`bin/dev_local_backfill.sh` v3.x), which works around the bug via bash-side env-var whitespace stripping. MIG-005 is the proper Python-side fix; the bash workaround can be retired once this PR merges.

---

## Evidence

Example env-var value as written by an operator who copy-pasted from a runbook:

```bash
export ieasyhydroforecast_ml_long_term_supported_modes="monthly, seasonal, quarter"
```

Observed parsed result (before this fix):

```python
>>> os.getenv('ieasyhydroforecast_ml_long_term_supported_modes').split(',')
['monthly', ' seasonal', ' quarter']
>>> 'seasonal' in ['monthly', ' seasonal', ' quarter']
False
```

After this fix:

```python
>>> [m.strip() for m in os.getenv('...').split(',') if m.strip()]
['monthly', 'seasonal', 'quarter']
>>> 'seasonal' in ['monthly', 'seasonal', 'quarter']
True
```

---

## Authority — exact file:line citation

`apps/long_term_forecasting/config_forecast.py:46` (before fix):

```python
self.LT_supported_modes = os.getenv('ieasyhydroforecast_ml_long_term_supported_modes').split(',')
```

Membership check at `config_forecast.py:58` (unchanged by this fix):

```python
assert forecast_mode in self.LT_supported_modes, f"Forecast config {forecast_mode} not supported. Supported configs: {self.LT_supported_modes}"
```

---

## Audit

Searched for similar `os.getenv(...).split(',')` patterns elsewhere in `apps/long_term_forecasting/` to confirm this is a single-site fix and not a systemic pattern:

- `apps/long_term_forecasting/config_forecast.py:46` — the only site reading this env var
- No other module re-reads `ieasyhydroforecast_ml_long_term_supported_modes` directly

The bash-side workaround lives at `bin/dev_local_backfill.sh` v3.x — it trims the env var with `tr -d ' '` (or equivalent) before exporting. The workaround was added as a defensive measure during reviewer round-5; the upstream Python fix in this PR makes it unnecessary.

---

## Fix

Single-site change. Replace the one-line raw split with a list comprehension that strips whitespace per entry and drops empty entries. Critically: **do NOT** add a `""` default to `os.getenv` — missing env var must still raise `AttributeError` on `.split()` to preserve the current fail-fast behavior.

```python
# MIG-005: strip whitespace per entry and drop empty entries.
# No `, ""` default on os.getenv — missing env var still raises
# AttributeError on .split(None) (preserves current fail-fast behavior).
self.LT_supported_modes = [
    m.strip()
    for m in os.getenv('ieasyhydroforecast_ml_long_term_supported_modes').split(',')
    if m.strip()
]
```

### Behavior changes documented

Both changes are desirable; both are intentional:

| Input env var | Before | After | Justification |
|---|---|---|---|
| `"monthly, seasonal"` | `["monthly", " seasonal"]` | `["monthly", "seasonal"]` | Whitespace tolerance — the bug fix |
| `"monthly,"` | `["monthly", ""]` | `["monthly"]` | Empty entry dropped (reviewer approved — desirable) |
| `",monthly,,seasonal,"` | `["", "monthly", "", "seasonal", ""]` | `["monthly", "seasonal"]` | Robust to operator typos |
| (env var missing) | `AttributeError` on `.split(",")` | `AttributeError` on `.split(",")` | **Unchanged** — fail-fast preserved |

The reviewer specifically flagged that adding a `""` default to `os.getenv` would silently turn a missing env var into `[]`, which is a worse failure mode than the current `AttributeError`. The list comprehension preserves the current fail-fast semantics.

### What this fix does NOT do

- It does NOT strip whitespace from `forecast_mode` at line 58. The reviewer reviewed this and concluded the line 46 fix is sufficient — callers of `load_forecast_config()` pass trimmed values, and stripping on both sides would mask other bugs.
- It does NOT change the assertion-vs-exception form at line 58 (could be a future cleanup; out of scope here).

---

## Tests

New test file: `apps/long_term_forecasting/tests/test_config_forecast.py`

The fixture `isolated_config` patches `sl.load_environment` to a no-op and sets the minimum env vars needed for `ForecastConfig._get_paths()` to construct without errors:

- `ieasyhydroforecast_configuration_path` → `tmp_path`
- `ieasyhydroforecast_ml_long_term_configuration` → `"long_term_configs"`
- `ieasyforecast_intermediate_data_path` → `tmp_path`
- `ieasyhydroforecast_ml_long_term_output_path` → `"lt_output"`

The env var under test (`ieasyhydroforecast_ml_long_term_supported_modes`) is varied per test. 7 tests total:

| # | Test | Env value | Expected `LT_supported_modes` |
|---|---|---|---|
| 1 | `test_lt_supported_modes_normalization[monthly-expected0]` | `"monthly"` | `["monthly"]` |
| 2 | `test_lt_supported_modes_normalization[monthly,seasonal-expected1]` | `"monthly,seasonal"` | `["monthly", "seasonal"]` |
| 3 | `test_lt_supported_modes_normalization[monthly, seasonal-expected2]` | `"monthly, seasonal"` (space after comma) | `["monthly", "seasonal"]` |
| 4 | `test_lt_supported_modes_normalization[ monthly , seasonal -expected3]` | `" monthly , seasonal "` (surrounding whitespace) | `["monthly", "seasonal"]` |
| 5 | `test_lt_supported_modes_normalization[,monthly,,seasonal,-expected4]` | `",monthly,,seasonal,"` (empty entries) | `["monthly", "seasonal"]` |
| 6 | `test_lt_supported_modes_normalization[monthly\t,\nseasonal-expected5]` | `"monthly\t,\nseasonal"` (tabs + newlines) | `["monthly", "seasonal"]` |
| 7 | `test_lt_supported_modes_fail_fast_on_missing_env` | (env var deleted) | `AttributeError` raised |

All 7 tests pass under `SAPPHIRE_TEST_ENV=True bash apps/run_tests.sh long_term_forecasting`.

---

## Acceptance criteria

- [ ] `config_forecast.py:46` uses the list comprehension form (`[m.strip() for m in ... if m.strip()]`)
- [ ] No `, ""` default added to `os.getenv` (fail-fast preserved)
- [ ] Membership check at `config_forecast.py:58` is UNCHANGED
- [ ] New test file `apps/long_term_forecasting/tests/test_config_forecast.py` exists with the `isolated_config` fixture and 7 tests (6 parameterized normalization + 1 fail-fast)
- [ ] All 7 new tests pass; full `SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting` passes with zero failures and no new skips
- [ ] No edits to `sapphire/services/`, `.github/workflows/`, other apps/ modules, or other files in `apps/long_term_forecasting/` beyond the two scoped (`config_forecast.py`, `tests/test_config_forecast.py`)
- [ ] PR description references this gi_draft

---

## Rollout

Doc-only PR (one production code line + new test file + new gi_draft + module_issues.md row). No migration needed. Low priority because:

1. No production deployment is currently known to set the env var with spaces (the env var is typically templated by deployment scripts that don't introduce whitespace).
2. The bash workaround in `bin/dev_local_backfill.sh` v3.x already shields dev workflows.
3. The fail-fast behavior is preserved — operators who do hit the bug today get a noisy `AssertionError` rather than silent data corruption.

After this PR merges, a follow-up cleanup PR can retire the bash workaround in `bin/dev_local_backfill.sh`.

---

## Process note

This bug was found during the reviewer's round-5 pass on `bin/dev_local_backfill.sh` v3.x. The wrapper had been doing bash-side env-var stripping as a workaround; the reviewer flagged that the workaround masks an upstream Python parsing bug that should be fixed at the source rather than papered over in shell.

The MIG-005 prefix reflects that this is companion work to the migration-toolkit family (MIG-001, MIG-002) — all surfaced during the same Tajik deployment work cycle, all small operator-facing footguns in the broader deployment runbook ecosystem.

---

## Out of scope

- Stripping `forecast_mode` at line 58 membership check (reviewer reviewed and rejected — line 46 fix is sufficient).
- Replacing the `assert` at line 58 with a proper exception (separate cleanup, not blocking).
- Auditing other `os.getenv(...).split(',')` patterns repo-wide (separate scope; not enough evidence of systemic issues to warrant a wider sweep).
- Retiring the bash-side workaround in `bin/dev_local_backfill.sh` v3.x (separate follow-up PR after this one merges).
