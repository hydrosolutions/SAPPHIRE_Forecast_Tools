# PP-027: Add per-station observability when EM ensemble is skipped

**Priority**: Medium
**Module**: postprocessing_forecasts (`pp`)
**Status**: Implemented (Phase 1 only — diagnostic logging)
**Branch**: TBD

## Problem

The EM (Ensemble Mean) computation in `ensemble_calculator.py` silently skips stations
when fewer than 2 individual models pass the skill thresholds (`sdivsigma < 0.6`,
`nse > 0.8`, `accuracy > 0.8`). There is no per-station diagnostic — operators only
discover missing EM records by querying the API after the fact.

**Example**: Station 16059 (S2) has significantly weaker skill metrics than 15189 (S1):
- S1: 73% of skill records pass all thresholds, 42/45 pentads have 2+ qualifying models
- S2: 46% pass, 38/43 pentads have 2+ models
- Current pentad 16: S1 has 4 qualifying models, S2 has only 1 → EM silently dropped

NE (Neural Ensemble) is unaffected because it's computed unconditionally before skill checks.

This is **not a bug** — the thresholds are working as designed. The issue is purely
observability: operators need to know which stations/periods are below EM quality thresholds.

## Root Cause Analysis

Three silent skip gates in `ensemble_calculator.create_ensemble_forecasts()`:

1. **Gate A** (line ~125): Inner join of forecasts × skill_stats drops stations with no
   matching skill metrics entirely
2. **Gate B** (`filter_for_highly_skilled_forecasts()`): Applies triple threshold filter —
   stations where no model passes all 3 thresholds get zero qualifying rows
3. **Gate C** (line ~159-161): `is_multi_model_composition()` discards rows where only 1
   model qualifies — EM requires a true multi-model average

All three gates are silent — they return early or filter without logging which stations
were affected.

## Proposed Changes

### Phase 1: Diagnostic logging (minimal, safe)

**File**: `apps/postprocessing_forecasts/src/ensemble_calculator.py`

After the inner merge (Gate A), compare station codes in `qualifying` vs input `forecasts`:

```python
input_codes = set(forecasts["code"].unique())
qualifying_codes = set(qualifying["code"].unique())
skipped_codes = input_codes - qualifying_codes
if skipped_codes:
    logger.info(
        "EM skipped for %d station(s) — no qualifying models after skill filter: %s",
        len(skipped_codes),
        ", ".join(sorted(skipped_codes)),
    )
```

After the multi-model composition filter (Gate C), log stations that had qualifying
models but were dropped due to single-model composition:

```python
pre_filter_codes = set(ensemble_avg["code"].unique()) if not ensemble_avg.empty else set()
# ... apply is_multi_model_composition filter ...
post_filter_codes = set(ensemble_avg["code"].unique()) if not ensemble_avg.empty else set()
single_model_codes = pre_filter_codes - post_filter_codes
if single_model_codes:
    logger.info(
        "EM skipped for %d station(s) — only 1 qualifying model (need 2+): %s",
        len(single_model_codes),
        ", ".join(sorted(single_model_codes)),
    )
```

### Phase 2: Write diagnostics integration (optional)

**File**: `apps/postprocessing_forecasts/src/write_diagnostics.py`

Add an EM coverage summary to the existing diagnostics output that shows per-station
EM production status for the current run.

## Acceptance Criteria

- [ ] When EM is skipped for a station due to Gate A (no skill metrics match), a log
  message at INFO level names the station(s)
- [ ] When EM is skipped due to Gate C (single-model only), a log message at INFO level
  names the station(s)
- [ ] Existing EM computation logic is NOT changed — only logging is added
- [ ] No new dependencies introduced
- [ ] All existing tests pass unchanged
- [ ] Add 1 test verifying the log messages appear when EM is skipped

## Files to Modify

| File | Change |
|------|--------|
| `apps/postprocessing_forecasts/src/ensemble_calculator.py` | Add per-station skip logging at Gates A and C |
| `apps/postprocessing_forecasts/tests/test_ensemble_calculator.py` | Add test for skip log messages |

## Risks

- **Very low risk**: Only adds `logger.info()` calls — no data flow changes
- Logging volume: With many stations × periods, log output could be verbose. Mitigate by
  logging once per run (set of skipped codes), not per-period

## Dependency Graph

```json
{
  "phases": [
    {
      "id": "P1",
      "name": "Diagnostic logging in ensemble_calculator",
      "depends_on": [],
      "files": ["src/ensemble_calculator.py", "tests/test_ensemble_calculator.py"]
    },
    {
      "id": "P2",
      "name": "Write diagnostics integration (optional)",
      "depends_on": ["P1"],
      "files": ["src/write_diagnostics.py"]
    }
  ]
}
```
