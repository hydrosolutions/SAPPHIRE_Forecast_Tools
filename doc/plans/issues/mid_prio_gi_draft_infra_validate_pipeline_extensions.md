# INFRA-014: Extend validate_pipeline.py with JSON output, baseline/delta, and new checks

**Status**: Review
**Module**: `infra` (cross-module)
**Priority**: Medium
**Labels**: `enhancement`, `testing`, `observability`, `developer-experience`

---

## Summary

Extend the existing `validate_pipeline.py` incrementally to bridge the gap between the manual review checklist template and automated verification. Four phases: (1) JSON output foundation, (2) baseline/delta mode, (3) five new check functions, (4) template update.

## Problem Statement

`validate_pipeline.py` already covers ~60% of the checks in the manual review checklist but:

- Output is text-only — no machine-readable format for historical comparison
- No baseline/delta tracking — silent count regressions go undetected
- Missing checks: ML flag distribution, snow norm-vs-operational detection, EM/NE parity, LT quantile ordering, data freshness
- The manual template requires copy-pasting curl commands for checks that `validate_pipeline.py` already has the data for

---

## Implementation Plan

### Phase 1: JSON Output Foundation

Extend `CheckResult` dataclass with `max_date` and `counts` fields. Add `results_to_json()` function and `--output-json <path>` CLI flag. All existing behavior is unchanged — JSON is opt-in.

**Files**: `validate_pipeline/validate_pipeline.py`, `validate_pipeline/tests/test_validate_pipeline.py`

**Steps:**

- [x] **1a.** Add `max_date: str | None = None` and `counts: dict = field(default_factory=dict)` to `CheckResult` dataclass
- [x] **1b.** Implement `results_to_json(results: list[CheckResult]) -> dict` — serialises all fields to a JSON-compatible dict
- [x] **1c.** Add `--output-json <path>` CLI flag; when supplied, write the JSON dict to the given path after all checks complete
- [x] **1d.** Ensure all existing checks populate `max_date` and `counts` where the data is already available; leave as `None`/`{}` where it is not
- [x] **1e.** Write `TestJsonOutput` (5 tests):
  - JSON is valid and parseable
  - All check names appear as keys
  - `status` field is one of `PASS`, `WARN`, `FAIL`, `SKIP`
  - `max_date` and `counts` fields present (may be `None`/`{}`)
  - Writing to a path produces a file with correct content

**Dependencies**: None

**Tests**: `TestJsonOutput` — 5 tests

---

### Phase 2: Baseline / Delta Mode

Add `--phase pre|post` and `--baseline <path>` CLI flags. Pre mode saves baseline JSON. Post mode loads baseline, computes deltas (count increases/decreases per check), and reports. A count decrease triggers `WARN`.

**Files**: `validate_pipeline/validate_pipeline.py`, `validate_pipeline/tests/test_validate_pipeline.py`

**Steps:**

- [x] **2a.** Add `--phase {pre,post}` flag; default behaviour (no flag) is unchanged
- [x] **2b.** Add `--baseline <path>` flag
- [x] **2c.** `--phase pre`: run all checks, write JSON to `--baseline` path (or a default path if omitted), exit 0
- [x] **2d.** `--phase post`: run all checks, load baseline JSON, compute per-check count deltas
- [x] **2e.** Delta report: for each check where a numeric count decreased vs baseline, emit a `WARN` line; increases and unchanged counts are informational only
- [x] **2f.** Write `TestPhaseMode` (8 tests):
  - Pre mode writes a file at the baseline path
  - Post mode loads the file and computes deltas
  - Count decrease → `WARN` in output
  - Count increase → informational, no `WARN`
  - Count unchanged → informational, no `WARN`
  - Missing baseline file in post mode → clear error message, non-zero exit
  - No `--phase` flag → existing behaviour, no delta report
  - `--phase pre` followed by `--phase post` produces consistent delta when nothing changed

**Dependencies**: Phase 1

**Tests**: `TestPhaseMode` — 8 tests

---

### Phase 3: New Checks

Five new check functions, all backward-compatible: each returns `SKIP` when the required data is absent.

**Files**: `validate_pipeline/validate_pipeline.py`, `validate_pipeline/tests/test_validate_pipeline.py`

#### 3a. `check_ml_flag_distribution()`

Query flag values for ML forecasts. Count records per flag value and store in `CheckResult.counts`. Emit `WARN` if all records carry the same flag value (stuck-flag detection). Return `SKIP` when no ML forecast records exist.

#### 3b. `check_snow_norm_dates()`

Query max dates of snow records. Detect whether any snow records have a date in the current operational year (vs. only year-2000 norm dates). Emit `WARN` if all snow dates fall in year 2000 — this catches the PREPG-003 symptom where the operational update window is missed and only historical norms are present.

#### 3c. `check_em_ne_parity()`

Query EM and NE record counts per horizon. A mismatch (EM count ≠ NE count for the same horizon) indicates an incomplete ensemble and triggers `WARN`. Return `SKIP` when neither EM nor NE records exist.

#### 3d. `check_data_freshness()`

For each dataset type (runoff, snow, forecasts), read `max_date` from the API. Compute the difference against `forecast_date`. Emit `WARN` for any dataset where `max_date` is more than `FRESHNESS_THRESHOLD_DAYS` days older than `forecast_date`. The threshold defaults to 3 and is configurable via the `FRESHNESS_THRESHOLD_DAYS` environment variable.

#### 3e. `run_tier2_long_term()`

Extend the existing short-term quantile ordering, discharge non-negative, and skill metric range checks to long-term (monthly) forecasts. Uses the same check logic as the existing short-term tier-2 checks, applied to the long-term API endpoints.

**Steps:**

- [x] **3a.** Implement `check_ml_flag_distribution()` with stuck-flag `WARN`
- [x] **3b.** Implement `check_snow_norm_dates()` with year-2000-only `WARN`
- [x] **3c.** Implement `check_em_ne_parity()` with horizon-level count comparison
- [x] **3d.** Implement `check_data_freshness()` with configurable threshold
- [x] **3e.** Implement `run_tier2_long_term()` extending existing tier-2 logic
- [x] **3f.** Register all five checks in the main check runner
- [x] **3g.** Write `TestNewChecks` (15 tests — 3 per check):
  - Each check returns `SKIP` when no data
  - Each check returns `PASS` on healthy data
  - Each check returns `WARN` on the trigger condition

**Dependencies**: Phase 1 (uses `CheckResult.max_date` and `counts` fields)

**Tests**: `TestNewChecks` — 15 tests

---

### Phase 4: Template Update (markdown only)

Update `doc/dev/review_checklist_local_template.md` to reference `validate_pipeline.py` JSON output and automated checks. Replace the manual baseline counting section with the `--phase pre` invocation. Add references to snow current-year detection and ML flag distribution checks. No code changes.

**Files**: `doc/dev/review_checklist_local_template.md`

**Steps:**

- [x] **4a.** Add a pre-run section: `bash apps/run_locally.sh validate --phase pre --baseline /tmp/baseline.json`
- [x] **4b.** Add a post-run section: `bash apps/run_locally.sh validate --phase post --baseline /tmp/baseline.json`
- [x] **4c.** Replace manual curl-based count checks with references to the automated JSON output
- [x] **4d.** Add snow current-year and ML flag distribution to the automated check reference table

**Dependencies**: Phases 1, 2, 3

---

## Backward Compatibility

All changes are additive:

- New `CheckResult` fields default to `None` / `{}`
- New CLI flags are optional — existing invocations in `run_locally.sh` work without modification
- New checks return `SKIP` when data is absent
- Exit code logic is unchanged
- The `--phase` flag defaults to no-op (existing behaviour)

---

## Acceptance Criteria

- [x] `--output-json` produces valid, parseable JSON with all check results
- [x] `--phase pre` saves baseline; `--phase post` loads and reports deltas
- [x] Count decrease in delta mode triggers `WARN` (not silent)
- [x] ML flag distribution reported in JSON `counts` field
- [x] Snow norm-vs-operational detection warns when only year-2000 dates are present
- [x] EM/NE parity mismatch triggers `WARN`
- [x] Data freshness `WARN` when `max_date` is more than 3 days stale
- [x] Long-term quantile ordering checked (not just short-term)
- [x] All existing tests pass unchanged
- [x] 28 new tests pass (5 + 8 + 15)
- [x] Template updated with automated check references
- [x] No changes to `sapphire/services/`

---

## Dependency Graph

```json
{
  "phases": {
    "P1_json_output": {
      "title": "JSON output foundation",
      "depends_on": [],
      "parallel_with": []
    },
    "P2_delta_mode": {
      "title": "Baseline / delta mode",
      "depends_on": ["P1_json_output"],
      "parallel_with": ["P3_new_checks"]
    },
    "P3_new_checks": {
      "title": "Five new check functions",
      "depends_on": ["P1_json_output"],
      "parallel_with": ["P2_delta_mode"]
    },
    "P4_template_update": {
      "title": "Template markdown update",
      "depends_on": ["P1_json_output", "P2_delta_mode", "P3_new_checks"],
      "parallel_with": []
    }
  },
  "execution_groups": [
    {"group": 1, "phases": ["P1_json_output"]},
    {"group": 2, "parallel": true, "phases": ["P2_delta_mode", "P3_new_checks"]},
    {"group": 3, "phases": ["P4_template_update"]}
  ]
}
```
