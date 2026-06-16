# Postprocessing maintenance `model_short` KeyError on empty DECAD read

**Status:** Phase 1 **complete** (crash resolved); Phases 2–3 **deferred
low-prio**. This plan went through two review cycles (critical review → planner
revision → verification review → planner revision); all blocking findings were
folded in.

- **Phase 1 — Crash-Fix Core: DONE.** Shipped the consolidated ensemble guards,
  the maintenance empty guard before virtual-stations, and the operational no-op
  guard. The DECAD `KeyError: 'model_short'` is fully closed via all paths. This
  was the high-priority content.
- **Phase 2 — Reader Contract: DEFERRED (low-prio).** Defensive hardening + a
  cleaner long-term empty-read contract; explicitly *not* required to stop the
  crash once Phase 1 landed, and the highest-blast-radius change of the three.
  Do it when the reader layer is next touched.
- **Phase 3 — Stale-EM Lookback Scoping: DEFERRED (low-prio).** With Phase 1's
  guards, the old trigger now no-ops gracefully; what remains is an efficiency
  gain plus an operational coverage decision (+ operator note + separate
  backfill). Not a crash fix. Depends on P1 only and may ship standalone.

## Problem

`bash apps/run_locally.sh daily` can fail in `postprocessing_forecasts`
maintenance for the DECAD horizon with `KeyError: 'model_short'`.

The scoped reader `read_individual_model_forecasts_for_dates()` returns a
**columnless** empty DataFrame (`pd.DataFrame()`, shape `(0, 0)`, default
`RangeIndex`) when no rows exist for the affected dates/codes. The maintenance
flow then calls `calculate_virtual_stations_data()` and the neural-ensemble
function on that frame **before** checking `modelled.empty`, and the ensemble
indexes `forecasts["model_short"]`, raising the KeyError.

Contributing condition: unbounded stale-EM detection pulls old dates that have
no individual-model data into the scoped read, producing the empty input that
crashes (addressed in Phase 3).

## 1. Goals / Non-goals

**Goals**

- Stop the DECAD maintenance crash caused by columnless empty modelled data
  reaching neural-ensemble logic.
- Make both pentad and decad ensemble functions safely return unchanged input on
  empty or incomplete forecast frames.
- Add early maintenance and operational no-op guards where reachable.
- In a later phase, standardize empty individual-reader outputs with schemaful,
  dtype-parity frames.
- In a later independent operational phase, scope stale-EM repair to the same
  lookback boundary as `detect_stale_quantiles()`.

**Non-goals**

- Redesigning gap detection.
- Migrating or rewriting historical archives.
- Changing successful neural-ensemble behavior for valid inputs.
- Implementing the one-off stale-EM historical backfill in this change.

## 2. Phased implementation

### Phase 1 — Crash-Fix Core

**Goal:** Stop the production crash without depending on reader-contract changes.

**Files**

- `apps/iEasyHydroForecast/setup_library.py`
  - `calculate_neural_ensemble_forecast`
  - `calculate_neural_ensemble_forecast_decade`
- `apps/postprocessing_forecasts/postprocessing_maintenance.py`
  - `_fill_gaps_for_horizon`
- Operational module containing `read_observed_and_modelled_data` (path to be
  confirmed from repo inspection), only if the reachability check proves an empty
  modelled read can occur.
- Relevant tests.

**Depends on:** None.

**Agents**

- Agent 1: update both ensemble functions in `setup_library.py` and add direct
  ensemble tests. (Both functions in one agent — shared file.)
- Agent 2: update `_fill_gaps_for_horizon`, verify `calculate_virtual_stations_data`,
  and add maintenance tests.
- Agent 3: inspect the operational path; add guard and saved-output test if
  reachable, or document "not reachable → guard omitted" with rationale.

**Acceptance criteria**

- Each ensemble function has **one consolidated guard at the top**, executing
  before any `forecasts["model_short"].str.contains(...)`. It must **not** rely on
  the existing `if not available_target_models: return forecasts` early-out (which
  sits after the first `.str` index).
- The guard returns `forecasts` unchanged and logs at the **same warning level and
  return semantics** as the existing no-target-models branch.
- The guard triggers when `forecasts.empty` **or** any required column is absent.
  - Required pentad columns: `model_short`, `date`, `code`,
    `forecasted_discharge`, `pentad_in_month`, `pentad_in_year`.
  - Required decad columns: `model_short`, `date`, `code`,
    `forecasted_discharge`, `decad_in_month`, `decad_in_year`.
- `_fill_gaps_for_horizon` checks `modelled.empty` **immediately after the read and
  before `calculate_virtual_stations_data`**.
- The empty-maintenance warning includes the horizon label, affected-date count,
  and gap-code count.
- `calculate_virtual_stations_data` is verified not to index `model_short` or
  period columns on a 0-row schemaful frame.
- Operational reachability resolves to **either** a real saved no-op output test
  **or** a documented "not reachable → guard omitted" note. No silent skip
  (zero-skips policy).

### Phase 2 — Reader Contract

**Goal:** Replace bare `pd.DataFrame()` empty returns with one canonical schemaful
empty individual-forecast contract.

**Files**

- `apps/postprocessing_forecasts/src/data_reader.py`
  - one shared empty-schema helper
  - four reader exits: `_normalize_lr_forecasts`, `_normalize_ml_forecasts`,
    `read_individual_model_forecasts`, `read_individual_model_forecasts_for_dates`
- Relevant tests only.

**Depends on:** Phase 1.

**Agents**

- Agent 1: **sole owner of all `data_reader.py` edits**, including the helper and
  wiring all four exits.
- Agent 2: caller audit only; must not edit `data_reader.py`.
- Agent 3: contract, dtype-parity, concat/merge, and downstream tests; must not
  edit `data_reader.py`.

**Acceptance criteria**

- All four empty exits use one shared helper, not inline per-branch schemas.
- Empty-schema columns are canonical per horizon.
- Empty-schema dtypes follow the dtype-parity rule in §3 — verified by test.
- `model_short` remains object/string-compatible for `.str`.
- `flag` remains numeric: nullable `Int64` if missing values can appear across
  LR/ML joins, otherwise the actual numeric dtype emitted by the non-empty reader.
- Caller audit confirms no dependency on columnless shape or empty-column
  behavior.

### Phase 3 — Stale-EM Lookback Scoping

**Goal:** Reduce unnecessary scoped model reads by applying the same lookback
cutoff semantics as `detect_stale_quantiles()`.

**Files**

- `apps/postprocessing_forecasts/postprocessing_maintenance.py`
  - stale-EM filtering in `_fill_gaps_for_horizon`
- Operator note / release-note location used by the project.
- Tests for boundary and logging behavior.

**Depends on:** Phase 1 only. (Phase 3's data path is `combined` from
`read_combined_forecasts`, which Phase 2 does not touch — see §7.)

**Agents**

- Agent 1: inspect `detect_stale_quantiles()` cutoff calculation and exact
  comparison operator.
- Agent 2: update the stale-EM filter and add detect-but-skip logging.
- Agent 3: write the operator note and follow-up issue text for the one-off wider
  backfill.

**Acceptance criteria**

- Phase 3 may ship as a standalone operational fix once P1 lands.
- The boundary comparison exactly matches `detect_stale_quantiles()`.
- A log records the count of out-of-window stale-EM rows detected but skipped.
- Documentation states pre-window stale EM no longer auto-repairs.
- A separate follow-up issue specifies the one-off wider backfill.
- Confirm Phase 2 does not touch `read_combined_forecasts`; record that Phase 3
  relies on `combined`'s current shape.

## 3. Canonical empty-schema helper

**Location:** `apps/postprocessing_forecasts/src/data_reader.py`

**Helper:** `_empty_individual_model_forecasts_schema(horizon: str) -> pd.DataFrame`

**Base columns:** `code`, `date`, `forecasted_discharge`, `model_short`, `q05`,
`q25`, `q75`, `q95`, `flag`

- Pentad adds: `pentad_in_month`, `pentad_in_year`
- Decad adds: `decad_in_month`, `decad_in_year`

**Dtype rule (parity, not hand-authored):**

- Do not hand-author independent dtypes.
- For every **canonical** column, the empty-frame dtype must equal the dtype the
  readers emit for that column **where the column is present** in their non-empty
  output. Determine and verify dtypes column-by-column using mocked non-empty
  reads.
- `model_short` must remain object/string-compatible (matches the reader and what
  `.str` requires).
- `flag` must remain numeric, matching real reader behavior (`Int64` nullable if
  NaN can appear across LR/ML joins; otherwise the int/float dtype the reader
  emits). It must **not** be object/string.

**Clarification on parity scope (orchestrator note, folded in at promotion).**
The four readers do **not** emit identical column sets — `_normalize_lr_forecasts`
carries LR-specific columns (`predictor`, `slope`, `intercept`, `rsquared`) and may
lack `q05..q95`/`flag`, while `_normalize_ml_forecasts` carries `q05..q95`/`flag`.
A single canonical empty schema therefore cannot match every reader's non-empty
column set one-for-one. Resolve this as follows:

- The canonical empty schema defines the **unified** individual-forecast contract.
- The dtype-parity rule and its test apply **per canonical column, on the
  intersection** of the canonical columns and the columns a given reader actually
  emits. A canonical column absent from a reader's non-empty output simply
  establishes the unified contract and is not subject to a parity comparison for
  that reader; it takes the documented unified dtype (`object`/string for
  `model_short`/`code`, `float64` for `forecasted_discharge`/`q05..q95`, `Int64`
  for `flag` and the period columns).
- The Phase 2 parity test compares dtypes only on the intersecting columns, per
  reader.

**Required helper users:** `_normalize_lr_forecasts`, `_normalize_ml_forecasts`,
`read_individual_model_forecasts`, `read_individual_model_forecasts_for_dates`.

## 4. Test matrix

**Phase 1**

- DECAD maintenance crash reproduction: scoped reader returns columnless
  `pd.DataFrame()`; `_fill_gaps_for_horizon` no longer reaches the ensemble with
  empty modelled data; no `KeyError: 'model_short'`.
- Direct pentad and decad ensemble tests: columnless empty input returns unchanged
  with a warning.
- Full ensemble path with 0-row **schemaful** input for both horizons: no NE rows
  added, no raise.
- Period-column hole: `model_short` present with matching ML models, required
  period columns absent; both horizons return unchanged with a warning.
- Missing `date`/`code`: `model_short` + matching ML models + period columns
  present, but `date` or `code` absent; both horizons return unchanged with a
  warning.
- Maintenance ordering: empty guard runs before `calculate_virtual_stations_data`.
- Virtual-stations verification: `calculate_virtual_stations_data` accepts a 0-row
  schemaful frame without indexing missing `model_short` or period columns.
- Log-capture assertion: the maintenance warning includes horizon label,
  affected-date count, and gap-code count.
- Operational empty-modelled: either the saved no-op output is asserted, or the
  documented unreachable/guard-omitted rationale is recorded.

**Phase 2**

- Parametrized contract test across all four reader exits and both horizons,
  asserting the same canonical empty schema.
- Dtype-parity test: `empty_frame.dtypes` equals the mocked non-empty reader dtypes
  column-by-column **on the intersecting columns** (per §3) — not a hard-coded
  dtype table.
- `.str` safety: empty `model_short` supports string operations.
- Downstream concat test: schemaful-empty + real numeric rows keeps `flag`
  numeric; `max()`, `flag in [1, 2]`, and `flag == 3` evaluate numerically (not
  merely "concat does not raise").
- Merge/concat consumer test with schemaful-empty input.
- Regression updates for any tests expecting `list(df.columns) == []` or
  columnless empties.

**Phase 3**

- Stale-EM exactly on the lookback boundary matches `detect_stale_quantiles()`
  included/excluded behavior.
- Out-of-window stale-EM count is logged.
- In-window stale-EM repair remains unchanged.
- Pre-window stale-EM is detected and skipped, not auto-repaired.

## 5. Caller-audit checklists

**Phase 2 reader-contract audit**

- Search reader callers for columnless assumptions: `== (0, 0)`, `.shape ==`,
  `.columns.empty`, `len(df.columns) == 0`, `if not df.columns`,
  `list(df.columns) == []`.
- Inspect positional/index-sensitive logic: `.iloc`, `reset_index`, `concat`,
  `merge`.
- Explicitly inspect `calculate_virtual_stations_data` and
  `read_observed_and_modelled_data` against columnless and schemaful empty frames.

**Phase 1 ensemble-guard audit**

- Enumerate all importers of `calculate_neural_ensemble_forecast` and
  `calculate_neural_ensemble_forecast_decade` (across modules, not just
  postprocessing).
- Confirm no importer relies on: the current `KeyError` for control flow; NE rows
  always being appended; the warning text for log scraping; intentionally partial
  frames being enriched.

## 6. Dependency graph

```json
{
  "phases": {
    "P1": { "name": "Crash-fix core", "depends_on": [], "parallel_agents": 3 },
    "P2": { "name": "Reader contract", "depends_on": ["P1"], "parallel_agents": 3 },
    "P3": { "name": "Stale-EM lookback scoping", "depends_on": ["P1"], "parallel_agents": 3 }
  },
  "minimum_safe_production_fix": "P1"
}
```

## 7. Risks & review notes

- **Phase 1 is the minimum-safe production fix.** It directly prevents the DECAD
  crash and does not rely on reader behavior.
- `iEasyHydroForecast/setup_library.py` is shared core code. The guard must be
  narrow and preserve unchanged-return semantics for valid inputs.
- **Phase 2 has the largest blast radius** because columnless empty frames become
  schemaful. The dtype-parity tests are the main control against silent
  numeric/`.str` regressions (notably `flag`).
- **Phase 3 changes operational coverage**: stale EM rows before the lookback
  window will no longer auto-repair. Operators need the detect-but-skip log and a
  separate one-off wider backfill to drain the existing backlog.
- Phase 3 relies on `combined` from `read_combined_forecasts`, **not** the four
  individual readers changed in Phase 2. This must be confirmed and documented
  before Phase 3 ships.
- Residual assumptions needing repo inspection during implementation: exact
  operational file containing `read_observed_and_modelled_data`; exact test
  paths/fixtures; project-standard `date` dtype; operator-doc/release-note
  location.
