# Runoff long-horizon hydrograph P4 implementation prompt — operator wrapper and old path retirement

> Paste the section between "--- BEGIN PROMPT ---" and "--- END PROMPT ---"
> to the implementation agent. Dispatch with `isolation: "worktree"` if your
> harness supports it — this phase touches shared infra
> (`apps/pipeline/pipeline_docker.py`) and snow-yearly-task byte-identity
> is a non-negotiable check. Plan at commit `ec03c44`; writer at commit
> `aeceebe`; P3 PROCEED at commit `fc22f6d`.

--- BEGIN PROMPT ---

You are an implementation agent on the SAPPHIRE forecast tools project.
Your role is **Phase 4 only** of the long-horizon runoff hydrograph
plan at
`doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md`.
This phase adds a yearly operator wrapper for the new monthly +
seasonal writer and retires the old norm-only runoff path. Snow
yearly recalculation is unrelated and MUST stay byte-identical.

## What you are doing

**Goal**:

1. Create a new sibling wrapper
   `bin/yearly_runoff_hydrograph_aggregation.sh` so operators can
   invoke the long-horizon writer (P1+P2 + P1-fix) in the yearly
   maintenance window.
2. Document the wrapper in
   `apps/preprocessing_runoff/README.md`.
3. Retire `YearlyMonthlyNormsRecalculation` (the old norm-only
   Luigi task) and its dispatcher key `"monthly_norms"` from
   `apps/pipeline/pipeline_docker.py`.
4. Mark `apps/preprocessing_runoff/sync_monthly_norms.py` as
   deprecated (either delete it OR add a clear deprecated header
   pointing operators to `sync_long_horizon_hydrograph.py`; pick
   the more conservative path — see "Retirement choice" below).
5. Add a static-check test that confirms the retirement happened
   and the snow yearly task is untouched.

**Files you may modify (exhaustive)**

- `bin/yearly_runoff_hydrograph_aggregation.sh` (CREATE)
- `apps/preprocessing_runoff/README.md` (EDIT)
- `apps/pipeline/pipeline_docker.py` (EDIT — surgical: remove
  one Luigi task and one dispatcher key)
- `apps/preprocessing_runoff/sync_monthly_norms.py` (EITHER
  DELETE entirely OR add deprecated header; do not change other
  behavior)
- A test file for the static-check (your choice of location —
  see "Test placement" below)

You may NOT modify any other file. In particular:

- **No edits to `apps/iEasyHydroForecast/forecast_library.py`** —
  the old `write_month_hydrograph_data` helper there is a
  dependency of `sync_monthly_norms.py` and is also used by snow
  / pentad / decad paths; do not touch it. Its retirement is
  out of scope for this plan.
- **No edits to `bin/yearly_snow_norm_recalculation.sh`**, the
  snow Luigi task `YearlySnowNormRecalculation`, the snow
  container `maintenance-snow-norms`, or any file under
  `sapphire/services/`.
- **No edits to `sync_long_horizon_hydrograph.py`** (the new
  writer at commit `aeceebe`) — it is the dispatch target of
  the new wrapper, not the wrapper's subject.
- **No edits to plan documents, decisions artifact, prior
  evidence files, or other planning files.**

## Source-of-truth references

- **Plan**: `doc/plans/issues/high_prio_gi_draft_runoff_long_horizon_hydrograph.md`
  (commit `ec03c44`). See §Phase 4 for the full contract.
- **Decisions artifact**: `doc/plans/working/runoff_long_horizon_hydrograph_decisions.md`
  (commit `4c49a4c`). D-Q3 (code org), D-Q5 (sibling wrapper).
- **Snow wrapper precedent**:
  `bin/yearly_snow_norm_recalculation.sh` (whole file). Match
  its shape — log naming, container naming, banner, command
  text — but with runoff-specific values, not snow.
- **New writer**:
  `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`
  (commit `aeceebe`). The wrapper's command is
  `uv run python sync_long_horizon_hydrograph.py` with
  optional `--target-year`.
- **Pipeline docker module**:
  `apps/pipeline/pipeline_docker.py:2034` (snow yearly task —
  DO NOT TOUCH), `:2040-2045` (old runoff
  `YearlyMonthlyNormsRecalculation` — REMOVE), and `:2090`
  (`"monthly_norms"` dispatcher key — REMOVE).

## Retirement choice — delete or deprecate sync_monthly_norms.py?

The plan allows either deletion or a deprecated header. Pick
**deprecated header** for this phase. Rationale: deletion is
irreversible by `git revert` semantics if there's a hidden
caller, and a deprecated header with a clear pointer to the
new script is the lower-risk move. A future cleanup phase can
delete the file once we've confirmed no remaining callers in
operational deployment scripts.

**Deprecated header shape** (add at the top of
`sync_monthly_norms.py`, after the existing imports / docstring):

```python
# DEPRECATED (2026-06-02). Use sync_long_horizon_hydrograph.py
# instead. This script wrote norm-only monthly hydrograph rows
# (`previous` and `current` were always None). The replacement
# writes the full triad (norm + previous + current) for monthly
# rows and additionally writes seasonal April-September rows.
# Operator wrapper: bin/yearly_runoff_hydrograph_aggregation.sh.
# This script may be deleted in a follow-up cleanup phase.
```

Place the header as a top-of-file comment block (after the
existing module docstring if any). Do NOT change any other
code in `sync_monthly_norms.py`. Do NOT add the deprecated
header to the module's `__doc__` (the comment block is more
visible to operators who `cat` the file).

## Wrapper shape

Match `bin/yearly_snow_norm_recalculation.sh` line-by-line:

- `#!/usr/bin/env bash` shebang.
- `set -euo pipefail`.
- Same date / log naming style — runoff-specific. Snow uses
  `LOG_FILE="${HOME}/sapphire/logs/yearly_snow_norm_recalculation_${TIMESTAMP}.log"`
  or similar — mirror the style with `yearly_runoff_hydrograph_aggregation`.
- Banner / help text on `--help`. State plainly that the script
  writes **monthly and seasonal runoff hydrograph triads**
  (norm + previous + current), not just norms.
- The actual invocation uses Docker (the snow wrapper invokes
  a `maintenance-snow-norms` container; the runoff wrapper
  should invoke a runoff-equivalent container). Match the
  pattern:
  - Snow: container `maintenance-snow-norms` from image
    `sapphire-pipeline` running
    `["uv", "run", "recalculate_snow_norms.py"]`.
  - Runoff: container `maintenance-monthly-norms`
    (the SAME container name the old runoff path already
    uses at `apps/pipeline/pipeline_docker.py:2040-2045`)
    from image `sapphire-preprunoff` running
    `["uv", "run", "sync_long_horizon_hydrograph.py"]`.

The wrapper accepts an optional `--target-year YYYY`
positional argument and forwards it to the Python script. If
not provided, the script defaults to current year (already
the writer's behaviour).

Exit codes:
- 0 on success
- Non-zero on docker invocation failure, mirroring snow.

## Pipeline retirement details

In `apps/pipeline/pipeline_docker.py`:

1. **Lines 2040-2045** (Luigi task `YearlyMonthlyNormsRecalculation`):
   Remove the entire class definition / task instantiation block
   for the OLD runoff task. Read the snow yearly task at
   line ~2034 first to confirm you're targeting the right block;
   the snow class is `YearlySnowNormRecalculation` and uses
   `command=["uv", "run", "recalculate_snow_norms.py"]`. The
   runoff class is `YearlyMonthlyNormsRecalculation` and uses
   `command=["uv", "run", "sync_monthly_norms.py"]` (the OLD
   script).

2. **Line 2090** (dispatcher key `"monthly_norms"`): Remove the
   key from whatever dispatcher dict / mapping it lives in. Do
   NOT remove or rename other dispatcher keys. If the mapping
   becomes empty as a side effect of this removal, leave the
   dict in place (don't restructure surrounding code).

3. **Surrounding context**: nothing else in
   `pipeline_docker.py` should change. Do not reorder imports,
   reformat unrelated lines, or rename other variables. The
   diff should be surgical.

If you cannot locate the exact lines (they may have shifted),
search by name:

```bash
grep -n 'YearlyMonthlyNormsRecalculation\|"monthly_norms"' apps/pipeline/pipeline_docker.py
```

These two grep matches should both disappear after your edit.
Also grep for `sync_monthly_norms.py` — only the deprecated
header reference should remain; any other reference (e.g. a
container `command=[...]` invocation) is part of the
retirement and should be removed if you find one.

## README documentation

In `apps/preprocessing_runoff/README.md`, add a short section
documenting the new wrapper. Match the existing README style
(check the surrounding sections to mirror tone and depth). The
section should include:

- Wrapper name and purpose ("yearly long-horizon hydrograph
  aggregation: monthly + seasonal triads").
- Invocation example (with optional `--target-year`).
- Cadence (yearly maintenance window per D-Q2).
- Prerequisites: SAPPHIRE preprocessing API up, iEH HF SSH
  tunnel up.
- Behavior: skip-and-continue for stations without monthly
  norms (cite the P1-fix at commit `aeceebe`).
- A note that the deprecated `sync_monthly_norms.py` is the
  predecessor; operators should use the new wrapper going
  forward.

Aim for ~15-25 lines of new README content. Do NOT rewrite
existing README sections.

## Test placement

Add a single static-check test in
`apps/preprocessing_runoff/test/test_yearly_monthly_norms_retired.py`
(NEW file). It can be a simple `pytest` test that:

```python
import pathlib
import re

import pytest

PIPELINE_DOCKER = pathlib.Path("apps/pipeline/pipeline_docker.py")


def test_yearly_monthly_norms_task_class_is_gone():
    content = PIPELINE_DOCKER.read_text()
    assert "YearlyMonthlyNormsRecalculation" not in content, (
        "Old runoff yearly monthly norms Luigi task class is still present. "
        "This was retired in Phase 4 of the runoff long-horizon hydrograph plan."
    )


def test_monthly_norms_dispatcher_key_is_gone():
    content = PIPELINE_DOCKER.read_text()
    # Use a strict pattern: the key is `"monthly_norms"` (with quotes), not a
    # substring like `_monthly_norms_` somewhere else.
    assert '"monthly_norms"' not in content, (
        "Old runoff monthly_norms dispatcher key is still present. "
        "This was retired in Phase 4 of the runoff long-horizon hydrograph plan."
    )


def test_snow_yearly_task_is_byte_identical():
    """Snow yearly recalc must NOT be touched by the runoff retirement."""
    content = PIPELINE_DOCKER.read_text()
    # Confirm the snow yearly recalc class and its command are still present.
    assert "YearlySnowNormRecalculation" in content
    assert "recalculate_snow_norms.py" in content
```

The relative path `apps/pipeline/pipeline_docker.py` assumes
the test is run from the repo root (which is how
`run_tests.sh` invokes pytest). Verify the relative path
resolves before submitting; if it doesn't, use a path-walk
helper that finds the repo root.

You may also add a shell-level shellcheck test in CI if there
is an existing pattern, but it is not required by the
plan — only that `bash -n` and `shellcheck -x` are clean on
the new wrapper.

## Self-review before returning

1. **Scope check**: `git status --short` should show:
   - One new file: `bin/yearly_runoff_hydrograph_aggregation.sh`
   - Modified: `apps/preprocessing_runoff/README.md`
   - Modified: `apps/pipeline/pipeline_docker.py`
   - Modified (deprecated header only): `apps/preprocessing_runoff/sync_monthly_norms.py`
   - One new file: `apps/preprocessing_runoff/test/test_yearly_monthly_norms_retired.py`
   No other files touched.

2. **Wrapper syntax**: `bash -n bin/yearly_runoff_hydrograph_aggregation.sh`
   exits 0. `shellcheck -x bin/yearly_runoff_hydrograph_aggregation.sh`
   exits 0 (install shellcheck via brew if missing; if you
   cannot run shellcheck, document this in the deliverable and
   skip — do NOT skip `bash -n`).

3. **Retirement grep**:
   `grep -n 'YearlyMonthlyNormsRecalculation\|"monthly_norms"' apps/pipeline/pipeline_docker.py`
   returns zero matches. The retirement test exercises this
   assertion.

4. **Snow byte-identity**: `git diff -U0 -- apps/pipeline/pipeline_docker.py`
   shows ONLY removals related to the retired runoff task and
   dispatcher key. No edits to `YearlySnowNormRecalculation` or
   `recalculate_snow_norms.py` references. The retirement test
   exercises this assertion too.

5. **README**: `grep -q 'yearly_runoff_hydrograph_aggregation'
   apps/preprocessing_runoff/README.md` exits 0.

6. **Deprecated header**: `grep -n 'DEPRECATED' apps/preprocessing_runoff/sync_monthly_norms.py`
   shows the header you added. No other code changes in that
   file.

7. **Test runner**: `cd apps && SAPPHIRE_TEST_ENV=True bash
   run_tests.sh preprocessing_runoff` passes. Expected: 326
   passed (323 previous + 3 new retirement tests), 2 skipped.
   Cross-module impact: `pipeline_docker.py` is in
   `apps/pipeline/`, not `apps/preprocessing_runoff/`, so the
   pipeline module's own test suite may also need to be sane.
   Run `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh
   pipeline 2>&1 | tail -10` as a cross-check; report the
   result in the deliverable.

8. **No real station codes**: not applicable to this phase (no
   data files touched), but confirm anyway by grepping the new
   wrapper and README content.

## Hard constraints (non-negotiable)

1. **Do NOT modify any file outside the exhaustive list above.**
2. **Do NOT touch the snow yearly task, snow wrapper, or any
   snow-recalc file.** Byte-identity is enforced by the
   retirement test.
3. **Do NOT add `# noqa` comments or new dependencies.**
4. **Do NOT delete `sync_monthly_norms.py` outright.** Use the
   deprecated header per "Retirement choice" above.
5. **Do NOT change other Luigi tasks** in `pipeline_docker.py`.
   Surgical edit only.
6. **Do NOT commit, push, branch, stage, or stash.** The
   orchestrator commits after deliberation.
7. **Do NOT skip `bash -n` or the retirement tests.** They must
   pass before you return.

## Deliverable format

Return a single short Markdown report (under ~150 lines):

1. **Summary** — 2-3 sentences: wrapper created; old Luigi task
   + dispatcher key retired; sync_monthly_norms.py deprecated;
   all tests pass.
2. **Files modified** — paths with line-count summary.
3. **Scope check** — confirm exactly the 5 files listed above;
   no others touched.
4. **Snow byte-identity** — confirm `git diff -U0 --
   apps/pipeline/pipeline_docker.py` shows only retirement
   removals (paste the diff if short).
5. **Wrapper syntax** — `bash -n` exit code; `shellcheck -x`
   exit code (or note if shellcheck unavailable).
6. **Retirement grep** — paste the result of `grep -n
   'YearlyMonthlyNormsRecalculation\|"monthly_norms"' apps/pipeline/pipeline_docker.py`
   (expected: nothing).
7. **README grep** — confirm
   `grep -q 'yearly_runoff_hydrograph_aggregation'
   apps/preprocessing_runoff/README.md` exits 0.
8. **Test runs** — paste tails of `run_tests.sh
   preprocessing_runoff` and `run_tests.sh pipeline` showing
   pass/fail/skip totals.
9. **Sensitive-data check** — confirm no real station codes.
10. **Coordination items** (optional) — anything the
    orchestrator should know (e.g. shellcheck not installed,
    pipeline tests had pre-existing skips, etc.).

## What success looks like

- One new wrapper script at
  `bin/yearly_runoff_hydrograph_aggregation.sh`, `bash -n`
  clean and shellcheck clean.
- README documents the wrapper.
- `pipeline_docker.py` no longer references
  `YearlyMonthlyNormsRecalculation` or `"monthly_norms"`.
- `sync_monthly_norms.py` has a deprecated header pointing
  operators to the new script; no other behavioural change.
- 3 new retirement tests pass; the existing 323
  preprocessing_runoff tests still pass; the pipeline module
  tests still pass.
- Snow yearly recalc is byte-identical to before this commit.
- No real station codes; no plan edits; no shared-service
  edits.
- Phase 5 (dashboard handoff stub) can dispatch in parallel,
  and after both P4 and P5 land, the plan is complete.

If you encounter an ambiguity (e.g. `pipeline_docker.py`'s
dispatcher key lives in a different structure than expected,
or the snow wrapper's docker invocation pattern uses an
unusual flag), STOP and escalate to the orchestrator with a
specific question. Do NOT guess on the retirement; an
incorrect edit to `pipeline_docker.py` could break operational
Luigi scheduling.

--- END PROMPT ---
