# Support `uzhm` org in pipeline timeout system

**Priority**: High
**Module**: pipeline (+ iEasyHydroForecast)
**Status**: Draft
**Blocking**: uzhm AWS deployment (week of 2026-04-21)

## Problem

On a fresh uzhm deployment, `LinRegInitial(PENTAD)` times out after 900 s
and fails the `RunInitializeWorkflow`, even after:

1. Installing a `timeout_config.yaml` with `uzhm_aws_override: 21600` for
   `LinRegInitial`.
2. Setting `IEASYHYDROFORECAST_ENVIRONMENT=uzhm_aws` in the pipeline env
   file.

The Luigi task log shows the default 900 s fallback was used:

```
LinRegInitial(timeout_seconds=, max_retries=, retry_delay=, prediction_mode=PENTAD)
...
Container ... timed out after 900 seconds
RuntimeError: Task timed out after 900 seconds (attempt 1/2)
```

The yaml override never reached the task. Three candidate root causes must
be distinguished in Phase 1 before patching anything:

- **C1** `apps/pipeline/src/timeout_manager.py::_detect_environment()`
  (lines 48–86) hardcodes `demo` / `kghm` / `tjhm` only; uzhm silently
  falls back to `demo_ch`. If the yaml's `demo_ch_override` isn't set for
  `LinRegInitial`, the resolver falls through to the hardcoded 900 s default.
- **C2** `timeout_config.yaml` path is unresolvable inside the pipeline
  container. The default lookup is
  `<ieasyforecast_configuration_path>/timeout_config.yaml`; the explicit
  override is `IEASYHYDROFORECAST_TIMEOUT_CONFIG_PATH`. If the file exists
  only on the host or the container-internal path was written into a
  host-form env var, loading silently fails with
  `Using default timeout values.` (timeout_manager.py:41).
- **C3** The file is loaded but `current_env` doesn't match any entry in
  `environments:` (related to C1) and no `<env>_override` is defined, so
  `get_task_parameters` returns the hard-coded default at
  timeout_manager.py:100–105.

All three are latent bugs; any one of them reproduces the observed
behaviour. The fix set below addresses C1 directly (code change) and C2/C3
via louder failure modes so future deployments can self-diagnose.

## Non-goals

- Rewriting the timeout resolution logic. Keep the current environment +
  override + complexity model; only extend / harden it.
- Touching `sapphire/services/` — timeout_config.yaml is pipeline-only.
- Changing task class names referenced by the yaml (these are the Luigi
  `__class__.__name__` lookups and are load-bearing across deployments).
- Backfilling timeout overrides for kghm / tjhm / demo — their existing
  entries continue to work unchanged.

## Phases

### Phase 1 — Diagnose which root cause applies on the uzhm server

**Goal**: Determine empirically whether C1, C2, or C3 is the active failure
path on the live uzhm deployment, so the subsequent code change is
demonstrably correct.

**Files**: none (read-only diagnostics inside `daily-maintenance` container).

**Depends on**: nothing.

**Agents**: no agent — orchestrator drives this with the user via shell
commands.

**Acceptance criteria**:

- [ ] Inside the container, `python -c "from apps.pipeline.src.timeout_manager
  import get_task_parameters; import json; print(json.dumps(
  get_task_parameters('LinRegInitial')))"` is run and the actual resolved
  `timeout_seconds`, `timeout_config`, and whether relative_complexity
  was applied is recorded in this file under "Findings".
- [ ] The TimeoutManager constructor's first print lines (config path +
  detected env) are captured from container logs.
- [ ] Outcome documented: which of C1 / C2 / C3 is responsible (one, or
  more than one).

### Phase 2 — Teach `_detect_environment()` to recognise uzhm

**Goal**: Extend `timeout_manager.py::_detect_environment()` so an uzhm
deployment resolves to `uzhm_local` (production) or `uzhm_aws`
(development) by the same rule used for kghm/tjhm.

**Files**:

- `apps/pipeline/src/timeout_manager.py` — add an `elif org == "uzhm"`
  branch after the tjhm branch (lines 80–82). No other edits.

**Depends on**: Phase 1 confirming C1 is at least partly responsible.

**Agents**: 1 × Sonnet 4.6 agent, isolation: worktree.

**Constraints handed to the agent**:

- Modify *only* `_detect_environment()`. Do not touch any other function,
  class, import, or behaviour.
- Mirror the exact style of the existing kghm / tjhm branches — one-line
  `elif` + ternary on `is_production`.
- Do NOT change `production_tags = ["local"]` — preserved per
  `doc/plans/deployment_new_hydromet_aws.md`.
- The added branch must be reached before the fallback at lines 84–86.

**Acceptance criteria**:

- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` — zero
  failures, zero unexpected skips.
- [ ] New test in `apps/pipeline/tests/test_timeout_manager.py` (added in
  Phase 4) asserts `("uzhm", "latest")` → `"uzhm_aws"` and
  `("uzhm", "local")` → `"uzhm_local"`.
- [ ] `git diff apps/pipeline/src/timeout_manager.py` shows only the new
  branch — no whitespace reflows or unrelated edits.

### Phase 3 — Make config-load failures visible

**Goal**: When `timeout_config.yaml` cannot be loaded (missing path, parse
error) or when `current_env` has no entry in the yaml, emit a WARNING via
the configured pipeline logger (not `print`), including the resolved path
and detected env. Silent fallback to 900 s is the failure mode that cost
a production deployment day — make it loud.

**Files**:

- `apps/pipeline/src/timeout_manager.py` — convert the two `print(…)`
  calls at lines 40–41 and 46 to `logger.warning` / `logger.info`; add a
  WARNING when `get_task_parameters` returns the hardcoded default
  because the task name is absent from the yaml AND the environment is
  absent from `environments:`.

**Depends on**: nothing (independent of Phase 2; can land in parallel).

**Agents**: 1 × Sonnet 4.6 agent, isolation: worktree.

**Constraints handed to the agent**:

- Use `logging.getLogger(__name__)` at module level; do not create a new
  logging handler.
- Preserve existing behaviour for successful loads (the current `print`
  at line 38 stays or is converted to `logger.info` — caller's choice).
- No changes to the default values themselves (900 / 2 / 5).
- Must not raise — this is a warning-only change; the process continues
  with defaults as before.

**Acceptance criteria**:

- [ ] Running the pipeline with `IEASYHYDROFORECAST_TIMEOUT_CONFIG_PATH`
  pointing at a non-existent file produces a visible `WARNING` line in
  the task's docker_logs and in the container stdout.
- [ ] Running with `ieasyhydroforecast_organization=uzhm` against a yaml
  that defines only `demo_ch` / `kghm_*` / `tjhm_*` produces a WARNING
  that the detected env is unmapped AND that per-task overrides will
  fall back to defaults.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline`
  remains green.

### Phase 4 — Tests

**Goal**: Add test coverage for Phase 2 and Phase 3 behaviour so the fix
doesn't regress.

**Files**:

- `apps/pipeline/tests/test_timeout_manager.py` — add cases:
  - `test_uzhm_aws` — `("uzhm", "latest")` → `"uzhm_aws"`
  - `test_uzhm_local` — `("uzhm", "local")` → `"uzhm_local"`
  - `test_missing_config_file_warns` — `IEASYHYDROFORECAST_TIMEOUT_CONFIG_PATH`
    set to nonexistent path raises a log record at WARNING level (use
    `caplog`). No exception raised.
  - `test_unknown_env_warns_and_uses_defaults` — yaml without uzhm entries +
    detected env `uzhm_aws` yields the 900 s default AND a WARNING log.
- `apps/pipeline/tests/conftest.py` — no changes; existing
  `tmp_timeout_config` + `reset_timeout_singleton` fixtures cover this.

**Depends on**: Phases 2 and 3.

**Agents**: 1 × Sonnet 4.6 agent.

**Acceptance criteria**:

- [ ] 4 new tests, all passing.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline`
  shows total test count increased by exactly 4 vs pre-change baseline.
- [ ] No test assertion on specific timeout values beyond what's already
  in the existing `tmp_timeout_config` fixture — use the same fixture
  to keep tests resilient to yaml edits.

### Phase 5 — Deployment doc update

**Goal**: Document how to wire `timeout_config.yaml` on a new deployment so
the uzhm experience (silent 900 s fallback) doesn't repeat.

**Files**:

- `doc/plans/deployment_new_hydromet_aws.md` — add a subsection under
  Phase 5 or Phase 8 titled "Timeout configuration" covering:
  - Where the yaml must live (`<data_folder>/config/timeout_config.yaml`
    on host; the container path via the existing config mount).
  - How to set `IEASYHYDROFORECAST_ENVIRONMENT` explicitly in the env file
    when needed.
  - How to verify inside the container (the one-liner from Phase 1).
- `doc/configuration.md` — add row for
  `IEASYHYDROFORECAST_TIMEOUT_CONFIG_PATH` and for
  `IEASYHYDROFORECAST_ENVIRONMENT` in the .env variable reference table.

**Depends on**: Phase 2 merged (so the table can reference the new
recognised env names).

**Agents**: 1 × Sonnet 4.6 agent.

**Acceptance criteria**:

- [ ] Deployment plan includes a copy-paste yaml skeleton with placeholders
  (`<org>_aws` / `<org>_local`) so each new hydromet starts from a working
  template rather than from scratch.
- [ ] `doc/configuration.md` reference table includes both env vars with
  example values and "Required-if" column correctly filled.

## Dependency graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 0 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": [], "parallel_agents": 1 },
    "P4": { "depends_on": ["P2", "P3"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P2"], "parallel_agents": 1 }
  }
}
```

P2 and P3 can run in parallel worktrees. P4 joins both before merge.

## Findings (filled in during Phase 1)

*To be completed.*

## Related code-gap tasks

- `#13` — add uzhm as a first-class org across the code (this plan covers
  the pipeline subset; other modules need separate follow-ups).
- `#15` — `DockerTaskBase.execute_with_retries` reports success when inner
  container errors (orthogonal but related reliability issue).
- `#17` — `ieasyhydroforecast_connect_to_iEH` documentation gap
  (misleading variable name, tripped this same deployment).
