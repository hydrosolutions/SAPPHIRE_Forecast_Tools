# Plan: extract the long-term mode-activity decision into a single authority (INFRA-022 option (d))

**Status**: Draft, revision 3 — **not approved, no code written**
**Date**: 2026-08-18 (rev 2 answered a review that returned "not safe to execute as written";
rev 3 answers a second review of rev 2, which confirmed the central fix but found the seam problem
in § "The new module" and several gaps below)
**Scope**: this plan is **only** the behavior-preserving extraction (P0–P3) plus its contract gate.
It ships **no user-visible change**: after it lands the scheduler behaves identically and *nothing
new consumes the authority yet*.
**Owner decision this implements**: INFRA-022 § "OPEN DECISION — which schedule authority?", option
**(d)** — extract the mode-activity decision into `iEasyHydroForecast` beside the resolver so that
`lt_schedule_query` and, **later**, `validate_pipeline` share one definition. *(Rev 3: the opening
previously read as though this plan wires both callers. It wires only the scheduler; validation
consumption is INFRA-022, and the input it needs is INFRA-028.)*
**Related**: **INFRA-028** (run manifest — the split-out Phase 4; validation's actual input),
**INFRA-022** (schedule-aware gating; consumes both), **INFRA-021** (must ship atomically with the
gating), **LTF-007** (10-vs-5 window mismatch), INFRA-020 (independent; but design INFRA-028's
manifest so C3 can reuse it).

---

## Why this is delicate

`lt_schedule_query.query_schedule()` is **live operational scheduling code**. It decides which
long-term modes run in production. Its stdout JSON is parsed by:

- `apps/run_locally.sh:285` (with a documented fallback to `is_lt_issue_window` on failure),
- `apps/pipeline/pipeline_docker.py:2124` — the Luigi `LTScheduleQuery` task, in Docker,
- `apps/run_docker_tests.sh:681`.

A refactor that changes *which modes are active*, or the *shape of that JSON*, silently changes what
the production pipeline runs. **The entire plan is therefore structured around proving behavioral
identity before anything is rewired**, not around the elegance of the end state.

## What is actually being extracted

Today the decision lives in one loop (`lt_schedule_query.py:88-131`) and mixes three concerns:

| Concern | Today | After |
|---|---|---|
| Config **loading and mutation** (mode JSON, per-model configs) | `ForecastConfig` | unchanged, stays in `long_term_forecasting` |
| The **decision predicates** (tolerance, wrap distance, `forecast_months` membership, non-operational set) | inline in the loop | new pure module in `iEasyHydroForecast` |
| **Iteration and short-circuiting** (which modes/models are consulted, and in what order) | `query_schedule` | **unchanged — stays in the caller** |
| **Output shaping** (`active_modes` / `skipped_modes` / `skill_metric_types` JSON) | `query_schedule` | unchanged |

> **Rev 2 correction — the first row is not "just loading".** `ForecastConfig` is stateful and
> **mutating**: one instance is reused across modes, `load_forecast_config` overwrites its
> current-mode state, and `synchronize_forecast_settings` **rewrites every model's
> `general_config.json` on disk** (`config_forecast.py:157-180`). Moving *when* inputs are gathered
> can therefore change on-disk side effects, not merely return values. This is the single strongest
> argument for leaving iteration in the caller.

> **Rev 2 correction — the predicate must not be mode-level.** A mode-level
> `mode_activity(today, mode, issue_day, model_forecast_months)` cannot preserve two short-circuits
> that exist today:
>
> - **Non-operational modes touch no config at all.** The `NON_OPERATIONAL_MODES` check happens
>   *before* `load_forecast_config` (`lt_schedule_query.py:88-91`). Any signature requiring
>   `issue_day` forces a config load for a mode the scheduler deliberately never loads.
> - **Model evaluation is lazy and ordered.** The loop breaks on the first unrestricted or
>   qualifying model (`:109-121`); later models are never read, so their exceptions and file states
>   are never observed. Materializing all `forecast_months` up front reads every model.
>
> **Therefore the extracted API is a set of small predicates the caller drives**, not one function
> that owns the loop:
>
> - `is_non_operational(mode) -> bool`
> - `day_distance(today_dom, issue_day) -> int` (moved verbatim)
> - `within_issue_window(distance) -> bool` (owns `ISSUE_DAY_TOLERANCE`)
> - `is_unrestricted(forecast_months) -> bool` — true for a falsy value **or** the exact ordered
>   list `[1..12]`; this is the pair of shortcuts that precede any date computation
> - `skip_reason_*` helpers, or the reason strings as module constants, so wording stays
>   single-sourced
>
> **Rev 3 correction — the nearest-date call must stay in the caller.** Rev 2 proposed a
> `model_scheduled_this_month(today, issue_day, forecast_months)` that would call
> `nearest_scheduled_issue_date` internally. Production outcomes would be identical, but the call
> would resolve through `long_term_schedule`'s binding instead of `lt_schedule_query`'s
> (imported at `lt_schedule_query.py:35`, invoked at `:118-121`) — **moving the monkeypatch seam**.
> That silently breaks P0's mock-based characterization and therefore contradicts P3's rule that
> every P0 test passes unmodified. Keep the call in `query_schedule`, so the loop reads:
>
> ```python
> for model_name in models:
>     forecast_months = config.get_forecast_months(model_name=model_name)
>     if lts.is_unrestricted(forecast_months):
>         any_model_scheduled = True
>         break
>     nearest = nearest_scheduled_issue_date(today, issue_day, forecast_months)  # caller's binding
>     if lts.within_issue_window(abs((today - nearest).days)):
>         any_model_scheduled = True
>         break
> ```
>
> This preserves the patch seam, the lazy `break`, model order, and `pandas.Timestamp` arithmetic.
> It also resolves rev 2's unplaced "define date normalization" requirement: normalization is a
> non-issue here, because the only date arithmetic stays where it already is. A second consumer
> that passes `datetime.date` must normalize at **its** boundary, not inside these predicates.
>
> `query_schedule` keeps its loop, its `try/except`, its ordering and its output dict.

Only the **decision predicates** row moves. `ForecastConfig` does **not** move — it lives in
`apps/long_term_forecasting/config_forecast.py` and moving it would drag the long-term module's
model-artifact handling into `iEasyHydroForecast`, which is the opposite of what (d) is for.

### The new module

`apps/iEasyHydroForecast/long_term_schedule.py`, beside `long_term_horizon_resolver.py`
(`validate_pipeline` already imports from this package at `validate_pipeline.py:35-49`, so no new
dependency direction is created):

- `ISSUE_DAY_TOLERANCE` — moved from `lt_schedule_query.py:52`, currently 10.
  **Rev 2 correction:** this single-sources the value between the *scheduler* and future consumers
  only. It is **not** repository-wide: model execution independently hard-codes 5
  (`lt_utils.py:202` — `:196` is the comment above it) and the migration path defines its own `_ALWAYS_SKIP_MODES`
  (`bin/utils/migration_py/long_forecast.py:69-72`). The 10-vs-5 divergence is filed as **LTF-007**
  and is explicitly **not** fixed here.
- `NON_OPERATIONAL_MODES` — moved from `lt_schedule_query.py:57`.
- `day_distance()` — moved verbatim, including the `30 - diff` wrap approximation. **Do not "fix"
  the approximation in this change**; that is a behavior change and belongs in its own issue.
  It must remain importable as `lt_schedule_query.day_distance` — `test_lt_schedule_query.py:14`
  imports it from there.
- `nearest_scheduled_issue_date()` — moved from `long_term_forecasting/lt_utils.py:134-174`, with a
  re-export shim in `lt_utils`.
  **Rev 2 correction:** rev 1 listed the wrong importers (it grepped the module, not the symbol).
  The only direct importers of *this symbol* are `lt_schedule_query.py:35` and
  `tests/test_lt_utils.py:13`; `lt_utils` also uses it internally at `:191-193`. The shim still
  matters, because **every** `lt_utils` importer breaks if the new module is unimportable in their
  import context — and they do not all set up `sys.path` the way `lt_schedule_query.py:27-35` does
  (compare `run_forecast.py:31-54`, `calibrate_and_hindcast.py:22-41`,
  `dev_code/post_process_csv_files.py:34`, `tests/test_quantile_bounds.py:11`).
- The predicate set listed above. All **pure**: no env reads, no file I/O, no `ForecastConfig`.
  Reason strings must reproduce today's exactly (`:90`, `:97`, `:104`, `:124`) because they surface
  in operator-facing JSON.
- **`forecast_months` semantics must be preserved exactly** (`:113-124`): any falsy value counts as
  unrestricted; the exact ordered list `[1..12]` short-circuits; anything else defers to
  `nearest_scheduled_issue_date`; **zero models means inactive**. Define whether the parameter is
  one list per call (model-level, as recommended) and keep model order.
- **Date type**: the scheduler passes `pandas.Timestamp` (`:150-153`), `validate_pipeline` carries
  `datetime.date` (`:1586`). Define and test normalization *now*, even though the second consumer
  is deferred to INFRA-028.

---

## What was Phase 4 — now INFRA-028

Rev 1 ended with a phase making `validate_pipeline` consume the new authority. Review found it was
not an executable phase: it left the mechanism open, named its producer as "possibly", and
delegated its acceptance criteria to another issue.

It also found the underlying idea insufficient. `forecast_months` is **not** in the mode JSON —
`ForecastConfig.get_forecast_months()` reads it from each model's `general_config.json` inside that
model's artifact directory (`config_forecast.py:278-288` → `:110-140`), while the mode JSON that
`long_term_horizon_resolver` reads *is* the same file `ForecastConfig.load_forecast_config` reads
(both resolve `ieasyforecast_configuration_path` / `ieasyhydroforecast_ml_long_term_configuration`).
So issue day and lead time are shared ground; per-model `forecast_months` are not. But more
fundamentally, **a re-derived schedule cannot see manual overrides, execution outcome, or the date
output was actually written under** (late forecasts snap back to the scheduled date,
`lt_utils.py:211-217`).

That work is therefore **split out into INFRA-028** (`high_prio_gi_draft_infra_long_term_run_manifest.md`)
as a run-scoped manifest emitted by whatever actually ran. **INFRA-022 depends on INFRA-028, and
INFRA-021 must ship atomically with INFRA-022.** This plan is a prerequisite of all three and
depends on none of them.

**This extraction remains worth doing on its own**: it single-sources the tolerance and the
non-operational set between the scheduler and its future consumers, which is where silent drift
starts — while changing no behavior today.

---

## Phases

### P0 — Characterization (no production code changes)

- **Goal**: pin today's behavior so any later drift is caught by a failing test, not by a
  deployment. This phase must land and pass **before** P1 touches anything.
- **Files**: `apps/long_term_forecasting/tests/test_lt_schedule_query.py` (extend only).
- **Depends on**: —
- **Agents**: 1.
- **Work**: two groups.

  **(a) Outcome matrix** — {inside window, `dist == tolerance`, `dist == tolerance + 1`,
  wrap-around across month end, mode in `NON_OPERATIONAL_MODES`, config-load error,
  `forecast_months` falsy/absent, `forecast_months == [1..12]` exactly, `forecast_months` excluding
  this month, restricted match in the previous/next month, a year-boundary match, `issue_day`
  clamping in short months, several models where only one qualifies, **zero models**, and an unknown
  `horizon_type` (active, but contributes no skill type)}. Assert `active_modes`, the **exact**
  `skipped_modes` reason strings, and `skill_metric_types`.

  **Negative controls for the `[1..12]` shortcut (rev 3):** the current check is exact equality
  against `list(range(1, 13))` (`:113-118`). Include a **permutation** (`[12, 11, …, 1]`) and a
  **tuple** of the same months, both of which must still reach `nearest_scheduled_issue_date`.
  Without these, a set-based or sorted implementation passes every outcome assertion while being
  measurably more permissive than today.

  **(b) Call-order and exception characterization** — added in rev 2, because this is what a
  refactor breaks and outcome assertions do not catch. Using mock assertions on the
  `ForecastConfig` double, prove: a non-operational mode triggers **no** `load_forecast_config`
  call at all; a too-distant mode triggers **no** model reads; once one model qualifies, later
  models' getters are **never called**. Also pin **ordering**: `active_modes` follows
  `LT_supported_modes` order, `skipped_modes` preserves insertion order, `skill_metric_types` its
  current dedup/sort.

  **Load count and order (rev 3).** On-disk mutation is this plan's principal risk, and every
  successful load writes (`config_forecast.py:110-111` → `:157-180`). So assert **exactly one**
  `load_forecast_config` call per consulted operational mode, in `LT_supported_modes` order, and
  that a caught load failure still **continues** to the next mode rather than aborting.

  **Exception expectations, stated as a table (rev 3)** — rev 2 said only "retain propagate-or-
  suppress behavior", which is not mechanically checkable. Current behavior:

  | Raised by | Today |
  |---|---|
  | `load_forecast_config` (`:93-98`) | **suppressed** → becomes a `skipped_modes` reason, loop continues |
  | `get_operational_issue_day` (`:100`) | propagates out of `query_schedule` |
  | `get_models_to_run` (`:109`) | propagates |
  | `get_forecast_months` (`:112`) | propagates |
  | `nearest_scheduled_issue_date` (`:118`) | propagates |
  | `get_horizon_type` (`:128`) | propagates |

  **The invalid-month case belongs here, not in the outcome matrix (rev 3).** A month list that
  leaves no candidate makes `min(candidates, …)` raise (`lt_utils.py:156-174`), and that call is
  outside the only `try` — so it **propagates** and there is no returned result to assert against.
  Rev 2 listed it among cases whose output fields must be asserted, which is not achievable.

  Also add a CLI test that `main()` stdout parses and contains the three keys with correct types.
  *(Rev 2: assert the keys are **present**, not that the set is exactly three — Luigi tolerates
  extra keys (`pipeline_docker.py:2275-2303`), `run_locally` uses `dict.get` (`:303-315`), and the
  Docker smoke reads only `active_modes` (`:683-693`). An exact-set assertion would be stricter
  than the real consumer contract.)*
- **Acceptance**: new tests pass against **unmodified** source. Run
  `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting`. Placeholder station
  codes only (`19999`).

### P1 — Create the pure module (no callers rewired)

- **Goal**: `long_term_schedule.py` exists, is pure, and is unit-tested in isolation.
- **Files**: `apps/iEasyHydroForecast/long_term_schedule.py` (new);
  `apps/iEasyHydroForecast/tests/test_long_term_schedule.py` (new);
  **`apps/iEasyHydroForecast/pyproject.toml`** — rev 2 addition: that file has an **explicit wheel
  include list** (`:40-48`) which would silently omit a new module.
- **Depends on**: P0.
- **Agents**: 1.
- **Work**: implement the predicate set per § "The new module" — predicates only, no loop. Reason
  strings copied character-for-character from `lt_schedule_query.py:90`, `:97`, `:104`, `:124`.
  **Copy** `nearest_scheduled_issue_date` here while leaving the `lt_utils` definition
  authoritative; P2 collapses the duplicate. *(Rev 2: rev 1 had P1 needing the helper it did not
  move until P2.)*
- **Acceptance**: unit tests cover the P0 matrix at predicate level; the module imports with **no**
  env vars set, without `setup_library`, and without filesystem access — prove the last with an
  explicit probe, not an assertion in prose.
  **Packaging (rev 3):** state the mapping before building, don't just append a filename. Flat
  consumers reach the module via a `sys.path` insert of the package directory
  (`lt_schedule_query.py:27-35`); package consumers import `iEasyHydroForecast.*`
  (`validate_pipeline.py:35-49`). Build a **non-editable wheel** and prove both namespaces resolve
  from it. This probe is the *only* wheel proof in the plan — see the P4 correction. Nothing else
  in the repo imports the module yet.

### P2 — Collapse the duplicate helper behind a compatibility shim

- **Goal**: one definition of `nearest_scheduled_issue_date`, no importer broken.
- **Files**: `apps/iEasyHydroForecast/long_term_schedule.py`,
  `apps/long_term_forecasting/lt_utils.py`.
- **Depends on**: P1.
- **Agents**: 1.
- **Work**: `lt_utils` re-exports from the new module, keeping the name importable as
  `lt_utils.nearest_scheduled_issue_date` for `lt_schedule_query.py:35`,
  `tests/test_lt_utils.py:13`, and `lt_utils`' own internal use at `:191-193`.
  **Import-context risk (rev 2):** `lt_schedule_query.py:27-35` inserts the `iEasyHydroForecast`
  directory on `sys.path` before importing, but other `lt_utils` importers do not
  (`run_forecast.py:31-54`, `calibrate_and_hindcast.py:22-41`,
  `dev_code/post_process_csv_files.py:34`, `tests/test_quantile_bounds.py:11`). If the shim's
  import fails in their context, **every** `lt_utils` importer breaks, not just the two that use
  this symbol. Test both entry styles.
  **Rev 3 nuance:** the flat-import risk is smaller than rev 2 implied — `lt_utils` imports
  `config_forecast` first (`lt_utils.py:14`), and that module already appends the flat-module
  directory (`config_forecast.py:13-24`). The shim must still be written explicitly rather than
  relying on that ordering.
- **Files (rev 3 addition)**: an import-probe test. Rev 2 said "test both entry styles" but
  allocated no file for it.
- **Acceptance**: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting` green
  with **no test edits**. If any test needs editing, stop — that means behavior moved, not just
  code. Plus an explicit probe importing `lt_utils` in **both** contexts: flat (as
  `lt_schedule_query` does) and package-style.

### P3 — Rewire `lt_schedule_query` to the single authority

- **Goal**: `query_schedule` keeps loading config and shaping JSON, but delegates the decision.
- **Files**: `apps/long_term_forecasting/lt_schedule_query.py`, plus the three files carrying stale
  line-number citations to the moved constants *(rev 3: rev 2's residual-risk section ordered these
  updates in P3 while the phase's own file list forbade them)*:
  `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py:256-258`,
  `bin/utils/migration_py/long_forecast.py:11-12`,
  `bin/initialize_long_forecast_history.sh:25-28`.
- **Depends on**: P2.
- **Agents**: 1.
- **Work**: replace the inline predicates with calls into the new module. **Keep the loop, the
  iteration order, the lazy `break`, the `try/except` around `load_forecast_config`, which calls
  sit outside it, the output dict, and the stderr-only logging.** The diff should read as
  "expression replaced by function call", never as restructuring.
- **Acceptance**: **every P0 test passes unmodified** — both the outcome matrix and the call-order
  group. If a P0 test needs changing, the refactor changed behavior and must be reworked, not the
  test. Module attributes that existing tests reach for must survive: `ForecastConfig` and `sl`
  are patched as `lt_schedule_query.*` (`test_lt_schedule_query.py:116-117`) and `day_distance` is
  imported from it directly (`:14`); `HORIZON_TYPE_TO_SKILL` likewise. Do not "clean up" imports.

### P4 — Contract gate *(was P5)*

- **Goal**: prove the operational consumers are unaffected by a change that should be invisible.
- **Depends on**: P3.
- **Agents**: 1.
- **Acceptance**:
  - `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero fail. Record the expected
    environment-gated skip set **before** starting (the runner has known env-dependent dashboard
    skips) and compare against it, rather than asserting "zero skips" in prose.
  - `apps/pipeline` tests, including the Luigi `LTScheduleQuery` task and the structural test at
    `test_lt_schedule_workflow.py:263-275`.
  - A **freshly built** `ltforecast` image, then the Docker smoke at `run_docker_tests.sh:661-701`,
    which invokes the CLI with `--today 2026-03-25` and parses `active_modes` at `:683-693`.
    **Rev 3 correction: this is not wheel proof.** That image copies the source and `uv sync`s with
    `iEasyHydroForecast` declared as an **editable** path dependency
    (`long_term_forecasting/Dockerfile:14-19`, `pyproject.toml:66-68`), so it would pass even with
    the module missing from the wheel include list. The only wheel proof is P1's non-editable probe.
    What this image *does* prove is that the CLI still runs end-to-end in its deployed form.
  - Extend the smoke with a **no-active-modes** case — but **fixture-driven** (rev 3). Activity
    depends on the deployment config mounted via `INTEGRATION_ENV_PATH` (`:666-681`), so no fixed
    date is reliably inactive across deployments. Supply a controlled config rather than assuming.
  - The smoke currently **skips silently when the image is absent**; a skip must not be read as a
    pass.

### (Deferred — not in this plan) Consume the authority from `validate_pipeline`

Tracked as **INFRA-022**, which now depends on **INFRA-028**. See § "What was Phase 4".

---

## Explicit non-goals

- **Do not** change `ISSUE_DAY_TOLERANCE` from 10 to 5 in this change, even though the comment says
  it should be. Single-source it here; change the value in a separate, visible commit.
- **Do not** fix the `30 - diff` month-wrap approximation. Behavior change; own issue.
- **Do not** move `ForecastConfig`.
- **Do not** alter the CLI JSON keys.

## Residual risks

1. **Exception behavior is subtler than rev 1 claimed.** Rev 1 said a missing model config raises
   out of `query_schedule` because `get_models_to_run()`/`get_forecast_months()` sit outside the
   `try`. They do sit outside it (`:93-121`) — but `load_forecast_config` itself calls
   `synchronize_forecast_settings`, which reads every model's config files
   (`config_forecast.py:110-111`, `:157-180`). So an artifact missing at load time normally raises
   **inside** the caught block and becomes a skipped-mode reason, not a CLI failure. An uncaught
   failure remains possible if a file disappears between load and access. P0(b) must pin whichever
   behavior is current, not the assumed one.
2. **Stale cross-references — three, not one.** The non-operational-modes concept is cited by line
   number at `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py:256-258`,
   `bin/utils/migration_py/long_forecast.py:11-12`, and `bin/initialize_long_forecast_history.sh:25-28`.
   Update all three in P3.
3. **A second live definition survives this plan.** `bin/utils/migration_py/long_forecast.py:69-72`
   defines its own `_ALWAYS_SKIP_MODES = {"monthly"}`, applied at `:197-203`. Decide deliberately
   whether that stays an independent migration rule or is folded in later — otherwise the
   "single-sourced" claim is overstated. *(Rev 3: rev 2 called this a "fourth definition"; there are
   two live set definitions — the scheduler's, which this plan moves, and the migration's, which
   stays.)*
4. **Docker/packaging**: verify a fresh image build and a non-editable wheel, not just the editable
   install path (`long_term_forecasting/Dockerfile:14-19`,
   `long_term_forecasting/pyproject.toml:66-68`).

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 }
  }
}
```

*(Rev 2: the old P4 — validation consumption — is now INFRA-028/INFRA-022. The old P5 contract gate
becomes P4 and remains the terminal phase. Rev 1's graph let the contract gate run in parallel with
the behavior-changing phase; with that phase removed, the chain is strictly linear.)*
