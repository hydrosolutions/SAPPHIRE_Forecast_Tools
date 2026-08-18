# Plan: single-source the long-term mode-activity decision (INFRA-022 option (d))

**Status**: Draft, revision 4 — **not approved, no code written**
**Date**: 2026-08-18

**Revision history** (each revision answers an out-of-loop review of the one before):
- **rev 1** → *"not safe to execute as written"*. Its Phase 4 was a placeholder; its predicate could
  not preserve the code's short-circuits.
- **rev 2** → split Phase 4 out as **INFRA-028**; made the decomposition caller-driven.
- **rev 3** → review confirmed the decomposition preserves the loop statement-for-statement, but
  caught that the predicate would still have called `nearest_scheduled_issue_date` internally,
  **moving the monkeypatch seam** and breaking the plan's own characterization tests. Call moved
  back to the caller.
- **rev 4 (this one)** → **placement changed**: the extracted module now lives in
  `apps/long_term_forecasting/`, not `apps/iEasyHydroForecast/`. See § "Why the placement changed".
  A whole phase is deleted as a result — and it was already vestigial.

**Owner decision this implements**: INFRA-022 § "OPEN DECISION — which schedule authority?",
option **(d)** — extract the mode-activity decision so it has exactly one definition.

**Scope**: the behavior-preserving extraction only. It ships **no user-visible change**: after it
lands the scheduler behaves identically and the CLI JSON is byte-for-byte what it was.

**Related**: **INFRA-028** (run manifest — the split-out Phase 4; what validation actually consumes),
**INFRA-022** (schedule-aware gating), **INFRA-021** (ships atomically with the gating),
**LTF-007** (10-vs-5 window mismatch — the *second* consumer of the extracted tolerance).

---

## Why the placement changed (rev 4)

Rev 1–3 put the module in `apps/iEasyHydroForecast/` on the reasoning that `lt_schedule_query` and
`validate_pipeline` would both call it. **`validate_pipeline` will not call it.** Evidence:

- Once INFRA-028's manifest is authoritative, validation needs *which modes to check* (from the
  manifest) and *the horizon value to query* — and the horizon value already comes from the
  **resolver**, which `validate_pipeline` imports today (`quarter_horizon_value()` at
  `validate_pipeline.py:539`, seasonal at `:562`). Tolerance, `forecast_months` and the
  non-operational set never enter validation's work.
- `bin/utils/migration_py/long_forecast.py` keeps its own `_ALWAYS_SKIP_MODES` (`:72`, applied
  `:197-203`) and **does not import `iEasyHydroForecast`** — only docstrings reference it.

So the real consumers are `lt_schedule_query` and — via **LTF-007** — `lt_utils`'s
`check_valid_forecast_issue_date`. **Both live in `apps/long_term_forecasting/`.** Nothing needs
these predicates across a package boundary.

That matters because every hard part of rev 3 existed *only* to cross that boundary:

| Rev 3 burden | Under rev 4 |
|---|---|
| Add the module to `iEasyHydroForecast/pyproject.toml`'s explicit wheel include list (`:40-48`) | **gone** — `long_term_forecasting/pyproject.toml` has no include list and no build-system section. *(Rev 4 correction: it is the **virtual root project** — `uv.lock:851-854` records source `.`; `iEasyHydroForecast` is the editable dependency, `uv.lock:669-672`. The conclusion stands, the rationale was wrong.)* |
| Prove a non-editable wheel exposes both flat and `iEasyHydroForecast.*` namespaces | **gone** — flat sibling import only, exactly like `lt_utils`'s `from config_forecast import ForecastConfig` |
| `lt_utils` re-export shim + import-context hazard that could break *every* `lt_utils` importer | **gone** — see below |
| Docker packaging proof | **gone** — `Dockerfile:15` copies the whole directory |

**And the phase it deletes was already vestigial.** Rev 1–2 moved `nearest_scheduled_issue_date`
because the predicate called it. Rev 3 put that call back in the caller — where
`lt_schedule_query.py:35` already imports it from `lt_utils`. Nothing has needed it moved since
rev 3, under either placement. Rev 4 strikes the phase outright; the helper **stays in `lt_utils`**.

**The placement was conditional on INFRA-028's missing-manifest question. That question is now
DECIDED (2026-08-18, owner): a missing manifest is a validation-infrastructure FAIL and validation
NEVER re-derives the schedule.** Therefore `validate_pipeline` never becomes a predicate consumer,
the module stays in `apps/long_term_forecasting/`, and the wheel-list and namespace work stay
deleted. **The P1 gate is satisfied — all phases may proceed.**

> *History, kept because it explains the phase graph:* rev 4 first claimed the plan "depends on none
> of them", which contradicted its own placement paragraph; the correction made P1 wait on this
> decision. The wait is now over. If INFRA-028's decision is ever revisited toward allowing
> re-derivation, the placement must be revisited with it.

A second, smaller conditional: INFRA-028 proposes carrying **per-mode horizon type and value** in the
manifest, while this plan's placement argument leans on validation resolving horizon values from
`long_term_horizon_resolver` (`validate_pipeline.py:539`, `:562`). Both cannot be the authority.
Recorded as an open question in INFRA-028; it does not change the placement either way, because
neither source is a scheduling *predicate*.

---

## Why this is delicate

`lt_schedule_query.query_schedule()` decides which long-term modes run in production, and its
stdout JSON is consumed by:

- `apps/run_locally.sh` — invoked `:285-286`, **parsed** `:303-315` (via `dict.get`), consumed
  `:641-671`, `:1215-1219`;
- `apps/pipeline/pipeline_docker.py` — the Luigi `LTScheduleQuery` task; command built `:2124`,
  stdout redirected `:2127`, **parsed** `:2259-2269`, validated `:2275-2303`, consumed `:2364-2382`;
- `apps/run_docker_tests.sh` — invoked `:675-681`, **parsed** `:683-693`.

A refactor that changes which modes are active, or the shape of that JSON, silently changes what
production runs. The whole plan is therefore structured around **proving behavioral identity before
rewiring**, not around the elegance of the end state.

## What is actually being extracted

| Concern | Today | After |
|---|---|---|
| Config **loading and mutation** | `ForecastConfig` | unchanged |
| The **decision predicates** (tolerance, wrap distance, `forecast_months` membership, non-operational set) | inline in the loop | new sibling module |
| **Iteration and short-circuiting** (which modes/models are consulted, in what order) | `query_schedule` | **unchanged — stays in the caller** |
| **Output shaping** (`active_modes` / `skipped_modes` / `skill_metric_types`) | `query_schedule` | unchanged |

> **The first row is not "just loading".** `ForecastConfig` is stateful and **mutating**: one
> instance is reused across modes, `load_forecast_config` overwrites its current-mode state, and
> `synchronize_forecast_settings` **rewrites every model's `general_config.json` on disk**
> (`config_forecast.py:110-111`, `:157-180`). Moving *when* inputs are gathered can change on-disk
> side effects, not merely return values. This is the strongest argument for leaving iteration in
> the caller.

> **The predicate must not be mode-level.** A mode-level predicate cannot preserve two
> short-circuits: non-operational modes touch **no config at all** (the check precedes
> `load_forecast_config`, `:88-91`), and model evaluation is **lazy and ordered** — the loop breaks
> on the first qualifying model (`:109-121`), so later models are never read.

### The new module

`apps/long_term_forecasting/lt_schedule_rules.py`, a flat sibling of `lt_schedule_query.py`:

- `ISSUE_DAY_TOLERANCE` — moved from `lt_schedule_query.py:52`, currently 10. **Do not change the
  value here**; that is LTF-007, which will make `lt_utils.check_valid_forecast_issue_date` (bare
  literal `5` at `lt_utils.py:202`) read this same constant.
- `NON_OPERATIONAL_MODES` — moved from `lt_schedule_query.py:57`. Note
  `bin/utils/migration_py/long_forecast.py:72` keeps an independent copy; deciding whether that
  folds in later is out of scope here.
- `is_non_operational(mode) -> bool`
- `day_distance(today_dom, issue_day) -> int` — moved **verbatim**, including the `30 - diff` wrap
  approximation. **Do not "fix" the approximation**; behavior change, own issue. It must remain
  importable as `lt_schedule_query.day_distance` (`test_lt_schedule_query.py:11-14` imports it from
  there) — a same-directory re-export.
- `is_unrestricted(forecast_months) -> bool` — true for a falsy value **or** the exact ordered list
  `[1..12]`; these are the two shortcuts that precede any date computation.
- `within_issue_window(distance) -> bool` — owns the tolerance comparison.
- The skip-reason strings as constants/helpers, reproducing today's wording exactly
  (`lt_schedule_query.py:90`, `:97`, `:104`, `:124`) — they surface in operator-facing JSON.

**`nearest_scheduled_issue_date` does not move.** It stays in `lt_utils.py:134-174`, and
`query_schedule` keeps calling it through its existing binding (`lt_schedule_query.py:35`). This
preserves the monkeypatch seam that the P0 tests depend on. The resulting loop:

```python
for model_name in models:
    forecast_months = config.get_forecast_months(model_name=model_name)
    if lsr.is_unrestricted(forecast_months):
        any_model_scheduled = True
        break
    nearest = nearest_scheduled_issue_date(today, issue_day, forecast_months)  # caller's binding
    if lsr.within_issue_window(abs((today - nearest).days)):
        any_model_scheduled = True
        break
```

**`forecast_months` semantics to preserve exactly** (`:113-124`): any falsy value is unrestricted;
the exact ordered list `[1..12]` short-circuits; anything else defers to
`nearest_scheduled_issue_date`; **zero models means inactive**.

**Date types**: the scheduler passes `pandas.Timestamp` (`:150-153`) and all date arithmetic stays
where it already is, so no normalization is introduced by this change. A future consumer passing
`datetime.date` normalizes at **its** boundary, not inside these predicates.

---

## What was Phase 4 — now INFRA-028

Rev 1 ended with a phase making `validate_pipeline` consume the authority. It was not an executable
phase, and the idea under it was insufficient: a re-derived schedule cannot see manual overrides
(`pipeline_docker.py:2339-2367` bypasses `LTScheduleQuery`), execution outcome, or the date output
was actually written under (late forecasts snap back, `lt_utils.py:211-217`). That work is
**INFRA-028**, a run-scoped manifest emitted by whatever actually ran.

**Dependency direction**: INFRA-028 → INFRA-022 → atomic with INFRA-021.

**Nothing in that chain depends on this plan** — validation consumes the manifest, not these
predicates. But this plan is **not** fully independent of the chain either: its *placement* is
conditional on INFRA-028's missing-manifest decision (see § "Why the placement changed"). P0 can
proceed now; P1 onward should wait for that decision to be recorded.

---

## Phases

### P0 — Characterization (no production code changes)

- **Goal**: pin today's behavior so drift is caught by a failing test, not a deployment. Must land
  and pass **before** P1.
- **Files**: `apps/long_term_forecasting/tests/test_lt_schedule_query.py` (extend only).
- **Depends on**: —
- **Agents**: 1.
- **Work**: two groups.

  **(a) Outcome matrix** — {inside window; `dist == tolerance`; `dist == tolerance + 1`;
  wrap-around across month end; mode in `NON_OPERATIONAL_MODES`; config-load error;
  `forecast_months` falsy/absent; `forecast_months == [1..12]` exactly; `forecast_months` excluding
  this month; restricted match in the previous/next month; a year-boundary match; `issue_day`
  clamping in short months; several models where only one qualifies; **zero models**; unknown
  `horizon_type` (active, contributes no skill type)}. Assert `active_modes`, the **exact**
  `skipped_modes` reason strings, and `skill_metric_types`.

  **Negative controls for the `[1..12]` shortcut**: the current check is exact equality against
  `list(range(1, 13))` (`:113-118`). Include a **permutation** and a **tuple** of the same months —
  both must still reach `nearest_scheduled_issue_date`. Without these, a set-based or sorted
  implementation passes every outcome assertion while being more permissive than today.

  **(b) Call-order and exception characterization** — what a refactor breaks and outcome assertions
  miss. Using mock assertions on the `ForecastConfig` double, prove: a non-operational mode triggers
  **no** `load_forecast_config` call; a too-distant mode triggers **no** model reads; once one model
  qualifies, later models' getters are **never called**. Pin **ordering**: `active_modes` follows
  `LT_supported_modes` order, `skipped_modes` preserves insertion order, `skill_metric_types` its
  current dedup/sort.

  **Load count and order** — on-disk mutation is this plan's principal risk and every successful
  load writes (`config_forecast.py:110-111` → `:157-180`). Assert **exactly one**
  `load_forecast_config` per consulted operational mode, in `LT_supported_modes` order, and that a
  caught load failure **continues** to the next mode.

  **Exception expectations, as a table** — current behavior:

  | Raised by | Today |
  |---|---|
  | `load_forecast_config` (`:93-98`) | **suppressed** → `skipped_modes` reason, loop continues |
  | `get_operational_issue_day` (`:100`) | propagates |
  | `get_models_to_run` (`:109`) | propagates |
  | `get_forecast_months` (`:112`) | propagates |
  | `nearest_scheduled_issue_date` (`:118`) | propagates |
  | `get_horizon_type` (`:128`) | propagates |

  A month list leaving **no candidate** belongs here, not in the outcome matrix: `min()` raises
  (`lt_utils.py:156-174`) from outside the only `try`, so it propagates and there is no result to
  assert against.

  Add a CLI test that `main()` stdout parses and contains the three keys with correct **types** —
  present, not "exactly three": Luigi tolerates extras (`pipeline_docker.py:2275-2303`),
  `run_locally` uses `dict.get` (`:303-315`), the Docker smoke reads only `active_modes`.
- **Acceptance**: new tests pass against **unmodified** source via
  `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh long_term_forecasting`. Placeholder station
  codes only (`19999`).

### P1 — Create the rules module (no callers rewired)

- **Goal**: `lt_schedule_rules.py` exists, is pure, and is unit-tested in isolation.
- **Files**: `apps/long_term_forecasting/lt_schedule_rules.py` (new);
  `apps/long_term_forecasting/tests/test_lt_schedule_rules.py` (new).
- **Depends on**: P0. *(The INFRA-028 missing-manifest gate that previously blocked this phase was
  decided on 2026-08-18 in favour of "never re-derive", which confirms this placement. Gate
  satisfied.)*
- **Agents**: 1.
- **Work**: implement the predicate set above — predicates and constants only, no loop, no
  `ForecastConfig`, no env reads, no file I/O. Reason strings copied character-for-character.
- **Acceptance**: unit tests cover **the P0 rows that are predicate-owned** — the window boundaries
  (`dist == tolerance`, `+1`), the wrap-around, non-operational membership, and the `is_unrestricted`
  cases including the permutation and tuple negative controls. *(Rev 4 correction: rev 4 said "the
  P0 matrix at predicate level", which overreaches — config-load failures, zero/multiple models,
  horizon mapping, nearest-month selection, year rollover and short-month clamping are owned by the
  caller or by `nearest_scheduled_issue_date`, both of which P1 explicitly excludes. Those rows stay
  caller-level in P0 only.)* The module imports with **no** env vars set and without touching the
  filesystem — prove with an explicit probe, not prose. Nothing else imports it yet.
  *(No packaging work: `long_term_forecasting/pyproject.toml` has no wheel include list, and
  `Dockerfile:15` copies the directory wholesale.)*

### P2 — Rewire `lt_schedule_query` *(was P3)*

- **Goal**: `query_schedule` keeps loading config and shaping JSON, delegating only the predicates.
- **Files**: `apps/long_term_forecasting/lt_schedule_query.py`, plus every file citing the moved
  constants at their old location. **Known sites — treat as a starting point, not a complete list**
  *(the count has grown at every review pass: three, then five, then the below; the implementer must
  re-grep rather than trust this enumeration)*:
  `apps/iEasyHydroForecast/tests/test_initialize_long_forecast.py:256-258`,
  `bin/utils/migration_py/long_forecast.py:11-12` **and** `:69-72` (two separate citations in one
  file), `bin/initialize_long_forecast_history.sh:25-28`,
  `doc/prod/update_data_migration_runbook.md:624-627`,
  `doc/plans/issues/high_prio_gi_draft_update_migration_p5_long_forecast.md:44-47`,
  `doc/plans/working/forecast_skill_eval_issue_day_investigation_prompt.md:34-44` and `:78-82`.
- **Depends on**: P1.
- **Agents**: 1.
- **Work**: replace inline predicates with calls into `lt_schedule_rules`. **Keep the loop, the
  iteration order, the lazy `break`, the `try/except` around `load_forecast_config`, which calls sit
  outside it, the output dict, and the stderr-only logging.** The diff should read as "expression
  replaced by function call", never as restructuring.
- **Acceptance**: **every P0 test passes unmodified** — both groups. If a P0 test needs changing,
  the refactor changed behavior and the refactor gets reworked, not the test. Module attributes
  existing tests reach for must survive: `ForecastConfig` and `sl` are patched as
  `lt_schedule_query.*` (`test_lt_schedule_query.py:116-117`), `day_distance` is imported from it
  directly (`:11-14`), and `HORIZON_TYPE_TO_SKILL` likewise. Do not "clean up" imports.

### P3 — Contract gate *(was P4)*

- **Goal**: prove the operational consumers are unaffected by a change that should be invisible.
- **Depends on**: P2.
- **Agents**: 1.
- **Acceptance**:
  - `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero fail. Record the expected
    environment-gated skip set **before** starting and compare against it, rather than asserting
    "zero skips" in prose.
  - `apps/pipeline` tests, including the Luigi `LTScheduleQuery` task and the structural test at
    `test_lt_schedule_workflow.py:263-275`.
  - A freshly built `ltforecast` image, then the Docker smoke at `run_docker_tests.sh:661-701`
    (`--today 2026-03-25`, parses `active_modes` at `:683-693`) — proving the CLI still runs
    end-to-end in its deployed form. Extend with a **no-active-modes** case, but **fixture-driven**:
    activity depends on the config mounted via `INTEGRATION_ENV_PATH` (`:666-681`), so no fixed date
    is reliably inactive across deployments. The smoke **skips silently when the image is absent** —
    a skip must not be read as a pass.

---

## Explicit non-goals

- **Do not** change `ISSUE_DAY_TOLERANCE` from 10 to 5 — that value decision is **LTF-007**.
- **Do not** fix the `30 - diff` month-wrap approximation.
- **Do not** move `ForecastConfig` or `nearest_scheduled_issue_date`.
- **Do not** alter the CLI JSON keys or types.
- **Do not** wire `validate_pipeline` to anything — that is INFRA-022, consuming INFRA-028.

## Residual risks

1. **Exception behavior is subtler than it looks.** `get_models_to_run()`/`get_forecast_months()`
   sit outside the `try` (`:93-121`), but `load_forecast_config` itself calls
   `synchronize_forecast_settings`, which reads every model's config files
   (`config_forecast.py:110-111`, `:157-180`). An artifact missing at load time normally raises
   **inside** the caught block and becomes a skipped-mode reason, not a CLI failure. An uncaught
   failure remains possible if a file disappears between load and access. P0(b) pins whichever
   behavior is current, not the assumed one.
2. **Several citations will go stale — they are correct *today*.** Every site listed in P2
   currently points at real content: `lt_schedule_query.py:54-91` still holds
   `NON_OPERATIONAL_MODES` and its skip. They break **when P2 moves the constants**, which is why
   they are in P2's file list rather than filed as a pre-existing defect. **The list has grown at
   every review pass — re-grep before executing P2 rather than trusting it.** *(Corrected in the third pass: an earlier draft called them
   "already stale" and likened them to MIG-002 — MIG-002 was a rollback dump-glob data-loss defect,
   not citation drift. The analogy was wrong and the tense was wrong.)* One of the known sites is an
   operator **runbook** (`doc/prod/update_data_migration_runbook.md`), so the update is not merely
   cosmetic. Note the sites are not all the same citation class — some name
   `NON_OPERATIONAL_MODES`, others the tolerance or the query itself — so a grep for one constant
   will not find them all.
3. **Three live schedule definitions survive, not one.** This plan single-sources only the
   scheduler's. Also live:
   - `bin/utils/migration_py/long_forecast.py:72` — its own `_ALWAYS_SKIP_MODES`, applied `:197-203`.
   - **The local runner's operational fallback** (`run_locally.sh:232-261`, selected on schedule-query
     failure at `:274-300`) — independently reimplements the 30-day wrap distance, issue-day
     defaults, **and a five-day window**. Note that is the *execution* value, not the scheduler's
     ten, so the fallback and the scheduler already disagree with each other (**LTF-007**).
   The "single-sourced" claim is therefore scoped to `apps/long_term_forecasting/` Python callers.
4. **LTF-007 is the second consumer and is not part of this plan.** Until it lands, the extracted
   tolerance has one reader, and `lt_utils.py:202` still hard-codes `5`.

```json
{
  "external_gates": {
    "INFRA-028-missing-manifest-decision": {
      "status": "SATISFIED 2026-08-18 — never re-derive; placement confirmed",
      "blocks": []
    }
  },
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 }
  }
}
```

**This plan is now unblocked end to end.** The gate is retained as a satisfied entry rather than
deleted, so that reopening the INFRA-028 decision visibly reopens this one.
