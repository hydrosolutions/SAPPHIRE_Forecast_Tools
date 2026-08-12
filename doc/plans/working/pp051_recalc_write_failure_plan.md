# PP-051 — Long-term skill-metrics recalc: implementation plan (Option A, LR-007 pattern)

**Status:** Not started. Draft plan — subject to CLAUDE.md's mandatory multi-model review
(out-of-loop verifier pass) before any phase begins execution. **Revised 2026-08-12** after an
out-of-loop adversarial review found 12 defects (2 Critical) in the prior version of this plan. The
core technical approach (§1a, Hard Contracts 7-8) changed; phase structure, ordering, and Hard
Contracts 1-6 did not. See §1a for what was wrong and §2 Contracts 7-8 for the fix.
**Base:** current `docs_fdc_flv_denominator_stability` tree (module `apps/postprocessing_forecasts`
only; `apps/iEasyHydroForecast` read but not modified). Source draft:
`doc/plans/issues/high_prio_gi_draft_pp_recalc_silent_api_write_failure.md` (PP-051). Do not restate
its evidence here — this plan only sequences the fix the draft already scoped.
**Owner-locked scope:** Option A — mirror LR-007's *shape* (a bool-return contract feeding the
existing `errors`/exit-code aggregation), not its implementation verbatim.
`forecast_library.py:3973-3979` has the same discarded-return-value defect this plan fixes here (see
updated §5) — a separate draft is being filed for it, and this plan deliberately does not reproduce
that defect. Option B (change `SAPPHIRE_API_FAILURE_MODE` default) is out of scope. Option C
(independent wrapper-side DB probe) remains an **optional** phase (P6); its status changed in this
revision — see P6.

---

## 1. Scope confirmed by direct inspection (2026-08-12; re-verified 2026-08-12 post-review — no line drift)

Five in-scope `save_*` functions in `apps/postprocessing_forecasts/src/file_writer.py`, each with
**exactly one non-test caller**, all in `apps/postprocessing_forecasts/recalculate_skill_metrics.py`
(re-verified by grep across `apps/` — see command trail in this session, not repeated here):

| Function | `file_writer.py` body | API-write try block(s) | Sole caller (`recalculate_skill_metrics.py`) | CSV fallback | Top-level empty-input guard |
|---|---|---|---|---|---|
| `save_skill_metrics` (pentad/decad) | `:291-379` | `:339-345` | `:240-245` | unconditional, **raises** on failure (`:331-336`) | **none** — see §1b |
| `save_monthly_skill_metrics` | `:382-463` | `:429-433` | `:361-366` | conditional on 2 env vars, **warns-only** if unset (`:413-427`) | yes, `:400-402` |
| `save_quarterly_skill_metrics` | `:640-672` | `:666-670` | `:430-437` | **none** — API-only | yes, `:652-654` |
| `save_seasonal_skill_metrics` | `:675-707` | `:701-705` | `:513-520` | **none** — API-only | yes, `:687-689` |
| `save_daily_skill_metrics` | `:581-632` | FDC `:614-618`, threshold `:624-628` | `:555-566` | **none** — API-only, two independent metric writes in one call | per-branch at call site, `:609`, `:623` |

All five currently `return None` unconditionally (success and swallowed-failure paths alike). All five
call sites but `save_daily_skill_metrics`'s check `if ret is None:` (dead-under-`warn` else branch);
the daily call site instead wraps the whole call in an outer `try/except` (`:557-566`) that can never
fire under `warn` mode because the failure is already swallowed one level down inside
`save_daily_skill_metrics` — this call site needs a **different** edit shape (return-value check, not
exception check) than the other four.

**Exit-code aggregation already exists and needs no new mechanism** — `recalculate_skill_metrics()`
accumulates a shared `errors` list across every mode block and does `sys.exit(1) if errors else
sys.exit(0)` at `:580-587`. Every phase below only needs to (a) make its `save_*` return `True`/`False`
instead of bare `None` per the corrected contract in §2, and (b) flip its call site's dead branch (`if
ret is None` / outer-`try/except`) into a check that appends to the *existing* `errors` list on
`False`. No change to the aggregation or exit mechanism itself.

**`apps/iEasyHydroForecast` is NOT modified.** This plan touches only
`apps/postprocessing_forecasts/src/file_writer.py` and
`apps/postprocessing_forecasts/recalculate_skill_metrics.py`. `_handle_api_write_error` and
`_get_api_failure_mode` (`forecast_library.py:82-116`) are consumed, not modified — the same shipped
LR-007 mechanism, unchanged. It is nonetheless exercised by every phase's mandatory full-suite
verification (§4 blanket note) — not because this plan needs its own separate scoped run, but because
CLAUDE.md's gate is always the full suite, and `postprocessing_forecasts` imports `forecast_library`
transitively (the existing `conftest.py:53-54` `_reset_api_singletons` autouse fixture already covers
this and requires no edits).

**Existing tests to build on, not duplicate:** `apps/postprocessing_forecasts/tests/test_file_writer.py`
already has `TestSaveMonthlySkillMetrics` (`:94`); `tests/test_recalc_workflow.py` already has
`test_save_error_accumulation` (`:232`), `test_monthly_save_error_causes_exit_1` (`:431`), and
`test_save_success_path` (`:300`). These currently exercise the **dead** `ret is not None` branch
(there is no code path today that produces a non-`None` failure signal under `warn` mode) — they must
be extended, not treated as already covering this defect. Read them before writing new tests to avoid
duplicate fixtures.

**Named test that pins the old contract and will break unannounced if not updated:**
`apps/postprocessing_forecasts/tests/test_file_writer.py:317` — `test_empty_dataframe_guarded_no_write`
asserts `result is None` for `save_monthly_skill_metrics` on empty input. Under §2 Contract 7 this
becomes `result is True` (empty input is success, not a null signal) — P3 must update this test by name,
not leave it to break silently. A second `is None` assertion exists at `test_file_writer.py:534`
(`test_empty_df_handled`, for `save_monthly_forecast_data`) — that function is **out of scope** (see §5
second bullet); do not touch it, and do not confuse the two similarly-shaped tests.

**Footnote, not a phase:** `apps/postprocessing_forecasts/postprocessing_forecasts.py.deprecated:155,197`
calls `save_skill_metrics` and checks `if ret is None`. The file's `.deprecated` extension means it
cannot be imported as a Python module, and nothing in the repo invokes it — this is a stale reference
only, not a caller requiring conversion in any phase below.

### 1a. The defect this plan must actually fix (CRITICAL 1 / 1b / 2 — verified against current tree)

The previous version of this plan said `api_ok` goes `False` only when `_handle_api_write_error` is
reached — i.e., only on a raised exception. That is wrong for the most common failure paths:

- `apps/postprocessing_forecasts/src/api_writer.py:429` — `_write_skill_metrics_to_api(data,
  horizon_type, year) -> bool` returns `False` **without raising** in four places:
  `SAPPHIRE_API_AVAILABLE` false (`:462-464`), `SAPPHIRE_API_ENABLED` not `"true"` (`:467-470`),
  `client.readiness_check()` false (`:478-480`), and "no records after filtering" (`:706-708`). It only
  raises for the actual `client.write_skill_metrics(...)` HTTP call itself (`:700`, unwrapped —
  propagates to the caller) or for an invalid `horizon_type` (`:456-460`, a programming error, not an
  operational one).
- `apps/postprocessing_forecasts/src/api_writer.py:711` — `_write_threshold_skill_metrics_to_api(data,
  year) -> bool` **never raises to its caller at all**: unavailable/disabled/not-ready/no-data
  (`:730-751`, `:793-795`) are the same shape, and the one HTTP call it makes (`:802`) is wrapped in its
  own inner `try/except` that converts `AttributeError` and any other `Exception` into `False`
  internally (`:801-818`).
- Every current call site (`file_writer.py:341,431,616,668,703,626`) wraps the call in a `try/except`
  routed only to `_handle_api_write_error` — and **discards the function's return value entirely** (the
  call is a bare statement, not an assignment). So the original plan would leave `api_ok` reading as
  success for every `False`-without-raising case above — which is exactly `SAPPHIRE_API_ENABLED=false`
  and "missing/unready client," the scenarios PP-051's own summary names. §2 Contract 7 fixes this by
  capturing both signals and by fixing the *initialization order* of the result variable (see Contract
  7 for why order matters, not just "capture the bool").

### 1b. Asymmetry in `save_skill_metrics` (pentad/decad) — accepted, not fixed here

Unlike its four siblings, `save_skill_metrics` (pentad/decad) has **no top-level `data is None or
data.empty` guard** — empty input flows straight through to an unconditional CSV write (writes an
empty file — pre-existing behavior) and to the API writer. Under Contract 7's capture-the-returned-bool
fix, an empty `data` argument to `save_skill_metrics` will therefore produce `api_ok = False` (the
writer's "no records" branch fires), not `True` as it would for the other four functions. **P2 does
not add a guard to fix this** — that would be a new, unscoped behavior change (whether pentad/decad
recalcs can legitimately be empty is a separate question this plan doesn't answer) and CLAUDE.md's
orchestration protocol requires changes stay "purely additive or modify only the specific behavior
described." This is called out explicitly so it is a documented, deliberate omission rather than a gap
discovered later — see the matching §5 residual-risk bullet.

---

## 2. Hard contracts every phase must preserve

State these explicitly in every phase's agent prompt — cite them, don't restate the reasoning:

1. **"Process every mode, then report."** `recalculate_skill_metrics()` must keep attempting every
   configured mode/horizon even when one API write fails — do not turn any in-scope change into a
   mid-run abort. This is what the `warn` default (`forecast_library.py:88`) exists to protect (draft's
   "Why the `warn` default may be intentional" section, LR-007 precedent).
2. **CSV behavior unchanged in `warn` mode wherever CSV exists today**: pentad/decad's unconditional
   write-and-raise-on-failure (`file_writer.py:331-336`) and monthly's conditional
   write-or-warn-and-skip (`:413-427`) must not change shape — only the function's *return value* and
   the *caller's* branch condition change.
3. **No `sapphire/services/` change.** Entirely `apps/` behavior — no service, schema, or endpoint edit.
4. **No half-converted return contract.** A `save_*` function must never be merged in a state where it
   returns a bool while its sole caller still checks `if ret is None:` (or vice versa) — that silently
   breaks the check it was aggregated to fix (a `True` success return would fail an `is None` test and
   get logged as an error; a stale `if ret is None` after `False` uses `False is None` → also
   evaluates False, silently going to the "error" `else` branch — different bug, same "don't split
   these" lesson). Each phase's function-change and call-site-change are one atomic diff from one agent;
   no phase merges with only one half done.
5. **`SAPPHIRE_API_FAILURE_MODE=fail` behavior is unchanged for all existing callers.**
   `_handle_api_write_error` (`forecast_library.py:95-116`) is not modified by this plan; `fail` mode
   still re-raises exactly as it does today, upstream of any of these changes.
6. **Before editing, re-grep current line numbers.** The table in §1 reflects the tree at plan-revision
   time (2026-08-12, re-verified). If the branch has moved since, an implementing agent must re-locate
   the function bodies and call sites by name/grep, not trust the hardcoded line numbers blindly.
7. **Empty-input vs. genuine-failure contract (resolves CRITICAL 1b).** Applies to every function this
   plan converts to a bool return:
   - If the function's own top-level guard finds nothing to do (`data is None or data.empty` for
     monthly/quarterly/seasonal; per-branch at the call site for `save_daily_skill_metrics`'s FDC and
     threshold inputs; **does not apply to `save_skill_metrics` — see §1b**), that branch contributes
     `True`. A quiet period (no closed quarter yet, no forecasts to score) is not a failure; reporting
     it as one would turn the exit code red on a healthy, data-driven no-op run.
   - Otherwise, the result is `False` unless **both** signals say success: the bool the writer actually
     **returns** (capture it — today's code discards it, §1a), and the wrapping `try/except` not having
     caught an exception. **Initialize the result variable to `False` before the
     `if api_writer.SAPPHIRE_API_AVAILABLE:` gate, not inside the `try`**, so a closed gate (client not
     installed) or a caught exception both leave it `False`, and only an explicit `True` return from the
     writer — reached and observed normally — sets it `True`. This ordering is what actually closes
     CRITICAL 1: under the original wording the result variable had no assignment at all in the
     gate-closed and swallowed-`False` cases, which is why they silently read as success.
   - **Do not change `_write_skill_metrics_to_api`'s or `_write_threshold_skill_metrics_to_api`'s own
     "no records after filtering" return value** (`api_writer.py:706-708`, `:793-795`) — out of this
     plan's owner-locked scope, and it would ripple into the several existing test files in
     `apps/postprocessing_forecasts/tests/` that already pin this exact `False`-on-empty-records
     behavior directly (`test_api_integration.py` alone has 4+ tests named
     `test_*_empty_data_returns_false`). Practical consequence: when a save_* function's `data` is
     **non-empty** (passed the guard above) but the writer's *internal* filtering — dropping rows with
     a missing horizon-in-year value, or the lead-aware NULL-`horizon_value` exclusion under
     `SAPPHIRE_SKILL_LEAD_AWARE=true` — still leaves zero records, the writer returns `False`, and that
     **is** surfaced as a recalc-level failure. "All rows were invalid" is a data-quality condition
     worth reporting, deliberately different from the top-level empty-DataFrame guard above. Each
     applicable phase's test criteria (P1, P2 optionally, P3) must include one test locking this down
     explicitly.
   - For `save_daily_skill_metrics`'s threshold branch specifically: the existing call site already
     guards on `threshold_metrics.empty` before calling the writer (`file_writer.py:623`), so
     `_write_threshold_skill_metrics_to_api`'s internal "no records" branch (`:793-795`) cannot be
     reached from non-empty input today — no test is needed to force it; do not assume this can never
     change.
8. **Signature/annotation/docstring updates are authorized, not a scope violation.** Every function
   whose return contract changes in this plan (`save_skill_metrics`, `save_monthly_skill_metrics`,
   `save_quarterly_skill_metrics`, `save_seasonal_skill_metrics`, `save_daily_skill_metrics`) must have
   its return type annotation changed to `-> bool` and its docstring's `Returns:` section rewritten to
   state the Contract 7 `True`/`False` meaning, **in the same diff** that changes the return
   statements — e.g. `save_daily_skill_metrics` is currently annotated `-> None` and documents `Returns:
   None` (`file_writer.py:585`, `:601-602`). Each phase's agent prompt must say this explicitly: "do not
   change function signatures" means the *parameter* list, not a return annotation left stale by design.

**Related-but-out-of-scope hazard (P3 only):** `file_writer.py:413-442` — `filepath` is bound only
inside `if csv_dir and csv_file:` (monthly's conditional-CSV branch) yet is referenced unconditionally
inside the `SAPPHIRE_CONSISTENCY_CHECK` branch below it, producing an `UnboundLocalError` when CSV env
vars are unset and the consistency check is enabled. This is being filed as its own draft by another
agent working in `doc/plans/issues/` concurrently — **do not fix it here.** P3 touches this exact
function, so its agent prompt and its required CSV-unconfigured test must both note the hazard so
nobody trips it or "fixes" it inline; see P3 below.

---

## 3. Phasing rationale

All five call sites live in the **same file** (`recalculate_skill_metrics.py`), in disjoint but
adjacent line ranges. Running multiple worktree-isolated agents on the same file in parallel risks
exactly the failure mode Hard Contract 4 warns about: two agents editing overlapping context, or one
agent's edit shifting line numbers the other agent was told to target, producing a merge that looks
clean but silently leaves one call site half-converted. There is no *logical* dependency between fixing
pentad/decad vs. quarterly vs. monthly vs. daily — but there is a **shared-file execution-order**
dependency. Phases below are therefore sequenced (not parallelized) purely for this reason; the
dependency graph's `depends_on` edges encode "must land and be merged first so line numbers and file
state are current," not a data/control dependency.

Ordering: the no-CSV-fallback functions (quarterly, seasonal — total data loss on a swallowed failure)
are the highest-value fix and go first (P1), so the riskiest gap is closed and independently verified
before touching the lower-risk, CSV-backed paths. Pentad/decad (P2) and monthly (P3) follow — both
already have a CSV safety net, so a defect in the *fix itself* there is lower-consequence. Daily (P4) is
last because its call site has a structurally different shape (exception-check → return-value-check)
and combines two independent API writes (FDC + threshold) into one bool, which needs the pattern
proven on the simpler cases first. One more asymmetry to carry through the ordering: P1 and P3's
functions have the Contract 7 "non-empty input, writer-internal empty records" ambiguity to test for
(their horizon-in-year columns can stay NaN only via the lead-aware `horizon_value` path, not the
primary period column — see the P1/P3 acceptance criteria); P2's function has no top-level empty guard
at all (§1b); P4's threshold branch cannot reach that ambiguity in practice (already guarded at the call
site) but its FDC branch can, structurally, the same way P1/P3 can.

---

## 4. Phases

Every acceptance criterion below that says a function "returns `False`" or "returns `True`" means an
**exact identity assertion** (`assert result is False` / `assert result is True`), never a truthy/falsy
check — today's code already returns `None`, which is falsy, so a falsy-only assertion would pass
against the unfixed code and make RED-first vacuous. Every phase's test list must cover **both** failure
mechanisms for its function(s): the writer *returning* `False` without raising (patch the writer with
`return_value=False`, not `side_effect=Exception`) and the writer *raising*
(`side_effect=Exception(...)`) — a `side_effect` test alone only proves the rarer exception path (§1a)
and would leave the headline defect unverified. Verification for every phase is the **full** suite per
CLAUDE.md: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` (zero fail, zero unexpected skip) — not
a module-scoped invocation. This transitively includes `apps/iEasyHydroForecast` (unmodified by this
plan, per §1, but part of the mandatory full-suite gate regardless).

### P1 — Quarterly + seasonal skill metrics (no-CSV-fallback horizons)

- **Goal:** `save_quarterly_skill_metrics` and `save_seasonal_skill_metrics` return a bool per Hard
  Contract 7: `True` when their top-level `data is None or data.empty` guard fires (`:652-654`,
  `:687-689`) or when the API write genuinely succeeds; `False` when the writer returns `False` (any of
  its four no-raise paths, §1a) or when `_handle_api_write_error` is reached via an exception. Their
  call sites (`recalculate_skill_metrics.py:430-437` and `:513-520`) replace `if ret is None:` with a
  check on the new bool and append to the existing `errors` list on `False`, using the same message
  style already used by the sibling forecast-data checks in the same function (e.g. `:236-238`).
- **Files:** `apps/postprocessing_forecasts/src/file_writer.py` (functions at `:640-672`, `:675-707`
  only), `apps/postprocessing_forecasts/recalculate_skill_metrics.py` (lines `:430-437`, `:513-520`
  only), `apps/postprocessing_forecasts/tests/test_file_writer.py`,
  `apps/postprocessing_forecasts/tests/test_recalc_workflow.py`.
- **Depends on:** none (first phase).
- **Agents:** 1, worktree-isolated. Prompt must include Hard Contracts 1-8 verbatim (§2, including the
  filepath-hazard note even though it doesn't apply to this phase's functions, so the agent recognizes
  it if seen elsewhere), the fitness line from CLAUDE.md verbatim, and: *"Do NOT change any existing
  function signature's parameter list (return-type annotation changes ARE authorized, per Contract 8).
  Do NOT touch `save_skill_metrics`, `save_monthly_skill_metrics`, or `save_daily_skill_metrics` — those
  are separate phases. Do NOT touch the API-only guard (`api_writer.SAPPHIRE_API_AVAILABLE`) or the
  `data is None or data.empty` early-return shape itself — only its return value (`None` → `True`) and
  the API-write try/except changes: capture `_write_skill_metrics_to_api`'s returned bool into a result
  variable initialized to `False` before the `if api_writer.SAPPHIRE_API_AVAILABLE:` gate, per Contract
  7. Do NOT change `_write_skill_metrics_to_api` itself (`api_writer.py`) — it is out of scope."*
- **Acceptance criteria:**
  - RED-first: a new test asserting `save_quarterly_skill_metrics` (and separately
    `save_seasonal_skill_metrics`) returns `False` under a mocked API write **exception** in default
    `warn` mode fails against the pre-fix code (proves the defect existed), then passes after the fix.
  - A second RED-first test asserting the same function returns `False` when the writer **returns
    `False` without raising** (`return_value=False`, e.g. simulating `SAPPHIRE_API_ENABLED=false` or an
    unready client) — this is the headline CRITICAL-1 case; a suite that only has the exception-based
    test above does not prove the actual defect is fixed.
  - A test proving the top-level empty-DataFrame guard now returns `True` (not `None`, not `False`) via
    exact identity assertion.
  - A test proving that when `data` is non-empty but `_write_skill_metrics_to_api`'s own internal
    filtering leaves zero records (construct via `SAPPHIRE_SKILL_LEAD_AWARE=true` with all rows'
    `horizon_value` set to `NaN` — see fixture pattern in `test_lead_aware_write_side_dedup.py`), the
    save_* function returns `False` per Contract 7's fourth bullet — this is the deliberate,
    documented-not-accidental part of the new contract.
  - `recalculate_skill_metrics.py` exits non-zero (`sys.exit(1)`, or assert on `errors` list contents
    per the existing `test_save_error_accumulation` pattern) when any `False` signal above is seen; the
    success path (real write succeeds) still returns `True` and the recalc still exits `0` with these
    two modes present. Confirm the test suite does **not** assert a CSV side effect for
    quarterly/seasonal (none should exist — API-only).
  - Mutation check: flipping the new `if not api_ok:` condition, or moving the `api_ok = False`
    initialization to inside the `try` block instead of before the gate, must make at least one new test
    fail — record which test catches each mutation.
  - `SAPPHIRE_API_FAILURE_MODE=fail` behavior for these two functions is unchanged (existing/extended
    test still shows the exception propagates in `fail` mode, unaffected by the new return value).
  - `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` (full suite) — 0 fail, 0 unexpected skip.

### P2 — Pentad/decad skill metrics (`save_skill_metrics`)

- **Goal:** same bool-return pattern applied to `save_skill_metrics` (`file_writer.py:291-379`,
  API-write try block `:339-345`); call site `recalculate_skill_metrics.py:240-245` flips its
  `if ret is None:` check. **This function has no top-level empty-input guard (§1b)** — do not add one;
  an empty `data` argument legitimately produces `False` here (the writer's "no records" branch fires)
  under Contract 7, and that is an accepted, documented asymmetry with P1/P3, not a bug to fix in this
  phase.
- **Files:** same four files as P1, restricted to `save_skill_metrics` and lines `:240-245`.
- **Depends on:** P1 (shared-file execution order per §3 — not a logical dependency).
- **Agents:** 1, worktree-isolated. Prompt: Hard Contracts 1-8, fitness line, plus: *"The CSV write at
  `:331-336` is unconditional and already raises on its own failure — do not touch that block at all,
  only the API-write try/except at `:339-345` and the function's return statement. Confirm by re-diff
  that the CSV write's raise-on-failure behavior is byte-identical before/after. Do NOT add a top-level
  `data.empty` guard to this function — see §1b of the plan; that is out of scope here."*
- **Acceptance criteria:** same test categories as P1 (exception-based RED-first, return-`False`-without-
  raising RED-first, mutation check), but:
  - CSV-still-written test: the CSV file **is still written** when the API write fails under `warn` mode
    (the unconditional-and-raising CSV path at `:331-336` must be exercised and pass even while the API
    branch returns `False`).
  - No top-level-empty-guard test is required (there is none to test — see Goal above); if the
    implementing agent finds it useful to add a test documenting the empty-input-produces-`False`
    behavior for symmetry with P1/P3's coverage, that is acceptable but not required.
  - Re-run `SAPPHIRE_TEST_ENV=True bash run_tests.sh` (full suite) — 0 fail, 0 unexpected skip, including
    the P1 tests (must still pass — proves no regression from sequential same-file edits).

### P3 — Monthly skill metrics (`save_monthly_skill_metrics`)

- **Goal:** same pattern applied to `save_monthly_skill_metrics` (`file_writer.py:382-463`, API-write
  try block `:429-433`); call site `recalculate_skill_metrics.py:361-366` flips its check. The
  top-level empty guard (`:400-402`) changes from `return None` to `return True` per Contract 7.
- **Files:** same four files, restricted to `save_monthly_skill_metrics` and lines `:361-366`.
- **Depends on:** P2.
- **Agents:** 1, worktree-isolated. Prompt: Hard Contracts 1-8, fitness line, plus: *"The CSV write is
  conditional on `ieasyforecast_intermediate_data_path` and `ieasyforecast_monthly_skill_metrics_file`
  both being set (`:413-427`); when either is unset it must keep warning-and-skipping, not raising —
  this is a distinct behavior from pentad/decad's unconditional CSV path in P2, do not make it
  unconditional. Only the API-write try/except at `:429-433`, the top-level guard's return value, and
  the function's final return statement change. KNOWN HAZARD, do not fix: `filepath` (used at `:442`
  inside the `SAPPHIRE_CONSISTENCY_CHECK` branch) is only assigned inside the `if csv_dir and csv_file:`
  block (`:418-419`) — if your CSV-unconfigured test runs with `SAPPHIRE_CONSISTENCY_CHECK=true` it will
  hit a pre-existing `UnboundLocalError` unrelated to this plan. Keep `SAPPHIRE_CONSISTENCY_CHECK` unset
  or `false` in that test. This bug is being filed separately — do not fix it here."*
- **Acceptance criteria:** same exception-based and return-`False`-without-raising RED-first tests as
  P1/P2 (mutation check included), plus:
  - Top-level empty-guard test updated: `test_file_writer.py:317`
    (`test_empty_dataframe_guarded_no_write`) must be changed from `assert result is None` to `assert
    result is True`, and its docstring updated to match ("returns `True`" not "returns `None`"). Name
    this test explicitly in the diff — do not leave it to fail unannounced.
  - Non-empty-input-but-internal-empty-records test (Contract 7 fourth bullet), same construction
    pattern as P1 (`SAPPHIRE_SKILL_LEAD_AWARE=true`, all `horizon_value` NaN).
  - CSV-configured case: CSV written, API fails (either mechanism) → CSV still present, function still
    returns `False`.
  - CSV-unconfigured case: warn-and-skip preserved (unaffected by the API return value), and the test
    must NOT enable `SAPPHIRE_CONSISTENCY_CHECK` (see hazard note above).
  - `SAPPHIRE_TEST_ENV=True bash run_tests.sh` (full suite) — 0 fail, 0 unexpected skip, P1+P2 tests
    still passing.

### P4 — Daily skill metrics block (`save_daily_skill_metrics`)

- **Goal:** `save_daily_skill_metrics` (`file_writer.py:581-632`) returns a single bool combining both
  independent writes, per Contract 7 applied separately to each branch then ANDed: for the FDC branch,
  `True` if `fdc_metrics` is empty/None (nothing to do, `:619-620`) or the write genuinely succeeds,
  `False` if `_write_skill_metrics_to_api` returns `False` (§1a — **must be captured, not discarded**,
  this is the headline CRITICAL-2 gap) or raises; symmetrically for the threshold branch using
  `_write_threshold_skill_metrics_to_api`, which **never raises to its caller** (§1a) — so this branch's
  result is driven entirely by capturing its returned bool, not by any exception check. Both writes must
  still be **attempted independently** even if the first fails — do not short-circuit. Call site
  `recalculate_skill_metrics.py:555-566` is restructured from its current outer `try/except` (which can
  never observe a swallowed inner failure under `warn` mode — the specific defect named in the PP-051
  draft and in CRITICAL 2) into a check on the returned bool, `errors.append(...)` on `False`, matching
  the style of the other four call sites.
- **Files:** `file_writer.py:581-632` only, `recalculate_skill_metrics.py:555-566` only, same two test
  files.
- **Depends on:** P3.
- **Agents:** 1, worktree-isolated. Prompt: Hard Contracts 1-8, fitness line, plus: *"This call site's
  current shape is an outer try/except around the whole call, not an `if ret is None:` check — do not
  just copy the P1-P3 diff pattern verbatim, the call site itself changes shape (exception-check →
  return-value-check). `_write_threshold_skill_metrics_to_api` (`api_writer.py:711`) never raises to its
  caller under any documented path — a test that only mocks an exception on this function does NOT
  exercise the real failure path (disabled/unavailable/not-ready all return `False` internally); you
  MUST also test `return_value=False`. The two API writes inside `save_daily_skill_metrics` are
  independent; a failure in the FDC write must not prevent the threshold write from being attempted, and
  vice versa — verify this with a test where FDC fails and threshold succeeds (function must still
  return `False` overall, but the threshold write must have been attempted and logged as succeeded)."*
- **Acceptance criteria:**
  - Mocked failure in FDC-only, threshold-only, and both → function returns `False` in all three cases,
    **each tested via both mechanisms** (mocked exception AND mocked `return_value=False`) — for the
    threshold branch, the `return_value=False` case is mandatory and must not be skipped in favor of the
    exception-only case, since that is the exact gap CRITICAL 2 identified.
  - In the FDC-only/threshold-only failure cases, the *other* write is proven to have been attempted
    (e.g. via a call-count assertion on the mocked API client, not just a truthy return).
  - `recalculate_skill_metrics.py` exits non-zero when this signal is `False`.
  - No CSV side-effect asserted (none exists for daily).
  - Both-succeed path returns `True` and recalc still exits `0`; both-empty-input path (`fdc_metrics`
    and `threshold_metrics` both `None`/empty) returns `True` per Contract 7, not `False`.
  - Mutation check as in prior phases, applied separately to the FDC-branch and threshold-branch result
    assignments (flipping either one alone must fail a test).
  - `SAPPHIRE_TEST_ENV=True bash run_tests.sh` (full suite) — 0 fail, 0 unexpected skip, all prior-phase
    tests still passing.

### P5 — Cross-phase integration verification (gate, no new production code)

- **Goal:** confirm the four independent fixes compose correctly — a single recalc run with **multiple**
  simultaneous swallowed API failures (e.g. quarterly *and* daily both fail, pentad/decad and monthly
  succeed) still (i) attempts every mode (Hard Contract 1), (ii) accumulates **all** failure messages
  into `errors`, not just the first, and (iii) exits non-zero exactly once at the end (`:580-587`), not
  per-mode. This is the check that P1-P4 didn't each pass in isolation while silently regressing the
  shared `errors`-list/exit-code mechanism they all write into. Also confirm, in the same run, that a
  mode with **legitimately empty input** (Contract 7's `True` case) among the failing modes does NOT
  contribute to `errors` and does NOT flip a passing run's exit code — this is the specific integration-
  level check for CRITICAL 1b, since P1-P4 each verify it only in isolation.
- **Files:** `apps/postprocessing_forecasts/tests/test_recalc_workflow.py` only (test-only phase — if
  this phase finds a defect, it is fixed by re-delegating a targeted patch to the specific P1-P4 phase
  whose diff caused it, not by writing new production code here).
- **Depends on:** P1, P2, P3, P4.
- **Agents:** 1 (test-writing only, no worktree isolation required — this phase does not touch
  production code).
- **Acceptance criteria:** one new integration test with ≥2 of the five in-scope functions mocked to
  fail simultaneously (`ALL` prediction mode or equivalent multi-mode setup), and at least one further
  in-scope function mocked to receive legitimately-empty input in the same run, proves (i)-(iii) above
  plus the empty-mode-doesn't-count check, with exact-count assertions on `len(errors)` and which
  messages are present (per the testing workflow's "exact counts, not vague checks" rule), not just
  "exit code is 1". Full suite green: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — 0 fail, 0
  unexpected skip. This is the last **mandatory** phase; P6 is optional and, as revised below, does not
  ship production code either.

### P6 — OPTIONAL, owner-gated: independent post-recalc DB probe (Option C) — DEMOTED to draft-only

**Revised status (post-review): this phase is not implementable as specified and is demoted.** The
prior version left the target table, horizon filter, freshness rule, connection mechanism, and
treatment of legitimately-empty results all undefined — a bare row-count or `MAX(date)` probe can pass
on stale rows, which would make this phase actively misleading if implemented as loosely as originally
written. Its prior rationale was also self-contradictory: it claimed to be "the only part of PP-051
that helps ad-hoc invocations bypassing the wrapper" while being scoped to a change *inside* that
wrapper — that is backwards. **Corrected claim: this phase, if built, would help exactly the
opposite population — operators who run the recalc through
`bin/bimonthly_long_term_skill_metrics_recalculation.sh` (the wrapper), giving them a second,
independent signal alongside the corrected P1-P5 exit code. It would do nothing for an operator who
invokes `recalculate_skill_metrics.py` directly, bypassing the wrapper entirely — that population's
only defense is the corrected exit code from P1-P5, unchanged by this phase.**

- **Goal (of the demoted phase):** produce a properly-scoped draft — not working code — for an
  independent verification step in `bin/bimonthly_long_term_skill_metrics_recalculation.sh` (loop at
  `:104-115`, wrapper `run_skill_metrics_recalc_once` sourced from
  `bin/utils/run_skill_metrics_recalc.sh`). The draft must specify, at minimum: which table(s) and
  columns the probe reads, the horizon filter per mode, what "fresh" means precisely (not a bare
  row-count or `MAX(date)`, which passes on stale rows), how the probe connects to the DB from a shell
  script, and how it distinguishes "legitimately nothing new to check" from "should have written
  something and didn't" — mirroring Contract 7's empty-vs-failure distinction, at the shell/DB level
  this time. Check `apps/pipeline/tests/test_bimonthly_skill_recalc.py` for the existing wrapper test
  harness before proposing a new one.
- **Files:** none in this repo tree change under this phase's revised scope — the deliverable is a draft
  document, not a diff to `bin/`.
- **Depends on:** P5.
- **Agents:** 1, no worktree isolation needed (no code changes).
- **Acceptance criteria:** a draft exists (as its own plan/issue file, filed through the normal
  `doc/plans/issues/` process by whoever picks this up — not by this plan, which does not touch that
  directory) that answers every bullet in the Goal above concretely enough for an implementing agent to
  build without improvising the semantics. **Do not implement the probe itself under P6** until that
  draft exists and is reviewed — this phase's own acceptance criterion is "draft complete," not
  "wrapper script changed."

---

## 5. Residual risk (verbatim — do not summarize away in the PR description)

- **This fix does not close PP-047.** PP-047 is a different layer: the API returning HTTP 200 having
  persisted zero rows (a server/DB-side silent failure). PP-051's fix makes the client correctly report
  *client-observed* signals (network errors, non-2xx responses, timeouts, and now also the no-raise
  disabled/unavailable/not-ready/no-records paths, per the CRITICAL-1/2 fix in §1a) — it has no
  visibility into a "200 OK, nothing written" response, because from the client's perspective that is
  indistinguishable from a genuine success. The two defects compose: even after this fix ships, a
  PP-047-class failure would still produce a `True`/success return here and a clean exit. Closing PP-047
  requires either a service-side fix (colleague-owned, `sapphire/services/`) or a client-side read-back
  verification (which is exactly what `SAPPHIRE_CONSISTENCY_CHECK` attempts for CSV, at
  `file_writer.py:348-378` / `:435-461` — but per the draft's evidence, that check compares against the
  CSV it just wrote, never the API, and is unwired for quarterly/seasonal entirely).
- **The four out-of-scope forecast-data `save_*` functions keep the same defect shape.**
  `save_forecast_data`, `save_monthly_forecast_data`, `save_quarterly_forecast_data`,
  `save_seasonal_forecast_data` share the `try/except → _handle_api_write_error → return None` pattern
  but have callers in the operational and maintenance entry points (not just this one recalc script),
  so their blast radius is materially different and was explicitly not mapped here. **File this as its
  own `mid_prio_gi_draft_pp_forecast_data_silent_api_write_failure.md` (or similar) in
  `doc/plans/issues/`, indexed in `module_issues.md`** — do not fold it into this plan's phases after
  the fact.
- **The LR-007 precedent this plan was originally modeled on shares this same defect, and this plan
  deliberately does not mirror it verbatim.** `forecast_library.py:3973-3979`'s `api_ok` pattern has the
  identical discarded-return-value shape that CRITICAL 1 found here — a separate draft is being filed
  for it. This plan's Contract 7 supersedes that precedent rather than copying it; do not treat "matches
  LR-007" as sufficient justification for a diff in any phase above — matching Contract 7 is what
  matters.
- **`save_skill_metrics` (pentad/decad) has no top-level empty-input guard, unlike its four siblings
  (§1b), and this plan does not add one.** An empty `data` argument to that function will read as
  `api_ok = False` post-fix (the writer's "no records" branch fires) rather than the `True` a caller
  might now expect by analogy with quarterly/seasonal/monthly. Whether pentad/decad recalcs can ever
  legitimately run on empty input, and whether it should get a matching guard, is an open question this
  plan does not answer — flag as a candidate for its own follow-up if it turns out to matter
  operationally.
- **Contract 7's "non-empty input, writer-internal empty records → `False`" branch is a deliberate,
  slightly conservative choice, not a proven-necessary one.** It treats "every row failed a validity
  check inside the writer" as a reportable failure even though today's code only logs it as a warning
  one level down. This is more cautious than silence, but it means an operator could see a new
  `errors`-list entry post-deployment for a condition that was previously invisible; that is the
  intended effect (surfacing a real data-quality signal), but note it explicitly here so it isn't
  mistaken for a regression when it first fires.
- **Option C, now demoted to draft-only (P6), leaves ad-hoc invocations of `recalculate_skill_metrics.py`
  (outside the bimonthly wrapper) dependent solely on the corrected exit code from P1-P5, with no
  independent DB check** — and, per P6's corrected rationale, even a fully-built Option C would not
  change this for the ad-hoc population; it only adds a second signal for wrapper-invoked runs. That is
  a real improvement over today (currently even the exit code is wrong), but correctness for ad-hoc runs
  fully depends on the Python process itself accurately detecting and reporting its own failure — there
  is no second, independent observer for that path, with or without P6.
- **`SAPPHIRE_API_FAILURE_MODE=fail` is not touched, so an operator who has already set `fail` (if any
  deployment does) sees no behavior change** — the fix specifically targets the `warn` default path,
  which is where the silent-success gap lives.
- **The daily block's combined bool (P4) collapses two independent write outcomes (FDC, threshold) into
  one signal.** After this fix, a caller can tell "at least one of the two daily writes failed" but not
  which one from the return value alone — the existing per-write log lines (`:614-618`, `:624-628`)
  remain the only way to distinguish them. This is a smaller-grained version of the same
  general limitation: a boolean success/failure signal is not a substitute for structured, itemized
  failure reporting, it is only the minimum fix needed to make the exit code truthful.

---

## 6. Follow-up filing (do this after P5, before or independent of P6)

Per §5's second bullet, file `doc/plans/issues/mid_prio_gi_draft_pp_forecast_data_silent_api_write_failure.md`
covering `save_forecast_data`, `save_monthly_forecast_data`, `save_quarterly_forecast_data`,
`save_seasonal_forecast_data` (`file_writer.py`, callers to be mapped fresh — do not assume they match
the skill-metrics callers). Add an entry to `doc/plans/module_issues.md`. This is documentation/planning
work, not a code phase, and is explicitly out of scope for the P1-P6 dependency graph below.

A second, independent draft is being filed concurrently (by another agent, outside this plan's scope)
for the LR-007 discarded-bool defect at `forecast_library.py:3973-3979` referenced in §5. This plan does
not name or index that file — do not invent a filename for it here; cross-reference it once it exists.

---

## 7. Dependency graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P1", "P2", "P3", "P4"], "parallel_agents": 1 },
    "P6": { "depends_on": ["P5"], "parallel_agents": 1 }
  }
}
```

P6 is optional and owner-gated (see §4 P6 preamble) — the graph lists it for completeness but, per its
revised scope, it produces a draft document, not a code change, and should not be started without
separate explicit sign-off, distinct from approval of P1-P5.
