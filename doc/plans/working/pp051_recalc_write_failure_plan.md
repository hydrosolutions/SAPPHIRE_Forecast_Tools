# PP-051 — Long-term skill-metrics recalc: implementation plan (Option A, LR-007 shape + five-member writer outcome)

**Status:** Not started. Draft plan — subject to CLAUDE.md's mandatory multi-model review
(out-of-loop verifier pass) before any phase begins execution. **This document has been through
multiple review-and-correction passes** (a confirm-fixes review that replaced Contract 7 wholesale
after the second revision's `False`-before-the-gate prescription was found to break the documented
`SAPPHIRE_API_ENABLED=false` deployment mode; a fourth-revision pass that added Contracts 8-11 and
fixed accumulated anchor drift; and a fifth pass, current as of 2026-08-13, that pinned the exception-path
`FAILED` assignment explicitly into Contract 7 and every converting phase's prompt, fixed further anchor
drift, and **split P4 (the daily skill-metrics block) out of this plan's executable phase sequence into
its own draft, PP-054**, because P4 was not executable as specified — see P4's placeholder entry in §4).
**The design is settled as of the `WriteOutcome` five-member outcome type (Contract 7, §2)** returned by
the two `api_writer.py` write functions themselves, so "no attempt was made" and "the attempt failed" are
no longer conflatable; do not re-derive or rename it (Contract 7). This plan currently carries Hard
Contracts 1-11 (§2); the phase structure is P0 → P1 → P2 → P3 → P5 → (P6 optional), with P4 retained only
as a non-executable placeholder — see §3's scope note and §7's dependency graph. See §1a for the
corrected write-outcome mechanism and §2 Contract 7 for the outcome-type contract.
**Base:** current `docs_fdc_flv_denominator_stability` tree (module `apps/postprocessing_forecasts`
only; `apps/iEasyHydroForecast` read but not modified). Source draft:
`doc/plans/issues/high_prio_gi_draft_pp_recalc_silent_api_write_failure.md` (PP-051). Do not restate
its evidence here — this plan only sequences the fix the draft already scoped.
**Owner-locked scope:** Option A — mirror LR-007's *shape* (a bool-return contract feeding the
existing `errors`/exit-code aggregation) at the `save_*`/`recalculate_skill_metrics.py` layer, now
built on an explicit outcome type at the `api_writer.py` layer beneath it (owner's chosen design, this
revision). `forecast_library.py:3973-3979` has the same discarded-return-value defect this plan fixes
here (see updated §5) — a separate draft is being filed for it, and this plan deliberately does not
reproduce that defect. Option B (change `SAPPHIRE_API_FAILURE_MODE` default) is out of scope. Option C
(independent wrapper-side DB probe) remains an **optional** phase (P6); its status changed in the
second revision — see P6.

---

## 1. Scope confirmed by direct inspection (2026-08-12; re-verified 2026-08-12 post-review — anchors below still had drift, see Contract 6)

Five in-scope `save_*` functions in `apps/postprocessing_forecasts/src/file_writer.py`, each with
**exactly one non-test caller**, all in `apps/postprocessing_forecasts/recalculate_skill_metrics.py`
(re-verified by grep across `apps/` — see command trail in this session, not repeated here):

| Function | `file_writer.py` body | API-write try block(s) | Sole caller (`recalculate_skill_metrics.py`) | CSV fallback | Top-level empty-input guard |
|---|---|---|---|---|---|
| `save_skill_metrics` (pentad/decad) | `:291-379` | `:339-345` | `:240-245` | unconditional, **raises** on failure (`:331-336`) | **none** — see §1b |
| `save_monthly_skill_metrics` | `:382-463` | `:429-433` | `:361-366` | conditional on 2 env vars, **warns-only** if unset (`:413-427`) | yes, `:400-402` |
| `save_quarterly_skill_metrics` | `:640-672` | `:666-670` | `:430-437` | **none** — API-only | yes, `:652-654` |
| `save_seasonal_skill_metrics` | `:675-707` | `:701-705` | `:513-520` | **none** — API-only | yes, `:687-689` |
| `save_daily_skill_metrics` | `:581-632` | FDC `:614-618`, threshold `:624-628` | `:555-566` | **none** — API-only, two independent metric writes in one call | per-branch, **inside this function**, `:609` (FDC), `:623` (threshold) — the `:555-566` call site has no guard of its own |

All five currently `return None` unconditionally (success and swallowed-failure paths alike). All five
call sites but `save_daily_skill_metrics`'s check `if ret is None:` (dead-under-`warn` else branch);
the daily call site instead wraps the whole call in an outer `try/except` (`:557-566`) that can never
fire under `warn` mode because the failure is already swallowed one level down inside
`save_daily_skill_metrics` — this call site needs a **different** edit shape (return-value check, not
exception check) than the other four.

**Exit-code aggregation already exists and needs no new mechanism** — `recalculate_skill_metrics()`
accumulates a shared `errors` list across every mode block and does `sys.exit(1) if errors else
sys.exit(0)` at `:580-587`. This describes the **P1-P3** work only (P4, the daily block, is split out —
see its placeholder entry in §4 and PP-054): each of those phases needs to (a)
make its `save_*` return `True`/`False` instead of bare `None` per the corrected contract in §2, and (b)
flip its call site's dead branch (`if ret is None` / outer-`try/except`) into a check that appends to the
*existing* `errors` list on `False` — the predicate is pinned in Contract 10, §2. No change to the
aggregation or exit mechanism itself. **P0 does neither (a) nor (b)** — it lands the `WriteOutcome` type
inside `api_writer.py` that P1-P3 then consume; see the P0 phase entry in §4.

**`apps/iEasyHydroForecast` is NOT modified.** This plan touches
`apps/postprocessing_forecasts/src/api_writer.py` (P0 only — see P0's Files list in §4),
`apps/postprocessing_forecasts/src/file_writer.py` (P1-P3; the daily block's edits are deferred to
PP-054), and
`apps/postprocessing_forecasts/recalculate_skill_metrics.py` (P1-P3, same deferral). `_handle_api_write_error` and
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

### 1a. The defect this plan must actually fix, and why the second revision's fix was wrong

**What is broken today (unchanged finding, still correct):**

- `apps/postprocessing_forecasts/src/api_writer.py:429` — `_write_skill_metrics_to_api(data,
  horizon_type, year) -> bool` returns `False` **without raising** in four places:
  `SAPPHIRE_API_AVAILABLE` false (`:462-464`), `SAPPHIRE_API_ENABLED` not `"true"` (`:467-470`),
  `client.readiness_check()` false (`:478-480`), and "no records after filtering" (`:706-708`, final
  return at `:708` — see §1a's line-range correction below). It raises for the actual
  `client.write_skill_metrics(...)` HTTP call itself (`:700`, unwrapped — propagates to the caller) and
  for an invalid `horizon_type` (`:456-460`, a programming error, not an operational one) — **but that
  is not the exhaustive raise inventory it may look like.** Also verified to raise: `_get_postprocessing_client()`
  itself (`:475`), `client.readiness_check()` (`:478` — the call, not just its `False` return), the
  date-computation block (`:553-592`, e.g. `.astype(int)`, `tl.get_date_for_pentad`/`get_date_for_decad`,
  `dt_module.date(year, m, 1)` on an out-of-range month), and the `from src.aggregation import
  get_season_months` import (`:589`). Unlike the threshold writer (which guards `if client is None: return
  False` at `:742-743`), this function has **no such guard** — a `None` client `AttributeError`s at
  `:478`. There is also an unreachable second `raise ValueError` at `:497-502` (unreachable because
  `HORIZON_TYPE_TO_API` at `:53-64` already holds all six keys the `elif` chain handles, so no input can
  fall through to it). **This list is a starting point, not a claim of exhaustiveness — P0's agent must
  re-derive the full raise inventory for this function by reading the body, not copy this bullet as
  complete.**
- `apps/postprocessing_forecasts/src/api_writer.py:711` — `_write_threshold_skill_metrics_to_api(data,
  year) -> bool` **does raise to its caller, verified** — this corrects an earlier revision of this plan,
  which claimed it never does. `_get_postprocessing_client()` is called at `:741` and
  `client.readiness_check()` at `:746`, and the function's `try:` does not open until `:753` — both of
  those guard calls, along with the `data is None or data.empty` guard (`:726-728`) and the
  availability/enabled/client-is-None checks (`:730-751`), sit **outside** the try block. Any exception
  from client construction or the readiness check — connection refused, DNS failure, timeout, i.e. the
  ordinary operational failures this plan exists to surface — escapes uncaught to this function's caller.
  Only the code from `:753` through the outer `except` at `:827-829` (see §1a's line-range correction
  below) is exception-protected. Inside that protected region: the one HTTP call it makes (`:802`) is
  wrapped in its own inner `try/except` that converts `AttributeError` (`:809-814`, client too old —
  "Stage 2 pending") and a 404 (`:815-824`, "endpoint not deployed yet") into `False` **by design, with an
  inline comment saying so** — these two are self-documented expected conditions, not failures. Any
  *other* exception from that inner block re-raises to the function's own outer `try/except`
  (`:827-829`, not `:826-828`), which logs "Failed to write threshold skill metrics to API" and also
  returns `False` — this outer-catch case is a genuine failure this function *does* absorb into a return
  value, but it does not change the fact that the pre-`:753` guard section can still raise uncaught. **P4
  must handle both: the caught-and-returned `False` from the protected region, and the raw exception from
  the unprotected guard section** — see P4 below.
- Every current call site (`file_writer.py:341,431,616,668,703,626`) wraps the call in a `try/except`
  routed only to `_handle_api_write_error` — and **discards the function's return value entirely** (the
  call is a bare statement, not an assignment). Nothing today observes any of the signals above; every
  call site reads as success regardless of what actually happened.

**Why the second revision's fix was wrong, and why it was not merely incomplete:** that revision kept a
single bool per `save_*` function and said to initialize it (`api_ok = False`) *before* the
`if api_writer.SAPPHIRE_API_AVAILABLE:` gate, so a closed gate would read as failure. But a closed gate
(`SAPPHIRE_API_AVAILABLE` false, i.e. `sapphire_api_client` not installed) and a disabled write
(`SAPPHIRE_API_ENABLED=false`) are **not failures** — both are documented, supported deployment
configurations (`doc/configuration.md:159`; `doc/plans/sapphire_api_integration_plan.md:283,321` —
local dev and the documented emergency rollback path both run with `SAPPHIRE_API_ENABLED=false`). Under
that fix, a recalc run in either configuration would report `False` for every mode, every time, and
`sys.exit(1)` on a run where the CSV write succeeded and nothing was actually wrong — a **worse** bug
than the one being fixed, because it would fire on every routine run of a supported mode rather than
only on genuine, intermittent failures. The LR-007 precedent this plan was modeled on gets this right:
it initializes `api_ok = True` (`forecast_library.py:3973`), the opposite of what the second revision
prescribed.

**The owner's chosen fix (this revision): stop collapsing distinct outcomes into one bool at the
source.** `_write_skill_metrics_to_api` and `_write_threshold_skill_metrics_to_api` each conflate up to
five outcomes into a single `True`/`False`. Only one of those — the readiness-check failing, or the
underlying HTTP call raising — is a genuine failure. The rest (client absent, writing disabled, nothing
to write, and for the threshold writer specifically, the Stage-2-not-deployed cases) are deliberate or
benign and must never be reported as failure. P0 (new, below) changes both functions' return type from
`bool` to an explicit `WriteOutcome` (defined in full in §2 Contract 7) that names which of these
happened, so every later phase — which still returns a plain bool from its `save_*` function to
preserve the existing `errors`-list aggregation (Hard Contract 4) — maps that outcome down to `True`
(success) for everything except the one genuinely-failed case.

### 1b. Asymmetry in `save_skill_metrics` (pentad/decad) — accepted, not fixed here; one mechanism correction

Unlike its four siblings, `save_skill_metrics` (pentad/decad) has **no top-level `data is None or
data.empty` guard** — a non-empty-but-columnless-frame or `None` flows straight through to an
unconditional CSV write (pre-existing behavior) and to the API writer, where Contract 7 now applies:
non-empty input reaching the writer with zero records after its own internal filtering produces
`WriteOutcome.SKIPPED_NO_RECORDS` → mapped to `True` (non-failure) by the `save_*` wrapper. **A bare
`pd.DataFrame()` (no columns) is a different case and does not reach that path at all** — correction to
this section from the prior revision, which claimed it does: `data[period_col] = ...`
(`file_writer.py:318`, unconditional column access, before either the CSV write or the API branch) 
raises `KeyError` immediately on a columnless frame. **Corrected description of the existing test (an
earlier revision of this section mischaracterized it):** `test_scoped_skill_recalc.py:210-214` sets
`calculate_skill_metrics.return_value = (pd.DataFrame(), non_empty_data, MagicMock())` — the bare
`pd.DataFrame()` is a real, literal empty DataFrame, the tuple's first element (standing in for
`calculate_skill_metrics`'s own empty-result return); it is not a `MagicMock`. The `MagicMock()` is the
tuple's unrelated *third* element. What actually prevents this test from hitting the `KeyError` described
above is that **`save_skill_metrics` itself is separately mocked at `:216`**, short-circuiting before the
real function body — and its `data[period_col] = ...` column access — ever runs; it has nothing to do
with how the empty DataFrame is constructed. P2's Contract-7 tests must exercise the writer's
`SKIPPED_NO_RECORDS` outcome using a **non-empty-but-zero-surviving-rows** DataFrame (e.g. all rows
dropped by the lead-aware filter) against the *real*, unmocked `save_skill_metrics`, not a bare
`pd.DataFrame()`, since the latter never reaches the code this plan changes. **P2 does not add a top-level empty guard to fix the `KeyError` exposure** — that would be a
new, unscoped behavior change (whether pentad/decad recalcs can legitimately run on empty input is a
separate question this plan doesn't answer) and CLAUDE.md's orchestration protocol requires changes
stay "purely additive or modify only the specific behavior described." This is called out explicitly so
it is a documented, deliberate omission rather than a gap discovered later — see the matching §5
residual-risk bullet.

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
4. **No half-converted return contract, at either layer.** Two layers change type in this plan:
   `api_writer.py`'s two write functions (`bool` → `WriteOutcome`, landed once in P0) and each
   `save_*` function in `file_writer.py` (`None` → `bool`, landed per-function in P1-P3 — the daily
   function's conversion is deferred to PP-054, see P4's placeholder in §4 — unchanged
   shape from earlier revisions of this plan). A `save_*` function must never be merged in a state
   where it returns a bool while its sole caller in `recalculate_skill_metrics.py` still checks
   `if ret is None:` (or vice versa) — that silently breaks the check it was aggregated to fix (a
   `True` success return would fail an `is None` test and get logged as an error; a stale
   `if ret is None` after `False` uses `False is None` → also evaluates False, silently going to the
   "error" `else` branch — different bug, same "don't split these" lesson). Each phase's
   function-change and call-site-change are one atomic diff from one agent; no phase merges with only
   one half done. P0 is the one phase where this rule applies within `api_writer.py` itself: the two
   functions' return statements and their direct unit tests (§ Contract 7, P0 below) must convert
   together, in one diff.
5. **`SAPPHIRE_API_FAILURE_MODE=fail` behavior is unchanged for all existing callers.**
   `_handle_api_write_error` (`forecast_library.py:95-116`) is not modified by this plan; `fail` mode
   still re-raises exactly as it does today, upstream of any of these changes.
6. **Before editing, re-verify every file:line anchor by grep — and report drift, don't silently
   edit around it.** The tables and line citations in this plan reflect the tree at various
   plan-revision timestamps; **four separate review rounds have each found stale or wrong anchors in this
   same document** (most recently: the `_setup_mocks` line numbers in P1-P3's Files lists, the
   `WriteOutcome`-function line ranges in §1a/P0, and the `_write_threshold_skill_metrics_to_api`
   raise-behavior claim itself — see §1a). Trusting a hand-copied line number in this plan, even one
   marked "verified," has repeatedly been wrong. Binding on every phase agent, standing instruction: **grep
   for the function/test name to confirm the current line number before editing by that number; if the
   grep result disagrees with what this plan states, treat that as a finding to report in the phase's
   summary (which lines moved and to where), not something to quietly patch around or ignore.** This does
   not relax any other contract — it is in addition to, not instead of, verifying blast radius and test
   counts described elsewhere in this plan.
7. **`WriteOutcome` contract (replaces the second revision's Contract 7 wholesale).** One bool cannot
   distinguish "nothing was attempted" from "the attempt failed" from "the attempt correctly found
   nothing to send" — conflating them is what made the second revision's fix worse than the bug it
   targeted (§1a). P0 replaces the bool return of `_write_skill_metrics_to_api` and
   `_write_threshold_skill_metrics_to_api` (`api_writer.py`) with an explicit outcome type, defined
   once in `api_writer.py` next to `HORIZON_TYPE_TO_API` and imported by `file_writer.py` through its
   existing `from . import api_writer` (no new import needed — reference as
   `api_writer.WriteOutcome.WROTE`, etc.):

   ```
   class WriteOutcome(Enum):
       WROTE = "wrote"                          # records were sent and accepted
       SKIPPED_BY_CONFIG = "skipped_by_config"   # client absent, or SAPPHIRE_API_ENABLED=false
       SKIPPED_NO_RECORDS = "skipped_no_records" # nothing to write after filtering
       SKIPPED_NOT_DEPLOYED = "skipped_not_deployed"  # threshold endpoint not yet deployed (Stage 2)
       FAILED = "failed"                         # readiness-check failure, or the write call raised
   ```

   **Member names are pinned, not illustrative — do not rename.** P1-P5 hardcode
   `WriteOutcome.SKIPPED_BY_CONFIG`, `FAILED`, and `SKIPPED_NOT_DEPLOYED` verbatim in their agent prompts
   and mutation checks; renaming any member breaks every later phase's literal text against a plan that
   has already survived three review rounds on this design. All five names above (`WROTE`,
   `SKIPPED_BY_CONFIG`, `SKIPPED_NO_RECORDS`, `SKIPPED_NOT_DEPLOYED`, `FAILED`) and their semantics are
   fixed.

   **Mapping inside `_write_skill_metrics_to_api`** (`api_writer.py:429`, unaffected branches unchanged
   — only the return statements convert):
   - Invalid `horizon_type` (`:456-460`) — unchanged, still `raise ValueError` (a programming error, not
     an outcome).
   - `SAPPHIRE_API_AVAILABLE` false (`:462-464`) → `SKIPPED_BY_CONFIG`.
   - `SAPPHIRE_API_ENABLED` not `"true"` (`:467-470`) → `SKIPPED_BY_CONFIG`.
   - `client.readiness_check()` false (`:478-480`) → `FAILED`. This is one of the two genuine-failure
     paths — do not fold it into a `SKIPPED_*` member.
   - Zero records after filtering (`:706-708`) → `SKIPPED_NO_RECORDS`. **Do not change this branch's
     *trigger* condition** (out of this plan's owner-locked scope) — only its *return value* converts,
     from `False` to `SKIPPED_NO_RECORDS`. **Exactly 3 existing tests are in scope for this rename, all in
     `test_api_integration.py`: `:735`, `:868`, `:1237`** (verified — not "4+" as an earlier revision of
     this plan claimed). Two more tests share the identical name `test_*_empty_data_returns_false` at
     `:373` and `:1576` but belong to `_write_combined_forecast_to_api` and
     `_write_monthly_ensemble_to_api` respectively — those functions are **out of scope** for this plan
     (§1, §5). P0's agent must not convert them by name-matching alone; name-search for this pattern and
     then filter by which function each test actually calls.
   - Success (`:698-705`) → `WROTE`.
   - `client.write_skill_metrics(...)` raising (`:700`, unwrapped) — **still propagates**, unchanged;
     see the call-site rule below for how the raised exception becomes an outcome one level up.

   **Mapping inside `_write_threshold_skill_metrics_to_api`** (`api_writer.py:711`, resolves the
   PP-051-adjacent gap the review flagged: **this function has two self-documented EXPECTED `False`
   returns that must not become `FAILED`**):
   - `data is None or data.empty` (top-of-function guard) → `SKIPPED_NO_RECORDS`.
   - `SAPPHIRE_API_AVAILABLE` false, `SAPPHIRE_API_ENABLED` not `"true"`, or `client is None` → all
     `SKIPPED_BY_CONFIG`.
   - `client.readiness_check()` false → `FAILED` (the other genuine-failure path, same reasoning as
     above).
   - Zero records after mapping (`if not records:`) → `SKIPPED_NO_RECORDS`.
   - `AttributeError` from `client.write_threshold_skill_metrics(...)` (`:809-814`, comment: "client
     does not support write_threshold_skill_metrics yet (Stage 2 pending)") → **`SKIPPED_NOT_DEPLOYED`,
     not `FAILED`.** Folding this into `FAILED` would make every DAILY/ALL recalc exit 1 permanently on
     any deployment where Stage 2 isn't rolled out yet — the exact regression this item exists to
     prevent.
   - HTTP 404 / "Not Found" (`:815-824`, inline comment: "this is expected before Stage 2 is
     deployed") → **`SKIPPED_NOT_DEPLOYED`**, same reasoning.
   - Any other exception *from the HTTP call itself* (`:802`), caught by the function's own outer
     `try/except` and logged as "Failed to write threshold skill metrics to API" → `FAILED`. This is the
     genuine-failure path this function's own protected region (`:753` onward) absorbs into a return value
     rather than propagating (verified: no test file in `apps/postprocessing_forecasts/tests/` calls this
     function directly today, so P0 must *add* new unit tests for this mapping — there is nothing existing
     to convert). **This does not cover every raise this function can produce** — see §1a's correction:
     `_get_postprocessing_client()` (`:741`) and `client.readiness_check()` (`:746`) sit *outside* the try
     block and raise straight to the caller, uncaught by this function at all. P0's tests must cover this
     mapped `FAILED` path; P4 (which calls this function) must separately handle the unmapped, propagating
     raise from the guard section — see P4 below.
   - Success → `WROTE`.
   - **Design choice, stated with justification (not left implicit):** `SKIPPED_NOT_DEPLOYED` is a
     distinct member rather than folded into `SKIPPED_BY_CONFIG`, because "the operator disabled this"
     and "this isn't rolled out on the server yet" are different facts an operator reading logs needs to
     tell apart — the first is a local choice, the second is an infrastructure gap elsewhere. Both map
     to the same non-failure outcome at the `save_*`/exit-code layer (see mapping below), so this
     distinction costs nothing at the exit-code layer and only adds clarity at the log layer.

   **`SAPPHIRE_API_FAILURE_MODE=ignore` (`forecast_library.py:82-92`, tail at `:114-116` — corrected from
   an earlier revision's `:113-116`; `:113` is the fail-mode bare `raise`, the warn/ignore tail is
   `:114-116`) — the design
   must state how this interacts with the outcome, and — corrected in this pass — must be consulted on
   BOTH paths that can produce `FAILED`, not just the raising one.** A prior version of this contract
   placed the `ignore` check exclusively inside the exception-handling branch, so a writer that returns
   `FAILED` **without raising** (e.g. `client.readiness_check()` returning `False`, mapped directly at
   `api_writer.py:478-480`) never reached any `except` and therefore never consulted
   `fl._get_api_failure_mode()` at all. Result: under `ignore`, a readiness-check failure would still exit
   1 while a raised write error correctly exited 0 — the same setting producing opposite outcomes, and
   readiness failure (the API being down) is the *common* case, so `ignore` would fail silence exactly
   where operators need it most. `_handle_api_write_error` is **not modified by this plan** (Hard Contract
   5) — it already fully embodies `SAPPHIRE_API_FAILURE_MODE` semantics (`fail` re-raises, `warn` logs and
   continues, `ignore` silently continues) for the exception path, and stays that way. The `WriteOutcome`
   conversion happens entirely at the call site, in `file_writer.py`, which already imports
   `forecast_library as fl` and can therefore call the existing `fl._get_api_failure_mode()` (also not
   modified):
   - The call-site pattern for every `save_*` function's API-write block becomes: initialize the
     outcome to `SKIPPED_BY_CONFIG` (the correct default when the gate never opens — this is the fix for
     what the second revision got backwards, since a closed gate is a configuration state, not a
     failure); if the gate is open, assign the outcome from the writer's **return value** (previously
     discarded, §1a) inside the `try`. If the `try` raises, call `fl._handle_api_write_error(e, ...)` as
     today (unchanged), **and then, in the same `except` block, explicitly assign the outcome variable to
     `FAILED`** — this is the literal line three prior revisions of this plan left unstated, and its
     absence is what would leave the pre-gate `SKIPPED_BY_CONFIG` initialization standing after the call
     returns, so a genuinely-raised write error reads as non-failure:
     ```python
     except Exception as e:
         fl._handle_api_write_error(e, "<description>")   # may re-raise under fail mode
         outcome = api_writer.WriteOutcome.FAILED          # <-- this line, currently unstated
     ```
     Under `warn`/`ignore` `_handle_api_write_error` does not re-raise, so execution reaches this
     assignment and then the same post-processing step described next; under `fail` it re-raises and the
     function never reaches either the assignment or that step, so Hard Contract 5 holds without
     special-casing. **Single, uniform post-processing step applied to the resulting outcome regardless of
     which mechanism produced it (returned directly, e.g. a readiness-check failure, OR assigned in the
     `except` block above):** if the outcome is `FAILED` and `fl._get_api_failure_mode() == "ignore"`,
     downgrade it to `SKIPPED_BY_CONFIG`; otherwise leave it as `FAILED`. This is what makes `ignore` mode
     behave identically for both failure mechanisms — the entire point of this correction.
   - **Justification for reusing `SKIPPED_BY_CONFIG` rather than adding an `IGNORED` member:** `ignore`
     mode is itself an operator configuration choice to make write outcomes invisible to the exit code —
     the same category as "writing disabled" and "client absent," just triggered by a different env var.
     Splitting it into a sixth member would not change the exit-code mapping (still non-failure) and
     would only matter if some future consumer needed to distinguish "disabled" from "ignored," which
     nothing in this plan's scope does.
   - **Acceptance criteria in P1-P3 must test both mechanisms under `ignore`, not just the raising one**
     (see each phase's acceptance list below, which previously covered only "mocked exception under
     `ignore`" — a `FAILED`-returned-directly-under-`ignore` test is now required in every phase).

   **The `save_*` mapping (what every phase after P0 implements):** `FAILED` → the function's bool
   return is `False`, and the caller in `recalculate_skill_metrics.py` appends to `errors`. **Every
   other outcome — `WROTE`, `SKIPPED_BY_CONFIG`, `SKIPPED_NO_RECORDS`, `SKIPPED_NOT_DEPLOYED` — → the
   function's bool return is `True`, and nothing is appended to `errors`.** State this explicitly in
   every phase's agent prompt: **"no API write was attempted" must never be reported as failure** —
   that sentence is the one-line summary of what the second revision's Contract 7 got backwards, and it
   is the thing every later phase must not regress.

   **Top-level empty-input guards are unchanged and layered on top of, not replaced by, this mapping.**
   The existing per-function `data is None or data.empty` guards (monthly/quarterly/seasonal's
   top-level checks; for `save_daily_skill_metrics`, per-branch guards **inside the function itself**, at
   `file_writer.py:609` (FDC) and `:623` (threshold) — **not at the `recalculate_skill_metrics.py:555-566`
   call site, which has no such guard of its own**, correcting an earlier revision's wording; **does not
   apply to `save_skill_metrics` — see §1b**) still short-circuit to `True` before the writer is ever
   called, exactly as in the earlier revisions of this plan. The `WriteOutcome` mapping above governs
   everything that reaches the writer; the top-level guards govern everything that doesn't.

   **Partial-row-loss caveat — carried forward into §5, not a phase requirement:** `WROTE` means *at
   least one* record was accepted, not that every input row survived (writer-internal NaN/NULL/dedup
   drops — see the §5 residual-risk bullet on this). This plan does not change that; it is noted here so
   no phase's tests assume `WROTE` implies row-complete.
8. **Signature/annotation/docstring updates are authorized, not a scope violation, at both layers.**
   `_write_skill_metrics_to_api` and `_write_threshold_skill_metrics_to_api` (`api_writer.py`, P0) must
   have their return type annotation changed from `-> bool` to `-> WriteOutcome` and their docstrings'
   `Returns:` sections rewritten to name the five outcomes. Every `save_*` function whose return
   contract changes in P1-P3 (`save_skill_metrics`, `save_monthly_skill_metrics`,
   `save_quarterly_skill_metrics`, `save_seasonal_skill_metrics`) must have
   its return type annotation changed to `-> bool` and its docstring's `Returns:` section rewritten to
   state the Contract 7 `True`/`False` meaning, **in the same diff** that changes the return
   statements — the stale-annotation pattern this rule exists for is visible today in
   `save_daily_skill_metrics`, currently annotated `-> None` and documenting `Returns: None`
   (`file_writer.py:585`, `:601-602`) — but that function's own conversion is deferred to PP-054 (see P4's
   placeholder in §4), so this rule applies to P1-P3's four functions here, not to that one. Each phase's
   agent prompt must say this explicitly: "do not
   change function signatures" means the *parameter* list, not a return annotation left stale by design.
9. **Log messages must carry the outcome, not the bare bool.** `recalculate_skill_metrics.py`'s
   error-message templates interpolate the discarded return value directly via `{ret}` at exactly eight
   **in-scope** sites — a `logger.error` line immediately followed by the `errors.append` line that
   reports the same value, at each of the four skill-metrics call sites this plan touches: `:244-245`
   (pentad/decad, P2), `:365-366` (monthly, P3), `:436-437` (quarterly, P1), `:519-520` (seasonal, P1).
   **`:237-238, :336-337, :405-406, :488-489` are a different, out-of-scope set of eight** (a grep for
   `{ret}` returns 16 hits total: the 8 in-scope pairs above plus these 8) — they belong to
   `save_forecast_data` / `save_monthly_forecast_data` / `save_quarterly_forecast_data` /
   `save_seasonal_forecast_data` respectively (§5's "four out-of-scope forecast-data `save_*` functions"
   bullet), which keep the `ret is None` convention and where `{ret}` still carries a real error string
   today — do not touch those four call sites (eight lines) in any phase below. Under the new contract,
   printing the in-scope `{ret}`s would show a bare `False` with no diagnostic value on **both** lines —
   the `logger.error` line is what an operator reads first, and leaving it printing `False` defeats this
   contract just as much as leaving the `errors.append` line unfixed — while the actual cause (readiness
   failure vs. a raised exception vs., pre-fix, nothing at all) is already logged one level down inside
   `api_writer.py` (`:463` unavailable, `:469` disabled, `:479` not-ready, `:707` no-records) and inside
   `_handle_api_write_error` for the exception path. Each phase converting its one in-scope call site must
   change **both** of its `{ret}`-style interpolations — the `logger.error` line and the `errors.append`
   line — to messages that reference the mode/horizon and point at the detailed log line already emitted
   below them (e.g. "quarterly skill metrics API write failed — see log above for detail"), not literally
   print `False` on either line.
10. **Call-site failure predicate is pinned: `if ret is False:`, not `if not ret:`.** Both satisfy the
    weaker "append to `errors` on `False`" wording used elsewhere in this plan, but they diverge on a
    stray `None` — which still occurs mid-rollout, from a not-yet-updated test mock for the *same*
    function this phase converts. **Mechanical warning, binding on every phase that converts a call site:**
    today's shape at every one of the four in-scope sites (`recalculate_skill_metrics.py:241-245,
    :362-366, :433-437, :516-520`) is `if ret is None: <log success> else: <log error + errors.append>`.
    Substituting the predicate alone (`is None` → `is False`) without also **swapping which body sits in
    the `if` and which sits in the `else`** inverts the semantics: a real success (`True`) would fall into
    the `else` and get logged/appended as an error, and a real failure (`False`) would satisfy the new
    `if ret is False:` condition but execute the body that used to mean success. The fix is not a
    find-and-replace on the condition text — it is `if ret is False: <log error + errors.append> else:
    <log success>`, bodies swapped. `if not ret:` treats `None` as failure and breaks two existing tests,
    only one of which is genuinely outside any phase's Files list today:
    `test_integration_postprocessing.py::test_pentad_mode_calls_correct_functions` (patches
    `save_skill_metrics` with `return_value=None` at `:3005-3007`, asserts exit 0 at `:3023`) is not in any
    phase's Files list. **The second, `test_wiring_integration.py`'s `capture_save_skill` `side_effect`
    (`return None` at `:1933` — corrected from an earlier revision's `:1932`, which is
    `saved_skills["pentad"] = df`, not the `return`), IS in P2's Files list** (the mock assignment at
    `test_wiring_integration.py:1942` that wires `save_skill_metrics.side_effect = capture_save_skill` is
    explicitly listed there) — an earlier revision of this contract claimed neither test was in any
    phase's Files list, which was only true for the first one. P2's agent will touch this file regardless;
    the `if ret is False:` choice still matters because it means this specific mock does not *need* to be
    converted for the exit-0 assertion at `:1949` to keep passing, reducing (not eliminating) P2's
    required edits here. `if ret is False:` treats `None` as success — matching today's *implicit*
    convention (every current call site's `if ret is None:` already treats the only value the function
    ever returns as success) and requiring no changes to either test file above. **Pinned choice: `if ret
    is False:`.** Justification: it is the lower-blast-radius option (no test files outside each phase's
    already-scoped Files list need touching), and it keeps faith with this plan's own stated philosophy —
    "no attempt/nothing sent must never be reported as failure" (Contract 7) — by erring toward
    non-failure on any value that is not an unambiguous, exact `False`. The trade-off, accepted
    explicitly: an un-converted sibling `save_*` mocked to return `None` reads as success throughout the
    rollout, which is only safe *because* Contract 4 requires each phase's function and call site to
    convert atomically — once every in-scope phase has landed, no production in-scope `save_*` can return
    anything but `True`/`False`, so the `None`-tolerance stops mattering for those functions. **P5 (§4)
    must add a check that no in-scope
    `save_*` function can still return `None`** (e.g. a signature/type-level or docstring-`Returns:`-only
    assertion is not sufficient — assert behaviorally, by calling each with an input shape that would have
    hit a `None` path pre-fix, across all **four** in-scope functions — `save_daily_skill_metrics` is out
    of scope, see the P4 placeholder in §4 and PP-054) once P0-P3 have landed, so this transitional
    tolerance does not become a silent permanent gap for those four.
11. **`SAPPHIRE_CONSISTENCY_CHECK` stays non-failing — this plan does not fold it into the `False`
    return.** It runs *after* the API write, inside `save_skill_metrics` (`:348-377`, pentad/decad, P2)
    and `save_monthly_skill_metrics` (`:435-461`, monthly, P3), logging `"CONSISTENCY CHECK FAILED"` while
    returning `None` today (soon `True`, per Contract 7, once P2/P3 land — i.e. it never contributes to
    the function's bool return, before or after this plan). This plan's thesis is "a logged error that
    never reaches the exit code is the bug" — which is exactly what `SAPPHIRE_CONSISTENCY_CHECK`'s current
    behavior is, so an implementing agent could plausibly read this plan as authorizing folding it into
    the `False` path too. **It does not.** `SAPPHIRE_CONSISTENCY_CHECK` is a separate, optional,
    env-var-gated diagnostic that compares the just-written CSV against itself (§5's PP-047 bullet) — its
    scope, trigger, and semantics are undefined by this plan and changing its failure-visibility would be
    an unscoped behavior change affecting every deployment that has the env var enabled. P2 and P3's agent
    prompts must state this explicitly: leave `SAPPHIRE_CONSISTENCY_CHECK`'s `None`/logged-only behavior
    untouched; only the function's *own* success/failure return (driven by the `WriteOutcome` mapping)
    converts.

**Related-but-out-of-scope hazard (P3 only):** `file_writer.py:413-442` — `filepath` is bound only
inside `if csv_dir and csv_file:` (monthly's conditional-CSV branch) yet is referenced unconditionally
inside the `SAPPHIRE_CONSISTENCY_CHECK` branch below it, producing an `UnboundLocalError` when CSV env
vars are unset and the consistency check is enabled. This is being filed as its own draft by another
agent working in `doc/plans/issues/` concurrently — **do not fix it here.** P3 touches this exact
function, so its agent prompt and its required CSV-unconfigured test must both note the hazard so
nobody trips it or "fixes" it inline; see P3 below.

---

## 3. Phasing rationale

**Scope note (post-split):** this section's ordering rationale, including the P4/daily discussion below,
was written while P4 was still an executable phase of this plan. P4 is now split out to PP-054 (see its
placeholder in §4) — the rationale for why daily would have been ordered last is retained below because
PP-054 inherits it, not because P4 still executes here. Everywhere below, "P1-P4" describes that original,
now-superseded ordering; this plan's actual executable sequence is P0 → P1 → P2 → P3 → P5 → (P6).

**P0 comes first because every later phase consumes its output.** P1-P3 each need to compare a
captured return value against `api_writer.WriteOutcome` members — that type does not exist until P0
lands it, and P0's own tests (§4 P0) are the only thing that pins the outcome-mapping contract
independent of any `save_*` function. This is a genuine data dependency (P1-P3 import the enum P0
defines), unlike the P1→P2→P3 ordering below, which is not.

All five `save_*` call sites live in the **same file** (`recalculate_skill_metrics.py`), in disjoint
but adjacent line ranges. Running multiple worktree-isolated agents on the same file in parallel risks
exactly the failure mode Hard Contract 4 warns about: two agents editing overlapping context, or one
agent's edit shifting line numbers the other agent was told to target, producing a merge that looks
clean but silently leaves one call site half-converted. There is no *logical* dependency between fixing
pentad/decad vs. quarterly vs. monthly vs. daily — but there is a **shared-file execution-order**
dependency, compounded in this revision by a **shared-fixture** dependency: `test_recalc_workflow.py`'s
`_setup_mocks` helper and several blocks in `test_wiring_integration.py` mock multiple `save_*`
functions' return values in the same place (§4, Hard Contract note in each phase). Phases below are
therefore sequenced (not parallelized) purely for this reason; the dependency graph's `depends_on`
edges encode "must land and be merged first so line numbers and file/fixture state are current," not a
data/control dependency (except P1-P3 → P0, which is a real data dependency, see above).

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
at all (§1b). **P4's `SKIPPED_NO_RECORDS` ambiguity does not exist for either branch — corrected from an
earlier revision of this section, which claimed the FDC branch could reach it "structurally, the same way
P1/P3 can."** `save_daily_skill_metrics` sets `fdc_data["day_in_year"] = 1` unconditionally for every row
(`file_writer.py:612`), so the `dropna` at `api_writer.py:507` never drops anything for this call path,
and FDC data carries no `horizon_value` column at all, so `:625`'s lead-aware exclusion sets it to `0`
rather than ever entering the NaN-filter branch that produces the ambiguity in P1/P3. **A non-empty
`fdc_metrics` therefore always yields at least one surviving record** — P4's agent must not attempt to
construct a `SKIPPED_NO_RECORDS`-via-internal-filtering test for the FDC branch; that shape is
unreachable on this call path. (The threshold branch was already correctly described as not reaching this
ambiguity, for a different reason — it is guarded before the call, per §2 Contract 7.)

---

## 4. Phases

Every acceptance criterion below that says a function "returns `False`" or "returns `True`" (P1-P3,
`save_*` layer) or "returns `WriteOutcome.X`" (P0, `api_writer.py` layer) means an **exact identity
assertion** (`assert result is False` / `assert result is api_writer.WriteOutcome.FAILED`, etc.), never
a truthy/falsy check — today's code returns bare `None`/`bool`, which coerces to falsy/truthy in ways
that would let a vacuous assertion pass against unfixed code. Every P1-P3 phase's test list must cover
**both** failure mechanisms for its function(s): the writer's outcome being `FAILED` without an
exception propagating (readiness-check failure — patch `_write_skill_metrics_to_api` with
`return_value=api_writer.WriteOutcome.FAILED`) and the writer *raising*
(`side_effect=Exception(...)`, which the call site converts to `FAILED` per Contract 7's failure-mode
rule) — a `side_effect` test alone only proves the rarer exception path and would leave the headline
defect (non-raising `False`/non-`WROTE` outcomes silently reading as success) unverified. Verification
for every phase is the **full** suite per CLAUDE.md: `cd apps && SAPPHIRE_TEST_ENV=True bash
run_tests.sh` (zero fail, zero unexpected skip) — not a module-scoped invocation. This transitively
includes `apps/iEasyHydroForecast` (unmodified by this plan, per §1, but part of the mandatory
full-suite gate regardless).

**Shared-fixture rule for every P1-P3 phase (do not skip):** `test_recalc_workflow.py`'s `_setup_mocks`
helper (`:99-161`, corrected from an earlier revision's `:99-140` — verified: `def _setup_mocks` opens at
`:99` but its body, including the `return {...}` block, runs to `:161`; the earlier range would have cut
off before the quarterly/seasonal mock entries and the `return` statement, used across ~17 call sites in
that file) and several blocks in
`test_wiring_integration.py` set mocked return values for *multiple* `save_*` functions in one place.
Each phase's agent prompt must say explicitly: **change only your own function's entries in these
shared blocks; leaving sibling functions' entries on the old `None`/string convention is correct until
their own phase runs** — that is not a partial fix, it is the intended state of a not-yet-converted
sibling, and "fixing" it early would silently duplicate another phase's work and risk a conflicting
diff when that phase lands.

### P0 — Writer outcome type (`api_writer.py`), landed before any `save_*` phase

- **Goal:** Define `WriteOutcome` (Contract 7) in `api_writer.py` and convert
  `_write_skill_metrics_to_api` and `_write_threshold_skill_metrics_to_api`'s return statements from
  `bool` to `WriteOutcome` per the mapping in Contract 7 — no other line in either function's body
  changes (validation, filtering, the HTTP calls themselves, and every *trigger* condition for each
  branch are unchanged; only what each branch returns changes). This phase does **not** touch
  `file_writer.py` or `recalculate_skill_metrics.py` — those consume the new type starting in P1.
  **`api_writer.py` has no `enum` import today (verified — its current imports are `datetime as
  dt_module`, `json`, `logging`, `os`, `pandas`, `tag_library`, `long_term_horizon_resolver`,
  `skill_lead_aware_flag`)**; P0 must add `from enum import Enum` to the top-of-file import block. This
  is the one addition authorized beyond the `WriteOutcome` definition and the return statements — without
  it `class WriteOutcome(Enum)` cannot be defined.
- **Files:** `apps/postprocessing_forecasts/src/api_writer.py` (the `WriteOutcome` definition plus the
  return statements inside `_write_skill_metrics_to_api` (`:429-708`, final return at `:708`) and
  `_write_threshold_skill_metrics_to_api` (`:711-829`, outer `except` returns at `:829`) only —
  **re-verify both ranges by grep before editing (Contract 6); an earlier revision of this plan cited
  `:707` and `:830` as the closing lines, which are off by one in each direction and would leave the last
  return statement of each function unedited if trusted literally** — do not touch
  `_write_combined_forecast_to_api`, `_write_quarterly_ensemble_to_api`,
  `_write_seasonal_ensemble_to_api`, or any other function in this file; they are out of scope),
  `apps/postprocessing_forecasts/tests/test_api_integration.py`,
  `apps/postprocessing_forecasts/tests/test_quarterly_api_writer.py`.
- **Depends on:** none (first phase).
- **Agents:** 1, worktree-isolated. Prompt must include Hard Contracts 1-11 verbatim (§2), the full
  Contract 7 `WriteOutcome` definition and mapping verbatim, the fitness line from CLAUDE.md verbatim,
  and: *"Do NOT change any existing function signature's parameter list (return-type annotation changes
  ARE authorized, per Contract 8). Do NOT change any branch's trigger condition (when a `SKIPPED_*`
  vs. `FAILED` vs. `WROTE` path is entered) — only the value each branch returns. Add `from enum import
  Enum` to the top-of-file import block — verified absent today (current imports: `datetime as
  dt_module`, `json`, `logging`, `os`, `pandas`, `tag_library`, `long_term_horizon_resolver`,
  `skill_lead_aware_flag`); this is the one addition permitted beyond the `WriteOutcome` definition and
  the return statements. Do NOT touch
  `file_writer.py` or `recalculate_skill_metrics.py` in this phase; their conversion is split between
  P1-P3 and PP-054. Verified
  blast radius for this phase specifically: `_write_skill_metrics_to_api` has exactly 5 production
  callers, all in `file_writer.py` (`:341,431,616,668,703`); `_write_threshold_skill_metrics_to_api` has
  exactly 1 (`:626`) — 6 production call sites total across both functions. **4 are deferred to P1-P3**
  (`:341` pentad/decad P2, `:431` monthly P3, `:668` quarterly P1, `:703` seasonal P1); **the remaining 2
  (`:616`, the daily block's FDC call, and `:626`, its threshold call) are deferred to PP-054**, not to
  any phase of this plan — do
  not edit them here regardless of which later work converts them. Exactly 2 test files assert directly on either function's return value: 8
  assertions in `test_api_integration.py` (`result = _write_skill_metrics_to_api(...)` pairs at
  `:536-537,563-564,591-592,640-641,758-759,891-892,1074-1075,1260-1261`) and 3 in
  `test_quarterly_api_writer.py` (`:82-83,131-132,183+185`) — 11 total, update every one to assert the
  corresponding `WriteOutcome` member instead of a bare bool. `_write_threshold_skill_metrics_to_api`
  has **zero** existing direct-call tests in any file — you must add new ones for its outcome mapping,
  there is nothing to convert. Do not confuse `_write_skill_metrics_to_api`/`_write_threshold_skill_metrics_to_api`
  with the similarly-shaped `_write_quarterly_ensemble_to_api`/`_write_seasonal_ensemble_to_api` and
  `_write_combined_forecast_to_api` in the same two test files — those are different functions with
  their own `assert result is True/False` pattern and are out of scope for this phase. **Corrected claim
  (§1a): `_write_threshold_skill_metrics_to_api` is NOT exception-free** — `_get_postprocessing_client()`
  (`:741`) and `client.readiness_check()` (`:746`) sit outside its `try:` (which opens at `:753`), so a
  connection failure there raises straight through this function, uncaught. Your new tests must cover
  both: the mapped `FAILED` outcome from the protected region's own outer `except` (`:827-829`), AND
  confirm — by a test that mocks `_get_postprocessing_client()` or `readiness_check()` to raise — that
  this function still propagates that exception rather than swallowing it (this is a proof of current
  behavior for P4 to build on, not a behavior change: this phase does not add a try around the guard
  section)."*
- **Acceptance criteria:**
  - Every existing direct assertion on `_write_skill_metrics_to_api`'s return value (11 total, both
    files, exact locations above) is updated to assert the correct `WriteOutcome` member by exact
    identity (`assert result is api_writer.WriteOutcome.WROTE`, etc.) — not a truthy/falsy check, not a
    membership check against a set of "success-like" values.
  - New tests added for `_write_threshold_skill_metrics_to_api`'s outcome mapping, covering: disabled
    (`SAPPHIRE_API_ENABLED=false`) → `SKIPPED_BY_CONFIG`; `AttributeError` from
    `client.write_threshold_skill_metrics` → `SKIPPED_NOT_DEPLOYED`; a mocked 404/"Not Found" exception
    → `SKIPPED_NOT_DEPLOYED`; a mocked *other* exception from the HTTP call itself (e.g. a 500 or a
    network error) → `FAILED`; empty/`None` input → `SKIPPED_NO_RECORDS`; success → `WROTE`. Each is an
    exact-identity assertion. **Plus one test proving `_write_threshold_skill_metrics_to_api` still
    raises (does not return any `WriteOutcome`) when `_get_postprocessing_client()` or
    `client.readiness_check()` itself raises** — this documents the unprotected-guard-section behavior
    P4 must handle at its own layer; it is not a new outcome mapping, so there is nothing to assert about
    `WriteOutcome` here beyond confirming the exception is not accidentally caught.
  - Mutation check: swapping the `SKIPPED_NOT_DEPLOYED` return for `FAILED` (or vice versa) in either of
    the two Stage-2-pending branches must make at least one new test fail — record which one.
  - No test outside these two files references either function's return value directly (confirmed by
    grep as part of this phase, not assumed) — if the agent's grep finds one this plan's blast-radius
    claim missed, treat that as a finding to report, not silently patch.
  - `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` (full suite) — 0 fail, 0 unexpected skip. Every
    test in `apps/postprocessing_forecasts/tests/` that calls these two functions but does **not**
    assert on the return value (there are several — `test_seasonal_integration.py`,
    `test_write_guard.py`, `test_lead_aware_write_side_dedup.py`, `test_multi_org_isolation.py`,
    `test_lead_aware_writer_reader_round_trip.py`, `test_api_writer_dedup.py`,
    `test_pp038_writer_reader.py`, `test_integration_postprocessing.py`, `test_file_writer.py` — a
    return-type change is source-compatible for callers that ignore the return value) must still pass
    unmodified; if any of these fails, the return-type change had a side effect beyond what this phase
    scoped, and that is a signal to stop and report, not to patch around it.

### P1 — Quarterly + seasonal skill metrics (no-CSV-fallback horizons)

- **Goal:** `save_quarterly_skill_metrics` and `save_seasonal_skill_metrics` return a bool per Contract
  7's `save_*` mapping: `True` when their top-level `data is None or data.empty` guard fires
  (`:652-654`, `:687-689`), or when the captured `WriteOutcome` is anything other than `FAILED`; `False`
  only when the captured outcome is `FAILED` (readiness-check failure, or an exception that — per
  Contract 7's failure-mode rule — is not running under `SAPPHIRE_API_FAILURE_MODE=ignore`). Their call
  sites (`recalculate_skill_metrics.py:430-437` and `:513-520`) replace `if ret is None:` with a check
  on the new bool and append to the existing `errors` list on `False`, using the message-content rule in
  Contract 9 (reference the log line below, don't interpolate the bare bool).
- **Files:** `apps/postprocessing_forecasts/src/file_writer.py` (functions at `:640-672`, `:675-707`
  only), `apps/postprocessing_forecasts/recalculate_skill_metrics.py` (lines `:430-437`, `:513-520`
  only), `apps/postprocessing_forecasts/tests/test_file_writer.py`,
  `apps/postprocessing_forecasts/tests/test_recalc_workflow.py` (only the `_setup_mocks` entries for
  `save_quarterly_skill_metrics` and `save_seasonal_skill_metrics` — **re-verify by grep before editing
  (Contract 6): an earlier revision of this plan cited `:132,134`, which is off by three; the current
  lines are `:135,137`** — do not touch any other function's entry in that helper, see the shared-fixture
  rule above),
  `apps/postprocessing_forecasts/tests/test_wiring_integration.py` (only the
  `save_quarterly_skill_metrics`/`save_seasonal_skill_metrics` mock entries, verified at `:2287,2289` —
  same shared-fixture rule; do **not** touch `:973,1257,1942,2271`, those are P2's pentad/decad entries
  in the same file).
- **Depends on:** P0 (needs `api_writer.WriteOutcome` to exist).
- **Agents:** 1, worktree-isolated. Prompt must include Hard Contracts 1-11 verbatim (§2, including the
  filepath-hazard note even though it doesn't apply to this phase's functions, so the agent recognizes
  it if seen elsewhere), the fitness line from CLAUDE.md verbatim, and: *"Do NOT change any existing
  function signature's parameter list (return-type annotation changes ARE authorized, per Contract 8).
  Do NOT touch `save_skill_metrics`, `save_monthly_skill_metrics`, or `save_daily_skill_metrics` — those
  are separate phases. Do NOT touch the API-only guard (`api_writer.SAPPHIRE_API_AVAILABLE`) or the
  `data is None or data.empty` early-return shape itself — only its return value (`None` → `True`) and
  the API-write try/except changes: initialize the outcome to `api_writer.WriteOutcome.SKIPPED_BY_CONFIG`
  before the `if api_writer.SAPPHIRE_API_AVAILABLE:` gate (a closed gate is a config state, not a
  failure — do NOT initialize to a failure-mapped value, that is the exact mistake the prior revision of
  this plan made and it broke `SAPPHIRE_API_ENABLED=false`); inside the gate, capture
  `_write_skill_metrics_to_api`'s returned `WriteOutcome` (previously discarded) — this covers BOTH a
  directly-returned `FAILED` (e.g. readiness-check failure) and a caught exception (if the call raises,
  keep calling `fl._handle_api_write_error(e, ...)` exactly as today, THEN — in the same `except` block —
  explicitly assign `outcome = api_writer.WriteOutcome.FAILED`; do NOT leave the pre-gate
  `SKIPPED_BY_CONFIG` default standing after `_handle_api_write_error` returns, or the raised path reads
  as success — this is the exact seam three prior revisions of this plan left unfixed for exceptions.
  Under `warn`/`ignore` this does not re-raise, so this assignment is reached and the outcome is
  captured). Then apply ONE uniform post-processing step regardless of which mechanism produced it: if the
  captured outcome is `FAILED` and
  `fl._get_api_failure_mode() == \"ignore\"`, downgrade it to `SKIPPED_BY_CONFIG`; otherwise leave it as
  `FAILED`. Map to the function's bool return using the pinned predicate (Contract 10): the call site's
  check is `if ret is False:`, not `if not ret:`. Do NOT change `_write_skill_metrics_to_api` itself
  (`api_writer.py`) — that was P0, already landed."*
- **Acceptance criteria:**
  - RED-first: a new test asserting `save_quarterly_skill_metrics` (and separately
    `save_seasonal_skill_metrics`) returns `False` under a mocked API write **exception** in default
    `warn` mode fails against the pre-P0/P1 code (proves the defect existed), then passes after the fix.
  - A second RED-first test asserting the same function returns `False` when the writer returns
    `api_writer.WriteOutcome.FAILED` directly (readiness-check failure, no exception) — this is the
    headline case; a suite that only has the exception-based test above does not prove the
    non-raising failure path is fixed.
  - A third test: mocked exception under `SAPPHIRE_API_FAILURE_MODE=ignore` → function still returns
    `True` (not `False`) and nothing is appended to `errors` — proves the ignore-mode consistency fix
    (§2 Contract 7); this test did not exist under either prior revision of this plan and its absence is
    exactly what let the second revision's bug through undetected.
  - A fourth test: `api_writer.WriteOutcome.FAILED` returned **directly** (no exception, e.g. a mocked
    readiness-check failure) under `SAPPHIRE_API_FAILURE_MODE=ignore` → function still returns `True`,
    nothing appended to `errors` — this is the second, previously-missing half of the `ignore`-mode fix
    (§2 Contract 7's corrected mapping): the third test above only proves the raising mechanism is
    consistent under `ignore`; this one proves the non-raising mechanism is too. Both are required because
    a prior revision of this contract only consulted `fl._get_api_failure_mode()` inside the `except`
    branch.
  - A test proving the top-level empty-DataFrame guard now returns `True` (not `None`, not `False`) via
    exact identity assertion.
  - A test proving that when `data` is non-empty but `_write_skill_metrics_to_api`'s own internal
    filtering leaves zero records (construct via `SAPPHIRE_SKILL_LEAD_AWARE=true` with all rows'
    `horizon_value` set to `NaN` — see fixture pattern in `test_lead_aware_write_side_dedup.py`), the
    writer returns `SKIPPED_NO_RECORDS` and the save_* function still returns `True` (this outcome is a
    non-failure per Contract 7's mapping — a change from what would have been asserted under the
    superseded second-revision Contract 7).
  - A test proving `SAPPHIRE_API_ENABLED=false` (client available but writing disabled) → save_*
    function returns `True`, not `False` — this is the specific documented-configuration regression the
    second revision of this plan would have introduced; it must be locked down explicitly, by name, not
    left to be caught incidentally by another test.
  - `recalculate_skill_metrics.py` exits non-zero (`sys.exit(1)`, or assert on `errors` list contents
    per the existing `test_save_error_accumulation` pattern) only when the captured outcome is `FAILED`;
    the success path (real write succeeds) still returns `True` and the recalc still exits `0` with
    these two modes present. Confirm the test suite does **not** assert a CSV side effect for
    quarterly/seasonal (none should exist — API-only).
  - Mutation check: flipping the new `if outcome is WriteOutcome.FAILED:`-shaped condition, or changing
    the pre-gate initialization to a failure-mapped value instead of `SKIPPED_BY_CONFIG`, must make at
    least one new test fail — record which test catches each mutation.
  - `SAPPHIRE_API_FAILURE_MODE=fail` behavior for these two functions is unchanged (existing/extended
    test still shows the exception propagates in `fail` mode, unaffected by the new return value).
  - **Verify, do not edit:** `tests/test_seasonal_integration.py:542-574`
    (`test_save_seasonal_skill_metrics_calls_api`) calls `save_seasonal_skill_metrics` directly, patches
    `_write_skill_metrics_to_api` with a bare `MagicMock` (no `return_value` set), and only asserts the
    mock was called with the right `horizon_type`/`year` — it does not assert on `save_seasonal_skill_metrics`'s
    return value at all, so it passes unmodified under this phase's change. It is in **no** Files list
    above and has no leave-alone instruction in any earlier revision of this plan — an agent could
    plausibly "fix" it while touching `save_seasonal_skill_metrics`. Confirm it still passes unmodified;
    do not add assertions to it or move it into this phase's Files list.
  - `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` (full suite) — 0 fail, 0 unexpected skip.

### P2 — Pentad/decad skill metrics (`save_skill_metrics`)

- **Goal:** same bool-return pattern applied to `save_skill_metrics` (`file_writer.py:291-379`,
  API-write try block `:339-345`); call site `recalculate_skill_metrics.py:240-245` flips its
  `if ret is None:` check into the pinned `if ret is False:` predicate (Contract 10), using the same
  outcome-capture/ignore-mode call-site pattern as P1. **This function has no top-level empty-input guard
  (§1b)** — do not add one; a non-empty `data` argument whose internal filtering leaves zero records
  legitimately produces `WriteOutcome.SKIPPED_NO_RECORDS` → `True` here (not `False` — see §1b's
  correction: **this is no longer the `False`-producing case it was under the superseded second-revision
  Contract 7 (§5)** — under this revision only a genuine `FAILED` outcome does), and that empty-guard
  asymmetry with P1/P3 is accepted, documented, not a bug to fix in this phase. A **bare, columnless**
  `pd.DataFrame()` is a different, pre-existing case that raises `KeyError` before either write is
  attempted (§1b) — do not construct a Contract-7 test around a bare `DataFrame()`; use a non-empty frame
  with zero rows surviving the writer's internal filter instead. **Leave `SAPPHIRE_CONSISTENCY_CHECK`
  (`:348-377`) untouched (Contract 11)** — it is logged-only today and stays logged-only; do not fold it
  into this function's bool return.
- **Files:** `apps/postprocessing_forecasts/src/file_writer.py` (`:291-379` only),
  `apps/postprocessing_forecasts/recalculate_skill_metrics.py` (`:240-245` only),
  `apps/postprocessing_forecasts/tests/test_file_writer.py`,
  `apps/postprocessing_forecasts/tests/test_recalc_workflow.py` (only the `_setup_mocks` entry for
  `save_skill_metrics` — **re-verify by grep before editing (Contract 6): an earlier revision of this
  plan cited `:114`, which is off by one; the current line is `:115`**; plus two named tests that must be
  **inverted, not extended** — see prompt note below — `test_save_error_accumulation` (`:232`) and
  `test_save_success_path` (`:300`). **Corrected — these are NOT both scoped to `BOTH` mode**, contrary to
  an earlier revision of this bullet: `test_save_error_accumulation` sets `SAPPHIRE_PREDICTION_MODE:
  "PENTAD"` (`:234`, re-verified by grep — an earlier pass of this correction cited `:233`, off by one);
  only `test_save_success_path` sets `"BOTH"` (`:302`, re-verified — an earlier pass cited `:301`, also
  off by one)),
  `apps/postprocessing_forecasts/tests/test_wiring_integration.py` (only the `save_skill_metrics` mock
  entries, verified at `:973,1257,1942,2271` — do **not** touch `:2287,2289`, those are P1's
  quarterly/seasonal entries in the same file; `:1257` is also one of the tests requiring inversion, see
  below),
  `apps/postprocessing_forecasts/tests/test_scoped_skill_recalc.py` (only the `save_skill_metrics`
  entries in its own mock setup, e.g. `:216` — same shared-fixture caution as the other files; see §1b's
  corrected description of what this file's `:210-214` mock actually does before touching it).
- **Depends on:** P1 (shared-file execution order per §3 — not a logical dependency).
- **Agents:** 1, worktree-isolated. Prompt: Hard Contracts 1-11, fitness line, plus: *"The CSV write at
  `:331-336` is unconditional and already raises on its own failure — do not touch that block at all,
  only the API-write try/except at `:339-345` and the function's return statement. Confirm by re-diff
  that the CSV write's raise-on-failure behavior is byte-identical before/after. Do NOT add a top-level
  `data.empty` guard to this function — see §1b of the plan; that is out of scope here. Do NOT touch
  `SAPPHIRE_CONSISTENCY_CHECK` (`:348-377`) — it stays logged-only, per Contract 11; do not fold its
  failure into this function's bool return even though this plan's general thesis is about surfacing
  swallowed failures. Outcome capture and the `ignore`-mode downgrade use the same single
  post-processing step as P1 (Contract 7): capture the `WriteOutcome` whether it was returned directly or
  assigned in the `except` block after catching a raised exception via `fl._handle_api_write_error(e,
  ...)` — that `except` block must explicitly set `outcome = api_writer.WriteOutcome.FAILED` after the
  call to `_handle_api_write_error` (do not leave the pre-gate `SKIPPED_BY_CONFIG` default standing after
  it returns under `warn`/`ignore` — the raised path must not read as success). If the result is
  `FAILED` and `fl._get_api_failure_mode() == \"ignore\"`, downgrade to `SKIPPED_BY_CONFIG`. Call-site
  predicate is `if ret is False:` (Contract 10), not `if not ret:`. INVERT, do not extend, these two named
  tests: `test_recalc_workflow.py:232` (`test_save_error_accumulation`) currently sets
  `save_skill_metrics.return_value = 'Error: write failed'` and asserts `exc_info.value.code == 1` — under
  the new contract a non-empty truthy string is not a meaningful mock for this function's real return type
  (`bool`); replace it with `return_value=False` and keep the exit-1 assertion, updating the docstring.
  `test_recalc_workflow.py:300` (`test_save_success_path`, docstring 'All saves return None → exit 0') and
  the equivalent block in `test_wiring_integration.py:1257` invert the same way: the mock becomes
  `return_value=True`/`return_value=False` per case, not `None`/a truthy string — read both test bodies
  before editing, they pin opposite things (success vs. failure) and must not be conflated."*
- **Acceptance criteria:** same test categories as P1 (exception-based RED-first, `FAILED`-without-
  raising RED-first, `ignore`-mode RED-first covering BOTH the raised-exception mechanism and the
  `FAILED`-returned-directly mechanism, mutation check), plus a `SAPPHIRE_API_FAILURE_MODE=fail` test
  (exception still propagates in `fail` mode, unaffected by the new return value — P1 has this
  criterion, this phase was missing it), but:
  - CSV-still-written test: the CSV file **is still written** when the API write fails under `warn` mode
    (the unconditional-and-raising CSV path at `:331-336` must be exercised and pass even while the API
    branch's outcome is `FAILED`).
  - No top-level-empty-guard test is required (there is none to test — see Goal above); if the
    implementing agent finds it useful to add a test documenting the
    non-empty-input-zero-surviving-records → `True` behavior for symmetry with P1/P3's coverage, that is
    acceptable but not required.
  - The two inverted tests (`test_save_error_accumulation`, `test_save_success_path`) and the
    `test_wiring_integration.py:1257` equivalent pass with the new mock shape and their docstrings no
    longer say "returns `None`" anywhere.
  - **Verify, do not edit:** `tests/test_workflow_integration.py` exercises
    `recalculate_skill_metrics()` (pentad/decad short-term data, per its own docstring and its use of
    `read_observed_and_modelled_data_pentade/decade`) against **real** `file_writer`/`api_writer` code
    (CSV in `tmp_path`, no mocks on the save layer) and already sets `SAPPHIRE_API_ENABLED: "false"` in
    its env fixture (`:109`). Verified: under the new contract this resolves to
    `WriteOutcome.SKIPPED_BY_CONFIG` → non-failure, so these tests need **no changes** and must still
    assert exit 0 after this phase — if any of them go red, that is a signal the outcome mapping or the
    pre-gate default is wrong, not that this file needs editing to match.
  - Re-run `SAPPHIRE_TEST_ENV=True bash run_tests.sh` (full suite) — 0 fail, 0 unexpected skip, including
    the P0+P1 tests (must still pass — proves no regression from sequential same-file/same-fixture
    edits).

### P3 — Monthly skill metrics (`save_monthly_skill_metrics`)

- **Goal:** same pattern applied to `save_monthly_skill_metrics` (`file_writer.py:382-463`, API-write
  try block `:429-433`); call site `recalculate_skill_metrics.py:361-366` flips its check, using the
  same outcome-capture/ignore-mode call-site pattern as P1/P2. The top-level empty guard (`:400-402`)
  changes from `return None` to `return True` per Contract 7.
- **Files:** `apps/postprocessing_forecasts/src/file_writer.py` (`:382-463` only),
  `apps/postprocessing_forecasts/recalculate_skill_metrics.py` (`:361-366` only),
  `apps/postprocessing_forecasts/tests/test_file_writer.py`,
  `apps/postprocessing_forecasts/tests/test_recalc_workflow.py` (only the `_setup_mocks` entry for
  `save_monthly_skill_metrics` — **re-verify by grep before editing (Contract 6): an earlier revision of
  this plan cited `:116`, which is off by one; the current line is `:117`**; plus
  `test_monthly_save_error_causes_exit_1` (`:431`), which must be **inverted, not extended** — see prompt
  note below),
  `apps/postprocessing_forecasts/tests/test_monthly_workflow_integration.py` (verify-only, see
  acceptance criteria — do not edit unless the verification below finds a real problem).
- **Depends on:** P2.
- **Agents:** 1, worktree-isolated. Prompt: Hard Contracts 1-11, fitness line, plus: *"The CSV write is
  conditional on `ieasyforecast_intermediate_data_path` and `ieasyforecast_monthly_skill_metrics_file`
  both being set (`:413-427`); when either is unset it must keep warning-and-skipping, not raising —
  this is a distinct behavior from pentad/decad's unconditional CSV path in P2, do not make it
  unconditional. Only the API-write try/except at `:429-433`, the top-level guard's return value, and
  the function's final return statement change, using the same
  initialize-to-`SKIPPED_BY_CONFIG`-before-the-gate / capture-the-`WriteOutcome`-either-way (returned
  directly, or explicitly assigned `outcome = api_writer.WriteOutcome.FAILED` in the `except` block after
  calling `fl._handle_api_write_error(e, ...)` — do not leave the pre-gate `SKIPPED_BY_CONFIG` default
  standing after that call returns under `warn`/`ignore`, see P1's prompt for the exact code shape) /
  single-`ignore`-mode-downgrade-step pattern as P1 and P2 —
  do NOT re-derive this shape independently, copy the P1/P2 pattern. Call-site predicate is `if ret is
  False:` (Contract 10). Do NOT fold `SAPPHIRE_CONSISTENCY_CHECK` (`:435-461`) into this function's bool
  return (Contract 11) — it stays logged-only, same as today, even though this plan's general thesis is
  about surfacing swallowed failures; that check's own scope is undefined by this plan. KNOWN HAZARD, do
  not fix: `filepath` (used at `:442` inside the `SAPPHIRE_CONSISTENCY_CHECK` branch) is only assigned
  inside the `if csv_dir and csv_file:` block (`:418-419`) — if your CSV-unconfigured test runs with
  `SAPPHIRE_CONSISTENCY_CHECK=true` it will hit a pre-existing `UnboundLocalError` unrelated to this plan.
  Keep `SAPPHIRE_CONSISTENCY_CHECK` unset or `false` in that test. This bug is being filed separately —
  do not fix it here. INVERT, do not extend, `test_recalc_workflow.py:431`
  (`test_monthly_save_error_causes_exit_1`) — same shape as P2's `test_save_error_accumulation`: replace
  its truthy-string mock with `return_value=False`, keep the exit-1 assertion, update the docstring."*
- **Acceptance criteria:** same exception-based, `FAILED`-without-raising, and `ignore`-mode RED-first
  (covering BOTH the raised-exception mechanism and the `FAILED`-returned-directly mechanism) tests as
  P1/P2 (mutation check included), plus a `SAPPHIRE_API_FAILURE_MODE=fail` test (exception still
  propagates in `fail` mode, unaffected by the new return value — P1 has this criterion, this phase was
  missing it), plus:
  - Top-level empty-guard test updated: `test_file_writer.py:317`
    (`test_empty_dataframe_guarded_no_write`) must be changed from `assert result is None` to `assert
    result is True`, and its docstring updated to match ("returns `True`" not "returns `None`"). Name
    this test explicitly in the diff — do not leave it to fail unannounced.
  - Non-empty-input-but-`SKIPPED_NO_RECORDS` test (Contract 7), same construction pattern as P1
    (`SAPPHIRE_SKILL_LEAD_AWARE=true`, all `horizon_value` NaN) — asserts the function still returns
    `True` (this outcome is a non-failure, not the `False` a superseded reading of Contract 7 might
    suggest).
  - CSV-configured case: CSV written, API outcome is `FAILED` (either mechanism) → CSV still present,
    function still returns `False`.
  - CSV-unconfigured case: warn-and-skip preserved (unaffected by the API outcome), and the test must
    NOT enable `SAPPHIRE_CONSISTENCY_CHECK` (see hazard note above).
  - `test_recalc_workflow.py:431` passes with the inverted mock shape and no longer implies truthy means
    failure.
  - **Verify, do not edit:** `tests/test_monthly_workflow_integration.py` exercises
    `recalculate_skill_metrics()` in `MONTHLY` mode against **real** `file_writer`/`api_writer` code and
    already sets `SAPPHIRE_API_ENABLED: "false"` in its env fixture (`:313`). Verified: under the new
    contract this resolves to `WriteOutcome.SKIPPED_BY_CONFIG` → non-failure, so these tests need **no
    changes** and must still assert exit 0 after this phase — same reasoning and same "if it goes red,
    that's a mapping bug, not a missing edit" rule as P2's `test_workflow_integration.py` check.
  - `SAPPHIRE_TEST_ENV=True bash run_tests.sh` (full suite) — 0 fail, 0 unexpected skip, P0+P1+P2 tests
    still passing.

### P4 — SPLIT OUT: Daily skill metrics block (`save_daily_skill_metrics`)

**Not part of this plan's executable phase sequence.** P4 (daily FDC + threshold skill-metrics writes)
was found not executable as specified — its `Files` list excluded `test_recalc_workflow.py` while four
of its acceptance criteria required driving `recalculate_skill_metrics()` end-to-end through exactly that
file's harness; it covers code with zero existing test coverage and no scoped DAILY-mode fixture chain;
its new branch has no Contract-9 message template; and two of its acceptance criteria ("both writes
attempted independently" vs. a required `fail`-mode propagation test) were mutually unsatisfiable as
worded. Re-scoping it inline kept reproducing these contradictions across revisions, so it is filed as its
own draft instead: **PP-054**,
[`doc/plans/issues/high_prio_gi_draft_pp_daily_threshold_skill_silent_api_write_failure.md`](../issues/high_prio_gi_draft_pp_daily_threshold_skill_silent_api_write_failure.md).
That draft cross-references this plan's Contract 7 (`WriteOutcome`, settled, not to be re-derived) and
carries forward the design work already done here — the FDC/threshold fixture shapes, the
`SKIPPED_NO_RECORDS`-unreachable-for-FDC finding (§3), and the "threshold branch is not exception-free"
finding (§1a) — so a future implementer does not have to re-derive them.

This placeholder is kept under the `P4` heading, not deleted or renumbered, so every cross-reference to
"P4" elsewhere in this plan (§1, §3's daily-ordering rationale, P0's blast-radius note, etc.) still
resolves to a real section. P0-P3 and P5-P6 proceed unaffected; see P5 below for the updated
dependency scope.

### P5 — Cross-phase integration verification (gate, no new production code)

**Scope note (post-split):** P4 (daily) is no longer part of this plan's executable sequence — see the
placeholder above and PP-054. Everywhere below that previously said "the four independent fixes" /
"five in-scope functions" now means the **three** independent fixes (P1: quarterly+seasonal, P2:
pentad/decad, P3: monthly) across the **four** in-scope `save_*` functions P0-P3 actually convert
(`save_quarterly_skill_metrics`, `save_seasonal_skill_metrics`, `save_skill_metrics`,
`save_monthly_skill_metrics`). `save_daily_skill_metrics` is out of this phase's scope entirely until
PP-054 lands and its own equivalent verification (if any) is scoped there.

- **Goal:** confirm the three independent fixes compose correctly — a single recalc run with **multiple**
  simultaneous genuine failures (`WriteOutcome.FAILED`, e.g. quarterly *and* monthly both fail, pentad/decad
  succeeds) still (i) attempts every mode (Hard Contract 1), (ii) accumulates **all** failure messages into
  `errors`, not just the first, and (iii) exits non-zero exactly once at the end (`:580-587`), not per-mode.
  This is the check that P1-P3 didn't each pass in isolation while silently regressing the shared
  `errors`-list/exit-code mechanism they all write into. Also confirm, in the same run, that a mode with a
  **non-`FAILED` outcome** (legitimately empty input, `SKIPPED_BY_CONFIG`, `SKIPPED_NO_RECORDS`) among the
  failing modes does NOT contribute to `errors` and does NOT flip a passing run's exit code — this is the
  specific integration-level check for the empty-vs-failure distinction, since P1-P3 each verify it only in
  isolation per function. Also confirm one run under `SAPPHIRE_API_FAILURE_MODE=ignore` with a mix of real
  exceptions across two different modes exits `0` (both convert to `SKIPPED_BY_CONFIG`, per Contract 7) —
  the specific cross-mode check for the `ignore`-mode fix, since P1-P3 each verify it only for their own
  function. **Also close out Contract 10's transitional tolerance:** with P0-P3 landed, confirm
  behaviorally that none of the **four** in-scope `save_*` functions can still return `None` — call each
  with an input shape that would have hit a `None`-producing path pre-fix (e.g. the top-level empty-input
  guard, or a writer outcome of each `WriteOutcome` member) and assert the return is `True`/`False` by
  exact identity, never `None`. This is the check that lets the `if ret is False:` predicate (pinned in
  Contract 10) stop being a transitional safety net for these four functions and confirms it isn't
  silently masking an unconverted one; `save_daily_skill_metrics` remains outside this check's scope until
  PP-054 lands (it still returns bare `None` today, and that is expected until then, not a regression).
- **Files:** `apps/postprocessing_forecasts/tests/test_recalc_workflow.py` only (test-only phase — if
  this phase finds a defect, it is fixed by re-delegating a targeted patch to the specific P0-P3 phase
  whose diff caused it, not by writing new production code here).
- **Depends on:** P0, P1, P2, P3.
- **Agents:** 1 (test-writing only, no worktree isolation required — this phase does not touch
  production code).
- **Acceptance criteria:** one new integration test with ≥2 of the four in-scope functions mocked to
  fail simultaneously (`ALL` prediction mode or equivalent multi-mode setup, excluding the daily block —
  it is not converted by this plan), and at least one further in-scope function mocked to receive a
  non-`FAILED` outcome in the same run, proves (i)-(iii) above plus
  the non-failure-mode-doesn't-count check, with exact-count assertions on `len(errors)` and which
  messages are present (per the testing workflow's "exact counts, not vague checks" rule), not just
  "exit code is 1". A second new test covers the cross-mode `ignore`-mode check described in the Goal. A
  third check (not necessarily a single test — a review pass across the four functions is acceptable)
  confirms the Contract 10 "no lingering `None`" behavioral check described in the Goal, for all four
  in-scope `save_*` functions. Full suite green: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` —
  0 fail, 0 unexpected skip. This is the last **mandatory** phase; P6 is optional and, as revised below,
  does not ship production code either.

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
independent signal alongside the corrected P0-P5 exit code. It would do nothing for an operator who
invokes `recalculate_skill_metrics.py` directly, bypassing the wrapper entirely — that population's
only defense is the corrected exit code from P0-P5, unchanged by this phase.**

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

- **A prior revision of this plan prescribed initializing the result variable to `False` before the
  `api_writer.SAPPHIRE_API_AVAILABLE` gate, and that prescription was wrong — recorded here so the
  reasoning is not silently lost and nobody reintroduces it.** `SAPPHIRE_API_AVAILABLE` false (client not
  installed) and `SAPPHIRE_API_ENABLED=false` are both documented, supported deployment configurations
  (`doc/configuration.md:159`; `doc/plans/sapphire_api_integration_plan.md:283,321` — local dev and the
  documented emergency-rollback path). A pre-gate `False` default would have made every recalc in either
  configuration report failure and `sys.exit(1)` even when the CSV write succeeded and nothing was
  actually wrong — worse than the silent-success bug being fixed, because it would fire on every routine
  run of a supported mode rather than only on genuine, intermittent failures. This revision's Contract 7
  fixes the mechanism at the source instead: `_write_skill_metrics_to_api` /
  `_write_threshold_skill_metrics_to_api` now return an explicit `WriteOutcome` that distinguishes
  "gate closed / disabled" (`SKIPPED_BY_CONFIG`, non-failure) from "genuinely failed"
  (`FAILED`, the only failure-mapped member) at the source, so the `save_*` layer never has to guess an
  ambiguous default.
- **This fix does not close PP-047.** PP-047 is a different layer: the API returning HTTP 200 having
  persisted zero rows (a server/DB-side silent failure). PP-051's fix makes the client correctly report
  *client-observed* signals (network errors, non-2xx responses, timeouts, readiness-check failures, and
  now also the previously-discarded config/no-records/not-deployed outcomes, per the `WriteOutcome`
  contract in §1a/§2) — it has no visibility into a "200 OK, nothing written" response, because from the
  client's perspective that is indistinguishable from a genuine success (`WROTE`). The two defects
  compose: even after this fix ships, a PP-047-class failure would still produce a `True`/success return
  here and a clean exit. Closing PP-047 requires either a service-side fix (colleague-owned,
  `sapphire/services/`) or a client-side read-back verification (which is exactly what
  `SAPPHIRE_CONSISTENCY_CHECK` attempts for CSV, at `file_writer.py:348-377` (corrected from an earlier
  revision's `:348-378` — verified: the block runs `:348-377`, `:378` is blank, `:379` is `return None`,
  now agreeing with Contract 11's citation) / `:435-461` — but per the
  draft's evidence, that check compares against the CSV it just wrote, never the API, and is unwired for
  quarterly/seasonal entirely).
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
  identical discarded-return-value shape found here — a separate draft is being filed for it. That
  precedent's own initialization (`api_ok = True` before the gate) happens to already be *closer* to
  this revision's correct default than the second revision of this plan was — but it still discards the
  writer's return value entirely, which this plan's Contract 7 does not. Do not treat "matches LR-007"
  as sufficient justification for a diff in any phase above — matching Contract 7 is what matters.
- **`save_skill_metrics` (pentad/decad) has no top-level empty-input guard, unlike its four siblings
  (§1b), and this plan does not add one.** Under this revision's Contract 7, that asymmetry is smaller
  than the earlier revision implied: a non-empty `data` argument whose internal filtering leaves zero
  records now maps to `SKIPPED_NO_RECORDS` → `True` (non-failure), the same as its siblings — so the
  earlier concern ("this function reads as failure where its siblings read as success") no longer
  applies for that case. What remains true and undocumented-by-guard: a **bare, columnless**
  `pd.DataFrame()` still raises `KeyError` before either write is attempted (§1b) — a
  pre-existing crash-on-malformed-input behavior, not a Contract 7 outcome, and out of this plan's scope
  to fix. Whether pentad/decad recalcs can legitimately run on a genuinely empty (not malformed) input,
  and whether a top-level guard should be added to match its siblings for symmetry/clarity, is an open
  question this plan does not answer.
- **Contract 7 (this revision) treats "non-empty input, zero records survive the writer's internal
  filtering" as `SKIPPED_NO_RECORDS` — a non-failure — reversing the *previous* revision's choice to
  treat it as a reportable failure.** The previous revision argued "every row failed validity" was a
  data-quality signal worth surfacing in `errors`; this revision's owner-chosen design instead treats
  "nothing to write" uniformly as non-failure regardless of *why* nothing survived (top-level empty
  input, or writer-internal filtering) — consistent with the stated rule that "no attempt/nothing sent
  must never be reported as failure." Practical consequence: a condition where every row in a batch was
  dropped by the lead-aware `horizon_value` filter, or was missing its horizon-in-year value, is **no
  longer visible in the recalc's exit code or `errors` list** — it remains visible one level down, in
  `api_writer.py`'s own log line (`:707` / `:794`), but an operator who only checks exit code / `errors`
  will not see it. This is the deliberate, owner-approved trade-off of this revision, not an oversight —
  flagged here explicitly so it is not later mistaken for a missed case, and so that if operational
  experience shows this data-quality signal needs to surface at the exit-code layer after all, that is a
  scoped follow-up, not a silent regression to chase.
- **Partial row loss inside a successful (`WROTE`) write is still invisible to the exit code.** `WROTE`
  means *at least one* record was accepted by the API — not that every input row survived. The writer
  drops NaN-horizon rows (`api_writer.py:507`, warning at `:515-520`), NULL-lead rows under
  `SAPPHIRE_SKILL_LEAD_AWARE=true` (`:620`, warning at `:614-619`), and upsert-key duplicates (`:639`,
  warning at `:641-646`) — and still returns `WROTE` (→ recalc exit `0`) as long as at least one row made
  it through. This plan's fix makes the *all-or-nothing* signal (attempted-and-accepted vs.
  genuinely-failed) truthful; it does not make the signal row-complete. An operator reading a clean exit
  code should not conclude every row landed — the per-write warning logs remain the only place that
  distinction is visible, exactly as they are today.
- **Option C, now demoted to draft-only (P6), leaves ad-hoc invocations of `recalculate_skill_metrics.py`
  (outside the bimonthly wrapper) dependent solely on the corrected exit code from P0-P3 and P5 (P4/daily
  excluded from this plan — see the next bullet), with no
  independent DB check** — and, per P6's corrected rationale, even a fully-built Option C would not
  change this for the ad-hoc population; it only adds a second signal for wrapper-invoked runs. That is
  a real improvement over today (currently even the exit code is wrong), but correctness for ad-hoc runs
  fully depends on the Python process itself accurately detecting and reporting its own failure — there
  is no second, independent observer for that path, with or without P6.
- **`SAPPHIRE_API_FAILURE_MODE=fail` is not touched, so an operator who has already set `fail` (if any
  deployment does) sees no behavior change** — the fix specifically targets the `warn` (and now also
  `ignore`, see Contract 7) paths, which is where the silent-success gap lives. `ignore` mode's own
  intent (make write outcomes invisible to the exit code) is preserved by design in this revision, not
  merely left unbroken by accident — see Contract 7's ignore-mode rule.
- **The daily block's silent-success defect is NOT fixed by this plan — it is split out to PP-054, and
  remains open until that draft is implemented.** `save_daily_skill_metrics` still returns bare `None`
  on both the success and the swallowed-failure path after P0-P3 and P5 land, exactly as it does today;
  `recalculate_skill_metrics.py`'s daily call site (`:555-566`) still cannot distinguish "wrote" from
  "silently failed under `warn` mode." Both writes it makes are API-only with no CSV fallback (like
  quarterly/seasonal), so a swallowed failure here is total data loss for that recalc's daily output — the
  same severity class this plan's P1 prioritized fixing first for quarterly/seasonal, just not yet fixed
  for daily. **`save_daily_skill_metrics` and `_write_threshold_skill_metrics_to_api` remain the only two
  functions in this defect family with zero test coverage** (verified — no test in
  `apps/postprocessing_forecasts/tests/` references either by name), unlike every other function this
  plan (P0-P3) converts, all of which had existing tests to extend or invert. An operator relying on a
  clean `recalculate_skill_metrics.py` exit code should not conclude the daily block's writes landed —
  this gap persists until PP-054 ships. (The design note this bullet replaces — that a combined bool for
  FDC+threshold can only report "at least one failed," not which — is still correct and now lives in
  PP-054's scope, not this plan's.)

---

## 6. Follow-up filing (do this after P5, before or independent of P6)

Per §5's "four out-of-scope forecast-data `save_*` functions" bullet, file
`doc/plans/issues/mid_prio_gi_draft_pp_forecast_data_silent_api_write_failure.md`
covering `save_forecast_data`, `save_monthly_forecast_data`, `save_quarterly_forecast_data`,
`save_seasonal_forecast_data` (`file_writer.py`, callers to be mapped fresh — do not assume they match
the skill-metrics callers). Add an entry to `doc/plans/module_issues.md`. This is documentation/planning
work, not a code phase, and is explicitly out of scope for the P0-P6 dependency graph below.

A second, independent draft is being filed concurrently (by another agent, outside this plan's scope)
for the LR-007 discarded-bool defect at `forecast_library.py:3973-3979` referenced in §5. This plan does
not name or index that file — do not invent a filename for it here; cross-reference it once it exists.

---

## 7. Dependency graph

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "status": "split_out", "note": "see doc/plans/issues/high_prio_gi_draft_pp_daily_threshold_skill_silent_api_write_failure.md (PP-054); not part of this plan's executable sequence, not a dependency of P5" },
    "P5": { "depends_on": ["P0", "P1", "P2", "P3"], "parallel_agents": 1 },
    "P6": { "depends_on": ["P5"], "parallel_agents": 1 }
  }
}
```

P0 is new in this revision — it lands the `WriteOutcome` type in `api_writer.py` that every other phase
consumes (§3), and is a genuine data dependency, not a shared-file ordering convenience like P1→P2→P3.
**P4 is retained in the graph as a non-executable node, not deleted**, so cross-references to "P4"
elsewhere in this plan keep resolving to a real entry; it carries no `depends_on`/`parallel_agents` because
it is not scheduled by this plan at all — see its placeholder phase entry in §4 and PP-054. P5 now depends
on P0-P3 only. P6 is optional and owner-gated (see §4 P6 preamble) — the graph lists it for completeness
but, per its revised scope, it produces a draft document, not a code change, and should not be started
without separate explicit sign-off, distinct from approval of P0-P3 and P5.
