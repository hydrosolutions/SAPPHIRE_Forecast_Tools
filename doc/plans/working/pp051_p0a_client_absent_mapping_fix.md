# PP-051 P0a — a missing client is a failed write, not a config skip

**Status:** Draft plan, reviewed (out-of-loop `codex exec`), not started. Fixes a defect in P0
(`532bf00f`, merged via PR #433).
**Revised** after review: the code change alone has **no operational effect** — see §3. The
production fix is the pre-gate default in the `save_*` layer, which lands in P1-P3 and PP-054.

---

## 1. The defect

P0 maps "client absent" to `WriteOutcome.SKIPPED_BY_CONFIG` — a non-failure. That is wrong:

- `SAPPHIRE_API_AVAILABLE` is set **only** by whether the import succeeded
  (`api_writer.py:88-96`). It is not an operator setting.
- `sapphire-api-client` is a **required dependency** — a hard git pin at
  `apps/postprocessing_forecasts/pyproject.toml:27`, not an optional extra.
- The documented CSV-only path is `SAPPHIRE_API_ENABLED=false` (`doc/configuration.md:159`;
  `doc/plans/sapphire_api_integration_plan.md:283`, `:321`). A **missing package** is documented
  nowhere as supported.

So a broken installation is reported as a deliberate choice. Quarterly, seasonal and daily are
API-only: they write nothing and the recalc exits 0 — PP-051's exact failure mode, reintroduced by
its own fix, in the case that most resembles a deployment accident.

## 2. The two conditions are separable at the gate

| Condition | Gate at each `save_*` | Writer called? | Correct meaning |
|---|---|---|---|
| `SAPPHIRE_API_AVAILABLE = False` (import failed) | **closed** | no | **failure** — required dependency missing |
| `SAPPHIRE_API_ENABLED=false` (operator choice) | open | yes → `SKIPPED_BY_CONFIG` | benign |

Every `save_*` gates on `SAPPHIRE_API_AVAILABLE` alone (`file_writer.py:339/341, 429/431, 614/616,
624/626, 666/668, 701/703`); `SAPPHIRE_API_ENABLED` is checked **inside** the writers
(`api_writer.py:491`, `:768`), after the gate. The two never share a path, so they can carry
different outcomes unambiguously.

**Correction to the parent plan's history:** round 2's review rejected the `False`-before-the-gate
initialization on the grounds that it would break *both* client-absent *and*
`SAPPHIRE_API_ENABLED=false`. Only the first is affected by the gate; the second always reaches the
writer. `WriteOutcome` remains the right answer — it also fixes the no-records and Stage-2 cases —
but the parent plan's §5 records the wrong reason.

## 3. The code change is defensive only — this is the review's key correction

The missing-client branches (`api_writer.py:486-488` skill, `:762-766` threshold) are **unreachable
from every production call site**, because all six are already behind the `SAPPHIRE_API_AVAILABLE`
gate. There are no other production callers. Changing those returns therefore has **no operational
effect**; it matters for direct callers and tests, and it stops the wrong semantics being copied
forward.

**The production fix is D2: the pre-gate default in each `save_*`.** When the gate is closed, the
`save_*` must resolve to failure, because the only thing that closes it is a missing required
dependency. That lands in P1-P3 (pentad/decad, monthly, quarterly, seasonal) and in **PP-054** for
the daily path.

**Residual gap to state plainly:** DAILY and ALL runs can still exit 0 after writing no daily
metrics until PP-054 implements the same gate fix. P0a does not close that.

## 4. Decisions (reviewer-endorsed)

- **D1 — map missing-client to `FAILED`.** No new member. Verified safe for every current consumer:
  all six production callers discard the value or cannot reach the branch, non-asserting tests
  ignore it, and no existing assertion covers this outcome.
- **D2 — initialize the `save_*` outcome to `FAILED` before every `SAPPHIRE_API_AVAILABLE` gate**,
  keeping `SAPPHIRE_API_ENABLED=false` mapped to `SKIPPED_BY_CONFIG` inside the writers.
- **D3 — `ignore` mode does not downgrade the outcome.** `forecast_library.py:110-116` makes
  `ignore` silent relative to `warn`; it suppresses logging, not failure accounting. The shipped LR
  wrappers already return failure for caught errors under `warn`/`ignore`. Making failures invisible
  to the exit code would be a separate feature, not a side effect of a logging mode.

## 5. Changes

**Code** — `apps/postprocessing_forecasts/src/api_writer.py`:
1. `_write_skill_metrics_to_api` missing-client branch (`:486-488`) → `WriteOutcome.FAILED`.
2. `_write_threshold_skill_metrics_to_api` missing-client branch (`:762-766`) → `WriteOutcome.FAILED`.
3. Documentation that will otherwise contradict the code: the `SKIPPED_BY_CONFIG` member comment
   (`:79`), the `WriteOutcome` class docstring (`:72-75`, calls client absence a benign
   configuration state), and both writers' Returns docstrings (`:471-476`, `:747-756`).

Do **not** touch `_write_combined_forecast_to_api` (`:190`) or any other writer — they still return
`bool` and are out of scope. Note `api_writer.py:239` belongs to that function, not to a skill
writer.

**Tests** — the client-absent assertions the first draft assumed **do not exist**. Existing tests at
`test_api_integration.py:522-539` and `:1028-1032` cover `SAPPHIRE_API_ENABLED=false`, not
`SAPPHIRE_API_AVAILABLE=False`. So:
- **Add** new missing-client tests for both writers asserting `FAILED`. Do **not** invert the
  disabled-configuration tests — that would turn a benign state into a failure, the exact trap.
- Keep the disabled tests asserting `SKIPPED_BY_CONFIG`, and pin `SAPPHIRE_API_AVAILABLE=True` in
  them so the two conditions are distinguished rather than coincidentally equal.
- One test asserting the two conditions yield **different** outcomes, so nothing can re-conflate
  them silently.

**Parent plan** — `pp051_recalc_write_failure_plan.md`. Contract 7 and §5 are **not sufficient**:
the rejected mappings are hardcoded in phase-local prompts and criteria (`:689-704` P1, `:800-818`
P2, `:866-884` P3, ignore-mode expectations `:714-724`, a mutation assertion `:742-744`, P5
integration `:950-973`) and §1's narrative (`:143-166`) still calls client absence benign. Every one
must be updated, or a phase will follow its local instructions and recreate the defect.

**PP-054** — must carry the same D2 gate fix for the daily path.

## 6. Out of scope — file separately

`api_writer.py:491` / `:768` parse `SAPPHIRE_API_ENABLED` as `os.getenv(...).lower() == "true"`, so
`"1"`, `"yes"` or a stray space read as deliberate disablement and yield a benign skip. Same
silent-success class, different trigger.

## 7. Acceptance criteria

- RED first: the new missing-client tests fail against current trunk, using exact identity
  (`assert result is WriteOutcome.FAILED`) — truthiness is vacuous here.
- Both writers return `FAILED` when the client is absent and `SKIPPED_BY_CONFIG` when
  `SAPPHIRE_API_ENABLED=false`, asserted as distinct outcomes.
- Existing disabled-configuration tests still pass **unmodified in intent** (still `SKIPPED_BY_CONFIG`).
- No other branch mapping, trigger condition, validation, filter or HTTP call changes.
- `file_writer.py` and `recalculate_skill_metrics.py` untouched — D2 belongs to P1-P3/PP-054.
- Mutation check: swap the two mappings, confirm the distinctness test goes red, restore.
- Full unscoped `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero
  unexpected skips. Trust per-module pass counts, not the banner.

## 8. Dependency graph

```json
{
  "phases": {
    "P0a": { "depends_on": [], "parallel_agents": 1 },
    "P0b": { "depends_on": ["P0a"], "parallel_agents": 1, "note": "parent-plan + PP-054 edits per §5; must precede P1" }
  }
}
```
