## `validate_pipeline --module machine_learning` matches zero checks and reports PASS on no evidence (INFRA-020)

**Status**: Draft (2026-07-23) — **diagnosis confirmed, proposed fix blocked on two owner decisions
(C3, C4) after out-of-loop review 2026-08-18**
**Module**: `apps/validate_pipeline` (+ `apps/run_locally.sh` summary reporting)
**Priority**: **High** (silent false assurance on the module with the most silent-write history)
**Labels**: `infra`, `validation`, `false-pass`, `machine_learning`, `observability`
**Discovered**: 2026-07-23, local pipeline health review (taj, `maxat_sapphire_2` @ `16fb9a9b`).
**Independently confirmed**: yes — out-of-loop `codex exec` review, read-only, fresh context.
**Related**:
- **ML-015** — operational ML NaN never remediated. INFRA-020 is *why nobody notices*; the
  2026-07-23 tjhm recurrence recorded in ML-015 § Field evidence (4) was reported `PASS`.
- **ML-002** — hindcast subprocess root cause (silent per-model failures).

---

## Symptom

Observed on 2026-07-23 (tjhm) for `SAPPHIRE_PREDICTION_MODE=PENTAD` and `=DECAD`; the
source analysis below shows it holds for **any execution that reaches Tier 1** under
the current tag definitions. A `machine_learning` run ends with:

```
--- Tier 1: Data Presence (pentad) ---

VALIDATION SUMMARY: 0 passed, 0 failed, 0 warned, 0 skipped
[OK]   machine_learning: PASS (5m 38s)
```

**Zero checks executed, and the runner reports PASS.**

Precisely stated: for any run that reaches Tier 1, the `--module machine_learning`
filter matches zero checks and exits zero — unless an untagged readiness failure
(e.g. API unavailable) independently fails the run first.

## Root cause (traced)

*(Line citations re-verified 2026-08-18 by two independent out-of-loop `codex exec` passes;
several had drifted and are corrected below.)*

1. `machine_learning` appears in `validate_pipeline.py` **only** in the two config
   maps — the `MODULE_DEFAULT_TARGET` entry at `:108` (map starts `:104`) and the
   `FORECAST_DAY_MODULES` entry at `:116` (set starts `:114`; `:113` is the comment).
2. **No Tier-1 or Tier-2 check is ever tagged `module="machine_learning"`.** The only
   module tags emitted anywhere are `linear_regression`, `long_term_forecasting`,
   `postprocessing_forecasts`, `preprocessing_gateway`, `preprocessing_runoff`.
3. The `--module` filter (`:1421-1422`; `:1420` is the comment) keeps only exact tag
   matches → Tier 1 is emptied.
4. Tier 2/3 never run because they require Tier-1 results — the guards are at `:1449`
   (`if tier1_results`) and `:1476` (`if tier1_results and not module_filter`); the
   previously cited `:1448`/`:1474` are the section comments.
5. Zero failures → exit 0. The decision is `print_summary`'s
   `return 1 if counts["FAIL"] > 0 else 0` at `:1284` (`:1270` is the function
   definition) → `run_locally.sh:1099-1104` converts that to `PASS`.

**Additional gap found by the out-of-loop reviewer:** ML writes its raw forecasts as
`horizon_type="day"` (`machine_learning/scr/utils_ml_forecast.py:713-732` documents the
contract; the hard-coded field assignment itself is `:788`, not the previously cited
`:776`, which is the preparation comment), but the
validator never queries the day horizon at all — its only short-term forecast query
uses the requested pentad/decade horizon and tags those results
`postprocessing_forecasts` (`:462`, `:470`). So **raw ML output is covered by no
check under any module tag**, not merely mis-tagged.

## Why it matters

The ML process can exit 0 having written nothing — or having written all-NaN rows —
and the pipeline still reports `machine_learning: PASS`. This module has a
documented history of exactly those failure modes (ML-002, ML-015), and it
is the one module with no effective post-run validation. Any operator or CI job
trusting `run_locally.sh` output is being told ML is healthy on **no evidence**.

This is a **pre-existing** defect, independent of the lead-aware flag work.

## Proposed fix (to be planned)

1. **Add ML-attributed Tier-1 presence checks** that query the horizon ML actually
   writes (`horizon_type="day"`), per model (TFT / TiDE / TSMixer), tagged
   `module="machine_learning"`. Presence alone is insufficient — see (2).
2. **Add a non-null / flag-distribution check** so an all-NaN write (`flag=1` for
   every row) FAILS rather than passing. This is the check that would have caught the
   ML-015 recurrences (incl. 2026-07-23 tjhm) on day one.
3. **Make "zero checks executed" a hard error, not a PASS.** A module filter that
   matches nothing is a bug in the filter or the tags — it must never be reported as
   success. This is the generic guard; it also protects any future module added to
   `MODULE_DEFAULT_TARGET` without corresponding checks.
   Distinguish two outcomes explicitly so the guard cannot mask a dependency outage:
   **(a)** no checks are *registered* for this module (tag/filter bug), vs
   **(b)** registered checks could not *execute* because a dependency (e.g. the
   postprocessing API) was unavailable — the latter must keep reporting the primary
   readiness failure.
4. Respect the forecast-day gate: on a non-forecast day the correct verdict is SKIP
   with the gate reason, not PASS-on-nothing (cf. INFRA-022).
   **Not achievable with the existing gate** — see Constraint C4 below.

---

## Constraints found by out-of-loop review (2026-08-18)

Two independent read-only `codex exec` passes reviewed this draft as an implementer's brief. Both
confirmed the **diagnosis** (no check is tagged `machine_learning`; raw day output is queried by
nothing). Both found the *proposed fix* not implementable as written. These constraints are
findings, not decisions — the ones marked **OWNER DECISION** need sign-off before planning.

### C1 — Five currently-passing tests contradict this draft's acceptance criteria

Any implementer will hit these on the first run. They must be **renegotiated explicitly**, never
quietly edited to match new behavior.

| Test | Asserts today | This draft demands |
|---|---|---|
| `test_validate_pipeline.py:1511-1523` `test_ml_flag_distribution_warn_stuck_flag` | all `flag=1` + finite values → **WARN** | all-flag-1 must **FAIL** |
| `test_validate_pipeline.py:302-332` `test_tier1_short_term_returns_expected_check_count` | `assert len(results) == 13` | +3 ML checks → 16 |
| `test_validate_pipeline.py:376-416` | the six model checks are tagged `postprocessing_forecasts` | retagging to `machine_learning` would strip that coverage |
| `test_validate_pipeline.py:129-133` `test_api_unavailable_exits_zero` | client absent → exit **0** | a blanket "zero checks ⇒ non-zero" flips it |
| `test_validate_pipeline.py:135-140` `test_api_disabled_exits_zero` | `SAPPHIRE_API_ENABLED=false` → exit **0** | same |

Note also that the *generic* NaN check returns WARN, not FAIL (`validate_pipeline.py:662-692`), so
adding ML checks alone does not make an all-NaN write fail — the **severity policy** has to change
too, and that is a contract change in its own right.

### C2 — The zero-match guard must key on *registered* checks, not on an empty result list

`validate_pipeline.py:1589-1597` deliberately returns 0 when the API client is absent or
`SAPPHIRE_API_ENABLED=false`, and those exits are locked by the two tests above. An empty
`tier1_results` is also produced legitimately when the postprocessing API is unready
(`:1418-1419`, `:1432-1433`) and by incompatible combinations such as
`--module long_term_forecasting --target short-term`. Static Tier-1 counts per module today:
`preprocessing_runoff` 2, `preprocessing_gateway` 3, `linear_regression` 1,
**`machine_learning` 0**, `postprocessing_forecasts` 7, `long_term_forecasting` 1 — ML is the only
API-ready module with none. The draft's (a)/(b) distinction is right but names no mechanism; a
check **registry** is required. Also note `--phase pre` returns 0 at `:1493-1495` when
`--baseline` is supplied, bypassing any exit-code contract.

### C3 — **OWNER DECISION**: the checks need an execution-expectation model, not a calendar gate

A calendar-day gate cannot tell "ML failed" from "ML was never supposed to run":

- `ML_MODE` defaults to `DECAD` (`run_locally.sh:154-156`) and an unset prediction mode defaults to
  `PENTAD` (`:1123-1127`), so ML is skipped for the mismatched mode (`:1144-1150`) — yet short-term
  validation still runs (`:1155-1158`).
- `machine_learning` is in both `DEMO_SKIP_MODULES` and `UZHM_SKIP_MODULES` (`:173-174`), so demo
  and the uzb deployment skip ML entirely.

Validation receives no run manifest. Deciding what supplies that expectation — a manifest, an
org/mode-aware rule, or a write receipt — is a design decision, not an implementation detail.

### C4 — **OWNER DECISION**: mode provenance, or an explicitly weaker verdict

Both callers store `horizon_type="day"` with no source-mode field (`utils_ml_forecast.py:788-800`;
the unique key omits mode at `:760-766`), and `test_api_integration.py:329-337` **locks** day
storage even for decade. On the 10th, 20th and month-end — dates in both calendars — a DECAD
validation can pass on PENTAD rows. The draft's "either add provenance or document the limitation"
is not a real option pair: documenting it means the module verdict is knowingly unsound on shared
dates. Choose durable provenance, a run-scoped write receipt, or an explicitly reduced claim.

Related: the existing non-forecast-day gate only converts **zero-record FAIL → SKIP**
(`:1333-1340`). It cannot turn PASS into SKIP, and short-term checks query from the most recent
boundary through today (`:459-481`), so on the 23rd after a run on the 20th, leftovers read as
fresh and PASS. Point 4 of the proposed fix is therefore not achievable by reusing the gate.

### C5 — Presence alone cannot detect a partial write

`check_presence` passes any non-empty frame (`:349-367`), and one surviving station or target row
satisfies it. An expected **station × target coverage** contract is required before acceptance
tests can be written; `doc/dev/testing_workflow.md:138-147` requires exact counts, not
existence-only assertions. The `BOTH`-mode loop compounds this: `validate` runs once per horizon
(`:1401-1427`) and `results_to_json` is keyed on check name alone (`:192-218`), so a duplicated
check name silently overwrites one horizon's result.

### C6 — The existing ML fixture cannot pin the raw-writer contract

`test_validate_pipeline.py:90-103` has `date`, discharge, model and quantiles but **no**
`horizon_type`, `target` or `flag`, while real records carry all three
(`utils_ml_forecast.py:788-800`). Reusing it would let tests pass without asserting the contract
the fix depends on.

---

## Acceptance criteria

*(Revised 2026-08-18 after out-of-loop review. Criteria that depend on an unresolved owner decision
are marked; do not begin implementation while any remain open.)*

- `validate_pipeline --module machine_learning` on a day when ML **was expected to run** emits a
  non-zero number of checks, at least one per configured model, tagged `machine_learning`.
  *Depends on C3 — "expected to run" is undefined until the expectation model is chosen.*
- Zero-row and all-NaN ML results each make the module validation **FAIL**, tested separately via
  **mocked API responses / isolated fixtures with explicit issue and target dates** — not by
  mutating live API data. *Requires the C1 severity-policy change (WARN → FAIL) to be agreed and
  the affected tests updated deliberately, in the same commit, with the reason recorded.*
- Partial writes fail: an expected station × target coverage contract is asserted, not mere
  presence. *Depends on C5.*
- Mode attribution is either sound or explicitly disclaimed in the check's own `detail` string.
  *Depends on C4.*
- A `--module` value for which **no checks are registered** exits non-zero with an explicit
  "no checks registered for module X" message, while API-absent / API-disabled / API-unready
  invocations keep their current exit-0 or readiness-FAIL behavior. *Per C2.*
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green, with new tests
  covering: zero-registration filter, all-NaN ML rows, partial write, `BOTH`-mode duplicate naming,
  and the non-forecast-day gate. The five contract changes in C1 are updated in the same commit,
  each with a comment naming this issue.

## Reproduction

```bash
ieasyhydroforecast_env_file_path=<env> SAPPHIRE_PREDICTION_MODE=DECAD ML_MODE=BOTH \
  bash apps/run_locally.sh machine_learning
# observe: "VALIDATION SUMMARY: 0 passed, 0 failed, 0 warned, 0 skipped" then PASS
```
