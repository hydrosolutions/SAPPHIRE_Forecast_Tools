## `validate_pipeline --module machine_learning` matches zero checks and reports PASS on no evidence (INFRA-020)

**Status**: Draft (2026-07-23) — **diagnosis confirmed, proposed fix blocked on two owner decisions
**READY TO PLAN** — C3, C4 and C5 resolved by the owner 2026-08-18 after five out-of-loop review
passes; C5's partial-write detection is deliberately deferred to a follow-up issue**
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

### C1 — One test must change; four others are compatible **if** the fix is shaped correctly

> **Corrected 2026-08-18 (third review pass).** An earlier version of this section listed five tests
> as contradictions requiring renegotiation. That was an overcorrection and would have told an
> implementer to break four working contracts. Only the count assertion necessarily changes. The
> distinction below is the useful part: each of the other four is a *design constraint on the shape
> of the fix*, not a contract to rewrite.

**Must change (1):**

| Test | Asserts today | Why it must change |
|---|---|---|
| `test_validate_pipeline.py:302-332` `test_tier1_short_term_returns_expected_check_count` | `assert len(results) == 13` | any added ML check changes the count; update deliberately, with a comment naming this issue |

**Must NOT be broken (4) — each constrains the fix:**

| Test | Asserts today | Constraint it imposes |
|---|---|---|
| `test_validate_pipeline.py:1512-1523` `test_ml_flag_distribution_warn_stuck_flag` | all `flag=1` with **finite** values → WARN | this is *not* the all-NaN case. An all-NaN FAIL check must be a **separate** check, leaving the finite stuck-flag WARN intact. Do not repurpose `check_ml_flag_distribution` |
| `test_validate_pipeline.py:376-416` | the six period-forecast checks are tagged `postprocessing_forecasts` | **add** raw-day ML checks; do **not** retag the existing six, which would strip processed-output coverage from postprocessing validation |
| `test_validate_pipeline.py:129-133` `test_api_unavailable_exits_zero` | client absent → exit 0 | the zero-match guard must not fire here — see C2 |
| `test_validate_pipeline.py:135-140` `test_api_disabled_exits_zero` | `SAPPHIRE_API_ENABLED=false` → exit 0 | same |

Note separately that the *generic* NaN check returns WARN, not FAIL (`validate_pipeline.py:662-692`).
Whether the new ML null-check FAILs where the generic one WARNs is a deliberate choice to state in
the plan — the two can differ, but the difference must be intentional and explained.

### C2 — The zero-match guard must key on *registered* checks, not on an empty result list

`validate_pipeline.py:1589-1597` deliberately returns 0 when the API client is absent or
`SAPPHIRE_API_ENABLED=false`, and those exits are locked by the two tests above. An empty
`tier1_results` is also produced legitimately when the postprocessing API is unready
(`:1418-1419`, `:1432-1433`) and by incompatible combinations such as
`--module long_term_forecasting --target short-term`. Static Tier-1 counts per module today:
`preprocessing_runoff` 2, `preprocessing_gateway` 3, `linear_regression` 1,
**`machine_learning` 0**, `postprocessing_forecasts` 7, `long_term_forecasting` 1 — ML is the only
API-ready module with none. The draft's (a)/(b) distinction is right but names no mechanism. A check
**registry** is one option; a static module→expected-check table, declarative check descriptors, or
precomputed per-tag counts would serve equally. The repo proves the distinction is *missing*, not
that any particular mechanism is mandatory — picking one is an implementation choice for the plan.
Also note `--phase pre` returns 0 at `:1493-1495` when `--baseline` is supplied, bypassing any
exit-code contract.

### C3 — **RESOLVED 2026-08-18 (owner): ML runs daily; the only gate is org-level**

> **Correction — the earlier version of C3 was wrong, and so were the two review passes that
> produced it.** It claimed a calendar gate would false-FAIL "default-PENTAD runs" because
> `ML_MODE` defaults to `DECAD` (`run_locally.sh:154-156`, skip at `:1144-1150`). **That is a
> `run_locally.sh`-only behaviour.** In production, Luigi's `RunMLModels` defaults
> `prediction_mode="ALL"`, which expands to `["PENTAD", "DECAD"]` and runs **both**
> (`pipeline_docker.py:785-794`). The reviews reasoned about the local runner as though it were the
> deployed path.

**Operational reality (owner):** ML runs **every day**. Each run takes fresh forcing data and the
latest Q data and produces a 10-day forecast. Raw output is therefore expected daily, independent of
pentad/decad boundaries.

**So the expectation model is far simpler than a manifest.** The single gate is **org-level
enablement**, read from deployment config / `ORG` — the same fact encoded by `DEMO_SKIP_MODULES` and
`UZHM_SKIP_MODULES` (`run_locally.sh:173-174`), where demo and uzb do not run ML at all. That is a
static per-deployment fact, so **INFRA-020 does not depend on INFRA-028's manifest.**

#### C3a — a latent defect this correction exposes

`machine_learning` is listed in **`FORECAST_DAY_MODULES`** (`validate_pipeline.py:113-118`), under
the comment *"Modules that only produce data on forecast days (not daily)."* **For ML that comment
is false.** Consequence once ML-tagged checks exist: `_apply_non_forecast_day_skip` (`:1333-1340`)
would downgrade a genuine "no day rows today" FAIL to **SKIP** on every non-boundary day — silently
recreating the PASS-on-nothing hole this issue exists to close, on ~24 days a month.

**The fix must remove `machine_learning` from `FORECAST_DAY_MODULES`, or make that gate
horizon-aware** so it applies to the pentad/decade products but not to daily output. Note
`linear_regression` and `postprocessing_forecasts` remain correctly listed — their products genuinely
are boundary-day only.

### C4 — **RESOLVED 2026-08-18 (owner): no provenance change needed**

> **Correction — the concern was misframed.** Earlier text held that because raw ML rows are all
> stored as `horizon_type="day"`, a DECAD validation could pass on PENTAD leftovers on shared dates.
> That treats day rows as *mode-specific evidence*. They are not: `_write_ml_forecast_to_api`
> documents `horizon_type` as **informational only** and stores everything as `day`
> (`utils_ml_forecast.py:713-732`), and ML produces those rows daily regardless of mode. Pentad and
> decade ML products are **separate rows at their own horizon types**, which is why filtering by
> horizon type separates them cleanly — as the owner pointed out.

**Decision: leave the write path and the service contract alone.** No durable provenance, no
reduced verdict. A raw-ML check is mode-agnostic **by design**, not as a disclaimed limitation. This
also means `test_api_integration.py:329-337`, which locks day-storage for a decade call, stays
untouched.

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

### C5 — **OWNER DECISION**: presence alone cannot detect a partial write, and the coverage universe is undefined

`check_presence` passes any non-empty frame (`:349-367`), and one surviving station or target row
satisfies it. An expected **station × target coverage** contract is required before acceptance
tests can be written; `doc/dev/testing_workflow.md:138-147` requires exact counts, not
existence-only assertions.

**DECIDED 2026-08-18 (owner): defer C5 to a follow-up issue.** "Assert expected coverage" is not
implementable until someone states **what the expected set is** — which station universe is
authoritative, which target dates are in scope for a given issue date, how models disabled for a
deployment are treated, and therefore what cardinality counts as complete. That is a second issue,
not an acceptance criterion here.

**INFRA-020 therefore ships without partial-write detection**, and that limitation must be stated in
the fix rather than left implicit: a write that lands one station's rows and drops the rest will
still PASS. What INFRA-020 *does* close is the larger hole — PASS on **nothing at all**, and PASS on
an all-NaN write. File the follow-up when this lands, so the gap is tracked rather than forgotten.

Two mechanical consequences to settle alongside it:

- **Pagination.** Every presence read is capped at `READ_LIMIT = 5000` in a single call
  (`:100-101`, `:338-340`). An exact coverage check on a larger deployment would read a complete
  but truncated response as a partial write. Either paginate or demonstrate the maximum expected
  cell count stays under the limit.
- **`BOTH`-mode naming.** `validate` runs Tier 1 once per horizon (`:1401-1427`) and
  `results_to_json` keys on check name alone (`:192-218`), so a duplicated check name silently
  overwrites one horizon's result. *Now a smaller problem given C4: the daily ML check is
  mode-agnostic, so it should be **emitted once per run**, not once per horizon — which sidesteps
  the collision rather than papering over it with horizon-qualified names.*

### C7 — Local and production disagree about whether ML runs for PENTAD

Recorded as context, not as a defect to fix here (owner decision 2026-08-18: note it, do not file a
separate issue). `run_locally.sh` skips ML unless the mode matches `ML_MODE`, which defaults to
`DECAD` (`:154-156`, `:1144-1150`), while Luigi runs **both** modes (`pipeline_docker.py:785-794`).
It is a deliberate local-speed tradeoff, but an undocumented one: **it is why two out-of-loop review
passes concluded that a calendar gate would false-FAIL PENTAD runs** (see C3's correction). Anyone
reasoning about ML scheduling from `run_locally.sh` alone will reach the same wrong answer.

### C6 — The existing ML fixture cannot pin the raw-writer contract

`test_validate_pipeline.py:90-103` has `date`, discharge, model and quantiles but **no**
`horizon_type`, `target` or `flag`, while real records carry all three
(`utils_ml_forecast.py:788-800`). Reusing it would let tests pass without asserting the contract
the fix depends on.

---

## Acceptance criteria

*(Revised 2026-08-18 after out-of-loop review, then again after the owner resolved C3, C4 and C5.
**No criterion below is blocked on an open decision** — C5's coverage work is deferred, not pending.)*

**The check shape, per the owner's answer:**

- **Every day**, on any ML-enabled deployment: `--module machine_learning` emits day-horizon
  presence checks — one per configured model (TFT / TiDE / TSMixer) — tagged `machine_learning`,
  asserting rows exist for today with non-NaN values. Emitted **once per run**, not once per
  horizon.
- **On pentad production days, additionally** the pentad ML rows are checked; **on decad production
  days**, the decade rows. *Note these largely exist already*: the six per-model checks at the
  requested horizon (`:459-481`) cover them and are tagged `postprocessing_forecasts`. Per C1 they
  must **not** be retagged — so confirm the existing coverage is adequate before adding anything,
  rather than duplicating it under a second tag.
- **On a deployment that does not run ML** (demo, uzhm — `run_locally.sh:173-174`): the ML checks
  do not run and do not fail. Determined from deployment config / `ORG`, not from a manifest.
- **`machine_learning` is removed from `FORECAST_DAY_MODULES`** (or that gate is made
  horizon-aware) — see C3a. Without this the new daily checks are silently downgraded to SKIP on
  every non-boundary day, which would reintroduce the defect this issue closes.
- Zero-row and all-NaN ML results each make the module validation **FAIL**, tested separately via
  **mocked API responses / isolated fixtures with explicit issue and target dates** — not by
  mutating live API data. *Implemented as a **new** check: per C1, the existing finite stuck-flag
  WARN (`check_ml_flag_distribution`) stays as it is. If the new ML null-check FAILs where the
  generic NaN check WARNs, say so explicitly in the plan.*
- Partial writes fail: an expected station × target coverage contract is asserted, not mere
  presence, and the read is paginated or proven to fit under `READ_LIMIT`. *Depends on C5.*
- Mode attribution is **sound** (durable provenance or a run-scoped write receipt), or the check
  reports a deliberately reduced status rather than PASS. *Per C4, a `detail`-string disclaimer
  alone is not sufficient — a PASS that is known to be unsound on shared dates is the defect this
  issue exists to remove.*
  *Depends on C4.*
- A `--module` value for which **no checks are registered** exits non-zero with an explicit
  "no checks registered for module X" message, while API-absent / API-disabled / API-unready
  invocations keep their current exit-0 or readiness-FAIL behavior. *Per C2.*
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green, with new tests
  covering: zero-registration filter, all-NaN ML rows, partial write, `BOTH`-mode duplicate naming,
  and the non-forecast-day gate. The single contract change in C1 (the Tier-1 count) is updated in
  the same commit,
  each with a comment naming this issue.

## Reproduction

```bash
ieasyhydroforecast_env_file_path=<env> SAPPHIRE_PREDICTION_MODE=DECAD ML_MODE=BOTH \
  bash apps/run_locally.sh machine_learning
# observe: "VALIDATION SUMMARY: 0 passed, 0 failed, 0 warned, 0 skipped" then PASS
```
