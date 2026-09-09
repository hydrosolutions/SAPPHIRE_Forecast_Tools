## `validate_pipeline --module machine_learning` matches zero checks and reports PASS on no evidence (INFRA-020)

**Status**: **Draft** (2026-07-23; last revised 2026-09-09). Diagnosis confirmed and independently
reproduced; the implementation plan exists and is named below. **No owner decision blocks this
issue.** One owner-facing question *is* still open — the plan's **M2.4**, headed `OPEN QUESTION
(owner)` — but it is explicitly non-blocking: M2 may ship under its option (ii) without an answer,
and M2.4 governs only how much the check may *claim*.

> *(Corrected 2026-09-09: this line previously read "**no owner decision is outstanding**", which
> contradicted the fourth bullet below and M2.4's own heading in the repair plan. Something **is**
> outstanding; it simply does not gate anything. The distinction is the point — "nothing
> outstanding" would have told a reader not to look.)*

> *(Status line repaired 2026-09-09. It previously carried two contradictory halves — "proposed fix
> blocked on two owner decisions" run straight into "READY TO PLAN", with unbalanced bold markers
> joining them. The first half was left in place when the second was appended. **Neither claim is
> made any more**, for the reasons below.)*
>
> - **"Blocked on two owner decisions" was stale.** Every owner decision this file raises was settled
>   on 2026-08-18 after five out-of-loop review passes, and each records its own resolution in place:
>   **C3** (*"RESOLVED 2026-08-18 (owner)"*) — ML runs daily and the only gate is org-level
>   enablement, so this issue needs no manifest; **C4** (*"RESOLVED 2026-08-18 (owner)"*) — no
>   provenance change; leave the write path and the service contract alone; **C5** (*"DECIDED
>   2026-08-18 (owner): defer C5 to a follow-up issue"*) — partial-write detection is deliberately
>   out of scope, which the Acceptance criteria now state explicitly. **C7** was likewise decided as
>   context to record rather than a defect to file. Nothing here waits on the owner.
> - **"READY TO PLAN" was stale in the opposite direction** — the planning it was waiting for has
>   since happened. The fix is **M2** of `doc/plans/working/validate_pipeline_repair_plan.md` (rev 4);
>   see § "Proposed fix — the implementation plan lives elsewhere" below.
> - **Why `Draft` and not `Ready`.** Per [`doc/plans/README.md`](../README.md), `Ready` means *"plan
>   reviewed, ready for implementation"*. Repair-plan **rev 4 has not had an out-of-loop review
>   pass**, which CLAUDE.md requires before implementation, so that promotion is not yet earned.
>   `doc/plans/module_issues.md` carries the same `Draft` status; the two agree deliberately.
>   Promote both together once rev 4 is reviewed.
> - **One open question remains, and it does not block.** The plan's **M2.4** asks whether to print a
>   mode-provenance caveat. That is optional hardening, not a correctness fix — M2 may ship under
>   option (ii) without it. It is **not** an owner decision this issue is waiting on.


**SUPERSEDED IN PART (2026-09-09, `refactor_run_locally_drop_ml_mode`)**: `ML_MODE` and its
`run_locally.sh`-only DECAD skip (`should_skip_ml_for_mode`) were removed entirely. Every passage
below citing `ML_MODE` (C3's correction, C7, and the `ML_MODE=BOTH` reproduction command) describes
that pre-refactor mechanism as history, not current `run_locally.sh` behaviour — `run_locally.sh`
no longer skips ML for any horizon based on a second variable. This does **not** change this
issue's core finding: `validate_pipeline`'s zero-check PASS-on-no-evidence defect for
`machine_learning` is independent of `ML_MODE` and is unaffected by its removal.
**Module**: `apps/validate_pipeline` (+ `apps/run_locally.sh` summary reporting)
**Priority**: **High** (silent false assurance on the module with the most silent-write history)
**Labels**: `infra`, `validation`, `false-pass`, `machine_learning`, `observability`
**Discovered**: 2026-07-23, local pipeline health review (taj, `maxat_sapphire_2` @ `16fb9a9b`).
**Independently confirmed**: yes — out-of-loop `codex exec` review, read-only, fresh context.
**Related**:
- **ML-015** — operational ML NaN never remediated. INFRA-020 is *why nobody notices*; the
  2026-07-23 tjhm recurrence recorded in ML-015 § Field evidence (4) was reported `PASS`.
- **ML-002** — hindcast subprocess root cause (silent per-model failures).
- **INFRA-031** — nothing verifies production forecast runs. This issue's false assurance is given
  to a developer running the pipeline by hand; INFRA-031 records that production has no data
  verification at all. Read it before pricing this one.
- **INFRA-045** — validator config robustness. Its owner decision **D1** and this issue's **M2.5**
  edit adjacent halves of the same guard; see § "Sequencing against INFRA-045 D1" below.

> **Citation freshness — read before trusting a line number in this file.**
> The `:NNN` citations below were last verified in bulk on **2026-08-18**. `validate_pipeline.py` and
> `run_locally.sh` have both moved substantially since (PR #486 alone inserted
> `_load_deployment_env`), and spot checks on 2026-09-09 confirmed **systematic drift**: e.g. `:1284`
> is cited for `print_summary`'s return, which is now `validate_pipeline.py:1327`; `run_locally.sh:173-174`
> is cited for the skip-module arrays, which are now `:224-225`. **Re-derive with `grep -n` before
> acting on any citation here.**
> The 2026-09-09 salvage pass re-derived only the citations in the passages it edited — those are
> marked *(re-derived 2026-09-09)*. A full sweep of the rest is separate, unfinished work.

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
and the pipeline still reports a PASS row for it (`machine_learning (<MODE>): PASS`
since `refactor_run_locally_drop_ml_mode` added the horizon suffix; the 2026-07-23
sample above predates that and is left as recorded). This module has a
documented history of exactly those failure modes (ML-002, ML-015), and it
is the one module with no effective post-run validation. Any operator or CI job
trusting `run_locally.sh` output is being told ML is healthy on **no evidence**.

This is a **pre-existing** defect, independent of the lead-aware flag work.

## Proposed fix — **the implementation plan lives elsewhere**

> **This issue is already planned.** The fix is **M2** in
> [`doc/plans/working/validate_pipeline_repair_plan.md`](../working/validate_pipeline_repair_plan.md)
> (rev 4), alongside M1 (INFRA-025) and M3 (INFRA-026). That plan carries a governing constraint this
> issue does not state and which shapes any fix here: **do not add new statuses.** `print_summary`
> returns non-zero only for `FAIL` (`validate_pipeline.py:1327`), `run_locally.sh` consumes only the
> process exit code, and the tests recognise exactly four statuses
> (`test_validate_pipeline.py:1083`) — so a new `ERROR` status would render **green**.
> *(Citations re-derived 2026-09-09.)*
>
> **Division of labour**: this issue states the problem and the constraints (C1-C7 below); the plan
> states the fix. Rev 4 of the plan folds in C1's add-don't-retag finding, C4's dissolution of the
> provenance concern, and C3a's `FORECAST_DAY_MODULES` defect as **M2.5**, plus a deployment-level
> ML-enablement gate as **M2.6**. Where the two disagree, the plan wins — **except where this issue
> cites a locked test, which always wins.**
>
> **M2.5 and M2.6 are not optional halves of one change.** M2.5 removes `machine_learning` from
> `FORECAST_DAY_MODULES` so the new daily checks are not downgraded to SKIP on ~24 days a month
> (C3a). M2.6 then gates the ML checks on deployment-level ML enablement, because without it M2.5
> leaves unconditional ML checks running on deployments that do not run ML at all. **Landing M2.5
> without M2.6 makes things worse than the current defect** — it converts a false PASS into a
> recurring false FAIL on demo and uzhm.
>
> **M2.6's motivating example is half stale — corrected 2026-09-09.** `machine_learning` is in both
> `DEMO_SKIP_MODULES` and `UZHM_SKIP_MODULES` (`run_locally.sh:224-225`). Of the two paths that used
> to reach validation after skipping the module:
> - **The bare `machine_learning` target is already guarded.** `run_module_validation
>   "machine_learning"` (`:2693`) sits inside the `else` of `if should_skip_module machine_learning`
>   (`:2664`), so on demo/uzhm the module is recorded as a skip and validation never runs. *(Verified
>   2026-09-09. `git log -L` attributes the guard itself to the original org-aware filtering commit
>   `d81adb68` and its `record_skip` line to INFRA-030 (`bf311583`); **this pass could not confirm
>   the attribution to INFRA-039** that an earlier draft asserted. The guard's existence is verified;
>   which issue closed it is not.)*
> - **The pipeline path is still live.** `run_short_term_pipeline` skips ML at `:1580` but then calls
>   `run_api_validation "short-term"` at `:1612` with **no module filter** — so ML-tagged checks
>   would run on a deployment that never ran ML. `run_all` (`:1685`) does the same at `:1697`, and
>   `run_daily_pipeline` at `:1872`.
>
> So M2.6 remains necessary, but its justification is the **unfiltered pipeline-level validation**,
> not the bare target.

### Sequencing against INFRA-045 D1

**These two must not be landed blind to each other.** *(Added 2026-09-09.)*

INFRA-045's owner decision **D1** makes `--target daily` derive `["pentad", "decade"]` from the
target, and its stated operator consequence depends on `_apply_non_forecast_day_skip()`
(`validate_pipeline.py:1352`) downgrading **absent decade data to SKIP** away from decade forecast
days. **M2.5 changes `FORECAST_DAY_MODULES` (`:116-120`) — the very set that same function consults
at `:1385`.** They edit adjacent halves of one guard: D1 changes *which horizons reach it*, M2.5
changes *which modules it downgrades*.

Both paths also run through the same locked test: **`test_all_forecast_modules_affected`**
(`test_validate_pipeline.py:937-963`, class `TestNonForecastDaySkip` at `:849`), which pins
`machine_learning` to SKIP.

**Whoever lands second must:**
1. re-derive the line numbers above — both will have moved;
2. re-check the other issue's **stated operator consequence** still holds after their change, and
   correct it in the other issue's file if it does not (INFRA-045's D1 text has already been
   corrected once for overstating an operator-visible effect);
3. reconcile `test_all_forecast_modules_affected` deliberately, not by letting it fail and then
   "fixing" it.

*(Retained below: the fix direction as originally stated, which the plan implements.)*

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
4. ~~Respect the forecast-day gate: on a non-forecast day the correct verdict is SKIP with the gate
   reason, not PASS-on-nothing (cf. INFRA-022).~~ **Superseded — inverted by C3.** ML runs **daily**,
   so for ML there is no such thing as a non-forecast day: `machine_learning` must come *out* of
   `FORECAST_DAY_MODULES` (C3a / plan M2.5), or the gate would downgrade genuine failures to SKIP on
   ~24 days a month. The gate stays correct for `linear_regression` and `postprocessing_forecasts`,
   whose products really are boundary-day only. The separate observation that the existing helper
   cannot produce SKIP for a *populated* dataset is preserved as **C4a** below.

---

## Constraints found by out-of-loop review (2026-08-18)

Two independent read-only `codex exec` passes reviewed this draft as an implementer's brief. Both
confirmed the **diagnosis** (no check is tagged `machine_learning`; raw day output is queried by
nothing). Both found the *proposed fix* not implementable as written. These constraints are
findings, not decisions.

> *(Corrected 2026-09-09.)* This preamble used to end *"— the ones marked **OWNER DECISION** need
> sign-off before planning."* **No constraint below is marked `OWNER DECISION` any more, and none
> needs sign-off.** C3, C4, C5 and C7 were all settled by the owner on 2026-08-18 and each heading
> now says so; C5's heading was the last one still carrying the old marker over a body that already
> read `DECIDED 2026-08-18`. Read C1, C2, C4a and C6 as design constraints on the fix, and C3, C4,
> C5 and C7 as recorded decisions.

### C1 — **Two** tests must change; four others are compatible **if** the fix is shaped correctly

> **Corrected 2026-08-18 (third review pass).** An earlier version of this section listed five tests
> as contradictions requiring renegotiation. That was an overcorrection and would have told an
> implementer to break four working contracts. The distinction below is the useful part: each of
> the "must NOT be broken" four is a *design constraint on the shape of the fix*, not a contract to
> rewrite.
>
> **Count corrected 2026-09-09 — this section said "one".** It missed
> `test_all_forecast_modules_affected`, which **C3a below invalidates**: that test pins
> `machine_learning` to SKIP through `_apply_non_forecast_day_skip`, and C3a's fix removes
> `machine_learning` from `FORECAST_DAY_MODULES`. Two locked contracts change, not one. The
> Acceptance criteria list has been corrected to match.

**Must change (2)** *(line numbers re-derived 2026-09-09; note the test directory is
`apps/validate_pipeline/test/`, not `tests/`)*:

| Test | Asserts today | Why it must change |
|---|---|---|
| `test_validate_pipeline.py:304-334` `test_tier1_short_term_returns_expected_check_count` | `assert len(results) == 13` (`:333`) | any added ML check changes the count; update deliberately, with a comment naming this issue |
| `test_validate_pipeline.py:937-963` `test_all_forecast_modules_affected` (class `TestNonForecastDaySkip`, `:849`) | `machine_learning` **must** become SKIP on a non-forecast day — `assert all(r.status == "SKIP" ...)` at `:963` | **invalidated by C3a** — ML runs daily, so this downgrade is what would reintroduce the false-green. Replace it deliberately; do not let it fail and then "fix" it |

**Must NOT be broken (4) — each constrains the fix:**

| Test | Asserts today | Constraint it imposes |
|---|---|---|
| `test_validate_pipeline.py:1514-1525` `test_ml_flag_distribution_warn_stuck_flag` *(re-derived 2026-09-09; the function under test is `check_ml_flag_distribution`, `validate_pipeline.py:905`)* | all `flag=1` with **finite** values → WARN | this is *not* the all-NaN case. An all-NaN FAIL check must be a **separate** check, leaving the finite stuck-flag WARN intact. Do not repurpose `check_ml_flag_distribution` |
| `test_validate_pipeline.py:378-418` `test_tier1_short_term_module_mapping` (class `TestModuleAttribution`, `:375`; assertions `:409-414`) *(re-derived 2026-09-09)* | the six period-forecast checks are tagged `postprocessing_forecasts` | **add** raw-day ML checks; do **not** retag the existing six, which would strip processed-output coverage from postprocessing validation |
| `test_validate_pipeline.py:131` `test_api_unavailable_exits_zero` *(re-derived 2026-09-09)* | client absent → exit 0 | the zero-match guard must not fire here — see C2 |
| `test_validate_pipeline.py:137` `test_api_disabled_exits_zero` *(re-derived 2026-09-09)* | `SAPPHIRE_API_ENABLED=false` → exit 0 | same |

Note separately that the *generic* NaN check returns WARN, not FAIL — `check_no_nan_in_forecasts` is defined at `validate_pipeline.py:705` and its `if nan_count > 0:` branch returns `status="WARN"` at `:725-730` *(endpoint corrected 2026-09-09: `:705` alone is only the `def`)*.
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

*(A second, contradictory copy of C4 stood here until 2026-09-09 — the original **unresolved**
version, left in place when the resolved one above was added. It demanded durable provenance or an
explicitly reduced verdict, which the resolved C4 above and the repair plan both reject, so the file
told an implementer two opposite things. Removed. The part of it that is independently true is
preserved as **C4a** below; the residual mode-overlap concern is the plan's **M2.4 open question**
— optional hardening that explicitly does **not** block M2 — not a requirement here.)*

### C4a — the non-forecast-day gate cannot deliver "SKIP on a quiet day"

Kept from the removed text because it is independently true and still constrains the fix.
*(Citations re-derived 2026-09-09.)*

The existing gate only converts a **zero-record FAIL → SKIP**: `_apply_non_forecast_day_skip`
(`validate_pipeline.py:1352`) downgrades only results that are `not r.critical`, `status == "FAIL"`,
`record_count == 0` **and** whose module is in `FORECAST_DAY_MODULES` (`:1380-1389`). It cannot turn
a PASS into a SKIP.

And short-term presence checks query **from the most recent boundary through today** —
`run_tier1_short_term` (`:381`) computes `boundary` at `:396-403` and passes `start_date=bd`,
`end_date=fd` to the per-model checks (`:477-489`). So on the 23rd, after a run on the 20th,
leftovers read as fresh and the check PASSes.

Any "quiet day ⇒ SKIP" requirement therefore needs the check itself gated; reusing the existing
helper cannot deliver it.

### C5 — **DECIDED 2026-08-18 (owner): partial-write detection is deferred to a follow-up issue** — presence alone cannot detect a partial write, and the coverage universe is undefined

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
- **Partial writes are NOT in scope.** *(Corrected 2026-09-09 — this criterion previously demanded a
  station × target coverage contract while **C5 above defers exactly that work to a follow-up issue**.
  The two could not both be satisfied.)* INFRA-020 ships **without** partial-write detection: a write
  that lands one station's rows and drops the rest still PASSes, and **that limitation must be stated
  in the fix**, not left implicit. File the follow-up when this lands.
- **Mode attribution: see the plan's M2.4 open question, not this list.** *(Corrected 2026-09-09 —
  this criterion previously required durable provenance or a reduced status, which the **resolved C4
  above** and the repair plan both reject.)* What survives is narrower than either: PENTAD writes a
  6-day span and DECAD an 11-day span from the same issue date
  (`machine_learning/make_forecast.py:611-614`), and both are stored as `horizon_type="day"`
  (`scr/utils_ml_forecast.py:818`), so a mode-agnostic day check cannot prove *which* mode produced
  overlapping rows. Whether to disclaim that, detect it via the 7-11 day span, or accept it is an
  **open question recorded in the plan's M2.4** — optional hardening that does not block M2, and
  **not** an owner decision this issue waits on — rather than an acceptance criterion here.
  *(Citations re-derived 2026-09-09.)*
- A `--module` value for which **no checks are registered** exits non-zero with an explicit
  "no checks registered for module X" message, while API-absent / API-disabled / API-unready
  invocations keep their current exit-0 or readiness-FAIL behavior. *Per C2.*
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green, with new tests
  covering: zero-**registration** filter, all-NaN ML rows, `BOTH`-mode single emission, and an
  ML-disabled deployment. *(Partial write removed — out of scope per C5.)*
- **Two** locked test contracts are changed deliberately, in the same commit, each with a comment
  naming this issue. *(Corrected 2026-09-09 — this list previously said "the single contract change
  in C1", which undercounts.)* *(Line numbers re-derived 2026-09-09 against
  `apps/validate_pipeline/test/test_validate_pipeline.py` — note the directory is `test/`, not
  `tests/`.)*

  | Test | Where | Asserts today | Why it must change |
  |---|---|---|---|
  | `test_tier1_short_term_returns_expected_check_count` | `:304-334`, assertion `assert len(results) == 13` at `:333` | the exact Tier-1 short-term check count | any added ML check changes the count |
  | `test_all_forecast_modules_affected` | `:937-963` (class `TestNonForecastDaySkip`, `:849`), assertion `assert all(r.status == "SKIP" ...)` at `:963` | `machine_learning` **must** become SKIP on a non-forecast day | invalidated by **C3a** — ML runs daily, so this downgrade is what reintroduces the false-green |

## Reproduction

**SUPERSEDED 2026-09-09**: `ML_MODE=BOTH` below is inert — `ML_MODE` no longer exists
(`refactor_run_locally_drop_ml_mode`). `SAPPHIRE_PREDICTION_MODE=DECAD` alone reproduces the same
observation.

```bash
ieasyhydroforecast_env_file_path=<env> SAPPHIRE_PREDICTION_MODE=DECAD ML_MODE=BOTH \
  bash apps/run_locally.sh machine_learning
# observe: "VALIDATION SUMMARY: 0 passed, 0 failed, 0 warned, 0 skipped" then PASS
```
