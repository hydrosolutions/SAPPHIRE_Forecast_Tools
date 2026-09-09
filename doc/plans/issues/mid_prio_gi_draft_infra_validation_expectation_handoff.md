## ALTERNATIVE to INFRA-028 — pass the resolved long-term mode list on the CLI instead of persisting a run manifest (INFRA-052)

> ## This is an ALTERNATIVE proposal, not a decision
>
> **The standing design is INFRA-028's run manifest.** The owner decided on 2026-08-18 that *"a
> missing manifest is a validation-infrastructure FAIL and validation NEVER re-derives the
> schedule"* — recorded in `doc/plans/working/lt_schedule_authority_extraction_plan.md:61-63`, with
> a machine-readable gate entry at `:347` and an explicit instruction at `:362` that the entry is
> kept rather than deleted **so that reopening the decision visibly reopens the extraction plan's
> own placement question.** That decision stands today.
>
> This file exists so the cheaper in-memory design is **visible and comparable**, not so it wins. It
> does **not** assert that INFRA-028 is wrong. It records an argument, the gaps that argument
> knowingly accepts, and one gap the argument does not currently cover — so the owner can choose
> between the two with both cost profiles written down. **If it is adopted, INFRA-028 is superseded;
> if it is rejected, this file is archived and INFRA-028 proceeds unchanged.** Nothing here should be
> implemented before that choice is made.

**Status**: Draft (2026-09-09) — **alternative proposal, awaiting owner choice against INFRA-028**
**Module**: `apps/run_locally.sh` (producer) → `apps/validate_pipeline` (consumer)
**Priority**: **Medium**
**Labels**: `infra`, `validation`, `long-term`, `contract`, `alternative`
**Found**: 2026-08-19 on the abandoned `docs_infra_validation_reframe` branch, while asking whether
INFRA-028's manifest was over-engineered. Salvaged 2026-09-09 with every citation re-derived against
trunk, and filed as a separate issue rather than as an edit to INFRA-028, per the owner decision
recorded above.
**Related**:
- **INFRA-028** — the standing run-manifest design. This issue argues for a cheaper transport of
  the same information. Read INFRA-028 first.
- **INFRA-022** — schedule-aware long-term gating. Consumes *whichever* of the two designs is
  chosen; the dependency is on the **content** (which modes were active), not the transport.
- **INFRA-021** — ships atomically with INFRA-022, and **owns forecast-date propagation, which is
  explicitly out of scope here** — see § "What this does NOT cover". *(Description corrected
  2026-09-09: this line called it "the long-term env crash", which is **false on current code** —
  `_load_deployment_env()` exists at `validate_pipeline.py:1580` and is called at `:1724`, shipped in
  PR #486, merged. The repair plan records the same at its § 5 INFRA-021/022 row. What INFRA-021
  still owns here is the forecast date, not the crash.)*
- **INFRA-031** — nothing verifies production forecast runs. Relevant because the argument below
  rests on who the validator's consumer actually is.
- **LTF-007** — the 10-day-vs-5-day gate mismatch. Under either design a mode that was admitted and
  then refused by every model is a genuine execution failure, not a gated SKIP.

---

## The argument

`validate_pipeline` cannot distinguish "the long-term pipeline failed to write output" from "no
output was ever supposed to exist". It demands month, quarter and season output unconditionally
(`apps/validate_pipeline/validate_pipeline.py:518-623` — `run_tier1_long_term` emits the month check
at `:526`, the quarter checks from `:555`, and the seasonal checks from `:594`, none of them gated
on whether the mode ran). On a legitimately gated day it therefore reports FAIL for output that was
never due. That is INFRA-022's defect, and both designs exist to supply the missing input.

Re-deriving the schedule inside the validator does not solve it — a re-derivation cannot see what
the run actually resolved. Both designs agree on that. **They differ only on how the resolved list
travels from producer to consumer.**

**The observation this proposal rests on: for the one caller that resolves the list, the hand-off is
in-memory, in one process, seconds apart.**

`apps/run_locally.sh`'s `run_long_term_operational_pipeline` (`:1639`):

- calls `query_lt_schedule` (`:1650`; the function is defined at `:346` and sets `LT_ACTIVE_MODES`
  at `:373-377`, logging it at `:391`);
- logs `Active modes: ${LT_ACTIVE_MODES}` at `:1664`;
- runs the forecast, postprocessing, skill and maintenance phases;
- calls `run_api_validation "long-term"` at `:1682`, which invokes the validator at `:1479` inside
  `run_api_validation` (`:1471`).

> **But only when at least one mode is active — corrected 2026-09-09.** Between the two, at `:1658`,
> the function tests `LT_ACTIVE_WINDOW` again and, when nothing is scheduled, logs a WARN,
> `record_skip`s at `:1660` and **`return 0` at `:1661`**. Validation at `:1682` is never reached on
> a no-active-mode day. See § "The gap this proposal does **not** currently cover" — this is the
> second half of that gap and it bites the proposal's headline case.

Same shell process, same function body, one variable already in scope. So on that path the
mechanisms that make a manifest *persistent* — a minted run id, stale-file rejection, a retention
policy, an explicit empty file on disk — are solving problems that only arise when a consumer runs
**later, or elsewhere.**

**And the delayed/remote consumer does not exist today.** Verified 2026-09-09 (full sweep in
INFRA-031): no `bin/` script, GitHub workflow, Compose file, systemd unit or deployment doc invokes
`validate_pipeline`; `RunLongTermWorkflow` (`apps/pipeline/pipeline_docker.py:2399`) never validates;
and the pipeline image cannot invoke it even accidentally, since it copies only
`apps/iEasyHydroForecast` and `apps/pipeline` (`apps/pipeline/Dockerfile:20`, `:23-24`).

## The proposal

- `run_locally.sh` passes its already-resolved `LT_ACTIVE_MODES` to `validate_pipeline` as a new
  `--active-modes` argument. No such argument exists today: the validator's parser declares
  `--target`, `--horizon`, `--module`, `--forecast-date`, `--output-json`, `--phase` and
  `--baseline` (`validate_pipeline.py:1668-1716`).
- An **empty value means "nothing was scheduled"** and is meaningful, not missing. It must be
  distinguishable from the argument being **absent**, so an old caller that does not pass it cannot
  silently gate every long-term check off.
  > **This bullet currently has no caller that can exercise it — corrected 2026-09-09.** On a
  > no-active-mode day `run_long_term_operational_pipeline` returns at `:1661` before reaching
  > validation, so nothing ever passes an *empty* `--active-modes`. Making the empty case reachable
  > requires moving or removing that early return — a **control-flow change to the runner**, beyond
  > "pass a variable that is already in scope". See § "The gap this proposal does **not** currently
  > cover".
- `validate_pipeline` gates its long-term checks on that list instead of demanding all three
  horizons.
- **No** manifest file, run id, staleness check, retention policy or config fingerprint.

## Gaps knowingly accepted

These are **not** oversights. They are the price of the minimal transport, and if this design is
adopted they must be stated in the fix and in the module's own documentation, so nobody later
reports them as new bugs.

| Gap | Effect | Why the proposal accepts it |
|---|---|---|
| **Per-model snapped output dates** | A late-accepted run stores the *scheduled* issue date, not the requested one — `check_valid_forecast_issue_date` snaps `today` back to `scheduled_issue_date` when `day_offset > 0` (`apps/long_term_forecasting/lt_utils.py:177`, snap at `:211-217`) — while validation queries the requested date. Models within one mode can differ, since each applies its own `forecast_months` | Rare, and visible to a developer who can read the log — which is the only consumer today |
| **As-run horizon values** | Validation re-resolves quarter and seasonal horizon values from *current* config (`validate_pipeline.py:555`, `:594`). After a config edit between run and validation, it queries a horizon the run never wrote to | Only bites on after-the-fact review, which is not the primary use |
| **Manual Luigi overrides** | `RunLongTermWorkflow` bypasses the schedule query when `active_modes` is supplied explicitly (`pipeline_docker.py:2409`, override branch at `:2446`) | **Moot while Luigi never validates** — and it stops being moot the moment INFRA-031 is acted on |

## The gap this proposal does **not** currently cover

Recorded here rather than in the table above, because these are holes in the proposal, not prices it
has decided to pay. **There are two, and the second is worse.**

### (a) The producer does not hold the list on every path that reaches the validator

- `run_long_term_pipeline` (`run_locally.sh:1615`) calls `run_api_validation "long-term"` at
  `:1636` **without ever calling `query_lt_schedule`** — `LT_ACTIVE_MODES` is unset there.
- `run_all` (`:1685`) runs the short-term pipeline, then `run_long_term_pipeline` (skipping it
  entirely for demo/uzhm at `:1690-1695`), then calls `run_api_validation "all"` unconditionally at
  `:1697` — again with no schedule resolution.

So on two of the three paths that validate the long-term tier, the caller has nothing to pass, and
`--active-modes` would be absent rather than empty. Any adoption of this design must say what the
validator does then. The honest options are: make those paths resolve the schedule too (which grows
the change), have the validator report the long-term tier as a configuration FAIL when the argument
is absent (which is INFRA-028's missing-manifest doctrine arriving by another route), or accept the
current unconditional FAILs on those two targets (which leaves INFRA-022 half-fixed).

### (b) On the one path that *does* hold the list, validation is skipped exactly when it matters

**Added 2026-09-09; this was missed when the proposal was first written.**
`run_long_term_operational_pipeline` resolves the schedule at `:1649-1651`, but then re-tests
`LT_ACTIVE_WINDOW` at `:1658` and, when **no mode is active**, logs a WARN, `record_skip`s at
`:1660` and **returns 0 at `:1661`** — before Phase 2 and, crucially, before
`run_api_validation "long-term"` at `:1682`.

The consequence is precise and awkward for this proposal:

| Case | Does the validator run? | Can `--active-modes` help? |
|---|---|---|
| Modes active | yes, at `:1682` | **yes** — this is the case the proposal genuinely serves |
| **No mode active** | **no — returned at `:1661`** | **no.** Nothing is passed because nothing is called |

So the proposal's own headline feature — *"an empty value means nothing was scheduled, and is
meaningful"* — has **no caller that can produce it**, and its first acceptance criterion ("on a day
when no long-term mode is active, long-term validation reports SKIP") **cannot be met by the CLI
argument alone.** Satisfying it needs the early return moved or removed, i.e. a control-flow change
to the runner, which is materially more than "pass a variable that is already in scope" — the
argument the whole proposal rests on.

Note this cuts at the proposal, not at the requirement: INFRA-022's no-active-day FAILs are still
reachable through `run_long_term_pipeline` (`:1636`) and `run_all` (`:1697`), which is gap (a).

**Taken together, (a) and (b) are the strongest argument on INFRA-028's side, and they should be
weighed as such.** They are stated here rather than buried because the point of filing this
alternative is a fair comparison, and an alternative that hides its own weak points is worse than no
alternative at all.

## What this does NOT cover

**Forecast-date propagation is explicitly out of scope.** Plumbing the run's forecast date through
to the validator's `--forecast-date` belongs to **INFRA-021**. Without it a backdated run validates
the wrong date entirely, which is a different and larger error than the per-model snap in the gaps
table above. Nothing in this proposal substitutes for it, and adopting this proposal does not
relieve INFRA-021 of it.

## If validation ever moves into production

Revisit this proposal immediately — **INFRA-031 is exactly that event.** The order to consider then:
a structured expectation payload (forecast date plus, per mode and model, `horizon_type`,
`horizon_value` and expected output date) carried on the CLI first; a persisted artifact once a
genuinely **delayed, retried, cross-host or independently scheduled** consumer exists. The
date-freezing work — freeze the resolved date at schedule resolution, propagate it into execution,
enforce a stored-date invariant at persistence — is transport-independent and would be needed by
any of these designs, INFRA-028 included.

## Acceptance criteria

*Applicable only if the owner chooses this design over INFRA-028.*

- On a day when no long-term mode is active, long-term validation reports **SKIP**, not FAIL, and
  the run's exit status is unaffected. **⚠ Not satisfiable by the CLI transport alone** *(added
  2026-09-09)*: on that exact day `run_long_term_operational_pipeline` `return 0`s at `:1661`
  without calling `run_api_validation`, so the validator never runs and there is no verdict to make
  SKIP. Meeting this criterion needs a runner control-flow change on top of the argument — which is
  work INFRA-028's transport does not obviously need either, but which this proposal must not
  pretend is free.
- On a day when modes *are* active, missing output for those modes still reports **FAIL**.
- A mode admitted by the scheduler whose models all refused to execute (**LTF-007**) reports
  **FAIL**, not SKIP — output was expected and is absent.
- An empty `--active-modes` value is distinguishable from the argument being absent, and the
  behaviour when it is absent is specified, not left to default.
- The three validating paths are reconciled: `run_long_term_operational_pipeline` (`:1682`),
  `run_long_term_pipeline` (`:1636`) and `run_all` (`:1697`) each have a stated, tested behaviour.
- A deployment that runs no long-term modes at all (demo, uzhm) produces no long-term FAILs,
  including via `run_all`'s unconditional `--target all`.
- The accepted gaps above are recorded in the module's own documentation, not only here.
- `test_long_term_never_skipped` (`apps/validate_pipeline/test/test_validate_pipeline.py:965-985`)
  is **intentionally superseded** — replaced, not deleted silently.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green.
