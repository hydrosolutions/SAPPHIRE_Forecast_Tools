## Long-term Tier-1 reports FAIL on a legitimately gated (non-issue) day (INFRA-022)

**Status**: Draft (2026-07-23)
**Module**: `apps/validate_pipeline` (`run_tier1_long_term`, `_apply_non_forecast_day_skip`)
**Priority**: **High** *(raised from Medium 2026-08-18 — this is now a hard prerequisite of
INFRA-021, not a follow-on; see below)*
**Labels**: `infra`, `validation`, `long-term`, `false-fail`, `gating`
**Discovered**: 2026-07-23, local pipeline health review (taj, `maxat_sapphire_2` @ `16fb9a9b`).
**Relationship to INFRA-021 — corrected 2026-08-18.** This draft previously read
"Blocked behind INFRA-021". That is now wrong in one direction and produced a circular dependency
once INFRA-021 was rescoped. The accurate statement:

- **Observability** ran one way: this defect was only *visible* once INFRA-021's crash was fixed,
  because the process used to die before Tier 1 emitted anything.
- **Delivery** runs the other way: INFRA-021 must **not ship without this gating**, because fixing
  the crash alone converts one traceback into recurring false FAILs on every legitimately gated day
  and on every deployment that does not run long-term at all.

> **Updated 2026-09-09 — the crash half has shipped, so read the first bullet in the past tense.**
> `_load_deployment_env()` now exists (`validate_pipeline.py:1580`) and `main()` calls it before any
> config access, returning 1 if it fails (`:1724`); the quarter and seasonal horizon resolutions are
> additionally guarded and append a `critical=True` FAIL row instead of raising (the `try`/`except`
> around `:555` and around `:594`). That landed in **PR #486, merged**, and
> `doc/plans/working/validate_pipeline_repair_plan.md` records the same in its § 5
> INFRA-021 / INFRA-022 row.
>
> **What that changes for this issue:** the false FAILs described below are **live and observable
> today** — no longer hidden behind a traceback, and no longer waiting on INFRA-021 to become
> visible. The atomic-landing argument in the second bullet still holds for what INFRA-021 still
> owns (forecast-date propagation), but nothing in this issue is blocked on the crash any more.
>
> *(INFRA-021's own issue file and its tracker row still describe the crash as live. Both are out of
> scope here and are recorded as a follow-up — do not read their staleness as contradicting this
> note.)*

So the two are **one atomic change**, not a queue. Neither blocks the other; they land together.
**Related**:
- **INFRA-020** — false-PASS counterpart; same class of defect, opposite sign.
- **INFRA-028** — the standing design for telling the validator what the run resolved. This issue
  consumes it.
- **INFRA-052** — an **alternative** to INFRA-028 (an in-memory `--active-modes` CLI argument rather
  than a persisted manifest). Filed so both designs are visible; **the 2026-08-18 manifest decision
  currently stands.** This issue's dependency is on the *content* — which modes were active — and is
  unaffected by which transport the owner picks.
- **INFRA-031** — nothing verifies production forecast runs. Relevant to how this issue's cost is
  priced: `validate_pipeline` has no production invoker, so a false FAIL here is seen by a developer
  running the pipeline, not by an operator.

---

## Symptom

On a day when the long-term forecast gate is legitimately **closed**, long-term
Tier-1 reports failures for data that correctly does not exist:

```
[FAIL] Long-term forecasts (month): no records            [long_term_forecasting]
[OK]   Monthly skill metrics: 5000 records
[FAIL] Long-term forecasts (quarter hv0): no records
[OK]   Quarterly skill metrics: 5000 records
[FAIL] Long-term forecasts (season issue 4 hv0): no records
[OK]   Seasonal skill metrics: 1812 records
```

Meanwhile `long_term_forecasting` itself correctly logged the gate decision for
every model, e.g.:

```
Model LR_Base not scheduled: 9 days from issue date 2026-08-01 — skipping
```

So the module behaved correctly and the validator called it a failure.

## Root cause (CONFIRMED by source inspection — not a hypothesis)

> **Still live on trunk — re-verified 2026-09-09.** *(Citations in this section re-derived; the rest
> of the file's `:NNN` citations date from 2026-08-18 and have drifted — re-derive before acting.)*
>
> - `run_tier1_long_term` (`validate_pipeline.py:518`) emits the month presence check at `:526`, the
>   quarter checks from `:555` and the seasonal checks from `:594`, **none of them gated** on whether
>   the mode ran.
> - `_apply_non_forecast_day_skip` (`:1352`) still hard-codes `"long-term": True` at `:1374`, and
>   returns immediately at `:1378-1379` when the horizon maps to `True`.
> - `long_term_forecasting` is still absent from `FORECAST_DAY_MODULES` (`:116-120`), so even
>   reaching the downgrade loop at `:1380-1389` would not help — the loop only touches modules in
>   that set.
> - The behaviour is still locked by `test_long_term_never_skipped`
>   (`apps/validate_pipeline/test/test_validate_pipeline.py:965-985`; note the directory is `test/`,
>   not `tests/`), whose own in-body comment at `:984` names the reason —
>   *"long_term_forecasting is not in FORECAST_DAY_MODULES"* — immediately above its assertion
>   `assert results[0].status == "FAIL"` at `:985`. *(Range widened 2026-09-09: an earlier `:965-983`
>   stopped short of both, so the claim about the comment could not be checked at the cited lines.)*
>
> **Two independent mechanisms** therefore keep long-term FAILs from being downgraded. A fix that
> addresses only one of them changes nothing.

`_apply_non_forecast_day_skip()` maps the long-term horizon to a constant `True`,
i.e. "always treat as a forecast day", so long-term FAILs are **never** downgraded
(`validate_pipeline.py:1374`):

```python
is_forecast_day = {
    "pentad": is_pentad_forecast_day(forecast_date),
    "decade": is_decad_forecast_day(forecast_date),
    # Long-term forecasts run on specific dates per month;
    # we cannot predict the schedule, so always treat as
    # potentially a non-forecast day when data is absent.
    "long-term": True,
```

`long_term_forecasting` is also absent from `FORECAST_DAY_MODULES` (`:113`), and the
behaviour is **locked by an explicit test**,
`test_validate_pipeline.py::test_long_term_never_skipped` ("We can't predict
long-term schedule, so we don't downgrade FAILs").

So this is **not** a bug in a schedule comparison — long-term gating is *deliberately
unimplemented*, on the stated premise that the schedule is unpredictable. **That
premise is now outdated**: `iEasyHydroForecast/long_term_horizon_resolver.py` exposes
a per-mode schedule (lead + `operational_issue_day`) that the write path already uses.

> An earlier draft of this issue hypothesised that a skip rule was misaligned to a
> 10/25 convention. That is **wrong** and was corrected by out-of-loop review: the
> validator never reads `LT_OPERATIONAL_ISSUE_DAYS`; the runner's 10/25 logic is only
> a *fallback* used when `lt_schedule_query.py` fails (`run_locally.sh:274`), and its
> normal path is already config-driven.

### Per-mode, not per-deployment

The schedule is exposed **per supported mode**, each with its own lead and issue day
(`long_term_horizon_resolver.py:112`). Month, quarter and season can therefore be
gated differently on the same date. A single deployment-level open/closed decision
would be wrong in both directions — it could skip a genuinely missing active mode, or
fail an inactive one.

Deployment evidence (read from the local deployment config repos on 2026-07-23, **not**
repo-verifiable — the only `operational_issue_day` tracked in this repo is
`apps/long_term_forecasting/config_monthly.json` = 25):

| Deployment | `operational_issue_day` |
|-----------|--------------------------|
| taj (tjhm) | 1 (all five modes) |
| kyg (kghm) | 10 (`month_0`) / 25 (other modes) |

## Why it matters

- A correct, quiet day produces FAIL lines, and `print_summary` turns any FAIL into a non-zero exit
  (`validate_pipeline.py:1327`), so the long-term target looks broken on every ordinary day.
  *(Reworded 2026-09-09: this read "Combined with INFRA-021's non-zero exit…", which described the
  pre-PR-#486 crash as the source of the non-zero exit. The crash is fixed; the non-zero exit these
  false FAILs produce is this issue's own, and it does not depend on INFRA-021 at all.)*
- **A check that is always red certifies nothing.** Once the long-term tier FAILs on every ordinary
  day, its verdict carries no information: a genuine long-term outage is indistinguishable from the
  normal case, and the reviewer or developer reading the run learns to discount the whole section.
  That is the same defect as INFRA-020's false PASS, with the sign reversed.

  > *Rewritten 2026-09-09.* The previous wording here was **"alarm fatigue: once operators learn
  > the long-term validation is always red, a genuine outage will not be noticed."* That
  > contradicts the finding recorded in **INFRA-031**: `validate_pipeline` has no production
  > invoker, so no operator ever sees this output. The cost is real but it lands on the developer
  > review gate, not on operations — and stating it as an operational risk would misprice the
  > issue. **The operational counterpart is INFRA-031, and it is a separate, unbuilt thing.**

## Proposed fix (to be planned)

1. Implement the long-term downgrade using a per-mode schedule authority. Decide activity
   **separately for every checked mode/horizon** and tag each result with its owning mode — do not
   collapse to one deployment-level gate.

   > **SETTLED — which schedule authority? (raised 2026-08-18, third out-of-loop pass; decided the
   > same day — see "DECIDED 2026-08-18: option (d)" below.)**
   >
   > *(Marker corrected 2026-09-09: this block opened with "**OPEN DECISION**" while its own body
   > records the answer. A reader scanning for blockers saw an open decision that had already been
   > made. The narrative is kept as posed, because the options explain why (d) was chosen — but
   > **nothing in this block is awaiting an answer.** The one thing still open in this issue is which
   > **transport** carries the resolved mode list, INFRA-028 or INFRA-052; see the transport note
   > further down.)*
   >
   > This step originally said to use `long_term_horizon_resolver`. **That resolver is not
   > sufficient on its own.** It exposes mode, lead time and issue day (`:33-49`, `:112-155`), but
   > the *real* operational schedule adds two things it does not model:
   >
   > - an **issue-day tolerance window** (`lt_schedule_query.py:50-52`), and
   > - **per-model `forecast_months`** — a mode can be configured yet inactive in the current
   >   month (`:88-131`).
   >
   > Gate on the resolver alone and a quarter or season mode that is simply not scheduled this
   > month still reports FAIL — the precise false alarm this issue exists to remove.
   >
   > **Two further gaps found while writing this up (2026-08-18), not in either review:**
   >
   > - **`NON_OPERATIONAL_MODES`.** `query_schedule` skips `{"monthly"}` outright
   >   (`lt_schedule_query.py:57`, `:89-91`) — and the comment there says such modes are
   >   *deliberately kept* in `ieasyhydroforecast_ml_long_term_supported_modes` so the maintenance
   >   pipeline can reference them. So a validator that iterates `supported_long_term_modes()`
   >   would demand operational output for a mode that is **never** operationally issued, and
   >   FAIL every single day. The resolver exposes no notion of "non-operational".
   > - **The tolerance is in flux.** `ISSUE_DAY_TOLERANCE = 10` carries the comment "Temporarily
   >   relaxed to 10 days… Must be changed back to 5 days for operational use"
   >   (`lt_schedule_query.py:50-52`). Any gating that hard-codes or re-derives this number will
   >   silently disagree with the scheduler the day it goes back to 5. Whatever is built must read
   >   the tolerance from **one** place.
   >
   > **Options:**
   >
   > | | Approach | Cost |
   > |---|---|---|
   > | (a) | validation calls `lt_schedule_query.query_schedule()` directly | single source of truth, but it lives in `long_term_forecasting`, builds a `ForecastConfig`, and calls `sl.load_environment()` internally (`:76`) — the very function INFRA-021 says must not be called unchanged. Heavy for a validator whose `pyproject.toml` declares only `pandas` |
   > | (b) | extend `long_term_horizon_resolver` to expose tolerance, `forecast_months` and non-operational modes | keeps the validator light, but creates a **second** schedule authority — and two definitions that must agree is how this class of bug arises in the first place |
   > | (c) | accept known-incomplete gating, document which cases still false-FAIL | cheapest and honest, but ships a check that is still wrong on unscheduled months |
   > | (d) | extract the mode-activity decision so it has exactly one definition | most up-front work, removes the drift risk permanently |
   >
   > *(The options above are recorded as they were posed. Option (d)'s **placement** has since moved:
   > it was originally "into `iEasyHydroForecast`, called by both `lt_schedule_query` and
   > `validate_pipeline`". Evidence gathered 2026-08-18 showed validation will not call it, so the
   > module now lives in `apps/long_term_forecasting/lt_schedule_rules.py` — see the plan, rev 4.)*
   >
   > **DECIDED 2026-08-18: option (d)** — the mode-activity decision gets one definition, planned in
   > `doc/plans/working/lt_schedule_authority_extraction_plan.md`.
   >
   > **But this issue does not consume that extraction.** Out-of-loop review established that a
   > re-derived schedule — however well single-sourced — cannot see manual overrides
   > (`pipeline_docker.py:2339-2367` bypasses `LTScheduleQuery`), execution outcome, or the date
   > output was actually written under (late forecasts snap back, `lt_utils.py:211-217`). **Gating
   > therefore consumes a run manifest, filed as INFRA-028, which this issue depends on.**
   >
   > **Transport note, added 2026-09-09 — the dependency is on the *content*, not the file.**
   > What this issue needs is a trustworthy statement of **which modes the run actually resolved**.
   > **INFRA-028's persisted run manifest is the standing design** and the owner decision behind it
   > (2026-08-18: *a missing manifest is a validation-infrastructure FAIL, and validation never
   > re-derives the schedule* —
   > `doc/plans/working/lt_schedule_authority_extraction_plan.md:61-63`) is unchanged.
   > **INFRA-052** proposes a cheaper alternative transport for the same content — an in-memory
   > `--active-modes` CLI argument passed by the caller — and is filed so the two can be compared;
   > it is **not** a decision, and it does not supersede INFRA-028.
   > Wherever this draft says "manifest" below, read it as *"whichever record of what the run
   > resolved is finally adopted"*. Nothing in this issue's design changes with the choice; only the
   > residual gaps differ, and INFRA-052 lists the ones its transport would knowingly accept.
   >
   > *(Note also that INFRA-052 records a gap in its own proposal that INFRA-028 does not have: two
   > of the three `run_locally.sh` paths that validate the long-term tier —
   > `run_long_term_pipeline` and `run_all` — never resolve the mode list at all, so the caller has
   > nothing to pass. Weigh that when the choice is made.)*
   >
   > *Corrected 2026-08-18 (plan rev 4):* an earlier version of this block implied gating would call
   > the extracted predicates. It will not. Validation needs *which modes to check* (the manifest)
   > and *the horizon value to query* — and the latter already comes from
   > `long_term_horizon_resolver` (`validate_pipeline.py:539`, `:562`). Tolerance, `forecast_months`
   > and the non-operational set never enter validation's work. Consequently the extraction is
   > **independent cleanup, not a prerequisite of this issue**; its real second consumer is
   > **LTF-007** (`lt_utils.check_valid_forecast_issue_date`).
   >
   > See also **LTF-007**: the scheduler's 10-day window vs execution's 5-day gate means a mode can
   > be admitted whose models all refuse to run. Note the *semantics* — under the manifest contract
   > that is a **genuine detected execution failure**, not a legitimately gated SKIP.
   **Skill-metric checks must NOT inherit the operational forecast gate** — they are
   historical and not date-filtered, so gating them would hide real starvation.
2. When the gate is closed, emit **SKIP with the gate reason** ("not a long-term
   issue day for <mode>; next issue date <date>"), matching how the short-term path
   already reports `[SKIP] LR details (pentad): not a pentad forecast day`.
3. Keep FAIL for the genuine case: gate **open** and records absent.

**Contract not to break:** do not weaken the open-gate case to a warning. The value of
this check is catching a missed issue-day run; only the closed-gate case should be
downgraded.

## Acceptance criteria

- On a non-issue day for the configured deployment, long-term Tier-1 reports SKIP
  with the gate reason and does **not** contribute failures.
- On an issue day with records genuinely missing, it still reports FAIL.
- Correct behaviour verified per **mode** for both issue-day conventions (taj day 1;
  kyg 10/25) — a fixture per convention, placeholder station codes only.
- A mode that is configured but **not scheduled in the current month** reports SKIP, not FAIL, and
  a day inside the **issue-day tolerance window** is treated as an issue day. *(Updated 2026-08-18:
  these come from the record of **what the run actually resolved** — INFRA-028's manifest under the
  standing design, or INFRA-052's `--active-modes` if that alternative is adopted. They are
  not re-derived here — neither is achievable from `long_term_horizon_resolver`, and re-deriving
  them would miss manual overrides regardless.)*
- A mode admitted by the scheduler whose models all refused to execute (**LTF-007**, the 10-vs-5
  band) reports **FAIL**, not SKIP — output was expected and is absent. Gating must not be built to
  excuse it.
- A deployment that runs **no** long-term modes at all (demo, uzhm) produces no long-term FAILs,
  including via `run_all`'s unconditional `--target all`. *(Citation re-derived 2026-09-09:
  `run_all` is `run_locally.sh:1685`; it skips the long-term pipeline for those orgs at `:1690-1695`
  but still calls `run_api_validation "all"` at `:1697`, which runs the long-term tier.)*
- `test_long_term_never_skipped` is **intentionally superseded**: it locks the current
  premise and must be replaced (not deleted silently) by tests asserting per-mode
  gating, so the behaviour change is explicit and reviewed.
- **Shared-fixture impact — re-scoped 2026-08-18 (third pass).** An earlier version of this bullet
  required the autouse fixture (`test_validate_pipeline.py:36-59`) to gain `operational_issue_day`,
  because gating was assumed to call `operational_schedule_for_mode`
  (`long_term_horizon_resolver.py:112-142`). **That justification is gone**: gating consumes the
  INFRA-028 manifest and calls no scheduling predicate, and `validate_pipeline` imports only the
  lead-value helpers (`:35-49`), which need `operational_month_lead_time` alone
  (`long_term_horizon_resolver.py:68-81`). The instruction outlived its reason — the same defect
  shape as the vestigial phase in the extraction plan.
  **What remains true, stated more precisely** *(the first replacement overstated this too)*: the
  tests at `:418-477` must be **re-examined**, not necessarily re-verdicted. The mapping test
  asserts only module attribution and is unaffected. The empty-quarter, empty-season and
  all-present tests can keep their current FAIL/PASS verdicts **if** supplied a manifest double
  that marks those modes active — what changes is that they now need such a double at all. Whether
  the shared fixture needs new fields depends on what that double carries, so decide it when the
  manifest schema is fixed, not now.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green.
