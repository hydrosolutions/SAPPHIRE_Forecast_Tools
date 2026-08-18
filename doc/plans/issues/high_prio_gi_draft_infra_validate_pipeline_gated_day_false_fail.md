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

- **Observability** runs one way: this defect is only *visible* after INFRA-021's crash is fixed,
  because the process currently dies before Tier 1 emits anything.
- **Delivery** runs the other way: INFRA-021 must **not ship without this gating**, because fixing
  the crash alone converts one traceback into recurring false FAILs on every legitimately gated day
  and on every deployment that does not run long-term at all.

So the two are **one atomic change**, not a queue. Neither blocks the other; they land together.
**Related**: **INFRA-020** (false-PASS counterpart — same class of defect, opposite sign).

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

`_apply_non_forecast_day_skip()` maps the long-term horizon to a constant `True`,
i.e. "always treat as a forecast day", so long-term FAILs are **never** downgraded
(`validate_pipeline.py:1325`):

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

- A correct, quiet day produces FAIL lines. Combined with INFRA-021's non-zero exit,
  the long-term target looks broken on every ordinary day.
- Alarm fatigue: once operators learn the long-term validation "is always red", a
  genuine long-term outage will not be noticed.

## Proposed fix (to be planned)

1. Implement the long-term downgrade using a per-mode schedule authority. Decide activity
   **separately for every checked mode/horizon** and tag each result with its owning mode — do not
   collapse to one deployment-level gate.

   > **OPEN DECISION — which schedule authority? (raised 2026-08-18, third out-of-loop pass.)**
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
   > | (d) | extract the mode-activity decision into `iEasyHydroForecast` beside the resolver; `lt_schedule_query` and `validate_pipeline` both call it | most up-front work, removes the drift risk permanently. Note `validate_pipeline` **already** imports from `iEasyHydroForecast` (`validate_pipeline.py:35-49`), so this adds no new dependency direction |
   >
   > **DECIDED 2026-08-18: option (d).** The extraction is planned in
   > `doc/plans/working/lt_schedule_authority_extraction_plan.md`.
   >
   > **But (d) alone is not sufficient for this issue.** Out-of-loop review of that plan established
   > that a re-derived schedule — however well single-sourced — cannot see manual overrides
   > (`pipeline_docker.py:2339-2367` bypasses `LTScheduleQuery`), execution outcome, or the date
   > output was actually written under (late forecasts snap back, `lt_utils.py:211-217`). Gating
   > therefore consumes a **run manifest**, filed as **INFRA-028**, which this issue now depends on.
   > See also **LTF-007**: the scheduler's 10-day window vs execution's 5-day gate means gating
   > built on the scheduler alone false-FAILs days 6-10 by construction.
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
  a day inside the **issue-day tolerance window** is treated as an issue day. *Both depend on the
  open schedule-authority decision above; neither is achievable with `long_term_horizon_resolver`
  alone.*
- A deployment that runs **no** long-term modes at all (demo, uzhm) produces no long-term FAILs,
  including via `run_all`'s unconditional `--target all` (`run_locally.sh:1228-1238`).
- `test_long_term_never_skipped` is **intentionally superseded**: it locks the current
  premise and must be replaced (not deleted silently) by tests asserting per-mode
  gating, so the behaviour change is explicit and reviewed.
- **Shared-fixture impact (flagged 2026-08-18):** the autouse fixture at
  `test_validate_pipeline.py:36-59` writes long-term configs carrying only
  `operational_month_lead_time` — **no `operational_issue_day`** — while schedule-aware gating
  needs both (`long_term_horizon_resolver.py:112-142`). Introducing gating will therefore also
  disturb the existing long-term mapping / empty-quarter / empty-season / all-present tests
  (`:418-477`). Update the fixture and those dates deliberately, in the same commit.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green.
