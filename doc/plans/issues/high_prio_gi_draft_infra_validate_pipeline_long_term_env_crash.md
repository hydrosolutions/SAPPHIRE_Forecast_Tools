## `validate_pipeline --target long-term` crashes: the validator never loads the `.env`, so `long-term-operational` exits 1 whenever the resolver's vars aren't exported (INFRA-021)

**Status**: Draft (2026-07-23) — **diagnosis confirmed; scope enlarged 2026-08-18 by out-of-loop
review. Must ship atomically with INFRA-022 + forecast-date propagation — see § "Scope change"
before planning.**
**Module**: `apps/validate_pipeline` (+ `apps/run_locally.sh:run_in_venv`, `apps/iEasyHydroForecast/long_term_horizon_resolver.py`)
**Priority**: **High** (long-term validation never completes — no verdict is produced and quarter/season checks never run; the operational target's exit code is red in this configuration)
**Labels**: `infra`, `validation`, `long-term`, `env-loading`, `crash`
**Discovered**: 2026-07-23, local pipeline health review (taj, `maxat_sapphire_2` @ `16fb9a9b`).
**Field evidence caveat**: the dated tjhm/kghm observations, timings and result counts in this
draft are **external evidence** — both 2026-08-18 verifier passes marked them *unverifiable* from
the repository, which holds no run logs, deployment `.env` files or API snapshots for those
sessions. Source-level claims were verified independently; the field observations were not, and
source inspection shows that exporting the three variable *names* alone is not generally
sufficient (the supported-mode list and valid config JSON with integer lead fields must also exist).
**Related**:
- **INFRA-020** — ML module validation no-op (the false-PASS counterpart).
- **INFRA-022** — long-term Tier-1 FAILs on legitimately gated days. **Now a hard prerequisite,
  not a follow-on** — see § "Scope change after out-of-loop review".
- **INFRA-025** — `iEasyHydroForecast` package shadowing (previously miscited here as INFRA-023).
- **LTF monthly horizon semantics** — introduced `long_term_horizon_resolver`.


## Reproduction 2026-08-17 — both organisations, both on trunk `8e3fc1bc`

Confirmed twice this session with the identical traceback, which narrows this from a possible
deployment condition to a **code defect**:

```
run_tier1_long_term
  -> quarter_horizon_value()                    long_term_horizon_resolver.py:70
  -> _ensure_supported_mode(config_name)                                  :173
  -> supported_long_term_modes()                                          :54
```

| Run | Org | Outcome |
|---|---|---|
| 2026-08-15 13:03, `long-term-operational` (`--continue-on-error`) | **kghm** | all 4 LT phases PASS, `api_validation (long-term)` **FAIL** |
| 2026-08-17 14:32, `long-term-operational` (`LT_FORECAST_TODAY=2026-08-01`) | **tjhm** | all 6 modules PASS, `api_validation (long-term)` **FAIL** |

Two additional observations worth keeping:

1. **It is horizon-scoped, not global.** Per-module validations in the same sessions succeeded
   (`--module preprocessing_runoff`, `--module preprocessing_gateway`,
   `--module linear_regression`, `--module postprocessing_forecasts`) because none of them enter
   the long-term tier. So the blast radius is exactly the long-term verification path — which is
   also the path with the least manual scrutiny.
   **Correction (2026-08-18 review):** it is *not* only `--target long-term`. `--target all`
   enters the same path (`validate_pipeline.py:1429`), and `main` auto-infers the long-term target
   for `--module long_term_forecasting` (`:1578-1583`). All three routes crash identically.

2. **The crash masks everything downstream of it.** None of the § 9.6 long-term skill/ensemble
   verification executes. Any claim that "long-term validation passed" in a prior review is
   unsupported for as long as this stands — the checks never ran.
   **Correction (2026-08-18 review):** the earlier wording "it raises before any long-term check
   runs" was wrong, and contradicted this draft's own § "Why it matters". Monthly forecast presence
   and monthly skill-metric presence **do execute** and append results (`:519-538`); the resolver
   is called only afterwards at `:539`. Those two results are then discarded unprinted because
   `run_tier1_long_term` never returns. The distinction matters for the fix: partial results
   already exist at the point of failure and a correct implementation must decide what to do with
   them, rather than assuming there is nothing to preserve.

**Interaction with INFRA-025 (package shadowing).** Until the `iEasyHydroForecast` package
shadowing was cleared *in the affected working copy*, `validate_pipeline` died at *import* and this
crash was unreachable. **Corrected 2026-08-18:** this section previously called that issue
`INFRA-023`, which is a different issue entirely (the yearly `monthly_norms` cron mapping,
`module_issues.md:60`); package shadowing is **INFRA-025** (`:117`) and is still `Draft`. Note also
that INFRA-025 is an environment hazard, not a code state — the shadowing directory is absent from
a clean checkout, so tracing INFRA-020/021 in source is *not* gated on it; only reproducing them in
a contaminated working copy is.


---

## Symptom

`bash apps/run_locally.sh long-term-operational` **exits 1**, while its
`PIPELINE SUMMARY` shows **all six modules PASS**:

```
[OK]   preprocessing_runoff: PASS (52s)
[OK]   preprocessing_gateway: PASS (31s)
[OK]   long_term_forecasting (operational): PASS (2m 54s)
[OK]   postprocessing_forecasts (long-term operational): PASS (1m 0s)
[OK]   postprocessing_forecasts (long-term skill metrics): PASS (1m 51s)
[OK]   postprocessing_forecasts (long-term maintenance): PASS (45s)
[ERROR]   api_validation (long-term): FAIL (0s)
```

The validation step dies with an **unhandled traceback**:

```
iEasyHydroForecast.long_term_horizon_resolver.LongTermHorizonResolverError:
Required environment variable 'ieasyhydroforecast_ml_long_term_supported_modes' is not set.
```

…even though that variable **is** set in the deployment `.env`.

## Root cause (traced, then proven by controlled experiment)

1. **`validate_pipeline.py` never loads the `.env`.** There is no `load_dotenv` /
   `load_environment` call anywhere in the file. It sees only the process
   environment it inherits.
2. **`run_in_venv` sets only two variables explicitly** —
   `ieasyhydroforecast_env_file_path` and `SAPPHIRE_PREDICTION_MODE` (`run_locally.sh`,
   `run_in_venv` env block). Note it uses `env` **without `-i`**, so the parent shell's
   environment *is* inherited; the crash therefore occurs only when the resolver's
   variables are absent from the caller's environment — which is the normal case when
   they live in the `.env` file rather than being exported. Every *module* copes
   regardless because each loads the `.env` itself (`sl.load_environment()`); the
   validator does not.
3. **`run_tier1_long_term` calls `quarter_horizon_value()` unconditionally** at
   `validate_pipeline.py:539` — it is **not** gated on `SAPPHIRE_SKILL_LEAD_AWARE`,
   so this is independent of the lead-aware rollout.
4. That resolver hard-requires a **chain** of variables, each raising
   `LongTermHorizonResolverError` via `_required_env` (`long_term_horizon_resolver.py:210`):
   `ieasyhydroforecast_ml_long_term_supported_modes` (`:54`) →
   `ieasyforecast_configuration_path` (`:202`, **note the different prefix** —
   `ieasyforecast_`, not `ieasyhydroforecast_`) →
   `ieasyhydroforecast_ml_long_term_configuration`.

### Causation proof

| Invocation | Result |
|-----------|--------|
| `env ieasyhydroforecast_env_file_path=<env> python validate_pipeline.py --target long-term` | **crash** (reproduces exactly) |
| same + the three resolver variables exported | **runs to completion**: `3 passed, 3 failed, 3 warned, 10 skipped` |

## Why it matters

- The long-term validation **never completes**. The checks preceding
  `quarter_horizon_value()` (monthly forecast presence, monthly skill metrics) do run
  and build results, but the crash aborts Tier 1 before any of them is printed or
  reaches the summary — so no long-term verdict is ever produced, and the quarter and
  season checks never run at all.
- `long-term-operational` returns a non-zero exit code in any environment where these
  variables are not exported into the shell. A cron/CI consumer sees a permanent
  failure, which trains operators to ignore the signal.
  **Corrected 2026-08-18:** not "on every run". The caller returns *before* validation
  for skipped organizations (`run_locally.sh:1184-1190`) and for dates with no active
  long-term modes (`:1202-1205`), and the validator itself exits 0 when the API client
  is absent or `SAPPHIRE_API_ENABLED=false` (`validate_pipeline.py:1589-1597`).
- The failure is an unhandled traceback, not a graceful validation failure, so it is
  indistinguishable from a genuine crash.

---

## Scope change after out-of-loop review (2026-08-18) — READ BEFORE PLANNING

Two independent read-only `codex exec` passes confirmed the diagnosis (no env loading; the
resolver call at `:539` is unconditional) and then found that **fixing it alone makes the
signal worse, not better**. This issue can no longer be planned as "add env loading".

**The env crash is the only thing currently suppressing a stream of false FAILs.**
`run_tier1_long_term` demands month, quarter **and** season output unconditionally
(`validate_pipeline.py:519-583`), and the "never skip long-term" behavior is *deliberately
locked* by a test:

```python
# test_validate_pipeline.py:963-983
def test_long_term_never_skipped(self):
    """We can't predict long-term schedule, so we don't downgrade FAILs."""
    ...
    assert results[0].status == "FAIL"
```

Meanwhile the real scheduler selects modes per issue-day and forecast-month
(`long_term_forecasting/lt_schedule_query.py:88-131`). So the moment env loading works:

| Scenario | Verdict |
|---|---|
| `demo` / `uzhm` — long-term generation skipped, but `run_all` still calls validation `--target all` (`run_locally.sh:1228-1238`) | **false FAIL** |
| A closed issue day (e.g. day 13 where modes are configured for days 1 and 25) | **false FAIL** |
| A quarter mode not scheduled in the current month | **false FAIL** |
| `LT_FORECAST_TODAY=2026-08-01` invoked on 2026-08-17 — override reaches generation (`:656-659`) but not validation, which defaults to `date.today` (`validate_pipeline.py:1586`) | **false FAIL** |
| Standalone `long_term_forecasting` simulating `LT_SIMULATE_YEARS=2024`, then validated against the current date (`:583-613`, `:1960-1965`) | **false FAIL** |

**Consequence:** INFRA-021 must land **atomically with INFRA-022's schedule-aware gating and with
forecast-date propagation** (`--forecast-date` plumbed from `LT_FORECAST_TODAY` / the simulation
year). Landing env loading on its own converts one loud traceback into recurring false alarms —
strictly worse, because the traceback at least cannot be ignored as noise. This contradicts the
CLAUDE.md forecast-date rule (§ "The Forecast Date Rule"), which requires the date to be captured
once and passed as a parameter.

**Re-pricing:** the tracker entry reads like a small env-loading fix. It is not. Scope is now
`INFRA-021 + INFRA-022 + forecast-date propagation`, with one locked test to renegotiate.

## Proposed fix (to be planned — pick one primary)

1. **Preferred: make `validate_pipeline.py` load the `.env`** from
   `ieasyhydroforecast_env_file_path`.
   **Corrected 2026-08-18 — do NOT do this "exactly as every module does".** The shared
   `setup_library.load_environment` is not a lightweight dotenv loader: it selects default files
   when no path is supplied (`setup_library.py:433-450`), raises on a missing file, mutates
   `IEASYHYDRO_HOST` (`:458-485`), and runs `validate_environment_variables` (`:493-498`). Calling
   it unchanged would violate this draft's own compatibility contract below. Specify a **minimal
   dotenv load** instead, and note two omissions this draft had:
   - `python-dotenv` is **not** a declared dependency of `validate_pipeline`
     (`pyproject.toml` lists `pandas` only) — the fix needs a dependency and lock-file change.
   - Loading must happen **before** the API URL and `SAPPHIRE_API_ENABLED` are read
     (`validate_pipeline.py:1365-1369`, `:1589-1597`), not next to the resolver, or the env file
     cannot influence API configuration.
   **Precedence contract to specify and test:** already-exported process variables
   must win over `.env` values; load only when an env path is supplied; and an
   absent/invalid env path must not break the short-term or standalone targets that
   work today without one.
2. Alternative/complementary: have `run_in_venv` forward the resolver's required
   variables. *Rejected as primary* — it re-hardcodes an env list that will drift
   again the next time the resolver gains a dependency.
3. **Independently**: make the long-term Tier-1 path degrade gracefully. A missing
   config/env should produce a validation **FAIL with a clear message**, never an
   unhandled traceback that aborts the remaining checks.
   **Refined 2026-08-18:** a single broad `try/except` around `run_tier1_long_term` is not
   sufficient — it would still discard the two monthly results that already exist at the point of
   failure (`:519-538`). Per-check exception boundaries are needed, plus an explicit policy for
   each failure class: `LongTermHorizonResolverError` (missing variable) and malformed
   JSON/invalid integer fields should **FAIL**, whereas `UnsupportedLongTermModeError` on a
   deployment that legitimately does not run quarter/season should **SKIP**
   (`long_term_horizon_resolver.py:172-178`). Treating "unsupported product" and "broken config"
   alike just creates a different false signal.

**Contract not to break:** the modules' own env loading is working correctly and must
not be changed. Do not gate `quarter_horizon_value()` on `SAPPHIRE_SKILL_LEAD_AWARE`
as a workaround — the resolver call is legitimate; the missing env loading is the bug.

## Acceptance criteria

*(Revised 2026-08-18 after out-of-loop review.)*

- `bash apps/run_locally.sh long-term-operational` with only
  `ieasyhydroforecast_env_file_path` set completes with the long-term validation
  **executing** (non-zero check count) and no traceback.
  **Not mechanically checkable as written** — the caller returns before validation for skipped
  orgs (`run_locally.sh:1184-1190`) and for dates with no active modes (`:1202-1205`), so a run
  can "complete" having validated nothing. The test must assert a **non-zero registered check
  count**, not command success.
- Removing a genuinely required config produces a graceful FAIL with the variable
  named, not a stack trace.
- Exit codes are specified and tested for each case: valid config + data present;
  valid config + records genuinely missing; required variable missing; malformed
  config; **API client absent or `SAPPHIRE_API_ENABLED=false` (must stay exit 0, per
  `validate_pipeline.py:1589-1597` and its locked tests at `test_validate_pipeline.py:129-140`);
  API configured but readiness failing (FAIL results, `:1371-1396`)**. The earlier single
  "API unavailable" row conflated the last two and was undecidable.
- A deployment that does not configure quarter/season **SKIPs** those checks rather than failing
  them; a deployment whose config is malformed **FAILs**. Per-mode gating comes from INFRA-022.
- The validated date is the date that was **forecast**, not `date.today` — `--forecast-date` is
  plumbed from `LT_FORECAST_TODAY` and from the standalone simulation year.
- Short-term / standalone validator invocations that currently work **without** an env
  file continue to work.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green, with a
  regression test that runs the long-term target with a minimal environment.
  **Two masks must be defeated or the test passes before *and* after the fix:** the autouse
  fixture at `test_validate_pipeline.py:36-59` installs all three resolver variables (it must be
  overridden/deleted for this test), and a missing `sapphire_api_client` returns 0 before
  validation is reached (`:1589-1592`), so the API path must be forced to execute with fakes.
- `test_long_term_never_skipped` (`test_validate_pipeline.py:963-983`) is updated deliberately as
  part of INFRA-022's gating, with a comment naming the issue — never silently edited to fit.

## Reproduction

```bash
cd apps/postprocessing_forecasts
env ieasyhydroforecast_env_file_path=<env> \
  ./.venv/bin/python ../validate_pipeline/validate_pipeline.py --target long-term
# -> LongTermHorizonResolverError traceback
```
