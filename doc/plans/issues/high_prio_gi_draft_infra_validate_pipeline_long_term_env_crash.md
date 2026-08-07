## `validate_pipeline --target long-term` crashes: the validator never loads the `.env`, so `long-term-operational` exits 1 whenever the resolver's vars aren't exported (INFRA-021)

**Status**: Draft (2026-07-23)
**Module**: `apps/validate_pipeline` (+ `apps/run_locally.sh:run_in_venv`, `apps/iEasyHydroForecast/long_term_horizon_resolver.py`)
**Priority**: **High** (long-term validation never completes — no verdict is produced and quarter/season checks never run; the operational target's exit code is red in this configuration)
**Labels**: `infra`, `validation`, `long-term`, `env-loading`, `crash`
**Discovered**: 2026-07-23, local pipeline health review (taj, `maxat_sapphire_2` @ `16fb9a9b`).
**Related**:
- **INFRA-020** — ML module validation no-op (the false-PASS counterpart).
- **INFRA-022** — long-term Tier-1 FAILs on legitimately gated days (surfaced only once this crash is worked around).
- **LTF monthly horizon semantics** — introduced `long_term_horizon_resolver`.

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
- `long-term-operational` returns a non-zero exit code on every run in any
  environment where these variables are not exported into the shell. A cron/CI
  consumer sees a permanent failure, which trains operators to ignore the signal.
- The failure is an unhandled traceback, not a graceful validation failure, so it is
  indistinguishable from a genuine crash.

## Proposed fix (to be planned — pick one primary)

1. **Preferred: make `validate_pipeline.py` load the `.env`**, exactly as every
   module does, from `ieasyhydroforecast_env_file_path`. This is the single change
   that removes the whole class of problem rather than the three variables that
   happen to be needed today.
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

**Contract not to break:** the modules' own env loading is working correctly and must
not be changed. Do not gate `quarter_horizon_value()` on `SAPPHIRE_SKILL_LEAD_AWARE`
as a workaround — the resolver call is legitimate; the missing env loading is the bug.

## Acceptance criteria

- `bash apps/run_locally.sh long-term-operational` with only
  `ieasyhydroforecast_env_file_path` set completes with the long-term validation
  **executing** (non-zero check count) and no traceback.
- Removing a genuinely required config produces a graceful FAIL with the variable
  named, not a stack trace.
- Exit codes are specified and tested for each case: valid config + data present;
  valid config + records genuinely missing; required variable missing; malformed
  config; API unavailable. (Replaces the earlier untestable "exit code reflects real
  data state" phrasing.)
- Short-term / standalone validator invocations that currently work **without** an env
  file continue to work.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green, with a
  regression test that runs the long-term target with a minimal environment.

## Reproduction

```bash
cd apps/postprocessing_forecasts
env ieasyhydroforecast_env_file_path=<env> \
  ./.venv/bin/python ../validate_pipeline/validate_pipeline.py --target long-term
# -> LongTermHorizonResolverError traceback
```
