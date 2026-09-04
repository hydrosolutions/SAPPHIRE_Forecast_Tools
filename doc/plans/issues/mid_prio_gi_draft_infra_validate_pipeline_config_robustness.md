# INFRA-045: `validate_pipeline` robustness gaps exposed by the deployment-env fix

**Status**: Draft (2026-09-04)
**Module**: `apps/validate_pipeline/validate_pipeline.py` (+ its test suite)
**Priority**: **Medium** — none of these breaks the validator's happy path, and `validate_pipeline`
still has no production invoker (it runs only from `apps/run_locally.sh`, so this is a dev gate, not
an operational one). But four of the six turn operator misconfiguration into a Python traceback or a
plausible wrong answer, which is exactly the class of thing a *validator* exists to prevent.
**Labels**: `infra`, `validate_pipeline`, `robustness`, `exit-contract`
**Found**: 2026-09-03/04, across three out-of-loop review rounds on the fix that made
`validate_pipeline.py` load the deployment `.env` file (the fix for the
`LongTermHorizonResolverError: Required environment variable
'ieasyhydroforecast_ml_long_term_supported_modes' is not set` crash).
**Related**: the validate_pipeline cluster — **INFRA-020/021/022/025/026/027/028/031** (check
semantics and false passes) and **INFRA-024** (exit-code attribution). This issue is about
*configuration robustness and the exit contract*, not check semantics.

> **Line numbers below are deliberately sparse and must be re-derived with `grep -n` at
> implementation time.** The file was edited three times while these findings were being collected;
> symbol names are the stable reference.

---

> ## ⚠ PREREQUISITE — this issue describes code that is NOT on `maxat_sapphire_2` yet
>
> Every finding below assumes the **deployment-env-load fix** for `validate_pipeline.py` has landed:
> the `_load_deployment_env()` loader called from `main()`, the `critical` field on `CheckResult`,
> the critical-row guard in the `--phase pre` block, and the tests named in F4 and F6. **None of that
> exists on trunk.** It is committed on the unmerged branch `docs_fd024_fd025_doc008`
> (`fix: validate_pipeline never loaded the deployment env file`).
>
> On trunk today, `main()` goes straight from argument parsing to validation with no env load, and
> the `--phase pre` block writes its baseline and returns 0 unconditionally.
>
> **Do not start this issue before that fix is merged.** If it is abandoned or reworked, F2, F3, F4
> and F6 must be re-derived rather than implemented as written — F2 and F3 exist *because* the fix
> newly exposes env-file values to code that never had to tolerate a malformed one.

## Why these were split out rather than fixed inline

They surfaced while fixing a different bug. Per CLAUDE.md, related defects found while mapping
become their own plan file instead of growing the patch. Two findings from the same review rounds
**were** fixed in that patch, because they were regressions it had itself introduced: a critical
configuration failure being dropped by `--module` filtering, and a broken seasonal config
suppressing the quarter checks. Everything below either pre-dates that patch or is a new *exposure*
of a pre-existing weakness rather than a new defect.

## The module's contract, for reference

The module docstring states the whole contract:

```
Exit codes:
    0 — all checks passed (or skipped/warned)
    1 — at least one check FAILed
```

Four of the six findings are violations of it: the process exits via an uncaught traceback, or exits
0 having not performed the validation it was asked for.

---

## F1 — `--output-json` and `--baseline` pointing at the same file clobbers the baseline

**Severity: Minor** (needs an operator to pass the same path twice, which nothing in the repo does).

In `validate()`, the JSON-output block runs **before** the `--phase pre` block. The phase block was
recently hardened so that a critical row leaves the baseline untouched and prints
`baseline at … left unchanged`. But if `output_json_path == baseline_path`, the JSON write has
already replaced that file with the incomplete snapshot, and the message is then false.

**Fix**: move the critical-row check above the JSON write, or refuse at argument-parse time when the
two paths resolve to the same file. The second is cheaper to reason about and gives a better error.

**Test**: run `--phase pre` with `--output-json` and `--baseline` set to the same existing file and a
critical row present; assert the file is byte-identical afterwards.

## F1b — a RELATIVE env-file pointer is accepted by `run_locally.sh` and rejected by the validator

**Severity: Important. Introduced** by the env-loading fix (found in its final review round, 2026-09-04).

`run_locally.sh` validates the pointer with `[ ! -f "$ieasyhydroforecast_env_file_path" ]`
(`run_locally.sh:1633`) from **the operator's** working directory, passes it through verbatim
(`:626`), and then runs the child inside `( cd "$module_dir"; ... )` (`:639-643`). The validator
therefore resolves the same relative path against `apps/postprocessing_forecasts/`.

**Reproduced 2026-09-04**: with `relative_test.env` present in the repo root,

```
( cd apps/postprocessing_forecasts && ieasyhydroforecast_env_file_path=relative_test.env     ./.venv/bin/python ../validate_pipeline/validate_pipeline.py --target short-term )
```
→ `[FAIL] ieasyhydroforecast_env_file_path=relative_test.env does not exist or is not a readable
file (cwd=/…/apps/postprocessing_forecasts)`, exit 1.

Before the env-loading fix the pointer was never opened, so a relative value was silently ignored
and short-term validation still ran on defaults. It now fails the run. **The failure is loud and
names the cwd**, so it costs one log read rather than a debugging session — that is why it is filed
rather than hot-fixed — but a pointer the launcher accepted should not be rejected downstream.

**Fix belongs in `run_locally.sh`, not the validator**: canonicalise
`ieasyhydroforecast_env_file_path` to an absolute path once, at validation time (`:1626-1638`),
before exporting it at `:626`. Resolving it inside the validator would require guessing a base
directory. Note this makes the fix touch a file outside `apps/validate_pipeline/`, so it is a
separate phase.

**Test**: a relative pointer valid from the repo root must work end to end through
`run_locally.sh`, and the canonicalised absolute value must be what the child receives.

## F2 — loading the deployment env file makes malformed values newly crashable

**Severity: Important** — this is a *new exposure* created by the env-loading fix, even though the
fragile code is pre-existing.

Before that fix the validator never read the deployment file, so only these four variables reached
it, and each had a fallback: `FRESHNESS_THRESHOLD_DAYS`, `SAPPHIRE_PREDICTION_MODE`,
`SAPPHIRE_API_URL`, `SAPPHIRE_API_ENABLED`. Now the file supplies them, so a typo in the file
reaches code that never had to tolerate one:

- `int(os.environ.get("FRESHNESS_THRESHOLD_DAYS", …))` in the freshness check raises `ValueError` on
  a non-numeric value — an uncaught traceback.
- `SAPPHIRE_API_URL=not-a-url` reaches the client constructor in `validate()`; the SDK's URL
  validation raises, again uncaught.

**Fix**: validate these two values where they are read, and convert a bad value into a `[FAIL]` row
naming the variable, the offending value and the file it came from — not a traceback. A validator
that dies on a malformed config is failing at its own job.

**Tests**: a deployment env file with `FRESHNESS_THRESHOLD_DAYS=abc`, and one with a malformed
`SAPPHIRE_API_URL`; both must exit 1 with a `[FAIL]` naming the variable, and neither may print a
traceback.

## F3 — an invalid `SAPPHIRE_PREDICTION_MODE` silently validates the wrong horizon

**Severity: Important. Pre-existing**, but reachable from the env file now.

`resolve_horizons` ends with `MODE_TO_HORIZONS.get(mode, ["pentad"])`. An unrecognised, **non-empty**
mode — `DECADES` instead of `DECAD`, say — silently selects pentad. Healthy pentad data then makes
the run exit 0 while the decade validation the operator asked for never happened.

This is the "silent fallback" shape: it fails reassuringly. An absent mode defaulting to pentad is
defensible; a *present but unrecognised* one is not — it is a typo the operator wants to hear about.

**Fix**: distinguish the two. Unset → keep today's default. Set but not in `MODE_TO_HORIZONS` →
`[FAIL]` naming the value and the accepted set, exit 1. Do not silently substitute.

**Test**: `SAPPHIRE_PREDICTION_MODE=DECADES` exits 1 naming the bad value; unset still defaults to
pentad with no warning.

## F4 — `check_presence` can still exit the process with a traceback

**Severity: Important. Pre-existing.**

`check_presence` guards its API call, but the pandas work after it is unguarded: a response whose
`date` column is duplicated makes `pd.to_datetime` raise `ValueError: cannot assemble with duplicate
keys`, which escapes to the CLI as a traceback.

A test currently pins this propagation
(`test_check_presence_valueerror_not_mislabelled_as_horizon_config`). **That test is pinning the
useful half only** — that the error is not mislabelled as a horizon-configuration failure — and its
docstring says so explicitly. **When this issue is implemented, update that test; do not treat it as
a blocker.** It was written knowing this issue would be filed.

**Fix**: a malformed API response should become a `[FAIL]` row naming the check and the response
problem, exit 1. Same contract as everything else.

## F5 — `--phase pre` returns 0 over ordinary FAIL rows: **the docstring is wrong, not the code**

**Severity: Minor (documentation). Pre-existing. DECIDED 2026-09-04.**

The `--phase pre` block writes the baseline and returns 0 even when `all_results` contains FAIL rows.
The owner asked the right question: *"if it just checks what is there, how can it fail?"*

Reading the code answers it. `--phase pre` is not a passive recorder — it runs the **full check
suite**, writes the results as a baseline, and returns 0. `--phase post` re-runs the same checks,
loads the baseline, and prints a delta report (`validate_pipeline.py:1343-1510`). The pair exists to
answer *"what did this pipeline run change?"*, so the failures the pre phase finds are **pre-existing
conditions in the data, not faults in the snapshot**. Reporting success means "the snapshot was
taken", which is the honest statement for that mode.

**Decision: keep the behaviour, fix the documentation.** **Two** docstrings promise it — the module docstring (`:17-19`) and `main()`'s own
(`:1513` on trunk) — and both must be amended. They promise `0 — all checks passed (or skipped/warned)` with no
exception, and that promise is what is false. Amend both to state that `--phase pre` returns 0 when the
baseline was written successfully, regardless of check outcomes, and that judging the data is the
post phase's job.

Do **not** change the exit code. Making `--phase pre` return 1 on ordinary FAIL rows would make a
pre-run snapshot of an already-imperfect deployment look like a failed command, which is precisely
the false-alarm shape this cluster is trying to remove.

*(A critical row — the requested validation could not be performed — is different and already
forces a non-zero exit there; that is not affected by this decision.)*

## F6 — the ambient-environment isolation tests are vacuous

**Severity: Minor. Introduced by the env-loading patch's own tests.**

`test_env_file_pointer_absent_by_default` and `test_ambient_env_vars_absent_by_default` observe
`os.environ` *after* the autouse fixture has run. In a clean CI environment those variables are
absent anyway, so both tests pass even if the fixture is gutted — they do not protect the
developer-shell isolation they were written for.

**Fix**: set the variables to a poison value in the test process, then assert that a *representative*
test still behaves correctly — i.e. exercise the isolation, don't observe its outcome. A
`subprocess` run of a small selection of the suite with the poison variables exported is the honest
form.

**Acceptance for this one specifically**: the new test must fail if any single entry is removed from
the fixture's variable list.

---

## Files that may be modified

- `apps/validate_pipeline/validate_pipeline.py`
- `apps/validate_pipeline/test/test_validate_pipeline.py`
- `apps/validate_pipeline/test/conftest.py` (F6 only)
- `apps/run_locally.sh` (**F1b only** — the pointer canonicalisation; no other change)

**Do not** change check semantics, thresholds, or which checks run — that is the INFRA-020..031
cluster's territory, not this issue's. This issue only changes what happens when *configuration* is
wrong.

## Acceptance criteria

- [ ] No malformed value of any of the four environment variables the validator reads produces a
      traceback; each produces a `[FAIL]` naming the variable and its value.
- [ ] `grep -rn "Traceback" ` over a run matrix covering: bad `FRESHNESS_THRESHOLD_DAYS`, bad
      `SAPPHIRE_API_URL`, unrecognised `SAPPHIRE_PREDICTION_MODE`, duplicate-key API response,
      `--output-json` == `--baseline` — returns nothing.
- [ ] F5 is a docstring change only; `--phase pre`'s exit code is unchanged for ordinary FAIL rows.
- [ ] The F6 test fails when any one variable is dropped from the fixture list.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.
- [ ] `ruff check` / `ruff format --check` clean on changed files.

## Phases

- **P1 — malformed config values (F2, F3).** Files: `validate_pipeline.py`, test file.
  Depends on: none. Agents: 1. Accept: F2 and F3 tests pass; no traceback in the run matrix.
- **P2 — malformed API response (F4).** Files: `validate_pipeline.py`, test file. Depends on: none
  (independent of P1). Agents: 1. Accept: F4 test passes **and
  `test_check_presence_valueerror_not_mislabelled_as_horizon_config` is updated, not deleted**.
- **P3 — path collision (F1) and the phase-pre docstring (F5).** Files: `validate_pipeline.py`,
  test file. Depends on: none. Agents: 1.
- **P4 — non-vacuous isolation tests (F6).** Files: test file, `conftest.py`. Depends on: none.
  Agents: 1. Accept: the drop-one-variable check fails as required.
- **P5 — relative-pointer canonicalisation (F1b).** Files: `apps/run_locally.sh` + its test
  harness. Depends on: none. Agents: 1. Accept: a relative pointer valid from the repo root
  works end to end; the child receives an absolute path.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": [], "parallel_agents": 1 },
    "P3": { "depends_on": [], "parallel_agents": 1 },
    "P4": { "depends_on": [], "parallel_agents": 1 },
    "P5": { "depends_on": [], "parallel_agents": 1 }
  }
}
```

## Out of scope

- Check semantics, freshness thresholds, and false-pass behaviour — INFRA-020..031.
- The long-term "no records" FAIL on a non-forecast day: **not a defect**. No long-term forecast is
  due on most days; the row is correct. Whether the validator should be schedule-aware is INFRA-028.
- Giving `validate_pipeline` a production invoker — INFRA-031.
