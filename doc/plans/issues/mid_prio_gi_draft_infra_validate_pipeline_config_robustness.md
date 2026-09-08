# INFRA-045: `validate_pipeline` robustness gaps exposed by the deployment-env fix

**Status**: Draft (2026-09-04). Prerequisite satisfied and all seven findings re-verified
2026-09-08; **four owner decisions below must be resolved before this becomes `Ready`** (see
§ Owner decisions). Status vocabulary is owned by `doc/plans/README.md` — this is not `Ready` yet.
**Module**: `apps/validate_pipeline/validate_pipeline.py` (+ its test suite)
**Priority**: **Medium** — none of these breaks the validator's happy path, and `validate_pipeline`
still has no production invoker (it runs only from `apps/run_locally.sh`, so this is a dev gate, not
an operational one). But four of the seven turn operator misconfiguration into a Python traceback or a
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

> ## ✅ PREREQUISITE SATISFIED — verified on trunk 2026-09-08
>
> Earlier revisions of this file carried a blocking box saying the deployment-env-load fix was not
> on `maxat_sapphire_2` and that this issue must not be started. **That fix merged as PR #486.**
> Confirmed present on trunk today: `_load_deployment_env()` called from `main()`, the `critical`
> field on `CheckResult` (`validate_pipeline.py:180`), the critical-row guard in the `--phase pre`
> block, and the tests F4 and F6 refer to.
>
> **This issue is unblocked.** The box is kept as a corrected note rather than deleted so that a
> reader who remembers the warning can see it was resolved, not silently dropped.

### Findings re-verified against trunk, 2026-09-08

Every finding below was re-checked after INFRA-030 (#497) and INFRA-044 (#498) merged. All are
still live. Line numbers move constantly in these two files — the citations here were re-derived on
that date and must be re-derived again with `grep -n` at implementation time.

| | still live? | evidence on trunk |
|---|---|---|
| F1 | yes | JSON-output block still precedes the `--phase pre` block in `validate()` |
| F1b | yes | see the corrected citations in that section — all four of its original ones were stale |
| F2 | yes | `int(os.environ.get("FRESHNESS_THRESHOLD_DAYS", ...))` unguarded, `validate_pipeline.py:1081` |
| F3 | yes | `return MODE_TO_HORIZONS.get(mode, ["pentad"])`, `validate_pipeline.py:1349` |
| F4 | yes | `check_presence`'s `try/except` covers the API call only; `pd.to_datetime(df["date"], ...)` sits after it, unguarded |
| F5 | yes (docs only) | decision already taken 2026-09-04; two docstrings still promise the unqualified exit contract |
| F6 | yes | both `test_env_file_pointer_absent_by_default` and `test_ambient_env_vars_absent_by_default` still observe absence rather than exercising the fixture |

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
**Scope widened 2026-09-08 after out-of-loop review — `--phase post` is the worse case.**

> The JSON-output write precedes **both** phase branches (`validate_pipeline.py:1535`). Under
> `--phase post` the write destroys the baseline **immediately before it is loaded**, so the
> comparison uses the current snapshot as its own baseline and reports **no deltas** — a silent
> "nothing changed" on a run where anything may have changed. That is worse than the `pre` case this
> finding was originally written about, because it produces a confident wrong answer rather than a
> false message.
>
> Reject the collision for `pre` **and** `post`, and detect **aliases** — resolved paths plus
> `os.path.samefile()` when both exist — not merely identical argument strings.

In `validate()`, the JSON-output block runs **before** the `--phase pre` block. The phase block was
recently hardened so that a critical row leaves the baseline untouched and prints
`baseline at … left unchanged`. But if `output_json_path == baseline_path`, the JSON write has
already replaced that file with the incomplete snapshot, and the message is then false.

**Fix**: move the critical-row check above the JSON write, or refuse at argument-parse time when the
two paths resolve to the same file. The second is cheaper to reason about and gives a better error.

**Test**: run `--phase pre` with `--output-json` and `--baseline` set to the same existing file and a
critical row present; assert the file is byte-identical afterwards.

## F1b — a RELATIVE env-file pointer is accepted by `run_locally.sh` and rejected by the validator

**Severity: Important. Newly *exposed*, not introduced**, by the env-loading fix (found in its final
review round, 2026-09-04; attribution corrected 2026-09-08 after out-of-loop review).

> The first wording — "introduced by the env-loading fix" — was too broad. Ordinary pipeline modules
> already call `setup_library.load_environment()`, which resolves the same relative pointer from
> their own module directory, so the launcher-wide relative-path defect pre-dates that fix. What the
> fix did was add one more consumer that now *fails loudly* on it. Fix it at the launcher boundary
> for all consumers, not for the validator alone.

`run_locally.sh` validates the pointer with `[ ! -f "$ieasyhydroforecast_env_file_path" ]`
(**`run_locally.sh:1802`**, re-derived 2026-09-08) from **the operator's** working directory, passes
it through verbatim in `run_in_venv`'s `env_cmd` (**`:653`**), and then runs the child inside
`( cd "$module_dir"; ... )` — the child changes directory and executes at **`:667`** (`~:656-658`
appends extra environment entries and was a mis-citation in the first correction of this file). The validator therefore resolves the same relative
path against `apps/postprocessing_forecasts/`.

> **All four line numbers in the original text of this finding were stale** (`:1633`, `:626`,
> `:639-643`, `:1626-1638`) — `run_locally.sh` has since gained the INFRA-030 `SKIP` status and the
> INFRA-044 exit-6 branch. The mechanism is unchanged and was re-confirmed on trunk; only the
> citations moved.

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
`ieasyhydroforecast_env_file_path` to an absolute path once, in the `validate_env` block that
already checks it (**`~:1799-1806`**), before it is passed through at **`:653`**. Resolving it inside the validator would require guessing a base
directory. Note this makes the fix touch a file outside `apps/validate_pipeline/`, so it is a
separate phase.

**Test**: a relative pointer valid from the repo root must work end to end through
`run_locally.sh`, and the canonicalised absolute value must be what the child receives.

## F2 — loading the deployment env file makes malformed values newly crashable

**Severity: Important** — this is a *new exposure* created by the env-loading fix, even though the
fragile code is pre-existing.

**History corrected 2026-09-08 after out-of-loop review.** The earlier claim that "only these four
variables reached it" is false: `env` is not invoked with `-i`, so **any exported process variable
already reached the child** before PR #486. The accurate distinction is that the file can now
populate values that were previously *unexported*. The validator's imported long-term resolver also
reads three mandatory variables (`long_term_horizon_resolver.py:52`, `:201`) — but those are already
converted into **critical FAIL rows** by the guards at `validate_pipeline.py:554`, so they are not
additional traceback cases and are out of F2's scope.

The four the validator reads directly, each with a fallback, are: `FRESHNESS_THRESHOLD_DAYS`, `SAPPHIRE_PREDICTION_MODE`,
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

**Severity: Important. Pre-existing.** Reachability corrected 2026-09-08 after out-of-loop review.

> **The earlier claim "reachable from the env file now" is FALSE.** `run_in_venv` always supplies
> `SAPPHIRE_PREDICTION_MODE` to the child — even as an empty string (`run_locally.sh:653`) — and the
> validator's `load_dotenv` uses `override=False` (`validate_pipeline.py:1583`), so an env-file value
> can never replace the launcher-supplied one. The launcher additionally rejects invalid *process*
> values for the primary short-term/all/LR targets (`run_locally.sh:1879`).
>
> F3 is still live, by two routes: **direct CLI invocation** of the validator, and **`target=daily`**,
> which deliberately bypasses that upstream mode check. State the impact with those qualifications —
> the unqualified "a typo in the env file silently validates the wrong horizon" is not true.

`resolve_horizons` ends with `MODE_TO_HORIZONS.get(mode, ["pentad"])`. An unrecognised, **non-empty**
mode — `DECADES` instead of `DECAD`, say — silently selects pentad. Healthy pentad data then makes
the run exit 0 while the decade validation the operator asked for never happened.

This is the "silent fallback" shape: it fails reassuringly. An absent mode defaulting to pentad is
defensible; a *present but unrecognised* one is not — it is a typo the operator wants to hear about.

**Fix**: distinguish the two. Unset → keep today's default. Set but not in `MODE_TO_HORIZONS` →
`[FAIL]` naming the value and the accepted set, exit 1. Do not silently substitute.

**Test**: `SAPPHIRE_PREDICTION_MODE=DECADES` exits 1 naming the bad value; unset still defaults to
pentad with no warning **for every target except `daily`**.

**Plus, per owner decision D1**: `--target daily` resolves `["pentad", "decade"]` from the target
itself regardless of the ambient mode. Test it two ways — with the mode unset (today's silent
pentad-only path) and with it set to `PENTAD` — and assert decade checks actually ran in both. A
test asserting only "no error" would pass against the current broken behaviour.

## F4 — `check_presence` can still exit the process with a traceback

**Severity: Important. Pre-existing.**

`check_presence` guards its API call, but the pandas work after it is unguarded: a response whose
`date` column is duplicated makes `pd.to_datetime` raise `ValueError: cannot assemble with duplicate
keys`, which escapes to the CLI as a traceback.

A test currently pins this propagation
(`test_check_presence_valueerror_not_mislabelled_though_still_propagates` — the name given in earlier revisions of this file, `..._as_horizon_config`, does not exist). **That test is pinning the
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
loads the baseline, and prints a delta report (`validate_pipeline.py:1564-1575`). The pair exists to
answer *"what did this pipeline run change?"*, so the failures the pre phase finds are **pre-existing
conditions in the data, not faults in the snapshot**. Reporting success means "the snapshot was
taken", which is the honest statement for that mode.

**Decision: keep the behaviour, fix the documentation.** **Two** docstrings promise it — the module docstring (`:17-19`) and `main()`'s own
(`main()` begins at `:1659` on trunk 2026-09-08) — and both must be amended. They promise `0 — all checks passed (or skipped/warned)` with no
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
- `apps/pipeline/tests/test_run_locally_orchestration.py` (**F1b only** — this is the launcher's
  actual test harness, which P5 must modify; it was missing from this list until 2026-09-08)

**Do not** change check semantics, thresholds, or which checks run — that is the INFRA-020..031
cluster's territory, not this issue's. This issue only changes what happens when *configuration* is
wrong.

> **One explicit, owner-approved exception (D1, 2026-09-08)**: `target=daily` derives
> `["pentad", "decade"]` from the target. That *does* change which checks run, and is allowed here
> only because the owner decided it after being shown the trade-off. It is not a licence to widen
> the rest of the issue — every other finding stays inside the configuration-robustness boundary.

## Acceptance criteria

- [ ] No malformed value of the environment variables in F2's inventory produces a traceback; each
      produces a `[FAIL]` naming the variable and its value.
- [ ] Each case below is proven by its **own subprocess assertion** over combined stdout+stderr,
      asserting the exact exit code and, where relevant, that the target file was preserved — not by
      a repo-wide `grep -rn "Traceback"`, which is ambiguous and cannot show the intended validation
      actually ran: bad `FRESHNESS_THRESHOLD_DAYS`, bad `SAPPHIRE_API_URL`, unrecognised
      `SAPPHIRE_PREDICTION_MODE`, duplicate-key API response, `--output-json` == `--baseline` under
      **both** `--phase pre` and `--phase post`.
- [ ] `--target daily` performs decade checks with `SAPPHIRE_PREDICTION_MODE` unset (D1), and no
      other target's horizon set changes. A deployment with stale decade data is expected to start
      FAILing `daily` — call that out in the PR description and the runbook rather than letting it
      surprise an operator.
- [ ] `doc/configuration.md`'s freshness default reads 3, matching the code (D2), and no other
      passage still says 7.
- [ ] F5 is a docstring change only; `--phase pre`'s exit code is unchanged for ordinary FAIL rows.
- [ ] The F6 test fails when its corresponding variable is dropped from the fixture list (one
      parametrised case per variable — see F6's proportionality note).
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.
      Run the **affected-scope suite after each phase**, not only once at the end (CLAUDE.md's
      standing precondition); P5 additionally needs `bash -n apps/run_locally.sh` and the
      `pipeline` harness suite.
- [ ] `ruff check` / `ruff format --check` clean on changed files.

## Phases

> **Serialisation corrected 2026-09-08.** P1–P4 were all marked "Depends on: none" while editing
> **the same two files** (`validate_pipeline.py` and `test_validate_pipeline.py`). Running them
> concurrently would collide. They are now a single serial chain; only P5 is genuinely independent,
> because it touches the launcher and its own harness. Run the affected-scope suite after **each**
> phase, not once at the end.

- **P1 — malformed config values (F2, F3) + the D1 `daily` horizon derivation.** Files:
  `validate_pipeline.py`, test file. Depends on: none. Agents: 1. Accept: F2 and F3 tests pass, each
  proven by its own subprocess assertion (exit code + message), not a repo-wide traceback grep; and
  `--target daily` runs decade checks with the mode unset. **This phase now carries a
  which-checks-run change (D1), so its review must confirm no other target's horizon set moved.**
- **P2 — malformed API response (F4).** Files: `validate_pipeline.py`, test file.
  **Depends on: P1** (same files). Agents: 1. Accept: F4 test passes **and
  `test_check_presence_valueerror_not_mislabelled_though_still_propagates` is updated, not deleted**.
  *Subject to the owner decision on whether F4 survives at all.*
- **P3 — path collision (F1, both phases) and the exit-contract docstrings (F5).** Files:
  `validate_pipeline.py`, test file. **Depends on: P2.** Agents: 1. Accept: collision rejected under
  `--phase pre` *and* `--phase post`, alias detection via `samefile()`, and the docstrings cover
  exit 2 as well as 0/1.
- **P4 — non-vacuous isolation tests (F6).** Files: test file, `conftest.py`. **Depends on: P3.**
  Agents: 1. Accept: one parametrised poison-variable case per fixture entry.
  *Subject to the owner decision on whether F6 survives.*
- **P5 — relative-pointer canonicalisation (F1b).** Files: `apps/run_locally.sh` +
  `apps/pipeline/tests/test_run_locally_orchestration.py`. Depends on: none (the only genuinely
  independent phase). Agents: 1. Accept: a relative pointer works end to end **from an arbitrary
  operator cwd, including a path containing spaces** — not only from the repo root; the child
  receives an absolute path; `bash -n apps/run_locally.sh` clean; the `pipeline` harness suite green.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P5": { "depends_on": [], "parallel_agents": 1 }
  }
}
```

## Owner decisions — these block `Ready` (raised by out-of-loop review, 2026-09-08)

**D1 — DECIDED 2026-09-08: yes, `target=daily` derives both horizons.**
`daily` runs PENTAD then DECAD, restores the original mode (`run_locally.sh:1750`, normally unset),
then invokes validation (`:1774`). With the mode unset, `resolve_horizons` defaults to `["pentad"]`
(`validate_pipeline.py:1335`) — so **`daily` already omits decade validation on the happy path, with
no typo involved.** INFRA-039 documented this as "Failure C". F3's own acceptance ("unset still
defaults to pentad") would deliberately preserve it. Options: (a) F3 rejects an unrecognised mode
only, and the `daily` omission stays with INFRA-039; (b) `target=daily` derives `["pentad",
"decade"]` from the *target*, ignoring the restored mode. (b) conflicts with this issue's own "do
not change which checks run" boundary, so it needs an explicit owner call rather than an
implementer's judgement.

> **Owner decision: option (b).** `target=daily` must derive `["pentad", "decade"]` from the
> **target**, not from the restored `SAPPHIRE_PREDICTION_MODE`. This deliberately crosses this
> issue's "do not change which checks run" boundary — that boundary is amended below rather than
> quietly ignored. Consequences the implementer must handle:
> - `daily` runs will now perform decade checks that never ran before, so a deployment with genuinely
>   stale or absent decade data will start FAILing a target that passed yesterday. **That is the
>   point** — it was passing on no evidence — but it is a visible behaviour change and needs saying
>   in the PR description and the runbook.
> - Do **not** change the unset-mode default for any other target; `resolve_horizons` keeps
>   defaulting to `["pentad"]` when it has nothing better to go on. The derivation is target-driven
>   and scoped to `daily`.
> - Cross-reference **INFRA-039 "Failure C"**, which documented this omission: it is now fixed here,
>   and INFRA-039's entry must say so rather than leaving two issues claiming it.

**D2 — DECIDED 2026-09-08: 3 is authoritative; the documentation is wrong.** The code default of
**3** (`validate_pipeline.py:100`) stands unchanged — **no runtime change** — and
`doc/configuration.md`'s **7** is corrected to 3. Sweep for any other passage stating a freshness
default before declaring this done; two documents disagreeing is how this arose. F2 additionally
rejects non-numeric **and negative** values (`int()` alone accepts a negative).

**D3 — STILL OPEN (owner has not decided; do not start P2 or P4 until they have).**
Proportionality: do F4 and F6 survive?
- **F4** — the duplicate-`date`-column response it guards against **cannot come from the installed
  client**: every SDK reader builds its DataFrame from decoded JSON records, and decoded JSON
  mappings cannot carry duplicate keys. The function-level exception is real but the input shape is
  contrived. Cut, or downgrade to Minor defensive hardening — **Important is not proportional**.
- **F6** — the diagnosis is right (`conftest.py:22` removes the variables before the assertions
  observe them), but the proposed nested subprocess matrix plus drop-one mutation proof is heavy for
  a six-entry tuple. A parametrised subprocess exporting one poison variable per case is
  proportional. Cut, or simplify.

**Recommendation on a fourth point (not blocking, taken unless overruled):** `SAPPHIRE_API_ENABLED`
should get domain validation. Today anything except the literal `"false"` counts as enabled
(`validate_pipeline.py:1743`), so a typo can leave producers disabled while the validator happily
checks stale API data — the same silent-wrong-answer shape as F3. Adding it is consistent with F2's
premise; the alternative is to narrow F2's acceptance criterion to exclude it explicitly.

## Out of scope

- Check semantics, freshness thresholds, and false-pass behaviour — INFRA-020..031.
- The long-term "no records" FAIL on a non-forecast day: **not a defect**. No long-term forecast is
  due on most days; the row is correct. Whether the validator should be schedule-aware is INFRA-028.
- Giving `validate_pipeline` a production invoker — INFRA-031.
