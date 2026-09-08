# INFRA-045: `validate_pipeline` robustness gaps exposed by the deployment-env fix

**Status**: **Ready** (2026-09-08, rev 2). Filed as Draft 2026-09-04. Its prerequisite (PR #486) is
merged, all seven findings were re-verified against trunk on 2026-09-08, and the three owner
decisions raised by the first out-of-loop review are resolved — **plus four more from a second
review on 2026-09-08, which found the first pass had left the document self-contradictory and
carrying one materially wrong operator claim** (§ Owner decisions): `target=daily` derives both
horizons; the code's freshness default of 3 is authoritative; **F4 and F6 are withdrawn**, leaving
**five active findings across four phases** (P1 → P3 → P6 on the validator and its docs, P5 on the
launcher) in two independent workstreams. Status vocabulary is owned by `doc/plans/README.md`.
**Module**: `apps/validate_pipeline/validate_pipeline.py` (+ its test suite)
**Priority**: **Medium** — none of these breaks the validator's happy path, and `validate_pipeline`
still has no production invoker (it runs only from `apps/run_locally.sh`, so this is a dev gate, not
an operational one). But three of the five active findings turn operator misconfiguration into a Python traceback or a
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
> block, and the tests the (now-withdrawn) F4 and F6 referred to.
>
> **This issue is unblocked.** The box is kept as a corrected note rather than deleted so that a
> reader who remembers the warning can see it was resolved, not silently dropped.

### Findings re-verified against trunk, 2026-09-08

Every finding below was re-checked after INFRA-030 (#497) and INFRA-044 (#498) merged. All seven
were live at that point; **F4 and F6 were subsequently withdrawn by owner decision**, so five are
active — see the two `WITHDRAWN` rows in the table. Line numbers move constantly in these two files — the citations here were re-derived on
that date and must be re-derived again with `grep -n` at implementation time.

| | still live? | evidence on trunk |
|---|---|---|
| F1 | yes | JSON-output block still precedes the `--phase pre` block in `validate()` |
| F1b | yes | see the corrected citations in that section — all four of its original ones were stale |
| F2 | yes | `int(os.environ.get("FRESHNESS_THRESHOLD_DAYS", ...))` unguarded, `validate_pipeline.py:1081` |
| F3 | yes | `return MODE_TO_HORIZONS.get(mode, ["pentad"])`, `validate_pipeline.py:1349` |
| ~~F4~~ | **WITHDRAWN** (owner, 2026-09-08) | defect is real at function level, but the input shape is unreachable via the installed client — see § Owner decisions |
| F5 | yes (docs only) | decision already taken 2026-09-04; two docstrings still promise the unqualified exit contract |
| ~~F6~~ | **WITHDRAWN** (owner, 2026-09-08) | diagnosis is correct — both tests still observe absence rather than exercising the fixture — but the fix was judged disproportionate |

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

Three of the five active findings are violations of it — the process exits via an uncaught traceback
(F2), or exits 0 having not performed the validation it was asked for (F3, and F1 under
`--phase post`). A fourth, F4, was originally counted here and has since been withdrawn.

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

**Fix**: convert a bad value into a `[FAIL]` row naming the variable and the offending value — not a
traceback. A validator that dies on a malformed config is failing at its own job.

> **Two corrections from the second review (2026-09-08):**
>
> 1. **"Validate where they are read" is not sufficient for `FRESHNESS_THRESHOLD_DAYS`.** It is read
>    only when Tier 1 produced results (`validate_pipeline.py:1501`, `run_tier2` at `:1185`), so if
>    the API is unavailable or a `--module` filter yields no rows, a malformed value is **never
>    diagnosed**. Worse, an ordinary FAIL row under `--phase pre` still returns 0 and writes the
>    baseline. Validate configuration in a **preflight that runs regardless of data availability**,
>    or emit it as a `critical=True` row — which survives `--module` filtering and preserves the
>    baseline (the mechanism added by PR #486 for exactly this shape).
> 2. **Do not promise to name "the file it came from".** With `override=False`, an ambient exported
>    value can win even when a deployment-file pointer is present, so the provenance would sometimes
>    be a lie. Either drop provenance from the message, or have the loader record whether each
>    binding pre-existed the file load and report only what it actually knows.

**Tests**: a deployment env file with `FRESHNESS_THRESHOLD_DAYS=abc`, one with
`FRESHNESS_THRESHOLD_DAYS=-1` (**D2 requires rejecting negatives; bare `int()` accepts them** — an
earlier revision listed only the non-numeric case), and one with a malformed `SAPPHIRE_API_URL`. All
must exit 1 with a `[FAIL]` naming the variable, and none may print a traceback. Add a case proving
the diagnosis still fires when **Tier 1 produced no rows**, which is the gap the placement
correction above exists for.

## F3 — an invalid `SAPPHIRE_PREDICTION_MODE` silently validates the wrong horizon

**Severity: Important. Pre-existing.** Reachability corrected 2026-09-08 after out-of-loop review.

> **The earlier claim "reachable from the env file now" is FALSE.** `run_in_venv` always supplies
> `SAPPHIRE_PREDICTION_MODE` to the child — even as an empty string (`run_locally.sh:653`) — and the
> validator's `load_dotenv` uses `override=False` (`validate_pipeline.py:1632`), so an env-file value
> can never replace the launcher-supplied one. The launcher additionally rejects invalid *process*
> values for the primary short-term/all/LR targets (`run_locally.sh:1879`).
>
> F3 is still live, by two routes: **direct CLI invocation** of the validator, and **`target=daily`**,
> which deliberately bypasses that upstream mode check.
>
> **Refined again 2026-09-08 (second review) — the correction above was itself overbroad.** "Not
> reachable from the env file" holds only *when launched through `run_locally.sh`*. A **direct CLI
> invocation with only `ieasyhydroforecast_env_file_path` set** genuinely does load a bad mode from
> the file, because nothing has pre-set the variable for `override=False` to defer to. So the env
> file **is** a live route for direct invocations — just not for launcher-driven ones.
>
> Note also that after **D1**, `daily` stops being an invalid-mode route at all: it will ignore the
> ambient mode and derive both horizons regardless.

`resolve_horizons` ends with `MODE_TO_HORIZONS.get(mode, ["pentad"])`. An unrecognised, **non-empty**
mode — `DECADES` instead of `DECAD`, say — silently selects pentad. Healthy pentad data then makes
the run exit 0 while the decade validation the operator asked for never happened.

This is the "silent fallback" shape: it fails reassuringly. An absent mode defaulting to pentad is
defensible; a *present but unrecognised* one is not — it is a typo the operator wants to hear about.

**Fix**: distinguish the two. Unset → keep today's default. Set but not in `MODE_TO_HORIZONS` →
`[FAIL]` naming the value and the accepted set, exit 1. Do not silently substitute.

**Test**: `SAPPHIRE_PREDICTION_MODE=DECADES` exits 1 naming the bad value.

> **Corrected 2026-09-08**: the earlier clause "unset still defaults to pentad **for every target
> except `daily`**" is **false** — `long-term` resolves to month, and an explicit `--horizon`
> bypasses mode resolution entirely.
>
> **Refined again (third review): "no target's horizon resolution changes except `daily`'s" is also
> wrong** — it contradicts F3 itself, which deliberately makes a *junk* mode FAIL where it currently
> resolves to pentad. State the requirement as: **no target's resolution changes for a VALID or
> UNSET mode**; a junk mode newly FAILs for every target that resolves from the mode, while `daily`,
> `long-term` and an explicit `--horizon` bypass mode resolution and are unaffected by the junk
> case.

**Plus, per owner decision D1**: `--target daily` resolves `["pentad", "decade"]` from the target
itself regardless of the ambient mode.

> **Implementation constraint (second review, 2026-09-08): the new `daily` branch must come AFTER
> the explicit `--horizon` branch.** Placing it first would break the documented `--horizon`
> override, which is currently tested. `--horizon` wins; `daily` only decides what to do when no
> explicit horizon was given.
>
> **Test all five ambient cases**, not the two an earlier revision named: mode **unset** (today's
> silent pentad-only path), `PENTAD`, `DECAD`, `BOTH`, and a junk value — asserting decade checks
> actually ran in every one. Two cases would pass a conditional implementation that still mishandles
> `DECAD`/`BOTH`/junk. Additionally assert that an explicit `--horizon pentad` **still wins** over
> the `daily` derivation, and state what `--target daily --module <m>` means: the CLI permits that
> combination, so show the module filter applying across **both** derived horizons.

## ~~F4 — `check_presence` can still exit the process with a traceback~~ — WITHDRAWN

> **Owner decision 2026-09-08: cut, to avoid over-complicating the issue.** The defect is real at
> the function level, but out-of-loop review established that the duplicate-`date`-column response
> it guards against **cannot arrive through the installed client** — every SDK reader builds its
> DataFrame from decoded JSON records, and decoded JSON mappings cannot carry duplicate keys. It was
> reachable only from a hand-constructed test input.
>
> **Consequence for whoever reads this later**: `check_presence` is left as it is, and
> `test_check_presence_valueerror_not_mislabelled_though_still_propagates` **stays exactly as
> written** — it pins today's propagation deliberately. Do not "fix" that test; its docstring
> explains why it pins only half of what it motivated. If the client's response construction ever
> changes so a malformed shape can reach this code, reopen this section rather than re-deriving it.
>
> The original analysis is kept below for that reopening.

**Severity: Important. Pre-existing.**

`check_presence` guards its API call, but the pandas work after it is unguarded: a response whose
`date` column is duplicated makes `pd.to_datetime` raise `ValueError: cannot assemble with duplicate
keys`, which escapes to the CLI as a traceback.

A test currently pins this propagation
(`test_check_presence_valueerror_not_mislabelled_though_still_propagates` — the name given in earlier revisions of this file, `..._as_horizon_config`, does not exist). **That test is pinning the
useful half only** — that the error is not mislabelled as a horizon-configuration failure — and its
docstring says so explicitly. ~~**When this issue is implemented, update that test; do not treat it
as a blocker.**~~ **← OBSOLETE. F4 is withdrawn, so this issue does NOT touch that test; it stays
exactly as written.** It was written knowing this issue would be filed.

~~**Fix**: a malformed API response should become a `[FAIL]` row naming the check and the response
problem, exit 1. Same contract as everything else.~~ **← OBSOLETE with the withdrawal; retained only
as the recipe if this is ever reopened.**

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

> **Add the missing regression test (second review, 2026-09-08).** Nothing currently pins the chosen
> behaviour — that an *ordinary* FAIL row under `--phase pre` returns **0** and still writes the
> baseline. Only the critical-row path is tested, so a future change could flip the ordinary case
> and no test would notice. F5's production change stays docstring-only; this is a test-only
> addition.
>
> **State the exit codes the subprocess assertions expect**, rather than leaving them inferable:
> malformed configuration → **1**; a parse-time `--output-json`/`--baseline` collision rejected by
> `parser.error()` → **2** (argparse's usage-error code, which is why F5's docstring correction must
> mention exit 2 at all).

*(A critical row — the requested validation could not be performed — is different and already
forces a non-zero exit there; that is not affected by this decision.)*

## ~~F6 — the ambient-environment isolation tests are vacuous~~ — WITHDRAWN

> **Owner decision 2026-09-08: cut, to avoid over-complicating the issue.** The diagnosis stands and
> is not disputed: `test_env_file_pointer_absent_by_default` and
> `test_ambient_env_vars_absent_by_default` observe `os.environ` *after* the autouse fixture has run,
> so in a clean environment they pass whether or not the fixture works. **They are decoration, and
> they remain in the tree.**
>
> This is an accepted, recorded risk rather than an oversight: if someone removes an entry from the
> fixture's variable list, no test will notice, and the symptom will be tests behaving differently on
> different developers' machines. That trail is recorded here so it is diagnosable when it happens.
> The proportionate fix, if it is ever wanted, is one parametrised subprocess case per variable —
> **not** the nested matrix plus drop-one mutation proof the original text below asks for.
>
> The original analysis is kept below for that reopening.

**Severity: Minor. Introduced by the env-loading patch's own tests.**

`test_env_file_pointer_absent_by_default` and `test_ambient_env_vars_absent_by_default` observe
`os.environ` *after* the autouse fixture has run. In a clean CI environment those variables are
absent anyway, so both tests pass even if the fixture is gutted — they do not protect the
developer-shell isolation they were written for.

~~**Fix**: set the variables to a poison value in the test process, then assert that a
*representative* test still behaves correctly — i.e. exercise the isolation, don't observe its
outcome.~~ ~~**Acceptance for this one specifically**: the new test must fail if any single entry is
removed from the fixture's variable list.~~

**← BOTH OBSOLETE. F6 is withdrawn**: no test is written, `conftest.py` is not touched. Retained
only as the recipe if this is ever reopened — and if it is, use one parametrised subprocess case per
variable, not a nested matrix.

---

## Files that may be modified

- `apps/validate_pipeline/validate_pipeline.py`
- `apps/validate_pipeline/test/test_validate_pipeline.py`
- ~~`apps/validate_pipeline/test/conftest.py`~~ — was F6 only; **F6 withdrawn, so this file is
  now out of scope and must not be touched**
- `doc/configuration.md` (**D2** — the freshness default, 7 → 3)
- `doc/dev/review_checklist_local_template.md` (**D7** — its validator procedure invokes a
  `run_locally.sh validate` target that does not exist (`run_locally.sh:2375` has no such case), so
  anyone following it fails immediately; replace it with a working direct invocation and note the
  new `daily` decade coverage)
- INFRA-039's issue file + `doc/plans/module_issues.md` (**D1** — its "Failure C" is fixed here, so
  its entry and tracker row must say so rather than leaving two issues claiming the same defect)

> These were missing from this list until 2026-09-08 even though D1, D2 and D7 require them. A phase
> that edits a file not on its allowed list is how scope creep enters unnoticed.
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

- [ ] No malformed value of **`FRESHNESS_THRESHOLD_DAYS` or `SAPPHIRE_API_URL`** produces a
      traceback; each produces a `[FAIL]` naming the variable and its value, and the diagnosis fires
      even when Tier 1 produced no rows. **Per D5 this issue does NOT cover `SAPPHIRE_API_ENABLED`**
      — an earlier criterion implying all four variables were covered was withdrawn.
- [ ] Each case below is proven by its **own subprocess assertion** over combined stdout+stderr,
      asserting the exact exit code and, where relevant, that the target file was preserved — not by
      a repo-wide `grep -rn "Traceback"`, which is ambiguous and cannot show the intended validation
      actually ran: bad `FRESHNESS_THRESHOLD_DAYS`, bad `SAPPHIRE_API_URL`, unrecognised
      `SAPPHIRE_PREDICTION_MODE`, and `--output-json` == `--baseline` under **both** `--phase pre`
      and `--phase post`. (The duplicate-key API response case is gone with F4.)
- [ ] `--target daily` performs decade checks for **all five ambient mode cases** (unset, PENTAD,
      DECAD, BOTH, junk), an explicit `--horizon pentad` still overrides it, `--target daily
      --module <m>` filters across both derived horizons, and **no other target's horizon set
      changes** (D1).
- [ ] The operator-visible consequence is described **accurately** in the PR and the runbook: stale
      decade data produces WARNs that do not change the exit code; absent decade data FAILs only on
      decade issue days. **Do not repeat the withdrawn "will start FAILing `daily`" claim.**
- [ ] A pre-D1 `daily` baseline is **refused** with a message telling the operator to retake it, not
      silently reused (D4); proven by a `daily` pre/post pair across the change.
- [ ] `doc/dev/review_checklist_local_template.md`'s validator procedure runs successfully as
      written (D7) — the current `run_locally.sh validate` command does not exist.
- [ ] An ordinary FAIL row under `--phase pre` still returns 0 and writes the baseline, pinned by a
      new regression test (F5); malformed config exits 1 and a parse-time path collision exits 2.
- [ ] `doc/configuration.md`'s freshness default reads 3, matching the code (D2), and no other
      passage still says 7.
- [ ] F5 is a docstring change only; `--phase pre`'s exit code is unchanged for ordinary FAIL rows.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.
      **CLAUDE.md requires the FULL `run_tests.sh` after every phase** — an earlier revision of this
      file weakened that to "affected-scope", which it must not. P5 additionally needs
      `bash -n apps/run_locally.sh` and the `pipeline` harness suite.
- [ ] `ruff check` / `ruff format --check` clean on changed files.

## Phases

> **Serialisation corrected 2026-09-08.** P1–P4 were all marked "Depends on: none" while editing
> **the same two files** (`validate_pipeline.py` and `test_validate_pipeline.py`). Running them
> concurrently would collide. **After the F4/F6 withdrawals the issue is four phases in two
> workstreams**: P1 → P3 → P6 on the validator and the documentation its decisions require, and P5
> on the launcher (genuinely independent). "Two phases" in earlier revisions miscounted, and no
> phase owned the documentation at all until 2026-09-08. Run the
> **full** `run_tests.sh` after **each** phase, not once at the end (CLAUDE.md).

- **P1 — malformed config values (F2, F3), the D1 `daily` horizon derivation, and the D4 baseline
  horizon metadata.** Files: `validate_pipeline.py`, test file. Depends on: none. Agents: 1.
  Accept — **the full contract, not a subset** (an earlier revision listed only the unset-mode case,
  which would have let a partial implementation pass):
  - F2 and F3 tests pass, each proven by its own subprocess assertion (exit code + message), not a
    repo-wide traceback grep, **including the Tier-1-produced-no-rows case and `-1`**;
  - `--target daily` runs decade checks for **all five ambient mode cases** (unset, PENTAD, DECAD,
    BOTH, junk);
  - an explicit `--horizon pentad` **still overrides** the `daily` derivation;
  - `--target daily --module <m>` filters across **both** derived horizons;
  - **no target's resolution changes for a valid or unset mode** (see F3's refined wording);
  - **D4**: the baseline records its resolved horizons, and a legacy horizonless baseline is refused
    with a retake message — proven by a `daily` pre/post pair across the change.
  **This phase carries a which-checks-run change (D1) and a baseline-format change (D4), so its
  review must confirm no other target's horizon set moved and that a matching baseline still loads.**
- ~~**P2 — malformed API response (F4).**~~ **WITHDRAWN** with F4 (owner, 2026-09-08). Note this
  means `test_check_presence_valueerror_not_mislabelled_though_still_propagates` is **not** touched
  by this issue at all — it stays as written.
- **P3 — path collision (F1, both phases) and the exit-contract docstrings (F5).** Files:
  `validate_pipeline.py`, test file. **Depends on: P1** (P2 withdrawn; same files as P1). Agents: 1.
  Accept additionally: **F5's regression test** pinning that an *ordinary* FAIL row under
  `--phase pre` still returns 0 and writes the baseline (only the critical-row path is tested today),
  and the stated exit codes — malformed config **1**, parse-time path collision **2**. Accept: collision rejected under
  `--phase pre` *and* `--phase post`, alias detection via `samefile()`, and the docstrings cover
  exit 2 as well as 0/1.
- ~~**P4 — non-vacuous isolation tests (F6).**~~ **WITHDRAWN** with F6 (owner, 2026-09-08).
  `conftest.py` is therefore not modified by this issue.
- **P6 — the documentation D1/D2/D7 require.** Files: `doc/configuration.md`,
  `doc/dev/review_checklist_local_template.md`, INFRA-039's issue file, `doc/plans/module_issues.md`.
  **Depends on: P1, P3** (it describes what they changed). Agents: 1. Accept: the freshness default
  reads 3 everywhere (D2); the review checklist's validator command runs as written (D7); INFRA-039's
  "Failure C" is marked fixed here rather than left claimed by two issues (D1); and the `daily`
  consequence is stated as WARN-plus-issue-day-FAIL, **not** as "will start FAILing".
  *This phase was missing entirely until 2026-09-08 — three owner decisions required documentation
  that no phase owned.*
- **P5 — relative-pointer canonicalisation (F1b).** Files: `apps/run_locally.sh` +
  `apps/pipeline/tests/test_run_locally_orchestration.py`. Depends on: none (the only genuinely
  independent phase). Agents: 1. Accept: a relative pointer works end to end **from an arbitrary
  operator cwd, including a path containing spaces** — not only from the repo root; the child
  receives an absolute path; `bash -n apps/run_locally.sh` clean; the `pipeline` harness suite green.

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P3": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P6": { "depends_on": ["P1", "P3"], "parallel_agents": 1 },
    "P5": { "depends_on": [], "parallel_agents": 1 }
  }
}
```

## Owner decisions — all resolved 2026-09-08 (raised across three out-of-loop review rounds)

**D1 — DECIDED 2026-09-08: yes, `target=daily` derives both horizons.**
`daily` runs PENTAD then DECAD, restores the original mode (`run_locally.sh:1750`, normally unset),
then invokes validation (`:1774`). With the mode unset, `resolve_horizons` defaults to `["pentad"]`
(`validate_pipeline.py:1348-1349`) — so **`daily` already omits decade validation on the happy path, with
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
> - `daily` runs will now perform decade checks that never ran before. **CORRECTED 2026-09-08 after
>   a second out-of-loop review — the earlier wording here ("stale decade data will start FAILing
>   `daily`") was wrong** and overstated the alarm. What actually happens:
>   - **stale** decade data produces `WARN` (`validate_pipeline.py:1103`), and warnings do **not**
>     change the exit code — so a stale deployment gets noisier output, not a failing run;
>   - **absent** decade data is downgraded to `SKIP` away from decade forecast days by
>     `_apply_non_forecast_day_skip()` (`:1368`), so it only FAILs **on decade issue days**;
>   - ordinary correctness failures in the decade checks can of course also fail `daily`.
>   State it that way in the PR description and the runbook. Do not repeat the "will start failing"
>   claim — it would have an operator bracing for the wrong thing.
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

**D3 — DECIDED 2026-09-08: cut both F4 and F6**, "to not over-complicate things". P2 and P4 are
withdrawn with them; the issue is now five active findings across **four** phases (P1 → P3 → P6,
plus the independent P5) in two workstreams. Each withdrawn section
keeps its original analysis and states what a later reader must NOT do (do not touch
`test_check_presence_valueerror_not_mislabelled_though_still_propagates`; do not touch
`conftest.py`), plus what the accepted residual risk is. The reasoning that led to the cut:
- **F4** — the duplicate-`date`-column response it guards against **cannot come from the installed
  client**: every SDK reader builds its DataFrame from decoded JSON records, and decoded JSON
  mappings cannot carry duplicate keys. The function-level exception is real but the input shape is
  contrived. Cut, or downgrade to Minor defensive hardening — **Important is not proportional**.
- **F6** — the diagnosis is right (`conftest.py:22` removes the variables before the assertions
  observe them), but the proposed nested subprocess matrix plus drop-one mutation proof is heavy for
  a six-entry tuple. A parametrised subprocess exporting one poison variable per case is
  proportional. Cut, or simplify.

**D4 — DECIDED 2026-09-08: pre-D1 `daily` baselines are REJECTED, not silently reused.** Baseline
metadata records date and target but **not horizons** (`validate_pipeline.py:248`), so an old
pentad-only `daily` baseline would be accepted after D1 as though it covered both — producing a
confident, wrong "nothing changed" comparison for decade. Record the resolved horizons in the
baseline and refuse a baseline whose horizon set does not match the current run, with a message
telling the operator to retake it. This expands baseline semantics, which is why it was an owner
call. Test a `daily` pre/post pair across the change.

> **Owned by P1** (assigned 2026-09-08 after the third review found no phase implemented it).
>
> **One point still needs your call before P1 starts**: D4's heading is `daily`-specific, but
> "refuse a baseline whose horizon set does not match" as written would reject **every** legacy
> horizonless baseline, on every target — because none of them records horizons. Decide: (a) reject
> only where it can actually mislead, i.e. `daily`, and accept a horizonless baseline for
> single-horizon targets; or (b) reject all legacy baselines and have every deployment retake them
> once. (a) is narrower and matches the defect; (b) is simpler to implement and reason about but
> invalidates baselines for targets whose behaviour did not change.

**D5 — DECIDED 2026-09-08: `SAPPHIRE_API_ENABLED` is left alone; the acceptance criterion is
narrowed instead.** Today anything but the literal `"false"` counts as enabled
(`validate_pipeline.py:1743`). That stays. **F2 covers `FRESHNESS_THRESHOLD_DAYS` and
`SAPPHIRE_API_URL` only**, and the acceptance criterion must stop implying it covers all four
variables the validator reads. Recorded as an accepted residual risk: a typo in that flag still
silently leaves API checks enabled.

**D6 — DECIDED 2026-09-08: unguarded path and baseline-shape I/O is filed separately, not added
here.** Output and baseline writes call `Path.write_text()` unguarded (`validate_pipeline.py:253`,
`:1542`) so an unwritable path raises `OSError`, and a syntactically valid but non-object baseline
such as `[]` makes `baseline.get(...)` raise `AttributeError` (`:278`) outside the post-phase catch.
Same family as F1, but this issue has been re-scoped twice already. File as its own draft, and
**stop describing INFRA-045 as closing configuration robustness generally** — it closes five named
findings.

**D7 — DECIDED 2026-09-08: the broken operator procedure is fixed as part of this issue.**
`doc/dev/review_checklist_local_template.md:188` tells an operator to run
`bash apps/run_locally.sh validate --phase …`. **There is no `validate` target** (`run_locally.sh:2375`)
and the launcher does not parse validator flags, so anyone following it fails immediately.

> **Third review: there are THREE such invocations, not one** —
> `doc/dev/review_checklist_local_template.md:196` (pre), `:1898` (post) and `:1915` (JSON output).
> P6 must replace **all three** and execute-check each; fixing only the one this section originally
> cited would leave the procedure broken two-thirds of the way through, which is the partial-fix
> shape this issue has already hit twice.

Replace them with working direct invocations of the validator and note there that `daily` now covers
decade.

**Recommendation on a further point — SUPERSEDED by D5, kept for the trail:** `SAPPHIRE_API_ENABLED`
should get domain validation. Today anything except the literal `"false"` counts as enabled
(`validate_pipeline.py:1743`), so a typo can leave producers disabled while the validator happily
checks stale API data — the same silent-wrong-answer shape as F3. Adding it is consistent with F2's
premise; the alternative is to narrow F2's acceptance criterion to exclude it explicitly.

## Out of scope

- Check semantics, freshness thresholds, and false-pass behaviour — INFRA-020..031.
- The long-term "no records" FAIL on a non-forecast day. **Corrected 2026-09-08: calling this "not a
  defect" was wrong.** `doc/plans/issues/high_prio_gi_draft_infra_validate_pipeline_gated_day_false_fail.md`
  (INFRA-022) identifies it explicitly as a recurring **false FAIL** — the output correctly does not
  exist on gated days, so failing on its absence is the validator being wrong, not the data. It is a
  known defect tracked by INFRA-022 (with INFRA-028's schedule-awareness as its prerequisite), and
  it stays out of *this* issue — but do not repeat the claim that it is correct behaviour.
- Giving `validate_pipeline` a production invoker — INFRA-031.
