# Agent Review & Verification Workflow

Procedure and templates for the multi-model review rules in
[`CLAUDE.md`](../../CLAUDE.md) (§ Multi-Model Review & Verification). This document is the
"how"; CLAUDE.md is the "must". Status vocabulary is owned by
[`../plans/README.md`](../plans/README.md); this doc does not restate it.

## Fitness statement (required in every high-claim-density prompt, verbatim)

> Every bullet must help an agent know what to inspect, what contract not to break, or what
> verification proves safety — otherwise cut it.

## Definition of done: tests are non-negotiable

Nothing is "done", review-eligible, or PR-eligible until the full affected-scope tests pass —
`run_tests.sh` for every touched module AND every module that imports the changed code, with **zero
failures and zero unexpected skips**. Run them before calling a milestone complete or handing an
artifact for review; this is a standing precondition, not a pre-PR afterthought (see CLAUDE.md
Testing Requirements). If the local worktree lacks a module's venv, either `uv sync --all-extras` it
and run it, or explicitly record it as deferred-to-CI **with the reason** — never silently skip.

## When mandatory multi-model review applies

Required for: implementation plans (`gi_*.md`), design docs, audit/investigation reports, and
multi-file patches — anything with many falsifiable claims about code or contracts, OR any patch
touching a high-risk coupling point (below).

Optional for: single-file mechanical patches, docs-only edits with no code claims, and artifacts
produced by a qualifying cross-vendor workflow (see "Cross-vendor workflow accelerators" below).

## Cross-vendor workflow accelerators (optional)

A multi-agent orchestration workflow that provides an independent review panel **plus** a quality
gate satisfies the mandatory-review + out-of-loop requirements for work run through it. Today the
`vision-decompose` (WF1) and `vision-build` (WF2) skills provide this (Codex planner/implementer +
Claude adversarial panel / quality gate).

Caveats — do not treat them as a dependency:
- They are **external plugin skills, not versioned in this repo**, so they can change or be
  unavailable to a teammate without the plugin.
- WF2 requires `<repo>/.claude/workflow-capabilities.json`, which is **absent by default**.

The repo-native path (`codex exec` verifier + `code-review` + human owner) satisfies the rule
**without** them. If the team wants this capability reliable, portable, and editable in-repo, vendor
a repo-native equivalent as a **separate `gi_draft` initiative** — do not couple the review rule to
undocumented external tooling.

## Adversarial review is REQUIRED — not just claim verification

There are two different out-of-loop passes; a high-claim artifact (plan, design, audit, or
multi-file / high-risk patch) needs the **adversarial** one, and it is the primary gate:

- **Claim verification** — you hand the reviewer a checklist of falsifiable claims; it confirms/refutes
  each. Catches "does the code do what I said" — but only checks what you thought to ask. Necessary,
  **not sufficient**; never treat a passing claim-verifier as review-complete.
- **Open-ended adversarial review** — you hand the reviewer the artifact/diff with **no checklist** and
  instruct it to find ANY defect, missing step, wrong assumption, edge case, or regression. Applies to
  **implementations (the diff) AND plans**. This is what catches what you didn't think to ask. Run it
  via read-only `codex exec` with an open-ended prompt; a fresh Claude subagent (no prior context) is
  the fallback — also open-ended.

Note: the dedicated `codex exec review` subcommand cannot run sandboxed (it requires
`--dangerously-bypass-approvals-and-sandbox`), so prefer the **read-only `codex exec`** form with the
artifact/diff pasted in the prompt.

## Out-of-loop verifier requirements

- Different tool/model where possible — default `codex exec`; fallback = a fresh Claude
  general-purpose subagent with **no prior session context**.
- **Read-only.** The verifier must not edit repo files.
- The artifact is passed **in the prompt, before it is committed**.
- Run against the patch's **target branch in a clean checkout**, and **diff against `origin/<target>`
  (e.g. `origin/maxat_sapphire_2`), never a stale LOCAL branch ref or the working tree.** A
  dirty/wrong-branch tree or a stale local ref produces false verdicts. (Seen twice: a wrong-branch
  run falsely refuted `round_3sf`/tombstone code that is present on the target; a run diffing against a
  stale local `maxat_sapphire_2` falsely reported an upstream `sapphire/services/` file as added by
  the change.)
- **Output contract — differs by review type; do not conflate them:**
  - Claim-verification / plan review: per-claim verdict `confirmed` / `refuted` / `unverifiable`,
    each with concrete evidence (path, symbol, test, or contract). No prose approval.
  - Diff-attack review: a findings array (file:line, severity, concrete failure scenario) — see
    "Attack axes for code diffs" § "Code-diff `codex exec` prompt template" below for the exact
    schema.

### `codex exec` verifier prompt template

```
codex exec -s read-only -C <clean_target_checkout> --color never -o <out.json> "$(cat <<'PROMPT'
READ-ONLY verification. Do NOT edit any file. You are a fresh, out-of-loop reviewer; assume nothing
from prior context.

ARTIFACT UNDER REVIEW (verify, do not trust):
<paste the full artifact text here>

TASK: For each falsifiable claim in the artifact — file paths, symbol names, described behavior,
conventions, DB/API contracts — verify it against the repo at the working directory. Return a JSON
array: [{claim, verdict: confirmed|refuted|unverifiable, evidence: "path/symbol/test or why
unverifiable"}]. Flag any claim that contradicts CLAUDE.md, doc/plans/README.md, or
doc/dev/testing_workflow.md. Do not propose design changes; only check factual accuracy.
PROMPT
)"
```

### Fresh-Claude fallback verifier prompt (Agent tool, general-purpose, no prior context)

```
READ-ONLY verification, fresh context. Work only in <clean_target_checkout>; do not edit any file.
Here is an artifact (verify, do not trust): <paste artifact>.
For each falsifiable claim (paths, symbols, behavior, conventions, contracts), check it against the
repo and return {claim, verdict: confirmed|refuted|unverifiable, evidence}. Flag contradictions with
CLAUDE.md / doc/plans/README.md / doc/dev/testing_workflow.md. Factual accuracy only — no design
opinions.
```

## Attack axes for code diffs

Claim verification (above) is necessary but **not sufficient** for any artifact — plan, design,
audit, or diff (see "Adversarial review is REQUIRED — not just claim verification"). Every artifact
also needs the open-ended adversarial pass. This section is that pass's concrete toolkit **when the
artifact under review is a diff**: pointed at a code diff, the claim-verification template above
finds almost nothing — a diff doesn't assert claims the way a plan's prose does, and "does the diff
do what the PR description says" is a much weaker question than "what does this diff break that
nobody asked about." The seven axes below are the diff-shaped form of the same open-ended
instruction that also applies to plans — this is **not** a "plans get claim-checking, diffs get
axes" split; a plan/design/audit gets claim verification (its content is largely falsifiable
claims) **plus** its own open-ended adversarial pass (no fixed axes — hand the reviewer the artifact
with no checklist, per "Adversarial review is REQUIRED" above). This section's axes are the template
for the out-of-loop pass required by "Post-implementation review gate" below, specifically for diffs.

This section does not replace the artifact template and does not restate `doc/dev/testing_workflow.md`
or the Orchestration Protocol. Every axis below is a real defect that shipped, or nearly shipped, in
this repo **while the claim-verification-only version of this workflow was already in force** — the
policy existed and did not catch these; only an open-ended diff attack did.

### The axes

Each is a question the reviewer must answer about the diff, not a box to tick.

1. **Implicit invariants.** List every invariant the diff relies on. Who enforces it in code — a
   type, a range check, a test? If nobody does, that is the bug. *`plot_manager` hardcoded the m0
   card's lead to `0` while `db.py` resolved `month_0`'s lead from config; nothing enforced
   "month_0 == lead 0." Same class recurred: a later fix inferred the forecast year by comparing
   month numbers, valid only for leads 0–11, and the config loader range-checks nothing.*
2. **Test contract vs code contract.** Do the tests prove the OPERATOR gets the right result, or
   only that the code agrees with itself? A test that manually seeds the state whose absence is the
   bug passes identically before and after the fix. *19 mutation-proven tests covered an m0-bulletin
   fix by seeding the very attribute whose absence caused the corruption — the operator still got a
   corrupt bulletin after save + reload.*
3. **Lifecycle / round-trip.** Does the fix survive save → reload → new session / process restart,
   or does it live only on an in-memory object? *The same m0 fix stored state on an in-memory
   object and never persisted it — it silently evaporated on reload.*
4. **Silent wrong-but-plausible fallback.** Does any error path swallow an exception (or take a
   default branch) and produce a plausible WRONG answer while telling the operator it succeeded?
   Failing reassuringly is worse than failing loudly. *A bare `except Exception` fell back to the
   wrong month while the UI reported "Bulletin saved successfully"; separately, `run_tests.sh`
   printed "All tests completed successfully!" while collecting zero tests.*
5. **Blast radius on stored/displayed numbers.** Does the diff change a number already PERSISTED in
   a DB table or DISPLAYED on a dashboard? If yes, treat it as the highest-severity class regardless
   of diff size — a formula fix to a stored value is a data-migration question, not a code-review
   question. *A skill metric's sign disagreed with its cited paper; flipping it would have silently
   redefined every value already written to the DB and shown on dashboards.*
6. **Kill-switch parity (flag-gated diffs only).** Attack flag-OFF and flag-ON as two separate
   contracts. Flag-OFF must be byte-identical to trunk — exercise that path explicitly, don't infer
   it from a green suite. Flag-ON must be correct on its own merits. *A crash existed only on the
   flag-ON path; the whole existing suite stayed green because every test in it ran flag-OFF.*
7. **Vacuity.** For each new/changed test, ask: would it still pass if the fix it covers were
   reverted? If yes, say so — it doesn't pin what it claims to pin. **Caveat:** golden/kill-switch
   tests are *supposed* to pass with the flag off, both before and after the change — that is their
   job; do not flag those as vacuous. Only flag a test whose stated purpose was to catch this bug and
   would not.

### Process rules

- **Stale premises.** Before fixing — or re-confirming — a reported finding, verify it against the
  CURRENT code, not the note, plan, or earlier review round that reported it. A finding scoped from a
  months-old memory note had two of three premises already false against current configs; three real
  fixes beats four fixes where one is fiction.
- **Run the out-of-loop diff pass BEFORE declaring the work done**, not as a formality attached to
  the PR afterward — it is a gate on the diff, matching "Post-implementation review gate" below.
- **Feed the reviewer the CONTRACT, not just the diff.** State explicitly what the operator/caller
  must observe (the before/after behavior, the flag default), so the reviewer attacks the contract
  instead of paraphrasing the diff back at you.
- **Re-review the fixes.** A fix is new, unreviewed code. A second review round over round-1's fixes
  found a fresh Important-severity defect in them — a fix is not safe merely because the finding it
  addresses was real; it needs its own pass.

### Code-diff `codex exec` prompt template

```
codex exec --skip-git-repo-check --sandbox read-only -C <clean_target_checkout> --color never \
  -o <out.json> "$(cat <<'PROMPT'
READ-ONLY adversarial review of a DIFF — not a claim checklist. Do NOT edit any file. You are a
fresh, out-of-loop reviewer; assume nothing from prior context and do not trust the summary below.

BUG BEING FIXED (as claimed by the author):
<one paragraph: what was broken, for whom, observed how>

DIFF UNDER REVIEW:
<paste `git diff <base>...HEAD`, or the specific files/hunks>

CONTRACTS THAT MUST HOLD (what the operator/caller must observe — verify against these, not the
diff's stated intent):
<e.g. "operator sees the CORRECT bulletin after save+reload+new session, or an explicit error —
never a silently wrong one"; "flag SAPPHIRE_FOO defaults to False, flag-OFF is byte-identical to
trunk"; "value X, once written to skill_metrics, is never silently redefined by this change">

SUSPICIOUS SEAMS TO ATTACK (name the specific ones for this diff):
<e.g. "state assigned to self.foo in PlotManager — persisted, or in-memory only?"; "except Exception
around the save path — what does it fall back to, and does the caller see success?"; "new/changed
tests — do they seed the state whose absence was the bug?">

TASK: Attack the diff against the contracts above, using these axes — do not merely restate what the
diff does:
1. Implicit invariants: what does the diff assume that no code enforces?
2. Test contract: do the tests prove the OPERATOR gets the right result, or only that the code
   agrees with itself (manually-seeded state, mocked-away the real bug, etc.)?
3. Lifecycle: does the fix survive save/reload/restart, or only live in memory for this session?
4. Silent fallback: any bare except / default branch that produces a plausible WRONG result while
   reporting success?
5. Blast radius: does this change a number already PERSISTED or DISPLAYED? If yes, flag as a
   data-migration question, not a code-review question.
6. Kill-switch parity (if flag-gated): is flag-OFF byte-identical to trunk? Is flag-ON correct?
7. Vacuity: for each new/changed test, would it still pass if the fix were reverted? Do NOT flag
   golden/kill-switch tests that are supposed to pass with the flag off both before and after.

Separate findings into two buckets: defects INTRODUCED by this diff, vs pre-existing defects the
diff happens to touch (report both; label which bucket).

Return a JSON array: [{axis, file, line, severity: Critical|Important|Minor, finding, concrete
failure scenario, introduced_by_diff: true|false}]. No prose approval, no design opinions beyond
what a Critical/Important finding requires.
PROMPT
)"
```

This findings-array shape is the diff-attack output contract; it is distinct from the per-claim
verdict contract used for plans/designs/audits (see "Out-of-loop verifier requirements" above) —
use the one that matches the review type, not both.

Fallback (Codex unavailable): the same prompt via a fresh Claude general-purpose subagent (Agent
tool, no prior session context) — same "different tool/model where possible" rule as the
claim-verification fallback above. Triage every returned finding against the current code before
acting on it (see "Stale premises" above) — the reviewer's verdict is evidence, not a mandate.

## Proportionality lens checklist (argue for cuts)

- [ ] Present-but-unnecessary content that doesn't change what an agent does.
- [ ] Over-specific line-number references likely to drift — prefer file + symbol.
- [ ] Duplicated docs — point to the one owner instead.
- [ ] Content that teaches how the code works instead of routing an agent to where to inspect.
- [ ] Restated status vocabulary (owned by [`../plans/README.md`](../plans/README.md)).

## Implemented vs documented separation

Every artifact must label: **implemented** (current behavior in code), **documented**
(aspirational / spec), and **drift** (where they disagree). Never present aspiration as current.

## Accuracy fixes vs design forks

- **Factual errors** the verifier finds may be applied directly.
- **Scope, design, semantics, security, or API-contract** changes **escalate to the human owner** —
  the verifier does not decide them.
- **Related bugs found while mapping** become `gi_draft_*` plan files under
  [`../plans/issues/`](../plans/issues/) (indexed in
  [`../plans/module_issues.md`](../plans/module_issues.md)), never inline fixes.

## Final confirm-fixes pass

After corrections are applied, run one lightweight pass re-reading the corrected artifact to catch
transcription drift introduced while applying fixes.

## Post-implementation review gate

No PR is approved on the implementer's `done` alone. Standard patches: in-loop `code-review`
(correctness) + human owner. Patches touching a high-risk coupling point (below): additionally one
**out-of-loop** reviewer running an OPEN-ENDED adversarial review of the diff (read-only `codex exec`,
or the fresh-Claude fallback — not merely a claim-checklist; use the "Attack axes for code diffs"
template above).
PASS = `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` zero-fail / zero-unexpected-skip
**and** both reviewers' Critical/Important items resolved or reasoned-rebutted **and** no
unescalated design/contract fork. Live-DB behavior changes additionally require the operational
checklist (local-only; see `doc/dev/review_checklist_local_template.md`).

## High-risk coupling points (name these explicitly in the review prompt)

- **`skill_metrics` / `long_forecasts` upsert keys + `horizon_value`** — long-term rows are keyed on
  a tuple including `date` and `horizon_value`, and **the per-horizon meaning of both fields is subtle
  and has changed over time** (`date` is an issue date for some horizons, a period-derived date for
  others; `horizon_value` is a configured lead for some horizons, a sentinel for others). **Verify the
  current semantics in `sapphire/services/postprocessing/app/models.py` and
  `apps/postprocessing_forecasts/src/api_writer.py` before changing any key** — mis-keying causes
  duplicate rows, stale survivors, or dropped leads.
- **`sapphire_api_client` ↔ `sapphire/services` enum label/value contract** — PostgreSQL stores the
  enum NAME while the API passes/returns the VALUE (e.g. `NAIVE_MEAN` vs `"Naive Mean"`); a mismatch
  surfaces as a 422. Verify model-name mapping on both write and read.
- **Dashboard readers + tombstone / `horizon_value`** — the dashboard read paths
  (`apps/forecast_dashboard/src/db.py`) and the postprocessing `data_reader` handle `horizon_value`
  and tombstone (`n_pairs == 0`) rows **differently per horizon and per reader** (some keep
  `horizon_value`, some drop it; some suppress tombstones). **Verify the specific reader before
  changing skill/lead keying** — a mismatch makes dashboard tiles silently break.
- **Hydrograph `round_3sf` actuals** — observed monthly/quarter/season actuals use the shared
  3-significant-figure discharge rounding (round-of-rounded cascade is deliberate). Do not apply that
  rounding to forecast outputs, and do not change the cascade. The helper name may differ by branch —
  verify the current symbol before relying on it.
- **Live DB / operational verification** — never commit station codes or discharge values; operational
  DB behavior changes require the local-only verification checklist.
- **`sapphire/services/` ownership boundary** — colleague-managed. Read and audit, but do not edit;
  any API-contract change (new endpoint, changed request/response schema) is a discussion first, not
  a code change.
