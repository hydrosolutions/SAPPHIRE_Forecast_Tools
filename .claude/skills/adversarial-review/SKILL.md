---
name: adversarial-review
model: sonnet
description: "Runs the out-of-loop adversarial diff review required by CLAUDE.md § Multi-Model Review & Verification and doc/dev/agent_review_workflow.md § Attack axes for code diffs, in one invocation. Use when: (1) a patch is about to be declared done and it touches a high-risk coupling point or is otherwise non-trivial, (2) before a PR is opened, (3) re-reviewing a round of fixes made in response to an earlier review. Computes the diff, runs a fresh out-of-loop reviewer (codex exec, or a fresh Claude subagent if codex is unavailable) against the diff-attack template, and returns findings for the caller to triage. Does not fix anything itself."
---

# Adversarial Review

Single-command out-of-loop review of a **code diff** (not a plan or claim list) using the attack
axes in [`doc/dev/agent_review_workflow.md`](../../../doc/dev/agent_review_workflow.md) § "Attack
axes for code diffs". That doc owns the axes, the worked examples, and the process rules; this
skill only owns *how to invoke the review in one step*. Read that section before using this skill
if you have not already — it explains why claim-verification templates find nothing on a diff.

**This skill finds and reports findings. It does not fix anything and it does not decide what's
real.** Triage is the caller's job (see "Triage, not obey" below).

## When to use

- A patch is about to be declared "done" and either touches a high-risk coupling point (listed in
  the doc) or is otherwise non-trivial (see "When mandatory multi-model review applies" in the doc).
- Before opening a PR — this is the out-of-loop half of the "Post-implementation review gate".
- **Re-reviewing a round of fixes.** A fix is new, unreviewed code; run this again on the new diff
  after applying fixes from a prior round. Do not assume a fix is safe because the finding it
  addresses was real.

Not for reviewing plans, designs, or audit reports — those use the claim-verification template
earlier in the same doc, not this one.

## Procedure

### 1. Compute the diff under review

```bash
BASE="${1:-origin/maxat_sapphire_2}"
git fetch origin "${BASE#origin/}" 2>/dev/null || true
git diff "${BASE}...HEAD" > /tmp/adversarial-review-diff.txt
wc -l /tmp/adversarial-review-diff.txt   # sanity check it's non-empty
```

Run this from a clean checkout of the branch under review, diffing against `origin/<base>` — never
a stale local branch ref or the dirty working tree (see "Out-of-loop verifier requirements" in the
doc; a wrong-branch or stale-ref diff has produced false verdicts before).

### 2. Fill in the template's three required inputs

The diff alone is not enough context for a useful review — fill these in yourself before running
anything (do not skip this and hand the reviewer just the diff):

- **The bug being fixed** — one paragraph, what was broken and how it was observed.
- **The contracts that must hold** — what the operator/caller must observe, stated as behavior, not
  as "the diff should do X." Include any feature flag and its default if the diff is flag-gated.
- **The suspicious seams to attack** — the specific lines/objects in *this* diff most likely to hide
  a lifecycle, fallback, or invariant bug (see the axes in the doc for what counts).

### 3. Run the out-of-loop reviewer

Prefer `codex exec`. Write output straight to a file with `-o`; **do not pipe the run through
`tail`/`head`** — that buffers the whole run and hides progress until it either finishes or hangs
silently, which has already cost time on this project.

```bash
OUT=/tmp/adversarial-review-out.json
codex exec --skip-git-repo-check --sandbox read-only -C <clean_target_checkout> --color never \
  -o "$OUT" "$(cat <<'PROMPT'
<fill in the code-diff template from doc/dev/agent_review_workflow.md § "Code-diff `codex exec`
prompt template", substituting the diff and the three inputs from step 2>
PROMPT
)"
```

Let the command run to completion (use `run_in_background` if invoking from the Bash tool and you
need to keep working, not a piped foreground tail); then read `$OUT` directly with the Read tool.

**Fallback if `codex` is unavailable or errors out**: launch a fresh general-purpose Claude agent
with **no prior session context** and the same filled-in template as its entire prompt. This
mirrors the doc's fallback rule for the claim-verification path — different tool/model where
possible, and always a reviewer that has not seen this session's assumptions.

### 4. Triage, not obey

The reviewer's output is evidence, not a verdict to execute mechanically:

- For every returned finding, verify it against the **current** code before acting — file exists,
  line matches, the described behavior is actually what the code does. Findings scoped from stale
  context (an old diff, a moved line, a premise already fixed) do happen; reject those explicitly,
  with a one-line reason, rather than silently fixing fiction or silently dropping a real one.
- **Golden/kill-switch tests are supposed to pass with the flag off, both before and after the
  change** — that is their entire job. If the reviewer flags one of those as "vacuous" or
  "wouldn't catch a revert," that is a false positive on the vacuity axis, not a real finding;
  don't act on it, and don't let it stand unchallenged if the finding is reused elsewhere.
- Separate `introduced_by_diff: true` findings (fix now, in scope) from `false` (pre-existing —
  file as a `gi_draft_*` issue per "Accuracy fixes vs design forks" in the doc, don't scope-creep
  the current patch).
- After fixes land for any Critical/Important finding, the fix is itself new code — go back to
  step 1 with the new diff. Do not close the review on the strength of round one.

## Output

Report to the caller: the findings list (axis, file:line, severity, concrete failure scenario,
introduced-by-diff or pre-existing), which findings were rejected as stale/incorrect and why, and
which remain open for the human owner or a follow-up fix round.
