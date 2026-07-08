# Agent Review & Verification Workflow

Procedure and templates for the multi-model review rules in
[`CLAUDE.md`](../../CLAUDE.md) (§ Multi-Model Review & Verification). This document is the
"how"; CLAUDE.md is the "must". Status vocabulary is owned by
[`../plans/README.md`](../plans/README.md); this doc does not restate it.

## Fitness statement (required in every high-claim-density prompt, verbatim)

> Every bullet must help an agent know what to inspect, what contract not to break, or what
> verification proves safety — otherwise cut it.

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

## Out-of-loop verifier requirements

- Different tool/model where possible — default `codex exec`; fallback = a fresh Claude
  general-purpose subagent with **no prior session context**.
- **Read-only.** The verifier must not edit repo files.
- The artifact is passed **in the prompt, before it is committed**.
- Run against the patch's **target branch in a clean checkout** (e.g. a worktree off
  `origin/maxat_sapphire_2`), NOT the current working tree — a dirty or wrong-branch tree yields
  false refutations. (Learned the hard way: a verifier run against the wrong branch falsely refuted
  `round_3sf` usage and dashboard tombstone suppression that are present on the target.)
- Output = per-claim verdict: `confirmed` / `refuted` / `unverifiable`, each with concrete evidence
  (path, symbol, test, or contract). No prose approval.

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
**out-of-loop** reviewer (`codex exec review` on the diff, or the fresh-Claude fallback).
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
