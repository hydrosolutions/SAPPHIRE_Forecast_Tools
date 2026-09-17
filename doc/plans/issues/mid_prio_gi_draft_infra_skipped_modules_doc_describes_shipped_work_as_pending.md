# INFRA-056: two issue files still describe shipped-or-withdrawn `run_locally.sh` work as pending

**Status**: Draft (2026-09-11)
**Module**: `doc/plans/issues/review_gi_draft_infra_skipped_modules_leave_no_summary_line.md` and
`doc/plans/issues/review_gi_draft_infra_run_locally_degraded_state.md` (a documentation defect — no
code is in scope; see § Out of scope)
**Priority**: **Medium** — no production path is affected; the cost is reviewer and implementer
time, and the risk that someone re-implements or regresses code that already works, or spends effort
chasing scope (`DEGRADED`) that was deliberately withdrawn and never built.
**Labels**: `infra`, `documentation`, `run_locally`
**Found**: 2026-09-11, during the out-of-loop review of the LTF-010/LTF-011 status sweep (branch
`docs_ltf010_011_status_sweep`). The first file was found during that review; the second was found
by a further out-of-loop review pass over the same branch, which identified the same defect class in
a second, unrelated issue file. Both were deliberately left out of the LTF sweep and filed together
here: fixing either properly means auditing that issue's file end to end, which is separate work from
the LTF sweep, and splitting one defect class across two issues would just create a second partial
fix.
**Related**: **INFRA-030** (the issue whose file is one of the two audited here — it has shipped;
this issue is about its file's prose, not its code), **INFRA-044** (the issue whose file is the
other one audited here — its proposed `DEGRADED` status was withdrawn, owner decision 2026-09-04,
and never built; the file still presents parts of that withdrawn work as a live deliverable),
**LTF-010**, **LTF-011** (the status sweep during which both were found).

---

## The defect (two files, one class)

Both files describe work that is no longer pending — one because it shipped, one because it was
withdrawn — as though it were still open.

### File 1 — `review_gi_draft_infra_skipped_modules_leave_no_summary_line.md` (INFRA-030)

INFRA-030 has shipped: `apps/run_locally.sh` already has the three-way `print_summary` branch and
the `record_skip` helper its own issue file proposed. But two passages in that file still describe
that work as outstanding:

- **Lines 119-123**, under point 2 of the "Status-matching landscape" section: *"`print_summary`'s
  own branching is still `if PASS … else`, and was never changed. That means today **every**
  non-`PASS` status is counted as a failure and printed in the failure branch."*
- **Lines 125-130**, continuing the same point: *"**That leaves the fix squarely with this
  issue.** Step 2 must convert `print_summary` to three explicit branches — `PASS`, the fail set,
  and `SKIP` — rather than adding a `SKIP` case to an `if/else` whose `else` currently means
  'failure'."*

Both are false against the current tree. Verified 2026-09-11:

- `apps/run_locally.sh:2128` — `print_summary()` already has three explicit branches:
  `if [ "$status" = "PASS" ]` (`:2146`), `elif [ "$status" = "SKIP" ]` rendering
  `"  ${mod}: SKIP (${reason})"` (`:2149-2151`), and an `else` (`:2152-2163`) that carries an inline
  comment explaining it deliberately still means "any other status is a failure" — a closed
  `PASS`/`SKIP` pair plus an open-ended fail branch, not the two-way `if/else` the passages
  describe.
- `apps/run_locally.sh:402` — `record_skip()` exists, wrapping `record_result` with a `SKIP` status
  and a reason string, exactly as the file's own "Implementation sketch" § step 1 proposed.
- It is called at `apps/run_locally.sh:1564` (`preprocessing_gateway`, org-level skip) and `:1575`
  (`machine_learning (${mode})`, org-level skip) — both line numbers match this issue's citations
  in its own § Mechanism, and there are more call sites elsewhere in the file (the org-level and
  schedule-gate branches enumerated in its § Acceptance criteria).

All four of these line numbers (`:2128`, `:402`, `:1564`, `:1575`) were re-verified against the
current tree while drafting this issue and match what was expected — nothing here needed
re-deriving, unlike the file's own repeated "line numbers are stale, re-derive with `grep -n`"
caveats elsewhere.

The file's **own header** (`**Status**: Review (2026-09-07)`) and its own § Testing / §
Acceptance criteria checkboxes already record the fix as implemented, tested, and reviewed — so
the file contradicts itself: the header and the checklists say "done", while these two passages,
left over from when the issue was still open, say "not done". A reader who trusts the header will
be fine; a reader who lands on lines 119-130 first — which read like the live, operative
instruction, not a historical note — will not.

### File 2 — `review_gi_draft_infra_run_locally_degraded_state.md` (INFRA-044)

INFRA-044's `DEGRADED` state was withdrawn (owner decision, 2026-09-04) and never built — a fact the
file itself already records in two places: the `**C2 / C3 — WITHDRAWN (owner decision, 2026-09-04)**`
block (`:145-152`), and a `> **Superseded note (2026-09-07)**` block (`:182-187`) recording that
INFRA-030 shipped its own `SKIP` renderer independently, so nothing was left waiting on `DEGRADED`.
But two further passages in the same file still present `DEGRADED` as live, undone work:

- **`:190`, C5**: *"No other `record_result` call site becomes `DEGRADED` in this issue. **Adding
  the state is the deliverable**; classifying other modules into it is not."* `DEGRADED` was
  withdrawn — there is no state to add, so there is no deliverable for this clause to describe.
- **`:415`, § Out of scope**: *"Adding a `SKIP` state (INFRA-030) — this issue **only unblocks
  it**."* INFRA-030 shipped its `SKIP` state independently, with no dependency on this issue's
  (withdrawn) `DEGRADED` work — verified against the file's own superseded note at `:182-187`, which
  records exactly that.

Verified 2026-09-11 against the current tree: `:190` and `:415` both match the quoted text above.

Both passages sit downstream of, and contradict, notes the same file already carries earlier (the
C2/C3 `WITHDRAWN` block and the 2026-09-07 superseded note) — the identical partial-fix pattern as
File 1: one passage in the file was corrected (or arrived pre-corrected, in this case, as the
superseded note), its neighbour was left standing.

## Why it is worth filing rather than fixing inline

This is the repository's own recognised failure mode: a partial documentation fix that corrects
only the passage a reviewer happened to cite leaves the file's other stale passages standing, and
the issue reads as closed while remaining live elsewhere. Fixing either file properly means reading
the whole file for the same class of defect, not patching the passages named above — that is
separate work from the LTF-010/LTF-011 sweep during which both were found, hence a new issue rather
than an inline correction. Filing both files under one issue, rather than two, keeps the audit from
being split into a first pass that catches File 1 and a second, separately-filed pass that catches
File 2 — exactly the multi-round partial-fix pattern this whole class of defect is about.

The concrete harm: an implementer who opens either file to work on it — INFRA-030's, to extend
`print_summary` for a future status, or INFRA-044's, to reconsider `DEGRADED` — is told by the stale
passages that the work is still open, and may re-implement or regress code that already works, or
spend effort resurrecting scope the owner already declined.

## Scope

This issue is a **documentation audit of two files**:

- `doc/plans/issues/review_gi_draft_infra_skipped_modules_leave_no_summary_line.md` (INFRA-030)
- `doc/plans/issues/review_gi_draft_infra_run_locally_degraded_state.md` (INFRA-044)

- It does **not** propose any change to `apps/run_locally.sh`. The code is correct as shipped in
  both cases — INFRA-030's three-way `print_summary` branch and `record_skip` both work and are
  covered by `TestSkipSummaryRows`; INFRA-044's `DEGRADED` state was deliberately never built, so
  there is no code to reconcile against. Only the prose in each issue's own file describing that
  state of affairs is wrong.
- It does **not** propose changes to any other issue file. Nothing else has been audited to the
  same standard as these two; a third file exhibiting the same pattern is out of scope here (file
  separately if found — see § Out of scope).
- The audit must cover the **whole of both files**, not only the four passages quoted above. In
  this repository, fixing only the cited passages while other stale-status prose remains is treated
  as closing the issue while leaving the underlying defect live — that is explicitly not acceptable
  here (see § Acceptance criteria).
- Historical/dated passages that are legitimately describing a pre-merge or withdrawn state — e.g.
  File 1's "Status-matching landscape — read this before touching `print_summary` (added
  2026-09-07)" section, File 2's C2/C3 `WITHDRAWN` block and 2026-09-07 superseded note, or either
  file's "verify against the tree you are working on" caveats — are not in scope to delete or
  flatten. They are correct as historical record; the defect in both files is that some other
  passage slipped from "not yet done" or "not built" into being read as a still-current instruction
  after the fix shipped or the state was withdrawn. The fix is to make clear which is which (e.g. a
  superseded/resolved note, matching the pattern each file already uses elsewhere), not to remove
  the history.

## What to verify while auditing

- **File 1**: Re-read the file end to end and check every passage that describes `print_summary`,
  `record_result`, `record_skip`, `_is_fail_status`, or the skip-recording call sites against the
  current `apps/run_locally.sh`, the same way this issue's § The defect did for lines 119-130.
  Check the INFRA-044 `DEGRADED` passage in this file (lines 136-141) for the same class of drift —
  it already carries an owner-decision note that the status was "not built", which reads as
  current, but confirm nothing near it has since changed.
- **File 2**: Re-read the file end to end and check every passage that describes the `DEGRADED`
  state, C2/C3, C5, and the § Out of scope section against the file's own withdrawal notes
  (`:145-152`, `:182-187`) — the same way this issue's § The defect did for `:190` and `:415`. Check
  whether any other clause besides C5 and the Out-of-scope bullet still treats `DEGRADED` as a
  pending deliverable.
- Confirm both files' automated-test citations (`TestSkipSummaryRows` and its individual test names
  for File 1; any test names File 2 cites) still exist under those names in
  `apps/pipeline/tests/test_run_locally_orchestration.py` — a renamed test cited under its old name
  is the same class of staleness as a superseded prose passage.

## Out of scope

- Any change to `apps/run_locally.sh` or its behavior.
- Registering this issue in `doc/plans/module_issues.md` — tracker registration is handled
  separately by the owner.
- Auditing issue files other than these two, even if they exhibit the same pattern (file separately
  if found).

## Acceptance criteria

- [ ] No passage in
      `doc/plans/issues/review_gi_draft_infra_skipped_modules_leave_no_summary_line.md` describes
      INFRA-030's shipped deliverable (the `print_summary` three-way branch, `record_skip`, and the
      skip-recording call sites) as work yet to be done.
- [ ] No passage in `doc/plans/issues/review_gi_draft_infra_run_locally_degraded_state.md` describes
      the withdrawn `DEGRADED` state, or any dependency on it, as live or pending work — including
      C5 (`:190`) and the § Out of scope bullet (`:415`).
- [ ] Dated/historical passages that correctly describe a pre-merge or withdrawn state (File 1's
      2026-09-07 "Status-matching landscape" note and PR #495 "verify against the tree" caveat;
      File 2's C2/C3 `WITHDRAWN` block and 2026-09-07 superseded note; and similar in either file)
      are preserved as historical record, not deleted or flattened — corrected only by clarifying
      that they predate the fix or the withdrawal, so each file's history of its issue remains
      legible.
- [ ] Both files have been read end to end for this class of defect, not only the four passages
      named in this issue — fixing only those four without the read-through does not satisfy this
      acceptance criterion.
- [ ] `apps/run_locally.sh` is unmodified by this issue's fix.
