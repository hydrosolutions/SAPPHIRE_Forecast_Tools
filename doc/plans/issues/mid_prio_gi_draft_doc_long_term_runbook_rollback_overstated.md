# DOC-006: Documented `SAPPHIRE_SKILL_LEAD_AWARE` rollback overstates what the code actually reverts

**Priority**: Medium
**Module**: doc, ltf, pp
**Status**: Draft (partially addressed — see "Resolution status" below)
**Created**: 2026-08-12
**Updated**: 2026-08-12 (branch `docs_checklist_lead_aware_full_history_recalc`) — runbook
rollback paragraph corrected; three of four acceptance criteria closed, one remains open.

## Resolution status (2026-08-12)

The rollback paragraph in `doc/prod/long_term_deploy_runbook.md` was rewritten on this
branch. It now states plainly that the flag only changes read/write behavior for new
rows, does not delete or collapse already-written per-lead rows, does not revert PP-038
monthly stratification (with a citation to `skill_lead_aware_flag.py`), and points to
restoring the Phase 3 DB backup — cross-referenced to
`update_deployment_checklist.md` § 3.6, which independently carries the same corrected
framing — as the actual reversal mechanism.

This closes acceptance criteria 1-3 below. It does **not** close criterion 4: the two
"Investigation gaps" are code/data-behavior questions (upsert-collision risk on
collapsed keys; whether `skill_metrics` has the same no-delete-path property as
`long_forecasts`), and a documentation correction cannot resolve them — they require
someone to trace the write paths and either confirm safety or build cleanup tooling.
They remain open below, unchanged from the original draft, and are carried forward
here as the explicit follow-up the criterion asks for. **This issue stays in Draft**
rather than moving to Review, because one criterion is still unmet; do not re-verify
criteria 1-3 in a future pass, they are settled — only the investigation gaps need
further work.

## Problem

`doc/prod/long_term_deploy_runbook.md` (lines ~125-127) documents rollback as:

> "To roll back: set `SAPPHIRE_SKILL_LEAD_AWARE=false` (or remove the line) in
> the `.env`; the code reverts to the pre-feature single-lead behavior. Re-run
> the recalc if you need the stored rows collapsed back."

This was checked against the code and is **not accurate** in two respects.

## What was verified

1. **PP-038 monthly stratification is unconditional and is not reverted by the
   flag.** `apps/iEasyHydroForecast/skill_lead_aware_flag.py:11-15` states this
   directly in the module docstring: "flag-OFF must reproduce current trunk
   behavior byte-for-byte, INCLUDING the already-shipped PP-038 monthly
   per-lead skill/ensemble stratification ... PP-038 is unconditional trunk
   behavior, not gated by this flag, and must NOT be reverted by any flag-OFF
   path." So "the code reverts to the pre-feature single-lead behavior" is
   false for monthly skill/ensemble stratification specifically — that part of
   the per-lead behavior stays on regardless of the flag.
2. **There is no deletion path for already-written per-lead `long_forecasts`
   rows.** A repo-wide search for `DELETE FROM` and related row-removal logic
   in `apps/postprocessing_forecasts/src/*.py` and
   `apps/postprocessing_forecasts/*.py` (the modules gated by
   `skill_lead_aware_enabled()`) returned nothing — all write paths are
   upsert-shaped, not delete-then-rewrite. A flag-off recalc runs the
   collapsed/legacy code paths and **upserts** collapsed/legacy keys; it does
   not touch or remove rows that were already written under per-lead keys by
   earlier flag-on recalcs. Those rows are left in place. The runbook's own
   Phase 5 (Cleanup) section independently confirms this is understood
   elsewhere in the document — it describes manual, reviewer-approved `DELETE`
   predicates for "old-convention rows" as a **separate, deliberate step**, not
   something rollback does automatically. The rollback paragraph does not
   cross-reference Phase 5 or make clear that manual cleanup is what "collapsed
   back" actually requires.

## Why this matters

The rollback paragraph reads as "flip the flag, optionally re-run the recalc,
you're back to pre-feature state." In reality:

- A flag-off recalc after a flag-on period leaves **orphaned per-lead rows**
  in `long_forecasts` (and correspondingly in `skill_metrics`, per the
  DOC-005 sibling draft's finding that Phase 4 doesn't even check
  `skill_metrics`) alongside newly-upserted collapsed rows — a **mixed**
  persisted state, not a clean reversion.
- Monthly stratification never reverts at all, so "single-lead behavior"
  is simply wrong for that horizon post-rollback.
- An operator following the documented rollback step may reasonably decide to
  skip a DB backup beforehand, believing the flag alone is a safe, reversible
  toggle. If a later re-enable of the flag then meets this mixed state, the
  Phase 3 full-history recalc and Phase 4 verification queries (already
  incomplete per DOC-005) are the only things standing between that mixed
  state and an inconsistent dashboard.

## What accurate rollback documentation would need to say

1. Flipping `SAPPHIRE_SKILL_LEAD_AWARE=false` changes **read/write code paths**
   going forward; it does not retroactively alter or remove any row already
   persisted under the per-lead convention.
2. Monthly skill/ensemble stratification (PP-038) is **not** controlled by this
   flag in either direction — do not describe rollback as restoring "single-lead
   behavior" for monthly without that caveat.
3. A true reversion to pre-feature state requires one of:
   - Restoring the pre-recalc DB backup taken in Phase 3 ("Backup first" —
     already a required step; the rollback section should point to it
     explicitly as the real undo mechanism), or
   - Running the same reviewer-approved, explicit cleanup predicates described
     in Phase 5, scoped to remove per-lead-keyed rows rather than
     deprecated-model rows.
4. "Re-run the recalc if you need the stored rows collapsed back" should be
   corrected or removed — a flag-off recalc does not collapse existing per-lead
   rows; it only adds/updates collapsed-key rows alongside them.

## Investigation gaps (flag for owner, not resolved here)

- Whether a flag-off recalc's collapsed-key upserts could **collide** with (and
  silently overwrite) a per-lead row that happens to share the same collapsed
  key is not established by this draft and would need its own check before any
  fix is written.
- Whether `skill_metrics` per-lead rows have the same "no delete path" property
  as `long_forecasts` was inferred from the same grep (scoped to
  `postprocessing_forecasts`) but not separately traced end-to-end for the
  `skill_metrics` write functions specifically.

## Acceptance criteria

- [x] Runbook rollback paragraph no longer claims "reverts to pre-feature
      single-lead behavior" without the PP-038 monthly-stratification caveat.
      **Closed 2026-08-12**: the rewritten paragraph states monthly
      stratification is not gated by the flag at all, citing
      `skill_lead_aware_flag.py` lines 11-14.
- [x] Runbook rollback paragraph no longer implies a flag-off recalc collapses
      already-written per-lead rows.
      **Closed 2026-08-12**: the rewritten paragraph states explicitly that no
      recalc collapses already-persisted per-lead rows.
- [x] Runbook points to the Phase 3 DB backup (or an explicit Phase-5-style
      cleanup) as the actual mechanism for a full reversion.
      **Closed 2026-08-12**: the rewritten paragraph names the Phase 3 backup
      as the actual reversal path and cross-references
      `update_deployment_checklist.md` § 3.6 for the full procedure.
- [ ] The two investigation gaps above are either closed or explicitly carried
      forward as a follow-up note in the corrected text.
      **Still open**: this is a code/data-behavior question, not a wording
      question — a runbook-paragraph correction cannot close it. Carried
      forward unchanged in "Investigation gaps" above; needs someone to trace
      the collapsed-key upsert path for collision risk and confirm (or refute)
      the `skill_metrics` no-delete-path assumption end-to-end.

## References

- `doc/prod/long_term_deploy_runbook.md` lines ~125-133 (rollback paragraph,
  corrected 2026-08-12 — was lines ~125-127 pre-correction), and § "Phase 5 —
  Cleanup" (lines 370-396) for the contrasting deliberate cleanup-predicate
  pattern
- `doc/prod/update_deployment_checklist.md` § 3.6 "Rollback" (lines
  ~1433-1442) — carries the same corrected framing and is now
  cross-referenced from the runbook paragraph
- `apps/iEasyHydroForecast/skill_lead_aware_flag.py:1-25` (module docstring —
  PP-038 unconditional-behavior statement)
- Repo-wide `DELETE FROM` search over
  `apps/postprocessing_forecasts/{src,.}/*.py` — no matches (verified during
  this review)
- Sibling draft: `mid_prio_gi_draft_doc_long_term_runbook_skill_verification_gap.md`
  (DOC-005) — Phase 4 does not check `skill_metrics`, which is also what a
  rollback-then-reenable cycle would need verified
