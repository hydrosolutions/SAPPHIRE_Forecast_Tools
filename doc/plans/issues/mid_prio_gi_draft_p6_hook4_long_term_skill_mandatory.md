# Flip P6 hook 4 (long-term skill recalc) from graceful-skip to mandatory

**Priority:** mid (operator-trust concern; can ship after P6)
**Type:** infra / migration toolkit
**Discovered:** 2026-06-07 during P6 review
**Blocks:** none (P6 ships with hook 4 graceful-skip + WARNING)
**Blocked by:** the long-term skill recalculation script (see "Pre-conditions")

## Summary

P6's regenerate-hooks meta-wrapper (`bin/initialize_regenerate_hooks.sh`)
runs four hooks by default: snow stats, hydrograph MONTH/SEASON,
short-term skill, and long-term skill. The first three are MANDATORY —
a missing script aborts the run with a hard error during the preflight
check. The fourth (long-term skill) is GRACEFUL-SKIP because the
underlying recalculation script does not yet exist on
`develop_migration_toolkit`.

When hook 4 gracefully skips, the wrapper emits a prominent WARNING:

> long-term skill recalc skipped (script `<name>` not deployed). You
> must manually recompute long-term skill metrics after long-term
> forecast data is populated. See follow-up:
> `doc/plans/issues/mid_prio_gi_draft_p6_hook4_long_term_skill_mandatory.md`.

This issue tracks the work to flip hook 4 to mandatory once the
underlying script lands.

## Why this is graceful-skip today

The architecture plan (§Q10) lists four regenerate hooks. The long-term
skill recalculation script does not yet exist on
`develop_migration_toolkit`. Two options were considered for P6:

1. Block P6 ship until the long-term skill recalc script lands.
2. Ship P6 with the long-term skill hook gracefully skipping + a
   prominent operator-facing WARNING.

Option 2 was chosen so P6's operational safety (cron-pause discipline,
late-start guard, mandatory-hook preflight for hooks 1-3) can land
without waiting on the long-term skill recalc work.

## Resolution path

When the long-term skill recalculation script lands on
`develop_migration_toolkit`:

1. **Update `bin/initialize_regenerate_hooks.sh`**:
   - Add the new script's path to `_preflight_validate_hooks()` so
     a missing hook 4 script aborts the run (matching the policy
     for hooks 1-3).
   - Remove the WARNING-on-graceful-skip branch from the hook 4
     execution path.
   - Update the dry-run inventory marker: `[GRACEFUL SKIP — see WARNING]`
     becomes `[MISS fatal]` when missing.

2. **Update tests**:
   - Flip `test_missing_long_term_skill_script_is_graceful_skip_with_warning`
     to `test_missing_long_term_skill_script_aborts_run` (mirrors the
     existing tests for hooks 1-3).
   - Remove the preflight test that pins the asymmetry.

3. **Update docs**:
   - Runbook §7.2 (Pre-flight): remove the carve-out that says hook 4
     gracefully skips.
   - Runbook §7.5 (Per-hook details): remove the WARNING text + the
     "manual recompute" instruction.
   - gi_draft §"Operational risks": remove the graceful-skip discussion.

4. **Close this issue + remove the WARNING reference** from the wrapper
   source.

## Acceptance

- [ ] The long-term skill recalc script exists at the canonical
      location and is executable.
- [ ] P6 `--dry-run` shows hook 4 as `[OK]` or `[MISS fatal]` (never
      `[GRACEFUL SKIP]`).
- [ ] The flipped test `test_missing_long_term_skill_script_aborts_run`
      passes.
- [ ] All four hooks now share the same MANDATORY policy.

## Out of scope

- Implementing the long-term skill recalculation script itself (that's
  a separate piece of work driven by the long-term forecasting module).
- Migrating long-term skill metrics historically (covered by P5's
  long-term forecast migration + downstream skill-metric work).

## Process note

Filed during the P6 round-2 review when the maintainer + reviewer
agreed that hook 4 should remain graceful for the initial P6 ship
because the underlying script is not yet deployed, but should flip to
mandatory once the script lands. The WARNING text in
`bin/initialize_regenerate_hooks.sh` references this issue file by
path so the operator has a stable artifact to consult.
