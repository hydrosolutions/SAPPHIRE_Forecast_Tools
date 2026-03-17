---
name: executing-issues
model: opus
description: Execute a discrete GitHub issue from a detailed plan file (gi_*.md). Use when implementing a specific, well-defined task with an existing implementation plan in doc/plans/issues/. Handles status tracking, verification, and issue lifecycle.
---

# Executing Issues

## Overview

Execute a discrete GitHub issue from a detailed implementation plan (`gi_*.md` file).

**When to use:**
- Implementing a specific issue with an existing plan in `doc/plans/issues/`
- The plan follows the issue-planning template (has Tasks, Acceptance Criteria, etc.)

**When NOT to use:**
- High-level architecture plans (use `executing-plans` instead)
- Plans that span multiple issues
- Exploratory or research tasks

**Announce at start:** "I'm using the executing-issues skill to implement [issue name]."

---

## The Process

### Step 1: Load Issue Plan

1. Read the issue file from `doc/plans/issues/gi_*.md`
2. Verify it has the required sections:
   - Summary
   - Implementation Steps (with checkboxes)
   - Acceptance Criteria
   - Testing section
3. If sections missing: Stop and ask user to complete the plan first

### Step 2: Update Status

1. Update the issue file: `**Status**: Draft` → `**Status**: In Progress`
2. Update `doc/plans/module_issues.md`: Change status to `In Progress`

### Step 3: Review Plan Critically

Before implementing:
1. Read the Technical Analysis and Implementation Plan sections
2. Verify the referenced files exist at the specified paths
3. Check if any dependencies are unmet
4. Identify any questions or concerns

**If concerns:** Raise them with user before starting implementation
**If no concerns:** Create TodoWrite from Implementation Steps and proceed

### Step 4: Execute Implementation

**Batch size: 3 tasks at a time**

For each task in Implementation Steps:
1. Mark as `in_progress` in TodoWrite
2. Follow the step exactly as written
3. If step references specific code: implement as shown
4. Mark checkbox in issue file: `- [ ]` → `- [x]`
5. Mark as `completed` in TodoWrite

**After each batch:**
- Show what was implemented
- Run any specified tests
- Say: "Batch complete. Ready for feedback."
- Wait for user input before continuing

### Step 5: Run Tests

After all implementation steps complete:
1. Run the test commands specified in the Testing section
2. Run any manual verification steps
3. Report results

**If tests fail:**
- Stop and report the failure
- Ask user how to proceed

### Step 6: Verify Test Completeness

Cross-reference what was written against the CLAUDE.md test categories.
For each category, check whether it applies and whether tests exist:

| Category | Required when | Check |
|----------|---------------|-------|
| **Unit tests** | Always | Every new/modified public function has happy-path + error-path tests |
| **Edge case tests** | Code touches DataFrames, dates, or numerics | Empty data, NaN, date boundaries, value boundaries covered |
| **Integration tests** | Multi-step workflows or pipelines | Real logic tested end-to-end, only external boundaries mocked |
| **API failure tests** | Code uses `sapphire_api_client` | API unavailable, disabled, not ready, CSV fallback all tested |

**Report to user:**
```
Test completeness check:
- Unit tests: [covered / gap: ...]
- Edge cases: [covered / not applicable / gap: ...]
- Integration: [covered / not applicable / gap: ...]
- API failure: [covered / not applicable / gap: ...]
```

**If gaps exist:** Write the missing tests before proceeding. Follow TDD — write the failing test first, then fix if needed.

### Step 7: Verify Acceptance Criteria

Go through each acceptance criterion:
1. Check if it's met
2. Mark checkbox in issue file: `- [ ]` → `- [x]`
3. If any criterion not met: Stop and report

### Step 8: Update Documentation

Before moving to review, check for documentation impact:

1. **Check the issue plan** for a "Documentation Impact" section — update listed docs
2. **If no section exists**, assess impact yourself.
   Search each file for references to changed/removed functionality:
   - Did inputs/outputs/usage change? → Update module README (`apps/<module>/README.md`)
   - Were modules added/removed or folder structure changed? → Update `README.md` (root)
   - Did module tables, architecture, or conventions change? → Update `CLAUDE.md`
   - Did configuration or env vars change? → Update `doc/configuration.md`
   - Did pipeline behavior change? → Update `doc/data_flow_*.md`
   - Did user-facing behavior change? → Update `doc/user_guide.md`
   - Did dev workflows or setup change? → Update `doc/development.md`
   - Did deployment procedures change? → Update `doc/deployment.md`, `doc/prod/`
   - Did stable patterns or project knowledge change? → Update Claude memory files
   - Is this fixing a known issue? → Update `doc/plans/module_issues.md`
3. **Update documentation in the same working tree** — docs ship with code, not after

**If no docs need updating:** State "No documentation impact" with brief rationale.

### Step 9: Submit for Review

**Do NOT mark the issue as Complete. The user reviews first.**

1. Update issue file status: `**Status**: In Progress` → `**Status**: Review`
2. Update `doc/plans/module_issues.md`: Change status to `Review`
3. Present a review summary:

   ```
   Issue [name] ready for review.

   Summary of changes:
   - [list of files modified/created]

   Tests:
   - [pass/fail status]
   - Test completeness: [unit/edge/integration/API — which apply, all covered]

   Documentation updated:
   - [list of docs changed, or "No documentation impact"]

   Acceptance criteria: All met
   - [list each criterion with status]
   ```

4. **STOP and wait for user review.**
   - The user may request changes, ask questions, or approve.
   - If changes requested: implement them, re-run tests, and re-submit for review.

### Step 10: Complete Issue

**Only after user approves the review:**

1. Update issue file status: `**Status**: Review` → `**Status**: Complete`

2. Update `doc/plans/module_issues.md`:
   - Change status to `Complete`
   - Add resolution date if not present

3. Report completion:
   ```
   Issue [name] complete.

   Next steps:
   - [ ] Create PR (if not already done)
   - [ ] Publish to GitHub Issues (if still draft)
   ```

4. **If issue is still a draft** (`*_gi_draft_*.md`):
   - Rename the file: replace the priority prefix with `review_` (e.g., `high_prio_gi_draft_foo.md` → `review_gi_draft_foo.md`)
   - Remind user: "This issue is ready to publish to GitHub. After creating the GitHub issue, rename the file to `<priority>_gi_<id>_<desc>.md` and move to `archive/` when done."

---

## File Locations

| File | Purpose |
|------|---------|
| `doc/plans/issues/gi_*.md` | Detailed implementation plans |
| `doc/plans/module_issues.md` | Issue index (update status here) |

---

## When to Stop and Ask

**STOP immediately when:**
- A referenced file doesn't exist
- Code example in plan doesn't match actual code structure
- Test fails and you don't know how to fix it
- Acceptance criterion is ambiguous
- You need to make a design decision not covered in the plan

**Ask for clarification rather than guessing.**

---

## Checklist

Before starting:
- [ ] Issue file exists in `doc/plans/issues/`
- [ ] Plan has Implementation Steps with checkboxes
- [ ] Plan has Acceptance Criteria
- [ ] Plan has Testing section

During execution:
- [ ] Status updated to In Progress
- [ ] Each step followed exactly
- [ ] Checkboxes marked as completed
- [ ] Tests run after each batch

Before review:
- [ ] All tests pass (zero skips unless dependency-gated)
- [ ] Test completeness verified against CLAUDE.md categories
- [ ] All acceptance criteria met and checked off
- [ ] Documentation updated (or "No impact" stated with rationale)
- [ ] Status updated to Review
- [ ] Review summary presented to user

After user approval:
- [ ] Status updated to Complete
- [ ] module_issues.md updated
- [ ] User reminded about GitHub publication (if draft)

---

## Example Flow

```
User: "Implement the preprunoff operational modes issue"

Claude: "I'm using the executing-issues skill to implement PREPQ-001.

Loading doc/plans/issues/gi_draft_preprunoff_operational_modes.md...

The plan has 5 implementation steps and 7 acceptance criteria.

Updating status to In Progress...

Starting batch 1 (steps 1-3):
1. Create config.yaml
2. Create config.py
3. Add mode parameter handling

[implements steps]

Batch complete. Files created:
- apps/preprocessing_runoff/config.yaml
- apps/preprocessing_runoff/src/config.py

Ready for feedback."

[... more batches, tests pass ...]

Claude: "Test completeness check:
- Unit tests: covered (8 tests for 3 new functions)
- Edge cases: covered (empty config, missing keys)
- Integration: covered (config → mode selection → execution)
- API failure: not applicable (no API calls in this change)

Documentation updated:
- apps/preprocessing_runoff/README.md — added mode parameter docs

Issue PREPQ-001 ready for review.

Summary of changes:
- apps/preprocessing_runoff/config.yaml (new)
- apps/preprocessing_runoff/src/config.py (new)
- apps/preprocessing_runoff/main.py (modified)

Tests: 8 passed, 0 failed, 0 skipped
Acceptance criteria: All 7 met

[Status → Review. Waiting for your approval.]"

User: "Looks good, one thing — add a note about default mode to the README"

Claude: [makes change, re-runs tests]
"Updated. Ready for review again."

User: "Approved"

Claude: "Status → Complete. Issue PREPQ-001 done.
This is still a draft — ready to publish to GitHub."
```

---

## Related Skills

- **issue-planning**: Use to create the detailed plan before executing
- **test-driven-development**: Follow during Step 4 (implementation) and Step 6 (filling test gaps)
- **documentation**: Reference for doc writing conventions during Step 8
- **requesting-code-review**: Use after user approval for additional peer review if needed
- **pre-deploy-validation**: Use after completion to validate before deployment
- **software-architecture**: Reference for code conventions
