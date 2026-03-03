---
name: executing-plans
model: opus
description: Use when you have a written implementation plan to execute in a separate session with review checkpoints
---

# Executing Plans

## Overview

Load plan, review critically, execute tasks in batches, report for review between batches.

**Core principle:** Batch execution with checkpoints for architect review.

**Announce at start:** "I'm using the executing-plans skill to implement this plan."

## The Process

### Step 1: Load and Review Plan
1. Read plan file
2. Review critically - identify any questions or concerns about the plan
3. If concerns: Raise them with your human partner before starting
4. If no concerns: Create TodoWrite and proceed

### Step 2: Execute Batch
**Default: First 3 tasks**

For each task:
1. Mark as in_progress
2. Follow each step exactly (plan has bite-sized steps)
3. Run verifications as specified
4. Mark as completed

### Step 3: Report
When batch complete:
- Show what was implemented
- Show verification output
- Say: "Ready for feedback."

### Step 4: Continue
Based on feedback:
- Apply changes if needed
- Execute next batch
- Repeat until complete

### Step 5: Verify Test Completeness

Cross-reference what was written against the CLAUDE.md test categories:

| Category | Required when | Check |
|----------|---------------|-------|
| **Unit tests** | Always | Every new/modified public function has happy-path + error-path tests |
| **Edge case tests** | Code touches DataFrames, dates, or numerics | Empty data, NaN, date boundaries, value boundaries covered |
| **Integration tests** | Multi-step workflows or pipelines | Real logic tested end-to-end, only external boundaries mocked |
| **API failure tests** | Code uses `sapphire_api_client` | API unavailable, disabled, not ready, CSV fallback all tested |

**If gaps exist:** Write the missing tests before proceeding. Follow TDD — write the failing test first.

### Step 6: Update Documentation

Before moving to completion, check for documentation impact.
Search each file for references to changed/removed functionality:

1. Did inputs/outputs/usage change? → Update module README (`apps/<module>/README.md`)
2. Were modules added/removed or folder structure changed? → Update `README.md` (root)
3. Did module tables, architecture, or conventions change? → Update `CLAUDE.md`
4. Did configuration or env vars change? → Update `doc/configuration.md`
5. Did pipeline behavior change? → Update `doc/data_flow_*.md`
6. Did user-facing behavior change? → Update `doc/user_guide.md`
7. Did dev workflows or setup change? → Update `doc/development.md`
8. Did deployment procedures change? → Update `doc/deployment.md`, `doc/prod/`
9. Did stable patterns or project knowledge change? → Update Claude memory files
10. Is this fixing a known issue? → Update `doc/plans/module_issues.md`

**If no docs need updating:** State "No documentation impact" with brief rationale.

### Step 7: Complete Development

After all tasks complete and verified:
- Announce: "All tasks complete. Running pre-deployment validation."
- Use the `pre-deploy-validation` skill to verify tests, Docker builds, and deployment readiness

## When to Stop and Ask for Help

**STOP executing immediately when:**
- Hit a blocker mid-batch (missing dependency, test fails, instruction unclear)
- Plan has critical gaps preventing starting
- You don't understand an instruction
- Verification fails repeatedly

**Ask for clarification rather than guessing.**

## When to Revisit Earlier Steps

**Return to Review (Step 1) when:**
- Partner updates the plan based on your feedback
- Fundamental approach needs rethinking

**Don't force through blockers** - stop and ask.

## Remember
- Review plan critically first
- Follow plan steps exactly
- Don't skip verifications
- Reference skills when plan says to
- Between batches: just report and wait
- Stop when blocked, don't guess

---

## Related Skills

- **executing-issues**: Use instead for discrete GitHub issues (`gi_*.md` files)
- **pre-deploy-validation**: Use after completion to validate before deployment
- **issue-planning**: Use to break down large plans into discrete issues