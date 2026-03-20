# Issue Triage & Implementation Skill - Design Plan

## Overview

This skill will guide systematic investigation of issues, help maintain context during slow debugging workflows, and optionally proceed to implementation (bug fix or feature development).

## Problem Statement

1. **Context loss**: Debugging workflows are slow (Docker builds, server tests). Easy to lose track of what's been tried.
2. **Ad-hoc investigation**: No systematic approach to comparing environments, checking logs, tracing data flow.
3. **Scattered findings**: Investigation notes get lost in conversation history.
4. **Transition friction**: Moving from "what's wrong?" to "let's fix it" requires mental context switch.

## Proposed Skill Structure

```
Issue Triage & Implementation

1. Issue Intake
   ├── Capture symptoms
   ├── Expected vs actual behavior
   └── Create/update module_issues.md entry

2. Investigation Framework
   ├── Quick checks (logs, recent changes)
   ├── Environment comparison (local vs Docker vs server)
   ├── Data flow tracing
   └── Hypothesis testing

3. Progress Tracking
   ├── What's been tried
   ├── What's been ruled out
   └── Current hypothesis

4. Decision Point
   ├── Root cause identified? → Proceed to implementation
   ├── Need more info? → Continue investigation
   └── Blocked? → Document and pause

5. Implementation (optional)
   ├── Bug fix workflow
   ├── Feature development workflow
   ├── GitHub issue creation
   └── PR preparation
```

## Detailed Section Design

### Section 1: Issue Intake

**Purpose**: Capture the issue clearly before diving into investigation.

**Template**:
```markdown
## Issue: [Brief title]

**Symptoms**:
- What you observed

**Expected behavior**:
- What should happen

**Context**:
- When did it start? (recent change, always broken, intermittent)
- Where does it occur? (local, Docker, server, specific environment)
- Related modules/files

**Initial hypothesis** (if any):
- Your best guess at the cause
```

**Action**: Create or update entry in `implementation_planning/module_issues.md`

### Section 2: Investigation Framework

**Purpose**: Systematic checklist to avoid missing obvious causes.

#### 2.1 Quick Checks (do first)
- [ ] Recent git changes: `git log --oneline -10`
- [ ] Recent Docker image changes
- [ ] Error messages in logs
- [ ] Environment variables set correctly

#### 2.2 Environment Comparison
| Aspect | Local | Docker | Server |
|--------|-------|--------|--------|
| Python version | | | |
| Key env vars | | | |
| Data paths | | | |
| Network access | | | |
| File permissions | | | |
| Date/timezone | | | |

#### 2.3 Data Flow Tracing
For data processing issues:
1. Check input data exists and is readable
2. Trace through each processing step
3. Check intermediate outputs
4. Verify final output location and format

#### 2.4 Hypothesis Testing
Structure: "If X is the cause, then Y should be true"
- State hypothesis
- Design test to verify/disprove
- Record result
- Update hypothesis

### Section 3: Progress Tracking

**Purpose**: Maintain context across sessions, prevent re-doing work.

**Format**:
```markdown
## Investigation Progress: [Issue ID]

### Session 1 (YYYY-MM-DD)
**Tried**:
- Checked X → Result: Y
- Tested Z → Result: W

**Ruled out**:
- Not a permission issue (verified file ownership)
- Not a network issue (curl works from container)

**Current hypothesis**:
- Likely date calculation bug causing skip condition

**Next steps**:
1. Add debug logging to date calculation
2. Compare date values between local and Docker

### Session 2 (YYYY-MM-DD)
...
```

### Section 4: Decision Point

**Purpose**: Clear criteria for moving forward.

**Decision Tree**:
```
Root cause identified?
├── Yes → Is it a bug or missing feature?
│   ├── Bug → Section 5.1: Bug Fix Workflow
│   └── Feature → Section 5.2: Feature Development
│
├── Partially → What's blocking?
│   ├── Need server access → Document, wait
│   ├── Need external dependency → Document, escalate
│   └── Need more investigation → Return to Section 2
│
└── No →
    ├── Tried everything? → Escalate, ask for help
    └── More ideas? → Return to Section 2
```

### Section 5: Implementation

#### 5.1 Bug Fix Workflow

1. **Create GitHub issue** (optional):
   ```bash
   gh issue create --title "Bug: [description]" --body "[details from intake]"
   ```

2. **Create fix branch**:
   ```bash
   git checkout -b fix/[issue-id]-[brief-description]
   ```

3. **Implement fix**:
   - Make minimal changes to fix the issue
   - Add/update tests to prevent regression
   - Update documentation if needed

4. **Verify fix**:
   - Run tests locally
   - Test in Docker
   - Use **pre-deploy-validation** skill before server deploy

5. **Create PR**:
   ```bash
   gh pr create --title "Fix: [description]" --body "[link to issue, summary of changes]"
   ```

#### 5.2 Feature Development Workflow

1. **Create GitHub issue**:
   ```bash
   gh issue create --title "Feature: [description]" --body "[requirements, acceptance criteria]"
   ```

2. **Create feature branch**:
   ```bash
   git checkout -b feature/[issue-id]-[brief-description]
   ```

3. **Plan implementation**:
   - List files to modify
   - Identify dependencies
   - Break into smaller tasks if complex

4. **Implement**:
   - Write code incrementally
   - Add tests as you go
   - Commit logical chunks

5. **Verify**:
   - Full test suite passes
   - Docker build works
   - Use **pre-deploy-validation** skill

6. **Create PR**:
   - Include before/after if applicable
   - Link to GitHub issue

---

## Integration Points

### With Existing Resources

| Resource | How Skill Uses It |
|----------|-------------------|
| `module_issues.md` | Create/update issue entries during intake |
| `uv_migration_plan.md` | Reference for expected module behavior |
| `task_completion_checklist` (memory) | Use for implementation verification |
| `suggested_commands` (memory) | Reference for common commands |

### With Other Skills

| Skill | Relationship |
|-------|--------------|
| **pre-deploy-validation** | Use after implementing fix, before server deploy |
| **skill-creator** | Use if creating new workflow-related skills |

---

## Example Walkthrough: PR-001

**Using this skill for the preprocessing_runoff issue**:

### Intake
```markdown
## Issue: PR-001 - Runoff data not updated in Docker container

**Symptoms**:
- Module runs without errors but data files not updated
- Works locally, fails in Docker on server

**Expected behavior**:
- Fresh runoff data fetched and written to output files

**Context**:
- Discovered during py312 migration testing
- Affects Docker deployment only
```

### Investigation (from debug agent findings)
```markdown
### Session 1 (2025-12-18)
**Tried**:
- Debug agent analyzed code
- Found date calculation in should_update_data()

**Findings**:
- Date logic compares forecast date to data freshness
- If forecast date is "old", module skips silently
- Docker container may have different date context

**Root cause identified**:
- Date calculation bug in skip logic
- When run date is backdated, module thinks data is fresh

**Next steps**:
1. Fix date comparison logic
2. Add debug logging
3. Test with explicit date override
```

### Implementation
- Bug fix workflow
- Create fix branch: `fix/pr-001-date-calculation`
- Modify date logic in preprocessing_runoff
- Add test case for backdated runs
- Use pre-deploy-validation before server test

---

## Open Questions

1. **Automation level**: Should the skill prompt Claude to automatically create `module_issues.md` entries, or just provide the template?

2. **GitHub integration**: Should creating GitHub issues be a standard step, or optional based on severity?

3. **Session persistence**: How to best preserve investigation progress across conversation sessions? Options:
   - Update `module_issues.md` with progress
   - Create session-specific notes file
   - Rely on Serena memories

4. **Scope boundary**: When does an issue become too complex for this skill? Criteria for escalation?

---

## Implementation Plan

### Phase 1: Core Skill
- [ ] Create SKILL.md with sections 1-4 (intake through decision)
- [ ] Test with a real issue (P-001 or PR-001)
- [ ] Iterate based on feedback

### Phase 2: Implementation Section
- [ ] Add section 5 (bug fix and feature workflows)
- [ ] Integrate with pre-deploy-validation skill
- [ ] Add GitHub CLI integration guidance

### Phase 3: Polish
- [ ] Add more examples
- [ ] Refine checklists based on usage
- [ ] Consider helper scripts (e.g., environment comparison tool)

---

*Created: 2025-12-18*
