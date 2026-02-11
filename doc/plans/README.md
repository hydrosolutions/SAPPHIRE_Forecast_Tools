# Planning Documents

This directory contains planning documents and issue tracking for the SAPPHIRE Forecast Tools project.

## Directory Structure

```
doc/plans/
├── README.md                    # This file
├── module_issues.md             # Index of all known issues (summary only)
├── issues/                      # Detailed issue implementation plans
│   ├── gi_draft_<module>_<desc>.md   # Draft issues (not yet on GitHub)
│   └── gi_<id>_<desc>.md             # Published issues (linked to GitHub)
└── *.md                         # Architecture and planning documents
```

## Workflow

### Planning Documents vs GitHub Issues

| Type | Purpose | Location |
|------|---------|----------|
| **Planning docs** | Architecture decisions, migration plans, research, multi-issue initiatives | `doc/plans/*.md` |
| **Issue index** | Summary/registry of all known issues | `doc/plans/module_issues.md` |
| **Issue plans** | Detailed implementation plans for discrete tasks | `doc/plans/issues/gi_*.md` |
| **GitHub Issues** | Public task tracking, PR linking, assignment | github.com |

### Issue Lifecycle

```
1. Discovery        → Add summary to module_issues.md
2. Planning         → Create gi_draft_<module>_<desc>.md with detailed plan
3. Review           → Review plan, refine until ready
4. Publish          → Create GitHub issue, rename file to gi_<id>_<desc>.md
5. Implementation   → Work on issue, PRs link automatically
6. Completion       → Close GitHub issue, update module_issues.md status
```

### File Naming Conventions

**Draft issues** (before GitHub):
```
gi_draft_<module>_<short_description>.md
```

**Published issues** (after GitHub):
```
gi_<github_id>_<short_description>.md
```

Examples:
- `gi_draft_prepq_operational_modes.md` → draft
- `gi_42_prepq_operational_modes.md` → published as GitHub #42

### Module Abbreviations

| Module | Abbreviation |
|--------|--------------|
| preprocessing_runoff | `prepq` |
| preprocessing_gateway | `prepg` |
| preprocessing_station_forcing | `prepf` |
| linear_regression | `lr` |
| machine_learning | `ml` |
| postprocessing_forecasts | `pp` |
| forecast_dashboard | `fd` |
| configuration_dashboard | `cd` |
| pipeline | `p` |
| iEasyHydroForecast | `iEHF` |
| reset_forecast_run_date | `r` |
| cross-module/infrastructure | `infra` |

## Issue Plan Template

Each detailed issue plan (`gi_*.md`) should be:
- **Standalone**: A junior developer can implement without full project context
- **Detailed**: Includes specific files, functions, and code changes
- **Testable**: Clear acceptance criteria and test requirements

See the `issue-planning` skill for the full template and guidance.

## Plan Tracker

### Active Plans

| Plan | Purpose | Status | Priority | Next Action |
|------|---------|--------|----------|-------------|
| `archive/uv_migration_plan_COMPLETED_2026-01-29.md` | Python 3.11→3.12 + uv migration | ✅ Complete | - | Archived |
| `phase7_package_upgrades.md` | Post-migration package upgrades | Not started | Low | Incremental during refactoring |
| `documentation_improvement_plan.md` | Comprehensive doc overhaul | Phase 1 partial | Medium | Quick Start section |
| `docker_health_score_improvement.md` | Docker security improvements | Phase 1 done | Low | Test updated uv.lock files |
| `deployment_improvement_planning.md` | Local dev workflow + Makefile | Not started | Low | Create Makefile when needed |
| `future_development_plans.md` | Future features roadmap | Reference doc | — | No action (roadmap) |

### Active Issues

| Issue | Module | Status | Priority | Blocking |
|-------|--------|--------|----------|----------|
| `gi_draft_preprunoff_operational_modes.md` | prepq | Implementation complete | **High** | py312 Phase 6 |
| `gi_draft_linreg_bugfix.md` | lr | Implementation complete | **High** | py312 Phase 6 |

### Priority Legend

- **High**: Blocking other work or critical path
- **Medium**: Important but not blocking
- **Low**: Nice to have, do when convenient
- **—**: Reference documents, no action needed

### Status Values

For plans: `Not started` → `In progress` → `Complete`
For issues: `Draft` → `Ready` → `In progress` → `Implementation complete` → `Verified` → `Closed`

## Skill-Assisted Workflow

Use these Claude Code skills to guide the planning process:

| Phase | Skill | Command | Purpose |
|-------|-------|---------|---------|
| Exploration | brainstorming | `/brainstorming` | Clarify requirements, explore approaches |
| Planning | issue-planning | `/issue-planning` | Create detailed `gi_*.md` plan files |
| Implementation | executing-issues | `/executing-issues` | Execute discrete issues with plans |
| Large Plans | executing-plans | `/executing-plans` | Execute multi-issue architecture work |
| Deployment | pre-deploy-validation | `/pre-deploy-validation` | Verify before pushing to production |

**Start with `/use-skills`** if unsure which skill applies — it includes a workflow decision tree.

## Related Resources

- **Skills**: `.claude/skills/use-skills/` - Workflow guidance and skill selection
- **Skills**: `.claude/skills/issue-planning/` - Guides structured issue analysis
- **Skills**: `.claude/skills/executing-issues/` - Guides issue implementation
- **Serena memories**: `.serena/memories/` - Project context and conventions
- **Archive**: `doc/plans/archive/` - Superseded planning documents