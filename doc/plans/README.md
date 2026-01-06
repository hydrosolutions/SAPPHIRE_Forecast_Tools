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
| `uv_migration_plan.md` | Python 3.11→3.12 + uv migration | Phase 6 (server testing) | **High** | Verify preprunoff with recent data |
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

## Related Resources

- **Skills**: `.claude/skills/issue-planning/` - Guides structured issue analysis
- **Skills**: `.claude/skills/executing-issues/` - Guides issue implementation
- **Serena memories**: `.serena/memories/` - Project context and conventions
- **Archive**: `doc/plans/archive/` - Superseded planning documents