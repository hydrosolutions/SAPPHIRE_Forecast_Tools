# Security Updates Tracking

This document tracks security updates needed across the SAPPHIRE repository. It focuses on
Python package vulnerabilities and provides actionable checklists for remediation.

For broader security improvements (Docker health scores, image hardening), see
[`docker_health_score_improvement.md`](docker_health_score_improvement.md).

---

## Quick Reference: Update Commands

### Update a single package in one module
```bash
cd apps/<module_name>
uv lock --upgrade-package <package_name>
```

### Update all packages in one module
```bash
cd apps/<module_name>
uv lock --upgrade
```

### Verify a package version in a lock file
```bash
grep -A 2 'name = "<package_name>"' apps/<module_name>/uv.lock | grep version
```

---

## Testing Before Commit

**All security updates must be tested locally before committing.**

### General Testing Requirements

1. **Run module tests locally** with the updated environment:
   ```bash
   cd apps/<module_name>
   uv sync
   uv run pytest
   ```

2. **Verify the package version** is correct in the lock file after update.

3. **Build Docker image locally** (if module has Dockerfile):
   ```bash
   docker build -t <module_name>:test apps/<module_name>
   ```

### Module-Specific Testing

| Module | Additional Testing Required |
|--------|----------------------------|
| forecast_dashboard | Run integration tests (`TEST_LOCAL=1 pytest`), manually test dashboard functionality in browser |
| machine_learning | Verify ML training/prediction workflows still function |
| pipeline | Test Luigi task execution with Docker orchestration |

---

## Active Security Updates

### SEC-005: bokeh >= 3.8.2 (forecast_dashboard)

**Severity**: Low (1 Dependabot alert)
**Affects**: forecast_dashboard only
**Discovered**: 2026-01-27
**Status**: BLOCKED - requires code changes

**Blocker**: bokeh is pinned to `<3.5` in `apps/forecast_dashboard/pyproject.toml` due to
breaking changes in bokeh 3.5+ (`FuncTickFormatter` was removed).

**Resolution**: The fix for `FuncTickFormatter` has been implemented in the `maxat_sapphire_2`
branch. This security update will be resolved when `maxat_sapphire_2` is merged into main.

#### Status

| Module | Directory | Required | Status |
|--------|-----------|----------|--------|
| forecast_dashboard | `apps/forecast_dashboard` | >= 3.8.2 | [~] Blocked - awaiting maxat_sapphire_2 merge |

---

## Completed Security Updates

| ID | Package | Version | Date Completed | Notes |
|----|---------|---------|----------------|-------|
| SEC-001 | urllib3 | 2.6.3 | 2026-01-27 | All 9 modules updated |
| SEC-002 | aiohttp | 3.13.3 | 2026-01-27 | machine_learning |
| SEC-003 | filelock | 3.20.3 | 2026-01-27 | machine_learning (with SEC-002) |
| SEC-004 | virtualenv | 20.36.1 | 2026-01-27 | iEasyHydroForecast |

---

## Monitoring & Future Updates

### Packages to Monitor

These security-sensitive packages are used across most modules:

| Package | Current Version | Last Checked | Notes |
|---------|-----------------|--------------|-------|
| urllib3 | 2.6.3 (target) | 2026-01-27 | HTTP client core |
| requests | 2.32.5 | 2026-01-27 | HTTP client |
| certifi | 2025.11.12 | 2026-01-27 | TLS certificates |
| cryptography | — | — | (Check if used) |
| idna | 3.11 | 2026-01-27 | Unicode domains |
| aiohttp | 3.13.3 (target) | 2026-01-27 | Async HTTP (machine_learning) |
| bokeh | 3.8.2 (target) | 2026-01-27 | Visualization (forecast_dashboard) |

### How to Add New Security Updates

1. Create a new section under "Active Security Updates" with ID `SEC-XXX`
2. Include:
   - Package name and required version
   - Severity and CVE (if applicable)
   - Status table showing all affected modules
   - Update commands
   - Verification steps
3. Move to "Completed Security Updates" when done

### Automated Scanning

Currently no automated vulnerability scanning is configured. Consider adding:

- **Dependabot**: Configured at org level; creates PRs for individual modules
- **pip-audit**: Can be run locally: `pip-audit`
- **GitHub Code Scanning**: Enable in repository settings
- **Quarterly rebuild**: See [`scheduled_security_rebuild.yml`](../../.github/workflows/scheduled_security_rebuild.yml)
  for automated quarterly Docker rebuilds

---

## Related Documents

- [`docker_health_score_improvement.md`](docker_health_score_improvement.md) - Docker image security
- [`module_issues.md`](module_issues.md) - General issue tracking
- [`uv_migration_plan (archived)`](archive/uv_migration_plan_COMPLETED_2026-01-29.md) - Python 3.12 migration (completed 2026-01-29)

---

*Last updated: 2026-01-27*
