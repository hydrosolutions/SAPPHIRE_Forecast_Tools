# Phase 7: Package Upgrades (Post-Migration)

**Extracted from**: `uv_migration_plan.md` (archived 2026-01-29)
**Status**: Not started
**When**: Incrementally, as part of module refactoring work

---

## Overview

This document tracks package upgrade work following the Python 3.12 + uv migration.

> **Note**: Package upgrades will be performed when working on refactoring each module, rather than as a separate bulk upgrade. This allows testing changes in context and reduces risk of introducing regressions across multiple modules simultaneously.

---

## Upgrade Procedure

### 1. Update iEasyHydroForecast (base library first)

```bash
cd apps/iEasyHydroForecast
uv lock --upgrade
uv sync
uv run pytest tests/ -v
```

### 2. Update each dependent module

```bash
# For each module:
cd apps/<module>
uv lock --upgrade
uv sync
# Run tests if available
```

### 3. Rebuild and push all images

Push changes to trigger CI/CD rebuild of all images.

### 4. Full server validation

Re-test complete forecast workflow with updated packages.

---

## Module Upgrade Status

| Module | Status | Notes |
|--------|--------|-------|
| iEasyHydroForecast | [ ] Not started | Base library - update first |
| preprocessing_runoff | [ ] Not started | |
| preprocessing_gateway | [ ] Not started | |
| preprocessing_station_forcing | [ ] Not started | |
| linear_regression | [ ] Not started | |
| machine_learning | [ ] Not started | Heavy deps (torch, darts) |
| postprocessing_forecasts | [ ] Not started | |
| forecast_dashboard | [ ] Not started | Panel/Bokeh stack |
| pipeline | [ ] Not started | Luigi orchestration |

---

## Known Package Updates Available

As of 2025-12-17 (check for newer versions before upgrading):

| Package | Version at Migration | Priority | Notes |
|---------|---------------------|----------|-------|
| scikit-learn | 1.7.2 | Medium | Check compatibility with ML models |
| urllib3 | 2.6.0 | High | Security updates |
| pytest | 9.0.1 | Low | Dev dependency |
| filelock | 3.20.0 | Low | |
| pre-commit | 4.5.0 | Low | Dev dependency |
| tzdata | 2025.2 | Low | Timezone data |

---

## Special Considerations

### Machine Learning Module

The ML module has heavy dependencies (torch, darts) that require careful testing:

1. **Pin major versions** to avoid breaking changes:
   ```toml
   torch = ">=2.8.0,<3.0"
   darts = ">=0.35.0,<0.36"
   ```

2. **Test model predictions** after any torch/darts upgrade

3. **DockerHub Scout grade** will likely remain C/D due to torch's dependency tree (see archived migration plan for rationale)

### Forecast Dashboard

Panel/Bokeh compatibility is critical:

1. **Bokeh <3.5** required for `FuncTickFormatter` compatibility
2. Test dashboard rendering after any Panel/Bokeh upgrade
3. Verify all interactive features work

---

## Upgrade Checklist (per module)

- [ ] Run `uv lock --upgrade`
- [ ] Run `uv sync`
- [ ] Run tests: `uv run pytest tests/ -v`
- [ ] Build Docker image locally
- [ ] Test Docker container
- [ ] Push to trigger CI/CD
- [ ] Verify CI/CD passes
- [ ] Test on server if applicable

---

## References

- [uv documentation - Upgrading dependencies](https://docs.astral.sh/uv/concepts/projects/dependencies/#upgrading-dependencies)
- Archived migration plan: `doc/plans/archive/uv_migration_plan_COMPLETED_2026-01-29.md`

---

*Document created: 2026-01-30*
