# Module Issues Index

This file tracks planned issues across SAPPHIRE modules.

---

## preprocessing_gateway (prepg)

### Issue PREPG-001: Yearly Norm Recalculation for Snow and Meteo Data
**Status**: Draft
**Priority**: Medium
**File**: `issues/gi_draft_prepg_yearly_norm_recalculation.md`
**GitHub**: (not yet published)

Extract norm calculation from daily processing and implement yearly maintenance task for snow (SWE, HS, RoF) and meteo (T, P) norms with configurable averaging window.

---

## postprocessing_forecasts (pp)

### Plan: Postprocessing Module Improvement
**Status**: Draft
**Priority**: High
**File**: `postprocessing_forecasts_improvement_plan.md`
**GitHub**: (not yet published)

Comprehensive refactoring plan covering:
- 5 critical bug fixes (return value tracking, uninitialized variables, unsafe array access, non-atomic writes, silent API failures)
- Performance improvements (batch upsert, vectorized operations, client reuse)
- Module separation into operational (real-time) and maintenance (overnight) components
- Complete testing strategy with 60+ unit and integration tests

### Issue PP-001: Duplicate Skill Metrics for Ensemble Mean
**Status**: Draft (superseded by improvement plan)
**Priority**: High
**File**: `issues/gi_duplicate_skill_metrics_ensemble_composition.md`
**GitHub**: (not yet published)

Fix duplicate skill metrics entries for ENSEMBLE_MEAN by tracking ensemble model composition.
Note: This issue is addressed as part of the comprehensive improvement plan.

---
