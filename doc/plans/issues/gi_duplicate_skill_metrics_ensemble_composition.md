# Issue: Duplicate Skill Metrics for Ensemble Mean and Missing Ensemble Composition Tracking

## Problem Description

### Observed Behavior
When writing skill metrics to the SAPPHIRE API, we encounter a **500 Internal Server Error** caused by duplicate entries in the skill metrics data for the same `(horizon_type, code, model_type, date, horizon_in_year)` combination.

From the database logs:
```sql
('PENTAD', '16151', 'ENSEMBLE_MEAN', '2026-01-21'::date, 3, 0.1067, 0.9886, 0.871, 0.9231, 0.1144, 13.0, 987)
('PENTAD', '16151', 'ENSEMBLE_MEAN', '2026-01-21'::date, 3, 56.6635, -3209.7469, NULL, NULL, 0.2581, 2.0, 988)

('PENTAD', '16153', 'ENSEMBLE_MEAN', '2026-01-21'::date, 3, 0.1627, 0.9735, 0.357, 1.0, 0.0392, 15.0, 994)
('PENTAD', '16153', 'ENSEMBLE_MEAN', '2026-01-21'::date, 3, NULL, NULL, 0.37, 1.0, 0.0061, 1.0, 995)
```

The same station code (16151, 16153) has multiple ENSEMBLE_MEAN entries for the same pentad on the same date, with drastically different metric values.

### Root Cause
**Ensemble Mean (EM)** is not a fixed model - it's a **dynamic average of well-performing models** that varies by:
- Station (code)
- Time period (pentad/decade)
- Historical performance of individual models

For example:
- Station A, Pentad 3: EM = average(LR, TFT, TiDE)
- Station A, Pentad 5: EM = average(TFT, TSMixer)
- Station B, Pentad 3: EM = average(LR, TFT, TiDE, TSMixer)

The current system calculates skill metrics for each unique ensemble composition but doesn't track **which models** are included in each ensemble. This results in multiple EM entries that appear as duplicates but represent different model combinations.

### Current Data Model Limitation
The current unique constraint on skill_metrics is:
```python
UniqueConstraint('horizon_type', 'code', 'model_type', 'date', name='uq_skill_metrics_horizon_code_model_date')
```

This doesn't account for:
1. Different ensemble compositions (which models are included)
2. Different `horizon_in_year` values (though this may be intentional)

## Proposed Solutions

### Option A: Add Ensemble Composition Tracking (Recommended)

Add a new field to track which models compose the ensemble:

**1. Database Schema Change**
```python
# In models.py - SkillMetric
ensemble_models = Column(String(100), nullable=True)  # e.g., "LR,TFT,TiDE" or "TFT,TSMixer"
```

**2. Update Unique Constraint**
```python
UniqueConstraint('horizon_type', 'code', 'model_type', 'date', 'ensemble_models',
                 name='uq_skill_metrics_horizon_code_model_date_ensemble')
```

**3. Update API Schema**
```python
# In schemas.py - SkillMetricBase
ensemble_models: Optional[str] = None  # Comma-separated list of models in ensemble
```

**4. Update Skill Metrics Calculation**
Modify the skill metrics calculation to include which models are in the ensemble:
- Track model selection logic
- Pass composition to output DataFrame
- Include in API payload

**Pros:**
- Complete traceability of ensemble composition
- Enables analysis of which model combinations perform best
- Solves the duplicate issue correctly

**Cons:**
- Requires database migration
- Requires changes across multiple modules

### Option B: Aggregate/Deduplicate Before API Write (Quick Fix)

Deduplicate skill metrics before sending to API by keeping only the "best" or "first" entry:

```python
# In _write_skill_metrics_to_api
data = data.drop_duplicates(
    subset=['code', 'model_short', f'{horizon_type}_in_year'],
    keep='first'
)
```

**Pros:**
- Quick to implement
- No database changes needed

**Cons:**
- Loses information about different ensemble compositions
- Arbitrary choice of which duplicate to keep
- Doesn't address root cause

### Option C: Include horizon_in_year in Unique Constraint

Update the database constraint to include `horizon_in_year`:

```python
UniqueConstraint('horizon_type', 'code', 'model_type', 'date', 'horizon_in_year',
                 name='uq_skill_metrics_horizon_code_model_date_horizon')
```

**Pros:**
- Allows different skill metrics per horizon within the same day

**Cons:**
- Still doesn't solve the duplicate EM issue for the same horizon_in_year
- Doesn't track ensemble composition

## Recommendation

**Implement Option A (Ensemble Composition Tracking)** with Option B as an immediate workaround.

### Immediate Workaround (Option B)
To unblock current operations, add deduplication in `_write_skill_metrics_to_api()`:

```python
# Deduplicate - keep first entry for each (code, model_short, horizon_in_year)
data = data.drop_duplicates(
    subset=['code', 'model_short', horizon_in_year_col],
    keep='first'
)
logger.warning(f"Deduplicated skill metrics: {original_count} -> {len(data)} records")
```

### Long-term Solution (Option A)
1. Add `ensemble_models` field to track composition
2. Update skill metrics calculation to capture which models are included
3. Update unique constraint
4. Update API schemas and client

## Files Affected

### Immediate Workaround
- `apps/iEasyHydroForecast/forecast_library.py` - `_write_skill_metrics_to_api()`

### Long-term Solution
- `sapphire/services/postprocessing/app/models.py` - SkillMetric model
- `sapphire/services/postprocessing/app/schemas.py` - SkillMetricBase schema
- `apps/iEasyHydroForecast/forecast_library.py` - skill metrics calculation and API write
- `apps/postprocessing_forecasts/` - ensemble calculation logic
- Database migration script

## Questions to Resolve

1. Where exactly is the ensemble model selection logic? (Need to trace the code that decides which models go into EM)
2. Should Neural Ensemble (NE) also track its composition, or is it always TFT+TiDE+TSMixer?
3. Is `horizon_in_year` intentionally excluded from the unique constraint, or should it be added?

## Related

- SAPPHIRE API Integration Plan: `doc/plans/sapphire_api_integration_plan.md`
- Postprocessing module: `apps/postprocessing_forecasts/`
