# Server Review Checklist — 2026-03-13

Verification plan for issue fixes that require server deployment.

## Pre-requisites

- All local checks from `review_checklist_local_2026-03-13.md` must pass first
- Branch merged to `main` and deployed to server
- Services running (`docker-compose up -d` in `sapphire/`)
- Database migrations applied

---

## Issue 1: API Client Missing Parameters

**Issue**: `review_gi_draft_api_client_missing_params.md`
**Status**: Draft — requires `sapphire-api-client` release first

### What was fixed
`sapphire-api-client` read methods missing query parameters:
- `read_forecasts()`: missing `model`, `target`, `start_target`, `end_target`
- `read_skill_metrics()`: missing `start_date`, `end_date`

### Server verification steps

1. **Verify updated client is installed**:
   ```bash
   pip show sapphire-api-client | grep Version
   # Must be version with new parameters
   ```

2. **Test new forecast query parameters**:
   ```python
   from sapphire_api_client import SapphireApiClient
   client = SapphireApiClient(base_url="http://localhost:8000")

   # These should work without error:
   forecasts = client.read_forecasts(model="LR", target="2026-03-15")
   forecasts = client.read_forecasts(start_target="2026-03-01", end_target="2026-03-15")
   ```

3. **Test new skill metrics parameters**:
   ```python
   metrics = client.read_skill_metrics(start_date="2026-01-01", end_date="2026-03-01")
   ```

4. **Verify forecast_dashboard migration** (once migrated):
   - Dashboard no longer uses raw `requests.get()` calls
   - Dashboard uses `SapphireApiClient` for all API calls
   - Filter controls on dashboard work correctly

### Pass criteria
- All new parameters accepted by client methods
- API returns filtered results (not full dataset)
- Dashboard displays correctly with filtered queries

---

## Issue 2: ML Forecast Read/Write Architecture Alignment

**Issue**: `review_gi_draft_fix_ml_forecast_api_reader.md`
**Status**: Review — Phases 1-2c done, Phase 3 (production deployment) pending

### What was fixed
- ML module wrote forecasts with inconsistent `horizon_type` (mix of day-level and period-level)
- Postprocessing reader didn't align with write format
- Created duplicate records and migration conflicts

### Server verification steps

1. **Pre-deployment: backup existing data**:
   ```bash
   # Dump current forecast records for comparison
   docker exec sapphire-postprocessing-api python -c "
   from app.database import SessionLocal
   from app.models import Forecast
   db = SessionLocal()
   count = db.query(Forecast).filter(Forecast.model_type == 'ML').count()
   print(f'ML forecast records: {count}')
   "
   ```

2. **Run data migration** (if migration script updated):
   ```bash
   docker exec -it sapphire-postprocessing-api /bin/bash
   python app/data_migrator.py --type forecast
   ```

3. **Verify no duplicate records after migration**:
   ```sql
   -- Check for duplicates on (code, date, model_type, horizon_type, period)
   SELECT code, date, model_type, horizon_type, period, COUNT(*)
   FROM forecasts
   WHERE model_type = 'ML'
   GROUP BY code, date, model_type, horizon_type, period
   HAVING COUNT(*) > 1;
   -- Expected: 0 rows
   ```

4. **Run one operational cycle and verify**:
   ```bash
   bash apps/run_locally.sh short-term
   ```
   Then check:
   - ML forecasts written with consistent `horizon_type`
   - Postprocessing reads ML forecasts without errors
   - No CardinalityViolation errors in logs

5. **Verify dashboard displays ML forecasts correctly**:
   - Forecast dashboard shows ML model results
   - No duplicate entries for same station/date/period

### Pass criteria
- Zero duplicate forecast records in DB
- Consistent `horizon_type` across all ML writes
- Clean postprocessing read (no alignment errors)
- Dashboard renders ML forecasts correctly

---

## Issue 3: ML-004 Gap-Fill API Write (when implemented)

**Issue**: `review_gi_draft_ml_hindcast_api_write_broken.md`
**Status**: Draft — not yet implemented

### What needs fixing
- Hindcast crash prevents API write from executing
- API write return value ignored
- Null-discharge filter uses wrong column name
- Duplicate records cause CardinalityViolation

### Server verification steps (post-implementation)

1. **Run ML maintenance gap-fill**:
   ```bash
   bash apps/run_locally.sh maintenance:machine_learning
   ```

2. **Verify gaps are actually filled in DB**:
   ```sql
   SELECT code, date, model_type, COUNT(*)
   FROM forecasts
   WHERE model_type = 'ML' AND date > CURRENT_DATE - INTERVAL '30 days'
   GROUP BY code, date, model_type;
   -- Should show continuous records (no gaps)
   ```

3. **Run again — should detect no new gaps**:
   ```bash
   bash apps/run_locally.sh maintenance:machine_learning
   # Logs should show "0 gaps detected" (not re-detecting same gaps)
   ```

### Pass criteria
- First run fills gaps and writes to API
- Second run detects zero gaps (idempotent)
- No CardinalityViolation errors

---

## Dependency Graph

```json
{
  "phases": {
    "phase_0": {
      "description": "All local checks pass (see review_checklist_local_2026-03-13.md)",
      "depends_on": []
    },
    "phase_1": {
      "description": "Deploy to server — merge, rebuild, migrate",
      "tasks": ["merge_to_main", "docker_rebuild", "run_migrations"],
      "parallel": false,
      "depends_on": ["phase_0"]
    },
    "phase_2": {
      "description": "Verify server-only issues",
      "tasks": ["issue_1_api_client", "issue_2_ml_alignment"],
      "parallel": true,
      "depends_on": ["phase_1"]
    },
    "phase_3": {
      "description": "ML-004 gap-fill (blocked on implementation)",
      "tasks": ["issue_3_gap_fill"],
      "parallel": false,
      "depends_on": ["phase_2"],
      "blocked": true,
      "blocked_reason": "Issue ML-004 is still in draft status"
    }
  },
  "agent_assignments": {
    "agent_1_deployer": {
      "phase": "phase_1",
      "action": "Merge branch, rebuild Docker images, run migrations",
      "note": "Human-driven — requires server access"
    },
    "agent_2_api_client_verifier": {
      "phase": "phase_2",
      "action": "Test new sapphire-api-client parameters on server",
      "depends_on": ["agent_1_deployer"]
    },
    "agent_3_ml_alignment_verifier": {
      "phase": "phase_2",
      "action": "Check DB for duplicates, run short-term pipeline, inspect logs",
      "depends_on": ["agent_1_deployer"]
    }
  }
}
```
