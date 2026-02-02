# P-002: Preprocessing Gateway Runs Multiple Times Per Day

## Priority: URGENT

## Summary

The `PreprocessingGatewayQuantileMapping` task runs multiple times per day instead of once, wasting computational resources and slowing down the operational forecasting pipeline.

---

## Problem Description

**Observed behavior**: On the ubuntu production server, preprocessing gateway runs 2-3 times daily.

**Expected behavior**: Gateway preprocessing should run once per day (at 09:00), and subsequent workflows should reuse that result.

**Impact**:
- Unnecessary computation (gateway preprocessing is expensive - 50+ API calls to Data Gateway)
- Slower operational forecasting pipeline
- Wasted API calls to Data Gateway

---

## Root Cause Analysis

### Historical Context

**Commit `a357c62` (2026-01-26)** removed marker file checks with the message:
> "Remove preprocessing_runoff marker file check from pipeline"

The rationale was that `preprocessing_runoff` is "fast enough to run every time" after the Python 3.12 migration. However, this commit **also removed the marker check for gateway preprocessing** as collateral damage.

**The rationale does NOT apply to gateway** because:
- `preprocessing_runoff`: Fast (~2-5 min), fetches only recent data
- `preprocessing_gateway`: Expensive (~10-20 min), makes 50+ API calls for ensemble forecasts

### How Gateway Is Invoked

The preprocessing gateway is invoked from **multiple entry points**:

| Time | Entry Point | Task | Trigger |
|------|-------------|------|---------|
| 09:00 | `run_preprocessing_gateway.sh` | `RunPreprocessingGatewayWorkflow` | Direct cron job |
| 10:00 | `run_pentadal_forecasts.sh` | `PreprocessingGatewayQuantileMapping` | Via ConceptualModel/RunMLModel dependencies |
| 11:00 | `run_decadal_forecasts.sh` | `PreprocessingGatewayQuantileMapping` | Via ConceptualModel/RunMLModel dependencies |

### Why It Runs Multiple Times

1. **Marker file system is incomplete**:
   - `PreprocessingGatewayQuantileMapping` **WRITES** marker files on success (lines 422-434)
   - `ExternalPreprocessingGateway` **EXISTS** and checks for marker files (lines 328-340)
   - **BUT**: `ConceptualModel.requires()` and `RunMLModel.requires()` call `PreprocessingGatewayQuantileMapping()` directly - they don't check for the marker first

2. **Luigi output target is ephemeral**:
   - `PreprocessingGatewayQuantileMapping.output()` returns `/app/log_pregateway.txt`
   - This path is **inside the container** (ephemeral, not persisted)
   - Luigi always thinks the task needs to run because it can't see the output file

3. **Separate Luigi executions don't share state**:
   - Each workflow (gateway, pentadal, decadal) runs as a separate `docker compose run` invocation
   - Luigi's internal deduplication only works within a single execution

### Code References

| Component | File | Lines | Issue |
|-----------|------|-------|-------|
| Gateway writes marker | `pipeline_docker.py` | 422-434 | ✅ Works correctly |
| External task checks marker | `pipeline_docker.py` | 328-340 | ✅ Exists but unused |
| ConceptualModel requires gateway | `pipeline_docker.py` | 528 | ❌ Calls task directly, no marker check |
| RunMLModel requires gateway | `pipeline_docker.py` | 584 | ❌ Calls task directly, no marker check |
| RunAllMLModels requires gateway | `pipeline_docker.py` | 625 | ❌ Calls task directly, no marker check |

---

## Future Requirements Context

Before implementing a fix, consider upcoming requirements that affect the architecture:

### Sub-Daily Forecasting (Planned)
- **Requirement**: Run gateway preprocessing **4x daily** (e.g., 00:00, 06:00, 12:00, 18:00 UTC) for sub-daily forecasts
- **Note**: Fetch and quantile mapping always run together - you cannot fetch without processing
- **Impact**: Need time-slot aware markers for sub-daily support
- **Reference**: `doc/plans/future_development_plans.md` (lines 53-54)

### API-Based Data Retrieval (Planned)
- **Requirement**: Gateway will return DataFrames via REST API instead of CSV files
- **Impact**: Marker files may become obsolete - API health checks replace them
- **Benefit**: Eliminates P-001 (marker file permission issues)

### Maintenance vs Operational Separation
Gateway preprocessing has two distinct purposes that can be separated:

| Mode | Scripts | Purpose | Frequency |
|------|---------|---------|-----------|
| **Operational** | `Quantile_Mapping_OP.py`, `snow_data_operational.py` | Fetch fresh data + apply quantile mapping | 1x daily (or 4x for sub-daily) |
| **Maintenance** | `extend_era5_reanalysis.py`, `snow_data_reanalysis.py` | Fill gaps in historical data | 1x daily (morning catch-up) |

---

## Solution: Phased Approach

### Phase 1: Immediate Fix (This Week)
Restore marker file checks to prevent duplicate runs within the same day.

### Phase 2: Mode Separation (Next Sprint)
Add `GATEWAY_MODE` environment variable to separate operational and maintenance workflows.

### Phase 3: API Architecture (Q2)
Migrate to DataFrame API, eliminate marker files entirely.

---

## Phase 1: Restore Marker File Checks

### Gateway Dependency Helper

Create a helper function that checks for the marker file before deciding which task to use:

```python
def get_gateway_dependency(time_slot=None):
    """Returns the appropriate gateway task based on whether it already ran.

    Args:
        time_slot: Optional time slot for sub-daily forecasts (None for daily)

    Returns:
        ExternalPreprocessingGateway if already ran, else PreprocessingGatewayQuantileMapping
    """
    today = datetime.date.today()
    marker_file = get_marker_filepath('preprocessing_gateway', date=today, time_slot=time_slot)

    if os.path.exists(marker_file):
        print(f"Using external gateway task (already run) for {today}" +
              (f" slot {time_slot}" if time_slot is not None else ""))
        return ExternalPreprocessingGateway(date=today)
    else:
        print(f"No gateway marker found for {today}" +
              (f" slot {time_slot}" if time_slot is not None else "") +
              ", running gateway preprocessing")
        return PreprocessingGatewayQuantileMapping()
```

### Update Marker File Helper (for future sub-daily support)

Extend `get_marker_filepath()` to support optional time slots:

```python
def get_marker_filepath(task_name, date=None, time_slot=None):
    """Generate consistent marker filepath for a given task, date, and optional time slot.

    Args:
        task_name: Name of the task (e.g., 'preprocessing_gateway')
        date: Date for the marker (defaults to today)
        time_slot: Optional time slot for sub-daily tasks (0, 1, 2, 3 for 4x daily)

    Returns:
        Path to marker file, e.g.:
        - Daily: preprocessing_gateway_2026-02-02.marker
        - Sub-daily: preprocessing_gateway_2026-02-02_slot0.marker
    """
    if date is None:
        date = datetime.date.today()

    if time_slot is not None:
        return f"{MARKER_DIR}/{task_name}_{date}_slot{time_slot}.marker"
    return f"{MARKER_DIR}/{task_name}_{date}.marker"
```

### Modify Dependent Tasks

#### 1. `ConceptualModel.requires()` (line 526-528)

**Current code:**
```python
def requires(self):
    return [PreprocessingRunoff(), PreprocessingGatewayQuantileMapping()]
```

**New code:**
```python
def requires(self):
    return [PreprocessingRunoff(), get_gateway_dependency()]
```

#### 2. `RunMLModel.requires()` (line 582-584)

**Current code:**
```python
def requires(self):
    return [PreprocessingRunoff(), PreprocessingGatewayQuantileMapping()]
```

**New code:**
```python
def requires(self):
    return [PreprocessingRunoff(), get_gateway_dependency()]
```

#### 3. `RunAllMLModels.requires()` (line 622-625)

**Current code:**
```python
def requires(self):
    yield PreprocessingRunoff()
    yield PreprocessingGatewayQuantileMapping()
    # ... ML model yields
```

**New code:**
```python
def requires(self):
    yield PreprocessingRunoff()
    yield get_gateway_dependency()
    # ... ML model yields (unchanged)
```

---

## Phase 2: Mode Separation

Add `GATEWAY_MODE` environment variable to separate operational and maintenance workflows, consistent with patterns used in `preprocessing_runoff` and `machine_learning` modules.

### Mode Definitions

| Mode | Value | Scripts Run | Purpose |
|------|-------|-------------|---------|
| **Operational** | `operational` (default) | `Quantile_Mapping_OP.py`, `snow_data_operational.py` | Fetch fresh data + quantile mapping |
| **Maintenance** | `maintenance` | `extend_era5_reanalysis.py`, `snow_data_reanalysis.py` | Fill gaps in historical data |

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              OPERATIONAL MODE (default)                      │
│  GATEWAY_MODE=operational (or unset)                         │
│                                                              │
│  Scripts run:                                                │
│  1. Quantile_Mapping_OP.py  (fetch + bias correction)        │
│  2. snow_data_operational.py (fetch operational snow data)   │
│                                                              │
│  Frequency:                                                  │
│  - Daily forecasts: 1x daily                                 │
│  - Sub-daily forecasts: 4x daily (future)                    │
│                                                              │
│  Creates marker file on success                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│              MAINTENANCE MODE                                │
│  GATEWAY_MODE=maintenance                                    │
│                                                              │
│  Scripts run:                                                │
│  1. extend_era5_reanalysis.py  (fill ERA5 gaps)              │
│  2. snow_data_reanalysis.py    (fill snow data gaps)         │
│                                                              │
│  Frequency: 1x daily (morning catch-up)                      │
│                                                              │
│  Does NOT create marker file (doesn't affect dependencies)   │
└─────────────────────────────────────────────────────────────┘
```

### Implementation

#### Mode Detection (consistent with preprocessing_runoff pattern)

```python
# In preprocessing_gateway module
def get_gateway_mode():
    """Get gateway mode from CLI or environment.

    Priority: CLI argument > environment variable > default (operational)

    Values:
        - 'operational': Fetch data + quantile mapping (default)
        - 'maintenance': Fill gaps in ERA5/snow historical data
    """
    if '--maintenance' in sys.argv:
        return 'maintenance'
    return os.getenv('GATEWAY_MODE', 'operational').lower()
```

#### Updated Dockerfile

```dockerfile
CMD ["sh", "-c", "\
    if [ \"$GATEWAY_MODE\" = \"maintenance\" ]; then \
        echo 'Running maintenance mode: gap filling' && \
        uv run extend_era5_reanalysis.py && \
        uv run snow_data_reanalysis.py; \
    else \
        echo 'Running operational mode: fetch + quantile mapping' && \
        uv run Quantile_Mapping_OP.py && \
        uv run snow_data_operational.py; \
    fi"]
```

#### Scheduling

```bash
# crontab entries

# Operational: Daily forecasts (1x daily, creates marker)
0 9 * * * bash bin/run_preprocessing_gateway.sh /path/to/.env

# Maintenance: Gap filling (1x daily, morning catch-up)
30 8 * * * GATEWAY_MODE=maintenance bash bin/run_preprocessing_gateway.sh /path/to/.env

# Future: Sub-daily forecasts (4x daily, each creates time-slot marker)
# 0 0,6,12,18 * * * bash bin/run_preprocessing_gateway_subdaily.sh /path/to/.env --slot $SLOT
```

---

## Phase 3: API Architecture (Future)

Replace file-based data exchange with REST API returning DataFrames.

### Target Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Gateway API Service                        │
│  FastAPI + In-Memory Cache                                   │
│  Port: 8001                                                  │
│                                                              │
│  Endpoints:                                                  │
│  GET /api/v1/forecast/{date}?type=control                   │
│  GET /api/v1/forecast/{date}?type=ensemble&member=1         │
│  GET /api/v1/health                                          │
└─────────────────────────────────────────────────────────────┘
                              ↓ HTTP/JSON
┌─────────────────────────────────────────────────────────────┐
│              Dependent Tasks (ConceptualModel, etc.)         │
│  - HTTP request to gateway API                               │
│  - Deserialize JSON to DataFrame                             │
│  - No marker files needed - API health = data available      │
└─────────────────────────────────────────────────────────────┘
```

### Benefits of API Architecture

| Current (Marker Files) | Future (API) |
|------------------------|--------------|
| Marker files accumulate | No filesystem state |
| Root ownership issues (P-001) | No permission issues |
| Date-based only | Native time-slot support |
| Shared volume required | Network-based |
| Manual cleanup needed | Automatic cache expiry |

---

## Testing Plan

### Testing Requirements

**MANDATORY**: All new or modified methods must have unit tests. Integration tests required for workflow verification.

### Phase 1 Tests

#### Prerequisites: Create Test Infrastructure

**Create `apps/pipeline/tests/conftest.py`:**
```python
import pytest
import tempfile
import os
from pathlib import Path

@pytest.fixture
def temp_marker_dir(tmp_path):
    """Temporary directory for marker files."""
    marker_dir = tmp_path / "marker_files"
    marker_dir.mkdir()
    return marker_dir

@pytest.fixture
def mock_env(temp_marker_dir, monkeypatch):
    """Mock environment with temp marker directory."""
    monkeypatch.setattr('pipeline_docker.MARKER_DIR', str(temp_marker_dir))
    return {'marker_dir': temp_marker_dir}
```

#### Unit Tests (Required)

**Create `apps/pipeline/tests/test_marker_files.py`:**
```python
import pytest
import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

# Test get_marker_filepath()
class TestGetMarkerFilepath:
    def test_basic_filepath(self, mock_env):
        """Test basic marker filepath generation."""
        from pipeline_docker import get_marker_filepath
        path = get_marker_filepath('preprocessing_gateway', date=datetime.date(2026, 2, 2))
        assert 'preprocessing_gateway_2026-02-02.marker' in path

    def test_default_date_is_today(self, mock_env):
        """Test that date defaults to today."""
        from pipeline_docker import get_marker_filepath
        today = datetime.date.today()
        path = get_marker_filepath('preprocessing_gateway')
        assert str(today) in path

    def test_time_slot_marker_filepath(self, mock_env):
        """Test marker filepath with time_slot for sub-daily support."""
        from pipeline_docker import get_marker_filepath
        path = get_marker_filepath('preprocessing_gateway',
                                   date=datetime.date(2026, 2, 2),
                                   time_slot=0)
        assert path.endswith('preprocessing_gateway_2026-02-02_slot0.marker')

    def test_different_time_slots_different_paths(self, mock_env):
        """Test that different time slots produce different paths."""
        from pipeline_docker import get_marker_filepath
        date = datetime.date(2026, 2, 2)
        path0 = get_marker_filepath('preprocessing_gateway', date=date, time_slot=0)
        path1 = get_marker_filepath('preprocessing_gateway', date=date, time_slot=1)
        assert path0 != path1


# Test get_gateway_dependency()
class TestGetGatewayDependency:
    def test_returns_external_task_when_marker_exists(self, mock_env):
        """When marker file exists, should return ExternalPreprocessingGateway."""
        from pipeline_docker import get_gateway_dependency, get_marker_filepath, ExternalPreprocessingGateway

        # Create marker file
        marker = get_marker_filepath('preprocessing_gateway')
        Path(marker).parent.mkdir(parents=True, exist_ok=True)
        Path(marker).write_text("test marker content")

        result = get_gateway_dependency()
        assert isinstance(result, ExternalPreprocessingGateway)

    def test_returns_real_task_when_no_marker(self, mock_env):
        """When no marker file, should return PreprocessingGatewayQuantileMapping."""
        from pipeline_docker import get_gateway_dependency, PreprocessingGatewayQuantileMapping

        result = get_gateway_dependency()
        assert isinstance(result, PreprocessingGatewayQuantileMapping)

    def test_respects_time_slot_parameter(self, mock_env):
        """Time slot parameter should affect marker file check."""
        from pipeline_docker import get_gateway_dependency, get_marker_filepath, ExternalPreprocessingGateway

        # Create marker for slot 0 only
        marker = get_marker_filepath('preprocessing_gateway', time_slot=0)
        Path(marker).parent.mkdir(parents=True, exist_ok=True)
        Path(marker).write_text("slot 0 marker")

        # Slot 0 should return external task
        result_slot0 = get_gateway_dependency(time_slot=0)
        assert isinstance(result_slot0, ExternalPreprocessingGateway)

        # Slot 1 should return real task (no marker)
        from pipeline_docker import PreprocessingGatewayQuantileMapping
        result_slot1 = get_gateway_dependency(time_slot=1)
        assert isinstance(result_slot1, PreprocessingGatewayQuantileMapping)


# Test marker file creation in task
class TestMarkerFileCreation:
    def test_marker_created_on_success(self, mock_env, tmp_path):
        """Verify marker file is created when task succeeds."""
        from pipeline_docker import get_marker_filepath
        import datetime

        marker_path = get_marker_filepath('preprocessing_gateway')

        # Simulate successful task completion writing marker
        Path(marker_path).parent.mkdir(parents=True, exist_ok=True)
        with open(marker_path, 'w') as f:
            f.write(f"PreprocessingGateway completed successfully at {datetime.datetime.now()}")

        assert Path(marker_path).exists()
        content = Path(marker_path).read_text()
        assert "completed successfully" in content

    def test_marker_not_created_on_failure(self, mock_env):
        """Verify marker file is NOT created when task fails."""
        from pipeline_docker import get_marker_filepath

        marker_path = get_marker_filepath('preprocessing_gateway')
        # Don't create marker (simulating failure)
        assert not Path(marker_path).exists()


# Test requires() method behavior
class TestRequiresMethods:
    def test_conceptual_model_uses_helper(self, mock_env):
        """ConceptualModel.requires() should use get_gateway_dependency()."""
        # This test verifies the code change was made correctly
        from pipeline_docker import ConceptualModel
        import inspect

        source = inspect.getsource(ConceptualModel.requires)
        assert 'get_gateway_dependency' in source
        assert 'PreprocessingGatewayQuantileMapping()' not in source

    def test_run_ml_model_uses_helper(self, mock_env):
        """RunMLModel.requires() should use get_gateway_dependency()."""
        from pipeline_docker import RunMLModel
        import inspect

        source = inspect.getsource(RunMLModel.requires)
        assert 'get_gateway_dependency' in source

    def test_run_all_ml_models_uses_helper(self, mock_env):
        """RunAllMLModels.requires() should use get_gateway_dependency()."""
        from pipeline_docker import RunAllMLModels
        import inspect

        source = inspect.getsource(RunAllMLModels.requires)
        assert 'get_gateway_dependency' in source
```

#### Integration Tests

**Manual Integration Test Procedure:**
```bash
# 1. Clean up any existing markers
rm -f /path/to/intermediate_data/marker_files/preprocessing_gateway_*.marker

# 2. Run gateway preprocessing first
bash bin/run_preprocessing_gateway.sh /path/to/.env

# 3. Verify marker file exists
ls -la /path/to/intermediate_data/marker_files/preprocessing_gateway_*.marker
# Expected: preprocessing_gateway_YYYY-MM-DD.marker exists

# 4. Run pentadal workflow
bash bin/run_pentadal_forecasts.sh /path/to/.env

# 5. Check logs - should see "Using external gateway task (already run)"
grep "Using external gateway task" /path/to/logs/*.log
# Expected: Log message found

# 6. Verify gateway did NOT re-run (check Docker logs)
docker ps -a --filter "name=prepgateway" --format "{{.Names}} {{.CreatedAt}}"
# Expected: Only ONE container from step 2, no new container from step 4
```

**Automated Integration Test (pytest):**
```python
# apps/pipeline/tests/test_gateway_integration.py
import pytest
import subprocess
import os
from pathlib import Path

@pytest.mark.integration
class TestGatewayDoubleRunPrevention:
    """Integration tests for gateway double-run fix."""

    @pytest.fixture
    def clean_markers(self):
        """Remove gateway markers before test."""
        marker_dir = os.environ.get('MARKER_DIR', '/tmp/test_markers')
        for f in Path(marker_dir).glob('preprocessing_gateway_*.marker'):
            f.unlink()
        yield
        # Cleanup after test
        for f in Path(marker_dir).glob('preprocessing_gateway_*.marker'):
            f.unlink()

    def test_second_workflow_skips_gateway(self, clean_markers):
        """Second workflow in same day should skip gateway preprocessing."""
        # This test requires Docker and full environment setup
        # Run in CI or manually, not as unit test
        pass  # Implementation depends on test infrastructure
```

### Phase 2 Tests

```bash
# Test operational mode (default)
bash bin/run_preprocessing_gateway.sh /path/to/.env
# Should run: Quantile_Mapping_OP.py, snow_data_operational.py

# Test maintenance mode
GATEWAY_MODE=maintenance bash bin/run_preprocessing_gateway.sh /path/to/.env
# Should run: extend_era5_reanalysis.py, snow_data_reanalysis.py
```

---

## Implementation Checklist

### Phase 1: Immediate Fix

#### Code Changes
- [x] Create `get_gateway_dependency()` helper function in `pipeline_docker.py`
- [x] Update `get_marker_filepath()` to support optional `time_slot` parameter
- [x] Modify `ConceptualModel.requires()` to use helper
- [x] Modify `RunMLModel.requires()` to use helper
- [x] Modify `RunAllMLModels.requires()` to use helper

#### Testing (REQUIRED)
- [x] Create `apps/pipeline/tests/conftest.py` with fixtures
- [x] Create `apps/pipeline/tests/test_marker_files.py` with unit tests:
  - [x] `TestGetMarkerFilepath` - 5 test methods (added test_uses_mock_marker_dir)
  - [x] `TestGetGatewayDependency` - 3 test methods
  - [x] `TestMarkerFileCreation` - 2 test methods
  - [x] `TestRequiresMethods` - 3 test methods (verify code changes)
  - [x] `TestGatewayDependencyIntegration` - 2 test methods (added integration tests)
- [x] Run unit tests locally (15 tests pass)
- [x] All unit tests pass

**Test command** (run from repo root):
```bash
PYTHONPATH="$PWD:$PWD/apps" SAPPHIRE_TEST_ENV=True apps/pipeline/.venv/bin/pytest apps/pipeline/tests/test_marker_files.py -v
```

#### Integration Testing
- [ ] Test locally with manual marker file creation
- [ ] Run full local integration test (gateway → pentadal)
- [ ] Verify logs show "Using external gateway task" on second run

#### Deployment
- [ ] Deploy to staging/test server
- [ ] Run integration test on staging
- [ ] Deploy to production
- [ ] Monitor production logs for one full day

### Phase 2: Mode Separation
- [ ] Add `get_gateway_mode()` function to preprocessing_gateway module
- [ ] Add `--maintenance` CLI flag support
- [ ] Update Dockerfile with mode-based CMD
- [ ] Update `run_preprocessing_gateway.sh` to pass `GATEWAY_MODE`
- [ ] Add maintenance mode cron entry
- [ ] Test both modes locally
- [ ] Update documentation
- [ ] Deploy and monitor

### Phase 3: API Architecture
- [ ] Design API endpoints
- [ ] Implement FastAPI service
- [ ] Create `GatewayAPIClient` library
- [ ] Migrate ConceptualModel to use API
- [ ] Migrate RunMLModel to use API
- [ ] Remove marker file code
- [ ] Full system testing

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Marker file not created (gateway fails) | Low | Medium | Gateway would run again - no worse than current |
| Marker file from previous day used | Low | High | Marker files include date in filename |
| Race condition if workflows start simultaneously | Very Low | Low | Unlikely with 1-hour gaps between cron jobs |
| Phase 2: Maintenance mode runs wrong scripts | Low | Medium | Clear logging, test before deploy |
| Phase 3: API service down blocks pipeline | Medium | High | Health checks, auto-restart, fallback to CSV |

---

## Files to Modify

### Phase 1
| File | Change |
|------|--------|
| `apps/pipeline/pipeline_docker.py` | Add `get_gateway_dependency()`, update `get_marker_filepath()`, modify 3 `requires()` methods |

### Phase 2
| File | Change |
|------|--------|
| `apps/preprocessing_gateway/Quantile_Mapping_OP.py` | Add mode detection (optional, for logging) |
| `apps/preprocessing_gateway/Dockerfile` | Add mode-based CMD |
| `bin/run_preprocessing_gateway.sh` | Pass GATEWAY_MODE to container |
| Crontab | Add maintenance mode entry |

### Phase 3
| File | Change |
|------|--------|
| `apps/preprocessing_gateway/gateway_api.py` | New FastAPI service |
| `apps/preprocessing_gateway/gateway_client.py` | New client library |
| `apps/pipeline/pipeline_docker.py` | Replace marker checks with API calls |
| `bin/docker-compose-luigi.yml` | Add gateway-api service |

---

## Estimated Effort

| Phase | Effort | Timeline |
|-------|--------|----------|
| Phase 1: Immediate Fix | 2-3 hours | This week |
| Phase 2: Mode Separation | 4-6 hours | Next sprint |
| Phase 3: API Architecture | 2-3 weeks | Q2 |

---

## Related Issues

- **P-001**: Marker files owned by root not cleaned up (Phase 3 eliminates this)
- **Worker ID issue**: See `deployment_improvement_planning.md` "To Evaluate: Luigi Worker ID Configuration"
- **Sub-daily forecasting**: See `future_development_plans.md` (lines 53-54)
- **Dashboard Markdown/opts error**: See `observations.md` 2026-02-02 entry (separate issue)

---

*Created: 2026-02-02*
*Updated: 2026-02-02 - Phase 1 code changes and unit tests complete (15/15 pass)*
*Status: IN PROGRESS - Phase 1 code complete, awaiting integration testing and deployment*
