# CLAUDE.md - SAPPHIRE Forecast Tools

This file provides guidance for AI assistants working on this codebase.

## Project Architecture

SAPPHIRE has two main component layers that interact with each other:

### 1. Application Modules (`apps/`)

Active Python modules that perform hydrological forecasting operations:

| Module | Purpose |
|--------|---------|
| `preprocessing_runoff` | Process runoff data from various sources |
| `preprocessing_gateway` | Gateway for preprocessing data from external APIs |
| `preprocessing_station_forcing` | Process station forcing data |
| `linear_regression` | Linear regression forecasting models |
| `machine_learning` | ML-based forecasting models |
| `postprocessing_forecasts` | Post-process forecast outputs |
| `configuration_dashboard` | Dashboard for system configuration |
| `forecast_dashboard` | Dashboard displaying forecast results |
| `reset_forecast_run_date` | Utility for resetting forecast dates |
| `iEasyHydroForecast` | Core forecasting library |

**Legacy module**: Only `backend/` is legacy and being phased out.

### 2. SAPPHIRE Services (`sapphire/services/`)

FastAPI microservices with PostgreSQL backends:

| Service | Port | Description |
|---------|------|-------------|
| `api-gateway` | 8000 | Routes requests to backend services |
| `preprocessing` | 8002 | API for preprocessing data storage |
| `postprocessing` | 8003 | API for forecast results and skill metrics |
| `user` | 8004 | User management |
| `auth` | 8005 | Authentication and authorization |

### Data I/O Transition

The `apps/` modules interact with `sapphire/services/` via REST API. The codebase is transitioning from CSV-based I/O to database-backed storage:

- **Legacy (being removed)**: CSV file reading/writing
- **Current**: REST API integration with `sapphire/services/`

CSV I/O will be removed once API integration is fully tested.

---

## Code Style Conventions

### Python Style

- **Line length**: 79-100 characters (prefer 79 for docstrings)
- **Imports**: Group by standard library, third-party, local; alphabetize within groups
- **Type hints**: Use for function signatures, especially public APIs
- **Docstrings**: Google-style with Args, Returns, Raises sections

```python
def calculate_forecast(
    data: pd.DataFrame,
    horizon: int,
    method: str = "linear"
) -> pd.DataFrame:
    """
    Calculate forecast for the given horizon.

    Args:
        data: Input DataFrame with date index and value columns
        horizon: Forecast horizon in days
        method: Forecasting method to use

    Returns:
        DataFrame with forecast values

    Raises:
        ValueError: If horizon is negative
    """
```

### Naming Conventions

- **Functions/methods**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private methods**: `_single_leading_underscore`

### API Patterns

FastAPI services follow these patterns:

```python
# Router organization
router = APIRouter(prefix="/forecasts", tags=["forecasts"])

# Endpoint naming: use nouns, not verbs
@router.get("/", response_model=list[ForecastResponse])
@router.get("/{forecast_id}", response_model=ForecastResponse)
@router.post("/", response_model=ForecastResponse)

# Use Pydantic models for request/response validation
class ForecastCreate(BaseModel):
    code: str
    date: date
    value: float

# Dependency injection for database sessions
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
```

### Upsert Pattern

For idempotent data operations, use the upsert pattern:

```python
from sqlalchemy.dialects.postgresql import insert

def upsert_record(db: Session, model: Type[Base], data: dict, unique_keys: list[str]):
    """Create or update record based on unique keys."""
    stmt = insert(model).values(**data)
    stmt = stmt.on_conflict_do_update(
        index_elements=unique_keys,
        set_={k: v for k, v in data.items() if k not in unique_keys}
    )
    db.execute(stmt)
    db.commit()
```

---

## Testing Requirements

### Before Committing or Moving to New Topic

**All tests must pass with zero skips before committing or moving to a new topic.** The full pre-commit validation has three stages:

1. **Unit/integration tests** (always): `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh`
2. **Local pipeline run** (after major changes): `bash apps/run_locally.sh all` — runs the full forecast pipeline against real data using local venvs, confirming nothing is broken end-to-end
3. **Docker smoke tests** (after major changes): `bash apps/run_docker_tests.sh --skip-ml` — builds all Docker images and verifies critical imports, catching dependency and packaging regressions

Stage 1 is required before every commit. Stages 2 and 3 are required after changes that affect module dependencies, entry points, Docker configuration, or cross-module data flow. See [`doc/dev/testing_workflow.md`](doc/dev/testing_workflow.md) for full details on all stages.

### Zero Skips Policy

**No tests may be skipped without justification.** If any tests are skipped or fail to collect, treat this as a red flag requiring investigation before proceeding. Do not accept "0 collected" or `pytest.skip()` as normal — find and fix the root cause.

**One exception**: dependency-gated skips are acceptable when `sapphire-api-client` is not installed. These tests guard on `SAPPHIRE_API_AVAILABLE` and skip with an explicit message like `pytest.skip("sapphire-api-client not installed")`. This is the only valid skip pattern — all other skips indicate hidden bugs.

### Test Categories

Every new feature or bug fix must include tests. The required categories depend on what changed:

#### 1. Unit Tests (always required)

Isolated tests for individual functions with all external dependencies mocked. Each new or modified public function needs at least:
- A happy-path test with typical input
- An error-path test (invalid input, exception handling)

**File naming**: `test_<module_or_topic>.py`

#### 2. Edge Case Tests (required for DataFrame, date, or numeric code)

Any code that processes DataFrames, dates, or numeric values must have edge case tests covering these scenarios:

| Category | Scenarios to test |
|----------|-------------------|
| **Empty data** | Empty DataFrame, single-row DataFrame, all-NaN columns |
| **NaN handling** | All NaN values, mixed NaN/valid, NaN-to-None conversion for API |
| **Date boundaries** | Year transitions (Dec 31 → Jan 1), leap year Feb 29, month boundaries |
| **Value boundaries** | Zero values, very small positives (0.001), very large values (10000+) |
| **Duplicates** | Duplicate date-station combinations |
| **Multi-entity** | Single station many dates, many stations single date |

See `preprocessing_runoff/test/test_edge_cases.py` as the reference implementation — it covers all of these categories in dedicated test classes.

**File naming**: `test_edge_cases.py` or edge case classes within the relevant test file.

#### 3. Integration Tests (required for multi-step workflows)

Tests that exercise the real logic across multiple internal functions, only mocking external boundaries (API clients, file I/O). Required when:
- A function calls multiple internal modules in sequence
- Data flows through a pipeline (read → transform → write)
- Entry points orchestrate multiple steps

Integration tests should:
- Use real logic for everything inside the boundary
- Only mock the external API client and filesystem
- Validate the full data flow, not just final output

See `postprocessing_forecasts/tests/test_integration_postprocessing.py` as the reference — it tests the full pipeline: skill CSV read → threshold filter → ensemble create → CSV + API write.

**File naming**: `test_integration_<topic>.py`

#### 4. API Failure Tests (required for any code using `sapphire_api_client`)

Any function that reads from or writes to the SAPPHIRE API must have tests for all failure modes:

```python
class TestWriteToApi:
    def test_returns_false_when_api_unavailable(self, data):
        """When sapphire_api_client is not installed."""
        with patch.object(module, "SAPPHIRE_API_AVAILABLE", False):
            assert module._write_to_api(data) is False

    def test_returns_false_when_api_disabled(self, data):
        """When SAPPHIRE_API_ENABLED=false."""
        with patch.object(module, "SAPPHIRE_API_AVAILABLE", True), \
             patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}):
            assert module._write_to_api(data) is False

    def test_returns_false_when_api_not_ready(self, data):
        """When readiness_check fails."""
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = False
        # ... assert returns False, no exception

    def test_csv_still_written_on_api_failure(self, data, tmp_path):
        """CSV fallback works when API fails."""
        # ... verify CSV written even when API raises
```

The full pattern is documented in `preprocessing_runoff/test/test_api_write.py`.

#### 5. Performance Benchmarks (optional, for optimization work)

Mark with `@pytest.mark.benchmark`. Skipped by default, run explicitly:
```bash
pytest <module>/tests/test_performance.py -v -k bench
```

See `postprocessing_forecasts/tests/test_performance.py` for the pattern.

### Required conftest.py Pattern

Any module that imports `forecast_library` or `setup_library` (directly or transitively) **must** have a `conftest.py` with the API singleton reset fixture:

```python
"""Shared fixtures for <module> tests."""
import os, sys
import pytest

sys.path.insert(
    0, os.path.join(os.path.dirname(__file__), '..', '..', 'iEasyHydroForecast')
)
import forecast_library as fl

@pytest.fixture(autouse=True)
def _reset_api_singletons():
    """Reset forecast_library API client singletons between tests.

    Without this, a mock injected by one test leaks into subsequent tests
    because the singleton caches the first client instance it creates.
    """
    fl._reset_api_clients()
    yield
    fl._reset_api_clients()
```

This fixture is already present in `iEasyHydroForecast`, `postprocessing_forecasts`, and `linear_regression`. Any new module using the API must add it.

### Test File Naming Conventions

| File name pattern | Contents |
|-------------------|----------|
| `test_<topic>.py` | Unit tests for a specific topic or module |
| `test_edge_cases.py` | Edge case and boundary condition tests |
| `test_api_write.py` / `test_api_read.py` | API integration tests (write/read paths) |
| `test_api_integration.py` | Combined API read/write tests |
| `test_integration_<topic>.py` | Multi-step workflow integration tests |
| `test_performance.py` | Performance benchmarks (`@pytest.mark.benchmark`) |

### Running Tests

Follow the full testing workflow in [`doc/dev/testing_workflow.md`](doc/dev/testing_workflow.md). The recommended way to run all module tests is:

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh
```

To run a single module:

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh <module_name>
```

Always use `run_tests.sh` rather than running pytest manually for individual modules — it handles test directory inconsistencies (`test/` vs `tests/`) and ensures nothing is forgotten.

#### Application Module Tests

Tests are run from the `apps/` directory:

```bash
cd apps

# Run tests for a specific module
SAPPHIRE_TEST_ENV=True pytest preprocessing_runoff/test
SAPPHIRE_TEST_ENV=True pytest reset_forecast_run_date/tests
SAPPHIRE_TEST_ENV=True pytest linear_regression/test

# Run iEasyHydroForecast tests
SAPPHIRE_TEST_ENV=True python -m unittest discover -s iEasyHydroForecast/tests -p 'test_*.py'
```

#### SAPPHIRE Service Tests

Tests are run from the service directory:

```bash
# Preprocessing service tests
cd sapphire/services/preprocessing
python -m pytest tests/ -v

# Postprocessing service tests
cd sapphire/services/postprocessing
python -m pytest tests/ -v
```

### Environment Variables for Testing

```bash
SAPPHIRE_TEST_ENV=True      # Use test database / test mode
SAPPHIRE_OPDEV_ENV=True     # Development/testing mode for apps/
```

---

## Running Services

### Start all services with Docker Compose

```bash
cd sapphire
docker-compose up -d
```

### Check service health

```bash
curl http://localhost:8000/health
curl http://localhost:8000/health/ready
```

### View logs

```bash
docker-compose logs -f preprocessing-api
```

### Stop services

```bash
docker-compose down
```

---

## Data Migration

Run migrations from inside the Docker containers:

### Preprocessing Data Migration

```bash
docker exec -it sapphire-preprocessing-api /bin/bash

# Inside the container:
python app/data_migrator.py --type runoff
python app/data_migrator.py --type hydrograph
python app/data_migrator.py --type meteo
python app/data_migrator.py --type snow
```

### Postprocessing Data Migration

```bash
docker exec -it sapphire-postprocessing-api /bin/bash

# Inside the container:
python app/data_migrator.py --type skillmetric --batch-size 1
python app/data_migrator.py --type lrforecast
python app/data_migrator.py --type combinedforecast
python app/data_migrator.py --type forecast
```

---

## Project Structure

```
SAPPHIRE_forecast_tools/
├── apps/                       # Active Python modules
│   ├── preprocessing_runoff/
│   ├── preprocessing_gateway/
│   ├── linear_regression/
│   ├── machine_learning/
│   ├── postprocessing_forecasts/
│   ├── iEasyHydroForecast/
│   └── ...
├── sapphire/
│   └── services/               # FastAPI microservices
│       ├── preprocessing/
│       │   ├── app/
│       │   └── tests/
│       └── postprocessing/
│           ├── app/
│           └── tests/
├── bin/                        # Shell scripts for deployment/cron
├── doc/
│   └── plans/                  # Implementation plans and issues
│       └── issues/             # Detailed issue files
├── backend/                    # LEGACY - being phased out
└── CLAUDE.md                   # This file
```

---

## Issue Planning

See `doc/plans/module_issues.md` for the index of planned issues.

Issue files are stored in `doc/plans/issues/` with naming convention:
- Draft: `gi_draft_<module>_<description>.md`
- Published: `gi_<github_id>_<description>.md`
