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

**Tests must pass before committing or moving to a new topic.**

### Zero Skips Policy

**No tests may be skipped.** All tests must run and pass. If any tests are skipped or fail to collect, treat this as a red flag requiring investigation before proceeding. Skipped tests often hide real bugs (e.g., missing dependencies causing `SAPPHIRE_API_AVAILABLE = False`). Do not accept "0 collected" or `pytest.skip()` as normal — find and fix the root cause.

### Test Coverage Requirements

1. **Unit tests**: Each new method should have adequate unit tests
2. **Workflow tests**: Tests covering entire processing workflows
3. **End-to-end tests**: Tests verifying complete system behavior

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

### Test Patterns

```python
import pytest
from unittest.mock import Mock, patch

class TestForecastCalculation:
    """Tests for forecast calculation functions."""

    def test_basic_forecast(self):
        """Test basic forecast calculation."""
        result = calculate_forecast(sample_data, horizon=5)
        assert len(result) == 5
        assert "forecast" in result.columns

    def test_invalid_horizon_raises_error(self):
        """Test that negative horizon raises ValueError."""
        with pytest.raises(ValueError, match="horizon"):
            calculate_forecast(sample_data, horizon=-1)

    @patch("module.external_api_call")
    def test_with_mocked_api(self, mock_api):
        """Test with mocked external API."""
        mock_api.return_value = {"data": [1, 2, 3]}
        result = function_using_api()
        mock_api.assert_called_once()
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
