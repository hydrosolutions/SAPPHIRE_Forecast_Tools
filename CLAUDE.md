# CLAUDE.md - SAPPHIRE Forecast Tools

This file provides guidance for AI assistants working on this codebase.

## Critical Constraints

### Sensitive Data

No passwords, no sensitive data like station codes or runoff data can ever be committed to GitHub. Before every commit, review all changed files for accidental inclusion of credentials, station codes, discharge values, or other operationally sensitive data.

### Ownership Boundaries

**`sapphire/services/` is managed by a colleague and must not be edited without coordination.** This includes all FastAPI service code, database models, migrations, and service tests. If a change requires modifying the API contract (new endpoints, changed request/response schemas), open a discussion first — do not edit the service code directly.

The `apps/` modules and everything else in the repository are fair game.

---

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
| `machine_learning` | ML-based forecasting models (short-term) |
| `long_term_forecasting` | Long-term (monthly) forecasting models |
| `postprocessing_forecasts` | Post-process forecast outputs |
| `configuration_dashboard` | Dashboard for system configuration |
| `forecast_dashboard` | Dashboard displaying forecast results |
| `reset_forecast_run_date` | Utility for resetting forecast dates |
| `validate_pipeline` | Pipeline validation utilities |
| `iEasyHydroForecast` | Core forecasting library |

**Legacy/deprecated modules:**
- `backend/` — legacy, being phased out
- `machine_learning_monthly/` — deprecated, replaced by `long_term_forecasting`

### 2. SAPPHIRE Services (`sapphire/services/`)

FastAPI microservices with PostgreSQL backends (managed by colleague — see Ownership Boundaries above):

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

### Pipeline Data Flow

For detailed data flow diagrams (Mermaid) showing how data moves through the
operational, maintenance, and annual recalculation pipelines, see:

- **Short-term** (pentad/decade): [`doc/data_flow_short_term.md`](doc/data_flow_short_term.md)
- **Long-term** (monthly): [`doc/data_flow_long_term.md`](doc/data_flow_long_term.md)

---

## Code Style Conventions

### Python Style

- **Line length**: 79 for docstrings, 100 for code
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

### Linting and Formatting (ruff)

The project uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting, configured in `ruff.toml` at the repo root. A pre-commit hook runs `ruff check --fix` and `ruff format` automatically on every commit.

**When refactoring a module**, clean it up first in a dedicated commit:

```bash
ruff check --fix apps/<module>/
ruff format apps/<module>/
ruff check apps/<module>/          # review remaining manual fixes
```

**Key rules enabled**: `E` (pycodestyle), `F` (pyflakes), `I` (isort), `UP` (pyupgrade), `B` (bugbear), `SIM` (simplify). The full rule selection and per-path ignores are documented in `ruff.toml`.

**Excluded from linting**: `backend/`, `machine_learning_monthly/`, `conceptual_model/`, `daily_runoff/` (legacy/deprecated).

**Do not** add `# noqa` comments to silence warnings without understanding the underlying issue. Fix the code instead, or add the rule to `ruff.toml` ignores if it is genuinely too noisy project-wide.

### Naming Conventions

- **Functions/methods**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private methods**: `_single_leading_underscore`

### Model Name Convention

`model_short` (e.g., `"TFT"`, `"LR"`) is the working identifier throughout the pipeline.
`model_long` (e.g., `"Temporal Fusion Transformer (TFT)"`) is a **display concern** defined
only in sapphire services (`ModelType.description` in
`sapphire/services/postprocessing/app/models.py`).

When refactoring a module:
- Do not add `model_long` to DataFrames or internal data flow
- Remove existing `model_long` assignments if the code is being touched
- Presentation boundaries (CSV output, dashboard display) should resolve long names
  from the API response field `model_type_description`, not from local dicts
- See `doc/plans/issues/mid_prio_gi_draft_infra_model_registry.md` for the per-module checklist

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

For idempotent data operations, use the upsert pattern. Note: this uses PostgreSQL-specific syntax (`ON CONFLICT`) — service tests use SQLite in-memory databases which handle this differently via SQLAlchemy's `insert().on_conflict_do_update()`.

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

## Git Conventions

### Branch Naming

Use descriptive branch names with a prefix indicating the type of work:

- `develop_<module>_<description>` — feature development branches
- `fix_<module>_<description>` — bug fix branches
- `infra_<description>` — infrastructure/cross-module changes

### Commit Messages

Write concise commit messages that focus on the "why" rather than the "what". Use imperative mood for the subject line (e.g., "Fix boundary day guard" not "Fixed boundary day guard").

### Pull Requests

- Keep PRs focused on a single issue or feature
- Target `main` for production-ready changes
- Include a summary of changes and test results in the PR description

---

## Testing Requirements

### Testing Philosophy

Good tests describe contracts — what must stay true even if the implementation changes. If a test breaks after a refactor that doesn't change behavior, the test was wrong.

**Golden Rules:**

1. **Test behavior, not implementation** — assert on outputs and public APIs. Do not inspect private attributes (`._internal_state`) unless no public API exists.
2. **Prefer fast, deterministic tests** — no `sleep()`, no uncontrolled `datetime.now()` or `random`. Pass the forecast date as a parameter (see "The Forecast Date Rule" below).
3. **Use fakes over mocks where practical** — a fake implementation (e.g., an in-memory store) is easier to read and more resilient than a chain of `MagicMock` assertions. Reserve `MagicMock` for external boundaries (API clients, file I/O, external services).
4. **Structure tests as Arrange → Act → Assert** — setup the data, call the function, check the result. Name fixtures descriptively (`pentad_skill_csv`, `df_with_missing_values`), not generically (`data`, `fixture1`).

### The Forecast Date Rule

The forecast date is a **domain concept** — the date a forecast is being produced for. It must be captured once at the pipeline entry point and passed as a parameter to all functions that need it. Do not scatter `date.today()` calls through business logic.

```python
# Entry point captures once:
forecast_date = date.today()

# All downstream functions receive it as a parameter:
def process_pentad(data: pd.DataFrame, forecast_date: date) -> pd.DataFrame:
    current_year = forecast_date.year
    pentad = get_pentad_in_year(forecast_date)
    ...

# NEVER use date.today() in a default argument:
# WRONG — evaluated once at import time, stale at year boundary
def get_date_for_pentad(pentad, year=datetime.now().year): ...

# CORRECT — caller passes explicitly
def get_date_for_pentad(pentad: int, year: int) -> date: ...
```

**Acceptable uses of `datetime.now()`**: Logging timestamps, file naming, and performance timers.

**Reference implementation**: `long_term_forecasting/__init__.py` with `initialize_today()` / `get_today()`.

### Before Committing or Moving to New Topic

**All tests must pass with zero skips before committing or moving to a new topic.** The full pre-commit validation has three stages:

1. **Unit/integration tests** (always): `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh`
2. **Local pipeline run** (after major changes): `bash apps/run_locally.sh all`
3. **Docker smoke tests** (after major changes): `bash apps/run_docker_tests.sh --skip-ml`

Stage 1 is required before every commit. Stages 2 and 3 are required after changes that affect module dependencies, entry points, Docker configuration, or cross-module data flow.

### Zero Skips Policy

**No tests may be skipped without justification.** If any tests are skipped or fail to collect, treat this as a red flag requiring investigation before proceeding. Do not accept "0 collected" or `pytest.skip()` as normal — find and fix the root cause.

**One exception**: dependency-gated skips are acceptable when `sapphire-api-client` is not installed. These tests guard on `SAPPHIRE_API_AVAILABLE` and skip with an explicit message like `pytest.skip("sapphire-api-client not installed")`. This is the only valid skip pattern — all other skips indicate hidden bugs.

### Test Categories and Writing Guide

Every new feature or bug fix must include tests. For the full specification of required test categories (unit, edge case, integration, API failure, performance), assertion quality rules, the conftest.py pattern, file naming conventions, and test anti-patterns, see [`doc/dev/testing_workflow.md`](doc/dev/testing_workflow.md).

### Running Tests

Always use `run_tests.sh` rather than running pytest manually:

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh              # all tests
SAPPHIRE_TEST_ENV=True bash run_tests.sh <module>      # single app module
bash run_tests.sh service:<service>                     # single service
```

For the full testing workflow (local pipeline, Docker smoke tests, CI/CD, server validation), see [`doc/dev/testing_workflow.md`](doc/dev/testing_workflow.md).

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
│   ├── long_term_forecasting/
│   ├── postprocessing_forecasts/
│   ├── iEasyHydroForecast/
│   ├── validate_pipeline/
│   └── ...
├── sapphire/
│   └── services/               # FastAPI microservices (colleague-managed)
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
├── ruff.toml                   # Linting/formatting config (repo-wide)
└── CLAUDE.md                   # This file
```

---

## Issue Planning

See `doc/plans/module_issues.md` for the index of planned issues.

Issue files are stored in `doc/plans/issues/` with naming convention:
- Draft: `<priority>_gi_draft_<module>_<description>.md` — where `<priority>` is `high_prio`, `mid_prio`, or `low_prio`
- Review: `review_gi_draft_<module>_<description>.md` — implementation complete, awaiting user review
- Published: `<priority>_gi_<github_id>_<description>.md`
- Lifecycle: `<priority>_gi_draft_*.md` → `review_gi_draft_*.md` → `archive/`
