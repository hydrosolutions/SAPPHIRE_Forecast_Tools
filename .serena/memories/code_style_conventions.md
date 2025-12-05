# SAPPHIRE Forecast Tools - Code Style and Conventions

## Python Version
- Minimum: Python 3.11
- Migration to Python 3.12 in progress (with uv package manager)

## Documentation Style

### Docstrings
Google-style docstrings are used:
```python
def function_name(param1, param2):
    """
    Brief description of the function.
    
    Parameters:
        param1 (type): Description of param1
        param2 (type): Description of param2
        
    Returns:
        return_type: Description of return value
        
    Raises:
        ExceptionType: When this exception is raised
    """
```

### Logging
Use Python's logging module:
```python
import logging
logger = logging.getLogger(__name__)

logger.debug("Debug message")
logger.info("Info message")
logger.warning("Warning message")
logger.error("Error message")
```

## Naming Conventions
- **Functions/variables**: snake_case (e.g., `parse_dates_robust`, `date_series`)
- **Classes**: PascalCase (e.g., `Site`, `PredictorDates`)
- **Constants**: UPPER_SNAKE_CASE
- **Private members**: Leading underscore (e.g., `_internal_helper`)

## Type Hints
Type hints are used selectively, especially in function signatures:
```python
def function_name(param: str, value: int = 0) -> pd.Series:
    ...
```

## Code Organization

### Module Structure
Each app module typically contains:
- Main script (e.g., `preprocessing.py`, `linreg.py`)
- `requirements.txt` or `pyproject.toml` for dependencies
- `Dockerfile` for containerization
- `test/` or `tests/` directory for tests
- `README.md` (optional)

### Import Order
1. Standard library imports
2. Third-party imports
3. Local/project imports

```python
import logging
import os

import pandas as pd
import numpy as np

from iEasyHydroForecast import forecast_library
```

## Testing Conventions
- Test files: `test_*.py`
- Test classes: `Test*`
- Test functions: `test_*`
- Use `SAPPHIRE_TEST_ENV=True` environment variable for test mode
- pytest is the primary test framework (some modules use unittest)

## Configuration
- Environment variables via `.env` files (using python-dotenv)
- JSON configuration files in `apps/config/`
- Separate `.env_develop` for local development

## Error Handling
- Use specific exception types
- Log errors before raising
- Provide informative error messages with context

```python
if not valid:
    error_msg = f"Invalid data in {column_name}. Expected format: YYYY-MM-DD"
    logger.error(error_msg)
    raise ValueError(error_msg)
```

## Pandas Best Practices
- Use explicit date parsing with fallback formats
- Handle missing data gracefully (NaN checks)
- Log warnings for partial data parsing success

---

# Recommended Improvements (Going Forward)

The following guidelines should be adopted for all new code and refactoring efforts:

## 1. Use Dataclasses for Data-Holding Classes
For classes that primarily hold data (like `Site`), use `@dataclass` to reduce boilerplate:

```python
from dataclasses import dataclass, field

@dataclass
class Site:
    code: str
    iehhf_site_id: int = -999
    name: str = "Name"
    lat: float = 0.0
    lon: float = 0.0
    # ... define defaults directly, no repetitive __init__ needed
```

Benefits:
- Automatic `__init__`, `__repr__`, `__eq__` generation
- Cleaner, more readable code
- Type hints are naturally integrated

## 2. Consistent Type Hints (Mandatory for New Code)
All new functions and methods should include complete type hints:

```python
# Good
def calculate_forecast(
    site: Site,
    group_id: str,
    df: pd.DataFrame,
    code_col: str = 'code'
) -> float | None:
    ...

# Avoid (no type hints)
def calculate_forecast(site, group_id, df, code_col='code'):
    ...
```

For complex types, use:
```python
from typing import TypeAlias

SiteList: TypeAlias = list[Site]
DataFrameDict: TypeAlias = dict[str, pd.DataFrame]
```

## 3. Use Logger Consistently (No print() in Production Code)
Replace all `print()` statements with appropriate logging levels:

```python
# Bad
print(f'Error {e}. Returning " ".')
print("\n\nDEBUG: Site: ", site.code)

# Good
logger.error(f'Error {e}. Returning empty string.')
logger.debug(f'Site: {site.code}')
```

Logging levels:
- `logger.debug()` - Detailed diagnostic info
- `logger.info()` - General operational messages
- `logger.warning()` - Something unexpected but recoverable
- `logger.error()` - Errors that need attention

## 4. Define Constants for Magic Values
Avoid hard-coded magic numbers scattered throughout the code:

```python
# Bad
self.predictor = predictor if predictor is not None else -10000.0
self.fc_qmin = fc_qmin if fc_qmin is not None else -10000.0

# Good
MISSING_VALUE: float = -10000.0
DEFAULT_SITE_ID: int = -999

self.predictor = predictor if predictor is not None else MISSING_VALUE
```

## 5. Reduce Code Duplication with Parameterization
When methods differ only by column names or small variations, consolidate them:

```python
# Bad: Separate methods for pentad and decad
def from_df_calculate_forecast_pentad(cls, site, pentad, df): ...
def from_df_calculate_forecast_decad(cls, site, decad, df): ...

# Good: Single parameterized method
def from_df_calculate_forecast(
    cls,
    site: Site,
    period_id: str,
    df: pd.DataFrame,
    period_col: str = 'pentad_in_year'  # or 'decad_in_year'
) -> float | None:
    ...
```

## 6. Standardize Test Directory Naming
Use `tests/` (plural) consistently across all modules:
```
apps/
├── module_name/
│   ├── tests/          # Preferred (plural)
│   │   ├── __init__.py
│   │   ├── test_main.py
│   │   └── conftest.py
```

## 7. Add Linting and Formatting Tools
Add these to `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.4.4
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.10.0
    hooks:
      - id: mypy
        additional_dependencies: [pandas-stubs, types-pytz]
```

Consider adding a `pyproject.toml` section for ruff configuration:
```toml
[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM"]
```

## 8. Prefer Modern Python Syntax (3.11+)
Since the project requires Python 3.11+, use modern syntax:

```python
# Use union syntax (not Optional)
def get_value() -> str | None:  # Not Optional[str]
    ...

# Use built-in generics
def process(items: list[str]) -> dict[str, int]:  # Not List[str], Dict[str, int]
    ...

# Use match statements where appropriate
match status:
    case "success":
        return True
    case "error":
        logger.error("Operation failed")
        return False
```
