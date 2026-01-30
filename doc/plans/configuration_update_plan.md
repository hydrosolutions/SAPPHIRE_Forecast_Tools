# SAPPHIRE Configuration Update Plan

## Overview

This document outlines a comprehensive plan to improve configuration management across the SAPPHIRE Forecast Tools project. The goals are to:

1. Move from organization-based to feature-flag-based configuration
2. Standardize configuration patterns across all modules
3. Improve developer experience and discoverability
4. Maintain security for credentials and secrets

**Created**: 2026-01-30
**Status**: Planning

---

## Problem Statement

### Current Issues

1. **Organization-based logic is inflexible**: The `ieasyhydroforecast_organization` variable (demo/kghm/tjhm) controls too many features implicitly. Adding new organizations requires code changes.

2. **Configuration sprawl**: 100+ environment variables in `.env` files with minimal structure, poor discoverability.

3. **Inconsistent patterns**: Each module loads configuration differently. Only `preprocessing_runoff` has a structured config.yaml approach.

4. **No validation**: Configuration errors only discovered at runtime, often with cryptic messages.

5. **Mixed concerns**: Secrets, deployment settings, and application defaults all live in the same `.env` file.

---

## Recommended Architecture

### Configuration Hierarchy (Priority: Highest to Lowest)

```
1. Environment Variables     <- Deployment overrides, CI/CD
2. .env File                 <- Secrets, deployment-specific paths
3. config.yaml               <- Application defaults (in repo)
4. Code Defaults             <- Fallback values
```

### File Organization

**Approach: Distributed module configs with centralized shared config**

Module-specific configuration lives with the module (`apps/<module>/config.yaml`) rather than centralized (`apps/config/module_configs/`). This keeps modules self-contained and matches how `preprocessing_runoff` already works.

```
apps/config/
├── .env                           # Deployment config (NOT in repo)
├── .env.example                   # Template (in repo)
├── config.yaml                    # Shared defaults (in repo)
├── config_station_selection.json  # Station configs (existing)
└── config_output.json             # Output configs (existing)

apps/<module>/                     # e.g., preprocessing_runoff, machine_learning
├── config.yaml                    # Module-specific defaults (in repo)
├── config.example.yaml            # Documented template (in repo)
├── src/
├── tests/
└── pyproject.toml

apps/iEasyHydroForecast/
├── config/
│   ├── __init__.py
│   ├── config_manager.py          # Central config loader
│   ├── schema.py                  # Type definitions (Pydantic)
│   └── validators.py              # Validation logic
└── src/
```

**Why distributed over centralized?**
- Module is self-contained: all related files in one directory
- Matches existing pattern (`preprocessing_runoff` already has `config.yaml`)
- Easier onboarding: "everything about module X is in `apps/X/`"
- Git history stays with the module
- Docker COPY is simpler: just copy the module directory

### Demo Data Separation

The `apps/config/` folder is legacy from the first demo version when Swiss public data was shipped with the repo. Analysis shows this folder mixes several concerns that should be separated.

**Why `apps/config/` stays (not renamed/moved):**
- Medium-high coupling: 26 files reference `ieasyforecast_configuration_path`
- Hardcoded paths in Docker detection logic (`forecast_dashboard/src/environment.py`)
- Docker volume mount patterns expect `config/` location
- Documentation explicitly recommends not changing the path
- Test fixtures and CI/CD depend on current structure

**What `apps/config/` should contain (configuration infrastructure):**
- `.env.example` - Template for deployment (tracked)
- `.env` - Actual deployment config (gitignored)
- `config.yaml` - Shared application defaults (tracked)
- `config_all_stations_library.json` - Reference data: station metadata (tracked)
- `config_station_selection.json` - Per-deployment selection (gitignored or tracked template)
- `locale/` - Localization strings (tracked, reference data)

**What moves to new `demo_data/` folder (sample data, not config):**
- Sample discharge Excel files (currently in `data/daily_runoff/`)
- Sample output CSVs for demonstration (currently in `apps/internal_data/`)
- Pre-filled demo configs as examples
- Report templates (currently in `data/templates/`)

**What stays in module `test/` directories (test fixtures):**
- Test fixtures belong with their modules, not in demo_data
- `apps/iEasyHydroForecast/tests/test_data/` - stays
- `apps/preprocessing_runoff/test/test_files/` - stays

**Recommended structure after reorganization:**

```
SAPPHIRE_forecast_tools/
├── apps/
│   ├── config/                              # Configuration infrastructure (KEEP)
│   │   ├── .env.example                     # Template (tracked)
│   │   ├── .env                             # Deployment-specific (gitignored)
│   │   ├── config.yaml                      # Shared defaults (tracked, NEW)
│   │   ├── config_all_stations_library.json # Reference data (tracked)
│   │   ├── config_station_selection.json    # Template or gitignored
│   │   ├── config_output.json.example       # Template (tracked)
│   │   └── locale/                          # Localization (tracked)
│   │       ├── en_CH/
│   │       └── ru_KG/
│   │
│   ├── internal_data/                       # Generated outputs only (gitignored)
│   │   └── .gitkeep
│   │
│   └── <module>/
│       ├── config.yaml                      # Module-specific defaults
│       └── tests/test_data/                 # Test fixtures (stay here)
│
├── demo_data/                               # NEW: Explicit demo package
│   ├── README.md                            # Quick start guide
│   ├── swiss_demo/
│   │   ├── config/
│   │   │   ├── config_station_selection.json  # Pre-filled for Swiss demo
│   │   │   └── config_output.json             # Demo output settings
│   │   ├── discharge_data/
│   │   │   ├── 12176_Sihl_example.xlsx        # From data/daily_runoff/
│   │   │   └── 12256_Rosegbach_example.xlsx
│   │   └── sample_outputs/
│   │       ├── hydrograph_day.csv             # Expected output format
│   │       └── forecasts_pentad.csv
│   └── templates/
│       ├── pentad_forecast_bulletin_template.xlsx
│       └── short_term_trad_sheet_template.xlsx
│
└── data/                                    # Deployment data (external or gitignored)
    ├── daily_runoff/                        # Move contents to demo_data/
    ├── templates/                           # Move to demo_data/templates/
    └── GIS/                                 # Keep or move based on size
```

**Key distinctions:**

| Content Type | Location | Tracked | Rationale |
|--------------|----------|---------|-----------|
| Config templates | `apps/config/*.example` | Yes | Shows format, safe to share |
| Reference data (stations, locales) | `apps/config/` | Yes | Domain definitions, all deployments use |
| Deployment config | `apps/config/.env` | No | Contains secrets, paths |
| Demo sample data | `demo_data/swiss_demo/` | Yes | Explicit demo, self-contained |
| Test fixtures | `apps/<module>/tests/` | Yes | Belong with their tests |
| Generated outputs | `apps/internal_data/` | No | Runtime artifacts |

### What Goes Where

| Configuration Type | Location | Tracked in Git |
|-------------------|----------|----------------|
| Database credentials | `apps/config/.env` only | No |
| API keys | `apps/config/.env` only | No |
| File paths (deployment-specific) | `apps/config/.env` | No |
| Organization identifier | `apps/config/.env` or `apps/config/config.yaml` | Optional |
| Feature flags (shared) | `apps/config/config.yaml` (overridable in `.env`) | Yes |
| Module defaults (thresholds, etc.) | `apps/<module>/config.yaml` | Yes |
| File naming conventions | `apps/config/config.yaml` | Yes |
| Localization strings | `apps/config/locale/` | Yes |
| Station reference library | `apps/config/config_all_stations_library.json` | Yes |
| Station selection (deployment) | `apps/config/config_station_selection.json` | No (or template) |
| Demo sample discharge data | `demo_data/swiss_demo/discharge_data/` | Yes |
| Demo sample outputs | `demo_data/swiss_demo/sample_outputs/` | Yes |
| Report templates | `demo_data/templates/` | Yes |
| Test fixtures | `apps/<module>/tests/test_data/` | Yes |
| Generated outputs | `apps/internal_data/` | No |

---

## Key Change: Organization to Feature Flags

### Current Pattern (Problematic)

```python
# Hard-coded organization checks scattered throughout code
organization = os.getenv('ieasyhydroforecast_organization')
if organization == 'kghm':
    run_ml_models = True
    run_cm_models = True
    connect_to_ieh_hf = True
elif organization == 'demo':
    run_ml_models = False
    # etc.
```

**Problems:**
- Can't test ML models in demo mode
- Adding `tjhm` requires code changes
- Organization identity conflated with features

### Proposed Pattern (Explicit Feature Flags)

```yaml
# config.yaml
organization: kghm  # For logging/identification only

features:
  run_ml_models: true
  run_cm_models: true
  connect_to_ieh_hf: true
  enable_notifications: true
```

```python
# Code checks features, not organization
config = SAPPHIREConfig.load()
if config.features.run_ml_models:
    run_ml_forecasts()
```

**Benefits:**
- Explicit control over each feature
- Test any feature combination
- New organizations don't require code changes
- Clear documentation of available features

### Migration Strategy

**Phase 1: Add feature flags alongside organization (backward compatible)**
```python
# Support both patterns during transition
org = os.getenv('ieasyhydroforecast_organization', 'demo')
# Derive defaults from organization
defaults = ORGANIZATION_DEFAULTS.get(org, {})
# Allow explicit override
run_ml = os.getenv('ieasyhydroforecast_run_ML_models',
                   str(defaults.get('run_ml_models', False)))
```

**Phase 2: Update code to check features first**
```python
# Feature flags take priority
if config.features.run_ml_models:
    # Run ML regardless of organization
```

**Phase 3: Deprecate organization-based inference**
- Log warnings when org is used to infer features
- Document migration in deployment guides

---

## Configuration Schema (Proposed)

### Shared Configuration (`apps/config/config.yaml`)

```yaml
# SAPPHIRE Forecast Tools - Shared Configuration
# Override values in .env or environment variables

organization: demo  # Identifier: demo | kghm | tjhm | custom

features:
  run_ml_models: false
  run_cm_models: false
  connect_to_ieh_hf: false
  enable_notifications: false

paths:
  configuration: apps/config
  intermediate_data: apps/internal_data
  templates: data/templates

files:
  daily_discharge: runoff_day.csv
  pentad_discharge: runoff_pentad.csv
  hydrograph_day: hydrograph_day.csv
  hydrograph_pentad: hydrograph_pentad.csv

localization:
  locale: en_CH
  locale_dir: apps/config/locale

logging:
  level: INFO
  file: ./forecast_logs.txt

docker:
  backend_tag: latest
  frontend_tag: latest
```

### Module Configuration (Example: `apps/preprocessing_runoff/config.yaml`)

This already exists and serves as the template for other modules.

```yaml
# preprocessing_runoff specific settings
# Override with environment variables prefixed with PREPROCESSING_

maintenance:
  lookback_days: 50

operational:
  fetch_yesterday: true
  fetch_morning: true

validation:
  enabled: true
  max_age_days: 3
  reliability_threshold: 80.0

site_cache:
  enabled: true
  max_age_days: 7
```

Other modules to add `config.yaml`:
- `apps/machine_learning/config.yaml` - model hyperparameters, training settings
- `apps/pipeline/config.yaml` - orchestration settings, timeout values
- `apps/postprocessing_forecasts/config.yaml` - skill metric thresholds

### Open Questions (To Be Analyzed in Detail Later)

The following considerations need deeper analysis during implementation:

**1. Organization Variable Future**

The `organization` variable (currently: `demo`, `kghm`, `tjhm`) may become deprecated or evolve:
- Current values are specific to initial deployments (Kyrgyzstan, Tajikistan, demo)
- Future deployments may include other Central Asian hydromets, Caucasus, Nepal, Switzerland
- Consider: Should `organization` become a free-form deployment identifier (e.g., `deployment_name`) rather than an enum?
- Consider: Should organization be purely for logging/identification with zero runtime behavior?
- Action: Design schema to be flexible for future organization changes

**2. Uniform Environment Variable Naming**

Current env vars have inconsistent prefixes:
- `ieasyhydroforecast_*` - most variables
- `ieasyforecast_*` - some path variables
- `IEASYHYDRO*` - database credentials
- `SAPPHIRE_*` - some newer variables

Question: Should all env vars be standardized to `SAPPHIRE_*` prefix?

Pros:
- Clear namespace, easy to identify project variables
- Reduces confusion with legacy `ieasyhydro` naming
- Better for documentation and discoverability

Cons:
- Breaking change for all existing deployments
- Need migration period with both old and new names
- Documentation and scripts need updating

Action: Analyze impact and decide during Phase 1 implementation

**3. Reference .env File**

The current `apps/config/.env` file is **severely outdated** and should not be used as the template.

Action: Use an operational deployment `.env` file (e.g., from kghm production) as the reference when creating `.env.example`. This ensures the template includes all currently-used variables with realistic examples.

### Environment Variables (`.env`)

```bash
#==============================================================================
# SAPPHIRE Forecast Tools - Deployment Configuration
# Copy from .env.example and customize for your deployment
#==============================================================================

#------------------------------------------------------------------------------
# DATABASE CREDENTIALS (REQUIRED for production deployments)
#------------------------------------------------------------------------------
# iEasyHydro HF High-Frequency database
IEASYHYDROHF_HOST=https://hf.ieasyhydro.org/api/v1/
IEASYHYDROHF_USERNAME=<username>
IEASYHYDROHF_PASSWORD=<password>

#------------------------------------------------------------------------------
# FEATURE FLAGS (override config.yaml defaults)
#------------------------------------------------------------------------------
ieasyhydroforecast_run_ML_models=False
ieasyhydroforecast_run_CM_models=False
ieasyhydroforecast_connect_to_iEH_HF=False

#------------------------------------------------------------------------------
# DEPLOYMENT PATHS (adjust for your server)
#------------------------------------------------------------------------------
ieasyhydroforecast_data_root_dir=/data/kyg_data_forecast_tools
ieasyforecast_configuration_path=../config
ieasyforecast_intermediate_data_path=../intermediate_data

#------------------------------------------------------------------------------
# API KEYS (if using ML models or Data Gateway)
#------------------------------------------------------------------------------
# ieasyhydroforecast_API_KEY_GATEAWAY=<api_key>

#------------------------------------------------------------------------------
# NOTIFICATIONS (optional)
#------------------------------------------------------------------------------
# SAPPHIRE_PIPELINE_SMTP_PASSWORD=<smtp_password>
# TWILIO_AUTH_TOKEN=<twilio_token>
```

---

## Implementation: ConfigManager

### Standardized Pattern (Based on preprocessing_runoff)

```python
# apps/iEasyHydroForecast/config/config_manager.py
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import os
import yaml
from dotenv import load_dotenv

@dataclass
class FeaturesConfig:
    """Feature toggles."""
    run_ml_models: bool = False
    run_cm_models: bool = False
    connect_to_ieh_hf: bool = False
    enable_notifications: bool = False

@dataclass
class PathsConfig:
    """File system paths."""
    configuration: Path
    intermediate_data: Path
    templates: Path

@dataclass
class SAPPHIREConfig:
    """Root configuration object."""
    organization: str
    features: FeaturesConfig
    paths: PathsConfig
    log_level: str
    in_docker: bool

    @classmethod
    def load(cls,
             env_file: Optional[Path] = None,
             config_file: Optional[Path] = None) -> 'SAPPHIREConfig':
        """
        Load configuration with priority:
        1. Environment variables (highest)
        2. .env file
        3. config.yaml
        4. Defaults (lowest)
        """
        # Load .env if exists
        if env_file and env_file.exists():
            load_dotenv(env_file)

        # Load config.yaml
        yaml_config = {}
        if config_file and config_file.exists():
            with open(config_file) as f:
                yaml_config = yaml.safe_load(f) or {}

        # Build config with overrides
        return cls._build_config(yaml_config)

    @classmethod
    def _build_config(cls, yaml_config: dict) -> 'SAPPHIREConfig':
        """Build config object with env override support."""
        # Organization
        org = os.getenv('ieasyhydroforecast_organization',
                       yaml_config.get('organization', 'demo'))

        # Features (env overrides yaml)
        features_yaml = yaml_config.get('features', {})
        features = FeaturesConfig(
            run_ml_models=_parse_bool(
                os.getenv('ieasyhydroforecast_run_ML_models'),
                features_yaml.get('run_ml_models', False)
            ),
            run_cm_models=_parse_bool(
                os.getenv('ieasyhydroforecast_run_CM_models'),
                features_yaml.get('run_cm_models', False)
            ),
            connect_to_ieh_hf=_parse_bool(
                os.getenv('ieasyhydroforecast_connect_to_iEH_HF'),
                features_yaml.get('connect_to_ieh_hf', False)
            ),
            enable_notifications=_parse_bool(
                os.getenv('ieasyhydroforecast_enable_notifications'),
                features_yaml.get('enable_notifications', False)
            ),
        )

        # ... build rest of config
        return cls(organization=org, features=features, ...)

def _parse_bool(env_value: Optional[str], default: bool) -> bool:
    """Parse boolean from environment variable."""
    if env_value is None:
        return default
    return env_value.lower() in ('true', 'yes', '1', 't', 'y')
```

---

## Migration Plan

### Phase 0: Demo Data Separation (Week 1)

Separate demo sample data from configuration infrastructure.

- [ ] Create `demo_data/` directory structure
- [ ] Create `demo_data/README.md` with quick start guide
- [ ] Move Swiss demo discharge files: `data/daily_runoff/*.xlsx` → `demo_data/swiss_demo/discharge_data/`
- [ ] Copy demo config examples: `apps/config/config_*.json` → `demo_data/swiss_demo/config/`
- [ ] Move report templates: `data/templates/` → `demo_data/templates/`
- [ ] Create sample outputs in `demo_data/swiss_demo/sample_outputs/`
- [ ] Clean `apps/internal_data/` - remove committed sample CSVs, add `.gitkeep`
- [ ] Update `.gitignore` for `apps/internal_data/` (generated outputs)
- [ ] Rename `.env` files to `.env.example` patterns
- [ ] Add current `.env*` files to `.gitignore`
- [ ] Update quick start documentation to reference `demo_data/`

### Phase 1: Configuration Infrastructure (Week 1-2)

- [ ] Create `apps/iEasyHydroForecast/config/` package
- [ ] Implement `ConfigManager` with schema
- [ ] Create `apps/config/config.yaml` with shared defaults
- [ ] Write tests for config loading
- [ ] Document configuration schema

### Phase 2: Migrate Shared Library (Week 2-3)

- [ ] Update `setup_library.py` to use ConfigManager
- [ ] Replace direct `os.getenv()` calls with config attributes
- [ ] Update tests to use config fixtures
- [ ] Maintain backward compatibility

### Phase 3: Migrate Modules (Week 3-5)

Priority order:
1. [ ] `preprocessing_runoff` - Already has good pattern, extend it
2. [ ] `pipeline` - High value, controls all workflows
3. [ ] `machine_learning` - Uses many feature flags
4. [ ] `postprocessing_forecasts` - Uses organization checks
5. [ ] Remaining modules (linear_regression, conceptual_model, etc.)
6. [ ] Dashboards

### Phase 4: Feature Flag Migration (Week 5-6)

- [ ] Add explicit feature flags to all deployments
- [ ] Update code to check features, not organization
- [ ] Add deprecation warnings for organization-based inference
- [ ] Update deployment documentation

### Phase 5: Cleanup (Week 6-7)

- [ ] Remove legacy organization-based logic
- [ ] Remove deprecated environment variables
- [ ] Update all documentation
- [ ] Final testing on all deployment configurations

---

## Testing Requirements

**Critical principle: The existing code cannot be broken. All tests in this repo must pass before any commit.**

### Testing Strategy

Every phase of implementation must include extensive testing:

1. **Unit tests** for all new configuration code
2. **Integration tests** for config loading across modules
3. **Regression tests** ensuring existing functionality works
4. **Full workflow tests** using `demo_data/`

### Unit Tests (Per Phase)

Each new component needs comprehensive unit tests:

```
apps/iEasyHydroForecast/tests/
├── test_config_manager.py      # ConfigManager loading, defaults, overrides
├── test_config_schema.py       # Schema validation, type checking
├── test_config_validators.py   # Custom validation logic
└── test_config_migration.py    # Backward compatibility tests
```

Test cases must cover:
- Default values when no config provided
- YAML config loading
- Environment variable overrides
- Priority ordering (env > yaml > defaults)
- Invalid config handling (clear error messages)
- Boolean parsing edge cases (`True`, `true`, `yes`, `1`, etc.)
- Path resolution (relative vs absolute)

### Integration Tests with demo_data

**The full forecast workflow using `demo_data/` can be fully integration tested without mocks.**

The Swiss demo data provides a complete, self-contained test environment:
- Sample discharge data (Excel files)
- Pre-configured stations (12176, 12256)
- Expected output formats

Integration test workflow:
```python
# tests/integration/test_full_workflow.py

def test_full_demo_workflow():
    """
    Run complete forecast pipeline with demo_data.
    No mocks needed - uses real data and real code paths.
    """
    # 1. Load demo configuration
    config = SAPPHIREConfig.load(
        config_file=Path("demo_data/swiss_demo/config/config.yaml")
    )

    # 2. Run preprocessing with demo discharge data
    # 3. Run linear regression
    # 4. Run postprocessing
    # 5. Verify outputs match expected format

    # Assert outputs are generated correctly
    assert Path("apps/internal_data/hydrograph_day.csv").exists()
    assert Path("apps/internal_data/forecasts_pentad.csv").exists()
```

### Test Checkpoints (Per Phase)

**Phase 0 (Demo Data Separation):**
- [ ] All existing tests still pass after file moves
- [ ] Demo workflow runs with new `demo_data/` location
- [ ] CI/CD pipeline passes

**Phase 1 (Config Infrastructure):**
- [ ] ConfigManager unit tests pass (100% coverage target)
- [ ] Config loading works with existing `.env` patterns
- [ ] Backward compatibility: old env var names still work

**Phase 2 (Shared Library Migration):**
- [ ] All `iEasyHydroForecast` tests pass
- [ ] No regressions in modules using shared library
- [ ] Integration test with demo workflow passes

**Phase 3 (Module Migration):**
- [ ] Each module's tests pass after migration
- [ ] Cross-module integration tests pass
- [ ] Full pipeline test passes

**Phase 4-5 (Feature Flags & Cleanup):**
- [ ] Feature flag combinations tested
- [ ] Deprecation warnings appear correctly
- [ ] Final full workflow test passes

### CI/CD Integration

All tests must run in CI before merge:

```yaml
# .github/workflows/build_test.yml additions
- name: Run config unit tests
  run: |
    SAPPHIRE_TEST_ENV=True pytest apps/iEasyHydroForecast/tests/test_config*.py -v

- name: Run integration tests with demo_data
  run: |
    SAPPHIRE_TEST_ENV=True pytest tests/integration/ -v --demo-data
```

### Test Data Management

- Test fixtures stay in `apps/<module>/tests/test_data/` (not in `demo_data/`)
- `demo_data/` is for integration testing and user onboarding
- No mocks needed for demo workflow - use real Swiss data
- Mocks only for external services (iEasyHydro HF API) when testing non-demo configs

---

## Security Considerations

### Secrets Management

**Critical: Never commit to version control:**
- Database passwords (`IEASYHYDROHF_PASSWORD`)
- API keys (`ieasyhydroforecast_API_KEY_GATEAWAY`)
- SMTP passwords (`SAPPHIRE_PIPELINE_SMTP_PASSWORD`)
- Notification tokens (`TWILIO_AUTH_TOKEN`)

**Recommended practices:**
1. Use `.env` files with restricted permissions (`chmod 600`)
2. Add `.env*` patterns to `.gitignore` (except `.env.example`)
3. Consider Docker secrets for production deployments
4. Document credential rotation process

### .gitignore Updates

```gitignore
# Environment files with credentials - NEVER commit
**/.env
**/.env_*
!**/.env.example
!**/.env*.example
apps/config/.env
apps/config/.env_develop
apps/config/.env_develop_*

# Keep templates
!apps/config/.env.example
!apps/config/.env.example.*

# Generated intermediate data (runtime artifacts)
apps/internal_data/*.csv
apps/internal_data/*.txt
apps/internal_data/*.pkl
apps/internal_data/*.json
!apps/internal_data/.gitkeep

# Deployment-specific station selection (if customized)
# apps/config/config_station_selection.json  # Uncomment if per-deployment

# Model artifacts (large binaries)
apps/config/models_and_scalers/*.csv
apps/config/models_and_scalers/*.pkl
```

---

## Developer Experience Improvements

### Configuration Discoverability

1. **config.example.yaml** - Self-documenting template in repo
2. **Startup validation** - Clear error messages on misconfiguration
3. **`--show-config` command** - Display current configuration
4. **Grouped .env sections** - Related variables together with comments

### Quick Start for New Developers

```bash
# 1. Copy shared config template
cp apps/config/.env.example apps/config/.env

# 2. For demo mode: copy demo configs (optional, defaults work)
cp demo_data/swiss_demo/config/* apps/config/

# 3. Edit .env with your credentials (for non-demo deployments)
vim apps/config/.env

# 4. Run (demo mode works with defaults)
cd apps/preprocessing_runoff
uv run main.py
```

**Demo data location:** Sample discharge files and expected outputs are in `demo_data/swiss_demo/`. Use these to understand data formats and verify your setup.

Module-specific configs (`apps/<module>/config.yaml`) ship with sensible defaults and don't need copying for basic usage.

### Testing Different Configurations

```python
# conftest.py - Test fixtures for different configs
@pytest.fixture
def config_demo():
    return SAPPHIREConfig.from_dict({
        'organization': 'demo',
        'features': {'run_ml_models': False}
    })

@pytest.fixture
def config_kghm_with_ml():
    return SAPPHIREConfig.from_dict({
        'organization': 'kghm',
        'features': {'run_ml_models': True}
    })
```

---

## Success Criteria

### Must Have
- [ ] All modules use consistent configuration loading
- [ ] Feature flags work independently of organization
- [ ] Secrets remain out of version control
- [ ] Clear error messages for misconfiguration
- [ ] Backward compatibility during migration

### Should Have
- [ ] Startup validation with helpful messages
- [ ] Configuration documentation auto-generated from schema
- [ ] Test fixtures for all configuration scenarios

### Nice to Have
- [ ] `--show-config` CLI command
- [ ] Configuration UI in dashboard
- [ ] Pydantic schema with JSON Schema export

---

## Related Documents

- `ieasyhydro_hf_migration_plan.md` - iEasyHydro HF migration
- `deployment_improvement_planning.md` - Deployment infrastructure
- `observations.md` - Configuration-related observations

---

## Appendix: Current Environment Variables Audit

### Organization & Features
- `ieasyhydroforecast_organization` - demo/kghm/tjhm
- `ieasyhydroforecast_run_ML_models` - True/False
- `ieasyhydroforecast_run_CM_models` - True/False

### Database Credentials
- `IEASYHYDRO_HOST` - Legacy iEH (to deprecate)
- `IEASYHYDRO_USERNAME` - Legacy iEH
- `IEASYHYDRO_PASSWORD` - Legacy iEH
- `ORGANIZATION_ID` - Legacy iEH
- `IEASYHYDROHF_HOST` - iEH HF (target)
- `IEASYHYDROHF_USERNAME` - iEH HF
- `IEASYHYDROHF_PASSWORD` - iEH HF

### Paths (~20+ variables)
- `ieasyhydroforecast_data_root_dir`
- `ieasyforecast_configuration_path`
- `ieasyforecast_intermediate_data_path`
- ... (many more)

### Files (~15+ variables)
- `ieasyforecast_daily_discharge_file`
- `ieasyforecast_pentad_discharge_file`
- `ieasyforecast_hydrograph_day_file`
- ... (many more)

### API Keys
- `ieasyhydroforecast_API_KEY_GATEAWAY`

### Docker
- `ieasyhydroforecast_backend_docker_image_tag`
- `IN_DOCKER_CONTAINER`

### Notifications
- `SAPPHIRE_PIPELINE_SMTP_PASSWORD`
- `TWILIO_AUTH_TOKEN`

---

*Last updated: 2026-01-30 (added demo_data separation, open questions, testing requirements)*
