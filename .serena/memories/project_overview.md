# SAPPHIRE Forecast Tools - Project Overview

## Purpose

SAPPHIRE Forecast Tools is an open-source operational runoff forecasting toolkit. It provides:

1. **Standalone ML Forecasting Models** — Production-ready machine learning models (TFT, TIDE, TSMixer) for operational runoff forecasting. These can be deployed anywhere in the world with minimal adaptation.

2. **Full Operational System** — A complete forecasting platform with dashboards, automated pipelines, and bulletin generation. Originally co-developed with Kyrgyz Hydromet and tailored for workflows of former Soviet Union countries (pentadal/decadal forecasts, Russian language support).

### Global Applicability

The **machine learning forecasting models** are designed for worldwide deployment:
- Pre-trained models learn patterns applicable across different river systems
- Flexible input: works with any daily discharge data source
- Weather forcing from global datasets (ECMWF IFS HRES)
- Docker-containerized for easy deployment on any infrastructure

The **full operational system** has region-specific features that may need adaptation:
- Bulletin templates designed for CIS hydromet reporting formats
- Russian language interface for configuration dashboard
- Pentadal (5-day) and decadal (10-day) forecast periods
- Integration with iEasyHydro database system

### Current Deployments
- **Kyrgyzstan** — Kyrgyz Hydromet (co-development partner)
- **Tajikistan** — Operational deployment
- **Future** — Expansion to other countries planned

## Key Features

- **Multiple forecast models**:
  - Linear regression (pentadal/decadal auto-regressive)
  - Deep learning: TFT, TIDE, TSMixer (daily forecasts)
  - Conceptual: airGR model suite with glacier melt
- **Flexible data sources**: iEasyHydro High Frequency, local Excel files, or custom integrations
- **Forecast dashboard**: Interactive web interface (Python Panel)
- **Workflow orchestration**: Luigi-based automated pipelines
- **Docker-containerized**: Easy deployment with GitHub Actions CI/CD
- **Ensemble forecasting**: Combines multiple models for robust predictions

## Tech Stack

- **Languages**: Python (≥3.11, migrating to 3.12), R (configuration dashboard only)
- **Package Management**: 
  - Modern: uv + pyproject.toml (preferred for new modules)
  - Legacy: pip + requirements.txt / conda
- **Build System**: Hatchling
- **Web Frameworks**: Panel (Python dashboard), Shiny (R configuration dashboard)
- **Workflow Orchestration**: Luigi
- **Containerization**: Docker
- **CI/CD**: GitHub Actions
- **Testing**: pytest, unittest
- **External Dependencies**: ieasyhydro-python-sdk, ieasyreports, sapphire-dg-client

## Project Structure

```
SAPPHIRE_forecast_tools/
├── apps/                          # Main software components
│   ├── iEasyHydroForecast/       # Core forecasting library (Python package)
│   ├── machine_learning/          # ML models (TFT, TIDE, TSMixer) ← globally applicable
│   ├── linear_regression/         # Linear regression forecasting
│   ├── conceptual_model/          # Rainfall-runoff models (airGR)
│   ├── preprocessing_runoff/      # Runoff data preprocessing
│   ├── preprocessing_gateway/     # Gridded weather data preprocessing
│   ├── postprocessing_forecasts/  # Post-processing and ensemble
│   ├── forecast_dashboard/        # Visualization dashboard (Panel)
│   ├── configuration_dashboard/   # Configuration dashboard (R Shiny)
│   ├── reset_forecast_run_date/   # Manual re-run utility
│   ├── pipeline/                  # Luigi orchestration
│   ├── config/                    # Configuration files
│   ├── internal_data/             # Runtime data storage
│   └── backend/                   # ⚠️ DEPRECATED - see note below
├── data/                          # Input data (daily runoff, GIS, templates)
├── doc/                           # Documentation
│   └── plans/                     # Planning docs and issue tracking
│       ├── issues/                # Detailed issue implementation plans (gi_*.md)
│       └── module_issues.md       # Issue index
├── .github/workflows/             # CI/CD pipelines
└── bin/                           # Shell scripts
```

## Module Categories

### Globally Applicable (minimal adaptation needed)
- `machine_learning/` — Pre-trained ML models for any region
- `preprocessing_gateway/` — Weather data from global sources (ECMWF)
- `iEasyHydroForecast/` — Core library functions
- `postprocessing_forecasts/` — Ensemble and output formatting

### Region-Specific (may need adaptation)
- `linear_regression/` — Pentadal/decadal format tied to CIS workflows
- `configuration_dashboard/` — Russian language, CIS station formats
- `forecast_dashboard/` — Bulletin templates for CIS reporting
- `conceptual_model/` — Site-specific calibration required

## Deprecated Modules

### `backend/` — DEPRECATED

The `backend/` module is deprecated and should not be used for new development. Its functionality has been split into dedicated modules:

| Old (backend) | New Module | Status |
|---------------|------------|--------|
| Data preprocessing | `preprocessing_runoff/` | ✅ Active |
| Linear regression | `linear_regression/` | ✅ Active |
| Output generation | `postprocessing_forecasts/` | ✅ Active |
| Forecasting logic | `iEasyHydroForecast/` | ✅ Active |

**What remains in `backend/`:**
- `tests/testspecial_*.ipynb` and `tests/testspecial_*.py` — Utility notebooks and scripts for visualization, debugging, and data exploration. These are kept for developer convenience but are not part of the production pipeline.

**Do not use:**
- `backend/src/` — Deprecated code
- `backend/forecast_script.py` — Use `linear_regression/` instead
- `backend/Dockerfile` — Not maintained

## Mid-Term Development Plans (SAPPHIRE v2)

### Architecture Changes
- **Database Backend** — Replace CSV file-based data storage with a proper database
- **API Layer** — Introduce an API for data access and integration
- **Multi-User Access** — Enable multiple users to access the dashboard simultaneously

### Extended Forecast Horizons
- **Long-Term Forecasts** — Monthly, quarterly, and seasonal forecast capabilities
- **Sub-Daily Forecasts** — Higher temporal resolution forecasting for flood warning applications

### Current Version (v1)
The current version uses CSV files for data storage and supports pentadal (5-day) and decadal (10-day) forecast horizons with single-user dashboard access.

## Main Branches

- `main`: Production branch for deployments
- `implementation_planning`: Feature development and planning
- `local`: Deployed versions at partner organizations (may lag behind main)
- `maxat-sapphire-2`: Development for SAPPHIRE v2 architecture changes
