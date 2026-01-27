# SAPPHIRE v2 Planning

## Overview

SAPPHIRE v2 is the next major version of the forecast tools, introducing architectural improvements and expanded capabilities.

## Key Changes from v1

### Architecture
- **Database Backend** — Replace CSV file-based data storage with a proper database
- **API Layer** — Introduce an API for data access and integration
- **Multi-User Access** — Enable multiple users to access the dashboard simultaneously

### Extended Forecast Horizons
- **Long-Term Forecasts** — Monthly, quarterly, and seasonal forecast capabilities
- **Sub-Daily Forecasts** — Higher temporal resolution forecasting for flood warning applications

## Demo Version

The working demo will be implemented in SAPPHIRE v2.

### Demo Scope
- 2 Swiss demo stations: Sihl (12176) and Rosegbach (12256)
- Linear regression module only
- Forecast dashboard (pentadal forecasts)
- Automated daily updates on testing server
- Uses publicly available BAFU data via [hydro_data_scraper](https://github.com/hydrosolutions/hydro_data_scraper)

### Demo Milestones

| Milestone | Goal | Key Deliverable |
|-----------|------|-----------------|
| 0.1 | Data Pipeline | Script to fetch BAFU data and convert to daily values |
| 0.2 | Configuration | Working demo config with no external dependencies |
| 0.3 | Pipeline | Successful end-to-end forecast run |
| 0.4 | Dashboard | Visible forecasts for demo stations |
| 0.5 | Automation | Daily updates on testing server |
| 0.6 | Documentation | Quick Start guide |

### Data Pipeline Details

**Data Source:** BAFU Hydrodaten (Swiss Federal Office for the Environment)

| Station | SAPPHIRE Code | BAFU Code | URL |
|---------|---------------|-----------|-----|
| Sihl - Zürich, Sihlhölzli | 12176 | 2176 | [Link](https://www.hydrodaten.admin.ch/en/seen-und-fluesse/stations-and-data/2176) |
| Rosegbach - Pontresina | 12256 | 2256 | [Link](https://www.hydrodaten.admin.ch/en/seen-und-fluesse/stations-and-data/2256) |

**Processing Required:**
- hydro_data_scraper returns 10-minute resolution data
- Aggregate to daily mean values for forecast tools
- Output format must match `runoff_day.csv` expected by linear_regression

### Out of Scope for Demo
- Machine learning models (no trained models for demo sites)
- Conceptual rainfall-runoff models
- iEasyHydro HF integration
- Data Gateway integration
- Configuration dashboard

---

## Post-Merge Checklist (maxat_sapphire_2)

After merging `maxat_sapphire_2` into `main`, complete these tasks:

- [ ] **SEC-005: Verify bokeh update** — The `maxat_sapphire_2` branch includes the fix for
  `FuncTickFormatter` (removed in bokeh 3.5+). After merge:
  1. Verify `apps/forecast_dashboard/pyproject.toml` has `bokeh>=3.8.2` (or similar)
  2. Run `uv lock` in forecast_dashboard to update lock file
  3. Run dashboard tests: `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard`
  4. Test dashboard manually in browser
  5. Update `doc/plans/security_updates.md` to mark SEC-005 as completed

---

*Created: 2026-01-07*
*Updated: 2026-01-27*
