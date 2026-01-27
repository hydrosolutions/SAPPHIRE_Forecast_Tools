# Documentation Improvement Plan

## Overview

This plan outlines the steps to transform the SAPPHIRE Forecast Tools documentation from a Central Asia-focused project to a versatile, internationally applicable operational forecasting toolkit while maintaining the existing user base.

---

## Current State Assessment

### Strengths
- Comprehensive technical documentation exists in `/doc/`
- Bilingual support (English/Russian) for Central Asian users
- Good folder structure documentation in README
- Detailed model descriptions with collapsible sections

### Areas for Improvement
- README is heavily Central Asia/Kyrgyz-focused
- No clear "Getting Started" quick-start guide
- Missing architecture diagrams
- No explicit documentation for adapting to new regions (e.g., Nepal)
- Model documentation mixed with project overview
- No contribution guidelines beyond basic PR instructions
- Missing versioning/changelog information

---

## Implementation Steps

### Phase 1: README Restructuring

#### Step 1.1: Create a Universal Project Introduction ✅ COMPLETED (2025-12-01)
- [x] Rewrite opening paragraphs to position as a **versatile operational forecasting toolkit**
- [x] Move Central Asia/SAPPHIRE project specifics to a "Project Origins" section
- [x] Add clear value proposition with tagline and two-tier explanation (Full operational system vs. Standalone forecast models)
- [x] Add "Active development" badge to replace "WORK IN PROGRESS"
- [x] Add "Key Features" section with 7 bullet points covering models, data sources, dashboard, Luigi orchestration, deployments, Docker/GitHub Actions, and ensemble forecasting

#### Step 1.2: Add Quick Start Section
- [ ] Create "Quick Start" section near top of README
- [ ] Include 3-5 steps to get a demo running
- [ ] Link to detailed deployment guide for production setup
- [ ] Add estimated time to get started

#### Step 1.3: Improve Visual Overview
- [ ] Add architecture diagram showing component relationships
- [ ] Create a simple flowchart of the forecasting pipeline
- [ ] Consider adding screenshots of dashboards

#### Step 1.4: Reorganize Content Structure
Proposed new README structure:
```
1. Project Title & Badges
2. One-paragraph Description (universal)
3. Key Features (bullet list)
4. Quick Start
5. Available Forecast Models (collapsible, keep current content)
6. Architecture Overview (new - with diagram)
7. Documentation Links (Configuration, Deployment, Development, User Guide)
8. Data Requirements
9. Regional Implementations
   - Central Asia (SAPPHIRE project)
   - Nepal (upcoming)
   - How to adapt for your region
10. Collaboration & Contributing
11. Funding & Acknowledgments
12. License
```

---

### Phase 2: Create Regional Adaptation Guide

#### Step 2.1: New Document - `doc/regional_adaptation.md`
- [ ] Document steps to adapt the tools for a new region
- [ ] List configuration files that need modification
- [ ] Explain data format requirements
- [ ] Provide checklist for new deployments

#### Step 2.2: Create Regional Examples
- [ ] Document Central Asia implementation as reference
- [ ] Create placeholder for Nepal implementation
- [ ] Include region-specific considerations (data sources, languages, etc.)

---

### Phase 3: Technical Documentation Improvements

#### Step 3.1: Architecture Documentation
- [ ] Create `doc/architecture.md` with system overview
- [ ] Document component interactions
- [ ] Explain data flow through the pipeline
- [ ] Add diagrams (Mermaid or images)

#### Step 3.2: API/Module Documentation
- [ ] Ensure each `/apps/` module has consistent README
- [ ] Document public interfaces for `iEasyHydroForecast` library
- [ ] Add docstrings to key functions (if missing)

#### Step 3.3: Configuration Reference
- [ ] Create comprehensive configuration reference
- [ ] Document all environment variables
- [ ] Explain each JSON config file's purpose and schema

#### Step 3.4: Deployment Infrastructure Requirements
- [ ] Document SSH tunnel requirements for iEasyHydro HF connectivity
  - If the forecast tools do not run on the same network as the iEasyHydro High Frequency software, an SSH tunnel must be established and kept open
  - Document autossh + systemd setup for persistent SSH tunnels
  - Include ssh-keyscan, key-based authentication setup, and systemd service configuration
  - Reference: conversation notes on robust SSH tunnel automation (2025-12-17)
- [ ] Document network topology options (same network vs. remote deployment)

#### Step 3.5: Server Resource Requirements
- [ ] Document minimum server requirements for different deployment scenarios:
  - Minimal (2 CPU, 4GB RAM): preprocessing and linear regression only
  - Standard (4 CPU, 8GB RAM): full pipeline including ML inference
  - ML training (8+ CPU, 16GB RAM): model retraining workloads
- [ ] Add guidance on monitoring resource usage during forecast runs
- [ ] Document memory requirements for different numbers of forecast stations

---

### Phase 4: User-Facing Documentation

#### Step 4.1: Improve User Guide
- [ ] Review and update `doc/user_guide.md`
- [ ] Add more screenshots and examples
- [ ] Create troubleshooting section

#### Step 4.2: Dashboard Documentation
- [ ] Update `doc/dashboard.md` with current features
- [ ] Add screenshots of key screens
- [ ] Document common workflows

#### Step 4.3: Localization Documentation
- [ ] Document how to add new languages
- [ ] Explain locale file structure
- [ ] Create template for new translations

---

### Phase 5: Developer Documentation

#### Step 5.1: Contributing Guide
- [ ] Create `CONTRIBUTING.md` in project root
- [ ] Document code style guidelines
- [ ] Explain PR process in detail
- [ ] Add development environment setup

#### Step 5.2: Testing Documentation
- [ ] Document testing approach
- [ ] Explain how to run tests locally
- [ ] Add CI/CD pipeline documentation

#### Step 5.3: Release Process
- [ ] Create `CHANGELOG.md` for version history
- [ ] Document versioning strategy
- [ ] Add release checklist

#### Step 5.3b: Security & Supply Chain Documentation

**Purpose:** Hydrometeorological services are critical infrastructure. Organizations deploying SAPPHIRE need confidence that the software is secure, professionally maintained, and safe to run on their systems.

**Current Security Measures (implemented):**
- ✅ SLSA Provenance attestation on all py312 Docker images
- ✅ Software Bill of Materials (SBOM) generated for all py312 images
- ✅ Cosign signing for py311 Docker images
- ✅ GitHub Actions CI/CD with no manual artifact handling
- ✅ Dependabot enabled for security vulnerability alerts
- ✅ Scheduled security rebuild workflow (quarterly base image updates)
- ✅ Non-root user in Docker containers (`appuser`)
- ✅ Minimal base images (slim-bookworm)

**Documentation Tasks:**

- [ ] **Create `doc/security.md`** covering:
  - Security philosophy and commitment statement
  - Supply chain security measures (SLSA, SBOM, signing)
  - How to verify image signatures and attestations
  - Dependency management approach
  - Vulnerability disclosure process
  - Security update policy

- [ ] **Create `doc/for-hydromets.md`** (or section in README) covering:
  - Why hydromets can trust this software
  - Professional development practices used
  - How organizations can audit the codebase
  - Data privacy considerations (no telemetry, local data processing)
  - Network security considerations (iEasyHydro HF connectivity)
  - Deployment security checklist

- [ ] **Add security badge to README**
  - Link to security.md
  - Consider OpenSSF Scorecard integration

**Verification Commands for Users:**
```bash
# Verify SLSA provenance attestation
docker buildx imagetools inspect hydrosolutions/sapphire-<module>:py312 --format '{{json .Provenance}}'

# Verify SBOM
docker buildx imagetools inspect hydrosolutions/sapphire-<module>:py312 --format '{{json .SBOM}}'

# Verify Cosign signature (py311)
cosign verify --key cosign.pub hydrosolutions/sapphire-pythonbaseimage:latest
```

**Future Considerations:**
- [ ] Consider creating a `code-quality` skill for Claude Code to enforce security best practices
- [ ] OpenSSF Best Practices badge
- [ ] Regular security audits documentation

#### Step 5.4: Maintenance Documentation
- [x] Create `doc/maintenance/docker-security-maintenance.md` ✅ (2025-12-01)
- [x] Add security disclaimer and liability section ✅ (2025-12-01)
- [x] Add template response for security inquiries ✅ (2025-12-01)
- [ ] Document update procedures for deployed systems
- [ ] Create runbook for common operational tasks
- [ ] Document backup and recovery procedures

#### Step 5.5: Development Environment Documentation
- [ ] Document macOS Docker development workflow
  - Using `host.docker.internal` for SSH tunnels to iEasyHydro HF
  - Server configuration requirements for Docker development
  - Reference: `doc/plans/issues/gi_draft_linreg_bugfix.md` (macOS Docker Compatibility section)
- [ ] Document local Docker testing procedures for each module
- [ ] Create troubleshooting guide for common development issues
- [x] Document MCP server setup for Claude Code ✅ (2025-12-05)

#### Step 5.6: Server Testing Workflow Documentation
- [x] Document server testing procedure ✅ (2025-12-06)
  - Added to `implementation_planning/uv_migration_plan.md` under "Server Testing Procedure" section

#### Step 5.7: UV Migration Documentation
- [x] Create temporary `doc/uv-migration-guide.md` ✅ (2025-12-17)
  - Quick comparison table (conda/pip vs uv)
  - uv installation instructions
  - Module-specific run examples for all Python modules
  - Docker image tag strategy (`:latest`, `:py312`, `:py311`)
  - Troubleshooting section
- [ ] After successful server testing: Update `doc/development.md` to replace conda/pip with uv
- [ ] After Phase 6 complete: Remove `doc/uv-migration-guide.md` (content merged into development.md)

#### Step 5.8: Server Testing Guide (standalone)
- [ ] Create standalone `doc/development/server-testing.md` guide covering:
  - How to temporarily deploy from feature branches
  - How to configure server to use different image tags (`:py312` vs `:latest`)
  - End-to-end testing checklist
  - Rollback procedures
  - Troubleshooting common issues (permissions, connections, etc.)
- [ ] Add cross-reference from CONTRIBUTING.md (when created)

##### Server Testing Workflow Summary

When testing new Docker images on the AWS server:

1. **Temporarily modify CI/CD** - Change `.github/workflows/deploy_main.yml` branch trigger from `main` to feature branch
2. **Push to trigger build** - CI/CD builds and pushes `:py312` tagged images to DockerHub
3. **Configure server** - Update server `.env` to use `:py312` tag instead of `:latest`
4. **Test end-to-end** - Run complete forecast cycle and verify all modules work
5. **Revert CI/CD** - Change branch trigger back to `main` after testing

Full procedure documented in: `doc/plans/uv_migration_plan.md` → "Server Testing Procedure"

##### MCP Server Setup for Claude Code (VS Code)

These instructions help you set up **Context7** and **Serena** MCP servers to enhance Claude Code's capabilities in VS Code.

**Prerequisites:**
- Claude Code extension installed in VS Code
- `uv` installed (for Serena): `curl -LsSf https://astral.sh/uv/install.sh | sh`

**1. Context7 - Library Documentation Lookup**

Context7 provides up-to-date documentation for libraries directly to Claude.

```bash
# Installation (one-time, in any terminal):
claude mcp add context7 --transport http https://mcp.context7.com/mcp
```

Usage examples:
- "Use context7 to find pandas DataFrame docs"
- "Look up the luigi Task API with context7"

**2. Serena - Semantic Code Analysis**

Serena provides intelligent code understanding, symbol navigation, and semantic search.

```bash
# Installation (run from project root directory):
cd /path/to/SAPPHIRE_forecast_tools
claude mcp add serena -- uvx --from git+https://github.com/oraios/serena serena start-mcp-server --context ide-assistant --project "$(pwd)"
```

Project configuration: A `.serena/project.yml` file already exists in the repository with:
- Python language server enabled
- UTF-8 encoding
- Gitignore integration
- Project memories for code conventions and suggested commands

Serena automatically enhances Claude's ability to:
- Navigate symbols and find references
- Understand code structure without reading entire files
- Search for patterns across the codebase
- Remember project-specific conventions

**Verification:**

```bash
claude mcp list
```

Expected output:
```
context7: https://mcp.context7.com/mcp (http)
serena: uvx --from git+https://github.com/oraios/serena serena start-mcp-server ...
```

**Removal (if needed):**

```bash
claude mcp remove context7
claude mcp remove serena
```

**Configuration storage:**
- MCP settings are stored in `~/settings.local.json`
- Project-specific Serena config is in `.serena/project.yml` (already in repo)
- Serena memories are in `.serena/memories/` (project-specific, gitignored)

**Tips:**
- Context7 is useful when you need to look up external library APIs
- Serena shines when navigating complex code - it reads symbols intelligently without loading entire files
- Both tools work together: Serena for internal code, Context7 for external libraries

---

### Phase 6: Documentation Infrastructure

#### Step 6.1: Documentation Site (MkDocs + GitHub Pages)
- [x] Set up MkDocs with Material theme ✅ (2025-12-01)
- [x] Create `mkdocs.yml` configuration ✅ (2025-12-01)
- [x] Create `doc/index.md` homepage ✅ (2025-12-01)
- [x] Create `.github/workflows/deploy_docs.yml` ✅ (2025-12-01)
- [ ] Enable GitHub Pages in repository settings:
  1. Go to https://github.com/hydrosolutions/SAPPHIRE_forecast_tools/settings/pages
  2. Under "Build and deployment" → Source: select **GitHub Actions**
  3. Save
- [ ] Test deployment with manual workflow trigger:
  1. Go to https://github.com/hydrosolutions/SAPPHIRE_forecast_tools/actions
  2. Select "Deploy Documentation" workflow
  3. Click "Run workflow" → select branch → "Run workflow"
  4. Wait for build to complete
  5. Visit https://hydrosolutions.github.io/SAPPHIRE_forecast_tools/
- [ ] (Optional) Enable auto-deploy: Uncomment the `push` trigger in `.github/workflows/deploy_docs.yml`
- [ ] Add documentation badge to README

#### Step 6.2: Automated Documentation
- [ ] Add docstring extraction for Python code
- [ ] Generate API reference automatically
- [ ] Set up documentation CI checks

---

### Phase 7: Module-Level Documentation

#### Documentation Location Strategy: Hybrid Approach

Each module in `/apps/` should have documentation in two places:

1. **Module README.md** (in `apps/<module>/README.md`)
   - Quick overview (1-2 paragraphs)
   - Usage example
   - Link to full documentation
   - Visible when browsing GitHub

2. **Detailed Documentation** (in `doc/modules/<module>.md`)
   - Comprehensive documentation
   - Environment variables
   - Data formats
   - Architecture details
   - Troubleshooting
   - Accessible via MkDocs site

**Rationale:** MkDocs is configured with `docs_dir: doc`, so detailed docs must live in `doc/`. Module READMEs provide quick context for developers browsing the codebase.

#### Step 7.0: Create Module Documentation Structure
- [ ] Create `doc/modules/` directory
- [ ] Update `mkdocs.yml` to include modules section in navigation:
  ```yaml
  nav:
    - Modules:
        - Overview: modules/index.md
        - preprocessing_gateway: modules/preprocessing_gateway.md
        - preprocessing_runoff: modules/preprocessing_runoff.md
        - preprocessing_station_forcing: modules/preprocessing_station_forcing.md
        - linear_regression: modules/linear_regression.md
        - machine_learning: modules/machine_learning.md
        - forecast_dashboard: modules/forecast_dashboard.md
        - pipeline: modules/pipeline.md
  ```
- [ ] Create `doc/modules/index.md` with module overview and links

#### Step 7.1: preprocessing_gateway Module

**Current State Assessment (2025-12-05):**
- ❌ No README.md (pyproject.toml references it but file doesn't exist)
- ⚠️ Partial docstrings - good in Quantile_Mapping_OP.py and dg_utils.py, minimal in extend_era5_reanalysis.py
- ⚠️ 16+ environment variables used but poorly documented
- ❌ No architecture diagram showing script dependencies
- ❌ No test suite (pyproject.toml references tests/ but none exist)
- ⚠️ Incomplete TODOs in doc/configuration.md (lines 140, 161, 195)

**Tasks:**

- [ ] **Create `apps/preprocessing_gateway/README.md`** (concise)
  ```markdown
  # Preprocessing Gateway Module

  Downloads and processes weather data from the SAPPHIRE Data Gateway,
  performs quantile mapping on ERA5/ECMWF data, and prepares forcing data
  for forecast models.

  ## Scripts
  - `Quantile_Mapping_OP.py` - Downloads and bias-corrects operational forecasts
  - `extend_era5_reanalysis.py` - Extends historical reanalysis data
  - `snow_data_operational.py` - Processes operational snow data

  ## Quick Start
  ```bash
  ieasyhydroforecast_env_file_path=/path/to/.env python Quantile_Mapping_OP.py
  ```

  ## Full Documentation
  See [preprocessing_gateway module documentation](../../doc/modules/preprocessing_gateway.md)
  ```

- [ ] **Create `doc/modules/preprocessing_gateway.md`** (comprehensive)
  - Module overview and purpose
  - Architecture diagram (Mermaid) showing script execution order
  - All 16+ environment variables with descriptions:
    - `ieasyhydroforecast_API_KEY_GATEAWAY` (note: typo in var name)
    - `ieasyhydroforecast_HRU_CONTROL_MEMBER`
    - `ieasyhydroforecast_HRU_ENSEMBLE`
    - `ieasyhydroforecast_HRU_SNOW_DATA`
    - `ieasyhydroforecast_OUTPUT_PATH_CM` / `_ENS` / `_DG` / `_REANALYSIS` / `_SNOW`
    - `ieasyhydroforecast_Q_MAP_PARAM_PATH`
    - `ieasyhydroforecast_SNOW_VARS`
    - `ieasyhydroforecast_config_file_data_gateway_name_twins`
    - `ieasyhydroforecast_models_and_scalers_path`
    - `ieasyhydroforecast_reanalysis_START_DATE` / `_END_DATE`
    - `ieasyforecast_intermediate_data_path`
  - Input/output data format specifications:
    - Control member CSV: `['date', 'P/T', 'code']`
    - Ensemble CSV: `['date', 'P/T', 'code', 'ensemble_member']`
    - Quantile mapping parameter files format
  - Data Gateway integration:
    - How to obtain API key access
    - sapphire-dg-client private repo access requirements
  - Quantile mapping explanation (formula: y_fit = a * y_era_5^b)
  - Troubleshooting guide

- [ ] **Complete TODOs in doc/configuration.md**
  - Line ~140: Link to data gateway chapter
  - Line ~161: Document output folder purposes
  - Line ~195: Complete machine learning configuration section

- [ ] **Improve function docstrings**
  - extend_era5_reanalysis.py - add comprehensive docstrings
  - get_era5_reanalysis_data.py - add comprehensive docstrings
  - Document magic numbers (195 days, 5-year batches)

#### Step 7.2: preprocessing_runoff Module
- [x] Assess documentation status (2025-01-09)
- [x] Create `apps/preprocessing_runoff/README.md` (comprehensive - already exists)
- [ ] Create `doc/modules/preprocessing_runoff.md` (comprehensive)
- [x] Document iEasyHydro HF SDK integration (2025-01-09 - added to README)
- [x] Document output formats (runoff_day.csv, hydrograph_day.csv) - already in README
- [x] Document limitations section (2025-01-09 - added to README)

##### iEasyHydro HF Coupling - Known Limitations (2025-01-09)

**Documented in `apps/preprocessing_runoff/README.md` → Limitations section:**

- **Manual stations only** - Forecasts are generated only for stations marked as `site_type: manual` in iEasyHydro HF. This is by design, as manual stations contain quality-controlled data suitable for forecasting. Automatic stations with the same site code are filtered out.

#### Step 7.3: preprocessing_station_forcing Module
- [ ] Assess documentation status
- [ ] Create README.md and doc/modules/ entry

#### Step 7.4: linear_regression Module
- [ ] Assess documentation status
- [ ] Create README.md and doc/modules/ entry
- [ ] Document hindcast mode (`--hindcast` flag)
- [ ] Document output formats

#### Step 7.5: machine_learning Module
- [ ] Assess documentation status
- [ ] Create README.md and doc/modules/ entry
- [ ] Document Darts/PyTorch dependencies
- [ ] Document model training vs inference modes
- **NOTE:** This module is the strategic priority for forecasting development going forward

#### Step 7.5b: conceptual_model Module ⚠️ MAINTENANCE-ONLY

**Status:** This module is being phased out and is in **maintenance-only mode**.

**Background:**
- The conceptual_model module is an R-based hydrological modeling module using the GR family of models
- It depends heavily on upstream GitHub packages that are **no longer actively maintained**:
  - `hydrosolutions/airGR_GM` - custom GR model fork
  - `hydrosolutions/airgrdatassim` - data assimilation for GR models
- These packages are installed from `master` branch without version pinning
- The module triggers security warnings on DockerHub due to older dependencies

**Decision (2025-12-06):**
- We will **phase out support** for this module over time
- Resources will be focused on the **machine_learning module** instead
- Existing customers who rely on this module will continue to be supported in maintenance mode
- No new features will be added to this module
- Security updates to the base image (rocker/tidyverse) will be applied when feasible

**Update (2026-01-27): CI/CD Disabled**
- R dependency builds broken in rocker/tidyverse upstream (urllib update incompatibility)
- CI builds disabled in: `build_test.yml`, `deploy_main.yml`, `scheduled_security_rebuild.yml`
- Existing Docker images frozen at current state
- To re-enable: fix R dependencies, uncomment jobs in workflows

**Documentation Tasks:**
- [x] Add deprecation notice to `apps/conceptual_model/README.md` (2025-12-06)
- [x] Document that upstream dependencies (airGR_GM, airgrdatassim) are unmaintained (2025-12-06)
- [x] Add migration guidance pointing users to machine_learning module (2025-12-06)
- [ ] Note in `doc/modules/conceptual_model.md` (when created) that this is legacy code

#### Step 7.6: forecast_dashboard Module
- [ ] Assess documentation status
- [ ] Create README.md and doc/modules/ entry
- [ ] Document Panel/Bokeh stack
- [ ] Include screenshots

#### Step 7.7: pipeline Module
- [ ] Assess documentation status
- [ ] Create README.md and doc/modules/ entry
- [ ] Document Luigi task dependencies
- [ ] Document Docker orchestration

#### Step 7.8: iEasyHydroForecast Library
- [ ] Review existing documentation
- [ ] Create `doc/modules/iEasyHydroForecast.md`
- [ ] Document public API
- [ ] Add usage examples

---

## Priority Order (Revised 2026-01-07)

Reordered to reflect operational needs. GitHub Pages enablement deferred until core documentation is polished.

| Priority | Task | Effort | Impact | Status |
|----------|------|--------|--------|--------|
| 1 | Step 1.2 - Quick Start section | Low | High | Not started |
| 2 | Configuration.md - Fix TODOs | Low | High | Identified |
| 3 | Step 7.0 - Module docs structure | Low | High | Not started |
| 4 | Step 5.3b - Security docs | Medium | High | Not started |
| 5 | Step 2.1 - Regional adaptation guide | Medium | High | Not started |
| 6 | Step 3.1 - Architecture diagram | Medium | Medium | Not started |
| 7 | Step 7.7 - Pipeline module docs | Medium | Medium | Not started |
| 8 | Step 4.1 - User guide update | Medium | Medium | Not started |
| 9 | Step 5.1 - CONTRIBUTING.md | Low | Medium | Not started |
| 10 | **Step 6.1 - Enable GitHub Pages** | Very Low | High | **After priorities 1-5 complete** |
| 11 | Step 7.1 - preprocessing_gateway docs | Medium | High | Not started |
| 12 | Step 7.2 - preprocessing_runoff docs | Medium | Medium | Not started |
| 13 | Step 7.4 - linear_regression docs | Medium | Medium | Not started |
| 14 | Step 7.5-7.8 - Other module docs | High | Medium | Not started |
| 15 | Step 5.3 - Changelog/versioning | Low | Low | Not started |

**Note:** Step 1.1 (Universal introduction) and Step 1.4 (README restructure) are already completed/in-progress.

---

## Immediate Action Items (< 1 hour each)

These quick wins unblock other work or fix user-facing issues:

- [ ] **Create doc/modules/ directory** — `mkdir doc/modules && touch doc/modules/index.md`
- [ ] **Fix configuration.md line 5** — Update or remove outdated "TODO: UPDATE FIGURE"
- [ ] **Fix configuration.md line ~140** — Add data gateway chapter link
- [ ] **Fix configuration.md line ~161** — Document output folder purposes
- [ ] **Fix configuration.md line ~195** — Complete ML configuration section
- [ ] **Add Quick Start to README** — 5 steps: clone, copy example .env, docker compose up, access dashboard, run first forecast
- [ ] **Update mkdocs.yml** — Add modules section to navigation

---

## Documentation Audit (2026-01-07)

### Current State Summary

| Category | Status | Notes |
|----------|--------|-------|
| Main README | ✅ Improved | Universal intro done, needs Quick Start |
| User Guide | ⚠️ Stale | References deprecated "backend", missing screenshots |
| Deployment Guide | ✅ Decent | Good structure, needs resource limits docs |
| Configuration | ⚠️ Incomplete | Multiple TODOs unfixed |
| MkDocs Site | ⚠️ Setup done | Waiting for content polish before enabling |
| Module Docs | ❌ Missing | `doc/modules/` directory doesn't exist |

### Module README Status

| Module | Has README | Quality |
|--------|------------|---------|
| machine_learning | ✅ | Good - detailed flagging system |
| preprocessing_gateway | ✅ | Minimal |
| preprocessing_runoff | ✅ | Needs review |
| iEasyHydroForecast | ✅ | Needs review |
| pipeline | ❌ | **Missing - critical** |
| linear_regression | ❌ | Missing |
| forecast_dashboard | ❌ | Missing |
| postprocessing_forecasts | ✅ | Needs review |

---

## Notes for Nepal Implementation

When adapting documentation for Nepal:
- Consider adding Nepali language support to dashboards
- Document Nepal-specific data sources
- Note any model parameters that need regional calibration
- Consider monsoon-specific forecasting requirements
- Document any modifications to the ML models for different climate regimes

---

## Success Criteria

The documentation improvement is complete when:
1. A new user can understand the project purpose within 2 minutes of reading the README
2. A hydromet organization can identify the steps to adapt the tools for their region
3. A developer can set up a development environment without asking questions
4. The project history and funders are appropriately acknowledged without dominating the documentation
5. Both existing (Central Asia) and new (Nepal) implementations are supported

---

## Timeline Tracking

| Step | Status | Notes |
|------|--------|-------|
| 1.1 | ✅ Completed | 2025-12-01 - Added universal intro, Key Features, Project Origins, active development badge |
| 1.2 | Not started | Quick Start section - **Priority 1** |
| 1.3 | Not started | Visual overview/diagrams |
| 1.4 | In progress | Partial - new structure partially implemented with Step 1.1 |
| 2.1 | Not started | Regional adaptation guide - **Priority 5** |
| 2.2 | Not started | Regional examples |
| 3.5 | Not started | Server resource requirements - added 2026-01-07 |
| 5.3b | Not started | Security & supply chain documentation - **Priority 4** |
| 5.4 | ✅ Completed | 2025-12-01 - Created docker-security-maintenance.md + security disclaimer + template response |
| 5.5 | ✅ Completed | 2025-12-05 - MCP server setup instructions for Context7 and Serena |
| 5.6 | ✅ Completed | 2025-12-06 - Server testing procedure documented in uv_migration_plan.md |
| 5.7 | ✅ Completed | 2025-12-17 - Created `doc/uv-migration-guide.md` (temporary guide for team) |
| 5.8 | Not started | Standalone server testing guide |
| 6.1 | In progress | 2025-12-01 - MkDocs setup complete, **enable after priorities 1-5** |
| 7.0 | Not started | Module documentation structure - **Priority 3** |
| 7.1 | Not started | 2025-12-05 - Assessment complete, tasks defined |
| 7.2 | Not started | preprocessing_runoff |
| 7.3 | Not started | preprocessing_station_forcing |
| 7.4 | Not started | linear_regression |
| 7.5-7.8 | Not started | Other modules |
| config.md | Identified | 2026-01-07 - TODOs on lines 5, ~140, ~161, ~195 need fixing - **Priority 2** |

---

*Document created: 2025-12-01*
*Last updated: 2026-01-07 - Documentation audit, reprioritized tasks, added immediate action items, added Step 3.5 resource requirements*
