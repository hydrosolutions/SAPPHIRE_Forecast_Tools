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

#### Step 1.1: Create a Universal Project Introduction
- [ ] Rewrite opening paragraphs to position as a **versatile operational forecasting toolkit**
- [ ] Move Central Asia/SAPPHIRE project specifics to a "Project Origins" or "History" section
- [ ] Add clear value proposition: what problems does this solve?
- [ ] Mention versatility for different hydromets (Central Asia, Nepal, etc.)

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

---

### Phase 6: Documentation Infrastructure

#### Step 6.1: Documentation Site (Optional)
- [ ] Consider setting up GitHub Pages or ReadTheDocs
- [ ] Organize documentation for web navigation
- [ ] Add search functionality

#### Step 6.2: Automated Documentation
- [ ] Add docstring extraction for Python code
- [ ] Generate API reference automatically
- [ ] Set up documentation CI checks

---

## Priority Order

| Priority | Task | Effort | Impact |
|----------|------|--------|--------|
| 1 | Step 1.1 - Universal introduction | Low | High |
| 2 | Step 1.4 - Reorganize README structure | Medium | High |
| 3 | Step 1.2 - Quick Start section | Low | High |
| 4 | Step 2.1 - Regional adaptation guide | Medium | High |
| 5 | Step 3.1 - Architecture documentation | Medium | Medium |
| 6 | Step 1.3 - Visual overview/diagrams | Medium | Medium |
| 7 | Step 5.1 - Contributing guide | Low | Medium |
| 8 | Step 4.1 - User guide improvements | Medium | Medium |
| 9 | Step 3.3 - Configuration reference | High | Medium |
| 10 | Step 5.3 - Changelog/versioning | Low | Low |

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
| 1.1 | Not started | |
| 1.2 | Not started | |
| 1.3 | Not started | |
| 1.4 | Not started | |
| 2.1 | Not started | |
| 2.2 | Not started | |
| ... | ... | |

---

*Document created: 2025-12-01*
*Last updated: 2025-12-01*
