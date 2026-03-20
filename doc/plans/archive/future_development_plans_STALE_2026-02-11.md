# Future Development Plans

This document tracks planned features and improvements that are not currently prioritized but may be implemented when funding or time becomes available.

---

## Overview

These items are **not bugs or blockers** - they represent potential enhancements, new features, or expansions that would add value but are not required for current operations.

---

## Planned Features

### 1. Preprocessing Station Forcing - Spatial Interpolation (Kriging)

**Module:** `apps/preprocessing_station_forcing/`

**Current State:** The module downloads and processes decadal meteorological station data (P, T) from the old iEasyHydro database (ieasyhydro-sdk). Core pipeline is functional but uses deprecated SDK.

**Planned Enhancements:**
1. **Migrate to iEasyHydro HF** - Update from `ieasyhydro-sdk` to `ieasyhydro-hf-sdk` for database access
2. **Add kriging (spatial interpolation)** - Create gridded forcing data from point observations

**Blocked By:**
- Migration to iEasyHydro HF (prerequisite for kriging)
- Norm data not yet available in iEasyHydro HF database
- Regime data not yet available

**TODOs in Code:**
```python
# preprocessing_station_forcing.py lines 94-108
# TODO: Migrate from ieasyhydro-sdk to ieasyhydro-hf-sdk
# TODO: Implement norm data processing once available in iEasyHydro HF
# TODO: Calculate percentage of norm data
# TODO: Implement Kriging code for spatial interpolation
# TODO: Write results from Kriging to raster format
```

**Estimated Effort:** Medium (2-4 weeks)

**Prerequisites:**
- iEasyHydro HF schema update for norm/regime data
- Geospatial dependencies (rasterio, etc.)

**Last Activity:** May 2024 (first iteration complete), October 2025 (dependency update)

---

### 2. Integration of monthly, quarterly, and seasonal hydrological forecasting
Currently being tested on laptops. Module machine_learning_monthly. 

### 3. Development and integration of sub-daily forecasting capabilities
Independent of iEasyHydro HF. Module yet to be designed and developed. Will get operational data through new API client (API details not yet known).  

### 4. Sapphire 2.0 - Next Generation Hydrological Forecasting System
Coupling database services with the modules and an API client for easier access and management.
Currently being developed in branch maxat_sapphire_2. Ses also doc/plans/sapphire_v2_planning.md for more details.


---



## How to Use This Document

1. **Adding new items:** When you identify a feature that's "nice to have" but not urgent, add it here instead of creating issues or TODOs that might be forgotten.

2. **When funding arrives:** Review this document to identify which features align with funding objectives.

3. **Removing items:** When a feature is implemented, move it to "Completed" section below or delete if no longer relevant.

4. **Linking to code:** Always reference the specific files/lines where TODOs exist in the codebase.

---

## Completed Features

*(Move items here when implemented)*

---

*Document created: 2025-12-05*
*Last updated: 2025-12-05*
