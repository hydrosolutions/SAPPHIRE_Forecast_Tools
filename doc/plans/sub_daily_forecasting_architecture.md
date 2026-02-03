# Sub-Daily Forecasting Architecture

## Status: Planning

## Overview

This document outlines the architecture for sub-daily (high-frequency) forecasting capabilities in SAPPHIRE. Sub-daily forecasts are critical for flood warning applications where lead times of hours rather than days are required.

---

## 1. Business Requirements

### 1.1 Use Cases

- **Flood early warning**: 6-12 hour lead time forecasts for rapid-response catchments
- **Dam operations**: Inflow forecasts at 6-hourly resolution for reservoir management
- **Urban drainage**: High-frequency precipitation-runoff modeling

### 1.2 Target Temporal Resolution

> **TODO**: Define target resolution (hourly? 3-hourly? 6-hourly?)

| Option | Pros | Cons |
|--------|------|------|
| Hourly | Highest resolution for flash floods | 24x computational cost, data availability |
| 3-hourly | Good balance | 8x cost |
| 6-hourly | Matches many NWP outputs | 4x cost, may miss fast events |

### 1.3 Target Forecast Horizons

> **TODO**: Define lead times for sub-daily forecasts

---

## 2. Data Sources

### 2.1 Operational Discharge Data

> **TODO**: Define API client requirements

**Current understanding:**
- Will get operational data through new API client (details not yet known)
- Independent of iEasyHydro HF (which is daily data)

**Questions to resolve:**
- [ ] What is the API endpoint/specification?
- [ ] Authentication mechanism?
- [ ] Data refresh frequency (e.g., data available at 00:15, 06:15, 12:15, 18:15 UTC)?
- [ ] Data format (JSON, CSV, other)?
- [ ] Rate limiting and error handling requirements?

### 2.2 Meteorological Forcing Data

> **TODO**: Define high-frequency forcing data sources

**Potential sources:**
- NWP ensemble forecasts (e.g., ECMWF, GFS) at 6-hourly resolution
- Radar-based nowcasts for short lead times
- Station-based real-time observations

### 2.3 Data Storage Schema

> **TODO**: Define sub-daily data storage approach

**Considerations:**
- Use `datetime` columns (not `date`) for sub-daily timestamps
- Timezone handling (UTC vs local time)
- Aggregation logic (sub-daily to daily for backward compatibility)
- Historical sub-daily data archival

---

## 3. Forecast Models

### 3.1 Model Applicability Assessment

> **TODO**: Evaluate which existing models can work with sub-daily data

| Model | Daily Use | Sub-Daily Applicability | Adaptations Needed |
|-------|-----------|-------------------------|-------------------|
| Linear Regression | Yes | ? | ? |
| Machine Learning (LSTM/Darts) | Yes | ? | ? |
| Conceptual Model (airGR) | Yes | ? | ? |

### 3.2 Lead Time Stratification

> **TODO**: Define how forecasts are stratified by lead time

**Questions:**
- Different models for different lead times (0-6h, 6-12h, 12-24h)?
- Blending of model outputs?
- Skill degradation with lead time?

### 3.3 Retraining Requirements

> **TODO**: Document retraining needs for sub-daily models

---

## 4. Pipeline Infrastructure

### 4.1 Time-Slot Aware Marker Files

**Design from P-002 (implemented but not yet used for sub-daily):**

The `get_marker_filepath()` function already supports time slots for sub-daily runs:

```python
def get_marker_filepath(task_name, date=None, time_slot=None):
    """Generate consistent marker filepath for a given task, date, and optional time slot.

    Args:
        task_name: Name of the task (e.g., 'preprocessing_gateway')
        date: Date for the marker (defaults to today)
        time_slot: Optional time slot for sub-daily tasks (0, 1, 2, 3 for 4x daily)

    Returns:
        Path to marker file, e.g.:
        - Daily: preprocessing_gateway_2026-02-02.marker
        - Sub-daily: preprocessing_gateway_2026-02-02_slot0.marker
    """
```

**Cron scheduling pattern for 4x daily runs:**
```bash
# Sub-daily forecasts (4x daily, each creates time-slot marker)
0 0,6,12,18 * * * bash bin/run_preprocessing_gateway_subdaily.sh /path/to/.env --slot $SLOT
```

### 4.2 Luigi Task Scheduling

**Requirement**: Gateway preprocessing must run **4x daily** (00:00, 06:00, 12:00, 18:00 UTC) for sub-daily forecasts.

**Note**: Fetch and quantile mapping must run together - you cannot fetch without processing.

**Implementation needs:**
- [ ] Create `bin/run_preprocessing_gateway_subdaily.sh` script
- [ ] Pass `--slot` parameter to container
- [ ] Update `get_gateway_dependency()` to accept time_slot from environment
- [ ] Create sub-daily workflow tasks in `pipeline_docker.py`

### 4.3 Container Orchestration

> **TODO**: Design for parallel 4x daily workflows

**Considerations:**
- Luigi worker ID configuration (see `deployment_improvement_planning.md`)
- Container hostname uniqueness causing task ownership conflicts
- Potential solution: Fixed `--worker-id` flags per time slot

---

## 5. Operational/Maintenance Mode Separation

**Design from P-002 (Phase 2 - not yet implemented):**

| Mode | Scripts Run | Purpose | Frequency |
|------|-------------|---------|-----------|
| **Operational** | `Quantile_Mapping_OP.py`, `snow_data_operational.py` | Fetch fresh data + quantile mapping | 1x daily (or 4x for sub-daily) |
| **Maintenance** | `extend_era5_reanalysis.py`, `snow_data_reanalysis.py` | Fill gaps in historical data | 1x daily (morning catch-up) |

**Implementation checklist (from P-002 Phase 2):**
- [ ] Add `get_gateway_mode()` function to preprocessing_gateway module
- [ ] Add `--maintenance` CLI flag support
- [ ] Update Dockerfile with mode-based CMD
- [ ] Update `run_preprocessing_gateway.sh` to pass `GATEWAY_MODE`
- [ ] Add maintenance mode cron entry
- [ ] Test both modes locally
- [ ] Update documentation

---

## 6. Dashboard & Bulletin Integration

### 6.1 Sub-Daily Forecast Visualization

> **TODO**: Design UI/UX for sub-daily forecasts

**Questions:**
- How to display 6-hourly forecasts on the dashboard?
- Time series vs hydrograph format?
- Aggregation options for user display?

### 6.2 Bulletin Format

> **TODO**: Define bulletin output for sub-daily forecasts

### 6.3 Skill Metrics

> **TODO**: Define performance metrics for sub-daily forecasts

**Considerations:**
- NSE/KGE at sub-daily resolution
- Lead-time dependent skill scores
- Probabilistic skill for ensemble forecasts

---

## 7. API Architecture (Future - Q2+)

**Long-term goal from P-002 Phase 3:**

Replace file-based data exchange with REST API returning DataFrames.

```
+-------------------------------------------------------------+
|                   Gateway API Service                        |
|  FastAPI + In-Memory Cache                                   |
|  Port: 8001                                                  |
|                                                              |
|  Endpoints:                                                  |
|  GET /api/v1/forecast/{date}?type=control                   |
|  GET /api/v1/forecast/{date}?type=ensemble&member=1         |
|  GET /api/v1/health                                          |
+-------------------------------------------------------------+
```

**Benefits:**
- Eliminates marker file accumulation (P-001)
- Native time-slot support
- No shared volume required
- Automatic cache expiry

---

## 8. Implementation Roadmap

### Phase 1: Infrastructure Preparation
- [ ] Implement P-002 Phase 2 (mode separation)
- [ ] Create sub-daily shell scripts
- [ ] Test time-slot markers end-to-end

### Phase 2: Data Pipeline
- [ ] Define and implement API client for operational data
- [ ] Set up sub-daily forcing data pipeline
- [ ] Test data retrieval at 6-hourly intervals

### Phase 3: Model Adaptation
- [ ] Evaluate model applicability
- [ ] Adapt/retrain models for sub-daily resolution
- [ ] Validate forecast skill

### Phase 4: Dashboard Integration
- [ ] Design sub-daily visualization
- [ ] Implement bulletin format
- [ ] Deploy and monitor

---

## 9. Dependencies & Blockers

| Dependency | Status | Notes |
|------------|--------|-------|
| API client specification | Blocked | Waiting for API details |
| P-002 Phase 2 (mode separation) | Not started | Prerequisite for sub-daily |
| Maxat's dashboard refactoring | In progress | SEC-005 bokeh update blocked |
| Sub-daily forcing data availability | Unknown | Need to identify sources |

---

## 10. References

- `doc/plans/future_development_plans.md` - High-level feature roadmap
- `doc/plans/archive/gi_P-002_gateway_double_run_RESOLVED_2026-02-03.md` - Time-slot marker design
- `doc/plans/sapphire_v2_planning.md` - SAPPHIRE 2.0 architecture (includes sub-daily mention)
- `doc/plans/deployment_improvement_planning.md` - Luigi worker ID configuration

---

*Created: 2026-02-03*
*Status: Planning - skeleton document, details to be filled in dedicated planning session*
