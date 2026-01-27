# GI PR-003: Swiss Data Source Integration & Module Refactoring

## Problem Statement

The preprocessing_runoff module needs to support Swiss demo data as an additional data source. This provides an opportunity to refactor `src/src.py` (currently ~4037 lines, guideline: 300) into a cleaner architecture that supports multiple data sources.

## Objectives

1. Add Swiss data source integration to preprocessing_runoff
2. Refactor `src/src.py` into logical modules during the integration
3. Establish a pattern for adding future data sources (Nepal, others)

## Current State

**File sizes** (January 2025):
- `src/src.py`: ~4037 lines
- `preprocessing_runoff.py`: ~500 lines
- `src/config.py`: ~150 lines

**Current data source switching**:
```python
# In preprocessing_runoff.py
if os.getenv('ieasyhydroforecast_connect_to_iEH') == 'True':
    # Legacy iEasyHydro path (TO BE REMOVED)
    runoff_data = src.get_runoff_data_for_sites(ieh_sdk, ...)
else:
    # iEasyHydro HF path (default)
    runoff_data = src.get_runoff_data_for_sites_HF(ieh_hf_sdk, ...)
```

**Note**: Legacy iEasyHydro support (`ieasyhydroforecast_connect_to_iEH`) will be **removed**, not refactored. Only iEasyHydro HF will be supported going forward.

## Implementation Plan

### Phase 1: Swiss Data Source Requirements

**Goal**: Understand Swiss data source API/format.

**Questions to answer**:
- [ ] What API/format does Swiss demo data use?
- [ ] Authentication requirements?
- [ ] Data format (JSON, CSV, other)?
- [ ] Site identification (codes, IDs)?
- [ ] Temporal resolution (daily, sub-daily)?
- [ ] Historical data availability?

**Deliverable**: Requirements document for Swiss data integration

---

### Phase 2: Module Extraction

**Goal**: Extract logical modules from `src/src.py` before adding new data source.

**Proposed structure**:
```
apps/preprocessing_runoff/src/
├── __init__.py           # Public API exports
├── config.py             # Configuration (existing)
├── api/                  # Data source clients
│   ├── __init__.py
│   ├── base.py           # Abstract base class for data sources
│   ├── ieasyhydro_hf.py  # iEasyHydro HF implementation
│   └── swiss.py          # Swiss data source (new)
├── processing/           # Data transformations
│   ├── __init__.py
│   ├── filtering.py      # Outlier filtering
│   ├── hydrograph.py     # Hydrograph conversion
│   └── validation.py     # Data validation
├── io/                   # File I/O
│   ├── __init__.py
│   ├── csv.py            # CSV read/write
│   ├── excel.py          # Excel read
│   └── cache.py          # Site caching
└── src.py                # Backward compatibility (re-exports)
```

**Note**: Legacy iEasyHydro code will be removed during refactoring (not extracted).

**Extraction order** (minimize risk):
1. **Remove** legacy iEasyHydro code (`ieasyhydroforecast_connect_to_iEH` paths)
2. Extract `io/cache.py` - site caching functions (isolated, well-tested)
3. Extract `io/csv.py` - CSV read/write functions
4. Extract `processing/filtering.py` - outlier filtering
5. Extract `processing/hydrograph.py` - hydrograph conversion
6. Extract `api/ieasyhydro_hf.py` - HF data fetching
7. Add `api/swiss.py` - Swiss data source

**Backward compatibility**: Keep `src.py` as a facade that re-exports all public functions.

---

### Phase 3: Swiss Data Source Implementation

**Goal**: Implement Swiss data source following the new architecture.

**Tasks**:
- [ ] Create `api/base.py` with abstract `DataSource` class
- [ ] Create `api/swiss.py` implementing the interface
- [ ] Add environment variable: `ieasyhydroforecast_data_source` with values: `ieasyhydro_hf`, `swiss`
- [ ] Update `preprocessing_runoff.py` to use data source factory
- [ ] Remove legacy iEasyHydro code paths and `ieasyhydroforecast_connect_to_iEH` variable
- [ ] Add Swiss-specific configuration to `config.yaml`
- [ ] Write tests for Swiss data source

**Data source interface**:
```python
# api/base.py
from abc import ABC, abstractmethod
from datetime import datetime
import pandas as pd

class DataSource(ABC):
    """Abstract base class for runoff data sources."""

    @abstractmethod
    def get_forecast_sites(self) -> tuple[list, list, list]:
        """Get sites enabled for forecasting.

        Returns:
            Tuple of (site_objects, site_codes, site_ids)
        """
        pass

    @abstractmethod
    def get_runoff_data(
        self,
        site_codes: list,
        site_ids: list,
        start_datetime: datetime,  # Use datetime, not date (for sub-daily data)
        end_datetime: datetime,
    ) -> pd.DataFrame:
        """Fetch runoff data for given sites and datetime range.

        Returns:
            DataFrame with columns: code, datetime, discharge
            Note: datetime column supports sub-daily resolution for high-frequency data
        """
        pass

    @property
    @abstractmethod
    def temporal_resolution(self) -> str:
        """Return the temporal resolution of this data source.

        Returns:
            One of: 'sub-daily', 'daily', 'pentad', 'decad', 'monthly'
        """
        pass
```

**Design considerations for future extensibility**:
- Use `datetime` (not `date`) throughout to support sub-daily/high-frequency data
- DataFrame datetime column should be timezone-aware
- Interface should be flexible enough for various data sources (APIs, files, databases)

---

### Phase 4: Testing & Documentation

**Goal**: Ensure all data sources work correctly.

**Tasks**:
- [ ] Unit tests for each extracted module
- [ ] Integration tests for Swiss data source
- [ ] Update README with multi-source documentation
- [ ] Document configuration options for each data source

---

## Acceptance Criteria

### Phase 1: Requirements
- [ ] Swiss data source requirements documented
- [ ] API access confirmed

### Phase 2: Refactoring
- [ ] Modules extracted from src.py
- [ ] All existing tests pass
- [ ] src.py provides backward compatibility

### Phase 3: Swiss Integration
- [ ] Swiss data source implemented
- [ ] Environment variable switching works
- [ ] Swiss-specific tests pass

### Phase 4: Documentation
- [ ] README updated
- [ ] Configuration documented
- [ ] Tests cover all data sources

---

## Notes

- Start refactoring only when Swiss requirements are clear
- Keep backward compatibility throughout
- Each phase should result in passing tests
- Consider Nepal data source requirements during design (but implement later)

**Future data sources** (design interface to accommodate):
- Nepal (specific API TBD)
- Other hydromet services
- Potentially file-based sources (CSV, Excel)

**High-frequency data support**:
- Current: daily resolution (date column)
- Future: sub-daily resolution (datetime column, timezone-aware)
- Interface uses `datetime` from the start to avoid breaking changes later
- Aggregation logic may be needed (sub-daily → daily for some forecast types)

## Related Issues

- gi_PR-002: iEasyHydro HF Data Retrieval Validation (completed phases 1-9)
- Future: gi_PR-004: Nepal Data Source Integration

---

## Status

**Current Phase**: Not started
**Blocked by**: Swiss data source API documentation/access
