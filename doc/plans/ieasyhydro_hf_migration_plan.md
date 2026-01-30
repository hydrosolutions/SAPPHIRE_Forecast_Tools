# iEasyHydro HF Migration Plan: Deprecating Legacy iEH API

## Overview

This document tracks the migration from iEasyHydro (iEH) to iEasyHydro High Frequency (iEH HF) across the SAPPHIRE Forecast Tools project. The iEH API is being deprecated in favor of the newer iEH HF database.

**Created**: 2026-01-30
**Status**: Planning

---

## Background

### Current State

SAPPHIRE Forecast Tools currently supports two data sources for runoff data:

1. **iEasyHydro (iEH)** - Legacy API, being deprecated
   - Environment variables: `IEASYHYDRO_HOST`, `IEASYHYDRO_USERNAME`, `IEASYHYDRO_PASSWORD`, `ORGANIZATION_ID`
   - Used via: `ieasyhydro-python-sdk` library

2. **iEasyHydro HF (iEH HF)** - New high-frequency database
   - Environment variables: `IEASYHYDROHF_HOST`, `IEASYHYDROHF_USERNAME`, `IEASYHYDROHF_PASSWORD`
   - Used via: `ieasyhydro-python-sdk` with HF-specific methods
   - Provides higher temporal resolution data

### Why Migrate?

- iEH is scheduled for deprecation
- iEH HF provides more frequent data updates
- Consolidating to single data source reduces complexity
- Better API design in iEH HF

---

## Migration Scope

### Modules Affected

| Module | iEH Usage | iEH HF Usage | Migration Status |
|--------|-----------|--------------|------------------|
| `preprocessing_runoff` | Legacy code paths | Primary path for kghm/tjhm | Partial |
| `iEasyHydroForecast` | Helper functions | New functions added | Partial |
| `pipeline` | Environment config | Environment config | Config only |
| `forecast_dashboard` | Display code | Display code | Display only |

### Code Locations

**iEH (Legacy) - to be deprecated:**
```
apps/iEasyHydroForecast/src/iEasyHydroForecast/setup_library.py
  - read_discharge_data_from_iEH()
  - Various iEH connection helpers

apps/config/.env*
  - IEASYHYDRO_HOST
  - IEASYHYDRO_USERNAME
  - IEASYHYDRO_PASSWORD
  - ORGANIZATION_ID
```

**iEH HF (Target):**
```
apps/preprocessing_runoff/src/preprocessing_runoff/preprocessing_runoff.py
  - read_all_runoff_data_from_ieh_hf()
  - Uses ieasyhydro-python-sdk with HF configuration

apps/config/.env*
  - IEASYHYDROHF_HOST
  - IEASYHYDROHF_USERNAME
  - IEASYHYDROHF_PASSWORD
```

---

## Configuration Changes

### Current Configuration (Mixed)

The `.env` file currently contains both iEH and iEH HF credentials:

```bash
# Legacy iEasyHydro (to be deprecated)
IEASYHYDRO_HOST=http://localhost:9000
IEASYHYDRO_USERNAME=<username>
IEASYHYDRO_PASSWORD=<password>
ORGANIZATION_ID=1

# iEasyHydro HF (new)
IEASYHYDROHF_HOST=https://hf.ieasyhydro.org/api/v1/
IEASYHYDROHF_USERNAME=<username>
IEASYHYDROHF_PASSWORD=<password>
```

### Target Configuration (Post-Migration)

After migration, only iEH HF configuration will be needed:

```bash
# iEasyHydro High Frequency Database
IEASYHYDROHF_HOST=https://hf.ieasyhydro.org/api/v1/
IEASYHYDROHF_USERNAME=<username>
IEASYHYDROHF_PASSWORD=<password>
```

### Feature Flag

Currently controlled by organization:
```bash
ieasyhydroforecast_organization=kghm  # Implies iEH HF required
```

Proposed: Explicit feature flag (see `configuration_update_plan.md`):
```bash
ieasyhydroforecast_connect_to_iEH_HF=True
```

---

## Migration Phases

### Phase 1: Audit Current Usage (TODO)

- [ ] Inventory all iEH-specific code paths
- [ ] Identify organization-specific iEH vs iEH HF logic
- [ ] Document which deployments use which API
- [ ] List all environment variables related to iEH/iEH HF

### Phase 2: Consolidate to iEH HF (TODO)

- [ ] Update `read_discharge_data_from_iEH()` to use iEH HF SDK
- [ ] Create migration shim for backward compatibility
- [ ] Update helper functions in `setup_library.py`
- [ ] Test with all organization configurations

### Phase 3: Remove Legacy Code (TODO)

- [ ] Remove iEH-specific code paths
- [ ] Remove legacy environment variables from documentation
- [ ] Update deployment documentation
- [ ] Remove legacy SDK initialization code

### Phase 4: Documentation (TODO)

- [ ] Update `doc/configuration.md` with iEH HF only
- [ ] Update deployment guides
- [ ] Archive migration documentation

---

## Deployment Considerations

### Organizations Requiring iEH HF

| Organization | iEH HF Required | Notes |
|--------------|-----------------|-------|
| `demo` | No | Uses local/test data |
| `kghm` | Yes | Production deployment |
| `tjhm` | Yes | Production deployment |

### SSH Tunnel Requirements

For organizations requiring iEH HF, SSH tunnel may be needed for secure database access. This is documented in server deployment procedures.

### Fallback Behavior

The system should gracefully handle iEH HF unavailability:
1. Log clear error message
2. Use cached data if available
3. Skip data fetch (don't crash pipeline)
4. Alert operators via configured channels

---

## Testing Requirements

### Unit Tests
- [ ] Test iEH HF SDK connection and authentication
- [ ] Test data retrieval for known stations
- [ ] Test error handling for connection failures
- [ ] Test data validation and parsing

### Integration Tests
- [ ] Test full preprocessing_runoff pipeline with iEH HF
- [ ] Test pipeline orchestration with iEH HF modules
- [ ] Test dashboard display of iEH HF data

### Deployment Tests
- [ ] Test demo deployment (no iEH HF)
- [ ] Test kghm deployment (with iEH HF)
- [ ] Test tjhm deployment (with iEH HF)

---

## Rollback Plan

If issues arise during migration:

1. **Configuration rollback**: Restore legacy environment variables
2. **Code rollback**: Revert to pre-migration commits
3. **Data fallback**: Use cached historical data

---

## Related Documents

- `configuration_update_plan.md` - Overall configuration management improvements
- `module_issues.md` - Known issues tracking
- `observations.md` - Runtime observations

---

## Notes

This refactoring should be coordinated with the broader configuration update plan. The move from organization-based to feature-flag-based configuration will simplify the iEH HF migration by making data source selection explicit.

---

*Last updated: 2026-01-30*
