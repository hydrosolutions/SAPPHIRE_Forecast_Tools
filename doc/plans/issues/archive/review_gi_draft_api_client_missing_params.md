> **NOTE**: This plan was auto-extracted from `forecast_dashboard_api_client_comparison.md`.
> It needs review and refining before it can be implemented.

# Add missing parameters to sapphire-api-client read methods

**Status**: Draft
**Module**: infra
**Priority**: High
**Labels**: `api`, `sapphire-api-client`, `forecast-dashboard`

---

## Summary

The `sapphire-api-client` is missing several query parameters that the API endpoints support and the forecast dashboard needs. Add the missing parameters to `read_forecasts()` and `read_skill_metrics()`.

## Context

`doc/plans/forecast_dashboard_api_client_comparison.md` identified the mismatches. The forecast dashboard currently uses raw `requests.get()` calls instead of the shared client because the client lacks required parameters. Fixing this unblocks dashboard migration to the standard client.

## Problem

### read_forecasts() missing parameters

The API endpoint supports these parameters, but the client doesn't expose them:
- `model` (filter by model_type: TFT, TiDE, LR, etc.)
- `target` (filter by target date)
- `start_target`, `end_target` (target date range)

### read_skill_metrics() missing parameters

The API endpoint supports:
- `start_date`, `end_date` (date range filtering)

But the client only exposes `horizon`, `code`, `model`.

## Implementation Steps

### Step 1: Update SapphirePostprocessingClient.read_forecasts()

**Repository**: `hydrosolutions/sapphire-api-client`
**File**: `src/sapphire_api_client/postprocessing.py`

Add parameters: `model`, `target`, `start_target`, `end_target`

### Step 2: Update SapphirePostprocessingClient.read_skill_metrics()

Add parameters: `start_date`, `end_date`

### Step 3: Add tests

Add unit tests verifying:
- New parameters are passed as query params in HTTP request
- None values are excluded from query string
- Integration with existing pagination logic

### Step 4: Publish updated client

Bump version and push to `hydrosolutions/sapphire-api-client`.

## Acceptance Criteria

- [ ] `read_forecasts()` accepts `model`, `target`, `start_target`, `end_target`
- [ ] `read_skill_metrics()` accepts `start_date`, `end_date`
- [ ] All new parameters are tested
- [ ] Existing tests still pass
- [ ] Updated client published

## Follow-up (separate issue)

After this is complete, migrate `forecast_dashboard/src/db.py` to use the client instead of raw `requests.get()`.

## Related

- `doc/plans/forecast_dashboard_api_client_comparison.md` (gap analysis)
- `doc/plans/sapphire_api_integration_plan.md` (overall integration)
