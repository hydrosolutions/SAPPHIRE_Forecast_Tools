# Linear Regression Forecasting Module

This module produces pentadal and decadal river discharge forecasts using linear regression. It aggregates daily discharge data into pentads (5-day periods) and decades (10-day periods), then applies statistical linear regression models to generate forecasts.

## Input

- **Configuration**: As described in `doc/configuration.md`
- **Daily discharge data**: Read from SAPPHIRE API (default) or CSV file
- Required columns: `code`, `date`, `discharge` (in m3/s)

## Output

- Pentadal and decadal forecasts with uncertainty estimates
- Forecasts are written to the SAPPHIRE postprocessing API (when enabled) or CSV files

## SAPPHIRE API Integration

This module supports reading runoff data from the SAPPHIRE preprocessing API. The API integration uses a **fail-fast** approach - if the API is unavailable, the module fails immediately with a clear error rather than silently falling back to potentially stale CSV data.

### Data Flow

```
┌─────────────────────────────────────┐
│         SAPPHIRE API                │
│  (preprocessing/runoff endpoint)    │
│       horizon_type = 'day'          │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│      Daily Discharge DataFrame      │
│   columns: code, date, discharge    │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│   Aggregate to Pentad/Decade        │
│   Apply Linear Regression Models    │
└─────────────┬───────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│         SAPPHIRE API                │
│   (postprocessing/forecasts)        │
└─────────────────────────────────────┘
```

### Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `SAPPHIRE_API_URL` | `http://localhost:8000` | Base URL of the SAPPHIRE API gateway |
| `SAPPHIRE_API_ENABLED` | `true` | Set to `false` to read from CSV files instead |
| `SAPPHIRE_CONSISTENCY_CHECK` | `false` | Set to `true` to read from both API and CSV and verify consistency |
| `SAPPHIRE_CONSISTENCY_STRICT` | `false` | Set to `true` to fail on value/NaN mismatches during consistency check |

### Operating Modes

| Mode | Environment Variables | Behavior |
|------|----------------------|----------|
| **Production** | Default settings | Read from API, fail fast if unavailable |
| **Local Development** | `SAPPHIRE_API_ENABLED=false` | Read from CSV files |
| **Validation** | `SAPPHIRE_CONSISTENCY_CHECK=true` | Read from both, compare data |
| **Strict Validation** | `SAPPHIRE_CONSISTENCY_CHECK=true`, `SAPPHIRE_CONSISTENCY_STRICT=true` | Fail on any mismatch |

### Consistency Checking

When `SAPPHIRE_CONSISTENCY_CHECK=true`, the module reads from both API and CSV sources and compares them:

- **Lenient mode** (default): NaN and value mismatches are logged as warnings but don't cause failure. This is useful when historical data has been cleaned differently between sources (e.g., outlier filtering).
- **Strict mode** (`SAPPHIRE_CONSISTENCY_STRICT=true`): Any mismatch causes failure.

Example output:
```
SAPPHIRE_CONSISTENCY_CHECK: Reading from both API and CSV (lenient)...
WARNING: Column 'discharge' has 109 NaN mismatches (likely from outlier filtering)
WARNING: Column 'discharge' has 19 value mismatches (max diff: 142.500000)
SAPPHIRE_CONSISTENCY_CHECK: PASSED - Data consistent (with warnings logged above)
```

## Development

### Run locally

From the directory `linear_regression`, run:
```bash
ieasyhydroforecast_env_file_path=path/to/.env python linear_regression.py
```

### Run tests

From the directory `apps`, run:
```bash
SAPPHIRE_TESTDEV_ENV=TRUE python -m pytest linear_regression/test -v
```

### Test files

| Test File | Description |
|-----------|-------------|
| `test_forecast_library_api.py` | Tests for API read integration and consistency checking |

## Troubleshooting

### API returns no data

If the API returns empty results when data exists in the database:

1. Check that the `horizon_type` filter uses the correct case. The API expects lowercase (`day`) but stores uppercase (`DAY`) internally.
2. Verify data exists:
   ```bash
   docker exec -it sapphire-preprocessing-db psql -U postgres -d preprocessing_db -c "SELECT COUNT(*) FROM runoffs WHERE horizon_type = 'DAY';"
   ```

### Consistency check shows NaN mismatches

This is expected when historical data has different outlier filtering between API and CSV sources. The mismatches are typically from:
- Values that were filtered as outliers in one source but not the other
- Different handling of missing data during initial data loading

In lenient mode (default), these are logged as warnings and don't cause failure.
