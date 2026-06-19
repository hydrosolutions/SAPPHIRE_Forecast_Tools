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
     ┌────────┴────────┐
     ▼                 ▼
┌──────────────┐  ┌──────────────────┐
│  SAPPHIRE    │  │   SAPPHIRE       │
│  API (post-  │  │   API (pre-      │  │   API (pre-      │
│  processing/ │  │   processing/    │  │   processing/    │
│  lr-forecast)│  │   hydrograph)    │  │   runoff)        │
└──────────────┘  └──────────────────┘  └──────────────────┘
```

### Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `SAPPHIRE_API_URL` | `http://localhost:8000` | Base URL of the SAPPHIRE API gateway |
| `SAPPHIRE_API_ENABLED` | `true` | Set to `false` to read from CSV files instead |
| `SAPPHIRE_SYNC_MODE` | `operational` | Sync mode for writes: `operational`, `maintenance`, or `initial` |
| `SAPPHIRE_CONSISTENCY_CHECK` | `false` | Set to `true` to read from both API and CSV and verify consistency |
| `SAPPHIRE_CONSISTENCY_STRICT` | `false` | Set to `true` to fail on value/NaN mismatches during consistency check |

### Sync Modes (SAPPHIRE_SYNC_MODE)

Controls how much data is written to the API for pentad/decad runoff data:

| Mode | Behavior | Use Case |
|------|----------|----------|
| `operational` (default) | Write only the latest date's data | Daily forecast runs |
| `maintenance` | Write the last 90 days of data | Backfill after outages, corrections; refresh elapsed-period runoff discharge |
| `initial` | Write all data | First-time setup, database rebuild |

### Pentad/decad runoff discharge backfill

The `runoffs` rows for `horizon_type` `pentad`/`decade` carry two values:

- **`predictor`** — backward-looking (pentad: 3-day discharge sum before the issue
  date; decad: previous decad mean). Computable at issue time.
- **`discharge`** — the forward-looking **forecast target** (mean discharge of the
  *upcoming* pentad/decad). It can only be computed once that period has elapsed, so
  it is written `NULL` for the current issue date and **backfilled later**.

The backfill happens in the **maintenance** run (`maintenance:linear_regression`,
which sets `SAPPHIRE_SYNC_MODE=maintenance`). Maintenance re-aggregates and re-writes
the trailing 90-day window of runoff **even when forecasts are already up to date**,
so elapsed-period `discharge` gets filled. Operational runs only write today's slice
and never backfill past rows.

Writes in `maintenance`/`initial` mode are **clobber-safe**: the module reads the
existing rows and merges, so an incoming `NULL` never overwrites an existing non-null
`discharge`/`predictor`, and a freshly-computed `discharge` is never dropped just
because its `predictor` is independently null. (Operational mode does not read/merge.)

**Per organisation.** Each run only touches the stations in its own config, so run
maintenance once per org with that org's env file:

```bash
ieasyhydroforecast_env_file_path=/path/to/<org>.env bash apps/run_locally.sh maintenance:linear_regression
```

**One-time backfill of older rows.** Rows older than the 90-day maintenance window
need a single `initial`-mode run (per org; split by horizon to bound payload size):

```bash
ieasyhydroforecast_env_file_path=/path/to/<org>.env SAPPHIRE_SYNC_MODE=initial \
  SAPPHIRE_PREDICTION_MODE=PENTAD bash apps/run_locally.sh maintenance:linear_regression
ieasyhydroforecast_env_file_path=/path/to/<org>.env SAPPHIRE_SYNC_MODE=initial \
  SAPPHIRE_PREDICTION_MODE=DECAD  bash apps/run_locally.sh maintenance:linear_regression
```

**Verify** (filter by the org's station-code prefix — the DB may hold several orgs):

```bash
curl -s "http://localhost:8000/api/preprocessing/runoff/?horizon=pentad&start_date=<YYYY-MM-DD>&limit=10000" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('discharge_null=', sum(1 for r in d if r.get('discharge') is None))"
```

Rows that remain `NULL` after maintenance are genuinely **daily-input-starved** (no
daily discharge in the target window) — expected, not a bug.

### Operating Modes

| Mode | Environment Variables | Behavior |
|------|----------------------|----------|
| **Production** | Default settings | Read from API, write latest data only |
| **Local Development** | `SAPPHIRE_API_ENABLED=false` | Read/write CSV files only |
| **Maintenance** | `SAPPHIRE_SYNC_MODE=maintenance` | Write last 90 days to API; backfill elapsed-period runoff discharge |
| **Initial Setup** | `SAPPHIRE_SYNC_MODE=initial` | Write all historical data to API |
| **Validation** | `SAPPHIRE_CONSISTENCY_CHECK=true` | Read from both sources, compare data |

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
| `test_forecast_library_api.py` | Tests for API read integration, runoff write/backfill (read-merge-write), and consistency checking |
| `test_integration_main.py` | Tests for `main()` control flow, incl. maintenance runoff refresh on caught-up hindcast |

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
