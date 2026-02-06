# Pre-processing of operational runoff data for hydrological forecasting
This component allows the reading of daily river runoff data from excel files and, if access is available, from the iEasyHydro database. The script is intended to run at 11 o'clock every day. It therefore includes daily average discharge data (in m3/s) from all dates prior to today and todays morning measurement of river runoff. The data is stored in a csv file.

We perform a rough filtering of the data to remove outliers. The filtering is based on the assumption that the discharge should not change too much from one day to the next. We calculate the inter-quartile-range and filter out all values that are below the first quartile minus 1.5 times the inter-quartile-range or above the third quartile plus 1.5 times the inter-quartile-range. The filtered data is then stored in a csv file.

## Input
- Configuration as described in doc/configuration.md
- Excel file(s) with daily river runoff data, one file per measurement site. The excel files have 2 header lines and one column for date in the format %d.%m.%Y and discharge as float in m3/s each. The first header line contains the unique code and name of the measurement site, separated by space. Some discharge data may come in a different format, with one header line only and the code ID in the name of the file. In this case, the code is extracted from the file name.

## Output
- CSV file with daily river runoff data for each site. The file contains the columns 'code', 'date', and 'discharge' (in m3/s).
- SAPPHIRE API database records (when API is enabled)

## SAPPHIRE API Integration

This module supports writing runoff data to the SAPPHIRE preprocessing API in addition to CSV files. The API integration uses **incremental sync** - only newly fetched data is sent to the API, while the complete dataset is always written to CSV.

### Data Flow

```
┌─────────────────────┐     ┌──────────────────────┐
│  Historical Data    │     │  iEasyHydro HF SDK   │
│  (CSV/Excel files)  │     │  (new records only)  │
└─────────┬───────────┘     └──────────┬───────────┘
          │                            │
          ▼                            ▼
    ┌─────────────────────────────────────────┐
    │         Combined DataFrame              │
    │    (full_data + new_data tracked)       │
    │    + Virtual station calculations       │
    └─────────────────┬───────────────────────┘
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
    ┌───────────┐           ┌───────────┐
    │  CSV File │           │ SAPPHIRE  │
    │ (all data)│           │    API    │
    └───────────┘           │(new only) │
                            └───────────┘
```

### Virtual Stations

Virtual stations (e.g., reservoir inflows calculated from weighted sums of contributing gauges) are handled specially:

1. **Calculation**: Virtual station discharge is calculated from the weighted sum of contributing stations' data, as configured in `config_virtual_stations.json`.

2. **API Sync**: After calculating virtual stations for the full dataset, the module extracts virtual station records for the same date range as the new data and includes them in the API sync.

3. **Consistency Checking**: The `verify_runoff_data_consistency()` function separates virtual station issues from regular station issues, since virtual stations may have sync delays due to their calculation dependencies.

### Why Incremental Sync?

- **Performance**: Avoids re-uploading 500k+ historical records on every run
- **Efficiency**: Only today's discharge measurements need to sync daily
- **Scalability**: API writes complete in seconds instead of hours

### Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `SAPPHIRE_API_URL` | `http://localhost:8000` | Base URL of the SAPPHIRE API gateway |
| `SAPPHIRE_API_ENABLED` | `true` | Set to `false` to disable API writes entirely |
| `SAPPHIRE_DEBUG_VERIFY` | `false` | Set to `true` to verify CSV and API data consistency after writes |

### Data Consistency Verification

When `SAPPHIRE_DEBUG_VERIFY=true`, the module compares CSV and API data after writing to ensure they match. This is useful during the transition period to verify the API integration is working correctly.

The verification checks:
- **Runoff data**: Compares `runoff_day.csv` with the `runoffs` database table
- **Hydrograph data**: Compares `hydrograph_day.csv` with the `hydrographs` database table

Each verification reports:
- Record counts in CSV vs API
- Missing records (in CSV but not API)
- Extra records (in API but not CSV)
- Value mismatches (same key, different values)

Example output:
```
✓ Runoff: Data consistent: 585606 CSV records match 585606 API records
✓ Hydrograph: Hydrograph data consistent: 14600 CSV records, 14600 API records
```

### Operating Modes (Future)

The incremental sync is designed to support different operating modes:

| Mode | Behavior | Use Case |
|------|----------|----------|
| **Operational** | Sync only today's data | Daily forecast runs |
| **Maintenance** | Sync last N days | Backfill after outages, corrections |
| **Initial** | Sync all historical data | First-time setup, database rebuild |

Currently, the module automatically determines what data is "new" based on the latest date in existing data. Future versions will allow explicit configuration of the sync window.

### Verifying API Data

To check data in the preprocessing database:

```bash
# Connect to the database
docker exec -it sapphire-preprocessing-db psql -U postgres -d preprocessing_db

# Count records
SELECT COUNT(*) FROM runoffs;

# Check date range
SELECT MIN(date), MAX(date) FROM runoffs;

# View recent records
SELECT * FROM runoffs ORDER BY date DESC LIMIT 10;
```

## Date Handling
While iEasyHydro HF stores date and time for each data point, this module only uses the date part for the daily river runoff data for producing daily and lower-resolution forecasts.  
This module standardizes on normalized pandas Timestamp objects for all date values.
This means:
- All datetime objects have their time components set to midnight (00:00:00)
- All date comparisons are consistent
- Dataframe merging on date columns works reliably
- Timezone information is preserved when relevant

Never use Python native `datetime.date` objects for date comparisons or merges as 
this causes incompatibilities with pandas Timestamp objects.

## Development
### Run locally
From the directory preprocessing_runoff run the following command:
```
ieasyhydroforecast_env_file_path=path/to/.env python preprocessing_runoff.py
```
### Run tests
From the directory apps run the following command:
```
SAPPHIRE_TESTDEV_ENV=TRUE python -m pytest preprocessing_runoff/test
```



