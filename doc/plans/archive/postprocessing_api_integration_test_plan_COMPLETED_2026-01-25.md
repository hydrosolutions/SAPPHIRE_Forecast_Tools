# Postprocessing API Integration Test Plan

## Objective

Verify that the postprocessing module correctly reads all data from the SAPPHIRE API when `SAPPHIRE_API_ENABLED=true`, and that the full postprocessing workflow completes successfully.

## Prerequisites

1. Docker services running (`cd sapphire && docker-compose up -d`)
2. Database populated with test data (via data migrators)
3. Environment configured for test deployment (kyg_data_forecast_tools config)

## Test Scenarios

### Test 1: Verify API Services Are Running

**Commands:**
```bash
curl http://localhost:8000/health
curl http://localhost:8000/health/ready
```

**Expected:** Both return healthy status

---

### Test 2: Verify Data Exists in API

**Commands:**
```bash
# Check preprocessing data (runoff for observed discharge)
curl "http://localhost:8000/api/preprocessing/runoff/?horizon=pentad&limit=5"
curl "http://localhost:8000/api/preprocessing/runoff/?horizon=decade&limit=5"

# Check postprocessing data (LR forecasts)
curl "http://localhost:8000/api/postprocessing/lr-forecast/?horizon=pentad&limit=5"
curl "http://localhost:8000/api/postprocessing/lr-forecast/?horizon=decade&limit=5"

# Check postprocessing data (ML forecasts)
curl "http://localhost:8000/api/postprocessing/forecast/?horizon=pentad&limit=5"
curl "http://localhost:8000/api/postprocessing/forecast/?horizon=decade&limit=5"
```

**Expected:** Each returns JSON array with records (not empty)

---

### Test 3: Run Postprocessing with API Enabled (PENTAD)

**Command:**
```bash
cd apps/postprocessing_forecasts
ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm \
SAPPHIRE_PREDICTION_MODE=PENTAD \
SAPPHIRE_API_ENABLED=true \
python postprocessing_forecasts.py
```

**Expected:**
- No errors during execution
- Log messages showing "Reading from SAPPHIRE API"
- Skill metrics calculated and saved
- Combined forecasts saved

**Verification:**
- Check output CSV files are created
- Check API has new skill metrics: `curl "http://localhost:8000/api/postprocessing/skill-metric/?limit=5"`

---

### Test 4: Run Postprocessing with API Enabled (DECAD)

**Command:**
```bash
cd apps/postprocessing_forecasts
ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm \
SAPPHIRE_PREDICTION_MODE=DECAD \
SAPPHIRE_API_ENABLED=true \
python postprocessing_forecasts.py
```

**Expected:**
- No errors during execution
- Decadal skill metrics calculated
- Combined forecasts saved for decade horizon

---

### Test 5: Compare API vs CSV Results (Consistency Check)

**Command:**
```bash
cd apps/postprocessing_forecasts
ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm \
SAPPHIRE_PREDICTION_MODE=PENTAD \
SAPPHIRE_API_ENABLED=true \
SAPPHIRE_CONSISTENCY_CHECK=true \
python postprocessing_forecasts.py
```

**Expected:**
- Consistency check logs show data matches between API and CSV
- Any mismatches are logged as warnings (not errors in lenient mode)

---

### Test 6: Verify CSV Fallback Works

**Command:**
```bash
cd apps/postprocessing_forecasts
ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm \
SAPPHIRE_PREDICTION_MODE=PENTAD \
SAPPHIRE_API_ENABLED=false \
python postprocessing_forecasts.py
```

**Expected:**
- Log messages showing "Reading from CSV"
- Process completes successfully using CSV files
- Same output as API mode (backward compatibility)

---

### Test 7: Verify Graceful Fallback on API Failure

**Setup:** Stop the API services
```bash
cd sapphire && docker-compose stop
```

**Command:**
```bash
cd apps/postprocessing_forecasts
ieasyhydroforecast_env_file_path=~/Documents/GitHub/kyg_data_forecast_tools/config/.env_develop_kghm \
SAPPHIRE_PREDICTION_MODE=PENTAD \
SAPPHIRE_API_ENABLED=true \
python postprocessing_forecasts.py
```

**Expected:**
- Warning logged about API being unavailable
- Automatic fallback to CSV
- Process completes successfully

**Cleanup:** Restart services
```bash
cd sapphire && docker-compose up -d
```

---

## Data Functions to Verify

| Function | Data Source | API Endpoint |
|----------|-------------|--------------|
| `read_observed_pentadal_data()` | Runoff (pentad) | `GET /api/preprocessing/runoff/?horizon=pentad` |
| `read_observed_decadal_data()` | Runoff (decade) | `GET /api/preprocessing/runoff/?horizon=decade` |
| `read_linreg_forecasts_pentad()` | LR Forecasts | `GET /api/postprocessing/lr-forecast/?horizon=pentad` |
| `read_linreg_forecasts_decade()` | LR Forecasts | `GET /api/postprocessing/lr-forecast/?horizon=decade` |
| `read_machine_learning_forecasts_pentad()` | ML Forecasts | `GET /api/postprocessing/forecast/?horizon=pentad` |
| `read_machine_learning_forecasts_decade()` | ML Forecasts | `GET /api/postprocessing/forecast/?horizon=decade` |

---

## Success Criteria

1. All 7 test scenarios pass
2. No unhandled exceptions during API reading
3. Output data matches expected format
4. CSV fallback works when API disabled or unavailable
5. Consistency check shows API and CSV data are equivalent

---

## Execution Order

1. Test 1 - Verify services running
2. Test 2 - Verify data exists
3. Test 6 - CSV fallback (baseline)
4. Test 3 - PENTAD with API
5. Test 4 - DECAD with API
6. Test 5 - Consistency check
7. Test 7 - Graceful fallback

---

## Notes

- Tests 3-5 require the full forecast pipeline to have run previously (LR, ML forecasts exist)
- If data is missing, run data migrators first:
  ```bash
  docker exec -it sapphire-postprocessing-api python app/data_migrator.py --type lrforecast
  docker exec -it sapphire-postprocessing-api python app/data_migrator.py --type forecast
  docker exec -it sapphire-preprocessing-api python app/data_migrator.py --type runoff
  ```

---

## Test Results (2026-01-25)

| Test | Description | Status | Notes |
|------|-------------|--------|-------|
| Test 1 | API health check | ✅ PASSED | Both /health and /health/ready returned healthy |
| Test 2 | Data exists in API | ✅ PASSED | All endpoints returned data |
| Test 6 | CSV fallback (baseline) | ✅ PASSED | Process completed using CSV files |
| Test 3 | PENTAD with API | ✅ PASSED | 4127 combined forecasts, 3951 skill metrics written |
| Test 4 | DECAD with API | ✅ PASSED | 2054 combined forecasts, 2044 skill metrics written |
| Test 5 | Consistency check | ✅ PASSED | API and CSV data verified consistent (4349 rows combined forecasts, 3951 rows skill metrics) |
| Test 7 | Graceful fallback | ✅ PASSED | Warnings logged, automatic CSV fallback, process completed |

### Summary

All 7 test scenarios passed. The postprocessing module correctly:
- Reads all data from SAPPHIRE API when enabled
- Falls back to CSV when API is disabled
- Falls back to CSV gracefully when API is unavailable
- Produces consistent results between API and CSV modes
- Writes combined forecasts and skill metrics to API
