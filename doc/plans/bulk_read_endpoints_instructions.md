# Instructions: Add Bulk-Read Endpoints to sapphire-api-client

## Context

The SAPPHIRE forecast system has two data access patterns:
1. **sapphire-api-client** — used by most modules, goes through REST API with pagination (10,000 records/page)
2. **Direct SQL** — used by `long_term_forecasting/data_interface.py` for performance (single query, no HTTP overhead)

The direct SQL approach bypasses API validation, exposes DB credentials to consumers, and creates a parallel code path to maintain. We want to eliminate it by adding **bulk-read endpoints** to the API services so the `sapphire-api-client` can fetch large training datasets efficiently — without paginated loops.

## Goal

Add `/bulk/` endpoints to the **preprocessing** and **postprocessing** FastAPI services that return all matching records in a single response (no pagination). Then add corresponding methods to the `sapphire-api-client` Python package. This lets the `long_term_forecasting` module replace its direct SQL with API calls while maintaining performance.

---

## Part 0: Safeguards (MUST implement)

Bulk endpoints return unbounded result sets. Without safeguards, they can crash the API server, block database writes, or time out silently. **Every bulk endpoint must implement all of the following.**

### 0.1 Require at least one filter

Reject requests that provide no filters at all. An unfiltered `GET /bulk/runoff/` would return the entire table — today that might be fine, but it's a time bomb as data grows.

```python
from fastapi import HTTPException

# At the top of every bulk endpoint:
if not any([horizon, code, start_date, end_date]):
    raise HTTPException(
        status_code=400,
        detail="Bulk endpoints require at least one filter (horizon, code, start_date, or end_date)."
    )
```

For endpoints where a type filter is already required (meteo_type, snow_type), that filter counts — no additional filter needed.

### 0.2 Row count safety limit

Add a `max_records` parameter (default 500,000) to every bulk endpoint. If the query would return more rows than `max_records`, return HTTP 413 instead of loading them all into memory. The client can narrow the date range or filter by code to reduce the result set.

Implementation in `crud.py` — do a COUNT first:

```python
def get_all_runoffs(db: Session, horizon=None, code=None,
                    start_date=None, end_date=None, max_records=500_000):
    query = db.query(Runoff)
    if horizon:
        horizon_enum = HorizonType(horizon)
        query = query.filter(Runoff.horizon_type == horizon_enum)
    if code:
        query = query.filter(Runoff.code == code)
    if start_date:
        query = query.filter(Runoff.date >= start_date)
    if end_date:
        query = query.filter(Runoff.date <= end_date)

    # Safety check: count before loading
    row_count = query.count()
    if row_count > max_records:
        raise ValueError(
            f"Query would return {row_count} records (limit: {max_records}). "
            f"Add filters to narrow the result set."
        )

    return query.order_by(Runoff.code, Runoff.date).all()
```

In `main.py`, catch the ValueError and return HTTP 413:

```python
@app.get("/bulk/runoff/", response_model=List[RunoffResponse])
def bulk_read_runoff(
    horizon: Optional[str] = None,
    code: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    max_records: int = 500_000,
    db: Session = Depends(get_db),
):
    if not any([horizon, code, start_date, end_date]):
        raise HTTPException(status_code=400,
            detail="Bulk endpoints require at least one filter.")
    try:
        return crud.get_all_runoffs(db, horizon=horizon, code=code,
                                    start_date=start_date, end_date=end_date,
                                    max_records=max_records)
    except ValueError as e:
        raise HTTPException(status_code=413, detail=str(e))
```

### 0.3 Database statement timeout

Set a 60-second statement timeout on bulk queries so they cannot block the database indefinitely. If a query takes longer than 60s, PostgreSQL will cancel it and the API returns an error.

```python
from sqlalchemy import text

def get_all_runoffs(db: Session, ...):
    # Set statement timeout for this transaction only
    db.execute(text("SET LOCAL statement_timeout = '60s'"))

    query = db.query(Runoff)
    # ... filters ...
    row_count = query.count()
    # ... safety check ...
    return query.order_by(Runoff.code, Runoff.date).all()
```

This protects against: accidental full-table scans, lock contention with concurrent writes, and runaway queries from unexpected data growth.

### 0.4 Response row count logging

Log the number of rows returned by every bulk call so you can monitor data growth over time.

```python
results = query.order_by(Runoff.code, Runoff.date).all()
logger.info(f"Bulk runoff query returned {len(results)} records "
            f"(filters: horizon={horizon}, code={code}, "
            f"start_date={start_date}, end_date={end_date})")
return results
```

### 0.5 Client-side timeout

The `sapphire-api-client` bulk methods must use a longer HTTP timeout than the default. The standard read methods likely use 30s (matching the gateway). Bulk methods should use 120s:

```python
def bulk_read_runoff(self, ...) -> pd.DataFrame:
    """Fetch all runoff records in a single request (no pagination)."""
    params = { ... }
    response = self._get("/bulk/runoff/", params=params, timeout=120)
    return pd.DataFrame(response)
```

If the client's `_get` method doesn't accept a `timeout` parameter, add support for it.

---

## Part 1: Preprocessing Service Bulk Endpoints

**Service location:** `sapphire/services/preprocessing/app/`

### 1.1 Add to `main.py`: Three new GET endpoints

#### `GET /bulk/runoff/`
Replaces this direct SQL in `data_interface.py:144-149`:
```sql
SELECT date, code, discharge FROM runoffs
WHERE [optional filters] ORDER BY code, date
```

Parameters (all optional query params):
- `horizon` (str) — filter by horizon type
- `code` (str) — filter by station code
- `start_date` (date) — inclusive start
- `end_date` (date) — inclusive end

Returns: `List[RunoffResponse]` — ALL matching records, no skip/limit.

#### `GET /bulk/meteo/`
Replaces this direct SQL in `data_interface.py:93-98`:
```sql
SELECT date, code, value as P FROM meteo
WHERE meteo_type = :meteo_type [AND optional filters] ORDER BY code, date
```

Parameters (all optional except meteo_type):
- `meteo_type` (str, required) — "P" or "T"
- `code` (str)
- `start_date` (date)
- `end_date` (date)

Returns: `List[MeteoResponse]` — ALL matching records, no skip/limit.

#### `GET /bulk/snow/`
Replaces this direct SQL in `data_interface.py:187-192`:
```sql
SELECT date, code, value as SWE FROM snow
WHERE snow_type = :snow_type [AND optional filters] ORDER BY code, date
```

Parameters (all optional except snow_type):
- `snow_type` (str, required) — "SWE", "ROF", or "HS"
- `code` (str)
- `start_date` (date)
- `end_date` (date)

Returns: `List[SnowResponse]` — ALL matching records, no skip/limit.

### 1.2 Add to `crud.py`: Three new read functions

These are identical to the existing paginated versions (`get_runoffs`, `get_hydrographs`, `get_meteo`, `get_snow`) but **without** `.offset(skip).limit(limit)`. They must include:
- `.order_by(Model.code, Model.date)`
- Statement timeout (Part 0.3)
- Row count safety check (Part 0.2)
- Response logging (Part 0.4)

Name them: `get_all_runoffs()`, `get_all_meteo()`, `get_all_snow()`.

### 1.3 Implementation pattern

Follow the existing endpoint pattern but with all safeguards from Part 0. Full example for runoff:

```python
# In main.py
@app.get("/bulk/runoff/", response_model=List[RunoffResponse])
def bulk_read_runoff(
    horizon: Optional[str] = None,
    code: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    max_records: int = 500_000,
    db: Session = Depends(get_db),
):
    if not any([horizon, code, start_date, end_date]):
        raise HTTPException(status_code=400,
            detail="Bulk endpoints require at least one filter "
                   "(horizon, code, start_date, or end_date).")
    try:
        return crud.get_all_runoffs(db, horizon=horizon, code=code,
                                    start_date=start_date, end_date=end_date,
                                    max_records=max_records)
    except ValueError as e:
        raise HTTPException(status_code=413, detail=str(e))

# In crud.py
def get_all_runoffs(db: Session, horizon=None, code=None,
                    start_date=None, end_date=None, max_records=500_000):
    db.execute(text("SET LOCAL statement_timeout = '60s'"))

    query = db.query(Runoff)
    if horizon:
        horizon_enum = HorizonType(horizon)
        query = query.filter(Runoff.horizon_type == horizon_enum)
    if code:
        query = query.filter(Runoff.code == code)
    if start_date:
        query = query.filter(Runoff.date >= start_date)
    if end_date:
        query = query.filter(Runoff.date <= end_date)

    row_count = query.count()
    if row_count > max_records:
        raise ValueError(
            f"Query would return {row_count} records (limit: {max_records}). "
            f"Add filters to narrow the result set.")

    results = query.order_by(Runoff.code, Runoff.date).all()
    logger.info(f"Bulk runoff: returned {len(results)} records "
                f"(horizon={horizon}, code={code}, "
                f"start_date={start_date}, end_date={end_date})")
    return results
```

Apply the same pattern to `get_all_meteo()` and `get_all_snow()`. For meteo and snow, the required type filter (`meteo_type` / `snow_type`) counts as a filter, so no additional filter is needed.

---

## Part 2: Postprocessing Service Bulk Endpoint

**Service location:** `sapphire/services/postprocessing/app/`

### 2.1 Add to `main.py`: One new GET endpoint

#### `GET /bulk/long-forecast/`
Replaces this direct SQL in `data_interface.py:749-758`:
```sql
SELECT date, code, q, q_xgb, q_lgbm, q_catboost, q_loc
FROM long_forecasts
WHERE UPPER(model_type::text) = UPPER(:model_type)
  AND UPPER(horizon_type::text) = UPPER(:horizon_type)
  AND horizon_value = :horizon_value
  AND date <= :today
ORDER BY code, date
```

Parameters:
- `horizon_type` (str, optional)
- `horizon_value` (int, optional)
- `code` (str, optional)
- `model` (str, optional) — model type filter
- `start_date` (str, optional)
- `end_date` (str, optional)

Returns: `List[LongForecastResponse]` — ALL matching records, no skip/limit.

### 2.2 Add to `crud.py`: `get_all_long_forecasts()`

Same pattern as existing `get_long_forecasts()` (lines 137-175 of current crud.py) but without `.offset(skip).limit(limit)`. Must include:
- `.order_by(LongForecast.code, LongForecast.date)`
- Statement timeout: `db.execute(text("SET LOCAL statement_timeout = '60s'"))`
- Row count safety check with `max_records=500_000`
- Response logging

The endpoint in `main.py` must enforce at least one filter and catch `ValueError` → HTTP 413, same as the preprocessing examples in Part 1.3.

---

## Part 3: Update sapphire-api-client Package

The `sapphire-api-client` is a separate package at https://github.com/hydrosolutions/sapphire-api-client.

### 3.1 Add bulk-read methods to `SapphirePreprocessingClient`

Add three new methods that call the `/bulk/` endpoints and return a pandas DataFrame:

```python
def bulk_read_runoff(self, horizon=None, code=None,
                     start_date=None, end_date=None) -> pd.DataFrame:
    """Fetch all runoff records in a single request (no pagination).

    At least one filter must be provided, or the server returns HTTP 400.
    If the result set exceeds the server's max_records limit (default 500k),
    the server returns HTTP 413 — narrow the date range or add a code filter.
    """
    params = {}
    if horizon: params["horizon"] = horizon
    if code: params["code"] = code
    if start_date: params["start_date"] = str(start_date)
    if end_date: params["end_date"] = str(end_date)
    response = self._get("/bulk/runoff/", params=params, timeout=120)
    return pd.DataFrame(response)

def bulk_read_meteo(self, meteo_type, code=None,
                    start_date=None, end_date=None) -> pd.DataFrame:
    """Fetch all meteo records in a single request (no pagination)."""
    params = {"meteo_type": meteo_type}
    if code: params["code"] = code
    if start_date: params["start_date"] = str(start_date)
    if end_date: params["end_date"] = str(end_date)
    response = self._get("/bulk/meteo/", params=params, timeout=120)
    return pd.DataFrame(response)

def bulk_read_snow(self, snow_type, code=None,
                   start_date=None, end_date=None) -> pd.DataFrame:
    """Fetch all snow records in a single request (no pagination)."""
    params = {"snow_type": snow_type}
    if code: params["code"] = code
    if start_date: params["start_date"] = str(start_date)
    if end_date: params["end_date"] = str(end_date)
    response = self._get("/bulk/snow/", params=params, timeout=120)
    return pd.DataFrame(response)
```

**Important:** If the client's `_get` method does not accept a `timeout` parameter, add support for it (pass through to `requests.get(url, params=params, timeout=timeout)`).

### 3.2 Add bulk-read method to `SapphirePostprocessingClient`

```python
def bulk_read_long_forecasts(self, horizon_type=None, horizon_value=None,
                              model=None, code=None, start_date=None,
                              end_date=None) -> pd.DataFrame:
    """Fetch all long forecast records in a single request (no pagination).

    At least one filter must be provided, or the server returns HTTP 400.
    """
    params = {}
    if horizon_type: params["horizon_type"] = horizon_type
    if horizon_value is not None: params["horizon_value"] = horizon_value
    if model: params["model"] = model
    if code: params["code"] = code
    if start_date: params["start_date"] = str(start_date)
    if end_date: params["end_date"] = str(end_date)
    response = self._get("/bulk/long-forecast/", params=params, timeout=120)
    return pd.DataFrame(response)
```

---

## Part 4: Existing Code & Schema Reference

To implement correctly, you need to understand the existing codebase. Here are the key files:

### Preprocessing service
- **Models:** `sapphire/services/preprocessing/app/models.py` — defines `Runoff`, `Hydrograph`, `Meteo`, `Snow` SQLAlchemy models
- **Schemas:** `sapphire/services/preprocessing/app/schemas.py` — defines Pydantic response schemas (`RunoffResponse`, `MeteoResponse`, `SnowResponse`)
- **CRUD:** `sapphire/services/preprocessing/app/crud.py` — existing paginated read functions to copy from
- **Endpoints:** `sapphire/services/preprocessing/app/main.py` — existing GET endpoints to follow as pattern

### Postprocessing service
- **Models:** `sapphire/services/postprocessing/app/models.py` — defines `LongForecast` model with columns: `horizon_type`, `horizon_value`, `code`, `date`, `model_type`, `valid_from`, `valid_to`, `q`, `q_obs`, `q_xgb`, `q_lgbm`, `q_catboost`, `q_loc`, `q05`-`q95`
- **Schemas:** `sapphire/services/postprocessing/app/schemas.py` — `LongForecastResponse` Pydantic schema
- **CRUD:** `sapphire/services/postprocessing/app/crud.py` — existing `get_long_forecasts()` with pagination
- **Endpoints:** `sapphire/services/postprocessing/app/main.py` — existing GET `/long-forecast/` endpoint

### Enums (important for filtering)
**Preprocessing** `HorizonType`: day, pentad, decade
**Preprocessing** `MeteoType`: T, P
**Preprocessing** `SnowType`: SWE, ROF, HS
**Postprocessing** `HorizonType`: day, pentad, decade, month, quarter, season
**Postprocessing** `ModelType`: TSMixer, TiDE, TFT, EM, NE, RRAM, LR, GBT, LR_Base, LR_SM, LR_SM_DT, LR_SM_ROF, MC_ALD, SM_GBT, SM_GBT_LR, SM_GBT_Norm, Skilled Mean, Naive Mean

---

## Part 5: What NOT to Do

1. **Do NOT modify existing paginated endpoints** — they are used by other modules in production
2. **Do NOT add authentication** — the existing endpoints don't have it; keep bulk endpoints consistent
3. **Do NOT skip the safeguards from Part 0** — every bulk endpoint MUST have: filter requirement, row count limit, statement timeout, and logging
4. **Do NOT change the database schema or models** — only add new endpoint routes and CRUD functions
5. **Do NOT modify the API gateway routing** — the preprocessing and postprocessing services are accessed directly by the client, not through the gateway, in the current architecture
6. **Do NOT remove the `max_records` default of 500,000** — this is a safety valve. If a caller genuinely needs more, they can pass a higher value explicitly, but the default must stay

---

## Part 6: Testing

### Service-side tests
For each new bulk endpoint, write tests covering both normal operation and safeguards:

**Normal operation:**
1. Inserts 5-10 test records into the test database
2. Calls with a filter (e.g., `code="15013"`) → expects only matching records
3. Calls with `start_date` and `end_date` → expects date-filtered results
4. Calls with no matching data → expects empty list

**Safeguard tests (critical):**
5. Calls with **no filters at all** → expects HTTP 400 with clear error message
6. Inserts records, calls with `max_records=3` when 5+ records exist → expects HTTP 413
7. Verifies response includes correct row count in logs (check log output or structured response)

For meteo/snow endpoints, verify that the required type filter (`meteo_type`/`snow_type`) satisfies the "at least one filter" requirement — no additional filter needed.

Follow the existing test patterns in the service test directories.

### Client-side tests
For each new client method, write a test that:
1. Mocks the HTTP GET call
2. Verifies the correct URL and query parameters are sent
3. Verifies the response is converted to a DataFrame correctly
4. Verifies that `timeout=120` is passed to the HTTP call
5. Verifies HTTP 400 (no filters) raises an appropriate client error
6. Verifies HTTP 413 (too many records) raises an appropriate client error with the message

---

## Part 7: Verification

After implementing, the `long_term_forecasting/data_interface.py` `DataInterfaceDB` class should be replaceable with API calls like:

```python
# Before (direct SQL):
discharge = self.get_runoff_data(end_date=today.strftime("%Y-%m-%d"))

# After (via API client):
client = SapphirePreprocessingClient(base_url=api_url)
discharge = client.bulk_read_runoff(end_date=today.strftime("%Y-%m-%d"))
```

And the `BasePredictorDataInterface` should be replaceable with:

```python
# Before (direct SQL):
df = self._execute_postprocessing_query(query, params)

# After (via API client):
client = SapphirePostprocessingClient(base_url=api_url)
df = client.bulk_read_long_forecasts(
    model=model_name, horizon_type=horizon_type,
    horizon_value=horizon_value, end_date=today.strftime("%Y-%m-%d")
)
```

Both should return equivalent DataFrames with the same columns and data types.
