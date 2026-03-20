# Preprocessing Service: Add Coverage Endpoints

**Status**: Draft
**Depends on**: Nothing (can be implemented independently)
**Depended on by**: `backfill_new_stations.py` (works without this, but less efficiently)

## Problem

The backfill script (`apps/preprocessing_gateway/backfill_new_stations.py`) needs to know
what data the API already has so it can detect gaps and avoid re-writing existing records.
Without coverage endpoints, it must either:
- Download all records to compare (expensive)
- Treat everything as new and rely on upserts (wasteful)

## Scope

Two changes in two separate packages:

### 1. Preprocessing Service (`sapphire/services/preprocessing/`)

Add `GET /meteo/coverage` and `GET /snow/coverage` endpoints that return
min/max date and record count per (type, code) group.

#### `app/schemas.py` — add response models

```python
class MeteoCoverageResponse(BaseModel):
    """Coverage summary for a (meteo_type, code) group."""
    meteo_type: str
    code: str
    min_date: DateType
    max_date: DateType
    record_count: int


class SnowCoverageResponse(BaseModel):
    """Coverage summary for a (snow_type, code) group."""
    snow_type: str
    code: str
    min_date: DateType
    max_date: DateType
    record_count: int
```

#### `app/crud.py` — add coverage query functions

```python
from sqlalchemy import func

def get_meteo_coverage(
    db: Session,
    meteo_type: Optional[str] = None,
    code: Optional[str] = None,
) -> List[dict]:
    """Return coverage summary per (meteo_type, code).

    SELECT meteo_type, code, MIN(date), MAX(date), COUNT(*)
    FROM meteo GROUP BY meteo_type, code
    """
    try:
        query = db.query(
            Meteo.meteo_type,
            Meteo.code,
            func.min(Meteo.date).label("min_date"),
            func.max(Meteo.date).label("max_date"),
            func.count(Meteo.id).label("record_count"),
        ).group_by(Meteo.meteo_type, Meteo.code)

        if meteo_type:
            meteo_type_enum = MeteoType(meteo_type)
            query = query.filter(Meteo.meteo_type == meteo_type_enum)
        if code:
            query = query.filter(Meteo.code == code)

        query = query.order_by(Meteo.meteo_type, Meteo.code)
        rows = query.all()

        return [
            {
                "meteo_type": row.meteo_type.value,
                "code": row.code,
                "min_date": row.min_date,
                "max_date": row.max_date,
                "record_count": row.record_count,
            }
            for row in rows
        ]
    except SQLAlchemyError as e:
        logger.error("Error fetching meteo coverage: %s", str(e), exc_info=True)
        raise


def get_snow_coverage(
    db: Session,
    snow_type: Optional[str] = None,
    code: Optional[str] = None,
) -> List[dict]:
    """Return coverage summary per (snow_type, code). Same pattern as meteo."""
    try:
        query = db.query(
            Snow.snow_type,
            Snow.code,
            func.min(Snow.date).label("min_date"),
            func.max(Snow.date).label("max_date"),
            func.count(Snow.id).label("record_count"),
        ).group_by(Snow.snow_type, Snow.code)

        if snow_type:
            snow_type_enum = SnowType(snow_type)
            query = query.filter(Snow.snow_type == snow_type_enum)
        if code:
            query = query.filter(Snow.code == code)

        query = query.order_by(Snow.snow_type, Snow.code)
        rows = query.all()

        return [
            {
                "snow_type": row.snow_type.value,
                "code": row.code,
                "min_date": row.min_date,
                "max_date": row.max_date,
                "record_count": row.record_count,
            }
            for row in rows
        ]
    except SQLAlchemyError as e:
        logger.error("Error fetching snow coverage: %s", str(e), exc_info=True)
        raise
```

#### `app/main.py` — add endpoints

```python
from app.schemas import MeteoCoverageResponse, SnowCoverageResponse

@app.get(
    "/meteo/coverage",
    response_model=List[MeteoCoverageResponse],
    tags=["Meteorological Data"],
)
def read_meteo_coverage(
        meteo_type: Optional[str] = None,
        code: Optional[str] = None,
        db: Session = Depends(get_db),
):
    """Get coverage summary for meteorological data."""
    try:
        return crud.get_meteo_coverage(db=db, meteo_type=meteo_type, code=code)
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch meteo coverage",
        )


@app.get(
    "/snow/coverage",
    response_model=List[SnowCoverageResponse],
    tags=["Snow Data"],
)
def read_snow_coverage(
        snow_type: Optional[str] = None,
        code: Optional[str] = None,
        db: Session = Depends(get_db),
):
    """Get coverage summary for snow data."""
    try:
        return crud.get_snow_coverage(db=db, snow_type=snow_type, code=code)
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch snow coverage",
        )
```

#### Testing prerequisites

The service currently lacks a proper test infrastructure for endpoint testing. Before
writing coverage endpoint tests, the service needs:

1. A `pyproject.toml` (following the postprocessing service pattern)
2. An upgraded `tests/conftest.py` with SQLite in-memory engine, `db_session` fixture,
   and `client` fixture (following `sapphire/services/postprocessing/tests/conftest.py`)

---

### 2. sapphire-api-client — add convenience methods

Add `get_meteo_coverage()` and `get_snow_coverage()` to `SapphirePreprocessingClient`.

```python
def get_meteo_coverage(
    self,
    meteo_type: str | None = None,
    code: str | None = None,
) -> list[dict]:
    """Query /meteo/coverage endpoint.

    Returns list of dicts with keys:
        meteo_type, code, min_date, max_date, record_count
    """
    params = {}
    if meteo_type:
        params["meteo_type"] = meteo_type
    if code:
        params["code"] = code
    response = self._get("/meteo/coverage", params=params)
    return response.json()


def get_snow_coverage(
    self,
    snow_type: str | None = None,
    code: str | None = None,
) -> list[dict]:
    """Query /snow/coverage endpoint."""
    params = {}
    if snow_type:
        params["snow_type"] = snow_type
    if code:
        params["code"] = code
    response = self._get("/snow/coverage", params=params)
    return response.json()
```

Once these client methods exist, `backfill_new_stations.py` can replace its
`requests.get()` calls with `client.get_meteo_coverage()` / `client.get_snow_coverage()`.
Until then, the direct `requests.get()` approach works fine.

## API Response Example

```
GET /meteo/coverage?meteo_type=T

[
  {"meteo_type": "T", "code": "15013", "min_date": "2000-01-01", "max_date": "2025-12-31", "record_count": 9497},
  {"meteo_type": "T", "code": "15025", "min_date": "2000-01-01", "max_date": "2025-12-31", "record_count": 9497}
]
```

## Verification

After implementing both changes:
```bash
# Service tests
cd sapphire/services/preprocessing
.venv/bin/python -m pytest tests/ -v

# Integration check from backfill script
SAPPHIRE_SYNC_MODE=initial uv run backfill_new_stations.py
```
