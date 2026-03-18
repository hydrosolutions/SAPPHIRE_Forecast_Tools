from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app import crud
from app.config import settings
from app.database import Base, engine, get_db
from app.logger import logger
from app.schemas import (
    BulletinBulkCreate,
    BulletinResponse,
    ForecastBulkCreate,
    ForecastResponse,
    LongForecastBulkCreate,
    LongForecastResponse,
    LRForecastBulkCreate,
    LRForecastResponse,
    LRVisibilityBulkCreate,
    LRVisibilityResponse,
    SkillMetricBulkCreate,
    SkillMetricResponse,
)

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="API for postprocessing forecast data and skill metrics for SAPPHIRE Forecast Tools",
    docs_url="/docs",
    redoc_url="/redoc",
)


@app.get("/", tags=["Root"])
def root():
    return {
        "message": "Welcome to the Postprocessing Service API",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Basic health check"""
    return {"status": "healthy", "service": "Postprocessing Service API"}


@app.get("/health/ready", tags=["Health"])
def readiness_check(db: Session = Depends(get_db)):
    """Check if service is ready (including database)"""
    try:
        # Try to execute a simple query
        db.execute(text("SELECT 1"))
        return {"status": "ready", "service": "Postprocessing Service API", "database": "connected"}
    except Exception as e:
        logger.error(f"Readiness check failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Service not ready"
        )


@app.post(
    "/forecast/",
    response_model=list[ForecastResponse],
    status_code=status.HTTP_201_CREATED,
    tags=["Forecast"],
)
def create_forecast(bulk_data: ForecastBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple forecasts in bulk"""
    try:
        return crud.create_forecast(db=db, bulk_data=bulk_data)
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create or update forecasts in bulk: {str(e)}",
        )


@app.get("/forecast/", response_model=list[ForecastResponse], tags=["Forecast"])
def read_forecast(
    horizon: str = None,
    code: str = None,
    model: str = None,
    start_date: str = None,
    end_date: str = None,
    start_target: str = None,
    end_target: str = None,
    target: str = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """Retrieve forecasts with optional filtering by horizon_type, code, model_type, date range"""
    try:
        forecasts = crud.get_forecast(
            db=db,
            horizon=horizon,
            code=code,
            model=model,
            start_date=start_date,
            end_date=end_date,
            start_target=start_target,
            end_target=end_target,
            target=target,
            skip=skip,
            limit=limit,
        )
        return forecasts
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve forecasts"
        )


@app.post(
    "/long-forecast/",
    response_model=list[LongForecastResponse],
    status_code=status.HTTP_201_CREATED,
    tags=["LongForecast"],
)
def create_long_forecast(bulk_data: LongForecastBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple long forecasts in bulk"""
    try:
        return crud.create_long_forecast(db=db, bulk_data=bulk_data)
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create or update long forecasts in bulk: {str(e)}",
        )


@app.get("/long-forecast/", response_model=list[LongForecastResponse], tags=["LongForecast"])
def read_long_forecast(
    horizon_type: str = None,
    horizon_value: int = None,
    code: str = None,
    model: str = None,
    start_date: str = None,
    end_date: str = None,
    valid_from: str = None,
    valid_to: str = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """Retrieve long forecasts with optional filtering by horizon type and value, code, model_type, date range, valid_from and valid_to"""
    try:
        long_forecasts = crud.get_long_forecast(
            db=db,
            horizon_type=horizon_type,
            horizon_value=horizon_value,
            code=code,
            model=model,
            start_date=start_date,
            end_date=end_date,
            valid_from=valid_from,
            valid_to=valid_to,
            skip=skip,
            limit=limit,
        )
        return long_forecasts
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve long forecasts",
        )


@app.post(
    "/lr-forecast/",
    response_model=list[LRForecastResponse],
    status_code=status.HTTP_201_CREATED,
    tags=["LRForecast"],
)
def create_lr_forecast(bulk_data: LRForecastBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple LR forecasts in bulk"""
    try:
        return crud.create_lr_forecast(db=db, bulk_data=bulk_data)
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create or update LR forecasts in bulk: {str(e)}",
        )


@app.get("/lr-forecast/", response_model=list[LRForecastResponse], tags=["LRForecast"])
def read_lr_forecast(
    horizon: str = None,
    code: str = None,
    start_date: str = None,
    end_date: str = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """Retrieve LR forecasts with optional filtering by horizon_type, code, date range"""
    try:
        lr_forecasts = crud.get_lr_forecast(
            db=db,
            horizon=horizon,
            code=code,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            limit=limit,
        )
        return lr_forecasts
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve LR forecasts",
        )


@app.post(
    "/skill-metric/",
    response_model=list[SkillMetricResponse],
    status_code=status.HTTP_201_CREATED,
    tags=["SkillMetric"],
)
def create_skill_metric(bulk_data: SkillMetricBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple skill metrics in bulk"""
    try:
        return crud.create_skill_metric(db=db, bulk_data=bulk_data)
    except SQLAlchemyError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create or update skill metrics in bulk: {str(e)}",
        )


@app.get("/skill-metric/", response_model=list[SkillMetricResponse], tags=["SkillMetric"])
def read_skill_metric(
    horizon: str = None,
    code: str = None,
    model: str = None,
    start_date: str = None,
    end_date: str = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """Retrieve skill metrics with optional filtering by horizon_type, code, model_type, date range"""
    try:
        skill_metrics = crud.get_skill_metric(
            db=db,
            horizon=horizon,
            code=code,
            model=model,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            limit=limit,
        )
        return skill_metrics
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve skill metrics",
        )


@app.post(
    "/bulletin/",
    response_model=list[BulletinResponse],
    status_code=status.HTTP_201_CREATED,
    tags=["Bulletin"],
)
def create_bulletin(bulk_data: BulletinBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple bulletins in bulk"""
    try:
        return crud.create_bulletin(db=db, bulk_data=bulk_data)
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create or update bulletins in bulk",
        )


@app.get("/bulletin/", response_model=list[BulletinResponse], tags=["Bulletin"])
def read_bulletin(
    horizon: str = None,
    year: int = None,
    horizon_value: int = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """Retrieve bulletins with optional filtering by horizon type and value, year, code, model_type, date range"""
    try:
        bulletins = crud.get_bulletin(
            db=db, horizon=horizon, year=year, horizon_value=horizon_value, skip=skip, limit=limit
        )
        return bulletins
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to retrieve bulletins"
        )


@app.delete("/bulletin/", status_code=status.HTTP_204_NO_CONTENT, tags=["Bulletin"])
def delete_bulletin(
    horizon: str,
    year: int,
    horizon_value: int,
    code: str,
    model: str,
    db: Session = Depends(get_db)
):
    """Delete a bulletin identified by its unique constraint fields:
    horizon_type, year, horizon_value, code, model_type"""
    try:
        deleted = crud.delete_bulletin(
            db=db,
            horizon=horizon,
            year=year,
            horizon_value=horizon_value,
            code=code,
            model=model
        )
        if not deleted:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Bulletin not found"
            )
    except HTTPException:
        raise
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete bulletin"
        )


@app.post("/lr-visibility/", response_model=list[LRVisibilityResponse], status_code=status.HTTP_201_CREATED, tags=["LRVisibility"])
def create_lr_visibility(bulk_data: LRVisibilityBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple LR visibility records in bulk"""
    try:
        return crud.create_lr_visibility(db=db, bulk_data=bulk_data)
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create or update LR visibility records in bulk",
        )


@app.get("/lr-visibility/", response_model=list[LRVisibilityResponse], tags=["LRVisibility"])
def read_lr_visibility(
    horizon: str = None,
    code: str = None,
    month: int = None,
    horizon_value: int = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    """Retrieve LR visibility records with optional filtering by horizon_type, code, month, horizon_value, year"""
    try:
        lr_visibility_records = crud.get_lr_visibility(
            db=db,
            horizon=horizon,
            code=code,
            month=month,
            horizon_value=horizon_value,
            skip=skip,
            limit=limit,
        )
        return lr_visibility_records
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve LR visibility records",
        )
