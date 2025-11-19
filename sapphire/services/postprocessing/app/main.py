from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from typing import List

from app import crud
from app.database import engine, Base, get_db
from app.schemas import ForecastResponse, ForecastBulkCreate, LRForecastResponse, LRForecastBulkCreate, SkillMetricResponse, SkillMetricBulkCreate
from app.logger import logger
from app.config import settings

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="API for postprocessing forecast data and skill metrics for SAPPHIRE Forecast Tools",
    docs_url="/docs",
    redoc_url="/redoc"
)


@app.get("/", tags=["Root"])
def root():
    return {
        "message": "Welcome to the Postprocessing Service API",
        "docs": "/docs",
        "health": "/health"
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
        return {
            "status": "ready",
            "service": "Postprocessing Service API",
            "database": "connected"
        }
    except Exception as e:
        logger.error(f"Readiness check failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not ready"
        )


@app.post("/forecast/",
          response_model=List[ForecastResponse],
          status_code=status.HTTP_201_CREATED,
          tags=["Forecast"])
def create_forecast(bulk_data: ForecastBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple forecasts in bulk"""
    try:
        return crud.create_forecast(db=db, bulk_data=bulk_data)
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create or update forecasts in bulk"
        )


@app.get("/forecast/", response_model=List[ForecastResponse], tags=["Forecast"])
def read_forecast(
    horizon: str = None,
    code: str = None,
    model: str = None,
    start_date: str = None,
    end_date: str = None,
    start_target: str = None,
    end_target: str = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
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
            skip=skip,
            limit=limit
        )
        return forecasts
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve forecasts"
        )


@app.post("/lr-forecast/",
          response_model=List[LRForecastResponse],
          status_code=status.HTTP_201_CREATED,
          tags=["LRForecast"])
def create_lr_forecast(bulk_data: LRForecastBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple LR forecasts in bulk"""
    try:
        return crud.create_lr_forecast(db=db, bulk_data=bulk_data)
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create or update LR forecasts in bulk"
        )


@app.get("/lr-forecast/", response_model=List[LRForecastResponse], tags=["LRForecast"])
def read_lr_forecast(
    horizon: str = None,
    code: str = None,
    start_date: str = None,
    end_date: str = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
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
            limit=limit
        )
        return lr_forecasts
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve LR forecasts"
        )


@app.post("/skill-metric/",
          response_model=List[SkillMetricResponse],
          status_code=status.HTTP_201_CREATED,
          tags=["SkillMetric"])
def create_skill_metric(bulk_data: SkillMetricBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple skill metrics in bulk"""
    try:
        return crud.create_skill_metric(db=db, bulk_data=bulk_data)
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create or update skill metrics in bulk"
        )


@app.get("/skill-metric/", response_model=List[SkillMetricResponse], tags=["SkillMetric"])
def read_skill_metric(
    horizon: str = None,
    code: str = None,
    model: str = None,
    start_date: str = None,
    end_date: str = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
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
            limit=limit
        )
        return skill_metrics
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve skill metrics"
        )
