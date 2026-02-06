from fastapi import FastAPI, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from datetime import date
from typing import List, Optional

from app import crud
from app.models import Runoff
from app.schemas import RunoffCreate, RunoffUpdate, RunoffResponse, RunoffBulkCreate, HydrographResponse, MeteoResponse, HydrographBulkCreate, MeteoBulkCreate, SnowResponse, SnowBulkCreate
from app.database import engine, Base, get_db
from app.logger import logger
from app.config import settings

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="API for preprocessing runoff, hydrograph, precipitation, and temperature data for SAPPHIRE Forecast Tools",
    docs_url="/docs",
    redoc_url="/redoc"
)


@app.get("/", tags=["Root"])
def root():
    return {
        "message": "Welcome to the Preprocessing Service API",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Basic health check"""
    return {"status": "healthy", "service": "Preprocessing Service API"}


@app.get("/health/ready", tags=["Health"])
def readiness_check(db: Session = Depends(get_db)):
    """Check if service is ready (including database)"""
    try:
        # Try to execute a simple query
        db.execute(text("SELECT 1"))
        return {
            "status": "ready",
            "service": "Preprocessing Service API",
            "database": "connected"
        }
    except Exception as e:
        logger.error(f"Readiness check failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not ready"
        )


@app.post("/runoff/",
          response_model=List[RunoffResponse],
          status_code=status.HTTP_201_CREATED,
          tags=["Runoff"])
def create_runoff(bulk_data: RunoffBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple runoffs in bulk"""
    try:
        return crud.create_runoff(db=db, bulk_data=bulk_data)
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create or update runoffs in bulk"
        )


@app.get("/runoff/", response_model=List[RunoffResponse], tags=["Runoff"])
def read_runoff(
        horizon: Optional[str] = None,
        code: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        skip: int = 0,
        limit: int = 100,
        db: Session = Depends(get_db)
):
    """Get runoffs with optional filtering and pagination"""
    try:
        return crud.get_runoff(
            db=db,
            horizon=horizon,
            code=code,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            limit=limit
        )
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch runoffs"
        )


@app.post("/hydrograph/",
          response_model=List[HydrographResponse],
          status_code=status.HTTP_201_CREATED,
          tags=["Hydrograph"])
def create_hydrograph(bulk_data: HydrographBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple hydrographs in bulk"""
    try:
        return crud.create_hydrograph(db=db, bulk_data=bulk_data)
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create or update hydrographs in bulk"
        )


@app.get("/hydrograph/", response_model=List[HydrographResponse], tags=["Hydrograph"])
def read_hydrographs(
        horizon: Optional[str] = None,
        code: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        skip: int = 0,
        limit: int = 100,
        db: Session = Depends(get_db)
):
    """Get hydrographs with optional filtering and pagination"""
    try:
        return crud.get_hydrograph(
            db=db,
            horizon=horizon,
            code=code,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            limit=limit
        )
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch hydrographs"
        )


@app.post("/meteo/",
          response_model=List[MeteoResponse],
          status_code=status.HTTP_201_CREATED,
          tags=["Meteorological Data"])
def create_meteo(bulk_data: MeteoBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple meteorological data records in bulk"""
    try:
        return crud.create_meteo(db=db, bulk_data=bulk_data)
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create or update meteorological data in bulk"
        )


@app.get("/meteo/", response_model=List[MeteoResponse], tags=["Meteorological Data"])
def read_meteo(
        meteo_type: Optional[str] = None,
        code: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        skip: int = 0,
        limit: int = 100,
        db: Session = Depends(get_db)
):
    """Get meteorological data with optional filtering and pagination"""
    try:
        return crud.get_meteo(
            db=db,
            meteo_type=meteo_type,
            code=code,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            limit=limit
        )
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch meteorological data"
        )


@app.post("/snow/",
          response_model=List[SnowResponse],
          status_code=status.HTTP_201_CREATED,
          tags=["Snow Data"])
def create_snow(bulk_data: SnowBulkCreate, db: Session = Depends(get_db)):
    """Create or update multiple snow data records in bulk"""
    try:
        return crud.create_snow(db=db, bulk_data=bulk_data)
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create or update snow data in bulk"
        )


@app.get("/snow/", response_model=List[SnowResponse], tags=["Snow Data"])
def read_snow(
        snow_type: Optional[str] = None,
        code: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        skip: int = 0,
        limit: int = 100,
        db: Session = Depends(get_db)
):
    """Get snow data with optional filtering and pagination"""
    try:
        return crud.get_snow(
            db=db,
            snow_type=snow_type,
            code=code,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            limit=limit
        )
    except SQLAlchemyError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to fetch snow data"
        )
