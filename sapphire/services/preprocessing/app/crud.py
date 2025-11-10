from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from typing import Optional

from app.models import Runoff, Hydrograph, Meteo
from app.schemas import RunoffCreate, RunoffUpdate, HydrographCreate, HydrographBulkCreate, MeteoBulkCreate
from typing import List
from app.logger import logger


def create_runoff(db: Session, bulk_data) -> list[Runoff]:
    """Create or update multiple runoffs in bulk (upsert based on horizon_type, code, date)"""
    try:
        db_runoffs = []

        for item in bulk_data.data:
            # Check if a record with the same (horizon_type, code, date) exists
            existing_runoff = db.query(Runoff).filter(
                Runoff.horizon_type == item.horizon_type,
                Runoff.code == item.code,
                Runoff.date == item.date
            ).first()

            if existing_runoff:
                # Update existing record
                for key, value in item.dict().items():
                    setattr(existing_runoff, key, value)
                db_runoffs.append(existing_runoff)
                logger.info(f"Updated runoff: {item.horizon_type}, {item.code}, {item.date}")
            else:
                # Create new record
                new_runoff = Runoff(**item.dict())
                db.add(new_runoff)
                db_runoffs.append(new_runoff)
                logger.info(f"Created runoff: {item.horizon_type}, {item.code}, {item.date}")

        db.commit()

        # Refresh all runoffs to get updated state
        for runoff in db_runoffs:
            db.refresh(runoff)

        logger.info(f"Processed {len(db_runoffs)} runoffs in bulk")
        return db_runoffs
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating runoffs in bulk: {str(e)}", exc_info=True)
        raise


def get_runoff(db: Session, horizon: Optional[str] = None, code: Optional[str] = None, start_date: Optional[str] = None,
                end_date: Optional[str] = None, skip: int = 0, limit: int = 100) -> list[Runoff]:
    """Get runoffs with optional filtering and pagination"""
    try:
        query = db.query(Runoff)
        if horizon:
            query = query.filter(Runoff.horizon_type == horizon)
        if code:
            query = query.filter(Runoff.code == code)
        if start_date:
            query = query.filter(Runoff.date >= start_date)
        if end_date:
            query = query.filter(Runoff.date <= end_date)
        results = query.offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} runoffs (code={code}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching runoffs: {str(e)}", exc_info=True)
        raise


def create_hydrograph(db: Session, bulk_data: HydrographBulkCreate) -> List[Hydrograph]:
    """Create or update multiple hydrographs in bulk (upsert based on horizon_type, code, date)"""
    try:
        db_hydrographs = []

        for item in bulk_data.data:
            # Check if a record with the same (horizon_type, code, date) exists
            existing_hydrograph = db.query(Hydrograph).filter(
                Hydrograph.horizon_type == item.horizon_type,
                Hydrograph.code == item.code,
                Hydrograph.date == item.date
            ).first()

            if existing_hydrograph:
                # Update existing record
                for key, value in item.model_dump().items():
                    setattr(existing_hydrograph, key, value)
                db_hydrographs.append(existing_hydrograph)
                logger.info(f"Updated hydrograph: {item.horizon_type}, {item.code}, {item.date}")
            else:
                # Create new record
                new_hydrograph = Hydrograph(**item.model_dump())
                db.add(new_hydrograph)
                db_hydrographs.append(new_hydrograph)
                logger.info(f"Created hydrograph: {item.horizon_type}, {item.code}, {item.date}")

        db.commit()

        # Refresh all hydrographs to get updated state
        for hydrograph in db_hydrographs:
            db.refresh(hydrograph)

        logger.info(f"Processed {len(db_hydrographs)} hydrographs in bulk")
        return db_hydrographs
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating hydrographs in bulk: {str(e)}", exc_info=True)
        raise


def get_hydrograph(db: Session, horizon: Optional[str] = None, code: Optional[str] = None,
                    start_date: Optional[str] = None, end_date: Optional[str] = None,
                    skip: int = 0, limit: int = 100) -> List[Hydrograph]:
    """Get hydrographs with optional filtering and pagination"""
    try:
        query = db.query(Hydrograph)
        if horizon:
            query = query.filter(Hydrograph.horizon_type == horizon)
        if code:
            query = query.filter(Hydrograph.code == code)
        if start_date:
            query = query.filter(Hydrograph.date >= start_date)
        if end_date:
            query = query.filter(Hydrograph.date <= end_date)
        results = query.offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} hydrographs (code={code}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching hydrographs: {str(e)}", exc_info=True)
        raise


def create_meteo(db: Session, bulk_data: MeteoBulkCreate) -> List[Meteo]:
    """Create or update multiple meteo records in bulk (upsert based on meteo_type, code, date)"""
    try:
        db_meteos = []

        for item in bulk_data.data:
            # Check if a record with the same (meteo_type, code, date) exists
            existing_meteo = db.query(Meteo).filter(
                Meteo.meteo_type == item.meteo_type,
                Meteo.code == item.code,
                Meteo.date == item.date
            ).first()

            if existing_meteo:
                # Update existing record
                for key, value in item.model_dump().items():
                    setattr(existing_meteo, key, value)
                db_meteos.append(existing_meteo)
                logger.info(f"Updated meteo: {item.meteo_type}, {item.code}, {item.date}")
            else:
                # Create new record
                new_meteo = Meteo(**item.model_dump())
                db.add(new_meteo)
                db_meteos.append(new_meteo)
                logger.info(f"Created meteo: {item.meteo_type}, {item.code}, {item.date}")

        db.commit()

        # Refresh all meteo records to get updated state
        for meteo in db_meteos:
            db.refresh(meteo)

        logger.info(f"Processed {len(db_meteos)} meteo records in bulk")
        return db_meteos

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating meteo records in bulk: {str(e)}", exc_info=True)
        raise


def get_meteo(db: Session, meteo_type: Optional[str] = None, code: Optional[str] = None,
               start_date: Optional[str] = None, end_date: Optional[str] = None,
               skip: int = 0, limit: int = 100) -> List[Meteo]:
    """Get meteorological records with optional filtering and pagination"""
    try:
        query = db.query(Meteo)
        if meteo_type:
            query = query.filter(Meteo.meteo_type == meteo_type)
        if code:
            query = query.filter(Meteo.code == code)
        if start_date:
            query = query.filter(Meteo.date >= start_date)
        if end_date:
            query = query.filter(Meteo.date <= end_date)
        results = query.offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} meteo records (code={code}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching meteo records: {str(e)}", exc_info=True)
        raise
