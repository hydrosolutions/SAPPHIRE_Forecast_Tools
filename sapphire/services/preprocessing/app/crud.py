from sqlalchemy import tuple_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from typing import List, Optional

from app.models import Runoff, Hydrograph, Meteo, Snow, HorizonType, MeteoType, SnowType
from app.schemas import RunoffCreate, RunoffUpdate, HydrographCreate, HydrographBulkCreate, MeteoBulkCreate, SnowBulkCreate
from app.logger import logger


def _has_changes(existing, incoming_data: dict) -> bool:
    """Return True if any field in incoming_data differs from the existing ORM record."""
    return any(getattr(existing, k) != v for k, v in incoming_data.items())


def create_runoff(db: Session, bulk_data) -> list[Runoff]:
    """Create or update multiple runoffs in bulk (upsert based on horizon_type, code, date)"""
    try:
        incoming = [item.dict() for item in bulk_data.data]
        keys = {(i["horizon_type"], i["code"], i["date"]) for i in incoming}

        existing_map = {
            (r.horizon_type, r.code, r.date): r
            for r in db.query(Runoff).filter(
                tuple_(Runoff.horizon_type, Runoff.code, Runoff.date).in_(keys)
            ).all()
        }

        db_runoffs = []
        changed = []  # only records that need refresh
        for data in incoming:
            key = (data["horizon_type"], data["code"], data["date"])
            existing = existing_map.get(key)

            if existing:
                if _has_changes(existing, data):
                    for k, v in data.items():
                        setattr(existing, k, v)
                    changed.append(existing)
                    logger.info(f"Updated runoff: {key}")
                else:
                    logger.debug(f"Skipped unchanged runoff: {key}")
                db_runoffs.append(existing)
            else:
                new = Runoff(**data)
                db.add(new)
                changed.append(new)
                db_runoffs.append(new)
                logger.info(f"Created runoff: {key}")

        if changed:
            db.commit()
            for r in changed:
                db.refresh(r)

        logger.info(f"Processed {len(db_runoffs)} runoffs in bulk")
        return db_runoffs
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating/updating runoffs in bulk: {str(e)}", exc_info=True)
        raise


def get_runoff(db: Session, horizon: Optional[str] = None, code: Optional[str] = None, start_date: Optional[str] = None,
                end_date: Optional[str] = None, skip: int = 0, limit: int = 100) -> list[Runoff]:
    """Get runoffs with optional filtering and pagination"""
    try:
        query = db.query(Runoff)
        if horizon:
            # Convert string to HorizonType enum for proper comparison
            horizon_enum = HorizonType(horizon)
            query = query.filter(Runoff.horizon_type == horizon_enum)
        if code:
            query = query.filter(Runoff.code == code)
        if start_date:
            query = query.filter(Runoff.date >= start_date)
        if end_date:
            query = query.filter(Runoff.date <= end_date)
        # Order by code and date for consistent pagination
        results = query.order_by(Runoff.code, Runoff.date).offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} runoffs (code={code}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching runoffs: {str(e)}", exc_info=True)
        raise


def create_hydrograph(db: Session, bulk_data: HydrographBulkCreate) -> List[Hydrograph]:
    """Create or update multiple hydrographs in bulk (upsert based on horizon_type, code, date)"""
    try:
        incoming = [item.model_dump() for item in bulk_data.data]
        keys = {(i["horizon_type"], i["code"], i["date"]) for i in incoming}

        existing_map = {
            (r.horizon_type, r.code, r.date): r
            for r in db.query(Hydrograph).filter(
                tuple_(Hydrograph.horizon_type, Hydrograph.code, Hydrograph.date).in_(keys)
            ).all()
        }

        db_hydrographs = []
        changed = []
        for data in incoming:
            key = (data["horizon_type"], data["code"], data["date"])
            existing = existing_map.get(key)

            if existing:
                if _has_changes(existing, data):
                    for k, v in data.items():
                        setattr(existing, k, v)
                    changed.append(existing)
                    logger.info(f"Updated hydrograph: {key}")
                else:
                    logger.debug(f"Skipped unchanged hydrograph: {key}")
                db_hydrographs.append(existing)
            else:
                new = Hydrograph(**data)
                db.add(new)
                changed.append(new)
                db_hydrographs.append(new)
                logger.info(f"Created hydrograph: {key}")

        if changed:
            db.commit()
            for h in changed:
                db.refresh(h)

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
            # Convert string to HorizonType enum for proper comparison
            horizon_enum = HorizonType(horizon)
            query = query.filter(Hydrograph.horizon_type == horizon_enum)
        if code:
            query = query.filter(Hydrograph.code == code)
        if start_date:
            query = query.filter(Hydrograph.date >= start_date)
        if end_date:
            query = query.filter(Hydrograph.date <= end_date)
        # Order by code and date for consistent pagination
        results = query.order_by(Hydrograph.code, Hydrograph.date).offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} hydrographs (code={code}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching hydrographs: {str(e)}", exc_info=True)
        raise


def create_meteo(db: Session, bulk_data: MeteoBulkCreate) -> List[Meteo]:
    """Create or update multiple meteo records in bulk (upsert based on meteo_type, code, date)"""
    try:
        incoming = [item.model_dump() for item in bulk_data.data]
        keys = {(i["meteo_type"], i["code"], i["date"]) for i in incoming}

        existing_map = {
            (r.meteo_type, r.code, r.date): r
            for r in db.query(Meteo).filter(
                tuple_(Meteo.meteo_type, Meteo.code, Meteo.date).in_(keys)
            ).all()
        }

        db_meteos = []
        changed = []
        for data in incoming:
            key = (data["meteo_type"], data["code"], data["date"])
            existing = existing_map.get(key)

            if existing:
                if _has_changes(existing, data):
                    for k, v in data.items():
                        setattr(existing, k, v)
                    changed.append(existing)
                    logger.info(f"Updated meteo: {key}")
                else:
                    logger.debug(f"Skipped unchanged meteo: {key}")
                db_meteos.append(existing)
            else:
                new = Meteo(**data)
                db.add(new)
                changed.append(new)
                db_meteos.append(new)
                logger.info(f"Created meteo: {key}")

        if changed:
            db.commit()
            for m in changed:
                db.refresh(m)

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
            # Convert string to MeteoType enum for proper comparison
            meteo_type_enum = MeteoType(meteo_type)
            query = query.filter(Meteo.meteo_type == meteo_type_enum)
        if code:
            query = query.filter(Meteo.code == code)
        if start_date:
            query = query.filter(Meteo.date >= start_date)
        if end_date:
            query = query.filter(Meteo.date <= end_date)
        # Order by code and date for consistent pagination
        results = query.order_by(Meteo.code, Meteo.date).offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} meteo records (code={code}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching meteo records: {str(e)}", exc_info=True)
        raise


def create_snow(db: Session, bulk_data: SnowBulkCreate) -> List[Snow]:
    """Create or update multiple snow records in bulk (upsert based on snow_type, code, date)"""
    try:
        incoming = [item.model_dump() for item in bulk_data.data]
        keys = {(i["snow_type"], i["code"], i["date"]) for i in incoming}

        existing_map = {
            (r.snow_type, r.code, r.date): r
            for r in db.query(Snow).filter(
                tuple_(Snow.snow_type, Snow.code, Snow.date).in_(keys)
            ).all()
        }

        db_snows = []
        changed = []
        for data in incoming:
            key = (data["snow_type"], data["code"], data["date"])
            existing = existing_map.get(key)

            if existing:
                if _has_changes(existing, data):
                    for k, v in data.items():
                        setattr(existing, k, v)
                    changed.append(existing)
                    logger.info(f"Updated snow: {key}")
                else:
                    logger.debug(f"Skipped unchanged snow: {key}")
                db_snows.append(existing)
            else:
                new = Snow(**data)
                db.add(new)
                changed.append(new)
                db_snows.append(new)
                logger.info(f"Created snow: {key}")

        if changed:
            db.commit()
            for s in changed:
                db.refresh(s)

        logger.info(f"Processed {len(db_snows)} snow records in bulk")
        return db_snows

    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating snow records in bulk: {str(e)}", exc_info=True)
        raise


def get_snow(db: Session, snow_type: Optional[str] = None, code: Optional[str] = None,
               start_date: Optional[str] = None, end_date: Optional[str] = None,
               skip: int = 0, limit: int = 100) -> List[Snow]:
    """Get snow records with optional filtering and pagination"""
    try:
        query = db.query(Snow)
        if snow_type:
            # Convert string to SnowType enum for proper comparison
            snow_type_enum = SnowType(snow_type)
            query = query.filter(Snow.snow_type == snow_type_enum)
        if code:
            query = query.filter(Snow.code == code)
        if start_date:
            query = query.filter(Snow.date >= start_date)
        if end_date:
            query = query.filter(Snow.date <= end_date)
        results = query.offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} snow records (code={code}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching snow records: {str(e)}", exc_info=True)
        raise
