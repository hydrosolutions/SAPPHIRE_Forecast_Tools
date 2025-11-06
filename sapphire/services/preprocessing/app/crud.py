from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from typing import Optional

from app.models import Runoff
from app.schemas import RunoffCreate, RunoffUpdate
from app.logger import logger


def create_runoff(db: Session, runoff: RunoffCreate) -> Runoff:
    """Create a new runoff or update if already exists based on unique constraint"""
    try:
        # Try to find existing runoff based on unique constraint (horizon_type, code, date)
        existing_runoff = db.query(Runoff).filter(
            Runoff.horizon_type == runoff.horizon_type,
            Runoff.code == runoff.code,
            Runoff.date == runoff.date
        ).first()

        if existing_runoff:
            # Update existing runoff
            for key, value in runoff.dict().items():
                setattr(existing_runoff, key, value)
            db.commit()
            db.refresh(existing_runoff)
            logger.info(f"Updated existing runoff with ID: {existing_runoff.id}")
            return existing_runoff
        else:
            # Create new runoff
            db_runoff = Runoff(**runoff.dict())
            db.add(db_runoff)
            db.commit()
            db.refresh(db_runoff)
            logger.info(f"Created runoff with ID: {db_runoff.id}")
            return db_runoff
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating runoff: {str(e)}")
        raise


def create_runoffs_bulk(db: Session, bulk_data) -> list[Runoff]:
    """Create or update multiple runoffs in bulk (upsert based on horizon_type, code, date)"""
    try:
        db_runoffs = []

        for item in bulk_data.data:
            # Check if a record with teh same (horizon_type, code, date) exists
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

        # db_runoffs = [Runoff(**item.dict()) for item in bulk_data.data]
        # db.add_all(db_runoffs)
        # db.commit()
        # for runoff in db_runoffs:
        #     db.refresh(runoff)
        # logger.info(f"Created {len(db_runoffs)} runoffs in bulk")
        # return db_runoffs
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating runoffs in bulk: {str(e)}", exc_info=True)
        raise


def get_runoffs(db: Session, horizon: Optional[str] = None, code: Optional[str] = None, start_date: Optional[str] = None,
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


def get_runoff(db: Session, runoff_id: int) -> Optional[Runoff]:
    """Get a single runoff by ID"""
    try:
        return db.query(Runoff).filter(Runoff.id == runoff_id).first()
    except SQLAlchemyError as e:
        logger.error(f"Error fetching runoff {runoff_id}: {str(e)}")
        raise


def update_runoff(
    db: Session,
    runoff_id: int,
    runoff: RunoffUpdate
) -> Optional[Runoff]:
    """Update an existing runoff"""
    try:
        db_runoff = get_runoff(db, runoff_id)
        if db_runoff:
            update_data = runoff.dict(exclude_unset=True)
            for key, value in update_data.items():
                setattr(db_runoff, key, value)
            db.commit()
            db.refresh(db_runoff)
            logger.info(f"Updated runoff with ID: {runoff_id}")
            return db_runoff
        return None
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error updating runoff {runoff_id}: {str(e)}")
        raise


def delete_runoff(db: Session, runoff_id: int) -> Optional[Runoff]:
    """Delete a runoff"""
    try:
        db_runoff = get_runoff(db, runoff_id)
        if db_runoff:
            db.delete(db_runoff)
            db.commit()
            logger.info(f"Deleted runoff with ID: {runoff_id}")
        return db_runoff
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error deleting runoff {runoff_id}: {str(e)}")
        raise
