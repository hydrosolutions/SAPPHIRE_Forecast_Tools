from sqlalchemy import tuple_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from typing import List, Optional

from app.models import Forecast, LongForecast, LRForecast, SkillMetric, Bulletin, LRVisibility
from app.schemas import ForecastBulkCreate, LongForecastBulkCreate, LRForecastBulkCreate, SkillMetricBulkCreate, BulletinBulkCreate, LRVisibilityBulkCreate
from app.logger import logger


def _has_changes(existing, incoming_data: dict) -> bool:
    """Return True if any field in incoming_data differs from the existing ORM record."""
    return any(getattr(existing, k) != v for k, v in incoming_data.items())


def create_forecast(db: Session, bulk_data: ForecastBulkCreate) -> List[Forecast]:
    """Create or update multiple forecasts in bulk (upsert based on horizon_type, code, model_type, date, target)"""
    try:
        incoming = [item.model_dump() for item in bulk_data.data]
        keys = {(i["horizon_type"], i["code"], i["model_type"], i["date"], i["target"]) for i in incoming}

        existing_map = {
            (r.horizon_type, r.code, r.model_type, r.date, r.target): r
            for r in db.query(Forecast).filter(
                tuple_(Forecast.horizon_type, Forecast.code, Forecast.model_type, Forecast.date, Forecast.target).in_(keys)
            ).all()
        }

        db_forecasts = []
        changed = []
        for data in incoming:
            key = (data["horizon_type"], data["code"], data["model_type"], data["date"], data["target"])
            existing = existing_map.get(key)

            if existing:
                if _has_changes(existing, data):
                    for k, v in data.items():
                        setattr(existing, k, v)
                    changed.append(existing)
                    logger.info(f"Updated forecast: {key}")
                else:
                    logger.debug(f"Skipped unchanged forecast: {key}")
                db_forecasts.append(existing)
            else:
                new = Forecast(**data)
                db.add(new)
                changed.append(new)
                db_forecasts.append(new)
                logger.info(f"Created forecast: {key}")

        if changed:
            db.commit()
            for f in changed:
                db.refresh(f)

        logger.info(f"Processed {len(db_forecasts)} forecasts in bulk")
        return db_forecasts
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating/updating forecasts in bulk: {str(e)}", exc_info=True)
        raise


def get_forecast(
    db: Session,
    horizon: Optional[str] = None,
    code: Optional[str] = None,
    model: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    start_target: Optional[str] = None,
    end_target: Optional[str] = None,
    target: Optional[str] = None,
    skip: int = 0,
    limit: int = 100
) -> List[Forecast]:
    """Retrieve forecasts with optional filtering by horizon_type, code, model_type, date range, and target range"""
    try:
        query = db.query(Forecast)
        if horizon:
            query = query.filter(Forecast.horizon_type == horizon)
        if code:
            query = query.filter(Forecast.code == code)
        if model:
            query = query.filter(Forecast.model_type == model)
        if start_date:
            query = query.filter(Forecast.date >= start_date)
        if end_date:
            query = query.filter(Forecast.date <= end_date)
        if start_target:
            query = query.filter(Forecast.target >= start_target)
        if end_target:
            query = query.filter(Forecast.target <= end_target)
        if target and target == "null":
            query = query.filter(Forecast.target.is_(None))

        results = query.offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} forecasts (code={code}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching forecasts: {str(e)}", exc_info=True)
        raise


def create_long_forecast(db: Session, bulk_data: LongForecastBulkCreate) -> List[LongForecast]:
    """Create or update multiple long forecasts in bulk (upsert based on horizon_type, horizon_value, code, date, model_type, valid_from, valid_to)"""
    try:
        incoming = [item.model_dump() for item in bulk_data.data]
        keys = {
            (i["horizon_type"], i["horizon_value"], i["code"], i["date"], i["model_type"], i["valid_from"], i["valid_to"])
            for i in incoming
        }

        existing_map = {
            (r.horizon_type, r.horizon_value, r.code, r.date, r.model_type, r.valid_from, r.valid_to): r
            for r in db.query(LongForecast).filter(
                tuple_(
                    LongForecast.horizon_type, LongForecast.horizon_value, LongForecast.code,
                    LongForecast.date, LongForecast.model_type, LongForecast.valid_from, LongForecast.valid_to
                ).in_(keys)
            ).all()
        }

        db_long_forecasts = []
        changed = []
        for data in incoming:
            key = (data["horizon_type"], data["horizon_value"], data["code"], data["date"], data["model_type"], data["valid_from"], data["valid_to"])
            existing = existing_map.get(key)

            if existing:
                if _has_changes(existing, data):
                    for k, v in data.items():
                        setattr(existing, k, v)
                    changed.append(existing)
                    logger.info(f"Updated long forecast: {key}")
                else:
                    logger.debug(f"Skipped unchanged long forecast: {key}")
                db_long_forecasts.append(existing)
            else:
                new = LongForecast(**data)
                db.add(new)
                changed.append(new)
                db_long_forecasts.append(new)
                logger.info(f"Created long forecast: {key}")

        if changed:
            db.commit()
            for lf in changed:
                db.refresh(lf)

        logger.info(f"Processed {len(db_long_forecasts)} long forecasts in bulk")
        return db_long_forecasts
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating/updating long forecasts in bulk: {str(e)}", exc_info=True)
        raise


def get_long_forecast(
    db: Session,
    horizon_type: Optional[str] = None,
    horizon_value: Optional[int] = None,
    code: Optional[str] = None,
    model: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    valid_from: Optional[str] = None,
    valid_to: Optional[str] = None,
    skip: int = 0,
    limit: int = 100
) -> List[LongForecast]:
    """Retrieve long forecasts with optional filtering by horizon type and value, code, model_type, date range, valid_from and valid_to"""
    try:
        query = db.query(LongForecast)
        if horizon_type:
            query = query.filter(LongForecast.horizon_type == horizon_type)
        if horizon_value is not None:
            query = query.filter(LongForecast.horizon_value == horizon_value)
        if code:
            query = query.filter(LongForecast.code == code)
        if model:
            query = query.filter(LongForecast.model_type == model)
        if start_date:
            query = query.filter(LongForecast.date >= start_date)
        if end_date:
            query = query.filter(LongForecast.date <= end_date)
        if valid_from:
            query = query.filter(LongForecast.valid_from >= valid_from)
        if valid_to:
            query = query.filter(LongForecast.valid_to <= valid_to)

        results = query.offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} long forecasts (code={code}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching long forecasts: {str(e)}", exc_info=True)
        raise


def create_lr_forecast(db: Session, bulk_data: LRForecastBulkCreate) -> List[LRForecast]:
    """Create or update multiple LR forecasts in bulk (upsert based on horizon_type, code, date)"""
    try:
        incoming = [item.model_dump() for item in bulk_data.data]
        keys = {(i["horizon_type"], i["code"], i["date"]) for i in incoming}

        existing_map = {
            (r.horizon_type, r.code, r.date): r
            for r in db.query(LRForecast).filter(
                tuple_(LRForecast.horizon_type, LRForecast.code, LRForecast.date).in_(keys)
            ).all()
        }

        db_lr_forecasts = []
        changed = []
        for data in incoming:
            key = (data["horizon_type"], data["code"], data["date"])
            existing = existing_map.get(key)

            if existing:
                if _has_changes(existing, data):
                    for k, v in data.items():
                        setattr(existing, k, v)
                    changed.append(existing)
                    logger.info(f"Updated LR forecast: {key}")
                else:
                    logger.debug(f"Skipped unchanged LR forecast: {key}")
                db_lr_forecasts.append(existing)
            else:
                new = LRForecast(**data)
                db.add(new)
                changed.append(new)
                db_lr_forecasts.append(new)
                logger.info(f"Created LR forecast: {key}")

        if changed:
            db.commit()
            for lrf in changed:
                db.refresh(lrf)

        logger.info(f"Processed {len(db_lr_forecasts)} LR forecasts in bulk")
        return db_lr_forecasts
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating/updating LR forecasts in bulk: {str(e)}", exc_info=True)
        raise


def get_lr_forecast(
    db: Session,
    horizon: Optional[str] = None,
    code: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    skip: int = 0,
    limit: int = 100
) -> List[LRForecast]:
    """Retrieve LR forecasts with optional filtering by horizon_type, code, and date range"""
    try:
        query = db.query(LRForecast)
        if horizon:
            query = query.filter(LRForecast.horizon_type == horizon)
        if code:
            query = query.filter(LRForecast.code == code)
        if start_date:
            query = query.filter(LRForecast.date >= start_date)
        if end_date:
            query = query.filter(LRForecast.date <= end_date)

        results = query.offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} LR forecasts (code={code}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching LR forecasts: {str(e)}", exc_info=True)
        raise


def create_skill_metric(db: Session, bulk_data: SkillMetricBulkCreate) -> List[SkillMetric]:
    """Create or update multiple skill metrics in bulk (upsert based on horizon_type, code, model_type, date, horizon_in_year)"""
    try:
        incoming = [item.model_dump() for item in bulk_data.data]
        keys = {(i["horizon_type"], i["code"], i["model_type"], i["date"], i["horizon_in_year"]) for i in incoming}

        existing_map = {
            (r.horizon_type, r.code, r.model_type, r.date, r.horizon_in_year): r
            for r in db.query(SkillMetric).filter(
                tuple_(
                    SkillMetric.horizon_type, SkillMetric.code, SkillMetric.model_type,
                    SkillMetric.date, SkillMetric.horizon_in_year
                ).in_(keys)
            ).all()
        }

        db_skill_metrics = []
        changed = []
        for data in incoming:
            key = (data["horizon_type"], data["code"], data["model_type"], data["date"], data["horizon_in_year"])
            existing = existing_map.get(key)

            if existing:
                if _has_changes(existing, data):
                    for k, v in data.items():
                        setattr(existing, k, v)
                    changed.append(existing)
                    logger.info(f"Updated skill metric: {key}")
                else:
                    logger.debug(f"Skipped unchanged skill metric: {key}")
                db_skill_metrics.append(existing)
            else:
                new = SkillMetric(**data)
                db.add(new)
                changed.append(new)
                db_skill_metrics.append(new)
                logger.info(f"Created skill metric: {key}")

        if changed:
            db.commit()
            for sm in changed:
                db.refresh(sm)

        logger.info(f"Processed {len(db_skill_metrics)} skill metrics in bulk")
        return db_skill_metrics
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating/updating skill metrics in bulk: {str(e)}", exc_info=True)
        raise


def get_skill_metric(
    db: Session,
    horizon: Optional[str] = None,
    code: Optional[str] = None,
    model: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    skip: int = 0,
    limit: int = 100
) -> List[SkillMetric]:
    """Retrieve skill metrics with optional filtering by horizon_type, code, model_type, and date range"""
    try:
        query = db.query(SkillMetric)
        if horizon:
            query = query.filter(SkillMetric.horizon_type == horizon)
        if code:
            query = query.filter(SkillMetric.code == code)
        if model:
            query = query.filter(SkillMetric.model_type == model)
        if start_date:
            query = query.filter(SkillMetric.date >= start_date)
        if end_date:
            query = query.filter(SkillMetric.date <= end_date)

        results = query.offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} skill metrics (code={code}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching skill metrics: {str(e)}", exc_info=True)
        raise


def create_bulletin(db: Session, bulk_data: BulletinBulkCreate) -> List[Bulletin]:
    """Create or update multiple bulletins in bulk (upsert based on horizon_type, year, horizon_value, code, model_type)"""
    try:
        incoming = [item.model_dump() for item in bulk_data.data]
        keys = {(i["horizon_type"], i["year"], i["horizon_value"], i["code"], i["model_type"]) for i in incoming}

        existing_map = {
            (r.horizon_type, r.year, r.horizon_value, r.code, r.model_type): r
            for r in db.query(Bulletin).filter(
                tuple_(
                    Bulletin.horizon_type, Bulletin.year, Bulletin.horizon_value,
                    Bulletin.code, Bulletin.model_type
                ).in_(keys)
            ).all()
        }

        db_bulletins = []
        changed = []
        for data in incoming:
            key = (data["horizon_type"], data["year"], data["horizon_value"], data["code"], data["model_type"])
            existing = existing_map.get(key)

            if existing:
                if _has_changes(existing, data):
                    for k, v in data.items():
                        setattr(existing, k, v)
                    changed.append(existing)
                    logger.info(f"Updated bulletin: {key}")
                else:
                    logger.debug(f"Skipped unchanged bulletin: {key}")
                db_bulletins.append(existing)
            else:
                new = Bulletin(**data)
                db.add(new)
                changed.append(new)
                db_bulletins.append(new)
                logger.info(f"Created bulletin: {key}")

        if changed:
            db.commit()
            for b in changed:
                db.refresh(b)

        logger.info(f"Processed {len(db_bulletins)} bulletins in bulk")
        return db_bulletins
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating/updating bulletins in bulk: {str(e)}", exc_info=True)
        raise


def get_bulletin(
    db: Session,
    horizon: Optional[str] = None,
    year: Optional[int] = None,
    horizon_value: Optional[int] = None,
    skip: int = 0,
    limit: int = 100
) -> List[Bulletin]:
    """Retrieve bulletins with optional filtering by horizon_type, year, horizon_value, code, and model_type"""
    try:
        query = db.query(Bulletin)
        if horizon:
            query = query.filter(Bulletin.horizon_type == horizon)
        if year:
            query = query.filter(Bulletin.year == year)
        if horizon_value is not None:
            query = query.filter(Bulletin.horizon_value == horizon_value)

        results = query.offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} bulletins (skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching bulletins: {str(e)}", exc_info=True)
        raise


def delete_bulletin(
    db: Session,
    horizon: str,
    year: int,
    horizon_value: int,
    code: str,
    model: str,
) -> bool:
    """Delete a bulletin by its unique constraint fields. Returns True if deleted, False if not found."""
    try:
        existing_bulletin = db.query(Bulletin).filter(
            Bulletin.horizon_type == horizon,
            Bulletin.year == year,
            Bulletin.horizon_value == horizon_value,
            Bulletin.code == code,
            Bulletin.model_type == model
        ).first()

        if not existing_bulletin:
            return False

        db.delete(existing_bulletin)
        db.commit()
        logger.info(f"Deleted bulletin: {horizon}, {year}, {horizon_value}, {code}, {model}")
        return True
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error deleting bulletin: {str(e)}", exc_info=True)
        raise


def create_lr_visibility(db: Session, bulk_data: LRVisibilityBulkCreate) -> List[LRVisibility]:
    """Create or update multiple LR visibility records in bulk (upsert based on horizon_type, code, month, horizon_value)"""
    try:
        incoming = [item.model_dump() for item in bulk_data.data]
        keys = {(i["horizon_type"], i["code"], i["month"], i["horizon_value"], i["year"]) for i in incoming}

        existing_map = {
            (r.horizon_type, r.code, r.month, r.horizon_value, r.year): r
            for r in db.query(LRVisibility).filter(
                tuple_(
                    LRVisibility.horizon_type, LRVisibility.code, LRVisibility.month,
                    LRVisibility.horizon_value, LRVisibility.year
                ).in_(keys)
            ).all()
        }

        db_lr_visibility = []
        changed = []
        for data in incoming:
            key = (data["horizon_type"], data["code"], data["month"], data["horizon_value"], data["year"])
            existing = existing_map.get(key)

            if existing:
                if _has_changes(existing, data):
                    for k, v in data.items():
                        setattr(existing, k, v)
                    changed.append(existing)
                    logger.info(f"Updated LR visibility: {key}")
                else:
                    logger.debug(f"Skipped unchanged LR visibility: {key}")
                db_lr_visibility.append(existing)
            else:
                new = LRVisibility(**data)
                db.add(new)
                changed.append(new)
                db_lr_visibility.append(new)
                logger.info(f"Created LR visibility: {key}")

        if changed:
            db.commit()
            for lrv in changed:
                db.refresh(lrv)

        logger.info(f"Processed {len(db_lr_visibility)} LR visibility records in bulk")
        return db_lr_visibility
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating/updating LR visibility records in bulk: {str(e)}", exc_info=True)
        raise


def get_lr_visibility(
    db: Session,
    horizon: Optional[str] = None,
    code: Optional[str] = None,
    month: Optional[int] = None,
    horizon_value: Optional[int] = None,
    skip: int = 0,
    limit: int = 100
) -> List[LRVisibility]:
    """Retrieve LR visibility records with optional filtering by horizon_type, code, month, horizon_value, and year"""
    try:
        query = db.query(LRVisibility)
        if horizon:
            query = query.filter(LRVisibility.horizon_type == horizon)
        if code:
            query = query.filter(LRVisibility.code == code)
        if month is not None:
            query = query.filter(LRVisibility.month == month)
        if horizon_value is not None:
            query = query.filter(LRVisibility.horizon_value == horizon_value)

        results = query.offset(skip).limit(limit).all()
        logger.info(f"Fetched {len(results)} LR visibility records (code={code}, month={month}, horizon_value={horizon_value}, skip={skip}, limit={limit})")
        return results
    except SQLAlchemyError as e:
        logger.error(f"Error fetching LR visibility records: {str(e)}", exc_info=True)
        raise
