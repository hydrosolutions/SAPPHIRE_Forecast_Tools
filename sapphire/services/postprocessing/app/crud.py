from sqlalchemy import and_
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from typing import List, Optional

from app.models import Forecast, LongForecast, LRForecast, SkillMetric
from app.schemas import ForecastBulkCreate, LongForecastBulkCreate, LRForecastBulkCreate, SkillMetricBulkCreate
from app.logger import logger

try:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    PG_AVAILABLE = True
except ImportError:
    PG_AVAILABLE = False


def _bulk_upsert(db: Session, model, bulk_items, unique_keys, constraint_name):
    """Generic batch upsert using PostgreSQL ON CONFLICT DO UPDATE.

    Falls back to N+1 ORM pattern if PostgreSQL dialect is not available.

    Args:
        db: SQLAlchemy session.
        model: ORM model class (e.g. Forecast).
        bulk_items: List of Pydantic schema objects with .model_dump().
        unique_keys: List of column names forming the unique constraint.
        constraint_name: Name of the unique constraint in the DB.

    Returns:
        List of ORM objects (freshly queried after upsert).
    """
    if not bulk_items:
        return []

    records = [item.model_dump() for item in bulk_items]

    # Determine which columns to update (all except the unique keys and 'id')
    all_keys = set(records[0].keys())
    update_cols = all_keys - set(unique_keys) - {'id'}

    use_pg = PG_AVAILABLE and 'postgresql' in str(db.bind.url)

    if use_pg:
        # PostgreSQL batch upsert: 1 INSERT + 1 SELECT instead of N+1
        stmt = pg_insert(model).values(records)
        if update_cols:
            stmt = stmt.on_conflict_do_update(
                constraint=constraint_name,
                set_={col: stmt.excluded[col] for col in update_cols},
            )
        else:
            stmt = stmt.on_conflict_do_nothing()
        db.execute(stmt)
        db.commit()

        # Bulk SELECT to return the upserted objects
        # Build filter for all unique key combinations
        if len(records) == 1:
            r = records[0]
            conditions = and_(
                *[getattr(model, k) == r[k] for k in unique_keys]
            )
            results = db.query(model).filter(conditions).all()
        else:
            # For large batches, query all matching records efficiently
            # Use IN-list filtering on the first unique key, then filter in Python
            first_key = unique_keys[0]
            first_key_values = list({r[first_key] for r in records})
            candidates = db.query(model).filter(
                getattr(model, first_key).in_(first_key_values)
            ).all()

            # Build a set of unique key tuples for fast lookup
            record_keys = {
                tuple(r[k] for k in unique_keys) for r in records
            }
            results = [
                obj for obj in candidates
                if tuple(getattr(obj, k) for k in unique_keys) in record_keys
            ]

        logger.info(f"Batch upserted {len(records)} {model.__tablename__} records")
        return results
    else:
        # Fallback: N+1 ORM pattern (for SQLite tests or non-PG backends)
        return _fallback_upsert(db, model, bulk_items, unique_keys)


def _fallback_upsert(db, model, bulk_items, unique_keys):
    """Original N+1 upsert pattern for non-PostgreSQL backends."""
    db_objects = []
    for item in bulk_items:
        data = item.model_dump()
        filters = [getattr(model, k) == data[k] for k in unique_keys]
        existing = db.query(model).filter(*filters).first()

        if existing:
            for key, value in data.items():
                setattr(existing, key, value)
            db_objects.append(existing)
        else:
            new_obj = model(**data)
            db.add(new_obj)
            db.flush()
            db_objects.append(new_obj)

    db.commit()
    for obj in db_objects:
        db.refresh(obj)
    return db_objects


def create_forecast(db: Session, bulk_data: ForecastBulkCreate) -> List[Forecast]:
    """Create or update multiple forecasts in bulk (upsert based on horizon_type, code, model_type, date, target)"""
    try:
        unique_keys = ['horizon_type', 'code', 'model_type', 'date', 'target']
        results = _bulk_upsert(
            db, Forecast, bulk_data.data, unique_keys,
            'uq_forecasts_horizon_code_model_date_target',
        )
        logger.info(f"Processed {len(results)} forecasts in bulk")
        return results
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
        unique_keys = ['horizon_type', 'horizon_value', 'code', 'date', 'model_type', 'valid_from', 'valid_to']
        results = _bulk_upsert(
            db, LongForecast, bulk_data.data, unique_keys,
            'uq_long_forecasts_horizon_type_value_code_date_model_from_to',
        )
        logger.info(f"Processed {len(results)} long forecasts in bulk")
        return results
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
        unique_keys = ['horizon_type', 'code', 'date']
        results = _bulk_upsert(
            db, LRForecast, bulk_data.data, unique_keys,
            'uq_lr_forecasts_horizon_code_date',
        )
        logger.info(f"Processed {len(results)} LR forecasts in bulk")
        return results
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
        unique_keys = ['horizon_type', 'code', 'model_type', 'date', 'horizon_in_year']
        results = _bulk_upsert(
            db, SkillMetric, bulk_data.data, unique_keys,
            'uq_skill_metrics_horizon_code_model_date_horizon',
        )
        logger.info(f"Processed {len(results)} skill metrics in bulk")
        return results
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
