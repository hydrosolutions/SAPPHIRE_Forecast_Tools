from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from typing import List, Optional

from app.models import Forecast, LongForecast, LRForecast, SkillMetric
from app.schemas import ForecastBulkCreate, LongForecastBulkCreate, LRForecastBulkCreate, SkillMetricBulkCreate
from app.logger import logger


def create_forecast(db: Session, bulk_data: ForecastBulkCreate) -> List[Forecast]:
    """Create or update multiple forecasts in bulk (upsert based on horizon_type, code, model_type, date)"""
    try:
        db_forecasts = []

        for item in bulk_data.data:
            # Check if a record with the same (horizon_type, code, model_type, date) exists
            existing_forecast = db.query(Forecast).filter(
                Forecast.horizon_type == item.horizon_type,
                Forecast.code == item.code,
                Forecast.model_type == item.model_type,
                Forecast.date == item.date,
                Forecast.target == item.target
            ).first()

            if existing_forecast:
                # Update existing record
                for key, value in item.model_dump().items():
                    setattr(existing_forecast, key, value)
                db_forecasts.append(existing_forecast)
                logger.info(f"Updated forecast: {item.horizon_type}, {item.code}, {item.model_type}, {item.date}, {item.target}")
            else:
                # Create new record
                new_forecast = Forecast(**item.model_dump())
                db.add(new_forecast)
                db_forecasts.append(new_forecast)
                logger.info(f"Created forecast: {item.horizon_type}, {item.code}, {item.model_type}, {item.date}, {item.target}")

        db.commit()

        # Refresh all forecasts to get updated state
        for forecast in db_forecasts:
            db.refresh(forecast)

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
        db_long_forecasts = []

        for item in bulk_data.data:
            # Check if a record with the same (horizon_type, horizon_value, code, date, model_type, valid_from, valid_to) exists
            existing_long_forecast = db.query(LongForecast).filter(
                LongForecast.horizon_type == item.horizon_type,
                LongForecast.horizon_value == item.horizon_value,
                LongForecast.code == item.code,
                LongForecast.date == item.date,
                LongForecast.model_type == item.model_type,
                LongForecast.valid_from == item.valid_from,
                LongForecast.valid_to == item.valid_to
            ).first()

            if existing_long_forecast:
                # Update existing record
                for key, value in item.model_dump().items():
                    setattr(existing_long_forecast, key, value)
                db_long_forecasts.append(existing_long_forecast)
                logger.info(f"Updated long forecast: {item.horizon_type}, {item.horizon_value}, {item.code}, {item.date}, {item.model_type}, {item.valid_from}, {item.valid_to}")
            else:
                # Create new record
                new_long_forecast = LongForecast(**item.model_dump())
                db.add(new_long_forecast)
                db_long_forecasts.append(new_long_forecast)
                logger.info(f"Created long forecast: {item.horizon_type}, {item.horizon_value}, {item.code}, {item.date}, {item.model_type}, {item.valid_from}, {item.valid_to}")

        db.commit()

        # Refresh all long forecasts to get updated state
        for long_forecast in db_long_forecasts:
            db.refresh(long_forecast)

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
        db_lr_forecasts = []

        for item in bulk_data.data:
            # Check if a record with the same (horizon_type, code, date) exists
            existing_lr_forecast = db.query(LRForecast).filter(
                LRForecast.horizon_type == item.horizon_type,
                LRForecast.code == item.code,
                LRForecast.date == item.date
            ).first()

            if existing_lr_forecast:
                # Update existing record
                for key, value in item.model_dump().items():
                    setattr(existing_lr_forecast, key, value)
                db_lr_forecasts.append(existing_lr_forecast)
                logger.info(f"Updated LR forecast: {item.horizon_type}, {item.code}, {item.date}")
            else:
                # Create new record
                new_lr_forecast = LRForecast(**item.model_dump())
                db.add(new_lr_forecast)
                db_lr_forecasts.append(new_lr_forecast)
                logger.info(f"Created LR forecast: {item.horizon_type}, {item.code}, {item.date}")

        db.commit()

        # Refresh all LR forecasts to get updated state
        for lr_forecast in db_lr_forecasts:
            db.refresh(lr_forecast)

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
    """Create or update multiple skill metrics in bulk (upsert based on horizon_type, code, model_type, date)"""
    try:
        db_skill_metrics = []

        for item in bulk_data.data:
            # Check if a record with the same (horizon_type, code, model_type, date) exists
            existing_skill_metric = db.query(SkillMetric).filter(
                SkillMetric.horizon_type == item.horizon_type,
                SkillMetric.code == item.code,
                SkillMetric.model_type == item.model_type,
                SkillMetric.date == item.date
            ).first()

            if existing_skill_metric:
                # Update existing record
                for key, value in item.model_dump().items():
                    setattr(existing_skill_metric, key, value)
                db_skill_metrics.append(existing_skill_metric)
                logger.info(f"Updated skill metric: {item.horizon_type}, {item.code}, {item.model_type}, {item.date}")
            else:
                # Create new record
                new_skill_metric = SkillMetric(**item.model_dump())
                db.add(new_skill_metric)
                db_skill_metrics.append(new_skill_metric)
                logger.info(f"Created skill metric: {item.horizon_type}, {item.code}, {item.model_type}, {item.date}")

        db.commit()

        # Refresh all skill metrics to get updated state
        for skill_metric in db_skill_metrics:
            db.refresh(skill_metric)

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
