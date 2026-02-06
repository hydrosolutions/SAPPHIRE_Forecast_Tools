from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from app import models, schemas
from app.logger import logger
from typing import Optional


def get_tasks(db: Session, skip: int = 0, limit: int = 100) -> list[models.Task]:
    """Get all tasks with pagination"""
    try:
        return db.query(models.Task).offset(skip).limit(limit).all()
    except SQLAlchemyError as e:
        logger.error(f"Error fetching tasks: {str(e)}")
        raise


def get_tasks_count(db: Session) -> int:
    """Get total count of tasks"""
    try:
        return db.query(models.Task).count()
    except SQLAlchemyError as e:
        logger.error(f"Error counting tasks: {str(e)}")
        raise


def get_task(db: Session, task_id: int) -> Optional[models.Task]:
    """Get a single task by ID"""
    try:
        return db.query(models.Task).filter(models.Task.id == task_id).first()
    except SQLAlchemyError as e:
        logger.error(f"Error fetching task {task_id}: {str(e)}")
        raise


def create_task(db: Session, task: schemas.TaskCreate) -> models.Task:
    """Create a new task"""
    try:
        db_task = models.Task(**task.model_dump())
        db.add(db_task)
        db.commit()
        db.refresh(db_task)
        logger.info(f"Created task with ID: {db_task.id}")
        return db_task
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error creating task: {str(e)}")
        raise


def update_task(
    db: Session,
    task_id: int,
    task: schemas.TaskUpdate
) -> Optional[models.Task]:
    """Update an existing task"""
    try:
        db_task = get_task(db, task_id)
        if db_task:
            update_data = task.model_dump(exclude_unset=True)
            for key, value in update_data.items():
                setattr(db_task, key, value)
            db.commit()
            db.refresh(db_task)
            logger.info(f"Updated task with ID: {task_id}")
        return db_task
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error updating task {task_id}: {str(e)}")
        raise


def delete_task(db: Session, task_id: int) -> Optional[models.Task]:
    """Delete a task"""
    try:
        db_task = get_task(db, task_id)
        if db_task:
            db.delete(db_task)
            db.commit()
            logger.info(f"Deleted task with ID: {task_id}")
        return db_task
    except SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error deleting task {task_id}: {str(e)}")
        raise
