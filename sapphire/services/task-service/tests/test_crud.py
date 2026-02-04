import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.database import Base
from app import models, schemas, crud
from app.config import get_settings

settings = get_settings()
TEST_DATABASE_URL = settings.test_database_url or "postgresql://postgres:password@db:5432/taskdb_test"

engine = create_engine(TEST_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(scope="function", autouse=True)
def setup_database():
    """Create tables before each test and drop after"""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def db_session():
    """Create a database session for tests"""
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()


def test_create_task(db_session):
    """Test CRUD create_task function"""
    task_data = schemas.TaskCreate(
        title="Test Task",
        description="Test Description",
        completed=False
    )

    task = crud.create_task(db_session, task_data)

    assert task.id is not None
    assert task.title == "Test Task"
    assert task.description == "Test Description"
    assert task.completed is False
    assert task.created_at is not None


def test_get_task(db_session):
    """Test CRUD get_task function"""
    # Create a task first
    task_data = schemas.TaskCreate(title="Test Task")
    created_task = crud.create_task(db_session, task_data)

    # Get the task
    retrieved_task = crud.get_task(db_session, created_task.id)

    assert retrieved_task is not None
    assert retrieved_task.id == created_task.id
    assert retrieved_task.title == "Test Task"


def test_get_task_not_found(db_session):
    """Test get_task with non-existent ID"""
    task = crud.get_task(db_session, 9999)
    assert task is None


def test_get_tasks(db_session):
    """Test CRUD get_tasks function"""
    # Create multiple tasks
    for i in range(5):
        task_data = schemas.TaskCreate(title=f"Task {i}")
        crud.create_task(db_session, task_data)

    # Get all tasks
    tasks = crud.get_tasks(db_session)

    assert len(tasks) == 5


def test_get_tasks_with_pagination(db_session):
    """Test get_tasks with skip and limit"""
    # Create tasks
    for i in range(10):
        task_data = schemas.TaskCreate(title=f"Task {i}")
        crud.create_task(db_session, task_data)

    # Get with pagination
    tasks = crud.get_tasks(db_session, skip=3, limit=4)

    assert len(tasks) == 4


def test_get_tasks_count(db_session):
    """Test get_tasks_count function"""
    # Create tasks
    for i in range(7):
        task_data = schemas.TaskCreate(title=f"Task {i}")
        crud.create_task(db_session, task_data)

    count = crud.get_tasks_count(db_session)
    assert count == 7


def test_update_task(db_session):
    """Test CRUD update_task function"""
    # Create a task
    task_data = schemas.TaskCreate(title="Original", description="Original Desc")
    created_task = crud.create_task(db_session, task_data)

    # Update the task
    update_data = schemas.TaskUpdate(title="Updated", completed=True)
    updated_task = crud.update_task(db_session, created_task.id, update_data)

    assert updated_task.title == "Updated"
    assert updated_task.completed is True
    assert updated_task.description == "Original Desc"  # Unchanged


def test_update_task_partial(db_session):
    """Test partial update of task"""
    # Create a task
    task_data = schemas.TaskCreate(title="Original", description="Desc")
    created_task = crud.create_task(db_session, task_data)

    # Update only completed status
    update_data = schemas.TaskUpdate(completed=True)
    updated_task = crud.update_task(db_session, created_task.id, update_data)

    assert updated_task.title == "Original"
    assert updated_task.completed is True


def test_update_task_not_found(db_session):
    """Test update_task with non-existent ID"""
    update_data = schemas.TaskUpdate(title="Updated")
    result = crud.update_task(db_session, 9999, update_data)
    assert result is None


def test_delete_task(db_session):
    """Test CRUD delete_task function"""
    # Create a task
    task_data = schemas.TaskCreate(title="To Delete")
    created_task = crud.create_task(db_session, task_data)

    # Delete the task
    deleted_task = crud.delete_task(db_session, created_task.id)

    assert deleted_task is not None
    assert deleted_task.id == created_task.id

    # Verify it's deleted
    retrieved_task = crud.get_task(db_session, created_task.id)
    assert retrieved_task is None


def test_delete_task_not_found(db_session):
    """Test delete_task with non-existent ID"""
    result = crud.delete_task(db_session, 9999)
    assert result is None
