import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.main import app
from app.database import Base, get_db
from app.config import get_settings

settings = get_settings()

# Use test database
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


@pytest.fixture(scope="function")
def client(db_session):
    """Create a test client with overridden database dependency"""

    def override_get_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_health_check(client):
    """Test the health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "service" in data


def test_root_endpoint(client):
    """Test the root endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    assert "message" in response.json()


def test_create_task(client):
    """Test creating a task"""
    response = client.post(
        "/tasks/",
        json={
            "title": "Test Task",
            "description": "Test Description",
            "completed": False
        }
    )
    assert response.status_code == 201
    data = response.json()
    assert data["title"] == "Test Task"
    assert data["description"] == "Test Description"
    assert data["completed"] is False
    assert "id" in data
    assert "created_at" in data


def test_read_tasks_empty(client):
    """Test reading tasks when database is empty"""
    response = client.get("/tasks/")
    assert response.status_code == 200
    data = response.json()
    assert data["tasks"] == []
    assert data["total"] == 0
    assert data["skip"] == 0
    assert data["limit"] == 100


def test_read_tasks(client):
    """Test reading all tasks"""
    # Create tasks first
    client.post("/tasks/", json={"title": "Task 1", "description": "Desc 1"})
    client.post("/tasks/", json={"title": "Task 2", "description": "Desc 2"})

    response = client.get("/tasks/")
    assert response.status_code == 200
    data = response.json()
    assert len(data["tasks"]) == 2
    assert data["total"] == 2


def test_read_task(client):
    """Test reading a specific task"""
    # Create a task
    create_response = client.post(
        "/tasks/",
        json={"title": "Task 1", "description": "Desc 1"}
    )
    task_id = create_response.json()["id"]

    # Get the task
    response = client.get(f"/tasks/{task_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == task_id
    assert data["title"] == "Task 1"


def test_read_task_not_found(client):
    """Test reading a non-existent task"""
    response = client.get("/tasks/9999")
    assert response.status_code == 404
    data = response.json()
    assert "not found" in data["detail"].lower()


def test_update_task(client):
    """Test updating a task"""
    # Create a task
    create_response = client.post(
        "/tasks/",
        json={"title": "Original Title", "description": "Original Desc"}
    )
    task_id = create_response.json()["id"]

    # Update the task
    response = client.put(
        f"/tasks/{task_id}",
        json={"title": "Updated Title", "completed": True}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["title"] == "Updated Title"
    assert data["completed"] is True
    assert data["description"] == "Original Desc"  # Should remain unchanged


def test_update_task_not_found(client):
    """Test updating a non-existent task"""
    response = client.put(
        "/tasks/9999",
        json={"title": "Updated Title"}
    )
    assert response.status_code == 404
    data = response.json()
    assert "not found" in data["detail"].lower()


def test_delete_task(client):
    """Test deleting a task"""
    # Create a task
    create_response = client.post(
        "/tasks/",
        json={"title": "Task to Delete", "description": "Will be deleted"}
    )
    task_id = create_response.json()["id"]

    # Delete the task
    response = client.delete(f"/tasks/{task_id}")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert data["task_id"] == task_id

    # Verify deletion
    get_response = client.get(f"/tasks/{task_id}")
    assert get_response.status_code == 404


def test_delete_task_not_found(client):
    """Test deleting a non-existent task"""
    response = client.delete("/tasks/9999")
    assert response.status_code == 404
    data = response.json()
    assert "not found" in data["detail"].lower()


def test_pagination(client):
    """Test pagination with skip and limit"""
    # Create multiple tasks
    for i in range(5):
        client.post("/tasks/", json={"title": f"Task {i + 1}"})

    # Test skip and limit
    response = client.get("/tasks/?skip=2&limit=2")
    assert response.status_code == 200
    data = response.json()
    assert len(data["tasks"]) == 2
    assert data["total"] == 5
    assert data["skip"] == 2
    assert data["limit"] == 2
    assert data["tasks"][0]["title"] == "Task 3"
    assert data["tasks"][1]["title"] == "Task 4"
