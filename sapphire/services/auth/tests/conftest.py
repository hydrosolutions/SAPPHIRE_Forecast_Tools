"""
Pytest configuration for auth service tests.

IMPORTANT: Environment variables must be set before any app imports because
app/config.py reads them at module level via pydantic-settings, and
app/main.py runs Base.metadata.create_all(bind=engine) at import time.
"""

import os
import sys
from pathlib import Path

# --- Environment setup (before ANY app imports) ---
os.environ["DATABASE_URL"] = "sqlite://"
os.environ["USER_SERVICE_URL"] = "http://localhost:8004"
os.environ["JWT_SECRET_KEY"] = "test-secret-key-for-testing-only"
os.environ["JWT_ALGORITHM"] = "HS256"
os.environ["ACCESS_TOKEN_EXPIRE_MINUTES"] = "30"
os.environ["REFRESH_TOKEN_EXPIRE_DAYS"] = "7"
os.environ["LOG_LEVEL"] = "INFO"

# Add project root so `from app.xxx import ...` works
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi.testclient import TestClient

from app.database import Base, get_db
from app.main import app

# ---------------------------------------------------------------------------
# Test engine: SQLite in-memory with StaticPool so all threads/connections
# share the same database (required for TestClient which runs in a thread).
# ---------------------------------------------------------------------------
engine = create_engine(
    "sqlite://",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestSessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=engine
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def setup_database():
    """Create all tables before each test, drop after."""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def db_session():
    """Provide a fresh SQLAlchemy session for CRUD tests."""
    session = TestSessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client():
    """FastAPI TestClient with database dependency override."""
    def override_get_db():
        session = TestSessionLocal()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()
