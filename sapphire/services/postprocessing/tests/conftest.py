"""
Pytest configuration for postprocessing service tests.

IMPORTANT: DATABASE_URL must be set before any app imports because
app/database.py reads it at module level (line 7) and app/main.py
runs Base.metadata.create_all(bind=engine) at import time (line 14).
"""

import os
import sys
from pathlib import Path

# --- Environment setup (before ANY app imports) ---
os.environ["DATABASE_URL"] = "sqlite://"

# Add project root so `from app.xxx import ...` works
sys.path.insert(0, str(Path(__file__).parent.parent))
# Add tests dir so `from factories import ...` works
sys.path.insert(0, str(Path(__file__).parent))
# Keep app dir on path for existing test_data_migrator.py imports
sys.path.insert(0, str(Path(__file__).parent.parent / "app"))

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
