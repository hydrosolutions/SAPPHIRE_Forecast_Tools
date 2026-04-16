"""
Pytest configuration for api-gateway service tests.

IMPORTANT: All environment variables must be set before any app imports
because app/config.py reads them at module level via pydantic-settings.
"""

import os
import sys
from pathlib import Path

# --- Environment setup (before ANY app imports) ---
os.environ["REQUEST_TIMEOUT"] = "5"
os.environ["HEALTH_CHECK_TIMEOUT"] = "3"
os.environ["API_KEY_ENABLED"] = "false"
os.environ["API_KEY"] = "test-api-key"
os.environ["RATE_LIMIT_ENABLED"] = "false"
os.environ["RATE_LIMIT"] = "100"
os.environ["PREPROCESSING_API_URL"] = "http://localhost:8002"
os.environ["POSTPROCESSING_API_URL"] = "http://localhost:8003"
os.environ["USER_API_URL"] = "http://localhost:8004"
os.environ["AUTH_API_URL"] = "http://localhost:8005"

# Add project root so `from app.xxx import ...` works
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """FastAPI TestClient for the api-gateway app."""
    from app.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def client_with_api_key():
    """TestClient with API key authentication enabled."""
    from app.config import settings
    from app.main import app

    original = settings.api_key_enabled
    settings.api_key_enabled = True
    with TestClient(app) as c:
        yield c
    settings.api_key_enabled = original
