"""
HTTP endpoint tests for the auth service.

Tests the FastAPI endpoints via TestClient. Endpoints that delegate to
the user service are tested with mocked httpx calls.
"""

from unittest.mock import patch, AsyncMock

import pytest
from fastapi import HTTPException

from app import auth, crud


# ---------------------------------------------------------------------------
# Health endpoints
# ---------------------------------------------------------------------------

class TestHealthEndpoints:
    """Tests for root, /health, and /health/ready."""

    def test_root(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        body = resp.json()
        assert "message" in body
        assert body["docs"] == "/docs"
        assert body["health"] == "/health"

    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "healthy"

    def test_health_ready(self, client):
        resp = client.get("/health/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ready"
        assert body["database"] == "connected"


# ---------------------------------------------------------------------------
# Login endpoint
# ---------------------------------------------------------------------------

class TestLoginEndpoint:
    """Tests for POST /login."""

    _user = {
        "id": 1,
        "email": "test@example.com",
        "username": "testuser",
        "full_name": "Test User",
        "is_active": True,
        "is_superuser": False,
        "roles": [],
    }

    @patch("app.crud.authenticate_user", new_callable=AsyncMock)
    def test_login_success(self, mock_auth, client):
        mock_auth.return_value = self._user.copy()

        resp = client.post(
            "/login",
            data={"username": "testuser", "password": "testpass"},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert "access_token" in body
        assert "refresh_token" in body
        assert body["token_type"] == "bearer"
        assert "user" in body

    @patch("app.crud.authenticate_user", new_callable=AsyncMock)
    def test_login_wrong_password(self, mock_auth, client):
        mock_auth.return_value = None

        resp = client.post(
            "/login",
            data={"username": "testuser", "password": "wrongpass"},
        )

        assert resp.status_code == 401

    @patch("app.crud.authenticate_user", new_callable=AsyncMock)
    def test_login_inactive_user(self, mock_auth, client):
        inactive_user = self._user.copy()
        inactive_user["is_active"] = False
        mock_auth.return_value = inactive_user

        resp = client.post(
            "/login",
            data={"username": "testuser", "password": "testpass"},
        )

        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Register endpoint
# ---------------------------------------------------------------------------

class TestRegisterEndpoint:
    """Tests for POST /register."""

    _payload = {
        "email": "newuser@example.com",
        "username": "newuser",
        "password": "securepass123",
        "full_name": "New User",
    }

    _user = {
        "id": 2,
        "email": "newuser@example.com",
        "username": "newuser",
        "full_name": "New User",
        "is_active": True,
        "is_superuser": False,
        "roles": [],
    }

    @patch("app.crud.create_user_via_user_service", new_callable=AsyncMock)
    def test_register_success(self, mock_create, client):
        mock_create.return_value = self._user.copy()

        resp = client.post("/register", json=self._payload)

        assert resp.status_code == 201
        body = resp.json()
        assert "access_token" in body
        assert "refresh_token" in body
        assert body["token_type"] == "bearer"
        assert "user" in body

    @patch("app.crud.create_user_via_user_service", new_callable=AsyncMock)
    def test_register_duplicate_email(self, mock_create, client):
        mock_create.side_effect = HTTPException(
            status_code=400, detail="Email already registered"
        )

        resp = client.post("/register", json=self._payload)

        assert resp.status_code == 400
        assert "Email already registered" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Refresh endpoint
# ---------------------------------------------------------------------------

class TestRefreshEndpoint:
    """Tests for POST /refresh."""

    @patch("app.crud.verify_refresh_token")
    def test_refresh_token_success(self, mock_verify, client):
        token = auth.create_refresh_token(
            data={"sub": "test@example.com", "user_id": 1}
        )
        mock_verify.return_value = True

        resp = client.post("/refresh", json={"refresh_token": token})

        assert resp.status_code == 200
        body = resp.json()
        assert "access_token" in body
        assert body["token_type"] == "bearer"

    def test_refresh_invalid_token(self, client):
        resp = client.post(
            "/refresh", json={"refresh_token": "garbage.token.value"}
        )

        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Logout endpoint
# ---------------------------------------------------------------------------

class TestLogoutEndpoint:
    """Tests for POST /logout."""

    @patch("app.crud.verify_refresh_token")
    def test_logout_success(self, mock_verify, client, db_session):
        token = auth.create_refresh_token(
            data={"sub": "test@example.com", "user_id": 1}
        )
        mock_verify.return_value = True
        crud.store_refresh_token(db_session, user_id=1, token=token)

        resp = client.post("/logout", json={"refresh_token": token})

        assert resp.status_code == 200
        assert resp.json()["message"] == "Successfully logged out"
