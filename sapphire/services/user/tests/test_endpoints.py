"""
HTTP endpoint tests for the user service.

Tests the FastAPI endpoints via TestClient, exercising the full
request -> Pydantic validation -> CRUD -> response serialization path.
"""

import pytest


# -------------------------------------------------------------------
# Health endpoints
# -------------------------------------------------------------------

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


# -------------------------------------------------------------------
# User endpoints
# -------------------------------------------------------------------

class TestUserEndpoints:
    """Tests for user CRUD endpoints."""

    def _payload(self, **overrides):
        """Build a user creation payload."""
        defaults = {
            "email": "test@example.com",
            "username": "testuser",
            "password": "securepass123",
            "full_name": "Test User",
        }
        defaults.update(overrides)
        return defaults

    def test_create_user(self, client):
        resp = client.post("/users/", json=self._payload())

        assert resp.status_code == 201
        body = resp.json()
        assert body["email"] == "test@example.com"
        assert body["username"] == "testuser"
        assert body["full_name"] == "Test User"
        assert body["is_active"] is True
        assert body["is_superuser"] is False
        assert body["id"] is not None
        # Password must not appear in the response
        assert "password" not in body
        assert "hashed_password" not in body

    def test_create_user_duplicate_email(self, client):
        client.post("/users/", json=self._payload())
        resp = client.post("/users/", json=self._payload(
            username="otheruser"
        ))

        assert resp.status_code == 400

    def test_create_user_duplicate_username(self, client):
        client.post("/users/", json=self._payload())
        resp = client.post("/users/", json=self._payload(
            email="other@example.com"
        ))

        assert resp.status_code == 400

    def test_get_users_empty(self, client):
        resp = client.get("/users/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_user_by_id(self, client):
        created = client.post("/users/", json=self._payload()).json()
        resp = client.get(f"/users/{created['id']}")

        assert resp.status_code == 200
        body = resp.json()
        assert body["id"] == created["id"]
        assert body["email"] == "test@example.com"
        assert body["username"] == "testuser"

    def test_get_user_not_found(self, client):
        resp = client.get("/users/9999")
        assert resp.status_code == 404

    def test_get_user_by_email(self, client):
        client.post("/users/", json=self._payload())
        resp = client.get("/users/by-email/test@example.com")

        assert resp.status_code == 200
        body = resp.json()
        assert body["email"] == "test@example.com"
        # UserInDB includes hashed_password
        assert "hashed_password" in body

    def test_get_user_by_username(self, client):
        client.post("/users/", json=self._payload())
        resp = client.get("/users/by-username/testuser")

        assert resp.status_code == 200
        body = resp.json()
        assert body["username"] == "testuser"
        assert "hashed_password" in body

    def test_update_user(self, client):
        created = client.post("/users/", json=self._payload()).json()
        resp = client.put(
            f"/users/{created['id']}",
            json={"full_name": "Updated Name"},
        )

        assert resp.status_code == 200
        body = resp.json()
        assert body["full_name"] == "Updated Name"
        assert body["email"] == "test@example.com"

    def test_delete_user(self, client):
        created = client.post("/users/", json=self._payload()).json()
        user_id = created["id"]

        resp = client.delete(f"/users/{user_id}")
        assert resp.status_code == 204

        resp = client.get(f"/users/{user_id}")
        assert resp.status_code == 404

    def test_pagination(self, client):
        for i in range(3):
            client.post("/users/", json=self._payload(
                email=f"user{i}@example.com",
                username=f"user{i}",
            ))

        resp = client.get("/users/", params={"limit": 2})
        assert resp.status_code == 200
        assert len(resp.json()) == 2


# -------------------------------------------------------------------
# Role endpoints
# -------------------------------------------------------------------

class TestRoleEndpoints:
    """Tests for role CRUD endpoints."""

    def _payload(self, **overrides):
        """Build a role creation payload."""
        defaults = {
            "name": "admin",
            "description": "Administrator role",
        }
        defaults.update(overrides)
        return defaults

    def test_create_role(self, client):
        resp = client.post("/roles/", json=self._payload())

        assert resp.status_code == 201
        body = resp.json()
        assert body["name"] == "admin"
        assert body["description"] == "Administrator role"
        assert body["id"] is not None

    def test_create_role_duplicate(self, client):
        client.post("/roles/", json=self._payload())
        resp = client.post("/roles/", json=self._payload())

        assert resp.status_code == 400

    def test_get_roles_empty(self, client):
        resp = client.get("/roles/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_roles(self, client):
        client.post("/roles/", json=self._payload(name="admin"))
        client.post("/roles/", json=self._payload(name="viewer"))

        resp = client.get("/roles/")
        assert resp.status_code == 200
        assert len(resp.json()) == 2


# -------------------------------------------------------------------
# Role assignment endpoints
# -------------------------------------------------------------------

class TestRoleAssignmentEndpoints:
    """Tests for POST /users/{user_id}/roles/{role_id}."""

    def _user_payload(self, **overrides):
        defaults = {
            "email": "test@example.com",
            "username": "testuser",
            "password": "securepass123",
            "full_name": "Test User",
        }
        defaults.update(overrides)
        return defaults

    def _role_payload(self, **overrides):
        defaults = {
            "name": "admin",
            "description": "Administrator role",
        }
        defaults.update(overrides)
        return defaults

    def test_assign_role(self, client):
        user = client.post("/users/", json=self._user_payload()).json()
        role = client.post("/roles/", json=self._role_payload()).json()

        resp = client.post(f"/users/{user['id']}/roles/{role['id']}")

        assert resp.status_code == 200
        body = resp.json()
        role_names = [r["name"] for r in body["roles"]]
        assert "admin" in role_names

    def test_assign_role_user_not_found(self, client):
        role = client.post("/roles/", json=self._role_payload()).json()
        resp = client.post(f"/users/9999/roles/{role['id']}")

        assert resp.status_code == 404

    def test_assign_role_role_not_found(self, client):
        user = client.post("/users/", json=self._user_payload()).json()
        resp = client.post(f"/users/{user['id']}/roles/9999")

        assert resp.status_code == 404
