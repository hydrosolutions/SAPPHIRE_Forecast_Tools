"""
HTTP endpoint tests for the api-gateway service.

Tests the FastAPI proxy routes, health endpoints, API key authentication,
middleware, and error handling via TestClient. All downstream httpx calls
are mocked — no real services are required.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_httpx_response(status_code=200, json_data=None):
    """Create a mock httpx Response object."""
    mock_response = MagicMock()
    mock_response.status_code = status_code
    mock_response.json.return_value = json_data if json_data is not None else {}
    mock_response.text = str(json_data) if json_data is not None else ""
    mock_response.headers = {}
    return mock_response


# ---------------------------------------------------------------------------
# Root and health endpoints
# ---------------------------------------------------------------------------

class TestRootAndHealth:
    """Tests for /, /health, /health/ready, and /health/services."""

    def test_root(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        body = resp.json()
        assert "message" in body
        assert "version" in body
        assert "services" in body
        assert isinstance(body["services"], list)

    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "healthy"
        assert "services" in body
        assert isinstance(body["services"], dict)

    @patch("app.main.httpx.AsyncClient")
    def test_health_ready_success(self, mock_client_class, client):
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.get.return_value = _mock_httpx_response(
            200, {"status": "ready"}
        )

        resp = client.get("/health/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ready"

    @patch("app.main.httpx.AsyncClient")
    def test_health_ready_service_down(self, mock_client_class, client):
        import httpx

        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.get.side_effect = httpx.ConnectError("connection refused")

        resp = client.get("/health/ready")
        assert resp.status_code == 503

    @patch("app.main.httpx.AsyncClient")
    def test_health_services(self, mock_client_class, client):
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.get.return_value = _mock_httpx_response(200, {"status": "healthy"})

        resp = client.get("/health/services")
        assert resp.status_code == 200
        body = resp.json()
        # All four services should be reported
        for service_name in ("preprocessing", "postprocessing", "user", "auth"):
            assert service_name in body
            assert body[service_name]["status"] == "healthy"


# ---------------------------------------------------------------------------
# API key authentication
# ---------------------------------------------------------------------------

class TestApiKeyAuth:
    """Tests for API key enforcement on protected routes."""

    @patch("app.main.httpx.AsyncClient")
    def test_no_api_key_when_disabled(self, mock_client_class, client):
        """No X-API-Key header is needed when api_key_enabled=False."""
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.request.return_value = _mock_httpx_response(
            200, {"status": "healthy"}
        )

        resp = client.get("/api/preprocessing/health")
        assert resp.status_code == 200

    def test_missing_api_key_when_enabled(self, client_with_api_key):
        """Missing X-API-Key header returns 401 when authentication is enabled."""
        resp = client_with_api_key.get("/api/preprocessing/health")
        assert resp.status_code == 401

    @patch("app.main.httpx.AsyncClient")
    def test_valid_api_key(self, mock_client_class, client_with_api_key):
        """Valid X-API-Key header passes authentication and proxies the request."""
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.request.return_value = _mock_httpx_response(
            200, {"status": "healthy"}
        )

        resp = client_with_api_key.get(
            "/api/preprocessing/health",
            headers={"X-API-Key": "test-api-key"},
        )
        assert resp.status_code == 200

    def test_invalid_api_key(self, client_with_api_key):
        """Wrong X-API-Key value returns 401."""
        resp = client_with_api_key.get(
            "/api/preprocessing/health",
            headers={"X-API-Key": "wrong-key"},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Proxy routes
# ---------------------------------------------------------------------------

class TestProxyRoutes:
    """Tests that proxy routes forward requests and responses correctly."""

    @patch("app.main.httpx.AsyncClient")
    def test_preprocessing_get(self, mock_client_class, client):
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.request.return_value = _mock_httpx_response(
            200, [{"id": 1}]
        )

        resp = client.get("/api/preprocessing/runoff/", params={"code": "15013"})
        assert resp.status_code == 200

    @patch("app.main.httpx.AsyncClient")
    def test_preprocessing_post(self, mock_client_class, client):
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.request.return_value = _mock_httpx_response(
            201, {"id": 1, "code": "15013"}
        )

        resp = client.post(
            "/api/preprocessing/runoff/",
            json={"data": [{"code": "15013", "discharge": 100.0}]},
        )
        assert resp.status_code == 201

    @patch("app.main.httpx.AsyncClient")
    def test_postprocessing_get(self, mock_client_class, client):
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.request.return_value = _mock_httpx_response(
            200, [{"forecast_id": 42}]
        )

        resp = client.get("/api/postprocessing/forecast/")
        assert resp.status_code == 200

    @patch("app.main.httpx.AsyncClient")
    def test_postprocessing_delete(self, mock_client_class, client):
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.request.return_value = _mock_httpx_response(204, None)
        mock_client.request.return_value.text = ""

        resp = client.delete(
            "/api/postprocessing/bulletin/", params={"code": "15013"}
        )
        assert resp.status_code == 204

    @patch("app.main.httpx.AsyncClient")
    def test_user_proxy(self, mock_client_class, client):
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.request.return_value = _mock_httpx_response(
            200, [{"user_id": 1, "username": "testuser"}]
        )

        resp = client.get("/api/user/users/")
        assert resp.status_code == 200

    @patch("app.main.httpx.AsyncClient")
    def test_auth_proxy_no_api_key_required(self, mock_client_class, client_with_api_key):
        """Auth routes bypass the API key check even when authentication is enabled."""
        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.request.return_value = _mock_httpx_response(
            200, {"status": "healthy"}
        )

        # No X-API-Key header — should still succeed for auth routes
        resp = client_with_api_key.get("/api/auth/health")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Proxy error handling
# ---------------------------------------------------------------------------

class TestProxyErrors:
    """Tests for gateway error responses when downstream services fail."""

    @patch("app.main.httpx.AsyncClient")
    def test_timeout(self, mock_client_class, client):
        """A downstream timeout returns 504 Gateway Timeout."""
        import httpx

        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.request.side_effect = httpx.TimeoutException("timed out")

        resp = client.get("/api/preprocessing/runoff/")
        assert resp.status_code == 504

    @patch("app.main.httpx.AsyncClient")
    def test_connection_error(self, mock_client_class, client):
        """A downstream connection error returns 503 Service Unavailable."""
        import httpx

        mock_client = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client
        mock_client.request.side_effect = httpx.ConnectError("connection refused")

        resp = client.get("/api/preprocessing/runoff/")
        assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

class TestMiddleware:
    """Tests for the request logging middleware."""

    def test_process_time_header(self, client):
        """Every response must include an X-Process-Time header."""
        resp = client.get("/")
        assert resp.status_code == 200
        assert "x-process-time" in resp.headers
