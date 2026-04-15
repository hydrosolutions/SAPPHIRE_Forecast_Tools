"""
HTTP endpoint tests for the preprocessing service.

Tests the FastAPI endpoints via TestClient, exercising the full
request → Pydantic validation → CRUD → response serialization path.
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
# Runoff endpoints
# -------------------------------------------------------------------

class TestRunoffEndpoints:
    """Tests for POST /runoff/ and GET /runoff/."""

    def _payload(self, **overrides):
        """Build a single-item runoff payload."""
        defaults = {
            "horizon_type": "pentad",
            "code": "15013",
            "date": "2024-06-15",
            "discharge": 100.0,
            "predictor": 80.0,
            "horizon_value": 3,
            "horizon_in_year": 33,
        }
        defaults.update(overrides)
        return {"data": [defaults]}

    def test_post_creates_runoff(self, client):
        resp = client.post("/runoff/", json=self._payload())

        assert resp.status_code == 201
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["horizon_type"] == "pentad"
        assert data[0]["discharge"] == 100.0
        assert data[0]["id"] is not None

    def test_get_empty(self, client):
        resp = client.get("/runoff/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_with_filters(self, client):
        payload = {"data": [
            {
                "horizon_type": "pentad", "code": "15013",
                "date": "2024-06-15", "discharge": 100.0,
                "predictor": 80.0, "horizon_value": 3, "horizon_in_year": 33,
            },
            {
                "horizon_type": "pentad", "code": "15014",
                "date": "2024-06-15", "discharge": 200.0,
                "predictor": 90.0, "horizon_value": 3, "horizon_in_year": 33,
            },
        ]}
        client.post("/runoff/", json=payload)

        resp = client.get("/runoff/", params={"code": "15013"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["discharge"] == 100.0

    def test_pagination(self, client):
        items = [
            {
                "horizon_type": "pentad", "code": f"1501{i}",
                "date": "2024-06-15", "discharge": 100.0 + i,
                "predictor": 80.0, "horizon_value": 3, "horizon_in_year": 33,
            }
            for i in range(3)
        ]
        client.post("/runoff/", json={"data": items})

        resp = client.get("/runoff/", params={"limit": 2})
        assert resp.status_code == 200
        assert len(resp.json()) == 2

        resp = client.get("/runoff/", params={"skip": 2, "limit": 10})
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_invalid_horizon_returns_422(self, client):
        resp = client.post(
            "/runoff/",
            json=self._payload(horizon_type="INVALID"),
        )
        assert resp.status_code == 422

    def test_upsert_via_endpoint(self, client):
        """POST the same unique key twice, second should update."""
        payload = self._payload(discharge=100.0)
        client.post("/runoff/", json=payload)

        payload["data"][0]["discharge"] = 999.0
        resp = client.post("/runoff/", json=payload)
        assert resp.status_code == 201
        assert resp.json()[0]["discharge"] == 999.0

        # Only 1 record should exist
        all_resp = client.get("/runoff/")
        assert len(all_resp.json()) == 1

    def test_bulk_multiple_records(self, client):
        """POST multiple records in a single bulk request."""
        items = [
            {
                "horizon_type": "pentad", "code": f"1501{i}",
                "date": "2024-06-15", "discharge": 100.0 + i,
                "horizon_value": 3, "horizon_in_year": 33,
            }
            for i in range(5)
        ]
        resp = client.post("/runoff/", json={"data": items})
        assert resp.status_code == 201
        assert len(resp.json()) == 5

    def test_empty_bulk_post(self, client):
        """POST with an empty data list returns empty 201."""
        resp = client.post("/runoff/", json={"data": []})
        assert resp.status_code == 201
        assert resp.json() == []


# -------------------------------------------------------------------
# Hydrograph endpoints
# -------------------------------------------------------------------

class TestHydrographEndpoints:
    """Tests for POST /hydrograph/ and GET /hydrograph/."""

    def _payload(self, **overrides):
        """Build a single-item hydrograph payload."""
        defaults = {
            "horizon_type": "pentad",
            "code": "15013",
            "date": "2024-06-15",
            "horizon_value": 3,
            "horizon_in_year": 33,
            "day_of_year": 167,
            "mean": 95.0,
            "norm": 90.0,
        }
        defaults.update(overrides)
        return {"data": [defaults]}

    def test_post_creates_hydrograph(self, client):
        resp = client.post("/hydrograph/", json=self._payload())

        assert resp.status_code == 201
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["horizon_type"] == "pentad"
        assert data[0]["mean"] == 95.0
        assert data[0]["id"] is not None

    def test_get_empty(self, client):
        resp = client.get("/hydrograph/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_with_filters(self, client):
        payload = {"data": [
            {
                "horizon_type": "pentad", "code": "15013",
                "date": "2024-06-15", "horizon_value": 3,
                "horizon_in_year": 33, "day_of_year": 167, "mean": 95.0,
            },
            {
                "horizon_type": "pentad", "code": "15014",
                "date": "2024-06-15", "horizon_value": 3,
                "horizon_in_year": 33, "day_of_year": 167, "mean": 80.0,
            },
        ]}
        client.post("/hydrograph/", json=payload)

        resp = client.get("/hydrograph/", params={"code": "15013"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["mean"] == 95.0

    def test_pagination(self, client):
        items = [
            {
                "horizon_type": "pentad", "code": f"1501{i}",
                "date": "2024-06-15", "horizon_value": 3,
                "horizon_in_year": 33, "day_of_year": 167,
            }
            for i in range(3)
        ]
        client.post("/hydrograph/", json={"data": items})

        resp = client.get("/hydrograph/", params={"limit": 2})
        assert resp.status_code == 200
        assert len(resp.json()) == 2

        resp = client.get("/hydrograph/", params={"skip": 2, "limit": 10})
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_upsert_via_endpoint(self, client):
        """POST the same unique key twice, second should update."""
        payload = self._payload(mean=95.0)
        client.post("/hydrograph/", json=payload)

        payload["data"][0]["mean"] = 200.0
        resp = client.post("/hydrograph/", json=payload)
        assert resp.status_code == 201
        assert resp.json()[0]["mean"] == 200.0

        all_resp = client.get("/hydrograph/")
        assert len(all_resp.json()) == 1

    def test_bulk_multiple_records(self, client):
        """POST multiple records in a single bulk request."""
        items = [
            {
                "horizon_type": "pentad", "code": f"1501{i}",
                "date": "2024-06-15", "horizon_value": 3,
                "horizon_in_year": 33, "day_of_year": 167,
            }
            for i in range(5)
        ]
        resp = client.post("/hydrograph/", json={"data": items})
        assert resp.status_code == 201
        assert len(resp.json()) == 5

    def test_empty_bulk_post(self, client):
        """POST with an empty data list returns empty 201."""
        resp = client.post("/hydrograph/", json={"data": []})
        assert resp.status_code == 201
        assert resp.json() == []


# -------------------------------------------------------------------
# Meteo endpoints
# -------------------------------------------------------------------

class TestMeteoEndpoints:
    """Tests for POST /meteo/ and GET /meteo/."""

    def _payload(self, **overrides):
        """Build a single-item meteo payload."""
        defaults = {
            "meteo_type": "T",
            "code": "15013",
            "date": "2024-06-15",
            "value": 22.5,
            "norm": 20.0,
            "day_of_year": 167,
        }
        defaults.update(overrides)
        return {"data": [defaults]}

    def test_post_creates_meteo(self, client):
        resp = client.post("/meteo/", json=self._payload())

        assert resp.status_code == 201
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["meteo_type"] == "T"
        assert data[0]["value"] == 22.5
        assert data[0]["id"] is not None

    def test_get_empty(self, client):
        resp = client.get("/meteo/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_with_filters(self, client):
        payload = {"data": [
            {
                "meteo_type": "T", "code": "15013",
                "date": "2024-06-15", "value": 22.5, "day_of_year": 167,
            },
            {
                "meteo_type": "T", "code": "15014",
                "date": "2024-06-15", "value": 18.0, "day_of_year": 167,
            },
        ]}
        client.post("/meteo/", json=payload)

        resp = client.get("/meteo/", params={"code": "15013"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["value"] == 22.5

    def test_pagination(self, client):
        items = [
            {
                "meteo_type": "T", "code": f"1501{i}",
                "date": "2024-06-15", "value": 20.0 + i, "day_of_year": 167,
            }
            for i in range(3)
        ]
        client.post("/meteo/", json={"data": items})

        resp = client.get("/meteo/", params={"limit": 2})
        assert resp.status_code == 200
        assert len(resp.json()) == 2

        resp = client.get("/meteo/", params={"skip": 2, "limit": 10})
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_get_filter_by_meteo_type(self, client):
        """GET /meteo/ uses meteo_type param (not horizon)."""
        payload = {"data": [
            {
                "meteo_type": "T", "code": "15013",
                "date": "2024-06-15", "value": 22.5, "day_of_year": 167,
            },
            {
                "meteo_type": "P", "code": "15013",
                "date": "2024-06-16", "value": 5.0, "day_of_year": 168,
            },
        ]}
        client.post("/meteo/", json=payload)

        resp = client.get("/meteo/", params={"meteo_type": "T"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["meteo_type"] == "T"

    def test_upsert_via_endpoint(self, client):
        """POST the same unique key twice, second should update."""
        payload = self._payload(value=22.5)
        client.post("/meteo/", json=payload)

        payload["data"][0]["value"] = 99.9
        resp = client.post("/meteo/", json=payload)
        assert resp.status_code == 201
        assert resp.json()[0]["value"] == 99.9

        all_resp = client.get("/meteo/")
        assert len(all_resp.json()) == 1

    def test_bulk_multiple_records(self, client):
        """POST multiple records in a single bulk request."""
        items = [
            {
                "meteo_type": "T", "code": f"1501{i}",
                "date": "2024-06-15", "value": 20.0 + i, "day_of_year": 167,
            }
            for i in range(5)
        ]
        resp = client.post("/meteo/", json={"data": items})
        assert resp.status_code == 201
        assert len(resp.json()) == 5

    def test_empty_bulk_post(self, client):
        """POST with an empty data list returns empty 201."""
        resp = client.post("/meteo/", json={"data": []})
        assert resp.status_code == 201
        assert resp.json() == []


# -------------------------------------------------------------------
# Snow endpoints
# -------------------------------------------------------------------

class TestSnowEndpoints:
    """Tests for POST /snow/ and GET /snow/."""

    def _payload(self, **overrides):
        """Build a single-item snow payload."""
        defaults = {
            "snow_type": "HS",
            "code": "15013",
            "date": "2024-06-15",
            "value": 50.0,
            "norm": 45.0,
        }
        defaults.update(overrides)
        return {"data": [defaults]}

    def test_post_creates_snow(self, client):
        resp = client.post("/snow/", json=self._payload())

        assert resp.status_code == 201
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["snow_type"] == "HS"
        assert data[0]["value"] == 50.0
        assert data[0]["id"] is not None

    def test_get_empty(self, client):
        resp = client.get("/snow/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_with_filters(self, client):
        payload = {"data": [
            {
                "snow_type": "HS", "code": "15013",
                "date": "2024-06-15", "value": 50.0,
            },
            {
                "snow_type": "HS", "code": "15014",
                "date": "2024-06-15", "value": 30.0,
            },
        ]}
        client.post("/snow/", json=payload)

        resp = client.get("/snow/", params={"code": "15013"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["value"] == 50.0

    def test_pagination(self, client):
        items = [
            {
                "snow_type": "HS", "code": f"1501{i}",
                "date": "2024-06-15", "value": 50.0 + i,
            }
            for i in range(3)
        ]
        client.post("/snow/", json={"data": items})

        resp = client.get("/snow/", params={"limit": 2})
        assert resp.status_code == 200
        assert len(resp.json()) == 2

        resp = client.get("/snow/", params={"skip": 2, "limit": 10})
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_get_filter_by_snow_type(self, client):
        """GET /snow/ uses snow_type param (not horizon)."""
        payload = {"data": [
            {
                "snow_type": "HS", "code": "15013",
                "date": "2024-06-15", "value": 50.0,
            },
            {
                "snow_type": "SWE", "code": "15013",
                "date": "2024-06-16", "value": 120.0,
            },
        ]}
        client.post("/snow/", json=payload)

        resp = client.get("/snow/", params={"snow_type": "HS"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["snow_type"] == "HS"

    def test_upsert_via_endpoint(self, client):
        """POST the same unique key twice, second should update."""
        payload = self._payload(value=50.0)
        client.post("/snow/", json=payload)

        payload["data"][0]["value"] = 999.0
        resp = client.post("/snow/", json=payload)
        assert resp.status_code == 201
        assert resp.json()[0]["value"] == 999.0

        all_resp = client.get("/snow/")
        assert len(all_resp.json()) == 1

    def test_bulk_multiple_records(self, client):
        """POST multiple records in a single bulk request."""
        items = [
            {
                "snow_type": "HS", "code": f"1501{i}",
                "date": "2024-06-15", "value": 50.0 + i,
            }
            for i in range(5)
        ]
        resp = client.post("/snow/", json={"data": items})
        assert resp.status_code == 201
        assert len(resp.json()) == 5

    def test_empty_bulk_post(self, client):
        """POST with an empty data list returns empty 201."""
        resp = client.post("/snow/", json={"data": []})
        assert resp.status_code == 201
        assert resp.json() == []


# -------------------------------------------------------------------
# Endpoint edge cases
# -------------------------------------------------------------------

class TestEndpointEdgeCases:
    """Cross-cutting endpoint edge case tests."""

    def test_invalid_horizon_type_returns_422(self, client):
        """POST runoff with an invalid horizon_type returns 422."""
        resp = client.post(
            "/runoff/",
            json={"data": [{
                "horizon_type": "INVALID",
                "code": "15013",
                "date": "2024-06-15",
                "discharge": 100.0,
                "horizon_value": 3,
                "horizon_in_year": 33,
            }]},
        )
        assert resp.status_code == 422

    def test_upsert_via_endpoint_runoff(self, client):
        """POST same runoff key twice: second call updates, only 1 row."""
        payload = {"data": [{
            "horizon_type": "pentad", "code": "15013",
            "date": "2024-06-15", "discharge": 100.0,
            "horizon_value": 3, "horizon_in_year": 33,
        }]}
        client.post("/runoff/", json=payload)

        payload["data"][0]["discharge"] = 999.0
        resp = client.post("/runoff/", json=payload)
        assert resp.status_code == 201
        assert resp.json()[0]["discharge"] == 999.0

        all_resp = client.get("/runoff/")
        assert len(all_resp.json()) == 1

    def test_bulk_multiple_records_meteo(self, client):
        """POST 5 meteo records at once."""
        items = [
            {
                "meteo_type": "T", "code": f"1501{i}",
                "date": "2024-06-15", "value": 20.0 + i, "day_of_year": 167,
            }
            for i in range(5)
        ]
        resp = client.post("/meteo/", json={"data": items})
        assert resp.status_code == 201
        assert len(resp.json()) == 5

    def test_empty_bulk_post_snow(self, client):
        """POST empty snow list returns 201 and []."""
        resp = client.post("/snow/", json={"data": []})
        assert resp.status_code == 201
        assert resp.json() == []
