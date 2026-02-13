"""
HTTP endpoint tests for the postprocessing service.

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
# Forecast endpoints
# -------------------------------------------------------------------

class TestForecastEndpoints:
    """Tests for POST /forecast/ and GET /forecast/."""

    def _payload(self, **overrides):
        """Build a single-item forecast payload."""
        defaults = {
            "horizon_type": "pentad",
            "code": "15013",
            "model_type": "LR",
            "date": "2024-06-15",
            "target": "2024-06-20",
            "flag": 1,
            "horizon_value": 3,
            "horizon_in_year": 33,
            "forecasted_discharge": 100.0,
        }
        defaults.update(overrides)
        return {"data": [defaults]}

    def test_post_creates_forecast(self, client):
        resp = client.post("/forecast/", json=self._payload())

        assert resp.status_code == 201
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["model_type"] == "LR"
        assert data[0]["forecasted_discharge"] == 100.0
        assert data[0]["id"] is not None

    def test_model_type_description_in_response(self, client):
        resp = client.post("/forecast/", json=self._payload())

        assert resp.status_code == 201
        assert resp.json()[0]["model_type_description"] == "Linear Regression"

    def test_get_empty(self, client):
        resp = client.get("/forecast/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_with_filters(self, client):
        payload = {"data": [
            {
                "horizon_type": "pentad", "code": "15013",
                "model_type": "LR", "date": "2024-06-15",
                "target": "2024-06-20", "flag": 1,
                "horizon_value": 3, "horizon_in_year": 33,
                "forecasted_discharge": 100.0,
            },
            {
                "horizon_type": "pentad", "code": "15014",
                "model_type": "LR", "date": "2024-06-15",
                "target": "2024-06-20", "flag": 1,
                "horizon_value": 3, "horizon_in_year": 33,
                "forecasted_discharge": 200.0,
            },
        ]}
        client.post("/forecast/", json=payload)

        resp = client.get("/forecast/", params={"code": "15013"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["forecasted_discharge"] == 100.0

    def test_pagination(self, client):
        items = [
            {
                "horizon_type": "pentad", "code": f"1501{i}",
                "model_type": "LR", "date": "2024-06-15",
                "target": "2024-06-20", "flag": 1,
                "horizon_value": 3, "horizon_in_year": 33,
                "forecasted_discharge": 100.0 + i,
            }
            for i in range(3)
        ]
        client.post("/forecast/", json={"data": items})

        resp = client.get("/forecast/", params={"limit": 2})
        assert resp.status_code == 200
        assert len(resp.json()) == 2

        resp = client.get("/forecast/", params={"skip": 2, "limit": 10})
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_invalid_enum_returns_422(self, client):
        resp = client.post(
            "/forecast/",
            json=self._payload(model_type="INVALID_MODEL"),
        )
        assert resp.status_code == 422


# -------------------------------------------------------------------
# LongForecast endpoints
# -------------------------------------------------------------------

class TestLongForecastEndpoints:
    """Tests for POST /long-forecast/ and GET /long-forecast/."""

    def _payload(self, **overrides):
        defaults = {
            "horizon_type": "month",
            "horizon_value": 1,
            "code": "15013",
            "date": "2024-06-15",
            "model_type": "GBT",
            "valid_from": "2024-07-01",
            "valid_to": "2024-07-31",
            "flag": 0,
            "q": 123.45,
        }
        defaults.update(overrides)
        return {"data": [defaults]}

    def test_post_creates_long_forecast(self, client):
        resp = client.post("/long-forecast/", json=self._payload())

        assert resp.status_code == 201
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["model_type"] == "GBT"
        assert data[0]["q"] == 123.45
        assert data[0]["id"] is not None

    def test_get_empty(self, client):
        resp = client.get("/long-forecast/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_with_filters(self, client):
        items = {"data": [
            {
                "horizon_type": "month", "horizon_value": 1,
                "code": "15013", "date": "2024-06-15",
                "model_type": "GBT",
                "valid_from": "2024-07-01", "valid_to": "2024-07-31",
            },
            {
                "horizon_type": "month", "horizon_value": 2,
                "code": "15013", "date": "2024-06-15",
                "model_type": "GBT",
                "valid_from": "2024-08-01", "valid_to": "2024-08-31",
            },
        ]}
        client.post("/long-forecast/", json=items)

        resp = client.get(
            "/long-forecast/",
            params={"horizon_type": "month", "horizon_value": 1},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["horizon_value"] == 1

    def test_invalid_model_type_returns_422(self, client):
        resp = client.post(
            "/long-forecast/",
            json=self._payload(model_type="BOGUS"),
        )
        assert resp.status_code == 422


# -------------------------------------------------------------------
# LRForecast endpoints
# -------------------------------------------------------------------

class TestLRForecastEndpoints:
    """Tests for POST /lr-forecast/ and GET /lr-forecast/."""

    def _payload(self, **overrides):
        defaults = {
            "horizon_type": "pentad",
            "code": "15013",
            "date": "2024-06-15",
            "horizon_value": 3,
            "horizon_in_year": 33,
            "slope": 1.2,
            "intercept": 10.0,
            "forecasted_discharge": 106.0,
        }
        defaults.update(overrides)
        return {"data": [defaults]}

    def test_post_creates_lr_forecast(self, client):
        resp = client.post("/lr-forecast/", json=self._payload())

        assert resp.status_code == 201
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["slope"] == 1.2
        assert data[0]["forecasted_discharge"] == 106.0
        assert data[0]["id"] is not None

    def test_get_empty(self, client):
        resp = client.get("/lr-forecast/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_with_date_range(self, client):
        items = {"data": [
            {
                "horizon_type": "pentad", "code": "15013",
                "date": "2024-06-10",
                "horizon_value": 2, "horizon_in_year": 32,
                "forecasted_discharge": 100.0,
            },
            {
                "horizon_type": "pentad", "code": "15013",
                "date": "2024-06-20",
                "horizon_value": 4, "horizon_in_year": 34,
                "forecasted_discharge": 200.0,
            },
        ]}
        client.post("/lr-forecast/", json=items)

        resp = client.get(
            "/lr-forecast/",
            params={"start_date": "2024-06-15", "end_date": "2024-06-25"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["forecasted_discharge"] == 200.0

    def test_no_model_type_description_field(self, client):
        """LRForecastResponse does not include model_type_description."""
        resp = client.post("/lr-forecast/", json=self._payload())
        assert resp.status_code == 201
        assert "model_type_description" not in resp.json()[0]


# -------------------------------------------------------------------
# SkillMetric endpoints
# -------------------------------------------------------------------

class TestSkillMetricEndpoints:
    """Tests for POST /skill-metric/ and GET /skill-metric/."""

    def _payload(self, **overrides):
        defaults = {
            "horizon_type": "pentad",
            "code": "15013",
            "model_type": "LR",
            "date": "2024-06-15",
            "horizon_in_year": 33,
            "nse": 0.75,
            "accuracy": 0.85,
            "n_pairs": 50,
        }
        defaults.update(overrides)
        return {"data": [defaults]}

    def test_post_creates_skill_metric(self, client):
        resp = client.post("/skill-metric/", json=self._payload())

        assert resp.status_code == 201
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["nse"] == 0.75
        assert data[0]["accuracy"] == 0.85
        assert data[0]["id"] is not None

    def test_get_empty(self, client):
        resp = client.get("/skill-metric/")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_filter_by_model(self, client):
        items = {"data": [
            {
                "horizon_type": "pentad", "code": "15013",
                "model_type": "LR", "date": "2024-06-15",
                "horizon_in_year": 33, "nse": 0.75,
            },
            {
                "horizon_type": "pentad", "code": "15013",
                "model_type": "EM", "date": "2024-06-15",
                "horizon_in_year": 33, "nse": 0.90,
            },
        ]}
        client.post("/skill-metric/", json=items)

        resp = client.get("/skill-metric/", params={"model": "LR"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["model_type"] == "LR"
        assert data[0]["nse"] == 0.75

    def test_model_type_description_correct(self, client):
        resp = client.post(
            "/skill-metric/",
            json=self._payload(model_type="EM"),
        )
        assert resp.status_code == 201
        desc = resp.json()[0]["model_type_description"]
        assert desc == "Ens. Mean with LR, TFT, TIDE (EM)"


# -------------------------------------------------------------------
# Endpoint edge cases
# -------------------------------------------------------------------

class TestEndpointEdgeCases:
    """Cross-cutting endpoint edge case tests."""

    def test_upsert_via_endpoint(self, client):
        """POST the same unique key twice, second should update."""
        payload = {
            "data": [{
                "horizon_type": "pentad", "code": "15013",
                "model_type": "LR", "date": "2024-06-15",
                "target": "2024-06-20", "flag": 1,
                "horizon_value": 3, "horizon_in_year": 33,
                "forecasted_discharge": 100.0,
            }]
        }
        client.post("/forecast/", json=payload)

        payload["data"][0]["forecasted_discharge"] = 999.0
        resp = client.post("/forecast/", json=payload)
        assert resp.status_code == 201
        assert resp.json()[0]["forecasted_discharge"] == 999.0

        # Only 1 record should exist
        all_resp = client.get("/forecast/")
        assert len(all_resp.json()) == 1

    def test_bulk_multiple_records(self, client):
        """POST multiple records in a single bulk request."""
        items = [
            {
                "horizon_type": "pentad", "code": f"1501{i}",
                "model_type": "LR", "date": "2024-06-15",
                "target": "2024-06-20", "flag": 1,
                "horizon_value": 3, "horizon_in_year": 33,
                "forecasted_discharge": 100.0 + i,
            }
            for i in range(5)
        ]
        resp = client.post("/forecast/", json={"data": items})
        assert resp.status_code == 201
        assert len(resp.json()) == 5

    def test_null_target_filter(self, client):
        """Filter forecasts where target is NULL."""
        payload = {
            "data": [
                {
                    "horizon_type": "pentad", "code": "15013",
                    "model_type": "LR", "date": "2024-06-15",
                    "target": None, "flag": 1,
                    "horizon_value": 3, "horizon_in_year": 33,
                    "forecasted_discharge": 100.0,
                },
                {
                    "horizon_type": "pentad", "code": "15014",
                    "model_type": "LR", "date": "2024-06-15",
                    "target": "2024-06-20", "flag": 1,
                    "horizon_value": 3, "horizon_in_year": 33,
                    "forecasted_discharge": 200.0,
                },
            ]
        }
        client.post("/forecast/", json=payload)

        resp = client.get("/forecast/", params={"target": "null"})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["code"] == "15013"
        assert data[0]["target"] is None

    def test_empty_bulk_post(self, client):
        """POST with an empty data list returns empty 201."""
        resp = client.post("/forecast/", json={"data": []})
        assert resp.status_code == 201
        assert resp.json() == []
