"""
Tests for the bulletin share feature: POST /bulletin/share and
GET /public/bulletin/{token}, plus the underlying CRUD functions.

Mirrors the style of TestBulletinEndpoints in test_endpoints.py.
"""

from datetime import datetime, timedelta, timezone

import pytest

from app.crud import create_bulletin_share, get_bulletin_share_by_token
from app.models import BulletinShare
from app.schemas import BulletinShareCreate


def _share_payload(**overrides):
    """Build a single bulletin-share creation payload (synthetic data only)."""
    defaults = {
        "horizon": "pentad",
        "year": 2026,
        "horizon_value": 26,
        "expires_at": (datetime.now(timezone.utc) + timedelta(days=5)).isoformat(),
        "payload": {
            "horizon": "pentad",
            "year": 2026,
            "horizon_value": 26,
            "stations": [
                {"code": "SYN001", "station_label": "Synthetic Station", "forecasted_discharge": 42.0}
            ],
        },
        "station_codes": ["SYN001"],
    }
    defaults.update(overrides)
    return defaults


# -------------------------------------------------------------------
# HTTP endpoint tests
# -------------------------------------------------------------------

class TestBulletinShareEndpoints:
    """Tests for POST /bulletin/share and GET /public/bulletin/{token}."""

    def test_post_creates_share_and_returns_token_url_expiry(self, client):
        payload = _share_payload()
        resp = client.post("/bulletin/share", json=payload)

        assert resp.status_code == 201
        body = resp.json()
        assert "token" in body and len(body["token"]) > 0
        assert body["url"].endswith(f"/public/bulletin/{body['token']}")
        assert "example-bulletins.test" in body["url"]
        assert body["expires_at"] is not None

    def test_tokens_are_unique_across_two_creates(self, client):
        resp1 = client.post("/bulletin/share", json=_share_payload())
        resp2 = client.post("/bulletin/share", json=_share_payload())

        assert resp1.status_code == 201
        assert resp2.status_code == 201
        assert resp1.json()["token"] != resp2.json()["token"]

    def test_get_public_bulletin_returns_stored_payload_when_not_expired(self, client):
        create_payload = _share_payload()
        create_resp = client.post("/bulletin/share", json=create_payload)
        token = create_resp.json()["token"]

        resp = client.get(f"/public/bulletin/{token}")
        assert resp.status_code == 200
        assert resp.json() == create_payload["payload"]

    def test_get_public_bulletin_returns_410_when_expired(self, client, db_session):
        past = datetime.now(timezone.utc) - timedelta(days=1)
        record = BulletinShare(
            token="expired-token-synthetic",
            horizon_type="pentad",
            year=2026,
            horizon_value=26,
            expires_at=past,
            payload={"stations": []},
            station_codes=[],
        )
        db_session.add(record)
        db_session.commit()

        resp = client.get("/public/bulletin/expired-token-synthetic")
        assert resp.status_code == 410

    def test_get_public_bulletin_returns_404_for_unknown_token(self, client):
        resp = client.get("/public/bulletin/does-not-exist-token")
        assert resp.status_code == 404


# -------------------------------------------------------------------
# CRUD unit tests
# -------------------------------------------------------------------

class TestBulletinShareCrud:
    """Tests for create_bulletin_share / get_bulletin_share_by_token."""

    def test_create_bulletin_share_persists_record(self, db_session):
        data = BulletinShareCreate(**_share_payload())
        record = create_bulletin_share(db_session, data)

        assert record.id is not None
        assert record.token
        assert record.horizon_type.value == "pentad"
        assert record.payload == data.payload
        assert record.station_codes == ["SYN001"]

    def test_get_bulletin_share_by_token_returns_none_for_unknown(self, db_session):
        assert get_bulletin_share_by_token(db_session, "nonexistent") is None

    def test_get_bulletin_share_by_token_returns_record(self, db_session):
        data = BulletinShareCreate(**_share_payload())
        created = create_bulletin_share(db_session, data)

        fetched = get_bulletin_share_by_token(db_session, created.token)
        assert fetched is not None
        assert fetched.id == created.id

    def test_create_bulletin_share_retries_once_on_token_clash(self, db_session, monkeypatch):
        """secrets.token_urlsafe is patched to return a fixed value once, then a
        different value, simulating a unique-constraint clash on the first
        attempt that succeeds on retry."""
        tokens = iter(["clashing-token", "fresh-token-after-retry"])

        def fake_token_urlsafe(_n):
            return next(tokens)

        # Pre-seed a row occupying "clashing-token" so the first mint attempt
        # collides.
        existing = BulletinShare(
            token="clashing-token",
            horizon_type="pentad",
            year=2025,
            horizon_value=1,
            expires_at=datetime.now(timezone.utc) + timedelta(days=1),
            payload={"stations": []},
            station_codes=None,
        )
        db_session.add(existing)
        db_session.commit()

        monkeypatch.setattr("app.crud.secrets.token_urlsafe", fake_token_urlsafe)

        data = BulletinShareCreate(**_share_payload())
        record = create_bulletin_share(db_session, data)

        assert record.token == "fresh-token-after-retry"
