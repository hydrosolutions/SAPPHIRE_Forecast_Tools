"""
JWT auth function tests for the auth service.

Tests the token creation and decoding functions in auth.py directly —
no HTTP layer, no database required.
"""

from datetime import timedelta

import pytest
from fastapi import HTTPException

from app import auth


class TestAccessToken:
    """Tests for access token creation and decoding."""

    def test_create_access_token(self):
        data = {"sub": "test@example.com", "user_id": 1}
        token = auth.create_access_token(data=data)

        payload = auth.decode_token(token)
        assert payload["sub"] == "test@example.com"
        assert payload["user_id"] == 1
        assert payload["type"] == "access"

    def test_access_token_expires(self):
        data = {"sub": "test@example.com", "user_id": 1}
        token = auth.create_access_token(
            data=data, expires_delta=timedelta(minutes=5)
        )

        payload = auth.decode_token(token)
        assert "exp" in payload
        # exp is a Unix timestamp; jose decodes it as an integer
        import time
        assert payload["exp"] > time.time()

    def test_decode_invalid_token(self):
        with pytest.raises(HTTPException) as exc_info:
            auth.decode_token("this.is.not.a.valid.jwt")

        assert exc_info.value.status_code == 401


class TestRefreshToken:
    """Tests for refresh token creation and decoding."""

    def test_create_refresh_token(self):
        data = {"sub": "test@example.com", "user_id": 1}
        token = auth.create_refresh_token(data=data)

        payload = auth.decode_token(token)
        assert payload["type"] == "refresh"
        assert payload["sub"] == "test@example.com"
        assert payload["user_id"] == 1

    def test_refresh_token_has_expiry(self):
        data = {"sub": "test@example.com", "user_id": 1}
        token = auth.create_refresh_token(data=data)

        payload = auth.decode_token(token)
        assert "exp" in payload
