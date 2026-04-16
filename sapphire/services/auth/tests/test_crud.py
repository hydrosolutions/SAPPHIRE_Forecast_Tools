"""
CRUD tests for the auth service.

Tests password verification and refresh token CRUD functions directly
against the SQLite in-memory database — no HTTP layer required.
"""

from datetime import datetime, timedelta

import pytest

from app import crud
from app.models import RefreshToken


class TestPasswordVerification:
    """Tests for passlib/bcrypt password hashing and verification."""

    def test_verify_correct_password(self):
        hashed = crud.pwd_context.hash("mysecretpassword")
        assert crud.verify_password("mysecretpassword", hashed) is True

    def test_verify_wrong_password(self):
        hashed = crud.pwd_context.hash("mysecretpassword")
        assert crud.verify_password("wrongpassword", hashed) is False


class TestRefreshTokenCRUD:
    """Tests for refresh token store/verify/revoke operations."""

    def test_store_refresh_token(self, db_session):
        db_token = crud.store_refresh_token(
            db_session, user_id=1, token="mytoken123"
        )

        assert db_token.id is not None
        assert db_token.user_id == 1
        assert db_token.token == "mytoken123"
        assert db_token.revoked is False
        assert db_token.expires_at > datetime.utcnow()

    def test_verify_valid_refresh_token(self, db_session):
        crud.store_refresh_token(db_session, user_id=1, token="validtoken")

        result = crud.verify_refresh_token(db_session, user_id=1, token="validtoken")
        assert result is True

    def test_verify_nonexistent_token(self, db_session):
        result = crud.verify_refresh_token(
            db_session, user_id=1, token="doesnotexist"
        )
        assert result is False

    def test_verify_revoked_token(self, db_session):
        crud.store_refresh_token(db_session, user_id=1, token="revokedtoken")
        crud.revoke_refresh_token(db_session, user_id=1, token="revokedtoken")

        result = crud.verify_refresh_token(
            db_session, user_id=1, token="revokedtoken"
        )
        assert result is False

    def test_revoke_refresh_token(self, db_session):
        crud.store_refresh_token(db_session, user_id=1, token="tokentorevoke")
        crud.revoke_refresh_token(db_session, user_id=1, token="tokentorevoke")

        db_token = db_session.query(RefreshToken).filter(
            RefreshToken.token == "tokentorevoke"
        ).first()
        assert db_token is not None
        assert db_token.revoked is True

    def test_revoke_all_refresh_tokens(self, db_session):
        for i in range(3):
            crud.store_refresh_token(
                db_session, user_id=42, token=f"token{i}"
            )

        crud.revoke_all_refresh_tokens(db_session, user_id=42)

        tokens = db_session.query(RefreshToken).filter(
            RefreshToken.user_id == 42
        ).all()
        assert len(tokens) == 3
        assert all(t.revoked is True for t in tokens)

    def test_verify_expired_token(self, db_session):
        # Insert a token that is already expired
        expired_token = RefreshToken(
            user_id=1,
            token="expiredtoken",
            revoked=False,
            expires_at=datetime.utcnow() - timedelta(days=1),
        )
        db_session.add(expired_token)
        db_session.commit()

        result = crud.verify_refresh_token(
            db_session, user_id=1, token="expiredtoken"
        )
        assert result is False
