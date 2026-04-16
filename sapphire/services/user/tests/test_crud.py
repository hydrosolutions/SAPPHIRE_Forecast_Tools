"""
CRUD tests for the user service.

Tests the SQLAlchemy CRUD functions directly (no HTTP layer) using
SQLite in-memory databases.
"""

import pytest

from app import crud
from app.models import User, Role
from factories import make_user, make_role


# -------------------------------------------------------------------
# User CRUD
# -------------------------------------------------------------------

class TestUserCRUD:
    """Tests for user create/read/update/delete operations."""

    def test_create_user(self, db_session):
        user = make_user()
        result = crud.create_user(db_session, user)

        assert result.id is not None
        assert result.email == "test@example.com"
        assert result.username == "testuser"
        assert result.full_name == "Test User"
        assert result.is_active is True
        assert result.is_superuser is False
        # Password must be hashed, not stored in plain text
        assert result.hashed_password != "securepass123"

    def test_get_user_by_id(self, db_session):
        created = crud.create_user(db_session, make_user())
        found = crud.get_user(db_session, created.id)

        assert found is not None
        assert found.id == created.id
        assert found.email == created.email

    def test_get_user_by_email(self, db_session):
        created = crud.create_user(db_session, make_user())
        found = crud.get_user_by_email(db_session, "test@example.com")

        assert found is not None
        assert found.id == created.id
        assert found.username == "testuser"

    def test_get_user_by_username(self, db_session):
        created = crud.create_user(db_session, make_user())
        found = crud.get_user_by_username(db_session, "testuser")

        assert found is not None
        assert found.id == created.id
        assert found.email == "test@example.com"

    def test_get_users_list(self, db_session):
        crud.create_user(db_session, make_user(
            email="a@example.com", username="usera"
        ))
        crud.create_user(db_session, make_user(
            email="b@example.com", username="userb"
        ))
        crud.create_user(db_session, make_user(
            email="c@example.com", username="userc"
        ))

        results = crud.get_users(db_session)
        assert len(results) == 3

    def test_get_users_filter_active(self, db_session):
        active_user = crud.create_user(db_session, make_user(
            email="active@example.com", username="activeuser"
        ))
        inactive_user = crud.create_user(db_session, make_user(
            email="inactive@example.com", username="inactiveuser"
        ))

        # Deactivate via direct attribute set
        inactive_user.is_active = False
        db_session.commit()
        db_session.refresh(inactive_user)

        active_results = crud.get_users(db_session, is_active=True)
        assert len(active_results) == 1
        assert active_results[0].id == active_user.id

        inactive_results = crud.get_users(db_session, is_active=False)
        assert len(inactive_results) == 1
        assert inactive_results[0].id == inactive_user.id

    def test_update_user(self, db_session):
        from app.schemas import UserUpdate

        created = crud.create_user(db_session, make_user())
        update = UserUpdate(full_name="Updated Name")
        updated = crud.update_user(db_session, created.id, update)

        assert updated.full_name == "Updated Name"
        assert updated.email == "test@example.com"

    def test_update_user_password(self, db_session):
        from app.schemas import UserUpdate

        created = crud.create_user(db_session, make_user())
        original_hash = created.hashed_password

        update = UserUpdate(password="newsecurepass456")
        updated = crud.update_user(db_session, created.id, update)

        assert updated.hashed_password != original_hash
        assert updated.hashed_password != "newsecurepass456"

    def test_delete_user(self, db_session):
        created = crud.create_user(db_session, make_user())
        user_id = created.id

        crud.delete_user(db_session, user_id)

        assert crud.get_user(db_session, user_id) is None

    def test_get_nonexistent_user(self, db_session):
        result = crud.get_user(db_session, 99999)
        assert result is None


# -------------------------------------------------------------------
# Role CRUD
# -------------------------------------------------------------------

class TestRoleCRUD:
    """Tests for role create/read operations."""

    def test_create_role(self, db_session):
        role = make_role()
        result = crud.create_role(db_session, role)

        assert result.id is not None
        assert result.name == "admin"
        assert result.description == "Administrator role"

    def test_get_role_by_id(self, db_session):
        created = crud.create_role(db_session, make_role())
        found = crud.get_role(db_session, created.id)

        assert found is not None
        assert found.id == created.id
        assert found.name == "admin"

    def test_get_role_by_name(self, db_session):
        created = crud.create_role(db_session, make_role())
        found = crud.get_role_by_name(db_session, "admin")

        assert found is not None
        assert found.id == created.id

    def test_get_roles_list(self, db_session):
        crud.create_role(db_session, make_role(name="admin"))
        crud.create_role(db_session, make_role(name="editor"))
        crud.create_role(db_session, make_role(name="viewer"))

        results = crud.get_roles(db_session)
        assert len(results) == 3

    def test_get_nonexistent_role(self, db_session):
        result = crud.get_role(db_session, 99999)
        assert result is None


# -------------------------------------------------------------------
# Role Assignment
# -------------------------------------------------------------------

class TestRoleAssignment:
    """Tests for assigning roles to users."""

    def test_assign_role(self, db_session):
        user = crud.create_user(db_session, make_user())
        role = crud.create_role(db_session, make_role())

        updated_user = crud.assign_role(db_session, user.id, role.id)

        assert len(updated_user.roles) == 1
        assert updated_user.roles[0].name == "admin"

    def test_assign_role_twice(self, db_session):
        user = crud.create_user(db_session, make_user())
        role = crud.create_role(db_session, make_role())

        crud.assign_role(db_session, user.id, role.id)
        updated_user = crud.assign_role(db_session, user.id, role.id)

        # Idempotent: only one role assigned
        assert len(updated_user.roles) == 1

    def test_assign_role_user_not_found(self, db_session):
        role = crud.create_role(db_session, make_role())

        with pytest.raises(ValueError, match="User not found"):
            crud.assign_role(db_session, 99999, role.id)

    def test_assign_role_role_not_found(self, db_session):
        user = crud.create_user(db_session, make_user())

        with pytest.raises(ValueError, match="Role not found"):
            crud.assign_role(db_session, user.id, 99999)

    def test_filter_users_by_role(self, db_session):
        user_a = crud.create_user(db_session, make_user(
            email="a@example.com", username="usera"
        ))
        crud.create_user(db_session, make_user(
            email="b@example.com", username="userb"
        ))
        role = crud.create_role(db_session, make_role(name="admin"))

        crud.assign_role(db_session, user_a.id, role.id)

        results = crud.get_users(db_session, role="admin")
        assert len(results) == 1
        assert results[0].id == user_a.id
