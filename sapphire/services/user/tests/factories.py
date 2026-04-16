"""
Sample data factory helpers for user service tests.

These create Pydantic schema objects with sensible defaults.
Override any field via keyword arguments.
"""

from app.schemas import UserCreate, RoleCreate


def make_user(**overrides):
    """Create a UserCreate with sensible defaults."""
    defaults = {
        "email": "test@example.com",
        "username": "testuser",
        "password": "securepass123",
        "full_name": "Test User",
    }
    defaults.update(overrides)
    return UserCreate(**defaults)


def make_role(**overrides):
    """Create a RoleCreate with sensible defaults."""
    defaults = {
        "name": "admin",
        "description": "Administrator role",
    }
    defaults.update(overrides)
    return RoleCreate(**defaults)
