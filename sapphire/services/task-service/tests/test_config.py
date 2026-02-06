import pytest
from app.config import get_settings, Settings


def test_get_settings():
    """Test that settings can be loaded"""
    settings = get_settings()
    assert settings is not None
    assert isinstance(settings, Settings)


def test_settings_has_required_fields():
    """Test that settings has all required fields"""
    settings = get_settings()
    assert hasattr(settings, 'app_name')
    assert hasattr(settings, 'database_url')
    assert hasattr(settings, 'log_level')
    assert hasattr(settings, 'cors_origins')


def test_settings_default_values():
    """Test default values in settings"""
    settings = get_settings()
    assert settings.app_name == "Task Management Service"
    assert settings.version == "1.0.0"
    assert settings.debug is False
    assert settings.log_level == "INFO"


def test_settings_singleton():
    """Test that get_settings returns the same instance (cached)"""
    settings1 = get_settings()
    settings2 = get_settings()
    assert settings1 is settings2
