"""Tests for the config module."""

import os
import sys
from pathlib import Path

import pytest

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
import config


class TestLoadConfig:
    """Tests for config loading functionality."""

    def setup_method(self):
        """Clear config cache before each test."""
        config._config = None

    def test_load_config_defaults_when_file_missing(self, tmp_path, monkeypatch):
        """Test that default values are returned when no config file exists."""
        # Point the config loader at a non-existent file in a temp directory
        monkeypatch.setattr(config, '_get_config_path', lambda: tmp_path / 'config.yaml')

        cfg = config.load_config()

        assert cfg['maintenance']['lookback_days'] == 30
        assert cfg['operational']['fetch_yesterday'] is True
        assert cfg['operational']['fetch_morning'] is True

    def test_load_config_from_actual_file(self):
        """Test loading configuration from the actual config.yaml file."""
        config_path = Path(__file__).parent.parent / 'config.yaml'

        if not config_path.exists():
            pytest.skip("config.yaml not found in module directory")

        cfg = config.load_config()

        # Verify the structure matches expectations
        assert 'maintenance' in cfg
        assert 'operational' in cfg
        assert 'lookback_days' in cfg['maintenance']
        assert isinstance(cfg['maintenance']['lookback_days'], int)

    def test_env_override_lookback_days(self):
        """Test that environment variable overrides config file value."""
        original_value = os.environ.get('PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS')

        try:
            os.environ['PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS'] = '60'
            cfg = config.load_config()

            assert cfg['maintenance']['lookback_days'] == 60
        finally:
            # Restore original environment
            if original_value is None:
                os.environ.pop('PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS', None)
            else:
                os.environ['PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS'] = original_value

    def test_env_override_invalid_value_uses_default(self):
        """Test that invalid environment variable is ignored with warning."""
        original_value = os.environ.get('PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS')

        try:
            os.environ['PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS'] = 'not_a_number'
            cfg = config.load_config()

            # Should fall back to default or file value
            assert isinstance(cfg['maintenance']['lookback_days'], int)
        finally:
            # Restore original environment
            if original_value is None:
                os.environ.pop('PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS', None)
            else:
                os.environ['PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS'] = original_value


class TestGetMaintenanceLookbackDays:
    """Tests for get_maintenance_lookback_days function."""

    def setup_method(self):
        """Clear config cache before each test."""
        config._config = None

    def test_returns_integer(self):
        """Test that the function returns an integer."""
        days = config.get_maintenance_lookback_days()
        assert isinstance(days, int)
        assert days > 0

    def test_returns_env_override(self):
        """Test that environment variable override works."""
        original_value = os.environ.get('PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS')

        try:
            os.environ['PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS'] = '14'
            config._config = None  # Clear cache
            days = config.get_maintenance_lookback_days()

            assert days == 14
        finally:
            config._config = None  # Clear cache
            if original_value is None:
                os.environ.pop('PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS', None)
            else:
                os.environ['PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS'] = original_value


class TestGetOperationalSettings:
    """Tests for get_operational_settings function."""

    def setup_method(self):
        """Clear config cache before each test."""
        config._config = None

    def test_returns_dict_with_expected_keys(self):
        """Test that operational settings contain expected keys."""
        settings = config.get_operational_settings()

        assert isinstance(settings, dict)
        assert 'fetch_yesterday' in settings
        assert 'fetch_morning' in settings


class TestConfigCaching:
    """Tests for configuration caching behavior."""

    def test_get_config_returns_same_instance(self):
        """Test that get_config returns cached configuration."""
        config._config = None

        cfg1 = config.get_config()
        cfg2 = config.get_config()

        # Should be the same object (cached)
        assert cfg1 is cfg2

    def test_reload_config_clears_cache(self):
        """Test that reload_config returns fresh configuration."""
        config._config = None

        _ = config.get_config()  # Load initial config
        cfg2 = config.reload_config()

        # After reload, the cache should point to the new config
        assert config._config is cfg2
