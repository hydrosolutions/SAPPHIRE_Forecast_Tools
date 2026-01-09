"""
Configuration loader for the preprocessing_runoff module.

Loads settings from config.yaml with environment variable overrides.
"""

import os
import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Default configuration values
DEFAULTS = {
    'maintenance': {
        'lookback_days': 30,
    },
    'operational': {
        'fetch_yesterday': True,
        'fetch_morning': True,
    },
}


def _get_config_path() -> Path:
    """Get the path to the config.yaml file."""
    # Config file is in the module root (parent of src/)
    src_dir = Path(__file__).parent
    module_dir = src_dir.parent
    return module_dir / 'config.yaml'


def _apply_env_overrides(config: dict) -> dict:
    """Apply environment variable overrides to configuration."""
    # Maintenance lookback days
    env_lookback = os.getenv('PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS')
    if env_lookback is not None:
        try:
            config['maintenance']['lookback_days'] = int(env_lookback)
            logger.debug(f"Override maintenance.lookback_days from env: {env_lookback}")
        except ValueError:
            logger.warning(
                f"Invalid PREPROCESSING_MAINTENANCE_LOOKBACK_DAYS value: {env_lookback}, "
                f"using default: {config['maintenance']['lookback_days']}"
            )

    return config


def load_config() -> dict:
    """
    Load configuration from config.yaml with defaults and env overrides.

    Priority (highest to lowest):
    1. Environment variables
    2. config.yaml values
    3. Default values

    Returns:
        dict: Configuration dictionary with 'maintenance' and 'operational' keys.
    """
    config = DEFAULTS.copy()
    config['maintenance'] = DEFAULTS['maintenance'].copy()
    config['operational'] = DEFAULTS['operational'].copy()

    config_path = _get_config_path()

    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                file_config = yaml.safe_load(f)

            if file_config:
                # Merge file config into defaults
                if 'maintenance' in file_config:
                    config['maintenance'].update(file_config['maintenance'])
                if 'operational' in file_config:
                    config['operational'].update(file_config['operational'])

            logger.debug(f"Loaded config from {config_path}")
        except Exception as e:
            logger.warning(f"Failed to load config from {config_path}: {e}, using defaults")
    else:
        logger.debug(f"Config file not found at {config_path}, using defaults")

    # Apply environment variable overrides
    config = _apply_env_overrides(config)

    return config


def get_maintenance_lookback_days() -> int:
    """Get the number of days to look back in maintenance mode."""
    config = load_config()
    return config['maintenance']['lookback_days']


def get_operational_settings() -> dict:
    """Get operational mode settings."""
    config = load_config()
    return config['operational']


# Module-level config instance (lazy loaded)
_config: dict | None = None


def get_config() -> dict:
    """
    Get the module configuration (cached).

    Returns:
        dict: Configuration dictionary.
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reload_config() -> dict:
    """
    Reload configuration from file (clears cache).

    Returns:
        dict: Fresh configuration dictionary.
    """
    global _config
    _config = load_config()
    return _config
