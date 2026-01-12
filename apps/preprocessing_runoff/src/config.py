"""
Configuration loader for the preprocessing_runoff module.

Loads settings from config.yaml with environment variable overrides.
"""

import os
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# Valid log levels and default
VALID_LOG_LEVELS = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
DEFAULT_LOG_LEVEL = 'INFO'

# Default configuration values
DEFAULTS = {
    'logging': {
        'log_level': None,  # None means use env or default INFO
    },
    'maintenance': {
        'lookback_days': 30,
    },
    'operational': {
        'fetch_yesterday': True,
        'fetch_morning': True,
    },
    'validation': {
        'enabled': True,
        'max_age_days': 3,
        'reliability_threshold': 80.0,
        'stats_file': 'reliability_stats.json',
    },
    'spot_check': {
        'enabled': True,
    },
    'site_cache': {
        'enabled': True,
        'cache_file': 'forecast_sites_cache.json',
        'max_age_days': 7,
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
    config['logging'] = DEFAULTS['logging'].copy()
    config['maintenance'] = DEFAULTS['maintenance'].copy()
    config['operational'] = DEFAULTS['operational'].copy()
    config['validation'] = DEFAULTS['validation'].copy()
    config['spot_check'] = DEFAULTS['spot_check'].copy()
    config['site_cache'] = DEFAULTS['site_cache'].copy()

    config_path = _get_config_path()

    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                file_config = yaml.safe_load(f)

            if file_config:
                # Merge file config into defaults
                if 'logging' in file_config:
                    config['logging'].update(file_config['logging'])
                if 'maintenance' in file_config:
                    config['maintenance'].update(file_config['maintenance'])
                if 'operational' in file_config:
                    config['operational'].update(file_config['operational'])
                if 'validation' in file_config:
                    config['validation'].update(file_config['validation'])
                if 'spot_check' in file_config:
                    config['spot_check'].update(file_config['spot_check'])
                if 'site_cache' in file_config:
                    config['site_cache'].update(file_config['site_cache'])

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


def get_validation_settings() -> dict:
    """
    Get validation settings for post-write data validation.

    Returns:
        dict with keys:
        - enabled: bool - whether validation is enabled
        - max_age_days: int - max age of data before flagging
        - reliability_threshold: float - percentage below which sites are flagged
        - stats_file: str - filename for reliability stats (relative path)
    """
    config = load_config()
    return config['validation']


def get_spot_check_settings() -> dict:
    """
    Get spot-check validation settings.

    Returns:
        dict with keys:
        - enabled: bool - whether spot-check validation is enabled
    """
    config = load_config()
    return config['spot_check']


def get_site_cache_settings() -> dict:
    """
    Get site cache settings.

    Returns:
        dict with keys:
        - enabled: bool - whether site caching is enabled
        - cache_file: str - filename for cache (relative to intermediate_data_path)
        - max_age_days: int - maximum cache age in days before considered stale
    """
    config = load_config()
    return config['site_cache']


def get_log_level() -> int:
    """
    Get the log level for the module.

    Priority (highest to lowest):
    1. config.yaml logging.log_level
    2. Environment variable 'log_level'
    3. Default: INFO

    Returns:
        int: Logging level constant (e.g., logging.INFO, logging.DEBUG)
    """
    config = load_config()

    # Check config.yaml first
    config_level = config.get('logging', {}).get('log_level')
    if config_level is not None:
        level_str = str(config_level).upper()
        if level_str in VALID_LOG_LEVELS:
            return getattr(logging, level_str)
        else:
            logger.warning(
                f"[CONFIG] Invalid log_level in config.yaml: {config_level}, "
                f"valid levels: {VALID_LOG_LEVELS}"
            )

    # Fall back to environment variable
    env_level = os.getenv('log_level')
    if env_level is not None:
        level_str = env_level.upper()
        if level_str in VALID_LOG_LEVELS:
            return getattr(logging, level_str)
        else:
            logger.warning(
                f"[CONFIG] Invalid log_level in environment: {env_level}, "
                f"valid levels: {VALID_LOG_LEVELS}"
            )

    # Default
    return getattr(logging, DEFAULT_LOG_LEVEL)


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
