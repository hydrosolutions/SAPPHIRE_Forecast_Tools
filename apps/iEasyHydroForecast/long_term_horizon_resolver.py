"""Resolve long-term forecast horizon values from deployment configuration."""

import json
import os
from pathlib import Path
from typing import Any

CONFIG_ROOT_ENV = "ieasyforecast_configuration_path"
LONG_TERM_CONFIG_DIR_ENV = "ieasyhydroforecast_ml_long_term_configuration"
SUPPORTED_MODES_ENV = "ieasyhydroforecast_ml_long_term_supported_modes"

QUARTER_CONFIG_NAME = "quarter"
OPERATIONAL_MONTH_LEAD_TIME_FIELD = "operational_month_lead_time"

_SEASONAL_CONFIG_NAMES_BY_ISSUE_MONTH = {
    1: "seasonal_january",
    2: "seasonal_february",
    3: "seasonal_march",
    4: "seasonal_april",
}


class LongTermHorizonResolverError(RuntimeError):
    """Base error for long-term horizon resolution failures."""


class UnsupportedLongTermModeError(LongTermHorizonResolverError):
    """Raised when the deployment does not support the requested long-term mode."""


def supported_long_term_modes() -> list[str]:
    """Return deployment-supported long-term modes from the environment."""
    raw_modes = _required_env(SUPPORTED_MODES_ENV)
    return [mode.strip() for mode in raw_modes.split(",") if mode.strip()]


def seasonal_config_name(issue_month: int) -> str:
    """Return the seasonal config name for a supported issue month."""
    try:
        return _SEASONAL_CONFIG_NAMES_BY_ISSUE_MONTH[issue_month]
    except KeyError as exc:
        raise ValueError(
            f"Seasonal issue month must be one of 1, 2, 3, or 4; got {issue_month!r}."
        ) from exc


def quarter_horizon_value() -> int:
    """Return the configured operational month lead time for quarterly forecasts."""
    return _horizon_value_for_mode(QUARTER_CONFIG_NAME)


def month_horizon_value(mode: str) -> int:
    """Return the configured operational month lead time for a monthly mode.

    Mirrors :func:`quarter_horizon_value` for the per-config monthly modes
    (e.g. ``"month_0"``, ``"month_1"``). Performs the supported-mode membership
    check and raises :class:`UnsupportedLongTermModeError` when ``mode`` is not
    offered by this deployment.
    """
    return _horizon_value_for_mode(mode)


def seasonal_horizon_value(issue_month: int) -> int:
    """Return the configured operational month lead time for a seasonal issue month."""
    return _horizon_value_for_mode(seasonal_config_name(issue_month))


def _horizon_value_for_mode(config_name: str) -> int:
    _ensure_supported_mode(config_name)
    config = _load_long_term_config(config_name)

    if OPERATIONAL_MONTH_LEAD_TIME_FIELD not in config:
        raise LongTermHorizonResolverError(
            f"Long-term config '{config_name}' is missing required field "
            f"'{OPERATIONAL_MONTH_LEAD_TIME_FIELD}'."
        )

    try:
        return int(config[OPERATIONAL_MONTH_LEAD_TIME_FIELD])
    except (TypeError, ValueError) as exc:
        raise LongTermHorizonResolverError(
            f"Long-term config '{config_name}' field "
            f"'{OPERATIONAL_MONTH_LEAD_TIME_FIELD}' must be an integer."
        ) from exc


def _ensure_supported_mode(config_name: str) -> None:
    modes = supported_long_term_modes()
    if config_name not in modes:
        raise UnsupportedLongTermModeError(
            f"Long-term mode '{config_name}' is not supported by this deployment. "
            f"Supported modes: {modes}."
        )


def _load_long_term_config(config_name: str) -> dict[str, Any]:
    config_path = _long_term_config_path(config_name)
    if not config_path.exists():
        raise FileNotFoundError(f"Long-term config '{config_name}' not found: {config_path}")

    try:
        with config_path.open() as config_file:
            config = json.load(config_file)
    except json.JSONDecodeError as exc:
        raise LongTermHorizonResolverError(
            f"Long-term config '{config_name}' contains invalid JSON: {config_path}"
        ) from exc

    if not isinstance(config, dict):
        raise LongTermHorizonResolverError(
            f"Long-term config '{config_name}' must contain a JSON object: {config_path}"
        )
    return config


def _long_term_config_path(config_name: str) -> Path:
    config_root = Path(_required_env(CONFIG_ROOT_ENV))
    long_term_config_dir = Path(_required_env(LONG_TERM_CONFIG_DIR_ENV))
    return config_root / long_term_config_dir / f"{config_name}.json"


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise LongTermHorizonResolverError(f"Required environment variable '{name}' is not set.")
    return value
