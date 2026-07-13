"""Resolve long-term forecast horizon values from deployment configuration."""

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CONFIG_ROOT_ENV = "ieasyforecast_configuration_path"
LONG_TERM_CONFIG_DIR_ENV = "ieasyhydroforecast_ml_long_term_configuration"
SUPPORTED_MODES_ENV = "ieasyhydroforecast_ml_long_term_supported_modes"

QUARTER_CONFIG_NAME = "quarter"
OPERATIONAL_MONTH_LEAD_TIME_FIELD = "operational_month_lead_time"
OPERATIONAL_ISSUE_DAY_FIELD = "operational_issue_day"

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


@dataclass(frozen=True)
class OperationalSchedule:
    """Configured operational lead time and issue day for a long-term mode.

    Attributes:
        mode: The long-term mode/config name (e.g. ``"month_1"``,
            ``"quarter"``, ``"seasonal_april"``).
        lead_time: ``operational_month_lead_time`` — the number of whole
            months between issue and target for this mode's single
            operational issuance.
        issue_day: ``operational_issue_day`` — the day-of-month this mode
            is operationally issued on.
    """

    mode: str
    lead_time: int
    issue_day: int


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


def seasonal_horizon_value(issue_month: int) -> int:
    """Return the configured operational month lead time for a seasonal issue month."""
    return _horizon_value_for_mode(seasonal_config_name(issue_month))


def _horizon_value_for_mode(config_name: str) -> int:
    _ensure_supported_mode(config_name)
    config = _load_long_term_config(config_name)
    return _require_int_field(config, config_name, OPERATIONAL_MONTH_LEAD_TIME_FIELD)


def operational_lead_for_mode(mode: str) -> int:
    """Return a mode's operational lead using ONLY its lead-time field.

    Read-path accessor for callers that need just the operational month
    lead (e.g. resolving which ``horizon_value`` to query/display) and not
    the issue day. Unlike ``operational_schedule_for_mode`` — which
    additionally requires ``operational_issue_day`` to be present — this
    reads only ``operational_month_lead_time``, so it succeeds on the
    taj-style configs that omit ``operational_issue_day`` and would
    otherwise crash a dashboard read.

    Args:
        mode: A deployment-supported long-term mode/config name
            (e.g. ``"month_0"``, ``"month_1"``, ``"quarter"``).

    Returns:
        The configured ``operational_month_lead_time`` for the mode.

    Raises:
        UnsupportedLongTermModeError: If `mode` is not in this
            deployment's supported long-term modes.
        LongTermHorizonResolverError: If the mode's config is missing
            ``operational_month_lead_time``, or the field is not an integer.
        FileNotFoundError: If the mode's config file does not exist.
    """
    return _horizon_value_for_mode(mode)


def operational_schedule_for_mode(mode: str) -> OperationalSchedule:
    """Return the configured operational lead time AND issue day for a mode.

    Generic accessor covering every deployment-supported long-term mode
    (``month_0``..``month_N``, ``quarter``, ``seasonal_*``) — unlike
    ``quarter_horizon_value``/``seasonal_horizon_value`` (which only read
    ``operational_month_lead_time``), this requires BOTH
    ``operational_month_lead_time`` AND ``operational_issue_day`` to be
    present on the mode's config, since callers need both to identify a
    row as a genuine operational issuance (see M1 P1 select_operational_
    issuances in ``postprocessing_forecasts/src/data_reader.py``).

    Args:
        mode: A deployment-supported long-term mode/config name.

    Returns:
        OperationalSchedule with the mode's configured lead_time and
        issue_day.

    Raises:
        UnsupportedLongTermModeError: If `mode` is not in this
            deployment's supported long-term modes.
        LongTermHorizonResolverError: If the mode's config is missing
            either required field, or a field is not an integer.
        FileNotFoundError: If the mode's config file does not exist.
    """
    _ensure_supported_mode(mode)
    config = _load_long_term_config(mode)
    lead_time = _require_int_field(config, mode, OPERATIONAL_MONTH_LEAD_TIME_FIELD)
    issue_day = _require_int_field(config, mode, OPERATIONAL_ISSUE_DAY_FIELD)
    return OperationalSchedule(mode=mode, lead_time=lead_time, issue_day=issue_day)


def operational_schedules() -> dict[str, OperationalSchedule]:
    """Return the operational schedule for every deployment-supported mode.

    Raises:
        LongTermHorizonResolverError: If any supported mode's config is
            missing ``operational_month_lead_time`` or
            ``operational_issue_day`` (propagated from
            `operational_schedule_for_mode`, so the error message
            identifies which mode is incomplete).
    """
    return {mode: operational_schedule_for_mode(mode) for mode in supported_long_term_modes()}


def _require_int_field(config: dict[str, Any], config_name: str, field: str) -> int:
    if field not in config:
        raise LongTermHorizonResolverError(
            f"Long-term config '{config_name}' is missing required field '{field}'."
        )

    try:
        return int(config[field])
    except (TypeError, ValueError) as exc:
        raise LongTermHorizonResolverError(
            f"Long-term config '{config_name}' field '{field}' must be an integer."
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
