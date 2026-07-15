# src/month_lead.py
"""Shared monthly-lead accessor for the UI layer.

``src/db.py`` resolves "what lead does the main monthly panel show" with a
nested ``_safe_lead`` closure inside ``_get_data_monthly`` — not importable
from outside that function. This module mirrors that same fallback + warn
semantics as standalone, importable helpers for the UI code (plot captions,
bulletin hydration, header text) that need the same answer.

Callers decide whether to consult the ``SAPPHIRE_SKILL_LEAD_AWARE`` flag
(``skill_lead_aware_flag.skill_lead_aware_enabled``) before calling these —
this module does not read the flag itself.
"""

import os

from dashboard.logger import setup_logger
from long_term_horizon_resolver import (
    LongTermHorizonResolverError,
    operational_lead_for_mode,
)

logger = setup_logger()


def _supported_modes() -> list[str]:
    return [
        m.strip()
        for m in os.getenv(
            "ieasyhydroforecast_ml_long_term_supported_modes", ""
        ).split(",")
    ]


def month_lead_for_mode(mode: str, default: int) -> int:
    """Resolve the operational lead for ``mode``, falling back to ``default``.

    Mirrors ``src.db._get_data_monthly``'s ``_safe_lead`` closure: resolves
    using only the config's lead-time field (``operational_lead_for_mode``),
    not the schedule accessor. On any resolution failure
    (``LongTermHorizonResolverError`` or a missing config file), logs a
    warning and falls back to ``default`` rather than raising.
    """
    try:
        return operational_lead_for_mode(mode)
    except (LongTermHorizonResolverError, FileNotFoundError):
        logger.warning(
            "month_lead_for_mode: could not resolve lead for %s; falling back to %s",
            mode,
            default,
        )
        return default


def primary_month_lead() -> int:
    """Resolve the deployment's primary (``month_1``) monthly lead.

    Returns ``operational_lead_for_mode("month_1")`` when ``month_1`` is a
    supported mode for this deployment; otherwise falls back to ``1``
    (matching the legacy hard-coded lead).
    """
    supported_modes = _supported_modes()
    if "month_1" not in supported_modes:
        return 1
    return month_lead_for_mode("month_1", 1)
