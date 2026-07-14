"""Shared feature-flag helper for the M1 lead-aware skill-metrics campaign.

``SAPPHIRE_SKILL_LEAD_AWARE`` will gate the config-driven per-lead
operational skill/ensemble work landing in ``apps/postprocessing_forecasts``
(and its dashboard/reader follow-ups) across phases P1-P4 of
``doc/plans/issues/high_prio_gi_draft_pp_lead_aware_skill.md``.

Default is OFF. As of P0 (this module), nothing is wired to the flag yet —
it is scaffolding only. Once call sites are gated (P1+), flag-OFF must
reproduce current trunk behavior byte-for-byte, INCLUDING the
already-shipped PP-038 monthly per-lead skill/ensemble stratification
(``GROUP_COLS``/``ENSEMBLE_KEY`` in ``skill_metrics.py`` and the
``horizon_value``-aware merges in ``ensemble_calculator.py``). PP-038 is
unconditional trunk behavior, not gated by this flag, and must NOT be
reverted by any flag-OFF path.

Mirrors the boolean ``_env_flag`` helper in
``apps/forecast_skill_eval/src/forecast_skill_eval/cli.py`` and the
explicit-token parsing style of ``_parse_threshold_env`` in
``apps/postprocessing_forecasts/src/skill_metrics.py``: case-insensitive,
whitespace-tolerant, and an explicit truthy/falsey token set rather than a
bare truthiness check on the raw string (so e.g. a stray
``SAPPHIRE_SKILL_LEAD_AWARE=0`` is unambiguously OFF, not "truthy because
non-empty").
"""

import os

SKILL_LEAD_AWARE_ENV = "SAPPHIRE_SKILL_LEAD_AWARE"

_TRUTHY_TOKENS = frozenset({"1", "true", "yes", "on"})
_FALSEY_TOKENS = frozenset({"", "0", "false", "no", "off"})


def skill_lead_aware_enabled() -> bool:
    """Return whether the M1 lead-aware skill/ensemble flag is ON.

    Reads ``SAPPHIRE_SKILL_LEAD_AWARE`` from the environment. Default
    (unset) is OFF.

    Returns:
        ``True`` if the env var holds a recognized truthy token
        (``1``/``true``/``yes``/``on``, case-insensitive), ``False`` if
        unset or a recognized falsey token (``0``/``false``/``no``/``off``).

    Raises:
        ValueError: If the env var is set to a value that is neither a
            recognized truthy nor falsey token — a typo'd value fails
            loudly instead of silently resolving to OFF.
    """
    raw = os.environ.get(SKILL_LEAD_AWARE_ENV, "")
    token = raw.strip().lower()
    if token in _TRUTHY_TOKENS:
        return True
    if token in _FALSEY_TOKENS:
        return False
    raise ValueError(
        f"Invalid value for {SKILL_LEAD_AWARE_ENV}: {raw!r} (expected one of "
        f"{sorted(_TRUTHY_TOKENS)} for ON or {sorted(_FALSEY_TOKENS)} for OFF)"
    )
