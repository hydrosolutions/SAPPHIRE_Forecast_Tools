"""LOCKED acceptance test for milestone M2 — linear_regression stops writing
pentad/decad hydrograph rows.

The pentad/decad hydrograph call sites in linear_regression.py (historically the
``fl.write_pentad_hydrograph_data(...)`` / ``fl.write_decad_hydrograph_data(...)``
calls around lines 788/808) must be removed: preprocessing_runoff owns the
pentad/decad hydrograph row after M2. This is checked at the call-site level
(an active, non-comment call), which is exactly what the criterion names.

The forecast_library helpers may still EXIST (their retirement of the API sink
is covered by the iEasyHydroForecast M2 test); what must be gone here is the
linear_regression *call site*.
"""

import os
import re

_LR_SOURCE = os.path.join(os.path.dirname(__file__), "..", "linear_regression.py")

# Matches an active CALL: the function name immediately followed by "(".
# Log-message string literals use the bare name without a "(" and are ignored.
_PENTAD_CALL = re.compile(r"write_pentad_hydrograph_data\s*\(")
_DECAD_CALL = re.compile(r"write_decad_hydrograph_data\s*\(")


def _active_lines():
    with open(_LR_SOURCE, encoding="utf-8") as handle:
        for raw in handle:
            stripped = raw.strip()
            if stripped and not stripped.startswith("#"):
                yield raw


def test_lr_has_no_active_pentad_hydrograph_write_call():
    offenders = [ln for ln in _active_lines() if _PENTAD_CALL.search(ln)]
    assert not offenders, (
        "linear_regression.py still calls write_pentad_hydrograph_data; M2 must "
        f"remove the pentad hydrograph call site. Offending lines: {offenders}"
    )


def test_lr_has_no_active_decad_hydrograph_write_call():
    offenders = [ln for ln in _active_lines() if _DECAD_CALL.search(ln)]
    assert not offenders, (
        "linear_regression.py still calls write_decad_hydrograph_data; M2 must "
        f"remove the decad hydrograph call site. Offending lines: {offenders}"
    )
