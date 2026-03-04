"""Short-term horizon configuration for pentad/decad parameterization.

Provides a frozen dataclass that captures all pentad/decad-specific
parameters so that paired functions can be collapsed into single
parameterized implementations.

The dataclass is dependency-free — PENTAD/DECAD instances are created
in entry points where tag_library and setup_library are already imported.
"""

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ShortTermHorizonConfig:
    """All pentad/decad-specific parameters in one place."""

    name: str  # "pentad" or "decad"
    period_col: str  # "pentad_in_year" or "decad_in_year"
    period_in_month_col: str  # "pentad_in_month" or "decad_in_month"
    get_period_func: Callable  # tl.get_pentad or tl.get_decad_in_month
    combined_csv_env: str  # env var name for combined forecast CSV
    skill_csv_env: str  # env var name for skill metrics CSV
    api_horizon_type: str  # "pentad" or "decad"
    neural_ensemble_func: Callable  # sl.calculate_neural_ensemble_forecast[_decade]
