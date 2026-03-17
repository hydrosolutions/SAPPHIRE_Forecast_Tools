"""Shared constants for postprocessing_forecasts tests.

Single source of truth for model name mappings, thresholds, and other
values used across test files.
"""

# Default skill thresholds matching env var defaults.
DEFAULT_THRESHOLDS = {
    "ieasyhydroforecast_efficiency_threshold": "0.6",  # sdivsigma < this
    "ieasyhydroforecast_accuracy_threshold": "0.8",  # accuracy > this
    "ieasyhydroforecast_nse_threshold": "0.8",  # nse > this
}

# Delta represents the measurement uncertainty tolerance band (m^3/s).
# accuracy = fraction of forecasts within observed +/- delta.
DEFAULT_DELTA = 5.0
