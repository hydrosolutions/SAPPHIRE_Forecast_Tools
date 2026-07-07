try:
    from iEasyHydroForecast.forecast_library import format_discharge, round_3sf
except ModuleNotFoundError:
    from forecast_library import format_discharge, round_3sf

__all__ = ["format_discharge", "round_3sf"]
