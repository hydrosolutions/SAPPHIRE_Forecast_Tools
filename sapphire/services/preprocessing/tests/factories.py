"""
Sample data factory helpers for preprocessing service tests.

These create Pydantic schema objects with sensible defaults.
Override any field via keyword arguments.
"""

from datetime import date

from app.schemas import RunoffCreate, HydrographCreate, MeteoCreate, SnowCreate


def make_runoff(**overrides):
    """Create a RunoffCreate with sensible defaults."""
    defaults = {
        "horizon_type": "pentad",
        "code": "15013",
        "date": date(2024, 6, 15),
        "discharge": 100.0,
        "predictor": 80.0,
        "horizon_value": 3,
        "horizon_in_year": 33,
    }
    defaults.update(overrides)
    return RunoffCreate(**defaults)


def make_hydrograph(**overrides):
    """Create a HydrographCreate with sensible defaults."""
    defaults = {
        "horizon_type": "pentad",
        "code": "15013",
        "date": date(2024, 6, 15),
        "horizon_value": 3,
        "horizon_in_year": 33,
        "day_of_year": 167,
        "count": 30,
        "mean": 95.0,
        "std": 15.0,
        "min": 60.0,
        "max": 140.0,
        "q05": 65.0,
        "q25": 80.0,
        "q50": 95.0,
        "q75": 110.0,
        "q95": 135.0,
        "norm": 90.0,
        "previous": 88.0,
        "current": 92.0,
    }
    defaults.update(overrides)
    return HydrographCreate(**defaults)


def make_meteo(**overrides):
    """Create a MeteoCreate with sensible defaults."""
    defaults = {
        "meteo_type": "T",
        "code": "15013",
        "date": date(2024, 6, 15),
        "value": 22.5,
        "norm": 20.0,
        "day_of_year": 167,
    }
    defaults.update(overrides)
    return MeteoCreate(**defaults)


def make_snow(**overrides):
    """Create a SnowCreate with sensible defaults."""
    defaults = {
        "snow_type": "HS",
        "code": "15013",
        "date": date(2024, 6, 15),
        "value": 50.0,
        "norm": 45.0,
    }
    defaults.update(overrides)
    return SnowCreate(**defaults)
