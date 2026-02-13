"""
Sample data factory helpers for postprocessing service tests.

These create Pydantic schema objects with sensible defaults.
Override any field via keyword arguments.
"""

from datetime import date

from app.schemas import (
    ForecastCreate,
    LongForecastCreate,
    LRForecastCreate,
    SkillMetricCreate,
)


def make_forecast(**overrides):
    """Create a ForecastCreate with sensible defaults."""
    defaults = {
        "horizon_type": "pentad",
        "code": "15013",
        "model_type": "LR",
        "date": date(2024, 6, 15),
        "target": date(2024, 6, 20),
        "flag": 1,
        "horizon_value": 3,
        "horizon_in_year": 33,
        "q05": 80.0,
        "q25": 90.0,
        "q50": 95.0,
        "q75": 110.0,
        "q95": 120.0,
        "forecasted_discharge": 100.0,
    }
    defaults.update(overrides)
    return ForecastCreate(**defaults)


def make_long_forecast(**overrides):
    """Create a LongForecastCreate with sensible defaults."""
    defaults = {
        "horizon_type": "month",
        "horizon_value": 1,
        "code": "15013",
        "date": date(2024, 6, 15),
        "model_type": "GBT",
        "valid_from": date(2024, 7, 1),
        "valid_to": date(2024, 7, 31),
        "flag": 0,
        "q": 123.45,
        "q_obs": 120.0,
        "q05": 100.0,
        "q25": 115.0,
        "q50": 123.0,
        "q75": 130.0,
        "q95": 140.0,
    }
    defaults.update(overrides)
    return LongForecastCreate(**defaults)


def make_lr_forecast(**overrides):
    """Create a LRForecastCreate with sensible defaults."""
    defaults = {
        "horizon_type": "pentad",
        "code": "15013",
        "date": date(2024, 6, 15),
        "horizon_value": 3,
        "horizon_in_year": 33,
        "discharge_avg": 100.0,
        "predictor": 80.0,
        "slope": 1.2,
        "intercept": 10.0,
        "forecasted_discharge": 106.0,
        "q_mean": 95.0,
        "q_std_sigma": 15.0,
        "delta": 5.0,
        "rsquared": 0.85,
    }
    defaults.update(overrides)
    return LRForecastCreate(**defaults)


def make_skill_metric(**overrides):
    """Create a SkillMetricCreate with sensible defaults."""
    defaults = {
        "horizon_type": "pentad",
        "code": "15013",
        "model_type": "LR",
        "date": date(2024, 6, 15),
        "horizon_in_year": 33,
        "sdivsigma": 0.8,
        "nse": 0.75,
        "delta": 5.0,
        "accuracy": 0.85,
        "mae": 10.5,
        "n_pairs": 50,
    }
    defaults.update(overrides)
    return SkillMetricCreate(**defaults)
