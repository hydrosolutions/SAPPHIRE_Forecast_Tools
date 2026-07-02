"""Contract: the summary table must agree with the forecast warning banner.

`widgets.get_forecast_warning` treats a model as "present" only when it has a
non-null `forecasted_discharge` on the selected date, and otherwise reports it
in the "No forecast data available for models ..." message. The summary table
built by `vizualization.create_forecast_summary_table` must match that: rows
whose `forecasted_discharge` is null on the max/selected date must be dropped,
not shown with a blank discharge cell.
"""

from types import SimpleNamespace

import numpy as np
import pandas as pd
from src import vizualization


def _model_selection():
    return SimpleNamespace(options={"LR Base": "LR_Base", "LR SM": "LR_SM"})


def _summary_table(forecasts_all):
    return vizualization.create_forecast_summary_table(
        lambda value: value,
        "season",
        forecasts_all,
        "Test River B",
        "2026-03-22",
        _model_selection(),
        "delta",
        0,
    )


def test_null_discharge_model_excluded_valid_model_retained():
    # Arrange: two models on the same max date, one with a null discharge
    forecasts_all = pd.DataFrame(
        [
            {
                "station_labels": "Test River B",
                "date": "2026-03-22",
                "model_short": "LR_Base",
                "forecasted_discharge": 100.0,
            },
            {
                "station_labels": "Test River B",
                "date": "2026-03-22",
                "model_short": "LR_SM",
                "forecasted_discharge": np.nan,
            },
        ]
    )

    # Act
    result = _summary_table(forecasts_all)

    # Assert: the null-discharge model is dropped, the valid one is kept
    assert "LR_Base" in result["Model"].values
    assert "LR_SM" not in result["Model"].values


def test_all_models_null_on_max_date_yields_empty_table():
    # Arrange: both models present but both have a null discharge
    forecasts_all = pd.DataFrame(
        [
            {
                "station_labels": "Test River B",
                "date": "2026-03-22",
                "model_short": "LR_Base",
                "forecasted_discharge": np.nan,
            },
            {
                "station_labels": "Test River B",
                "date": "2026-03-22",
                "model_short": "LR_SM",
                "forecasted_discharge": np.nan,
            },
        ]
    )

    # Act
    result = _summary_table(forecasts_all)

    # Assert: nothing survives the null-discharge filter
    assert result.empty
