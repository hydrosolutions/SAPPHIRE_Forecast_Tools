from __future__ import annotations

import math

import pandas as pd
import pytest

from forecast_skill_eval import api_readers
from forecast_skill_eval.api_readers import (
    read_forecasts,
    read_hydrograph_norms,
    read_long_forecasts,
    read_lr_forecasts,
    read_runoff_observed,
    select_point_value,
)


def _rows(count: int, **base: object) -> list[dict[str, object]]:
    return [base | {"row_id": index} for index in range(count)]


def _lr_row(
    *,
    date: str,
    horizon_in_year: int,
    discharge: float | None = 10.0,
    **overrides: object,
) -> dict[str, object]:
    return {
        "horizon": "pentad",
        "code": "19999",
        "date": date,
        "horizon_in_year": horizon_in_year,
        "forecasted_discharge": discharge,
    } | overrides


def test_paginated_readers_fetch_all_pages(fake_client_factory) -> None:
    client = fake_client_factory(
        forecasts_rows=_rows(
            5,
            horizon="day",
            code="19999",
            model="model-a",
            horizon_in_year=1,
            forecasted_discharge=10.0,
        ),
        long_forecasts_rows=_rows(
            5,
            horizon="month",
            code="19999",
            model="model-a",
            q=10.0,
        ),
        hydrograph_rows=_rows(5, horizon="day", code="19999", discharge=10.0),
        runoff_rows=_rows(5, horizon="day", code="19999", discharge=10.0),
    )

    short_result = read_forecasts(
        client,
        horizon="day",
        code="19999",
        model="model-a",
        target=None,
        start_target="2024-01-01",
        end_target="2024-01-05",
        limit=2,
    )
    long_result = read_long_forecasts(
        client,
        horizon="month",
        code="19999",
        model="model-a",
        horizon_value=None,
        valid_from="2024-01-01",
        valid_to="2024-05-31",
        limit=2,
    )
    norms_result = read_hydrograph_norms(
        client,
        horizon="day",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-01-05",
        limit=2,
    )
    observed_result = read_runoff_observed(
        client,
        horizon="day",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-01-05",
        limit=2,
    )

    assert len(short_result.data) == 5
    assert len(long_result.data) == 5
    assert len(norms_result.data) == 5
    assert len(observed_result.data) == 5

    method_names = [name for name, _kwargs in client.calls]
    assert method_names.count("read_short_term_forecasts") == 3
    assert method_names.count("read_long_term_forecasts") == 3
    assert method_names.count("read_hydrograph") == 3
    assert method_names.count("read_runoff") == 3
    assert "read_forecasts" not in method_names
    assert "read_long_forecasts" not in method_names
    long_calls = [kwargs for name, kwargs in client.calls if name == "read_long_term_forecasts"]
    assert {call["horizon_type"] for call in long_calls} == {"month"}
    assert all("horizon" not in call for call in long_calls)


def test_lr_forecast_reader_paginates_and_normalizes_rows(fake_client_factory) -> None:
    client = fake_client_factory(
        lr_forecasts_rows=_rows(
            5,
            horizon="pentad",
            code="19999",
            date="2024-01-05",
            horizon_in_year=1,
            forecasted_discharge=10.0,
            flag="future-proof",
        ),
    )

    result = read_lr_forecasts(
        client,
        horizon="pentad",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-01-31",
        limit=2,
    )

    assert len(result.data) == 5
    assert result.data["model"].tolist() == ["LR"] * 5
    assert "model_type" not in result.data.columns
    assert result.data["horizon_in_year"].tolist() == [1] * 5
    assert result.data["date"].tolist() == ["2024-01-05"] * 5
    assert result.data["forecasted_discharge"].tolist() == [10.0] * 5
    assert result.data["flag"].tolist() == ["future-proof"] * 5
    assert result.data["point_value"].tolist() == [10.0] * 5
    assert result.data["point_value_note"].tolist() == [""] * 5

    lr_calls = [kwargs for name, kwargs in client.calls if name == "read_lr_forecasts"]
    assert [call["skip"] for call in lr_calls] == [0, 2, 4]
    assert [call["limit"] for call in lr_calls] == [2, 2, 2]


def test_lr_forecast_reader_drops_horizon_zero_sentinels(fake_client_factory) -> None:
    client = fake_client_factory(
        lr_forecasts_rows=[
            _lr_row(date="2024-01-05", horizon_in_year=0, discharge=999.0),
            _lr_row(date="2024-01-10", horizon_in_year=2, discharge=12.5),
        ],
    )

    result = read_lr_forecasts(
        client,
        horizon="pentad",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-01-31",
        limit=10,
    )

    assert len(result.data) == 1
    assert result.dropped_sentinels == 1
    assert result.data.iloc[0]["horizon_in_year"] == 2
    assert result.data.iloc[0]["point_value"] == 12.5
    assert result.data.iloc[0]["point_value_note"] == ""


def test_lr_forecast_reader_notes_null_forecasted_discharge(fake_client_factory) -> None:
    client = fake_client_factory(
        lr_forecasts_rows=[
            _lr_row(date="2024-01-05", horizon_in_year=1, discharge=None),
        ],
    )

    result = read_lr_forecasts(
        client,
        horizon="pentad",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-01-31",
        limit=10,
    )

    assert math.isnan(result.data.iloc[0]["point_value"])
    assert "forecasted_discharge" in result.data.iloc[0]["point_value_note"]


def test_lr_target_synthesis_uses_issue_date_only_for_year(fake_client_factory) -> None:
    client = fake_client_factory(
        lr_forecasts_rows=[
            _lr_row(date="2024-12-31", horizon_in_year=1, row_id="boundary"),
            _lr_row(date="2024-03-25", horizon_in_year=18, row_id="mid-year"),
        ],
    )

    result = read_lr_forecasts(
        client,
        horizon="pentad",
        code="19999",
        start_date="2024-01-01",
        end_date="2024-12-31",
        limit=10,
    )

    rows = result.data.set_index("row_id")
    assert pd.Timestamp(rows.loc["boundary", "target"]).year == 2025
    assert rows.loc["boundary", "horizon_in_year"] == 1
    assert pd.Timestamp(rows.loc["mid-year", "target"]).year == 2024
    assert rows.loc["mid-year", "horizon_in_year"] == 18
    assert "period_key" not in result.data.columns


def test_read_forecasts_output_remains_unchanged(fake_client_factory) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            {
                "horizon": "pentad",
                "code": "19999",
                "model": "ML",
                "date": "2024-01-04",
                "target": "2024-01-05",
                "horizon_in_year": 1,
                "forecasted_discharge": 11.0,
            },
        ],
    )

    result = read_forecasts(
        client,
        horizon="pentad",
        code="19999",
        model="ML",
        target=None,
        start_target="2024-01-01",
        end_target="2024-01-31",
        limit=10,
    )

    row = result.data.iloc[0]
    assert row["model"] == "ML"
    assert row["date"] == "2024-01-04"
    assert row["target"] == "2024-01-05"
    assert row["point_value"] == 11.0
    assert row["point_value_note"] == ""
    assert "read_lr_forecasts" not in [name for name, _kwargs in client.calls]


def test_short_forecast_reader_drops_horizon_zero_sentinels(fake_client_factory) -> None:
    client = fake_client_factory(
        forecasts_rows=[
            {
                "horizon": "day",
                "code": "19999",
                "model": "model-a",
                "horizon_in_year": 0,
                "forecasted_discharge": 999.0,
            },
            {
                "horizon": "day",
                "code": "19999",
                "model": "model-a",
                "horizon_in_year": 3,
                "forecasted_discharge": 12.5,
            },
        ],
    )

    result = read_forecasts(
        client,
        horizon="day",
        code="19999",
        model="model-a",
        target=None,
        start_target="2024-01-01",
        end_target="2024-01-31",
        limit=10,
    )

    assert len(result.data) == 1
    assert result.dropped_sentinels == 1
    assert result.data.iloc[0]["horizon_in_year"] == 3
    assert result.data.iloc[0]["point_value"] == 12.5
    assert result.data.iloc[0]["point_value_note"] == ""


def test_point_value_selection_is_deterministic_for_short_and_long() -> None:
    assert select_point_value({"forecasted_discharge": 8.25}, forecast_type="short") == (
        8.25,
        "",
    )
    assert select_point_value({"q": 1.0, "q50": 2.0, "q_loc": 3.0}, forecast_type="long") == (
        1.0,
        "",
    )
    assert select_point_value({"q": None, "q50": 2.0, "q_loc": 3.0}, forecast_type="long") == (
        2.0,
        "",
    )
    assert select_point_value({"q": None, "q50": None, "q_loc": 3.0}, forecast_type="long") == (
        3.0,
        "",
    )

    value, note = select_point_value(
        {"q": None, "q50": None, "q_loc": None},
        forecast_type="long",
    )
    assert math.isnan(value)
    assert "q, q50, or q_loc" in note


def test_long_forecast_reader_adds_point_values_and_missing_note(fake_client_factory) -> None:
    client = fake_client_factory(
        long_forecasts_rows=[
            {"horizon": "month", "code": "19999", "model": "model-a", "q": 1.0, "q50": 2.0},
            {"horizon": "month", "code": "19999", "model": "model-a", "q": None, "q50": 2.0},
            {"horizon": "month", "code": "19999", "model": "model-a", "q_loc": 3.0},
            {"horizon": "month", "code": "19999", "model": "model-a"},
        ],
    )

    result = read_long_forecasts(
        client,
        horizon="month",
        code="19999",
        model="model-a",
        horizon_value=None,
        valid_from="2024-01-01",
        valid_to="2024-12-31",
        limit=10,
    )

    assert result.data["point_value"].iloc[:3].tolist() == [1.0, 2.0, 3.0]
    assert math.isnan(result.data["point_value"].iloc[3])
    assert "q, q50, or q_loc" in result.data["point_value_note"].iloc[3]


def test_api_client_dependency_gate_uses_standard_skip_message() -> None:
    if not api_readers.SAPPHIRE_API_AVAILABLE:
        pytest.skip("sapphire-api-client not installed")

    assert api_readers.SapphirePostprocessingClient is not None
    assert api_readers.SapphirePreprocessingClient is not None
