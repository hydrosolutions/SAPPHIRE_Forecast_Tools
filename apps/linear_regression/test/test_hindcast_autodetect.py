"""
Tests for API-first hindcast auto-detect with CSV fallback.

Tests get_last_forecast_dates_per_gauge(), _get_last_dates_from_api(),
and _get_last_dates_from_csv() — the three functions refactored in LR-006.
"""

import datetime as dt
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
import forecast_library as fl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from linear_regression import (
    get_last_forecast_dates_per_gauge,
)

# =============================================================================
# Helper: build a minimal LR forecast DataFrame for mocking
# =============================================================================


def _make_lr_df(rows):
    """Return a DataFrame with date/code/value columns from a list of dicts."""
    return pd.DataFrame(rows)


# =============================================================================
# Test 1: API returns LR forecast records for 3 gauges
# =============================================================================


def test_api_returns_three_gauges_pentad():
    """API path returns max date per gauge; CSV is never consulted."""
    mock_client = MagicMock()
    mock_client.read_lr_forecasts.side_effect = [
        _make_lr_df(
            [
                {"date": "2024-06-10", "code": "15013", "value": 1.0},
                {"date": "2024-06-15", "code": "15013", "value": 1.0},
                {"date": "2024-06-10", "code": "15014", "value": 2.0},
                {"date": "2024-06-20", "code": "15014", "value": 2.0},
                {"date": "2024-06-05", "code": "15015", "value": 3.0},
            ]
        ),
        pd.DataFrame(),  # second pagination call → empty → loop breaks
    ]

    with patch.object(fl, "_get_postprocessing_client", return_value=mock_client):
        with patch("linear_regression._get_last_dates_from_csv") as mock_csv:
            result = get_last_forecast_dates_per_gauge("PENTAD")

    assert result["15013"] == dt.date(2024, 6, 15)
    assert result["15014"] == dt.date(2024, 6, 20)
    assert result["15015"] == dt.date(2024, 6, 5)
    assert mock_csv.call_count == 0


# =============================================================================
# Test 2: API returns records for pentad only; mode=BOTH
# =============================================================================


def test_api_pentad_only_mode_both():
    """BOTH mode: pentad data from API wins; empty decade does not trigger CSV."""
    mock_client = MagicMock()

    def _side_effect(horizon, **kwargs):
        if horizon == "pentad" and kwargs.get("skip", 0) == 0:
            return _make_lr_df(
                [
                    {"date": "2024-06-15", "code": "A001", "value": 1.0},
                    {"date": "2024-06-20", "code": "A002", "value": 2.0},
                ]
            )
        # decade call or any pagination continuation → empty
        return pd.DataFrame()

    mock_client.read_lr_forecasts.side_effect = _side_effect

    with patch.object(fl, "_get_postprocessing_client", return_value=mock_client):
        with patch("linear_regression._get_last_dates_from_csv") as mock_csv:
            result = get_last_forecast_dates_per_gauge("BOTH")

    assert result["A001"] == dt.date(2024, 6, 15)
    assert result["A002"] == dt.date(2024, 6, 20)
    assert mock_csv.call_count == 0


# =============================================================================
# Test 3: API raises connection error → fallback to CSV
# =============================================================================


def test_api_connection_error_falls_back_to_csv(tmp_path, monkeypatch):
    """When read_lr_forecasts raises ConnectionError, CSV fallback is used."""
    mock_client = MagicMock()
    mock_client.read_lr_forecasts.side_effect = ConnectionError("refused")

    csv_df = pd.DataFrame(
        {
            "date": ["2024-05-10", "2024-05-15"],
            "code": [15013, 15013],
            "value": [1.0, 1.0],
        }
    )
    csv_path = tmp_path / "forecast_pentad_linreg.csv"
    csv_df.to_csv(csv_path, index=False)

    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.delenv("ieasyforecast_analysis_pentad_file", raising=False)

    with patch.object(fl, "_get_postprocessing_client", return_value=mock_client):
        result = get_last_forecast_dates_per_gauge("PENTAD")

    assert result["15013"] == dt.date(2024, 5, 15)


# =============================================================================
# Test 4: API returns empty DataFrame → fallback to CSV
# =============================================================================


def test_api_empty_dataframe_falls_back_to_csv(tmp_path, monkeypatch):
    """When read_lr_forecasts returns empty DataFrame, CSV fallback is used."""
    mock_client = MagicMock()
    mock_client.read_lr_forecasts.return_value = pd.DataFrame()

    csv_df = pd.DataFrame(
        {
            "date": ["2024-04-05", "2024-04-10"],
            "code": [20001, 20001],
            "value": [5.0, 5.0],
        }
    )
    csv_path = tmp_path / "forecast_pentad_linreg.csv"
    csv_df.to_csv(csv_path, index=False)

    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.delenv("ieasyforecast_analysis_pentad_file", raising=False)

    with patch.object(fl, "_get_postprocessing_client", return_value=mock_client):
        result = get_last_forecast_dates_per_gauge("PENTAD")

    assert result["20001"] == dt.date(2024, 4, 10)


# =============================================================================
# Test 5: _get_postprocessing_client() returns None → fallback to CSV
# =============================================================================


def test_client_none_falls_back_to_csv(tmp_path, monkeypatch):
    """When client is None (API not configured), CSV fallback is used."""
    csv_df = pd.DataFrame(
        {
            "date": ["2024-03-05", "2024-03-10"],
            "code": [30001, 30001],
            "value": [7.0, 7.0],
        }
    )
    csv_path = tmp_path / "forecast_pentad_linreg.csv"
    csv_df.to_csv(csv_path, index=False)

    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.delenv("ieasyforecast_analysis_pentad_file", raising=False)

    with patch.object(fl, "_get_postprocessing_client", return_value=None):
        result = get_last_forecast_dates_per_gauge("PENTAD")

    assert result["30001"] == dt.date(2024, 3, 10)


# =============================================================================
# Test 6: CSV fallback with env-var custom filename (pentad)
# =============================================================================


def test_csv_fallback_custom_pentad_filename(tmp_path, monkeypatch, disable_api_client):
    """CSV fallback reads the filename given by ieasyforecast_analysis_pentad_file."""
    csv_df = pd.DataFrame(
        {
            "date": ["2024-06-15", "2024-06-20"],
            "code": [15013, 15013],
            "value": [1.0, 1.0],
        }
    )
    csv_path = tmp_path / "my_custom_pentad.csv"
    csv_df.to_csv(csv_path, index=False)

    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_analysis_pentad_file", "my_custom_pentad.csv")

    result = get_last_forecast_dates_per_gauge("PENTAD")

    assert result["15013"] == dt.date(2024, 6, 20)


# =============================================================================
# Test 7: CSV fallback, env var not set, default CSV exists
# =============================================================================


def test_csv_fallback_default_pentad_filename(tmp_path, monkeypatch, disable_api_client):
    """Without ieasyforecast_analysis_pentad_file, default filename is used."""
    csv_df = pd.DataFrame(
        {
            "date": ["2024-06-10", "2024-06-15"],
            "code": [15014, 15014],
            "value": [2.0, 2.0],
        }
    )
    csv_path = tmp_path / "forecast_pentad_linreg.csv"
    csv_df.to_csv(csv_path, index=False)

    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.delenv("ieasyforecast_analysis_pentad_file", raising=False)

    result = get_last_forecast_dates_per_gauge("PENTAD")

    assert result["15014"] == dt.date(2024, 6, 15)


# =============================================================================
# Test 8: CSV fallback, env-var-named file does not exist → empty dict
# =============================================================================


def test_csv_fallback_missing_file_returns_empty(tmp_path, monkeypatch, disable_api_client):
    """When the env-var-named CSV does not exist, an empty dict is returned."""
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_analysis_pentad_file", "nonexistent.csv")

    result = get_last_forecast_dates_per_gauge("PENTAD")

    assert result == {}


# =============================================================================
# Test 9: API returns data; CSV has additional gauges — API result wins
# =============================================================================


def test_api_result_wins_over_csv(tmp_path, monkeypatch):
    """API data is used; CSV gauge C not present because CSV is not consulted."""
    mock_client = MagicMock()
    mock_client.read_lr_forecasts.side_effect = [
        _make_lr_df(
            [
                {"date": "2024-06-15", "code": "A", "value": 1.0},
                {"date": "2024-06-20", "code": "B", "value": 2.0},
            ]
        ),
        pd.DataFrame(),  # pagination ends
    ]

    csv_df = pd.DataFrame(
        {
            "date": ["2024-06-10", "2024-06-10", "2024-06-10"],
            "code": ["A", "B", "C"],
            "value": [1.0, 2.0, 3.0],
        }
    )
    csv_path = tmp_path / "forecast_pentad_linreg.csv"
    csv_df.to_csv(csv_path, index=False)

    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.delenv("ieasyforecast_analysis_pentad_file", raising=False)

    with patch.object(fl, "_get_postprocessing_client", return_value=mock_client):
        result = get_last_forecast_dates_per_gauge("PENTAD")

    assert "A" in result
    assert "B" in result
    assert "C" not in result


# =============================================================================
# Test 10: API returns records; mode=DECAD only
# =============================================================================


def test_api_decad_mode_only():
    """DECAD mode: only horizon='decade' is queried; pentad is never requested."""
    mock_client = MagicMock()
    mock_client.read_lr_forecasts.side_effect = [
        _make_lr_df(
            [
                {"date": "2024-06-10", "code": "D001", "value": 1.0},
                {"date": "2024-06-20", "code": "D002", "value": 2.0},
            ]
        ),
        pd.DataFrame(),  # pagination ends
    ]

    with patch.object(fl, "_get_postprocessing_client", return_value=mock_client):
        result = get_last_forecast_dates_per_gauge("DECAD")

    assert result["D001"] == dt.date(2024, 6, 10)
    assert result["D002"] == dt.date(2024, 6, 20)

    # Verify read_lr_forecasts was called with horizon="decade" only
    call_args_list = mock_client.read_lr_forecasts.call_args_list
    horizons_queried = [call.kwargs.get("horizon") for call in call_args_list]
    assert "decade" in horizons_queried
    assert "pentad" not in horizons_queried
    # Exactly one call with actual data (second pagination call may occur)
    decade_calls = [h for h in horizons_queried if h == "decade"]
    assert len(decade_calls) >= 1


# =============================================================================
# Test 11: CSV fallback with ieasyforecast_analysis_decad_file env var
# =============================================================================


def test_csv_fallback_custom_decad_filename(tmp_path, monkeypatch, disable_api_client):
    """CSV fallback reads the filename given by ieasyforecast_analysis_decad_file."""
    csv_df = pd.DataFrame(
        {
            "date": ["2024-06-10", "2024-06-20"],
            "code": [16001, 16001],
            "value": [4.0, 4.0],
        }
    )
    csv_path = tmp_path / "custom_decad.csv"
    csv_df.to_csv(csv_path, index=False)

    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyforecast_analysis_decad_file", "custom_decad.csv")

    result = get_last_forecast_dates_per_gauge("DECAD")

    assert result["16001"] == dt.date(2024, 6, 20)
