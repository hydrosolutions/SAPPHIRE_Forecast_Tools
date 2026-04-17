"""Tests for add_new_station.py per-code API read refactor.

Covers:
- Per-code reads when org config is available
- Fallback with extended timeout when config is unavailable
- Stale stations not treated as new
- Genuinely new stations trigger hindcast
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd

# Mock heavy dependencies before importing add_new_station
sys.modules["darts"] = MagicMock()
sys.modules["darts.TimeSeries"] = MagicMock()
sys.modules["darts.concatenate"] = MagicMock()
sys.modules["darts.utils"] = MagicMock()
sys.modules["darts.utils.timeseries_generation"] = MagicMock()
sys.modules["darts.models"] = MagicMock()
sys.modules["pytorch_lightning"] = MagicMock()
sys.modules["pytorch_lightning.callbacks"] = MagicMock()
sys.modules["torch"] = MagicMock()
sys.modules["pe_oudin"] = MagicMock()
sys.modules["pe_oudin.PE_Oudin"] = MagicMock()
sys.modules["suntime"] = MagicMock()

_mock_sl = MagicMock()
_mock_sl.load_environment = MagicMock()
sys.modules["setup_library"] = _mock_sl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scr"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

import add_new_station  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_forecast_df(codes=None, n_days=5):
    """Build a minimal forecast DataFrame for testing.

    Args:
        codes: List of station codes. Defaults to ["19999"].
        n_days: Number of consecutive forecast dates per code.

    Returns:
        DataFrame matching the expected forecast schema.
    """
    if codes is None:
        codes = ["19999"]
    rows = []
    for code in codes:
        for i in range(n_days):
            rows.append(
                {
                    "code": str(code),
                    "forecast_date": pd.Timestamp("2024-01-01") + pd.Timedelta(days=i),
                    "date": pd.Timestamp("2024-01-06") + pd.Timedelta(days=i),
                    "flag": 0,
                    "Q5": 50.0,
                    "Q25": 80.0,
                    "Q50": 100.0,
                    "Q75": 120.0,
                    "Q95": 150.0,
                }
            )
    return pd.DataFrame(rows)


def _make_hydroposts_df(codes, model="TFT"):
    """Build a minimal hydroposts_available_for_ml_forecasting DataFrame.

    Args:
        codes: List of station codes to include.
        model: Which model column to set True. All columns are set True for
            simplicity — the model parameter is kept for API compatibility.

    Returns:
        DataFrame with code and model flag columns.
    """
    return pd.DataFrame(
        {
            "code": [str(c) for c in codes],
            "TFT": [True] * len(codes),
            "TIDE": [True] * len(codes),
            "TSMIXER": [True] * len(codes),
            "ARIMA": [True] * len(codes),
        }
    )


_BASE_ENV = {
    "SAPPHIRE_MODEL_TO_USE": "TFT",
    "ieasyforecast_intermediate_data_path": "/tmp/test_add_station",
    "ieasyhydroforecast_OUTPUT_PATH_DISCHARGE": "output",
    "ieasyhydroforecast_OUTPUT_PATH_REANALYSIS": "reanalysis",
}

# ---------------------------------------------------------------------------
# Test class: Per-code read behavior
# ---------------------------------------------------------------------------


class TestPerCodeReadBehavior:
    """Tests for per-code API reads in add_new_station.main()."""

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("add_new_station.SAPPHIRE_API_AVAILABLE", True)
    @patch("add_new_station._write_ml_forecast_to_api")
    @patch("add_new_station.call_hindcast_script")
    @patch("add_new_station._read_ml_forecasts_from_api")
    @patch("add_new_station.get_permitted_station_codes")
    @patch("add_new_station.utils_ml_forecast.get_hydroposts_for_pentadal_and_decadal_forecasts")
    @patch("add_new_station.sl")
    @patch("os.listdir", return_value=["some_file.nc"])
    @patch("os.path.exists", return_value=True)
    def test_per_code_reads_called_with_code_kwarg(
        self,
        mock_exists,
        mock_listdir,
        mock_sl,
        mock_get_hydroposts,
        mock_permitted,
        mock_read_api,
        mock_hindcast,
        mock_write_api,
    ):
        """When org config is available, each read uses code= kwarg and
        no start_date filter is applied. No hindcast should be triggered
        when all ML-capable codes are already in the forecast.
        """
        mock_sl.load_environment.return_value = None
        mock_permitted.return_value = {"19999"}

        # Only 19999 is ML-capable — no new codes expected
        hydroposts_df = _make_hydroposts_df(["19999"])
        mock_get_hydroposts.return_value = (["19999"], ["19999"], hydroposts_df)
        mock_read_api.return_value = _make_forecast_df(codes=["19999"])

        add_new_station.main()

        # 2 codes x 2 horizon types = at least 2 calls (1 code x 2 here)
        assert mock_read_api.call_count >= 2, (
            f"Expected at least 2 API read calls, got {mock_read_api.call_count}"
        )
        # Each call must use code= keyword arg and must NOT filter by start_date
        for c in mock_read_api.call_args_list:
            assert "code" in c.kwargs, f"Each call must specify code=, got kwargs={c.kwargs}"
            assert c.kwargs.get("start_date") is None, (
                f"Must not filter by date, got start_date={c.kwargs.get('start_date')}"
            )
        # No new codes -> no hindcast
        mock_hindcast.assert_not_called()

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("add_new_station.SAPPHIRE_API_AVAILABLE", True)
    @patch("add_new_station._write_ml_forecast_to_api")
    @patch("add_new_station.call_hindcast_script")
    @patch("add_new_station._read_ml_forecasts_from_api")
    @patch("add_new_station.get_permitted_station_codes")
    @patch("add_new_station.utils_ml_forecast.get_hydroposts_for_pentadal_and_decadal_forecasts")
    @patch("add_new_station.sl")
    @patch("os.listdir", return_value=["some_file.nc"])
    @patch("os.path.exists", return_value=True)
    def test_fallback_uses_extended_timeout(
        self,
        mock_exists,
        mock_listdir,
        mock_sl,
        mock_get_hydroposts,
        mock_permitted,
        mock_read_api,
        mock_hindcast,
        mock_write_api,
    ):
        """When org config is unavailable (permitted_codes=None), the fallback
        all-codes read must use timeout=120.
        """
        mock_sl.load_environment.return_value = None
        # Config unavailable
        mock_permitted.return_value = None

        hydroposts_df = _make_hydroposts_df(["19999"])
        mock_get_hydroposts.return_value = (["19999"], ["19999"], hydroposts_df)
        mock_read_api.return_value = _make_forecast_df(codes=["19999"])

        add_new_station.main()

        # All calls should use timeout=120 (fallback path)
        for c in mock_read_api.call_args_list:
            assert c.kwargs.get("timeout") == 120, (
                f"Fallback must use timeout=120, got timeout={c.kwargs.get('timeout')} in call {c}"
            )
        mock_hindcast.assert_not_called()

    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("add_new_station.SAPPHIRE_API_AVAILABLE", True)
    @patch("add_new_station._write_ml_forecast_to_api")
    @patch("add_new_station.call_hindcast_script")
    @patch("add_new_station._read_ml_forecasts_from_api")
    @patch("add_new_station.get_permitted_station_codes")
    @patch("add_new_station.utils_ml_forecast.get_hydroposts_for_pentadal_and_decadal_forecasts")
    @patch("add_new_station.sl")
    @patch("os.listdir", return_value=["some_file.nc"])
    @patch("os.path.exists", return_value=True)
    def test_stale_station_not_treated_as_new(
        self,
        mock_exists,
        mock_listdir,
        mock_sl,
        mock_get_hydroposts,
        mock_permitted,
        mock_read_api,
        mock_hindcast,
        mock_write_api,
    ):
        """A station with old (stale) forecast data must NOT trigger a hindcast.

        The stale-station guard must treat the station as "already covered"
        regardless of how old its forecast dates are.
        """
        mock_sl.load_environment.return_value = None
        mock_permitted.return_value = {"19999"}

        hydroposts_df = _make_hydroposts_df(["19999"])
        mock_get_hydroposts.return_value = (["19999"], ["19999"], hydroposts_df)

        # Return forecast data with old dates (2022) — station exists but is stale
        rows = []
        for i in range(5):
            rows.append(
                {
                    "code": "19999",
                    "forecast_date": pd.Timestamp("2022-01-01") + pd.Timedelta(days=i),
                    "date": pd.Timestamp("2022-01-06") + pd.Timedelta(days=i),
                    "flag": 0,
                    "Q5": 50.0,
                    "Q25": 80.0,
                    "Q50": 100.0,
                    "Q75": 120.0,
                    "Q95": 150.0,
                }
            )
        stale_forecast = pd.DataFrame(rows)
        mock_read_api.return_value = stale_forecast

        add_new_station.main()

        # Station already has forecasts (even stale ones) — must NOT trigger hindcast
        mock_hindcast.assert_not_called()

    @patch("pandas.DataFrame.to_csv")
    @patch.dict(os.environ, _BASE_ENV, clear=False)
    @patch("add_new_station.SAPPHIRE_API_AVAILABLE", True)
    @patch("add_new_station._write_ml_forecast_to_api")
    @patch("add_new_station.call_hindcast_script")
    @patch("add_new_station._read_ml_forecasts_from_api")
    @patch("add_new_station.get_permitted_station_codes")
    @patch("add_new_station.utils_ml_forecast.get_hydroposts_for_pentadal_and_decadal_forecasts")
    @patch("add_new_station.sl")
    @patch("os.listdir", return_value=["some_file.nc"])
    @patch("os.path.exists", return_value=True)
    def test_genuinely_new_station_triggers_hindcast(
        self,
        mock_exists,
        mock_listdir,
        mock_sl,
        mock_get_hydroposts,
        mock_permitted,
        mock_read_api,
        mock_hindcast,
        mock_write_api,
        mock_to_csv,
    ):
        """A station with no existing forecasts must trigger a hindcast call.

        Station "19999" has existing forecasts. Station "19998" has no
        forecasts at all. The hindcast must be called for "19998".
        """
        mock_sl.load_environment.return_value = None
        mock_permitted.return_value = {"19999", "19998"}

        # Both 19999 and 19998 are ML-capable
        hydroposts_df = _make_hydroposts_df(["19999", "19998"])
        mock_get_hydroposts.return_value = (
            ["19999", "19998"],
            ["19999", "19998"],
            hydroposts_df,
        )

        # 19999 has forecasts; 19998 has none
        def read_side_effect(**kwargs):
            if kwargs.get("code") == "19999":
                return _make_forecast_df(codes=["19999"])
            return pd.DataFrame()

        mock_read_api.side_effect = read_side_effect

        # Hindcast returns a valid DataFrame for 19998
        mock_hindcast.return_value = _make_forecast_df(codes=["19998"])
        mock_write_api.return_value = True

        add_new_station.main()

        # New station must trigger hindcast
        mock_hindcast.assert_called()

        # "19998" must appear somewhere in the hindcast call args
        hindcast_calls = mock_hindcast.call_args_list
        assert any("19998" in str(c) for c in hindcast_calls), (
            f"Expected '19998' in hindcast call args, got: {hindcast_calls}"
        )
