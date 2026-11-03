"""
Tests for the DataInterface class

Note: These tests require environment variables to be set (via .env file).
Without proper configuration, tests will be skipped with a warning.
Run with: ieasyhydroforecast_env_file_path="path/to/.env" python -m pytest tests/test_data_interface.py
"""

import json
import os
import warnings
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# Check if required environment is available before importing DataInterface
def _check_environment_available():
    """Check if required environment variables are set or can be loaded."""
    # If env file path is provided, we can try to load it
    # If organization is already set, environment is available
    return bool(
        os.getenv("ieasyhydroforecast_env_file_path")
        or os.getenv("ieasyhydroforecast_organization")
    )


# Determine if tests should be skipped
ENV_AVAILABLE = _check_environment_available()
SKIP_REASON = (
    "Environment not configured. Set ieasyhydroforecast_env_file_path or "
    "ieasyhydroforecast_organization to run these tests."
)

if not ENV_AVAILABLE:
    warnings.warn(f"DataInterface tests skipped: {SKIP_REASON}", UserWarning, stacklevel=2)


# Conditionally import DataInterface to avoid initialization errors
if ENV_AVAILABLE:
    from data_interface import DataInterface

# Always import the DB/scoping classes — they are tested with mocks
# and do not require a live database or real .env file.
from data_interface import (
    BasePredictorDataInterface,
    DataInterfaceDB,
)


@pytest.mark.skipif(not ENV_AVAILABLE, reason=SKIP_REASON)
class TestDataInterface:
    """Test suite for DataInterface class"""

    @pytest.fixture
    def data_interface(self):
        """Create a DataInterface instance for testing"""
        return DataInterface()

    def test_load_base_data(self, data_interface):
        """Test 1: Load base data without snow"""
        # Get base data without snow variables
        result = data_interface.get_base_data(forcing_HRU="00003")

        # Assertions
        assert "temporal_data" in result
        assert "static_data" in result
        assert "offset_date_base" in result

        # Check that temporal_data is a DataFrame
        assert isinstance(result["temporal_data"], pd.DataFrame)

        # Check that static_data is a DataFrame
        assert isinstance(result["static_data"], pd.DataFrame)

        # Check that temporal data has required columns
        assert "date" in result["temporal_data"].columns
        assert "code" in result["temporal_data"].columns
        assert "P" in result["temporal_data"].columns
        assert "T" in result["temporal_data"].columns

        print("✓ Base data loaded successfully")
        print(f"  Temporal data shape: {result['temporal_data'].shape}")
        print(f"  Static data shape: {result['static_data'].shape}")
        print(f"  Offset base: {result['offset_date_base']} days")

    def test_load_snow_data(self, data_interface):
        """Test 2: Load snow data with valid HRU combination"""
        # Load snow data directly
        snow_df, max_date = data_interface.load_snow_data(HRU="00003", variable="SWE")

        # Assertions
        assert isinstance(snow_df, pd.DataFrame)
        assert isinstance(max_date, pd.Timestamp)

        # Check that snow data has required columns
        assert "date" in snow_df.columns
        assert "code" in snow_df.columns
        assert "SWE" in snow_df.columns

        # Check data types
        assert pd.api.types.is_datetime64_any_dtype(snow_df["date"])
        assert pd.api.types.is_integer_dtype(snow_df["code"])

        print("✓ Snow data loaded successfully")
        print(f"  Snow data shape: {snow_df.shape}")
        print(f"  Max date: {max_date}")

        # Test extending base data with snow
        base_result = data_interface.get_base_data(forcing_HRU="00003")
        extended_result = data_interface.extend_base_data_with_snow(
            base_data=base_result["temporal_data"], HRUs_snow=["00003"], snow_variables=["SWE"]
        )

        # Check that snow variable is in temporal data
        assert "SWE" in extended_result["temporal_data"].columns
        assert extended_result["offset_date_snow"] is not None

        print("✓ Base data extended with snow successfully")
        print(f"  Offset snow: {extended_result['offset_date_snow']} days")

    def test_load_snow_data_wrong_hru_combination(self, data_interface):
        """Test 3: Load snow data with wrong HRU combination (should raise error)"""
        # Try to load snow data with invalid HRU
        with pytest.raises(AssertionError, match="HRU .* not in available snow HRUs"):
            data_interface.load_snow_data(
                HRU="99999",  # Invalid HRU
                variable="SWE",
            )

        print("✓ Correctly raised error for invalid HRU")

        # Try to load snow data with invalid variable
        with pytest.raises(AssertionError, match="Variable .* not in available snow variables"):
            data_interface.load_snow_data(
                HRU="00003",
                variable="INVALID_VAR",  # Invalid variable
            )

        print("✓ Correctly raised error for invalid variable")


class TestConfiguredAPI:
    @pytest.fixture
    def interfaces(self, monkeypatch):
        from data_interface import DataInterfaceDB, BasePredictorDataInterface
        monkeypatch.setenv("SAPPHIRE_API_URL", "http://localhost:8100")
        # Deliberately conflicting direct-DB settings must never be used.
        monkeypatch.setenv("PREPROCESSING_DB_PORT", "5433")
        monkeypatch.setenv("POSTPROCESSING_DB_PORT", "5434")
        with patch("data_interface.sl.load_environment"), patch.object(DataInterfaceDB, "_get_paths"), \
             patch("sapphire_api_client.SapphirePreprocessingClient") as pre, \
             patch("sapphire_api_client.SapphirePostprocessingClient") as post:
            db = DataInterfaceDB(station_codes=["12345"])
            bp = BasePredictorDataInterface(station_codes=["12345"])
            yield db, bp, pre, post

    @pytest.mark.parametrize("method,kwargs,api_method,column", [
        ("get_runoff_data", {}, "read_runoff", "discharge"),
        ("get_meteo_data", {"meteo_type": "P"}, "read_meteo", "P"),
        ("get_snow_data", {"variable": "RoF"}, "read_snow", "ROF"),
    ])
    def test_inputs_use_configured_api(self, interfaces, method, kwargs, api_method, column):
        db, _, pre, _ = interfaces
        row = {"date": "2026-10-30", "code": "12345", "value": 2, "discharge": 3,
               **{f"value{i}": i for i in range(1,15)}}
        read = getattr(pre.return_value, api_method)
        # Short pages must not truncate history when the server caps page size.
        read.side_effect = [pd.DataFrame([row]), pd.DataFrame([{**row, "date": "2026-10-31"}]), pd.DataFrame()]
        result = getattr(db, method)(start_date="2026-10-01", end_date="2026-10-31", **kwargs)
        pre.assert_called_once_with(base_url="http://localhost:8100")
        assert len(result) == 2
        assert result["code"].tolist() == [12345, 12345]
        assert column in result
        assert [c.kwargs["skip"] for c in read.call_args_list] == [0, 1, 2]
        for call in read.call_args_list:
            assert call.kwargs["code"] == "12345"
            assert call.kwargs["start_date"] == "2026-10-01"
            assert call.kwargs["end_date"] == "2026-10-31"
        if column == "discharge":
            assert read.call_args.kwargs["horizon"] == "day"
        if column == "ROF":
            assert read.call_args.kwargs["snow_type"] == "ROF"
            assert result["ROF_14"].tolist() == [14, 14]

    def test_predictions_use_same_api_and_filters(self, interfaces):
        _, bp, _, post = interfaces
        post.return_value.read_long_term_forecasts.side_effect = [pd.DataFrame([
            {"date": "2020-01-25", "code": "12345", "q": 4, "q_xgb": 5}
        ]), pd.DataFrame()]
        frame, cols = bp.get_base_predictor_data_database("GBT", "quarter", 2)
        post.assert_called_once_with(base_url="http://localhost:8100")
        call = post.return_value.read_long_term_forecasts.call_args_list[0]
        assert call.kwargs["model"] == "GBT"
        assert call.kwargs["code"] == "12345"
        assert call.kwargs["horizon_type"] == "quarter"
        assert call.kwargs["horizon_value"] == 2
        assert call.kwargs["end_date"]
        assert cols == ["GBT", "GBT_xgb"]
        assert frame["GBT"].tolist() == [4]

    def test_api_failure_propagates(self, interfaces):
        db, _, pre, _ = interfaces
        pre.return_value.read_snow.side_effect = RuntimeError("API unavailable")
        with pytest.raises(RuntimeError, match="API unavailable"):
            db.get_snow_data("SWE")

    @pytest.mark.parametrize("codes", [None, [], ["12345", "67890"]])
    def test_station_selection(self, interfaces, codes):
        db, _, pre, _ = interfaces
        db.station_codes = codes
        pre.return_value.read_meteo.return_value = pd.DataFrame()
        result = db.get_meteo_data("T")
        assert result.empty and list(result.columns) == ["date", "code", "T"]
        assert [c.kwargs["code"] for c in pre.return_value.read_meteo.call_args_list] == (codes or [None])

    def test_explicit_station_cannot_escape_selection(self, interfaces):
        db, _, pre, _ = interfaces
        assert db.get_runoff_data(code="99999").empty
        pre.return_value.read_runoff.assert_not_called()

    def test_static_station_filter_preserved(self, interfaces, tmp_path):
        db, _, _, _ = interfaces
        path = tmp_path / "static.csv"
        path.write_text("code,area\n12345,1\n99999,2\n")
        db.PATH_TO_STATIC_FEATURES = str(path)
        assert db._prepare_static_data()["code"].tolist() == [12345]


def _import_read_station_codes():
    """Import _read_station_codes from run_forecast, mocking heavy deps.

    run_forecast imports lt_utils which requires lt_forecasting (an optional
    standalone library not always installed in the test environment).  We stub
    every leaf module in the import chain so the function-under-test can be
    loaded without a full model install.

    All sys.modules mutations are done inside a single patch.dict so they are
    fully rolled back after the import block, preventing test-order pollution.
    """
    import sys
    import types

    # Build stub modules for every missing piece of lt_forecasting.
    leaf_stubs = {
        "lt_forecasting": {},
        "lt_forecasting.forecast_models": {},
        "lt_forecasting.forecast_models.deep_models": {},
        "lt_forecasting.forecast_models.deep_models.uncertainty_mixture": {
            "UncertaintyMixtureModel": MagicMock(),
        },
        "lt_forecasting.forecast_models.LINEAR_REGRESSION": {
            "LinearRegressionModel": MagicMock(),
        },
        "lt_forecasting.forecast_models.SciRegressor": {
            "SciRegressor": MagicMock(),
        },
    }
    stubs = {}
    for name, attrs in leaf_stubs.items():
        mod = types.ModuleType(name)
        for attr, val in attrs.items():
            setattr(mod, attr, val)
        stubs[name] = mod

    # Capture the real lt_utils module (if already imported) so patch.dict
    # can restore it on exit.  Set to a fresh copy inside the block so the
    # import machinery will reimport against our stubs.
    real_lt_utils = sys.modules.get("lt_utils")
    real_run_forecast = sys.modules.get("run_forecast")

    # Temporarily mark both as absent so Python reimports them with stubs.
    stubs_with_evictions = dict(stubs)
    # patch.dict keeps keys that exist in the original dict; we need them
    # absent, so we remove them before entering and let patch.dict restore
    # their original values on exit.
    for key in ("lt_utils", "run_forecast"):
        sys.modules.pop(key, None)

    try:
        with patch.dict(sys.modules, stubs_with_evictions):
            import run_forecast as rf

            fn = rf._read_station_codes
    finally:
        # Restore the previously-loaded real modules so later tests are
        # unaffected (patch.dict restores the stubs, but lt_utils/run_forecast
        # were already absent before the with-block so they stay absent after
        # unless we put them back explicitly).
        if real_lt_utils is not None:
            sys.modules["lt_utils"] = real_lt_utils
        else:
            sys.modules.pop("lt_utils", None)
        if real_run_forecast is not None:
            sys.modules["run_forecast"] = real_run_forecast
        else:
            sys.modules.pop("run_forecast", None)

    return fn


class TestReadStationCodes:
    """Test _read_station_codes() in run_forecast.py."""

    def test_read_station_codes_list_format(self, tmp_path, monkeypatch):
        """stationsID as list of ints returns list[str]."""
        config = {"stationsID": [12345, 67890]}
        config_file = tmp_path / "config_station_selection.json"
        config_file.write_text(json.dumps(config))
        monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
        monkeypatch.setenv(
            "ieasyforecast_config_file_station_selection",
            "config_station_selection.json",
        )
        _read_station_codes = _import_read_station_codes()
        codes = _read_station_codes()
        assert codes == ["12345", "67890"]

    def test_read_station_codes_empty(self, tmp_path, monkeypatch):
        """Empty stationsID returns empty list."""
        config = {"stationsID": []}
        config_file = tmp_path / "config_station_selection.json"
        config_file.write_text(json.dumps(config))
        monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
        monkeypatch.setenv(
            "ieasyforecast_config_file_station_selection",
            "config_station_selection.json",
        )
        _read_station_codes = _import_read_station_codes()
        codes = _read_station_codes()
        assert codes == []
