"""
Tests for the DataInterface class

Note: These tests require environment variables to be set (via .env file).
Without proper configuration, tests will be skipped with a warning.
Run with: ieasyhydroforecast_env_file_path="path/to/.env" python -m pytest tests/test_data_interface.py
"""

import contextlib
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
    _build_postprocessing_db_url,
    _build_preprocessing_db_url,
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


class TestDataInterfaceDBOrgScoping:
    """Test org-scoping SQL filters in DataInterfaceDB."""

    @pytest.fixture
    def patched_db(self, monkeypatch):
        """Create DataInterfaceDB with mocked engine and env."""
        monkeypatch.setenv("ieasyhydroforecast_env_file_path", "/dev/null")
        monkeypatch.setenv("ieasyforecast_configuration_path", "/dev/null")

        with patch("data_interface.sl.load_environment"), patch("data_interface.create_engine"):
            db = DataInterfaceDB.__new__(DataInterfaceDB)
            db.station_codes = ["12345", "67890"]
            db.engine = MagicMock()
            db.PATH_TO_STATIC_FEATURES = "/dev/null"
            db.connection_string = "sqlite://"
        return db

    def test_station_filter_adds_in_clause(self, patched_db):
        """_add_station_filter adds IN clause with correct params."""
        conditions = []
        params = {}
        patched_db._add_station_filter(conditions, params)
        assert len(conditions) == 1
        assert "code IN (:sc_0, :sc_1)" in conditions[0]
        assert params["sc_0"] == "12345"
        assert params["sc_1"] == "67890"

    def test_no_station_codes_no_filter(self, patched_db):
        """No IN clause when station_codes is None."""
        patched_db.station_codes = None
        conditions = []
        params = {}
        patched_db._add_station_filter(conditions, params)
        assert len(conditions) == 0
        assert len(params) == 0

    def test_empty_station_codes_no_filter(self, patched_db):
        """Empty list behaves like None — no IN clause."""
        patched_db.station_codes = []
        conditions = []
        params = {}
        patched_db._add_station_filter(conditions, params)
        assert len(conditions) == 0
        assert len(params) == 0

    def test_single_code_and_station_codes_both_apply(self, patched_db):
        """Both single code param and station_codes produce AND conditions."""
        conditions = ["code = :code"]
        params = {"code": "12345"}
        patched_db._add_station_filter(conditions, params)
        assert len(conditions) == 2
        assert "code = :code" in conditions[0]
        assert "code IN" in conditions[1]

    def test_static_data_filtered_by_station_codes(self, patched_db, tmp_path):
        """_prepare_static_data filters CSV to org stations only."""
        csv_file = tmp_path / "static_features.csv"
        csv_file.write_text("code,feature1\n12345,1.0\n67890,2.0\n99999,3.0\n")
        patched_db.PATH_TO_STATIC_FEATURES = str(csv_file)
        result = patched_db._prepare_static_data()
        assert set(result["code"].astype(str)) == {"12345", "67890"}
        assert len(result) == 2

    def test_static_data_unfiltered_when_no_station_codes(self, patched_db, tmp_path):
        """All codes returned when station_codes is None."""
        csv_file = tmp_path / "static_features.csv"
        csv_file.write_text("code,feature1\n12345,1.0\n67890,2.0\n99999,3.0\n")
        patched_db.PATH_TO_STATIC_FEATURES = str(csv_file)
        patched_db.station_codes = None
        result = patched_db._prepare_static_data()
        assert len(result) == 3

    @pytest.mark.parametrize(
        "method_name,extra_kwargs",
        [
            ("get_runoff_data", {}),
            ("get_meteo_data", {"meteo_type": "P"}),
            ("get_snow_data", {"variable": "SWE"}),
        ],
    )
    def test_station_filter_applied_to_all_query_methods(
        self, patched_db, method_name, extra_kwargs
    ):
        """IN clause appears in SQL for all query methods."""
        captured_queries = []

        def mock_execute(query, params=None):
            captured_queries.append(str(query))
            return pd.DataFrame(columns=["date", "code", "value"])

        patched_db._execute_query = mock_execute
        with contextlib.suppress(Exception):
            getattr(patched_db, method_name)(**extra_kwargs)
        assert len(captured_queries) >= 1
        assert any("code IN" in q for q in captured_queries), (
            f"No IN clause found in {method_name} queries: {captured_queries}"
        )

    def test_get_base_data_propagates_station_filter(self, patched_db, tmp_path):
        """All SQL from get_base_data() chain contains IN clause."""
        csv_file = tmp_path / "static_features.csv"
        csv_file.write_text("code,feature1\n12345,1.0\n67890,2.0\n")
        patched_db.PATH_TO_STATIC_FEATURES = str(csv_file)

        captured_queries = []

        def mock_execute(query, params=None):
            captured_queries.append(str(query))
            # Return minimal valid DataFrame
            return pd.DataFrame(
                {
                    "date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                    "code": [12345, 67890],
                    "discharge": [1.0, 2.0],
                    "P": [0.5, 0.6],
                    "T": [5.0, 6.0],
                }
            )

        patched_db._execute_query = mock_execute
        with contextlib.suppress(Exception):
            patched_db.get_base_data(forcing_HRU="00003")
        assert len(captured_queries) >= 1
        for q in captured_queries:
            assert "code IN" in q, f"Missing IN clause in query: {q[:100]}"


class TestBasePredictorDataInterfaceOrgScoping:
    """Test org-scoping filters in BasePredictorDataInterface."""

    @pytest.fixture
    def patched_bpdi(self, monkeypatch):
        """Create BasePredictorDataInterface with mocked env."""
        monkeypatch.setenv("ieasyhydroforecast_env_file_path", "/dev/null")

        with patch("data_interface.sl.load_environment"):
            bpdi = BasePredictorDataInterface.__new__(BasePredictorDataInterface)
            bpdi.station_codes = ["12345", "67890"]
            bpdi._postprocessing_engine = MagicMock()
            bpdi.postprocessing_connection_string = "sqlite://"
        return bpdi

    def test_base_predictor_station_filter(self, patched_bpdi):
        """code IN clause present when station_codes set."""
        captured = []

        def mock_exec(query, params=None):
            captured.append(str(query))
            return pd.DataFrame(
                columns=["date", "code", "q", "q_xgb", "q_lgbm", "q_catboost", "q_loc"]
            )

        patched_bpdi._execute_postprocessing_query = mock_exec
        with contextlib.suppress(Exception):
            patched_bpdi.get_base_predictor_data_database("test_model")
        assert len(captured) >= 1
        assert "code IN" in captured[0]

    def test_base_predictor_no_filter_when_none(self, patched_bpdi):
        """No IN clause when station_codes is None."""
        patched_bpdi.station_codes = None
        captured = []

        def mock_exec(query, params=None):
            captured.append(str(query))
            return pd.DataFrame(
                columns=["date", "code", "q", "q_xgb", "q_lgbm", "q_catboost", "q_loc"]
            )

        patched_bpdi._execute_postprocessing_query = mock_exec
        with contextlib.suppress(Exception):
            patched_bpdi.get_base_predictor_data_database("test_model")
        assert len(captured) >= 1
        assert "code IN" not in captured[0]

    def test_base_predictor_empty_list_no_filter(self, patched_bpdi):
        """Empty list behaves like None."""
        patched_bpdi.station_codes = []
        captured = []

        def mock_exec(query, params=None):
            captured.append(str(query))
            return pd.DataFrame(
                columns=["date", "code", "q", "q_xgb", "q_lgbm", "q_catboost", "q_loc"]
            )

        patched_bpdi._execute_postprocessing_query = mock_exec
        with contextlib.suppress(Exception):
            patched_bpdi.get_base_predictor_data_database("test_model")
        assert len(captured) >= 1
        assert "code IN" not in captured[0]

    def test_base_predictor_order_by_preserved(self, patched_bpdi):
        """ORDER BY comes after WHERE clause (regression test)."""
        captured = []

        def mock_exec(query, params=None):
            captured.append(str(query))
            return pd.DataFrame(
                columns=["date", "code", "q", "q_xgb", "q_lgbm", "q_catboost", "q_loc"]
            )

        patched_bpdi._execute_postprocessing_query = mock_exec
        with contextlib.suppress(Exception):
            patched_bpdi.get_base_predictor_data_database("test_model")
        assert len(captured) >= 1
        query = captured[0]
        where_pos = query.find("WHERE")
        order_pos = query.find("ORDER BY")
        assert where_pos < order_pos, (
            f"ORDER BY ({order_pos}) should come after WHERE ({where_pos})"
        )


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


class TestBuildPostprocessingDbUrl:
    """Tests for _build_postprocessing_db_url helper."""

    def test_outside_docker_default_port(self, monkeypatch):
        """Outside Docker: uses localhost:5434."""
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("POSTPROCESSING_DB", "test_db")
        monkeypatch.delenv("IN_DOCKER", raising=False)
        monkeypatch.delenv("POSTPROCESSING_DB_PORT", raising=False)

        url = _build_postprocessing_db_url()

        assert url == "postgresql://testuser:testpass@localhost:5434/test_db"

    def test_inside_docker(self, monkeypatch):
        """Inside Docker: uses postprocessing-db:5432."""
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("POSTPROCESSING_DB", "test_db")
        monkeypatch.setenv("IN_DOCKER", "True")

        url = _build_postprocessing_db_url()

        assert url == "postgresql://testuser:testpass@postprocessing-db:5432/test_db"

    def test_custom_port(self, monkeypatch):
        """POSTPROCESSING_DB_PORT overrides default 5434."""
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("POSTPROCESSING_DB", "test_db")
        monkeypatch.setenv("POSTPROCESSING_DB_PORT", "5555")
        monkeypatch.delenv("IN_DOCKER", raising=False)

        url = _build_postprocessing_db_url()

        assert url == "postgresql://testuser:testpass@localhost:5555/test_db"

    def test_custom_port_ignored_in_docker(self, monkeypatch):
        """Inside Docker, port is always 5432 regardless of POSTPROCESSING_DB_PORT."""
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("POSTPROCESSING_DB", "test_db")
        monkeypatch.setenv("IN_DOCKER", "True")
        monkeypatch.setenv("POSTPROCESSING_DB_PORT", "5555")

        url = _build_postprocessing_db_url()

        assert url == "postgresql://testuser:testpass@postprocessing-db:5432/test_db"

    def test_special_chars_in_password(self, monkeypatch):
        """Passwords with special chars are URL-encoded."""
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "p@ss/word")
        monkeypatch.setenv("POSTPROCESSING_DB", "test_db")
        monkeypatch.delenv("IN_DOCKER", raising=False)
        monkeypatch.delenv("POSTPROCESSING_DB_PORT", raising=False)

        url = _build_postprocessing_db_url()

        assert "p%40ss%2Fword" in url

    def test_missing_user_raises(self, monkeypatch):
        """Missing POSTGRES_USER raises ValueError."""
        monkeypatch.delenv("POSTGRES_USER", raising=False)
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("POSTPROCESSING_DB", "test_db")

        with pytest.raises(ValueError, match="POSTGRES_USER"):
            _build_postprocessing_db_url()

    def test_missing_password_raises(self, monkeypatch):
        """Missing POSTGRES_PASSWORD raises ValueError."""
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
        monkeypatch.setenv("POSTPROCESSING_DB", "test_db")

        with pytest.raises(ValueError, match="POSTGRES_PASSWORD"):
            _build_postprocessing_db_url()

    def test_missing_db_name_raises(self, monkeypatch):
        """Missing POSTPROCESSING_DB raises ValueError."""
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.delenv("POSTPROCESSING_DB", raising=False)

        with pytest.raises(ValueError, match="POSTPROCESSING_DB"):
            _build_postprocessing_db_url()

    def test_multiple_missing_lists_all(self, monkeypatch):
        """All components missing: error lists all three."""
        monkeypatch.delenv("POSTGRES_USER", raising=False)
        monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)
        monkeypatch.delenv("POSTPROCESSING_DB", raising=False)

        with pytest.raises(ValueError, match="POSTGRES_USER.*POSTGRES_PASSWORD.*POSTPROCESSING_DB"):
            _build_postprocessing_db_url()


class TestBuildPreprocessingDbUrl:
    """Tests for _build_preprocessing_db_url helper."""

    def test_outside_docker_default_port(self, monkeypatch):
        """Outside Docker: uses localhost:5433."""
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("PREPROCESSING_DB", "test_db")
        monkeypatch.delenv("IN_DOCKER", raising=False)
        monkeypatch.delenv("PREPROCESSING_DB_PORT", raising=False)

        url = _build_preprocessing_db_url()

        assert url == "postgresql://testuser:testpass@localhost:5433/test_db"

    def test_inside_docker(self, monkeypatch):
        """Inside Docker: uses preprocessing-db:5432."""
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.setenv("PREPROCESSING_DB", "test_db")
        monkeypatch.setenv("IN_DOCKER", "True")

        url = _build_preprocessing_db_url()

        assert url == "postgresql://testuser:testpass@preprocessing-db:5432/test_db"

    def test_missing_db_name_raises(self, monkeypatch):
        """Missing PREPROCESSING_DB raises ValueError."""
        monkeypatch.setenv("POSTGRES_USER", "testuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
        monkeypatch.delenv("PREPROCESSING_DB", raising=False)

        with pytest.raises(ValueError, match="PREPROCESSING_DB"):
            _build_preprocessing_db_url()


# Run with "ieasyhydroforecast_env_file_path="../../../kyg_data_forecast_tools/config/.env_develop_kghm" python -m tests.test_data_interface"
if __name__ == "__main__":
    test_suite = TestDataInterface()
    # Create DataInterface instance directly (not through fixture)
    di = DataInterface()

    print("\n=== Running Test 1: Load Base Data ===")
    test_suite.test_load_base_data(di)

    print("\n=== Running Test 2: Load Snow Data ===")
    test_suite.test_load_snow_data(di)

    print("\n=== Running Test 3: Wrong HRU Combination ===")
    test_suite.test_load_snow_data_wrong_hru_combination(di)

    print("\n=== All tests completed successfully! ===")
