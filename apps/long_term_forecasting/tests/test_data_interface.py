"""
Tests for the DataInterface class
"""
import pytest
import pandas as pd
import os
import sys

# Add parent directory to path to import data_interface
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from data_interface import DataInterface


class TestDataInterface:
    """Test suite for DataInterface class"""

    @pytest.fixture
    def data_interface(self):
        """Create a DataInterface instance for testing"""
        return DataInterface()

    def test_load_base_data(self, data_interface):
        """Test 1: Load base data without snow"""
        # Get base data without snow variables
        result = data_interface.get_base_data(
            forcing_HRU="00003"
        )
        
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
        
        print(f"✓ Base data loaded successfully")
        print(f"  Temporal data shape: {result['temporal_data'].shape}")
        print(f"  Static data shape: {result['static_data'].shape}")
        print(f"  Offset base: {result['offset_date_base']} days")

    def test_load_snow_data(self, data_interface):
        """Test 2: Load snow data with valid HRU combination"""
        # Load snow data directly
        snow_df, max_date = data_interface.load_snow_data(
            HRU="00003",
            variable="SWE"
        )
        
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
        
        print(f"✓ Snow data loaded successfully")
        print(f"  Snow data shape: {snow_df.shape}")
        print(f"  Max date: {max_date}")
        
        # Test extending base data with snow
        base_result = data_interface.get_base_data(forcing_HRU="00003")
        extended_result = data_interface.extend_base_data_with_snow(
            base_data=base_result["temporal_data"],
            HRUs_snow=["00003"],
            snow_variables=["SWE"]
        )
        
        # Check that snow variable is in temporal data
        assert "SWE" in extended_result["temporal_data"].columns
        assert extended_result["offset_date_snow"] is not None
        
        print(f"✓ Base data extended with snow successfully")
        print(f"  Offset snow: {extended_result['offset_date_snow']} days")

    def test_load_snow_data_wrong_hru_combination(self, data_interface):
        """Test 3: Load snow data with wrong HRU combination (should raise error)"""
        # Try to load snow data with invalid HRU
        with pytest.raises(AssertionError, match="HRU .* not in available snow HRUs"):
            data_interface.load_snow_data(
                HRU="99999",  # Invalid HRU
                variable="SWE"
            )
        
        print(f"✓ Correctly raised error for invalid HRU")
        
        # Try to load snow data with invalid variable
        with pytest.raises(AssertionError, match="Variable .* not in available snow variables"):
            data_interface.load_snow_data(
                HRU="00003",
                variable="INVALID_VAR"  # Invalid variable
            )
        
        print(f"✓ Correctly raised error for invalid variable")


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