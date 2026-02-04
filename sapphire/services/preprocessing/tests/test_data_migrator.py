"""
Tests for data_migrator.py

These tests verify the transformation logic of data migrators to ensure
the status quo is preserved when integrating the new sapphire-api-client.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile
import os

# Add app to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "app"))

from data_migrator import (
    MigrationStats,
    RunoffDataMigrator,
    HydrographDataMigrator,
    MeteoDataMigrator,
    SnowDataMigrator,
)


class TestMigrationStats:
    """Tests for MigrationStats dataclass."""

    def test_str_representation(self):
        """Test string representation of stats."""
        stats = MigrationStats(
            data_type="runoff",
            horizon_type="day",
            total_records=1000,
            successful=950,
            failed=50,
            duration_seconds=10.5,
            records_per_second=90.48,
            batch_count=10,
        )
        result = str(stats)
        assert "RUNOFF" in result
        assert "DAY" in result
        assert "1,000" in result
        assert "950" in result
        assert "95.00%" in result


class TestRunoffDataMigrator:
    """Tests for RunoffDataMigrator transformation methods."""

    def setup_method(self):
        """Set up test migrator."""
        self.migrator = RunoffDataMigrator(api_base_url="http://test:8000")

    def test_get_data_type(self):
        """Test data type is 'runoff'."""
        assert self.migrator.get_data_type() == "runoff"

    def test_prepare_day_data_basic(self):
        """Test basic daily runoff data transformation."""
        df = pd.DataFrame({
            "date": ["2024-01-15", "2024-01-16"],
            "code": ["12345", "12345"],
            "discharge": [100.5, 150.3],
        })

        records = self.migrator.prepare_day_data(df)

        assert len(records) == 2
        assert records[0]["horizon_type"] == "day"
        assert records[0]["code"] == "12345"
        assert records[0]["date"] == "2024-01-15"
        assert records[0]["discharge"] == 100.5
        assert records[0]["predictor"] is None  # Daily data has no predictor
        assert records[0]["horizon_value"] == 15  # Day of month
        assert records[0]["horizon_in_year"] == 15  # Day of year

    def test_prepare_day_data_null_discharge(self):
        """Test that null discharge is handled correctly."""
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "code": ["12345"],
            "discharge": [None],
        })

        records = self.migrator.prepare_day_data(df)

        assert records[0]["discharge"] is None

    def test_prepare_day_data_nan_discharge(self):
        """Test that NaN discharge is converted to None."""
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "code": ["12345"],
            "discharge": [float("nan")],
        })

        records = self.migrator.prepare_day_data(df)

        assert records[0]["discharge"] is None

    def test_prepare_day_data_code_as_string(self):
        """Test that code is always converted to string."""
        df = pd.DataFrame({
            "date": ["2024-01-01"],
            "code": [12345],  # Integer code
            "discharge": [100.0],
        })

        records = self.migrator.prepare_day_data(df)

        assert records[0]["code"] == "12345"
        assert isinstance(records[0]["code"], str)

    def test_prepare_pentad_data_basic(self):
        """Test basic pentad runoff data transformation."""
        df = pd.DataFrame({
            "date": ["2024-01-05"],
            "code": ["12345"],
            "discharge_avg": [120.5],
            "predictor": [80.0],
            "pentad": [1],
            "pentad_in_year": [1],
        })

        records = self.migrator.prepare_pentad_data(df)

        assert len(records) == 1
        assert records[0]["horizon_type"] == "pentad"
        assert records[0]["code"] == "12345"
        assert records[0]["discharge"] == 120.5
        assert records[0]["predictor"] == 80.0
        assert records[0]["horizon_value"] == 1
        assert records[0]["horizon_in_year"] == 1

    def test_prepare_pentad_data_null_values(self):
        """Test pentad data with null values."""
        df = pd.DataFrame({
            "date": ["2024-01-05"],
            "code": ["12345"],
            "discharge_avg": [None],
            "predictor": [float("nan")],
            "pentad": [1],
            "pentad_in_year": [1],
        })

        records = self.migrator.prepare_pentad_data(df)

        assert records[0]["discharge"] is None
        assert records[0]["predictor"] is None

    def test_prepare_decade_data_basic(self):
        """Test basic decade runoff data transformation."""
        df = pd.DataFrame({
            "date": ["2024-01-10"],
            "code": ["12345"],
            "discharge_avg": [130.5],
            "predictor": [90.0],
            "decad_in_year": [1],
        })

        records = self.migrator.prepare_decade_data(df)

        assert len(records) == 1
        assert records[0]["horizon_type"] == "decade"
        assert records[0]["code"] == "12345"
        assert records[0]["discharge"] == 130.5
        assert records[0]["predictor"] == 90.0
        assert records[0]["horizon_value"] == 1
        assert records[0]["horizon_in_year"] == 1


class TestHydrographDataMigrator:
    """Tests for HydrographDataMigrator transformation methods."""

    def setup_method(self):
        """Set up test migrator."""
        self.migrator = HydrographDataMigrator(api_base_url="http://test:8000")

    def test_get_data_type(self):
        """Test data type is 'hydrograph'."""
        assert self.migrator.get_data_type() == "hydrograph"

    def test_prepare_day_data_basic(self):
        """Test basic daily hydrograph data transformation."""
        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "day_of_year": [15],
            "count": [30],
            "mean": [100.0],
            "std": [10.0],
            "min": [80.0],
            "max": [120.0],
            "5%": [82.0],
            "25%": [90.0],
            "50%": [100.0],
            "75%": [110.0],
            "95%": [118.0],
            "2024": [95.0],
            "2025": [105.0],
        })

        records = self.migrator.prepare_day_data(df)

        assert len(records) == 1
        assert records[0]["horizon_type"] == "day"
        assert records[0]["code"] == "12345"
        assert records[0]["day_of_year"] == 15
        assert records[0]["horizon_in_year"] == 15
        assert records[0]["horizon_value"] == 15  # Day of month
        assert records[0]["count"] == 30
        assert records[0]["mean"] == 100.0
        assert records[0]["std"] == 10.0
        assert records[0]["min"] == 80.0
        assert records[0]["max"] == 120.0
        assert records[0]["q05"] == 82.0
        assert records[0]["q25"] == 90.0
        assert records[0]["q50"] == 100.0
        assert records[0]["q75"] == 110.0
        assert records[0]["q95"] == 118.0
        assert records[0]["norm"] is None
        assert records[0]["previous"] == 95.0
        assert records[0]["current"] == 105.0

    def test_prepare_day_data_null_values(self):
        """Test daily hydrograph with null statistical values."""
        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "day_of_year": [15],
            "count": [None],
            "mean": [float("nan")],
            "std": [None],
            "min": [None],
            "max": [None],
            "5%": [None],
            "25%": [None],
            "50%": [None],
            "75%": [None],
            "95%": [None],
        })

        records = self.migrator.prepare_day_data(df)

        assert records[0]["count"] is None
        assert records[0]["mean"] is None
        assert records[0]["std"] is None
        assert records[0]["q05"] is None

    def test_prepare_pentad_data_basic(self):
        """Test basic pentad hydrograph data transformation."""
        df = pd.DataFrame({
            "date": ["2024-01-05"],
            "code": ["12345"],
            "pentad": [1],
            "pentad_in_year": [1],
            "day_of_year": [5],
            "mean": [100.0],
            "min": [80.0],
            "max": [120.0],
            "q05": [82.0],
            "q25": [90.0],
            "q75": [110.0],
            "q95": [118.0],
            "norm": [95.0],
            "2024": [92.0],
            "2025": [102.0],
        })

        records = self.migrator.prepare_pentad_data(df)

        assert len(records) == 1
        assert records[0]["horizon_type"] == "pentad"
        assert records[0]["horizon_value"] == 1
        assert records[0]["horizon_in_year"] == 1
        assert records[0]["day_of_year"] == 5
        assert records[0]["mean"] == 100.0
        assert records[0]["norm"] == 95.0
        assert records[0]["count"] is None  # Not in pentad data
        assert records[0]["std"] is None  # Not in pentad data
        assert records[0]["q50"] is None  # Not in pentad data

    def test_prepare_decade_data_basic(self):
        """Test basic decade hydrograph data transformation."""
        df = pd.DataFrame({
            "date": ["2024-01-10"],
            "code": ["12345"],
            "decad_in_month": [1],
            "decad_in_year": [1],
            "day_of_year": [10],
            "mean": [100.0],
            "min": [80.0],
            "max": [120.0],
            "q05": [82.0],
            "q25": [90.0],
            "q75": [110.0],
            "q95": [118.0],
            "norm": [95.0],
            "2024": [92.0],
            "2025": [102.0],
        })

        records = self.migrator.prepare_decade_data(df)

        assert len(records) == 1
        assert records[0]["horizon_type"] == "decade"
        assert records[0]["horizon_value"] == 1
        assert records[0]["horizon_in_year"] == 1
        assert records[0]["day_of_year"] == 10


class TestMeteoDataMigrator:
    """Tests for MeteoDataMigrator transformation methods."""

    def test_get_data_type(self):
        """Test data type is 'meteo'."""
        migrator = MeteoDataMigrator(meteo_type="T")
        assert migrator.get_data_type() == "meteo"

    def test_prepare_day_data_temperature(self):
        """Test daily temperature data transformation."""
        migrator = MeteoDataMigrator(meteo_type="T")

        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "T": [15.567],
            "norm": [12.0],
        })

        records = migrator.prepare_day_data(df)

        assert len(records) == 1
        assert records[0]["meteo_type"] == "T"
        assert records[0]["code"] == "12345"
        assert records[0]["date"] == "2024-01-15"
        assert records[0]["value"] == 15.57  # Rounded to 2 decimals
        assert records[0]["norm"] == 12.0
        assert records[0]["day_of_year"] == 15

    def test_prepare_day_data_precipitation(self):
        """Test daily precipitation data transformation."""
        migrator = MeteoDataMigrator(meteo_type="P")

        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "P": [5.234],
            "norm": [4.0],
        })

        records = migrator.prepare_day_data(df)

        assert records[0]["meteo_type"] == "P"
        assert records[0]["value"] == 5.23  # Rounded to 2 decimals

    def test_prepare_day_data_null_values(self):
        """Test meteo data with null values."""
        migrator = MeteoDataMigrator(meteo_type="T")

        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "T": [None],
            "norm": [float("nan")],
        })

        records = migrator.prepare_day_data(df)

        assert records[0]["value"] is None
        assert records[0]["norm"] is None

    def test_prepare_pentad_data_raises(self):
        """Test that pentad is not supported for meteo."""
        migrator = MeteoDataMigrator(meteo_type="T")
        df = pd.DataFrame()

        with pytest.raises(NotImplementedError):
            migrator.prepare_pentad_data(df)

    def test_prepare_decade_data_raises(self):
        """Test that decade is not supported for meteo."""
        migrator = MeteoDataMigrator(meteo_type="T")
        df = pd.DataFrame()

        with pytest.raises(NotImplementedError):
            migrator.prepare_decade_data(df)

    def test_load_and_merge_data_with_dashboard(self):
        """Test loading and merging reanalysis with dashboard data."""
        migrator = MeteoDataMigrator(meteo_type="T")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create reanalysis file
            reanalysis_path = Path(tmpdir) / "00003_T_reanalysis.csv"
            pd.DataFrame({
                "date": ["2024-01-01", "2024-01-02"],
                "code": ["12345", "12345"],
                "T": [10.0, 12.0],
            }).to_csv(reanalysis_path, index=False)

            # Create dashboard file with norms
            dashboard_path = Path(tmpdir) / "00003_T_reanalysis_dashboard.csv"
            pd.DataFrame({
                "date": ["2024-01-01", "2024-01-02"],
                "code": ["12345", "12345"],
                "T_norm": [8.0, 9.0],
            }).to_csv(dashboard_path, index=False)

            df = migrator.load_and_merge_data(reanalysis_path, dashboard_path)

            assert len(df) == 2
            assert "norm" in df.columns
            assert df.iloc[0]["norm"] == 8.0
            assert df.iloc[1]["norm"] == 9.0

    def test_load_and_merge_data_without_dashboard(self):
        """Test loading reanalysis when dashboard file doesn't exist."""
        migrator = MeteoDataMigrator(meteo_type="T")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create reanalysis file only
            reanalysis_path = Path(tmpdir) / "00003_T_reanalysis.csv"
            pd.DataFrame({
                "date": ["2024-01-01"],
                "code": ["12345"],
                "T": [10.0],
            }).to_csv(reanalysis_path, index=False)

            dashboard_path = Path(tmpdir) / "nonexistent.csv"

            df = migrator.load_and_merge_data(reanalysis_path, dashboard_path)

            assert len(df) == 1
            assert "norm" in df.columns
            assert df.iloc[0]["norm"] is None


class TestSnowDataMigrator:
    """Tests for SnowDataMigrator transformation methods."""

    def test_get_data_type(self):
        """Test data type is 'snow'."""
        migrator = SnowDataMigrator(snow_type="HS")
        assert migrator.get_data_type() == "snow"

    def test_prepare_day_data_basic(self):
        """Test basic snow height data transformation."""
        migrator = SnowDataMigrator(snow_type="HS")

        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "value": [50.0],
            "value1": [40.0],
            "value2": [45.0],
            "value3": [50.0],
            "value4": [55.0],
            "value5": [60.0],
            "value6": [None],
            "value7": [None],
            "value8": [None],
            "value9": [None],
            "value10": [None],
            "value11": [None],
            "value12": [None],
            "value13": [None],
            "value14": [None],
        })

        records = migrator.prepare_day_data(df)

        assert len(records) == 1
        assert records[0]["snow_type"] == "HS"
        assert records[0]["code"] == "12345"
        assert records[0]["date"] == "2024-01-15"
        assert records[0]["value"] == 50.0
        assert records[0]["value1"] == 40.0
        assert records[0]["value2"] == 45.0
        assert records[0]["value3"] == 50.0
        assert records[0]["value4"] == 55.0
        assert records[0]["value5"] == 60.0
        assert records[0]["value6"] is None

    def test_prepare_day_data_swe(self):
        """Test SWE data transformation."""
        migrator = SnowDataMigrator(snow_type="SWE")

        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "value": [100.0],
            **{f"value{i}": [None] for i in range(1, 15)},
        })

        records = migrator.prepare_day_data(df)

        assert records[0]["snow_type"] == "SWE"
        assert records[0]["value"] == 100.0

    def test_prepare_day_data_rof(self):
        """Test ROF (runoff) data transformation."""
        migrator = SnowDataMigrator(snow_type="rof")  # Lowercase input

        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "value": [25.0],
            **{f"value{i}": [None] for i in range(1, 15)},
        })

        records = migrator.prepare_day_data(df)

        assert records[0]["snow_type"] == "ROF"  # Uppercase in output

    def test_prepare_day_data_null_values(self):
        """Test snow data with null values."""
        migrator = SnowDataMigrator(snow_type="HS")

        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "value": [None],
            **{f"value{i}": [float("nan")] for i in range(1, 15)},
        })

        records = migrator.prepare_day_data(df)

        assert records[0]["value"] is None
        assert records[0]["value1"] is None

    def test_prepare_pentad_data_raises(self):
        """Test that pentad is not supported for snow."""
        migrator = SnowDataMigrator(snow_type="HS")
        df = pd.DataFrame()

        with pytest.raises(NotImplementedError):
            migrator.prepare_pentad_data(df)

    def test_prepare_decade_data_raises(self):
        """Test that decade is not supported for snow."""
        migrator = SnowDataMigrator(snow_type="HS")
        df = pd.DataFrame()

        with pytest.raises(NotImplementedError):
            migrator.prepare_decade_data(df)

    def test_load_and_merge_data_with_secondary(self):
        """Test loading and merging main with secondary (zone) data."""
        migrator = SnowDataMigrator(snow_type="HS")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create main file
            main_path = Path(tmpdir) / "00003_HS.csv"
            pd.DataFrame({
                "date": ["2024-01-01"],
                "code": ["12345"],
                "HS": [50.0],
            }).to_csv(main_path, index=False)

            # Create secondary file with zone values
            secondary_path = Path(tmpdir) / "KGZ500m_HS.csv"
            pd.DataFrame({
                "date": ["2024-01-01"],
                "code": ["12345"],
                "HS_1": [40.0],
                "HS_2": [45.0],
                "HS_3": [50.0],
                **{f"HS_{i}": [None] for i in range(4, 15)},
            }).to_csv(secondary_path, index=False)

            df = migrator.load_and_merge_data(main_path, secondary_path)

            assert len(df) == 1
            assert "value" in df.columns
            assert df.iloc[0]["value"] == 50.0
            assert df.iloc[0]["value1"] == 40.0
            assert df.iloc[0]["value2"] == 45.0
            assert df.iloc[0]["value3"] == 50.0

    def test_load_and_merge_data_without_secondary(self):
        """Test loading main file when secondary doesn't exist."""
        migrator = SnowDataMigrator(snow_type="HS")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create main file only
            main_path = Path(tmpdir) / "00003_HS.csv"
            pd.DataFrame({
                "date": ["2024-01-01"],
                "code": ["12345"],
                "HS": [50.0],
            }).to_csv(main_path, index=False)

            secondary_path = Path(tmpdir) / "nonexistent.csv"

            df = migrator.load_and_merge_data(main_path, secondary_path)

            assert len(df) == 1
            assert df.iloc[0]["value"] == 50.0
            # Zone values should be None
            for i in range(1, 15):
                assert df.iloc[0][f"value{i}"] is None


class TestDataMigratorBase:
    """Tests for DataMigrator base class methods."""

    def test_send_batch_success(self):
        """Test successful batch send."""
        migrator = RunoffDataMigrator(api_base_url="http://test:8000")

        with patch.object(migrator.session, 'post') as mock_post:
            mock_response = Mock()
            mock_response.raise_for_status = Mock()
            mock_post.return_value = mock_response

            batch = [{"code": "12345", "date": "2024-01-01"}]
            success, count = migrator.send_batch(batch)

            assert success is True
            assert count == 1
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            assert call_args[0][0] == "http://test:8000/runoff/"
            assert call_args[1]["json"] == {"data": batch}

    def test_send_batch_failure(self):
        """Test batch send failure."""
        import requests
        migrator = RunoffDataMigrator(api_base_url="http://test:8000")

        with patch.object(migrator.session, 'post') as mock_post:
            # Use requests exception type which is caught by send_batch
            mock_post.side_effect = requests.exceptions.ConnectionError("Connection error")

            batch = [{"code": "12345", "date": "2024-01-01"}]
            success, count = migrator.send_batch(batch)

            assert success is False
            assert count == 0

    def test_migrate_csv_day_horizon(self):
        """Test migrate_csv with day horizon."""
        migrator = RunoffDataMigrator(api_base_url="http://test:8000", batch_size=2)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test CSV
            csv_path = Path(tmpdir) / "runoff_day.csv"
            pd.DataFrame({
                "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
                "code": ["12345", "12345", "12345"],
                "discharge": [100.0, 110.0, 120.0],
            }).to_csv(csv_path, index=False)

            with patch.object(migrator, 'send_batch') as mock_send:
                mock_send.return_value = (True, 2)

                stats = migrator.migrate_csv(csv_path, "day")

                assert stats.data_type == "runoff"
                assert stats.horizon_type == "day"
                assert stats.total_records == 3
                # With batch_size=2, should have 2 batches
                assert mock_send.call_count == 2

    def test_migrate_csv_invalid_horizon(self):
        """Test migrate_csv with invalid horizon type."""
        migrator = RunoffDataMigrator(api_base_url="http://test:8000")

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            pd.DataFrame({"date": ["2024-01-01"], "code": ["12345"], "discharge": [100.0]}).to_csv(csv_path, index=False)

            with pytest.raises(ValueError, match="Unknown horizon type"):
                migrator.migrate_csv(csv_path, "invalid")


class TestRecordSchemaCompliance:
    """Tests to verify records match expected API schema."""

    def test_runoff_day_record_schema(self):
        """Verify runoff day record has all required fields."""
        migrator = RunoffDataMigrator()
        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "discharge": [100.0],
        })

        records = migrator.prepare_day_data(df)
        record = records[0]

        # Required fields per API schema
        required_fields = ["horizon_type", "code", "date", "discharge", "predictor",
                          "horizon_value", "horizon_in_year"]
        for field in required_fields:
            assert field in record, f"Missing required field: {field}"

    def test_hydrograph_day_record_schema(self):
        """Verify hydrograph day record has all required fields."""
        migrator = HydrographDataMigrator()
        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "day_of_year": [15],
            "count": [30],
            "mean": [100.0],
            "std": [10.0],
            "min": [80.0],
            "max": [120.0],
            "5%": [82.0],
            "25%": [90.0],
            "50%": [100.0],
            "75%": [110.0],
            "95%": [118.0],
        })

        records = migrator.prepare_day_data(df)
        record = records[0]

        # Required fields per API schema
        required_fields = ["horizon_type", "code", "date", "day_of_year",
                          "horizon_value", "horizon_in_year",
                          "count", "mean", "std", "min", "max",
                          "q05", "q25", "q50", "q75", "q95",
                          "norm", "previous", "current"]
        for field in required_fields:
            assert field in record, f"Missing required field: {field}"

    def test_meteo_day_record_schema(self):
        """Verify meteo day record has all required fields."""
        migrator = MeteoDataMigrator(meteo_type="T")
        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "T": [15.0],
            "norm": [12.0],
        })

        records = migrator.prepare_day_data(df)
        record = records[0]

        # Required fields per API schema
        required_fields = ["meteo_type", "code", "date", "value", "norm", "day_of_year"]
        for field in required_fields:
            assert field in record, f"Missing required field: {field}"

    def test_snow_day_record_schema(self):
        """Verify snow day record has all required fields."""
        migrator = SnowDataMigrator(snow_type="HS")
        df = pd.DataFrame({
            "date": ["2024-01-15"],
            "code": ["12345"],
            "value": [50.0],
            **{f"value{i}": [None] for i in range(1, 15)},
        })

        records = migrator.prepare_day_data(df)
        record = records[0]

        # Required fields per API schema
        required_fields = ["snow_type", "code", "date", "value"]
        required_fields += [f"value{i}" for i in range(1, 15)]
        for field in required_fields:
            assert field in record, f"Missing required field: {field}"
