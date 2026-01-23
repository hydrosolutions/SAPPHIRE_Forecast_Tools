"""
Tests for postprocessing data_migrator.py

These tests verify the transformation logic of forecast data migrators to ensure
the status quo is preserved when integrating the new sapphire-api-client.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch
import tempfile
import requests

# Add app to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "app"))

from data_migrator import (
    MigrationStats,
    DataMigrator,
    CombinedForecastDataMigrator,
    LRForecastDataMigrator,
    SkillMetricDataMigrator,
)


class TestMigrationStats:
    """Tests for MigrationStats dataclass."""

    def test_str_representation(self):
        """Test string representation of stats."""
        stats = MigrationStats(
            horizon_type="pentad",
            total_records=1000,
            successful=950,
            failed=50,
            duration_seconds=10.5,
            records_per_second=90.48,
            batch_count=10,
        )
        result = str(stats)
        assert "PENTAD" in result
        assert "1,000" in result
        assert "950" in result
        assert "95.00%" in result


class TestCombinedForecastDataMigrator:
    """Tests for CombinedForecastDataMigrator transformation methods."""

    def setup_method(self):
        """Set up test migrator."""
        self.migrator = CombinedForecastDataMigrator(
            api_base_url="http://test:8000",
            sub_url="forecasts"
        )

    def test_prepare_pentad_data_basic(self):
        """Test basic pentad forecast data transformation."""
        df = pd.DataFrame({
            "code": ["12345"],
            "model_short": ["ANN"],
            "date": ["2024-01-05"],
            "flag": [1],
            "pentad_in_month": [1],
            "pentad_in_year": [1],
            "Q5": [80.0],
            "Q25": [90.0],
            "Q75": [110.0],
            "Q95": [120.0],
            "forecasted_discharge": [100.0],
        })

        records = self.migrator.prepare_pentad_data(df)

        assert len(records) == 1
        assert records[0]["horizon_type"] == "pentad"
        assert records[0]["code"] == "12345"
        assert records[0]["model_type"] == "ANN"
        assert records[0]["date"] == "2024-01-05"
        assert records[0]["flag"] == 1
        assert records[0]["horizon_value"] == 1
        assert records[0]["horizon_in_year"] == 1
        assert records[0]["q05"] == 80.0
        assert records[0]["q25"] == 90.0
        assert records[0]["q50"] is None  # Always None per implementation
        assert records[0]["q75"] == 110.0
        assert records[0]["q95"] == 120.0
        assert records[0]["forecasted_discharge"] == 100.0
        assert records[0]["target"] is None

    def test_prepare_pentad_data_skips_lr_model(self):
        """Test that LR model records are skipped."""
        df = pd.DataFrame({
            "code": ["12345", "12345"],
            "model_short": ["LR", "ANN"],
            "date": ["2024-01-05", "2024-01-05"],
            "flag": [1, 1],
            "pentad_in_month": [1, 1],
            "pentad_in_year": [1, 1],
            "Q5": [80.0, 85.0],
            "Q25": [90.0, 95.0],
            "Q75": [110.0, 115.0],
            "Q95": [120.0, 125.0],
            "forecasted_discharge": [100.0, 105.0],
        })

        records = self.migrator.prepare_pentad_data(df)

        # Only ANN record should be included, LR is skipped
        assert len(records) == 1
        assert records[0]["model_type"] == "ANN"

    def test_prepare_pentad_data_skips_missing_pentad_in_year(self):
        """Test that records with missing pentad_in_year are skipped."""
        df = pd.DataFrame({
            "code": ["12345", "12346"],
            "model_short": ["ANN", "ANN"],
            "date": ["2024-01-05", "2024-01-10"],
            "flag": [1, 1],
            "pentad_in_month": [1, 2],
            "pentad_in_year": [1, None],
            "Q5": [80.0, 85.0],
            "Q25": [90.0, 95.0],
            "Q75": [110.0, 115.0],
            "Q95": [120.0, 125.0],
            "forecasted_discharge": [100.0, 105.0],
        })

        records = self.migrator.prepare_pentad_data(df)

        assert len(records) == 1
        assert records[0]["code"] == "12345"

    def test_prepare_pentad_data_null_values(self):
        """Test pentad data with null values."""
        df = pd.DataFrame({
            "code": ["12345"],
            "model_short": ["ANN"],
            "date": ["2024-01-05"],
            "flag": [None],
            "pentad_in_month": [1],
            "pentad_in_year": [1],
            "Q5": [None],
            "Q25": [float("nan")],
            "Q75": [None],
            "Q95": [None],
            "forecasted_discharge": [None],
        })

        records = self.migrator.prepare_pentad_data(df)

        assert records[0]["flag"] is None
        assert records[0]["q05"] is None
        assert records[0]["q25"] is None
        assert records[0]["forecasted_discharge"] is None

    def test_prepare_decade_data_basic(self):
        """Test basic decade forecast data transformation."""
        df = pd.DataFrame({
            "code": ["12345"],
            "model_short": ["ANN"],
            "date": ["2024-01-10"],
            "flag": [1],
            "decad": [1],
            "decad_in_year": [1],
            "Q5": [80.0],
            "Q25": [90.0],
            "Q75": [110.0],
            "Q95": [120.0],
            "forecasted_discharge": [100.0],
        })

        records = self.migrator.prepare_decade_data(df)

        assert len(records) == 1
        assert records[0]["horizon_type"] == "decade"
        assert records[0]["code"] == "12345"
        assert records[0]["horizon_value"] == 1
        assert records[0]["horizon_in_year"] == 1

    def test_prepare_decade_data_skips_lr_model(self):
        """Test that LR model records are skipped in decade data."""
        df = pd.DataFrame({
            "code": ["12345"],
            "model_short": ["LR"],
            "date": ["2024-01-10"],
            "flag": [1],
            "decad": [1],
            "decad_in_year": [1],
            "Q5": [80.0],
            "Q25": [90.0],
            "Q75": [110.0],
            "Q95": [120.0],
            "forecasted_discharge": [100.0],
        })

        records = self.migrator.prepare_decade_data(df)

        assert len(records) == 0


class TestLRForecastDataMigrator:
    """Tests for LRForecastDataMigrator transformation methods."""

    def setup_method(self):
        """Set up test migrator."""
        self.migrator = LRForecastDataMigrator(
            api_base_url="http://test:8000",
            sub_url="lr-forecast"
        )

    def test_prepare_pentad_data_basic(self):
        """Test basic pentad LR forecast data transformation."""
        df = pd.DataFrame({
            "code": ["12345"],
            "date": ["2024-01-05"],
            "pentad_in_month": [1],
            "pentad_in_year": [1],
            "discharge_avg": [100.0],
            "predictor": [80.0],
            "slope": [1.2],
            "intercept": [10.0],
            "forecasted_discharge": [106.0],
            "q_mean": [95.0],
            "q_std_sigma": [15.0],
            "delta": [5.0],
            "rsquared": [0.85],
        })

        records = self.migrator.prepare_pentad_data(df)

        assert len(records) == 1
        assert records[0]["horizon_type"] == "pentad"
        assert records[0]["code"] == "12345"
        assert records[0]["date"] == "2024-01-05"
        assert records[0]["horizon_value"] == 1
        assert records[0]["horizon_in_year"] == 1
        assert records[0]["discharge_avg"] == 100.0
        assert records[0]["predictor"] == 80.0
        assert records[0]["slope"] == 1.2
        assert records[0]["intercept"] == 10.0
        assert records[0]["forecasted_discharge"] == 106.0
        assert records[0]["q_mean"] == 95.0
        assert records[0]["q_std_sigma"] == 15.0
        assert records[0]["delta"] == 5.0
        assert records[0]["rsquared"] == 0.85

    def test_prepare_pentad_data_null_values(self):
        """Test pentad LR data with null values."""
        df = pd.DataFrame({
            "code": ["12345"],
            "date": ["2024-01-05"],
            "pentad_in_month": [1],
            "pentad_in_year": [1],
            "discharge_avg": [None],
            "predictor": [float("nan")],
            "slope": [None],
            "intercept": [None],
            "forecasted_discharge": [None],
            "q_mean": [None],
            "q_std_sigma": [None],
            "delta": [None],
            "rsquared": [None],
        })

        records = self.migrator.prepare_pentad_data(df)

        assert records[0]["discharge_avg"] is None
        assert records[0]["predictor"] is None
        assert records[0]["slope"] is None
        assert records[0]["rsquared"] is None

    def test_prepare_decade_data_basic(self):
        """Test basic decade LR forecast data transformation."""
        df = pd.DataFrame({
            "code": ["12345"],
            "date": ["2024-01-10"],
            "decad_in_month": [1],
            "decad_in_year": [1],
            "discharge_avg": [100.0],
            "predictor": [80.0],
            "slope": [1.2],
            "intercept": [10.0],
            "forecasted_discharge": [106.0],
            "q_mean": [95.0],
            "q_std_sigma": [15.0],
            "delta": [5.0],
            "rsquared": [0.85],
        })

        records = self.migrator.prepare_decade_data(df)

        assert len(records) == 1
        assert records[0]["horizon_type"] == "decade"
        assert records[0]["code"] == "12345"
        assert records[0]["horizon_value"] == 1
        assert records[0]["horizon_in_year"] == 1

    def test_prepare_decade_data_code_as_string(self):
        """Test that code is converted to string."""
        df = pd.DataFrame({
            "code": [12345],  # Integer
            "date": ["2024-01-10"],
            "decad_in_month": [1],
            "decad_in_year": [1],
            "discharge_avg": [100.0],
            "predictor": [80.0],
            "slope": [1.2],
            "intercept": [10.0],
            "forecasted_discharge": [106.0],
            "q_mean": [95.0],
            "q_std_sigma": [15.0],
            "delta": [5.0],
            "rsquared": [0.85],
        })

        records = self.migrator.prepare_decade_data(df)

        assert records[0]["code"] == "12345"
        assert isinstance(records[0]["code"], str)


class TestSkillMetricDataMigrator:
    """Tests for SkillMetricDataMigrator transformation methods."""

    def setup_method(self):
        """Set up test migrator."""
        self.migrator = SkillMetricDataMigrator(
            api_base_url="http://test:8000",
            sub_url="skill-metrics"
        )

    def test_prepare_pentad_data_basic(self):
        """Test basic pentad skill metric data transformation."""
        df = pd.DataFrame({
            "code": ["12345"],
            "model_short": ["ANN"],
            "pentad_in_year": [1],
            "sdivsigma": [0.8],
            "nse": [0.75],
            "delta": [5.0],
            "accuracy": [0.85],
            "mae": [10.5],
            "n_pairs": [50],
        })

        records = self.migrator.prepare_pentad_data(df)

        assert len(records) == 1
        assert records[0]["horizon_type"] == "pentad"
        assert records[0]["code"] == "12345"
        assert records[0]["model_type"] == "ANN"
        assert records[0]["horizon_in_year"] == 1
        # Date is looked up from pentad_to_date mapping
        assert records[0]["date"] == "2025-12-31"  # pentad 1
        assert records[0]["sdivsigma"] == 0.8
        assert records[0]["nse"] == 0.75
        assert records[0]["delta"] == 5.0
        assert records[0]["accuracy"] == 0.85
        assert records[0]["mae"] == 10.5
        assert records[0]["n_pairs"] == 50

    def test_prepare_pentad_data_various_horizons(self):
        """Test pentad skill metrics with various horizon values."""
        df = pd.DataFrame({
            "code": ["12345", "12345", "12345"],
            "model_short": ["ANN", "ANN", "ANN"],
            "pentad_in_year": [1, 36, 72],  # First, middle, last pentad
            "sdivsigma": [0.8, 0.7, 0.9],
            "nse": [0.75, 0.80, 0.70],
            "delta": [5.0, 4.0, 6.0],
            "accuracy": [0.85, 0.90, 0.80],
            "mae": [10.5, 9.0, 12.0],
            "n_pairs": [50, 55, 45],
        })

        records = self.migrator.prepare_pentad_data(df)

        assert len(records) == 3
        assert records[0]["date"] == "2025-12-31"  # pentad 1
        assert records[1]["date"] == "2026-06-25"  # pentad 36
        assert records[2]["date"] == "2026-12-25"  # pentad 72

    def test_prepare_pentad_data_null_values(self):
        """Test pentad skill metrics with null values."""
        df = pd.DataFrame({
            "code": ["12345"],
            "model_short": ["ANN"],
            "pentad_in_year": [1],
            "sdivsigma": [None],
            "nse": [float("nan")],
            "delta": [None],
            "accuracy": [None],
            "mae": [None],
            "n_pairs": [None],
        })

        records = self.migrator.prepare_pentad_data(df)

        assert records[0]["sdivsigma"] is None
        assert records[0]["nse"] is None
        assert records[0]["delta"] is None
        assert records[0]["accuracy"] is None
        assert records[0]["mae"] is None
        assert records[0]["n_pairs"] is None

    def test_prepare_decade_data_basic(self):
        """Test basic decade skill metric data transformation."""
        df = pd.DataFrame({
            "code": ["12345"],
            "model_short": ["LR"],
            "decad_in_year": [1],
            "sdivsigma": [0.8],
            "nse": [0.75],
            "delta": [5.0],
            "accuracy": [0.85],
            "mae": [10.5],
            "n_pairs": [50],
        })

        records = self.migrator.prepare_decade_data(df)

        assert len(records) == 1
        assert records[0]["horizon_type"] == "decade"
        assert records[0]["code"] == "12345"
        assert records[0]["model_type"] == "LR"
        assert records[0]["horizon_in_year"] == 1
        assert records[0]["date"] == "2025-12-31"  # decade 1

    def test_prepare_decade_data_various_horizons(self):
        """Test decade skill metrics with various horizon values."""
        df = pd.DataFrame({
            "code": ["12345", "12345", "12345"],
            "model_short": ["ANN", "ANN", "ANN"],
            "decad_in_year": [1, 18, 36],  # First, middle, last decade
            "sdivsigma": [0.8, 0.7, 0.9],
            "nse": [0.75, 0.80, 0.70],
            "delta": [5.0, 4.0, 6.0],
            "accuracy": [0.85, 0.90, 0.80],
            "mae": [10.5, 9.0, 12.0],
            "n_pairs": [50, 55, 45],
        })

        records = self.migrator.prepare_decade_data(df)

        assert len(records) == 3
        assert records[0]["date"] == "2025-12-31"  # decade 1
        assert records[1]["date"] == "2026-06-20"  # decade 18
        assert records[2]["date"] == "2026-12-20"  # decade 36


class TestDataMigratorBase:
    """Tests for DataMigrator base class methods."""

    def test_send_batch_success(self):
        """Test successful batch send."""
        migrator = LRForecastDataMigrator(
            api_base_url="http://test:8000",
            sub_url="lr-forecast"
        )

        with patch.object(migrator.session, 'post') as mock_post:
            mock_response = Mock()
            mock_response.raise_for_status = Mock()
            mock_post.return_value = mock_response

            batch = [{"code": "12345", "date": "2024-01-01"}]
            success, count = migrator.send_batch(batch, "lr-forecast")

            assert success is True
            assert count == 1
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            assert call_args[0][0] == "http://test:8000/lr-forecast/"
            assert call_args[1]["json"] == {"data": batch}

    def test_send_batch_failure(self):
        """Test batch send failure."""
        migrator = LRForecastDataMigrator(
            api_base_url="http://test:8000",
            sub_url="lr-forecast"
        )

        with patch.object(migrator.session, 'post') as mock_post:
            mock_post.side_effect = requests.exceptions.ConnectionError("Connection error")

            batch = [{"code": "12345", "date": "2024-01-01"}]
            success, count = migrator.send_batch(batch, "lr-forecast")

            assert success is False
            assert count == 0

    def test_migrate_csv_pentad_horizon(self):
        """Test migrate_csv with pentad horizon."""
        migrator = LRForecastDataMigrator(
            api_base_url="http://test:8000",
            batch_size=2,
            sub_url="lr-forecast"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create test CSV
            csv_path = Path(tmpdir) / "lr_forecast_pentad.csv"
            pd.DataFrame({
                "code": ["12345", "12345", "12345"],
                "date": ["2024-01-05", "2024-01-10", "2024-01-15"],
                "pentad_in_month": [1, 2, 3],
                "pentad_in_year": [1, 2, 3],
                "discharge_avg": [100.0, 110.0, 120.0],
                "predictor": [80.0, 85.0, 90.0],
                "slope": [1.2, 1.3, 1.4],
                "intercept": [10.0, 11.0, 12.0],
                "forecasted_discharge": [106.0, 116.0, 126.0],
                "q_mean": [95.0, 100.0, 105.0],
                "q_std_sigma": [15.0, 16.0, 17.0],
                "delta": [5.0, 6.0, 7.0],
                "rsquared": [0.85, 0.86, 0.87],
            }).to_csv(csv_path, index=False)

            with patch.object(migrator, 'send_batch') as mock_send:
                mock_send.return_value = (True, 2)

                stats = migrator.migrate_csv(csv_path, "pentad")

                assert stats.horizon_type == "pentad"
                assert stats.total_records == 3
                # With batch_size=2, should have 2 batches
                assert mock_send.call_count == 2

    def test_migrate_csv_invalid_horizon(self):
        """Test migrate_csv with invalid horizon type."""
        migrator = LRForecastDataMigrator(
            api_base_url="http://test:8000",
            sub_url="lr-forecast"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            pd.DataFrame({
                "code": ["12345"],
                "date": ["2024-01-05"],
                "pentad_in_month": [1],
                "pentad_in_year": [1],
                "discharge_avg": [100.0],
                "predictor": [80.0],
                "slope": [1.2],
                "intercept": [10.0],
                "forecasted_discharge": [106.0],
                "q_mean": [95.0],
                "q_std_sigma": [15.0],
                "delta": [5.0],
                "rsquared": [0.85],
            }).to_csv(csv_path, index=False)

            # Day horizon not supported for postprocessing
            with pytest.raises(ValueError, match="Unknown horizon type"):
                migrator.migrate_csv(csv_path, "day")


class TestRecordSchemaCompliance:
    """Tests to verify records match expected API schema."""

    def test_combined_forecast_pentad_record_schema(self):
        """Verify combined forecast pentad record has all required fields."""
        migrator = CombinedForecastDataMigrator(sub_url="forecasts")
        df = pd.DataFrame({
            "code": ["12345"],
            "model_short": ["ANN"],
            "date": ["2024-01-05"],
            "flag": [1],
            "pentad_in_month": [1],
            "pentad_in_year": [1],
            "Q5": [80.0],
            "Q25": [90.0],
            "Q75": [110.0],
            "Q95": [120.0],
            "forecasted_discharge": [100.0],
        })

        records = migrator.prepare_pentad_data(df)
        record = records[0]

        required_fields = [
            "horizon_type", "code", "model_type", "date", "target", "flag",
            "horizon_value", "horizon_in_year",
            "q05", "q25", "q50", "q75", "q95", "forecasted_discharge"
        ]
        for field in required_fields:
            assert field in record, f"Missing required field: {field}"

    def test_lr_forecast_pentad_record_schema(self):
        """Verify LR forecast pentad record has all required fields."""
        migrator = LRForecastDataMigrator(sub_url="lr-forecast")
        df = pd.DataFrame({
            "code": ["12345"],
            "date": ["2024-01-05"],
            "pentad_in_month": [1],
            "pentad_in_year": [1],
            "discharge_avg": [100.0],
            "predictor": [80.0],
            "slope": [1.2],
            "intercept": [10.0],
            "forecasted_discharge": [106.0],
            "q_mean": [95.0],
            "q_std_sigma": [15.0],
            "delta": [5.0],
            "rsquared": [0.85],
        })

        records = migrator.prepare_pentad_data(df)
        record = records[0]

        required_fields = [
            "horizon_type", "code", "date", "horizon_value", "horizon_in_year",
            "discharge_avg", "predictor", "slope", "intercept",
            "forecasted_discharge", "q_mean", "q_std_sigma", "delta", "rsquared"
        ]
        for field in required_fields:
            assert field in record, f"Missing required field: {field}"

    def test_skill_metric_pentad_record_schema(self):
        """Verify skill metric pentad record has all required fields."""
        migrator = SkillMetricDataMigrator(sub_url="skill-metrics")
        df = pd.DataFrame({
            "code": ["12345"],
            "model_short": ["ANN"],
            "pentad_in_year": [1],
            "sdivsigma": [0.8],
            "nse": [0.75],
            "delta": [5.0],
            "accuracy": [0.85],
            "mae": [10.5],
            "n_pairs": [50],
        })

        records = migrator.prepare_pentad_data(df)
        record = records[0]

        required_fields = [
            "horizon_type", "code", "model_type", "date", "horizon_in_year",
            "sdivsigma", "nse", "delta", "accuracy", "mae", "n_pairs"
        ]
        for field in required_fields:
            assert field in record, f"Missing required field: {field}"
