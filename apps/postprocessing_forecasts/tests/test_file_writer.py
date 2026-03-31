"""Tests for src/file_writer.py — atomic CSV writes and save orchestration.

Moved from iEasyHydroForecast/tests/test_forecast_library.py (TestAtomicWriteCSV).
"""

import os
import sys
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from conftest import DECAD, PENTAD
from src import api_writer, file_writer


class TestAtomicWriteCSV:
    """Tests for atomic_write_csv that prevents data loss."""

    @pytest.fixture(autouse=True)
    def setup_dirs(self, tmp_path):
        self.test_dir = str(tmp_path)
        self.test_file = os.path.join(self.test_dir, "test_output.csv")
        self.test_data = pd.DataFrame(
            {
                "code": ["15102", "15124", "15136"],
                "value": [100.0, 200.0, 300.0],
                "date": ["2023-05-25", "2023-05-25", "2023-05-25"],
            }
        )

    def test_atomic_write_success(self):
        """atomic_write_csv writes data to a new file."""
        file_writer.atomic_write_csv(self.test_data, self.test_file, index=False)
        assert os.path.exists(self.test_file)
        result = pd.read_csv(self.test_file, dtype={"code": str})
        assert_frame_equal(result, self.test_data, check_dtype=False)

    def test_atomic_write_overwrites_existing(self):
        """atomic_write_csv overwrites existing files correctly."""
        initial_data = pd.DataFrame({"code": ["OLD"], "value": [999]})
        initial_data.to_csv(self.test_file, index=False)

        file_writer.atomic_write_csv(self.test_data, self.test_file, index=False)
        result = pd.read_csv(self.test_file, dtype={"code": str})
        assert_frame_equal(result, self.test_data, check_dtype=False)

    def test_atomic_write_preserves_original_on_failure(self):
        """Original file preserved if write fails (core atomic property)."""
        initial_data = pd.DataFrame({"code": ["ORIGINAL"], "value": [123]})
        initial_data.to_csv(self.test_file, index=False)

        def failing_to_csv(*args, **kwargs):
            raise OSError("Simulated write failure")

        with patch.object(pd.DataFrame, "to_csv", failing_to_csv):
            with pytest.raises(IOError):
                file_writer.atomic_write_csv(self.test_data, self.test_file, index=False)

        assert os.path.exists(self.test_file)
        result = pd.read_csv(self.test_file)
        assert_frame_equal(result, initial_data, check_dtype=False)

    def test_atomic_write_creates_directory(self):
        """atomic_write_csv creates parent directories if needed."""
        nested_path = os.path.join(self.test_dir, "subdir1", "subdir2", "output.csv")
        file_writer.atomic_write_csv(self.test_data, nested_path, index=False)
        assert os.path.exists(nested_path)
        result = pd.read_csv(nested_path, dtype={"code": str})
        assert_frame_equal(result, self.test_data, check_dtype=False)

    def test_atomic_write_with_kwargs(self):
        """atomic_write_csv passes kwargs to to_csv correctly."""
        file_writer.atomic_write_csv(self.test_data, self.test_file, index=False, sep=";")
        with open(self.test_file) as f:
            content = f.read()
            assert ";" in content
            assert "15102" in content
            assert "100.0" in content

    def test_atomic_write_no_temp_files_remain(self):
        """No temporary files remain after successful write."""
        file_writer.atomic_write_csv(self.test_data, self.test_file, index=False)
        files = os.listdir(self.test_dir)
        assert len(files) == 1
        assert files[0] == "test_output.csv"


class TestSaveMonthlySkillMetrics:
    """Tests for save_monthly_skill_metrics().

    Follows the same pattern as save_skill_metrics(PENTAD/DECAD, ...):
    round, clean codes, convert horizon to int, sort, atomic CSV, API write.
    """

    @pytest.fixture(autouse=True)
    def save_env(self, tmp_path):
        """Set env vars so save_monthly_skill_metrics can resolve paths."""
        overrides = {
            "ieasyforecast_intermediate_data_path": str(tmp_path),
            "ieasyforecast_monthly_skill_metrics_file": "skill_monthly.csv",
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_CONSISTENCY_CHECK": "false",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            self.tmp_path = tmp_path
            yield tmp_path

    @pytest.fixture
    def monthly_skill_data(self):
        """Representative monthly skill metrics DataFrame."""
        return pd.DataFrame(
            {
                "month_in_year": [6, 6, 6, 3, 3, 3],
                "code": ["15013", "15013", "15013", "15014", "15014", "15014"],
                "model_short": ["GBT", "LR_Base", "EM", "GBT", "LR_Base", "EM"],
                "composition": [
                    "",
                    "",
                    "GBT, LR_Base",
                    "",
                    "",
                    "GBT, LR_Base",
                ],
                "sdivsigma": [0.34567, 0.41234, 0.29876, 0.50123, 0.36789, 0.32345],
                "nse": [0.92345, 0.88765, 0.94567, 0.81234, 0.90123, 0.86789],
                "delta": [12.5, 12.5, 12.5, 8.3, 8.3, 8.3],
                "accuracy": [0.92, 0.88, 0.95, 0.85, 0.91, 0.89],
                "mae": [2.12345, 3.45678, 1.89012, 4.56789, 2.34567, 3.01234],
                "n_pairs": [10, 10, 10, 8, 8, 8],
                "crps": [15.234, 18.567, np.nan, 12.345, 14.678, np.nan],
            }
        )

    def test_csv_written_to_correct_path(self, monthly_skill_data):
        """CSV file is created at the env-var-configured path."""
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        assert os.path.exists(csv_path)

    def test_csv_columns_present(self, monthly_skill_data):
        """CSV has all required columns including crps."""
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        saved = pd.read_csv(csv_path)

        for col in [
            "month_in_year",
            "code",
            "model_short",
            "sdivsigma",
            "nse",
            "delta",
            "accuracy",
            "mae",
            "n_pairs",
            "crps",
        ]:
            assert col in saved.columns, f"Missing column: {col}"

    def test_csv_sorted_by_month_code_model(self, monthly_skill_data):
        """CSV is sorted by (month_in_year, code, model_short)."""
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        saved = pd.read_csv(csv_path)

        # month_in_year should be ascending: 3 before 6
        assert saved.iloc[0]["month_in_year"] == 3
        assert saved.iloc[-1]["month_in_year"] == 6

        # Within each month, model_short should be sorted
        m3 = saved[saved["month_in_year"] == 3]
        model_shorts = list(m3["model_short"])
        assert model_shorts == sorted(model_shorts)

    def test_values_rounded_to_4_decimals(self, monthly_skill_data):
        """Float values are rounded to 4 decimal places."""
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        saved = pd.read_csv(csv_path, dtype={"code": str})

        row = saved[
            (saved["code"] == "15013")
            & (saved["model_short"] == "GBT")
            & (saved["month_in_year"] == 6)
        ]
        assert len(row) == 1
        # 0.34567 -> 0.3457
        assert abs(row.iloc[0]["sdivsigma"] - 0.3457) < 1e-5
        # 2.12345 -> 2.1234 (banker's rounding: 4 is even, rounds down)
        assert abs(row.iloc[0]["mae"] - 2.1234) < 1e-5

    def test_code_cleaned_no_dot_zero(self):
        """Code values like '15013.0' are cleaned to '15013'."""
        data = pd.DataFrame(
            {
                "month_in_year": [1],
                "code": [15013.0],  # float code
                "model_short": ["GBT"],
                "sdivsigma": [0.3],
                "nse": [0.9],
                "delta": [5.0],
                "accuracy": [0.9],
                "mae": [2.0],
                "n_pairs": [10],
                "crps": [12.0],
            }
        )
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_skill_metrics(data)

        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        saved = pd.read_csv(csv_path, dtype={"code": str})
        assert saved.iloc[0]["code"] == "15013"

    def test_month_in_year_is_int(self, monthly_skill_data):
        """month_in_year column is written as integer."""
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        saved = pd.read_csv(csv_path)
        assert saved["month_in_year"].dtype in (np.int64, np.int32, int)

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_api_write_called_with_month_horizon(self, mock_api_write, monthly_skill_data):
        """API writer is called with horizon_type='month' and year."""
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            mock_api_write.return_value = True
            file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2024)

        mock_api_write.assert_called_once()
        call_args = mock_api_write.call_args
        assert call_args[0][1] == "month"  # second positional arg
        # First arg is the data DataFrame
        assert len(call_args[0][0]) == 6
        # Third arg is the year
        assert call_args[0][2] == 2024

    def test_api_not_called_when_unavailable(self, monthly_skill_data):
        """API writer is not called when sapphire-api-client unavailable."""
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch("src.api_writer._write_skill_metrics_to_api") as mock_api,
        ):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)
            mock_api.assert_not_called()

    def test_composition_column_preserved_in_csv(self):
        """Composition column survives CSV round-trip."""
        data = pd.DataFrame(
            {
                "month_in_year": [6, 6],
                "code": ["15013", "15013"],
                "model_short": ["GBT", "EM"],
                "composition": ["", "GBT, LR_Base"],
                "sdivsigma": [0.3, 0.2],
                "nse": [0.9, 0.95],
                "delta": [12.5, 12.5],
                "accuracy": [0.9, 0.95],
                "mae": [2.0, 1.5],
                "n_pairs": [10, 10],
                "crps": [15.0, np.nan],
            }
        )
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_skill_metrics(data)

        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        saved = pd.read_csv(csv_path, dtype={"code": str})
        assert "composition" in saved.columns
        em_row = saved[saved["model_short"] == "EM"].iloc[0]
        assert em_row["composition"] == "GBT, LR_Base"

    def test_empty_dataframe_handled(self):
        """Empty DataFrame doesn't crash save_monthly_skill_metrics.

        An empty DataFrame with correct columns should produce a valid
        (empty) CSV file without raising on .astype(int).
        """
        data = pd.DataFrame(
            {
                "month_in_year": pd.Series([], dtype=float),
                "code": pd.Series([], dtype=str),
                "model_short": pd.Series([], dtype=str),
                "sdivsigma": pd.Series([], dtype=float),
                "nse": pd.Series([], dtype=float),
                "delta": pd.Series([], dtype=float),
                "accuracy": pd.Series([], dtype=float),
                "mae": pd.Series([], dtype=float),
                "n_pairs": pd.Series([], dtype=float),
                "crps": pd.Series([], dtype=float),
            }
        )
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_skill_metrics(data)

        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        assert os.path.exists(csv_path)
        saved = pd.read_csv(csv_path)
        assert len(saved) == 0

    def test_crps_nan_written_to_csv(self, monthly_skill_data):
        """NaN CRPS values (from EM/Naive Mean) are written to CSV.

        EM and Naive Mean have no quantile distribution so CRPS is NaN.
        The CSV should contain these NaN values without dropping rows.
        """
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        saved = pd.read_csv(csv_path, dtype={"code": str})
        em_rows = saved[saved["model_short"] == "EM"]
        assert len(em_rows) == 2
        assert all(pd.isna(em_rows["crps"]))

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_csv_written_when_api_fails(self, mock_api_write, monthly_skill_data):
        """CSV is still written to disk when API write raises."""
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        assert os.path.exists(csv_path)
        saved = pd.read_csv(csv_path, dtype={"code": str})
        assert len(saved) == 6


class TestSaveMonthlyForecastData:
    """Tests for save_monthly_forecast_data()."""

    @pytest.fixture(autouse=True)
    def save_env(self, tmp_path):
        """Set env vars for monthly forecast data save."""
        overrides = {
            "ieasyforecast_intermediate_data_path": str(tmp_path),
            "ieasyforecast_monthly_combined_forecast_file": "combined_monthly.csv",
            "SAPPHIRE_API_ENABLED": "false",
            "SAPPHIRE_CONSISTENCY_CHECK": "false",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            self.tmp_path = tmp_path
            yield

    @pytest.fixture
    def monthly_joint_data(self):
        """Monthly joint forecasts DataFrame."""
        return pd.DataFrame(
            {
                "code": ["15013", "15013", "15013", "15013"],
                "year": [2023, 2023, 2024, 2024],
                "month": [6, 6, 6, 6],
                "month_in_year": [6, 6, 6, 6],
                "date": pd.to_datetime(
                    [
                        "2023-06-01",
                        "2023-06-01",
                        "2024-06-01",
                        "2024-06-01",
                    ]
                ),
                "forecasted_discharge": [100.123456, 105.654321, 102.111, 107.222],
                "model_short": ["GBT", "EM", "GBT", "EM"],
                "composition": ["", "GBT, LR_Base", "", "GBT, LR_Base"],
            }
        )

    def test_csv_written(self, monthly_joint_data):
        """CSV file is created at the env-var-configured path."""
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_forecast_data(monthly_joint_data)

        csv_path = os.path.join(str(self.tmp_path), "combined_monthly.csv")
        assert os.path.exists(csv_path)

    def test_latest_csv_written(self, monthly_joint_data):
        """Latest CSV file is also created."""
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_forecast_data(monthly_joint_data)

        latest_path = os.path.join(str(self.tmp_path), "combined_monthly_latest.csv")
        assert os.path.exists(latest_path)

    def test_values_rounded(self, monthly_joint_data):
        """Float values are rounded to 3 decimal places."""
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_forecast_data(monthly_joint_data)

        csv_path = os.path.join(str(self.tmp_path), "combined_monthly.csv")
        saved = pd.read_csv(csv_path)
        row = saved[(saved["model_short"] == "GBT") & (saved["year"] == 2023)]
        assert len(row) == 1
        # 100.123456 -> 100.123
        assert abs(row.iloc[0]["forecasted_discharge"] - 100.123) < 1e-4

    def test_code_cleaned(self):
        """Float codes like 15013.0 are cleaned to '15013'."""
        data = pd.DataFrame(
            {
                "code": [15013.0],
                "year": [2023],
                "month": [6],
                "month_in_year": [6],
                "date": pd.to_datetime(["2023-06-01"]),
                "forecasted_discharge": [100.0],
                "model_short": ["GBT"],
            }
        )
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_monthly_forecast_data(data)

        csv_path = os.path.join(str(self.tmp_path), "combined_monthly.csv")
        saved = pd.read_csv(csv_path, dtype={"code": str})
        assert saved.iloc[0]["code"] == "15013"

    def test_ensemble_api_write_called(self, monthly_joint_data):
        """Ensemble API writer is called from save_monthly_forecast_data."""
        with patch(
            "src.api_writer._write_monthly_ensemble_to_api",
            return_value=True,
        ) as mock_api:
            file_writer.save_monthly_forecast_data(monthly_joint_data)
            mock_api.assert_called_once()
            # Verify it receives the full simulated DataFrame
            call_data = mock_api.call_args[0][0]
            assert len(call_data) >= len(monthly_joint_data)

    def test_csv_still_written_when_api_fails(self, monthly_joint_data):
        """CSV still written even when ensemble API write returns False."""
        with patch(
            "src.api_writer._write_monthly_ensemble_to_api",
            return_value=False,
        ):
            file_writer.save_monthly_forecast_data(monthly_joint_data)

        csv_path = os.path.join(str(self.tmp_path), "combined_monthly.csv")
        assert os.path.exists(csv_path)
        saved = pd.read_csv(csv_path)
        assert len(saved) == 4, f"Expected 4 rows from monthly_joint_data, got {len(saved)}"

    def test_empty_df_handled(self):
        """Empty DataFrame does not crash."""
        data = pd.DataFrame()
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            result = file_writer.save_monthly_forecast_data(data)
        assert result is None


class TestSaveMonthlyForecastDataApiWithoutCsv:
    """Tests that save_monthly_forecast_data writes to API even when CSV
    env vars are not configured.

    Regression tests for PP-032: the early return at line 451 when CSV
    path is missing caused the API write at line 501 to be skipped.
    """

    @pytest.fixture(autouse=True)
    def no_csv_env(self, tmp_path):
        """Deliberately leave CSV env vars unset so CSV path is empty."""
        overrides = {
            "ieasyforecast_intermediate_data_path": "",
            "ieasyforecast_monthly_combined_forecast_file": "",
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_CONSISTENCY_CHECK": "false",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            yield

    @pytest.fixture
    def monthly_ensemble_data(self):
        """Monthly joint forecasts with ensemble rows."""
        return pd.DataFrame(
            {
                "code": ["15013", "15013", "15013"],
                "year": [2024, 2024, 2024],
                "month": [6, 6, 6],
                "month_in_year": [6, 6, 6],
                "date": pd.to_datetime(["2024-06-01"] * 3),
                "forecasted_discharge": [102.5, 101.0, 103.0],
                "model_short": ["EM", "Naive Mean", "Skilled Mean"],
                "composition": ["GBT, LR_Base"] * 3,
            }
        )

    def test_api_write_called_when_csv_not_configured(self, monthly_ensemble_data):
        """PP-032 regression: API write must happen even without CSV path.

        Before the fix, save_monthly_forecast_data() returned early when
        ieasyforecast_intermediate_data_path or
        ieasyforecast_monthly_combined_forecast_file was empty, skipping
        the api_writer._write_monthly_ensemble_to_api() call entirely.
        """
        with patch(
            "src.api_writer._write_monthly_ensemble_to_api",
            return_value=True,
        ) as mock_api:
            file_writer.save_monthly_forecast_data(monthly_ensemble_data)
            mock_api.assert_called_once()

    def test_api_receives_ensemble_data_without_csv(self, monthly_ensemble_data):
        """The DataFrame passed to the API writer contains ensemble rows."""
        with patch(
            "src.api_writer._write_monthly_ensemble_to_api",
            return_value=True,
        ) as mock_api:
            file_writer.save_monthly_forecast_data(monthly_ensemble_data)
            call_data = mock_api.call_args[0][0]
            models = set(call_data["model_short"].unique())
            assert "EM" in models


class TestSaveMonthlyForecastDataApiWithCsv:
    """PP-032: API write must also happen when CSV path IS configured."""

    @pytest.fixture
    def monthly_ensemble_data(self):
        """Monthly joint forecasts with ensemble rows."""
        return pd.DataFrame(
            {
                "code": ["15013", "15013", "15013"],
                "year": [2024, 2024, 2024],
                "month": [6, 6, 6],
                "month_in_year": [6, 6, 6],
                "date": pd.to_datetime(["2024-06-01"] * 3),
                "forecasted_discharge": [102.5, 101.0, 103.0],
                "model_short": ["EM", "Naive Mean", "Skilled Mean"],
                "composition": ["GBT, LR_Base"] * 3,
            }
        )

    def test_api_write_called_when_csv_is_configured(self, monthly_ensemble_data, tmp_path):
        """API write must happen even when CSV path IS configured."""
        overrides = {
            "ieasyforecast_intermediate_data_path": str(tmp_path),
            "ieasyforecast_monthly_combined_forecast_file": "test_monthly.csv",
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_CONSISTENCY_CHECK": "false",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            with patch(
                "src.api_writer._write_monthly_ensemble_to_api",
                return_value=True,
            ) as mock_api:
                file_writer.save_monthly_forecast_data(monthly_ensemble_data)
                mock_api.assert_called_once()


class TestSaveForecastDataAtomicWrites:
    """Tests that save_forecast_data(PENTAD/DECAD, ...) write correct output files."""

    @pytest.fixture(autouse=True)
    def save_env(self, tmp_path):
        """Set env vars so save functions can resolve paths."""
        overrides = {
            "ieasyforecast_intermediate_data_path": str(tmp_path),
            "ieasyforecast_combined_forecast_pentad_file": "combined_pentad.csv",
            "ieasyforecast_combined_forecast_decad_file": "combined_decad.csv",
            "SAPPHIRE_API_ENABLED": "false",
            "SAPPHIRE_CONSISTENCY_CHECK": "false",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            self.tmp_path = tmp_path
            yield

    @pytest.fixture
    def pentad_data(self):
        return pd.DataFrame(
            {
                "code": ["10001", "10001"],
                "date": pd.to_datetime(["2024-01-06", "2024-01-11"]),
                "pentad_in_month": [2, 3],
                "pentad_in_year": [2, 3],
                "forecasted_discharge": [100.0, 110.0],
                "model_short": ["LR", "LR"],
            }
        )

    @pytest.fixture
    def decade_data(self):
        return pd.DataFrame(
            {
                "code": ["10001", "10001"],
                "date": pd.to_datetime(["2024-01-15", "2024-01-25"]),
                "decad_in_month": [2, 3],
                "decad_in_year": [2, 3],
                "forecasted_discharge": [150.0, 160.0],
                "model_short": ["TFT", "TFT"],
            }
        )

    def test_pentad_writes_two_files(self, pentad_data):
        """save_forecast_data(PENTAD, ...) writes combined + latest CSVs."""
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_forecast_data(PENTAD, pentad_data)

        full_csv = self.tmp_path / "combined_pentad.csv"
        latest_csv = self.tmp_path / "combined_pentad_latest.csv"
        assert full_csv.exists(), "combined_pentad.csv not written"
        assert latest_csv.exists(), "combined_pentad_latest.csv not written"

        full = pd.read_csv(full_csv)
        assert len(full) == 2
        assert set(full["model_short"]) == {"LR"}
        assert list(full["forecasted_discharge"]) == [100.0, 110.0]

        latest = pd.read_csv(latest_csv)
        assert len(latest) >= 1
        assert "forecasted_discharge" in latest.columns

    @patch("src.file_writer.atomic_write_csv")
    def test_pentad_atomic_write_failure_raises(self, mock_atomic, pentad_data):
        """IOError from atomic_write_csv propagates."""
        mock_atomic.side_effect = OSError("disk full")
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            with pytest.raises(IOError, match="disk full"):
                file_writer.save_forecast_data(PENTAD, pentad_data)

    def test_decade_writes_two_files(self, decade_data):
        """save_forecast_data(DECAD, ...) writes combined + latest CSVs."""
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_forecast_data(DECAD, decade_data)

        full_csv = self.tmp_path / "combined_decad.csv"
        latest_csv = self.tmp_path / "combined_decad_latest.csv"
        assert full_csv.exists(), "combined_decad.csv not written"
        assert latest_csv.exists(), "combined_decad_latest.csv not written"

        full = pd.read_csv(full_csv)
        assert len(full) == 2
        assert set(full["model_short"]) == {"TFT"}
        assert list(full["forecasted_discharge"]) == [150.0, 160.0]

        latest = pd.read_csv(latest_csv)
        assert len(latest) >= 1
        assert "forecasted_discharge" in latest.columns

    @patch("src.file_writer.atomic_write_csv")
    def test_decade_atomic_write_failure_raises(self, mock_atomic, decade_data):
        """IOError from atomic_write_csv propagates."""
        mock_atomic.side_effect = OSError("disk full")
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            with pytest.raises(IOError, match="disk full"):
                file_writer.save_forecast_data(DECAD, decade_data)


class TestDateStringRoundTrip:
    """Tests that get_latest_forecasts receives datetime dates, not strings."""

    @pytest.fixture(autouse=True)
    def save_env(self, tmp_path):
        overrides = {
            "ieasyforecast_intermediate_data_path": str(tmp_path),
            "ieasyforecast_combined_forecast_pentad_file": "combined_pentad.csv",
            "ieasyforecast_combined_forecast_decad_file": "combined_decad.csv",
            "SAPPHIRE_API_ENABLED": "false",
            "SAPPHIRE_CONSISTENCY_CHECK": "false",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            self.tmp_path = tmp_path
            yield

    def test_get_latest_forecasts_receives_datetime_pentad(self):
        """get_latest_forecasts receives datetime dates, not strings.

        We wrap get_latest_forecasts to capture the date dtype at call
        time (before the caller mutates the DataFrame to string dates).
        """
        captured_dtypes = {}

        original_glf = file_writer.get_latest_forecasts

        def spy_glf(df, **kwargs):
            captured_dtypes["date"] = df["date"].dtype
            return original_glf(df, **kwargs)

        data = pd.DataFrame(
            {
                "code": ["10001", "10001"],
                "date": pd.to_datetime(["2024-01-06", "2024-01-11"]),
                "pentad_in_month": [2, 3],
                "pentad_in_year": [2, 3],
                "forecasted_discharge": [100.0, 110.0],
                "model_short": ["LR", "LR"],
            }
        )
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch("src.file_writer.get_latest_forecasts", side_effect=spy_glf),
        ):
            file_writer.save_forecast_data(PENTAD, data)

        assert pd.api.types.is_datetime64_any_dtype(captured_dtypes["date"])

    def test_get_latest_forecasts_receives_datetime_decade(self):
        """Decade: get_latest_forecasts receives datetime dates."""
        captured_dtypes = {}

        original_glf = file_writer.get_latest_forecasts

        def spy_glf(df, **kwargs):
            captured_dtypes["date"] = df["date"].dtype
            return original_glf(df, **kwargs)

        data = pd.DataFrame(
            {
                "code": ["10001", "10001"],
                "date": pd.to_datetime(["2024-01-15", "2024-01-25"]),
                "decad_in_month": [2, 3],
                "decad_in_year": [2, 3],
                "forecasted_discharge": [150.0, 160.0],
                "model_short": ["TFT", "TFT"],
            }
        )
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch("src.file_writer.get_latest_forecasts", side_effect=spy_glf),
        ):
            file_writer.save_forecast_data(DECAD, data)

        assert pd.api.types.is_datetime64_any_dtype(captured_dtypes["date"])

    def test_csv_output_has_string_dates_pentad(self):
        """Written CSV has %Y-%m-%d formatted date strings."""
        data = pd.DataFrame(
            {
                "code": ["10001"],
                "date": pd.to_datetime(["2024-01-06"]),
                "pentad_in_month": [2],
                "pentad_in_year": [2],
                "forecasted_discharge": [100.0],
                "model_short": ["LR"],
            }
        )
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_forecast_data(PENTAD, data)

        csv_path = os.path.join(str(self.tmp_path), "combined_pentad.csv")
        saved = pd.read_csv(csv_path)
        assert saved.iloc[0]["date"] == "2024-01-06"

    def test_csv_output_has_string_dates_decade(self):
        """Written decade CSV has %Y-%m-%d formatted date strings."""
        data = pd.DataFrame(
            {
                "code": ["10001"],
                "date": pd.to_datetime(["2024-01-15"]),
                "decad_in_month": [2],
                "decad_in_year": [2],
                "forecasted_discharge": [150.0],
                "model_short": ["TFT"],
            }
        )
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False):
            file_writer.save_forecast_data(DECAD, data)

        csv_path = os.path.join(str(self.tmp_path), "combined_decad.csv")
        saved = pd.read_csv(csv_path)
        assert saved.iloc[0]["date"] == "2024-01-15"
