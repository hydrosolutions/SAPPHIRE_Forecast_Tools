"""Tests for src/file_writer.py — atomic CSV writes and save orchestration.

Moved from iEasyHydroForecast/tests/test_forecast_library.py (TestAtomicWriteCSV).
"""

import os
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from conftest import DECAD, PENTAD
from src import api_writer, file_writer


class TestApiWritingEnabledHelper:
    """Tests for api_writer.api_writing_enabled() -- the additive helper
    file_writer.py's pre-gate defaults use to distinguish "operator
    disabled API writing" from "the client is genuinely missing"."""

    def test_defaults_to_enabled(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SAPPHIRE_API_ENABLED", None)
            assert api_writer.api_writing_enabled() is True

    def test_explicit_true(self):
        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}):
            assert api_writer.api_writing_enabled() is True

    def test_explicit_false(self):
        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}):
            assert api_writer.api_writing_enabled() is False

    def test_case_insensitive_false(self):
        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "False"}):
            assert api_writer.api_writing_enabled() is False


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

    def test_empty_dataframe_guarded_no_write(self):
        """Empty DataFrame is guarded: returns True (PP-051 P3, Contract 7 --
        empty input is success, not a null signal) and does NOT touch the CSV.

        Mirrors save_quarterly/seasonal_skill_metrics. A transient empty read
        must not overwrite an existing monthly skill CSV empty (data loss).
        The guard returns before any CSV or API write is attempted.
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
        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch("src.file_writer.atomic_write_csv") as mock_write,
        ):
            result = file_writer.save_monthly_skill_metrics(data)

        assert result is True
        mock_write.assert_not_called()
        # No CSV created by the guarded call.
        assert not os.path.exists(csv_path)

    def test_empty_dataframe_preserves_existing_csv(self):
        """A transient empty frame leaves a previously-written CSV intact."""
        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        # Pre-existing good CSV on disk.
        good = pd.DataFrame(
            {
                "month_in_year": [6],
                "code": ["19999"],
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
        good.to_csv(csv_path, index=False)

        empty = pd.DataFrame(
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
            file_writer.save_monthly_skill_metrics(empty)

        saved = pd.read_csv(csv_path, dtype={"code": str})
        assert len(saved) == 1
        assert saved.iloc[0]["code"] == "19999"

    def test_nonempty_dataframe_still_writes(self):
        """Non-empty frame still writes the CSV (guard does not regress the
        happy path)."""
        data = pd.DataFrame(
            {
                "month_in_year": [1],
                "code": ["19999"],
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
        assert os.path.exists(csv_path)
        saved = pd.read_csv(csv_path, dtype={"code": str})
        assert len(saved) == 1

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
        """CSV is still written to disk when API write raises -- and the
        function still reports the API failure as False (PP-051 P3,
        Contract 2: monthly's conditional CSV write is unaffected by the
        API branch's outcome)."""
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_monthly_skill_metrics(monthly_skill_data)

        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        assert os.path.exists(csv_path)
        saved = pd.read_csv(csv_path, dtype={"code": str})
        assert len(saved) == 6
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_wrote_outcome_returns_true(self, mock_api_write, monthly_skill_data):
        mock_api_write.return_value = api_writer.WriteOutcome.WROTE
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_skipped_by_config_outcome_returns_true(self, mock_api_write, monthly_skill_data):
        """SAPPHIRE_API_ENABLED=false is a documented, benign deployment
        mode -- the writer maps it to SKIPPED_BY_CONFIG internally, below
        the SAPPHIRE_API_AVAILABLE gate. SAPPHIRE_API_AVAILABLE is pinned
        True here so this is distinguishable from the missing-client case
        (test_client_unavailable_returns_false below)."""
        mock_api_write.return_value = api_writer.WriteOutcome.SKIPPED_BY_CONFIG
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_skipped_no_records_outcome_returns_true(self, mock_api_write, monthly_skill_data):
        mock_api_write.return_value = api_writer.WriteOutcome.SKIPPED_NO_RECORDS
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_failed_outcome_without_exception_returns_false(
        self, mock_api_write, monthly_skill_data
    ):
        """Headline case: a directly-returned FAILED (e.g. a readiness-check
        failure) with no exception raised must still surface as a failure --
        the non-raising path a bare `if ret is None:` swallowed pre-fix."""
        mock_api_write.return_value = api_writer.WriteOutcome.FAILED
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_warn_returns_false(self, mock_api_write, monthly_skill_data):
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_ignore_returns_false(self, mock_api_write, monthly_skill_data):
        """`ignore` mode suppresses _handle_api_write_error's logging only --
        never the failure outcome. A raised write error still returns False
        under ignore, same as under warn -- there is no downgrade step."""
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "ignore"}),
        ):
            result = file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_failed_outcome_under_ignore_returns_false(self, mock_api_write, monthly_skill_data):
        """Second half of the no-downgrade proof: a directly-returned FAILED
        (no exception -- e.g. a readiness-check failure) also stays False
        under ignore mode, not just the raised-exception mechanism above."""
        mock_api_write.return_value = api_writer.WriteOutcome.FAILED
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "ignore"}),
        ):
            result = file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_fail_mode_propagates(self, mock_api_write, monthly_skill_data):
        """SAPPHIRE_API_FAILURE_MODE=fail is unchanged: the exception still
        propagates uncaught, unaffected by the new True/False return
        contract (Hard Contract 5)."""
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "fail"}),
        ):
            with pytest.raises(RuntimeError, match="API connection refused"):
                file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2025)

    def test_client_unavailable_returns_false(self, monthly_skill_data):
        """A closed SAPPHIRE_API_AVAILABLE gate (missing sapphire-api-client,
        a required dependency) is a genuine failure, not a benign config
        skip -- the pre-gate default is FAILED, not SKIPPED_BY_CONFIG."""
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write,
        ):
            result = file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2025)
            mock_api_write.assert_not_called()
        assert result is False

    def test_client_unavailable_and_writing_disabled_returns_true(self, monthly_skill_data):
        """Defect fix: SAPPHIRE_API_ENABLED=false is a documented, benign
        deployment mode (doc/configuration.md, and the documented rollback
        path) that must succeed whether or not sapphire-api-client is
        importable. Before this fix, a closed SAPPHIRE_API_AVAILABLE gate
        left the pre-gate default at FAILED even when the operator had
        explicitly disabled API writing -- conflating "dependency missing"
        with "writing disabled". This must return True, not False."""
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}),
            patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write,
        ):
            result = file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2025)
            mock_api_write.assert_not_called()
        assert result is True

    def test_skipped_no_records_via_internal_filtering_returns_true(self):
        """Non-empty input whose every row is excluded by the writer's own
        lead-aware NaN-horizon_value filter -> WriteOutcome.SKIPPED_NO_RECORDS
        internally -> True at this layer (a non-failure per Contract 7).
        Exercises the REAL, unmocked _write_skill_metrics_to_api (only its
        client factory is faked) -- mirrors the fixture pattern in
        test_lead_aware_write_side_dedup.py. `month_in_year` itself stays
        populated (this function's own `.astype(int)` cast would raise on
        NaN there) -- the row-dropping filter this test targets operates on
        the separate `horizon_value` column instead."""
        data = pd.DataFrame(
            {
                "month_in_year": [6, 6],
                "code": ["19999", "19999"],
                "model_short": ["LR", "LR"],
                "horizon_value": [float("nan"), float("nan")],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.9, 0.8],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.85],
                "mae": [2.0, 3.0],
                "n_pairs": [10, 10],
                "crps": [0.5, 0.6],
            }
        )
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_SKILL_LEAD_AWARE": "true"}),
            patch("src.api_writer._get_postprocessing_client", return_value=mock_client),
        ):
            result = file_writer.save_monthly_skill_metrics(data, year=2025)
        mock_client.write_skill_metrics.assert_not_called()
        assert result is True

    def test_csv_unconfigured_warns_and_skips(self, monthly_skill_data):
        """CSV write is conditional (Contract 2, unchanged by this phase):
        when either env var is unset the write warns-and-skips rather than
        raising -- distinct from pentad/decad's unconditional-and-raising
        CSV path (P2), and must not be harmonised with it. The function
        must still succeed via the API branch. SAPPHIRE_CONSISTENCY_CHECK
        stays unset/false here (from the class's save_env fixture) to avoid
        the separately-filed PP-053 UnboundLocalError hazard: `filepath` is
        only bound inside `if csv_dir and csv_file:` yet is referenced
        unconditionally by the SAPPHIRE_CONSISTENCY_CHECK branch below it."""
        overrides = {
            "ieasyforecast_intermediate_data_path": "",
            "ieasyforecast_monthly_skill_metrics_file": "",
        }
        with (
            patch.dict(os.environ, overrides),
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch(
                "src.api_writer._write_skill_metrics_to_api",
                return_value=api_writer.WriteOutcome.WROTE,
            ),
        ):
            result = file_writer.save_monthly_skill_metrics(monthly_skill_data, year=2025)

        assert result is True
        csv_path = os.path.join(str(self.tmp_path), "skill_monthly.csv")
        assert not os.path.exists(csv_path)


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


# ===========================================================================
# PP-051 P1 — save_quarterly_skill_metrics / save_seasonal_skill_metrics
#
# Both functions are API-only (no CSV fallback), so a swallowed write
# failure here is total loss of that recalc's output for the horizon.
# Contract 7 (api_writer.WriteOutcome) mapping under test: FAILED -> False,
# every other outcome (WROTE, SKIPPED_BY_CONFIG, SKIPPED_NO_RECORDS) ->
# True. All assertions are exact identity (`is True` / `is False`) --
# truthiness is vacuous against the pre-fix bare `None` return.
#
# P0a correction (pp051_p0a_client_absent_mapping_fix.md, landed after the
# parent plan document): a closed SAPPHIRE_API_AVAILABLE gate (missing
# sapphire-api-client) is a genuine FAILED, not SKIPPED_BY_CONFIG -- the
# pre-gate default below is `WriteOutcome.FAILED`, not SKIPPED_BY_CONFIG as
# the parent plan's Contract 7 text still literally reads. P0a's D3 also
# removes the ignore-mode downgrade step Contract 7 originally specified:
# `ignore` suppresses _handle_api_write_error's logging only, never the
# outcome -- a FAILED stays FAILED under both `warn` and `ignore`; only
# `fail` changes behavior (re-raises). Tests below assert that directly.
#
# Out-of-loop-review defect fix (post-P0a): the FAILED pre-gate default
# above only holds when the operator has API writing enabled. When
# SAPPHIRE_API_ENABLED=false, the pre-gate default is SKIPPED_BY_CONFIG
# instead, REGARDLESS of whether the client is importable -- a disabled
# gate that never opens must not be reported as a failure just because a
# dependency happens to also be missing. See
# test_client_unavailable_and_writing_disabled_returns_true below.
# ===========================================================================


class TestSaveQuarterlySkillMetrics:
    """Tests for save_quarterly_skill_metrics() -- API-only, no CSV fallback."""

    @pytest.fixture(autouse=True)
    def save_env(self):
        overrides = {
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            yield

    @pytest.fixture
    def quarterly_skill_data(self):
        return pd.DataFrame(
            {
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR", "EM"],
                "sdivsigma": [0.3, 0.25],
                "nse": [0.9, 0.92],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.91],
                "mae": [2.0, 1.8],
                "n_pairs": [10, 10],
                "crps": [0.5, np.nan],
            }
        )

    def test_empty_dataframe_returns_true(self):
        """Top-level empty guard: True (not None), writer never called."""
        with patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write:
            result = file_writer.save_quarterly_skill_metrics(pd.DataFrame(), year=2025)
        mock_api_write.assert_not_called()
        assert result is True

    def test_none_input_returns_true(self):
        with patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write:
            result = file_writer.save_quarterly_skill_metrics(None, year=2025)
        mock_api_write.assert_not_called()
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_wrote_outcome_returns_true(self, mock_api_write, quarterly_skill_data):
        mock_api_write.return_value = api_writer.WriteOutcome.WROTE
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_quarterly_skill_metrics(quarterly_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_skipped_by_config_outcome_returns_true(self, mock_api_write, quarterly_skill_data):
        """SAPPHIRE_API_ENABLED=false is a documented, benign deployment
        mode -- the writer maps it to SKIPPED_BY_CONFIG internally, below
        the SAPPHIRE_API_AVAILABLE gate. SAPPHIRE_API_AVAILABLE is pinned
        True here so this is distinguishable from the missing-client case
        (test_client_unavailable_returns_false below), per the P0a
        addendum's test guidance."""
        mock_api_write.return_value = api_writer.WriteOutcome.SKIPPED_BY_CONFIG
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_quarterly_skill_metrics(quarterly_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_skipped_no_records_outcome_returns_true(self, mock_api_write, quarterly_skill_data):
        mock_api_write.return_value = api_writer.WriteOutcome.SKIPPED_NO_RECORDS
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_quarterly_skill_metrics(quarterly_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_failed_outcome_without_exception_returns_false(
        self, mock_api_write, quarterly_skill_data
    ):
        """Headline case: a directly-returned FAILED (e.g. a readiness-check
        failure) with no exception raised must still surface as a failure --
        the non-raising path a bare `if ret is None:` swallowed pre-fix."""
        mock_api_write.return_value = api_writer.WriteOutcome.FAILED
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_quarterly_skill_metrics(quarterly_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_warn_returns_false(self, mock_api_write, quarterly_skill_data):
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_quarterly_skill_metrics(quarterly_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_ignore_returns_false(self, mock_api_write, quarterly_skill_data):
        """`ignore` mode suppresses _handle_api_write_error's logging only --
        never the failure outcome (P0a D3). A raised write error still
        returns False under ignore, same as under warn -- there is no
        downgrade step."""
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "ignore"}),
        ):
            result = file_writer.save_quarterly_skill_metrics(quarterly_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_failed_outcome_under_ignore_returns_false(self, mock_api_write, quarterly_skill_data):
        """Second half of the no-downgrade proof: a directly-returned FAILED
        (no exception -- e.g. a readiness-check failure) also stays False
        under ignore mode, not just the raised-exception mechanism above."""
        mock_api_write.return_value = api_writer.WriteOutcome.FAILED
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "ignore"}),
        ):
            result = file_writer.save_quarterly_skill_metrics(quarterly_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_fail_mode_propagates(self, mock_api_write, quarterly_skill_data):
        """SAPPHIRE_API_FAILURE_MODE=fail is unchanged: the exception still
        propagates uncaught, unaffected by the new True/False return
        contract (Hard Contract 5)."""
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "fail"}),
        ):
            with pytest.raises(RuntimeError, match="API connection refused"):
                file_writer.save_quarterly_skill_metrics(quarterly_skill_data, year=2025)

    def test_client_unavailable_returns_false(self, quarterly_skill_data):
        """A closed SAPPHIRE_API_AVAILABLE gate (missing sapphire-api-client,
        a required dependency) is a genuine failure, not a benign config
        skip -- P0a correction: the pre-gate default is FAILED, not
        SKIPPED_BY_CONFIG."""
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write,
        ):
            result = file_writer.save_quarterly_skill_metrics(quarterly_skill_data, year=2025)
            mock_api_write.assert_not_called()
        assert result is False

    def test_client_unavailable_and_writing_disabled_returns_true(self, quarterly_skill_data):
        """Defect fix: SAPPHIRE_API_ENABLED=false is a documented, benign
        deployment mode (doc/configuration.md, and the documented rollback
        path) that must succeed whether or not sapphire-api-client is
        importable. Before this fix, a closed SAPPHIRE_API_AVAILABLE gate
        left the pre-gate default at FAILED even when the operator had
        explicitly disabled API writing -- conflating "dependency missing"
        with "writing disabled". This must return True, not False."""
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}),
            patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write,
        ):
            result = file_writer.save_quarterly_skill_metrics(quarterly_skill_data, year=2025)
            mock_api_write.assert_not_called()
        assert result is True

    def test_skipped_no_records_via_internal_filtering_returns_true(self):
        """Non-empty input whose every row is excluded by the writer's own
        lead-aware NaN-horizon_value filter -> WriteOutcome.SKIPPED_NO_RECORDS
        internally -> True at this layer (a non-failure per Contract 7, not
        the False a pre-P0a reading might suggest). Exercises the REAL,
        unmocked _write_skill_metrics_to_api (only its client factory is
        faked) -- mirrors the fixture pattern in
        test_lead_aware_write_side_dedup.py."""
        data = pd.DataFrame(
            {
                "quarter_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR", "LR"],
                "horizon_value": [float("nan"), float("nan")],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.9, 0.8],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.85],
                "mae": [2.0, 3.0],
                "n_pairs": [10, 10],
                "crps": [0.5, 0.6],
            }
        )
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_SKILL_LEAD_AWARE": "true"}),
            patch("src.api_writer._get_postprocessing_client", return_value=mock_client),
        ):
            result = file_writer.save_quarterly_skill_metrics(data, year=2025)
        mock_client.write_skill_metrics.assert_not_called()
        assert result is True


class TestSaveSeasonalSkillMetrics:
    """Tests for save_seasonal_skill_metrics() -- API-only, no CSV fallback."""

    @pytest.fixture(autouse=True)
    def save_env(self):
        overrides = {
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            yield

    @pytest.fixture
    def seasonal_skill_data(self):
        return pd.DataFrame(
            {
                "season_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR", "EM"],
                "sdivsigma": [0.3, 0.25],
                "nse": [0.9, 0.92],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.91],
                "mae": [2.0, 1.8],
                "n_pairs": [10, 10],
                "crps": [0.5, np.nan],
            }
        )

    def test_empty_dataframe_returns_true(self):
        """Top-level empty guard: True (not None), writer never called."""
        with patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write:
            result = file_writer.save_seasonal_skill_metrics(pd.DataFrame(), year=2025)
        mock_api_write.assert_not_called()
        assert result is True

    def test_none_input_returns_true(self):
        with patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write:
            result = file_writer.save_seasonal_skill_metrics(None, year=2025)
        mock_api_write.assert_not_called()
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_wrote_outcome_returns_true(self, mock_api_write, seasonal_skill_data):
        mock_api_write.return_value = api_writer.WriteOutcome.WROTE
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_seasonal_skill_metrics(seasonal_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_skipped_by_config_outcome_returns_true(self, mock_api_write, seasonal_skill_data):
        """SAPPHIRE_API_ENABLED=false is a documented, benign deployment
        mode -- the writer maps it to SKIPPED_BY_CONFIG internally, below
        the SAPPHIRE_API_AVAILABLE gate. SAPPHIRE_API_AVAILABLE is pinned
        True here so this is distinguishable from the missing-client case
        (test_client_unavailable_returns_false below), per the P0a
        addendum's test guidance."""
        mock_api_write.return_value = api_writer.WriteOutcome.SKIPPED_BY_CONFIG
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_seasonal_skill_metrics(seasonal_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_skipped_no_records_outcome_returns_true(self, mock_api_write, seasonal_skill_data):
        mock_api_write.return_value = api_writer.WriteOutcome.SKIPPED_NO_RECORDS
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_seasonal_skill_metrics(seasonal_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_failed_outcome_without_exception_returns_false(
        self, mock_api_write, seasonal_skill_data
    ):
        """Headline case: a directly-returned FAILED (e.g. a readiness-check
        failure) with no exception raised must still surface as a failure --
        the non-raising path a bare `if ret is None:` swallowed pre-fix."""
        mock_api_write.return_value = api_writer.WriteOutcome.FAILED
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_seasonal_skill_metrics(seasonal_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_warn_returns_false(self, mock_api_write, seasonal_skill_data):
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_seasonal_skill_metrics(seasonal_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_ignore_returns_false(self, mock_api_write, seasonal_skill_data):
        """`ignore` mode suppresses _handle_api_write_error's logging only --
        never the failure outcome (P0a D3). A raised write error still
        returns False under ignore, same as under warn -- there is no
        downgrade step."""
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "ignore"}),
        ):
            result = file_writer.save_seasonal_skill_metrics(seasonal_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_failed_outcome_under_ignore_returns_false(self, mock_api_write, seasonal_skill_data):
        """Second half of the no-downgrade proof: a directly-returned FAILED
        (no exception -- e.g. a readiness-check failure) also stays False
        under ignore mode, not just the raised-exception mechanism above."""
        mock_api_write.return_value = api_writer.WriteOutcome.FAILED
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "ignore"}),
        ):
            result = file_writer.save_seasonal_skill_metrics(seasonal_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_fail_mode_propagates(self, mock_api_write, seasonal_skill_data):
        """SAPPHIRE_API_FAILURE_MODE=fail is unchanged: the exception still
        propagates uncaught, unaffected by the new True/False return
        contract (Hard Contract 5)."""
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "fail"}),
        ):
            with pytest.raises(RuntimeError, match="API connection refused"):
                file_writer.save_seasonal_skill_metrics(seasonal_skill_data, year=2025)

    def test_client_unavailable_returns_false(self, seasonal_skill_data):
        """A closed SAPPHIRE_API_AVAILABLE gate (missing sapphire-api-client,
        a required dependency) is a genuine failure, not a benign config
        skip -- P0a correction: the pre-gate default is FAILED, not
        SKIPPED_BY_CONFIG."""
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write,
        ):
            result = file_writer.save_seasonal_skill_metrics(seasonal_skill_data, year=2025)
            mock_api_write.assert_not_called()
        assert result is False

    def test_client_unavailable_and_writing_disabled_returns_true(self, seasonal_skill_data):
        """Defect fix: SAPPHIRE_API_ENABLED=false is a documented, benign
        deployment mode (doc/configuration.md, and the documented rollback
        path) that must succeed whether or not sapphire-api-client is
        importable. Before this fix, a closed SAPPHIRE_API_AVAILABLE gate
        left the pre-gate default at FAILED even when the operator had
        explicitly disabled API writing -- conflating "dependency missing"
        with "writing disabled". This must return True, not False."""
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}),
            patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write,
        ):
            result = file_writer.save_seasonal_skill_metrics(seasonal_skill_data, year=2025)
            mock_api_write.assert_not_called()
        assert result is True

    def test_skipped_no_records_via_internal_filtering_returns_true(self):
        """Non-empty input whose every row is excluded by the writer's own
        lead-aware NaN-horizon_value filter -> WriteOutcome.SKIPPED_NO_RECORDS
        internally -> True at this layer (a non-failure per Contract 7, not
        the False a pre-P0a reading might suggest). Exercises the REAL,
        unmocked _write_skill_metrics_to_api (only its client factory is
        faked) -- mirrors the fixture pattern in
        test_lead_aware_write_side_dedup.py."""
        data = pd.DataFrame(
            {
                "season_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR", "LR"],
                "horizon_value": [float("nan"), float("nan")],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.9, 0.8],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.85],
                "mae": [2.0, 3.0],
                "n_pairs": [10, 10],
                "crps": [0.5, 0.6],
            }
        )
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_SKILL_LEAD_AWARE": "true"}),
            patch("src.api_writer._get_postprocessing_client", return_value=mock_client),
        ):
            result = file_writer.save_seasonal_skill_metrics(data, year=2025)
        mock_client.write_skill_metrics.assert_not_called()
        assert result is True


# ===========================================================================
# PP-051 P2 — save_skill_metrics (pentad/decad)
#
# Unlike its quarterly/seasonal siblings (P1) this function has NO
# top-level empty-input guard (PP-051 plan §1b) and its CSV write is
# unconditional and already raises on its own failure -- that CSV
# behavior is unchanged by this phase (Contract 2). Contract 7 mapping
# under test: FAILED -> False, every other outcome (WROTE,
# SKIPPED_BY_CONFIG, SKIPPED_NO_RECORDS) -> True. All assertions are
# exact identity (`is True` / `is False`) -- truthiness is vacuous
# against the pre-fix bare `None` return.
#
# Pre-gate default is WriteOutcome.FAILED (P0a correction): a closed
# SAPPHIRE_API_AVAILABLE gate means the required sapphire-api-client
# dependency is missing -- a genuine failure, not a configuration
# choice. SAPPHIRE_API_ENABLED=false is handled *inside* the writer,
# below this gate, and maps to SKIPPED_BY_CONFIG there. `ignore` mode
# never downgrades the outcome (P0a/D3) -- it only suppresses
# _handle_api_write_error's logging, not failure accounting.
#
# Out-of-loop-review defect fix (post-P0a): the FAILED pre-gate default
# above only holds when the operator has API writing enabled. When
# SAPPHIRE_API_ENABLED=false, the pre-gate default is SKIPPED_BY_CONFIG
# instead, REGARDLESS of whether the client is importable -- a disabled
# gate that never opens must not be reported as a failure just because a
# dependency happens to also be missing. See
# test_client_unavailable_and_writing_disabled_returns_true below.
# ===========================================================================


class TestSaveSkillMetrics:
    """Tests for save_skill_metrics() -- pentad/decad, CSV + API."""

    @pytest.fixture(autouse=True)
    def save_env(self, tmp_path):
        """Set env vars so save_skill_metrics can resolve paths."""
        overrides = {
            "ieasyforecast_intermediate_data_path": str(tmp_path),
            "ieasyforecast_pentadal_skill_metrics_file": "skill_pentad.csv",
            "SAPPHIRE_API_ENABLED": "true",
            "SAPPHIRE_CONSISTENCY_CHECK": "false",
            "SAPPHIRE_TEST_ENV": "True",
        }
        with patch.dict(os.environ, overrides):
            self.tmp_path = tmp_path
            yield tmp_path

    @pytest.fixture
    def pentad_skill_data(self):
        return pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR", "EM"],
                "sdivsigma": [0.3, 0.25],
                "nse": [0.9, 0.92],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.91],
                "mae": [2.0, 1.8],
                "n_pairs": [10, 10],
                "crps": [0.5, np.nan],
            }
        )

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_wrote_outcome_returns_true(self, mock_api_write, pentad_skill_data):
        mock_api_write.return_value = api_writer.WriteOutcome.WROTE
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_skill_metrics(PENTAD, pentad_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_skipped_by_config_outcome_returns_true(self, mock_api_write, pentad_skill_data):
        """SAPPHIRE_API_ENABLED=false is a documented, benign deployment
        mode -- the writer maps it to SKIPPED_BY_CONFIG internally, below
        the SAPPHIRE_API_AVAILABLE gate. SAPPHIRE_API_AVAILABLE is pinned
        True here so this is distinguishable from the missing-client case
        (test_client_unavailable_returns_false below)."""
        mock_api_write.return_value = api_writer.WriteOutcome.SKIPPED_BY_CONFIG
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_skill_metrics(PENTAD, pentad_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_skipped_no_records_outcome_returns_true(self, mock_api_write, pentad_skill_data):
        mock_api_write.return_value = api_writer.WriteOutcome.SKIPPED_NO_RECORDS
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_skill_metrics(PENTAD, pentad_skill_data, year=2025)
        assert result is True

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_failed_outcome_without_exception_returns_false(
        self, mock_api_write, pentad_skill_data
    ):
        """Headline case: a directly-returned FAILED (e.g. a readiness-check
        failure) with no exception raised must still surface as a failure --
        the non-raising path a bare `if ret is None:` swallowed pre-fix."""
        mock_api_write.return_value = api_writer.WriteOutcome.FAILED
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_skill_metrics(PENTAD, pentad_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_warn_returns_false(self, mock_api_write, pentad_skill_data):
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True):
            result = file_writer.save_skill_metrics(PENTAD, pentad_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_ignore_returns_false(self, mock_api_write, pentad_skill_data):
        """`ignore` mode suppresses _handle_api_write_error's logging only --
        never the failure outcome. A raised write error still returns False
        under ignore, same as under warn -- there is no downgrade step."""
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "ignore"}),
        ):
            result = file_writer.save_skill_metrics(PENTAD, pentad_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_failed_outcome_under_ignore_returns_false(self, mock_api_write, pentad_skill_data):
        """Second half of the no-downgrade proof: a directly-returned FAILED
        (no exception -- e.g. a readiness-check failure) also stays False
        under ignore mode, not just the raised-exception mechanism above."""
        mock_api_write.return_value = api_writer.WriteOutcome.FAILED
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "ignore"}),
        ):
            result = file_writer.save_skill_metrics(PENTAD, pentad_skill_data, year=2025)
        assert result is False

    @patch("src.api_writer._write_skill_metrics_to_api")
    def test_exception_under_fail_mode_propagates(self, mock_api_write, pentad_skill_data):
        """SAPPHIRE_API_FAILURE_MODE=fail is unchanged: the exception still
        propagates uncaught, unaffected by the new True/False return
        contract (Hard Contract 5)."""
        mock_api_write.side_effect = RuntimeError("API connection refused")
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_API_FAILURE_MODE": "fail"}),
        ):
            with pytest.raises(RuntimeError, match="API connection refused"):
                file_writer.save_skill_metrics(PENTAD, pentad_skill_data, year=2025)

    def test_client_unavailable_returns_false(self, pentad_skill_data):
        """A closed SAPPHIRE_API_AVAILABLE gate (missing sapphire-api-client,
        a required dependency) is a genuine failure, not a benign config
        skip -- the pre-gate default is FAILED, not SKIPPED_BY_CONFIG."""
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write,
        ):
            result = file_writer.save_skill_metrics(PENTAD, pentad_skill_data, year=2025)
            mock_api_write.assert_not_called()
        assert result is False

    def test_client_unavailable_and_writing_disabled_returns_true(self, pentad_skill_data):
        """Defect fix: SAPPHIRE_API_ENABLED=false is a documented, benign
        deployment mode (doc/configuration.md, and the documented rollback
        path) that must succeed whether or not sapphire-api-client is
        importable. Before this fix, a closed SAPPHIRE_API_AVAILABLE gate
        left the pre-gate default at FAILED even when the operator had
        explicitly disabled API writing -- conflating "dependency missing"
        with "writing disabled". This must return True, not False."""
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", False),
            patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "false"}),
            patch("src.api_writer._write_skill_metrics_to_api") as mock_api_write,
        ):
            result = file_writer.save_skill_metrics(PENTAD, pentad_skill_data, year=2025)
            mock_api_write.assert_not_called()
        assert result is True

    def test_skipped_no_records_via_internal_filtering_returns_true(self):
        """Non-empty input whose every row is excluded by the writer's own
        lead-aware NaN-horizon_value filter -> WriteOutcome.SKIPPED_NO_RECORDS
        internally -> True at this layer (this outcome is a non-failure per
        Contract 7). Exercises the REAL, unmocked _write_skill_metrics_to_api
        (only its client factory is faked) -- mirrors the fixture pattern in
        test_lead_aware_write_side_dedup.py. `pentad_in_year` itself stays
        populated (this function's own `.astype(int)` cast at :319 would
        raise on NaN there) -- the row-dropping filter this test targets
        operates on the separate `horizon_value` column instead."""
        data = pd.DataFrame(
            {
                "pentad_in_year": [1, 1],
                "code": ["19999", "19999"],
                "model_short": ["LR", "LR"],
                "horizon_value": [float("nan"), float("nan")],
                "sdivsigma": [0.3, 0.4],
                "nse": [0.9, 0.8],
                "delta": [5.0, 5.0],
                "accuracy": [0.9, 0.85],
                "mae": [2.0, 3.0],
                "n_pairs": [10, 10],
                "crps": [0.5, 0.6],
            }
        )
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch.dict(os.environ, {"SAPPHIRE_SKILL_LEAD_AWARE": "true"}),
            patch("src.api_writer._get_postprocessing_client", return_value=mock_client),
        ):
            result = file_writer.save_skill_metrics(PENTAD, data, year=2025)
        mock_client.write_skill_metrics.assert_not_called()
        assert result is True

    def test_csv_still_written_when_api_write_fails(self, pentad_skill_data):
        """CSV write is unconditional (Contract 2, unchanged by this phase):
        the CSV must still be written -- and the function must still report
        the API failure as False -- when the API branch's outcome is FAILED
        under warn mode."""
        with (
            patch.object(api_writer, "SAPPHIRE_API_AVAILABLE", True),
            patch(
                "src.api_writer._write_skill_metrics_to_api",
                side_effect=RuntimeError("API connection refused"),
            ),
        ):
            result = file_writer.save_skill_metrics(PENTAD, pentad_skill_data, year=2025)

        csv_path = os.path.join(str(self.tmp_path), "skill_pentad.csv")
        assert os.path.exists(csv_path), "CSV should be written even when API fails"
        saved = pd.read_csv(csv_path)
        assert len(saved) == 2
        assert result is False
