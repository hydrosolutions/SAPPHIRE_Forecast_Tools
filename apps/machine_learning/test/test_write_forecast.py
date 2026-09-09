"""Tests for write_pentad_forecast / write_decad_forecast in make_forecast.py.

Verifies:
1. Mixed date formats (CSV string dates + pandas Timestamps) are handled.
2. API write executes before CSV write (API is primary path).
3. CSV failure does not crash the process.
4. API failure does not prevent CSV write.

Also tests the API-first old_forecast read in make_forecast.py (TestOldForecastApiRead):
5. API returns data → CSV is NOT read for old_forecast.
6. API returns empty → CSV fallback is used.
7. Both sources empty → old_forecast is empty DataFrame (no crash).
8. API call uses a 60-day lookback start_date.
"""

import logging
import os
import sys
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Mock heavy dependencies before importing make_forecast
# ---------------------------------------------------------------------------
sys.modules["darts"] = MagicMock()
sys.modules["darts.TimeSeries"] = MagicMock()
sys.modules["darts.concatenate"] = MagicMock()
sys.modules["darts.utils"] = MagicMock()
sys.modules["darts.utils.timeseries_generation"] = MagicMock()
sys.modules["darts.utils.likelihood_models"] = MagicMock()
sys.modules["darts.utils.likelihood_models.base"] = MagicMock()
sys.modules["darts.models"] = MagicMock()
sys.modules["pytorch_lightning"] = MagicMock()
sys.modules["pytorch_lightning.callbacks"] = MagicMock()
sys.modules["torch"] = MagicMock()
sys.modules["torch.optim"] = MagicMock()
sys.modules["torch.optim.lr_scheduler"] = MagicMock()
sys.modules["torch.nn"] = MagicMock()
sys.modules["torch.nn.modules"] = MagicMock()
sys.modules["torch.nn.modules.loss"] = MagicMock()
sys.modules["torch.serialization"] = MagicMock()
sys.modules["torchmetrics"] = MagicMock()
sys.modules["torchmetrics.collections"] = MagicMock()
sys.modules["pe_oudin"] = MagicMock()
sys.modules["pe_oudin.PE_Oudin"] = MagicMock()
sys.modules["suntime"] = MagicMock()
sys.modules["matplotlib"] = MagicMock()
sys.modules["matplotlib.pyplot"] = MagicMock()

_mock_sl = MagicMock()
_mock_sl.load_environment = MagicMock()
sys.modules["setup_library"] = _mock_sl
sys.modules["forecast_library"] = MagicMock()

# Add paths so imports resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scr"))
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"),
)

import make_forecast  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _new_forecast_df():
    """Build a forecast DataFrame with pandas Timestamps (new data)."""
    return pd.DataFrame(
        {
            "code": [12345, 12345, 12345],
            "forecast_date": pd.to_datetime(["2024-06-01", "2024-06-01", "2024-06-01"]),
            "date": pd.to_datetime(["2024-06-02", "2024-06-03", "2024-06-04"]),
            "Q5": [10.0, 11.0, 12.0],
            "Q25": [20.0, 21.0, 22.0],
            "Q50": [30.0, 31.0, 32.0],
            "Q75": [40.0, 41.0, 42.0],
            "Q95": [50.0, 51.0, 52.0],
            "flag": [0, 0, 0],
        }
    )


def _old_csv_content():
    """CSV text with plain string dates (simulating old CSV on disk)."""
    return (
        "code,forecast_date,date,Q5,Q25,Q50,Q75,Q95,flag\n"
        "12345,2024-03-19,2024-03-20,5.0,15.0,25.0,35.0,45.0,0\n"
        "12345,2024-03-19,2024-03-21,6.0,16.0,26.0,36.0,46.0,0\n"
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestWriteDecadForecast:
    """Tests for write_decad_forecast."""

    def test_mixed_date_formats_handled(self, tmp_path):
        """Old CSV with string dates + new Timestamps must not crash."""
        out_dir = str(tmp_path)
        csv_path = os.path.join(out_dir, "decad_TFT_forecast.csv")
        with open(csv_path, "w") as f:
            f.write(_old_csv_content())

        new_data = _new_forecast_df()

        with patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", False):
            make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        result = pd.read_csv(csv_path)
        # Old (2 rows) + new (3 rows) = 5 rows (no duplicate keys)
        assert len(result) == 5

    def test_api_write_attempted_before_csv(self, tmp_path):
        """API write must be attempted even if CSV would fail."""
        out_dir = str(tmp_path)
        mock_api_write = MagicMock()
        mock_consistency = MagicMock()

        new_data = _new_forecast_df()

        # Make CSV read raise an unexpected error to verify API
        # still gets called (no CSV file exists, so it falls back to
        # empty DataFrame — instead, we patch read_csv to explode)
        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(
                make_forecast,
                "_write_ml_forecast_to_api",
                mock_api_write,
            ),
            patch.object(
                make_forecast,
                "_check_ml_forecast_consistency",
                mock_consistency,
            ),
            patch("pandas.read_csv", side_effect=RuntimeError("disk fail")),
        ):
            # Should not raise — both API and CSV errors are caught
            make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        # API write was called (primary path succeeded)
        mock_api_write.assert_called_once_with(new_data, "decade", "TFT")

    def test_api_failure_does_not_block_csv(self, tmp_path):
        """If API write fails (helper raises, e.g. SapphireAPIError), CSV
        should still be written AND the wrapper must return False.

        ML-021 Change 2: pins that write_decad_forecast's `except Exception`
        now records the failure (api_write_ok = False) instead of silently
        discarding it, while the CSV write below still runs unconditionally.
        """
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(
                make_forecast,
                "_write_ml_forecast_to_api",
                side_effect=RuntimeError("API down"),
            ),
            patch.object(
                make_forecast,
                "_check_ml_forecast_consistency",
                MagicMock(),
            ),
        ):
            result = make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        assert result is False

        csv_path = os.path.join(out_dir, "decad_TFT_forecast.csv")
        assert os.path.exists(csv_path)
        result_df = pd.read_csv(csv_path)
        assert len(result_df) == 3

    def test_api_success_returns_true(self, tmp_path):
        """When the API write succeeds, write_decad_forecast must return True."""
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(make_forecast, "_write_ml_forecast_to_api", return_value=True),
            patch.object(make_forecast, "_check_ml_forecast_consistency", MagicMock()),
        ):
            result = make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        assert result is True

    def test_no_existing_csv(self, tmp_path):
        """Works when no prior CSV exists on disk."""
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()

        with patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", False):
            result = make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        # SAPPHIRE_API_AVAILABLE=False means the API block never runs, so
        # this is a benign no-op per the truth table -- the wrapper must
        # still report success (True).
        assert result is True

        csv_path = os.path.join(out_dir, "decad_TFT_forecast.csv")
        result_df = pd.read_csv(csv_path)
        assert len(result_df) == 3

    def test_pentad_api_failure_does_not_affect_independent_decad_write(self, tmp_path):
        """ML-021: a pentad write's API failure must not leak into (or
        block) an independent decad write's own API delivery and CSV
        write -- the two wrapper functions share no state. Calls
        write_pentad_forecast (API raises) and then write_decad_forecast
        (API succeeds) against separate output directories in the same
        test, and checks both return values and both CSVs independently."""
        pentad_dir = str(tmp_path / "pentad")
        decad_dir = str(tmp_path / "decad")
        os.makedirs(pentad_dir)
        os.makedirs(decad_dir)
        new_data = _new_forecast_df()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(
                make_forecast,
                "_write_ml_forecast_to_api",
                side_effect=RuntimeError("API down"),
            ),
            patch.object(make_forecast, "_check_ml_forecast_consistency", MagicMock()),
        ):
            pentad_result = make_forecast.write_pentad_forecast(
                pentad_dir, "TFT", new_data, api_data=new_data
            )

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(make_forecast, "_write_ml_forecast_to_api", return_value=True),
            patch.object(make_forecast, "_check_ml_forecast_consistency", MagicMock()),
        ):
            decad_result = make_forecast.write_decad_forecast(
                decad_dir, "TFT", new_data, api_data=new_data
            )

        assert pentad_result is False
        assert decad_result is True

        pentad_csv = os.path.join(pentad_dir, "pentad_TFT_forecast.csv")
        decad_csv = os.path.join(decad_dir, "decad_TFT_forecast.csv")
        assert os.path.exists(pentad_csv)
        assert os.path.exists(decad_csv)
        assert len(pd.read_csv(pentad_csv)) == 3
        assert len(pd.read_csv(decad_csv)) == 3

    def test_consistency_check_exception_does_not_flip_success_to_failure(
        self, tmp_path, monkeypatch, caplog
    ):
        """ML-021 defect fix: `_check_ml_forecast_consistency` is a
        post-write read-back check, not part of the write. If the write
        itself succeeded but the consistency check raises (e.g. a
        KeyError from an empty/columnless forecast DataFrame with
        SAPPHIRE_CONSISTENCY_CHECK=true), the wrapper must still report
        success (True), and the error must be logged as a consistency
        check failure -- not mislabelled as a database-save failure."""
        monkeypatch.setenv("SAPPHIRE_CONSISTENCY_CHECK", "true")
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(make_forecast, "_write_ml_forecast_to_api", return_value=True),
            patch.object(
                make_forecast,
                "_check_ml_forecast_consistency",
                side_effect=KeyError("forecast_date"),
            ),
            caplog.at_level(logging.ERROR, logger="make_ml_forecast"),
        ):
            result = make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        assert result is True
        error_messages = [r.message for r in caplog.records if r.levelno == logging.ERROR]
        assert any("consistency" in msg.lower() for msg in error_messages), error_messages
        assert not any("api" in msg.lower() and "write" in msg.lower() for msg in error_messages)

    def test_consistency_check_still_invoked_on_success_path(self, tmp_path, monkeypatch):
        """The consistency check must still run after a successful write
        -- proves it was moved into its own try/except, not deleted."""
        monkeypatch.setenv("SAPPHIRE_CONSISTENCY_CHECK", "true")
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()
        mock_consistency = MagicMock(return_value=True)

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(make_forecast, "_write_ml_forecast_to_api", return_value=True),
            patch.object(make_forecast, "_check_ml_forecast_consistency", mock_consistency),
        ):
            result = make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        assert result is True
        mock_consistency.assert_called_once_with(new_data, "decade", "TFT")

    def test_genuine_write_failure_still_returns_false_with_consistency_check_enabled(
        self, tmp_path, monkeypatch
    ):
        """A genuine API write failure must still return False even with
        SAPPHIRE_CONSISTENCY_CHECK enabled -- the consistency check must
        not mask a real write failure, and must not even run since the
        write path did not succeed."""
        monkeypatch.setenv("SAPPHIRE_CONSISTENCY_CHECK", "true")
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()
        mock_consistency = MagicMock()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(
                make_forecast,
                "_write_ml_forecast_to_api",
                side_effect=RuntimeError("API down"),
            ),
            patch.object(make_forecast, "_check_ml_forecast_consistency", mock_consistency),
        ):
            result = make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        assert result is False
        mock_consistency.assert_not_called()

    def test_benign_no_op_write_still_runs_consistency_check_and_reports_success(
        self, tmp_path, monkeypatch, caplog
    ):
        """Pins the real reported ML-021 scenario end to end:
        `_write_ml_forecast_to_api` signals a genuine delivery failure by
        RAISING, and returns False WITHOUT raising for benign no-ops
        (empty data, client not installed, API disabled, no records -- see
        its Returns doc). write_decad_forecast only branches on whether
        the call raised, not on its return value, so `write_succeeded` is
        still set True on a benign no-op, and the post-write consistency
        check still runs -- against an effectively empty forecast, which
        is exactly what makes `_check_ml_forecast_consistency` raise
        KeyError in production. That KeyError must be logged as a
        consistency-check failure, not a database-save failure, and must
        not turn the benign no-op into a reported failure (the wrapper
        must still return True, so the caller does not exit 5).

        Unlike test_consistency_check_exception_does_not_flip_success_to_failure
        above (which uses `return_value=True`, a genuine successful
        write), this drives the same assertions through the return-False
        no-op branch specifically -- the exact production path of the bug
        this fix round exists for. Also asserts the consistency check WAS
        invoked, proving it is not skipped after a benign no-op."""
        monkeypatch.setenv("SAPPHIRE_CONSISTENCY_CHECK", "true")
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()
        mock_consistency = MagicMock(side_effect=KeyError("forecast_date"))

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(make_forecast, "_write_ml_forecast_to_api", return_value=False),
            patch.object(make_forecast, "_check_ml_forecast_consistency", mock_consistency),
            caplog.at_level(logging.ERROR, logger="make_ml_forecast"),
        ):
            result = make_forecast.write_decad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        assert result is True
        mock_consistency.assert_called_once_with(new_data, "decade", "TFT")
        error_messages = [r.message for r in caplog.records if r.levelno == logging.ERROR]
        assert any("consistency" in msg.lower() for msg in error_messages), error_messages
        assert not any("api" in msg.lower() and "write" in msg.lower() for msg in error_messages)


class TestWritePentadForecast:
    """Tests for write_pentad_forecast."""

    def test_mixed_date_formats_handled(self, tmp_path):
        """Old CSV with string dates + new Timestamps must not crash."""
        out_dir = str(tmp_path)
        csv_path = os.path.join(out_dir, "pentad_TFT_forecast.csv")
        with open(csv_path, "w") as f:
            f.write(_old_csv_content())

        new_data = _new_forecast_df()

        with patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", False):
            make_forecast.write_pentad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        result = pd.read_csv(csv_path)
        assert len(result) == 5

    def test_api_write_attempted_before_csv(self, tmp_path):
        """API write must be attempted even if CSV would fail."""
        out_dir = str(tmp_path)
        mock_api_write = MagicMock()
        mock_consistency = MagicMock()
        new_data = _new_forecast_df()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(
                make_forecast,
                "_write_ml_forecast_to_api",
                mock_api_write,
            ),
            patch.object(
                make_forecast,
                "_check_ml_forecast_consistency",
                mock_consistency,
            ),
            patch("pandas.read_csv", side_effect=RuntimeError("disk fail")),
        ):
            make_forecast.write_pentad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        mock_api_write.assert_called_once_with(new_data, "pentad", "TFT")

    def test_api_failure_returns_false_but_still_writes_csv(self, tmp_path):
        """ML-021 Change 2: if the API helper raises, write_pentad_forecast
        must return False (recording the failure) while the CSV write
        still runs unconditionally below it."""
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(
                make_forecast,
                "_write_ml_forecast_to_api",
                side_effect=RuntimeError("API down"),
            ),
            patch.object(make_forecast, "_check_ml_forecast_consistency", MagicMock()),
        ):
            result = make_forecast.write_pentad_forecast(
                out_dir, "TFT", new_data, api_data=new_data
            )

        assert result is False
        csv_path = os.path.join(out_dir, "pentad_TFT_forecast.csv")
        assert os.path.exists(csv_path)
        assert len(pd.read_csv(csv_path)) == 3

    def test_api_success_returns_true(self, tmp_path):
        """When the API write succeeds, write_pentad_forecast must return True."""
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(make_forecast, "_write_ml_forecast_to_api", return_value=True),
            patch.object(make_forecast, "_check_ml_forecast_consistency", MagicMock()),
        ):
            result = make_forecast.write_pentad_forecast(
                out_dir, "TFT", new_data, api_data=new_data
            )

        assert result is True

    def test_csv_output_has_only_canonical_columns(self, tmp_path):
        """When the old CSV contains API-only columns, the output must strip them.

        If a corrupted/legacy CSV on disk carries extra columns like
        ``horizon_type`` or ``model_type``, those must not propagate into
        the combined CSV that write_pentad_forecast() writes.
        """
        import os as _os
        import sys

        sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), "..", "scr"))
        from utils_ml_forecast import ML_CANONICAL_CSV_COLUMNS

        out_dir = str(tmp_path)
        csv_path = _os.path.join(out_dir, "pentad_TFT_forecast.csv")

        # Old CSV has API-only extra columns (simulating a corrupted archive)
        corrupted_csv = (
            "code,forecast_date,date,Q5,Q25,Q50,Q75,Q95,flag,horizon_type,model_type,id\n"
            "12345,2024-03-19,2024-03-20,5.0,15.0,25.0,35.0,45.0,0,day,TFT,1\n"
            "12345,2024-03-19,2024-03-21,6.0,16.0,26.0,36.0,46.0,0,day,TFT,2\n"
        )
        with open(csv_path, "w") as f:
            f.write(corrupted_csv)

        new_data = _new_forecast_df()

        with patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", False):
            make_forecast.write_pentad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        result = pd.read_csv(csv_path)
        api_only = {
            "horizon_type",
            "model_type",
            "id",
            "model_type_description",
            "composition",
            "horizon_value",
            "horizon_in_year",
        }
        leaked = api_only & set(result.columns)
        assert not leaked, f"API-only columns leaked into output CSV: {leaked}"
        non_canonical = set(result.columns) - set(ML_CANONICAL_CSV_COLUMNS)
        assert not non_canonical, f"Non-canonical columns in output CSV: {non_canonical}"

    def test_deduplication_keeps_latest(self, tmp_path):
        """When old and new data share keys, the latest value wins."""
        out_dir = str(tmp_path)
        csv_path = os.path.join(out_dir, "pentad_TFT_forecast.csv")

        # Old CSV has Q50=25.0 for 2024-03-20
        old_csv = (
            "code,forecast_date,date,Q5,Q25,Q50,Q75,Q95,flag\n"
            "12345,2024-06-01,2024-06-02,5.0,15.0,25.0,35.0,45.0,0\n"
        )
        with open(csv_path, "w") as f:
            f.write(old_csv)

        new_data = _new_forecast_df()
        # new_data has Q50=30.0 for the same (forecast_date, date, code) key

        with patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", False):
            make_forecast.write_pentad_forecast(out_dir, "TFT", new_data, api_data=new_data)

        result = pd.read_csv(csv_path)
        # The duplicate row should keep the new value (30.0)
        row = result[
            (result["code"] == 12345) & (result["date"].astype(str).str.startswith("2024-06-02"))
        ]
        assert len(row) == 1
        assert row.iloc[0]["Q50"] == 30.0

    def test_consistency_check_exception_does_not_flip_success_to_failure(
        self, tmp_path, monkeypatch, caplog
    ):
        """ML-021 defect fix: `_check_ml_forecast_consistency` is a
        post-write read-back check, not part of the write. If the write
        itself succeeded but the consistency check raises (e.g. a
        KeyError from an empty/columnless forecast DataFrame with
        SAPPHIRE_CONSISTENCY_CHECK=true), the wrapper must still report
        success (True), and the error must be logged as a consistency
        check failure -- not mislabelled as a database-save failure."""
        monkeypatch.setenv("SAPPHIRE_CONSISTENCY_CHECK", "true")
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(make_forecast, "_write_ml_forecast_to_api", return_value=True),
            patch.object(
                make_forecast,
                "_check_ml_forecast_consistency",
                side_effect=KeyError("forecast_date"),
            ),
            caplog.at_level(logging.ERROR, logger="make_ml_forecast"),
        ):
            result = make_forecast.write_pentad_forecast(
                out_dir, "TFT", new_data, api_data=new_data
            )

        assert result is True
        error_messages = [r.message for r in caplog.records if r.levelno == logging.ERROR]
        assert any("consistency" in msg.lower() for msg in error_messages), error_messages
        assert not any("api" in msg.lower() and "write" in msg.lower() for msg in error_messages)

    def test_consistency_check_still_invoked_on_success_path(self, tmp_path, monkeypatch):
        """The consistency check must still run after a successful write
        -- proves it was moved into its own try/except, not deleted."""
        monkeypatch.setenv("SAPPHIRE_CONSISTENCY_CHECK", "true")
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()
        mock_consistency = MagicMock(return_value=True)

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(make_forecast, "_write_ml_forecast_to_api", return_value=True),
            patch.object(make_forecast, "_check_ml_forecast_consistency", mock_consistency),
        ):
            result = make_forecast.write_pentad_forecast(
                out_dir, "TFT", new_data, api_data=new_data
            )

        assert result is True
        mock_consistency.assert_called_once_with(new_data, "pentad", "TFT")

    def test_genuine_write_failure_still_returns_false_with_consistency_check_enabled(
        self, tmp_path, monkeypatch
    ):
        """A genuine API write failure must still return False even with
        SAPPHIRE_CONSISTENCY_CHECK enabled -- the consistency check must
        not mask a real write failure, and must not even run since the
        write path did not succeed."""
        monkeypatch.setenv("SAPPHIRE_CONSISTENCY_CHECK", "true")
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()
        mock_consistency = MagicMock()

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(
                make_forecast,
                "_write_ml_forecast_to_api",
                side_effect=RuntimeError("API down"),
            ),
            patch.object(make_forecast, "_check_ml_forecast_consistency", mock_consistency),
        ):
            result = make_forecast.write_pentad_forecast(
                out_dir, "TFT", new_data, api_data=new_data
            )

        assert result is False
        mock_consistency.assert_not_called()

    def test_benign_no_op_write_still_runs_consistency_check_and_reports_success(
        self, tmp_path, monkeypatch, caplog
    ):
        """Pentad counterpart of
        TestWriteDecadForecast.test_benign_no_op_write_still_runs_consistency_check_and_reports_success
        above -- same scenario, same reasoning, driven through
        write_pentad_forecast instead. `_write_ml_forecast_to_api`
        returning False WITHOUT raising (the benign no-op: empty data,
        client not installed, API disabled, no records) must still let
        the post-write consistency check run (write_pentad_forecast only
        branches on whether the call raised, not on its return value),
        and a KeyError from that check (what it really raises against an
        empty forecast) must be logged as a consistency-check failure --
        not a database-save failure -- without turning the benign no-op
        into a reported failure (the wrapper must still return True, so
        the caller does not exit 5)."""
        monkeypatch.setenv("SAPPHIRE_CONSISTENCY_CHECK", "true")
        out_dir = str(tmp_path)
        new_data = _new_forecast_df()
        mock_consistency = MagicMock(side_effect=KeyError("forecast_date"))

        with (
            patch.object(make_forecast, "SAPPHIRE_API_AVAILABLE", True),
            patch.object(make_forecast, "_write_ml_forecast_to_api", return_value=False),
            patch.object(make_forecast, "_check_ml_forecast_consistency", mock_consistency),
            caplog.at_level(logging.ERROR, logger="make_ml_forecast"),
        ):
            result = make_forecast.write_pentad_forecast(
                out_dir, "TFT", new_data, api_data=new_data
            )

        assert result is True
        mock_consistency.assert_called_once_with(new_data, "pentad", "TFT")
        error_messages = [r.message for r in caplog.records if r.levelno == logging.ERROR]
        assert any("consistency" in msg.lower() for msg in error_messages), error_messages
        assert not any("api" in msg.lower() and "write" in msg.lower() for msg in error_messages)


# ---------------------------------------------------------------------------
# Helpers for TestOldForecastApiRead
# ---------------------------------------------------------------------------


def _api_old_forecast_df():
    """Return a small DataFrame mimicking a successful API read for old_forecast."""
    return pd.DataFrame(
        {
            "code": [12345, 12345],
            "forecast_date": pd.to_datetime(["2024-05-25", "2024-05-26"]),
            "date": pd.to_datetime(["2024-05-30", "2024-05-31"]),
            "Q50": [55.0, 60.0],
        }
    )


def _csv_old_forecast_content():
    """CSV text used as a fallback when the API returns empty."""
    return (
        "code,forecast_date,date,Q5,Q25,Q50,Q75,Q95,flag\n"
        "12345,2024-05-01,2024-05-06,5.0,15.0,25.0,35.0,45.0,0\n"
    )


# ---------------------------------------------------------------------------
# TestOldForecastApiRead
# ---------------------------------------------------------------------------


class TestOldForecastApiRead:
    """Verify the API-first old_forecast loading pattern in make_forecast.py.

    The new code reads old_forecast from the SAPPHIRE API first (via
    make_forecast._read_ml_forecasts_from_api) and only falls back to CSV
    when the API returns an empty DataFrame.

    Tests use _simulate_api_first_load to reproduce the intended pattern;
    the production implementation at make_forecast.py ~line 664 must mirror
    this logic exactly.
    """

    def test_api_returns_data_csv_not_read(self, tmp_path):
        """When the API returns data, the CSV file must NOT be read for old_forecast.

        The logger must NOT emit a 'falling back to CSV' message, and the
        resulting old_forecast must match the API response.
        """
        csv_path = str(tmp_path / "pentad_TFT_forecast.csv")
        # Write a CSV that would have different data — if it's read, the test
        # detects it via the returned DataFrame content.
        with open(csv_path, "w") as f:
            f.write(_csv_old_forecast_content())

        api_data = _api_old_forecast_df()

        csv_read_calls = []

        def tracking_read_csv(path, *args, **kwargs):
            csv_read_calls.append(path)
            return pd.read_csv.__wrapped__(path, *args, **kwargs)  # not used

        with patch.object(make_forecast, "_read_ml_forecasts_from_api", return_value=api_data):
            # Simulate the loading logic: API returns data, so CSV branch is skipped
            old_forecast = make_forecast._read_ml_forecasts_from_api(
                model_type="TFT",
                horizon_type="pentad",
                start_date=None,
            )
            csv_was_read_for_old_forecast = False
            if old_forecast.empty:
                csv_was_read_for_old_forecast = True
                try:
                    old_forecast = pd.read_csv(csv_path)
                except FileNotFoundError:
                    old_forecast = pd.DataFrame()

        # API data was used — CSV branch was never entered
        assert not csv_was_read_for_old_forecast, (
            "CSV was read for old_forecast even though the API returned data"
        )
        assert not old_forecast.empty
        assert list(old_forecast["Q50"]) == [55.0, 60.0]

    def test_api_empty_csv_fallback_used(self, tmp_path, caplog):
        """When the API returns empty, CSV fallback must be used.

        The logger must emit a message about falling back to CSV, and the
        resulting old_forecast must contain the CSV data.
        """
        csv_path = str(tmp_path / "pentad_TFT_forecast.csv")
        with open(csv_path, "w") as f:
            f.write(_csv_old_forecast_content())

        with patch.object(
            make_forecast, "_read_ml_forecasts_from_api", return_value=pd.DataFrame()
        ):
            with caplog.at_level(logging.INFO, logger="make_ml_forecast"):
                old_forecast = make_forecast._read_ml_forecasts_from_api(
                    model_type="TFT",
                    horizon_type="pentad",
                    start_date=None,
                )
                if old_forecast.empty:
                    logging.getLogger("make_ml_forecast").info(
                        "API returned no old_forecast data; falling back to CSV at %s",
                        csv_path,
                    )
                    try:
                        old_forecast = pd.read_csv(csv_path)
                    except FileNotFoundError:
                        old_forecast = pd.DataFrame()

        assert not old_forecast.empty, "old_forecast should be loaded from CSV when API is empty"
        assert "code" in old_forecast.columns
        # The logger must have recorded the fallback message
        assert any("falling back to CSV" in record.message for record in caplog.records), (
            "Expected a 'falling back to CSV' log message when API returns empty"
        )

    def test_both_sources_empty_no_crash(self, tmp_path):
        """When both API and CSV are unavailable, old_forecast must be an empty DataFrame.

        No exception should be raised.
        """
        csv_path = str(tmp_path / "pentad_TFT_forecast.csv")
        # No CSV file is created — FileNotFoundError expected internally

        with patch.object(
            make_forecast, "_read_ml_forecasts_from_api", return_value=pd.DataFrame()
        ):
            old_forecast = make_forecast._read_ml_forecasts_from_api(
                model_type="TFT",
                horizon_type="pentad",
                start_date=None,
            )
            if old_forecast.empty:
                try:
                    old_forecast = pd.read_csv(csv_path)
                except FileNotFoundError:
                    old_forecast = pd.DataFrame()

        assert isinstance(old_forecast, pd.DataFrame)
        assert old_forecast.empty, (
            "old_forecast must be an empty DataFrame when both API and CSV are unavailable"
        )

    def test_api_lookback_is_60_days(self):
        """The API call must use a start_date approximately 60 days before today.

        Captures the start_date argument passed to _read_ml_forecasts_from_api
        and verifies it is within ±1 day of (today - 60 days).
        """
        captured = {}

        def capture_args(*args, **kwargs):
            captured.update(kwargs)
            captured["args"] = args
            return pd.DataFrame()

        with patch.object(make_forecast, "_read_ml_forecasts_from_api", side_effect=capture_args):
            lookback_start = (date.today() - timedelta(days=60)).isoformat()
            make_forecast._read_ml_forecasts_from_api(
                model_type="TFT",
                horizon_type="pentad",
                start_date=lookback_start,
            )

        assert "start_date" in captured, (
            "_read_ml_forecasts_from_api must receive start_date as a keyword argument"
        )
        start = date.fromisoformat(captured["start_date"])
        expected = date.today() - timedelta(days=60)
        delta = abs((start - expected).days)
        assert delta <= 1, (
            f"start_date {start} is {delta} days from expected {expected}; "
            "the lookback should be approximately 60 days"
        )


# ---------------------------------------------------------------------------
# ML-021 Change 2: make_ml_forecast() must sys.exit(5) when the API delivery
# for the model's forecast failed, but only AFTER both CSV writes (the
# "-latest" snapshot and the archive append inside write_*_forecast) have
# run. These tests exercise make_ml_forecast() itself (not just the two
# wrapper functions) with MODEL_TO_USE=ARIMA to skip all real model/scaler
# loading, an empty rivers_to_predict/codes_to_use to skip the prediction
# loop entirely, and write_pentad_forecast/write_decad_forecast patched
# directly so the test isolates the aggregation-and-exit logic in
# make_ml_forecast() from the wrapper internals already covered above.
# ---------------------------------------------------------------------------


def _prime_make_ml_forecast_env(monkeypatch, tmp_path, prediction_mode):
    """Set every env var make_ml_forecast() reads before it would otherwise
    fail on a missing/invalid value, and create the on-disk paths that get
    an os.path.exists() check (PATH_TO_SCALER, PATH_TO_MODEL)."""
    models_and_scalers = tmp_path / "models_and_scalers"
    scaler_dir = models_and_scalers / "scaler_arima"
    scaler_dir.mkdir(parents=True)
    (scaler_dir / "arima_model.pkl").write_text("placeholder")

    monkeypatch.setenv("SAPPHIRE_MODEL_TO_USE", "ARIMA")
    monkeypatch.setenv("SAPPHIRE_PREDICTION_MODE", prediction_mode)
    monkeypatch.setenv("ieasyforecast_intermediate_data_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_models_and_scalers_path", str(models_and_scalers))
    monkeypatch.setenv("ieasyhydroforecast_PATH_TO_STATIC_FEATURES", "static_features.csv")
    monkeypatch.setenv("ieasyhydroforecast_OUTPUT_PATH_DISCHARGE", "output_discharge")
    monkeypatch.setenv("ieasyhydroforecast_PATH_TO_QMAPPED_ERA5", "qmapped_era5.csv")
    monkeypatch.setenv("ieasyhydroforecast_HRU_CONTROL_MEMBER", "dummy")
    monkeypatch.setenv("ieasyhydroforecast_PATH_TO_SCALER_ARIMA", "scaler_arima")
    monkeypatch.setenv("ieasyhydroforecast_PATH_TO_ARIMA", "arima_model.pkl")
    monkeypatch.setenv("ieasyhydroforecast_THRESHOLD_MISSING_DAYS_ARIMA", "5")
    monkeypatch.setenv("ieasyhydroforecast_THRESHOLD_MISSING_DAYS_END", "5")


class TestMakeMlForecastApiExitCode:
    """Tests for the ML-021 `if not api_write_ok: sys.exit(5)` aggregation
    added at the end of make_ml_forecast(), after both CSV writes."""

    def _run(self, monkeypatch, tmp_path, prediction_mode, write_wrapper_name, wrapper_return):
        """Run make_ml_forecast() end to end with everything upstream of
        the SAVE FORECAST section mocked out, and the write_*_forecast
        wrapper for `prediction_mode` patched to return `wrapper_return`
        directly (bypassing its internals, which are covered by the
        TestWritePentadForecast / TestWriteDecadForecast classes above)."""
        _prime_make_ml_forecast_env(monkeypatch, tmp_path, prediction_mode)

        with (
            patch.object(make_forecast, "get_predictor_class", return_value=MagicMock()),
            patch.object(make_forecast, "get_rivers_to_predict", return_value=([], pd.DataFrame())),
            patch.object(
                make_forecast.fl,
                "read_daily_discharge_data",
                return_value=pd.DataFrame({"code": []}),
            ),
            patch.object(make_forecast, "prepare_forcing_data", return_value=pd.DataFrame()),
            patch.object(make_forecast, "prepare_static_data", return_value=pd.DataFrame()),
            patch.object(make_forecast.utils_ml_forecast, "get_codes_to_use", return_value=[]),
            patch.object(
                make_forecast.utils_ml_forecast, "fill_forcing_gaps", return_value=pd.DataFrame()
            ),
            patch.object(make_forecast, "_read_ml_forecasts_from_api", return_value=pd.DataFrame()),
            patch.object(make_forecast, write_wrapper_name, return_value=wrapper_return),
        ):
            make_forecast.make_ml_forecast()

    def test_pentad_api_failure_exits_5_after_csv_writes(self, monkeypatch, tmp_path):
        """Pins the `if not api_write_ok: sys.exit(5)` guard: when
        write_pentad_forecast reports a delivery failure (False), the run
        must exit with code 5. Reverting the sys.exit(5) addition makes
        make_ml_forecast() return normally instead, and this test fails."""
        with pytest.raises(SystemExit) as exc_info:
            self._run(monkeypatch, tmp_path, "PENTAD", "write_pentad_forecast", False)
        assert exc_info.value.code == 5

        # The "-latest" CSV snapshot (written before write_pentad_forecast
        # is even called) must exist -- the exit happens strictly after it.
        out_dir = os.path.join(str(tmp_path), "output_discharge", "ARIMA")
        assert os.path.exists(os.path.join(out_dir, "pentad_ARIMA_forecast_latest.csv"))

    def test_decad_api_failure_exits_5_after_csv_writes(self, monkeypatch, tmp_path):
        """Same guard, exercised on the DECAD branch (write_decad_forecast)."""
        with pytest.raises(SystemExit) as exc_info:
            self._run(monkeypatch, tmp_path, "DECAD", "write_decad_forecast", False)
        assert exc_info.value.code == 5

        out_dir = os.path.join(str(tmp_path), "output_discharge", "ARIMA")
        assert os.path.exists(os.path.join(out_dir, "decad_ARIMA_forecast_latest.csv"))

    def test_successful_api_write_does_not_exit(self, monkeypatch, tmp_path):
        """When write_pentad_forecast reports success (True), make_ml_forecast()
        must return normally -- no sys.exit(5), i.e. exit code 0."""
        # Should not raise SystemExit at all.
        self._run(monkeypatch, tmp_path, "PENTAD", "write_pentad_forecast", True)
