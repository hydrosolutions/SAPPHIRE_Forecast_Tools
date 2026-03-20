"""Integration tests for multi-org read/write isolation (INFRA-012 Phase 3).

Verifies that:
  - Reader functions with codes= return only the requested stations (per-code
    API loop correctly scopes data).
  - Reader functions with codes=None return all stations (no over-filtering).
  - The write guard catches cross-org contamination in combined forecasts and
    skill metrics writes, and does NOT warn for single-org clean writes.
  - End-to-end: reading with correct scoping then writing produces no guard
    warnings; reading without scoping and writing triggers the guard.

PP-025 is fully implemented; all tests are expected to pass.
"""

import json
import logging
import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Adjust sys.path is handled by conftest.py
from src import api_writer, data_reader

# ---------------------------------------------------------------------------
# Test constants
# ---------------------------------------------------------------------------

DEMO_CODES = ["99001", "99002", "99003"]
OTHER_CODES = ["88001", "88002", "88003"]
ALL_CODES = DEMO_CODES + OTHER_CODES


# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------


def _make_skill_metrics_df(codes: list[str], n_pentads: int = 3) -> pd.DataFrame:
    """Build a synthetic skill metrics DataFrame for the given station codes.

    Args:
        codes: Station codes to include.
        n_pentads: Number of pentad rows per code.

    Returns:
        DataFrame with columns required by _write_skill_metrics_to_api.
    """
    rows = []
    for code in codes:
        for pentad in range(1, n_pentads + 1):
            rows.append(
                {
                    "code": code,
                    "pentad_in_year": pentad,
                    "model_short": "LR",
                    "n_pairs": 10,
                    "nse": 0.7,
                    "accuracy": 0.8,
                    "sdivsigma": 0.5,
                    "delta": 0.1,
                    "mae": 2.0,
                }
            )
    return pd.DataFrame(rows)


def _make_combined_forecast_df(codes: list[str], date_str: str = "2026-01-06") -> pd.DataFrame:
    """Build a synthetic combined forecast DataFrame for the given station codes.

    Args:
        codes: Station codes to include.
        date_str: Forecast date string (boundary day).

    Returns:
        DataFrame with columns required by _write_combined_forecast_to_api.
    """
    rows = []
    for code in codes:
        for model in ["TFT", "EM", "NE"]:
            rows.append(
                {
                    "code": code,
                    "date": date_str,
                    "pentad_in_month": 1,
                    "pentad_in_year": 1,
                    "forecasted_discharge": 42.0,
                    "model_short": model,
                }
            )
    return pd.DataFrame(rows)


def _make_monthly_forecast_api_df(codes: list[str]) -> pd.DataFrame:
    """Build a synthetic API-format monthly forecast DataFrame.

    The _normalize_monthly_forecasts() function expects valid_from and
    model_type columns (as returned by the API, not the internal format).

    Args:
        codes: Station codes to include.

    Returns:
        DataFrame in API response format (pre-normalization).
    """
    rows = []
    for code in codes:
        rows.append(
            {
                "code": code,
                "valid_from": "2026-01-01",
                "valid_to": "2026-01-31",
                "model_type": "LR_Base",
                "horizon_type": "month",
                "horizon_value": 1,
                "q": 55.0,
                "q50": 55.0,
                "flag": 0,
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def multi_org_env(tmp_path):
    """Set up environment for demo org with station selection config.

    Writes a config_station_selection.json containing only DEMO_CODES,
    patches environment variables to point at it, resets api_writer
    singletons before and after, so each test sees a clean write guard.

    Args:
        tmp_path: pytest-provided temporary directory.

    Yields:
        tmp_path (for tests that need to write extra files).
    """
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    selection = {"stationsID": DEMO_CODES}
    (config_dir / "config_station_selection.json").write_text(json.dumps(selection))

    env_vars = {
        "ieasyforecast_configuration_path": str(config_dir),
        "ieasyforecast_config_file_station_selection": "config_station_selection.json",
        "ieasyforecast_config_file_station_selection_decad": "",
        "ieasyhydroforecast_organization": "demo",
        "SAPPHIRE_API_ENABLED": "true",
        "SAPPHIRE_API_URL": "http://localhost:8000",
    }
    with patch.dict(os.environ, env_vars):
        # Reset singletons so they pick up new env variables
        api_writer._reset_api_client()
        yield tmp_path
        api_writer._reset_api_client()


# ---------------------------------------------------------------------------
# Helper: build a side_effect function for read_skill_metrics mock
# ---------------------------------------------------------------------------


def _make_skill_metrics_side_effect(all_df: pd.DataFrame):
    """Return a side_effect callable that filters by the 'code' kwarg.

    Simulates server-side filtering: the API returns only rows that match
    the requested code when code= is supplied, or all rows when omitted.

    Args:
        all_df: Complete DataFrame to filter from.

    Returns:
        Callable suitable for use as mock side_effect.
    """

    def _side_effect(**kwargs):
        code = kwargs.get("code")
        if code is not None:
            subset = all_df[all_df["code"] == str(code)].copy()
            # API returns horizon_in_year / model_type column names
            if "pentad_in_year" in subset.columns:
                subset = subset.rename(
                    columns={"pentad_in_year": "horizon_in_year", "model_short": "model_type"}
                )
            return subset if not subset.empty else pd.DataFrame()
        # No code filter — return all rows in API format
        ret = all_df.copy()
        if "pentad_in_year" in ret.columns:
            ret = ret.rename(
                columns={"pentad_in_year": "horizon_in_year", "model_short": "model_type"}
            )
        return ret

    return _side_effect


# ===========================================================================
# Read isolation tests
# ===========================================================================


class TestReadIsolation:
    """Verify that reader functions honour the codes= parameter."""

    @patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.data_reader.SapphirePostprocessingClient")
    def test_read_skill_metrics_with_codes_filters_correctly(self, MockClient):
        """Requesting DEMO_CODES returns only those stations.

        The mock simulates server-side filtering: when code= is passed, only
        matching rows are returned.  The reader must concatenate them and
        never mix in OTHER_CODES.
        """
        mock_client = MockClient.return_value
        mock_client.readiness_check.return_value = True

        all_data = _make_skill_metrics_df(ALL_CODES)
        mock_client.read_skill_metrics.side_effect = _make_skill_metrics_side_effect(all_data)

        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}):
            result = data_reader.read_skill_metrics("pentad", codes=DEMO_CODES)

        assert not result.empty, "Expected non-empty result for DEMO_CODES"
        assert set(result["code"].unique()) == set(DEMO_CODES)
        assert not any(c in result["code"].values for c in OTHER_CODES)

    @patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.data_reader.SapphirePostprocessingClient")
    def test_read_combined_forecasts_with_codes_filters_correctly(self, MockClient):
        """Requesting DEMO_CODES combined forecasts returns only those stations.

        Uses read_short_term_forecasts mock with server-side filtering by code.
        """
        mock_client = MockClient.return_value
        mock_client.readiness_check.return_value = True

        all_data = _make_combined_forecast_df(ALL_CODES)

        def _forecast_side_effect(**kwargs):
            code = kwargs.get("code")
            if code is not None:
                subset = all_data[all_data["code"] == str(code)].copy()
                if "pentad_in_year" in subset.columns:
                    subset = subset.rename(
                        columns={
                            "pentad_in_year": "horizon_in_year",
                            "pentad_in_month": "horizon_value",
                            "model_short": "model_type",
                        }
                    )
                return subset if not subset.empty else pd.DataFrame()
            ret = all_data.copy()
            if "pentad_in_year" in ret.columns:
                ret = ret.rename(
                    columns={
                        "pentad_in_year": "horizon_in_year",
                        "pentad_in_month": "horizon_value",
                        "model_short": "model_type",
                    }
                )
            return ret

        mock_client.read_short_term_forecasts.side_effect = _forecast_side_effect

        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}):
            result = data_reader.read_combined_forecasts("pentad", codes=DEMO_CODES)

        assert not result.empty, "Expected non-empty result for DEMO_CODES"
        assert set(result["code"].unique()) == set(DEMO_CODES)
        assert not any(c in result["code"].values for c in OTHER_CODES)

    @patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.data_reader.SapphirePostprocessingClient")
    def test_read_monthly_combined_with_codes_filters_correctly(self, MockClient):
        """Requesting monthly combined forecasts with codes= returns only those stations.

        Uses read_long_term_forecasts mock with server-side filtering by code.
        The mock returns data in API format (with valid_from, model_type) so
        the normalizer can process it correctly.
        """
        mock_client = MockClient.return_value
        mock_client.readiness_check.return_value = True

        all_data = _make_monthly_forecast_api_df(ALL_CODES)

        def _lt_forecast_side_effect(**kwargs):
            code = kwargs.get("code")
            if code is not None:
                subset = all_data[all_data["code"] == str(code)].copy()
                return subset if not subset.empty else pd.DataFrame()
            return all_data.copy()

        mock_client.read_long_term_forecasts.side_effect = _lt_forecast_side_effect

        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}):
            result = data_reader.read_monthly_combined_forecasts(codes=DEMO_CODES)

        assert not result.empty, "Expected non-empty result for DEMO_CODES"
        assert set(result["code"].unique()) == set(DEMO_CODES)
        assert not any(c in result["code"].values for c in OTHER_CODES)

    @patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.data_reader.SapphirePostprocessingClient")
    def test_read_skill_metrics_without_codes_returns_all(self, MockClient):
        """Calling read_skill_metrics with codes=None returns all stations.

        The 'no code filter' path must not silently drop any stations.
        """
        mock_client = MockClient.return_value
        mock_client.readiness_check.return_value = True

        all_data = _make_skill_metrics_df(ALL_CODES)

        def _all_skill_metrics(**kwargs):
            # No code kwarg — return everything in API format
            ret = all_data.copy()
            ret = ret.rename(
                columns={"pentad_in_year": "horizon_in_year", "model_short": "model_type"}
            )
            return ret

        mock_client.read_skill_metrics.side_effect = _all_skill_metrics

        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}):
            result = data_reader.read_skill_metrics("pentad", codes=None)

        assert not result.empty, "Expected non-empty result for codes=None"
        assert set(result["code"].unique()) == set(ALL_CODES)

    @patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.data_reader.SapphirePostprocessingClient")
    def test_read_combined_forecasts_without_codes_returns_all(self, MockClient):
        """Calling read_combined_forecasts with codes=None returns all stations.

        The unrestricted path must not silently filter any stations.
        """
        mock_client = MockClient.return_value
        mock_client.readiness_check.return_value = True

        all_data = _make_combined_forecast_df(ALL_CODES)

        def _all_forecasts(**kwargs):
            ret = all_data.copy()
            ret = ret.rename(
                columns={
                    "pentad_in_year": "horizon_in_year",
                    "pentad_in_month": "horizon_value",
                    "model_short": "model_type",
                }
            )
            return ret

        mock_client.read_short_term_forecasts.side_effect = _all_forecasts

        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}):
            result = data_reader.read_combined_forecasts("pentad", codes=None)

        assert not result.empty, "Expected non-empty result for codes=None"
        assert set(result["code"].unique()) == set(ALL_CODES)


# ===========================================================================
# Write guard tests
# ===========================================================================


class TestWriteGuard:
    """Verify that the write guard correctly detects cross-org contamination."""

    @patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.api_writer._get_postprocessing_client")
    def test_write_guard_catches_cross_org_combined_forecast(
        self, mock_get_client, multi_org_env, caplog
    ):
        """Writing combined forecasts with OTHER_CODES triggers WRITE GUARD warning.

        The write guard inspects batch_codes against the configured station
        selection (DEMO_CODES only).  Sending ALL_CODES must produce a warning
        that names the unexpected OTHER_CODES.
        """
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = len(ALL_CODES)
        mock_get_client.return_value = mock_client

        data = _make_combined_forecast_df(ALL_CODES)

        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            api_writer._write_combined_forecast_to_api(data, "pentad")

        assert "WRITE GUARD" in caplog.text, (
            "Expected 'WRITE GUARD' warning when OTHER_CODES are present in the batch"
        )
        # At least one of the unexpected codes should appear in the log
        assert any(code in caplog.text for code in OTHER_CODES)

    @patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.api_writer._get_postprocessing_client")
    def test_write_guard_passes_single_org_combined_forecast(
        self, mock_get_client, multi_org_env, caplog
    ):
        """Writing combined forecasts with only DEMO_CODES does NOT warn.

        When the batch is fully within the configured station selection,
        the write guard must remain silent.
        """
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_forecasts.return_value = len(DEMO_CODES)
        mock_get_client.return_value = mock_client

        data = _make_combined_forecast_df(DEMO_CODES)

        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            api_writer._write_combined_forecast_to_api(data, "pentad")

        assert "WRITE GUARD" not in caplog.text, (
            "No WRITE GUARD warning expected when batch is within configured codes"
        )

    @patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.api_writer._get_postprocessing_client")
    def test_write_guard_catches_cross_org_skill_metrics(
        self, mock_get_client, multi_org_env, caplog
    ):
        """Writing skill metrics with OTHER_CODES triggers WRITE GUARD warning.

        Same guard logic applies to skill metric writes as to forecast writes.
        """
        mock_client = MagicMock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = len(ALL_CODES)
        mock_get_client.return_value = mock_client

        data = _make_skill_metrics_df(ALL_CODES)

        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            api_writer._write_skill_metrics_to_api(data, "pentad", year=2026)

        assert "WRITE GUARD" in caplog.text, (
            "Expected 'WRITE GUARD' warning when OTHER_CODES present in skill metrics batch"
        )
        assert any(code in caplog.text for code in OTHER_CODES)


# ===========================================================================
# End-to-end scenarios
# ===========================================================================


class TestEndToEndIsolation:
    """End-to-end read→write isolation scenarios."""

    @patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.api_writer._get_postprocessing_client")
    @patch("src.data_reader.SapphirePostprocessingClient")
    def test_full_isolation_scenario(
        self, MockReadClient, mock_get_write_client, multi_org_env, caplog
    ):
        """Reading with codes=DEMO_CODES then writing produces no guard warning.

        Arrange: API returns only DEMO_CODES rows for each code query.
        Act: Read skill metrics scoped to DEMO_CODES; build write payload.
        Assert: Write completes without any WRITE GUARD warning — the batch
            is clean.
        """
        # Arrange — read mock
        read_client = MockReadClient.return_value
        read_client.readiness_check.return_value = True

        all_data = _make_skill_metrics_df(ALL_CODES)
        read_client.read_skill_metrics.side_effect = _make_skill_metrics_side_effect(all_data)

        # Arrange — write mock
        write_client = MagicMock()
        write_client.readiness_check.return_value = True
        write_client.write_skill_metrics.return_value = len(DEMO_CODES)
        mock_get_write_client.return_value = write_client

        # Act
        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}):
            read_result = data_reader.read_skill_metrics("pentad", codes=DEMO_CODES)

        assert set(read_result["code"].unique()) == set(DEMO_CODES), (
            "Read step must return only DEMO_CODES"
        )

        # Feed read result into write
        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            api_writer._write_skill_metrics_to_api(read_result, "pentad", year=2026)

        # Assert — no guard warning because the write batch is clean
        assert "WRITE GUARD" not in caplog.text, (
            "No WRITE GUARD warning expected after a correctly scoped read"
        )

    @patch("src.api_writer.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.data_reader.SAPPHIRE_API_AVAILABLE", True)
    @patch("src.api_writer._get_postprocessing_client")
    @patch("src.data_reader.SapphirePostprocessingClient")
    def test_contaminated_read_triggers_write_guard(
        self, MockReadClient, mock_get_write_client, multi_org_env, caplog
    ):
        """Reading without codes= (all stations) then writing triggers WRITE GUARD.

        Simulates the failure mode where an operator forgets to scope the read.
        The API returns ALL_CODES; writing that full set must trigger the guard
        because the configured station selection contains only DEMO_CODES.
        """
        # Arrange — read mock returns ALL_CODES (no server-side filtering)
        read_client = MockReadClient.return_value
        read_client.readiness_check.return_value = True

        all_data = _make_skill_metrics_df(ALL_CODES)

        def _unscoped_read(**kwargs):
            ret = all_data.copy()
            ret = ret.rename(
                columns={"pentad_in_year": "horizon_in_year", "model_short": "model_type"}
            )
            return ret

        read_client.read_skill_metrics.side_effect = _unscoped_read

        # Arrange — write mock
        write_client = MagicMock()
        write_client.readiness_check.return_value = True
        write_client.write_skill_metrics.return_value = len(ALL_CODES)
        mock_get_write_client.return_value = write_client

        # Act — unscoped read
        with patch.dict(os.environ, {"SAPPHIRE_API_ENABLED": "true"}):
            read_result = data_reader.read_skill_metrics("pentad", codes=None)

        assert set(read_result["code"].unique()) == set(ALL_CODES), (
            "Unscoped read must return all codes"
        )

        # Write the contaminated result
        with caplog.at_level(logging.WARNING, logger="src.api_writer"):
            api_writer._write_skill_metrics_to_api(read_result, "pentad", year=2026)

        # Assert — guard fires because OTHER_CODES are not in configured list
        assert "WRITE GUARD" in caplog.text, (
            "Expected WRITE GUARD warning when writing unscoped (contaminated) data"
        )
        assert any(code in caplog.text for code in OTHER_CODES)
