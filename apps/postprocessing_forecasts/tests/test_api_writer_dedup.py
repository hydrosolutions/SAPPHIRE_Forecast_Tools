"""Tests for the deduplication guard in _write_skill_metrics_to_api().

The dedup block sorts df_rec by _composition with na_position="first" and
keep="last", retaining the row with a non-None (or alphabetically last)
composition when multiple rows share the same DB upsert key
(code, model_type, _date, horizon_in_year_col).
"""

import os
import sys
from unittest.mock import Mock, patch

import pandas as pd
import pytest

# Add iEasyHydroForecast to path for tag_library used by api_writer
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

from src.api_writer import SAPPHIRE_API_AVAILABLE, _write_skill_metrics_to_api


class TestSkillMetricsDedup:
    """Tests for the deduplication guard in _write_skill_metrics_to_api().

    The DB unique constraint is (horizon_type, code, model_type, date,
    horizon_in_year).  When multiple rows share that key — which can happen
    for monthly/quarterly ensemble baselines due to CRPS merge fan-out — the
    dedup block retains the row with a non-None composition value.
    """

    @pytest.fixture(autouse=True)
    def _set_api_env(self, monkeypatch):
        """Enable API by default for all dedup tests."""
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_duplicate_skill_metrics_deduplicated(self, mock_client_class):
        """Duplicate rows on upsert key are collapsed; non-None composition retained.

        Two rows for Naive Mean share the same (code, model_type, _date,
        month_in_year) key but differ in composition: one is None, one is
        'LR, TFT'.  After dedup exactly one Naive Mean record survives, and
        it carries composition='LR, TFT'.  The distinct LR row is not dropped.
        """
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 2
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [15013, 15013, 15013],
                "month_in_year": [1, 1, 1],
                "model_short": ["Naive Mean", "Naive Mean", "LR"],
                "composition": [None, "LR, TFT", None],
                "sdivsigma": [0.5, 0.5, 0.5],
                "nse": [0.8, 0.8, 0.8],
                "delta": [0.1, 0.1, 0.1],
                "accuracy": [0.9, 0.9, 0.9],
                "mae": [5.0, 5.0, 5.0],
                "n_pairs": [10, 10, 10],
            }
        )

        _write_skill_metrics_to_api(data, "month", 2025)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        # Three input rows -> two distinct upsert keys -> two records
        assert len(call_args) == 2

        # The surviving Naive Mean record must carry the non-None composition
        naive_mean_records = [r for r in call_args if r["model_type"] == "Naive Mean"]
        assert len(naive_mean_records) == 1
        assert naive_mean_records[0]["composition"] == "LR, TFT"

        # The LR record is present and unaffected
        lr_records = [r for r in call_args if r["model_type"] == "LR"]
        assert len(lr_records) == 1

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_non_duplicate_records_pass_through(self, mock_client_class):
        """Records with distinct upsert keys are all written without loss.

        Three rows each have a different combination of (code, model_type,
        month_in_year), so no dedup occurs and all three arrive at the API.
        """
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 3
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [15013, 15013, 15014],
                "month_in_year": [1, 2, 1],
                "model_short": ["LR", "LR", "TFT"],
                "sdivsigma": [0.5, 0.6, 0.4],
                "nse": [0.8, 0.75, 0.85],
                "delta": [0.1, 0.12, 0.09],
                "accuracy": [0.9, 0.88, 0.91],
                "mae": [5.0, 5.5, 4.8],
                "n_pairs": [10, 12, 8],
            }
        )

        _write_skill_metrics_to_api(data, "month", 2025)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        # All three distinct records must be present — none dropped
        assert len(call_args) == 3

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_pentad_records_unaffected(self, mock_client_class):
        """Pentad records with distinct keys all pass through dedup unchanged.

        Pentad ensemble baselines do not produce composition duplicates in
        normal operation.  Three rows with distinct (code, model_type,
        pentad_in_year) keys must all reach the API.
        """
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 3
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [15013, 15013, 15013],
                "pentad_in_year": [1, 1, 2],
                "model_short": ["EM", "LR", "EM"],
                "composition": ["LR, TFT", None, "LR, TFT"],
                "sdivsigma": [0.3, 0.5, 0.35],
                "nse": [0.85, 0.8, 0.82],
                "delta": [0.08, 0.1, 0.09],
                "accuracy": [0.92, 0.9, 0.91],
                "mae": [4.0, 5.0, 4.2],
                "n_pairs": [15, 10, 14],
            }
        )

        _write_skill_metrics_to_api(data, "pentad", 2025)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        # Zero duplicates — all three records must be written
        assert len(call_args) == 3

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_multiple_non_none_compositions_keeps_last_alphabetically(self, mock_client_class):
        """When two rows share a key and both have non-None compositions, keep the alphabetically last.

        sort_values('_composition', na_position='first') followed by
        drop_duplicates(keep='last') retains the row that sorts last.
        'LR, TFT, TiDE' sorts after 'LR, TFT', so 'LR, TFT, TiDE' is kept.
        """
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [15013, 15013],
                "month_in_year": [1, 1],
                "model_short": ["EM", "EM"],
                "composition": ["LR, TFT", "LR, TFT, TiDE"],
                # Use different metric values to confirm which row is retained
                "sdivsigma": [0.3, 0.4],
                "nse": [0.85, 0.90],
                "delta": [0.08, 0.07],
                "accuracy": [0.92, 0.94],
                "mae": [4.0, 3.8],
                "n_pairs": [15, 16],
            }
        )

        _write_skill_metrics_to_api(data, "month", 2025)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        # Two duplicate rows -> one record after dedup
        assert len(call_args) == 1

        # The alphabetically last composition string must be retained
        assert call_args[0]["composition"] == "LR, TFT, TiDE"
