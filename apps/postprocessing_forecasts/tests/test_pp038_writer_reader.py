"""PP-038 writer + reader wiring tests.

Tests that:
1. Writer (_write_skill_metrics_to_api) sends horizon_value in the payload
   and that distinct leads are NOT collapsed by drop_duplicates.
2. Non-month horizons (no horizon_value column in input) send sentinel 0 —
   never NULL — so the cross-horizon crud tuple hazard cannot trigger.
3. _normalize_api_monthly_skill_metrics passes horizon_value through and
   coerces NaN to 0.
4. _normalize_monthly_forecasts coerces horizon_value NaN to 0.
5. read_monthly_skill_metrics returns a DataFrame that carries horizon_value.

TDD: these tests were written before the implementation changes.
"""

import os
import sys
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.api_writer import SAPPHIRE_API_AVAILABLE, _write_skill_metrics_to_api
from src.data_reader import (
    _normalize_api_monthly_skill_metrics,
    _normalize_monthly_forecasts,
    read_monthly_skill_metrics,
)

STATION = "19999"


# ============================================================================
# Writer — horizon_value in payload and dedup key
# ============================================================================


@pytest.mark.skipif(not SAPPHIRE_API_AVAILABLE, reason="sapphire-api-client not installed")
class TestWriterHorizonValue:
    """_write_skill_metrics_to_api sends horizon_value and preserves per-lead rows."""

    @pytest.fixture(autouse=True)
    def _api_enabled(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_month_two_leads_not_collapsed(self, mock_client_class):
        """Month rows with horizon_value 0 and 1 both survive dedup — not collapsed.

        The upsert_key must include horizon_value so that (code, model_type,
        date, month_in_year, horizon_value=0) and (..., horizon_value=1) are
        treated as distinct rows.
        """
        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 2
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [STATION, STATION],
                "month_in_year": [3, 3],
                "model_short": ["LR_Base", "LR_Base"],
                "horizon_value": [0, 1],
                "sdivsigma": [0.40, 0.50],
                "nse": [0.85, 0.78],
                "delta": [0.10, 0.12],
                "accuracy": [0.90, 0.87],
                "mae": [4.0, 5.5],
                "n_pairs": [10, 10],
            }
        )

        _write_skill_metrics_to_api(data, "month", 2025)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        assert len(call_args) == 2, (
            f"Expected 2 records (one per lead), got {len(call_args)}"
        )
        leads = {r["horizon_value"] for r in call_args}
        assert leads == {0, 1}, f"Expected leads {{0, 1}}, got {leads}"

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_month_horizon_value_present_in_each_record(self, mock_client_class):
        """Every written record carries a concrete integer horizon_value."""
        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [STATION],
                "month_in_year": [6],
                "model_short": ["GBT"],
                "horizon_value": [2],
                "sdivsigma": [0.35],
                "nse": [0.88],
                "delta": [0.09],
                "accuracy": [0.92],
                "mae": [3.5],
                "n_pairs": [8],
            }
        )

        _write_skill_metrics_to_api(data, "month", 2025)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        assert len(call_args) == 1
        rec = call_args[0]
        assert "horizon_value" in rec, "horizon_value must be present in the written record"
        assert rec["horizon_value"] == 2
        assert isinstance(rec["horizon_value"], int)

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_pentad_no_horizon_value_col_sends_zero(self, mock_client_class):
        """Pentad input without horizon_value column → horizon_value=0 in payload (never NULL).

        This prevents the cross-horizon NULL-tuple corruption hazard in the
        shared crud upsert, which evaluates NULL-containing tuples as never
        matching existing rows.
        """
        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [STATION],
                "pentad_in_year": [5],
                "model_short": ["LR"],
                # deliberately no horizon_value column
                "sdivsigma": [0.45],
                "nse": [0.82],
                "delta": [0.11],
                "accuracy": [0.89],
                "mae": [4.8],
                "n_pairs": [14],
            }
        )

        _write_skill_metrics_to_api(data, "pentad", 2025)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        assert len(call_args) == 1
        rec = call_args[0]
        assert "horizon_value" in rec, (
            "horizon_value must be present even when the input has no column"
        )
        assert rec["horizon_value"] == 0, (
            f"Non-month horizons must send sentinel 0, got {rec['horizon_value']!r}"
        )
        assert rec["horizon_value"] is not None, "horizon_value must never be NULL"

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_quarter_no_horizon_value_col_sends_zero(self, mock_client_class):
        """Quarter input without horizon_value column → sentinel 0 in payload."""
        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [STATION],
                "quarter_in_year": [2],
                "model_short": ["LR_Base"],
                # no horizon_value column
                "sdivsigma": [0.40],
                "nse": [0.85],
                "delta": [0.10],
                "accuracy": [0.91],
                "mae": [3.9],
                "n_pairs": [12],
            }
        )

        _write_skill_metrics_to_api(data, "quarter", 2025)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        assert len(call_args) == 1
        rec = call_args[0]
        assert "horizon_value" in rec
        assert rec["horizon_value"] == 0

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_month_nan_horizon_value_coerced_to_zero(self, mock_client_class):
        """NaN horizon_value in month input is coerced to 0 (not sent as NULL)."""
        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [STATION],
                "month_in_year": [1],
                "model_short": ["LR_Base"],
                "horizon_value": [np.nan],
                "sdivsigma": [0.42],
                "nse": [0.83],
                "delta": [0.10],
                "accuracy": [0.90],
                "mae": [4.1],
                "n_pairs": [10],
            }
        )

        _write_skill_metrics_to_api(data, "month", 2025)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        assert len(call_args) == 1
        rec = call_args[0]
        assert rec["horizon_value"] == 0, (
            f"NaN horizon_value must be coerced to sentinel 0, got {rec['horizon_value']!r}"
        )

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_composition_dedup_still_works_with_same_lead(self, mock_client_class):
        """Same (code, model, month, horizon_value) with two compositions → keep non-None.

        The composition-based dedup must still collapse fan-out duplicates when
        horizon_value is the same (i.e., the leads are the same).
        """
        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [STATION, STATION],
                "month_in_year": [4, 4],
                "model_short": ["Naive Mean", "Naive Mean"],
                "horizon_value": [0, 0],   # same lead → upsert_key collision
                "composition": [None, "GBT, LR"],
                "sdivsigma": [0.5, 0.5],
                "nse": [0.8, 0.8],
                "delta": [0.1, 0.1],
                "accuracy": [0.9, 0.9],
                "mae": [5.0, 5.0],
                "n_pairs": [10, 10],
            }
        )

        _write_skill_metrics_to_api(data, "month", 2025)

        call_args = mock_client.write_skill_metrics.call_args[0][0]

        # Two same-lead rows collapse to one; non-None composition retained
        assert len(call_args) == 1
        assert call_args[0]["composition"] == "GBT, LR"


# ============================================================================
# Reader — _normalize_api_monthly_skill_metrics
# ============================================================================


class TestNormalizeApiMonthlySkillMetrics:
    """_normalize_api_monthly_skill_metrics passes horizon_value through."""

    def test_horizon_value_preserved_when_present(self):
        """horizon_value column is preserved (not renamed or dropped)."""
        df = pd.DataFrame(
            {
                "horizon_in_year": [3, 6],
                "model_type": ["GBT", "LR"],
                "code": [STATION, STATION],
                "horizon_value": [1, 2],
                "sdivsigma": [0.4, 0.5],
                "nse": [0.85, 0.78],
                "mae": [3.5, 4.2],
                "n_pairs": [10, 10],
            }
        )

        result = _normalize_api_monthly_skill_metrics(df)

        assert "horizon_value" in result.columns, (
            "horizon_value must be present after normalization"
        )
        assert list(result["horizon_value"]) == [1, 2]

    def test_nan_horizon_value_coerced_to_zero(self):
        """NaN horizon_value (legacy rows) is coerced to 0 after normalization."""
        df = pd.DataFrame(
            {
                "horizon_in_year": [3],
                "model_type": ["GBT"],
                "code": [STATION],
                "horizon_value": [np.nan],
                "sdivsigma": [0.4],
                "nse": [0.85],
                "mae": [3.5],
                "n_pairs": [10],
            }
        )

        result = _normalize_api_monthly_skill_metrics(df)

        assert "horizon_value" in result.columns
        assert int(result["horizon_value"].iloc[0]) == 0

    def test_horizon_in_year_renamed_to_month_in_year(self):
        """horizon_in_year → month_in_year rename is unchanged."""
        df = pd.DataFrame(
            {
                "horizon_in_year": [4],
                "model_type": ["GBT"],
                "code": [STATION],
                "horizon_value": [0],
                "nse": [0.85],
                "n_pairs": [10],
            }
        )

        result = _normalize_api_monthly_skill_metrics(df)

        assert "month_in_year" in result.columns
        assert "horizon_in_year" not in result.columns
        assert result["month_in_year"].iloc[0] == 4

    def test_no_horizon_value_col_does_not_raise(self):
        """DataFrame without horizon_value column still normalizes without error."""
        df = pd.DataFrame(
            {
                "horizon_in_year": [3],
                "model_type": ["GBT"],
                "code": [STATION],
                "nse": [0.85],
                "n_pairs": [10],
            }
        )

        result = _normalize_api_monthly_skill_metrics(df)

        assert "month_in_year" in result.columns


# ============================================================================
# Reader — _normalize_monthly_forecasts
# ============================================================================


class TestNormalizeMonthlyForecasts:
    """_normalize_monthly_forecasts coerces horizon_value NaN to sentinel 0."""

    def _make_raw(self, horizon_value):
        """Minimal API-like raw forecast DataFrame."""
        return pd.DataFrame(
            {
                "horizon_type": ["month"],
                "horizon_value": [horizon_value],
                "code": [STATION],
                "model_type": ["GBT"],
                "valid_from": ["2025-03-01"],
                "valid_to": ["2025-03-31"],
                "date": ["2025-02-01"],
                "flag": [0],
                "q50": [120.0],
                "q05": [90.0],
                "q10": [95.0],
                "q25": [105.0],
                "q75": [135.0],
                "q90": [145.0],
                "q95": [150.0],
            }
        )

    def test_nan_horizon_value_coerced_to_zero(self):
        """NaN horizon_value is coerced to 0 so groupby doesn't drop it."""
        raw = self._make_raw(np.nan)
        result = _normalize_monthly_forecasts(raw)

        assert "horizon_value" in result.columns
        assert int(result["horizon_value"].iloc[0]) == 0

    def test_integer_horizon_value_preserved(self):
        """Concrete horizon_value is preserved as int."""
        raw = self._make_raw(2)
        result = _normalize_monthly_forecasts(raw)

        assert "horizon_value" in result.columns
        assert result["horizon_value"].iloc[0] == 2

    def test_model_type_renamed_to_model_short(self):
        """model_type → model_short rename is unchanged."""
        raw = self._make_raw(1)
        result = _normalize_monthly_forecasts(raw)

        assert "model_short" in result.columns
        assert "model_type" not in result.columns


# ============================================================================
# Reader — read_monthly_skill_metrics exposes horizon_value
# ============================================================================


class TestReadMonthlySkillMetricsHorizonValue:
    """read_monthly_skill_metrics returns horizon_value in the result columns."""

    def test_horizon_value_in_result_columns_when_api_returns_it(self, monkeypatch):
        """When the API response includes horizon_value, it appears in the result."""
        api_response = pd.DataFrame(
            {
                "horizon_in_year": [3, 3],
                "model_type": ["GBT", "LR"],
                "code": [STATION, STATION],
                "horizon_value": [0, 1],
                "sdivsigma": [0.4, 0.5],
                "nse": [0.85, 0.78],
                "delta": [0.10, 0.12],
                "accuracy": [0.90, 0.87],
                "mae": [3.5, 4.2],
                "n_pairs": [10, 10],
            }
        )

        with patch("src.data_reader._read_monthly_skill_metrics_api") as mock_api:
            mock_api.return_value = api_response
            result = read_monthly_skill_metrics(codes=[STATION])

        assert "horizon_value" in result.columns, (
            "read_monthly_skill_metrics must expose horizon_value when the API returns it"
        )

    def test_horizon_value_preserved_values(self, monkeypatch):
        """horizon_value values are preserved correctly (0 and 1)."""
        api_response = pd.DataFrame(
            {
                "horizon_in_year": [5, 5],
                "model_type": ["GBT", "GBT"],
                "code": [STATION, STATION],
                "horizon_value": [0, 1],
                "nse": [0.82, 0.79],
                "n_pairs": [8, 8],
            }
        )

        with patch("src.data_reader._read_monthly_skill_metrics_api") as mock_api:
            mock_api.return_value = api_response
            result = read_monthly_skill_metrics(codes=[STATION])

        assert set(result["horizon_value"].unique()) == {0, 1}
