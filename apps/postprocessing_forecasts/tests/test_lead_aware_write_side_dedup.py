"""M1 P5c: lead-awareness on the WRITE side.

Two flag-gated fixes proven here:

FIX #1 — the SEASONAL operational-ensemble dedup in
``postprocessing_operational_long_term.py`` must NOT collapse distinct
``horizon_value`` leads for the same (season_year, season, code, model)
under flag-ON (season is multi-lead and its combined reader preserves a
populated ``horizon_value``). The QUARTERLY dedup, by contrast, is
single-lead: it always uses the legacy 4-column key under BOTH flag states,
because the quarterly combined reader drops ``horizon_value`` and hv-keying
would split a stale NaN-lead existing row from the fresh real-lead row and
duplicate it on write. Flag-OFF keeps the exact legacy key (byte-identical).

FIX #5 — ``_write_skill_metrics_to_api`` in ``src/api_writer.py`` must, under
flag-ON, WARN-exclude rows whose ``horizon_value`` is NaN rather than coerce
them to real lead-0 (which can overwrite a genuine lead-0 row via the
upsert-key dedup). Flag-OFF keeps the legacy ``fillna(0)`` sentinel behavior,
and a non-month horizon with no ``horizon_value`` column still writes 0.
"""

import importlib.util
import os
import sys
from unittest.mock import Mock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.api_writer import SAPPHIRE_API_AVAILABLE, _write_skill_metrics_to_api

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

CODE = "19999"


def _load_long_term_module():
    """Import postprocessing_operational_long_term with its side effects.

    The module configures logging and creates a ``logs/`` dir at import;
    that is acceptable in the test env (other modules do the same).
    """
    spec = importlib.util.spec_from_file_location(
        "postprocessing_operational_long_term_p5c",
        os.path.join(SCRIPT_DIR, "postprocessing_operational_long_term.py"),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_LT = _load_long_term_module()


# --------------------------------------------------------------------------
# FIX #1 — quarterly / seasonal dedup preserves distinct leads under flag-ON
# --------------------------------------------------------------------------
def _quarterly_two_leads():
    """Two rows: same (year, quarter, code, model), distinct horizon_value."""
    return pd.DataFrame(
        {
            "year": [2025, 2025],
            "quarter_in_year": [1, 1],
            "code": [CODE, CODE],
            "model_short": ["LR_Base", "LR_Base"],
            "horizon_value": [0, 1],
            "q50": [100.0, 111.0],
        }
    )


def _quarterly_existing_no_hv_and_fresh_with_hv():
    """Reproduce the exact P5c bug shape on the write side.

    ``existing_q`` (as ``read_quarterly_combined_forecasts`` returns it for the
    single-lead quarter deployment) carries NO ``horizon_value`` column; the
    fresh ``quarterly_joint`` carries a real configured lead. After
    ``pd.concat`` the stale existing row's ``horizon_value`` is NaN while the
    fresh row's is a real lead — but both are the SAME
    (year, quarter, code, model). The fresh row is tagged with q50=20 so it can
    be identified as the ``keep="last"`` survivor.
    """
    existing_q = pd.DataFrame(
        {
            "year": [2025],
            "quarter_in_year": [1],
            "code": [CODE],
            "model_short": ["LR_Base"],
            "q50": [100.0],
        }
    )
    fresh = pd.DataFrame(
        {
            "year": [2025],
            "quarter_in_year": [1],
            "code": [CODE],
            "model_short": ["LR_Base"],
            "horizon_value": [3],
            "q50": [20.0],
        }
    )
    return pd.concat([existing_q, fresh], ignore_index=True)


def _seasonal_two_leads():
    return pd.DataFrame(
        {
            "season_year": [2025, 2025],
            "season_in_year": [1, 1],
            "code": [CODE, CODE],
            "model_short": ["LR_Base", "LR_Base"],
            "horizon_value": [0, 1],
            "q50": [100.0, 111.0],
        }
    )


class TestQuarterlyDedupLeadAware:
    def test_flag_on_collapses_same_key_single_lead(self, monkeypatch):
        # Quarter is single-lead: flag-ON must COLLAPSE two rows sharing
        # (year, quarter, code, model) to one, mirroring trunk/flag-OFF,
        # regardless of horizon_value.
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        result = _LT._dedup_quarterly_joint(_quarterly_two_leads())
        assert len(result) == 1
        # keep="last" retains the second row (lead 1).
        assert result.iloc[0]["horizon_value"] == 1

    def test_flag_on_stale_existing_without_hv_is_replaced(self, monkeypatch):
        # REGRESSION (P5c bug): existing_q has no horizon_value column, so
        # after pd.concat with a fresh real-lead row the existing row is
        # NaN-lead. hv-keyed dedup would treat NaN vs the real lead as
        # distinct and keep BOTH, duplicating the stale row on write. The
        # legacy 4-column key must collapse them to the fresh keep="last".
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        combined = _quarterly_existing_no_hv_and_fresh_with_hv()
        result = _LT._dedup_quarterly_joint(combined)
        assert len(result) == 1
        # The fresh real-lead row wins (tagged q50=20), not the stale row.
        assert result.iloc[0]["q50"] == 20.0

    def test_flag_off_collapses_to_one(self, monkeypatch):
        # LOCKED legacy: 4-column key ignores horizon_value -> one survives.
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        result = _LT._dedup_quarterly_joint(_quarterly_two_leads())
        assert len(result) == 1
        # keep="last" retains the second row (lead 1).
        assert result.iloc[0]["horizon_value"] == 1

    def test_flag_on_without_horizon_value_column_matches_legacy(self, monkeypatch):
        # No horizon_value column: nothing to stratify on, behaves as legacy.
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        df = _quarterly_two_leads().drop(columns=["horizon_value"])
        result = _LT._dedup_quarterly_joint(df)
        assert len(result) == 1


class TestSeasonalDedupLeadAware:
    def test_flag_on_preserves_both_leads(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")
        result = _LT._dedup_seasonal_joint(_seasonal_two_leads())
        assert len(result) == 2
        assert sorted(result["horizon_value"]) == [0, 1]

    def test_flag_off_collapses_to_one(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)
        result = _LT._dedup_seasonal_joint(_seasonal_two_leads())
        assert len(result) == 1
        assert result.iloc[0]["horizon_value"] == 1


# --------------------------------------------------------------------------
# FIX #5 — NaN horizon_value WARN-excluded under flag-ON, sentinel under OFF
# --------------------------------------------------------------------------
def _skill_df_lead0_and_nan():
    """One genuine lead-0 row + one NaN-lead row for the SAME upsert key.

    Both rows share (code, model, quarter, target-date). Coercing NaN->0
    (legacy behavior) would collapse them via the upsert-key dedup and
    could overwrite the real lead-0 row.
    """
    return pd.DataFrame(
        {
            "code": [CODE, CODE],
            "quarter_in_year": [1, 1],
            "model_short": ["LR", "LR"],
            "horizon_value": [0, float("nan")],
            "nse": [0.90, 0.10],
            "sdivsigma": [0.30, 0.99],
            "delta": [0.08, 0.50],
            "accuracy": [0.92, 0.20],
            "mae": [4.0, 40.0],
            "n_pairs": [15, 15],
        }
    )


class TestWriteSkillMetricsNaNLead:
    @pytest.fixture(autouse=True)
    def _set_api_env(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_API_ENABLED", "true")

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_flag_on_warn_excludes_nan_and_preserves_lead0(
        self, mock_client_class, monkeypatch, caplog
    ):
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        with caplog.at_level("WARNING"):
            _write_skill_metrics_to_api(_skill_df_lead0_and_nan(), "quarter", 2025)

        records = mock_client.write_skill_metrics.call_args[0][0]
        # NaN-lead row dropped -> exactly the genuine lead-0 record survives.
        assert len(records) == 1
        assert records[0]["horizon_value"] == 0
        # The real lead-0 row is preserved INTACT (not overwritten by the
        # NaN row's inferior metrics).
        assert records[0]["nse"] == pytest.approx(0.90)
        # WARN naming the excluded count was emitted.
        assert any("NULL-lead" in r.message and "skipped" in r.message for r in caplog.records)

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_flag_off_coerces_nan_to_sentinel_zero(self, mock_client_class, monkeypatch):
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")
        # LOCKED legacy: NaN -> 0, then upsert-key dedup collapses the two
        # rows into one (keep="last" -> the NaN-origin row wins).
        monkeypatch.delenv("SAPPHIRE_SKILL_LEAD_AWARE", raising=False)

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        _write_skill_metrics_to_api(_skill_df_lead0_and_nan(), "quarter", 2025)

        records = mock_client.write_skill_metrics.call_args[0][0]
        assert len(records) == 1
        # Both rows normalized to horizon_value=0 (sentinel).
        assert records[0]["horizon_value"] == 0

    @patch("src.api_writer.SapphirePostprocessingClient")
    def test_non_month_horizon_no_column_writes_sentinel_under_flag_on(
        self, mock_client_class, monkeypatch
    ):
        # Guard unchanged: a horizon with NO horizon_value column still gets
        # the concrete sentinel 0 (cross-horizon NULL-tuple hazard), even
        # under flag-ON.
        if not SAPPHIRE_API_AVAILABLE:
            pytest.skip("sapphire-api-client not installed")
        monkeypatch.setenv("SAPPHIRE_SKILL_LEAD_AWARE", "true")

        mock_client = Mock()
        mock_client.readiness_check.return_value = True
        mock_client.write_skill_metrics.return_value = 1
        mock_client_class.return_value = mock_client

        data = pd.DataFrame(
            {
                "code": [CODE],
                "pentad_in_year": [1],
                "model_short": ["LR"],
                "nse": [0.8],
                "sdivsigma": [0.5],
                "delta": [0.1],
                "accuracy": [0.9],
                "mae": [5.0],
                "n_pairs": [10],
            }
        )

        _write_skill_metrics_to_api(data, "pentad", 2025)

        records = mock_client.write_skill_metrics.call_args[0][0]
        assert len(records) == 1
        assert records[0]["horizon_value"] == 0
