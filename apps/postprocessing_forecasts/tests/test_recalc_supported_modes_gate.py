"""Tests for the supported-modes recalc gate (finding #2).

`recalculate_skill_metrics.py` gates each long-term horizon block on
`prediction_mode` AND, under SAPPHIRE_SKILL_LEAD_AWARE, on whether the
deployment actually configures an operational mode for that horizon
(`_long_term_horizon_supported`).

Rationale: the long-term readers warn+return EMPTY when a horizon has no
configured operational schedules under the flag. Without this gate, the
block would still read (empty) -> compute empty skill ->
`build_stale_tombstones(existing, empty, ...)` would tombstone (n_pairs=0)
ALL of that horizon's stored skill, and `save_*_skill_metrics` would
overwrite the skill CSV empty -> data loss on a misconfig.

The gate:
  * Flag OFF  -> always True (byte-identical legacy behavior).
  * Flag ON + >=1 mode configured for the horizon -> True.
  * Flag ON + no mode configured -> WARNING + False -> block skipped
    (no read, no calc, no tombstone, no save).
  * Flag ON + a configured mode's config is incomplete -> the underlying
    LongTermHorizonResolverError PROPAGATES (fail-loud, not swallowed).

Note: a SUPPORTED horizon whose groups all fall below min-n still emits
empty and tombstones as before -- the gate only skips UNSUPPORTED horizons.
"""

import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))
sys.path.insert(0, SCRIPT_DIR)

import recalculate_skill_metrics as recalc  # noqa: E402  (path set above)
from long_term_horizon_resolver import LongTermHorizonResolverError

# Reuse the established recalc-driver mocks/harness from the sibling test file.
from tests.test_recalc_workflow import (
    _setup_mocks,
    import_recalc_module,
)

WARNING_NEEDLE_A = "no operational"
WARNING_NEEDLE_B = "ieasyhydroforecast_ml_long_term_supported_modes"


def _has_skip_warning(caplog):
    return any(
        WARNING_NEEDLE_A in r.getMessage().lower() and WARNING_NEEDLE_B in r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING
    )


# ---------------------------------------------------------------------------
# Unit tests: _long_term_horizon_supported (direct)
# ---------------------------------------------------------------------------


class TestLongTermHorizonSupportedHelper:
    def test_flag_off_true_even_when_schedules_empty(self, monkeypatch):
        """Flag OFF -> always True; schedules resolver must not even be consulted."""
        monkeypatch.setattr(recalc, "skill_lead_aware_enabled", lambda: False)
        mock_dr = MagicMock()
        monkeypatch.setattr(recalc, "data_reader", mock_dr)

        assert recalc._long_term_horizon_supported("month") is True
        # Flag OFF short-circuits before touching the resolver.
        mock_dr._operational_schedules_for_horizon_type.assert_not_called()

    def test_flag_on_nonempty_schedules_true(self, monkeypatch):
        monkeypatch.setattr(recalc, "skill_lead_aware_enabled", lambda: True)
        mock_dr = MagicMock()
        mock_dr._operational_schedules_for_horizon_type.return_value = {"quarter": object()}
        monkeypatch.setattr(recalc, "data_reader", mock_dr)

        assert recalc._long_term_horizon_supported("quarter") is True
        mock_dr._operational_schedules_for_horizon_type.assert_called_once_with("quarter")

    def test_flag_on_empty_schedules_false_and_warns(self, monkeypatch, caplog):
        # month/quarter branch: driven by the reader's operational schedules.
        monkeypatch.setattr(recalc, "skill_lead_aware_enabled", lambda: True)
        mock_dr = MagicMock()
        mock_dr._operational_schedules_for_horizon_type.return_value = {}
        monkeypatch.setattr(recalc, "data_reader", mock_dr)

        with caplog.at_level(logging.WARNING):
            result = recalc._long_term_horizon_supported("month")

        assert result is False
        assert _has_skip_warning(caplog)

    def test_flag_on_resolver_error_propagates(self, monkeypatch):
        """A configured-but-malformed mode raises during resolution; the
        helper must NOT swallow it (fail-loud contract)."""
        monkeypatch.setattr(recalc, "skill_lead_aware_enabled", lambda: True)
        mock_dr = MagicMock()
        mock_dr._operational_schedules_for_horizon_type.side_effect = LongTermHorizonResolverError(
            "month_0 missing operational_issue_day"
        )
        monkeypatch.setattr(recalc, "data_reader", mock_dr)

        with pytest.raises(LongTermHorizonResolverError, match="operational_issue_day"):
            recalc._long_term_horizon_supported("month")


class TestSeasonHorizonSupportedHelper:
    """Season gate consistency (fix 5b): the season branch is driven by
    ``_supported_seasonal_issue_leads()`` -- the SAME source the seasonal
    recalc block reads -- NOT the reader's looser ``seasonal_*`` schedule
    match. This prevents a mis-named seasonal mode from opening the gate on a
    horizon the block reads nothing for (which would tombstone stored skill).
    """

    def test_season_flag_off_true(self, monkeypatch):
        """Flag OFF -> always True, even with no seasonal issue leads."""
        monkeypatch.setattr(recalc, "skill_lead_aware_enabled", lambda: False)
        monkeypatch.setattr(recalc, "_supported_seasonal_issue_leads", lambda: [])
        assert recalc._long_term_horizon_supported("season") is True

    def test_season_flag_on_issue_leads_nonempty_true(self, monkeypatch):
        """Flag ON + >=1 seasonal issue lead -> True. The reader's schedule
        match is NOT consulted for season (gate relies on the issue-leads
        helper, not the loose seasonal_* match)."""
        monkeypatch.setattr(recalc, "skill_lead_aware_enabled", lambda: True)
        monkeypatch.setattr(recalc, "_supported_seasonal_issue_leads", lambda: [3])
        mock_dr = MagicMock()
        # Deliberately empty: were the gate still using this, it would return False.
        mock_dr._operational_schedules_for_horizon_type.return_value = {}
        monkeypatch.setattr(recalc, "data_reader", mock_dr)

        assert recalc._long_term_horizon_supported("season") is True
        mock_dr._operational_schedules_for_horizon_type.assert_not_called()

    def test_season_flag_on_issue_leads_empty_false_and_warns(self, monkeypatch, caplog):
        """Flag ON + no seasonal issue lead -> False + WARNING, EVEN WHEN the
        reader's loose seasonal_* schedule match WOULD have returned non-empty.
        Proves the gate now relies on the issue-leads helper, not that match."""
        monkeypatch.setattr(recalc, "skill_lead_aware_enabled", lambda: True)
        monkeypatch.setattr(recalc, "_supported_seasonal_issue_leads", lambda: [])
        mock_dr = MagicMock()
        # Non-empty: a mis-named seasonal_* mode the block would read nothing for.
        mock_dr._operational_schedules_for_horizon_type.return_value = {"season": object()}
        monkeypatch.setattr(recalc, "data_reader", mock_dr)

        with caplog.at_level(logging.WARNING):
            result = recalc._long_term_horizon_supported("season")

        assert result is False
        assert _has_skip_warning(caplog)
        mock_dr._operational_schedules_for_horizon_type.assert_not_called()


# ---------------------------------------------------------------------------
# Integration: the gate actually SKIPS / REACHES the quarterly block
# (seam = data_reader.read_quarterly_forecasts + build_stale_tombstones)
# ---------------------------------------------------------------------------


@pytest.fixture
def _mock_data():
    return pd.DataFrame(
        {
            "code": ["19999"],
            "date": pd.to_datetime(["2024-01-05"]),
            "forecasted_discharge": [100.0],
        }
    )


@pytest.fixture
def _mock_skill():
    return pd.DataFrame({"pentad_in_year": [1], "code": ["19999"], "sdivsigma": [0.3]})


def _run_quarterly(env, schedules_return, mock_data, mock_skill):
    """Drive recalc in QUARTERLY mode with the given env + resolver return,
    returning the data_reader/file_writer mocks and the imported module."""
    with patch.dict(os.environ, {"SAPPHIRE_PREDICTION_MODE": "QUARTERLY", **env}):
        with patch.dict(sys.modules, {}):
            mocks = _setup_mocks(mock_data, mock_skill)
            mocks[
                "data_reader"
            ]._operational_schedules_for_horizon_type.return_value = schedules_return

            module, spec = import_recalc_module()
            spec.loader.exec_module(module)
            module._read_station_codes = MagicMock(return_value=["19999"])

            with pytest.raises(SystemExit) as exc_info:
                module.recalculate_skill_metrics()

            return exc_info.value.code, mocks, module


class TestQuarterlyBlockGate:
    def test_flag_on_no_quarter_mode_skips_block(self, _mock_data, _mock_skill):
        """Flag ON + empty quarter schedules -> block skipped: no read, no
        tombstone (which would zero out stored quarterly skill)."""
        code, mocks, module = _run_quarterly(
            {"SAPPHIRE_SKILL_LEAD_AWARE": "true"}, {}, _mock_data, _mock_skill
        )
        assert code == 0
        # Block skipped entirely: no read, no calc, no tombstone, no save.
        # (read_quarterly_forecasts is the block's entry read; build_stale_tombstones
        #  and save_quarterly_skill_metrics are only reachable after it.)
        mocks["data_reader"].read_quarterly_forecasts.assert_not_called()
        mocks["data_reader"].read_quarterly_observations.assert_not_called()
        mocks["skill_metrics"].calculate_quarterly_skill_metrics.assert_not_called()
        mocks["file_writer"].save_quarterly_skill_metrics.assert_not_called()

    def test_flag_on_quarter_mode_reaches_block(self, _mock_data, _mock_skill):
        """Flag ON + a configured quarter mode -> block runs (reads happen)."""
        code, mocks, module = _run_quarterly(
            {"SAPPHIRE_SKILL_LEAD_AWARE": "true"},
            {"quarter": object()},
            _mock_data,
            _mock_skill,
        )
        assert code == 0
        mocks["data_reader"].read_quarterly_forecasts.assert_called_once()

    def test_flag_off_runs_block_unchanged(self, _mock_data, _mock_skill):
        """Flag OFF -> gate is a no-op, block runs regardless of schedules
        (byte-identical to legacy)."""
        code, mocks, module = _run_quarterly({}, {}, _mock_data, _mock_skill)
        assert code == 0
        mocks["data_reader"].read_quarterly_forecasts.assert_called_once()
        # Flag OFF must not even consult the resolver.
        mocks["data_reader"]._operational_schedules_for_horizon_type.assert_not_called()
