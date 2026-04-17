"""Tests for scoped skill metric recalculation (SAPPHIRE_RECALC_STATION_CODE).

Covers:
- _read_station_codes env var override
- _run_short_term_recalc empty DataFrame guard
- _run_short_term_recalc virtual station skip
"""

import importlib.util
import json
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import src.horizon_config as _real_horizon_config

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, SCRIPT_DIR)


def _load_recalc_module(mocks):
    """Load recalculate_skill_metrics with the given sys.modules mocks in place."""
    # Prevent stale cached module from leaking between tests
    sys.modules.pop("recalculate_skill_metrics", None)
    sys.modules.pop("recalculate_skill_metrics_module", None)

    spec = importlib.util.spec_from_file_location(
        "recalculate_skill_metrics_module",
        os.path.join(SCRIPT_DIR, "recalculate_skill_metrics.py"),
    )
    module = importlib.util.module_from_spec(spec)

    # Build a src package mock whose attributes match the sub-module mocks.
    # `from src import data_reader` resolves via the src package object, NOT
    # via sys.modules["src.data_reader"], so the attributes must be set here.
    mock_src = MagicMock()
    mock_src.data_reader = mocks["data_reader"]
    mock_src.file_writer = mocks["file_writer"]
    mock_src.skill_metrics = mocks["skill_metrics"]
    mock_src.postprocessing_tools = mocks["pt"]

    # Inject mocks before exec so module-level imports resolve to our fakes
    sys.modules["setup_library"] = mocks["sl"]
    sys.modules["tag_library"] = MagicMock()
    sys.modules["src"] = mock_src
    sys.modules["src.skill_metrics"] = mocks["skill_metrics"]
    sys.modules["src.file_writer"] = mocks["file_writer"]
    sys.modules["src.data_reader"] = mocks["data_reader"]
    sys.modules["src.postprocessing_tools"] = mocks["pt"]
    sys.modules["src.horizon_config"] = _real_horizon_config

    spec.loader.exec_module(module)
    return module


def _make_mocks():
    """Return a minimal set of mocks needed to load recalculate_skill_metrics."""
    mock_sl = MagicMock()
    mock_sl.calculate_virtual_stations_data.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast.side_effect = lambda x: x
    mock_sl.calculate_neural_ensemble_forecast_decade.side_effect = lambda x: x

    mock_skill_metrics = MagicMock()
    mock_file_writer = MagicMock()
    mock_data_reader = MagicMock()

    mock_pt = MagicMock()
    # timer() is used as a context manager — make it behave like one
    mock_timer_ctx = MagicMock()
    mock_timer_ctx.__enter__ = MagicMock(return_value=None)
    mock_timer_ctx.__exit__ = MagicMock(return_value=False)
    mock_pt.timer.return_value = mock_timer_ctx
    mock_pt.TimingStats.return_value = MagicMock()

    return {
        "sl": mock_sl,
        "skill_metrics": mock_skill_metrics,
        "file_writer": mock_file_writer,
        "data_reader": mock_data_reader,
        "pt": mock_pt,
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pentad_config():
    """Minimal ShortTermHorizonConfig-like mock for pentad."""
    cfg = MagicMock()
    cfg.name = "pentad"
    cfg.period_col = "pentad_in_year"
    cfg.station_selection_env = "ieasyforecast_config_file_station_selection"
    cfg.neural_ensemble_func = MagicMock(side_effect=lambda x: x)
    return cfg


@pytest.fixture
def non_empty_data():
    """Non-empty observed/modelled DataFrame with required columns."""
    return pd.DataFrame(
        {
            "code": ["19999"],
            "date": pd.to_datetime(["2024-01-05"]),
            "forecasted_discharge": [100.0],
        }
    )


# ---------------------------------------------------------------------------
# Test 1: _read_station_codes — env var override short-circuits file I/O
# ---------------------------------------------------------------------------


def test_read_station_codes_override(pentad_config):
    """When SAPPHIRE_RECALC_STATION_CODE is set, skip file reading and return it."""
    mocks = _make_mocks()

    with patch.dict(os.environ, {"SAPPHIRE_RECALC_STATION_CODE": "19999"}):
        with patch.dict(sys.modules, {}):
            module = _load_recalc_module(mocks)

            with patch("builtins.open") as mock_open:
                result = module._read_station_codes(pentad_config)

    assert result == ["19999"]
    mock_open.assert_not_called()


# ---------------------------------------------------------------------------
# Test 2: _read_station_codes — reads station codes from config file
# ---------------------------------------------------------------------------


def test_read_station_codes_no_override(pentad_config):
    """Without SAPPHIRE_RECALC_STATION_CODE, read codes from the config file."""
    mocks = _make_mocks()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"stationsID": ["19999", "19998"]}, f)
        tmp_path = f.name

    try:
        tmp_dir = os.path.dirname(tmp_path)
        tmp_file = os.path.basename(tmp_path)

        env_override = {
            "SAPPHIRE_RECALC_STATION_CODE": "",
            "ieasyforecast_configuration_path": tmp_dir,
            "ieasyforecast_config_file_station_selection": tmp_file,
        }

        with patch.dict(os.environ, env_override):
            with patch.dict(sys.modules, {}):
                module = _load_recalc_module(mocks)
                result = module._read_station_codes(pentad_config)

        assert result == ["19999", "19998"]
    finally:
        os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# Test 3: _run_short_term_recalc — early return when data is empty
# ---------------------------------------------------------------------------


def test_run_short_term_recalc_empty_data_returns_early(pentad_config):
    """Empty observed+modelled DataFrames cause early return without skill calc."""
    mocks = _make_mocks()
    mocks["data_reader"].read_observed_and_modelled_data.return_value = (
        pd.DataFrame(),
        pd.DataFrame(),
    )

    with patch.dict(os.environ, {"SAPPHIRE_RECALC_STATION_CODE": "19999"}):
        with patch.dict(sys.modules, {}):
            module = _load_recalc_module(mocks)

            timing_stats_ = MagicMock()
            result = module._run_short_term_recalc(
                pentad_config,
                2024,
                [],
                timing_stats_,
                codes=["19999"],
            )

    mocks["skill_metrics"].calculate_skill_metrics.assert_not_called()
    assert result is timing_stats_


# ---------------------------------------------------------------------------
# Test 4: _run_short_term_recalc — skip virtual stations for scoped recalc
# ---------------------------------------------------------------------------


def test_run_short_term_recalc_skips_virtual_stations(pentad_config, non_empty_data):
    """When SAPPHIRE_RECALC_STATION_CODE is set, virtual station calc is skipped."""
    mocks = _make_mocks()
    mocks["data_reader"].read_observed_and_modelled_data.return_value = (
        non_empty_data,
        non_empty_data,
    )
    mocks["skill_metrics"].calculate_skill_metrics.return_value = (
        pd.DataFrame(),
        non_empty_data,
        MagicMock(),
    )
    mocks["file_writer"].save_forecast_data.return_value = None
    mocks["file_writer"].save_skill_metrics.return_value = None

    with patch.dict(os.environ, {"SAPPHIRE_RECALC_STATION_CODE": "19999"}):
        with patch.dict(sys.modules, {}):
            module = _load_recalc_module(mocks)

            module._run_short_term_recalc(
                pentad_config,
                2024,
                [],
                MagicMock(),
                codes=["19999"],
            )

    mocks["sl"].calculate_virtual_stations_data.assert_not_called()
