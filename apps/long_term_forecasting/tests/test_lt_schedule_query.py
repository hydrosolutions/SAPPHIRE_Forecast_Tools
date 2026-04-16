"""Tests for lt_schedule_query module."""

import io
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lt_schedule_query import HORIZON_TYPE_TO_SKILL, day_distance, main, query_schedule


class TestDayDistance:
    """Tests for day-of-month distance calculation."""

    def test_same_day(self):
        assert day_distance(10, 10) == 0

    def test_nearby(self):
        assert day_distance(12, 10) == 2
        assert day_distance(8, 10) == 2

    def test_far_apart(self):
        assert day_distance(25, 10) == 15

    def test_wrap_around_end_of_month(self):
        # Day 1 is 3 days from day 28 via wrap (30-27=3)
        assert day_distance(1, 28) == 3

    def test_wrap_around_symmetric(self):
        assert day_distance(28, 1) == 3


class TestHorizonTypeMapping:
    """Tests for horizon type to skill metric type mapping."""

    def test_month_maps_to_monthly(self):
        assert HORIZON_TYPE_TO_SKILL["month"] == "MONTHLY"

    def test_quarter_maps_to_quarterly(self):
        assert HORIZON_TYPE_TO_SKILL["quarter"] == "QUARTERLY"

    def test_season_maps_to_seasonal(self):
        assert HORIZON_TYPE_TO_SKILL["season"] == "SEASONAL"


def make_mock_config(modes, issue_day, models, forecast_months_map=None, horizon_type="month"):
    """Create a mock ForecastConfig that can be loaded per-mode.

    Args:
        modes: List of supported mode names.
        issue_day: operational_issue_day for all modes.
        models: List of model names for get_models_to_run.
        forecast_months_map: Dict of model_name -> forecast_months list.
            Defaults to all months if not provided.
        horizon_type: Horizon type string.
    """
    if forecast_months_map is None:
        forecast_months_map = {}

    config = MagicMock()
    config.LT_supported_modes = modes

    def load_config(forecast_mode):
        config._current_mode = forecast_mode

    config.load_forecast_config = MagicMock(side_effect=load_config)
    config.get_operational_issue_day = MagicMock(return_value=issue_day)
    config.get_models_to_run = MagicMock(return_value=models)
    config.get_horizon_type = MagicMock(return_value=horizon_type)

    def get_forecast_months(model_name):
        return forecast_months_map.get(model_name, list(range(1, 13)))

    config.get_forecast_months = MagicMock(side_effect=get_forecast_months)

    return config


def make_multi_mode_config(mode_configs):
    """Create a mock that behaves differently per mode.

    Args:
        mode_configs: Dict of mode_name -> {issue_day, models, forecast_months_map, horizon_type}
    """
    config = MagicMock()
    config.LT_supported_modes = list(mode_configs.keys())

    current = {}

    def load_config(forecast_mode):
        current.clear()
        current.update(mode_configs[forecast_mode])

    config.load_forecast_config = MagicMock(side_effect=load_config)
    config.get_operational_issue_day = MagicMock(side_effect=lambda: current["issue_day"])
    config.get_models_to_run = MagicMock(side_effect=lambda: current["models"])
    config.get_horizon_type = MagicMock(side_effect=lambda: current.get("horizon_type", "month"))

    def get_forecast_months(model_name):
        fm = current.get("forecast_months_map", {})
        return fm.get(model_name, list(range(1, 13)))

    config.get_forecast_months = MagicMock(side_effect=get_forecast_months)

    return config


class TestQuerySchedule:
    """Tests for query_schedule with mocked ForecastConfig."""

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_day_12_month_0_active(self, mock_sl, mock_fc_cls):
        """Day 12: month_0 (issue_day=10) active, month_1 (issue_day=25) not."""
        mock_fc_cls.return_value = make_multi_mode_config(
            {
                "month_0": {"issue_day": 10, "models": ["LR_Base"]},
                "month_1": {"issue_day": 25, "models": ["LR_Base"]},
            }
        )

        result = query_schedule(pd.Timestamp("2026-03-12"))

        assert result["active_modes"] == ["month_0"]
        assert "month_1" in result["skipped_modes"]
        assert "MONTHLY" in result["skill_metric_types"]

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_day_27_months_1_9_active(self, mock_sl, mock_fc_cls):
        """Day 27: month_1 through month_9 (issue_day=25) active."""
        modes = {
            "month_0": {"issue_day": 10, "models": ["LR_Base"]},
        }
        for i in range(1, 10):
            modes[f"month_{i}"] = {"issue_day": 25, "models": ["LR_Base"]}

        mock_fc_cls.return_value = make_multi_mode_config(modes)

        result = query_schedule(pd.Timestamp("2026-03-27"))

        assert "month_0" not in result["active_modes"]
        assert len(result["active_modes"]) == 9
        assert all(f"month_{i}" in result["active_modes"] for i in range(1, 10))

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_no_modes_active_when_far_from_all_issue_days(self, mock_sl, mock_fc_cls):
        """Day 13: >10 days from both issue_day=1 and issue_day=25.

        ISSUE_DAY_TOLERANCE is temporarily 10 (widened from 5).
        day_distance(13, 1) = min(12, 18) = 12 > 10 → skipped
        day_distance(13, 25) = min(12, 18) = 12 > 10 → skipped
        """
        mock_fc_cls.return_value = make_multi_mode_config(
            {
                "month_0": {"issue_day": 1, "models": ["LR_Base"]},
                "month_1": {"issue_day": 25, "models": ["LR_Base"]},
            }
        )

        result = query_schedule(pd.Timestamp("2026-03-13"))

        assert result["active_modes"] == []
        assert len(result["skipped_modes"]) == 2

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_quarter_mode_in_valid_month(self, mock_sl, mock_fc_cls):
        """Quarter mode in January (valid quarter month) should be active."""
        mock_fc_cls.return_value = make_multi_mode_config(
            {
                "quarter": {
                    "issue_day": 25,
                    "models": ["Q_Model"],
                    "forecast_months_map": {"Q_Model": [1, 5, 7, 10]},
                    "horizon_type": "quarter",
                },
            }
        )

        result = query_schedule(pd.Timestamp("2026-01-26"))

        assert "quarter" in result["active_modes"]
        assert "QUARTERLY" in result["skill_metric_types"]

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_quarter_mode_in_invalid_month(self, mock_sl, mock_fc_cls):
        """Quarter mode in March (not a quarter month) should be skipped."""
        mock_fc_cls.return_value = make_multi_mode_config(
            {
                "quarter": {
                    "issue_day": 25,
                    "models": ["Q_Model"],
                    "forecast_months_map": {"Q_Model": [1, 5, 7, 10]},
                    "horizon_type": "quarter",
                },
            }
        )

        result = query_schedule(pd.Timestamp("2026-03-26"))

        assert "quarter" not in result["active_modes"]
        assert "quarter" in result["skipped_modes"]

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_skill_metric_types_deduplication(self, mock_sl, mock_fc_cls):
        """Multiple monthly modes should produce a single MONTHLY entry."""
        modes = {}
        for i in range(3):
            modes[f"month_{i}"] = {
                "issue_day": 10,
                "models": ["LR_Base"],
                "horizon_type": "month",
            }
        mock_fc_cls.return_value = make_multi_mode_config(modes)

        result = query_schedule(pd.Timestamp("2026-03-12"))

        assert result["skill_metric_types"] == ["MONTHLY"]

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_mixed_skill_metric_types(self, mock_sl, mock_fc_cls):
        """Active modes with different horizon types produce all types."""
        mock_fc_cls.return_value = make_multi_mode_config(
            {
                "month_1": {
                    "issue_day": 25,
                    "models": ["LR"],
                    "horizon_type": "month",
                },
                "quarter": {
                    "issue_day": 25,
                    "models": ["Q"],
                    "forecast_months_map": {"Q": [1, 5, 7, 10]},
                    "horizon_type": "quarter",
                },
                "seasonal": {
                    "issue_day": 25,
                    "models": ["S"],
                    "forecast_months_map": {"S": [1, 2, 3, 4]},
                    "horizon_type": "season",
                },
            }
        )

        result = query_schedule(pd.Timestamp("2026-01-26"))

        assert "MONTHLY" in result["skill_metric_types"]
        assert "QUARTERLY" in result["skill_metric_types"]
        assert "SEASONAL" in result["skill_metric_types"]

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_config_load_error_skips_mode(self, mock_sl, mock_fc_cls):
        """Mode with broken config is skipped gracefully."""
        config = MagicMock()
        config.LT_supported_modes = ["month_0", "broken"]

        call_count = [0]

        def load_config(forecast_mode):
            call_count[0] += 1
            if forecast_mode == "broken":
                raise FileNotFoundError("config not found")
            config.get_operational_issue_day.return_value = 10
            config.get_models_to_run.return_value = ["LR"]
            config.get_horizon_type.return_value = "month"
            config.get_forecast_months.return_value = list(range(1, 13))

        config.load_forecast_config = MagicMock(side_effect=load_config)
        mock_fc_cls.return_value = config

        result = query_schedule(pd.Timestamp("2026-03-12"))

        assert "month_0" in result["active_modes"]
        assert "broken" in result["skipped_modes"]
        assert "config load error" in result["skipped_modes"]["broken"]

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_monthly_mode_skipped_as_non_operational(self, mock_sl, mock_fc_cls):
        """monthly mode is skipped as non-operational even when issue_day matches."""
        mock_fc_cls.return_value = make_multi_mode_config(
            {
                "monthly": {"issue_day": 10, "models": ["LR_Base"]},
                "month_0": {"issue_day": 10, "models": ["LR_Base"]},
            }
        )

        result = query_schedule(pd.Timestamp("2026-03-12"))

        assert "monthly" in result["skipped_modes"]
        assert "non-operational" in result["skipped_modes"]["monthly"]
        assert "month_0" in result["active_modes"]
        assert "monthly" not in result["active_modes"]

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_non_operational_mode_not_loaded(self, mock_sl, mock_fc_cls):
        """monthly mode is skipped before load_forecast_config is ever called."""
        mock_fc_cls.return_value = make_mock_config(
            modes=["monthly"],
            issue_day=10,
            models=["LR_Base"],
        )

        result = query_schedule(pd.Timestamp("2026-03-12"))

        assert result["active_modes"] == []
        assert "monthly" in result["skipped_modes"]
        assert mock_fc_cls.return_value.load_forecast_config.call_count == 0


class TestMainStdoutContract:
    """Verify that main() prints valid JSON as its last stdout line.

    The shell parser in run_locally.sh splits stdout on newlines and reads
    the last element as JSON.  These tests confirm that contract holds even
    when other content (e.g. log lines leaked from run_in_venv) precedes the
    JSON line.
    """

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_last_stdout_line_is_valid_json(self, mock_sl, mock_fc_cls):
        """main() with --today 2026-03-24 prints valid JSON as its last line.

        month_1 has issue_day=25, today is day 24 — distance is 1 which is
        within ISSUE_DAY_TOLERANCE, so month_1 is active.
        """
        # Arrange
        mock_fc_cls.return_value = make_multi_mode_config(
            {
                "month_1": {
                    "issue_day": 25,
                    "models": ["LR_Base"],
                    "horizon_type": "month",
                },
            }
        )

        captured = io.StringIO()

        # Act
        with (
            patch("sys.argv", ["lt_schedule_query.py", "--today", "2026-03-24"]),
            patch("sys.stdout", captured),
        ):
            main()

        # Assert — last non-empty line must be valid JSON
        stdout_text = captured.getvalue()
        lines = [line for line in stdout_text.splitlines() if line.strip()]
        assert lines, "main() produced no stdout output"
        last_line = lines[-1]
        parsed = json.loads(last_line)
        assert "active_modes" in parsed
        assert "skipped_modes" in parsed
        assert "skill_metric_types" in parsed

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_json_keys_present_with_active_mode(self, mock_sl, mock_fc_cls):
        """Parsed JSON contains the three expected top-level keys."""
        # Arrange
        mock_fc_cls.return_value = make_multi_mode_config(
            {
                "month_1": {
                    "issue_day": 25,
                    "models": ["LR_Base"],
                    "horizon_type": "month",
                },
            }
        )

        captured = io.StringIO()

        # Act
        with (
            patch("sys.argv", ["lt_schedule_query.py", "--today", "2026-03-24"]),
            patch("sys.stdout", captured),
        ):
            main()

        # Assert
        stdout_text = captured.getvalue()
        last_line = [ln for ln in stdout_text.splitlines() if ln.strip()][-1]
        parsed = json.loads(last_line)

        assert isinstance(parsed["active_modes"], list)
        assert isinstance(parsed["skipped_modes"], dict)
        assert isinstance(parsed["skill_metric_types"], list)
        assert "month_1" in parsed["active_modes"]
        assert "MONTHLY" in parsed["skill_metric_types"]

    @patch("lt_schedule_query.ForecastConfig")
    @patch("lt_schedule_query.sl")
    def test_last_line_still_valid_json_when_prefixed_by_log_lines(self, mock_sl, mock_fc_cls):
        """Simulates run_in_venv log contamination: extra lines before JSON.

        Even when fake log lines appear above the JSON line (as can happen
        when run_locally.sh captures both stdout streams), reading only the
        last newline-delimited element still yields valid JSON.
        """
        # Arrange
        mock_fc_cls.return_value = make_multi_mode_config(
            {
                "month_1": {
                    "issue_day": 25,
                    "models": ["LR_Base"],
                    "horizon_type": "month",
                },
            }
        )

        captured = io.StringIO()

        # Act — inject fake "log" prefix lines before main() writes its JSON
        with (
            patch("sys.argv", ["lt_schedule_query.py", "--today", "2026-03-24"]),
            patch("sys.stdout", captured),
        ):
            # Simulate lines that run_in_venv or a wrapper might emit to stdout
            print("INFO: activating virtual environment")
            print("INFO: running lt_schedule_query.py")
            main()

        # Assert — the last non-empty line is still valid JSON
        stdout_text = captured.getvalue()
        lines = [line for line in stdout_text.splitlines() if line.strip()]
        last_line = lines[-1]
        parsed = json.loads(last_line)
        assert "active_modes" in parsed
        assert "skipped_modes" in parsed
        assert "skill_metric_types" in parsed
