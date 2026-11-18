"""Real shell routing preserves simulation scope through final validation."""

import os
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "run_locally.sh"


@pytest.mark.parametrize(
    "settings,expected",
    [
        ("", ["month_0:2024:1"]),
        (
            "LT_SIMULATE_YEARS='2023 2024'; LT_SIMULATE_MODES='0 2'; LT_SIMULATE_NUM_MONTHS=2",
            ["month_0:2023:2", "month_0:2024:2", "month_2:2023:2", "month_2:2024:2"],
        ),
        ("lt_forecast_mode=quarter; LT_SIMULATE_YEARS=2025", ["quarter:2025:1"]),
        (
            "LT_SIMULATE_YEARS=$'2023\\n2024'; LT_SIMULATE_MODES=$'0\\n2'",
            ["month_0:2023:1", "month_0:2024:1", "month_2:2023:1", "month_2:2024:1"],
        ),
        ("LT_SIMULATE_MODES='   '", [""]),
        ("LT_SIMULATE_YEARS='   '", [""]),
    ],
)
def test_scope_reaches_long_term_all_and_bare_module_only(tmp_path, settings, expected):
    env = os.environ.copy()
    for name in (
        "lt_forecast_mode",
        "LT_SIMULATE_YEARS",
        "LT_SIMULATE_MODES",
        "LT_SIMULATE_NUM_MONTHS",
    ):
        env.pop(name, None)
    script = f'''
source "{SCRIPT}"
LOG_FILE="{tmp_path}/run.log"
ERROR_DIR="{tmp_path}"
run_in_venv() {{ printf 'CALL'; printf ' <%s>' "$@"; printf '\\n'; }}
{settings}
run_api_validation long-term
run_long_term_forecasting
run_api_validation short-term
run_api_validation long-term
run_api_validation all
run_module_validation long_term_forecasting
'''
    result = subprocess.run(["/bin/bash", "-c", script], env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [
        line
        for line in result.stdout.splitlines()
        if line.startswith("CALL") and "validate_pipeline.py" in line
    ]
    assert len(calls) == 5
    assert "--lt-simulation" not in calls[0]  # Legacy invocation before simulation.
    assert "--lt-simulation" not in calls[1]  # Current short-term checks.
    for call in calls[2:]:
        assert call.count("<--lt-simulation>") == len(expected)
        for value in expected:
            assert f"<{value}>" in call
