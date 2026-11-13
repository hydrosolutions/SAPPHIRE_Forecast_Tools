"""Tests for the alert pattern used by bin/monitoring/docker_log_watcher.sh.

The script has top-level side effects and cannot be sourced, so the real
``pattern=`` value and grep flags are read from the script text and applied
with the actual grep binary.
"""

import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
WATCHER_SH = REPO_ROOT / "bin" / "monitoring" / "docker_log_watcher.sh"


def _read_pattern_and_flags():
    text = WATCHER_SH.read_text()
    pattern = re.search(r'^pattern="(.*)"$', text, re.MULTILINE).group(1)
    flags = re.search(r'grep --line-buffered (-[A-Za-z]+) "\$pattern"', text).group(1)
    return pattern, flags


def _alerts(line: str) -> bool:
    pattern, flags = _read_pattern_and_flags()
    result = subprocess.run(
        ["grep", flags, "--", pattern],
        input=line + "\n",
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


@pytest.mark.parametrize(
    "line",
    [
        "2026-11-13 09:32:03,218 - WARNING - 404 GET //wp-includes/wlwmanifest.xml (172.18.0.1) 0.38ms",
        "2026-11-13 16:25:22,915 404 GET /.env (172.18.0.1) 0.51ms",
        "2026-11-13 16:12:03,300 - WARNING - 404 GET /.git/config (172.18.0.1) 0.40ms",
        "2026-11-13 05:40:48,850 404 GET /style.php (172.18.0.1) 0.43ms",
        "2026-11-13 13:51:28,636 404 GET /api/session/properties (172.18.0.1) 0.67ms",
        "2026-11-13 12:08:22,404 - DEBUG - get_long_forecasts completed in 0.063s",
        "2026-11-13 06:50:15,825 - WARNING - 404 GET /.env.nexkit-missing-99731935c2937f663bf0804fb454fa0c (172.18.0.1) 0.36ms",
        "2026-11-13 19:50:03,049 404 GET /.svn/wc.db (172.18.0.1) 0.64ms",
        "2026-11-13 10:00:00,500 - INFO - 200 GET / (172.18.0.1) 12.00ms",
        "2026-11-13 10:00:00,123 - INFO - loaded with errors='coerce'",
        "2026-11-13 10:00:00,123 - WARNING - 403 POST /login (172.18.0.1) 1.00ms",
    ],
    ids=[
        "warning_404_wp_scanner",
        "plain_404_env_probe",
        "warning_404_git_probe",
        "plain_404_style_php",
        "plain_404_session_properties",
        "timestamp_millis_404",
        "warning_404_long_path",
        "plain_404_svn_probe",
        "timestamp_millis_500_info_200",
        "lowercase_errors_word",
        "warning_403_post",
    ],
)
def test_client_errors_and_noise_do_not_alert(line):
    # Arrange: line is a 4xx / non-error / timestamp-digit case
    # Act
    matched = _alerts(line)
    # Assert
    assert not matched


@pytest.mark.parametrize(
    "line",
    [
        "2026-11-13 10:00:00,123 - ERROR - Failed to fetch forecasts from API",
        "2026-11-13 10:00:00,123 - CRITICAL - dashboard cannot start",
        "ERROR:tornado.application:Uncaught exception GET / (172.18.0.1)",
        "Traceback (most recent call last):",
        "ValueError: Exception raised in render",
        "2026-11-13 10:00:00,123 500 GET /dashboard (172.18.0.1) 3.10ms",
        "2026-11-13 10:00:00,123 - ERROR - 502 GET /api (172.18.0.1) 3.10ms",
    ],
    ids=[
        "app_error_level",
        "app_critical_level",
        "tornado_stdlib_error",
        "python_traceback",
        "exception_keyword",
        "http_500_access_line",
        "error_level_with_502",
    ],
)
def test_server_side_failures_alert(line):
    # Arrange: line is a real server-side failure
    # Act
    matched = _alerts(line)
    # Assert
    assert matched
