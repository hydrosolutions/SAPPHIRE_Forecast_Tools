"""Dedicated recovery shell contract, with Docker and tunnel boundaries faked."""

import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]


def run_wrapper(tmp_path, settings="", fail="", locked=False, reuse=True, platform="Linux"):
    # Copy the real wrapper but replace common helpers: no network/tunnel jobs.
    script = tmp_path / "run_latest_forecast_recovery.sh"
    script.write_text((ROOT / "bin" / script.name).read_text())
    utils = tmp_path / "utils"
    utils.mkdir()
    (utils / "common_functions.sh").write_text("""
read_configuration() {
    source "$1"
    ieasyhydroforecast_data_root_dir="$TEST_ROOT"
    ieasyhydroforecast_env_file_path=/data/config/.env_test
    ieasyhydroforecast_backend_docker_image_tag=test-tag
}
establish_ssh_tunnel() { echo establish >> "$TEST_EVENTS"; }
cleanup() { echo cleanup >> "$TEST_EVENTS"; }
""")
    config = tmp_path / "env with spaces"
    config.write_text(
        """ieasyhydroforecast_run_ML_models=true
SAPPHIRE_TEST_ENV=false
SAPPHIRE_API_ENABLED=true
ML_FAILED_ISSUE_RECOVERY_ENABLED=true
ieasyforecast_configuration_path=../../../data/config
ieasyforecast_intermediate_data_path=../../../data/intermediate_data
ieasyhydroforecast_available_ML_models=TFT,TIDE,TSMIXER
"""
        + settings
    )
    stubs = tmp_path / "stubs"
    stubs.mkdir()
    uname = stubs / "uname"
    uname.write_text(f"#!/bin/bash\necho {platform}\n")
    uname.chmod(0o755)
    docker = stubs / "docker"
    docker.write_text("""#!/usr/bin/env python3
import json, os, sys
with open(os.environ["TEST_CALLS"], "a") as f:
    f.write(json.dumps(sys.argv[1:]) + "\\n")
args = sys.argv[1:]
if "--handoff" in args:
    from pathlib import Path
    target = args[args.index("--handoff") + 1]
    path = Path(os.environ["TEST_ROOT"] + target)
    assert path.is_file(), "handoff must exist before container startup"
    assert path.stat().st_size == 0, "fake producer must not receive old authorization"
fail = os.environ.get("TEST_FAIL")
sys.exit(7 if fail and any(fail == arg for arg in args) else 0)
""")
    docker.chmod(0o755)
    date = stubs / "date"
    date.write_text('#!/bin/bash\necho date >> "$TEST_EVENTS"\necho 2026-11-17\n')
    date.chmod(0o755)
    lock = tmp_path / "data/intermediate_data/.latest_forecast_recovery.lock"
    if locked:
        lock.mkdir(parents=True)
    env = dict(
        os.environ,
        TEST_ROOT=str(tmp_path),
        TEST_CALLS=str(tmp_path / "calls"),
        TEST_EVENTS=str(tmp_path / "events"),
        TEST_FAIL=fail,
        PATH=f"{stubs}:{os.environ['PATH']}",
    )
    result = subprocess.run(
        ["bash", str(script), str(config)] + (["--reuse-existing-tunnel"] if reuse else []),
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    calls = (
        [json.loads(line) for line in (tmp_path / "calls").read_text().splitlines()]
        if (tmp_path / "calls").exists()
        else []
    )
    events = (tmp_path / "events").read_text().splitlines()
    return result, calls, events, lock


@pytest.mark.parametrize("fail", ["", "TFT", "recover_latest_periods.py"])
def test_all_models_then_consumer_even_after_failure(tmp_path, fail):
    result, calls, events, lock = run_wrapper(tmp_path, fail=fail)
    assert result.returncode == int(bool(fail)), result.stderr
    assert len(calls) == 4
    assert [call[call.index("--models") + 1] for call in calls[:3]] == ["TFT", "TIDE", "TSMIXER"]
    for call in calls:
        assert call[0] == "run"
        assert call[call.index("--forecast-date") + 1] == "2026-11-17"
        assert f"{tmp_path}/data/config:/data/config:ro" in call
        assert f"{tmp_path}/data/intermediate_data:/data/intermediate_data:rw" in call
        assert "ieasyhydroforecast_env_file_path=/data/config/.env_test" in call
    assert all("SAPPHIRE_PREDICTION_MODE=BOTH" in call for call in calls)
    assert all(
        "--latest" in call and "--execute" in call and "--configured-stations" in call
        for call in calls[:3]
    )
    assert "recover_latest_periods.py" in calls[-1]
    assert events == ["date"]
    assert not lock.exists()


@pytest.mark.parametrize(
    "setting",
    [
        "ieasyhydroforecast_run_ML_models=false",
        "SAPPHIRE_API_ENABLED=false",
        "ML_FAILED_ISSUE_RECOVERY_ENABLED=false",
        "SAPPHIRE_TEST_ENV=True",
    ],
)
def test_disabled_paths_are_benign(tmp_path, setting):
    result, calls, _, _ = run_wrapper(tmp_path, settings=setting + "\n")
    assert result.returncode == 0
    assert not calls


def test_locked_run_never_removes_existing_lock_or_containers(tmp_path):
    result, calls, _, lock = run_wrapper(tmp_path, locked=True)
    assert result.returncode == 1
    assert not calls
    assert lock.exists()


def test_unsupported_model_and_standalone_tunnel(tmp_path):
    result, calls, events, _ = run_wrapper(
        tmp_path, settings="ieasyhydroforecast_available_ML_models='UNKNOWN, tFt '\n", reuse=False
    )
    assert result.returncode == 0
    assert len(calls) == 2
    assert "unsupported" in result.stdout
    assert events == ["date", "establish", "cleanup"]


@pytest.mark.parametrize("setting", ["", "SAPPHIRE_API_URL=http://localhost:8000\n"])
def test_macos_localhost_api_override(tmp_path, setting, monkeypatch):
    monkeypatch.delenv("SAPPHIRE_API_URL", raising=False)
    result, calls, _, _ = run_wrapper(tmp_path, settings=setting, platform="Darwin")
    assert result.returncode == 0
    assert all("SAPPHIRE_API_URL=http://host.docker.internal:8000" in call for call in calls)


@pytest.mark.parametrize("value", ["0", "no", "", "garbage"])
def test_api_requires_explicit_true(tmp_path, value):
    result, calls, _, _ = run_wrapper(tmp_path, settings=f"SAPPHIRE_API_ENABLED={value}\n")
    assert result.returncode == 0
    assert not calls


@pytest.mark.parametrize("setting", ["unset SAPPHIRE_API_ENABLED\n", "SAPPHIRE_API_ENABLED=tRuE\n"])
def test_api_unset_default_and_case_insensitive_true_enable(tmp_path, setting):
    result, calls, _, _ = run_wrapper(tmp_path, settings=setting)
    assert result.returncode == 0
    assert len(calls) == 4


@pytest.mark.parametrize("fail", ["", "recover_latest_forecasts.py", "recover_latest_long_term.py"])
def test_lt_runs_independently_after_ml_disabled_and_finalizes_failure(tmp_path, fail):
    result, calls, events, lock = run_wrapper(
        tmp_path,
        settings="ieasyhydroforecast_run_ML_models=false\nieasyhydroforecast_ml_long_term_supported_modes=month_0\n",
        fail=fail,
    )
    assert result.returncode == int(bool(fail)), result.stderr
    assert len(calls) == 2
    assert "recover_latest_forecasts.py" in calls[0]
    assert "recover_latest_long_term.py" in calls[1]
    handoffs = [call[call.index("--handoff") + 1] for call in calls]
    assert handoffs[0] == handoffs[1] and handoffs[0].startswith(
        "/data/intermediate_data/.lt-recovery."
    )
    assert f"{tmp_path}/data/config:/data/config" in calls[0]  # LT config synchronization needs RW.
    assert f"{tmp_path}/data/config:/data/config:ro" in calls[1]
    assert not list((tmp_path / "data/intermediate_data").glob(".lt-recovery.*"))
    assert not lock.exists() and events == ["date"]


def test_lt_kill_switch_preserves_ml(tmp_path):
    result, calls, _, _ = run_wrapper(
        tmp_path,
        settings="ieasyhydroforecast_ml_long_term_supported_modes=month_0\nLT_FAILED_ISSUE_RECOVERY_ENABLED=false\n",
    )
    assert result.returncode == 0 and len(calls) == 4


@pytest.mark.parametrize("fail", ["", "TFT", "recover_latest_periods.py"])
def test_both_enabled_share_date_and_lt_continues_after_ml_failure(tmp_path, fail):
    result, calls, events, lock = run_wrapper(
        tmp_path,
        settings="ieasyhydroforecast_ml_long_term_supported_modes=month_0\n",
        fail=fail,
    )
    assert result.returncode == int(bool(fail)), result.stderr
    assert len(calls) == 6
    assert [call[call.index("--models") + 1] for call in calls[:3]] == ["TFT", "TIDE", "TSMIXER"]
    assert "recover_latest_periods.py" in calls[3]
    assert "recover_latest_forecasts.py" in calls[4]
    assert "recover_latest_long_term.py" in calls[5]
    assert all(call[call.index("--forecast-date") + 1] == "2026-11-17" for call in calls)
    assert events == ["date"] and not lock.exists()
