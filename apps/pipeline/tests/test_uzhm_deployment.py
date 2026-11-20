"""Uzbek gateway/LT routing and image selection regressions."""

import os
import subprocess

import pytest
from conftest import APPS_DIR, run_main


@pytest.mark.parametrize(
    "target,module",
    [
        ("preprocessing_gateway", "preprocessing_gateway"),
        ("maintenance:preprocessing_gateway", "preprocessing_gateway"),
        ("long-term-operational", "long_term_forecasting"),
        ("maintenance:long_term_forecasting", "long_term_forecasting"),
    ],
)
@pytest.mark.parametrize("org,enabled", [("uzhm", True), ("demo", False)])
def test_org_gates(synth_tree, target, module, org, enabled):
    env = {"ieasyhydroforecast_organization": org}
    if target == "maintenance:long_term_forecasting":
        env.update(lt_forecast_mode="month_1", LT_RECOVERY_DATE="2026-08-01")
    result = run_main(synth_tree, target, extra_env=env)
    out = result.stdout + result.stderr
    if target == "maintenance:long_term_forecasting" and not enabled:
        assert result.returncode != 0, out
        assert "not available for this deployment" in out
    else:
        assert result.returncode == 0, out
    calls = [line for line in synth_tree.calls() if f"module={module} " in line]
    assert bool(calls) is enabled, out
    if not enabled and target != "maintenance:long_term_forecasting":
        assert f"SKIP (not required for {org} org)" in out


@pytest.mark.parametrize("target", ["machine_learning", "maintenance:machine_learning"])
def test_ml_remains_skipped(synth_tree, target):
    result = run_main(synth_tree, target, extra_env={"ieasyhydroforecast_organization": "uzhm"})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "SKIP (not required for uzhm org)" in result.stdout + result.stderr
    assert not synth_tree.calls()


@pytest.mark.parametrize(
    "org,extra",
    [
        ("uzhm", ["prepgateway", "lt-forecasting"]),
        ("demo", []),
        ("kghm", ["prepgateway", "ml", "lt-forecasting", "conceptmod"]),
        ("tjhm", ["prepgateway", "ml", "lt-forecasting"]),
    ],
)
def test_image_selection(tmp_path, org, extra):
    calls = tmp_path / "calls"
    docker = tmp_path / "docker"
    docker.write_text('#!/bin/bash\nprintf "%s\\n" "$*" >> "$DOCKER_CALLS"\n')
    docker.chmod(0o755)
    result = subprocess.run(
        ["bash", str(APPS_DIR.parent / "bin/utils/pull_docker_images.sh"), "test"],
        env={
            **os.environ,
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "DOCKER_CALLS": str(calls),
            "ieasyhydroforecast_organization": org,
        },
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    expected = ["pythonbaseimage", "preprunoff", "linreg", "postprocessing", "rerun"] + extra
    assert calls.read_text().splitlines() == [
        f"pull mabesa/sapphire-{name}:test" for name in expected
    ]
