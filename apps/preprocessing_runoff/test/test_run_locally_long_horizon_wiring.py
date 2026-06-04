"""Static guard for local preprocessing runoff maintenance wiring."""

from pathlib import Path


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "apps" / "run_locally.sh").is_file():
            return parent
    raise FileNotFoundError("Could not locate apps/run_locally.sh")


def test_run_locally_preprunoff_maintenance_runs_long_horizon_writer():
    content = (_repo_root() / "apps" / "run_locally.sh").read_text()

    maintenance_start = content.index("run_maintenance_preprocessing_runoff()")
    next_function = content.index("run_maintenance_preprocessing_gateway()", maintenance_start)
    function_body = content[maintenance_start:next_function]

    assert "preprocessing_runoff.py -- --maintenance" in function_body
    assert "sync_long_horizon_hydrograph.py" in function_body
    assert function_body.index("preprocessing_runoff.py -- --maintenance") < function_body.index(
        "sync_long_horizon_hydrograph.py"
    )
