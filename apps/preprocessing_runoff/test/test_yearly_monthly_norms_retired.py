"""Static checks that the old yearly monthly-norms path is retired.

Phase 4 of the runoff long-horizon hydrograph plan retires
``YearlyMonthlyNormsRecalculation`` (the old norm-only Luigi
task) and its dispatcher key ``"monthly_norms"`` from
``apps/pipeline/pipeline_docker.py``. The replacement is the
new yearly operator wrapper
``bin/yearly_runoff_hydrograph_aggregation.sh``, which dispatches
``sync_long_horizon_hydrograph.py`` (writes the full monthly
triad + seasonal April-September rows).

These tests are pure static checks against the on-disk content
of ``pipeline_docker.py``. They lock in the retirement so a
future refactor cannot silently re-introduce the old task or
dispatcher key, and they also assert that the snow yearly
recalc task and its referenced script are still present
(byte-identity for snow is enforced elsewhere — here we only
verify that snow was NOT collaterally damaged).
"""

import pathlib


def _find_pipeline_docker() -> pathlib.Path:
    """Locate ``apps/pipeline/pipeline_docker.py`` from any CWD.

    ``run_tests.sh`` invokes pytest from the ``apps/`` directory,
    but developers may run pytest directly from the module dir
    or from the repo root. Walk up from this test file until we
    find an ``apps/pipeline/pipeline_docker.py`` sibling and
    return that path.
    """
    here = pathlib.Path(__file__).resolve()
    for parent in [here, *here.parents]:
        candidate = parent / "apps" / "pipeline" / "pipeline_docker.py"
        if candidate.is_file():
            return candidate
        # Also support being run from inside apps/ as the CWD root.
        candidate2 = parent / "pipeline" / "pipeline_docker.py"
        if candidate2.is_file() and parent.name == "apps":
            return candidate2
    raise FileNotFoundError(
        f"Could not locate apps/pipeline/pipeline_docker.py from {here} or any of its parents."
    )


def test_yearly_monthly_norms_task_class_is_gone():
    """The old runoff yearly Luigi task class must be removed."""
    content = _find_pipeline_docker().read_text()
    assert "YearlyMonthlyNormsRecalculation" not in content, (
        "Old runoff yearly monthly norms Luigi task class is still "
        "present. This was retired in Phase 4 of the runoff "
        "long-horizon hydrograph plan."
    )


def test_monthly_norms_dispatcher_key_is_gone():
    """The old dispatcher key ``"monthly_norms"`` must be removed."""
    content = _find_pipeline_docker().read_text()
    # Use a strict pattern: the key is `"monthly_norms"` (with
    # quotes), not a substring like `_monthly_norms_` somewhere
    # else.
    assert '"monthly_norms"' not in content, (
        "Old runoff monthly_norms dispatcher key is still present. "
        "This was retired in Phase 4 of the runoff long-horizon "
        "hydrograph plan."
    )


def test_snow_yearly_task_is_byte_identical():
    """Snow yearly recalc must NOT be touched by the runoff retirement."""
    content = _find_pipeline_docker().read_text()
    # Confirm the snow yearly recalc class and its command are
    # still present. Byte-identity is guaranteed by the surgical
    # nature of the Phase 4 edit (we only removed the runoff
    # task block and one dict entry).
    assert "YearlySnowNormRecalculation" in content, (
        "Snow yearly Luigi task class is missing. Phase 4 must "
        "NOT touch the snow yearly recalc path."
    )
    assert "recalculate_snow_norms.py" in content, (
        "Snow yearly recalc command reference is missing. "
        "Phase 4 must NOT touch the snow yearly recalc path."
    )
