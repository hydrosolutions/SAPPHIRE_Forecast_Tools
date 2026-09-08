"""Static guard for local preprocessing runoff maintenance wiring."""

from pathlib import Path


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "apps" / "run_locally.sh").is_file():
            return parent
    raise FileNotFoundError("Could not locate apps/run_locally.sh")


def _function_body() -> str:
    content = (_repo_root() / "apps" / "run_locally.sh").read_text()
    maintenance_start = content.index("run_maintenance_preprocessing_runoff()")
    next_function = content.index("run_maintenance_preprocessing_gateway()", maintenance_start)
    return content[maintenance_start:next_function]


def test_run_locally_preprunoff_maintenance_runs_long_horizon_writer():
    function_body = _function_body()

    assert "preprocessing_runoff.py -- --maintenance" in function_body
    assert "sync_long_horizon_hydrograph.py" in function_body
    assert function_body.index("preprocessing_runoff.py -- --maintenance") < function_body.index(
        "sync_long_horizon_hydrograph.py"
    )
    assert "lt_rc -eq 2" in function_body
    assert "Long-horizon hydrograph sync produced no records" in function_body
    assert "lt_rc -eq 5" in function_body
    assert "Long-horizon hydrograph sync had API read/write failure(s)" in function_body
    assert "lt_rc -eq 4" in function_body
    assert "lt_rc -eq 6" in function_body
    assert "Long-horizon hydrograph sync had SDK norm lookup failure(s)" in function_body
    assert function_body.index("lt_rc -eq 2") < function_body.index("lt_rc -eq 5")
    assert function_body.index("lt_rc -eq 5") < function_body.index("lt_rc -eq 4")
    assert function_body.index("lt_rc -eq 4") < function_body.index("lt_rc -eq 6")
    assert "rc=$lt_rc" in function_body[function_body.index("lt_rc -eq 5") :]


def test_lt_rc_four_is_informational_and_records_no_row():
    """INFRA-044 C4: lt_rc=4 (PARTIAL SDK norm-lookup failure) is
    informational -- INFO log, no `record_result` call, and module `rc`
    is not set from it (stays 0).
    """
    function_body = _function_body()

    idx_4 = function_body.index("[ $lt_rc -eq 4 ]")
    idx_6 = function_body.index("[ $lt_rc -eq 6 ]")
    block_4 = function_body[idx_4:idx_6]

    assert "log INFO" in block_4
    assert "log ERROR" not in block_4
    assert "record_result" not in block_4, (
        "lt_rc=4 must record no result row at all (INFRA-044 owner decision)"
    )
    assert "rc=$lt_rc" not in block_4, "lt_rc=4 must leave the module rc at 0"


def test_lt_rc_six_matches_historical_exit_four_fail_handling():
    """INFRA-044 C4: lt_rc=6 (TOTAL SDK norm-lookup failure) gets
    byte-for-byte today's (pre-INFRA-044) exit-4 handling: the exact same
    two ERROR log calls and the exact same `record_result` call shape as
    the historical exit-4 branch used, verbatim (only the surrounding
    comments changed), and module `rc` NOT set from it (stays 0, since the
    sub-step's own FAIL row -- not the surrounding module -- carries the
    failure). It must be an explicit branch, not the generic
    `elif [ $lt_rc -ne 0 ]` catch-all (which sets `rc=$lt_rc` and would
    newly fail the whole maintenance module for lt_rc=6).

    Pinning the exact message text (not just call counts/substrings) is
    the point of the "byte-for-byte" claim in this test's name: a looser
    assertion would still pass if either ERROR message's wording changed
    substantially.
    """
    function_body = _function_body()

    idx_6 = function_body.index("[ $lt_rc -eq 6 ]")
    idx_catchall = function_body.index("[ $lt_rc -ne 0 ]")
    assert idx_6 < idx_catchall, "the lt_rc=6 branch must precede (and thus pre-empt) the catch-all"
    block_6 = function_body[idx_6:idx_catchall]

    assert block_6.count("log ERROR") == 2, (
        "the historical exit-4 FAIL handling logged exactly two ERROR lines"
    )
    assert 'log ERROR "Long-horizon hydrograph sync had SDK norm lookup failure(s)"' in block_6, (
        "first ERROR line must match the historical exit-4 wording verbatim"
    )
    assert (
        'log ERROR "  Counts are in the LONG-HORIZON RUN SUMMARY block near the end of '
        "${CURRENT_MODULE_LOG} -- also tailed under the 'preprocessing_runoff (long-horizon "
        "sync)' row below.\"" in block_6
    ), "second ERROR line must match the historical exit-4 wording verbatim"
    record_result_line = next(
        line
        for line in block_6.splitlines()
        if line.strip().startswith('record_result "preprocessing_runoff (long-horizon sync)"')
    )
    assert record_result_line.strip() == (
        'record_result "preprocessing_runoff (long-horizon sync)" "FAIL" "$lt_elapsed" '
        '"$CURRENT_MODULE_LOG"'
    ), "record_result call shape (label, status, elapsed, log path) must match verbatim"
    assert "rc=$lt_rc" not in block_6, (
        "lt_rc=6 must not fail the surrounding maintenance module (rc stays 0); "
        "only the long-horizon sync's own row is FAIL"
    )
