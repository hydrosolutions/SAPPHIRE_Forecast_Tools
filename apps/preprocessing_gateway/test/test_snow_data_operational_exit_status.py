"""
Tests for PREPG-009: `snow_data_operational.main()` must not report
success when snow tasks fail.

Before this fix, `main()` looped over every HRU x variable task,
logged `Failed to get snow data for HRU ..., ...` per failure, then
unconditionally logged `Snow data processing complete (N tasks)` --
counting tasks *attempted*, not *succeeded* -- and fell off the end.
`if __name__ == "__main__": main()` discarded whatever `main()`
returned, so even a 6/6 failure exited 0 and looked like a PASS to
automation.

Decided contract (see
doc/plans/issues/archive/mid_prio_gi_draft_prepg_snow_task_failures_exit_zero.md):
a single non-zero aggregate exit status, no graded codes. Partial vs.
total failure is distinguished only in the log (named failed tasks +
succeeded/failed counts), never in the exit status.

These tests exercise the *actual entry point* by re-executing the
module's `if __name__ == "__main__":` guard with `runpy.run_path(...,
run_name="__main__")`, asserting the real process status
(`SystemExit`), not `main()`'s return value. Calling `sdo.main()`
directly and wrapping it in `sys.exit(...)` from the test itself would
NOT catch a regression where the file's own `__main__` block still
discards the return value -- the test would be asserting its own
wrapping, not the script's. Two traps this closes:

1. A regression where 1 of N tasks fails but the process still exits
   0 would satisfy a total-failure-only test.
2. Asserting `main() == 1` (or re-wrapping it in the test) can pass
   while the real script still discards `main()`'s return and exits
   0, because `__main__` in the file is what matters.

Only external boundaries are mocked (the Data Gateway snow client);
`get_snow_data_operational`'s own success/failure decision is exactly
the thing under test, so it runs for real via a controlled fake client
that succeeds or fails per (HRU, variable) task.

Run::

    cd apps
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_snow_data_operational_exit_status.py -v
"""

import os
import runpy
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Mock the sapphire_dg_client package before importing any module under
# test -- it's a private package not installed in the test environment.
sys.modules["sapphire_dg_client"] = MagicMock()
sys.modules["sapphire_dg_client.client"] = MagicMock()
sys.modules["sapphire_dg_client.SapphireDGClient"] = MagicMock()
sys.modules["sapphire_dg_client.snow_model"] = MagicMock()

import setup_library  # noqa: E402
import snow_data_operational as sdo  # noqa: E402

# Two HRUs x three variables = six tasks, matching the 6-task scenario
# recorded in the issue.
TEST_HRUS = ["19999", "28888"]
TEST_VARS = ["SWE", "HS", "RoF"]
TOTAL_TASKS = len(TEST_HRUS) * len(TEST_VARS)
# The order main() iterates in: HRU outer loop, variable inner loop.
TASK_ORDER = [(hru, var) for hru in TEST_HRUS for var in TEST_VARS]


def _make_dg_snow_csv(code: str, var_name: str) -> pd.DataFrame:
    """Minimal 4-header-row DG snow CSV for one code, parseable by
    `dg_utils.transform_snow_data`."""
    dates = [(datetime.today() - timedelta(days=d)).strftime("%d.%m.%Y") for d in range(3, -1, -1)]
    header_rows = [["header", "meta"] for _ in range(4)]
    data_rows = [[d, 10.0 + i] for i, d in enumerate(dates)]
    return pd.DataFrame(header_rows + data_rows, columns=["Unnamed: 0", code])


def _make_fake_dg_client(fail_keys, dg_dir):
    """Fake Data Gateway snow client. `get_operational` fails for any
    (hru, variable) in `fail_keys` with a plain (non-gap) error, and
    otherwise writes a valid CSV and returns its path."""
    call_log = []

    def get_operational(hru_code, date, parameter, directory):
        call_log.append((hru_code, parameter))
        if (hru_code, parameter) in fail_keys:
            raise RuntimeError(f"synthetic failure for HRU {hru_code}, {parameter}")
        df = _make_dg_snow_csv(hru_code, parameter)
        out_path = os.path.join(directory, f"{hru_code}_{parameter}.csv")
        df.to_csv(out_path, index=False)
        return out_path

    fake_client = MagicMock()
    fake_client.get_operational.side_effect = get_operational
    return fake_client, call_log


@pytest.fixture()
def snow_main_env(tmp_path, monkeypatch):
    """Minimal environment for `snow_data_operational.main()` to reach
    its task loop without touching the real filesystem or network."""
    intermediate = tmp_path / "intermediate"
    intermediate.mkdir()

    env_vars = {
        "ieasyhydroforecast_API_KEY_GATEAWAY": "test-api-key-123",
        "SAPPHIRE_DG_HOST": "https://dg.example.com",
        "ieasyforecast_intermediate_data_path": str(intermediate),
        "ieasyhydroforecast_OUTPUT_PATH_DG": "dg_download",
        "ieasyhydroforecast_OUTPUT_PATH_SNOW": "snow",
        "ieasyhydroforecast_HRU_SNOW_DATA": ",".join(TEST_HRUS),
        "ieasyhydroforecast_SNOW_VARS": ",".join(TEST_VARS),
        # Skip real API writes entirely (dg_utils.write_snow_to_api
        # returns False early) -- out of scope for PREPG-009, see the
        # module docstring above.
        "SAPPHIRE_API_ENABLED": "false",
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)

    return {"tmp_path": tmp_path, "dg_dir": str(intermediate / "dg_download")}


def _run_real_entry_point(fail_keys, dg_dir, monkeypatch):
    """Execute the module's own `if __name__ == "__main__":` guard via
    runpy, so the test exercises exactly what the process does, not a
    re-implementation of it.

    `runpy.run_path` re-executes `snow_data_operational.py`'s top level
    fresh, including its own `from sapphire_dg_client import
    snow_model` -- which resolves against whatever object currently
    sits at `sys.modules["sapphire_dg_client"]`. Other test files in
    this suite also stub that entry at collection time, so by the time
    a test runs, it may no longer be the same object `sdo.snow_model`
    was bound to at `sdo`'s own first import. Pin a fresh, known mock
    into `sys.modules` right here (monkeypatch reverts it after the
    test) so the re-executed script's fresh import sees exactly the
    fake client this test configured, regardless of collection order.
    """
    fake_client, call_log = _make_fake_dg_client(fail_keys, dg_dir)
    fresh_dg_client_pkg = MagicMock()
    fresh_dg_client_pkg.snow_model.SapphireSnowModelClient.return_value = fake_client
    monkeypatch.setitem(sys.modules, "sapphire_dg_client", fresh_dg_client_pkg)

    with (
        patch.object(setup_library, "load_environment"),
        pytest.raises(SystemExit) as exc_info,
    ):
        runpy.run_path(sdo.__file__, run_name="__main__")
    return exc_info.value.code, call_log


class TestEntryPointExitStatus:
    """Parameterised over all-succeed / partial-failure / all-fail,
    asserting the real process exit status."""

    @pytest.mark.parametrize(
        "fail_indices,expect_nonzero",
        [
            pytest.param([], False, id="all_succeed"),
            pytest.param([0], True, id="partial_failure_first_task"),
            pytest.param(list(range(TOTAL_TASKS)), True, id="all_fail"),
        ],
    )
    def test_process_exit_status(self, snow_main_env, fail_indices, expect_nonzero, monkeypatch):
        fail_keys = {TASK_ORDER[i] for i in fail_indices}
        exit_code, call_log = _run_real_entry_point(fail_keys, snow_main_env["dg_dir"], monkeypatch)

        # All six tasks must run regardless of an earlier failure --
        # the loop must not abort on first failure.
        assert len(call_log) == TOTAL_TASKS
        assert set(call_log) == set(TASK_ORDER)

        if expect_nonzero:
            assert exit_code not in (0, None), (
                f"Expected a non-zero exit status for fail_keys={fail_keys!r}, got {exit_code!r}"
            )
        else:
            assert exit_code in (0, None), (
                f"Expected exit status 0 for an all-succeed run, got {exit_code!r}"
            )

    def test_all_tasks_run_even_with_early_failure(self, snow_main_env, monkeypatch):
        """One bad HRU/variable must not prevent the others being
        fetched -- the loop must run every task regardless of an
        earlier failure."""
        fail_keys = {TASK_ORDER[0], TASK_ORDER[2]}
        exit_code, call_log = _run_real_entry_point(fail_keys, snow_main_env["dg_dir"], monkeypatch)

        assert len(call_log) == TOTAL_TASKS
        assert set(call_log) == set(TASK_ORDER)
        assert exit_code not in (0, None)


class TestSummaryLogging:
    """The completion summary must name succeeded/failed counts and,
    on failure, which tasks failed.

    `caplog` cannot be used here: the module's own top-level setup does
    `logger.handlers = []` on every (re-)execution, which strips
    whatever handler `caplog` attached to the root logger before
    `runpy.run_path` re-executes the file. The module's `console_handler`
    writes to `sys.stderr`, so `capsys` (which pytest's default capture
    already substitutes before that handler is constructed) sees the
    same text an operator or automation reading the process's stderr
    would.
    """

    def test_all_succeed_summary_and_exit(self, snow_main_env, capsys, monkeypatch):
        exit_code, call_log = _run_real_entry_point(set(), snow_main_env["dg_dir"], monkeypatch)
        stderr = capsys.readouterr().err

        assert len(call_log) == TOTAL_TASKS
        assert exit_code in (0, None)

        assert f"{TOTAL_TASKS}/{TOTAL_TASKS} succeeded" in stderr, (
            f"Expected a full-success summary line, got:\n{stderr}"
        )
        # A fully successful run must not log any per-task failure line.
        assert "Failed to get snow data for HRU" not in stderr

    def test_partial_failure_summary_names_failed_tasks(self, snow_main_env, capsys, monkeypatch):
        # Fail HRU 19999/HS (task index 1) and HRU 28888/RoF (task
        # index 5); the rest succeed -> 4/6 succeeded.
        fail_keys = {("19999", "HS"), ("28888", "RoF")}
        exit_code, call_log = _run_real_entry_point(fail_keys, snow_main_env["dg_dir"], monkeypatch)
        stderr = capsys.readouterr().err

        assert len(call_log) == TOTAL_TASKS
        assert exit_code not in (0, None)

        assert "4/6 succeeded" in stderr, f"Expected '4/6 succeeded' in summary, got:\n{stderr}"
        # The failed tasks must be individually named somewhere in the
        # log output (per-task error line and/or the aggregate list).
        assert "19999" in stderr and "HS" in stderr
        assert "28888" in stderr and "RoF" in stderr

    def test_all_fail_summary(self, snow_main_env, capsys, monkeypatch):
        exit_code, call_log = _run_real_entry_point(
            set(TASK_ORDER), snow_main_env["dg_dir"], monkeypatch
        )
        stderr = capsys.readouterr().err

        assert len(call_log) == TOTAL_TASKS
        assert exit_code not in (0, None)

        assert "0/6 succeeded" in stderr, f"Expected '0/6 succeeded' in summary, got:\n{stderr}"
