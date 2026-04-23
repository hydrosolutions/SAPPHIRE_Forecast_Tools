"""Smoke tests for sync_monthly_norms.py CLI entry point.

Tests cover happy-path filtering, zero-SDK-sites edge case, RuntimeError
propagation from the library, --dry-run mode, --current-year passthrough,
and --help.

All external boundaries (SDK init, library calls) are mocked.
"""

import logging
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Ensure the preprocessing_runoff directory is on sys.path so that the
# entry-point module can be imported directly.
PREPROC_RUNOFF_DIR = os.path.join(os.path.dirname(__file__), "..")
if PREPROC_RUNOFF_DIR not in sys.path:
    sys.path.insert(0, PREPROC_RUNOFF_DIR)

# Also ensure iEasyHydroForecast is on path (needed by setup_library import
# inside sync_monthly_norms).
IEHF_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast")
if IEHF_DIR not in sys.path:
    sys.path.insert(0, IEHF_DIR)

# Import the module under test once at collection time.
# All tests patch attributes on the already-imported module object.
import sync_monthly_norms as _mod  # noqa: E402

# ---------------------------------------------------------------------------
# Patch target constants (all relative to the sync_monthly_norms namespace)
# ---------------------------------------------------------------------------
_MOD = "sync_monthly_norms"
_PATCH_LOAD_ENV = f"{_MOD}.sl.load_environment"
_PATCH_SDK = f"{_MOD}.IEasyHydroHFSDK"
_PATCH_GET_SITES = f"{_MOD}.get_all_forecast_sites_from_HF_SDK"
_PATCH_MANUAL = f"{_MOD}._get_manual_site_codes"
_PATCH_WRITE = f"{_MOD}.write_month_hydrograph_data"


# ---------------------------------------------------------------------------
# Helper: run main() with argv, return exit code
# ---------------------------------------------------------------------------


def _run_main(argv: list[str], extra_patches: list | None = None) -> int:
    """
    Run sync_monthly_norms.main() with sys.argv = argv.

    Returns the integer exit code (0 for normal return, 0/nonzero for
    SystemExit).  Never re-raises SystemExit.

    *extra_patches* is a list of already-entered patch context managers
    that the caller has set up; this helper adds the common infrastructure
    patches on top.
    """
    with patch("sys.argv", [f"{_MOD}.py"] + argv):
        try:
            _mod.main()
            return 0
        except SystemExit as exc:
            return int(exc.code) if exc.code is not None else 0


# ---------------------------------------------------------------------------
# Shared mock SDK factory
# ---------------------------------------------------------------------------


def _make_sdk() -> MagicMock:
    return MagicMock(name="IEasyHydroHFSDK_instance")


# ---------------------------------------------------------------------------
# Test 1: happy path — one manual site filtered out, library called correctly
# ---------------------------------------------------------------------------


class TestMainHappyPath:
    """main() filters manual sites and calls the library with sdk-only codes."""

    def test_write_called_with_sdk_only_codes(self):
        """write_month_hydrograph_data receives only the SDK sites."""
        mock_sdk_inst = _make_sdk()

        with (
            patch(_PATCH_LOAD_ENV),
            patch(_PATCH_SDK, return_value=mock_sdk_inst),
            patch(
                _PATCH_GET_SITES,
                return_value=(MagicMock(), ["10001", "16022", "16230"], [1, 2, 3]),
            ),
            patch(_PATCH_MANUAL, return_value=["10001"]),
            patch(_PATCH_WRITE, return_value=True) as mock_write,
        ):
            exit_code = _run_main([])

        assert exit_code == 0, f"Expected exit code 0, got {exit_code}"
        mock_write.assert_called_once()

        call_args = mock_write.call_args
        code_list_arg = call_args.args[0] if call_args.args else call_args.kwargs["code_list"]
        assert "10001" not in code_list_arg, "Manual site must be filtered out"
        assert "16022" in code_list_arg
        assert "16230" in code_list_arg

    def test_manual_site_info_logged(self, caplog):
        """An info-level log must be emitted for each skipped manual site."""
        mock_sdk_inst = _make_sdk()

        with (
            patch(_PATCH_LOAD_ENV),
            patch(_PATCH_SDK, return_value=mock_sdk_inst),
            patch(
                _PATCH_GET_SITES,
                return_value=(MagicMock(), ["10001", "16022"], [1, 2]),
            ),
            patch(_PATCH_MANUAL, return_value=["10001"]),
            patch(_PATCH_WRITE, return_value=True),
            caplog.at_level(logging.INFO),
        ):
            _run_main([])

        relevant = [r for r in caplog.records if "10001" in r.message]
        assert relevant, "Expected a log message mentioning the skipped manual site '10001'"
        # Must be info or below (not warning/error)
        for r in relevant:
            assert r.levelno <= logging.INFO, (
                f"Log for manual site should be INFO, got {r.levelname}"
            )


# ---------------------------------------------------------------------------
# Test 2: zero SDK sites after filtering → exit 2
# ---------------------------------------------------------------------------


class TestZeroSdkSites:
    """When all forecast sites are manual, exit code must be 2."""

    def test_exit_code_2_when_all_manual(self):
        mock_sdk_inst = _make_sdk()

        with (
            patch(_PATCH_LOAD_ENV),
            patch(_PATCH_SDK, return_value=mock_sdk_inst),
            patch(
                _PATCH_GET_SITES,
                return_value=(MagicMock(), ["10001", "10002"], [1, 2]),
            ),
            patch(_PATCH_MANUAL, return_value=["10001", "10002"]),
            patch(_PATCH_WRITE, return_value=False),
        ):
            exit_code = _run_main([])

        assert exit_code == 2, f"Expected exit code 2, got {exit_code}"

    def test_library_not_called_when_no_sdk_sites(self):
        """write_month_hydrograph_data must not be called when list is empty."""
        mock_sdk_inst = _make_sdk()

        with (
            patch(_PATCH_LOAD_ENV),
            patch(_PATCH_SDK, return_value=mock_sdk_inst),
            patch(
                _PATCH_GET_SITES,
                return_value=(MagicMock(), ["10001"], [1]),
            ),
            patch(_PATCH_MANUAL, return_value=["10001"]),
            patch(_PATCH_WRITE) as mock_write,
        ):
            _run_main([])

        mock_write.assert_not_called()


# ---------------------------------------------------------------------------
# Test 3: library raises RuntimeError → exit 1
# ---------------------------------------------------------------------------


class TestLibraryRuntimeError:
    """RuntimeError from write_month_hydrograph_data must produce exit code 1."""

    def test_exit_code_1_on_runtime_error(self):
        mock_sdk_inst = _make_sdk()

        with (
            patch(_PATCH_LOAD_ENV),
            patch(_PATCH_SDK, return_value=mock_sdk_inst),
            patch(
                _PATCH_GET_SITES,
                return_value=(MagicMock(), ["16022"], [1]),
            ),
            patch(_PATCH_MANUAL, return_value=[]),
            patch(_PATCH_WRITE, side_effect=RuntimeError("API unavailable")),
        ):
            exit_code = _run_main([])

        assert exit_code == 1, f"Expected exit code 1, got {exit_code}"

    def test_runtime_error_is_logged_not_swallowed(self, caplog):
        """The RuntimeError message must appear in logs, not be silently consumed."""
        mock_sdk_inst = _make_sdk()

        with (
            patch(_PATCH_LOAD_ENV),
            patch(_PATCH_SDK, return_value=mock_sdk_inst),
            patch(
                _PATCH_GET_SITES,
                return_value=(MagicMock(), ["16022"], [1]),
            ),
            patch(_PATCH_MANUAL, return_value=[]),
            patch(_PATCH_WRITE, side_effect=RuntimeError("API unavailable")),
            caplog.at_level(logging.ERROR),
        ):
            _run_main([])

        error_records = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert error_records, "Expected an ERROR-level log when RuntimeError is raised"


# ---------------------------------------------------------------------------
# Test 4: --dry-run flag
# ---------------------------------------------------------------------------


class TestDryRun:
    """--dry-run must print the site list and exit 0 without calling the library."""

    def test_dry_run_does_not_call_library(self):
        mock_sdk_inst = _make_sdk()

        with (
            patch(_PATCH_LOAD_ENV),
            patch(_PATCH_SDK, return_value=mock_sdk_inst),
            patch(
                _PATCH_GET_SITES,
                return_value=(MagicMock(), ["10001", "16022", "16230"], [1, 2, 3]),
            ),
            patch(_PATCH_MANUAL, return_value=["10001"]),
            patch(_PATCH_WRITE) as mock_write,
        ):
            exit_code = _run_main(["--dry-run"])

        assert exit_code == 0, f"Expected exit code 0 from dry-run, got {exit_code}"
        mock_write.assert_not_called()

    def test_dry_run_prints_sdk_site_list(self, capsys):
        """stdout must contain the filtered sdk_only_codes and current_year."""
        mock_sdk_inst = _make_sdk()

        with (
            patch(_PATCH_LOAD_ENV),
            patch(_PATCH_SDK, return_value=mock_sdk_inst),
            patch(
                _PATCH_GET_SITES,
                return_value=(MagicMock(), ["10001", "16022", "16230"], [1, 2, 3]),
            ),
            patch(_PATCH_MANUAL, return_value=["10001"]),
            patch(_PATCH_WRITE),
        ):
            _run_main(["--dry-run"])

        captured = capsys.readouterr()
        combined = captured.out + captured.err
        assert "16022" in combined, "Expected sdk site 16022 in dry-run output"
        assert "16230" in combined, "Expected sdk site 16230 in dry-run output"


# ---------------------------------------------------------------------------
# Test 5: --current-year passthrough
# ---------------------------------------------------------------------------


class TestCurrentYear:
    """--current-year=2020 must be forwarded to write_month_hydrograph_data."""

    def test_current_year_passed_to_library(self):
        mock_sdk_inst = _make_sdk()

        with (
            patch(_PATCH_LOAD_ENV),
            patch(_PATCH_SDK, return_value=mock_sdk_inst),
            patch(
                _PATCH_GET_SITES,
                return_value=(MagicMock(), ["16022"], [1]),
            ),
            patch(_PATCH_MANUAL, return_value=[]),
            patch(_PATCH_WRITE, return_value=True) as mock_write,
        ):
            _run_main(["--current-year", "2020"])

        mock_write.assert_called_once()
        call_kwargs = mock_write.call_args.kwargs
        call_args = mock_write.call_args.args

        # current_year may be positional (index 2) or keyword
        current_year = call_kwargs.get("current_year") or (
            call_args[2] if len(call_args) > 2 else None
        )
        assert current_year == 2020, f"Expected current_year=2020, got {current_year!r}"


# ---------------------------------------------------------------------------
# Test 6: --help exits 0 and prints usage
# ---------------------------------------------------------------------------


class TestHelp:
    """--help must exit 0 and print a usage message."""

    def test_help_exits_zero(self):
        with pytest.raises(SystemExit) as exc_info:
            with patch("sys.argv", [f"{_MOD}.py", "--help"]):
                _mod.main()
        assert exc_info.value.code == 0

    def test_help_prints_usage(self, capsys):
        try:
            with patch("sys.argv", [f"{_MOD}.py", "--help"]):
                _mod.main()
        except SystemExit:
            pass

        captured = capsys.readouterr()
        combined = captured.out + captured.err
        assert "usage" in combined.lower(), (
            f"Expected 'usage' in --help output, got: {combined[:300]!r}"
        )
