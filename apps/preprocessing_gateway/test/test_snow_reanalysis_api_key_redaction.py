"""
Unit tests for the PREPG-015 API-key redaction sites in
snow_data_operational.py, snow_data_renalysis.py, and
get_era5_reanalysis_data.py.

These are the three sites added in the second PREPG-015 pass, additional
to the five already covered in Quantile_Mapping_OP.py (see
test_transport_retry.py::TestCallSiteRedaction and ::TestRedactApiKey).
A separate module rather than growing test_transport_retry.py, since these
are three different modules with their own import/mocking setup and none
of them touch Quantile_Mapping_OP.py.

Note on logging vs capsys: each module in this package configures its
root logger with a `logging.StreamHandler()` at *import* time, which
binds a direct reference to the real `sys.stderr` object as it exists at
import time -- before pytest's `capsys` fixture has swapped `sys.stderr`
for the current test. As a result `logger.error(...)` output is NOT
observed by `capsys.readouterr().err` (verified empirically: a probe
script confirmed `capsys` sees nothing from a pre-bound StreamHandler).
`caplog` is therefore the correct tool for asserting on logged content
here, and is the only assertion these tests make on it -- a
`capsys.err` assertion on these logger sites cannot fail regardless of
whether redaction actually happened, and a check that cannot fail is
worse than no check: it reads as coverage of the stderr path while
proving nothing. The uncaught-exception-to-real-stderr path (Python's
default excepthook on an exception this module does not catch) is a
real, still-open gap -- tracked separately as PREPG-017 -- and is not
exercised by these tests.

Run::

    cd apps
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_snow_reanalysis_api_key_redaction.py -v
"""

import logging
import os
import sys
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Mock the sapphire_dg_client package before importing any module under
# test -- it's a private package not installed in the test environment.
sys.modules["sapphire_dg_client"] = MagicMock()
sys.modules["sapphire_dg_client.client"] = MagicMock()
sys.modules["sapphire_dg_client.SapphireDGClient"] = MagicMock()
sys.modules["sapphire_dg_client.snow_model"] = MagicMock()

import get_era5_reanalysis_data as gerd
import snow_data_operational as sdo
import snow_data_renalysis as sdr

FAKE_KEY = "FAKE-KEY-DO-NOT-USE"


class TestSnowOperationalDgErrorRedacted:
    """Site: snow_data_operational.get_snow_data_operational's
    'Error getting snow data from Data Gateway' logger.error call."""

    def test_dg_exception_message_redacted(self, tmp_path, caplog):
        caplog.set_level(logging.ERROR)
        secret_message = (
            "Failed to get data from "
            f"api/calculations/snow?hru_code=19999&api_key={FAKE_KEY}: "
            '{"message": "Snow data not available", "success": false}'
        )
        mock_client = MagicMock()
        mock_client.get_operational.side_effect = Exception(secret_message)

        result = sdo.get_snow_data_operational(
            mock_client,
            hru="19999",
            variable="SWE",
            date="2024-06-15",
            dg_path=str(tmp_path / "dg"),
            save_path=str(tmp_path / "save"),
        )

        # Control flow preserved: the function still returns False here,
        # it does not raise or exit.
        assert result is False

        error_text = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
        assert FAKE_KEY not in error_text
        assert "api_key=***" in error_text
        # Diagnostics preserved: endpoint fragment and server response
        # text, not just the HRU code (which also appears in unrelated
        # info-level progress logs).
        assert "api/calculations/snow" in error_text
        assert "Snow data not available" in error_text

        # No capsys.err assertion here: this logger's StreamHandler
        # binds sys.stderr at import time, before capsys swaps it, so
        # such an assertion could never fail -- see module docstring
        # and PREPG-017 for the genuinely uncovered stderr path.

    def test_no_api_key_in_exception_passed_through_unaffected(self, tmp_path, caplog):
        """A DG exception with no api_key in it logs normally -- the
        redaction call is a no-op, not a behaviour change."""
        caplog.set_level(logging.ERROR)
        mock_client = MagicMock()
        mock_client.get_operational.side_effect = Exception("connection timed out")

        result = sdo.get_snow_data_operational(
            mock_client,
            hru="19999",
            variable="SWE",
            date="2024-06-15",
            dg_path=str(tmp_path / "dg"),
            save_path=str(tmp_path / "save"),
        )

        assert result is False
        error_text = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
        assert "connection timed out" in error_text


class TestSnowReanalysisDgErrorRedacted:
    """Site: snow_data_renalysis.get_snow_data_reanalysis's
    'Error getting reanalysis data from Data Gateway' logger.error call.

    This site sits in a five-year batch loop in production, so a
    single unredacted occurrence would repeat many times per run --
    same fix, same test shape as the operational site above."""

    def test_dg_exception_message_redacted(self, tmp_path, caplog):
        caplog.set_level(logging.ERROR)
        secret_message = (
            "Failed to get data from "
            f"api/calculations/reanalysis?hru_code=19999&api_key={FAKE_KEY}: "
            '{"message": "Reanalysis data not available", "success": false}'
        )
        mock_client = MagicMock()
        mock_client.get_snow_reanalysis.side_effect = Exception(secret_message)

        result = sdr.get_snow_data_reanalysis(
            mock_client,
            hru="19999",
            variable="SWE",
            start_date="2020-01-01",
            end_date="2020-12-31",
            dg_path=str(tmp_path / "dg"),
            save_path=str(tmp_path / "save"),
        )

        assert result is False

        error_text = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
        assert FAKE_KEY not in error_text
        assert "api_key=***" in error_text
        assert "api/calculations/reanalysis" in error_text
        assert "Reanalysis data not available" in error_text

        # No capsys.err assertion here: see PREPG-017 note above.


class TestEra5ReanalysisControlMemberErrorRedacted:
    """Site: get_era5_reanalysis_data.main()'s control-member
    `client.era5_land.get_era5_land(...)` call, previously with no
    exception boundary at all -- an uncaught DG ValueError reached
    Python's default traceback handler and printed the raw key. Now
    wrapped in a minimal try/except that logs the redacted FULL
    TRACEBACK (not just str(e)) and exits 1: `except Exception` also
    catches bugs in our own code (TypeError, KeyError, AttributeError),
    and those need a real stack to debug, not a single redacted line.
    Matches the file's own log-then-exit(1) convention used a few lines
    above for the missing-API-key precondition, and the
    print(traceback.format_exc()) sites in Quantile_Mapping_OP.py."""

    def _env(self, tmp_path, monkeypatch):
        intermediate = tmp_path / "intermediate_data"
        dg_dir = intermediate / "dg_download"
        reanalysis_dir = intermediate / "reanalysis"
        models_dir = tmp_path / "models"
        for d in (dg_dir, reanalysis_dir, models_dir):
            d.mkdir(parents=True, exist_ok=True)

        env_vars = {
            "ieasyforecast_intermediate_data_path": str(intermediate),
            "ieasyhydroforecast_OUTPUT_PATH_DG": "dg_download",
            "ieasyhydroforecast_OUTPUT_PATH_REANALYSIS": "reanalysis",
            "ieasyhydroforecast_HRU_CONTROL_MEMBER": "19999",
            "ieasyhydroforecast_API_KEY_GATEAWAY": "FAKE-KEY-DO-NOT-USE",
            # Deliberately point at a path that does not exist, so
            # perform_qmapping is False and the (irrelevant to this
            # test) quantile-mapping params are never read.
            "ieasyhydroforecast_Q_MAP_PARAM_PATH": "qmap_params_missing",
            "ieasyhydroforecast_models_and_scalers_path": str(models_dir),
            "ieasyhydroforecast_reanalysis_START_DATE": "2020-01-01",
            "ieasyhydroforecast_reanalysis_END_DATE": "2020-12-31",
        }
        for k, v in env_vars.items():
            monkeypatch.setenv(k, v)

    def test_dg_exception_message_redacted_and_script_still_fails(
        self, tmp_path, monkeypatch, caplog
    ):
        caplog.set_level(logging.ERROR)
        self._env(tmp_path, monkeypatch)

        secret_message = (
            "Failed to get data from "
            f"api/calculations/era5?hru_code=19999&api_key={FAKE_KEY}: "
            '{"message": "ERA5 data not available", "success": false}'
        )
        mock_dg = MagicMock()
        mock_dg.era5_land.get_era5_land.side_effect = ValueError(secret_message)

        stack = ExitStack()
        stack.enter_context(patch("get_era5_reanalysis_data.sl.load_environment"))
        stack.enter_context(
            patch(
                "get_era5_reanalysis_data.sapphire_dg_client.client.SapphireDGClient",
                return_value=mock_dg,
            )
        )

        with stack:
            with pytest.raises(SystemExit) as exc_info:
                gerd.main()

        # The script must still fail on this error exactly as before.
        assert exc_info.value.code == 1

        error_text = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
        assert FAKE_KEY not in error_text
        assert "api_key=***" in error_text
        assert "api/calculations/era5" in error_text
        assert "ERA5 data not available" in error_text
        # The traceback must survive, redacted, not just the message:
        # a full stack is what makes a bug in our own code (not just a
        # DG ValueError) debuggable. A pass here must be because the
        # key was actually removed from a real traceback, not because
        # almost nothing was logged.
        assert "Traceback (most recent call last)" in error_text
        assert "ValueError" in error_text

        # No capsys.err assertion here: see PREPG-017 note in the
        # module docstring -- this logger's StreamHandler binds
        # sys.stderr at import time, before capsys swaps it, so such
        # an assertion could never fail.
