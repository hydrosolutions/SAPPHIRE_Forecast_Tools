"""
Unit tests for the Data Gateway transport-retry helper (PREPG-010) and
the API-key redaction helper (PREPG-015).

Covers `_call_with_transport_retry` and `_redact_api_key` in isolation
-- no client, no main(), no filesystem -- plus main()-level coverage of
the three PREPG-015 redaction call sites (control member, today
ensemble loop, yesterday fallback ensemble loop), each with its own
minimal, self-contained environment fixture. For main()-level coverage
of the transport-retry behaviour at those same three call sites, see
test_integration_preprocessing_gateway.py::TestTransportRetryMainLevel.

Run::

    cd apps
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_transport_retry.py -v
"""

import json
import logging
import os
import sys
import time
from contextlib import ExitStack
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "iEasyHydroForecast"))

# Mock the sapphire_dg_client before importing -- it's a private package
# not installed in the test environment.
sys.modules["sapphire_dg_client"] = MagicMock()
sys.modules["sapphire_dg_client.client"] = MagicMock()
sys.modules["sapphire_dg_client.SapphireDGClient"] = MagicMock()
sys.modules["sapphire_dg_client.snow_model"] = MagicMock()

import dg_utils
import Quantile_Mapping_OP as qm

# The package-wide autouse `_no_retry_sleep` fixture in conftest.py
# patches Quantile_Mapping_OP._retry_sleep to a no-op for every test in
# this package (PREPG-010) -- no per-file fixture needed here. Tests
# below that need to *observe* the pause (asserting it is invoked)
# monkeypatch _retry_sleep again inside the test body, which runs after
# that fixture's setup and wins for the duration of the test.


class TestCallWithTransportRetry:
    """Unit tests for _call_with_transport_retry."""

    def test_autouse_fixture_actually_disabled_the_retry_sleep(self):
        """Guards against conftest's `_no_retry_sleep` silently no-opping.

        That fixture only patches `_retry_sleep` if
        "Quantile_Mapping_OP" is already in sys.modules when it runs
        (see its docstring's LIMITATION) -- this file imports the
        module at top level, so the patch should always have taken
        effect here. If this assertion ever fails, every other test in
        this file that triggers a retry is sleeping for real.
        """
        assert qm._retry_sleep is not time.sleep

    def test_success_first_attempt_no_retry(self):
        """A download_fn that succeeds immediately is called exactly once."""
        calls = []

        def download_fn():
            calls.append(1)
            return "ok"

        result = qm._call_with_transport_retry(download_fn, context="unit test")
        assert result == "ok"
        assert len(calls) == 1

    def test_connection_error_retried_once_then_succeeds(self):
        """requests.exceptions.ConnectionError on attempt 1 is retried;
        attempt 2 succeeds -> exactly 2 calls, recovered result returned.

        Uses the class requests actually raises, not the builtin
        ConnectionError -- a test against the builtin would prove
        nothing about handler dispatch.
        """
        calls = []

        def download_fn():
            calls.append(1)
            if len(calls) == 1:
                raise requests.exceptions.ConnectionError("reset by peer")
            return "recovered"

        result = qm._call_with_transport_retry(download_fn, context="unit test")
        assert result == "recovered"
        assert len(calls) == 2

    def test_chunked_encoding_error_retried_separately(self):
        """ChunkedEncodingError -- a sibling class of ConnectionError,
        not a subclass -- is retried on the same terms."""
        calls = []

        def download_fn():
            calls.append(1)
            if len(calls) == 1:
                raise requests.exceptions.ChunkedEncodingError("truncated body")
            return "recovered"

        result = qm._call_with_transport_retry(download_fn, context="unit test")
        assert result == "recovered"
        assert len(calls) == 2

    def test_ssl_error_is_retried_same_as_connection_error(self):
        """SSLError subclasses ConnectionError and IS retried
        (deliberate, see PREPG-010) -- exhaustion after exactly 2 calls,
        so a future 'tightening' that re-adds an exclusion fails
        visibly."""
        calls = []

        def download_fn():
            calls.append(1)
            raise requests.exceptions.SSLError("handshake failure")

        with pytest.raises(requests.exceptions.SSLError):
            qm._call_with_transport_retry(download_fn, context="unit test")
        assert len(calls) == 2

    def test_proxy_error_is_retried_same_as_connection_error(self):
        """ProxyError also subclasses ConnectionError and is retried."""
        calls = []

        def download_fn():
            calls.append(1)
            raise requests.exceptions.ProxyError("proxy refused connection")

        with pytest.raises(requests.exceptions.ProxyError):
            qm._call_with_transport_retry(download_fn, context="unit test")
        assert len(calls) == 2

    def test_exhaustion_reraises_original_exception_unchanged(self):
        """An always-failing ConnectionError propagates with its
        original type, message, and identity after exactly 2 attempts --
        not converted to sys.exit, ValueError, or any other type."""
        calls = []
        original = requests.exceptions.ConnectionError(
            "('Connection aborted.', ConnectionResetError(54, 'Connection reset by peer'))"
        )

        def download_fn():
            calls.append(1)
            raise original

        with pytest.raises(requests.exceptions.ConnectionError) as exc_info:
            qm._call_with_transport_retry(download_fn, context="unit test")

        assert len(calls) == 2
        assert exc_info.value is original
        assert str(exc_info.value) == str(original)

    def test_a_third_attempt_would_fail_this_test(self):
        """An implementation that retries a 3rd time is wrong: verify no
        3rd call happens even when download_fn would succeed on it."""
        calls = []

        def download_fn():
            calls.append(1)
            if len(calls) < 3:
                raise requests.exceptions.ConnectionError("still resetting")
            return "should never be reached"

        with pytest.raises(requests.exceptions.ConnectionError):
            qm._call_with_transport_retry(download_fn, context="unit test")
        assert len(calls) == 2

    def test_non_matching_valueerror_not_retried(self):
        """A ValueError (e.g. the today->yesterday fallback trigger) is
        not in the retryable set: exactly 1 call, and it propagates
        immediately, unchanged."""
        calls = []

        def download_fn():
            calls.append(1)
            raise ValueError("Couldn't find any files for the given HRU code, date and models!")

        with pytest.raises(ValueError, match="Couldn't find any files"):
            qm._call_with_transport_retry(download_fn, context="unit test")
        assert len(calls) == 1

    def test_non_matching_generic_exception_not_retried(self):
        """A generic Exception (e.g. the control-member 'Operational
        data for HRU' message) is not retried either."""
        calls = []

        def download_fn():
            calls.append(1)
            raise Exception("Operational data for HRU 19999 not available")

        with pytest.raises(Exception, match="Operational data for HRU"):
            qm._call_with_transport_retry(download_fn, context="unit test")
        assert len(calls) == 1

    def test_sleep_invoked_once_between_attempts_with_hardcoded_duration(self, monkeypatch):
        """The pause is a single hard-coded duration, invoked once
        between the 2 attempts -- not a backoff schedule."""
        sleep_calls = []
        monkeypatch.setattr(qm, "_retry_sleep", lambda seconds: sleep_calls.append(seconds))

        calls = []

        def download_fn():
            calls.append(1)
            if len(calls) == 1:
                raise requests.exceptions.ConnectionError("reset")
            return "ok"

        qm._call_with_transport_retry(download_fn, context="unit test")
        assert sleep_calls == [qm._RETRY_SLEEP_SECONDS]

    def test_sleep_not_invoked_on_immediate_success(self, monkeypatch):
        sleep_calls = []
        monkeypatch.setattr(qm, "_retry_sleep", lambda seconds: sleep_calls.append(seconds))

        qm._call_with_transport_retry(lambda: "ok", context="unit test")
        assert sleep_calls == []

    def test_retry_attempt_count_is_hardcoded_at_two(self):
        """No env var / config surface exists for the attempt count."""
        assert qm._RETRY_MAX_ATTEMPTS == 2


class TestRetryLoggingHygiene:
    """PREPG-015: retry log lines must never carry the raw exception,
    endpoint, or URL -- the Data Gateway embeds the live API key in some
    error messages (sapphire_dg_client/client_base.py:55-60)."""

    def test_retry_warning_omits_raw_exception_text(self, caplog):
        caplog.set_level(logging.WARNING)
        calls = []
        secret_message = (
            "Bad gateway response for "
            "https://dg.example.com/api/v1/download?api_key=SUPER-SECRET-KEY-19999"
        )

        def download_fn():
            calls.append(1)
            if len(calls) == 1:
                raise requests.exceptions.ConnectionError(secret_message)
            return "ok"

        qm._call_with_transport_retry(download_fn, context="HRU 19999 model 3 date 2024-01-01")

        warning_text = "\n".join(
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        )
        assert "SUPER-SECRET-KEY-19999" not in warning_text
        assert "api_key" not in warning_text
        assert "https://dg.example.com" not in warning_text
        # Still useful: names the context and the exception class.
        assert "HRU 19999 model 3" in warning_text
        assert "ConnectionError" in warning_text

    def test_retry_warning_only_logged_when_a_retry_actually_happens(self, caplog):
        caplog.set_level(logging.WARNING)

        qm._call_with_transport_retry(lambda: "ok", context="no retry expected")
        assert not any("Transport fault" in r.getMessage() for r in caplog.records)

    def test_exhaustion_does_not_log_the_raw_exception_either(self, caplog):
        """On exhaustion the helper re-raises; it must not also log the
        raw exception text on the way out (that's left to the caller,
        which is responsible for its own redaction)."""
        caplog.set_level(logging.WARNING)
        secret_message = "leaked?api_key=SHOULD-NOT-APPEAR-IN-WARNING"

        def download_fn():
            raise requests.exceptions.ConnectionError(secret_message)

        with pytest.raises(requests.exceptions.ConnectionError):
            qm._call_with_transport_retry(download_fn, context="unit test")

        warning_text = "\n".join(
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        )
        assert "SHOULD-NOT-APPEAR-IN-WARNING" not in warning_text


class TestRedactApiKey:
    """Unit tests for `dg_utils.redact_api_key` (PREPG-015).

    Moved from `Quantile_Mapping_OP._redact_api_key` to
    `dg_utils.redact_api_key` (now public -- it is shared across
    modules: Quantile_Mapping_OP.py, snow_data_operational.py,
    snow_data_renalysis.py, get_era5_reanalysis_data.py).
    `Quantile_Mapping_OP.py` calls it as `dg_utils.redact_api_key(...)`
    at its five sites; these tests exercise the helper directly.

    Obviously-fake credential throughout (`FAKE-KEY-DO-NOT-USE`) --
    never a real-looking key, per CLAUDE.md. Covers both the regex
    pattern pass and the literal-value pass added for the ": "-inside-
    the-key defect (the helper reads
    `ieasyhydroforecast_API_KEY_GATEAWAY` from the environment at call
    time; tests set/unset it via `monkeypatch`, never a real .env).
    """

    FAKE_KEY = "FAKE-KEY-DO-NOT-USE"

    @pytest.fixture(autouse=True)
    def _no_ambient_live_key(self, monkeypatch):
        """Deterministic baseline: no live key in the environment unless
        a test explicitly sets one via `monkeypatch.setenv`. Without
        this, `redact_api_key`'s literal-value pass would silently pick
        up whatever `ieasyhydroforecast_API_KEY_GATEAWAY` happens to be
        set to in the developer's shell (it is not set in this repo's
        test env, but nothing should rely on that)."""
        monkeypatch.delenv("ieasyhydroforecast_API_KEY_GATEAWAY", raising=False)

    def test_key_last_terminated_by_colon_json_body_survives(self):
        """The observed shape: key is the last query param, followed by
        ': ' and the server's JSON body. A naive `api_key=[^&\\s]*`
        would run past the key and eat the colon plus the leading part
        of the JSON -- this pins that it doesn't."""
        message = (
            "Failed to get data from "
            "api/calculations/operational/template/RSMinerva?hru_code=19999"
            f"&start_date=2023-06-15&api_key={self.FAKE_KEY}: "
            '{"message": "Operational data for HRU 19999 is not available for this date", '
            '"success": false}'
        )
        redacted = dg_utils.redact_api_key(message)
        assert self.FAKE_KEY not in redacted
        assert "api_key=***" in redacted
        # The response text -- the reason these lines exist -- survives intact.
        assert (
            '{"message": "Operational data for HRU 19999 is not available for this date", '
            '"success": false}' in redacted
        )

    def test_key_followed_by_ampersand_next_param_survives(self):
        """The key is not guaranteed to be last -- a future endpoint
        could append a further query param after it."""
        message = f"endpoint?hru_code=19999&api_key={self.FAKE_KEY}&models=1,2,3"
        redacted = dg_utils.redact_api_key(message)
        assert self.FAKE_KEY not in redacted
        assert "api_key=***" in redacted
        assert "models=1,2,3" in redacted

    def test_key_at_end_of_string(self):
        message = f"endpoint?hru_code=19999&api_key={self.FAKE_KEY}"
        redacted = dg_utils.redact_api_key(message)
        assert redacted == "endpoint?hru_code=19999&api_key=***"

    def test_key_containing_colon_both_parts_redacted(self):
        """The partial-leak bug this revision fixes: a credential value
        that itself contains a colon (not followed by a space) must be
        redacted in full, not just up to the first colon. Only a colon
        immediately followed by a space (the actual message separator)
        terminates the match."""
        message = 'endpoint?api_key=prefix:suffix: {"message": "nope"}'
        redacted = dg_utils.redact_api_key(message)
        assert "prefix" not in redacted
        assert "suffix" not in redacted
        assert "api_key=***" in redacted
        # The JSON body -- separated by the real ': ' -- survives intact.
        assert '{"message": "nope"}' in redacted

    def test_case_insensitive_variants_all_redacted(self):
        """API_KEY= (upper, with underscore) and ApiKey= (mixed case,
        no underscore) must both redact -- not just a case-fold of the
        exact 'api_key=' literal."""
        upper = f"endpoint?API_KEY={self.FAKE_KEY}&next=1"
        camel = f"endpoint?ApiKey={self.FAKE_KEY}&next=1"

        redacted_upper = dg_utils.redact_api_key(upper)
        redacted_camel = dg_utils.redact_api_key(camel)

        assert self.FAKE_KEY not in redacted_upper
        assert "next=1" in redacted_upper
        assert self.FAKE_KEY not in redacted_camel
        assert "next=1" in redacted_camel

    def test_unrelated_name_ending_in_api_key_is_not_matched(self):
        """DEFECT 1: the pattern must not fire inside an unrelated
        identifier like 'backup_api_key=' -- without the leading
        boundary check, a match starting mid-name would truncate
        whatever followed it, corrupting diagnostics that have nothing
        to do with the real credential."""
        message = f'ep?api_key={self.FAKE_KEY}: {{"message":"backup_api_key=disabled"}}'
        redacted = dg_utils.redact_api_key(message)
        assert self.FAKE_KEY not in redacted
        assert "api_key=***" in redacted
        # The unrelated "backup_api_key=disabled" field is untouched --
        # not partially eaten, not redacted (it's a different field).
        assert "backup_api_key=disabled" in redacted

    def test_colon_space_inside_key_fully_redacted_when_env_var_set(self, monkeypatch):
        """DEFECT 2: a credential value that itself contains ': ' is
        genuinely ambiguous to the pattern alone (': ' is also the real
        message separator) -- this is fixed via the literal-value pass,
        not the pattern, so it requires the live key to be known via the
        env var the Data Gateway client actually reads it from."""
        tricky_key = f"{self.FAKE_KEY}: SUFFIX"
        monkeypatch.setenv("ieasyhydroforecast_API_KEY_GATEAWAY", tricky_key)

        message = f'ep?api_key={tricky_key}: {{"m":"x"}}'
        redacted = dg_utils.redact_api_key(message)

        assert self.FAKE_KEY not in redacted
        assert "SUFFIX" not in redacted
        assert "api_key=***" in redacted
        # Exactly one placeholder, not "***: ***" or "api_key=******" --
        # the literal pass runs first and the pattern pass then collapses
        # the already-redacted "api_key=***" without doubling it up.
        assert redacted == 'ep?api_key=***: {"m":"x"}'

    def test_colon_space_inside_key_partially_redacted_when_env_var_unset(self):
        """Same tricky-key message as above, but with no live key known
        to the helper (env var unset, the default in this test class).
        This must not crash, and must still redact what the pattern
        alone can reach -- the pattern-only limitation from DEFECT 2 is
        expected here, not a regression."""
        tricky_key = f"{self.FAKE_KEY}: SUFFIX"
        message = f'ep?api_key={tricky_key}: {{"m":"x"}}'

        redacted = dg_utils.redact_api_key(message)

        assert self.FAKE_KEY not in redacted
        assert "api_key=***" in redacted
        # The pattern-only limitation: text after the key's own ": " is
        # indistinguishable from the JSON separator, so it survives.
        assert "SUFFIX" in redacted

    def test_short_env_var_skips_literal_pass(self, monkeypatch):
        """A live key shorter than `_MIN_LITERAL_KEY_LENGTH` (e.g. a
        stray short placeholder in a test .env) must not trigger a
        blanket substring replacement -- that would corrupt any message
        that happens to contain the same short string as ordinary text."""
        monkeypatch.setenv("ieasyhydroforecast_API_KEY_GATEAWAY", "abc")

        message = "Unrelated diagnostic text that happens to contain abc in the middle."
        redacted = dg_utils.redact_api_key(message)

        assert redacted == message

    def test_no_api_key_passed_through_unchanged(self):
        """A message with no `api_key=` at all is untouched (byte-identical)."""
        message = "Failed to get data from api/calc?hru_code=19999: some other server error"
        assert dg_utils.redact_api_key(message) == message

    def test_redact_does_not_mutate_original_exception(self):
        """Redaction must operate on the formatted string only -- never
        on the exception object itself. `str(exc)` must still contain
        the original, unredacted text after redaction has run."""
        secret_message = (
            f'https://dg.example.com/api?api_key={self.FAKE_KEY}: {{"message": "nope"}}'
        )
        exc = ValueError(secret_message)

        redacted = dg_utils.redact_api_key(str(exc))

        assert "api_key=***" in redacted
        assert self.FAKE_KEY not in redacted
        # The exception itself is unchanged.
        assert str(exc) == secret_message
        assert self.FAKE_KEY in str(exc)


def _make_dg_control_member_csv(
    code: str, date_str: str, t_value: float, p_value: float
) -> pd.DataFrame:
    """Build a minimal 7-header-row DG control-member CSV DataFrame.

    Mirrors the real Data Gateway control-member CSV shape (see
    test_integration_preprocessing_gateway.py::make_dg_control_member_csv),
    trimmed to a single code/date -- just enough for
    dg_utils.transform_data_file_control_member to parse without error,
    which is all TestCallSiteRedaction needs from the control-member
    step on its way to the ensemble loop.
    """
    cols = ["Station", code, f"{code}.1", f"{code}.2"]
    header_rows = [[f"header_{i}", f"meta_{i}", f"meta_{i}", f"meta_{i}"] for i in range(7)]
    data_row = [[date_str, t_value, p_value, 0.0]]
    return pd.DataFrame(header_rows + data_row, columns=cols)


@pytest.fixture()
def dg_call_site_env(tmp_path, monkeypatch):
    """Minimal environment for exercising main()'s three DG call sites.

    Sets up only what `main()` reads on its way through the
    control-member and ensemble download loops -- no snow, no
    reanalysis, no SAPPHIRE API writes (disabled per-test via
    `SAPPHIRE_API_AVAILABLE`). Station code 19999 throughout, never a
    real HRU.

    This is intentionally a smaller, standalone fixture rather than a
    reuse of test_integration_preprocessing_gateway.py's `gateway_env` /
    `gateway_env_ensemble` -- PREPG-015's allowed-file list excludes
    that module.

    Sets ieasyhydroforecast_run_CM_models=true so every test in this
    file keeps reaching the ensemble download under the PREPG-023 C1
    consumption gate -- none of the tests here exercise the gate-closed
    skip path, only redaction on the control-member and ensemble call
    sites.
    """
    intermediate = tmp_path / "intermediate_data"
    dg_dir = intermediate / "dg_download"
    cm_dir = intermediate / "control_member"
    ens_dir = intermediate / "ensemble"
    config_dir = tmp_path / "config"
    models_dir = tmp_path / "models"
    for d in (dg_dir, cm_dir, ens_dir, config_dir, models_dir):
        d.mkdir(parents=True, exist_ok=True)

    # NOTE: deliberately do NOT create models_dir/qmap_params -- its
    # absence tells QM to skip quantile mapping (perform_qmapping=False),
    # which keeps the control-member success path minimal.
    config_file = config_dir / "data_gateway_name_twins.json"
    config_file.write_text(json.dumps({"gateway_name_twins": {}}))

    env_vars = {
        "ieasyforecast_intermediate_data_path": str(intermediate),
        "ieasyhydroforecast_OUTPUT_PATH_CM": "control_member",
        "ieasyhydroforecast_OUTPUT_PATH_ENS": "ensemble",
        "ieasyhydroforecast_OUTPUT_PATH_DG": "dg_download",
        "ieasyhydroforecast_HRU_CONTROL_MEMBER": "19999",
        "ieasyhydroforecast_HRU_ENSEMBLE": "19999",
        "ieasyhydroforecast_run_CM_models": "true",
        "ieasyhydroforecast_API_KEY_GATEAWAY": "FAKE-KEY-DO-NOT-USE",
        "ieasyhydroforecast_Q_MAP_PARAM_PATH": "qmap_params",
        "ieasyhydroforecast_models_and_scalers_path": str(models_dir),
        "ieasyforecast_configuration_path": str(config_dir),
        "ieasyhydroforecast_config_file_data_gateway_name_twins": "data_gateway_name_twins.json",
    }
    for k, v in env_vars.items():
        monkeypatch.setenv(k, v)

    return {"dg_dir": dg_dir, "cm_dir": cm_dir, "ens_dir": ens_dir}


class TestCallSiteRedaction:
    """main()-level coverage for PREPG-015: verifies the three log/print
    call sites redact `api_key` from Data Gateway exception messages,
    without altering exception identity or breaking the
    today->yesterday fallback match.

    Mirrors the mocking pattern of
    test_integration_preprocessing_gateway.py::TestTransportRetryMainLevel
    (same repo, not imported from here -- see `dg_call_site_env` above).
    """

    TODAY = datetime(2024, 6, 15)
    TODAY_STR = "2024-06-15"
    YESTERDAY_STR = "2024-06-14"
    HRU = "19999"
    FAKE_KEY = "FAKE-KEY-DO-NOT-USE"

    def _run_main(self, mock_dg):
        """Enter the patches shared by every test in this class.

        Returns an ExitStack (itself a context manager) with
        sl.load_environment neutralised, the DG client mocked, API
        writes disabled, and the forecast date fixed to TODAY.
        """
        stack = ExitStack()
        stack.enter_context(patch("Quantile_Mapping_OP.sl.load_environment"))
        stack.enter_context(
            patch(
                "Quantile_Mapping_OP.sapphire_dg_client.client.SapphireDGClient",
                return_value=mock_dg,
            )
        )
        stack.enter_context(patch.object(qm, "SAPPHIRE_API_AVAILABLE", False))
        mock_datetime = stack.enter_context(patch("Quantile_Mapping_OP.datetime"))
        mock_datetime.today.return_value = self.TODAY
        return stack

    def _successful_control_member_mock(self, dg_dir):
        """A control-member download that always succeeds immediately,
        so tests targeting the ensemble loop can get past it."""
        cm_df = _make_dg_control_member_csv(self.HRU, "01.01.2024", 5.0, 2.0)
        cm_csv_path = str(dg_dir / "cm_19999.csv")

        def _side_effect(**kwargs):
            cm_df.to_csv(cm_csv_path, index=False)
            return cm_csv_path

        return _side_effect

    def test_control_member_operational_error_is_redacted(self, dg_call_site_env, caplog):
        """Site 1 (`logger.error`, ~:796): the observed exposure, fires
        on the routine 'data not published yet' condition."""
        caplog.set_level(logging.ERROR)
        secret_message = (
            "Failed to get data from "
            f"api/calculations/operational/template/RSMinerva?hru_code={self.HRU}"
            f"&start_date=2023-06-15&api_key={self.FAKE_KEY}: "
            '{"message": "Operational data for HRU 19999 is not available for this date", '
            '"success": false}'
        )
        mock_dg = MagicMock()
        mock_dg.operational.get_control_spinup_and_forecast.side_effect = Exception(secret_message)

        with self._run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code == 1
        error_text = "\n".join(r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)
        assert self.FAKE_KEY not in error_text
        assert "api_key=***" in error_text
        # Diagnostics preserved: endpoint, HRU, and the server's response text.
        assert "RSMinerva" in error_text
        assert "Operational data for HRU 19999 is not available for this date" in error_text
        assert '"success": false' in error_text

    def test_ensemble_yesterday_fallback_error_is_redacted(self, dg_call_site_env, capsys):
        """Site 2 (`print`, ~:949): the yesterday-fallback branch,
        reached after today's data is absent -- same routine path as
        site 1, newly identified by this issue. Also proves the
        today->yesterday fallback trigger still matches even though the
        triggering exception's own message carries an `api_key`."""
        env = dg_call_site_env
        mock_dg = MagicMock()
        mock_dg.operational.get_control_spinup_and_forecast.side_effect = (
            self._successful_control_member_mock(env["dg_dir"])
        )

        today_message = (
            "Couldn't find any files for the given HRU code, date and models! "
            f"hru_code={self.HRU}&date={self.TODAY_STR}&api_key={self.FAKE_KEY}"
        )
        yesterday_secret = (
            "Failed to get data from "
            f"api/calculations/ensemble?hru_code={self.HRU}&date={self.YESTERDAY_STR}"
            f"&api_key={self.FAKE_KEY}: "
            '{"message": "No ensemble data available", "success": false}'
        )

        def ens_side_effect(hru_code, date, models, directory):
            if date == self.TODAY_STR:
                raise ValueError(today_message)
            raise ValueError(yesterday_secret)

        mock_dg.ecmwf_ens.get_ensemble_forecast.side_effect = ens_side_effect

        with self._run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        # The fallback contract: matching still worked despite the
        # api_key embedded in the outer (today) exception's message.
        assert f"No data for {self.TODAY_STR}, trying {self.YESTERDAY_STR}" in out
        # Whole captured output, not just this site's own print line: the
        # very next statement, `print(_redact_api_key(traceback.format_exc()))`,
        # also renders e2's message (as the traceback's final line) and must
        # not leak the raw key either.
        assert self.FAKE_KEY not in out
        assert "api_key=***" in out
        assert "No ensemble data available" in out

    def test_ensemble_yesterday_fallback_traceback_is_redacted(self, dg_call_site_env, capsys):
        """The `print(traceback.format_exc())` immediately after site 2
        renders e2's own str() as its final line -- it must be redacted
        too, or it silently re-leaks the key one line below the fix.
        A pass here must be because the key was actually removed, not
        because nothing was printed: assert the traceback's structural
        markers survive alongside the absence of the raw key."""
        env = dg_call_site_env
        mock_dg = MagicMock()
        mock_dg.operational.get_control_spinup_and_forecast.side_effect = (
            self._successful_control_member_mock(env["dg_dir"])
        )

        today_message = "Couldn't find any files for the given HRU code, date and models! "
        yesterday_secret = (
            "Failed to get data from "
            f"api/calculations/ensemble?hru_code={self.HRU}&date={self.YESTERDAY_STR}"
            f"&api_key={self.FAKE_KEY}: "
            '{"message": "No ensemble data available", "success": false}'
        )

        def ens_side_effect(hru_code, date, models, directory):
            if date == self.TODAY_STR:
                raise ValueError(today_message)
            raise ValueError(yesterday_secret)

        mock_dg.ecmwf_ens.get_ensemble_forecast.side_effect = ens_side_effect

        with self._run_main(mock_dg):
            with pytest.raises(SystemExit):
                qm.main()

        out = capsys.readouterr().out
        assert self.FAKE_KEY not in out
        assert "api_key=***" in out
        # Diagnostics survived: this proves redaction, not suppression.
        assert "Traceback (most recent call last)" in out
        assert "ValueError" in out
        # Diagnostics unique to THIS exception's message (not just the
        # HRU code, which also appears in unrelated progress prints):
        # the endpoint path fragment and the server's response text.
        # A redactor broad enough to also destroy these would still
        # pass on the checks above.
        assert "api/calculations/ensemble" in out
        assert "No ensemble data available" in out

    def test_ensemble_unexpected_error_is_redacted(self, dg_call_site_env, capsys):
        """Site 3 (`print`, ~:956): a ValueError that does NOT match the
        'no files' fallback trigger -- same client, same exposure."""
        env = dg_call_site_env
        mock_dg = MagicMock()
        mock_dg.operational.get_control_spinup_and_forecast.side_effect = (
            self._successful_control_member_mock(env["dg_dir"])
        )

        secret_message = (
            "Failed to get data from "
            f"api/calculations/ensemble?hru_code={self.HRU}&date={self.TODAY_STR}"
            f"&api_key={self.FAKE_KEY}: "
            '{"message": "Internal server error", "success": false}'
        )

        def ens_side_effect(hru_code, date, models, directory):
            raise ValueError(secret_message)

        mock_dg.ecmwf_ens.get_ensemble_forecast.side_effect = ens_side_effect

        with self._run_main(mock_dg):
            with pytest.raises(SystemExit) as exc_info:
                qm.main()

        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        # Whole captured output: the very next statement,
        # `print(_redact_api_key(traceback.format_exc()))`, also renders
        # e's message (as the traceback's final line) and must not leak
        # the raw key either.
        assert self.FAKE_KEY not in out
        assert "api_key=***" in out
        assert "Internal server error" in out

    def test_ensemble_unexpected_error_traceback_is_redacted(self, dg_call_site_env, capsys):
        """The `print(traceback.format_exc())` immediately after site 3
        renders e's own str() as its final line -- it must be redacted
        too. Diagnostics (traceback markers, exception type) must
        survive so a passing test proves redaction, not suppression."""
        env = dg_call_site_env
        mock_dg = MagicMock()
        mock_dg.operational.get_control_spinup_and_forecast.side_effect = (
            self._successful_control_member_mock(env["dg_dir"])
        )

        secret_message = (
            "Failed to get data from "
            f"api/calculations/ensemble?hru_code={self.HRU}&date={self.TODAY_STR}"
            f"&api_key={self.FAKE_KEY}: "
            '{"message": "Internal server error", "success": false}'
        )

        def ens_side_effect(hru_code, date, models, directory):
            raise ValueError(secret_message)

        mock_dg.ecmwf_ens.get_ensemble_forecast.side_effect = ens_side_effect

        with self._run_main(mock_dg):
            with pytest.raises(SystemExit):
                qm.main()

        out = capsys.readouterr().out
        assert self.FAKE_KEY not in out
        assert "api_key=***" in out
        assert "Traceback (most recent call last)" in out
        assert "ValueError" in out
        # Diagnostics unique to THIS exception's message (not just the
        # HRU code, which also appears in unrelated progress prints):
        # the endpoint path fragment and the server's response text.
        assert "api/calculations/ensemble" in out
        assert "Internal server error" in out
