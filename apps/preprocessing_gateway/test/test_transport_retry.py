"""
Unit tests for the Data Gateway transport-retry helper (PREPG-010).

Covers `_call_with_transport_retry` in isolation -- no client, no
main(), no filesystem. For main()-level coverage of the three call
sites (control member, today ensemble loop, yesterday fallback
ensemble loop), see
test_integration_preprocessing_gateway.py::TestTransportRetryMainLevel.

Run::

    cd apps
    SAPPHIRE_TEST_ENV=True pytest preprocessing_gateway/test/test_transport_retry.py -v
"""

import logging
import os
import sys
import time
from unittest.mock import MagicMock

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
