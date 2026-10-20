"""Tests for NotificationManager in apps/pipeline/src/notification_manager.py.

Covers:
- send_email() success, with attachments, SMTP failure
- send_failure_notification() with/without recipients, with additional_info
"""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from apps.pipeline.src.notification_manager import NotificationManager


class TestSendEmail:
    """Test NotificationManager.send_email()."""

    def test_success(self, mock_smtp):
        """Successful email send calls SMTP methods in order."""
        result = NotificationManager.send_email(
            recipients=["user@example.com"],
            subject="Test Subject",
            message="Test body",
        )

        assert result is True
        mock_smtp.starttls.assert_called_once()
        mock_smtp.login.assert_called_once_with("testuser", "testpass")
        mock_smtp.sendmail.assert_called_once()
        mock_smtp.quit.assert_called_once()

    def test_with_attachments(self, mock_smtp, tmp_path):
        """Files are attached to the email."""
        # Create temp files to attach
        file1 = tmp_path / "log1.txt"
        file1.write_text("log content 1")
        file2 = tmp_path / "log2.txt"
        file2.write_text("log content 2")

        result = NotificationManager.send_email(
            recipients=["user@example.com"],
            subject="Test",
            message="Body",
            attachment_paths=[str(file1), str(file2)],
        )

        assert result is True
        mock_smtp.sendmail.assert_called_once()

    def test_smtp_failure(self, mock_smtp):
        """SMTP failure returns False without crashing."""
        mock_smtp.login.side_effect = Exception("Auth failed")

        result = NotificationManager.send_email(
            recipients=["user@example.com"],
            subject="Test",
            message="Body",
        )

        assert result is False


class TestSendFailureNotification:
    """Test NotificationManager.send_failure_notification()."""

    def test_with_recipients(self, mock_smtp, monkeypatch):
        """Sends email when recipients are configured."""
        monkeypatch.setenv(
            "SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS", "admin@example.com,ops@example.com"
        )
        monkeypatch.setenv("ieasyhydroforecast_organization", "kghm")

        result = NotificationManager.send_failure_notification(
            task_name="TestTask",
            error_details="Something went wrong",
        )

        assert result is True
        mock_smtp.sendmail.assert_called_once()

    def test_no_recipients(self, monkeypatch):
        """Returns False without attempting to send when no recipients."""
        monkeypatch.setenv("SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS", "")

        result = NotificationManager.send_failure_notification(
            task_name="TestTask",
            error_details="Something went wrong",
        )

        assert result is False

    def test_with_additional_info(self, mock_smtp, monkeypatch):
        """Additional info dict items appear in message body."""
        monkeypatch.setenv("SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS", "admin@example.com")
        monkeypatch.setenv("ieasyhydroforecast_organization", "demo")

        result = NotificationManager.send_failure_notification(
            task_name="TestTask",
            error_details="Error details here",
            additional_info={"Timeout (seconds)": 900, "Max retries": 3},
        )

        assert result is True
        # Verify the email was sent (content is in the MIME message)
        mock_smtp.sendmail.assert_called_once()

    def test_with_log_files(self, mock_smtp, monkeypatch, tmp_path):
        """Log files are attached to the notification."""
        monkeypatch.setenv("SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS", "admin@example.com")
        monkeypatch.setenv("ieasyhydroforecast_organization", "demo")

        log_file = tmp_path / "task_log.txt"
        log_file.write_text("task log content")

        result = NotificationManager.send_failure_notification(
            task_name="TestTask",
            error_details="Error details",
            log_file_paths=[str(log_file)],
        )

        assert result is True


class TestAlertLabel:
    """Test NotificationManager.get_alert_label() and format_subject()."""

    def test_alert_org_beats_organization(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_ALERT_ORG", "kghm-demo")
        monkeypatch.setenv("ieasyhydroforecast_organization", "kghm")
        assert NotificationManager.get_alert_label() == "kghm-demo"

    def test_organization_fallback_keeps_case(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_ALERT_ORG", raising=False)
        monkeypatch.setenv("ieasyhydroforecast_organization", "tjHM")
        assert NotificationManager.get_alert_label() == "tjHM"

    def test_unknown_fallback(self, monkeypatch):
        monkeypatch.delenv("SAPPHIRE_ALERT_ORG", raising=False)
        monkeypatch.delenv("ieasyhydroforecast_organization", raising=False)
        assert NotificationManager.get_alert_label() == "unknown"

    def test_non_ascii_and_newlines_stripped(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_ALERT_ORG", " kg\r\nhm\u00e9\t ")
        assert NotificationManager.get_alert_label() == "kghm"

    def test_brackets_removed(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_ALERT_ORG", "kghm] [SAPPHIRE tjhm")
        assert NotificationManager.get_alert_label() == "kghm SAPPHIRE tjhm"

    def test_all_garbage_falls_through(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_ALERT_ORG", "\r\n\u00e9\u0416")
        monkeypatch.setenv("ieasyhydroforecast_organization", "uzhm")
        assert NotificationManager.get_alert_label() == "uzhm"

    def test_format_subject(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_ALERT_ORG", "kghm")
        assert (
            NotificationManager.format_subject("PIPELINE", "hello")
            == "[SAPPHIRE kghm PIPELINE] hello"
        )

    def test_failure_notification_subject(self, monkeypatch):
        monkeypatch.setenv("SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS", "a@example.com")
        monkeypatch.setenv("SAPPHIRE_ALERT_ORG", "kghm")
        with patch.object(NotificationManager, "send_email", return_value=True) as m:
            NotificationManager.send_failure_notification("LinearRegression", "boom")
        subject = m.call_args.kwargs["subject"]
        assert subject.startswith("[SAPPHIRE kghm PIPELINE] \u274c LinearRegression failed - ")
