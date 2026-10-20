"""Tests for the Subject line composed by bin/monitoring/lib/mail.sh."""

import os
import stat
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MAIL_SH = REPO_ROOT / "bin" / "monitoring" / "lib" / "mail.sh"

BASE_ENV = (
    "SAPPHIRE_PIPELINE_SMTP_SERVER=smtp.example.com\n"
    "SAPPHIRE_PIPELINE_SMTP_PORT=587\n"
    "SAPPHIRE_PIPELINE_SMTP_USERNAME=user\n"
    "SAPPHIRE_PIPELINE_SMTP_PASSWORD=dummy\n"
    "SAPPHIRE_PIPELINE_SENDER_EMAIL=from@example.com\n"
    "SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS=to@example.com\n"
)


@pytest.fixture
def run_mail(tmp_path):
    """Run mail_send against a fake msmtp; return the captured message."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    capture = tmp_path / "captured.txt"
    fake = bindir / "msmtp"
    fake.write_text(f'#!/bin/bash\ncat > "{capture}"\n')
    fake.chmod(fake.stat().st_mode | stat.S_IXUSR)

    def _run(env_lines="", process_env=None, category="DOCKER"):
        envf = tmp_path / "env"
        envf.write_text(BASE_ENV + env_lines)
        env = {k: v for k, v in os.environ.items()
               if k not in ("SAPPHIRE_ALERT_ORG", "ieasyhydroforecast_organization")}
        env["PATH"] = f"{bindir}{os.pathsep}{env['PATH']}"
        env["ENVF"] = str(envf)
        env["MAIL_SH"] = str(MAIL_SH)
        env["CAT"] = category
        env.update(process_env or {})
        subprocess.run(
            ["bash", "-c",
             'source "$MAIL_SH"; mail_resolve_config "$ENVF"; '
             'mail_send "$CAT" "x crashed" "body"'],
            env=env, check=True, capture_output=True,
        )
        return capture.read_text()

    return _run


def _subject_lines(msg):
    return [ln for ln in msg.splitlines() if ln.startswith("Subject:")]


def test_env_file_alert_org_beats_organization(run_mail):
    msg = run_mail("SAPPHIRE_ALERT_ORG=kghm-demo\nieasyhydroforecast_organization=kghm\n")
    assert _subject_lines(msg) == ["Subject: [SAPPHIRE kghm-demo DOCKER] x crashed"]


def test_process_env_beats_env_file(run_mail):
    msg = run_mail(
        "SAPPHIRE_ALERT_ORG=from-file\nieasyhydroforecast_organization=kghm\n",
        process_env={"SAPPHIRE_ALERT_ORG": "from-proc"},
    )
    assert _subject_lines(msg) == ["Subject: [SAPPHIRE from-proc DOCKER] x crashed"]


def test_organization_fallback(run_mail):
    msg = run_mail("ieasyhydroforecast_organization=tjhm\n", category="HEALTHCHECK")
    assert _subject_lines(msg) == ["Subject: [SAPPHIRE tjhm HEALTHCHECK] x crashed"]


def test_quoted_env_file_value(run_mail):
    msg = run_mail('SAPPHIRE_ALERT_ORG="uzhm"\n')
    assert _subject_lines(msg) == ["Subject: [SAPPHIRE uzhm DOCKER] x crashed"]


def test_newline_never_creates_header(run_mail):
    msg = run_mail(
        "ieasyhydroforecast_organization=kghm\n",
        process_env={"SAPPHIRE_ALERT_ORG": "evil\nBcc: a@example.com"},
    )
    assert len(_subject_lines(msg)) == 1
    assert not any(ln.startswith("Bcc:") for ln in msg.splitlines())
    assert _subject_lines(msg)[0].startswith("Subject: [SAPPHIRE evilBcc: a@example.com DOCKER]")


def test_env_file_inline_comment_dropped(run_mail):
    msg = run_mail("SAPPHIRE_ALERT_ORG=kghm-demo # demo box\n")
    assert _subject_lines(msg) == ["Subject: [SAPPHIRE kghm-demo DOCKER] x crashed"]


@pytest.mark.parametrize("raw", ["kghm-demo ", '"kghm-demo"', "'kghm-demo' "])
def test_env_file_space_and_quotes_trimmed(run_mail, raw):
    msg = run_mail(f"SAPPHIRE_ALERT_ORG={raw}\n")
    assert _subject_lines(msg) == ["Subject: [SAPPHIRE kghm-demo DOCKER] x crashed"]


def test_whitespace_only_falls_through_to_organization(run_mail):
    msg = run_mail("SAPPHIRE_ALERT_ORG=   \nieasyhydroforecast_organization=kghm\n")
    assert _subject_lines(msg) == ["Subject: [SAPPHIRE kghm DOCKER] x crashed"]


def test_brackets_removed_from_label(run_mail):
    msg = run_mail(process_env={"SAPPHIRE_ALERT_ORG": "kghm] [SAPPHIRE tjhm"})
    assert _subject_lines(msg) == [
        "Subject: [SAPPHIRE kghm SAPPHIRE tjhm DOCKER] x crashed"
    ]


@pytest.mark.parametrize(
    "script, category",
    [
        ("bin/monitoring/docker.sh", "DOCKER"),
        ("bin/monitoring/docker_log_watcher.sh", "DASHBOARD"),
        ("bin/email_healthcheck_report.sh", "HEALTHCHECK"),
    ],
)
def test_callers_use_expected_category(script, category):
    calls = [
        ln.strip()
        for ln in (REPO_ROOT / script).read_text().splitlines()
        if not ln.strip().startswith("#") and "mail_send " in ln
    ]
    assert calls, f"no mail_send call found in {script}"
    for ln in calls:
        first = ln.split("mail_send ", 1)[1].split()[0]
        assert first == category, f"{script}: {ln}"
