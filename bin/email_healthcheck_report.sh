#!/usr/bin/env bash
# =============================================================================
# SAPPHIRE Healthcheck Email Report
# =============================================================================
#
# Runs bin/handover_healthcheck.sh and emails the FULL result to the
# deployment's alert recipients -- every run, not only on a problem.
#
# Scheduled from cron, roughly an hour after the daily decadal forecast run,
# this IS the dead-man's switch: a mail that only goes out "on problems"
# makes silence ambiguous between "healthy" and "cron stopped / the server
# died / mail itself broke" -- exactly the failure mode that let a broken
# pipeline sit unnoticed on this deployment for months. Sending every day
# also proves the check actually ran. The subject carries the verdict so a
# clean day is one glance and a bad day stands out; there is no quiet mode.
#
# Usage:
#   bash bin/email_healthcheck_report.sh
#   bash bin/email_healthcheck_report.sh --env-file /data/<data_dir>/config/.env_develop_kghm
#
# Exit: 0  = the mail was sent (regardless of the health check's own PASS/
#            WARN/FAIL verdict -- that verdict lives in the mail, not here).
#       !=0 = the mail could NOT be sent (msmtp missing, SMTP config
#            incomplete, env file not found, or the send itself failed).
#            cron should only complain when THIS -- the reporting -- broke.
# =============================================================================
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HEALTHCHECK="$SCRIPT_DIR/handover_healthcheck.sh"

# Shared SMTP alerting plumbing (env-file resolution, completeness check,
# compose-and-send) used by bin/monitoring/docker.sh and
# bin/monitoring/docker_log_watcher.sh too; see bin/monitoring/lib/mail.sh.
# This script already derived SMTP settings by grepping the env file rather
# than sourcing it, so unlike those two this is not a behaviour change here.
source "$SCRIPT_DIR/monitoring/lib/mail.sh"

# --- Locate the deployment env file -----------------------------------------
# Same detection scheme as bin/handover_healthcheck.sh, deliberately not
# reinvented: an explicit --env-file wins, otherwise the first
# /data/*/config/.env_develop_* found.
ENV_FILE=""
[ "${1:-}" = "--env-file" ] && ENV_FILE="${2:-}"
if [ -z "$ENV_FILE" ]; then
    ENV_FILE=$(ls -1 /data/*/config/.env_develop_* 2>/dev/null | head -1)
fi

# --- Run the health check ---------------------------------------------------
# Capture stdout AND stderr together, and the exit code. The check exits 1
# when anything FAILed -- expected, and must NOT stop the mail from going
# out; that is the entire point of this script.
if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
    RAW_OUTPUT=$(bash "$HEALTHCHECK" --env-file "$ENV_FILE" 2>&1)
else
    RAW_OUTPUT=$(bash "$HEALTHCHECK" 2>&1)
fi
HC_RC=$?

# --- Strip ANSI colour codes -------------------------------------------------
# The check colours its [ OK ]/[WARN]/[FAIL] labels for a terminal; those
# escape sequences are unreadable noise in a mail client.
PLAIN_OUTPUT=$(printf '%s\n' "$RAW_OUTPUT" | sed -E $'s/\x1b\\[[0-9;]*[a-zA-Z]//g')

# --- Redact anything that looks like a credential ---------------------------
# Defence in depth: the health check should never emit these, but a future
# change to it must not silently start mailing secrets. Whole line is
# replaced (not just the matched word) so a redacted line can't leak the
# surrounding context either. Case-insensitive via tolower() rather than a
# sed "I" flag, which BSD sed (used when testing this script locally) does
# not support -- awk's tolower() behaves identically on GNU and BSD awk.
REDACTED_OUTPUT=$(printf '%s\n' "$PLAIN_OUTPUT" | awk '
{
    line = tolower($0)
    if (line ~ /password|secret|api[_-]?key|token|pgpassword/) {
        print "[REDACTED LINE -- matched a credential pattern]"
    } else {
        print
    }
}')

# --- Organisation name for the subject, and (while the file is open anyway)
# the SMTP settings used later --------------------------------------------
# Prefer the deployment's own org id; fall back to the hostname when the env
# file is missing or does not set it. mail_resolve_config also sets
# MAIL_SMTP_SERVER/PORT/USER/PASS, MAIL_SENDER and MAIL_RECIPIENTS from the
# same file read, reused by the completeness check and send below -- it is
# only called here, when $ENV_FILE is already confirmed to exist, so a
# missing file never reaches grep (matching the old guarded ORG lookup
# rather than letting grep itself report "No such file or directory").
if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
    mail_resolve_config "$ENV_FILE"
else
    MAIL_ORG=$(hostname)
fi
ORG="$MAIL_ORG"

# --- Parse the check's own summary line for pass/warn/fail counts ----------
# handover_healthcheck.sh's final printf emits (post ANSI-strip):
#   "  N passed, M warning(s), K failure(s)"
# If this does not match -- the check's output format drifted from what this
# script understands -- say so in the subject rather than claiming a verdict
# that was not actually read.
SUMMARY_LINE=$(printf '%s\n' "$PLAIN_OUTPUT" \
    | grep -E '^[[:space:]]*[0-9]+ passed, [0-9]+ warning\(s\), [0-9]+ failure\(s\)' | tail -1)

plural() { [ "$1" = "1" ] && printf '%s' "$2" || printf '%s' "$3"; }

# SUBJECT holds the TEXT only -- mail_send below adds the
# "[SAPPHIRE $MAIL_ORG] " prefix, same as it does for the other two callers.
SUBJECT=""
if [ -n "$SUMMARY_LINE" ]; then
    N_PASS=$(printf '%s' "$SUMMARY_LINE" | grep -oE '[0-9]+ passed'  | grep -oE '[0-9]+')
    N_WARN=$(printf '%s' "$SUMMARY_LINE" | grep -oE '[0-9]+ warning' | grep -oE '[0-9]+')
    N_FAIL=$(printf '%s' "$SUMMARY_LINE" | grep -oE '[0-9]+ failure' | grep -oE '[0-9]+')
    if [ "${N_FAIL:-0}" -gt 0 ]; then
        SUBJECT="$N_FAIL FAILED, $N_WARN $(plural "$N_WARN" warning warnings)"
    elif [ "${N_WARN:-0}" -gt 0 ]; then
        SUBJECT="OK - $N_PASS passed, $N_WARN $(plural "$N_WARN" warning warnings)"
    else
        SUBJECT="OK - $N_PASS passed"
    fi
else
    SUBJECT="status unknown - see body"
fi

# --- Compose the body --------------------------------------------------------
BODY=$(printf 'SAPPHIRE health check report\nHost:       %s\nSent:       %s\nEnv file:   %s\nCheck exit: %s\n\n%s\n' \
    "$(hostname)" "$(date '+%F %H:%M %Z')" "${ENV_FILE:-<not found>}" "$HC_RC" "$REDACTED_OUTPUT")

# --- From here on: any failure to actually send must be LOUD ---------------
if [ -z "$ENV_FILE" ] || [ ! -f "$ENV_FILE" ]; then
    echo "[ERROR] no deployment env file found (looked under /data/*/config/.env_develop_*; no --env-file given) -- cannot read SMTP settings, mail NOT sent" >&2
    exit 1
fi

# MAIL_SMTP_*/MAIL_SENDER/MAIL_RECIPIENTS were already resolved by
# mail_resolve_config above (same six variables, same grep-not-source style
# bin/monitoring/docker.sh and docker_log_watcher.sh now also use).
if ! mail_config_is_complete; then
    echo "[ERROR] SMTP configuration incomplete in $ENV_FILE (need all 6 SAPPHIRE_PIPELINE_SMTP_*/SENDER_EMAIL/EMAIL_RECIPIENTS vars) -- mail NOT sent" >&2
    exit 1
fi

if ! command -v msmtp >/dev/null 2>&1; then
    echo "[ERROR] msmtp not found on PATH -- cannot send mail (health check verdict was: [SAPPHIRE $MAIL_ORG] $SUBJECT)" >&2
    exit 1
fi

# --- Send ---------------------------------------------------------------
# mail_send applies the "[SAPPHIRE $MAIL_ORG] " subject prefix, handles the
# password temp file, the comma-to-space recipient split and --from; see
# bin/monitoring/lib/mail.sh. $BODY is passed through verbatim (no `echo -e`
# -- this script never interpreted backslash escapes in the body, and
# mail_send doesn't either).
SEND_OUT=$(mail_send "$SUBJECT" "$BODY" 2>&1)
SEND_RC=$?

if [ "$SEND_RC" -ne 0 ]; then
    echo "[ERROR] msmtp failed (exit $SEND_RC) -- mail NOT sent. msmtp output:" >&2
    echo "$SEND_OUT" >&2
    exit 1
fi

exit 0
