#!/bin/bash
# =============================================================================
# SAPPHIRE monitoring mail helper
# =============================================================================
#
# Shared SMTP alerting plumbing for:
#   - bin/monitoring/docker.sh
#   - bin/monitoring/docker_log_watcher.sh
#   - bin/email_healthcheck_report.sh
#
# This file is a library -- source it, do not execute it:
#
#   SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
#   source "$SCRIPT_DIR/lib/mail.sh"      # docker.sh / docker_log_watcher.sh
#   source "$SCRIPT_DIR/monitoring/lib/mail.sh"   # email_healthcheck_report.sh
#
# It does not depend on the caller's working directory and does not assume
# anything about its own location beyond its own path, so it is safe to
# source from a systemd unit with an absolute ExecStart and cwd=/.
#
# Provides:
#
#   mail_resolve_config <env_file>
#       Greps the SMTP/sender/recipient variables and the organisation id
#       out of <env_file> (does NOT execute/source it -- see note below) and
#       sets these globals:
#         MAIL_SMTP_SERVER, MAIL_SMTP_PORT, MAIL_SMTP_USER, MAIL_SMTP_PASS,
#         MAIL_SENDER, MAIL_RECIPIENTS, MAIL_ORG
#       MAIL_ORG resolution order:
#         1. $SAPPHIRE_ALERT_ORG from the environment, if set and non-empty
#            after sanitisation (see below) -- mail-subject label override.
#         2. ieasyhydroforecast_organization from the env file.
#         3. `hostname`, when ieasyhydroforecast_organization is absent or
#            empty in the env file (and SAPPHIRE_ALERT_ORG is unset/empty).
#
#       Why an override exists instead of just changing the organisation:
#       ieasyhydroforecast_organization is a pipeline identity, not a label --
#       it also drives timeout configuration selection, dashboard URL
#       derivation and station filtering elsewhere in the system. A
#       demo/staging box that intentionally runs a copy of another
#       deployment's configuration (same env file, same
#       ieasyhydroforecast_organization) cannot change that value just to get
#       a distinct mail subject without breaking those other behaviours.
#       SAPPHIRE_ALERT_ORG lets an operator relabel mail subjects only, e.g.
#       by adding Environment="SAPPHIRE_ALERT_ORG=kghm-demo" to the systemd
#       unit, without touching any config the pipeline itself reads.
#
#       SAPPHIRE_ALERT_ORG is sanitised before use: it lands in a mail
#       header (the "[SAPPHIRE <org>] " subject prefix), and unlike the env
#       file -- which an operator controls and formats carefully -- it comes
#       from the environment, so a stray newline could inject arbitrary mail
#       headers and non-ASCII bytes have previously reached production and
#       arrived mangled. Bytes outside printable ASCII (0x20-0x7E, which
#       excludes CR/LF and tabs along with any non-ASCII byte) are stripped,
#       not rejected outright, so a mostly-clean value (e.g. trailing CRLF
#       from how the unit file quoted it) still works; if stripping leaves it
#       empty, resolution falls through to the env file / hostname exactly as
#       if SAPPHIRE_ALERT_ORG had never been set.
#
#   mail_config_is_complete
#       Returns 0 when MAIL_SMTP_SERVER/PORT/USER/PASS, MAIL_SENDER and
#       MAIL_RECIPIENTS are all non-empty, 1 otherwise. Does not print
#       anything or exit -- callers keep their own error message and exit
#       behaviour, which differ script to script.
#
#   mail_send <subject_text> <body> [extra_headers]
#       Composes and sends one message via msmtp:
#         - Subject: "[SAPPHIRE $MAIL_ORG] <subject_text>"
#         - To / From headers, any caller-supplied extra_headers lines
#           (e.g. "Content-Type: text/plain; charset=UTF-8") after From
#         - blank line, then body, printed verbatim (no further escape
#           processing -- see note below on why that is the caller's job)
#         - the SMTP password via a mode-600 temp file, never on argv
#         - the comma-separated recipient list split into separate msmtp
#           arguments (the To: header keeps the comma-separated form)
#         - --from "$MAIL_SENDER" (msmtp refuses to send without it)
#       Returns msmtp's exit code. Cleans up its password temp file in all
#       cases.
#
#   Callers keep their own subject TEXT and body composition (including any
#   optional attached/appended log content) -- only the plumbing above is
#   shared. This is deliberate, not an omission: the three callers disagree
#   on whether a given section of the body is `echo -e` (backslash-escape
#   interpreted) or printed literally -- e.g. docker_log_watcher.sh's
#   send_alert() applies `echo -e` to its $message but plain `echo` to the
#   $LOG_CONTEXT it appends, while email_healthcheck_report.sh never
#   interprets escapes at all. A single generic "append this" step inside
#   mail_send could not reproduce all three byte-for-byte, so each caller
#   renders its own final body text (applying `echo -e` itself wherever it
#   previously did) and passes the finished bytes in; mail_send's job starts
#   only at the headers.
#
# -----------------------------------------------------------------------
# Deliberate behaviour change: env file is grepped, never sourced
# -----------------------------------------------------------------------
# bin/monitoring/docker.sh and bin/monitoring/docker_log_watcher.sh used to
# load their env file with `set -o allexport; source "$ENV_FILE"; set
# +o allexport`, which EXECUTES the file as shell. bin/email_healthcheck_
# report.sh already used grep instead. This helper standardises on grep for
# all three callers: an env file holding an SMTP credential should not be
# executed by a service running unattended under systemd.
#
# Verified before making this change: both monitoring scripts use only the
# seven variables mail_resolve_config extracts below (the six
# SAPPHIRE_PIPELINE_SMTP_*/SENDER_EMAIL/EMAIL_RECIPIENTS variables plus
# ieasyhydroforecast_organization) from their env file and nothing else, so
# nothing in either script depends on the sourcing having executed the rest
# of the file.
#
# Consequence: a malformed env file that previously caused a shell syntax
# error when sourced (sourcing failure, non-zero exit from a bad line) now
# instead yields empty variables for whichever line failed to parse as
# "KEY=VALUE", which mail_config_is_complete still catches and reports as an
# incomplete SMTP configuration. Both outcomes fail loudly and send no mail;
# only the failure mode/message changes, not whether the script proceeds.
# =============================================================================

mail_resolve_config() {
    local env_file="$1"

    MAIL_SMTP_SERVER=$(grep -m1 -E '^SAPPHIRE_PIPELINE_SMTP_SERVER='      "$env_file" | cut -d= -f2-)
    MAIL_SMTP_PORT=$(grep -m1   -E '^SAPPHIRE_PIPELINE_SMTP_PORT='        "$env_file" | cut -d= -f2-)
    MAIL_SMTP_USER=$(grep -m1   -E '^SAPPHIRE_PIPELINE_SMTP_USERNAME='    "$env_file" | cut -d= -f2-)
    MAIL_SMTP_PASS=$(grep -m1   -E '^SAPPHIRE_PIPELINE_SMTP_PASSWORD='    "$env_file" | cut -d= -f2-)
    MAIL_SENDER=$(grep -m1      -E '^SAPPHIRE_PIPELINE_SENDER_EMAIL='     "$env_file" | cut -d= -f2-)
    MAIL_RECIPIENTS=$(grep -m1  -E '^SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS=' "$env_file" | cut -d= -f2-)

    # Mail-only deployment-label override: see "MAIL_ORG resolution order"
    # above. Stripped to printable ASCII (0x20-0x7E) so neither a non-ASCII
    # byte nor an injected CR/LF header can reach the Subject line composed
    # in mail_send; an empty result after stripping is treated the same as
    # "unset" and falls through below.
    MAIL_ORG=$(printf '%s' "${SAPPHIRE_ALERT_ORG:-}" | tr -cd '\40-\176')

    # Same prefer-org-fallback-hostname approach used by all three callers
    # already; tr set strips a trailing \r and any quoting around the value.
    if [ -z "$MAIL_ORG" ]; then
        MAIL_ORG=$(grep -m1 -E '^ieasyhydroforecast_organization=' "$env_file" | cut -d= -f2- | tr -d '\r"'\''')
    fi
    [ -z "$MAIL_ORG" ] && MAIL_ORG=$(hostname)

    # Explicit success return: none of the current callers use `set -e`, but
    # without this the function's exit status is whatever the `[ -z ... ] &&`
    # short-circuit above happened to leave (0 only when the hostname
    # fallback actually ran) -- a landmine for any future caller that does.
    return 0
}

mail_config_is_complete() {
    [ -n "$MAIL_SMTP_SERVER" ] && [ -n "$MAIL_SMTP_PORT" ] && [ -n "$MAIL_SMTP_USER" ] \
        && [ -n "$MAIL_SMTP_PASS" ] && [ -n "$MAIL_SENDER" ] && [ -n "$MAIL_RECIPIENTS" ]
}

mail_send() {
    local subject_text="$1"
    local body="$2"
    local extra_headers="${3:-}"

    local subject="[SAPPHIRE $MAIL_ORG] $subject_text"

    # Use a file for the password instead of exposing it in process arguments
    local pass_file
    pass_file=$(mktemp)
    echo "$MAIL_SMTP_PASS" > "$pass_file"
    chmod 600 "$pass_file"

    # The To: header keeps the comma-separated form (correct for mail
    # headers), but msmtp takes each recipient as its own argument, so the
    # commas are converted to spaces only for the argument list below.
    local recipient_args=${MAIL_RECIPIENTS//,/ }

    {
        echo "Subject: $subject"
        echo "To: $MAIL_RECIPIENTS"
        echo "From: $MAIL_SENDER"
        [ -n "$extra_headers" ] && printf '%s\n' "$extra_headers"
        echo
        printf '%s\n' "$body"
    } | msmtp --host="$MAIL_SMTP_SERVER" --port="$MAIL_SMTP_PORT" --auth=on \
              --user="$MAIL_SMTP_USER" --passwordeval="cat $pass_file" \
              --tls=on --tls-starttls=on --from="$MAIL_SENDER" $recipient_args

    local rc=$?
    rm -f "$pass_file"
    return $rc
}
