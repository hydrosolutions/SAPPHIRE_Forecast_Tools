#!/usr/bin/env bash
# =============================================================================
# SAPPHIRE Support Bundle Collector
# =============================================================================
#
# For the KGHM sysadmin (or any operator) escalating a problem the deployment
# health check can't fix: collects everything the Provider will ask for into
# ONE archive, instead of the current back-and-forth of "can you also send
# me...". Read-only — changes nothing on this host.
#
# Collects automatically:
#   - bin/handover_healthcheck.sh output (stdout+stderr+exit code)
#   - `docker ps -a` for the WHOLE host
#   - tail (last 200 lines) of the 3 newest failure_log_* files
#   - systemd status of the monitoring units + any tunnel unit (matched by
#     name, not assumed)
#   - disk usage (/, /var/lib/docker, docker system df)
#   - tail (last 100 lines) of the most recent sapphire cron log
#   - repo state (git log -1 --oneline, git status --short)
#   - hostname, uptime, date, invoking user
# Prompts the operator on screen for two things it cannot know:
#   - what command was run and the exact error
#   - what changed recently
#
# Every credential-looking line (password/secret/api key/token) is redacted
# BEFORE it is written to any collected file. The .env file itself is never
# read. A second grep pass runs over the staged bundle after collection and
# prints a warning if anything survived.
#
# A missing/inaccessible item never aborts the bundle — it is recorded as
# "not available: <reason>" in its own file and the script moves on. No sudo
# is ever invoked.
#
# Usage:
#   bash bin/collect_support_bundle.sh
#   bash bin/collect_support_bundle.sh --env-file /data/<data_dir>/config/.env_develop_kghm
#
# Output: a single .tar.gz written to $HOME, whose full path is printed as
# the LAST line of output — that is the only thing the operator needs to
# attach to the escalation.
# =============================================================================

PASS=0; WARN=0; ALARM=0
ok()   { printf '  \033[32m[ OK ]\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
warn() { printf '  \033[33m[WARN]\033[0m %s\n' "$*"; WARN=$((WARN+1)); }
bad()  { printf '  \033[31m[FAIL]\033[0m %s\n' "$*"; ALARM=$((ALARM+1)); }
head_() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

# --- Redaction --------------------------------------------------------------
# Case-insensitive match on: password, secret, api[_-]?key, token, PGPASSWORD
# (PGPASSWORD is already covered by "password" but named explicitly to match
# the spec literally). Applied to EVERY collected file, not just the .env —
# a container log or a cron log can just as easily echo a password.
REDACT_PATTERN='password|secret|api[_-]?key|token|pgpassword'

redact_stream() {
    # Reads stdin, writes stdin to stdout with any matching line replaced by
    # a marker. Deliberately a plain read loop (not sed/awk -I) so it behaves
    # the same on GNU and BSD userlands.
    local line
    while IFS= read -r line || [ -n "$line" ]; do
        if printf '%s\n' "$line" | grep -qiE "$REDACT_PATTERN"; then
            echo "[REDACTED: line matched a credential pattern]"
        else
            printf '%s\n' "$line"
        fi
    done
}

# write_section <path> <title>: reads content from stdin, redacts it, and
# writes it to <path> under a small header. Used for every successfully
# collected item so the redaction pass is never accidentally skipped.
write_section() {
    local path="$1" title="$2"
    {
        echo "=== $title ==="
        echo "collected: $(date '+%F %H:%M:%S %Z')"
        echo
        redact_stream
    } > "$path"
}

# write_missing <path> <title> <reason>: records a soft failure. This is the
# ONLY way an unavailable item is handled — never an abort.
write_missing() {
    local path="$1" title="$2" reason="$3"
    {
        echo "=== $title ==="
        echo "not available: $reason"
    } > "$path"
    warn "$title: not available ($reason)"
}

# --- Locate the deployment ---------------------------------------------------
# Same detection as bin/handover_healthcheck.sh: look for the first
# /data/*/config/.env_develop_* unless overridden with --env-file. Do not
# invent a second heuristic.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
HEALTHCHECK="$SCRIPT_DIR/handover_healthcheck.sh"

ENV_FILE=""
[ "${1:-}" = "--env-file" ] && ENV_FILE="${2:-}"
if [ -z "$ENV_FILE" ]; then
    ENV_FILE=$(ls -1 /data/*/config/.env_develop_* 2>/dev/null | head -1)
fi
DATA_DIR=""
if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
    DATA_DIR=$(dirname "$(dirname "$ENV_FILE")")
fi

echo "SAPPHIRE support bundle collector — $(date '+%F %H:%M %Z') on $(hostname 2>/dev/null || echo unknown)"
echo "Running as: $(whoami 2>/dev/null || echo unknown)"

head_ "Deployment detection (same method as handover_healthcheck.sh)"
if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
    ok "env file: $ENV_FILE (contents are never read into the bundle)"
    ok "data dir: $DATA_DIR"
else
    warn "no .env_develop_* found under /data/*/config/ (pass --env-file <path> to override) — failure-log collection may be limited"
fi

# --- Staging area -------------------------------------------------------------
STAMP=$(date '+%Y%m%d_%H%M%S')
BUNDLE_DIRNAME="sapphire_support_bundle_${STAMP}"
STAGE_PARENT=$(mktemp -d "${TMPDIR:-/tmp}/sapphire_support_bundle.XXXXXX" 2>/dev/null) || STAGE_PARENT="/tmp/sapphire_support_bundle.$$"
trap 'rm -rf "$STAGE_PARENT"' EXIT
STAGE="$STAGE_PARENT/$BUNDLE_DIRNAME"
mkdir -p "$STAGE/failure_logs"
BUNDLE_PATH="${HOME:-/tmp}/${BUNDLE_DIRNAME}.tar.gz"

# --- Operator-supplied context ------------------------------------------------
# These two things cannot be derived from the host — ask first, while the
# operator is at the keyboard, so the rest of the script can run unattended.
read_multiline() {
    # Prompts go to stderr, not stdout: the caller captures this function's
    # stdout with $(...) to get the answer text. If the prompt were on
    # stdout too, it would be swallowed into the captured string instead of
    # ever reaching the operator's screen — exactly the silent "did this
    # hang?" situation this function exists to avoid.
    local prompt="$1" line ans=""
    echo "$prompt" >&2
    echo "(Type your answer, one or more lines. When finished, type a line containing ONLY the word END and press Enter — do NOT press Ctrl-C, that abandons the bundle.)" >&2
    while IFS= read -r line; do
        [ "$line" = "END" ] && break
        ans="${ans}${line}
"
    done
    printf '%s' "$ans"
}

head_ "Operator report"
Q1_ANSWER=$(read_multiline "Q1: What command did you run, and what was the exact error?")
echo
Q2_ANSWER=$(read_multiline "Q2: What changed recently — an update, a reboot, a config edit?")
{
    echo "Q1: What command did you run, and what was the exact error?"
    echo "$Q1_ANSWER"
    echo
    echo "Q2: What changed recently — an update, a reboot, a config edit?"
    echo "$Q2_ANSWER"
} | write_section "$STAGE/operator_report.txt" "Operator-reported context"
ok "operator report captured"

# --- Host info ----------------------------------------------------------------
head_ "Host info"
{
    echo "hostname: $(hostname 2>/dev/null || echo 'not available')"
    echo "date: $(date '+%F %H:%M:%S %Z')"
    echo "uptime: $(uptime 2>/dev/null || echo 'not available')"
    echo "invoking user: $(whoami 2>/dev/null || echo 'not available')"
} | write_section "$STAGE/host_info.txt" "Host info: hostname, uptime, date, invoking user"
ok "host info captured"

# --- Health check ---------------------------------------------------------
head_ "Health check (bin/handover_healthcheck.sh)"
if [ -f "$HEALTHCHECK" ]; then
    if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
        HC_OUT=$(bash "$HEALTHCHECK" --env-file "$ENV_FILE" 2>&1)
    else
        HC_OUT=$(bash "$HEALTHCHECK" 2>&1)
    fi
    HC_RC=$?
    {
        printf '%s\n' "$HC_OUT"
        echo
        echo "exit code: $HC_RC (non-zero is EXPECTED when the check found a problem — it does not mean this bundle failed)"
    } | write_section "$STAGE/healthcheck.txt" "bin/handover_healthcheck.sh — stdout+stderr combined"
    ok "healthcheck captured (exit code $HC_RC)"
else
    write_missing "$STAGE/healthcheck.txt" "handover_healthcheck.sh output" "not found at $HEALTHCHECK"
fi

# --- Docker containers (whole host) ------------------------------------------
head_ "Docker containers (whole host)"
if command -v docker >/dev/null 2>&1 && docker ps >/dev/null 2>&1; then
    docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' 2>&1 \
        | write_section "$STAGE/docker_ps.txt" "docker ps -a (all containers on this host, not just sapphire-*)"
    ok "docker ps -a captured"
else
    write_missing "$STAGE/docker_ps.txt" "docker ps -a" "docker not installed, or not reachable as $(whoami 2>/dev/null) (check 'groups \$(whoami)' for the docker group)"
fi

# --- Recent pipeline failure logs --------------------------------------------
head_ "Recent pipeline failure logs"
if [ -n "$DATA_DIR" ] && [ -d "$DATA_DIR/intermediate_data/docker_logs" ]; then
    FAILURE_FILES=$(ls -t "$DATA_DIR"/intermediate_data/docker_logs/failure_log_* 2>/dev/null | head -3)
    if [ -n "$FAILURE_FILES" ]; then
        N=0
        while IFS= read -r f; do
            [ -z "$f" ] && continue
            N=$((N+1))
            base=$(basename "$f")
            LINES_TOTAL=$(wc -l < "$f" 2>/dev/null | tr -d ' ')
            [ -z "$LINES_TOTAL" ] && LINES_TOTAL='?'
            tail -n 200 "$f" 2>&1 \
                | write_section "$STAGE/failure_logs/${base}.tail.txt" \
                    "TAIL of $base — showing the LAST 200 of ${LINES_TOTAL} lines (full file omitted, can be hundreds of KB)"
        done <<< "$FAILURE_FILES"
        ok "$N failure log(s) captured (tail -200 each)"
    else
        write_missing "$STAGE/failure_logs/none.txt" "failure logs" "no failure_log_* files under $DATA_DIR/intermediate_data/docker_logs"
    fi
else
    write_missing "$STAGE/failure_logs/none.txt" "failure logs" "data directory not found (DATA_DIR='${DATA_DIR:-unknown}')"
fi

# --- systemd: monitoring units + tunnel --------------------------------------
head_ "systemd units (monitoring + tunnel)"
if command -v systemctl >/dev/null 2>&1; then
    {
        for unit in docker-monitor.service dashboard-log-watcher.service; do
            echo "$unit: is-active=$(systemctl is-active "$unit" 2>&1) is-enabled=$(systemctl is-enabled "$unit" 2>&1)"
        done
        # Match on how the unit BEHAVES, not on a guessed naming convention —
        # an earlier tool missed the real unit "ieasyhydrohf-tunnel" by
        # searching only for "ieh". Collect every candidate, not just the
        # first, and dedupe across both listings.
        TUNNEL_UNITS=$( { systemctl list-units --type=service --all --no-legend 2>/dev/null | awk '{print $1}'; \
                           systemctl list-unit-files --no-legend 2>/dev/null | awk '{print $1}'; } \
                         | grep -iE 'tunnel|autossh|ieasyhydro' | sort -u )
        if [ -n "$TUNNEL_UNITS" ]; then
            while IFS= read -r u; do
                [ -z "$u" ] && continue
                echo "$u: is-active=$(systemctl is-active "$u" 2>&1) is-enabled=$(systemctl is-enabled "$u" 2>&1)"
            done <<< "$TUNNEL_UNITS"
        else
            echo "no tunnel/autossh/ieasyhydro systemd unit found by name match"
        fi
    } | write_section "$STAGE/systemd_units.txt" "systemd unit status: monitoring units + any tunnel unit (matched by name, not assumed)"
    ok "systemd unit status captured"
else
    write_missing "$STAGE/systemd_units.txt" "systemd unit status" "systemctl not available on this host"
fi

# --- Disk ---------------------------------------------------------------------
head_ "Disk usage"
{
    echo "--- df -h / ---"
    df -h / 2>&1
    echo
    echo "--- df -h /var/lib/docker ---"
    if [ -e /var/lib/docker ]; then
        df -h /var/lib/docker 2>&1
    else
        echo "not available: /var/lib/docker does not exist on this host"
    fi
    echo
    echo "--- docker system df ---"
    if command -v docker >/dev/null 2>&1; then
        DSYS=$(docker system df 2>&1)
        DSYS_RC=$?
        if [ "$DSYS_RC" -eq 0 ]; then
            printf '%s\n' "$DSYS"
        else
            echo "not available: 'docker system df' failed (exit $DSYS_RC): $DSYS"
        fi
    else
        echo "not available: docker not installed"
    fi
} | write_section "$STAGE/disk.txt" "Disk usage: df -h /, df -h /var/lib/docker, docker system df"
ok "disk usage captured"

# --- Cron log -------------------------------------------------------------
head_ "Cron log (most recent)"
CRON_LOG=$(ls -t "$HOME"/logs/sapphire_*.log /home/*/logs/sapphire_*.log 2>/dev/null | head -1)
if [ -n "$CRON_LOG" ] && [ -f "$CRON_LOG" ]; then
    LINES_TOTAL=$(wc -l < "$CRON_LOG" 2>/dev/null | tr -d ' ')
    [ -z "$LINES_TOTAL" ] && LINES_TOTAL='?'
    tail -n 100 "$CRON_LOG" 2>&1 \
        | write_section "$STAGE/cron_log_tail.txt" "TAIL of $CRON_LOG — showing the LAST 100 of ${LINES_TOTAL} lines"
    ok "cron log tail captured ($CRON_LOG)"
else
    write_missing "$STAGE/cron_log_tail.txt" "cron log" "no sapphire_*.log found under \$HOME/logs or /home/*/logs"
fi

# --- Repo state -----------------------------------------------------------
head_ "Repository state"
if git -C "$REPO_DIR" rev-parse --git-dir >/dev/null 2>&1; then
    {
        echo "--- git -C $REPO_DIR log -1 --oneline ---"
        git -C "$REPO_DIR" log -1 --oneline 2>&1
        echo
        echo "--- git -C $REPO_DIR status --short ---"
        git -C "$REPO_DIR" status --short 2>&1
    } | write_section "$STAGE/repo_state.txt" "Repo state: $REPO_DIR"
    ok "repo state captured"
else
    write_missing "$STAGE/repo_state.txt" "repo state" "$REPO_DIR is not a git checkout"
fi

# --- README -----------------------------------------------------------------
{
    echo "SAPPHIRE support bundle"
    echo "Generated: $(date '+%F %H:%M:%S %Z') on $(hostname 2>/dev/null || echo 'unknown host')"
    echo
    echo "NOTICE: this archive contains operational hydrological data (station"
    echo "codes, discharge values, log excerpts). It is for the Provider's use"
    echo "diagnosing THIS deployment only — send it only to the Provider,"
    echo "do not forward it onward."
    echo
    echo "Credential-looking lines (password/secret/api key/token) were redacted"
    echo "before any file below was written. See redaction_check.txt for the"
    echo "result of a second grep pass run over this staged bundle afterwards."
    echo
    echo "Contents:"
    echo "  operator_report.txt   - what the operator ran/saw and what changed recently"
    echo "  host_info.txt         - hostname, uptime, date, invoking user"
    echo "  healthcheck.txt       - bin/handover_healthcheck.sh output + exit code"
    echo "  docker_ps.txt         - docker ps -a for the whole host"
    echo "  failure_logs/         - tail (last 200 lines) of the 3 newest failure logs"
    echo "  systemd_units.txt     - is-active/is-enabled for monitoring + tunnel units"
    echo "  disk.txt              - df -h /, /var/lib/docker, docker system df"
    echo "  cron_log_tail.txt     - last 100 lines of the most recent sapphire cron log"
    echo "  repo_state.txt        - git log -1 --oneline + git status --short"
    echo "  redaction_check.txt   - result of the post-build credential grep"
    echo
    echo "Any item that could not be collected says so in its own file"
    echo "(\"not available: <reason>\") instead of being silently missing."
} > "$STAGE/README.txt"

# --- Redaction verification --------------------------------------------------
# Defense in depth: redaction already happened at write time (write_section),
# this is a second pass over the assembled bundle so a survivor is caught
# before the archive is sent, not after.
head_ "Redaction verification"
# Exclude the script's own meta files: README.txt and this check's own output
# describe the redaction policy in prose ("password/secret/api key/token"),
# which would otherwise trip this same pattern on every single run and turn
# a real alarm into noise nobody trusts.
REDACT_HITS=$(grep -rlEi "$REDACT_PATTERN" "$STAGE" 2>/dev/null | grep -vE '/(README\.txt|redaction_check\.txt)$' || true)
if [ -n "$REDACT_HITS" ]; then
    bad "credential-looking text SURVIVED redaction in: $(printf '%s ' $REDACT_HITS) — DO NOT SEND until reviewed"
    {
        echo "SURVIVED in the following file(s) — reviewed manually before sending:"
        printf '%s\n' "$REDACT_HITS"
    } > "$STAGE/redaction_check.txt"
else
    ok "no credential-looking lines found in the staged bundle"
    echo "none found." > "$STAGE/redaction_check.txt"
fi

# --- Build archive --------------------------------------------------------
head_ "Building archive"
TAR_ERR="$STAGE_PARENT/tar_err.txt"
tar czf "$BUNDLE_PATH" -C "$STAGE_PARENT" "$BUNDLE_DIRNAME" 2>"$TAR_ERR"
TAR_RC=$?
if [ "$TAR_RC" -ne 0 ]; then
    bad "tar failed (exit $TAR_RC): $(cat "$TAR_ERR" 2>/dev/null)"
else
    ok "archive written ($(du -h "$BUNDLE_PATH" 2>/dev/null | cut -f1) )"
fi

# --- Summary ------------------------------------------------------------
printf '\n\033[1m== Summary ==\033[0m\n'
printf '  %d collected, \033[33m%d not available\033[0m, \033[31m%d alarm(s)\033[0m\n' "$PASS" "$WARN" "$ALARM"
echo "  Attach the archive below to the escalation. A '[WARN] ... not available'"
echo "  item above just means that piece could not be collected on this host —"
echo "  the rest of the bundle is still useful."
[ -n "$REDACT_HITS" ] && echo "  DO NOT SEND yet — review redaction_check.txt in the archive first."
echo
echo "Support bundle written to:"
echo "$BUNDLE_PATH"
