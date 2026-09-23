#!/usr/bin/env bash
# =============================================================================
# SAPPHIRE Handover Health Check
# =============================================================================
#
# Read-only status report for a RUNNING SAPPHIRE deployment. Changes nothing.
# Written for the handover of a long-lived server, so it DETECTS the real
# configuration rather than assuming the documented one.
#
# Usage:
#   bash bin/handover_healthcheck.sh
#   bash bin/handover_healthcheck.sh --env-file /data/<data_dir>/config/.env_develop_kghm
#
# Exit: 0 = no FAILs (WARNs may be present), 1 = at least one FAIL.
# =============================================================================
set -uo pipefail

PASS=0; WARN=0; FAIL=0
ok()   { printf '  \033[32m[ OK ]\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
warn() { printf '  \033[33m[WARN]\033[0m %s\n' "$*"; WARN=$((WARN+1)); }
bad()  { printf '  \033[31m[FAIL]\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }
head_() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

ENV_FILE=""
[ "${1:-}" = "--env-file" ] && ENV_FILE="${2:-}"

echo "SAPPHIRE handover health check — $(date '+%F %H:%M %Z') on $(hostname)"
echo "Running as: $(whoami)"

# This script reports on a DEPLOYMENT SERVER. On a developer machine the
# host-level checks (systemd units, cron daemon, /var/backups, /data) describe
# things that are not supposed to exist there, so they are skipped rather than
# reported as failures.
SERVER_MODE=1
if [ "$(uname -s)" != "Linux" ]; then
    SERVER_MODE=0
    printf '\n\033[33mNOT A DEPLOYMENT SERVER (%s).\033[0m Host-level checks — systemd units, cron\n' "$(uname -s)"
    printf 'daemon, backups, data directory — are SKIPPED, not failed. Container and API\n'
    printf 'checks still run against whatever is on localhost. For a real report, run this\n'
    printf 'on the server.\n'
elif [ ! -d /data ] && ! systemctl list-unit-files >/dev/null 2>&1; then
    SERVER_MODE=0
    printf '\n\033[33mNo /data and no systemd — treating as a non-server host.\033[0m Host checks skipped.\n'
fi
skip() { printf '  \033[90m[SKIP]\033[0m %s\n' "$*"; }

# --- Locate the deployment -------------------------------------------------
head_ "Deployment layout (detected, not assumed)"
REPO_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ok "repo: $REPO_DIR ($(git -C "$REPO_DIR" log -1 --oneline 2>/dev/null || echo 'not a git checkout'))"

if [ -z "$ENV_FILE" ]; then
    ENV_FILE=$(ls -1 /data/*/config/.env_develop_* 2>/dev/null | head -1)
fi
if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
    ok "env file: $ENV_FILE"
    DATA_DIR=$(dirname "$(dirname "$ENV_FILE")")
    ok "data dir: $DATA_DIR"
    PERM=$(stat -c %a "$ENV_FILE" 2>/dev/null)
    [ "$PERM" = "600" ] && ok "env permissions 600" \
        || warn "env permissions are $PERM — should be 600 (holds DB password and JWT secret)"
elif [ "$SERVER_MODE" -eq 1 ]; then
    bad "no .env_develop_* found under /data/*/config/ — pass --env-file <path>"
    DATA_DIR=""
else
    skip "no deployment .env on this host (expected off-server)"
    DATA_DIR=""
fi

# --- Containers ------------------------------------------------------------
head_ "Containers"
if ! docker ps >/dev/null 2>&1; then
    bad "cannot talk to Docker as $(whoami) — is this user in the 'docker' group? (groups \$(whoami))"
else
    UP=$(docker ps --filter name=sapphire- --format '{{.Names}}' | wc -l)
    [ "$UP" -ge 10 ] && ok "$UP sapphire containers running" \
        || warn "$UP sapphire containers running (a full stack is ~10-11)"
    DOWN=$(docker ps -a --filter name=sapphire- --filter status=exited --format '{{.Names}}')
    [ -n "$DOWN" ] && warn "exited container(s): $(echo "$DOWN" | tr '\n' ' ')" || ok "no exited sapphire containers"
    REST=$(docker ps --filter name=sapphire- --format '{{.Names}} {{.Status}}' | grep -ci restarting || true)
    [ "$REST" -gt 0 ] && bad "$REST container(s) stuck restarting" || true
fi

# --- Service endpoints -----------------------------------------------------
head_ "Service endpoints"
curl -sf --max-time 5 http://localhost:8000/health/ready >/dev/null 2>&1 \
    && ok "api-gateway /health/ready" || bad "api-gateway /health/ready NOT responding — the pipeline cannot read or write data"
for p in 8002 8003 8004 8005; do
    curl -sf --max-time 5 "http://localhost:$p/health" >/dev/null 2>&1 \
        && ok "service on :$p healthy" || bad "service on :$p not responding"
done
curl -sf --max-time 5 http://localhost:8082/ >/dev/null 2>&1 \
    && ok "Luigi scheduler :8082" || warn "Luigi :8082 not responding — scheduled forecasts will fail"
# The dashboard occasionally needs a moment under load — retry once before
# calling it down, so a slow response is not reported as an outage.
C=""
for attempt in 1 2; do
    C=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 "http://localhost:5006/forecast_dashboard" 2>/dev/null)
    [ "$C" = "200" ] && break
    [ "$attempt" = "1" ] && sleep 3
done
if [ "$C" = "200" ]; then
    ok "dashboard :5006 serving (HTTP 200)"
elif [ "$SERVER_MODE" -eq 1 ]; then
    bad "dashboard :5006 returned '$C' after 2 attempts — forecasters cannot use the tool"
else
    skip "dashboard :5006 not running on this host"
fi
echo "  note: a '(unhealthy)' label on sapphire-dashboard is a known false alarm (image has no curl)."

# --- Data freshness — the check that actually matters ----------------------
head_ "Data freshness"
[ "$SERVER_MODE" -eq 0 ] && echo "  (local database — dates below reflect test data, not production)"
if curl -sf --max-time 5 http://localhost:8000/health/ready >/dev/null 2>&1; then
    # The API applies offset/limit with NO ORDER BY (crud.py), so a plain
    # "limit=N" returns an ARBITRARY page — on a large table that is the oldest
    # rows, and max(date) over it is meaningless. Always bound by start_date so
    # only recent rows can come back.
    SINCE=$(date -d '45 days ago' +%F 2>/dev/null || date -v-45d +%F 2>/dev/null)
    curl -s --max-time 30 "http://localhost:8003/lr-forecast/?start_date=${SINCE}&limit=5000" 2>/dev/null > /tmp/_hc_lrf.json
    curl -s --max-time 30 "http://localhost:8002/runoff/?start_date=${SINCE}&limit=20000"     2>/dev/null > /tmp/_hc_ro.json
    python3 - << 'PY'
import json, collections, datetime, sys
def load(p):
    try: return json.load(open(p))
    except Exception: return []
today = datetime.date.today()
def age(ds):
    try: return (today - datetime.date.fromisoformat(ds[:10])).days
    except Exception: return None
lrf = load("/tmp/_hc_lrf.json")
if not lrf:
    print("  \033[31m[FAIL]\033[0m no forecasts in the last 45 days — the pipeline has stopped publishing")
else:
    m = collections.defaultdict(list)
    for r in lrf: m[r.get("horizon_type","?")].append(r.get("date",""))
    for k, v in sorted(m.items()):
        latest = max(v); a = age(latest)
        tag = "\033[32m[ OK ]\033[0m" if a is not None and a <= 11 else "\033[33m[WARN]\033[0m"
        print(f"  {tag} {k} forecasts: latest {latest} ({a} days old)")
ro = load("/tmp/_hc_ro.json")
if not ro:
    print("  \033[33m[WARN]\033[0m no discharge data in the last 45 days — check the iEasyHydro connection")
if ro:
    m = collections.defaultdict(list)
    for r in ro:
        if r.get("horizon_type") == "day": m[r["code"]].append(r["date"])
    stale = []
    for k in sorted(m):
        a = age(max(m[k]))
        if a is not None and a > 7: stale.append(f"{k} ({a}d)")
    if stale:
        print(f"  \033[33m[WARN]\033[0m discharge data stale for: {', '.join(stale)} — check the iEasyHydro tunnel")
    else:
        print(f"  \033[32m[ OK ]\033[0m discharge data current for all {len(m)} stations")
PY
    rm -f /tmp/_hc_lrf.json /tmp/_hc_ro.json
else
    warn "skipped — API gateway is down"
fi

# --- Recent pipeline failures ----------------------------------------------
head_ "Recent pipeline runs"
if [ -n "$DATA_DIR" ] && [ -d "$DATA_DIR/intermediate_data/docker_logs" ]; then
    RECENT=$(find "$DATA_DIR/intermediate_data/docker_logs" -name 'failure_log_*' -mtime -3 2>/dev/null | wc -l)
    [ "$RECENT" -eq 0 ] && ok "no failure logs in the last 3 days" \
        || warn "$RECENT failure log(s) in the last 3 days — newest: $(ls -t "$DATA_DIR"/intermediate_data/docker_logs/failure_log_* 2>/dev/null | head -1)"
elif [ "$SERVER_MODE" -eq 1 ]; then
    warn "docker_logs directory not found under $DATA_DIR/intermediate_data/"
else
    skip "no deployment data directory on this host"
fi

# --- Scheduling ------------------------------------------------------------
head_ "Cron schedule"
if [ "$SERVER_MODE" -eq 0 ]; then
    skip "scheduling is a server concern"
else
MINE=$(crontab -l 2>/dev/null | grep -c 'bin/run_' || true)
if [ "${MINE:-0}" -gt 0 ]; then
    ok "$MINE SAPPHIRE cron entries under $(whoami)"
else
    FOUND=""
    for u in sapphire ubuntu root; do
        N=$(sudo -n crontab -u "$u" -l 2>/dev/null | grep -c 'bin/run_' || true)
        [ "${N:-0}" -gt 0 ] && FOUND="$FOUND $u($N)"
    done
    [ -n "$FOUND" ] && warn "no cron entries for $(whoami); the schedule belongs to:$FOUND — read with 'sudo crontab -u <user> -l'" \
        || warn "no SAPPHIRE cron entries found for $(whoami); check other accounts with 'sudo crontab -u <user> -l'"
fi
systemctl is-active --quiet cron 2>/dev/null && ok "cron daemon running" || bad "cron daemon NOT running — nothing is scheduled"
fi

# --- Monitoring ------------------------------------------------------------
head_ "Monitoring"
if [ "$SERVER_MODE" -eq 0 ]; then
    skip "systemd monitoring units are a server concern"
else
# Ask systemd about each unit directly. Parsing `list-unit-files` output was
# unreliable (column widths, truncation, SIGPIPE from `grep -q`) and reported a
# running unit as missing.
for unit in docker-monitor.service dashboard-log-watcher.service; do
    STATE=$(systemctl is-active "$unit" 2>/dev/null)
    if [ "$STATE" = "active" ]; then
        ok "$unit active"
    elif systemctl cat "$unit" >/dev/null 2>&1; then
        bad "$unit is installed but $STATE — no automated failure detection"
    else
        bad "$unit is NOT INSTALLED — no automated failure detection"
    fi
done
if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ]; then
    MISS=""
    for v in SAPPHIRE_PIPELINE_SMTP_SERVER SAPPHIRE_PIPELINE_SMTP_PORT SAPPHIRE_PIPELINE_SMTP_USERNAME \
             SAPPHIRE_PIPELINE_SMTP_PASSWORD SAPPHIRE_PIPELINE_SENDER_EMAIL SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS; do
        grep -qE "^${v}=." "$ENV_FILE" || MISS="$MISS $v"
    done
    [ -z "$MISS" ] && ok "all 6 SMTP alert variables set" || warn "SMTP variables missing:$MISS (alerts cannot send)"
fi
fi

# --- Backups ---------------------------------------------------------------
head_ "Backups"
if [ "$SERVER_MODE" -eq 0 ]; then
    skip "scheduled backups are a server concern"
else
BDIR=$(grep -rhoE '\-d +[^ ]+' <(crontab -l 2>/dev/null; sudo -n crontab -u sapphire -l 2>/dev/null) 2>/dev/null \
       | awk '{print $2}' | head -1)
BDIR="${BDIR:-/var/backups/sapphire}"
if [ -d "$BDIR" ]; then
    FRESH=$(find "$BDIR" -name '*.dump' -mtime -2 2>/dev/null | wc -l)
    [ "$FRESH" -ge 4 ] && ok "$FRESH dumps newer than 48h in $BDIR" \
        || bad "only $FRESH fresh dump(s) in $BDIR — expected 4 (preprocessing, postprocessing, user, auth)"
    FAILED=$(find "$BDIR" -name '*.FAILED' 2>/dev/null | wc -l)
    [ "$FAILED" -eq 0 ] && ok "no .FAILED backup artifacts" || bad "$FAILED .FAILED backup artifact(s) in $BDIR"
else
    bad "backup directory $BDIR does not exist — no backups are being taken"
fi
fi

# --- iEasyHydro tunnel ------------------------------------------------------
head_ "iEasyHydro HF connection"
if [ "$SERVER_MODE" -eq 0 ]; then
    skip "tunnel/connection is a server concern"
else
# Match on how the unit BEHAVES, not on a guessed naming convention: any unit
# whose name or description mentions a tunnel / autossh / iEasyHydro counts.
# (An earlier version searched only for the substring "ieh", which missed the
# real unit name "ieasyhydrohf-tunnel.service".)
TUNNEL=$(systemctl list-units --type=service --all --no-legend 2>/dev/null \
         | grep -iE 'tunnel|autossh|ieasyhydro|ieh[-_]' \
         | awk '{print $1}' | head -1)
[ -z "$TUNNEL" ] && TUNNEL=$(systemctl list-unit-files --no-legend 2>/dev/null \
         | grep -iE 'tunnel|autossh|ieasyhydro|ieh[-_]' | awk '{print $1}' | head -1)
if [ -n "$TUNNEL" ]; then
    systemctl is-active --quiet "$TUNNEL" && ok "$TUNNEL active" || bad "$TUNNEL NOT running — discharge data will go stale"
elif [ -n "$ENV_FILE" ] && grep -qE '^IEASYHYDROHF_HOST=https?://(hf\.)?ieasyhydro' "$ENV_FILE" 2>/dev/null; then
    ok "iEasyHydro HF reached directly over the internet (no tunnel expected)"
elif pgrep -af 'autossh|ssh .*-L .*5555' >/dev/null 2>&1; then
    bad "a tunnel process is running but NO systemd unit manages it — it will not survive a reboot"
else
    warn "no iEasyHydro tunnel unit or process found — confirm how this deployment reaches iEasyHydro HF"
fi
fi

# --- Disk ------------------------------------------------------------------
head_ "Disk"
for target in / /var/lib/docker "${DATA_DIR:-}"; do
    [ -n "$target" ] && [ -e "$target" ] || continue
    pct=$(df -P "$target" 2>/dev/null | awk 'NR==2 {print $5}')
    [ -n "$pct" ] || continue
    N=${pct%\%}
    if   [ "$N" -ge 90 ]; then bad  "$target is ${pct} full"
    elif [ "$N" -ge 80 ]; then warn "$target is ${pct} full"
    else                       ok   "$target at ${pct}"
    fi
done

# --- Exit-code fix ----------------------------------------------------------
head_ "Script versions"
N=$(grep -l '\[retcode\]' "$REPO_DIR"/bin/run_*.sh 2>/dev/null | wc -l)
[ "$N" -ge 5 ] && ok "$N wrappers propagate Luigi failures correctly" \
    || warn "only $N wrapper(s) have the [retcode] fix — a failed forecast may still exit 0; judge runs by failure logs"

# --- Summary ---------------------------------------------------------------
printf '\n\033[1m== Summary ==\033[0m\n'
printf '  %d passed, \033[33m%d warning(s)\033[0m, \033[31m%d failure(s)\033[0m\n' "$PASS" "$WARN" "$FAIL"
if [ "$FAIL" -gt 0 ]; then
    echo "  Action required — resolve every [FAIL] before handover sign-off."
    exit 1
fi
[ "$WARN" -gt 0 ] && echo "  No blocking failures. Review each [WARN] and record it as accepted or fixed."
exit 0
