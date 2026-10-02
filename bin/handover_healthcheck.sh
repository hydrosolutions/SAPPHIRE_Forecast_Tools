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

# Scratch space for this run. Unique per invocation (mktemp) so two concurrent
# runs never overwrite each other's files, and removed on any exit — normal
# completion or early exit — via the trap.
HC_TMPDIR=$(mktemp -d "${TMPDIR:-/tmp}/sapphire_healthcheck.XXXXXX" 2>/dev/null) || HC_TMPDIR="/tmp/sapphire_healthcheck.$$"
trap 'rm -rf "$HC_TMPDIR"' EXIT

PASS=0; WARN=0; FAIL=0
ok()   { printf '  \033[32m[ OK ]\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
warn() { printf '  \033[33m[WARN]\033[0m %s\n' "$*"; WARN=$((WARN+1)); }
bad()  { printf '  \033[31m[FAIL]\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }
head_() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

# Dispatch machine-readable verdict lines ("LEVEL<TAB>message", LEVEL one of
# OK/WARN/FAIL) emitted by a python helper into the shell's ok/warn/bad
# counters, so a [FAIL] printed by python still flips the exit code.
#
# Read from a FILE (`done < "$1"`), never from a pipe (`cmd | while read`):
# a pipe puts the loop body in a subshell, so PASS/WARN/FAIL increments
# there are lost the moment the subshell exits and the counters silently
# stay at zero. Sets the global VERDICT_COUNT (not a return value piped
# through command substitution, which would reintroduce the same subshell
# trap) so the caller can detect "python produced nothing".
dispatch_verdicts() {
    VERDICT_COUNT=0
    while IFS=$'\t' read -r level msg || [ -n "$level" ]; do
        [ -z "$level" ] && continue
        VERDICT_COUNT=$((VERDICT_COUNT+1))
        case "$level" in
            OK)   ok   "$msg" ;;
            WARN) warn "$msg" ;;
            FAIL) bad  "$msg" ;;
            *)    bad  "unrecognized verdict line from freshness check: $level $msg" ;;
        esac
    done < "$1"
}

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
    LRF_JSON="$HC_TMPDIR/lrf.json"
    RO_JSON="$HC_TMPDIR/ro.json"
    VERDICTS_ST="$HC_TMPDIR/verdicts_st"
    ERR_ST="$HC_TMPDIR/err_st"
    curl -s --max-time 30 "http://localhost:8003/lr-forecast/?start_date=${SINCE}&limit=5000" 2>/dev/null > "$LRF_JSON"
    curl -s --max-time 30 "http://localhost:8002/runoff/?start_date=${SINCE}&limit=20000"     2>/dev/null > "$RO_JSON"
    # This block used to print the coloured [ OK ]/[WARN]/[FAIL] labels itself
    # with print(), which never touched the shell's ok/warn/bad counters — a
    # [FAIL] printed here left FAIL=0 and the script exited 0. It now emits
    # machine-readable "LEVEL<TAB>message" lines to a file; the shell reads
    # that file (not a pipe — see dispatch_verdicts) and calls ok/warn/bad
    # itself, so the visible output is byte-identical but the counters (and
    # exit code) are now correct.
    HC_LRF_JSON="$LRF_JSON" HC_RO_JSON="$RO_JSON" python3 - > "$VERDICTS_ST" 2>"$ERR_ST" << 'PY'
import json, collections, calendar, datetime, os

def load(p):
    try: return json.load(open(p))
    except Exception: return []

# Test-only clock override: set HC_FAKE_NOW (ISO 8601, e.g.
# "2026-09-25T02:00:00") to exercise the grace-period logic below without
# touching the system clock. Interpreted as LOCAL time, exactly like the
# production `datetime.datetime.now()` path below. Unset in production,
# where this is exactly the previous `datetime.date.today()` behaviour.
_fake_now = os.environ.get("HC_FAKE_NOW")
now = datetime.datetime.fromisoformat(_fake_now) if _fake_now else datetime.datetime.now()
# Use the server's LOCAL hour, not a UTC conversion. cron interprets
# crontab hours in the server's own local timezone, and doc/deployment.md
# (see "Set up cron job", ~line 927) has operators pick the LOCAL hour that
# lands the run in the morning local time on their deployment -- the
# documented "UTC schedule" is a conversion aid for picking that local
# hour, not something every deployment's cron actually fires at. The run
# always happens in the morning LOCAL time, at a DIFFERENT UTC hour per
# deployment; comparing against a fixed UTC hour would be correct for only
# one timezone and wrong for the rest. A local-hour cutoff is correct on
# every deployment without needing to know its timezone at all.
now_local_hour = now.hour
today = now.date()

def age(ds):
    try: return (today - datetime.date.fromisoformat(ds[:10])).days
    except Exception: return None

def emit(level, msg): print(f"{level}\t{msg}")

# --- Forecast issue-day rule -------------------------------------------
# Source of truth: apps/validate_pipeline/validate_pipeline.py
# (is_pentad_forecast_day, is_decad_forecast_day, most_recent_pentad_boundary,
# ~lines 120-145) and apps/linear_regression/linear_regression.py
# get_forecast_days_for_month (~line 435). This health check cannot import
# apps/ (different interpreter/paths), so the rule is reimplemented here —
# keep the two definitions in step if the issue-day schedule ever changes.
#   PENTAD issue days: 5, 10, 15, 20, 25, last day of month
#   DECAD  issue days: 10, 20, last day of month
PENTAD_FIXED_DAYS = (5, 10, 15, 20, 25)
DECAD_FIXED_DAYS = (10, 20)

def _issue_days(d, fixed_days):
    last_day = calendar.monthrange(d.year, d.month)[1]
    return sorted(set(fixed_days) | {last_day})

def most_recent_issue_day(d, fixed_days):
    """Most recent issue day <= d, wrapping into the previous month."""
    for b in reversed(_issue_days(d, fixed_days)):
        if b <= d.day:
            return datetime.date(d.year, d.month, b)
    prev_month_last = d.replace(day=1) - datetime.timedelta(days=1)
    return most_recent_issue_day(prev_month_last, fixed_days)

# Grace period: the run for issue day D happens ON day D, in the morning,
# LOCAL time on whatever deployment this is (doc/deployment.md's cron
# section has operators pick local cron hours that land the run in
# "morning local time" -- see the note above). Expecting D's forecast
# immediately at midnight would false-alarm every single issue day before
# cron has even run — worse than the bug being fixed, since people stop
# reading a check that cries wolf. GRACE_CUTOFF_HOUR=12 (local noon) is
# comfortably past any "morning local" run on every deployment in
# doc/deployment.md's timezone table, while still catching a genuinely
# missed run the same day it was missed rather than days later. Change
# GRACE_CUTOFF_HOUR to retune the buffer -- keep it in LOCAL hours, not UTC
# (see the note above on why local, not UTC, is correct here).
GRACE_CUTOFF_HOUR = 12

def expected_issue_day(today_, now_local_hour, fixed_days):
    """The issue day whose forecast should already exist, given today's
    local calendar date and the current local hour."""
    candidate = most_recent_issue_day(today_, fixed_days)
    if candidate == today_ and now_local_hour < GRACE_CUTOFF_HOUR:
        # Still within today's grace window on an issue day: today's run may
        # not have happened yet, so the forecast actually due is still the
        # previous issue day's.
        candidate = most_recent_issue_day(today_ - datetime.timedelta(days=1), fixed_days)
    return candidate

HORIZON_FIXED_DAYS = {"pentad": PENTAD_FIXED_DAYS, "decade": DECAD_FIXED_DAYS}

lrf = load(os.environ["HC_LRF_JSON"])
if not lrf:
    emit("FAIL", "no forecasts in the last 45 days — the pipeline has stopped publishing")
else:
    m = collections.defaultdict(list)
    for r in lrf: m[r.get("horizon_type","?")].append(r.get("date",""))
    for k, v in sorted(m.items()):
        latest = max(v); a = age(latest)
        fixed_days = HORIZON_FIXED_DAYS.get(k)
        if fixed_days is None:
            # Unknown horizon_type (neither pentad nor decade) - fall back
            # to the old flat threshold so an unexpected value can't crash
            # the health check.
            level = "OK" if a is not None and a <= 11 else "WARN"
            emit(level, f"{k} forecasts: latest {latest} ({a} days old)")
            continue
        due = expected_issue_day(today, now_local_hour, fixed_days)
        try:
            latest_date = datetime.date.fromisoformat(latest[:10])
        except Exception:
            latest_date = None
        if latest_date is not None and latest_date >= due:
            # `a` (age) is shown for operator context only -- due-date
            # comparison above is what decides OK/WARN, not age.
            emit("OK", f"{k} forecasts: latest {latest} ({a} days old)")
        else:
            emit("WARN", f"{k} forecasts: newest is {latest}, but a run was due {due.isoformat()}")
ro = load(os.environ["HC_RO_JSON"])
if not ro:
    emit("WARN", "no discharge data in the last 45 days — check the iEasyHydro connection")
if ro:
    m = collections.defaultdict(list)
    for r in ro:
        if r.get("horizon_type") == "day": m[r["code"]].append(r["date"])
    stale = []
    for k in sorted(m):
        a = age(max(m[k]))
        if a is not None and a > 7: stale.append(f"{k} ({a}d)")
    if stale:
        emit("WARN", f"discharge data stale for: {', '.join(stale)} — check the iEasyHydro tunnel")
    else:
        emit("OK", f"discharge data current for all {len(m)} stations")
PY
    RC_ST=$?
    if [ "$RC_ST" -ne 0 ]; then
        bad "data freshness (short-term forecasts/discharge) check crashed — python exited $RC_ST: $(head -1 "$ERR_ST" 2>/dev/null)"
    fi
    dispatch_verdicts "$VERDICTS_ST"
    if [ "$RC_ST" -eq 0 ] && [ "$VERDICT_COUNT" -eq 0 ]; then
        bad "data freshness (short-term forecasts/discharge) check produced no output — could not be evaluated"
    fi

    # Long-term (monthly/seasonal) forecasting is optional per deployment, so
    # detect it before judging it — otherwise a deployment that never runs
    # long-term forecasting would get a false alarm here, which is exactly
    # the failure mode this script exists to avoid. The gate variable
    # `ieasyhydroforecast_ml_long_term_configuration` is required whenever
    # long-term is used (commented out otherwise — see apps/config/.env_develop
    # and doc/configuration.md), so its presence in the env is the signal.
    if [ -n "$ENV_FILE" ] && [ -f "$ENV_FILE" ] \
       && grep -qE '^ieasyhydroforecast_ml_long_term_configuration=.+' "$ENV_FILE" 2>/dev/null; then
        # Same pagination trap as lr-forecast above (offset/limit, no ORDER
        # BY) — bound by start_date so an arbitrary old page can't masquerade
        # as "no recent data". Without start_date, an unbounded limit=N on a
        # large table returns the oldest rows, not the newest, and max(date)
        # would report a healthy long-term pipeline as months stale.
        #
        # Long-term forecasts are issued monthly/seasonally on deployment-
        # configured issue days (e.g. the 10th and 25th on one deployment,
        # the 1st on another) — not every 5-10 days like short-term.
        #
        # The DUE-DATE computation below needs only ~1 month of lookback
        # regardless of cadence (it is derived from the calendar + the
        # configured issue days, not from the window; see the day-31 note in
        # most_recent_issue_day below). But this window also bounds what the
        # "latest on file" lookup can SEE, and that lookup must cover the
        # SLOWEST cadence this script now groups by horizon_type: a single
        # seasonal mode (e.g. a deployment with only "seasonal_april", not
        # KGHM's four seasonal_* months) may legitimately produce output only
        # once a year. 120 days would make that mode's genuinely-current
        # last run invisible for 8+ months, reporting "no forecasts in the
        # window" — technically still correct (not-yet-due months report OK
        # regardless, via the "nothing scheduled this month" branch below),
        # but needlessly alarming-looking next to an "OK" and useless as
        # evidence during an overdue month. 400 days (a full year + ~5 weeks
        # margin for run-day slip) guarantees a once-a-year mode's last real
        # run is always inside the window, so "nothing published in the
        # window" stays a meaningful signal rather than a window-sizing
        # artifact — it remains the right test, just needs sizing to the
        # true slowest cadence rather than the short-term-derived 120 days.
        # Still bounded (not unbounded) for the same pagination-trap reason
        # as lr-forecast above, and still doubles as the flat-staleness
        # fallback's window when issue days can't be resolved at all.
        #
        # `limit` raised from 5000 to 20000 alongside the window widening:
        # the no-ORDER-BY offset/limit trap bites harder the wider the
        # window gets, because a longer start_date span can hold more total
        # rows, making it MORE likely an unordered page of `limit` rows
        # misses the genuinely newest ones for one or more horizon types —
        # confirmed empirically against this repo's own (heavily-seeded, not
        # production-representative) local dev DB: start_date=2025-01-01
        # with limit=5000 returned an arbitrary 5000-row slice whose month
        # max(date) was months staler, and whose quarter rows were absent
        # entirely, versus the true values visible only above ~75000. A
        # real deployment's actual row volume in a 400-day window cannot be
        # verified here (station counts and discharge/forecast data are not
        # committed to this repo), so 20000 (matching this script's existing
        # runoff/?limit=20000 call above) is a judgment call, not a proof —
        # if a deployment's real volume ever exceeds it, this inherits the
        # same silent-truncation risk the pre-existing endpoints already
        # have. The actual fix is adding ORDER BY to the API endpoint, which
        # lives in sapphire/services/ and is out of scope for this script.
        LT_SINCE=$(date -d '400 days ago' +%F 2>/dev/null || date -v-400d +%F 2>/dev/null)
        LTF_JSON="$HC_TMPDIR/ltf.json"
        VERDICTS_LT="$HC_TMPDIR/verdicts_lt"
        ERR_LT="$HC_TMPDIR/err_lt"
        curl -s --max-time 30 "http://localhost:8003/long-forecast/?start_date=${LT_SINCE}&limit=20000" 2>/dev/null > "$LTF_JSON"

        # Resolve the per-mode operational issue days so staleness can be
        # judged against when a run was actually DUE, same as the short-term
        # block above, instead of only a flat day count.
        # `ieasyhydroforecast_ml_long_term_configuration` is a DIRECTORY NAME
        # (apps/long_term_forecasting/config_forecast.py:39,55 joins it onto
        # the config root), not a path itself, so it must be joined onto the
        # config directory. That root is awkward to resolve from here via its
        # own env var (config_forecast.py:38 reads
        # `ieasyhydroforecast_configuration_path`, a path that is itself
        # relative, e.g. `../../../<data>/config` — see
        # doc/prod/long_term_recovery_runbook.md:148). This script already
        # knows the config directory without resolving that relative path at
        # all: $ENV_FILE lives at <data>/config/.env_develop_*, so
        # dirname($ENV_FILE) IS that same directory (confirmed against
        # sapphire/.env_kghm, where `ieasyforecast_configuration_path` and
        # `ieasyhydroforecast_configuration_path` are both set to the
        # identical value).
        LT_SUBDIR=$(grep -E '^ieasyhydroforecast_ml_long_term_configuration=' "$ENV_FILE" | tail -1 | cut -d= -f2- | tr -d '\r')
        LT_MODES=$(grep -E '^ieasyhydroforecast_ml_long_term_supported_modes=' "$ENV_FILE" | tail -1 | cut -d= -f2- | tr -d '\r')
        LT_CONFIG_DIR=""
        [ -n "$LT_SUBDIR" ] && LT_CONFIG_DIR="$(dirname "$ENV_FILE")/$LT_SUBDIR"
        # The PARENT of LT_CONFIG_DIR -- needed separately below because
        # model_folder resolves against it, not against LT_CONFIG_DIR itself
        # (see the comment at mode_active_this_month's model_folder join).
        # dirname($ENV_FILE) already equals that parent (same reasoning as
        # LT_CONFIG_DIR above); computed directly here rather than via
        # os.path.dirname(LT_CONFIG_DIR) in python, which would silently
        # give the wrong answer if the configured subdirectory name were
        # ever itself nested or carried a trailing slash.
        LT_CONFIG_ROOT="$(dirname "$ENV_FILE")"

        # Same fix as the short-term block above: emit "LEVEL<TAB>message"
        # instead of printing the coloured label directly, so the verdict
        # flows through ok/warn/bad and the exit code stays authoritative.
        HC_LTF_JSON="$LTF_JSON" HC_LT_CONFIG_DIR="$LT_CONFIG_DIR" HC_LT_CONFIG_ROOT="$LT_CONFIG_ROOT" HC_LT_MODES="$LT_MODES" HC_LT_SINCE_DAYS=400 \
            python3 - > "$VERDICTS_LT" 2>"$ERR_LT" << 'PY'
import calendar, datetime, json, os


def load(p):
    try: return json.load(open(p))
    except Exception: return []


# Test-only clock override: see the matching note on the short-term block
# above (HC_FAKE_NOW). Interpreted as LOCAL time there too.
_fake_now = os.environ.get("HC_FAKE_NOW")
now = datetime.datetime.fromisoformat(_fake_now) if _fake_now else datetime.datetime.now()
# Same LOCAL-hour convention as the short-term block above — see the
# comment there (cron reads crontab hours in the server's own local
# timezone; doc/deployment.md's "Set up cron job" section, ~line 927, has
# operators pick the local hour that lands the run in the morning local
# time). Duplicated rather than imported because this runs in its own
# `python3` subprocess, separate from the short-term block's — keep the
# two in step if the convention ever changes.
now_local_hour = now.hour
today = now.date()
LTF_WINDOW_DAYS = int(os.environ.get("HC_LT_SINCE_DAYS", "400"))


def age(ds):
    try: return (today - datetime.date.fromisoformat(ds[:10])).days
    except Exception: return None


def emit(level, msg): print(f"{level}\t{msg}")


ltf = load(os.environ["HC_LTF_JSON"])
latest = max((r.get("date", "") for r in ltf), default=None)
latest_age = age(latest) if latest else None


def fallback(reason):
    # The original flat 120-day-window / 45-day-warn behaviour, used
    # whenever the due-date path below could not be evaluated. Must never
    # itself WARN just because configuration was unreadable — a deployment
    # that runs long-term fine but stores its config somewhere unexpected
    # must not start failing its health check — so this degrades to the
    # same check that ran before this due-date logic existed.
    note = (f" [due-date check unavailable: {reason}; "
            f"using {LTF_WINDOW_DAYS}-day staleness check instead]")
    if latest is None:
        emit("WARN", "no long-term forecasts in the last "
             f"{LTF_WINDOW_DAYS} days — long-term forecasting is configured "
             f"but appears to have stopped publishing{note}")
    else:
        level = "OK" if latest_age is not None and latest_age <= 45 else "WARN"
        emit(level, f"long-term forecasts: latest {latest} ({latest_age} days old){note}")


# --- Resolve per-mode operational_issue_day --------------------------------
# Source of truth: apps/long_term_forecasting/config_forecast.py
# (get_operational_issue_day, ~line 227) and
# apps/long_term_forecasting/lt_schedule_query.py (NON_OPERATIONAL_MODES,
# ~line 57). This health check cannot import apps/ (different interpreter
# and paths — same reason as the short-term block above), so the rule is
# reimplemented here from the on-disk mode JSONs directly — keep in sync if
# the long-term schedule config shape ever changes.
#
# NON_OPERATIONAL_MODES: "monthly" is deliberately kept in
# ieasyhydroforecast_ml_long_term_supported_modes by some deployments so
# other tooling can reference it, but lt_schedule_query.py excludes it from
# scheduling — it is never an operationally issued mode. Treating its
# configured day as a due date would invent a deadline nothing is ever
# expected to meet, exactly the false-alarm failure mode this check exists
# to avoid.
NON_OPERATIONAL_MODES = {"monthly"}

config_dir = os.environ.get("HC_LT_CONFIG_DIR", "")
# PARENT of config_dir, passed in separately from the shell (not derived
# here with os.path.dirname(config_dir) -- see mode_active_this_month's
# model_folder join for why that would be the wrong computation to make in
# python).
config_root = os.environ.get("HC_LT_CONFIG_ROOT", "")
modes = [m.strip() for m in os.environ.get("HC_LT_MODES", "").split(",") if m.strip()]
operational_modes = [m for m in modes if m not in NON_OPERATIONAL_MODES]


def as_int_days(value):
    """operational_issue_day is annotated list[int] in config_forecast.py,
    but get_operational_issue_day() does int(...) on it, so in production it
    is a single int. Handle both shapes defensively — a deployment config
    may legitimately carry either."""
    if isinstance(value, list):
        return [int(v) for v in value]
    return [int(value)]


mode_configs = {}
mode_issue_days = {}
fail_reason = None

if not config_dir:
    fail_reason = "ieasyhydroforecast_ml_long_term_configuration not set in env"
elif not os.path.isdir(config_dir):
    fail_reason = f"config directory {config_dir} not found"
elif not operational_modes:
    fail_reason = "no operational long-term modes in ieasyhydroforecast_ml_long_term_supported_modes"
else:
    for mode in operational_modes:
        mode_path = os.path.join(config_dir, f"{mode}.json")
        try:
            with open(mode_path) as f:
                mode_config = json.load(f)
        except Exception as e:
            fail_reason = f"{mode}.json unreadable/invalid ({e.__class__.__name__})"
            break
        if "operational_issue_day" not in mode_config:
            fail_reason = f"{mode}.json missing 'operational_issue_day'"
            break
        try:
            mode_issue_days[mode] = as_int_days(mode_config["operational_issue_day"])
        except Exception as e:
            fail_reason = f"{mode}.json 'operational_issue_day' not parseable as int ({e.__class__.__name__})"
            break
        mode_configs[mode] = mode_config
    if fail_reason is None and not mode_issue_days:
        fail_reason = "no operational_issue_day values resolved from any configured mode"

# --- Per-mode forecast_months gating ---------------------------------------
# Source of truth: apps/long_term_forecasting/lt_schedule_query.py:107-124
# (query_schedule's "any_model_scheduled" check) and
# apps/long_term_forecasting/config_forecast.py:278-288 (get_forecast_months
# default). A mode's operational_issue_day fires monthly in cron regardless
# of whether the mode's models actually produce anything that month (e.g. a
# seasonal mode restricted to April-September) — counting it as "due" every
# month would warn every single off-month, exactly the recurring false alarm
# this check exists to avoid. So before a mode's issue day is allowed into
# the due-date union, at least one of its models must be scheduled in the
# CURRENT calendar month.
#
# Deliberately NOT replicated: lt_schedule_query.ISSUE_DAY_TOLERANCE (a
# +/-N-day window around the issue day, currently 10, commented there as
# "temporarily relaxed ... must be changed back to 5 for operational use").
# That number is in flux and owned by lt_schedule_query.py; re-deriving it
# here would make this script a second, competing schedule authority — see
# doc/plans/issues/high_prio_gi_draft_infra_validate_pipeline_gated_day_false_fail.md
# (option (b)) for why that pattern is explicitly flagged as a problem. This
# check only asks "is this calendar month in forecast_months", not "is today
# within N days of the issue day".
ALL_MONTHS = set(range(1, 13))
current_month = today.month


def mode_active_this_month(mode, mode_config):
    """Return (active, fail_reason). `active` is only meaningful when
    fail_reason is None. Mirrors query_schedule's per-model scan: a mode is
    active this month if ANY of its models is unrestricted or lists the
    current month in forecast_months.

    An unreadable/malformed model config is NOT evidence either way -- but
    unlike an unreadable operational_issue_day (which discards the one
    thing this check has no substitute for), a failure here degrades only
    THIS mode's gating, not the issue days already resolved for every other
    mode. See the caller: a mode this function can't evaluate is folded in
    as active (the conservative choice -- see the comment there)."""
    if "model_folder" not in mode_config or "models_to_use" not in mode_config:
        return False, f"{mode}.json missing 'model_folder' or 'models_to_use' (needed to check forecast_months)"
    models_to_use = mode_config["models_to_use"]
    if not isinstance(models_to_use, dict):
        return False, f"{mode}.json 'models_to_use' is not a family->model map"
    # model_folder resolves against config_root (the CONFIG directory), NOT
    # against config_dir (the long_term_configs SUBdirectory the mode JSONs
    # themselves live in) -- genuinely surprising, but that is what the real
    # code does: config_forecast.py:38 CONFIG_PATH = the config directory;
    # :55 LT_forecast_configs = CONFIG_PATH/LT_CONFIGS (== config_dir here,
    # where <mode>.json is read from); :72 model_folder =
    # os.path.join(CONFIG_PATH, model_folder) -- CONFIG_PATH, one level up
    # from LT_forecast_configs, not LT_forecast_configs itself. Using
    # config_dir here instead of config_root silently resolved to a path one
    # directory too deep and made every model_folder lookup fail, defeating
    # the forecast_months gating entirely (confirmed on the KGHM server).
    mode_model_root = os.path.join(config_root, mode_config["model_folder"])
    for family, model_names in models_to_use.items():
        if not isinstance(model_names, list):
            return False, f"{mode}.json 'models_to_use[{family}]' is not a list"
        for model_name in model_names:
            general_config_path = os.path.join(mode_model_root, family, model_name, "general_config.json")
            try:
                with open(general_config_path) as f:
                    general_config = json.load(f)
            except Exception as e:
                return False, (f"{mode}/{family}/{model_name}/general_config.json "
                                f"unreadable/invalid ({e.__class__.__name__})")
            forecast_months = general_config.get("forecast_months")
            # Absent, or covering all twelve months, means NO restriction
            # (config_forecast.py:286-288's default) -- active regardless of
            # which month it is.
            if not forecast_months or set(forecast_months) >= ALL_MONTHS:
                return True, None
            if current_month in set(forecast_months):
                return True, None
    # Every model's config was read successfully; none scheduled this month.
    # This is a legitimate "not due", not a failure.
    return False, None


# The issue days resolved above and the forecast_months gating below are two
# INDEPENDENT things that must fail independently. Issue days are the part
# this check has no substitute for (that's what `fallback()` is for, already
# handled above via `fail_reason`). Gating is a refinement on top of that --
# one mode's model tree being absent on a given host (observed in practice:
# a seasonal/monthly mode present in
# ieasyhydroforecast_ml_long_term_supported_modes without a corresponding
# models_and_scalers/ tree on that deployment) must not discard every issue
# day that WAS read correctly and drop the whole section to the old flat
# staleness window -- that threw away the valuable, successfully-resolved
# part over a failure in the refinement.
#
# So a gating failure for one mode does not touch fail_reason / fallback()
# at all. Instead: that mode is folded in as ACTIVE (its issue day counts),
# and the first such failure PER HORIZON is recorded so that horizon's
# verdict says gating could not be fully applied, rather than silently
# guessing. This is deliberately the conservative direction: an unreadable
# mode could in fact be seasonal and not due this month, so treating it as
# active can produce a false WARN -- but the alternative (treating it as
# inactive) risks the opposite, silently hiding a genuinely missed run. A
# visible, explained warning is recoverable (an operator reads the note and
# checks); a silently dropped issue day is not. This check exists to catch
# missed runs, so that asymmetry is why "active" is the default here, not
# "inactive".
mode_active = {}
mode_gating_reason = {}
if fail_reason is None:
    for mode in mode_issue_days:
        active, reason = mode_active_this_month(mode, mode_configs[mode])
        mode_gating_reason[mode] = reason
        mode_active[mode] = True if reason is not None else active  # conservative: can't tell -> count it

if fail_reason is not None:
    fallback(fail_reason)
else:
    # --- Group by horizon type ----------------------------------------
    # Source of truth for why this grouping matters: the long_forecasts
    # table (sapphire/services/postprocessing/app/models.py HorizonType,
    # ~line 21-29) holds three distinct horizons -- "month", "quarter",
    # "season" -- produced by different modes on different schedules. A
    # single flat max(date)/single due-date across all of them lets a
    # current monthly forecast mask a dead seasonal or quarterly pipeline
    # (e.g. month_1 ran yesterday, seasonal_april hasn't run since April --
    # the old flat check would still say OK). Each mode declares its own
    # horizon via get_horizon_type() (config_forecast.py:269-276, default
    # "month" when the key is absent) -- confirmed on the KGHM server:
    # quarter.json -> "quarter", seasonal_april.json -> "season",
    # month_1.json -> absent -> "month" (the documented default). The API
    # rows carry the identical lowercase strings in their own horizon_type
    # field (HorizonType enum values), so grouping both sides on that raw
    # string needs no translation table.
    modes_by_horizon = {}
    for mode in mode_issue_days:
        h = mode_configs[mode].get("horizon_type") or "month"
        modes_by_horizon.setdefault(h, []).append(mode)

    ltf_by_horizon = {}
    for r in ltf:
        h = r.get("horizon_type", "?")
        ltf_by_horizon.setdefault(h, []).append(r)

    configured_horizons = set(modes_by_horizon)
    # Rows for a horizon no configured mode declares are not evidence of
    # anything this check can judge (no schedule to compare against) --
    # ignore them for verdict purposes, but say so once rather than letting
    # them silently vanish from the output.
    unconfigured = sorted(set(ltf_by_horizon) - configured_horizons)
    if unconfigured:
        parts = ", ".join(f"{h!r} ({len(ltf_by_horizon[h])} row(s))" for h in unconfigured)
        emit("OK", f"long-term forecasts: ignoring rows with horizon_type {parts} -- not declared by any configured mode's horizon_type")

    # Only qualify the message with "(horizon)" when there is more than one
    # to disambiguate -- a single-horizon deployment's output must stay
    # exactly as before (same wording the companion document already shows).
    multi_horizon = len(configured_horizons) > 1

    GRACE_CUTOFF_HOUR = 12  # same convention/value as the short-term block above

    def issue_days_in_month(d, issue_days):
        # Day-of-month issue days that can actually occur in THIS month — a
        # mode configured for day 31 simply never fires in a 30-day month
        # (or February), exactly like cron's own day-of-month field: no
        # wrap, no clamp, the run is just skipped that month. The recursion
        # in most_recent_issue_day below then looks to the previous month,
        # matching that real-world behaviour rather than inventing an
        # end-of-month substitute day the production schedule does not use.
        last_day = calendar.monthrange(d.year, d.month)[1]
        return sorted(dd for dd in issue_days if dd <= last_day)

    def most_recent_issue_day(d, issue_days, _depth=0):
        if _depth > 36:  # ~3 years of recursion guard; issue_days is non-empty here
            return None
        for b in reversed(issue_days_in_month(d, issue_days)):
            if b <= d.day:
                return datetime.date(d.year, d.month, b)
        prev_month_last = d.replace(day=1) - datetime.timedelta(days=1)
        return most_recent_issue_day(prev_month_last, issue_days, _depth + 1)

    def expected_issue_day(issue_days):
        """The issue day whose long-term forecast should already exist,
        given today's local calendar date and the current local hour."""
        candidate = most_recent_issue_day(today, issue_days)
        if candidate == today and now_local_hour < GRACE_CUTOFF_HOUR:
            # Still within today's grace window on an issue day: today's run
            # may not have happened yet, so the forecast actually due is
            # still the previous issue day's.
            candidate = most_recent_issue_day(today - datetime.timedelta(days=1), issue_days)
        return candidate

    def emit_horizon_verdict(label, h, modes_h):
        active_modes_h = [m for m in modes_h if mode_active[m]]
        reason_h = next((mode_gating_reason[m] for m in modes_h if mode_gating_reason[m] is not None), None)
        gating_note = f" (seasonal gating unavailable: {reason_h})" if reason_h else ""
        rows_h = ltf_by_horizon.get(h, [])
        latest_h = max((r.get("date", "") for r in rows_h), default=None)
        latest_age_h = age(latest_h) if latest_h else None

        if not active_modes_h:
            # Every configured mode in this horizon read fine (reason_h is
            # None whenever active_modes_h is empty -- a gating failure
            # always counts its mode as active), and forecast_months
            # excludes all of them this month: genuinely nothing is due
            # right now. Report OK, not a staleness WARN for a run that was
            # never going to happen this month.
            note = f" (forecast_months excludes month {current_month} for every configured mode)"
            if latest_h is not None:
                emit("OK", f"{label} forecasts: no long-term run scheduled this month{note}; latest on file is {latest_h} ({latest_age_h} days old)")
            else:
                emit("OK", f"{label} forecasts: no long-term run scheduled this month{note}; no forecasts in the last {LTF_WINDOW_DAYS} days either")
            return

        issue_days_h = set()
        for m in active_modes_h:
            issue_days_h.update(mode_issue_days[m])

        due = expected_issue_day(issue_days_h)
        if due is None:
            # Not reachable in practice (active_modes_h non-empty guarantees
            # issue_days_h non-empty, so the recursion above always finds a
            # match well within its depth guard) -- kept as a labelled,
            # non-crashing safety net rather than an unhandled exception.
            emit("WARN", f"{label} forecasts: could not resolve a due date from issue days {sorted(issue_days_h)} (unexpected)")
        elif latest_h is None:
            emit("WARN", f"{label} forecasts: no forecasts in the last {LTF_WINDOW_DAYS} days, but a run was due {due.isoformat()}{gating_note}")
        else:
            try:
                latest_date = datetime.date.fromisoformat(latest_h[:10])
            except Exception:
                latest_date = None
            if latest_date is not None and latest_date >= due:
                # `latest_age_h` is shown for operator context only — the
                # due-date comparison above is what decides OK/WARN, not age.
                emit("OK", f"{label} forecasts: latest {latest_h} ({latest_age_h} days old){gating_note}")
            else:
                emit("WARN", f"{label} forecasts: newest is {latest_h}, but a run was due {due.isoformat()}{gating_note}")

    for h in sorted(configured_horizons):
        label = f"long-term ({h})" if multi_horizon else "long-term"
        emit_horizon_verdict(label, h, modes_by_horizon[h])
PY
        RC_LT=$?
        if [ "$RC_LT" -ne 0 ]; then
            bad "data freshness (long-term forecasts) check crashed — python exited $RC_LT: $(head -1 "$ERR_LT" 2>/dev/null)"
        fi
        dispatch_verdicts "$VERDICTS_LT"
        if [ "$RC_LT" -eq 0 ] && [ "$VERDICT_COUNT" -eq 0 ]; then
            bad "data freshness (long-term forecasts) check produced no output — could not be evaluated"
        fi
    else
        skip "long-term forecasting not configured on this deployment (ieasyhydroforecast_ml_long_term_configuration not set in env)"
    fi
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
