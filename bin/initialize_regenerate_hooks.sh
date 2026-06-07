#!/usr/bin/env bash
# ============================================================================
# initialize_regenerate_hooks.sh
#
# Update-time migration toolkit — Phase P6 meta-wrapper.
#
# Purpose:
#   Orchestrates the four regenerate / gap-backfill hooks that close coverage
#   gaps after the CSV-source migrations of P1a/P1b/P1c/P2a/P2b/P3/P4a/P4b/P5.
#   All four hooks already exist as standalone scripts in bin/; P6 is a thin
#   orchestrator that adds operational safety (cron-pause, late-start guard)
#   and operator UX (dry-run inventory, default-on / opt-out flag pattern).
#
#   The hooks are (default ON; pass `--skip-hook-<name>` to disable each):
#     1. snow stats         -> bin/backfill_snow_stats_history.sh
#     2. hydrograph M/S     -> bin/yearly_runoff_hydrograph_aggregation.sh
#                              (per-year loop from --start-year to current year)
#     3. short-term skill   -> bin/yearly_skill_metrics_recalculation.sh
#                              (architecture §Q10 named initialize_site_backfill.sh
#                              but that script does not exist on this branch;
#                              yearly_skill_metrics_recalculation.sh is the
#                              functional equivalent for short-term recalc).
#     4. long-term skill    -> bin/bimonthly_long_term_skill_metrics_recalculation.sh
#
# Operational safety contracts (architecture §Q9 + §Q10):
#   - Cron-pause discipline: cron is paused BEFORE any hook runs and RESTORED
#     on EXIT/INT/TERM regardless of which hook failed.  Because
#     bin/utils/common_functions.sh does not currently ship a cron-pause helper,
#     P6 implements the pause/restore locally via a trap-protected sequence
#     that calls `crontab -l` / `crontab` directly. The same approach was used
#     by snow-stats backfill operators historically; centralising it into
#     common_functions.sh is out of P6 file-scope (deferred — see gi_draft).
#   - Late-start guard: refuses to start within --late-start-window-minutes
#     (default 30) of the next cron tick to avoid the cron stomping the
#     half-finished backfill. Opt-out: --allow-late-start.
#
# Fail-fast vs continue-on-error (architecture §Q10):
#   Default: fail-fast. The first hook to exit non-zero aborts the run and
#   restores cron via the trap.
#   Opt-in: --continue-on-error keeps running subsequent hooks even when one
#   fails. The wrapper exit code is non-zero if any hook failed.
#
# Station-filter contract (P0 binding flag):
#   --station-filter is NOT honored by P6 because the four hooks all operate
#   organisation-wide (yearly recalculations across all stations). The flag
#   is intentionally absent from the CLI. The `--help` text documents this so
#   operators porting muscle memory from P1a/P1b/P3/P5 are not surprised.
#
# Usage:
#   bash bin/initialize_regenerate_hooks.sh <env_file_path> [OPTIONS]
#
# Arguments:
#   env_file_path        Path to the .env_<org> file (required).
#
# Options:
#   --dry-run                              Print the inventory without invoking
#                                          any hook script.
#   --start-year <YYYY>                    Forwarded to snow + hydrograph hooks.
#                                          Default: hooks' own defaults apply.
#   --skip-hook-snow-stats                 Skip the snow-stats yearly backfill.
#   --skip-hook-hydrograph-month-season    Skip the hydrograph MONTH+SEASON
#                                          yearly loop.
#   --skip-hook-short-term-skill           Skip the short-term skill recalc.
#   --skip-hook-long-term-skill            Skip the long-term skill recalc.
#   --late-start-window-minutes <N>        Refuse to start within N minutes of
#                                          the next cron tick. Default: 30.
#   --allow-late-start                     Bypass the late-start guard.
#   --continue-on-error                    Continue to next hook on failure.
#                                          Default: fail-fast.
#   -h, --help                             Print this message and exit 0.
#
# Examples:
#   # Dry-run inventory (safe; no DB writes; previews late-start gate).
#   bash bin/initialize_regenerate_hooks.sh /data/taj_data/config/.env_tjhm --dry-run
#
#   # Full run with default cron-pause + late-start guard.
#   bash bin/initialize_regenerate_hooks.sh /data/taj_data/config/.env_tjhm
#
#   # Skip the long-running snow stats hook; run the rest.
#   bash bin/initialize_regenerate_hooks.sh /data/taj_data/config/.env_tjhm \
#       --skip-hook-snow-stats
#
#   # Start year override (forwarded to snow + hydrograph).
#   bash bin/initialize_regenerate_hooks.sh /data/taj_data/config/.env_tjhm \
#       --start-year 2015
#
#   # Operator already inside the late-start window but accepts the risk.
#   bash bin/initialize_regenerate_hooks.sh /data/taj_data/config/.env_tjhm \
#       --allow-late-start
# ============================================================================

set -eo pipefail
# NOTE: set -u is intentionally omitted; common_functions.sh is not strict-mode
# safe (see comment in update_migration_helpers.sh).

# ---------------------------------------------------------------------------
# Source shared helpers (P0 foundation)
# ---------------------------------------------------------------------------
# shellcheck disable=SC1091  # path computed at runtime; can't be statically resolved
source "$(dirname "$0")/utils/update_migration_helpers.sh"

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Defaults / flag variables
# ---------------------------------------------------------------------------
ENV_FILE=""
DRY_RUN=false
START_YEAR=""
SKIP_SNOW_STATS=false
SKIP_HYDROGRAPH_MS=false
SKIP_SHORT_TERM_SKILL=false
SKIP_LONG_TERM_SKILL=false
LATE_START_WINDOW_MIN=30
ALLOW_LATE_START=false
CONTINUE_ON_ERROR=false

# Internal: per-script paths discovered relative to this wrapper. Recomputed in
# main() so the test suite (which invokes the wrapper via subprocess.run from
# arbitrary working dirs) sees consistent paths.
HOOK_SNOW_STATS=""
HOOK_HYDROGRAPH_MS=""
HOOK_SHORT_TERM_SKILL=""
HOOK_LONG_TERM_SKILL=""

# Internal: cron-pause state. Populated by _pause_cron and consumed by
# _restore_cron via the trap.
_CRON_BACKUP_PATH=""
_CRON_WAS_PAUSED=false

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/initialize_regenerate_hooks.sh <env_file_path> [OPTIONS]

Run the four regenerate / gap-backfill hooks under cron-pause discipline +
late-start guard. Each hook is default-ON; pass `--skip-hook-<name>` to
opt-out per-hook.

Hooks (in execution order):
  1. snow-stats                  -> backfill_snow_stats_history.sh
  2. hydrograph-month-season     -> yearly_runoff_hydrograph_aggregation.sh
                                    (per-year loop; --start-year forwarded)
  3. short-term-skill            -> yearly_skill_metrics_recalculation.sh
  4. long-term-skill             -> bimonthly_long_term_skill_metrics_recalculation.sh

Arguments:
  env_file_path                          Path to the .env_<org> file (required).

Options:
  --dry-run                              Print the inventory without invoking
                                         any hook script. No cron-pause, no
                                         late-start abort -- safe to run any
                                         time.
  --start-year <YYYY>                    Forwarded to the snow + hydrograph
                                         hooks. Default: each hook's own
                                         default applies.
  --skip-hook-snow-stats                 Skip the snow-stats yearly backfill.
  --skip-hook-hydrograph-month-season    Skip the hydrograph MONTH+SEASON
                                         per-year loop.
  --skip-hook-short-term-skill           Skip the short-term skill recalc.
  --skip-hook-long-term-skill            Skip the long-term skill recalc.
  --late-start-window-minutes <N>        Refuse to start within N minutes of
                                         the next cron tick. Default: 30.
                                         Set to 0 to disable.
  --allow-late-start                     Bypass the late-start guard.
  --continue-on-error                    Keep running subsequent hooks when
                                         one fails. Default: fail-fast.
  -h, --help                             Print this message and exit 0.

Station-filter contract (informational):
  This wrapper does NOT honor the binding `--station-filter` flag from P1a/
  P1b/P1c/P3/P5 because all four hooks operate organisation-wide (yearly
  recalculations across every station). The flag is intentionally absent.

Examples:
  bash bin/initialize_regenerate_hooks.sh /data/taj_data/config/.env_tjhm --dry-run
  bash bin/initialize_regenerate_hooks.sh /data/taj_data/config/.env_tjhm
  bash bin/initialize_regenerate_hooks.sh /data/taj_data/config/.env_tjhm \
      --skip-hook-snow-stats --start-year 2015
  bash bin/initialize_regenerate_hooks.sh /data/taj_data/config/.env_tjhm \
      --allow-late-start --continue-on-error
USAGE
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

parse_args() {
    # Handle --help / -h before requiring the positional arg.
    for arg in "$@"; do
        case "$arg" in
            -h|--help) print_usage; exit 0 ;;
        esac
    done

    if [[ $# -eq 0 ]]; then
        echo -e "${RED}Error: env_file_path is required.${NC}" >&2
        echo "" >&2
        print_usage >&2
        exit 1
    fi

    # First positional argument is the env file.
    if [[ "$1" != -* ]]; then
        ENV_FILE="$1"
        shift
    else
        echo -e "${RED}Error: first argument must be the env_file_path (got: $1).${NC}" >&2
        echo "" >&2
        print_usage >&2
        exit 1
    fi

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --start-year)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --start-year requires a YYYY value.${NC}" >&2
                    exit 1
                fi
                if ! [[ "$2" =~ ^[0-9]{4}$ ]]; then
                    echo -e "${RED}Error: --start-year must be a four-digit year, got '$2'.${NC}" >&2
                    exit 1
                fi
                START_YEAR="$2"
                shift 2
                ;;
            --skip-hook-snow-stats)
                SKIP_SNOW_STATS=true
                shift
                ;;
            --skip-hook-hydrograph-month-season)
                SKIP_HYDROGRAPH_MS=true
                shift
                ;;
            --skip-hook-short-term-skill)
                SKIP_SHORT_TERM_SKILL=true
                shift
                ;;
            --skip-hook-long-term-skill)
                SKIP_LONG_TERM_SKILL=true
                shift
                ;;
            --late-start-window-minutes)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --late-start-window-minutes requires a numeric value.${NC}" >&2
                    exit 1
                fi
                if ! [[ "$2" =~ ^[0-9]+$ ]]; then
                    echo -e "${RED}Error: --late-start-window-minutes must be a non-negative integer, got '$2'.${NC}" >&2
                    exit 1
                fi
                LATE_START_WINDOW_MIN="$2"
                shift 2
                ;;
            --allow-late-start)
                ALLOW_LATE_START=true
                shift
                ;;
            --continue-on-error)
                CONTINUE_ON_ERROR=true
                shift
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                echo -e "${RED}Error: unknown argument: $1${NC}" >&2
                echo "" >&2
                print_usage >&2
                exit 1
                ;;
        esac
    done
}

# ---------------------------------------------------------------------------
# Cron-pause / restore.
#
# Why this lives in the wrapper rather than common_functions.sh:
#   common_functions.sh does not currently ship a cron-pause helper. Adding
#   one would change a shared helper and would expand P6's file-scope. The
#   gi_draft documents the gap so a follow-up issue can extract this into a
#   reusable helper after P6 lands.
#
# Behaviour:
#   _pause_cron writes the current `crontab -l` output to a temp file and
#   replaces the crontab with an empty file. On EXIT/INT/TERM, _restore_cron
#   re-installs the saved crontab from the backup file.
#
#   If `crontab` is not installed, or the user has no crontab, the pause is a
#   no-op (logged) and _CRON_WAS_PAUSED stays false so the trap is also a
#   no-op.
# ---------------------------------------------------------------------------
_pause_cron() {
    if ! command -v crontab >/dev/null 2>&1; then
        umh_log_redacted "cron-pause: crontab(1) not installed; skipping pause/restore"
        return 0
    fi

    local cron_dump_rc=0
    local cron_dump_out
    cron_dump_out="$(crontab -l 2>&1)" || cron_dump_rc=$?

    # `crontab -l` exits 1 when no crontab exists. Distinguish that benign
    # case from a real error.
    if [[ $cron_dump_rc -ne 0 ]]; then
        if echo "$cron_dump_out" | grep -qiE "no crontab|no such"; then
            umh_log_redacted "cron-pause: no crontab installed for user; skipping pause/restore"
            return 0
        fi
        umh_log_redacted "cron-pause: WARNING crontab -l failed (${cron_dump_rc}); skipping pause"
        return 0
    fi

    if [[ -z "$cron_dump_out" ]]; then
        umh_log_redacted "cron-pause: crontab is empty; skipping pause/restore"
        return 0
    fi

    # Stash the backup under the temp workspace if available; otherwise mktemp.
    if [[ -n "${TMPDIRS[*]:-}" ]]; then
        _CRON_BACKUP_PATH="${TMPDIRS[0]}/crontab_backup.txt"
    else
        _CRON_BACKUP_PATH="$(mktemp -t crontab_backup.XXXXXX)"
    fi
    printf '%s\n' "$cron_dump_out" > "$_CRON_BACKUP_PATH"
    chmod 600 "$_CRON_BACKUP_PATH"

    # Install an empty crontab.
    if printf '' | crontab -; then
        _CRON_WAS_PAUSED=true
        umh_log_redacted "cron-pause: paused user crontab (backup saved to a private temp file)"
    else
        umh_log_redacted "cron-pause: WARNING failed to install empty crontab; pause aborted"
        _CRON_BACKUP_PATH=""
    fi
}

_restore_cron() {
    if [[ "$_CRON_WAS_PAUSED" != true ]]; then
        return 0
    fi
    if [[ -z "$_CRON_BACKUP_PATH" || ! -f "$_CRON_BACKUP_PATH" ]]; then
        umh_log_redacted "cron-restore: WARNING backup file missing; cannot restore"
        return 0
    fi
    if crontab "$_CRON_BACKUP_PATH"; then
        umh_log_redacted "cron-restore: user crontab restored"
    else
        umh_log_redacted "cron-restore: ERROR failed to restore crontab from ${_CRON_BACKUP_PATH}"
        umh_log_redacted "cron-restore: manual recovery: crontab \"${_CRON_BACKUP_PATH}\""
    fi
}

# ---------------------------------------------------------------------------
# Late-start guard.
#
# Reads the current crontab via `crontab -l` and computes the minutes until
# the next tick. If the next tick is within --late-start-window-minutes and
# --allow-late-start is NOT set, abort.
#
# Notes:
# - We parse only minute and hour fields; day-of-month / month / day-of-week
#   are treated as wildcards for the purposes of the window. This is
#   intentionally conservative — a more granular parser would be brittle.
# - When crontab is missing or empty, we log a WARN and proceed.
# - The computation is done in Python (already required by P0 helpers); the
#   logic is simple enough to keep inline.
# ---------------------------------------------------------------------------
_minutes_to_next_cron_tick() {
    if ! command -v crontab >/dev/null 2>&1; then
        echo ""
        return 0
    fi
    local cron_out
    if ! cron_out="$(crontab -l 2>/dev/null)"; then
        echo ""
        return 0
    fi
    if [[ -z "$cron_out" ]]; then
        echo ""
        return 0
    fi
    python3 - <<PYEOF
import sys
from datetime import datetime, timedelta

# Read crontab lines from stdin (passed via heredoc + here-string below).
cron_text = """$cron_out"""

def expand_field(field, lo, hi):
    """Return the sorted set of values matching one cron field over [lo, hi]."""
    out = set()
    for token in field.split(","):
        token = token.strip()
        step = 1
        if "/" in token:
            base, step_str = token.split("/", 1)
            try:
                step = int(step_str)
            except ValueError:
                continue
        else:
            base = token
        if base == "*":
            start, end = lo, hi
        elif "-" in base:
            try:
                a, b = base.split("-", 1)
                start, end = int(a), int(b)
            except ValueError:
                continue
        else:
            try:
                v = int(base)
                start, end = v, v
            except ValueError:
                continue
        if start < lo or end > hi or start > end:
            continue
        out.update(range(start, end + 1, step))
    return sorted(out)

def parse_lines(text):
    """Yield (minutes_set, hours_set) for each schedule line."""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        minute_set = expand_field(parts[0], 0, 59)
        hour_set = expand_field(parts[1], 0, 23)
        if minute_set and hour_set:
            yield set(minute_set), set(hour_set)

now = datetime.now()
min_delta = None
for minutes, hours in parse_lines(cron_text):
    # Scan up to 48 hours ahead.
    for offset in range(0, 60 * 48):
        ts = now + timedelta(minutes=offset)
        if ts.minute in minutes and ts.hour in hours:
            # Skip "now" itself; we want the NEXT tick.
            if offset == 0:
                continue
            if min_delta is None or offset < min_delta:
                min_delta = offset
            break
print(min_delta if min_delta is not None else "")
PYEOF
}

_check_late_start_guard() {
    if [[ "$LATE_START_WINDOW_MIN" -le 0 ]]; then
        umh_log_redacted "late-start guard: disabled (window=${LATE_START_WINDOW_MIN})"
        return 0
    fi

    local minutes
    minutes="$(_minutes_to_next_cron_tick 2>/dev/null || true)"
    minutes="${minutes//[$'\r\n ']/}"

    if [[ -z "$minutes" ]]; then
        umh_log_redacted "late-start guard: could not determine next cron tick (no crontab or parse failure); proceeding"
        return 0
    fi

    umh_log_redacted "late-start guard: next cron tick in ${minutes} minute(s) (window=${LATE_START_WINDOW_MIN})"

    if [[ "$minutes" -lt "$LATE_START_WINDOW_MIN" ]]; then
        if [[ "$ALLOW_LATE_START" == true ]]; then
            umh_log_redacted "late-start guard: WITHIN window (${minutes}<${LATE_START_WINDOW_MIN}); --allow-late-start set, proceeding"
            return 0
        fi
        if [[ "$DRY_RUN" == true ]]; then
            umh_log_redacted "late-start guard: WITHIN window (${minutes}<${LATE_START_WINDOW_MIN}); --dry-run exempt, proceeding"
            return 0
        fi
        umh_log_redacted "ERROR: late-start guard tripped — next cron tick in ${minutes} minute(s),"
        umh_log_redacted "       which is within the ${LATE_START_WINDOW_MIN}-minute guard window."
        umh_log_redacted ""
        umh_log_redacted "Resolution — pick ONE:"
        umh_log_redacted "  (a) Postpone until after the next cron tick."
        umh_log_redacted "  (b) Pass --allow-late-start to proceed at your own risk."
        umh_log_redacted "  (c) Pass --late-start-window-minutes 0 to disable the guard."
        exit 1
    fi

    umh_log_redacted "late-start guard: OK (${minutes}>=${LATE_START_WINDOW_MIN})"
}

# ---------------------------------------------------------------------------
# Per-hook invocation builders.
#
# Each builder echoes the exact command line that would be executed. The
# dry-run path prints these; the real run executes them.
# ---------------------------------------------------------------------------
_build_snow_stats_cmd() {
    local args=()
    args+=("$HOOK_SNOW_STATS")
    if [[ -n "$START_YEAR" ]]; then
        args+=("--start-year" "$START_YEAR")
    fi
    printf '%q ' "${args[@]}"
}

_build_hydrograph_year_cmd() {
    local year="$1"
    printf '%q ' "$HOOK_HYDROGRAPH_MS" "$ENV_FILE" "--target-year" "$year"
}

_build_short_term_skill_cmd() {
    printf '%q ' "$HOOK_SHORT_TERM_SKILL" "$ENV_FILE"
}

_build_long_term_skill_cmd() {
    printf '%q ' "$HOOK_LONG_TERM_SKILL" "$ENV_FILE"
}

# Compute the per-year hydrograph range [start, end].
# start defaults to current_year - 5 when --start-year is not supplied (a
# conservative recent-window — operators wanting the full archive must pass
# --start-year explicitly).
_hydrograph_year_range() {
    local current_year
    current_year="$(date +%Y)"
    local start
    if [[ -n "$START_YEAR" ]]; then
        start="$START_YEAR"
    else
        start=$((current_year - 5))
    fi
    if [[ "$start" -gt "$current_year" ]]; then
        echo ""
        return 0
    fi
    seq "$start" "$current_year"
}

# ---------------------------------------------------------------------------
# Dry-run inventory printer.
# ---------------------------------------------------------------------------
_print_dry_run_inventory() {
    umh_log_redacted "========================================"
    umh_log_redacted " DRY RUN — inventory only, no hook script invoked"
    umh_log_redacted "========================================"

    umh_log_redacted "Hook execution plan (default-on; opt-out via --skip-hook-<name>):"
    umh_log_redacted ""

    # 1. snow-stats
    if [[ "$SKIP_SNOW_STATS" == true ]]; then
        umh_log_redacted "  [SKIP]  hook 1/4: snow-stats (--skip-hook-snow-stats)"
    elif [[ ! -x "$HOOK_SNOW_STATS" && ! -f "$HOOK_SNOW_STATS" ]]; then
        umh_log_redacted "  [MISS]  hook 1/4: snow-stats — script not found at ${HOOK_SNOW_STATS}; will skip at run-time"
    else
        umh_log_redacted "  [RUN]   hook 1/4: snow-stats"
        umh_log_redacted "          cmd: ieasyhydroforecast_env_file_path=${ENV_FILE} bash $(_build_snow_stats_cmd)"
    fi

    # 2. hydrograph month-season
    if [[ "$SKIP_HYDROGRAPH_MS" == true ]]; then
        umh_log_redacted "  [SKIP]  hook 2/4: hydrograph-month-season (--skip-hook-hydrograph-month-season)"
    elif [[ ! -x "$HOOK_HYDROGRAPH_MS" && ! -f "$HOOK_HYDROGRAPH_MS" ]]; then
        umh_log_redacted "  [MISS]  hook 2/4: hydrograph-month-season — script not found at ${HOOK_HYDROGRAPH_MS}; will skip at run-time"
    else
        umh_log_redacted "  [RUN]   hook 2/4: hydrograph-month-season (per-year loop)"
        local year
        local years
        years="$(_hydrograph_year_range)"
        if [[ -z "$years" ]]; then
            umh_log_redacted "          (no years in range; nothing to do)"
        else
            while IFS= read -r year; do
                [[ -z "$year" ]] && continue
                umh_log_redacted "          cmd: bash $(_build_hydrograph_year_cmd "$year")"
            done <<< "$years"
        fi
    fi

    # 3. short-term-skill
    if [[ "$SKIP_SHORT_TERM_SKILL" == true ]]; then
        umh_log_redacted "  [SKIP]  hook 3/4: short-term-skill (--skip-hook-short-term-skill)"
    elif [[ ! -x "$HOOK_SHORT_TERM_SKILL" && ! -f "$HOOK_SHORT_TERM_SKILL" ]]; then
        umh_log_redacted "  [MISS]  hook 3/4: short-term-skill — script not found at ${HOOK_SHORT_TERM_SKILL}; will skip at run-time"
    else
        umh_log_redacted "  [RUN]   hook 3/4: short-term-skill"
        umh_log_redacted "          cmd: bash $(_build_short_term_skill_cmd)"
    fi

    # 4. long-term-skill
    if [[ "$SKIP_LONG_TERM_SKILL" == true ]]; then
        umh_log_redacted "  [SKIP]  hook 4/4: long-term-skill (--skip-hook-long-term-skill)"
    elif [[ ! -x "$HOOK_LONG_TERM_SKILL" && ! -f "$HOOK_LONG_TERM_SKILL" ]]; then
        umh_log_redacted "  [MISS]  hook 4/4: long-term-skill — script not found at ${HOOK_LONG_TERM_SKILL}; will skip at run-time"
    else
        umh_log_redacted "  [RUN]   hook 4/4: long-term-skill"
        umh_log_redacted "          cmd: bash $(_build_long_term_skill_cmd)"
    fi

    umh_log_redacted ""
    umh_log_redacted "Pause/restore plan:"
    umh_log_redacted "  - pause: 'crontab -' (empty crontab) before any hook runs"
    umh_log_redacted "  - restore: 'crontab <backup_file>' on EXIT/INT/TERM"
    umh_log_redacted "  - dry-run does NOT actually pause cron"
    umh_log_redacted ""
    umh_log_redacted "Late-start window: ${LATE_START_WINDOW_MIN} minute(s)$( [[ "$ALLOW_LATE_START" == true ]] && echo " (--allow-late-start: would bypass)" )"
    umh_log_redacted "Fail policy:        $( [[ "$CONTINUE_ON_ERROR" == true ]] && echo "continue-on-error" || echo "fail-fast" )"
}

# ---------------------------------------------------------------------------
# Hook runners (real run path).
#
# Each returns the underlying script's exit code. The main loop applies the
# fail-fast / continue-on-error policy.
# ---------------------------------------------------------------------------
_run_snow_stats_hook() {
    if [[ "$SKIP_SNOW_STATS" == true ]]; then
        umh_log_redacted "snow-stats: SKIPPED (--skip-hook-snow-stats)"
        return 0
    fi
    if [[ ! -f "$HOOK_SNOW_STATS" ]]; then
        umh_log_redacted "snow-stats: WARNING script not found at ${HOOK_SNOW_STATS}; skipping gracefully"
        return 0
    fi
    umh_log_redacted "snow-stats: BEGIN ($(_build_snow_stats_cmd))"
    local start_args=()
    if [[ -n "$START_YEAR" ]]; then
        start_args+=("--start-year" "$START_YEAR")
    fi
    local rc=0
    # The script reads ieasyhydroforecast_env_file_path from the environment.
    ieasyhydroforecast_env_file_path="$ENV_FILE" bash "$HOOK_SNOW_STATS" "${start_args[@]}" 2>&1 \
        | tee -a "${log_file:-/dev/null}" \
        || rc=$?
    # NB: the value of $? after `|` is the right-most command (`tee`); use
    # PIPESTATUS for the actual script exit code. The `|| rc=$?` above caught
    # the pipe failure; promote PIPESTATUS for accuracy.
    if [[ ${PIPESTATUS[0]:-0} -ne 0 ]]; then
        rc="${PIPESTATUS[0]}"
    fi
    umh_log_redacted "snow-stats: END (exit=${rc})"
    return "$rc"
}

_run_hydrograph_ms_hook() {
    if [[ "$SKIP_HYDROGRAPH_MS" == true ]]; then
        umh_log_redacted "hydrograph-month-season: SKIPPED (--skip-hook-hydrograph-month-season)"
        return 0
    fi
    if [[ ! -f "$HOOK_HYDROGRAPH_MS" ]]; then
        umh_log_redacted "hydrograph-month-season: WARNING script not found at ${HOOK_HYDROGRAPH_MS}; skipping gracefully"
        return 0
    fi
    local years
    years="$(_hydrograph_year_range)"
    if [[ -z "$years" ]]; then
        umh_log_redacted "hydrograph-month-season: no years in range; nothing to do"
        return 0
    fi
    local total_rc=0
    local year
    while IFS= read -r year; do
        [[ -z "$year" ]] && continue
        umh_log_redacted "hydrograph-month-season: BEGIN year=${year}"
        local rc=0
        bash "$HOOK_HYDROGRAPH_MS" "$ENV_FILE" --target-year "$year" 2>&1 \
            | tee -a "${log_file:-/dev/null}" \
            || rc=$?
        if [[ ${PIPESTATUS[0]:-0} -ne 0 ]]; then
            rc="${PIPESTATUS[0]}"
        fi
        umh_log_redacted "hydrograph-month-season: END year=${year} (exit=${rc})"
        if [[ "$rc" -ne 0 ]]; then
            total_rc="$rc"
            if [[ "$CONTINUE_ON_ERROR" != true ]]; then
                return "$rc"
            fi
        fi
    done <<< "$years"
    return "$total_rc"
}

_run_short_term_skill_hook() {
    if [[ "$SKIP_SHORT_TERM_SKILL" == true ]]; then
        umh_log_redacted "short-term-skill: SKIPPED (--skip-hook-short-term-skill)"
        return 0
    fi
    if [[ ! -f "$HOOK_SHORT_TERM_SKILL" ]]; then
        umh_log_redacted "short-term-skill: WARNING script not found at ${HOOK_SHORT_TERM_SKILL}; skipping gracefully"
        return 0
    fi
    umh_log_redacted "short-term-skill: BEGIN ($(_build_short_term_skill_cmd))"
    local rc=0
    bash "$HOOK_SHORT_TERM_SKILL" "$ENV_FILE" 2>&1 \
        | tee -a "${log_file:-/dev/null}" \
        || rc=$?
    if [[ ${PIPESTATUS[0]:-0} -ne 0 ]]; then
        rc="${PIPESTATUS[0]}"
    fi
    umh_log_redacted "short-term-skill: END (exit=${rc})"
    return "$rc"
}

_run_long_term_skill_hook() {
    if [[ "$SKIP_LONG_TERM_SKILL" == true ]]; then
        umh_log_redacted "long-term-skill: SKIPPED (--skip-hook-long-term-skill)"
        return 0
    fi
    if [[ ! -f "$HOOK_LONG_TERM_SKILL" ]]; then
        umh_log_redacted "long-term-skill: WARNING script not found at ${HOOK_LONG_TERM_SKILL}; skipping gracefully"
        return 0
    fi
    umh_log_redacted "long-term-skill: BEGIN ($(_build_long_term_skill_cmd))"
    local rc=0
    bash "$HOOK_LONG_TERM_SKILL" "$ENV_FILE" 2>&1 \
        | tee -a "${log_file:-/dev/null}" \
        || rc=$?
    if [[ ${PIPESTATUS[0]:-0} -ne 0 ]]; then
        rc="${PIPESTATUS[0]}"
    fi
    umh_log_redacted "long-term-skill: END (exit=${rc})"
    return "$rc"
}

# ---------------------------------------------------------------------------
# Trap: restore cron on any exit path.
# ---------------------------------------------------------------------------
_on_exit() {
    local rc=$?
    _restore_cron
    return "$rc"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    parse_args "$@"

    print_banner
    echo "| Running Regenerate / Gap-Backfill Hooks (P6 meta-wrapper)"

    # Validate env file exists before loading.
    if [[ ! -f "$ENV_FILE" ]]; then
        echo -e "${RED}Error: env file not found: ${ENV_FILE}${NC}" >&2
        exit 1
    fi

    # Load .env vars and derive ieasyhydroforecast_* path variables.
    read_configuration "${ENV_FILE}"

    # Validate required env vars.
    if [[ -z "${ieasyhydroforecast_data_root_dir:-}" ]]; then
        echo -e "${RED}Error: Required environment variable ieasyhydroforecast_data_root_dir is not set. Check your .env file.${NC}" >&2
        exit 1
    fi

    # Discover sibling hook scripts.
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    HOOK_SNOW_STATS="${script_dir}/backfill_snow_stats_history.sh"
    HOOK_HYDROGRAPH_MS="${script_dir}/yearly_runoff_hydrograph_aggregation.sh"
    HOOK_SHORT_TERM_SKILL="${script_dir}/yearly_skill_metrics_recalculation.sh"
    HOOK_LONG_TERM_SKILL="${script_dir}/bimonthly_long_term_skill_metrics_recalculation.sh"

    # Set up log file.
    local LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/regenerate_hooks"
    mkdir -p "${LOG_DIR}"
    local TIMESTAMP
    TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
    log_file="${LOG_DIR}/regenerate_hooks_${TIMESTAMP}.log"
    export log_file

    echo "| Log file: ${log_file}"
    echo ""

    umh_log_redacted "Starting regenerate / gap-backfill hooks orchestrator"
    umh_log_redacted "  env_file:                       ${ENV_FILE}"
    umh_log_redacted "  start_year:                     ${START_YEAR:-<hook default>}"
    umh_log_redacted "  skip_hook_snow_stats:           ${SKIP_SNOW_STATS}"
    umh_log_redacted "  skip_hook_hydrograph_ms:        ${SKIP_HYDROGRAPH_MS}"
    umh_log_redacted "  skip_hook_short_term_skill:     ${SKIP_SHORT_TERM_SKILL}"
    umh_log_redacted "  skip_hook_long_term_skill:      ${SKIP_LONG_TERM_SKILL}"
    umh_log_redacted "  late_start_window_minutes:     ${LATE_START_WINDOW_MIN}"
    umh_log_redacted "  allow_late_start:               ${ALLOW_LATE_START}"
    umh_log_redacted "  continue_on_error:              ${CONTINUE_ON_ERROR}"
    umh_log_redacted "  dry_run:                        ${DRY_RUN}"

    # -------------------------------------------------------------------------
    # Late-start guard. Runs BEFORE cron-pause so the operator can postpone
    # without ever touching cron.
    # -------------------------------------------------------------------------
    _check_late_start_guard

    # -------------------------------------------------------------------------
    # Dry-run branch.
    # -------------------------------------------------------------------------
    if [[ "$DRY_RUN" == true ]]; then
        _print_dry_run_inventory
        echo ""
        echo -e "${YELLOW}DRY RUN complete — no hooks were invoked, cron not paused.${NC}"
        echo "  Re-run without --dry-run to execute."
        exit 0
    fi

    # -------------------------------------------------------------------------
    # Real run: install the exit trap BEFORE pausing cron so any failure of
    # _pause_cron still triggers _restore_cron.
    # -------------------------------------------------------------------------
    trap _on_exit EXIT INT TERM

    _pause_cron

    # -------------------------------------------------------------------------
    # Hook execution loop. Each runner returns the underlying script's exit
    # code. The fail-fast / continue-on-error policy is enforced here.
    # -------------------------------------------------------------------------
    local overall_rc=0
    local hook_rc
    local failed_hooks=()

    for hook_name in snow_stats hydrograph_ms short_term_skill long_term_skill; do
        hook_rc=0
        case "$hook_name" in
            snow_stats)        _run_snow_stats_hook       || hook_rc=$? ;;
            hydrograph_ms)     _run_hydrograph_ms_hook    || hook_rc=$? ;;
            short_term_skill)  _run_short_term_skill_hook || hook_rc=$? ;;
            long_term_skill)   _run_long_term_skill_hook  || hook_rc=$? ;;
        esac

        if [[ "$hook_rc" -ne 0 ]]; then
            failed_hooks+=("${hook_name}=${hook_rc}")
            overall_rc="$hook_rc"
            if [[ "$CONTINUE_ON_ERROR" != true ]]; then
                umh_log_redacted "fail-fast: aborting remaining hooks (use --continue-on-error to override)"
                break
            fi
        fi
    done

    # -------------------------------------------------------------------------
    # Summary.
    # -------------------------------------------------------------------------
    echo ""
    echo -e "${BOLD}========================================"
    echo -e " REGENERATE HOOKS COMPLETE"
    echo -e "========================================${NC}"
    echo ""

    if [[ ${#failed_hooks[@]} -eq 0 ]]; then
        echo -e "${GREEN}All requested hooks completed successfully.${NC}"
    else
        echo -e "${RED}One or more hooks failed: ${failed_hooks[*]}${NC}"
    fi
    echo ""
    echo "Log file: ${log_file}"

    exit "$overall_rc"
}

main "$@"
