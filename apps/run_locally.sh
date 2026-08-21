#!/usr/bin/env bash

# =============================================================================
# SAPPHIRE Local Forecast Pipeline Runner
# =============================================================================
#
# Runs SAPPHIRE forecast modules locally using each module's uv-based .venv,
# following the correct production dependency order.
#
# Daily run (recommended — does everything):
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#     bash apps/run_locally.sh daily
#
# Operational usage (individual horizon):
#   SAPPHIRE_PREDICTION_MODE=PENTAD \
#     ieasyhydroforecast_env_file_path=/path/to/.env \
#     bash apps/run_locally.sh short-term
#
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#     bash apps/run_locally.sh long-term
#
#   # Long-term operational (full pipeline with run_forecast.py, months 0-9):
#   ieasyhydroforecast_env_file_path=/path/to/.env \
#     bash apps/run_locally.sh long-term-operational
#
#   # Long-term with specific years/months/modes:
#   LT_SIMULATE_YEARS="2024 2025" LT_SIMULATE_NUM_MONTHS=3 \
#     ieasyhydroforecast_env_file_path=/path/to/.env \
#     bash apps/run_locally.sh long-term
#
#   # Long-term full coverage (all 10 modes):
#   LT_SIMULATE_MODES="0 1 2 3 4 5 6 7 8 9" \
#     ieasyhydroforecast_env_file_path=/path/to/.env \
#     bash apps/run_locally.sh long-term
#
# Maintenance usage (gap-fill, hindcast, recalculation):
#   SAPPHIRE_PREDICTION_MODE=BOTH \
#     ieasyhydroforecast_env_file_path=/path/to/.env \
#     bash apps/run_locally.sh maintenance
#
#   bash apps/run_locally.sh maintenance:linear_regression
#   bash apps/run_locally.sh maintenance:postprocessing_forecasts
#   bash apps/run_locally.sh maintenance:postprocessing_long_term
#   bash apps/run_locally.sh recalculate_skill_metrics
#   bash apps/run_locally.sh recalculate_snow_norms
#   bash apps/run_locally.sh calibrate_long_term
#
# Combined targets:
#   daily                                Short-term daily + long-term (gated by day-of-month)
#
# Operational targets:
#   short-term                          Short-term forecast pipeline
#   long-term                           Long-term forecast pipeline (simulate mode, for testing)
#   long-term-operational               Full long-term pipeline (preprocessing + run_forecast.py months 0-9 + postprocess)
#   all                                 Both pipelines
#   <module>                            Single module by name (with per-module validation)
#
# Maintenance targets:
#   maintenance                              All maintenance tasks
#   maintenance:preprocessing_runoff         Runoff gap-filling + long-horizon hydrograph
#   maintenance:preprocessing_gateway        Extend ERA5 reanalysis data
#   maintenance:linear_regression            Linear regression hindcast
#   maintenance:machine_learning             ML NaN recalc + gap-fill + new stations
#   maintenance:postprocessing_forecasts     Fill missing ensemble forecasts
#   maintenance:postprocessing_long_term     Fill missing monthly ensemble forecasts
#   calibrate_long_term                      Calibrate and hindcast long-term models
#   recalculate_skill_metrics                Full skill metrics rebuild (yearly)
#   recalculate_snow_norms                   Yearly snow norm recalculation
#   yearly                                   All yearly tasks (snow norms + skill metrics)
#
# Flags:
#   --continue-on-error   Don't abort on first module failure
#   --dry-run             Validate environment and venvs without running
#   --help                Show full help message
#
# Per-module validation:
#   When running a single module (e.g. run_locally.sh linear_regression),
#   API data validation runs automatically after the module finishes.
#   Only that module's data is checked (Tier 1 + Tier 2).
#   For standalone use: validate_pipeline.py --module <module_name>
#
# machine_learning mode resolution (bare target vs. maintenance/daily):
#   The bare `machine_learning` target has no outer per-mode loop, so it
#   resolves SAPPHIRE_PREDICTION_MODE/ML_MODE itself, via
#   resolve_ml_bare_target_modes: unset derives the mode from ML_MODE (WARN);
#   SAPPHIRE_PREDICTION_MODE=BOTH loops over PENTAD and DECAD, but each pass
#   is still filtered through should_skip_ml_for_mode against ML_MODE
#   (default DECAD) -- so SAPPHIRE_PREDICTION_MODE=BOTH alone only runs
#   DECAD. To actually run both horizons, ALSO set ML_MODE=BOTH (which by
#   itself, even with SAPPHIRE_PREDICTION_MODE unset, is sufficient to run
#   both); an explicit single-valued SAPPHIRE_PREDICTION_MODE that conflicts
#   with a single-valued ML_MODE errors out naming both variables rather than
#   silently picking one. `maintenance:machine_learning`, `daily`, `all` and
#   `maintenance` instead resolve the mode via their own outer per-mode loop
#   (which also honours ML_MODE) before calling into machine_learning, so an
#   operator moving between the bare target and these should not assume the
#   two resolve a given SAPPHIRE_PREDICTION_MODE/ML_MODE combination the same
#   way.
#
# Prerequisites:
#   - Bash 4.4+ (macOS ships 3.2; install via: brew install bash)
#   - Each module needs a .venv: cd apps/<module> && uv sync --all-extras
#   - A valid .env file for your organization
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="${LOG_DIR}/run_locally_${TIMESTAMP}.log"

# Flags (set by argument parsing)
CONTINUE_ON_ERROR=false
DRY_RUN=false

# Set true by the CONTINUE_ON_ERROR guard idiom itself whenever it aborts a
# pipeline function with CONTINUE_ON_ERROR=false. This is a fact recorded at
# the abort site, not an inference from the target name -- used to gate the
# continue-on-error hint.
PIPELINE_ABORTED=false

# Invocation-time mode env vars, captured before anything in main() (the
# per-mode dispatch loops, or the default applied to ML_MODE below) can
# mutate/resolve them, so emit_continue_on_error_hint can reproduce the
# operator's original horizon in its printed retry command. Read-only after
# capture; never used for pipeline logic itself -- see the capture sites
# (main()'s first line, and just above ML_MODE's default below).
INVOCATION_SAPPHIRE_PREDICTION_MODE=""
INVOCATION_ML_MODE=""

# Error capture
ERROR_TAIL_LINES=30
ERROR_DIR="$(mktemp -d)"
trap 'rm -rf "$ERROR_DIR"' EXIT

# Tracking arrays (pipeline modules)
declare -a RESULTS_MODULE=()
declare -a RESULTS_STATUS=()
declare -a RESULTS_TIME=()
declare -a RESULTS_ERROR_LOG=()

# Tracking arrays (API validation — reported separately in summary)
declare -a VALIDATION_MODULE=()
declare -a VALIDATION_STATUS=()
declare -a VALIDATION_TIME=()
declare -a VALIDATION_ERROR_LOG=()

# Short-term pipeline modules (in dependency order)
SHORT_TERM_MODULES=(
    preprocessing_runoff
    preprocessing_gateway
    linear_regression
    machine_learning
    postprocessing_forecasts
)

# Valid single-module targets
ALL_MODULES=(
    preprocessing_runoff
    preprocessing_gateway
    linear_regression
    machine_learning
    postprocessing_forecasts
    long_term_forecasting
)

# ML models and scripts
ML_MODELS=(TFT TIDE TSMIXER)
ML_SCRIPTS=(
    recalculate_nan_forecasts.py
    make_forecast.py
    fill_ml_gaps.py
)

# ML maintenance scripts (no make_forecast.py — only gap-fill and recalc)
ML_MAINTENANCE_SCRIPTS=(
    recalculate_nan_forecasts.py
    fill_ml_gaps.py
    add_new_station.py
)

# Machine learning only runs for DECAD mode (PENTAD forecasts use LR only).
# Override with ML_MODE=BOTH to restore old behavior.
# INVOCATION_ML_MODE captures whether the operator actually set ML_MODE,
# before this line's own default overwrites it -- ML_MODE is never mutated
# again after this point, so this is the only place that distinction is
# still observable. Used only by emit_continue_on_error_hint.
INVOCATION_ML_MODE="${ML_MODE:-}"
ML_MODE="${ML_MODE:-DECAD}"

# Modules with maintenance modes
MAINTENANCE_MODULES=(
    preprocessing_runoff
    preprocessing_gateway
    linear_regression
    machine_learning
    postprocessing_forecasts
)

# Organization-aware module skip list.
# Demo org only needs: preprocessing_runoff, linear_regression, postprocessing_forecasts.
# Matches Docker image pull logic in bin/utils/pull_docker_images.sh.
# ORG is resolved in resolve_org() — it reads from the shell environment first,
# then falls back to extracting ieasyhydroforecast_organization from the .env file.
ORG=""
DEMO_SKIP_MODULES=(preprocessing_gateway machine_learning long_term_forecasting)
UZHM_SKIP_MODULES=(preprocessing_gateway machine_learning long_term_forecasting)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

log() {
    local level="$1"
    shift
    local msg="$*"
    local ts
    ts="$(date '+%Y-%m-%d %H:%M:%S')"
    local line="[${ts}] [${level}] ${msg}"

    # Color based on level. printf '%b' expands the ANSI escapes in the
    # color variables (BLUE/GREEN/YELLOW/RED/NC); '%s' leaves $line itself
    # literal so backslash sequences inside a logged message (e.g. a path
    # containing \x27 or \c, as emit_continue_on_error_hint's shell-quoted
    # hint can) are never reinterpreted -- `echo -e` would otherwise undo
    # shell_quote's escaping. (INFRA-037 round-6 review)
    case "$level" in
        INFO)  printf '%b%s%b\n' "$BLUE" "$line" "$NC" ;;
        OK)    printf '%b%s%b\n' "$GREEN" "$line" "$NC" ;;
        WARN)  printf '%b%s%b\n' "$YELLOW" "$line" "$NC" ;;
        ERROR) printf '%b%s%b\n' "$RED" "$line" "$NC" ;;
        *)     printf '%s\n' "$line" ;;
    esac

    # Also write to log file (without color codes), literal for the same
    # reason as above.
    printf '%s\n' "$line" >> "$LOG_FILE"
}

banner() {
    local msg="$1"
    local sep="$(printf '=%.0s' {1..60})"
    log INFO "$sep"
    log INFO "$msg"
    log INFO "$sep"
}

get_timestamp() {
    date +%s
}

# shell_quote - single-quote a string so it is safe to paste verbatim into a
# POSIX shell, regardless of its content (spaces, semicolons, backticks,
# $(...) substitutions, embedded single quotes, ...). Standard technique:
# close the current single-quoted string, emit an escaped literal quote,
# reopen. Used by emit_continue_on_error_hint so the command it prints can
# never execute injected content when an operator copy-pastes it.
shell_quote() {
    local s="$1"
    printf "'%s'" "${s//\'/\'\\\'\'}"
}

format_duration() {
    local seconds=$1
    if (( seconds >= 3600 )); then
        printf "%dh %dm %ds" $((seconds/3600)) $((seconds%3600/60)) $((seconds%60))
    elif (( seconds >= 60 )); then
        printf "%dm %ds" $((seconds/60)) $((seconds%60))
    else
        printf "%ds" "$seconds"
    fi
}

# Check if today's day-of-month is within ±5 days of any long-term issue day.
# Uses LT_FORECAST_TODAY override if set, otherwise today's real date.
# Sets LT_GATE_DAY, LT_GATE_ISSUE_DAYS, and LT_ACTIVE_WINDOW (nearest
# matching issue day, e.g. "10" or "25") for use in log messages and
# window-aware pipeline logic.
is_lt_issue_window() {
    local issue_days="${LT_OPERATIONAL_ISSUE_DAYS:-10 25}"
    LT_GATE_ISSUE_DAYS="$issue_days"
    LT_ACTIVE_WINDOW=""

    local today_dom
    if [ -n "${LT_FORECAST_TODAY:-}" ]; then
        # Extract day from YYYY-MM-DD (macOS then GNU fallback)
        today_dom=$(date -j -f "%Y-%m-%d" "${LT_FORECAST_TODAY}" "+%d" 2>/dev/null \
                    || date -d "${LT_FORECAST_TODAY}" "+%d" 2>/dev/null)
        today_dom=$((10#$today_dom))
    else
        today_dom=$((10#$(date "+%d")))
    fi
    LT_GATE_DAY="$today_dom"

    local best_day=""
    local best_diff=999
    for issue_day in $issue_days; do
        local diff=$(( today_dom - issue_day ))
        if [ $diff -lt 0 ]; then diff=$(( -diff )); fi
        # Also check wrap-around (e.g., day 1 near day 28+)
        local wrap_diff=$(( 30 - diff ))
        if [ $wrap_diff -lt $diff ]; then diff=$wrap_diff; fi
        if [ $diff -le 5 ] && [ $diff -lt $best_diff ]; then
            best_diff=$diff
            best_day=$issue_day
        fi
    done

    if [ -n "$best_day" ]; then
        LT_ACTIVE_WINDOW="$best_day"
        return 0
    fi
    return 1
}

# Query the Python schedule helper to determine which long-term modes
# are active today.  Sets LT_ACTIVE_MODES (space-separated mode names),
# LT_SKILL_METRIC_TYPES (space-separated), and LT_ACTIVE_WINDOW (non-empty
# if any modes are active).  Falls back to is_lt_issue_window() on failure.
query_lt_schedule() {
    local today_arg=""
    if [ -n "${LT_FORECAST_TODAY:-}" ]; then
        today_arg="--today ${LT_FORECAST_TODAY}"
    fi

    local json_output
    json_output=$(run_in_venv long_term_forecasting lt_schedule_query.py \
        -- $today_arg 2>/dev/null) || {
        log WARN "lt_schedule_query.py failed, falling back to is_lt_issue_window"
        if is_lt_issue_window; then
            LT_ACTIVE_MODES="${LT_OPERATIONAL_MODES:-month_0 month_1 month_2 month_3 month_4 month_5 month_6 month_7 month_8 month_9}"
            if [ "$LT_ACTIVE_WINDOW" = "25" ]; then
                LT_SKILL_METRIC_TYPES="MONTHLY QUARTERLY SEASONAL"
            else
                LT_SKILL_METRIC_TYPES="MONTHLY"
            fi
        else
            LT_ACTIVE_MODES=""
            LT_SKILL_METRIC_TYPES=""
            LT_ACTIVE_WINDOW=""
        fi
        return 0
    }

    # Parse JSON output — run_in_venv contaminates stdout with log lines;
    # the JSON is always the last line, so extract it with tail.
    LT_ACTIVE_MODES=$(echo "$json_output" | tail -n 1 | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(' '.join(d.get('active_modes', [])))
" 2>/dev/null) || LT_ACTIVE_MODES=""

    LT_SKILL_METRIC_TYPES=$(echo "$json_output" | tail -n 1 | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(' '.join(d.get('skill_metric_types', [])))
" 2>/dev/null) || LT_SKILL_METRIC_TYPES=""

    if [ -n "$LT_ACTIVE_MODES" ]; then
        LT_ACTIVE_WINDOW="active"  # non-empty signals active
    else
        LT_ACTIVE_WINDOW=""
    fi

    log INFO "Schedule query: active_modes=[${LT_ACTIVE_MODES}] skill_types=[${LT_SKILL_METRIC_TYPES}]"
}

record_result() {
    local module="$1"
    local status="$2"
    local elapsed="$3"
    local error_log="${4:-}"
    RESULTS_MODULE+=("$module")
    RESULTS_STATUS+=("$status")
    RESULTS_TIME+=("$elapsed")
    RESULTS_ERROR_LOG+=("$error_log")
}

record_validation() {
    local module="$1"
    local status="$2"
    local elapsed="$3"
    local error_log="${4:-}"
    VALIDATION_MODULE+=("$module")
    VALIDATION_STATUS+=("$status")
    VALIDATION_TIME+=("$elapsed")
    VALIDATION_ERROR_LOG+=("$error_log")
}

# emit_continue_on_error_hint - Tell the operator why a pipeline phase
# stopped and how to run past the failure anyway. Called exactly once from
# main(), after the target's pipeline function has already returned, gated
# on PIPELINE_ABORTED (see its declaration above) -- a fact recorded by the
# CONTINUE_ON_ERROR guard idiom itself, not inferred from the target name.
#
# This fires whether or not the failing step happened to be the last
# guarded step in its enclosing pipeline function (e.g. `yearly`'s final
# step, run_recalculate_skill_metrics, has nothing after it to skip), and
# whether or not the *target* as a whole stopped (e.g. `all` continues into
# the long-term pipeline and aggregate validation after a short-term abort
# -- only the short-term phase itself stopped). So the wording below must
# not claim any modules remained unrun, and must scope "stopped" to the
# phase that aborted, not to the whole run. (INFRA-037)
#
# The printed command interpolates ieasyhydroforecast_env_file_path so it is
# actually copy-pasteable: validate_env REQUIRES that variable to be set to
# an existing file, or the script exits 1 before ever reaching
# --continue-on-error's effect (round-4 review finding). The value comes
# from the current run's own environment, which validate_env has already
# echoed as "Env file: ..." earlier in this same run's output, so including
# it here discloses nothing new.
#
# Every interpolated value is passed through shell_quote so the printed line
# is safe to paste verbatim regardless of its content (spaces, `;`,
# backticks, $(...) -- round-5 review finding 1a/1b). The script path uses
# SCRIPT_DIR (an absolute path computed once at the top of this file) rather
# than a literal "apps/run_locally.sh", so the line also works when the
# operator's shell is not cwd'd at the repo root (finding 1b).
#
# SAPPHIRE_PREDICTION_MODE and ML_MODE are included via
# INVOCATION_SAPPHIRE_PREDICTION_MODE / INVOCATION_ML_MODE -- captured
# before this run's own dispatch loops could mutate/resolve them (see their
# declarations near the top of this file) -- so a resumed non-daily target
# such as `short-term` reruns the SAME horizon the operator originally
# invoked, not whatever SAPPHIRE_PREDICTION_MODE happens to hold at the
# moment the aborting phase returned (round-5 review finding 1c). Each is
# only emitted when the operator actually set it, so an unset mode does not
# turn into a spurious empty assignment. LT_FORECAST_TODAY remains
# deliberately left out -- it is never mutated mid-run the way
# SAPPHIRE_PREDICTION_MODE is, so omitting it was never the defect; adding
# it would still turn this into a general-purpose command reconstructor
# rather than a targeted fix for what round-5 actually flagged.
emit_continue_on_error_hint() {
    local target="$1"
    local env_file="${ieasyhydroforecast_env_file_path:-}"
    # Resolve a relative env path to absolute so the printed hint still
    # works when pasted from a different cwd (same class of defect already
    # fixed for the script path via SCRIPT_DIR above). validate_env has
    # already confirmed this file exists, so cd'ing into its directory is
    # safe; portable (no GNU readlink -f) for macOS. An already-absolute
    # path is left untouched. Does not change what validate_env itself
    # accepts. (INFRA-037 round-6 review)
    case "$env_file" in
        /*|"") ;;
        *) env_file="$(cd "$(dirname "$env_file")" && pwd)/$(basename "$env_file")" ;;
    esac
    local prefix=""
    [ -n "$env_file" ] && prefix="ieasyhydroforecast_env_file_path=$(shell_quote "$env_file") "
    [ -n "$INVOCATION_SAPPHIRE_PREDICTION_MODE" ] && \
        prefix="${prefix}SAPPHIRE_PREDICTION_MODE=$(shell_quote "$INVOCATION_SAPPHIRE_PREDICTION_MODE") "
    [ -n "$INVOCATION_ML_MODE" ] && \
        prefix="${prefix}ML_MODE=$(shell_quote "$INVOCATION_ML_MODE") "
    local cmd
    cmd="bash $(shell_quote "${SCRIPT_DIR}/run_locally.sh") --continue-on-error $(shell_quote "$target")"
    log WARN "A pipeline phase stopped at its first failing module because --continue-on-error is not set."
    log WARN "To continue past failures instead of stopping at the first, run: ${prefix}${cmd}"
    log WARN "Note: even with --continue-on-error, this run will still exit non-zero — it does not make a failing run look successful."
}

check_venv() {
    local module="$1"
    local python_path="${SCRIPT_DIR}/${module}/.venv/bin/python"
    if [ ! -f "$python_path" ]; then
        return 1
    fi
    return 0
}

should_skip_module() {
    local module="$1"
    if [ "$ORG" = "demo" ]; then
        for skip in "${DEMO_SKIP_MODULES[@]}"; do
            [ "$module" = "$skip" ] && return 0
        done
    elif [ "$ORG" = "uzhm" ]; then
        for skip in "${UZHM_SKIP_MODULES[@]}"; do
            [ "$module" = "$skip" ] && return 0
        done
    fi
    return 1
}

should_skip_ml_for_mode() {
    local current_mode="$1"
    # ML_MODE=BOTH means run ML for every mode (legacy behavior)
    [ "$ML_MODE" = "BOTH" ] && return 1
    [ "$current_mode" != "$ML_MODE" ]
}

# resolve_ml_bare_target_modes - Validate SAPPHIRE_PREDICTION_MODE and ML_MODE
# for the bare `machine_learning` single-module target, then populate the
# global ML_BARE_RESOLVED_MODES array with the mode(s) to run.
#
# Unlike the daily/maintenance loops, the bare target has no outer mode loop
# to resolve SAPPHIRE_PREDICTION_MODE for it, so it must validate and resolve
# both variables itself instead of silently forwarding an empty mode or
# silently discarding an explicit request via should_skip_ml_for_mode.
#
# Exits the script (exit 1) on invalid or mutually inconsistent input, since
# this is only ever called from that one case branch before anything has run.
resolve_ml_bare_target_modes() {
    local requested_mode="${SAPPHIRE_PREDICTION_MODE:-}"

    case "$requested_mode" in
        ""|PENTAD|DECAD|BOTH) ;;
        *)
            log ERROR "Invalid SAPPHIRE_PREDICTION_MODE for machine_learning target: '${requested_mode}' (expected unset/empty, PENTAD, DECAD, or BOTH)"
            exit 1
            ;;
    esac

    case "$ML_MODE" in
        PENTAD|DECAD|BOTH) ;;
        *)
            log ERROR "Invalid ML_MODE for machine_learning target: '${ML_MODE}' (expected PENTAD, DECAD, or BOTH)"
            exit 1
            ;;
    esac

    ML_BARE_RESOLVED_MODES=()

    if [ "$requested_mode" = "BOTH" ]; then
        local mode
        for mode in PENTAD DECAD; do
            if should_skip_ml_for_mode "$mode"; then
                log INFO "Skipping machine_learning for ${mode} (ML_MODE=${ML_MODE})"
            else
                ML_BARE_RESOLVED_MODES+=("$mode")
            fi
        done
    elif [ -n "$requested_mode" ]; then
        if [ "$ML_MODE" = "BOTH" ] || [ "$ML_MODE" = "$requested_mode" ]; then
            ML_BARE_RESOLVED_MODES=("$requested_mode")
        else
            log ERROR "Inconsistent ML mode request for machine_learning target: SAPPHIRE_PREDICTION_MODE=${requested_mode} but ML_MODE=${ML_MODE}"
            exit 1
        fi
    else
        if [ "$ML_MODE" = "BOTH" ]; then
            log WARN "SAPPHIRE_PREDICTION_MODE not set for machine_learning target; deriving (PENTAD DECAD) from ML_MODE=BOTH"
            ML_BARE_RESOLVED_MODES=(PENTAD DECAD)
        else
            log WARN "SAPPHIRE_PREDICTION_MODE not set for machine_learning target; deriving ${ML_MODE} from ML_MODE=${ML_MODE}"
            ML_BARE_RESOLVED_MODES=("$ML_MODE")
        fi
    fi
}

resolve_org() {
    # 1. Shell environment takes precedence
    if [ -n "${ieasyhydroforecast_organization:-}" ]; then
        ORG="$ieasyhydroforecast_organization"
        return
    fi
    # 2. Extract from .env file if available
    local env_file="${ieasyhydroforecast_env_file_path:-}"
    if [ -n "$env_file" ] && [ -f "$env_file" ]; then
        local from_file
        from_file=$(grep -m1 '^ieasyhydroforecast_organization=' "$env_file" \
                    | cut -d'=' -f2 | tr -d '[:space:]"'"'")
        if [ -n "$from_file" ]; then
            ORG="$from_file"
            return
        fi
    fi
    # 3. Not set — run all modules (production default)
    ORG=""
}

# ---------------------------------------------------------------------------
# run_in_venv - Central executor
# ---------------------------------------------------------------------------
# Runs a Python script inside a module's .venv in a subshell.
# Usage: run_in_venv <module> <script> [extra_env_vars...] [-- script_args...]
#
# Extra env vars are passed as KEY=VALUE arguments before --.
# Script arguments come after --.

run_in_venv() {
    local module="$1"
    local script="$2"
    shift 2

    local extra_env=()
    local script_args=()
    local past_separator=false

    for arg in "$@"; do
        if [ "$arg" = "--" ]; then
            past_separator=true
            continue
        fi
        if [ "$past_separator" = true ]; then
            script_args+=("$arg")
        else
            extra_env+=("$arg")
        fi
    done

    local module_dir="${SCRIPT_DIR}/${module}"
    local python_path="${module_dir}/.venv/bin/python"

    if [ ! -f "$python_path" ]; then
        log ERROR "No .venv/bin/python found for ${module}"
        return 1
    fi

    log INFO "  Running: ${module}/${script} ${script_args[*]:-}"

    # Build env command
    local env_cmd=(
        env
        "ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path:-}"
        "SAPPHIRE_PREDICTION_MODE=${SAPPHIRE_PREDICTION_MODE:-}"
    )
    for ev in ${extra_env[@]+"${extra_env[@]}"}; do
        env_cmd+=("$ev")
    done

    # Build tee targets: always the main log, plus per-module log if set
    local tee_targets=("$LOG_FILE")
    if [ -n "${CURRENT_MODULE_LOG:-}" ]; then
        tee_targets+=("$CURRENT_MODULE_LOG")
    fi

    # Run in subshell from the module directory
    (
        cd "$module_dir"
        "${env_cmd[@]}" "$python_path" "$script" ${script_args[@]+"${script_args[@]}"} 2>&1
    ) | tee -a "${tee_targets[@]}"

    return "${PIPESTATUS[0]}"
}

# ---------------------------------------------------------------------------
# Module runner functions
# ---------------------------------------------------------------------------

run_preprocessing_runoff() {
    banner "Module: preprocessing_runoff"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_runoff.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv preprocessing_runoff preprocessing_runoff.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "preprocessing_runoff completed in $(format_duration $elapsed)"
        record_result "preprocessing_runoff" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "preprocessing_runoff failed (exit $rc) after $(format_duration $elapsed)"
        record_result "preprocessing_runoff" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_preprocessing_gateway() {
    banner "Module: preprocessing_gateway"
    local start
    start=$(get_timestamp)
    local rc=0

    CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_gateway.log"
    > "$CURRENT_MODULE_LOG"
    for script in Quantile_Mapping_OP.py extend_era5_reanalysis.py snow_data_operational.py; do
        run_in_venv preprocessing_gateway "$script" || { rc=$?; break; }
    done

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "preprocessing_gateway completed in $(format_duration $elapsed)"
        record_result "preprocessing_gateway" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "preprocessing_gateway failed (exit $rc) after $(format_duration $elapsed)"
        record_result "preprocessing_gateway" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_linear_regression() {
    banner "Module: linear_regression"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/linear_regression.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv linear_regression linear_regression.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "linear_regression completed in $(format_duration $elapsed)"
        record_result "linear_regression" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "linear_regression failed (exit $rc) after $(format_duration $elapsed)"
        record_result "linear_regression" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_machine_learning() {
    banner "Module: machine_learning"
    local start
    start=$(get_timestamp)
    local rc=0

    CURRENT_MODULE_LOG="${ERROR_DIR}/machine_learning.log"
    > "$CURRENT_MODULE_LOG"
    for model in "${ML_MODELS[@]}"; do
        log INFO "  Model: ${model}"
        for script in "${ML_SCRIPTS[@]}"; do
            run_in_venv machine_learning "$script" \
                "SAPPHIRE_MODEL_TO_USE=${model}" \
                "SAPPHIRE_CONSISTENCY_CHECK=${SAPPHIRE_CONSISTENCY_CHECK:-false}" \
                || { rc=$?; break 2; }
        done
    done

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "machine_learning completed in $(format_duration $elapsed)"
        record_result "machine_learning" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "machine_learning failed (exit $rc) after $(format_duration $elapsed)"
        record_result "machine_learning" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_postprocessing_forecasts() {
    banner "Module: postprocessing_forecasts (operational)"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/postprocessing_forecasts.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv postprocessing_forecasts postprocessing_operational.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "postprocessing_forecasts completed in $(format_duration $elapsed)"
        record_result "postprocessing_forecasts" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "postprocessing_forecasts failed (exit $rc) after $(format_duration $elapsed)"
        record_result "postprocessing_forecasts" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_long_term_forecasting() {
    banner "Module: long_term_forecasting (simulate)"
    local start
    start=$(get_timestamp)
    local rc=0
    local any_failed=false

    # Operational run_forecast.py only works on predefined forecast dates,
    # so we use simulate_forecasts.py instead — it sets a historical "today"
    # and exercises the same code path.
    local sim_years="${LT_SIMULATE_YEARS:-2024}"
    local sim_num_months="${LT_SIMULATE_NUM_MONTHS:-1}"

    CURRENT_MODULE_LOG="${ERROR_DIR}/long_term_forecasting.log"
    > "$CURRENT_MODULE_LOG"
    # If lt_forecast_mode is set, run just that mode
    if [ -n "${lt_forecast_mode:-}" ]; then
        log INFO "  Running single mode: ${lt_forecast_mode} (years=${sim_years}, num_months=${sim_num_months})"
        run_in_venv long_term_forecasting dev_code/simulate_forecasts.py \
            "lt_forecast_mode=${lt_forecast_mode}" \
            -- --years ${sim_years} --all --num_months "${sim_num_months}" \
            || rc=$?
    else
        # Default: month_0 only (sufficient to test all model types).
        # Override with LT_SIMULATE_MODES="0 1 2 3 4 5 6 7 8 9" for full coverage.
        local modes="${LT_SIMULATE_MODES:-0}"
        for month in $modes; do
            log INFO "  Month: ${month} (years=${sim_years}, num_months=${sim_num_months})"
            if ! run_in_venv long_term_forecasting dev_code/simulate_forecasts.py \
                "lt_forecast_mode=month_${month}" \
                -- --years ${sim_years} --all --num_months "${sim_num_months}"; then
                log WARN "  month_${month} failed, continuing with next month"
                any_failed=true
            fi
        done
        if [ "$any_failed" = true ]; then
            rc=1
        fi
    fi

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "long_term_forecasting completed in $(format_duration $elapsed)"
        record_result "long_term_forecasting" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "long_term_forecasting had failures after $(format_duration $elapsed)"
        record_result "long_term_forecasting" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_long_term_forecasting_operational() {
    banner "Module: long_term_forecasting (operational)"
    local start
    start=$(get_timestamp)
    local rc=0
    local any_failed=false

    # Prefer LT_ACTIVE_MODES (full mode names from query_lt_schedule),
    # then LT_OPERATIONAL_MODES (escape hatch, bare numbers with month_ prefix),
    # then default to all 10 months.
    local modes=""
    if [ -n "${LT_ACTIVE_MODES:-}" ]; then
        modes="$LT_ACTIVE_MODES"
    elif [ -n "${LT_OPERATIONAL_MODES:-}" ]; then
        # Legacy: bare numbers like "0 1 2" — prefix with month_
        for m in $LT_OPERATIONAL_MODES; do
            modes="${modes:+$modes }month_${m}"
        done
    else
        modes="month_0 month_1 month_2 month_3 month_4 month_5 month_6 month_7 month_8 month_9"
    fi

    local run_args=(--all)
    if [ -n "${LT_FORECAST_TODAY:-}" ]; then
        run_args=(--today "${LT_FORECAST_TODAY}")
    fi

    CURRENT_MODULE_LOG="${ERROR_DIR}/long_term_forecasting_operational.log"
    > "$CURRENT_MODULE_LOG"
    for mode in $modes; do
        log INFO "  Mode: ${mode} (operational)"
        if ! run_in_venv long_term_forecasting run_forecast.py \
            "lt_forecast_mode=${mode}" \
            -- "${run_args[@]}"; then
            log WARN "  ${mode} failed, continuing with next mode"
            any_failed=true
        fi
    done
    if [ "$any_failed" = true ]; then
        rc=1
    fi

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "long_term_forecasting (operational) completed in $(format_duration $elapsed)"
        record_result "long_term_forecasting (operational)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "long_term_forecasting (operational) had failures after $(format_duration $elapsed)"
        record_result "long_term_forecasting (operational)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

# ---------------------------------------------------------------------------
# Maintenance runner functions
# ---------------------------------------------------------------------------

run_maintenance_preprocessing_runoff() {
    banner "Maintenance: preprocessing_runoff --maintenance + long-horizon hydrograph"
    local start
    start=$(get_timestamp)
    local rc=0

    CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_runoff_maintenance.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv preprocessing_runoff preprocessing_runoff.py -- --maintenance || rc=$?

    if [ $rc -eq 0 ]; then
        local lt_rc=0
        log INFO "Daily runoff maintenance completed; syncing long-horizon hydrograph norms"

        local lt_start
        lt_start=$(get_timestamp)
        CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_runoff_long_horizon.log"
        > "$CURRENT_MODULE_LOG"

        if [ -n "${RUNOFF_LONG_HORIZON_TARGET_YEAR:-}" ]; then
            log INFO "  Long-horizon target year: ${RUNOFF_LONG_HORIZON_TARGET_YEAR}"
            if run_in_venv preprocessing_runoff sync_long_horizon_hydrograph.py -- \
                --target-year "${RUNOFF_LONG_HORIZON_TARGET_YEAR}"; then
                lt_rc=0
            else
                lt_rc=$?
            fi
        else
            if run_in_venv preprocessing_runoff sync_long_horizon_hydrograph.py; then
                lt_rc=0
            else
                lt_rc=$?
            fi
        fi

        local lt_elapsed=$(( $(get_timestamp) - lt_start ))

        if [ $lt_rc -eq 2 ]; then
            log WARN "Long-horizon hydrograph sync produced no records; continuing maintenance"
            # Sub-step was non-fatal: restore the maintenance log now, since
            # the module log recorded below (rc still 0) should reflect the
            # successful maintenance run, not this non-fatal sub-step.
            CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_runoff_maintenance.log"
        elif [ $lt_rc -eq 5 ]; then
            log ERROR "Long-horizon hydrograph sync had API read/write failure(s)"
            rc=$lt_rc
            # Fatal: leave CURRENT_MODULE_LOG on the long-horizon log so the
            # FAIL row recorded below references the output that explains it.
        elif [ $lt_rc -eq 4 ]; then
            log ERROR "Long-horizon hydrograph sync had SDK norm lookup failure(s)"
            # Pointer only (counts already survive the error-details tail --
            # see the sub-step's own LONG-HORIZON RUN SUMMARY print, which
            # lands last in this log, ahead of the tail window). Distinguish
            # a few failed stations from a total outage by reading them, not
            # by re-deriving them here. (INFRA-037)
            log ERROR "  Counts are in the LONG-HORIZON RUN SUMMARY block near the end of ${CURRENT_MODULE_LOG} -- also tailed under the 'preprocessing_runoff (long-horizon sync)' row below."
            record_result "preprocessing_runoff (long-horizon sync)" "FAIL" "$lt_elapsed" "$CURRENT_MODULE_LOG"
            # Downgraded, not fatal to the overall module (rc stays 0):
            # restore the maintenance log for the module-level record below.
            CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_runoff_maintenance.log"
        elif [ $lt_rc -ne 0 ]; then
            rc=$lt_rc
            # Fatal: leave CURRENT_MODULE_LOG on the long-horizon log so the
            # FAIL row recorded below references the output that explains it.
        else
            # lt_rc -eq 0: sub-step succeeded, restore the maintenance log.
            CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_runoff_maintenance.log"
        fi
    fi

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "preprocessing_runoff maintenance completed in $(format_duration $elapsed)"
        record_result "preprocessing_runoff (maintenance)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "preprocessing_runoff maintenance failed (exit $rc) after $(format_duration $elapsed)"
        record_result "preprocessing_runoff (maintenance)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_maintenance_preprocessing_gateway() {
    banner "Maintenance: preprocessing_gateway (extend ERA5 reanalysis)"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_gateway_maintenance.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv preprocessing_gateway extend_era5_reanalysis.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "preprocessing_gateway maintenance completed in $(format_duration $elapsed)"
        record_result "preprocessing_gateway (maintenance)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "preprocessing_gateway maintenance failed (exit $rc) after $(format_duration $elapsed)"
        record_result "preprocessing_gateway (maintenance)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_maintenance_linear_regression() {
    banner "Maintenance: linear_regression --hindcast"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/linear_regression_hindcast.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv linear_regression linear_regression.py SAPPHIRE_SYNC_MODE=maintenance -- --hindcast
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "linear_regression hindcast completed in $(format_duration $elapsed)"
        record_result "linear_regression (hindcast)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "linear_regression hindcast failed (exit $rc) after $(format_duration $elapsed)"
        record_result "linear_regression (hindcast)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_maintenance_machine_learning() {
    banner "Maintenance: machine_learning (NaN recalc + gap-fill + new stations)"
    local start
    start=$(get_timestamp)
    local rc=0

    CURRENT_MODULE_LOG="${ERROR_DIR}/machine_learning_maintenance.log"
    > "$CURRENT_MODULE_LOG"
    for model in "${ML_MODELS[@]}"; do
        log INFO "  Model: ${model}"
        for script in "${ML_MAINTENANCE_SCRIPTS[@]}"; do
            run_in_venv machine_learning "$script" \
                "SAPPHIRE_MODEL_TO_USE=${model}" \
                "SAPPHIRE_CONSISTENCY_CHECK=${SAPPHIRE_CONSISTENCY_CHECK:-false}" \
                || { rc=$?; break 2; }
        done
    done

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "machine_learning maintenance completed in $(format_duration $elapsed)"
        record_result "machine_learning (maintenance)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "machine_learning maintenance failed (exit $rc) after $(format_duration $elapsed)"
        record_result "machine_learning (maintenance)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_maintenance_postprocessing_forecasts() {
    banner "Maintenance: postprocessing_forecasts (gap-fill)"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/postprocessing_forecasts_maintenance.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv postprocessing_forecasts postprocessing_maintenance.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "postprocessing_forecasts maintenance completed in $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (maintenance)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "postprocessing_forecasts maintenance failed (exit $rc) after $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (maintenance)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_recalculate_skill_metrics() {
    banner "Recalculate: postprocessing_forecasts (full skill metrics rebuild)"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/postprocessing_forecasts_recalculate.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv postprocessing_forecasts recalculate_skill_metrics.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "skill metrics recalculation completed in $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (recalculate)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "skill metrics recalculation failed (exit $rc) after $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (recalculate)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_recalculate_snow_norms() {
    banner "Recalculate: preprocessing_gateway (yearly snow norms)"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_gateway_snow_norms.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv preprocessing_gateway recalculate_snow_norms.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "snow norm recalculation completed in $(format_duration $elapsed)"
        record_result "preprocessing_gateway (snow norms)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "snow norm recalculation failed (exit $rc) after $(format_duration $elapsed)"
        record_result "preprocessing_gateway (snow norms)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_initialize_deployment() {
    banner "Initialize: Full deployment setup"
    local start
    start=$(get_timestamp)

    # Read start_date: shell env takes precedence, then grep from .env file
    # (run_locally.sh never sources .env — mirrors the resolve_org() pattern)
    local start_date="${ieasyhydroforecast_START_DATE:-}"
    if [ -z "$start_date" ]; then
        local env_file="${ieasyhydroforecast_env_file_path:-}"
        if [ -n "$env_file" ] && [ -f "$env_file" ]; then
            start_date=$(grep -m1 '^ieasyhydroforecast_START_DATE=' "$env_file" \
                         | cut -d'=' -f2 | tr -d '[:space:]"'"'")
        fi
    fi
    if [ -z "$start_date" ]; then
        log ERROR "ieasyhydroforecast_START_DATE must be set in .env (or shell env) for initialization"
        return 1
    fi

    log INFO "=== SAPPHIRE Initialization ==="
    log INFO "Start date: $start_date"

    # Step 1: Read data sources and populate CSV cache (maintenance fetch + 30-day API sync)
    log INFO "--- Step 1/5: Preprocessing runoff (maintenance) ---"
    run_maintenance_preprocessing_runoff
    local rc=$?
    if [ $rc -ne 0 ]; then
        log ERROR "Initialization failed at Step 1 (preprocessing runoff)"
        return $rc
    fi

    # Step 2: Push full CSV history to API (initial sync)
    # CRITICAL: This is a hard prerequisite for Step 3 — the hindcast reads
    # observations from the preprocessing API (no CSV fallback when
    # SAPPHIRE_API_ENABLED=true). The script sets SAPPHIRE_SYNC_MODE=initial
    # internally; the env var below is a safety net.
    log INFO "--- Step 2/5: Initial API sync (full history) ---"
    CURRENT_MODULE_LOG="${ERROR_DIR}/initial_api_sync.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv preprocessing_runoff initial_api_sync.py \
        "SAPPHIRE_SYNC_MODE=initial"
    rc=$?
    if [ $rc -ne 0 ]; then
        log ERROR "Initialization failed at Step 2 (initial API sync)"
        return $rc
    fi

    # Step 3: Hindcast for each horizon
    # Values must be uppercase PENTAD/DECAD — linear_regression.py compares
    # case-sensitively (lines 646-647), lowercase causes silent no-op.
    for mode in PENTAD DECAD; do
        log INFO "--- Step 3/5: Hindcast ($mode) ---"
        CURRENT_MODULE_LOG="${ERROR_DIR}/linear_regression_init_${mode}.log"
        > "$CURRENT_MODULE_LOG"
        run_in_venv linear_regression linear_regression.py \
            "SAPPHIRE_PREDICTION_MODE=$mode" -- \
            --hindcast --start-date "$start_date"
        rc=$?
        if [ $rc -ne 0 ]; then
            log ERROR "Initialization failed at Step 3 (hindcast $mode)"
            return $rc
        fi
    done

    # Step 4: Skill metrics for each horizon
    # Values must be uppercase PENTAD/DECAD — recalculate_skill_metrics.py
    # validates against VALID_MODES and exits on unrecognized values.
    for mode in PENTAD DECAD; do
        log INFO "--- Step 4/5: Skill metrics ($mode) ---"
        CURRENT_MODULE_LOG="${ERROR_DIR}/skill_metrics_init_${mode}.log"
        > "$CURRENT_MODULE_LOG"
        run_in_venv postprocessing_forecasts recalculate_skill_metrics.py \
            "SAPPHIRE_PREDICTION_MODE=$mode"
        rc=$?
        if [ $rc -ne 0 ]; then
            log ERROR "Initialization failed at Step 4 (skill metrics $mode)"
            return $rc
        fi
    done

    log INFO "--- Step 5/5: Verification ---"
    local elapsed=$(( $(get_timestamp) - start ))
    log OK "Initialization complete in $(format_duration $elapsed). Start the dashboard to verify data."
    record_result "initialize_deployment" "PASS" "$elapsed" ""
    return 0
}

run_calibrate_long_term() {
    banner "Calibrate: long_term_forecasting"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/long_term_forecasting_calibrate.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv long_term_forecasting calibrate_and_hindcast.py \
        "lt_forecast_mode=${lt_forecast_mode:-monthly}" \
        -- --all
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "long_term calibration completed in $(format_duration $elapsed)"
        record_result "long_term_forecasting (calibrate)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "long_term calibration failed (exit $rc) after $(format_duration $elapsed)"
        record_result "long_term_forecasting (calibrate)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_postprocessing_long_term() {
    banner "Module: postprocessing_forecasts (long-term operational)"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/postprocessing_forecasts_long_term_operational.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv postprocessing_forecasts postprocessing_operational_long_term.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "postprocessing long-term operational completed in $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (long-term operational)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "postprocessing long-term operational failed (exit $rc) after $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (long-term operational)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_maintenance_postprocessing_long_term() {
    banner "Maintenance: postprocessing_forecasts (long-term gap-fill)"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/postprocessing_forecasts_long_term_maintenance.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv postprocessing_forecasts postprocessing_maintenance_long_term.py \
        "POSTPROCESSING_GAPFILL_WINDOW_MONTHS=${POSTPROCESSING_GAPFILL_WINDOW_MONTHS:-3}"
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "postprocessing long-term maintenance completed in $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (long-term maintenance)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "postprocessing long-term maintenance failed (exit $rc) after $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (long-term maintenance)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

run_recalculate_long_term_skill_metrics() {
    local modes="${1:-MONTHLY QUARTERLY SEASONAL}"
    banner "Recalculate: long-term skill metrics (${modes})"
    local start
    start=$(get_timestamp)
    local rc=0

    CURRENT_MODULE_LOG="${ERROR_DIR}/postprocessing_forecasts_recalculate_lt.log"
    > "$CURRENT_MODULE_LOG"

    for mode in $modes; do
        log INFO "  Recalculating: ${mode}"
        run_in_venv postprocessing_forecasts recalculate_skill_metrics.py \
            "SAPPHIRE_PREDICTION_MODE=${mode}" || { rc=$?; break; }
    done

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "long-term skill metrics recalculation completed in $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (long-term skill metrics)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "long-term skill metrics recalculation failed (exit $rc) after $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (long-term skill metrics)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return $rc
}

# ---------------------------------------------------------------------------
# API validation
# ---------------------------------------------------------------------------

run_api_validation() {
    local target="${1:-short-term}"
    banner "API Data Validation"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/api_validation.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv postprocessing_forecasts ../validate_pipeline/validate_pipeline.py \
        -- --target "$target"
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "api_validation completed in $(format_duration $elapsed)"
        record_validation "api_validation (${target})" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log WARN "api_validation reported failures after $(format_duration $elapsed)"
        record_validation "api_validation (${target})" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return 0  # don't abort pipeline mid-run; failures surface in summary
}

# run_module_validation MODULE [LABEL_SUFFIX]
#
# LABEL_SUFFIX is optional and additive (INFRA-037): every existing
# single-argument call site behaves exactly as before (log path
# "api_validation_${module}.log", row label "api_validation (${module})").
# When a suffix is given (e.g. a mode name), both the log path and the
# recorded row label are made unique per suffix, so callers that invoke
# this once per mode (e.g. the bare `machine_learning` target under
# ML_MODE=BOTH) don't have each call overwrite the previous call's log
# file and don't get indistinguishable PASS/FAIL rows in the summary.
run_module_validation() {
    local module="$1"
    local suffix="${2:-}"
    local label="$module"
    local log_suffix="$module"
    if [ -n "$suffix" ]; then
        label="${module} ${suffix}"
        log_suffix="${module}_${suffix}"
    fi

    banner "API Data Validation (${label})"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/api_validation_${log_suffix}.log"
    > "$CURRENT_MODULE_LOG"
    # `|| rc=$?` (not a bare call + `local rc=$?`) is required here: every
    # call site of run_module_validation is itself a bare statement (not
    # wrapped in `||`/`if`), so under `set -euo pipefail` a bare failing
    # run_in_venv would trip `set -e` and kill the whole script on the
    # spot -- before record_validation below ever runs, silently defeating
    # the "don't abort pipeline mid-run" contract this function documents.
    # (Found while testing INFRA-037 defect 1: with ML_MODE=BOTH, a FAIL
    # from either mode's validation previously killed run_locally.sh
    # outright instead of producing a FAIL row.)
    local rc=0
    run_in_venv postprocessing_forecasts ../validate_pipeline/validate_pipeline.py \
        -- --module "$module" || rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "api_validation (${label}) completed in $(format_duration $elapsed)"
        record_validation "api_validation (${label})" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log WARN "api_validation (${label}) reported failures after $(format_duration $elapsed)"
        record_validation "api_validation (${label})" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
    fi
    return 0  # don't abort pipeline mid-run; failures surface in summary
}

# ---------------------------------------------------------------------------
# Pipeline orchestrators
# ---------------------------------------------------------------------------

run_short_term_pipeline() {
    banner "SHORT-TERM FORECAST PIPELINE"

    local original_mode="${SAPPHIRE_PREDICTION_MODE:-}"
    local modes_to_run=()

    # Determine which modes to run
    if [ "$original_mode" = "BOTH" ]; then
        modes_to_run=(PENTAD DECAD)
        log INFO "BOTH mode: will run PENTAD then DECAD"
    elif [ -n "$original_mode" ]; then
        modes_to_run=("$original_mode")
    else
        modes_to_run=(PENTAD)
        log WARN "SAPPHIRE_PREDICTION_MODE not set, defaulting to PENTAD"
    fi

    # Preprocessing runs once regardless of mode
    log INFO "Running preprocessing (shared across modes)..."
    run_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    if should_skip_module preprocessing_gateway; then
        log INFO "Skipping preprocessing_gateway (not required for ${ORG} org)"
    else
        run_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    fi

    # Forecasting + postprocessing runs per mode
    for mode in "${modes_to_run[@]}"; do
        export SAPPHIRE_PREDICTION_MODE="$mode"
        log INFO "Running forecasting for mode: ${mode}"

        if should_skip_module machine_learning; then
            :
        elif should_skip_ml_for_mode "$mode"; then
            log INFO "Skipping machine_learning for ${mode} (ML_MODE=${ML_MODE})"
        else
            run_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
        fi
        run_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
        run_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    done

    # Restore original mode
    export SAPPHIRE_PREDICTION_MODE="$original_mode"

    run_api_validation "short-term"
}

run_long_term_pipeline() {
    banner "LONG-TERM FORECAST PIPELINE"

    if should_skip_module long_term_forecasting; then
        log INFO "Skipping long-term pipeline (not required for ${ORG} org)"
        return 0
    fi

    # Phase 1: Generate forecasts
    run_long_term_forecasting || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }

    # Phase 2: Operational postprocessing
    run_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }

    # Phase 3: Long-term skill metrics (monthly + quarterly + seasonal)
    run_recalculate_long_term_skill_metrics || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }

    # Phase 4: Maintenance gap-fill (creates ensembles using skill metrics)
    run_maintenance_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }

    run_api_validation "long-term"
}

run_long_term_operational_pipeline() {
    banner "LONG-TERM OPERATIONAL PIPELINE"

    if should_skip_module long_term_forecasting; then
        log INFO "Skipping long-term operational pipeline (not required for ${ORG} org)"
        return 0
    fi

    # Determine which modes are active today (config-aware)
    if [ -z "${LT_ACTIVE_WINDOW:-}" ]; then
        query_lt_schedule
    fi

    # Phase 1: Preprocessing (shared, runs once)
    log INFO "Phase 1: Preprocessing"
    run_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    run_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }

    if [ -z "${LT_ACTIVE_WINDOW:-}" ]; then
        log WARN "No active modes today, skipping long-term pipeline"
        return 0
    fi

    log INFO "Active modes: ${LT_ACTIVE_MODES}"

    # Phase 2: Generate forecasts (only active modes)
    run_long_term_forecasting_operational || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }

    # Phase 3: Operational postprocessing
    run_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }

    # Phase 4: Skill metrics — driven by schedule query
    if [ -n "${LT_SKILL_METRIC_TYPES:-}" ]; then
        run_recalculate_long_term_skill_metrics "$LT_SKILL_METRIC_TYPES" || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    else
        run_recalculate_long_term_skill_metrics "MONTHLY" || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    fi

    # Phase 5: Maintenance gap-fill (creates ensembles using skill metrics)
    run_maintenance_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }

    run_api_validation "long-term"
}

run_all() {
    banner "FULL PIPELINE (short-term + long-term)"
    run_short_term_pipeline
    local st_rc=$?
    local lt_rc=0
    if ! should_skip_module long_term_forecasting; then
        run_long_term_pipeline
        lt_rc=$?
    fi

    run_api_validation "all"

    if [ $st_rc -ne 0 ] || [ $lt_rc -ne 0 ]; then
        return 1
    fi
    return 0
}

run_maintenance_pipeline() {
    banner "MAINTENANCE PIPELINE"

    local original_mode="${SAPPHIRE_PREDICTION_MODE:-}"
    local modes_to_run=()

    if [ "$original_mode" = "BOTH" ]; then
        modes_to_run=(PENTAD DECAD)
        log INFO "BOTH mode: will run maintenance for PENTAD then DECAD"
    elif [ -n "$original_mode" ]; then
        modes_to_run=("$original_mode")
    else
        modes_to_run=(PENTAD)
        log WARN "SAPPHIRE_PREDICTION_MODE not set, defaulting to PENTAD"
    fi

    # Preprocessing maintenance runs once (mode-independent)
    run_maintenance_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    if ! should_skip_module preprocessing_gateway; then
        run_maintenance_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    fi

    # linear_regression and ML maintenance run per mode
    for mode in "${modes_to_run[@]}"; do
        export SAPPHIRE_PREDICTION_MODE="$mode"
        log INFO "Running maintenance for mode: ${mode}"

        if should_skip_module machine_learning; then
            :
        elif should_skip_ml_for_mode "$mode"; then
            log INFO "Skipping machine_learning maintenance for ${mode} (ML_MODE=${ML_MODE})"
        else
            run_maintenance_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
        fi
        run_maintenance_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
        run_maintenance_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    done

    # Long-term maintenance (mode-independent, runs once)
    if ! should_skip_module long_term_forecasting; then
        log INFO "Running long-term postprocessing maintenance"
        run_maintenance_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    fi

    export SAPPHIRE_PREDICTION_MODE="$original_mode"
}

run_daily_pipeline() {
    banner "DAILY PIPELINE (short-term + maintenance + long-term if near issue day)"

    local original_mode="${SAPPHIRE_PREDICTION_MODE:-}"

    # --- Phase 1: Preprocessing (runs once) ---
    log INFO "Phase 1: Preprocessing (shared across all horizons)"
    run_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    if ! should_skip_module preprocessing_gateway; then
        run_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    fi

    # --- Phase 2: Maintenance preprocessing (runs once) ---
    log INFO "Phase 2: Maintenance preprocessing"
    run_maintenance_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    if ! should_skip_module preprocessing_gateway; then
        run_maintenance_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    fi

    # --- Phase 3: Forecasting + postprocessing per horizon ---
    for mode in PENTAD DECAD; do
        export SAPPHIRE_PREDICTION_MODE="$mode"
        log INFO "Phase 3: ML + linear regression + postprocessing (${mode})"

        if should_skip_module machine_learning; then
            :
        elif should_skip_ml_for_mode "$mode"; then
            log INFO "Skipping machine_learning for ${mode} (ML_MODE=${ML_MODE})"
        else
            run_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
        fi
        run_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
        run_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    done

    # --- Phase 4: Maintenance per horizon ---
    for mode in PENTAD DECAD; do
        export SAPPHIRE_PREDICTION_MODE="$mode"
        log INFO "Phase 4: ML + LR + postprocessing maintenance (${mode})"

        if should_skip_module machine_learning; then
            :
        elif should_skip_ml_for_mode "$mode"; then
            log INFO "Skipping machine_learning maintenance for ${mode} (ML_MODE=${ML_MODE})"
        else
            run_maintenance_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
        fi
        run_maintenance_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
        run_maintenance_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    done

    # Restore original mode
    export SAPPHIRE_PREDICTION_MODE="$original_mode"

    # --- Phase 5: Long-term forecasting (config-aware scheduling) ---
    if should_skip_module long_term_forecasting; then
        log INFO "Phase 5: Skipping long-term forecasting (not required for ${ORG} org)"
    else
        query_lt_schedule
        if [ -n "${LT_ACTIVE_WINDOW:-}" ]; then
            log INFO "Phase 5: Long-term forecasting (active modes: ${LT_ACTIVE_MODES})"
            run_long_term_forecasting_operational || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
            run_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
            if [ -n "${LT_SKILL_METRIC_TYPES:-}" ]; then
                run_recalculate_long_term_skill_metrics "$LT_SKILL_METRIC_TYPES" || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
            else
                run_recalculate_long_term_skill_metrics "MONTHLY" || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
            fi
            run_maintenance_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
        else
            log INFO "Phase 5: Skipping long-term forecasting — no modes active today"
        fi
    fi

    run_api_validation "daily"
}

run_yearly_pipeline() {
    banner "YEARLY PIPELINE (snow norms + skill metrics)"
    if should_skip_module preprocessing_gateway; then
        log INFO "Skipping snow norm recalculation (not required for ${ORG} org)"
    else
        run_recalculate_snow_norms || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
    fi
    run_recalculate_skill_metrics || { [ "$CONTINUE_ON_ERROR" = false ] && { PIPELINE_ABORTED=true; return 1; }; }
}

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

validate_env() {
    local target="$1"
    local errors=0

    log INFO "Validating environment..."

    # Check env file
    if [ -z "${ieasyhydroforecast_env_file_path:-}" ]; then
        log ERROR "ieasyhydroforecast_env_file_path is not set"
        errors=$((errors + 1))
    elif [ ! -f "$ieasyhydroforecast_env_file_path" ]; then
        log ERROR "Env file not found: ${ieasyhydroforecast_env_file_path}"
        errors=$((errors + 1))
    else
        log OK "Env file: ${ieasyhydroforecast_env_file_path}"
    fi

    # Check prediction mode for targets that need it
    # (daily sets its own mode, so no warning needed)
    case "$target" in
        short-term|all|maintenance|maintenance:linear_regression|maintenance:machine_learning|maintenance:postprocessing_forecasts|recalculate_skill_metrics)
            if [ -z "${SAPPHIRE_PREDICTION_MODE:-}" ]; then
                log WARN "SAPPHIRE_PREDICTION_MODE not set (will default to PENTAD)"
            else
                log OK "Prediction mode: ${SAPPHIRE_PREDICTION_MODE}"
            fi
            ;;
        daily)
            log OK "Prediction mode: PENTAD + DECAD (daily) + long-term (gated)"
            ;;
        long-term|long-term-operational|calibrate_long_term|recalculate_snow_norms|yearly|maintenance:postprocessing_long_term)
            # These targets don't depend on SAPPHIRE_PREDICTION_MODE
            ;;
    esac

    # Determine which modules to validate
    local modules_to_check=()
    case "$target" in
        short-term)                      modules_to_check=("${SHORT_TERM_MODULES[@]}") ;;
        long-term)                       modules_to_check=(long_term_forecasting postprocessing_forecasts) ;;
        long-term-operational)           modules_to_check=(preprocessing_runoff preprocessing_gateway long_term_forecasting postprocessing_forecasts) ;;
        all)                             modules_to_check=("${ALL_MODULES[@]}") ;;
        daily)                           modules_to_check=("${SHORT_TERM_MODULES[@]}" long_term_forecasting) ;;
        maintenance)                     modules_to_check=("${MAINTENANCE_MODULES[@]}" long_term_forecasting) ;;
        maintenance:postprocessing_long_term) modules_to_check=(postprocessing_forecasts) ;;
        maintenance:*)                   modules_to_check=("${target#maintenance:}") ;;
        calibrate_long_term)             modules_to_check=(long_term_forecasting) ;;
        recalculate_skill_metrics)       modules_to_check=(postprocessing_forecasts) ;;
        recalculate_snow_norms)          modules_to_check=(preprocessing_gateway) ;;
        yearly)                          modules_to_check=(preprocessing_gateway postprocessing_forecasts) ;;
        initialize)                      modules_to_check=(preprocessing_runoff linear_regression postprocessing_forecasts) ;;
        *)                               modules_to_check=("$target") ;;
    esac

    # Check venvs (skip modules not needed for this org)
    for module in "${modules_to_check[@]}"; do
        if should_skip_module "$module"; then
            log INFO "venv: ${module} (skipped for ${ORG} org)"
            continue
        fi
        if check_venv "$module"; then
            log OK "venv: ${module}/.venv/bin/python"
        else
            log ERROR "Missing venv: ${module}/.venv/bin/python (run: cd apps/${module} && uv sync --all-extras)"
            errors=$((errors + 1))
        fi
    done

    if [ $errors -gt 0 ]; then
        log ERROR "Validation failed with ${errors} error(s)"
        return 1
    fi

    log OK "Validation passed"
    return 0
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

print_error_details() {
    # Print last N lines of log for each failed entry in the given arrays.
    # Usage: print_error_details <label> MODULE_ARRAY STATUS_ARRAY ERROR_LOG_ARRAY
    local label="$1"
    shift
    local -n _modules=$1
    local -n _statuses=$2
    local -n _error_logs=$3

    local has_failures=false
    for i in "${!_statuses[@]}"; do
        if [ "${_statuses[$i]}" = "FAIL" ]; then
            has_failures=true
            break
        fi
    done
    [ "$has_failures" = false ] && return 0

    echo ""
    echo "" >> "$LOG_FILE"
    local sep="$(printf '=%.0s' {1..60})"
    echo -e "${RED}${sep}${NC}"
    echo "$sep" >> "$LOG_FILE"
    echo -e "${RED}${label} (last ${ERROR_TAIL_LINES} lines per failure)${NC}"
    echo "${label} (last ${ERROR_TAIL_LINES} lines per failure)" >> "$LOG_FILE"
    echo -e "${RED}${sep}${NC}"
    echo "$sep" >> "$LOG_FILE"

    for i in "${!_modules[@]}"; do
        if [ "${_statuses[$i]}" = "FAIL" ]; then
            local err_log="${_error_logs[$i]:-}"
            echo ""
            echo "" >> "$LOG_FILE"
            echo -e "${BOLD}--- ${_modules[$i]} ---${NC}"
            echo "--- ${_modules[$i]} ---" >> "$LOG_FILE"
            if [ -n "$err_log" ] && [ -s "$err_log" ]; then
                tail -"${ERROR_TAIL_LINES}" "$err_log" | while IFS= read -r line; do
                    echo "  $line"
                    echo "  $line" >> "$LOG_FILE"
                done
            else
                echo "  (no output captured)"
                echo "  (no output captured)" >> "$LOG_FILE"
            fi
        fi
    done
    echo ""
    echo "" >> "$LOG_FILE"
}

print_summary() {
    local total_time=$1
    echo "" | tee -a "$LOG_FILE"
    banner "PIPELINE SUMMARY"

    # --- Module results ---
    local pass_count=0
    local fail_count=0

    for i in "${!RESULTS_MODULE[@]}"; do
        local mod="${RESULTS_MODULE[$i]}"
        local status="${RESULTS_STATUS[$i]}"
        local elapsed="${RESULTS_TIME[$i]}"
        local duration
        duration="$(format_duration "$elapsed")"

        if [ "$status" = "PASS" ]; then
            log OK "  ${mod}: PASS (${duration})"
            pass_count=$((pass_count + 1))
        else
            log ERROR "  ${mod}: FAIL (${duration})"
            fail_count=$((fail_count + 1))
        fi
    done

    # --- Validation results ---
    local val_pass=0
    local val_fail=0

    if [ ${#VALIDATION_MODULE[@]} -gt 0 ]; then
        echo "" | tee -a "$LOG_FILE"
        banner "API VALIDATION SUMMARY"

        for i in "${!VALIDATION_MODULE[@]}"; do
            local mod="${VALIDATION_MODULE[$i]}"
            local status="${VALIDATION_STATUS[$i]}"
            local elapsed="${VALIDATION_TIME[$i]}"
            local duration
            duration="$(format_duration "$elapsed")"

            if [ "$status" = "PASS" ]; then
                log OK "  ${mod}: PASS (${duration})"
                val_pass=$((val_pass + 1))
            else
                log ERROR "  ${mod}: FAIL (${duration})"
                val_fail=$((val_fail + 1))
            fi
        done
    fi

    # --- Totals ---
    echo "" | tee -a "$LOG_FILE"
    local summary="Modules: ${pass_count} passed, ${fail_count} failed"
    if [ ${#VALIDATION_MODULE[@]} -gt 0 ]; then
        summary="${summary} | Validation: ${val_pass} passed, ${val_fail} failed"
    fi
    log INFO "${summary} | $(format_duration "$total_time") elapsed"
    log INFO "Log file: ${LOG_FILE}"

    # --- Error details ---
    if [ $fail_count -gt 0 ]; then
        print_error_details "MODULE ERROR DETAILS" \
            RESULTS_MODULE RESULTS_STATUS RESULTS_ERROR_LOG
    fi

    if [ $val_fail -gt 0 ]; then
        print_error_details "VALIDATION ERROR DETAILS" \
            VALIDATION_MODULE VALIDATION_STATUS VALIDATION_ERROR_LOG
    fi

    if [ $fail_count -gt 0 ] || [ $val_fail -gt 0 ]; then
        return 1
    fi
    return 0
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash apps/run_locally.sh [FLAGS] TARGET

Combined targets:
  daily                   Short-term daily run: PENTAD + DECAD + maintenance
                          + long-term forecasting (gated by day-of-month)

Operational targets:
  short-term              Run the short-term forecast pipeline
  long-term               Long-term pipeline in simulate mode (for testing)
  long-term-operational   Full long-term pipeline: preprocessing + run_forecast.py
                          (months 0-9) + postprocess + skill metrics + gap-fill
  all                     Run both short-term and long-term pipelines
  <module_name>           Run a single module (see list below)

Maintenance targets:
  maintenance             Run all maintenance tasks (gap-fill + hindcast)
  maintenance:preprocessing_runoff    Runoff gap-filling + long-horizon hydrograph
  maintenance:preprocessing_gateway   Extend ERA5 reanalysis data
  maintenance:linear_regression       Linear regression hindcast
  maintenance:machine_learning        ML NaN recalc + gap-fill + new stations
  maintenance:postprocessing_forecasts  Fill missing ensemble forecasts
  maintenance:postprocessing_long_term  Fill missing monthly ensemble forecasts
  calibrate_long_term     Calibrate and hindcast long-term models
  recalculate_skill_metrics  Full skill metrics rebuild (run yearly)
  recalculate_snow_norms  Yearly snow norm recalculation
  yearly                  All yearly tasks (snow norms + skill metrics)

Initialization targets:
  initialize              First-time deployment setup: full data sync,
                          hindcast from START_DATE, and skill metrics.
                          Requires ieasyhydroforecast_START_DATE in .env.

Modules (for single-module operational runs):
  preprocessing_runoff    Process runoff data
  preprocessing_gateway   Quantile mapping, ERA5 extension, snow data
  linear_regression       Linear regression forecasts
  machine_learning        ML forecasts (TFT, TIDE, TSMIXER). Resolves its own
                          prediction mode(s) via resolve_ml_bare_target_modes
                          instead of crashing on an empty mode -- see the
                          SAPPHIRE_PREDICTION_MODE / ML_MODE entries below.
  postprocessing_forecasts  Post-process forecast outputs
  long_term_forecasting   Long-term monthly forecasts

Flags:
  --continue-on-error     Don't abort on first module failure
  --dry-run               Validate environment and venvs without running
  --help                  Show this help message

Environment variables:
  ieasyhydroforecast_env_file_path   Path to .env config file (required)
  SAPPHIRE_PREDICTION_MODE           PENTAD, DECAD, or BOTH (short-term/maintenance).
                                        Three targets resolve this differently, so check
                                        which one you are running:
                                          - bare `machine_learning`: validates this against
                                            ML_MODE itself (resolve_ml_bare_target_modes).
                                            Unset derives the mode from ML_MODE (WARN);
                                            BOTH loops over PENTAD and DECAD, but each pass
                                            is still filtered against ML_MODE (default DECAD),
                                            so SAPPHIRE_PREDICTION_MODE=BOTH alone only runs
                                            DECAD -- also set ML_MODE=BOTH to run both (which
                                            by itself is enough, even with this var unset); an
                                            explicit single value that conflicts with a
                                            single-valued ML_MODE errors out naming both
                                            variables instead of silently picking one.
                                          - `maintenance:machine_learning` / `daily` /
                                            `all` / `maintenance`: resolved by an outer
                                            per-mode loop in run_locally.sh itself, which
                                            also consults ML_MODE via should_skip_ml_for_mode.
                                          - every other module target: forwarded as-is to
                                            the module's own venv invocation.
  lt_forecast_mode                   Specific month for long-term (e.g. month_3)
  LT_SIMULATE_YEARS                  Space-separated years to simulate (default: 2024)
  LT_SIMULATE_NUM_MONTHS             Months to simulate per year (default: 1)
  LT_SIMULATE_MODES                  Space-separated month modes to run (default: "0")
  LT_OPERATIONAL_MODES                Month modes for long-term-operational (default: "0 1 2 3 4 5 6 7 8 9")
  LT_FORECAST_TODAY                   Override today's date for run_forecast.py (YYYY-MM-DD)
  LT_OPERATIONAL_ISSUE_DAYS            Days of month for LT issue window (default: "10 25").
                                        25th runs all horizons; 10th runs monthly only.
  POSTPROCESSING_GAPFILL_WINDOW_MONTHS  Lookback for long-term gap-fill (default: 3)
  RUNOFF_LONG_HORIZON_TARGET_YEAR        Optional target year for monthly/seasonal runoff
                                        hydrograph rows in maintenance:preprocessing_runoff.
                                        Defaults to the current calendar year.
  ieasyhydroforecast_organization        Organization name (demo, kghm, tjhm, uzhm).
                                          Demo/uzhm skip: preprocessing_gateway, machine_learning, long_term_forecasting.
  ML_MODE                                 Which prediction mode ML runs for (default: DECAD).
                                            Set ML_MODE=BOTH to run ML for all modes.
  ieasyhydroforecast_START_DATE        Hindcast start date for initialize target (YYYY-MM-DD)
  SAPPHIRE_SKILL_LEAD_AWARE               Lead-aware long-term operational selection: per-lead
                                            skill metrics & ensembles, config-driven issuance
                                            selection, and dashboard target-period display
                                            (default OFF). Requires both operational_month_lead_time
                                            and operational_issue_day in the configured long-term
                                            modes, plus a recalc after enabling (pin the window with
                                            SAPPHIRE_RECALC_START_YEAR - the default is only
                                            current_year-20). Leaving it off makes Tajik monthly
                                            target months read one month late.
                                            See doc/dev/update_dev_deployment.md Step 3.5.

Examples:
  # Full daily run (PENTAD + DECAD + maintenance)
  ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh daily

  # Single horizon operational run
  SAPPHIRE_PREDICTION_MODE=PENTAD \
    ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh short-term

  # Maintenance only (all modules)
  SAPPHIRE_PREDICTION_MODE=BOTH \
    ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh maintenance

  # Single maintenance module
  SAPPHIRE_PREDICTION_MODE=PENTAD \
    ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh maintenance:linear_regression

  # Postprocessing gap-fill
  SAPPHIRE_PREDICTION_MODE=BOTH \
    ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh maintenance:postprocessing_forecasts

  # Yearly skill metrics recalculation
  SAPPHIRE_PREDICTION_MODE=BOTH \
    ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh recalculate_skill_metrics

  # All yearly tasks (snow norms + skill metrics)
  ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh yearly

  # Full long-term operational pipeline (months 0-9)
  # Runs automatically when within ±5 days of issue days (10th or 25th)
  ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh long-term-operational

  # Long-term operational with explicit date override
  LT_FORECAST_TODAY=2026-02-25 \
    ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh long-term-operational

  # Daily run — long-term auto-triggers near 10th/25th
  ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh daily

  # Long-term calibration
  ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh calibrate_long_term

  # Long-term postprocessing gap-fill
  ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh maintenance:postprocessing_long_term

  # Dry run
  bash apps/run_locally.sh --dry-run maintenance

  # First-time deployment initialization
  ieasyhydroforecast_START_DATE=2000-01-06 \
    ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh initialize
USAGE
}

# ---------------------------------------------------------------------------
# Argument parsing and main dispatch
# ---------------------------------------------------------------------------

main() {
    # Capture the operator's invocation-time SAPPHIRE_PREDICTION_MODE before
    # anything below -- the unconditional export default further down, or
    # the per-mode dispatch loops in the case statement -- can overwrite it.
    # Used only by emit_continue_on_error_hint to reproduce the original
    # horizon in its printed retry command; never read for pipeline logic.
    INVOCATION_SAPPHIRE_PREDICTION_MODE="${SAPPHIRE_PREDICTION_MODE:-}"

    local target=""

    # Parse arguments
    while [ $# -gt 0 ]; do
        case "$1" in
            --continue-on-error) CONTINUE_ON_ERROR=true ;;
            --dry-run)           DRY_RUN=true ;;
            --help|-h)           print_usage; exit 0 ;;
            -*)
                echo "Unknown flag: $1"
                print_usage
                exit 1
                ;;
            *)
                if [ -n "$target" ]; then
                    echo "Multiple targets not supported. Got: '$target' and '$1'"
                    print_usage
                    exit 1
                fi
                target="$1"
                ;;
        esac
        shift
    done

    if [ -z "$target" ]; then
        echo "Error: No target specified."
        echo ""
        print_usage
        exit 1
    fi

    # Validate target
    local valid_targets="daily short-term long-term long-term-operational all maintenance calibrate_long_term recalculate_skill_metrics recalculate_snow_norms yearly maintenance:postprocessing_long_term initialize"
    local is_valid=false
    for t in $valid_targets; do
        [ "$target" = "$t" ] && is_valid=true
    done
    for mod in "${ALL_MODULES[@]}"; do
        [ "$target" = "$mod" ] && is_valid=true
    done
    for mod in "${MAINTENANCE_MODULES[@]}"; do
        [ "$target" = "maintenance:${mod}" ] && is_valid=true
    done

    if [ "$is_valid" = false ]; then
        echo "Unknown target: $target"
        echo ""
        print_usage
        exit 1
    fi

    # Create log directory
    mkdir -p "$LOG_DIR"

    # Resolve organization (shell env → .env file → empty = run all)
    resolve_org

    banner "SAPPHIRE Local Pipeline Runner"
    log INFO "Target: ${target}"
    log INFO "Organization: ${ORG:-<not set, running all modules>}"
    log INFO "Continue on error: ${CONTINUE_ON_ERROR}"
    log INFO "Dry run: ${DRY_RUN}"
    log INFO "ML mode: ${ML_MODE}"
    log INFO "Log file: ${LOG_FILE}"

    if [ "$ORG" = "demo" ]; then
        log INFO "Demo org: skipping modules — ${DEMO_SKIP_MODULES[*]}"
    elif [ "$ORG" = "uzhm" ]; then
        log INFO "Uzhm org: skipping modules — ${UZHM_SKIP_MODULES[*]}"
    fi

    # Validate environment
    if ! validate_env "$target"; then
        exit 1
    fi

    # Dry run stops here
    if [ "$DRY_RUN" = true ]; then
        log OK "Dry run complete. Environment is valid."
        exit 0
    fi

    # Export env vars for subprocesses
    export ieasyhydroforecast_env_file_path="${ieasyhydroforecast_env_file_path:-}"
    export SAPPHIRE_PREDICTION_MODE="${SAPPHIRE_PREDICTION_MODE:-}"

    local pipeline_start
    pipeline_start=$(get_timestamp)

    # Dispatch
    local exit_code=0
    case "$target" in
        # Combined target
        daily)
            run_daily_pipeline || exit_code=$?
            ;;
        # Pipeline targets
        short-term)
            run_short_term_pipeline || exit_code=$?
            ;;
        long-term)
            run_long_term_pipeline || exit_code=$?
            ;;
        long-term-operational)
            run_long_term_operational_pipeline || exit_code=$?
            ;;
        all)
            run_all || exit_code=$?
            ;;
        # Maintenance targets
        maintenance)
            run_maintenance_pipeline || exit_code=$?
            ;;
        maintenance:preprocessing_runoff)
            run_maintenance_preprocessing_runoff || exit_code=$?
            ;;
        maintenance:preprocessing_gateway)
            if should_skip_module preprocessing_gateway; then
                log INFO "Skipping maintenance:preprocessing_gateway (not required for ${ORG} org)"
            else
                run_maintenance_preprocessing_gateway || exit_code=$?
            fi
            ;;
        maintenance:linear_regression)
            run_maintenance_linear_regression || exit_code=$?
            ;;
        maintenance:machine_learning)
            if should_skip_module machine_learning; then
                log INFO "Skipping maintenance:machine_learning (not required for ${ORG} org)"
            else
            local original_mode="${SAPPHIRE_PREDICTION_MODE:-}"
            local modes_to_run=()
            if [ "$original_mode" = "BOTH" ]; then
                modes_to_run=(PENTAD DECAD)
                log INFO "BOTH mode: will run ML maintenance for PENTAD then DECAD"
            elif [ -n "$original_mode" ]; then
                modes_to_run=("$original_mode")
            else
                modes_to_run=(PENTAD)
                log WARN "SAPPHIRE_PREDICTION_MODE not set, defaulting to PENTAD"
            fi
            for mode in "${modes_to_run[@]}"; do
                if should_skip_ml_for_mode "$mode"; then
                    log INFO "Skipping machine_learning maintenance for ${mode} (ML_MODE=${ML_MODE})"
                    continue
                fi
                export SAPPHIRE_PREDICTION_MODE="$mode"
                log INFO "Running ML maintenance for mode: ${mode}"
                run_maintenance_machine_learning || { exit_code=$?; break; }
            done
            export SAPPHIRE_PREDICTION_MODE="$original_mode"
            fi
            ;;
        maintenance:postprocessing_forecasts)
            run_maintenance_postprocessing_forecasts || exit_code=$?
            ;;
        maintenance:postprocessing_long_term)
            if should_skip_module long_term_forecasting; then
                log INFO "Skipping maintenance:postprocessing_long_term (not required for ${ORG} org)"
            else
                run_maintenance_postprocessing_long_term || exit_code=$?
            fi
            ;;
        recalculate_skill_metrics)
            run_recalculate_skill_metrics || exit_code=$?
            ;;
        recalculate_snow_norms)
            if should_skip_module preprocessing_gateway; then
                log INFO "Skipping recalculate_snow_norms (not required for ${ORG} org)"
            else
                run_recalculate_snow_norms || exit_code=$?
            fi
            ;;
        yearly)
            run_yearly_pipeline || exit_code=$?
            ;;
        calibrate_long_term)
            if should_skip_module long_term_forecasting; then
                log INFO "Skipping calibrate_long_term (not required for ${ORG} org)"
            else
                run_calibrate_long_term || exit_code=$?
            fi
            ;;
        initialize)
            run_initialize_deployment || exit_code=$?
            ;;
        # Single operational module targets (with per-module validation)
        preprocessing_runoff)
            run_preprocessing_runoff || exit_code=$?
            run_module_validation "preprocessing_runoff"
            ;;
        preprocessing_gateway)
            if should_skip_module preprocessing_gateway; then
                log INFO "Skipping preprocessing_gateway (not required for ${ORG} org)"
            else
                run_preprocessing_gateway || exit_code=$?
                run_module_validation "preprocessing_gateway"
            fi
            ;;
        linear_regression)
            run_linear_regression || exit_code=$?
            run_module_validation "linear_regression"
            ;;
        machine_learning)
            if should_skip_module machine_learning; then
                log INFO "Skipping machine_learning (not required for ${ORG} org)"
            else
                local original_mode="${SAPPHIRE_PREDICTION_MODE:-}"
                resolve_ml_bare_target_modes
                local ran_modes=()
                for mode in "${ML_BARE_RESOLVED_MODES[@]}"; do
                    export SAPPHIRE_PREDICTION_MODE="$mode"
                    log INFO "Running machine_learning for mode: ${mode}"
                    ran_modes+=("$mode")
                    run_machine_learning || { exit_code=$?; break; }
                done
                # Validate only the mode(s) actually attempted, each under
                # its own mode -- validating the pre-loop original_mode
                # would check a horizon ML did not necessarily produce, and
                # a mode that never ran (loop broke early) must not be
                # validated either. Pass the mode as run_module_validation's
                # label suffix (INFRA-037) so PENTAD/DECAD each get their
                # own log file and summary row instead of the second call
                # truncating the first's log and reusing its label.
                for mode in ${ran_modes[@]+"${ran_modes[@]}"}; do
                    export SAPPHIRE_PREDICTION_MODE="$mode"
                    run_module_validation "machine_learning" "$mode"
                done
                export SAPPHIRE_PREDICTION_MODE="$original_mode"
            fi
            ;;
        postprocessing_forecasts)
            run_postprocessing_forecasts || exit_code=$?
            run_module_validation "postprocessing_forecasts"
            ;;
        long_term_forecasting)
            if should_skip_module long_term_forecasting; then
                log INFO "Skipping long_term_forecasting (not required for ${ORG} org)"
            else
                run_long_term_forecasting || exit_code=$?
                run_module_validation "long_term_forecasting"
            fi
            ;;
    esac

    local pipeline_elapsed=$(( $(get_timestamp) - pipeline_start ))

    # If the run aborted via the CONTINUE_ON_ERROR guard idiom, tell the
    # operator why and how to proceed. Exactly one emission per run.
    # PIPELINE_ABORTED can only become true when CONTINUE_ON_ERROR is false
    # (see the guard idiom), so no extra condition is needed here.
    if [ "$PIPELINE_ABORTED" = true ]; then
        emit_continue_on_error_hint "$target"
    fi

    # Print summary if we ran anything
    if [ ${#RESULTS_MODULE[@]} -gt 0 ]; then
        print_summary "$pipeline_elapsed" || exit_code=1
    fi

    exit $exit_code
}

# main() wraps arg parsing + dispatch + summary so this file can be sourced
# by a test harness without executing a real pipeline run: sourcing still
# runs the top-of-file initialisation (set -euo pipefail, mktemp -d, and the
# EXIT trap that removes ERROR_DIR), which is unavoidable and harmless, but
# it does NOT invoke main — that only happens when the script is executed
# directly (`bash run_locally.sh ...`), which is the only way it is invoked
# today. The guard below lets a harness source the file and call individual
# functions (e.g. emit_continue_on_error_hint) directly.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
