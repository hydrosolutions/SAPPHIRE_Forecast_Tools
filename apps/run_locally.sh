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
#   daily                                Short-term daily run (PENTAD + DECAD + maintenance)
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
#   maintenance:preprocessing_runoff         Runoff gap-filling (30-day lookback)
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
# Prerequisites:
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
    add_new_station.py
)

# ML maintenance scripts (no make_forecast.py — only gap-fill and recalc)
ML_MAINTENANCE_SCRIPTS=(
    recalculate_nan_forecasts.py
    fill_ml_gaps.py
    add_new_station.py
)

# Modules with maintenance modes
MAINTENANCE_MODULES=(
    preprocessing_runoff
    preprocessing_gateway
    linear_regression
    machine_learning
    postprocessing_forecasts
)

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

    # Color based on level
    case "$level" in
        INFO)  echo -e "${BLUE}${line}${NC}" ;;
        OK)    echo -e "${GREEN}${line}${NC}" ;;
        WARN)  echo -e "${YELLOW}${line}${NC}" ;;
        ERROR) echo -e "${RED}${line}${NC}" ;;
        *)     echo "$line" ;;
    esac

    # Also write to log file (without color codes)
    echo "$line" >> "$LOG_FILE"
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

check_venv() {
    local module="$1"
    local python_path="${SCRIPT_DIR}/${module}/.venv/bin/python"
    if [ ! -f "$python_path" ]; then
        return 1
    fi
    return 0
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
    for ev in "${extra_env[@]}"; do
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
        "${env_cmd[@]}" "$python_path" "$script" "${script_args[@]}" 2>&1
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

    # Operational run_forecast.py uses today's date (or LT_FORECAST_TODAY override)
    # and checks each mode's operational_issue_day to decide whether to run.
    local modes="${LT_OPERATIONAL_MODES:-0 1 2 3 4 5 6 7 8 9}"
    local today_args=()
    if [ -n "${LT_FORECAST_TODAY:-}" ]; then
        today_args=(--today "${LT_FORECAST_TODAY}")
    fi

    CURRENT_MODULE_LOG="${ERROR_DIR}/long_term_forecasting_operational.log"
    > "$CURRENT_MODULE_LOG"
    for month in $modes; do
        log INFO "  Month: month_${month} (operational)"
        if ! run_in_venv long_term_forecasting run_forecast.py \
            "lt_forecast_mode=month_${month}" \
            -- --all "${today_args[@]}"; then
            log WARN "  month_${month} failed, continuing with next month"
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
    banner "Maintenance: preprocessing_runoff --maintenance"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/preprocessing_runoff_maintenance.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv preprocessing_runoff preprocessing_runoff.py -- --maintenance
    local rc=$?

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
    run_in_venv linear_regression linear_regression.py -- --hindcast
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
    run_in_venv postprocessing_forecasts postprocessing_maintenance.py \
        "POSTPROCESSING_GAPFILL_WINDOW_DAYS=${POSTPROCESSING_GAPFILL_WINDOW_DAYS:-7}"
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

run_recalculate_monthly_skill_metrics() {
    banner "Recalculate: monthly skill metrics"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/postprocessing_forecasts_recalculate_monthly.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv postprocessing_forecasts recalculate_skill_metrics.py \
        "SAPPHIRE_PREDICTION_MODE=MONTHLY"
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "monthly skill metrics recalculation completed in $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (monthly skill metrics)" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log ERROR "monthly skill metrics recalculation failed (exit $rc) after $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (monthly skill metrics)" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
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

run_module_validation() {
    local module="$1"
    banner "API Data Validation (${module})"
    local start
    start=$(get_timestamp)

    CURRENT_MODULE_LOG="${ERROR_DIR}/api_validation_${module}.log"
    > "$CURRENT_MODULE_LOG"
    run_in_venv postprocessing_forecasts ../validate_pipeline/validate_pipeline.py \
        -- --module "$module"
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "api_validation (${module}) completed in $(format_duration $elapsed)"
        record_validation "api_validation (${module})" "PASS" "$elapsed" "$CURRENT_MODULE_LOG"
    else
        log WARN "api_validation (${module}) reported failures after $(format_duration $elapsed)"
        record_validation "api_validation (${module})" "FAIL" "$elapsed" "$CURRENT_MODULE_LOG"
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
    run_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    run_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # Forecasting + postprocessing runs per mode
    for mode in "${modes_to_run[@]}"; do
        export SAPPHIRE_PREDICTION_MODE="$mode"
        log INFO "Running forecasting for mode: ${mode}"

        run_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    done

    # Restore original mode
    export SAPPHIRE_PREDICTION_MODE="$original_mode"

    run_api_validation "short-term"
}

run_long_term_pipeline() {
    banner "LONG-TERM FORECAST PIPELINE"

    # Phase 1: Generate forecasts
    run_long_term_forecasting || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # Phase 2: Operational postprocessing
    run_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # Phase 3: Monthly skill metrics (needed for ensemble creation)
    run_recalculate_monthly_skill_metrics || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # Phase 4: Maintenance gap-fill (creates ensembles using skill metrics)
    run_maintenance_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    run_api_validation "long-term"
}

run_long_term_operational_pipeline() {
    banner "LONG-TERM OPERATIONAL PIPELINE"

    # Phase 1: Preprocessing (shared, runs once)
    log INFO "Phase 1: Preprocessing"
    run_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    run_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # Phase 2: Generate forecasts (operational run_forecast.py, months 0-9)
    run_long_term_forecasting_operational || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # Phase 3: Operational postprocessing
    run_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # Phase 4: Monthly skill metrics (needed for ensemble creation)
    run_recalculate_monthly_skill_metrics || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # Phase 5: Maintenance gap-fill (creates ensembles using skill metrics)
    run_maintenance_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    run_api_validation "long-term"
}

run_all() {
    banner "FULL PIPELINE (short-term + long-term)"
    run_short_term_pipeline
    local st_rc=$?
    run_long_term_pipeline
    local lt_rc=$?

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
    run_maintenance_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    run_maintenance_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # linear_regression and ML maintenance run per mode
    for mode in "${modes_to_run[@]}"; do
        export SAPPHIRE_PREDICTION_MODE="$mode"
        log INFO "Running maintenance for mode: ${mode}"

        run_maintenance_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_maintenance_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_maintenance_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    done

    # Long-term maintenance (mode-independent, runs once)
    log INFO "Running long-term postprocessing maintenance"
    run_maintenance_postprocessing_long_term || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    export SAPPHIRE_PREDICTION_MODE="$original_mode"
}

run_daily_pipeline() {
    banner "DAILY PIPELINE (short-term: PENTAD + DECAD + maintenance)"

    local original_mode="${SAPPHIRE_PREDICTION_MODE:-}"

    # --- Phase 1: Preprocessing (runs once) ---
    log INFO "Phase 1: Preprocessing (shared across all horizons)"
    run_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    run_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # --- Phase 2: Maintenance preprocessing (runs once) ---
    log INFO "Phase 2: Maintenance preprocessing"
    run_maintenance_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    run_maintenance_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # --- Phase 3: Forecasting + postprocessing per horizon ---
    for mode in PENTAD DECAD; do
        export SAPPHIRE_PREDICTION_MODE="$mode"
        log INFO "Phase 3: ML + linear regression + postprocessing (${mode})"

        run_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    done

    # --- Phase 4: Maintenance per horizon ---
    for mode in PENTAD DECAD; do
        export SAPPHIRE_PREDICTION_MODE="$mode"
        log INFO "Phase 4: ML + LR + postprocessing maintenance (${mode})"

        run_maintenance_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_maintenance_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_maintenance_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    done

    # Restore original mode
    export SAPPHIRE_PREDICTION_MODE="$original_mode"

    run_api_validation "daily"
}

run_yearly_pipeline() {
    banner "YEARLY PIPELINE (snow norms + skill metrics)"
    run_recalculate_snow_norms || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    run_recalculate_skill_metrics || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
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
            log OK "Prediction mode: PENTAD + DECAD (daily)"
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
        *)                               modules_to_check=("$target") ;;
    esac

    # Check venvs
    for module in "${modules_to_check[@]}"; do
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
                          (does not include long-term forecasting)

Operational targets:
  short-term              Run the short-term forecast pipeline
  long-term               Long-term pipeline in simulate mode (for testing)
  long-term-operational   Full long-term pipeline: preprocessing + run_forecast.py
                          (months 0-9) + postprocess + skill metrics + gap-fill
  all                     Run both short-term and long-term pipelines
  <module_name>           Run a single module (see list below)

Maintenance targets:
  maintenance             Run all maintenance tasks (gap-fill + hindcast)
  maintenance:preprocessing_runoff    Runoff gap-filling (30-day lookback)
  maintenance:preprocessing_gateway   Extend ERA5 reanalysis data
  maintenance:linear_regression       Linear regression hindcast
  maintenance:machine_learning        ML NaN recalc + gap-fill + new stations
  maintenance:postprocessing_forecasts  Fill missing ensemble forecasts
  maintenance:postprocessing_long_term  Fill missing monthly ensemble forecasts
  calibrate_long_term     Calibrate and hindcast long-term models
  recalculate_skill_metrics  Full skill metrics rebuild (run yearly)
  recalculate_snow_norms  Yearly snow norm recalculation
  yearly                  All yearly tasks (snow norms + skill metrics)

Modules (for single-module operational runs):
  preprocessing_runoff    Process runoff data
  preprocessing_gateway   Quantile mapping, ERA5 extension, snow data
  linear_regression       Linear regression forecasts
  machine_learning        ML forecasts (TFT, TIDE, TSMIXER)
  postprocessing_forecasts  Post-process forecast outputs
  long_term_forecasting   Long-term monthly forecasts

Flags:
  --continue-on-error     Don't abort on first module failure
  --dry-run               Validate environment and venvs without running
  --help                  Show this help message

Environment variables:
  ieasyhydroforecast_env_file_path   Path to .env config file (required)
  SAPPHIRE_PREDICTION_MODE           PENTAD, DECAD, or BOTH (short-term/maintenance)
  lt_forecast_mode                   Specific month for long-term (e.g. month_3)
  LT_SIMULATE_YEARS                  Space-separated years to simulate (default: 2024)
  LT_SIMULATE_NUM_MONTHS             Months to simulate per year (default: 1)
  LT_SIMULATE_MODES                  Space-separated month modes to run (default: "0")
  LT_OPERATIONAL_MODES                Month modes for long-term-operational (default: "0 1 2 3 4 5 6 7 8 9")
  LT_FORECAST_TODAY                   Override today's date for run_forecast.py (YYYY-MM-DD)
  POSTPROCESSING_GAPFILL_WINDOW_MONTHS  Lookback for long-term gap-fill (default: 3)

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
  ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh long-term-operational

  # Long-term operational with date override
  LT_FORECAST_TODAY=2026-02-25 \
    ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh long-term-operational

  # Long-term calibration
  ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh calibrate_long_term

  # Long-term postprocessing gap-fill
  ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh maintenance:postprocessing_long_term

  # Dry run
  bash apps/run_locally.sh --dry-run maintenance
USAGE
}

# ---------------------------------------------------------------------------
# Argument parsing and main dispatch
# ---------------------------------------------------------------------------

main() {
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
    local valid_targets="daily short-term long-term long-term-operational all maintenance calibrate_long_term recalculate_skill_metrics recalculate_snow_norms yearly maintenance:postprocessing_long_term"
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

    banner "SAPPHIRE Local Pipeline Runner"
    log INFO "Target: ${target}"
    log INFO "Continue on error: ${CONTINUE_ON_ERROR}"
    log INFO "Dry run: ${DRY_RUN}"
    log INFO "Log file: ${LOG_FILE}"

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
            run_maintenance_preprocessing_gateway || exit_code=$?
            ;;
        maintenance:linear_regression)
            run_maintenance_linear_regression || exit_code=$?
            ;;
        maintenance:machine_learning)
            run_maintenance_machine_learning || exit_code=$?
            ;;
        maintenance:postprocessing_forecasts)
            run_maintenance_postprocessing_forecasts || exit_code=$?
            ;;
        maintenance:postprocessing_long_term)
            run_maintenance_postprocessing_long_term || exit_code=$?
            ;;
        recalculate_skill_metrics)
            run_recalculate_skill_metrics || exit_code=$?
            ;;
        recalculate_snow_norms)
            run_recalculate_snow_norms || exit_code=$?
            ;;
        yearly)
            run_yearly_pipeline || exit_code=$?
            ;;
        calibrate_long_term)
            run_calibrate_long_term || exit_code=$?
            ;;
        # Single operational module targets (with per-module validation)
        preprocessing_runoff)
            run_preprocessing_runoff || exit_code=$?
            run_module_validation "preprocessing_runoff"
            ;;
        preprocessing_gateway)
            run_preprocessing_gateway || exit_code=$?
            run_module_validation "preprocessing_gateway"
            ;;
        linear_regression)
            run_linear_regression || exit_code=$?
            run_module_validation "linear_regression"
            ;;
        machine_learning)
            run_machine_learning || exit_code=$?
            run_module_validation "machine_learning"
            ;;
        postprocessing_forecasts)
            run_postprocessing_forecasts || exit_code=$?
            run_module_validation "postprocessing_forecasts"
            ;;
        long_term_forecasting)
            run_long_term_forecasting || exit_code=$?
            run_module_validation "long_term_forecasting"
            ;;
    esac

    local pipeline_elapsed=$(( $(get_timestamp) - pipeline_start ))

    # Print summary if we ran anything
    if [ ${#RESULTS_MODULE[@]} -gt 0 ]; then
        print_summary "$pipeline_elapsed" || exit_code=1
    fi

    exit $exit_code
}

main "$@"
