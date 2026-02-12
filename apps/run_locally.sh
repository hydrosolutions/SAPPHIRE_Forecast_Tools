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
# Maintenance usage (gap-fill, hindcast, recalculation):
#   SAPPHIRE_PREDICTION_MODE=BOTH \
#     ieasyhydroforecast_env_file_path=/path/to/.env \
#     bash apps/run_locally.sh maintenance
#
#   bash apps/run_locally.sh maintenance:linear_regression
#   bash apps/run_locally.sh maintenance:postprocessing_forecasts
#   bash apps/run_locally.sh recalculate_skill_metrics
#   bash apps/run_locally.sh calibrate_long_term
#
# Combined targets:
#   daily                                Full daily run (PENTAD + DECAD + maintenance)
#
# Operational targets:
#   short-term                          Short-term forecast pipeline
#   long-term                           Long-term forecast pipeline (months 0-9)
#   all                                 Both pipelines
#   <module>                            Single module by name
#
# Maintenance targets:
#   maintenance                              All maintenance tasks
#   maintenance:preprocessing_runoff         Runoff gap-filling (30-day lookback)
#   maintenance:preprocessing_gateway        Extend ERA5 reanalysis data
#   maintenance:linear_regression            Linear regression hindcast
#   maintenance:machine_learning             ML NaN recalc + gap-fill + new stations
#   maintenance:postprocessing_forecasts     Fill missing ensemble forecasts
#   calibrate_long_term                      Calibrate and hindcast long-term models
#   recalculate_skill_metrics                Full skill metrics rebuild (yearly)
#
# Flags:
#   --continue-on-error   Don't abort on first module failure
#   --dry-run             Validate environment and venvs without running
#   --help                Show full help message
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

# Tracking arrays
declare -a RESULTS_MODULE=()
declare -a RESULTS_STATUS=()
declare -a RESULTS_TIME=()

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
    RESULTS_MODULE+=("$module")
    RESULTS_STATUS+=("$status")
    RESULTS_TIME+=("$elapsed")
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

    # Run in subshell from the module directory
    (
        cd "$module_dir"
        "${env_cmd[@]}" "$python_path" "$script" "${script_args[@]}" 2>&1
    ) | tee -a "$LOG_FILE"

    return "${PIPESTATUS[0]}"
}

# ---------------------------------------------------------------------------
# Module runner functions
# ---------------------------------------------------------------------------

run_preprocessing_runoff() {
    banner "Module: preprocessing_runoff"
    local start
    start=$(get_timestamp)

    run_in_venv preprocessing_runoff preprocessing_runoff.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "preprocessing_runoff completed in $(format_duration $elapsed)"
        record_result "preprocessing_runoff" "PASS" "$elapsed"
    else
        log ERROR "preprocessing_runoff failed (exit $rc) after $(format_duration $elapsed)"
        record_result "preprocessing_runoff" "FAIL" "$elapsed"
    fi
    return $rc
}

run_preprocessing_gateway() {
    banner "Module: preprocessing_gateway"
    local start
    start=$(get_timestamp)
    local rc=0

    for script in Quantile_Mapping_OP.py extend_era5_reanalysis.py snow_data_operational.py; do
        run_in_venv preprocessing_gateway "$script" || { rc=$?; break; }
    done

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "preprocessing_gateway completed in $(format_duration $elapsed)"
        record_result "preprocessing_gateway" "PASS" "$elapsed"
    else
        log ERROR "preprocessing_gateway failed (exit $rc) after $(format_duration $elapsed)"
        record_result "preprocessing_gateway" "FAIL" "$elapsed"
    fi
    return $rc
}

run_linear_regression() {
    banner "Module: linear_regression"
    local start
    start=$(get_timestamp)

    run_in_venv linear_regression linear_regression.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "linear_regression completed in $(format_duration $elapsed)"
        record_result "linear_regression" "PASS" "$elapsed"
    else
        log ERROR "linear_regression failed (exit $rc) after $(format_duration $elapsed)"
        record_result "linear_regression" "FAIL" "$elapsed"
    fi
    return $rc
}

run_machine_learning() {
    banner "Module: machine_learning"
    local start
    start=$(get_timestamp)
    local rc=0

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
        record_result "machine_learning" "PASS" "$elapsed"
    else
        log ERROR "machine_learning failed (exit $rc) after $(format_duration $elapsed)"
        record_result "machine_learning" "FAIL" "$elapsed"
    fi
    return $rc
}

run_postprocessing_forecasts() {
    banner "Module: postprocessing_forecasts (operational)"
    local start
    start=$(get_timestamp)

    run_in_venv postprocessing_forecasts postprocessing_operational.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "postprocessing_forecasts completed in $(format_duration $elapsed)"
        record_result "postprocessing_forecasts" "PASS" "$elapsed"
    else
        log ERROR "postprocessing_forecasts failed (exit $rc) after $(format_duration $elapsed)"
        record_result "postprocessing_forecasts" "FAIL" "$elapsed"
    fi
    return $rc
}

run_long_term_forecasting() {
    banner "Module: long_term_forecasting"
    local start
    start=$(get_timestamp)
    local rc=0
    local any_failed=false

    # If lt_forecast_mode is set, run just that month
    if [ -n "${lt_forecast_mode:-}" ]; then
        log INFO "  Running single mode: ${lt_forecast_mode}"
        run_in_venv long_term_forecasting run_forecast.py \
            "lt_forecast_mode=${lt_forecast_mode}" \
            -- --all \
            || rc=$?
    else
        # Run all months 0-9; continue even if one fails
        for month in 0 1 2 3 4 5 6 7 8 9; do
            log INFO "  Month: ${month}"
            if ! run_in_venv long_term_forecasting run_forecast.py \
                "lt_forecast_mode=month_${month}" \
                -- --all; then
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
        record_result "long_term_forecasting" "PASS" "$elapsed"
    else
        log ERROR "long_term_forecasting had failures after $(format_duration $elapsed)"
        record_result "long_term_forecasting" "FAIL" "$elapsed"
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

    run_in_venv preprocessing_runoff preprocessing_runoff.py -- --maintenance
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "preprocessing_runoff maintenance completed in $(format_duration $elapsed)"
        record_result "preprocessing_runoff (maintenance)" "PASS" "$elapsed"
    else
        log ERROR "preprocessing_runoff maintenance failed (exit $rc) after $(format_duration $elapsed)"
        record_result "preprocessing_runoff (maintenance)" "FAIL" "$elapsed"
    fi
    return $rc
}

run_maintenance_preprocessing_gateway() {
    banner "Maintenance: preprocessing_gateway (extend ERA5 reanalysis)"
    local start
    start=$(get_timestamp)

    run_in_venv preprocessing_gateway extend_era5_reanalysis.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "preprocessing_gateway maintenance completed in $(format_duration $elapsed)"
        record_result "preprocessing_gateway (maintenance)" "PASS" "$elapsed"
    else
        log ERROR "preprocessing_gateway maintenance failed (exit $rc) after $(format_duration $elapsed)"
        record_result "preprocessing_gateway (maintenance)" "FAIL" "$elapsed"
    fi
    return $rc
}

run_maintenance_linear_regression() {
    banner "Maintenance: linear_regression --hindcast"
    local start
    start=$(get_timestamp)

    run_in_venv linear_regression linear_regression.py -- --hindcast
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "linear_regression hindcast completed in $(format_duration $elapsed)"
        record_result "linear_regression (hindcast)" "PASS" "$elapsed"
    else
        log ERROR "linear_regression hindcast failed (exit $rc) after $(format_duration $elapsed)"
        record_result "linear_regression (hindcast)" "FAIL" "$elapsed"
    fi
    return $rc
}

run_maintenance_machine_learning() {
    banner "Maintenance: machine_learning (NaN recalc + gap-fill + new stations)"
    local start
    start=$(get_timestamp)
    local rc=0

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
        record_result "machine_learning (maintenance)" "PASS" "$elapsed"
    else
        log ERROR "machine_learning maintenance failed (exit $rc) after $(format_duration $elapsed)"
        record_result "machine_learning (maintenance)" "FAIL" "$elapsed"
    fi
    return $rc
}

run_maintenance_postprocessing_forecasts() {
    banner "Maintenance: postprocessing_forecasts (gap-fill)"
    local start
    start=$(get_timestamp)

    run_in_venv postprocessing_forecasts postprocessing_maintenance.py \
        "POSTPROCESSING_GAPFILL_WINDOW_DAYS=${POSTPROCESSING_GAPFILL_WINDOW_DAYS:-7}"
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "postprocessing_forecasts maintenance completed in $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (maintenance)" "PASS" "$elapsed"
    else
        log ERROR "postprocessing_forecasts maintenance failed (exit $rc) after $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (maintenance)" "FAIL" "$elapsed"
    fi
    return $rc
}

run_recalculate_skill_metrics() {
    banner "Recalculate: postprocessing_forecasts (full skill metrics rebuild)"
    local start
    start=$(get_timestamp)

    run_in_venv postprocessing_forecasts recalculate_skill_metrics.py
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "skill metrics recalculation completed in $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (recalculate)" "PASS" "$elapsed"
    else
        log ERROR "skill metrics recalculation failed (exit $rc) after $(format_duration $elapsed)"
        record_result "postprocessing_forecasts (recalculate)" "FAIL" "$elapsed"
    fi
    return $rc
}

run_calibrate_long_term() {
    banner "Calibrate: long_term_forecasting"
    local start
    start=$(get_timestamp)

    run_in_venv long_term_forecasting calibrate_and_hindcast.py \
        "lt_forecast_mode=${lt_forecast_mode:-monthly}" \
        -- --all
    local rc=$?

    local elapsed=$(( $(get_timestamp) - start ))
    if [ $rc -eq 0 ]; then
        log OK "long_term calibration completed in $(format_duration $elapsed)"
        record_result "long_term_forecasting (calibrate)" "PASS" "$elapsed"
    else
        log ERROR "long_term calibration failed (exit $rc) after $(format_duration $elapsed)"
        record_result "long_term_forecasting (calibrate)" "FAIL" "$elapsed"
    fi
    return $rc
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

        run_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    done

    # Restore original mode
    export SAPPHIRE_PREDICTION_MODE="$original_mode"
}

run_long_term_pipeline() {
    banner "LONG-TERM FORECAST PIPELINE"
    run_long_term_forecasting
}

run_all() {
    banner "FULL PIPELINE (short-term + long-term)"
    run_short_term_pipeline
    local st_rc=$?
    run_long_term_pipeline
    local lt_rc=$?

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

        run_maintenance_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_maintenance_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_maintenance_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    done

    export SAPPHIRE_PREDICTION_MODE="$original_mode"
}

run_daily_pipeline() {
    banner "DAILY PIPELINE (PENTAD + DECAD + maintenance)"

    local original_mode="${SAPPHIRE_PREDICTION_MODE:-}"

    # --- Phase 1: Operational preprocessing (runs once) ---
    log INFO "Phase 1: Preprocessing (shared across all horizons)"
    run_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    run_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # --- Phase 2: ML forecasting (runs once — produces 10-day forecasts) ---
    log INFO "Phase 2: Machine learning forecasts (horizon-independent)"
    run_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # --- Phase 3: LR + postprocessing per horizon ---
    for mode in PENTAD DECAD; do
        export SAPPHIRE_PREDICTION_MODE="$mode"
        log INFO "Phase 3: Linear regression + postprocessing (${mode})"

        run_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    done

    # --- Phase 4: Maintenance preprocessing (runs once) ---
    log INFO "Phase 4: Maintenance preprocessing"
    run_maintenance_preprocessing_runoff || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    run_maintenance_preprocessing_gateway || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # --- Phase 5: ML maintenance (runs once — horizon-independent) ---
    log INFO "Phase 5: ML maintenance (horizon-independent)"
    run_maintenance_machine_learning || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }

    # --- Phase 6: LR + postprocessing maintenance per horizon ---
    for mode in PENTAD DECAD; do
        export SAPPHIRE_PREDICTION_MODE="$mode"
        log INFO "Phase 6: LR + postprocessing maintenance (${mode})"

        run_maintenance_linear_regression || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
        run_maintenance_postprocessing_forecasts || { [ "$CONTINUE_ON_ERROR" = false ] && return 1; }
    done

    # Restore original mode
    export SAPPHIRE_PREDICTION_MODE="$original_mode"
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
    esac

    # Determine which modules to validate
    local modules_to_check=()
    case "$target" in
        short-term)                      modules_to_check=("${SHORT_TERM_MODULES[@]}") ;;
        long-term)                       modules_to_check=(long_term_forecasting) ;;
        all)                             modules_to_check=("${ALL_MODULES[@]}") ;;
        daily)                           modules_to_check=("${SHORT_TERM_MODULES[@]}") ;;
        maintenance)                     modules_to_check=("${MAINTENANCE_MODULES[@]}") ;;
        maintenance:*)                   modules_to_check=("${target#maintenance:}") ;;
        calibrate_long_term)             modules_to_check=(long_term_forecasting) ;;
        recalculate_skill_metrics)       modules_to_check=(postprocessing_forecasts) ;;
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

print_summary() {
    local total_time=$1
    echo "" | tee -a "$LOG_FILE"
    banner "PIPELINE SUMMARY"

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

    echo "" | tee -a "$LOG_FILE"
    log INFO "Total: ${pass_count} passed, ${fail_count} failed, $(format_duration "$total_time") elapsed"
    log INFO "Log file: ${LOG_FILE}"

    if [ $fail_count -gt 0 ]; then
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
  daily                   Full daily run: PENTAD + DECAD + maintenance
                          (preprocessing runs once, shared across horizons)

Operational targets:
  short-term              Run the short-term forecast pipeline
  long-term               Run the long-term forecast pipeline (months 0-9)
  all                     Run both short-term and long-term pipelines
  <module_name>           Run a single module (see list below)

Maintenance targets:
  maintenance             Run all maintenance tasks (gap-fill + hindcast)
  maintenance:preprocessing_runoff    Runoff gap-filling (30-day lookback)
  maintenance:preprocessing_gateway   Extend ERA5 reanalysis data
  maintenance:linear_regression       Linear regression hindcast
  maintenance:machine_learning        ML NaN recalc + gap-fill + new stations
  maintenance:postprocessing_forecasts  Fill missing ensemble forecasts
  calibrate_long_term     Calibrate and hindcast long-term models
  recalculate_skill_metrics  Full skill metrics rebuild (run yearly)

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

  # Long-term calibration
  ieasyhydroforecast_env_file_path=~/config/.env \
    bash apps/run_locally.sh calibrate_long_term

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
    local valid_targets="daily short-term long-term all maintenance calibrate_long_term recalculate_skill_metrics"
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
        recalculate_skill_metrics)
            run_recalculate_skill_metrics || exit_code=$?
            ;;
        calibrate_long_term)
            run_calibrate_long_term || exit_code=$?
            ;;
        # Single operational module targets
        preprocessing_runoff)
            run_preprocessing_runoff || exit_code=$?
            ;;
        preprocessing_gateway)
            run_preprocessing_gateway || exit_code=$?
            ;;
        linear_regression)
            run_linear_regression || exit_code=$?
            ;;
        machine_learning)
            run_machine_learning || exit_code=$?
            ;;
        postprocessing_forecasts)
            run_postprocessing_forecasts || exit_code=$?
            ;;
        long_term_forecasting)
            run_long_term_forecasting || exit_code=$?
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
