#!/usr/bin/env bash
# ============================================================================
# dev_local_backfill.sh
#
# Dev-machine helper that populates a local SAPPHIRE stack with an
# organization's operational data, so a developer can iterate against
# realistic data without needing production access.
#
# This script orchestrates pre-existing wrappers — the migration toolkit
# (bin/initialize_*_history.sh §5 wrappers), sibling LR/LT hindcast
# scripts (bin/initialize_site_backfill.sh, apps/long_term_forecasting/
# calibrate_and_hindcast.py), and the regenerate-hooks meta-wrapper
# (bin/initialize_regenerate_hooks.sh). It is intentionally additive: it
# never edits source data, never writes to production, and is intended for
# DEV-MACHINE USE ONLY.
#
# Phase summary (default vs opt-in):
#   1. Preflight          (DEFAULT;  --skip-preflight to disable)
#   2. CSV->DB migration  (DEFAULT;  --skip-csv to disable)
#   3. Today's pipeline   (OPT-IN;   --run-pipeline)
#   4a. LR hindcast       (OPT-IN;   --run-lr-hindcasts)
#   4b. ML hindcast       (OPT-IN;   --run-ml-hindcasts -> NOT IMPLEMENTED)
#   4c. LT hindcast       (OPT-IN;   --run-lt-hindcasts)
#   5. Regenerate hooks   (OPT-IN;   --run-hooks)
#   6. Verification       (DEFAULT;  --skip-verify to disable)
#
# Prerequisites (caller satisfies):
#   - Local SAPPHIRE stack up:  cd sapphire && docker compose up -d
#   - Stack health green:       curl -sf http://localhost:8000/health/ready
#   - Alembic at head on both service DBs (script enforces this)
#   - Organization data root locally available with intermediate_data/
#   - On Apple Silicon:         export DOCKER_DEFAULT_PLATFORM=linux/amd64
#   - iEH HF SSH tunnel up IF --run-hooks
#   - Post-MIG-003 (PR #362) + post-MIG-005 (PR #363) state of
#     maxat_sapphire_2. Without MIG-003 merged, Phase 2's
#     initialize_long_forecast_history.sh would error on lowercase
#     horizon_type in SQL.
#
# This script must NEVER be invoked on a production server. The
# --run-hooks phase touches crontab(1) (pauses + restores cron). The
# CSV->DB phase writes to whatever local databases the local stack
# points at — that is the entire purpose. Do not run against production.
# ============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TIMESTAMP="$(date -u '+%Y%m%dT%H%M%SZ')"
LOG_DIR="${REPO_ROOT}/logs/dev_local_backfill"
LOG_FILE="${LOG_DIR}/backfill_${TIMESTAMP}.log"

DEFAULT_IMAGE="mabesa/sapphire-prepgateway:2026-06"

# Defaults
ENV_FILE=""
IMAGE="${DEFAULT_IMAGE}"
DRY_RUN=false
CONTINUE_ON_ERROR=false
SKIP_PREFLIGHT=false
SKIP_CSV=false
SKIP_VERIFY=false
RUN_PIPELINE=false
RUN_LR_HINDCASTS=false
RUN_ML_HINDCASTS=false
RUN_LT_HINDCASTS=false
RUN_HOOKS=false
LR_SKIP_PREPRUNOFF=false
LR_SKIP_LINREG=false
LR_SKIP_SKILL=false
LR_SITE_CODE=""
HINDCAST_START_YEAR="2010"

# Accumulator: set to true by fail_or_warn when --continue-on-error degrades
# a fatal to a warning. Final exit-code check honors this.
ANY_PHASE_FAILED=false

# Colour support (only when stdout is a TTY).
if [[ -t 1 ]]; then
    C_RESET=$'\033[0m'
    C_RED=$'\033[31m'
    C_GREEN=$'\033[32m'
    C_YELLOW=$'\033[33m'
    C_BLUE=$'\033[34m'
    C_BOLD=$'\033[1m'
else
    C_RESET=""
    C_RED=""
    C_GREEN=""
    C_YELLOW=""
    C_BLUE=""
    C_BOLD=""
fi

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

_ts() {
    date -u '+%Y-%m-%dT%H:%M:%SZ'
}

log()    { printf '%s [INFO]  %s\n' "$(_ts)" "$*"; }
phase()  { printf '\n%s%s [PHASE] %s%s\n' "${C_BOLD}${C_BLUE}" "$(_ts)" "$*" "${C_RESET}"; }
ok()     { printf '%s [%sOK%s]    %s\n' "$(_ts)" "${C_GREEN}" "${C_RESET}" "$*"; }
warn()   { printf '%s [%sWARN%s]  %s\n' "$(_ts)" "${C_YELLOW}" "${C_RESET}" "$*" >&2; }
fatal()  {
    printf '%s [%sFATAL%s] %s\n' "$(_ts)" "${C_RED}" "${C_RESET}" "$*" >&2
    exit 1
}

# fail_or_warn: under --continue-on-error, downgrade a fatal to a warning
# and mark ANY_PHASE_FAILED so the script still exits non-zero at the end.
fail_or_warn() {
    if [[ "${CONTINUE_ON_ERROR}" == "true" ]]; then
        warn "$*"
        ANY_PHASE_FAILED=true
    else
        fatal "$*"
    fi
}

# ---------------------------------------------------------------------------
# CLI argument helpers
# ---------------------------------------------------------------------------

require_value() {
    # require_value <flag-name> <value>
    # Exits 2 (with usage hint) if value is empty or looks like another flag.
    local flag="$1"
    local val="${2-}"
    if [[ -z "${val}" || "${val}" == --* ]]; then
        printf '%s [%sFATAL%s] %s requires a value\n' \
            "$(_ts)" "${C_RED}" "${C_RESET}" "${flag}" >&2
        printf 'Run with --help for usage.\n' >&2
        exit 2
    fi
}

validate_yyyy() {
    # validate_yyyy <flag-name> <value>
    local flag="$1"
    local val="$2"
    if ! [[ "${val}" =~ ^[0-9]{4}$ ]]; then
        printf '%s [%sFATAL%s] %s expects a 4-digit year, got %q\n' \
            "$(_ts)" "${C_RED}" "${C_RESET}" "${flag}" "${val}" >&2
        exit 2
    fi
    if (( val < 1900 || val > 2100 )); then
        printf '%s [%sFATAL%s] %s year out of range [1900,2100], got %s\n' \
            "$(_ts)" "${C_RED}" "${C_RESET}" "${flag}" "${val}" >&2
        exit 2
    fi
}

validate_station_code() {
    # validate_station_code <flag-name> <value>
    # All-numeric (sentinel 19999 is acceptable; real codes never logged).
    local flag="$1"
    local val="$2"
    if ! [[ "${val}" =~ ^[0-9]+$ ]]; then
        printf '%s [%sFATAL%s] %s expects a numeric station code (e.g. 19999), got non-numeric value\n' \
            "$(_ts)" "${C_RED}" "${C_RESET}" "${flag}" >&2
        exit 2
    fi
}

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

print_help() {
    cat <<'HELP'
Usage: bash bin/dev_local_backfill.sh <env_file> [OPTIONS]

Populate a local SAPPHIRE stack with an organization's operational data.

By default runs phases 1 (preflight), 2 (CSV->DB migration), 6 (verify).
Pipeline, hindcasts, and hooks are OPT-IN (off by default).

Arguments:
  env_file                  Path to org's .env file (e.g. ~/.../taj_data_forecast_tools/config/.env_develop_tjhm)

Options:
  --image TAG               prepgateway image tag (default: mabesa/sapphire-prepgateway:2026-06)
  --dry-run                 Pass --dry-run to each underlying wrapper that supports it.
                            NOTE: section-5 wrappers (Phase 2) and regenerate-hooks (Phase 5)
                            accept --dry-run; initialize_site_backfill.sh (Phase 4a) and
                            calibrate_and_hindcast.py (Phase 4c) do NOT — for those,
                            dry-run logs the command + returns 0 without invocation.
                            Wrappers still pull images, read DB target state, and write
                            log files; no INSERT/UPDATE to DBs.
  --continue-on-error       Phase failures degrade to warnings instead of fatal. Sets the
                            ANY_PHASE_FAILED accumulator; the script still exits non-zero
                            at the end if any phase failed.
  --skip-preflight          Skip Phase 1 prereq checks.
  --skip-csv                Skip Phase 2 section-5 CSV->DB migration.
  --run-pipeline            Phase 3: run today's operational pipeline.
                            Wall-clock: ~10-30 min depending on station count.
                            Current-date only — does NOT backfill historical PENTAD/DECADE.
  --run-lr-hindcasts        Phase 4a: LR + skill hindcast via bin/initialize_site_backfill.sh.
                            Wall-clock: ~30 min to 3 hours.
  --lr-skip-preprunoff      (Phase 4a passthrough) Skip preprunoff step inside site-backfill.
  --lr-skip-linreg          (Phase 4a passthrough) Skip linreg step inside site-backfill.
  --lr-skip-skill           (Phase 4a passthrough) Skip skill recalc inside site-backfill.
  --lr-site-code <CODE>     (Phase 4a passthrough) Single-station hindcast (debugging or sentinel 19999).
  --run-ml-hindcasts        Phase 4b: NOT IMPLEMENTED — exits with documentation pointer
                            to MIG-004 (the future ML hindcast wrapper spec). When MIG-004
                            ships: hours per model; potentially days for full multi-model coverage.
  --run-lt-hindcasts        Phase 4c: LT hindcast per supported mode via
                            calibrate_and_hindcast.py. Wall-clock: ~30 min to 2 hours per
                            mode; total typically 3-10 hours across 5 modes.
  --run-hooks               Phase 5: regenerate hooks (snow stats + hydrograph
                            month/season/quarter + skill).
                            Wall-clock: ~30 min for typical org.
                            WARNING: TOUCHES YOUR CRONTAB. Pauses cron at start, restores
                            on exit. We pass --allow-unpaused-cron so pause failures
                            degrade to warnings on dev hosts. Note: --allow-unpaused-cron
                            only handles content-level pause failures; missing crontab(1)
                            binary still hard-fails (per P6 design).
  --skip-verify             Skip Phase 6 row-count verification.
  --start-year YYYY         Hindcast / hook start year (default: 2010).
                            Forwarded to Phase 4a as --start-date "${YYYY}-01-01" and to
                            Phase 5 as --start-year YYYY.
  -h, --help                Print this and exit.

Examples:
  # Standard run (CSV->DB only; ~20-30 min):
  bash bin/dev_local_backfill.sh ~/Documents/GitHub/taj_data_forecast_tools/config/.env_develop_tjhm

  # Dry-run inventory of CSV phase:
  bash bin/dev_local_backfill.sh ~/.../.env_develop_tjhm --dry-run

  # Full: CSV + today's pipeline + hooks (no historical hindcasts):
  bash bin/dev_local_backfill.sh ~/.../.env_develop_tjhm --run-pipeline --run-hooks --start-year 2010

Prerequisites (caller must satisfy before running):
  1. Local SAPPHIRE stack up: cd sapphire && docker compose up -d
  2. Stack health green: curl -sf http://localhost:8000/health/ready
  3. Alembic at head on both service DBs (script enforces this)
  4. Organization data root locally available with intermediate_data/ populated
  5. On Apple Silicon: export DOCKER_DEFAULT_PLATFORM=linux/amd64
  6. iEH HF SSH tunnel up IF --run-hooks (sync_long_horizon_hydrograph needs the SDK)
  7. (this script assumes post-MIG-003 state of maxat_sapphire_2; without
     MIG-003 PR #362 merged, Phase 2's initialize_long_forecast_history.sh
     would error on lowercase horizon_type in SQL)

Intended for dev-machine use only. NEVER invoke on a production server.
HELP
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

if [[ $# -eq 0 ]]; then
    print_help
    exit 2
fi

# First positional must be env_file unless it's a help flag.
case "${1-}" in
    -h|--help)
        print_help
        exit 0
        ;;
    --*)
        printf '%s [FATAL] First argument must be path to env_file, got option %q\n' \
            "$(_ts)" "$1" >&2
        printf 'Run with --help for usage.\n' >&2
        exit 2
        ;;
    "")
        print_help
        exit 2
        ;;
    *)
        ENV_FILE="$1"
        shift
        ;;
esac

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            print_help
            exit 0
            ;;
        --image)
            require_value "--image" "${2-}"
            IMAGE="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --continue-on-error)
            CONTINUE_ON_ERROR=true
            shift
            ;;
        --skip-preflight)
            SKIP_PREFLIGHT=true
            shift
            ;;
        --skip-csv)
            SKIP_CSV=true
            shift
            ;;
        --skip-verify)
            SKIP_VERIFY=true
            shift
            ;;
        --run-pipeline)
            RUN_PIPELINE=true
            shift
            ;;
        --run-lr-hindcasts)
            RUN_LR_HINDCASTS=true
            shift
            ;;
        --lr-skip-preprunoff)
            LR_SKIP_PREPRUNOFF=true
            shift
            ;;
        --lr-skip-linreg)
            LR_SKIP_LINREG=true
            shift
            ;;
        --lr-skip-skill)
            LR_SKIP_SKILL=true
            shift
            ;;
        --lr-site-code)
            require_value "--lr-site-code" "${2-}"
            validate_station_code "--lr-site-code" "$2"
            LR_SITE_CODE="$2"
            shift 2
            ;;
        --run-ml-hindcasts)
            RUN_ML_HINDCASTS=true
            shift
            ;;
        --run-lt-hindcasts)
            RUN_LT_HINDCASTS=true
            shift
            ;;
        --run-hooks)
            RUN_HOOKS=true
            shift
            ;;
        --start-year)
            require_value "--start-year" "${2-}"
            validate_yyyy "--start-year" "$2"
            HINDCAST_START_YEAR="$2"
            shift 2
            ;;
        *)
            printf '%s [FATAL] Unknown option %q\n' "$(_ts)" "$1" >&2
            printf 'Run with --help for usage.\n' >&2
            exit 2
            ;;
    esac
done

# Validate ENV_FILE
if [[ -z "${ENV_FILE}" ]]; then
    fatal "env_file argument is required. Run with --help for usage."
fi
if [[ ! -f "${ENV_FILE}" ]]; then
    fatal "env_file not found: ${ENV_FILE}"
fi

# ---------------------------------------------------------------------------
# Tee all output to log file
# ---------------------------------------------------------------------------

mkdir -p "${LOG_DIR}"
# Redirect stdout/stderr through tee so the operator sees output AND the log
# captures it. Using 'exec' here applies the redirect to the rest of the
# script.
exec > >(tee -a "${LOG_FILE}") 2>&1

# ---------------------------------------------------------------------------
# Run banner
# ---------------------------------------------------------------------------

log "dev_local_backfill.sh v3.5 starting"
log "  env_file:           ${ENV_FILE}"
log "  image:              ${IMAGE}"
log "  dry_run:            ${DRY_RUN}"
log "  continue_on_error:  ${CONTINUE_ON_ERROR}"
log "  skip_preflight:     ${SKIP_PREFLIGHT}"
log "  skip_csv:           ${SKIP_CSV}"
log "  skip_verify:        ${SKIP_VERIFY}"
log "  run_pipeline:       ${RUN_PIPELINE}"
log "  run_lr_hindcasts:   ${RUN_LR_HINDCASTS}"
log "  run_ml_hindcasts:   ${RUN_ML_HINDCASTS}"
log "  run_lt_hindcasts:   ${RUN_LT_HINDCASTS}"
log "  run_hooks:          ${RUN_HOOKS}"
log "  hindcast_start_yr:  ${HINDCAST_START_YEAR}"
log "  log_file:           ${LOG_FILE}"

# ---------------------------------------------------------------------------
# Phase 1 — Preflight
# ---------------------------------------------------------------------------

run_phase_1_preflight() {
    if [[ "${SKIP_PREFLIGHT}" == "true" ]]; then
        log "Phase 1 (preflight) skipped via --skip-preflight"
        return 0
    fi

    phase "Phase 1 — Preflight"

    # 1.1 Docker daemon reachable
    if ! docker info > /dev/null 2>&1; then
        fail_or_warn "Docker daemon is not reachable. Start Docker Desktop / dockerd and retry."
        return 0
    fi
    ok "docker daemon reachable"

    # 1.2 api-gateway readiness
    if ! curl -sf http://localhost:8000/health/ready > /dev/null; then
        fail_or_warn "api-gateway not ready at http://localhost:8000/health/ready. Bring the stack up: 'cd sapphire && docker compose up -d' and re-check 'curl -sf http://localhost:8000/health/ready'."
        return 0
    fi
    ok "api-gateway /health/ready: OK"

    # 1.3 Alembic head HARD-ENFORCED on both service DBs
    local prep_head post_head
    prep_head="$(docker exec sapphire-preprocessing-api alembic current 2>/dev/null | awk '/\(head\)/{print $1}')" || true
    if [[ -z "${prep_head}" ]]; then
        fail_or_warn "Could not read alembic head from sapphire-preprocessing-api. Run 'docker exec sapphire-preprocessing-api alembic current' manually to diagnose."
        return 0
    fi
    log "preprocessing alembic head: ${prep_head}"

    post_head="$(docker exec sapphire-postprocessing-api alembic current 2>/dev/null | awk '/\(head\)/{print $1}')" || true
    if [[ -z "${post_head}" ]]; then
        fail_or_warn "Could not read alembic head from sapphire-postprocessing-api. Run 'docker exec sapphire-postprocessing-api alembic current' manually to diagnose."
        return 0
    fi
    log "postprocessing alembic head: ${post_head}"
    ok "alembic head detected on both service DBs"

    # 1.4 arm64 + DOCKER_DEFAULT_PLATFORM check (warn-only)
    local arch
    arch="$(uname -m)"
    if [[ "${arch}" =~ ^(arm64|aarch64)$ ]] && [[ -z "${DOCKER_DEFAULT_PLATFORM-}" ]]; then
        warn "Host is ${arch} but DOCKER_DEFAULT_PLATFORM is unset. Recommend: export DOCKER_DEFAULT_PLATFORM=linux/amd64"
    else
        log "platform check: arch=${arch}, DOCKER_DEFAULT_PLATFORM=${DOCKER_DEFAULT_PLATFORM:-<unset>}"
    fi

    # 1.5 Pre-snapshot row counts on preprocessing-db (display only, no gating)
    local prep_user prep_db
    prep_user="$(docker exec sapphire-preprocessing-db printenv POSTGRES_USER 2>/dev/null || true)"
    prep_db="$(docker exec sapphire-preprocessing-db printenv POSTGRES_DB 2>/dev/null || true)"
    if [[ -z "${prep_user}" || -z "${prep_db}" ]]; then
        warn "Could not read POSTGRES_USER/POSTGRES_DB from sapphire-preprocessing-db. Skipping pre-snapshot."
    else
        log "preprocessing-db pre-snapshot (database=${prep_db}):"
        local tbl
        for tbl in runoffs hydrographs meteo snow; do
            local count
            count="$(docker exec sapphire-preprocessing-db psql -U "${prep_user}" -d "${prep_db}" -tAc "SELECT COUNT(*) FROM ${tbl};" 2>/dev/null || echo "?")"
            log "  ${tbl}: ${count} rows"
        done
    fi

    ok "Phase 1 (preflight) completed"
}

# ---------------------------------------------------------------------------
# Phase 2 — CSV -> DB migration
# ---------------------------------------------------------------------------

run_phase_2_csv() {
    if [[ "${SKIP_CSV}" == "true" ]]; then
        log "Phase 2 (CSV->DB) skipped via --skip-csv"
        return 0
    fi

    phase "Phase 2 — CSV->DB migration (section-5 wrappers)"

    local wrappers=(
        "initialize_runoff_day_history.sh"
        "initialize_meteo_history.sh"
        "initialize_snow_history.sh"
        "initialize_hydrograph_day_history.sh"
        "initialize_long_forecast_history.sh"
    )

    local w wrapper_path extra_args
    for w in "${wrappers[@]}"; do
        wrapper_path="${SCRIPT_DIR}/${w}"
        if [[ ! -f "${wrapper_path}" ]]; then
            fail_or_warn "Phase 2 wrapper missing: ${wrapper_path}"
            continue
        fi
        log "running ${w}"
        extra_args=()
        if [[ "${DRY_RUN}" == "true" ]]; then
            extra_args+=("--dry-run")
        fi
        if ! bash "${wrapper_path}" "${ENV_FILE}" --image "${IMAGE}" "${extra_args[@]}"; then
            fail_or_warn "Phase 2 wrapper failed: ${w}"
            continue
        fi
        ok "${w} completed"
    done

    ok "Phase 2 (CSV->DB) completed"
}

# ---------------------------------------------------------------------------
# Phase 3 — Today's operational pipeline
# ---------------------------------------------------------------------------

run_phase_3_pipeline() {
    if [[ "${RUN_PIPELINE}" != "true" ]]; then
        log "Phase 3 (today's pipeline) not requested (--run-pipeline)"
        return 0
    fi

    phase "Phase 3 — Today's operational pipeline"
    warn "SCOPE: current-date only; does NOT backfill historical PENTAD/DECADE forecasts."

    local steps=(
        "run_preprocessing_gateway.sh"
        "run_daily_maintenance.sh"
    )

    local step step_path
    for step in "${steps[@]}"; do
        step_path="${SCRIPT_DIR}/${step}"
        if [[ ! -f "${step_path}" ]]; then
            fail_or_warn "Phase 3 wrapper missing: ${step_path}"
            continue
        fi

        if [[ "${DRY_RUN}" == "true" ]]; then
            log "DRY-RUN: would invoke 'bash ${step_path} ${ENV_FILE}'"
            continue
        fi

        log "running ${step}"
        if ! bash "${step_path}" "${ENV_FILE}"; then
            fail_or_warn "Phase 3 wrapper failed: ${step}"
            continue
        fi
        ok "${step} completed"
    done

    ok "Phase 3 (today's pipeline) completed"
}

# ---------------------------------------------------------------------------
# Phase 4a — LR + skill hindcast
# ---------------------------------------------------------------------------

run_phase_4a_lr_hindcast() {
    if [[ "${RUN_LR_HINDCASTS}" != "true" ]]; then
        log "Phase 4a (LR hindcast) not requested (--run-lr-hindcasts)"
        return 0
    fi

    phase "Phase 4a — LR + skill hindcast"

    local wrapper="${SCRIPT_DIR}/initialize_site_backfill.sh"
    if [[ ! -f "${wrapper}" ]]; then
        fail_or_warn "Phase 4a wrapper missing: ${wrapper} (LR hindcast wrapper not present on this branch)"
        return 0
    fi

    # Build passthrough flags
    local passthrough=()
    passthrough+=("--start-date" "${HINDCAST_START_YEAR}-01-01")
    if [[ "${LR_SKIP_PREPRUNOFF}" == "true" ]]; then
        passthrough+=("--skip-preprunoff")
    fi
    if [[ "${LR_SKIP_LINREG}" == "true" ]]; then
        passthrough+=("--skip-linreg")
    fi
    if [[ "${LR_SKIP_SKILL}" == "true" ]]; then
        passthrough+=("--skip-skill")
    fi
    if [[ -n "${LR_SITE_CODE}" ]]; then
        passthrough+=("--site-code" "${LR_SITE_CODE}")
    fi

    # IMPORTANT: initialize_site_backfill.sh does NOT support --dry-run.
    # In dry-run we log the command and return 0; we do NOT forward --dry-run.
    if [[ "${DRY_RUN}" == "true" ]]; then
        log "DRY-RUN (no --dry-run forwarded; child does not support it): bash ${wrapper} ${ENV_FILE} ${passthrough[*]}"
        ok "Phase 4a (LR hindcast) dry-run logged"
        return 0
    fi

    log "running initialize_site_backfill.sh (this can take 30 min to 3 hours)"
    if ! bash "${wrapper}" "${ENV_FILE}" "${passthrough[@]}"; then
        fail_or_warn "Phase 4a wrapper failed: initialize_site_backfill.sh"
        return 0
    fi
    ok "Phase 4a (LR hindcast) completed"
}

# ---------------------------------------------------------------------------
# Phase 4b — ML hindcast SCAFFOLD (not implemented)
# ---------------------------------------------------------------------------

run_phase_4b_ml_hindcast() {
    if [[ "${RUN_ML_HINDCASTS}" != "true" ]]; then
        log "Phase 4b (ML hindcast) not requested (--run-ml-hindcasts)"
        return 0
    fi

    phase "Phase 4b — ML hindcast (NOT IMPLEMENTED)"

    log "Phase 4b is documented-not-implemented in v3.5 of this script."
    log "The proper path forward is the MIG-004 ML hindcast wrapper, currently"
    log "in spec review. See:"
    log "  doc/plans/issues/high_prio_gi_draft_migration_ml_hindcast_wrapper.md"
    log ""
    log "The MIG-004 wrapper will wrap apps/machine_learning/hindcast_ML_models.py"
    log "and expose its env-var contract:"
    log "  - SAPPHIRE_MODEL_TO_USE"
    log "  - SAPPHIRE_HINDCAST_MODE"
    log "  - ieasyhydroforecast_NEW_STATIONS  (per-station scoping)"
    log "  - ieasyhydroforecast_START_DATE"
    log "  - ieasyhydroforecast_END_DATE"
    log ""
    log "Until MIG-004 ships, ML hindcast must be invoked manually. This script"
    log "intentionally refuses to auto-invoke hindcast_ML_models.py because the"
    log "env-var orchestration is fragile and worth a dedicated wrapper."

    fail_or_warn "Phase 4b (--run-ml-hindcasts) not implemented; awaiting MIG-004 wrapper."
}

# ---------------------------------------------------------------------------
# Phase 4c — LT hindcast
# ---------------------------------------------------------------------------

run_phase_4c_lt_hindcast() {
    if [[ "${RUN_LT_HINDCASTS}" != "true" ]]; then
        log "Phase 4c (LT hindcast) not requested (--run-lt-hindcasts)"
        return 0
    fi

    phase "Phase 4c — LT hindcast"

    # Source common_functions.sh and read configuration (loads env vars from
    # the org's .env file).
    local common="${SCRIPT_DIR}/utils/common_functions.sh"
    if [[ ! -f "${common}" ]]; then
        fail_or_warn "Phase 4c could not source ${common}"
        return 0
    fi
    # shellcheck disable=SC1090
    source "${common}"

    # read_configuration sets a number of env vars from $ENV_FILE.
    if ! read_configuration "${ENV_FILE}"; then
        fail_or_warn "Phase 4c: read_configuration failed for ${ENV_FILE}"
        return 0
    fi

    # B1 fix (round-6 reviewer): bash-side whitespace normalization on the
    # comma-separated env var. MIG-005 fixed the Python side too; this is
    # belt-and-suspenders in case the env file is consumed by code that
    # bypasses the MIG-005-fixed path.
    local modes_raw="${ieasyhydroforecast_ml_long_term_supported_modes:-}"
    # Strip every whitespace character (spaces, tabs, etc.) from the value.
    modes_raw="${modes_raw//[[:space:]]/}"

    if [[ -z "${modes_raw}" ]]; then
        fail_or_warn "Phase 4c: ieasyhydroforecast_ml_long_term_supported_modes is empty after whitespace strip; nothing to hindcast"
        return 0
    fi

    # Comma-split into bash array.
    local LT_MODES=()
    IFS=',' read -r -a LT_MODES <<< "${modes_raw}"

    # Resolve LT config dir.
    local lt_config_dir="${ieasyhydroforecast_configuration_path:-}/${ieasyhydroforecast_ml_long_term_configuration:-}"
    if [[ -z "${ieasyhydroforecast_configuration_path:-}" ]] || [[ -z "${ieasyhydroforecast_ml_long_term_configuration:-}" ]]; then
        fail_or_warn "Phase 4c: ieasyhydroforecast_configuration_path or ieasyhydroforecast_ml_long_term_configuration not set"
        return 0
    fi
    log "LT config dir: ${lt_config_dir}"
    log "LT modes (post-strip): ${modes_raw}"

    local mode mode_config
    for mode in "${LT_MODES[@]}"; do
        # Defensive: skip empty modes (e.g. trailing comma).
        if [[ -z "${mode}" ]]; then
            continue
        fi
        mode_config="${lt_config_dir}/${mode}.json"
        if [[ ! -f "${mode_config}" ]]; then
            fail_or_warn "Phase 4c: config file missing for mode '${mode}': ${mode_config}"
            continue
        fi

        if [[ "${DRY_RUN}" == "true" ]]; then
            log "DRY-RUN (calibrate_and_hindcast.py has no --dry-run; logging command only):"
            log "  lt_forecast_mode=${mode} ieasyhydroforecast_env_file_path=${ENV_FILE} uv run python apps/long_term_forecasting/calibrate_and_hindcast.py --all"
            continue
        fi

        log "Phase 4c: hindcasting mode '${mode}' (this can take 30 min to 2 hours per mode)"
        if ! ( cd "${REPO_ROOT}" && \
               lt_forecast_mode="${mode}" \
               ieasyhydroforecast_env_file_path="${ENV_FILE}" \
               ieasyhydroforecast_ml_long_term_supported_modes="${modes_raw}" \
               uv run python apps/long_term_forecasting/calibrate_and_hindcast.py --all ); then
            fail_or_warn "Phase 4c failed for mode '${mode}'"
            continue
        fi
        ok "Phase 4c: mode '${mode}' completed"
    done

    ok "Phase 4c (LT hindcast) completed"
}

# ---------------------------------------------------------------------------
# Phase 5 — Regenerate hooks
# ---------------------------------------------------------------------------

run_phase_5_hooks() {
    if [[ "${RUN_HOOKS}" != "true" ]]; then
        log "Phase 5 (regenerate hooks) not requested (--run-hooks)"
        return 0
    fi

    phase "Phase 5 — Regenerate hooks"
    warn "WARNING: Phase 5 TOUCHES YOUR CRONTAB. Cron is paused at start and"
    warn "restored on exit. We pass --allow-unpaused-cron so content-level pause"
    warn "failures degrade to WARN on dev hosts. NOTE: --allow-unpaused-cron does"
    warn "NOT bypass a missing crontab(1) binary; that still hard-fails per P6 design."

    local wrapper="${SCRIPT_DIR}/initialize_regenerate_hooks.sh"
    if [[ ! -f "${wrapper}" ]]; then
        fail_or_warn "Phase 5 wrapper missing: ${wrapper}"
        return 0
    fi

    local extra_args=()
    extra_args+=("--allow-unpaused-cron")
    extra_args+=("--start-year" "${HINDCAST_START_YEAR}")
    if [[ "${DRY_RUN}" == "true" ]]; then
        extra_args+=("--dry-run")
    fi

    log "running initialize_regenerate_hooks.sh"
    if ! bash "${wrapper}" "${ENV_FILE}" "${extra_args[@]}"; then
        fail_or_warn "Phase 5 wrapper failed: initialize_regenerate_hooks.sh"
        return 0
    fi

    ok "Phase 5 (regenerate hooks) completed"
}

# ---------------------------------------------------------------------------
# Phase 6 — Verification
# ---------------------------------------------------------------------------

run_phase_6_verify() {
    if [[ "${SKIP_VERIFY}" == "true" ]]; then
        log "Phase 6 (verification) skipped via --skip-verify"
        return 0
    fi

    phase "Phase 6 — Verification (row counts)"

    # Pull DB credentials from container env.
    local prep_user prep_db post_user post_db
    prep_user="$(docker exec sapphire-preprocessing-db printenv POSTGRES_USER 2>/dev/null || true)"
    if [[ -z "${prep_user}" ]]; then
        fail_or_warn "Phase 6: could not read POSTGRES_USER from sapphire-preprocessing-db"
        return 0
    fi
    prep_db="$(docker exec sapphire-preprocessing-db printenv POSTGRES_DB 2>/dev/null || true)"
    if [[ -z "${prep_db}" ]]; then
        fail_or_warn "Phase 6: could not read POSTGRES_DB from sapphire-preprocessing-db"
        return 0
    fi
    post_user="$(docker exec sapphire-postprocessing-db printenv POSTGRES_USER 2>/dev/null || true)"
    if [[ -z "${post_user}" ]]; then
        fail_or_warn "Phase 6: could not read POSTGRES_USER from sapphire-postprocessing-db"
        return 0
    fi
    post_db="$(docker exec sapphire-postprocessing-db printenv POSTGRES_DB 2>/dev/null || true)"
    if [[ -z "${post_db}" ]]; then
        fail_or_warn "Phase 6: could not read POSTGRES_DB from sapphire-postprocessing-db"
        return 0
    fi

    log "preprocessing-db summary (database=${prep_db}):"
    local prep_sql post_sql
    prep_sql=$'SELECT \'runoffs.\'||horizon_type::text AS family, COUNT(*) AS rows, MIN(date), MAX(date) FROM runoffs GROUP BY horizon_type \
UNION ALL SELECT \'hydrographs.\'||horizon_type::text, COUNT(*), MIN(date), MAX(date) FROM hydrographs GROUP BY horizon_type \
UNION ALL SELECT \'meteo.\'||meteo_type::text, COUNT(*), MIN(date), MAX(date) FROM meteo GROUP BY meteo_type \
UNION ALL SELECT \'snow.\'||snow_type::text, COUNT(*), MIN(date), MAX(date) FROM snow GROUP BY snow_type \
ORDER BY family;'
    if ! docker exec sapphire-preprocessing-db psql -U "${prep_user}" -d "${prep_db}" -c "${prep_sql}"; then
        fail_or_warn "Phase 6: preprocessing-db query failed (display-only; not gating)"
    fi

    log "postprocessing-db summary (database=${post_db}):"
    post_sql=$'SELECT \'forecasts.\'||model_type::text AS family, COUNT(*) AS rows FROM forecasts GROUP BY model_type \
UNION ALL SELECT \'long_forecasts.\'||horizon_type::text, COUNT(*) FROM long_forecasts GROUP BY horizon_type \
UNION ALL SELECT \'lr_forecasts.\'||horizon_type::text, COUNT(*) FROM lr_forecasts GROUP BY horizon_type \
ORDER BY family;'
    if ! docker exec sapphire-postprocessing-db psql -U "${post_user}" -d "${post_db}" -c "${post_sql}"; then
        fail_or_warn "Phase 6: postprocessing-db query failed (display-only; not gating)"
    fi

    ok "Phase 6 (verification) completed"
}

# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

main() {
    run_phase_1_preflight
    run_phase_2_csv
    run_phase_3_pipeline
    run_phase_4a_lr_hindcast
    run_phase_4b_ml_hindcast
    run_phase_4c_lt_hindcast
    run_phase_5_hooks
    run_phase_6_verify

    printf '\n'
    if [[ "${ANY_PHASE_FAILED}" == "true" ]]; then
        printf '%s [%sNOTE%s] one or more phases failed (--continue-on-error was set); exiting non-zero\n' \
            "$(_ts)" "${C_RED}" "${C_RESET}" >&2
        exit 1
    fi
    log "All phases completed successfully"
    log "Log file: ${LOG_FILE}"
    exit 0
}

main
