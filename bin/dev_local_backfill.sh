#!/usr/bin/env bash
# =============================================================================
# bin/dev_local_backfill.sh
#
# Populate a LOCAL sapphire stack with an organization's operational data, for
# DEV-MACHINE use. Orchestrates the proven §5 CSV→DB migration wrappers, and
# offers opt-in phases for the operational pipeline + historical hindcasts +
# regenerate hooks.
#
# Default flow (Phases 1, 2, 6 only):
#   preflight → CSV→DB migration → verify
#
# Opt-in flows (enable per-phase via flags):
#   --run-pipeline      Phase 3: today's operational pipeline run (current-date only)
#   --run-hindcasts     Phase 4: historical LR + ML + PENTAD/DECADE hindcasts (HOURS; scaffold)
#   --run-hooks         Phase 5: regenerate hooks (snow stats, hydrograph long-horizon, skill metrics)
#                                 — TOUCHES THE USER'S CRONTAB; degrades pause failures to warnings
#
# Prerequisites the CALLER must satisfy before running:
#   - Local sapphire stack is up: `cd sapphire && docker compose up -d`
#   - Stack health green: `curl -sf http://localhost:8000/health/ready`
#   - Alembic at head on both service DBs (the script enforces this).
#   - Organization data root locally available with intermediate_data/ populated.
#   - On Apple Silicon: `export DOCKER_DEFAULT_PLATFORM=linux/amd64`.
#   - iEH HF SSH tunnel up IF --run-hooks (the sync_long_horizon step needs the SDK).
#
# Failure handling:
#   By default any phase failure is FATAL (exit 1). Use --continue-on-error to
#   degrade to warn-and-continue (useful when iterating).
#
# Usage:
#   bash bin/dev_local_backfill.sh <env_file>                         # phases 1, 2, 6
#   bash bin/dev_local_backfill.sh <env_file> --dry-run               # all CSV wrappers in dry-run mode
#   bash bin/dev_local_backfill.sh <env_file> --run-pipeline          # add today's pipeline
#   bash bin/dev_local_backfill.sh <env_file> --run-hooks --start-year 2010  # forward start-year
#
# Step-through Phase 5 hooks directly when debugging:
#   # Snow stats/norms only
#   bash bin/initialize_regenerate_hooks.sh "$ENV_FILE" --start-year 2010 \
#     --skip-hook-hydrograph-month-season --skip-hook-short-term-skill \
#     --skip-hook-long-term-skill --allow-unpaused-cron
#
#   # Hydrograph MONTH/SEASON/QUARTER only. Tajik HM has no iEH HF monthly
#   # norms, so skip this hook for tjhm; test it with Kyrgyz HM data instead.
#   bash bin/initialize_regenerate_hooks.sh "$KYRG_ENV" --start-year 2010 \
#     --skip-hook-snow-stats --skip-hook-short-term-skill \
#     --skip-hook-long-term-skill --allow-unpaused-cron
#
#   # Skill metrics only
#   bash bin/initialize_regenerate_hooks.sh "$ENV_FILE" \
#     --skip-hook-snow-stats --skip-hook-hydrograph-month-season \
#     --allow-unpaused-cron
#
# =============================================================================

set -euo pipefail

# -----------------------------------------------------------------------------
# Defaults + arg parsing
# -----------------------------------------------------------------------------

IMAGE="mabesa/sapphire-prepgateway:2026-06"
DRY_RUN=false
CONTINUE_ON_ERROR=false
SKIP_PREFLIGHT=false
SKIP_CSV=false
RUN_PIPELINE=false      # Phase 3 opt-in
RUN_HINDCASTS=false     # Phase 4 opt-in
RUN_HOOKS=false         # Phase 5 opt-in (TOUCHES CRONTAB)
SKIP_VERIFY=false
HINDCAST_START_YEAR=2010
ENV_FILE=""

# Colors (silent under non-TTY)
if [[ -t 1 ]]; then
    BOLD='\033[1m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; BLUE='\033[0;34m'; NC='\033[0m'
else
    BOLD=''; GREEN=''; YELLOW=''; RED=''; BLUE=''; NC=''
fi

usage() {
    cat <<EOF
Usage: bash bin/dev_local_backfill.sh <env_file> [OPTIONS]

Populate a local sapphire stack with an org's operational data.

By default runs phases 1 (preflight), 2 (CSV→DB migration), 6 (verify).
Pipeline, hindcasts, and hooks are OPT-IN (off by default).

Arguments:
  env_file                  Path to org's .env file (e.g. ~/Documents/GitHub/
                            taj_data_forecast_tools/config/.env_develop_tjhm).

Options:
  --image TAG               prepgateway image tag (default: ${IMAGE})
  --dry-run                 Pass --dry-run to each underlying wrapper. NOTE:
                            wrappers still pull images, read DB target state,
                            and write log files; no INSERT/UPDATE to DBs.
  --continue-on-error       Phase failures degrade to warnings instead of fatal.
  --skip-preflight          Skip Phase 1 prereq checks.
  --skip-csv                Skip Phase 2 §5 CSV→DB migration.
  --run-pipeline            Phase 3: run today's operational pipeline (current
                            date only — does NOT backfill historical PENTAD/
                            DECADE; for historical, use --run-hindcasts).
  --run-hindcasts           Phase 4: historical LR + ML + PENTAD/DECADE
                            hindcast (HOURS). Currently a SCAFFOLD — points
                            you at the per-module entry points to run by hand.
  --run-hooks               Phase 5: regenerate hooks (snow stats, hydrograph
                            month/season/quarter, skill metrics).
                            WARNING: TOUCHES YOUR CRONTAB. The orchestrator
                            pauses cron at start and restores on exit. We pass
                            --allow-unpaused-cron so pause failures degrade
                            to warnings on dev hosts without crontab.
  --skip-verify             Skip Phase 6 row-count verification.
  --start-year YYYY         Hindcast / hook start year (default: ${HINDCAST_START_YEAR}).
                            Forwarded to Phase 4 hindcasts AND Phase 5 hooks
                            (specifically the hydrograph month/season per-year loop).
  -h, --help                Print this and exit.

Examples:
  # Standard run (CSV→DB only; ~20-30 min):
  bash bin/dev_local_backfill.sh ~/Documents/GitHub/taj_data_forecast_tools/config/.env_develop_tjhm

  # Dry-run inventory of CSV phase:
  bash bin/dev_local_backfill.sh ~/.../.env_develop_tjhm --dry-run

  # Full: CSV + today's pipeline + hooks (does NOT include historical hindcasts):
  bash bin/dev_local_backfill.sh ~/.../.env_develop_tjhm --run-pipeline --run-hooks --start-year 2010

Step-through Phase 5 hooks:
  # Snow stats/norms only:
  bash bin/initialize_regenerate_hooks.sh "\$ENV_FILE" --start-year 2010 \\
    --skip-hook-hydrograph-month-season \\
    --skip-hook-short-term-skill \\
    --skip-hook-long-term-skill \\
    --allow-unpaused-cron

  # Hydrograph MONTH/SEASON/QUARTER only:
  # Tajik HM has no iEH HF monthly norms, so skip this for tjhm.
  # Use Kyrgyz HM data for this hook; station 16059 is a known check station.
  bash bin/initialize_regenerate_hooks.sh "\$KYRG_ENV" --start-year 2010 \\
    --skip-hook-snow-stats \\
    --skip-hook-short-term-skill \\
    --skip-hook-long-term-skill \\
    --allow-unpaused-cron

  # Skill metrics only:
  bash bin/initialize_regenerate_hooks.sh "\$ENV_FILE" \\
    --skip-hook-snow-stats \\
    --skip-hook-hydrograph-month-season \\
    --allow-unpaused-cron

  # Verify Kyrgyz hydrograph test station after the hydrograph hook:
  docker exec sapphire-preprocessing-db psql -U postgres -d preprocessing_db -P pager=off -c "
  SELECT horizon_type, code, COUNT(*) AS rows, MIN(date), MAX(date),
         COUNT(norm) AS norm_rows, COUNT(previous) AS previous_rows,
         COUNT(current) AS current_rows
  FROM hydrographs
  WHERE code = '16059'
    AND horizon_type IN ('MONTH', 'SEASON', 'QUARTER')
  GROUP BY horizon_type, code
  ORDER BY horizon_type, code;"

EOF
}

# Helper: consume a flag that takes a value, validating $2 exists
require_value() {
    local flag="$1"
    local val="${2:-}"
    if [[ -z "$val" || "$val" == --* ]]; then
        echo -e "${RED}ERROR: $flag requires a value.${NC}" >&2
        usage
        exit 2
    fi
    printf '%s' "$val"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --image)
            IMAGE="$(require_value "--image" "${2:-}")"; shift 2 ;;
        --dry-run)
            DRY_RUN=true; shift ;;
        --continue-on-error)
            CONTINUE_ON_ERROR=true; shift ;;
        --skip-preflight)
            SKIP_PREFLIGHT=true; shift ;;
        --skip-csv)
            SKIP_CSV=true; shift ;;
        --run-pipeline)
            RUN_PIPELINE=true; shift ;;
        --run-hindcasts)
            RUN_HINDCASTS=true; shift ;;
        --run-hooks)
            RUN_HOOKS=true; shift ;;
        --skip-verify)
            SKIP_VERIFY=true; shift ;;
        --start-year)
            HINDCAST_START_YEAR="$(require_value "--start-year" "${2:-}")"; shift 2 ;;
        -h|--help)
            usage; exit 0 ;;
        --)
            shift; break ;;
        *)
            if [[ -z "$ENV_FILE" ]]; then
                ENV_FILE="$1"; shift
            else
                echo -e "${RED}Unknown argument: $1${NC}" >&2; usage; exit 2
            fi
            ;;
    esac
done

if [[ -z "$ENV_FILE" ]]; then
    echo -e "${RED}ERROR: env_file argument is required.${NC}" >&2
    usage
    exit 2
fi

case "$ENV_FILE" in
    \~)
        ENV_FILE="$HOME"
        ;;
    \~/*)
        ENV_FILE="$HOME/${ENV_FILE#\~/}"
        ;;
esac

if [[ ! -f "$ENV_FILE" ]]; then
    echo -e "${RED}ERROR: env file not found: $ENV_FILE${NC}" >&2
    exit 2
fi

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG_DIR="${REPO_ROOT}/logs/dev_local_backfill"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/backfill_${TIMESTAMP}_$$.log"
if bash -c ': > >(cat >/dev/null)' 2>/dev/null; then
    exec > >(tee -a "$LOG_FILE") 2>&1
else
    printf '[%s] WARN: console tee unavailable; writing output only to %s\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$LOG_FILE" >&2
    exec >> "$LOG_FILE" 2>&1
    printf '[%s] WARN: console tee unavailable; writing output only to %s\n' \
        "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$LOG_FILE"
fi

log()   { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
phase() { printf '\n%b[%s] === %s ===%b\n' "$BOLD$BLUE" "$(date -u +%H:%M:%SZ)" "$*" "$NC"; }
ok()    { printf '%b[%s] OK: %s%b\n' "$GREEN" "$(date -u +%H:%M:%SZ)" "$*" "$NC"; }
warn()  { printf '%b[%s] WARN: %s%b\n' "$YELLOW" "$(date -u +%H:%M:%SZ)" "$*" "$NC"; }
fatal() { printf '%b[%s] FATAL: %s%b\n' "$RED" "$(date -u +%H:%M:%SZ)" "$*" "$NC"; exit 1; }

# fail_or_warn — fatal by default; warn-and-continue if --continue-on-error is set
fail_or_warn() {
    if [[ "$CONTINUE_ON_ERROR" == true ]]; then
        warn "$*"
    else
        fatal "$* (re-run with --continue-on-error to degrade to warning)"
    fi
}

log "=============================================================="
log "dev_local_backfill.sh"
log "env_file        : $ENV_FILE"
log "image           : $IMAGE"
log "dry_run         : $DRY_RUN"
log "continue_on_err : $CONTINUE_ON_ERROR"
log "phases enabled  : preflight=$([ $SKIP_PREFLIGHT = false ] && echo yes || echo no) csv=$([ $SKIP_CSV = false ] && echo yes || echo no) pipeline=$RUN_PIPELINE hindcasts=$RUN_HINDCASTS hooks=$RUN_HOOKS verify=$([ $SKIP_VERIFY = false ] && echo yes || echo no)"
log "start_year      : $HINDCAST_START_YEAR (forwarded to hindcasts + hooks)"
log "log_file        : $LOG_FILE"
log "=============================================================="

cd "$REPO_ROOT"

# -----------------------------------------------------------------------------
# Phase 1: Preflight (enforces alembic at head — does not just warn)
# -----------------------------------------------------------------------------

if [[ "$SKIP_PREFLIGHT" != true ]]; then
    phase "Phase 1 — Preflight"

    docker info > /dev/null 2>&1 || fatal "docker daemon not reachable"

    if ! curl -sf http://localhost:8000/health/ready > /dev/null; then
        fatal "api-gateway not ready at http://localhost:8000/health/ready. Bring stack up first:
  cd sapphire && docker compose up -d
  until curl -fsS http://localhost:8000/health/ready >/dev/null; do sleep 2; done"
    fi
    ok "api-gateway ready"

    # Alembic head — HARD ENFORCE on both DBs
    for svc in preprocessing postprocessing; do
        current=$(docker exec "sapphire-${svc}-api" alembic current 2>/dev/null \
                  | awk 'NF && $1 !~ /^INFO$/ {print $1; exit}' || true)
        head=$(docker exec "sapphire-${svc}-api" alembic heads 2>/dev/null \
               | awk 'NF {print $1; exit}' || true)
        if [[ -z "$current" || -z "$head" ]]; then
            fatal "$svc alembic revision unreadable. Stack may not be healthy:
  docker exec sapphire-${svc}-api alembic current
  docker exec sapphire-${svc}-api alembic heads"
        fi
        if [[ "$current" != "$head" ]]; then
            fatal "$svc alembic is not at head (current=$current, head=$head). Apply migrations, then retry:
  docker exec sapphire-${svc}-api alembic upgrade head"
        fi
        log "$svc alembic head: $head"
    done

    # arch check on Apple Silicon
    ARCH=$(uname -m)
    if [[ "$ARCH" =~ ^(arm64|aarch64)$ ]] && [[ -z "${DOCKER_DEFAULT_PLATFORM:-}" ]]; then
        warn "arm64 host without DOCKER_DEFAULT_PLATFORM=linux/amd64 — image pulls may fail"
        warn "  export DOCKER_DEFAULT_PLATFORM=linux/amd64 in your shell, then retry"
    fi

    # Pre-snapshot row counts so verify can compare
    PREP_USER=$(docker exec sapphire-preprocessing-db printenv POSTGRES_USER 2>/dev/null || true)
    PREP_DB=$(docker exec sapphire-preprocessing-db printenv POSTGRES_DB 2>/dev/null || echo preprocessing_db)
    if [[ -n "$PREP_USER" ]]; then
        log "Pre-backfill row counts (preprocessing-db=$PREP_DB):"
        docker exec sapphire-preprocessing-db psql -U "$PREP_USER" -d "$PREP_DB" -tAc \
            "SELECT 'runoffs:'||COUNT(*) FROM runoffs
             UNION ALL SELECT 'hydrographs:'||COUNT(*) FROM hydrographs
             UNION ALL SELECT 'meteo:'||COUNT(*) FROM meteo
             UNION ALL SELECT 'snow:'||COUNT(*) FROM snow" 2>/dev/null | sed 's/^/  /'
    fi

    ok "Preflight passed"
fi

# -----------------------------------------------------------------------------
# Phase 2: CSV → DB migration (§5 wrappers)
# -----------------------------------------------------------------------------

if [[ "$SKIP_CSV" != true ]]; then
    phase "Phase 2 — CSV → DB migration (§5 wrappers, ~20-30 min)"

    WRAPPER_FLAGS=( --image "$IMAGE" )
    [[ "$DRY_RUN" == true ]] && WRAPPER_FLAGS+=( --dry-run )

    WRAPPERS=(
        bin/initialize_runoff_day_history.sh
        bin/initialize_meteo_history.sh
        bin/initialize_snow_history.sh
        bin/initialize_hydrograph_day_history.sh
        bin/initialize_long_forecast_history.sh
    )

    for w in "${WRAPPERS[@]}"; do
        if [[ ! -f "$w" ]]; then
            fail_or_warn "$w not found"
            continue
        fi
        log "running: $w"
        if ! bash "$w" "$ENV_FILE" "${WRAPPER_FLAGS[@]}"; then
            fail_or_warn "$w exited non-zero"
        fi
    done

    ok "Phase 2 complete"
fi

# -----------------------------------------------------------------------------
# Phase 3 — Today's operational pipeline (OPT-IN; current date only, NOT historical)
# -----------------------------------------------------------------------------

if [[ "$RUN_PIPELINE" == true ]]; then
    phase "Phase 3 — Today's operational pipeline run (current date only)"

    warn "Scope: this phase runs the operational pipeline for TODAY only."
    warn "  It does NOT backfill historical PENTAD/DECADE coverage."
    warn "  For historical PENTAD/DECADE/LR/ML, use --run-hindcasts (HOURS, scaffold)."

    if [[ "$DRY_RUN" == true ]]; then
        log "DRY RUN: would invoke bin/run_preprocessing_gateway.sh + bin/run_daily_maintenance.sh"
    else
        if [[ -f bin/run_preprocessing_gateway.sh ]]; then
            if ! bash bin/run_preprocessing_gateway.sh "$ENV_FILE"; then
                fail_or_warn "run_preprocessing_gateway.sh exited non-zero"
            fi
        else
            fail_or_warn "bin/run_preprocessing_gateway.sh not found"
        fi

        if [[ -f bin/run_daily_maintenance.sh ]]; then
            if ! bash bin/run_daily_maintenance.sh "$ENV_FILE"; then
                fail_or_warn "run_daily_maintenance.sh exited non-zero"
            fi
        else
            fail_or_warn "bin/run_daily_maintenance.sh not found"
        fi
    fi

    ok "Phase 3 complete"
fi

# -----------------------------------------------------------------------------
# Phase 4 — Historical hindcasts (OPT-IN; SCAFFOLD)
# -----------------------------------------------------------------------------

if [[ "$RUN_HINDCASTS" == true ]]; then
    phase "Phase 4 — Historical hindcasts (LR + ML + PENTAD/DECADE; start_year=${HINDCAST_START_YEAR})"

    warn "Phase 4 is a SCAFFOLD ONLY — the per-module hindcast entry points are not"
    warn "  yet validated by this script. Until that's done, run them manually:"
    warn "    - PENTAD/DECADE aggregations: iterate bin/run_pentadal_forecasts.sh + bin/run_decadal_forecasts.sh over historical dates"
    warn "    - LR pentadal/decadal forecast hindcasts: see apps/linear_regression/scr/*hindcast*"
    warn "    - ML forecast hindcasts: see apps/machine_learning/scr/*hindcast* or apps/machine_learning/dev_code/"
    warn "    - Long-term forecast simulations: apps/long_term_forecasting/dev_code/simulate_forecasts.py"
    warn ""
    warn "After running them manually, re-invoke this script with:"
    warn "  bash bin/dev_local_backfill.sh \"$ENV_FILE\" --skip-csv --run-hooks --start-year $HINDCAST_START_YEAR"

    fail_or_warn "Phase 4 hindcast implementation pending — see warnings above for manual steps"
fi

# -----------------------------------------------------------------------------
# Phase 5 — Regenerate hooks (OPT-IN; TOUCHES CRONTAB)
# -----------------------------------------------------------------------------

if [[ "$RUN_HOOKS" == true ]]; then
    phase "Phase 5 — Regenerate hooks (snow stats, hydrograph month/season/quarter, skill metrics)"

    warn "This phase TOUCHES YOUR CRONTAB:"
    warn "  - The orchestrator pauses cron at start, restores on exit."
    warn "  - We pass --allow-unpaused-cron so pause failures degrade to warnings"
    warn "    (safe on dev hosts with no crontab)."
    warn "  - If your crontab is touched and a restore fails, the orchestrator logs"
    warn "    instructions to recover from a backup file."

    HOOK_FLAGS=( --allow-unpaused-cron --start-year "$HINDCAST_START_YEAR" )
    [[ "$DRY_RUN" == true ]] && HOOK_FLAGS+=( --dry-run )

    if [[ -f bin/initialize_regenerate_hooks.sh ]]; then
        if ! bash bin/initialize_regenerate_hooks.sh "$ENV_FILE" "${HOOK_FLAGS[@]}"; then
            fail_or_warn "initialize_regenerate_hooks.sh exited non-zero"
        fi
    else
        fail_or_warn "bin/initialize_regenerate_hooks.sh not found"
    fi

    ok "Phase 5 complete"
fi

# -----------------------------------------------------------------------------
# Phase 6 — Verification (row counts; DB names sourced from containers)
# -----------------------------------------------------------------------------

if [[ "$SKIP_VERIFY" != true ]]; then
    phase "Phase 6 — Verification"

    PREP_USER=$(docker exec sapphire-preprocessing-db printenv POSTGRES_USER 2>/dev/null) || \
        fail_or_warn "could not read POSTGRES_USER from sapphire-preprocessing-db"
    PREP_DB=$(docker exec sapphire-preprocessing-db printenv POSTGRES_DB 2>/dev/null) || \
        fail_or_warn "could not read POSTGRES_DB from sapphire-preprocessing-db"
    POST_USER=$(docker exec sapphire-postprocessing-db printenv POSTGRES_USER 2>/dev/null) || \
        fail_or_warn "could not read POSTGRES_USER from sapphire-postprocessing-db"
    POST_DB=$(docker exec sapphire-postprocessing-db printenv POSTGRES_DB 2>/dev/null) || \
        fail_or_warn "could not read POSTGRES_DB from sapphire-postprocessing-db"

    log "preprocessing-db ($PREP_DB) row counts by family:"
    docker exec sapphire-preprocessing-db psql -U "$PREP_USER" -d "$PREP_DB" -P pager=off -c "
        SELECT 'runoffs.'||horizon_type::text AS family, COUNT(*) AS rows, MIN(date) AS min_date, MAX(date) AS max_date
          FROM runoffs GROUP BY horizon_type
        UNION ALL
        SELECT 'hydrographs.'||horizon_type::text, COUNT(*), MIN(date), MAX(date)
          FROM hydrographs GROUP BY horizon_type
        UNION ALL
        SELECT 'meteo.'||meteo_type::text, COUNT(*), MIN(date), MAX(date)
          FROM meteo GROUP BY meteo_type
        UNION ALL
        SELECT 'snow.'||snow_type::text, COUNT(*), MIN(date), MAX(date)
          FROM snow GROUP BY snow_type
        ORDER BY family;
    " 2>&1 | sed 's/^/  /' || fail_or_warn "preprocessing-db verify SQL failed"

    log "postprocessing-db ($POST_DB) row counts by family:"
    docker exec sapphire-postprocessing-db psql -U "$POST_USER" -d "$POST_DB" -P pager=off -c "
        SELECT 'forecasts.'||model_type::text AS family, COUNT(*) AS rows
          FROM forecasts GROUP BY model_type
        UNION ALL
        SELECT 'long_forecasts.'||horizon_type::text, COUNT(*) FROM long_forecasts GROUP BY horizon_type
        UNION ALL
        SELECT 'lr_forecasts.'||horizon_type::text, COUNT(*) FROM lr_forecasts GROUP BY horizon_type
        ORDER BY family;
    " 2>&1 | sed 's/^/  /' || fail_or_warn "postprocessing-db verify SQL failed"

    ok "Verification complete"
fi

log "=============================================================="
log "Backfill finished. Log: $LOG_FILE"
log "=============================================================="
