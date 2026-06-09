#!/usr/bin/env bash

# =============================================================================
# SAPPHIRE Database Backup
# =============================================================================
#
# Dumps the four active SAPPHIRE Postgres databases (preprocessing,
# postprocessing, user, auth) using pg_dump's custom format (suitable for
# pg_restore). Each dump is written to $BACKUP_DIR as a timestamped .dump
# file, verified with pg_restore --list, and old dumps past the retention
# window are pruned.
#
# No log file is written — stdout/stderr only. Cron is expected to capture
# the output.
#
# Usage:
#   bash bin/backup_sapphire_db.sh
#   bash bin/backup_sapphire_db.sh -d /mnt/backups/sapphire
#   bash bin/backup_sapphire_db.sh --retention-days 60
#   bash bin/backup_sapphire_db.sh --dry-run
#   bash bin/backup_sapphire_db.sh --env-file /data/<data>/config/.env_develop_kghm
#
# Prerequisites:
#   - Docker daemon running with SAPPHIRE DB containers up
#   - sapphire/.env populated with POSTGRES_USER, POSTGRES_PASSWORD and the
#     four DB names (PREPROCESSING_DB, POSTPROCESSING_DB, USER_DB, AUTH_DB),
#     or pass a custom path via --env-file to override the default sapphire/.env
#   - Run from repository root (parent of sapphire/)
#   - $BACKUP_DIR must exist and be writable by the invoking user
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Colors (matching bin/reset_sapphire_db.sh)
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
COMPOSE_DIR="sapphire"
ENV_FILE="${COMPOSE_DIR}/.env"
COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.yml"

# Flags / defaults
BACKUP_DIR="/var/backups/sapphire"
RETENTION_DAYS=30
DRY_RUN=false

# Result tracking
FAILED_DBS=()
SUCCEEDED_DBS=()

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

log() {
    local level="$1"
    shift
    local msg="$*"
    local ts
    ts="$(date '+%Y-%m-%d %H:%M:%S')"

    case "$level" in
        INFO)  echo -e "${BLUE}[${ts}] ${msg}${NC}" ;;
        OK)    echo -e "${GREEN}[${ts}] ${msg}${NC}" ;;
        WARN)  echo -e "${YELLOW}[${ts}] ${msg}${NC}" >&2 ;;
        ERROR) echo -e "${RED}[${ts}] ${msg}${NC}" >&2 ;;
        *)     echo "[${ts}] ${msg}" ;;
    esac
}

banner() {
    local msg="$1"
    echo ""
    echo -e "${BOLD}========================================${NC}"
    echo -e "${BOLD} ${msg}${NC}"
    echo -e "${BOLD}========================================${NC}"
}

file_size_human() {
    local f="$1"
    if [ ! -f "$f" ]; then
        echo "0B"
        return
    fi
    # BSD/macOS `stat -f %z`; GNU/Linux `stat -c %s`
    local bytes
    if bytes=$(stat -c %s "$f" 2>/dev/null); then
        :
    else
        bytes=$(stat -f %z "$f" 2>/dev/null || echo 0)
    fi
    # Render in a portable way using awk
    awk -v b="$bytes" 'BEGIN {
        split("B KB MB GB TB", u, " ");
        i = 1;
        while (b >= 1024 && i < 5) { b = b / 1024; i++; }
        if (i == 1) printf "%dB", b; else printf "%.1f%s", b, u[i];
    }'
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/backup_sapphire_db.sh [FLAGS]

Dump the four SAPPHIRE Postgres databases to timestamped .dump files.

Flags:
  -d, --backup-dir PATH      Directory to write dumps to
                             (default: /var/backups/sapphire)
  -e, --env-file PATH        Path to the env file with POSTGRES_USER,
                             POSTGRES_PASSWORD and the four DB names
                             (default: sapphire/.env)
  -r, --retention-days N     Delete .dump files older than N days
                             (default: 30; pass 0 to keep all)
      --dry-run              Log actions without running pg_dump or deleting
  -h, --help                 Show this help message

Exit status:
  0  All four dumps succeeded and verified
  1  One or more dumps failed, env/config missing, or backup dir invalid

Examples:
  bash bin/backup_sapphire_db.sh
  bash bin/backup_sapphire_db.sh -d /mnt/backups/sapphire -r 60
  bash bin/backup_sapphire_db.sh --dry-run
  bash bin/backup_sapphire_db.sh --env-file /data/<data>/config/.env_develop_kghm
USAGE
}

# ---------------------------------------------------------------------------
# Precondition checks
# ---------------------------------------------------------------------------

check_repo_root() {
    if [ ! -f "${COMPOSE_FILE}" ]; then
        log ERROR "Must run from the repository root (parent of sapphire/)."
        log ERROR "  Expected: ${COMPOSE_FILE}"
        exit 1
    fi
}

load_env() {
    if [ ! -f "${ENV_FILE}" ]; then
        log ERROR "Env file not found: ${ENV_FILE}"
        log ERROR "  Copy sapphire/.env.example to sapphire/.env and fill in values."
        exit 1
    fi

    # Load required variables without echoing them. We parse only the keys we
    # need rather than `source`-ing the file, to avoid executing arbitrary
    # content and to keep POSTGRES_PASSWORD out of error paths.
    local required=(POSTGRES_USER POSTGRES_PASSWORD PREPROCESSING_DB POSTPROCESSING_DB USER_DB AUTH_DB)
    local var val
    for var in "${required[@]}"; do
        # Match `VAR=value`, strip optional surrounding quotes on value.
        val=$(grep -E "^[[:space:]]*${var}=" "${ENV_FILE}" | tail -n 1 | sed -E "s/^[[:space:]]*${var}=//; s/^\"(.*)\"$/\1/; s/^'(.*)'$/\1/")
        if [ -z "${val:-}" ]; then
            log ERROR "Required variable ${var} is unset or empty in ${ENV_FILE}."
            exit 1
        fi
        # Export without logging the value
        export "${var}=${val}"
    done
}

check_docker() {
    if ! docker info >/dev/null 2>&1; then
        log ERROR "Docker daemon is not running or not accessible to this user."
        exit 1
    fi
}

check_backup_dir() {
    if [ ! -d "${BACKUP_DIR}" ]; then
        log ERROR "Backup directory does not exist: ${BACKUP_DIR}"
        log ERROR "  Create it first, e.g.: sudo mkdir -p '${BACKUP_DIR}' && sudo chown \"\$USER\" '${BACKUP_DIR}'"
        exit 1
    fi
    if [ ! -w "${BACKUP_DIR}" ]; then
        log ERROR "Backup directory is not writable: ${BACKUP_DIR}"
        exit 1
    fi
}

check_container_running() {
    local container="$1"
    if ! docker ps --format '{{.Names}}' | grep -qx "${container}"; then
        log ERROR "Container '${container}' is not running. Start SAPPHIRE services first."
        return 1
    fi
    return 0
}

# ---------------------------------------------------------------------------
# Backup a single database
# ---------------------------------------------------------------------------

backup_db() {
    local container="$1"
    local db_name="$2"
    local timestamp
    timestamp="$(date '+%Y-%m-%d_%H%M%S')"
    local out_file="${BACKUP_DIR}/${db_name}_${timestamp}.dump"

    log INFO "Backup start: db=${db_name} container=${container} file=${out_file}"

    if [ "${DRY_RUN}" = true ]; then
        log INFO "  [dry-run] Would run: docker exec -e PGPASSWORD=*** ${container} pg_dump -U \"\$POSTGRES_USER\" -d ${db_name} --format=custom --compress=6"
        log INFO "  [dry-run] Would verify with: pg_restore --list ${out_file}"
        SUCCEEDED_DBS+=("${db_name}")
        return 0
    fi

    if ! check_container_running "${container}"; then
        FAILED_DBS+=("${db_name}")
        return 1
    fi

    # Run pg_dump inside the container. PGPASSWORD is passed via -e so it is
    # not visible in `ps` output on the host. Output is streamed to the dump
    # file on the host via stdout redirection.
    if docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" "${container}" \
        pg_dump -U "${POSTGRES_USER}" -d "${db_name}" --format=custom --compress=6 \
        > "${out_file}" 2> >(while IFS= read -r line; do log WARN "  [${db_name}] ${line}"; done); then
        :
    else
        log ERROR "pg_dump failed for ${db_name}. Renaming artifact to .FAILED."
        mv -f "${out_file}" "${out_file}.FAILED" 2>/dev/null || true
        FAILED_DBS+=("${db_name}")
        return 1
    fi

    # Verify: non-empty file
    if [ ! -s "${out_file}" ]; then
        log ERROR "Dump file is empty for ${db_name}. Renaming to .FAILED."
        mv -f "${out_file}" "${out_file}.FAILED" 2>/dev/null || true
        FAILED_DBS+=("${db_name}")
        return 1
    fi

    # Verify: pg_restore --list should succeed on a valid custom-format archive.
    # We copy the file into the container (tmpfs /tmp) to avoid needing
    # pg_restore on the host.
    if ! docker exec -i "${container}" bash -lc "cat > /tmp/verify.dump && pg_restore --list /tmp/verify.dump >/dev/null && rm -f /tmp/verify.dump" < "${out_file}" 2>/dev/null; then
        log ERROR "pg_restore --list verification failed for ${db_name}. Renaming to .FAILED."
        mv -f "${out_file}" "${out_file}.FAILED" 2>/dev/null || true
        FAILED_DBS+=("${db_name}")
        return 1
    fi

    local size
    size="$(file_size_human "${out_file}")"
    log OK "Backup done: db=${db_name} file=${out_file} size=${size}"
    SUCCEEDED_DBS+=("${db_name}")
    return 0
}

# ---------------------------------------------------------------------------
# Retention
# ---------------------------------------------------------------------------

prune_old_backups() {
    if [ "${RETENTION_DAYS}" -le 0 ]; then
        log INFO "Retention disabled (--retention-days 0). Keeping all dumps."
        return 0
    fi

    log INFO "Pruning .dump files older than ${RETENTION_DAYS} days in ${BACKUP_DIR} (keeps .FAILED files)."

    if [ "${DRY_RUN}" = true ]; then
        local would_delete=0
        while IFS= read -r -d '' f; do
            log INFO "  [dry-run] Would delete: ${f}"
            would_delete=$((would_delete + 1))
        done < <(find "${BACKUP_DIR}" -maxdepth 1 -type f -name '*.dump' -mtime "+${RETENTION_DAYS}" -print0 2>/dev/null)
        log INFO "  [dry-run] Candidates: ${would_delete}"
        return 0
    fi

    local deleted=0
    while IFS= read -r -d '' f; do
        if rm -f "${f}"; then
            log INFO "  Deleted: ${f}"
            deleted=$((deleted + 1))
        else
            log WARN "  Failed to delete: ${f}"
        fi
    done < <(find "${BACKUP_DIR}" -maxdepth 1 -type f -name '*.dump' -mtime "+${RETENTION_DAYS}" -print0 2>/dev/null)

    log OK "Pruned ${deleted} old dump(s)."
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    # Parse arguments
    while [ $# -gt 0 ]; do
        case "$1" in
            -d|--backup-dir)
                if [ $# -lt 2 ]; then
                    log ERROR "Flag $1 requires a value."
                    exit 1
                fi
                BACKUP_DIR="$2"
                shift 2
                ;;
            -r|--retention-days)
                if [ $# -lt 2 ]; then
                    log ERROR "Flag $1 requires a value."
                    exit 1
                fi
                if ! [[ "$2" =~ ^[0-9]+$ ]]; then
                    log ERROR "--retention-days must be a non-negative integer, got: $2"
                    exit 1
                fi
                RETENTION_DAYS="$2"
                shift 2
                ;;
            -e|--env-file)
                if [ $# -lt 2 ]; then
                    log ERROR "Flag $1 requires a value."
                    exit 1
                fi
                ENV_FILE="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            *)
                log ERROR "Unknown flag: $1"
                print_usage
                exit 1
                ;;
        esac
    done

    banner "SAPPHIRE Database Backup"

    if [ "${DRY_RUN}" = true ]; then
        log WARN "Dry-run mode: no pg_dump calls, no deletions."
    fi

    check_repo_root
    load_env
    check_backup_dir
    if [ "${DRY_RUN}" = false ]; then
        check_docker
    fi

    log INFO "Backup directory: ${BACKUP_DIR}"
    log INFO "Retention days:   ${RETENTION_DAYS}"

    # Four active DBs — container/db pairs
    backup_db "sapphire-preprocessing-db"  "${PREPROCESSING_DB}"  || true
    backup_db "sapphire-postprocessing-db" "${POSTPROCESSING_DB}" || true
    backup_db "sapphire-user-db"           "${USER_DB}"           || true
    backup_db "sapphire-auth-db"           "${AUTH_DB}"           || true

    # Retention runs regardless of per-DB outcome; .FAILED files are never pruned.
    prune_old_backups

    banner "BACKUP SUMMARY"
    log INFO "Succeeded: ${#SUCCEEDED_DBS[@]} (${SUCCEEDED_DBS[*]:-none})"
    if [ "${#FAILED_DBS[@]}" -gt 0 ]; then
        log ERROR "Failed:    ${#FAILED_DBS[@]} (${FAILED_DBS[*]})"
        exit 1
    fi
    log OK "All four dumps succeeded and verified."
    exit 0
}

main "$@"
