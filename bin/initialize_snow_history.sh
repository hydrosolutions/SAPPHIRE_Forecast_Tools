#!/usr/bin/env bash
# ============================================================================
# initialize_snow_history.sh
#
# Workaround for two known bugs that combine to drop ~96% of snow.value during
# P1 of the historical backfill runbook:
#
#   1. apps/preprocessing_gateway/dg_utils.py write_snow_to_api sends
#      value=None for most records despite the CSV having real values (the
#      mechanism is unclear; only the ~365-day operational Data Gateway window
#      reaches the API intact).
#
#   2. sapphire/services/preprocessing/app/crud.py _has_changes + setattr
#      overwrites existing non-NULL values with incoming NULL when any other
#      field differs.
#
# This script bypasses both bugs by reading the snow CSVs directly, filtering
# to rows with a parseable real value, and POSTing minimal payloads
# (snow_type, code, date, value) to the preprocessing API.  Sending only
# non-NULL values sidesteps the overwrite bug.  The operation is idempotent on
# (snow_type, code, date) thanks to the service-side upsert.
#
# Verified on TAJ 2026-06-04: snow.value_rows went from ~5k to ~157k per
# snow_type across 17 stations.
#
# Usage:
#   bash bin/initialize_snow_history.sh <env_file_path> [OPTIONS]
#
# Arguments:
#   env_file_path   Path to the .env_<org> file (required, no default).
#
# Options:
#   --dry-run            Discover CSVs and count rows; do NOT POST anything.
#   --api-url <URL>      Override the target endpoint.
#                        Default: http://localhost:8002/snow/
#   --batch-size <N>     Records per POST batch.
#                        Default: 500
#   --image <IMAGE>      Docker image that provides python3 + urllib.
#                        Default: mabesa/sapphire-prepgateway:<tag>
#                        where <tag> comes from ieasyhydroforecast_backend_docker_image_tag.
#   -h, --help           Print this message and exit 0.
#
# Examples:
#   bash bin/initialize_snow_history.sh /data/taj_data/config/.env_tjhm
#   bash bin/initialize_snow_history.sh /data/taj_data/config/.env_tjhm --dry-run
#   bash bin/initialize_snow_history.sh /data/kghm_data/config/.env_kghm \
#       --api-url http://localhost:8002/snow/ --batch-size 1000
#
# Prerequisites:
#   - Docker daemon running.
#   - sapphire-preprocessing-api container running (or api-url reachable).
#   - Snow CSVs present under <data_ref_dir>/intermediate_data/snow_data/.
# ============================================================================

set -eo pipefail
# NOTE: set -u is intentionally omitted; common_functions.sh is not strict-mode safe.

# ---------------------------------------------------------------------------
# Source shared helpers
# ---------------------------------------------------------------------------
# shellcheck disable=SC1091  # path computed at runtime; can't be statically resolved
source "$(dirname "$0")/utils/common_functions.sh"

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
API_URL="http://localhost:8002/snow/"
BATCH_SIZE=500
IMAGE_OVERRIDE=""

# log_file is set inside main() after LOG_DIR is known; early log_message
# calls fall back to /dev/null via the ${log_file:-/dev/null} default.
log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "${log_file:-/dev/null}"
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/initialize_snow_history.sh <env_file_path> [OPTIONS]

Push historical snow CSV data directly to the preprocessing API, bypassing
dg_utils.write_snow_to_api (which drops ~96 % of values on TAJ / KGHM).

Arguments:
  env_file_path        Path to the .env_<org> file (required).

Options:
  --dry-run            Discover CSVs and count value rows; do NOT POST.
  --api-url <URL>      Target endpoint  (default: http://localhost:8002/snow/).
  --batch-size <N>     Records per POST batch  (default: 500).
  --image <IMAGE>      Docker image providing python3+urllib
                       (default: mabesa/sapphire-prepgateway:<tag>).
  -h, --help           Print this message and exit 0.

Examples:
  bash bin/initialize_snow_history.sh /data/taj_data/config/.env_tjhm
  bash bin/initialize_snow_history.sh /data/taj_data/config/.env_tjhm --dry-run
  bash bin/initialize_snow_history.sh /data/kghm_data/config/.env_kghm \
      --api-url http://localhost:8002/snow/ --batch-size 1000
USAGE
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

parse_args() {
    # Handle --help / -h before requiring the positional arg
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

    # First positional argument is the env file
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
            --api-url)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --api-url requires a URL value.${NC}" >&2
                    exit 1
                fi
                API_URL="$2"
                shift 2
                ;;
            --batch-size)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --batch-size requires a numeric value.${NC}" >&2
                    exit 1
                fi
                BATCH_SIZE="$2"
                shift 2
                ;;
            --image)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --image requires an image name.${NC}" >&2
                    exit 1
                fi
                IMAGE_OVERRIDE="$2"
                shift 2
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
# Main
# ---------------------------------------------------------------------------

main() {
    parse_args "$@"

    # Print the banner before read_configuration (matches initialize_site_backfill.sh)
    print_banner
    echo "| Running Snow History Initialization (direct CSV -> API push)"

    # Validate env file exists before loading
    if [[ ! -f "$ENV_FILE" ]]; then
        echo -e "${RED}Error: env file not found: ${ENV_FILE}${NC}" >&2
        exit 1
    fi

    # Load .env vars and derive ieasyhydroforecast_* path variables
    read_configuration "${ENV_FILE}"

    # Validate required env vars
    if [[ -z "${ieasyhydroforecast_data_root_dir:-}" ]] || \
       [[ -z "${ieasyhydroforecast_data_ref_dir:-}" ]]; then
        echo -e "${RED}Error: Required environment variables are not set. Check your .env file.${NC}" >&2
        exit 1
    fi

    # Resolve image: CLI override wins; fall back to env-tag default
    local IMAGE
    if [[ -n "$IMAGE_OVERRIDE" ]]; then
        IMAGE="$IMAGE_OVERRIDE"
    else
        IMAGE="mabesa/sapphire-prepgateway:${ieasyhydroforecast_backend_docker_image_tag:-latest}"
    fi

    # Derived paths
    local SNOW_DATA_HOST="${ieasyhydroforecast_data_ref_dir}/intermediate_data/snow_data"
    local LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/snow_history_init"
    mkdir -p "${LOG_DIR}"

    local TIMESTAMP
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    log_file="${LOG_DIR}/snow_history_init_${TIMESTAMP}.log"

    echo "| Log file: ${log_file}"
    echo ""

    log_message "Starting snow history initialization"
    log_message "  env_file:   ${ENV_FILE}"
    log_message "  snow_data:  ${SNOW_DATA_HOST}"
    log_message "  api_url:    ${API_URL}"
    log_message "  batch_size: ${BATCH_SIZE}"
    log_message "  image:      ${IMAGE}"
    log_message "  dry_run:    ${DRY_RUN}"

    # -------------------------------------------------------------------------
    # Pre-flight checks
    # -------------------------------------------------------------------------

    # 1. Snow data directory must exist
    if [[ ! -d "$SNOW_DATA_HOST" ]]; then
        log_message "ERROR: Snow data directory not found: ${SNOW_DATA_HOST}"
        exit 1
    fi
    log_message "Pre-flight: snow data dir OK (${SNOW_DATA_HOST})"

    # 2. Docker must be running
    if ! docker info > /dev/null 2>&1; then
        log_message "ERROR: Docker is not running. Please start Docker and try again."
        exit 1
    fi
    log_message "Pre-flight: Docker daemon OK"

    # 3. Pull image if not present locally
    if ! docker image inspect "$IMAGE" > /dev/null 2>&1; then
        log_message "Image ${IMAGE} not found locally, pulling..."
        if ! docker pull "$IMAGE"; then
            log_message "ERROR: Failed to pull Docker image ${IMAGE}"
            exit 1
        fi
        log_message "Pull complete: ${IMAGE}"
    else
        log_message "Pre-flight: image present locally (${IMAGE})"
    fi

    # 4. API readiness check (warn-only — the API may be up even if /health 404s)
    if curl -sf --max-time 5 "${API_URL%/}/health" > /dev/null 2>&1 || \
       curl -sf --max-time 5 "${API_URL}" > /dev/null 2>&1; then
        log_message "Pre-flight: API reachable at ${API_URL}"
    else
        log_message "WARN: API health check inconclusive at ${API_URL} — continuing anyway"
    fi

    # -------------------------------------------------------------------------
    # Write the Python helper to the log dir (survives next to the log file)
    # -------------------------------------------------------------------------
    local PYTHON_HELPER="${LOG_DIR}/snow_push_helper_${TIMESTAMP}.py"

    # 'PYEOF' prevents shell variable expansion inside the heredoc.
    cat > "$PYTHON_HELPER" <<'PYEOF'
#!/usr/bin/env python3
"""Direct snow CSV -> preprocessing API push.

Called by bin/initialize_snow_history.sh — do not invoke directly.

Bypasses dg_utils.write_snow_to_api (which on TAJ/KGHM lands only ~5k of
~157k records with non-NULL value).  Reads each CSV under
SNOW_DATA_DIR/<var>/<HRU>_<var>.csv, filters to rows with a real numeric
value, and POSTs them to the preprocessing API in batches.

Sending only non-NULL values avoids the service-side _has_changes
overwrite-with-NULL bug entirely.  Only minimal fields are sent:
  snow_type, code, date, value

The operation is idempotent on (snow_type, code, date) thanks to the
service-side upsert.
"""
from __future__ import annotations

import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

SNOW_DATA_DIR = Path(os.getenv("SNOW_DATA_DIR", "/snow_data"))
API_URL = os.getenv("API_URL", "http://localhost:8002/snow/")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
DRY_RUN = "--dry-run" in sys.argv

# Map dir / column names to canonical SnowType enum values
SNOW_TYPE_CANONICAL = {
    "HS": "HS", "SWE": "SWE", "ROF": "ROF", "RoF": "ROF", "rof": "ROF",
}


def discover_csvs(root):
    if not root.is_dir():
        print(f"ERROR: snow data dir not found: {root}")
        sys.exit(1)
    out = []
    for var_dir in sorted(root.iterdir()):
        if not var_dir.is_dir():
            continue
        canonical = SNOW_TYPE_CANONICAL.get(var_dir.name)
        if canonical is None:
            print(f"  skip unknown var dir: {var_dir.name}")
            continue
        for csv_path in sorted(var_dir.glob("*.csv")):
            stem = csv_path.stem
            suffix = f"_{var_dir.name}"
            hru = stem[: -len(suffix)] if stem.endswith(suffix) else stem
            out.append((var_dir.name, canonical, hru, csv_path))
    return out


def read_records(csv_path, var_col, canonical):
    records = []
    skipped_null = 0
    skipped_parse = 0
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw = (row.get(var_col) or "").strip()
            if raw == "" or raw.lower() in ("nan", "none", "null"):
                skipped_null += 1
                continue
            try:
                value = float(raw)
            except ValueError:
                skipped_parse += 1
                continue
            value = round(value, 5)
            code = (row.get("code") or "").strip()
            date = (row.get("date") or "").strip()[:10]
            if not code or not date:
                skipped_parse += 1
                continue
            records.append({
                "snow_type": canonical,
                "code": code,
                "date": date,
                "value": value,
            })
    return records, skipped_null, skipped_parse


def post_batch(batch, url, timeout=120):
    body = json.dumps({"data": batch}).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return (200 <= resp.status < 300), f"HTTP {resp.status}"
    except urllib.error.HTTPError as e:
        body_txt = ""
        try:
            body_txt = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        return False, f"HTTP {e.code}: {body_txt}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def main():
    print("=== initialize_snow_history.sh: direct snow CSV -> API push ===")
    print(f"snow data dir:  {SNOW_DATA_DIR}")
    print(f"api url:        {API_URL}")
    print(f"batch size:     {BATCH_SIZE}")
    print(f"dry run:        {DRY_RUN}")
    print()

    csvs = discover_csvs(SNOW_DATA_DIR)
    if not csvs:
        print("No CSVs discovered. Exiting.")
        return

    print(f"Discovered {len(csvs)} CSV(s):")
    for var, canonical, hru, path in csvs:
        size_mb = path.stat().st_size / (1024 * 1024)
        print(f"  {canonical:3s} ({var:>3s}) / {hru:25s}  ->  {path.name}  ({size_mb:.1f} MiB)")
    print()

    total_sent = 0
    total_failed = 0
    total_skipped = 0
    started = time.time()

    for var, canonical, hru, csv_path in csvs:
        t0 = time.time()
        records, skipped_null, skipped_parse = read_records(csv_path, var, canonical)
        n = len(records)
        total_skipped += skipped_null + skipped_parse
        print(
            f"[{canonical}/{hru}] read {n} records "
            f"(skipped {skipped_null} null, {skipped_parse} parse) "
            f"in {time.time()-t0:.1f}s"
        )
        if not n:
            continue

        if DRY_RUN:
            print(f"  DRY_RUN: would POST {n} records")
            print(f"  first record: {records[0]}")
            print(f"  last  record: {records[-1]}")
            continue

        sent = 0
        failed = 0
        n_batches = (n + BATCH_SIZE - 1) // BATCH_SIZE
        t1 = time.time()
        for i in range(0, n, BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            ok, msg = post_batch(batch, API_URL)
            if ok:
                sent += len(batch)
            else:
                failed += len(batch)
                print(f"  batch {batch_num}/{n_batches} FAILED: {msg}")
            if batch_num % 20 == 0 or batch_num == n_batches:
                elapsed = time.time() - t1
                rate = sent / elapsed if elapsed > 0 else 0
                print(
                    f"  progress {batch_num}/{n_batches} ({sent}/{n} sent, {rate:.0f} rec/s)"
                )

        total_sent += sent
        total_failed += failed
        print(f"  DONE {canonical}/{hru}: sent {sent}, failed {failed} "
              f"in {time.time()-t1:.1f}s")
        print()

    elapsed = time.time() - started
    print("=" * 60)
    print(f"GRAND TOTAL: sent {total_sent}, failed {total_failed}, "
          f"skipped {total_skipped} (NULL/parse) in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
PYEOF

    log_message "Python helper written to: ${PYTHON_HELPER}"

    # -------------------------------------------------------------------------
    # Run the Python helper inside the prepgateway image
    # -------------------------------------------------------------------------
    local DRY_RUN_FLAG=""
    if [[ "$DRY_RUN" == true ]]; then
        DRY_RUN_FLAG="--dry-run"
    fi

    log_message "========================================"
    log_message "Running snow push via docker"
    log_message "  image:      ${IMAGE}"
    log_message "  snow_data:  ${SNOW_DATA_HOST} -> /snow_data (ro)"
    log_message "  script:     ${PYTHON_HELPER} -> /script.py (ro)"
    log_message "  api_url:    ${API_URL}"
    log_message "  batch_size: ${BATCH_SIZE}"
    if [[ -n "$DRY_RUN_FLAG" ]]; then
        log_message "  mode:       DRY RUN (no POSTs)"
    else
        log_message "  mode:       REAL run"
    fi
    log_message "========================================"

    # shellcheck disable=SC2086  # DRY_RUN_FLAG intentionally unquoted: empty=no arg; non-empty single word
    docker run --rm --network host \
        -v "${SNOW_DATA_HOST}:/snow_data:ro" \
        -v "${PYTHON_HELPER}:/script.py:ro" \
        -e "SNOW_DATA_DIR=/snow_data" \
        -e "API_URL=${API_URL}" \
        -e "BATCH_SIZE=${BATCH_SIZE}" \
        "$IMAGE" \
        python3 /script.py ${DRY_RUN_FLAG} \
        2>&1 | tee -a "${log_file}"

    local DOCKER_EXIT=${PIPESTATUS[0]}

    # -------------------------------------------------------------------------
    # Post-run summary and next steps
    # -------------------------------------------------------------------------
    echo ""
    echo -e "${BOLD}========================================"
    echo -e " SNOW HISTORY INIT COMPLETE"
    echo -e "========================================${NC}"
    echo ""

    if [[ "$DOCKER_EXIT" -ne 0 ]]; then
        echo -e "${RED}Docker run exited with code ${DOCKER_EXIT}. Check the log above and:${NC}"
        echo "  ${log_file}"
        echo ""
    else
        if [[ "$DRY_RUN" == true ]]; then
            echo -e "${YELLOW}DRY RUN complete — no records were POSTed.${NC}"
            echo "  Re-run without --dry-run to push data."
        else
            echo -e "${GREEN}Push complete.${NC}"
        fi
        echo ""
    fi

    echo "Verify value rows in the preprocessing DB:"
    echo "  docker exec sapphire-preprocessing-db \\"
    echo "    psql -U postgres -d preprocessing_db -c \\"
    echo "    \"SELECT snow_type, COUNT(*) AS total_rows, COUNT(value) AS value_rows FROM snow GROUP BY snow_type ORDER BY snow_type;\""
    echo ""
    echo "Log file: ${log_file}"

    exit "$DOCKER_EXIT"
}

main "$@"
