#!/usr/bin/env bash
# ============================================================================
# initialize_meteo_history.sh
#
# Update-time migration wrapper for meteo T/P historical CSVs (Phase P1b).
#
# Purpose:
#   Push historical meteo (Temperature / Precipitation) data from per-HRU CSVs
#   to the preprocessing API. Replaces the old
#   `sapphire/services/preprocessing/app/data_migrator.py` MeteoDataMigrator
#   pathway, which hardcoded a single HRU code. This wrapper discovers ALL
#   `<HRU>_T_reanalysis.csv` and `<HRU>_P_reanalysis.csv` files under
#   `<data_ref_dir>/intermediate_data/hindcast_forcing/`, optionally merges
#   per-HRU `*_dashboard.csv` files for the `norm` field, and POSTs minimal
#   non-null payloads to `/meteo/`.
#
# Architecture quirk:
#   The operational `extend_era5_reanalysis.py` path skips API writes for
#   reanalysis data — historical reanalysis lives ONLY in CSV. This wrapper
#   is therefore the historical migration path. See architecture plan §Q1
#   per-data-type strategy table (row "Meteo T/P reanalysis and dashboard
#   norms") and Stage A.2 §C audit.
#
# Safety:
#   - Minimal non-null payloads (`meteo_type`, `code`, `date`, optional
#     `value`, optional `norm`, optional `day_of_year`) per universal
#     safe-write rule (architecture §Q2 layer 2). Missing one side
#     (value or norm) is never sent as null.
#   - MODE detection ("full-import" if target meteo_type empty;
#     "pre-cutoff" otherwise) handled per meteo_type independently via
#     `migration_py._common.detect_mode`.
#   - Sentinel-aware: `--station-filter <HRU>` (the locked P0 forward
#     contract) restricts processing to a single HRU code.
#
# Usage:
#   bash bin/initialize_meteo_history.sh <env_file_path> [OPTIONS]
#
# Arguments:
#   env_file_path        Path to the .env_<org> file (required).
#
# Options:
#   --dry-run            Discover HRUs and emit inventory; do NOT POST.
#   --api-url <URL>      Target endpoint (default: http://localhost:8002/meteo/).
#   --batch-size <N>     Records per POST batch (default: 500).
#   --image <IMAGE>      Docker image providing python3+urllib
#                        (default: mabesa/sapphire-prepgateway:<configured_tag>).
#   --station-filter <CODE>
#                        Process only the HRU code matching <CODE>.
#                        Locked forward contract from P0 (see runbook §4.3
#                        canary procedure). Honored by all CSV-source wrappers.
#   --meteo-type T|P|both
#                        Restrict to T-only, P-only, or both (default: both).
#   -h, --help           Print this message and exit 0.
#
# Examples:
#   bash bin/initialize_meteo_history.sh /data/taj_data/config/.env_tjhm --dry-run
#   bash bin/initialize_meteo_history.sh /data/taj_data/config/.env_tjhm \
#       --station-filter 19999 --meteo-type T
#   bash bin/initialize_meteo_history.sh /data/taj_data/config/.env_tjhm \
#       --api-url http://localhost:8002/meteo/ --batch-size 1000
#
# Prerequisites:
#   - Docker daemon running.
#   - sapphire-preprocessing-api container running (or api-url reachable).
#   - Reanalysis CSVs present under
#     <data_ref_dir>/intermediate_data/hindcast_forcing/<HRU>_T_reanalysis.csv
#     and/or <HRU>_P_reanalysis.csv.
#
# NOTE: `set -eo pipefail` only — strict `set -u` interacts badly with the
# unbound-var quirk at bin/utils/common_functions.sh:274-282 (see
# update_migration_helpers.sh KNOWN QUIRK block).
# ============================================================================

set -eo pipefail

# ---------------------------------------------------------------------------
# Source shared helpers (update_migration_helpers.sh also sources
# common_functions.sh, so read_configuration/print_banner are available).
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
API_URL="http://localhost:8002/meteo/"
BATCH_SIZE=500
IMAGE_OVERRIDE=""
STATION_FILTER=""
METEO_TYPE_FILTER="both"

# log_file is set inside main() after LOG_DIR is known.
log_message() {
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] $1" | tee -a "${log_file:-/dev/null}"
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------

print_usage() {
    cat <<'USAGE'
Usage: bash bin/initialize_meteo_history.sh <env_file_path> [OPTIONS]

Push historical meteo (T/P) reanalysis CSV data to the preprocessing API.
Discovers all HRU-keyed reanalysis CSVs under
<data_ref_dir>/intermediate_data/hindcast_forcing/ and POSTs minimal
non-null payloads. Replaces the old data_migrator.py MeteoDataMigrator
which hardcoded a single HRU code.

Arguments:
  env_file_path        Path to the .env_<org> file (required).

Options:
  --dry-run            Inventory HRUs and counts; do NOT POST.
  --api-url <URL>      Target endpoint (default: http://localhost:8002/meteo/).
  --batch-size <N>     Records per POST batch (default: 500).
  --image <IMAGE>      Docker image providing python3+urllib
                       (default: mabesa/sapphire-prepgateway:<tag>).
  --station-filter <CODE>
                       Process only the HRU code matching <CODE>.
  --meteo-type T|P|both
                       Restrict to T-only, P-only, or both (default: both).
  -h, --help           Print this message and exit 0.

Examples:
  bash bin/initialize_meteo_history.sh /data/taj_data/config/.env_tjhm --dry-run
  bash bin/initialize_meteo_history.sh /data/taj_data/config/.env_tjhm \
      --station-filter 19999 --meteo-type T
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
            --station-filter)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --station-filter requires a station code value.${NC}" >&2
                    exit 1
                fi
                STATION_FILTER="$2"
                shift 2
                ;;
            --meteo-type)
                if [[ $# -lt 2 || "$2" == -* ]]; then
                    echo -e "${RED}Error: --meteo-type requires T, P, or both.${NC}" >&2
                    exit 1
                fi
                case "$2" in
                    T|P|both)
                        METEO_TYPE_FILTER="$2"
                        ;;
                    *)
                        echo -e "${RED}Error: --meteo-type must be 'T', 'P', or 'both' (got: $2).${NC}" >&2
                        exit 1
                        ;;
                esac
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

    print_banner
    echo "| Running Meteo History Initialization (T/P CSV -> API push)"

    if [[ ! -f "$ENV_FILE" ]]; then
        echo -e "${RED}Error: env file not found: ${ENV_FILE}${NC}" >&2
        exit 1
    fi

    read_configuration "${ENV_FILE}"

    if [[ -z "${ieasyhydroforecast_data_root_dir:-}" ]] || \
       [[ -z "${ieasyhydroforecast_data_ref_dir:-}" ]]; then
        echo -e "${RED}Error: Required environment variables are not set. Check your .env file.${NC}" >&2
        exit 1
    fi

    # Resolve image via shared helper (logs WARNING on unpinned tag if a
    # deployment container is detected on this host).
    local IMAGE IMAGE_SOURCE
    IMAGE=$(umh_resolve_image "$IMAGE_OVERRIDE" "${ieasyhydroforecast_backend_docker_image_tag:-}")
    IMAGE_SOURCE="${UMH_LAST_IMAGE_SOURCE:-unknown}"

    # Derived paths
    local METEO_DATA_HOST="${ieasyhydroforecast_data_ref_dir}/intermediate_data/hindcast_forcing"
    local LOG_DIR="${ieasyhydroforecast_data_root_dir}/logs/meteo_history_init"
    mkdir -p "${LOG_DIR}"

    local TIMESTAMP
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    log_file="${LOG_DIR}/meteo_history_init_${TIMESTAMP}.log"

    echo "| Log file: ${log_file}"
    echo ""

    log_message "Starting meteo history initialization"
    log_message "  env_file:        ${ENV_FILE}"
    log_message "  meteo_data:      ${METEO_DATA_HOST}"
    log_message "  api_url:         ${API_URL}"
    log_message "  batch_size:      ${BATCH_SIZE}"
    log_message "  station_filter:  ${STATION_FILTER:-(none)}"
    log_message "  meteo_type:      ${METEO_TYPE_FILTER}"
    log_message "  dry_run:         ${DRY_RUN}"

    # Uniform image-resolution line (shared helper).
    umh_print_image_resolution_line "$IMAGE" "$IMAGE_SOURCE"

    # -------------------------------------------------------------------------
    # Pre-flight checks
    # -------------------------------------------------------------------------

    if [[ ! -d "$METEO_DATA_HOST" ]]; then
        log_message "ERROR: Meteo data directory not found: ${METEO_DATA_HOST}"
        exit 1
    fi
    log_message "Pre-flight: meteo data dir OK (${METEO_DATA_HOST})"

    if ! docker info > /dev/null 2>&1; then
        log_message "ERROR: Docker is not running. Please start Docker and try again."
        exit 1
    fi
    log_message "Pre-flight: Docker daemon OK"

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

    if curl -sf --max-time 5 "${API_URL%/}/health" > /dev/null 2>&1 || \
       curl -sf --max-time 5 "${API_URL}" > /dev/null 2>&1; then
        log_message "Pre-flight: API reachable at ${API_URL}"
    else
        log_message "WARN: API health check inconclusive at ${API_URL} — continuing anyway"
    fi

    # -------------------------------------------------------------------------
    # MODE detection (per meteo_type, via target DB inspection).
    # Skipped in DRY_RUN to keep the dry-run host-independent.
    # -------------------------------------------------------------------------
    local MODE_T="full-import" MODE_P="full-import"
    local CUTOFF_T="" CUTOFF_P=""

    if [[ "$DRY_RUN" == false ]]; then
        local helper_dir
        helper_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/utils" && pwd)"

        for mt in T P; do
            # Skip the type we're not processing.
            if [[ "$METEO_TYPE_FILTER" != "both" && "$METEO_TYPE_FILTER" != "$mt" ]]; then
                continue
            fi
            local count_min sql
            sql="SELECT COUNT(*) || '|' || COALESCE(MIN(date)::text, '') FROM meteo WHERE meteo_type='${mt}';"
            if count_min=$(docker exec sapphire-preprocessing-db \
                psql -U postgres -d preprocessing_db -P pager=off -t -A \
                -c "$sql" 2>/dev/null); then
                local target_count="${count_min%%|*}"
                local target_min_date="${count_min#*|}"
                local mode_cutoff
                mode_cutoff=$(python3 - "$target_count" "$target_min_date" "$helper_dir" <<'PYEOF'
import sys
target_count = int(sys.argv[1].strip() or "0")
target_min_date = sys.argv[2].strip() or None
helper_dir = sys.argv[3]
sys.path.insert(0, helper_dir)
from migration_py import _common
mode, cutoff = _common.detect_mode(
    target_count=target_count, target_min_date=target_min_date
)
print(f"{mode}|{cutoff or ''}")
PYEOF
                )
                local mode="${mode_cutoff%%|*}"
                local cutoff="${mode_cutoff#*|}"
                if [[ "$mt" == "T" ]]; then
                    MODE_T="$mode"
                    CUTOFF_T="$cutoff"
                else
                    MODE_P="$mode"
                    CUTOFF_P="$cutoff"
                fi
                log_message "MODE meteo_type=${mt}: mode=${mode} cutoff=${cutoff:-none} target_count=${target_count}"
            else
                log_message "WARN: could not query meteo target state for meteo_type=${mt}; defaulting to full-import"
            fi
        done
    else
        log_message "MODE detection skipped (dry-run)."
    fi

    # -------------------------------------------------------------------------
    # Write the Python helper to the log dir (survives next to the log file).
    # -------------------------------------------------------------------------
    local PYTHON_HELPER="${LOG_DIR}/meteo_push_helper_${TIMESTAMP}.py"

    # 'PYEOF' prevents shell variable expansion inside the heredoc.
    cat > "$PYTHON_HELPER" <<'PYEOF'
#!/usr/bin/env python3
"""Direct meteo CSV -> preprocessing API push.

Called by bin/initialize_meteo_history.sh — do not invoke directly.

P1b design: discover every <HRU>_T_reanalysis.csv / <HRU>_P_reanalysis.csv
under METEO_DATA_DIR via glob — contrast against the old data_migrator.py
which hardcoded a single HRU literal. Optionally merge per-HRU
*_dashboard.csv for the `norm` field. POST minimal non-null payloads
(meteo_type, code, date, optional value, optional norm, optional day_of_year)
to the API. Idempotent on (meteo_type, code, date).
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

METEO_DATA_DIR = Path(os.getenv("METEO_DATA_DIR", "/meteo_data"))
API_URL = os.getenv("API_URL", "http://localhost:8002/meteo/")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
STATION_FILTER = os.getenv("STATION_FILTER", "").strip()
METEO_TYPE_FILTER = os.getenv("METEO_TYPE_FILTER", "both").strip()
DRY_RUN = "--dry-run" in sys.argv

# Filename pattern: <HRU>_<T|P>_reanalysis.csv
# We extract the HRU code from the filename rather than trusting the `code`
# CSV column (which in the old archives also contains the HRU).
REANALYSIS_RE = re.compile(r"^(?P<hru>[A-Za-z0-9]+)_(?P<mt>[TP])_reanalysis\.csv$")


def discover_reanalysis_files(root: Path) -> list[tuple[str, str, Path, Path | None]]:
    """Glob the data root for <HRU>_T_reanalysis.csv / <HRU>_P_reanalysis.csv.

    Returns a list of (hru, meteo_type, reanalysis_path, dashboard_path_or_None)
    tuples. The dashboard file lives at the same dir with suffix
    `_dashboard.csv` (e.g. 00001_T_reanalysis_dashboard.csv).
    """
    if not root.is_dir():
        print(f"ERROR: meteo data dir not found: {root}")
        sys.exit(1)
    out: list[tuple[str, str, Path, Path | None]] = []
    for path in sorted(root.glob("*_reanalysis.csv")):
        m = REANALYSIS_RE.match(path.name)
        if m is None:
            continue
        hru = m.group("hru")
        mt = m.group("mt")
        if STATION_FILTER and hru != STATION_FILTER:
            continue
        if METEO_TYPE_FILTER != "both" and mt != METEO_TYPE_FILTER:
            continue
        dash_name = path.name.replace(".csv", "_dashboard.csv")
        dash_path = path.with_name(dash_name)
        out.append((hru, mt, path, dash_path if dash_path.is_file() else None))
    return out


def parse_iso_date_or_none(s: str) -> str | None:
    """Return the leading YYYY-MM-DD slice if it parses, else None."""
    s = (s or "").strip()
    if len(s) < 10:
        return None
    try:
        datetime.strptime(s[:10], "%Y-%m-%d")
        return s[:10]
    except ValueError:
        return None


def parse_float_or_none(raw: str) -> float | None:
    raw = (raw or "").strip()
    if raw == "" or raw.lower() in ("nan", "none", "null"):
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def load_norm_map(dash_path: Path, meteo_type: str) -> dict[tuple[str, str], float]:
    """Read <hru>_<T|P>_reanalysis_dashboard.csv and return (code, date) -> norm.

    Dashboard header: code,<MT>_norm,date,<MT>
    """
    norm_col = f"{meteo_type}_norm"
    out: dict[tuple[str, str], float] = {}
    with dash_path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date = parse_iso_date_or_none(row.get("date", ""))
            code = (row.get("code") or "").strip()
            if not date or not code:
                continue
            norm = parse_float_or_none(row.get(norm_col, ""))
            if norm is None:
                continue
            out[(code, date)] = norm
    return out


def build_records(
    reanalysis_path: Path,
    dash_path: Path | None,
    meteo_type: str,
    cutoff: str | None,
) -> tuple[list[dict], int, int, str | None, str | None]:
    """Build POST records for one HRU+meteo_type pair.

    Returns (records, skipped_null, skipped_parse, date_min, date_max).
    Skips rows when both `value` and `norm` would be null (no point posting
    an empty record) and when date < cutoff in pre-cutoff mode.
    """
    norm_map: dict[tuple[str, str], float] = {}
    if dash_path is not None:
        norm_map = load_norm_map(dash_path, meteo_type)

    value_col = meteo_type  # 'T' or 'P'
    records: list[dict] = []
    skipped_null = 0
    skipped_parse = 0
    date_min: str | None = None
    date_max: str | None = None

    with reanalysis_path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date = parse_iso_date_or_none(row.get("date", ""))
            code = (row.get("code") or "").strip()
            if not date or not code:
                skipped_parse += 1
                continue

            # Cutoff filter (full-import => cutoff is None; pre-cutoff => date < cutoff).
            if cutoff and date >= cutoff:
                continue

            value = parse_float_or_none(row.get(value_col, ""))
            norm = norm_map.get((code, date))

            if value is None and norm is None:
                skipped_null += 1
                continue

            rec: dict[str, object] = {
                "meteo_type": meteo_type,
                "code": code,
                "date": date,
            }
            if value is not None:
                rec["value"] = round(value, 5)
            if norm is not None:
                rec["norm"] = round(norm, 5)
            try:
                rec["day_of_year"] = datetime.strptime(date, "%Y-%m-%d").timetuple().tm_yday
            except ValueError:
                # Should be unreachable thanks to parse_iso_date_or_none.
                pass
            records.append(rec)

            if date_min is None or date < date_min:
                date_min = date
            if date_max is None or date > date_max:
                date_max = date

    return records, skipped_null, skipped_parse, date_min, date_max


def post_batch(batch: list[dict], url: str, timeout: int = 120) -> tuple[bool, str]:
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
    except Exception as e:  # noqa: BLE001 - want all exception classes here
        return False, f"{type(e).__name__}: {e}"


def main() -> None:
    cutoff_t = os.getenv("CUTOFF_T", "").strip() or None
    cutoff_p = os.getenv("CUTOFF_P", "").strip() or None

    print("=== initialize_meteo_history.sh: meteo CSV -> API push ===")
    print(f"meteo data dir:    {METEO_DATA_DIR}")
    print(f"api url:           {API_URL}")
    print(f"batch size:        {BATCH_SIZE}")
    print(f"station filter:    {STATION_FILTER or '(none)'}")
    print(f"meteo type filter: {METEO_TYPE_FILTER}")
    print(f"cutoff T:          {cutoff_t or '(none = full-import)'}")
    print(f"cutoff P:          {cutoff_p or '(none = full-import)'}")
    print(f"dry run:           {DRY_RUN}")
    print()

    files = discover_reanalysis_files(METEO_DATA_DIR)
    if not files:
        if STATION_FILTER or METEO_TYPE_FILTER != "both":
            print(
                f"No reanalysis CSVs matched filters "
                f"(station_filter={STATION_FILTER!r}, meteo_type={METEO_TYPE_FILTER!r}). "
                "Exiting."
            )
        else:
            print("No reanalysis CSVs discovered. Exiting.")
        return

    print(f"Discovered {len(files)} reanalysis CSV(s):")
    distinct_hrus: set[str] = set()
    for hru, mt, rpath, dpath in files:
        distinct_hrus.add(hru)
        dash_tag = "dashboard=yes" if dpath else "dashboard=no"
        size_mb = rpath.stat().st_size / (1024 * 1024)
        # NEVER print real codes — but HRU codes here come straight from
        # disk filenames; do redacted-style counting for the summary at the
        # bottom. Per-file detail is intentionally one line; operators rely on
        # it to validate discovery.
        print(f"  meteo_type={mt} hru={hru} {dash_tag} ({size_mb:.2f} MiB) -> {rpath.name}")
    print(f"distinct HRUs discovered (redacted count): {len(distinct_hrus)}")
    print()

    total_sent = 0
    total_failed = 0
    total_skipped = 0
    started = time.time()

    for hru, mt, rpath, dpath in files:
        cutoff = cutoff_t if mt == "T" else cutoff_p
        t0 = time.time()
        records, skipped_null, skipped_parse, date_min, date_max = build_records(
            rpath, dpath, mt, cutoff
        )
        n = len(records)
        total_skipped += skipped_null + skipped_parse
        date_range = f"{date_min}..{date_max}" if date_min else "(empty)"
        print(
            f"[meteo_type={mt}/hru={hru}] read {n} record(s) "
            f"(skipped {skipped_null} null, {skipped_parse} parse, "
            f"cutoff={cutoff or 'none'}, range={date_range}) "
            f"in {time.time() - t0:.2f}s"
        )

        if not n:
            continue

        if DRY_RUN:
            print(f"  DRY_RUN: would POST {n} record(s)")
            print(f"  first: {records[0]}")
            print(f"  last:  {records[-1]}")
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
                    f"  progress {batch_num}/{n_batches} "
                    f"({sent}/{n} sent, {rate:.0f} rec/s)"
                )

        total_sent += sent
        total_failed += failed
        print(
            f"  DONE meteo_type={mt}/hru={hru}: sent {sent}, failed {failed} "
            f"in {time.time() - t1:.2f}s"
        )
        print()

    elapsed = time.time() - started
    print("=" * 60)
    print(
        f"GRAND TOTAL: sent {total_sent}, failed {total_failed}, "
        f"skipped {total_skipped} (NULL/parse) in {elapsed:.2f}s"
    )


if __name__ == "__main__":
    main()
PYEOF

    log_message "Python helper written to: ${PYTHON_HELPER}"

    # -------------------------------------------------------------------------
    # Run the Python helper inside the prepgateway image.
    # -------------------------------------------------------------------------
    local DRY_RUN_FLAG=""
    if [[ "$DRY_RUN" == true ]]; then
        DRY_RUN_FLAG="--dry-run"
    fi

    log_message "========================================"
    log_message "Running meteo push via docker"
    log_message "  image:            ${IMAGE}"
    log_message "  meteo_data:       ${METEO_DATA_HOST} -> /meteo_data (ro)"
    log_message "  script:           ${PYTHON_HELPER} -> /script.py (ro)"
    log_message "  api_url:          ${API_URL}"
    log_message "  batch_size:       ${BATCH_SIZE}"
    log_message "  station_filter:   ${STATION_FILTER:-(none)}"
    log_message "  meteo_type:       ${METEO_TYPE_FILTER}"
    log_message "  mode_T:           ${MODE_T} cutoff=${CUTOFF_T:-none}"
    log_message "  mode_P:           ${MODE_P} cutoff=${CUTOFF_P:-none}"
    if [[ -n "$DRY_RUN_FLAG" ]]; then
        log_message "  mode:             DRY RUN (no POSTs)"
    else
        log_message "  mode:             REAL run"
    fi
    log_message "========================================"

    # shellcheck disable=SC2086  # DRY_RUN_FLAG intentionally unquoted: empty=no arg; non-empty single word
    docker run --rm --network host \
        -v "${METEO_DATA_HOST}:/meteo_data:ro" \
        -v "${PYTHON_HELPER}:/script.py:ro" \
        -e "METEO_DATA_DIR=/meteo_data" \
        -e "API_URL=${API_URL}" \
        -e "BATCH_SIZE=${BATCH_SIZE}" \
        -e "STATION_FILTER=${STATION_FILTER}" \
        -e "METEO_TYPE_FILTER=${METEO_TYPE_FILTER}" \
        -e "CUTOFF_T=${CUTOFF_T}" \
        -e "CUTOFF_P=${CUTOFF_P}" \
        "$IMAGE" \
        python3 /script.py ${DRY_RUN_FLAG} \
        2>&1 | tee -a "${log_file}"

    local DOCKER_EXIT=${PIPESTATUS[0]}

    # -------------------------------------------------------------------------
    # Post-run summary and next steps
    # -------------------------------------------------------------------------
    echo ""
    echo -e "${BOLD}========================================"
    echo -e " METEO HISTORY INIT COMPLETE"
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

    echo "Verify meteo rows in the preprocessing DB:"
    echo "  docker exec sapphire-preprocessing-db \\"
    echo "    psql -U postgres -d preprocessing_db -P pager=off -c \\"
    echo "    \"SELECT meteo_type, COUNT(*) AS rows, COUNT(DISTINCT code) AS code_count, COUNT(value) AS value_rows, COUNT(norm) AS norm_rows, MIN(date) AS min_date, MAX(date) AS max_date FROM meteo GROUP BY meteo_type ORDER BY meteo_type;\""
    echo ""
    echo "Log file: ${log_file}"

    exit "$DOCKER_EXIT"
}

main "$@"
