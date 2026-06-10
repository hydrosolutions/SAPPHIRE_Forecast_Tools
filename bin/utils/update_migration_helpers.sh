#!/usr/bin/env bash
# ============================================================================
# update_migration_helpers.sh
#
# Shared helpers for the update-time migration toolkit (P0 foundation).
#
# Purpose:
#   Provides reusable shell primitives that every migration wrapper under
#   bin/initialize_*_history.sh and bin/export_*_history.sh sources to avoid
#   re-implementing image resolution, temp-workspace hygiene, manifest
#   validation, and redacted logging.
#
# Contract:
#   This file is intended to be SOURCED only. It defines functions prefixed
#   "umh_" (update-migration-helpers) and does not execute logic at source time.
#   It sources bin/utils/common_functions.sh for read_configuration, banner,
#   and env parsing.
#
# NOTE: This helper file is intended to be sourced from migration wrappers
# under `bin/initialize_*_history.sh` and `bin/export_*_history.sh`. It sources
# `bin/utils/common_functions.sh` for `read_configuration`, banner, and env
# parsing. The wrappers run with `set -eo pipefail` and intentionally omit
# `set -u` because `bin/utils/common_functions.sh:274-282` references
# `$ieasyhydroforecast_ssh_tunnel_pid` without a default-empty guard, which
# fires under strict mode at trap-cleanup time. This file's own functions
# guard every external variable as `${name:-}` so callers that opt in to
# strict mode locally do not break.
#
# v2 R3 — Trap signal set:
#   `umh_acquire_temp_workspace` registers a cleanup trap on EXIT INT TERM
#   (widened from EXIT-only) so Ctrl-C and SIGTERM during a wrapper run do
#   not orphan temp dirs containing real station codes. SIGKILL and power-
#   loss cannot be trapped — the operator must use the §10 runbook manual
#   cleanup glob (`rm -rf "${ieasyhydroforecast_data_root_dir}/logs/"*_tmp`)
#   to recover.
#
# Usage (from a wrapper):
#   set -eo pipefail
#   source "$(dirname "$0")/utils/update_migration_helpers.sh"
#   ...
#   IMAGE=$(umh_resolve_image "$IMAGE_OVERRIDE" "${ieasyhydroforecast_backend_docker_image_tag:-}")
#   TMPDIR=$(umh_acquire_temp_workspace runoff_day)
#   umh_validate_export_manifest "$EXPORT_CSV" runoff_period
#
# ============================================================================

# Prevent double-source. Re-sourcing would re-register traps.
if [[ -n "${_UMH_SOURCED:-}" ]]; then
    return 0
fi
_UMH_SOURCED=1

# ---------------------------------------------------------------------------
# Source shared base helpers (read_configuration, print_banner, ...)
# ---------------------------------------------------------------------------
# shellcheck disable=SC1091  # path computed at runtime; can't be statically resolved
source "$(dirname "${BASH_SOURCE[0]}")/common_functions.sh"

# ---------------------------------------------------------------------------
# Internal state: temp dirs registered for trap cleanup
# ---------------------------------------------------------------------------
# Initialised once; appended-to by umh_acquire_temp_workspace. Using an array
# (rather than re-assigning) preserves earlier temp dirs across multiple
# acquires within a single wrapper run.
if [[ -z "${TMPDIRS+x}" ]]; then
    TMPDIRS=()
fi

# ---------------------------------------------------------------------------
# umh_log_redacted: echo to stdout + log_file, NEVER include raw station codes.
# Caller is responsible for redacting any station-code content before
# constructing the message.
# ---------------------------------------------------------------------------
umh_log_redacted() {
    local msg="${1:-}"
    echo "[$(date +"%Y-%m-%d %H:%M:%S")] ${msg}" | tee -a "${log_file:-/dev/null}"
}

# ---------------------------------------------------------------------------
# umh_check_arch_platform: warn Apple Silicon / arm64 operators if
# DOCKER_DEFAULT_PLATFORM is unset. The published mabesa/sapphire-prepgateway
# tags are currently amd64-only; without the override, docker pull fails on
# arm64 with a "no matching manifest for linux/arm64/v8 in the manifest list"
# error.
#
# Testability seam:
#   The architecture string is taken from $UMH_ARCH_OVERRIDE when set, else
#   `uname -m`. The override env var lets the shell test suite simulate an
#   arm64 host without stubbing `uname` on PATH. It is an internal test hook
#   and is intentionally NOT documented in operator-facing material.
# ---------------------------------------------------------------------------
umh_check_arch_platform() {
    local _arch
    _arch="${UMH_ARCH_OVERRIDE:-$(uname -m)}"
    if [[ "$_arch" =~ ^(arm64|aarch64)$ && -z "${DOCKER_DEFAULT_PLATFORM:-}" ]]; then
        echo "WARNING: arm64/Apple Silicon host detected. The mabesa/sapphire-prepgateway images are amd64-only. Set DOCKER_DEFAULT_PLATFORM=linux/amd64 before invoking the wrapper, or the docker pull will fail." >&2
    fi
}

# ---------------------------------------------------------------------------
# umh_resolve_image: pick CLI override > configured tag > FALLBACK.
# Inputs:  $1 = CLI override (may be empty), $2 = configured tag (may be empty)
# Stdout:  resolved image string
# Stderr:  WARNING line if resolved tag is :local or :latest AND a deployment
#          container is detected on this host. Also emits an arm64 platform
#          warning (via umh_check_arch_platform) so every wrapper inherits it.
#
# NOTE: docker ps (no -a) misses stopped containers; maintenance-stopped
# deployments fall through to Python-only warning (not elevated). Override
# --image explicitly if elevation is required.
# ---------------------------------------------------------------------------
umh_resolve_image() {
    local cli_override="${1:-}"
    local configured_tag="${2:-}"
    local image=""
    local source=""

    # Arm64 preflight: warn on Apple Silicon hosts without
    # DOCKER_DEFAULT_PLATFORM=linux/amd64. Wrappers inherit this automatically
    # by calling umh_resolve_image during their image-resolution step.
    umh_check_arch_platform

    if [[ -n "$cli_override" ]]; then
        image="$cli_override"
        source="cli"
    elif [[ -n "$configured_tag" ]]; then
        image="mabesa/sapphire-prepgateway:${configured_tag}"
        source="configured"
    else
        image="mabesa/sapphire-prepgateway:latest"
        source="fallback"
    fi

    # Elevated warning when an unpinned tag is resolved on a deployment host.
    # (S1 limitation comment above describes the maintenance-stopped gap.)
    # Guidance is abstract (no literal month) because dated tags rotate
    # month-by-month; operators verify currently-available tags on Docker Hub.
    case "$image" in
        *:local|*:latest)
            if command -v docker >/dev/null 2>&1; then
                if [[ -n "$(docker ps --filter 'name=sapphire-preprocessing-db' --quiet 2>/dev/null)" ]]; then
                    echo "WARNING: resolved image '${image}' uses an unpinned tag on a deployment host. Pin to a current dated tag published on Docker Hub (format YYYY-MM, e.g. the latest available). Verify available tags at https://hub.docker.com/r/mabesa/sapphire-prepgateway/tags." >&2
                fi
            fi
            ;;
    esac

    # Internal: expose the resolved source via a global for the caller's log line.
    UMH_LAST_IMAGE_SOURCE="$source"
    echo "$image"
}

# ---------------------------------------------------------------------------
# umh_require_env_var: exit 1 with a redacted message if the named variable
# is unset or empty. Never prints the variable's value.
# ---------------------------------------------------------------------------
umh_require_env_var() {
    local name="${1:-}"
    if [[ -z "$name" ]]; then
        echo "ERROR: umh_require_env_var: variable name argument missing" >&2
        exit 1
    fi
    local value="${!name:-}"
    if [[ -z "$value" ]]; then
        echo "ERROR: required environment variable '${name}' is unset or empty (value redacted)" >&2
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# _umh_cleanup_tempdirs: invoked by trap on EXIT INT TERM.
# Removes every temp dir registered in TMPDIRS. Errors during cleanup are
# logged to stderr but do not abort the trap.
# ---------------------------------------------------------------------------
_umh_cleanup_tempdirs() {
    local dir
    for dir in "${TMPDIRS[@]:-}"; do
        if [[ -n "$dir" && -d "$dir" ]]; then
            rm -rf -- "$dir" 2>/dev/null || \
                echo "WARNING: failed to remove temp workspace ${dir}" >&2
        fi
    done
}

# ---------------------------------------------------------------------------
# umh_acquire_temp_workspace: create a strict-perm temp dir + register cleanup.
# Inputs:  $1 = wrapper short name (e.g. "runoff_day")
# Stdout:  absolute path to the created directory.
# Side effects:
#   - sets umask 077 in the calling shell (persists for the wrapper run);
#   - appends the new dir to TMPDIRS;
#   - on first call, installs an EXIT/INT/TERM trap that calls
#     _umh_cleanup_tempdirs (v2 R3 widened signal set).
#
# v2 R3 caveat (SIGKILL / power-loss):
#   The trap cannot fire on SIGKILL or a power loss. Orphaned temp dirs under
#   ${ieasyhydroforecast_data_root_dir}/logs/*_tmp/ may contain real station
#   codes. The §10 runbook manual cleanup (`rm -rf "${data_root}/logs/"*_tmp`)
#   is the operator-side recovery path for that case.
# ---------------------------------------------------------------------------
umh_acquire_temp_workspace() {
    local short_name="${1:-}"
    if [[ -z "$short_name" ]]; then
        echo "ERROR: umh_acquire_temp_workspace: wrapper short name argument missing" >&2
        exit 1
    fi
    if [[ -z "${ieasyhydroforecast_data_root_dir:-}" ]]; then
        echo "ERROR: ieasyhydroforecast_data_root_dir is unset; cannot create temp workspace" >&2
        exit 1
    fi

    # Persist the restrictive umask for the rest of the wrapper run.
    umask 077

    local ts
    ts="$(date -u +%Y%m%dT%H%M%SZ)"
    local new="${ieasyhydroforecast_data_root_dir}/logs/${short_name}_tmp/${ts}"

    if [[ -e "$new" ]]; then
        echo "ERROR: temp workspace already exists (pick a new timestamp): ${new}" >&2
        exit 1
    fi
    mkdir -p "$new"
    chmod 700 "$new"

    # Append-safe registration; multiple acquires preserve earlier dirs.
    TMPDIRS+=("$new")

    # Install the cleanup trap once.
    if [[ -z "${_UMH_TRAP_INSTALLED:-}" ]]; then
        # shellcheck disable=SC2064  # function name expansion is intentional here
        trap _umh_cleanup_tempdirs EXIT INT TERM
        _UMH_TRAP_INSTALLED=1
    fi

    echo "$new"
}

# ---------------------------------------------------------------------------
# umh_validate_export_manifest: thin wrapper around migration_py._common
# validate_manifest. Exits 1 with a descriptive message on failure.
# Inputs:  $1 = export CSV path, $2 = expected export_type string
# ---------------------------------------------------------------------------
umh_validate_export_manifest() {
    local csv_path="${1:-}"
    local expected_type="${2:-}"
    if [[ -z "$csv_path" || -z "$expected_type" ]]; then
        echo "ERROR: umh_validate_export_manifest: <csv_path> and <expected_export_type> are both required" >&2
        exit 1
    fi
    local helper_dir
    helper_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

    if ! python3 - "$csv_path" "$expected_type" "$helper_dir" <<'PYEOF'
import sys
csv_path = sys.argv[1]
expected = sys.argv[2]
helper_dir = sys.argv[3]
sys.path.insert(0, helper_dir)
from migration_py import _common
try:
    _common.validate_manifest(csv_path, expected)
except _common.ManifestError as exc:
    print(f"ERROR: manifest validation failed: {exc}", file=sys.stderr)
    sys.exit(1)
PYEOF
    then
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# umh_print_image_resolution_line: emit a uniform one-line image-resolution
# log entry so every wrapper logs the same shape.
# Inputs:  $1 = resolved image, $2 = source (cli|configured|fallback)
# ---------------------------------------------------------------------------
umh_print_image_resolution_line() {
    local image="${1:-}"
    local source="${2:-${UMH_LAST_IMAGE_SOURCE:-unknown}}"
    umh_log_redacted "  image: ${image} source=${source}"
}

# ---------------------------------------------------------------------------
# KNOWN QUIRK (not patched by this helper):
# bin/utils/common_functions.sh:274-282 references $ieasyhydroforecast_ssh_tunnel_pid
# without a default-empty guard. Under `set -u`, this fires at cleanup. Wrappers
# must therefore run with `set -eo pipefail` (NOT `set -euo`). See also
# ~/.claude/memory/snow_backfill_cleanup_exit1_quirk.md.
# ---------------------------------------------------------------------------
