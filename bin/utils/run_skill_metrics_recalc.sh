#!/bin/bash
# Shared helper for skill-metrics recalculation docker-run invocations.
#
# This file defines a single function:
#   run_skill_metrics_recalc_once <mode> <log_dir> <timestamp> <container_name>
#
# It encapsulates the macOS `IEASYHYDROHF_HOST` override, container-name and
# service-log derivation, stale-container cleanup, the `docker run` that
# invokes `uv run recalculate_skill_metrics.py`, exit-code capture, and
# post-run container removal.
#
# Preconditions (caller's responsibility):
#   * The caller MUST have sourced `bin/utils/common_functions.sh` and called
#     `read_configuration <env_file>` so that the following env vars are set:
#       - ieasyhydroforecast_data_root_dir
#       - ieasyhydroforecast_env_file_path
#       - ieasyhydroforecast_data_ref_dir
#       - ieasyhydroforecast_container_data_ref_dir
#       - ieasyhydroforecast_backend_docker_image_tag (optional; defaults to "latest")
#       - IEASYHYDROHF_HOST (used by the macOS override block)
#   * The caller MUST have already verified Docker is running, already pulled
#     the image if necessary, already established the SSH tunnel (if needed),
#     and already registered any outer-script traps (`trap cleanup EXIT`, etc).
#
# This helper installs NO traps of its own and never calls `exit`. It returns
# the captured container exit code (or 2 if invoked with an empty mode).
#
# Info messages emitted by this helper use plain `echo` (the outer script's
# `log_message` function is not in scope inside the helper).

run_skill_metrics_recalc_once() {
    local mode="$1"
    local log_dir="$2"
    local timestamp="$3"
    local container_name="$4"

    # Reject empty mode — the helper must not fall back to ambient env.
    if [ -z "$mode" ]; then
        echo "ERROR: run_skill_metrics_recalc_once requires a non-empty mode as the first argument" >&2
        return 2
    fi

    local SERVICE_LOG="${log_dir}/${container_name}_${timestamp}.log"

    # macOS Docker compatibility
    local DOCKER_HOST_OVERRIDE=""
    if [[ "$(uname)" == "Darwin" ]]; then
        if [[ "$IEASYHYDROHF_HOST" == *"localhost"* ]]; then
            local DOCKER_IEASYHYDROHF_HOST="${IEASYHYDROHF_HOST//localhost/host.docker.internal}"
            echo "macOS detected: overriding IEASYHYDROHF_HOST for Docker container"
            echo "  Original: $IEASYHYDROHF_HOST"
            echo "  Docker:   $DOCKER_IEASYHYDROHF_HOST"
            DOCKER_HOST_OVERRIDE="-e IEASYHYDROHF_HOST=${DOCKER_IEASYHYDROHF_HOST}"
        fi
    fi

    # Image resolution — mirrors the outer script's IMAGE_ID. The image-existence
    # check and `docker pull` stay in the outer script (called once per
    # invocation, not once per mode).
    local IMAGE_ID="mabesa/sapphire-postprocessing:${ieasyhydroforecast_backend_docker_image_tag:-latest}"

    echo "Container name: $container_name"
    echo "Service log: $SERVICE_LOG"

    # Remove any existing container with the same name
    if docker ps -a --format '{{.Names}}' | grep -q "^${container_name}$"; then
        echo "Removing existing container: $container_name"
        docker rm -f "$container_name"
    fi

    # Run the skill metrics recalculation container
    docker run \
        --name "$container_name" \
        --network host \
        -e ieasyhydroforecast_data_root_dir=${ieasyhydroforecast_data_root_dir} \
        -e ieasyhydroforecast_env_file_path=${ieasyhydroforecast_env_file_path} \
        -e SAPPHIRE_OPDEV_ENV=True \
        -e IN_DOCKER=True \
        -e SAPPHIRE_PREDICTION_MODE=${mode} \
        ${DOCKER_HOST_OVERRIDE} \
        -v ${ieasyhydroforecast_data_ref_dir}/config:${ieasyhydroforecast_container_data_ref_dir}/config \
        -v ${ieasyhydroforecast_data_ref_dir}/intermediate_data:${ieasyhydroforecast_container_data_ref_dir}/intermediate_data \
        --memory=8g \
        --memory-swap=12g \
        ${IMAGE_ID} \
        uv run recalculate_skill_metrics.py \
        2>&1 | tee "$SERVICE_LOG"

    local EXIT_CODE=$?

    # Capture container exit code if different from tee exit code
    local CONTAINER_EXIT_CODE
    CONTAINER_EXIT_CODE=$(docker inspect "$container_name" --format='{{.State.ExitCode}}' 2>/dev/null || echo "$EXIT_CODE")

    if [ "$CONTAINER_EXIT_CODE" -eq 0 ]; then
        echo "Skill metrics recalculation completed successfully (mode=${mode})"
    else
        echo "WARNING: Skill metrics recalculation (mode=${mode}) completed with exit code: $CONTAINER_EXIT_CODE"
        echo "Check log file for details: $SERVICE_LOG"
    fi

    # Clean up the container
    docker rm -f "$container_name" 2>/dev/null

    return "$CONTAINER_EXIT_CODE"
}
