#!/bin/bash
# Dedicated latest-only repair; never invoke the image's maintenance CMD.
# Usage: bash bin/run_latest_forecast_recovery.sh <env> [--reuse-existing-tunnel]
umask 077
source "$(dirname "$0")/utils/common_functions.sh"
REUSE_TUNNEL=false
if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    echo "Usage: $0 <env> [--reuse-existing-tunnel]" >&2
    exit 2
fi
if [ "$#" -eq 2 ]; then
    [ "$2" = --reuse-existing-tunnel ] || exit 2
    REUSE_TUNNEL=true
fi
read_configuration "$1"
FORECAST_DATE=$(date +%F) || exit 1
case "${ieasyhydroforecast_run_ML_models:-false}" in
    true|True|TRUE) ;; *) echo '| Latest recovery skipped: ML disabled.'; exit 0 ;;
esac
API_ENABLED=$(printf '%s' "${SAPPHIRE_API_ENABLED-true}" | tr '[:upper:]' '[:lower:]')
if [ "$API_ENABLED" != true ]; then
    echo '| Latest recovery skipped: API disabled.'
    exit 0
fi
case "${ML_FAILED_ISSUE_RECOVERY_ENABLED:-true}" in
    false|False|FALSE) echo '| Latest recovery skipped: recovery disabled.'; exit 0 ;;
esac
case "${SAPPHIRE_TEST_ENV:-false}" in true|True|TRUE) echo '| Latest recovery skipped: test mode.'; exit 0 ;; esac

# Match pipeline_docker.py's deployment-relative bind convention.
MOUNTS=()
for key in ieasyforecast_configuration_path ieasyforecast_intermediate_data_path; do
    value=${!key}
    if [ -z "$value" ]; then
        echo "| Latest recovery: required path $key is missing." >&2
        exit 1
    fi
    bind_path=${value#../../..}
    case "$bind_path" in /*) ;; *) echo "| Latest recovery: invalid path for $key." >&2; exit 1 ;; esac
    host_path="${ieasyhydroforecast_data_root_dir}${bind_path}"
    mode=rw
    [ "$key" = ieasyforecast_configuration_path ] && mode=ro
    MOUNTS+=(-v "${host_path}:${bind_path}:${mode}")
    if [ "$key" = ieasyforecast_intermediate_data_path ]; then
        INTERMEDIATE_HOST=$host_path
    fi
done
# A host lock shared by all dedicated invocations, independent of date/model.
# Never remove a pre-existing lock: after a crash the operator checks containers
# have stopped before removing the empty lock directory and retrying.
mkdir -p "$INTERMEDIATE_HOST" || exit 1
LOCK_DIR="$INTERMEDIATE_HOST/.latest_forecast_recovery.lock"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
    echo '| Latest recovery locked: another run or an uncleared interrupted run.' >&2
    exit 1
fi
finish_recovery() {
    [ "$REUSE_TUNNEL" = true ] || cleanup
    rmdir "$LOCK_DIR"
}
trap finish_recovery EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
if [ "$REUSE_TUNNEL" = false ]; then
    unset ieasyhydroforecast_ssh_tunnel_pid
    establish_ssh_tunnel || exit 1
fi
DOCKER_ENV=(-e "ieasyhydroforecast_env_file_path=$ieasyhydroforecast_env_file_path"
    -e SAPPHIRE_OPDEV_ENV=True -e IN_DOCKER=True -e SAPPHIRE_PREDICTION_MODE=BOTH)
if [ "$(uname -s)" = Darwin ] && [[ "${IEASYHYDROHF_HOST:-}" == *localhost* ]]; then
    DOCKER_ENV+=(-e "IEASYHYDROHF_HOST=${IEASYHYDROHF_HOST//localhost/host.docker.internal}")
fi
RECOVERY_API_URL=${SAPPHIRE_API_URL:-http://localhost:8000}
if [ "$(uname -s)" = Darwin ] && [[ "$RECOVERY_API_URL" == *localhost* ]]; then
    DOCKER_ENV+=(-e "SAPPHIRE_API_URL=${RECOVERY_API_URL//localhost/host.docker.internal}")
fi
OVERALL_STATUS=0
IFS=',' read -r -a MODELS <<< "${ieasyhydroforecast_available_ML_models:-TFT,TIDE,TSMIXER}"
for MODEL in "${MODELS[@]}"; do
    MODEL=$(printf '%s' "$MODEL" | tr '[:lower:]' '[:upper:]' | tr -d '[:space:]')
    case "$MODEL" in
        TFT|TIDE|TSMIXER) ;;
        *) echo '| Latest recovery: skipping unsupported configured ML model.'; continue ;;
    esac
    docker run --rm --network host "${MOUNTS[@]}" "${DOCKER_ENV[@]}" \
        -e "SAPPHIRE_MODEL_TO_USE=$MODEL" \
        "mabesa/sapphire-ml:${ieasyhydroforecast_backend_docker_image_tag}" \
        uv run recover_failed_forecasts.py --latest --forecast-date "$FORECAST_DATE" \
        --models "$MODEL" --configured-stations --execute
    STATUS=$?
    [ "$STATUS" -eq 0 ] || OVERALL_STATUS=1
done
# Drain verified results even when a producer failed. No normal PP or skills.
docker run --rm --network host "${MOUNTS[@]}" "${DOCKER_ENV[@]}" \
    "mabesa/sapphire-postprocessing:${ieasyhydroforecast_backend_docker_image_tag}" \
    uv run recover_latest_periods.py --forecast-date "$FORECAST_DATE"
STATUS=$?
[ "$STATUS" -eq 0 ] || OVERALL_STATUS=1
exit "$OVERALL_STATUS"
