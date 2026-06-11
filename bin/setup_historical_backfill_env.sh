#!/usr/bin/env bash
# Configure the current shell for the historical backfill runbook.
#
# IMPORTANT: source this file. Running it as `bash ...` cannot export variables
# or functions back to your interactive shell.
#
# Examples:
#   source bin/setup_historical_backfill_env.sh --profile taj
#   source bin/setup_historical_backfill_env.sh --profile taj --local
#   source bin/setup_historical_backfill_env.sh --env-file ~/Documents/GitHub/taj_data_forecast_tools/config/.env_develop_tjhm

_hb_is_sourced() {
  if [ -n "${BASH_VERSION:-}" ]; then
    [ "${BASH_SOURCE[0]}" != "$0" ]
    return
  fi
  case "${ZSH_EVAL_CONTEXT:-}" in
    *:file) return 0 ;;
  esac
  return 1
}

if ! _hb_is_sourced; then
  echo "ERROR: source this script so it can configure your current shell."
  echo "Example: source bin/setup_historical_backfill_env.sh --profile taj --local"
  exit 2
fi

_hb_usage() {
  cat <<'USAGE'
Usage: source bin/setup_historical_backfill_env.sh [OPTIONS]

Configure shell variables/functions for doc/prod/historical_backfill_runbook.md.

Options:
  --profile taj|uzb|kyg       Data profile. Defaults can be inferred from --env-file.
  --org tjhm|uzhm|kghm        Organization slug. Default by profile: taj=tjhm,
                              uzb=uzhm, kyg=kghm.
  --local                     Use $HOME/Documents/GitHub/<profile>_data_forecast_tools.
                              Without --local, default data dir is /data/<profile>_data_forecast_tools.
  --data-dir PATH             Explicit data directory.
  --env-file PATH             Explicit forecast-tools env file. Supports leading ~/.
  --compose-env-file PATH     Env file for DB/compose backup variables. Defaults to
                              <repo>/sapphire/.env.
  --repo PATH                 Repository root. Defaults to this script's parent directory.
  --log-dir PATH              Operator log root. Defaults to the data-root log
                              directory, i.e. dirname(DATA_DIR)/logs.
  --sample-code CODE          Safe sample code for examples. Default: 19999.
  --no-load                   Only export variables and define load_backfill_env; do
                              not call read_configuration immediately.
  -h, --help                  Show this help.

Examples:
  source bin/setup_historical_backfill_env.sh --profile taj
  source bin/setup_historical_backfill_env.sh --profile taj --local
  source bin/setup_historical_backfill_env.sh --env-file ~/Documents/GitHub/taj_data_forecast_tools/config/.env_develop_tjhm
USAGE
}

_hb_expand_path() {
  case "$1" in
    \~) printf "%s\n" "$HOME" ;;
    \~/*) printf "%s/%s\n" "$HOME" "${1#\~/}" ;;
    *) printf "%s\n" "$1" ;;
  esac
}

_hb_org_for_profile() {
  case "$1" in
    taj) printf "%s\n" "tjhm" ;;
    uzb) printf "%s\n" "uzhm" ;;
    kyg) printf "%s\n" "kghm" ;;
    *) return 1 ;;
  esac
}

_hb_profile_for_org() {
  case "$1" in
    tjhm) printf "%s\n" "taj" ;;
    uzhm) printf "%s\n" "uzb" ;;
    kghm) printf "%s\n" "kyg" ;;
    *) return 1 ;;
  esac
}

_hb_infer_org_from_env() {
  case "$1" in
    *kghm) printf "%s\n" "kghm" ;;
    *tjhm) printf "%s\n" "tjhm" ;;
    *uzhm) printf "%s\n" "uzhm" ;;
    *) return 1 ;;
  esac
}

_hb_infer_profile_from_data_dir() {
  local base
  base="$(basename "$1")"
  case "$base" in
    taj_data_forecast_tools|taj_data) printf "%s\n" "taj" ;;
    uzb_data_forecast_tools|uzb_data) printf "%s\n" "uzb" ;;
    kyg_data_forecast_tools|kyg_data) printf "%s\n" "kyg" ;;
    *) return 1 ;;
  esac
}

if [ -n "${BASH_VERSION:-}" ]; then
  _hb_script_path="${BASH_SOURCE[0]}"
else
  _hb_script_path="$0"
fi
case "$_hb_script_path" in
  /*) ;;
  *) _hb_script_path="$(pwd)/$_hb_script_path" ;;
esac
_hb_default_repo="$(cd "$(dirname "$_hb_script_path")/.." && pwd)"

_hb_profile=""
_hb_org=""
_hb_local=false
_hb_data_dir=""
_hb_env_file=""
_hb_compose_env_file=""
_hb_repo="$_hb_default_repo"
_hb_log_dir=""
_hb_sample_code="19999"
_hb_load=true

while [ "$#" -gt 0 ]; do
  case "$1" in
    --profile)
      [ "$#" -ge 2 ] || { echo "ERROR: --profile requires a value"; return 2; }
      _hb_profile="$2"
      shift 2
      ;;
    --org)
      [ "$#" -ge 2 ] || { echo "ERROR: --org requires a value"; return 2; }
      _hb_org="$2"
      shift 2
      ;;
    --local)
      _hb_local=true
      shift
      ;;
    --data-dir)
      [ "$#" -ge 2 ] || { echo "ERROR: --data-dir requires a value"; return 2; }
      _hb_data_dir="$(_hb_expand_path "$2")"
      shift 2
      ;;
    --env-file)
      [ "$#" -ge 2 ] || { echo "ERROR: --env-file requires a value"; return 2; }
      _hb_env_file="$(_hb_expand_path "$2")"
      shift 2
      ;;
    --compose-env-file)
      [ "$#" -ge 2 ] || { echo "ERROR: --compose-env-file requires a value"; return 2; }
      _hb_compose_env_file="$(_hb_expand_path "$2")"
      shift 2
      ;;
    --repo)
      [ "$#" -ge 2 ] || { echo "ERROR: --repo requires a value"; return 2; }
      _hb_repo="$(_hb_expand_path "$2")"
      shift 2
      ;;
    --log-dir)
      [ "$#" -ge 2 ] || { echo "ERROR: --log-dir requires a value"; return 2; }
      _hb_log_dir="$(_hb_expand_path "$2")"
      shift 2
      ;;
    --sample-code)
      [ "$#" -ge 2 ] || { echo "ERROR: --sample-code requires a value"; return 2; }
      _hb_sample_code="$2"
      shift 2
      ;;
    --no-load)
      _hb_load=false
      shift
      ;;
    -h|--help)
      _hb_usage
      return 0
      ;;
    *)
      echo "ERROR: unknown option: $1"
      _hb_usage
      return 2
      ;;
  esac
done

if [ -n "$_hb_env_file" ]; then
  if [ -z "$_hb_org" ]; then
    _hb_org="$(_hb_infer_org_from_env "$_hb_env_file" 2>/dev/null || true)"
  fi
  if [ -z "$_hb_data_dir" ]; then
    if _hb_data_dir="$(cd "$(dirname "$_hb_env_file")/.." 2>/dev/null && pwd)"; then
      :
    else
      _hb_data_dir="$(dirname "$(dirname "$_hb_env_file")")"
    fi
  fi
  if [ -z "$_hb_profile" ]; then
    _hb_profile="$(_hb_infer_profile_from_data_dir "$_hb_data_dir" 2>/dev/null || true)"
  fi
fi

if [ -z "$_hb_profile" ] && [ -n "$_hb_org" ]; then
  _hb_profile="$(_hb_profile_for_org "$_hb_org" 2>/dev/null || true)"
fi

if [ -z "$_hb_profile" ]; then
  echo "ERROR: --profile is required unless it can be inferred from --env-file."
  _hb_usage
  return 2
fi

case "$_hb_profile" in
  taj|uzb|kyg) ;;
  *)
    echo "ERROR: unsupported profile: $_hb_profile (expected taj, uzb, or kyg)"
    return 2
    ;;
esac

if [ -z "$_hb_org" ]; then
  _hb_org="$(_hb_org_for_profile "$_hb_profile")"
fi

case "$_hb_org" in
  tjhm|uzhm|kghm) ;;
  *)
    echo "ERROR: unsupported org: $_hb_org (expected tjhm, uzhm, or kghm)"
    return 2
    ;;
esac

if [ -z "$_hb_data_dir" ]; then
  if [ "$_hb_local" = true ]; then
    _hb_data_dir="$HOME/Documents/GitHub/${_hb_profile}_data_forecast_tools"
  else
    _hb_data_dir="/data/${_hb_profile}_data_forecast_tools"
  fi
fi

case "$_hb_data_dir" in
  "$HOME"/*) _hb_local=true ;;
esac

if [ -z "$_hb_env_file" ]; then
  if [ -f "${_hb_data_dir}/config/.env_${_hb_org}" ]; then
    _hb_env_file="${_hb_data_dir}/config/.env_${_hb_org}"
  elif [ -f "${_hb_data_dir}/config/.env_develop_${_hb_org}" ]; then
    _hb_env_file="${_hb_data_dir}/config/.env_develop_${_hb_org}"
  else
    _hb_env_file="${_hb_data_dir}/config/.env_${_hb_org}"
  fi
fi

if [ -z "$_hb_compose_env_file" ]; then
  _hb_compose_env_file="${_hb_repo}/sapphire/.env"
fi

if [ -z "$_hb_log_dir" ]; then
  _hb_log_dir="$(dirname "$_hb_data_dir")/logs"
fi

export DATA_PROFILE="$_hb_profile"
export ORG_SLUG="$_hb_org"
export DATA_DIR="$_hb_data_dir"
export ENV_FILE_PATH="$_hb_env_file"
export COMPOSE_ENV_FILE="$_hb_compose_env_file"
export LOG_DIR="$_hb_log_dir"
export REPO="$_hb_repo"
export ENV_FILE="$_hb_env_file"
export SAMPLE_CODE="$_hb_sample_code"
export TODAY_UTC
TODAY_UTC="$(date -u +%F)"

load_backfill_env() {
  if [ -z "${ENV_FILE:-}" ]; then
    echo "ERROR: ENV_FILE is not set. Re-run setup_historical_backfill_env.sh."
    return 1
  fi

  case "$ENV_FILE" in
    \~) ENV_FILE="$HOME" ;;
    \~/*) ENV_FILE="$HOME/${ENV_FILE#\~/}" ;;
  esac
  ENV_FILE_PATH="${ENV_FILE_PATH:-$ENV_FILE}"
  case "$ENV_FILE_PATH" in
    \~) ENV_FILE_PATH="$HOME" ;;
    \~/*) ENV_FILE_PATH="$HOME/${ENV_FILE_PATH#\~/}" ;;
  esac
  COMPOSE_ENV_FILE="${COMPOSE_ENV_FILE:-$REPO/sapphire/.env}"
  case "$COMPOSE_ENV_FILE" in
    \~) COMPOSE_ENV_FILE="$HOME" ;;
    \~/*) COMPOSE_ENV_FILE="$HOME/${COMPOSE_ENV_FILE#\~/}" ;;
  esac
  export ENV_FILE ENV_FILE_PATH COMPOSE_ENV_FILE

  if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: ENV_FILE does not exist: $ENV_FILE"
    echo "Use --env-file or check --profile/--org/--data-dir."
    return 1
  fi
  case "$ENV_FILE" in
    *kghm|*tjhm|*uzhm) ;;
    *)
      echo "ERROR: ENV_FILE must end in kghm, tjhm, or uzhm for common_functions.sh."
      echo "Got: $ENV_FILE"
      return 1
      ;;
  esac
  if [ ! -f "$REPO/bin/utils/common_functions.sh" ]; then
    echo "ERROR: common_functions.sh not found under REPO=$REPO"
    return 1
  fi

  # shellcheck source=bin/utils/common_functions.sh
  source "$REPO/bin/utils/common_functions.sh"
  read_configuration "$ENV_FILE"
  export START_DATE="${ieasyhydroforecast_START_DATE:?ieasyhydroforecast_START_DATE missing}"
  export BACKEND_TAG="${ieasyhydroforecast_backend_docker_image_tag:-local}"
}

if [ "$_hb_load" = true ]; then
  load_backfill_env || return $?
fi

cat <<EOF
Historical backfill shell configured.
  DATA_PROFILE=$DATA_PROFILE
  ORG_SLUG=$ORG_SLUG
  DATA_DIR=$DATA_DIR
  ENV_FILE=$ENV_FILE
  COMPOSE_ENV_FILE=$COMPOSE_ENV_FILE
  REPO=$REPO
  LOG_DIR=$LOG_DIR
  START_DATE=${START_DATE:-unset}
  BACKEND_TAG=${BACKEND_TAG:-unset}

Run phases from the runbook with:
  cd "\$REPO"
  load_backfill_env
EOF

unset _hb_profile _hb_org _hb_local _hb_data_dir _hb_env_file _hb_compose_env_file
unset _hb_repo _hb_log_dir _hb_sample_code _hb_load _hb_script_path _hb_default_repo
unset -f _hb_is_sourced _hb_usage _hb_expand_path _hb_org_for_profile
unset -f _hb_profile_for_org _hb_infer_org_from_env _hb_infer_profile_from_data_dir
