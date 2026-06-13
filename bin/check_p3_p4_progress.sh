#!/usr/bin/env bash
# check_p3_p4_progress.sh — read-only progress/completion check for backfill
# runbook phases P3 (snow stats + norms) and P4 (LR PENTAD/DECAD hindcasts).
#
# Reports, for each phase: whether its process is still running, its log
# completion markers, and the DB acceptance counts. Read-only (pgrep / cat /
# grep / docker ps / SELECT only) — safe to run any time on a deployment server.
#
# Usage (on the server):
#   load_backfill_env            # sets $ieasyhydroforecast_data_root_dir and START_DATE
#   bash bin/check_p3_p4_progress.sh
#
# Optional overrides (if your paths / container names differ):
#   DATA_ROOT=/data START_DATE=2010-01-01 \
#   PREP_DB=sapphire-preprocessing-db POST_DB=sapphire-postprocessing-db \
#     bash bin/check_p3_p4_progress.sh
#
# Notes:
#   - GROUP BY all horizon/snow types (no enum-literal WHERE) to avoid the
#     uppercase-PG-enum mismatch (MIG-003 class).
#   - P3's wrapper exits 1 even on full success (unbound ssh_tunnel_pid cleanup
#     quirk) — trust the ledger + "all years processed" marker, not exit code.

DATA_ROOT="${DATA_ROOT:-${ieasyhydroforecast_data_root_dir:-}}"
START_DATE="${START_DATE:-${ieasyhydroforecast_START_DATE:-}}"
START_YEAR="${START_DATE%%-*}"
PREP_DB="${PREP_DB:-sapphire-preprocessing-db}"
POST_DB="${POST_DB:-sapphire-postprocessing-db}"

# Run a single read-only SQL statement against a containerised Postgres.
psql_q() {  # $1=container $2=db_name $3=sql
  local user
  user="$(docker exec "$1" printenv POSTGRES_USER 2>/dev/null)"
  if [ -z "$user" ]; then
    echo "    (could not read POSTGRES_USER from container '$1' — is it running?)"
    return 1
  fi
  docker exec -i "$1" psql -U "$user" -d "$2" -P pager=off -c "$3" 2>&1 | sed 's/^/    /'
}

echo "================================================================"
echo " P3 / P4 progress check   ($(date -u +%Y-%m-%dT%H:%M:%SZ) UTC)"
echo " DATA_ROOT=${DATA_ROOT:-<unset>}   START_DATE=${START_DATE:-<unset>}"
echo "================================================================"

echo
echo "===== Are P3/P4 processes still running? ====="
pgrep -af 'backfill_snow_stats_history|recalculate_snow_norms|initialize_site_backfill' \
  || echo "  (no P3/P4 host processes running)"
docker ps --format '{{.Names}}\t{{.Image}}\t{{.Status}}' 2>/dev/null \
  | grep -iE 'prepgateway|linreg' \
  || echo "  (no prepgateway/linreg backfill containers running)"

# ---------------------------------------------------------------- P3
echo
echo "===== P3 — snow stats / norms ====="
SNOW_LOG_DIR="${DATA_ROOT}/logs/snow_stat_backfill"
if [ -n "$DATA_ROOT" ] && [ -d "$SNOW_LOG_DIR" ]; then
  PROG="${SNOW_LOG_DIR}/backfill_progress.txt"
  if [ -f "$PROG" ]; then
    COMPLETED="$(sort -u "$PROG" | wc -l | tr -d ' ')"
    if [ -n "$START_YEAR" ]; then
      EXPECTED="$(( $(date +%Y) - START_YEAR ))"
      echo "  progress ledger: completed=${COMPLETED}  expected=${EXPECTED}"
      if [ "$COMPLETED" -ge "$EXPECTED" ]; then
        echo "  -> ledger COMPLETE"
      else
        echo "  -> ledger INCOMPLETE"
      fi
    else
      echo "  progress ledger: completed=${COMPLETED}  (START_YEAR unknown — run load_backfill_env to compare)"
    fi
  else
    echo "  no backfill_progress.txt yet"
  fi
  if grep -qil "all years processed" "$SNOW_LOG_DIR"/p3_snow_stats_outer_*.log 2>/dev/null; then
    echo "  outer log: 'all years processed' marker FOUND"
  else
    echo "  outer log: 'all years processed' marker NOT yet present"
  fi
  echo "  (P3 wrapper exits 1 even on success — judge by ledger + marker, not exit code)"
else
  echo "  snow_stat_backfill log dir not found (${SNOW_LOG_DIR:-<DATA_ROOT unset>})"
  echo "  -> run 'load_backfill_env' first, or pass DATA_ROOT=..."
fi
echo "  --- DB: snow norm/stat coverage (preprocessing) ---"
psql_q "$PREP_DB" preprocessing_db \
  "SELECT snow_type, COUNT(*) rows, COUNT(DISTINCT code) hru, MIN(date) min_date, MAX(date) max_date, COUNT(*) FILTER (WHERE norm IS NOT NULL) norm, COUNT(*) FILTER (WHERE mean IS NOT NULL) mean, COUNT(*) FILTER (WHERE q05 IS NOT NULL) q05, COUNT(*) FILTER (WHERE q95 IS NOT NULL) q95 FROM snow GROUP BY snow_type ORDER BY snow_type"

# ---------------------------------------------------------------- P4
echo
echo "===== P4 — LR PENTAD/DECAD hindcasts ====="
SITE_LOG_DIR="${DATA_ROOT}/logs/site_backfill"
if [ -n "$DATA_ROOT" ] && [ -d "$SITE_LOG_DIR" ]; then
  LATEST="$(ls -t "$SITE_LOG_DIR"/p4_lr_outer_*.log 2>/dev/null | head -1)"
  if [ -n "$LATEST" ]; then
    echo "  latest outer log: $LATEST"
    if grep -qi "Backfill complete" "$LATEST"; then
      echo "  -> 'Backfill complete' FOUND"
    else
      echo "  -> 'Backfill complete' NOT yet present (still running or failed)"
    fi
    grep -iE "PASS|FAIL|SKIP|Phase 2|BACKFILL SUMMARY|run finished" "$LATEST" | tail -8 | sed 's/^/    /'
  else
    echo "  no p4_lr_outer_*.log found"
  fi
else
  echo "  site_backfill log dir not found (${SITE_LOG_DIR:-<DATA_ROOT unset>})"
  echo "  -> run 'load_backfill_env' first, or pass DATA_ROOT=..."
fi
echo "  --- DB: LR forecasts (postprocessing) ---"
psql_q "$POST_DB" postprocessing_db \
  "SELECT horizon_type, COUNT(*) rows, COUNT(DISTINCT code) sites, MIN(date) min_issue, MAX(date) max_issue FROM lr_forecasts GROUP BY horizon_type ORDER BY horizon_type"
echo "  --- DB: pentad/decad hydrographs (preprocessing) ---"
psql_q "$PREP_DB" preprocessing_db \
  "SELECT horizon_type, COUNT(*) rows, COUNT(DISTINCT code) sites, MAX(date) max_date, COUNT(*) FILTER (WHERE mean IS NOT NULL) mean FROM hydrographs GROUP BY horizon_type ORDER BY horizon_type"

# ---------------------------------------------------------------- verdict guide
echo
echo "===== How to read it ====="
echo "  P3 DONE  : ledger completed>=expected AND 'all years processed' marker AND"
echo "             snow types show non-null norm + stat columns (mean/q05/q95)."
echo "  P4 DONE  : 'Backfill complete' + PASS Phase 2 in the log, AND lr_forecasts has"
echo "             BOTH a PENTAD and a DECADE row (max_issue at today/last boundary),"
echo "             AND hydrographs shows PENTAD + DECADE rows with mean populated."
