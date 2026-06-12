#!/usr/bin/env bash
# check_quarter_hydrograph.sh — PREPQ-008 read-only diagnostic.
#
# Confirms whether the quarterly long-horizon hydrograph write landed, and scans
# the P9 (runoff hydrograph aggregation) logs for failure signatures.
#
# Read-only: it only runs SELECT queries and greps logs. Safe on any deployment.
#
# Usage (on a deployment server, from the repo root or anywhere):
#   bash bin/check_quarter_hydrograph.sh
#
# Optional overrides if your container/DB names differ:
#   DB_CONTAINER=sapphire-preprocessing-db DB_NAME=preprocessing_db \
#     bash bin/check_quarter_hydrograph.sh
#
# If the log section says the dir is missing, run 'load_backfill_env' first so
# $ieasyhydroforecast_data_root_dir is set, then re-run.

DB_CONTAINER="${DB_CONTAINER:-sapphire-preprocessing-db}"
DB_NAME="${DB_NAME:-preprocessing_db}"

PGUSER="$(docker exec "$DB_CONTAINER" printenv POSTGRES_USER 2>/dev/null)"
if [ -z "$PGUSER" ]; then
  echo "ERROR: could not read POSTGRES_USER from container '$DB_CONTAINER'." >&2
  echo "       Set DB_CONTAINER=<name> and retry." >&2
  exit 1
fi

echo "============ hydrographs by horizon_type (look for the QUARTER row) ============"
docker exec -i "$DB_CONTAINER" psql -U "$PGUSER" -d "$DB_NAME" -P pager=off <<'SQL'
SELECT horizon_type,
       COUNT(*)                                     AS rows,
       COUNT(DISTINCT code)                          AS sites,
       MIN(date) AS min_date, MAX(date) AS max_date,
       COUNT(*) FILTER (WHERE norm IS NOT NULL)      AS norm_rows,
       COUNT(*) FILTER (WHERE previous IS NOT NULL)  AS prev_rows,
       COUNT(*) FILTER (WHERE current IS NOT NULL)   AS curr_rows
FROM hydrographs
GROUP BY horizon_type
ORDER BY horizon_type;
SQL

echo
echo "============ P9 logs: error signatures vs quarterly writes ============"
LOG_DIR="${ieasyhydroforecast_data_root_dir:-}/logs/runoff_hydrograph_aggregation"
if [ -d "$LOG_DIR" ]; then
  if grep -rqiE "422|invalid input value for enum|Failed at batch" "$LOG_DIR"; then
    echo ">>> FOUND error signatures (last 40 matching lines):"
    grep -rniE "422|invalid input value for enum|Failed at batch" "$LOG_DIR" | tail -40
  else
    echo ">>> No 422 / enum-error / batch-failure signatures — clean."
  fi
  echo "----- quarterly-write log lines (last 20) -----"
  grep -rniE "quarter" "$LOG_DIR" | tail -20 || echo "(none)"
else
  echo "Log dir not found: $LOG_DIR"
  echo "Run 'load_backfill_env' first (sets \$ieasyhydroforecast_data_root_dir), then re-run."
fi
