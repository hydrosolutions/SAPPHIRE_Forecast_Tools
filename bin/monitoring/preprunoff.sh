#!/bin/bash
# =============================================================================
# preprunoff.sh - Monitor preprocessing_runoff maintenance logs
# =============================================================================
#
# DESCRIPTION:
#   Analyzes preprocessing_runoff maintenance logs for:
#   - Site classification results (hydro-only, dual-type, meteo-only breakdown)
#   - Meteo-only site listings
#   - Success/error messages
#   - Spot-check validation results
#
# USAGE:
#   ./monitor_preprunoff_logs.sh [OPTIONS]
#
# OPTIONS:
#   -f, --file PATH      Log file path (default: /home/ubuntu/logs/sapphire_preprunoff_maintenance.log)
#   -n, --lines N        Number of recent lines to analyze (default: 500)
#   -t, --tail           Follow log file in real-time (like tail -f)
#   -c, --check SITE     Check if specific site appears in meteo-only (can repeat)
#   -a, --all            Show all matches, not just summary
#   -h, --help           Show this help message
#
# EXAMPLES:
#   ./monitor_preprunoff_logs.sh                         # Quick summary of recent logs
#   ./monitor_preprunoff_logs.sh -t                      # Live monitoring
#   ./monitor_preprunoff_logs.sh -c 16059 -c 15020      # Check if sites are misclassified
#   ./monitor_preprunoff_logs.sh -n 1000 -a             # Detailed analysis of last 1000 lines
#
# EXIT CODES:
#   0 - Success, no issues found
#   1 - Warnings found (specified site in meteo-only, or other warnings)
#   2 - Errors found in logs
#   3 - Log file not found or not readable
#
# MONITORED PATTERNS:
#   - Site classification: "[DATA] Site classification across ALL pages:"
#   - Meteo-only sites: "[DATA] Meteo-only sites"
#   - Spot-check results: "Spot-check"
#   - Errors: "ERROR", "Exception", "Traceback"
#   - Success: "completed successfully", "Maintenance mode complete"
#
# RELATED:
#   - GI PR-004: Pagination bug fix for station_type misclassification
#   - daily_preprunoff_maintenance.sh: The script that generates these logs
#
# =============================================================================

set -euo pipefail

# Configuration
DEFAULT_LOG_FILE="/home/ubuntu/logs/sapphire_preprunoff_maintenance.log"
DEFAULT_LINES=500

# Sites to check (populated via -c flag)
SITES_TO_CHECK=()

# Colors for output
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
LOG_FILE="$DEFAULT_LOG_FILE"
NUM_LINES="$DEFAULT_LINES"
TAIL_MODE=false
SHOW_ALL=false

usage() {
    head -45 "$0" | grep -E "^#" | sed 's/^# //' | sed 's/^#//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--file)
            LOG_FILE="$2"
            shift 2
            ;;
        -n|--lines)
            NUM_LINES="$2"
            shift 2
            ;;
        -t|--tail)
            TAIL_MODE=true
            shift
            ;;
        -c|--check)
            SITES_TO_CHECK+=("$2")
            shift 2
            ;;
        -a|--all)
            SHOW_ALL=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Check if log file exists
if [[ ! -f "$LOG_FILE" ]]; then
    echo -e "${RED}ERROR: Log file not found: $LOG_FILE${NC}"
    exit 3
fi

if [[ ! -r "$LOG_FILE" ]]; then
    echo -e "${RED}ERROR: Log file not readable: $LOG_FILE${NC}"
    exit 3
fi

echo -e "${BLUE}=== Preprocessing Runoff Log Monitor ===${NC}"
echo "Log file: $LOG_FILE"
echo "Analyzing: last $NUM_LINES lines"
echo ""

# Exit code tracking
EXIT_CODE=0

# -----------------------------------------------------------------------------
# Tail mode - live monitoring
# -----------------------------------------------------------------------------
if [[ "$TAIL_MODE" == true ]]; then
    echo -e "${BLUE}Live monitoring (Ctrl+C to stop)...${NC}"
    echo ""
    tail -f "$LOG_FILE" | grep --line-buffered -E \
        "(Site classification across ALL pages|Meteo-only sites|Spot-check|ERROR|Exception|completed successfully|Maintenance mode)"
    exit 0
fi

# -----------------------------------------------------------------------------
# Analysis mode
# -----------------------------------------------------------------------------

# Get recent lines
RECENT_LOGS=$(tail -n "$NUM_LINES" "$LOG_FILE")

# 1. Site Classification Summary
echo -e "${BLUE}--- Site Classification ---${NC}"
CLASSIFICATION=$(echo "$RECENT_LOGS" | grep -E "Site classification across ALL pages" | tail -5 || true)
if [[ -n "$CLASSIFICATION" ]]; then
    echo "$CLASSIFICATION"
else
    echo "No classification entries found in last $NUM_LINES lines"
fi
echo ""

# 2. Meteo-only Sites Check
echo -e "${BLUE}--- Meteo-only Sites ---${NC}"
METEO_ONLY=$(echo "$RECENT_LOGS" | grep -E "Meteo-only sites" | tail -5 || true)
if [[ -n "$METEO_ONLY" ]]; then
    if [[ "$SHOW_ALL" == true ]]; then
        echo "$METEO_ONLY"
    else
        echo "$METEO_ONLY" | tail -1
    fi

    # Check for specific sites if requested via -c flag
    if [[ ${#SITES_TO_CHECK[@]} -gt 0 ]]; then
        echo ""
        echo "Checking specified sites:"
        for site in "${SITES_TO_CHECK[@]}"; do
            if echo "$METEO_ONLY" | grep -q "'$site'"; then
                echo -e "${YELLOW}  $site: FOUND in meteo-only list${NC}"
                EXIT_CODE=1
            else
                echo -e "${GREEN}  $site: Not in meteo-only list (OK)${NC}"
            fi
        done
    fi
else
    echo "No meteo-only entries found (all sites have hydro data)"
fi
echo ""

# 3. Spot-check Results
echo -e "${BLUE}--- Spot-check Validation ---${NC}"
SPOT_CHECKS=$(echo "$RECENT_LOGS" | grep -E "Spot-check" | tail -10 || true)
if [[ -n "$SPOT_CHECKS" ]]; then
    if [[ "$SHOW_ALL" == true ]]; then
        echo "$SPOT_CHECKS"
    else
        # Show summary line and last few entries
        SUMMARY=$(echo "$SPOT_CHECKS" | grep -E "Result:" | tail -1 || true)
        if [[ -n "$SUMMARY" ]]; then
            echo "$SUMMARY"
        fi
        echo "$SPOT_CHECKS" | grep -v "Result:" | tail -3 || true
    fi
else
    echo "No spot-check entries found"
fi
echo ""

# 4. Error Messages
echo -e "${BLUE}--- Errors ---${NC}"
ERRORS=$(echo "$RECENT_LOGS" | grep -E "(ERROR|Exception|Traceback)" | tail -10 || true)
if [[ -n "$ERRORS" ]]; then
    echo -e "${RED}Errors found:${NC}"
    echo "$ERRORS"
    if [[ $EXIT_CODE -lt 2 ]]; then
        EXIT_CODE=2
    fi
else
    echo -e "${GREEN}No errors found${NC}"
fi
echo ""

# 5. Success Messages
echo -e "${BLUE}--- Success Messages ---${NC}"
SUCCESS=$(echo "$RECENT_LOGS" | grep -E "(completed successfully|Maintenance mode complete|Data fetch complete)" | tail -5 || true)
if [[ -n "$SUCCESS" ]]; then
    echo -e "${GREEN}$SUCCESS${NC}"
else
    echo "No success messages found in last $NUM_LINES lines"
fi
echo ""

# 6. Summary
echo -e "${BLUE}=== Summary ===${NC}"
case $EXIT_CODE in
    0)
        echo -e "${GREEN}Status: OK - No issues detected${NC}"
        ;;
    1)
        echo -e "${YELLOW}Status: WARNING - Check site classification above${NC}"
        ;;
    2)
        echo -e "${RED}Status: ERROR - Errors found in logs${NC}"
        ;;
esac

exit $EXIT_CODE
