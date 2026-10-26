#!/bin/bash

# =============================================================================
# SAPPHIRE server update (interactive)
# =============================================================================
#
# Updates a SAPPHIRE Forecast Tools production server (Ubuntu + Docker) to the
# latest code, step by step. Before every step the script explains what it is
# about to do, prints the exact commands and asks for confirmation. It is safe
# to answer "n" to skip any step, and "q" to stop at any time.
# The script NEVER deletes Docker volumes (no "down -v", no "volume rm", no
# "system prune") and never removes the crontab.
#
# Usage (recommended: copy this script OUTSIDE the repository, e.g. scp it to ~/):
#   bash ~/update_sapphire_server.sh <path-to-env-file>
#   REPO_DIR=/path/to/SAPPHIRE_Forecast_Tools bash ~/update_sapphire_server.sh <env>
#   e.g. bash ~/update_sapphire_server.sh /data/<data_dir>/config/.env_develop_<org>
# Why outside the repo: an untracked copy inside bin/ would make "git pull"
# refuse to overwrite it once the file is committed upstream.
# The repository is found via $REPO_DIR, else the parent of this script's
# directory (if it holds sapphire/docker-compose.yml), else
# /data/SAPPHIRE_Forecast_Tools.
#
# Steps:
#   0. Pre-flight checks (time of day, git branch, image tags, docker, sudo)
#   1. Back up the env file, crontab, git commit, image list and the 4 databases
#   2. Get the new code (git pull, optionally discarding local changes)
#   3. Check the .env template for new settings and edit the env file
#   4. Delete the old pipeline images (mabesa/sapphire-*)
#   5. Rebuild the services, restart the stack, wait until it is ready,
#      make sure the Luigi daemon is running
#   6. Update the databases (alembic upgrade head)
#   7. Run the main jobs once to test (one question per job)
#   8. One-time long-term skill recalculation (long-term deployments only)
#   9. Check the dashboard and the containers
#  10. Check that the cron jobs are still installed
#
# Everything is logged to $HOME/backups/sapphire_<timestamp>/update.log.
# Answers are read from /dev/tty because stdout is piped through tee.
#
# Note: this script pulls new code with git, which may overwrite this very
# file while bash is running it. Therefore all logic lives in main() and the
# last line calls it, so bash has parsed the whole file before running.
# =============================================================================

set -u
set -o pipefail

PRODUCTION_BRANCH="maxat_sapphire_2"
TOTAL_STEPS=10

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

header() {
    echo
    echo "================================================================"
    echo "$1"
    echo "================================================================"
}

info() { echo "==> $*"; }
warn() { echo "WARNING: $*"; }
err()  { echo "ERROR: $*"; }

# Read one line from the terminal (the script's stdout goes through tee).
read_tty() {
    local answer=""
    if ! read -r answer < /dev/tty; then
        echo
        err "Cannot read from the terminal (/dev/tty). Run this script in an interactive shell."
        exit 1
    fi
    printf '%s' "$answer" | tr '[:upper:]' '[:lower:]'
}

# Plain yes/no question. Returns 0 for yes, 1 for no.
ask_yes_no() {
    local a
    while true; do
        printf '%s [y/n] ' "$1"
        a="$(read_tty)"
        case "$a" in
            y|yes) return 0 ;;
            n|no)  return 1 ;;
            *) echo "Please answer y or n." ;;
        esac
    done
}

# Step question. Returns 0 = run, 1 = skip, 2 = quit.
ask_step() {
    local a
    while true; do
        printf 'Run this step? [y]es / [n]o, skip / [q]uit '
        a="$(read_tty)"
        case "$a" in
            y|yes) return 0 ;;
            n|no)  return 1 ;;
            q|quit) return 2 ;;
            *) echo "Please answer y, n or q." ;;
        esac
    done
}

# Echo a command, run it, return its status. A failure marks the step failed.
STEP_FAILED=0
run_cmd() {
    local rc
    echo "+ $1"
    eval "$1"
    rc=$?
    if [ "$rc" -ne 0 ]; then
        err "Command failed with exit code $rc: $1"
        STEP_FAILED=1
    fi
    return "$rc"
}

# Like run_cmd but the exit status is ignored (used where errors are expected).
run_cmd_ignore() {
    echo "+ $1"
    eval "$1"
    return 0
}

where_things_are() {
    echo "Backup/log directory: $B"
    echo "Log file:             $B/update.log"
    if [ "$DB_BACKUP_DONE" -eq 1 ]; then
        echo "Database backups:     $DB_BACKUP_DIR"
    else
        echo "Database backups:     (not created in this run)"
    fi
}

rollback_hint() {
    echo "Rollback hint:"
    echo "  git checkout $OLD_COMMIT"
    echo "  cp \"$B/$(basename "$ENV")\" \"$ENV\""
    echo "  then rerun step 5 (build + restart)."
    echo "  Restore the databases from $DB_BACKUP_DIR only if data got damaged"
    echo "  (see bin/reset_sapphire_db.sh --help)."
}

# Print a step header and ask. Runs the step function when confirmed.
# Args: number, title, explanation, commands text, function name
run_step() {
    local n="$1" title="$2" explain="$3" cmds="$4" fn="$5" rc a
    header "STEP $n/$TOTAL_STEPS: $title"
    echo "$explain"
    echo
    echo "Commands that will run:"
    echo "$cmds" | sed 's/^/  /'
    echo
    ask_step
    rc=$?
    if [ "$rc" -eq 1 ]; then
        echo "Skipped."
        return 0
    elif [ "$rc" -eq 2 ]; then
        echo "Quitting at your request."
        where_things_are
        exit 0
    fi
    STEP_FAILED=0
    "$fn"
    if [ "$STEP_FAILED" -ne 0 ]; then
        err "Step $n had a failure (see above)."
        if ! ask_yes_no "Continue to the next step anyway?"; then
            where_things_are
            rollback_hint
            exit 1
        fi
    else
        info "Step $n finished."
    fi
    return 0
}

show_tags() {
    grep -E '^[[:space:]]*(export[[:space:]]+)?ieasyhydroforecast_(backend|frontend)_docker_image_tag' "$ENV" \
        || echo "(no docker_image_tag lines found in $ENV)"
}

tag_value() {
    grep -E "^[[:space:]]*(export[[:space:]]+)?ieasyhydroforecast_${1}_docker_image_tag" "$ENV" \
        | tail -n 1 | sed -E 's/^[^=]*=//; s/[[:space:]#].*$//; s/["'"'"']//g'
}

# Poll a URL until it answers. Args: url, timeout seconds, interval seconds.
poll_url() {
    local url="$1" timeout="$2" interval="$3" waited=0
    while [ "$waited" -lt "$timeout" ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            echo "READY: $url answers after ${waited}s"
            return 0
        fi
        sleep "$interval"
        waited=$((waited + interval))
    done
    echo "NOT READY: $url did not answer within ${timeout}s"
    return 1
}

# ---------------------------------------------------------------------------
# Step bodies
# ---------------------------------------------------------------------------

step1_body() {
    run_cmd 'cp "$ENV" "$B/"'
    run_cmd_ignore 'crontab -l > "$B/crontab.txt" 2>/dev/null || { echo "(no crontab installed)"; : > "$B/crontab.txt"; }'
    run_cmd 'git rev-parse HEAD > "$B/git_commit.txt"'
    run_cmd_ignore 'docker images | grep -E "sapphire|mabesa" > "$B/docker_images.txt"'
    OLD_COMMIT="$(cat "$B/git_commit.txt" 2>/dev/null || echo "$OLD_COMMIT")"
    DB_BACKUP_DIR="/var/backups/sapphire/pre_update_$(date +%Y%m%d_%H%M%S)"
    run_cmd 'sudo mkdir -p "$DB_BACKUP_DIR"' || return
    run_cmd 'sudo chown "$USER" "$DB_BACKUP_DIR"' || return
    DB_BACKUP_DONE=1
    if [ -f bin/backup_sapphire_db.sh ]; then
        run_cmd 'bash bin/backup_sapphire_db.sh -e "$ENV" -d "$DB_BACKUP_DIR" -r 30' || return
    else
        info "bin/backup_sapphire_db.sh is not in this (older) checkout; using the copy from origin/$PRODUCTION_BRANCH"
        run_cmd 'git fetch origin' || return
        run_cmd 'git show "origin/$PRODUCTION_BRANCH:bin/backup_sapphire_db.sh" > "$B/backup_sapphire_db.sh"' || return
        run_cmd 'bash "$B/backup_sapphire_db.sh" -e "$ENV" -d "$DB_BACKUP_DIR" -r 30' || return
    fi
    run_cmd 'ls -la "$DB_BACKUP_DIR"'
    local n
    n="$(ls "$DB_BACKUP_DIR"/*.dump 2>/dev/null | wc -l)"
    info "Found $n .dump file(s) in $DB_BACKUP_DIR"
    if [ "$n" -lt 4 ]; then
        warn "Fewer than 4 database dumps found. Check the output above before continuing."
        STEP_FAILED=1
    fi
}

step2_body() {
    local changed
    run_cmd 'git status --short'
    changed="$(git diff HEAD --name-only)"
    if [ -n "$changed" ]; then
        echo "Modified tracked files:"
        echo "$changed" | sed 's/^/  /'
        if ask_yes_no "Discard these local changes with git restore? (they are saved in $B/local_changes.diff first)"; then
            run_cmd 'git diff HEAD > "$B/local_changes.diff"' || return
            run_cmd 'git diff HEAD --name-only | xargs -r git restore --staged --worktree --'
        else
            warn "Keeping local changes; git pull may fail."
        fi
    fi
    run_cmd 'git pull'
    run_cmd 'git log --oneline -5'
    NEW_COMMIT="$(git rev-parse HEAD)"
    if [ "$NEW_COMMIT" = "$OLD_COMMIT" ]; then
        info "Already up to date (${NEW_COMMIT:0:8})."
    else
        info "Updated from ${OLD_COMMIT:0:8} to ${NEW_COMMIT:0:8}"
        info "Commits pulled: $(git rev-list --count "$OLD_COMMIT..$NEW_COMMIT" 2>/dev/null || echo '?')"
    fi
}

step3_body() {
    local d
    d="$(git diff "$OLD_COMMIT" HEAD -- apps/config/.env 2>/dev/null)"
    if [ -z "$d" ]; then
        info "No new settings in the template."
    else
        echo "+ git diff $OLD_COMMIT HEAD -- apps/config/.env"
        echo "$d"
    fi
    show_tags
    if ask_yes_no "Open the env file in nano now to edit it?"; then
        run_cmd 'nano "$ENV" < /dev/tty > /dev/tty'
        info "Tag lines after editing:"
        show_tags
    fi
}

step4_body() {
    run_cmd_ignore "docker images 'mabesa/sapphire-*' --format '{{.Repository}}:{{.Tag}}' | xargs -r docker rmi"
    echo
    info "An error about sapphire-dashboard being in use is expected; the next step pulls the new dashboard image."
}

step5_body() {
    run_cmd 'docker compose --env-file "$ENV" -f sapphire/docker-compose.yml build' || return
    run_cmd 'bash bin/restart_sapphire_stack.sh "$ENV"' || return
    run_cmd 'poll_url http://localhost:8000/health/ready 180 5' || return
    if curl -fsS http://localhost:8082/ > /dev/null 2>&1; then
        info "Luigi daemon is running."
    else
        warn "Luigi daemon is not answering on port 8082; starting it."
        run_cmd 'COMPOSE_PROJECT_NAME=sapphire docker compose -f bin/docker-compose-luigi.yml -p sapphire up -d luigi-daemon'
        run_cmd 'poll_url http://localhost:8082/ 60 5'
    fi
}

step6_body() {
    : > "$B/alembic_pre.txt"
    run_cmd 'docker exec sapphire-preprocessing-api alembic current | tee -a "$B/alembic_pre.txt"'
    run_cmd 'docker exec sapphire-postprocessing-api alembic current | tee -a "$B/alembic_pre.txt"'
    run_cmd 'docker exec sapphire-preprocessing-api alembic upgrade head'
    run_cmd 'docker exec sapphire-postprocessing-api alembic upgrade head'
    info "After the upgrade:"
    run_cmd 'docker exec sapphire-preprocessing-api alembic current'
    run_cmd 'docker exec sapphire-postprocessing-api alembic current'
}

job_body() {
    if [ ! -f "bin/$JOB_SCRIPT" ]; then
        err "bin/$JOB_SCRIPT not found (was step 2 skipped?)"
        STEP_FAILED=1
        return
    fi
    run_cmd "bash bin/$JOB_SCRIPT \"\$ENV\""
}

step8_body() {
    if [ ! -f bin/bimonthly_long_term_skill_metrics_recalculation.sh ]; then
        err "bin/bimonthly_long_term_skill_metrics_recalculation.sh not found (was step 2 skipped?)"
        STEP_FAILED=1
        return
    fi
    run_cmd 'SAPPHIRE_RECALC_START_YEAR=2000 bash bin/bimonthly_long_term_skill_metrics_recalculation.sh "$ENV"'
}

step9_body() {
    local code
    run_cmd 'docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"'
    run_cmd 'docker logs --tail 50 sapphire-dashboard 2>&1'
    code="$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5006/forecast_dashboard)"
    info "Dashboard HTTP status: $code (200 expected)"
    echo "Note: 'unhealthy' on sapphire-dashboard is a known healthcheck bug and can be ignored."
    if ask_yes_no "Is the dashboard stuck or crash-looping? Recreate it?"; then
        run_cmd 'docker rm -f sapphire-dashboard'
        run_cmd 'bash bin/daily_update_sapphire_frontend.sh "$ENV"'
    fi
}

step10_body() {
    local n old
    n="$(crontab -l 2>/dev/null | grep -cE '^[[:space:]]*[0-9*@]')"
    old="$(grep -cE '^[[:space:]]*[0-9*@]' "$B/crontab.txt" 2>/dev/null)"
    if [ "${n:-0}" -eq 0 ] && [ "${old:-0}" -gt 0 ]; then
        warn "Crontab has no schedule lines now, but the backup has $old."
        if ask_yes_no "Restore the crontab from $B/crontab.txt?"; then
            run_cmd 'crontab "$B/crontab.txt"'
        fi
    else
        info "OK: $n schedule line(s) in the crontab."
    fi
}

# ---------------------------------------------------------------------------
# Pre-flight (step 0): informational, never aborts on its own
# ---------------------------------------------------------------------------
preflight() {
    local hour branch bt ft
    header "STEP 0/$TOTAL_STEPS: Pre-flight checks"
    hour=$((10#$(date -u +%H)))
    if { [ "$hour" -ge 3 ] && [ "$hour" -le 6 ]; } || { [ "$hour" -ge 19 ] && [ "$hour" -le 21 ]; }; then
        warn "It is $(date -u +%H:%M) UTC. Cron jobs run between 03:00-06:59 and 19:00-21:59 UTC;"
        warn "updating now may collide with a running pipeline."
        if ! ask_yes_no "Continue anyway?"; then
            echo "Stopping. Nothing was changed."; where_things_are; exit 0
        fi
    else
        info "Time of day OK ($(date -u +%H:%M) UTC, no cron window)."
    fi

    branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
    if [ "$branch" != "$PRODUCTION_BRANCH" ]; then
        warn "Current git branch is '$branch', not the production branch '$PRODUCTION_BRANCH'."
        if ! ask_yes_no "Continue on branch '$branch'?"; then
            echo "Stopping. Nothing was changed."; where_things_are; exit 0
        fi
    else
        info "Git branch is $branch."
    fi

    info "Docker image tags in $ENV:"
    show_tags
    bt="$(tag_value backend)"; ft="$(tag_value frontend)"
    if [ "$bt" != "latest" ] || [ "$ft" != "latest" ]; then
        warn "Images tagged :local are no longer built (last built May 2026). Set both tags to latest in step 3."
    fi

    if docker info > /dev/null 2>&1; then
        info "docker works."
    else
        warn "docker does not work for this user (is the daemon running? are you in the docker group?)."
    fi
    if sudo -n true 2> /dev/null; then
        info "sudo works without a password."
    else
        warn "sudo needs a password: you will be asked for it during the database backup."
    fi
}

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
main() {
    local script_dir repo_root job
    if [ $# -ne 1 ] || [ ! -f "$1" ]; then
        echo "Usage: bash bin/update_sapphire_server.sh <path-to-env-file>"
        echo "  e.g. bash bin/update_sapphire_server.sh /data/<data_dir>/config/.env_develop_<org>"
        [ $# -eq 1 ] && echo "Env file not found: $1"
        return 1
    fi

    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [ -n "${REPO_DIR:-}" ]; then
        repo_root="$REPO_DIR"
    elif [ -f "$(dirname "$script_dir")/sapphire/docker-compose.yml" ]; then
        repo_root="$(dirname "$script_dir")"
    else
        repo_root="/data/SAPPHIRE_Forecast_Tools"
    fi
    if [ ! -f "$repo_root/sapphire/docker-compose.yml" ] \
        || ! git -C "$repo_root" rev-parse --is-inside-work-tree > /dev/null 2>&1; then
        echo "Cannot find the SAPPHIRE repository; set REPO_DIR=/path/to/SAPPHIRE_Forecast_Tools"
        return 1
    fi
    cd "$repo_root" || { echo "Cannot cd to $repo_root"; return 1; }

    ENV="$(readlink -f "$1")"
    DATA_DIR="$(dirname "$(dirname "$ENV")")"
    LT_CONFIG_DIR="$DATA_DIR/config/long_term_configs"
    USE_LT=0
    [ -d "$LT_CONFIG_DIR" ] && USE_LT=1

    B="$HOME/backups/sapphire_$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$B" || { echo "Cannot create $B"; return 1; }
    exec > >(tee -a "$B/update.log") 2>&1

    DB_BACKUP_DIR="/var/backups/sapphire/pre_update_$(date +%Y%m%d_%H%M%S)"
    DB_BACKUP_DONE=0
    OLD_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
    NEW_COMMIT="$OLD_COMMIT"

    header "SAPPHIRE server update"
    echo "Host:      $(hostname)"
    echo "User:      $USER"
    echo "UTC time:  $(date -u '+%Y-%m-%d %H:%M:%S')"
    echo "Repo dir:  $repo_root"
    echo "Branch:    $(git rev-parse --abbrev-ref HEAD 2>/dev/null)"
    echo "HEAD:      $(git log --oneline -1 2>/dev/null)"
    echo "Env file:  $ENV"
    echo "Backup/log dir: $B"
    if [ "$USE_LT" -eq 1 ]; then
        echo "Long-term forecasts: configured ($LT_CONFIG_DIR)"
    else
        echo "Long-term forecasts: not configured on this deployment"
    fi

    preflight

    run_step 1 "Back up config, crontab, git commit, images and databases" \
        "Saves the env file, crontab, current git commit and image list to $B, then dumps the 4 databases. Needed so you can roll back." \
"cp \"\$ENV\" \"$B/\"; crontab -l > $B/crontab.txt; git rev-parse HEAD > $B/git_commit.txt
docker images | grep -E \"sapphire|mabesa\" > $B/docker_images.txt
sudo mkdir -p /var/backups/sapphire/pre_update_<timestamp>; sudo chown \"\$USER\" <that dir>
bash bin/backup_sapphire_db.sh -e \"\$ENV\" -d <that dir> -r 30" \
        step1_body

    run_step 2 "Get the new code" \
        "Shows local changes (offers to discard them, saving a copy first), then pulls the latest code with git." \
"git status --short
git pull
git log --oneline -5" \
        step2_body

    run_step 3 "Check the .env for new settings" \
        "Shows settings added to the template apps/config/.env since the old commit and lets you edit your env file (set both image tags to latest)." \
"git diff <old commit> HEAD -- apps/config/.env
nano \"\$ENV\"   (only if you say yes)" \
        step3_body

    run_step 4 "Delete the old pipeline images" \
        "Removes the old mabesa/sapphire-* images so fresh ones are pulled. Images in use by running containers cannot be removed; that is fine." \
"docker images 'mabesa/sapphire-*' --format '{{.Repository}}:{{.Tag}}' | xargs -r docker rmi" \
        step4_body

    run_step 5 "Rebuild the services and restart the stack" \
        "A build is required: a restart alone keeps the OLD service code. Warnings about unset \"ieasyhydroforecast_data_ref_dir\" during the *build* are harmless, but if they appear during the restart, stop and investigate. Then waits for readiness and makes sure the Luigi daemon runs. This can take several minutes." \
"docker compose --env-file \"\$ENV\" -f sapphire/docker-compose.yml build
bash bin/restart_sapphire_stack.sh \"\$ENV\"
curl -sf http://localhost:8000/health/ready   (every 5 s, up to 180 s)
if Luigi does not answer on :8082:
  COMPOSE_PROJECT_NAME=sapphire docker compose -f bin/docker-compose-luigi.yml -p sapphire up -d luigi-daemon" \
        step5_body

    run_step 6 "Update the databases (alembic)" \
        "Applies pending database schema migrations in the preprocessing and postprocessing services. The version before the upgrade is saved in $B/alembic_pre.txt." \
"docker exec sapphire-preprocessing-api alembic current
docker exec sapphire-postprocessing-api alembic current
docker exec sapphire-preprocessing-api alembic upgrade head
docker exec sapphire-postprocessing-api alembic upgrade head
(then alembic current again for both)" \
        step6_body

    header "STEP 7/$TOTAL_STEPS: Run the main jobs once to test"
    echo "Each job gets its own question. Follow progress in the Luigi UI at http://$(hostname):8082"
    echo "Note: these jobs can take many minutes."
    for job in run_preprocessing_gateway run_pentadal_forecasts run_decadal_forecasts run_long_term_forecasts run_daily_maintenance; do
        if [ "$job" = "run_long_term_forecasts" ] && [ "$USE_LT" -eq 0 ]; then
            echo
            info "bin/$job.sh: Not used on this deployment — skipped"
            continue
        fi
        JOB_SCRIPT="$job.sh"
        run_step "7 ($job)" "Run $job.sh" \
            "Runs this job once through Luigi to test the new version." \
            "bash bin/$job.sh \"\$ENV\"" \
            job_body
    done

    if [ "$USE_LT" -eq 1 ]; then
        run_step 8 "One-time long-term skill recalculation" \
            "The new version re-enables quarter/season forecasts; recalculate long-term skill metrics over the full history. Takes a long time." \
            "SAPPHIRE_RECALC_START_YEAR=2000 bash bin/bimonthly_long_term_skill_metrics_recalculation.sh \"\$ENV\"" \
            step8_body
    else
        header "STEP 8/$TOTAL_STEPS: Long-term skill recalculation"
        echo "Not used on this deployment — skipped"
    fi

    run_step 9 "Check the dashboard and containers" \
        "Lists containers, shows the dashboard log and checks that the dashboard answers. 'unhealthy' on sapphire-dashboard is a known healthcheck bug and can be ignored." \
"docker ps --format \"table {{.Names}}\\t{{.Image}}\\t{{.Status}}\"
docker logs --tail 50 sapphire-dashboard
curl -s -o /dev/null -w \"%{http_code}\" http://localhost:5006/forecast_dashboard
(optional, if you say yes) docker rm -f sapphire-dashboard; bash bin/daily_update_sapphire_frontend.sh \"\$ENV\"" \
        step9_body

    run_step 10 "Check cron is still installed" \
        "Counts the schedule lines in the crontab and offers to restore the saved copy if they have vanished." \
"crontab -l | grep -cE '^[[:space:]]*[0-9*@]'
(only if 0 and the backup has lines) crontab $B/crontab.txt" \
        step10_body

    header "SUMMARY"
    NEW_COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "Commit:  ${OLD_COMMIT:0:8} -> ${NEW_COMMIT:0:8}"
    where_things_are
    echo
    rollback_hint
    return 0
}

# Keep this as the very last line (see the note in the header comment).
main "$@"; exit $?
