# Plan - Local tjhm Historical Observed-Discharge Backfill

**Status:** DRAFT, revised per adversarial review (APPROVE-WITH-CHANGES; must-fix #1-#4 + low #5-#6 applied 2026-06-19). For owner sign-off; do not execute until signed off.

**Scope:** local tjhm only first; server parity is planned separately below.
No `sapphire/services/**` edits. Any code change, if owner chooses one of the
optional paths, is delegated to Sonnet agents and verified with
`cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh <module>`.

**Sensitive-data rule:** this committed plan uses sentinel station code `19999`
and local-only placeholders for real stations. Do not paste real station codes,
discharge values, env contents, or unmasked query output into committed files.

---

## Problem And Verified State

Local tjhm short-term skill is under-paired because the local preprocessing DB
has only recent observed runoff rows. The owner-provided verified state is:

- local tjhm DB has only 2026 observed discharge for the owner target stations:
  daily, pentad, and decade rows exist only in the current year.
- kghm reference has 2010 through 2026 observed history and short-term skill
  computes with useful `n_pairs`.
- the LR maintenance fix in commit `4237963` made the caught-up hindcast path
  refresh observed pentad/decade runoff targets, but the recurring maintenance
  window is only 90 days. It improves "skips" into computed rows, but still
  leaves tjhm `n_pairs` near 0-1.

Code path confirmation:

- `bin/initialize_site_backfill.sh` is the current end-to-end wrapper. Its
  header states it runs preprocessing-runoff, LR PENTAD/DECAD, and skill recalc
  in `SAPPHIRE_SYNC_MODE=initial`, and that it runs all stations
  (`bin/initialize_site_backfill.sh:4-14`).
- `--site-code` is **not a filter**; it is for log readability and SQL count
  verification only (`bin/initialize_site_backfill.sh:30-32`,
  `bin/initialize_site_backfill.sh:107-108`).
- Phase 1 of the wrapper runs the preprunoff image with
  `SAPPHIRE_SYNC_MODE=initial` and mounts the deployment data
  `intermediate_data` directory (`bin/initialize_site_backfill.sh:291-308`).
- Phase 2 runs LR PENTAD and DECAD with `SAPPHIRE_SYNC_MODE=initial` and
  `uv run linear_regression.py --hindcast --start-date ...`
  (`bin/initialize_site_backfill.sh:361-399`).
- LR reads daily discharge through `read_daily_discharge_data()`
  (`apps/iEasyHydroForecast/forecast_library.py:1264-1265`) and writes observed
  period runoff through `write_pentad_time_series_data()` and
  `write_decad_time_series_data()` (`apps/linear_regression/linear_regression.py:785-823`).
- The period runoff writer maps `discharge_avg` to API `discharge`
  (`apps/iEasyHydroForecast/forecast_library.py:3665-3682`) and, in
  maintenance/initial modes, preserves existing non-null `discharge` and
  `predictor` values by read-merge-write before upsert
  (`apps/iEasyHydroForecast/forecast_library.py:3704-3755`).
- **DAILY runoff has NO such protection (reviewer finding).** P2's daily write goes
  through a *different* function - `apps/preprocessing_runoff/src/src.py:4343-4433`
  (`client.write_runoff()` at `:4431`, no read, no merge) - and the service upsert
  (`sapphire/services/preprocessing/app/crud.py:35-43`) blind-overwrites every
  column, so an incoming null `discharge` clobbers an existing non-null one. The
  daily path is therefore NOT null-clobber-safe on rerun (see P2, Risks, Rollback).
- Skill recalc reads short-term observations from the runoff API, mapping
  internal `decad` to API `decade`
  (`apps/postprocessing_forecasts/src/data_reader.py:1440-1458`,
  `apps/postprocessing_forecasts/src/data_reader.py:2084-2118`).

### Source Decision

Use the local tjhm CSV history as the authoritative local source for this local
repair, and override the current env start date with:

```text
START_DATE=2010-01-01
```

Justification:

- The local data directory is available through the existing symlink
  `$HOME/Documents/GitHub/taj_data_forecast_tools`.
- Read-only CSV inspection showed:
  - `intermediate_data/runoff_day.csv`: 437,270 data rows, date span
    1940-01-01 through 2026-06-19.
  - `intermediate_data/runoff_pentad.csv`: 49,211 data rows, date span
    1940-01-05 through 2026-06-15.
  - `intermediate_data/runoff_decad.csv`: 24,586 data rows, date span
    1940-01-10 through 2026-06-10.
- A local-only masked check confirmed the two owner target stations have daily
  CSV history across the same full span; those real codes and outputs are not
  recorded here.
- `2010-01-01` matches the kghm comparison window for skill comparison.
  **REVIEWER FINDING (corrected):** `ieasyhydroforecast_START_DATE` / `--start-date`
  is passed to the container but is **never read** by any code in
  `apps/preprocessing_runoff/`. The daily write window is bounded only by CSV
  contents + sync mode; the local CSV spans **1940-01-01**, so **P2 imports the full
  ~1940->now daily history, not 2010->now**. `--start-date` only bounds the LR hindcast
  (P3) and is a no-op on the daily write. Pre-2010 daily rows are harmless (extra
  history) but must be expected - do not treat the daily span as 2010-bounded.

Important caveat: `preprocessing_runoff`'s HF fetch mode currently recognizes
only `operational` and `maintenance`. If passed `initial`, it warns and falls
  back to operational fetch (`apps/preprocessing_runoff/src/src.py:3635-3639`).
The local wrapper still works because the historical CSV already exists locally
and the API writer accepts `SAPPHIRE_SYNC_MODE=initial`, writing all rows it was
given (`apps/preprocessing_runoff/src/src.py:4385-4408`). Do not assume the same
path fetches full history directly from iEH HF unless P0 proves it or a Sonnet
code change adds explicit initial-mode HF fetching.

---

## Reconciliation With Existing Plan

`doc/plans/working/runoff_pentad_decad_discharge_backfill_plan.md` handled the
LR-side problem: maintenance could skip period runoff target refresh once LR
forecasts were caught up, and the service upsert could clobber sibling fields.
That plan's Part 1 is done by the landed LR fix.

This plan is the missing history-sourcing Part 2 for the local tjhm DB. The
earlier plan assumed historical daily runoff already existed somewhere the LR
aggregation could read. Local tjhm DB does not have that history, so this plan
first seeds daily observed runoff from the local CSV history, then lets LR
initial mode aggregate pentad/decade observed runoff and regenerate LR
hindcasts.

---

## Phases

### P0 - Source And State Probe

**Goal:** Confirm the chosen source and target DB state without writes.

**Files:** none modified. Read-only:
`CLAUDE.md`, `bin/initialize_site_backfill.sh`,
`apps/preprocessing_runoff/src/src.py`,
`apps/preprocessing_runoff/initial_api_sync.py`,
`apps/iEasyHydroForecast/forecast_library.py`,
`apps/linear_regression/linear_regression.py`,
`apps/postprocessing_forecasts/src/data_reader.py`,
`doc/prod/historical_backfill_runbook.md`, local CSV headers/spans.

**Depends on:** none.

**Agents:** 1 planner. No Sonnet implementation agent.

**Acceptance criteria:**

- Confirm local CSV span and row counts without printing discharge values.
- Confirm the two owner target stations have historical source rows using
  local-only variables, not committed output:

  ```bash
  export TARGET_A="<owner local real code; do not commit>"
  export TARGET_B="<owner local real code; do not commit>"
  ```

- Confirm local API state is still recent-only before writes using local-only
  target variables and non-committed output.
- Confirm services are healthy and local iEH HF tunnel state is known, even
  though local source decision is CSV-first.
- Confirm owner accepts `START_DATE=2010-01-01`.

### P1 - Scoped Validation Gate

**Goal:** Decide whether to run an all-station local write immediately or first
add a true station-filtered validation path.

**Files:** none if owner accepts current tooling. Optional Sonnet code path, only
if owner requires true write-scoped validation:
`bin/initialize_site_backfill.sh`, plus the minimum `apps/` modules needed to
honor a station filter consistently across preprunoff, LR, and skill recalc.

**Depends on:** P0.

**Agents:** 1 planner for the default no-code gate. If optional station-filter
support is chosen: 1 Sonnet implementation agent, then planner review.

**Acceptance criteria:**

- The owner explicitly chooses one:
  - **Default:** accept all-station local backfill because the wrapper cannot
    true-filter execution today. Do NOT pass `--site-code` at all: it does not
    scope execution and makes `verify_db_counts` return 0 rows for a sentinel
    (verify owner targets interactively with real codes instead).
  - **Optional code path:** add true station-filter support first, delegate to a
    Sonnet agent, and verify touched modules with `run_tests.sh`.
- If optional code is chosen, the agent prompt must state: do not edit
  `sapphire/services/**`; do not change unrelated function signatures or data
  flow; preserve default all-station behavior when no filter is supplied.
- If no code is chosen, P2 is the first write and its blast radius is all tjhm
  stations.

### P2 - Local Historical Daily Backfill

**Goal:** Write daily observed runoff history from local CSV to the local
preprocessing API.

**Files:** no repo files modified in the default path. Runtime writes only to
local DB and runtime logs.

**Depends on:** P1.

**Agents:** execution agent/operator after owner approval; no code agent.

**Acceptance criteria:**

- Run `bin/initialize_site_backfill.sh` Phase 1 only:

  ```bash
  cd "$HOME/Documents/GitHub/SAPPHIRE_forecast_tools"
  bash bin/initialize_site_backfill.sh \
    "$HOME/Documents/GitHub/taj_data_forecast_tools/config/.env_develop_tjhm" \
    --start-date 2010-01-01 \
    --skip-linreg \
    --skip-skill
  ```
  > **Do NOT pass `--site-code 19999`.** `--site-code` is cosmetic (it does not
  > scope execution - all stations run regardless) and the wrapper's
  > `verify_db_counts` filters `WHERE code='<site-code>'`, so a sentinel code makes
  > every post-run check return **0 rows** and a successful backfill look like a
  > no-op. Omit it -> the all-sites `GROUP BY` verification branch runs. Verify owner
  > target stations **interactively with their real codes** (never committed).

- **Mandatory pre-P2 DB dump first** (the daily path is not null-clobber-safe - see
  Risks/Rollback). Post-run API/SQL checks (interactive, real codes) show daily
  `runoffs` span the **full CSV history (~1940 -> current date - NOT 2010-bounded;
  see Source Decision)**, with row counts comparable to (or exceeding) the kghm
  reference window.
- No real station codes or discharge values are committed in logs/docs.
- If Phase 1 fails to import full daily history because `initial` was treated as
  operational source mode, stop and use one of these owner-approved fallback
  paths:
  - run `apps/preprocessing_runoff/initial_api_sync.py` to import existing CSV
    history directly; or
  - delegate a Sonnet fix adding explicit preprunoff `initial` source mode from
    `ieasyhydroforecast_START_DATE`, then verify with
    `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff`.

### P3 - Local LR Period Backfill

**Goal:** Regenerate pentad/decade observed runoff targets and LR hindcasts from
the now-populated daily history.

**Files:** no repo files modified in the default path. Runtime writes only to
local preprocessing/postprocessing DBs and runtime logs.

**Depends on:** P2.

**Agents:** execution agent/operator after owner approval; no code agent.

**Acceptance criteria:**

- Run `bin/initialize_site_backfill.sh` Phase 2 only:

  ```bash
  cd "$HOME/Documents/GitHub/SAPPHIRE_forecast_tools"
  bash bin/initialize_site_backfill.sh \
    "$HOME/Documents/GitHub/taj_data_forecast_tools/config/.env_develop_tjhm" \
    --start-date 2010-01-01 \
    --skip-preprunoff \
    --skip-skill
  ```
  > Omit `--site-code` (see P2 note - cosmetic + makes verification return 0 rows).
  > Here `--start-date 2010-01-01` IS honored (it bounds the LR hindcast window).

- Pentad and decade observed runoff rows exist for the owner target stations
  across the 2010 through current window.
- LR hindcast rows exist for PENTAD and DECADE across the same historical issue
  window.
- Rerun safety is verified by the read-merge-write path: existing non-null
  `discharge` or `predictor` is not overwritten by an incoming null.

### P4 - Local Skill Recalc And Verification

**Goal:** Recalculate short-term skill after observed runoff and LR hindcasts are
historical.

**Files:** no repo files modified in the default path. Runtime writes only to
local postprocessing DB and runtime logs.

**Depends on:** P3.

**Agents:** execution agent/operator after owner approval; no code agent.

**Acceptance criteria:**

- Prefer scoped recalc for validation by setting the existing
  `SAPPHIRE_RECALC_STATION_CODE` environment variable inside the postprocessing
  run when feasible. If using the wrapper, note it currently does not expose
  this variable.
- Default wrapper command:

  ```bash
  cd "$HOME/Documents/GitHub/SAPPHIRE_forecast_tools"
  bash bin/initialize_site_backfill.sh \
    "$HOME/Documents/GitHub/taj_data_forecast_tools/config/.env_develop_tjhm" \
    --start-date 2010-01-01 \
    --skip-preprunoff \
    --skip-linreg
  ```
  > Omit `--site-code` (see P2 note). True per-station scoping of the recalc is only
  > possible by running `apps/postprocessing_forecasts/recalculate_skill_metrics.py`
  > directly with `SAPPHIRE_RECALC_STATION_CODE` set (read at
  > `recalculate_skill_metrics.py:106-109`); the wrapper / `run_skill_metrics_recalc_once`
  > does NOT forward that var, so via the wrapper the recalc runs all stations.

- **Concrete acceptance threshold (not "comparable to kghm"):** for the owner
  target stations + LR model, pentad and decade `n_pairs` should reach roughly the
  kghm range (~9-21) for established horizon-of-year periods - and in any case be
  clearly **> 1** (the pre-backfill value), not 0-1. Record the actual kghm range at
  P0 and use it as the numeric bar.
- If all-station recalc is too slow, delegate a small wrapper enhancement to
  pass `SAPPHIRE_RECALC_STATION_CODE` through `run_skill_metrics_recalc_once`;
  verify with `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`.

### P5 - Sign-Off And Documentation

**Goal:** Preserve the evidence needed for owner sign-off without committing
sensitive data.

**Files:** this plan may be updated; optional runbook clarification in
`doc/prod/historical_backfill_runbook.md` only after owner approval.

**Depends on:** P4.

**Agents:** 1 planner/doc agent. If docs are edited, no test run is required
unless code also changed.

**Acceptance criteria:**

- Owner receives masked before/after coverage summary:
  daily, pentad, decade observed rows; LR hindcast coverage; LR skill `n_pairs`.
- No local cron is restored or modified; this is a one-time local rebuild.
- Any server-runbook edit is reviewed as a doc-only change and keeps sentinel
  examples.

---

## Server Deployment Parity

The production path should not live in `doc/prod/update_deployment_checklist.md`.
That checklist explicitly treats historical migration/backfill as a separate
runbook activity (`doc/prod/update_deployment_checklist.md:1034-1079`).

The right home is `doc/prod/historical_backfill_runbook.md`, which already maps:

- P2 to daily runoff/day hydrograph history using `initialize_site_backfill.sh`
  (`doc/prod/historical_backfill_runbook.md:1083-1138`).
- P4 to LR PENTAD/DECAD hindcasts using the same wrapper
  (`doc/prod/historical_backfill_runbook.md:1349-1396`).
- P8 to short-term skill recalc using the same wrapper
  (`doc/prod/historical_backfill_runbook.md:1956-2012`).

Runbook clarification needed: P2/P4 should explicitly state that short-term
observed pentad/decade `runoffs.discharge` recovery depends on P2 daily history
first, then P4 LR period aggregation. The runbook should also warn that current
preprunoff `initial` mode does not itself prove a full iEH HF historical fetch;
the server must either already have full local source files, or the code must be
extended to fetch HF history from `START_DATE` before relying on server iEH HF as
the source.

### Server Command Sequence

Server execution belongs after the LR fix is deployed to the server image.
Use sentinel code `19999` in saved docs/log examples; real station checks are
interactive only.

```bash
cd /data/SAPPHIRE_Forecast_Tools
source bin/setup_historical_backfill_env.sh --profile taj
load_backfill_env

export START_DATE=2010-01-01
export SAMPLE_CODE=19999   # cosmetic only - see note below
```

> Same `--site-code` caveat as local: it does NOT scope execution and makes
> `verify_db_counts` return 0 rows for a sentinel. The server commands below pass
> `--site-code "$SAMPLE_CODE"` only to keep saved examples sentinel-safe; for real
> verification, omit it (all-sites branch) or query real codes interactively.
> Also: `--start-date` is honored only by LR (P4), not by the daily import (P2).

Pre-write gates:

```bash
docker ps
curl -fsS http://localhost:8000/health/ready
systemctl status autossh-ieasyhydro.service --no-pager

# Confirm the server's DEPLOYED code/image includes the LR read-merge-write fix
# (4237963) before P4. Do NOT use `git merge-base --is-ancestor 4237963 HEAD`: the
# hash changes when the fix is squashed/rebased into maxat_sapphire_2 (false
# negative), and its exit code was not enforced. Use a content check on the
# deployed file and treat absence as a HARD gate (abort):
grep -n "read-merge-write\|sync_mode in" apps/iEasyHydroForecast/forecast_library.py
# Expect the maintenance/initial read-merge-write branch (~forecast_library.py:3704).
# If absent, the fix is NOT deployed -> STOP: land it on maxat_sapphire_2, rebuild
# mabesa/sapphire-linreg, redeploy, then retry. Also confirm the running image was
# built after the fix landed:
docker image inspect "mabesa/sapphire-linreg:${ieasyhydroforecast_backend_docker_image_tag:-latest}" --format '{{.Created}}'
```

Backup and pause cron per the runbook before writes:

```bash
export BACKUP_DIR="/var/backups/sapphire/pre_tjhm_short_skill_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$BACKUP_DIR"
crontab -l > "$BACKUP_DIR/crontab_backup.txt"
crontab -r

bash bin/backup_sapphire_db.sh --env-file "$ENV_FILE"
```

Server P2:

```bash
bash bin/initialize_site_backfill.sh "$ENV_FILE" \
  --start-date "$START_DATE" \
  --site-code "$SAMPLE_CODE" \
  --skip-linreg \
  --skip-skill
```

Server P4:

```bash
bash bin/initialize_site_backfill.sh "$ENV_FILE" \
  --start-date "$START_DATE" \
  --site-code "$SAMPLE_CODE" \
  --skip-preprunoff \
  --skip-skill
```

Server P8 short-term:

```bash
bash bin/initialize_site_backfill.sh "$ENV_FILE" \
  --start-date "$START_DATE" \
  --site-code "$SAMPLE_CODE" \
  --skip-preprunoff \
  --skip-linreg
```

Server verification:

- run the runbook P0/P10 runoff, LR, and skill SQL with sentinel examples.
- interactively check owner-selected real stations without saving real codes to
  committed artifacts.
- restore cron only after owner sign-off:

```bash
crontab "$BACKUP_DIR/crontab_backup.txt"
```

Current project-memory note: the paused Tajik AWS historical backfill had cron
paused, a verified DB backup, and several persisted phases already completed.
It was interrupted during long-term P7 by server power cycles. This local
observed-discharge repair must not assume that server state is current; rerun
server P0 inventory before deciding whether P2/P4/P8 are needed.

---

## Risks And Rollback

| Risk | Level | Mitigation |
|---|---:|---|
| `--site-code` misunderstood as an execution filter | High | Treat it as verification only; owner must accept all-station local write or approve a Sonnet station-filter change. |
| Preprunoff `initial` does not fetch full iEH HF history | High | Local source is existing CSV. For server iEH-source parity, add a runbook gate or code support before relying on direct HF history. |
| Upsert null clobber - pentad/decad period path | Medium | LR period runoff writer uses read-merge-write in `initial`/`maintenance` (`forecast_library.py:3704-3751`); verify no non-null field regresses to null. |
| Upsert null clobber - **DAILY path (P2)** | **High** | The read-merge-write protection does NOT cover the daily writer (`preprocessing_runoff/src/src.py:4343-4433`, no merge) - the service upsert (`crud.py:35-43`) blind-overwrites, so an incoming null `discharge` clobbers existing good daily data. **Mandatory pre-P2 DB dump.** Never rerun P2 over a date range where the CSV has gaps. A byte-identical rerun is a true no-op (daily has no `predictor`; only `discharge` is at risk). |
| Runtime/all-station blast radius | Medium | Start with P0 source/state probe; run P2/P3 separately; use logs and SQL counts after each phase. |
| Partial paused server project state | Medium | Server must rerun P0 inventory and use DB backup before any write. |
| ML-017 blocks EM/NE recovery | High | Out of scope here. This plan restores observed runoff and LR skill; EM/NE short-term skill still depends on fixing the ML ERA5-gap NaN cascade. |
| Decade ML skill starved on tjhm | Medium | **Root cause RESOLVED 2026-07-15** (PP-043, archived): not a code bug — the tjhm ML **forecast** archive is incomplete (decade half never finished; interrupted backfill), so `n_pairs` starves for want of forecasts. Proven locally: migrate the Tajik combined CSVs → `n_pairs` 0-1→14-15, zero code change. **Residual server task now owned here:** finish the tjhm ML pentad/decade archive (from-file combined CSVs via `data_migrator.py --type combinedforecast`; dedup on `(code,model_short,date)` first — see [[ml_fromfile_combinedforecast_migration]]), then recalc. Do not block LR observed backfill on it. |

Rollback:

- Local: **take a DB dump before P2 - MANDATORY, not optional.** The daily write
  path is NOT null-clobber-safe (see Risks / Code path confirmation), so reruns are
  idempotent ONLY when every rerun supplies non-null `discharge` for every
  `(code,date)` it touches: a byte-identical rerun is a no-op, but a gap-containing
  rerun clobbers existing good data. The pre-P2 dump is the real rollback.
- Server: follow `doc/prod/historical_backfill_runbook.md` backup/rollback
  procedure. Do not use `git reset` or service edits as rollback.

---

## Dependency Graph

```json
{
  "phases": {
    "P0": {
      "depends_on": [],
      "parallel_agents": 1
    },
    "P1": {
      "depends_on": ["P0"],
      "parallel_agents": 1
    },
    "P2": {
      "depends_on": ["P1"],
      "parallel_agents": 1
    },
    "P3": {
      "depends_on": ["P2"],
      "parallel_agents": 1
    },
    "P4": {
      "depends_on": ["P3"],
      "parallel_agents": 1
    },
    "P5": {
      "depends_on": ["P4"],
      "parallel_agents": 1
    },
    "server_parity": {
      "depends_on": ["P5"],
      "parallel_agents": 1
    }
  }
}
```

---

## Execution Record (local tjhm, 2026-06-19) - COMPLETE, PASS

Executed P0->P4 locally, phase-by-phase with owner sign-off. Masked codes
(TARGET_A/TARGET_B = the two owner tjhm stations; KGZ_REF = kghm reference).

**Decisions taken:** all-station scope (no `--site-code`); `START_DATE=2010-01-01`;
pre-P2 dump to `~/sapphire_db_backups`.

**Local-execution requirement (NEW finding):** on the arm64 mac the
`mabesa/sapphire-*:latest` images have no arm64 manifest. Every phase needed
`export DOCKER_DEFAULT_PLATFORM=linux/amd64` (preprunoff, linreg, postprocessing).
This is local-only; servers are amd64 and do not need it.

**P0 (read-only):** confirmed targets recent-only (2026: day 73 / pentad 14 /
decad 7; LR skill n_pairs 0..1); CSV source full 1940->2026; KGZ_REF LR skill bar
n_pairs ~15..21.

**P2 (daily import) - PASS:** mandatory dump verified first (4 dumps to
`~/sapphire_db_backups`). Daily runoff per target 73 -> 31,582 rows
(1940-01-01..2026-06-19; 6,014 in 2010..2026); recent 2026 rows preserved
(73->170). Side effects: Phase-1 preprunoff also did its normal hydrograph API
write and rewrote intermediate runoff/hydrograph CSVs (expected, broader than
daily runoff). `initial` source mode warns and falls back to operational
(expected; CSV-existing path).

**P3 (LR period backfill) - PASS:** observed pentad 14->1185, decade 7->592
(2010->2026, ~kghm scale); LR hindcasts pentad 1185, decade 592. Read-merge-write
held: pre-existing recent rows retained, zero regressions to null. Residual
historical nulls outside the recent span are genuine data gaps, not clobbers.

**P4 (skill recalc) - PASS (acceptance gate):**
```
LR pentad skill: TARGET_A 0..1 -> 14..15 ; TARGET_B 0..1 -> 15..17
LR decade skill: TARGET_A 1..1 -> 14..15 ; TARGET_B 1..1 -> 16..17
KGZ_REF LR bar: 15..21  -> targets now in reference-scale range. PASS.
```
No "No short-term observations available" skip; no ERROR/Traceback.

**Outcome:** local tjhm **LR** short-term skill RESTORED (pentad + decade).
EM/NE and ML-model (TFT/TiDE/TSMixer) skill remain thin/absent for tjhm because
their forecasts are NaN under **ML-017** (ERA5-gap cascade) - out of scope here.

**Still open:** (1) server parity - deploy LR fix `4237963` to the server image,
then run the Server Deployment Parity sequence above (server DB already has most
history per the paused server backfill project; rerun server P0 inventory first).
(2) ML-017 fix to recover EM/NE short-term skill. (3) Plan note: verbose linreg
runtime logs under `~/Documents/GitHub/logs/site_backfill/` are local-only and may
contain sensitive values - do not commit.
