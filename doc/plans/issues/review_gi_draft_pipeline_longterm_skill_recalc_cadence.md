# Long-term skill-metric recalc cadence: bimonthly, tied to long-term forecast runs

## Problem

Monthly/quarterly/seasonal skill metrics don't refresh on the server because the
only scheduled skill-recalc job runs **once a year on December 31** and
hardcodes `SAPPHIRE_PREDICTION_MODE=BOTH`, which covers only `PENTAD`+`DECAD`.

Operational consequence: long-term skill tiles on the Kyrgyz Hydromet staging
dashboard stay empty until the next December run — and even then only if a
maintainer manually overrides the mode.

Locally, `apps/run_locally.sh` already solves this: after long-term forecasts it
iterates `MONTHLY QUARTERLY SEASONAL` and runs `recalculate_skill_metrics.py`
once per mode (`apps/run_locally.sh:1000-1024`, called at line 1143).

## Decision

Mirror `run_locally.sh` on the server: schedule a **long-term skill recalc**
that runs **after `run_long_term_forecasts.sh`** on the 10th and 25th, so
monthly-tier skill is refreshed shortly after each new long-term forecast
instead of once a year.

**Decisions (locked in):**
- **Cron timing:** 10th & 25th at **10:00 UTC** (4 hours after the 06:00 UTC
  long-term forecast cron).
- **Mode order:** `MONTHLY → QUARTERLY → SEASONAL`. `MONTHLY` runs first (most
  operationally visible), matches `run_locally.sh`.
- **Failure policy:** **log and continue**. If one mode fails, the remaining
  modes still run — `QUARTERLY` flakiness shouldn't block `MONTHLY`. The
  wrapper exits non-zero at the end if any mode failed, so cron/log review
  still surfaces the problem.

## Approach

Build on the existing `bin/yearly_skill_metrics_recalculation.sh`, which
already runs a full skill recalc via `docker run` and already respects
`SAPPHIRE_PREDICTION_MODE` at line 119
(`-e SAPPHIRE_PREDICTION_MODE=${SAPPHIRE_PREDICTION_MODE:-BOTH}`). Extract its
`docker run` body into a shared helper, then add a new
`bin/bimonthly_long_term_skill_metrics_recalculation.sh` that loops
`MONTHLY`, `QUARTERLY`, `SEASONAL` and invokes the helper once per mode with
log-and-continue.

**Why shell, not Luigi:** the current Dec 31 cron goes through Luigi via
`PeriodicMaintenanceWrapper`, but that task's only real value-add is a
marker file (prevents re-runs in the same year). The bimonthly recalc is
idempotent — running it twice is harmless, just wasted CPU. The shell path
is ~40 lines of new code vs. ~60 lines of Luigi task + Python tests, mirrors
`run_locally.sh` almost line-for-line, and is easier for operators to
invoke manually for the one-off catch-up run.

## Scope

**In scope:**
- New script `bin/bimonthly_long_term_skill_metrics_recalculation.sh`
- Refactor: extract the shared macOS-override + `docker run` block from
  `yearly_skill_metrics_recalculation.sh` into
  `bin/utils/run_skill_metrics_recalc.sh`. Yearly path's `docker run`
  arguments stay byte-identical.
- Pytest tests (subprocess-driven): one for the helper in isolation against
  a stub `docker`, one for the bimonthly wrapper against a fake helper
  (no docker stub needed).
- Cron line in `doc/deployment.md`

**Out of scope:**
- Modifying `run_long_term_forecasts.sh` (separate job)
- Modifying `YearlySkillRecalculation` Luigi task or the Dec 31 cron
- Modifying `recalculate_skill_metrics.py` (already supports the three modes)
- Adding `DAILY` to the rotation — not used on any production dashboard
- `sapphire/services/` — no service changes

## Files touched

| File | Change |
|------|--------|
| `bin/utils/run_skill_metrics_recalc.sh` (new) | Helper function `run_skill_metrics_recalc_once <mode> <log_dir> <timestamp> <container_name>`: encapsulates the macOS `IEASYHYDROHF_HOST` override, container-name + service-log derivation, stale-container removal, `docker run`, exit-code capture, and post-run `docker rm`. **Does not set traps. Does not read ambient `SAPPHIRE_PREDICTION_MODE`.** Returns the container exit code. |
| `bin/yearly_skill_metrics_recalculation.sh` | Replace the macOS-override + docker-run + cleanup block (current lines 86-142) with a single call to the helper. **Docker run arguments must be byte-identical to today** (same container name `postprc-skill-recalc`, same service-log filename pattern, same env, same volumes, same memory limits). |
| `bin/bimonthly_long_term_skill_metrics_recalculation.sh` (new) | Iterates `MONTHLY`, `QUARTERLY`, `SEASONAL`; log-and-continue; exits non-zero if any mode failed. Uses mode-suffixed container names (`postprc-skill-recalc-<MODE>`) so service-log filenames and Docker container names are unique per mode. |
| `apps/pipeline/tests/test_bimonthly_skill_recalc.py` (new, pytest — path subject to repo convention check) | Two test cases: (a) helper in isolation with a stubbed `docker` invocation, (b) bimonthly wrapper with the helper replaced by a recorded fake (no docker stub needed). |
| `doc/deployment.md` | Add cron line; document timing rationale |

**Files NOT modified:**
- `bin/run_long_term_forecasts.sh`
- `bin/run_periodic_maintenance.sh`
- `apps/pipeline/pipeline_docker.py`
- `apps/postprocessing_forecasts/recalculate_skill_metrics.py`
- `sapphire/services/*`

## Plan

### Phase P1 — Extract shared helper

**Goal:** Pull the macOS-override + docker-run body from
`bin/yearly_skill_metrics_recalculation.sh` into a reusable helper so both the
yearly and bimonthly scripts call it. **Yearly script's `docker run` arguments
must remain byte-identical.** The only acceptable behavior change in the
yearly path: the three macOS-override info messages and the two container-name
info messages are written with plain `echo` instead of the outer script's
`log_message` (so they reach cron stdout but not the per-script
`${LOG_DIR}/run_${TIMESTAMP}.log` file). Data flow is unchanged.

**Files:**
- `bin/utils/run_skill_metrics_recalc.sh` (new)
- `bin/yearly_skill_metrics_recalculation.sh` (refactor)

**Depends on:** none

**Parallel agents:** 1 (Sonnet 4.6, **worktree isolation** — refactor risk)

**Agent instructions must include:**

1. **Helper file** `bin/utils/run_skill_metrics_recalc.sh` contains:
   - A header comment documenting the preconditions: caller MUST have sourced
     `common_functions.sh` and called `read_configuration` (so that
     `ieasyhydroforecast_*` env vars and `IEASYHYDROHF_HOST` are set);
     caller MUST have already started the SSH tunnel (if needed) and set up
     any outer-script traps. The helper installs no traps of its own.
   - A single function:
     ```
     run_skill_metrics_recalc_once <mode> <log_dir> <timestamp> <container_name>
     ```
     Arguments:
     - `$1` mode — one of `PENTAD`, `DECAD`, `BOTH`, `MONTHLY`, `DAILY`,
       `QUARTERLY`, `SEASONAL`, `ALL`. Passed verbatim as
       `-e SAPPHIRE_PREDICTION_MODE=$1` in the docker run. The helper does
       NOT read ambient `SAPPHIRE_PREDICTION_MODE`.
     - `$2` log_dir — absolute path for service-log file.
     - `$3` timestamp — used in the service-log filename.
     - `$4` container_name — used as `--name` AND as the stem of the
       service-log filename: `${log_dir}/${container_name}_${timestamp}.log`.
   - Inside the function, in this order:
     1. Compute `SERVICE_LOG="${log_dir}/${container_name}_${timestamp}.log"`.
     2. macOS override block: replicate current `yearly:86-95` logic
        verbatim, but replace the `log_message` calls with plain `echo` to
        stdout. Produces local `DOCKER_HOST_OVERRIDE` string.
     3. Image resolution: replicate `IMAGE_ID` setup from `yearly:65`
        (`mabesa/sapphire-postprocessing:${ieasyhydroforecast_backend_docker_image_tag:-latest}`).
        **Do NOT** move the image-existence check or `docker pull` into the
        helper — those stay in the outer script (called once per invocation,
        not once per mode).
     4. Pre-cleanup: remove any stale container matching `$container_name`,
        replicating `yearly:105-109` verbatim (with `echo` instead of
        `log_message`).
     5. `docker run` block: replicate `yearly:112-127` verbatim except:
        - `--name $CONTAINER_NAME` → `--name "$4"`
        - `-e SAPPHIRE_PREDICTION_MODE=${SAPPHIRE_PREDICTION_MODE:-BOTH}`
          → `-e SAPPHIRE_PREDICTION_MODE=$1`
        - `${IMAGE_ID}` stays
        - All other env vars (`ieasyhydroforecast_data_root_dir`,
          `ieasyhydroforecast_env_file_path`, `SAPPHIRE_OPDEV_ENV=True`,
          `IN_DOCKER=True`), volumes, and memory limits
          (`--memory=8g --memory-swap=12g`) are preserved **byte-identical**.
     6. Exit-code capture: replicate `yearly:129-139` verbatim, with `echo`
        instead of `log_message`.
     7. Post-run cleanup: `docker rm -f "$4" 2>/dev/null` (replicates
        `yearly:142`).
     8. `return "$CONTAINER_EXIT_CODE"`.
   - **Must NOT** register any `trap`. **Must NOT** call `exit`.

2. **Yearly script** `bin/yearly_skill_metrics_recalculation.sh`:
   - Keep lines 1-85 unchanged (banner, `read_configuration`, env validation,
     log dir creation, `TIMESTAMP`, `log_message` function, `docker info`
     check, image-pull block, `establish_ssh_tunnel`, `trap cleanup EXIT`,
     the 8g/12g `MEMORY_LIMIT`/`MEMORY_SWAP` vars — these become unused in
     the refactored version but leave them alone to minimize diff).
   - Source the helper after the image-pull block:
     ```bash
     source "$(dirname "$0")/utils/run_skill_metrics_recalc.sh"
     ```
   - Replace lines 86-142 (macOS override through final `docker rm`) with:
     ```bash
     run_skill_metrics_recalc_once \
       "${SAPPHIRE_PREDICTION_MODE:-BOTH}" \
       "$LOG_DIR" \
       "$TIMESTAMP" \
       "postprc-skill-recalc"
     ```
   - Keep lines 144-152 unchanged (log-rotation `find`, trailing banners).
   - Note: container name stays exactly `postprc-skill-recalc`; service log
     filename stays `${LOG_DIR}/postprc-skill-recalc_${TIMESTAMP}.log`.
     **Byte-identical to today.**

3. **Hard rules:**
   - **Do NOT** change `common_functions.sh` or any other shared file.
   - **Do NOT** modify `recalculate_skill_metrics.py` or any Python file.
   - **Do NOT** add, remove, or reorder any env var in the `docker run`.
   - **Do NOT** change memory limits, network mode, volume mounts, or image
     name resolution.
   - **Do NOT** add any `trap` inside the helper.
   - **Do NOT** allow the helper to fall back to ambient
     `SAPPHIRE_PREDICTION_MODE` — if `$1` is empty, exit 2 with an error.

**Acceptance criteria:**
- `bash -n bin/yearly_skill_metrics_recalculation.sh` and
  `bash -n bin/utils/run_skill_metrics_recalc.sh` pass syntax check.
- `shellcheck bin/yearly_skill_metrics_recalculation.sh
  bin/utils/run_skill_metrics_recalc.sh` produces no new warnings beyond
  any already present in the yearly script today.
- Diff check: `diff <(old yearly docker-run args, captured with bash -x)
  <(new yearly docker-run args)` shows no differences in the env vars,
  `--name`, volumes, memory limits, or command. (Agent should capture with
  `bash -x` against a stub `docker` that echoes its args and exits 0. The
  P3 pytest test provides this harness — P1 agent can invoke it manually.)
- No traps are declared inside `run_skill_metrics_recalc.sh` (grep check).

### Phase P2 — New bimonthly wrapper

**Goal:** New script that iterates three modes with log-and-continue, sourcing
the helper from P1. Each mode uses a distinct container name and service-log
filename — no cross-mode collisions.

**Files:** `bin/bimonthly_long_term_skill_metrics_recalculation.sh` (new)

**Depends on:** P1

**Parallel agents:** 1 (Sonnet 4.6)

**Agent instructions must include:**
- Model the structure on `bin/yearly_skill_metrics_recalculation.sh`: source
  `common_functions.sh`, `print_banner`, `read_configuration`, env validation,
  log dir setup using a **distinct** subdir
  `${ieasyhydroforecast_data_root_dir}/logs/skill_metrics_recalc_longterm/`,
  `docker info` check, image pull (once — NOT per mode), SSH tunnel,
  `trap cleanup EXIT`, `TIMESTAMP=$(date +%Y%m%d_%H%M%S)` captured once.
- Source the helper: `source "$(dirname "$0")/utils/run_skill_metrics_recalc.sh"`.
- Iterate modes in this exact order: `MONTHLY QUARTERLY SEASONAL` (literal
  bash array or for-string).
- For each mode:
  ```
  container_name="postprc-skill-recalc-${mode}"
  run_skill_metrics_recalc_once "$mode" "$LOG_DIR" "$TIMESTAMP" "$container_name"
  rc=$?
  ```
  On `rc != 0`, append `$mode` to a `failed_modes` array and log
  `[WARN] Mode ${mode}: failed with exit ${rc}` (via the outer script's
  `log_message` so it reaches both console and per-script log).
  On `rc == 0`, log `[INFO] Mode ${mode}: success`.
  **Do not `exit`** inside the loop — keep iterating.
- After the loop: print a summary — `[SUMMARY] Completed 3/3 modes, 0
  failures` or `[SUMMARY] Completed 2/3 modes, 1 failure: QUARTERLY`. Exit
  `1` if `failed_modes` is non-empty, else `0`.
- 15-day log rotation `find` at the end (copy the pattern from line 146 of
  the yearly script, adapted to the new log dir).
- **Do NOT** reuse the yearly script's log directory — this job has its own
  retention.
- **Do NOT** export `SAPPHIRE_PREDICTION_MODE` — pass it to the helper as
  the positional argument. Any export would leak into other tools and
  contradict the helper's "no ambient env" rule.
- **Do NOT** call `establish_ssh_tunnel` per-mode — it is already called
  once before the loop. (See Assumptions & Risks below for rationale.)

**Acceptance criteria:**
- `bash -n bin/bimonthly_long_term_skill_metrics_recalculation.sh` passes.
- Shellcheck produces no new warnings.
- Dry inspection: the script's mode iteration uses literal
  `MONTHLY QUARTERLY SEASONAL` in that order; container names are
  `postprc-skill-recalc-${mode}`.
- No `SAPPHIRE_PREDICTION_MODE=` export anywhere in the file (grep check).

### Phase P3 — Test harness (pytest, subprocess-driven)

**Goal:** Two pytest test modules. **P3a** exercises the helper with a
stubbed `docker` to confirm the helper plumbs `SAPPHIRE_PREDICTION_MODE`
and `--name` correctly and does not leak a trap. **P3b** exercises the
bimonthly wrapper with the helper replaced by a recorded fake — asserts
mode order, log-and-continue, summary, and exit-code behavior. P3b does
not need a docker stub.

**Files:** `apps/pipeline/tests/test_bimonthly_skill_recalc.py` and
`apps/pipeline/tests/test_run_skill_metrics_recalc_helper.py`
(**subject to convention check** — agent must first look for existing
shell-script tests in `apps/pipeline/tests/`, `bin/tests/`, and
`apps/*/tests/` to pick the right location; pytest + `subprocess.run` is the
default if no prior convention).

**Depends on:** P2

**Parallel agents:** 1 (Sonnet 4.6)

**Agent instructions must include:**

1. **Convention check first.** Grep for `subprocess.run(["bash"` across
   `apps/*/tests/` to see if any existing test drives a shell script. If
   yes, follow that pattern. If no, use the pattern below.

2. **P3a — helper unit test** (`test_run_skill_metrics_recalc_helper.py`):
   - Create a temp directory per test. Drop a stub `docker` bash script
     into it that:
     - For `docker info`, `docker image inspect`, `docker ps`, `docker
       inspect`, `docker rm`, `docker pull`: exit 0 (silent success).
     - For `docker run`: write all args to
       `$STUB_LOG_FILE` (env var), then exit with
       `$STUB_RUN_EXIT_CODE` (default 0).
   - Set `PATH="$tmpdir:$PATH"` and minimal env
     (`ieasyhydroforecast_data_root_dir`, `ieasyhydroforecast_env_file_path`,
     `ieasyhydroforecast_data_ref_dir`,
     `ieasyhydroforecast_container_data_ref_dir`,
     `ieasyhydroforecast_backend_docker_image_tag=latest`,
     `IEASYHYDROHF_HOST=someremote:80`).
   - Source the helper inside a short bash one-liner that calls it, e.g.:
     ```
     bash -c 'source bin/utils/run_skill_metrics_recalc.sh && \
       run_skill_metrics_recalc_once MONTHLY /tmp/lg 20260422_100000 test-container'
     ```
   - Assertions:
     - Stub log file contains `-e SAPPHIRE_PREDICTION_MODE=MONTHLY`.
     - Stub log file contains `--name test-container`.
     - Stub log file does NOT contain `SAPPHIRE_PREDICTION_MODE=BOTH`
       (i.e. the helper ignores any ambient `SAPPHIRE_PREDICTION_MODE`
       even when set in the parent env — add a test case that exports
       `SAPPHIRE_PREDICTION_MODE=DECAD` before invocation and confirms
       `MONTHLY` still wins).
     - Helper exits with the stubbed run exit code (test both 0 and 42).
     - `grep -c '^trap ' bin/utils/run_skill_metrics_recalc.sh` returns 0.
     - Calling the helper with an empty first arg produces exit 2.

3. **P3b — bimonthly wrapper integration test**
   (`test_bimonthly_skill_recalc.py`):
   - Do NOT stub `docker`. Instead, before invoking the bimonthly script,
     write a fake helper into a temp `utils/run_skill_metrics_recalc.sh`
     that ignores all args except `$1` (mode) and writes the mode plus its
     configured exit code to a record file. Copy the bimonthly script
     itself into the temp dir alongside `utils/` so
     `source "$(dirname "$0")/utils/..."` resolves to the fake.
   - Also fake `common_functions.sh` in the temp dir with no-op
     `print_banner`, `read_configuration`, `establish_ssh_tunnel`,
     `cleanup`, and a `log_message` that echoes. Set env vars directly in
     the subprocess environment.
   - Test cases (use pytest parametrize):
     1. All three modes succeed → exit 0, record file shows exactly
        `MONTHLY`, `QUARTERLY`, `SEASONAL` in that order, summary line
        contains `3/3` and `0 failures`.
     2. `QUARTERLY` fails (fake returns 5 for that mode) → exit 1, record
        file shows all three modes ran, summary line mentions `QUARTERLY`
        and only `QUARTERLY`.
     3. `MONTHLY` fails → exit 1, record shows all three modes ran,
        summary mentions `MONTHLY`.
     4. All three fail → exit 1, summary lists all three failed modes.
   - Assert the container-name passed to the fake is
     `postprc-skill-recalc-${mode}` for each mode.

4. **Registration with `run_tests.sh`:** Since the tests are `.py`, they
   are picked up by pytest automatically if placed in a directory already
   covered by `apps/run_tests.sh pipeline`. Agent must verify by running
   `SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` after adding the
   tests and confirming both new test files appear in the collection list.

**Acceptance criteria:**
- Both test files pass locally.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` exits 0
  with zero unexpected skips, and both new test modules are in the
  collection output.
- No change to `apps/run_tests.sh` itself is needed (if one is needed,
  flag back to orchestrator rather than editing).

### Phase P4 — Cron + deployment docs

**Goal:** Document the new cron line. Operator installs it post-merge.

**Files:** `doc/deployment.md`

**Depends on:** P2

**Parallel agents:** 1 (Sonnet 4.6)

**Agent instructions must include:**
- In the "Set up cron job" section (around line 680-742), add an entry
  between the long-term forecast (line 726) and the yearly skill recalc
  (line 739). Cron expression:
  ```
  0 10 10,25 * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/bimonthly_long_term_skill_metrics_recalculation.sh /data/<data_folder>/config/<env_file> >> /home/ubuntu/logs/sapphire_bimonthly_longterm_skill_recalc_$(date +\%Y\%m\%d).log 2>&1
  ```
- Brief note: runs 4h after the 06:00 UTC long-term forecast cron on the 10th
  and 25th. Log-and-continue — one failing mode does not block the others,
  but the job exits non-zero so errors surface in the cron log. The yearly
  Dec 31 entry is retained as a full-history safety net.

**Acceptance criteria:**
- `grep bimonthly_long_term_skill_metrics_recalculation doc/deployment.md`
  returns the new cron line.

### Phase P5 — Verification

**Goal:** Run tests, confirm nothing else regressed.

**Depends on:** P1, P2, P3, P4

**Parallel agents:** 0 (orchestrator runs this)

**Commands:**
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline`
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts`
- `bash -n bin/yearly_skill_metrics_recalculation.sh`
- `bash -n bin/bimonthly_long_term_skill_metrics_recalculation.sh`
- `bash -n bin/utils/run_skill_metrics_recalc.sh`
- Manual diff review: the refactor in P1 must produce the same `docker run`
  invocation as before for the yearly path.

## Dependency graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P1", "P2", "P3", "P4"], "parallel_agents": 0 }
  }
}
```

## Assumptions & risks (acknowledged, not fixed in this PR)

1. **SSH tunnel lifetime across the three sequential recalcs.** The tunnel is
   established once by the outer bimonthly wrapper. Three back-to-back
   recalcs could run long enough for the tunnel to idle-drop, which would
   fail subsequent modes. `establish_ssh_tunnel` is not called per-mode —
   doing so here would complicate trap ownership. **Mitigation:** first real
   run is monitored; if the second or third mode fails with a connection
   error, add per-mode tunnel re-check in a follow-up.

2. **Runtime budget / 4 h buffer after the 06:00 UTC long-term forecast
   cron.** Three 8 GB skill recalcs may take 60-120+ minutes in total. If LT
   forecasts occasionally overrun past 10:00 UTC, the bimonthly job could
   start before forecasts finish, producing incomplete metrics. No automated
   dependency is added here — we rely on typical timing. **Mitigation:**
   after the first scheduled run, record actual runtimes and confirm the
   buffer; push to 12:00 UTC if marginal.

3. **Idempotence of `recalculate_skill_metrics.py`.** Rationale rests on
   re-runs being safe (upsert, not append). Spot-check on the first real
   bimonthly run: invoke twice and confirm the second invocation does not
   duplicate rows in the CSV backup or API. If it does, we'll need an
   early-exit-if-marker-exists guard. **Not in scope for this PR** — treat
   as a post-deploy verification step.

4. **`recalculate_skill_metrics.py` left unmodified.** Any change to that
   script's mode handling would invalidate this plan. Out of scope — flag in
   review if anyone proposes touching it concurrently.

## PR shape

**P1 + P2 land as a single PR.** The refactor (P1) exists to enable the new
bimonthly feature (P2); reviewing them separately would obscure the
motivation. P3 (tests) and P4 (docs) also land in the same PR — CLAUDE.md
requires tests alongside any feature change. Net: one focused PR containing
the helper, the yearly refactor, the new bimonthly script, both test files,
and the cron doc update.

## Rollout (post-merge, operator side)

1. Pull `maxat_sapphire_2` on server.
2. **One-off catch-up run** to unblock Kyrgyz staging today:
   `bash bin/bimonthly_long_term_skill_metrics_recalculation.sh <env_file>`
3. **Idempotence spot-check** (see Assumption 3): run step 2 again
   immediately. Confirm no duplicate rows in skill-metric storage.
4. Add the cron line from `doc/deployment.md` to the system crontab.
5. After the next scheduled 10th/25th tick, verify monthly skill tiles
   populate on the dashboard and record the actual runtime (for Assumption 2).
