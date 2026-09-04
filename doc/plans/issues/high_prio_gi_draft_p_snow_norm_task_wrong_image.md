# P-061 — The yearly snow-norm task runs in an image that does not contain its script

**Module:** `pipeline`
**Priority:** High
**Status:** Review — P1 implemented, P2 verified 2026-09-04
**Found:** 2026-09-04, while reviewing PREPG-022
**Blocks:** PREPG-022 (`high_prio_gi_draft_prepg_snow_norms_hydrological_window.md`)
**Related:** P-007 (recorded the same gap in passing, `doc/plans/module_issues.md:76`)

---

## Problem

The documented 31-August cron (`doc/deployment.md:1008`) runs
`bin/run_periodic_maintenance.sh snow_norms`, which routes through Luigi to
`YearlySnowNormRecalculation` (`apps/pipeline/pipeline_docker.py:2011-2039`).
That task launches:

```python
image_name="sapphire-pipeline",              # pipeline_docker.py:2031
command=["uv", "run", "recalculate_snow_norms.py"],
```

`recalculate_snow_norms.py` lives in `apps/preprocessing_gateway/`. The
`sapphire-pipeline` image does not contain it: `apps/pipeline/Dockerfile` copies
only `apps/iEasyHydroForecast` (`:20`) and `apps/pipeline` (`:23`), and
`_standard_maintenance_volumes()` (`pipeline_docker.py:1668-1682`) mounts config
and intermediate data only — no source. So the scheduled yearly snow-norm
recalculation cannot execute on any server.

`YearlySnowNormRecalculation` is the **only** task in `pipeline_docker.py` that
uses `sapphire-pipeline`; all eight other module tasks use their own module image
(`sapphire-prepgateway`, `sapphire-preprunoff`, `sapphire-postprocessing`, …).
This reads as a copy-paste slip, not a design choice.

**Impact.** Snow norms, climatology statistics and percentile bands have not been
refreshed by the scheduled job. Whatever is in the servers' `snow` tables was
written by a manual run of the legacy wrapper or by a backfill. The failure is
quiet: P-007 separately records that container exit codes are discarded, so the
Luigi task reports success either way, and the marker file is still written.

## Fix

```python
image_name="sapphire-prepgateway",
```

`apps/preprocessing_gateway/Dockerfile` copies the gateway source (`:23`), runs
`uv sync --frozen --no-dev` against the gateway's own `pyproject.toml`/`uv.lock`
(`:27-29`), and leaves `WORKDIR /app/apps/preprocessing_gateway` (`:27`) — so
`uv run recalculate_snow_norms.py` resolves the script *and* its dependencies
with no other change. `GatewayMaintenance` (`pipeline_docker.py:1700-1715`)
already runs that image with the identical `_standard_maintenance_volumes()` /
`_common_maintenance_env()` pair, so the volume and env shape is proven.

The legacy wrapper `bin/yearly_snow_norm_recalculation.sh:74` independently
confirms the intended image: `mabesa/sapphire-prepgateway`.

### Rejected alternative

Adding `COPY apps/preprocessing_gateway` to `apps/pipeline/Dockerfile` would also
need the gateway's dependencies installed into the pipeline venv (the image runs
`uv sync` against `apps/pipeline` only, `Dockerfile:26-28`), enlarging a shared
image to carry a second module. The image swap is strictly smaller and matches
every sibling task.

---

## Phases

### P1 — Swap the image and pin it

**Goal:** the scheduled task runs the script; a regression is caught by a test.

**Files (only these):**
- `apps/pipeline/pipeline_docker.py` — line 2031 only, plus the
  `YearlySnowNormRecalculation` docstring
- `apps/pipeline/tests/test_maintenance_tasks.py` — add tests

**Depends on:** —
**Agents:** 1

**Scope:**
- Change `image_name="sapphire-pipeline"` to `image_name="sapphire-prepgateway"`.
- Extend the docstring to name the image and why (the script lives in
  `preprocessing_gateway`).
- Add a test asserting `YearlySnowNormRecalculation` requests
  `sapphire-prepgateway` and the command `["uv", "run", "recalculate_snow_norms.py"]`
  — patch `run_docker_container` and assert on the recorded kwargs, following the
  existing patterns in `test_maintenance_tasks.py`.
- Add a companion test asserting **every** `DockerTaskBase` subclass in
  `pipeline_docker.py` that overrides `command` names an image whose module
  directory contains that script. If a cheap structural form of this is not
  available, instead assert that no maintenance task requests
  `sapphire-pipeline` — that is the specific slip, and it is trivially checkable.

**Do NOT** change the schedule, the task name, the marker path, the retry/timeout
settings, `mem_limit`/`memswap_limit`, the volumes or the environment list.
Do NOT touch `apps/pipeline/Dockerfile`. Do NOT touch
`recalculate_snow_norms.py` — that is PREPG-022.

**Acceptance criteria:**
- `YearlySnowNormRecalculation` requests `sapphire-prepgateway`.
- Marker path, container name (`maintenance-snow-norms`) and log path unchanged.
- `test_snow_norms_routing` and the marker test (`test_maintenance_tasks.py:295-300`,
  `:328-337`) pass unmodified.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh pipeline` — zero fail, zero unexpected skip.
- Then the full `SAPPHIRE_TEST_ENV=True bash run_tests.sh`.

### P2 — Verify the container actually starts

**Goal:** evidence the script is reachable, not just that a string changed.

**Depends on:** P1
**Agents:** 0 (orchestrator)

**DONE 2026-09-04.** Verified directly against the published images (not repo
source — the deployed image could have diverged, which was the one thing that
could have falsified the premise):

```
# OLD image — the bug, confirmed in the production artifact
$ docker run --rm --platform linux/amd64 --entrypoint sh mabesa/sapphire-pipeline:latest \
    -c 'pwd; ls /app/apps/; find /app -name recalculate_snow_norms.py'
/app
iEasyHydroForecast
pipeline
(find returns nothing — the script is absent)

# NEW image — the fix
$ docker run --rm --platform linux/amd64 --entrypoint sh mabesa/sapphire-prepgateway:latest \
    -c 'pwd; ls recalculate_snow_norms.py; uv run python -c "import dg_utils, recalculate_snow_norms"'
/app/apps/preprocessing_gateway
-rw-r--r-- 1 root root 16671 Sep  2 11:34 recalculate_snow_norms.py
SAPPHIRE_API_AVAILABLE = True
has recalculate_norms   = True
```

This settles both halves: the script is genuinely absent from the image the
schedule was using, and genuinely present *and importable with its API client
installed* in the image it now uses. `SAPPHIRE_API_AVAILABLE = True` matters
specifically — had `sapphire-api-client` been missing from the gateway image,
`recalculate_norms()` would return `False` early and the job would still do
nothing, just for a new reason.

Not covered by this check: a real end-to-end run against a configured
deployment. The import-level proof plus the identical volume/env shape already
proven by `GatewayMaintenance` is the evidence available without an operator
env file.

---

## Dependency graph

```json
{
  "phases": {
    "P1": { "depends_on": [], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 0 }
  }
}
```

---

## Out of scope

- P-007's discarded container exit codes. This issue makes the task *able* to
  run; P-007 is why nobody noticed it wasn't. Both are needed; they are separate.
- The write-range defect itself — PREPG-022.
- The cadence (31 August stands, owner decision 2026-08-19).
- Any other task's image assignment beyond the assertion added in P1.
