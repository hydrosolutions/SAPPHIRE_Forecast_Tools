# PP-035: FrontendUpdate fails — docker CLI not available in pipeline container

**Module**: pipeline
**Priority**: Low
**Labels**: `bug`, `pipeline`, `infrastructure`

---

## Symptom

Daily maintenance completes 12/15 tasks successfully but `FrontendUpdate`
fails with:

```
FileNotFoundError: [Errno 2] No such file or directory: 'docker'
```

The dashboard is not automatically restarted after maintenance. Manual
workaround: `docker restart sapphire-dashboard` on the host.

## Root Cause

`FrontendUpdate` (`pipeline_docker.py:1879-1936`) calls the `docker` CLI
binary via `subprocess.run(["docker", "compose", ...])` to cycle the
dashboard containers. However, the pipeline Docker image does not install
the `docker` CLI — only the Python `docker` SDK (`docker>=7.0.0` in
`pyproject.toml`).

| Component | Status |
|-----------|--------|
| `/var/run/docker.sock` mounted | Yes (`docker-compose-luigi.yml:37`) |
| GID for socket access | Yes (`docker-compose-luigi.yml:43`) |
| Python `docker` SDK | Installed (`pyproject.toml:11`) |
| `docker` CLI binary | **Missing** — not in any Dockerfile |

## Impact

- Dashboard does not auto-refresh after daily maintenance
- `SendPipelineCompletionNotification` and `RunDailyMaintenanceWorkflow`
  are marked as failed dependencies
- No data loss — all forecast/preprocessing tasks succeed independently

## Options

### Option A: Install docker CLI in the pipeline image (minimal fix)

Add to `apps/pipeline/Dockerfile`:

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*
```

Pros: smallest code change, `FrontendUpdate` works as-is.
Cons: increases image size, adds apt dependency.

### Option B: Rewrite FrontendUpdate to use Python docker SDK

Replace `subprocess.run(["docker", ...])` calls with equivalent Python
`docker` SDK calls using the already-installed `docker` package. The SDK
communicates directly with the Docker socket.

Pros: no image changes, more idiomatic, consistent with other Docker tasks
in the pipeline (e.g., `DockerTaskBase` already uses the SDK).
Cons: more code to change, `docker compose` operations (down/up) are not
trivially available in the SDK (would need to use `docker.from_env()` to
stop/remove/pull/start containers individually).

### Option C: Move dashboard restart to the host

Remove `FrontendUpdate` from the Luigi DAG entirely. Add a simple
`docker restart sapphire-dashboard` step to the cron job after the
maintenance script completes.

Pros: simplest, no image or pipeline code changes.
Cons: moves logic outside the orchestrated pipeline.

## Recommendation

Option C for now (unblocks the pipeline immediately), then Option B when
refactoring the pipeline module.

## Files

- `apps/pipeline/pipeline_docker.py` — `FrontendUpdate` class (line ~1879)
- `apps/pipeline/Dockerfile` — no docker CLI installed
- `bin/docker-compose-luigi.yml` — socket mount config
