# Updating a Local Dev Deployment

**Audience:** A developer who already has the SAPPHIRE forecast tools running
locally (uv, Docker, module venvs, a working `.env`) and needs to bring it
back in sync after a stretch of upstream changes — then run the
`run_locally.sh` pipelines.

**If you are setting up from scratch** (no working install yet), follow
[`doc/development.md`](../development.md) first, then come back here.

This runbook uses `.env_sandro_kghm` as the example env file and the Kyrgyz
(`kghm`) organization throughout. Substitute your own env file path.

---

## Why this is needed

Three things go stale after upstream changes and must be refreshed in order:

1. **Module venvs** — the `sapphire-api-client` pin and other dependencies move
   over time. Each module's `.venv` must be re-synced or the pipeline runs
   against an old client (symptoms: import errors, `422` responses from the
   APIs). All modules pin the **same** client commit, so there is no per-module
   divergence to chase.
2. **The services database schema** — the FastAPI services create their schema
   with `Base.metadata.create_all()`, **not** Alembic migrations. New columns
   only appear if you drop and recreate the DB volumes. Pulling new code is not
   enough.
3. **`run_locally.sh` does not create venvs** — it only *checks* for
   `apps/<module>/.venv/bin/python` and aborts if one is missing. Syncing is on
   you.

---

## Step 0 — Pull the latest code

```bash
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools   # adjust to your clone path
git checkout develop_two_model_ensemble          # or the branch you were told to use
git pull
```

Confirm you are on the expected branch and commit before continuing:

```bash
git status
git log --oneline -5
```

---

## Step 1 — Re-sync every module venv (the critical step)

Run this from the repo root. It re-syncs all modules the pipeline touches,
including `iEasyHydroForecast` (the shared library) and `validate_pipeline`
(used for the post-run API checks):

```bash
for m in preprocessing_runoff preprocessing_gateway linear_regression \
         machine_learning postprocessing_forecasts long_term_forecasting \
         iEasyHydroForecast validate_pipeline; do
  echo "=== syncing $m ===" && ( cd "apps/$m" && uv sync --all-extras ) || break
done
```

Notes:

- Use **`--all-extras`**, not plain `uv sync` — the extras include the
  dev/test dependencies the validation steps rely on. This is the exact command
  `run_locally.sh` points you to when a venv is missing or stale.
- `uv sync` will fetch the new `sapphire-api-client` git commit automatically.
  If a sync fails on the client, the commit hash in the module's
  `pyproject.toml` may not exist yet in the client repo — check that you pulled
  the latest app code (Step 0).
- If you prefer to pin the interpreter explicitly: `uv sync --all-extras
  --python 3.12`.

---

## Step 2 — Bring the services up and reset the database

The pipeline reads observations from the **preprocessing** API and writes
forecasts/skill metrics to the **postprocessing** API. They must be running and
healthy, and the schema must match the new code.

```bash
# Start the services if they aren't already up
cd sapphire && docker compose up -d && cd ..

# Reset the DB so the schema matches the new client/code.
# This: stop -> drop volumes -> rebuild -> start -> health-check -> migrate -> verify
bash bin/reset_sapphire_db.sh
```

Useful flags for `reset_sapphire_db.sh`:

| Flag | Effect |
|------|--------|
| `--preprocessing-only` | Reset only the preprocessing DB |
| `--postprocessing-only` | Reset only the postprocessing DB |
| `--skip-rebuild` | Reuse current images (skip the Docker rebuild) |
| `--skip-migration` | Recreate volumes but don't run the data migration |
| `-y` | Don't prompt for confirmation |

> **Heads up — the migration is slow.** Restoring the postprocessing DB
> (months 1–9 + seasonal forecasts) can take **~3 hours**. If you only need
> short-term pentad/decade work right now, you can scope the reset
> (`--preprocessing-only` plus a lighter postprocessing migration) — but a full
> reset is the safe default after a long gap.

Verify the gateway is ready before moving on:

```bash
curl http://localhost:8000/health/ready
```

> **If the services are down or unhealthy**, modules silently fall back to CSV
> output and your forecasts never reach the database — dashboards then show
> stale data. Always confirm `health/ready` returns OK before a real run.

---

## Step 3 — Dry-run validation (do this before any real run)

`--dry-run` checks that your env file exists and that **every** required module
venv is present, without executing anything:

```bash
ieasyhydroforecast_env_file_path=apps/config/.env_sandro_kghm \
  bash apps/run_locally.sh --dry-run short-term
```

Expected: it lists `venv: <module>/.venv/bin/python` as OK for each module and
reports the environment is valid.

If it reports **`Missing venv: apps/<module>/.venv/bin/python`**, that module's
sync in Step 1 failed — re-run `cd apps/<module> && uv sync --all-extras` and
read the error.

---

## Step 4 — Run a pipeline

```bash
SAPPHIRE_PREDICTION_MODE=PENTAD \
  ieasyhydroforecast_env_file_path=apps/config/.env_sandro_kghm \
  bash apps/run_locally.sh short-term
```

Common targets (run `bash apps/run_locally.sh --help` for the full list):

| Target | What it runs |
|--------|--------------|
| `short-term` | Preprocessing → ML/LR → postprocessing (pentad/decade) |
| `daily` | Full daily run: short-term + maintenance + long-term (if near an issue day) |
| `all` | Short-term **and** long-term |
| `<module>` | A single module, e.g. `preprocessing_runoff` |
| `maintenance` | Gap-fill, hindcast, recalculation tasks |
| `yearly` | Recalculate skill metrics + snow norms |

Logs are written to `apps/logs/run_locally_<TIMESTAMP>.log`.

---

## Gotchas

- **`kghm` runs all modules.** The org skip-list only applies to `demo` and
  `uzhm` (which skip `preprocessing_gateway`, `machine_learning`, and
  `long_term_forecasting`). With a `kghm` env file, all modules run — so all
  their venvs must be synced (Step 1).
- **`SAPPHIRE_PREDICTION_MODE` is case-sensitive** — use `PENTAD`, `DECAD`, or
  `BOTH` in uppercase.
- **macOS bash.** The script needs bash ≥ 4.4 (arrays, `set -u`). The system
  bash is 3.2 — install a modern one with `brew install bash` if `run_locally.sh`
  fails with syntax errors.
- **The env file is yours, not in the repo.** `.env_sandro_kghm` holds paths
  and credentials. `run_locally.sh` does not source it — it greps specific keys
  (`ieasyhydroforecast_organization`, `ieasyhydroforecast_START_DATE`) from it
  and passes `ieasyhydroforecast_env_file_path` down to each module. Shell env
  vars take precedence over values in the file.

---

## Quick reference (copy-paste, full refresh)

```bash
# 0. Latest code
cd ~/Documents/GitHub/SAPPHIRE_forecast_tools && git pull

# 1. Re-sync all module venvs
for m in preprocessing_runoff preprocessing_gateway linear_regression \
         machine_learning postprocessing_forecasts long_term_forecasting \
         iEasyHydroForecast validate_pipeline; do
  ( cd "apps/$m" && uv sync --all-extras ) || break
done

# 2. Services up + DB reset
cd sapphire && docker compose up -d && cd ..
bash bin/reset_sapphire_db.sh
curl http://localhost:8000/health/ready

# 3. Validate
ieasyhydroforecast_env_file_path=apps/config/.env_sandro_kghm \
  bash apps/run_locally.sh --dry-run short-term

# 4. Run
SAPPHIRE_PREDICTION_MODE=PENTAD \
  ieasyhydroforecast_env_file_path=apps/config/.env_sandro_kghm \
  bash apps/run_locally.sh short-term
```
