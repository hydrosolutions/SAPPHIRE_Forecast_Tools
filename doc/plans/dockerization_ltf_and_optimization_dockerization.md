# Plan: CPU-Only PyTorch + Dockerize Long-Term Forecasting

**Status: Ready** | INFRA-008 | Reviewed 2026-03-05

## Context

SAPPHIRE runs on standard AWS EC2 instances and aging hydromet server infrastructure
with no GPUs. Both the `machine_learning` and `long_term_forecasting` modules use
PyTorch but operate exclusively in CPU mode. Currently:

- **ML module**: Docker image includes ~3 GB of unused CUDA/nvidia libraries
  (torch==2.8.0 from PyPI resolves CUDA-enabled wheels)
- **LT module**: Has no Dockerfile at all; its uv.lock resolves torch with
  CUDA deps (`nvidia-*`, `cuda-bindings`)

Additionally, the upstream `lt-forecasting` package (hydrosolutions/long-term-forecasting)
has been updated by a colleague with relaxed dependency pins. The current
`override-dependencies` for pandas and scikit-learn may no longer be needed.

This plan switches both modules to CPU-only PyTorch wheels, updates the
`lt-forecasting` dependency, and creates the LT Dockerfile.

---

## Phase 1: ML module -- CPU-only PyTorch

### 1.1 Edit `apps/machine_learning/pyproject.toml`

Edit the existing `[tool.uv]` and `[tool.uv.sources]` sections (starting at
line 37) to add the CPU-only index and torch source:

```toml
[tool.uv]
allow-insecure-host = []

[[tool.uv.index]]
name = "pytorch-cpu"
url = "https://download.pytorch.org/whl/cpu"
explicit = true

[tool.uv.sources]
iEasyHydroForecast = { path = "../iEasyHydroForecast", editable = true }
torch = { index = "pytorch-cpu" }
```

### 1.2 Regenerate `apps/machine_learning/uv.lock`

```bash
cd apps/machine_learning && uv lock
```

Verify: lock file should no longer contain any `nvidia-*` or `triton` packages.

### 1.3 Verify locally

```bash
cd apps/machine_learning
uv sync --frozen --no-dev
uv run python -c "import torch; print(torch.__version__); print('CUDA:', torch.cuda.is_available())"
# Expected: 2.8.0+cpu, CUDA: False
```

### 1.4 Run tests

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning
```

---

## Phase 2: LT module -- update lt-forecasting + CPU-only PyTorch

This phase has two sub-steps to isolate dependency-tree changes from the CPU
index change. If `uv lock` fails, this makes it clear which change caused it.

### 2.1 Update `lt-forecasting` and attempt override removal

First, update the upstream package and try removing the overrides:

1. In `[tool.uv.sources]`, update the `lt-forecasting` git ref to the latest
   commit from the colleague's update:

   ```toml
   lt-forecasting = { git = "https://github.com/hydrosolutions/long-term-forecasting.git", rev = "<new-commit-hash>" }
   ```

2. Remove the `override-dependencies` block:

   ```toml
   # REMOVE these lines from [tool.uv]:
   override-dependencies = [
       "pandas>=2.2.2",
       "scikit-learn>=1.5.0",
   ]
   ```

3. Run `uv lock` (without the CPU index change yet):

   ```bash
   cd apps/long_term_forecasting && uv lock
   ```

4. If it resolves cleanly, the overrides are no longer needed.
5. If it fails with a pandas or scikit-learn conflict, re-add only the
   override(s) that are still required and re-run `uv lock`.

### 2.2 Add CPU-only PyTorch index

Edit the existing `[tool.uv]` and `[tool.uv.sources]` sections to add the
CPU-only index and torch source. The final result (assuming overrides were
successfully removed) should look like:

```toml
[tool.uv]
allow-insecure-host = []

[[tool.uv.index]]
name = "pytorch-cpu"
url = "https://download.pytorch.org/whl/cpu"
explicit = true

[tool.uv.sources]
iEasyHydroForecast = { path = "../iEasyHydroForecast", editable = true }
lt-forecasting = { git = "https://github.com/hydrosolutions/long-term-forecasting.git", rev = "<new-commit-hash>" }
torch = { index = "pytorch-cpu" }
```

Note: `torch = { index = "pytorch-cpu" }` in `[tool.uv.sources]` applies to
all resolutions of the `torch` package -- both the direct dependency and
`lt-forecasting`'s transitive torch dependency. However, the PyTorch CPU index
sometimes lags behind PyPI by a few days for new releases. If `uv lock` fails
because a torch version isn't available on the CPU index, pin torch to the
latest version available there.

**Risk (verified 2026-03-06):** The current `pyproject.toml` specifies
`torch>=2.9.0`. The PyTorch CPU index may not yet have 2.9.x wheels. If
`uv lock` fails after adding the CPU index, either relax the pin to
`torch>=2.8.0` or pin to the latest version available on the CPU index
(e.g. `torch==2.8.0`). The ML module pins `torch==2.8.0` so 2.8.0+cpu is
known to work.

### 2.3 Regenerate `apps/long_term_forecasting/uv.lock`

```bash
cd apps/long_term_forecasting && uv lock
```

Verify: lock file should no longer contain `nvidia-*`, `cuda-bindings`, or `triton`.

### 2.4 Verify locally

```bash
cd apps/long_term_forecasting
uv sync --frozen --no-dev
uv run python -c "import torch; import catboost; import lightgbm; import xgboost; import sapphire_api_client; from ieasyhydro_sdk.sdk import IEasyHydroSDK; print('OK')"
```

---

## Phase 3: Create LT Dockerfile

### 3.1 Create `apps/long_term_forecasting/Dockerfile`

Follows the ML module Dockerfile pattern exactly:

```dockerfile
# syntax=docker/dockerfile:1

# Python 3.12 + uv image for long_term_forecasting module
# CPU-only PyTorch configured via pyproject.toml [tool.uv.sources]

FROM mabesa/sapphire-pythonbaseimage:latest AS base

ENV SAPPHIRE_OPDEV_ENV=True
ENV IN_DOCKER=True

USER root
WORKDIR /app

COPY apps/iEasyHydroForecast /app/apps/iEasyHydroForecast
COPY apps/long_term_forecasting /app/apps/long_term_forecasting

WORKDIR /app/apps/long_term_forecasting
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# RUN_MODE selects entry point:
#   "forecast" (default): operational (run_forecast.py --all)
#   "maintenance":        calibration + hindcasting (calibrate_and_hindcast.py --all)
#   "maintenance_tune":   calibration with hyperparameter tuning (--all only,
#                         no way to tune a subset of models via Docker)
# lt_forecast_mode env var (e.g. "month_1") is read by the scripts directly.
CMD ["sh", "-c", \
    "if [ \"$RUN_MODE\" = \"maintenance\" ]; then \
        uv run calibrate_and_hindcast.py --all; \
    elif [ \"$RUN_MODE\" = \"maintenance_tune\" ]; then \
        uv run calibrate_and_hindcast.py --all --tune_hyperparameters; \
    else \
        uv run run_forecast.py --all; \
    fi"]
```

Notes:
- No `apt-get install` needed -- geospatial packages (fiona, pyproj, shapely)
  and `psycopg2-binary` ship manylinux wheels with bundled native libraries.
  If `psycopg2-binary` is ever replaced with `psycopg2`, a `libpq-dev` install
  step will be needed.
- `lt_forecast_mode` is read by the Python scripts via `os.getenv()`, no CMD
  logic needed for it

---

## Phase 4: Docker smoke tests

### 4.1 Update `apps/run_docker_tests.sh`

Add to TARGETS array (after the `ml` entry, line 82):

```bash
"ltforecast|mabesa/sapphire-ltforecast|apps/long_term_forecasting/Dockerfile|import torch; import catboost; import lightgbm; import xgboost; import sapphire_api_client; from ieasyhydro_sdk.sdk import IEasyHydroSDK; import pandas; import numpy|"
```

Update `print_usage()` to list `ltforecast` as a target.

Update the valid targets error message in `main()` (line 440).

Add `--skip-lt` flag (mirrors `--skip-ml`) since this is also a large image
(PyTorch + catboost + lightgbm + xgboost + pytorch-lightning).

### 4.2 Verify

```bash
bash apps/run_docker_tests.sh ltforecast
```

---

## Phase 5: CI/CD integration

### 5.1 Update `.github/workflows/build_test.yml`

Add env var:
```yaml
LT_IMAGE_NAME: mabesa/sapphire-ltforecast
```

Add `test_long_term_forecasting` job (follows `test_machine_learning` pattern):
- Install uv + Python 3.12
- `uv sync --all-extras` in `apps/long_term_forecasting`
- Verify imports: torch, catboost, lightgbm, xgboost, sapphire_api_client,
  ieasyhydro_sdk, pandas, numpy

Add `build_long_term_forecasting` job (follows `build_machine_learning` pattern):
- `needs: [test_long_term_forecasting, build_base_image]`
- `timeout-minutes: 60`
- Free disk space step (same as ML job)
- Build base image first, then build LT image (note: `build_postprocessing`
  currently omits the base image build step -- do NOT copy that pattern)

Add `build_long_term_forecasting` to `summarize_builds.needs` array.

Add LT image to the summary echo list.

---

## Files Modified

| File | Action |
|------|--------|
| `apps/machine_learning/pyproject.toml` | Add CPU-only torch index |
| `apps/machine_learning/uv.lock` | Regenerate (smaller, no CUDA) |
| `apps/long_term_forecasting/pyproject.toml` | Update lt-forecasting ref, remove overrides, add CPU-only torch index |
| `apps/long_term_forecasting/uv.lock` | Regenerate (smaller, no CUDA) |
| `apps/long_term_forecasting/Dockerfile` | **New file** |
| `apps/run_docker_tests.sh` | Add ltforecast target + --skip-lt flag |
| `.github/workflows/build_test.yml` | Add test + build jobs for LT |

## Deferred (separate tasks)

- Luigi pipeline integration (`apps/pipeline/pipeline_docker.py`) -- requires
  designing dependency graph for LT in the pipeline
- `deploy_main.yml` updates -- done when merging to main
- Docker image size audit after CPU switch
- `.dockerignore` for LT and ML modules (excludes `tests/`, `dev_code/`,
  `__pycache__/`, `.venv/` from build context)

## Verification

1. `uv lock` succeeds for LT without `override-dependencies` (or with minimal overrides)
2. `uv sync --frozen --no-dev` succeeds for both modules
3. `import torch; torch.cuda.is_available()` returns `False` in both
4. `SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning` passes
5. `bash apps/run_docker_tests.sh ml` passes (CPU-only ML image)
6. `bash apps/run_docker_tests.sh ltforecast` passes (new LT image)
7. Push branch, verify CI jobs pass in GitHub Actions

## Expected Savings

- ML image: ~3 GB smaller (CUDA libs removed)
- LT image: estimated ~2-2.5 GB (CPU-only torch + catboost/lightgbm/xgboost
  is still substantial; previous ~3.5 GB estimate included CUDA overhead)
- CI build times: significantly faster (fewer/smaller downloads)
