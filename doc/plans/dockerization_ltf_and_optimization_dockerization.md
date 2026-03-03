# Plan: CPU-Only PyTorch + Dockerize Long-Term Forecasting

## Context

SAPPHIRE runs on standard AWS EC2 instances and aging hydromet server infrastructure
with no GPUs. Both the `machine_learning` and `long_term_forecasting` modules use
PyTorch but operate exclusively in CPU mode. Currently:

- **ML module**: Docker image includes ~3 GB of unused CUDA/nvidia libraries
  (torch==2.8.0 from PyPI resolves CUDA-enabled wheels)
- **LT module**: Has no Dockerfile at all; its uv.lock resolves torch==2.10.0
  with CUDA deps + `cuda-bindings`

This plan switches both modules to CPU-only PyTorch wheels and creates the LT
Dockerfile, saving ~3 GB per image and enabling LT to run in Docker.

---

## Phase 1: ML module — CPU-only PyTorch

### 1.1 Edit `apps/machine_learning/pyproject.toml`

Add CPU-only index and torch source (lines 37-43):

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
# Expected: 2.8.0, CUDA: False
```

### 1.4 Run tests

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning
```

---

## Phase 2: LT module — CPU-only PyTorch

### 2.1 Edit `apps/long_term_forecasting/pyproject.toml`

Add CPU-only index and torch source (lines 64-75):

```toml
[tool.uv]
allow-insecure-host = []

[[tool.uv.index]]
name = "pytorch-cpu"
url = "https://download.pytorch.org/whl/cpu"
explicit = true

override-dependencies = [
    "pandas>=2.2.2",
    "scikit-learn>=1.5.0",
]

[tool.uv.sources]
iEasyHydroForecast = { path = "../iEasyHydroForecast", editable = true }
torch = { index = "pytorch-cpu" }
```

### 2.2 Regenerate `apps/long_term_forecasting/uv.lock`

```bash
cd apps/long_term_forecasting && uv lock
```

Verify: lock file should no longer contain `nvidia-*`, `cuda-bindings`, or `triton`.

### 2.3 Verify locally

```bash
cd apps/long_term_forecasting
uv sync --frozen --no-dev
uv run python -c "import torch; import catboost; import lightgbm; import xgboost; print('OK')"
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
#   "maintenance_tune":   calibration with hyperparameter tuning
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
- No `apt-get install` needed — geospatial packages (fiona, pyproj, shapely)
  ship manylinux wheels with bundled native libraries
- `lt_forecast_mode` is read by the Python scripts via `os.getenv()`, no CMD
  logic needed for it

---

## Phase 4: Docker smoke tests

### 4.1 Update `apps/run_docker_tests.sh`

Add to TARGETS array (after the `ml` entry, line 82):

```bash
"ltforecast|mabesa/sapphire-ltforecast|apps/long_term_forecasting/Dockerfile|import torch; import catboost; import lightgbm; import xgboost; import pandas; import numpy|"
```

Update `print_usage()` to list `ltforecast` as a target.

Update the valid targets error message in `main()` (line 440).

Add `--skip-lt` flag (mirrors `--skip-ml`) since this is also a large image.

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
- Verify imports: torch, catboost, lightgbm, xgboost, pandas, numpy

Add `build_long_term_forecasting` job (follows `build_machine_learning` pattern):
- `needs: [test_long_term_forecasting, build_base_image]`
- `timeout-minutes: 60`
- Free disk space step (same as ML job)
- Build base image, then build LT image

Add `build_long_term_forecasting` to `summarize_builds.needs` array.

Add LT image to the summary echo list.

---

## Files Modified

| File | Action |
|------|--------|
| `apps/machine_learning/pyproject.toml` | Add CPU-only torch index |
| `apps/machine_learning/uv.lock` | Regenerate (smaller, no CUDA) |
| `apps/long_term_forecasting/pyproject.toml` | Add CPU-only torch index |
| `apps/long_term_forecasting/uv.lock` | Regenerate (smaller, no CUDA) |
| `apps/long_term_forecasting/Dockerfile` | **New file** |
| `apps/run_docker_tests.sh` | Add ltforecast target + --skip-lt flag |
| `.github/workflows/build_test.yml` | Add test + build jobs for LT |

## Deferred (separate tasks)

- Luigi pipeline integration (`apps/pipeline/pipeline_docker.py`) — requires
  designing dependency graph for LT in the pipeline
- `deploy_main.yml` updates — done when merging to main
- Docker image size audit after CPU switch

## Verification

1. `uv sync --frozen --no-dev` succeeds for both modules
2. `import torch; torch.cuda.is_available()` returns `False` in both
3. `SAPPHIRE_TEST_ENV=True bash run_tests.sh machine_learning` passes
4. `bash apps/run_docker_tests.sh ml` passes (CPU-only ML image)
5. `bash apps/run_docker_tests.sh ltforecast` passes (new LT image)
6. Push branch, verify CI jobs pass in GitHub Actions

## Expected Savings

- ML image: ~3 GB smaller (CUDA libs removed)
- LT image: starts at ~1.0-1.5 GB instead of ~3.5 GB (CPU-only from the start)
- CI build times: significantly faster (fewer/smaller downloads)
