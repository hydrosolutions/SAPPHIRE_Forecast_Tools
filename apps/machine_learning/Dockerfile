# syntax=docker/dockerfile:1

# Python 3.12 + uv image for machine_learning module
# This image runs in parallel with the :latest (Python 3.11 + pip) version during migration
# See: implementation_planning/uv_migration_plan.md
#
# NOTE: This image has a larger footprint due to torch/darts dependencies.
# Expected DockerHub Scout health grade: C/D (acceptable for ML workloads)

FROM mabesa/sapphire-pythonbaseimage:latest AS base

# Set environment variables for runtime detection
ENV SAPPHIRE_OPDEV_ENV=True
ENV IN_DOCKER=True

# Switch to root for installation and runtime
# This module writes to shared volumes, requiring root access
USER root

# Set working directory
WORKDIR /app

# Copy iEasyHydroForecast (local dependency referenced in pyproject.toml)
COPY apps/iEasyHydroForecast /app/apps/iEasyHydroForecast

# Copy machine_learning source code (includes pyproject.toml and uv.lock)
COPY apps/machine_learning /app/apps/machine_learning

# Install all dependencies using uv sync (includes iEasyHydroForecast from local path)
# This creates a virtual environment and installs from uv.lock for reproducibility
# Note: torch and darts are large packages, this step may take a while
WORKDIR /app/apps/machine_learning
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Run the application using uv run (activates the virtual environment automatically)
# RUN_MODE can be "forecast", "maintenance", or empty (runs all)
# SAPPHIRE_MODEL_TO_USE can be "TFT", "TIDE", or "TSMIXER"
# SAPPHIRE_PREDICTION_MODE can be "PENTAD" or "DECAD"
CMD ["sh", "-c", "echo \"Environment Variables:\" && echo \"RUN_MODE=$RUN_MODE\" && echo \"SAPPHIRE_MODEL_TO_USE=$SAPPHIRE_MODEL_TO_USE\" && echo \"SAPPHIRE_PREDICTION_MODE=$SAPPHIRE_PREDICTION_MODE\" && if [ \"$RUN_MODE\" = \"forecast\" ]; then uv run make_forecast.py; elif [ \"$RUN_MODE\" = \"maintenance\" ]; then uv run recalculate_nan_forecasts.py && uv run fill_ml_gaps.py && uv run add_new_station.py; else uv run recalculate_nan_forecasts.py && uv run make_forecast.py && uv run fill_ml_gaps.py && uv run add_new_station.py; fi"]
