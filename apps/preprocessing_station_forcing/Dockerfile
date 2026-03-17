# syntax=docker/dockerfile:1

# Python 3.12 + uv image for preprocessing_station_forcing module
# Migration completed 2026-01-29 (Python 3.12 + uv)
# See: doc/plans/archive/uv_migration_plan_COMPLETED_2026-01-29.md

FROM mabesa/sapphire-pythonbaseimage:latest AS base

# Set environment variable for runtime detection
ENV SAPPHIRE_OPDEV_ENV=True

# Switch to root for installation and runtime
# This module writes to shared volumes, requiring root access
USER root

# Set working directory
WORKDIR /app

# Copy iEasyHydroForecast (local dependency referenced in pyproject.toml)
COPY apps/iEasyHydroForecast /app/apps/iEasyHydroForecast

# Copy preprocessing_station_forcing source code (includes pyproject.toml and uv.lock)
COPY apps/preprocessing_station_forcing /app/apps/preprocessing_station_forcing

# Install all dependencies using uv sync (includes iEasyHydroForecast from local path)
# This creates a virtual environment and installs from uv.lock for reproducibility
WORKDIR /app/apps/preprocessing_station_forcing
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Run the application using uv run (activates the virtual environment automatically)
CMD ["uv", "run", "preprocessing_station_forcing.py"]
