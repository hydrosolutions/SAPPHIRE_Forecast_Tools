# syntax=docker/dockerfile:1

# Python 3.12 + uv image for preprocessing_gateway module
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

# Copy preprocessing_gateway source code (includes pyproject.toml and uv.lock)
COPY apps/preprocessing_gateway /app/apps/preprocessing_gateway

# Install all dependencies using uv sync (includes iEasyHydroForecast from local path)
# This creates a virtual environment and installs from uv.lock for reproducibility
WORKDIR /app/apps/preprocessing_gateway
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Run the application using uv run (activates the virtual environment automatically)
# Runs three scripts sequentially: quantile mapping, ERA5 extension, and snow data processing
#
# Sync modes (controlled via SAPPHIRE_SYNC_MODE env var):
#   operational (default) - writes yesterday's + today's data
#   maintenance           - writes recent history (30 days snow, 365 days reanalysis)
#   initial               - writes all data
#
# Examples:
#   docker run preprocessing-gateway                                      # operational (default)
#   docker run -e SAPPHIRE_SYNC_MODE=maintenance preprocessing-gateway    # maintenance
#   docker run preprocessing-gateway uv run backfill_new_stations.py      # backfill
CMD ["sh", "-c", "uv run Quantile_Mapping_OP.py && uv run extend_era5_reanalysis.py && uv run snow_data_operational.py"]
