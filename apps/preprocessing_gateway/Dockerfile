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
# Runs three scripts: quantile mapping, ERA5 extension, and snow data processing.
#
# PREPG-024: snow has no data dependency on the meteo branch (quantile
# mapping + ERA5 extension) -- it reads a different Data Gateway endpoint
# and writes a different API sink -- so it must run regardless of whether
# the meteo branch failed. It still must NOT run concurrently with the
# meteo branch: Quantile_Mapping_OP.py deletes every file in the shared
# OUTPUT_PATH_DG, and snow downloads into that same directory. So the
# sequence is: run the meteo branch (QM && extend, unchanged -- extend
# reads control-member CSVs only QM writes) -> capture its status -> run
# snow sequentially afterwards regardless -> capture its status -> exit
# non-zero if either branch failed. See
# doc/plans/issues/mid_prio_gi_draft_prepg_qm_failure_suppresses_snow.md.
# This CMD and run_locally.sh's run_preprocessing_gateway() must express
# the same logic -- keep them in sync.
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
CMD ["sh", "-c", "uv run Quantile_Mapping_OP.py && uv run extend_era5_reanalysis.py; meteo_rc=$?; uv run snow_data_operational.py; snow_rc=$?; if [ $meteo_rc -ne 0 ] || [ $snow_rc -ne 0 ]; then echo \"preprocessing_gateway: FAILED stage(s) -- meteo_rc=$meteo_rc snow_rc=$snow_rc\" >&2; exit 1; fi; exit 0"]
