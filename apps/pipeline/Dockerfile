# syntax=docker/dockerfile:1

# Python 3.12 + uv image for pipeline module
# Migration completed 2026-01-29 (Python 3.12 + uv)
# See: doc/plans/archive/uv_migration_plan_COMPLETED_2026-01-29.md

FROM mabesa/sapphire-pythonbaseimage:latest AS base

# Set environment variables
ENV SAPPHIRE_OPDEV_ENV=True
ENV PATH="/home/appuser/.local/bin:${PATH}"

# Switch to root for installation
USER root

# Set working directory
WORKDIR /app

# Copy iEasyHydroForecast (local dependency referenced in pyproject.toml)
COPY apps/iEasyHydroForecast /app/apps/iEasyHydroForecast

# Copy pipeline source code (includes pyproject.toml and uv.lock)
COPY apps/pipeline /app/apps/pipeline
COPY apps/pipeline/luigi.cfg /app/luigi.cfg

# Install all dependencies using uv sync (includes iEasyHydroForecast from local path)
# This creates a virtual environment and installs from uv.lock for reproducibility
RUN --mount=type=cache,target=/root/.cache/uv \
    cd /app/apps/pipeline && uv sync --frozen --no-dev

# Set PYTHONPATH to include /app for Luigi module resolution
ENV PYTHONPATH="${PYTHONPATH}:/app"

# Run as root (required for Docker socket access)
# The command to run when the container starts up
# Using uv run to ensure Luigi runs in the virtual environment
# Note: Must run from /app/apps/pipeline where the .venv is located
CMD ["sh", "-c", "cd /app/apps/pipeline && uv run luigi --module apps.pipeline.pipeline_docker PreprocessingRunoff --local-scheduler"]
