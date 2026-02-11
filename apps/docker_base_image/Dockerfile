# syntax=docker/dockerfile:1

# Python 3.12 + uv base image for SAPPHIRE Forecast Tools
# Migration completed 2026-01-29 (Python 3.12 + uv)
# See: doc/plans/archive/uv_migration_plan_COMPLETED_2026-01-29.md

ARG PYTHON_VERSION=3.12
FROM python:${PYTHON_VERSION}-slim-bookworm AS base

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1
# Keeps Python from buffering stdout and stderr.
ENV PYTHONUNBUFFERED=1
# Set an environment variable to indicate the container environment.
ENV SAPPHIRE_OPDEV_ENV=True
# uv optimization settings
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Install minimal system dependencies
# Reduced from original to improve Docker Scout health grade
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        git \
        libkrb5-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Upgrade pip to fix CVE vulnerability (pip 25.0.1 -> 25.3+)
RUN pip install --no-cache-dir --upgrade "pip>=25.3"

# Create the non-root user with specific UID/GID
# Check the UID/GID of the host user and group with `id -u` and `id -g`
# For Ubuntu servers this is usually 1000:1000
# Custom builds can override with --build-arg USER_ID=xxx GROUP_ID=xxx
ARG USER_ID=1000
ARG GROUP_ID=1000
RUN groupadd --gid $GROUP_ID appgroup && \
    useradd --uid $USER_ID --gid $GROUP_ID --create-home appuser

# Handle docker group for containers that need Docker socket access
# Adaptive approach - use existing group with GID 999 or create one
RUN DOCKER_GRP=$(getent group 999 | cut -d: -f1 || echo "") && \
    if [ -z "$DOCKER_GRP" ]; then \
        echo "Creating docker_ext group with GID 999" && \
        groupadd -g 999 docker_ext && \
        DOCKER_GRP="docker_ext"; \
    else \
        echo "Using existing group $DOCKER_GRP with GID 999"; \
    fi && \
    usermod -aG $DOCKER_GRP appuser && \
    echo "Added appuser to group $DOCKER_GRP"

# Set permissions for the application directory
WORKDIR /app
RUN chown -R appuser:appgroup /app

# Switch to the non-root user
USER appuser

# =============================================================================
# Base image complete. Child images should:
# 1. Copy their module source code (includes pyproject.toml and uv.lock)
# 2. Copy iEasyHydroForecast as a local dependency (referenced in pyproject.toml)
# 3. Run `uv sync --frozen --no-dev` to install all Python packages
# 4. Use `uv run <script.py>` to execute scripts
# =============================================================================
