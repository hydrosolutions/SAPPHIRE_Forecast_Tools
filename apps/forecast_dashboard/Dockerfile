# syntax=docker/dockerfile:1

# Python 3.12 + uv image for forecast_dashboard module
# This image runs in parallel with the :latest (Python 3.11 + pip) version during migration
# See: implementation_planning/uv_migration_plan.md

FROM mabesa/sapphire-pythonbaseimage:latest AS base

# Run as root to allow running of containers from within dashboard
USER root

# Set environment variables
ENV SAPPHIRE_OPDEV_ENV=True
ENV PATH="/root/.local/bin:${PATH}"
ENV DISPLAY=:0
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy iEasyHydroForecast (local dependency referenced in pyproject.toml)
COPY apps/iEasyHydroForecast /app/apps/iEasyHydroForecast

# Copy forecast_dashboard source code (includes pyproject.toml and uv.lock)
COPY apps/forecast_dashboard /app/apps/forecast_dashboard

# Install all dependencies using uv sync (includes iEasyHydroForecast from local path)
# This creates a virtual environment and installs from uv.lock for reproducibility
WORKDIR /app/apps/forecast_dashboard
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Expose the ports that the application listens on.
# Port 5006 for the pentad dashboard
# Port 5007 for the decad dashboard
EXPOSE 5006
EXPOSE 5007

# Run the application using uv run (activates the virtual environment automatically)
# The CMD is typically overridden by docker-compose or deployment scripts
# Default: serve dashboard on port 5006
CMD ["sh", "-c", "uv run panel serve forecast_dashboard.py --address 0.0.0.0 --port 5006 --allow-websocket-origin=${ieasyhydroforecast_url:-localhost:5006}"]
