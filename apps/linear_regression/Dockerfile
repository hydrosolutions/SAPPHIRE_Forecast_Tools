# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/engine/reference/builder/

#FROM ubuntu:latest as build

FROM mabesa/sapphire-pythonbaseimage:latest AS base

# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into
# into this layer.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=apps/linear_regression/requirements.txt,target=/app/apps/linear_regression/requirements.txt \
    pip install --trusted-host pypi.python.org -r apps/linear_regression/requirements.txt

    # Set an environment variable that can be read by the application to know if it
# is running in a container.
ENV SAPPHRIE_OPDEV_ENV=true

# Setting the working directory to /app
WORKDIR /app

# All files in the current directory are copied into the container into
# directory app.
COPY apps/linear_regression /app/apps/linear_regression

# Run the application.
CMD ["sh", "-c", "PYTHONPATH=/app/apps/iEasyHydroForecast python apps/linear_regression/linear_regression.py"]
