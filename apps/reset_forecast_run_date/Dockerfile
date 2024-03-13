# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/engine/reference/builder/

#FROM ubuntu:latest as build

ARG PYTHON_VERSION=3.10.7
FROM python:${PYTHON_VERSION}-slim-buster as base

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

# Set an environment variable that can be read by the application to know if it
# is running in a container.
ENV IN_DOCKER_CONTAINER=true

# Setting the working directory to /app
WORKDIR /app

# Copy the source code into the container.
# All files in the current directory are copied into the container into
# directory app.
COPY apps/reset_forecast_run_date /app/apps/reset_forecast_run_date

# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into
# into this layer.
# Update libraries of the operating system. Especially the ones which are flaged
# for vulnerabilities in dockerhub. The -y flag automatically anwwers yes to all
# prompts.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=apps/reset_forecast_run_date/requirements.txt,target=/app/apps/reset_forecast_run_date/requirements.txt \
    # Update the packages to the latest OS version
    #apt-get update && \
    #apt-get install -y gcc git libkrb5-dev && \
    #apt-get install -y libtasn1-6 libpcre2-dev openssl libssl-dev && \
    #apt-get install -y libncurses-dev binutils openssh-server libtiff5-dev && \
    #apt-get install -y libpcre3-dev && \
    #apt-get install -y gnutls-bin libc6 && \
    #apt-get upgrade -y gcc libc6 && \
    #apt-get clean && \
    # update python packages \
    pip install --upgrade pip setuptools wheel tornado && \
    pip install --trusted-host pypi.python.org -r /app/apps/reset_forecast_run_date/requirements.txt

# Run the application.
CMD ["python", "apps/reset_forecast_run_date/rerun_forecast.py"]
