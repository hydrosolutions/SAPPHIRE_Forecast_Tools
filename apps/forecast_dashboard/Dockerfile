# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/engine/reference/builder/

#FROM ubuntu:latest as build

ARG PYTHON_VERSION=3.11.9
FROM python:${PYTHON_VERSION}-slim-buster as base

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

# All needed files in the current directory are copied into the container into
# directory app.
#RUN mkdir -p /app

COPY apps/iEasyHydroForecast app/apps/iEasyHydroForecast
COPY apps/forecast_dashboard app/apps/forecast_dashboard

# Specify the working directory inside the container
WORKDIR /app

# When building the image, the path to the Dockerfile needs to be specified with
# the -f tag.

# Add Display to the container. This is needed for the dashboard to work.
ENV DISPLAY :0

# Update libraries of the operating system. Especially the ones which are flaged
# for vulnerabilities in dockerhub. The -y flag automatically anwwers yes to all
# prompts.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=python_requirements.txt,target=python_requirements.txt \
    apt-get update && \
    apt-get install -y gcc git libkrb5-dev && \
    apt-get install -y libtasn1-6 libpcre2-dev openssl libssl-dev && \
    apt-get install -y libncurses-dev binutils openssh-server libtiff5-dev && \
    apt-get install -y libpcre3-dev && \
    apt-get install -y gnutls-bin libc6 && \
    apt-get upgrade -y gcc libc6 && \
    apt-get clean && \
    # install python packages \
    pip install --upgrade pip setuptools wheel tornado urllib3 Pillow && \
    pip install --trusted-host pypi.python.org -r python_requirements.txt && \
    pip install git+https://github.com/hydrosolutions/ieasyreports.git@main && \
    pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk

# Expose the port that the application listens on.
EXPOSE 5006

# Test if file apps/forecast_dashboard/pentad_dashboard.py exists. Print the result.
# CMD test -f apps/forecast_dashboard/pentad_dashboard.py && echo "File exists" || echo "File does not exist"

# Run the application.
CMD ["panel", "serve", "apps/forecast_dashboard/pentad_dashboard.py", "--show", "--autoreload", "--port", "5006", "--allow-websocket-origin", "*"]

