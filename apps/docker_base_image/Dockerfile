# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-slim-bookworm AS base

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1
# Keeps Python from buffering stdout and stderr.
ENV PYTHONUNBUFFERED=1
# Set an environment variable to indicate the container environment.
ENV SAPPHIRE_OPDEV_ENV=True
ENV PATH="/home/appuser/.local/bin:${PATH}"

# Update the packages to the latest OS version.
RUN apt-get clean && \
    apt-get install debian-archive-keyring && \
    apt-get update && \
    apt-get install -y gcc git libkrb5-dev && \
    apt-get install -y libtasn1-6 libpcre2-dev openssl libssl-dev && \
    apt-get install -y libncurses-dev binutils openssh-server libtiff5-dev && \
    apt-get install -y libpcre3-dev && \
    apt-get install -y gnutls-bin libc6 && \
    apt-get upgrade -y gcc libc6 && \
    apt-get clean

# Install sudo should child images need it.
RUN apt-get install -y sudo && \
    echo "appuser ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# Create the non-root user with specific UID/GID
# Check the UID/GID of the host user and group with `id -u` and `id -g`
# For Ubuntu servers this is usually 1000:1000
# This aligns container permissions with host permissions by default.
# Custom builds for other users can be created by passing the USER_ID and
# GROUP_ID build arguments
# docker build --build-arg USER_ID=10001 --build-arg GROUP_ID=10001 ...
ARG USER_ID=1000
ARG GROUP_ID=1000
RUN groupadd --gid $GROUP_ID appgroup && \
    useradd --uid $USER_ID --gid $GROUP_ID --create-home appuser
# Add appuser to docker group
# More robust handling of docker group
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

# Copy the source code into the container.
COPY --chown=appuser:appgroup apps/iEasyHydroForecast/ /app/apps/iEasyHydroForecast

# Install dependencies as the non-root user
RUN --mount=type=cache,target=/home/appuser/.cache/pip \
    --mount=type=bind,source=apps/docker_base_image/requirements.txt,target=/app/apps/docker_base_image/requirements.txt \
    pip install --upgrade pip && \
    pip uninstall -y setuptools && \
    pip install --no-cache-dir "setuptools>=70.0.0" wheel tornado && \
    pip install --trusted-host pypi.python.org -r /app/apps/docker_base_image/requirements.txt && \
    pip install git+https://github.com/hydrosolutions/ieasyhydro-python-sdk && \
    sudo pip install --no-cache-dir /app/apps/iEasyHydroForecast
