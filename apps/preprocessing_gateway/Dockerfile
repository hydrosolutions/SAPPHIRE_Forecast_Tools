# syntax=docker/dockerfile:1

# Creates a docker image for running the preprocessing gateway module.
#
# We first download the operational data and perform quantile mapping on it and
# then extend the ERA5 reanalysis data so we have reanalysis data over the entire
# time period available.

FROM mabesa/sapphire-pythonbaseimage:latest AS base

# Setting the working directory to /app
WORKDIR /app

# Copy the source code into the container.
# All files in the current directory are copied into the container into
# directory app.
COPY apps/preprocessing_gateway /app/apps/preprocessing_gateway

RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=apps/preprocessing_gateway/requirements.txt,target=/app/apps/preprocessing_gateway/requirements.txt \
    pip install --upgrade pip setuptools wheel tornado && \
    pip install --trusted-host pypi.python.org -r /app/apps/preprocessing_gateway/requirements.txt && \
    pip install git+https://github.com/hydrosolutions/sapphire-dg-client.git

# Run the application.
CMD ["sh", "-c", "python apps/preprocessing_gateway/Quantile_Mapping_OP.py && python apps/preprocessing_gateway/extend_era5_reanalysis.py"]
