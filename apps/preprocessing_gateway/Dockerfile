# syntax=docker/dockerfile:1

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
CMD ["python", "apps/preprocessing_gateway/Quantile_Mapping_OP.py"]
