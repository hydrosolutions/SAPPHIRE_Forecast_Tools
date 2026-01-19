# pythonbaseimage has appuser as default non-root user
FROM mabesa/sapphire-pythonbaseimage:latest AS base

# Set an environment variable that can be read by the application to know if it
# is running in a container.
#ENV IN_DOCKER_CONTAINER=true
ENV SAPPHRIE_OPDEV_ENV=True
ENV PATH="/home/appuser/.local/bin:${PATH}"

# Setting the working directory to /app
WORKDIR /app

# All files in the current directory are copied into the container into
# directory app.
COPY --chown=appuser:appgroup apps/pipeline /app/apps/pipeline
COPY --chown=appuser:appgroup apps/pipeline/luigi.cfg /app/luigi.cfg

RUN --mount=type=bind,source=apps/pipeline/requirements.txt,target=/app/apps/pipeline/requirements.txt \
    pip install --no-cache-dir -r /app/apps/pipeline/requirements.txt && \
    which luigi && \
    echo $PATH

# Set PYTHONPATH to include /app
ENV PYTHONPATH="${PYTHONPATH}:/app"

# The command to run when the container starts up
CMD ["luigi", "--module", "apps.pipeline.pipeline_docker", "PreprocessingRunoff", "--local-scheduler"]

