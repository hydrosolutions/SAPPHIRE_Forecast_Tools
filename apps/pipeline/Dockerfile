FROM mabesa/sapphire-pythonbaseimage:latest AS base

# Set an environment variable that can be read by the application to know if it
# is running in a container.
#ENV IN_DOCKER_CONTAINER=true
ENV SAPPHRIE_OPDEV_ENV=True

# Setting the working directory to /app
WORKDIR /app

# All files in the current directory are copied into the container into
# directory app.
COPY apps/pipeline /app/apps/pipeline

# No additional requirements to install for this application.
