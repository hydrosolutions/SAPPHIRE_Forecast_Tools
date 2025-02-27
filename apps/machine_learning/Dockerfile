# syntax=docker/dockerfile:1

FROM mabesa/sapphire-pythonbaseimage:latest AS base

# Set an environment variable that can be read by the application to know if it
# is running in a container.
ENV SAPPHRIE_OPDEV_ENV=true
ENV IN_DOCKER=true

# Setting the working directory to /app
WORKDIR /app

# All files in the current directory are copied into the container into
# directory app.
COPY apps/machine_learning /app/apps/machine_learning

# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into
# into this layer.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=apps/machine_learning/requirements.txt,target=/app/apps/machine_learning/requirements.txt \
    pip install --trusted-host pypi.python.org -r apps/machine_learning/requirements.txt && \
    pip install git+https://github.com/hydrosolutions/sapphire-dg-client.git


# Run the application.
# This cmd is moved to the pipeline definition
#CMD ["sh", "-c", "PYTHONPATH=/app/apps/iEasyHydroForecast python apps/machine_learning/make_forecast.py && python apps/machine_learning/fill_ml_gaps.py  && python apps/machine_learning/add_new_station.py"]
# New CMD to allow for different run modes
# RUN_MODE can be forecast, maintenance or not given (which defaults to running all maintenance and forecast tasks)
CMD ["sh", "-c", "echo \"Environment Variables:\" && echo \"RUN_MODE=$RUN_MODE\" && echo \"SAPPHIRE_MODEL_TO_USE=$SAPPHIRE_MODEL_TO_USE\" && echo \"SAPPHIRE_PREDICTION_MODE=$SAPPHIRE_PREDICTION_MODE\" && export PYTHONPATH=/app/apps/iEasyHydroForecast && if [ \"$RUN_MODE\" = \"forecast\" ]; then python apps/machine_learning/make_forecast.py; elif [ \"$RUN_MODE\" = \"maintenance\" ]; then python apps/machine_learning/recalculate_nan_forecasts.py && python apps/machine_learning/fill_ml_gaps.py && python apps/machine_learning/add_new_station.py; else python apps/machine_learning/recalculate_nan_forecasts.py && python apps/machine_learning/make_forecast.py && python apps/machine_learning/fill_ml_gaps.py && python apps/machine_learning/add_new_station.py; fi"]