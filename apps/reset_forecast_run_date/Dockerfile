# syntax=docker/dockerfile:1

FROM mabesa/sapphire-pythonbaseimage:latest AS base

# Setting the working directory to /app
WORKDIR /app

# Copy the source code into the container.
# All files in the current directory are copied into the container into
# directory app.
COPY apps/reset_forecast_run_date /app/apps/reset_forecast_run_date

RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=apps/reset_forecast_run_date/requirements.txt,target=/app/apps/reset_forecast_run_date/requirements.txt \
    pip install --upgrade pip setuptools wheel tornado && \
    pip install --trusted-host pypi.python.org -r /app/apps/reset_forecast_run_date/requirements.txt

# Run the application.
CMD ["python", "apps/reset_forecast_run_date/rerun_forecast.py"]
