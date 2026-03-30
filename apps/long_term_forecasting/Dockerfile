# syntax=docker/dockerfile:1

# Python 3.12 + uv image for long_term_forecasting module
# CPU-only PyTorch configured via pyproject.toml [tool.uv.sources]

FROM mabesa/sapphire-pythonbaseimage:latest AS base

ENV SAPPHIRE_OPDEV_ENV=True
ENV IN_DOCKER=True

USER root
WORKDIR /app

COPY apps/iEasyHydroForecast /app/apps/iEasyHydroForecast
COPY apps/long_term_forecasting /app/apps/long_term_forecasting

WORKDIR /app/apps/long_term_forecasting
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# RUN_MODE selects entry point:
#   "forecast" (default): operational (run_forecast.py --all)
#   "maintenance":        calibration + hindcasting (calibrate_and_hindcast.py --all)
#   "maintenance_tune":   calibration with hyperparameter tuning (--all only,
#                         no way to tune a subset of models via Docker)
# lt_forecast_mode env var (e.g. "month_1") is read by the scripts directly.
CMD ["sh", "-c", \
    "if [ \"$RUN_MODE\" = \"maintenance\" ]; then \
        uv run calibrate_and_hindcast.py --all; \
    elif [ \"$RUN_MODE\" = \"maintenance_tune\" ]; then \
        uv run calibrate_and_hindcast.py --all --tune_hyperparameters; \
    else \
        uv run run_forecast.py --all; \
    fi"]
