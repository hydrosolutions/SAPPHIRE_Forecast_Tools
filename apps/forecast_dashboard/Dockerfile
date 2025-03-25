# syntax=docker/dockerfile:1

FROM mabesa/sapphire-pythonbaseimage:latest AS base

# Copy the contents of the apps/forecast_dashboard directory into the container
COPY --chown=appuser:appgroup apps/forecast_dashboard /app/apps/forecast_dashboard

# Specify the working directory inside the container
WORKDIR /app/apps/forecast_dashboard

# Env
ENV SAPPHIRE_OPDEV_ENV=True
ENV PATH="/home/appuser/.local/bin:${PATH}"

# Add Display to the container. This is needed for the dashboard to work.
ENV DISPLAY=:0

# Set environment variable to force unbuffered output
ENV PYTHONUNBUFFERED=1

# Update libraries of the operating system. Especially the ones which are flaged
# for vulnerabilities in dockerhub. The -y flag automatically anwwers yes to all
# prompts.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=apps/forecast_dashboard/requirements.txt,target=/app/apps/forecast_dashboard/requirements.txt \
    pip install --trusted-host pypi.python.org -r /app/apps/forecast_dashboard/requirements.txt && \
    python -m pip install git+https://github.com/hydrosolutions/ieasyreports.git@main

# Expose the port that the application listens on.
# Port 5006 for the pentad dashboard
# Port 5007 for the decad dashboard
EXPOSE 5006
EXPOSE 5007

# Test if file apps/forecast_dashboard/pentad_dashboard.py exists. Print the result.
# CMD test -f apps/forecast_dashboard/pentad_dashboard.py && echo "File exists" || echo "File does not exist"

# Run the application.
# On local machine
#CMD ["panel", "serve", "pentad_dashboard.py", "--show", "--autoreload", "--port", "5006"]
# On server
#CMD panel serve --allow-websocket-origin=$ieasyhydroforecast_url pentad_dashboard.py --show --autoreload --num-threads 4 --address 0.0.0.0

# CMD to list all files in app/apps/forecast_dashboard and print the results to the console.
#CMD ["/bin/sh", "-c", "echo 'here!' && ls /app/apps/forecast_dashboard && echo ' ' && pip list"]
