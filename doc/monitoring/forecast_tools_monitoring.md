# Monitoring the SAPPHIRE Forecast Tools
To monitor the the SAPPHIRE Forecast Tools, the following components are set up: 

- Monitoring of the pipeline tasks  
- Healthcheck for the Docker containers running the forecast dashboards
- Watchtower notifications for Docker container updates
- Systemd service for monitoring the Docker containers 
- Systemd service for monitoring the dashboard logs

This document provides a guide on how to set up the monitoring system for the forecast dashboards. We also provide troubleshooting tips for common issues.

Use the following table of contents to navigate through the document:
<!-- toc -->
- [Monitoring the SAPPHIRE Forecast Tools](#monitoring-the-sapphire-forecast-tools)
  - [Pipeline Tasks Monitoring](#pipeline-tasks-monitoring)
  - [Watchtower Notifications](#watchtower-notifications)
  - [Docker Healthcheck](#docker-healthcheck)
  - [Systemd Service for Docker Container Monitoring](#systemd-service-for-docker-container-monitoring)
    - [🔧 Installation Steps](#-installation-steps)
    - [📧 Email Configuration](#-email-configuration)
    - [📜 Systemd Service Configuration](#-systemd-service-configuration)
    - [📋 Enable and Start the Service](#-enable-and-start-the-service)
    - [Check status and logs](#check-status-and-logs)
    - [Testing the Setup](#testing-the-setup)
  - [Systemd Service for Dashboard Log Monitoring](#systemd-service-for-dashboard-log-monitoring)
    - [🔧 Installation Steps](#-installation-steps-1)
    - [📋 Configuration](#-configuration)
    - [📧 Email Configuration](#-email-configuration-1)
    - [📜 Systemd Service Configuration](#-systemd-service-configuration-1)
    - [📋 Enable and Start the Service](#-enable-and-start-the-service-1)
    - [Check Status and Logs](#check-status-and-logs-1)
    - [Testing the Setup](#testing-the-setup-1)
  - [Troubleshooting](#troubleshooting)
    - [Email Notifications Not Received](#email-notifications-not-received)
    - [Docker Monitor Not Detecting Events](#docker-monitor-not-detecting-events)
    - [Log Watcher Not Capturing Errors](#log-watcher-not-capturing-errors)
    - [Systemd Service Issues](#systemd-service-issues)
    - [Log Directory Issues](#log-directory-issues)
<!-- tocstop -->

## Pipeline Tasks Monitoring
The monitoring of the pipeline tasks is set up in the module `pipeline`. 

In your .env file, set the following environment variables for email notifications:

```bash
SAPPHIRE_PIPELINE_SMTP_SERVER=<your_smtp_server>
SAPPHIRE_PIPELINE_SMTP_PORT=<your_smtp_port>
SAPPHIRE_PIPELINE_SMTP_USERNAME=<your_smtp_username>
SAPPHIRE_PIPELINE_SMTP_PASSWORD=<your_smtp_password>
SAPPHIRE_PIPELINE_SENDER_EMAIL=<your_sender_email>
SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS=<your_recipient_emails_comma_separated>
```

## Watchtower Notifications
Watchtower is a tool that automatically updates running Docker containers. It is configured to send notifications when a container is updated (see file `bin/docker-compose-dashboards.yml`). No need to set up any additional environment variables for this feature.

## Docker Healthcheck 
The Docker healthcheck is set up in the `docker-compose-dashboards.yml` file. It checks if the dashboard is running and sends a notification if it is not. No need to set up any additional environment variables for this feature.

## Systemd Service for Docker Container Monitoring
The `bin/docker_monitor.sh` script is designed to monitor the health of Docker containers running the SAPPHIRE Forecast Tools dashboards. It checks for unexpected crashes and healthcheck failures, and sends email alerts with recent logs.

### 🔧 Installation Steps
After cloning the repository (see doployment guide), make sure the bash script is executable:

```bash
chmod +x /path/to/SAPPHIRE_FORECAST_TOOLS/bin/docker_monitor.sh
```

### 📧 Email Configuration
In your .env file, set the following environment variables for email notifications (if not already done so for the pipeline tasks):

```bash
SAPPHIRE_PIPELINE_SMTP_SERVER=<your_smtp_server>
SAPPHIRE_PIPELINE_SMTP_PORT=<your_smtp_port>
SAPPHIRE_PIPELINE_SMTP_USERNAME=<your_smtp_username>
SAPPHIRE_PIPELINE_SMTP_PASSWORD=<your_smtp_password>
SAPPHIRE_PIPELINE_SENDER_EMAIL=<your_sender_email>
SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS=<your_recipient_emails_comma_separated>
```

### 📜 Systemd Service Configuration
Create a systemd service file for the docker monitor. This service will run the `docker_monitor.sh` script to check the health of the Docker containers.
Create a file named `docker_monitor.service` in `/etc/systemd/system/` with the following content:

```ini
# Example systemd service file for the docker_monitor service
# systemd unit file: /etc/systemd/system/docker-monitor.service
[Unit]
Description=Docker Container Health and Crash Monitor
After=docker.service
Requires=docker.service

[Service]
Environment="DOCKER_MONITOR_ENV_PATH=/path/to/.env"
ExecStart=/path/to/SAPPHIRE_Forecast_Tools/bin/docker_monitor.sh
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

Make susre to replace `/path/to/your/.env` with the actual path to your `.env` file and `/path/to/your/docker_monitor.sh` with the path to the `docker_monitor.sh` script.

### 📋 Enable and Start the Service

```bash
sudo systemctl daemon-reexec
sudo systemctl daemon-reload
sudo systemctl enable docker-monitor.service
sudo systemctl enable dashboard_log_watcher.service
```

### Check status and logs
```bash
sudo systemctl status docker-monitor.service
journalctl -u docker-monitor.service -f 
```

### Testing the Setup
You can manually test the setup by stopping the Docker container running the dashboard or by simulating a healthcheck failure. This should trigger an email alert with the recent logs.

To simulate a container crash, you can run the following command:

```bash
docker stop <container_name>
```
To simulate a healthcheck failure, you can modify the healthcheck command in your Dockerfile or docker-compose file to return a non-zero exit code.

## Systemd Service for Dashboard Log Monitoring

The `bin/docker_log_watcher.sh` script monitors the logs of the SAPPHIRE dashboard containers for error patterns such as HTTP 404 errors, general errors, and exceptions. When such patterns are detected, the script sends email alerts with the relevant log lines.

### 🔧 Installation Steps

After cloning the repository, make the bash script executable:

```bash
chmod +x /path/to/SAPPHIRE_FORECAST_TOOLS/bin/docker_log_watcher.sh
```

### 📋 Configuration

The script monitors the following containers by default:
- `sapphire-frontend-forecast-pentad`
- `sapphire-frontend-forecast-decad`

It searches for the following error patterns:
- `404` (HTTP Not Found errors)
- `ERROR` (General error messages)
- `Exception` (Exception traces)

To modify these settings, edit the script variables at the top:
```bash
containers=("sapphire-frontend-forecast-pentad" "sapphire-frontend-forecast-decad")
pattern="404|ERROR|Exception"
MIN_ALERT_INTERVAL=300  # Minimum seconds between alerts (5 minutes)
```

### 📧 Email Configuration

The script uses the same email configuration as the Docker monitor. No additional configuration is required if you've already set up the environment variables for the Docker monitor.

### 📜 Systemd Service Configuration

Create a systemd service file for the log watcher:

```ini
# systemd unit file: /etc/systemd/system/dashboard-log-watcher.service
[Unit]
Description=Dashboard Log Error Watcher
After=docker.service
Requires=docker.service

[Service]
Environment="DOCKER_MONITOR_ENV_PATH=/path/to/.env"
ExecStart=/path/to/SAPPHIRE_Forecast_Tools/bin/docker_log_watcher.sh
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```

Replace `/path/to/.env` with the actual path to your `.env` file, and `/path/to/SAPPHIRE_Forecast_Tools/bin/docker_log_watcher.sh` with the path to the script.

### 📋 Enable and Start the Service

```bash
sudo systemctl daemon-reload
sudo systemctl enable dashboard-log-watcher.service
sudo systemctl start dashboard-log-watcher.service
```

### Check Status and Logs

```bash
sudo systemctl status dashboard-log-watcher.service
journalctl -u dashboard-log-watcher.service -f
```

### Testing the Setup

You can test the log watcher by deliberately triggering an error in one of the monitored containers:

```bash
docker exec sapphire-frontend-forecast-pentad bash -c 'echo "ERROR: This is a test error" >&2'
```

If everything is set up correctly, you should receive an email alert containing this test error message.

## Troubleshooting

### Email Notifications Not Received

1. **Check SMTP Configuration**
   ```bash
   # View the environment variables loaded by the script
   sudo systemctl cat docker-monitor.service | grep Environment
   ```
   Verify the path is correct and the file exists with proper permissions.

2. **Test Email Connectivity**
   ```bash
   # Install mailutils if needed
   sudo apt install mailutils
   
   # Test connection to SMTP server
   echo "Test" | mail -s "Test Email" -S smtp=$SAPPHIRE_PIPELINE_SMTP_SERVER \
     -S smtp-use-starttls -S smtp-auth=login \
     -S smtp-auth-user=$SAPPHIRE_PIPELINE_SMTP_USERNAME \
     -S smtp-auth-password=$SAPPHIRE_PIPELINE_SMTP_PASSWORD \
     $SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS
   ```

3. **Check Script Logs**
   ```bash
   # View recent log entries for email sending issues
   journalctl -u docker-monitor.service | grep -i "email\|smtp\|send_alert"
   journalctl -u dashboard-log-watcher.service | grep -i "email\|smtp\|send_alert"
   ```

### Docker Monitor Not Detecting Events

1. **Check Docker Events Stream**
   ```bash
   # Test if Docker events are being generated
   docker events --filter event=health_status --format '{{.Status}} {{.Actor.Attributes.name}}'
   
   # In another terminal, trigger a health check
   docker inspect --format "{{.State.Health.Status}}" sapphire-frontend-forecast-pentad
   ```

2. **Verify Docker Socket Access**
   ```bash
   # Ensure the user running the service has access to Docker
   sudo usermod -aG docker $USER  # Replace $USER with service user
   sudo systemctl restart docker-monitor.service
   ```

3. **Check Named Pipe**
   ```bash
   # Verify the named pipe exists and has correct permissions
   ls -la /var/log/docker_monitor/event_pipe
   ```

### Log Watcher Not Capturing Errors

1. **Test Pattern Matching**
   ```bash
   # Test if your regex pattern works as expected
   docker logs sapphire-frontend-forecast-pentad 2>&1 | grep -Ei "404|ERROR|Exception"
   ```

2. **Verify Container Names**
   ```bash
   # Check if the container names match exactly
   docker ps --format "{{.Names}}"
   ```

3. **Inspect Rate Limiting**
   ```bash
   # Check if rate limiting is preventing alerts
   journalctl -u dashboard-log-watcher.service | grep "Rate limiting"
   ```

### Systemd Service Issues

1. **Check Service Status**
   ```bash
   systemctl status docker-monitor.service
   systemctl status dashboard-log-watcher.service
   ```

2. **Environment File Issues**
   ```bash
   # Verify the .env file exists and is readable
   sudo -u <service-user> test -r /path/to/.env && echo "File exists and is readable"
   
   # Check for proper formatting in .env file
   grep -v "^#" /path/to/.env | grep -v "^$"
   ```

3. **Script Permission Issues**
   ```bash
   # Ensure scripts are executable
   sudo chmod +x /path/to/SAPPHIRE_Forecast_Tools/bin/docker_monitor.sh
   sudo chmod +x /path/to/SAPPHIRE_Forecast_Tools/bin/docker_log_watcher.sh
   
   # Check script ownership
   ls -la /path/to/SAPPHIRE_Forecast_Tools/bin/docker_*.sh
   ```

4. **Service Definition Problems**
   ```bash
   # Validate systemd service files
   systemd-analyze verify docker-monitor.service
   systemd-analyze verify dashboard-log-watcher.service
   ```

### Log Directory Issues

1. **Check Log Directory Permissions**
   ```bash
   # Verify log directory exists and has proper permissions
   sudo ls -la /var/log/docker_monitor/
   
   # Create if it doesn't exist
   sudo mkdir -p /var/log/docker_monitor/
   sudo chown <service-user>:<service-group> /var/log/docker_monitor/
   ```

2. **Monitor Disk Space**
   ```bash
   # Check available disk space
   df -h /var/log
   
   # List largest log files
   find /var/log/docker_monitor -type f -name "*.log" -exec du -h {} \; | sort -rh | head -n 10
   ```

If you encounter an issue not covered in this section, please check the systemd journal logs for more details:

```bash
journalctl -u docker-monitor.service -n 100
journalctl -u dashboard-log-watcher.service -n 100
```


