#!/bin/bash

# Script to set up Luigi daemon on Ubuntu servers
# Usage: sudo bash setup_luigi_daemon.sh [BASE_SAPCA_PATH] [BASE_SAPCA_ENV_FILE_PATH]
# 
# Parameters:
#   BASE_SAPCA_PATH   - Path to SAPPHIRE_forecast_tools (default: /data/SAPPHIRE_forecast_tools)
#   BASE_SAPCA_ENV_FILE_PATH   - Path to environment file (default: /data/SAPPHIRE_forecast_tools/config/.env)

# Print usage information
usage() {
  echo "Usage: sudo bash $(basename $0) [BASE_SAPCA_PATH] [BASE_SAPCA_ENV_FILE_PATH]"
  echo ""
  echo "Parameters:"
  echo "  BASE_SAPCA_PATH           - Path to SAPPHIRE_forecast_tools"
  echo "                              Default: /data/SAPPHIRE_forecast_tools"
  echo "  BASE_SAPCA_ENV_FILE_PATH  - Path to environment file"
  echo "                              Default: /data/SAPPHIRE_forecast_tools/config/.env"
  echo ""
  echo "Examples:"
  echo "  sudo bash $(basename $0)"
  echo "  sudo bash $(basename $0) /custom/path/to/SAPPHIRE"
  echo "  sudo bash $(basename $0) /custom/path/to/SAPPHIRE /custom/path/to/.env"
  echo ""
  echo "Note: Both paths must exist for the script to run successfully."
}

# Default values
BASE_SAPCA_PATH=${1:-"/data/SAPPHIRE_forecast_tools"}
BASE_SAPCA_ENV_FILE_PATH=${2:-"/data/SAPPHIRE_forecast_tools/config/.env"}

# Validate if paths exist 
if [ ! -d "$BASE_SAPCA_PATH" ]; then
  echo "Error: BASE_SAPCA_PATH path '$BASE_SAPCA_PATH' does not exist."
  usage
  exit 1 
fi

if [ ! -f "$BASE_SAPCA_ENV_FILE_PATH" ]; then
  echo "Error: BASE_SAPCA_ENV_FILE_PATH file '$BASE_SAPCA_ENV_FILE_PATH' does not exist. "
  usage
  exit 1
fi

echo "Setting up Luigi daemon..."
echo "Using SAPPHIRE path: $BASE_SAPCA_PATH"
if [ ! -z "$BASE_SAPCA_ENV_FILE_PATH" ]; then
    echo "Using environment file: $BASE_SAPCA_ENV_FILE_PATH"
fi

# Create luigi user if it doesn't exist
if ! id -u luigi >/dev/null 2>&1; then
    echo "Creating luigi user..."
    useradd -r -s /bin/false luigi
fi

# Create necessary directories
echo "Creating directories..."
mkdir -p /var/log/luigi
mkdir -p /var/lib/luigi
mkdir -p /etc/luigi

# Set ownership
chown luigi:luigi /var/log/luigi
chown luigi:luigi /var/lib/luigi

# Create Luigi configuration file
echo "Creating Luigi configuration..."
cat > /etc/luigi/luigi.cfg << 'EOF'
[core]
default-scheduler-host = localhost
default-scheduler-port = 8082
default-scheduler-url = http://localhost:8082
logging_conf_file = /etc/luigi/logging.cfg

[scheduler]
record_task_history = True
state_path = /var/lib/luigi/state.pickle
remove_delay = 86400
worker_disconnect_delay = 60

[task_history]
db_connection = sqlite:////var/lib/luigi/luigi-task-history.db

[retcode]
# Return codes for different task statuses
already_running = 10
missing_data = 20
not_run = 25
task_failed = 30
scheduling_error = 35
unhandled_exception = 40
EOF

# Create logging configuration
cat > /etc/luigi/logging.cfg << 'EOF'
[loggers]
keys = root

[handlers]
keys = console, file

[formatters]
keys = detail

[logger_root]
level = INFO
handlers = console, file

[handler_console]
class = StreamHandler
level = INFO
formatter = detail
args = (sys.stdout,)

[handler_file]
class = handlers.RotatingFileHandler
level = INFO
formatter = detail
args = ('/var/log/luigi/luigi.log', 'a', 10485760, 5)

[formatter_detail]
format = %(asctime)s %(name)-15s %(levelname)-8s %(message)s
EOF

# Create systemd service file
echo "Creating systemd service..."
cat > /etc/systemd/system/luigid.service << EOF
[Unit]
Description=Luigi Scheduler Daemon
Documentation=https://luigi.readthedocs.io
After=network.target

[Service]
Type=simple
User=luigi
Group=luigi
Environment="PYTHONPATH=${BASE_SAPCA_PATH}:/app"
EnvironmentFile=${BASE_SAPCA_ENV_FILE_PATH}
ExecStart=/usr/local/bin/luigid --port 8082 --pidfile /var/run/luigid.pid --config /etc/luigi/luigi.cfg
ExecStop=/bin/kill -TERM \$MAINPID
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Create log rotation configuration
echo "Setting up log rotation..."
cat > /etc/logrotate.d/luigi << 'EOF'
/var/log/luigi/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    create 0644 luigi luigi
    sharedscripts
    postrotate
        systemctl reload luigid > /dev/null 2>&1 || true
    endscript
}
EOF

# Enable and start the service
echo "Enabling and starting Luigi daemon..."
systemctl daemon-reload
systemctl enable luigid
systemctl start luigid

# Check status
echo "Checking Luigi daemon status..."
systemctl status luigid

echo "Luigi daemon setup complete!"
echo "Access Luigi web interface at: http://localhost:8082"
echo ""
echo "To check logs: journalctl -u luigid -f"
echo "To restart: sudo systemctl restart luigid"
echo "To stop: sudo systemctl stop luigid"