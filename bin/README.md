# System Requirements
1. Ubuntu Server: The script is designed for Ubuntu systems that use systemd  
2. Root/sudo access: Required for creating system users, directories, and services  
3. systemd: Used for service management  

# Software Prerequisites
1. Python 3.8 or higher: Required for running Luigi  
2. Luigi package: Must be installed before running the setup script

Note: The script assumes Luigi is already installed at /usr/local/bin/luigid  

3. SAPPHIRE_forecast_tools: Must be properly installed at the specified path  (recommended: /data/SAPPHIRE_forecast_tools)  
4. Environment file: A properly configured .env file must exist 

# Directory Structure
The following structure must exist before running the script:

- Your SAPPHIRE_forecast_tools installation directory (default: /data/SAPPHIRE_forecast_tools)  
- An environment file (default: /data/SAPPHIRE_forecast_tools/config/.env)  

# Ports
Port 8082: Must be available for the Luigi web interface

# Running the Setup Script
```bash
# Navigate to the script directory
cd /path/to/SAPPHIRE_forecast_tools

# Run the setup script with sudo
sudo bash bin/setup_luigi_daemon.sh [BASE_SAPCA_PATH] [BASE_SAPCA_ENV_FILE_PATH]
```

# What the Script Sets Up
1. A dedicated 'luigi' system user  
2. Required directories with appropriate permissions  
3. Luigi configuration files  
4. A systemd service for persistent daemon operation  
5. Log rotation for Luigi logs  

After setup, you can access the Luigi web interface at http://localhost:8082 and manage the service with standard systemd commands (systemctl start/stop/restart luigid).