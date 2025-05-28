# Luigi Daemon Development Instructions

# Using run_luigi_dev.sh for Development
The `dev/run_luigi_dev.sh` script provides an easy way to run a Luigi daemon for development purposes without setting up a full systemd service. This is ideal for testing and development work. For deployment, use the systemd service as described in the [ubuntu_setup](ubuntu_setup.md) documentation.

What this script does:
1. Sets up the Python environment with the correct PYTHONPATH
2. Creates a logs directory for Luigi output
3. Checks if Luigi is installed and installs it if needed
4. Stops any existing Luigi daemon processes
5. Starts a new Luigi daemon on port 8082 in background mode
6. Verifies the daemon started successfully

# Usage Instructions

## 1. Run the script
```bash
# Navigate to the SAPPHIRE_forecast_tools root directory
cd /path/to/SAPPHIRE_forecast_tools
   
# Run the development Luigi daemon script
bash dev/run_luigi_dev.sh
```

## 2. Verify the daemon is running
- Check the output for "Luigi daemon started successfully!"  
- Open the web UI at http://localhost:8082 in your browser  
- You should see the Luigi Task Visualiser interface  

## 3. Run your Luigi tasks
```bash
# Example: Run the workflow with the local scheduler
PYTHONPATH='.' luigi --module apps.pipeline.pipeline_docker RunWorkflow --local-scheduler
```
## 4. Monitor task execution 
- Check the web interface at http://localhost:8082  
- View logs in the logs directory

## 5. Stop the Luigi daemon when done
```bash
# Use the provided kill command or simply: 
kill $(cat /tmp/luigid.pid)
```


