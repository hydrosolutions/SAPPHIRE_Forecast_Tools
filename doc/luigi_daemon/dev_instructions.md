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

## 0. Prerequisites
Please ensure that you have the [prerequisites](../development.md) available and the .env file is set up correctly. 

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
Run one of the Luigi tasks defined in your pipeline. You can run a workflow using the shell scripts provided in the `bin` directory:

```bash
# Example: Run the PreprocessingRunoff workflow
bash bin/run_preprocessing_runoff.sh <path/to/.env>
```
Please remember that the intended sequence of tasks is:  
1. run_preprocessing_runoff.sh and run_preprocessing_gateway.sh
2. run_pentadal_forecasts.sh and run_decadal_forecasts.sh

## 4. Monitor task execution 
- Check the web interface at http://localhost:8082  
- Check the docker desktop image and container logs 
- View logs in the logs directory

## 5. Stop the Luigi daemon when done
```bash
# Use the provided kill command or simply: 
kill $(cat /tmp/luigid.pid)
```


