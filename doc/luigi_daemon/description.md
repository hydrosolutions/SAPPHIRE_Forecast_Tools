# Luigi Daemon 
The Luigi daemon (luigid) is a critical component for managing complex workflows in SAPPHIRE_forecast_tools. Here's why it's needed:

## Role in Pipeline Management
The Luigi daemon serves as a central scheduler that:

- Tracks task dependencies and execution state: Ensures tasks run in the correct order based on their dependencies  
- Prevents duplicate work: Knows which tasks have already completed successfully  
- Provides failure handling: Can resume a pipeline from the point of failure rather than starting over  
- Maintains persistent state: Preserves pipeline state information across system reboots  
- Enables centralized monitoring: Offers a web interface at http://localhost:8082 to track task execution status  

## Connection to the Pipeline
Looking at the SAPPHIRE pipeline code, the workflow contains numerous interdependent tasks (preprocessing, forecasting, post-processing). When these tasks are executed:

1. Each task registers with the Luigi daemon  
2. The daemon tracks which tasks have completed successfully  
3. The daemon only allows dependent tasks to run when prerequisites are satisfied  
4. If the pipeline stops unexpectedly, the daemon knows which tasks were completed  

This orchestration is essential for reliability in forecasting workflows that might involve lengthy processing steps and complex dependencies between tasks.

## Operational Importance
For production environments, the Luigi daemon is deployed as a systemd service to ensure it:

- Starts automatically on system boot  
- Restarts if it crashes  
- Logs activity properly  
- Runs with appropriate permissions  

Without the daemon, managing task completion state and dependencies would need to be handled manually, making pipeline execution much less reliable.

## Production setup
For production use, we set up the Luigi daemon as a systemd service using the script `bin/setup_luigi_daemon.sh`. This ensures that the daemon runs continuously in the background, automatically starting on system boot and restarting if it crashes. The service is configured to log output to a specified directory, allowing for easy monitoring and debugging. See [ubuntu_setup.md](ubuntu_setup.md) documentation for details on how to set up the systemd service.

## Development setup
For development purposes, we provide a script (`dev/run_luigi_dev.sh`) that allows you to run the Luigi daemon without needing to set up a full systemd service. This is useful for testing and development work, as it simplifies the process of starting and stopping the daemon. See the [dev_instructions.md](dev_instructions.md) documentation for details on how to use this script.