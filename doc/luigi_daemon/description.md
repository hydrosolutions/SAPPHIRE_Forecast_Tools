# Luigi Daemon
The Luigi daemon (luigid) is a critical component for managing complex workflows in SAPPHIRE_Forecast_Tools. Here's why it's needed:

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
For production environments, the Luigi daemon is deployed as a persistent service to ensure it:

- Starts automatically on system boot  
- Restarts if it crashes  
- Logs activity properly  
- Runs with appropriate permissions  

Without the daemon, managing task completion state and dependencies would need to be handled manually, making pipeline execution much less reliable.

## Production setup (recommended)
Run the Luigi daemon as a persistent Docker container via Docker Compose. This matches the rest of the stack and is what the run scripts expect.

- Compose file: `bin/docker-compose-luigi.yml`
- Service name: `luigi-daemon` (tasks resolve this DNS name)
- Policy: `restart: unless-stopped`

Key notes:
- Use a stable Compose project name so the daemon and tasks share one network (for example: `export COMPOSE_PROJECT_NAME=sapphire`).
- Start the daemon once and keep it running:
	- `docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon`
- Access the web UI at: `http://localhost:8082`

Complete Ubuntu instructions (including decommissioning legacy setups) are in [ubuntu_setup.md](ubuntu_setup.md).

Networking reminder:
- Tasks connect to the scheduler using `scheduler_host = luigi-daemon`, `scheduler_port = 8082` inside the Compose network.
- Ensure daemon and task commands use the same Compose project/directory so they share one network.

## Development setup
For development, you can run a temporary Luigi daemon without Compose using `dev/run_luigi_dev.sh`. This is useful for quick, local testing. See [dev_instructions.md](dev_instructions.md).

## Legacy (non-Docker) option
If you must run luigid as a system service (`systemd`), see the legacy notes in [ubuntu_setup.md](ubuntu_setup.md). The Compose-based approach above is the primary and supported method.