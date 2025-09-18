# GitHub Copilot Instructions for SAPPHIRE Forecast Tools

## Project Overview
SAPPHIRE Forecast Tools is a comprehensive hydrological forecasting system that provides water flow predictions using multiple modeling approaches including linear regression, machine learning models, and conceptual hydrological models.

## Architecture
- **Pipeline-based**: Uses Luigi task orchestration for workflow management
- **Docker containerized**: Each model runs in isolated Docker containers
- **Multi-organization**: Supports different configurations (demo, kghm, etc.)
- **Multi-temporal**: Supports pentadal (5-day) and decadal (10-day) forecasts

## Key Components

### Pipeline (apps/pipeline/)
- `pipeline_docker.py`: Main Luigi workflow orchestration
- Task-based execution with timeout and retry mechanisms
- Docker container management for different forecast models

### Models
- **Linear Regression** (`apps/linear_regression/`): Statistical forecasting
- **Machine Learning** (`apps/machine_learning/`): ML-based forecasting models
- **Conceptual Model** (`apps/conceptual_model/`): Hydrological process models

### Dashboards
- **Forecast Dashboard** (`apps/forecast_dashboard/`): Web interface for viewing forecasts
- **Configuration Dashboard** (`apps/configuration_dashboard/`): System configuration interface

### Preprocessing
- **Gateway Preprocessing** (`apps/preprocessing_gateway/`): Weather data processing
- **Runoff Preprocessing** (`apps/preprocessing_runoff/`): Historical flow data processing
- **Station Forcing** (`apps/preprocessing_station_forcing/`): Station-specific data prep

## Environment Configuration
- Uses `.env` files for configuration management
- Environment class handles configuration loading
- Docker image tags and organization settings configurable
- Path management with `ieasyforecast_*` environment variables

## Key Patterns

### Docker Task Base Class
- All Docker tasks inherit from `DockerTaskBase`
- Implements timeout management, retry logic, and failure notifications
- Standardized logging and marker file creation

### Workflow Organization
```python
# Example workflow pattern
class RunPentadalWorkflow(luigi.Task):
    def requires(self):
        return [LinearRegression(), RunAllMLModels(), PostProcessingForecasts()]
```

### Path Management
- Use `get_absolute_path()` for converting relative to absolute paths
- Use `get_bind_path()` for Docker volume binding
- Environment variables define data root directories

### Marker Files
- Tasks create marker files for completion tracking
- External tasks check for existing marker files
- Located in `{intermediate_data_path}/marker_files/`

## Coding Guidelines

### Error Handling
- Always implement timeout and retry mechanisms for long-running tasks
- Send failure notifications with log attachments
- Use try-catch blocks with proper cleanup

### Luigi Tasks
- Inherit from appropriate base classes (`DockerTaskBase`, `luigi.Task`)
- Implement `requires()`, `output()`, and `run()` methods
- Use marker files for external task dependencies

### Docker Integration
- Use `setup_docker_volumes()` for volume configuration
- Pass environment variables through `environment` parameter
- Implement proper container cleanup

### Configuration
- Access configuration through the `Environment` class
- Use environment variables for paths and settings
- Support multiple organizations with conditional logic

## File Naming Conventions
- Docker logs: `log_{taskname}_{timestamp}.txt`
- Marker files: `{taskname}_{date}.marker`
- Configuration files: `config_{purpose}.json`

## Common Environment Variables
- `ieasyhydroforecast_env_file_path`: Path to environment file
- `ieasyforecast_intermediate_data_path`: Working data directory
- `ieasyforecast_configuration_path`: Configuration files location
- `ieasyhydroforecast_organization`: Target organization (demo, kghm)
- `ieasyhydroforecast_run_ML_models`: Enable/disable ML models
- `ieasyhydroforecast_run_CM_models`: Enable/disable conceptual models

## Testing
- Use pytest for unit testing (`apps/pytest.ini`)
- Test files located in `tests/` subdirectories
- Run tests with `apps/run_tests.sh`

## When Contributing
1. Follow the existing class inheritance patterns
2. Implement proper timeout and retry mechanisms
3. Add appropriate logging and error handling
4. Update configuration files if needed
5. Test with both demo and production configurations
6. Document any new environment variables

## Debugging Tips
- Check Docker logs in `{intermediate_data_path}/docker_logs/`
- Verify marker files for task completion status
- Use Luigi's dependency visualization for workflow debugging
- Check environment variable resolution in path functions
