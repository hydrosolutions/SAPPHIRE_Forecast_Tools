# Conceptual Model Module

> **MAINTENANCE-ONLY:** This module is being phased out. No new features will be added. For new deployments, consider using the [machine_learning](../machine_learning/) module instead.

R-based hydrological forecasting module using the GR4J rainfall-runoff model with CemaNeige snow module. Produces ensemble streamflow forecasts using ECMWF weather forecast data.

## Overview

This module implements a lumped conceptual hydrological model combining:
- **GR4J** - 4-parameter daily rainfall-runoff model
- **CemaNeige** - Snow accumulation and melt module with 5 elevation bands

The model runs operationally to produce probabilistic (ensemble) streamflow forecasts.

## Scripts

| Script | Purpose |
|--------|---------|
| `run_operation_forecasting_CM.R` | Main operational forecast script |
| `run_initial.R` | Initial model setup and calibration |
| `run_manual_hindcast.R` | Manual hindcast runs for testing |
| `functions/functions_operational.R` | Core operational functions |
| `functions/functions_hindcast.R` | Hindcasting functions |

## Dependencies

### R Packages (CRAN)
- `tidyverse==2.0.0` - Data manipulation and visualization
- `jsonlite==1.8.8` - JSON configuration parsing
- `usethis==2.2.3` - R project utilities
- `here==1.0.1` - Path management

### R Packages (GitHub - Unmaintained)
- `hydrosolutions/airGR_GM` - Modified GR models
- `hydrosolutions/airgrdatassim` - Data assimilation for GR

**Warning:** The GitHub packages are no longer actively maintained.

## Docker Usage

The module uses `rocker/tidyverse:4.4.1` as base image (linux/amd64 only).

```bash
# Build the image (from repository root)
docker build --platform linux/amd64 \
    -f apps/conceptual_model/Dockerfile \
    -t mabesa/sapphire-conceptualmodel:latest .

# Run the container
docker run --rm --platform linux/amd64 \
    -e ieasyhydroforecast_env_file_path=/data/config/.env \
    -e IN_DOCKER_CONTAINER=True \
    -v /path/to/data/config:/data/config \
    -v /path/to/data/daily_runoff:/data/daily_runoff \
    -v /path/to/data/intermediate_data:/data/intermediate_data \
    -v /path/to/data/bin:/data/bin \
    -v /path/to/data/conceptual_model:/data/conceptual_model \
    mabesa/sapphire-conceptualmodel:latest
```

**Note:** On Apple Silicon Macs, the `--platform linux/amd64` flag enables QEMU emulation (slow but functional).

## Data Flow

```
ECMWF Weather Forecasts    Historical Runoff Data
(from preprocessing_gateway)    (from preprocessing_runoff)
         |                              |
         v                              v
    +-----------------------------------------+
    |    run_operation_forecasting_CM.R       |
    |    - Load basin parameters              |
    |    - Run CemaNeige snow model           |
    |    - Run GR4J rainfall-runoff model     |
    |    - Generate ensemble forecasts        |
    +-----------------------------------------+
                      |
                      v
            Ensemble Streamflow Forecasts
            (intermediate_data/forecasts/)
```

## Key Environment Variables

| Variable | Description |
|----------|-------------|
| `ieasyhydroforecast_env_file_path` | Path to the .env configuration file |
| `IN_DOCKER_CONTAINER` | Set to `True` when running in Docker |
| `ieasyhydroforecast_PATH_TO_JSON` | Path to JSON configuration directory |
| `ieasyhydroforecast_FILE_SETUP` | Name of the conceptual model config file |
| `ieasyhydroforecast_PATH_TO_Q` | Path to discharge data |

See [doc/configuration.md](../../doc/configuration.md) for the complete list.

## Local Development

```bash
# Set the path to your environment configuration file
export ieasyhydroforecast_env_file_path=/path/to/config/.env
export SAPPHIRE_OPDEV_ENV=True

# Run the operational forecast script
Rscript run_operation_forecasting_CM.R
```

## Deprecation Notice

This module is in **maintenance-only mode** for the following reasons:

1. **Unmaintained dependencies**: The `airGR_GM` and `airgrdatassim` GitHub packages are no longer actively maintained
2. **Security warnings**: DockerHub security scans flag older dependencies
3. **Architecture limitations**: The `rocker/tidyverse` base image is only available for linux/amd64, making local development on ARM machines slow due to emulation
4. **Strategic direction**: Development resources are focused on the `machine_learning` module

### What this means:
- Existing deployments will continue to be supported
- Critical bug fixes will be applied
- Security updates to the base image will be applied when feasible
- No new features will be added
- New users should consider the `machine_learning` module instead

## Related Documentation

- [Configuration Guide](../../doc/configuration.md) - Environment variable reference
- [Workflows](../../doc/workflows.md) - Pipeline architecture and scheduling
- [Machine Learning Module](../machine_learning/) - Recommended alternative for new deployments
