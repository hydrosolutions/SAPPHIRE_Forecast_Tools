# SAPPHIRE Base Docker Image
This base Docker image provides the foundation for all SAPPHIRE Forecast Tools containerized components.

## Overview
This Python 3.11 image serves as the common base for all SAPPHIRE Forecast Tools microservices, ensuring consistency across environments from development to deployment at Kyrgyz Hydromet.

## Key Features
- Python 3.11 on Debian Bookworm (slim) optimized for forecasting operations
- Non-root user execution with UID/GID 10001:10001 for improved security
- Pre-installed iEasyHydroForecast package and required dependencies
- Buildkit-enabled caching for efficient builds
- Compatible across cloud and local deployments

## Usage
### Building the Base Image

```bash
# From project root
docker build -t mabesa/sapphire-pythonbaseimage:latest -f ./apps/docker_base_image/Dockerfile .

# With custom user ID (for local volume permissions)
docker build --build-arg USER_ID=$(id -u) --build-arg GROUP_ID=$(id -g) \
  -t mabesa/sapphire-pythonbaseimage:custom -f ./apps/docker_base_image/Dockerfile .
```

### Using as Base for Component Images

```bash
FROM mabesa/sapphire-pythonbaseimage:latest

# Your component-specific instructions
```

## Volume Permissions
Since the container runs as appuser (UID=1000), ensure mounted volumes have proper permissions:

```bash
# On the host system before mounting
sudo chown -R 1000:1000 /path/to/data
```

## Security Considerations

- Non-root execution: Container runs as user appuser (not root)
- Fixed UID/GID: Consistent 1000:1000 across environments
- Digital signing: Images signed with Cosign in CI/CD pipeline

## CI/CD Integration
This image is automatically built and published via GitHub Actions when changes are pushed to the main branch:
```yaml
- name: Build Docker image
  run: |
    DOCKER_BUILDKIT=1 docker build --no-cache \
    -t "${{ env.BASE_IMAGE_NAME }}:${{ env.IMAGE_TAG }}" \
    -f ./apps/docker_base_image/Dockerfile .
```

## Building for Local Development
For local development across the SAPPHIRE ecosystem:

```bash
# Clean existing containers/images
bash bin/utils/clean_docker.sh

# Build all images
ieasyhydroforecast_data_root_dir=/path/to/data bash bin/utils/build_docker_images.sh latest
```

## Deployment Considerations
For deployment at Kyrgyz Hydromet:

- Ensure all mounted volumes have appropriate permissions
- Use latest tag for deployments on the staging server, and the local tag for production deployments on the local server

## Troubleshooting
If experiencing permission issues with mounted volumes:

- Check the UID/GID of your server user: id -u and id -g
- Adjust volume permissions: sudo chown -R 1000:1000 /path/to/volume
- For special cases, rebuild with custom IDs: --build-arg USER_ID=10001 --build-arg GROUP_ID=10001