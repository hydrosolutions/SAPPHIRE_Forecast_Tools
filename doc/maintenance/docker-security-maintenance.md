# Docker Image Security Maintenance

This document describes how Docker images for SAPPHIRE Forecast Tools are kept secure through automated quarterly rebuilds.

## Overview

- **Automated Schedule**: All Docker images are rebuilt quarterly (Jan 1, Apr 1, Jul 1, Oct 1)
- **Fresh Upstream**: Each rebuild pulls the latest base images (e.g., `python:3.11-slim-bookworm`) to pick up security patches
- **Quarterly Tags**: Each rebuild creates versioned tags (e.g., `:2025-Q1`) for reproducibility
- **Tests Run First**: Images are only pushed if the test suite passes

## Automated Quarterly Rebuild

The GitHub Actions workflow `scheduled_security_rebuild.yml` runs automatically on:
- January 1st
- April 1st
- July 1st
- October 1st

### What Happens During a Rebuild

1. **Base image rebuilt** from fresh `python:3.11-slim-bookworm`
2. **Tests run** to verify the rebuilt image works correctly
3. **Base image pushed** with `:latest` and quarterly tag (e.g., `:2025-Q1`)
4. **All module images rebuilt** using the fresh base image
5. **Module images pushed** with `:latest` and quarterly tag

## Manual Rebuild (On-Demand)

If a critical security vulnerability is announced between quarters, you can trigger a rebuild manually:

1. Go to the repository on GitHub
2. Click the **Actions** tab
3. Select **Quarterly Security Rebuild** from the left sidebar
4. Click **Run workflow**
5. (Optional) Check "Rebuild only base image" if you only need to update the base
6. Click the green **Run workflow** button

The workflow will run and push fresh images with the current quarter's tag.

## Checking for Vulnerabilities

### DockerHub Scout

DockerHub provides vulnerability scanning for all images:

1. Go to [DockerHub - sapphire-pythonbaseimage](https://hub.docker.com/r/mabesa/sapphire-pythonbaseimage)
2. Click on the **Tags** tab
3. Look for the vulnerability count badge next to each tag
4. Click on a tag to see detailed vulnerability information

### Understanding Vulnerability Reports

- **Critical/High**: Should be addressed promptly - consider triggering a manual rebuild
- **Medium**: Usually addressed in the next quarterly rebuild
- **Low**: Informational, typically no action needed

Note: Some vulnerabilities may not have fixes available yet from upstream. These will be resolved when the upstream maintainers release patches.

## Reproducibility with Quarterly Tags

### The Problem with `:latest`

Using `FROM baseimage:latest` means builds at different times may produce different results, which can complicate debugging and rollbacks.

### Solution: Quarterly Tags

Each quarterly rebuild creates versioned tags:
- `:2025-Q1` - January 2025 rebuild
- `:2025-Q2` - April 2025 rebuild
- `:2025-Q3` - July 2025 rebuild
- `:2025-Q4` - October 2025 rebuild

### When to Use Each Tag

| Tag | Use Case |
|-----|----------|
| `:latest` | Development, getting newest security fixes |
| `:2025-Q1` | Production deployments requiring reproducibility |
| `:v0.2.0` | Specific release versions |

### Pinning to a Quarterly Tag

To ensure reproducible builds, update your Dockerfile:

```dockerfile
# Instead of:
FROM mabesa/sapphire-pythonbaseimage:latest

# Use:
FROM mabesa/sapphire-pythonbaseimage:2025-Q1
```

## Rollback Procedure

If a quarterly rebuild introduces issues:

### Quick Fix: Pin to Previous Quarter

Update affected Dockerfiles to use the previous quarter's tag:

```dockerfile
# Roll back from Q2 to Q1
FROM mabesa/sapphire-pythonbaseimage:2025-Q1
```

### Rebuild Locally

If you need to rebuild with the old base:

```bash
# Pull the previous quarter's base image
docker pull mabesa/sapphire-pythonbaseimage:2025-Q1

# Build your module using that base
docker build --build-arg BASE_TAG=2025-Q1 -t my-module:rollback .
```

### Report the Issue

If a rebuild breaks functionality:
1. Open an issue at [GitHub Issues](https://github.com/hydrosolutions/SAPPHIRE_forecast_tools/issues)
2. Include the quarterly tag that caused the issue
3. Describe what broke and any error messages

## For System Administrators

### Updating Deployed Systems

After a quarterly rebuild:

1. **Pull new images**:
   ```bash
   docker-compose pull
   ```

2. **Restart services**:
   ```bash
   docker-compose down
   docker-compose up -d
   ```

3. **Verify functionality**: Check that forecasts run correctly

### Staying on a Specific Version

If you need stability over newest security fixes:

1. Update `docker-compose.yml` to pin specific tags:
   ```yaml
   services:
     preprocessing:
       image: mabesa/sapphire-preprunoff:2025-Q1
   ```

2. Only update when you're ready to test the new version

## Available Images

| Image | Description |
|-------|-------------|
| `mabesa/sapphire-pythonbaseimage` | Base Python image with common dependencies |
| `mabesa/sapphire-pipeline` | Luigi pipeline orchestrator |
| `mabesa/sapphire-dashboard` | Forecast visualization dashboard |
| `mabesa/sapphire-preprunoff` | Runoff data preprocessing |
| `mabesa/sapphire-prepgateway` | Gateway data preprocessing |
| `mabesa/sapphire-linreg` | Linear regression forecasting |
| `mabesa/sapphire-ml` | Machine learning forecasting |
| `mabesa/sapphire-postprocessing` | Forecast postprocessing |
| `mabesa/sapphire-rerun` | Forecast rerun utility |
| `mabesa/sapphire-conceptmod` | Conceptual model forecasting |

## Questions?

For questions about security maintenance:
- Check existing [GitHub Issues](https://github.com/hydrosolutions/SAPPHIRE_forecast_tools/issues)
- Open a new issue if needed
