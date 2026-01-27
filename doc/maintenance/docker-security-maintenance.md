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

---

## Security Disclaimer and Liability

### Software Provided "As-Is"

SAPPHIRE Forecast Tools is provided under the MIT License on an "as-is" basis. While we make reasonable efforts to maintain security through quarterly rebuilds and dependency updates, **hydrosolutions GmbH and contributors cannot guarantee the absence of security vulnerabilities** in the software or its dependencies.

### Deploying Organization Responsibilities

Organizations deploying SAPPHIRE Forecast Tools are responsible for:

1. **Security assessment** - Evaluating whether the software meets their security requirements
2. **Infrastructure security** - Securing the host systems, networks, and access controls
3. **Monitoring** - Monitoring for security issues in their deployment
4. **Updates** - Keeping deployments updated with latest images
5. **Data protection** - Protecting sensitive data processed by the system
6. **Compliance** - Ensuring deployment meets their regulatory requirements

### Non-Root Container Mode

Our default images run as root for cross-platform compatibility. For organizations requiring non-root containers:

- We provide documented runtime override options in the [UV Migration Plan](../../implementation_planning/uv_migration_plan.md#2-non-root-user-strategy)
- The deploying organization assumes responsibility for host permission management
- Permission issues from non-root mode are outside our support scope

### Template Response for Security Inquiries

When partners raise security concerns, use this response:

> SAPPHIRE Forecast Tools is open-source software provided under the MIT License. Our images run as root by default for cross-platform compatibility across development (macOS) and production (Ubuntu) environments.
>
> For organizations requiring non-root containers, we provide documented runtime override options. The deploying organization assumes responsibility for:
> - Host permission management when enabling non-root mode
> - Security assessment of the deployment
> - Infrastructure and network security
> - Compliance with their regulatory requirements
>
> We maintain the software through quarterly security rebuilds and welcome security issue reports via GitHub. However, as stated in our MIT License, the software is provided "as-is" without warranty.

### Reporting Security Issues

If you discover a security vulnerability:

1. **Do not** open a public GitHub issue
2. Contact hydrosolutions directly via email
3. Provide details of the vulnerability
4. Allow reasonable time for a fix before public disclosure

### License

This software is licensed under the [MIT License](../../LICENSE).
