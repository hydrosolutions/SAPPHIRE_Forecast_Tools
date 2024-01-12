# Deployment

This document describes how to deploy the application.

## Prerequisites
Install the software [Docker](https://docs.docker.com/install/)

Start a Watchtower instance to automatically update the application when a new version is available:
```bash
docker run -d \
  --name watchtower \
  -v /var/run/docker.sock:/var/run/docker.sock \
  containrrr/watchtower --label-enable
```
The label-enable option tells watchtower to only update containers that have the label com.centurylinklabs.watchtower.enable=true.




