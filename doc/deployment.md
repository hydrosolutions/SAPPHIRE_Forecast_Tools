# Deployment

This document describes how to deploy the application.

## Prerequisites
On the computer where the forecast tools are deployed:

Install the software [Docker](https://docs.docker.com/install/)

Start a Watchtower instance to automatically update the application when a new version is available:
```bash
docker run -d \
  --name watchtower \
  -v /var/run/docker.sock:/var/run/docker.sock \
  containrrr/watchtower --label-enable --interval 30
```
The label-enable option tells watchtower to only update containers that have the label com.centurylinklabs.watchtower.enable=true in their run command (see below).

## Deployment

### Forecast dashboard
```bash
docker pull mabesa/sapphire-dashboard:<tag_name>
docker run -d --label=com.centurylinklabs.watchtower.enable=true mabesa/sapphire-dashboard:<tag_name>
```





