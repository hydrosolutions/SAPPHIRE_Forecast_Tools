# Deployment

This repository uses GitHub Actions to automatize the deployment of the forecast tools. Every time a release is published or edited, the updated image will be deployed automatically on the host machine where the Docker repositories are watched for updates. The following section describes the steps that are required on the host machine to deploy the forecast tools.

## Prerequisites
Perform the following steps the computer where the forecast tools are deployed.

### Download this repository
Download the [repository](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools) to the host machine. The repository can be downloaded as a zip file from the GitHub website. Unzip the file and move the folder to the desired location on the host machine. This allows you to only perform minimal edits to the configuration files within the designed folder structure.

### Install Docker & run Watchtower
Install the software [Docker](https://docs.docker.com/install/)

Start a Watchtower instance to automatically update the application when a new version is available:
```bash
docker run -d \
  --name watchtower \
  -v /var/run/docker.sock:/var/run/docker.sock \
  containrrr/watchtower --label-enable --interval 30 --cleanup
```
The label-enable option tells watchtower to only update containers that have the label com.centurylinklabs.watchtower.enable=true in their run command (see below). The interval option tells watchtower to check for new versions every 30 seconds. After testing, this intervall can be increased. The cleanup option tells watchtower to remove old images after updating the container.

### Install Chrome Browser
Chrome browser was used to visualized the dashboards in this project. The use of a different browser has not been tested. If you prefer to use a different browser, you will have to edit the bat files bat/configuration.bat and bat/dashboard.bat in the bat folder.

## Deployment
The sections below describe the steps that are required to deploy the forecast tools on the host machine.

### .env
As the file system structure may differ between the development platform and the host machine, the SAPPHIRE forecast tools read in a .env file which may be a copy of the .env_develop file, when run within a docker container.

Copy .env_develop to .env and edit the file where required (if for example the locations for the bulletin templates on the host machine differs from the location of the bulletin templates in this repository).


### Configuration dashboard
Pull the latest image from Docker Hub:
```bash
docker pull mabesa/sapphire-configuration:latest
```
Run the image:
```bash
docker run -d --label=com.centurylinklabs.watchtower.enable=true -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/config:/app/apps/config -v <full_path_to>/data:/app/data -p 3647:3647 --name fcdashboard mabesa/sapphire-configuration:latest
```
Replace <full_path_to> with your local path to the folders. The -v option mounts the folders on the host machine to the folders in the docker container. The -p option maps the port 3647 on the host machine to the port 3647 in the docker container. The -e option sets the environment variable IN_DOCKER_CONTAINER to True. This is required to run the dashboard locally in the docker container. The --label option tells watchtower to update the container when a new version is available.

An example run command is:
```bash
docker run -d --label=com.centurylinklabs.watchtower.enable=true -e "IN_DOCKER_CONTAINER=True" -v /home/sarah/SAPPHIRE_Forecast_Tools/apps/config:/app/apps/config -v /home/sarah/SAPPHIRE_Forecast_Tools/data:/app/data -p 3647:3647 --name fcconfiguration mabesa/sapphire-configuration:latest
```



### Forecast dashboard
```bash
docker pull mabesa/sapphire-dashboard:latest
docker run -d --label=com.centurylinklabs.watchtower.enable=true --name fcdashboard mabesa/sapphire-dashboard:latest
```





