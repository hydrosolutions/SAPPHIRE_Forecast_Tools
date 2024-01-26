# Deployment

This repository uses GitHub Actions to automatize the deployment of the forecast tools. Every time a release is published or edited, the updated image will be deployed automatically on the host machine where the Docker repositories are watched for updates. The following section describes the steps that are required on the host machine to initially deploy the forecast tools.

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

### Install Chrome Browser -> TODO: check if this is still necessary
Chrome browser was used to visualized the dashboards in this project. The use of a different browser has not been tested. If you prefer to use a different browser, you will have to edit the bat files bat/configuration.bat and bat/dashboard.bat in the bat folder.
The configuration dashboard also works in the edge browser.

## Deployment
The sections below describe the steps that are required to deploy the forecast tools on the host machine.

### .env
As the file system structure may differ between the development platform and the host machine, the SAPPHIRE forecast tools read in a .env file which may be a copy of the .env_develop file, when run within a docker container.

Copy .env_develop to .env and edit the file where required (if for example the locations for the bulletin templates on the host machine differs from the location of the bulletin templates in this repository). Please read the instructions about the configuration of the .env file in the file [doc/configuration.md](doc/configuration.md).

### Configuration dashboard
Pull the latest image from Docker Hub:
```bash
docker pull mabesa/sapphire-configuration:latest
```
Run the image:
```bash
docker run -d --label=com.centurylinklabs.watchtower.enable=true -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/config:/app/apps/config -v <full_path_to>/data:/app/data -v <full_path_to>/bat:/app/bat -p 3647:3647 --name fcconfig mabesa/sapphire-configuration:latest
```
Replace <full_path_to> with your local path to the folders. The -v option mounts the folders on the host machine to the folders in the docker container. The -p option maps the port 3647 on the host machine to the port 3647 in the docker container. The -e option sets the environment variable IN_DOCKER_CONTAINER to True. This is required to run the dashboard locally in the docker container. The --label option tells watchtower to update the container when a new version is available.

An example run command is:
```bash
docker run -d --label=com.centurylinklabs.watchtower.enable=true -e "IN_DOCKER_CONTAINER=True" -v /home/sarah/SAPPHIRE_Forecast_Tools/apps/config:/app/apps/config -v /home/sarah/SAPPHIRE_Forecast_Tools/data:/app/data -v /home/sarah/SAPPHIRE_Forecast_Tools/bat:/app/bat -p 3647:3647 --name fcconfig mabesa/sapphire-configuration:latest
```

Make sure the image and tool names in bat/configuration.bat are correct for the host machine. For example, if you are using a different image name, you will have to replace fcconfig in line 1 with your image name. If you have changed the port number in the run command, you will have to change the port number in line 2 of bat/configuration.bat.

Create a shortcut to bat/configuration_dashboard/configuration.bat on your desktop. You can edit the icon of the shortcut by opening the preferences and selecting the icon available at bat/configuration_dashboard/Station.ico.

Now you're done! Double click the shortcut to open the dashboard in a browser window.

### Backend
Pull the docker image from Docker Hub. Open a terminal and run the following command:
```bash
docker pull mabesa/sapphire-backend:latest
```
Run the image:
```bash
docker run -d --label=com.centurylinklabs.watchtower.enable=true -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/apps/config:/app/apps/config -v <full_path_to>/apps/internal_data:/app/apps/internal_data -v <full_path_to>/data:/app/data -v <full_path_to>/bat:/app/bat -p 9000:8801 --name fcbackend mabesa/sapphire-backend:latest
```
Replace <full_path_to> with your local path to the folders. The -v option mounts the folders on the host machine to the folders in the docker container. The -e option sets the environment variable IN_DOCKER_CONTAINER to True. This is required to run the dashboard locally in the docker container. The --label option tells watchtower to update the container when a new version is available.
Note that you'll only need the -p option if you wish to open a port to access the iEasyHydro database. In that case you'll need to make sure the port mapping is correct.

Now we set up the Task Scheduler on Windows to restart the backend every day at 10 a.m. Open the Task Scheduler and create a new task. Give the task a name and select the option to run the task with the highest privileges whether user is logged on or not. Under Triggers select the option to run the task daily at 10 a.m. Under Actions select the bat file /bat/backend/backend.bat. Under Conditions select the option to wake the computer to run the task. Under Settings select the option to stop the existing instance if the task is already running when the task scheduler wants to start it. Click OK to save the task.

TODO: comments about the task scheduler user/user group to choose

To test if the task scheduler works, you can run the task manually. You can also check the task history to see if the task was run successfully.


### Forecast dashboard
Pull the docker image from Docker Hub. Open a terminal and run the following command:
```bash
docker pull mabesa/sapphire-dashboard:latest
```
Run the image:
```bash
docker run -d --label=com.centurylinklabs.watchtower.enable=true -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/apps/config:/app/apps/config -v <full_path_to>/apps/internal_data:/app/apps/internal_data -v <full_path_to>/data:/app/data -v <full_path_to>/bat:/app/bat -p 5006:5006 --name fcbackend mabesa/sapphire-dashboard:latest
```
Note that you will have to replace <full_path_to> with the full path to your local repository.

Make sure that the image name is correct in the .bat file. Then create a shortcut to bat/dashboard.bat on your desktop. You can edit the icon of the shortcut by opening the preferences and selecting the icon available at bat/dashboard/Station.ico.



