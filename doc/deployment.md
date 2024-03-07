# Deployment

This repository uses GitHub Actions to automatize the deployment of the forecast tools. Every time a release is published or edited, the updated image will be deployed automatically on the host machine where the Docker repositories are watched for updates. The following section describes the steps that are required on the host machine to initially deploy the forecast tools.

## Prerequisites
Perform the following steps the computer where the forecast tools are deployed.

### Download this repository
Download the [repository](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools) to the host machine. The repository can be downloaded as a zip file from the GitHub website. Unzip the file and move the folder to the desired location on the host machine. This allows you to only perform minimal edits to the configuration files within the designed folder structure.

### Install Docker & run Watchtower
Install the software [Docker](https://docs.docker.com/install/). Note that you will need administrator rights to install Docker successfully. To allow that Docker services are started when the computer is turned on, you will have to change the settings in the Docker Desktop application. Open the application and go to Settings > General. Select the option to start Docker Desktop when you log in. In addition, you can tell windows to automatically start the docker services by following these steps:
1. Open the Run dialog by pressing the Windows key + R.
2. Type services.msc and press Enter.
3. In the Services window, scroll down to Docker Desktop Service.
4. Right-click on Docker Desktop Service and select Properties.
5. In the Properties window, change the Startup type to Automatic.
6. Click OK to save the changes.
The above steps are for Windows 10. If you are using a different version of Windows, the steps may differ. They are necessary to ensure that the containers are restarted automatically when the computer was turned off.

Start a Watchtower instance to automatically update the application when a new version is available by typing the following command in a Unix terminal. If you are using Windows, you should have Linux for Window (WSL) installed and use the Linux terminal. To check if your wsl is ready for use, open a PwerShell window and type:
```bash
wsl
```
If you get an error message, you may have to trubble shoot your wsl installation. See [this link](https://docs.microsoft.com/en-us/windows/wsl/install-win10) for more information.

```bash
docker run -d \
  --name watchtower \
  --restart always \
  -v /var/run/docker.sock:/var/run/docker.sock \
  containrrr/watchtower --label-enable --interval 30 --cleanup
```
The label-enable option tells watchtower to only update containers that have the label com.centurylinklabs.watchtower.enable=true in their run command (see below). The interval option tells watchtower to check for new versions every 30 seconds. After testing, this intervall can be increased. The cleanup option tells watchtower to remove old images after updating the container. We further tell watchtower to restart the container always, for example after the computer was turned off.

Note that you should not stop the running containers for the watchtower to be able to automatically deploy software updates.

### Install Chrome Browser (optional)
Chrome browser was used to visualized the dashboards in this project. If you prefer to use a different browser, you will have to edit the bat files bat/configuration.bat and bat/dashboard.bat in the bat folder.

## Deployment
The sections below describe the steps that are required to deploy the forecast tools on the host machine. If you want to test the tools with the demo data, you don't need to adapt the files in the apps/config folder and skip the .env chapter below.

### .env
Edit the apps/config/.env file where required. If, for example, the locations for the bulletin templates on the host machine differs from the location of the bulletin templates in this repository you need to adapt the path in the .env file. Please carefully follow the instructions about the configuration of the .env file in the file [doc/configuration.md](configuration.md).

If you wish to link the forecast tools with the iEasyHydro database you will have to provide the access credentials in .env and make sure the database is accessible through a port (in the example below we use port 9000).

### Configuration dashboard
Pull the latest image from Docker Hub:
```bash
docker pull mabesa/sapphire-configuration:deploy
```
Run the image:
```bash
docker run -d --label=com.centurylinklabs.watchtower.enable=true -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/config:/app/apps/config -v <full_path_to>/data:/app/data -p 3647:3647 --name fcconfig mabesa/sapphire-configuration:deploy
```
Replace <full_path_to> with your local path to the folders. The -v option mounts the folders on the host machine to the folders in the docker container. The -p option maps the port 3647 on the host machine to the port 3647 in the docker container. The -e option sets the environment variable IN_DOCKER_CONTAINER to True. This is required to run the dashboard locally in the docker container. The --label option tells watchtower to update the container when a new version is available.

An example run command is:
```bash
docker run -d --label=com.centurylinklabs.watchtower.enable=true -e "IN_DOCKER_CONTAINER=True" -v /home/sarah/SAPPHIRE_Forecast_Tools/apps/config:/app/apps/config -v /home/sarah/SAPPHIRE_Forecast_Tools/data:/app/data -p 3647:3647 --name fcconfig mabesa/sapphire-configuration:deploy
```
Check if the configuration dashboard is running correctly by opening a browser window and typing 127.0.0.1:3647 in the address bar.

Make sure the image and tool names in bat/configuration.bat are correct for the host machine. For example, if you are using a different image name, you will have to replace fcconfig in line 1 with your image name. If you have changed the port number in the run command, you will have to change the port number in line 2 of bat/configuration.bat.

Create a shortcut to bat/configuration_dashboard/configuration.bat on your desktop. You can edit the icon of the shortcut by opening the preferences and selecting the icon available at bat/configuration_dashboard/Station.ico.

Now you're done! Double click the shortcut to open the dashboard in a browser window.

### Backend
Pull the docker image from Docker Hub. Open a terminal and run the following command:
```bash
docker pull mabesa/sapphire-backend:deploy
```
Run the image:
```bash
docker run -d --label=com.centurylinklabs.watchtower.enable=true -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/apps/config:/app/apps/config -v <full_path_to>/apps/internal_data:/app/apps/internal_data -p 9000:8801 --name fcbackend mabesa/sapphire-backend:deploy
```
Replace <full_path_to> with your local path to the folders. The -v option mounts the folders on the host machine to the folders in the docker container. The -e option sets the environment variable IN_DOCKER_CONTAINER to True. This is required to run the dashboard locally in the docker container. The --label option tells watchtower to update the container when a new version is available.
Note that you'll only need the -p option if you wish to open a port to access the iEasyHydro database. In that case you'll need to make sure the port mapping is correct.

For testing the backend, we recommend to choose a recent date to start the forecasting for. You can set the date in the file apps/internal_data/last_successful_run.txt to one calendar day before the date you want to start the forecasting. The date must be in the format YYYY-MM-DD. We recommend starting the forecasting one day before the start of the most recent pentad.

Test if the backend is running. You can do this by checking the Logs tab in the Docker Desktop application. If there are recent outputs but there is no error message displayed at the bottom of the log tab, the backend is running correctly.

Now we need to make sure the bat file is set to go. We provide a basic bat file that will start the backend. If you need to set up a connection to the iEasyHydro database as preliminary to run the backend, you may edit the backend file. Below is an example by @maxatp of how to establish an ssh tunnel prior to running the backend:
```
@echo off
setlocal enabledelayedexpansion

set REMOTE_USER=<user_name_of_machine_running_iEasyHydro>
set REMOTE_HOST=<ip_address_of_machine_running_iEasyHydro>
set REMOTE_PORT=<port_open_on_machine_running_iEasyHydro>
set LOCAL_PORT=9000
set DESTINATION_HOST=localhost
set DESTINATION_PORT=<port_open_on_machine_running_iEasyHydro>
set PASSWORD=<password_to_access_machine_running_iEasyHydro>

set TUNNEL_EXISTS=false

REM Check if the tunnel already exists
for /f "tokens=3,4,5" %%i in ('netstat -ano ^| findstr "1:%LOCAL_PORT%"') do (
    if %%i=="0.0.0.0:0" if %%j=="LISTENING" goto TunnelPid
    :TunnelPid
        set TUNNEL_PID=%%k
    if %%i=="0.0.0.0:0" if %%j=="LISTENING" goto TunnelExists
    :TunnelExists
        set TUNNEL_EXISTS=true
)

if !TUNNEL_PID! == 0 (
    echo here1
    echo SSH tunnel does not exist. Creating SSH tunnel...
    set TUNNEL_EXISTS=false
) else (
    REM Check if the SSH tunnel process is still running
    tasklist /fi "PID eq !TUNNEL_PID!" 2>nul | find /i /n "!TUNNEL_PID!" >nul
    if "%errorlevel%"=="0" (
        echo here2
        echo Tunnel already exists. PID: !TUNNEL_PID!
    ) else (
        echo here3
        echo Tunnel PID !TUNNEL_PID! not found. Creating SSH tunnel...
        set TUNNEL_EXISTS=false
    )
)

if !TUNNEL_EXISTS! == false (
    echo here4
    REM Create the SSH tunnel
    echo Creating SSH tunnel...
    start plink -batch -pw !PASSWORD! -L !LOCAL_PORT!:!DESTINATION_HOST!:!DESTINATION_PORT! !REMOTE_USER!@!REMOTE_HOST! -P !REMOTE_PORT!
    echo SSH tunnel created.
)

docker stop fcbackend || echo.
timeout /nobreak /t 5 >nul
docker start fcbackend

endlocal
```
Test if the bat file works by double clicking the file. Output without error messages should be produced in the Docker Desktop logs of the backend container.

Now we set up the Task Scheduler on Windows to restart the backend every day at 10 a.m. Open the Task Scheduler and create a new task. Give the task a name and select the option to run the task with the highest privileges whether user is logged on or not. Under Triggers select the option to run the task daily at 10 a.m. Under Actions select the bat file /bat/backend/backend.bat. Under Conditions select the option to wake the computer to run the task. Under Settings select the option to stop the existing instance if the task is already running when the task scheduler wants to start it. Click OK to save the task.

To test if the task scheduler works, you can run the task manually. You can also check the task history to see if the task was run successfully.

If you deploy for the first time, it is recommended to run hindcasts. This will produce the statistics on model efficiency and forecast errors displayed in the forecast dashboard. To run hindcasts, you will have to set the date in the file apps/internal_data/last_successful_run.txt to one calendar day before the date you want to start the hindcasts. The date must be in the format YYYY-MM-DD. We recommend starting the hindcasts with the date 2004-12-30. The hindcasts will take several hours to days to run. To speed up the process you can set write_excel in config/config_output.yaml to false. You can check the progress of the hindcasts by looking at the logs of the backend container in the Docker Desktop application.


### Forecast dashboard
Pull the docker image from Docker Hub. Open a terminal and run the following command:
```bash
docker pull mabesa/sapphire-dashboard:deploy
```
Run the image:
```bash
docker run -d --label=com.centurylinklabs.watchtower.enable=true -e "IN_DOCKER_CONTAINER=True" -v <full_path_to>/apps/config:/app/apps/config -v <full_path_to>/apps/internal_data:/app/apps/internal_data -v <full_path_to>/data:/app/data -p 5006:5006 --name fcbackend mabesa/sapphire-dashboard:deploy
```
Note that you will have to replace <full_path_to> with the full path to your local repository.

Test if the dashboard is operational by opening a browser window and typing http://localhost:5006 in the address bar. If the dashboard is displayed correctly, you can close the browser window.

Make sure that the image name is correct in the .bat file. Then create a shortcut to bat/dashboard.bat on your desktop. You can edit the icon of the shortcut by opening the preferences and selecting the icon available at bat/dashboard/Station.ico.

Note that the restart option for docker run does not work for the forecast dashboard. Docker services need to be up and running for the forecast dashboard to be able to mount all volumes correctly. The dashboard container is started upon double click on the shortcut. As the start-up of the container can take a few seconds, the dashboard may not be displayed correctly if the container is started before the docker services are up and running. In that case, you can reload the browser window or close the browser window and double click the shortcut again.



