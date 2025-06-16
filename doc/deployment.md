<h1>Installation</h1>
This document describes the steps for the installation of the SAPPHIRE Forecast Tools. The forecast tools have been developed for installation on an Ubuntu server, OS version 24.4 LTS.

- [Prerequisites](#prerequisites)
  - [Server requirements](#server-requirements)
  - [Skills required](#skills-required)
  - [Software requirements](#software-requirements)
- [Step-by-step instructions](#step-by-step-instructions)
  - [Download this repository](#download-this-repository)
  - [General information for deployment](#general-information-for-deployment)
  - [Deployment of demo version on a local machine](#deployment-of-demo-version-on-a-local-machine)
  - [Deployment with private data on a server](#deployment-with-private-data-on-a-server)
    - [Configuring your server](#configuring-your-server)
    - [Copy your data to the repository](#copy-your-data-to-the-repository)
    - [Adapt the configuration files](#adapt-the-configuration-files)
    - [Deploy the forecast tools](#deploy-the-forecast-tools)
    - [Accessing the outputs](#accessing-the-outputs)
    - [Monitoring the forecast tools](#monitoring-the-forecast-tools)
  - [Set up cron job](#set-up-cron-job)
  - [Testing the deployment](#testing-the-deployment)



# Prerequisites

## Server requirements
The forecast tools have been developed and tested on **Ubuntu 20.04 LTS**. We therefore recommend you use the same operating system. The forecast tools can be deployed on other operating systems, but the installation instructions may differ from the instructions in this document. The forecast tools have been developed to run on a server. We recommend that the server has at least **8 GB of RAM** and 4 CPU cores. The server should further have at least **12 GB of free storage**. If you plan to integrate large forecasting models your storage requirements may be higher.

## Skills required
Although we try to provide very detailed step-by-step instructions some basic skills are recommended: The user should have basic knowledge of the **command line interface (CLI)** and should be able to run commands in the CLI. The user should have basic knowledge of **Git** and should be able to clone a repository from GitHub. We further recommend that the user has basic knowledge of **Docker** and is able to run Docker containers.

## Software requirements
The following software is required to deploy the forecast tools:
- Docker Engine
- Git (optional)


Whereby Docker Engine are required for the installation of the forecast tools. Git is used to clone the GitHub repository with the folder structure and example files.

Please follow the **detailed installation instructions for Docker Engine on Ubuntu** in the [Docker documentation](https://docs.docker.com/engine/install/ubuntu/).

**Git** can be installed from the command line as follows:
```bash
sudo apt-get update
sudo apt-get install git
```

Perform the following steps the computer where the forecast tools are deployed.

# Step-by-step instructions
## Download this repository
Download the [repository](https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools) to the host machine. This will give you the folder structure with which you can quickly deploy the forecast tools. You can, however, also build your own folder structure. If you choose to do so, you will have to adapt the paths in the .env file and the run commands accordingly.
<details>
<summary>Manual download</summary>
The repository can be downloaded as a zip file from the GitHub website. Unzip the file and move the folder to the desired location on the host machine. This allows you to only perform minimal edits to the configuration files within the designed folder structure.
</details>

<details>
<summary>Instructions using Git</summary>
Alternatively you can clone the repository using git. On the server open a terminal and type the following commands:

```bash
git clone https://github.com/hydrosolutions/SAPPHIRE_Forecast_Tools.git
```
</details>

## General information for deployment
We provide a script the should take care of most deployment steps for you and run the forecast tools for the first time.
The script is located in the bin folder and run as follows from the SAPPHIRE_Forecast_Tools folder:
```bash
ieasyhydroforecast_url=<base url> nohup bash .bin/deploy_sapphire_forecast_tools.sh <env_file_path> > deployment.log 2>&1 &
```
where the env_file_path is the absolute path to your .env file. This command will log all output of the command to a file deployment.log in your folder SAPPHIRE_Forecast_Tools. You can view the progress of the deployment script by looking at the log file, for example with `less deployment.log`, and by checking the progress of the individual containers with `docker ps -a` and `docker logs <container_name>`.

Please note that the deployment script assumes that the pentad forecast dashboard will be deployed at fc.pentad.<base url> and the decad dashboard will be deployed at fc.decad.<base url>. 


## Deployment of demo version on a local machine
The demo version comes with public example data as well as with the configuration files that are set up to work with the example data. The demo version is a good starting point to get to know the forecast tools and to test the basic functionality of the tools. Please note that only linear regression models are  currently available in the demo version.

The sections below describe the steps that are required to deploy the forecast tools on a host machine (tested on ubuntu server). If you want to test the tools with the demo data, you don't need to adapt the files in the apps/config folder and skip the .env chapter below.

TODO: Detailed instructions

## Deployment with private data on a server
The full power of the forecast tools can of course only be unleashed by deploying the tools with your own operational data and with your own hydrological models. You can further boost your forecasting by integrating the forecast tools with iEasyHydro High Frequency (an open-source software for operational hydrology) and the SAPPHIRE Data Gateway (an open source software for processing publicly awailable weather forecasts and results from the TopoPyScale snow model, also open-source). However, this is a major undertaking since quite some software installation and configuration needs to be done. We assume here that you have iEasyHydro or iEasyHydro High Frequency, SAPPHIRE Data Gateway and TopoPyScale installed and that access credentials for these tools are available (e.g. for iEH HF you'll need access credentials for a regular user, we recommend creating one specifically for the forecast tools). The following steps are required to deploy the forecast tools with your own data:

- Configuring your server
- Copy your data to the server
- Adapt the configuration files

### Configuring your server
You may have to open specific ports on your server to allow you to view the dashboards in a browser. The following ports are typically used for the SAPPHIRE Forecast Tools:
- 22 for ssh
- 80 for http
- 81 for nginx proxy manager (optional)
- 443 for https
- 3647 for the configuration dashboard (optional)
- 5006 for the forecast dashboard for displaying pentadal forecasts
- 5007 for the forecast dashboard for displaying decadal forecasts
- 8082 for the luigi task monitor (optional)

For setting up the Luigi daemon (which runs on port 8082), please refer to the [Luigi daemon setup documentation](luigi_daemon/ubuntu_setup.md) for production environments or the [Luigi daemon development instructions](luigi_daemon/dev_instructions.md) for development environments.

### Copy your data to the repository
We recommend that you follow the folder structure of the repository. Please review the example data in the data folder to understand the folder structure and the data formats. You can copy your data to the data folder in the SAPPHIRE_Forecast_Tools folder or to any other location on your server.

To copy individual files from your local machine to the server you can use the scp command. The following command copies a file from your local machine to the server:
```bash
scp /path/to/local/file username@hostname:/path/to/remote/file
```

To copy an entire folder from your local machine to the server you can use the -r option. The following command copies a folder from your local machine to the server:
```bash
scp -r /path/to/local/folder username@hostname:/path/to/remote/folder
```

Note that you might have to authenticate yourself with a password. You can also use a key pair for authentication. Please refer to the scp documentation for more information.

### Adapt the configuration files
To be described.

### Deploy the forecast tools
We provide you with a shell script that pulls the latest images from Docker Hub and runs the containers. The script is located in the bin folder and run as follows from the SAPPHIRE_Forecast_Tools folder:
```bash
ieasyhydroforecast_url=<base url> bash ./bin/deploy_sapphire_forecast_tools.sh <absolute_path_to_data_directory>/config/.env_develop_kghm
```
The path to the data root folder is the parent directory of your data folder where you store your discharge, bulletin templates and other data.
For deployment with sensitive data, we recommend a separate data folder which is located at the same hierarchical level as the SAPPHIRE_Forecast_Tools folder. In this case the path to the data root folder would be:
```bash
ieasyhydroforecast_url=<base url> bash .bin/run_sapphire_forecast_tools.sh /absolute/path/to/parent/directory/of/SAPPHIRE_Forecast_Tools
```

Please note that the deployment script assumes that the pentad forecast dashboard will be deployed at fc.pentad.<base url> and the decad dashboard will be deployed at fc.decad.<base url>. You will have to configure your proxy manager and domain manager to forward port 5006 to fc.pentad.<base url> and port 5007 to fc.decad.<base url>. 

For convenience sake you may want to run the forecast tools in the background and redirect the output to a log file. You can do this by running the following command:
```bash
nohup bash .bin/run_sapphire_forecast_tools.sh /absolute/path/to/SAPPHIRE_Forecast_Tools > logfile.log 2>&1 &
```
This will run the forecast tools in the background and redirect the output to a log file called logfile.log. The log file will be stored in the SAPPHIRE_Forecast_Tools folder.

### Accessing the outputs
You should now be able to view the configuration dashboard in your browser under <your servers url> and the forecast dashboard under <your servers url>:5006/forecast_dashboard for pentadal or <your servers url>:5007/forecast_dashboard for decadal forecasts. You can trigger the writing of the forecast bulletins from the forecast dashboard.

### Monitoring the forecast tools
You can check the progress of the forecast tools by looking at the log file with one of the following command:
```bash
less logfile.log
```
to view the entire files (exit less mode by typing q and then enter) or
```bash
tail -f logfile.log
```
to view the last lines of the file.

You can further monitor individual containers by running the following command:
```bash
docker ps -a
```
This will list all containers that are currently running. You can check the logs of the containers with:
```bash
docker logs <container_name>
```

You can also check the progress of the luigi tasks by opening a browser window and typing <your servers url>:8082 in the address bar. This will open the luigi task monitor and show you the status of the individual modules that are run by the forecast tools.

For comprehensive monitoring of the SAPPHIRE Forecast Tools, including automated alerts and systemd services for monitoring Docker containers and logs, please refer to the [detailed monitoring documentation](monitoring/forecast_tools_monitoring.md).


## Set up cron job
We recommend setting up a cron job to restart the backend every day after you have checked the operational river discharge data and before you have to send out the forecast bulletin.

To edit the cron jobs, type the following command to the console:
```bash
crontab -e
```
Add the following line to the crontab file (skip the comment line, this is just for your information):
```bash
# m h  dom mon dow   command
3 5 * * * cd /data/SAPPHIRE_Forecast_Tools && /bin/bash -c 'bash bin/run_sapphire_forecast_tools.sh /path/to/.env' >> /data/logs/daily_run_of_sapphire_forecast_tools.log 2>&1
2 20 * * * cd /data/SAPPHIRE_Forecast_Tools && /bin/bash -c 'bash bin/daily_update_sapphire_frontend.sh /path/to/.env' >> /data/logs/restart_dashboards.log 2>&1
0 10 * * * cd /data/SAPPHIRE_Forecast_Tools && /bin/bash -c 'bash bin/daily_ml_maintenance.sh /path/to/.env' >> /data/logs/ml_maintenance.log 2>&1
```
To run the forecast tools every day at 5:03 a.m (server time, check by typing `date` to the console). The log file will be stored in the logs folder in the data folder.

To check if the cron job has been set up correctly, you can list the cron jobs with the following command:
```bash
crontab -l
```


## Testing the deployment
After correct deployment, forecast bulletins should now be produced automatically one day before the beginning of each pentad. We recommend the following strategy to test if the deployment has been successful:
1. Check the logs of the backend container in the Docker Desktop application. If there are no error messages displayed at the bottom of the log tab, the backend is running correctly.
2. Check if the forecast bulletins are produced correctly. You can do this by checking the folder data/reports (if you have not reconfigured the output directory for the bulletins).
3. Run hindcasts for the period 2004-12-30 to the present date. This will produce the statistics on model efficiency and forecast errors displayed in the forecast dashboard. To run hindcasts, you will have to set the date in the file apps/internal_data/last_successful_run.txt to one calendar day before the date you want to start the hindcasts. The date must be in the format YYYY-MM-DD. We recommend starting the hindcasts with the date 2004-12-30. The hindcasts will take several hours to days to run. To speed up the process you can set write_excel in config/config_output.yaml to false. You can check the progress of the hindcasts by looking at the logs of the backend container in the Docker Desktop application. Note that we recommend producing bulletins for the pervious years forecasts that can be cross-examined with your forecasts from the previous year. This is an important step.
4. Check if the forecast dashboard is operational by doubble-clicking the dashboard icon. If the dashboard is displayed correctly and the displayed data makes sense, you can close the browser window.
5. Check if the configuration dashboard is operational by double-clicking the icon of the configuration dashboard. Test if the selection of stations has an effect on the results produced by the forecast tools by manually trigggering a re-run of the latest forecast and checking if the changes have an effect on the forecast bulletins and the forecast dashboard. Note that the station selection may still be limited by the apps/config/config_development_restrict_station_selection.yaml.

