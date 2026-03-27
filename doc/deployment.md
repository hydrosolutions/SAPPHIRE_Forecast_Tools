<h1>Installation</h1>
This document describes the steps for the installation of the SAPPHIRE Forecast Tools. The forecast tools have been developed for installation on an Ubuntu server, OS version 24.04 LTS.

- [Prerequisites](#prerequisites)
  - [Skills required](#skills-required)
  - [Software requirements](#software-requirements)
  - [Server requirements](#server-requirements)
    - [Option A: Provisioning on AWS](#option-a-provisioning-on-aws)
    - [Option B: Organization-owned server](#option-b-organization-owned-server)
    - [Install software on the server](#install-software-on-the-server)
    - [Verify server readiness](#verify-server-readiness)
- [Step-by-step instructions](#step-by-step-instructions)
  - [Download this repository](#download-this-repository)
  - [General information for deployment](#general-information-for-deployment)
  - [Deployment of demo version on a local machine](#deployment-of-demo-version-on-a-local-machine)
  - [Deployment with private data on a server](#deployment-with-private-data-on-a-server)
    - [Configuring your server](#configuring-your-server)
      - [Set up the Luigi Daemon (Production)](#set-up-the-luigi-daemon-production)
      - [Set up SSH tunnel to iEasyHydro HF (if required)](#set-up-ssh-tunnel-to-ieasyhydro-hf-if-required)
    - [Copy your data to the repository](#copy-your-data-to-the-repository)
    - [Adapt the configuration files](#adapt-the-configuration-files)
    - [Deploy the forecast tools](#deploy-the-forecast-tools)
    - [Accessing the outputs](#accessing-the-outputs)
    - [Monitoring the forecast tools](#monitoring-the-forecast-tools)
  - [Set up cron job](#set-up-cron-job)
  - [Testing the deployment](#testing-the-deployment)



# Prerequisites

## Skills required

The deployment is done entirely from the command line over SSH. The person
performing the deployment should be comfortable with:

- **SSH**: connecting to a remote server, managing key pairs
- **Linux command line**: navigating directories, editing files (e.g., with
  `nano` or `vim`), reading logs, running scripts
- **Git**: cloning a repository, checking out a branch
- **Docker & Docker Compose**: starting/stopping containers, reading container
  logs (`docker logs`), understanding multi-service `docker-compose.yml` files
- **Basic networking**: understanding ports, checking if a service is listening
  (`curl`), configuring firewall rules or security groups

Optional, depending on your setup:

- **DNS management**: creating A records for dashboard subdomains (if using
  reverse proxy)
- **nginx**: configuring reverse proxy rules and SSL certificates (if exposing
  dashboards via HTTPS)
- **systemd**: creating and managing services (if setting up SSH tunnels or
  monitoring)

## Software requirements

### Required for all deployments

| Software | Purpose |
|----------|---------|
| [Docker Engine](https://docs.docker.com/engine/install/ubuntu/) (includes Compose v2) | Runs all services, pipeline, and dashboards |
| Git | Clone the repository |

### Required depending on your setup

| Software | When needed |
|----------|------------|
| autossh | SSH tunnel to iEasyHydro HF on a different network |
| nginx | Reverse proxy for dashboards and API behind HTTPS |
| [certbot](https://certbot.eff.org/) | Free SSL certificates from Let's Encrypt |

Installation commands are in the
[Install software on the server](#install-software-on-the-server) section.

## Server requirements

The forecast tools have been developed and tested on **Ubuntu 24.04 LTS**.
We recommend the same operating system. Other Linux distributions may work
but the instructions below assume Ubuntu.

**Minimum hardware:**

| Resource | Small deployment | Large deployment |
|----------|-----------------|------------------|
| RAM | 8 GB | 16 GB |
| CPU cores | 4 | 4 |
| Storage | 20 GB | 64 GB |

Storage requirements depend on the number of stations, models enabled, and
how much historical data is migrated. A deployment with ~20 stations and
linear regression only needs ~20 GB. Deployments with 100+ stations, ML
models, and full hindcast history can reach 50–64 GB. When in doubt, start
with 30 GB and expand later.

Choose the option that matches your infrastructure, then continue to
[Verify server readiness](#verify-server-readiness) before proceeding.

### Option A: Provisioning on AWS

Follow the official AWS guide to launch an EC2 instance:
[Launch an instance using the new launch instance wizard](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EC2_GetStarted.html)

Use these SAPPHIRE-specific settings during the launch wizard:

| Parameter | Recommended value | Notes |
|-----------|-------------------|-------|
| AMI | Ubuntu Server 24.04 LTS (HVM, SSD) | Search "ubuntu 24.04" in the AMI catalog |
| Instance type | `t3.xlarge` (4 vCPU, 16 GB) | `t3.large` (2 vCPU, 8 GB) sufficient without ML models |
| Key pair | Create or select an ED25519 key pair | Store the `.pem` file securely; this is your only SSH access |
| Storage | 30 GB gp3 (adjust per table above) | EBS volumes can be resized later without downtime |
| Security group | Create new, configure below | Name it e.g. `sapphire-sg` |

#### AWS security group

AWS security groups act as a firewall: inbound traffic is blocked by default,
and only ports with an explicit rule are reachable. This means you do **not**
need deny rules for the database or API ports — simply don't add them.

For the mechanics of creating and editing security groups, see the AWS
documentation:
[Control traffic to your AWS resources using security groups](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-security-groups.html)

**Inbound rules to add:**

| Type | Port | Source | Purpose |
|------|------|--------|---------|
| SSH | 22 | Your IP or office CIDR | Admin access. **Never** use `0.0.0.0/0` |
| HTTP | 80 | `0.0.0.0/0` | Redirect to HTTPS / Let's Encrypt validation |
| HTTPS | 443 | `0.0.0.0/0` | Dashboards and (optionally) API via reverse proxy |
| Custom TCP | 5006 | `0.0.0.0/0` | Pentad dashboard (skip if behind reverse proxy) |
| Custom TCP | 5007 | `0.0.0.0/0` | Decad dashboard (skip if behind reverse proxy) |
| Custom TCP | 8082 | Your IP or office CIDR | Luigi web UI (restrict to admins) |

**Outbound rules:** Leave the default (allow all outbound). The pipeline needs
to reach Docker Hub, external APIs (iEasyHydro HF, SAPPHIRE Data Gateway),
and SMTP servers for monitoring alerts.

### Option B: Organization-owned server

Most hydromet services deploy on their own infrastructure (physical server or
VM managed by the organization's IT department). The hardware and port
requirements are the same as for AWS — only the provisioning steps differ.

#### Request from your IT team

Before you begin, ask your IT department to prepare a server with:

| Requirement | Details |
|-------------|---------|
| Operating system | Ubuntu 24.04 LTS (fresh install preferred) |
| Hardware | See [minimum hardware table](#server-requirements) above |
| SSH access | Key-based SSH access with `sudo` privileges |
| Outbound internet | The server must be able to reach Docker Hub, GitHub, and external APIs |
| Static IP or hostname | Needed if dashboards will be accessed from outside the local network |

#### Firewall configuration with `ufw`

Ubuntu's built-in firewall (`ufw`) is the equivalent of AWS security groups.
If your organization uses a different firewall (e.g., `iptables`, a hardware
firewall), apply the same port rules through that tool.

```bash
# Enable ufw (if not already active)
sudo ufw enable

# Allow SSH (always do this first to avoid locking yourself out)
sudo ufw allow from <your-ip-or-cidr> to any port 22

# Allow HTTP and HTTPS
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Allow dashboards (skip if using reverse proxy on port 443)
sudo ufw allow 5006/tcp
sudo ufw allow 5007/tcp

# Allow Luigi web UI (restrict to admin IPs)
sudo ufw allow from <your-ip-or-cidr> to any port 8082
```

**Verify:**
```bash
sudo ufw status verbose
```
Expected output: rules for ports 22, 80, 443, 5006, 5007, 8082. No rules for
8000–8005 or 5433–5436 — these stay localhost-only by default.

> **Important:** Do not open ports 8000–8005 (API services) or 5433–5436
> (databases). These are accessed only from localhost by the pipeline and
> dashboards. Exposing database ports is a security risk.

### Install software on the server

SSH into your server and install the required software.

**Docker Engine** — follow the full instructions for Ubuntu:
[Docker Engine installation on Ubuntu](https://docs.docker.com/engine/install/ubuntu/)

**All other tools** in one step:
```bash
sudo apt-get update
sudo apt-get install -y git autossh curl
```

Install `nginx` and `certbot` only if you plan to set up a reverse proxy
for the dashboards:
```bash
sudo apt-get install -y nginx
sudo snap install --classic certbot
```

### Verify server readiness

After completing the steps above, run the checks below. All must pass before
proceeding to the [step-by-step instructions](#step-by-step-instructions).

```bash
# 1. Operating system
lsb_release -ds
# Expected: Ubuntu 24.04.x LTS
```

```bash
# 2. Hardware resources
echo "RAM: $(free -h | awk '/Mem:/ {print $2}')"
echo "CPUs: $(nproc)"
echo "Disk: $(df -h / | awk 'NR==2 {print $4}') free"
# Expected: RAM ≥8 GB, CPUs ≥4, Disk ≥20 GB free
```

```bash
# 3. Docker and Compose
docker --version
docker compose version
# Expected: Docker 24.x or newer, Compose v2.x.x
```

```bash
# 4. Outbound connectivity
curl -s --max-time 5 https://registry-1.docker.io/v2/ && echo "Docker Hub: OK"
curl -s --max-time 5 https://github.com > /dev/null && echo "GitHub: OK"
# Expected: both print OK
```

```bash
# 5. Ports are free (no existing services on our ports)
ss -tlnp | grep -E ':(5006|5007|8000|8002|8003|8004|8005|8082)\b' || echo "All ports free: OK"
# Expected: "All ports free: OK"
```

If any check fails, fix the issue before continuing. Common problems:

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `docker: command not found` | Docker not installed | Follow [Docker install instructions](https://docs.docker.com/engine/install/ubuntu/) |
| `docker compose version` prints v1.x | Old Docker Compose standalone | Remove it (`sudo apt remove docker-compose`) and install Docker Engine which includes Compose v2 |
| `permission denied` on docker commands | User not in `docker` group | `sudo usermod -aG docker $USER` then log out and back in |
| Docker Hub connectivity fails | Outbound traffic blocked | Ask IT to allow HTTPS to `registry-1.docker.io` and `production.cloudflare.docker.com` |
| A port is already in use | Another service is listening | Identify it with `ss -tlnp | grep :<port>` and stop or reconfigure it |

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
You may have to open specific ports on your server. The table below lists all ports used by the SAPPHIRE Forecast Tools, grouped by function. Pay attention to the "Expose to internet?" column — database ports must never be exposed, and API ports should stay on localhost unless you explicitly need external access.

| Port | Service | Group | Expose to internet? |
|------|---------|-------|---------------------|
| 22 | SSH | Infrastructure | Yes (key-based only) |
| 80 | HTTP | Infrastructure | Yes |
| 443 | HTTPS | Infrastructure | Yes |
| 81 | Nginx Proxy Manager (optional) | Infrastructure | Optional |
| 8082 | Luigi task monitor | Pipeline | Optional (behind auth) |
| 5006 | Pentad forecast dashboard | Dashboards | Yes (via reverse proxy) |
| 5007 | Decad forecast dashboard | Dashboards | Yes (via reverse proxy) |
| 3647 | Configuration dashboard (optional) | Dashboards | Optional |
| 8000 | API Gateway | SAPPHIRE Services | Localhost only* |
| 8002 | Preprocessing API | SAPPHIRE Services | Localhost only |
| 8003 | Postprocessing API | SAPPHIRE Services | Localhost only |
| 8004 | User API | SAPPHIRE Services | Localhost only |
| 8005 | Auth API | SAPPHIRE Services | Localhost only |
| 5433 | Preprocessing DB (PostgreSQL) | SAPPHIRE Services | **Never** |
| 5434 | Postprocessing DB (PostgreSQL) | SAPPHIRE Services | **Never** |
| 5435 | User DB (PostgreSQL) | SAPPHIRE Services | **Never** |
| 5436 | Auth DB (PostgreSQL) | SAPPHIRE Services | **Never** |

\* API Gateway may be exposed via reverse proxy if external API access is needed.

> **Note:** The SAPPHIRE services (API stack + databases) must be running before
> the pipeline. See `sapphire/README.md` for setup instructions.

#### Set up the Luigi Daemon (Production)
For a production environment, the Luigi scheduler daemon (`luigid`) must be running persistently to manage the pipeline tasks. Since the entire application stack runs in Docker, the recommended approach is to run the daemon as a persistent Docker container.

1.  **Start the Luigi Daemon Container:**
    Navigate to your `SAPPHIRE_forecast_tools` project root and use Docker Compose to start the daemon in the background. The `restart: unless-stopped` policy in the compose file ensures it will start on boot and restart if it fails.
    ```bash
    # Make sure you are in the SAPPHIRE_forecast_tools directory
    docker compose -f bin/docker-compose-luigi.yml up -d luigi-daemon
    ```

2.  **Verify the Service:**
    Check that the container is running correctly.
    ```bash
    docker ps | grep luigi-daemon
    ```
    You should see the `sapphire-luigi-daemon` container with a status of `Up`. You can access the Luigi web interface at `http://<your-server-ip>:8082`.

#### Set up SSH tunnel to iEasyHydro HF (if required)

If the SAPPHIRE forecast tools need to access an iEasyHydro High Frequency (HF) database that is not directly reachable from the server (e.g., on a different network or behind a firewall), you must set up a persistent SSH tunnel using `autossh` and `systemd`.

**When is this needed?**
- The iEasyHydro HF server is on a different network than the SAPPHIRE server
- The iEasyHydro HF API listens only on `localhost` on its host machine (common security configuration)

**When is this NOT needed?**
- The iEasyHydro HF API is directly reachable from the SAPPHIRE server (same network, API bound to network interface)
- You are using the iEasyHydro HF cloud version (configure the cloud endpoint in `.env` instead)

**Setup summary:**
1. Install `autossh` on the SAPPHIRE server
2. Generate a dedicated SSH key pair
3. Install the public key on the iEasyHydro HF server (coordinate with the remote IT team)
4. Create a `systemd` service that maintains the tunnel automatically (auto-start on boot, auto-reconnect on drop)
5. Configure your `.env` to point at `localhost:<tunnel-port>`

For the full step-by-step instructions with all commands, see the [Update Deployment Checklist — Section 1.2, Option B or C](prod/update_deployment_checklist.md#12-ieasyhydro-hf-connectivity-if-applicable).

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
Once the Luigi daemon is running as a system service, you can schedule the individual pipeline steps using `cron`. This approach decouples the tasks, allowing them to run at different times based on data availability. Luigi's scheduler will automatically handle dependencies between tasks.

To edit the cron jobs, type the following command to the console:
```bash
crontab -e
```

**Important: Log rotation**

The crontab below uses timestamped log files (e.g., `sapphire_gateway_20260116.log`) to prevent logs from growing indefinitely. A cleanup job deletes logs older than 7 days. Make sure the log directory exists:
```bash
mkdir -p /home/ubuntu/logs
```

Add the following to the crontab file. Adjust times for your timezone (example below uses UTC, with jobs running in the morning Bishkek time):
```bash
# m h  dom mon dow   command
# ---------------------------------------------------------------------------
# SAPPHIRE Forecast Tools Schedule (Times in UTC)
# ---------------------------------------------------------------------------
# NOTE: The Luigi daemon (luigid) must be running as a service for these tasks.
#
# Log cleanup: delete logs older than 7 days (runs daily at 02:00 UTC)
0 2 * * * find /home/ubuntu/logs -name "sapphire_*.log" -mtime +7 -delete
#
# (1) Gateway Preprocessing at 03:00 UTC (09:00 Bishkek). Independent of daily data.
0 3 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_preprocessing_gateway.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_gateway_$(date +\%Y\%m\%d).log 2>&1
#
# (2) Pentadal Forecast at 04:00 UTC (10:00 Bishkek). Luigi triggers runoff preprocessing.
0 4 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_pentadal_forecasts.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_pentadal_$(date +\%Y\%m\%d).log 2>&1
#
# (3) Decadal Forecast at 05:00 UTC (11:00 Bishkek). Luigi uses completed runoff task.
0 5 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_decadal_forecasts.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_decadal_$(date +\%Y\%m\%d).log 2>&1
#
# (4) Daily maintenance via Luigi (replaces individual maintenance cron jobs)
# Luigi enforces dependency order: PrepRunoff + Gateway → LinReg → ML → PostProcessing → Frontend
# ML concurrency is limited to 3 via Luigi resources.
0 19 * * * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_daily_maintenance.sh /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_maintenance_$(date +\%Y\%m\%d).log 2>&1
#
# (5) Periodic maintenance tasks (bimonthly/yearly)
# Bimonthly long-term postprocessing (1st of odd months)
0 22 1 1,3,5,7,9,11 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh long_term /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_periodic_longterm_$(date +\%Y\%m\%d).log 2>&1
# Yearly skill recalculation (January 1)
0 1 1 1 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh skill_recalc /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_periodic_skillrecalc_$(date +\%Y\%m\%d).log 2>&1
# Yearly snow norm recalculation (August 25)
0 2 25 8 * cd /data/SAPPHIRE_Forecast_Tools && bash bin/run_periodic_maintenance.sh snow_norms /data/kyg_data_forecast_tools/config/.env_develop_kghm >> /home/ubuntu/logs/sapphire_periodic_snownorms_$(date +\%Y\%m\%d).log 2>&1
```
To check if the cron jobs have been set up correctly, you can list them with `crontab -l`.


## Testing the deployment
After correct deployment, forecast bulletins should now be produced automatically one day before the beginning of each pentad. We recommend the following strategy to test if the deployment has been successful:
1. Check the logs of the backend container in the Docker Desktop application. If there are no error messages displayed at the bottom of the log tab, the backend is running correctly.
2. Check if the forecast bulletins are produced correctly. You can do this by checking the folder data/reports (if you have not reconfigured the output directory for the bulletins).
3. Run hindcasts for the period 2004-12-30 to the present date. This will produce the statistics on model efficiency and forecast errors displayed in the forecast dashboard. To run hindcasts, you will have to set the date in the file apps/internal_data/last_successful_run.txt to one calendar day before the date you want to start the hindcasts. The date must be in the format YYYY-MM-DD. We recommend starting the hindcasts with the date 2004-12-30. The hindcasts will take several hours to days to run. To speed up the process you can set write_excel in config/config_output.yaml to false. You can check the progress of the hindcasts by looking at the logs of the backend container in the Docker Desktop application. Note that we recommend producing bulletins for the pervious years forecasts that can be cross-examined with your forecasts from the previous year. This is an important step.
4. Check if the forecast dashboard is operational by doubble-clicking the dashboard icon. If the dashboard is displayed correctly and the displayed data makes sense, you can close the browser window.
5. Check if the configuration dashboard is operational by double-clicking the icon of the configuration dashboard. Test if the selection of stations has an effect on the results produced by the forecast tools by manually trigggering a re-run of the latest forecast and checking if the changes have an effect on the forecast bulletins and the forecast dashboard. Note that the station selection may still be limited by the apps/config/config_development_restrict_station_selection.yaml.

