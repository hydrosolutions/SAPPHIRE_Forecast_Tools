import luigi
import os
import subprocess
import time
from dotenv import load_dotenv

from . import docker_utils as dok

# To test the connection to the iEasyHydro server
from ieasyhydro_sdk.sdk import IEasyHydroSDK
from ieasyhydro_sdk.filters import BasicDataValueFilters
import datetime as dt
import pandas as pd


TAG = os.getenv('SAPPHIRE_FORECAST_TOOLS_DOCKER_TAG', 'latest')

class Environment:
    def __init__(self, dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)

    def get(self, key):
        return os.getenv(key)

# Initialize the Environment class with the path to your .env file
env = Environment('../sensitive_data_forecast_tools/config/.env_develop_kghm')

# We have a second environment file with the credentials to the iEH server
env_ieh = Environment('../sensitive_data_forecast_tools/config/.env_ieh')


class PreprocessingRunoff(luigi.Task):

    ssh_host = env.get('IEH_SSH_HOST')
    ssh_port = int(env.get('IEH_SSH_PORT'))
    ssh_user = env.get('IEH_SSH_USER')
    ssh_password = env.get('IEH_SSH_PASSWORD')
    local_port = int(env.get('IEH_SSH_LOCAL_PORT'))
    remote_host = env.get('IEH_SSH_DESTINATION_HOST')
    remote_port = int(env.get('IEH_SSH_DESTINATION_PORT'))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def output(self):

        # Get the path to the output file
        output_file_path = os.path.join(
            env.get("ieasyforecast_intermediate_data_path"),
            env.get("ieasyforecast_daily_discharge_file")
            )

        return luigi.LocalTarget(output_file_path)

    def run(self):
        # Connect to the remote server
        print(f"ssh_host: {self.ssh_host}")
        print(f"ssh_port: {self.ssh_port}")
        print(f"ssh_user: {self.ssh_user}")
        print(f"local_port: {self.local_port}")
        print(f"remote_port: {self.remote_port}")
        try:
            # Open an SSH tunnel
            command = f"ssh -N -L {self.local_port}:{self.remote_host}:{self.remote_port} {self.ssh_user}@{self.ssh_host} -p {self.ssh_port}"
            ssh_tunnel = subprocess.Popen(command, shell=True)

            print(f"SSH tunnel opened: localhost:{self.local_port} -> {self.ssh_host}:{self.remote_port}")
            print(f"Connected to {self.ssh_host}!")

            # Run the preprocessing runoff script
            #self.run_preprocessing_runoff()
            docker_client = dok.DockerClient()
            container = docker_client.run_container(
                'mabesa/sapphire-preprunoff:{TAG}',
                'sapphire-preprunoff',
                ['python', 'apps/preprocessing_runoff/preprocessing_runoff.py'],
                ({
                    'ports': {'8881/tcp': 8881},
                #    'volumes': [f"{env.get('ieasyforecast_configuration_path')}:/sensitive_data_forecast_tools/config",
                #                f"{env.get('ieasyforecast_daily_discharge_path')}:/sensitive_data_forecast_tools/daily_runoff",
                #                f"{env.get('ieasyforecast_intermediate_data_path')}:/sensitive_data_forecast_tools/intermediate_data"],
                #    'labels': {'com.centurylinklabs.watchtower.enable=true'},
                #    'environment': ['SAPPHIRE_OPDEV_ENV=True',
                #                    'PYTHONPATH=/app/apps/iEasyHydroForecast']
                })
                )

            for log in container.logs(stream=True):
                print(log.decode().strip())

            # Wait for the container to finish and then remove it
            container.wait()

        except Exception as e:
            print(f"Error opening SSH tunnel or running docker image: {e}")

        finally:
            # Close the SSH tunnel
            ssh_tunnel.terminate()

    def complete(self):
        if not self.output().exists():
            return False

        # Get the modified time of the output file
        output_file_mtime = os.path.getmtime(self.output().path)

        # Get the current time
        current_time = time.time()

        # Check if the output file was modified within the last number of seconds
        return current_time - output_file_mtime < 10  # 24 * 60 * 60




