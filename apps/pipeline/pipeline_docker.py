# Description: This file contains the luigi tasks to run the docker containers
#   for the forecast tools pipeline.
#
# Run: PYTHONPATH='.' luigi --module apps.pipeline.pipeline_docker RunWorkflow --local-scheduler
#

import luigi
import os
import time
import docker
import datetime
import re
from dotenv import load_dotenv

# Import local utils
from apps.pipeline.src import pipeline_utils as pu

# Import docker client for luigi, may not be required
#from . import docker_utils as dok
#from . import docker_runner as dokr



TAG = os.getenv('SAPPHIRE_FORECAST_TOOLS_DOCKER_TAG', 'latest')
SAPPHIRE_DG_HOST = os.getenv('SAPPHIRE_DG_HOST', 'localhost')


class Environment:
    def __init__(self, dotenv_path):
        print(f"Current working directory: {os.getcwd()}")
        print(f"Loading environment variables from {dotenv_path}")
        load_dotenv(dotenv_path=dotenv_path)

    def get(self, key):
        return os.getenv(key)

# Initialize the Environment class with the path to your .env file
env = Environment('../sensitive_data_forecast_tools/config/.env_develop_kghm')

# Function to convert a relative path to an absolute path
def get_absolute_path(relative_path):
    #print("In get_absolute_path: ")
    #print(" - Relative path: ", relative_path)

    # Test if there environment variable "ieasyforecast_data_root_dir" is set
    data_root_dir = os.getenv('ieasyhydroforecast_data_root_dir')
    if data_root_dir:
        # If it is set, use it as the root directory
        # Strip the relative path from 2 "../" strings
        relative_path = re.sub(r'\.\./\.\./\.\.', '', relative_path)

        return data_root_dir + relative_path

    else:
        # Current working directory. Should be one above the root of the project
        cwd = os.getcwd()
        # Strip the relative path from 2 "../" strings
        relative_path = re.sub(r'\.\./\.\./\.\.', '', relative_path)

        return os.path.join(cwd, relative_path)

def get_bind_path(relative_path):
    # Strip the relative path from ../../.. to get the path to bind to the container
    relative_path = re.sub(r'\.\./\.\./\.\.', '', relative_path)

    return relative_path

def get_local_path(relative_path):
    # Strip 2 ../ of the relative path
    relative_path = re.sub(f'\.\./\.\./', '', relative_path)

    return relative_path



class PreprocessingRunoff(luigi.Task):

    def output(self):

        # Get the path to the output file
        #print(f"cwd: {os.getcwd()}")
        #print(f"ieasyforecast_intermediate_data_path: {env.get('ieasyforecast_intermediate_data_path')}")
        #print(f"ieasyforecast_daily_discharge_file: {env.get('ieasyforecast_daily_discharge_file')}")
        output_file_path = os.path.join(
            env.get("ieasyforecast_intermediate_data_path"),
            env.get("ieasyforecast_daily_discharge_file")
            )
        #print(f"Output file path: {output_file_path}")

        return luigi.LocalTarget(output_file_path)

    def run(self):

        # Construct the absolute volume paths to bind to the containers
        absolute_volume_path_config = get_absolute_path(
            env.get('ieasyforecast_configuration_path'))
        absolute_volume_path_internal_data = get_absolute_path(
            env.get('ieasyforecast_intermediate_data_path'))
        absolute_volume_path_discharge = get_absolute_path(
            env.get('ieasyforecast_daily_discharge_path'))
        bind_volume_path_config = get_bind_path(
            env.get('ieasyforecast_configuration_path'))
        bind_volume_path_internal_data = get_bind_path(
            env.get('ieasyforecast_intermediate_data_path'))
        bind_volume_path_discharge = get_bind_path(
            env.get('ieasyforecast_daily_discharge_path'))

        #print(f"env.get('ieasyforecast_configuration_path'): {env.get('ieasyforecast_configuration_path')}")
        #print(f"absolute_volume_path_config: {absolute_volume_path_config}")
        #print(f"absolute_volume_path_internal_data: {absolute_volume_path_internal_data}")
        #print(f"absolute_volume_path_discharge: {absolute_volume_path_discharge}")
        #print(f"bind_volume_path_config: {bind_volume_path_config}")
        #print(f"bind_volume_path_internal_data: {bind_volume_path_internal_data}")
        #print(f"bind_volume_path_discharge: {bind_volume_path_discharge}")

        # Run the docker container to pre-process runoff data
        client = docker.from_env()

        # Pull the latest image
        if pu.there_is_a_newer_image_on_docker_hub(
            client, repository='mabesa', image_name='sapphire-preprunoff', tag=TAG):
            print("Pulling the latest image from Docker Hub.")
            client.images.pull('mabesa/sapphire-preprunoff', tag=TAG)

        # Define environment variables
        environment = [
            'SAPPHIRE_OPDEV_ENV=True',
        ]

        # Define volumes
        volumes = {
            absolute_volume_path_config: {'bind': bind_volume_path_config, 'mode': 'rw'},
            absolute_volume_path_internal_data: {'bind': bind_volume_path_internal_data, 'mode': 'rw'},
            absolute_volume_path_discharge: {'bind': bind_volume_path_discharge, 'mode': 'rw'}
        }

        # Run the container
        container = client.containers.run(
            f"mabesa/sapphire-preprunoff:{TAG}",
            detach=True,
            environment=environment,
            volumes=volumes,
            name="preprunoff",
            #labels=labels,
            network='host'  # To test
        )

        print(f"Container {container.id} is running.")

        # Wait for the container to finish running
        container.wait()

        print(f"Container {container.id} has stopped.")

    def complete(self):
        if not self.output().exists():
            return False

        # Get the modified time of the output file
        output_file_mtime = os.path.getmtime(self.output().path)

        # Get the current time
        current_time = time.time()

        # Check if the output file was modified within the last number of seconds
        return current_time - output_file_mtime < 10  # 24 * 60 * 60


class PreprocessingGatewayQuantileMapping(luigi.Task):

    def output(self):
        output_file_path = os.path.join(
            os.getenv('ieasyforecast_intermediate_data_path'),
            os.getenv('ieasyhydroforecast_OUTPUT_PATH_CM'))
        print("DEGUB: output_file_path: ", output_file_path)
        return luigi.LocalTarget(output_file_path)

    def run(self):

        # Construct the absolute volume paths to bind to the containers
        absolute_volume_path_config = get_absolute_path(
            env.get('ieasyforecast_configuration_path'))
        absolute_volume_path_internal_data = get_absolute_path(
            env.get('ieasyforecast_intermediate_data_path'))
        bind_volume_path_config = get_bind_path(
            env.get('ieasyforecast_configuration_path'))
        bind_volume_path_internal_data = get_bind_path(
            env.get('ieasyforecast_intermediate_data_path'))

        # Run the docker container to pre-process runoff data
        client = docker.from_env()

        # Pull the latest image
        if pu.there_is_a_newer_image_on_docker_hub(
            client, repository='mabesa', image_name='sapphire-prepgateway', tag=TAG):
            print("Pulling the latest image from Docker Hub.")
            client.images.pull('mabesa/sapphire-prepgateway', tag=TAG)

        # Define environment variables
        environment = [
            'SAPPHIRE_OPDEV_ENV=True',
            'SAPPHIRE_DG_HOST=' + SAPPHIRE_DG_HOST
        ]

        # Define volumes
        volumes = {
            absolute_volume_path_config: {'bind': bind_volume_path_config, 'mode': 'rw'},
            absolute_volume_path_internal_data: {'bind': bind_volume_path_internal_data, 'mode': 'rw'}
        }

        # Run the container
        container = client.containers.run(
            f"mabesa/sapphire-prepgateway:{TAG}",
            detach=True,
            environment=environment,
            volumes=volumes,
            name="prepgateway",
            #labels=labels,
            network='host'  # To test
        )

        print(f"Container {container.id} is running.")

        # Wait for the container to finish running
        result = container.wait()

        # Get the exit status code
        exit_status = result['StatusCode']
        print(f"Container {container.id} exited with status code {exit_status}.")

        # Get the logs from the container
        logs = container.logs().decode('utf-8')
        print(f"Logs from container {container.id}:\n{logs}")

        print(f"Container {container.id} has stopped.")

    def complete(self):
        if not self.output().exists():
            return False

        # Get the modified time of the output file
        output_file_mtime = os.path.getmtime(self.output().path)

        # Get the current time
        current_time = time.time()

        # Check if the output file was modified within the last number of seconds
        return current_time - output_file_mtime < 10


class LinearRegression(luigi.Task):

    def requires(self):
        return PreprocessingRunoff()

    def output(self):

        # Get the path to the output file
        #print(f"cwd: {os.getcwd()}")
        #print(f"ieasyforecast_intermediate_data_path: {env.get('ieasyforecast_intermediate_data_path')}")
        #print(f"ieasyforecast_analysis_pentad_file: {env.get('ieasyforecast_analysis_pentad_file')}")
        output_file_path = os.path.join(
            env.get("ieasyforecast_intermediate_data_path"),
            env.get("ieasyforecast_analysis_pentad_file")
            )
        #print(f"Output file path: {output_file_path}")

        return luigi.LocalTarget(output_file_path)

    def run(self):
        # Construct the absolute volume paths to bind to the containers
        absolute_volume_path_config = get_absolute_path(
            env.get('ieasyforecast_configuration_path'))
        absolute_volume_path_internal_data = get_absolute_path(
            env.get('ieasyforecast_intermediate_data_path'))
        absolute_volume_path_discharge = get_absolute_path(
            env.get('ieasyforecast_daily_discharge_path'))
        bind_volume_path_config = get_bind_path(
            env.get('ieasyforecast_configuration_path'))
        bind_volume_path_internal_data = get_bind_path(
            env.get('ieasyforecast_intermediate_data_path'))
        bind_volume_path_discharge = get_bind_path(
            env.get('ieasyforecast_daily_discharge_path'))

        #print(f"env.get('ieasyforecast_configuration_path'): {env.get('ieasyforecast_configuration_path')}")
        #print(f"absolute_volume_path_config: {absolute_volume_path_config}")
        #print(f"absolute_volume_path_internal_data: {absolute_volume_path_internal_data}")
        #print(f"absolute_volume_path_discharge: {absolute_volume_path_discharge}")
        #print(f"bind_volume_path_config: {bind_volume_path_config}")
        #print(f"bind_volume_path_internal_data: {bind_volume_path_internal_data}")
        #print(f"bind_volume_path_discharge: {bind_volume_path_discharge}")

        # Run the docker container to pre-process runoff data
        client = docker.from_env()

        # Pull the latest image
        if pu.there_is_a_newer_image_on_docker_hub(
            client, repository='mabesa', image_name='sapphire-linreg', tag=TAG):
            print("Pulling the latest image from Docker Hub.")
            client.images.pull('mabesa/sapphire-linreg', tag=TAG)

        # Define environment variables
        environment = [
            'SAPPHIRE_OPDEV_ENV=True',
        ]

        # Define volumes
        volumes = {
            absolute_volume_path_config: {'bind': bind_volume_path_config, 'mode': 'rw'},
            absolute_volume_path_internal_data: {'bind': bind_volume_path_internal_data, 'mode': 'rw'},
            absolute_volume_path_discharge: {'bind': bind_volume_path_discharge, 'mode': 'rw'}
        }

        # Run the container
        container = client.containers.run(
            f"mabesa/sapphire-linreg:{TAG}",
            detach=True,
            environment=environment,
            volumes=volumes,
            name="linreg",
            #labels=labels,
            network='host'  # To test
        )

        print(f"Container {container.id} is running.")

        # Wait for the container to finish running
        container.wait()

        print(f"Container {container.id} has stopped.")

    def complete(self):
        if not self.output().exists():
            return False

        # Get the modified time of the output file
        output_file_mtime = os.path.getmtime(self.output().path)

        # Get the current time
        current_time = time.time()

        # Check if the output file was modified within the last number of seconds
        return current_time - output_file_mtime < 10  # 24 * 60 * 60


class PostProcessingForecasts(luigi.Task):

    def requires(self):
        return LinearRegression()

    def output(self):

        # Get the path to the output file
        #print(f"cwd: {os.getcwd()}")
        #print(f"ieasyforecast_intermediate_data_path: {env.get('ieasyforecast_intermediate_data_path')}")
        #print(f"ieasyforecast_pentadal_skill_metrics_file: {env.get('ieasyforecast_pentadal_skill_metrics_file')}")
        output_file_path = os.path.join(
            env.get("ieasyforecast_intermediate_data_path"),
            env.get("ieasyforecast_pentadal_skill_metrics_file")
            )
        #print(f"Output file path: {output_file_path}")

        return luigi.LocalTarget(output_file_path)

    def run(self):
        # Construct the absolute volume paths to bind to the containers
        absolute_volume_path_config = get_absolute_path(
            env.get('ieasyforecast_configuration_path'))
        absolute_volume_path_internal_data = get_absolute_path(
            env.get('ieasyforecast_intermediate_data_path'))
        bind_volume_path_config = get_bind_path(
            env.get('ieasyforecast_configuration_path'))
        bind_volume_path_internal_data = get_bind_path(
            env.get('ieasyforecast_intermediate_data_path'))

        #print(f"absolute_volume_path_internal_data: {absolute_volume_path_internal_data}")
        #print(f"bind_volume_path_internal_data: {bind_volume_path_internal_data}")

        # Run the docker container to pre-process runoff data
        client = docker.from_env()

        # Pull the latest image
        if pu.there_is_a_newer_image_on_docker_hub(
            client, repository='mabesa', image_name='sapphire-postprocessing', tag=TAG):
            print("Pulling the latest image from Docker Hub.")
            client.images.pull('mabesa/sapphire-postprocessing', tag=TAG)

        # Define environment variables
        environment = [
            'SAPPHIRE_OPDEV_ENV=True',
        ]

        # Define volumes
        volumes = {
            absolute_volume_path_config: {'bind': bind_volume_path_config, 'mode': 'rw'},
            absolute_volume_path_internal_data: {'bind': bind_volume_path_internal_data, 'mode': 'rw'},
        }

        # Run the container
        container = client.containers.run(
            f"mabesa/sapphire-postprocessing:{TAG}",
            detach=True,
            environment=environment,
            volumes=volumes,
            name="postprocessing",
            #labels=labels,
            network='host'  # To test
        )

        print(f"Container {container.id} is running.")

        # Wait for the container to finish running
        container.wait()

        print(f"Container {container.id} has stopped.")

    def complete(self):
        if not self.output().exists():
            return False

        # Get the modified time of the output file
        output_file_mtime = os.path.getmtime(self.output().path)

        # Get the current time
        current_time = time.time()

        # Check if the output file was modified within the last number of seconds
        return current_time - output_file_mtime < 10  # 24 * 60 * 60

"""
class DeleteOldGateywayFiles(luigi.Task):

    # Define the folder path where the files are stored
    folder_path = get_local_path(os.path.join(
        env.get("ieasyforecast_intermediate_data_path"),
        env.get("ieasyhydroforecast_OUTPUT_PATH_DG")
    ))
    # Define the number of days old the files should be before they are deleted
    days_old = luigi.IntParameter(default=2)

    def run(self):
        print("Running DeleteOldGatewayFiles")
        print(self.folder_path)
        # Test if the path exists
        if not os.path.exists(self.folder_path):
            print(f"The path {self.folder_path} does not exist.")

        # Delete files older than `days_old`
        age_limit = datetime.datetime.now() - datetime.timedelta(days=self.days_old)
        for filename in os.listdir(self.folder_path):
            file_path = os.path.join(self.folder_path, filename)
            file_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
            if file_time < age_limit:
                os.remove(file_path)
                print(f"Deleted {file_path} as it was older than {self.days_old} days.")
"""

class RunWorkflow(luigi.Task):

    def requires(self):
        return [#PostProcessingForecasts(),
                PreprocessingRunoff(),
                PreprocessingGatewayQuantileMapping(),
                #DeleteOldGateywayFiles()
                ]

    def run(self):
        print("Workflow completed.")
        return None



if __name__ == '__main__':
    luigi.build([RunWorkflow()])