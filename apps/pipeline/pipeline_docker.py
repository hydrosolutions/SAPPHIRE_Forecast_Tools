# Description: This file contains the luigi tasks to run the docker containers
#   for the forecast tools pipeline.
#
# Run: PYTHONPATH='.' luigi --module apps.pipeline.pipeline_docker PreprocessingRunoff --local-scheduler
#

import luigi
import os
import subprocess
import time
import docker
import re
import platform
from dotenv import load_dotenv

# Import local utils
from apps.pipeline.src import pipeline_utils as pu

# Import docker client for luigi, may not be required
#from . import docker_utils as dok
#from . import docker_runner as dokr



TAG = os.getenv('SAPPHIRE_FORECAST_TOOLS_DOCKER_TAG', 'latest')


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
        #print(" - Using ieasyforecast_data_root_dir: ", data_root_dir)
        # Strip the relative path from 2 "../" strings
        relative_path = re.sub(r'\.\./\.\./\.\.', '', relative_path)
        #print(" - Relative path: ", relative_path)
        #print(" - Absolute path: ", os.path.join(data_root_dir, relative_path))
        #print(" - Absolute path method 2: ", data_root_dir + relative_path)
        return data_root_dir + relative_path
    else:
        # Current working directory. Should be one above the root of the project
        cwd = os.getcwd()
        #print(" - Current working directory: ", cwd)
        # Strip the relative path from 2 "../" strings
        relative_path = re.sub(r'\.\./\.\./\.\.', '', relative_path)
        #print(" - Relative path: ", relative_path)
        return os.path.join(cwd, relative_path)

def get_bind_path(relative_path):
    # Strip the relative path from ../../.. to get the path to bind to the container
    relative_path = re.sub(r'\.\./\.\./\.\.', '', relative_path)
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
        absolute_volume_path_internal_data = get_absolute_path(
            env.get('ieasyforecast_intermediate_data_path'))
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

