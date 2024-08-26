# Description: This file contains the luigi tasks to run the docker containers
#   for the forecast tools pipeline. For different organizations, we define
#   different workflows. The workflows are defined in the RunWorkflow class.
#   Available organizations are:
#   - demo (default, publicly available data, linear regression only)
#   - kghm (private data, linear regression, machine learning and conceptual
#           hydrological model)
#   The organization is definded in the .env file.
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



class Environment:
    def __init__(self, dotenv_path):
        print(f"Current working directory: {os.getcwd()}")
        print(f"Loading environment variables from {dotenv_path}")
        load_dotenv(dotenv_path=dotenv_path)

    def get(self, key, default=None):
        return os.getenv(key, default)

# Initialize the Environment class with the path to your .env file
env_file_path = os.getenv('ieasyhydroforecast_env_file_path')
env = Environment(env_file_path)
# Get the tag of the docker image to use
TAG = env.get('ieasyhydroforecast_backend_docker_image_tag')
# Get the organization for which to run the forecast tools
ORGANIZATION = env.get('ieasyhydroforecast_organization')
# URL of the sapphire data gateway
SAPPHIRE_DG_HOST = env.get('SAPPHIRE_DG_HOST')




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
        return luigi.LocalTarget(f'/app/log_preprunoff.txt')

    def run(self):
        print("------------------------------------")
        print(" Running PreprocessingRunoff task.")
        print("------------------------------------")
        # Construct the absolute volume paths to bind to the containers
        # TODO: Insted of constructing the paths here, we define relative paths
        # to data ref dir in the .env file and use them here.
        # Use also ieasyhydroforecast_data_ref_dir (absolute pathe) and
        # ieasyhydroforecast_container_data_ref_dir (bind path)
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

        print(f"env.get('ieasyforecast_configuration_path'): {env.get('ieasyforecast_configuration_path')}")
        print(f"absolute_volume_path_config: {absolute_volume_path_config}")
        print(f"absolute_volume_path_internal_data: {absolute_volume_path_internal_data}")
        print(f"absolute_volume_path_discharge: {absolute_volume_path_discharge}")
        print(f"bind_volume_path_config: {bind_volume_path_config}")
        print(f"bind_volume_path_internal_data: {bind_volume_path_internal_data}")
        print(f"bind_volume_path_discharge: {bind_volume_path_discharge}")

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

        # Create the output marker file
        with self.output().open('w') as f:
            f.write('Task completed')

    '''
    def complete(self):
        if not self.output().exists():
            return False

        # Get the modified time of the output file
        output_file_mtime = os.path.getmtime(self.output().path)

        # Get the current time
        current_time = time.time()

        # Check if the output file was modified within the last number of seconds
        return current_time - output_file_mtime < 10  # 24 * 60 * 60
    '''

class PreprocessingGatewayQuantileMapping(luigi.Task):

    def output(self):
         # Define a unique output file for this task
        return luigi.LocalTarget(f'/app/log_prepgateway.txt')

    def run(self):
        print("------------------------------------")
        print(" Running PreprocessingGateway task.")
        print("------------------------------------")

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

        # Create the output marker file
        with self.output().open('w') as f:
            f.write('Task completed')

class LinearRegression(luigi.Task):

    def requires(self):
        return PreprocessingRunoff()

    def output(self):
        return luigi.LocalTarget(f'/app/log_linreg.txt')

    def run(self):
        print("------------------------------------")
        print(" Running LinearRegression task.")
        print("------------------------------------")

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

        # Write the output marker file
        with self.output().open('w') as f:
            f.write('Task completed')


class ConceptualModel(luigi.Task):

    def requires(self):
        return [PreprocessingRunoff(), PreprocessingGatewayQuantileMapping()]

    def output(self):
        return luigi.LocalTarget(f'/app/log_conceptmod.txt')

    def run(self):
        print("------------------------------------")
        print(" Running ConceptualModel task.")
        print("------------------------------------")

        # Construct the absolute volume paths to bind to the containers
        absolute_volume_path_config = get_absolute_path(
            env.get('ieasyforecast_configuration_path'))
        absolute_volume_path_internal_data = get_absolute_path(
            env.get('ieasyforecast_intermediate_data_path'))
        absolute_volume_path_conceptmod = get_absolute_path(
            env.get('ieasyhydroforecast_conceptual_model_path'))
        bind_volume_path_config = get_bind_path(
            env.get('ieasyforecast_configuration_path'))
        bind_volume_path_internal_data = get_bind_path(
            env.get('ieasyforecast_intermediate_data_path'))
        bind_volume_path_conceptmod = get_bind_path(
            env.get('ieasyhydroforecast_conceptual_model_path'))

        # Run the docker container to pre-process runoff data
        client = docker.from_env()

        # Pull the latest image
        if pu.there_is_a_newer_image_on_docker_hub(
            client, repository='mabesa', image_name='sapphire-conceptmod', tag=TAG):
            print("Pulling the latest image from Docker Hub.")
            client.images.pull('mabesa/sapphire-conceptmod', tag=TAG)

        # Define environment variables
        environment = [
            'SAPPHIRE_OPDEV_ENV=True',
            'IN_DOCKER_CONTAINER=True'
        ]

        # Define volumes
        volumes = {
            absolute_volume_path_config: {'bind': bind_volume_path_config, 'mode': 'rw'},
            absolute_volume_path_internal_data: {'bind': bind_volume_path_internal_data, 'mode': 'rw'},
            absolute_volume_path_conceptmod: {'bind': bind_volume_path_conceptmod, 'mode': 'rw'}
        }

        # Run the container
        container = client.containers.run(
            f"mabesa/sapphire-conceptmod:{TAG}",
            detach=True,
            environment=environment,
            volumes=volumes,
            name="conceptmod",
            #labels=labels,
            network='host'  # To test
        )

        print(f"Container {container.id} is running.")

        # Wait for the container to finish running
        container.wait()

        print(f"Container {container.id} has stopped.")

        # Write the output marker file
        with self.output().open('w') as f:
            f.write('Task completed')


class RunMLModel(luigi.Task):
    model_type = luigi.Parameter()
    prediction_mode = luigi.Parameter()

    def requires(self):
        return [PreprocessingRunoff(), PreprocessingGatewayQuantileMapping()]

    def output(self):
        return luigi.LocalTarget(f'/app/log_ml_{self.model_type}_{self.prediction_mode}.txt')

    def run(self):
        print("------------------------------------")
        print(" Running MachineLearning task.")
        print("------------------------------------")

        # Construct the absolute volume paths to bind to the containers
        absolute_volume_path_config = get_absolute_path(
            env.get('ieasyforecast_configuration_path'))
        absolute_volume_path_internal_data = get_absolute_path(
            env.get('ieasyforecast_intermediate_data_path'))
        bind_volume_path_config = get_bind_path(
            env.get('ieasyforecast_configuration_path'))
        bind_volume_path_internal_data = get_bind_path(
            env.get('ieasyforecast_intermediate_data_path'))

        #print(f"env.get('ieasyforecast_configuration_path'): {env.get('ieasyforecast_configuration_path')}")
        #print(f"absolute_volume_path_config: {absolute_volume_path_config}")
        #print(f"absolute_volume_path_internal_data: {absolute_volume_path_internal_data}")
        #print(f"bind_volume_path_config: {bind_volume_path_config}")
        #print(f"bind_volume_path_internal_data: {bind_volume_path_internal_data}")

        # Run the docker container to forecast using machine learning
        client = docker.from_env()

        # Pull the latest image
        if pu.there_is_a_newer_image_on_docker_hub(
            client, repository='mabesa', image_name='sapphire-ml', tag=TAG):
            print("Pulling the latest image from Docker Hub.")
            client.images.pull('mabesa/sapphire-ml', tag=TAG)

        # Define environment variables
        environment = [
            'SAPPHIRE_OPDEV_ENV=True',
            'IN_DOCKER=True',
            f'SAPPHIRE_MODEL_TO_USE={self.model_type}',  # TFT, TIDE, TSMIXER, ARIMA
            f'SAPPHIRE_PREDICTION_MODE={self.prediction_mode}'  # PENTAD, DECAD
        ]
        print(f"Environment variables:\n{environment}")

        # Define volumes
        volumes = {
            absolute_volume_path_config: {'bind': bind_volume_path_config, 'mode': 'rw'},
            absolute_volume_path_internal_data: {'bind': bind_volume_path_internal_data, 'mode': 'rw'}
        }
        print(f"Volumes:\n{volumes}")


        # Run the container
        container = client.containers.run(
            f"mabesa/sapphire-ml:{TAG}",
            detach=True,
            environment=environment,
            volumes=volumes,
            name=f"ml_{self.model_type}_{self.prediction_mode}",
            #labels=labels,
            network='host'  # To test
        )

        print(f"Container {container.id} is running.")

        # Wait for the container to finish running
        container.wait()

        # Get the logs from the container
        logs = container.logs().decode('utf-8')
        print(f"Logs from container {container.id}:\n{logs}")

        print(f"Container {container.id} has stopped.")

        # Write the output marker file
        with self.output().open('w') as f:
            f.write('Task completed')

class RunAllMLModels(luigi.WrapperTask):
    def requires(self):
        # Ensure preprocessing tasks are completed first
        yield PreprocessingRunoff()
        yield PreprocessingGatewayQuantileMapping()

        models = ['TFT', 'TIDE', 'TSMIXER', 'ARIMA']
        prediction_modes = ['PENTAD', 'DECAD']

        for model in models:
            for mode in prediction_modes:
                yield RunMLModel(model_type=model, prediction_mode=mode)

class PostProcessingForecasts(luigi.Task):

    def requires(self):
        return LinearRegression()

    def output(self):
        return luigi.LocalTarget(f'/app/log_postproc.txt')

    def run(self):
        print("------------------------------------")
        print(" Running PostprocessingForecasts task.")
        print("------------------------------------")

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

        # Write the output marker file
        with self.output().open('w') as f:
            f.write('Task completed')

    '''
    def complete(self):
        if not self.output().exists():
            return False

        # Get the modified time of the output file
        output_file_mtime = os.path.getmtime(self.output().path)

        # Get the current time
        current_time = time.time()

        # Check if the output file was modified within the last number of seconds
        return current_time - output_file_mtime < 10  # 24 * 60 * 60
    '''

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
        if ORGANIZATION=='demo':
            print("Running demo workflow.")
            return [PostProcessingForecasts()]

        elif ORGANIZATION=='kghm':
            print("Running KGHM workflow.")
            return [PostProcessingForecasts(),
                    RunAllMLModels(),
                    #DeleteOldGateywayFiles()
                    ]

        # You can add workflow definitions for other organizations here.

    def run(self):
        print("Workflow completed.")
        return None


if __name__ == '__main__':
    luigi.build([RunWorkflow()])