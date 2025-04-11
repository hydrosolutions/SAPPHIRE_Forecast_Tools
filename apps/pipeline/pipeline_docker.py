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
import glob
import time
import docker
import datetime
import re
from dotenv import load_dotenv
from typing import Optional, Tuple
import signal
from contextlib import contextmanager
import threading

# Import local utils
from apps.pipeline.src import pipeline_utils as pu
from apps.pipeline.src.environment import Environment
from apps.pipeline.src.notification_manager import NotificationManager
from apps.pipeline.src.timeout_manager import get_task_parameters


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



class PreprocessingRunoff(pu.TimeoutMixin, luigi.Task):
    # Set timeout to 15 minutes (900 seconds)
    timeout_seconds = luigi.IntParameter(default=None)
    max_retries = luigi.IntParameter(default=None)
    retry_delay = luigi.IntParameter(default=None)

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_preprunoff_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params['timeout_seconds']

        if self.max_retries is None:
            self.max_retries = task_params['max_retries']

        if self.retry_delay is None:
            self.retry_delay = task_params['retry_delay']

    def output(self):
        # Test if docker_logs is available and create it if its are not.
        if not os.path.exists(f'self.intermediate_data_path/docker_logs'):
            os.makedirs(f'self.intermediate_data_path/docker_logs')
        return luigi.LocalTarget(f'/app/log_preprunoff.txt')

    def _run_container(self, attempt_number) -> tuple[Optional[str], int, str]:
        """
        Run the docker container and return container ID, exit status, and logs
        """
        client = docker.from_env()

        try:
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

            print(f"env.get('ieasyforecast_configuration_path'): {env.get('ieasyforecast_configuration_path')}")
            print(f"absolute_volume_path_config: {absolute_volume_path_config}")
            print(f"absolute_volume_path_internal_data: {absolute_volume_path_internal_data}")
            print(f"absolute_volume_path_discharge: {absolute_volume_path_discharge}")
            print(f"bind_volume_path_config: {bind_volume_path_config}")
            print(f"bind_volume_path_internal_data: {bind_volume_path_internal_data}")
            print(f"bind_volume_path_discharge: {bind_volume_path_discharge}")

            # Pull the latest image if needed
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

            # Run the container with unique name for each attempt
            container = client.containers.run(
                f"mabesa/sapphire-preprunoff:{TAG}",
                detach=True,
                environment=environment,
                volumes=volumes,
                name=f"preprunoff_attempt_{attempt_number}_{time.time()}",  # Unique name per attempt
                network='host'
            )

            print(f"Container {container.id} is running.")

            # Wait for container with timeout
            try:
                self.run_with_timeout(container.wait)
                exit_status = 0
            except TimeoutError:
                print(f"Container {container.id} timed out after {self.timeout_seconds} seconds")
                container.stop()
                exit_status = 124
            logs = container.logs().decode('utf-8')

            print(f"Container {container.id} exited with status code {exit_status}")
            print(f"Logs from container {container.id}:\n{logs}")

            # Clean up container
            try:
                container.remove()
            except Exception as e:
                print(f"Warning: Could not remove container {container.id}: {str(e)}")

            return container.id, exit_status, logs

        except Exception as e:
            print(f"Error running container: {str(e)}")
            if 'container' in locals() and container:
                try:
                    container.stop()
                    container.remove()
                except:
                    pass
            return None, 1, str(e)

    def run(self):
        logger = pu.TaskLogger()
        start_time = datetime.datetime.now()

        print("------------------------------------")
        print(" Running PreprocessingRunoff task.")
        print("------------------------------------")

        attempts = 0
        final_status = "Failed"
        details = ""

        try:
            while attempts < self.max_retries:
                attempts += 1
                print(f"Attempt {attempts} of {self.max_retries}")

                container_id, exit_status, logs = self._run_container(attempts)

                if exit_status == 0:
                    # Success - write output and exit
                    with open(self.docker_logs_file_path, 'w') as f:
                        f.write('Task completed successfully\n')
                        f.write(f'Container ID: {container_id}\n')
                        # log timeout configuration to log file
                        f.write(f'Timeout: {self.timeout_seconds}\n')
                        f.write(f'Max retries: {self.max_retries}\n')
                        f.write(f'Logs:\n{logs}')
                    final_status = "Success"
                    details = f"Completed on attempt {attempts}"

                    # Create the output marker file
                    with self.output().open('w') as f:
                        f.write('Task completed')

                    break

                if exit_status == 124:  # Timeout
                    final_status = "Timeout"
                    details = f"Task timed out after {self.timeout_seconds} seconds"
                    break

                if attempts < self.max_retries:
                    print(f"Container failed with status {exit_status}. Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    print(f"Container failed after {self.max_retries} attempts.")
                    raise RuntimeError(f"Task failed after {self.max_retries} attempts. Last exit status: {exit_status}\nLogs:\n{logs}")

        finally:
            end_time = datetime.datetime.now()
            logger.log_task_timing(
                task_name="PreprocessingRunoff",
                start_time=start_time,
                end_time=end_time,
                status=final_status,
                details=details
            )


class PreprocessingGatewayQuantileMapping(pu.TimeoutMixin, luigi.Task):
    # Set timeout to 30 minutes (1800 seconds)
    timeout_seconds = luigi.IntParameter(default=None)
    max_retries = luigi.IntParameter(default=None)
    retry_delay = luigi.IntParameter(default=None)

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_pregateway_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    # Note: docker_logs get cleaned up after n days in the pipeline_logs directory

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params['timeout_seconds']

        if self.max_retries is None:
            self.max_retries = task_params['max_retries']

        if self.retry_delay is None:
            self.retry_delay = task_params['retry_delay']

    def output(self):
        # The output file is written in the docker container, so it
        # automatically disappears after the container is removed and does not
        # have to be cleaned up.
        # Test if docker_logs directory is available and create them if they are not.
        if not os.path.exists(f'self.intermediate_data_path/docker_logs'):
            os.makedirs(f'self.intermediate_data_path/docker_logs')

        return luigi.LocalTarget(f'/app/log_pregateway.txt')

    def _run_container(self, attempt_number) -> tuple[Optional[str], int, str]:
        """
        Run the docker container and return container ID, exit status, and logs
        """
        client = docker.from_env()

        try:
            # Construct the absolute volume paths to bind to the containers
            absolute_volume_path_config = get_absolute_path(
                env.get('ieasyforecast_configuration_path'))
            absolute_volume_path_internal_data = get_absolute_path(
                env.get('ieasyforecast_intermediate_data_path'))
            bind_volume_path_config = get_bind_path(
                env.get('ieasyforecast_configuration_path'))
            bind_volume_path_internal_data = get_bind_path(
                env.get('ieasyforecast_intermediate_data_path'))

            # Pull the latest image if needed
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

            # Run the container with unique name for each attempt
            container = client.containers.run(
                f"mabesa/sapphire-prepgateway:{TAG}",
                detach=True,
                environment=environment,
                volumes=volumes,
                name=f"prepgateway_attempt_{attempt_number}_{time.time()}",  # Unique name per attempt
                network='host'
            )

            print(f"Container {container.id} is running.")

            # Wait for container with timeout
            try:
                self.run_with_timeout(container.wait)
                exit_status = 0
            except TimeoutError:
                print(f"Container {container.id} timed out after {self.timeout_seconds} seconds")
                container.stop()
                exit_status = 124
            logs = container.logs().decode('utf-8')

            print(f"Container {container.id} exited with status code {exit_status}")
            print(f"Logs from container {container.id}:\n{logs}")

            # Clean up container
            try:
                container.remove()
            except Exception as e:
                print(f"Warning: Could not remove container {container.id}: {str(e)}")

            return container.id, exit_status, logs

        except Exception as e:
            print(f"Error running container: {str(e)}")
            if container:
                try:
                    container.stop()
                    container.remove()
                except:
                    pass
            return None, 1, str(e)

    def run(self):

        logger = pu.TaskLogger()
        start_time = datetime.datetime.now()

        print("------------------------------------")
        print(" Running PreprocessingGateway task.")
        print("------------------------------------")

        attempts = 0
        final_status = "Failed"
        details = ""

        try:
            while attempts < self.max_retries:
                attempts += 1
                print(f"Attempt {attempts} of {self.max_retries}")

                container_id, exit_status, logs = self._run_container(attempts)

                if exit_status == 0:
                    # Success - write output and exit
                    with open(self.docker_logs_file_path, 'w') as f:
                        f.write('Task completed successfully\n')
                        f.write(f'Container ID: {container_id}\n')
                        f.write(f'Timeout: {self.timeout_seconds}\n')
                        f.write(f'Max retries: {self.max_retries}\n')
                        f.write(f'Logs:\n{logs}')
                    final_status = "Success"
                    details = f"Completed on attempt {attempts}"

                    # Create the output marker file
                    with self.output().open('w') as f:
                        f.write('Task completed')

                    break

                if exit_status == 124:  # Timeout
                    final_status = "Timeout"
                    details = f"Task timed out after {self.timeout_seconds} seconds"
                    break

                if attempts < self.max_retries:
                    print(f"Container failed with status {exit_status}. Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    print(f"Container failed after {self.max_retries} attempts.")
                    raise RuntimeError(f"Task failed after {self.max_retries} attempts. Last exit status: {exit_status}\nLogs:\n{logs}")

        finally:
            end_time = datetime.datetime.now()
            logger.log_task_timing(
                task_name="PreprocessingGatewayQuantileMapping",
                start_time=start_time,
                end_time=end_time,
                status=final_status,
                details=details
            )


class LinearRegression(pu.TimeoutMixin, luigi.Task):
    # Set timeout to 10 minutes (600 seconds)
    timeout_seconds = luigi.IntParameter(default=None)
    max_retries = luigi.IntParameter(default=None)
    retry_delay = luigi.IntParameter(default=None)

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_linreg_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params['timeout_seconds']

        if self.max_retries is None:
            self.max_retries = task_params['max_retries']

        if self.retry_delay is None:
            self.retry_delay = task_params['retry_delay']

    def requires(self):
        return PreprocessingRunoff()

    def output(self):
        return luigi.LocalTarget(f'/app/log_linreg.txt')

    def _run_container(self, attempt_number) -> tuple[Optional[str], int, str]:
        """
        Run the docker container and return container ID, exit status, and logs
        """
        client = docker.from_env()

        try:
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

            # Pull the latest image if needed
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

            # Run the container with unique name for each attempt
            container = client.containers.run(
                f"mabesa/sapphire-linreg:{TAG}",
                detach=True,
                environment=environment,
                volumes=volumes,
                name=f"linreg_attempt_{attempt_number}_{time.time()}",  # Unique name per attempt
                network='host'
            )

            print(f"Container {container.id} is running.")

            # Wait for container with timeout
            try:
                self.run_with_timeout(container.wait)
                exit_status = 0
            except TimeoutError:
                print(f"Container {container.id} timed out after {self.timeout_seconds} seconds")
                container.stop()
                exit_status = 124
            logs = container.logs().decode('utf-8')

            print(f"Container {container.id} exited with status code {exit_status}")
            print(f"Logs from container {container.id}:\n{logs}")

            # Clean up container
            try:
                container.remove()
            except Exception as e:
                print(f"Warning: Could not remove container {container.id}: {str(e)}")

            return container.id, exit_status, logs

        except Exception as e:
            print(f"Error running container: {str(e)}")
            if 'container' in locals() and container:
                try:
                    container.stop()
                    container.remove()
                except:
                    pass
            return None, 1, str(e)

    def run(self):
        logger = pu.TaskLogger()
        start_time = datetime.datetime.now()

        print("------------------------------------")
        print(" Running LinearRegression task.")
        print("------------------------------------")

        attempts = 0
        final_status = "Failed"
        details = ""

        try:
            while attempts < self.max_retries:
                attempts += 1
                print(f"Attempt {attempts} of {self.max_retries}")

                container_id, exit_status, logs = self._run_container(attempts)

                if exit_status == 0:
                    # Success - write output and exit
                    with open(self.docker_logs_file_path, 'w') as f:
                        f.write('Task completed successfully\n')
                        f.write(f'Container ID: {container_id}\n')
                        f.write(f'Timeout: {self.timeout_seconds}\n')
                        f.write(f'Max retries: {self.max_retries}\n')
                        f.write(f'Logs:\n{logs}')
                    final_status = "Success"
                    details = f"Completed on attempt {attempts}"

                    # Create the output marker file
                    with self.output().open('w') as f:
                        f.write('Task completed')

                    break

                if exit_status == 124:  # Timeout
                    final_status = "Timeout"
                    details = f"Task timed out after {self.timeout_seconds} seconds"
                    break

                if attempts < self.max_retries:
                    print(f"Container failed with status {exit_status}. Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    print(f"Container failed after {self.max_retries} attempts.")
                    raise RuntimeError(f"Task failed after {self.max_retries} attempts. Last exit status: {exit_status}\nLogs:\n{logs}")

        finally:
            end_time = datetime.datetime.now()
            logger.log_task_timing(
                task_name="LinearRegression",
                start_time=start_time,
                end_time=end_time,
                status=final_status,
                details=details
            )


class ConceptualModel(pu.TimeoutMixin, luigi.Task):
    # Set timeout to 30 minutes (1800 seconds)
    timeout_seconds = luigi.IntParameter(default=None)
    max_retries = luigi.IntParameter(default=None)
    retry_delay = luigi.IntParameter(default=None)

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_conceptmod_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params['timeout_seconds']

        if self.max_retries is None:
            self.max_retries = task_params['max_retries']

        if self.retry_delay is None:
            self.retry_delay = task_params['retry_delay']

    def requires(self):
        return [PreprocessingRunoff(), PreprocessingGatewayQuantileMapping()]

    def output(self):
        return luigi.LocalTarget(f'/app/log_conceptmod.txt')

    def _run_container(self, attempt_number) -> tuple[Optional[str], int, str]:
        """
        Run the docker container and return container ID, exit status, and logs
        """
        client = docker.from_env()
        container = None

        try:
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

            # Pull the latest image if needed
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

            # Run the container with unique name for each attempt
            container = client.containers.run(
                f"mabesa/sapphire-conceptmod:{TAG}",
                detach=True,
                environment=environment,
                volumes=volumes,
                name=f"conceptmod_attempt_{attempt_number}_{time.time()}",  # Unique name per attempt
                network='host'
            )

            print(f"Container {container.id} is running.")

            # Wait for container with timeout
            try:
                self.run_with_timeout(container.wait)
                exit_status = 0
            except TimeoutError:
                print(f"Container {container.id} timed out after {self.timeout_seconds} seconds")
                container.stop()
                exit_status = 124
            logs = container.logs().decode('utf-8')

            print(f"Container {container.id} exited with status code {exit_status}")
            print(f"Logs from container {container.id}:\n{logs}")

            # Clean up container
            try:
                container.remove()
            except Exception as e:
                print(f"Warning: Could not remove container {container.id}: {str(e)}")

            return container.id, exit_status, logs

        except Exception as e:
            print(f"Error running container: {str(e)}")
            if container:
                try:
                    container.stop()
                    container.remove()
                except:
                    pass
            return None, 1, str(e)

    def run(self):
        logger = pu.TaskLogger()
        start_time = datetime.datetime.now()

        print("------------------------------------")
        print(" Running ConceptualModel task.")
        print("------------------------------------")

        attempts = 0
        final_status = "Failed"
        details = ""

        try:
            while attempts < self.max_retries:
                attempts += 1
                print(f"Attempt {attempts} of {self.max_retries}")

                container_id, exit_status, logs = self._run_container(attempts)

                if exit_status == 0:
                    # Success - write output and exit
                    with open(self.docker_logs_file_path, 'w') as f:
                        f.write('Task completed successfully\n')
                        f.write(f'Container ID: {container_id}\n')
                        f.write(f'Timeout: {self.timeout_seconds}\n')
                        f.write(f'Max retries: {self.max_retries}\n')
                        f.write(f'Logs:\n{logs}')
                    final_status = "Success"
                    details = f"Completed on attempt {attempts}"

                    # Create the output marker file
                    with self.output().open('w') as f:
                        f.write('Task completed')

                    break

                if exit_status == 124:  # Timeout
                    final_status = "Timeout"
                    details = f"Task timed out after {self.timeout_seconds} seconds"
                    break

                if attempts < self.max_retries:
                    print(f"Container failed with status {exit_status}. Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    print(f"Container failed after {self.max_retries} attempts.")
                    raise RuntimeError(f"Task failed after {self.max_retries} attempts. Last exit status: {exit_status}\nLogs:\n{logs}")

        finally:
            end_time = datetime.datetime.now()
            logger.log_task_timing(
                task_name="ConceptualModel",
                start_time=start_time,
                end_time=end_time,
                status=final_status,
                details=details
            )


class RunMLModel(pu.TimeoutMixin, luigi.Task):
    model_type = luigi.Parameter()
    prediction_mode = luigi.Parameter()
    run_mode = luigi.Parameter(default='forecast')

    # Set timeout to 8 minutes (480 seconds)
    timeout_seconds = luigi.IntParameter(default=None)
    max_retries = luigi.IntParameter(default=None)
    retry_delay = luigi.IntParameter(default=None)

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params['timeout_seconds']

        if self.max_retries is None:
            self.max_retries = task_params['max_retries']

        if self.retry_delay is None:
            self.retry_delay = task_params['retry_delay']

    def requires(self):
        return [PreprocessingRunoff(), PreprocessingGatewayQuantileMapping()]

    def output(self):
        return luigi.LocalTarget(f'/app/log_ml_{self.model_type}_{self.prediction_mode}.txt')

    def run(self):

        # Define the logging output of the task.
        docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_ml_{self.model_type}_{self.prediction_mode}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        logger = pu.TaskLogger()
        start_time = datetime.datetime.now()
        container = None
        final_status = "Failed"
        details = ""

        try:
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
                f'SAPPHIRE_PREDICTION_MODE={self.prediction_mode}',  # PENTAD, DECAD
                f'RUN_MODE={self.run_mode}'  # only run make_forecast.py in operational mode
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

            try:
                self.run_with_timeout(container.wait)
                logs = container.logs().decode('utf-8')

                with open(docker_logs_file_path, 'w') as f:
                    f.write('Task completed\n')
                    f.write(f'Container ID: {container.id}\n')
                    f.write(f'Timeout: {self.timeout_seconds}\n')
                    f.write(f'Max retries: {self.max_retries}\n')
                    f.write(f'Logs:\n{logs}')

                final_status = "Success"
                details = "Task completed successfully"

                # Create the output marker file
                with self.output().open('w') as f:
                    f.write('Task completed')

            except TimeoutError:
                container.stop()
                final_status = "Timeout"
                details = f"Task timed out after {self.timeout_seconds} seconds"
                raise

        except Exception as e:
            details = str(e)
            raise

        finally:
            end_time = datetime.datetime.now()
            logger.log_task_timing(
                task_name=f"RunMLModel_{self.model_type}_{self.prediction_mode}",
                start_time=start_time,
                end_time=end_time,
                status=final_status,
                details=details
            )

            if container:
                try:
                    container.remove()
                except:
                    pass


class RunAllMLModels(luigi.WrapperTask):
    def requires(self):
        # Ensure preprocessing tasks are completed first
        yield PreprocessingRunoff()
        yield PreprocessingGatewayQuantileMapping()

        models = ['TFT', 'TIDE', 'TSMIXER', 'ARIMA']
        prediction_modes = ['PENTAD', 'DECAD']

        for model in models:
            for mode in prediction_modes:
                yield RunMLModel(model_type=model, prediction_mode=mode, run_mode='forecast')


class PostProcessingForecasts(pu.TimeoutMixin, luigi.Task):
    # Set timeout to 15 minutes (900 seconds)
    timeout_seconds = luigi.IntParameter(default=None)
    max_retries = luigi.IntParameter(default=None)
    retry_delay = luigi.IntParameter(default=None)

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_postproc_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params['timeout_seconds']

        if self.max_retries is None:
            self.max_retries = task_params['max_retries']

        if self.retry_delay is None:
            self.retry_delay = task_params['retry_delay']

    def requires(self):
        if ORGANIZATION=='demo':
            return LinearRegression()
        if ORGANIZATION=='kghm':
            return [ConceptualModel(), RunAllMLModels(), LinearRegression()]

    def output(self):
        return luigi.LocalTarget(f'/app/log_postproc.txt')

    def _run_container(self, attempt_number) -> tuple[Optional[str], int, str]:
        """
        Run the docker container and return container ID, exit status, and logs
        """
        client = docker.from_env()

        try:
            # Construct the absolute volume paths to bind to the containers
            absolute_volume_path_config = get_absolute_path(
                env.get('ieasyforecast_configuration_path'))
            absolute_volume_path_internal_data = get_absolute_path(
                env.get('ieasyforecast_intermediate_data_path'))
            bind_volume_path_config = get_bind_path(
                env.get('ieasyforecast_configuration_path'))
            bind_volume_path_internal_data = get_bind_path(
                env.get('ieasyforecast_intermediate_data_path'))

            # Pull the latest image if needed
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

            # Run the container with unique name for each attempt
            container = client.containers.run(
                f"mabesa/sapphire-postprocessing:{TAG}",
                detach=True,
                environment=environment,
                volumes=volumes,
                name=f"postprocessing_attempt_{attempt_number}_{time.time()}",  # Unique name per attempt
                network='host'
            )

            print(f"Container {container.id} is running.")

            # Wait for container with timeout
            try:
                self.run_with_timeout(container.wait)
                exit_status = 0
            except TimeoutError:
                print(f"Container {container.id} timed out after {self.timeout_seconds} seconds")
                container.stop()
                exit_status = 124
            logs = container.logs().decode('utf-8')

            print(f"Container {container.id} exited with status code {exit_status}")
            print(f"Logs from container {container.id}:\n{logs}")

            # Clean up container
            try:
                container.remove()
            except Exception as e:
                print(f"Warning: Could not remove container {container.id}: {str(e)}")

            return container.id, exit_status, logs

        except Exception as e:
            print(f"Error running container: {str(e)}")
            if 'container' in locals() and container:
                try:
                    container.stop()
                    container.remove()
                except:
                    pass
            return None, 1, str(e)

    def run(self):
        logger = pu.TaskLogger()
        start_time = datetime.datetime.now()

        print("------------------------------------")
        print(" Running PostprocessingForecasts task.")
        print("------------------------------------")

        attempts = 0
        final_status = "Failed"
        details = ""

        try:
            while attempts < self.max_retries:
                attempts += 1
                print(f"Attempt {attempts} of {self.max_retries}")

                container_id, exit_status, logs = self._run_container(attempts)

                if exit_status == 0:
                    # Success - write output and exit
                    with open(self.docker_logs_file_path, 'w') as f:
                        f.write('Task completed successfully\n')
                        f.write(f'Container ID: {container_id}\n')
                        f.write(f'Timeout: {self.timeout_seconds}\n')
                        f.write(f'Max retries: {self.max_retries}\n')
                        f.write(f'Logs:\n{logs}')
                    final_status = "Success"
                    details = f"Completed on attempt {attempts}"

                    # Create the output marker file
                    with self.output().open('w') as f:
                        f.write('Task completed')

                    break

                if exit_status == 124:  # Timeout
                    final_status = "Timeout"
                    details = f"Task timed out after {self.timeout_seconds} seconds"
                    break

                if attempts < self.max_retries:
                    print(f"Container failed with status {exit_status}. Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    print(f"Container failed after {self.max_retries} attempts.")
                    raise RuntimeError(f"Task failed after {self.max_retries} attempts. Last exit status: {exit_status}\nLogs:\n{logs}")

        finally:
            end_time = datetime.datetime.now()
            logger.log_task_timing(
                task_name="PostProcessingForecasts",
                start_time=start_time,
                end_time=end_time,
                status=final_status,
                details=details
            )


class DeleteOldGatewayFiles(pu.TimeoutMixin, luigi.Task):
    # Fix the typo in the class name (was "Gateywayy")

    # Define the folder path where the files are stored
    folder_path = get_local_path(os.path.join(
        env.get("ieasyforecast_intermediate_data_path"),
        env.get("ieasyhydroforecast_OUTPUT_PATH_DG")
    ))
    # Define the number of days old the files should be before they are deleted
    days_old = luigi.IntParameter(default=2)

    # Set timeout to 5 minutes (300 seconds) - should be plenty for a file deletion task
    timeout_seconds = luigi.IntParameter(default=None)
    max_retries = luigi.IntParameter(default=None)
    retry_delay = luigi.IntParameter(default=None)

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_deleteOldGatewayFiles_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params['timeout_seconds']

        if self.max_retries is None:
            self.max_retries = task_params['max_retries']

        if self.retry_delay is None:
            self.retry_delay = task_params['retry_delay']

    def output(self):
        return luigi.LocalTarget(f'/app/log_deleteoldfiles.txt')

    def _delete_old_files(self) -> tuple[int, list[str], list[str]]:
        """
        Delete files older than days_old and return count of deleted files,
        list of deleted files, and any errors encountered
        """
        deleted_files = []
        errors = []
        deleted_count = 0

        try:
            # Test if the path exists
            if not os.path.exists(self.folder_path):
                errors.append(f"The path {self.folder_path} does not exist.")
                return 0, deleted_files, errors

            # Delete files older than `days_old`
            age_limit = datetime.datetime.now() - datetime.timedelta(days=self.days_old)

            for filename in os.listdir(self.folder_path):
                try:
                    file_path = os.path.join(self.folder_path, filename)

                    # Skip directories
                    if os.path.isdir(file_path):
                        continue

                    file_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                    if file_time < age_limit:
                        os.remove(file_path)
                        deleted_files.append(file_path)
                        deleted_count += 1
                        print(f"Deleted {file_path} as it was older than {self.days_old} days.")
                except Exception as e:
                    error_msg = f"Error processing file {filename}: {str(e)}"
                    errors.append(error_msg)
                    print(error_msg)

            return deleted_count, deleted_files, errors

        except Exception as e:
            error_msg = f"Error in delete_old_files: {str(e)}"
            errors.append(error_msg)
            print(error_msg)
            return deleted_count, deleted_files, errors

    def run(self):
        logger = pu.TaskLogger()
        start_time = datetime.datetime.now()

        print("------------------------------------")
        print(" Running DeleteOldGatewayFiles task.")
        print("------------------------------------")
        print(f"Looking for files older than {self.days_old} days in: {self.folder_path}")

        final_status = "Failed"
        details = ""

        try:
            # Run with timeout protection
            try:
                # Using a lambda here to call our method with self's timeout
                self.run_with_timeout(lambda: self._delete_old_files())
                deleted_count, deleted_files, errors = self._delete_old_files()

                # Format results for the log file
                result_details = [
                    f"Found and deleted {deleted_count} files older than {self.days_old} days.",
                ]

                if deleted_count > 0:
                    result_details.append("\nDeleted files:")
                    for file_path in deleted_files:
                        result_details.append(f"- {file_path}")

                if errors:
                    result_details.append("\nErrors encountered:")
                    for error in errors:
                        result_details.append(f"- {error}")

                # Write detailed output
                with open(self.docker_logs_file_path, 'w') as f:
                    f.write('Task completed successfully\n')
                    f.write('\n'.join(result_details))

                final_status = "Success"
                details = f"Deleted {deleted_count} files"

                # Create the output marker file
                with self.output().open('w') as f:
                    f.write(f'Task completed: deleted {deleted_count} files')

            except TimeoutError:
                final_status = "Timeout"
                details = f"Task timed out after {self.timeout_seconds} seconds"

                with open(self.docker_logs_file_path, 'w') as f:
                    f.write(f'Task timed out after {self.timeout_seconds} seconds')

                with self.output().open('w') as f:
                    f.write('Task timed out after {self.timeout_seconds} seconds')

        except Exception as e:
            error_message = f"Unexpected error: {str(e)}"
            print(error_message)
            details = error_message

            # Try to write to output even in case of error
            try:
                with open(self.docker_logs_file_path, 'w') as f:
                    f.write(f'Task failed: {error_message}')

                with self.output().open('w') as f:
                    f.write('Task failed: ' + error_message)
            except:
                pass

            raise

        finally:
            end_time = datetime.datetime.now()
            logger.log_task_timing(
                task_name="DeleteOldGatewayFiles",
                start_time=start_time,
                end_time=end_time,
                status=final_status,
                details=details
            )


class LogFileCleanup(pu.TimeoutMixin, luigi.Task):

    log_directory = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs"
    days_to_keep = luigi.IntParameter(default=15)
    file_pattern = 'log_*.txt'

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_dockerLogsFileCleanup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    timeout_seconds = luigi.IntParameter(default=None)
    max_retries = luigi.IntParameter(default=None)
    retry_delay = luigi.IntParameter(default=None)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params['timeout_seconds']

        if self.max_retries is None:
            self.max_retries = task_params['max_retries']

        if self.retry_delay is None:
            self.retry_delay = task_params['retry_delay']

    def output(self):
        return luigi.LocalTarget(f'/app/log_cleanuplogs.txt')

    def run(self):

        logger = pu.TaskLogger()
        start_time = datetime.datetime.now()

        try:

            # Calculate cutoff date
            cutoff_date = datetime.datetime.now() - datetime.timedelta(days=self.days_to_keep)

            # Get list of log files matching pattern
            file_path_pattern = os.path.join(self.log_directory, self.file_pattern)
            log_files = glob.glob(file_path_pattern)

            # Track statistics
            deleted_count = 0
            failed_count = 0

            for file_path in log_files:
                try:
                    # Get file modification time
                    file_mtime = os.path.getmtime(file_path)
                    file_datetime = datetime.datetime.fromtimestamp(file_mtime)

                    # Check if file is older than cutoff date
                    if file_datetime < cutoff_date:
                        # Delete the file
                        os.remove(file_path)
                        deleted_count += 1
                except Exception as e:
                    failed_count += 1

            # Write summary to output file
            with open(self.docker_logs_file_path, 'w') as f:
                summary = {
                    'timestamp': datetime.datetime.now().isoformat(),
                    'log_directory': self.log_directory,
                    'file_pattern': self.file_pattern,
                    'days_to_keep': self.days_to_keep,
                    'cutoff_date': cutoff_date.isoformat(),
                    'total_files_found': len(log_files),
                    'files_deleted': deleted_count,
                    'failures': failed_count
                }
                for key, value in summary.items():
                    f.write(f"{key}: {value}\n")
            status = "Success"
            details = f"Deleted {deleted_count} files, {failed_count} failures"

            # Create the output marker file
            with self.output().open('w') as f:
                f.write('Task completed')

        except Exception as e:
            print(f"Error in LogFileCleanup: {str(e)}")
            status = "Failed"
            details = str(e)
            raise

        finally:
            end_time = datetime.datetime.now()

            logger.log_task_timing(
                task_name="LogFileCleanup",
                start_time=start_time,
                end_time=end_time,
                status=status,
                details=details
            )


class SendPipelineCompletionNotification(luigi.Task):
    """Send notification when the entire pipeline is complete."""

    # Custom message parameter
    custom_message = luigi.Parameter(default="")

    # Tasks this notification depends on
    depends_on = luigi.Parameter(default=[])

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_sendPipelineCompletionNotification_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"


    def requires(self):
        return self.depends_on

    def output(self):
        return luigi.LocalTarget(f'/app/log_notification.txt')

    def run(self):
        print("------------------------------------")
        print(" Sending pipeline completion notifications.")
        print("------------------------------------")

        logger = pu.TaskLogger()
        start_time = datetime.datetime.now()

        success = True
        notification_results = []

        try:
            # Get email recipients from environment variable
            email_recipients_str = os.getenv('SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS', '')
            if email_recipients_str:
                email_recipients = [email.strip() for email in email_recipients_str.split(',')]
            else:
                email_recipients = []

            # Get parameters from timeout manager
            task_name = self.__class__.__name__
            task_params = get_task_parameters(task_name)

            # Create notification messages
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            subject = f"{ORGANIZATION.upper()} {task_params['timeout_config']} Forecast Pipeline Complete - {current_time}"

            # Base message
            message = f"Sapphire Forecast Pipeline for {ORGANIZATION.upper()} completed successfully at {current_time}.\n\n"

            # Add custom message if provided
            if self.custom_message:
                message += f"Message: {self.custom_message}\n\n"

            # Add links to dashboard if applicable
            dashboard_url = os.getenv('ieasyhydroforecast_url', '')
            if dashboard_url:
                message += f"View the latest forecasts on the dashboard: {dashboard_url}\n\n"

            # Add a summary of tasks that were run
            message += f"Tasks completed for {ORGANIZATION.upper()}:\n"
            if ORGANIZATION == 'demo':
                message += "- PreprocessingRunoff\n"
                message += "- LinearRegression\n"
                message += "- PostProcessingForecasts\n"
                message += "- LogFileCleanup\n"
            elif ORGANIZATION == 'kghm':
                message += "- PreprocessingRunoff\n"
                message += "- LinearRegression\n"
                message += "- PostProcessingForecasts\n"
                message += "- RunAllMLModels\n"
                message += "- ConceptualModel\n"
                message += "- LogFileCleanup\n"
                message += "- DeleteOldGatewayFiles\n"

            message += "\nThis is an automated notification."

            # Send email notifications if recipients are specified
            if email_recipients:
                # You could also attach summary files or plots here
                attachment_paths = []

                email_success = NotificationManager.send_email(
                    recipients=email_recipients,
                    subject=subject,
                    message=message,
                    attachment_paths=attachment_paths
                )

                if email_success:
                    notification_results.append(f"Email sent to {', '.join(email_recipients)}")
                else:
                    notification_results.append(f"Failed to send email to {', '.join(email_recipients)}")
                    success = False
            else:
                notification_results.append("No email recipients configured")

            # Write output
            with open(self.docker_logs_file_path, 'w') as f:
                f.write(f"Notification task completed at {current_time}\n\n")
                f.write("\n".join(notification_results))

            # Create the output marker file
            with self.output().open('w') as f:
                f.write('Task completed')

        except Exception as e:
            print(f"Error sending notifications: {str(e)}")
            success = False

            with open(self.docker_logs_file_path, 'w') as f:
                f.write(f"Notification task failed: {str(e)}")

        finally:
            end_time = datetime.datetime.now()
            status = "Success" if success else "Failed"
            details = ", ".join(notification_results) if notification_results else "No notifications sent"

            logger.log_task_timing(
                task_name="SendPipelineCompletionNotification",
                start_time=start_time,
                end_time=end_time,
                status=status,
                details=details
            )


class RunWorkflow(luigi.Task):
    """Main wrapper task that runs the entire forecast pipeline."""

    # Parameters for notifications
    custom_message = luigi.Parameter(default="")

    # Flag to control whether to send notifications
    send_notifications = luigi.BoolParameter(default=True)

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get('ieasyforecast_intermediate_data_path'))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_runWorkflow_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"


    def requires(self):

        # Test if directory of docker_logs_file_path exists and create it if not
        os.makedirs(os.path.dirname(self.docker_logs_file_path), exist_ok=True)

        if ORGANIZATION=='demo':
            print("Running demo workflow.")
            base_tasks = [PostProcessingForecasts(),
                          LogFileCleanup()]

        elif ORGANIZATION=='kghm':
            print("Running KGHM workflow.")
            base_tasks =  [PostProcessingForecasts(),
                           RunAllMLModels(),
                           ConceptualModel(),
                           DeleteOldGatewayFiles(),
                           LogFileCleanup()
                           ]
        # You can add workflow definitions for other organizations here.

        # Default to demo workflow
        else:
            print("ORGANIZATION not specified.\n  -> Defaulting to demo workflow.")
            base_task = [PostProcessingForecasts()]

        # If notifications are enabled, add the notification task
        if self.send_notifications:
            return SendPipelineCompletionNotification(
                custom_message=self.custom_message,
                depends_on=base_tasks  # Pass the base tasks as dependencies
            )
        else:
            return base_tasks

    def output(self):
        return luigi.LocalTarget(f'/app/log_workflow_complete.txt')

    def run(self):
        print("Workflow completed.")

        # Create output file to mark completion
        with open(self.docker_logs_file_path, 'w') as f:
            f.write(f"Workflow for {ORGANIZATION} completed at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Create the output marker file
        with self.output().open('w') as f:
            f.write('Task completed')


if __name__ == '__main__':
    luigi.build([RunWorkflow()])