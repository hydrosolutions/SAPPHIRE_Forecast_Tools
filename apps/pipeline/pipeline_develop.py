import os
import time
import subprocess
import paramiko  # for ssh connection
import socket
import threading
from dotenv import load_dotenv

from tempfile import mkdtemp
import logging

from ieasyhydro_sdk.sdk import IEasyHydroSDK
from ieasyhydro_sdk.filters import BasicDataValueFilters
import datetime as dt
import pandas as pd

import luigi

from luigi.local_target import LocalFileSystem

logger = logging.getLogger('luigi-interface')

try:
    import docker
    from docker.errors import ContainerError, ImageNotFound, APIError

except ImportError:
    logger.warning('docker is not installed. DockerTask requires docker.')
    docker = None

"""
class DockerTask(luigi.Task):

    @property
    def image(self):
        return 'alpine'

    @property
    def command(self):
        return "echo hello world"

    @property
    def name(self):
        return None

    @property
    def host_config_options(self):
        '''
        Override this to specify host_config options like gpu requests or shm
        size e.g. `{"device_requests": [docker.types.DeviceRequest(count=1, capabilities=[["gpu"]])]}`

        See https://docker-py.readthedocs.io/en/stable/api.html#docker.api.container.ContainerApiMixin.create_host_config
        '''
        return {}

    @property
    def container_options(self):
        '''
        Override this to specify container options like user or ports e.g.
        `{"user": f"{os.getuid()}:{os.getgid()}"}`

        See https://docker-py.readthedocs.io/en/stable/api.html#docker.api.container.ContainerApiMixin.create_container
        '''
        return {}

    @property
    def environment(self):
        return {}

    @property
    def container_tmp_dir(self):
        return '/tmp/luigi'

    @property
    def binds(self):
        '''
        Override this to mount local volumes, in addition to the /tmp/luigi
        which gets defined by default. This should return a list of strings.
        e.g. ['/hostpath1:/containerpath1', '/hostpath2:/containerpath2']
        '''
        return None

    @property
    def network_mode(self):
        return ''

    @property
    def docker_url(self):
        return None

    @property
    def auto_remove(self):
        return True

    @property
    def force_pull(self):
        return False

    @property
    def mount_tmp(self):
        return True

    def __init__(self, *args, **kwargs):
        '''
        When a new instance of the DockerTask class gets created:
        - call the parent class __init__ method
        - start the logger
        - init an instance of the docker client
        - create a tmp dir
        - add the temp dir to the volume binds specified in the task
        '''
        super(DockerTask, self).__init__(*args, **kwargs)
        self.__logger = logger

        '''init docker client
        using the low level API as the higher level API does not allow to mount single
        files as volumes
        '''
        self._client = docker.APIClient(self.docker_url)

        # add latest tag if nothing else is specified by task
        if ':' not in self.image:
            self._image = ':'.join([self.image, 'latest'])
        else:
            self._image = self.image

        if self.mount_tmp:
            # create a tmp_dir, NOTE: /tmp needs to be specified for it to work on
            # macOS, despite what the python documentation says
            self._host_tmp_dir = mkdtemp(suffix=self.task_id,
                                         prefix='luigi-docker-tmp-dir-',
                                         dir='/tmp')

            self._binds = ['{0}:{1}'.format(self._host_tmp_dir, self.container_tmp_dir)]
        else:
            self._binds = []

        # update environment property with the (internal) location of tmp_dir
        self.environment['LUIGI_TMP_DIR'] = self.container_tmp_dir

        # add additional volume binds specified by the user to the tmp_Dir bind
        if isinstance(self.binds, str):
            self._binds.append(self.binds)
        elif isinstance(self.binds, list):
            self._binds.extend(self.binds)

        # derive volumes (ie. list of container destination paths) from
        # specified binds
        self._volumes = [b.split(':')[1] for b in self._binds]


    def run(self):

        # get local image
        local_images = self._client.images(name=self._image)
        local_image_id = local_images[0].id if local_images else None

        # get remote image ID
        try:
            remote_image_inspect = self._client.inspect_image(f"{self._image}:latest")
            remote_image_id = remote_image_inspect['Id']
        except APIError as e:
            self.__logger.warning("Error in Docker API: " + e.explanation)
            raise

        # compare local to remote image ID & remove the local image if they differ
        if local_image_id and local_image_id != remote_image_id:
            self.__logger.info("Removing local image: " + self._image)
            self._client.remove_image(local_image_id)

        # pull the image if it does not exist locally
        if self.force_pull or len(self._client.images(name=self._image)) == 0:
            logger.info('Pulling docker image ' + self._image)
            try:
                for logline in self._client.pull(self._image, stream=True):
                    logger.debug(logline.decode('utf-8'))
            except APIError as e:
                self.__logger.warning("Error in Docker API: " + e.explanation)
                raise

        # remove clashing container if a container with the same name exists
        if self.auto_remove and self.name:
            try:
                self._client.remove_container(self.name,
                                              force=True)
            except APIError as e:
                self.__logger.warning("Ignored error in Docker API: " + e.explanation)

        # run the container
        try:
            logger.debug('Creating image: %s command: %s volumes: %s'
                         % (self._image, self.command, self._binds))

            host_config = self._client.create_host_config(binds=self._binds,
                                                          network_mode=self.network_mode,
                                                          **self.host_config_options)

            container = self._client.create_container(self._image,
                                                      command=self.command,
                                                      name=self.name,
                                                      environment=self.environment,
                                                      volumes=self._volumes,
                                                      host_config=host_config,
                                                      **self.container_options)
            self._client.start(container['Id'])

            exit_status = self._client.wait(container['Id'])
            # docker-py>=3.0.0 returns a dict instead of the status code directly
            if type(exit_status) is dict:
                exit_status = exit_status['StatusCode']

            if exit_status != 0:
                stdout = False
                stderr = True
                error = self._client.logs(container['Id'],
                                          stdout=stdout,
                                          stderr=stderr)
            if self.auto_remove:
                try:
                    self._client.remove_container(container['Id'])
                except docker.errors.APIError:
                    self.__logger.warning("Container " + container['Id'] +
                                          " could not be removed")
            if exit_status != 0:
                raise ContainerError(container, exit_status, self.command, self._image, error)

        except ContainerError as e:
            # catch non zero exti status and return it
            container_name = ''
            if self.name:
                container_name = self.name
            try:
                message = e.message
            except AttributeError:
                message = str(e)
            self.__logger.error("Container " + container_name +
                                " exited with non zero code: " + message)
            raise
        except ImageNotFound:
            self.__logger.error("Image " + self._image + " not found")
            raise
        except APIError as e:
            self.__logger.error("Error in Docker API: "+e.explanation)
            raise

        # delete temp dir
        filesys = LocalFileSystem()
        if self.mount_tmp and filesys.exists(self._host_tmp_dir):
            filesys.remove(self._host_tmp_dir, recursive=True)
"""

class Environment:
    def __init__(self, dotenv_path):
        load_dotenv(dotenv_path=dotenv_path)

    def get(self, key):
        return os.getenv(key)

# Initialize the Environment class with the path to your .env file
env = Environment('../../../sensitive_data_forecast_tools/config/.env_develop_kghm')

# We have a second environment file with the credentials to the iEH server
env_ieh = Environment('../../../sensitive_data_forecast_tools/config/.env_ieh')


class RunPreprocessingRunoff(luigi.Task):
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

    def run_preprocessing_runoff(self):
        print("\n\n{task} says: Running preprocessing runoff!\n\n".format(task=self.__class__.__name__))

        script_path = "../preprocessing_runoff/preprocessing_runoff.py"
        env = os.environ.copy()
        env["SAPPHIRE_OPDEV_ENV"] = "True"
        result = subprocess.run(["python", script_path], env=env, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"Script {script_path} failed with error: {result.stderr}")
        else:
            print(f"Script {script_path} output: {result.stdout}")

    def test_ssh_connection(self):
        print("\n\n{task} says: Testing SSH connection!\n\n".format(task=self.__class__.__name__))

        print("DEBUG: IEASYHYDRO_HOST: ", os.getenv("IEASYHYDRO_HOST"))

        # Load sdk configuration from .env
        ieh_sdk = IEasyHydroSDK()

        predictor_dates = [dt.datetime(2024, 6, 3, 0, 0, 0), dt.datetime.today()]

        # Define date filter
        filters = BasicDataValueFilters(
            local_date_time__gte=predictor_dates[0],
            local_date_time__lt=predictor_dates[1]
        )

        site = '15102'

        # Get data
        qdata = ieh_sdk.get_data_values_for_site(
            [site],
            'discharge_daily_average',
            filters=filters
        )
        qdata = pd.DataFrame(qdata['data_values'])
        print("get_data_values_for_site:\n", qdata)

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

            # Test if ssh connection is working
            self.test_ssh_connection()

            # Run the preprocessing runoff script
            #self.run_preprocessing_runoff()

        except Exception as e:
            print(f"Error opening SSH tunnel: {e}")

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

"""
class RunLinearRegression(luigi.Task):
    def requires(self):
        return RunPreprocessingRunoff()

    def output(self):
        # Load the configuration file for development mode
        env_file_path = "../../../sensitive_data_forecast_tools/config/.env_develop_kghm"
        load_dotenv(dotenv_path=env_file_path)

        # Get the path to the output file
        output_file_path = os.path.join(
            os.getenv("ieasyforecast_intermediate_data_path"),
            os.getenv("ieasyforecast_last_successful_run_file")
            )

        return luigi.LocalTarget(output_file_path)

    def run(self):
        print("\n\n{task} says: Running linear regression!\n\n".format(task=self.__class__.__name__))

        script_path = "../linear_regression/linear_regression.py"
        env = os.environ.copy()
        env["SAPPHIRE_OPDEV_ENV"] = "True"
        result = subprocess.run(["python", script_path], env=env, capture_output=True, text=True)

        if result.returncode != 0:
            raise Exception(f"Script {script_path} failed with error: {result.stderr}")
        else:
            print(f"Script {script_path} output: {result.stdout}")

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
if __name__ == "__main__":
    luigi.run(['RunPreprocessingRunoff', '--local-scheduler'])

    # Run in development:
    # python pipeline_develop.py RunPreprocessingRunoff --local-scheduler