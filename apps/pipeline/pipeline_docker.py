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

import datetime
import glob
import json
import os
import platform
import re
import time
from typing import Any

import docker
import luigi

# Import local utils
from apps.pipeline.src import pipeline_utils as pu
from apps.pipeline.src.environment import Environment
from apps.pipeline.src.notification_manager import NotificationManager
from apps.pipeline.src.timeout_manager import get_task_parameters

# Initialize the Environment class with the path to your .env file
env_file_path = os.getenv("ieasyhydroforecast_env_file_path")
env = Environment(env_file_path)
# Get the tag of the docker image to use
TAG = env.get("ieasyhydroforecast_backend_docker_image_tag")
# Get the organization for which to run the forecast tools
ORGANIZATION = env.get("ieasyhydroforecast_organization")
# URL of the sapphire data gateway
SAPPHIRE_DG_HOST = env.get("SAPPHIRE_DG_HOST")
RUN_ML_MODELS = env.get("ieasyhydroforecast_run_ML_models")
RUN_CM_MODELS = env.get("ieasyhydroforecast_run_CM_models")


# Function to convert a relative path to an absolute path
def get_absolute_path(relative_path):
    # print("In get_absolute_path: ")
    # print(" - Relative path: ", relative_path)

    # Test if there environment variable "ieasyforecast_data_root_dir" is set
    data_root_dir = os.getenv("ieasyhydroforecast_data_root_dir")
    if data_root_dir:
        # If it is set, use it as the root directory
        # Strip the relative path from 2 "../" strings
        relative_path = re.sub(r"\.\./\.\./\.\.", "", relative_path)

        return data_root_dir + relative_path

    else:
        # Current working directory. Should be one above the root of the project
        cwd = os.getcwd()
        # Strip the relative path from 2 "../" strings
        relative_path = re.sub(r"\.\./\.\./\.\.", "", relative_path)

        return os.path.join(cwd, relative_path)


def get_bind_path(relative_path):
    # Strip the relative path from ../../.. to get the path to bind to the container
    relative_path = re.sub(r"\.\./\.\./\.\.", "", relative_path)

    return relative_path


def get_local_path(relative_path):
    # Strip 2 ../ of the relative path
    relative_path = re.sub(r"\.\./\.\./", "", relative_path)

    return relative_path


def setup_docker_volumes(env, paths=None):
    """Set up Docker volumes from environment variables."""
    if paths is None:
        paths = ["ieasyforecast_configuration_path", "ieasyforecast_intermediate_data_path"]

    volumes = {}
    for path_key in paths:
        if env.get(path_key):
            absolute_path = get_absolute_path(env.get(path_key))
            bind_path = get_bind_path(env.get(path_key))
            volumes[absolute_path] = {"bind": bind_path, "mode": "rw"}

    return volumes


# Define global paths for marker files
# Note: Use get_bind_path() because this code runs INSIDE a Docker container.
# get_bind_path() returns the container-internal path that matches the volume mount.
# get_absolute_path() would return the HOST path which doesn't exist inside the container.
MARKER_DIR = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/marker_files"
os.makedirs(MARKER_DIR, exist_ok=True)  # Ensure directory exists


def get_marker_filepath(task_name, date=None, time_slot=None):
    """Generate consistent marker filepath for a given task, date, and optional time slot.

    Args:
        task_name: Name of the task (e.g., 'preprocessing_gateway')
        date: Date for the marker (defaults to today)
        time_slot: Optional time slot for sub-daily tasks (0, 1, 2, 3 for 4x daily)

    Returns:
        Path to marker file, e.g.:
        - Daily: preprocessing_gateway_2026-02-02.marker
        - Sub-daily: preprocessing_gateway_2026-02-02_slot0.marker
    """
    if date is None:
        date = datetime.date.today()

    if time_slot is not None:
        return f"{MARKER_DIR}/{task_name}_{date}_slot{time_slot}.marker"
    return f"{MARKER_DIR}/{task_name}_{date}.marker"


def get_gateway_dependency(time_slot=None):
    """Returns the appropriate gateway task based on whether it already ran.

    For daily runs (time_slot is None), this function checks if the preprocessing
    gateway has already run today by looking for its daily marker file. If found,
    it returns an ExternalPreprocessingGateway task (which just checks the marker
    exists). Otherwise, it returns a PreprocessingGatewayQuantileMapping task to
    actually run the preprocessing.

    For sub-daily runs (time_slot is not None), this function always returns
    PreprocessingGatewayQuantileMapping so that the correct time-slot-specific
    marker is produced and validated.

    Args:
        time_slot: Optional time slot for sub-daily forecasts (None for daily)

    Returns:
        ExternalPreprocessingGateway if already ran (daily runs only),
        else PreprocessingGatewayQuantileMapping.
    """
    today = datetime.date.today()

    # Daily runs: allow reuse via ExternalPreprocessingGateway
    if time_slot is None:
        marker_file = get_marker_filepath("preprocessing_gateway", date=today)
        if os.path.exists(marker_file):
            print(f"Using external gateway task (already run) for {today}")
            return ExternalPreprocessingGateway(date=today)
        else:
            print(f"No gateway marker found for {today}, running gateway preprocessing")
            return PreprocessingGatewayQuantileMapping()

    # Sub-daily runs: always run preprocessing to ensure correct slot-specific marker
    print(f"Sub-daily gateway run for {today} slot {time_slot}: running gateway preprocessing")
    return PreprocessingGatewayQuantileMapping()


def get_maintenance_marker_filepath(task_name, date=None):
    """Marker file for maintenance tasks, separate from operational markers.

    Args:
        task_name: Name of the maintenance task (e.g., 'gateway', 'preprunoff')
        date: Date for the marker (defaults to today)

    Returns:
        Path to marker file with 'maintenance_' prefix.
    """
    return get_marker_filepath(f"maintenance_{task_name}", date=date)


def get_docker_host_env_overrides():
    """Return env var overrides for macOS Docker host networking.

    On macOS, containers can't reach localhost on the host. Docker provides
    'host.docker.internal' as a DNS name for the host. This function
    detects macOS and returns the necessary env var overrides.

    Returns:
        List of env var strings to add to container environment.
    """
    overrides = []
    if platform.system() == "Darwin":
        hf_host = env.get("IEASYHYDROHF_HOST") or os.getenv("IEASYHYDROHF_HOST", "")
        if "localhost" in hf_host:
            overrides.append(
                f"IEASYHYDROHF_HOST={hf_host.replace('localhost', 'host.docker.internal')}"
            )
    return overrides


class DockerTaskBase(pu.TimeoutMixin, luigi.Task):
    """Base class for Docker-based Luigi tasks with common functionality."""

    # Common timeout parameters
    timeout_seconds = luigi.OptionalIntParameter(default=None)
    max_retries = luigi.OptionalIntParameter(default=None)
    retry_delay = luigi.OptionalIntParameter(default=None)

    # Log file path
    docker_logs_file_path = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params["timeout_seconds"]

        if self.max_retries is None:
            self.max_retries = task_params["max_retries"]

        if self.retry_delay is None:
            self.retry_delay = task_params["retry_delay"]

        # Ensure logs directory exists
        if self.docker_logs_file_path:
            os.makedirs(os.path.dirname(self.docker_logs_file_path), exist_ok=True)

    def send_failure_notification(self, error_details, logs=None):
        """Send failure notification with log file attachments"""
        from apps.pipeline.src.notification_manager import NotificationManager

        # Get the task name
        task_name = self.__class__.__name__

        # Collect any log files
        log_file_paths = []
        if self.docker_logs_file_path and os.path.exists(self.docker_logs_file_path):
            log_file_paths.append(self.docker_logs_file_path)

        # If logs were provided, write them to a temporary file and attach
        if logs:
            temp_log_path = (
                f"{os.path.dirname(self.docker_logs_file_path)}/failure_log_{int(time.time())}.txt"
            )
            try:
                with open(temp_log_path, "w") as f:
                    f.write(logs)
                log_file_paths.append(temp_log_path)
            except Exception as e:
                print(f"Failed to write logs to temp file: {str(e)}")

        # Additional info about the task
        additional_info = {
            "Task": task_name,
            "Timeout (seconds)": self.timeout_seconds,
            "Max retries": self.max_retries,
            "Retry delay": self.retry_delay,
            "Failure time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Send the notification
        return NotificationManager.send_failure_notification(
            task_name=task_name,
            error_details=error_details,
            log_file_paths=log_file_paths,
            additional_info=additional_info,
        )

    def run_docker_container(
        self,
        image_name: str,
        container_name: str,
        volumes: dict[str, dict[str, str]],
        environment: list[str],
        attempt_number: int,
        network: str = "host",
        mem_limit: str | None = None,
        memswap_limit: str | None = None,
        command: Any | None = None,
    ) -> tuple[str | None, int, str]:
        """Run a Docker container and handle timeouts and cleanup.

        Args:
            image_name: Docker image name (e.g., 'sapphire-preprunoff')
            container_name: Base name for the container
            volumes: Volume mount specification
            environment: List of env var strings
            attempt_number: Current attempt number (for unique naming)
            network: Docker network mode
            mem_limit: Container memory limit (e.g., '4g')
            memswap_limit: Container memory+swap limit (e.g., '6g')
            command: Override the container CMD (str or list)
        """
        client = docker.from_env()
        container = None

        try:
            # Pull the latest image if needed
            repo = "mabesa"
            tag = os.getenv("ieasyhydroforecast_backend_docker_image_tag", "latest")

            if pu.there_is_a_newer_image_on_docker_hub(
                client, repository=repo, image_name=image_name, tag=tag
            ):
                print(f"Pulling the latest image for {image_name} from Docker Hub.")
                client.images.pull(f"{repo}/{image_name}", tag=tag)

            # Build kwargs for containers.run()
            run_kwargs = dict(
                detach=True,
                environment=environment,
                volumes=volumes,
                name=f"{container_name}_attempt_{attempt_number}_{time.time()}",
                network=network,
            )
            if mem_limit is not None:
                run_kwargs["mem_limit"] = mem_limit
            if memswap_limit is not None:
                run_kwargs["memswap_limit"] = memswap_limit
            if command is not None:
                run_kwargs["command"] = command

            # Run the container with unique name
            container = client.containers.run(
                f"{repo}/{image_name}:{tag}",
                **run_kwargs,
            )

            print(f"Container {container.id} is running.")

            # Wait for container with timeout
            try:
                wait_result = self.run_with_timeout(container.wait)
                raw_status = (
                    wait_result.get("StatusCode", 1) if isinstance(wait_result, dict) else 1
                )
                # Exclude bool: isinstance(True, int) is True, and False == 0, so a
                # bool StatusCode would silently read as success without this check.
                exit_status = raw_status if type(raw_status) is int else 1
            except TimeoutError:
                print(f"Container {container.id} timed out after {self.timeout_seconds} seconds")
                container.stop()
                exit_status = 124

            logs = container.logs().decode("utf-8")
            print(f"Container {container.id} exited with status code {exit_status}")

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
                except Exception:
                    pass
            return None, 1, str(e)

    def execute_with_retries(self, container_run_func):
        """Execute container function with retry logic and logging."""
        logger = pu.TaskLogger()
        start_time = datetime.datetime.now()

        print("------------------------------------")
        print(f" Running {self.__class__.__name__} task.")
        print("------------------------------------")

        attempts = 0
        final_status = "Failed"
        details = ""

        try:
            while attempts < self.max_retries:
                attempts += 1
                print(f"Attempt {attempts} of {self.max_retries}")

                container_id, exit_status, logs = container_run_func(attempts)

                if exit_status == 0:
                    # Success - write output and exit
                    with open(self.docker_logs_file_path, "w") as f:
                        f.write("Task completed successfully\n")
                        f.write(f"Container ID: {container_id}\n")
                        f.write(f"Timeout: {self.timeout_seconds}\n")
                        f.write(f"Max retries: {self.max_retries}\n")
                        f.write(f"Logs:\n{logs}")

                    final_status = "Success"
                    details = f"Completed on attempt {attempts}"

                    # Create the output marker file
                    with self.output().open("w") as f:
                        f.write("Task completed")

                    break

                if exit_status == 124:  # Timeout
                    final_status = "Timeout"
                    details = f"Task timed out after {self.timeout_seconds} seconds"
                    self.send_failure_notification(
                        f"Task timed out after {self.timeout_seconds}s "
                        f"on attempt {attempts}/{self.max_retries}",
                        logs,
                    )
                    raise RuntimeError(
                        f"Task timed out after {self.timeout_seconds} seconds "
                        f"(attempt {attempts}/{self.max_retries})"
                    )

                if attempts < self.max_retries:
                    print(
                        f"Container failed with status {exit_status}. Retrying in {self.retry_delay} seconds..."
                    )
                    time.sleep(self.retry_delay)
                else:
                    print(f"Container failed after {self.max_retries} attempts.")
                    error_msg = f"Task failed after {self.max_retries} attempts. Last exit status: {exit_status}"

                    # Send failure notification
                    self.send_failure_notification(error_msg, logs)

                    raise RuntimeError(
                        f"Task failed after {self.max_retries} attempts. Last exit status: {exit_status}\nLogs:\n{logs}"
                    )

        finally:
            end_time = datetime.datetime.now()
            logger.log_task_timing(
                task_name=self.__class__.__name__,
                start_time=start_time,
                end_time=end_time,
                status=final_status,
                details=details,
            )

        return final_status, details


class ExternalConceptualModel(luigi.ExternalTask):
    """
    External task that represents conceptual model being run by a separate process.
    This task checks for a marker file that indicates the model has been run.
    """

    # Define the date parameter to check for today's marker
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        # Look for a marker file that indicates the conceptual model has run
        marker_file = get_marker_filepath("conceptual_model", date=self.date)
        return luigi.LocalTarget(marker_file)


class ExternalPreprocessingGateway(luigi.ExternalTask):
    """
    External task that represents preprocessing gateway being done by a separate
    process. This task checks for a marker file that indicates preprocessing is
    complete.
    """

    # Define the date parameter to check for today's marker
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        # Look for a marker file that indicates preprocessing is complete
        marker_file = get_marker_filepath("preprocessing_gateway", date=self.date)
        return luigi.LocalTarget(f"{marker_file}")


class ExternalPreprocessingRunoff(luigi.ExternalTask):
    """
    External task that represents preprocessing runoff being done by a separate
    process. This task checks for a marker file that indicates preprocessing is
    complete.
    """

    # Define the date parameter to check for today's marker
    date = luigi.DateParameter(default=datetime.date.today())

    def output(self):
        # Look for a marker file that indicates preprocessing is complete
        marker_file = get_marker_filepath("preprocessing_runoff", date=self.date)
        return luigi.LocalTarget(marker_file)


class PreprocessingRunoff(DockerTaskBase):
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_preprunoff_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def output(self):
        return luigi.LocalTarget("/app/log_preprunoff.txt")

    def run(self):
        # Set up volumes
        volumes = setup_docker_volumes(
            env,
            [
                "ieasyforecast_configuration_path",
                "ieasyforecast_intermediate_data_path",
                "ieasyforecast_daily_discharge_path",
            ],
        )

        # Define environment variables
        gsheets_enabled = os.environ.get("GOOGLE_SHEETS_ENABLED", "")
        gsheets_id = os.environ.get("GOOGLE_SHEETS_DISCHARGE_ID", "")
        gsheets_creds = os.environ.get("GOOGLE_SHEETS_CREDENTIALS_PATH", "")
        gsheets_codes = os.environ.get("GOOGLE_SHEETS_SITE_CODES", "")
        environment = [
            f"ieasyhydroforecast_env_file_path={env_file_path}",
            f"GOOGLE_SHEETS_ENABLED={gsheets_enabled}",
            f"GOOGLE_SHEETS_DISCHARGE_ID={gsheets_id}",
            f"GOOGLE_SHEETS_CREDENTIALS_PATH={gsheets_creds}",
            f"GOOGLE_SHEETS_SITE_CODES={gsheets_codes}",
        ]

        # Execute with retries using the base class method
        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-preprunoff",
                container_name="preprunoff",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
            )
        )
        # Note: Marker file writing removed - preprocessing_runoff now runs every time
        # to ensure fresh data (fast enough after py312 migration)


class PreprocessingGatewayQuantileMapping(DockerTaskBase):
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_pregateway_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def output(self):
        return luigi.LocalTarget("/app/log_pregateway.txt")

    def run(self):
        # Set up volumes
        volumes = setup_docker_volumes(
            env, ["ieasyforecast_configuration_path", "ieasyforecast_intermediate_data_path"]
        )

        # Define environment variables
        environment = [
            f"ieasyhydroforecast_env_file_path={env_file_path}",
            "SAPPHIRE_DG_HOST=" + SAPPHIRE_DG_HOST,
        ]

        # Execute with retries using the base class method
        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-prepgateway",
                container_name="prepgateway",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
            )
        )

        # Write marker file only if successful
        if status == "Success":
            # Create the marker file that dependent tasks will check for
            today = datetime.date.today()
            marker_file = get_marker_filepath("preprocessing_gateway", date=today)
            print(f"Writing success marker file to: {marker_file}")
            with open(marker_file, "w") as f:
                f.write(f"PreprocessingGateway completed successfully at {datetime.datetime.now()}")
            # Verify file was created
            if os.path.exists(marker_file):
                print(f"✅ Marker file created successfully at {marker_file}")
            else:
                print(f"❌ Failed to create marker file at {marker_file}")


class RunPreprocessingGatewayWorkflow(luigi.Task):
    """Workflow for gateway preprocessing that can run early (10:00)."""

    # Use the intermediate_data_path for log files
    intermediate_data_path = get_bind_path(env.get("ieasyforecast_intermediate_data_path"))
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_preprocessing_gateway_workflow_{datetime.date.today()}.txt"

    def requires(self):
        # Only gateway preprocessing
        return PreprocessingGatewayQuantileMapping()

    def output(self):
        return luigi.LocalTarget("/app/log_preprocessing_gateway_complete.txt")

    def run(self):
        print("Gateway preprocessing workflow completed.")

        # Create output file to mark completion
        with open(self.docker_logs_file_path, "w") as f:
            f.write(
                f"Gateway preprocessing workflow completed at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        with self.output().open("w") as f:
            f.write("Gateway preprocessing completed")


class RunPreprocessingRunoffWorkflow(luigi.Task):
    """Workflow for runoff preprocessing that must wait for data (11:00)."""

    # Use the intermediate_data_path for log files
    intermediate_data_path = get_bind_path(env.get("ieasyforecast_intermediate_data_path"))
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_preprocessing_runoff_workflow_{datetime.date.today()}.txt"

    def requires(self):
        # Only runoff preprocessing
        return PreprocessingRunoff()

    def output(self):
        return luigi.LocalTarget("/app/log_preprocessing_runoff_complete.txt")

    def run(self):
        print("Runoff preprocessing workflow completed.")

        # Create output file to mark completion
        with open(self.docker_logs_file_path, "w") as f:
            f.write(
                f"Runoff preprocessing workflow completed at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        with self.output().open("w") as f:
            f.write("Runoff preprocessing completed")


class LinearRegression(DockerTaskBase):
    """Run linear regression model in a Docker container."""

    # Define parameters for the task
    prediction_mode = luigi.Parameter(default="ALL")  # ALL, PENTAD, or DECAD
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_linreg_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def requires(self):
        # Always run preprocessing_runoff - it's fast enough now and ensures fresh data
        return PreprocessingRunoff()

    def output(self):
        return luigi.LocalTarget("/app/log_linreg.txt")

    def run(self):
        # Set up volumes
        volumes = setup_docker_volumes(
            env,
            [
                "ieasyforecast_configuration_path",
                "ieasyforecast_intermediate_data_path",
                "ieasyforecast_daily_discharge_path",
            ],
        )

        # Define environment variables
        environment = [
            f"ieasyhydroforecast_env_file_path={env_file_path}",
            f"SAPPHIRE_PREDICTION_MODE={self.prediction_mode}",
        ]

        # Execute with retries using the base class method
        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-linreg",
                container_name="linreg",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
            )
        )


class ConceptualModel(DockerTaskBase):
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_conceptmod_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def requires(self):
        # PreprocessingRunoff runs every time (fast enough after py312 migration)
        # Gateway uses marker file check to prevent double runs
        return [PreprocessingRunoff(), get_gateway_dependency()]

    def output(self):
        return luigi.LocalTarget("/app/log_conceptmod.txt")

    def run(self):
        # Set up volumes - following DockerTaskBase pattern
        volumes = setup_docker_volumes(
            env,
            [
                "ieasyforecast_configuration_path",
                "ieasyforecast_intermediate_data_path",
                "ieasyhydroforecast_conceptual_model_path",
            ],
        )

        # Define environment variables
        environment = ["SAPPHIRE_OPDEV_ENV=True", "IN_DOCKER_CONTAINER=True"]

        # Execute with retries using the base class method
        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-conceptmod",
                container_name="conceptmod",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
            )
        )

        # Write marker file only if successful
        if status == "Success":
            # Create the marker file that dependent tasks will check for
            today = datetime.date.today()
            marker_file = get_marker_filepath("conceptual_model", date=today)
            print(f"Writing success marker file to: {marker_file}")
            with open(marker_file, "w") as f:
                f.write(f"ConceptualModel completed successfully at {datetime.datetime.now()}")
            # Verify file was created
            if os.path.exists(marker_file):
                print(f"✅ Marker file created successfully at {marker_file}")
            else:
                print(f"❌ Failed to create marker file at {marker_file}")


class RunMLModel(DockerTaskBase):
    model_type = luigi.Parameter()
    prediction_mode = luigi.Parameter()
    run_mode = luigi.Parameter(default="forecast")

    # Define the logging output path dynamically
    @property
    def docker_logs_file_path(self):
        return f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_ml_{self.model_type}_{self.prediction_mode}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def requires(self):
        # PreprocessingRunoff runs every time (fast enough after py312 migration)
        # Gateway uses marker file check to prevent double runs
        return [PreprocessingRunoff(), get_gateway_dependency()]

    def output(self):
        return luigi.LocalTarget(f"/app/log_ml_{self.model_type}_{self.prediction_mode}.txt")

    def run(self):
        # Set up volumes
        volumes = setup_docker_volumes(
            env, ["ieasyforecast_configuration_path", "ieasyforecast_intermediate_data_path"]
        )

        # Define environment variables
        environment = [
            f"ieasyhydroforecast_env_file_path={env_file_path}",
            "IN_DOCKER=True",
            f"SAPPHIRE_MODEL_TO_USE={self.model_type}",
            f"SAPPHIRE_PREDICTION_MODE={self.prediction_mode}",
            f"RUN_MODE={self.run_mode}",
        ]

        # Execute with retries using the base class method
        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-ml",
                container_name=f"ml_{self.model_type}_{self.prediction_mode}_{attempt}",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                network="host",
            )
        )


class RunAllMLModels(luigi.WrapperTask):
    """Wrapper task to run all ML models in parallel for specified prediction modes."""

    # Prediction mode can be ALL, PENTAD, or DECAD
    prediction_mode = luigi.Parameter(default="ALL")

    def requires(self):
        # PreprocessingRunoff runs every time (fast enough after py312 migration)
        # Gateway uses marker file check to prevent double runs
        yield PreprocessingRunoff()
        yield get_gateway_dependency()

        # Get the list of available ML models from .env file
        models = env.get("ieasyhydroforecast_available_ML_models").split(",")

        # Determine which prediction modes to run based on the parameter
        if self.prediction_mode == "ALL":
            prediction_modes = ["PENTAD", "DECAD"]
        else:
            prediction_modes = [self.prediction_mode]

        for model in models:
            for mode in prediction_modes:
                yield RunMLModel(model_type=model, prediction_mode=mode, run_mode="forecast")


class PostProcessingForecasts(DockerTaskBase):
    """Post-process forecasts from different models."""

    # Add prediction mode parameter for mode-specific processing
    prediction_mode = luigi.Parameter(default="PENTAD")  # PENTAD or DECAD

    # Define logging output file path
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_postproc_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def requires(self):
        # Start with LinearRegression as the base requirement
        dependencies = [LinearRegression(prediction_mode=self.prediction_mode)]

        # Add ML models if enabled
        if RUN_ML_MODELS == "True":
            # Get the list of available ML models from .env file
            models = env.get("ieasyhydroforecast_available_ML_models").split(",")
            for model in models:
                dependencies.append(
                    RunMLModel(
                        model_type=model, prediction_mode=self.prediction_mode, run_mode="forecast"
                    )
                )

        # Add conceptual model if enabled
        if RUN_CM_MODELS == "True":
            # Check if the conceptual model has already run today
            today = datetime.date.today()
            marker_file = get_marker_filepath("conceptual_model", date=today)

            if os.path.exists(marker_file):
                print(f"Using external conceptual model task (already run) for {today}")
                dependencies.append(ExternalConceptualModel())
            else:
                print(f"No conceptual model marker found for {today}, adding to dependencies")
                dependencies.append(ConceptualModel())

        return dependencies

    def output(self):
        return luigi.LocalTarget("/app/log_postproc.txt")

    def run(self):
        # Set up volumes
        volumes = setup_docker_volumes(
            env, ["ieasyforecast_configuration_path", "ieasyforecast_intermediate_data_path"]
        )

        # Define environment variables
        environment = [
            f"ieasyhydroforecast_env_file_path={env_file_path}",
            f"SAPPHIRE_PREDICTION_MODE={self.prediction_mode}",  # Pass prediction mode to container
        ]

        # Execute with retries using the base class method
        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-postprocessing",
                container_name="postprocessing",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                network="host",
            )
        )


class DeleteOldGatewayFiles(pu.TimeoutMixin, luigi.Task):
    # Fix the typo in the class name (was "Gateywayy")

    # Define the folder path where the files are stored
    folder_path = get_local_path(
        os.path.join(
            env.get("ieasyforecast_intermediate_data_path"),
            env.get("ieasyhydroforecast_OUTPUT_PATH_DG"),
        )
    )
    # Define the number of days old the files should be before they are deleted
    days_old = luigi.IntParameter(default=2)

    # Set timeout to 5 minutes (300 seconds) - should be plenty for a file deletion task
    timeout_seconds = luigi.OptionalIntParameter(default=None)
    max_retries = luigi.OptionalIntParameter(default=None)
    retry_delay = luigi.OptionalIntParameter(default=None)

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get("ieasyforecast_intermediate_data_path"))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_deleteOldGatewayFiles_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params["timeout_seconds"]

        if self.max_retries is None:
            self.max_retries = task_params["max_retries"]

        if self.retry_delay is None:
            self.retry_delay = task_params["retry_delay"]

    def output(self):
        return luigi.LocalTarget("/app/log_deleteoldfiles.txt")

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
                # Run deletion with timeout protection and capture result
                deleted_count, deleted_files, errors = self.run_with_timeout(
                    lambda: self._delete_old_files()
                )

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
                with open(self.docker_logs_file_path, "w") as f:
                    f.write("Task completed successfully\n")
                    f.write("\n".join(result_details))

                final_status = "Success"
                details = f"Deleted {deleted_count} files"

                # Create the output marker file
                with self.output().open("w") as f:
                    f.write(f"Task completed: deleted {deleted_count} files")

            except TimeoutError:
                final_status = "Timeout"
                details = f"Task timed out after {self.timeout_seconds} seconds"

                with open(self.docker_logs_file_path, "w") as f:
                    f.write(f"Task timed out after {self.timeout_seconds} seconds")

                with self.output().open("w") as f:
                    f.write(f"Task timed out after {self.timeout_seconds} seconds")

        except Exception as e:
            error_message = f"Unexpected error: {str(e)}"
            print(error_message)
            details = error_message

            # Try to write to output even in case of error
            try:
                with open(self.docker_logs_file_path, "w") as f:
                    f.write(f"Task failed: {error_message}")

                with self.output().open("w") as f:
                    f.write("Task failed: " + error_message)
            except Exception:
                pass

            raise

        finally:
            end_time = datetime.datetime.now()
            logger.log_task_timing(
                task_name="DeleteOldGatewayFiles",
                start_time=start_time,
                end_time=end_time,
                status=final_status,
                details=details,
            )


class DeleteOldMarkerFiles(pu.TimeoutMixin, luigi.Task):
    """
    Delete marker files older than a specified number of days.

    Marker files are created by various tasks (PreprocessingRunoff,
    PreprocessingGatewayQuantileMapping, ConceptualModel) to track workflow
    completion. Old marker files should be cleaned up to prevent accumulation.
    """

    # Use the MARKER_DIR constant for the folder path
    folder_path = MARKER_DIR
    # Define the number of days old the files should be before they are deleted
    days_old = luigi.IntParameter(default=3)

    # Set timeout to 5 minutes (300 seconds) - should be plenty for a file deletion task
    timeout_seconds = luigi.OptionalIntParameter(default=None)
    max_retries = luigi.OptionalIntParameter(default=None)
    retry_delay = luigi.OptionalIntParameter(default=None)

    # Use the intermediate_data_path for log files
    intermediate_data_path = get_bind_path(env.get("ieasyforecast_intermediate_data_path"))
    # Define the logging output of the task
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_deleteOldMarkerFiles_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params["timeout_seconds"]

        if self.max_retries is None:
            self.max_retries = task_params["max_retries"]

        if self.retry_delay is None:
            self.retry_delay = task_params["retry_delay"]

    def output(self):
        return luigi.LocalTarget("/app/log_deleteoldmarkerfiles.txt")

    def _delete_old_files(self) -> tuple[int, list[str], list[str]]:
        """
        Delete marker files older than days_old and return count of deleted files,
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

                    # Only process marker files
                    if not filename.endswith(".marker"):
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
        print(" Running DeleteOldMarkerFiles task.")
        print("------------------------------------")
        print(f"Looking for marker files older than {self.days_old} days in: {self.folder_path}")

        final_status = "Failed"
        details = ""

        try:
            # Run with timeout protection
            try:
                # Run deletion with timeout protection and capture result
                deleted_count, deleted_files, errors = self.run_with_timeout(
                    lambda: self._delete_old_files()
                )

                # Format results for the log file
                result_details = [
                    f"Found and deleted {deleted_count} marker files older than {self.days_old} days.",
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
                with open(self.docker_logs_file_path, "w") as f:
                    f.write("Task completed successfully\n")
                    f.write("\n".join(result_details))

                final_status = "Success"
                details = f"Deleted {deleted_count} marker files"

                # Create the output marker file
                with self.output().open("w") as f:
                    f.write(f"Task completed: deleted {deleted_count} marker files")

            except TimeoutError:
                final_status = "Timeout"
                details = f"Task timed out after {self.timeout_seconds} seconds"

                with open(self.docker_logs_file_path, "w") as f:
                    f.write(f"Task timed out after {self.timeout_seconds} seconds")

                with self.output().open("w") as f:
                    f.write(f"Task timed out after {self.timeout_seconds} seconds")

        except Exception as e:
            error_message = f"Unexpected error: {str(e)}"
            print(error_message)
            details = error_message

            # Try to write to output even in case of error
            try:
                with open(self.docker_logs_file_path, "w") as f:
                    f.write(f"Task failed: {error_message}")

                with self.output().open("w") as f:
                    f.write("Task failed: " + error_message)
            except Exception:
                pass

            raise

        finally:
            end_time = datetime.datetime.now()
            logger.log_task_timing(
                task_name="DeleteOldMarkerFiles",
                start_time=start_time,
                end_time=end_time,
                status=final_status,
                details=details,
            )


class LogFileCleanup(pu.TimeoutMixin, luigi.Task):
    log_directory = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs"
    days_to_keep = luigi.IntParameter(default=15)
    file_pattern = "log_*.txt"

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get("ieasyforecast_intermediate_data_path"))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_dockerLogsFileCleanup_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    timeout_seconds = luigi.OptionalIntParameter(default=None)
    max_retries = luigi.OptionalIntParameter(default=None)
    retry_delay = luigi.OptionalIntParameter(default=None)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Get parameters from timeout manager
        task_name = self.__class__.__name__
        task_params = get_task_parameters(task_name)

        if self.timeout_seconds is None:
            self.timeout_seconds = task_params["timeout_seconds"]

        if self.max_retries is None:
            self.max_retries = task_params["max_retries"]

        if self.retry_delay is None:
            self.retry_delay = task_params["retry_delay"]

    def output(self):
        return luigi.LocalTarget("/app/log_cleanuplogs.txt")

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
                except Exception:
                    failed_count += 1

            # Write summary to output file
            with open(self.docker_logs_file_path, "w") as f:
                summary = {
                    "timestamp": datetime.datetime.now().isoformat(),
                    "log_directory": self.log_directory,
                    "file_pattern": self.file_pattern,
                    "days_to_keep": self.days_to_keep,
                    "cutoff_date": cutoff_date.isoformat(),
                    "total_files_found": len(log_files),
                    "files_deleted": deleted_count,
                    "failures": failed_count,
                }
                for key, value in summary.items():
                    f.write(f"{key}: {value}\n")
            status = "Success"
            details = f"Deleted {deleted_count} files, {failed_count} failures"

            # Create the output marker file
            with self.output().open("w") as f:
                f.write("Task completed")

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
                details=details,
            )


class SendPipelineCompletionNotification(luigi.Task):
    """Send notification when the entire pipeline is complete."""

    # Custom message parameter
    custom_message = luigi.Parameter(default="")

    # Declared so Luigi's metaclass accepts the kwarg; actual task list
    # is handled by __init__ / self._depends_on. Must be a string default
    # — a non-string default (e.g. []) gets serialized as "()" and crashes
    # the remote scheduler.
    depends_on = luigi.Parameter(default="")

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get("ieasyforecast_intermediate_data_path"))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_sendPipelineCompletionNotification_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def __init__(self, *args, depends_on=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._depends_on = depends_on or []

    def requires(self):
        return self._depends_on

    def output(self):
        return luigi.LocalTarget("/app/log_notification.txt")

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
            email_recipients_str = os.getenv("SAPPHIRE_PIPELINE_EMAIL_RECIPIENTS", "")
            if email_recipients_str:
                email_recipients = [email.strip() for email in email_recipients_str.split(",")]
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
            dashboard_url = os.getenv("ieasyhydroforecast_url", "")
            if dashboard_url:
                message += f"View the latest forecasts on the dashboard: {dashboard_url}\n\n"

            # Add a summary of tasks that were run
            message += f"Tasks completed for {ORGANIZATION.upper()}:\n"
            if ORGANIZATION == "demo":
                message += "- PreprocessingRunoff\n"
                message += "- LinearRegression\n"
                message += "- PostProcessingForecasts\n"
                message += "- LogFileCleanup\n"
            elif ORGANIZATION == "kghm":
                message += "- PreprocessingRunoff\n"
                message += "- LinearRegression\n"
                message += "- PostProcessingForecasts\n"
                message += "- RunAllMLModels\n"
                message += "- ConceptualModel\n"
                message += "- LogFileCleanup\n"
                message += "- DeleteOldGatewayFiles\n"

            elif ORGANIZATION == "uzhm":
                message += "- PreprocessingRunoff\n"
                message += "- LinearRegression\n"
                message += "- PostProcessingForecasts\n"
                message += "- LogFileCleanup\n"

            message += "\nThis is an automated notification."

            # Send email notifications if recipients are specified
            if email_recipients:
                # You could also attach summary files or plots here
                attachment_paths = []

                email_success = NotificationManager.send_email(
                    recipients=email_recipients,
                    subject=subject,
                    message=message,
                    attachment_paths=attachment_paths,
                )

                if email_success:
                    notification_results.append(f"Email sent to {', '.join(email_recipients)}")
                else:
                    notification_results.append(
                        f"Failed to send email to {', '.join(email_recipients)}"
                    )
                    success = False
            else:
                notification_results.append("No email recipients configured")

            # Write output
            with open(self.docker_logs_file_path, "w") as f:
                f.write(f"Notification task completed at {current_time}\n\n")
                f.write("\n".join(notification_results))

            # Create the output marker file
            with self.output().open("w") as f:
                f.write("Task completed")

        except Exception as e:
            print(f"Error sending notifications: {str(e)}")
            success = False

            with open(self.docker_logs_file_path, "w") as f:
                f.write(f"Notification task failed: {str(e)}")

        finally:
            end_time = datetime.datetime.now()
            status = "Success" if success else "Failed"
            details = (
                ", ".join(notification_results) if notification_results else "No notifications sent"
            )

            logger.log_task_timing(
                task_name="SendPipelineCompletionNotification",
                start_time=start_time,
                end_time=end_time,
                status=status,
                details=details,
            )


class RunPentadalWorkflow(luigi.Task):
    """Workflow for pentadal forecasting."""

    # Parameters for notifications
    custom_message = luigi.Parameter(default="")
    send_notifications = luigi.BoolParameter(default=True)

    # Use the intermediate_data_path for log files
    intermediate_data_path = get_bind_path(env.get("ieasyforecast_intermediate_data_path"))
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_pentadal_workflow_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def requires(self):
        # Base tasks for pentadal forecasting
        base_tasks = []

        # Always include Linear Regression
        base_tasks.append(LinearRegression(prediction_mode="PENTAD"))

        # Add ML models if enabled
        if RUN_ML_MODELS == "True":
            models = env.get("ieasyhydroforecast_available_ML_models").split(",")
            for model in models:
                base_tasks.append(
                    RunMLModel(model_type=model, prediction_mode="PENTAD", run_mode="forecast")
                )

        # Add Conceptual Model if enabled
        if RUN_CM_MODELS == "True":
            base_tasks.append(ConceptualModel())

        # Add post-processing after all forecasts
        base_tasks.append(PostProcessingForecasts(prediction_mode="PENTAD"))

        # Add cleanup tasks
        base_tasks.append(LogFileCleanup())
        base_tasks.append(DeleteOldMarkerFiles())
        if RUN_ML_MODELS == "True" or RUN_CM_MODELS == "True":
            base_tasks.append(DeleteOldGatewayFiles())

        # If notifications are enabled, wrap with notification task
        if self.send_notifications:
            return SendPipelineCompletionNotification(
                custom_message=f"PENTAD {self.custom_message}", depends_on=base_tasks
            )
        else:
            return base_tasks

    def output(self):
        return luigi.LocalTarget("/app/log_pentadal_workflow_complete.txt")

    def run(self):
        print("Pentadal workflow completed.")

        with open(self.docker_logs_file_path, "w") as f:
            f.write(
                f"Pentadal workflow for {ORGANIZATION} completed at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        with self.output().open("w") as f:
            f.write("Pentadal workflow completed")


class RunDecadalWorkflow(luigi.Task):
    """Workflow for decadal forecasting."""

    # Parameters for notifications
    custom_message = luigi.Parameter(default="")
    send_notifications = luigi.BoolParameter(default=True)

    # Use the intermediate_data_path for log files
    intermediate_data_path = get_bind_path(env.get("ieasyforecast_intermediate_data_path"))
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_decadal_workflow_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def requires(self):
        # Base tasks for decadal forecasting
        base_tasks = []

        # Always include Linear Regression
        base_tasks.append(LinearRegression(prediction_mode="DECAD"))

        # Add ML models if enabled
        if RUN_ML_MODELS == "True":
            models = env.get("ieasyhydroforecast_available_ML_models").split(",")
            for model in models:
                base_tasks.append(
                    RunMLModel(model_type=model, prediction_mode="DECAD", run_mode="forecast")
                )

        # Add Conceptual Model if enabled
        if RUN_CM_MODELS == "True":
            base_tasks.append(ConceptualModel())

        # Add post-processing after all forecasts
        base_tasks.append(PostProcessingForecasts(prediction_mode="DECAD"))

        # Add cleanup tasks
        base_tasks.append(LogFileCleanup())
        base_tasks.append(DeleteOldMarkerFiles())
        if RUN_ML_MODELS == "True" or RUN_CM_MODELS == "True":
            base_tasks.append(DeleteOldGatewayFiles())

        # If notifications are enabled, wrap with notification task
        if self.send_notifications:
            return SendPipelineCompletionNotification(
                custom_message=f"DECAD {self.custom_message}", depends_on=base_tasks
            )
        else:
            return base_tasks

    def output(self):
        return luigi.LocalTarget("/app/log_decadal_workflow_complete.txt")

    def run(self):
        print("Decadal workflow completed.")

        with open(self.docker_logs_file_path, "w") as f:
            f.write(
                f"Decadal workflow for {ORGANIZATION} completed at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        with self.output().open("w") as f:
            f.write("Decadal workflow completed")


class RunWorkflow(luigi.Task):
    """Main wrapper task that runs the entire forecast pipeline."""

    """This task is being deprecated in favor of RunPentadalWorkflow and RunDecadalWorkflow."""

    # Parameters for notifications
    custom_message = luigi.Parameter(default="")

    # Flag to control whether to send notifications
    send_notifications = luigi.BoolParameter(default=True)

    mode = luigi.Parameter(default="ALL")  # ALL, PENTAD, or DECAD

    # Use the intermediate_data_path for log files instead of /app/
    intermediate_data_path = get_bind_path(env.get("ieasyforecast_intermediate_data_path"))
    # Define the logging output of the task.
    docker_logs_file_path = f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/docker_logs/log_runWorkflow_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

    def requires(self):
        # Test if directory of docker_logs_file_path exists and create it if not
        os.makedirs(os.path.dirname(self.docker_logs_file_path), exist_ok=True)

        if self.mode == "PENTAD":
            return RunPentadalWorkflow(
                custom_message=self.custom_message, send_notifications=self.send_notifications
            )
        elif self.mode == "DECAD":
            return RunDecadalWorkflow(
                custom_message=self.custom_message, send_notifications=self.send_notifications
            )
        else:  # ALL or default
            # Run both workflows
            return [
                RunPentadalWorkflow(
                    custom_message=self.custom_message, send_notifications=self.send_notifications
                ),
                RunDecadalWorkflow(
                    custom_message=self.custom_message, send_notifications=self.send_notifications
                ),
            ]

    def output(self):
        return luigi.LocalTarget("/app/log_workflow_complete.txt")

    def run(self):
        print("Workflow completed.")

        # Create output file to mark completion
        with open(self.docker_logs_file_path, "w") as f:
            f.write(
                f"Workflow for {ORGANIZATION} completed at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        # Create the output marker file
        with self.output().open("w") as f:
            f.write("Task completed")


# =============================================================================
# Maintenance Tasks
#
# These tasks replicate the behavior of the bin/daily_*.sh and bin/yearly_*.sh
# maintenance scripts as Luigi tasks. This formalizes dependencies between
# maintenance steps and provides retry logic, notifications, and marker files.
#
# The original shell scripts remain functional for manual invocation.
# =============================================================================


def _common_maintenance_env():
    """Environment variables shared by all maintenance containers."""
    base = [
        f"ieasyhydroforecast_env_file_path={env_file_path}",
        "SAPPHIRE_OPDEV_ENV=True",
        "IN_DOCKER=True",
    ]
    base.extend(get_docker_host_env_overrides())
    return base


def _standard_maintenance_volumes(extra_volume_keys=None):
    """Standard volume mounts for maintenance containers.

    Args:
        extra_volume_keys: Additional env-var keys to mount (e.g.,
            'ieasyforecast_daily_discharge_path').
    """
    keys = [
        "ieasyforecast_configuration_path",
        "ieasyforecast_intermediate_data_path",
    ]
    if extra_volume_keys:
        keys.extend(extra_volume_keys)
    return setup_docker_volumes(env, keys)


class GatewayMaintenance(DockerTaskBase):
    """Run preprocessing gateway in maintenance mode (30-day lookback)."""

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_maintenance_gateway_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    def requires(self):
        return []

    def output(self):
        marker = get_maintenance_marker_filepath("gateway")
        return luigi.LocalTarget(marker)

    def run(self):
        volumes = _standard_maintenance_volumes()
        environment = _common_maintenance_env() + [
            "SAPPHIRE_SYNC_MODE=maintenance",
        ]

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-prepgateway",
                container_name="maintenance-gateway",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="4g",
                memswap_limit="6g",
            )
        )


class PrepRunoffMaintenance(DockerTaskBase):
    """Run preprocessing runoff in maintenance mode (30-day lookback)."""

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_maintenance_preprunoff_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    def requires(self):
        return []

    def output(self):
        marker = get_maintenance_marker_filepath("preprunoff")
        return luigi.LocalTarget(marker)

    def run(self):
        volumes = _standard_maintenance_volumes(
            extra_volume_keys=["ieasyforecast_daily_discharge_path"]
        )
        gsheets_enabled = os.environ.get("GOOGLE_SHEETS_ENABLED", "")
        gsheets_id = os.environ.get("GOOGLE_SHEETS_DISCHARGE_ID", "")
        gsheets_creds = os.environ.get("GOOGLE_SHEETS_CREDENTIALS_PATH", "")
        gsheets_codes = os.environ.get("GOOGLE_SHEETS_SITE_CODES", "")
        environment = _common_maintenance_env() + [
            "SAPPHIRE_SYNC_MODE=maintenance",
            f"GOOGLE_SHEETS_ENABLED={gsheets_enabled}",
            f"GOOGLE_SHEETS_DISCHARGE_ID={gsheets_id}",
            f"GOOGLE_SHEETS_CREDENTIALS_PATH={gsheets_creds}",
            f"GOOGLE_SHEETS_SITE_CODES={gsheets_codes}",
        ]

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-preprunoff",
                container_name="maintenance-preprunoff",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="4g",
                memswap_limit="6g",
            )
        )


class LinRegMaintenance(DockerTaskBase):
    """Run linear regression in maintenance (hindcast) mode."""

    prediction_mode = luigi.Parameter()  # PENTAD or DECAD

    @property
    def docker_logs_file_path(self):
        return (
            f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
            f"/docker_logs/log_maintenance_linreg_{self.prediction_mode}_"
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

    def requires(self):
        return PrepRunoffMaintenance()

    def output(self):
        marker = get_maintenance_marker_filepath(f"linreg_{self.prediction_mode}")
        return luigi.LocalTarget(marker)

    def run(self):
        volumes = _standard_maintenance_volumes(
            extra_volume_keys=["ieasyforecast_daily_discharge_path"]
        )
        environment = _common_maintenance_env() + [
            f"SAPPHIRE_PREDICTION_MODE={self.prediction_mode}",
            "RUN_MODE=maintenance",
            "SAPPHIRE_SYNC_MODE=maintenance",
        ]

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-linreg",
                container_name=f"maintenance-linreg-{self.prediction_mode}",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="4g",
                memswap_limit="6g",
            )
        )


class MLMaintenance(DockerTaskBase):
    """Run a single ML model in maintenance mode.

    Uses Luigi resources to serialize ML maintenance containers.
    """

    model_type = luigi.Parameter()
    prediction_mode = luigi.Parameter()

    resources = {"ml_memory": 1}

    @property
    def docker_logs_file_path(self):
        return (
            f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
            f"/docker_logs/log_maintenance_ml_{self.model_type}_"
            f"{self.prediction_mode}_"
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

    def requires(self):
        return [PrepRunoffMaintenance(), GatewayMaintenance()]

    def output(self):
        marker = get_maintenance_marker_filepath(f"ml_{self.model_type}_{self.prediction_mode}")
        return luigi.LocalTarget(marker)

    def run(self):
        volumes = _standard_maintenance_volumes()
        environment = _common_maintenance_env() + [
            f"SAPPHIRE_MODEL_TO_USE={self.model_type}",
            f"SAPPHIRE_PREDICTION_MODE={self.prediction_mode}",
            "RUN_MODE=maintenance",
        ]

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-ml",
                container_name=(f"maintenance-ml-{self.model_type}-{self.prediction_mode}"),
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="12g",
                memswap_limit="16g",
                network="host",
            )
        )


class PostProcessingMaintenance(DockerTaskBase):
    """Run postprocessing in maintenance mode (gap-fill ensembles)."""

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_maintenance_postproc_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    def requires(self):
        deps = [
            LinRegMaintenance(prediction_mode="PENTAD"),
            LinRegMaintenance(prediction_mode="DECAD"),
        ]
        if RUN_ML_MODELS == "True":
            models = env.get("ieasyhydroforecast_available_ML_models").split(",")
            for model in models:
                for mode in ["PENTAD", "DECAD"]:
                    deps.append(MLMaintenance(model_type=model, prediction_mode=mode))
        return deps

    def output(self):
        marker = get_maintenance_marker_filepath("postproc")
        return luigi.LocalTarget(marker)

    def run(self):
        volumes = _standard_maintenance_volumes()
        environment = _common_maintenance_env() + [
            "SAPPHIRE_PREDICTION_MODE=BOTH",
        ]

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-postprocessing",
                container_name="maintenance-postproc",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="4g",
                memswap_limit="6g",
                command=["uv", "run", "postprocessing_maintenance.py"],
                network="host",
            )
        )


class RunDailyMaintenanceWorkflow(luigi.Task):
    """Top-level daily maintenance orchestrator.

    Triggers the full maintenance dependency chain:
    PostProcessingMaintenance (-> LinReg -> PrepRunoff, ML -> Gateway)

    Frontend update is handled by bin/daily_update_sapphire_frontend.sh
    on the host after the Luigi pipeline completes.
    """

    send_notifications = luigi.BoolParameter(default=True)

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_daily_maintenance_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    def requires(self):
        tasks = [PostProcessingMaintenance()]
        if self.send_notifications:
            return SendPipelineCompletionNotification(
                custom_message="Daily maintenance completed",
                depends_on=tasks,
            )
        return tasks

    def output(self):
        return luigi.LocalTarget("/app/log_daily_maintenance_complete.txt")

    def run(self):
        os.makedirs(os.path.dirname(self.docker_logs_file_path), exist_ok=True)
        with open(self.docker_logs_file_path, "w") as f:
            f.write(
                f"Daily maintenance completed at "
                f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        with self.output().open("w") as f:
            f.write("Daily maintenance completed")


# --- Periodic maintenance tasks ---


class LongTermPostProcessingMaintenance(DockerTaskBase):
    """Bimonthly long-term postprocessing (monthly ensemble gap-fill)."""

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_maintenance_lt_postproc_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    def output(self):
        marker = get_maintenance_marker_filepath("lt_postproc")
        return luigi.LocalTarget(marker)

    def run(self):
        volumes = _standard_maintenance_volumes()
        environment = _common_maintenance_env()

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-postprocessing",
                container_name="maintenance-lt-postproc",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="8g",
                memswap_limit="12g",
                command=["uv", "run", "postprocessing_maintenance_long_term.py"],
                network="host",
            )
        )


class YearlySkillRecalculation(DockerTaskBase):
    """Annual full recalculation of all skill metrics."""

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_maintenance_skill_recalc_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    def output(self):
        marker = get_maintenance_marker_filepath("skill_recalc")
        return luigi.LocalTarget(marker)

    def run(self):
        volumes = _standard_maintenance_volumes()
        environment = _common_maintenance_env() + [
            "SAPPHIRE_PREDICTION_MODE=BOTH",
        ]

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-postprocessing",
                container_name="maintenance-skill-recalc",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="8g",
                memswap_limit="12g",
                command=["uv", "run", "recalculate_skill_metrics.py"],
                network="host",
            )
        )


class YearlySnowNormRecalculation(DockerTaskBase):
    """Annual snow norm recalculation from historical reanalysis CSVs."""

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_maintenance_snow_norms_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    def output(self):
        marker = get_maintenance_marker_filepath("snow_norms")
        return luigi.LocalTarget(marker)

    def run(self):
        volumes = _standard_maintenance_volumes()
        environment = _common_maintenance_env()

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-pipeline",
                container_name="maintenance-snow-norms",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="4g",
                memswap_limit="6g",
                command=["uv", "run", "recalculate_snow_norms.py"],
                network="host",
            )
        )


class RunPeriodicMaintenanceWorkflow(luigi.Task):
    """Parameterized workflow for periodic maintenance tasks.

    Args:
        task_type: One of 'long_term', 'skill_recalc', 'snow_norms'
    """

    task_type = luigi.Parameter()

    def requires(self):
        task_map = {
            "long_term": LongTermPostProcessingMaintenance(),
            "skill_recalc": YearlySkillRecalculation(),
            "snow_norms": YearlySnowNormRecalculation(),
        }
        if self.task_type not in task_map:
            raise ValueError(
                f"Unknown task_type '{self.task_type}'. Expected one of: {list(task_map.keys())}"
            )
        return task_map[self.task_type]

    def output(self):
        return luigi.LocalTarget(f"/app/log_periodic_maintenance_{self.task_type}_complete.txt")

    def run(self):
        with self.output().open("w") as f:
            f.write(
                f"Periodic maintenance ({self.task_type}) completed at "
                f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )


# --- Operational long-term forecasting tasks ---


class LTScheduleQuery(DockerTaskBase):
    """Run lt_schedule_query.py to determine which long-term modes are active.

    Writes JSON result to the shared intermediate_data volume so that
    RunLongTermWorkflow can read it via _read_schedule_result().
    """

    today = luigi.Parameter(default="")  # optional date override for testing

    SCHEDULE_RESULT_PATH = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}/lt_schedule_result.json"
    )

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_lt_schedule_query_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    # No resources — do NOT add lt_memory here (would deadlock).

    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget("/app/log_schedule_query.txt")

    def run(self):
        # Delete stale JSON from a previous pipeline run. If the container
        # fails to start, the old file must not be silently reused.
        if os.path.exists(self.SCHEDULE_RESULT_PATH):
            os.remove(self.SCHEDULE_RESULT_PATH)

        volumes = setup_docker_volumes(
            env,
            [
                "ieasyforecast_configuration_path",
                "ieasyforecast_intermediate_data_path",
            ],
        )

        environment = [
            f"ieasyhydroforecast_env_file_path={env_file_path}",
            "IN_DOCKER=True",
        ]
        # Do NOT add RUN_MODE=forecast — this is a schedule query, not a
        # forecast run.
        # Do NOT call get_docker_host_env_overrides() — the schedule query
        # reads local config files only.

        base_cmd = "uv run python lt_schedule_query.py"
        if self.today:
            base_cmd += f" --today {self.today}"
        command = ["sh", "-c", f"{base_cmd} > {self.SCHEDULE_RESULT_PATH}"]

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-lt-forecasting",
                container_name=f"lt_schedule_query_{attempt}",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                command=command,
                mem_limit="2g",
                network="sapphire_sapphire-network",
            )
        )


class RunLongTermForecast(DockerTaskBase):
    """Run a single long-term forecast mode (e.g. month_0, quarter).

    Parameterized per-mode task. The bash entry script determines which
    modes are active via lt_schedule_query.py and passes them as Luigi
    parameters.
    """

    forecast_mode = luigi.Parameter()  # e.g. "month_0", "quarter"

    resources = {"lt_memory": 1}  # serialize long-term runs (memory)

    @property
    def docker_logs_file_path(self):
        return (
            f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
            f"/docker_logs/log_lt_forecast_{self.forecast_mode}_"
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

    def requires(self):
        return [PreprocessingRunoff(), get_gateway_dependency()]

    def output(self):
        return luigi.LocalTarget(f"/app/log_lt_forecast_{self.forecast_mode}.txt")

    def run(self):
        volumes = setup_docker_volumes(
            env,
            [
                "ieasyforecast_configuration_path",
                "ieasyforecast_intermediate_data_path",
            ],
        )

        environment = [
            f"ieasyhydroforecast_env_file_path={env_file_path}",
            "IN_DOCKER=True",
            f"lt_forecast_mode={self.forecast_mode}",
            "RUN_MODE=forecast",
        ]
        environment.extend(get_docker_host_env_overrides())
        environment.append("SAPPHIRE_API_URL=http://api-gateway:8000")

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-lt-forecasting",
                container_name=f"lt_forecast_{self.forecast_mode}_{attempt}",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="12g",
                memswap_limit="16g",
                network="sapphire_sapphire-network",
            )
        )


class LongTermPostProcessing(DockerTaskBase):
    """Operational postprocessing after all long-term forecast modes complete."""

    active_modes = luigi.Parameter()  # comma-separated, e.g. "month_0,quarter"
    skill_metric_types = luigi.Parameter(default="MONTHLY")

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_lt_postprocessing_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    def requires(self):
        modes = [m.strip() for m in self.active_modes.split(",") if m.strip()]
        return [RunLongTermForecast(forecast_mode=mode) for mode in modes]

    def output(self):
        return luigi.LocalTarget("/app/log_lt_postprocessing.txt")

    def run(self):
        volumes = _standard_maintenance_volumes()
        environment = _common_maintenance_env()
        environment.append(f"SAPPHIRE_SKILL_METRIC_TYPES={self.skill_metric_types}")
        environment.append("SAPPHIRE_API_URL=http://api-gateway:8000")

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-postprocessing",
                container_name=f"lt-postprocessing_{attempt}",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="8g",
                memswap_limit="12g",
                command=[
                    "uv",
                    "run",
                    "postprocessing_operational_long_term.py",
                ],
                network="sapphire_sapphire-network",
            )
        )


def _read_schedule_result():
    """Read and validate the schedule query JSON from the shared volume.

    Raises RuntimeError with diagnostic paths if the file is missing,
    empty, or malformed.
    """
    path = LTScheduleQuery.SCHEDULE_RESULT_PATH
    log_path = LTScheduleQuery.docker_logs_file_path

    if not os.path.exists(path):
        raise RuntimeError(
            f"Schedule result file not found at {path}. Check schedule query logs at {log_path}"
        )

    with open(path) as f:
        content = f.read()

    if not content.strip():
        raise RuntimeError(
            f"Schedule result file is empty at {path}. Check schedule query logs at {log_path}"
        )

    try:
        result = json.loads(content)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Schedule result file contains invalid JSON at {path}: {e}. "
            f"Check schedule query logs at {log_path}"
        ) from e

    # Validate required keys and types
    for key in ("active_modes", "skill_metric_types", "skipped_modes"):
        if key not in result:
            raise RuntimeError(
                f"Schedule result missing required key '{key}' at {path}. "
                f"Check schedule query logs at {log_path}"
            )

    if not isinstance(result["active_modes"], list) or not all(
        isinstance(m, str) for m in result["active_modes"]
    ):
        raise RuntimeError(
            f"active_modes must be a list of strings, got {result['active_modes']!r} at {path}"
        )

    if not isinstance(result["skill_metric_types"], list) or not all(
        isinstance(t, str) for t in result["skill_metric_types"]
    ):
        raise RuntimeError(
            f"skill_metric_types must be a list of strings, got "
            f"{result['skill_metric_types']!r} at {path}"
        )

    if not isinstance(result["skipped_modes"], dict):
        raise RuntimeError(
            f"skipped_modes must be a dict, got {type(result['skipped_modes']).__name__} at {path}"
        )

    return result


class RunLongTermWorkflow(luigi.Task):
    """Top-level orchestrator for operational long-term forecasting.

    When active_modes is empty (default), runs LTScheduleQuery to
    determine which modes are active today, then dispatches forecast
    tasks via dynamic dependencies (yield in run()). When active_modes
    is provided explicitly, skips the schedule query and uses the
    given modes directly.
    """

    active_modes = luigi.Parameter(default="")  # comma-separated override
    skill_metric_types = luigi.Parameter(default="MONTHLY")
    send_notifications = luigi.BoolParameter(default=True)
    custom_message = luigi.Parameter(default="")

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_long_term_workflow_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    def _parse_override_modes(self):
        """Parse active_modes parameter into a clean list.

        Returns empty list if active_modes is empty, whitespace-only,
        or contains only separators (e.g. ","). Both requires() and
        run() use this to make a consistent override-vs-schedule
        decision — they must never diverge.
        """
        if not self.active_modes.strip():
            return []
        return [m.strip() for m in self.active_modes.split(",") if m.strip()]

    def requires(self):
        if not self._parse_override_modes():
            return LTScheduleQuery()
        return []

    def output(self):
        return luigi.LocalTarget("/app/log_long_term_workflow_complete.txt")

    def run(self):
        # --- Step 1: Determine active modes ---
        # _parse_override_modes() is shared with requires() to ensure
        # the override-vs-schedule decision is always consistent.
        override_modes = self._parse_override_modes()

        if override_modes:
            # Override path: modes provided directly, no schedule query
            modes = override_modes
            skill_types = self.skill_metric_types
            if skill_types == "MONTHLY" and any(m not in ("month_0",) for m in modes):
                print(
                    "Warning: active_modes provided manually but "
                    "skill_metric_types defaults to MONTHLY. "
                    "Pass --skill-metric-types if needed."
                )
        else:
            # Schedule query path: read result from shared volume
            schedule = _read_schedule_result()
            modes = schedule["active_modes"]
            skill_types = ",".join(schedule["skill_metric_types"])  # always present in JSON

        # --- Step 1b: Log schedule decisions for operator visibility ---
        if not override_modes and modes:
            print(f"Schedule query: {len(modes)} active mode(s): {modes}")
            for mode, reason in schedule.get("skipped_modes", {}).items():
                print(f"  Skipped {mode}: {reason}")
        elif override_modes:
            print(f"Override: using manually provided modes: {modes}")

        # --- Step 2: Early exit if nothing to do ---
        if not modes:
            print("No long-term forecast modes active today.")
            if not override_modes:
                for mode, reason in schedule.get("skipped_modes", {}).items():
                    print(f"  Skipped {mode}: {reason}")
            os.makedirs(os.path.dirname(self.docker_logs_file_path), exist_ok=True)
            with open(self.docker_logs_file_path, "w") as f:
                f.write("No active modes today")
            with self.output().open("w") as f:
                f.write("No active modes today")
            return

        # --- Step 3: Build task list (same logic as old requires()) ---
        modes_str = ",".join(modes)
        base_tasks = []
        for mode in modes:
            base_tasks.append(RunLongTermForecast(forecast_mode=mode))
        base_tasks.append(
            LongTermPostProcessing(active_modes=modes_str, skill_metric_types=skill_types)
        )
        base_tasks.append(LogFileCleanup())
        base_tasks.append(DeleteOldMarkerFiles())

        # --- Step 4: Yield dynamic dependencies ---
        # Yield base_tasks first. Luigi pauses the generator until all
        # complete, so the notification runs only after forecasts finish.
        # Do NOT pass depends_on=base_tasks — Luigi Parameters serialize
        # Task objects to strings, which crashes the remote scheduler.
        yield base_tasks

        if self.send_notifications:
            yield SendPipelineCompletionNotification(
                custom_message=f"LONG_TERM {self.custom_message}",
            )

        # --- Step 5: Guard + write completion markers ---
        # Luigi resumes the generator only after all yielded deps are
        # complete. The guard is defense-in-depth against the pre-existing
        # exit-code bug (DockerTaskBase reports exit_status=0
        # unconditionally).
        for mode in modes:
            marker = f"/app/log_lt_forecast_{mode}.txt"
            if not os.path.exists(marker):
                raise RuntimeError(
                    f"Forecast task for mode '{mode}' did not produce "
                    f"marker {marker}. Check Luigi web UI for "
                    f"UPSTREAM_FAILED status."
                )

        os.makedirs(os.path.dirname(self.docker_logs_file_path), exist_ok=True)
        with open(self.docker_logs_file_path, "w") as f:
            f.write(
                f"Long-term workflow for {ORGANIZATION} completed at "
                f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        with self.output().open("w") as f:
            f.write("Long-term workflow completed")


# =============================================================================
# Initialization tasks
#
# These tasks perform first-time database population for a new SAPPHIRE
# deployment: full historical data sync, hindcast from a configurable start
# date, and skill metrics computation.
#
# Workflow dependency chain:
#   PrepRunoffMaintenance → InitialApiSync → LinRegInitial(PENTAD/DECAD)
#   → SkillMetricsInitial(PENTAD/DECAD) → RunInitializeWorkflow
# =============================================================================


class InitialApiSync(DockerTaskBase):
    """Push full CSV history to the preprocessing API.

    Runs initial_api_sync.py which reads all cached CSV files (daily,
    pentadal, decadal) and writes them to the API with
    SAPPHIRE_SYNC_MODE=initial (all records, no date filter).

    Must run after PrepRunoffMaintenance (which populates the CSV cache).
    """

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_initial_api_sync_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    def requires(self):
        return PrepRunoffMaintenance()

    def output(self):
        marker = get_marker_filepath("initial_api_sync")
        return luigi.LocalTarget(marker)

    def run(self):
        volumes = _standard_maintenance_volumes(
            extra_volume_keys=["ieasyforecast_daily_discharge_path"]
        )
        gsheets_enabled = os.environ.get("GOOGLE_SHEETS_ENABLED", "")
        gsheets_id = os.environ.get("GOOGLE_SHEETS_DISCHARGE_ID", "")
        gsheets_creds = os.environ.get("GOOGLE_SHEETS_CREDENTIALS_PATH", "")
        gsheets_codes = os.environ.get("GOOGLE_SHEETS_SITE_CODES", "")
        environment = _common_maintenance_env() + [
            "SAPPHIRE_SYNC_MODE=initial",
            f"GOOGLE_SHEETS_ENABLED={gsheets_enabled}",
            f"GOOGLE_SHEETS_DISCHARGE_ID={gsheets_id}",
            f"GOOGLE_SHEETS_CREDENTIALS_PATH={gsheets_creds}",
            f"GOOGLE_SHEETS_SITE_CODES={gsheets_codes}",
        ]

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-preprunoff",
                container_name="initial-api-sync",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="4g",
                memswap_limit="6g",
                command=["uv", "run", "initial_api_sync.py"],
            )
        )


class LinRegInitial(DockerTaskBase):
    """Run hindcast with explicit start date for initialization.

    Unlike LinRegMaintenance (which uses the Docker CMD's RUN_MODE
    conditional and auto-detects the start date), this task passes
    --start-date explicitly via a command override.
    """

    prediction_mode = luigi.Parameter()  # PENTAD or DECAD

    @property
    def docker_logs_file_path(self):
        return (
            f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
            f"/docker_logs/log_initial_linreg_{self.prediction_mode}_"
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

    def requires(self):
        return InitialApiSync()

    def output(self):
        marker = get_marker_filepath(f"initial_linreg_{self.prediction_mode}")
        return luigi.LocalTarget(marker)

    def run(self):
        start_date = os.environ.get("ieasyhydroforecast_START_DATE", "")
        if not start_date:
            raise ValueError("ieasyhydroforecast_START_DATE must be set for initialization")

        volumes = _standard_maintenance_volumes(
            extra_volume_keys=["ieasyforecast_daily_discharge_path"]
        )
        environment = _common_maintenance_env() + [
            f"SAPPHIRE_PREDICTION_MODE={self.prediction_mode}",
            "SAPPHIRE_SYNC_MODE=initial",
        ]

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-linreg",
                container_name=f"initial-linreg-{self.prediction_mode}",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="4g",
                memswap_limit="6g",
                command=[
                    "uv",
                    "run",
                    "linear_regression.py",
                    "--hindcast",
                    "--start-date",
                    start_date,
                ],
            )
        )


class SkillMetricsInitial(DockerTaskBase):
    """Recalculate skill metrics for a single prediction mode after initialization hindcast."""

    prediction_mode = luigi.Parameter()  # PENTAD or DECAD

    @property
    def docker_logs_file_path(self):
        return (
            f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
            f"/docker_logs/log_initial_skill_{self.prediction_mode}_"
            f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )

    def requires(self):
        return LinRegInitial(prediction_mode=self.prediction_mode)

    def output(self):
        marker = get_marker_filepath(f"initial_skill_{self.prediction_mode}")
        return luigi.LocalTarget(marker)

    def run(self):
        volumes = _standard_maintenance_volumes()
        environment = _common_maintenance_env() + [
            f"SAPPHIRE_PREDICTION_MODE={self.prediction_mode}",
        ]

        status, details = self.execute_with_retries(
            lambda attempt: self.run_docker_container(
                image_name="sapphire-postprocessing",
                container_name=f"initial-skill-{self.prediction_mode}",
                volumes=volumes,
                environment=environment,
                attempt_number=attempt,
                mem_limit="8g",
                memswap_limit="12g",
                command=["uv", "run", "recalculate_skill_metrics.py"],
                network="host",
            )
        )


class RunInitializeWorkflow(luigi.Task):
    """Top-level initialization orchestrator.

    Triggers the full initialization dependency chain:
    PrepRunoffMaintenance → InitialApiSync → LinRegInitial(PENTAD/DECAD)
    → SkillMetricsInitial(PENTAD/DECAD)

    Usage:
        ieasyhydroforecast_START_DATE=2000-01-06 \\
        luigi --module pipeline_docker RunInitializeWorkflow
    """

    docker_logs_file_path = (
        f"{get_bind_path(env.get('ieasyforecast_intermediate_data_path'))}"
        f"/docker_logs/log_initialize_workflow_"
        f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    def requires(self):
        return [
            SkillMetricsInitial(prediction_mode="PENTAD"),
            SkillMetricsInitial(prediction_mode="DECAD"),
        ]

    def output(self):
        return luigi.LocalTarget(get_marker_filepath("initial_workflow_complete"))

    def run(self):
        os.makedirs(os.path.dirname(self.docker_logs_file_path), exist_ok=True)
        with open(self.docker_logs_file_path, "w") as f:
            f.write(
                f"Initialization completed at "
                f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
        with self.output().open("w") as f:
            f.write("Initialization completed")


if __name__ == "__main__":
    luigi.build([RunWorkflow()])
