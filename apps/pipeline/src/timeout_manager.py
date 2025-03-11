"""
Timeout configuration manager for the forecast pipeline tasks.
This module provides functionality to determine appropriate timeout
values for different tasks based on the current environment.
"""

import os
import socket
import yaml
from typing import Dict, Any, Optional, Tuple

class TimeoutManager:
    """
    Manages timeout configurations for pipeline tasks across different environments.
    Automatically detects the environment and provides appropriate timeout values.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the timeout manager with the given config path.

        Args:
            config_path: Path to the YAML configuration file. If None, uses
                         environment variable or default location.
        """
        if config_path is None:
            # Try to get config path from environment
            config_path = os.environ.get(
                'IEASYHYDROFORECAST_TIMEOUT_CONFIG_PATH',
                os.path.join(os.environ.get('ieasyforecast_configuration_path', ''),
                            'timeout_config.yaml')
            )

        # Load configuration from YAML file
        try:
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
            print(f"Loaded timeout configuration from: {config_path}")
        except Exception as e:
            print(f"Warning: Could not load timeout configuration from {config_path}: {str(e)}")
            print("Using default timeout values.")
            self.config = {"environments": {}, "tasks": {}}

        # Detect the current environment or use environment variable if set
        self.current_env = self._detect_environment()
        print(f"Detected execution environment: {self.current_env}")

    def _detect_environment(self) -> str:
        """
        Detect which environment we're running in based on organization and image tag.

        Returns:
            String identifier of the current environment.
        """
        # First priority: Check if explicitly set in environment variables
        if 'IEASYHYDROFORECAST_ENVIRONMENT' in os.environ:
            env = os.environ['IEASYHYDROFORECAST_ENVIRONMENT'].lower()
            if env in self.config['environments']:
                return env
            else:
                print(f"Warning: Environment '{env}' from environment variable not found in configuration.")

        # Get the organization and image tag
        org = os.environ.get('ieasyhydroforecast_organization', '').lower()
        image_tag = os.environ.get('ieasyhydroforecast_backend_docker_image_tag', '').lower()

        print(f"Detecting environment based on: organization={org}, image_tag={image_tag}")

        # Define which tags are considered "production" (used on local servers)
        production_tags = ['local']

        # Check if the current tag is a production tag
        is_production = any(tag in image_tag for tag in production_tags)

        # Map organization and production status to environment
        if org == 'demo':
            return 'demo_ch'
        elif org == 'kghm':
            return 'kghm_local' if is_production else 'kghm_aws'
        elif org == 'tjhm':
            return 'tjhm_local' if is_production else 'tjhm_aws'

        # Fallback to demo if we couldn't detect
        print(f"Warning: Could not detect environment from organization '{org}' and image tag '{image_tag}'")
        print("Falling back to demo_ch environment.")
        return 'demo_ch'

    def get_task_parameters(self, task_name: str) -> Dict[str, Any]:
        """
        Get the complete set of parameters for a task, including timeout,
        max retries, and retry delay.

        Args:
            task_name: The name of the task class.

        Returns:
            Dictionary of task parameters.
        """
        # Start with default parameters
        params = {
            'timeout_seconds': 900,  # 15 minutes default
            'max_retries': 2,
            'retry_delay': 5,
            'timeout_config': 'default'
        }

        # Get task-specific overrides if available
        if task_name in self.config.get('tasks', {}):
            task_config = self.config['tasks'][task_name]

            # Check for environment-specific timeout override
            env_override_key = f"{self.current_env}_override"
            if env_override_key in task_config and task_config[env_override_key] is not None:
                params['timeout_seconds'] = task_config[env_override_key]
            elif 'relative_complexity' in task_config:
                # Calculate based on base timeout and relative complexity
                env_config = self.config.get('environments', {}).get(self.current_env, {})
                base_timeout = env_config.get('base_timeout', 900)
                relative_complexity = task_config.get('relative_complexity', 1.0)
                params['timeout_seconds'] = int(base_timeout * relative_complexity)

            # Add other task parameters if specified
            if 'max_retries' in task_config:
                params['max_retries'] = task_config['max_retries']
            if 'retry_delay' in task_config:
                params['retry_delay'] = task_config['retry_delay']

        # Get time out config to print to email notification
        params['timeout_config'] = self.current_env

        return params

    def get_timeout_seconds(self, task_name: str) -> int:
        """
        Get just the timeout_seconds for a task.

        Args:
            task_name: The name of the task class.

        Returns:
            Timeout value in seconds.
        """
        return self.get_task_parameters(task_name)['timeout_seconds']

    def get_max_retries(self, task_name: str) -> int:
        """
        Get just the max_retries for a task.

        Args:
            task_name: The name of the task class.

        Returns:
            Maximum number of retries.
        """
        return self.get_task_parameters(task_name)['max_retries']

    def get_retry_delay(self, task_name: str) -> int:
        """
        Get just the retry_delay for a task.

        Args:
            task_name: The name of the task class.

        Returns:
            Delay between retries in seconds.
        """
        return self.get_task_parameters(task_name)['retry_delay']

# Singleton instance for use across the application
_timeout_manager = None

def get_timeout_manager() -> TimeoutManager:
    """
    Get or create the singleton timeout manager instance.

    Returns:
        The timeout manager instance.
    """
    global _timeout_manager
    if _timeout_manager is None:
        _timeout_manager = TimeoutManager()
    return _timeout_manager

def get_task_parameters(task_name: str) -> Dict[str, Any]:
    """
    Convenience function to get all parameters for a task.

    Args:
        task_name: The name of the task class.

    Returns:
        Dictionary with task parameters.
    """
    return get_timeout_manager().get_task_parameters(task_name)