#!/usr/bin/env python
"""
Diagnostic script for timeout configuration validation.
This script checks if the timeout configuration is correctly set up
and prints out the timeout settings for each task in each environment.
"""

import os
import sys
import socket
import yaml
import argparse
from tabulate import tabulate

# Add the project root to the Python path to allow importing modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from apps.pipeline.src.environment import Environment
    from apps.pipeline.src.timeout_manager import TimeoutManager
except ImportError:
    print("Error: Could not import project modules. Make sure you're running this script from the project root.")
    sys.exit(1)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Validate and display timeout configuration settings."
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to the timeout configuration file (default: use environment variable or config path)"
    )
    parser.add_argument(
        "--env",
        type=str,
        help="Environment to display (default: all environments)"
    )
    parser.add_argument(
        "--task",
        type=str,
        help="Task to display (default: all tasks)"
    )
    parser.add_argument(
        "--detect",
        action="store_true",
        help="Try to auto-detect the current environment and highlight its values"
    )
    return parser.parse_args()

def load_config(config_path=None):
    """Load the timeout configuration file."""
    if config_path is None:
        # Try to get config path from environment
        env_file_path = os.getenv('ieasyhydroforecast_env_file_path')
        if env_file_path:
            try:
                env = Environment(env_file_path)
                config_path = env_file_path.replace('.env_develop_kghm', 'timeout_config.yaml')
            except Exception as e:
                print(f"Warning: Could not load Environment: {str(e)}")
                config_path = 'timeout_config.yaml'
        else:
            # Same folder as ieasyhydroforecast_env_file_path but replace filename (.env* -> timeout_config.yaml)
            config_path = env_file_path.replace('.env_develop_kghm', 'timeout_config.yaml')

            config_path = 'timeout_config.yaml'

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        print(f"Loaded timeout configuration from: {config_path}")
        return config
    except Exception as e:
        print(f"Error: Could not load timeout configuration from {config_path}: {str(e)}")
        sys.exit(1)

def detect_environment(config):
    """Try to detect the current environment based on organization and image tag."""
    # Initialize TimeoutManager to use its detection logic
    timeout_manager = TimeoutManager(config)
    detected_env = timeout_manager._detect_environment()

    # Print detection details
    org = os.environ.get('ieasyhydroforecast_organization', 'unknown')
    image_tag = os.environ.get('ieasyhydroforecast_backend_docker_image_tag', 'unknown')
    hostname = socket.gethostname()

    print(f"Organization: {org}")
    print(f"Image Tag: {image_tag}")
    print(f"Hostname: {hostname}")
    print(f"Detected environment: {detected_env}")

    # Show applicable tags for this environment
    if detected_env in config.get('environments', {}):
        applicable_tags = config['environments'][detected_env].get('applicable_tags', [])
        print(f"Applicable tags for this environment: {', '.join(applicable_tags)}")

    return detected_env

def display_task_timeouts(config, env_filter=None, task_filter=None, detected_env=None):
    """Display timeout settings for tasks in a formatted table."""
    environments = config.get('environments', {})
    tasks = config.get('tasks', {})

    # Filter environments if requested
    if env_filter:
        if env_filter in environments:
            environments = {env_filter: environments[env_filter]}
        else:
            print(f"Warning: Environment '{env_filter}' not found in configuration.")
            return

    # Filter tasks if requested
    if task_filter:
        if task_filter in tasks:
            tasks = {task_filter: tasks[task_filter]}
        else:
            print(f"Warning: Task '{task_filter}' not found in configuration.")
            return

    # Display environments information
    print("\nEnvironment Settings:")
    env_headers = ["Environment", "Base Timeout", "Description", "Applicable Tags"]
    env_rows = []

    for env_name, env_config in environments.items():
        is_detected = (detected_env == env_name)
        applicable_tags = env_config.get('applicable_tags', [])

        env_row = [
            f"{env_name} *" if is_detected else env_name,
            env_config.get('base_timeout', 'N/A'),
            env_config.get('description', ''),
            ", ".join(applicable_tags) if applicable_tags else "None specified"
        ]
        env_rows.append(env_row)

    print(tabulate(env_rows, headers=env_headers, tablefmt="grid"))

    # Display task timeout information per environment
    print("\nTask Timeout Settings (in seconds):")

    # Prepare headers for the task table
    task_headers = ["Task"] + list(environments.keys())
    task_rows = []

    for task_name, task_config in tasks.items():
        task_row = [task_name]

        for env_name in environments.keys():
            # Check for environment-specific override
            env_override_key = f"{env_name}_override"
            if env_override_key in task_config and task_config[env_override_key] is not None:
                timeout = task_config[env_override_key]
            elif 'relative_complexity' in task_config:
                # Calculate based on base timeout and relative complexity
                base_timeout = environments.get(env_name, {}).get('base_timeout', 900)
                relative_complexity = task_config.get('relative_complexity', 1.0)
                timeout = int(base_timeout * relative_complexity)
            else:
                timeout = "N/A"

            # Highlight the detected environment
            if detected_env == env_name:
                task_row.append(f"{timeout} *")
            else:
                task_row.append(timeout)

        task_rows.append(task_row)

    print(tabulate(task_rows, headers=task_headers, tablefmt="grid"))

    # Display additional task parameters
    print("\nTask Additional Parameters:")
    param_headers = ["Task", "Relative Complexity", "Max Retries", "Retry Delay"]
    param_rows = []

    for task_name, task_config in tasks.items():
        param_row = [
            task_name,
            task_config.get('relative_complexity', 'N/A'),
            task_config.get('max_retries', 'N/A'),
            task_config.get('retry_delay', 'N/A')
        ]
        param_rows.append(param_row)

    print(tabulate(param_rows, headers=param_headers, tablefmt="grid"))

    if detected_env:
        print("\n* Current detected environment")

def main():
    """Main function to run the diagnostic script."""
    args = parse_args()

    # Load configuration
    config = load_config(args.config)

    # Detect environment if requested
    detected_env = None
    if args.detect:
        detected_env = detect_environment(config)

    # Display timeout settings
    display_task_timeouts(config, args.env, args.task, detected_env)

if __name__ == "__main__":
    main()