##################################################
# Configuration Class for Long-Term Forecasting
##################################################


import os
import sys
import glob
import pandas as pd
import numpy as np
import json
from collections import defaultdict, deque
from typing import List, Dict, Set, Any, Tuple

from __init__ import logger 


# Local libraries, installed with pip install -e ./iEasyHydroForecast
# Get the absolute path of the directory containing the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the iEasyHydroForecast directory
forecast_dir = os.path.join(script_dir, '..', 'iEasyHydroForecast')

# Add the forecast directory to the Python path
sys.path.append(forecast_dir)

# Import the setup_library module from the iEasyHydroForecast package
import setup_library as sl


class ForecastConfig:
    def __init__(self):
        sl.load_environment()
        
        self._get_paths()
        
    def _get_paths(self):
        """
        Retrieve necessary file paths for forecasting configuration.
        """
        # Access the environment variables
        self.CONFIG_PATH = os.getenv('ieasyhydroforecast_configuration_path')
        self.LT_CONFIGS = os.getenv('ieasyhydroforecast_ml_long_term_configuration')

        self.LT_supported_modes = os.getenv('ieasyhydroforecast_ml_long_term_supported_modes').split(',')

        self.intermediate_data_path = os.getenv('ieasyforecast_intermediate_data_path')
        self.LT_output_path = os.getenv('ieasyhydroforecast_ml_long_term_output_path')
        self.LT_output_path = os.path.join(self.intermediate_data_path, self.LT_output_path)

        # Forecast Configurations
        self.LT_forecast_configs = os.path.join(self.CONFIG_PATH, self.LT_CONFIGS)

    def load_forecast_config(self, 
                             forecast_mode : str):
        
        assert forecast_mode in self.LT_supported_modes, f"Forecast config {forecast_mode} not supported. Supported configs: {self.LT_supported_modes}"
        
        self.forecast_mode = forecast_mode

        self.config_path = os.path.join(self.LT_forecast_configs, f"config_{forecast_mode}"+".json")
        with open(self.config_path, 'r') as f:
            config = json.load(f)
        
        self.forecast_config = config

        # Get all model paths:
        self.model_folder = self.forecast_config["model_folder"]
        self.model_folder = os.path.join(self.CONFIG_PATH, self.model_folder)
        self.models_to_use = self.forecast_config["models_to_use"]

        self.all_paths = {}
        self.all_models = []

        for family, models in self.models_to_use.items():
            for model_name in models:
                model_path = os.path.join(self.model_folder, family, model_name)
                # Check if model path exists
                if not os.path.exists(model_path):
                    raise FileNotFoundError(f"Model path {model_path} does not exist. Please check your configuration.")
                self.all_paths[model_name] = model_path
                self.all_models.append(model_name)


        # Get the model dependencies:
        # Example SM_GBT : ["LR_1", "SciRegressor_1"] -> SM_GBT depends on LR_1 and SciRegressor_1
        self.model_dependencies = self.forecast_config.get("model_dependencies", {})

        # Order models based on dependencies
        try:
            self.ordered_models = order_models_by_dependencies(
                self.all_models, 
                self.model_dependencies
            )
            
            # Optional: Print execution plan
            if logger:
                execution_plan = visualize_dependencies(
                    self.all_models,
                    self.model_dependencies,
                    self.ordered_models
                )
                logger.info(f"\nModel Execution Plan:\n{execution_plan}")
                
        except ValueError as e:
            logger.error(f"Failed to order models: {e}")
            raise


    def get_model_specific_config(self,
                                  model_name : str):
        
        model_path = self.all_paths.get(model_name)
        if not model_path:
            raise ValueError(f"Model {model_name} not found in configuration paths.")
        
        config_file = os.path.join(model_path, "model_config.json")
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Configuration file {config_file} does not exist for model {model_name}.")
        with open(config_file, 'r') as f:
            model_config = json.load(f)

        general_config_path = os.path.join(model_path, "general_config.json")
        if not os.path.exists(general_config_path):
            raise FileNotFoundError(f"General configuration file {general_config_path} does not exist for model {model_name}.")
        with open(general_config_path, 'r') as f:
            general_config = json.load(f)

        feature_config_path = os.path.join(model_path, "feature_config.json")
        if not os.path.exists(feature_config_path):
            raise FileNotFoundError(f"Feature configuration file {feature_config_path} does not exist for model {model_name}.")
        with open(feature_config_path, 'r') as f:
            feature_config = json.load(f)

        path_config_path = os.path.join(model_path, "data_paths.json")
        if not os.path.exists(path_config_path):
            raise FileNotFoundError(f"Path configuration file {path_config_path} does not exist for model {model_name}.")
        with open(path_config_path, 'r') as f:
            path_config = json.load(f)
        
        return {
            "model_config": model_config,
            "general_config": general_config,
            "feature_config": feature_config,
            "path_config": path_config
        }


    def get_models_to_run(self) -> List[str]:
        return self.all_models

    def get_model_dependencies(self, model_name: str = None) -> List[str]:
        if model_name:
            return self.model_dependencies.get(model_name, [])
        return self.model_dependencies
    
    def get_model_execution_order(self) -> List[str]:
        return self.ordered_models

    def get_data_dependencies(self,
                              model_name : str) -> Dict[str, int]:
        return self.forecast_config["data_dependencies"].get(model_name, {})

    def get_forecast_horizons(self) -> int:
        return self.forecast_config["forecast_horizon"]

    def get_forecast_offsets(self) -> int:
        return self.forecast_config["offset"]
    
    def get_forcing_HRU(self) -> str:
        return self.forecast_config["forcing_HRU"]

    def get_output_path(self,
                        model_name : str) -> str:
        
        return os.path.join(self.LT_output_path, self.forecast_mode, model_name)
    
    def get_calibration_status(self,
                               model_name : str) -> bool:
        return self.forecast_config["is_calibrated"].get(model_name, False)

    def update_calibration_status(self,
                                  model_name : str,
                                  status : bool):
        self.forecast_config["is_calibrated"][model_name] = status

    def get_hyperparameter_tuning_status(self,
                                         model_name : str) -> bool:
        return self.forecast_config["is_hyperparameter_tuned"].get(model_name, False)
    
    def update_hyperparameter_tuning_status(self,
                                            model_name : str,
                                            status : bool):
        self.forecast_config["is_hyperparameter_tuned"][model_name] = status

    def get_start_date(self) -> str:
        return self.forecast_config.get("start_date", None)

    def write_updated_config(self):
        with open(self.config_path, 'w') as f:
            json.dump(self.forecast_config, f, indent=4)



def order_models_by_dependencies(all_models: List[str], 
                                 model_dependencies: Dict[str, List[str]]) -> List[str]:
    """
    Order models using topological sorting to ensure dependencies are met.
    
    Args:
        all_models: List of all model names
        model_dependencies: Dict mapping model -> list of models it depends on
        
    Returns:
        Ordered list of models where dependencies come before dependents
        
    Raises:
        ValueError: If circular dependencies are detected
    """
    
    # Create adjacency list and in-degree counter
    graph = defaultdict(list)
    in_degree = {model: 0 for model in all_models}
    
    # Build the dependency graph
    # If A depends on B, then B -> A (B must run before A)
    for model, deps in model_dependencies.items():
        for dep in deps:
            if dep not in all_models:
                raise ValueError(f"Dependency '{dep}' for model '{model}' not found in model list")
            graph[dep].append(model)
            in_degree[model] += 1
    
    # Start with models that have no dependencies (in_degree = 0)
    queue = deque([model for model in all_models if in_degree[model] == 0])
    ordered_models = []
    
    while queue:
        # Process model with no remaining dependencies
        current = queue.popleft()
        ordered_models.append(current)
        
        # Update dependencies for models that depend on current
        for dependent in graph[current]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)
    
    # Check for circular dependencies
    if len(ordered_models) != len(all_models):
        unprocessed = set(all_models) - set(ordered_models)
        raise ValueError(f"Circular dependency detected! Cannot process: {unprocessed}")
    
    return ordered_models


def visualize_dependencies(all_models: List[str], 
                          model_dependencies: Dict[str, List[str]],
                          ordered_models: List[str]) -> str:
    """
    Create a text visualization of model dependencies and execution order.
    """
    output = []
    output.append("=" * 60)
    output.append("MODEL EXECUTION ORDER")
    output.append("=" * 60)
    
    # Group models by execution level
    levels = defaultdict(list)
    model_to_level = {}
    
    for i, model in enumerate(ordered_models):
        deps = model_dependencies.get(model, [])
        if not deps:
            level = 0
        else:
            level = max(model_to_level.get(dep, 0) for dep in deps) + 1
        model_to_level[model] = level
        levels[level].append(model)
    
    # Display by levels
    for level in sorted(levels.keys()):
        output.append(f"\nLevel {level} (can run in parallel):")
        for model in levels[level]:
            deps = model_dependencies.get(model, [])
            if deps:
                output.append(f"  • {model} <- depends on: {', '.join(deps)}")
            else:
                output.append(f"  • {model} (standalone)")
    
    output.append("\n" + "=" * 60)
    output.append("EXECUTION SEQUENCE:")
    output.append("=" * 60)
    for i, model in enumerate(ordered_models, 1):
        output.append(f"{i:2d}. {model}")
    
    return "\n".join(output)



# Example usage and testing
if __name__ == "__main__":
    # Test case 1: Linear dependencies
    test_models = ["LR_1", "SciRegressor_1", "SM_GBT", "Ensemble_Final"]
    test_deps = {
        "SM_GBT": ["LR_1", "SciRegressor_1"],
        "Ensemble_Final": ["SM_GBT", "LR_1"]
    }
    
    ordered = order_models_by_dependencies(test_models, test_deps)
    print("Test 1 - Order:", ordered)
    # Expected: ['LR_1', 'SciRegressor_1', 'SM_GBT', 'Ensemble_Final']
    
    # Test case 2: Circular dependency (should raise error)
    circular_deps = {
        "A": ["B"],
        "B": ["C"],
        "C": ["A"]
    }
    try:
        ordered = order_models_by_dependencies(["A", "B", "C"], circular_deps)
    except ValueError as e:
        print(f"Test 2 - Caught circular dependency: {e}")
    
    # Test case 3: Complex dependencies
    complex_models = ["M1", "M2", "M3", "M4", "M5", "M6"]
    complex_deps = {
        "M3": ["M1", "M2"],
        "M4": ["M3", "M6"],
        "M5": ["M2"],
        "M6": ["M5"]
    }
    
    ordered = order_models_by_dependencies(complex_models, complex_deps)
    print("\nTest 3 - Complex dependencies:")
    print("Order:", ordered)
    print(visualize_dependencies(complex_models, complex_deps, ordered))