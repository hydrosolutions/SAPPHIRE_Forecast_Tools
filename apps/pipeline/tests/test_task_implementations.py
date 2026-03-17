"""Tests for existing operational task classes in pipeline_docker.py.

Verifies requires(), output(), env vars, and dependency chains for:
- PreprocessingRunoff
- PreprocessingGatewayQuantileMapping
- LinearRegression
- ConceptualModel
- RunMLModel
- RunAllMLModels
- PostProcessingForecasts
- RunPentadalWorkflow / RunDecadalWorkflow
- RunWorkflow
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))


class TestPreprocessingRunoff:
    """Test PreprocessingRunoff task."""

    def test_no_dependencies(self, mock_env):
        """PreprocessingRunoff has no upstream dependencies."""
        from pipeline_docker import PreprocessingRunoff

        task = PreprocessingRunoff()
        deps = task.requires()
        # Luigi base requires() returns [] when no dependencies defined
        assert deps is None or deps == [] or deps == ()

    def test_output_path(self, mock_env):
        """Output is /app/log_preprunoff.txt."""
        from pipeline_docker import PreprocessingRunoff

        task = PreprocessingRunoff()
        assert task.output().path == "/app/log_preprunoff.txt"


class TestPreprocessingGateway:
    """Test PreprocessingGatewayQuantileMapping task."""

    def test_no_dependencies(self, mock_env):
        """PreprocessingGatewayQuantileMapping has no upstream dependencies."""
        from pipeline_docker import PreprocessingGatewayQuantileMapping

        task = PreprocessingGatewayQuantileMapping()
        deps = task.requires()
        assert deps is None or deps == [] or deps == ()

    def test_output_path(self, mock_env):
        """Output is /app/log_pregateway.txt."""
        from pipeline_docker import PreprocessingGatewayQuantileMapping

        task = PreprocessingGatewayQuantileMapping()
        assert task.output().path == "/app/log_pregateway.txt"


class TestLinearRegression:
    """Test LinearRegression task."""

    def test_requires_preprocessing_runoff(self, mock_env):
        """LinearRegression requires PreprocessingRunoff."""
        from pipeline_docker import LinearRegression, PreprocessingRunoff

        task = LinearRegression(prediction_mode="PENTAD")
        dep = task.requires()
        assert isinstance(dep, PreprocessingRunoff)

    def test_default_prediction_mode(self, mock_env):
        """Default prediction mode is ALL."""
        from pipeline_docker import LinearRegression

        task = LinearRegression()
        assert task.prediction_mode == "ALL"

    def test_output_path(self, mock_env):
        """Output is /app/log_linreg.txt."""
        from pipeline_docker import LinearRegression

        task = LinearRegression()
        assert task.output().path == "/app/log_linreg.txt"


class TestConceptualModel:
    """Test ConceptualModel task."""

    def test_requires_runoff_and_gateway(self, mock_env):
        """ConceptualModel requires PreprocessingRunoff + gateway dependency."""
        from pipeline_docker import ConceptualModel, PreprocessingRunoff

        task = ConceptualModel()
        deps = task.requires()
        assert isinstance(deps, list)
        assert len(deps) == 2
        assert isinstance(deps[0], PreprocessingRunoff)

    def test_output_path(self, mock_env):
        """Output is /app/log_conceptmod.txt."""
        from pipeline_docker import ConceptualModel

        task = ConceptualModel()
        assert task.output().path == "/app/log_conceptmod.txt"


class TestRunMLModel:
    """Test RunMLModel task."""

    def test_requires_runoff_and_gateway(self, mock_env):
        """RunMLModel requires PreprocessingRunoff + gateway dependency."""
        from pipeline_docker import PreprocessingRunoff, RunMLModel

        task = RunMLModel(model_type="TFT", prediction_mode="PENTAD")
        deps = task.requires()
        assert isinstance(deps, list)
        assert len(deps) == 2
        assert isinstance(deps[0], PreprocessingRunoff)

    def test_output_path_includes_params(self, mock_env):
        """Output path includes model_type and prediction_mode."""
        from pipeline_docker import RunMLModel

        task = RunMLModel(model_type="TFT", prediction_mode="PENTAD")
        path = task.output().path
        assert "TFT" in path
        assert "PENTAD" in path

    def test_parameters(self, mock_env):
        """Task stores model_type and prediction_mode."""
        from pipeline_docker import RunMLModel

        task = RunMLModel(model_type="TIDE", prediction_mode="DECAD", run_mode="maintenance")
        assert task.model_type == "TIDE"
        assert task.prediction_mode == "DECAD"
        assert task.run_mode == "maintenance"


class TestRunAllMLModels:
    """Test RunAllMLModels wrapper task."""

    def test_yields_all_model_horizon_combos(self, mock_env):
        """RunAllMLModels yields model x horizon combinations."""
        from pipeline_docker import RunAllMLModels, RunMLModel

        task = RunAllMLModels(prediction_mode="ALL")
        deps = list(task.requires())

        # Should yield PreprocessingRunoff, gateway, and model tasks
        ml_tasks = [d for d in deps if isinstance(d, RunMLModel)]
        # With TFT,TIDE models and PENTAD,DECAD modes → 4 ML tasks
        assert len(ml_tasks) == 4

    def test_single_mode_yields_subset(self, mock_env):
        """RunAllMLModels with single mode yields only that mode."""
        from pipeline_docker import RunAllMLModels, RunMLModel

        task = RunAllMLModels(prediction_mode="PENTAD")
        deps = list(task.requires())

        ml_tasks = [d for d in deps if isinstance(d, RunMLModel)]
        # With TFT,TIDE models and PENTAD only → 2 ML tasks
        assert len(ml_tasks) == 2
        for t in ml_tasks:
            assert t.prediction_mode == "PENTAD"


class TestPostProcessingForecasts:
    """Test PostProcessingForecasts task."""

    def test_requires_linear_regression(self, mock_env):
        """Always requires LinearRegression."""
        from pipeline_docker import LinearRegression, PostProcessingForecasts

        task = PostProcessingForecasts(prediction_mode="PENTAD")
        deps = task.requires()
        lr_tasks = [d for d in deps if isinstance(d, LinearRegression)]
        assert len(lr_tasks) == 1
        assert lr_tasks[0].prediction_mode == "PENTAD"

    def test_ml_tasks_when_enabled(self, mock_env, monkeypatch):
        """Includes ML tasks when RUN_ML_MODELS is True."""
        import pipeline_docker

        monkeypatch.setattr(pipeline_docker, "RUN_ML_MODELS", "True")

        task = pipeline_docker.PostProcessingForecasts(prediction_mode="PENTAD")
        deps = task.requires()
        ml_tasks = [d for d in deps if isinstance(d, pipeline_docker.RunMLModel)]
        assert len(ml_tasks) > 0

    def test_no_ml_tasks_when_disabled(self, mock_env):
        """No ML tasks when RUN_ML_MODELS is False."""
        import pipeline_docker

        # mock_env sets RUN_ML_MODELS to 'False' by default
        task = pipeline_docker.PostProcessingForecasts(prediction_mode="PENTAD")
        deps = task.requires()
        ml_tasks = [d for d in deps if isinstance(d, pipeline_docker.RunMLModel)]
        assert len(ml_tasks) == 0

    def test_output_path(self, mock_env):
        """Output is /app/log_postproc.txt."""
        from pipeline_docker import PostProcessingForecasts

        task = PostProcessingForecasts()
        assert task.output().path == "/app/log_postproc.txt"


class TestWorkflows:
    """Test workflow orchestrator tasks."""

    def test_pentadal_includes_lr(self, mock_env):
        """RunPentadalWorkflow includes LinearRegression(PENTAD)."""
        from pipeline_docker import RunPentadalWorkflow

        task = RunPentadalWorkflow(send_notifications=False)
        deps = task.requires()
        assert isinstance(deps, list)
        # Should contain at least LinearRegression and PostProcessingForecasts
        class_names = [type(d).__name__ for d in deps]
        assert "LinearRegression" in class_names
        assert "PostProcessingForecasts" in class_names

    def test_decadal_includes_lr(self, mock_env):
        """RunDecadalWorkflow includes LinearRegression(DECAD)."""
        from pipeline_docker import RunDecadalWorkflow

        task = RunDecadalWorkflow(send_notifications=False)
        deps = task.requires()
        assert isinstance(deps, list)
        class_names = [type(d).__name__ for d in deps]
        assert "LinearRegression" in class_names

    def test_workflow_mode_routing(self, mock_env):
        """RunWorkflow routes to correct sub-workflows."""
        from pipeline_docker import (
            RunDecadalWorkflow,
            RunPentadalWorkflow,
            RunWorkflow,
        )

        pentad = RunWorkflow(mode="PENTAD", send_notifications=False)
        assert isinstance(pentad.requires(), RunPentadalWorkflow)

        decad = RunWorkflow(mode="DECAD", send_notifications=False)
        assert isinstance(decad.requires(), RunDecadalWorkflow)

        both = RunWorkflow(mode="ALL", send_notifications=False)
        deps = both.requires()
        assert isinstance(deps, list)
        assert len(deps) == 2

    def test_pentadal_output(self, mock_env):
        """RunPentadalWorkflow output path."""
        from pipeline_docker import RunPentadalWorkflow

        task = RunPentadalWorkflow()
        assert "pentadal" in task.output().path

    def test_decadal_output(self, mock_env):
        """RunDecadalWorkflow output path."""
        from pipeline_docker import RunDecadalWorkflow

        task = RunDecadalWorkflow()
        assert "decadal" in task.output().path
