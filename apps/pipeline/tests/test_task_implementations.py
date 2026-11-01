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
    """Test RunAllMLModels wrapper task.

    ML-020 P0: RunAllMLModels always yields canonical DECAD tasks regardless of
    the prediction_mode parameter. Luigi deduplicates at runtime when both
    PENTAD and DECAD workflows share the same task identity.
    """

    def test_yields_canonical_decad_tasks_for_all_models(self, mock_env):
        """RunAllMLModels(ALL) yields one DECAD task per model, not PENTAD+DECAD."""
        from pipeline_docker import RunAllMLModels, RunMLModel

        task = RunAllMLModels(prediction_mode="ALL")
        deps = list(task.requires())

        ml_tasks = [d for d in deps if isinstance(d, RunMLModel)]
        # With TFT,TIDE models → 2 DECAD tasks (no PENTAD tasks)
        assert len(ml_tasks) == 2
        for t in ml_tasks:
            assert t.prediction_mode == "DECAD", (
                f"Expected canonical DECAD, got {t.prediction_mode} for {t.model_type}"
            )

    def test_pentad_mode_still_canonical_decad(self, mock_env):
        """RunAllMLModels(PENTAD) yields canonical DECAD tasks, not PENTAD."""
        from pipeline_docker import RunAllMLModels, RunMLModel

        task = RunAllMLModels(prediction_mode="PENTAD")
        deps = list(task.requires())

        ml_tasks = [d for d in deps if isinstance(d, RunMLModel)]
        assert len(ml_tasks) == 2
        for t in ml_tasks:
            assert t.prediction_mode == "DECAD", (
                f"RunAllMLModels(PENTAD) must produce canonical DECAD, got {t.prediction_mode}"
            )

    def test_all_modes_produce_identical_task_identity(self, mock_env):
        """ALL/PENTAD/DECAD all produce the exact same canonical DECAD RunMLModel tasks."""
        from pipeline_docker import RunAllMLModels, RunMLModel

        def ml_identities(mode):
            task = RunAllMLModels(prediction_mode=mode)
            return {
                (d.model_type, d.prediction_mode, d.run_mode)
                for d in task.requires()
                if isinstance(d, RunMLModel)
            }

        identities_all = ml_identities("ALL")
        identities_pentad = ml_identities("PENTAD")
        identities_decad = ml_identities("DECAD")

        assert identities_all == identities_pentad == identities_decad, (
            f"Task identities differ: ALL={identities_all}, "
            f"PENTAD={identities_pentad}, DECAD={identities_decad}"
        )
        # Confirm all are DECAD
        for _model_type, mode, run_mode in identities_all:
            assert mode == "DECAD"
            assert run_mode == "forecast"

    def test_pentadal_workflow_ml_same_identity_as_decadal_workflow(self, mock_env, monkeypatch):
        """RunPentadalWorkflow and RunDecadalWorkflow produce identical DECAD ML task objects."""
        import pipeline_docker

        monkeypatch.setattr(pipeline_docker, "RUN_ML_MODELS", "True")

        pentad_wf = pipeline_docker.RunPentadalWorkflow(send_notifications=False)
        decad_wf = pipeline_docker.RunDecadalWorkflow(send_notifications=False)

        def ml_identities(deps):
            return {
                (d.model_type, d.prediction_mode, d.run_mode)
                for d in deps
                if isinstance(d, pipeline_docker.RunMLModel)
            }

        pentad_ml = ml_identities(pentad_wf.requires())
        decad_ml = ml_identities(decad_wf.requires())

        assert pentad_ml == decad_ml, (
            f"Workflow ML identities diverge: PENTAD={pentad_ml}, DECAD={decad_ml}"
        )
        for _, mode, _ in pentad_ml:
            assert mode == "DECAD"

    def test_postprocessing_pentad_and_decad_same_ml_identity(self, mock_env, monkeypatch):
        """PostProcessingForecasts(PENTAD) and (DECAD) depend on same canonical DECAD ML tasks."""
        import pipeline_docker

        monkeypatch.setattr(pipeline_docker, "RUN_ML_MODELS", "True")

        pp_pentad = pipeline_docker.PostProcessingForecasts(prediction_mode="PENTAD")
        pp_decad = pipeline_docker.PostProcessingForecasts(prediction_mode="DECAD")

        def ml_identities(deps):
            return {
                (d.model_type, d.prediction_mode, d.run_mode)
                for d in deps
                if isinstance(d, pipeline_docker.RunMLModel)
            }

        pentad_ml = ml_identities(pp_pentad.requires())
        decad_ml = ml_identities(pp_decad.requires())

        assert pentad_ml == decad_ml, (
            f"PostProcessingForecasts ML identities diverge: PENTAD={pentad_ml}, DECAD={decad_ml}"
        )
        for _, mode, _ in pentad_ml:
            assert mode == "DECAD"


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
