"""Tests for maintenance task classes in pipeline_docker.py.

Covers:
- Preprocessing maintenance tasks (GatewayMaintenance, PrepRunoffMaintenance)
- Forecasting maintenance tasks (LinRegMaintenance, MLMaintenance)
- Postprocessing and frontend tasks
- Workflow orchestrators (RunDailyMaintenanceWorkflow, RunPeriodicMaintenanceWorkflow)
- Dependency chains
- Marker file conventions (maintenance_ prefix)
- MLMaintenance resource declaration
"""

import datetime
import os
import sys
from unittest.mock import patch

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))


class TestHelperFunctions:
    """Test maintenance-related helper functions."""

    def test_get_maintenance_marker_filepath(self, mock_env):
        """Maintenance markers have 'maintenance_' prefix."""
        from pipeline_docker import get_maintenance_marker_filepath

        path = get_maintenance_marker_filepath("gateway", date=datetime.date(2026, 3, 1))
        assert "maintenance_gateway_2026-03-01.marker" in path

    def test_maintenance_markers_differ_from_operational(self, mock_env):
        """Maintenance and operational markers don't collide."""
        from pipeline_docker import (
            get_maintenance_marker_filepath,
            get_marker_filepath,
        )

        date = datetime.date(2026, 3, 1)
        operational = get_marker_filepath("gateway", date=date)
        maintenance = get_maintenance_marker_filepath("gateway", date=date)
        assert operational != maintenance
        # Check filenames only (full paths may contain 'maintenance' from pytest tmp dirs)
        op_filename = os.path.basename(operational)
        maint_filename = os.path.basename(maintenance)
        assert maint_filename.startswith("maintenance_")
        assert not op_filename.startswith("maintenance_")

    def test_get_docker_host_env_overrides_non_darwin(self, mock_env):
        """Returns empty list on non-macOS platforms."""
        from pipeline_docker import get_docker_host_env_overrides

        with patch("pipeline_docker.platform.system", return_value="Linux"):
            overrides = get_docker_host_env_overrides()
        assert overrides == []


class TestGatewayMaintenance:
    """Test GatewayMaintenance task."""

    def test_no_dependencies(self, mock_env):
        """GatewayMaintenance has no upstream dependencies."""
        from pipeline_docker import GatewayMaintenance

        task = GatewayMaintenance()
        assert task.requires() == []

    def test_output_uses_maintenance_marker(self, mock_env):
        """Output marker has maintenance_ prefix."""
        from pipeline_docker import GatewayMaintenance

        task = GatewayMaintenance()
        path = task.output().path
        assert "maintenance_gateway_" in path
        assert path.endswith(".marker")


class TestPrepRunoffMaintenance:
    """Test PrepRunoffMaintenance task."""

    def test_no_dependencies(self, mock_env):
        """PrepRunoffMaintenance has no upstream dependencies."""
        from pipeline_docker import PrepRunoffMaintenance

        task = PrepRunoffMaintenance()
        assert task.requires() == []

    def test_output_uses_maintenance_marker(self, mock_env):
        """Output marker has maintenance_ prefix."""
        from pipeline_docker import PrepRunoffMaintenance

        task = PrepRunoffMaintenance()
        path = task.output().path
        assert "maintenance_preprunoff_" in path
        assert path.endswith(".marker")


class TestLinRegMaintenance:
    """Test LinRegMaintenance task."""

    def test_requires_preprunoff_maintenance(self, mock_env):
        """LinRegMaintenance requires PrepRunoffMaintenance."""
        from pipeline_docker import LinRegMaintenance, PrepRunoffMaintenance

        task = LinRegMaintenance(prediction_mode="PENTAD")
        dep = task.requires()
        assert isinstance(dep, PrepRunoffMaintenance)

    def test_output_includes_mode(self, mock_env):
        """Output marker includes prediction mode."""
        from pipeline_docker import LinRegMaintenance

        task = LinRegMaintenance(prediction_mode="DECAD")
        path = task.output().path
        assert "maintenance_linreg_DECAD_" in path

    def test_pentad_and_decad_different_markers(self, mock_env):
        """PENTAD and DECAD have different marker files."""
        from pipeline_docker import LinRegMaintenance

        pentad = LinRegMaintenance(prediction_mode="PENTAD")
        decad = LinRegMaintenance(prediction_mode="DECAD")
        assert pentad.output().path != decad.output().path


class TestMLMaintenance:
    """Test MLMaintenance task."""

    def test_requires_preprunoff_and_gateway(self, mock_env):
        """MLMaintenance requires both preprocessing maintenance tasks."""
        from pipeline_docker import (
            GatewayMaintenance,
            MLMaintenance,
            PrepRunoffMaintenance,
        )

        task = MLMaintenance(model_type="TFT", prediction_mode="PENTAD")
        deps = task.requires()
        assert len(deps) == 2
        assert isinstance(deps[0], PrepRunoffMaintenance)
        assert isinstance(deps[1], GatewayMaintenance)

    def test_resource_declaration(self, mock_env):
        """MLMaintenance declares ml_memory resource for concurrency limiting."""
        from pipeline_docker import MLMaintenance

        task = MLMaintenance(model_type="TFT", prediction_mode="PENTAD")
        assert hasattr(task, "resources")
        assert task.resources == {"ml_memory": 1}

    def test_output_includes_model_and_mode(self, mock_env):
        """Output marker includes model type and prediction mode."""
        from pipeline_docker import MLMaintenance

        task = MLMaintenance(model_type="TIDE", prediction_mode="DECAD")
        path = task.output().path
        assert "maintenance_ml_TIDE_DECAD_" in path


class TestPostProcessingMaintenance:
    """Test PostProcessingMaintenance task."""

    def test_requires_both_linreg_modes(self, mock_env):
        """Requires LinRegMaintenance for both PENTAD and DECAD."""
        from pipeline_docker import (
            LinRegMaintenance,
            PostProcessingMaintenance,
        )

        task = PostProcessingMaintenance()
        deps = task.requires()

        linreg_tasks = [d for d in deps if isinstance(d, LinRegMaintenance)]
        assert len(linreg_tasks) == 2
        modes = {t.prediction_mode for t in linreg_tasks}
        assert modes == {"PENTAD", "DECAD"}

    def test_includes_ml_when_enabled(self, mock_env, monkeypatch):
        """Includes MLMaintenance tasks when ML models are enabled."""
        import pipeline_docker

        monkeypatch.setattr(pipeline_docker, "RUN_ML_MODELS", "True")

        task = pipeline_docker.PostProcessingMaintenance()
        deps = task.requires()
        ml_tasks = [d for d in deps if isinstance(d, pipeline_docker.MLMaintenance)]
        # TFT,TIDE x PENTAD,DECAD = 4 ML tasks
        assert len(ml_tasks) == 4
        combos = {(d.model_type, d.prediction_mode) for d in ml_tasks}
        assert ("TFT", "PENTAD") in combos
        assert ("TFT", "DECAD") in combos
        assert ("TIDE", "PENTAD") in combos
        assert ("TIDE", "DECAD") in combos

    def test_excludes_ml_when_disabled(self, mock_env):
        """Excludes ML tasks when ML models are disabled."""
        from pipeline_docker import MLMaintenance, PostProcessingMaintenance

        task = PostProcessingMaintenance()
        deps = task.requires()
        ml_tasks = [d for d in deps if isinstance(d, MLMaintenance)]
        assert len(ml_tasks) == 0

    def test_output_uses_maintenance_marker(self, mock_env):
        """Output marker has maintenance_ prefix."""
        from pipeline_docker import PostProcessingMaintenance

        task = PostProcessingMaintenance()
        path = task.output().path
        assert "maintenance_postproc_" in path


class TestRunDailyMaintenanceWorkflow:
    """Test RunDailyMaintenanceWorkflow orchestrator."""

    def test_dependency_chain_without_notifications(self, mock_env):
        """Without notifications, requires PostProcessingMaintenance."""
        from pipeline_docker import (
            RunDailyMaintenanceWorkflow,
        )

        task = RunDailyMaintenanceWorkflow(send_notifications=False)
        deps = task.requires()
        assert isinstance(deps, list)
        class_names = [type(d).__name__ for d in deps]
        assert "PostProcessingMaintenance" in class_names

    def test_with_notifications(self, mock_env):
        """With notifications, wraps in SendPipelineCompletionNotification."""
        from pipeline_docker import (
            RunDailyMaintenanceWorkflow,
            SendPipelineCompletionNotification,
        )

        task = RunDailyMaintenanceWorkflow(send_notifications=True)
        dep = task.requires()
        assert isinstance(dep, SendPipelineCompletionNotification)

    def test_full_dag_depth(self, mock_env):
        """Walk the full DAG from workflow to leaf tasks."""
        from pipeline_docker import (
            LinRegMaintenance,
            PostProcessingMaintenance,
            PrepRunoffMaintenance,
            RunDailyMaintenanceWorkflow,
        )

        workflow = RunDailyMaintenanceWorkflow(send_notifications=False)
        deps = workflow.requires()

        # Find PostProcessingMaintenance in deps
        postproc = [d for d in deps if isinstance(d, PostProcessingMaintenance)][0]

        # PostProcessingMaintenance depends on LinRegMaintenance
        postproc_deps = postproc.requires()
        linreg_tasks = [d for d in postproc_deps if isinstance(d, LinRegMaintenance)]
        assert len(linreg_tasks) == 2

        # Each LinRegMaintenance depends on PrepRunoffMaintenance
        for lr in linreg_tasks:
            assert isinstance(lr.requires(), PrepRunoffMaintenance)


class TestPeriodicMaintenanceTasks:
    """Test periodic (bimonthly/yearly) maintenance tasks."""

    def test_long_term_postprocessing_output(self, mock_env):
        """LongTermPostProcessingMaintenance has correct marker."""
        from pipeline_docker import LongTermPostProcessingMaintenance

        task = LongTermPostProcessingMaintenance()
        path = task.output().path
        assert "maintenance_lt_postproc_" in path

    def test_yearly_skill_recalculation_output(self, mock_env):
        """YearlySkillRecalculation has correct marker."""
        from pipeline_docker import YearlySkillRecalculation

        task = YearlySkillRecalculation()
        path = task.output().path
        assert "maintenance_skill_recalc_" in path

    def test_yearly_snow_norm_output(self, mock_env):
        """YearlySnowNormRecalculation has correct marker."""
        from pipeline_docker import YearlySnowNormRecalculation

        task = YearlySnowNormRecalculation()
        path = task.output().path
        assert "maintenance_snow_norms_" in path


class TestRunPeriodicMaintenanceWorkflow:
    """Test RunPeriodicMaintenanceWorkflow parameterized orchestrator."""

    def test_long_term_routing(self, mock_env):
        """task_type='long_term' routes to LongTermPostProcessingMaintenance."""
        from pipeline_docker import (
            LongTermPostProcessingMaintenance,
            RunPeriodicMaintenanceWorkflow,
        )

        task = RunPeriodicMaintenanceWorkflow(task_type="long_term")
        dep = task.requires()
        assert isinstance(dep, LongTermPostProcessingMaintenance)

    def test_skill_recalc_routing(self, mock_env):
        """task_type='skill_recalc' routes to YearlySkillRecalculation."""
        from pipeline_docker import (
            RunPeriodicMaintenanceWorkflow,
            YearlySkillRecalculation,
        )

        task = RunPeriodicMaintenanceWorkflow(task_type="skill_recalc")
        dep = task.requires()
        assert isinstance(dep, YearlySkillRecalculation)

    def test_snow_norms_routing(self, mock_env):
        """task_type='snow_norms' routes to YearlySnowNormRecalculation."""
        from pipeline_docker import (
            RunPeriodicMaintenanceWorkflow,
            YearlySnowNormRecalculation,
        )

        task = RunPeriodicMaintenanceWorkflow(task_type="snow_norms")
        dep = task.requires()
        assert isinstance(dep, YearlySnowNormRecalculation)

    def test_invalid_task_type(self, mock_env):
        """Invalid task_type raises ValueError."""
        from pipeline_docker import RunPeriodicMaintenanceWorkflow

        task = RunPeriodicMaintenanceWorkflow(task_type="invalid")
        with pytest.raises(ValueError, match="Unknown task_type"):
            task.requires()

    def test_output_includes_task_type(self, mock_env):
        """Output path includes the task_type parameter."""
        from pipeline_docker import RunPeriodicMaintenanceWorkflow

        task = RunPeriodicMaintenanceWorkflow(task_type="long_term")
        assert "long_term" in task.output().path
