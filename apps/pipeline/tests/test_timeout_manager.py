"""Tests for TimeoutManager in apps/pipeline/src/timeout_manager.py.

Covers:
- Default parameters when no config file exists
- Environment detection for each org x tag combination
- Task-specific overrides from config YAML
- Singleton behavior
- Convenience functions
"""

import os
import sys

import yaml

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from apps.pipeline.src.timeout_manager import TimeoutManager


class TestDefaults:
    """Test default behavior when config file is missing or empty."""

    def test_fallback_when_config_missing(self, tmp_path, monkeypatch):
        """TimeoutManager falls back to empty config when file is missing."""
        monkeypatch.setenv(
            "IEASYHYDROFORECAST_TIMEOUT_CONFIG_PATH",
            str(tmp_path / "nonexistent.yaml"),
        )
        tm = TimeoutManager(config_path=str(tmp_path / "nonexistent.yaml"))
        params = tm.get_task_parameters("AnyTask")
        assert params["timeout_seconds"] == 900
        assert params["max_retries"] == 2
        assert params["retry_delay"] == 5

    def test_unknown_task_returns_defaults(self, tmp_timeout_config, reset_timeout_singleton):
        """Unknown task name returns hardcoded defaults (not env base_timeout)."""
        tm = TimeoutManager()
        params = tm.get_task_parameters("NonExistentTask")
        # Unknown tasks get the hardcoded default (900), not the env base_timeout
        assert params["timeout_seconds"] == 900
        assert params["max_retries"] == 2
        assert params["retry_delay"] == 5


class TestEnvironmentDetection:
    """Test _detect_environment() for each org x tag combination."""

    def _make_manager(self, monkeypatch, org, tag, tmp_timeout_config):
        """Helper to create TimeoutManager with specific org/tag."""
        monkeypatch.setenv("ieasyhydroforecast_organization", org)
        monkeypatch.setenv("ieasyhydroforecast_backend_docker_image_tag", tag)
        # Remove explicit override if set
        monkeypatch.delenv("IEASYHYDROFORECAST_ENVIRONMENT", raising=False)
        return TimeoutManager()

    def test_demo_org(self, monkeypatch, tmp_timeout_config, reset_timeout_singleton):
        tm = self._make_manager(monkeypatch, "demo", "latest", tmp_timeout_config)
        assert tm.current_env == "demo_ch"

    def test_kghm_local(self, monkeypatch, tmp_timeout_config, reset_timeout_singleton):
        tm = self._make_manager(monkeypatch, "kghm", "local", tmp_timeout_config)
        assert tm.current_env == "kghm_local"

    def test_kghm_aws(self, monkeypatch, tmp_timeout_config, reset_timeout_singleton):
        tm = self._make_manager(monkeypatch, "kghm", "latest", tmp_timeout_config)
        assert tm.current_env == "kghm_aws"

    def test_tjhm_local(self, monkeypatch, tmp_timeout_config, reset_timeout_singleton):
        tm = self._make_manager(monkeypatch, "tjhm", "local", tmp_timeout_config)
        assert tm.current_env == "tjhm_local"

    def test_tjhm_aws(self, monkeypatch, tmp_timeout_config, reset_timeout_singleton):
        tm = self._make_manager(monkeypatch, "tjhm", "py312", tmp_timeout_config)
        assert tm.current_env == "tjhm_aws"

    def test_explicit_env_override(self, monkeypatch, tmp_timeout_config, reset_timeout_singleton):
        """IEASYHYDROFORECAST_ENVIRONMENT overrides detection."""
        monkeypatch.setenv("IEASYHYDROFORECAST_ENVIRONMENT", "kghm_local")
        monkeypatch.setenv("ieasyhydroforecast_organization", "demo")
        tm = TimeoutManager()
        assert tm.current_env == "kghm_local"

    def test_unknown_org_falls_back_to_demo(
        self, monkeypatch, tmp_timeout_config, reset_timeout_singleton
    ):
        tm = self._make_manager(monkeypatch, "unknown_org", "latest", tmp_timeout_config)
        assert tm.current_env == "demo_ch"


class TestTaskSpecificOverrides:
    """Test task parameters with config-driven overrides."""

    def test_relative_complexity(self, tmp_timeout_config, reset_timeout_singleton, monkeypatch):
        """TestTask has relative_complexity=2.0, demo_ch base=600 → 1200."""
        monkeypatch.setenv("ieasyhydroforecast_organization", "demo")
        monkeypatch.delenv("IEASYHYDROFORECAST_ENVIRONMENT", raising=False)
        tm = TimeoutManager()
        params = tm.get_task_parameters("TestTask")
        assert params["timeout_seconds"] == 1200  # 600 * 2.0
        assert params["max_retries"] == 5
        assert params["retry_delay"] == 10

    def test_env_override_takes_priority(
        self, tmp_timeout_config, reset_timeout_singleton, monkeypatch
    ):
        """OverrideTask has kghm_local_override=3600."""
        monkeypatch.setenv("ieasyhydroforecast_organization", "kghm")
        monkeypatch.setenv("ieasyhydroforecast_backend_docker_image_tag", "local")
        monkeypatch.delenv("IEASYHYDROFORECAST_ENVIRONMENT", raising=False)
        tm = TimeoutManager()
        params = tm.get_task_parameters("OverrideTask")
        assert params["timeout_seconds"] == 3600

    def test_mlmaintenance_kghm_aws_override_resolves_to_46800(
        self, tmp_path, reset_timeout_singleton, monkeypatch
    ):
        """Kyrgyz non-local tags resolve to kghm_aws and use the matching ML override."""
        config_path = tmp_path / "timeout_config.yaml"
        config_path.write_text(
            yaml.dump(
                {
                    "environments": {"kghm_aws": {"base_timeout": 900}},
                    "tasks": {"MLMaintenance": {"kghm_aws_override": 46800}},
                }
            )
        )
        monkeypatch.setenv("IEASYHYDROFORECAST_TIMEOUT_CONFIG_PATH", str(config_path))
        monkeypatch.setenv("ieasyhydroforecast_organization", "kghm")
        monkeypatch.setenv("ieasyhydroforecast_backend_docker_image_tag", "latest")
        monkeypatch.delenv("IEASYHYDROFORECAST_ENVIRONMENT", raising=False)

        tm = TimeoutManager()
        params = tm.get_task_parameters("MLMaintenance")

        assert tm.current_env == "kghm_aws"
        assert params["timeout_seconds"] == 46800

    def test_mlmaintenance_wrong_env_key_keeps_900_default(
        self, tmp_path, reset_timeout_singleton, monkeypatch
    ):
        """A kghm_local key is a silent no-op when the detected env is kghm_aws."""
        config_path = tmp_path / "timeout_config.yaml"
        config_path.write_text(
            yaml.dump(
                {
                    "environments": {"kghm_aws": {"base_timeout": 900}},
                    "tasks": {"MLMaintenance": {"kghm_local_override": 46800}},
                }
            )
        )
        monkeypatch.setenv("IEASYHYDROFORECAST_TIMEOUT_CONFIG_PATH", str(config_path))
        monkeypatch.setenv("ieasyhydroforecast_organization", "kghm")
        monkeypatch.setenv("ieasyhydroforecast_backend_docker_image_tag", "latest")
        monkeypatch.delenv("IEASYHYDROFORECAST_ENVIRONMENT", raising=False)

        tm = TimeoutManager()
        params = tm.get_task_parameters("MLMaintenance")

        assert tm.current_env == "kghm_aws"
        assert params["timeout_seconds"] == 900


class TestSingleton:
    """Test singleton behavior of get_timeout_manager()."""

    def test_returns_same_instance(self, tmp_timeout_config, reset_timeout_singleton):
        from apps.pipeline.src.timeout_manager import get_timeout_manager

        tm1 = get_timeout_manager()
        tm2 = get_timeout_manager()
        assert tm1 is tm2

    def test_reset_creates_new_instance(self, tmp_timeout_config, reset_timeout_singleton):
        from apps.pipeline.src import timeout_manager as tm_mod
        from apps.pipeline.src.timeout_manager import get_timeout_manager

        tm1 = get_timeout_manager()
        tm_mod._timeout_manager = None
        tm2 = get_timeout_manager()
        assert tm1 is not tm2


class TestConvenienceFunctions:
    """Test module-level convenience functions."""

    def test_get_task_parameters(self, tmp_timeout_config, reset_timeout_singleton):
        from apps.pipeline.src.timeout_manager import get_task_parameters

        params = get_task_parameters("TestTask")
        assert "timeout_seconds" in params
        assert "max_retries" in params
        assert "retry_delay" in params

    def test_get_timeout_seconds(self, tmp_timeout_config, reset_timeout_singleton, monkeypatch):
        monkeypatch.setenv("ieasyhydroforecast_organization", "demo")
        monkeypatch.delenv("IEASYHYDROFORECAST_ENVIRONMENT", raising=False)
        from apps.pipeline.src import timeout_manager as tm_mod

        tm_mod._timeout_manager = None
        tm = TimeoutManager()
        assert tm.get_timeout_seconds("TestTask") == 1200

    def test_get_max_retries(self, tmp_timeout_config, reset_timeout_singleton):
        tm = TimeoutManager()
        assert tm.get_max_retries("TestTask") == 5

    def test_get_retry_delay(self, tmp_timeout_config, reset_timeout_singleton):
        tm = TimeoutManager()
        assert tm.get_retry_delay("TestTask") == 10
