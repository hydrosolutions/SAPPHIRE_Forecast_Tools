"""Uzhm maintenance must fail before touching deployment inputs or artifacts."""

from unittest.mock import MagicMock

import calibrate_and_hindcast as calibration
import pytest


@pytest.fixture(autouse=True)
def clear_inherited_calibration_policy(monkeypatch):
    monkeypatch.delenv("ieasyhydroforecast_lt_allow_calibration", raising=False)


@pytest.fixture
def protected_deployment(monkeypatch, tmp_path):
    monkeypatch.delenv("ieasyhydroforecast_env_file_path", raising=False)
    monkeypatch.setenv("ieasyhydroforecast_organization", " UZHM ")
    paths = [tmp_path / "config.json", tmp_path / "model.bin"]
    for path in paths:
        path.write_bytes(b"synthetic sentinel: preserve bytes")
    forbidden = {}
    for name in (
        "_read_station_codes",
        "ForecastConfig",
        "DataInterface",
        "DataInterfaceDB",
        "create_model_instance",
        "save_forecast",
    ):
        mock = MagicMock(side_effect=AssertionError(f"unexpected {name}"))
        monkeypatch.setattr(calibration, name, mock)
        forbidden[name] = mock
    yield paths, forbidden
    for path in paths:
        assert path.read_bytes() == b"synthetic sentinel: preserve bytes"
    for mock in forbidden.values():
        mock.assert_not_called()


@pytest.mark.parametrize(
    "argv",
    [
        ["--all"],
        ["--models", "SyntheticModel"],
        ["--all", "--tune_hyperparameters"],
        ["--models", "SyntheticModel", "--tune_hyperparameters"],
    ],
)
@pytest.mark.parametrize("source", ["inherited", "file", "file_with_inherited_override"])
def test_cli_refuses_before_loading_or_writing(
    monkeypatch, tmp_path, protected_deployment, argv, source
):
    env_file = tmp_path / ".env"
    env_file.write_text("ieasyhydroforecast_organization=' Uzhm '\n")
    if source == "file":
        monkeypatch.delenv("ieasyhydroforecast_organization")
    elif source == "file_with_inherited_override":
        monkeypatch.setenv("ieasyhydroforecast_organization", "other")

    # Exercise the actual dotenv precedence used by setup_library without its
    # unrelated deployment validation or host normalization.
    def load_environment():
        calibration.sl.load_dotenv(env_file)
        return str(env_file)

    monkeypatch.setattr(calibration.sl, "load_environment", load_environment)
    monkeypatch.setattr(calibration.sys, "argv", ["calibrate_and_hindcast.py", *argv])
    with pytest.raises(SystemExit) as exc:
        calibration.main()
    assert exc.value.code == 1


@pytest.mark.parametrize("tune", [False, True])
def test_direct_entry_refuses(monkeypatch, protected_deployment, tune):
    load = MagicMock(return_value=None)
    monkeypatch.setattr(calibration.sl, "load_environment", load)
    with pytest.raises(PermissionError, match="prohibited for Uzhm"):
        calibration.calibrate_and_hindcast(True, [], tune)
    load.assert_called_once_with()


@pytest.mark.parametrize("helper", ["calibrate_model", "tune_hyperparameters_model"])
@pytest.mark.parametrize("source", ["inherited", "file_with_inherited_override"])
def test_direct_helpers_refuse_without_loading_environment(
    monkeypatch, tmp_path, protected_deployment, helper, source
):
    if source == "file_with_inherited_override":
        env_file = tmp_path / ".env"
        env_file.write_text("ieasyhydroforecast_organization=uzhm\n")
        monkeypatch.setenv("ieasyhydroforecast_env_file_path", str(env_file))
        monkeypatch.setenv("ieasyhydroforecast_organization", "other")
    load = MagicMock(side_effect=AssertionError("must not reload env"))
    monkeypatch.setattr(calibration.sl, "load_environment", load)
    config = MagicMock()
    kwargs = dict(
        model_name="SyntheticModel",
        forecast_configs=config,
        temporal_data=None,
        static_data=None,
        data_interface=None,
    )
    if helper == "calibrate_model":
        kwargs.update(offset_base=None, offset_discharge=None)
    with pytest.raises(PermissionError):
        getattr(calibration, helper)(**kwargs)
    assert config.mock_calls == []
    load.assert_not_called()


def test_other_organization_reaches_existing_entry_flow(monkeypatch):
    monkeypatch.setenv("ieasyhydroforecast_organization", "other")
    monkeypatch.delenv("ieasyhydroforecast_env_file_path", raising=False)
    monkeypatch.setattr(calibration.sl, "load_environment", lambda: None)
    station_load = MagicMock(side_effect=LookupError("existing flow reached"))
    monkeypatch.setattr(calibration, "_read_station_codes", station_load)
    with pytest.raises(LookupError, match="existing flow reached"):
        calibration.calibrate_and_hindcast(True, [])
    station_load.assert_called_once_with()


@pytest.mark.parametrize("route", ["all", "models", "tune", "calibrate_helper", "tune_helper"])
@pytest.mark.parametrize(
    "source", ["inherited", "file", "file_false_inherited_true", "inherited_false_file_true"]
)
def test_policy_survives_organization_rename(
    monkeypatch, tmp_path, protected_deployment, route, source
):
    monkeypatch.setenv("ieasyhydroforecast_organization", "renamed")
    policy_key = "ieasyhydroforecast_lt_allow_calibration"
    monkeypatch.delenv(policy_key, raising=False)
    env_file = tmp_path / ".env"
    file_value = "true" if source == "inherited_false_file_true" else "False"
    env_file.write_text(f"ieasyhydroforecast_organization=renamed\n{policy_key}={file_value}\n")
    if source == "inherited":
        monkeypatch.setenv(policy_key, "False")
    else:
        monkeypatch.setenv("ieasyhydroforecast_env_file_path", str(env_file))
        if source == "file_false_inherited_true":
            monkeypatch.setenv(policy_key, "true")
        elif source == "inherited_false_file_true":
            monkeypatch.setenv(policy_key, "False")
    load = MagicMock(return_value=str(env_file) if source != "inherited" else None)
    monkeypatch.setattr(calibration.sl, "load_environment", load)
    if route in {"all", "models", "tune"}:
        args = {
            "all": ["--all"],
            "models": ["--models", "SyntheticModel"],
            "tune": ["--all", "--tune_hyperparameters"],
        }[route]
        monkeypatch.setattr(calibration.sys, "argv", ["calibrate_and_hindcast.py", *args])
        with pytest.raises(SystemExit) as exc:
            calibration.main()
        assert exc.value.code == 1
    else:
        config = MagicMock()
        kwargs = dict(
            model_name="SyntheticModel",
            forecast_configs=config,
            temporal_data=None,
            static_data=None,
            data_interface=None,
        )
        if route == "calibrate_helper":
            kwargs.update(offset_base=None, offset_discharge=None)
        helper = (
            calibration.calibrate_model
            if route == "calibrate_helper"
            else calibration.tune_hyperparameters_model
        )
        with pytest.raises(PermissionError, match=policy_key):
            helper(**kwargs)
        assert config.mock_calls == []
        load.assert_not_called()


@pytest.mark.parametrize("value", ["false", "0", "no", "off", "", "typo"])
def test_explicit_non_truthy_policy_fails_closed(monkeypatch, value):
    monkeypatch.setenv("ieasyhydroforecast_organization", "other")
    monkeypatch.delenv("ieasyhydroforecast_env_file_path", raising=False)
    monkeypatch.setenv("ieasyhydroforecast_lt_allow_calibration", value)
    with pytest.raises(PermissionError, match="lt_allow_calibration"):
        calibration._ensure_calibration_allowed()


@pytest.mark.parametrize("value", [None, "true", "1", " YES ", "On"])
@pytest.mark.parametrize("source", ["inherited", "file"])
def test_other_org_absent_or_true_policy_preserves_entry(monkeypatch, tmp_path, value, source):
    monkeypatch.setenv("ieasyhydroforecast_organization", "other")
    monkeypatch.delenv("ieasyhydroforecast_env_file_path", raising=False)
    monkeypatch.delenv("ieasyhydroforecast_lt_allow_calibration", raising=False)
    env_file = None
    if source == "inherited" and value is not None:
        monkeypatch.setenv("ieasyhydroforecast_lt_allow_calibration", value)
    elif source == "file":
        env_file = tmp_path / ".env"
        env_file.write_text(
            "ieasyhydroforecast_organization=other\n"
            + (f"ieasyhydroforecast_lt_allow_calibration='{value}'\n" if value is not None else "")
        )
    monkeypatch.setattr(calibration.sl, "load_environment", lambda: env_file)
    station_load = MagicMock(side_effect=LookupError("existing flow reached"))
    monkeypatch.setattr(calibration, "_read_station_codes", station_load)
    with pytest.raises(LookupError, match="existing flow reached"):
        calibration.calibrate_and_hindcast(True, [])
    station_load.assert_called_once_with()


def test_true_policy_never_overrides_uzhm(monkeypatch, protected_deployment):
    monkeypatch.setenv("ieasyhydroforecast_lt_allow_calibration", "true")
    with pytest.raises(PermissionError, match="prohibited for Uzhm"):
        calibration._ensure_calibration_allowed()
