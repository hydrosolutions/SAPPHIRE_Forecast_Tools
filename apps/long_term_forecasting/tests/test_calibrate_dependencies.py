"""Check historical predictor loading and forwarding to model constructors."""
from unittest.mock import MagicMock

import pandas as pd
import pytest

import calibrate_and_hindcast as calibration


@pytest.mark.parametrize("db_enabled", [False, True])
@pytest.mark.parametrize("operation", ["calibrate", "tune"])
@pytest.mark.parametrize("save_success", [False, True])
def test_dependency_frames_reach_model(monkeypatch, db_enabled, operation, save_success):
    config = MagicMock()
    config.all_paths = {"SM_GBT_LR": "/models/SM_GBT_LR/config.json"}
    config.get_model_specific_config.return_value = {
        "general_config": {"model_type": "sciregressor"}, "path_config": {}
    }
    config.get_model_dependencies.return_value = {"SM_GBT_LR": ["LR_Base"]}
    config.get_data_dependencies.return_value = {}
    config.get_horizon_type.return_value = "quarter"
    config.get_operational_month_lead_time.return_value = 1
    config.get_operational_issue_day.return_value = 25
    config.get_output_path.return_value = "/outputs/LR_Base"
    predictors = pd.DataFrame({"date": pd.to_datetime(["2026-09-25"]), "code": [123], "LR_Base": [4.0]})
    loader = MagicMock()
    loader.load_all_dependencies_database.return_value = (predictors, ["LR_Base"])
    loader.load_all_dependencies_csv.return_value = (predictors, ["LR_Base"])
    interface = MagicMock(return_value=loader)
    monkeypatch.setattr(calibration, "BasePredictorDataInterface", interface)
    monkeypatch.setattr(calibration, "SAPPHIRE_API_AVAILABLE", db_enabled)
    model = MagicMock()
    model.tune_hyperparameters.return_value = (True, "done")
    model.calibrate_model_and_hindcast.return_value = pd.DataFrame({
        "date": pd.to_datetime(["2026-09-25"]), "code": [123], "Q_SM_GBT_LR": [4.0]
    })
    factory = MagicMock(return_value=model)
    monkeypatch.setattr(calibration, "create_model_instance", factory)
    monkeypatch.setattr(calibration, "post_process_lt_forecast", lambda **kw: kw["raw_forecast"])
    monkeypatch.setattr(calibration, "save_forecast", MagicMock(return_value=save_success))
    kwargs = dict(model_name="SM_GBT_LR", forecast_configs=config,
                  temporal_data=pd.DataFrame(), static_data=pd.DataFrame(),
                  data_interface=MagicMock(), station_codes=["123"])
    if operation == "calibrate":
        assert calibration.calibrate_model(**kwargs, offset_base=0, offset_discharge=0) is save_success
    else:
        assert calibration.tune_hyperparameters_model(**kwargs) == (True, "done")
    interface.assert_called_once_with(station_codes=["123"])
    assert factory.call_args.kwargs["base_predictors"] is predictors
    assert factory.call_args.kwargs["base_model_names"] == ["LR_Base"]
    if db_enabled:
        loader.load_all_dependencies_database.assert_called_once_with(
            all_dependencies_models=["LR_Base"], horizon_type="quarter", horizon_value=1
        )
        loader.load_all_dependencies_csv.assert_not_called()
    else:
        loader.load_all_dependencies_csv.assert_called_once_with(
            all_dependencies_models=["LR_Base"],
            all_dependencies_paths=["/outputs/LR_Base/LR_Base_hindcast.csv"],
        )
        loader.load_all_dependencies_database.assert_not_called()


def test_no_dependencies_does_not_load(monkeypatch):
    config = MagicMock()
    config.get_model_dependencies.return_value = {}
    loader = MagicMock()
    monkeypatch.setattr(calibration, "BasePredictorDataInterface", loader)
    assert calibration._load_dependency_predictors(config, "LR_Base") == (None, [])
    loader.assert_not_called()


def test_missing_database_history_does_not_fall_back(monkeypatch):
    config = MagicMock()
    config.get_model_dependencies.return_value = {"SM_GBT_LR": ["LR_Base"]}
    loader = MagicMock()
    loader.load_all_dependencies_database.return_value = (pd.DataFrame(), [])
    monkeypatch.setattr(calibration, "SAPPHIRE_API_AVAILABLE", True)
    monkeypatch.setattr(calibration, "BasePredictorDataInterface", MagicMock(return_value=loader))
    with pytest.raises(ValueError, match="No historical dependency"):
        calibration._load_dependency_predictors(config, "SM_GBT_LR")
    loader.load_all_dependencies_csv.assert_not_called()
