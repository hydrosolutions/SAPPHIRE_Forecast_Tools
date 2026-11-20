"""Long-term horizon visibility is independent of short-term model execution."""
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import panel as pn
import pytest

from dashboard import config
from dashboard.widget_manager import WidgetManager


@pytest.fixture
def configured_widgets(monkeypatch):
    """Run real config and widget construction with filesystem/server IO stubbed."""
    monkeypatch.setattr(config, "setup_panel", lambda pn: None)
    monkeypatch.setattr(config, "load_env_and_icons", lambda: ("", False, ""))
    monkeypatch.setattr(config, "setup_directories", lambda: "/tmp")
    viz = SimpleNamespace(create_reload_button=lambda horizon: pn.pane.Str(""))
    monkeypatch.setattr(config, "setup_localization", lambda pn: viz)
    monkeypatch.setattr(config, "get_horizon", lambda: ("pentad", "1"))
    monkeypatch.setattr("dashboard.widgets.create_language_buttons", lambda: pn.pane.Str(""))
    monkeypatch.setenv("ieasyreports_report_output_path", "/tmp")
    monkeypatch.setenv("ieasyhydroforecast_run_CM_models", "False")
    dm = MagicMock()
    dm.forecasts_all = pd.DataFrame({"date": [pd.Timestamp("2026-11-15")]})
    dm.get_bulletin_metadata.return_value = (date(2026, 11, 15), 1, 2026)
    dm.get_available_models.return_value = {"LR": "LR"}
    dm.get_best_models.return_value = []
    dm.resolve_model_values.return_value = []
    dm._data = {"hydrograph_day_all": pd.DataFrame({"station_labels": [], "date": []})}
    return lambda cfg: WidgetManager(dm, cfg, {"Test Basin": ["Synthetic station"]})


@pytest.mark.parametrize("ml", [False, True])
@pytest.mark.parametrize("flag", [None, "true", "YES", "1", "t", "y", "false", "", "invalid", " true "])
def test_environment_to_both_horizon_widgets(monkeypatch, configured_widgets, ml, flag):
    monkeypatch.setenv("ieasyhydroforecast_run_ML_models", str(ml))
    key = "ieasyhydroforecast_display_long_term_forecasts"
    if flag is None:
        monkeypatch.delenv(key, raising=False)
    else:
        monkeypatch.setenv(key, flag)
    cfg = config.init_dashboard(pn)
    expected = ml if flag is None else flag.lower() in ("true", "yes", "1", "t", "y")
    assert cfg.display_ML_forecasts is ml
    assert cfg.display_long_term_forecasts is expected
    wm = configured_widgets(cfg)
    options = {"pentad", "decade"} | ({"month", "season"} if expected else set())
    assert set(wm.horizon_selector.options.values()) == options
    assert set(wm.publish_horizon_multiselect.options.values()) == options
    assert cfg.display_ML_forecasts is ml


@pytest.mark.parametrize("ml", [False, True])
@pytest.mark.parametrize("legacy", [True, False])
def test_legacy_config_visibility(configured_widgets, monkeypatch, ml, legacy):
    monkeypatch.setenv("ieasyhydroforecast_run_ML_models", str(ml))
    cfg = config.init_dashboard(pn)
    values = dict(vars(cfg))
    if legacy:
        values.pop("display_long_term_forecasts")
    else:
        values["display_long_term_forecasts"] = None
    wm = configured_widgets(SimpleNamespace(**values))
    expected = {"pentad", "decade"} | ({"month", "season"} if ml else set())
    assert set(wm.horizon_selector.options.values()) == expected
    assert set(wm.publish_horizon_multiselect.options.values()) == expected
