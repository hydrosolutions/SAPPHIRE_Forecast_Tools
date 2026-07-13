"""GOLDEN tests — current monthly-lead behaviour (the kill-switch path).

These lock the CURRENT behaviour of the dashboard's monthly-lead handling on
``origin/maxat_sapphire_2`` (hard-coded lead 1 in five places).  They are all
GREEN today and must stay green under the kill-switch
``SAPPHIRE_LTF_DASH_LEAD_AWARE=false`` after the fix lands (P1-P3): flag-off must
be byte-identical to ``maxat``.

Every test sets ``SAPPHIRE_LTF_DASH_LEAD_AWARE=false`` — ignored today (the flag
does not exist yet), meaningful after P1.  Some of these assertions intentionally
lock a KNOWN BUG (e.g. the ``month_0`` card currently merges lead-1 skill) — that
is deliberate: the golden proves the kill-switch reproduces today's behaviour.

See:
- doc/plans/working/ltf_monthly_horizon_value_implementation_plan.md  (P-TEST)
- doc/plans/issues/high_prio_gi_draft_ltf_monthly_horizon_value_semantics.md

No real station codes / discharge values: synthetic ``17999`` (Tajik-shaped) and
``15999`` (Kyrgyz-shaped) codes and arbitrary discharge numbers.
"""

import datetime
import json
import sys
import types
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests

from src import db
from dashboard import widgets
from dashboard.data_manager import DataManager
from dashboard.plot_manager import _format_forecast_info

FLAG = "SAPPHIRE_LTF_DASH_LEAD_AWARE"


# ---------------------------------------------------------------------------
# Fixtures / builders (synthetic codes 15999 / 17999, arbitrary discharge)
# ---------------------------------------------------------------------------

def _mock_response(json_data, status_code=200):
    """Lightweight fake requests.Response (mirrors test_db._make_mock_response)."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def _long_forecast_record(code, horizon_value, q):
    """A monthly long-forecast API row (valid_from April → month_in_year 4)."""
    return {
        "id": 100 + horizon_value,
        "horizon_type": "month",
        "horizon_value": horizon_value,
        "code": code,
        "date": "2026-03-22",
        "model_type": "GBT",
        "model_type_description": "GBT",
        "valid_from": "2026-04-01",
        "valid_to": "2026-04-30",
        "flag": 0,
        "composition": "",
        "q": q,
        "q_obs": None,
        "q_xgb": None,
        "q_lgbm": None,
        "q_catboost": None,
        "q_loc": None,
        "q05": 100.0,
        "q10": 105.0,
        "q25": 110.0,
        "q50": 120.0,
        "q75": 130.0,
        "q90": 135.0,
        "q95": 140.0,
    }


def _skill_record_with_lead(code, month_in_year, horizon_value, delta):
    """A monthly skill-metric API row that carries horizon_value (post-PP-038)."""
    return {
        "id": 300 + horizon_value + int(delta * 10),
        "horizon_type": "month",
        "horizon_in_year": month_in_year,
        "code": code,
        "model_type": "GBT",
        "model_type_description": "GBT",
        "date": "2026-03-15",
        "horizon_value": horizon_value,
        "sdivsigma": 0.5 + delta,
        "nse": 0.8,
        "delta": delta,
        "accuracy": 90.0 + delta,
        "mae": 1.0 + delta,
        "n_pairs": 12,
        "crps": None,
        "pbias": None,
        "kgelf": None,
        "nse_log": None,
        "fhv": None,
        "flv": None,
    }


def _patch_processing(monkeypatch):
    """Neutralise label/i18n processing so merges are observable (mirrors test_db)."""
    monkeypatch.setattr("src.db.processing.add_labels_to_hydrograph", lambda df, stations: df)
    monkeypatch.setattr(
        "src.db.processing.internationalize_forecast_model_names",
        lambda fn, df, **kw: df,
    )


def _setup_config(monkeypatch, tmp_path, name, modes):
    """Write per-mode config JSONs and point the resolver env at them.

    modes: dict {config_mode_name: operational_month_lead_time}.
    Sets supported_modes = the mode names (comma-joined).  Mirrors the helper in
    test_monthly_lead_regression.py so the guard tests are forward-compatible
    with the resolver the fix introduces.
    """
    config_dir = tmp_path / name
    config_dir.mkdir()
    for mode_name, lead in modes.items():
        (config_dir / f"{mode_name}.json").write_text(
            json.dumps({"operational_month_lead_time": lead})
        )
    monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
    monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", name)
    monkeypatch.setenv(
        "ieasyhydroforecast_ml_long_term_supported_modes", ",".join(modes.keys())
    )


# ---------------------------------------------------------------------------
# Pure function: _format_forecast_info (plot_manager) — Defect A caption site
# ---------------------------------------------------------------------------

class TestFormatForecastInfoCurrent:
    def test_month1_current_caption(self, monkeypatch):
        """month_1 currently captions the month AFTER the issue month (lead-1)."""
        monkeypatch.setenv(FLAG, "false")

        result = _format_forecast_info(datetime.date(2026, 7, 1), "month_1")

        expected = (
            "Monthly runoff forecast for August  \n"
            "Forecast issue date: 1st of July 2026 (month_1)"
        )
        assert result == expected, f"Expected {expected!r}, got {result!r}"

    def test_month0_current_caption(self, monkeypatch):
        """month_0 currently captions the issue month itself (lead-0)."""
        monkeypatch.setenv(FLAG, "false")

        result = _format_forecast_info(datetime.date(2026, 7, 10), "month_0")

        expected = (
            "Monthly runoff forecast for July  \n"
            "Forecast issue date: 10th of July 2026 (month_0)"
        )
        assert result == expected, f"Expected {expected!r}, got {result!r}"


# ---------------------------------------------------------------------------
# Pure function: format_horizon_info (widgets) — Defect J header site
# ---------------------------------------------------------------------------

class TestFormatHorizonInfoCurrent:
    def test_month_ignores_passed_target_and_recomputes(self, monkeypatch):
        """The month header currently IGNORES forecast_horizon/forecast_year and
        recomputes the target month from last_date (production month + 1)."""
        monkeypatch.setenv(FLAG, "false")

        # Pass a deliberately-inconsistent target (month 1, year 2030): if the
        # current code used them the output would differ.  It does not.
        result = widgets.format_horizon_info(
            "month", forecast_horizon=1, forecast_year=2030,
            last_date=datetime.date(2026, 7, 1),
        )

        # production_date = 2026-06-30 → recomputed target = July, year = 2026.
        expected = "month: July 2026, produced on Jun 30, 2026"
        assert result == expected, f"Expected {expected!r}, got {result!r}"


# ---------------------------------------------------------------------------
# db.get_data("month", ...) — kghm-shaped, multi-lead skill (Defects A + F)
# Locks the CURRENT/kill-switch behaviour AND fills the _op_lead coverage gap.
# ---------------------------------------------------------------------------

class TestGetDataMonthMultiLeadCurrent:
    def test_main_panel_hv1_and_m0_card_annotated_from_lead1_stats(self, monkeypatch):
        """kghm: main panel selects hv1 forecast rows and merges hv1 skill; the
        month_0 (hv0) card is currently annotated from the lead-1-filtered stats.

        This locks Defect F's current (buggy) behaviour as the kill-switch path
        and exercises the multi-lead `_op_lead` filter (both leads present).
        """
        monkeypatch.setenv(FLAG, "false")
        monkeypatch.setenv(
            "ieasyhydroforecast_ml_long_term_supported_modes", "month_0,month_1"
        )
        code = "15999"

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url and params.get("horizon_type") == "month":
                hv = params.get("horizon_value")
                if hv == 1:
                    return _mock_response([_long_forecast_record(code, 1, 111.0)])
                if hv == 0:
                    return _mock_response([_long_forecast_record(code, 0, 222.0)])
                return _mock_response([])
            if "/skill-metric/" in url and params.get("horizon") == "month":
                return _mock_response([
                    _skill_record_with_lead(code, 4, 0, delta=1.0),  # lead-0 skill
                    _skill_record_with_lead(code, 4, 1, delta=5.0),  # lead-1 skill
                ])
            return _mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        _patch_processing(monkeypatch)
        all_stations = pd.DataFrame({"code": [code], "station_labels": ["Test Reservoir K"]})

        data = db.get_data("month", code, all_stations)

        # Main panel: hv1 forecast (q=111) with lead-1 skill (delta=5.0).
        fa = data["forecasts_all"]
        assert list(fa["forecasted_discharge"]) == [111.0], (
            f"main panel should select hv1 forecast rows; got {list(fa['forecasted_discharge'])}"
        )
        assert fa["delta"].iloc[0] == 5.0

        # month_0 card: hv0 forecast (q=222) but currently annotated with lead-1
        # skill (delta=5.0) — the Defect F bug, locked as the kill-switch path.
        m0 = data["long_forecasts_m0"]
        assert list(m0["forecasted_discharge"]) == [222.0], (
            f"m0 card should carry hv0 forecast rows; got {list(m0['forecasted_discharge'])}"
        )
        assert m0["delta"].iloc[0] == 5.0, (
            "m0 currently merges the lead-1-filtered stats (Defect F, locked as flag-off)"
        )


# ---------------------------------------------------------------------------
# Flag-ON invariants (GUARD) — kghm must stay lead-1 under the FIX path.
# Green today (flag ignored) AND after the fix (resolver → same result).  These
# are NOT xfail: they are invariants that a naive "always resolve hv0" fix would
# break, catching a regression the tjhm xfail alone cannot.
# ---------------------------------------------------------------------------

class TestGetDataMonthLeadGuards:
    def test_kghm_main_panel_stays_hv1_under_flag_on(self, monkeypatch, tmp_path):
        """kghm, flag ON, config-resolved: the main panel must select the lead-1
        product (hv1, q=111) — the resolver returns 1 for kghm's month_1.

        Guards against a hardcoded-hv0 fix that would pass the tjhm→hv0
        regression while silently breaking kghm.  Green now (flag ignored → hv1)
        and green after (resolver → hv1).
        """
        monkeypatch.setenv(FLAG, "true")
        # kghm: month_1.json → lead 1 (flagship, day 25), month_0.json → lead 0 (day 10).
        _setup_config(monkeypatch, tmp_path, "kghm", {"month_0": 0, "month_1": 1})
        code = "15999"

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url and params.get("horizon_type") == "month":
                hv = params.get("horizon_value")
                if hv == 1:
                    return _mock_response([_long_forecast_record(code, 1, 111.0)])
                if hv == 0:
                    return _mock_response([_long_forecast_record(code, 0, 222.0)])
                return _mock_response([])
            return _mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        _patch_processing(monkeypatch)
        all_stations = pd.DataFrame({"code": [code], "station_labels": ["Test Reservoir K"]})

        data = db.get_data("month", code, all_stations)

        fa = data["forecasts_all"]
        assert list(fa["forecasted_discharge"]) == [111.0], (
            "kghm main panel must stay lead 1 (hv1, q=111) under the fix — "
            f"got {list(fa['forecasted_discharge'])}"
        )

    def test_tjhm_month0_card_absent_under_flag_on(self, monkeypatch, tmp_path):
        """tjhm (no month_0 in supported_modes), flag ON: the month_0 card is
        absent — long_forecasts_m0 is empty.  The month_0 gate holds for tjhm
        regardless of the flag.  Green now and after the fix.
        """
        monkeypatch.setenv(FLAG, "true")
        # tjhm: month_1.json → lead 0 (flagship, day 1), month_2.json → lead 1. No month_0.
        _setup_config(monkeypatch, tmp_path, "tjhm", {"month_1": 0, "month_2": 1})
        code = "17999"

        def mock_get(url, **kwargs):
            params = kwargs.get("params", {})
            if "/long-forecast/" in url and params.get("horizon_type") == "month":
                hv = params.get("horizon_value")
                if hv == 1:
                    return _mock_response([_long_forecast_record(code, 1, 111.0)])
                if hv == 0:
                    return _mock_response([_long_forecast_record(code, 0, 222.0)])
                return _mock_response([])
            return _mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        _patch_processing(monkeypatch)
        all_stations = pd.DataFrame({"code": [code], "station_labels": ["Test River T"]})

        data = db.get_data("month", code, all_stations)

        assert data["long_forecasts_m0"].empty, (
            "tjhm has no month_0 mode → the m0 card must be absent regardless of the flag"
        )


# ---------------------------------------------------------------------------
# Bulletin metadata (data_manager) + m0 hydration (bulletin_manager)
# Defects G + bulletin-year — current behaviour.
# ---------------------------------------------------------------------------

class TestBulletinMetadataCurrent:
    def test_forecast_year_is_issue_year(self, monkeypatch):
        """get_bulletin_metadata('month') currently returns forecast_year =
        last_date.year (the issue year), read directly from the main panel."""
        monkeypatch.setenv(FLAG, "false")

        fake = types.SimpleNamespace(
            forecasts_all=pd.DataFrame(
                {"date": pd.to_datetime(["2026-07-01"]), "month_in_year": [8]}
            ),
            horizon_in_year=lambda horizon: "month_in_year",
        )

        last_date, forecast_horizon, forecast_year = DataManager.get_bulletin_metadata(
            fake, "month"
        )

        # last_date = issue date + 1 day; forecast_horizon read from month_in_year.
        assert forecast_horizon == 8
        assert forecast_year == last_date.year, (
            "current rule: forecast_year is the issue year (last_date.year)"
        )
        assert forecast_year == 2026


class TestMonthHydrationParamsCurrent:
    def test_uses_main_panel_bulletin_metadata(self, monkeypatch):
        """_month_hydration_params currently hydrates from the MAIN-panel
        get_bulletin_metadata('month'), not the m0 frame."""
        monkeypatch.setenv(FLAG, "false")

        BulletinManager = _import_bulletin_manager()

        called_horizons = []

        def _get_bulletin_metadata(horizon):
            called_horizons.append(horizon)
            # (last_date, target_month_in_year, target_year) — August target.
            return datetime.date(2026, 7, 2), 8, 2026

        fake = types.SimpleNamespace(
            dm=types.SimpleNamespace(get_bulletin_metadata=_get_bulletin_metadata)
        )

        target_month, target_year, days_in_month = BulletinManager._month_hydration_params(fake)

        assert called_horizons == ["month"], (
            "current: m0 bulletin hydrates from the main 'month' panel metadata"
        )
        assert (target_month, target_year, days_in_month) == (8, 2026, 31)  # Aug 2026


# ---------------------------------------------------------------------------
# Import helper: bring in BulletinManager with heavy deps stubbed if absent.
# Mirrors the bootstrap in tests/test_bulletin_month_hydration.py.
# ---------------------------------------------------------------------------

def _import_bulletin_manager():
    fake_keys = [
        "panel", "panel.viewable", "panel.widgets", "panel.layout",
        "panel.pane", "panel.template", "src.gettext_config",
        "dashboard.logger", "src.db",
    ]
    saved = {k: sys.modules[k] for k in fake_keys if k in sys.modules}
    try:
        for mod in [
            "panel", "panel.viewable", "panel.widgets", "panel.layout",
            "panel.pane", "panel.template",
        ]:
            if mod not in sys.modules:
                sys.modules[mod] = MagicMock()
        if "src.gettext_config" not in sys.modules:
            gc = types.ModuleType("src.gettext_config")
            gc._ = lambda x: x
            gc.translation_manager = MagicMock()
            sys.modules["src.gettext_config"] = gc
        if "dashboard.logger" not in sys.modules:
            lg = types.ModuleType("dashboard.logger")
            lg.setup_logger = MagicMock(return_value=MagicMock())
            sys.modules["dashboard.logger"] = lg
        if "src.db" not in sys.modules:
            sys.modules["src.db"] = MagicMock()
        from dashboard.bulletin_manager import BulletinManager
        return BulletinManager
    finally:
        for k in fake_keys:
            if k in saved:
                sys.modules[k] = saved[k]
            elif k in sys.modules:
                del sys.modules[k]
