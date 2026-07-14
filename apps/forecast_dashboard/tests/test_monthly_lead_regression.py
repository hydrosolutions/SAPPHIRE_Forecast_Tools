"""REGRESSION tests — DESIRED monthly-lead behaviour (the corrected contract).

These assert the CORRECTED behaviour the fix must produce under the flag. The
fix originally landed on this branch behind ``SAPPHIRE_LTF_DASH_LEAD_AWARE``
(default ON); trunk (PR #414, "M1 P3") independently shipped the same ``db.py``
fix behind the pre-existing ``SAPPHIRE_SKILL_LEAD_AWARE`` flag (default OFF),
using ``operational_lead_for_mode`` instead of the (now-removed)
``month_horizon_value``. This branch converged onto trunk's flag/accessor —
these tests now assert the same corrected contract via that flag/accessor.

Every test sets ``SAPPHIRE_SKILL_LEAD_AWARE=true``.

See:
- doc/plans/working/ltf_monthly_horizon_value_implementation_plan.md  (P-TEST)
- doc/plans/issues/high_prio_gi_draft_ltf_monthly_horizon_value_semantics.md
- doc/plans/issues/high_prio_gi_draft_pp_lead_aware_skill.md  (M1 P3, trunk's
  db.py implementation this branch converged onto)

No real station codes / discharge values: synthetic ``17999`` (Tajik-shaped) and
``15999`` (Kyrgyz-shaped) codes and arbitrary discharge numbers.
"""

import datetime
import json
import types
from unittest.mock import MagicMock

import pandas as pd
import panel as pn
import pytest
import requests
from dashboard import widgets
from dashboard.data_manager import DataManager
from dashboard.widget_manager import WidgetManager
from src import db

FLAG = "SAPPHIRE_SKILL_LEAD_AWARE"


# ---------------------------------------------------------------------------
# Fixtures / builders (synthetic codes 15999 / 17999, arbitrary discharge)
# ---------------------------------------------------------------------------

def _mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def _long_forecast_record(code, horizon_value, q):
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
    monkeypatch.setattr("src.db.processing.add_labels_to_hydrograph", lambda df, stations: df)
    monkeypatch.setattr(
        "src.db.processing.internationalize_forecast_model_names",
        lambda fn, df, **kw: df,
    )


def _setup_config(monkeypatch, tmp_path, name, modes):
    """Write per-mode config JSONs and point the resolver env at them.

    modes: dict {config_mode_name: operational_month_lead_time}.
    Sets supported_modes = the mode names (comma-joined).
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


# ===========================================================================
# P1 / Defect A — tjhm main panel must resolve lead 0, not literal lead 1.
# ===========================================================================

class TestMainPanelResolvesLead:
    def test_tjhm_main_panel_selects_hv0(self, monkeypatch, tmp_path):
        """tjhm (no month_0): the main monthly panel must select the lead-0
        flagship (hv0), not the hard-coded lead-1 product."""
        monkeypatch.setenv(FLAG, "true")
        # tjhm: month_1.json → lead 0 (flagship), month_2.json → lead 1. No month_0.
        _setup_config(
            monkeypatch, tmp_path, "tjhm", {"month_1": 0, "month_2": 1}
        )
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

        fa = data["forecasts_all"]
        assert list(fa["forecasted_discharge"]) == [222.0], (
            "tjhm main panel must resolve lead 0 (hv0 forecast q=222), not literal lead 1 (q=111)"
        )


# ===========================================================================
# P1 / Defect J — format_horizon_info must USE the passed target month + year.
# ===========================================================================

class TestFormatHorizonInfoUsesPassedTarget:
    def test_december_lead1_rolls_to_january_next_year(self, monkeypatch):
        """A December-issued lead-1 forecast targets January of the FOLLOWING
        year; the header must name that month AND year."""
        monkeypatch.setenv(FLAG, "true")

        # Issue in December 2026 (production_date 2026-12-30); target = January 2027.
        result = widgets.format_horizon_info(
            "month", forecast_horizon=1, forecast_year=2027,
            last_date=datetime.date(2026, 12, 31),
        )

        expected = "month: January 2027, produced on Dec 30, 2026"
        assert result == expected, f"Expected {expected!r}, got {result!r}"

    def test_tjhm_lead0_uses_issue_month_as_target(self, monkeypatch):
        """tjhm lead-0: target month = issue month (July), not the recomputed
        month+1 (August)."""
        monkeypatch.setenv(FLAG, "true")

        result = widgets.format_horizon_info(
            "month", forecast_horizon=7, forecast_year=2026,
            last_date=datetime.date(2026, 7, 2),
        )

        expected = "month: July 2026, produced on Jul 1, 2026"
        assert result == expected, f"Expected {expected!r}, got {result!r}"


# ===========================================================================
# P2 / Defect F — each card merges only its displayed lead's skill stats.
# ===========================================================================

class TestMonth0CardStatsFiltering:
    def test_m0_card_merges_lead0_stats_when_both_present(self, monkeypatch, tmp_path):
        """kghm, both leads' skill present: the m0 (lead-0) card must merge the
        lead-0 skill (delta=1.0), not the lead-1 skill (delta=5.0)."""
        monkeypatch.setenv(FLAG, "true")
        # kghm config: month_1 → lead 1 (main panel), month_0 → lead 0 (m0 card).
        # Flag-on resolves the main lead from config (no silent fallback), so a
        # resolvable config must be present — as in production.
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
            if "/skill-metric/" in url and params.get("horizon") == "month":
                return _mock_response([
                    _skill_record_with_lead(code, 4, 0, delta=1.0),
                    _skill_record_with_lead(code, 4, 1, delta=5.0),
                ])
            return _mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        _patch_processing(monkeypatch)
        all_stations = pd.DataFrame({"code": [code], "station_labels": ["Test Reservoir K"]})

        data = db.get_data("month", code, all_stations)

        m0 = data["long_forecasts_m0"]
        # No duplicate rows: an unfiltered merge can leave the lead-0 row first
        # while still duplicating the (code, model) row — a single-cell delta
        # check alone would false-pass.  The m0 frame must stay 1:1.
        assert len(m0) == 1, (
            f"m0 frame must have exactly one (code, model) row; got {len(m0)}: "
            f"{m0[['code', 'model_short', 'horizon_value']].to_dict('records') if 'horizon_value' in m0.columns else m0[['code', 'model_short']].to_dict('records')}"
        )
        assert m0["delta"].iloc[0] == 1.0, (
            "m0 card must carry the lead-0 skill (delta=1.0), not the lead-1 skill (delta=5.0)"
        )

    def test_m0_card_blank_when_no_lead0_stats(self, monkeypatch, tmp_path):
        """kghm, only lead-1 skill present: the m0 (lead-0) card must render its
        metric columns blank/NaN — never filled from another lead."""
        monkeypatch.setenv(FLAG, "true")
        # kghm config: month_1 → lead 1 (main panel), month_0 → lead 0 (m0 card).
        # Flag-on resolves the main lead from config (no silent fallback), so a
        # resolvable config must be present — as in production.
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
            if "/skill-metric/" in url and params.get("horizon") == "month":
                # Only lead-1 skill exists; lead-0 has none.
                return _mock_response([_skill_record_with_lead(code, 4, 1, delta=5.0)])
            return _mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        _patch_processing(monkeypatch)
        all_stations = pd.DataFrame({"code": [code], "station_labels": ["Test Reservoir K"]})

        data = db.get_data("month", code, all_stations)

        m0 = data["long_forecasts_m0"]
        # The lead-0 FORECAST row must be RETAINED — a fix that simply drops the
        # whole m0 row (and its columns) must not false-pass the blank check.
        assert list(m0["forecasted_discharge"]) == [222.0], (
            f"the lead-0 m0 forecast row (q=222) must be retained; got "
            f"{list(m0['forecasted_discharge']) if 'forecasted_discharge' in m0.columns else '(no forecast column)'}"
        )
        # ... and its skill metric must be blank (NaN / absent), not borrowed
        # from lead-1 (delta=5.0).
        blank = ("delta" not in m0.columns) or pd.isna(m0["delta"].iloc[0])
        assert blank, (
            "m0 card must be blank for its lead (lead-0) when no lead-0 stats exist, "
            "not filled from lead-1 (delta=5.0)"
        )


# ===========================================================================
# P3 / Defect G + bulletin-year — m0 bulletin target month/year.
# ===========================================================================

class TestBulletinTargetMonthYear:
    def test_december_lead1_forecast_year_rolls_to_next_year(self, monkeypatch):
        """A December-issued lead-1 forecast targets January of the following
        year, so get_bulletin_metadata('month') must return that target year."""
        monkeypatch.setenv(FLAG, "true")

        # Issued 2026-12-25 (kghm day-25), targeting January (month_in_year 1) 2027.
        fake = types.SimpleNamespace(
            forecasts_all=pd.DataFrame(
                {"date": pd.to_datetime(["2026-12-25"]), "month_in_year": [1]}
            ),
            horizon_in_year=lambda horizon: "month_in_year",
        )

        _last_date, forecast_horizon, forecast_year = DataManager.get_bulletin_metadata(
            fake, "month"
        )

        assert forecast_horizon == 1
        assert forecast_year == 2027, (
            "Dec-issued lead-1 targets January of the FOLLOWING year (2027), not the issue year"
        )


# ===========================================================================
# operational_lead_for_mode resolver (trunk M1 P3's accessor — supersedes this
# branch's now-removed month_horizon_value, which did the same job).
# ===========================================================================

def test_format_forecast_info_uses_resolved_lead_target():
    """The caption names the target month from the RESOLVED lead threaded into
    ``_format_forecast_info(issue_date, horizon_label, lead=...)``, not from the
    mode-name string literal."""
    from dashboard.plot_manager import _format_forecast_info

    # tjhm lead-0 issued 2026-07-01 → names July (the issue month), NOT August.
    tjhm = _format_forecast_info(datetime.date(2026, 7, 1), "month_1", lead=0)
    assert tjhm.startswith("Monthly runoff forecast for July"), tjhm

    # kghm lead-1 issued 2026-07-01 → names August (issue_month + 1).
    kghm = _format_forecast_info(datetime.date(2026, 7, 1), "month_1", lead=1)
    assert kghm.startswith("Monthly runoff forecast for August"), kghm


def test_operational_lead_for_mode_resolves_lead_per_config(monkeypatch, tmp_path):
    """long_term_horizon_resolver.operational_lead_for_mode resolves the
    per-config lead, mirroring quarter/season, and raises for an unsupported
    mode. Mirrors this module's src.month_lead helpers, which wrap this same
    accessor with graceful degradation for the UI layer."""
    from long_term_horizon_resolver import (
        UnsupportedLongTermModeError,
        operational_lead_for_mode,
    )

    # tjhm-shaped: month_1 → lead 0; month_0 is not supported.
    _setup_config(monkeypatch, tmp_path, "tjhm", {"month_1": 0, "month_2": 1})
    assert operational_lead_for_mode("month_1") == 0
    with pytest.raises(UnsupportedLongTermModeError):
        operational_lead_for_mode("month_0")

    # kghm-shaped: month_1 → lead 1; month_0 → lead 0.
    _setup_config(monkeypatch, tmp_path, "kghm", {"month_0": 0, "month_1": 1})
    assert operational_lead_for_mode("month_1") == 1
    assert operational_lead_for_mode("month_0") == 0


# ===========================================================================
# Defect 1 (adversarial review) — a stale cross-horizon period number must
# not leak into the month header on a failed metadata refresh.
#
# widget_manager._on_change deliberately leaves self.forecast_horizon stale
# when dm.get_bulletin_metadata(horizon) raises for the newly-selected
# horizon (no data yet). Under the flag, widgets.format_horizon_info's month
# branch trusts forecast_horizon as a 1..12 month index — so a stale
# pentad_in_year (1..72) or quarter_in_year (1..4) either crashes
# (months_en[30] → IndexError) or silently renders the wrong month
# (months_en[3] == "March" when the stale value happens to be <= 12).
# ===========================================================================

class _FakeDM:
    """Minimal dm stand-in for driving WidgetManager._on_change directly."""

    def __init__(self, fail_horizons):
        self.forecasts_all = pd.DataFrame()
        self._fail_horizons = fail_horizons

    def load_station(self, horizon, code):
        pass

    def update_sites_for_pentad(self, _, horizon, pentad, decad):
        pass

    def invalidate_render_cache(self):
        pass

    def get_bulletin_metadata(self, horizon):
        if horizon in self._fail_horizons:
            # Mirrors data_manager.get_bulletin_metadata's real failure mode
            # when forecasts_all has no valid dates for this horizon yet.
            raise ValueError("No valid forecast dates available")
        return (datetime.date(2026, 8, 1), 8, 2026)


class TestOnChangeStaleMetadataGuard:
    """widget_manager._on_change / widgets.format_horizon_info: when
    dm.get_bulletin_metadata(horizon) raises for the newly-selected horizon,
    the PREVIOUS horizon's cached (last_date, forecast_horizon, forecast_year)
    triple must not be trusted for the month header — no crash, no silently
    wrong month.
    """

    @staticmethod
    def _make_wm(initial_horizon, stale_forecast_horizon, stale_forecast_year, stale_last_date):
        """A lightweight fake WidgetManager `self` carrying stale bulletin
        metadata left over from a previously-selected horizon — sufficient to
        call WidgetManager._wire_station_period_change(fake_self, dm, pm) and
        then invoke the resulting closure directly. Mirrors the fake_self
        pattern in test_bulletin_publish.py (constructing a real WidgetManager
        is impractical: its __init__ needs a full DataManager,
        DashboardConfig, and station_dict)."""
        fake_self = types.SimpleNamespace(
            _gettext=lambda s: s,
            horizon_selector=pn.widgets.Select(
                options=["pentad", "decade", "month", "quarter", "season"],
                value=initial_horizon,
            ),
            station_selector=pn.widgets.Select(
                options=["19999 - Test River"], value="19999 - Test River"
            ),
            pentad_selector=pn.widgets.Select(options=[30], value=30),
            decad_selector=pn.widgets.Select(options=[10], value=10),
            date_picker=types.SimpleNamespace(value=None),
            forecast_card=types.SimpleNamespace(visible=False),
            horizon_info_pane=types.SimpleNamespace(object=""),
            _dashboard_tabs=types.SimpleNamespace(active=1),
            _post_load_callbacks=[],
            # Stale metadata left over from the PREVIOUS (initial_horizon) selection.
            _metadata_horizon=initial_horizon,
            forecast_horizon=stale_forecast_horizon,
            forecast_year=stale_forecast_year,
            last_date=stale_last_date,
        )
        fake_self.refresh_model_checkbox = lambda: None
        fake_self.refresh_warnings = lambda: None
        fake_self._refresh_horizon_info_pane = lambda: (
            WidgetManager._refresh_horizon_info_pane(fake_self)
        )
        return fake_self

    def test_switch_to_month_with_no_data_does_not_crash(self, monkeypatch):
        """Repro: on 'pentad' (forecast_horizon=30), switch to 'month' with no
        monthly forecast data yet → get_bulletin_metadata('month') raises
        ValueError. Flag ON: this must not crash (months_en[30] is an
        IndexError before the fix) and must not render a bogus month."""
        monkeypatch.setenv(FLAG, "true")
        fake_self = self._make_wm(
            "pentad", stale_forecast_horizon=30, stale_forecast_year=2026,
            stale_last_date=datetime.date(2026, 7, 26),
        )
        dm = _FakeDM(fail_horizons={"month"})
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)
        # Simulate the user picking "month" in the selector — this fires the
        # pn.depends(watch=True) watcher synchronously, exactly as Panel does.
        # The assignment itself must not raise — that is the crash regression
        # check.
        fake_self.horizon_selector.value = "month"

        assert fake_self.horizon_info_pane.object == "", (
            "no valid metadata for 'month' yet — header must be blank, "
            f"got {fake_self.horizon_info_pane.object!r}"
        )

    def test_switch_to_month_with_stale_in_range_value_not_silently_wrong(self, monkeypatch):
        """Same failure, but the stale value (quarter_in_year=3) is <= 12 and
        would silently pass a naive '1 <= n <= 12' range check, rendering
        'March' instead of being recognised as belonging to a different
        horizon."""
        monkeypatch.setenv(FLAG, "true")
        fake_self = self._make_wm(
            "quarter", stale_forecast_horizon=3, stale_forecast_year=2026,
            stale_last_date=datetime.date(2026, 7, 1),
        )
        dm = _FakeDM(fail_horizons={"month"})
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)
        fake_self.horizon_selector.value = "month"

        assert fake_self.horizon_info_pane.object == "", (
            "stale quarter_in_year=3 must not be silently rendered as March — "
            f"got {fake_self.horizon_info_pane.object!r}"
        )

    def test_successful_switch_still_renders_month_header(self, monkeypatch):
        """Control: when get_bulletin_metadata succeeds for the new horizon,
        the month header renders normally (the guard must not suppress the
        valid case)."""
        monkeypatch.setenv(FLAG, "true")
        fake_self = self._make_wm(
            "pentad", stale_forecast_horizon=30, stale_forecast_year=2026,
            stale_last_date=datetime.date(2026, 7, 26),
        )
        dm = _FakeDM(fail_horizons=set())  # get_bulletin_metadata succeeds
        pm = MagicMock()

        WidgetManager._wire_station_period_change(fake_self, dm, pm)
        fake_self.horizon_selector.value = "month"

        assert fake_self.horizon_info_pane.object != "", (
            "a successful metadata refresh must still render the month header"
        )
        assert "August" in fake_self.horizon_info_pane.object
