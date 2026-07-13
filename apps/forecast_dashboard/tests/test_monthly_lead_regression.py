"""REGRESSION tests — DESIRED monthly-lead behaviour (red now, green after fix).

These assert the CORRECTED behaviour the fix (P1-P3) must produce.  They are RED
today (the flag / month resolver do not exist yet, so the buggy path runs) and
are marked ``@pytest.mark.xfail(strict=True)``: they fail now, pass when the
phase lands, and ``strict=True`` turns the eventual unexpected pass into a
failure that forces removing the marker — that is the phase's pass criterion.

Cases that need a not-yet-existing signature (``month_horizon_value``) are left
as ``@pytest.mark.skip`` stubs documenting the intended assertions; those are
authored in the implementing phase.

Every test sets ``SAPPHIRE_LTF_DASH_LEAD_AWARE=true``.

See:
- doc/plans/working/ltf_monthly_horizon_value_implementation_plan.md  (P-TEST)
- doc/plans/issues/high_prio_gi_draft_ltf_monthly_horizon_value_semantics.md

No real station codes / discharge values: synthetic ``17999`` (Tajik-shaped) and
``15999`` (Kyrgyz-shaped) codes and arbitrary discharge numbers.
"""

import datetime
import json
import types
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests

from src import db
from dashboard import widgets
from dashboard.data_manager import DataManager

FLAG = "SAPPHIRE_LTF_DASH_LEAD_AWARE"


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
    @pytest.mark.xfail(strict=True, reason="P1/A: main panel must resolve lead 0 for tjhm")
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
    @pytest.mark.xfail(
        strict=True, reason="P1/J: header must use passed target month+year (Dec lead-1 rollover)"
    )
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

    @pytest.mark.xfail(
        strict=True, reason="P1/J: header must use passed target month (tjhm lead-0)"
    )
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
    @pytest.mark.xfail(
        strict=True, reason="P2/F(a): m0 card must merge lead-0 stats, not lead-1"
    )
    def test_m0_card_merges_lead0_stats_when_both_present(self, monkeypatch):
        """kghm, both leads' skill present: the m0 (lead-0) card must merge the
        lead-0 skill (delta=1.0), not the lead-1 skill (delta=5.0)."""
        monkeypatch.setenv(FLAG, "true")
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

    @pytest.mark.xfail(
        strict=True, reason="P2/F(b): m0 card must blank when its lead has no stats"
    )
    def test_m0_card_blank_when_no_lead0_stats(self, monkeypatch):
        """kghm, only lead-1 skill present: the m0 (lead-0) card must render its
        metric columns blank/NaN — never filled from another lead."""
        monkeypatch.setenv(FLAG, "true")
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
    @pytest.mark.xfail(
        strict=True, reason="P3/year: Dec lead-1 bulletin forecast_year must roll to next year"
    )
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

    @pytest.mark.skip(
        reason="authored in P3: needs m0-frame bulletin hydration signature "
        "(get_bulletin_metadata / _month_hydration_params m0 variant)"
    )
    def test_m0_bulletin_hydrates_from_m0_frame_target_month(self):
        """INTENDED (author in P3):

        The month_0 bulletin must hydrate from the m0 frame's target month, not
        the main panel's.  Concretely, with a kghm deployment where the main
        panel is lead 1 (target month N+1) and the m0 card is lead 0 (target
        month N), adding the m0 card to the bulletin must resolve the norm and
        month length for month N (the m0 target), not N+1.

        This needs an m0-aware hydration entry point (e.g. a horizon/frame
        argument to get_bulletin_metadata or a dedicated _month0_hydration_params)
        that does not exist yet — assert once that signature lands.
        """


# ===========================================================================
# month_horizon_value resolver — authored in P1 (signature does not exist yet).
# ===========================================================================

@pytest.mark.skip(
    reason="authored in P1: _format_forecast_info needs the resolved-lead signature"
)
def test_format_forecast_info_uses_resolved_lead_target():
    """INTENDED (author in P1, once _format_forecast_info takes the resolved lead
    — do not guess the new signature here):

    The caption must name the target month from the RESOLVED lead, not from the
    mode-name string:
    - tjhm lead-0 forecast issued 2026-07-01 → names July (the issue month),
      NOT August.
    - kghm lead-1 forecast issued 2026-07-01 → names August (issue_month + 1).

    The current signature is _format_forecast_info(issue_date, horizon_label) and
    branches on the "month_1"/"month_0" literal; the fix threads the resolved
    lead in instead.  Assert once that signature lands.
    """


@pytest.mark.skip(reason="authored in P1: needs month_horizon_value")
def test_month_horizon_value_resolves_lead_per_config():
    """INTENDED (author in P1, once long_term_horizon_resolver.month_horizon_value
    exists — do not guess its signature here):

    - tjhm-shaped config: month_horizon_value("month_1") -> 0
    - kghm-shaped config: month_horizon_value("month_1") -> 1
    - kghm-shaped config: month_horizon_value("month_0") -> 0
    - tjhm-shaped config: month_horizon_value("month_0") -> raises
      UnsupportedLongTermModeError (mode absent from supported_modes); the caller
      must membership-check before calling, mirroring quarter/season.
    """
