"""Tests for the quarterly snapshot feature (PP-075 Q2).

Covers:
  1. _capture_quarterly_snapshot — no source df returns None; hidden card
     (source df cleared) returns None; default-first-row selection; model
     lookup by identity not by sorted position; inverted/one-sided bounds
     rejected; both-null accepted; invalid quarter dates rejected.
  2. _populate_quarterly_from_snapshot — None snapshot clears all q-attrs;
     snapshot with valid dates + bounds populates q-attrs; snapshot with null
     bounds retains section heading (valid_from/valid_to set) but null Q values.
  3. _sanitize_quarterly_snapshot — non-finite floats become None; dates
     kept as strings; None input returns None.
  4. _site_to_records — quarterly_snapshot included in each record; None
     snapshot serialized as None.
  5. _load_bulletin_from_api — quarterly_snapshot read from first non-null
     row; absence of column yields None; populates quarterly attrs.
  6. _on_add staging — successful POST promotes to bulletin_sites; failed
     POST restores previous site state (forecasts, quarterly_snapshot);
     no entry after first failed Add; subsequent monthly-cell edit cannot
     persist the failed candidate's snapshot.
  7. _on_add_m0 staging — mirrors _on_add for the m0 path.
  8. serialize_site (bulletin_publish) — month horizon adds quarterly key:
     null for no snapshot; six fields + v_min/v_max for valid snapshot; raw
     values not rounded; non-month horizons omit the key.
  9. assemble_bulletin_snapshot — period conflict raises ValueError; no
     conflict passes; existing share expiry/auth behavior unchanged.

has_quarterly section presence and period-conflict detection are covered by
write_to_excel boundary tests in test_quarterly_export_roundtrip.py.

Synthetic station 19999 only. No real station codes or discharge values.
"""

from __future__ import annotations

import sys
import types
from datetime import date as _date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Bootstrap: stub heavy imports before loading bulletin_manager
# ---------------------------------------------------------------------------

_FAKE_KEYS = [
    "panel",
    "panel.viewable",
    "panel.widgets",
    "panel.layout",
    "panel.pane",
    "panel.template",
    "src.gettext_config",
    "dashboard.logger",
    "src.db",
    "skill_lead_aware_flag",
    "dashboard.utils",
]

_saved = {k: sys.modules[k] for k in _FAKE_KEYS if k in sys.modules}

try:
    for _mod in [
        "panel", "panel.viewable", "panel.widgets",
        "panel.layout", "panel.pane", "panel.template",
    ]:
        if _mod not in sys.modules:
            sys.modules[_mod] = MagicMock()

    if "src.gettext_config" not in sys.modules:
        _gc = types.ModuleType("src.gettext_config")
        _gc._ = lambda x: x
        _gc.translation_manager = MagicMock()
        sys.modules["src.gettext_config"] = _gc

    if "dashboard.logger" not in sys.modules:
        _lg = types.ModuleType("dashboard.logger")
        _lg.setup_logger = MagicMock(return_value=MagicMock())
        sys.modules["dashboard.logger"] = _lg

    if "src.db" not in sys.modules:
        sys.modules["src.db"] = MagicMock()

    if "skill_lead_aware_flag" not in sys.modules:
        _slaf = types.ModuleType("skill_lead_aware_flag")
        _slaf.skill_lead_aware_enabled = lambda: False
        sys.modules["skill_lead_aware_flag"] = _slaf

    if "dashboard.utils" not in sys.modules:
        _du = types.ModuleType("dashboard.utils")
        _du.hydrate_month_hydrograph_stats = MagicMock()
        _du.hydrate_season_hydrograph_stats = MagicMock()
        _du.rehydrate_sites_hydrograph_stats = MagicMock()
        _du.rehydrate_sites_linreg_predictor = MagicMock()
        sys.modules["dashboard.utils"] = _du

    import sys as _sys
    import os as _os
    _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))

    from dashboard import bulletin_manager  # noqa: E402

    _capture_quarterly_snapshot = bulletin_manager._capture_quarterly_snapshot
    _populate_quarterly_from_snapshot = bulletin_manager._populate_quarterly_from_snapshot
    _sanitize_quarterly_snapshot = bulletin_manager._sanitize_quarterly_snapshot
    _is_valid_quarter = bulletin_manager._is_valid_quarter
    _snap_float = bulletin_manager._snap_float
    _site_to_records = bulletin_manager._site_to_records
    _load_bulletin_from_api = bulletin_manager._load_bulletin_from_api
    BulletinManager = bulletin_manager.BulletinManager

finally:
    for _k in _FAKE_KEYS:
        if _k in _saved:
            sys.modules[_k] = _saved[_k]
        elif _k in sys.modules:
            del sys.modules[_k]
    del _saved, _FAKE_KEYS

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.site import SapphireSite  # noqa: E402
from dashboard import bulletin_publish as bp  # noqa: E402


# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

Q1_VF = "2026-01-01"
Q1_VT = "2026-03-31"


def _make_wm(
    source_df=None,
    tabulator_value=None,
    tabulator_selection=None,
):
    """Minimal wm stub for _capture_quarterly_snapshot."""
    tab_q = types.SimpleNamespace(
        value=tabulator_value,
        selection=tabulator_selection,
    )
    return types.SimpleNamespace(
        _quarterly_source_df=source_df,
        forecast_tabulator_q=tab_q,
    )


def _q_source_row(model="LR", vf=Q1_VF, vt=Q1_VT):
    return pd.DataFrame([{
        "model_short": model,
        "valid_from": pd.Timestamp(vf),
        "valid_to": pd.Timestamp(vt),
        "forecasted_discharge": 100.0,
        "Q25": 80.0,
        "Q75": 120.0,
        "date": pd.Timestamp("2025-12-15"),
        "station_labels": "19999 - River R вдхр P",
    }])


def _q_tabulator_row(model="LR", q=100.0, lb=80.0, ub=120.0):
    return pd.DataFrame([{
        "Model": model,
        "Forecasted discharge": q,
        "Forecast lower bound": lb,
        "Forecast upper bound": ub,
        "δ": 20.0,
        "s/σ": 0.5,
        "MAE": 5.0,
        "Accuracy": 90.0,
    }])


def _make_reservoir_site(code="19999"):
    site = SapphireSite(
        code=code,
        river_name_ru="River R",
        punkt_name_ru="вдхр P",
        basin_ru="Basin B",
        bulletin_order=1,
    )
    site.station_label = f"{code} - River R вдхр P"
    site.hydrograph_norm = None
    return site


def _make_non_reservoir_site(code="19999"):
    site = SapphireSite(
        code=code,
        river_name_ru="River R",
        punkt_name_ru="Punkt P",
        basin_ru="Basin B",
        bulletin_order=1,
    )
    site.station_label = f"{code} - River R Punkt P"
    site.hydrograph_norm = None
    return site


def _make_forecast_df(model="LR", q=50.0, lb=40.0, ub=60.0):
    return pd.DataFrame([{
        "Model": model,
        "Forecasted discharge": q,
        "Forecast lower bound": lb,
        "Forecast upper bound": ub,
        "δ": 10.0,
        "s/σ": 0.5,
        "MAE": 2.0,
        "Accuracy": 85.0,
    }])


def _make_add_manager_stub(site, bulletin_sites=None):
    """Minimal stub for the parts of BulletinManager._on_add we exercise."""
    if bulletin_sites is None:
        bulletin_sites = []
    wm = types.SimpleNamespace(
        horizon_selector=types.SimpleNamespace(value="month"),
        station_selector=types.SimpleNamespace(value=site.station_label),
        forecast_tabulator=types.SimpleNamespace(
            value=_make_forecast_df(),
            selection=[0],
        ),
        date_picker=types.SimpleNamespace(value=_date(2025, 12, 15)),
        _quarterly_source_df=_q_source_row(),
        forecast_tabulator_q=types.SimpleNamespace(
            value=_q_tabulator_row(),
            selection=[0],
        ),
        add_to_bulletin_popup=types.SimpleNamespace(
            object="", alert_type="success", visible=False,
        ),
    )
    cfg = types.SimpleNamespace(
        viz=types.SimpleNamespace(
            app_state=types.SimpleNamespace(pipeline_running=False),
        ),
    )
    dm = types.SimpleNamespace(
        sites_list=[site],
        forecasts_all=pd.DataFrame(columns=["station_labels", "valid_from", "model_short", "date"]),
        get_bulletin_metadata=MagicMock(return_value=(pd.Timestamp("2025-12-15"), 1, 2026)),
    )
    stub = types.SimpleNamespace(
        wm=wm,
        cfg=cfg,
        dm=dm,
        bulletin_sites=bulletin_sites,
        _horizon_context=lambda: ("month", 2026, 1),
        _month_hydration_params=lambda: (1, 2026, 31),
        _update_bulletin_table=MagicMock(),
        _show_popup=MagicMock(),
        _show_popup_m0=MagicMock(),
    )
    return stub


# ---------------------------------------------------------------------------
# 1. _capture_quarterly_snapshot
# ---------------------------------------------------------------------------


class TestCaptureQuarterlySnapshot:
    def test_returns_none_when_source_df_absent(self):
        wm = _make_wm(source_df=None)
        assert _capture_quarterly_snapshot(wm, lambda x: x) is None

    def test_returns_none_when_source_df_empty(self):
        wm = _make_wm(source_df=pd.DataFrame())
        assert _capture_quarterly_snapshot(wm, lambda x: x) is None

    def test_returns_none_when_tabulator_empty(self):
        wm = _make_wm(source_df=_q_source_row(), tabulator_value=pd.DataFrame())
        assert _capture_quarterly_snapshot(wm, lambda x: x) is None

    def test_returns_none_when_tabulator_none(self):
        wm = _make_wm(source_df=_q_source_row(), tabulator_value=None)
        assert _capture_quarterly_snapshot(wm, lambda x: x) is None

    def test_default_first_row_when_selection_empty(self):
        wm = _make_wm(
            source_df=_q_source_row(),
            tabulator_value=_q_tabulator_row(),
            tabulator_selection=[],
        )
        snap = _capture_quarterly_snapshot(wm, lambda x: x)
        assert snap is not None
        assert snap["model_type"] == "LR"

    def test_default_first_row_when_selection_none(self):
        wm = _make_wm(
            source_df=_q_source_row(),
            tabulator_value=_q_tabulator_row(),
            tabulator_selection=None,
        )
        snap = _capture_quarterly_snapshot(wm, lambda x: x)
        assert snap is not None

    def test_captures_model_from_identity_not_sorted_position(self):
        # Two-model tabulator; source has the same two models.
        # Sort the tabulator so the second model appears first — identity
        # must be resolved by model name, not by row index.
        source = pd.concat([
            _q_source_row("LR"),
            _q_source_row("GBT"),
        ], ignore_index=True)
        tab = pd.concat([
            _q_tabulator_row("GBT", q=200.0),
            _q_tabulator_row("LR", q=100.0),
        ], ignore_index=True)
        wm = _make_wm(source_df=source, tabulator_value=tab, tabulator_selection=[0])
        snap = _capture_quarterly_snapshot(wm, lambda x: x)
        # Row 0 of the SORTED tabulator is GBT
        assert snap is not None
        assert snap["model_type"] == "GBT"
        assert snap["forecasted_discharge"] == pytest.approx(200.0)

    def test_rejects_inverted_bounds(self):
        wm = _make_wm(
            source_df=_q_source_row(),
            tabulator_value=_q_tabulator_row(lb=120.0, ub=80.0),
            tabulator_selection=[0],
        )
        with pytest.raises(ValueError, match="inverted bounds"):
            _capture_quarterly_snapshot(wm, lambda x: x)

    def test_rejects_one_sided_lower_missing(self):
        wm = _make_wm(
            source_df=_q_source_row(),
            tabulator_value=_q_tabulator_row(lb=float("nan"), ub=120.0),
            tabulator_selection=[0],
        )
        with pytest.raises(ValueError, match="one-sided bounds"):
            _capture_quarterly_snapshot(wm, lambda x: x)

    def test_rejects_one_sided_upper_missing(self):
        wm = _make_wm(
            source_df=_q_source_row(),
            tabulator_value=_q_tabulator_row(lb=80.0, ub=float("nan")),
            tabulator_selection=[0],
        )
        with pytest.raises(ValueError, match="one-sided bounds"):
            _capture_quarterly_snapshot(wm, lambda x: x)

    def test_accepts_both_bounds_null(self):
        wm = _make_wm(
            source_df=_q_source_row(),
            tabulator_value=_q_tabulator_row(lb=float("nan"), ub=float("nan")),
            tabulator_selection=[0],
        )
        snap = _capture_quarterly_snapshot(wm, lambda x: x)
        assert snap is not None
        assert snap["fc_lower"] is None
        assert snap["fc_upper"] is None

    def test_rejects_non_quarter_dates(self):
        # valid_from = Feb 1, valid_to = Apr 30 — not a calendar quarter
        bad_source = _q_source_row()
        bad_source["valid_from"] = pd.Timestamp("2026-02-01")
        bad_source["valid_to"] = pd.Timestamp("2026-04-30")
        wm = _make_wm(
            source_df=bad_source,
            tabulator_value=_q_tabulator_row(),
            tabulator_selection=[0],
        )
        assert _capture_quarterly_snapshot(wm, lambda x: x) is None

    def test_valid_snapshot_fields(self):
        wm = _make_wm(
            source_df=_q_source_row(),
            tabulator_value=_q_tabulator_row(q=100.0, lb=80.0, ub=120.0),
            tabulator_selection=[0],
        )
        snap = _capture_quarterly_snapshot(wm, lambda x: x)
        assert snap == {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": pytest.approx(100.0),
            "fc_lower": pytest.approx(80.0),
            "fc_upper": pytest.approx(120.0),
        }

    def test_non_finite_q_becomes_none(self):
        wm = _make_wm(
            source_df=_q_source_row(),
            tabulator_value=_q_tabulator_row(q=float("inf"), lb=80.0, ub=120.0),
            tabulator_selection=[0],
        )
        snap = _capture_quarterly_snapshot(wm, lambda x: x)
        assert snap is not None
        assert snap["forecasted_discharge"] is None

    def test_returns_none_when_model_not_in_source(self):
        # Tabulator shows "GBT" but source only has "LR"
        wm = _make_wm(
            source_df=_q_source_row("LR"),
            tabulator_value=_q_tabulator_row("GBT"),
            tabulator_selection=[0],
        )
        assert _capture_quarterly_snapshot(wm, lambda x: x) is None


# ---------------------------------------------------------------------------
# 2. _populate_quarterly_from_snapshot
# ---------------------------------------------------------------------------


class TestPopulateQuarterlyFromSnapshot:
    def test_none_snapshot_clears_attrs(self):
        site = _make_reservoir_site()
        site.quarterly_snapshot = None
        _populate_quarterly_from_snapshot(site, lambda x: x)
        assert site.forecast_q_min_q is None
        assert site.forecast_q_max_q is None
        assert site.quarterly_valid_from is None
        assert site.quarterly_valid_to is None

    def test_absent_snapshot_clears_attrs(self):
        site = _make_reservoir_site()
        # No quarterly_snapshot attribute at all
        _populate_quarterly_from_snapshot(site, lambda x: x)
        assert site.forecast_q_min_q is None
        assert site.forecast_q_max_q is None

    def test_snapshot_with_bounds_populates_q_attrs(self):
        site = _make_reservoir_site()
        site.quarterly_snapshot = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 80.0,
            "fc_upper": 120.0,
        }
        _populate_quarterly_from_snapshot(site, lambda x: x)
        assert site.forecast_q_min_q == pytest.approx(80.0)
        assert site.forecast_q_max_q == pytest.approx(120.0)
        assert site.quarterly_valid_from == pd.Timestamp(Q1_VF)
        assert site.quarterly_valid_to == pd.Timestamp(Q1_VT)

    def test_snapshot_with_null_bounds_sets_dates_but_null_q(self):
        site = _make_reservoir_site()
        site.quarterly_snapshot = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": None,
            "fc_lower": None,
            "fc_upper": None,
        }
        _populate_quarterly_from_snapshot(site, lambda x: x)
        # Dates must be set so period heading shows in Excel
        assert site.quarterly_valid_from == pd.Timestamp(Q1_VF)
        assert site.quarterly_valid_to == pd.Timestamp(Q1_VT)
        # Q values must be None (not fabricated)
        assert site.forecast_q_min_q is None
        assert site.forecast_q_max_q is None

    def test_snapshot_computes_volume_from_dates(self):
        site = _make_reservoir_site()
        site.quarterly_snapshot = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 100.0,
            "fc_upper": 100.0,
        }
        _populate_quarterly_from_snapshot(site, lambda x: x)
        # Q1 2026: Jan 1 – Mar 31 = 90 days inclusive
        expected_seconds = 90 * 86400
        expected_v = 100.0 * expected_seconds / 1_000_000
        assert site.forecast_v_min_q == pytest.approx(expected_v)
        assert site.forecast_v_max_q == pytest.approx(expected_v)


# ---------------------------------------------------------------------------
# 3. _sanitize_quarterly_snapshot
# ---------------------------------------------------------------------------


class TestSanitizeQuarterlySnapshot:
    def test_none_input_returns_none(self):
        assert _sanitize_quarterly_snapshot(None) is None

    def test_non_finite_numeric_becomes_none(self):
        snap = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": float("inf"),
            "fc_lower": float("nan"),
            "fc_upper": 120.0,
        }
        out = _sanitize_quarterly_snapshot(snap)
        assert out["forecasted_discharge"] is None
        assert out["fc_lower"] is None
        assert out["fc_upper"] == pytest.approx(120.0)

    def test_dates_kept_as_strings(self):
        snap = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 80.0,
            "fc_upper": 120.0,
        }
        out = _sanitize_quarterly_snapshot(snap)
        assert out["valid_from"] == Q1_VF
        assert out["valid_to"] == Q1_VT
        assert out["model_type"] == "LR"


# ---------------------------------------------------------------------------
# 4. _site_to_records
# ---------------------------------------------------------------------------


class TestSiteToRecords:
    def _make_site_with_forecasts(self, code="19999", snap=None):
        site = types.SimpleNamespace(
            code=code,
            basin_ru="Basin B",
            station_label=f"{code} - River R Punkt P",
            forecasts=pd.DataFrame([{
                "Model": "LR",
                "Forecasted discharge": 50.0,
                "Forecast lower bound": 40.0,
                "Forecast upper bound": 60.0,
                "δ": 10.0,
                "s/σ": 0.5,
                "MAE": 2.0,
                "Accuracy": 85.0,
            }]),
        )
        if snap is not None:
            site.quarterly_snapshot = snap
        return site

    def test_quarterly_snapshot_included_in_record(self):
        snap = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 80.0,
            "fc_upper": 120.0,
        }
        site = self._make_site_with_forecasts(snap=snap)
        records = _site_to_records("month", 2026, 1, site)
        assert len(records) == 1
        assert records[0]["quarterly_snapshot"] == snap

    def test_none_snapshot_serialized_as_none(self):
        site = self._make_site_with_forecasts()  # no snapshot attr
        records = _site_to_records("month", 2026, 1, site)
        assert records[0]["quarterly_snapshot"] is None

    def test_multiple_model_rows_each_carry_snapshot(self):
        snap = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 80.0,
            "fc_upper": 120.0,
        }
        site = self._make_site_with_forecasts(snap=snap)
        site.forecasts = pd.concat(
            [site.forecasts, site.forecasts.assign(**{"Model": "GBT"})],
            ignore_index=True,
        )
        records = _site_to_records("month", 2026, 1, site)
        assert len(records) == 2
        for r in records:
            assert r["quarterly_snapshot"] == snap


# ---------------------------------------------------------------------------
# 5. _on_add staging
# ---------------------------------------------------------------------------


class TestOnAddStaging:
    """Tests that _on_add stages, then promotes on success, restores on failure."""

    def test_successful_add_promotes_to_bulletin_sites(self, monkeypatch):
        site = _make_reservoir_site()
        stub = _make_add_manager_stub(site)
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())

        BulletinManager._on_add(stub)

        assert site in stub.bulletin_sites
        assert stub._show_popup.call_args[0][0] == "Added to bulletin table"

    def test_successful_add_sets_quarterly_snapshot(self, monkeypatch):
        site = _make_reservoir_site()
        stub = _make_add_manager_stub(site)
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())

        BulletinManager._on_add(stub)

        snap = getattr(site, 'quarterly_snapshot', None)
        assert snap is not None
        assert snap["model_type"] == "LR"
        assert snap["valid_from"] == Q1_VF

    def test_failed_first_add_leaves_site_not_in_bulletin_sites(self, monkeypatch):
        site = _make_reservoir_site()
        stub = _make_add_manager_stub(site)
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )

        BulletinManager._on_add(stub)

        assert site not in stub.bulletin_sites

    def test_failed_first_add_does_not_set_snapshot(self, monkeypatch):
        site = _make_reservoir_site()
        assert not hasattr(site, 'quarterly_snapshot')
        stub = _make_add_manager_stub(site)
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )

        BulletinManager._on_add(stub)

        # Snapshot must not be left on the site after a failed first Add
        assert not hasattr(site, 'quarterly_snapshot')

    def test_failed_replacement_add_restores_previous_snapshot(self, monkeypatch):
        site = _make_reservoir_site()
        old_snap = {
            "model_type": "GBT",
            "valid_from": "2025-10-01",
            "valid_to": "2025-12-31",
            "forecasted_discharge": 90.0,
            "fc_lower": 70.0,
            "fc_upper": 110.0,
        }
        site.quarterly_snapshot = old_snap
        stub = _make_add_manager_stub(site, bulletin_sites=[site])
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )

        BulletinManager._on_add(stub)

        # Snapshot restored to old value
        assert site.quarterly_snapshot == old_snap

    def test_failed_add_restores_old_forecasts(self, monkeypatch):
        site = _make_reservoir_site()
        old_forecasts = _make_forecast_df(q=999.0)
        site.forecasts = old_forecasts.copy()
        stub = _make_add_manager_stub(site, bulletin_sites=[site])
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )

        BulletinManager._on_add(stub)

        pd.testing.assert_frame_equal(
            site.forecasts.reset_index(drop=True),
            old_forecasts.reset_index(drop=True),
        )

    def test_cell_edit_after_failed_add_carries_old_snapshot(self, monkeypatch):
        """A monthly-cell edit must not persist the failed candidate's snapshot."""
        site = _make_reservoir_site()
        old_snap = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 80.0,
            "fc_upper": 120.0,
        }
        site.quarterly_snapshot = old_snap
        site.forecasts = _make_forecast_df()

        # First: a failed replacement Add
        stub = _make_add_manager_stub(site, bulletin_sites=[site])
        saved_calls: list[list] = []
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )
        BulletinManager._on_add(stub)

        # Snapshot is restored to old_snap; now simulate a cell edit
        save_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", save_mock)

        # Mimic _on_bulletin_edit: save with current site state
        from dashboard.bulletin_manager import _site_to_records as _str
        records = _str("month", 2026, 1, site)
        assert records[0]["quarterly_snapshot"] == old_snap

    def test_failed_add_shows_error_popup(self, monkeypatch):
        site = _make_reservoir_site()
        stub = _make_add_manager_stub(site)
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )

        BulletinManager._on_add(stub)

        stub._show_popup.assert_called_once()
        call_kwargs = stub._show_popup.call_args[1]
        assert call_kwargs.get("alert_type") == "danger"

    def test_successful_add_promotes_derived_quarterly_attrs(self, monkeypatch):
        """__dict__.update must carry derived attrs, not just the snapshot dict."""
        site = _make_reservoir_site()
        stub = _make_add_manager_stub(site)
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())

        BulletinManager._on_add(stub)

        # _populate_quarterly_from_snapshot runs on candidate with Q1 snapshot
        # (lb=80, ub=120, Q1 2026 = 90 days).
        assert site.forecast_q_min_q == pytest.approx(80.0)
        assert site.forecast_q_max_q == pytest.approx(120.0)
        assert site.quarterly_valid_from == pd.Timestamp(Q1_VF)
        assert site.quarterly_valid_to == pd.Timestamp(Q1_VT)
        expected_v_min = 80.0 * 90 * 86400 / 1_000_000
        expected_v_max = 120.0 * 90 * 86400 / 1_000_000
        assert site.forecast_v_min_q == pytest.approx(expected_v_min)
        assert site.forecast_v_max_q == pytest.approx(expected_v_max)

    def test_failed_add_does_not_set_derived_quarterly_attrs(self, monkeypatch):
        """Failed POST must not leave derived quarterly attrs on selected_site."""
        site = _make_reservoir_site()
        assert not hasattr(site, 'forecast_q_min_q')
        stub = _make_add_manager_stub(site)
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )

        BulletinManager._on_add(stub)

        assert not hasattr(site, 'forecast_q_min_q')
        assert not hasattr(site, 'quarterly_valid_from')


# ---------------------------------------------------------------------------
# 6. _on_add_m0 staging (mirrors _on_add)
# ---------------------------------------------------------------------------


class TestOnAddM0Staging:
    def _make_m0_manager_stub(self, site, bulletin_sites=None):
        if bulletin_sites is None:
            bulletin_sites = []
        wm = types.SimpleNamespace(
            horizon_selector=types.SimpleNamespace(value="month"),
            station_selector=types.SimpleNamespace(value=site.station_label),
            forecast_tabulator_m0=types.SimpleNamespace(
                value=_make_forecast_df(),
                selection=[0],
            ),
            date_picker=types.SimpleNamespace(value=_date(2025, 12, 15)),
            _quarterly_source_df=_q_source_row(),
            forecast_tabulator_q=types.SimpleNamespace(
                value=_q_tabulator_row(),
                selection=[0],
            ),
            add_to_bulletin_m0_popup=types.SimpleNamespace(
                object="", alert_type="success", visible=False,
            ),
        )
        cfg = types.SimpleNamespace(
            viz=types.SimpleNamespace(
                app_state=types.SimpleNamespace(pipeline_running=False),
            ),
        )
        dm = types.SimpleNamespace(
            sites_list=[site],
            long_forecasts_m0=pd.DataFrame(
                columns=["station_labels", "valid_from", "model_short", "date"],
            ),
            get_bulletin_metadata=MagicMock(return_value=(pd.Timestamp("2025-12-15"), 1, 2026)),
        )
        stub = types.SimpleNamespace(
            wm=wm,
            cfg=cfg,
            dm=dm,
            bulletin_sites=bulletin_sites,
            _horizon_context=lambda: ("month", 2026, 1),
            _month_hydration_params=lambda: (1, 2026, 31),
            _update_bulletin_table=MagicMock(),
            _show_popup_m0=MagicMock(),
        )
        return stub

    def test_successful_m0_add_promotes_to_bulletin_sites(self, monkeypatch):
        site = _make_reservoir_site()
        stub = self._make_m0_manager_stub(site)
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())

        BulletinManager._on_add_m0(stub)

        assert site in stub.bulletin_sites

    def test_failed_m0_add_restores_snapshot(self, monkeypatch):
        site = _make_reservoir_site()
        old_snap = {
            "model_type": "GBT",
            "valid_from": "2025-10-01",
            "valid_to": "2025-12-31",
            "forecasted_discharge": 90.0,
            "fc_lower": 70.0,
            "fc_upper": 110.0,
        }
        site.quarterly_snapshot = old_snap
        stub = self._make_m0_manager_stub(site, bulletin_sites=[site])
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )

        BulletinManager._on_add_m0(stub)

        assert site.quarterly_snapshot == old_snap

    def test_successful_m0_add_promotes_derived_quarterly_attrs(self, monkeypatch):
        """__dict__.update must carry derived attrs on the m0 path."""
        site = _make_reservoir_site()
        stub = self._make_m0_manager_stub(site)
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())

        BulletinManager._on_add_m0(stub)

        assert site.forecast_q_min_q == pytest.approx(80.0)
        assert site.forecast_q_max_q == pytest.approx(120.0)
        assert site.quarterly_valid_from == pd.Timestamp(Q1_VF)
        assert site.quarterly_valid_to == pd.Timestamp(Q1_VT)

    def test_failed_m0_first_add_leaves_site_not_in_bulletin_sites(self, monkeypatch):
        site = _make_reservoir_site()
        stub = self._make_m0_manager_stub(site)
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )

        BulletinManager._on_add_m0(stub)

        assert site not in stub.bulletin_sites

    def test_failed_m0_first_add_does_not_set_snapshot(self, monkeypatch):
        site = _make_reservoir_site()
        assert not hasattr(site, 'quarterly_snapshot')
        stub = self._make_m0_manager_stub(site)
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )

        BulletinManager._on_add_m0(stub)

        assert not hasattr(site, 'quarterly_snapshot')

    def test_failed_m0_add_does_not_set_derived_attrs(self, monkeypatch):
        site = _make_reservoir_site()
        assert not hasattr(site, 'forecast_q_min_q')
        stub = self._make_m0_manager_stub(site)
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )

        BulletinManager._on_add_m0(stub)

        assert not hasattr(site, 'forecast_q_min_q')
        assert not hasattr(site, 'quarterly_valid_from')


# ---------------------------------------------------------------------------
# 7. serialize_site — quarterly field
# ---------------------------------------------------------------------------


class TestSerializeSiteQuarterly:
    def _make_hydrated_site_month(self, snap=None):
        site = types.SimpleNamespace(
            code="19999",
            station_label="19999 - River R Punkt P",
            basin_ru="Basin B",
            river_name_ru="River R",
            forecast_model="LR",
            forecast_expected=50.0,
            forecast_q_min=40.0,
            forecast_q_max=60.0,
            forecast_v_min=0.5,
            forecast_v_max=0.6,
            forecast_norm=55.0,
            perc_norm=90.0,
        )
        if snap is not None:
            site.quarterly_snapshot = snap
        return site

    def test_month_no_snapshot_adds_quarterly_null(self):
        site = self._make_hydrated_site_month()
        row = bp.serialize_site(site, "month")
        assert "quarterly" in row
        assert row["quarterly"] is None

    def test_month_with_snapshot_adds_quarterly_object(self):
        snap = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 80.0,
            "fc_upper": 120.0,
        }
        site = self._make_hydrated_site_month(snap=snap)
        row = bp.serialize_site(site, "month")
        q = row["quarterly"]
        assert q is not None
        assert q["model_type"] == "LR"
        assert q["valid_from"] == Q1_VF
        assert q["valid_to"] == Q1_VT
        assert q["forecasted_discharge"] == pytest.approx(100.0)
        assert q["fc_lower"] == pytest.approx(80.0)
        assert q["fc_upper"] == pytest.approx(120.0)

    def test_month_snapshot_v_min_v_max_derived_from_dates(self):
        # Q1 2026 = 90 days inclusive; v = Q * 90 * 86400 / 1e6
        snap = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 100.0,
            "fc_upper": 100.0,
        }
        site = self._make_hydrated_site_month(snap=snap)
        row = bp.serialize_site(site, "month")
        q = row["quarterly"]
        expected_v = 100.0 * 90 * 86400 / 1_000_000
        assert q["v_min"] == pytest.approx(expected_v)
        assert q["v_max"] == pytest.approx(expected_v)

    def test_month_snapshot_null_bounds_gives_null_volumes(self):
        snap = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": None,
            "fc_upper": None,
        }
        site = self._make_hydrated_site_month(snap=snap)
        row = bp.serialize_site(site, "month")
        assert row["quarterly"]["v_min"] is None
        assert row["quarterly"]["v_max"] is None

    def test_non_month_horizon_omits_quarterly_key(self):
        site = types.SimpleNamespace(
            code="19999",
            station_label="19999 - River R Punkt P",
            basin_ru="Basin B",
            river_name_ru="River R",
            forecast_model="LR",
            forecast_expected=50.0,
            forecast_q_min=40.0,
            forecast_q_max=60.0,
            forecast_v_min=0.5,
            forecast_v_max=0.6,
            forecast_norm=55.0,
            perc_norm=90.0,
        )
        row = bp.serialize_site(site, "season")
        assert "quarterly" not in row

    def test_raw_values_not_rounded(self):
        # Stored value 123.456789 must survive without 3-sf rounding
        snap = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 123.456789,
            "fc_lower": 110.111222,
            "fc_upper": 135.999888,
        }
        site = self._make_hydrated_site_month(snap=snap)
        row = bp.serialize_site(site, "month")
        assert row["quarterly"]["forecasted_discharge"] == pytest.approx(123.456789)
        assert row["quarterly"]["fc_lower"] == pytest.approx(110.111222)
        assert row["quarterly"]["fc_upper"] == pytest.approx(135.999888)


# ---------------------------------------------------------------------------
# 10. assemble_bulletin_snapshot — period conflict
# ---------------------------------------------------------------------------


class TestAssembleBulletinSnapshotConflict:
    def _make_site_with_snap(self, snap, code="19999"):
        site = types.SimpleNamespace(
            code=code,
            station_label=f"{code} - River R Punkt P",
            basin_ru="Basin B",
            river_name_ru="River R",
            forecast_model="LR",
            forecast_expected=50.0,
            forecast_q_min=40.0,
            forecast_q_max=60.0,
            forecast_v_min=0.5,
            forecast_v_max=0.6,
            forecast_norm=55.0,
            perc_norm=90.0,
        )
        if snap is not None:
            site.quarterly_snapshot = snap
        return site

    def _run_assemble(self, bulletin_sites, selected_codes):
        """Call assemble_bulletin_snapshot with mocked API and bulletin loader."""
        import io
        bdf = pd.DataFrame([
            {"code": s.code, "year": 2026, "horizon_value": 1}
            for s in bulletin_sites
        ])

        with (
            patch.object(bp.tl, "get_date_for_pentad", return_value=None),
            patch("dashboard.bulletin_manager._load_bulletin_from_api", return_value=bulletin_sites),
            patch("src.db._read_data", return_value=bdf),
        ):
            dm = types.SimpleNamespace(sites_list=[])
            return bp.assemble_bulletin_snapshot(
                "month", selected_codes, dm, _date(2026, 1, 15),
            )

    def test_no_conflict_passes(self):
        snap = {"model_type": "LR", "valid_from": Q1_VF, "valid_to": Q1_VT,
                "forecasted_discharge": 100.0, "fc_lower": 80.0, "fc_upper": 120.0}
        s1 = self._make_site_with_snap(snap, "19999")
        s2 = self._make_site_with_snap(snap, "19998")
        result = self._run_assemble([s1, s2], ["19999", "19998"])
        assert "payload" in result

    def test_conflicting_periods_raise_value_error(self):
        snap_a = {"model_type": "LR", "valid_from": Q1_VF, "valid_to": Q1_VT,
                  "forecasted_discharge": 100.0, "fc_lower": 80.0, "fc_upper": 120.0}
        snap_b = {"model_type": "LR", "valid_from": "2026-04-01", "valid_to": "2026-06-30",
                  "forecasted_discharge": 110.0, "fc_lower": 85.0, "fc_upper": 130.0}
        s1 = self._make_site_with_snap(snap_a, "19999")
        s2 = self._make_site_with_snap(snap_b, "19998")

        with pytest.raises(ValueError, match="conflicting quarterly"):
            self._run_assemble([s1, s2], ["19999", "19998"])
