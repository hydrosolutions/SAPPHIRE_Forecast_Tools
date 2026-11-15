"""Save/reload roundtrip tests for the quarterly snapshot feature (PP-075 phase-E).

Covers:
  1. _site_to_records → _load_bulletin_from_api roundtrip preserves all quarterly
     attrs; fake persistence boundary uses deepcopy of records; no live quarter
     API fetch during reload.
  2. Frozen values: attrs loaded from the saved snapshot are unaffected by
     subsequent upstream source-frame changes.
  3. Main selected model change resolves candidate target period (flag ON) — the
     resolver receives the CANDIDATE (which carries the new tabulator selection),
     not the old selected_site.
  4. Retry works: after a failed Add leaves the site unchanged, a retry with
     valid data succeeds.
  5. Invalid one-sided / inverted bounds raise before POST: _save_bulletin_to_api
     is never called.
  6. _on_add_m0: same resolver / full-promotion guarantees as _on_add.

Synthetic station 19999 only. No real station codes or discharge values.
"""

from __future__ import annotations

import copy
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

    import os as _os
    sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))

    from dashboard import bulletin_manager  # noqa: E402

    _site_to_records = bulletin_manager._site_to_records
    _load_bulletin_from_api = bulletin_manager._load_bulletin_from_api
    _sanitize_quarterly_snapshot = bulletin_manager._sanitize_quarterly_snapshot
    _populate_quarterly_from_snapshot = bulletin_manager._populate_quarterly_from_snapshot
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
# Shared helpers
# ---------------------------------------------------------------------------

Q1_VF = "2026-01-01"
Q1_VT = "2026-03-31"


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


def _q_source_row(model="LR", vf=Q1_VF, vt=Q1_VT, lb=80.0, ub=120.0, q=100.0):
    return pd.DataFrame([{
        "model_short": model,
        "valid_from": pd.Timestamp(vf),
        "valid_to": pd.Timestamp(vt),
        "forecasted_discharge": q,
        "Q25": lb,
        "Q75": ub,
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


def _make_add_manager_stub(site, bulletin_sites=None, tabulator_model="LR",
                           q_source=None, q_tabulator=None):
    if bulletin_sites is None:
        bulletin_sites = []
    if q_source is None:
        q_source = _q_source_row()
    if q_tabulator is None:
        q_tabulator = _q_tabulator_row()
    wm = types.SimpleNamespace(
        horizon_selector=types.SimpleNamespace(value="month"),
        station_selector=types.SimpleNamespace(value=site.station_label),
        forecast_tabulator=types.SimpleNamespace(
            value=_make_forecast_df(model=tabulator_model),
            selection=[0],
        ),
        date_picker=types.SimpleNamespace(value=_date(2025, 12, 15)),
        _quarterly_source_df=q_source,
        forecast_tabulator_q=types.SimpleNamespace(
            value=q_tabulator,
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
        forecasts_all=pd.DataFrame(
            columns=["station_labels", "valid_from", "model_short", "date"]
        ),
        get_bulletin_metadata=MagicMock(return_value=(pd.Timestamp("2025-12-15"), 1, 2026)),
    )
    return types.SimpleNamespace(
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


def _make_m0_manager_stub(site, bulletin_sites=None, tabulator_model="LR",
                          q_source=None, q_tabulator=None, m0_df=None):
    if bulletin_sites is None:
        bulletin_sites = []
    if q_source is None:
        q_source = _q_source_row()
    if q_tabulator is None:
        q_tabulator = _q_tabulator_row()
    if m0_df is None:
        m0_df = pd.DataFrame(columns=["station_labels", "valid_from", "model_short", "date"])
    wm = types.SimpleNamespace(
        horizon_selector=types.SimpleNamespace(value="month"),
        station_selector=types.SimpleNamespace(value=site.station_label),
        forecast_tabulator_m0=types.SimpleNamespace(
            value=_make_forecast_df(model=tabulator_model),
            selection=[0],
        ),
        date_picker=types.SimpleNamespace(value=_date(2025, 12, 15)),
        _quarterly_source_df=q_source,
        forecast_tabulator_q=types.SimpleNamespace(
            value=q_tabulator,
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
        long_forecasts_m0=m0_df,
        get_bulletin_metadata=MagicMock(return_value=(pd.Timestamp("2025-12-15"), 1, 2026)),
    )
    return types.SimpleNamespace(
        wm=wm,
        cfg=cfg,
        dm=dm,
        bulletin_sites=bulletin_sites,
        _horizon_context=lambda: ("month", 2026, 1),
        _month_hydration_params=lambda: (1, 2026, 31),
        _update_bulletin_table=MagicMock(),
        _show_popup_m0=MagicMock(),
    )


# ---------------------------------------------------------------------------
# 1. Save → reload roundtrip
# ---------------------------------------------------------------------------


class TestSaveReloadRoundtrip:
    """_site_to_records output fed into _load_bulletin_from_api reproduces attrs."""

    def _build_records_df(self, site, horizon_type="month", year=2026, hv=1):
        """Call _site_to_records and deep-copy the records (simulating persistence)."""
        records = _site_to_records(horizon_type, year, hv, site)
        frozen = copy.deepcopy(records)
        return pd.DataFrame(frozen)

    def test_roundtrip_preserves_quarterly_snapshot(self):
        site = _make_reservoir_site()
        site.forecasts = _make_forecast_df()
        site.quarterly_snapshot = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 80.0,
            "fc_upper": 120.0,
        }
        df = self._build_records_df(site)

        # Fresh site (no snapshot yet)
        fresh_site = _make_reservoir_site()
        fresh_site.forecasts = _make_forecast_df()

        with patch.object(bulletin_manager.db, '_read_data', return_value=df):
            loaded = _load_bulletin_from_api("month", 2026, 1, [fresh_site])

        assert len(loaded) == 1
        loaded_site = loaded[0]
        snap = loaded_site.quarterly_snapshot
        assert snap is not None
        assert snap["model_type"] == "LR"
        assert snap["valid_from"] == Q1_VF
        assert snap["valid_to"] == Q1_VT
        assert snap["forecasted_discharge"] == pytest.approx(100.0)
        assert snap["fc_lower"] == pytest.approx(80.0)
        assert snap["fc_upper"] == pytest.approx(120.0)

    def test_roundtrip_populates_derived_quarterly_attrs(self):
        """After reload, forecast_q_min_q / quarterly_valid_from etc. are set
        from the stored snapshot — not from a live API call."""
        site = _make_reservoir_site()
        site.forecasts = _make_forecast_df()
        site.quarterly_snapshot = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 80.0,
            "fc_upper": 120.0,
        }
        df = self._build_records_df(site)

        fresh_site = _make_reservoir_site()
        fresh_site.forecasts = _make_forecast_df()

        with patch.object(bulletin_manager.db, '_read_data', return_value=df):
            loaded = _load_bulletin_from_api("month", 2026, 1, [fresh_site])

        ls = loaded[0]
        assert ls.forecast_q_min_q == pytest.approx(80.0)
        assert ls.forecast_q_max_q == pytest.approx(120.0)
        assert ls.quarterly_valid_from == pd.Timestamp(Q1_VF)
        assert ls.quarterly_valid_to == pd.Timestamp(Q1_VT)
        # Volume from Q1 2026 = 90 days inclusive
        expected_v = 80.0 * 90 * 86400 / 1_000_000
        assert ls.forecast_v_min_q == pytest.approx(expected_v)

    def test_roundtrip_no_live_quarter_api_fetch(self):
        """Reload uses only the stored snapshot; no DB call beyond _read_data."""
        site = _make_reservoir_site()
        site.forecasts = _make_forecast_df()
        site.quarterly_snapshot = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 80.0,
            "fc_upper": 120.0,
        }
        df = self._build_records_df(site)

        fresh_site = _make_reservoir_site()
        fresh_site.forecasts = _make_forecast_df()

        # Track all db method calls during reload.
        db_mock = MagicMock()
        db_mock._read_data.return_value = df

        with patch.object(bulletin_manager, 'db', db_mock):
            loaded = _load_bulletin_from_api("month", 2026, 1, [fresh_site])

        # Only _read_data should have been called; no get_long_forecasts_* calls.
        called_methods = {call[0] for call in db_mock.method_calls}
        assert '_read_data' in called_methods
        for m in ('get_long_forecasts_season', 'get_long_forecasts'):
            assert m not in called_methods, f"Unexpected live fetch: {m}"

    def test_roundtrip_null_snapshot_clears_quarterly_attrs(self):
        """A site with no snapshot reloads with cleared quarterly attrs."""
        site = _make_reservoir_site()
        site.forecasts = _make_forecast_df()
        # No quarterly_snapshot attr → _site_to_records emits quarterly_snapshot=None
        df = self._build_records_df(site)

        fresh_site = _make_reservoir_site()
        fresh_site.forecasts = _make_forecast_df()

        with patch.object(bulletin_manager.db, '_read_data', return_value=df):
            loaded = _load_bulletin_from_api("month", 2026, 1, [fresh_site])

        ls = loaded[0]
        assert ls.quarterly_snapshot is None
        assert ls.forecast_q_min_q is None
        assert ls.forecast_q_max_q is None


# ---------------------------------------------------------------------------
# 2. Frozen values: upstream source changes don't affect loaded attrs
# ---------------------------------------------------------------------------


class TestFrozenValues:
    """Loaded bulletin quarterly attrs come from the saved snapshot, not live data."""

    @pytest.mark.parametrize("add_method", ["_on_add", "_on_add_m0"])
    def test_frozen_lifecycle_through_add_persist_reload_serialize(
        self, monkeypatch, add_method
    ):
        """Full frozen-copy lifecycle: Add writes Q1 snapshot into fake persistence,
        live quarterly card is then swapped to 999, reload from frozen records into a
        fresh site, serialize_site returns the original Q1 bounds/dates/volumes."""
        # Capture what db._save_data receives (fake persistence boundary).
        persisted_records: list[dict] = []

        def fake_save_data(_service, _resource, records):
            for r in records:
                persisted_records.append(copy.deepcopy(r))

        site = _make_reservoir_site()
        # q_src discharge = 100.0; q_tab discharge = 107.5 (edited, distinct from
        # source) so the lifecycle test verifies an operator-edited discharge survives
        # Add → persist → reload → serialize_site unchanged.
        q_src = _q_source_row(lb=80.0, ub=120.0, q=100.0)
        q_tab = _q_tabulator_row(lb=80.0, ub=120.0, q=107.5)

        if add_method == "_on_add":
            stub = _make_add_manager_stub(site, q_source=q_src, q_tabulator=q_tab)
            add_fn = BulletinManager._on_add
        else:
            stub = _make_m0_manager_stub(site, q_source=q_src, q_tabulator=q_tab)
            add_fn = BulletinManager._on_add_m0

        with patch.object(bulletin_manager.db, '_save_data', fake_save_data):
            add_fn(stub)

        assert persisted_records, "Add must have written at least one record"

        # Swap the live quarterly card to completely different values so a re-Add
        # or a live fetch would yield 999 instead of 80/120.
        stub.wm._quarterly_source_df = _q_source_row(lb=999.0, ub=999.0, q=999.0)
        stub.wm.forecast_tabulator_q = types.SimpleNamespace(
            value=_q_tabulator_row(lb=999.0, ub=999.0, q=999.0),
            selection=[0],
        )

        # Reload from the frozen records (deepcopy = real persistence boundary).
        frozen_df = pd.DataFrame(copy.deepcopy(persisted_records))
        fresh_site = _make_reservoir_site()

        with patch.object(bulletin_manager.db, '_read_data', return_value=frozen_df):
            loaded = _load_bulletin_from_api("month", 2026, 1, [fresh_site])

        assert len(loaded) == 1, "reload must return exactly one site"
        ls = loaded[0]

        # serialize_site reads from site.quarterly_snapshot (the frozen snapshot,
        # not the live 999 card).
        row = bp.serialize_site(ls, "month")
        q = row["quarterly"]
        assert q is not None
        # Edited discharge (107.5, not source 100.0) must survive the full cycle.
        assert q["forecasted_discharge"] == pytest.approx(107.5)
        assert q["fc_lower"] == pytest.approx(80.0)
        assert q["fc_upper"] == pytest.approx(120.0)
        assert q["valid_from"] == Q1_VF
        assert q["valid_to"] == Q1_VT
        # Q1 2026: 90 days inclusive → v = Q * 90 * 86400 / 1e6
        assert q["v_min"] == pytest.approx(80.0 * 90 * 86400 / 1_000_000)
        assert q["v_max"] == pytest.approx(120.0 * 90 * 86400 / 1_000_000)
        # Live-frame values must NOT appear.
        assert q["fc_lower"] != pytest.approx(999.0)

    def test_changing_forecast_tabulator_does_not_alter_saved_q_attrs(self, monkeypatch):
        """After a successful Add, changing the forecast tabulator selection (without
        calling Add again) does not mutate the site's quarterly attrs."""
        site = _make_reservoir_site()
        stub = _make_add_manager_stub(site)
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())

        BulletinManager._on_add(stub)
        q_min_after_add = site.forecast_q_min_q

        # Simulate changing the tabulator selection without re-adding
        stub.wm.forecast_tabulator.value = _make_forecast_df(model="GBT", q=999.0)
        stub.wm.forecast_tabulator.selection = [0]

        # Site attrs are unchanged — only another Add call would update them.
        assert site.forecast_q_min_q == pytest.approx(q_min_after_add)


# ---------------------------------------------------------------------------
# 3. Selected model change resolves candidate target (flag ON)
# ---------------------------------------------------------------------------


class TestResolvesCandidateTarget:
    """With skill_lead_aware_enabled(), the resolver receives the CANDIDATE."""

    def test_resolver_uses_candidate_model_not_old_site_model(self, monkeypatch):
        """When the operator switches from model LR to GBT in the tabulator,
        _resolve_month_target_period must use candidate.forecasts (GBT),
        not selected_site.forecasts (LR)."""
        # Build forecasts_all with:
        #   LR → valid_from = 2026-01-01 (target Jan)
        #   GBT → valid_from = 2026-02-01 (target Feb)
        station_label = "19999 - River R вдхр P"
        forecasts_all = pd.DataFrame([
            {
                "station_labels": station_label,
                "model_short": "LR",
                "valid_from": pd.Timestamp("2026-01-01"),
                "date": pd.Timestamp("2025-12-15"),
            },
            {
                "station_labels": station_label,
                "model_short": "GBT",
                "valid_from": pd.Timestamp("2026-02-01"),
                "date": pd.Timestamp("2025-12-15"),
            },
        ])

        site = _make_reservoir_site()
        # Site's current (old) forecasts carry LR
        site.forecasts = _make_forecast_df(model="LR")

        # Tabulator now shows GBT selected (operator switched model)
        stub = _make_add_manager_stub(site, tabulator_model="GBT")
        stub.dm.forecasts_all = forecasts_all

        # Enable the flag
        monkeypatch.setattr(
            bulletin_manager, 'skill_lead_aware_enabled', lambda: True,
        )
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())

        BulletinManager._on_add(stub)

        # With the fix, candidate.forecasts = GBT row, so resolver narrows to
        # GBT rows → bulletin_target_period = (2, 2026).
        # Without the fix it would narrow to LR rows → (1, 2026).
        assert site.bulletin_target_period == (2, 2026)

    def test_resolver_uses_candidate_model_m0_path(self, monkeypatch):
        """Same guarantee on the _on_add_m0 path."""
        station_label = "19999 - River R вдхр P"
        m0_df = pd.DataFrame([
            {
                "station_labels": station_label,
                "model_short": "LR",
                "valid_from": pd.Timestamp("2026-01-01"),
                "date": pd.Timestamp("2025-12-15"),
            },
            {
                "station_labels": station_label,
                "model_short": "GBT",
                "valid_from": pd.Timestamp("2026-02-01"),
                "date": pd.Timestamp("2025-12-15"),
            },
        ])

        site = _make_reservoir_site()
        site.forecasts = _make_forecast_df(model="LR")

        stub = _make_m0_manager_stub(site, tabulator_model="GBT", m0_df=m0_df)

        monkeypatch.setattr(
            bulletin_manager, 'skill_lead_aware_enabled', lambda: True,
        )
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())

        BulletinManager._on_add_m0(stub)

        assert site.bulletin_target_period == (2, 2026)


# ---------------------------------------------------------------------------
# 4. Retry works after a failed Add
# ---------------------------------------------------------------------------


class TestRetryAfterFailure:
    def test_retry_after_failed_add_succeeds(self, monkeypatch):
        """First Add fails (site unchanged); second Add succeeds."""
        site = _make_reservoir_site()
        stub = _make_add_manager_stub(site)

        # First attempt: POST fails
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("transient error")),
        )
        BulletinManager._on_add(stub)

        assert site not in stub.bulletin_sites
        assert not hasattr(site, 'quarterly_snapshot')

        # Second attempt: POST succeeds
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        BulletinManager._on_add(stub)

        assert site in stub.bulletin_sites
        snap = getattr(site, 'quarterly_snapshot', None)
        assert snap is not None
        assert snap["model_type"] == "LR"
        assert site.forecast_q_min_q == pytest.approx(80.0)

    @pytest.mark.parametrize("add_method", ["_on_add", "_on_add_m0"])
    def test_cell_edit_after_failed_add_carries_old_quarterly_snapshot(
        self, monkeypatch, add_method
    ):
        """After a failed Add whose candidate had Q300/240/360, the REAL
        _on_bulletin_edit delta handler must POST the old Q100/80/120 quarterly
        snapshot while the monthly forecast bounds DO reflect the new δ.

        Uses visibly different stored vs candidate values so a regression that
        leaks candidate state into the site would be caught by the bounds check.
        """
        # Stored snapshot: Q=100, lb=80, ub=120.
        old_snap = {
            "model_type": "LR",
            "valid_from": Q1_VF,
            "valid_to": Q1_VT,
            "forecasted_discharge": 100.0,
            "fc_lower": 80.0,
            "fc_upper": 120.0,
        }
        site = _make_reservoir_site()
        site.quarterly_snapshot = old_snap
        site.forecasts = _make_forecast_df(model="LR", q=50.0, lb=40.0, ub=60.0)

        # Candidate quarterly card: distinctly different Q=300, lb=240, ub=360.
        candidate_q_src = _q_source_row(q=300.0, lb=240.0, ub=360.0)
        candidate_q_tab = _q_tabulator_row(q=300.0, lb=240.0, ub=360.0)

        if add_method == "_on_add":
            add_stub = _make_add_manager_stub(
                site, bulletin_sites=[site],
                q_source=candidate_q_src, q_tabulator=candidate_q_tab,
            )
            add_fn = BulletinManager._on_add
        else:
            add_stub = _make_m0_manager_stub(
                site, bulletin_sites=[site],
                q_source=candidate_q_src, q_tabulator=candidate_q_tab,
            )
            add_fn = BulletinManager._on_add_m0

        # Fail the Add: candidate (240/360) must not contaminate site.
        monkeypatch.setattr(
            bulletin_manager, "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("POST failed")),
        )
        add_fn(add_stub)

        # Confirm site keeps the old snapshot, not the candidate's 240/360.
        assert site.quarterly_snapshot["fc_lower"] == pytest.approx(80.0)
        assert site.quarterly_snapshot["fc_upper"] == pytest.approx(120.0)

        # Attach bulletin_tabulator onto add_stub.wm so the candidate quarterly
        # card (Q300/240/360) is still present when _on_bulletin_edit calls
        # _capture_quarterly_snapshot.  A fresh wm would lose that card.
        bulletin_tab_df = pd.DataFrame([{
            "Hydropost": site.station_label,
            "Model": "LR",
            "Basin": "Basin B",
            "Forecasted discharge": 50.0,
            "Forecast lower bound": 40.0,
            "Forecast upper bound": 60.0,
            "δ": 10.0,
            "s/σ": 0.5,
            "MAE": 2.0,
            "Accuracy": 85.0,
        }])
        add_stub.wm.bulletin_tabulator = types.SimpleNamespace(
            value=bulletin_tab_df,
            patch=MagicMock(),
        )
        # Use add_stub directly — same wm, so candidatequarter Q300/240/360 is capturable.
        edit_stub = add_stub

        # Capture the POST payload at the _save_bulletin_to_api boundary.
        captured_sites: list = []

        def capture_save(_ht, _year, _hv, sites):
            captured_sites.extend(copy.deepcopy(sites))

        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", capture_save)

        # Invoke the REAL _on_bulletin_edit with a δ edit (Q=50, δ=8 → lb=42, ub=58).
        delta_event = types.SimpleNamespace(row=0, column="delta", value=8.0)
        BulletinManager._on_bulletin_edit(edit_stub, delta_event)

        assert captured_sites, "_on_bulletin_edit must POST after a δ edit"
        saved = captured_sites[0]

        # Quarterly snapshot must be the original (80/120), not the failed candidate.
        assert saved.quarterly_snapshot["fc_lower"] == pytest.approx(80.0)
        assert saved.quarterly_snapshot["fc_upper"] == pytest.approx(120.0)
        assert saved.quarterly_snapshot["forecasted_discharge"] == pytest.approx(100.0)

        # Monthly forecast bounds must reflect the δ edit (Q=50 ± δ=8).
        assert saved.forecasts["Forecast lower bound"].iloc[0] == pytest.approx(42.0)
        assert saved.forecasts["Forecast upper bound"].iloc[0] == pytest.approx(58.0)


# ---------------------------------------------------------------------------
# 5. Invalid bounds — POST not attempted
# ---------------------------------------------------------------------------


class TestInvalidBoundsNoPOST:
    """One-sided or inverted quarterly bounds must abort before POST."""

    def _run_add_with_bounds(self, monkeypatch, lb, ub):
        site = _make_reservoir_site()
        stub = _make_add_manager_stub(
            site,
            q_tabulator=_q_tabulator_row(lb=lb, ub=ub),
        )
        save_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", save_mock)
        BulletinManager._on_add(stub)
        return site, stub, save_mock

    def test_inverted_bounds_no_post(self, monkeypatch):
        site, stub, save_mock = self._run_add_with_bounds(monkeypatch, lb=120.0, ub=80.0)
        save_mock.assert_not_called()
        assert site not in stub.bulletin_sites
        # Error popup shown
        stub._show_popup.assert_called_once()
        assert stub._show_popup.call_args[1].get("alert_type") == "danger"

    def test_one_sided_lower_missing_no_post(self, monkeypatch):
        site, stub, save_mock = self._run_add_with_bounds(
            monkeypatch, lb=float("nan"), ub=120.0
        )
        save_mock.assert_not_called()
        assert site not in stub.bulletin_sites

    def test_one_sided_upper_missing_no_post(self, monkeypatch):
        site, stub, save_mock = self._run_add_with_bounds(
            monkeypatch, lb=80.0, ub=float("nan")
        )
        save_mock.assert_not_called()
        assert site not in stub.bulletin_sites

    def test_valid_bounds_does_post(self, monkeypatch):
        """Control: valid symmetric bounds reach POST."""
        site, stub, save_mock = self._run_add_with_bounds(
            monkeypatch, lb=80.0, ub=120.0
        )
        save_mock.assert_called_once()
        assert site in stub.bulletin_sites
