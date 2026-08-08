"""Unit tests for write-time re-hydration of the LR predictor.

Bug: the PREDICTOR column of a written pentad/decad bulletin was filled in
only for the station that happened to be loaded in the dashboard last —
every other station in the bulletin got an empty cell.

Cause: `db.get_linreg_predictor` is scoped to a single station code, so
`dm.linreg_predictor` only ever covers the currently loaded station, and
`update_site_attributes_with_linear_regression_predictor` sets
`site.linreg_predictor = None` for every site that frame does not cover.
Adding station A to the bulletin and then selecting station B wiped A's
predictor. `_on_write` re-hydrated the hydrograph envelope per site but not
the predictor. This file exercises:

  1. `rehydrate_sites_linreg_predictor` populates EVERY site from its own
     per-station fetch (the regression guard for the reported bug).
  2. It selects the same row the interactive path selects — the preceding
     period-in-year, latest date.
  3. It is empty/missing-column/exception-safe and never blanks a value it
     could not resolve.
  4. `_on_write` invokes it for pentad/decad and skips it for the long
     horizons, whose bulletins carry no PREDICTOR tag.

`dashboard.utils` has no Panel dependency and is imported normally.
`dashboard.bulletin_manager` does, so the heavy dependencies are mocked at
import time and `sys.modules` restored afterwards — mirroring the bootstrap
in test_bulletin_edit_persistence.py.
"""

import sys
import types
from unittest.mock import MagicMock

import pandas as pd
import pytest
from dashboard.utils import rehydrate_sites_linreg_predictor

# ---------------------------------------------------------------------------
# Bootstrap: mock heavy dashboard dependencies before importing the module.
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
]

_saved = {k: sys.modules[k] for k in _FAKE_KEYS if k in sys.modules}

try:
    for _mod in [
        "panel",
        "panel.viewable",
        "panel.widgets",
        "panel.layout",
        "panel.pane",
        "panel.template",
    ]:
        if _mod not in sys.modules:
            sys.modules[_mod] = MagicMock()

    if "src.gettext_config" not in sys.modules:
        _gc = types.ModuleType("src.gettext_config")
        _gc._ = lambda x: x  # no-op translation
        _gc.translation_manager = MagicMock()
        sys.modules["src.gettext_config"] = _gc

    if "dashboard.logger" not in sys.modules:
        _lg = types.ModuleType("dashboard.logger")
        _lg.setup_logger = MagicMock(return_value=MagicMock())
        sys.modules["dashboard.logger"] = _lg

    if "src.db" not in sys.modules:
        sys.modules["src.db"] = MagicMock()

    from dashboard import bulletin_manager  # noqa: E402

    BulletinManager = bulletin_manager.BulletinManager

finally:
    for _k in _FAKE_KEYS:
        if _k in _saved:
            sys.modules[_k] = _saved[_k]
        elif _k in sys.modules:
            del sys.modules[_k]
    del _saved, _FAKE_KEYS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_site(code, predictor=None):
    return types.SimpleNamespace(code=code, linreg_predictor=predictor)


def _predictor_frame(rows, hin="pentad_in_year"):
    """Frame shaped like `db.get_linreg_predictor` output.

    Each row is (period_in_year, date, predictor).
    """
    return pd.DataFrame(
        [
            {hin: period, "date": pd.Timestamp(date), "predictor": predictor}
            for period, date, predictor in rows
        ]
    )


class FakeDb:
    """Per-station predictor source; records the codes it was asked for."""

    def __init__(self, frames_by_code, hin="pentad_in_year"):
        self._frames = frames_by_code
        self._hin = hin
        self.requested = []

    def get_linreg_predictor(self, horizon, code):
        self.requested.append((horizon, code))
        return self._frames.get(code, pd.DataFrame())


# ---------------------------------------------------------------------------
# Tests: every site is hydrated from its own fetch
# ---------------------------------------------------------------------------


class TestEverySiteIsHydrated:
    def test_all_sites_get_their_own_predictor(self):
        """The reported bug: only the last-loaded station had a predictor."""
        db = FakeDb(
            {
                "99001": _predictor_frame([(25, "2026-05-05", 11.1)]),
                "99002": _predictor_frame([(25, "2026-05-05", 22.2)]),
                "99003": _predictor_frame([(25, "2026-05-05", 33.3)]),
            }
        )
        sites = [_make_site("99001"), _make_site("99002"), _make_site("99003")]

        rehydrate_sites_linreg_predictor(sites, "pentad", 26, db)

        assert [s.linreg_predictor for s in sites] == [
            pytest.approx(11.1),
            pytest.approx(22.2),
            pytest.approx(33.3),
        ]

    def test_each_site_is_fetched_with_its_own_code(self):
        """Guards the root cause: the predictor must not be read from a
        single dashboard-wide frame scoped to one station."""
        db = FakeDb({})
        sites = [_make_site("99001"), _make_site("99002")]

        rehydrate_sites_linreg_predictor(sites, "pentad", 26, db)

        assert db.requested == [("pentad", "99001"), ("pentad", "99002")]

    def test_previously_wiped_predictor_is_restored(self):
        """A site added to the bulletin, then wiped to None by a station
        switch, must come back with its own value at write time."""
        db = FakeDb({"99001": _predictor_frame([(25, "2026-05-05", 11.1)])})
        site = _make_site("99001", predictor=None)

        rehydrate_sites_linreg_predictor([site], "pentad", 26, db)

        assert site.linreg_predictor == pytest.approx(11.1)


# ---------------------------------------------------------------------------
# Tests: row selection matches the interactive path
# ---------------------------------------------------------------------------


class TestRowSelection:
    def test_reads_the_period_preceding_the_forecast_period(self):
        db = FakeDb(
            {
                "99001": _predictor_frame(
                    [
                        (24, "2026-04-30", 1.0),
                        (25, "2026-05-05", 2.0),
                        (26, "2026-05-10", 3.0),
                    ]
                )
            }
        )
        site = _make_site("99001")

        rehydrate_sites_linreg_predictor([site], "pentad", 26, db)

        assert site.linreg_predictor == pytest.approx(2.0)

    def test_latest_date_wins_within_the_period(self):
        db = FakeDb(
            {
                "99001": _predictor_frame(
                    [
                        (25, "2025-05-05", 9.9),
                        (25, "2026-05-05", 2.0),
                        (25, "2024-05-05", 8.8),
                    ]
                )
            }
        )
        site = _make_site("99001")

        rehydrate_sites_linreg_predictor([site], "pentad", 26, db)

        assert site.linreg_predictor == pytest.approx(2.0)

    def test_decade_horizon_uses_the_decad_period_column(self):
        db = FakeDb({"99001": _predictor_frame([(11, "2026-04-20", 5.5)], hin="decad_in_year")})
        site = _make_site("99001")

        rehydrate_sites_linreg_predictor([site], "decade", 12, db)

        assert site.linreg_predictor == pytest.approx(5.5)


# ---------------------------------------------------------------------------
# Tests: failures never blank a value or stop the write
# ---------------------------------------------------------------------------


class TestGracefulFailure:
    @pytest.mark.parametrize(
        "frame",
        [pd.DataFrame(), None],
        ids=["empty_frame", "none_frame"],
    )
    def test_no_data_leaves_existing_value_untouched(self, frame):
        db = FakeDb({"99001": frame})
        site = _make_site("99001", predictor=7.7)

        rehydrate_sites_linreg_predictor([site], "pentad", 26, db)

        assert site.linreg_predictor == pytest.approx(7.7)

    def test_missing_period_row_leaves_existing_value_untouched(self):
        db = FakeDb({"99001": _predictor_frame([(30, "2026-06-01", 1.0)])})
        site = _make_site("99001", predictor=7.7)

        rehydrate_sites_linreg_predictor([site], "pentad", 26, db)

        assert site.linreg_predictor == pytest.approx(7.7)

    def test_missing_predictor_column_leaves_existing_value_untouched(self):
        db = FakeDb(
            {"99001": pd.DataFrame([{"pentad_in_year": 25, "date": pd.Timestamp("2026-05-05")}])}
        )
        site = _make_site("99001", predictor=7.7)

        rehydrate_sites_linreg_predictor([site], "pentad", 26, db)

        assert site.linreg_predictor == pytest.approx(7.7)

    def test_one_failing_station_does_not_stop_the_others(self):
        class ExplodingDb(FakeDb):
            def get_linreg_predictor(self, horizon, code):
                if code == "99001":
                    raise RuntimeError("API unreachable")
                return super().get_linreg_predictor(horizon, code)

        db = ExplodingDb({"99002": _predictor_frame([(25, "2026-05-05", 22.2)])})
        sites = [_make_site("99001", predictor=7.7), _make_site("99002")]

        rehydrate_sites_linreg_predictor(sites, "pentad", 26, db)  # must not raise

        assert sites[0].linreg_predictor == pytest.approx(7.7)
        assert sites[1].linreg_predictor == pytest.approx(22.2)


# ---------------------------------------------------------------------------
# Tests: _on_write wires the re-hydration in
# ---------------------------------------------------------------------------


def _make_write_manager_stub(sites, horizon="pentad"):
    """A lightweight fake `self` sufficient to call `_on_write` unbound.

    Mirrors the stub in test_bulletin_edit_persistence.py — constructing a
    real BulletinManager would run _load_bulletin_from_api and wire Panel
    watchers.
    """
    wm = types.SimpleNamespace(
        basin_selector=types.SimpleNamespace(value="All basins"),
        horizon_selector=types.SimpleNamespace(value=horizon),
        write_bulletin_popup=types.SimpleNamespace(object=None, alert_type=None, visible=False),
        downloader=types.SimpleNamespace(refresh_file_list=MagicMock()),
    )
    dm = types.SimpleNamespace(
        get_bulletin_metadata=MagicMock(return_value=(pd.Timestamp("2026-05-05"), 26, 2026)),
        forecasts_all=pd.DataFrame(),
        sites_list=sites,
    )
    return types.SimpleNamespace(
        wm=wm,
        dm=dm,
        cfg=types.SimpleNamespace(env_file_path="/tmp/env"),
        _processing=types.SimpleNamespace(get_bulletin_header_info=MagicMock(return_value={})),
        bulletin_sites=sites,
        _write_to_excel=MagicMock(),
        _show_write_popup=MagicMock(),
    )


class TestOnWriteRehydratesPredictors:
    @pytest.fixture(autouse=True)
    def _stub_other_write_steps(self, monkeypatch):
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_hydrograph_stats", MagicMock())
        monkeypatch.setattr(bulletin_manager, "_populate_forecast_attributes", MagicMock())

    @pytest.mark.parametrize("horizon", ["pentad", "decade"])
    def test_short_horizons_rehydrate_predictors_for_all_bulletin_sites(self, monkeypatch, horizon):
        rehydrate_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_linreg_predictor", rehydrate_mock)
        sites = [
            types.SimpleNamespace(code="99001", forecasts=pd.DataFrame()),
            types.SimpleNamespace(code="99002", forecasts=pd.DataFrame()),
        ]
        fake_self = _make_write_manager_stub(sites, horizon=horizon)

        BulletinManager._on_write(fake_self, event=None)

        rehydrate_mock.assert_called_once()
        passed_sites, passed_horizon, passed_period = rehydrate_mock.call_args.args[:3]
        assert [s.code for s in passed_sites] == ["99001", "99002"]
        assert passed_horizon == horizon
        assert passed_period == 26

    @pytest.mark.parametrize("horizon", ["month", "season"])
    def test_long_horizons_skip_predictor_rehydration(self, monkeypatch, horizon):
        """The month/season bulletin templates carry no PREDICTOR tag, so the
        extra per-station API calls would be pure cost."""
        rehydrate_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_linreg_predictor", rehydrate_mock)
        sites = [types.SimpleNamespace(code="99001", forecasts=pd.DataFrame())]
        fake_self = _make_write_manager_stub(sites, horizon=horizon)

        BulletinManager._on_write(fake_self, event=None)

        rehydrate_mock.assert_not_called()

    def test_predictor_failure_does_not_block_the_write(self, monkeypatch):
        monkeypatch.setattr(
            bulletin_manager,
            "rehydrate_sites_linreg_predictor",
            MagicMock(side_effect=RuntimeError("API unreachable")),
        )
        sites = [types.SimpleNamespace(code="99001", forecasts=pd.DataFrame())]
        fake_self = _make_write_manager_stub(sites)

        BulletinManager._on_write(fake_self, event=None)  # must not raise

        fake_self._write_to_excel.assert_called_once()
        fake_self._show_write_popup.assert_called_once_with("Bulletin saved successfully")
