"""FD-029 P1: the quarterly card renderer, caption, and bulletin input.

Covers the renderer's `filter_by_date=False` bypass and all-NaN accuracy
guard (`src/vizualization.py`), the caption built from selected rows
instead of stale site attributes (`dashboard/plot_manager.py`), and the
month bulletin's interim `get_long_forecasts_quarter` input
(`dashboard/bulletin_manager.py`) — see the plan's "Tests" section,
items 4, 6, 8, 9, 10.

Station code `19999` throughout (no real station codes).
"""

import functools
import json
import types
import warnings
from datetime import date
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import requests
from dashboard import bulletin_manager, widgets
from dashboard.plot_manager import PlotManager, _format_quarterly_forecast_info
from src import db, vizualization
from src.site import SapphireSite

STATION_CODE = "19999"
STATION_LABEL = f"{STATION_CODE} - Test River"


def _make_mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


class _FakeCard:
    """Minimal stand-in for a Panel card with a visible attribute."""

    def __init__(self, visible=True):
        self.visible = visible


def _make_reservoir_site(code=STATION_CODE):
    return SapphireSite(code=code, punkt_name_ru="Тест вдхр", station_label="x")


def _make_stub_pm(quarterly_df):
    """PlotManager instance with __init__ bypassed (see tests/test_widgets.py:60-140).

    Only the attributes read by update_quarterly_summary_tabulator are
    populated; `_cfg.viz` is the REAL vizualization module so the renderer
    is genuinely exercised, not faked out.
    """
    pm = object.__new__(PlotManager)
    pm._ = lambda s: s
    pm._cfg = types.SimpleNamespace(viz=vizualization)
    pm.summary_table_q_card = _FakeCard()

    site = _make_reservoir_site()
    pm._dm = types.SimpleNamespace(
        sites_list=[site],
        long_forecasts_quarter=quarterly_df,
    )

    pm._wm = types.SimpleNamespace(
        horizon_selector=types.SimpleNamespace(value="month"),
        station_selector=types.SimpleNamespace(value=STATION_LABEL),
        model_checkbox=types.SimpleNamespace(
            options={
                "LR_Base": "LR_Base",
                "LR_SM": "LR_SM",
                "Skilled Mean": "Skilled Mean",
                "Naive Mean": "Naive Mean",
                "GBT": "GBT",
            }
        ),
        range_selector="delta",
        range_slider=0,
        date_picker=types.SimpleNamespace(value=date(2026, 1, 1)),
        forecast_tabulator_q=widgets.create_forecast_tabulator(),
        forecast_info_q=types.SimpleNamespace(object=""),
    )
    return pm, site


def _quarter_row(**overrides):
    row = {
        "station_labels": STATION_LABEL,
        "code": STATION_CODE,
        "model_short": "GBT",
        "date": pd.Timestamp("2026-03-25"),
        "valid_from": pd.Timestamp("2026-04-01"),
        "valid_to": pd.Timestamp("2026-06-30"),
        "forecasted_discharge": 100.0,
        "Q25": 90.0,
        "Q75": 110.0,
        "delta": np.nan,
        "sdivsigma": np.nan,
        "mae": np.nan,
        "accuracy": 80.0,
        "quarter_in_year": 2,
        "year": 2026,
        "is_native": False,
        "quarter_issue_date": pd.Timestamp("2026-03-25"),
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Test 4: renderer through the card — Problem 3 (mixed a/b/c dates hide models)
# ---------------------------------------------------------------------------


class TestCardSelectionThroughPlotManager:
    def test_mixed_issue_dates_keep_every_model_on_the_card(self):
        """LR_Base/LR_SM dated 2026-03-25 (native) and Skilled Mean dated
        2026-04-01, all Apr-Jun. Trunk's renderer reduces to `date.max()`
        (2026-04-01) and drops both LR rows off the card (live kghm
        defect). After P1 the card selects the whole target quarter (max
        valid_from) and calls the renderer with filter_by_date=False, so
        all three models survive."""
        quarterly_df = pd.DataFrame(
            [
                _quarter_row(model_short="LR_Base", is_native=True, accuracy=90.0),
                _quarter_row(model_short="LR_SM", is_native=True, accuracy=88.0),
                _quarter_row(
                    model_short="Skilled Mean",
                    date=pd.Timestamp("2026-04-01"),
                    is_native=False,
                    accuracy=70.0,
                ),
            ]
        )
        pm, _site = _make_stub_pm(quarterly_df)

        pm.update_quarterly_summary_tabulator()

        assert pm.summary_table_q_card.visible is True
        shown_models = set(pm._wm.forecast_tabulator_q.value["Model"])
        assert shown_models == {"LR_Base", "LR_SM", "Skilled Mean"}, (
            f"Expected all three models on the card, got {shown_models!r}"
        )


# ---------------------------------------------------------------------------
# Test 6: caption uses the selected rows, never stale site attributes
# ---------------------------------------------------------------------------


class TestQuarterlyCaptionSelectedRows:
    def test_stale_site_attributes_do_not_leak_into_caption(self):
        stale_site = types.SimpleNamespace(
            quarterly_valid_from=pd.Timestamp("2026-04-01"),
            quarterly_valid_to=pd.Timestamp("2026-06-30"),
        )

        caption = _format_quarterly_forecast_info(
            lambda s: s,
            stale_site,
            None,
            valid_from=pd.Timestamp("2026-07-01"),
            valid_to=pd.Timestamp("2026-09-30"),
            quarter_issue_date=pd.Timestamp("2026-06-25"),
        )

        assert "Jul 2026" in caption
        assert "Sep 2026" in caption
        assert "Apr 2026" not in caption


# ---------------------------------------------------------------------------
# Test 8: delta bounds for rows without native quantiles
# ---------------------------------------------------------------------------


class TestDeltaBoundsOnCard:
    def test_delta_fills_bounds_and_missing_delta_stays_empty(self):
        quarterly_df = pd.DataFrame(
            [
                _quarter_row(
                    model_short="GBT",
                    delta=5.0,
                    forecasted_discharge=100.0,
                    Q25=np.nan,
                    Q75=np.nan,
                    accuracy=80.0,
                ),
                _quarter_row(
                    model_short="Skilled Mean",
                    delta=5.0,
                    forecasted_discharge=100.0,
                    Q25=np.nan,
                    Q75=np.nan,
                    accuracy=75.0,
                ),
                _quarter_row(
                    model_short="Naive Mean",
                    delta=np.nan,
                    forecasted_discharge=100.0,
                    Q25=np.nan,
                    Q75=np.nan,
                    accuracy=60.0,
                ),
            ]
        )
        pm, _site = _make_stub_pm(quarterly_df)

        pm.update_quarterly_summary_tabulator()

        assert pm.summary_table_q_card.visible is True
        table = pm._wm.forecast_tabulator_q.value.set_index("Model")
        assert table.loc["GBT", "Forecast lower bound"] == 95.0
        assert table.loc["GBT", "Forecast upper bound"] == 105.0
        assert table.loc["Skilled Mean", "Forecast lower bound"] == 95.0
        assert table.loc["Skilled Mean", "Forecast upper bound"] == 105.0
        assert pd.isna(table.loc["Naive Mean", "Forecast lower bound"])
        assert pd.isna(table.loc["Naive Mean", "Forecast upper bound"])


# ---------------------------------------------------------------------------
# Test 9: all-NaN accuracy must not raise/warn out of idxmax
# ---------------------------------------------------------------------------


class TestAllNanAccuracyGuard:
    def test_all_nan_accuracy_selects_first_row_without_warning(self):
        forecasts_all = pd.DataFrame(
            [
                {
                    "station_labels": STATION_LABEL,
                    "date": pd.Timestamp("2026-03-25"),
                    "model_short": "GBT",
                    "forecasted_discharge": 100.0,
                    "Q25": 90.0,
                    "Q75": 110.0,
                    "delta": np.nan,
                    "sdivsigma": np.nan,
                    "mae": np.nan,
                    "accuracy": np.nan,
                },
                {
                    "station_labels": STATION_LABEL,
                    "date": pd.Timestamp("2026-03-25"),
                    "model_short": "Naive Mean",
                    "forecasted_discharge": 90.0,
                    "Q25": 80.0,
                    "Q75": 100.0,
                    "delta": np.nan,
                    "sdivsigma": np.nan,
                    "mae": np.nan,
                    "accuracy": np.nan,
                },
            ]
        )
        wm = types.SimpleNamespace(horizon_selector=types.SimpleNamespace(value="quarter"))
        model_selection = types.SimpleNamespace(options={"GBT": "GBT", "Naive Mean": "Naive Mean"})
        tabulator = widgets.create_forecast_tabulator()

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            result = vizualization.create_forecast_summary_tabulator(
                lambda s: s,
                wm,
                forecasts_all,
                STATION_LABEL,
                "2026-03-25",
                model_selection,
                "delta",
                0,
                tabulator,
            )

        assert result.selection == [0]


# ---------------------------------------------------------------------------
# Test 10: month bulletin's interim get_long_forecasts_quarter input
# ---------------------------------------------------------------------------


class TestBulletinQuarterInput:
    def test_head1_no_longer_picks_a_rolling_backfill(self, monkeypatch, tmp_path):
        """Through bulletin_manager._populate_forecast_attributes (via
        _load_bulletin_from_api) with the REAL get_long_forecasts_quarter
        bound to today=2026-12-26: a native Q1 2027 row (2026-12-25) plus a
        rolling Feb-Apr 2027 row (2026-12-26, LATER) -> after P1 the
        rolling row is dropped by the calendar-quarter filter, so head(1)
        on the remaining single row gives quarterly_valid_from=2027-01-01.
        On trunk the rolling row wins head(1) by date (2027-02-01)."""
        config_dir = tmp_path / "kghm_schedule"
        config_dir.mkdir()
        (config_dir / "quarter.json").write_text(
            json.dumps(
                {
                    "operational_month_lead_time": 1,
                    "operational_issue_day": 25,
                }
            )
        )
        monkeypatch.setenv("ieasyforecast_configuration_path", str(tmp_path))
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_configuration", "kghm_schedule")
        monkeypatch.setenv("ieasyhydroforecast_ml_long_term_supported_modes", "quarter")

        native_row = {
            "id": 500,
            "horizon_type": "quarter",
            "horizon_value": 1,
            "code": STATION_CODE,
            "date": "2026-12-25",
            "model_type": "LR_Base",
            "model_type_description": "x",
            "valid_from": "2027-01-01",
            "valid_to": "2027-03-31",
            "flag": 0,
            "composition": "",
            "q": 150.0,
            "q_obs": None,
            "q_xgb": None,
            "q_lgbm": None,
            "q_catboost": None,
            "q_loc": None,
            "q05": 130.0,
            "q10": 135.0,
            "q25": 140.0,
            "q50": 150.0,
            "q75": 160.0,
            "q90": 165.0,
            "q95": 170.0,
        }
        rolling_row = {
            **native_row,
            "id": 501,
            "date": "2026-12-26",
            "valid_from": "2027-02-01",
            "valid_to": "2027-04-30",
            "q": 999.0,
        }
        bulletin_record = {
            "code": STATION_CODE,
            "model_type": "LR_Base",
            "forecasted_discharge": 100.0,
            "fc_lower": 90.0,
            "fc_upper": 110.0,
            "delta": 1.0,
            "sdivsigma": 2.0,
            "mae": 3.0,
            "accuracy": 90.0,
        }

        def mock_get(url, **kwargs):
            if "/long-forecast/" in url:
                return _make_mock_response([native_row, rolling_row])
            if "/bulletin/" in url:
                return _make_mock_response([bulletin_record])
            return _make_mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)

        # `db.get_long_forecasts_quarter` is this test's own (guaranteed
        # real) `from src import db` — always the genuine implementation
        # regardless of collection order.
        #
        # `bulletin_manager.db`, however, is whatever `bulletin_manager.py`'s
        # own `from src import db` bound AT ITS IMPORT TIME:
        # test_bulletin_header_date.py (collected earlier, alphabetically)
        # temporarily fakes `sys.modules["src.db"]` while it imports
        # `dashboard.bulletin_manager` for the first time, so in a
        # full-suite run `bulletin_manager.db` is a stale MagicMock, never
        # the real module, no matter what `src.db` is restored to
        # afterward. Standalone (this file alone, or before that other
        # file), `bulletin_manager.db` IS the real `src.db` module — the
        # SAME object as this test's own `db` import.
        #
        # That identity matters: mutating an ATTRIBUTE of `bulletin_manager.
        # db` (e.g. `bulletin_manager.db._read_data = ...`) mutates the
        # REAL module's own global whenever it happens to be the same
        # object (the standalone/unpolluted case) — which then corrupts
        # `get_long_forecasts_quarter`'s OWN internal `_read_data_paginated`
        # -> `_read_data` call (it resolves `_read_data` via its own
        # module globals, the same ones just overwritten), so it stops
        # calling `requests.get` for the long-forecast fetch and returns
        # empty. That is what made the very first version of this test
        # order-dependent — passing only when the pollution above made
        # `bulletin_manager.db` a SEPARATE (fake) object.
        #
        # Fix: never mutate the real `db` module's attributes. Instead,
        # replace `bulletin_manager.db` outright with a fresh, disposable
        # namespace exposing exactly what `_populate_forecast_attributes`/
        # `_load_bulletin_from_api` call on it. This is unconditional and
        # object-identity-independent: it does not matter whether
        # `bulletin_manager.db` started out real or already-polluted —
        # either way it ends up pointing at this namespace for the
        # duration of the test, and the REAL `src.db` module (whose
        # globals `real_get_quarter` closes over) is never touched, so its
        # own internal `_read_data_paginated`/`_read_data` calls still go
        # through the unmodified `requests.get` mock above.
        real_get_quarter = db.get_long_forecasts_quarter
        fake_bulletin_db = types.SimpleNamespace(
            get_long_forecasts_quarter=functools.partial(
                real_get_quarter, today=date(2026, 12, 26)
            ),
            _read_data=lambda *a, **k: pd.DataFrame([bulletin_record]),
        )
        monkeypatch.setattr(bulletin_manager, "db", fake_bulletin_db)
        monkeypatch.setattr(
            bulletin_manager, "hydrate_month_hydrograph_stats", lambda *a, **k: None
        )

        site = _make_reservoir_site()

        bulletin_manager._load_bulletin_from_api("month", 2027, 1, [site])

        assert site.quarterly_valid_from == pd.Timestamp("2027-01-01"), (
            f"Expected the native Q1 row to win head(1), got "
            f"{getattr(site, 'quarterly_valid_from', 'MISSING')!r}"
        )
