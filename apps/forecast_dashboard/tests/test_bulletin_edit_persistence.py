"""Unit tests for in-cell edit persistence of the Forecast bulletin table.

Bug: editing a value cell in `bulletin_tabulator` (forecasted discharge,
bounds, delta, s/sigma, MAE, accuracy) was silently lost — there was no
on-edit callback wiring the edit back into `site.forecasts`, and
`_on_write` never refreshed the site attributes the Excel writer reads
from before writing. This file exercises:

  1. `BulletinManager._on_bulletin_edit` writes the edited value into the
     matching `site.forecasts` row.
  2. `_on_bulletin_edit` persists the edit via `_save_bulletin_to_api`.
  3. Editing an identity column (Hydropost/Model/Basin) is a no-op.
  4. Column resolution works for both the translated display name and the
     tabulator `field` name form of `event.column`.
  5. Editing δ re-derives the forecast lower/upper bounds (Q ∓ δ) in
     `site.forecasts`, persists them, and patches them into the visible table.
  6. `_populate_forecast_attributes` (extracted from `_load_bulletin_from_api`)
     correctly hydrates a site's short-term forecast attributes from
     `site.forecasts` using the real `SapphireSite.get_forecast_attributes_for_site`.

bulletin_manager.py imports `panel as pn` which is not installed in the test
environment. We mock the heavy dependencies at import time so the module can
be imported and its callables tested in isolation. Mirrors the bootstrap in
test_bulletin_header_date.py / test_bulletin_month_hydration.py.
"""

import sys
import types
from unittest.mock import MagicMock

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Bootstrap: mock heavy dashboard dependencies before importing the module.
# The fakes are injected temporarily, the import is performed, then
# sys.modules is restored to its prior state so that other test modules
# collected later are not contaminated by our stubs.
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
    _populate_forecast_attributes = bulletin_manager._populate_forecast_attributes
    _apply_delta_bounds = bulletin_manager._apply_delta_bounds

finally:
    for _k in _FAKE_KEYS:
        if _k in _saved:
            sys.modules[_k] = _saved[_k]
        elif _k in sys.modules:
            del sys.modules[_k]
    del _saved, _FAKE_KEYS

# src/site.py has no heavy dependencies, so it can be imported normally.
from src.site import SapphireSite  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeEditEvent:
    """Stand-in for a Panel Tabulator CellEditEvent."""

    def __init__(self, column, row, value):
        self.column = column
        self.row = row
        self.value = value


def _make_bulletin_row(
    station_label="99001 - Test Site",
    model="LR",
    basin="Test Basin",
    forecasted_discharge=100.0,
):
    """One row as it would appear in bulletin_tabulator.value."""
    return {
        "Hydropost": station_label,
        "Model": model,
        "Basin": basin,
        "Forecasted discharge": forecasted_discharge,
        "Forecast lower bound": 90.0,
        "Forecast upper bound": 110.0,
        "δ": 1.0,
        "s/σ": 2.0,
        "MAE": 3.0,
        "Accuracy": 90.0,
    }


def _make_site_with_forecasts(
    station_label="99001 - Test Site",
    model="LR",
    forecasted_discharge=100.0,
):
    """A minimal site object exposing the attrs _on_bulletin_edit relies on."""
    site = types.SimpleNamespace()
    site.station_label = station_label
    site.forecasts = pd.DataFrame(
        [
            {
                "Model": model,
                "Forecasted discharge": forecasted_discharge,
                "Forecast lower bound": 90.0,
                "Forecast upper bound": 110.0,
                "δ": 1.0,
                "s/σ": 2.0,
                "MAE": 3.0,
                "Accuracy": 90.0,
            }
        ]
    )
    return site


def _make_manager_stub(site, horizon_context=("pentad", 2026, 26)):
    """A lightweight fake `self` sufficient to call _on_bulletin_edit unbound.

    Constructing a real BulletinManager is impractical here: its __init__
    calls _load_bulletin_from_api and wires several Panel widget watchers.
    Since _on_bulletin_edit only touches self.wm.bulletin_tabulator,
    self.bulletin_sites and self._horizon_context(), a SimpleNamespace with
    just those attributes is enough to exercise the real method body via
    BulletinManager._on_bulletin_edit(fake_self, event).
    """
    bulletin_df = pd.DataFrame(
        [
            _make_bulletin_row(
                station_label=site.station_label,
                model=site.forecasts["Model"].iloc[0],
                forecasted_discharge=site.forecasts["Forecasted discharge"].iloc[0],
            )
        ]
    )
    wm = types.SimpleNamespace(
        bulletin_tabulator=types.SimpleNamespace(value=bulletin_df, patch=MagicMock())
    )
    return types.SimpleNamespace(
        wm=wm,
        bulletin_sites=[site],
        _horizon_context=lambda: horizon_context,
    )


# ---------------------------------------------------------------------------
# Tests: _on_bulletin_edit updates site.forecasts
# ---------------------------------------------------------------------------


class TestOnBulletinEditUpdatesForecasts:
    def test_edit_by_field_name_updates_matching_forecasts_row(self, monkeypatch):
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        site = _make_site_with_forecasts()
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="forecasted_discharge", row=0, value="123.4")

        BulletinManager._on_bulletin_edit(fake_self, event)

        mask = site.forecasts["Model"] == "LR"
        assert site.forecasts.loc[mask, "Forecasted discharge"].iloc[0] == pytest.approx(123.4)

    def test_edit_by_display_name_updates_matching_forecasts_row(self, monkeypatch):
        """event.column may arrive as the translated display name instead of
        the tabulator field name; both must resolve to the same cell."""
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        site = _make_site_with_forecasts()
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="Forecasted discharge", row=0, value="77.7")

        BulletinManager._on_bulletin_edit(fake_self, event)

        mask = site.forecasts["Model"] == "LR"
        assert site.forecasts.loc[mask, "Forecasted discharge"].iloc[0] == pytest.approx(77.7)

    def test_edit_coerces_string_value_to_float(self, monkeypatch):
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        site = _make_site_with_forecasts()
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="fc_lower", row=0, value="85.5")

        BulletinManager._on_bulletin_edit(fake_self, event)

        mask = site.forecasts["Model"] == "LR"
        new_value = site.forecasts.loc[mask, "Forecast lower bound"].iloc[0]
        assert new_value == pytest.approx(85.5)
        assert isinstance(new_value, float)

    def test_edit_leaves_other_rows_untouched(self, monkeypatch):
        """Editing one site's row must not disturb another model's row on
        the same site."""
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        site = _make_site_with_forecasts(model="LR")
        # Add a second model row for the same site.
        site.forecasts = pd.concat(
            [
                site.forecasts,
                pd.DataFrame(
                    [
                        {
                            "Model": "TFT",
                            "Forecasted discharge": 200.0,
                            "Forecast lower bound": 190.0,
                            "Forecast upper bound": 210.0,
                            "δ": 1.0,
                            "s/σ": 2.0,
                            "MAE": 3.0,
                            "Accuracy": 90.0,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="forecasted_discharge", row=0, value="123.4")

        BulletinManager._on_bulletin_edit(fake_self, event)

        tft_row = site.forecasts.loc[site.forecasts["Model"] == "TFT"]
        assert tft_row["Forecasted discharge"].iloc[0] == pytest.approx(200.0)


# ---------------------------------------------------------------------------
# Tests: editing δ re-derives the forecast bounds
# ---------------------------------------------------------------------------


class TestDeltaEditUpdatesBounds:
    """δ and the bounds describe one interval (Q ∓ δ, as
    processing.calculate_forecast_range builds it), so an edited δ must move
    both bounds instead of leaving the row internally inconsistent."""

    @pytest.mark.parametrize("column", ["delta", "δ"])
    def test_delta_edit_recomputes_both_bounds(self, monkeypatch, column):
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        site = _make_site_with_forecasts(forecasted_discharge=100.0)
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column=column, row=0, value="5")

        BulletinManager._on_bulletin_edit(fake_self, event)

        row = site.forecasts.loc[site.forecasts["Model"] == "LR"].iloc[0]
        assert row["δ"] == pytest.approx(5.0)
        assert row["Forecast lower bound"] == pytest.approx(95.0)
        assert row["Forecast upper bound"] == pytest.approx(105.0)

    def test_recomputed_bounds_are_rounded_to_three_decimals(self, monkeypatch):
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        site = _make_site_with_forecasts(forecasted_discharge=10.0)
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="delta", row=0, value=0.1234)

        BulletinManager._on_bulletin_edit(fake_self, event)

        row = site.forecasts.loc[site.forecasts["Model"] == "LR"].iloc[0]
        assert row["Forecast lower bound"] == pytest.approx(9.877)
        assert row["Forecast upper bound"] == pytest.approx(10.123)

    def test_recomputed_bounds_are_persisted_to_the_api(self, monkeypatch):
        save_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", save_mock)
        site = _make_site_with_forecasts(forecasted_discharge=100.0)
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="delta", row=0, value="5")

        BulletinManager._on_bulletin_edit(fake_self, event)

        saved_site = save_mock.call_args.args[-1][0]
        row = saved_site.forecasts.loc[saved_site.forecasts["Model"] == "LR"].iloc[0]
        assert row["Forecast lower bound"] == pytest.approx(95.0)
        assert row["Forecast upper bound"] == pytest.approx(105.0)

    def test_recomputed_bounds_are_patched_into_the_displayed_table(self, monkeypatch):
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        site = _make_site_with_forecasts(forecasted_discharge=100.0)
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="delta", row=0, value="5")

        BulletinManager._on_bulletin_edit(fake_self, event)

        patch_mock = fake_self.wm.bulletin_tabulator.patch
        patch_mock.assert_called_once()
        patches = patch_mock.call_args.args[0]
        assert patches["Forecast lower bound"] == [(0, pytest.approx(95.0))]
        assert patches["Forecast upper bound"] == [(0, pytest.approx(105.0))]

    def test_delta_edit_leaves_other_model_rows_untouched(self, monkeypatch):
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        site = _make_site_with_forecasts(model="LR", forecasted_discharge=100.0)
        site.forecasts = pd.concat(
            [
                site.forecasts,
                pd.DataFrame(
                    [
                        {
                            "Model": "TFT",
                            "Forecasted discharge": 200.0,
                            "Forecast lower bound": 190.0,
                            "Forecast upper bound": 210.0,
                            "δ": 1.0,
                            "s/σ": 2.0,
                            "MAE": 3.0,
                            "Accuracy": 90.0,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="delta", row=0, value="5")

        BulletinManager._on_bulletin_edit(fake_self, event)

        tft = site.forecasts.loc[site.forecasts["Model"] == "TFT"].iloc[0]
        assert tft["Forecast lower bound"] == pytest.approx(190.0)
        assert tft["Forecast upper bound"] == pytest.approx(210.0)

    def test_non_delta_edit_does_not_touch_bounds(self, monkeypatch):
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        site = _make_site_with_forecasts(forecasted_discharge=100.0)
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="sdivsigma", row=0, value="0.4")

        BulletinManager._on_bulletin_edit(fake_self, event)

        row = site.forecasts.loc[site.forecasts["Model"] == "LR"].iloc[0]
        assert row["Forecast lower bound"] == pytest.approx(90.0)
        assert row["Forecast upper bound"] == pytest.approx(110.0)

    def test_non_numeric_delta_leaves_bounds_untouched(self, monkeypatch):
        """A δ the operator typed as text cannot yield bounds; the stored
        bounds must be left alone rather than blanked."""
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", MagicMock())
        site = _make_site_with_forecasts(forecasted_discharge=100.0)
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="delta", row=0, value="not a number")

        BulletinManager._on_bulletin_edit(fake_self, event)

        row = site.forecasts.loc[site.forecasts["Model"] == "LR"].iloc[0]
        assert row["Forecast lower bound"] == pytest.approx(90.0)
        assert row["Forecast upper bound"] == pytest.approx(110.0)
        fake_self.wm.bulletin_tabulator.patch.assert_not_called()

    def test_display_patch_failure_does_not_lose_the_edit(self, monkeypatch):
        """Refreshing the visible cells is cosmetic — a failure there must
        neither raise nor prevent the recomputed bounds from being saved."""
        save_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", save_mock)
        site = _make_site_with_forecasts(forecasted_discharge=100.0)
        fake_self = _make_manager_stub(site)
        fake_self.wm.bulletin_tabulator.patch = MagicMock(
            side_effect=RuntimeError("frontend gone")
        )
        event = FakeEditEvent(column="delta", row=0, value="5")

        BulletinManager._on_bulletin_edit(fake_self, event)  # must not raise

        row = site.forecasts.loc[site.forecasts["Model"] == "LR"].iloc[0]
        assert row["Forecast lower bound"] == pytest.approx(95.0)
        save_mock.assert_called_once()


class TestApplyDeltaBounds:
    """Direct unit tests of the `_apply_delta_bounds` helper."""

    def _frame(self, discharge=100.0):
        return pd.DataFrame(
            [
                {
                    "Model": "LR",
                    "Forecasted discharge": discharge,
                    "Forecast lower bound": 90.0,
                    "Forecast upper bound": 110.0,
                }
            ]
        )

    def test_returns_true_and_updates_bounds(self):
        df = self._frame()
        mask = df["Model"] == "LR"

        assert _apply_delta_bounds(df, mask, 2.5) is True
        assert df.loc[0, "Forecast lower bound"] == pytest.approx(97.5)
        assert df.loc[0, "Forecast upper bound"] == pytest.approx(102.5)

    def test_missing_discharge_column_is_a_noop(self):
        df = pd.DataFrame([{"Model": "LR", "Forecast lower bound": 90.0}])
        mask = df["Model"] == "LR"

        assert _apply_delta_bounds(df, mask, 2.5) is False
        assert df.loc[0, "Forecast lower bound"] == pytest.approx(90.0)

    @pytest.mark.parametrize("delta", [None, "", "abc", float("nan")])
    def test_unusable_delta_is_a_noop(self, delta):
        df = self._frame()
        mask = df["Model"] == "LR"

        assert _apply_delta_bounds(df, mask, delta) is False
        assert df.loc[0, "Forecast lower bound"] == pytest.approx(90.0)
        assert df.loc[0, "Forecast upper bound"] == pytest.approx(110.0)

    def test_missing_discharge_value_yields_undefined_bounds(self):
        """With no Q there is no Q ∓ δ; the bounds become NaN rather than
        keeping values that contradict the new δ."""
        df = self._frame(discharge=float("nan"))
        mask = df["Model"] == "LR"

        assert _apply_delta_bounds(df, mask, 2.5) is True
        assert pd.isna(df.loc[0, "Forecast lower bound"])
        assert pd.isna(df.loc[0, "Forecast upper bound"])


# ---------------------------------------------------------------------------
# Tests: _on_bulletin_edit persists via _save_bulletin_to_api
# ---------------------------------------------------------------------------


class TestOnBulletinEditPersistence:
    def test_edit_triggers_save_to_api_with_edited_site(self, monkeypatch):
        save_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", save_mock)
        site = _make_site_with_forecasts()
        fake_self = _make_manager_stub(site, horizon_context=("pentad", 2026, 26))
        event = FakeEditEvent(column="forecasted_discharge", row=0, value="123.4")

        BulletinManager._on_bulletin_edit(fake_self, event)

        save_mock.assert_called_once_with("pentad", 2026, 26, [site])
        saved_site = save_mock.call_args.args[-1][0]
        mask = saved_site.forecasts["Model"] == "LR"
        assert saved_site.forecasts.loc[mask, "Forecasted discharge"].iloc[0] == pytest.approx(
            123.4
        )

    def test_save_failure_is_caught_and_logged(self, monkeypatch):
        """A raised exception from the API save must not propagate — the
        in-cell edit already happened, and the UI callback must not crash
        the dashboard."""
        monkeypatch.setattr(
            bulletin_manager,
            "_save_bulletin_to_api",
            MagicMock(side_effect=RuntimeError("API unreachable")),
        )
        site = _make_site_with_forecasts()
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="forecasted_discharge", row=0, value="123.4")

        BulletinManager._on_bulletin_edit(fake_self, event)  # must not raise

        mask = site.forecasts["Model"] == "LR"
        assert site.forecasts.loc[mask, "Forecasted discharge"].iloc[0] == pytest.approx(123.4)


# ---------------------------------------------------------------------------
# Tests: identity columns are read-only / no-ops
# ---------------------------------------------------------------------------


class TestOnBulletinEditIdentityColumnsAreNoOps:
    @pytest.mark.parametrize("column", ["station_label", "Hydropost"])
    def test_editing_hydropost_is_a_noop(self, monkeypatch, column):
        save_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", save_mock)
        site = _make_site_with_forecasts()
        before = site.forecasts.copy(deep=True)
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column=column, row=0, value="Some Other Station")

        BulletinManager._on_bulletin_edit(fake_self, event)

        pd.testing.assert_frame_equal(site.forecasts, before)
        save_mock.assert_not_called()

    @pytest.mark.parametrize("column", ["model_short", "Model"])
    def test_editing_model_is_a_noop(self, monkeypatch, column):
        save_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", save_mock)
        site = _make_site_with_forecasts()
        before = site.forecasts.copy(deep=True)
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column=column, row=0, value="TFT")

        BulletinManager._on_bulletin_edit(fake_self, event)

        pd.testing.assert_frame_equal(site.forecasts, before)
        save_mock.assert_not_called()

    @pytest.mark.parametrize("column", ["basin_ru", "Basin"])
    def test_editing_basin_is_a_noop(self, monkeypatch, column):
        save_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", save_mock)
        site = _make_site_with_forecasts()
        before = site.forecasts.copy(deep=True)
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column=column, row=0, value="Some Other Basin")

        BulletinManager._on_bulletin_edit(fake_self, event)

        pd.testing.assert_frame_equal(site.forecasts, before)
        save_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: row resolution failures are handled gracefully
# ---------------------------------------------------------------------------


class TestOnBulletinEditGracefulFailure:
    def test_unknown_site_is_dropped_without_raising(self, monkeypatch):
        save_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", save_mock)
        site = _make_site_with_forecasts(station_label="99001 - Test Site")
        fake_self = _make_manager_stub(site)
        # Point the tabulator row at a station not present in bulletin_sites.
        fake_self.wm.bulletin_tabulator.value = pd.DataFrame(
            [_make_bulletin_row(station_label="99999 - Unknown Site")]
        )
        event = FakeEditEvent(column="forecasted_discharge", row=0, value="1.0")

        BulletinManager._on_bulletin_edit(fake_self, event)  # must not raise

        save_mock.assert_not_called()

    def test_unknown_model_is_dropped_without_raising(self, monkeypatch):
        save_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", save_mock)
        site = _make_site_with_forecasts(model="LR")
        fake_self = _make_manager_stub(site)
        # Tabulator row references a model that does not exist in site.forecasts.
        fake_self.wm.bulletin_tabulator.value = pd.DataFrame(
            [_make_bulletin_row(station_label=site.station_label, model="TFT")]
        )
        event = FakeEditEvent(column="forecasted_discharge", row=0, value="1.0")

        BulletinManager._on_bulletin_edit(fake_self, event)  # must not raise

        save_mock.assert_not_called()

    def test_out_of_range_row_index_is_dropped_without_raising(self, monkeypatch):
        save_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager, "_save_bulletin_to_api", save_mock)
        site = _make_site_with_forecasts()
        fake_self = _make_manager_stub(site)
        event = FakeEditEvent(column="forecasted_discharge", row=99, value="1.0")

        BulletinManager._on_bulletin_edit(fake_self, event)  # must not raise

        save_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: _populate_forecast_attributes (extracted helper), short-term branch
# ---------------------------------------------------------------------------


def _make_write_popup_stub():
    """A minimal `self.wm.write_bulletin_popup` stand-in (a real `pn.pane.Alert`
    in production, here a plain namespace with the attrs `_show_write_popup`
    sets)."""
    return types.SimpleNamespace(object=None, alert_type=None, visible=False)


def _make_write_manager_stub(sites, basin_selector_value="All basins", horizon="pentad"):
    """A lightweight fake `self` sufficient to call `_on_write` unbound.

    Constructing a real BulletinManager is impractical here (see
    `_make_manager_stub`); `_on_write` only touches `self.bulletin_sites`,
    `self.wm`, `self.dm`, `self._processing`, `self._write_to_excel`, and
    `self._show_write_popup`, so a SimpleNamespace with just those attributes
    is enough to exercise the real method body via
    `BulletinManager._on_write(fake_self, event)`.
    """
    wm = types.SimpleNamespace(
        basin_selector=types.SimpleNamespace(value=basin_selector_value),
        horizon_selector=types.SimpleNamespace(value=horizon),
        write_bulletin_popup=_make_write_popup_stub(),
        downloader=types.SimpleNamespace(refresh_file_list=MagicMock()),
    )
    dm = types.SimpleNamespace(
        get_bulletin_metadata=MagicMock(return_value=(pd.Timestamp("2026-07-01"), 26, 2026)),
        forecasts_all=pd.DataFrame(),
        sites_list=sites,
    )
    cfg = types.SimpleNamespace(env_file_path="/tmp/env")
    processing = types.SimpleNamespace(get_bulletin_header_info=MagicMock(return_value={}))
    return types.SimpleNamespace(
        wm=wm,
        dm=dm,
        cfg=cfg,
        _processing=processing,
        bulletin_sites=sites,
        _write_to_excel=MagicMock(),
        _show_write_popup=MagicMock(),
    )


# ---------------------------------------------------------------------------
# Tests: _show_write_popup (transient "Write bulletin" result alert)
# ---------------------------------------------------------------------------


class TestShowWritePopup:
    def test_show_write_popup_sets_message_and_visible(self, monkeypatch):
        periodic_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager.pn.state, "add_periodic_callback", periodic_mock)
        wm = types.SimpleNamespace(write_bulletin_popup=_make_write_popup_stub())
        fake_self = types.SimpleNamespace(wm=wm)

        BulletinManager._show_write_popup(fake_self, "X")

        assert fake_self.wm.write_bulletin_popup.object == "X"
        assert fake_self.wm.write_bulletin_popup.alert_type == "success"
        assert fake_self.wm.write_bulletin_popup.visible is True
        periodic_mock.assert_called_once()
        assert periodic_mock.call_args.args[1] == 3000
        assert periodic_mock.call_args.kwargs.get("count") == 1

    def test_show_write_popup_danger_type(self, monkeypatch):
        periodic_mock = MagicMock()
        monkeypatch.setattr(bulletin_manager.pn.state, "add_periodic_callback", periodic_mock)
        wm = types.SimpleNamespace(write_bulletin_popup=_make_write_popup_stub())
        fake_self = types.SimpleNamespace(wm=wm)

        BulletinManager._show_write_popup(fake_self, "Y", alert_type="danger")

        assert fake_self.wm.write_bulletin_popup.alert_type == "danger"


# ---------------------------------------------------------------------------
# Tests: _on_write calls _show_write_popup on success / error
# ---------------------------------------------------------------------------


class TestOnWriteShowsWritePopup:
    def test_on_write_success_shows_success_popup(self, monkeypatch):
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_hydrograph_stats", MagicMock())
        monkeypatch.setattr(bulletin_manager, "_populate_forecast_attributes", MagicMock())
        site = types.SimpleNamespace(code="99001", forecasts=pd.DataFrame())
        fake_self = _make_write_manager_stub([site])

        BulletinManager._on_write(fake_self, event=None)

        fake_self._show_write_popup.assert_called_once_with("Bulletin saved successfully")
        fake_self.wm.downloader.refresh_file_list.assert_called_once()

    def test_on_write_error_shows_danger_popup(self, monkeypatch):
        monkeypatch.setattr(bulletin_manager, "rehydrate_sites_hydrograph_stats", MagicMock())
        monkeypatch.setattr(bulletin_manager, "_populate_forecast_attributes", MagicMock())
        site = types.SimpleNamespace(code="99001", forecasts=pd.DataFrame())
        fake_self = _make_write_manager_stub([site])
        fake_self._write_to_excel = MagicMock(side_effect=RuntimeError("disk full"))

        BulletinManager._on_write(fake_self, event=None)  # must not raise

        fake_self._show_write_popup.assert_called_once_with(
            "Failed to write bulletin", alert_type="danger"
        )


class TestPopulateForecastAttributesShortTermBranch:
    """Exercises the real SapphireSite.get_forecast_attributes_for_site via
    the extracted _populate_forecast_attributes helper (the `else` branch,
    used for pentad/decade horizons)."""

    def test_short_term_branch_sets_attributes_from_site_forecasts(self):
        site = SapphireSite(code="99001")
        site.punkt_name_ru = "Test Punkt"
        site.forecasts = pd.DataFrame(
            {
                "Forecasted discharge": [12.5],
                "Forecast lower bound": [10.0],
                "Forecast upper bound": [15.0],
                "Model": ["LR"],
            }
        )

        _populate_forecast_attributes(site, "pentad", 2026, 26)

        assert site.forecast_expected == 12.5
        assert site.forecast_lower_bound == 10.0
        assert site.forecast_upper_bound == 15.0

    def test_short_term_branch_reflects_edited_value(self):
        """Simulates the write-time refresh: an in-cell edit changes
        site.forecasts, and _populate_forecast_attributes must re-derive
        forecast_expected from the edited value (not a stale attribute)."""
        site = SapphireSite(code="99001")
        site.punkt_name_ru = "Test Punkt"
        site.forecasts = pd.DataFrame(
            {
                "Forecasted discharge": [12.5],
                "Forecast lower bound": [10.0],
                "Forecast upper bound": [15.0],
                "Model": ["LR"],
            }
        )
        _populate_forecast_attributes(site, "pentad", 2026, 26)
        assert site.forecast_expected == 12.5

        # Simulate a live in-cell edit landing in site.forecasts.
        site.forecasts.loc[site.forecasts["Model"] == "LR", "Forecasted discharge"] = 999.0

        _populate_forecast_attributes(site, "pentad", 2026, 26)

        assert site.forecast_expected == 999.0
