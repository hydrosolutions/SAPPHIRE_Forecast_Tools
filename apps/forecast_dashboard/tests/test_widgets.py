"""Unit tests for forecast_dashboard/dashboard/widgets.py and PlotManager.

Covers create_horizon_selector() with ML forecasts enabled, disabled,
and the default (backwards-compatible) argument.

Also covers PlotManager.set_forecast_cards_visibility() card-hiding logic
without requiring a running Panel server.
"""

import sys
import os
import types

# Make the dashboard package importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dashboard import widgets
from dashboard.plot_manager import PlotManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _patch_gettext(monkeypatch):
    """Monkeypatch dashboard.widgets._ to an identity function."""
    monkeypatch.setattr(widgets, "_", lambda s: s)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_create_horizon_selector_with_ml_enabled(monkeypatch):
    """All four horizons are present when ML forecasts are enabled."""
    _patch_gettext(monkeypatch)
    widget = widgets.create_horizon_selector(True)
    assert set(widget.options.values()) == {"pentad", "decade", "month", "season"}
    assert widget.value == "pentad"


def test_create_horizon_selector_with_ml_disabled(monkeypatch):
    """Only short-term horizons are present when ML forecasts are disabled."""
    _patch_gettext(monkeypatch)
    widget = widgets.create_horizon_selector(False)
    assert set(widget.options.values()) == {"pentad", "decade"}
    assert widget.value == "pentad"


def test_create_horizon_selector_default_matches_ml_enabled(monkeypatch):
    """Default call (no arg) exposes all four horizons — backwards compatible."""
    _patch_gettext(monkeypatch)
    widget = widgets.create_horizon_selector()
    assert set(widget.options.values()) == {"pentad", "decade", "month", "season"}
    assert widget.value == "pentad"


# ---------------------------------------------------------------------------
# PlotManager.set_forecast_cards_visibility — quarterly card hide/show logic
# ---------------------------------------------------------------------------

class _FakeCard:
    """Minimal stand-in for a Panel card with a visible attribute."""
    def __init__(self, visible=True):
        self.visible = visible


class _FakeWarning:
    """Minimal stand-in for forecast_warning pane."""
    def __init__(self):
        self.visible = True


def _make_stub_pm():
    """Return a PlotManager instance with its __init__ bypassed.

    Only the attributes accessed by set_forecast_cards_visibility are
    populated — avoids the Panel/server/API dependencies of the real __init__.
    """
    pm = object.__new__(PlotManager)
    # Attributes read by the four-card loop
    pm.linreg_card = _FakeCard()
    pm.hydrograph_card = _FakeCard()
    pm.skill_metrics_card = _FakeCard()
    pm.skill_table_card = _FakeCard()
    # Month-0 summary card (already handled correctly)
    pm.summary_table_m0_card = _FakeCard()
    # Quarterly summary card (the subject of the bug fix)
    pm.summary_table_q_card = _FakeCard()
    # Summary table card (sizing_mode / height branch)
    pm.summary_table_card = _FakeCard()
    pm.summary_table_card.sizing_mode = "stretch_both"
    pm.summary_table_card.height = None
    # WidgetManager stub — only forecast_warning and forecast_summary_table needed
    wm = types.SimpleNamespace(
        forecast_warning=_FakeWarning(),
        forecast_summary_table=types.SimpleNamespace(value=["row1", "row2"]),
    )
    pm._wm = wm
    return pm


class TestSetForecastCardsVisibility:
    """set_forecast_cards_visibility hides the quarterly card on non-month horizons."""

    def test_quarterly_card_hidden_when_not_is_month(self):
        """Switching away from month (is_month=False) must hide summary_table_q_card."""
        pm = _make_stub_pm()
        pm.summary_table_q_card.visible = True  # start visible (simulates month horizon)

        pm.set_forecast_cards_visibility(visible=False, is_month=False)

        assert pm.summary_table_q_card.visible is False, (
            "summary_table_q_card must be hidden when is_month=False"
        )

    def test_quarterly_card_not_force_hidden_when_is_month(self):
        """Calling with is_month=True must NOT force-hide the quarterly card.

        Visibility-on is the responsibility of update_quarterly_summary_tabulator().
        """
        pm = _make_stub_pm()
        pm.summary_table_q_card.visible = True  # pre-set visible

        pm.set_forecast_cards_visibility(visible=False, is_month=True)

        assert pm.summary_table_q_card.visible is True, (
            "set_forecast_cards_visibility must not hide summary_table_q_card "
            "when is_month=True"
        )

    def test_quarterly_card_absent_does_not_raise(self):
        """If summary_table_q_card is absent the method must not raise."""
        pm = _make_stub_pm()
        del pm.summary_table_q_card  # simulate old layout without the card

        # Should complete without AttributeError
        pm.set_forecast_cards_visibility(visible=False, is_month=False)


# ---------------------------------------------------------------------------
# TestGetPredictorsWarning — dynamic year column, no hardcoded "2026"
# ---------------------------------------------------------------------------

import datetime
import numpy as np
import pandas as pd


class _FixedDateTimeClass:
    """Stand-in for the datetime.datetime class, frozen at 2030-06-15."""

    @staticmethod
    def now():
        # Construct the return value via the real datetime.date to avoid
        # any circular reference with the monkeypatched attribute.
        return _FixedDateTimeClass._FIXED_DT

    _FIXED_DT = datetime.datetime(2030, 6, 15, 12, 0, 0)


class _FakeDtModule:
    """Minimal stand-in for the ``datetime`` module as used in widgets.py.

    widgets.py does ``import datetime as dt`` then calls
    ``dt.datetime.now().date()``.  Replacing ``widgets.dt`` with this object
    keeps the real ``datetime`` module untouched.
    """

    datetime = _FixedDateTimeClass


def _fake_station(label="15013 - Test"):
    return types.SimpleNamespace(value=label)


def _hydrograph_df(station_label, date_val, year_col, discharge_val):
    """Build a minimal hydrograph_day_all DataFrame."""
    return pd.DataFrame({
        "station_labels": [station_label],
        "date": [pd.to_datetime(date_val)],
        year_col: [discharge_val],
    })


class TestGetPredictorsWarning:
    """get_predictors_warning uses the current year dynamically (no hardcoded 2026)."""

    def _patch_date(self, monkeypatch):
        # Replace the entire ``dt`` name in widgets so dt.datetime.now()
        # returns 2030-06-15 without mutating the real datetime module.
        monkeypatch.setattr(widgets, "dt", _FakeDtModule)

    # ------------------------------------------------------------------
    # Regression: current-year column present and has a valid value → None
    # ------------------------------------------------------------------
    def test_current_year_value_present_returns_none(self, monkeypatch):
        """REGRESSION: with year_col='2030' having a value, no warning is issued.

        Before the fix this raises KeyError because it tries to read column '2026'.
        After the fix it reads '2030' and returns None.
        """
        self._patch_date(monkeypatch)
        station = _fake_station()
        df = _hydrograph_df(
            station_label=station.value,
            date_val="2030-06-15",
            year_col="2030",
            discharge_val=123.4,
        )
        data = {"hydrograph_day_all": df}
        result = widgets.get_predictors_warning(station, data)
        assert result is None, (
            "Expected no warning when today's discharge is present"
        )

    # ------------------------------------------------------------------
    # Current-year value is NaN → alert returned
    # ------------------------------------------------------------------
    def test_current_year_value_nan_returns_alert(self, monkeypatch):
        """NaN discharge for today in the correct year column → alert pane."""
        self._patch_date(monkeypatch)
        station = _fake_station()
        df = _hydrograph_df(
            station_label=station.value,
            date_val="2030-06-15",
            year_col="2030",
            discharge_val=np.nan,
        )
        data = {"hydrograph_day_all": df}
        result = widgets.get_predictors_warning(station, data)
        assert result is not None, (
            "Expected an alert pane when today's discharge is NaN"
        )

    # ------------------------------------------------------------------
    # No row for today → alert returned
    # ------------------------------------------------------------------
    def test_no_row_for_today_returns_alert(self, monkeypatch):
        """No matching row for today's date → alert pane."""
        self._patch_date(monkeypatch)
        station = _fake_station()
        df = _hydrograph_df(
            station_label=station.value,
            date_val="2030-06-14",  # yesterday, not today
            year_col="2030",
            discharge_val=99.0,
        )
        data = {"hydrograph_day_all": df}
        result = widgets.get_predictors_warning(station, data)
        assert result is not None, (
            "Expected an alert pane when there is no row for today"
        )

    # ------------------------------------------------------------------
    # year_col entirely absent from DataFrame → alert, no KeyError
    # ------------------------------------------------------------------
    def test_year_col_absent_returns_alert_not_keyerror(self, monkeypatch):
        """Missing year column (e.g. data not yet updated) → alert, not KeyError."""
        self._patch_date(monkeypatch)
        station = _fake_station()
        # Build a row for today but with a *different* year column
        df = _hydrograph_df(
            station_label=station.value,
            date_val="2030-06-15",
            year_col="2029",  # '2030' column is absent
            discharge_val=50.0,
        )
        data = {"hydrograph_day_all": df}
        result = widgets.get_predictors_warning(station, data)
        assert result is not None, (
            "Expected an alert pane when the year column is absent"
        )


# ---------------------------------------------------------------------------
# TestGetForecastWarning — missing-model detection across all dates
# ---------------------------------------------------------------------------


def _make_forecasts_all(rows):
    """Build a forecasts_all DataFrame from a list of dicts."""
    return pd.DataFrame(rows, columns=["station_labels", "date", "model_short",
                                       "forecasted_discharge"])


class TestGetForecastWarning:
    """get_forecast_warning warns when expected models are absent for the date."""

    TARGET_DATE = pd.Timestamp("2026-05-31")
    EARLIER_DATE = pd.Timestamp("2026-05-26")
    STATION_LABEL = "15013 - Test"

    def _station(self):
        return types.SimpleNamespace(value=self.STATION_LABEL)

    # ------------------------------------------------------------------
    # Regression #1 (MUST FAIL before fix):
    # LR present on target date; EM+TFT only on an earlier date → warns
    # ------------------------------------------------------------------
    def test_regression_absent_models_trigger_warning(self):
        """REGRESSION: models absent on the target date (rows only on earlier dates)
        must be flagged as missing.

        Old code returns None here because there are no NaN rows on target date.
        New code computes expected={LR,EM,TFT} from all station rows, finds only
        LR present on target date, and returns an alert listing EM and TFT.
        """
        df = _make_forecasts_all([
            # LR present on target date with a valid value
            {
                "station_labels": self.STATION_LABEL,
                "date": self.TARGET_DATE,
                "model_short": "LR",
                "forecasted_discharge": 42.0,
            },
            # EM and TFT only on an earlier date → they are part of expected set
            {
                "station_labels": self.STATION_LABEL,
                "date": self.EARLIER_DATE,
                "model_short": "EM",
                "forecasted_discharge": 55.0,
            },
            {
                "station_labels": self.STATION_LABEL,
                "date": self.EARLIER_DATE,
                "model_short": "TFT",
                "forecasted_discharge": 60.0,
            },
        ])
        station = self._station()
        result = widgets.get_forecast_warning(station, {"forecasts_all": df},
                                              self.TARGET_DATE)
        assert result is not None, (
            "Expected a warning when models (EM, TFT) are absent on the target date"
        )
        alert_text = result.object
        assert "EM" in alert_text, f"Alert should mention EM; got: {alert_text}"
        assert "TFT" in alert_text, f"Alert should mention TFT; got: {alert_text}"

    # ------------------------------------------------------------------
    # Present-but-NaN: row exists on target date but discharge is NaN → flagged
    # ------------------------------------------------------------------
    def test_present_but_nan_is_flagged(self):
        """A model row with NaN forecasted_discharge on target date is missing."""
        df = _make_forecasts_all([
            {
                "station_labels": self.STATION_LABEL,
                "date": self.TARGET_DATE,
                "model_short": "LR",
                "forecasted_discharge": 42.0,
            },
            {
                "station_labels": self.STATION_LABEL,
                "date": self.TARGET_DATE,
                "model_short": "EM",
                "forecasted_discharge": np.nan,
            },
        ])
        station = self._station()
        result = widgets.get_forecast_warning(station, {"forecasts_all": df},
                                              self.TARGET_DATE)
        assert result is not None, (
            "Expected a warning when a model has NaN forecasted_discharge"
        )
        assert "EM" in result.object

    # ------------------------------------------------------------------
    # All expected models present with valid values → no warning (None)
    # ------------------------------------------------------------------
    def test_all_models_present_returns_none(self):
        """No warning when every expected model has a value on the target date."""
        df = _make_forecasts_all([
            {
                "station_labels": self.STATION_LABEL,
                "date": self.TARGET_DATE,
                "model_short": "LR",
                "forecasted_discharge": 42.0,
            },
            {
                "station_labels": self.STATION_LABEL,
                "date": self.TARGET_DATE,
                "model_short": "EM",
                "forecasted_discharge": 55.0,
            },
        ])
        station = self._station()
        result = widgets.get_forecast_warning(station, {"forecasts_all": df},
                                              self.TARGET_DATE)
        assert result is None, (
            "Expected no warning when all models have values on the target date"
        )

    # ------------------------------------------------------------------
    # LR-only station: expected == {LR}, LR present → no false positive
    # ------------------------------------------------------------------
    def test_lr_only_station_no_false_positive(self):
        """A station with only LR in its history and LR present → no warning."""
        df = _make_forecasts_all([
            {
                "station_labels": self.STATION_LABEL,
                "date": self.TARGET_DATE,
                "model_short": "LR",
                "forecasted_discharge": 30.0,
            },
        ])
        station = self._station()
        result = widgets.get_forecast_warning(station, {"forecasts_all": df},
                                              self.TARGET_DATE)
        assert result is None, (
            "LR-only station with LR present must not trigger a warning"
        )

    # ------------------------------------------------------------------
    # Station has no rows at all → alert
    # ------------------------------------------------------------------
    def test_station_not_in_data_returns_alert(self):
        """Station label not present in forecasts_all → alert, no exception."""
        df = _make_forecasts_all([
            {
                "station_labels": "99999 - Other",
                "date": self.TARGET_DATE,
                "model_short": "LR",
                "forecasted_discharge": 10.0,
            },
        ])
        station = self._station()  # "15013 - Test" is absent
        result = widgets.get_forecast_warning(station, {"forecasts_all": df},
                                              self.TARGET_DATE)
        assert result is not None, (
            "Expected an alert when the station has no rows in forecasts_all"
        )

    # ------------------------------------------------------------------
    # forecasts_all is empty → alert, no exception
    # ------------------------------------------------------------------
    def test_empty_forecasts_all_returns_alert(self):
        """Empty forecasts_all DataFrame → alert pane, no KeyError."""
        df = _make_forecasts_all([])
        station = self._station()
        result = widgets.get_forecast_warning(station, {"forecasts_all": df},
                                              self.TARGET_DATE)
        assert result is not None, (
            "Expected an alert pane when forecasts_all is empty"
        )

    # ------------------------------------------------------------------
    # forecasts_all missing station_labels column → alert, no exception
    # ------------------------------------------------------------------
    def test_missing_station_labels_column_returns_alert(self):
        """forecasts_all without 'station_labels' column → alert, no exception."""
        df = pd.DataFrame({"date": [self.TARGET_DATE], "model_short": ["LR"],
                           "forecasted_discharge": [1.0]})
        station = self._station()
        result = widgets.get_forecast_warning(station, {"forecasts_all": df},
                                              self.TARGET_DATE)
        assert result is not None, (
            "Expected an alert when station_labels column is absent"
        )

    # ------------------------------------------------------------------
    # forecasts_all is None → alert, no exception
    # ------------------------------------------------------------------
    def test_none_forecasts_all_returns_alert(self):
        """forecasts_all key is None → alert pane, no AttributeError."""
        station = self._station()
        result = widgets.get_forecast_warning(station, {"forecasts_all": None},
                                              self.TARGET_DATE)
        assert result is not None, (
            "Expected an alert when forecasts_all is None"
        )

    # ------------------------------------------------------------------
    # All models missing on target date → generic message (no model list)
    # ------------------------------------------------------------------
    def test_all_models_missing_uses_generic_message(self):
        """When NO model has a forecast for the target date, the alert must use
        the generic 'No forecast data available for {station} on {date}' message
        and must NOT enumerate the individual model names.

        The station has rows for LR, EM, TFT on an earlier date (so they are
        part of expected_models) but has NO rows at all on TARGET_DATE.
        present_models will be empty, so old code would list all three models;
        new code must fall back to the generic message.
        """
        df = _make_forecasts_all([
            {
                "station_labels": self.STATION_LABEL,
                "date": self.EARLIER_DATE,
                "model_short": "LR",
                "forecasted_discharge": 42.0,
            },
            {
                "station_labels": self.STATION_LABEL,
                "date": self.EARLIER_DATE,
                "model_short": "EM",
                "forecasted_discharge": 55.0,
            },
            {
                "station_labels": self.STATION_LABEL,
                "date": self.EARLIER_DATE,
                "model_short": "TFT",
                "forecasted_discharge": 60.0,
            },
        ])
        station = self._station()
        result = widgets.get_forecast_warning(station, {"forecasts_all": df},
                                              self.TARGET_DATE)
        assert result is not None, (
            "Expected an alert when no model has a forecast for the target date"
        )
        alert_text = result.object
        # Generic message must be present
        assert self.STATION_LABEL in alert_text, (
            f"Alert should mention the station label; got: {alert_text}"
        )
        # Must NOT list individual model names
        assert "LR" not in alert_text, (
            f"Alert must not list model LR when all models are absent; got: {alert_text}"
        )
        assert "EM" not in alert_text, (
            f"Alert must not list model EM when all models are absent; got: {alert_text}"
        )
        assert "TFT" not in alert_text, (
            f"Alert must not list model TFT when all models are absent; got: {alert_text}"
        )
        assert "models" not in alert_text, (
            f"Alert must not contain 'models' when all models are absent; got: {alert_text}"
        )


# ---------------------------------------------------------------------------
# TestGetPeriodWarning — deterministic via explicit today= argument
# ---------------------------------------------------------------------------


class TestGetPeriodWarning:
    """get_period_warning warns when displayed forecast target period != current period."""

    # ------------------------------------------------------------------
    # pentad: forecast_period=31, today=2026-06-08 → current pentad is 32
    # ------------------------------------------------------------------
    def test_pentad_outdated_returns_alert(self):
        """Pentad-31 forecast shown on a day that is in pentad-32 → non-None alert."""
        today = datetime.date(2026, 6, 8)
        # Verify via the real tl helper so the test stays correct if the
        # formula ever changes.
        expected_current = int(widgets.tl.get_pentad_in_year(today))
        assert expected_current == 32, (
            f"Test pre-condition: expected pentad 32 for 2026-06-08, got {expected_current}"
        )
        result = widgets.get_period_warning(
            horizon="pentad",
            forecast_period=31,
            forecast_year=2026,
            today=today,
        )
        assert result is not None, (
            "Expected an alert when displayed pentad (31) != current pentad (32)"
        )
        alert_text = result.object
        assert "31" in alert_text, f"Alert should mention forecast period 31; got: {alert_text}"
        assert "32" in alert_text, f"Alert should mention current period 32; got: {alert_text}"

    # ------------------------------------------------------------------
    # decade: forecast_period=16, today=2026-06-08 → current decad is 16 → None
    # ------------------------------------------------------------------
    def test_decade_current_period_returns_none(self):
        """Decad-16 forecast shown on a day that is in decad-16 → None."""
        today = datetime.date(2026, 6, 8)
        expected_current = int(widgets.tl.get_decad_in_year(today))
        assert expected_current == 16, (
            f"Test pre-condition: expected decad 16 for 2026-06-08, got {expected_current}"
        )
        result = widgets.get_period_warning(
            horizon="decade",
            forecast_period=16,
            forecast_year=2026,
            today=today,
        )
        assert result is None, (
            "Expected no alert when displayed decad (16) == current decad (16)"
        )

    # ------------------------------------------------------------------
    # month: forecast_period=6, today=2026-06-08 → current month is 6 → None
    # ------------------------------------------------------------------
    def test_month_current_period_returns_none(self):
        """Month-6 forecast shown on a June day → None."""
        today = datetime.date(2026, 6, 8)
        result = widgets.get_period_warning(
            horizon="month",
            forecast_period=6,
            forecast_year=2026,
            today=today,
        )
        assert result is None, (
            "Expected no alert when displayed month (6) == current month (6)"
        )

    # ------------------------------------------------------------------
    # month: forecast_period=6, today=2026-07-01 → current month is 7 → alert
    # ------------------------------------------------------------------
    def test_month_outdated_returns_alert(self):
        """Month-6 forecast shown in July → non-None alert."""
        today = datetime.date(2026, 7, 1)
        result = widgets.get_period_warning(
            horizon="month",
            forecast_period=6,
            forecast_year=2026,
            today=today,
        )
        assert result is not None, (
            "Expected an alert when displayed month (6) != current month (7)"
        )

    # ------------------------------------------------------------------
    # season: forecast_period=1, forecast_year=2025, today=2026-06-08 → alert
    # ------------------------------------------------------------------
    def test_season_outdated_year_returns_alert(self):
        """Season forecast for 2025 shown in 2026 → non-None alert (year differs)."""
        today = datetime.date(2026, 6, 8)
        result = widgets.get_period_warning(
            horizon="season",
            forecast_period=1,
            forecast_year=2025,
            today=today,
        )
        assert result is not None, (
            "Expected an alert when displayed season year (2025) != current year (2026)"
        )

    # ------------------------------------------------------------------
    # season: forecast_period=1, forecast_year=2026, today=2026-06-08 → None
    # ------------------------------------------------------------------
    def test_season_current_year_returns_none(self):
        """Season forecast for 2026 shown in 2026 → None (same year, period always 1)."""
        today = datetime.date(2026, 6, 8)
        result = widgets.get_period_warning(
            horizon="season",
            forecast_period=1,
            forecast_year=2026,
            today=today,
        )
        assert result is None, (
            "Expected no alert when displayed season year (2026) == current year (2026)"
        )

    # ------------------------------------------------------------------
    # forecast_period is None → None (no warning, guard clause)
    # ------------------------------------------------------------------
    def test_none_forecast_period_returns_none(self):
        """forecast_period=None → None without error."""
        result = widgets.get_period_warning(
            horizon="pentad",
            forecast_period=None,
            forecast_year=2026,
            today=datetime.date(2026, 6, 8),
        )
        assert result is None, "Expected None when forecast_period is None"

    # ------------------------------------------------------------------
    # forecast_year is None → None (no warning, guard clause)
    # ------------------------------------------------------------------
    def test_none_forecast_year_returns_none(self):
        """forecast_year=None → None without error."""
        result = widgets.get_period_warning(
            horizon="pentad",
            forecast_period=31,
            forecast_year=None,
            today=datetime.date(2026, 6, 8),
        )
        assert result is None, "Expected None when forecast_year is None"

    # ------------------------------------------------------------------
    # Unknown horizon → None
    # ------------------------------------------------------------------
    def test_unknown_horizon_returns_none(self):
        """An unrecognised horizon string → None without error."""
        result = widgets.get_period_warning(
            horizon="biweekly",
            forecast_period=5,
            forecast_year=2026,
            today=datetime.date(2026, 6, 8),
        )
        assert result is None, "Expected None for an unknown horizon"


# ---------------------------------------------------------------------------
# TestCreateDatePicker — empty / NaN / populated DataFrame
# ---------------------------------------------------------------------------

class TestCreateDatePicker:
    """create_date_picker falls back to today when forecast_df has no valid dates."""

    def _today(self):
        return datetime.datetime.now().date()

    def _patch_gettext(self, monkeypatch):
        monkeypatch.setattr(widgets, "_", lambda s: s)

    # ------------------------------------------------------------------
    # Empty DataFrame (zero rows) → no crash, value == today
    # ------------------------------------------------------------------
    def test_empty_dataframe_returns_picker_with_today(self, monkeypatch):
        """Empty DataFrame → DatePicker built without error; .value == today."""
        self._patch_gettext(monkeypatch)
        df = pd.DataFrame({"date": pd.Series([], dtype="datetime64[ns]")})
        picker = widgets.create_date_picker(df)
        assert isinstance(picker, widgets.pn.widgets.DatePicker), (
            "Expected a DatePicker widget"
        )
        assert picker.value == self._today(), (
            f"Expected value=today ({self._today()}), got {picker.value}"
        )

    # ------------------------------------------------------------------
    # DataFrame whose 'date' column is all NaT → no crash, value == today
    # ------------------------------------------------------------------
    def test_all_nat_date_column_returns_picker_with_today(self, monkeypatch):
        """All-NaT 'date' column → DatePicker built without error; .value == today."""
        self._patch_gettext(monkeypatch)
        df = pd.DataFrame(
            {"date": pd.to_datetime([None, None, None])}
        )
        picker = widgets.create_date_picker(df)
        assert isinstance(picker, widgets.pn.widgets.DatePicker)
        assert picker.value == self._today(), (
            f"Expected value=today ({self._today()}), got {picker.value}"
        )

    # ------------------------------------------------------------------
    # Populated DataFrame with valid dates → .value == max date (unchanged behaviour)
    # ------------------------------------------------------------------
    def test_populated_dataframe_returns_max_date(self, monkeypatch):
        """Populated DataFrame → .value equals the max date's .date()."""
        self._patch_gettext(monkeypatch)
        dates = pd.to_datetime(["2026-03-01", "2026-04-15", "2026-02-10"])
        df = pd.DataFrame({"date": dates})
        picker = widgets.create_date_picker(df)
        expected = datetime.date(2026, 4, 15)
        assert picker.value == expected, (
            f"Expected value={expected}, got {picker.value}"
        )
