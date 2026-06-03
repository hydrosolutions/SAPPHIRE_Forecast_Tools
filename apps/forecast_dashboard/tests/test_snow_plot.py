from types import SimpleNamespace

import holoviews as hv
import pandas as pd
import pytest
from src import vizualization

hv.extension("bokeh")


def _snow_frame(variable="HS", mean_values=None, overrides=None):
    dates = pd.date_range("2026-01-01", periods=5, freq="D")
    values = [15.0, 16.0, 17.0, 18.0, 19.0]
    data = {
        "code": ["19999"] * len(dates),
        "date": dates,
        variable: values,
        "norm": [12.0, 13.0, 14.0, 15.0, 16.0],
        "mean": mean_values if mean_values is not None else [13.0, 14.0, 15.0, 16.0, 17.0],
        "min": [5.0, 6.0, 7.0, 8.0, 9.0],
        "max": [30.0, 31.0, 32.0, 33.0, 34.0],
        "5%": [7.0, 8.0, 9.0, 10.0, 11.0],
        "25%": [10.0, 11.0, 12.0, 13.0, 14.0],
        "50%": [14.0, 15.0, 16.0, 17.0, 18.0],
        "75%": [20.0, 21.0, 22.0, 23.0, 24.0],
        "95%": [26.0, 27.0, 28.0, 29.0, 30.0],
        "last_year": [11.0, 12.0, 13.0, 14.0, 15.0],
        "current_year": values,
    }
    if overrides:
        data.update(overrides)
    return pd.DataFrame(data)


def _widget_manager(horizon="pentad"):
    return SimpleNamespace(horizon_selector=SimpleNamespace(value=horizon))


def _predictor_dates(*args, **kwargs):
    return pd.DataFrame({
        "predictor_start_date": [pd.Timestamp("2026-01-01")],
        "predictor_end_date": [pd.Timestamp("2026-01-03")],
        "forecast_start_date": [pd.Timestamp("2026-01-04")],
        "forecast_end_date": [pd.Timestamp("2026-01-05")],
    })


@pytest.fixture(autouse=True)
def _patch_plot_dependencies(monkeypatch):
    monkeypatch.setattr(vizualization, "_", lambda text: text)
    monkeypatch.setattr(vizualization.processing, "add_predictor_dates", _predictor_dates)
    vizualization.PlotCache.clear()


def _plot(frame, variable="HS", date_picker="2026-01-04"):
    return vizualization.plot_daily_snow_data(
        lambda text: text,
        _widget_manager(),
        {variable: frame},
        variable,
        "19999 - Test Basin",
        pd.Timestamp(date_picker),
        pd.DataFrame(),
    )


def _elements(plot, element_type=None):
    elements = list(plot) if isinstance(plot, hv.Overlay) else [plot]
    if element_type is None:
        return elements
    return [element for element in elements if isinstance(element, element_type)]


def _labels(plot, element_type=None):
    return [element.label for element in _elements(plot, element_type)]


def _curve_by_label(plot, label):
    for curve in _elements(plot, hv.Curve):
        if curve.label == label:
            return curve
    raise AssertionError(f"Curve labelled {label!r} not found")


def test_snow_plot_contains_min_max_area():
    plot = _plot(_snow_frame())

    assert "Full range legend entry" in _labels(plot, hv.Area)


def test_snow_plot_contains_percentile_bands():
    plot = _plot(_snow_frame())

    labels = _labels(plot, hv.Area)
    assert "90-percentile range legend entry" in labels
    assert "50-percentile range legend entry" in labels


def test_snow_plot_contains_mean_or_norm_line():
    mean_plot = _plot(_snow_frame())
    norm_plot = _plot(_snow_frame(mean_values=[None, None, None, None, None]))

    assert "Mean legend entry" in _labels(mean_plot, hv.Curve)
    assert "Norm" in _labels(norm_plot, hv.Curve)
    assert "Mean legend entry" not in _labels(norm_plot, hv.Curve)


def test_snow_plot_contains_last_and_current_year_lines():
    plot = _plot(_snow_frame())

    labels = _labels(plot, hv.Curve)
    assert "Last year legend entry" in labels
    assert any(label.startswith("Current year") for label in labels)


def test_snow_plot_preserves_forecast_curve():
    plot = _plot(_snow_frame(), date_picker="2026-01-04")

    assert "Forecast" in _labels(plot, hv.Curve)


def test_snow_plot_hs_uses_already_converted_cm_contract():
    plot = _plot(_snow_frame(overrides={
        "HS": [15.0, 15.0, 15.0, 15.0, 15.0],
        "current_year": [15.0, 15.0, 15.0, 15.0, 15.0],
    }))

    current_year_curve = next(
        curve for curve in _elements(plot, hv.Curve)
        if curve.label.startswith("Current year")
    )
    assert list(current_year_curve.dimension_values("current_year")) == [15.0] * 5


def test_snow_plot_y_axis_includes_all_visible_layers():
    plot = _plot(_snow_frame(overrides={
        "min": [2.0, 6.0, 7.0, 8.0, 9.0],
        "max": [30.0, 31.0, 32.0, 33.0, 60.0],
        "5%": [4.0, 8.0, 9.0, 10.0, 11.0],
        "25%": [10.0, 11.0, 12.0, 13.0, 14.0],
        "75%": [20.0, 21.0, 22.0, 23.0, 24.0],
        "95%": [26.0, 27.0, 28.0, 29.0, 55.0],
        "mean": [13.0, 14.0, 15.0, 16.0, 17.0],
        "last_year": [11.0, 12.0, 13.0, 14.0, 15.0],
        "current_year": [15.0, 16.0, 17.0, 18.0, 19.0],
    }))

    assert plot.opts.get("plot").kwargs["ylim"] == pytest.approx((1.8, 66.0))


def _plot_for_display_window(
    frame,
    variable="HS",
    date_picker="2025-12-15",
    display_start_month=9,
    display_start_day=1,
):
    return vizualization.plot_daily_snow_data(
        lambda text: text,
        _widget_manager(),
        {variable: frame},
        variable,
        "19999 - Test Basin",
        pd.Timestamp(date_picker),
        pd.DataFrame(),
        snow_display_start_month=display_start_month,
        snow_display_start_day=display_start_day,
    )


def _single_snow_frame(ref_date):
    return pd.DataFrame({
        "code": ["19999"],
        "date": [pd.Timestamp(ref_date)],
        "HS": [15.0],
        "norm": [12.0],
        "mean": [13.0],
        "min": [5.0],
        "max": [30.0],
        "5%": [7.0],
        "25%": [10.0],
        "50%": [14.0],
        "75%": [20.0],
        "95%": [26.0],
        "last_year": [11.0],
        "current_year": [15.0],
    })


def test_snow_plot_labels_use_calendar_year_wording_when_start_is_jan_1():
    plot = _plot_for_display_window(
        _snow_frame(),
        date_picker="2026-01-04",
        display_start_month=1,
        display_start_day=1,
    )

    labels = _labels(plot, hv.Curve)
    assert "Last year legend entry" in labels
    assert "Current year, 3 day mean: 16.0 cm" in labels


def test_snow_plot_labels_use_season_wording_when_start_is_sept_1():
    plot = _plot_for_display_window(
        _snow_frame(overrides={
            "date": pd.date_range("2025-12-11", periods=5, freq="D"),
        }),
        date_picker="2025-12-15",
        display_start_month=9,
        display_start_day=1,
    )

    labels = _labels(plot, hv.Curve)
    assert any("Current season 2025/26" in label for label in labels)
    assert any("Previous season 2025/26" in label for label in labels)

    no_current_value_plot = _plot_for_display_window(
        _snow_frame(overrides={
            "date": pd.date_range("2025-12-11", periods=5, freq="D"),
        }).drop(columns=["current_year"]),
        date_picker="2025-12-15",
        display_start_month=9,
        display_start_day=1,
    )
    no_current_value_labels = _labels(no_current_value_plot, hv.Curve)
    assert any("Current season 2025/26" in label for label in no_current_value_labels)
    assert any("Previous season 2025/26" in label for label in no_current_value_labels)


def test_snow_plot_season_year_label_transitions_at_start_day():
    cases = [
        ("2025-08-31", "Current season 2024/25", "Previous season 2024/25"),
        ("2025-09-01", "Current season 2025/26", "Previous season 2024/25"),
    ]

    for ref_date, current_label, previous_label in cases:
        plot = _plot_for_display_window(
            _single_snow_frame(ref_date),
            date_picker=ref_date,
            display_start_month=9,
            display_start_day=1,
        )

        labels = _labels(plot, hv.Curve)
        assert any(current_label in label for label in labels)
        assert any(previous_label in label for label in labels)
