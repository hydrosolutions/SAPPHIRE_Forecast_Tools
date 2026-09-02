from datetime import date, datetime
from types import SimpleNamespace

import holoviews as hv
import pandas as pd
import pytest
from bokeh.models import ColumnDataSource
from src import vizualization
from src.snow_window import snow_display_window

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
    assert "Current year" in labels


def test_snow_plot_current_season_label_has_no_mean_annotation():
    # Calendar-year (non-hydrological) display: label is plain "Current year"
    # with no ", 3 day mean: <value> <unit>" suffix.
    calendar_plot = _plot_for_display_window(
        _snow_frame(),
        date_picker="2026-01-04",
        display_start_month=1,
        display_start_day=1,
    )
    calendar_labels = _labels(calendar_plot, hv.Curve)
    assert "Current year" in calendar_labels
    assert all("3 day mean" not in label for label in calendar_labels)
    assert all("10 day mean" not in label for label in calendar_labels)

    # Hydrological-year (season) display: label is the bare season string.
    season_plot = _plot_for_display_window(
        _snow_frame(overrides={
            "date": pd.date_range("2025-12-11", periods=5, freq="D"),
        }),
        date_picker="2025-12-15",
        display_start_month=9,
        display_start_day=1,
    )
    season_labels = _labels(season_plot, hv.Curve)
    assert "Current season 2025/26" in season_labels
    assert all("3 day mean" not in label for label in season_labels)
    assert all("10 day mean" not in label for label in season_labels)


def test_snow_plot_labels_use_season_wording_when_start_is_sept_1():
    plot = _plot_for_display_window(
        _snow_frame(overrides={
            "date": pd.date_range("2025-12-11", periods=5, freq="D"),
        }),
        date_picker="2025-12-15",
        display_start_month=9,
        display_start_day=1,
    )

    # FD-024: previous_season is one hydrological YEAR behind current_season,
    # not one day. current_ref falls on 2025-12-15 (season 2025/26 for a
    # 09-01 start), so previous season is 2024/25.
    labels = _labels(plot, hv.Curve)
    assert any("Current season 2025/26" in label for label in labels)
    assert any("Previous season 2024/25" in label for label in labels)

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
    assert any("Previous season 2024/25" in label for label in no_current_value_labels)


def test_snow_plot_season_year_label_transitions_at_start_day():
    # FD-024: previous_season must be exactly one hydrological year behind
    # current_season on EVERY reference date, not only on the exact season
    # start day (the old day-subtraction bug only crossed the boundary on
    # that one day). Re-expressed from the old boundary-only assertions
    # (which asserted "Previous season 2024/25" for both 2025-08-31 and
    # 2025-09-01 — the buggy same-year pairing off the boundary) to check
    # the invariant across both dates.
    cases = [
        ("2025-08-31", "Current season 2024/25", "Previous season 2023/24"),
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


def test_snow_plot_season_label_leap_day_reference_does_not_raise():
    # FD-024: a naive fix that derives the previous season via
    # current_ref.replace(year=current_ref.year - 1) raises ValueError for
    # a Feb-29 reference whose previous calendar year is not a leap year —
    # 2028 is a leap year, 2027 is not, so this is exactly that case. The
    # chosen fix derives previous_season from the season's start year (an
    # int), never from date arithmetic on current_ref, so it must produce
    # sane labels here and must not raise.
    reference = date(2028, 2, 29)
    current_season = vizualization._snow_season_label(reference, 1, 1)
    start_year = vizualization._season_start_year(reference, 1, 1)
    previous_season = vizualization._season_label_from_start_year(start_year - 1)

    assert current_season == "2028/29"
    assert previous_season == "2027/28"


# ── snow_ref_date / date_picker two-reference-date bug regression ────────


def _rendered_source_lengths(plot):
    """Return the length of every column in every ColumnDataSource of the
    Bokeh figure rendered from `plot`."""
    bokeh_fig = hv.render(plot)
    lengths = []
    for source in bokeh_fig.select(ColumnDataSource):
        for column in source.data.values():
            lengths.append(len(column))
    return lengths


def _frame_spanning_window_boundary():
    """Build an HS frame with rows both inside and outside the Sep1-Aug31
    2026/27 hydrological-year window (the window ``snow_display_window(9, 1,
    date(2026, 9, 2))`` computes), each with a distinct ``current_year``
    value. A window that is too wide (e.g. off by a year, or missing its
    upper bound) or too narrow is directly detectable by comparing the
    plotted values against the in-window subset, rather than merely
    checking "something rendered".
    """
    dates = pd.to_datetime([
        "2026-08-28", "2026-08-29", "2026-08-30", "2026-08-31",  # OUT: before window
        "2026-09-01", "2026-09-02", "2026-09-03",                 # IN:  window
        "2027-08-31",                                              # IN:  last day of window
        "2027-09-01",                                              # OUT: after window
    ])
    n = len(dates)
    values = [100.0 + i for i in range(n)]
    return pd.DataFrame({
        "code": ["19999"] * n,
        "date": dates,
        "HS": values,
        "norm": [12.0] * n,
        "mean": [13.0] * n,
        "min": [5.0] * n,
        "max": [30.0] * n,
        "5%": [7.0] * n,
        "25%": [10.0] * n,
        "50%": [14.0] * n,
        "75%": [20.0] * n,
        "95%": [26.0] * n,
        "last_year": [11.0] * n,
        "current_year": values,
    })


def test_snow_plot_uses_snow_ref_date_window_when_provided():
    """Regression test: the data-fetch window (keyed on snow_ref_date) and
    the plot window must be the SAME window, or the plot renders empty even
    though data was fetched for it.

    date_picker (2026-07-05) falls in the hydrological year 2025/26, but the
    data (and the real fetch, once fixed) live in 2026/27 — the season that
    starts on the configured snow_ref_date (2026-09-02). Passing snow_ref_date
    explicitly must make the plot use the 2026/27 window and actually show
    the data, instead of the empty 2025/26 window the bug produced.

    Strengthened beyond "something rendered": the frame spans the window
    boundary on both sides, so the test asserts the plotted "current year"
    curve carries exactly the in-window dates/values — a widened window (off
    by a year, or missing its upper bound) would pull in the OUT-tagged rows
    and fail the equality check even though it would still pass a bare
    length>0 assertion.
    """
    frame = _frame_spanning_window_boundary()

    plot = vizualization.plot_daily_snow_data(
        lambda text: text,
        _widget_manager(),
        {"HS": frame},
        "HS",
        "19999 - Test Basin",
        pd.Timestamp("2026-09-02"),
        pd.DataFrame(),
        snow_display_start_month=9,
        snow_display_start_day=1,
        snow_ref_date=date(2026, 9, 2),
    )

    display_begin, display_end = snow_display_window(9, 1, date(2026, 9, 2))
    expected = frame[
        (frame["date"] >= display_begin) & (frame["date"] <= display_end)
    ].sort_values("date")
    assert len(expected) == 4  # sanity: the window boundary rows are as designed above

    current_year_curve = next(
        curve for curve in _elements(plot, hv.Curve)
        if curve.label.startswith("Current season") or curve.label.startswith("Current year")
    )
    plotted_dates = pd.to_datetime(list(current_year_curve.dimension_values("date")))
    plotted_values = list(current_year_curve.dimension_values("current_year"))

    assert plotted_values == expected["current_year"].tolist()
    assert list(plotted_dates) == list(expected["date"])
    assert all(display_begin <= d <= display_end for d in plotted_dates)


def test_snow_plot_falls_back_to_date_picker_window_when_ref_date_is_none():
    """Without snow_ref_date, plot_daily_snow_data must keep its
    pre-existing behaviour of deriving the display window from date_picker
    alone (the fix must be purely additive).

    With the same frame and date_picker as the previous test but no
    snow_ref_date, the window is computed from date_picker's own
    hydrological year (2025/26), which does not overlap the frame's
    2026/27-season dates — reproducing the original bug's empty-plot
    symptom and confirming the fallback branch's window formula is
    unchanged.
    """
    frame = _snow_frame(overrides={
        "date": pd.date_range("2026-09-01", periods=5, freq="D"),
    })

    plot = vizualization.plot_daily_snow_data(
        lambda text: text,
        _widget_manager(),
        {"HS": frame},
        "HS",
        "19999 - Test Basin",
        pd.Timestamp("2026-07-05"),
        pd.DataFrame(),
        snow_display_start_month=9,
        snow_display_start_day=1,
        snow_ref_date=None,
    )

    lengths = _rendered_source_lengths(plot)
    assert lengths  # sources exist
    assert all(length == 0 for length in lengths)


# ── adversarial-review follow-ups: items 2, 3, 4, 5 ───────────────────────


def test_snow_plot_accepts_date_datetime_and_timestamp_ref_date():
    """Item 2: snow_ref_date may arrive as a bare date, a datetime, or a
    pd.Timestamp — nearly every date in this codebase IS a pd.Timestamp
    (forecasts_all['date'].max(), db.get_bulletin_metadata, ...), and
    plot_daily_snow_data is untyped so nothing upstream rejects one. All
    three must normalise to the SAME display window and none may raise —
    snow_display_window compares ref_date against a bare datetime.date
    internally and raises TypeError for an un-normalised Timestamp
    ("Cannot compare Timestamp with datetime.date") or datetime ("can't
    compare datetime.datetime to datetime.date").
    """
    frame = _frame_spanning_window_boundary()
    ref_variants = [
        date(2026, 9, 2),
        datetime(2026, 9, 2),
        pd.Timestamp("2026-09-02"),
    ]

    results = []
    for ref in ref_variants:
        plot = vizualization.plot_daily_snow_data(
            lambda text: text,
            _widget_manager(),
            {"HS": frame},
            "HS",
            "19999 - Test Basin",
            pd.Timestamp("2026-09-02"),
            pd.DataFrame(),
            snow_display_start_month=9,
            snow_display_start_day=1,
            snow_ref_date=ref,
        )
        curve = next(
            curve for curve in _elements(plot, hv.Curve)
            if curve.label.startswith("Current season") or curve.label.startswith("Current year")
        )
        dates = list(pd.to_datetime(list(curve.dimension_values("date"))))
        values = list(curve.dimension_values("current_year"))
        results.append((dates, values))

    assert results[0] == results[1] == results[2]
    # Sanity: the window was actually applied (not empty, not the whole frame).
    assert len(results[0][0]) == 4


def test_snow_plot_forecast_curve_splits_on_snow_ref_date_not_date_picker():
    """Item 3: whenever date_picker falls before the plotted display window
    — precisely the condition that produced the original blank-card bug —
    splitting the "Forecast" curve on date_picker selects the ENTIRE window
    and draws/legends the whole raw observed series as "Forecast". The
    split must use snow_ref_date instead (SnowMapper's forward projection
    is what actually lies beyond it) whenever snow_ref_date is supplied.
    """
    dates = pd.date_range("2026-09-01", periods=20, freq="D")
    n = len(dates)
    values = [100.0 + i for i in range(n)]
    frame = pd.DataFrame({
        "code": ["19999"] * n,
        "date": dates,
        "HS": values,
        "norm": [12.0] * n,
        "mean": [13.0] * n,
        "min": [5.0] * n,
        "max": [130.0] * n,
        "5%": [7.0] * n,
        "25%": [10.0] * n,
        "50%": [14.0] * n,
        "75%": [20.0] * n,
        "95%": [26.0] * n,
        "last_year": [11.0] * n,
        "current_year": values,
    })

    plot = vizualization.plot_daily_snow_data(
        lambda text: text,
        _widget_manager(),
        {"HS": frame},
        "HS",
        "19999 - Test Basin",
        pd.Timestamp("2026-07-05"),  # earlier than display_begin (2026-09-01)
        pd.DataFrame(),
        snow_display_start_month=9,
        snow_display_start_day=1,
        snow_ref_date=date(2026, 9, 10),
    )

    forecast_curve = _curve_by_label(plot, "Forecast")
    plotted_dates = pd.to_datetime(list(forecast_curve.dimension_values("date")))
    plotted_values = list(forecast_curve.dimension_values("HS"))

    expected_dates = pd.date_range("2026-09-10", "2026-09-20", freq="D")
    assert list(plotted_dates) == list(expected_dates)
    assert plotted_values == [100.0 + i for i in range(9, 20)]
    # Not the whole 20-row window — a date_picker split would include all of it.
    assert len(plotted_dates) == 11


def test_snow_plot_season_and_title_follow_plotted_window_not_date_picker():
    """Item 4: with an all-null current_year column, the season reference
    must fall back to the plotted window's reference (snow_ref_date), not
    date_picker. Reproduces the review's exact scenario: a fresh
    hydrological year whose current_year column is all-NaN,
    snow_ref_date=2026-09-02, date_picker=2026-07-05 (which is in season
    2025/26), start=09-01 — the plotted window is season 2026/27, and the
    legend/title must say so, not "2025/26", while the axis starts
    2026-09-01.

    Because current_year is entirely NaN, plot_runoff_line's own NaN guard
    returns an unlabeled hv.Curve([]) for the "Current season" line — so the
    visible curve label to assert on here is "Previous season", carried by
    the (non-NaN) last_year column.

    FD-024 note: previous_season is now correctly one hydrological year
    behind current_season (2025/26, not 2026/27 — see
    doc/plans/issues/review_gi_draft_fd_snow_season_label_off_by_day.md),
    so "Previous
    season" is still the only observable label here even though its value
    changed: current_season is 2026/27 (window_ref-derived, unchanged by
    FD-024) but its curve renders unlabeled. This label remains a valid
    detector for the window_ref regression this test targets — it is
    computed from current_ref, so reverting THIS test's regression (the
    _snow_current_season_reference window_ref fallback) changes current_ref
    to date_picker's own 2025/26 season and this label to one season
    earlier again, 2024/25.
    """
    dates = pd.date_range("2026-09-01", periods=5, freq="D")
    n = len(dates)
    frame = pd.DataFrame({
        "code": ["19999"] * n,
        "date": dates,
        "HS": [15.0] * n,
        "norm": [12.0] * n,
        "mean": [13.0] * n,
        "min": [5.0] * n,
        "max": [30.0] * n,
        "5%": [7.0] * n,
        "25%": [10.0] * n,
        "50%": [14.0] * n,
        "75%": [20.0] * n,
        "95%": [26.0] * n,
        "last_year": [11.0] * n,
        "current_year": [None] * n,  # all-null current_year column
    })

    plot = vizualization.plot_daily_snow_data(
        lambda text: text,
        _widget_manager(),
        {"HS": frame},
        "HS",
        "19999 - Test Basin",
        pd.Timestamp("2026-07-05"),
        pd.DataFrame(),
        snow_display_start_month=9,
        snow_display_start_day=1,
        snow_ref_date=date(2026, 9, 2),
    )

    labels = _labels(plot, hv.Curve)
    assert "Previous season 2025/26" in labels

    title = plot.opts.get("plot").kwargs["title"]
    assert "2026/27" in title
    assert "2025/26" not in title

    xlim = plot.opts.get("plot").kwargs["xlim"]
    assert xlim[0] == pd.Timestamp("2026-09-01")


def test_snow_plot_returns_no_data_message_when_window_has_no_rows():
    """Item 5: rows exist for the station, but none fall inside the
    computed display window (distinct from "no data at all" and "no data
    for this station" — the two pre-existing guards). Without an explicit
    guard, every glyph builder degrades to hv.Curve([]) and the figure
    still renders a full title/axes/legend with nothing plotted — the
    structural reason the original two-reference-date bug went unnoticed.
    The message must name the plotted season.
    """
    # All rows fall in the PREVIOUS hydrological year (2025/26); the window
    # for snow_ref_date=2026-09-02 is 2026/27 (Sep 2026 - Aug 2027), so none
    # of these rows land inside it.
    dates = pd.date_range("2025-10-01", periods=5, freq="D")
    n = len(dates)
    frame = pd.DataFrame({
        "code": ["19999"] * n,
        "date": dates,
        "HS": [15.0] * n,
        "norm": [12.0] * n,
        "mean": [13.0] * n,
        "min": [5.0] * n,
        "max": [30.0] * n,
        "5%": [7.0] * n,
        "25%": [10.0] * n,
        "50%": [14.0] * n,
        "75%": [20.0] * n,
        "95%": [26.0] * n,
        "last_year": [11.0] * n,
        "current_year": [15.0] * n,
    })

    plot = vizualization.plot_daily_snow_data(
        lambda text: text,
        _widget_manager(),
        {"HS": frame},
        "HS",
        "19999 - Test Basin",
        pd.Timestamp("2026-09-05"),
        pd.DataFrame(),
        snow_display_start_month=9,
        snow_display_start_day=1,
        snow_ref_date=date(2026, 9, 2),
    )

    # The early-return path returns a bare Curve, not the multi-layer
    # Overlay the normal (non-empty) path composes via `*`.
    assert isinstance(plot, hv.Curve)
    assert not isinstance(plot, hv.Overlay)

    title = plot.opts.get("plot").kwargs["title"]
    assert title == "No HS data for season 2026/27"
