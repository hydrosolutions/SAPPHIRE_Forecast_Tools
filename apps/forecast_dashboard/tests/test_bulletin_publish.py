"""Unit tests for the "Publish bulletin" feature (FD-017).

Covers:
  1. dashboard.bulletin_publish.compute_next_period_start — pure, no
     Panel import — expiry across pentad-72, decade-36, and month
     December rollovers, plus mid-range cases.
  2. dashboard.bulletin_publish.serialize_site — short-term vs
     month/season field sets, NaN -> None JSON-safety.
  3. dashboard.bulletin_publish.assemble_bulletin_snapshot — selected
     code with bulletin data is included; selected code without data is
     reported in skipped_codes and omitted from the payload; the
     (year, horizon_value) period is derived from the persisted bulletin
     records for the selected stations (via db._read_data), never from
     dm.get_bulletin_metadata/dm.forecasts_all; a horizon with no
     persisted bulletin rows for the selected codes yields an empty
     ``stations`` list and reports every selected code as skipped
     (no exception).
  4. dashboard.widget_manager.WidgetManager._on_generate_links_click —
     the "Generate links" button handler: N selected horizons -> N
     POSTs -> N links surfaced; an empty horizon produces no link plus a
     warning (other horizons still processed); any assemble/POST failure
     aborts immediately and renders no partial links (all-or-nothing).

Only synthetic station codes/values are used throughout (90001/90002/...
and small round numbers) — no real station codes or discharge values.

Panel is a genuine dependency of this venv (see pyproject.toml), so
dashboard.bulletin_manager / dashboard.widget_manager are imported
directly rather than faked via sys.modules stubs (contrast with the
sys.modules-stub bootstrap used in test_bulletin_edit_persistence.py for
environments where panel may not be installed). The network-touching
pieces (`_load_bulletin_from_api`, `_post_bulletin_share`) are always
monkeypatched so no test performs real I/O.
"""

from __future__ import annotations

import datetime as dt
import os
import sys
import types

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import dashboard.bulletin_manager as bulletin_manager  # noqa: E402
import dashboard.widget_manager as widget_manager  # noqa: E402
from dashboard import bulletin_publish as bp  # noqa: E402
from dashboard import widgets  # noqa: E402
from dashboard.widget_manager import WidgetManager  # noqa: E402
from src import db  # noqa: E402 - same module object bp's deferred import binds to

UTC = dt.UTC


# ---------------------------------------------------------------------------
# 0. New widget creators (dashboard/widgets.py)
# ---------------------------------------------------------------------------


class TestPublishWidgetCreators:
    def test_horizon_multiselect_includes_all_when_ml_enabled(self):
        widget = widgets.create_publish_horizon_multiselect(True)
        assert set(widget.options.values()) == {"pentad", "decade", "month", "season"}
        assert widget.value == []

    def test_horizon_multiselect_excludes_long_term_when_ml_disabled(self):
        widget = widgets.create_publish_horizon_multiselect(False)
        assert set(widget.options.values()) == {"pentad", "decade"}

    def test_station_multiselect_flattens_grouped_dict(self):
        station_dict = {
            "Basin A": ["90001 - River A Punkt A", "90002 - River B Punkt B"],
            "Basin B": ["90003 - River C Punkt C"],
        }
        widget = widgets.create_publish_station_multiselect(station_dict)
        assert set(widget.options) == {
            "90001 - River A Punkt A",
            "90002 - River B Punkt B",
            "90003 - River C Punkt C",
        }
        assert widget.value == []

    def test_station_multiselect_handles_empty_station_dict(self):
        widget = widgets.create_publish_station_multiselect({})
        assert widget.options == {}

    def test_generate_links_button_is_a_button(self):
        button = widgets.create_generate_links_button()
        assert button.name == "Generate links"

    def test_publish_results_pane_starts_empty(self):
        pane = widgets.create_publish_results_pane()
        assert pane.object == ""


# ---------------------------------------------------------------------------
# 1. compute_next_period_start
# ---------------------------------------------------------------------------


class TestComputeNextPeriodStart:
    @pytest.mark.parametrize(
        "horizon, forecast_horizon, forecast_year, expected",
        [
            # Pentad-72 wraps into pentad 1 of the following year.
            ("pentad", 72, 2026, dt.datetime(2027, 1, 1, tzinfo=UTC)),
            # Mid-range pentad: pentad 13 (3rd pentad of March) -> pentad 14.
            ("pentad", 13, 2026, dt.datetime(2026, 3, 6, tzinfo=UTC)),
            # Decade-36 wraps into decade 1 of the following year.
            ("decade", 36, 2026, dt.datetime(2027, 1, 1, tzinfo=UTC)),
            # Mid-range decade: decade 10 (1st decade of April) -> decade 11.
            ("decade", 10, 2026, dt.datetime(2026, 4, 11, tzinfo=UTC)),
            # December -> January rollover.
            ("month", 12, 2026, dt.datetime(2027, 1, 1, tzinfo=UTC)),
            # Mid-range month.
            ("month", 6, 2026, dt.datetime(2026, 7, 1, tzinfo=UTC)),
            # Season: single annual April-September window -> next year's season.
            ("season", 1, 2026, dt.datetime(2027, 4, 1, tzinfo=UTC)),
        ],
    )
    def test_expiry_across_boundaries(self, horizon, forecast_horizon, forecast_year, expected):
        result = bp.compute_next_period_start(
            horizon, forecast_horizon, forecast_year, dt.date(2026, 7, 1)
        )
        assert result == expected
        assert result.tzinfo == UTC

    def test_forecast_date_is_not_read_via_now(self):
        """Passing a wildly different forecast_date must not change the
        result — the period arithmetic is fully determined by
        forecast_horizon/forecast_year, never by wall-clock time."""
        r1 = bp.compute_next_period_start("pentad", 13, 2026, dt.date(2000, 1, 1))
        r2 = bp.compute_next_period_start("pentad", 13, 2026, dt.date(2099, 12, 31))
        assert r1 == r2


# ---------------------------------------------------------------------------
# 2. serialize_site
# ---------------------------------------------------------------------------


def _site_with_attrs(code="90001", **attrs):
    site = types.SimpleNamespace(
        code=code,
        station_label=f"{code} - Test River Test Punkt",
        basin_ru="Test Basin",
        river_name_ru="Test River",
        forecast_model="LR",
    )
    for key, value in attrs.items():
        setattr(site, key, value)
    return site


class TestSerializeSite:
    def test_short_term_field_set(self):
        site = _site_with_attrs(
            forecast_expected=10.5,
            forecast_lower_bound=9.0,
            forecast_upper_bound=12.0,
            forecast_delta=1.2,
            forecast_sdivsigma=0.5,
            forecast_mae=0.4,
            forecast_accuracy=88.0,
            perc_norm=101.0,
        )
        row = bp.serialize_site(site, "pentad")

        assert row["code"] == "90001"
        assert row["station_label"] == "90001 - Test River Test Punkt"
        assert row["basin"] == "Test Basin"
        assert row["river"] == "Test River"
        assert row["model"] == "LR"
        assert row["forecasted_discharge"] == 10.5
        assert row["fc_lower"] == 9.0
        assert row["fc_upper"] == 12.0
        assert row["delta"] == 1.2
        assert row["sdivsigma"] == 0.5
        assert row["mae"] == 0.4
        assert row["accuracy"] == 88.0
        assert row["perc_norm"] == 101.0
        # Month/season-only fields must not leak into the short-term set.
        assert "q_min" not in row
        assert "v_min" not in row
        assert "norm" not in row

    @pytest.mark.parametrize("horizon", ["month", "season"])
    def test_long_term_field_set(self, horizon):
        site = _site_with_attrs(
            forecast_expected=5.0,
            forecast_q_min=1.0,
            forecast_q_max=2.0,
            forecast_v_min=3.0,
            forecast_v_max=4.0,
            forecast_norm=6.0,
            perc_norm=83.3,
        )
        row = bp.serialize_site(site, horizon)

        assert row["code"] == "90001"
        assert row["forecasted_discharge"] == 5.0
        assert row["q_min"] == 1.0
        assert row["q_max"] == 2.0
        assert row["v_min"] == 3.0
        assert row["v_max"] == 4.0
        assert row["norm"] == 6.0
        assert row["perc_norm"] == 83
        # Short-term-only fields must not leak into the long-term set.
        assert "fc_lower" not in row
        assert "delta" not in row
        assert "accuracy" not in row

    def test_nan_and_missing_attrs_become_none(self):
        site = _site_with_attrs(forecast_expected=float("nan"))
        row = bp.serialize_site(site, "pentad")
        assert row["forecasted_discharge"] is None
        # forecast_lower_bound was never set on this bare site.
        assert row["fc_lower"] is None


class TestSerializeSiteRounding:
    """Non-round inputs must come out rounded exactly as the Excel
    bulletin would render them (see apps/forecast_dashboard/src/bulletins.py):
    discharge/volume fields -> 3 significant figures (round_3sf), DELTA/
    SDIVSIGMA -> round(x, 2), PERC_NORM -> round(x) (int). mae/accuracy
    have no Excel column and must stay raw.
    """

    def test_short_term_rounding(self):
        site = _site_with_attrs(
            forecast_expected=12.34567,
            forecast_lower_bound=9.876,
            forecast_upper_bound=1234.5,
            forecast_delta=1.239,
            forecast_sdivsigma=0.677,
            forecast_mae=0.4321,
            forecast_accuracy=88.0,
            perc_norm=83.6,
        )
        row = bp.serialize_site(site, "pentad")

        assert row["forecasted_discharge"] == 12.3
        assert row["fc_lower"] == 9.88
        assert row["fc_upper"] == 1230
        assert row["delta"] == 1.24
        assert row["sdivsigma"] == 0.68
        assert row["perc_norm"] == 84
        # mae/accuracy have no Excel column - they stay RAW (full precision).
        assert row["mae"] == 0.4321
        assert row["accuracy"] == 88.0

    @pytest.mark.parametrize("horizon", ["month", "season"])
    def test_long_term_rounding(self, horizon):
        site = _site_with_attrs(
            forecast_expected=5.0,
            forecast_q_min=12.34567,
            forecast_q_max=2.0,
            forecast_v_min=1234.5,
            forecast_v_max=4.0,
            forecast_norm=6.0,
            perc_norm=83.3,
        )
        row = bp.serialize_site(site, horizon)

        assert row["q_min"] == 12.3
        assert row["v_min"] == 1230
        assert row["perc_norm"] == 83

    def test_negative_discharge_becomes_none(self):
        """Excel's round_discharge_to_comma_separated_string renders a
        blank cell for ANY negative value, before rounding — a legitimate
        case for forecast_lower_bound on low-flow forecasts. None is the
        JSON equivalent of that blank cell."""
        site = _site_with_attrs(
            forecast_expected=10.0,
            forecast_lower_bound=-3.2,
        )
        row = bp.serialize_site(site, "pentad")
        assert row["fc_lower"] is None
        assert row["forecasted_discharge"] == 10.0

        long_term_site = _site_with_attrs(forecast_q_min=-1.5)
        long_term_row = bp.serialize_site(long_term_site, "month")
        assert long_term_row["q_min"] is None

    def test_perc_norm_banker_rounding(self):
        """PERC_NORM uses Python's round() (banker's rounding to even),
        matching Excel's round_percentage_to_integer_string."""
        site_down = _site_with_attrs(forecast_expected=10.0, perc_norm=84.5)
        assert bp.serialize_site(site_down, "pentad")["perc_norm"] == 84

        site_up = _site_with_attrs(forecast_expected=10.0, perc_norm=85.5)
        assert bp.serialize_site(site_up, "pentad")["perc_norm"] == 86

    def test_round_2dp_handles_nan_none(self):
        site = _site_with_attrs(forecast_expected=10.0, forecast_delta=float("nan"))
        # forecast_sdivsigma intentionally left unset -> missing attribute.
        row = bp.serialize_site(site, "pentad")
        assert row["delta"] is None
        assert row["sdivsigma"] is None


# ---------------------------------------------------------------------------
# 3. assemble_bulletin_snapshot
# ---------------------------------------------------------------------------


class _FakeDataManager:
    """Minimal DataManager stand-in: assemble_bulletin_snapshot only
    touches `sites_list` — the (year, horizon_value) period now comes
    from db._read_data, not dm.get_bulletin_metadata."""

    def __init__(self, sites_list):
        self.sites_list = sites_list


def _bulletin_records_df(rows):
    """Build a synthetic bulletin DataFrame as db._read_data would return.

    `rows` is a list of dicts with at least `code`, `year`,
    `horizon_value` (as db._read_data("postprocessing", "bulletin", ...)
    would return for the "bulletin" resource).
    """
    if not rows:
        return pd.DataFrame(columns=["code", "year", "horizon_value"])
    return pd.DataFrame(rows)


class TestAssembleBulletinSnapshot:
    def test_selected_code_with_data_is_included(self, monkeypatch):
        site = _site_with_attrs(
            code="90001",
            forecast_expected=10.0,
            forecast_lower_bound=9.0,
            forecast_upper_bound=11.0,
            forecast_delta=1.0,
            forecast_sdivsigma=0.4,
            forecast_mae=0.3,
            forecast_accuracy=90.0,
            perc_norm=100.0,
        )
        monkeypatch.setattr(bulletin_manager, "_load_bulletin_from_api", lambda *a, **k: [site])
        monkeypatch.setattr(
            db,
            "_read_data",
            lambda *a, **k: _bulletin_records_df(
                [{"code": "90001", "year": 2026, "horizon_value": 13}]
            ),
        )
        dm = _FakeDataManager([site])

        result = bp.assemble_bulletin_snapshot("pentad", ["90001"], dm, dt.date(2026, 3, 1))

        assert result["skipped_codes"] == []
        stations = result["payload"]["stations"]
        assert len(stations) == 1
        assert stations[0]["code"] == "90001"
        assert result["payload"]["horizon"] == "pentad"
        assert result["payload"]["year"] == 2026
        assert result["payload"]["horizon_value"] == 13
        assert result["payload"]["valid_from"] == "2026-03-01"
        assert result["payload"]["valid_to"] == "2026-03-05"
        assert result["payload"]["expires_at"] == "2026-03-06T00:00:00Z"

    def test_selected_code_without_data_is_skipped(self, monkeypatch):
        site = _site_with_attrs(code="90001", forecast_expected=10.0)
        monkeypatch.setattr(bulletin_manager, "_load_bulletin_from_api", lambda *a, **k: [site])
        monkeypatch.setattr(
            db,
            "_read_data",
            lambda *a, **k: _bulletin_records_df(
                [{"code": "90001", "year": 2026, "horizon_value": 13}]
            ),
        )
        dm = _FakeDataManager([site])

        result = bp.assemble_bulletin_snapshot(
            "pentad", ["90001", "90002"], dm, dt.date(2026, 3, 1)
        )

        assert result["skipped_codes"] == ["90002"]
        codes_in_payload = [s["code"] for s in result["payload"]["stations"]]
        assert codes_in_payload == ["90001"]

    def test_no_bulletin_records_for_selected_codes_yields_empty_stations(self, monkeypatch):
        """A horizon whose bulletin query returns no rows for the
        selected codes (e.g. no bulletin has ever been produced for this
        horizon/station combo, or the active dashboard horizon differs
        from the one being published) must not raise — it should report
        an empty snapshot and skip every selected code."""
        load_calls = []
        monkeypatch.setattr(
            bulletin_manager,
            "_load_bulletin_from_api",
            lambda *a, **k: load_calls.append(a) or [],
        )
        # Bulletin rows exist for this horizon, but only for a different
        # (foreign) station code — none for the selected codes.
        monkeypatch.setattr(
            db,
            "_read_data",
            lambda *a, **k: _bulletin_records_df(
                [{"code": "99999", "year": 2026, "horizon_value": 4}]
            ),
        )
        dm = _FakeDataManager([])

        result = bp.assemble_bulletin_snapshot("month", ["90001"], dm, dt.date(2026, 3, 1))

        assert result["payload"]["stations"] == []
        assert result["skipped_codes"] == ["90001"]
        assert result["payload"]["horizon"] == "month"
        assert result["payload"]["year"] is None
        assert result["payload"]["horizon_value"] is None
        assert result["payload"]["valid_from"] is None
        assert result["payload"]["expires_at"] is None
        # No point querying _load_bulletin_from_api once we know there is
        # no data for the selected stations.
        assert load_calls == []

    def test_period_is_taken_from_bulletin_records_not_from_dm(self, monkeypatch):
        """The active dashboard horizon (whatever dm would return via the
        legacy get_bulletin_metadata) must NOT influence the resolved
        (year, horizon_value) — only the persisted bulletin records for
        the selected stations under the horizon being published do. Using
        a dm without get_bulletin_metadata at all proves it is never
        called."""
        site = _site_with_attrs(code="90001", forecast_expected=5.0)
        seen_args = []

        def fake_load(horizon, forecast_year, forecast_horizon, sites_list):
            seen_args.append((horizon, forecast_year, forecast_horizon))
            return [site]

        monkeypatch.setattr(bulletin_manager, "_load_bulletin_from_api", fake_load)
        # Two rows for the selected station under the "month" horizon —
        # 2025/9 and the later 2026/4 — the LATEST one must be picked.
        monkeypatch.setattr(
            db,
            "_read_data",
            lambda *a, **k: _bulletin_records_df(
                [
                    {"code": "90001", "year": 2025, "horizon_value": 9},
                    {"code": "90001", "year": 2026, "horizon_value": 4},
                ]
            ),
        )
        dm = _FakeDataManager([site])  # no get_bulletin_metadata attribute at all

        result = bp.assemble_bulletin_snapshot("month", ["90001"], dm, dt.date(2026, 3, 1))

        assert seen_args == [("month", 2026, 4)]
        assert result["payload"]["year"] == 2026
        assert result["payload"]["horizon_value"] == 4
        assert result["payload"]["valid_from"] == "2026-04-01"


# ---------------------------------------------------------------------------
# 4. WidgetManager._on_generate_links_click (button handler)
# ---------------------------------------------------------------------------


class _FakeWidget:
    def __init__(self, value):
        self.value = value


class _FakeResultsPane:
    def __init__(self):
        self.object = ""


def _make_wm_stub(horizons, station_labels):
    """A lightweight fake `self` sufficient to call
    _on_generate_links_click unbound (mirrors _make_manager_stub in
    test_bulletin_edit_persistence.py). Constructing a real WidgetManager
    is impractical here: its __init__ needs a full DataManager,
    DashboardConfig, and station_dict."""
    fake_self = types.SimpleNamespace(
        _gettext=lambda s: s,
        _dm=object(),
        publish_horizon_multiselect=_FakeWidget(horizons),
        publish_station_multiselect=_FakeWidget(station_labels),
        publish_results_pane=_FakeResultsPane(),
    )
    fake_self._render_publish_results = lambda links, warnings, skipped: (
        WidgetManager._render_publish_results(fake_self, links, warnings, skipped)
    )
    return fake_self


def _fake_payload(horizon, stations_codes):
    return {
        "horizon": horizon,
        "year": 2026,
        "horizon_value": 1,
        "valid_from": "2026-01-01",
        "valid_to": "2026-01-05",
        "generated_at": "2026-01-01T00:00:00Z",
        "expires_at": "2026-01-06T00:00:00Z",
        "stations": [{"code": c} for c in stations_codes],
    }


class TestOnGenerateLinksClick:
    def test_n_selected_horizons_yield_n_posts_and_n_links(self, monkeypatch):
        fake_self = _make_wm_stub(["pentad", "month"], ["90001 - Test River X"])
        post_calls = []

        def fake_assemble(horizon, selected_codes, dm, forecast_date):
            assert selected_codes == ["90001"]
            return {"payload": _fake_payload(horizon, ["90001"]), "skipped_codes": []}

        def fake_post(payload):
            post_calls.append(payload)
            return {
                "token": f"tok-{payload['horizon']}",
                "url": f"https://example.org/public/bulletin/{payload['horizon']}",
                "expires_at": payload["expires_at"],
            }

        monkeypatch.setattr(
            widget_manager.bulletin_publish, "assemble_bulletin_snapshot", fake_assemble
        )
        monkeypatch.setattr(widget_manager.db, "_post_bulletin_share", fake_post)

        WidgetManager._on_generate_links_click(fake_self, None)

        assert len(post_calls) == 2
        assert {c["horizon"] for c in post_calls} == {"pentad", "month"}
        rendered = fake_self.publish_results_pane.object
        assert "https://example.org/public/bulletin/pentad" in rendered
        assert "https://example.org/public/bulletin/month" in rendered

    def test_empty_horizon_produces_no_link_but_a_warning(self, monkeypatch):
        fake_self = _make_wm_stub(["pentad", "month"], ["90001 - Test River X"])
        post_calls = []

        def fake_assemble(horizon, selected_codes, dm, forecast_date):
            if horizon == "month":
                return {"payload": _fake_payload(horizon, []), "skipped_codes": ["90001"]}
            return {"payload": _fake_payload(horizon, ["90001"]), "skipped_codes": []}

        def fake_post(payload):
            post_calls.append(payload)
            return {
                "token": "tok",
                "url": f"https://example.org/public/bulletin/{payload['horizon']}",
                "expires_at": payload["expires_at"],
            }

        monkeypatch.setattr(
            widget_manager.bulletin_publish, "assemble_bulletin_snapshot", fake_assemble
        )
        monkeypatch.setattr(widget_manager.db, "_post_bulletin_share", fake_post)

        WidgetManager._on_generate_links_click(fake_self, None)

        # Only the pentad horizon actually shared a link.
        assert len(post_calls) == 1
        assert post_calls[0]["horizon"] == "pentad"
        rendered = fake_self.publish_results_pane.object
        assert "https://example.org/public/bulletin/pentad" in rendered
        assert "https://example.org/public/bulletin/month" not in rendered
        # A warning about the empty horizon must be surfaced.
        assert "month" in rendered.lower() or "Warnings" in rendered

    def test_assemble_failure_aborts_with_no_partial_links(self, monkeypatch):
        """The first horizon assembles+posts successfully; the second
        horizon's assembly raises. The handler must show only the error —
        NOT the first horizon's already-generated link."""
        fake_self = _make_wm_stub(["pentad", "month"], ["90001 - Test River X"])
        post_calls = []

        def fake_assemble(horizon, selected_codes, dm, forecast_date):
            if horizon == "month":
                raise RuntimeError("boom")
            return {"payload": _fake_payload(horizon, ["90001"]), "skipped_codes": []}

        def fake_post(payload):
            post_calls.append(payload)
            return {
                "token": "tok",
                "url": f"https://example.org/public/bulletin/{payload['horizon']}",
                "expires_at": payload["expires_at"],
            }

        monkeypatch.setattr(
            widget_manager.bulletin_publish, "assemble_bulletin_snapshot", fake_assemble
        )
        monkeypatch.setattr(widget_manager.db, "_post_bulletin_share", fake_post)

        WidgetManager._on_generate_links_click(fake_self, None)

        rendered = fake_self.publish_results_pane.object
        assert "https://example.org" not in rendered
        assert "Error" in rendered or "error" in rendered.lower()

    def test_post_failure_aborts_with_no_partial_links(self, monkeypatch):
        """The first horizon's share POST succeeds; the second horizon's
        POST raises. No links (including the first) may be rendered."""
        fake_self = _make_wm_stub(["pentad", "month"], ["90001 - Test River X"])
        post_calls = []

        def fake_assemble(horizon, selected_codes, dm, forecast_date):
            return {"payload": _fake_payload(horizon, ["90001"]), "skipped_codes": []}

        def fake_post(payload):
            post_calls.append(payload)
            if payload["horizon"] == "month":
                raise RuntimeError("service unreachable")
            return {
                "token": "tok",
                "url": f"https://example.org/public/bulletin/{payload['horizon']}",
                "expires_at": payload["expires_at"],
            }

        monkeypatch.setattr(
            widget_manager.bulletin_publish, "assemble_bulletin_snapshot", fake_assemble
        )
        monkeypatch.setattr(widget_manager.db, "_post_bulletin_share", fake_post)

        WidgetManager._on_generate_links_click(fake_self, None)

        rendered = fake_self.publish_results_pane.object
        assert "https://example.org" not in rendered
        assert "Error" in rendered or "error" in rendered.lower()

    def test_no_horizon_selected_shows_warning_and_makes_no_calls(self, monkeypatch):
        fake_self = _make_wm_stub([], ["90001 - Test River X"])
        assemble_calls = []
        post_calls = []
        monkeypatch.setattr(
            widget_manager.bulletin_publish,
            "assemble_bulletin_snapshot",
            lambda *a, **k: assemble_calls.append(a) or {},
        )
        monkeypatch.setattr(
            widget_manager.db,
            "_post_bulletin_share",
            lambda *a, **k: post_calls.append(a) or {},
        )

        WidgetManager._on_generate_links_click(fake_self, None)

        assert assemble_calls == []
        assert post_calls == []
        assert fake_self.publish_results_pane.object != ""

    def test_no_station_selected_shows_warning_and_makes_no_calls(self, monkeypatch):
        fake_self = _make_wm_stub(["pentad"], [])
        assemble_calls = []
        monkeypatch.setattr(
            widget_manager.bulletin_publish,
            "assemble_bulletin_snapshot",
            lambda *a, **k: assemble_calls.append(a) or {},
        )

        WidgetManager._on_generate_links_click(fake_self, None)

        assert assemble_calls == []
        assert fake_self.publish_results_pane.object != ""

    def test_station_codes_extracted_from_labels(self, monkeypatch):
        fake_self = _make_wm_stub(["pentad"], ["90001 - Test River X", "90002 - Test River Y"])
        seen_codes = []

        def fake_assemble(horizon, selected_codes, dm, forecast_date):
            seen_codes.extend(selected_codes)
            return {"payload": _fake_payload(horizon, selected_codes), "skipped_codes": []}

        monkeypatch.setattr(
            widget_manager.bulletin_publish, "assemble_bulletin_snapshot", fake_assemble
        )
        monkeypatch.setattr(
            widget_manager.db,
            "_post_bulletin_share",
            lambda payload: {"token": "t", "url": "https://x", "expires_at": payload["expires_at"]},
        )

        WidgetManager._on_generate_links_click(fake_self, None)

        assert seen_codes == ["90001", "90002"]
