"""GOLDEN tests — current monthly-lead behaviour (the kill-switch path).

These lock the CURRENT behaviour of the dashboard's monthly-lead handling on
``origin/maxat_sapphire_2`` (hard-coded lead 1 in five places).  They are all
GREEN today and must stay green under the kill-switch
``SAPPHIRE_SKILL_LEAD_AWARE=false`` (default) after the fix lands: flag-off must
be byte-identical to ``maxat``.

Every test sets ``SAPPHIRE_SKILL_LEAD_AWARE=false`` explicitly (matching the
flag's default-OFF value) so the legacy path is exercised. Some of these
assertions intentionally lock a KNOWN BUG (e.g. the ``month_0`` card currently
merges lead-1 skill) — that is deliberate: the golden proves the kill-switch
reproduces today's behaviour.

See:
- doc/plans/working/ltf_monthly_horizon_value_implementation_plan.md  (P-TEST)
- doc/plans/issues/high_prio_gi_draft_ltf_monthly_horizon_value_semantics.md
- doc/plans/issues/high_prio_gi_draft_pp_lead_aware_skill.md  (M1 P3, trunk's
  db.py implementation this branch converged onto)

No real station codes / discharge values: synthetic ``17999`` (Tajik-shaped) and
``15999`` (Kyrgyz-shaped) codes and arbitrary discharge numbers.
"""

import datetime
import json
import types
from unittest.mock import MagicMock

import pandas as pd
import requests
from dashboard import plot_manager, widgets
from dashboard.data_manager import DataManager
from src import db

FLAG = "SAPPHIRE_SKILL_LEAD_AWARE"


# ---------------------------------------------------------------------------
# Fixtures / builders (synthetic codes 15999 / 17999, arbitrary discharge)
# ---------------------------------------------------------------------------

def _mock_response(json_data, status_code=200):
    """Lightweight fake requests.Response (mirrors test_db._make_mock_response)."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


def _long_forecast_record(code, horizon_value, q):
    """A monthly long-forecast API row (valid_from April → month_in_year 4)."""
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
    """A monthly skill-metric API row that carries horizon_value (post-PP-038)."""
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
    """Neutralise label/i18n processing so merges are observable (mirrors test_db)."""
    monkeypatch.setattr("src.db.processing.add_labels_to_hydrograph", lambda df, stations: df)
    monkeypatch.setattr(
        "src.db.processing.internationalize_forecast_model_names",
        lambda fn, df, **kw: df,
    )


def _setup_config(monkeypatch, tmp_path, name, modes):
    """Write per-mode config JSONs and point the resolver env at them.

    modes: dict {config_mode_name: operational_month_lead_time}.
    Sets supported_modes = the mode names (comma-joined).  Mirrors the helper in
    test_monthly_lead_regression.py so the guard tests are forward-compatible
    with the resolver the fix introduces.
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


# ---------------------------------------------------------------------------
# _format_forecast_info's callers (plot_manager) — Defect A caption sites.
#
# _format_forecast_info itself does not read SAPPHIRE_SKILL_LEAD_AWARE — the
# gate lives in the CALLERS (update_forecast_tabulator:296 for month_1,
# update_forecast_tabulator_m0:340 for month_0). Calling _format_forecast_info
# directly with monkeypatch.setenv(FLAG, ...) is decorative: it never
# exercises those gates, so a caller that resolved the lead unconditionally
# (breaking the kill-switch) would still leave a directly-called test green.
# These tests drive the real callers and spy on the `lead` kwarg the caller
# passes down, so the gate itself is what's being pinned.
# ---------------------------------------------------------------------------

class TestFormatForecastInfoCurrent:
    def test_month1_caption_flag_off_passes_lead_none(self, monkeypatch):
        """update_forecast_tabulator (plot_manager.py:296): flag OFF must pass
        lead=None into _format_forecast_info regardless of the resolved
        operational lead — the legacy month_1 caption always assumes lead-1
        via the horizon_label fallback, never the resolved lead."""
        monkeypatch.setenv(FLAG, "false")
        # tjhm-shaped: resolved lead is 0; flag-off must still ignore it.
        monkeypatch.setattr(plot_manager, "_primary_month_lead", lambda: 0)

        calls = []

        def _spy_format_forecast_info(issue_date, horizon_label, lead=None):
            calls.append({"issue_date": issue_date, "horizon_label": horizon_label, "lead": lead})
            return "STUBBED"

        monkeypatch.setattr(plot_manager, "_format_forecast_info", _spy_format_forecast_info)

        forecasts_all = pd.DataFrame({"date": pd.to_datetime(["2026-07-01"])})
        fake_self = types.SimpleNamespace(
            _=lambda s: s,
            _cfg=types.SimpleNamespace(
                viz=types.SimpleNamespace(
                    create_forecast_summary_tabulator=lambda *a, **k: None
                )
            ),
            _wm=types.SimpleNamespace(
                horizon_selector=types.SimpleNamespace(value="month"),
                station_selector=MagicMock(),
                date_picker=MagicMock(),
                model_checkbox=MagicMock(),
                range_selector=MagicMock(),
                range_slider=MagicMock(),
                forecast_tabulator=MagicMock(),
                forecast_info_m1=types.SimpleNamespace(object=""),
            ),
            _dm=types.SimpleNamespace(forecasts_all=forecasts_all),
        )

        plot_manager.PlotManager.update_forecast_tabulator(fake_self)

        assert calls == [
            {"issue_date": forecasts_all["date"].max(), "horizon_label": "month_1", "lead": None}
        ], f"Expected lead=None (flag off), got {calls!r}"
        assert fake_self._wm.forecast_info_m1.object == "STUBBED"

    def test_month0_caption_flag_off_passes_lead_none(self, monkeypatch):
        """update_forecast_tabulator_m0 (plot_manager.py:340): flag OFF must
        pass lead=None into _format_forecast_info (the month_0 branch then
        falls back to the issue month itself)."""
        monkeypatch.setenv(FLAG, "false")

        calls = []

        def _spy_format_forecast_info(issue_date, horizon_label, lead=None):
            calls.append({"issue_date": issue_date, "horizon_label": horizon_label, "lead": lead})
            return "STUBBED"

        monkeypatch.setattr(plot_manager, "_format_forecast_info", _spy_format_forecast_info)

        # Legacy (flag-off) m0 visibility gate: summary_target_month =
        # (issue_month % 12) + 1. Issue in July (7) -> gate expects August (8),
        # so m0's own frame must carry an August date to pass it (this is the
        # pre-existing Defect F quirk — locked as the kill-switch path
        # elsewhere in this file; irrelevant to the lead kwarg under test).
        forecasts_all = pd.DataFrame({"date": pd.to_datetime(["2026-07-01"])})
        m0 = pd.DataFrame({"date": pd.to_datetime(["2026-08-01"])})
        fake_self = types.SimpleNamespace(
            _=lambda s: s,
            _cfg=types.SimpleNamespace(
                viz=types.SimpleNamespace(
                    create_forecast_summary_tabulator=lambda *a, **k: None
                )
            ),
            _wm=types.SimpleNamespace(
                station_selector=MagicMock(),
                model_checkbox=MagicMock(),
                range_selector=MagicMock(),
                range_slider=MagicMock(),
                forecast_tabulator_m0=MagicMock(),
                forecast_info_m0=types.SimpleNamespace(object=""),
            ),
            _dm=types.SimpleNamespace(forecasts_all=forecasts_all, long_forecasts_m0=m0),
            summary_table_m0_card=types.SimpleNamespace(visible=None),
        )

        plot_manager.PlotManager.update_forecast_tabulator_m0(fake_self)

        assert fake_self.summary_table_m0_card.visible is True, (
            "fixture must reach the _format_forecast_info call, not the "
            "early-return visibility gate"
        )
        assert calls == [
            {"issue_date": m0["date"].max().date(), "horizon_label": "month_0", "lead": None}
        ], f"Expected lead=None (flag off), got {calls!r}"
        assert fake_self._wm.forecast_info_m0.object == "STUBBED"


class TestFormatForecastInfoGuardOn:
    """Companion flag-ON cases — prove the same callers pass a real lead once
    the flag is on (guards a naive always-off mutation, symmetric to the
    flag-off checks above)."""

    def test_month1_caption_flag_on_passes_resolved_lead(self, monkeypatch):
        monkeypatch.setenv(FLAG, "true")
        monkeypatch.setattr(plot_manager, "_primary_month_lead", lambda: 1)

        calls = []

        def _spy_format_forecast_info(issue_date, horizon_label, lead=None):
            calls.append({"issue_date": issue_date, "horizon_label": horizon_label, "lead": lead})
            return "STUBBED"

        monkeypatch.setattr(plot_manager, "_format_forecast_info", _spy_format_forecast_info)

        forecasts_all = pd.DataFrame({"date": pd.to_datetime(["2026-07-01"])})
        fake_self = types.SimpleNamespace(
            _=lambda s: s,
            _cfg=types.SimpleNamespace(
                viz=types.SimpleNamespace(
                    create_forecast_summary_tabulator=lambda *a, **k: None
                )
            ),
            _wm=types.SimpleNamespace(
                horizon_selector=types.SimpleNamespace(value="month"),
                station_selector=MagicMock(),
                date_picker=MagicMock(),
                model_checkbox=MagicMock(),
                range_selector=MagicMock(),
                range_slider=MagicMock(),
                forecast_tabulator=MagicMock(),
                forecast_info_m1=types.SimpleNamespace(object=""),
            ),
            _dm=types.SimpleNamespace(forecasts_all=forecasts_all),
        )

        plot_manager.PlotManager.update_forecast_tabulator(fake_self)

        assert calls == [
            {"issue_date": forecasts_all["date"].max(), "horizon_label": "month_1", "lead": 1}
        ], f"Expected lead=1 (flag on, resolved), got {calls!r}"

    def test_month0_caption_flag_on_passes_lead_zero(self, monkeypatch):
        monkeypatch.setenv(FLAG, "true")
        monkeypatch.setattr(plot_manager, "_primary_month_lead", lambda: 1)

        calls = []

        def _spy_format_forecast_info(issue_date, horizon_label, lead=None):
            calls.append({"issue_date": issue_date, "horizon_label": horizon_label, "lead": lead})
            return "STUBBED"

        monkeypatch.setattr(plot_manager, "_format_forecast_info", _spy_format_forecast_info)

        # Flag-on m0 visibility gate: summary_target_month =
        # ((issue_month - 1 + primary_lead) % 12) + 1 = ((7-1+1)%12)+1 = 8.
        forecasts_all = pd.DataFrame({"date": pd.to_datetime(["2026-07-01"])})
        m0 = pd.DataFrame({"date": pd.to_datetime(["2026-08-01"])})
        fake_self = types.SimpleNamespace(
            _=lambda s: s,
            _cfg=types.SimpleNamespace(
                viz=types.SimpleNamespace(
                    create_forecast_summary_tabulator=lambda *a, **k: None
                )
            ),
            _wm=types.SimpleNamespace(
                station_selector=MagicMock(),
                model_checkbox=MagicMock(),
                range_selector=MagicMock(),
                range_slider=MagicMock(),
                forecast_tabulator_m0=MagicMock(),
                forecast_info_m0=types.SimpleNamespace(object=""),
            ),
            _dm=types.SimpleNamespace(forecasts_all=forecasts_all, long_forecasts_m0=m0),
            summary_table_m0_card=types.SimpleNamespace(visible=None),
        )

        plot_manager.PlotManager.update_forecast_tabulator_m0(fake_self)

        assert fake_self.summary_table_m0_card.visible is True
        assert calls == [
            {"issue_date": m0["date"].max().date(), "horizon_label": "month_0", "lead": 0}
        ], f"Expected lead=0 (m0 card is always the lead-0 product), got {calls!r}"


# ---------------------------------------------------------------------------
# Pure function: format_horizon_info (widgets) — Defect J header site
# ---------------------------------------------------------------------------

class TestFormatHorizonInfoCurrent:
    def test_month_ignores_passed_target_and_recomputes(self, monkeypatch):
        """The month header currently IGNORES forecast_horizon/forecast_year and
        recomputes the target month from last_date (production month + 1)."""
        monkeypatch.setenv(FLAG, "false")

        # Pass a deliberately-inconsistent target (month 1, year 2030): if the
        # current code used them the output would differ.  It does not.
        result = widgets.format_horizon_info(
            "month", forecast_horizon=1, forecast_year=2030,
            last_date=datetime.date(2026, 7, 1),
        )

        # production_date = 2026-06-30 → recomputed target = July, year = 2026.
        expected = "month: July 2026, produced on Jun 30, 2026"
        assert result == expected, f"Expected {expected!r}, got {result!r}"


# ---------------------------------------------------------------------------
# db.get_data("month", ...) — kghm-shaped, multi-lead skill (Defects A + F)
# Locks the CURRENT/kill-switch behaviour AND fills the _op_lead coverage gap.
# ---------------------------------------------------------------------------

class TestGetDataMonthMultiLeadCurrent:
    def test_main_panel_hv1_and_m0_card_annotated_from_lead1_stats(self, monkeypatch):
        """kghm: main panel selects hv1 forecast rows and merges hv1 skill; the
        month_0 (hv0) card is currently annotated from the lead-1-filtered stats.

        This locks Defect F's current (buggy) behaviour as the kill-switch path
        and exercises the multi-lead `_op_lead` filter (both leads present).
        """
        monkeypatch.setenv(FLAG, "false")
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
                    _skill_record_with_lead(code, 4, 0, delta=1.0),  # lead-0 skill
                    _skill_record_with_lead(code, 4, 1, delta=5.0),  # lead-1 skill
                ])
            return _mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        _patch_processing(monkeypatch)
        all_stations = pd.DataFrame({"code": [code], "station_labels": ["Test Reservoir K"]})

        data = db.get_data("month", code, all_stations)

        # Main panel: hv1 forecast (q=111) with lead-1 skill (delta=5.0).
        fa = data["forecasts_all"]
        assert list(fa["forecasted_discharge"]) == [111.0], (
            f"main panel should select hv1 forecast rows; got {list(fa['forecasted_discharge'])}"
        )
        assert fa["delta"].iloc[0] == 5.0

        # month_0 card: hv0 forecast (q=222) but currently annotated with lead-1
        # skill (delta=5.0) — the Defect F bug, locked as the kill-switch path.
        m0 = data["long_forecasts_m0"]
        assert list(m0["forecasted_discharge"]) == [222.0], (
            f"m0 card should carry hv0 forecast rows; got {list(m0['forecasted_discharge'])}"
        )
        assert m0["delta"].iloc[0] == 5.0, (
            "m0 currently merges the lead-1-filtered stats (Defect F, locked as flag-off)"
        )


# ---------------------------------------------------------------------------
# Flag-ON invariants (GUARD) — kghm must stay lead-1 under the FIX path.
# Green today (flag ignored) AND after the fix (resolver → same result).  These
# are NOT xfail: they are invariants that a naive "always resolve hv0" fix would
# break, catching a regression the tjhm xfail alone cannot.
# ---------------------------------------------------------------------------

class TestGetDataMonthLeadGuards:
    def test_kghm_main_panel_stays_hv1_under_flag_on(self, monkeypatch, tmp_path):
        """kghm, flag ON, config-resolved: the main panel must select the lead-1
        product (hv1, q=111) — the resolver returns 1 for kghm's month_1.

        Guards against a hardcoded-hv0 fix that would pass the tjhm→hv0
        regression while silently breaking kghm.  Green now (flag ignored → hv1)
        and green after (resolver → hv1).
        """
        monkeypatch.setenv(FLAG, "true")
        # kghm: month_1.json → lead 1 (flagship, day 25), month_0.json → lead 0 (day 10).
        _setup_config(monkeypatch, tmp_path, "kghm", {"month_0": 0, "month_1": 1})
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
            return _mock_response([])

        monkeypatch.setattr(requests, "get", mock_get)
        _patch_processing(monkeypatch)
        all_stations = pd.DataFrame({"code": [code], "station_labels": ["Test Reservoir K"]})

        data = db.get_data("month", code, all_stations)

        fa = data["forecasts_all"]
        assert list(fa["forecasted_discharge"]) == [111.0], (
            "kghm main panel must stay lead 1 (hv1, q=111) under the fix — "
            f"got {list(fa['forecasted_discharge'])}"
        )

    def test_tjhm_month0_card_absent_under_flag_on(self, monkeypatch, tmp_path):
        """tjhm (no month_0 in supported_modes), flag ON: the month_0 card is
        absent — long_forecasts_m0 is empty.  The month_0 gate holds for tjhm
        regardless of the flag.  Green now and after the fix.
        """
        monkeypatch.setenv(FLAG, "true")
        # tjhm: month_1.json → lead 0 (flagship, day 1), month_2.json → lead 1. No month_0.
        _setup_config(monkeypatch, tmp_path, "tjhm", {"month_1": 0, "month_2": 1})
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

        assert data["long_forecasts_m0"].empty, (
            "tjhm has no month_0 mode → the m0 card must be absent regardless of the flag"
        )


# ---------------------------------------------------------------------------
# Bulletin metadata (data_manager) + m0 hydration (bulletin_manager)
# Defects G + bulletin-year — current behaviour.
# ---------------------------------------------------------------------------

class TestBulletinMetadataCurrent:
    def test_forecast_year_is_issue_year_for_dec_issued_lead1(self, monkeypatch):
        """get_bulletin_metadata('month') flag OFF currently returns
        forecast_year = last_date.year (the issue year) — it never rolls the
        year, even for a December-issued forecast that targets January of the
        FOLLOWING year.

        Discriminating fixture (kghm-shaped: issued 2026-12-25, targeting
        January [month_in_year=1]): flag ON rolls this to 2027 (see
        test_monthly_lead_regression.py::
        TestBulletinTargetMonthYear::test_december_lead1_forecast_year_rolls_to_next_year).
        A fixture where the roll is a no-op (e.g. a mid-year issue date, or a
        target month that doesn't precede the issue month) would pass this
        assertion under EITHER flag value — deleting the
        skill_lead_aware_enabled() guard at data_manager.py:377 must turn
        this RED.
        """
        monkeypatch.setenv(FLAG, "false")

        fake = types.SimpleNamespace(
            forecasts_all=pd.DataFrame(
                {"date": pd.to_datetime(["2026-12-25"]), "month_in_year": [1]}
            ),
            horizon_in_year=lambda horizon: "month_in_year",
        )

        last_date, forecast_horizon, forecast_year = DataManager.get_bulletin_metadata(
            fake, "month"
        )

        # last_date = issue date + 1 day = 2026-12-26 (still December).
        assert forecast_horizon == 1
        assert forecast_year == last_date.year == 2026, (
            "flag-off (kill-switch) rule: forecast_year is the issue year "
            "(last_date.year), NOT rolled to 2027 even though the target "
            "month (January) precedes the issue month (December)"
        )


class TestLastDateYearVsMaxDateYearAsymmetry:
    """Pins a real discrepancy the adversarial review flagged while
    re-fixturing the test above: get_bulletin_metadata's flag-OFF branch
    derives the year from `last_date` (= max_date + 1 day), while the
    flag-ON branch derives it from `max_date` directly (data_manager.py:376-380).
    These normally agree, but diverge when the +1-day arithmetic itself
    crosses a calendar-year boundary: a Dec-31-issued, LEAD-0 forecast
    (target month = December, the same month/year as the issue).

    Flag ON is correct here: the target month (12) does not precede the
    issue month (12), so no rollover should happen -> 2026. Flag OFF
    (last_date.year) rolls anyway, purely as an artifact of the +1-day
    arithmetic landing on Jan 1 -> 2027, which is wrong for a lead-0 target.
    This is a pre-existing kill-switch quirk (present on trunk, not
    introduced by this branch) and is intentionally NOT fixed here — fixing
    data_manager.py would violate the flag-off byte-identical-to-trunk
    contract this branch must uphold. See report for the explicit verdict.
    """

    def test_dec31_lead0_flag_on_does_not_roll_year(self, monkeypatch):
        monkeypatch.setenv(FLAG, "true")

        fake = types.SimpleNamespace(
            forecasts_all=pd.DataFrame(
                {"date": pd.to_datetime(["2026-12-31"]), "month_in_year": [12]}
            ),
            horizon_in_year=lambda horizon: "month_in_year",
        )

        _last_date, forecast_horizon, forecast_year = DataManager.get_bulletin_metadata(
            fake, "month"
        )

        assert forecast_horizon == 12
        assert forecast_year == 2026, (
            "flag-ON (correct/lead-aware): a Dec-31 lead-0 forecast targets "
            "December of the SAME year — must not roll to 2027"
        )

    def test_dec31_lead0_flag_off_rolls_year_anyway_known_quirk(self, monkeypatch):
        monkeypatch.setenv(FLAG, "false")

        fake = types.SimpleNamespace(
            forecasts_all=pd.DataFrame(
                {"date": pd.to_datetime(["2026-12-31"]), "month_in_year": [12]}
            ),
            horizon_in_year=lambda horizon: "month_in_year",
        )

        last_date, forecast_horizon, forecast_year = DataManager.get_bulletin_metadata(
            fake, "month"
        )

        # last_date = 2026-12-31 + 1 day = 2027-01-01 -> .year is 2027, purely
        # from the +1-day arithmetic, NOT from any lead/target reasoning.
        assert last_date.year == 2027
        assert forecast_horizon == 12
        assert forecast_year == 2027, (
            "flag-OFF (kill-switch, known quirk): diverges from the flag-ON "
            "result (2026) for a Dec-31 lead-0 issue — locked as-is because "
            "flag-off must stay byte-identical to trunk"
        )
