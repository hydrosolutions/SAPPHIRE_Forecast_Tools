"""Round-trip tests for the quarterly snapshot at the write_to_excel
report-generator boundary (PP-075 Q).

Strategy: bootstrap src.bulletins with a minimal real Tag stub so lambdas
are preserved; replace MultiSectionReportGenerator with a capturer that
records list_objects_per_section and tags_per_section.  Call the REAL
write_to_excel so the section-selection and tag-binding logic is not
duplicated here.  _populate_quarterly_from_snapshot hydrates site attrs;
expected Excel values use round_discharge_to_comma_separated_string directly
so the assertion is independently computed, not a copy of the production lambda.

Synthetic stations 19999 / 19998 only. No real station codes.
"""

from __future__ import annotations

import sys
import types
from datetime import date as _date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Bootstrap: stubs for heavy imports BEFORE importing src.bulletins
# ---------------------------------------------------------------------------

_FAKE_KEYS = [
    "panel", "panel.viewable", "panel.widgets",
    "panel.layout", "panel.pane", "panel.template",
    "src.gettext_config", "dashboard.logger", "src.db", "src.reports",
    "ieasyreports", "ieasyreports.settings",
    "ieasyreports.core", "ieasyreports.core.tags", "ieasyreports.core.tags.tag",
    "ieasyreports.core.report_generator", "ieasyreports.exceptions",
    "tag_library", "skill_lead_aware_flag", "dashboard.utils",
    "src.bulletins", "src",
]

_saved = {k: sys.modules[k] for k in _FAKE_KEYS if k in sys.modules}

try:
    for _mod in [
        "panel", "panel.viewable", "panel.widgets",
        "panel.layout", "panel.pane", "panel.template",
        "tag_library",
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

    if "src.reports" not in sys.modules:
        sys.modules["src.reports"] = MagicMock()

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

    for _mod in [
        "ieasyreports", "ieasyreports.settings",
        "ieasyreports.core", "ieasyreports.core.tags", "ieasyreports.exceptions",
    ]:
        if _mod not in sys.modules:
            sys.modules[_mod] = MagicMock()

    # Real Tag stub: preserves get_value_fn so captured lambdas can be exercised
    # on site objects after write_to_excel returns.
    _tag_mod = types.ModuleType("ieasyreports.core.tags.tag")

    class _StubTag:
        def __init__(self, name, get_value_fn, tag_settings=None,
                     data=False, header=False):
            self.name = name
            self.get_value_fn = get_value_fn
            self.data = data
            self.header = header
            self._obj = None

        def set_context(self, ctx):
            if isinstance(ctx, dict):
                self._obj = ctx.get("obj", self._obj)

        def replace(self, _cell_value=None):
            if callable(self.get_value_fn) and self._obj is not None:
                return self.get_value_fn(obj=self._obj)
            if callable(self.get_value_fn):
                return self.get_value_fn()
            return self.get_value_fn

    _tag_mod.Tag = _StubTag
    sys.modules["ieasyreports.core.tags.tag"] = _tag_mod

    # Real stub DefaultReportGenerator so MultiSectionReportGenerator inherits correctly.
    _rg_mod = types.ModuleType("ieasyreports.core.report_generator")

    class _StubDefaultReportGenerator:
        pass

    _rg_mod.DefaultReportGenerator = _StubDefaultReportGenerator
    sys.modules["ieasyreports.core.report_generator"] = _rg_mod

    # Force a fresh import of src.bulletins so the Tag lookup resolves to _StubTag.
    for _clear in ("src.bulletins", "src"):
        sys.modules.pop(_clear, None)

    import sys as _sys
    import os as _os
    _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))

    from src import bulletins
    from dashboard import bulletin_manager
    from dashboard import bulletin_publish as bp
    from src.site import SapphireSite

    _populate_quarterly_from_snapshot = bulletin_manager._populate_quarterly_from_snapshot
    write_to_excel = bulletins.write_to_excel
    round_discharge = bulletins.round_discharge_to_comma_separated_string

finally:
    for _k in _FAKE_KEYS:
        if _k in _saved:
            sys.modules[_k] = _saved[_k]
        elif _k in sys.modules:
            del sys.modules[_k]
    del _saved, _FAKE_KEYS


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

Q1_VF, Q1_VT = "2026-01-01", "2026-03-31"
Q2_VF, Q2_VT = "2026-04-01", "2026-06-30"


def _make_snap(fc_lower=80.0, fc_upper=120.0, vf=Q1_VF, vt=Q1_VT):
    return {
        "model_type": "LR",
        "valid_from": vf,
        "valid_to": vt,
        "forecasted_discharge": 100.0,
        "fc_lower": fc_lower,
        "fc_upper": fc_upper,
    }


def _make_reservoir(code="19999", snap=None):
    """Return a SapphireSite whose punkt_name_ru contains 'вдхр' (reservoir)."""
    site = SapphireSite(
        code=code,
        river_name_ru="River R",
        punkt_name_ru="вдхр P",
        basin_ru="Basin B",
        bulletin_order=1,
    )
    site.station_label = f"{code} - River R вдхр P"
    site.hydrograph_norm = None
    if snap is not None:
        site.quarterly_snapshot = snap
    return site


def _header_df():
    return pd.DataFrame([{
        "year": 2026,
        "month_number": 1,
        "month_str_nom_ru": "январь",
        "month_str_gen_ru": "января",
        "prev_year": 2025,
    }])


def _run_write_to_excel(bulletin_sites, monkeypatch):
    """Call real write_to_excel with a capturer replacing MultiSectionReportGenerator.

    Returns captured dict with keys 'list_objects_per_section' and
    'tags_per_section'.  Re-raises ValueError from conflict detection (not
    swallowed so callers can assert on it).
    """
    captured = {}

    class _Capturer:
        def __init__(self, tags, template, templates_directory_path,
                     reports_directory_path, tag_settings,
                     requires_header=False, tags_per_section=None):
            captured["tags_per_section"] = tags_per_section

        def validate(self):
            pass

        def generate_report_multi(self, list_objects_per_section=None,
                                   output_filename=None, output_path=None):
            captured["list_objects_per_section"] = list_objects_per_section

    monkeypatch.setattr(bulletins, "MultiSectionReportGenerator", _Capturer)
    monkeypatch.setenv("ieasyreports_report_output_path", "/tmp")
    monkeypatch.setenv("ieasyforecast_template_month_bulletin_file", "tmpl.xlsx")
    monkeypatch.setenv("ieasyreports_templates_directory_path", "/tmp")
    monkeypatch.setenv("ieasyhydroforecast_organization", "test")

    write_to_excel(
        sites_list=[],
        bulletin_sites=bulletin_sites,
        header_df=_header_df(),
        env_file_path="",
        horizon="month",
    )
    return captured


# ---------------------------------------------------------------------------
# 1. Section 2 presence at the generate_report_multi boundary
# ---------------------------------------------------------------------------


class TestWriteToExcelSec2Presence:
    """list_objects_per_section[2] must reflect quarterly_snapshot presence."""

    def test_absent_snapshot_sec2_is_none(self, monkeypatch):
        site = _make_reservoir()  # no quarterly_snapshot attr
        cap = _run_write_to_excel([site], monkeypatch)
        assert cap["list_objects_per_section"][2] is None

    def test_snapshot_null_bounds_still_included_in_sec2(self, monkeypatch):
        """Snapshot present with null bounds → section heading retained, site in sec2."""
        site = _make_reservoir(snap=_make_snap(fc_lower=None, fc_upper=None))
        cap = _run_write_to_excel([site], monkeypatch)
        sec2 = cap["list_objects_per_section"][2]
        assert sec2 is not None and site in sec2

    def test_mixed_reservoirs_sec2_contains_only_snapshot_entries(self, monkeypatch):
        site_with = _make_reservoir("19999", snap=_make_snap())
        site_without = _make_reservoir("19998")  # no snapshot
        cap = _run_write_to_excel([site_with, site_without], monkeypatch)
        sec2 = cap["list_objects_per_section"][2] or []
        assert site_with in sec2
        assert site_without not in sec2

    def test_zero_bounds_site_included_in_sec2(self, monkeypatch):
        """Zero is a valid numeric bound; site must appear in sec2."""
        site = _make_reservoir(snap=_make_snap(fc_lower=0.0, fc_upper=0.0))
        cap = _run_write_to_excel([site], monkeypatch)
        sec2 = cap["list_objects_per_section"][2]
        assert sec2 is not None and site in sec2

    def test_non_reservoir_with_snapshot_excluded_from_sec2(self, monkeypatch):
        """Non-reservoir (no 'вдхр') is filtered out before section-2 selection."""
        non_res = SapphireSite(
            code="19999",
            river_name_ru="River R",
            punkt_name_ru="Punkt P",  # no вдхр → non-reservoir
            basin_ru="Basin B",
            bulletin_order=1,
        )
        non_res.station_label = "19999 - River R Punkt P"
        non_res.hydrograph_norm = None
        non_res.quarterly_snapshot = _make_snap()
        cap = _run_write_to_excel([non_res], monkeypatch)
        assert cap["list_objects_per_section"][2] is None


# ---------------------------------------------------------------------------
# 2. Conflict detection through real write_to_excel
# ---------------------------------------------------------------------------


class TestWriteToExcelConflict:
    def test_conflicting_periods_raise_value_error(self, monkeypatch):
        site_q1 = _make_reservoir("19999", snap=_make_snap(vf=Q1_VF, vt=Q1_VT))
        site_q2 = _make_reservoir("19998", snap=_make_snap(vf=Q2_VF, vt=Q2_VT))
        monkeypatch.setattr(bulletins, "MultiSectionReportGenerator", MagicMock())
        monkeypatch.setenv("ieasyreports_report_output_path", "/tmp")
        monkeypatch.setenv("ieasyforecast_template_month_bulletin_file", "tmpl.xlsx")
        monkeypatch.setenv("ieasyreports_templates_directory_path", "/tmp")
        monkeypatch.setenv("ieasyhydroforecast_organization", "test")

        with pytest.raises(ValueError, match="[Cc]onflict"):
            write_to_excel(
                sites_list=[],
                bulletin_sites=[site_q1, site_q2],
                header_df=_header_df(),
                env_file_path="",
                horizon="month",
            )

    def test_conflict_detected_before_generator_constructed(self, monkeypatch):
        """ValueError is raised before MultiSectionReportGenerator.__init__ runs."""
        init_called = []

        class _Sentinel:
            def __init__(self, *args, **kwargs):
                init_called.append(True)
            def validate(self): pass
            def generate_report_multi(self, *args, **kwargs): pass

        monkeypatch.setattr(bulletins, "MultiSectionReportGenerator", _Sentinel)
        monkeypatch.setenv("ieasyreports_report_output_path", "/tmp")
        monkeypatch.setenv("ieasyforecast_template_month_bulletin_file", "tmpl.xlsx")
        monkeypatch.setenv("ieasyreports_templates_directory_path", "/tmp")
        monkeypatch.setenv("ieasyhydroforecast_organization", "test")

        site_q1 = _make_reservoir("19999", snap=_make_snap(vf=Q1_VF, vt=Q1_VT))
        site_q2 = _make_reservoir("19998", snap=_make_snap(vf=Q2_VF, vt=Q2_VT))

        with pytest.raises(ValueError):
            write_to_excel(
                sites_list=[],
                bulletin_sites=[site_q1, site_q2],
                header_df=_header_df(),
                env_file_path="",
                horizon="month",
            )
        assert not init_called

    def test_same_period_two_reservoirs_no_conflict(self, monkeypatch):
        site_a = _make_reservoir("19999", snap=_make_snap(vf=Q1_VF, vt=Q1_VT))
        site_b = _make_reservoir("19998", snap=_make_snap(vf=Q1_VF, vt=Q1_VT))
        cap = _run_write_to_excel([site_a, site_b], monkeypatch)
        assert "list_objects_per_section" in cap


# ---------------------------------------------------------------------------
# 3. Sec2 tag value roundtrip via captured lambdas
# ---------------------------------------------------------------------------


class TestSec2TagFormattingRoundtrip:
    """Exercise captured sec2 tag lambdas with sites hydrated by
    _populate_quarterly_from_snapshot.  The expected value is independently
    computed by calling round_discharge_to_comma_separated_string on the
    known input — not by copying the tag lambda.
    """

    def _get_tag(self, cap, name):
        for t in (cap["tags_per_section"] or [[]])[2] or []:
            if t.name == name:
                return t
        raise KeyError(f"sec2 tag {name!r} not found")

    def _hydrated(self, fc_lower, fc_upper):
        """Reservoir site with fc_lower/fc_upper populated via real production function."""
        site = _make_reservoir(snap=_make_snap(fc_lower=fc_lower, fc_upper=fc_upper))
        _populate_quarterly_from_snapshot(site, lambda x: x)
        return site

    def test_q_min_tag_value_matches_formatter_applied_to_raw_bound(self, monkeypatch):
        fc_lower = 80.1234
        site = self._hydrated(fc_lower=fc_lower, fc_upper=120.0)
        cap = _run_write_to_excel([site], monkeypatch)
        tag = self._get_tag(cap, "Q_MIN")
        tag.set_context({"obj": site})
        assert tag.replace() == round_discharge(fc_lower)

    def test_q_max_tag_value_matches_formatter_applied_to_raw_bound(self, monkeypatch):
        fc_upper = 120.5678
        site = self._hydrated(fc_lower=80.0, fc_upper=fc_upper)
        cap = _run_write_to_excel([site], monkeypatch)
        tag = self._get_tag(cap, "Q_MAX")
        tag.set_context({"obj": site})
        assert tag.replace() == round_discharge(fc_upper)

    def test_null_bounds_produce_blank_q_cells(self, monkeypatch):
        site = self._hydrated(fc_lower=None, fc_upper=None)
        cap = _run_write_to_excel([site], monkeypatch)
        for name in ("Q_MIN", "Q_MAX"):
            tag = self._get_tag(cap, name)
            tag.set_context({"obj": site})
            assert tag.replace() == "", f"sec2 {name} must be blank for null bounds"

    def test_zero_bounds_produce_zero_string(self, monkeypatch):
        site = self._hydrated(fc_lower=0.0, fc_upper=0.0)
        cap = _run_write_to_excel([site], monkeypatch)
        for name in ("Q_MIN", "Q_MAX"):
            tag = self._get_tag(cap, name)
            tag.set_context({"obj": site})
            assert tag.replace() == "0", f"sec2 {name} must be '0' for zero bounds"


# ---------------------------------------------------------------------------
# 4. serialize_site raw precision + consistency with Excel formatter
# ---------------------------------------------------------------------------


class TestSerializeSiteRawPrecision:
    """_quarterly_snapshot_to_public preserves fc_lower/fc_upper as raw floats.
    The Excel formatter (round_discharge_to_comma_separated_string) produces a
    different, rounded string.  Both start from the same snapshot value.
    """

    def _site_month(self, snap=None):
        site = types.SimpleNamespace(
            code="19999",
            station_label="19999 - River R вдхр P",
            basin_ru="Basin B",
            river_name_ru="River R",
            forecast_model="LR",
            forecast_expected=None,
            forecast_q_min=None,
            forecast_q_max=None,
            forecast_v_min=None,
            forecast_v_max=None,
            forecast_norm=None,
            perc_norm=None,
        )
        if snap is not None:
            site.quarterly_snapshot = snap
        return site

    def test_fc_lower_preserved_raw(self):
        fc_lower = 80.1234
        site = self._site_month(snap=_make_snap(fc_lower=fc_lower, fc_upper=120.0))
        row = bp.serialize_site(site, "month")
        assert row["quarterly"]["fc_lower"] == pytest.approx(80.1234)

    def test_fc_upper_preserved_raw(self):
        fc_upper = 120.5678
        site = self._site_month(snap=_make_snap(fc_lower=80.0, fc_upper=fc_upper))
        row = bp.serialize_site(site, "month")
        assert row["quarterly"]["fc_upper"] == pytest.approx(120.5678)

    def test_null_snapshot_quarterly_key_is_none(self):
        site = self._site_month()  # no quarterly_snapshot
        row = bp.serialize_site(site, "month")
        assert row["quarterly"] is None

    def test_raw_differs_from_excel_formatted_value(self):
        """JSON preserves raw float; Excel rounds to 3sf.  Intentionally different."""
        fc_lower = 80.1234
        site = self._site_month(snap=_make_snap(fc_lower=fc_lower, fc_upper=120.0))
        row = bp.serialize_site(site, "month")
        raw = row["quarterly"]["fc_lower"]
        excel_fmt = round_discharge(fc_lower)
        assert isinstance(raw, float) and raw == pytest.approx(80.1234)
        assert excel_fmt == "80,1"  # 80.1234 → 3sf → 80.1 → comma-sep

    @pytest.mark.parametrize("fc_lower,fc_upper", [
        (0.0, 0.0),
        (0.001, 0.002),
        (1234.5, 5678.9),
    ])
    def test_raw_values_preserved_for_various_magnitudes(self, fc_lower, fc_upper):
        site = self._site_month(snap=_make_snap(fc_lower=fc_lower, fc_upper=fc_upper))
        row = bp.serialize_site(site, "month")
        assert row["quarterly"]["fc_lower"] == pytest.approx(fc_lower)
        assert row["quarterly"]["fc_upper"] == pytest.approx(fc_upper)


# ---------------------------------------------------------------------------
# 5. assemble_bulletin_snapshot: conflict check is scoped to selected_codes
# ---------------------------------------------------------------------------


class TestAssembleBulletinSnapshotSubsetScoping:
    """The quarterly-period conflict check in assemble_bulletin_snapshot must be
    scoped to selected_codes only, so a consistent subset can be shared even
    when the bulletin DB contains other sites with conflicting periods.
    """

    def _run_assemble(self, bulletin_sites, selected_codes):
        bdf = pd.DataFrame([
            {"code": s.code, "year": 2026, "horizon_value": 1}
            for s in bulletin_sites
        ])
        with (
            patch(
                "dashboard.bulletin_manager._load_bulletin_from_api",
                return_value=bulletin_sites,
            ),
            patch("src.db._read_data", return_value=bdf),
        ):
            dm = types.SimpleNamespace(sites_list=[])
            return bp.assemble_bulletin_snapshot(
                "month", selected_codes, dm, _date(2026, 1, 15),
            )

    def test_selecting_one_of_two_conflicting_sites_passes(self):
        """19999 (Q1) and 19998 (Q2) both in bulletin.
        Selecting ONLY 19999 (consistent subset) must not raise."""
        site_q1 = _make_reservoir("19999", snap=_make_snap(vf=Q1_VF, vt=Q1_VT))
        site_q2 = _make_reservoir("19998", snap=_make_snap(vf=Q2_VF, vt=Q2_VT))
        result = self._run_assemble([site_q1, site_q2], ["19999"])
        assert "payload" in result

    def test_selecting_both_conflicting_sites_raises(self):
        """Selecting both conflicting sites must raise ValueError."""
        site_q1 = _make_reservoir("19999", snap=_make_snap(vf=Q1_VF, vt=Q1_VT))
        site_q2 = _make_reservoir("19998", snap=_make_snap(vf=Q2_VF, vt=Q2_VT))
        with pytest.raises(ValueError, match="[Cc]onflict"):
            self._run_assemble([site_q1, site_q2], ["19999", "19998"])

    def test_non_selected_conflicting_site_does_not_appear_in_stations(self):
        """Non-selected sites must not appear in the payload stations list."""
        site_q1 = _make_reservoir("19999", snap=_make_snap(vf=Q1_VF, vt=Q1_VT))
        site_q1.forecast_model = "LR"
        site_q1.forecast_expected = None
        site_q1.forecast_q_min = None
        site_q1.forecast_q_max = None
        site_q1.forecast_v_min = None
        site_q1.forecast_v_max = None
        site_q1.forecast_norm = None
        site_q1.perc_norm = None
        site_q2 = _make_reservoir("19998", snap=_make_snap(vf=Q2_VF, vt=Q2_VT))
        result = self._run_assemble([site_q1, site_q2], ["19999"])
        codes = [s["code"] for s in result["payload"]["stations"]]
        assert "19999" in codes
        assert "19998" not in codes
