"""Tests for hydrological_round and
round_discharge_hydrological_to_comma_separated_string (bulletins.py).

These functions provide 3-significant-figure rounding with a comma decimal
separator for the long-term forecast bulletin value columns.  A key bug that
motivated this addition: values >= 1000 were rendered in scientific notation
(e.g. '1.23E+4') because Decimal.__str__ uses exponential form for large
values.  The fix is to use format(d, 'f') instead of str(d).

Import bootstrap mirrors test_bulletin_header_date.py: we mock heavy Panel
dependencies before importing src.bulletins so the pure helpers can be
tested in isolation.
"""

import sys
import types
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Bootstrap: mock heavy dashboard dependencies before importing src.bulletins
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
    "src.reports",
    "ieasyreports",
    "ieasyreports.settings",
    "ieasyreports.core",
    "ieasyreports.core.tags",
    "ieasyreports.core.tags.tag",
    "ieasyreports.core.report_generator",
    "tag_library",
    "openpyxl",
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
        "openpyxl",
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

    for _mod in [
        "ieasyreports",
        "ieasyreports.settings",
        "ieasyreports.core",
        "ieasyreports.core.tags",
        "ieasyreports.core.tags.tag",
        "ieasyreports.core.report_generator",
    ]:
        if _mod not in sys.modules:
            sys.modules[_mod] = MagicMock()

    from src import bulletins

    hydrological_round = bulletins.hydrological_round
    round_discharge_hydrological_to_comma_separated_string = (
        bulletins.round_discharge_hydrological_to_comma_separated_string
    )

finally:
    for _k in _FAKE_KEYS:
        if _k in _saved:
            sys.modules[_k] = _saved[_k]
        elif _k in sys.modules:
            del sys.modules[_k]
    del _saved, _FAKE_KEYS


# ---------------------------------------------------------------------------
# Tests for hydrological_round
# ---------------------------------------------------------------------------


class TestHydrologicalRound:
    """Unit tests for the hydrological_round helper."""

    def test_none_returns_none(self):
        assert hydrological_round(None) is None

    def test_zero_returns_decimal_zero(self):
        result = hydrological_round(0)
        assert result == Decimal("0.00")

    def test_zero_float_returns_decimal_zero(self):
        result = hydrological_round(0.0)
        assert result == Decimal("0.00")

    def test_three_sig_figs_1_234(self):
        """1.234 rounded to 3 sig figs → 1.23"""
        result = hydrological_round(1.234)
        assert result == Decimal("1.23")

    def test_three_sig_figs_12_34(self):
        """12.34 rounded to 3 sig figs → 12.3"""
        result = hydrological_round(12.34)
        assert result == Decimal("12.3")

    def test_three_sig_figs_123_4(self):
        """123.4 rounded to 3 sig figs → 123"""
        result = hydrological_round(123.4)
        assert result == Decimal("123")

    def test_three_sig_figs_0_123(self):
        """0.123 rounded to 3 sig figs → 0.123 (≥ 3 sig figs kept for < 1)"""
        # For numbers < 1 the function quantizes to 2 decimal places → 0.12
        result = hydrological_round(0.123)
        assert result == Decimal("0.12")

    def test_rounding_bump_999_9(self):
        """999.9 rounded to 3 sig figs → 1000"""
        result = hydrological_round(999.9)
        assert result == Decimal("1000")

    # ── Bug-fix: large values must NOT use scientific notation ───────────────

    def test_large_value_1234_5(self):
        """1234.5 → 1230 (not 1.23E+3 in string form)."""
        result = hydrological_round(1234.5)
        assert result == Decimal("1230")

    def test_large_value_fixed_point_format_1234_5(self):
        """format(result, 'f') must not contain 'E' or 'e'."""
        result = hydrological_round(1234.5)
        formatted = format(result, "f")
        assert "E" not in formatted, f"Scientific notation found: {formatted}"
        assert "e" not in formatted, f"Scientific notation found: {formatted}"

    def test_large_value_12312_3(self):
        """12312.3 rounded to 3 sig figs → 12300."""
        result = hydrological_round(12312.3)
        assert result == Decimal("12300")

    def test_large_value_fixed_point_format_12312_3(self):
        """format(hydrological_round(12312.3), 'f') must not contain exponent."""
        result = hydrological_round(12312.3)
        formatted = format(result, "f")
        assert "E" not in formatted, f"Scientific notation found: {formatted}"
        assert "e" not in formatted, f"Scientific notation found: {formatted}"

    def test_large_value_123456(self):
        """123456.0 rounded to 3 sig figs → 123000."""
        result = hydrological_round(123456.0)
        assert result == Decimal("123000")


# ---------------------------------------------------------------------------
# Tests for round_discharge_hydrological_to_comma_separated_string
# ---------------------------------------------------------------------------


class TestRoundDischargeHydrologicalToCommaSeparatedString:
    """Unit tests for the public formatter function."""

    # ── Basic 3-sig-fig + comma decimal ─────────────────────────────────────

    def test_1_234_gives_1_comma_23(self):
        assert round_discharge_hydrological_to_comma_separated_string(1.234) == "1,23"

    def test_12_34_gives_12_comma_3(self):
        assert round_discharge_hydrological_to_comma_separated_string(12.34) == "12,3"

    def test_123_4_gives_123(self):
        assert round_discharge_hydrological_to_comma_separated_string(123.4) == "123"

    def test_0_123_gives_0_comma_12(self):
        assert round_discharge_hydrological_to_comma_separated_string(0.123) == "0,12"

    # ── The scientific-notation bug fix (regression tests) ───────────────────

    def test_1234_5_gives_1230_no_sci_notation(self):
        result = round_discharge_hydrological_to_comma_separated_string(1234.5)
        assert result == "1230", f"Expected '1230', got {result!r}"
        assert "E" not in result and "e" not in result

    def test_12312_3_gives_12300_no_sci_notation(self):
        result = round_discharge_hydrological_to_comma_separated_string(12312.3)
        assert result == "12300", f"Expected '12300', got {result!r}"
        assert "E" not in result and "e" not in result

    def test_123456_gives_123000_no_sci_notation(self):
        result = round_discharge_hydrological_to_comma_separated_string(123456.0)
        assert result == "123000", f"Expected '123000', got {result!r}"
        assert "E" not in result and "e" not in result

    # ── Rounding bump ────────────────────────────────────────────────────────

    def test_999_9_rounds_up_to_1000(self):
        assert round_discharge_hydrological_to_comma_separated_string(999.9) == "1000"

    # ── Zero ────────────────────────────────────────────────────────────────

    def test_zero_int_gives_0_comma_00(self):
        assert round_discharge_hydrological_to_comma_separated_string(0) == "0,00"

    def test_zero_float_gives_0_comma_00(self):
        assert round_discharge_hydrological_to_comma_separated_string(0.0) == "0,00"

    # ── Small value collapse (documents current behavior) ───────────────────

    def test_very_small_0_001234_collapses_to_0_comma_00(self):
        """0.001234 < 0.01, so after quantize(1.00) it becomes 0.00 → '0,00'."""
        result = round_discharge_hydrological_to_comma_separated_string(0.001234)
        assert result == "0,00"

    # ── Blank handling ───────────────────────────────────────────────────────

    def test_none_gives_blank(self):
        assert round_discharge_hydrological_to_comma_separated_string(None) == ""

    def test_negative_gives_blank(self):
        assert round_discharge_hydrological_to_comma_separated_string(-5.0) == ""
