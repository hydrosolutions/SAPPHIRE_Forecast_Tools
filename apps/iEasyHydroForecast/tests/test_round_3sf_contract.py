"""LOCKED acceptance tests for milestone M1 — the 3-significant-figure
hydrological rounding contract.

Covers the M1 rounding-contract criteria for the two shared helpers that back
BOTH the hydrographs write path and the bulletin/dashboard formatter:

  * ``round_3sf(x) -> float | None``  — decimal-safe HALF-UP 3 sig figs
  * ``format_discharge(x) -> str``    — magnitude-aware 3sf string, locale-free

These are the unit contract only (per-horizon stored==displayed parity is M2/M3).

Constraints honoured: fake station code '19999' where a code is needed, no real
discharge values, no pinning of internal implementation beyond the published
numeric/string contract.

These tests MUST fail until ``round_3sf`` and ``format_discharge`` exist in
``iEasyHydroForecast.forecast_library`` and satisfy the contract, and must not
be weakened.
"""

import pytest

from iEasyHydroForecast import forecast_library as fl

# A fake station code, used only to document that these helpers operate on
# discharge values for a (fake) site; no real code or discharge is used.
FAKE_STATION_CODE = "19999"


# ---------------------------------------------------------------------------
# round_3sf — decimal-safe HALF-UP 3 significant figures (numeric equalities)
# ---------------------------------------------------------------------------

# (input, expected) — these are the load-bearing equalities from the vision.
# 2.565 and -2.565 are the canary that separates decimal-safe HALF-UP from a
# naive binary-float round() (which wrongly yields 2.56 / -2.56).
ROUND_3SF_CASES = [
    (1245.67, 1250),
    (124.67, 125),
    (24.67, 24.7),
    (2.565, 2.57),
    (0.2368, 0.237),
    (99.95, 100),
    (999.5, 1000),
    (9.995, 10),
    (0.9995, 1),
    (-2.565, -2.57),  # negatives round away from zero
]


@pytest.mark.parametrize("value, expected", ROUND_3SF_CASES)
def test_round_3sf_numeric_equalities(value, expected):
    """round_3sf produces the exact 3sf HALF-UP value (decimal-safe)."""
    result = fl.round_3sf(value)
    assert result == expected


def test_round_3sf_returns_float():
    """The contract is ``-> float`` for finite input, not Decimal/str/int."""
    result = fl.round_3sf(24.67)
    assert isinstance(result, float)


def test_round_3sf_decimal_safe_not_binary_round():
    """2.565 must round UP to 2.57 (binary float round() gives 2.56)."""
    assert fl.round_3sf(2.565) == 2.57
    assert fl.round_3sf(-2.565) == -2.57
    # Guard against an implementation that merely calls builtin round():
    assert fl.round_3sf(2.565) != 2.56


def test_round_3sf_zero_and_negative_zero():
    """0.0 and -0.0 both map to 0.0 without raising (no log/divide-by-zero)."""
    assert fl.round_3sf(0.0) == 0.0
    assert fl.round_3sf(-0.0) == 0.0


# ---------------------------------------------------------------------------
# round_3sf — non-finite / None contract (load-bearing): -> None, never raises
# ---------------------------------------------------------------------------

NON_FINITE_INPUTS = [
    None,
    float("nan"),
    float("inf"),
    float("-inf"),
]


@pytest.mark.parametrize("value", NON_FINITE_INPUTS)
def test_round_3sf_non_finite_returns_none(value):
    """round_3sf(None|NaN|+Inf|-Inf) returns None and does not raise."""
    assert fl.round_3sf(value) is None


# ---------------------------------------------------------------------------
# format_discharge — 3sf STRING, trailing significant zeros, locale-free
# ---------------------------------------------------------------------------


def test_format_discharge_preserves_trailing_significant_zeros():
    """Trailing significant zeros are kept: 10.0 -> '10.0', 1.0 -> '1.00'."""
    assert fl.format_discharge(10.0) == "10.0"
    assert fl.format_discharge(1.0) == "1.00"


# (input, expected string) — magnitude-aware 3sf strings that do not cross a
# magnitude band on rounding, so the display precision is unambiguous.
FORMAT_DISCHARGE_CASES = [
    (124.67, "125"),
    (24.67, "24.7"),
    (2.565, "2.57"),
    (0.2368, "0.237"),
]


@pytest.mark.parametrize("value, expected", FORMAT_DISCHARGE_CASES)
def test_format_discharge_string_form(value, expected):
    """format_discharge renders the magnitude-aware 3sf string, plain '.'."""
    assert fl.format_discharge(value) == expected


@pytest.mark.parametrize(
    "value",
    [124.67, 24.67, 2.565, 0.2368, 10.0, 1.0],
)
def test_format_discharge_agrees_numerically_with_round_3sf(value):
    """The formatted string parses (plain-dot) back to round_3sf(value).

    This ties the formatter to the shared 3sf helper so stored == displayed,
    without pinning the exact display precision beyond the contract.
    """
    parsed = float(fl.format_discharge(value))
    assert parsed == fl.round_3sf(value)


def test_format_discharge_is_locale_free():
    """Locale-free: plain '.' decimal, never a comma (locale is layered on)."""
    formatted = fl.format_discharge(2.565)
    assert "," not in formatted
    assert "." in formatted


@pytest.mark.parametrize("value", NON_FINITE_INPUTS)
def test_format_discharge_non_finite_returns_empty_string(value):
    """format_discharge(None|NaN|+Inf|-Inf) -> '' (blank cell), never raises."""
    assert fl.format_discharge(value) == ""
