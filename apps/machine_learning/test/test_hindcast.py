"""
Tests for hindcast_ML_models.py structural properties.

These tests validate source-level contracts without executing main() or
importing heavy ML dependencies (torch, darts, pytorch_lightning).

Covered contracts:
- Write order: API write appears before CSV write in main()
- Dead code: PATH_TO_PAST_DISCHARGE removed
- Flag convention: flag=4 (valid) and flag=3 (NaN) are assigned and documented
- normalize_ml_csv_columns called before CSV write
"""

import os

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HINDCAST_PATH = os.path.join(os.path.dirname(__file__), "..", "hindcast_ML_models.py")


def _read_source() -> str:
    """Return full source of hindcast_ML_models.py as a string."""
    with open(_HINDCAST_PATH) as fh:
        return fh.read()


def _extract_main_body(source: str) -> str:
    """Return only the body of main() — everything after 'def main():'."""
    marker = "def main():"
    idx = source.find(marker)
    assert idx != -1, "def main(): not found in hindcast_ML_models.py"
    return source[idx:]


# ---------------------------------------------------------------------------
# Test 1 — Write order: API first, then CSV
# ---------------------------------------------------------------------------


def _find_uncommented(source: str, needle: str) -> int:
    """Return the position of the first occurrence of *needle* that is NOT on
    a comment line (i.e. the line does not start with optional whitespace + #).
    Returns -1 if not found.
    """
    pos = 0
    while True:
        idx = source.find(needle, pos)
        if idx == -1:
            return -1
        # Find the start of the line containing this match
        line_start = source.rfind("\n", 0, idx) + 1
        line = source[line_start:idx]
        if not line.lstrip().startswith("#"):
            return idx
        pos = idx + 1


def test_api_write_before_csv_in_main():
    """API write must appear before CSV write in hindcast_ML_models.py main().

    The API is the primary storage path; CSV is the archive/fallback.
    Commented-out to_csv lines are excluded from this check.
    """
    main_body = _extract_main_body(_read_source())

    api_pos = _find_uncommented(main_body, "_write_ml_forecast_to_api")
    csv_pos = _find_uncommented(main_body, ".to_csv(")

    assert api_pos != -1, "_write_ml_forecast_to_api not found (uncommented) in main()"
    assert csv_pos != -1, ".to_csv() not found (uncommented) in main()"
    assert api_pos < csv_pos, (
        f"API write (pos {api_pos}) must come before CSV write (pos {csv_pos}) "
        "in main().  API is the primary path; CSV is archive/fallback."
    )


def test_api_write_guarded_by_sapphire_api_available():
    """API write must be guarded by SAPPHIRE_API_AVAILABLE to avoid crashes
    when the sapphire-api-client package is not installed."""
    main_body = _extract_main_body(_read_source())
    api_pos = main_body.find("_write_ml_forecast_to_api")
    assert api_pos != -1, "_write_ml_forecast_to_api not found in main()"

    # The guard must appear somewhere before the API call
    snippet_before = main_body[:api_pos]
    assert "SAPPHIRE_API_AVAILABLE" in snippet_before, (
        "_write_ml_forecast_to_api must be guarded by 'if SAPPHIRE_API_AVAILABLE'"
    )


# ---------------------------------------------------------------------------
# Test 2 — Dead code PATH_TO_PAST_DISCHARGE removed
# ---------------------------------------------------------------------------


def test_dead_code_path_to_past_discharge_removed():
    """PATH_TO_PAST_DISCHARGE should not appear in hindcast_ML_models.py.

    The variable was a dead-code remnant that read a discharge file path from
    the environment but never used it in the hindcast logic.  It has been
    removed to keep the module clean.
    """
    source = _read_source()
    assert "PATH_TO_PAST_DISCHARGE" not in source, (
        "Dead code PATH_TO_PAST_DISCHARGE still present in hindcast_ML_models.py"
    )


# ---------------------------------------------------------------------------
# Test 3 — Flag convention documented and implemented
# ---------------------------------------------------------------------------


def test_flag_4_assigned_for_valid_forecasts():
    """flag = 4 must be assigned as the default (valid forecast) value."""
    main_body = _extract_main_body(_read_source())
    assert 'flag"] = 4' in main_body, (
        "flag = 4 assignment not found in main(). "
        "Flag convention: 4 = valid forecast (all quantiles present)."
    )


def test_flag_3_assigned_for_nan_forecasts():
    """flag = 3 must be assigned when forecast values are NaN."""
    main_body = _extract_main_body(_read_source())
    assert 'flag"] = 3' in main_body, (
        "flag = 3 assignment not found in main(). Flag convention: 3 = NaN / missing forecast."
    )


def test_flag_convention_documented_in_comments():
    """A comment documenting flag values 3 and 4 must appear in main().

    The comment distinguishes hindcast flags (3/4) from operational flags
    (0/1/2) used by make_forecast.py.
    """
    main_body = _extract_main_body(_read_source())
    # Check that an explanatory comment references both flag values
    assert "3" in main_body and "4" in main_body, "Flag values 3 and 4 must appear in main()"
    # A comment block must be present near the flag assignments
    flagging_section = main_body[
        main_body.find('flag"] = 4') - 300 : main_body.find('flag"] = 4') + 100
    ]
    assert "#" in flagging_section, (
        "No comment found near flag assignments — flag convention must be documented"
    )


# ---------------------------------------------------------------------------
# Test 4 — normalize_ml_csv_columns called before CSV write
# ---------------------------------------------------------------------------


def test_normalize_ml_csv_columns_imported():
    """normalize_ml_csv_columns must be imported in hindcast_ML_models.py."""
    source = _read_source()
    assert "normalize_ml_csv_columns" in source, (
        "normalize_ml_csv_columns import missing from hindcast_ML_models.py"
    )


def test_normalize_before_csv_write_in_output_section():
    """normalize_ml_csv_columns must be called before .to_csv() in main().

    The function normalises column names so that the CSV schema matches the
    expected format.  It must be applied to the DataFrame before writing.
    """
    main_body = _extract_main_body(_read_source())

    # Anchor to the output section (after the flag block, before end)
    output_anchor = "Write to SAPPHIRE API"
    if output_anchor not in main_body:
        # Fallback anchor if comment wording differs
        output_anchor = "SAVE HINDECAST"

    output_section = main_body[main_body.find(output_anchor) :]

    normalize_pos = output_section.find("normalize_ml_csv_columns(")
    csv_pos = output_section.find(".to_csv(")

    assert normalize_pos != -1, (
        "normalize_ml_csv_columns() not called in the output section of main()"
    )
    assert csv_pos != -1, ".to_csv() not found in the output section of main()"
    assert normalize_pos < csv_pos, (
        f"normalize_ml_csv_columns (pos {normalize_pos}) must be called "
        f"before .to_csv() (pos {csv_pos}) in the output section"
    )
