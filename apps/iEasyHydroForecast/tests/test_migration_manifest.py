"""Tests for migration_py._common.validate_manifest — Stage E items #2, #3.

v2 R6 adds station_count and date_min/date_max cross-checks (and matching
typed exceptions ManifestStationCountMismatchError, ManifestDateRangeMismatchError).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "bin" / "utils"))

from migration_py import _common  # noqa: E402

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _write_csv(path: Path, rows: list[tuple[str, str, str]]) -> None:
    """Write a CSV with header `code,date,value` and the given rows."""
    lines = ["code,date,value"]
    for code, date, value in rows:
        lines.append(f"{code},{date},{value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _default_rows() -> list[tuple[str, str, str]]:
    # 5 rows, single sentinel station 19999, dates 2026-01-01 .. 2026-01-05.
    return [
        ("19999", "2026-01-01", "10.0"),
        ("19999", "2026-01-02", "10.5"),
        ("19999", "2026-01-03", "10.7"),
        ("19999", "2026-01-04", "11.0"),
        ("19999", "2026-01-05", "11.2"),
    ]


def _default_manifest(
    *,
    row_count: int = 5,
    station_count: int = 1,
    export_type: str = "runoff_period",
    date_min: str = "2026-01-01",
    date_max: str = "2026-01-05",
) -> str:
    return (
        f"export_type={export_type}\n"
        f"row_count={row_count}\n"
        f"station_count={station_count}\n"
        f"date_min={date_min}\n"
        f"date_max={date_max}\n"
    )


@pytest.fixture()
def manifest_pair(tmp_path):
    """Write a default CSV+manifest pair; return the CSV path."""
    csv_path = tmp_path / "runoff_period_export.csv"
    _write_csv(csv_path, _default_rows())
    manifest_path = tmp_path / "runoff_period_export.csv.manifest"
    manifest_path.write_text(_default_manifest(), encoding="utf-8")
    return csv_path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_validate_manifest_happy_path(manifest_pair):
    result = _common.validate_manifest(manifest_pair, "runoff_period")
    assert isinstance(result, dict)
    assert result["export_type"] == "runoff_period"
    assert result["row_count"] == "5"
    assert result["station_count"] == "1"


def test_validate_manifest_missing_raises(tmp_path):
    csv_path = tmp_path / "no_manifest.csv"
    _write_csv(csv_path, _default_rows())
    with pytest.raises(_common.ManifestMissingError):
        _common.validate_manifest(csv_path, "runoff_period")


def test_validate_manifest_row_count_mismatch_raises(tmp_path):
    csv_path = tmp_path / "rowcount.csv"
    _write_csv(csv_path, _default_rows())  # 5 rows
    manifest = tmp_path / "rowcount.csv.manifest"
    manifest.write_text(_default_manifest(row_count=10), encoding="utf-8")
    with pytest.raises(_common.ManifestRowCountMismatchError) as exc_info:
        _common.validate_manifest(csv_path, "runoff_period")
    msg = str(exc_info.value)
    assert "10" in msg and "5" in msg


def test_validate_manifest_wrong_export_type_raises(tmp_path):
    csv_path = tmp_path / "wrongtype.csv"
    _write_csv(csv_path, _default_rows())
    manifest = tmp_path / "wrongtype.csv.manifest"
    manifest.write_text(_default_manifest(export_type="runoff_period"), encoding="utf-8")
    with pytest.raises(_common.ManifestExportTypeMismatchError):
        _common.validate_manifest(csv_path, "hydrograph_period")


def test_validate_manifest_unknown_export_type_raises(tmp_path):
    csv_path = tmp_path / "unknowntype.csv"
    _write_csv(csv_path, _default_rows())
    manifest = tmp_path / "unknowntype.csv.manifest"
    manifest.write_text(_default_manifest(export_type="not_a_real_type"), encoding="utf-8")
    with pytest.raises(_common.ManifestExportTypeMismatchError):
        _common.validate_manifest(csv_path, "runoff_period")


def test_validate_manifest_station_count_mismatch_raises(tmp_path):
    """v2 R6: manifest says station_count=3 but CSV has 5 distinct codes."""
    csv_path = tmp_path / "stationcount.csv"
    rows = [
        ("19999", "2026-01-01", "1.0"),
        ("00000", "2026-01-02", "2.0"),
        ("00001", "2026-01-03", "3.0"),
        ("00002", "2026-01-04", "4.0"),
        ("00003", "2026-01-05", "5.0"),
    ]
    _write_csv(csv_path, rows)
    manifest = tmp_path / "stationcount.csv.manifest"
    manifest.write_text(_default_manifest(row_count=5, station_count=3), encoding="utf-8")
    with pytest.raises(_common.ManifestStationCountMismatchError) as exc_info:
        _common.validate_manifest(csv_path, "runoff_period")
    msg = str(exc_info.value)
    assert "3" in msg and "5" in msg


def test_validate_manifest_date_range_mismatch_raises(tmp_path):
    """v2 R6: manifest says date_max=2025-12-31 but CSV reaches 2026-01-15."""
    csv_path = tmp_path / "daterange.csv"
    rows = [
        ("19999", "2026-01-01", "1.0"),
        ("19999", "2026-01-15", "2.0"),
    ]
    _write_csv(csv_path, rows)
    manifest = tmp_path / "daterange.csv.manifest"
    manifest.write_text(
        _default_manifest(
            row_count=2,
            station_count=1,
            date_min="2026-01-01",
            date_max="2025-12-31",
        ),
        encoding="utf-8",
    )
    with pytest.raises(_common.ManifestDateRangeMismatchError) as exc_info:
        _common.validate_manifest(csv_path, "runoff_period")
    msg = str(exc_info.value)
    assert "2025-12-31" in msg and "2026-01-15" in msg


def test_validate_manifest_strips_comments_and_blanks(tmp_path):
    csv_path = tmp_path / "comments.csv"
    _write_csv(csv_path, _default_rows())
    manifest = tmp_path / "comments.csv.manifest"
    manifest.write_text(
        "# generated by export_runoff_period_history.sh\n"
        "\n"
        "export_type=runoff_period\n"
        "# pre-cutoff filter applied\n"
        "row_count=5\n"
        "\n"
        "station_count=1\n"
        "date_min=2026-01-01\n"
        "date_max=2026-01-05\n",
        encoding="utf-8",
    )
    result = _common.validate_manifest(csv_path, "runoff_period")
    assert result["export_type"] == "runoff_period"


def test_validate_manifest_handles_trailing_whitespace(tmp_path):
    csv_path = tmp_path / "trailing.csv"
    _write_csv(csv_path, _default_rows())
    manifest = tmp_path / "trailing.csv.manifest"
    manifest.write_text(
        "export_type=runoff_period   \n"
        "row_count=5  \n"
        "station_count=1\t\n"
        "date_min=2026-01-01 \n"
        "date_max=2026-01-05\n",
        encoding="utf-8",
    )
    result = _common.validate_manifest(csv_path, "runoff_period")
    assert result["row_count"] == "5"
