"""Sentinel-fixture guard — Stage E item #11.

Walks `apps/iEasyHydroForecast/tests/fixtures/migration_csv/` and asserts:
- the directory exists
- no `test_*.py` or `conftest.py` files live there (pytest collection trap)
- every 5-digit number in every fixture file is in the sentinel allowlist
- the README documents the sentinel-code policy

The 5-digit regex uses word boundaries so that ISO dates (YYYY-MM-DD with
4-digit years), 4-digit years, and 6-digit timestamps are not flagged.
"""

from __future__ import annotations

import re
from pathlib import Path

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
_FIXTURES_DIR = _REPO_ROOT / "apps" / "iEasyHydroForecast" / "tests" / "fixtures" / "migration_csv"

ALLOWED_STATION_CODES: frozenset[str] = frozenset(
    {"19999"} | {f"0000{i}" for i in range(10)}  # 00000 .. 00009 for HRU sentinels
)

FIVE_DIGIT_CODE_RE = re.compile(r"\b\d{5}\b")


def test_fixtures_dir_exists():
    assert _FIXTURES_DIR.is_dir(), f"fixture dir missing: {_FIXTURES_DIR}"


def test_no_test_files_in_fixtures():
    """Pytest would collect test_*.py / conftest.py under the fixtures dir."""
    offenders: list[str] = []
    for path in _FIXTURES_DIR.rglob("*"):
        if not path.is_file():
            continue
        name = path.name
        if (name.startswith("test_") and name.endswith(".py")) or name == "conftest.py":
            offenders.append(str(path.relative_to(_REPO_ROOT)))
    assert not offenders, f"test files are forbidden under fixtures dir: {offenders}"


def test_fixture_files_contain_only_allowed_station_codes():
    """Every 5-digit number in every fixture file must be in the allowlist."""
    failures: list[str] = []
    for path in _FIXTURES_DIR.rglob("*"):
        if not path.is_file():
            continue
        # Skip the policy artefacts themselves.
        if path.name in {".gitkeep", "README.md"}:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            failures.append(f"{path}: read failed: {exc}")
            continue
        for match in FIVE_DIGIT_CODE_RE.finditer(text):
            code = match.group(0)
            if code not in ALLOWED_STATION_CODES:
                failures.append(
                    f"{path.relative_to(_REPO_ROOT)}: disallowed 5-digit code "
                    f"{code!r} (allowed: 19999 or 00000..00009)"
                )
    assert not failures, "\n".join(failures)


def test_fixture_readme_documents_policy():
    readme = _FIXTURES_DIR / "README.md"
    assert readme.is_file(), f"README missing: {readme}"
    text = readme.read_text(encoding="utf-8").lower()
    required_substrings = [
        "sentinel",
        "19999",
        "no real station codes",
        # Markdown-literal `test_*.py` mention; case-insensitive substring
        # check tolerates "No test files allowed here" headings, etc.
        "test_*.py",
    ]
    missing = [s for s in required_substrings if s not in text]
    assert not missing, f"README is missing required policy strings: {missing}"
