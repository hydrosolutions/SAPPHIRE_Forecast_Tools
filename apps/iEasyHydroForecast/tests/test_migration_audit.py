"""Tests for migration_py._audit (stdlib-only import audit) — Stage E item #8.

The key assertion is that the actual shipped `migration_py/` package modules
use stdlib only. The other tests exercise corner cases of the parser
(relative imports, dotted paths, aliases, intra-package allowlist).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Repo root: apps/iEasyHydroForecast/tests/test_*.py -> parents[3]
_REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_REPO_ROOT / "bin" / "utils"))

from migration_py import _audit  # noqa: E402

_MIGRATION_PY_DIR = _REPO_ROOT / "bin" / "utils" / "migration_py"


def test_collect_imported_roots_skips_relative_imports():
    source = "from . import x\nfrom .sib import y\n"
    assert _audit.collect_imported_roots(source) == set()


def test_collect_imported_roots_strips_dotted_paths():
    source = "import a.b.c\nfrom d.e import f\n"
    assert _audit.collect_imported_roots(source) == {"a", "d"}


def test_collect_imported_roots_handles_aliases():
    source = "import numpy as np\nfrom pandas import DataFrame as DF\n"
    assert _audit.collect_imported_roots(source) == {"numpy", "pandas"}


def test_package_module_names_lists_top_level_only(tmp_path):
    (tmp_path / "_common.py").write_text("")
    (tmp_path / "_audit.py").write_text("")
    (tmp_path / "runoff_day.py").write_text("")
    (tmp_path / "__init__.py").write_text("")
    sub = tmp_path / "subpkg"
    sub.mkdir()
    (sub / "x.py").write_text("")

    names = _audit.package_module_names(tmp_path)
    assert names == {"_common", "_audit", "runoff_day"}
    assert "__init__" not in names
    assert "x" not in names  # subpackage not recursed


def test_audit_stdlib_only_passes_for_real_migration_py():
    """The actual shipped modules under bin/utils/migration_py/ use stdlib only.

    This is the binding contract enforced by P0: any agent adding pandas,
    requests, psycopg2, etc. to a migration_py module causes this assertion
    to fail in CI.
    """
    violations = _audit.audit_stdlib_only(_MIGRATION_PY_DIR)
    assert violations == [], f"migration_py modules introduced non-stdlib imports: {violations}"


def test_audit_stdlib_only_catches_third_party_import(tmp_path):
    fake = tmp_path / "fake_module.py"
    fake.write_text("import pandas\nimport scipy.stats\n")
    violations = _audit.audit_stdlib_only(tmp_path)
    joined = "\n".join(violations)
    assert "pandas" in joined
    assert "scipy" in joined
    # One violation per offending import.
    assert len(violations) == 2


def test_audit_stdlib_only_allows_intra_package(tmp_path):
    (tmp_path / "a.py").write_text("from b import x\n")
    (tmp_path / "b.py").write_text("pass\n")
    violations = _audit.audit_stdlib_only(tmp_path)
    assert violations == []
