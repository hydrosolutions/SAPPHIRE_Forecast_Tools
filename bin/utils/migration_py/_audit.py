"""Stdlib-only import audit for the migration_py package.

This module is deliberately introspective: it walks every ``*.py`` file under
the ``migration_py/`` package directory, parses each one with ``ast.parse``,
and reports any imported root module that is neither part of the Python
standard library (``sys.stdlib_module_names``) nor a sibling module inside
this package.

The audit is invoked from
``apps/iEasyHydroForecast/tests/test_migration_audit.py``; a non-empty list
means the test fails. The intent is to prevent silent addition of third-party
dependencies (e.g. ``pandas``, ``requests``, ``psycopg2``) that would force a
new Docker image variant — explicitly out of scope per architecture §Q1.

No third-party imports are allowed here; the audit module is itself under
audit scrutiny.
"""

from __future__ import annotations

import ast
import pathlib
import sys


def collect_imported_roots(source: str) -> set[str]:
    """Parse Python source and return all imported root module names.

    Uses :func:`ast.parse` and walks all :class:`ast.Import` and
    :class:`ast.ImportFrom` nodes.

    - For ``import a.b.c`` -> ``{"a"}`` (root only).
    - For ``from a.b import c`` -> ``{"a"}``.
    - For ``from . import x`` or ``from .sibling import y`` (level >= 1) ->
      nothing (relative imports are intra-package sibling references and are
      never checked against ``sys.stdlib_module_names``).

    Args:
        source: Python source text.

    Returns:
        Set of root module names imported in this source.
    """
    tree = ast.parse(source)
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                # alias.name is the full dotted path; take the first segment.
                root = alias.name.split(".", 1)[0]
                if root:
                    roots.add(root)
        elif isinstance(node, ast.ImportFrom):
            if (node.level or 0) > 0:
                # Relative import; treat as intra-package sibling.
                continue
            if node.module is None:
                continue
            root = node.module.split(".", 1)[0]
            if root:
                roots.add(root)
    return roots


def package_module_names(package_dir: pathlib.Path) -> set[str]:
    """Return module names defined in this package directory.

    Used as the intra-package allowlist. Includes every ``*.py`` file stem
    under ``package_dir`` (top-level only; does not recurse). Excludes
    ``__init__`` since ``from __init__ import x`` is not legal.

    Args:
        package_dir: path to the migration_py package directory.

    Returns:
        Set of module names (e.g. ``{"_common", "_audit", "runoff_day"}``).
    """
    names: set[str] = set()
    if not package_dir.is_dir():
        return names
    for entry in package_dir.iterdir():
        if not entry.is_file():
            continue
        if entry.suffix != ".py":
            continue
        stem = entry.stem
        if stem == "__init__":
            continue
        names.add(stem)
    return names


def audit_stdlib_only(package_dir: pathlib.Path) -> list[str]:
    """Audit every ``*.py`` under ``package_dir`` for stdlib-only imports.

    Returns a list of human-readable violation messages, one per offending
    ``(file, import)`` pair. Empty list means PASS.

    Logic::

        allowed = sys.stdlib_module_names | package_module_names(package_dir)
        for each *.py file in package_dir (top-level):
            imported = collect_imported_roots(file.read_text())
            violations = imported - allowed
            for v in sorted(violations):
                append "<relpath>: non-stdlib import: <v>"

    Args:
        package_dir: path to the migration_py package directory.

    Returns:
        Sorted list of violation strings; empty means clean.
    """
    package_dir = pathlib.Path(package_dir)
    allowed = set(sys.stdlib_module_names) | package_module_names(package_dir)
    violations: list[str] = []
    if not package_dir.is_dir():
        return violations
    for entry in sorted(package_dir.iterdir()):
        if not entry.is_file() or entry.suffix != ".py":
            continue
        try:
            source = entry.read_text(encoding="utf-8")
        except OSError as exc:  # pragma: no cover - defensive
            violations.append(f"{entry.name}: cannot read source: {exc}")
            continue
        imported = collect_imported_roots(source)
        for offending in sorted(imported - allowed):
            violations.append(f"{entry.name}: non-stdlib import: {offending}")
    return violations
