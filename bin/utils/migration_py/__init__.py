"""Namespace package for stdlib-only update-time migration helpers.

Modules under this package MUST import only from the Python standard library or
from sibling modules inside this package. Any third-party import would imply a
new Docker image variant (see architecture plan §Q1). The `_audit.py` module
enforces this rule via an AST walk + `sys.stdlib_module_names` check; the
companion pytest module `apps/iEasyHydroForecast/tests/test_migration_audit.py`
fails CI if a violation is introduced.
"""
