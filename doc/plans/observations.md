# Observations Log

A running log of observations, warnings, and notes discovered during development and operations.
Periodically review and triage into formal issues in `module_issues.md` or GitHub Issues.

## How to Use

1. **Capture quickly** — Add observations with date and context
2. **Review periodically** — Weekly or before releases, review and triage
3. **Triage outcomes**:
   - Create formal issue in `module_issues.md` → mark as `[TRIAGED: XX-001]`
   - Not actionable → mark as `[WONTFIX]` with reason
   - Already fixed → mark as `[RESOLVED]`

---

## 2026-01-27

### Dashboard: No Snow Data Displayed (Production Server)

**Source**: Testing current main on production server
**Date**: 2026-01-27

Snow data is not showing in the dashboard on the deployed version.

**Investigation needed**:
1. Check preprocessing_gateway logs for errors
2. Check if snow data exists in intermediate files
3. Check if dashboard has an issue reading/displaying snow data

**Assessment**: TBD - needs investigation
**Status**:

---

### Pipeline: Luigi Deprecation Warnings

**Source**: `bash run_tests.sh` on branch `dependency_updates_Jan_26`

```
pipeline/.venv/lib/python3.12/site-packages/luigi/parameter.py:408
  DeprecationWarning: datetime.datetime.utcfromtimestamp() is deprecated and scheduled
  for removal in a future version. Use timezone-aware objects to represent datetimes in
  UTC: datetime.datetime.fromtimestamp(timestamp, datetime.UTC).

pipeline/.venv/lib/python3.12/site-packages/luigi/__init__.py:87
  DeprecationWarning: Autoloading range tasks by default has been deprecated and will be
  removed in a future version. To get the behavior now add an option to luigi.cfg:
    [core]
      autoload_range: false
```

**Assessment**: These are upstream Luigi warnings, not our code. Monitor for Luigi updates that fix these.

---

### Pipeline: Test Class Collection Warning

**Source**: `bash run_tests.sh`

```
pipeline/tests/test_preprocessing.py:6
  PytestCollectionWarning: cannot collect test class 'TestPreprocessingGateway' because
  it has a __init__ constructor (from: tests/test_preprocessing.py)
    class TestPreprocessingGateway(luigi.Task):
```

**Assessment**: `TestPreprocessingGateway` is a Luigi Task, not a pytest test class. The naming is confusing pytest. Consider renaming to avoid `Test` prefix, or add `# noqa` marker.

---

### Linear Regression: No Tests

**Source**: `bash run_tests.sh`

```
tests failed (1) linear_regression. No tests collected.
```

**Assessment**: `apps/linear_regression/test/test_config.py` exists but contains no test functions - only imports and path setup. Either add actual tests or remove the file to avoid false failures.

---

## Template

```markdown
### [Module]: [Brief Title]

**Source**: [command/file/observation context]
**Date**: YYYY-MM-DD

[Description or error output]

**Assessment**: [Initial thoughts on cause/fix/priority]
**Status**: [blank | TRIAGED: XX-001 | WONTFIX: reason | RESOLVED]
```

---

## Triaged Items

| Date | Observation | Outcome |
|------|-------------|---------|
| — | — | — |

---

*Last updated: 2026-01-27*
