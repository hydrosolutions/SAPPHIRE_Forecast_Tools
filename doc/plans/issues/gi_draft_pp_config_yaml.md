# Add config.yaml to postprocessing_forecasts module

**Status**: Draft
**Module**: postprocessing_forecasts
**Priority**: Low
**Labels**: `config`, `deduplication`, `maintenance-mode`

---

## Summary

The `POSTPROCESSING_GAPFILL_WINDOW_DAYS` setting has its default value (`7`) hardcoded in three separate places. Add a `config.yaml` to the postprocessing_forecasts module following the established `preprocessing_runoff` pattern, so the default lives in one place.

## Context

The gap-fill lookback window is consumed only by `postprocessing_maintenance.py` but its default is duplicated:

| File | Line | Current code |
|------|------|-------------|
| `apps/postprocessing_forecasts/postprocessing_maintenance.py` | 67 | `os.getenv('POSTPROCESSING_GAPFILL_WINDOW_DAYS', '7')` |
| `bin/daily_postprc_maintenance.sh` | 119 | `POSTPROCESSING_GAPFILL_WINDOW_DAYS=${POSTPROCESSING_GAPFILL_WINDOW_DAYS:-7}` |
| `apps/run_locally.sh` | 502 | `POSTPROCESSING_GAPFILL_WINDOW_DAYS=${POSTPROCESSING_GAPFILL_WINDOW_DAYS:-7}` |

The `preprocessing_runoff` module already has a `config.yaml` + `src/config.py` with the same pattern (env overrides yaml overrides defaults). This issue applies the same approach to postprocessing_forecasts.

## Desired Outcome

1. Single source of truth for the gap-fill lookback default
2. Consistent config pattern across modules
3. Shell scripts no longer need to know the default value

---

## Implementation Plan

### Step 1: Create `apps/postprocessing_forecasts/config.yaml`

```yaml
# Configuration for the postprocessing_forecasts module
#
# Values can be overridden via environment variables.

maintenance:
  # Number of days to look back when filling ensemble gaps
  # Override with: POSTPROCESSING_GAPFILL_WINDOW_DAYS
  gapfill_window_days: 7
```

Keep it minimal — only the settings that exist today. Other settings can be added later as needed.

### Step 2: Create `apps/postprocessing_forecasts/src/config.py`

Follow the `preprocessing_runoff/src/config.py` pattern:
- `DEFAULTS` dict with fallback values
- `load_config()` reads yaml, merges with defaults
- `_apply_env_overrides()` applies `POSTPROCESSING_GAPFILL_WINDOW_DAYS`
- `get_gapfill_window_days() -> int` convenience accessor

Simplified version (postprocessing_forecasts has fewer settings than preprocessing_runoff):

```python
"""Configuration loader for the postprocessing_forecasts module."""

import os
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

DEFAULTS = {
    'maintenance': {
        'gapfill_window_days': 7,
    },
}


def _get_config_path() -> Path:
    src_dir = Path(__file__).parent
    module_dir = src_dir.parent
    return module_dir / 'config.yaml'


def _apply_env_overrides(config: dict) -> dict:
    env_val = os.getenv('POSTPROCESSING_GAPFILL_WINDOW_DAYS')
    if env_val is not None:
        try:
            config['maintenance']['gapfill_window_days'] = int(env_val)
        except ValueError:
            logger.warning(
                f"Invalid POSTPROCESSING_GAPFILL_WINDOW_DAYS: {env_val}, "
                f"using default: {config['maintenance']['gapfill_window_days']}"
            )
    return config


def load_config() -> dict:
    config = {'maintenance': DEFAULTS['maintenance'].copy()}
    config_path = _get_config_path()

    if config_path.exists():
        try:
            with open(config_path, 'r') as f:
                file_config = yaml.safe_load(f)
            if file_config and 'maintenance' in file_config:
                config['maintenance'].update(file_config['maintenance'])
        except Exception as e:
            logger.warning(f"Failed to load {config_path}: {e}")

    return _apply_env_overrides(config)


def get_gapfill_window_days() -> int:
    return load_config()['maintenance']['gapfill_window_days']
```

### Step 3: Update `postprocessing_maintenance.py`

Replace:
```python
lookback = int(
    os.getenv('POSTPROCESSING_GAPFILL_WINDOW_DAYS', '7')
)
```

With:
```python
from src.config import get_gapfill_window_days
lookback = get_gapfill_window_days()
```

### Step 4: Update shell scripts

Both shell scripts currently pass the env var with a hardcoded default. After this change, they should pass the env var **only if it's set**, letting the Python config handle the default.

**`bin/daily_postprc_maintenance.sh` line 119** — change:
```bash
-e POSTPROCESSING_GAPFILL_WINDOW_DAYS=${POSTPROCESSING_GAPFILL_WINDOW_DAYS:-7} \
```
to:
```bash
${POSTPROCESSING_GAPFILL_WINDOW_DAYS:+-e POSTPROCESSING_GAPFILL_WINDOW_DAYS=${POSTPROCESSING_GAPFILL_WINDOW_DAYS}} \
```

This uses the `${var:+...}` syntax: only pass `-e ...` if the variable is set. If unset, the Python config.yaml default (7) applies.

**`apps/run_locally.sh` line 502** — change:
```bash
"POSTPROCESSING_GAPFILL_WINDOW_DAYS=${POSTPROCESSING_GAPFILL_WINDOW_DAYS:-7}"
```
to:
```bash
${POSTPROCESSING_GAPFILL_WINDOW_DAYS:+"POSTPROCESSING_GAPFILL_WINDOW_DAYS=${POSTPROCESSING_GAPFILL_WINDOW_DAYS}"}
```

Same pattern: only include the env var in the array if it's set externally.

### Step 5: Add tests

Add tests in `apps/postprocessing_forecasts/tests/test_config.py`:

1. `test_default_gapfill_window_days` — no env var, no yaml → returns 7
2. `test_yaml_overrides_default` — yaml has 14, no env var → returns 14
3. `test_env_overrides_yaml` — yaml has 14, env has 21 → returns 21
4. `test_invalid_env_value_uses_default` — env has "abc" → returns 7 with warning
5. `test_missing_yaml_file_uses_defaults` — no config.yaml → returns 7

### Step 6: Verify

```bash
cd apps
SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
```

---

## Dependencies

- `pyyaml` must be available. Check `pyproject.toml` — it's likely already a transitive dependency.

## Risk

Low. The env var override ensures backward compatibility — existing deployments that set `POSTPROCESSING_GAPFILL_WINDOW_DAYS` in their environment will continue working identically. Only the "who provides the default" changes.

## Scope

This issue covers only `POSTPROCESSING_GAPFILL_WINDOW_DAYS`. Other postprocessing settings (thresholds, prediction mode) remain env-var-only for now — they can be migrated to config.yaml in follow-up work if desired.
