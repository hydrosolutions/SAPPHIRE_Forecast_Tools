# Migration CSV Fixtures

This directory holds CSV/JSON sentinel fixtures used by the update-time migration
toolkit tests under `apps/iEasyHydroForecast/tests/test_migration_*.py`.

## Sentinel-code policy

All fixture files in this directory MUST use sentinel station codes:

- `19999` for general runoff/hydrograph/meteo/snow station codes, and for ML
  backfill canary fixtures; use `00001`/`00002` when a second distinct sentinel
  is needed.
- `00000` through `00009` for HRU (hydrological response unit) sentinels.

Any other 5-digit code is treated as a real station code and is forbidden. The
test `test_migration_fixture_guard.py` enforces this with a grep walk over every
file in this directory. The policy is: no real station codes, ever.

## No test files allowed here

No `test_*.py` or `conftest.py` files may live under this directory. Pytest's
default discovery would collect them as tests, producing spurious failures and
confusing test reports. Put fixture-loading helpers in the parent
`apps/iEasyHydroForecast/tests/conftest.py` if needed (and re-verify the conftest
does not break unrelated tests).

## What goes here

CSV files representing tiny snippets of source data (runoff exports, hydrograph
exports, snow archives, manifest pairs) used to exercise the migration helper
modules. Keep each fixture small (under 1 KiB if possible) and use realistic-
looking dates / float values - just never real station codes or real discharge
values.

Per-phase subdirectories are allowed (e.g. `runoff_day/`, `hydrograph_day/`) as
P1/P2/P3 ship their tests. The guard test walks recursively.
