# INFRA-059: CI only runs pytest for two of nine `apps/` modules; the rest only verify imports

**Status**: Draft (2026-09-26)
**Module**: infra (`.github/workflows/build_test.yml`, `.github/workflows/deploy_local.yml`)
**Priority**: High. Merge approvals for every other module currently rely on a local
`SAPPHIRE_TEST_ENV=True bash run_tests.sh <module>` run that CI does not itself verify happened or
passed.

## Problem

Every module job in both workflows installs its module's dependencies with `uv sync --all-extras`,
then either runs pytest or just imports a handful of packages. Verified directly against both files
on trunk (`maxat_sapphire_2`):

**`build_test.yml`** — job name, step name, and whether it runs pytest:
- `test_ieasyhydroforecast` (`:33`): **pytest** — `Test with pytest` step at `:54-57`
  (`SAPPHIRE_TEST_ENV=True iEasyHydroForecast/.venv/bin/python -m pytest iEasyHydroForecast/tests/ -v`).
- `test_pipeline` (`:125`): import only — `Verify imports` at `:142-144` (`luigi`, `docker`, `yaml`,
  `requests`, `tenacity`, `dotenv`).
- `test_preprocessing_runoff` (`:192`): **pytest** — `Test with pytest` at `:217-220`
  (`SAPPHIRE_TEST_ENV=True preprocessing_runoff/.venv/bin/python -m pytest preprocessing_runoff/test/ -v`).
- `test_preprocessing_gateway` (`:242`): import only — `Verify imports` at `:263`.
- `test_machine_learning` (`:291`): import only — `Verify imports` at `:308`.
- `test_long_term_forecasting` (`:349`): import only — `Verify imports` at `:366`.
- `test_dashboard` (`:407`): import only — `Verify imports` at `:424`.
- `test_postprocessing` (`:452`): import only — `Verify imports` at `:452-472`
  (`import pandas; import numpy; import openpyxl`).
- `test_linear_regression` (`:497`): import only — `Verify imports` at `:514`.

**`deploy_local.yml`** — same pattern, confirmed independently:
- `test_ieasyhydroforecast_py312` (`:25`): **pytest** at `:46-49`.
- `test_pipeline_py312` (`:96`): import only.
- `test_preprocessing_runoff_py312` (`:179`): **pytest** at `:204-207`.
- `test_preprocessing_gateway_py312` (`:242`): import only, `:263`.
- `test_machine_learning_py312` (`:300`): import only, `:317`.
- `test_long_term_forecasting_py312` (`:323`): import only, `:340`.
- `test_dashboard_py312` (`:436`): import only, `:453`.
- `test_postprocessing_py312` (`:490`): import only, `:507`.
- `test_linear_regression_py312` (`:544`): import only, `:561`
  (`import pandas; import numpy; import docker; from ieasyhydro_sdk.sdk import ...`).

So **`forecast_dashboard`, `postprocessing_forecasts`, `linear_regression`, `machine_learning`,
`long_term_forecasting`, `pipeline` and `preprocessing_gateway`** never run their test suite in CI on
either workflow — only `iEasyHydroForecast` and `preprocessing_runoff` do. A PR that breaks any test
in the other seven modules' suites (all of which exist and pass locally) shows green CI on both
required workflows.

## Related

- **INFRA-003** ("Add pytest-cov with threshold enforcement to CI") presupposes tests already run in
  CI; it cannot be implemented module-by-module until this gap closes for that module.
- `service:<name>` jobs (the FastAPI services under `sapphire/services/`, colleague-owned) are out of
  scope here — this issue is about the `apps/` module jobs only.

## Proposed fix

For each of the seven import-only jobs, replace (or add alongside) the `Verify imports` step with a
pytest step matching the two working examples' shape:

```yaml
- name: Test with pytest
  working-directory: ./apps
  run: |
    SAPPHIRE_TEST_ENV=True <module>/.venv/bin/python -m pytest <module>/<tests-dir>/ -v
```

using each module's actual tests directory (`tests/` for most, `test/` for `preprocessing_runoff`;
verify each module's own directory name before writing its step — do not assume `tests/`
uniformly). Keep jobs independent and parallel (no new `needs:` edges) — the point is coverage per
job, not sequencing.

`forecast_dashboard` needs the existing env-gated skip handling respected (its Playwright/Chromium
tests are gated by `importorskip`/`skipif`, per CLAUDE.md's Zero Skips Policy exception list) — do not
force those to run in CI if they are not already runnable there; a plain `pytest tests/ -v` already
skips them correctly, matching what `run_tests.sh forecast_dashboard` does locally.

## Acceptance criteria

- Every module job in both `build_test.yml` and `deploy_local.yml` runs its module's pytest suite
  (either directly, matching the two existing examples, or via `bash run_tests.sh <module>`) and the
  job **fails the check** on a test failure — prove this with one deliberately failing test added
  temporarily to a currently-import-only module, pushed on a throwaway branch, showing the job go red,
  then removed before merge.
- No job's runtime regresses to the point of blocking normal PR turnaround (parallel jobs, no new
  cross-job `needs:` dependencies).
- `git diff --stat` is limited to the two workflow files.

## Out of scope

- `service:<name>` CI jobs (colleague-owned, `sapphire/services/`).
- Coverage thresholds (INFRA-003, blocked on this).
- Any change to what `run_tests.sh` itself considers a valid skip.
