## An empty `apps/iEasyHydroForecast/iEasyHydroForecast/` shadows the package and kills every `validate_pipeline` run (INFRA-025)

**Status**: Draft (2026-08-14)
**Module**: `apps/iEasyHydroForecast` (packaging), `apps/validate_pipeline` (fallback), and
every module target in `apps/run_locally.sh` that calls per-module validation
**Priority**: **High** — while present, **100% of per-module and pipeline API validation is
dead**, and the pipeline still exits 0. Trivial to fix, easy to reintroduce.
**Labels**: `infra`, `packaging`, `validate_pipeline`, `tooling`
**Found**: 2026-08-14, local kghm review on `maxat_sapphire_2` @ `8e3fc1bc`, immediately
after `preprocessing_runoff` PASSed.
**Related**: INFRA-020 (ML validation matches zero checks and reports PASS on no evidence),
INFRA-021 / INFRA-022 (`validate_pipeline` long-term crash + false-FAIL). Same tool, and
this defect **hides all three** — the crash happens at import, before any check runs.

> **Provenance correction (2026-08-16).** The checkout moved from `maxat_sapphire_2` to
> `fix_lr010_lr011_write_contract` at **2026-08-14 16:00** (git reflog), so every run from the
> full-history recalc onward executed on that branch (now `849c8736`), **not** on trunk as the
> line above states. That branch's diff vs trunk touches only
> `apps/linear_regression/linear_regression.py`, `apps/iEasyHydroForecast/forecast_library.py`,
> their tests, and docs — **none of the files this issue concerns** — so the finding holds
> identically on trunk. Recorded for accuracy of the audit trail, not because the conclusion changes.

---

## Symptom

Every single-module target ends with a `validate_pipeline --module <name>` call that dies:

```
ModuleNotFoundError: No module named 'iEasyHydroForecast.long_term_horizon_resolver'
```

...twice — once for the `try`, once for the `except ModuleNotFoundError` fallback
(`validate_pipeline.py:36` and `:44`). `run_module_validation()` deliberately
`return 0`s so the pipeline is not aborted mid-run, so the **module target still exits 0**
and only a `FAIL` line in the validation summary marks it.

## Root cause — two layers

**Layer 1 (the trigger): a stray, empty, same-named subdirectory.**
`apps/iEasyHydroForecast/iEasyHydroForecast/` existed containing only
`tests/test_data/` — **zero files**, so git neither tracks nor reports it, and a fresh
clone does not have it.

The editable install's `.pth` puts the package's own *contents* directory on `sys.path`:

```
$ cat .venv/.../_editable_impl_ieasyhydroforecast.pth
/Users/…/apps/iEasyHydroForecast
```

With that on `sys.path`, `import iEasyHydroForecast` resolves to the **empty
subdirectory** as an implicit namespace package. The parent import *succeeds*, every
submodule import then fails, and — critically — the parent is now cached in `sys.modules`
with the wrong `__path__`, so `validate_pipeline`'s fallback (`sys.path.insert(0, apps/)`
then retry) **cannot work**: re-importing a submodule does not re-resolve a cached
parent's `__path__`.

**Layer 2 (the latent fragility): the editable install never exposed the package.**
Removing the stray directory changes the error to `No module named 'iEasyHydroForecast'`
— i.e. with the `.pth` alone the package is *not importable at all*, because the path
entry points at the directory that **contains** `__init__.py` and the modules, rather than
at its parent `apps/`. Everything works today only because callers `cd` into a module
directory and do `sys.path.insert(0, '..')`. Any script that does not follow that
convention cannot import `iEasyHydroForecast` from the venv.

So the stray directory did not break a working install — it converted a *silently
non-functional* install into a loudly broken one, while simultaneously disabling the
fallback that had been papering over it.

## How the directory gets created (so the fix sticks)

`apps/iEasyHydroForecast/pyproject.toml` documents:

```
# Use: cd apps && SAPPHIRE_TEST_ENV=True uv run --directory iEasyHydroForecast pytest iEasyHydroForecast/tests/
```

`--directory iEasyHydroForecast` makes the cwd `apps/iEasyHydroForecast`, so the trailing
`iEasyHydroForecast/tests/` resolves to **`apps/iEasyHydroForecast/iEasyHydroForecast/tests/`**
— exactly the shadowing path. Any test that materialises a `test_data` directory under its
own relative path recreates the shadow. **Fixing only the directory guarantees recurrence.**

## Verification performed

| Step | Result |
|---|---|
| `validate_pipeline --module preprocessing_runoff`, stray dir present | `ModuleNotFoundError` ×2, no checks run |
| Move stray dir aside, re-run identical command | **`VALIDATION SUMMARY: 5 passed, 0 failed, 0 warned, 5 skipped`** |
| `find … -type f` under the stray dir | `0` files |
| `git ls-files` / `git status` on it | untracked and unreported (empty dirs) |

## Proposed fix

1. Correct the documented pytest invocation in `apps/iEasyHydroForecast/pyproject.toml` to
   a path that cannot nest (`pytest tests/` from the package dir, matching the existing
   `testpaths = ["tests"]`).
2. Make the editable install expose the package properly, so `import iEasyHydroForecast`
   works from any cwd without the `sys.path.insert(0, '..')` convention. Decide explicitly
   whether that convention stays as the supported mechanism — if so, say so in the module
   README and drop the misleading editable dependency.
3. Make `validate_pipeline`'s fallback robust: insert the path **before** the first import,
   or purge `sys.modules['iEasyHydroForecast']` and `importlib.invalidate_caches()` inside
   the `except` branch. As written it cannot recover from the exact failure it is there to
   catch.
4. Consider a `.gitignore` entry for `apps/iEasyHydroForecast/iEasyHydroForecast/` so a
   recreated shadow is at least visible.

## Acceptance criteria

- With the stray directory recreated, `validate_pipeline --module <any>` still runs its
  checks (proves fix 3 independently of fix 1).
- `python -c "import iEasyHydroForecast.long_term_horizon_resolver"` succeeds from a cwd
  outside `apps/`, using a module venv (proves fix 2).
- The documented test command, run verbatim, does not create a nested package directory.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh` green.

## Contract not to break

- `run_module_validation()` intentionally returns 0 so a validation failure does not abort
  a pipeline mid-run. Do **not** change that as part of this fix — but note it is why this
  went unnoticed, and pair the fix with INFRA-020's "reports PASS on no evidence" concern.
- The `cd <module> && sys.path.insert(0, '..')` convention is load-bearing for every
  module entry point. Do not remove it before fix 2 is proven.
