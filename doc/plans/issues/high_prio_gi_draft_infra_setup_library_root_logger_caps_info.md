## `setup_library`'s import-time `basicConfig` caps the root logger at WARNING — 13 entry points log INFO into the void (INFRA-029)

**Status**: Draft (2026-08-18)
**Module**: `apps/iEasyHydroForecast/setup_library.py` (cause); consumers in
`apps/postprocessing_forecasts`, `apps/preprocessing_gateway`, `apps/preprocessing_runoff`,
`apps/preprocessing_station_forcing`, `apps/reset_forecast_run_date`
**Priority**: **High** — not a data defect, a *diagnosis* defect. In the 13 affected entry points
every operational record emitted at INFO ("wrote N rows", "no gaps found", "skipping X") is
discarded before it reaches either the console or the module's own log file. Owner to confirm the
rating.
**Labels**: `infra`, `logging`, `observability`, `cross-module`
**Found**: 2026-08-18, local kghm (kyg) end-to-end review on `maxat_sapphire_2` @ `a304ffb0`.
**Related**: **INFRA-024** (failure *causes* logged at DEBUG are unattributable — same blind
spot, one level down), **PP-045** (its backfill CLI is one of the affected entry points — see
"Consequence for PP-045"), PREPG-009 / PP-051 / PP-054 (silent-success family: those modules
misreport an outcome; this one reports nothing at all).

---

## Observation

`bash apps/run_locally.sh maintenance:postprocessing_long_term` (kghm, 2026-08-18 17:42) ran
for **58 seconds**, exited 0, and the only module output in the pipeline log was a pandas
`FutureWarning`:

```
[17:42:32] [INFO]   Running: postprocessing_forecasts/postprocessing_maintenance_long_term.py
.../src/data_reader.py:729: FutureWarning: The behavior of DataFrame concatenation with ...
[17:43:30] [OK] postprocessing long-term maintenance completed in 58s
```

The module is not silent by construction — `postprocessing_maintenance_long_term.py` contains
**38** `logger.info(...)` calls, including "No monthly ensemble gaps found. Nothing to fill.",
"Monthly gap-fill results saved successfully." and a per-horizon banner. None of them was
emitted. Its own rotating log file, `apps/postprocessing_forecasts/logs/log_maintenance_long_term`,
has **zero** lines from 2026-08-18 (last written 2026-08-17, and those two lines are both
`WARNING`).

The same day, on the same checkout:

| Log file | Lines dated 2026-08-18 | INFO | WARNING | ERROR |
|---|---|---|---|---|
| `postprocessing_forecasts/logs/log_maintenance` | 282 | **0** | 280 | 2 |
| `postprocessing_forecasts/logs/log_recalc` | 30,026 | **0** | 30,024 | 2 |
| `postprocessing_forecasts/logs/log_maintenance_long_term` | 0 | **0** | 0 | 0 |

A 4.2 MB log with 30,024 warnings and not one INFO line is the signature.

## Mechanism — PROVEN by direct execution

`logging.basicConfig()` **does nothing if the root logger already has handlers**, unless
`force=True` is passed. `setup_library` configures the root logger at **import** time:

```python
# apps/iEasyHydroForecast/setup_library.py:44-54
logging.basicConfig(level=logging.WARNING)      # sets root level = WARNING
...
logger = logging.getLogger()
logger.handlers = []
logger.addHandler(console_handler)               # root now HAS a handler
```

Every affected entry point imports `setup_library` **before** configuring its own logging, so
its subsequent `logging.basicConfig(level=logging.DEBUG)` is a no-op and the root level stays
at WARNING. The module then replaces the root handlers with its own file + console handlers —
so the handlers are correct, the formatter is correct, the file is created, and every
`logger.info()` is dropped by the *level* check before any handler sees it.

Reproduced directly (read-only, `postprocessing_forecasts/.venv`):

```
$ python -c "import logging, setup_library; logging.basicConfig(level=logging.DEBUG); ..."
root level AFTER import setup_library:   WARNING
handlers:                                [<StreamHandler <stderr>>]
root level after basicConfig(DEBUG):     WARNING
isEnabledFor(INFO):                      False
```

and end-to-end on a real entry point:

```
$ python -c "import backfill_period_forecasts; ..."
root level:                              WARNING
root handlers:                           [TimedRotatingFileHandler .../logs/log_operational,
                                          StreamHandler <stderr>]
module logger effective level:           WARNING
module logger isEnabledFor(INFO):        False
```

## Scope — 13 of the 19 entry points that follow this pattern are affected

Nineteen entry points call `logging.basicConfig(level=DEBUG|INFO)` **after** importing
`setup_library`, and **none** passes `force=True`, so in all 19 that call is a no-op. Six of them
survive anyway because they follow it with an explicit `logger.setLevel(...)` on the root logger;
the other 13 do not:

| Module | Affected entry points | Suppressed `logger.info` calls |
|---|---|---|
| `postprocessing_forecasts` | `recalculate_skill_metrics.py` (43), `postprocessing_maintenance_long_term.py` (38), `postprocessing_maintenance.py` (26), `postprocessing_operational_long_term.py` (26), `postprocessing_operational.py` (23), `backfill_period_forecasts.py` (4) | **160** |
| `preprocessing_runoff` | `sync_long_horizon_hydrograph.py` (10), `backfill_discharge_aggregation.py` (9), `sync_monthly_norms.py` (8), `sync_short_horizon_hydrograph.py` (5) | **32** |
| `preprocessing_gateway` | `backfill_new_stations.py` (16) — the only one of the module's six without a `setLevel` | **16** |
| `preprocessing_station_forcing`, `reset_forecast_run_date` | `preprocessing_station_forcing.py`, `rerun_forecast.py` | 0 (same structure, no INFO calls today) |

**Total suppressed: 208 `logger.info` call sites**, of which `postprocessing_forecasts` is 77%.
That module is where the pipeline's operational record is supposed to live, which is why the
practical impact is concentrated there even though the defect is repo-wide.

**The six that escape, and why that is the actual finding.** `preprocessing_gateway`'s
`Quantile_Mapping_OP.py:131`, `snow_data_renalysis.py:84`, `snow_data_operational.py:74`,
`extend_era5_reanalysis.py:75` and `get_era5_reanalysis_data.py:73`, plus
`linear_regression.py:157`, each end the same copy-pasted 20-line logging block with
`logger.setLevel(logging.INFO|DEBUG)` on the **root** logger — which does take effect, because an
explicit `setLevel` is not subject to `basicConfig`'s already-configured check. The 13 affected
files are running a copy of that same block **with the last line missing**. Nothing marks that
line as load-bearing, nothing tests for it, and its absence is invisible until someone asks why a
module produced no log. That is the defect to fix — not 13 individual omissions.

**Patterns that work, for the fix to copy.** `linear_regression.py` is one of the 19 and escapes
as described above; the other three below are **not** members of the 19 — they never call
`basicConfig` after importing `setup_library` — and are listed because they show what a correct
configuration looks like:

- `apps/preprocessing_runoff/preprocessing_runoff.py:76-99` — reads a configured level
  (`src/config.py::get_log_level`, default INFO), sets `logger.setLevel(logging.DEBUG)` on the
  root and filters **per handler** (`file_handler.setLevel(DEBUG)`, `console_handler.setLevel(log_level)`).
  This is the reference implementation and the pattern to generalise.
- `apps/linear_regression/linear_regression.py:157` — explicit `logger.setLevel(logging.DEBUG)`
  after its own no-op `basicConfig` at `:134` (it imports `setup_library` at `:126`). It also does
  `logger.info = print` at `:154`, which routes INFO to stdout **bypassing the file handler**, so
  LR's console output and its log file disagree by construction. The `setLevel` is the pattern to
  keep; the `logger.info = print` is a workaround to revisit, not to copy.
- `apps/machine_learning/*.py` — use **named** loggers with explicit `logger.setLevel(DEBUG)`
  (e.g. `recalculate_nan_forecasts.py:36`, `fill_ml_gaps.py:37`), which is why ML output was
  visible throughout the 2026-08-18 review while postprocessing output was not.
- `apps/long_term_forecasting/__init__.py:18` — explicit `setLevel`.

**Why this went unnoticed for so long.** The modules that still appear to talk use `print`, not
logging: `sync_long_horizon_hydrograph.py:772` prints its whole `LONG-HORIZON RUN SUMMARY` block,
`recalculate_nan_forecasts.py:463` prints its closing line, and `linear_regression.py:154` rebinds
`logger.info = print`. So an operator watching a pipeline run sees output and infers logging works
— while the modules that use the logger properly, such as `postprocessing_maintenance_long_term.py`,
are the ones that go dark. Output volume is not evidence that logging is configured.

**A second landmine in the same family, not fixed by fixing this one:**
`apps/machine_learning/scr/BaseDartsDLPredictor.py:16` and `apps/machine_learning/scr/utils_ml_forecast.py:38`
call `logging.getLogger().setLevel(logging.WARNING)` at import — an import-time mutation of the
*root* logger by a library module. Any consumer that imports them inherits it regardless of its
own configuration. Record it; do not fix it under this issue without checking why it is there.

## Consequence for PP-045

`backfill_period_forecasts.py` emits **all** of its operator-facing output through
`logger.info` — `:237` (failure-mode notice), `:247` (plan), `:273` (per-year "ok"), `:290`
("Backfill finished successfully.") — and the dry-run summary the runbook tells operators to
read is `logger.info` in `postprocessing_operational.py:209-214`. Under the current
configuration **none of it is emitted**, as the second reproduction above shows for exactly
that module. So:

- `doc/prod/backfill_period_forecasts_runbook.md`'s dry-run step cannot work as written —
  the operator sees no summary line and cannot distinguish "nothing to write" from "did not run".
- This does **not** change what the CLI *writes*; exit codes and API writes are unaffected.

A correction has been added to the runbook and to PP-045's issue pointing here.

## Why it matters beyond convenience

- **Post-hoc diagnosis is impossible.** The 2026-08-18 LT maintenance run did 58 seconds of
  work and left no record of what it examined, filled or skipped. The monthly coverage it was
  supposed to explain — `EM` present for 2026-06 but absent for 2026-05 and 2026-07, and
  `Naive Mean` additionally absent for 2026-07 — is exactly the question its own INFO lines
  would have answered ("No monthly ensemble gaps found" vs "Saved N rows"). **Re-check that
  coverage question once this is fixed**; it is currently undecidable from the run itself, and
  both readings (a legitimate EM admission gate vs a gap-filler that did nothing and reported
  success) remain open.
- **It compounds INFRA-024.** That issue records failure *causes* being logged at DEBUG; this
  one removes INFO as well, leaving WARNING as the lowest visible level across five modules.
- **Cron and CI see nothing.** A scheduled run's log is the only artefact; a WARNING-only log
  cannot show that the run did the right thing.

---

## Proposed direction (owner to choose)

- **(a) Remove the logging configuration from `setup_library` entirely.** A library should not
  configure the root logger at import. Highest blast radius: any script that relied on
  `setup_library` for its handlers loses them, so this must be paired with (c).
- **(b) Minimal: pass `force=True`** in each affected entry point's `basicConfig` call — or add
  the missing `logger.setLevel(...)` line the six escaping files already have, which is the smaller
  diff and matches existing in-repo practice. Either is one line per file with no behaviour change
  elsewhere. Both leave the import-time side effect in place for the next module to trip over.
- **(c) A shared `configure_logging(name, level=None)` helper** in `iEasyHydroForecast`, modelled
  on `preprocessing_runoff.py:76-99` (env/config-driven level, DEBUG to file, configured level to
  console), called explicitly by each entry point. Removes the duplication of the same 20-line
  block across 19 files.

Recommended: **(b) now, (a)+(c) as the real fix** — (b) restores observability in one small,
reviewable patch without touching import-time behaviour; (a)+(c) removes the trap.

**Before raising levels, size the output.** `log_recalc` already reaches 4.2 MB per run at
WARNING only, and `apps/*/logs/` rotation is **per run, not per day**, so 30 backups can be a
single afternoon. Enabling INFO across `recalculate_skill_metrics` without checking volume is a
disk-space change as well as an observability change.

## Testing

- [ ] Unit test in `apps/iEasyHydroForecast/tests/`: after importing `setup_library`, a fresh
      `logging.basicConfig(level=logging.INFO)` **does** take effect (post-fix), asserting
      `logging.getLogger().isEnabledFor(logging.INFO)`.
- [ ] Per-module smoke test: run each affected entry point with a no-op configuration and assert
      its log file contains its own start banner. This is the test that would have caught the
      empty `log_maintenance_long_term`.
- [ ] Regression guard: a test that fails if any `apps/*/[a-z]*.py` calls
      `logging.basicConfig(...)` **after** importing `setup_library` without either `force=True`
      or a following explicit `setLevel` — i.e. one that would have flagged all 13 today and none
      of the 6 that escape.
- [ ] Confirm no test asserts on the current (suppressed) log contents before changing levels.

## Out of scope

- Changing the *level* of individual messages (INFO → WARNING and so on).
- The `print()`-based output in `machine_learning` and `linear_regression`.
- `machine_learning/scr/*`'s root-level mutation — recorded above, decided separately.
- Log rotation policy (see the `apps/logs` rotation note in PP-045's runbook).

## Acceptance criteria

- [ ] An affected entry point's INFO lines appear in both its console output and its own log file.
- [ ] `apps/postprocessing_forecasts/logs/log_maintenance_long_term` contains a start banner and a
      per-horizon outcome line after a maintenance run.
- [ ] `backfill_period_forecasts.py --dry-run` prints its summary line, and the runbook's
      correction is removed once it does.
- [ ] The reference pattern is documented in one place, not copied into 19 files.
- [ ] `SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.
