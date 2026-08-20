# ML-021: `make_forecast.py` exits 0 after writing no forecasts to the API

**Status**: Draft (2026-08-20; revised same day after two independent out-of-loop reviews)
**Module**: `apps/machine_learning` (`make_forecast.py`, `scr/utils_ml_forecast.py`)
**Priority**: High — an operational ML run can report success on every layer
(`make_forecast.py` exit 0, `run_locally.sh` `PASS`) while writing **nothing** to
the database. Invisible until someone notices the dashboard is empty.
**Labels**: `ml`, `api`, `silent-failure`, `exit-code`
**Discovered**: 2026-08-20, building the operator runbook for "machine_learning
produces no forecasts" on a remote deployment.

The **machine_learning analogue of PP-051** (worked across five postprocessing
horizons, PRs #433/#434/#435/#436; PP-054/PP-055 still open). Same shape, untouched here.

---

## Defect

`make_forecast.py` is the **sole producer of new operational forecasts** and the
only caller of `write_pentad_forecast` / `write_decad_forecast` (`:860`, `:870`).
It is **not** the only writer to the API — see "Scope". It contains no `sys.exit`,
so absent a propagating exception it always exits 0.

Both writers swallow every failure mode of the API write:

```python
# make_forecast.py:167-175 (pentad; :218-226 is the identical decad path)
if SAPPHIRE_API_AVAILABLE:
    try:
        _write_ml_forecast_to_api(data_for_api, "pentad", MODEL_TO_USE)
        _check_ml_forecast_consistency(forecast_pentad, "pentad", MODEL_TO_USE)
    except Exception as e:
        logger.error(f"Failed to write pentad forecast to API: {e}")
        # Don't fail the whole process - continue to CSV
```

### Leak 1 — silent `False` returns to a caller that ignores the return

`_write_ml_forecast_to_api` returns `bool` and returns `False` without raising at
four points. **Three are reachable; the fourth is bypassed.**

| Line | Condition | Level | Visible? | Reachable? |
|---|---|---|---|---|
| `:739` (cond `:737`) | `sapphire-api-client` not installed | `WARNING` | never emitted | **No — bypassed** |
| `:745` (cond `:743`) | `SAPPHIRE_API_ENABLED` not `true` | `INFO` | **nowhere** | Yes |
| `:755` (cond `:753`) | `readiness_check()` false | `WARNING` | run log only | Yes |
| `:816` (cond `:814`) | **Record set empty** | `INFO` | **nowhere** | Yes |

Neither `write_pentad_forecast` (`:151-199`) nor `write_decad_forecast` inspects
the return — the calls at `:171` and `:222` are bare.

**`:739` is bypassed, not merely dead.** Every caller guards with
`if SAPPHIRE_API_AVAILABLE:` — `make_forecast.py:168`/`:219`,
`recalculate_nan_forecasts.py:430`, `fill_ml_gaps.py:378`,
`hindcast_ML_models.py:500`, `add_new_station.py:302`/`:343`,
`initialize_ml_tool.py:166`/`:187`. **A missing client therefore produces total
silence** — it is a *third* completely invisible cause, not a diagnosable one.
Any fix must either always call the helper or emit an explicit `NOT_AVAILABLE`
outcome in the outer branch.

**`:816` is the common case.** It fires when the record set is empty, which
happens when **`rivers_to_predict` is empty** — the loop domain at `:749`, built
by `get_rivers_to_predict` (`:480-492`) as the station-selection union
intersected with the per-model `== True` column.

> **Do not confuse this with `codes_to_use`.** That three-input intersection
> (`:613`) drives only the PET/daylight enrichment loop at `:634` and never
> filters the forecast loop. A short `codes_to_use` produces `flag=1`/`flag=2` or
> NaN rows that **are** written — degraded data, not zero rows. An earlier draft
> had this backwards.

**Success is louder than failure.** `:810-812` is a bare `print()`, captured by
`run_in_venv`'s `2>&1 | tee` (`run_locally.sh:456-459`), while the empty-set and
disabled cases are silent. **Caveat for anyone building a check on it:** the
helper prints success and returns `True` even when `write_forecasts()` returns a
count of **zero** (`:805-813`), and the count is station-*days*, not stations.

### Leak 2 — the `except Exception` in the caller

Attempted-and-rejected writes are caught, logged at ERROR, and execution
continues to CSV. Still exit 0. Unlike the skip sites this one *does* reach both
logs (it is on the `make_ml_forecast` logger, `:174`/`:225`).

### Where evidence lands — and one myth to retire

`make_forecast.py:90-96` builds a `console_handler` and never attaches it (`:96`
commented out). **That does not silence the module.** `make_ml_forecast` sets its
own level (`:93` `DEBUG`), so root's `WARNING` level never gates it, and root's
`StreamHandler` (`NOTSET`, from `setup_library:52-54`) emits it. ML records reach
stderr → `apps/logs/run_locally_*.log`. **An empty ML section in the run log IS
evidence ML never started.**

> **Do not "fix" `:96` by attaching the handler.** Propagation already delivers
> these records to root's stream handler; attaching a second one with propagation
> still enabled would **duplicate** all stderr/run-log output. If the handler is
> ever attached, `logger.propagate = False` must be set in the same change.
> Better: leave it and add a comment saying propagation is deliberate.

Destination map — needed by anyone writing a diagnostic or a test:

| Emitter | `machine_learning/logs/log` | `apps/logs/run_locally_*.log` |
|---|---|---|
| `make_ml_forecast` logger | yes (file handler) | yes (propagation) |
| `scr.utils_ml_forecast` — `:745`, `:755`, `:816` | **no** | only if ≥ `WARNING` |
| `print` at `:810` | no | **yes** |

The rotating file handler is attached to the **named** logger only
(`make_forecast.py:92-95`; same in `recalculate_nan_forecasts.py:35-38`,
`fill_ml_gaps.py:36-39`). No ML script attaches a handler to root, so the
readiness WARNING reaches the **run log only**. Note `logs/log` is **cumulative
across runs** — a match there may be stale.

### The root-level cap, and when it lifts

`scr/utils_ml_forecast.py:38` runs `logging.getLogger().setLevel(logging.WARNING)`
at import; `:42` calls `basicConfig(level=WARNING)`; `:43` takes
`getLogger(__name__)` with no level.

> **Implementer note — the INFRA-029 interaction is conditional.**
> `make_forecast.py` imports `scr.utils_ml_forecast` (`:108`) before
> `setup_library` (`:129`), so utils caps root **first**, and `setup_library:44`'s
> `basicConfig` is then a **no-op** (root already has a handler).
> `setup_library:52-54` replaces root's handlers but never its level.
>
> | INFRA-029 fix form | root level after | `:745`/`:816` visible? |
> |---|---|---|
> | none (today) | WARNING | no |
> | `setup_library:44` → `basicConfig(level=INFO)` | WARNING | **no** (no-op) |
> | `basicConfig(force=True)` or `getLogger().setLevel(INFO)` | INFO | **yes** |
>
> **Do not rely on log visibility either way** — prefer step 1.

### Amplifier — and why it constrains the fix

`run_machine_learning` (`run_locally.sh:541-549`) loops `ML_MODELS × ML_SCRIPTS`
with `break 2` at `:547`; `recalculate_nan_forecasts.py` runs before
`make_forecast.py` (`:141-145`). A failure for `TFT` prevents the operational
writer for **all three** models.

> **This is a hard constraint on the proposed fix, not just context.** If
> `make_forecast.py` starts exiting non-zero on an API failure, then under
> today's orchestration a first-model API outage would stop the remaining models
> from computing **or writing their CSV fallback** — strictly worse than today,
> where all three still run. Any fix must keep all models and their CSV writes
> running, and surface failure only after the loop.

---

## An existing precedent to extend, not re-derive

`recalculate_nan_forecasts.py:429-459` already captures the bool into
`api_write_ok`, warns on `False`, and logs `"API write unsuccessful; data
persisted only in CSV: %s"`, with tests at
`apps/machine_learning/test/test_recalculate_nan_api_write.py`. It still exits 0 —
the reporting half is done, the exit-code half is not. Extend this shape.

---

## Reproduction

1. Stop the **postprocessing** service only, leaving preprocessing available (a
   globally unreachable URL makes the API-first discharge/meteo reads raise
   first, which is a different failure).
2. ```bash
   cd apps && SAPPHIRE_PREDICTION_MODE=DECAD \
     ieasyhydroforecast_env_file_path="$ENVFILE" \
     bash run_locally.sh machine_learning
   ```
   The env file is required — `run_in_venv` forwards it empty when unset
   (`run_locally.sh:442`) and `sl.load_environment()` (`:512`) fails first.
3. Observe: `make_forecast.py` exits 0 and the module records `PASS`, the
   `decad_<MODEL>_forecast.csv` archive updates, and no database rows appear. The
   only trace is one WARNING in `apps/logs/run_locally_*.log`.
   **Assert on the module's status, not the runner's** — single-module runs also
   invoke validation, which records its own readiness failure and makes
   `print_summary` force a final exit 1 for a different reason.

---

## Proposed direction (needs owner sign-off — do not implement from this draft)

1. `write_pentad_forecast` / `write_decad_forecast` return an explicit outcome —
   `WROTE(n)` / `DISABLED` / `NOT_AVAILABLE` / `NOTHING_TO_WRITE` / `FAILED` —
   instead of `None`. **Prefer this to any log-visibility fix**: a return value
   cannot be suppressed by logger configuration.
2. Aggregate outcomes **after** the model loop and fail once at the end, so all
   models and their CSV fallbacks still run (see the `break 2` constraint).
3. Decide what `NOTHING_TO_WRITE` should mean. It is currently indistinguishable
   from success and is the most common real cause. An empty `rivers_to_predict`
   is arguably a configuration error worth failing on — an owner decision.
4. Classify `DISABLED` **before** `NOT_AVAILABLE`, so a deliberate CSV-only
   deployment without the client installed is not reported as a failure.
5. Optionally raise `:745`/`:816` to WARNING — secondary to step 1, and per the
   table above not something an INFRA-029 fix can be assumed to deliver.

**Sequencing caution.** The naive fix can be worse than the bug. This is
*inferred by analogy*, not quoted, from LR-011 (`:129` lists
`SAPPHIRE_API_ENABLED=false` as a reproduction condition; `:154-161` folds the
bool into `api_ok`; `:141-143` gives LR-007's contract that `api_ok=False` →
`sys.exit(1)`). **That is the LR consumer's behaviour — ML's callers currently
ignore `False`**, and LR-011 `:144-148`/`:170-175` leave the abort-vs-warn policy
explicitly **open**. The primary ML sequencing constraint is its own `break 2`,
not LR.

`SAPPHIRE_API_ENABLED=false` is a genuinely supported mode:
`forecast_library.py:2780-2794` is a hard switch to CSV with no fallback on error.

---

## Scope boundaries

- **`_write_ml_forecast_to_api` has six callers.** Besides `make_forecast.py`:
  `recalculate_nan_forecasts.py:434`, `fill_ml_gaps.py:381`,
  `hindcast_ML_models.py:502`, `add_new_station.py:304`/`:345`,
  `initialize_ml_tool.py:168`/`:189`. Changing its **signature or semantics
  touches all six** — prefer changing the two `write_*_forecast` wrappers.
  `recalculate_nan_forecasts.py` and `fill_ml_gaps.py` are **repair** writers and
  run operationally; keep that distinction, it affects Step 3 verification in the
  runbook.
- **Do not** change forecast content, computation, or CSV archive format.
- **Do not** fold in ML-016 or INFRA-030.
- `sapphire/services/` is out of scope.

---

## Acceptance criteria

1. Every outcome except `DISABLED` — and only when the required CSV write
   succeeded — is treated as a failure and reported with its cause.
2. `SAPPHIRE_API_ENABLED=false` exits 0 when the CSV write succeeds; tests cover
   disabled+client-missing, disabled+CSV-success, and disabled+CSV-failure
   independently. (Existing code intentionally swallows archive-CSV failures;
   an unscoped "disabled always exits 0" would pin that silence.)
3. Tests cover the three reachable skip sites (`:745`, `:755`, `:816`) and the
   `except Exception` path. `:739` is covered as a **guarded** branch, not a
   production path.
4. A test pins that one model's API failure does not prevent the other two models
   from computing and writing CSV.
5. The chosen semantics for `NOTHING_TO_WRITE` is recorded here before implementation.
6. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero
   unexpected skips.

---

## Related

| ID | Relation |
|---|---|
| PP-051 | Same defect across five postprocessing horizons — reuse its resolved shape |
| PP-054 / PP-055 | Still open; do not diverge from what they settle |
| LR-011 / LR-010 | Analogy only; the abort-vs-warn policy is **open** there |
| INFRA-029 | Root cap — see the conditional table before assuming its fix helps |
| ML-016 | Bare target crashes on empty `SAPPHIRE_PREDICTION_MODE` |
| — | [ML debugging runbook](../../prod/ml_no_forecasts_debug_runbook.md) |
