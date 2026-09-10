# ML-021: `make_forecast.py` exits 0 after writing no forecasts to the API

**Status**: Review (2026-09-09 — implemented; see "Implemented 2026-09-09" below). Reviewed
2026-09-08 (third out-of-loop pass, trunk `ebe422fc`) and **unblocked 2026-09-08 by owner
decisions**; see "Owner decisions taken 2026-09-08" for the settled truth table, which
**supersedes acceptance criterion 1** — this review history stays below and is not restated here.
That review found the original proposal did **not** fix the headline scenario and that one step
was out of its claimed scope; both are resolved below. The original text was **NOT safe to
implement as written** — the defect was confirmed still live, but the proposed direction did not
fix the headline scenario, and one of its steps was not achievable in the scope it claimed.
See "Review 2026-09-08" and "Owner decisions taken 2026-09-08" before treating the superseded
proposal below as a work order. Originally Draft 2026-08-20, revised same day after two
independent out-of-loop reviews.

**Terminology note (2026-09-09, `refactor_run_locally_drop_ml_mode`)**: this file's several
`ML_MODE=BOTH` mentions (decision 4, the truth table discussion, § "Note on scope", and the
implementation record) predate `ML_MODE`'s removal from `run_locally.sh` and were already loose
shorthand at the time — the mechanism they describe (the horizon loop running PENTAD then DECAD
without stopping on a PENTAD failure) is driven by `SAPPHIRE_PREDICTION_MODE=BOTH`, which is what
`run_locally.sh`'s own comments now say (see `run_machine_learning`/
`run_maintenance_machine_learning`). `ML_MODE` no longer exists; nothing in this file's actual
shipped resolution (decisions 3/4, the exit-5 truth table, the log-suffix fix) depended on it.
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
`run_in_venv`'s `2>&1 | tee` (`run_locally.sh:642-643`), while the empty-set and
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

`run_machine_learning` (`run_locally.sh:717-743`) loops `ML_MODELS × ML_SCRIPTS`
with `break 2` at `:731`; `recalculate_nan_forecasts.py` runs before
`make_forecast.py` in the `ML_SCRIPTS` array (`:174-178`). A failure for `TFT` prevents the operational
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
   (`run_locally.sh:626`) and `sl.load_environment()` (`:512`) fails first.
3. Observe: `make_forecast.py` exits 0 and the module records `PASS`, the
   `decad_<MODEL>_forecast.csv` archive updates, and no database rows appear. The
   only trace is one WARNING in `apps/logs/run_locally_*.log`.
   **Assert on the module's status, not the runner's** — single-module runs also
   invoke validation, which records its own readiness failure and makes
   `print_summary` force a final exit 1 for a different reason.

---

## Review 2026-09-08 — the plan does not fix the bug it describes

Third out-of-loop pass, verified at file:line against trunk. **The defect is still live**:
`make_forecast.py:171` and `:222` call the writer bare, `:173-175`/`:224-226` catch and continue, and
there is no `sys.exit` (`:876`).

### Two blocking findings

**1. The proposed `WROTE(n)` cannot be produced by a wrapper-only change, and the exact reported
failure survives the fix.** `_write_ml_forecast_to_api` does `count = client.write_forecasts(records)`
and then `return True` **regardless of count** (`utils_ml_forecast.py:805-813`) — it also *prints*
"Successfully wrote 0 ML forecast records". So an API that accepts the call and stores nothing is
reported as success, and no wrapper-level change can see it. **The plan's own headline scenario —
"exits 0 after writing no forecasts" — is therefore still reachable after implementing the plan.**

Smallest correction: keep the helper's boolean and make a zero count return `False`, or have it
return the count. **Do not give the helper an enum** — every `Enum` member is truthy, so the six
caller modules that currently do `if api_ok:`/`if ok:` would silently treat a failure member as
success.

**2. "Aggregate after the model loop" is not achievable inside `make_forecast.py`.** The `break 2`
lives in the shell (`run_locally.sh:775-783`), so the loop that must not be broken is *outside* the
process whose exit code we are changing. A Python-side aggregate cannot span three separate
processes. **`run_locally.sh` must be in scope**, and it must distinguish "API delivery failed after
the CSV fallback succeeded — continue, remember it" from an ordinary computation failure that should
still fail fast. A dedicated exit code is the minimal mechanism; a blanket "continue on any non-zero"
would silently discard the existing fail-fast contract. Also decide whether the guarantee extends to
`ML_MODE=BOTH`, whose outer loop currently stops DECAD after a PENTAD failure and is **test-pinned**
at `test_run_locally_orchestration.py:713-725`.

### Two further defects in the plan text

**3. Acceptance criterion 1 is self-contradictory.** "Every outcome except `DISABLED` … is treated as
a failure" classifies a successful `WROTE` as a failure. It also leaves "the required CSV write"
undefined while the script writes **two** files (`*_forecast_latest.csv` and the archive
`*_forecast.csv`), and swallows archive failures separately (`:178-199`, `:228-250`). This needs an
explicit truth table.

**4. `NOTHING_TO_WRITE` and `DISABLED` need a precedence rule.** With `SAPPHIRE_API_ENABLED=false`
*and* zero eligible rivers, returning only `DISABLED` exits 0 having produced no fresh forecast;
treating `NOTHING_TO_WRITE` as always-fatal makes a legitimately-zero-station model an operational
failure. The wrapper also **cannot** distinguish "zero rivers selected" from "rivers selected but
every predictor returned an empty frame" — both arrive as an empty frame.

### Corrections to this document's factual claims

- **"A missing client is a third completely invisible cause"** — **wrong end-to-end.** With API mode
  on, the run fails earlier: `read_daily_discharge_data` selects the API
  (`forecast_library.py:2514-2521`) and raises for a missing client (`:2334`). Invisible only if a
  wrapper is called directly. The claim holds at wrapper level, not operationally.
- **"Empty `rivers_to_predict` is effectively the only source of an empty record set"** — **wrong.**
  With rivers selected, every predictor can still return an empty frame; `:806-829` assigns `flag=2`
  but appends no rows.
- **"A short `codes_to_use` necessarily produces degraded rows rather than zero rows"** — **wrong.**
  Missing enrichment can make the predictor raise or return empty, which appends nothing.
- **PP-051's shipped shape is NOT the enum proposed here.** It is `WROTE`, `SKIPPED_BY_CONFIG`,
  `SKIPPED_NO_RECORDS`, `SKIPPED_NOT_DEPLOYED`, `FAILED` (`postprocessing_forecasts/src/api_writer.py:67-83`),
  and its doctrine is explicit: **only `FAILED` is a failure**, so `SKIPPED_NO_RECORDS` is benign.
  That directly contradicts acceptance criterion 1 here. Since this issue says not to diverge from
  PP-051's settled shape, the divergence must be resolved deliberately, not by accident.
- **`recalculate_nan_forecasts.py` is a weaker precedent than claimed.** It captures the bool but
  collapses disabled, missing client, no-replacements, readiness failure and exceptions into one
  generic warning (`:455-459`), and calls deliberate CSV-only mode "unsuccessful".
- The **ML-016** note is stale: the bare target now resolves and validates modes
  (`run_locally.sh:529-580`). Does not affect this issue's reasoning.

### Owner decisions taken 2026-09-08 — these settle the plan

1. **A zero-row save is a failure.** The database accepting the request and storing nothing must
   report failure. This is the reported bug, so `_write_ml_forecast_to_api` must stop returning
   `True` on a zero count (`utils_ml_forecast.py:805-813`) and must stop printing
   "Successfully wrote 0 …".
2. **"Nothing to send" is NOT a failure.** Zero eligible stations / an empty record set is a normal
   outcome, matching PP-051's shipped doctrine (`SKIPPED_NO_RECORDS` is benign). **It must be logged
   loudly** — today it is INFO under a WARNING-capped root logger, so it reaches no log at all. Raise
   it to WARNING. This resolves the contradiction with acceptance criterion 1: AC1's "every outcome
   except DISABLED is a failure" is **wrong and is superseded by the truth table below**.
3. **`run_locally.sh` IS in scope.** One model's save failure must not stop the other models from
   running and writing their CSV backups. Implement with a dedicated exit code meaning "the forecast
   computed and its CSV was written, but the database save failed — record it and continue"; every
   other non-zero keeps today's fail-fast behaviour.
4. **In `ML_MODE=BOTH`, a PENTAD failure must no longer stop DECAD.** *This diverges from the
   recommendation and widens the change*: the current stop-on-first-failure behaviour is
   **test-pinned at `test_run_locally_orchestration.py:713-725`**, so that test must be deliberately
   inverted, not deleted, and the inversion must be called out in the PR.

### The truth table (supersedes acceptance criterion 1)

| Situation | Task result | Why |
|---|---|---|
| Rows saved, count > 0 | **success** | |
| Request accepted, **0 rows stored** | **FAILURE** | decision 1 — the reported bug |
| API unreachable (readiness false) | **FAILURE** | a genuine delivery failure |
| The save call raised | **FAILURE** | a genuine delivery failure |
| Nothing to send (no stations, or no records produced) | **success**, logged at WARNING | decision 2 |
| `SAPPHIRE_API_ENABLED=false` | **success** | supported CSV-only mode |
| Client not installed | **success** | dependency-gated; note it is unreachable operationally anyway (an API-mode run fails earlier at `forecast_library.py:2334`) |

**CSV is out of scope.** The two CSV writes keep exactly today's behaviour, including today's
swallowing of archive failures. Failure is keyed on the database save alone. Anything else would
change behaviour this issue did not set out to change — file it separately if it matters.

### Precedence: "nothing to send" wins, and must be detected FIRST

Added 2026-09-08 after the confirm pass, which found the table underdetermined: the helper today
checks client availability (`:737`), disabled (`:742`) and readiness (`:752`) **before** it ever
discovers the record set is empty (`:804`). Two states therefore overlap and the table alone does not
say which applies:

- **disabled + nothing to send** → returns at the disabled branch, so the WARNING decision 2 requires
  is **never emitted**;
- **readiness-false + nothing to send** → classified as a delivery failure, though decision 2 says
  "nothing to send" is a success.

**Rule: emptiness is evaluated before client construction and before the readiness check.** If there
is nothing to send, return the benign "nothing to send" outcome and log it at WARNING, whatever the
API's state — we cannot have failed to deliver something we never had. Only if there *is* something
to send do the client/disabled/readiness branches apply.

This is the same ordering class that produced two defects in PREPG-026: a check placed where it reads
naturally rather than where the semantics require it.

### Scope of decision 4 — which loops

The confirm pass found the decision named only the bare-target loop (`run_locally.sh:2569`), but mode
loops also exist at `:1528` (short-term), `:1665` (maintenance), `:1716`/`:1733` (daily operational
and maintenance phases) and `:2486` (direct ML maintenance). Changing only the bare target would
leave `short-term` and `daily` still stopping before DECAD — the decision half-applied, which is
worse than not applying it, because the behaviour would then differ by entry point.

**Rule: keep `break 2` fail-fast *within* the failed horizon, but continue to the next horizon.**
Apply at every entry point listed above; if any is deliberately excluded, say which and why here.

**Two tests pin the current behaviour and must both be deliberately inverted, not deleted:**
`test_run_locally_orchestration.py:713-725` (stop-on-first-failure) and `:756-774` (which expects
validation to run for PENTAD only, because DECAD never ran). Call both inversions out in the PR.

### Decisions as originally raised (superseded by the above)

1. **The success/failure truth table**, explicitly: which outcomes fail, whether a zero API count
   fails, and which of the two CSV writes is "required".
2. **Is `SKIPPED_NO_RECORDS` benign (PP-051's doctrine) or fatal here?** They cannot both be true.
3. **Is `run_locally.sh` in scope?** If not, finding 2 says the fix cannot be done without making
   things worse, and this issue should be reduced to reporting-only.
4. Does the "other models still run" guarantee extend to `ML_MODE=BOTH`?

### Note on scope, given the standing "keep changes minimal" instruction

`make_forecast.py` is the **only** one of nine call sites that discards the helper's return; the
other six modules already capture it (`ok`, `api_ok`, `api_write_ok`). Combined with finding 1's
minimal correction, a much smaller fix than the proposed enum appears available: make the helper
report a zero count as failure, capture the boolean in the two wrappers, aggregate within
`make_ml_forecast`, and add one dedicated exit code in `run_locally.sh`. That is a direction, not a
decision — it still depends on answers 1-4.

## ~~Proposed direction~~ — **SUPERSEDED 2026-09-08. DO NOT IMPLEMENT THIS SECTION.**

> This block is retained only as the record of what was originally proposed and why it was rejected.
> **The work order is "Owner decisions taken 2026-09-08" and the truth table above.** Specifically:
> step 1's `WROTE(n)` **cannot be produced by a wrapper-only change** (the helper discards the count);
> step 2's Python-side "aggregate after the model loop" is **impossible** (the loop is in the shell);
> step 3 is **decided** — nothing to send is benign; step 5's "optionally raise to WARNING" is **not
> optional**, it is required by decision 2 and is the only thing that makes the condition visible.

### Original text, superseded

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

> **Criterion 1 below is SUPERSEDED by the truth table above** — it classifies a successful write as
> a failure. The truth table governs. The remaining criteria stand, with two additions:
>
> **7. The headline regression is pinned by a test**: non-empty records, `write_forecasts()` returns
> `0` → the helper reports failure, the run exits non-zero, and no "Successfully wrote 0" line is
> produced. No existing test covers this — today's tests cover a positive count
> (`test_api_integration.py:223`) and empty input that never reaches the API (`:383`).
> **8. A test pins that "nothing to send" is logged at WARNING and does NOT fail**, including when
> the API is disabled and when readiness is false — the two overlapping states above.

1. ~~Every outcome except `DISABLED` — and only when the required CSV write
   succeeded — is treated as a failure and reported with its cause.~~ **SUPERSEDED — see the truth
   table.**
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

## Implemented 2026-09-09

Implements the truth table and the four owner decisions above. Commits `685016e5` (the fix) and
`1bded0cb` (two review-found corrections, same PR) on branch `docs_close_out_499_500`.

- **`_write_ml_forecast_to_api`** (`apps/machine_learning/scr/utils_ml_forecast.py:713-859`) now
  matches the truth table: it **raises `SapphireAPIError`** for a genuine delivery failure —
  `readiness_check()` false (`:781-785`), or `client.write_forecasts()` returning a falsy/zero
  count (`:850-855`, the reported bug: the code no longer logs "Successfully wrote 0…"). It
  **returns `False` without raising** for the benign no-ops decision 2 covers — empty input
  (`:757-763`), the client library not installed (`:765-767`), `SAPPHIRE_API_ENABLED=false`
  (`:770-773`), and an empty record set after dedup/build (`:837-844`). Emptiness is checked first
  (`:757`), before client construction or the readiness check, matching the precedence rule the
  confirm pass added.
- **`write_pentad_forecast` / `write_decad_forecast`** (`make_forecast.py:151-231`, `:234-318`)
  catch that exception, log it, and return `api_write_ok=False` — `True` for every other outcome,
  including the benign no-ops. The CSV writes (today-latest and the archive append) run
  unconditionally either way, matching the "CSV is out of scope" decision.
- **`make_ml_forecast`** (`make_forecast.py:968-972`) calls `sys.exit(5)` when `api_write_ok` is
  `False` — the dedicated exit code decision 3 required, chosen because
  `sync_long_horizon_hydrograph.py` already uses 5 for an API read/write failure (INFRA-044
  precedent).
- **`run_locally.sh`** (`run_machine_learning`, `:767-833`) treats `script_rc == 5` as
  "computed and CSV-written, but the database save failed": it logs an ERROR, sets
  `db_save_failed=true`, and continues the `model x script` loop instead of `break 2`-ing —
  so an ML failure for one model, or one horizon under `ML_MODE=BOTH`, no longer stops the
  remaining models or the other horizon from computing and writing their CSV backups (decisions 3
  and 4). Every other non-zero `script_rc` still `break 2`s exactly as before. If nothing else
  failed, the module's own `rc` is set to 5 at the end (`:816-818`) so the row is still recorded
  `FAIL` and the overall run still exits non-zero — decision 3 changes *what continues*, not
  *whether the run is reported as failing*.
- **Two defects found by the post-implementation out-of-loop review and fixed in the same PR**
  (commit `1bded0cb`):
  - (a) The post-write `_check_ml_forecast_consistency` call shared a `try` block with the API
    write (`make_forecast.py:184-205`), so an exception raised by the consistency check itself was
    caught by the same `except Exception` and mislabelled `api_write_ok=False` → exit 5. Concrete
    case: `SAPPHIRE_CONSISTENCY_CHECK=true` with nothing to send — `_check_ml_forecast_consistency`
    does `csv_data["forecast_date"] = pd.to_datetime(csv_data["forecast_date"])`
    (`utils_ml_forecast.py:992`) outside its own `try`, which raises `KeyError` on an empty frame,
    making a legitimately-nothing-to-do run (decision 2: success) claim a save failure. Fixed by
    giving the consistency check its own `try`/`except`, run only when the write itself succeeded,
    that can no longer touch `api_write_ok`.
  - (b) `run_machine_learning` wrote every invocation's log to the same fixed path
    (`${ERROR_DIR}/machine_learning.log`, truncated with `>` on every call). Once decision 4
    (commit `685016e5` — the `continue` that replaced the aborting `return 1`) let a second
    (DECAD) invocation run after a failed PENTAD one under `ML_MODE=BOTH`, DECAD's successful output
    overwrote the log PENTAD's `FAIL` row pointed at, so `MODULE ERROR DETAILS` showed the wrong
    horizon's output. Fixed by suffixing the log filename with `SAPPHIRE_PREDICTION_MODE`
    (`run_locally.sh:790-795`, falling back to the unsuffixed name when unset) — the same shape as
    `run_module_validation`'s `LABEL_SUFFIX` (INFRA-037). The recorded row label stays
    `"machine_learning"` either way; only the log file path changed.
    **SUPERSEDED 2026-09-09 (`refactor_run_locally_drop_ml_mode`)**: the row label no longer stays
    unsuffixed. The owner decision that removed `ML_MODE` also gave the PASS/FAIL/SKIP row the same
    horizon suffix as the log file (e.g. `machine_learning (PENTAD)`, `machine_learning
    (maintenance) (DECAD)`) — see `test_pentad_failure_log_is_not_overwritten_by_decad_success` and
    `test_pentad_maintenance_failure_log_is_not_overwritten_by_decad_success` in
    `test_run_locally_orchestration.py`. This sentence accurately describes commit `685016e5`'s own
    diff at the time it was written; it is no longer true of current `run_locally.sh`.
- **Verification.** `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` is green across all 16
  modules/services with zero unexpected skips (re-run in this review session: `machine_learning`
  161 passed, `pipeline` 407 passed — both figures re-verified directly, not carried over from the
  implementer's report). Ten tests were added in the second commit (`1bded0cb`) — 8 in
  `apps/machine_learning/test/test_write_forecast.py`, 2 in
  `apps/pipeline/tests/test_run_locally_orchestration.py` — each mutation-verified against a
  revert of the line it guards, per the commit message.

### Deliberately not addressed

Three **pre-existing** defects surfaced by the reviews are not fixed by this PR and need their own
issues:

1. **The consistency check is a no-op for pentad/decad.** `_write_ml_forecast_to_api` stores every
   record with `"horizon_type": "day"` regardless of the caller's horizon
   (`utils_ml_forecast.py:717-721` docstring, `:806-818` the actual write), but
   `_check_ml_forecast_consistency` reads back with `horizon=horizon_type` where `horizon_type` is
   literally `"pentad"` or `"decade"` (`:1002-1003`/`:1016-1017`), so it always finds nothing, logs
   "No API data found for consistency check" (`:1027`), and returns `True`. With
   `SAPPHIRE_CONSISTENCY_CHECK=true`, the check therefore verifies nothing while reporting
   consistency.
2. **`apps/machine_learning/locally_run_ml_forecasts.sh:78`** pipes `make_forecast.py` through
   `tee` with no `pipefail`/`PIPESTATUS` anywhere in the script (verified: neither appears in the
   file), so the new exit 5 is discarded, the script continues, and it still prints "All runs
   completed. Check logs/summary.log for the last 10 lines of each run." (`:127`) reporting
   success.
3. **`apps/machine_learning/make_forecast.py`'s outer `if SAPPHIRE_API_AVAILABLE:` guard**
   (`:182`/`:265`) bypasses `_write_ml_forecast_to_api` entirely when the client is absent — the
   supported CSV-only mode. With the client absent and nothing to forecast, no "no records" warning
   is emitted at all, because the function that would emit it is never called.

**Open design question for the owner**: whether a genuine consistency *mismatch* (as opposed to
the no-op above) should fail the run. Today `_check_ml_forecast_consistency`'s return value is
discarded by both callers (`make_forecast.py:201`/`:284` — the return value is not captured), and
this PR deliberately preserved that; only the exception path was touched (defect (a) above).

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
