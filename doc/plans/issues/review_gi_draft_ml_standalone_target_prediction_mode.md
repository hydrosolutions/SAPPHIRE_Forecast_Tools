# ML-016: Standalone `run_locally.sh machine_learning` target crashes — empty `SAPPHIRE_PREDICTION_MODE` (and ignores `ML_MODE`)

**Status**: Review (2026-06-19). Implemented (verified 2026-08-20) — see
§ Implementation status below. This fix ships on the same branch as
INFRA-037 (`review_gi_draft_infra_run_locally_aborts_on_expected_preprocessing_failure.md`);
the full `run_tests.sh` gate is confirmed green for that branch: 16/16
modules and services pass, zero failures, and no skips introduced by the
branch (15 skips pre-existed; 1 more arrived from trunk during a rebase,
gated on bash < 4). Multiple rounds of out-of-loop adversarial review have run
against the branch.
**Module**: `apps/run_locally.sh` (ML dispatch) + `apps/machine_learning/recalculate_nan_forecasts.py`
**Priority**: High (broke the documented per-module verification command used by the local review checklist; recurring)
**Labels**: `ml`, `orchestration`, `run_locally`, `dx`, `error-message`
**Discovered**: 2026-06-19, local pipeline review (tjhm + kghm). Previously observed but never filed —
local review checklists `2026-05-05` (line ~630) and `2026-06-12` (line ~132) note the same crash.

---

## Symptom

Running the per-module ML target exactly as documented (review checklist Section 5):

```bash
ieasyhydroforecast_env_file_path=<env> bash apps/run_locally.sh machine_learning
# (optionally) ML_MODE=BOTH ... bash apps/run_locally.sh machine_learning
```

failed almost immediately (exit 1):

```
INFO - Model to use: TFT
DEBUG - Prediction mode:                ← EMPTY
Traceback (most recent call last):
  File ".../recalculate_nan_forecasts.py", line 468, in <module>
  File ".../recalculate_nan_forecasts.py", line 163, in recalculate_nan_forecasts
ValueError: Prediction mode %s is not supported.
Please choose one of the following prediction modes: PENTAD, DECAD
```

Reproduced on **both** tjhm and kghm. The error string also printed a literal `%s` (see bug 2).

## Root cause

Two independent bugs.

### Bug 1 — standalone target never resolved a prediction mode (and ignored `ML_MODE`)

The bare case (was `apps/run_locally.sh:1932`, now `:2216`) called `run_machine_learning` directly:

```bash
machine_learning)
    if should_skip_module machine_learning; then ...
    else
        run_machine_learning || exit_code=$?      # <-- no mode resolution
        run_module_validation "machine_learning"
    fi ;;
```

`run_machine_learning` (`:717`) loops `ML_MODELS × ML_SCRIPTS` and calls `run_in_venv`, which
forwards `SAPPHIRE_PREDICTION_MODE=${SAPPHIRE_PREDICTION_MODE:-}` (`:627`) — i.e. **empty** when the
caller did not export it. Unlike every other ML path, the bare target did **not** wrap the call in
the mode-resolution loop:
- `short-term`/`all` orchestration: `run_short_term_pipeline`, `:1358-1367` (`BOTH → PENTAD,DECAD`;
  else the requested single mode, or default PENTAD + WARN when unset)
- `daily` orchestration: same per-mode structure, but `run_daily_pipeline`'s Phase 3 always runs
  both `PENTAD` and `DECAD` explicitly rather than defaulting — an unset
  `SAPPHIRE_PREDICTION_MODE` never reaches ML empty on `daily`
- `maintenance:machine_learning`: `:2145-2153` (same `BOTH`/single/default-PENTAD pattern as
  `run_short_term_pipeline`)

Consequently the standalone target also **silently ignored `ML_MODE`** — `should_skip_ml_for_mode`
is only consulted inside those loops, never previously in `run_machine_learning`. So `ML_MODE=BOTH`
had no effect, and the first script in `ML_SCRIPTS` (`recalculate_nan_forecasts.py`) received an
empty mode.

`recalculate_nan_forecasts.py:160-164` (current, post-fix — see § Implementation status for the
message wording):
```python
PREDICTION_MODE = os.getenv("SAPPHIRE_PREDICTION_MODE")   # no default
if PREDICTION_MODE not in ["PENTAD", "DECAD"]:
    raise ValueError(f"Prediction mode {PREDICTION_MODE!r} is not supported...")
```
empty (or `BOTH`) → raises.

### Bug 2 — broken error message (`%s` never interpolated)

**This was not a single site.** Five `ValueError` messages across four ML files used an
un-formatted `%s` placeholder and printed the literal string `%s` instead of the offending value —
actively unhelpful while debugging. All five are the "prediction mode" / "model" validation guards,
one per maintenance/repair entry point plus the operational one:

| File | Line (current) | Function | Message |
|---|---|---|---|
| `recalculate_nan_forecasts.py` | `:163` | `recalculate_nan_forecasts` | `Prediction mode %s is not supported...` |
| `make_forecast.py` | `:459` | `get_predictor_class` | `Model %s is not supported...` |
| `make_forecast.py` | `:527` | `make_ml_forecast` | `Prediction mode %s is not supported...` |
| `fill_ml_gaps.py` | `:167` | `fill_ml_gaps` | `Prediction mode %s is not supported...` |
| `hindcast_ML_models.py` | `:157` | `main` | `Prediction mode %s is not supported...` |

An earlier draft of this issue named only the `recalculate_nan_forecasts.py` site — that
undercounted by four.

## Workaround (confirmed, superseded — see § Implementation status)

```bash
SAPPHIRE_PREDICTION_MODE=DECAD bash apps/run_locally.sh machine_learning   # PASS
```

(`DECAD` is ML's operational mode; pentad short-term uses LR.)

This workaround is no longer necessary for the crash itself — the bare target now resolves a mode
on its own (§ Implementation status) — but `DECAD` remains the right mode to request if you want the
operational behaviour rather than a default/derived one.

## Impact

- The documented standalone ML verification command crashed — disrupted the local review checklist
  and any manual/debug ML run. Recurred across review sessions.
- **Production `daily`/`all` pipeline was NOT affected** — `daily` sets both modes explicitly in its
  loop, and `run_short_term_pipeline` (used by `all`) already had its own mode-resolution loop; only
  the bare single-module target lacked one.
- `ML_MODE` was silently inert on the standalone target (surprising; contradicted the checklist
  Section 5 note "Set `ML_MODE=BOTH` …").

## Fix options (as originally proposed)

1. **(preferred, implemented)** Wrap the bare `machine_learning)` case in a mode-resolution step
   that respects `SAPPHIRE_PREDICTION_MODE`/`ML_MODE` and defaults with a WARN. This also makes
   `ML_MODE` work on the standalone target.
2. Defensive default in `recalculate_nan_forecasts.py`: treat unset/empty mode as `DECAD` (or fail
   with a clear message that names the value). **Not implemented** — superseded by option 1, which
   resolves the mode before any ML script runs rather than defaulting inside one script.
3. Fix the `%s` formatting regardless (independent, trivial). **Implemented**, at all five sites
   (see § Implementation status).

## Implementation status (2026-08-20)

Both bugs were fixed on this branch. The full
`cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` gate is confirmed green for this branch:
16/16 modules and services pass, zero failures, and no skips introduced by the branch (15 skips
pre-existed; 1 more arrived from trunk during a rebase, gated on bash < 4). Multiple rounds of
out-of-loop adversarial review have run against the branch.

### Bug 1 fix — `resolve_ml_bare_target_modes`

A new function, `resolve_ml_bare_target_modes` (`apps/run_locally.sh:514-560`), is called from the
bare `machine_learning)` case (`:2216-2242`) before `run_machine_learning` runs. Unlike a simple
port of the `daily`/`maintenance` default-PENTAD pattern, it:

- Validates `SAPPHIRE_PREDICTION_MODE` against `{"", PENTAD, DECAD, BOTH}` and `ML_MODE` against
  `{PENTAD, DECAD, BOTH}`, erroring out (`exit 1`, naming the invalid value) on anything else —
  this did not exist anywhere in the codebase before; every other caller trusted the two variables
  implicitly.
- Accepts `SAPPHIRE_PREDICTION_MODE=BOTH` directly on the bare target (previously only
  `linear_regression`/`postprocessing_forecasts` accepted `BOTH`; ML rejected it) and loops PENTAD
  then DECAD, honouring `ML_MODE` via `should_skip_ml_for_mode` for each.
- When `SAPPHIRE_PREDICTION_MODE` is unset, derives the mode from `ML_MODE` (single value, or both
  modes if `ML_MODE=BOTH`) with a `WARN` — matching the spirit of the `daily`/`maintenance`
  default, but deriving from `ML_MODE` instead of hardcoding `PENTAD`.
- When both variables are set to conflicting single values (e.g.
  `SAPPHIRE_PREDICTION_MODE=PENTAD ML_MODE=DECAD`), **errors out naming both variables** rather than
  silently picking one — a case the original three-option list did not anticipate, because none of
  the existing mode-resolution loops elsewhere in the file had to arbitrate a conflict between the
  two variables (they only ever read `SAPPHIRE_PREDICTION_MODE` and used `ML_MODE` as a per-mode
  skip filter, never as a second source of a *requested* mode).

The resolved mode(s) populate the global `ML_BARE_RESOLVED_MODES` array, which the case block loops
over, exporting `SAPPHIRE_PREDICTION_MODE` per iteration and restoring the caller's original value
afterward — the same restore-on-exit shape `run_short_term_pipeline` and
`maintenance:machine_learning` already used.

### Bug 2 fix — all five `%s` sites interpolate

Each of the five sites listed above now uses an f-string with `!r}` around the offending value
(e.g. `f"Prediction mode {PREDICTION_MODE!r} is not supported..."`), so the raised message names
the actual value (including distinguishing `None`/empty from a typo) instead of printing a literal
`%s`.

### What was not changed

- `recalculate_nan_forecasts.py`'s own `os.getenv("SAPPHIRE_PREDICTION_MODE")` call and validation
  (fix option 2) were left as-is — the mode is now guaranteed non-empty and valid by the time any ML
  script runs from `run_locally.sh`'s bare target, so a defensive default inside the script would be
  redundant for that caller. It remains the only guard for anyone invoking the script directly
  outside `run_locally.sh`.
- `maintenance:machine_learning` and the `daily`/`all` mode loops were **not** rewritten to call
  `resolve_ml_bare_target_modes` — they already had working mode resolution before this issue was
  filed (see Bug 1's "not affected" list above) and were out of scope for this fix.

## Acceptance criteria

- [x] `bash apps/run_locally.sh machine_learning` (no extra env) runs without crashing — resolves to
      a sensible default mode (derived from `ML_MODE`, with a WARN), consistent in spirit with the
      `daily`/`maintenance` default.
- [x] `ML_MODE=BOTH bash apps/run_locally.sh machine_learning` runs PENTAD then DECAD (no longer inert).
- [x] Error message in all five sites (`recalculate_nan_forecasts.py`, `make_forecast.py` ×2,
      `fill_ml_gaps.py`, `hindcast_ML_models.py`) interpolates the actual mode/model value.
- [ ] Review checklist Section 5 command works as written — updated in
      `doc/dev/review_checklist_local_template.md` §5 to describe the new resolution behaviour;
      not yet re-run end-to-end against a live deployment as part of this pass.
- [x] Tests/lint pass; sentinel station codes only in any fixtures — `cd apps && SAPPHIRE_TEST_ENV=True
      bash run_tests.sh` is confirmed green for this branch's full affected scope: 16/16 modules and
      services, zero failures, no skips introduced by the branch (15 pre-existed; 1 more arrived from
      trunk during a rebase, gated on bash < 4).

## Out of scope
- The operational ML-NaN problem (TFT/TiDE/TSMixer producing NaN) — see ML-015 and ML-002.
- INFRA-037 (the co-dependent `daily`-aborts-before-ML issue) — related but tracked separately; see
  its own file for the `--continue-on-error` hint and the `preprocessing_runoff` long-horizon
  sub-step exit-code changes.
