# ML-016: Standalone `run_locally.sh machine_learning` target crashes — empty `SAPPHIRE_PREDICTION_MODE` (and ignores `ML_MODE`)

**Status**: Draft (2026-06-19)
**Module**: `apps/run_locally.sh` (ML dispatch) + `apps/machine_learning/recalculate_nan_forecasts.py`
**Priority**: High (breaks the documented per-module verification command used by the local review checklist; recurring)
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

fails almost immediately (exit 1):

```
INFO - Model to use: TFT
DEBUG - Prediction mode:                ← EMPTY
Traceback (most recent call last):
  File ".../recalculate_nan_forecasts.py", line 468, in <module>
  File ".../recalculate_nan_forecasts.py", line 163, in recalculate_nan_forecasts
ValueError: Prediction mode %s is not supported.
Please choose one of the following prediction modes: PENTAD, DECAD
```

Reproduced on **both** tjhm and kghm. The error string also prints a literal `%s` (see bug 2).

## Root cause

Two independent bugs.

### Bug 1 — standalone target never resolves a prediction mode (and ignores `ML_MODE`)

The bare case (`apps/run_locally.sh:1932`) calls `run_machine_learning` directly:

```bash
machine_learning)
    if should_skip_module machine_learning; then ...
    else
        run_machine_learning || exit_code=$?      # <-- no mode resolution
        run_module_validation "machine_learning"
    fi ;;
```

`run_machine_learning` (`:533`) loops `ML_MODELS × ML_SCRIPTS` and calls `run_in_venv`, which
forwards `SAPPHIRE_PREDICTION_MODE=${SAPPHIRE_PREDICTION_MODE:-}` (`:443`) — i.e. **empty** when the
caller did not export it. Unlike every other ML path, the bare target does **not** wrap the call in
the mode-resolution loop:
- `daily`/`all` orchestration: `:1110–1150` (`BOTH → PENTAD,DECAD`; else default + WARN)
- `maintenance:machine_learning`: `:1859–1879` (same pattern)

Consequently the standalone target also **silently ignores `ML_MODE`** — `should_skip_ml_for_mode`
is only consulted inside those loops, never in `run_machine_learning`. So `ML_MODE=BOTH` has no
effect, and the first script in `ML_SCRIPTS` (`recalculate_nan_forecasts.py`) receives an empty mode.

`recalculate_nan_forecasts.py:159–163`:
```python
PREDICTION_MODE = os.getenv("SAPPHIRE_PREDICTION_MODE")   # no default
if PREDICTION_MODE not in ["PENTAD", "DECAD"]:
    raise ValueError("Prediction mode %s is not supported...")
```
empty (or `BOTH`) → raises.

### Bug 2 — broken error message (`%s` never interpolated)

`recalculate_nan_forecasts.py:162` uses a `%s` placeholder with no `%`-formatting, so the message
prints the literal `%s` instead of the offending value — actively unhelpful while debugging.

## Workaround (confirmed)

```bash
SAPPHIRE_PREDICTION_MODE=DECAD bash apps/run_locally.sh machine_learning   # PASS
```

(`DECAD` is ML's operational mode; pentad short-term uses LR.)

## Impact

- The documented standalone ML verification command crashes — disrupts the local review checklist
  and any manual/debug ML run. Recurs across review sessions.
- **Production `daily`/`all` pipeline is NOT affected** — it sets the mode in its loop.
- `ML_MODE` is silently inert on the standalone target (surprising; contradicts checklist Section 5
  note "Set `ML_MODE=BOTH` …").

## Fix options

1. **(preferred)** Wrap the bare `machine_learning)` case in the same mode-resolution loop used by
   `daily`/`maintenance` (respect `SAPPHIRE_PREDICTION_MODE`/`ML_MODE`, default to a mode with WARN).
   This also makes `ML_MODE` work on the standalone target.
2. Defensive default in `recalculate_nan_forecasts.py`: treat unset/empty mode as `DECAD` (or fail
   with a clear message that names the value).
3. Fix the `%s` formatting regardless (independent, trivial): `f"Prediction mode {PREDICTION_MODE} is not supported..."`.

Doing (1) + (3) is the clean fix; (2) is a cheap safety net.

## Acceptance criteria

- [ ] `bash apps/run_locally.sh machine_learning` (no extra env) runs without crashing — resolves to a
      sensible default mode (with a WARN) or both modes, consistent with `daily`.
- [ ] `ML_MODE=BOTH bash apps/run_locally.sh machine_learning` runs PENTAD then DECAD (no longer inert).
- [ ] Error message in `recalculate_nan_forecasts.py` interpolates the actual mode value.
- [ ] Review checklist Section 5 command works as written (update the doc if the contract changes).
- [ ] Tests/lint pass; sentinel station codes only in any fixtures.

## Out of scope
- The operational ML-NaN problem (TFT/TiDE/TSMixer producing NaN) — see ML-015 and ML-002.
