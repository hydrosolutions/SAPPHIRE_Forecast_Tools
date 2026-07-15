# PP-040: `ARIMA` has no `ModelType` enum value — latent 422 if any deployment enables it

**Status**: Draft (2026-07-14)
**Module**: `apps/machine_learning` (fair game) + `sapphire/services/postprocessing` (`ModelType` enum —
colleague-managed, coordinate before editing) + `apps/postprocessing_forecasts/src/api_writer.py`
(fair game)
**Priority**: Low
**Labels**: `model_type`, `enum`, `latent`, `machine_learning`

---

## Summary

`apps/machine_learning/make_forecast.py` fully implements ARIMA as a short-term ML model:
`get_predictor_class()` dispatches `MODEL_TO_USE == "ARIMA"` to `predictor_ARIMA.PREDICTOR`
(imported at the top of the file: `from scr import TFTPredictor, TiDEPredictor, TSMixerPredictor,
predictor_ARIMA, utils_ml_forecast`), and the module's own error message advertises it as a valid
choice: `"Please choose one of the following models: TFT, TIDE, TSMIXER, ARIMA"`
(`make_forecast.py:459-461`). Trained ARIMA artefacts exist in the deployment data repos.

However:

- `ModelType` (`sapphire/services/postprocessing/app/models.py:32-52`) has no `ARIMA` member.
- `apps/postprocessing_forecasts/src/api_writer.py`'s `MODEL_TYPE_MAP` (lines 23-41) — the dict
  the operational writer uses to translate `model_short` into the API's `model_type` value before
  POSTing — has no `"ARIMA"` entry either.

So if any deployment ever sets `ARIMA` in `ieasyhydroforecast_available_ML_models`, ARIMA
forecasts will run successfully through `make_forecast.py` and then fail to write: the API POST
will 422 (unknown `model_type`) unless a colleague-managed enum migration lands first, or — if a
future refactor made `MODEL_TYPE_MAP` fall through to the raw model name instead of 422'ing
loud — the row would be silently written under an inconsistent/unexpected value instead. Verify
which failure mode is current before assuming either; the point is that neither the plan nor the
service today expects `"ARIMA"` to pass its `ModelType` check.

## Why this is latent, not live

Checked every real deployment `.env` in `~/Documents/GitHub/kyg_data_forecast_tools/config/` and
`~/Documents/GitHub/taj_data_forecast_tools/config/` (all env variants: `.env_kghm_server`,
`.env_develop_kghm.server`, `.env_develop_kghm`, `.env_kghm`, `.env_bea_kghm`,
`.env_develop_tjhm`, `.env_max_tjhm`):

```
ieasyhydroforecast_available_ML_models=TFT,TIDE,TSMIXER
```

on every one of them. No deployed environment enables `ARIMA`. The sole exception is
`.env_sandro_kghm` (a personal dev env), which sets
`ieasyhydroforecast_available_ML_models=TFT,TIDE,TSMIXER,LSTM` — but `LSTM` is not a model
`apps/machine_learning` supports at all (no `LSTMPredictor`, no `MODEL_TO_USE == "LSTM"` branch in
`get_predictor_class()`), so that entry is a stale personal-env leftover, not evidence of a
deployed capability gap. It does not change the assessment: ARIMA is dormant in every real
deployment.

## Why the coercion fix (same branch) does not resolve this

The companion patch on this branch adds `ModelType.coerce()` (`models.py`) plus a Pydantic
`field_validator` so name-form (`"TIDE"`, the DB-stored enum *name*) and case-variant model names
resolve to the matching enum *member*. That only helps for names that already have a member to
resolve to. `ARIMA` has no member of any name/case in the enum, so `coerce()` returns it unchanged
(see the trailing `return value` in `models.py`'s `coerce` classmethod) and Pydantic validation
still rejects it. Coercion is a normalization fix, not a vocabulary-extension fix.

## What enabling ARIMA operationally would require (not done here — see Decision below)

1. **`ARIMA` added to `ModelType`** (`sapphire/services/postprocessing/app/models.py:32-52`), with
   a `description` entry if a display name is wanted — service change, colleague-managed, open a
   discussion first per `CLAUDE.md` Ownership Boundaries.
2. **An Alembic migration** adding the new value to the Postgres enum type, e.g.
   `ALTER TYPE modeltype ADD VALUE 'ARIMA'` — must run in an autocommit block (Postgres does not
   allow `ALTER TYPE ... ADD VALUE` inside a transaction in older PG versions, and even PG 12+ does
   not allow using the new value in the same transaction that adds it). See
   `sapphire/services/postprocessing/alembic/versions/` for the existing migration pattern
   (colleague-managed).
3. **`"ARIMA": "ARIMA"` added to `MODEL_TYPE_MAP`** in
   `apps/postprocessing_forecasts/src/api_writer.py` (fair game, app-side) so the operational
   writer actually maps it through instead of relying on the `.fillna(...astype(str))` fallback at
   `api_writer.py:325-327`, which would silently pass the raw `model_short` through only for a
   value not present as a map key — worth confirming this fallback's exact behavior against a live
   422 before relying on it as a safety net.

## Why this was NOT fixed now

No deployment runs ARIMA. Adding an enum member requires a colleague-managed Postgres enum
migration — real operational risk (schema change, coordination, deploy window) for zero present
benefit, since nothing exercises the `ARIMA` path today. Filed as low-priority so it surfaces
*before* someone flips `ieasyhydroforecast_available_ML_models` to include `ARIMA` on a real
deployment, rather than being discovered as a production 422 after the fact.

## Acceptance criteria (when picked up)

- [ ] Confirm with the ML/model owner whether ARIMA is intended to go operational on any
      deployment, and on what timeline.
- [ ] If yes: coordinate the `ModelType` enum addition + Alembic migration with the service owner
      (see Ownership Boundaries in `CLAUDE.md`), then add the `api_writer.MODEL_TYPE_MAP` entry.
- [ ] If no (indefinitely dormant): downgrade this to a documentation note only, no code change
      needed.
- [ ] A test should cover the mapping (`MODEL_TYPE_MAP` contains `"ARIMA"`) once the enum member
      exists — do not add the app-side map entry before the enum member exists, or operational
      writes will just move the 422 from "unmapped" to "unknown model_type value".

## One-line correction to an earlier (incorrect) session note

An earlier session's patch reportedly added an `RRMAMBA` `ModelType` enum member. `RRMAMBA` does
not exist anywhere in `apps/` or `sapphire/services/` code (confirmed via repo-wide grep on this
branch) — it appears only as deployment **config vocabulary**
(`ieasyhydroforecast_ASSIMILATION_MODELS=RRMAMBA,RRLSTM` in both `kyg_data_forecast_tools/config/`
and `taj_data_forecast_tools/config/` env files, and as a per-station boolean flag in
`config_stations_available_for_ml_forecasts.json` in both repos) that no `apps/` code currently
reads (`grep -rn "ASSIMILATION_MODELS" apps/` and `grep -rn "RRMAMBA" apps/` both return nothing).
The real, implemented, enum-backed model is `RRAM` (`"Rainfall runoff assimilation model"`,
already in `ModelType` and already used end-to-end — `setup_library.py:3181` etc., `api_writer.py`
`MODEL_TYPE_MAP["RRAM"] = "RRAM"`). Do not re-add `RRMAMBA` to the enum; if the `RRMAMBA` config
vocabulary is ever wired up to real code, verify first whether it denotes a distinct model or is
itself a stale/aspirational name for `RRAM`.
