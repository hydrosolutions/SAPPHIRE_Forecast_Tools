## `GBT_Base` model name has no `ModelType` enum value → long-term forecast writes 422 (LTF-GBTBASE)

**Status**: Cannot Reproduce / Pending Server Confirmation (2026-07-14) — originally Draft
(2026-06-13). See the dated update at the bottom of this file: not reproducible against either
local deployment data repo (`kyg_data_forecast_tools`, `taj_data_forecast_tools`); recommend
closing unless confirmed on a live server.
**Module**: `apps/long_term_forecasting` (config + `lt_utils.map_model_name_to_model_type`) + `sapphire/services/postprocessing` (`ModelType` enum — colleague-managed)
**Priority**: **High**
**Labels**: `long-term`, `model_type`, `enum`, `deployment`, `coordination`, `data-integrity`
**Discovered**: 2026-06-13 during planning/review of the ML from-file backfill (Workstream B), reviewing the long-forecast importer (`bin/utils/migration_py/long_forecast.py`).
**Related**:
- ML from-file backfill plan ([`../working/ml_fromfile_backfill_plan.md`](../working/ml_fromfile_backfill_plan.md)) — its v2 **M1** finding; the toolkit fails-fast on this.
- PREPQ-008 — sibling enum-422 bug class (`horizon_type`); this is the `model_type` analog.

---

## Summary

The long-term config (`config_monthly.json`) lists a model named **`GBT_Base`**. The operational
long-term write path maps model names via `map_model_name_to_model_type`
(`apps/.../lt_utils.py:250-271`), which **returns unknown names verbatim**. The postprocessing
`ModelType` enum (`sapphire/services/postprocessing/app/models.py`) has **`GBT` but not
`GBT_Base`**. So any attempt to POST a `GBT_Base` long-term forecast to the postprocessing API is
rejected with a `422` (invalid `model_type`) — and because the client posts in batches, **the
whole batch fails**. Dry-run can't catch it (validated only server-side on a real POST).

This affects **both** the operational long-term write path **and** the from-file backfill
(Workstream B). The WS-B toolkit is planned to **fail-fast** on unmappable model names rather than
422 mid-batch, but that only surfaces the gap — it does not resolve where `GBT_Base` should land.

## Impact

- Latent **production** issue: if `GBT_Base` long-term forecasts are produced and written, they
  fail — so `long_forecasts` has no `GBT_Base` coverage (or operational writes have been silently
  erroring).
- Blocks **full** long-term from-file backfill coverage (WS-B) for any deployment whose config
  includes `GBT_Base`.

## Decision needed (the coordination)

Confirm with the long-term/model owner what `GBT_Base` is and whether it belongs in
`long_forecasts`, then pick one:

1. **Map** `GBT_Base → GBT` — if it is a GBT variant stored under `GBT`. Fix in
   `map_model_name_to_model_type` (and mirror in the WS-B toolkit). Toolkit-side + app-side.
2. **Add `GBT_Base` to `ModelType`** — if it is a genuinely distinct model. **Service change
   (colleague-managed `sapphire/services/`)** — coordinate; needs an Alembic enum migration.
3. **Remove `GBT_Base` from config** — if it is a calibration-only artifact, not a forecast model.

## Acceptance criteria

- [ ] `GBT_Base`'s intended representation in `long_forecasts` is confirmed with the model owner.
- [ ] One of (1)/(2)/(3) implemented; operational LT and WS-B backfill no longer risk a `422` on
      `GBT_Base` (either it writes under a valid `ModelType`, or it is intentionally excluded with a
      recorded decision).
- [ ] A test covers the chosen behavior (mapped / accepted / excluded).

---

## 2026-07-14 — CANNOT REPRODUCE on the local deployment configs; recommend closing pending server confirmation

Re-investigated while verifying a companion `ModelType.coerce()` patch. The premise as originally
written does not hold against what is actually deployed:

**What I searched:**
- `find ~/Documents/GitHub/kyg_data_forecast_tools ~/Documents/GitHub/taj_data_forecast_tools
  -iname "config_monthly*"` — **no hits**. There is no `config_monthly.json` anywhere in either
  deployment's data repo.
- `grep -rn "GBT_Base" ~/Documents/GitHub/kyg_data_forecast_tools
  ~/Documents/GitHub/taj_data_forecast_tools` — **no hits** anywhere in either data repo, including
  every operational per-mode config file that actually gets loaded
  (`config/long_term_configs/month_0.json` through `month_3.json`, `quarter.json`, and all
  `seasonal_*.json` in both repos — checked each file individually).
- Traced the real config-loading path: `apps/long_term_forecasting/config_forecast.py`'s
  `ForecastConfig._get_paths()` reads `ieasyhydroforecast_configuration_path` (→
  `<org>_data_forecast_tools/config`) and `ieasyhydroforecast_ml_long_term_configuration` (→
  `long_term_configs` on both `kghm` and `tjhm`, confirmed in `.env_kghm_server` /
  `.env_develop_tjhm`), then `load_forecast_config()` opens
  `{configuration_path}/{ml_long_term_configuration}/{forecast_mode}.json`. Both `kghm`'s and
  `tjhm`'s operational `month_1.json` (and the sibling month/quarter/seasonal files) list
  `"Base": ["LR_Base", "GBT"]` — **`GBT`, not `GBT_Base`**.
- `apps/long_term_forecasting/lt_utils.py:236-247`'s `MODEL_NAME_TO_MODEL_TYPE` dict (consumed by
  `map_model_name_to_model_type()`) has `LR_Base`, `LR_SM`, `LR_SM_DT`, `LR_SM_ROF`, `SM_GBT`,
  `SM_GBT_LR`, `SM_GBT_Norm`, `MC_ALD`, `GBT` — all four GBT-family names present in the deployed
  configs are already mapped. `GBT_Base` is not in this dict, but it's also not in any deployed
  config that would ever pass it in.

**What I did find:** `GBT_Base` **does** exist, but only in two places, neither of which is the
operational config path:
- `apps/long_term_forecasting/config_monthly.json` — a bundled template config at the module root
  of the *source* repo (`"models_to_use": {"Base": ["LR_Base", "GBT_Base"]}`). This is not on the
  path `ForecastConfig` actually loads from (it loads
  `{configuration_path}/{ml_long_term_configuration}/{forecast_mode}.json`, i.e. from the data
  repo, not from `apps/long_term_forecasting/` itself).
- `apps/long_term_forecasting/readme.md` — usage examples for the standalone
  `calibrate_and_hindcast.py` / `run_forecast.py` scripts (e.g.
  `python calibrate_and_hindcast.py --models LR_Base GBT_Base LR_SM`), which use `lt_forecast_mode`
  values like `monthly` rather than the `month_1`/`month_2`/... mode names the real deployments use.

**Two possibilities, not yet distinguished:**
(a) `config_monthly.json` is a stale/example template that predates the current
`long_term_configs/month_N.json` per-mode convention, and `GBT_Base` was renamed to `GBT` when that
convention was adopted — in which case this issue describes a historical, already-resolved state,
not a current bug.
(b) The **deployed server** config differs from what's checked into the local
`kyg_data_forecast_tools`/`taj_data_forecast_tools` clones I have — my check only covers those two
local data repos, not the server's live config tree.

**Recommendation:** close this issue unless someone confirms `GBT_Base` is present in a **deployed
server's** live `long_term_configs/*.json` (or wherever `ieasyhydroforecast_ml_long_term_configuration`
points on that server). Exact check to run there:

```bash
grep -rn "GBT_Base" "$(dirname "$0")"/../config/long_term_configs/  # or wherever
  # ieasyhydroforecast_ml_long_term_configuration resolves to on that server's .env
```

i.e., on the server, `grep -rn "GBT_Base" <configuration_path>/<ml_long_term_configuration>/`. If
that returns nothing, this issue can be archived as not-applicable. If it returns a hit, the
original triage (map→GBT / add enum value / remove from config) still applies and the priority
should stay High.

**Status changed:** Draft → **Cannot Reproduce / Pending Server Confirmation**. Not resolved, not
deleted — see `doc/plans/module_issues.md` for the corresponding row update.
