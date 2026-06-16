## `GBT_Base` model name has no `ModelType` enum value → long-term forecast writes 422 (LTF-GBTBASE)

**Status**: Draft (2026-06-13) — **flagged for next-week high-priority work**
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
