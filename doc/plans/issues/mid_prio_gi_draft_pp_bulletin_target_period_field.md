# Bulletin schema cannot represent a per-site target period — reload heuristic is a guess, not a fix

**Status**: Draft
**Module**: sapphire/services/postprocessing (`Bulletin` model — colleague-managed) + apps/forecast_dashboard (follow-up once the field exists)
**Priority**: Medium
**Labels**: `schema`, `postprocessing`, `bulletin`, `coordination`, `data-integrity`
**Discovered**: 2026-07-14, during FD-018 review (m0-card / main-panel bulletin per-site target month fix, branch `fix_fd_m0_bulletin_target_month`)
**Related**: FD-018 (m0 bulletin per-site target period — implemented dashboard-side workaround), `low_prio_gi_draft_fd_bulletin_same_station_both_cards.md` (sibling issue, same schema)

---

## Summary

`Bulletin` (`sapphire/services/postprocessing/app/models.py:253-286`) has no column that
records *which calendar period a row's forecast targets*. Every row saved together in one
bulletin — whether it came from the main monthly panel (Kyrgyz `month_1`, target = issue
month + 1) or from the m0 card (lead-0, target = the issue month itself) — carries the
identical `horizon_value`, because that value is bulletin-wide, not per-lead. After a
save-then-reload, nothing in the persisted record can tell the dashboard which target month
a given row actually belongs to. The dashboard works around this with a best-effort
heuristic that matches persisted `(model_type, forecasted_discharge)` values against both
source frames; it is described in detail below and is **explicitly a guess**, not a fix.

## Problem

### 1. `Bulletin` has no target-period column

```python
# sapphire/services/postprocessing/app/models.py:253-286
class Bulletin(Base):
    __tablename__ = "bulletins"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # Metadata fields
    horizon_type = Column(SQLEnum(HorizonType), nullable=False)
    year = Column(Integer, nullable=False)
    horizon_value = Column(Integer, nullable=False)

    code = Column(String(10), nullable=False)
    model_type = Column(SQLEnum(ModelType), nullable=False)

    basin_name = Column(String(100))
    station_label = Column(String(100))
    forecasted_discharge = Column(Float)
    fc_lower = Column(Float)
    fc_upper = Column(Float)
    delta = Column(Float)
    sdivsigma = Column(Float)
    mae = Column(Float)
    accuracy = Column(Float)

    __table_args__ = (
        Index("ix_bulletins_horizon_year_number", "horizon_type", "year", "horizon_value"),
        UniqueConstraint(
            "horizon_type", "year", "horizon_value", "code",
            name="uq_bulletins_horizon_year_number_code",
        ),
    )
```

Confirmed: the exact column list is `horizon_type, year, horizon_value, code, model_type,
basin_name, station_label, forecasted_discharge, fc_lower, fc_upper, delta, sdivsigma, mae,
accuracy`. There is **no `valid_from`/`valid_to`** and no `target_month`/`target_year` —
unlike `LongForecast` (`models.py:117-146`), which has `valid_from = Column(Date,
nullable=False)` and `valid_to = Column(Date, nullable=False)` for exactly this purpose.

### 2. `horizon_value` is bulletin-wide, not per-lead

`apps/forecast_dashboard/dashboard/bulletin_manager.py:627-632`:

```python
def _horizon_context(self) -> tuple[str, int, int]:
    return (
        self.wm.horizon_selector.value,
        self.wm.forecast_year,
        self.wm.forecast_horizon,
    )
```

Both `_on_add` (main panel, line 830) and `_on_add_m0` (m0 card, line 906) call
`_save_bulletin_to_api(*self._horizon_context(), [selected_site])` — the *same*
`wm.forecast_horizon` scalar, regardless of which card the site came from. That scalar also
mirrors verbatim into every saved row via `_site_to_records`
(`bulletin_manager.py:345-368`, confirmed: it writes exactly the 14 `Bulletin` columns above,
nothing else — no `valid_from` or period field). And `horizon_value` is part of the DB unique
key (`UniqueConstraint("horizon_type", "year", "horizon_value", "code")`, confirmed above) —
so it cannot be repurposed as a per-lead discriminator without also touching the uniqueness
semantics.

### 3. Consequence: the correct target period is unrecoverable after reload

Because no persisted field distinguishes an m0 row from a main-panel row, a genuine cold
reload (`_load_bulletin_from_api`, `bulletin_manager.py:457-556`) cannot determine which
target month a given site's forecast actually applies to. The dashboard's current
workaround (`_resolve_reload_month_target_period`, `bulletin_manager.py:219-327`) is a
**best-effort heuristic**, not a correct reconstruction:

- It matches each reloaded site's persisted `(model, forecasted_discharge)` pairs
  (`site.forecasts`, populated from the API response) against **both** `main_df`
  (`dm.forecasts_all`) and `m0_df` (`dm.long_forecasts_m0`).
- A station whose persisted discharge is found **only** in `m0_df` is treated as a
  confident m0-card match and its target period is used.
- Any other outcome — matched in both frames (ambiguous), matched in neither (the
  underlying forecast has since been recalculated), or no m0 data available for this
  deployment/station at all — falls back to the bulletin-wide default target month, which
  is exactly today's (safe but wrong-for-m0) pre-existing behavior.
- On write, if a site's target period could not be confidently resolved this session,
  `_on_write` (`bulletin_manager.py:1057-1100`) collects it into `unresolved_codes` and
  surfaces a warning popup (`"Bulletin saved, but the target month could not be confirmed
  for: <codes>"`) instead of silently writing a bulletin that may carry the wrong month's
  norm/day-count for that station.

This heuristic is **never worse than trunk's pre-FD-018 behavior** (which always used the
bulletin-wide month for every site, silently) — worst case it falls back to the same
bulletin-wide value it always used, now with an operator-visible warning. But it is
fundamentally a guess: it can misfire whenever the underlying forecast values change between
save and reload (a re-run with different discharge, differing float rounding, etc.), and it
cannot be made reliable without a schema change.

## The Ask

Add a target-period field to `Bulletin`, either:

1. **`valid_from` / `valid_to`** (mirroring `LongForecast.valid_from`/`valid_to`,
   `models.py:128-129`) — most consistent with the existing long-horizon schema, or
2. **`target_month` / `target_year`** (explicit, coarser-grained; sufficient since the
   dashboard only needs month granularity for the bulletin)

plus the corresponding Alembic migration (current head:
`sapphire/services/postprocessing/alembic/versions/b2c3d4e5f6a7_add_bulletin_share.py`,
`revision='b2c3d4e5f6a7'` — mirror its single-purpose `op.add_column`/`upgrade()`/
`downgrade()` shape), so the dashboard can persist and read back each site's true target
period instead of guessing at it.

**This is colleague-owned** (`sapphire/services/`, per `CLAUDE.md`'s Ownership Boundaries
section: *"`sapphire/services/` is managed by a colleague and must not be edited without
coordination... If a change requires modifying the API contract... open a discussion first —
do not edit the service code directly."*). This issue is that discussion request, not an
implementation plan for the service side.

### Dashboard-side follow-up (once the field exists)

Once `Bulletin` carries a real target-period field, three dashboard changes become
straightforward and the heuristic can be deleted entirely:

- `_site_to_records` (`bulletin_manager.py:345-368`) — write the site's captured target
  period into the new field.
- `_load_bulletin_from_api` (`bulletin_manager.py:457-556`) — read the field back directly
  instead of calling `_resolve_reload_month_target_period`.
- `_resolve_reload_month_target_period` and its helper `_forecast_value_matches`
  (`bulletin_manager.py:184-327`) — delete; no longer needed.

## Acceptance Criteria

- [ ] Decision made with the service owner: `valid_from`/`valid_to` vs.
      `target_month`/`target_year` on `Bulletin`.
- [ ] `Bulletin` model updated with the chosen field(s); Alembic migration added
      (`down_revision='b2c3d4e5f6a7'`).
- [ ] `BulletinCreate`/`BulletinResponse` schemas updated to include the new field(s).
- [ ] Service tests cover create/read of the new field(s) (mirror
      `sapphire/services/postprocessing/tests/test_endpoints.py` bulletin test class).
- [ ] Dashboard-side follow-up (see above) tracked as a linked task once the field ships;
      `_resolve_reload_month_target_period` and `_forecast_value_matches` deleted, and the
      `test_bulletin_m0_target_period.py` reload tests updated to assert on the real field
      instead of the heuristic's match/fallback behavior.
- [ ] No station codes or discharge values committed in code, fixtures, or docs (use
      `19999` for any example station code).

## Out of Scope

- Redesigning `horizon_value` itself (see the sibling issue for the DB unique-key
  implications of allowing the same station in a bulletin twice).
- Backfilling target periods for existing `bulletins` rows (new rows only; existing rows
  keep falling back to the heuristic/bulletin-wide default until re-saved).

## References

- Dashboard-side heuristic and its docstring: `apps/forecast_dashboard/dashboard/bulletin_manager.py:219-327`
- Operator-facing warning on write: `apps/forecast_dashboard/dashboard/bulletin_manager.py:1057-1100`
- Reload-time resolution: `apps/forecast_dashboard/dashboard/bulletin_manager.py:507-544`
- `Bulletin` model: `sapphire/services/postprocessing/app/models.py:253-286`
- `LongForecast` model (has the fields being proposed here): `sapphire/services/postprocessing/app/models.py:117-146`
- Current Alembic head: `sapphire/services/postprocessing/alembic/versions/b2c3d4e5f6a7_add_bulletin_share.py`
- Tests locking current (heuristic) behavior: `apps/forecast_dashboard/tests/test_bulletin_m0_target_period.py`
