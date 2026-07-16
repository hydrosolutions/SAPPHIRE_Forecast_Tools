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
a given row actually belongs to.

**Update 2026-07-14 (owner decision, branch `fix_fd_m0_bulletin_target_month`):** the
dashboard previously worked around this with a best-effort heuristic
(`_resolve_reload_month_target_period`) that matched persisted `(model_type,
forecasted_discharge)` values against both source frames. An adversarial review found the
heuristic actively **worse than trunk** — not merely unhelpful — in three ways: (1) it could
confidently resolve to the WRONG frame when an operator-edited discharge happened to coincide
with the other frame's value; (2) `site.bulletin_target_period` could go stale, because
`DataManager.load_station` reuses site objects across station/horizon/date switches
(`_sites_list` is not rebuilt), so a value cached by an earlier m0 add could leak into a later,
unrelated reload; (3) a malformed `valid_from` in a matched row raised inside the resolver, and
the caller's broad `except Exception` then silently discarded the whole saved bulletin. **The
heuristic has been deleted.** Reload is now intentionally byte-identical to trunk (always the
bulletin-wide period, both flag states); the add-time capture + write-time override (the part
of FD-018 that IS correct) is unaffected and stays. The schema ask below is unchanged and still
needed — it is the only sound way to make reload per-site-correct.

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
reload (`_load_bulletin_from_api`) cannot determine which target month a given site's
forecast actually applies to. As of 2026-07-14 the dashboard no longer attempts to guess this
(the `_resolve_reload_month_target_period` heuristic described in earlier drafts of this issue
has been **deleted** — see the Update note above): reload always uses the bulletin-wide
target period, exactly like pre-FD-018 trunk, for every site regardless of which card added
it. The write-time path is unaffected: if a site's own add-time target-period resolution
failed (missing/NaT `valid_from`), `_on_write` collects it into `unresolved_codes` and surfaces
a warning popup (`"Bulletin saved, but the target month could not be confirmed for: <codes>"`)
instead of silently writing a bulletin that may carry the wrong month's norm/day-count for that
station — this part is unchanged by the heuristic's removal.

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

The heuristic is already deleted (2026-07-14); no further dashboard cleanup is blocked on
that. Once `Bulletin` carries a real target-period field, two dashboard changes make reload
genuinely per-site-correct:

- `_site_to_records` (`bulletin_manager.py`) — write the site's captured target period into
  the new field.
- `_load_bulletin_from_api` (`bulletin_manager.py`) — read the field back directly and use it
  as `target_period`, instead of always falling back to the bulletin-wide period.

## Acceptance Criteria

- [ ] Decision made with the service owner: `valid_from`/`valid_to` vs.
      `target_month`/`target_year` on `Bulletin`.
- [ ] `Bulletin` model updated with the chosen field(s); Alembic migration added
      (`down_revision='b2c3d4e5f6a7'`).
- [ ] `BulletinCreate`/`BulletinResponse` schemas updated to include the new field(s).
- [ ] Service tests cover create/read of the new field(s) (mirror
      `sapphire/services/postprocessing/tests/test_endpoints.py` bulletin test class).
- [x] Reload heuristic (`_resolve_reload_month_target_period` / `_forecast_value_matches`)
      deleted 2026-07-14 (owner decision — proven worse than trunk); reload now always uses
      the bulletin-wide period, byte-identical to pre-FD-018 trunk.
- [ ] Dashboard-side follow-up (see above) tracked as a linked task once the field ships: read
      the new field in `_load_bulletin_from_api` and use it as `target_period` instead of the
      bulletin-wide fallback.
- [ ] No station codes or discharge values committed in code, fixtures, or docs (use
      `19999` for any example station code).

## Out of Scope

- Redesigning `horizon_value` itself (see the sibling issue for the DB unique-key
  implications of allowing the same station in a bulletin twice).
- Backfilling target periods for existing `bulletins` rows (new rows only; existing rows
  keep falling back to the heuristic/bulletin-wide default until re-saved).

## References

- Reload (now trunk-identical, with stale-cache clearing): `apps/forecast_dashboard/dashboard/bulletin_manager.py::_load_bulletin_from_api`
- Add-time capture (unaffected by this update, still the real fix): `apps/forecast_dashboard/dashboard/bulletin_manager.py::_resolve_month_target_period`, `_on_add`, `_on_add_m0`
- Operator-facing warning on write: `apps/forecast_dashboard/dashboard/bulletin_manager.py::_on_write`
- `Bulletin` model: `sapphire/services/postprocessing/app/models.py:253-286`
- `LongForecast` model (has the fields being proposed here): `sapphire/services/postprocessing/app/models.py:117-146`
- Current Alembic head: `sapphire/services/postprocessing/alembic/versions/b2c3d4e5f6a7_add_bulletin_share.py`
- Tests: `apps/forecast_dashboard/tests/test_bulletin_m0_target_period.py`
