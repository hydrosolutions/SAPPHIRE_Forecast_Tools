# Bulletin cannot hold both the m0 and main-panel forecast for the same station

**Status**: Draft
**Module**: apps/forecast_dashboard (`bulletin_manager.py`) + sapphire/services/postprocessing (`Bulletin` unique key — colleague-managed)
**Priority**: Low
**Labels**: `bulletin`, `dashboard`, `product-question`, `data-model`
**Discovered**: 2026-07-14, during FD-018 review (m0-card / main-panel bulletin per-site target month fix, branch `fix_fd_m0_bulletin_target_month`) — review finding #3
**Related**: `mid_prio_gi_draft_pp_bulletin_target_period_field.md` (sibling issue, same schema)

---

## Summary

A station can only appear **once** in a given bulletin: `bulletin_sites` is keyed by
station `code` alone, and adding the same station from the m0 card after adding it from the
main panel (or vice versa) silently replaces the earlier entry rather than adding a second
row. This is enforced at the in-memory list level **and** at the database level via
`Bulletin`'s unique constraint. Whether an operator should be able to put both the
current-month (m0) and next-month (main panel) forecast for one station into a single
bulletin is a **product question for the hydrologists**, not something this issue asserts is
a bug.

## Problem

### 1. In-memory: `bulletin_sites` keyed by `code` alone, both cards replace

`apps/forecast_dashboard/dashboard/bulletin_manager.py:822` (`_on_add`, main panel):

```python
existing = next((s for s in self.bulletin_sites if s.code == selected_site.code), None)
if existing is None:
    self.bulletin_sites.append(selected_site)
else:
    self.bulletin_sites[self.bulletin_sites.index(existing)] = selected_site
```

`apps/forecast_dashboard/dashboard/bulletin_manager.py:900` (`_on_add_m0`, m0 card) — the
identical pattern:

```python
existing = next((s for s in self.bulletin_sites if s.code == selected_site.code), None)
if existing is None:
    self.bulletin_sites.append(selected_site)
else:
    self.bulletin_sites[self.bulletin_sites.index(existing)] = selected_site
```

Both lookups match on `s.code` only — there is no distinction between "this station's main-
panel forecast" and "this station's m0 forecast" in the key. Adding station `19999` from the
main panel, then adding it again from the m0 card, replaces the main-panel entry in
`bulletin_sites` with the m0 entry.

### 2. Enforced at the DB layer too — not just an in-memory oversight

`sapphire/services/postprocessing/app/models.py:277-286`:

```python
__table_args__ = (
    Index("ix_bulletins_horizon_year_number", "horizon_type", "year", "horizon_value"),
    UniqueConstraint(
        "horizon_type", "year", "horizon_value", "code",
        name="uq_bulletins_horizon_year_number_code",
    ),
)
```

`horizon_value` is bulletin-wide (see the sibling issue,
`mid_prio_gi_draft_pp_bulletin_target_period_field.md`, for why), so both cards save under
the same `(horizon_type, year, horizon_value)` triple. The unique key is therefore
effectively `(horizon_type, year, horizon_value, code)` — **one row per station per
bulletin**. Even a fixed in-memory list that allowed two entries for the same `code` could
not be persisted as two rows: the second `_save_bulletin_to_api` call
(`bulletin_manager.py:830`/`906`) would upsert over the first at the API/DB layer.

This is locked down by a test on this branch,
`test_same_station_both_cards_collides_at_the_api_not_just_in_memory`
(`apps/forecast_dashboard/tests/test_bulletin_m0_target_period.py:1002-1045`), which adds
station `99001` from the main panel (discharge 100.0), then from the m0 card (discharge
50.0), against a fake store whose upsert key mirrors the real DB unique constraint, and
asserts:

```python
assert len(store.rows) == 1
assert store.rows[0]["code"] == "99001"
assert store.rows[0]["forecasted_discharge"] == 50.0  # the m0 (last) add won
```

i.e. exactly one row survives, and whichever card was used *second* wins. The test's own
docstring frames this explicitly as *"intentionally NOT a bug this fix redesigns — it pins
the current (safe: no exception, no duplicate rows) behavior so a future fix for finding #3
has a locked contract to change deliberately."*

## Consequence

An operator cannot include both the current-month (m0, lead-0) forecast and the next-month
(main panel, lead-1) forecast for the **same station** in one bulletin. Today, whichever card
was used most recently for a given station silently wins in both the UI list and on save —
there is no error, but there is also no way to see both numbers side by side in the bulletin
output.

## Open Question (for the hydrologists — not asserted as a bug)

Does the bulletin product need to show both the current-month and next-month forecast for
the same station at once? Two plausible answers, both legitimate:

- **No** — a bulletin row represents "the forecast for station X in this bulletin," and a
  station naturally has one row regardless of which card produced it. Current behavior is
  correct; this issue can be closed as "working as intended" once confirmed.
- **Yes** — operators want to publish both leads for a station in one document (e.g. a
  "this month / next month" comparison), in which case the identity of a bulletin row needs
  to include the target period (or lead), not just the station code.

## Scope of a Fix (if the answer is "yes")

Any fix spans **both** layers and cannot be done dashboard-only:

- **Dashboard**: change the `bulletin_sites` lookup key from `code` alone to
  `(code, target_period)` (or `(code, card)`) in both `_on_add` and `_on_add_m0`
  (`bulletin_manager.py:822`, `:900`), and everywhere else `bulletin_sites` is searched by
  `code` alone (e.g. `_on_bulletin_edit`, `_on_remove` — not audited in detail here, would
  need a full pass).
- **Service** (colleague-owned, `sapphire/services/`): the `Bulletin` unique constraint
  would need to include the target-period field proposed in
  `mid_prio_gi_draft_pp_bulletin_target_period_field.md` (or the lead), replacing
  `("horizon_type", "year", "horizon_value", "code")` — an Alembic migration and a
  discussion with the service owner per `CLAUDE.md`'s Ownership Boundaries section.

This issue deliberately does **not** propose that redesign — it documents the constraint and
raises the product question. FD-018 (the branch this was found on) deliberately did not
redesign this either; it only fixed each card's target-month hydration for the case where a
station is added from just one card.

## Acceptance Criteria

- [ ] Hydrologists/product owner consulted: is one row per station per bulletin the intended
      product behavior, or should both leads be representable at once?
- [ ] If "working as intended": this issue is closed with that decision recorded, and the
      locked test (`test_same_station_both_cards_collides_at_the_api_not_just_in_memory`) is
      left as-is (it documents intended behavior).
- [ ] If a fix is wanted: filed as its own issue depending on both this one and
      `mid_prio_gi_draft_pp_bulletin_target_period_field.md` (the schema change is a
      prerequisite), spanning the dashboard key-lookup change and the service unique-key
      migration.
- [ ] No station codes or discharge values committed in code, fixtures, or docs (use
      `19999` for any example station code — the existing locked test predates this
      convention and uses `99001`; do not change it as part of this issue).

## Out of Scope

- Implementing either the dashboard or service-side change — this issue only documents the
  constraint and asks the product question.
- The target-period schema change itself — tracked in
  `mid_prio_gi_draft_pp_bulletin_target_period_field.md`.

## References

- `_on_add` (main panel replace-by-code): `apps/forecast_dashboard/dashboard/bulletin_manager.py:822`
- `_on_add_m0` (m0 card replace-by-code): `apps/forecast_dashboard/dashboard/bulletin_manager.py:900`
- `Bulletin` unique constraint: `sapphire/services/postprocessing/app/models.py:277-286`
- Locked-behavior test: `apps/forecast_dashboard/tests/test_bulletin_m0_target_period.py:1002-1045`
- Sibling issue (schema): `doc/plans/issues/mid_prio_gi_draft_pp_bulletin_target_period_field.md`
