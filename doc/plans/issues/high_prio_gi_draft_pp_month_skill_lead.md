# Month long-term skill metrics: stratify by forecast lead (`horizon_value` schema change)

**Status**: Draft
**Module**: postprocessing_forecasts (apps) + postprocessing service (`sapphire/services/postprocessing`, **colleague-owned**)
**Priority**: High
**Labels**: `postprocessing`, `skill-metrics`, `schema-change`, `needs-coordination`

---

## Summary

Make **month** long-term skill metrics per-forecast-lead (so distinct monthly
issuances — e.g. Kyrgyz lead-0 on the 10th and lead-1 on the 25th — get separate
skill rows), by adding a `horizon_value` column to the `SkillMetric` model and
grouping monthly skill by lead.

## Context

Long-term skill was being pooled across forecast leads. The **P-PIPE** series
(already merged into `develop_forecast_skill_eval`) fixed **season** skill: it
reuses the `season_in_year` slot — which is otherwise a constant `1` — to carry the
issue lead (0/1/2/3), so the four leads persist as distinct rows on the existing
`SkillMetric` unique key with **no schema change**. **Quarter** is operationally a
single configured lead and is left as-is.

**Month is the remaining gap and cannot use the season trick.** For month,
`horizon_in_year` already holds the calendar month (1–12) — a meaningful value with
no free slot to repurpose for the lead. So month needs a genuine new column.

See the archived finding `doc/plans/issues/archive/high_prio_gi_draft_pp_skill_lead_pooling.md`
and the working analysis `doc/plans/working/skill_lead_aware_plan_revised.md`.

## Problem

`calculate_monthly_skill_metrics` groups by `["month_in_year", "code", "model_short"]`
and discards the lead, so two forecasts for the same target month issued at
different leads are averaged into one skill number. The monthly reader already
preserves `horizon_value` from the API, but the skill calculator drops it, and the
persistence layer has nowhere to store a per-lead distinction for month.

## Desired Outcome

- A monthly forecast issued at lead 0 and one at lead 1 for the same target month
  produce **two** skill rows, each reflecting only that issuance's accuracy.
- The rows persist without colliding/overwriting on write.
- `n_pairs < 2` rows are still dropped (existing floor, commit `0f62c1ad`).
- Existing season/quarter behaviour is unchanged.

---

## Technical Analysis

### Current Implementation

**Service (`sapphire/services/postprocessing/app/`, colleague-owned):**
- `models.py:196` `class SkillMetric` — has `horizon_in_year` (`:206`) but **no
  `horizon_value`**. Unique constraint `models.py:228-234` =
  `(horizon_type, code, model_type, date, horizon_in_year)`.
- `crud.py:277` `create_skill_metric` performs a **manual upsert**, matching
  existing rows on `(horizon_type, code, model_type, date, horizon_in_year)`
  (`crud.py:285-288`). This is a second collision point beyond the DB constraint —
  per-lead rows would match an existing row and overwrite it.
- `schemas.py:161` `SkillMetricBase` — has `horizon_in_year` (`:166`), no
  `horizon_value`. `get_skill_metric` read filter at `crud.py:328`.

**Apps (`apps/postprocessing_forecasts/src/`):**
- `data_reader.py:1111` `_normalize_monthly_forecasts` **preserves** `horizon_value`
  from the API row (the lead is available); `read_monthly_forecasts` `:1006`;
  `read_monthly_skill_metrics` `:289`.
- `skill_metrics.py:1142` `calculate_monthly_skill_metrics` groups lead-free at
  **multiple** sites: point metrics `:1206`, the per-group loop `:1220` (3-tuple
  unpack `for (miy, code, model), grp in ...`), the merge `:1247`, `merge_keys`
  `:1258`, the Ensemble-Mean groupby `:1309/:1326`, and the Skilled-Mean / Naive-Mean
  paths. None include the lead.
- `api_writer.py:481-482` sets `horizon_in_year_col = "month_in_year"` for month and
  writes `horizon_in_year` (`:350` in the forecast path; the skill writer
  `_write_skill_metrics_to_api` builds its payload + an in-process `upsert_key`
  ≈ `["code", "model_type", "_date", horizon_in_year_col]` — **verify exact line at
  implementation**, ~`api_writer.py:590`). `horizon_value` is not sent for skill.

### Root Cause

Month has no spare key dimension: `horizon_in_year` is the calendar month. Without a
dedicated `horizon_value` column on `SkillMetric`, per-lead month rows are
indistinguishable on the unique key and collapse on write. The calculator also never
carries the lead into its group keys.

---

## Implementation Plan

### Approach

Add a nullable `horizon_value` (Integer) column to `SkillMetric`, extend the unique
key (and the crud manual-upsert match) to include it, then make the monthly skill
calculator group by the lead and the writer send it. This mirrors how season carries
the lead, but via a real column instead of repurposing `season_in_year`. Season and
quarter writers continue to set `horizon_value` to a stable value (e.g. their
existing lead/`season_in_year`, or a sentinel) so the shared unique key behaves
consistently across horizons.

**Considered alternative (rejected):** repurpose the `date` field for month to encode
the issue month. It avoids a schema change (the dashboard already drops the skill
`date`), but makes month's `date` semantically inconsistent with quarter/season,
breaks at year boundaries, and still requires the calculator change. Cleaner to add
the column once and make all horizons consistent.

### Coordination (REQUIRED before service edits)

`sapphire/services/postprocessing` is colleague-owned. The model/schema/crud/migration
changes must be agreed with the owner before implementation. The apps-side changes can
be developed against the agreed contract.

### Files to Modify

| File | Changes |
|------|---------|
| `sapphire/services/postprocessing/app/models.py` | Add `horizon_value` column to `SkillMetric` (`:206` area); add it to the unique constraint (`:228-234`) |
| `sapphire/services/postprocessing/app/schemas.py` | Add `horizon_value: int` (or `int \| None`) to `SkillMetricBase` (`:161-166`) |
| `sapphire/services/postprocessing/app/crud.py` | Include `horizon_value` in the `create_skill_metric` match key (`:285-288`); optional read filter in `get_skill_metric` (`:328`) |
| `sapphire/services/postprocessing/alembic/versions/<new>.py` | New migration: add column + backfill + extend unique constraint |
| `sapphire/services/postprocessing/tests/` | Update SkillMetric create/read/upsert tests (SQLite in-memory, `insert().on_conflict_do_update` parity) |
| `apps/postprocessing_forecasts/src/skill_metrics.py` | Add `horizon_value` to every month group key / merge / unpack (`:1206/:1220/:1247/:1258/:1309/:1326` + SM/Naive) |
| `apps/postprocessing_forecasts/src/api_writer.py` | Send `horizon_value` in the skill payload and add it to the in-process upsert key for month |
| `apps/postprocessing_forecasts/src/data_reader.py` | Ensure `read_monthly_forecasts` carries `horizon_value` into the skill calc input (it is preserved at `:1111`; confirm it survives to the calculator) |
| `apps/postprocessing_forecasts/recalculate_skill_metrics.py` | If monthly forecasts are read per-lead like seasonal (`read_seasonal_forecasts(horizon_value=...)`), mirror that loop for month |

### Implementation Steps

- [ ] **(Coordinate)** Agree the `SkillMetric.horizon_value` contract + migration/backfill strategy with the service owner.
- [ ] Service: add `horizon_value` column + extend unique constraint (`models.py`).
- [ ] Service: add `horizon_value` to `SkillMetricBase` schema (`schemas.py`).
- [ ] Service: include `horizon_value` in `create_skill_metric` match key (`crud.py`).
- [ ] Service: alembic migration — add column (nullable), backfill existing rows (see Risks), then extend the unique constraint; update service tests.
- [ ] Apps: add `horizon_value` to ALL month group keys/merges/unpacks in `skill_metrics.py` (raw, EM, Skilled-Mean, Naive, and the CRPS/per-group loop — make tuple unpacks arity-safe).
- [ ] Apps: send `horizon_value` in `_write_skill_metrics_to_api` and add it to the month upsert key.
- [ ] Apps: confirm the monthly reader feeds `horizon_value` to the calculator (per-lead loop if needed).
- [ ] Verify per-lead month skill rows persist distinctly; recalc locally.

### Code Examples

```python
# models.py — SkillMetric
horizon_value = Column(Integer, nullable=True)  # forecast lead; NULL for legacy rows
__table_args__ = (
    Index("ix_skill_metrics_horizon_code_model_date", "horizon_type", "code", "model_type", "date"),
    UniqueConstraint(
        "horizon_type", "code", "model_type", "date", "horizon_in_year", "horizon_value",
        name="uq_skill_metrics_horizon_code_model_date_horizon_value",
    ),
)
```

```python
# crud.py — create_skill_metric match key must include the lead
existing_map = {
    (r.horizon_type, r.code, r.model_type, r.date, r.horizon_in_year, r.horizon_value): r
    for r in db.query(SkillMetric).filter(...)
}
```

```python
# skill_metrics.py — month grouping carries the lead
GROUP_COLS = ["month_in_year", "horizon_value", "code", "model_short"]
point = merged.groupby(GROUP_COLS)[...].agg(...)
for key, grp in merged.groupby(GROUP_COLS):   # arity-safe: dict(zip(GROUP_COLS, key))
    ...
```

---

## Testing

### Test Cases

- [ ] Service: two SkillMetric rows differing only in `horizon_value` both persist (no overwrite); upsert updates the matching `(…, horizon_value)` row only.
- [ ] Service: legacy row with NULL/default `horizon_value` round-trips.
- [ ] Apps: monthly skill with two leads for one (month, code, model) → two rows with the correct per-lead metrics; n_pairs<2 still dropped.
- [ ] Apps: EM / Skilled-Mean / Naive month baselines computed per lead.
- [ ] Regression: season/quarter skill rows unchanged.

### Testing Commands

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
bash run_tests.sh service:postprocessing
```

### Manual Verification

Recalc month skill locally against a DB with multi-lead monthly forecasts; confirm
distinct per-lead rows and sensible `n_pairs`.

---

## Documentation Impact

- [ ] `apps/postprocessing_forecasts/README.md` — if skill-metric schema/outputs documented
- [ ] `doc/data_flow_long_term.md` — month skill now per-lead
- [ ] Claude memory `skill_lead_aware_project.md` — update on completion
- [ ] Service migration/deploy notes (`doc/prod/`) — the alembic migration + backfill must run on deployed DBs
- [ ] No other impact expected

---

## Out of Scope

- Quarter multi-lead stratification (operational decision: single lead, unchanged).
- Dashboard "smallest-lead vs operational-lead" headline (current behaviour is fine).
- Re-deriving lead for legacy month rows that lack an issue date.

## Dependencies

- **Service-owner coordination** for the `SkillMetric` schema change + migration.
- Migration/backfill must run on all deployed postprocessing DBs before the apps-side
  per-lead writer is enabled in production.

## Risks & Unknowns

- **Unique-constraint + NULLs:** Postgres treats NULL as distinct in unique
  constraints, so legacy rows with `horizon_value=NULL` could permit duplicates.
  Decide: backfill a concrete default (e.g. the operational month lead) and make the
  column `NOT NULL`, or accept nullable with a partial/coalesced constraint.
- **Many group-key sites:** the month skill path has ~6 lead-free group/merge/unpack
  sites (incl. fixed-length tuple unpacks and the EM/Skilled-Mean weighting merges);
  all must be updated consistently or month CRPS/EM will silently mis-key. Consider a
  single `GROUP_COLS` constant.
- **Backfill cost:** regenerating historical month skill per-lead is a recalc, which
  is heavy (see DB-reset notes ~3h for postprocessing).

## Acceptance Criteria

- [ ] `SkillMetric` persists `horizon_value`; unique key + crud upsert include it.
- [ ] Monthly skill rows are per-lead; no two leads collapse into one row.
- [ ] `n_pairs < 2` rows still dropped; season/quarter unchanged.
- [ ] `run_tests.sh postprocessing_forecasts` and `run_tests.sh service:postprocessing` green, zero unexpected skips.
- [ ] Migration + backfill documented for deployment.

---

## References

- Archived finding: `doc/plans/issues/archive/high_prio_gi_draft_pp_skill_lead_pooling.md`
- Working analysis: `doc/plans/working/skill_lead_aware_plan_revised.md`
- P-PIPE season precedent: `apps/postprocessing_forecasts/src/skill_metrics.py` (`calculate_seasonal_skill_metrics`), `apps/iEasyHydroForecast/long_term_horizon_resolver.py`
- Min-`n_pairs` floor: commit `0f62c1ad`
