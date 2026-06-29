# Month long-term skill metrics: stratify by forecast lead (horizon_value schema change)

**Status**: Draft
**Module**: postprocessing_forecasts (apps) + postprocessing service (`sapphire/services/postprocessing`, **colleague-owned**)
**Priority**: High
**Labels**: `postprocessing`, `skill-metrics`, `schema-change`, `needs-coordination`

---

## Precondition Verification (DB query — CONFIRMED)

A read-only query against the live postprocessing DB proves the premise. The query to verify:

```sql
SELECT
    model_type,
    horizon_value,
    COUNT(*)  AS row_count
FROM   long_forecasts
WHERE  horizon_type = 'MONTH'
GROUP  BY model_type, horizon_value
ORDER  BY model_type, horizon_value;
```

**Confirmed result — real models
(LR_BASE, GBT, MC_ALD, LR_SM, LR_SM_DT, LR_SM_ROF, SM_GBT, SM_GBT_NORM, SM_GBT_LR):**
- Each real model carries **4 distinct horizon_value values: {0, 1, 2, 3}**, ~115k–127k rows each.
- **97–100% of rows have `horizon_value == derived lead`** (months from issue `date` to
  `valid_from`). For real models, `horizon_value` IS the forecast lead.
- 96.8% of `(code, model, target_month)` groups have more than one distinct issue date.

**Baselines (NAIVE_MEAN, SKILLED_MEAN, ENSEMBLE_MEAN):**
- Carry `horizon_value ∈ {1…12}` — the **target month number**, not a lead. 100% match target
  month. This is a known "two populations" artifact: computed baselines never received a lead stamp
  from the producer.
- Baseline `horizon_value` must NOT be trusted as a lead for skill grouping. The lead must be
  derived from the real-model grouping and carried through to baseline computation (see Problem).

**Conclusion: the premise is confirmed. Month skill genuinely pools 4 distinct leads today.
PP-038 is actionable. Do NOT close this issue as not-applicable.**

---

## Summary

`calculate_monthly_skill_metrics` (`skill_metrics.py:1142`) — the only hand-rolled long-term skill
calculator (unlike quarter/season which use `_calculate_aggregated_skill_metrics`) — groups month
skill by `["month_in_year", "code", "model_short"]`. This collapses all 4 forecast leads (0, 1, 2,
3) for the same model into one skill row — a wrong-value aggregation. The EM / Naive Mean /
Skilled Mean aggregation groupbys (`skill_metrics.py:1285`, `:1424`, `:1607`) further average
across leads before computing ensemble skill.

Fixing this requires:

1. **Schema change** (service, colleague-owned): add `horizon_value` column to `skill_metrics`,
   extend the unique constraint to include it.
2. **Apps changes**: add `horizon_value` to the skill groupby keys, EM/Naive/SM aggregation
   groupbys, writer upsert key, writer payload, and reader read-back.
3. **Migration + backfill**: staged add → backfill → NOT NULL → constraint swap, with all
   non-month skill writers sending an agreed sentinel (e.g. `0`) to avoid a cross-horizon
   NULL-tuple corruption hazard in the shared upsert function.

---

## Context

The P-PIPE series (merged into `develop_forecast_skill_eval`) made **season** skill per-lead by
including the lead dimension in `time_group_cols`. **Quarter** is operationally single-lead
(`iEasyHydroForecast/long_term_horizon_resolver.py:47`).

**Month is NOT like quarter.** The DB proves that month producers stamp leads 0–3. Month is the
only long-term skill calculator still hand-rolled; all others use
`_calculate_aggregated_skill_metrics`. For season, the per-lead dimension was encoded by
overloading `season_in_year`. For month, `horizon_value` is already stamped correctly by the
producer on real-model rows — it is simply not carried into the skill groupby key.

`operational_issue_days` in `forecast_skill_eval/src/forecast_skill_eval/pairs.py` is a whitelist
filter that selects which issue dates are used for skill evaluation, not a lead stratifier. It does
not replace per-lead grouping and is not relevant to this fix.

n_pairs is **not** starved: ~115k+ rows per model across 4 leads means approximately 28k–32k rows
per lead pair across all stations and years — comfortably above the n_pairs >= 2 floor (commit
`0f62c1ad`). The floor still applies to each per-lead row after the split.

---

## Problem

`calculate_monthly_skill_metrics` (`skill_metrics.py:1142`) groups by
`["month_in_year", "code", "model_short"]` at every point-metric, CRPS, and aggregation site.
Because real models emit 4 distinct leads, every `(month, code, model)` group currently contains a
mix of lead-0, lead-1, lead-2, and lead-3 forecasts. Consequences:

- **Per-model skill rows** report a misleading NSE/MAE/CRPS that is an unintended average across
  leads with different accuracy profiles.
- **EM / Naive Mean / Skilled Mean aggregation groupbys** (`skill_metrics.py:1285`, `:1424`,
  `:1607`) group by `["year", "month", "code"]` — they average across all models AND all leads of
  the same model simultaneously, producing wrong ensemble values before any skill is computed.
- **Baseline overload:** baselines in `long_forecasts` store `horizon_value = target month (1–12)`,
  not a lead. Skill grouping must derive the lead from the real-model groupby and carry it through
  baseline computation; trusting stored baseline `horizon_value` is incorrect.
- **API writer** (`api_writer.py:593, 596`) deduplicates on
  `(code, model_type, _date, month_in_year)` — for two rows with the same target month but
  different leads, this silently discards all but the last lead before the API write.
- **DB schema** has no `horizon_value` column on `skill_metrics` (`models.py:206`). Even if the
  calculator produced per-lead rows, they would collide on the unique constraint
  `uq_skill_metrics_horizon_code_model_date_horizon` (`models.py:228-234`).

---

## Desired Outcome

- Month skill rows are keyed `[month_in_year, horizon_value, code, model_short]` — one row per
  lead per calendar month per station per model.
- EM / Naive Mean / Skilled Mean are computed per-lead (the aggregation groupby includes
  `horizon_value` before averaging).
- The `skill_metrics` DB table and unique constraint include `horizon_value`.
- The API writer includes `horizon_value` in the payload and deduplicates including it.
- `read_monthly_skill_metrics` (`data_reader.py:289`) returns lead-aware rows; the dashboard tile
  shows the single operationally-current lead (decide: lead-0 or lead-1 per deployment config,
  mirror season's approach of selecting a single `season_in_year` value).
- `n_pairs < 2` rows are still dropped (`skill_metrics.py:1373`).
- Existing season / quarter / pentad / decade behaviour is unchanged.

---

## Technical Analysis

> Every file:line below was verified against the working tree on the
> `develop_forecast_skill_eval` branch.

### Coordination (REQUIRED before any service edit)

`sapphire/services/postprocessing` is colleague-owned. The schema change (`models.py`,
`schemas.py`, `crud.py`, new alembic migration) must be agreed with the owner before
implementation. Propose:

1. `horizon_value: int | None = None` added to `SkillMetricBase` initially (Optional so existing
   pentad/decade/quarter/season payloads keep validating; a required `int` would 422 every current
   skill write on deploy).
2. Staged NOT NULL migration as described in Implementation.
3. Sentinel value `0` agreed for non-month horizons (pentad, decade, quarter, season).
4. Cron pause window agreed for migration + backfill per environment.

### Service (`sapphire/services/postprocessing/app/` — colleague-owned)

**`models.py:196`** — `class SkillMetric` (`__tablename__ = "skill_metrics"`):
- `:206` — `horizon_in_year = Column(Integer, nullable=False)` — **no `horizon_value` column**.
- `:224-235` — `__table_args__` with `UniqueConstraint("horizon_type", "code", "model_type",
  "date", "horizon_in_year", name="uq_skill_metrics_horizon_code_model_date_horizon")`.
- Baseline migration: `alembic/versions/34b227f37299_baseline.py:150` (revision `34b227f37299`,
  `down_revision = None`).

**`schemas.py:161`** — `class SkillMetricBase(BaseModel)`. Current fields include
`horizon_in_year: int` at `:166`; **no `horizon_value`**. Inheritance chain:
- `SkillMetricCreate(SkillMetricBase)` at `:183` — inherits directly.
- `SkillMetricBulkCreate(BaseModel)` at `:187-188` wraps `data: list[SkillMetricCreate]`
  (does NOT inherit base directly; picks up `horizon_value` via `SkillMetricCreate`).
- `SkillMetricResponse(SkillMetricBase)` at `:191` — inherits directly.

Adding `horizon_value: int | None = None` to `SkillMetricBase` (`:161`) propagates to all three;
no separate schema edits needed.

**`crud.py:277-325`** — `create_skill_metric` is a **single manual Python upsert serving ALL
horizons** (pentad / decade / month / quarter / season). This is **not**
`insert().on_conflict_do_update()`. The match tuple is built in **four coupled spots** that must
all change together:

- `:281` — `keys = {(i["horizon_type"], i["code"], i["model_type"], i["date"], i["horizon_in_year"]) …}`
- `:283-284` — `existing_map` comprehension keyed `(r.horizon_type, r.code, r.model_type, r.date, r.horizon_in_year): r`
- `:286-289` — `tuple_(SkillMetric.horizon_type, SkillMetric.code, SkillMetric.model_type, SkillMetric.date, SkillMetric.horizon_in_year).in_(keys)`
- `:296` — per-row `key = (data["horizon_type"], data["code"], data["model_type"], data["date"], data["horizon_in_year"])`

**CRITICAL cross-horizon NULL-tuple hazard:** A `tuple_`-IN comparison where any element is `NULL`
evaluates to `NULL` (never `TRUE`) in both SQLite and Postgres. If `horizon_value` is added to
this tuple and any non-month skill writer sends `NULL`, that row never matches its existing row and
pile-up-inserts duplicate rows on every recalc — **silent corruption for ALL horizons**. Resolution:
make `horizon_value` NOT NULL everywhere (month → actual lead; pentad/decade/quarter/season →
agreed sentinel `0`), backfill legacy rows, then include in the constraint and all four match spots.

### Apps — calculator, writer, reader

**`skill_metrics.py:1142`** — `calculate_monthly_skill_metrics`. All groupby and merge-key sites
currently use `["month_in_year", "code", "model_short"]` (no lead awareness):

| Line | Site | Current key | Fix needed |
|------|------|-------------|-----------|
| `:1206` | Raw point-metric groupby | `["month_in_year","code","model_short"]` | Add `"horizon_value"` |
| `:1220` | CRPS for-loop groupby | `("month_in_year","code","model_short")` | Add `"horizon_value"` |
| `:1245-1248` | CRPS→point merge `on=` | `["month_in_year","code","model_short"]` | Add `"horizon_value"` |
| `:1258` | `merge_keys` for EM filter | `["month_in_year","code","model_short"]` | Add `"horizon_value"` |
| `:1285` | **EM aggregation groupby** | `["year","month","code"]` | Add `"horizon_value"` — **must change FIRST** |
| `:1309` | EM skill groupby | `["month_in_year","code","model_short","composition"]` | Add `"horizon_value"` |
| `:1325` | EM CRPS for-loop groupby | `["month_in_year","code","model_short"]` | Add `"horizon_value"` |
| `:1345-1347` | EM CRPS merge `on=` | `["month_in_year","code","model_short"]` | Add `"horizon_value"` |
| `:1424` | **Naive aggregation groupby** | `["year","month","code"]` | Add `"horizon_value"` — **must change FIRST** |
| `:1453-1455` | Naive skill groupby | `["month_in_year","code","model_short","composition"]` | Add `"horizon_value"` |
| `:1469` | Naive CRPS for-loop groupby | `["month_in_year","code","model_short"]` | Add `"horizon_value"` |
| `:1489-1492` | Naive CRPS merge `on=` | `["month_in_year","code","model_short"]` | Add `"horizon_value"` |
| `:1558` | SM `qualifying_keys` | `["month_in_year","code","model_short"]` | Add `"horizon_value"` |
| `:1561-1563` | SM filter merge `on=` | `["month_in_year","code","model_short"]` | Add `"horizon_value"` |
| `:1571-1573` | SM weights merge `on=` | `["month_in_year","code","model_short"]` | Add `"horizon_value"` |
| `:1607` | **SM aggregation groupby** | `["year","month","code"]` | Add `"horizon_value"` — **must change FIRST** |
| `:1636-1638` | SM skill groupby | `["month_in_year","code","model_short","composition"]` | Add `"horizon_value"` |
| `:1652` | SM CRPS for-loop groupby | `["month_in_year","code","model_short"]` | Add `"horizon_value"` |

**Ordering requirement:** the aggregation groupbys (`:1285` / `:1424` / `:1607`) must gain
`horizon_value` **before** their downstream skill groupbys. A downstream groupby cannot include
`horizon_value` if the aggregation DataFrame never produced it — that is a guaranteed `KeyError`.

**Recommendation:** introduce two shared constants at module scope:
```python
GROUP_COLS = ["month_in_year", "horizon_value", "code", "model_short"]
ENSEMBLE_KEY = ["year", "month", "horizon_value", "code"]
```
Thread `GROUP_COLS` through all 18 sites in the table above; replace `["year","month","code"]`
groupbys with `ENSEMBLE_KEY`. Make fixed-length tuple unpacks (e.g. `for (miy, code, model), grp`)
arity-safe after adding the lead dimension (they become 4-tuples).

**Baseline overload handling:** baselines in `long_forecasts` store `horizon_value = target month
(1–12)`, not a lead. The per-lead baseline must be derived by grouping real models by lead (carry
`horizon_value` through the `ENSEMBLE_KEY` groupby onwards). Do not trust the stored baseline
`horizon_value` for skill grouping. This is analogous to how season carries `season_in_year`
through `time_group_cols` rather than trusting stored values.

---

**`data_reader.py:1111-1132`** — `_normalize_monthly_forecasts`: calls `df.copy()`, renames
`model_type → model_short`, coerces `code` to string — but **does not normalize `horizon_value`**.
A NaN lead (legacy / NULL row from the API) is silently dropped by any subsequent `groupby`.

Fix before any groupby on `horizon_value`:
```python
df["horizon_value"] = df["horizon_value"].fillna(0).astype(int)
```

---

**`data_reader.py:289-317`** — `read_monthly_skill_metrics`: returns rows keyed
`[month_in_year, code, model_short, …]` with no lead awareness. After the schema change the API
returns 4 rows per `(month, code, model)`. The month dashboard tile would duplicate or select
arbitrarily.

Fix: decide and document which lead the dashboard headline shows (recommend: expose `horizon_value`
from the reader and let the caller filter to the operationally-current lead, mirroring how season
selects `season_in_year == 1`). Pinning the row count per `(month, code, model)` to 1 in tests
will catch regressions.

---

**`api_writer.py:421`** — `_write_skill_metrics_to_api`:
- `:482` — sets `horizon_in_year_col = "month_in_year"` for month (correct, unchanged).
- `:593` — `upsert_key = ["code", "model_type", "_date", horizon_in_year_col]` — excludes
  `horizon_value`. Two rows with different leads but the same target month share the same key.
- `:596` — `df_rec.drop_duplicates(subset=upsert_key, keep="last")` — silently discards all but
  the last lead before the API write.
- `:630-640` — `records_df` payload includes `horizon_in_year` but **no `horizon_value`**.

Fixes:
1. Add `"horizon_value"` to `upsert_key` at `:593`.
2. The `:596` dedup no longer collapses distinct leads (distinct `horizon_value` means different key).
3. Add `"horizon_value": df_rec["horizon_value"].astype(int)` to `records_df` at `:630-640`.

---

**`recalculate_skill_metrics.py:271`** — calls `read_monthly_forecasts` in a single call; the
calculator splits internally by `horizon_value`. **No per-lead read loop is needed in the
recalculator.** (Season loops only because it reuses `season_in_year` derived from `horizon_value`;
month does not need this.) Do not add a spurious loop.

---

## Implementation Plan

### Phase 1 — Service schema (colleague-coordinated; must land before Phase 2)

| File | Change |
|------|--------|
| `sapphire/services/postprocessing/app/schemas.py:161` | Add `horizon_value: int \| None = None` to `SkillMetricBase`. Propagates to `SkillMetricCreate` (`:183`), `SkillMetricBulkCreate` (via `SkillMetricCreate`), and `SkillMetricResponse` (`:191`) automatically. |
| `sapphire/services/postprocessing/app/models.py` | Add nullable `horizon_value` column (see migration below); drop + recreate unique constraint. |
| `sapphire/services/postprocessing/app/crud.py:277-325` | Add `horizon_value` to all four match-tuple spots (`:281`, `:283-284`, `:286-289`, `:296`). |
| `sapphire/services/postprocessing/alembic/versions/<new>.py` | Staged migration (see migration ops). |
| `sapphire/services/postprocessing/tests/` | Service-level upsert tests (see Testing). |

### Phase 2 — Apps calculator, writer, reader (after Phase 1 deployed + sentinels backfilled)

| File | Change |
|------|--------|
| `apps/postprocessing_forecasts/src/skill_metrics.py:1142+` | Introduce `GROUP_COLS`/`ENSEMBLE_KEY` constants; update all 18 groupby/merge sites in the table above. Fix aggregation groupbys (`:1285`, `:1424`, `:1607`) before downstream skill groupbys. Make tuple-unpacks arity-safe. |
| `apps/postprocessing_forecasts/src/api_writer.py:593,596,630-640` | Add `horizon_value` to `upsert_key` (`:593`), include in `records_df` payload (`:630-640`); `:596` dedup no longer collapses leads. |
| `apps/postprocessing_forecasts/src/data_reader.py:1111-1132` | `fillna(0).astype(int)` on `horizon_value` in `_normalize_monthly_forecasts` before any groupby. |
| `apps/postprocessing_forecasts/src/data_reader.py:289-317` | Make `read_monthly_skill_metrics` lead-aware; expose `horizon_value` or filter to single operational lead for dashboard. |

### Migration ops — exact order (and the lock warning)

```python
# upgrade()

# Step 1: add nullable column with server_default so in-flight writes during
#         the migration window do not 500
op.add_column(
    "skill_metrics",
    sa.Column("horizon_value", sa.Integer(), nullable=True, server_default="0"),
)

# Step 2: horizon-aware backfill
#   - Legacy MONTH rows: set to the operational lead the month writer will emit
#     after Phase 2 (MUST match exactly or first post-deploy recalc creates duplicate rows)
#   - All other horizons: set to sentinel 0
#   Run as a batch UPDATE to avoid lock contention on large tables.
op.execute(
    "UPDATE skill_metrics SET horizon_value = 0 WHERE horizon_value IS NULL"
)

# Step 3: promote to NOT NULL after backfill is complete
op.alter_column("skill_metrics", "horizon_value", nullable=False)

# Step 4: swap unique constraint
#   alembic cannot extend a constraint in-place; must DROP old and CREATE new.
op.drop_constraint(
    "uq_skill_metrics_horizon_code_model_date_horizon",
    "skill_metrics",
    type_="unique",
)
op.create_unique_constraint(
    "uq_skill_metrics_horizon_code_model_date_horizon_value",
    "skill_metrics",
    ["horizon_type", "code", "model_type", "date", "horizon_in_year", "horizon_value"],
)

# downgrade()
# Drop new constraint + column only.
# Do NOT recreate the old narrower unique constraint if per-lead rows now exist —
# that would raise on duplicate (horizon_type,code,model_type,date,horizon_in_year)
# combinations.
op.drop_constraint(
    "uq_skill_metrics_horizon_code_model_date_horizon_value",
    "skill_metrics",
    type_="unique",
)
op.drop_column("skill_metrics", "horizon_value")
```

**Lock / downtime notes:**
- `ALTER COLUMN … NOT NULL` (full-table scan) and `CREATE UNIQUE CONSTRAINT` (index build) both
  take **ACCESS EXCLUSIVE** on `skill_metrics` on deployed Postgres. Schedule a maintenance window;
  the table is large after historical recalc.
- **Pause the operational and recalc cron** during migrate + backfill on each environment.
  Reference: `doc/prod/historical_backfill_runbook.md`.
- Run migration + backfill + month-skill-recalc **once per environment** (kyg, taj, uzb).
- The backfill value for existing MONTH rows must exactly equal what the apps writer will emit
  post-Phase-2. A mismatch causes the first post-deploy recalc to insert parallel duplicate rows
  (idempotency trap).

### Implementation Steps

- [ ] **(Coordinate)** Agree schema contract, sentinel value (`0`), migration staging, and
      cron-pause window with service owner before writing any code.
- [ ] Service Phase 1: add nullable `horizon_value` column + `int | None = None` to
      `SkillMetricBase`. Deploy; verify no 422 on existing pentad/decade/quarter/season skill writes.
- [ ] All apps skill writers emit a concrete `horizon_value` (month → lead from `horizon_value`
      column in the forecast DataFrame; pentad/decade/quarter/season → sentinel `0`).
- [ ] Run backfill UPDATE on each environment DB. Confirm `horizon_value IS NOT NULL` for all rows.
- [ ] NOT NULL migration + constraint swap + all four crud match spots (`:281`, `:283-284`,
      `:286-289`, `:296`).
- [ ] Apps Phase 2: introduce `GROUP_COLS` / `ENSEMBLE_KEY`; fix aggregation groupbys (`:1285` /
      `:1424` / `:1607`) first, then downstream skill groupbys and helper CRPS loops and merges.
- [ ] `api_writer` upsert_key + payload + `:596` dedup fix.
- [ ] `data_reader` `fillna+int-cast` in `_normalize_monthly_forecasts`; lead-aware read-back in
      `read_monthly_skill_metrics`; decide dashboard headline (single operational lead).
- [ ] Recalc month skill locally; verify per-lead rows; confirm n_pairs per group; confirm tile
      shows single row per `(month, code, model)`.

---

## Testing

### Service-level test cases

- [ ] **Two rows differing only in `horizon_value` both persist.** Call `create_skill_metric` with
      lead-0 and lead-1 rows for the same `(horizon_type, code, model_type, date, horizon_in_year)`.
      Assert `db.query(SkillMetric).count() == 2`. Verify the manual upsert path (NOT
      `insert().on_conflict_do_update()`).
- [ ] **Re-upsert same row stays at count 1.** Upserting the same `(…, horizon_value=1)` row twice
      must leave exactly one row (update, not insert).
- [ ] **NULL-tuple hazard.** Row with `horizon_value = 0` exists; incoming payload also has
      `horizon_value = 0`. Must match and update (no duplicate insert). Cover both SQLite (service
      tests) and document Postgres semantics in a comment.
- [ ] **Existing non-month payload without `horizon_value` validates.** A `SkillMetricBulkCreate`
      payload for pentad omitting `horizon_value` must not raise a 422 while the field is
      `int | None`.

### Apps-level test cases

- [ ] **Per-lead month skill rows.** Feed `calculate_monthly_skill_metrics` with two forecast sets
      for the same `(month, code, model)` at lead-0 and lead-1. Assert output contains exactly 2
      rows for that `(month, code, model)` with `horizon_value ∈ {0, 1}`. Both pass n_pairs >= 2.
- [ ] **EM / Naive / Skilled-Mean computed per lead.** Three forecasts for the same target month,
      two distinct leads, from two models: assert that `model_short == "EM"` rows in the output
      have one row per lead value present in the input (not one pooled row).
- [ ] **NaN horizon_value not dropped.** `_normalize_monthly_forecasts` with a row carrying
      `horizon_value = NaN` must appear in groupby output after `fillna(0).astype(int)`.
- [ ] **API writer emits horizon_value.** The `records_df` payload for month includes
      `horizon_value`; the `upsert_key` includes it; two rows differing only in lead are NOT
      collapsed by `drop_duplicates`.
- [ ] **Read-back single-rowed.** After the schema change, assert that `read_monthly_skill_metrics`
      returns exactly one row per `(month, code, model)` for the dashboard tile (row count equals
      the number of unique `(month, code, model)` combinations).
- [ ] **Regression — other horizons unchanged.** Season / quarter / pentad / decade skill rows are
      unaffected; sentinel `horizon_value = 0` on those rows does not alter their recalc results.

### n_pairs check before releasing

With ~115k+ rows per real model across 4 leads, n_pairs per `(month, code, model, lead)` group
scales with years of archive (~one observation per year per calendar month). Verify no group falls
below the floor (`skill_metrics.py:1373`) before declaring the split acceptable.

### Testing Commands

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts
bash run_tests.sh service:postprocessing
```

### Manual Verification

After Phase 2 recalc on a local DB holding real month forecast data:

```sql
-- 1. Four leads per real model
SELECT model_type, horizon_value, COUNT(*)
FROM   skill_metrics
WHERE  horizon_type = 'MONTH'
  AND  model_type NOT IN ('NAIVE_MEAN','SKILLED_MEAN','ENSEMBLE_MEAN')
GROUP  BY model_type, horizon_value
ORDER  BY model_type, horizon_value;

-- 2. No remaining cross-lead pooling (no row should now have n_pairs >> archive_years)
SELECT MIN(n_pairs), MAX(n_pairs), AVG(n_pairs)
FROM   skill_metrics
WHERE  horizon_type = 'MONTH';
```

Confirm dashboard month tile shows exactly one row per `(month, code, model)`.

---

## Documentation Impact

- [ ] `apps/postprocessing_forecasts/README.md` — month skill keying by lead.
- [ ] `doc/data_flow_long_term.md` — month skill keyed by `horizon_value` (leads 0–3 for real
      models; sentinel 0 for baselines).
- [ ] Service migration/deploy notes (`doc/prod/`) — alembic staged ops, ACCESS EXCLUSIVE lock
      window, sentinel value, per-environment recalc order, downgrade caveat.
- [ ] Claude memory — record confirmed premise and Phase completion checkpoints.

---

## Out of Scope

- Quarter multi-lead stratification (operationally single-lead, unchanged).
- Pentad / decade skill keying (unchanged; only the shared sentinel touches them).
- Dashboard "which lead to display" product decision beyond confirming the tile shows exactly one
  row per `(month, code, model)` after the change.
- `recalculate_skill_metrics.py` per-lead read loop — not needed; month is read in one
  `read_monthly_forecasts` call and split internally.

## Dependencies

- **Service-owner coordination** — schema contract, sentinel value, migration staging, and cron-pause
  window must be agreed before any implementation.
- **Phase 1 deployed + sentinels backfilled** before Phase 2 apps changes go to production.
- **Migration + sentinel backfill** must run on all deployed postprocessing DBs (kyg, taj, uzb)
  before the per-lead writer is enabled.
- **Cron pause** during migration + backfill window on each environment.

## Risks & Unknowns

- **Cross-horizon NULL-tuple corruption.** `create_skill_metric` is the single write path for ALL
  horizons; adding `horizon_value` to the match tuple without making it NOT NULL causes
  pile-up-inserts on every recalc for non-month horizons. Mitigated by the NOT NULL + sentinel
  design.
- **Baseline overload.** Stored baseline `horizon_value` = target month (1–12), not lead. If the
  calculator trusts stored baseline `horizon_value`, the groupby key is wrong. Mitigated by
  deriving lead from real-model grouping and carrying it through.
- **Display regression.** Lead-aware rows make `read_monthly_skill_metrics` return 4 rows per
  `(month, code, model)`; the tile must select one. Must be decided before Phase 2 ships.
- **Migration lock.** Unique-index build and SET NOT NULL take ACCESS EXCLUSIVE on deployed
  Postgres. Needs a maintenance window.
- **Backfill sentinel mismatch.** The backfill value for existing MONTH rows must exactly equal
  what the writer emits post-Phase-2. A mismatch causes duplicate rows on the first recalc.
- **Downgrade hazard.** The `downgrade()` step must NOT recreate the old narrower unique constraint
  if per-lead rows now exist — it would fail on duplicate `(horizon_type,code,model_type,date,
  horizon_in_year)` combinations.

## Acceptance Criteria

- [ ] `SELECT DISTINCT horizon_value FROM skill_metrics WHERE horizon_type = 'MONTH' AND model_type NOT IN ('NAIVE_MEAN','SKILLED_MEAN','ENSEMBLE_MEAN')` returns `{0, 1, 2, 3}` — four distinct lead values.
- [ ] No two real-model month skill rows share the full unique key
      `(horizon_type, code, model_type, date, horizon_in_year, horizon_value)`.
- [ ] Non-month skill rows are unchanged; sentineled at `horizon_value = 0`; no duplicate inserts
      from the NULL-tuple hazard.
- [ ] EM / Naive Mean / Skilled Mean month rows carry a lead (`horizon_value ∈ {0,1,2,3}`) derived
      from real-model grouping, not a target-month number.
- [ ] `n_pairs >= 2` enforced per-row; per-lead n_pairs quantified and confirmed acceptable.
- [ ] Dashboard month skill tile shows exactly one row per `(month, code, model)`.
- [ ] `run_tests.sh postprocessing_forecasts` and `run_tests.sh service:postprocessing` green,
      zero unexpected skips.
- [ ] Migration (staged add → backfill → NOT NULL → constraint-swap), lock window, sentinel value,
      per-environment recalc order, and downgrade caveat documented for deployment.

---

## References

- DB query target: `long_forecasts` (postprocessing DB, `horizon_type = 'MONTH'`).
- Service: `sapphire/services/postprocessing/app/models.py:196-236`,
  `schemas.py:161-199`, `crud.py:277-325`,
  `alembic/versions/34b227f37299_baseline.py:129-153` (baseline migration, `down_revision=None`).
- Apps calculator: `skill_metrics.py:1142,1206,1220,1245,1258,1285,1309,1325,1345,1373,
  1424,1453,1469,1489,1558,1561,1571,1607,1636,1652`.
- Apps writer: `api_writer.py:421,482,593,596,630-640`.
- Apps reader: `data_reader.py:289-317,1111-1132`.
- Recalculator: `recalculate_skill_metrics.py:271` (single `read_monthly_forecasts` call;
  no per-lead loop needed).
- n_pairs floor: commit `0f62c1ad`.
- Deploy ops: `doc/prod/historical_backfill_runbook.md`.
