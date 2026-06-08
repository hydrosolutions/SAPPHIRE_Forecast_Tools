# GitHub Issue: PR-QHN-001

**Title**: `feat(preprocessing_runoff): add QUARTER hydrograph norms to preprocessing DB`

**Labels**: `enhancement`, `preprocessing_runoff`, `sapphire-services-coordination`, `high-priority`

**Assignees**: max, mabesa

**Status**: Apps-side implemented (commits `df8b424`, `8a27768` on
`develop_preprocessing_runoff_quarterly_hydrograph_norms`). Service-side already shipped by Max
(`2be58f7`, migration `d4e5f6a7b8c9`). **Blocked on Owner B** (`sapphire-api-client`) before
end-to-end verification and deploy.

---

## Summary

The SAPPHIRE 2 `hydrographs` table (preprocessing) supports horizons `{DAY, PENTAD, DECADE, MONTH,
SEASON, YEAR}`. The postprocessing `long_forecasts` table additionally uses `QUARTER`, populated
across 2006–2026 for four calendar quarters and multiple models. Operational dashboards that display
long-term quarterly forecasts therefore have no climatology row (`hydrographs.norm`) to compare
against — the QUARTER norm baseline is missing.

This issue adds QUARTER as a first-class horizon in `hydrographs`: enum value, aggregation logic
(mean of three constituent monthly norms, mirroring `SEASON`), wiring into the existing annual
aggregation job, and tests. **Norm-only** — no stat aggregates (`count/mean/std/min/max/q05..q95`),
matching the existing `SEASON` and `MONTH` patterns.

---

## Resolved design decisions

| # | Question | Resolution | Rationale |
|---|---|---|---|
| OQ1 | `day_of_year` convention | **Leap-aware** `dt.date(year, q_start_month, 1).timetuple().tm_yday` (Q1=1; Q2=91/92; Q3=182/183; Q4=274/275). **Diverges from the original static-lookup draft.** | The quarter row's `date` is the first-of-quarter date, so a leap-aware DOY keeps `date` and `day_of_year` internally consistent (e.g. 2024-04-01 → DOY 92, not 91). It also matches the existing `SEASON` record, which uses leap-aware `tm_yday` for the same Apr-1 date. `MONTH`'s static `MID_MONTH_DOY` is a separate mid-month-label convention and not a precedent here. |
| OQ2 | Missing-month policy | **All-or-nothing**: if any of a quarter's three monthly values is NULL/non-finite, the aggregate is NULL. | Mirrors `SEASON`; avoids silently changing the denominator. Intentionally stricter than postprocessing's `QUARTER_MIN_MONTHS = 2` (2-of-3) tolerance. |
| OQ3 | Populate `previous`/`current`? | **Yes** — mean of the quarter's three monthly `previous`/`current`, all-or-nothing. | Consistency with `SEASON`'s intent. `norm` is climatology; `previous`/`current` are year-specific. |
| OQ4 | Cron cadence | **Once per year** (existing `0 3 1 1 *`). | `norm` is climatology and does not change intra-year. |
| — | `forecast_library.py` validators | **Dropped from scope** (originally proposed to "loosen now"). | Long-horizon hydrograph rows are written via `client.write_hydrograph` and read via `client.read_hydrograph` (dashboard `src/db.py`). `forecast_library.py` is **not** on this path — its three read guards (`~2775`, `~2857`, `~2992`) are a legacy CSV/forecast-run path that does not even wire `month`. Loosening it would add dead code with no consumer. |

---

## Ownership split

Three ownership domains. **A and B must land, and consumers must re-pin, before C's write/read path
can be verified end-to-end.**

### Owner A — `sapphire/services/preprocessing/` (colleague-managed, Max) — ✅ DONE
- `QUARTER = "quarter"` added to `HorizonType` in `app/models.py` (commit `2be58f7`), positioned
  between MONTH and SEASON. The enum is shared by `Runoff` and `Hydrograph` via bare
  `SQLEnum(HorizonType)`.
- Migration `alembic/versions/d4e5f6a7b8c9_add_quarter_to_horizontype.py` ships
  `ALTER TYPE horizontype ADD VALUE IF NOT EXISTS 'QUARTER' BEFORE 'SEASON'` (down_revision
  `9f1e72108f01`). **Note:** the Postgres enum stores the **uppercase** member name `'QUARTER'`,
  while the Python value is lowercase `"quarter"` (SQLAlchemy `native_enum` persists member names).
  Both are correct — do not "fix" the migration to lowercase.
- **Service-tests caveat:** tests use in-memory SQLite via `create_all`, never run Alembic, never
  exercise `ALTER TYPE`. A green service suite gives **zero** signal that the Postgres migration is
  correct — Max must validate against a real Postgres instance.
- **Still outstanding for A:** service-side tests covering a `quarter` hydrograph payload (none
  exist yet). We cannot add these (ownership boundary) — Max to decide.

### Owner B — `sapphire-api-client` (upstream library, external git repo) — 🚩 BLOCKER
- Add `"quarter"` to `VALID_HORIZONS` in `sapphire_api_client/validators.py`. Today:
  `{"day","pentad","decade","month","season","year"}` — no `quarter`. (`quarter` exists only in
  `VALID_LONG_FORECAST_HORIZONS`, which the hydrograph path does not use.)
- `write_hydrograph` **and** `read_hydrograph` validate against `VALID_HORIZONS`
  (`preprocessing.py:180`). Until this lands, both the preprocessing_runoff write and the dashboard
  read of `"quarter"` raise `ValueError` before reaching the API. **This gates both directions.**
- Tag a release / new commit; re-pin every consumer's `apps/*/pyproject.toml` (currently
  `a196e1728f2447ec416c77cd54c9d6899a86d9e6`).
- **Owner C's tests pass today via MagicMock and do not exercise this gate** — a green app suite is
  not end-to-end proof. No real write/read of `"quarter"` is valid until B lands and consumers
  re-pin.

### Owner C — `apps/preprocessing_runoff/` (our team) — ✅ IMPLEMENTED
- Quarterly aggregation helpers + orchestration hook in `sync_long_horizon_hydrograph.py`
  (commit `df8b424`).
- Tests in `apps/preprocessing_runoff/test/` (singular `test/`).
- Docs in `doc/data_flow_long_term.md` (commit `8a27768`).

---

## What was implemented (as-built, commit `df8b424`)

`apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`, purely additive:

```python
QUARTER_MONTHS = {1: (1, 2, 3), 2: (4, 5, 6), 3: (7, 8, 9), 4: (10, 11, 12)}

def _quarterly_field_mean(monthly_records, quarter, field) -> float | None:
    # mean of the 3 constituent monthly values, all-or-nothing (None if any
    # constituent month is missing from the input set or non-finite). Denominator 3.

def build_quarterly_records(monthly_records, code, target_year) -> list[dict]:
    # 4 records; date = first-of-quarter; day_of_year = leap-aware tm_yday;
    # horizon_value = horizon_in_year = quarter 1..4; norm/previous/current via
    # _json_safe(_quarterly_field_mean(...)); NO stat fields.

def write_station_quarterly_hydrograph(code, monthly_records, client, target_year, today) -> list[dict]:
    # mirrors write_station_seasonal_hydrograph; one client.write_hydrograph(records) call.
```

Hook in `write_long_horizon_hydrograph`, after the seasonal append:

```python
all_records.extend(
    write_station_quarterly_hydrograph(
        code=str(code), monthly_records=monthly_records,
        client=client, target_year=target_year, today=today,
    )
)
```

No existing function signature, data-flow, or control-flow was changed. One existing orchestrator
test expectation was updated (13 records / 2 write-calls → 17 / 3) to reflect the additive quarter
behaviour.

### QUARTER row shape on `hydrographs`

| Field | Q1 | Q2 | Q3 | Q4 |
|---|---|---|---|---|
| `horizon_type` | `"quarter"` | `"quarter"` | `"quarter"` | `"quarter"` |
| `date` | `YYYY-01-01` | `YYYY-04-01` | `YYYY-07-01` | `YYYY-10-01` |
| `day_of_year` (leap-aware) | 1 | 91 / 92 | 182 / 183 | 274 / 275 |
| `horizon_value` / `horizon_in_year` | 1 | 2 | 3 | 4 |
| `norm` | mean(Jan,Feb,Mar norms) | mean(Apr,May,Jun) | mean(Jul,Aug,Sep) | mean(Oct,Nov,Dec) |
| `previous` / `current` | mean of constituent monthly previous/current | … | … | … |
| stat fields | NULL | NULL | NULL | NULL |

---

## Acceptance criteria

1. `psql -d preprocessing_db -c "\dT+ horizontype"` lists `quarter` alongside the existing six. *(Owner A — done; verify on a real Postgres after migration.)*
2. One run of `bin/yearly_runoff_hydrograph_aggregation.sh` for target year `YYYY` produces exactly `4 × N` new `hydrographs` rows with `horizon_type='quarter'` for N stations. *(Requires Owner B.)*
3. Every QUARTER row has `count/mean/std/min/max/q05..q95` all NULL; `norm` non-NULL subject to the all-or-nothing rule.
4. For each station × quarter, `norm` equals the mean of that station's three constituent MONTH `norm` values within `1e-9`. SQL spot-check on station `19999`. *(Requires Owner B.)*
5. Dates follow `{YYYY-01-01, YYYY-04-01, YYYY-07-01, YYYY-10-01}`; `day_of_year` is leap-aware.
6. `horizon_value`, `horizon_in_year` ∈ `{1,2,3,4}`.
7. `SAPPHIRE_TEST_ENV=True bash run_tests.sh` passes with zero skips beyond the documented
   `sapphire-api-client` gate. *(Apps-side: ✅ 337 passed, 2 pre-existing unrelated skips in
   `test_src.py`.)*
8. `doc/data_flow_long_term.md` reflects the QUARTER aggregation step and the join contract.
   *(✅ commit `8a27768`.)*

---

## Consumer / join contract (for the deferred dashboard work)

A future dashboard join between preprocessing QUARTER hydrograph norms and postprocessing
`long_forecasts` QUARTER rows **must use period keys** (`code`, `horizon_type`, `horizon_value`),
**not** `date` or `day_of_year`: hydrograph norm rows are written for the current target year only
(no historical backfill), while `long_forecasts` span many years. The QUARTER period keys
(`date = YYYY-{01,04,07,10}-01`, `horizon_value` 1–4, `horizon_type` `"quarter"`) deliberately match
the postprocessing convention at `apps/postprocessing_forecasts/src/api_writer.py:1043-1051`, so no
translation layer is needed.

---

## Out of scope / non-goals

- Stat aggregates (count/mean/std/min/max/q05..q95) for QUARTER — norm-only mirrors SEASON.
- Populating/revising the `YEAR` horizon.
- Historical backfill of QUARTER rows for prior years (the annual job covers the current year).
- Changes to the postprocessing `long_forecasts` QUARTER handling.
- Adding QUARTER rows to the `runoffs` table (the shared enum permits it; nothing here produces or
  reads such rows).
- Loosening `forecast_library.py` validators (off the long-term-data path — see OQ resolution).
- Backfilling stat aggregates for existing SEASON/MONTH rows.

### Known inconsistencies (intentionally deferred)

- `apps/iEasyHydroForecast/forecast_library.py` rejects `"quarter"` in its read guards (`~2775`,
  `~2857`, `~2992`) and its year-column write path (`~3432`). Not a blocker — no consumer reads
  long-horizon hydrograph through this module. First future caller of `read_hydrograph_data("quarter")`
  there would throw.
- `apps/validate_pipeline/validate_pipeline.py` does not validate `quarter` (`--horizon` choices,
  tier-1 long-term checks). Pipeline-validation coverage for QUARTER is a follow-up.
- `data_migrator.py` day/pentad/decade chains are unaffected (QUARTER is norm-only, not CSV-migrated).

---

## Follow-up — PR-QHN-002 (dashboard long-horizon norm display)

No long-horizon norm (MONTH/SEASON/QUARTER) is read or plotted today (`src/db.py` returns empty
hydrograph frames; `data_manager.py:273` short-circuits long horizons). PR-QHN-002 should surface
all three. Latent quirks to fix there: `DataManager.horizon_in_year("quarter")` returns `None`
(diverges from `src/db.py:_horizon_in_year_col` → `"quarter_in_year"`); `get_long_forecasts_quarter`
dedups on `["code","model_short"]` and would need `quarter_in_year` in any 4-quarter norm join.

---

## Verify locally (once Owner B lands and consumers re-pin)

```bash
cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff   # apps-side, MagicMock
bash run_tests.sh service:preprocessing                                    # service (SQLite)
bash apps/run_locally.sh preprocessing_runoff                              # real end-to-end
psql -d preprocessing_db -c \
  "SELECT horizon_value, date, norm FROM hydrographs \
   WHERE code='19999' AND horizon_type='quarter' ORDER BY horizon_value;"
```

---

## References

- `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` — SEASON/MONTH/QUARTER aggregation.
- `sapphire/services/preprocessing/app/models.py` — `HorizonType` (QUARTER present, commit `2be58f7`).
- `sapphire/services/preprocessing/alembic/versions/d4e5f6a7b8c9_add_quarter_to_horizontype.py`.
- `sapphire_api_client/validators.py:12` — `VALID_HORIZONS` (needs `"quarter"`).
- `apps/postprocessing_forecasts/src/aggregation.py:217-282` — `QUARTER_MONTHS`.
- `apps/postprocessing_forecasts/src/api_writer.py:1043-1051` — QUARTER date/horizon_value convention.
- `apps/forecast_dashboard/src/db.py` — hydrograph reads via the api-client (`_read_data`).
- `doc/data_flow_long_term.md` — long-horizon hydrograph norm aggregation + join contract.
- `bin/yearly_runoff_hydrograph_aggregation.sh` — annual job entry point.
