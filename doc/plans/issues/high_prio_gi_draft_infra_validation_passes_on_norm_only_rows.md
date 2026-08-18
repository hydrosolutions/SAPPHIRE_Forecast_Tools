## `validate_pipeline` reports PASS + "fresh" for a dataset whose values are all NULL (INFRA-026)

**Status**: Draft (2026-08-14)
**Module**: `apps/validate_pipeline` (`check_data_freshness`, `check_snow_operational_values`,
Tier-1 presence checks)
**Priority**: **High** — this is the tool the review process trusts to say whether a module
did its job. It currently certifies a total upstream outage as healthy.
**Labels**: `infra`, `validate_pipeline`, `false-pass`, `data-quality`
**Found**: 2026-08-14, local kghm review on `maxat_sapphire_2` @ `8e3fc1bc`, during § 3
`preprocessing_gateway`.
**Related**: INFRA-020 (ML validation matches zero checks and reports PASS on no evidence) —
**same failure family**: a check that cannot fail is worse than no check. **INFRA-025** (package
shadowing, which was hiding all validation output until cleared in that working copy).
*Id corrected 2026-08-18: this line previously said INFRA-023, which is an unrelated issue — the
yearly `monthly_norms` cron mapping (`module_issues.md:60`). Shadowing is INFRA-025 (`:117`).*

> **Provenance correction (2026-08-16).** The checkout moved from `maxat_sapphire_2` to
> `fix_lr010_lr011_write_contract` at **2026-08-14 16:00** (git reflog), so every run from the
> full-history recalc onward executed on that branch (now `849c8736`), **not** on trunk as the
> line above states. That branch's diff vs trunk touches only
> `apps/linear_regression/linear_regression.py`, `apps/iEasyHydroForecast/forecast_library.py`,
> their tests, and docs — **none of the files this issue concerns** — so the finding holds
> identically on trunk. Recorded for accuracy of the audit trail, not because the conclusion changes.

---

## Observation — three PASSes over a complete failure

`preprocessing_gateway` ran with **all six snow tasks failing**:

```
ERROR - Error getting snow data from Data Gateway for HRU <X>, SWE: … 
        {"message": "Operational data for HRU <X> is not available for date 2026-08-09 00:00:00!", "success": false}
ERROR - Failed to get snow data for HRU <X>, SWE
   … (x6: SWE, HS, RoF for two HRUs)
INFO  - Snow data processing complete (6 tasks)
```

Yet the run reported:

| Signal | Reported | Reality |
|---|---|---|
| Module exit | **PASS**, exit 0 | 6/6 snow tasks errored |
| `Snow (SWE)` Tier-1 | **OK — 80 records** | rows exist, but `value` is NULL from 2026-08-01 on |
| `Snow operational values` | **OK — 80 records, years: [2026]** | passes because dates aren't year-2000; never inspects `value` |
| `Data freshness` | **OK — all 3 datasets fresh (threshold=3d)** | snow's last non-null value is **2026-07-31**, i.e. 14 days stale |

Measured directly against the API, both stations, window 2026-07-25..08-29:

```
meteo T : rows=36  non-null=35  last non-null 2026-08-28   (14 non-null days AFTER today — extension works)
meteo P : rows=36  non-null=35  last non-null 2026-08-28   (14 non-null days AFTER today)
snow SWE: rows=36  non-null= 7  last non-null 2026-07-31   ( 0 non-null days after today)
```

A representative stale row: `{'snow_type':'SWE','date':'2026-08-29','value':None,'norm':0.048,…}`.

## Root cause

The preprocessing store carries **norm-only placeholder rows**: a row exists for every date
with `norm` populated and `value` NULL. Every check above counts **rows**, never
**non-null values**:

- `check_data_freshness` (`validate_pipeline.py:1018`) computes lag from each Tier-1
  result's `max_date`, and `max_date` comes from the row dates. Placeholder rows extend to
  2026-08-29, so lag is negative and the dataset is "fresh".
- `check_snow_operational_values` tests that dates are not year-2000 (the PREPG-003
  signature). Placeholder rows are current-year, so it passes.
- Tier-1 presence tests `len(records) > 0`.

The meteo comparison is what makes this unambiguous: same shape, same check, but meteo
genuinely has values — so the checks cannot distinguish "extended successfully" from
"placeholder rows only". **Both report OK.**

## Why it matters

This is the automated gate that `doc/dev/review_checklist_local_template.md` § 0.4/§ 9a
offers as the alternative to manual inspection, and that operators are pointed at after a
deployment. As written, a complete upstream data outage — the exact thing it exists to
catch — produces a clean green run.


## Additional instances found 2026-08-16/17 (kghm + tjhm) — same family, same fix direction

The original report covered **rows present, values all NULL**. Two further mechanisms produce
the identical outcome — absence reported as health — and belong in the same fix:

### (2) An absent dataset is *excluded* from freshness rather than failing it

kghm, 2026-08-17: `preprocessing_runoff` wrote nothing (source had no weekend data), so the
runoff check produced **no `max_date`**, so runoff was **dropped from the freshness
evaluation** — leaving:

```
[FAIL] Runoff (day): no records
[OK]   Data freshness: all 1 datasets fresh (threshold=3d)     <- only hydrograph evaluated
```

Freshness reports "all fresh" *because* the primary dataset is missing. A dataset with no rows
must be **stale/failed for freshness purposes**, not silently removed from the denominator.

### (3) Per-station absence is concealed by an aggregate count

Observed twice in one day on tjhm:

- `[OK] Runoff (day): 22 records` **passed** while a configured station had **zero rows ever**
  (2000-01-01..2026-08-17).
- `[OK] Meteo (T): 80 records`, `[OK] Snow (SWE): 80 records`, `[OK] Data freshness: all 3
  datasets fresh` **passed** while the selected station had **zero meteo and zero snow rows**.

Measured coverage: **3 of 18** ML-configured tjhm stations have no meteo, **1 of 18** has no
runoff at all — none of which is visible in a total.

### Why (3) matters more than it looks — the emergent blind spot

A station configured as ML-available but lacking inputs is invisible at **every** layer:

| Layer | Behaviour |
|---|---|
| `preprocessing_*` | writes nothing, no per-station complaint |
| `machine_learning` | produces no forecast, no error |
| `fill_ml_gaps` (maintenance) | reports **"No missing forecasts"** — not in the gap universe |
| `validate_pipeline` | aggregate row counts **PASS** |
| `run_locally.sh` summary | **PASS** |

No single component is wrong; the gap is emergent. **The per-cell `(station, model, date)`
matrix is the only layer that would catch it** — which is the concrete justification for that
requirement, previously argued only on principle.

### Consequence for the fix

The proposed fix section must therefore cover **three** rules, not one:

1. rows present + values all NULL ⇒ FAIL (original);
2. dataset absent ⇒ **stale/failed for freshness**, never excluded from the denominator;
3. presence is asserted **per expected cell**, and missing cells are named — an aggregate total
   is a summary, not a verification.


## Proposed fix

1. Tier-1 presence checks should report **both** a row count and a **non-null value count**,
   and fail/warn when non-null is 0 despite rows existing.
2. `check_data_freshness` must derive `max_date` from rows with a **non-null value**, not
   from all rows. (Keep the existing behaviour available for genuinely value-less datasets
   if any exist — state which.)
3. `check_snow_operational_values` should assert non-null `value` coverage in the recent
   window, not only that dates are not year-2000.
4. A module whose sub-tasks all errored should not report PASS — see the companion issue
   PREPG-009.

## Acceptance criteria

- A fixture with rows present but all `value` NULL produces **FAIL/WARN**, not PASS, for
  presence, freshness and the snow operational check.
- A fixture with genuine values still passes, byte-identically to today.
- The reported message distinguishes "N records, M with values" so a reviewer can see the
  difference without a separate query.
- Re-running today's kghm scenario flags snow as stale while leaving meteo green.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh validate_pipeline` green.

## Contract not to break

- Norm-only rows are **legitimate data** (climatological norms are a real product); the fix
  must not delete or reject them, only stop counting them as observations.
- `run_module_validation()` returning 0 is intentional (do not abort a pipeline mid-run);
  this fix changes what is *reported*, not the exit contract.
