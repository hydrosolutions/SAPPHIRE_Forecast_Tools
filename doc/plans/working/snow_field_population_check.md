# Snow Field Population Check — Phase 0 Go/No-Go Note

**Date produced:** 2026-05-31

---

## 1. Probe Summary

The probe was run against the local SAPPHIRE stack using URL form 1
(`http://localhost:8000/api/preprocessing/snow/`), which returned HTTP 200 on
the first connectivity test. URL forms 2 and 3 were not needed. Two date
windows were queried: the primary window `2025-01-01` to `2026-12-31` and an
alternate window `2024-01-01` to `2024-12-31`. Snow types checked: HS, ROF,
SWE. Station code used for the probe: `<redacted>`.

---

## 2. Per-Snow-Type Results

### Primary window: 2025-01-01 to 2026-12-31

| Snow type | Rows | mean | min | max | q05 | q25 | q50 | q75 | q95 | previous | current |
|-----------|------|------|-----|-----|-----|-----|-----|-----|-----|----------|---------|
| HS        | 669  | 0    | 0   | 0   | 0   | 0   | 0   | 0   | 0   | 0        | 0       |
| ROF       | 712  | 0    | 0   | 0   | 0   | 0   | 0   | 0   | 0   | 0        | 0       |
| SWE       | 712  | 0    | 0   | 0   | 0   | 0   | 0   | 0   | 0   | 0        | 0       |

All counts are **non-null count out of total rows**. Zero non-null for every
stat field across all three snow types.

### Alternate window: 2024-01-01 to 2024-12-31

| Snow type | Rows | mean | min | max | q05 | q25 | q50 | q75 | q95 | previous | current |
|-----------|------|------|-----|-----|-----|-----|-----|-----|-----|----------|---------|
| HS        | 366  | 0    | 0   | 0   | 0   | 0   | 0   | 0   | 0   | 0        | 0       |
| ROF       | 366  | 0    | 0   | 0   | 0   | 0   | 0   | 0   | 0   | 0        | 0       |
| SWE       | 366  | 0    | 0   | 0   | 0   | 0   | 0   | 0   | 0   | 0        | 0       |

Same result: rows returned but stat fields all null.

### Observation on populated fields

The response schema includes the stat fields; they exist in the JSON but carry
`null` values. Fields that ARE populated in every row:

- `snow_type` — string (e.g. `"HS"`)
- `code` — station identifier (redacted in this note)
- `date` — ISO date string
- `value` — float (e.g. `0.06` for HS, `20.4` for SWE)
- `id` — integer primary key

Fields `norm`, `value1`–`value14`, `count`, `std`, and all stat fields
(`mean`, `min`, `max`, `q05`, `q25`, `q50`, `q75`, `q95`, `previous`,
`current`) are null in every row returned across both date windows and all
three snow types.

---

## 3. Date-Window Observation

The maximum date returned in the primary window was `2026-12-31` — the data
spans through the end of 2026. No dates beyond 2026-12-31 were observed.

---

## 4. Errors

No HTTP errors occurred. URL form 1
(`http://localhost:8000/api/preprocessing/snow/?snow_type=HS&code=<redacted>&start_date=…`)
returned HTTP 200 for all three snow types in both date windows.

---

## 5. Conclusion

Rows are present for the probe station across both date windows and all three
snow types (HS: 669+366 rows, ROF: 712+366 rows, SWE: 712+366 rows). This is
not a zero-row case. The stat fields exist in the API schema but have never
been written to the database for this station — every row has `null` for every
one of `mean`, `min`, `max`, `q05`, `q25`, `q50`, `q75`, `q95`, `previous`,
and `current`.

Classification: **rows present but stat fields entirely unpopulated** —
distinct from "no data returned".

`DECISION: STOP — escalate to write-side owner (preprocessing-gateway maintainer; see Coordination item 1 of the plan).`

`DATE_WINDOW_DECISION: KEEP existing PREVIOUS_YEAR-01-01 to CURRENT_YEAR-12-31`

(The `2026-12-31` upper bound in the data matches the query parameter; it does
not represent data that was ingested beyond the current calendar year.)

---

## 6. Resolution (2026-06-01, evening)

The write-side gap identified above was addressed by the gateway snow-stat
population plan (`doc/plans/issues/high_prio_gi_draft_gateway_snow_stat_population.md`).
That plan landed nine commits on this branch — `97c889c`, `a294540`,
`b1582c7`, `90a574f`, `2793b62`, `222fa8a`, `2317d87`, `327bbb8`,
`2e13c30` — extending `recalculate_snow_norms.py` to write the ten stat
fields per `(snow_type, code, date)`, plus a backfill wrapper at
`bin/backfill_snow_stats_history.sh` for historical years.

Gateway plan Phase 3 verified end-to-end success at 954s for year 2026
across SWE/HS/RoF (evidence at
`doc/plans/working/snow_stat_population_e2e_evidence.md`). The
historical backfill is running in background at the time of this
update and will populate years 2010-2025.

### Updated probe — 2026-06-01

Fresh `urllib`-based probe (sandboxed-curl alternative) against the
same local stack, scoped to HS for 2026:

```
HS 2026 rows: 29200
  mean: 20440 non-null
  min: 20440 non-null
  max: 20440 non-null
  q05: 20440 non-null
  q25: 20440 non-null
  q50: 20440 non-null
  q75: 20440 non-null
  q95: 20440 non-null
  previous: 19152 non-null
  current: 8801 non-null
```

All ten dashboard stat fields are non-zero. The lower `current` count
(8801) is the expected partial-year effect — dates after 2026-06-01
have no observed `value` yet, so `current = None` by design (DEFAULT 2
in the gateway plan's decision artifact). ROF and SWE show the same
pattern with comparable counts per the gateway plan's P3 evidence.

### Decision

`DECISION: PROCEED to Phase 1 (data contract in db.py).`

Phase 0.5 (partial-population fallback) is NOT triggered — gateway plan
landed Option A directly. The fallback path remains documented for the
edge case where stat fields revert to null in some future operational
context.
