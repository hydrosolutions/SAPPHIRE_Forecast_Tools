# PREPG-021: HS values absent upstream for Jul–Sep 2025 — the dashboard's HS "previous year" band is blank right now

**Status**: Draft (2026-08-21)
**Module**: `apps/preprocessing_gateway` (source: Data Gateway / upstream), surfaces in
`forecast_dashboard`
**Priority**: **Medium** — user-visible on a deployed dashboard, but **not a code defect**: the
data does not exist upstream, and no code change can synthesise it. Confirmed present on the
servers, not just locally.
**Labels**: `preprocessing_gateway`, `snow-data`, `data-gap`, `upstream`, `dashboard`
**Found**: 2026-08-21, during the local rehearsal of **PREPG-007** P3.
**Related**: **PREPG-007** — P3's own POST verification is what exposed this, and it will look like
a P3 failure to anyone who runs it. **ML-017 WS-4** — same shape (days genuinely absent in DG, no
downstream fix possible).

> Sanitized: aggregate counts only. No station codes, SWE/HS values or discharge appear here.

---

## What is missing

`HS` (snow height) rows exist for **Jul–Sep 2025** but their `value` is NULL. Measured on a local
DB carrying the full archive (80 codes):

| month | 2024 `value` | **2025 `value`** | 2026 `value` |
|---|---|---|---|
| Jul | 2,480 | **2,000** (partial) | 2,395 |
| Aug | 2,480 | **0** (empty) | 1,997 |
| Sep | 2,400 | **480** (mostly empty) | — (future) |
| Oct–Dec | 2,480 | 2,480 (recovered) | — |

**It is not seasonal.** The same months in **2024 are complete**, and 2025 recovers fully from
October. It is a contiguous ~3-month hole, bounded on both sides by good data.

**`SWE` is unaffected** over exactly the same period (2,480/2,480 for Jul and Aug 2025), so this is
specific to HS rather than a general snow outage — which is the detail that makes "the gateway was
down" an unsatisfying explanation.

## Why it is visible now, and why it looks like a P3 failure

`previous` for a given date is the **previous year's value for the same day**. So the 2025 hole
produces a matching hole in 2026:

| 2026 month | HS rows | HS `previous` |
|---|---|---|
| Jan–Jun | ~2,400 each | complete |
| Jul | 2,480 | **2,000** |
| Aug | 2,480 | **0** |

All 80 codes affected. On the dashboard this is a **blank HS previous-year band for the current
period**.

**Anyone running PREPG-007's P3 will see this in the POST verification and reasonably suspect the
recalc.** It is not the recalc: `recalculate_snow_norms.py` computed `norm` and `q50` at
19,440/19,440 for 2026 in the same run. It cannot produce `previous` from source values that are
NULL.

## Why the self-healing fix cannot repair it

PREPG-007's Change B re-writes the last 365 days on every maintenance run, which is exactly the
mechanism intended to heal historical holes — and it **does** work: the same rehearsal healed
+1,512 `value` and `current` rows per snow type.

It cannot heal this one, because the maintenance sync writes **what the Data Gateway returns**. If
the DG has no HS values for those dates, re-syncing writes nothing. Additionally, Jul 2025 and most
of Aug 2025 have now fallen **outside** the rolling 365-day window entirely.

## What would actually fix it

1. **Establish whether the DG still lacks HS for 2025-07-01 … 2025-09-30.** Probe the snow endpoint
   for those dates directly; do not infer it from our DB, which only records what we were given.
2. **If the DG has the data now**, a bounded one-time backfill for that range — note this is
   *outside* the 365-day window, so the routine maintenance sync will not pick it up and a
   `SNOW_RECALC_YEAR`-style scoped run is needed.
3. **If the DG does not have it**, escalate upstream. Until then the band is legitimately blank and
   the dashboard should ideally say so rather than showing an unexplained empty region — a
   presentation decision, filed here only as a note.

## Acceptance criteria

- The DG probe result for 2025-07-01 … 2025-09-30 is **recorded in this issue** (has data / does
  not), with the date the probe was run — the answer changes over time and an undated "checked, it
  was missing" is worthless later.
- If backfilled: HS `value` coverage for Jul–Sep 2025 matches the SWE coverage for the same period,
  and HS `previous` for Jul–Sep 2026 becomes non-null for the same codes.
- The dashboard HS previous-year band renders for the current period.

## Explicitly NOT in scope

- Changing `recalculate_snow_norms.py`. It is behaving correctly on the input it has.
- Widening the 365-day maintenance window. That window is deliberate (PREPG-007) and widening it to
  reach a 14-month-old gap would be the wrong instrument.
- Interpolating across a three-month hole. Filling a quarter of a year of snow height by
  interpolation would produce plausible-looking numbers that are not observations — cf. ML-017,
  where the same temptation is bounded to gaps of 1–3 days.
