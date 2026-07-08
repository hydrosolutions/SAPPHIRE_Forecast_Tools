# Acceptance Scenarios (Given/When/Then)

These are checked against the ORIGINAL vision after each milestone by WF2's conformance gate.
The implementer never sees or edits them. Numbers use fake station "19999" except where a
real iEH HF reference value is required for parity (user-supplied, kept out of git).

## Rounding helper
- **S1 — 3sf half-up reference cases.** Given the hydrological rounding helper, when applied to
  1245.67 / 124.67 / 24.67 / 2.565 / 0.2368, then it returns 1250 / 125 / 24.7 / 2.57 / 0.237.
- **S2 — edge cases.** Given the helper, when applied to a value ≥1000 (2565→2570), a sub-1
  value (0.04567→0.0457), and to NaN/None/negative, then it returns 3 sig figs half-up and
  handles non-finite gracefully (empty display in the bulletin path, as today).

## Decadal / pentadal (SDK-first, fallback)
- **S3 — decadal SDK-first.** Given a station-period where iEH HF returns a `WDDCA` value, when
  `preprocessing_runoff` builds the decad hydrograph, then the stored decadal average equals
  iEH HF's `WDDCA` to 3 sig figs.
- **S4 — decadal fallback.** Given a station-period where iEH HF has no `WDDCA`, when the decad
  hydrograph is built, then the stored decadal average equals
  `round_3sf(mean(WDDA daily values over that calendar decad))`, half-up.
- **S5 — pentadal SDK-first + fallback.** Analogous to S3/S4 using `WDFA` for pentads.

## Monthly / quarter / season
- **S6 — monthly default (from decadal).** Given `SAPPHIRE_MONTHLY_FROM_DECADAL=true` and a
  month with decadal averages d1,d2,d3, when the monthly hydrograph is built, then the stored
  monthly = `round_3sf((d1+d2+d3)/3)` (simple unweighted mean).
- **S7 — monthly matches iEH HF reference.** Given the user-supplied iEH HF reference monthly
  values for specific station-months (default config), when SAPPHIRE computes those monthly
  values, then they match to 3 sig figs.
- **S8 — monthly alternative (daily-based).** Given `SAPPHIRE_MONTHLY_FROM_DECADAL=false`, when
  the monthly hydrograph is built, then the stored monthly = `round_3sf(daily mean over the
  month)`, and it differs from the decadal simple-mean for a non-30-day month.
- **S9 — quarter/season on rounded monthly.** Given monthly values are built, when quarter and
  season hydrographs are built, then quarter = `round_3sf(mean of its 3 monthly values)` and
  season = `round_3sf(mean of the 6 Apr–Sep monthly values)`.

## Consolidation / ownership
- **S10 — single writer.** Given a full `preprocessing_runoff` run, when it completes, then the
  `hydrographs` table has day, pentad, decad, month, quarter, season rows for the stations, all
  written by `preprocessing_runoff`; and `linear_regression` no longer writes any hydrograph
  rows (the duplicate writer in `forecast_library` is gone/unused).
- **S13 — idempotent upsert.** Given `preprocessing_runoff` runs twice, when the second run
  completes, then hydrograph rows are upserted with no duplicates and stable values.

## Bulletin display
- **S11 — bulletin display rounding.** Given a bulletin is generated, when a discharge value
  like 1245.67 or 0.2368 is rendered, then it displays as `1250` / `0,237` (3 sig figs, comma
  decimal), consistent with the reference doc.

## Invariant (actuals-only)
- **S12 — no forecast behavior change.** Given the change is deployed, when forecast norms, the
  rolling pentad/decad forecast statistics, and skill metrics are compared before vs after,
  then they are unchanged.

## Rounding — boundaries & implementation (added from review)
- **S14 — power-of-ten boundaries.** Given the rounder, when applied to 99.95 / 999.5 / 9.995 /
  0.9995, then it returns 100 / 1000 / 10.0 / 1.00 (3 sig figs, retained trailing zeros in the
  stored/display representation as specified).
- **S15 — decimal-safe, not binary float.** Given the rounder is implemented via Decimal, when
  applied to 2.565, then it returns 2.57 (proving it does not go through a binary-float
  `round()` that would yield 2.56).
- **S16 — negatives / zero / non-finite.** Given the rounder, when applied to a negative
  (e.g. -2.565), exactly 0, `-0.0`, NaN, None, inf, then it follows the brief's defined policy
  (per the "Open decisions"/brief once fixed) deterministically and never raises.

## Fallback / provenance / completeness (added from review)
- **S17 — missing-decad month.** Given a month with only 2 of 3 decadal values available (no
  daily fallback for the third), when the monthly is built, then it follows the decided policy
  (default: `null`, not a 2-decad mean) — deterministically.
- **S18 — mixed-provenance month.** Given a month whose decads mix SDK `WDDCA` and
  computed-from-`WDDA` fallback values, when the monthly is built, then it is computed from
  whatever the per-decad rule yields and (per decision) provenance is recorded/logged; the
  result is reproducible.
- **S19 — completeness gate (in-progress period).** Given the current pentad/decad is still
  in progress, when aggregation runs, then no final value is written for that period (a partial
  SDK/daily value is not treated as final).

## Calendar correctness (added from review)
- **S20 — leap & short February.** Given February in a non-leap year (3rd decad = days 21–28)
  and a leap year (21–29), when decad/month are built, then the calendar-period generator
  assigns days correctly (no March 1 bleed, no missing Feb 29) and the monthly equals
  `round_3sf(mean(d1,d2,d3))`.
- **S21 — season boundary & timezone.** Given daily timestamps near midnight around Apr 1 and
  Sep 30, when period/season assignment runs, then each day maps to the correct
  local-hydrological decad/month/season (the Apr–Sep season boundary is exact).

## Storage integrity & pipeline (added from review)
- **S22 — upsert preserves envelope.** Given an existing pentad/decad row with
  `norm/min/max/q05–q95` populated, when an actuals-only write updates `current`/`previous`,
  then the envelope columns are unchanged (byte-for-byte).
- **S23 — no collision with rolling rows.** Given the rolling forecast-statistic row for a
  pentad/decad, when preprocessing_runoff writes the calendar actual, then the forecast-facing
  columns are not overwritten (S12's mechanism).
- **S24 — pipeline freshness.** Given the daily/period data has refreshed, when a bulletin is
  generated, then the hydrograph rows it reads were produced by the current run (not stale from
  a prior day) — per the decided writer schedule.
- **S25 — backfill equivalence (3 years, in scope).** Given the prior 3 years of
  pentad/decad/month/quarter/season actuals are backfilled with the new method, when the
  bulletin renders "previous", then those historical values match iEH HF to 3 sig figs, and the
  climatology envelope/`norm` for those rows is unchanged.

## Bulletin output (added from review)
- **S26 — Excel workbook formatting.** Given a bulletin is generated, when the produced Excel
  workbook cell is inspected (not just the Python string), then the discharge value shows the
  specified 3sf value with the specified locale/number format (decimal comma, thousands
  handling), e.g. `1245.67 → 1250`, `0.2368 → 0,237`.

## Offline test contract (added from review)
- **S27 — offline fixtures gate the build.** Given the offline build harness (no network), when
  the deterministic unit/contract tests run against recorded WDFA/WDDCA fixtures (present /
  missing / stale / API-failure; anonymized, no real station codes or discharges), then S1–S2,
  S4, S6, S8–S10, S13–S23, S26 all pass offline; the live-SDK scenarios (S3, S5, S7, coverage)
  are marked as separate live-certification tests that run in CI/on-server, not in the offline
  gate.
