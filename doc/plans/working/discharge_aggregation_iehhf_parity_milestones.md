# Final Milestone List — iEH-HF-parity discharge aggregation
(WF1 round-3 converged list + baked-in fixes from rounds 1–3 and user decisions. Feeds WF2.)

Dependency graph: M1 → M2 → M3 → M4 (M2 depends M1; M3 depends M2; M4 depends M2,M3).

---

## M1 — Shared 3-Significant-Figure Discharge Contract
**Goal:** One shared numeric rounder + display formatter used by storage and bulletin/dashboard,
no aggregation change yet.
**Files:** likely `apps/iEasyHydroForecast/forecast_library.py` (shared helper home) + the
bulletin/dashboard display call sites in `apps/forecast_dashboard/src/bulletins.py`.
**Acceptance:**
- `round_3sf` numeric equalities: 1245.67==1250, 124.67==125, 24.67==24.7, 2.565==2.57,
  0.2368==0.237, 99.95==100, 999.5==1000, 9.995==10, 0.9995==1; 0/-0.0==0.0.
- Negatives away-from-zero: round_3sf(-2.565)==-2.57.
- **Non-finite: `round_3sf(None|NaN|±Inf) -> None`** (no raise).
- `format_discharge` renders 3sf **string** with trailing significant zeros ("10.0", "1.00");
  non-finite/None -> "". Locale-free (plain "."); the existing bulletin layer keeps decimal-comma.
- Bulletin/dashboard display path routes **currently-stored** values through the shared
  contract (pre-parity). Per-horizon stored==displayed-under-new-rules checks live in M2/M3.
**Scenarios:** S1, S2, S14, S15, S16.

## M2 — Pentad & Decad Actuals from preprocessing_runoff (single writer)
**Goal:** preprocessing_runoff produces the FULL pentad/decad row (envelope+norm unchanged method
+ new 3sf actuals); retire the pentad/decad legacy writers in the same deliverable.
**Files (in scope):** `apps/preprocessing_runoff/…` (new writer); retire pentad/decad call sites in
`apps/linear_regression/linear_regression.py` (~788/808) and
`apps/iEasyHydroForecast/forecast_library.py` (`write_pentad_hydrograph_data`,
`write_decad_hydrograph_data`) — **only the pentad/decad call paths into `_write_hydrograph_to_api`,
NOT the shared sink (month path stays live until M3).**
**Acceptance:**
- Closed pentad/decad: `current` = round_3sf(WDFA/WDDCA) for the most-recent year; `previous` =
  same for prior year — **both computed from source (SDK-first + 80% WDDA fallback), previous NOT
  read from stored rows / NOT gated on M4.**
- Fallback only if ≥80% of period days present, else `null`. In-progress period → no finalized value.
- Issue-date key: last day of previous period (decad 2 → Jan 10; decad 1 → Dec 31 prior year);
  exactly one row per (horizon_type, code, issue_date); no duplicate/orphan.
- Full-row write: envelope (mean/min/max/q05–q95) + norm by the SAME method as today; omitted
  fields never nulled.
- **Pre-cutover parity diff-test gate:** shadow-compute new vs old on same inputs → envelope/norm
  byte-identical, actuals == expected 3sf → THEN remove old pentad/decad writer. No live dual-write.
- Legacy pentad/decad writers no longer write; stored actuals == bulletin/dashboard display (3sf).
**Scenarios:** S3, S4, S5, S19, S22, S23, S13, S10 (pentad/decad), S11 (pentad/decad), S12 (inputs unchanged).

## M3 — Month / Quarter / Season Actuals from preprocessing_runoff (single writer)
**Goal:** preprocessing_runoff is the sole producer of month/quarter/season actuals; configurable
monthly method; deterministic rounded-aggregation chain.
**Files (in scope):** `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py`,
`apps/preprocessing_runoff/sync_monthly_norms.py` (retire/replace); month call path into
`forecast_library._write_hydrograph_to_api`.
**Acceptance:**
- Monthly default (`SAPPHIRE_MONTHLY_FROM_DECADAL` unset/true) = round_3sf(simple mean of 3 rounded
  decadal actuals); `null` if <3. `false` = round_3sf(mean(WDDA over month)) under ≥80% rule else null.
- Quarter = round_3sf(mean of 3 rounded monthly), calendar Q1–Q4; null if any month missing.
  Season Apr–Sep = round_3sf(mean of 6 rounded monthly); null if any missing. **Round-of-rounded
  cascade is intended (hydromet requirement).**
- Null-propagation cascade documented; `false` daily path is the sparse-decad escape hatch.
- Issue-date keys: month=last day of prev month, quarter=last day of prev quarter, season=last day
  before Apr 1; no duplicate/orphan.
- "Envelope" here = **`norm` only** (no quantile bands exist for these horizons — do not invent).
- Old long-horizon writers retired **after** the pre-cutover parity diff-test gate passes.
- Stored actuals == bulletin/dashboard display (3sf) for month/quarter/season.
**Scenarios:** S6, S7, S8, S9, S17, S18, S20, S21, S11 (month/quarter/season), S26.

## M4 — Historical Backfill (3 yr) + Operator Documentation
**Goal:** Backfill prior 3 years of all five horizons with the consolidated rules, behind safety rails.
**Acceptance:**
- **Dry-run** mode: compute + report before/after diff without writing.
- **Pre-backfill snapshot** of affected rows (inspect/restore) before write-mode overwrites.
- **Post-write verification** confirms written rows follow the same SDK-first/fallback/monthly-config/
  rounded-aggregation/nullability/issue-date rules as normal runs.
- Covers pentad/decad/month/quarter/season, current+previous, prior 3 years.
- No residual old-writer rows for affected keys.
- Forecast predictors + stored norm/envelope unchanged (apart from relocation-equivalent recompute).
- Docs updated: 3sf contract, SDK-first/fallback, `SAPPHIRE_MONTHLY_FROM_DECADAL`, issue-date keys,
  actuals-only scope, backfill dry-run/snapshot/verify workflow.
**Scenarios:** S25, S10 (full), S12 (post-backfill), S27 (offline fixtures gate).
