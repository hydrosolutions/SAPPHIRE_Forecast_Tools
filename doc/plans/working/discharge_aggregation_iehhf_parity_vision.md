# Vision Brief — iEH-HF-parity discharge aggregation in the hydrographs table

## Goal
Make SAPPHIRE's aggregated discharge averages (pentad, decad, month, quarter, season)
stored in the `hydrographs` table and displayed in the forecast dashboard/bulletin match
the iEasyHydro HF (iEH HF) reference **exactly to 3 significant figures**. iEH HF is the
authoritative source operational hydrologists cross-check against; today SAPPHIRE's values
differ by ~0.1–0.8% on monthly values (empirically e.g. 711 vs iEH HF 712 for one station),
which undermines confidence in SAPPHIRE bulletins.

## User / problem
Operational hydrologists at Kyrgyz Hydromet (and other Central-Asian hydromets) compare
SAPPHIRE bulletin values against iEH HF. iEH HF derives period averages with a specific
rounded-decadal methodology; SAPPHIRE currently computes unrounded true daily means and has
no correct hydrological (3-significant-figure) rounding, producing small but visible
mismatches.

## Root cause (empirically confirmed against live iEH HF; stations 16059/16159/15189/15216, 2023–2025)
- iEH HF **decadal** `WDDCA` = `round_3sf(mean(WDDA daily-average over the calendar decad))`,
  half-up. Built from `WDDA` (daily average), not `WDD` (morning). `WDDCA`/`WDFA` are already
  stored & populated in iEH HF for sampled stations.
- iEH HF **monthly** = **simple unweighted mean of the 3 rounded decadal values** (NOT
  day-weighted). iEH HF stores **no** monthly discharge value.
- SAPPHIRE **monthly** = unrounded true daily mean of `WDDA`. The gap = methodology
  (simple-decad-mean vs daily-mean) + missing 3sf rounding. **No `WDD`/`WDDA` series bug.**
- SAPPHIRE has **no** correct 3-significant-figure rounding helper; all existing
  `round_discharge*` functions are banded fixed-decimal and diverge from the reference for
  values ≥1000 (e.g. 1245.67→1246 instead of 1250) and <1 (0.2368→0.24 instead of 0.237),
  and use banker's (half-even) rounding instead of half-up.

## Solution
1. **Hydrological rounding — TWO separate layers.**
   (a) **Numeric `round_3sf(x) -> float | None`:** 3 significant figures, HALF-UP, **decimal-safe**
   (`Decimal(str(x))` + `quantize(..., ROUND_HALF_UP)`, NOT binary-float `round()` which gives
   the wrong `2.565→2.56`). For negatives, HALF-UP is applied **on the absolute value with the
   sign re-applied** (`-2.565 → -2.57`, away from zero). Results stated as **numeric equalities**:
   `round_3sf(1245.67)==1250`, `124.67==125`, `24.67==24.7`, `2.565==2.57`, `0.2368==0.237`,
   `99.95==100`, `999.5==1000`, `9.995==10`, `0.9995==1`; `0` and `-0.0` → `0.0`.
   **Non-finite contract (load-bearing for aggregation):** `round_3sf(None | NaN | +Inf | -Inf)
   -> None` (propagates as missing so the downstream null-guards handle it) — it does NOT raise.
   (b) **Display formatter `format_discharge(x) -> str`:** takes the ORIGINAL value and produces
   the 3-sig-fig **string** directly (magnitude-aware), preserving trailing significant zeros
   (`"10.0"`, `"1.00"`); non-finite/None → **empty string**. It does NOT rely on `round_3sf`'s
   float carrying precision — a plain float cannot distinguish `10.0` from `10`, so trailing-zero
   preservation is (b)'s sole responsibility (all trailing-zero examples belong here, not to (a)).
   **Locale is NOT in scope for `format_discharge`:** it emits a plain `.`-decimal string; the
   existing bulletin/dashboard display layer keeps applying the decimal-comma / thousands locale
   exactly as it does today. Both layers apply the identical 3sf HALF-UP rule, so the displayed
   string always agrees with the stored `round_3sf` value.
   The SAME numeric `round_3sf` (a) backs both the hydrograph write path and the bulletin
   formatter (b), so stored and displayed 3sf values never diverge for the same input. This
   replaces the banded `round_discharge*` functions where they format discharge.
2. **Pentad & decad actual period averages — SDK-first, auto-fallback.** Read `WDFA`
   (pentad) / `WDDCA` (decad) from iEH HF as authoritative; auto-fallback to
   `round_3sf(mean(WDDA over the calendar period))` whenever the SDK has no value for a
   station-period (hindcasts, gaps, API unavailable). Always SDK-first with automatic
   fallback (not a config toggle). Store rounded to 3 sig figs. **Both `current` (forecast/most-
   recent year) and `previous` (prior year) are computed from source** by the same SDK-first +
   fallback rule for their respective years — `previous` is recomputed from the prior year's
   `WDFA`/`WDDCA` (or WDDA fallback), NOT read from stored prior-year rows — so `previous` is
   correct immediately and is NOT gated on the M4 backfill.
   **Completeness gate:** only accept an SDK value (or finalize a computed one) for a period
   that has **closed**; never finalize a pentad/decad/month from a partial, in-progress
   period (an in-progress SDK value can be a partial mean and must not be treated as final).
   **Daily-mean fallback completeness rule:** when self-computing from daily `WDDA`, write a
   value only if **≥80% of the period's calendar days are present** (reusing the existing
   `monthly_mean_threshold_80` convention); otherwise store `null`. This applies to every
   daily-mean fallback path (pentad, decad, and the config=false monthly).
3. **Monthly — deployment-configurable.** Default `SAPPHIRE_MONTHLY_FROM_DECADAL=true` =
   `round_3sf(simple mean of the 3 rounded decadal averages)` (matches iEH HF / hydromet
   practice); `null` if fewer than 3 decadal actuals. Alternative `false` = daily-based mean
   `round_3sf(mean(WDDA over the calendar month))`, subject to the ≥80%-days completeness rule
   (else `null`). **Quarterly** = `round_3sf(mean of its 3 rounded monthly values)`, where the
   quarter months are the calendar quarters `Q1=(Jan,Feb,Mar) / Q2=(Apr,May,Jun) /
   Q3=(Jul,Aug,Sep) / Q4=(Oct,Nov,Dec)` (existing `QUARTER_MONTHS`); `null` if any of its 3
   monthly actuals is missing. **Seasonal** = the single **Apr–Sep** season =
   `round_3sf(mean of the 6 rounded monthly values Apr..Sep)`; `null` if any is missing.
   Quarter/season have **no iEH HF source** — their "parity" is this deterministic
   rounded-aggregation chain, not a match to an iEH HF value. Aggregation shape unchanged; only
   the rounded-monthly inputs + 3sf outputs are new.
   **Round-of-rounded cascade is INTENTIONAL** (month = round(mean of rounded decads); quarter/
   season = round(mean of rounded months)). It compounds rounding, but this is exactly what the
   hydromets want — a **relic of manual calculation** where each level was computed from the
   published rounded values of the level below. Do NOT "fix" it by rounding once from daily source.
   **Null-propagation cascade** (documented, testable): a thin decad (<80% days, no SDK) → `null`
   decad → `null` month (decadal mode) → `null` quarter/season. The **escape hatch** for sparse
   decads is `SAPPHIRE_MONTHLY_FROM_DECADAL=false` (direct daily-mean month under the 80% rule).
4. **Consolidate all hydrograph writing into `preprocessing_runoff`.** Relocate the **entire
   pentad/decad row computation** — BOTH the climatology envelope (`mean/min/max/q05–q95` rolling
   stats + the `norm` fetch, produced by the SAME method as today) AND the actuals — out of
   `linear_regression`/`iEasyHydroForecast.forecast_library` (remove the duplicate
   `_write_hydrograph_to_api`) into `preprocessing_runoff`; likewise home the
   month/quarter/season aggregation there. `preprocessing_runoff` becomes the single
   owner/producer/writer of the full `hydrographs` row for every horizon. This is a *relocation*
   of the envelope computation, not merely "preserve an existing envelope" — after
   consolidation, `preprocessing_runoff` must itself produce a fully-populated envelope.
5. **Documentation.** Update the data-flow docs and module docs to explain the averaging
   methodology, the SDK-first/fallback rule, the deployment config, and the actuals-only
   scope.

## Scope = "ACTUALS ONLY"
Only the ACTUAL observed period-average values (this-year "current" and last-year "previous")
change to match iEH HF. The multi-year climatology envelope (`norm`, `min`, `max`, `q05–q95`)
and the forecast-facing rolling pentad/decad statistics are **left unchanged**. No forecast
norm, forecast input, or skill-metric behavior changes.

**This invariant must be PROVEN, not asserted:** before/as part of the work, inventory every
read of the `hydrographs` table by horizon and column, and confirm no forecast input reads the
columns we change (`current`/`previous`). Regression tests must compare forecast **inputs**
(predictors, norms) before-vs-after, not only final skill metrics. Two mechanical risks to
close (see Verification-gated constraints): (a) calendar-actual writes must not collide with /
overwrite the rolling forecast-statistic rows in the shared `(horizon_type, code, date)` key
space; (b) an actuals-only write must not null the climatology envelope columns via upsert.

## Parity definition (storage vs display)
"Match to 3 significant figures" is defined on the **stored numeric value** (3sf, half-up).
Bulletin **display** formatting — thousands separators, decimal comma vs point (locale),
trailing significant zeros (e.g. `10.0` vs `10`), and the Excel cell number format — is a
**separate, explicitly-specified** concern layered on top of the rounded numeric value, not
conflated with it.

## Hard constraints
- No real station codes or discharge values committed to git (use "19999" in tests).
- `sapphire/services/` is colleague-owned — no edits without coordination. Work within the
  existing `/hydrograph` API + `hydrographs` schema (it already supports
  pentad/decade/month/quarter/season `horizon_type`s). If a contract change seems needed,
  raise a discussion first rather than editing service code.
- "Match exactly" = equal after 3-significant-figure HALF-UP rounding.
- Config is **deployment-level env vars**, not per-station.
- All work on a separate feature branch (base = `maxat_sapphire_2`). Full test suite must pass
  with zero skips: `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh`.

## Non-goals
- Not changing the daily discharge fetch (`WDD`/`WDDA`) or the daily hydrograph value.
- Not rebuilding/altering forecast climatology/norms or the rolling pentad/decad forecast
  statistics.
- Not touching forecast skill metrics or the forecasting models.
- Not editing `sapphire/services/` API/DB/schema.

> **Correction (was a contradiction):** quarter and season ARE in scope for parity. Their
> aggregation *shape* is unchanged (still mean-of-monthly), but their **inputs become the
> rounded monthly chain and their outputs are 3sf-rounded** — an intended parity change, not a
> methodology redesign. This is deliberately NOT listed as a non-goal.

## Write model (resolved from pre-flight)
The `/hydrograph` write is a **full-row read-modify-write**: the service `setattr`s every column
from `model_dump()` (no `exclude_unset`), so any field the client omits or sends as `null` is
written as **NULL**. Therefore an "actuals-only" write that omitted the envelope would **destroy**
`norm/mean/min/max/q05–q95`. **Resolution (no service change):** `preprocessing_runoff` computes
and writes the **complete row every time** — the climatology envelope (computed by the SAME
rolling method as today, so unchanged) PLUS the new actuals (`current`/`previous` from
`WDFA`/`WDDCA`, 3sf-rounded). "Actuals-only" therefore means *only the actual values change
semantically*; the envelope is recomputed identically and never dropped.

## Reader inventory (resolved from pre-flight)
- **No forecast MODEL reads `hydrographs` as a predictor.** Linear regression's predictor comes
  from the `runoffs` table (`predictor` column); `machine_learning` and `long_term_forecasting`
  never read `hydrographs`. So `current`/`previous` are **display-only** (dashboard plots +
  bulletin this-year/last-year values).
- **Forecast-critical columns to preserve unchanged:** `norm` (consumed by `forecast_skill_eval`
  as the skill-scoring reference in `NormResolver`, and by dashboard bulletins) and the
  `mean/min/max/q05–q95` climatology envelope (the dashboard hydrograph bands). These are all
  climatology — recompute them by the SAME method; do not 3sf-round or otherwise alter the
  STORED envelope/`norm`. (Bulletin *display* may render them via the 3sf helper — that is
  cosmetic and does not touch the stored value `forecast_skill_eval` reads.)

## Verification-gated constraints (must hold; verified in pre-flight)
- **Single writer, single row, issue-date key:** after consolidation, `preprocessing_runoff` is
  the ONLY writer of each pentad/decad row and recomputes it **wholesale** (envelope + norm +
  actuals). It MUST key the row on the **issue date** (`get_issue_date_from_pentad`/`_decad` =
  last day of the previous period), the same convention the forecast writer uses today — so the
  consolidated write lands on the one existing row rather than creating a second, disjoint
  calendar-date row. `linear_regression` no longer writes hydrograph rows. Verify: no duplicate
  and no orphaned rows after the switch.
- **Envelope + norm method unchanged:** the climatology columns are produced by the same rolling
  computation and the same norm fetch as today (verify column-by-column equality before/after
  for a fixed input). Only `current`/`previous` change (new source + 3sf); monthly/quarter/season
  actuals change per the new method.
- **Actuals-only invariant proven** via the reader inventory above + forecast-input regression
  tests (confirm `forecast_skill_eval` norms and any hydrograph read are unchanged).

## Decomposition guidance (milestone sequencing & ownership — from WF1 review round 1)
- **No dual-writer window.** The milestone that first introduces the new `preprocessing_runoff`
  pentad/decad write MUST, in the SAME milestone, retire (or no-op) the `linear_regression` /
  `forecast_library` pentad/decad writer. Both writers hitting the same
  `(horizon_type, code, issue_date)` row with different rounding would clobber/flap — never
  allow a phase where both are live. Likewise for month/quarter/season vs the old
  `sync_long_horizon_hydrograph` / `sync_monthly_norms` writers.
- **Envelope ownership moves with the writer.** Because the old writer is retired in the same
  milestone, that milestone must also relocate the envelope+`norm` computation (unchanged method)
  so `preprocessing_runoff` produces the COMPLETE row (envelope + norm + actuals) — verified by a
  single-writer run yielding a fully-populated envelope with no residual old-writer row.
- **Deterministic actual column:** `current` = the forecast/most-recent year's closed-period
  actual; `previous` = the prior year's. State this rule so the write target is unambiguous.
- **Key convention for every horizon:** pentad/decad rows are keyed on the **issue date** (last
  day of the previous period). State the explicit `date` key for month/quarter/season rows too so
  "no duplicate / no orphaned alternate-key rows" is testable across all five horizons.
- **Bulletin parity for ALL horizons.** The stored-vs-displayed 3sf parity check applies to
  pentad/decad AND month/quarter/season — every milestone that writes a horizon's actual must
  also verify the bulletin/dashboard display of that horizon consumes the same rounded value.
- **Backfill milestone is lower-risk by construction** — since consolidation + single-writer land
  with the new writes, the final milestone is just: backfill prior 3 years with identical rules,
  assert zero old-writer rows remain, and prove forecast predictors/norms unchanged.
- **M1 scope is the rounding contract only.** M1 (no deps) must be verifiable on its own: it owns
  the `round_3sf`/`format_discharge` unit contract, and "the bulletin/dashboard display path
  applies `round_3sf`/`format_discharge` to whatever is currently stored." The per-horizon
  *stored==displayed under the new parity rules* checks belong to M2/M3 (where those stored
  values first exist), NOT to M1.
- **Legacy-writer retirement is cross-module — name the files in scope.** Retiring the old writer
  in M2/M3 touches modules OTHER than `preprocessing_runoff`: M2 scope must name
  `apps/linear_regression/linear_regression.py` (calls ~788/808) and
  `apps/iEasyHydroForecast/forecast_library.py` (`write_pentad_hydrograph_data`,
  `write_decad_hydrograph_data`, shared `_write_hydrograph_to_api`); M3 scope must name
  `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` and `sync_monthly_norms.py`.
  Removing another module's side-effect is higher-risk than computing values, so it is called out
  explicitly, not discovered mid-implementation.
  **Retire per-horizon CALL SITES, not the shared sink.** `_write_hydrograph_to_api` in
  `forecast_library` is ALSO the sink for `write_month_hydrograph_data` (via `sync_monthly_norms`).
  M2 must silence only the **pentad/decad** call paths into it — the **month** call path stays
  live until M3 replaces it. Never disable the shared function wholesale in M2 (that would kill
  monthly writes before M3 lands).
- **Pre-cutover parity diff-test gate (reconciles "no dual-writer window" with cutover safety).**
  Before M2/M3 remove an old writer, they MUST pass a **shadow-compute comparison gate** (a
  test/one-off script, NOT a live dual-write to the same rows): run the new `preprocessing_runoff`
  computation and the old writer's computation on the SAME inputs and assert (a) the
  `norm/mean/min/max/q05–q95` columns are **byte-identical** to the old output, and (b) `current`/
  `previous` equal the independently-computed expected 3sf values (they intentionally CHANGE — the
  gate confirms they change to exactly the right values). Only after this gate passes is the old
  writer removed. This gives a verification window without ever having two live writers on the
  same `(horizon_type, code, issue_date)` row.
- **Month/quarter/season "envelope" = `norm` only.** The current long-horizon writers emit only
  `norm` + `current`/`previous` for these horizons — there is NO existing `min/max/q05–q95`
  quantile band for month/quarter/season (unlike pentad/decad). Do NOT invent new bands; "preserve
  envelope" for these horizons means preserve `norm` (+ whatever the current writer produces).
- **Issue-date key — worked example.** "Issue date = last day of the period BEFORE the one being
  described" (`get_issue_date_from_pentad`/`_decad`). E.g. decad_in_year 2 (Jan 11–20) is keyed to
  `date = Jan 10`; decad_in_year 1 is keyed to `Dec 31 of the prior year`. State the analogous
  explicit key for month (last day of previous month), quarter (last day of previous quarter), and
  season (last day before Apr 1) so duplicate/orphan detection is testable across all five horizons.
- **Backfill safety rail (M4).** The 3-year backfill overwrites historical production rows — a
  class of operation that has previously corrupted historical data in this project. M4 MUST
  provide a **dry-run mode** (compute + report the diff without writing) AND take a
  **pre-backfill snapshot** of affected rows with a **post-write verification** step, before
  committing overwrites.

## Definition of done
For a set of live Kyrgyz stations, SAPPHIRE's stored and bulletin-displayed **decadal** and
**monthly** discharge values match iEH HF exactly to 3 significant figures, with monthly =
simple mean of the 3 rounded decadal values; the hydrological rounding is a single shared 3sf
half-up helper used in both the write and bulletin paths; all hydrograph horizons are written
by `preprocessing_runoff`; the monthly method is switchable by a deployment env var; docs are
updated; and the full test suite is green with zero skips.

## Resolved decisions
1. **Missing/incomplete decad → monthly = `null`.** When a month has <3 decadal values (a
   `WDDCA` missing with no daily fallback possible), emit `null` for that month — never
   fabricate from a partial set of decads. (Rare: ~6 station-months in 2024.)
2. **Backfill = 3 years.** A one-time backfill of the prior **3 years** of pentad/decad/month
   (and derived quarter/season) actuals is **in scope**, as its own milestone, so "previous"
   bulletin comparisons match iEH HF.
3. **Provenance = log/audit only.** Record the per-value source (SDK `WDDCA`/`WDFA` vs
   computed-from-`WDDA`) in logs/audit output, **not** in the table (no service/schema change).
4. **Config granularity = deployment-level boolean.** Confirmed one deployment = one hydromet,
   so `SAPPHIRE_MONTHLY_FROM_DECADAL` (default `true`) is sufficient; no per-station/basin map.
5. **Daily-mean fallback completeness = ≥80%** of the period's days present, else `null`.
6. **`round_3sf` non-finite/None → `None`** (propagates as missing); `format_discharge` → `""`.
7. **Cutover safety = pre-cutover parity diff-test gate** (shadow-compute comparison before the
   old writer is removed), NOT a live production dual-write / parallel-run.
8. **Quarter/season round-of-rounded cascade is intended** (hydromet requirement; a relic of
   manual calculation) — do NOT round once from daily source.
9. **`format_discharge` is locale-free** (plain `.`-decimal); the existing bulletin layer keeps
   the decimal-comma / thousands locale.

## Pre-flight findings (risks measured against live iEH HF, 2024)
- **Coverage:** 93.7% of sites (59/63) report `WDDCA`/`WDFA`; ~89–95% of station-decads present.
  Gaps are whole-station or whole-decad dropouts — **never** "daily present but average missing."
- **Fallback frequency (operational):** 0 cases of WDDCA-missing-while-daily-present → the
  self-compute fallback is **defense-in-depth**, not the common path (still required for
  deep-history hindcasts where HF may not have stored averages).
- **In-progress:** HF withholds decadal/pentadal averages until the period closes → SDK-first
  cannot ingest a partial; the current period is simply absent until close.
- **February/leap & season boundary:** safe; values stamped mid-period at noon local, no
  month/season-edge ambiguity.
- **Missing-decad is the one live edge:** ~4.6% both-missing incl. 6 station-months with only
  2 of 3 decads → drives Open decision #1 (missing-decad monthly policy).

## Riskiest assumptions to stress-test
1. `WDFA`/`WDDCA` populated for ALL operational stations — **measured 93.7% for 2024**; robust
   auto-fallback still required, especially for **older hindcast years not yet sampled**.
2. The simple-mean monthly rule is universal across deployed hydromets (hence the config
   toggle) — confirmed for Kyrgyz; unverified for other deployments.
3. Decoupling pentad/decad writing from `linear_regression` must not change forecast inputs or
   behavior — **reader inventory shows no forecast model reads `hydrographs`; `current`/`previous`
   are display-only** — but the issue-date key + full-row recompute must be implemented exactly
   to avoid duplicate/clobbered rows.
