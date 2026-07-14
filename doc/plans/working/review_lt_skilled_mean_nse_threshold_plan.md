# Plan v3 — Long-term Skilled Mean: relax skill gate to "NSE > 0 only"

> **⚠️ IMPLEMENTED — DO NOT RE-IMPLEMENT. Awaiting review only.**
> Verified against `origin/maxat_sapphire_2` on 2026-07-14 (out-of-loop Codex review). Shipped via
> PR #405. The long-term overrides and env parsing are at
> `apps/postprocessing_forecasts/src/skill_metrics.py:107-196`; the long-term Skilled Mean call
> sites use them at `skill_metrics.py:2772-2775` and
> `apps/postprocessing_forecasts/src/ensemble_calculator.py:303-308`, `:668-676`.
>
> **One correction to the text below:** it documents the long-term default as `nse=0.0`. The
> shipped default is **`1e-9`**, because the shared filter is inclusive (`>=`) — see
> `skill_metrics.py:116-119` and `:1919-1922`. A literal `0.0` would have admitted models with
> exactly NSE == 0. The effective gate is "**NSE > 0** via epsilon", not threshold `0.0`.
> Short-term stays at 0.8. Read the rest of this file as history.

**Status:** v3. v1 review = NO-GO (4 conceptual blockers, all fixed in v2); v2 review =
**GO-WITH-CHANGES** (4 refinements — shared env parser, both-sides lead-value fallback, explicit
single-model=discard, EM forecast-output regression — all folded in below). Ready to implement.
**Base:** PR #403 (stale-aggregate skill fix) is **merged** to `maxat_sapphire_2`; branch off
current trunk. Module: `apps/postprocessing_forecasts` (fair game). Line numbers below are from
the post-#403 tree and are indicative — confirm at implementation.

---

## 1. Goal (owner-locked)

Long-term **Skilled Mean** should include **all long-term models with `NSE > 0`**. Short-term
(pentad/decad) unchanged. **EM must NOT change** (owner-locked) — this is the crux the v1 plan got
wrong.

Gate for long-term Skilled Mean = **NSE > 0 only**: `nse` → `0.0`, efficiency (`sdivsigma`) and
`accuracy` gates **disabled**.

## 2. Verified mechanism (corrected from v1)

- Gate: `filter_for_highly_skilled_forecasts(skill_stats, **overrides)` (`skill_metrics.py`
  ~L1716). AND-filter over `THRESHOLD_METRICS`; per-metric threshold from env var /
  `default_threshold`; accepts `**overrides`; the exact string `"False"` disables a gate.
  Registry keys are **`sdivsigma`, `nse`, `accuracy`** ("efficiency" is only the env-var name).
- **Corrected identity: `NSE = 1 − sdivsigma²`** (`skill_metrics.py:167/184/188`). Hence
  `sdivsigma < 0.6` ⇔ `NSE > 0.64`. Lowering only `nse` leaves `NSE > 0.64` (efficiency) +
  `accuracy > 0.8` binding → efficiency **and** accuracy must be disabled for the NSE>0 goal.
- **EM is built from the threshold-filtered pool at some horizons — this is the scope trap:**
  | horizon / side | EM built from | Skilled Mean built from |
  |---|---|---|
  | monthly, skill (`skill_metrics.py`) | filtered pool @ **:1296** → EM @ :1325 | separate filter @ **~:1558** |
  | monthly, forecast (`ensemble_calculator.py`) | **shared** filter @ **:285** (EM @ :321 **and** Skilled Mean @ :335) | same shared filter @ :285 |
  | quarter/season, skill | (aggregated EM) | Skilled Mean filter @ **~:2519** |
  | quarter/season, forecast | **fixed** `mean(LR_BASE, LR_SM)` @ **:622** (`AGGREGATED_EM_RAW_MODELS`) | Skilled Mean filter @ **:610** |
  So monthly EM (skill+forecast) currently rides the *same* filter as Skilled Mean; quarter/season
  EM is the fixed 2-model mean and is unaffected by the gate.
- **Forecast-side monthly Skilled Mean is NOT lead-aware:** merge/weight keys are
  `[month_in_year, code, model_short]` (`ensemble_calculator.py:286/375/385`), while the skill side
  groups by `horizon_value` (`GROUP_COLS`, `skill_metrics.py:108`). The EM path there *does* add
  `horizon_value` to its group when present (`:281`), but the Skilled-Mean merge/weights do not.

## 3. Proposed change (v2)

**Relax ONLY the Skilled-Mean selection, never EM. Make the forecast-side monthly Skilled Mean
lead-aware.**

### 3a. Config (new long-term-only env vars; ST untouched)
- `ieasyhydroforecast_nse_threshold_long_term` → default `0.0`
- `ieasyhydroforecast_efficiency_threshold_long_term` → default `"False"` (disabled)
- `ieasyhydroforecast_accuracy_threshold_long_term` → default `"False"` (disabled)
- One resolver helper returns the LT override dict; used by all LT **Skilled-Mean** call sites.
- **Env parser (REQUIRED, shared):** add ONE helper that parses each new LT var — case-insensitive
  disable tokens (`false/off/none/disable/''` → gate disabled) and numeric strings → `float`; an
  invalid value raises a clear config error (never a bare `float("false")` ValueError). The LT
  override helper uses it; do NOT rely on the filter's exact-`"False"` behavior.

### 3b. Skill side (`skill_metrics.py`)
- Monthly **Skilled Mean** (`_add_skilled_mean`, ~:1558): pass LT overrides.
- Monthly **EM** (~:1296): **leave at default** — do NOT relax (keeps EM membership unchanged).
- Quarter/season **Skilled Mean** (~:2519): pass LT overrides.

### 3c. Forecast side (`ensemble_calculator.py`) — requires a small refactor, not just overrides
- Monthly (:285) currently uses **one** filtered pool for EM **and** Skilled Mean. **Split it:**
  - EM uses `filter_for_highly_skilled_forecasts(skill_stats)` with **defaults** (unchanged EM).
  - Skilled Mean uses a **second** call with the LT overrides.
- Make the monthly Skilled-Mean **merge/weight keys lead-aware**: add `horizon_value` to
  `merge_keys`/`mae_df`/`qualifying_keys` (`:286/:375/:385`) **only when BOTH sides carry it** —
  present in *both* `joint`/`forecasts` AND `skill_filtered`/`mae_df`; otherwise use the 3-key legacy
  form. One-sided presence must NOT force a 4-key merge (it would mismatch — existing tests have
  forecast `horizon_value` but skill rows without it, `test_monthly_ensemble_creation.py:1466/1481`).
  This aligns forecast-side pool selection with the lead-aware skill side.
- Quarter/season Skilled Mean (:610): pass LT overrides. EM there is fixed LR (:622) → untouched.

### 3d. Short-term — untouched
- `skill_metrics.py:~1874` and `ensemble_calculator.py:113` keep defaults (no overrides).

## 4. Tests (lock the behavior)
- **EM unchanged (forecast OUTPUT, not just membership):** a direct `create_monthly_ensemble_forecasts`
  test where the relaxed Skilled Mean admits extra models but the **EM row's keys, composition,
  forecasted_discharge, and quantiles are byte-identical** to default behavior (guards the forecast-side
  split, §3c). Plus EM membership/skill identical on the skill side.
- **Skilled Mean broadened (LT):** models with NSE ∈ {−0.2, 0.1, 0.64, 0.9}; LT Skilled-Mean pool =
  {0.1, 0.64, 0.9} regardless of `sdivsigma`/`accuracy`.
- **LT exclusion:** NSE ≤ 0 excluded even if accuracy/efficiency would pass.
- **Short-term unchanged:** pentad/decad still require NSE>0.8 ∧ sdivsigma<0.6 ∧ accuracy>0.8.
- **Skill/forecast consistency, lead-aware:** forecast-side monthly Skilled-Mean selects the SAME
  per-`(code, month, horizon_value)` pool as the skill side — a model NSE>0 at lead 1 but ≤0 at
  lead 2 is in/out per lead. (Existing `test_monthly_ensemble_creation.py:1479` only asserts
  separate output rows, not separate pools — add a real per-lead-pool assertion.)
- **Single positive model:** exactly one model with NSE>0 → **Skilled-Mean row is DISCARDED**
  (preserve current single-model-discard behavior: `skill_metrics.py:1640`, `ensemble_calculator.py:443`,
  `:2575`) — assert NO Skilled-Mean row, no crash. (No semantic change unless the owner requests it.)
- **Empty pool:** no model NSE>0 → no Skilled-Mean row, no crash.
- **Env parsing:** `"False"`, `"false"`, `"0.0"` behave as specified.
- Placeholder code `19999` only. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh postprocessing_forecasts` (0 fail / 0 unexpected skip).

## 5. Rollout
1. Implement + module suite green.
2. **Full long-term skill recalc** (regenerates Skilled-Mean forecasts + skill consistently).
3. **Operational sanity-check:** before/after Skilled-Mean membership+skill on a few sites; confirm
   **EM is byte-unchanged**; confirm dashboard/bulletins read correctly.
4. Per-deployment recalc (Tajik + Kyrgyz + others).

## 6. Risks / open questions
1. **MAE weighting under the relaxed gate:** Skilled Mean is inverse-MAE weighted
   (`skill_metrics.py:1572`, `ensemble_calculator.py:380`), so a low-NSE but low-MAE model can
   dominate. Confirm acceptable, or reconsider weighting (flagged, out of scope unless owner wants it).
2. **Forecast-side lead-aware change is behavioral** beyond the threshold: it changes monthly
   Skilled-Mean pooling even at the *current* threshold (it fixes a latent non-lead-aware bug).
   Verify this is desired and covered by tests; it interacts with the PP-032/PP-038 lineage.
3. **Disabling accuracy entirely** for LT — owner-approved; re-confirm given (1).
4. Ensure the monthly forecast-side **filter split** does not alter EM output at all (regression test).
5. Quarter/season aggregated Skilled-Mean must honor the LT overrides (not a hard-coded threshold).

## 7. For the re-reviewer
Verify against code (`file:line`). GO / GO-WITH-CHANGES / NO-GO, ordered findings with evidence.
Specifically confirm: (a) the corrected `NSE = 1 − sdivsigma²`; (b) that **every EM call site stays
at default** and **only Skilled-Mean call sites** are relaxed (incl. the forecast-side split at
`ensemble_calculator.py:285`); (c) the lead-aware forecast-side keys with legacy fallback; (d) tests
lock EM-unchanged + per-lead pools + short-term-unchanged. Do not implement — plan review only.
