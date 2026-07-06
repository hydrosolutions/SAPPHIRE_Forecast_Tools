# Forecast Skill Evaluation — Irrigation Limit-Plan Decision (Report Draft)

**Status:** results complete — corrected module-correctness re-run 2026-07-03.

Re-run date: 2026-07-03. Artifacts:
`apps/forecast_skill_eval/artifacts/rerun_2026-07-03_corrected/`
(this run evaluates **two below-norm thresholds side by side** — the operational
`below_norm` = value < 0.80 × norm limit-plan trigger and a new
`below_norm_100` = value < 1.0 × norm plain-below-average event — and keeps the
phase-2 `season` column {all, irrigation, non_irrigation} in
`contingency_metrics.csv` plus the `persistence` baseline in `baselines.csv`.
It **supersedes** `rerun_2026-07-02_both_thresholds/` (and the earlier
`rerun_2026-06-30*` paths): the numbers changed because of four module-correctness
bug fixes — short-term de-leaking, a date-based long-term regime split, a
historical LR issue→target repair, and a quarter mis-stratification fix (see
**Corrections in this revision** below).

---

## Corrections in this revision

The previous draft's numbers were inflated or mislabelled by four bugs that are
now fixed. Every number in this revision reflects the corrected run. The four
corrections are:

1. **Short-term de-leaking (issue-before-target + one-forecast-per-target).**
   Short-term forecasts were previously scored on *every* intraday re-issue,
   including in-period nowcasts that had already observed part of their own
   target period — an information leak that inflated skill. The corrected run
   keeps only genuine forecasts (issued strictly before the target period) and
   exactly one forecast per target. Effect: pentad EM POD 0.928 → **0.899**,
   HSS 0.871 → **0.847** (n 11 342 → 3 384); decade EM POD 0.905 → **0.845**,
   HSS 0.840 → **0.791**. Still strong — now honest.
2. **Long-term regime split (date-based, genuine-2026).** The old "operational"
   long-term regime was flag-based and was dominated by *pre-2026 backfills that
   had been mislabelled operational*. The corrected run defines "operational" as
   **genuinely post-2026 real-time** forecasts only (very thin: month L0 n = 40,
   quarter Q1 n = 40, season L0 n = 49) and moves the full pre-2026 archive into
   **hindcast**, where it belongs. The old "operational" long-term numbers were
   really hindcast (e.g. old month L0 HSS 0.864 ≈ new **hindcast** HSS 0.875).
   Consequently the long-term sections below now **lead with the hindcast tables**
   (robust, full-archive skill) and present the genuine-2026 operational numbers
   only as a thin, preliminary footnote.
3. **Historical LR issue→target repair.** Pre-2024 short-term LR forecasts were
   mis-aligned by one period (issue vs target). This is now corrected, so the LR
   hindcast archive is valid and the LR short-term numbers can be trusted.
4. **Quarter mis-stratification (overloaded `horizon_value`).** The quarter
   horizon previously showed four bogus "leads" L1–L4 that were in fact the
   *target quarter* leaking through an overloaded `horizon_value`, with two
   forecast sources pooled and intra-quarter re-issues double-counted. Quarter is
   now **deduped** (one forecast per station / target quarter / year / model) and
   shown **per target quarter Q1–Q4**, each effectively a single-lead forecast;
   season is shown per **genuine lead 0–3** (re-issues deduped within each lead).

---

## What this report now covers — two thresholds

This report carries **two below-norm events reported side by side** and they
must **not** be compared cell-for-cell:

- **`below_norm` (value < 0.80 × norm)** — the operational **irrigation
  limit-plan / restriction** trigger. This is the primary story of the report:
  it is the boundary the river-basin organization actually uses to decide
  whether to impose a limit plan. It is the **rarer** event (lower base rate).
- **`below_norm_100` (value < 1.0 × norm)** — **plain below-average flow**: any
  period whose runoff falls short of the climatological norm. This is a
  **common** event (base rate typically ~0.4–0.8) and describes general
  below-average water availability rather than a restriction decision.

**Non-comparability caveat (applies everywhere both appear):** the two events
have **different base rates**, so their POD / FAR / HSS are **not comparable
directly**. HSS in particular is base-rate sensitive — a lower HSS on the
common 1.0 × norm event does *not* mean worse detection than on the rarer
0.80 × norm event. Read the 0.80 numbers as the operational-restriction result
and the 1.0 numbers as a separate description of below-average-flow detection.

---

## Purpose

Evaluate, for every forecast horizon, every model, and every station over the
entire DB archive, how often a runoff forecast would have driven the **wrong
irrigation water-distribution plan**.

The river basin organization issues a **limit plan** (farmers receive less
water) when forecast runoff falls below a configurable fraction of the
climatological norm — default **80 % of norm** (threshold editable). Otherwise
the **normal distribution plan** applies. The decision is the binary
classification `value < THRESHOLD × norm`, applied to both the forecast and the
observed runoff against the same boundary.

Error semantics (positive class = limit-plan event):

- **False positive (false alarm, FP):** forecast below threshold → limit plan
  imposed, but observed runoff was fine → farmers needlessly under-supplied.
- **False negative (miss, FN):** forecast at/above threshold → normal plan run,
  but observed runoff was actually scarce → over-allocation, shortage
  mid-season. **Operationally the costliest error.**

---

## Metric definitions

All metrics come from the 2×2 contingency table for the binary event
(positive class = **below-norm / limit-plan event**):

|                       | Observed below-norm | Observed fine |
|-----------------------|---------------------|---------------|
| **Forecast below-norm** | TP (hit)          | FP (false alarm) |
| **Forecast fine**       | FN (miss)         | TN (correct)  |

- **POD — Probability of Detection** = `TP / (TP + FN)`. Fraction of *genuine*
  below-norm events the forecast caught (1 = all, 0 = none). The miss rate is
  `FN rate = 1 − POD` — the operationally costly error (missed shortage).
- **FAR — False Alarm Ratio** = `FP / (TP + FP)`. Of all the times the forecast
  raised the alarm (predicted below-norm), the fraction that were wrong (water
  was actually fine). 0 = no false alarms. (Not the same as POFD = `FP/(FP+TN)`,
  which conditions on the non-events.)
- **POFD — Probability of False Detection** = `FP / (FP + TN)`. Fraction of
  *non-events* wrongly flagged.
- **CSI — Critical Success Index** = `TP / (TP + FP + FN)`. Hits over hits +
  both error types (ignores correct negatives).
- **Frequency bias** = `(TP + FP) / (TP + FN)`. >1 = over-forecasts the event,
  <1 = under-forecasts.
- **HSS — Heidke Skill Score** = accuracy relative to random chance, corrected
  for the base rate:
  `HSS = 2(TP·TN − FP·FN) / [(TP+FN)(FN+TN) + (TP+FP)(FP+TN)]`.
  1 = perfect, **0 = no better than chance / climatology**, <0 = worse than
  chance. Because of the base-rate correction, always saying "no event"
  (climatology) scores HSS 0, so any model with HSS > 0 genuinely beats
  climatology.
- **PSS — Peirce (Hanssen–Kuipers) Skill Score** = `POD − POFD`. −1 to 1; how
  well the forecast separates events from non-events. Unlike HSS it is
  independent of the base rate and cannot be hedged by over/under-forecasting.
- **POD CI 95%** = Wilson 95 % confidence interval on POD (robust for
  proportions near 0/1 and small n). Narrow = reliable; wide = few pairs.

---

## Coverage and data quality

n_pairs below are the `below_norm` event, pooled provenance, summed over all
models and leads. The **operational** column is the corrected genuine-forecast
count (short-term de-leaked; long-term genuine-2026 only); the **hindcast**
column is the robust full-archive count that the long-term sections now lead with.

| Horizon | n_pairs (all regimes) | n_pairs operational | n_pairs hindcast | Stations |
|---------|----------------------:|--------------------:|-----------------:|:--------:|
| day     | 6 789   | 260    | 6 529   | 66 |
| pentad  | 126 380 | 25 947 | 100 433 | 81 |
| decade  | 63 590  | 13 603 | 49 987  | 81 |
| month   | 548 790 | 8 836  | 539 954 | 53 |
| quarter | 30 709  | 826    | 29 883  | 53 |
| season  | 16 494  | 723    | 15 771  | 51 |

The **pooled station set is 83 distinct codes (65 Kyrgyz, 17 Tajik, 1 other)**;
not every station has data at every horizon, so per-horizon coverage ranges from
51 (season) to 81 (pentad/decade).

**Short-term operational counts fell sharply after de-leaking** (correction 1):
the per-target dedup and issue-before-target filter drop the intraday re-issues
and in-period nowcasts, so e.g. pentad EM operational n falls from 11 342 to
3 384. The remaining pairs are genuine forecasts.

**Long-term operational is now genuinely thin** (correction 2): only post-2026
real-time forecasts qualify as operational (month L0 n = 40, quarter Q1 n = 40,
season L0 n = 49). The robust long-term skill lives in the hindcast column
(month 539 954 pairs, quarter 29 883, season 15 771), which is why the long-term
sections lead with hindcast.

**Org balance:** 65 Kyrgyz stations vs 17 Tajik (plus 1 other). Long-term
n_pairs are dominated by Kyrgyz stations; Tajik pairs are thin at longer leads
and carry wide Wilson CIs.

**DAY horizon is exploratory:** operational n_pairs 56–108 across models after
de-leaking (POOLED). Results are consistent in direction but CIs are very wide;
do not over-interpret absolute numbers.

---

## Exclusion ledger

Total exclusions from the raw paired archive (2 460 466 excluded pairs):

| Reason | Count |
|--------|-------|
| `forecast_sentinel` | 1 376 188 |
| **`forecast_rolling_window`** | **549 868** |
| `observed_missing` | 210 001 |
| `forecast_actual_nan_flag` | 126 468 |
| `norm_unavailable_long_term` | 90 775 |
| `forecast_missing` | 64 476 |
| `observed_unmatched` | 35 936 |
| `norm_unavailable_lt_min_years` | 4 086 |
| `forecast_error_flag` | 2 147 |
| `observed_incomplete_month/quarter/season` | 521 |

**Rolling-window exclusion (549 868 rows):** the evaluator detected and
discarded the erroneous rolling-31-day product that had been stored in the
month archive. These rows correspond to forecasts that were issued daily with a
rolling 31-day target window rather than calendar-aligned month boundaries.
Retaining them would conflate the calendar-month skill with a fundamentally
different product and inflate n_pairs spuriously.

---

## norm_provenance breakdown

Metrics are computed separately by provenance; figures use the canonical
provenance per horizon:

| Horizon | Canonical provenance | Meaning |
|---------|---------------------|---------|
| day, pentad, decade | `calculated` | Station-level norm recomputed from DB archive |
| month | `official` | Official norms from the hydromet service |
| quarter, season | `aggregated_from_monthly` | Monthly official norms aggregated |

---

## Results — short-term horizons (day / pentad / decade)

Metrics below are **operational regime, basin=all, POOLED** (aggregated over
all stations, no per-station codes). Wilson 95 % CIs are shown for POD.

### Pentad (5-day) — most data, most reliable

| Model | TP | FP | FN | TN | n_pairs | base_rate | POD | FAR | HSS | PSS | POD CI 95 % |
|-------|-----|-----|-----|-----|---------|-----------|-----|-----|-----|-----|-------------|
| EM    | 1261 | 109 | 141 | 1873 | 3 384 | 0.41 | **0.899** | 0.080 | **0.847** | 0.844 | [0.883, 0.914] |
| TiDE  | 943  | 88  | 130 | 1371 | 2 532 | 0.42 | 0.879 | 0.085 | 0.823 | 0.819 | [0.858, 0.897] |
| TFT   | 815  | 84  | 112 | 1229 | 2 240 | 0.41 | 0.879 | 0.093 | 0.819 | 0.815 | [0.857, 0.899] |
| NE    | 897  | 88  | 121 | 1231 | 2 337 | 0.44 | 0.881 | 0.089 | 0.817 | 0.814 | [0.860, 0.900] |
| TSMixer | 843 | 93  | 128 | 1452 | 2 516 | 0.39 | 0.868 | 0.099 | 0.813 | 0.808 | [0.845, 0.888] |
| LR    | 3483 | 428 | 816 | 8211 | 12 938 | 0.33 | 0.810 | 0.109 | 0.778 | 0.761 | [0.798, 0.822] |

(De-leaked, corrected run: only genuine forecasts issued before the target
pentad, one per target. EM operational n falls from 11 342 to 3 384; HSS
0.871 → 0.847. LR keeps a larger n because it is archived at one issue per target.)

Key observations:
- **FP/FN balance:** EM is close to balanced (FP 109 vs FN 141); LR is strongly
  FN-heavy (FN 816 vs FP 428, ratio 0.52) — more missed limit-plan events.
- All ML models still outperform LR on HSS and PSS after de-leaking.
- **FN rate stays low** for top models: EM misses only ~10 % of true limit-plan
  events (POD 0.899). FAR 8.0 % is operationally acceptable.

#### Below-norm (1.0 × norm) — plain below-average flow (pentad)

Reported side by side with the 0.80 table above. **Do not compare cell-for-cell:**
the 1.0 × norm event is far more common (pentad base rate ≈ 0.64–0.68 vs ≈ 0.39–0.44
for 0.80 × norm), so its POD/FAR/HSS describe general below-average-flow
detection, not the limit-plan decision. HSS is compressed by the higher base
rate and is *not* directly comparable to the 0.80 HSS.

| Model | TP | FP | FN | TN | n_pairs | base_rate | POD | FAR | HSS | PSS | POD CI 95 % |
|-------|-----|-----|-----|-----|---------|-----------|-----|-----|-----|-----|-------------|
| EM    | 2115 | 126 | 129 | 1014 | 3 384 | 0.66 | **0.943** | 0.056 | **0.831** | 0.832 | [0.932, 0.951] |
| TFT   | 1373 | 85  | 102 | 680  | 2 240 | 0.66 | 0.931 | 0.058 | 0.815 | 0.820 | [0.917, 0.943] |
| NE    | 1482 | 98  | 103 | 654  | 2 337 | 0.68 | 0.935 | 0.062 | 0.803 | 0.805 | [0.922, 0.946] |
| TiDE  | 1581 | 109 | 122 | 720  | 2 532 | 0.67 | 0.928 | 0.064 | 0.794 | 0.797 | [0.915, 0.940] |
| TSMixer | 1467 | 110 | 135 | 804 | 2 516 | 0.64 | 0.916 | 0.070 | 0.791 | 0.795 | [0.901, 0.928] |
| LR    | 7151 | 723 | 678 | 4386 | 12 938 | 0.61 | 0.913 | 0.092 | 0.773 | 0.772 | [0.907, 0.919] |

At the plain below-average threshold, POD rises across the board (EM 0.943) —
most periods that fall short of the norm are detected — and FAR falls (EM 0.056)
because the event is common. This is a description of below-average-flow
detection only; the limit-plan decision remains the 0.80 table above.

### Decade (10-day)

| Model | TP | FP | FN | TN | n_pairs | base_rate | POD | FAR | HSS | PSS | POD CI 95 % |
|-------|-----|-----|-----|-----|---------|-----------|-----|-----|-----|-----|-------------|
| EM    | 639 | 71 | 117 | 1067 | 1 894 | 0.40 | **0.845** | 0.100 | **0.791** | 0.783 | [0.818, 0.869] |
| NE    | 514 | 78 | 91  | 705  | 1 388 | 0.44 | 0.850 | 0.132 | 0.752 | 0.750 | [0.819, 0.876] |
| TSMixer | 450 | 74 | 75 | 609 | 1 208 | 0.43 | 0.857 | 0.141 | 0.749 | 0.749 | [0.825, 0.884] |
| TiDE  | 464 | 75 | 87  | 709  | 1 335 | 0.41 | 0.842 | 0.139 | 0.749 | 0.746 | [0.809, 0.870] |
| TFT   | 458 | 79 | 90  | 722  | 1 349 | 0.41 | 0.836 | 0.147 | 0.739 | 0.737 | [0.802, 0.864] |
| LR    | 1488 | 235 | 618 | 4088 | 6 429 | 0.33 | 0.707 | 0.136 | 0.684 | 0.652 | [0.687, 0.726] |

(De-leaked, corrected run: only genuine forecasts issued before the target
decade, one per target. EM operational n falls from 5 086 to 1 894; POD
0.905 → 0.845, HSS 0.840 → 0.791. LR keeps a larger n because it is archived
at one issue per target.)

Key observations: same pattern as pentad — EM leads (POD 0.845, HSS 0.791),
LR is strongly FN-heavy (FN 618 vs FP 235, ratio 0.38) — more missed
limit-plan events. All ML models still clear LR on HSS after de-leaking.

#### Below-norm (1.0 × norm) — plain below-average flow (decade)

Side by side with the 0.80 table above. **Not comparable cell-for-cell:** the
1.0 × norm event is more common (decade base rate ≈ 0.60–0.69 vs ≈ 0.33–0.44 for
0.80 × norm); base-rate-sensitive HSS is compressed relative to the 0.80 story.

| Model | TP | FP | FN | TN | n_pairs | base_rate | POD | FAR | HSS | PSS | POD CI 95 % |
|-------|-----|-----|-----|-----|---------|-----------|-----|-----|-----|-----|-------------|
| EM    | 1167 | 98 | 85 | 544 | 1 894 | 0.66 | **0.932** | 0.077 | **0.783** | 0.779 | [0.917, 0.945] |
| TFT   | 816  | 78 | 79 | 376 | 1 349 | 0.66 | 0.912 | 0.087 | 0.740 | 0.740 | [0.891, 0.929] |
| TiDE  | 829  | 89 | 73 | 344 | 1 335 | 0.68 | 0.919 | 0.097 | 0.720 | 0.714 | [0.899, 0.935] |
| NE    | 886  | 95 | 75 | 332 | 1 388 | 0.69 | 0.922 | 0.097 | 0.709 | 0.699 | [0.903, 0.937] |
| TSMixer | 770 | 86 | 69 | 283 | 1 208 | 0.69 | 0.918 | 0.100 | 0.694 | 0.685 | [0.897, 0.935] |
| LR    | 3355 | 505 | 519 | 2050 | 6 429 | 0.60 | 0.866 | 0.131 | 0.668 | 0.668 | [0.855, 0.876] |

### Day (exploratory, n_pairs 56–108)

| Model | n_pairs | POD | FAR | HSS | PSS |
|-------|---------|-----|-----|-----|-----|
| TFT     | 56  | 0.571 | 0.059 | 0.536 | 0.536 |
| TiDE    | 108 | 0.514 | 0.182 | 0.509 | 0.459 |
| TSMixer | 96  | 0.517 | 0.375 | 0.403 | 0.383 |

Day-horizon skill is substantially lower than pentad/decade. POD ~0.51–0.57
with HSS ~0.40–0.54. After de-leaking, the day operational archive collapses to
only 56–108 genuine pairs per model, so the CIs are very wide (±0.15–0.17 on
POD); interpret direction only, not absolute numbers. The tiny sample reflects
that the day-resolution ML archive is limited to approximately the last 1–2
years (vs 15+ years for pentad/decade) and that de-leaking keeps only one
genuine forecast per target.

#### Below-norm (1.0 × norm) — plain below-average flow (day)

Side by side with the 0.80 day table above. **Not comparable cell-for-cell:**
the day 1.0 × norm base rate (≈ 0.61–0.84) is far higher than the 0.80 × norm
base rate (≈ 0.30–0.50), so HSS is not comparable across the two events.

| Model | n_pairs | POD | FAR | HSS | PSS |
|-------|---------|-----|-----|-----|-----|
| TFT     | 56  | 0.723 | 0.029 | 0.398 | 0.612 |
| TiDE    | 108 | 0.682 | 0.224 | 0.360 | 0.372 |
| TSMixer | 96  | 0.705 | 0.127 | 0.197 | 0.261 |

---

## Results — long-term horizons (month / quarter / season)

Long-term horizons emit **no lead-aggregated POOLED row**. Every metric is
per-lead. Figures label these `month L0`, `month L1`, etc. where lead 0 is
the nearest forecast (i.e., smallest forecast offset).

**Regime note (correction 2).** Real-time long-term forecasting at these sites
only began around 2026, so the genuine-operational archive is very thin (month
L0 n = 40, quarter Q1 n = 40, season L0 n = 49) and cannot carry a reliable
skill estimate. The **robust** long-term skill lives in the full pre-2026
**hindcast** archive. Each section below therefore **leads with the hindcast
tables** and reports the genuine-2026 operational numbers only as a short
*preliminary — too thin to score* footnote. The operational-vs-hindcast contrast
is the subject of **fig3**.

### Month — HINDCAST (EM, POOLED, official provenance) — robust

| Lead | TP | FP | FN | TN | n_pairs | POD | FAR | HSS | PSS | POD CI 95 % |
|------|-----|-----|-----|------|---------|-----|-----|-----|-----|-------------|
| L0   | 1402 | 96 | 207 | 5376 | 7 081 | **0.871** | 0.064 | **0.875** | 0.854 | [0.854, 0.887] |
| L1   | 375  | 32 | 83  | 1675 | 2 165 | 0.819 | 0.079 | 0.834 | 0.800 | [0.781, 0.851] |
| L2   | 96   | 2  | 30  | 585  | 713   | 0.762 | 0.020 | 0.831 | 0.758 | [0.680, 0.828] |
| L3   | 52   | 7  | 28  | 557  | 644   | 0.650 | 0.119 | 0.719 | 0.638 | [0.541, 0.745] |

HSS degrades gently from L0 (0.875) to L3 (0.719). The hindcast archive is
robust (L0 n_pairs 7 081). Leads L4–L12 exist but are sparse and concentrated on
a small set of Kyrgyz stations; the figures cap month at L0–L3.

Among all long-term models, EM leads the month L0 hindcast (HSS 0.875), just
ahead of **Skilled Mean** (POD 0.877, FAR 0.095, HSS 0.862) and well above the
**Naive Mean** (POD 0.778, HSS 0.794), MC_ALD (0.819), GBT (0.789) and LR_Base
(0.776). The skill-weighted ensemble genuinely beats the straight average.

**Preliminary — genuine-2026 operational (too thin to score).** Month
operational EM: L0 n = 40 (POD 0.571, HSS 0.688), L1 n = 21 (POD/HSS = 1.000, a
20-event artefact), L2 n = 14, L3 n = 5. These carry Wilson CIs so wide they
convey no reliable signal; use the hindcast table above. (Across all models the
best-sampled operational L0 cell is MC_ALD at n = 134, HSS 0.820 — still an
order of magnitude below the hindcast sample.)

#### Below-norm (1.0 × norm) — plain below-average flow (month hindcast, EM per-lead)

Side by side with the 0.80 hindcast table above. **Not comparable cell-for-cell:**
the 1.0 × norm month event is much more common (base rate ≈ 0.32–0.48 vs
≈ 0.12–0.23 for 0.80 × norm), and base-rate-sensitive HSS shifts accordingly.

| Lead | TP | FP | FN | TN | n_pairs | POD | FAR | HSS | PSS | POD CI 95 % |
|------|-----|-----|-----|------|---------|-----|-----|-----|-----|-------------|
| L0   | 3172 | 154 | 247 | 3508 | 7 081 | **0.928** | 0.046 | **0.887** | 0.886 | [0.919, 0.936] |
| L1   | 906  | 72  | 111 | 1076 | 2 165 | 0.891 | 0.074 | 0.830 | 0.828 | [0.870, 0.909] |
| L2   | 250  | 10  | 32  | 421  | 713   | 0.887 | 0.038 | 0.875 | 0.863 | [0.844, 0.918] |
| L3   | 172  | 17  | 34  | 421  | 644   | 0.835 | 0.090 | 0.814 | 0.796 | [0.778, 0.879] |

### Quarter — HINDCAST (EM, POOLED, aggregated_from_monthly) — robust, by target quarter

Quarter is now **deduped** (one forecast per station / target quarter / year /
model) and stratified by **target quarter Q1–Q4**, each effectively a
single-lead forecast. (The earlier draft's L1–L4 "leads" were an artefact of an
overloaded `horizon_value` — see **Corrections in this revision**.)

| Target quarter | TP | FP | FN | TN | n_pairs | POD | FAR | HSS | PSS | POD CI 95 % |
|------|-----|-----|-----|------|---------|-----|-----|-----|-----|-------------|
| Q1 (Jan–Mar) | 35 | 7  | 34 | 789 | 865 | 0.507 | 0.167 | **0.607** | 0.498 | [0.392, 0.622] |
| Q2 (Apr–Jun) | 25 | 18 | 0  | 39  | 82  | **1.000** | 0.419 | 0.569 | 0.684 | [0.867, 1.000] |
| Q3 (Jul–Sep) | 36 | 9  | 17 | 82  | 144 | 0.679 | 0.200 | 0.599 | 0.580 | [0.545, 0.789] |
| Q4 (Oct–Dec) | 27 | 16 | 79 | 767 | 889 | 0.255 | 0.372 | **0.315** | 0.234 | [0.181, 0.345] |

**Q4 (Oct–Dec) is markedly harder than Q1 (Jan–Mar).** The two well-sampled
quarters carry the operational story: EM hindcast HSS falls from **0.607 at Q1**
(POD 0.507, base rate 0.08) to **0.315 at Q4** (POD 0.255, base rate 0.12) —
autumn/early-winter below-norm quarters are missed far more often (Q4: 79 misses
vs only 27 hits). **Q2 and Q3 are thin for EM** (n = 82 and 144), so their high
POD/HSS should be read with caution (Q2 POD 1.000 is a zero-miss, small-n
artefact); the robust EM signal is Q1 and Q4. At the quarter horizon the
below-norm event is rare in the well-sampled quarters (base rate ≈ 0.08–0.12)
and hard to catch.

**Preliminary — genuine-2026 operational (too thin to score).** Real-time 2026
so far covers only the year's first quarters: quarter operational EM Q1 n = 40
(POD 0.667, FAR 0.250, HSS 0.627), Q2 n = 2 (undefined). Rely on the hindcast
table.

#### Below-norm (1.0 × norm) — plain below-average flow (quarter hindcast, EM by target quarter)

Side by side with the 0.80 hindcast table above. **Not comparable cell-for-cell:**
the 1.0 × norm quarter event is far more common (base rate ≈ 0.31–0.62 vs
≈ 0.08–0.37 for 0.80 × norm); HSS shifts with the base rate.

| Target quarter | TP | FP | FN | TN | n_pairs | POD | FAR | HSS | PSS | POD CI 95 % |
|------|-----|-----|-----|------|---------|-----|-----|-----|-----|-------------|
| Q1 (Jan–Mar) | 177 | 31 | 87  | 570 | 865 | 0.670 | 0.149 | 0.658 | 0.619 | [0.612, 0.724] |
| Q2 (Apr–Jun) | 39  | 16 | 5   | 22  | 82  | 0.886 | 0.291 | 0.475 | 0.465 | [0.760, 0.950] |
| Q3 (Jul–Sep) | 70  | 10 | 19  | 45  | 144 | 0.787 | 0.125 | 0.586 | 0.605 | [0.690, 0.859] |
| Q4 (Oct–Dec) | 176 | 62 | 128 | 523 | 889 | 0.579 | 0.261 | 0.499 | 0.473 | [0.523, 0.633] |

The per-quarter figures **fig1_quarter** and **fig4_quarter** now show the Q1–Q4
target-quarter stratification (they previously showed the bogus L1–L4 leads).

### Season — HINDCAST (EM, POOLED, aggregated_from_monthly) — robust, by genuine lead

Season is shown by its **genuine forecast leads 0–3** (re-issues deduped within
each lead, ~870 pairs per lead). Lead 0 is the nearest forecast.

| Lead | TP | FP | FN | TN | n_pairs | POD | FAR | HSS | PSS | POD CI 95 % |
|------|-----|-----|-----|-----|---------|-----|-----|-----|-----|-------------|
| L0   | 91 | 36 | 97  | 659 | 883 | 0.484 | 0.283 | **0.490** | 0.432 | [0.414, 0.555] |
| L1   | 69 | 36 | 116 | 655 | 876 | 0.373 | 0.343 | 0.381 | 0.321 | [0.307, 0.445] |
| L2   | 63 | 40 | 119 | 649 | 871 | 0.346 | 0.388 | 0.343 | 0.288 | [0.281, 0.418] |
| L3   | 58 | 42 | 125 | 639 | 864 | 0.317 | 0.420 | 0.306 | 0.255 | [0.254, 0.388] |

Seasonal forecasts carry only **modest** positive skill (HSS 0.31–0.49) and low
POD (0.32–0.48) across all leads: **roughly 52–68 % of true limit-plan seasons
are missed**, consistent with the fundamental difficulty of seasonal prediction.
FAR is elevated (0.28–0.42) — around 1 in 3 season-ahead limit-plan signals is a
false alarm. Nevertheless HSS > 0 confirms skill over climatology at every lead,
and HSS degrades gently from L0 (0.490) to L3 (0.306). At season, EM and Naive
Mean coincide (the ensemble reduces to the straight average), and **LR_Base** is
competitive (L0 HSS 0.419, L1 0.362) — no clear ML advantage at the seasonal
scale.

**Preliminary — genuine-2026 operational (too thin to score).** Season
operational EM: L0 n = 49 (POD 0.375, FAR 0.500, HSS 0.206), L1 n = 49 (POD
0.375, HSS 0.171). Directionally consistent with the hindcast (weak positive
skill) but far too few pairs for a firm estimate.

#### Below-norm (1.0 × norm) — plain below-average flow (season hindcast, EM per-lead)

Side by side with the 0.80 hindcast table above. **Not comparable cell-for-cell:**
the 1.0 × norm season event is much more common (base rate ≈ 0.50 vs ≈ 0.21 for
0.80 × norm), so its higher POD/HSS reflect the higher base rate — not stronger
limit-plan detection.

| Lead | TP | FP | FN | TN | n_pairs | POD | FAR | HSS | PSS | POD CI 95 % |
|------|-----|-----|-----|-----|---------|-----|-----|-----|-----|-------------|
| L0   | 315 | 81 | 124 | 363 | 883 | 0.718 | 0.205 | 0.535 | 0.535 | [0.674, 0.758] |
| L1   | 295 | 79 | 139 | 363 | 876 | 0.680 | 0.211 | 0.502 | 0.501 | [0.634, 0.722] |
| L2   | 285 | 82 | 146 | 358 | 871 | 0.661 | 0.223 | 0.476 | 0.475 | [0.615, 0.704] |
| L3   | 272 | 82 | 157 | 353 | 864 | 0.634 | 0.232 | 0.446 | 0.446 | [0.587, 0.678] |

---

## Baseline comparison

### Climatology

Climatology forecasts the norm — it never predicts a below-norm event, so it
issues no limit-plan signals.  By construction:

- **POD = 0** — misses 100 % of all true below-norm events
- **FAR = 0** — no false alarms (never alarms at all)
- **HSS = 0** — no skill above chance

Every evaluated model achieves **HSS > 0** at all horizons, confirming positive
skill over the trivially no-skill climatology baseline.

Pooled-operational base rates (fraction of all periods that are genuinely
below-norm) and the events climatology misses entirely:

(Base rates are for the operational **0.80 × norm** limit-plan event.)

Short-term rows are operational (de-leaked); long-term rows are hindcast (the
robust regime these sections lead with).

| Horizon | Base rate | Events missed by climatology |
|---------|-----------|------------------------------|
| day     | ≈ 0.30–0.50 | 30–50 % of all day-periods (varies by model) |
| pentad  | 0.41      | 41 % of all pentad-periods |
| decade  | 0.40      | 40 % of all decade-periods |
| month (L0, hindcast) | 0.23  | 23 % of all month-periods |
| quarter (Q1, hindcast) | 0.08 | 8 % of all quarter-periods (Q1) |
| season (L0, hindcast) | 0.21  | 21 % of all season-periods |

**Are our forecasts better than climatology?**

Yes — decisively for short-term, positive but weaker for long-term.

- **Pentad EM** (POD 0.899, HSS 0.847 vs climatology POD 0, HSS 0): EM
  catches 89.9 % of the below-norm shortage events that climatology misses
  entirely.  At a pentad base rate of 0.41, climatology flags zero events; EM
  flags 90 % of the genuine ones.
- **Decade EM** (POD 0.845, HSS 0.791): EM catches 84.5 % of events
  climatology misses.  Stronger than LR (POD 0.707, HSS 0.684).
- **Month L0 EM / Skilled Mean** (hindcast POD 0.871–0.877, HSS 0.875–0.862):
  strong skill; climatology misses all 23 % of below-norm months.  Monthly
  forecasts are reliable enough to inform seasonal irrigation planning one month
  ahead.
- **Quarter Q1 EM** (hindcast POD 0.507, HSS 0.607): positive and moderate.
  Climatology misses all 8 % of below-norm Q1 quarters; EM catches 50.7 % of them
  (other models do better — Skilled Mean HSS 0.736). Q4 is much harder (HSS
  0.315) — see the per-target-quarter Quarter section.
- **Season L0 EM** (hindcast POD 0.484, HSS 0.490): weak but positive.
  Climatology misses all 21 % of below-norm seasons; EM catches 48.4 % of them.

In every fig4 panel, a labelled annotation shows the climatology reference
(POD = 0, HSS = 0, base rate) so the gain from any real model is immediately
visible.

### Naive Mean (unweighted ensemble mean)

**Naive Mean is NOT a climatology baseline.**  It is the *unweighted* mean of
all model forecast outputs — the straight average across all models with no
skill-based weighting — and serves as a lower bound for what any
ensemble-averaging strategy should achieve.  Climatology and Naive Mean are
distinct concepts:

| Reference | Definition | POD (pentad) | HSS (pentad) |
|-----------|-----------|-------------|-------------|
| Climatology | Always predicts norm, never alarms | 0.000 | 0.000 |
| Naive Mean  | Unweighted average of all model outputs | ~0.8–0.9 | ~0.8–0.9 |
| EM (best ensemble) | Skill-weighted ensemble | 0.899 | 0.847 |

In fig4, Naive Mean appears with a **purple** tick label (family "Unweighted
mean"), clearly separated from the green skill-weighted ensembles (EM, NE,
Skilled Mean) and from the climatology annotation in each panel.

### LR / LR_Base proxy

For short-term horizons (where LR is directly available), LR is included as a
named model.  For long-term horizons, LR_Base is the proxy.

Summary of best model vs. climatology vs. LR:
- **Pentad EM**: HSS 0.847 — far above climatology (0.00). Beats LR (0.778).
- **Decade EM**: HSS 0.791 — far above climatology. Beats LR (0.684).
- **Month L0 EM** (hindcast): HSS 0.875 — strong skill. Naive Mean HSS 0.794.
- **Season L0 EM** (hindcast): HSS 0.490 — positive but modest; LR_Base similar
  (0.419).

For pentad/decade, ML ensemble models (EM, NE) substantially exceed the LR
baseline. For season, the gap narrows and LR_Base is competitive with EM.

### Persistence (last observed flow)

Persistence uses the most recently observed flow as the forecast.  It is a
far stronger baseline than climatology because it carries real signal at
short time-scales (auto-correlation in streamflow), whereas climatology
carries none.  The phase-2 evaluation adds persistence to `baselines.csv`,
matched to the same forecast instances as each comparison model (same n).

Persistence metrics (operational, POOLED, basin = all, season = all,
canonical provenance; comparison model = EM except day = TFT):

| Horizon | Persistence POD | Persistence FAR | Persistence HSS | n_matched |
|---------|----------------|----------------|----------------|-----------|
| day     | 1.000 | 0.152 | **0.821** | 56 |
| pentad  | 0.859 | 0.156 | 0.745 | 3 380 |
| decade  | 0.780 | 0.216 | 0.639 | 1 876 |
| month L0 | 0.583 | 0.369 | 0.496 | 7 057 |
| quarter Q1 | 0.368 | 0.671 | 0.287 | 846 |
| season L0 | 0.376 | 0.606 | 0.226 | 847 |

(Short-term rows are operational and de-leaked, matched to the same instances as
the comparison model — hence n_matched drops with the de-leaked archive. Month /
quarter / season rows are hindcast, matched to the EM hindcast.)

**Three-way skill ladder — climatology / persistence / best model:**

| Horizon | Climatology HSS | Persistence HSS | Best model HSS | Winner |
|---------|:--------------:|:--------------:|:-------------:|--------|
| day     | 0 | 0.821 | 0.536 (TFT) | **Persistence** |
| pentad  | 0 | 0.745 | 0.847 (EM)   | EM |
| decade  | 0 | 0.639 | 0.791 (EM)   | EM |
| month L0 | 0 | 0.496 | 0.875 (EM)  | EM |
| quarter Q1 | 0 | 0.287 | 0.607 (EM) | EM |
| season L0 | 0 | 0.226 | 0.490 (EM)  | EM |

Key findings:

- **Everything beats climatology.** All candidate models and persistence have
  HSS > 0 at every horizon.  Climatology (POD = 0, HSS = 0) is decisively
  below both persistence and the trained models.
- **Candidate models beat persistence from pentad onwards.**  At pentad and
  longer horizons the ensemble and ML models consistently outperform
  persistence, with the gap growing at longer leads: +0.10 at pentad,
  +0.15 at decade, +0.38 at month L0.  This is the expected pattern for
  well-trained models — as persistence degrades with longer forecast lead,
  the models' learned climatological signal takes over.
- **At the day scale, persistence beats our thin ML models.**  Day-horizon
  persistence achieves HSS 0.82 against only 0.40–0.54 for TFT/TSMixer/TiDE.
  This is largely a sample-size effect: after de-leaking, day-horizon ML
  forecasts are drawn from a tiny operational archive (n = 56–108 vs ~3 400 at
  pentad).  With more operational data the gap would likely close, but day-ahead
  persistence (last observed flow) is a very strong competitor at this scale.

The three-way ladder is visualised in **fig5_persistence_vs_models.png**.
Fig4 panels (per-horizon POD/FAR bars) now include both climatology and
persistence as reference annotations in the lower-right corner.

---

## Lead-time stratification summary

Long-term rows are hindcast (the robust regime).

| Horizon | Best lead | HSS (best) | HSS (worst shown) | Degradation |
|---------|-----------|-----------|-------------------|-------------|
| day     | n/a (no lead) | 0.536 (TFT) | — | — |
| pentad  | n/a       | 0.847 (EM) | — | — |
| decade  | n/a       | 0.791 (EM) | — | — |
| month   | L0        | 0.875 (EM) | 0.719 (L3) | −0.16 over 3 leads |
| quarter | Q1 (target qtr) | 0.607 (EM) | 0.315 (Q4) | by target quarter, not lead |
| season  | L0        | 0.490 (EM) | 0.306 (L3) | −0.18 over 3 leads |

Month shows the clearest monotonic degradation with lead. Season degrades more
slowly but is moderate-to-weak at all leads. **Quarter is no longer
lead-stratified** — it is now reported per target quarter (Q1–Q4); Q1 (Jan–Mar)
is the best-sampled, strongest quarter (HSS 0.607) and Q4 (Oct–Dec) the hardest
(HSS 0.315), so the "best/worst" columns above are Q1 and Q4 rather than leads.

---

## Seasonal disaggregation (irrigation season Apr–Sep)

Phase-2 re-run splits every metric into three season cuts:
- **irrigation** — Apr–Sep (the growing and water-distribution season; the
  period when the limit-plan decision has the greatest operational cost)
- **non_irrigation** — Oct–Mar (low-demand period)
- **all** — full year (same as the numbers in the sections above)

All rows below are **basin = all, EM model, canonical provenance**. Short-term
horizons (pentad, decade) are **operational** (de-leaked); month L0 and
season L0 are **hindcast** (the robust regime these long-term sections lead
with).

### Seasonal POD / FAR / HSS table (EM, POOLED)

Both events are shown. **The two events are not comparable cell-for-cell**
(different base rates); the 0.80 × norm rows are the limit-plan story, the
1.0 × norm rows describe plain below-average flow.

| Horizon | Event | Season | n | POD | FAR | HSS | POD CI 95 % |
|---------|-------|--------|---|-----|-----|-----|-------------|
| pentad | 0.80 | all | 3 384 | 0.899 | 0.080 | 0.847 | [0.883, 0.914] |
| pentad | 0.80 | irrigation (Apr–Sep) | 1 687 | 0.893 | 0.086 | 0.814 | [0.870, 0.912] |
| pentad | 0.80 | non-irrigation (Oct–Mar) | 1 697 | **0.908** | 0.071 | **0.877** | [0.882, 0.929] |
| pentad | 1.0  | all | 3 384 | 0.943 | 0.056 | 0.831 | [0.932, 0.951] |
| pentad | 1.0  | irrigation (Apr–Sep) | 1 687 | 0.940 | 0.060 | 0.788 | [0.925, 0.952] |
| pentad | 1.0  | non-irrigation (Oct–Mar) | 1 697 | **0.946** | 0.052 | **0.864** | [0.930, 0.958] |
| decade | 0.80 | all | 1 894 | 0.845 | 0.100 | 0.791 | [0.818, 0.869] |
| decade | 0.80 | irrigation (Apr–Sep) | 909 | 0.844 | 0.103 | 0.763 | [0.807, 0.876] |
| decade | 0.80 | non-irrigation (Oct–Mar) | 985 | **0.846** | 0.096 | **0.813** | [0.804, 0.881] |
| decade | 1.0  | all | 1 894 | 0.932 | 0.077 | 0.783 | [0.917, 0.945] |
| decade | 1.0  | irrigation (Apr–Sep) | 909 | 0.924 | 0.085 | 0.716 | [0.901, 0.942] |
| decade | 1.0  | non-irrigation (Oct–Mar) | 985 | **0.940** | 0.069 | **0.833** | [0.919, 0.957] |
| month L0 (hind) | 0.80 | all | 7 081 | 0.871 | 0.064 | 0.875 | [0.854, 0.887] |
| month L0 (hind) | 0.80 | irrigation (Apr–Sep) | 2 853 | **0.872** | 0.070 | 0.850 | [0.850, 0.892] |
| month L0 (hind) | 0.80 | non-irrigation (Oct–Mar) | 4 228 | 0.869 | 0.055 | **0.890** | [0.841, 0.894] |
| month L0 (hind) | 1.0  | all | 7 081 | 0.928 | 0.046 | 0.887 | [0.919, 0.936] |
| month L0 (hind) | 1.0  | irrigation (Apr–Sep) | 2 853 | **0.930** | 0.048 | 0.856 | [0.916, 0.941] |
| month L0 (hind) | 1.0  | non-irrigation (Oct–Mar) | 4 228 | 0.926 | 0.044 | **0.901** | [0.913, 0.937] |
| season L0 (hind) | 0.80 | all / irrigation | 883 | 0.484 | 0.283 | 0.490 | [0.414, 0.555] |
| season L0 (hind) | 1.0  | all / irrigation | 883 | 0.718 | 0.205 | 0.535 | [0.674, 0.758] |

**Note on season horizon:** All season-ahead forecasts (Apr–Sep seasonal
runoff) target the irrigation season by definition.  There is no
non-irrigation split for the season horizon.

### Interpretation

**POD is essentially comparable in and out of the irrigation season.**
After de-leaking, the earlier in-season POD advantage disappears: at the pentad
scale (0.80 × norm), EM detects 89.3 % of below-norm events during Apr–Sep vs
90.8 % during Oct–Mar (difference −1.5 pp, well within CI overlap); at decade the
two are effectively equal (84.4 % vs 84.6 %); at month L0 in-season is marginally
higher (87.2 % vs 86.9 %). The takeaway is that **detection is stable across the
year** — there is no meaningful in-season penalty, which is what matters
operationally.

**HSS is lower in-season** at every horizon (pentad 0.814 vs 0.877; decade 0.763
vs 0.813; month L0 0.850 vs 0.890).  This is a base-rate effect: below-norm
events are more frequent in the irrigation season (e.g. pentad base rate 0.49
in-season vs 0.34 out-of-season), which raises the chance baseline and compresses
HSS even though raw detection is unchanged.

**Operational significance:** the irrigation season is precisely when the
limit-plan decision matters most.  The finding confirms that the EM model detects
shortages just as well in-season as out-of-season, so irrigation managers can
rely on the pooled-year skill estimates as a representative estimate of in-season
performance.

The seasonal disaggregation figures are shown in **fig6_season_pod.png**
(POD with Wilson CI, irrigation vs non-irrigation vs all, for pentad,
decade, and month L0).

---

## Interpretation for the irrigation decision

The key operational question is: **what fraction of true below-norm runoff
events are missed** (FN rate = 1 − POD)?

- **Pentad/decade (ML models):** FN rate 10–16 % (pentad EM 10 %, decade EM
  15 %). False alarms (FP rate relative to positive events) ≈ 8–15 %. Both are
  operationally low. An irrigation manager using EM pentad forecasts would miss
  roughly 1 in 10 genuine shortage events.
- **Pentad/decade (LR):** FN rate 19–29 %. LR is FN-heavy — much more likely
  to miss a genuine shortage. ML models offer a clear advantage.
- **Month L0 (EM / Skilled Mean, hindcast):** FN rate ≈ 12–13 %. Strong skill,
  comparable to short-term ML. Monthly forecasts are reliable enough to inform
  seasonal irrigation planning one month ahead. (Genuine-2026 operational month
  data is too thin to score — see the Month section.)
- **Month L3 (EM, hindcast):** FN rate 35 %. Skill degrades substantially at
  3-month lead. Three-month-ahead seasonal signals should be treated as
  directional guidance only.
- **Quarter (by target quarter) / Season (by lead) (hindcast):** FN rate ≈
  49–75 % (EM) — quarter Q1 ≈ 49 % rising to ≈ 75 % at Q4; season 52–68 % across
  leads. In most cells more than half of true limit-plan events are missed. These
  horizons provide only weak-to-moderate positive skill (strongest at quarter
  Q1) — useful for ensemble signal but not, in general, for firm operational
  decisions. FAR is also elevated (17–42 %).
- **Day (exploratory):** FN rate 43–49 %. Very low coverage after de-leaking
  (n = 56–108) and inherent day-scale variability mean day-ahead limit-plan
  decisions carry substantial uncertainty.

**Recommendation:** rely on pentad/decade ML ensemble (EM or NE) for the core
irrigation planning decision. Monthly (L0–L1) forecasts can support 1–2 month
ahead preparation. Treat quarter/season forecasts as risk-screening tools, not
firm allocation triggers.

---

## Percentile-based extreme-event detection (low- and high-flow)

The irrigation decision above uses the fixed **0.80 × norm** threshold. This
section generalises the evaluation to **empirical-percentile events** — a
distinct use case from the limit-plan decision — covering both tails of the
flow distribution:

- **Low-flow (drought):** observed/forecast runoff **below** the 10th
  (`low_p10`) and 5th (`low_p5`) percentile — severe and extreme shortage,
  relevant to drought response beyond the routine limit plan.
- **High-flow (flood / hydropower / reservoir):** runoff **above** the 90th
  (`high_p90`) and 95th (`high_p95`) percentile — a separate operational
  concern (flood warning, reservoir filling, hydropower scheduling).

Percentile thresholds are **empirical, computed per station and per
period-of-year** from the observed archive (same `min_years ≥ 10` gate as the
norm), so each station/period carries its own seasonal thresholds. The
contingency machinery is identical to the below-norm case: positive class =
event occurred (below the threshold for low-flow, above it for high-flow).

Numbers below are from the corrected run (`rerun_2026-07-03_corrected/`), which
carries the four percentile events alongside the below-norm events. Rows are
**EM, POOLED, basin = all, season = all, canonical provenance**; short-term
horizons are **operational** (de-leaked), long-term horizons use **hindcast** at
the smallest lead (month / season L0) and first target quarter (quarter Q1) —
matching the regime the long-term sections lead with.

### Table — EM, POOLED, canonical provenance, season = all (short-term operational, long-term hindcast)

| Horizon | Event | base rate | POD | FAR | HSS | POD 95 % CI | n |
|---------|-------|:---------:|:---:|:---:|:---:|:-----------:|---:|
| pentad | low_p5 | 0.13 | 0.70 | 0.25 | 0.68 | [0.66, 0.74] | 3 358 |
| pentad | low_p10 | 0.21 | 0.77 | 0.20 | 0.73 | [0.74, 0.80] | 3 358 |
| pentad | high_p90 | 0.09 | 0.75 | 0.24 | 0.73 | [0.70, 0.80] | 3 358 |
| pentad | high_p95 | 0.06 | 0.64 | 0.27 | 0.67 | [0.57, 0.71] | 3 358 |
| decade | low_p5 | 0.14 | 0.61 | 0.30 | 0.60 | [0.55, 0.67] | 1 879 |
| decade | low_p10 | 0.22 | 0.74 | 0.20 | 0.71 | [0.69, 0.78] | 1 879 |
| decade | high_p90 | 0.09 | 0.68 | 0.20 | 0.71 | [0.61, 0.75] | 1 879 |
| decade | high_p95 | 0.05 | 0.62 | 0.26 | 0.66 | [0.53, 0.72] | 1 879 |
| month L0 (hind) | low_p5 | 0.12 | 0.49 | 0.18 | 0.58 | [0.46, 0.53] | 7 035 |
| month L0 (hind) | low_p10 | 0.17 | 0.69 | 0.14 | 0.72 | [0.66, 0.71] | 7 035 |
| month L0 (hind) | high_p90 | 0.08 | **0.74** | 0.18 | **0.76** | [0.71, 0.78] | 7 035 |
| month L0 (hind) | high_p95 | 0.05 | 0.43 | 0.24 | 0.53 | [0.38, 0.48] | 7 035 |
| quarter Q1 (hind) | low_p5 | 0.05 | 0.23 | 0.52 | 0.29 | [0.13, 0.38] | 862 |
| quarter Q1 (hind) | low_p10 | 0.10 | 0.30 | 0.38 | 0.36 | [0.21, 0.40] | 862 |
| quarter Q1 (hind) | high_p90 | 0.11 | 0.32 | 0.36 | 0.38 | [0.24, 0.42] | 862 |
| quarter Q1 (hind) | high_p95 | 0.05 | 0.29 | 0.48 | 0.35 | [0.18, 0.43] | 862 |
| season L0 (hind) | low_p5 | 0.07 | 0.11 | 0.50 | 0.17 | [0.06, 0.22] | 882 |
| season L0 (hind) | low_p10 | 0.11 | 0.15 | 0.50 | 0.19 | [0.10, 0.24] | 882 |
| season L0 (hind) | high_p90 | 0.10 | 0.32 | 0.41 | 0.37 | [0.23, 0.42] | 882 |
| season L0 (hind) | high_p95 | 0.07 | 0.18 | 0.54 | 0.23 | [0.10, 0.29] | 882 |

**Day horizon:** no percentile events are reported. The `min_years ≥ 10` gate
cannot form daily percentile thresholds — a daily percentile needs ~10 years of
observations for each day-of-year period, which the thin day archive
(~1–2 years) does not provide. (The below-norm day decision uses the norm, not
an empirical percentile, so it is unaffected.)

### Key findings

- **High-flow detection is genuinely skilful at pentad, decade, and month L0.**
  EM catches 68–75 % of 90th-percentile high-flow events (HSS 0.71–0.76), with
  **month L0 the strongest** (POD 0.74, HSS 0.76, FAR 0.18). This is the
  principal new result: the same models that drive the irrigation decision also
  provide usable flood / high-flow signal at short-to-medium range.
- **Extreme tails are harder than moderate tails.** Detection drops from the
  moderate percentile to the extreme one at every horizon — e.g. pentad
  high_p90 0.75 → high_p95 0.64; month L0 high_p90 0.74 → high_p95 0.43. Rarer
  events (lower base rate) are intrinsically harder to catch and carry wider
  CIs.
- **Low-flow percentiles track the below-norm story but are more demanding.**
  The 10th/5th percentiles are stricter thresholds than 0.80 × norm, so POD is
  lower than the below-norm POD at the same horizon (pentad low_p10 0.77 vs
  below-norm 0.90; month L0 low_p10 0.69 vs 0.87). Pentad/decade still catch
  74–77 % of moderate (10th-percentile) low-flow events.
- **Quarter and season remain weak across all percentile events** (POD
  0.10–0.32; quarter shown at target quarter Q1), consistent with the below-norm
  finding — long-range extreme detection is risk-screening only.

**Caveats specific to percentile events:**

- Percentile events use a slightly smaller station set than below-norm (the
  `min_years ≥ 10` gate on empirical percentiles drops stations with short
  records), so n is modestly lower than the below-norm columns.
- The most extreme events (p5 / p95) have low base rates (0.03–0.10) and
  correspondingly wide Wilson CIs; treat single-horizon extreme numbers as
  indicative.
- Per-station and per-event detail (including low/high tails) is explorable in
  the Streamlit dashboard
  (`apps/forecast_skill_eval/src/forecast_skill_eval/dashboard/`).

---

## Return-period detection (flood / hydropower)

> **Status — not recomputed in the corrected run.** Return-period (EVT) events
> were **not** part of the corrected `rerun_2026-07-03_corrected/` artifact, so
> the table below still carries the earlier **phase-2c geometry**: the
> short-term rows are **pre-de-leaking** (leaked intraday re-issues included,
> hence the larger n) and the long-term rows are the old flag-based
> "operational" (largely pre-2026 backfill = effectively hindcast). Read the
> whole section as **indicative pending a corrected RP re-run**, not as
> de-leaked skill. The below-norm and percentile sections above supersede it for
> any decision.

Return periods restate the high-flow question in the language flood and
hydropower operators use — not "above the 90th percentile" but "a 5-, 10-, 30-,
or 100-year event". Return levels are estimated **per station and per
period-of-year** by fitting a GEV (`scipy.stats.genextreme`) to each period's
annual realisations and taking the `1 − 1/T` quantile; an event is a period
whose runoff exceeds its own `T`-year return level. By construction the low
return periods approximate the percentile events (rp10 ≈ 90th percentile — in
the corrected percentile run above `high_p90` reads 0.75 / 0.73 at pentad, close
to the phase-2c rp10 0.81 / 0.80); the value of the EVT framing is the explicit
rarity scale and the (extrapolated) rarer levels.

Rows are **EM, POOLED, basin = all, season = all, canonical provenance**
(long-term = smallest lead), **phase-2c re-run (pre-correction — see status note
above)**. `pos_events` = observed return-level exceedances (TP + FN) — the
effective sample per cell.

| Horizon | RP | base rate | POD [95 % CI] | FAR | HSS | pos_events |
|---------|----|:---------:|:-------------:|:---:|:---:|:----------:|
| pentad | rp5 | 0.12 | 0.87 [0.86, 0.89] | 0.16 | **0.83** | 1 389 |
| pentad | rp10 | 0.07 | 0.81 [0.78, 0.83] | 0.19 | 0.80 | 726 |
| pentad | rp30 | 0.03 | 0.69 [0.64, 0.75] | 0.18 | 0.74 | 281 |
| pentad | rp100 | 0.003 | 0.51 [0.36, 0.67] | 0.66 | 0.41 | 37 |
| decade | rp5 | 0.11 | 0.82 [0.79, 0.85] | 0.20 | 0.79 | 545 |
| decade | rp10 | 0.06 | 0.72 [0.66, 0.76] | 0.19 | 0.75 | 292 |
| decade | rp30 | 0.02 | 0.57 [0.48, 0.66] | 0.29 | 0.63 | 105 |
| decade | rp100 | 0.002 | 0.11 [0.02, 0.44] | 0.95 | 0.07 | 9 |
| month | rp5 | 0.19 | 0.86 [0.84, 0.88] | 0.08 | **0.87** | 1 194 |
| month | rp10 | 0.09 | 0.84 [0.81, 0.86] | 0.12 | 0.84 | 594 |
| month | rp30 | 0.03 | 0.22 [0.17, 0.28] | 0.29 | 0.33 | 199 |
| month | rp100 | 0.005 | 0.12 [0.05, 0.27] | 0.71 | 0.16 | 34 |
| quarter | rp5 | 0.21 | 0.49 [0.44, 0.54] | 0.27 | 0.50 | 386 |
| quarter | rp10 | 0.11 | 0.27 [0.21, 0.33] | 0.34 | 0.34 | 206 |
| quarter | rp30 | 0.05 | 0.10 [0.06, 0.18] | 0.59 | 0.15 | 88 |
| quarter | rp100 | 0.005 | 0.10 [0.02, 0.40] | 0.89 | 0.10 | 10 |
| season | rp5 | 0.16 | 0.30 [0.24, 0.36] | 0.44 | 0.31 | 213 |
| season | rp10 | 0.09 | 0.17 [0.11, 0.25] | 0.50 | 0.22 | 113 |
| season | rp30 | 0.05 | 0.05 [0.02, 0.14] | 0.77 | 0.07 | 59 |
| season | rp100 | 0.01 | 0.07 [0.01, 0.30] | 0.83 | 0.09 | 15 |

(n_pairs per horizon, **pre-correction phase-2c counts**: pentad 11 238,
decade 5 032, month 6 315, quarter 1 866, season 1 323. A corrected re-run would
shrink the short-term counts sharply, as it did for the de-leaked below-norm and
percentile events above.)

### Key findings

- **Frequent return-period floods (5- and 10-year) are detected well at the
  operationally relevant short horizons.** Pentad and decade catch 72–87 % of
  rp5/rp10 exceedances (HSS 0.75–0.83); **month L0 is strongest** (rp5 HSS 0.87,
  rp10 0.84). This is directly useful for hydropower / reservoir-inflow
  anticipation at 5-day to 1-month lead.
- **Skill falls off steeply with rarity.** By rp30 the effective sample shrinks
  (105–281 events at pentad/decade) and HSS drops; at rp100 there are only
  9–37 events per horizon — POD swings wildly and FAR is high. **Treat rp100
  (and rp30 beyond pentad) as illustrative extrapolation, not verified skill.**
- **Longer aggregation horizons detect rare high-flow poorly.** Quarter and
  season HSS collapse from rp5 (0.50 / 0.31) to rp30 (0.15 / 0.07): a seasonal
  forecast cannot anticipate a specific rare high-flow period.
- **Consistency check passed:** rp10 ≈ `high_p90` at every horizon, confirming
  the per-period return levels are internally consistent with the empirical
  percentile events.

**Feasibility caveat (archive-limited):** return levels are fitted on ~26
annual values per period, so rp5/rp10 are interpolation (reliable), rp30 is a
stretch, and **rp100 is beyond reliable estimation** — it extrapolates far past
the data and its handful of events carry very wide CIs. The tool computes all
four for completeness; only rp5/rp10 (and rp30 at pentad/decade) carry enough
events for a credible skill statement.

---

## Probabilistic forecast verification (predictive distribution)

All sections above score the **point** forecast (a single value, yes/no
decision). But the models emit a full **predictive distribution** — a quantile
band `q05…q95` — which the deterministic scores ignore. This section scores the
*distribution* itself: is the forecast's stated uncertainty **trustworthy**
(calibration), how **tight** is it (sharpness), and does the whole distribution
beat climatology (CRPS)? This is what risk-based consumers (reservoir /
hydropower / flood operators) actually act on.

**Metrics** (see the Metric-definitions box for the deterministic ones):

- **CRPS** (Continuous Ranked Probability Score) — generalises MAE to a full
  distribution; lower is better. **CRPSS** = `1 − CRPS/CRPS_climatology` (>0 beats
  climatology). The climatology reference uses the **identical grid estimator**,
  so CRPSS is unbiased. CRPS integrates the pinball loss with an explicit
  **tail penalty**, so an over-confident narrow band that misses is *not*
  rewarded.
- **Coverage / reliability** — does the nominal *P*% interval actually contain
  the observation *P*% of the time? `coverage_90` (q05–q95), `coverage_80`
  (q10–q90, **long-term only** — the short-term grid has no q10/q90), with a
  Wilson CI; `reliability = |coverage − nominal|` (0 = perfectly calibrated).
- **Sharpness (norm-normalised)** — interval width relative to the norm; only
  meaningful *given* calibration.
- **Brier / Brier skill score** — scores the forecast *probability* of the
  below-norm event (from the band) rather than the binary flag.

Numbers are from the corrected run (`rerun_2026-07-03_corrected/`), **EM, POOLED,
canonical provenance, season = all**; short-term **operational** (de-leaked),
long-term **hindcast** at the smallest lead. Only models that emit a usable band
are scored; **GBT / SM_GBT\* long-term models carry no band and stay point-only**.

| Horizon | Grid | n | CRPSS | Coverage 90 % | Coverage 80 % | Reliability | Sharpness (norm) | Brier SS |
|---------|------|---:|:-----:|:-------------:|:-------------:|:-----------:|:----------------:|:--------:|
| pentad | short5 | 3 384 | **0.68** | 0.90 | — | 0.00 | 0.28 | 0.79 |
| decade | short5 | 1 894 | 0.65 | 0.90 | — | 0.00 | 0.31 | 0.77 |
| month L0 (hind) | long7 | 7 081 | **0.72** | 0.94 | 0.87 | 0.04 | 0.28 | **0.81** |
| quarter Q1 (hind) | long7 | 865 | 0.52 | 0.96 | 0.91 | 0.06 | 0.53 | 0.51 |
| season L0 (hind) | long7 | 883 | 0.27 | 0.85 | 0.77 | 0.05 | 0.59 | 0.38 |

*(Coverage 90 ideal = 0.90; Coverage 80 ideal = 0.80; Reliability = |coverage −
nominal|, lower is better; CRPSS / Brier SS > 0 beats climatology.)*

**Day horizon:** EM emits no ensemble band at day, so EM is absent here;
day-scale probabilistic scores exist only for TFT / TiDE / TSMixer (short5).

### Key findings

- **The forecasts are well calibrated.** EM's 90 % bands actually contain
  90–96 % of observations at pentad/decade/month/quarter (reliability 0.00–0.06)
  — the stated uncertainty is trustworthy, not over- or under-confident. Only
  the **season** horizon is mildly under-covered (0.85 vs 0.90, reliability
  0.05). This is the headline probabilistic result: operators can take the EM
  interval at face value at short-to-medium range.
- **The full distribution beats climatology at every horizon** (CRPSS
  0.25–0.72), strongest at month (0.72), pentad (0.68), decade (0.65), moderate
  at quarter Q1 (0.52); weakest at season (0.27) — the same short-good /
  long-weak gradient as the point scores.
- **Sharpness matches confidence to horizon.** Bands are tight relative to the
  norm at pentad/decade/month (~0.28–0.31) and appropriately wide at
  quarter/season (~0.53–0.59) — the models widen their intervals where skill is
  genuinely lower rather than staying falsely narrow.
- **Probabilistic below-norm skill (Brier SS)** is strong at pentad/decade/month
  (0.77–0.81) and drops at quarter Q1 (0.51) / season (0.38), consistent with
  the deterministic contingency results.

**Caveats specific to probabilistic scores:**

- **Cross-grid CRPS is not comparable.** Short-term (4-node `short5`, no q10/q90,
  q50 = point) and long-term (7-node `long7`) CRPS are computed over different
  node sets; raw `crps` is never ranked across grids (the dashboard restricts
  CRPSS/sharpness rankings to a single grid). CRPSS, being a dimensionless skill
  ratio, is compared only in spirit — the table tags the grid per row.
- **`coverage_80` is long-term-only** (the short-term grid lacks q10/q90).
- Only band-bearing models are scored; per-model / per-station probabilistic
  detail is explorable in the dashboard's **Probabilistic** view.

---

## Value metrics — magnitude accuracy & decision value

The contingency and probabilistic sections score *whether the decision was
right* and *whether the uncertainty is trustworthy*. This section scores the two
things left: **how accurate the magnitude is** (continuous/volume) and **how
much the decision is worth** (economic value). Numbers are from the corrected run
(`rerun_2026-07-03_corrected/`), **EM, canonical provenance**; short-term
**operational** (de-leaked), long-term **hindcast** at the smallest lead.

### Continuous / volume accuracy

- **KGE-2009** (Kling–Gupta) and **NSE** (Nash–Sutcliffe): 1 = perfect, and both
  are meaningful **per station** (they compare a series against its own
  variance).
- **Relative volume error (rve)** = `(ΣΣfc − Σobs)/Σobs`: systematic
  over/under-forecast of total water — the number that matters for allocation
  and reservoir/hydropower planning.

| Horizon | KGE (per-station median) † | NSE (per-station median) † | rel. volume error ‡ | n stations |
|---------|:------------------------:|:------------------------:|:-----------------:|:----------:|
| pentad | **0.96** | 0.97 | +1.1 % | 51 |
| decade | 0.95 | 0.95 | +0.9 % | 50 |
| month L0 | **0.97** | 0.98 | +0.5 % | 53 |
| quarter Q1 | 0.78 | 0.75 | +0.4 % | 53 |
| season L0 | **0.41** | 0.23 | −0.3 % | 51 |

† **Per-station KGE/NSE could not be recomputed in this POOLED-only refresh** —
the per-station distribution lives in the station-coded artifacts, which are out
of scope for the sanitized run. The per-station medians above are **carried from
the prior run as indicative**; the methodology (per-station fit) is unchanged by
the corrections, which affect regime labelling and de-leaking rather than the
shape of each station's series. ‡ **rve is refreshed from the corrected POOLED
run** (relative volume error is pooling-invariant).

**Key findings:**

- **EM tracks each station's flow very well at pentad/decade/month** (per-station
  median KGE 0.95–0.97, NSE 0.95–0.98, indicative) and is **essentially
  volume-unbiased at every horizon** (corrected rve within ±1.1 %). For
  water-accounting, EM neither systematically over- nor under-allocates.
- **Skill falls off at seasonal range** (per-station KGE 0.41, NSE 0.23) — the
  same short-good / long-weak gradient as every other metric family.
- **CRITICAL reporting caveat — do not use pooled KGE/NSE.** Aggregated across
  all stations, KGE/NSE are badly **inflated** because between-station variance
  (small creeks vs large rivers) dominates the denominator: the corrected pooled
  season NSE is **0.97** vs the honest per-station median of **0.23**. The table
  above uses the **per-station median**; the dashboard's Value view shows the
  full per-station distribution. (rve and the REV below are not affected — rve is
  relative and REV is derived from the contingency table.)

### Relative economic value (cost–loss)

The **potential economic value** `V(α)` (Richardson 2000 / Wilks) of the
below-norm decision, for a consumer whose cost-of-action / loss-of-inaction
ratio is `α`. Its analytic peak is `V_max = H − F` (hit rate − false-alarm rate,
the Peirce skill score) at `α = base rate`:

| Horizon | V_max (peak value) | at α* (base rate) |
|---------|:------------------:|:-----------------:|
| pentad | **0.84** | 0.41 |
| decade | 0.78 | 0.40 |
| month L0 (hind) | **0.85** | 0.23 |
| quarter Q1 (hind) | 0.50 | 0.08 |
| season L0 (hind) | 0.43 | 0.21 |

- **The below-norm forecast delivers high decision value at pentad/decade/month**
  (V_max 0.78–0.85 — a consumer captures ~80–85 % of the value a perfect forecast
  would, at their optimal cost-loss ratio), and **moderate value at
  quarter/season** (0.43–0.50; quarter shown at target quarter Q1). The full
  `V(α)` curve per model is in the dashboard;
  it is not clamped, so a skill-negative model/α shows `V < 0` (acting on it
  loses money vs climatology).

Together the value metrics confirm the operational message from the other
sections — **rely on pentad/decade/month EM for the irrigation decision; treat
quarter/season as risk-screening** — and add that EM is volume-unbiased, which
matters directly for allocation and hydropower.

---

## Caveats and limitations

1. **DAY horizon is thin and exploratory.** After de-leaking, n_pairs is only
   56–108 across models; CIs span ±15–17 pp. Use for direction only.
2. **Long-term coverage is Kyrgyz-dominated.** 65 of 83 stations are Kyrgyz;
   Tajik hindcast-month pairs at leads ≥ 2 are scarce. Tajik seasonal skill
   estimates carry wide CIs and may not generalise.
3. **Rolling-window product excluded** (549 868 rows): confirms the exclusion
   filter is working correctly. The remaining month metrics reflect only
   calendar-aligned monthly forecasts.
4. **Long-term operational is genuinely thin; the sections lead with hindcast.**
   Real-time long-term forecasting only began ~2026, so the genuine-operational
   archive is tiny (month L0 n = 40, quarter Q1 n = 40, season L0 n = 49) and
   cannot carry a reliable skill estimate. The robust long-term skill is the
   **hindcast** (full pre-2026 archive: month L0 n = 7 081, quarter Q1 n = 865,
   season L0 n = 883), which the long-term sections report as the headline. The
   genuine-2026 operational numbers are shown only as preliminary footnotes.
   fig3 contrasts the two; because they are not sample-matched, any cell where
   operational ≥ hindcast is a sample-composition artefact.
5. **Two thresholds reported side by side, not comparable.** The operational
   story is the **0.80 × norm** limit-plan event; the **1.0 × norm** plain
   below-average event is reported alongside it for reference only. The two have
   different base rates, so their POD/FAR/HSS must not be compared cell-for-cell
   (HSS is base-rate sensitive). The tool supports re-running with any threshold.
6. **Quarter Q2 artefact:** 82 pairs, POD = 1.00 — a zero-miss, small-n
   artefact. Treat that cell as unreliable.
7. **`min_years = 10`** filter applied: stations with fewer than 10 years of
   paired data were excluded from long-term norm computation.

---

## Figures

All figures are in `doc/plans/working/forecast_skill_eval_figures/`:

- **fig1_performance_diagram.png** — Roebber performance diagram (success ratio
  vs POD) for all (h_label, model) combinations, operational regime. Colour =
  base horizon; marker shape = model (stable global assignment; same shape
  always maps to the same model across all h_labels). Two legends: horizon
  colour (lower-left) and model shape (upper-left). Dimmer markers = farther
  long-term leads.

- **fig1_day.png / fig1_pentad.png / fig1_decade.png** — per-horizon Roebber
  diagrams for each short-term horizon separately. Single colour (the horizon
  colour from fig1). Same stable model→shape mapping. Legend: model shape
  (upper-left) and horizon colour patch (lower-left). CSI and frequency-bias
  grid as in the combined diagram.

- **fig1_month.png / fig1_quarter.png / fig1_season.png** — per-horizon Roebber
  diagrams for each long-term horizon. Markers are coloured by **lead** for
  month/season and by **target quarter** for quarter (plasma colormap; 4 distinct
  colours). Approved ranges: month L0–L3, quarter Q1–Q4, season L0–L3. Legend:
  model shape (upper-left) and lead/target-quarter colour (lower-left). For
  month/season these reveal how skill degrades with increasing lead; for quarter
  they contrast the four target quarters (Q1 strongest, Q4 hardest). **Preliminary:** the
  long-term per-horizon operational diagrams are drawn from the thin genuine-2026
  operational archive; read them as indicative and rely on the hindcast tables
  (and **fig3**) for the robust long-term estimate.

- **fig2_hss_heatmap.png** — HSS heatmap, model × h_label, operational regime.
  Columns: `day`, `pentad`, `decade`, `month L0`–`month L3`, `quarter Q1`–
  `quarter Q4`, `season L0`–`season L3`. Month leads L4–L12 are excluded
  (deprecated, sparse data). **Note:** the long-term (month/quarter/season)
  columns reflect the thin genuine-2026 operational archive and are preliminary;
  the hindcast tables in the long-term sections are the robust reference.

- **fig3_operational_vs_hindcast_hss.png** — **the key long-term figure**:
  operational vs hindcast HSS by lead (month / season) and target quarter
  (quarter) for the best-sampled model per horizon. Each bar is annotated with
  its n_pairs, making the
  thin-operational-vs-robust-hindcast contrast explicit. **CAVEAT:** Operational
  and hindcast are NOT sample-matched (different stations / dates / n; e.g. month
  EM operational L0 n ≈ 40 vs hindcast n ≈ 7 081). Where operational HSS ≥
  hindcast HSS it is a sample-composition artifact, not genuine skill. The
  genuine-2026 operational bars are preliminary; the hindcast bars are the robust
  estimate. Month limited to L0–L3 (L4–L12 deprecated).

- **fig4_day.png / fig4_pentad.png / fig4_decade.png** — per-model POD (green)
  and FAR (red) bar chart for each short-term horizon, all models, operational
  regime (basin = all, code = POOLED). Wilson 95 % CI whiskers on POD; integer
  FP count annotated above each FAR bar. x-tick labels colour-coded by model
  family: blue = LR/Statistical, orange = ML/GBT, **green = Ensemble
  (skill-weighted; EM / NE / Skilled Mean)**, **purple = Unweighted mean
  (Naive Mean — straight average of all outputs, not climatology)**. Each panel
  includes a labelled annotation box in the lower-right corner showing **both
  reference baselines**: climatology (POD = 0, HSS = 0, misses all events) and
  persistence (POD and HSS from phase-2 baselines.csv). This lets a reader see
  the full three-way ranking at a glance.

- **fig4_month.png / fig4_quarter.png / fig4_season.png** — same POD / FAR /
  FP layout as the short-term fig4 figures, but faceted by lead for month/season
  and by target quarter for quarter (month L0–L3, quarter Q1–Q4, season L0–L3).
  The canonical panel (L0 for month/season, Q1 for quarter) carries both
  climatology and persistence reference annotations; other panels carry
  climatology only. Family colour-coding
  (see above) enables direct ML-vs-LR-vs-ensemble comparison; Naive Mean
  appears in purple ("Unweighted mean") not in grey. **Preliminary:** these
  long-term per-lead operational diagrams are 2026-only (thin); the hindcast
  tables and **fig3** carry the robust long-term skill.

- **fig5_persistence_vs_models.png** — grouped bar chart: for each horizon
  (x-axis), three bars show HSS of climatology (light grey, HSS = 0),
  persistence (dark grey), and the best skilled model (coloured by horizon).
  Long-term horizons use their canonical panel (month/season L0, quarter Q1).
  This is the "three-way skill ladder": climatology ◀ persistence ◀ skilled
  models (for pentad and longer); at day scale persistence beats the thin ML
  models.

- **fig6_season_pod.png** — three-panel figure (pentad / decade / month L0),
  each showing POD with Wilson 95 % CI whiskers for three season cuts:
  non-irrigation (Oct–Mar, blue), all year (grey), and irrigation (Apr–Sep,
  orange). Model = EM; short-term operational, month L0 hindcast; POOLED.
  Confirms that detection is comparable in and out of the irrigation season
  (within CI overlap) — there is no meaningful in-season penalty when limit-plan
  decisions are most consequential.

### Figures — hindcast (full archive)

The figures above depict the **operational** regime — the genuine real-time
forecast archive, which is thin, especially for the long-term horizons (e.g.
month L0 operational n = 40). The `*_hindcast.png` figures below mirror them
one-for-one but are computed on the **full historical hindcast archive
(2000–2025)**, so their samples are far larger (e.g. month L0 hindcast n ≈ 7 081
vs operational n = 40). This is the robust view, and for the long-term horizons
(month / quarter / season) it is the one to trust; the operational figures above
reflect the still-thin genuine real-time regime. Short-term horizons are already
well-sampled operationally, so the hindcast panels there mainly confirm the
operational picture on the longer archive. All hindcast figures are in the same
directory (`doc/plans/working/forecast_skill_eval_figures/`, `_hindcast` suffix).
There is no `fig3` hindcast variant — **fig3** is itself the
operational-vs-hindcast contrast.

- **fig1_performance_diagram_hindcast.png** — Roebber performance diagram
  (success ratio vs POD) for all (h_label, model) combinations, **hindcast
  regime**. Same encoding as `fig1_performance_diagram.png` (colour = base
  horizon, marker shape = model), but every point rests on the full-archive
  sample.

- **fig1_day_hindcast.png / fig1_pentad_hindcast.png /
  fig1_decade_hindcast.png** — per-horizon Roebber diagrams for each short-term
  horizon, hindcast regime. Counterparts to the operational `fig1_{day,pentad,
  decade}.png`.

- **fig1_month_hindcast.png / fig1_quarter_hindcast.png /
  fig1_season_hindcast.png** — per-horizon Roebber diagrams for each long-term
  horizon, markers coloured by lead for month/season and by target quarter for
  quarter (month L0–L3, quarter Q1–Q4, season L0–L3), hindcast regime. **These
  are the robust long-term Roebber diagrams**: unlike
  their operational counterparts (drawn from the thin genuine-2026 archive), they
  are computed on the full hindcast archive and should be read as the reliable
  long-term view.

- **fig2_hss_heatmap_hindcast.png** — HSS heatmap, model × h_label, **hindcast
  regime**. Same columns as `fig2_hss_heatmap.png`; the long-term columns here
  are the robust full-archive estimate rather than the thin operational one.

- **fig4_day_hindcast.png / fig4_pentad_hindcast.png /
  fig4_decade_hindcast.png** — per-model POD (green) / FAR (red) bar charts for
  each short-term horizon, hindcast regime. Same layout, family colour-coding and
  climatology/persistence reference annotations as the operational `fig4_{day,
  pentad,decade}.png`, computed on the full archive.

- **fig4_month_hindcast.png / fig4_quarter_hindcast.png /
  fig4_season_hindcast.png** — per-model POD / FAR faceted by lead for
  month/season and by target quarter for quarter (month L0–L3, quarter Q1–Q4,
  season L0–L3), **hindcast regime**. **The robust long-term
  per-lead view**: these carry the full-archive samples the operational
  `fig4_{month,quarter,season}.png` lack, so they are the ones to rely on for
  long-term per-lead skill.

- **fig5_persistence_vs_models_hindcast.png** — three-way skill ladder
  (climatology ◀ persistence ◀ best skilled model, HSS per horizon), hindcast
  regime. Counterpart to `fig5_persistence_vs_models.png` with long-term bars
  drawn from the robust full-archive sample.

- **fig6_season_pod_hindcast.png** — three-panel POD-with-Wilson-CI figure
  (pentad / decade / month L0) across the three season cuts (non-irrigation /
  all / irrigation), hindcast regime. Counterpart to `fig6_season_pod.png`; the
  month L0 panel in particular is far better sampled here.

---

## Related documents

- Planner prompt / locked requirements: `forecast_skill_eval_planner_prompt.md`
- Run configuration: `apps/forecast_skill_eval/artifacts/rerun_2026-07-03_corrected/run_config.json`
- Corrected artifacts: `apps/forecast_skill_eval/artifacts/rerun_2026-07-03_corrected/`
  (canonical source; short-term de-leaked + date-based long-term regime split +
  LR issue→target repair; both thresholds `below_norm` 0.80 × norm and
  `below_norm_100` 1.0 × norm, season column, persistence baseline, percentile /
  probabilistic / value metrics)
- Lead-aware figure script: `doc/plans/working/forecast_skill_eval_figures/make_figures.py`
- Summary (auto-generated): `apps/forecast_skill_eval/artifacts/rerun_2026-07-03_corrected/summary.md`
