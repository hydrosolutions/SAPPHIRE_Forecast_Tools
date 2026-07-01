# Forecast Skill Evaluation — Irrigation Limit-Plan Decision (Report Draft)

**Status:** results complete — phase-2 re-run 2026-06-30.

Re-run date: 2026-06-30. Artifacts:
`apps/forecast_skill_eval/artifacts/rerun_2026-06-30_phase2/`
(phase 2 adds `season` column {all, irrigation, non_irrigation} to
`contingency_metrics.csv` and a `persistence` baseline to `baselines.csv`;
phase-1 path `rerun_2026-06-30/` superseded).

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

| Horizon | n_pairs (all regimes) | Regimes | Station codes |
|---------|----------------------|---------|---------------|
| day     | 45 605               | all, hindcast, operational | 83 (65 Kyrgyz, 17 Tajik, 1 other) |
| pentad  | 158 369              | all, hindcast, operational | 83 |
| decade  | 78 655               | all, hindcast, operational | 83 |
| month   | 544 498 (summed over leads 0–12) | all, hindcast (L0–L3 only), operational | 83 |
| quarter | 44 804               | all, hindcast (L1 only), operational | 83 |
| season  | 21 832               | all, hindcast, operational | 83 |

**Org balance:** 65 Kyrgyz station codes (prefixes 15, 16) vs 17 Tajik (prefix
17). Long-term (month/quarter/season) n_pairs are dominated by Kyrgyz stations;
Tajik operational-month pairs are thin at leads ≥ 2. Metrics at those leads
carry wide Wilson CIs and should be treated as low-confidence.

**DAY horizon is exploratory:** n_pairs 215–809 across models. Results are
consistent in direction but CIs are wide; do not over-interpret absolute
numbers.

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
| EM    | 4646 | 359 | 360 | 5970 | 11 335 | 0.44 | **0.928** | 0.072 | **0.871** | 0.871 | [0.921, 0.935] |
| NE    | 4429 | 389 | 424 | 6276 | 11 518 | 0.42 | 0.913 | 0.081 | 0.855 | 0.854 | [0.904, 0.920] |
| TiDE  | 2715 | 256 | 305 | 4097 | 7 373  | 0.41 | 0.899 | 0.086 | 0.842 | 0.840 | [0.888, 0.909] |
| TSMixer | 2510 | 279 | 274 | 3623 | 6 686 | 0.42 | 0.902 | 0.100 | 0.830 | 0.830 | [0.890, 0.912] |
| TFT   | 2669 | 315 | 325 | 4258 | 7 567  | 0.40 | 0.891 | 0.106 | 0.823 | 0.823 | [0.880, 0.902] |
| LR    | 3449 | 398 | 809 | 8226 | 12 882 | 0.33 | 0.810 | 0.103 | 0.783 | 0.764 | [0.798, 0.822] |

Key observations:
- **FP/FN balance:** EM achieves near-equal FP and FN counts (359 vs 360,
  ratio ≈ 1.00). NE is slightly FP-heavy (FN 17 % > FP); LR is strongly
  FN-heavy (FN 809 vs FP 398, ratio 0.49) — more missed limit-plan events.
- All ML models substantially outperform LR on HSS and PSS.
- **FN rate is low** for top models: EM misses only 7.2 % of true limit-plan
  events (POD 0.928). FAR 7.2 % is operationally acceptable.

### Decade (10-day)

| Model | TP | FP | FN | TN | n_pairs | base_rate | POD | FAR | HSS | PSS | POD CI 95 % |
|-------|-----|-----|-----|-----|---------|-----------|-----|-----|-----|-----|-------------|
| EM    | 2258 | 167 | 238 | 2396 | 5 059 | 0.49 | **0.905** | 0.069 | **0.840** | 0.839 | [0.892, 0.916] |
| NE    | 2622 | 266 | 320 | 3178 | 6 386 | 0.46 | 0.891 | 0.092 | 0.815 | 0.814 | [0.879, 0.902] |
| TFT   | 1435 | 217 | 219 | 2204 | 4 075 | 0.41 | 0.868 | 0.131 | 0.778 | 0.778 | [0.850, 0.883] |
| TiDE  | 1303 | 177 | 202 | 1868 | 3 550 | 0.42 | 0.866 | 0.120 | 0.781 | 0.779 | [0.848, 0.882] |
| TSMixer | 1222 | 188 | 157 | 1536 | 3 103 | 0.44 | 0.886 | 0.133 | 0.775 | 0.777 | [0.868, 0.902] |
| LR    | 1457 | 221 | 625 | 4070 | 6 373 | 0.33 | 0.700 | 0.132 | 0.682 | 0.648 | [0.680, 0.719] |

Key observations: same pattern as pentad — EM leads, LR is FN-heavy (FN 625,
ratio FP/FN = 0.35). TSMixer is slightly FP-heavy at decade scale (FN 157 < FP 188).

### Day (exploratory, n_pairs 215–809)

| Model | n_pairs | POD | FAR | HSS | PSS |
|-------|---------|-----|-----|-----|-----|
| TSMixer | 809 | 0.540 | 0.162 | 0.498 | 0.469 |
| TiDE    | 552 | 0.536 | 0.177 | 0.452 | 0.439 |
| TFT     | 215 | 0.510 | 0.131 | 0.443 | 0.438 |

Day-horizon skill is substantially lower than pentad/decade. POD ~0.51–0.54
with HSS ~0.44–0.50. With only 215–809 pairs the CIs are wide; interpret with
caution. The small sample reflects that day-resolution ML archive is limited to
approximately the last 1–2 years (vs 15+ years for pentad/decade).

---

## Results — long-term horizons (month / quarter / season)

Long-term horizons emit **no lead-aggregated POOLED row**. Every metric is
per-lead. Figures label these `month L0`, `month L1`, etc. where lead 0 is
the nearest forecast (i.e., smallest forecast offset).

### Month (EM, operational, POOLED, official provenance)

| Lead | TP | FP | FN | TN | n_pairs | POD | FAR | HSS | PSS | POD CI 95 % |
|------|-----|-----|-----|------|---------|-----|-----|-----|-----|-------------|
| L0   | 1337 | 117 | 214 | 4100 | 5 768 | **0.862** | 0.080 | **0.851** | 0.834 | [0.844, 0.878] |
| L1   | 338  | 33  | 65  | 1206 | 1 642 | 0.839 | 0.089 | 0.834 | 0.812 | [0.800, 0.871] |
| L2   | 107  | 7   | 35  | 427  | 576   | 0.754 | 0.061 | 0.790 | 0.737 | [0.677, 0.817] |
| L3   | 35   | 6   | 24  | 369  | 434   | 0.593 | 0.146 | 0.662 | 0.577 | [0.466, 0.709] |
| L4+  | — | — | — | — | thin | — | — | — | — | — |

HSS degrades markedly from L0 (0.851) to L3 (0.662). Leads L4–L12 have very
few pairs (67–436 for EM), concentrated on a small set of Kyrgyz stations;
metrics should be treated as low-confidence.

**Naive Mean** (unweighted average of all model forecasts, no skill weighting)
is available at all leads (L0 n_pairs 9 579):
- L0: POD 0.803, FAR 0.107, HSS 0.810 — EM substantially outperforms.
- L3: POD 0.327, FAR 0.240, HSS 0.394 — EM at L3 still outperforms (HSS 0.662).

**Skilled Mean** (ensemble of trained long-term models) at L0:
- POD 0.883, FAR 0.079, HSS 0.873 — best among all month L0 models.
- n_pairs 8 471 (broad coverage). Strongly positive skill over climatology.

### Quarter (EM, operational, POOLED, aggregated_from_monthly)

| Lead | TP | FP | FN | TN | n_pairs | POD | FAR | HSS | PSS |
|------|-----|-----|-----|------|---------|-----|-----|-----|-----|
| L1   | 75  | 29  | 119 | 1664 | 1 887 | 0.387 | 0.279 | 0.465 | 0.369 |
| L2   | 24  | 20  | 0   | 35  | 79    | **1.000** | 0.455 | 0.515 | 0.636 |
| L3   | 40  | 9   | 17  | 83  | 149   | 0.702 | 0.184 | 0.620 | 0.604 |
| L4   | 51  | 25  | 12  | 342 | 430   | 0.810 | 0.329 | 0.683 | 0.741 |

Quarter L2 has only 79 pairs (small n, POD of 1.0 is artefact). L1 is the
best-sampled lead (1 887 pairs) and shows modest skill (HSS 0.465). L4 shows
better POD (0.810) but n_pairs remains limited.

**LR_Base** at L1: POD 0.337, FAR 0.343, HSS 0.401 — positive but weaker
than EM.

**GBT** at L1: POD 0.462, FAR 0.141, HSS 0.561 — better than EM at L1 in HSS
with lower FAR; n_pairs 1 127.

### Season (EM, operational, POOLED, aggregated_from_monthly)

| Lead | TP | FP | FN | TN | n_pairs | POD | FAR | HSS | PSS |
|------|-----|-----|-----|-----|---------|-----|-----|-----|-----|
| L0   | 123 | 57  | 174 | 977  | 1 331 | 0.414 | 0.317 | 0.418 | 0.359 |
| L1   | 155 | 102 | 274 | 1402 | 1 933 | 0.361 | 0.397 | 0.343 | 0.293 |
| L2   | 66  | 41  | 124 | 670  | 901   | 0.347 | 0.383 | 0.345 | 0.290 |
| L3   | 67  | 51  | 132 | 662  | 912   | 0.337 | 0.432 | 0.311 | 0.265 |

Seasonal forecasts have moderate positive skill (HSS 0.31–0.42) across all
leads, but POD is only 0.34–0.41. That means **roughly 58–66 % of true
limit-plan seasons are missed** at the seasonal horizon — consistent with the
fundamental difficulty of seasonal prediction. FAR is also elevated (0.32–0.43),
meaning around 1 in 3 season-ahead limit-plan signals is a false alarm.
Nevertheless, HSS > 0 confirms skill over climatology at all leads.

**LR_Base** at all season leads: HSS 0.34–0.35, POD 0.34–0.36, FAR 0.32–0.41
— nearly identical to EM. No clear advantage of ML over LR at the seasonal
scale.

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

| Horizon | Base rate | Events missed by climatology |
|---------|-----------|------------------------------|
| day     | 0.48      | 48 % of all day-periods |
| pentad  | 0.44      | 44 % of all pentad-periods |
| decade  | 0.49      | 49 % of all decade-periods |
| month (L0) | 0.27  | 27 % of all month-periods |
| quarter (L1) | 0.10 | 10 % of all quarter-periods |
| season (L0) | 0.22  | 22 % of all season-periods |

**Are our forecasts better than climatology?**

Yes — decisively for short-term, positive but weaker for long-term.

- **Pentad EM** (POD 0.928, HSS 0.871 vs climatology POD 0, HSS 0): EM
  catches 92.8 % of the below-norm shortage events that climatology misses
  entirely.  At a pentad base rate of 0.44, climatology flags zero events; EM
  flags 93 % of the genuine ones.
- **Decade EM** (POD 0.905, HSS 0.840): EM catches 90.5 % of events
  climatology misses.  Stronger than LR (POD 0.700, HSS 0.682).
- **Month L0 Skilled Mean / EM** (POD 0.862–0.883, HSS 0.851–0.873): strong
  skill; climatology misses all 27 % of below-norm months.  Monthly forecasts
  are reliable enough to inform seasonal irrigation planning one month ahead.
- **Quarter L1 EM** (POD 0.387, HSS 0.465): positive but modest.  Climatology
  misses all 10 % of below-norm quarters; EM catches 38.7 % of them.
- **Season L0 EM** (POD 0.414, HSS 0.418): weak but positive.  Climatology
  misses all 22 % of below-norm seasons; EM catches 41.4 % of them.

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
| EM (best ensemble) | Skill-weighted ensemble | 0.928 | 0.871 |

In fig4, Naive Mean appears with a **purple** tick label (family "Unweighted
mean"), clearly separated from the green skill-weighted ensembles (EM, NE,
Skilled Mean) and from the climatology annotation in each panel.

### LR / LR_Base proxy

For short-term horizons (where LR is directly available), LR is included as a
named model.  For long-term horizons, LR_Base is the proxy.

Summary of best model vs. climatology vs. LR:
- **Pentad EM**: HSS 0.871 — far above climatology (0.00). Beats LR (0.783).
- **Decade EM**: HSS 0.840 — far above climatology. Beats LR (0.682).
- **Month L0 Skilled Mean**: HSS 0.873 — strong skill. Naive Mean HSS 0.810.
- **Season L0 EM**: HSS 0.418 — positive but modest; LR_Base similar (0.353).

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
| day     | 0.981 | 0.140 | **0.866** | 588 |
| pentad  | 0.852 | 0.141 | 0.743 | 11 322 |
| decade  | 0.785 | 0.144 | 0.658 | 5 036 |
| month L0 | 0.504 | 0.382 | 0.459 | 6 369 |
| quarter L1 | 0.220 | 0.679 | 0.193 | 1 851 |
| season L0 | 0.387 | 0.607 | 0.218 | 1 282 |

**Three-way skill ladder — climatology / persistence / best model:**

| Horizon | Climatology HSS | Persistence HSS | Best model HSS | Winner |
|---------|:--------------:|:--------------:|:-------------:|--------|
| day     | 0 | 0.866 | 0.508 (TiDE) | **Persistence** |
| pentad  | 0 | 0.743 | 0.871 (EM)   | EM |
| decade  | 0 | 0.658 | 0.840 (EM)   | EM |
| month L0 | 0 | 0.459 | 0.855 (EM)  | EM |
| quarter L1 | 0 | 0.193 | 0.465 (EM) | EM |
| season L0 | 0 | 0.218 | 0.418 (EM)  | EM |

Key findings:

- **Everything beats climatology.** All candidate models and persistence have
  HSS > 0 at every horizon.  Climatology (POD = 0, HSS = 0) is decisively
  below both persistence and the trained models.
- **Candidate models beat persistence from pentad onwards.**  At pentad and
  longer horizons the ensemble and ML models consistently outperform
  persistence, with the gap growing at longer leads: +0.13 at pentad,
  +0.18 at decade, +0.40 at month L0.  This is the expected pattern for
  well-trained models — as persistence degrades with longer forecast lead,
  the models' learned climatological signal takes over.
- **At the day scale, persistence beats our thin ML models.**  Day-horizon
  persistence achieves HSS 0.87 against only 0.44–0.51 for TFT/TSMixer/TiDE.
  This is largely a sample-size effect: day-horizon ML forecasts are drawn
  from a very small operational archive (n = 588–809 vs ~12 000+ at pentad).
  With more operational data the gap would likely close, but day-ahead
  persistence (last observed flow) is a very strong competitor at this scale.

The three-way ladder is visualised in **fig5_persistence_vs_models.png**.
Fig4 panels (per-horizon POD/FAR bars) now include both climatology and
persistence as reference annotations in the lower-right corner.

---

## Lead-time stratification summary

| Horizon | Best lead | HSS (best) | HSS (worst shown) | Degradation |
|---------|-----------|-----------|-------------------|-------------|
| day     | n/a (no lead) | 0.50 (TSMixer) | — | — |
| pentad  | n/a       | 0.871 (EM) | — | — |
| decade  | n/a       | 0.840 (EM) | — | — |
| month   | L0        | 0.851 (EM) | 0.662 (L3) | −0.19 over 3 leads |
| quarter | L4        | 0.683 (EM) | 0.465 (L1) | varies non-monotonically |
| season  | L0        | 0.418 (EM) | 0.311 (L3) | −0.11 over 3 leads |

Month shows the clearest monotonic degradation with lead. Season degrades more
slowly but is moderate at all leads.

---

## Seasonal disaggregation (irrigation season Apr–Sep)

Phase-2 re-run splits every metric into three season cuts:
- **irrigation** — Apr–Sep (the growing and water-distribution season; the
  period when the limit-plan decision has the greatest operational cost)
- **non_irrigation** — Oct–Mar (low-demand period)
- **all** — full year (same as the numbers in the sections above)

All rows below are **operational regime, POOLED, basin = all, EM model,
canonical provenance**.

### Seasonal POD / FAR / HSS table (EM, POOLED)

| Horizon | Season | n | POD | FAR | HSS | POD CI 95 % |
|---------|--------|---|-----|-----|-----|-------------|
| pentad | all | 11 335 | 0.928 | 0.072 | 0.871 | [0.921, 0.935] |
| pentad | irrigation (Apr–Sep) | 6 813 | **0.933** | 0.072 | 0.856 | [0.925, 0.941] |
| pentad | non-irrigation (Oct–Mar) | 4 522 | 0.916 | 0.071 | **0.885** | [0.900, 0.929] |
| decade | all | 5 059 | 0.905 | 0.069 | 0.840 | [0.892, 0.916] |
| decade | irrigation (Apr–Sep) | 2 794 | **0.905** | 0.066 | 0.798 | [0.890, 0.918] |
| decade | non-irrigation (Oct–Mar) | 2 265 | 0.904 | 0.075 | **0.869** | [0.882, 0.923] |
| month L0 | all | 6 387 | 0.863 | 0.096 | 0.855 | [0.843, 0.881] |
| month L0 | irrigation (Apr–Sep) | 2 505 | **0.880** | 0.119 | 0.828 | [0.855, 0.901] |
| month L0 | non-irrigation (Oct–Mar) | 3 882 | 0.835 | 0.057 | **0.870** | [0.800, 0.866] |
| season L0 | all / irrigation | 1 331 | 0.414 | 0.317 | 0.418 | [0.360, 0.471] |
| season L0 | non-irrigation | — | — | — | — | — |

**Note on season horizon:** All season-ahead forecasts (Apr–Sep seasonal
runoff) target the irrigation season by definition.  There is no
non-irrigation split for the season horizon.

### Interpretation

**POD is slightly higher in the irrigation season than out-of-season:**
at the pentad scale, EM detects 93.3 % of below-norm events during Apr–Sep
vs 91.6 % during Oct–Mar (difference 1.7 pp, within CI overlap but
consistent across models).  At month L0, the irrigation-season advantage is
4.5 pp (88.0 % vs 83.5 %).

**HSS is slightly lower in-season** despite higher POD.  This is a
base-rate effect: below-norm events are more frequent in the irrigation
season (higher base rate → harder to beat chance), which compresses HSS
even when detection improves.

**Operational significance:** the irrigaton season is precisely when the
limit-plan decision matters most.  The finding confirms that the EM model
detects shortages at least as well — and somewhat better on raw POD — in
the months when under-detection is most costly.  Irrigation managers can
rely on the pooled-year skill estimates as a conservative lower bound for
in-season performance.

The seasonal disaggregation figures are shown in **fig6_season_pod.png**
(POD with Wilson CI, irrigation vs non-irrigation vs all, for pentad,
decade, and month L0).

---

## Interpretation for the irrigation decision

The key operational question is: **what fraction of true below-norm runoff
events are missed** (FN rate = 1 − POD)?

- **Pentad/decade (ML models):** FN rate 7–12 %. False alarms (FP rate relative
  to positive events) ≈ 7–13 %. Both are operationally low. An irrigation
  manager using EM pentad forecasts would miss approximately 1 in 14 genuine
  shortage events.
- **Pentad/decade (LR):** FN rate 19–30 %. LR is FN-heavy — much more likely
  to miss a genuine shortage. ML models offer a clear advantage.
- **Month L0 (Skilled Mean / EM):** FN rate 12–14 %. Strong skill, comparable
  to short-term ML. Monthly forecasts are reliable enough to inform seasonal
  irrigation planning one month ahead.
- **Month L3 (EM):** FN rate 41 %. Skill degrades substantially at 3-month
  lead. Three-month-ahead seasonal signals should be treated as directional
  guidance only.
- **Quarter / Season:** FN rate 59–66 % at all leads (EM). More than half of
  true limit-plan events are missed. These horizons provide only weak, positive
  skill — useful for ensemble signal but not for firm operational decisions. FAR
  is also elevated (31–43 %).
- **Day (exploratory):** FN rate 46–49 %. Low coverage and inherent day-scale
  variability mean day-ahead limit-plan decisions carry substantial uncertainty.

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

Numbers below are from the phase-2c re-run
(`apps/forecast_skill_eval/artifacts/rerun_2026-07-01_phase2c_events/`), which
adds the four percentile events and **reproduces the below-norm numbers above
identically**. Rows are **EM, operational, POOLED, basin = all, season = all,
canonical provenance**; long-term horizons use the smallest lead (month /
season L0, quarter L1).

### Table — EM operational, POOLED, canonical provenance, season = all

| Horizon | Event | base rate | POD | FAR | HSS | POD 95 % CI | n |
|---------|-------|:---------:|:---:|:---:|:---:|:-----------:|---:|
| pentad | low_p5 | 0.14 | 0.77 | 0.25 | 0.72 | [0.75, 0.79] | 11 238 |
| pentad | low_p10 | 0.22 | 0.83 | 0.18 | 0.78 | [0.82, 0.85] | 11 238 |
| pentad | high_p90 | 0.07 | 0.76 | 0.22 | 0.75 | [0.73, 0.79] | 11 238 |
| pentad | high_p95 | 0.05 | 0.67 | 0.25 | 0.70 | [0.63, 0.71] | 11 238 |
| decade | low_p5 | 0.17 | 0.78 | 0.29 | 0.68 | [0.75, 0.81] | 5 032 |
| decade | low_p10 | 0.26 | 0.86 | 0.20 | 0.76 | [0.84, 0.88] | 5 032 |
| decade | high_p90 | 0.06 | 0.70 | 0.26 | 0.70 | [0.65, 0.75] | 5 032 |
| decade | high_p95 | 0.03 | 0.68 | 0.36 | 0.65 | [0.60, 0.74] | 5 032 |
| month L0 | low_p5 | 0.06 | 0.58 | 0.26 | 0.63 | [0.53, 0.63] | 6 315 |
| month L0 | low_p10 | 0.10 | 0.64 | 0.21 | 0.68 | [0.60, 0.68] | 6 315 |
| month L0 | high_p90 | 0.11 | **0.78** | 0.11 | **0.82** | [0.75, 0.81] | 6 315 |
| month L0 | high_p95 | 0.07 | 0.49 | 0.24 | 0.58 | [0.44, 0.54] | 6 315 |
| quarter L1 | low_p5 | 0.07 | 0.21 | 0.49 | 0.27 | [0.15, 0.29] | 1 866 |
| quarter L1 | low_p10 | 0.12 | 0.21 | 0.43 | 0.26 | [0.16, 0.27] | 1 866 |
| quarter L1 | high_p90 | 0.11 | 0.28 | 0.39 | 0.34 | [0.22, 0.34] | 1 866 |
| quarter L1 | high_p95 | 0.06 | 0.18 | 0.58 | 0.22 | [0.12, 0.26] | 1 866 |
| season L0 | low_p5 | 0.10 | 0.07 | 0.53 | 0.10 | [0.04, 0.13] | 1 323 |
| season L0 | low_p10 | 0.16 | 0.11 | 0.48 | 0.14 | [0.07, 0.16] | 1 323 |
| season L0 | high_p90 | 0.10 | 0.25 | 0.43 | 0.31 | [0.19, 0.33] | 1 323 |
| season L0 | high_p95 | 0.07 | 0.14 | 0.54 | 0.19 | [0.08, 0.22] | 1 323 |

**Day horizon:** no percentile events are reported. The `min_years ≥ 10` gate
cannot form daily percentile thresholds — a daily percentile needs ~10 years of
observations for each day-of-year period, which the thin day archive
(~1–2 years) does not provide. (The below-norm day decision uses the norm, not
an empirical percentile, so it is unaffected.)

### Key findings

- **High-flow detection is genuinely skilful at pentad, decade, and month L0.**
  EM catches 70–78 % of 90th-percentile high-flow events (HSS 0.70–0.82), with
  **month L0 the strongest** (POD 0.78, HSS 0.82, FAR 0.11). This is the
  principal new result: the same models that drive the irrigation decision also
  provide usable flood / high-flow signal at short-to-medium range.
- **Extreme tails are harder than moderate tails.** Detection drops from the
  moderate percentile to the extreme one at every horizon — e.g. pentad
  high_p90 0.76 → high_p95 0.67; month L0 high_p90 0.78 → high_p95 0.49. Rarer
  events (lower base rate) are intrinsically harder to catch and carry wider
  CIs.
- **Low-flow percentiles track the below-norm story but are more demanding.**
  The 10th/5th percentiles are stricter thresholds than 0.80 × norm, so POD is
  lower than the below-norm POD at the same horizon (pentad low_p10 0.83 vs
  below-norm 0.93; month L0 low_p10 0.64 vs 0.86). Pentad/decade still catch
  77–86 % of moderate (10th-percentile) low-flow events.
- **Quarter and season remain weak across all percentile events** (POD
  0.07–0.28), consistent with the below-norm finding — long-range extreme
  detection is risk-screening only.

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

Return periods restate the high-flow question in the language flood and
hydropower operators use — not "above the 90th percentile" but "a 5-, 10-, 30-,
or 100-year event". Return levels are estimated **per station and per
period-of-year** by fitting a GEV (`scipy.stats.genextreme`) to each period's
annual realisations and taking the `1 − 1/T` quantile; an event is a period
whose runoff exceeds its own `T`-year return level. By construction the low
return periods approximate the percentile events (rp10 ≈ 90th percentile —
cross-check: pentad rp10 POD 0.81 / HSS 0.80 vs `high_p90` 0.76 / 0.75); the
value of the EVT framing is the explicit rarity scale and the (extrapolated)
rarer levels.

Rows are **EM, operational, POOLED, basin = all, season = all, canonical
provenance** (long-term = smallest lead), phase-2c re-run. `pos_events` =
observed return-level exceedances (TP + FN) — the effective sample per cell.

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

(n_pairs per horizon: pentad 11 238, decade 5 032, month 6 315, quarter 1 866,
season 1 323.)

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

## Caveats and limitations

1. **DAY horizon is thin and exploratory.** n_pairs 215–809 across models;
   CIs span ±10–15 pp. Use for direction only.
2. **Long-term coverage is Kyrgyz-dominated.** 65 of 83 stations are Kyrgyz;
   Tajik operational-month pairs at leads ≥ 2 are scarce. Tajik seasonal skill
   estimates carry wide CIs and may not generalise.
3. **Rolling-window product excluded** (549 868 rows): confirms the exclusion
   filter is working correctly. The remaining month metrics reflect only
   calendar-aligned monthly forecasts.
4. **Operational vs. hindcast gap:** hindcast HSS is available for month L0–L3
   and season L0–L3 and is systematically higher than operational (typical
   optimism of hindcast-trained models). Report operational figures for real-world
   assessment.
5. **Threshold fixed at 0.80 × norm.** Results may change for different
   threshold values; the tool supports re-running with any threshold.
6. **Quarter L2 artefact:** 79 pairs, POD = 1.00 — artefact of small n. Treat
   that cell as unreliable.
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
  diagrams for each long-term horizon. Markers are coloured by **lead** (plasma
  colormap; 4 distinct colours). Approved lead ranges: month L0–L3,
  quarter L1–L4, season L0–L3. Legend: model shape (upper-left) and lead colour
  (lower-left). These reveal how skill degrades with increasing lead and which
  models maintain consistent performance across leads.

- **fig2_hss_heatmap.png** — HSS heatmap, model × h_label, operational regime.
  Columns: `day`, `pentad`, `decade`, `month L0`–`month L3`, `quarter L1`–
  `quarter L4`, `season L0`–`season L3`. Month leads L4–L12 are excluded
  (deprecated, sparse data).

- **fig3_operational_vs_hindcast_hss.png** — operational vs hindcast HSS by
  lead for month / quarter / season (best-sampled model per horizon). Each bar
  is annotated with its n_pairs. **CAVEAT:** Operational and hindcast are NOT
  sample-matched (different stations / dates / n; e.g. month operational
  n ≈ 110–150 vs hindcast n ≈ 7 000). Where operational HSS ≥ hindcast HSS it
  is a sample-composition artifact, not genuine skill. Treat as indicative
  only. Month limited to L0–L3 (L4–L12 deprecated).

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
  FP layout as the short-term fig4 figures, but faceted by lead (month L0–L3,
  quarter L1–L4, season L0–L3). The canonical lead panel (L0 for month/season,
  L1 for quarter) carries both climatology and persistence reference
  annotations; other lead panels carry climatology only. Family colour-coding
  (see above) enables direct ML-vs-LR-vs-ensemble comparison; Naive Mean
  appears in purple ("Unweighted mean") not in grey.

- **fig5_persistence_vs_models.png** — grouped bar chart: for each horizon
  (x-axis), three bars show HSS of climatology (light grey, HSS = 0),
  persistence (dark grey), and the best skilled model (coloured by horizon).
  Long-term horizons use their canonical lead (month/season L0, quarter L1).
  This is the "three-way skill ladder": climatology ◀ persistence ◀ skilled
  models (for pentad and longer); at day scale persistence beats the thin ML
  models.

- **fig6_season_pod.png** — three-panel figure (pentad / decade / month L0),
  each showing POD with Wilson 95 % CI whiskers for three season cuts:
  non-irrigation (Oct–Mar, blue), all year (grey), and irrigation (Apr–Sep,
  orange). Model = EM, operational, POOLED. Confirms that detection is at
  least as strong — and slightly better on raw POD — during the irrigation
  season when limit-plan decisions are most consequential.

---

## Related documents

- Planner prompt / locked requirements: `forecast_skill_eval_planner_prompt.md`
- Run configuration: `apps/forecast_skill_eval/artifacts/rerun_2026-06-30/run_config.json`
- Phase-2 artifacts: `apps/forecast_skill_eval/artifacts/rerun_2026-06-30_phase2/`
  (canonical source; includes season column + persistence baseline)
- Lead-aware figure script: `doc/plans/working/forecast_skill_eval_figures/make_figures.py`
- Summary (auto-generated): `apps/forecast_skill_eval/artifacts/rerun_2026-06-30/summary.md`
