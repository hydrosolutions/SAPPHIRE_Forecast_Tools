# GitHub Issue: FD-002

**Title**: `feat(forecast_dashboard): Add new skill metrics visualization with plain-language interpretation`

**Labels**: `enhancement`, `forecast_dashboard`, `skill-metrics`, `medium-priority`

**Status**: Draft

**Depends on**: Phase 4c (Tier 1 metrics) and Phase 4d (Tier 2 metrics) in `postprocessing_unified_plan.md`

---

## Summary

Add visualization of new skill metrics (PBIAS, KGElf, NSE_log, FHV, FLV, F1/CSI for return periods, low-flow contingency) to the forecast dashboard. Each metric must include a **plain-language interpretation** so that operational hydrologists — many of whom currently work with only 2 metrics (accuracy and s/sigma) — can understand what the numbers mean without consulting documentation.

## Motivation

The SAPPHIRE system is adding new skill metrics to its yearly recalculation pipeline (see `postprocessing_unified_plan.md` Phases 4c/4d). These metrics will be available via the postprocessing API. However, API-only access risks the metrics going unused — especially at hydromet services with limited capacity. A dashboard visualization with clear interpretation ensures the metrics are actually examined and inform model selection decisions.

**User quote (operational hydrologist review):** "Even with API-only access, someone needs to interpret these metrics. Adding 8 new metrics without interpretation guidance means 8 new numbers that get ignored."

## Design Requirement: Plain-Language Interpretation

Every metric displayed on the dashboard must include a human-readable interpretation sentence. This is **not optional** — it is the primary feature that makes these metrics useful to operational staff.

### Interpretation Templates

#### Tier 1 Metrics (all temporal scales)

| Metric | Example Value | Interpretation Template |
|--------|---------------|------------------------|
| PBIAS | -12% | "The model **overestimates** total runoff by **12%**" |
| PBIAS | +8% | "The model **underestimates** total runoff by **8%**" |
| PBIAS | +2% | "The model has **negligible volume bias** (2%)" |
| KGElf | 0.82 | "**Good** overall performance with **good** low-flow reliability" |
| KGElf | 0.45 | "**Fair** overall performance with **poor** low-flow reliability" |
| KGElf | -0.2 | "**Poor** performance — worse than using the long-term mean" |
| NSE_log | 0.78 | "**Good** performance during low-flow periods" |
| NSE_log | 0.35 | "**Poor** performance during low-flow periods" |

#### Tier 2 Metrics (daily/sub-daily, yearly recalculation)

| Metric | Example Value | Interpretation Template |
|--------|---------------|------------------------|
| FHV | -18% | "The model **underestimates** peak flows (top 2%) by **18%**" |
| FHV | +5% | "The model has **slight overestimation** of peak flows (+5%)" |
| FLV | +25% | "The model **overestimates** low flows (bottom 30%) by **25%**" |
| FLV | -10% | "The model **underestimates** low flows by **10%**" |
| F1 (2yr) | 0.75 | "**Good** detection of 2-year flood events" |
| F1 (2yr) | 0.40 | "**Poor** flood detection — missed many events or gave many false alarms" |
| F1 (5yr) | 0.30 | "**Poor** detection of 5-year floods *(note: rare events, limited data)*" |
| Low-flow CSI | 0.60 | "**Fair** detection of drought conditions (below Q90)" |

### Quality Categories

Use consistent thresholds for the quality labels:

| Category | KGElf / NSE_log | |PBIAS| | |FHV| / |FLV| | F1 / CSI |
|----------|-----------------|--------|----------------|----------|
| **Very good** | > 0.75 | < 10% | < 10% | > 0.70 |
| **Good** | 0.50–0.75 | 10–15% | 10–20% | 0.50–0.70 |
| **Fair** | 0.00–0.50 | 15–25% | 20–40% | 0.30–0.50 |
| **Poor** | < 0.00 | > 25% | > 40% | < 0.30 |

These thresholds are based on Moriasi et al. (2007, 2015) for bias metrics and standard forecast verification practice for categorical metrics. They should be configurable via environment variables or config.yaml.

## Proposed Dashboard Layout

### Option A: Dedicated "Model Performance Report" Tab

A new tab on the forecast dashboard, updated yearly after `recalculate_skill_metrics.py` runs:

```
┌─────────────────────────────────────────────────────────┐
│  Model Performance Report — Station 15102               │
│  Last updated: 2026-01-15 (yearly recalculation)        │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  Overall Performance                                    │
│  ┌──────────┬──────────┬──────────┬──────────┐         │
│  │ NSE      │ Accuracy │ PBIAS    │ KGElf    │         │
│  │ 0.85     │ 82%      │ -5%      │ 0.71     │         │
│  │ V.Good   │ Good     │ V.Good   │ Good     │         │
│  └──────────┴──────────┴──────────┴──────────┘         │
│  "Model overestimates total runoff by 5%.               │
│   Good overall performance with good low-flow           │
│   reliability."                                         │
│                                                         │
│  Flood Detection (daily forecasts)                      │
│  ┌─────────────────────────────────────────┐           │
│  │ Return Period │ F1    │ Hits │ Misses   │           │
│  │ 2-year        │ 0.72  │ 8/11 │ 3        │           │
│  │ 5-year        │ 0.50  │ 2/4  │ 2        │           │
│  │ "Good 2-year flood detection.            │           │
│  │  Fair 5-year detection (limited data)."  │           │
│  └─────────────────────────────────────────┘           │
│                                                         │
│  Low-Flow Performance                                   │
│  ┌─────────────────────────────────────────┐           │
│  │ NSE_log │ KGElf │ FLV     │ Low-Q CSI  │           │
│  │ 0.68    │ 0.71  │ -12%    │ 0.55       │           │
│  │ Good    │ Good  │ Good    │ Fair        │           │
│  │ "Good low-flow prediction. Model        │           │
│  │  underestimates low flows by 12%."      │           │
│  └─────────────────────────────────────────┘           │
│                                                         │
│  Peak Flow Performance                                  │
│  ┌──────────────────────────────┐                      │
│  │ FHV: -18%  (Fair)            │                      │
│  │ "Model underestimates peak   │                      │
│  │  flows by 18%."              │                      │
│  └──────────────────────────────┘                      │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Option B: Summary Badges on Main Dashboard

Add compact badges to the existing station view:

```
Station 15102 — LR Model
[NSE: 0.85 ✓] [Bias: -5% ✓] [Low-flow: Good] [Flood det.: Good]
```

Clicking a badge opens the full report (Option A).

### Recommendation

**Start with Option A** (dedicated tab). Option B requires modifying the existing main dashboard layout, which is higher risk. Option A is purely additive.

## Implementation Tasks

### Phase 1: Data Access Layer
- [ ] Add API client calls to read new skill metrics from postprocessing API
- [ ] Handle missing metrics gracefully (older data without Tier 1/2 metrics)
- [ ] Cache yearly metrics (they change only once per year)

### Phase 2: Interpretation Engine
- [ ] Create `MetricInterpreter` class that maps (metric_name, value) to (quality_category, interpretation_text)
- [ ] Support i18n — interpretation templates must be translatable (Russian, Kyrgyz at minimum)
- [ ] Make quality thresholds configurable via config.yaml
- [ ] Handle edge cases: NaN metrics, insufficient data warnings

### Phase 3: Visualization Components
- [ ] Create "Model Performance Report" tab
- [ ] Overall performance summary cards with quality badges
- [ ] Flood detection table with contingency counts
- [ ] Low-flow performance section
- [ ] Peak flow performance section
- [ ] Per-model comparison view (side-by-side for LR vs TFT vs EM)
- [ ] Per-temporal-resolution tabs (pentadal, decadal, monthly)

### Phase 4: Station Comparison View (optional)
- [ ] Cross-station heatmap (stations × metrics) for quick identification of problem stations
- [ ] Export to CSV for hydromet annual reports

## Acceptance Criteria

- [ ] Each metric has a plain-language interpretation visible next to the numeric value
- [ ] Interpretations use consistent quality categories (Very good / Good / Fair / Poor)
- [ ] Dashboard handles missing metrics without crashing (e.g., Tier 2 metrics unavailable for monthly forecasts)
- [ ] Quality thresholds are configurable, not hardcoded
- [ ] Translations available for Russian at minimum
- [ ] Page loads within 3 seconds (metrics are pre-calculated, only display is needed)

## Internationalization

Interpretation templates must support translation. Example structure:

```python
INTERPRETATION_TEMPLATES = {
    "pbias_overestimate": {
        "en": "The model **overestimates** total runoff by **{value}%**",
        "ru": "Модель **переоценивает** общий сток на **{value}%**",
    },
    "pbias_underestimate": {
        "en": "The model **underestimates** total runoff by **{value}%**",
        "ru": "Модель **недооценивает** общий сток на **{value}%**",
    },
    "kgelf_good": {
        "en": "**Good** overall performance with **good** low-flow reliability",
        "ru": "**Хорошее** общее качество с **хорошей** надёжностью в межень",
    },
    # ...
}
```

## References

- Moriasi et al. (2007, 2015) — performance rating thresholds for PBIAS
- Operational hydrologist review (2026-02-17) — interpretation requirement originated from domain expert feedback on metric adoption
- `postprocessing_unified_plan.md` Phases 4c/4d — metric calculation implementation

## Notes

- This issue is **blocked** by Phases 4c (Tier 1 metrics) and 4d (Tier 2 metrics) — the metrics must be calculated and available via API before they can be visualized
- The interpretation engine should be designed as a reusable component — it may also be used in automated reports or email alerts in future
- Consider colorblind-safe color coding for quality categories
