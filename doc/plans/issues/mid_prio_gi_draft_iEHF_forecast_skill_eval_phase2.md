# forecast_skill_eval Phase 2 — seasonal disaggregation, persistence baseline, percentile & return-period detection

**Status**: Draft (scope + priorities agreed with user 2026-06-30)
**Module**: `apps/forecast_skill_eval`
**Priority**: Medium (A/B), Low (C/D)
**Labels**: `forecast-skill-eval`, `evaluation`, `enhancement`

---

## Summary
Extend the irrigation FP/FN skill evaluator (Phase 1, results in
`doc/plans/working/forecast_skill_eval_report_draft.md`) along four lines: seasonal
disaggregation, a persistence baseline, percentile-based event detection (low **and**
high flow), and return-period (flood) detection.

## Context
Phase 1 evaluates the binary **below-norm / limit-plan** decision (`value < 0.80×norm`)
pooled across the whole year, with **climatology** (always-normal → POD 0/HSS 0) and
**operational_proxy** (LR/LR_Base) baselines. The user wants the evaluation extended.

## Scope & priorities (agreed)

### A — Seasonal disaggregation (PRIORITY 1)
Stratify the below-norm skill by season, with the **April–September irrigation season**
as the headline window (that's when the limit-plan decision matters). Add a season /
target-month stratification dimension (irrigation Apr–Sep vs non-irrigation Oct–Mar, and
ideally per-calendar-month) alongside the existing regime/provenance dimensions, plus a
CLI filter. Surface in artifacts + report + figures.

### B — Persistence baseline (PRIORITY 2)
Add a **persistence** baseline = forecast equals the **last measured flow** (most recent
completed observed period before the forecast issue date), classified against the same
`0.80×norm` boundary. New `build_persistence_baseline` in `baselines.py`, wired into the
orchestrator + artifacts, shown alongside climatology in the report/figures. Expected to
beat climatology at short lead and decay with lead — a more informative naive benchmark.
- Climatology baseline: already exists (POD 0, HSS 0, misses 100% of events).
- operational_proxy: already exists (= LR / LR_Base statistical model, not persistence).

### C — Percentile detection, low **and** high flow (PRIORITY 3)
Generalize the event rule (today only `value < threshold×norm`) to percentile events:
- **Low-flow:** observed/forecast **below** the 10th / 5th / (1st) percentile.
- **High-flow:** observed/forecast **above** the 90th / 95th / (99th) percentile.
Percentiles computed per station (and per period/season as appropriate). Same contingency
machinery, new event definition + CLI (`--event {below-norm,low-pctile,high-pctile}
--percentile P`). High-flow is a **distinct use case** (flood / hydropower / reservoir),
likely its own report section.

### D — Return-period detection (PRIORITY 4, exploratory — feasibility-limited)
Detect flows exceeding **5 / 10 / 30 / 100-yr** return levels via extreme-value (e.g. GEV
on annual/seasonal maxima) per station. **Limit:** archive is ~17–26 years, so 5–10-yr is
marginally estimable, 30-yr is a stretch, and **100-yr is beyond reliable estimation**
(extrapolation far past the data → very wide CIs). Report 30/100-yr as exploratory only,
or omit. Build after A–C.

## Open questions (resolve before C/D)
- C: exact percentile set (low 10/5/1, high 90/95/99?) and percentile basis (whole record
  vs per-period vs per-season).
- C: is high-flow a separate report/use case from the irrigation low-flow decision?
- D: EVT method + minimum years per station; whether to attempt 30/100-yr at all.

## Acceptance criteria
- [ ] A: results stratified by season; Apr–Sep irrigation window reported separately; CLI filter; tests.
- [ ] B: persistence baseline computed + shown vs climatology + models; tests.
- [ ] C: low- and high-flow percentile events evaluated; tests; report section.
- [ ] D: return-period detection where estimable, with explicit feasibility caveats.
- [ ] Read-only (no DB writes); no station codes in tracked outputs; `run_tests.sh forecast_skill_eval` green.

## References
- Phase-1 report: `doc/plans/working/forecast_skill_eval_report_draft.md`
- Eval app: `apps/forecast_skill_eval/src/forecast_skill_eval/` (`pairs.py`, `observed_truth.py`, `contingency.py`, `baselines.py`, `orchestrator.py`, `cli.py`, `config.py`)
- Figures: `doc/plans/working/forecast_skill_eval_figures/make_figures.py`
