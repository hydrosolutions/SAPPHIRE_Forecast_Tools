# Plan — Below-norm (1.0) recalculation + H1 report refresh

**Created:** 2026-07-02
**Owner branch (proposed):** `develop_forecast_skill_eval_below_norm` (off `maxat_sapphire_2`)
**Status:** planned — not started. Gated on user go + local DB availability.

## Motivation

The forecast-skill evaluation currently defines the below-norm event as
`value < 0.80 × norm` — the operational **irrigation limit-plan** trigger. The
funder H1 report and the skill-eval report should ALSO carry the plain
**below-norm** event (`value < 1.0 × norm`) so we can talk about general
below-average water availability, not only the specific 80 % restriction rule.

**Decision (user, 2026-07-02):** compute **both** thresholds — keep 0.80 (limit
plan) and add 1.0 (below norm). The 0.80 cut stays the "operational restriction"
story; the 1.0 cut is the broader below-average-flow skill.

**Consequence to remember:** below-norm (1.0) has a much higher base rate
(≈ 0.5, higher on right-skewed flow distributions) than the 0.80 event, so its
POD/FAR/HSS will differ substantially and are NOT comparable to the 0.80 numbers
cell-for-cell. Report them side by side, never conflate.

## Current-state facts (verified 2026-07-02)

- CLI: `python -m forecast_skill_eval.cli` (module entry; no console script).
  `--threshold` (default 0.80) sets the factor for the `below_norm` event.
- `events.py`: `EventDef(name, direction, percentile, return_period=...)`.
  `below_norm = EventDef("below_norm", "below", None)` — factor comes from
  `--threshold`; there is **no per-event factor field**.
- Known-good run command (from prior session, adapt run-id/dates):
  ```
  uv run --project apps/forecast_skill_eval python -m forecast_skill_eval.cli \
    --base-url http://localhost:8000 --threshold 0.80 \
    --horizons day pentad decade month quarter season \
    --min-years 10 --operational-start 2024-01-01 \
    --provenance day=calculated pentad=calculated decade=official \
      month=official quarter=aggregated_from_monthly season=aggregated_from_monthly \
    --events <event list> --run-id <id>
  ```
- Flags `SAPPHIRE_SKILL_PROB` / `SAPPHIRE_SKILL_VALUE` add the probabilistic and
  value-metric families (default off).
- Artifacts + figures dirs are gitignored (operational data). Only the tracked
  `forecast_skill_eval_report_draft.md` and the untracked H1 report carry
  results — POOLED, no station codes.

---

## Phase 0 — Pre-flight (no code)

- **Goal:** confirm environment and lock the implementation approach.
- **Steps:**
  - Bring up local `postprocessing`/`preprocessing` DB stack; confirm the same
    83-station archive (65 kyg / 17 taj / 1 other) the last run used is present.
    Eval reads via `--base-url http://localhost:8000` (no auth/env needed).
  - Confirm `develop_forecast_skill_eval_phase4` residue / branch off clean
    `maxat_sapphire_2` (Phase-2/3/4 already merged via PRs #397, #400).
- **Acceptance:** DB healthy; `curl localhost:8000/health/ready` OK; branch created.

## Phase 1 — Add the below-norm (1.0) event  *(code — delegate to Sonnet)*

- **Goal:** make one run compute both 0.80 and 1.0 below-norm events.
- **Files:** `apps/forecast_skill_eval/src/forecast_skill_eval/events.py`,
  its reclassify caller (whichever module reads `EventDef` factor), and the
  matching tests under `apps/forecast_skill_eval/tests/`.
- **Approach (recommended):** add an optional `factor: float | None = None` field
  to `EventDef`; define `EventDef("below_norm_100", "below", None, factor=1.0)`.
  `below_norm` continues to draw its factor from `--threshold` (0.80) for
  backward-compatibility. Reclassify uses `event.factor` when set, else the
  global threshold. **Purely additive** — existing `below_norm` output must stay
  byte-identical (regression test).
- **Constraint to the agent:** do NOT change existing function signatures, the
  `below_norm` semantics, or control flow. Additive field + one new EventDef +
  tests only.
- **Alternative (no code):** two full runs (`--threshold 0.80`, then `1.0`) into
  separate artifact dirs. Rejected as default — repeats the expensive fetch and
  complicates the report merge — but keep as fallback if the EventDef change
  proves invasive.
- **Acceptance:** `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_skill_eval`
  green, zero skips; new equivalence test proves `below_norm` unchanged.

## Phase 2 — Recalculate (data run; depends on P1)

- **Goal:** one comprehensive artifact set so the whole report is internally
  consistent.
- **Run:** full archive, all stations/models, 6 horizons, with
  `--events below_norm below_norm_100 low_p10 low_p5 high_p90 high_p95` and
  `SAPPHIRE_SKILL_PROB=true SAPPHIRE_SKILL_VALUE=true`, into
  `artifacts/rerun_2026-07-02_both_thresholds/` (gitignored).
  Expect ~1–2 h (data fetch + pairing dominate; reclassify already vectorised).
  - **Scope option:** if the full prob+value+events run is too slow, run the two
    below-norm events on their own for the contingency refresh and reuse the
    existing prob/value/high-flow artifacts (they are threshold-independent for
    the 1.0 addition). Note the split in the report if taken.
- **Acceptance:** contingency CSV carries both `below_norm` and `below_norm_100`;
  0.80 numbers reproduce the current report; 1.0 base rate ≈ 0.5 sanity-checks.

## Phase 3 — Update the skill-eval report draft (depends on P2)

- **Goal:** add the below-norm (1.0) results next to the 0.80 limit-plan results.
- **File:** `doc/plans/working/forecast_skill_eval_report_draft.md` (tracked).
- **Steps:** add a below-norm (1.0) results block per horizon; explicitly
  contrast base rates and warn against cross-threshold comparison; refresh
  `make_figures.py` / figures if a below-norm panel is wanted (figures dir is
  gitignored). Keep POOLED, **no station codes**.
- **Acceptance:** both thresholds documented; sensitive-data scan clean.

## Phase 4 — Propagate to the H1 funder report (depends on P3)

- **Goal:** reflect both thresholds + drop in the screenshots.
- **File:** `doc/reports/2026_H1_report.md` (untracked).
- **Steps:**
  - Appendix: refresh 0.80 figures if the fresh run shifted them; add a short
    below-norm (1.0) line in funder terms (general below-average skill vs the
    0.80 restriction rule). Keep it short — appendix, not body.
  - Confirm the population-impact paragraph still reads correctly against both.
  - Add a one-line pointer in the appendix that the full technical evaluation is
    available as a companion annex (the P5 deliverable).
  - Insert the two screenshots the user provides into
    `doc/reports/2026_H1_figures/` (`unified_dashboard.png`,
    `monthly_seasonal_forecast.png`) — placeholders already wired in the report.
- **Acceptance:** report renders with figures; sensitive-data scan clean;
  numbers trace to the P2 artifact set.

## Phase 5 — Technical reports, two tiers (depends on P3)

Produce the polished technical evaluation in **two tiers** with strict data
governance. Register for both = polished technical (keep tables + rigour + an
executive summary), NOT a second non-technical rewrite — the H1 appendix already
fills the funder-digest role.

### Phase 5a — Shareable aggregate report (NO station codes)

- **Audience:** all hydromets + the funder. POOLED aggregates only.
- **File:** NEW `doc/reports/2026_H1_skill_eval_annex.md` (+ figures beside it).
  Because it is code-free it *may* optionally be tracked; default = keep
  untracked with the H1 report. Leave `forecast_skill_eval_report_draft.md` as
  the internal working draft — do not overwrite it.
- **Steps (mostly polish; no new analysis):**
  - Add a proper **title + executive summary** (headline findings, short
    paragraph + bullets) — the one genuinely new piece of writing.
  - **Strip internal plumbing:** the `Status:` line, all `phase-N` / `re-run` /
    `rerun_*` / `artifacts/…` path mentions, `--run-id`, `SAPPHIRE_SKILL_*` flag
    names, and the entire "Related documents" section.
  - **Remove sensitive hints:** delete the station-code-prefix line ("prefixes
    15, 16 … 17") and soften the "Station codes" coverage-table column.
  - **Embed the figures** (17 pooled PNGs, current in
    `doc/plans/working/forecast_skill_eval_figures/`) with clean captions,
    replacing the filename-list "Figures" section; add any below-norm (1.0)
    figure from P3. Copy PNGs into a folder beside the annex
    (e.g. `doc/reports/2026_H1_skill_eval_figures/`).
  - **Sensitive spot-check** ~4 figures to confirm they are pooled / show no
    station codes.
- **Acceptance:** renders standalone; sensitive-scan clean (zero codes / paths /
  discharge / credentials); numbers match the P2 artifact set; H1 appendix's
  companion-annex pointer (P4) resolves to this file.

### Phase 5b — Per-hydromet internal reports (WITH that org's station codes)

- **Audience:** each operational hydromet, for its own internal use only.
- **Scope:** one report per **operational** hydromet — **Kyrgyz** and **Tajik**.
  Uzbek excluded (4-site demo, no evaluation archive). **Org membership is defined
  by each deployment's own station config** (`config_all_stations_library` /
  `config_station_selection` on that org's server), **NOT by station-code prefix.**
  Prefixes are **river basins** (16 = Syr Darya, 17 = Amu Darya, 15 = another
  basin), and a service operates stations across basins — e.g. the Tajik
  deployment operates Syr-Darya (`16…`) stations alongside its Amu-Darya (`17…`)
  stations. Any station not listed in a known org's config is excluded from
  per-hydromet reports (retained only in the 5a pooled aggregate).
- **Content:** per-station tables (station code + POD/FAR/HSS/n + Wilson CI, both
  thresholds) drawn from the per-station rows in the P2 contingency output, plus
  the shared **pooled** figures for context. **No per-station figure
  generation** (decided: tables + pooled figures — no `make_figures.py`
  extension).
- **DATA GOVERNANCE (HARD — these files contain station codes):**
  - **Never committed to git.** Write to a gitignored, untracked location
    (e.g. `reports_internal/skill_eval/<org>/…`, add the dir to `.gitignore`);
    verify `git status` never lists them.
  - **Strict org isolation:** assign each station to an org **by membership in
    that org's deployment station config** (`config_all_stations_library` /
    `config_station_selection`), **never by code prefix** (prefix = basin, not
    service). The Kyrgyz file must contain only Kyrgyz-config codes and the Tajik
    file only Tajik-config codes; verify each file's code set against the
    source-of-truth config for that org. Any station not in a known org's config
    is excluded (do NOT guess an org from its basin prefix — that leaks codes
    across services).
  - **Delivery** to each hydromet via their own secure channel only.
  - **Tajik caveat:** flag long-term (month/quarter/season) numbers as
    low-confidence / wide-CI where the Tajik station sample is thin. **Recompute
    the per-org station balance from config membership, not prefix counts** — the
    earlier "65 Kyrgyz vs 17 Tajik by prefix" figure was a basin miscount and must
    not be reused. (The tracked `forecast_skill_eval_report_draft.md` line ~85
    carries that same prefix-based mislabel; correct or drop it if the draft is
    ever used for org attribution — Phase 5a already strips it from the shareable
    annex.)
- **Acceptance:** exactly one file per operational org; each contains only its
  org's codes (verified); none tracked by git (`git status` clean of them);
  Tajik low-confidence caveat present.

## Phase 6 — Verify + present (depends on P4, P5a, P5b)

- Full test suite green; three-axis check on every document (H1 report, H1
  appendix, shareable annex, per-hydromet reports): accuracy → source; register
  appropriate to each; correct data tier (aggregate = zero codes; per-hydromet =
  only its own org's codes, never in git). Present for sign-off. Nothing
  committed or shared without explicit user go.

---

## Dependency graph

```json
{
  "phases": {
    "P0":  { "depends_on": [], "parallel_agents": 0 },
    "P1":  { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2":  { "depends_on": ["P1"], "parallel_agents": 0 },
    "P3":  { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4":  { "depends_on": ["P3"], "parallel_agents": 1 },
    "P5a": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P5b": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P6":  { "depends_on": ["P4", "P5a", "P5b"], "parallel_agents": 0 }
  }
}
```

(P4, P5a, and P5b are independent and may run in parallel after P3.)

## Out of scope / parked

- User-facing "run the quality metrics from the dashboard" button (a
  forecast_dashboard button that triggers the eval and opens the viewer) —
  noted as a future idea, **low priority, no project hours remain**. Not in this
  plan.
- Vectorising `prob_metrics._score_pairs` (prob-run perf) — unrelated, parked.
