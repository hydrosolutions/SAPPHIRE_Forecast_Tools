# Postprocessing Skill-Metrics & Ensemble Correctness — Campaign Design

**Vision:** correct, internally-consistent postprocessing of forecasts — fix the confirmed
problems without introducing new errors.

**Base:** `maxat_sapphire_2` @ `502ac63c` (post PR #411 min-n/stale merge).
**Branch:** `develop_postprocessing_skill_correctness`.
**Spine:** vision WF1 (decompose) → WF2 (build), TDD, cross-vendor quality gate → PR → `run_tests.sh`.
**Source:** read-only Claude+Codex audit (2026-07-08), re-verified on this base. All 16 findings
present; the merged min-n gate is defeated by finding #1.

## Ratified design decisions (Phase-0 brainstorming, 2026-07-08)
- **D1** — short-term skilled gate is INCLUSIVE (`NSE≥0.8`, `accuracy≥0.80`, `sdivsigma≤thr`);
  long-term stays strict `NSE>0`. (Matches Central-Asia hydromet acceptance convention.)
- **D2** — the long-term NSE gate must NOT be silently disable-able: reject disable tokens for its
  env var and add an `isfinite` guard (also closes the nan-threshold hole). sdivsigma/accuracy
  remain disable-able for long-term (intended).
- **D3** — ONE canonical CRPS: both apps use the textbook `crps_from_quantiles` (factor-2 + flat
  tails) via a shared helper; fix NaN-quantile poisoning in the same change.
- **D4** — eval `short_term_dedup_one_per_target` defaults ON (one pair per target, matching the
  operational side; closes the operational-vs-eval divergence).
- **D5** — `fdc_flv` adopts Yilmaz et al. (2008) Eq.4 (leading negative sign + min-deviation
  normalization). Lowest priority.

## Mandatory (not a decision)
- **#1** — long-term monthly & seasonal skill count each stored issue-date row as a separate
  obs↔sim pair (`read_monthly_forecasts` has no dedup; `_deduplicate_seasonal_forecasts` keeps
  `date`). This inflates `n_pairs` and **defeats the just-merged min-n gate** (Codex reproduced:
  2 target seasons → n_pairs=5 → passes SEASON≥5). Fix = per-target dedup (keep latest issue)
  BEFORE the `n_pairs` count, for the raw-model/skill grouping; keep per-issue grouping only for
  EM composition. Quarterly is already safe (reader dedups).

## Milestones (WF1 will adversarially refine ordering/deps; this is the seed)
| ID | Findings | Scope (functions) | Acceptance |
|----|----------|-------------------|------------|
| **M1** (HIGH) | #1 (+re-baseline min-n) | `skill_metrics.calculate_monthly_skill_metrics`, `_calculate_aggregated_skill_metrics` | Reissued target contributes exactly 1 pair; seasonal repro (2 targets → n_pairs=2) locked; min-n gate no longer foolable |
| **M2** | D1/#3, D2/#4, #12, #11 | `filter_for_highly_skilled_forecasts`, `_parse_threshold_env`, `_long_term_threshold_overrides` | Boundary models (0.80) kept short-term; LT NSE gate un-disable-able; nan/invalid tokens raise; disable tokens robust |
| **M3** | #2 | `calculate_all_skill_metrics` | NSE/MAE/pbias/kgelf/nse_log use obs/sim mask only; delta mask applies to accuracy only; matches standalone fns |
| **M4** | D3/#5, #6 | shared `crps_from_quantiles`; both apps' CRPS callers | Both apps return identical textbook CRPS; NaN quantile row doesn't poison the group |
| **M5** | D4/#7 | `forecast_skill_eval/config.py` (+ callers) | Default run dedups short-term re-issues; documented; opt-out still available |
| **M6** | #8 | `forecast_skill_eval/orchestrator.py` | Requested `rp5` events produce contingency rows (or are rejected at config validation) |
| M7 (DEFERRED → issues) | D5/#14, #15, #16 | `fdc_flv`, `binary_contingency`, `calculate_sharpness` | Filed as gi_draft issues, not built this campaign |
| Coordination (not ours) | #9, #10 | `sapphire/services/` (colleague-owned) | Raised as discussion/issue; no code by us |

## No-new-errors guardrails
Per milestone: locked regression test authored before the fix (must fail→pass), Codex implements,
cross-vendor quality gate (strong Claude + `codex exec review` read the diff), then
`SAPPHIRE_TEST_ENV=True bash run_tests.sh <module>` (zero failures / zero unexpected skips) and
changed-line coverage before PR. EM output must stay byte-identical where a fix is not meant to
change it (M1's dedup changes n_pairs/NSE by design — re-baseline those tests explicitly).
