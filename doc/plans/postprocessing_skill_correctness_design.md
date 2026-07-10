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

---

## Review update (2026-07-08) — WF1 output + critical milestone review

**WF1 (vision-decompose) result:** 6 milestones, all 4 adversarial reviewers approved. Applied
advisories: M6 scoped to PRODUCE rp5 rows only (the "reject at config validation" branch would
break the locked test_events.py:585-590 contract that rp5/rp10/rp30/rp100 are accepted); deps
relaxed to two tracks (skill_metrics M1→M2→M3→M4 serial for same-file churn; forecast_skill_eval
M5 ∥ M6 independent); "byte-identical" reworded to "numerically unchanged for unmodified paths".

**Confirmed domain facts (user, 2026-07-08):** ONE target season (irrigation, months 4–9);
`season_in_year` = the ISSUE LEAD, not a distinct season; skill + ensembles are wanted PER LEAD;
min-n floors (MONTH≥4/QUARTER≥5/SEASON≥5) = minimum number of YEARS of paired history.

**M1 INVERTED — now BLOCKED pending design.** The user confirmed day-10 vs day-25 issues within
the same issue-month are DIFFERENT leads that must each get their own skill metric + ensemble.
Current `horizon_value` is a COARSE month-level lead (0–3) that collapses them; the issue day is
stored (`LongForecast.date`) but is not a grouping key. So the original M1 plan (dedup re-issues,
keep latest) is WRONG — it would delete a forecast the user wants to keep. Correct direction:
STRATIFY skill/ensembles by a finer lead, not collapse. This is a lead-taxonomy redesign, overlaps
the parked lead-aware project (`SAPPHIRE_SKILL_LEAD_AWARE`, not on this base), and may touch the
colleague-owned skill_metrics/long_forecasts key. OPEN DECISION (awaiting user): (a) define what a
lead is (issue day-of-month / issue-date / lead-days bucket); (b) how it's keyed/stored (re-encode
horizon_value vs add a field → service coordination); (c) campaign path — design-first / fold-in /
park M1 & ship M2–M6.

**M2–M6:** independent of the lead question; not yet critically reviewed with the user (pending).
NOT fed to WF2 yet — no build until the user approves the path.

---

## M1 design pass — RESOLVED (2026-07-08, config-driven)

Source of truth = deployment long-term configs (`<org>_data_forecast_tools/config/long_term_configs/`),
NOT the relic-contaminated DB. Empirics: DB has ~32 issue-dates/target-year for MONTH (pentad-day
relics from backfill/hindcast); operationally each mode issues ONCE.

**Operational taxonomy (per org, from config `operational_issue_day` / `operational_month_lead_time`
/ `target_*_month`):**
- Kyrgyz: month_0 (d10,L0), month_1/2/3 (d25, L1/2/3), quarter (d25,L1), seasonal Jan/Feb/Mar/Apr
  (d25, L3/2/1/0, target months 4–9). 9 modes.
- Tajik: month_1/2/3 (d1, L0/1/2), quarter (d1,L0), seasonal_april (d1,L0, months 4–9). 5 modes.

**Resolved M1 design:**
1. Lead taxonomy = config modes (month-level lead); one `(horizon, target-window, lead,
   operational_issue_day)` per mode per org. Stored in `horizon_value` (month-level → NO service
   schema change). Matches the parked lead-aware derived `lead_months`.
2. Pairing = ONE operational forecast per `(code, mode, target-year)`: **select by configured lead
   across all history** — the issuance whose derived lead `(valid_from.y-date.y)*12 +
   (valid_from.m-date.m)` == mode's `operational_month_lead_time`; among candidates prefer
   `operational_issue_day`, else nearest/latest. Collapses relics; uses full hindcast history →
   `n_pairs = number of target-years`.
3. Skill + ensembles computed per mode/lead (preserves per-lead separation). Relics excluded from
   operational skill (candidates for the merged stale-tombstone cleanup).
4. Gates: #411 hard floor (MONTH≥4/QUARTER≥5/SEASON≥5) for ensemble-eligibility + per-lead
   NaN-at-`n_pairs<2` for variance metrics (sdivsigma/nse).

**Relationship to parked lead-aware project:** this IS that project, refined. Reuse its locked
decisions + its already-built P2 reader lead-derivation (flag-gated, uncommitted); ADD the
config-driven operational-issuance selection (parked plan accepted intra-lead re-issue pooling as
"rare" — we now know those are relics and select the operational one instead). Reconcile with the
merged #411 min-n/tombstone (built after the parked P2). Recommend RESUMING the lead-aware
plan/branch rather than building M1 from scratch.

---

## M2–M6 critical review — RESOLVED (2026-07-08)

- **M2** (gates): operator = **global inclusive `>=`/`<=`** + **long-term NSE threshold = small epsilon**
  so NSE=0 stays excluded (honours strict-positive without a horizon flag). Lock the long-term NSE
  env var against disable tokens (metric-specific in `_parse_threshold_env`); add `isfinite` guard
  (rejects nan/inf, closes #12); route the short-term direct-env read through the lenient parser
  (closes #11). sdivsigma/accuracy stay disable-able long-term.
- **M3** (per-metric masks, #2): split masks — NSE/MAE/pbias/kgelf/nse_log use obs/sim-only; delta
  mask applies to accuracy/delta only. **Stored `n_pairs` = obs/sim pair count** (accuracy uses its
  own delta-valid subset internally). Numerically identical where delta is finite (monthly fills 0.0).
- **M4** (CRPS, D3/#5+#6): one canonical `crps_from_quantiles` (factor-2 + flat tails + NaN-quantile
  handling) in **`iEasyHydroForecast`** (both apps already depend on it); both import it. Stored
  postprocessing `crps` changes (~2x, informational, no gate) — **accept mixed old/new until next
  recalc** (no special action).
- **M5** (eval short-term dedup, D4/#7): flip `short_term_dedup_one_per_target` default **ON** using
  the existing latest-issue rule; **minimal scope** — eval↔operational LONG-TERM parity is handled
  inside M1's lead-aware resumption (its P4), not here.
- **M6** (rp events, #8): **DEFERRED to the M7 tier** — eval-only feature-completeness (a silent
  no-op), not a core skill/ensemble correctness/consistency fix. Backlog with #14/#15/#16.

**Campaign scope is now M1–M5.** M1 = resume+refine the parked lead-aware project (config-driven
operational-issuance selection + reconcile with merged #411). M2–M5 = independent, mostly single-file
correctness/consistency fixes. M6 + M7 (#14/#15/#16) = deferred backlog issues. #9/#10 = colleague
service coordination.
