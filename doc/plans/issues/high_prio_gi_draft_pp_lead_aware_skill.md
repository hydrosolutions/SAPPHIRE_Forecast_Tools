# gi_draft: pp — config-driven per-lead operational skill & ensembles (M1 / lead-aware resumption)

**Fitness:** every bullet must help an agent know what to inspect, what contract not to break, or what
verification proves safety — otherwise cut it.

> Scope was hardened by an adversarial review (2026-07-08, `codex exec`): 8 findings folded in — the
> flag-off baseline reality, the full set of aggregated key sites, a dedicated quarter subphase, the
> #411 tombstone-sentinel interaction, the dashboard/writer surfaces, and a broadened regression matrix.

## Problem (implemented vs intended)
Long-term monthly & seasonal skill count **each stored issue-date row** as a separate obs↔sim pair
(`calculate_monthly_skill_metrics`, `_calculate_aggregated_skill_metrics` in
`apps/postprocessing_forecasts/src/skill_metrics.py`). The `long_forecasts` DB holds many
backfill/hindcast issue dates per target (measured: MONTH ~32 issue-dates/target-year; SEASON avg
1.26), so `n_pairs` inflates and the merged **#411 min-n gate is defeated** (Codex-reproduced: 2
target years → n_pairs=5 → passes SEASON≥5). CONFIRMED bug; fix direction = **stratify by the true
operational lead, not collapse blindly**.

## Intended behavior (owner-ratified 2026-07-08)
- ONE irrigation season (config `SAPPHIRE_SEASON_*`, months 4–9). `season_in_year` = issue LEAD, not a
  season. Skill + ensembles per lead, pooled across YEARS; min-n floors = minimum YEARS.
- Lead taxonomy = deployment **long-term configs** (source of truth, NOT the relic DB); each mode issued
  ONCE at a configured `operational_issue_day` + `operational_month_lead_time`: KYG 9 modes (month_0
  d10/L0; month_1–3 d25/L1–3; quarter d25/L1; seasonal Jan/Feb/Mar/Apr d25/L3–0, target 4–9); TAJ 5
  modes (month_1–3 d1/L0–2; quarter d1/L0; seasonal_april d1/L0).
- Lead is MONTH-level → stored in existing `horizon_value` → **no service-schema change** (verify).
- Pairing = one operational forecast per `(code, mode, target-year)`, selected by configured lead across
  history; relics excluded; n_pairs = years.
- Gate reconciliation: keep #411 hard floor + per-lead NaN-at-`n_pairs<2` for variance metrics.

## Reuse (do not rebuild)
This IS the parked lead-aware project, refined. Decisions/P0 locked in
`doc/plans/working/skill_lead_aware_plan_revised.md`; its P2 reader lead-derivation (flag-gated) was
built (uncommitted). RESUME + ADD config-driven operational-issuance selection + reconcile with #411
(built AFTER that P2).

## Flag & boundaries
- New flag `SAPPHIRE_SKILL_LEAD_AWARE`, default OFF, via ONE shared helper. **Flag-OFF must be
  byte-identical to pre-lead-aware behavior** — see P0 (monthly `GROUP_COLS`/`ENSEMBLE_KEY` already
  include `horizon_value` UNGUARDED on this base; the flag must wrap/neutralize that too, else "flag-off
  golden" is meaningless).
- Apps only. **No `sapphire/services/` edit** (month-level lead fits `horizon_value`); if a schema/GET
  change seems required, STOP and escalate (service is colleague-owned).

## Phases

### P0 — baseline audit + flag scaffold (no behavior change)
- Enumerate every EXISTING lead-aware delta already on this base (e.g. `skill_metrics.py` `GROUP_COLS`
  / `ENSEMBLE_KEY` ~250-251, ~1551-1557; any horizon_value in monthly grouping) — no
  `SAPPHIRE_SKILL_LEAD_AWARE` guard exists yet (`rg` confirms).
- Introduce one shared flag helper; wrap ALL lead-aware read/group/write/dashboard paths so flag-OFF
  reproduces the pre-lead-aware behavior.
- Capture the **flag-OFF golden baseline** (API-shaped skill payloads, CSVs, tombstone rows, dashboard
  frames) BEFORE any new edit. **Depends on:** none.

### P1 — config-driven operational-issuance selection
- Resolver (`iEasyHydroForecast/long_term_horizon_resolver.py`): add generic enumeration exposing, per
  supported mode (month_0..3, quarter, seasonal_*), both `operational_month_lead_time` and
  `operational_issue_day` (only lead helpers exist today; there is NO `operational_issue_day` accessor).
- Add a **pure `select_operational_issuances` step** immediately after long-forecast normalization and
  BEFORE aggregation/skill/ensemble, for month + quarter + season: derive lead
  `(valid_from.y−date.y)*12 + (valid_from.m−date.m)`; keep one row per `(code, mode, target-year)` whose
  derived lead == config lead, preferring `operational_issue_day` (deterministic tie + no-candidate
  behavior defined); drop baseline rows (EM/Naive/Skilled — they carry no issue date) before selection;
  carry/overwrite the selected lead into `horizon_value`. **Depends on:** P0.

### P1b — quarter lead subphase (quarter needs its own coverage)
Quarter is NOT covered by skill grouping alone — multiple paths drop/overwrite/blend/dedup
`horizon_value`: `_QUARTERLY_FC_COLS` omits it (`data_reader.py:57-72`); normalization drops it for
non-season (`~3144-3200`); monthly→quarter aggregation groups period/code/model only
(`aggregation.py:217-282`); the writer overwrites row lead with `quarter_horizon_value()`
(`api_writer.py:1093`); gap detector + operational/maintenance dedup are period-only. Preserve/carry
`horizon_value` through all of these under the flag; writer prefers row `horizon_value`. **Depends on:** P1.

### P2 — per-lead skill + ensembles (enumerate EVERY key site)
Monthly skill already stratifies by lead; the AGGREGATED path does not. Under the flag add the derived
lead to ALL of: `_calculate_aggregated_skill_metrics` empty/output schemas + `metric_group_cols`
(`skill_metrics.py:2343`), point-metric group-key unpacking, CRPS record dicts, EM group + `time_group_cols`,
and the aggregated ensemble helpers `_add_naive_mean_aggregated` / `_add_skilled_mean_aggregated`
(weight joins) / `_create_aggregated_ensemble_forecasts` (`~2578-2842`, `ensemble_calculator.py:595-842`).
Reconcile with #411 gate (floor + NaN-at-<2). TDD proving separate per-lead rows for point metrics,
CRPS, EM, Naive, and Skilled Mean. **Depends on:** P1, P1b.

### P2b — #411 tombstone reconciliation (flag-aware)
`stale_tombstones.py` (~52-90) and recalc (~367-369, ~450-452) inject sentinel `horizon_value=0` for
quarter/season when emitted frames omit the column. Under flag-ON, emitted quarter/season skill frames
MUST carry real `horizon_value`; make tombstone key construction flag-aware; emit transitional
tombstones for old sentinel-0 keys; tests for hv0/hv1 coexistence, #411 min-n drops, and dashboard
filtering of tombstoned per-lead rows. **Depends on:** P2.

### P3 — read-path + dashboard follow-up (broader than the monthly hv=1 merge)
Flag-gated, preserve/merge on the lead key across ALL sites: `get_long_forecasts` drops `horizon_value`
(`db.py:655-741`); `_get_data_monthly` filters stats to hv=1 and reuses for month_0 (`~885-1065`);
quarter stats merge period-only; `get_long_forecasts_quarter` dedups latest by code/model only. Handle
month_0 separately from month_1; empty schemas keep `horizon_value`. Dashboard data-frame tests for
monthly hv0/hv1 and quarter hv0/hv1. **Depends on:** P2.

### P3b — writer/API contract verification
`file_writer` sort/consistency keys omit `horizon_value` (`~366-411, ~617-652`); `_write_aggregated_forecasts_to_api`
overwrites quarter lead (`~995-1153`); service model comments define quarter/season skill `horizon_value`
as sentinel 0 (`models.py:207-236`); skill-metric GET has no `horizon_value` filter (`main.py:219-241`).
Under flag-ON: include `horizon_value` in skill sort/consistency keys; preserve row lead on quarter
writes; confirm skill POST/GET round-trips per-lead rows; **decide explicitly** whether service
docs/client filtering need change → if yes, ESCALATE (do not edit services). **Depends on:** P2.

### P4 — verify (broad regression matrix + flag-off golden)
Locked regressions: monthly reissues collapse to one pair per (code,mode,target-year); quarter
direct-only WITH aggregated-empty; monthly→quarter aggregation stratified by lead; aggregated
EM/Naive/Skilled/CRPS emit per-lead rows; #411 min-n drop → correct tombstone (right lead); dashboard
merges show hv0 vs hv1 correctly; selection tie/no-candidate deterministic; and the **flag-OFF golden**
(files + API-shaped records) byte-identical. Seasonal repro (3 target years at a fixed lead + re-issues
→ n_pairs=3; 2 real years must NOT pass SEASON≥5) is necessary but NOT sufficient on its own.
**Depends on:** P3, P3b, P2b.

```json
{"phases":{"P0":{"depends_on":[],"parallel_agents":1},"P1":{"depends_on":["P0"],"parallel_agents":1},"P1b":{"depends_on":["P1"],"parallel_agents":1},"P2":{"depends_on":["P1","P1b"],"parallel_agents":1},"P2b":{"depends_on":["P2"],"parallel_agents":1},"P3":{"depends_on":["P2"],"parallel_agents":1},"P3b":{"depends_on":["P2"],"parallel_agents":1},"P4":{"depends_on":["P2b","P3","P3b"],"parallel_agents":0}}}
```

## High-risk coupling (see doc/dev/agent_review_workflow.md — verify, don't assume)
`skill_metrics`/`long_forecasts` keys + `horizon_value` (per-horizon meaning is subtle/has drifted);
dashboard readers + tombstone/`horizon_value`; `sapphire/services/` boundary (no edits — escalate).

## Acceptance
Flag-ON: per-lead skill rows keyed by derived lead; one operational pair per (code,mode,target-year);
#411 floor honored; tombstones per-lead-correct; dashboard tiles per lead; full P4 matrix green.
Flag-OFF: byte-identical to the P0 golden. Depends only on building atop the corrected `skill_metrics.py`
(M2/M3/M4).

---

## Round-2 review refinements (folded 2026-07-10, second adversarial pass)

Sharpen the phases above with these (each maps to a phase):

- **P0 ordering (was L):** add the shared flag and NEUTRALIZE the already-present unguarded monthly
  lead-aware deltas FIRST, THEN capture the flag-OFF golden. Add a verification step that the parked
  lead-aware doc/configs exist (or inline the needed decisions) — do not assume absent external state.
- **P1 selector unit (H):** the selected unit is `(mode/horizon_type, code, model_short, target_year,
  target_period, lead)` — not just `(code, mode, target-year)`. Tests with 2 models × multiple monthly
  targets in one year.
- **P1 require-not-prefer (H):** REQUIRE both the configured lead AND `operational_issue_day` (explicit
  tolerance if any); if no exact operational candidate exists for a (target, model), DROP it and LOG —
  do not fall back to a backfill row. Identify baseline rows (EM/Naive/Skilled) by **canonical
  model-name filtering**, not a missing-issue-date heuristic.
- **P1 read-then-derive-then-filter (H):** under flag-ON, raw-forecast selection reads WITHOUT a
  `horizon_value` filter, derives the lead from `valid_from`/`date`, THEN filters. Keep display/read
  `horizon_value` filters separate from selector input (`data_reader.py` ~2722-2728, ~2800-2806,
  ~2878-2884, ~2961-2967, ~3021-3025).
- **P1 read-window vs target (H):** the API issue-date read window (`data_reader.py:1111-1128`,
  service `crud.py:183-186`) is issue-date-based but selection is target-period-based — expand the
  issue-date start backward by the max configured lead, then trim selected rows by `valid_from` to the
  requested target-year range. Add a Jan / Q1 boundary regression.
- **P1 selector placement (M):** apply selection ONLY to raw forecast inputs before skill/ensemble
  generation — NOT to the combined existing-forecast readers that also use `_normalize_combined_forecasts`
  (`~3006-3055`, `~3144-3200`). Use an explicit parameter or a separate helper call site.
- **P1b quarter write/read contract (M):** carry quarter `date` through direct-quarter normalization
  (`_QUARTERLY_FC_COLS` ~57-72) and the writer (`api_writer.py:1084-1125`); the quarter writer prefers
  row `date`/`horizon_value`, falling back only for monthly-aggregated quarter rows. Test the quarter
  write→read→derive round-trip.
- **P3 monthly breadth (M):** make monthly operational/dashboard handling config-driven over ALL
  supported `month_N` modes (not hard-coded latest + hv0/hv1); merge stats on `horizon_value`
  (`data_reader.py:1223-1233`, `db.py:898-909, 964-979`); P4 dashboard tests cover hv0/1/2/3 where supported.
- **P3/maintenance gap detector (M):** under flag-ON include `horizon_value` in the monthly gap
  detector pairs / model pairs / `gap_set` / `gap_keys` (`gap_detector.py:229-333`;
  `postprocessing_maintenance_long_term.py:160-219`); regression where one lead has EM and another doesn't.

**Review status:** two adversarial passes complete (8 + 9 findings, all folded). The plan is now
directionally sound and names the coupling/sites. Remaining specificity is build-phase detail — each M1
sub-phase carries its own TDD + out-of-loop adversarial review + tests-as-done per the merged workflow;
do NOT loop the plan review further (proportionality).
