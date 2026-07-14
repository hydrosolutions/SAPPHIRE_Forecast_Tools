# Revised plan — lead-aware long-term skill metrics (post adversarial review)

Implements issue `doc/plans/issues/high_prio_gi_draft_pp_skill_lead_pooling.md`.
Revised after a 5-lens adversarial review (verdict: **REVISE** — proceed only after
the changes below). All blockers were verified against code. Workflow run
`wf_1efa3781-779`.

## Verdict summary

Approach is sound (flag-gated, default-off apps-side prep + docs-only service
request; CLAUDE.md authorizes the `apps/postprocessing_forecasts` edits the issue
file wrongly calls "colleague-managed"). But the plan as first drafted was
under-specified in ways that would silently corrupt skill output. Five blockers:

- **B1 — P3 scope understated ~10x.** ~15 hardcoded 3-element group keys / 3-tuple
  unpacks (`skill_metrics.py:1208` point, `:1313` EM, `:1447` Naive, `:1630`
  Skilled-Mean) plus the aggregated-path CRPS guard `len(group_key)==3` at `:2168`.
  Adding a 4th key → `ValueError` (month) or **silent mis-bind** (quarter/season:
  `piy=1, code=horizon_value, model=code`) then `KeyError`/empty on
  `merge(..., on=metric_group_cols)` because `crps_records` never carries the lead.
- **B2 — aggregated-quarter lead is undefined.** The 3 monthly forecasts of one
  issuance carry *distinct* leads; adding `horizon_value` to
  `aggregate_monthly_fc_to_quarterly` groupby (`aggregation.py:252`) → singleton
  groups all dropped by `QUARTER_MIN_MONTHS=2` (`:257`) → **empties the quarter
  source** while looking "cleaner" (verification false-positive).
- **B3 — `horizon_value` is overloaded and is NOT lead for quarter/season.**
  `api_writer.py:1051` writes `horizon_value = quarter number`; `:1067` writes
  `horizon_value = 1` for *every* season row; `:348` writes `pentad_in_month`. So
  stratifying skill on `horizon_value` is a **no-op for direct-quarter** (= quarter
  number) and **impossible for season** (constant 1). The core mechanism is
  unfounded for the two worst horizons until the semantics are pinned. The two
  quarter sources (aggregated-from-monthly vs direct-API) also carry incompatible
  `horizon_value` meanings yet get pooled.
- **B4 — in-process dedup collapses leads before the API.** `api_writer.py:592`
  does `drop_duplicates(subset=['code','model_type','_date',horizon_in_year])`
  *before* any API call. With no lead in that key, all leads collapse last-write-wins
  **even with the flag ON and a migrated DB** — so "apps side ready" is false.
- **B5 — no min-n_pairs guard → re-creates the n_pairs=1 corruption.** Saved-recalc
  season median `n_pairs≈3`; split across up to 4 leads ⇒ mostly `n_pairs=1`.
  `sdivsigma`/`nse` use `np.std(ddof=1)` → NaN/undefined at n=1. This is the exact
  pathology behind the 2026-02-25 corrupted-CSV backup.

Strengths the panel affirmed: the default-off flag + docs-only service request is the
right safety posture; parity-to-`forecast_skill_eval` is the right contract; and this
plan's problem statement is *more* accurate than the issue file (season is read direct
from the API — there is no `aggregate_monthly_fc_to_seasonal`).

## Decisions — RESOLVED by user 2026-06-29

1. **(B3) Lead column → REUSE `horizon_value`.** Do not introduce `lead_months`. The
   skill dimension and the new `SkillMetric` service column are both named
   `horizon_value`. **Critical reconciliation:** the value stored in `horizon_value`
   in `long_forecasts` is NOT the lead for quarter (= quarter number) or season
   (= constant 1); it only equals the lead for month. Therefore the implementation must
   **derive** the lead for quarter/season (see decision 2) and write that derived value
   into the skill `horizon_value` — it must **never propagate** the stored
   quarter-number / constant-1. Accepted debt: `horizon_value` now means
   position-in-period in `long_forecasts` and lead in `skill_metrics`; document this
   overload at both write sites and in the service-request doc.
2. **(B2) Aggregated quarter/season lead → DERIVE single-valued lead = months from
   issue date to period start**, computed per issuance BEFORE aggregation. Approved.
   **Feasibility gate → PASSED (2026-06-29).** The API `LongForecastResponse`
   (`sapphire/services/postprocessing/app/schemas.py:48-102`) carries both the issue
   date `date` and the target-period start `valid_from` for all horizons. Derivation
   formula (matches the monthly definition):
   `lead_months = (valid_from.year - date.year)*12 + (valid_from.month - date.month)`.
   Caveat: for quarter/season the issue `date` is in the raw API response but is dropped
   by the `_QUARTERLY_FC_COLS` / `_SEASONAL_FC_COLS` projections
   (`data_reader.py:37-69`); `valid_from` survives. P2 must add `date` to both column
   lists before deriving. Month already has `horizon_value` = lead from config.
3. **(B5 + headline) Operational headline → SMALLEST LEAD "for now".** Per target
   period/code/model, the headline row is the smallest-lead issuance. Per-lead detail
   rows are still persisted (decision: per-lead for every lead). Still adopt the
   coverage / min-`n_pairs` gate (reuse `QUARTER_MIN_MONTHS=2`, `SEASON_MIN_COVERAGE=0.5`
   + an `n_pairs` floor for point metrics, since `sdivsigma`/`nse` are undefined at
   n=1) before persisting; surface `n_pairs` on every per-lead row. The smallest-lead
   headline should be chosen among leads that pass the gate.

Also in P0 (smaller): re-derive expected per-lead `n_pairs` from the **saved
post-filter recalc**, not the raw join (raw 21/93.5 is optimistic); decide whether
`date` joins the key (Kyrgyz 10th vs 25th issuances — verify whether
`(lead, period, code, model)` uniquely identifies an issuance); resolve the ownership
contradiction (CLAUDE.md says `apps/` is fair game; correct the issue file Status
lines 3-7/81); flipping the flag on a migrated DB **requires** a coordinated
dashboard/read-path follow-up (read path drops `horizon_value` at
`data_reader.py:3073` and groups by `[period,code,model]`) — flag stays OFF until that
ships.

## P0 spot-check results (kghm `postprocessing_db`, 2026-06-29, read-only)

Real models only (excluded `ENSEMBLE_MEAN`/`SKILLED_MEAN`/`NAIVE_MEAN`). Lead derived
`(yr(valid_from)-yr(date))*12 + (mon(valid_from)-mon(date))`. Quarter has 2 leads (0,1);
season has 4 leads (0–3); season_in_year ≡ 1 (Apr–Sep, one season/yr).

- **A (lead uniqueness) → key = lead-only, NO `date`.** 97% quarter / 94% season groups
  carry >1 issue date, but median distinct-dates ≈ median distinct-years → the pooling is
  across-year (intended). Adding `date` to the key would split each year into a singleton
  (`n_pairs=1` everywhere) — the opposite of the goal. Lead-only matches
  `forecast_skill_eval`. Intra-year re-issue pooling is the minority (12% quarter / 42%
  season) and acceptable for now.
- **B (sample size) → min-`n_pairs` gate justified, season is the binding case.**
  Forecast-year coverage per lead is healthy (median 20 quarter / 26 season), but
  obs-paired `n_pairs` in the existing `skill_metrics` is thin: season median **3–7 per
  lead**, up to **36% of groups ≤2**; splitting quarter by its 2 leads ~halves its median
  (15–17 → ~8). **Gate design (frozen):** under the flag, NaN-out variance-dependent
  metrics (`sdivsigma`, `nse`, anything using `std(ddof=1)`) when `n_pairs < 2`; always
  surface `n_pairs`; do **not** hard-drop rows (let the smallest-lead headline + dashboard
  suppress thin tiles). Coverage gates reuse `QUARTER_MIN_MONTHS=2`/`SEASON_MIN_COVERAGE=0.5`.
  A higher publishable floor (e.g. `n_pairs≥5`) is a dashboard-side choice, deferred.
- **Season "already split?" — RESOLVED (reconciled 2026-06-29).** No. Current code writes
  season skill with `horizon_in_year = season_in_year ≡ 1` (`aggregation.py:198`,
  `api_writer.py:487/635`), pooling all 4 leads. The DB's `horizon_in_year ∈ {0,1,2,3}`
  season rows are **legacy/stale** (prior version or migrator; current code only writes
  `1`, and the upsert key at `api_writer.py:592` lets them coexist without colliding).
  → Full scope stands: month + quarter + season all currently pool leads. The stale
  season rows are a **cleanup item** for the service-migration/ops step (not P2/P3).
- **Scope-narrowing to confirm in P2:** quarter/season skill forecasts appear to be read
  **direct from the API** (each row carries `date` + `valid_from`), so the lead is derived
  per-row — the B2 "aggregated-quarter lead" concern likely does not apply to the live
  path. P2 must confirm which path feeds `calculate_{quarterly,seasonal}_skill_metrics`
  and only touch `aggregation.py` if that path is actually used for skill.

## Revised phases

### P0 — Design + empirical pin-down (no production code; read-only DB query allowed)
Decisions 1–3 RESOLVED (above). Remaining P0 work: (a) **verify the issue date is
available** in the quarter/season forecast read path so the lead can be derived
(feasibility gate for decision 2); (b) define the exact lead-derivation formula by
replicating how the monthly path computes its lead; (c) date-in-key → **recommend NO** (key =
`[period, horizon_value, code, model_short]`, no `date`), for parity with
`forecast_skill_eval` which keys on lead only (`pairs.py:340`). Rationale: Kyrgyz 10th
(in-month, lead 0) vs 25th (prior-month, lead 1+) issuances differ in lead, so they do
not collide on a lead-only key; same-lead/different-date collisions are expected to be
rare. Confirm with a quick DB spot-check that `(horizon_value, period, code, model)`
uniquely identifies an issuance before freezing; (d) re-derive expected per-lead `n_pairs` from the saved
post-filter recalc; (e) confirm the service unique-key shape
`(horizon_type, code, model_type, date, horizon_in_year, horizon_value)`; (f) resolve
the ownership-Status contradiction in the issue file. Output: decision record folded
into the issue + a parity contract for P4. **Depends on:** none.

### P1 — Service-change request + draft migration (DOCS ONLY, parallel)
As before, plus: state the read-path/dashboard follow-up as an explicit precondition,
and reflect the P0 lead-column decision (`lead_months` vs `horizon_value`) in the
proposed model field + unique constraint. **No edits under `sapphire/services/`.**
**Depends on:** P0.

### P2 — Derive the lead + carry it through readers + aggregation (gated)
- **Quarter/season: DERIVE the lead** (months from issue date to period start) in the
  read/aggregation path — do NOT propagate the stored `horizon_value` (quarter-number /
  constant-1). Concretely: add `date` (issue date) to `_QUARTERLY_FC_COLS`
  (`data_reader.py:54-69`) and `_SEASONAL_FC_COLS` (`:37-52`) so it survives the
  projection (`valid_from` already does), then compute
  `horizon_value = (valid_from.year-date.year)*12 + (valid_from.month-date.month)`.
  Month: the existing `horizon_value` already equals the lead; carry it through
  unchanged. Write the derived lead into the `horizon_value` column used by the skill
  grouping.
- Enumerate and gate **all** lead-dropping chokepoints, not just `data_reader.py:3073`:
  `_QUARTERLY_FC_COLS` projection, **both** `drop_duplicates` subsets and **both**
  column projections in the two sibling quarter readers (~`:2678/:2686` and
  `:2819/:2825`).
- Restrict the lead-aware grouping to the **skill-pairing branch only** — do **not**
  change `aggregate_monthly_fc_to_quarterly`'s forecast-value output that feeds
  `long_forecasts` (written with `horizon_value=quarter` at `api_writer.py:1051`), or
  explicitly accept+document that quarterly forecast values change under the flag.
- Define `lead` for baseline/migrated rows (EM/Naive/Skilled-Mean have no issue date;
  migrated rows may be null). Decide drop-vs-sentinel; use `dropna=False` or a sentinel
  on per-lead groupbys.
- TDD: mixed-lead constituent test (leads {0,1,2}) asserting expected per-lead quarter
  row count and the keep/drop rule under `QUARTER_MIN_MONTHS`.
**Depends on:** P0.

### P2 — STATUS: DONE (verified 2026-06-29, worktree `agent-ae45b44a29b5253b8`, uncommitted)
`data_reader.py` (+59/-3) + new `tests/test_skill_lead_aware_reader.py` (16 tests). Flag-OFF
byte-identical (regression tests pin it); flag-ON derives `horizon_value` for quarter/season;
month untouched. `run_tests.sh postprocessing_forecasts` = 1385 passed, 0 skips; ruff clean.
Findings: **season** = direct-API only; **quarter** = mixed (direct API + monthly-aggregated).
Aggregated-only quarter rows have no `date` → `horizon_value = NaN` (lead underivable);
`aggregation.py` was NOT modified (correct — lead never enters value aggregation).

**Carry-over for P3 (from P2 review):**
- Flag-ON, quarter dedup now keeps both the direct (numeric lead) and aggregated (NaN lead)
  rows for a target that has both. **P3 must drop NaN-lead quarter rows before per-lead
  grouping** (use `dropna` on the lead in the groupby) so the NaN aggregated row doesn't
  pool with / double-count the direct row. Distinguish from baseline-model rows
  (EM/Naive/SM) that may carry a real `date` and thus a derivable lead — only genuinely
  date-less rows are NaN.
- Verify how EM/Naive/Skilled-Mean long-term forecast rows get their lead (do they carry a
  `date`?) so they are not silently dropped by the NaN filter.

### P3 — Skill grouping + writer (gated)
- Introduce a single `GROUP_COLS` constant; rewrite **every** unpack site
  (`:1208/:1313/:1447/:1630` and the `len(group_key)==3` guard at `:2168`) to be
  arity-agnostic (`dict(zip(group_cols, group_key))`). Add the lead to `crps_records`
  and to **all** `merge(on=...)` lists incl. Skilled-Mean weighting
  (`:1525/:1536/:1541/:1550`).
- Add `api_writer.py:592` to scope: when flag ON, `upsert_key` must include the lead;
  when OFF, unchanged. Add the lead to the `empty_stats` fallback schemas under the
  flag.
- Implement the min-n / coverage guard from P0 under the flag; surface `n_pairs`.
- Add an **API capability probe**: when flag ON, check the postprocessing OpenAPI /
  SkillMetric schema for the lead field and **hard-fail loudly** if absent, instead of
  emitting a colliding/422 payload (defends the `preprocessing_api_stale_worker_enum_422`
  failure mode). Treat the env flag as *intent*, the probe as the *gate*.
- TDD-first, named test files; assert EM, Skilled-Mean weighting, Naive-Mean, and CRPS
  keys are correct **per lead** for month, quarter, AND season — red before green.
**Depends on:** P0 (design), P2.

### P4 — Parity with `forecast_skill_eval` (reframed)
Assert the lead *definition* matches eval (`pairs.py:340`, `contingency.py:100`) **and**
that operational persistence adds the min-n guard + `n_pairs` exposure the diagnostic
eval app relies on its analyst to apply manually. Assert the numeric lead value
end-to-end, not just "grouping is stratified." **Depends on:** P3.

### P5 — Verify (reframed; DB persistence deferred)
- Quarter/season have **no CSV artifact** (`save_seasonal_skill_metrics` is API-only;
  quarterly saver calls the API directly) — drop the "CSV-path" criterion for them.
  Instead: unit test mocking the API client, asserting the per-lead records list carries
  the lead with correct values for month, quarter, AND season.
- **Flag-OFF golden snapshot**: capture current skill-write payloads + quarterly
  aggregation output on the parent commit; assert flag-OFF reproduces them byte-for-byte.
- Add a `lead_aware_on` conftest fixture; run every new behavioral assertion under
  **both** flag states; changed-line coverage on flag-ON branches (default-off leaves
  them dead otherwise — zero-skips won't catch an `if flag:` that never runs).
- Replace "no n_pairs=1 regression" with fixture-known expected per-lead counts + the
  coverage gate emitting/suppressing rows per rule.
- Document that the real-write 422 risk persists until the service migration lands.
**Depends on:** P4.

### P6 — Issue lifecycle
Correct the issue file's misleading "colleague-managed — coordinate" Status (lines 3-7,
81) and the "aggregated from monthly" season claim (lines 42-49, contradicted by
`data_reader.py:2696` reading direct from API); fold the locked decisions in; transition
draft → `review_gi_draft_pp_skill_lead_pooling.md`. **Depends on:** P5.

## Dependency graph (corrected)

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 0 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P0", "P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P4"], "parallel_agents": 0 },
    "P6": { "depends_on": ["P5"], "parallel_agents": 1 }
  }
}
```
P3 now depends on **P0** (design), not P1; P1 (docs) runs fully parallel to P2/P3.
```
