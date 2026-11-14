# PP-072 — EM (short-term) = Skilled Mean method; long-term = Naive + Skilled Mean only

**Status:** Implementation reviewed and verified, including the final member-write fix. The owner explicitly accepted the 12 existing skips and two expected failures on 2026-11-14 for this publication; deployment is pending.
owner decisions recorded below (equal weights, no LR q05/q95). **ID:** PP-072 (origin's highest is PP-066; PP-067–071 are claimed by unpushed local issue files). **Module:** `apps/postprocessing_forecasts`. No `sapphire/services/` change
needed (EM stays a valid enum, `sapphire/services/postprocessing/app/models.py:38`).

## Relation to existing issues (read first)

- **Supersedes** `mid_prio_gi_draft_pp_remove_monthly_em.md` (PP-059) and
  `low_prio_gi_draft_pp_vestigial_long_term_em.md` (PP-058) for the long-term part. Inherit their
  **Surface C (gap-detector trap)** and **stale-tombstone** sections; do not re-derive them.
- **Reverses** their explicit statements that short-term EM "NSE ≥ 0.8 gate must stay"
  (PP-059:158, PP-058:118). The current code is NSE ≥ 0 since `740ee9bb`; this plan moves it to the
  Skilled-Mean rule below. PP-059 is sequenced after PP-057 as an evidence dependency — confirm that
  ordering still holds.

## Decision (owner, 2026-11-11)

- **Short-term (pentad/decad): "EM"** is computed the same way as the long-term Skilled Mean.
- **Long-term: Naive Mean + Skilled Mean only.** No long-term EM (quarterly already has none, PP-065 P1c).

Short-term EM takes the **Skilled Mean pool** but stays an **equal-weight** mean (maintenance mode: no
1/MAE weights, no `eps`, no new weighting code):
1. **Pool:** short-term NSE gate (D6; default `1e-9`, inclusive, i.e. "> 0"; sdivsigma/accuracy off by default) **and** `n_pairs >= K`.
2. **Mean:** unweighted, as today (`740ee9bb`).
3. **K:** `ieasyhydroforecast_min_pairs_short_term`, default 4 (as monthly); gates configured per D6.
4. **LR bands:** LR contributes q50 = point and q25/q75 = point ∓ delta. **No q05/q95 for LR** (nothing
   uses them); q05/q95 come from the non-LR members, as today. The ± delta band is the hydromet
   convention (a forecast within ± delta counts as good); it is a tolerance band, **not** a calibrated
   prediction interval.

## Current behaviour (verified by reading, 2026-11-11)

| Horizon | EM today | Skilled Mean today |
|---|---|---|
| Short-term | unweighted mean; finite NSE ≥ 0; no `n_pairs` gate (`ensemble_calculator.py:159-165`) | none |
| Monthly | unweighted mean; default env thresholds + `n_pairs ≥ 4` (`:394-411`) | NSE>0 override, 1/MAE |
| Quarterly | none (`_skip_em_for_quarter`) | NSE>0 override, `n_pairs ≥ 10`, 1/MAE |
| Seasonal | LR_BASE+LR_SM mean, no skill gate (`:844-863`) | NSE>0 override, `n_pairs ≥ 5`, 1/MAE |

K defaults: `skill_metrics.py:202-205`. 55 test files match `\bEM\b`.

## Verified facts that shape the design

- **delta** (`skill_stats.delta`) is meant to be the hydromet delta of the observations: it should depend only
  on (code, horizon type, period) and be identical for all models. **Live data contradicts this** (differs
  between models; bug filed as PP-074), so EM takes the delta from the
  **LR skill row only** (latest `date` if several); NaN when no row has obs, sim and delta all finite
  (`skill_metrics.py:~418-480`) → LR dropped. It reaches `create_ensemble_forecasts` on both the operational
  (`postprocessing_operational.py:169`) and the maintenance path. So delta availability is closed;
  missing-delta behaviour is decided in D2.
- **`n_pairs`** is effectively years of valid pairs per (code, period, model). Short
  histories may produce undefined NSE and are excluded by the finite check. The minimum
  history gate also excludes otherwise finite skills with insufficient matched years.
- **Gap detector:** `postprocessing_maintenance_long_term.py` lines 136, 212, 664 and ~707 pass
  `ensemble_models={"EM","Skilled Mean","Naive Mean"}`; `gap_detector.py:~117-135` flags every missing
  (date, code) per model. Removing EM only from the builders makes every monthly/seasonal pair a
  permanent EM gap.
- **Recalc tombstones (from PP-059, not re-read):** stale-key diffing writes `n_pairs=0` null rows for
  monthly/season EM skill keys that stop being emitted.
- **Production never runs the legacy EM skill block** (`skill_metrics.py:~2201-2330`):
  `recalculate_skill_metrics.py:224` passes `exclude_models=["EM"]`.

> **Status 2026-11-13:** P1–P4 are implemented. The sections "Verified facts" and "Current behaviour" below describe
> the state BEFORE the change and contain stale line numbers; the long-term EM and the legacy EM skill block no longer
> exist. Short-term EM skill, one shared delta and the dashboard ranges are tracked in PP-075.

## Owner decisions (2026-11-11/12)

- **D1:** supersede PP-058/PP-059; their "short-term NSE ≥ 0.8 must stay" is reversed.
- **D2:** LR with NaN delta is dropped from EM (point and bands); LR q50 = point; LR q25/q75 = point ∓ delta;
  no LR q05/q95. Non-LR members must still supply all four finite bands (`740ee9bb`).
- **D3:** **equal weights** for short-term EM (supersedes the earlier per-group-eps choice; no weighting code).
- **D4:** keep old monthly/season EM skill rows — exclude EM from the recalc stale-key diff; verify the
  mechanism in `recalculate_skill_metrics.py` first.
- **D5:** K = same as monthly (4), new short-term env var.
- **D6 — Separate gate configuration (2026-11-12):** short-term EM has its **own** env vars, independent of the
  long-term ones: `ieasyhydroforecast_nse_threshold_short_term` (default `1e-9`, may not be disabled),
  `ieasyhydroforecast_efficiency_threshold_short_term` (default `False`),
  `ieasyhydroforecast_accuracy_threshold_short_term` (default `False`) and
  `ieasyhydroforecast_min_pairs_short_term` (default 4). Long-term `*_long_term` vars are untouched.
  Same parser/disable-token rules as `_parse_threshold_env`.

- **D7 — Dashboard shows old long-term EM (2026-11-12):** after P3 the dashboard (monthly: `db.py:769-816`;
  season: `db.py:1134-1215`, latest row per model) keeps showing the last stored monthly/season EM row until it
  is superseded; only quarter filters EM (`db.py:1025-1027`). **Accepted for now.** A frontend EM filter is a
  separate task to discuss with Maxat; the dashboard may keep showing the last stored EM row until superseded (operators should not read it as a current forecast). No `db.py` edit in P3.
- **D8 — D4 scope (from the P0 inventory):** the stale-tombstone exemption covers `month_in_year` and
  `season_in_year` only, matched on the canonical key `ENSEMBLE_MEAN` (not `"EM"`); quarter EM rows keep being
  tombstoned (existing test `test_pp065_p1c_quarter_ensemble_skill.py` requires it). Implemented in
  `src/stale_tombstones.py::build_stale_tombstones`, before the `emitted.empty` branch.
- **D9 — short-term gate env precedence (owner, backwards compatibility):** per metric, the raw value is the
  `ieasyhydroforecast_*_threshold_short_term` var if set (empty string = unset), else the existing un-suffixed
  var (`ieasyhydroforecast_nse_threshold` / `_efficiency_threshold` / `_accuracy_threshold`), else the default
  (NSE 1e-9, others off). `min_pairs_short_term` has no fallback (default 4). Errors name the var that supplied the
  value. NSE cannot be disabled: an explicit `_short_term` disable token raises; a disable token from the
  un-suffixed fallback logs a warning and uses the default epsilon (a crash is worse than a default).
  Rationale: existing deployed un-suffixed threshold values must keep governing
  short-term EM when no explicit short-term override is set. Supersedes the earlier
  statement that the un-suffixed vars affect no output. Inclusive semantics are unchanged.

## Documentation notes (required in P4; no code)

- Skill (NSE, MAE, `n_pairs`) used to select EM members is computed on the same history; it is
  **in-sample**, so EM/Skilled Mean skill is optimistic. Leave-one-year-out evaluation is out of scope
  (maintenance mode).
- NSE > 0 means "better than the climatological mean"; at pentad/decad lead it is a weak filter. Operators
  can tighten it through the NSE env var (e.g. 0.8).
- LR bands are ± delta tolerance bands, not prediction intervals.
- With K = 4 each NSE/MAE rests on about 4 yearly pairs; models with ~1 pair (new ML models) are excluded.

## Phases (each phase carries its own tests; phases that share files run serially)

### P0 — Read-only inventory and measurement
- Consumers of long-term EM (dashboard incl. `forecast_dashboard/src/db.py:1027`, which filters EM for
  quarter only — verify seasonal display; bulletin; `api_writer.py`; `data_reader.py`).
- **Before/after EM row counts** for the candidate rule (NSE > 0, K ∈ {2,3,4}) on a dev DB or replay,
  per horizon, including TFT/TSMixer coverage.
- Env values per deployment; conflicts with PR #538 boundary-EM contracts and `740ee9bb`.
- One line to log (out of scope): dashboard summary table (`processing.calculate_forecast_range`) ignores
  Q25/Q75 for pentad/decad.
- **Acceptance:** written inventory + counts; confirm the D4 mechanism and the D5 drop-count is acceptable.

### P1 — Short-term EM: pool and LR delta bands
- **Files:** `src/ensemble_calculator.py` (`create_ensemble_forecasts`), `src/skill_metrics.py` (K env helper).
- Replace the hard-coded NSE ≥ 0 / `min_pairs=None` gate; drop LR rows with non-finite delta; set LR q50 =
  point and q25/q75 = point ∓ delta (q05/q95 stay NaN for LR). Keep the `740ee9bb` guards (min 2 distinct
  models, non-LR four-band rule, NE/EM never candidates, monotonicity guard).
- **Acceptance:** hand-computed synthetic example (LR + one ML model); LR-with-NaN-delta dropped; K env
  parsing error names the variable.

### P2 — Delete the legacy EM skill block
- `skill_metrics.py:~2201-2330`: delete (or prove unreachable) rather than align. Update the two tests that
  call `calculate_skill_metrics` without `exclude_models`
  (`test_edge_cases.py:743,804`, `test_integration_postprocessing.py:2669-2791`).
- **Depends on:** P1.

### P3 — Remove long-term EM
- **Files:** `src/ensemble_calculator.py` (monthly and `_create_aggregated_ensemble_forecasts`),
  `src/skill_metrics.py` (monthly EM skill; `:2746` shared aggregated call), `src/gap_detector.py` callers
  and **`postprocessing_maintenance_long_term.py` lines 136, 212, 664, ~707 (remove "EM" from
  `ensemble_models`)**, `_skip_em_for_quarter` and its call sites, stale comment `:455`.
- **Depends on:** P1, P2 (shared files; do not run in parallel).
- **Acceptance:** monthly/season outputs = Naive Mean + Skilled Mean; **no EM gap reported** by the gap
  detector on a replay; old monthly/season EM skill rows preserved (D4), no tombstones written.

### P4 — Docs
- `apps/postprocessing_forecasts/README.md` and `doc/user_guide.md` (sections from `22205d0c`),
  `doc/plans/module_issues.md` (PP-058, PP-059), `doc/data_flow_*.md`, PP-065 text ("matches monthly").

### P5 — Verification and rollout
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh` zero fail/zero unexpected skips; `apps/run_locally.sh all`;
  `apps/run_docker_tests.sh --skip-ml`. Independent out-of-loop review on the final diff.
- **Merge = deploy:** short-term EM numbers change; monthly/seasonal EM disappear. Announce first.

#### Rollout notes (2026-11-12)

**What changes at deploy**
- Pentad/decad EM: members need finite NSE > 0 (or the deployed threshold, see below) and at least 4 yearly pairs
  (`ieasyhydroforecast_min_pairs_short_term`, default 4); equal-weight mean; the LR forecast contributes median = point
  and q25/q75 = point ∓ delta (the LR skill row's delta, PP-074); no LR q05/q95.
- Monthly and seasonal: no new EM (Naive Mean + Skilled Mean only; quarterly EM was removed in PP-065). Old EM forecast
  and skill rows stay in the database; the dashboard may keep showing the last stored monthly/season EM (D7, accepted).

**Env vars (D9, backward compatible)**
- Each gate reads `ieasyhydroforecast_{nse,efficiency,accuracy}_threshold_short_term`; if unset it falls back to the
  existing `ieasyhydroforecast_{nse,efficiency,accuracy}_threshold`; if neither is set the defaults apply (NSE > 0,
  other gates off). `..._min_pairs_short_term` has no old equivalent.
- A server that sets only the old variables keeps its thresholds, plus the new minimum
  history requirement. Replay confirmed that these settings affect EM eligibility.
- To relax EM on a server, set `..._efficiency_threshold_short_term=False` and
  `..._accuracy_threshold_short_term=False`. This is a per-server deployment decision.

**Before deploying to another server**
- Check, with a maintenance run or the replay, that EM is still produced for most stations under that server's env
  values. If a deployment ends up with no EM at all, `validate_pipeline` fails on "Forecasts (EM, pentad|decad)".
- After deploy, run `maintenance:postprocessing_forecasts` once so existing short-term EM is recomputed. It does not
  remove old EM rows that no longer qualify (PP-073) and re-saves old monthly EM rows on long-term runs (PP-073).

**Operators:** the long-term "EM = mean(LR_BASE, LR_SM)" checks in the deploy runbooks no longer apply (marked obsolete);
no replacement gate is defined here.

**Current publication validation:** independent follow-up review cleared the narrow
short-term member-write fix. Final full-suite results are 8,848 passes, 12 existing skips,
two existing expected failures and zero failures. The corrected recalculation, final
postprocessing Docker build/smoke and shared-delta/four-year-rule API readback passed.
Earlier operational and maintenance evidence predates the recalculation-only fix;
the documented aggregate validator-versus-schedule mismatch remains explicit. See
PP-075's Verification section for exact scope. The owner explicitly accepted the 12 existing skips and two expected failures on
2026-11-14 for this publication. Global repository policy is unchanged; frontend work
is deferred and deployment is pending.

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P2"], "parallel_agents": 1 },
    "P4": { "depends_on": ["P3"], "parallel_agents": 1 },
    "P5": { "depends_on": ["P4"], "parallel_agents": 1 }
  }
}
```
