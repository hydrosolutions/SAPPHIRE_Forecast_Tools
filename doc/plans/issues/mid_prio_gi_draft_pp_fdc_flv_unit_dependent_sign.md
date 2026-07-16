# PP-044: `fdc_flv` sign depends on the magnitude/unit of observed discharge, not just model bias

> **ID note**: Originally filed as `PP-040` on branch `fix_pp_informational_metrics`; renumbered
> to `PP-044` at merge (trunk `PP-040` = ARIMA ModelType, `PP-041` = long_forecasts stale
> invalidation, `PP-042` = ensemble-exclusion form mismatch, `PP-043` = archived decadal skill
> starvation). No other references exist outside this file and `module_issues.md`.

**Status**: Draft
**Module**: postprocessing_forecasts (apps) + postprocessing service schema
(`sapphire/services/postprocessing`, **colleague-owned** — only if Option (b) below is chosen)
**Priority**: Medium
**Labels**: `postprocessing`, `skill-metrics`, `metric-design`, `needs-owner-decision`

---

## Implemented vs Deferred (read this first)

This issue is the deliberately-deferred follow-up to a fix already shipped on this branch:

- **DONE** (commit `6ca1537a`, "Fix fdc_flv docstring: sign is denominator-conditional, not
  bias-only"): the docstring of `fdc_flv()` was corrected to state precisely that the returned
  sign depends on `sign(sum(log(obs_low)))`, not only on the model's bias direction.
- **DONE** (same commit area, `apps/postprocessing_forecasts/tests/test_tier2_metrics.py:178-208`):
  a characterization test, `test_sign_flips_when_low_flows_are_below_1`, PINS the current
  behaviour (`obs` in `[0.01, 0.5]`, all low flows < 1.0) so a future refactor cannot silently
  change the sign without a failing test forcing a second look.
- **NOT DONE, and intentionally out of scope for that commit**: no arithmetic in `fdc_flv()`
  was changed. This was deliberate — every historical stored `flv` value would silently change
  meaning if the formula changed underneath it.
- **THIS ISSUE** is the deferred design decision: what, if anything, to do about the underlying
  metric-design defect that makes the fix above necessary in the first place.

---

## The defect

### Evidence (verified against the working tree on this branch)

`apps/postprocessing_forecasts/src/skill_metrics.py`, function `fdc_flv(obs, sim)` starting at
**line 629**:

```
689	    obs_low = obs_sorted[-k:]
690	    sim_low = sim_sorted[-k:]
691	    if np.any(obs_low <= 0) or np.any(sim_low <= 0):
692	        return np.nan
693	    log_obs_low = np.log(obs_low)
694	    log_sim_low = np.log(sim_low)
695	    sum_log_obs = np.sum(log_obs_low)
696	    if abs(sum_log_obs) < 1e-10:
697	        return np.nan
698	    return 100.0 * (np.sum(log_sim_low) - sum_log_obs) / sum_log_obs
```

(`obs_low`/`sim_low` are the bottom ~30% of flows — `k = max(int(np.floor(n * 0.3)), 1)`,
lines 685-690 — sorted from the descending-sorted arrays.)

The FLV is `100 * (sum(log(sim_low)) - sum(log(obs_low))) / sum(log(obs_low))`. The
**denominator, `sum(log(obs_low))`, is only positive when every low flow exceeds 1.0** in
whatever unit `obs` happens to be stored in. If the low flows are below 1.0, every term
`log(obs_low)` is negative, so the denominator is negative, and **the sign of the returned FLV
inverts relative to an otherwise-identical station whose low flows exceed 1.0** — even though
the model's bias direction (over- vs under-estimation) is unchanged.

This is already documented at length in the corrected docstring (lines 629-679 of the same
file), which this issue does not need to re-derive, but the load-bearing sentence is:

> "SIGN IS CONDITIONAL ON THE SIGN OF THE DENOMINATOR (sum(log(obs_low))), NOT ONLY ON THE
> MODEL'S BIAS DIRECTION — it is NOT simply 'positive = sim exceeds obs'."

### Persistence — confirmed

`flv` is a **persisted, cross-station-comparable** skill metric, not just an internal
intermediate value:

- **`apps/postprocessing_forecasts/src/api_writer.py`**, the nullable metric-column tuple built
  before the bulk API write, includes `"flv"` at **line 660** (within the `for col in (...)`
  block spanning roughly lines 649-661, alongside `sdivsigma`, `nse`, `delta`, `accuracy`, `mae`,
  `crps`, `pbias`, `kgelf`, `nse_log`, `fhv`).
- **`sapphire/services/postprocessing/app/schemas.py`**, `flv: float | None = None` is a field
  on `SkillMetricBase` at **line 181** (alongside `fhv` at line 180), which propagates to
  `SkillMetricCreate` and `SkillMetricResponse` per that file's inheritance chain.

Both claims verified directly against this branch's working tree; not inferred from memory.

### Consequence

Because `flv` is written to a shared DB table and read back for dashboards/reports that compare
or rank stations, **the sign of a stored metric currently depends on the scale/unit of the
observed discharge** (m³/s vs mm/d; a large river vs a small headwater catchment) — not solely
on whether the model over- or under-predicts low flows. A dashboard or report that treats `flv`
as cross-station comparable (e.g., "which stations have the worst low-flow bias, and in which
direction") is silently mixing two opposite sign conventions depending on each station's typical
low-flow magnitude.

### Numeric worked example (verified by direct computation, not hand arithmetic)

Both cases use the **identical model bias**: `sim = 1.5 × obs` (a uniform 50% over-prediction of
low flows). Only the observation *magnitude* differs.

**Case A — large river** (low flows well above 1.0, e.g. m³/s for a big basin):
`obs_low = [12.0, 15.0, 18.0]`, `sim_low = [18.0, 22.5, 27.0]`

```
sum(log(obs_low)) =  8.0833   (positive — every value > 1.0)
sum(log(sim_low)) =  9.2997
flv = 100 * (9.2997 - 8.0833) / 8.0833 = +15.05%
```

**Case B — headwater station** (low flows below 1.0, e.g. m³/s for a small basin — use station
code `19999` for any local test fixture, per repo convention):
`obs_low = [0.05, 0.08, 0.12]`, `sim_low = [0.075, 0.12, 0.18]`

```
sum(log(obs_low)) = -7.6417   (negative — every value < 1.0)
sum(log(sim_low)) = -6.4253
flv = 100 * (-6.4253 - (-7.6417)) / (-7.6417) = -15.92%
```

Same relative bias (`sim = 1.5 × obs`), **opposite reported sign** (+15.05% vs -15.92%), solely
because of the magnitude of the observed discharge. This generalizes algebraically: since
`sim = c · obs` implies `log(sim_low) = log(obs_low) + log(c)` termwise, the numerator reduces to
`n_low · log(c)`, which has a fixed sign determined only by `c` (here always positive, since
`c = 1.5 > 1`); only the **denominator**'s sign — determined purely by whether the low flows are
above or below 1.0 — flips the reported FLV sign. The docstring's "positive = overestimation"
framing therefore holds for Case A and is exactly backwards for Case B.

### Relationship to the canonical Yilmaz metric — verified against the docstring

The docstring (lines 663-679) already states, and this issue does not dispute:

> "This is a SIMPLIFIED variant inspired by Yilmaz et al. (2008) Eq. 4, not a literal
> implementation of it. The original formula subtracts a per-series minimum-of-segment anchor
> before summing (and carries a leading -1) ... This implementation omits that anchor, so it
> behaves as a plain log-magnitude bias ratio instead."

In other words, this implementation measures **log-magnitude bias** of the low-flow segment, not
the low-flow **FDC shape** (baseflow-recession curvature) that the anchored Yilmaz formula is
designed to capture and that is scale-invariant by construction. The omitted per-segment minimum
anchor is exactly the term that would make the metric invariant to a uniform multiplicative bias
and to the unit/magnitude of `obs`.

---

## Decision needed (owner + hydrologist sign-off — no code without it)

`flv` is written by `apps/postprocessing_forecasts` but stored via the **colleague-owned**
service schema (`sapphire/services/postprocessing/app/schemas.py:181`), and Option (b) below
would touch stored service data. Neither option should be implemented before the module owner
and a hydrologist agree on which one to take — this issue exists to present the choice, not to
resolve it.

### Option (a) — Rename/reframe: stop implying Yilmaz parity or cross-station comparability

Rename the function/metric (e.g. `flv` → `flv_log_bias`, or keep the column name but change its
description) and document explicitly that it is a **per-station, scale-dependent log-bias
indicator** — valid for tracking a single station's bias over time or across models at that
station, but **not meaningful to compare, rank, or aggregate across stations** of different
discharge magnitude.

- **Cost**: Low. No arithmetic change, no DB migration, no historical-value reinterpretation.
- **Concession**: The metric's name/description no longer implies the properties (scale
  invariance, Yilmaz Eq. 4 parity) that "FLV" conventionally suggests to a hydrologist reading a
  skill report. Anyone currently comparing `flv` across stations must be told to stop.

### Option (b) — Implement the true anchored Yilmaz Eq. 4 (scale-invariant)

Add the per-segment minimum-of-segment anchor and the leading `-1` from Yilmaz et al. (2008)
Eq. 4, making the metric invariant to a uniform multiplicative bias and to the unit/magnitude of
`obs`. This is the metric the current name and docstring's "inspired by ... Eq. 4" language
imply it should be.

- **Cost**: High. Every historical `flv` value already written to the postprocessing DB and
  shown on dashboards was computed with the old, non-anchored formula and has an unknown,
  scale-dependent relationship to what the anchored formula would produce for the same data — it
  cannot be recomputed from the stored value alone; it requires **re-running the metric against
  the underlying obs/sim series**. This is a coordinated data migration (recompute-and-overwrite
  or explicitly invalidate every historical `flv` row), not a drive-by code change.
- **Correctness**: Produces a metric that is actually scale-invariant and comparable across
  stations, matching both the metric's name and the literature it cites.
- **Coordination**: Requires the colleague who owns `sapphire/services/postprocessing` (schema
  is unaffected — no new column — but a bulk historical rewrite of a served table needs
  sign-off) and a hydrologist to confirm the anchored formula's headline sign convention (the
  docstring notes the canonical metric is commonly cited with the *opposite* headline sign:
  positive = underestimation) before any dashboard consumers are updated.

Both options leave the already-shipped docstring correction and characterization test
(`test_sign_flips_when_low_flows_are_below_1`) valid: Option (a) keeps them as-is (still describes
the shipped log-bias formula, just under a new name); Option (b) requires updating both once the
new arithmetic lands, and updating/retiring the characterization test to pin the new
(scale-invariant) behaviour instead.

---

## Acceptance Criteria

This issue is resolved when the owner has picked a direction; the criteria below describe what
"done" looks like for each — they are not both required.

**If Option (a) is chosen:**
- [ ] `fdc_flv` (or its persisted column) is renamed/relabeled to make clear it is a per-station
      log-magnitude bias indicator, not a scale-invariant FDC-shape metric.
- [ ] Docstring, `sapphire/services/postprocessing/app/schemas.py` field description (if the DB
      column is also renamed — coordinate with owner), and any dashboard/report label are
      updated consistently.
- [ ] Any existing consumer that compares/ranks `flv` across stations is flagged or updated to
      stop doing so, or documented as intentionally not cross-station-comparable.
- [ ] No arithmetic change; existing characterization tests continue to pass unmodified.

**If Option (b) is chosen:**
- [ ] `fdc_flv` implements the anchored Yilmaz et al. (2008) Eq. 4 formula (per-segment minimum
      anchor + leading `-1`), verified against a known worked example from the paper or a
      trusted reference implementation.
- [ ] The new formula is provably scale-invariant: the worked example in this issue (Case A vs
      Case B, same `sim = 1.5 × obs` bias) produces the **same sign** (and same magnitude,
      modulo the anchor term) for both cases.
- [ ] A coordinated migration plan for historical `flv` values is agreed with the service owner
      (recompute from stored obs/sim series, or explicitly invalidate/null out — decide which),
      with sign-off from a hydrologist on the resulting sign convention.
- [ ] `test_sign_convention_matches_docstring` and `test_sign_flips_when_low_flows_are_below_1`
      (`apps/postprocessing_forecasts/tests/test_tier2_metrics.py:164-208`) are updated to pin
      the new scale-invariant behaviour (both cases should now agree in sign for the same bias
      direction) — the old assertions describing the unit-dependent sign flip must be retired,
      not left dangling as an unexplained contradiction.
- [ ] Docstring updated to remove the "NOTE ON PROVENANCE" caveat once the implementation is no
      longer a simplified variant.

---

## Out of Scope

- Changing `fdc_flv`'s arithmetic without an owner decision (this issue exists specifically to
  get that decision before any code changes).
- `fdc_fhv` (`skill_metrics.py:599-626`, high-flow volume bias) — not audited here; it divides
  by `sum(obs_high)` (a raw, always-positive sum for positive discharge), not
  `sum(log(obs_high))`, so it does not share this specific denominator-sign defect. Worth a
  separate look if a similar pattern is suspected there, but not claimed or verified in this
  issue.
- Any dashboard-side display/formatting change for `flv` — deferred until the owner decision
  determines whether the column is renamed.

## Dependencies

- **Owner decision** between Option (a) and (b) — blocking; no implementation should start
  before this is made.
- **Option (b) only**: colleague coordination on `sapphire/services/postprocessing` (bulk
  historical data rewrite of a served table) and hydrologist sign-off on the resulting sign
  convention.

## References

- `apps/postprocessing_forecasts/src/skill_metrics.py:629-700` — `fdc_flv()`, including the
  already-corrected docstring (commit `6ca1537a`) and the unchanged arithmetic at lines 689-698.
- `apps/postprocessing_forecasts/src/api_writer.py:649-661` — nullable metric-column tuple
  including `"flv"` at line 660, built before the bulk skill-metric API write.
- `sapphire/services/postprocessing/app/schemas.py:180-181` — `fhv`/`flv` fields on
  `SkillMetricBase` (colleague-owned).
- `apps/postprocessing_forecasts/tests/test_tier2_metrics.py:164-208` —
  `test_sign_convention_matches_docstring` and `test_sign_flips_when_low_flows_are_below_1`
  (the already-shipped characterization tests this issue's Option (b) would need to retire).
- Yilmaz, K.K., Gupta, H.V. & Wagener, T. (2008). A process-based diagnostic approach to model
  evaluation. Water Resources Research 44, Eq. 4 (anchored, scale-invariant low-flow FDC volume
  metric — not what this codebase currently implements).
