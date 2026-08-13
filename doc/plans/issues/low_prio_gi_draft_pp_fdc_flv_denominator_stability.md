# PP-052: `fdc_flv` denominator guard catches exact degeneracy but not near-zero instability

**Status**: Draft
**Module**: postprocessing_forecasts (apps)
**Priority**: Low
**Labels**: `postprocessing`, `skill-metrics`, `numerical-stability`

---

## Summary

`fdc_flv()` (`apps/postprocessing_forecasts/src/skill_metrics.py:629`, arithmetic at line 700)
divides by `sum_log_obs = np.sum(np.log(obs_low))` — the sum of the natural log of the bottom
30% of observed flows. The only guard on this denominator is exact-degeneracy:

```
697    if abs(sum_log_obs) < 1e-10:
698        return np.nan
```

This catches the case where the sum is *exactly* zero (or numerically indistinguishable from
it), but not the case where it is *small relative to the spread of its own terms*. A station
whose bottom-30% flows cluster just above 1.0, in whatever unit the deployment stores discharge
in, has `log(obs_low) ≈ 0` termwise, so `sum_log_obs` can land at, say, `1e-3` — comfortably past
the `1e-10` guard, but still tiny enough that dividing by it inflates `|flv|` arbitrarily. The
function returns a finite number, and that number is persisted and displayed like any other.

## Same root cause as PP-044, different symptom

PP-044 (`doc/plans/issues/mid_prio_gi_draft_pp_fdc_flv_unit_dependent_sign.md`) already documents
that `sum_log_obs`'s **sign** depends on whether the bottom-30% flows are above or below 1.0 —
an artifact of observation magnitude/units, not model bias direction. This issue is the same
unanchored-denominator property showing up as **magnitude blow-up** instead of **sign
inversion**: both are consequences of dividing by `sum(log(obs_low))` with no anchor term (see
the docstring's `NOTE ON PROVENANCE`, lines 661-674, on why this simplified variant lacks the
per-segment minimum anchor that would make it scale-invariant). Whoever picks up either issue
should read the other — they are one property, not two coincidences.

## Impact

- **Registry classification**: `DAILY_METRIC_REGISTRY["flv"]` (`skill_metrics.py:95`) sets
  `min_points: 10`, `higher_is_better: None` — informational, "closer to 0 is better". It is in
  no skilled-forecast gate (checked: not referenced by any gating logic), so there is **no
  gating exposure** — an inflated value cannot exclude a model from an ensemble or flip a
  pass/fail check.
- **Persistence**: written via `api_writer.py:661` (`"flv"` in the nullable metric-column tuple)
  and `file_writer.py:595` (Tier 2 FDC metrics, API-only, no CSV fallback for this path).
- **Display**: `apps/forecast_dashboard/src/vizualization.py:4649` includes `flv` in the
  displayed column list, `:4679` labels it `FLV`. Because the registry semantics are "closer to
  0 is better," an inflated `|flv|` reads on the dashboard as a poorly-performing model at that
  station — when it is really a denominator artifact of the station's observation scale, not a
  model property.
- **Correctness of forecasts/skill gates**: none. This is a display/reporting quality issue on
  an informational metric only.

## Hard constraint — do not change sign, anchor, or arithmetic

Per PP-044 and the function's own docstring `NOTE ON PROVENANCE` (`skill_metrics.py:661-674`):
`fdc_flv` is a deliberately SIMPLIFIED variant of Yilmaz et al. (2008) Eq. 4 — it omits the
per-series minimum-of-segment anchor and the leading negative sign, and its values are NOT
comparable to published FLV. The docstring explicitly states the sign/arithmetic must not be
"corrected" without a coordinated migration of historically stored `flv` values. Two
characterization tests pin current behavior and must continue to pass **unchanged** by any fix
here: `test_sign_convention_matches_docstring`
(`apps/postprocessing_forecasts/tests/test_tier2_metrics.py:164`) and
`test_sign_flips_when_low_flows_are_below_1` (`:178`).

A fix for this issue must **not** touch the sign, the anchor term, or the core arithmetic
(`100.0 * (np.sum(log_sim_low) - sum_log_obs) / sum_log_obs`). It may only change *when* the
function returns `NaN` instead of a value.

**Even a guard-only change has a consequence worth flagging**: any change to the NaN condition
alters which stations show a value at all — a currently-displayed (if inflated) `flv` at some
station would become blank instead. That's a smaller, more contained change than touching the
arithmetic, but it is not free, and should be called out to whoever reviews the fix.

## Options (not a decision — for the owner to choose)

1. **Relative-magnitude stability guard**: return `NaN` when `|sum_log_obs|` is small *relative
   to the spread* of `log(obs_low)` (e.g. relative to `std(log(obs_low))` or the range of the
   terms), rather than against the fixed absolute `1e-10`. This targets the actual failure mode
   (near-cancellation of terms with mixed-ish or near-zero individual logs) instead of only
   exact zero.
2. **Surface the instability at the display/documentation layer**: leave the arithmetic and
   guard as-is, but flag in the dashboard or docs that `flv` values near a denominator-instability
   threshold should be read with caution — no code change to `fdc_flv` itself.
3. **Accept and document**: note the residual in the function's docstring/provenance note as a
   known limitation of the simplified variant, take no further action.

## Verification a fix would need

- Both existing characterization tests
  (`test_sign_convention_matches_docstring`, `test_sign_flips_when_low_flows_are_below_1`) must
  continue to pass **unmodified** — they exercise `obs` ranges (`[1, 50]` and `[0.01, 0.5]`) far
  from the near-zero-denominator regime, so they should be unaffected by a guard-only change;
  confirm this rather than assume it.
- A new test fixture with bottom-30% flows clustered close to 1.0 (e.g. `[0.9, 1.0, 1.1]`-scale)
  that currently returns a large finite `|flv|` and, after Option 1, returns `NaN`.
- Confirm the existing `test_exactly_10_points` boundary test (`obs = sim`, `flv == 0` exactly)
  still passes — a relative-spread guard must not treat a legitimately-zero numerator/denominator
  pair (identical obs/sim) as unstable.
- No change to `DAILY_METRIC_REGISTRY["flv"]` gating semantics is implied or required by this
  issue.

## Out of Scope

- Anchoring the formula to canonical Yilmaz Eq. 4, renaming the metric, or any sign change — all
  covered by PP-044's Option (a)/(b) decision, not this issue.
- `fdc_fhv` (`skill_metrics.py:600`) — divides by `sum(obs_high)` (always positive for positive
  discharge, no log transform), so it does not share this denominator-instability pattern.

## References

- `apps/postprocessing_forecasts/src/skill_metrics.py:629-700` — `fdc_flv()`.
- `apps/postprocessing_forecasts/src/skill_metrics.py:90-98` — `DAILY_METRIC_REGISTRY`.
- `apps/postprocessing_forecasts/src/api_writer.py:661` — `flv` in the persisted metric-column
  tuple.
- `apps/postprocessing_forecasts/src/file_writer.py:595` — Tier 2 FDC metrics, API-only write.
- `apps/forecast_dashboard/src/vizualization.py:4649,4679` — dashboard display column/label.
- `apps/postprocessing_forecasts/tests/test_tier2_metrics.py:164,178` — characterization tests
  that must continue to pass unchanged.
- `doc/plans/issues/mid_prio_gi_draft_pp_fdc_flv_unit_dependent_sign.md` (PP-044) — sibling
  issue on the same unanchored-denominator root cause, sign-inversion symptom.
