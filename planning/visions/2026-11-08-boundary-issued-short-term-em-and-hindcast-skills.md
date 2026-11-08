# Boundary-issued short-term EM and hindcast skills

## 1. Outcome, scope and authority

Produce trustworthy pentadal and decadal Ensemble Mean (EM) forecasts and retrospective hindcast skills. Reconstruct as much supported history as possible for target periods from **2010 onward**, through an explicitly captured run cutoff. Use the active configured station cohort, accounting for virtual-station constituents and actual availability. Report gaps; do not promise complete history for every station or model.

This is retrospective selection and evaluation, **not independent operational or out-of-sample validation**. This vision states required behavior, not behavior already implemented on the target branch. Publishing the vision authorizes documentation only; implementation, deployment and operational data changes require their separate release.

Scope includes correcting current code that publishes wrong-window or incomplete **boundary-issued pentadal and decadal ML, NE and EM outputs**. Ignore unsuitable historical ML inputs; do not blanket-rewrite historical ML records. Other issue-date products are outside this work. Monthly, quarterly and seasonal ensemble algorithms and thresholds remain unchanged. No ML retraining or new uncertainty-calibration method is included.

Keep normal pipeline I/O API-backed. Checked historical archives may be explicit, bounded reconstruction inputs; they must not become a silent operational CSV fallback. Service code, schemas and API-contract changes require coordination under [CLAUDE.md](../../CLAUDE.md).

## 2. Calendar and input validity

| Horizon | Target periods within each month |
|---|---|
| Pentad | 1–5, 6–10, 11–15, 16–20, 21–25, 26–last day |
| Decad | 1–10, 11–20, 21–last day |

The issue date is **exactly one day before the target period starts**. Other ML issue dates must not contribute to EM. Bounds and year matching use the target period: the first target period of 2010 needs the 31 December 2009 issue if it exists. Missing source is a gap, not permission to invent it.

Use validated period-level forecasts where available. Where a correct period forecast is unavailable, derive it from validated daily predictions issued on that **same boundary date**:

- Clip daily targets to the exact calendar period before aggregation.
- Require the complete expected set of calendar dates. Distinct timestamps alone do not prove coverage: several timestamps on one date cannot replace a missing day.
- For a derived non-LR contributor, every required day must have a finite point and all four finite quantiles: `q05`, `q25`, `q75`, `q95`.
- Average the validated daily quantities over that period. Never fill missing predictions with observations, another issue date or out-of-period targets.
- Resolve identity and archive-version ambiguity explicitly before averaging. Duplicate copies and competing versions must not silently overweight a model or count as extra models. Unresolved inputs become reported gaps, not arbitrary mixtures. Do not introduce a new weighting algorithm or change the arithmetic for already-valid inputs.

A horizon label or zero/zero period indices establish neither correct nor incorrect numeric aggregation. Validate issue/target identity, quantity interpretation, target window and required coverage. Unknown-window period aggregates cannot qualify from labels alone. Conversely, missing historical run, commit or checkpoint attribution is a provenance limitation—not an automatic veto on otherwise validated daily inputs or a requirement for out-of-sample validity.

Virtual coverage must be established for the actual station/model/issue/period keys. A union of constituent codes across archives does not prove that the necessary inputs coexist. Validate existing virtual forecasts and derived coverage; do not hardcode a cohort size.

These protections apply to boundary-issued short-term ML, NE and EM outputs. NE keeps its existing averaging formula, although availability may decrease when invalid inputs are excluded. Do not alter unrelated issue-date products.

## 3. Component selection and EM assembly

First recalculate **component hindcast skills from the validated period inputs**. Select members using the recalculated NSE for the same station, horizon and calendar period: **NSE ≥ 0**. Zero passes; negative or undefined NSE does not. S/σ and accuracy must not impose additional membership gates. Do not blindly reuse cached skills calculated from questionable inputs.

The five-year reporting minimum below applies to EM skills only; this vision does not add a five-year component-selection gate.

For each boundary issue:

- LR needs a finite validated period point forecast. It contributes to the point mean only; do not invent LR quantiles.
- A non-LR contributor needs a finite point and all four finite quantiles to join either pool.
- Require at least **two distinct point-contributing models** and at least one eligible non-LR quantile contributor. Otherwise emit no EM forecast.
- Take the arithmetic mean of the eligible point contributions. Separately average each quantile over the eligible non-LR pool. Do not add skill weights.
- **NE must not contribute to EM**: its component ML models are already direct candidates.
- Allow composition to vary by historical issue. Record actual point contributors, including LR, and make the different quantile-contributor pool clear.
- Do not clamp the EM point into the quantile bands.

Bands remain **model-derived bands**, not automatically calibrated quantiles of a combined predictive distribution. LR-derived `delta` remains a separate reference quantity, not an EM error interval.

Retain the existing monotonicity guard after averaging quantiles: crossed averaged quantiles are adjusted to keep the bands ordered. This preserves the existing band convention; it is not new calibration. Do not clamp the EM point or introduce a new crossed-contributor rejection policy.

## 4. EM hindcast skill reporting

After reconstructing EM, score its actual historical series against matching valid, finite observed target-period means. For each station, horizon and calendar-period bucket, include the varying historical compositions in **one EM series**. Composition is provenance, not an axis that fragments the sample and later loses rows during persistence.

Validate forecast and observation identities before joining. Duplicate observations or forecast versions must not multiply pairs. Count distinct **target years** with both a valid EM forecast and a matching observation; missing observations do not count.

Report an EM skill metric only when:

1. At least **five distinct matched years** exist for that station and calendar period; and
2. That metric is mathematically defined for those pairs.

A five-year span with only two paired years does not qualify. Five duplicate rows from one year do not qualify. An undefined metric remains unavailable even with five or more matched years; never fabricate a zero.

An otherwise valid EM forecast and its model-derived bands may remain available without reportable skills. Show sample count and historical coverage, including insufficient-history cases, through supported presentation and audit paths. Distinct-year counting is a reporting requirement, not authorization for a new database field. Existing `n_pairs` may represent matched years only after proving one validated pair per period/year; do not assume that equivalence for legacy rows.

This is a current retrospective assessment. Do not manufacture historical as-of skill vintages or imply that the assessment was available at each historical issue date.

## 5. Bounded replacement, safety and verification

Before operational writes, declare the exact configured cohort, horizons, boundary issue-date/target bounds and cutoff. Separately declare the **skill reporting vintages and calendar-period buckets** subject to replacement: skill-record dates are not forecast issue dates. Do not translate forecast history bounds into blanket historical metric deletion.

Within the declared scope:

- Replace valid EM forecasts and withdraw obsolete EM forecasts that no longer have valid inputs or sufficient contributors.
- Replace reportable EM skills and withdraw insufficient-history or otherwise unavailable EM skills.
- Withdrawal means obsolete results are no longer returned or displayed as valid. Choose a verified native mechanism; do not invent a service flag or deletion contract.
- Prevent stale results from returning through CSV/cache fallback **or through an older stored skill vintage**, including when an API response is nonempty.

Protect unrelated stations, horizons, non-boundary records and longer-term products. This does not authorize blanket ML/LR deletion or historical ML bulk repair. Current boundary-writing defects must be corrected without silently expanding historical write scope.

Use before-state → authorized operations → after-state → exact audit, **without backups or dumps**. Distinguish confirmed empty reads from failed or incomplete reads. Stop on failures; do not fall back to stale data, silently retry, roll back automatically or report partial writes as success. If a write partially succeeds, stop further work and report the incomplete state.

Reconcile intended writes, acknowledgements and readback. Verify complete native identities and values, absence of obsolete in-scope results, and unchanged protected records—not just counts. Forecast identity includes station, model, horizon, issue date and target; skill identity uses its own reporting-date and period/lead fields. Verify both against the actual service contract before writing. Keep sensitive operational evidence private.

### Implementation evidence to inspect

Paths below are repository-relative; inspect the current checkout rather than assuming a private prototype is present.

| Source | Relevant hazard or contract |
|---|---|
| `apps/postprocessing_forecasts/src/data_reader.py` | `_apply_day_coverage_filter` currently permits half-calendar coverage; timestamp counts are insufficient. Archive cutover is a nested helper inside `_read_ml_forecasts_pp_api`, not proof of per-key validity. Inspect normalization and empty/error fallback paths. |
| `apps/postprocessing_forecasts/src/ensemble_calculator.py` and `apps/postprocessing_forecasts/src/skill_metrics.py` | Inspect short-term membership, LR handling, quantile postprocessing and composition grouping. Existing generic EM scoring can split by composition; the required varying-composition assessment must not be lost during saving. |
| `apps/postprocessing_forecasts/recalculate_skill_metrics.py`, `apps/postprocessing_forecasts/backfill_period_forecasts.py`, `apps/postprocessing_forecasts/src/file_writer.py`, `apps/postprocessing_forecasts/src/api_writer.py` | `_run_short_term_recalc` and generic backfill also write peer forecasts; they are not safe EM-only runners as-is. Latest-per-period selection can discard historical issues. Verify serialization, full keys, acknowledgement handling and readback instead of prescribing unchanged writer reuse. |
| `apps/forecast_dashboard/src/db.py` | Inspect latest-vintage selection versus invalid/tombstone filtering; filtering invalid rows first can expose an older skill. Verify absence through actual reader/display paths without changing long-term behavior. |
| `apps/iEasyHydroForecast/setup_library.py` | Inspect virtual per-key availability and NE arithmetic; preserve the NE formula. |
| `apps/machine_learning/reaggregate_day_to_periods.py` | The utility averages all DAY targets, rounds to three decimals and writes zero period indices. This is a known unsafe mechanism, not proof of every stored row's ancestry. Trace the actual current period-writing path; the current ML writer itself writes DAY forecasts. |
| `sapphire/services/postprocessing/app/models.py` | Read-only verification of forecast/skill identities and enum mapping. API value `EM` and enum member `ENSEMBLE_MEAN` are distinct representations. Coordinate any service changes. |

### Acceptance evidence

| Case | Required result |
|---|---|
| February and month ends | Pentad issued 25 February covers 26–28 or 26–29; decad issued 20 February covers 21–28 or 21–29. A decad covering 21–31 needs all eleven days. |
| Year boundary | Available 31 December 2009 inputs can serve the first 2010 target period; issue-year filtering must not discard them. |
| Coverage and identity | Reject a missing day, duplicate timestamps masquerading as complete coverage, ambiguous payloads and other-issue substitution. Ignore targets outside the period. |
| Membership and arithmetic | NSE zero passes; negative/undefined fails; disabled metrics do not gate. LR affects points only; NE is excluded; minimum composition and unweighted means hold. |
| Varying composition and skills | Score the actual EM series across compositions. Four matched years give no skill; five allow only defined metrics. Duplicates and missing observations cannot inflate years. |
| Publication and withdrawal | Forecasts/bands can remain without skills. Counts/coverage remain truthful. Neither empty-response fallback nor an older stored vintage resurrects withdrawn results. |
| Failures and protected scope | Read failures, dropped intended payloads, partial acknowledgements and readback mismatches stop the run. Verify protected records and long-term behavior remain unchanged. |

Follow [CLAUDE.md](../../CLAUDE.md), the [testing workflow](../../doc/dev/testing_workflow.md) and [independent-review workflow](../../doc/dev/agent_review_workflow.md). Use the repository's native `run_tests.sh` gates and independent review; this document is not evidence that those implementation requirements have passed.
