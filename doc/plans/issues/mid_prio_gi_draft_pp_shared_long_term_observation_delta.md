# Shared long-term observation delta — vision and draft plan

**Status:** Draft — statistical requirements agreed; ready for bounded E0 wiring/compatibility check,
not yet for E1 delegation. Seasonal record mapping and historical-display semantics remain to settle.
**Modules:** `apps/preprocessing_runoff`, `apps/postprocessing_forecasts`, `apps/forecast_dashboard`.
**Priority:** Medium (proposed). **Related:** [PP-075](mid_prio_gi_draft_pp_short_term_em_skill_and_delta_ranges.md), PP-074, FD-023.
**Evidence baseline:** `origin/maxat_sapphire_2` at `4c8899f6`, inspected 2026-11-14. No live payload or deployment verification.

**Maintenance-mode constraint (owner):** implement only what is necessary for shared long-term dashboard ranges.
Prefer existing API fields and a narrowly scoped producer/reader change. No major refactoring, general aggregation
cleanup, new model or all-horizon migration. Defer optional metadata/versioning and short-term/backend harmonization;
expand the service contract only if the agreed minimum behavior cannot use existing fields safely.

## Working vision

Publish one observation-derived tolerance for a station and target period, independently of whether any forecast
model has usable skills. For the same observation reference history, every model and forecast lead targeting that
period receives the same delta. The dashboard uses it for Delta and Mixed ranges without manufacturing missing bounds.
Delta is an observation-derived hydromet tolerance, not a calibrated predictive interval.

The owner requested planning after considering a simple runoff regression as a storage carrier. The recommended
direction is model-independent statistics; a regression may be useful as a forecast baseline, but is unnecessary
for computing observation variability and is outside this draft's proposed scope.

## Implemented behavior and the gap

- `apps/postprocessing_forecasts/src/data_reader.py::_aggregate_daily_to_monthly` already computes
  `delta = 0.674 * sample_std` across accepted yearly monthly means, grouped by station/calendar month.
- `apps/postprocessing_forecasts/src/aggregation.py::aggregate_monthly_obs_to_quarterly` and
  `aggregate_monthly_obs_to_seasonal` compute the same statistic across yearly quarter/season means.
  Compute variability of the complete target-period series; averaging monthly standard deviations is not equivalent.
- These paths replace undefined standard deviation with zero. A singleton history therefore resembles genuine zero
  variability. The new publication contract must distinguish these; any change to historical accuracy needs its own decision.
- `apps/forecast_dashboard/src/db.py` currently obtains long-term delta through model-specific skill joins.
  Availability is coupled to skill records, rather than an authoritative observation-statistics read.
- `sapphire/services/preprocessing/app/{models,schemas}.py` already exposes nullable `Hydrograph.std` and `count`.
  Its unique identity is `(horizon_type, code, date)`; it has no reference-window/version or forecast-lead field.
- `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` writes long-term norms/current/previous values;
  it does not currently produce the multiyear standard deviations and counts needed here.
- `sapphire/services/preprocessing/app/crud.py::create_hydrograph` dumps defaults and replaces every supplied model
  field on upsert. A statistics-only write can erase actuals/norms; existing actuals writes can erase statistics.

## Decisions to consolidate with the owner

The first interview round is recorded below; remaining questions are unresolved. Recommendations are not decisions.

| Question | Options / decision needed |
|---|---|
| Intended use — decided | Ranges first; preserve historical accuracy calculations (owner, 2026-11-14). |
| Reference history — decided | 2000–2025 inclusive, configurable per deployment (owner, 2026-11-14). Keep this range reference independent of skill-scoring configuration. |
| Initial horizons — decided | Month/quarter/season and dashboard first; short-term and backend harmonization is a required follow-up, not discarded scope (owner, 2026-11-14). |
| Accepted observations — decided | Retain existing postprocessing rules: month requires at least 50% daily observations; quarter requires all three valid monthly means; season requires at least 50% of its configured monthly means (owner, 2026-11-14). Preserve the existing period aggregation arithmetic; do not substitute rounded hydrograph actuals. |
| Minimum history — decided | At least 50% of potential years in the configured reference period must have a valid target-period observation (owner, 2026-11-14). For 2000–2025 inclusive: 13 of 26 distinct years. Use `ceil(0.5 * (end_year - start_year + 1))`, with at least two observations required for sample std. Below threshold: unavailable. |
| Cutoff and refresh | Engineering default: reuse existing yearly/manual hydrograph maintenance to recompute the fixed reference period, including corrections. Do not add a scheduler. E0 verifies the entrypoint and completed-period cutoff. |
| Historical viewing | Prefer the agreed fixed reference climatology for dashboard history; confirm this minimal behavior. Original-issuance statistics versioning is deferred unless essential. Saved bulletin snapshots stay frozen. |
| Missing/stale source | Successful computation below the agreed eligibility threshold publishes null std and the actual count. Read/transport failures abort that update and preserve the previous complete payload; they must not be treated as an empty successful history. |

The existing GBT bounds helper requires three years; PP-075 EM skills require four distinct matched target years.
Neither threshold automatically governs this observation-statistics product. `count` must represent accepted years,
not daily samples or forecast/observation skill pairs (`n_pairs`). Genuine zero variability remains valid.
Preserve the existing historical aggregator defaults, including `fillna(0)`, in stage 1. Use a separate publication
calculator or a narrow opt-in that leaves existing callers unchanged; enforce unavailable insufficient-history
statistics in the new range publication path. Add a regression proving historical delta and accuracy unchanged.

Observation definitions already differ: postprocessing monthly aggregation accepts 50% daily coverage;
hydrograph actuals use rounded decadal aggregation by default, or an 80% daily-coverage fallback. Postprocessing
quarterly aggregation requires three months; its seasonal aggregation uses configurable months and a fixed
`SEASON_MIN_COVERAGE = 0.5` coverage constant, while
the hydrograph writer uses complete April–September. The new statistics follow the agreed postprocessing rules;
existing published actuals and norms remain unchanged. Historical simulations must not silently consume future observations.

## Storage options for discussion with Maxat

| Option | Benefit | Cost / limitation |
|---|---|---|
| Existing `Hydrograph.std/count` (preferred candidate) | Model-independent, existing read/write fields; store std and derive `delta = 0.674 * std` | Moderate application work: history retrieval, coordinated complete writer and reader. Fits only one agreed reference climatology per dated target period. |
| Dedicated observation-statistics contract (deferred) | Could support requirements existing fields cannot meet | Consider only if an essential requirement blocks field reuse; no new schema/versioning for optional provenance. |
| LR or skill-row carrier | Can reuse existing model-facing delta plumbing | Adds model/skill availability dependence; an LR carrier also adds training and evaluation work. Not recommended as canonical storage. |

Existing `std/count` field reuse does not itself require service approval. E0 verifies date/target-period mapping,
season identity and complete-payload write ownership in application code. Coordinate with Maxat only if an actual
API-contract change is needed. Monthly hydrograph `horizon_value` is a month number,
not a forecast lead. Do not reuse short-term delta or borrow another target's/lead's value to fill a missing source.
One owning producer should publish a complete payload; do not introduce independent partial writers against the
current replacement-upsert contract. Service code is read-only until coordination explicitly authorizes changes.

Concrete E0 compatibility check: `build_seasonal_record` uses April–September, whereas the agreed statistic uses
`get_season_months()`. `hydrate_season_hydrograph_stats` selects season 1 without a full period identity. Do not
attach a different configured season's statistics to April–September norms/actuals, or add ambiguous duplicate
season rows. Resolve this mapping before fixing E1's file scope; discuss a contract change only if field reuse
cannot represent the agreed period safely.

## PP-075 frontend contract already agreed

- Delta is the default; Native uses Q25/Q75 (stored EM calculated bounds; LR uses shared delta).
- Manual width is `p * Q`; Mixed width is `min(delta, p * Q)`, where `p = slider_percent / 100`.
- Missing required input leaves both endpoints unavailable, with no automatic method fallback.
- Calculated negative lower endpoints are clamped to zero consistently across surfaces.
- Include existing month/quarter/season cards and outputs; do not invent long-term forecast charts.
- Saved bulletin numeric bounds remain frozen; editing delta is an explicit override to `Q ± delta`.
- Until an authoritative long-term source exists, long-term Delta/Mixed are unavailable. This issue supplies that source;
  it does not reopen the four range methods or recompute existing bulletin snapshots.
- Long-term volume bounds continue to derive from selected discharge endpoints using the existing conversion.

## Proposed phases — conditional on the decisions above

E0 must cut any work not required to publish and consume the agreed statistic. Reuse existing scheduling/configuration
where it fits; no new scheduling framework or generalized statistics service. Each implementation phase uses one
scoped Sonnet implementation agent, followed by orchestrator diff review and
affected-scope tests. No implementation is authorized by this draft. Final file allowlists are fixed in E0 before delegation.

| Phase / dependency | Goal and bounded file scope | Agents | Acceptance |
|---|---|---|---|
| E0 / — | Verify seasonal identity, complete-payload writer and existing maintenance wiring; settle historical-display semantics; update this draft/index and exact helper/file scope. Escalate only actual contract changes to Maxat. | 1 planning agent; owner/Maxat only for unresolved semantics/contracts | Existing-field compatibility established or a concrete contract dependency identified; file allowlists fixed before E1. |
| E1 / E0 | Produce and persist agreed statistics. Candidate owner: `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py` and focused tests under its `test/`; helper location and history-reader reuse settled in E0. Existing postprocessing aggregators are reference evidence, not automatically changed. | 1 implementation agent | Known synthetic std/count, missing-history behavior and complete repeated writes verified; norms/actuals unchanged. |
| E2 / E1 | Read shared delta in `apps/forecast_dashboard/src/db.py`, with focused `tests/`; connect to PP-075's common range calculation. Any configuration wiring is named in E0. | 1 implementation agent | Every model for the same target/reference gets identical delta even without skill; no row multiplication, cross-period leakage or lead confusion. |
| E3 / E2 | Independent open-ended review, operational verification plan and documentation. Scope: changed tests plus relevant module READMEs, `doc/configuration.md`, `doc/data_flow_long_term.md`, `doc/user_guide.md`. | 1 independent reviewer; fixes delegated separately | Review findings resolved, affected/full tests pass, cross-module pipeline/smoke evidence recorded, deployment readiness distinguished from deployment. |

If E0 selects dedicated storage, revise E1/E2 file scopes and dependencies with Maxat and the owner;
do not implement that expansion implicitly. Changed accuracy semantics are outside stage 1.
No new model, framework or broad aggregation refactor.

## Verification and acceptance

- Synthetic annual period means produce the expected sample std and year count; singleton/all-missing histories
  stay unavailable and constant eligible history produces genuine zero. Test nonfinite observations and duplicate years.
- Pin the 50% historical-year rule: 12 valid years out of 26 is unavailable, 13 qualifies; an odd-length reference
  period rounds the required count up. Count each accepted year once; missing years remain in the denominator.
- Pin at least 50% daily coverage per month, all three valid months per quarter, at least 50% valid configured
  months per season (round up), and season/year boundaries, including cross-year seasons.
- Pin the fixed reference window and agreed cutoff. If historical display uses original issuance statistics, test
  the corresponding as-of cutoff; if it uses current reference climatology, label that behavior explicitly.
  This range product must not change historical skill calculations or introduce a new hindcast scoring path.
- API-boundary round trips and repeat maintenance writes retain std/count, norms and actuals together; unavailable
  replacements do not leave an old valid-looking statistic. Use mocked contracts first, no operational fixtures in Git.
- Dashboard joins preserve row identity and count; missing skills or hidden models cannot change delta. Verify all
  PP-075 methods, display rounding and frozen bulletin save/reload/export behavior with the new source.
- Run `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh <module>` for changed modules and importers, then
  `SAPPHIRE_TEST_ENV=True bash run_tests.sh` before commit. Record the owner's accepted 12 existing skips and two
  expected failures by identity/reason; investigate additions or collection failures.
- This changes cross-module data flow: run `bash apps/run_locally.sh all` and `bash apps/run_docker_tests.sh --skip-ml`
  after implementation under the agreed local configuration. Keep operational measurements/credentials outside Git.
- Document calculation/reference settings and missing-data behavior in configuration/user docs; update the long-term
  data-flow diagram and producer/consumer READMEs. No root architecture/framework change is currently proposed.

## Boundaries and rollout

Existing PP-075 backend A–D and the four-year EM skill rule remain intact. The November 8 reconstruction vision is
deprecated. Historical skill recomputation, a new runoff regression, all-horizon storage migration and changes to
existing actual/norm arithmetic are excluded from stage 1. Inventory any existing stored fields
before populating them, verify the existing read/write contract (coordinate actual changes with Maxat), and stage producer availability before
enabling the consumer. No server deployment is assumed complete; no live backfill is authorized by this draft.

## Required follow-up — short-term and backend harmonization

The owner explicitly requires remembering this second stage (2026-11-14); it is deferred, not a blocker to delivering
stage 1. Carry it forward in the issue index and handover without implementing or designing a general migration now.

- [ ] Plan pentad/decade observation statistics against the agreed long-term contract: reference history,
  period definition, minimum history, missing-value semantics and independent storage/read ownership.
- [ ] Map backend consumers (reported skill delta, forecast accuracy tolerance and ensemble/LR bounds) and
  agree which adopt the shared source. Preserve PP-075 A–D until that change is explicitly authorized.
- [ ] Decide historical metric compatibility/recalculation and rollout with the owner and Maxat before changing
  stored/reported numbers. Create a linked implementation issue when stage-1 contract decisions are stable.

```json
{"phases":{
  "E0":{"depends_on":[],"parallel_agents":1},
  "E1":{"depends_on":["E0"],"parallel_agents":1},
  "E2":{"depends_on":["E1"],"parallel_agents":1},
  "E3":{"depends_on":["E2"],"parallel_agents":1}
}}
```
