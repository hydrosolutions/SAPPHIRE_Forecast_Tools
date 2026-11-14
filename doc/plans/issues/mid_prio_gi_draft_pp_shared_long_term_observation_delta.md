# Shared long-term observation delta — vision and draft plan

**Status:** In Progress — E0 and E1 complete locally; E1 independently reviewed and verified. E2 has not started; no deployment or live refresh is claimed.
Implementation has not started. Exact E2 presentation test/locale files are fixed before E2 delegation.
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

Owner decisions and the bounded engineering choices established in E0 follow.

| Question | Options / decision needed |
|---|---|
| Intended use — decided | Ranges first; preserve historical accuracy calculations (owner, 2026-11-14). |
| Reference history — decided | 2000–2025 inclusive, configurable per deployment (owner, 2026-11-14). Keep this range reference independent of skill-scoring configuration. |
| Initial horizons — decided | Month/quarter/season and dashboard first; short-term and backend harmonization is a required follow-up, not discarded scope (owner, 2026-11-14). |
| Accepted observations — decided | Retain existing postprocessing rules: month requires at least 50% daily observations; quarter requires all three valid monthly means; season requires at least 50% of its configured monthly means (owner, 2026-11-14). Preserve the existing period aggregation arithmetic; do not substitute rounded hydrograph actuals. |
| Minimum history — decided | At least 50% of potential years in the configured reference period must have a valid target-period observation (owner, 2026-11-14). For 2000–2025 inclusive: 13 of 26 distinct years. Use `ceil(0.5 * (end_year - start_year + 1))`, with at least two observations required for sample std. Below threshold: unavailable. |
| Seasonal scope — decided | April–September only in stage 1 (owner, 2026-11-14). Other target seasons have unavailable Delta/Mixed bounds; their support is deferred. Existing norms/actuals keep their April–September meaning. |
| Cutoff and refresh | Reuse yearly/manual hydrograph maintenance, including observation corrections. Read the entire configured reference period; require its end year to be completed relative to the entrypoint's captured date. Reject an incomplete reference configuration rather than shrink its denominator. `--target-year` remains the storage/actuals year, not the reference cutoff. |
| Historical viewing — decided | Use the same configured 2000–2025 reference for historical dashboard views, labelled with the reference years (owner, 2026-11-14). No original-issuance statistics reconstruction. Saved bulletin numeric bounds stay frozen. |
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
the hydrograph writer uses complete April–September. The new statistics follow the agreed postprocessing coverage
and averaging rules, but stage-1 seasonal statistics are deliberately limited to April–September;
existing published actuals and norms remain unchanged. Do not feed this range-statistics product into historical
scoring; the owner's historical-dashboard choice deliberately permits the fixed later reference climatology.

## Storage choice and deferred alternatives

| Option | Benefit | Cost / limitation |
|---|---|---|
| Existing `Hydrograph.std/count` (selected after E0) | Model-independent, existing read/write fields; store std and derive `delta = 0.674 * std` | Application history retrieval, coordinated complete writer and reader. One configured reference climatology per dated target period; no version metadata. |
| Dedicated observation-statistics contract (deferred) | Could support requirements existing fields cannot meet | Consider only if an essential requirement blocks field reuse; no new schema/versioning for optional provenance. |
| LR or skill-row carrier | Can reuse existing model-facing delta plumbing | Adds model/skill availability dependence; an LR carrier also adds training and evaluation work. Not recommended as canonical storage. |

Existing `std/count` field reuse does not itself require service approval. E0 verified date/target-period mapping,
April–September season identity and complete-payload write ownership in application code. Coordinate with Maxat only if an actual
API-contract change is needed. Monthly hydrograph `horizon_value` is a month number,
not a forecast lead. Do not reuse short-term delta or borrow another target's/lead's value to fill a missing source.
One owning producer should publish a complete payload; do not introduce independent partial writers against the
current replacement-upsert contract. Service code is read-only until coordination explicitly authorizes changes.

Seasonal compatibility is resolved by the owner's April–September-only decision. Publish those statistics on
the existing April 1/season-1 row; match forecast `valid_from/valid_to` to April 1–September 30 of the same year.
Never join by the forecast's `season_in_year`: `get_long_forecasts_season` fills that column from forecast lead,
whereas Hydrograph season 1 is a period identifier. Keep `hydrate_season_hydrograph_stats` and its norm/actual
selection unchanged. Do not create extra seasonal rows or repurpose fields for another season.

## E0 wiring results and implementation contract

- Configuration: add `SAPPHIRE_RANGE_REFERENCE_START_YEAR=2000` and
  `SAPPHIRE_RANGE_REFERENCE_END_YEAR=2025`, independent of skill-scoring settings and shared by producer/dashboard.
  Validate the ordered completed-year range; the eligible-year threshold is `max(2, ceil(number_of_years / 2))`.
- Producer: reuse `_read_daily_runoff(client, code, year)` once per reference year/station; each annual read fits
  the existing 10000-row limit. Do not use postprocessing's failure-to-empty reader. Calculate the entire station's
  unrounded period statistics before its first write, then attach `std/count` to existing monthly, seasonal and
  quarterly payloads. Preserve three write calls/order, current error/status routing and actual/norm arithmetic.
  A failed history read prevents that station's publication; existing partial-write handling is unchanged.
  Reuse target/previous-year reads where they overlap reference history. The new calculator excludes NaN/infinite
  observations from both coverage counts and means; historical scoring aggregators are unchanged.
- Scheduler: `bin/yearly_runoff_hydrograph_aggregation.sh` and
  `apps/run_locally.sh::run_maintenance_preprocessing_runoff` already invoke the owning writer. No scheduler,
  shell-entrypoint, Dockerfile or dependency change is required. Do not use deprecated `sync_monthly_norms.py`,
  whose norm-only writes would erase statistics, just as they erase actuals today.
- Keep the existing `resolve_sdk_station_codes` scope: it excludes manual/Google-Sheets stations. This phase
  does not add station discovery or write partial statistics-only rows for those stations; absent source means
  unavailable ranges. Do not describe the source as covering stations outside the existing writer's scope.
- Reader: add a dedicated unrounded statistics read in dashboard `src/db.py`; leave
  `get_hydrograph_pentad_all` unchanged (it drops `std/count` for existing display consumers). Query the
  publication year from the once-captured date at `DataManager.load_station`, passed explicitly. A missing/null
  current publication remains unavailable; do not resurrect older nonnull values. A read failure is not valid data.
- Join by station and target calendar month/quarter, or the validated April–September interval, independently
  of forecast issue year, model and lead. Preserve row order/count, native quantiles and existing skill columns.
  Add a separate `range_delta` after skill joins, including when unavailable; long-term range calculation must
  consume this column explicitly and never fall back to the model-skill `delta` column.
- In the long-term forecast summary/bulletin handoff, the displayed/editable range delta must come from
  `range_delta`, not the retained skill delta. Identify it as range tolerance with the reference-period label;
  existing accuracy values and the underlying skill frames stay unchanged. Avoid showing a different delta
  beside bounds calculated from the shared source.
- Populate main monthly, m0, all configured monthly-mode frames, the monthly quarter card, standalone quarter
  and season frames. Existing target/lead selection and captions remain intact. PP-075 handles common range
  controls, reference-period labelling, quarter-card quantile substitution removal and bulletin/export consistency.
- Rollout: producer and dashboard must use the same settings. Because existing rows carry no reference-version
  metadata, pause affected dashboard consumption before the first changed-reference write. Resume with the same
  new settings only after every intended station/horizon in the publication year has been verified refreshed;
  keep consumption paused after a partial failure. This is a coordinated maintenance/restart procedure, not a new
  feature flag or versioning system. Ordinary reruns under unchanged settings retain station-level failure behavior.

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

## Implementation phases and file scope

Use one scoped Sonnet implementation agent per implementation phase, followed by orchestrator diff review and
affected-scope tests. E0 recorded the decisions; E1 implements only the scoped application producer. No service code or operational data was changed.

| Phase / dependency | Goal and bounded file scope | Agents | Acceptance |
|---|---|---|---|
| E0 / — **Complete** | Verify seasonal mapping/wiring; record owner decisions and the scope below. | Orchestrator + independent read-only review | April–September mapping and fixed-reference historical display resolved; no service/schema dependency for this scope; review corrections confirmed. |
| E1 / E0 **Complete locally** | Produce and persist statistics using the exact producer scope below. Existing postprocessing aggregators remain unchanged reference evidence. | 1 implementation agent | Synthetic std/count, thresholds, pre-write read failures and complete repeated writes verified; norms/actuals unchanged. |
| E2 / E1 | Add the independent dashboard statistics read/joins; integrate with the already-agreed PP-075 frontend behavior using the scope below. | 1 implementation agent | All existing surfaces use the selected method; every model targeting the same period receives the same range delta even without skill; no row multiplication or lead confusion. |
| E3 / E2 | Independent open-ended review, operational verification plan and documentation. Scope: changed tests plus relevant module READMEs, `doc/configuration.md`, `doc/data_flow_long_term.md`, `doc/user_guide.md`. | 1 independent reviewer; fixes delegated separately | Review findings resolved, affected/full tests pass, cross-module pipeline/smoke evidence recorded, deployment readiness distinguished from deployment. |

**E1 allowlist (relative to `apps/preprocessing_runoff/`):** new `src/long_term_range_stats.py` (small pure
calculator/config helper), `sync_long_horizon_hydrograph.py`, new `test/test_long_term_range_stats.py`, and
`test/test_sync_long_horizon_hydrograph.py`. Existing `test/test_long_horizon_monthly_decadal_m3.py`,
`test/test_virtual_station_norms_long_horizon.py` and `test/test_backfill_discharge_aggregation_m4.py` may receive
only necessary fixture updates and statistics-preservation assertions. No postprocessing, scheduler or service edits.
The backfill utility delegates to the same writer through a capturing client, so it inherits complete payloads;
assert captured and forwarded `std/count` explicitly in mocked tests. Its existing `_COMPARE_FIELDS` excludes
these fields, so its verifier is not statistics round-trip evidence. Do not change backfill orchestration or run a live backfill.

**E2 allowlist (relative to `apps/forecast_dashboard/`):** `src/db.py`, `dashboard/data_manager.py` (only explicit
load-date plumbing), new `tests/test_long_term_range_delta.py`, and narrow existing reader/lead regression tests.
The PP-075 presentation integration additionally owns `src/processing.py`, `src/vizualization.py`,
`dashboard/{widgets,widget_manager,plot_manager,bulletin_manager,bulletin_publish}.py` and their focused tests;
`src/{site,bulletins}.py` only if necessary for the agreed bounds/formatting handoff. Translation entries follow
the existing locale mechanism. Keep `dashboard/utils.py` hydration and native quantiles unchanged. Before E2
delegation, name the exact existing test/locale files touched by its presentation subtask; no unrelated UI cleanup.

No new model, framework, shared cross-module aggregation refactor or statistics-versioning infrastructure.
Narrow additive parameter plumbing is permitted only for the statistics payload/load-date described above;
preserve existing positional callers and otherwise keep signatures/data flow unchanged.

## Verification and acceptance

E0 verification was read-only code/API-contract inspection, an independent open-ended review, confirm-fixes and
`git diff --check`. No live deployment was checked or code changed. The unchanged code baseline's prior full-suite
result is 8,866 passed, 12 owner-accepted skips and two expected failures; tests were not rerun for this E0 doc update.

- Synthetic annual period means produce the expected sample std and year count; singleton/all-missing histories
  stay unavailable and constant eligible history produces genuine zero. Test nonfinite observations and duplicate years.
- Pin the 50% historical-year rule: 12 valid years out of 26 is unavailable, 13 qualifies; an odd-length reference
  period rounds the required count up. Count each accepted year once; missing years remain in the denominator.
- Pin at least 50% daily coverage per month, all three valid months per quarter, at least three valid April–September
  months per seasonal year, and leap/year boundaries. Other seasonal windows have unavailable range delta.
- Pin the completed fixed reference window and its label. Older dashboard forecasts deliberately use the current
  publication's fixed-reference delta; a forecast issue-year change alone must not change that value.
  This range product must not change historical skill calculations or introduce a new hindcast scoring path.
- API-boundary round trips and repeat maintenance writes retain std/count, norms and actuals together; unavailable
  replacements do not leave an old valid-looking statistic. Use mocked contracts first, no operational fixtures in Git.
- Dashboard joins preserve row identity and count; missing skills or hidden models cannot change delta. Verify all
  PP-075 methods, display rounding and frozen bulletin save/reload/export behavior with the new source.
- Cover every monthly mode and quarter card, December-issued January targets, arbitrary monthly/seasonal leads,
  wrong seasonal intervals, genuine std=0, and current null/missing statistics with an older nonnull row.
- Run `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh <module>` for changed modules and importers, then
  `SAPPHIRE_TEST_ENV=True bash run_tests.sh` before commit. Record the owner's accepted 12 existing skips and two
  expected failures by identity/reason; investigate additions or collection failures.
- This changes cross-module data flow: run `bash apps/run_locally.sh all` and `bash apps/run_docker_tests.sh --skip-ml`
  after implementation under the agreed local configuration. Keep operational measurements/credentials outside Git.
- Document calculation/reference settings and missing-data behavior in configuration/user docs; update the long-term
  data-flow diagram and producer/consumer READMEs. No root architecture/framework change is currently proposed.

## E1 implementation and verification — 2026-11-14

Implemented the pure observation calculator and existing full-payload writer integration. Monthly, quarterly and
April–September records carry raw sample `std` and accepted-year `count`; the dashboard will derive `0.674 * std`
in E2. The existing actual/norm arithmetic and historical skill code remain unchanged. Invalid reference settings
raise before writes; annual history read failures use existing station failure routing. Direct writer calls without
new statistics preserve existing values, including JSON-safe null counts. Successful insufficient-history refreshes
replace old statistics with explicit null standard deviation and the actual count.

- Final producer suite: **639 passed, two owner-accepted existing skips** via `run_tests.sh preprocessing_runoff`.
- Full suite: **8,947 passed, 12 owner-accepted existing skips, two expected failures**, all 16 suites successful.
  This was followed by the final producer rerun above after one seasonal null-count correction and its regression.
  Accepted skip identities/reasons remain: `test_validate_dashboard_origins.py` (old system Bash, one),
  preprocessing `test_src.py` (missing existing fixture helpers, two), long-term `test_data_interface.py`
  (unconfigured environment, three), and dashboard `test_integration.py`/`test_integration_bea.py`
  (live integrations, six). Existing expected failures remain the configuration shell helper and deferred
  forecast-side tombstone case. No additional skips or expected failures were accepted.
- An earlier full run had 18 pipeline teardown errors because a concurrent producer test run appended to the
  shared test log. The isolated rerun passed; do not overlap these suites. A slow sandboxed producer run was
  interrupted and rerun with the same test permissions as the successful baseline.
- Independent open-ended review and confirm-fixes completed. Nullable-count serialization and final backfill
  forwarding/replacement assertions were corrected; final reviewer result: no findings, prior findings resolved.
- `ruff check` on the five changed Python files and `git diff --check` pass.
- `run_docker_tests.sh --skip-ml`: eight builds and eight import smokes passed; ML intentionally excluded by the
  requested command. The final changed preprocessing image was rebuilt and its producer import checked again.
  Builds used tracked source plus the scoped patch, excluding linked environments and local operational files.

No live backfill, deployment or `run_locally.sh all` against an operational configuration was performed. The
integrated local pipeline run remains part of E3 under an agreed isolated configuration; server refresh and
producer/consumer reference-setting alignment remain rollout work with Maxat. E2 dashboard integration is next.
Implementation is on the isolated `fix/pp075-long-term-range-stats` branch; the original checkout remains untouched.

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
  "E0":{"depends_on":[],"parallel_agents":1,"status":"complete"},
  "E1":{"depends_on":["E0"],"parallel_agents":1,"status":"complete"},
  "E2":{"depends_on":["E1"],"parallel_agents":1},
  "E3":{"depends_on":["E2"],"parallel_agents":1}
}}
```
