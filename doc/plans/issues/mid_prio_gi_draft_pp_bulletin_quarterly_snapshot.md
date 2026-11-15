# PP-075: persist the monthly bulletin’s quarterly reservoir section

**Status:** Review — owner approved including the scoped service changes in this PR on 2026-11-15. Local implementation, tests and independent review are complete; owner/Maxat review and service-first deployment remain separate.
**Priority:** Medium. **Parent:** [PP-075 long-term ranges](mid_prio_gi_draft_pp_shared_long_term_observation_delta.md).
**Owner decision, 2026-11-15:** plan persistent quarterly storage with Maxat before completing E2.
The owner has authorized service implementation in this PR. No deployment or message to Maxat has been performed.

**Latest owner direction:** save potentially edited quarterly forecasts from the Forecast tab and let
bulletin writers read persisted values, as for other horizons. Keep the solution minimal. The owner
explicitly confirmed that each saved monthly bulletin keeps its own frozen quarterly copy.

The existing `bulletins` table/API already accepts `horizon_type="quarter"`, but referencing that mutable
quarter row would let an explicit quarterly re-save change older monthly exports. Therefore keep the
quarterly copy on the **existing monthly bulletin row**, using the single nullable field below. Do not
create another table, introduce record links/versioning, or require a second quarterly write. Existing
API support for standalone quarterly rows remains independent; this proposal does not add a standalone
quarter-save screen or action. Such rows cannot update a monthly bulletin’s saved copy.

## Problem verified in code

The monthly reservoir Excel bulletin includes a supplementary quarterly section. Its bounds are not
saved with the monthly bulletin. `dashboard/bulletin_manager.py` fetches the latest quarterly forecast
in `_on_add`, `_on_add_m0` and `_populate_forecast_attributes`; `_reshape_long_forecast_for_bulletin`
renames native `Q25/Q75` to export bounds. Reload/write can therefore change that section, and Manual,
Delta or Mixed selected in the dashboard can disagree with the exported numbers.

Existing `BulletinBase` / `Bulletin` store one primary forecast per
`(horizon_type, year, horizon_value, code)`. The current `/bulletin/` POST/GET can round-trip the primary
numeric bounds, but have no field for the supplementary quarter or its dates.
`create_bulletin` currently uses `model_dump()` with defaults, which matters for compatibility below.

Evidence: `sapphire/services/postprocessing/app/{models,schemas,crud,main}.py` (read only);
`apps/forecast_dashboard/dashboard/{bulletin_manager,bulletin_publish}.py`;
`apps/forecast_dashboard/src/{site,bulletins}.py`.

## Suggested minimum contract

Add **one nullable JSON column**, tentatively `quarterly_snapshot`, to the existing `bulletins` row,
and one optional, explicitly typed nested object to `BulletinBase` / `BulletinResponse`.
Using the existing SQLAlchemy `JSON` convention is sufficient; JSONB is Maxat’s choice if preferred.
No new endpoint, table, uniqueness key or generic versioning system is needed.

| Nested field | Type / meaning |
|---|---|
| `model_type` | Existing model enum; the actual selected quarterly model |
| `valid_from` | ISO calendar date; first day of the forecast quarter |
| `valid_to` | ISO calendar date; inclusive last day of that same quarter |
| `forecasted_discharge` | Finite number or null; frozen quarterly forecast |
| `fc_lower` | Finite number or null; frozen selected lower bound |
| `fc_upper` | Finite number or null; frozen selected upper bound |

Require all six keys when the object is present; nullable values must be explicit. Bounds must be
both numeric and ordered, or both null. Validate a complete calendar quarter, as the dashboard already
does. The object applies only to monthly bulletin rows containing a supplementary quarterly forecast.
Station identity is inherited from the parent row; do not duplicate it in JSON.

Persist the selected **numeric bounds**, not instructions to recalculate them. Method, percentage,
reference history, native quantiles, observation rows and model skill values are not required to
reproduce the bounds and are outside this minimal proposal. Volume bounds are derived from the frozen
endpoints and frozen inclusive quarter dates using the existing conversion; no duplicate volume fields.
Existing norm/percentage semantics are unchanged; this proposal freezes forecast bounds and period,
not the entire mutable bulletin document.

### Save/update and old-record behavior

- New monthly save: include the captured object, or explicit null when no supplementary quarter is selected/available.
- Existing row, field omitted: preserve its stored quarterly snapshot, so an older client cannot erase it.
- Explicit null: clear the supplementary snapshot. Explicit object: replace it atomically with the monthly save.
- Apply omission handling only to this new field; do not change the established semantics of other fields.
  Maxat should inspect Pydantic `model_fields_set` / equivalent before the current default-inclusive dump.
- Migration leaves old rows null. Their historical quarterly bounds cannot be reconstructed reliably;
  do not backfill them from current forecasts. Display/export the supplementary section as unavailable
  until the operator explicitly re-adds the bulletin selection. Never silently substitute native bounds.
- GET returns the object or null. Exact JSON round-trip and null semantics are acceptance criteria.
- Sanitize this nested snapshot before dashboard POST: missing/non-finite numeric values become null,
  dates become ISO strings. The existing dashboard sanitizer is top-level only; do not assume it handles JSON children.
- In the service, after distinguishing omission, dump this nested typed object in JSON mode so date
  objects can be stored in the JSON column. Preserve existing primary-field serialization. Verify an actual
  database commit/read with dates and missing bounds, not just Pydantic validation.

### Dashboard changes after agreement

Use the existing **monthly Add / Add-m0** action to save the selected monthly row and the selected
quarterly table row together in one existing `/bulletin/` POST. Save the current edited table values;
no separate quarterly save button or second POST is required for the monthly supplementary copy.

Validate the edited bound pair before mutating saved bulletin state. Stage the candidate monthly row
and quarterly copy locally in these two Add paths, perform the existing POST, and only on success
replace the in-memory saved site/entry. On validation or POST failure, retain the previous saved state
(or no entry for a failed first Add), report the failure, and leave Forecast-tab edits available for
correction/retry. A later monthly-cell edit must not accidentally persist the failed quarterly candidate.
This is local save-success handling, not a generic draft/transaction framework; unrelated horizon flows
remain outside scope.

At that action, capture only from the current station’s successfully populated, visible
quarterly summary card. Hidden/unavailable cards retain old tabulator values today; those must produce
no snapshot. Validate selection indices against the current table. Capture the selected row, its model
and its validated target dates. Save the selected row’s **current numeric values, including operator
edits to forecast discharge and lower/upper bounds**. Do not recompute them from quantiles, delta or
current controls during save. Model and period identity come from the unedited row metadata, never
from matching an edited discharge to a raw forecast. Keep identity fields read-only; this requires no
new editing framework or general draft-management system.
If the card offers multiple models, honor its row selection; when none is selected, use the same explicit
first-row default as the primary Add flow. Resolve dates against that selected model and quarter in the
loaded quarterly frame, not an arbitrary latest forecast or the monthly target month. An ambiguous or
unavailable match leaves the supplementary snapshot unavailable. No new selection UI is proposed. Keep the card’s selected row identity/period alongside the rendered
values so sorting or numeric edits do not change its identity; use narrow quarterly-card plumbing,
not a new cross-horizon state abstraction.

Carry the snapshot through `_site_to_records`, `_load_bulletin_from_api` and site attributes. On reload,
write and share, populate quarterly discharge/volume bounds solely from the saved snapshot. Stop the
three live quarterly bound reconstruction paths. Changing controls or upstream forecasts must not alter
saved bounds; an explicit re-add replaces the snapshot. Editing monthly delta changes only monthly
bounds. Reuse the existing Forecast-tab table editing and explicit save/Add convention. No separate
quarterly editing screen or automatic saving on cell edit is proposed.

Distinguish no saved quarter from a saved quarter whose bounds are unavailable:
- No saved quarterly record/snapshot: omit the supplementary section (legacy rows remain absent).
- A saved quarter with both bounds null: retain its section and period heading with blank bound/volume cells.
- Mixed reservoirs: include the saved quarterly entries; missing entries do not acquire fabricated bounds.
Use persisted entry presence, not `None` versus `NaN` or bound truthiness, to determine section presence.
This requires a narrow change to `bulletins.py::has_quarterly` as well as the period-conflict check.

`bulletin_publish.serialize_site` currently omits the supplementary section. Proposed public JSON:
add `quarterly: null` for no saved entry; otherwise one object with the six persisted fields above plus
`v_min` and `v_max` derived in the existing volume units (million cubic metres). Keep stored discharge
values unchanged in this additive object; do not store a second presentation copy. Missing volume bounds
are null. Excel applies its existing display formatting; tests compare its displayed values with the
same formatting applied to JSON numbers, separately from exact persistence round-trip assertions. The public endpoint returns JSON directly; there is no renderer to
add or change. The share service already stores an opaque `payload: dict`; no additional share endpoint/schema
is proposed. Verify Excel/public JSON numeric parity and retain existing share access/expiry behavior.

The Excel quarterly section has one shared period heading. Proposed minimal behavior: before export,
detect conflicting non-null saved quarter date pairs across reservoir rows and report the conflict,
asking the operator to remove conflicting stations from this export or defer export. Re-adding helps
only when available quarters align: the current card offers the latest displayable quarter, not an
older-quarter picker. Do not add such a picker for this maintenance change. Do not label different quarters as the first
station’s quarter. Apply the same consistency check before creating a new share snapshot. This guard
is part of the proposal for agreement; per-period layout redesign is deferred.

### Minimum operator workflow

1. In the Forecast tab, select a quarterly model and optionally edit its discharge and bound cells.
2. Use the existing monthly Add / Add-m0 action. It saves both selected monthly values and the current
   quarterly values in the same monthly bulletin record. Make this existing action’s combined effect
   explicit in its adjacent help text; no extra save button or second API transaction.
3. The bulletin writer reloads that monthly record, reads `quarterly_snapshot`, and formats its saved
   values. Editing controls, updating model forecasts, or saving any standalone quarter cannot change it.
4. To replace the monthly copy, explicitly select/edit and Add again. An ordinary monthly bulletin-cell
   edit carries the existing snapshot unchanged; it must not recapture the Forecast tab.

## Deployment review for Maxat

1. Is one typed nullable `quarterly_snapshot` JSON field on the existing bulletin row acceptable?
2. Can the service preserve this new field when omitted and clear it only on explicit null?
3. Review the implemented six-field contract, quarter-date validation, and old-row null behavior before deployment.
4. Deploy migration/service support first, verify POST→GET on synthetic records, then deploy the dashboard.
   An old service may ignore unknown request fields: deployment verification must prove persistence, not just HTTP success.

No backend forecast/delta recalculation is required. No operational records or station/discharge data
belong in migrations, fixtures, documentation examples or commits; tests use synthetic fixtures only.

## Bounded phases and acceptance

| Phase / dependencies | Owner / allowed scope | Acceptance |
|---|---|---|
| Q0 / — | Orchestrator + independent read-only reviewer; this proposal and PP-075 index only | Owner authorized this PR’s contract and scoped service work; Maxat reviews migration/rollout before deployment |
| Q1 / Q0 | Owner-authorized delegated service implementation in this PR: Bulletin model/schema, one additive migration, narrow CRUD handling and service tests | New/old row POST→GET; omission preserves, null clears; no changes to primary fields or uniqueness |
| Q2 / Q1 contract | One delegated dashboard agent; bulletin_manager, site, bulletin_publish; widgets/widget_manager/plot_manager (quarterly read-only identity and row-period handoff only); bulletins (section-presence and period-conflict checks only); focused tests | Selected quarter captured; same numeric bounds after save/reload/export/share; missing stays blank/null; no live reconstruction |
| Q3 / Q1 + Q2 | Independent read-only reviewer; fixes delegated separately | Focused/full runner checks; synthetic API round-trip; existing monthly/quarter/season/short-term behavior and accepted baseline preserved; deployment separately verified |

Regression example uses synthetic values only: quarterly Q=100, native 90–110, Manual 20% gives 80–120.
Save, change upstream forecast and selector, reload and export: still 80–120, with volume calculated
from the saved quarter dates. Repeat with missing Delta: both bounds remain null, including after reload.
Test monthly delta edit does not change the supplementary quarterly snapshot. Exercise both main and m0
Add paths, a hidden quarterly card retaining stale values, two reservoirs with conflicting quarters,
existing rows without snapshots, present snapshots with all bounds null, mixed presence, None/NaN-equivalent
export behavior, omission/null distinction, nested date/null serialization, edited discharge/bounds surviving
save/reload, and the defined public share JSON object. Reject a one-sided/inverted edited bound pair
and simulate POST failure in both Add paths: no unsaved quarterly values in Excel/reload/share, no entry
after failed first Add, retained Forecast-tab edits, and no accidental save of the failed candidate
through a subsequent monthly-cell edit. No new public-rendering framework or endpoint.

```json
{"phases":{"Q0":{"depends_on":[],"parallel_agents":1},"Q1":{"depends_on":["Q0"],"parallel_agents":1},"Q2":{"depends_on":["Q1"],"parallel_agents":1},"Q3":{"depends_on":["Q1","Q2"],"parallel_agents":1}}}
```

## Proposal review evidence (earlier revision)

Independent read-only review on 2026-11-15 identified nested JSON serialization, stale hidden-card
selection, conflicting quarter headings, and an incorrect assumption about a public renderer. The
proposal above incorporates those corrections. A fresh confirm-fixes pass returned no remaining
findings. This is review of the proposal, not Maxat’s agreement or verification of an implementation.

A subsequent fresh review found that quarterly Excel section presence currently depends on bound values;
the revised contract above explicitly distinguishes absent entries from present entries with null bounds.
Share JSON shape and the feasible remedy for conflicting quarter headings are now stated. The latest
owner direction requires preserving Forecast-tab edits. Reusing a mutable standalone quarter row was
considered; the owner explicitly retained frozen monthly copies. One nullable field on the existing
monthly bulletin row remains the proposed minimum. This revision was sent back for independent review.

The revised independent simplicity review supports one nullable field and one monthly POST. Its only
remaining finding was local state replacement before save success; the Add-path staging/failure rule
and focused acceptance cases above address that concrete edited-bound rejection scenario. The narrow
confirm-fixes review returned no remaining findings. No implementation changes are included in this
planning revision; this is proposal review, not service implementation or deployment approval.

## Implementation evidence — 2026-11-15 (in progress)

The owner explicitly authorized including the service changes in this PR. The service patch adds
`quarterly_snapshot` through revision `c3d4e5f6a7b8` after `b2c3d4e5f6a7`, with typed nested validation
and omission-preserving updates. The focused postprocessing service runner passed 244 tests with no
skips after test corrections. Independent review requested stronger preservation assertions; these
were added, and the final confirm-fixes pass returned no remaining findings. This is local evidence,
not deployment approval.

A source-only service image built locally. An isolated PostgreSQL 16 database successfully ran the
existing migration chain, accepted a synthetic legacy row, and upgraded to the new revision with the
row intact and its snapshot SQL NULL. Downgrading removed only the new column and retained the primary
row. The temporary container/network were removed; no operational database was accessed.

Dashboard implementation now captures edited quarterly values in monthly Add/Add-m0, stages candidate
state until save success, and reloads/exports only the saved snapshot. Quarter identity is readonly;
hidden/stale cards are unavailable. Monthly cell edits preserve the snapshot. Excel and share checks
cover missing bounds, frozen dates/volumes, mixed presence and selected-station period conflicts.

The final focused dashboard runner passed 1,072 tests with the six accepted live-integration skips.
The final full runner passed 9,328 tests with the same 12 accepted skips and two expected failures;
skip identities/reasons matched the checkpoint. Independent review found no production defects and
requested stronger regression tests; these are corrected, and the final confirm-fixes/documentation
review returned no remaining findings. Eight non-ML Docker builds and eight import smokes passed.

Deploy the migration and service before the dashboard, then verify persistence on the actual deployment
with Maxat. No deployment, operational refresh or live pipeline run is claimed; the latter still needs
an agreed non-operational configuration.
