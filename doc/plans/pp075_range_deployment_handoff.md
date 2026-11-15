# PP-075 range checkpoint — handoff for Maxat

**Status:** local implementation/verification checkpoint; no deployment or live refresh is asserted.
E1 is committed as `2b924654`. E2 now includes locally implemented and tested
[quarterly-copy persistence](issues/mid_prio_gi_draft_pp_bulletin_quarterly_snapshot.md), with service support authorized in this PR and independent review complete. This handoff
is for coordination and verification, not evidence that a server already has these changes.

## What needs coordination

- **Observation statistics:** existing preprocessing `Hydrograph.std/count` fields are sufficient.
  Deploy the application writer and dashboard with the same reference settings; no service schema
  change is needed for this part. Defaults are 2000–2025 inclusive.
- **Quarterly bulletin copies:** the owner authorized the nullable field and scoped service implementation
  in this PR. Migration/API deployment and a verified persistence round-trip are required before deploying
  the dashboard writer. Local implementation and tests are complete; deployment is not asserted.
- **Short-term backend:** PR #539 is merged, but server deployment remains unverified. Confirm the
  deployed version supports shared delta and stored EM bounds before relying on those values.

## Observation-statistics rollout

1. Agree the deployment, captured publication/load year and reference years. Both producer and
   dashboard need `SAPPHIRE_RANGE_REFERENCE_START_YEAR` and `SAPPHIRE_RANGE_REFERENCE_END_YEAR` with
   identical values. The reference end must precede the run/load year. Keep configuration and data
   outside Git. Confirm the intended station set, including the existing exclusion of manual sites.
2. Use the existing coordinated maintenance window. Before the first changed-reference write, pause
   affected dashboard consumption. There is no reference-version field: a new label must not be shown
   against an old or partly refreshed publication.
3. Pull or build the intended preprocessing writer image first and verify its revision/digest against
   the approved checkpoint. The maintenance wrapper reuses an already cached tag and pulls only when
   the image is absent; updating a checkout or republishing a tag alone does not update that local image.
   Refresh using the existing owner-approved long-horizon entry point
   `bin/yearly_runoff_hydrograph_aggregation.sh` with the external environment file and
   `--target-year` set to the dashboard’s publication year. The script may write operational data;
   execute only as the agreed deployment operation. Do not use deprecated `sync_monthly_norms.py`.
   The writer’s `--dry-run` resolves stations only; it does not validate calculated statistics or writes.
4. Inspect the run summary and API responses for every intended station/horizon. Verify 12 monthly,
   four quarterly and one April–September seasonal row in the publication year, with correct period
   dates and `std/count` alongside norms/current/previous. Legitimately insufficient history must have
   unavailable std and the actual accepted-year count; zero variability is a valid zero. API failure
   or a partially refreshed changed-reference publication is not readiness. Keep consumption paused
   until coverage is verified. Existing station-level error/exit semantics remain unchanged.
5. Check the stored statistics explicitly through the preprocessing API. The existing backfill
   comparison excludes these fields, so a successful backfill comparison alone is insufficient.
   Confirm repeat publication retains the complete payload. No live backfill has been run here.
6. Restart the dashboard with matching settings and verify the reference label and range values.
   It reads the current load-year publication even for older forecasts; it does not fall back to a
   previous year or model skill delta. When changing or reverting a reference window, repeat the
   coordinated publication/check procedure rather than changing only the dashboard setting.

## Small synthetic acceptance check

Use isolated synthetic records in an agreed test environment; do not commit operational measurements,
station identifiers or credentials. The local unit/integration suite already exercises these contracts
with mocked API boundaries; deployed API round-trip remains a separate check.

- Use two models targeting the same month with different skill deltas. Both must receive the same
  observation range δ and preserve their original accuracy/skill values.
- With the default 26-year reference: count 12 is unavailable; count 13 qualifies. A finite std of zero
  produces δ=0. For eligible std, compare δ with `0.674 × std` before display rounding.
- Check all four methods and both short-term chart paths. Native uses Q25–Q75 (LR uses shared δ);
  Delta uses δ; Manual uses the selected percentage; Mixed uses the smaller half-width. Missing required
  inputs leave both endpoints unavailable. Separate daily forecast overlays stay native.
- Apply a new method and switch station/horizon/tab: monthly primary, eligible m0 and quarterly cards
  refresh together; long-term controls remain reachable; the reference label follows loaded metadata.
- Read an older forecast: it uses the same current reference-period δ. A non-April–September season
  has no Delta/Mixed bounds. No extra long-term chart or quarter selector is introduced.

## Quarterly storage deployment gate

The owner approved implementing the reviewed proposal in this PR. Its minimal workflow is existing monthly Add/Add-m0
saving the edited quarterly row as a frozen copy on that same monthly bulletin row, using one POST.
A successful later quarterly forecast/model refresh must not change the copy. Missing-bound snapshots
retain a quarterly section with blank cells; absent snapshots do not fabricate one.

The migration revision is `c3d4e5f6a7b8` (after `b2c3d4e5f6a7`); it adds only nullable
`bulletins.quarterly_snapshot` and performs no backfill. Deploy the migration/service before the
dashboard writer.

After deploying the service change with Maxat, verify real POST→GET persistence, omission/null behavior and nested
ISO dates/null numbers before enabling its dashboard writer. Older services may ignore unknown fields,
so HTTP success alone is not proof. Local regression tests cover Add/save/reload/Excel/public JSON,
including failed saves and conflicting quarters. No generic versioning, new table or second quarterly
write is needed. Existing published share payloads remain frozen.

## Local evidence versus deployment evidence

Record the checkpoint’s exact runner results in the [implementation plan](issues/mid_prio_gi_draft_pp_shared_long_term_observation_delta.md).
The owner accepts the existing 12 skipped tests and two expected failures; compare identities, not
just totals. Run the full repository runner before committing and focused dashboard tests after relevant
changes. Non-ML Docker builds/import smokes were verified locally. A full live pipeline run still needs
an agreed non-operational configuration; it has not been claimed or performed against deployment data.
Short-term/backend harmonization remains a tracked follow-up, not an added rollout task here.
