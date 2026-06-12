## Long-horizon quarterly hydrograph write fails — API rejects `horizon_type="quarter"` (PREPQ-008)

**Status**: Investigated & resolved 2026-06-12 — root cause confirmed and fix applied (see **Resolution update** below; the original analysis is retained for the record)
**Module**: `apps/preprocessing_runoff` (writer) + `sapphire/services/preprocessing` (deployed schema/DB — colleague-managed) + `sapphire-api-client` (write-path Literals — external repo)
**Priority**: **High** (deployment-blocking — the maintenance run aborts and the quarterly long-term norm hydrograph is never written)
**Labels**: `preprocessing-runoff`, `long-horizon`, `enum`, `schema-drift`, `deployment-blocker`
**Discovered**: 2026-06-12 during the `preprocessing_runoff (maintenance)` pipeline run.
**Related**:
- **MIG-003** ([`high_prio_gi_draft_migration_horizon_type_case_coercion.md`](high_prio_gi_draft_migration_horizon_type_case_coercion.md)) — same `horizon_type` enum-mismatch bug class.
- **FD-015** (archived) — quarter/season long-horizon skill/hydrograph display; this is the upstream data source for the quarterly cards, so the dashboard quarterly view has no data wherever this write fails.
- Source enum authority: `sapphire/services/preprocessing/app/models.py:6-14` (`HorizonType`, includes `QUARTER = "quarter"`).
- Migration authority: `sapphire/services/preprocessing/alembic/versions/d4e5f6a7b8c9_add_quarter_to_horizontype.py` (adds `QUARTER` PG enum label; revises `9f1e72108f01`).

---

## Resolution update (2026-06-12)

Investigation (`doc/plans/working/runoff_quarter_horizon_type_investigation_plan.md`) **refined
the root cause**: it was **not** a stale image or an unapplied migration. On the local stack the
preprocessing-api runs from a **bind mount** with source/DB already current (enum + migration
present), but the `uvicorn` worker had started *before* the source gained `QUARTER` and runs
without `--reload` — so it served a stale in-memory schema. `docker restart
sapphire-preprocessing-api` cleared the 422 (confirmed: the live OpenAPI `HorizonType` enum now
includes `quarter`).

Work completed:
- **Ops fix (local):** restart — done, verified.
- **Defensive hardening (P1):** the writer now degrades gracefully on per-station API read/write
  failures instead of aborting the whole run — branch `fix_runoff_long_horizon_degrade_gracefully`,
  commit `8ef2b0b`.
- **Collaborator remediation runbook:** `doc/prod/remediate_quarter_horizon_type_422.md` covers
  local (bind-mount restart), servers (pull updated `mabesa/sapphire-preprocessing:latest`), and
  the image-owner rebuild/push gate.
- **Deferred follow-up:** api-client `quarter` Literal hygiene → INFRA-019.

Server deployments (which pull `mabesa/sapphire-preprocessing:latest`) still need the
image-owner rebuild+push sequence in the runbook before they are confirmed clear; the local
bind-mount diagnosis does not by itself prove server state.

---

## Summary

The long-horizon hydrograph writer emits records with `horizon_type="quarter"`, but the **deployed** preprocessing API rejected them with a `422` validation error whose accepted set is `'day', 'pentad', 'decade', 'month', 'season'` and `'year'` — **note: `quarter` is absent, but `year` is present**. The whole maintenance run aborts at batch 1/1, so the quarterly long-term norm hydrograph is never persisted for any station.

This is **not a logic bug in the writer** — current source already supports `quarter` (enum value added in commit `2be58f7` "Add QUARTER to preprocessing HorizonType (match postprocessing)", plus Alembic migration `d4e5f6a7b8c9`). The defect is **schema/deployment drift**: the running preprocessing service image and/or its database predate that change, so the deployed FastAPI request schema does not yet accept `quarter`.

---

## Live evidence (2026-06-12)

```
--- preprocessing_runoff (maintenance) ---
ERROR - Batch 1 failed: API request failed (422): [{'type': 'enum',
  'loc': ['body', 'data', 0, 'horizon_type'],
  'msg': "Input should be 'day', 'pentad', 'decade', 'month', 'season' or 'year'",
  'input': 'quarter', ...}, ...]
ERROR - Unexpected error during long-horizon monthly hydrograph ingestion:
  Failed at batch 1/1: API request failed (422): ...
  File ".../sync_long_horizon_hydrograph.py", line 449, in main
    records = write_long_horizon_hydrograph(...)
  File ".../sync_long_horizon_hydrograph.py", line 368, in write_long_horizon_hydrograph
    write_station_quarterly_hydrograph(...)
  File ".../sync_long_horizon_hydrograph.py", line 332, in write_station_quarterly_hydrograph
    client.write_hydrograph(records)
  sapphire_api_client.client.SapphireAPIError: Failed at batch 1/1: API request failed (422): ...
```

The `422` is FastAPI **request-body** validation, returned by the deployed service — i.e. the running image's Pydantic schema, not the local source.

---

## Root cause (to confirm during development)

Three layers can each carry a stale `horizon_type` definition; the live `422` proves at least the deployed service is behind:

1. **Writer** — `apps/preprocessing_runoff/sync_long_horizon_hydrograph.py:300` (`build_quarterly_records`) sets `"horizon_type": "quarter"`. This is correct against current source.
2. **Deployed preprocessing service** — the running image rejects `quarter`. Source `models.py` already has `QUARTER`, so the deployed image predates commit `2be58f7`. **Suspected primary cause.**
3. **Live preprocessing DB** — even with a fresh image, the PG `horizontype` enum needs the `QUARTER` label. Migration `d4e5f6a7b8c9_add_quarter_to_horizontype` (`ALTER TYPE horizontype ADD VALUE IF NOT EXISTS 'QUARTER' BEFORE 'SEASON'`) must be applied. Confirm whether `alembic upgrade head` reached this revision on the affected deployment.
4. **`sapphire-api-client`** (pin `7bd349172ef24576b654a7b78f38734de3f2e657`) — the write-path Literals are **inconsistent**: `postprocessing_base.py:99` includes `quarter`, but `postprocessing.py:94` and `short_term.py:113` list only `day, pentad, decade, month, season, year` (no `quarter`). The hydrograph write path did not block client-side (the request reached the server), but this inconsistency should be reconciled so the client validates uniformly.

---

## Impact

- Maintenance `preprocessing_runoff` run aborts (`main()` re-raises) at the quarterly batch.
- Monthly and seasonal long-horizon records for the in-progress station may already be written before the abort, leaving a **partial** hydrograph (no quarterly rows) and no records at all for stations after the failure point.
- Dashboard quarterly long-horizon cards (FD-015 territory) have no data on any deployment still running the stale image.

---

## Proposed fix direction (develop later)

> Ownership note: `sapphire/services/` and the Alembic migration are colleague-managed — coordinate, do not edit directly. The `sapphire-api-client` is an external repo.

1. **Diagnose the affected deployment**: capture the deployed `sapphire-preprocessing` image build SHA (does it include `2be58f7`?) and probe the live enum:
   `SELECT enumlabel FROM pg_enum WHERE enumtypid='horizontype'::regtype ORDER BY enumsortorder;`
2. **Rebuild + redeploy** the preprocessing service from source at/after `2be58f7`, and run `alembic upgrade head` so migration `d4e5f6a7b8c9` lands on the live preprocessing DB.
3. **Reconcile the api-client Literals** to include `quarter` consistently across write paths, then re-pin (see [sapphire-api-client re-pin procedure] — touches ~17 files / 10 modules).
4. **Decide error-handling policy** in `sync_long_horizon_hydrograph.py`: should a quarterly batch failure abort the entire maintenance run, or should the run degrade gracefully (monthly/seasonal already written) and surface the quarterly failure as a non-fatal warning? Current behavior aborts everything.
5. **Add a regression test** (preprocessing_runoff) that a quarterly hydrograph record round-trips against the current schema, so writer ⇆ schema drift is caught in CI rather than in production.

---

## Acceptance criteria

- [ ] Root cause confirmed (deployed image SHA + live `horizontype` enum labels documented).
- [ ] Quarterly hydrograph records (`horizon_type="quarter"`) are accepted by the target deployment's API and persisted.
- [ ] Maintenance `preprocessing_runoff` run completes without the `422` abort.
- [ ] `sapphire-api-client` write-path Literals are consistent for `quarter` (or a decision recorded if intentionally divergent).
- [ ] Regression test added and passing under `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff`.
- [ ] Error-handling policy for partial long-horizon writes decided and implemented.
