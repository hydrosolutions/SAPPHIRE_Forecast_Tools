# Remediation: `horizon_type="quarter"` rejected with HTTP 422 (PREPQ-008)

**Audience**: deployment sysadmins (Tajik / Kyrgyz / Uzbek / ZRH), the services/image owner, and developers running local stacks.
**Last updated**: 2026-06-12

## Symptom

The `preprocessing_runoff` maintenance run (long-horizon hydrograph ingestion) aborts with:

```
ERROR - Batch 1 failed: API request failed (422): [{'type': 'enum',
  'loc': ['body', 'data', 0, 'horizon_type'],
  'msg': "Input should be 'day', 'pentad', 'decade', 'month', 'season' or 'year'",
  'input': 'quarter', ...}]
```

The accepted set advertised by the API is **missing `quarter`**. The writer
(`sync_long_horizon_hydrograph.py`) correctly emits `horizon_type="quarter"`; the
**deployed preprocessing API is behind the source**, which already supports it
(enum value added in commit `2be58f7`; PG enum label added by Alembic migration
`d4e5f6a7b8c9`).

Effect: the whole maintenance run aborts, so the quarterly long-term norm hydrograph is
never written (and the dashboard quarterly cards have no data).

---

## Step 1 — Detect (run on the affected deployment)

Adjust container/service names and DB name to your compose project if they differ.

**1a. What enum does the *running* API actually serve?** (the authoritative check)

Run it *inside* the API container with `python3` (the image ships Python but not `curl`),
and locate the enum by content rather than assuming its schema key name:

```bash
docker exec sapphire-preprocessing-api python3 -c "import json,urllib.request; \
s=json.load(urllib.request.urlopen('http://localhost:8002/openapi.json'))['components']['schemas']; \
[print(k,'=',v['enum'],'| quarter present:', 'quarter' in v['enum']) \
 for k,v in s.items() if v.get('enum') and 'pentad' in v['enum']]"
```

(If the API port is published to the host, `curl -fsS http://localhost:8002/openapi.json`
from the host works too.)

- `quarter present: True` → the running API is fine; the 422 is something else.
- `quarter present: False` → **affected**. Continue.

**1b. Does the database enum have the label?**

```bash
PGUSER=$(docker exec sapphire-preprocessing-db printenv POSTGRES_USER)
docker exec sapphire-preprocessing-db psql -U "$PGUSER" -d preprocessing_db -tAc \
  "SELECT enumlabel FROM pg_enum WHERE enumtypid='horizontype'::regtype ORDER BY enumsortorder"
```

Expect `QUARTER` to appear (between `MONTH` and `SEASON`). If absent, the migration has
not been applied.

**1c. Is Alembic at/after the migration?**

```bash
docker exec sapphire-preprocessing-api alembic current   # expect d4e5f6a7b8c9 (or later)
```

**Probe the *same* service the writer hits.** `sync_long_horizon_hydrograph.py` writes via
`SAPPHIRE_API_URL` (default the api-gateway on `:8000`), which routes to the preprocessing
service. If your deployment runs more than one preprocessing instance, make sure Step 1a
probes the instance behind that gateway — probing a different container can give a
misleading result.

**Interpreting the signals (mixed states):**

| 1a (API serves `quarter`) | 1b (DB has `QUARTER`) | What it means → go to |
|---|---|---|
| No | Yes | API process/image is behind the DB → **Branch A** (bind mount) or **Branch B** (baked image) |
| No | No | Both API and DB are behind → **Branch B**, and ensure the migration runs (auto on redeploy, or apply manually) |
| Yes | No | API accepts `quarter` but the **write will still fail at the DB** with `invalid input value for enum horizontype` (a different error, not a 422) → apply the migration: `docker exec sapphire-preprocessing-api alembic upgrade head` |
| Yes | Yes | Not this issue — investigate the specific 422 payload |

---

## Step 2 — Fix (pick the branch that matches your deployment)

### Branch A — Local / dev stack running from a BIND MOUNT (stale worker)

Symptom signature: source `models.py` and the DB enum are CORRECT (1b shows `QUARTER`,
1c is at head), but 1a is missing `quarter`. The uvicorn worker started before the
mounted source was updated and runs without `--reload`, so it holds the old schema in
memory.

```bash
docker restart sapphire-preprocessing-api
```

Then re-run Step 1a — `quarter` should now appear. Done.

### Branch B — Server running the promoted image `mabesa/sapphire-preprocessing:latest`

All hydromet deployments (Tajik / Kyrgyz / Uzbek / ZRH) run the **pulled** image
`mabesa/sapphire-preprocessing:latest` — they do not build locally.

Because `latest` is a single moving tag, remediation is a **two-step sequence across two
actors, in order**:

1. **Image owner — FIRST (once, see Branch C):** build `mabesa/sapphire-preprocessing` from
   source at/after commit `2be58f7` and push it to `:latest`. Until the registry's `latest`
   is updated, a `docker compose pull` on a server just re-fetches the same stale image — so
   this step must happen before any server pulls.
2. **Each server — AFTER the new `latest` is pushed:**

   ```bash
   cd <deployment>/sapphire && docker compose pull preprocessing-api \
     && docker compose up -d preprocessing-api
   ```

   This pulls the updated `latest` and recreates the container. If the image entrypoint runs
   `alembic upgrade head` on start (verify for the build), the migration `d4e5f6a7b8c9` is
   applied automatically; otherwise apply it explicitly:

   ```bash
   docker exec sapphire-preprocessing-api alembic upgrade head
   ```

3. **Verify on each server:** re-run Step 1a (API serves `quarter`) and Step 1b (DB enum has
   `QUARTER`). Since the tag name never changes, confirm you actually pulled the new build by
   comparing the image digest / created date before and after:

   ```bash
   docker image inspect mabesa/sapphire-preprocessing:latest --format '{{.Id}} {{.Created}}'
   ```

> `latest` carries no version information, so the tag alone cannot tell you whether a server
> has the fix — always confirm with Step 1a plus the digest/created date, never the tag name.

> Do not hand-edit the PG enum on a live DB. Use the Alembic migration
> (`ALTER TYPE horizontype ADD VALUE IF NOT EXISTS 'QUARTER' BEFORE 'SEASON'`), which is
> idempotent, so it is safe to re-run.

### Branch C — Image owner (rebuild + push `latest`) — DO THIS FIRST

Servers can only get the fix once `mabesa/sapphire-preprocessing:latest` in the registry
points at a fixed build. Before pushing, confirm the image bakes in the fix:

- Source includes commit `2be58f7` ("Add QUARTER to preprocessing HorizonType").
- The migration file `sapphire/services/preprocessing/alembic/versions/d4e5f6a7b8c9_add_quarter_to_horizontype.py`
  is present in the image.
- Post-build smoke check: start the image and run Step 1a — the served enum must include
  `quarter`.

Then build and push `:latest`, and notify the deployment sysadmins to run Branch B step 2.
(Note: `latest` is mutable and unversioned; when the canonical versioned tag — e.g.
`:v1.0.0` — is adopted, prefer pinning servers to it so "which build is deployed" is
unambiguous.)

---

## Step 3 — Verify the operational fix

```bash
# 1) API now accepts quarter (Step 1a shows it), and
# 2) optionally re-run the maintenance job and confirm no 422:
#    (needs the iEH HF tunnel + the deployment's env)
cd apps/preprocessing_runoff && uv run python sync_long_horizon_hydrograph.py --target-year <year>

# 3) confirm quarter rows landed (use a non-sensitive sample code such as 19999):
PGUSER=$(docker exec sapphire-preprocessing-db printenv POSTGRES_USER)
docker exec sapphire-preprocessing-db psql -U "$PGUSER" -d preprocessing_db -tAc \
  "SELECT horizon_type, COUNT(*) FROM hydrographs WHERE horizon_type = 'QUARTER' GROUP BY horizon_type"
```

> The `horizontype` PG enum stores **UPPERCASE** labels (`DAY`, `PENTAD`, …, `QUARTER`,
> `SEASON`, `YEAR`) — SQLAlchemy persists the enum *name*, not the lowercase `.value` the API
> accepts. So raw SQL must compare against `'QUARTER'` (or `horizon_type::text = 'QUARTER'`);
> a lowercase `'quarter'` literal raises `invalid input value for enum horizontype`. Same
> gotcha as MIG-003.

Re-runs are safe: hydrograph writes are idempotent upserts, so re-running completes any
rows skipped by an earlier aborted run.

---

## Related defensive change (PREPQ-008 P1)

Branch `fix_runoff_long_horizon_degrade_gracefully` (commit `8ef2b0b`) makes the writer
**degrade gracefully**: a per-station API read/write failure now logs a WARNING and
continues to the next station instead of aborting the whole run. Exit-code behavior after
that lands:

- **exit 0** — at least one station completed all its writes (partial per-station failures
  are logged as warnings).
- **exit 2** — every *attempted* station failed its API read/write (or there was nothing to
  write). Check the logs to disambiguate.

This hardening does **not** replace fixing the schema drift above — it only prevents one
station's (or a systemic) failure from silently aborting the entire maintenance run.

## References

- Issue: `doc/plans/issues/high_prio_gi_draft_runoff_quarter_horizon_type_rejected.md` (PREPQ-008)
- Investigation: `doc/plans/working/runoff_quarter_horizon_type_investigation_plan.md`
- api-client `quarter` Literal hygiene follow-up: INFRA-019
  (`doc/plans/issues/mid_prio_gi_draft_infra_api_client_quarter_literal_consistency.md`)
- Same `horizon_type` enum-case gotcha in migration-toolkit SQL: MIG-003
  (`doc/plans/issues/high_prio_gi_draft_migration_horizon_type_case_coercion.md`)
