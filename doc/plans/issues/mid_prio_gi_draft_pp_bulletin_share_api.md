# Bulletin Share API — snapshot storage + public token endpoint

**Status**: Draft
**Module**: sapphire/services/postprocessing + api-gateway
**Priority**: Medium
**Labels**: `feature`, `services`, `postprocessing`, `api-gateway`

---

## Summary

Add a `bulletin_share` table plus two endpoints — an internal `POST /bulletin/share`
that stores a frozen bulletin snapshot and mints a capability token, and a
**public** `GET /public/bulletin/{token}` that returns that snapshot until it
expires — so the dashboard can hand third parties a shareable JSON link.

## Context

Third-party organizations need to retrieve SAPPHIRE bulletin data in JSON
without a SAPPHIRE account. The design (see
`doc/plans/publish_bulletin_api_design.md`) is: the **dashboard assembles** a
full Excel-equivalent snapshot per horizon (the computed fields — % of norm,
volumes, norms, header dates — do not exist in the `bulletins` table; they are
computed dashboard-side), POSTs it here, and receives one shareable URL per
horizon. Each link is a **frozen snapshot** that **expires when the next period
of its horizon begins**.

This issue is the **service side** of that feature. The dashboard side is a
separate issue (`mid_prio_gi_draft_fd_publish_bulletin.md`) and depends on the
contract defined here.

## Problem

There is no storage for a point-in-time bulletin snapshot, no token concept for
external consumers, and no public (unauthenticated-by-key) route. Today the only
external auth is the gateway's single static `X-API-Key` (disabled by default),
and everything else is interactive JWT login.

## Desired Outcome

- A `bulletin_share` row can be created via `POST /bulletin/share`, returning
  `{token, url, expires_at}`.
- `GET /public/bulletin/{token}` returns the stored JSON payload when
  `now < expires_at`, `410 Gone` when expired, `404` when unknown.
- The public GET is reachable through the gateway **without** `X-API-Key`.
- Migration creates the table; service tests cover create/read/expiry/unknown.

---

## Technical Analysis

### Current Implementation

**postprocessing service** (`sapphire/services/postprocessing`):
- Models: `app/models.py:241-274` — `Bulletin` (table `bulletins`). Imports at
  `models.py:1-6` (`Column, Date, Float, Index, Integer, String, UniqueConstraint`,
  `Enum as SQLEnum`). **No `func`, no JSON column type imported anywhere in the
  repo** — a JSON column needs the generic `from sqlalchemy import JSON` (works on
  both Postgres and the SQLite test DB; do NOT use `postgresql.JSONB` or the
  in-memory tests break).
- Base/session: `app/database.py:8-10` (`engine`, `SessionLocal`,
  `Base = declarative_base()`); `get_db()` dependency at `app/database.py:13-22`.
- Config: `app/config.py:1-15` — `Settings(BaseSettings)`, module-level
  `settings = Settings()`. Required settings must also be set in
  `tests/conftest.py:14-19` before app import.
- Endpoints (decorators directly on `app`, no APIRouter): `app/main.py` — POST
  `/bulletin/` `250-259`, GET `/bulletin/` `262-286`, DELETE `/bulletin/`
  `289-317`. Error convention: business errors → `HTTPException` with
  `status.HTTP_*`; DB errors caught as `SQLAlchemyError` → 500. 404 pattern at
  `main.py:308-310`.
- Schemas: `app/schemas.py:204-234` — `BulletinBase/Create/BulkCreate/Response`
  (`class Config: from_attributes = True`).
- CRUD: `app/crud.py` — `create_bulletin` `384-433`, `get_bulletin` `435-458`.
- **Migrations = Alembic** (no `create_all` in app code):
  `alembic.ini`, `alembic/env.py` (`target_metadata = Base.metadata`,
  `compare_type=True`), versions in `alembic/versions/`. Applied at container
  start: `Dockerfile:18` (`alembic upgrade head && uvicorn ...`).
  **Current head**: `a1b2c3d4e5f6_add_horizon_value_to_skill_metrics.py`
  (`revision='a1b2c3d4e5f6'`). Mirror the single-purpose migration shape in that
  file (`op.create_table`, `op.create_index`, `upgrade()`/`downgrade()`).
- Tests: `tests/conftest.py` — env vars set `14-19`, in-memory SQLite +
  `StaticPool` `42-49`, autouse `create_all/drop_all` `56-61`, `db_session`
  `64-71`, `client` (TestClient + `dependency_overrides[get_db]`) `74-87`.
  Mirror `tests/test_endpoints.py:395-471` (`TestBulletinEndpoints`).
  Run: `bash apps/run_tests.sh service:postprocessing`.

**api-gateway** (`sapphire/services/api-gateway`):
- `verify_api_key` dependency: `app/main.py:51-59` (disabled unless
  `API_KEY_ENABLED`).
- `proxy_request` helper: `app/main.py:62-120` (note `follow_redirects=True` at
  `:83` — the gateway follows downstream 3xx rather than passing it through).
- Public precedent (no key): `auth_proxy` at `app/main.py:224-227` — omits the
  `Depends(verify_api_key)` dependency.
- Config: `app/config.py` (`settings = Settings()` `:48`); `SERVICES`/
  `get_service_url` `27-45`.

### Root Cause (what's missing)

No snapshot table, no token endpoints, no public gateway route.

---

## Implementation Plan

### Approach

Store the dashboard-assembled snapshot verbatim as a JSON blob keyed by an
opaque token, with a server-side `expires_at`. The service does **not** compute
or reshape the payload — it stores and serves it. The public GET is the only
security-relevant surface and is guarded solely by the high-entropy token +
expiry (capability-URL model, per design). The internal `POST /bulletin/share`
rides the existing `/api/postprocessing/` proxy — same posture as every current
bulletin write (no per-consumer auth in MVP; documented as a known limitation).

### Files to Create

| File | Purpose |
|------|---------|
| `sapphire/services/postprocessing/alembic/versions/<rev>_add_bulletin_share.py` | Migration creating `bulletin_share` (`down_revision='a1b2c3d4e5f6'`) |

### Files to Modify

| File | Changes |
|------|---------|
| `postprocessing/app/models.py` | Add `BulletinShare` model + `from sqlalchemy import JSON` (and `func`, `DateTime` for timestamps) |
| `postprocessing/app/schemas.py` | Add `BulletinShareCreate` (request) + `BulletinShareCreateResponse` (`{token,url,expires_at}`) + `BulletinSharePublicResponse` (the payload wrapper) |
| `postprocessing/app/crud.py` | `create_bulletin_share(db, data) -> BulletinShare` (mint token, insert) and `get_bulletin_share_by_token(db, token) -> BulletinShare | None` |
| `postprocessing/app/main.py` | `POST /bulletin/share` and `GET /public/bulletin/{token}` routes |
| `postprocessing/app/config.py` | Add `public_bulletin_base_url: str` (for building the returned URL) |
| `postprocessing/tests/conftest.py` | Add `PUBLIC_BULLETIN_BASE_URL` env var (line ~14-19) if the setting is required |
| `api-gateway/app/main.py` | Add public `GET /public/bulletin/{token}` passthrough (no `verify_api_key`) |

### Implementation Steps

- [ ] Step 1: Add `BulletinShare` model in `models.py` (columns below); import
  `JSON`, `DateTime`, `func`.
- [ ] Step 2: Generate an Alembic revision (`down_revision='a1b2c3d4e5f6'`)
  creating `bulletin_share` with a unique index on `token`.
- [ ] Step 3: Add `public_bulletin_base_url` to `config.py`; add the env var to
  `tests/conftest.py`.
- [ ] Step 4: Add Pydantic schemas in `schemas.py`.
- [ ] Step 5: Add `create_bulletin_share` + `get_bulletin_share_by_token` in
  `crud.py` (token via `secrets.token_urlsafe(32)`; retry once on unique clash).
- [ ] Step 6: Add the two routes in `main.py` (410 when expired, 404 when
  unknown — mirror `main.py:308-310`).
- [ ] Step 7: Add the public passthrough in `api-gateway/app/main.py`.
- [ ] Step 8: Tests for both services.

### Code Examples

Model (`models.py`):
```python
from sqlalchemy import JSON, DateTime, func  # add to existing imports

class BulletinShare(Base):
    __tablename__ = "bulletin_share"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    token = Column(String(64), nullable=False, unique=True, index=True)
    horizon_type = Column(SQLEnum(HorizonType), nullable=False)
    year = Column(Integer, nullable=False)
    horizon_value = Column(Integer, nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    payload = Column(JSON, nullable=False)          # frozen Excel-equivalent snapshot
    station_codes = Column(JSON, nullable=True)     # reference only
```

Schemas (`schemas.py`):
```python
class BulletinShareCreate(BaseModel):
    horizon: HorizonType
    year: int
    horizon_value: int
    expires_at: datetime
    payload: dict
    station_codes: list[str] | None = None

class BulletinShareCreateResponse(BaseModel):
    token: str
    url: str
    expires_at: datetime

class BulletinSharePublicResponse(BaseModel):
    # returned verbatim to third parties
    payload: dict
    expires_at: datetime
```

Routes (`main.py`):
```python
@app.post("/bulletin/share", response_model=BulletinShareCreateResponse,
          status_code=status.HTTP_201_CREATED, tags=["Bulletin"])
def create_bulletin_share(body: BulletinShareCreate, db: Session = Depends(get_db)):
    try:
        rec = crud.create_bulletin_share(db, body)
    except SQLAlchemyError:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "…")
    url = f"{settings.public_bulletin_base_url.rstrip('/')}/public/bulletin/{rec.token}"
    return BulletinShareCreateResponse(token=rec.token, url=url, expires_at=rec.expires_at)

@app.get("/public/bulletin/{token}", tags=["Public"])
def get_public_bulletin(token: str, db: Session = Depends(get_db)):
    rec = crud.get_bulletin_share_by_token(db, token)
    if rec is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Not found")
    # compare tz-aware; treat naive stored value as UTC
    now = datetime.now(timezone.utc)
    exp = rec.expires_at if rec.expires_at.tzinfo else rec.expires_at.replace(tzinfo=timezone.utc)
    if now >= exp:
        raise HTTPException(status.HTTP_410_GONE, "Link expired")
    return rec.payload            # the frozen snapshot, verbatim
```

Gateway passthrough (`api-gateway/app/main.py`, before the generic
`/api/postprocessing/{path:path}` route):
```python
@app.api_route("/public/bulletin/{token}", methods=["GET"], tags=["Public"])
async def public_bulletin_proxy(token: str, request: Request):
    return await proxy_request(SERVICES["postprocessing"], f"/public/bulletin/{token}",
                               request, request.method)
```

---

## Testing

### Test Cases (postprocessing — mirror `tests/test_endpoints.py:395-471`)

- [ ] `POST /bulletin/share` returns 201 with `token`, `url` (contains
  `public_bulletin_base_url` + token), and echoed `expires_at`; a row exists.
- [ ] Token is unique across two creates.
- [ ] `GET /public/bulletin/{token}` returns the exact stored `payload` when not
  expired.
- [ ] Returns `410` when `expires_at` is in the past (create a row with a past
  timestamp via `db_session`).
- [ ] Returns `404` for an unknown token.
- [ ] CRUD unit tests for `create_bulletin_share` / `get_bulletin_share_by_token`
  (`db_session` fixture).

### Test Cases (api-gateway — mirror `api-gateway/tests/test_endpoints.py`)

- [ ] `/public/bulletin/{token}` reaches the postprocessing proxy target and does
  NOT require `X-API-Key` (even with `API_KEY_ENABLED=true`).

### Testing Commands

```bash
bash apps/run_tests.sh service:postprocessing
# api-gateway is NOT in run_tests.sh SERVICE_MODULES — run its suite directly:
cd sapphire/services/api-gateway && .venv/bin/pytest tests/ -v
```

### Manual Verification

```bash
cd sapphire && docker-compose up -d
# create a share (through the gateway, same proxy as other writes):
curl -X POST http://localhost:8000/api/postprocessing/bulletin/share \
  -H 'Content-Type: application/json' \
  -d '{"horizon":"pentad","year":2026,"horizon_value":26,"expires_at":"2999-01-01T00:00:00Z","payload":{"stations":[]}}'
# open the returned public URL:
curl http://localhost:8000/public/bulletin/<token>
```

---

## Documentation Impact

- [x] `CLAUDE.md` — noted the public `/public/bulletin/{token}` route, the
  `bulletin_share` table, and the required `PUBLIC_BULLETIN_BASE_URL` env var
  under the services table.
- [x] Configuration docs (`doc/configuration.md`) — added `PUBLIC_BULLETIN_BASE_URL`
  to the services-side variable table (Optional; default `http://localhost:8000`).
- [x] `doc/prod/update_deployment_checklist.md` — noted `PUBLIC_BULLETIN_BASE_URL`
  as a new optional var to set for externally-reachable share links.
- [x] `.env.example` for postprocessing — added `PUBLIC_BULLETIN_BASE_URL`
  (created the file; api-gateway needed no new setting).
- [ ] `doc/data_flow_*.md` — deferred; bulletin sharing is a dashboard-triggered
  export, not part of the pipeline data flow. Revisit with FD-017 if useful.
- [ ] Claude memory — hold until merged/deployed, then note the public
  capability-URL route + token model.

---

## Out of Scope

- Per-consumer / per-organization tokens and scoping (none exists in the user
  model).
- Manual revoke / list-active-links endpoints (generate-only MVP).
- Rate limiting implementation (the gateway `RATE_LIMIT*` settings are stubs).
  **Recommended** for an internet-facing route — track as a follow-up.
- Computing/reshaping the payload server-side (dashboard owns payload shape).

## Dependencies

- None blocking. The dashboard issue depends on THIS contract (table + endpoint
  shapes + `expires_at` semantics + `url` format).

## Acceptance Criteria

- [ ] `bulletin_share` table created via Alembic migration
  (`down_revision='a1b2c3d4e5f6'`); `alembic upgrade head` succeeds.
- [ ] `POST /bulletin/share` → 201 `{token, url, expires_at}`.
- [ ] `GET /public/bulletin/{token}` → payload (live), 410 (expired), 404
  (unknown).
- [ ] Public GET reachable through the gateway without `X-API-Key`.
- [ ] `PUBLIC_BULLETIN_BASE_URL` config wired (postprocessing) + in test conftest.
- [ ] `bash apps/run_tests.sh service:postprocessing` passes; gateway suite passes.
- [ ] No station codes / discharge values committed in code, fixtures, or docs.

---

## References

- Design: `doc/plans/publish_bulletin_api_design.md`
- Dashboard companion issue: `doc/plans/issues/mid_prio_gi_draft_fd_publish_bulletin.md`
- Mirror examples: `postprocessing/app/main.py:250-317`,
  `postprocessing/tests/test_endpoints.py:395-471`,
  `api-gateway/app/main.py:224-227`,
  `postprocessing/alembic/versions/a1b2c3d4e5f6_add_horizon_value_to_skill_metrics.py`
