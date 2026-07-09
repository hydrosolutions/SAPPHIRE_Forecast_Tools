# Publish Bulletin API — Design

**Status:** Design (validated) — awaiting colleague coordination on the `sapphire/services/` API contract before implementation.

**Author:** brainstormed with Maxat, 2026-07.

## 1. Goal

From the forecast dashboard, a user selects one or more **horizons** and a
subset of **stations**, clicks **"Generate links"**, and receives **one
shareable URL per horizon**. Each URL returns a **frozen JSON snapshot** of the
full Excel-equivalent bulletin data for the selected stations in that horizon.
Each link **auto-expires when the next period of its horizon begins** (e.g. a
pentad link dies when the next pentad starts; a month link when the next month
starts).

The URLs are handed to third-party organizations so they can retrieve the
bulletin data in JSON without a SAPPHIRE account.

## 2. Key decisions (validated)

| Decision | Choice |
|----------|--------|
| Data semantics | **Frozen snapshot** captured at generation time — not live. |
| Validity window | Link **invalidated when the next period of its horizon starts** (server-enforced `expires_at`). |
| Payload content | **Everything the Excel bulletin contains** (raw fields + computed % of norm, volumes, norms, header/period dates). |
| Authentication | **Secret link** — token embedded in the URL (capability URL). No headers needed by the third party. |
| Storage/serving | **Backend service** — new endpoints + table in colleague-managed `sapphire/services/`. |
| Link management | **Generate-only** for MVP. No list/revoke UI (YAGNI). |
| Missing station data | **Omit + report skipped** — drop stations with no data for a horizon, show the user which were skipped. |
| Who can generate | Any authenticated dashboard user (no per-org scoping — none exists today). |

## 3. Architecture

The feature spans two ownership domains.

### 3.1 `apps/forecast_dashboard` (fair game)

- **UI**: horizon multi-select + station multi-select + "Generate links" button
  + a results panel listing the generated URLs (one per horizon) to copy, plus
  a "skipped stations" report.
- **Snapshot assembly**: reuse the *same* per-site pipeline that feeds
  `write_to_excel` / `_populate_forecast_attributes` (in
  `dashboard/bulletin_manager.py` and `src/site.py`), but serialize the full
  field set to a JSON payload instead of rendering Excel. This is essential:
  the computed Excel fields (% of norm, volumes, norm, header dates) **do not
  exist in the `bulletins` DB table** — they are computed dashboard-side.
- **Service client**: POST the assembled snapshot to the new share endpoint and
  receive `{token, url, expires_at}` (thin wrapper alongside `src/db.py`
  helpers).

### 3.2 `sapphire/services/` (colleague-managed — requires coordination)

> Per repo ownership rules, these are proposed contract changes to discuss with
> the service owner before any implementation. Do NOT edit service code directly.

**New table `bulletin_share`** (postprocessing DB):

| Column | Type | Notes |
|--------|------|-------|
| `id` | PK | autoincrement |
| `token` | String, unique, indexed | 32-byte URL-safe random |
| `horizon_type` | Enum `HorizonType` | reuse existing enum |
| `year` | Integer | |
| `horizon_value` | Integer | pentad/decade/month/season index |
| `expires_at` | DateTime (UTC) | start of next period for the horizon |
| `created_at` | DateTime (UTC) | |
| `payload` | JSON/JSONB | the frozen Excel-equivalent snapshot |
| `station_codes` | JSON/array | for reference/debugging |

**New endpoints (postprocessing `main.py`, following the existing decorator +
`crud.py` + `schemas.py` pattern):**

- `POST /bulletin/share` — internal, authenticated (behind existing gateway
  `X-API-Key`). Body: `{horizon, year, horizon_value, expires_at, payload,
  station_codes}`. Mints a high-entropy token, stores the record, returns
  `{token, url, expires_at}`.
- `GET /public/bulletin/{token}` — **public** (no `X-API-Key`). Returns the
  stored `payload` if `now < expires_at`; otherwise `410 Gone`. Unknown token →
  `404`.

**Gateway (`api-gateway/main.py`):** add a **public** passthrough route for
`/public/bulletin/{token}` that does NOT require `X-API-Key`, mirroring how
`/api/auth/{path}` is already public. All other new routes reuse the existing
`/api/postprocessing/{path}` proxy.

## 4. Data flow

1. User selects horizons + stations, clicks **Generate links**.
2. For each selected horizon, the dashboard:
   - Builds the `bulletin_sites` for the selected stations (same logic as
     `_on_write` / `_populate_forecast_attributes`).
   - Assembles the full Excel-equivalent JSON payload (station rows + period/
     header metadata).
   - Computes `expires_at` = start of next period for that horizon (forecast
     date passed in explicitly — see the Forecast Date Rule).
   - POSTs the snapshot to `POST /bulletin/share`, receives token + URL.
3. Dashboard displays the URLs (one per horizon) + the skipped-station report.
4. Third party GETs the public URL → JSON, until expiry.

## 5. Payload schema (per horizon link)

```json
{
  "horizon": "pentad",
  "year": 2026,
  "horizon_value": 26,
  "valid_from": "2026-05-06",
  "valid_to": "2026-05-10",
  "generated_at": "2026-05-05T09:00:00Z",
  "expires_at": "2026-05-11T00:00:00Z",
  "stations": [
    {
      "code": "…",
      "station_label": "…",
      "basin": "…",
      "river": "…",
      "model": "…",
      "forecasted_discharge": 0,
      "fc_lower": 0,
      "fc_upper": 0,
      "delta": 0,
      "sdivsigma": 0,
      "mae": 0,
      "accuracy": 0,
      "perc_norm": 0,
      "q_min": 0,
      "q_max": 0,
      "v_min": 0,
      "v_max": 0,
      "norm": 0
    }
  ]
}
```

Short-term horizons (pentad/decade) carry the short-term field set; month/season
additionally carry volumes (`v_min`/`v_max`) and `norm` — mirroring the Excel.
Exact field list per horizon to be finalized against `src/bulletins.py` tags and
`src/site.py` attributes during implementation.

## 6. Token & security

- Token: `secrets.token_urlsafe(32)`, stored server-side. URL is a capability
  link over HTTPS: `https://<gateway-host>/public/bulletin/<token>`.
- Expiry enforced **server-side** (`expires_at`), never client-trusted.
- **Risks / notes:**
  - Token-in-URL is a bearer capability — anyone with the link has access.
    Mitigated by the ≤ one-period lifetime + HTTPS. Do not log full URLs.
  - **Rate limiting is an unimplemented config stub** in the gateway
    (`RATE_LIMIT`/`RATE_LIMIT_ENABLED`). Recommend basic throttling on the
    public route — flagged for the colleague.
  - Generating a link deliberately exposes station codes + discharge to a third
    party. It is an intentional, permissioned action. (No per-org scoping in
    MVP; none exists in the user model today.)
  - `<gateway-host>` must be an internet-reachable HTTPS host. Confirm the
    public base URL per deployment (kyg/taj) before shipping.

## 7. Expiry computation

A per-horizon "start of next period" helper, reusing existing pentad/decade/
month boundary utilities in `iEasyHydroForecast` / the dashboard. The forecast
date is passed in explicitly (no `date.today()` in business logic — Forecast
Date Rule). Result stored as a UTC timestamp.

- pentad → start of next pentad
- decade → start of next decade
- month → first day of next month
- season → start of next season

## 8. Error handling

- Station with no bulletin data for a horizon → **omitted**; dashboard reports
  which stations were skipped for which horizons after generation.
- Horizon that yields zero stations → **no link generated** for it, with a
  warning to the user.
- Service unreachable / share POST fails → error popup; **no partial links**
  shown.
- Expired token → `410 Gone` (JSON error body). Unknown token → `404`.

## 9. Testing

**apps/ (this repo):**
- Payload assembly from `bulletin_sites` produces the full Excel-equivalent
  field set (unit; parametrized per horizon).
- Per-horizon `expires_at` computation (unit; parametrized; injected forecast
  date).
- Button handler produces N links for N selected horizons, skips empty
  horizons, and reports skipped stations (faked service client).
- Service-client wrapper serializes/POSTs correctly and surfaces failures.

**services/ (colleague, if/when they implement):**
- `POST /bulletin/share` returns token + `expires_at`; token is unique.
- `GET /public/bulletin/{token}` returns payload before expiry; `410` after
  expiry; `404` for unknown token.
- Public route reachable without `X-API-Key`; internal routes still gated.

## 10. Out of scope (MVP / YAGNI)

- Link management UI (list active links, manual revoke/regenerate before
  expiry).
- Per-organization / per-consumer tokens and scoping.
- Rate limiting implementation (recommended, but owned by the service side).

## 11. Coordination gate

Before implementation starts:
1. Share sections 3.2 and 5 with the `sapphire/services/` owner and agree the
   API contract (table, two endpoints, gateway public route, payload shape).
2. Confirm the public HTTPS base URL per deployment.

Once the contract is agreed, split into issue plans:
- **apps issue**: UI + payload assembly + expiry helper + service client +
  tests.
- **services issue** (colleague or coordinated): table + endpoints + gateway
  route + tests.
