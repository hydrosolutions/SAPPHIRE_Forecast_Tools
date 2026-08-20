# INFRA-035: Legacy dashboard healthchecks curl endpoints that are never served

**Status**: Draft
**Module**: infra — `bin/docker-compose-dashboards.yml`
**Priority**: Medium (no wrong forecast data; it destroys the signal you need during an outage)
**Labels**: `bug`, `deployment`, `dashboard`
**Found by**: out-of-loop review of INFRA-032 (codex, 2026-08-20). Verified independently.

---

## Summary

Both services in `bin/docker-compose-dashboards.yml` declare a healthcheck against a
path the Panel server does not serve, so both containers are reported **unhealthy for
their whole lifetime** even when the dashboard is working perfectly.

## Evidence

| Service | Healthcheck URL | Actually served |
|---|---|---|
| `pentaddashboard` | `http://localhost:5006/pentad_dashboard` (`:72`) | `/forecast_dashboard` |
| `decaddashboard` | `http://localhost:5007/decad_dashboard` (`:118`) | `/forecast_dashboard` |

Both services run `forecast_dashboard.py` (`:55`, `:101`). `panel serve` derives the
route from the script name, so the only application route is `/forecast_dashboard`.
`sapphire/docker-compose.yml:254` has it right — it curls `/forecast_dashboard` — which
is what makes this look like leftover drift from a rename rather than a deliberate
difference. `doc/deployment.md:722-724` also verifies `/forecast_dashboard`.

## Why it matters

A permanently-unhealthy container is worse than no healthcheck. It trains whoever reads
`docker compose ps` to ignore the health column on these services, which is exactly the
column that would otherwise reveal a real failure — and both services are
`restart: always` (`:59`, `:105`), so a genuinely broken dashboard presents as a restart
loop that the health column was supposed to make obvious.

This is the same species as the INFRA-032 finding that a malformed origin value produces
a silent permanent restart loop rather than a loud failure: **the check that looks like
it proves health cannot fail.**

## Desired outcome

Both healthchecks target `/forecast_dashboard` on their respective ports, and
`docker compose ps` shows `healthy` for a working dashboard.

## Acceptance criteria

- `docker compose -f bin/docker-compose-dashboards.yml up -d decaddashboard`, wait for
  the interval, and `docker compose ps` reports **healthy**, not `unhealthy`.
- The same check on a deliberately broken configuration (e.g. an empty origin value)
  reports **unhealthy** — proving the check can still fail, which is the whole point.

## Note for whoever picks this up

Confirm against the Panel version actually pinned in
`apps/forecast_dashboard/uv.lock` that the route is `/forecast_dashboard` and that no
`--prefix` argument is in play, rather than assuming it from the filename.
