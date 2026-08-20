# INFRA-034: `ALLOWED_WEBSOCKET_ORIGINS` is set in both compose files and read by nothing

**Status**: Draft
**Module**: infra — `sapphire/docker-compose.yml`, `bin/docker-compose-dashboards.yml`, `doc/deployment.md`
**Priority**: Medium (no wrong behaviour today; it misdirects operators during an outage)
**Labels**: `cleanup`, `deployment`, `dashboard`
**Found while**: mapping the origin allow-list for INFRA-032. Not fixed inline, per CLAUDE.md.

---

## Summary

`ALLOWED_WEBSOCKET_ORIGINS` is injected into the dashboard containers at three sites and
is **never read** — not by the container `command:`, not by any Python in
`apps/forecast_dashboard`, not by Bokeh. The origin arguments are built directly from
`ieasyhydroforecast_url_pentad` / `_decad` in the `command:` string.

## Evidence

Set at:

- `sapphire/docker-compose.yml:226`
- `bin/docker-compose-dashboards.yml:46` (pentad) and `:92` (decad)

Consumed at: nowhere. `grep -rn "ALLOWED_WEBSOCKET_ORIGINS"` across the repo returns
only those three assignments plus `doc/deployment.md:761`. The actual mechanism is the
`sed` pipeline at `sapphire/docker-compose.yml:231` and
`bin/docker-compose-dashboards.yml:50,96`, which reads the `ieasyhydroforecast_url_*`
variable and emits repeated `--allow-websocket-origin=` arguments.

Bokeh does support an env-var route — `BOKEH_ALLOW_WS_ORIGIN`, per
`bokeh/server/views/ws.py:118` — but it is spelled differently and is not set here.

## Why it is worth fixing

`doc/deployment.md:761` tells operators to "**set `ALLOWED_WEBSOCKET_ORIGINS`** on the
Panel/Bokeh container". An operator debugging a rejected WebSocket will set it,
observe no change, and conclude the origin check is broken rather than that they edited
an inert variable. That is a costly false trail during an outage.

## Options

- **(a) Delete the three assignments** and fix `doc/deployment.md:761` to name
  `ieasyhydroforecast_url_pentad` / `_decad` as the real knob. Smallest, honest.
- **(b) Make it real** by having the `command:` consume `ALLOWED_WEBSOCKET_ORIGINS`
  instead, with the `ieasyhydroforecast_url_*` value as its default. Better name, but
  it is a second way to configure one thing and duplicates the precedence question
  INFRA-032 just resolved.

Recommend (a). (b) should only be considered if the dashboard command is being moved
into an entrypoint script for other reasons.

## Boundary

`doc/deployment.md:761` is **already being corrected by INFRA-032**, which fixes the
value form in that same sentence (it instructs a URL scheme that crashes the server) and
stops presenting `ALLOWED_WEBSOCKET_ORIGINS` as the knob. Re-read the sentence as it
stands after INFRA-032 lands before editing it — the naming half may already be done,
leaving only the three assignments to delete.

## Acceptance criteria

- `grep -rn "ALLOWED_WEBSOCKET_ORIGINS" .` returns nothing outside this issue file.
- Both dashboards still start and accept their configured origins after the change —
  verify with `docker inspect <container> --format '{{join .Config.Cmd " "}}'` that the
  `--allow-websocket-origin=` arguments are unchanged.
