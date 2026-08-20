# INFRA-033: Remaining WebSocket-origin doc passages, incl. a local-dev recipe that cannot work

**Status**: Draft — rev 2, rescoped after INFRA-032 absorbed the deployment-path passages
**Module**: infra (documentation) — `doc/development.md`, plus a re-check of what INFRA-032 edited
**Priority**: High
**Labels**: `bug`, `documentation`, `deployment`, `dashboard`
**Found while**: mapping the origin allow-list for INFRA-032. Not fixed inline, per CLAUDE.md.

---

## Summary

Five documentation passages described `ieasyhydroforecast_url_pentad` / `_decad`
incorrectly. **INFRA-032 fixes four of them** because they sit directly on the change it
ships. This issue owns the fifth — the local-development recipe in `doc/development.md`,
which has two independent bugs — and requires a **re-check pass** over the four, because
a partial doc fix that closes the issue is the recurring failure mode here.

## The two facts every passage must respect

Established by executing the pinned Bokeh 3.8.2 (`bokeh.server.util`) and cross-checked
against the upstream 3.8.2 source:

1. An entry is `HOST[:PORT]`, **no scheme**, IPv4 only. A `https://` entry raises
   `ValueError: Invalid port in host value` and the server exits. The scheme is also
   unnecessary — a bare hostname already matches an `https://` Origin, since neither
   side carries an explicit port and Bokeh assumes `:80` for both.
2. An `Origin` with an explicit port matches **only** an entry with that same port.
   `0.0.0.0:5006`, `localhost:5006` and `127.0.0.1:5006` are three different origins.

Not theoretical: `doc/prod/long_term_deploy_runbook.md:359-362` records a production
crash-loop with `ERROR: Empty host value` — the sibling `ValueError` from the same
function, raised when the variable resolved to blank.

## What this issue owns

**`doc/development.md:1379-1381`** — the documented way to run the dashboard locally:

```bash
ieasyhydroforecast_url_decad=0.0.0.0:5007 ieasyhydroforecast_url_pentad=0.0.0.0:5006 \
  bash bin/daily_update_sapphire_frontend.sh <path>/config/.env
```

Two independent bugs — fixing either alone leaves the recipe broken:

1. **The values were discarded.** The script calls `read_configuration`, which before
   INFRA-032 overwrote both with the derived public hostnames. INFRA-032 guards the
   derivation, so an exported value now survives; verify this recipe against the
   post-INFRA-032 code rather than assuming.
2. **The values are the wrong origin even when honoured.** A developer opening
   `localhost:5006` sends `Origin: http://localhost:5006`, which `0.0.0.0:5006` does not
   match. The recipe must list the origin the browser will actually send — most likely
   `localhost:5006` — or list several comma-separated.

The following sentence ("these two urls need to be specified in your .env file under
variable `ieasyhydroforecast_url`") conflates three variables and should be rewritten or
cut.

Also confirm whether the bare `docker run` recipe at `doc/development.md:1390-1395`
still works: it uses the image `CMD` (`apps/forecast_dashboard/Dockerfile:42`), whose
default is `${ieasyhydroforecast_url:-localhost:5006}` — a *different* variable from the
two this issue is about. Either document that or fix the CMD default.

## Re-check pass over what INFRA-032 edited

INFRA-032 edits `doc/configuration.md:162-164`, the value-form sentences at
`doc/deployment.md:761` and `doc/plans/deployment_new_hydromet_aws.md:580-582`, and the
wrong restart verb at `doc/deployment.md:726`. **Read each as it stands after INFRA-032
lands** and confirm, rather than assume:

| Passage | Must end up saying |
|---|---|
| `doc/configuration.md:162` | `ieasyhydroforecast_url` is dereferenced unconditionally; it is not "unused for LAN-only deployments" |
| `doc/configuration.md:163-164` | Derived **only if unset** (true after INFRA-032), with the value-form rules |
| `doc/deployment.md:761` | Names `ieasyhydroforecast_url_pentad`/`_decad`, no scheme, no `ALLOWED_WEBSOCKET_ORIGINS` |
| `doc/deployment.md:726` | `up -d --force-recreate decaddashboard`, never `restart` |
| `deployment_new_hydromet_aws.md:580-582` | No scheme; env-file assignment now genuinely works |

Anything still wrong there is this issue's to finish.

## Dependency

**Sequence after INFRA-032.** Its guard is what makes an env-file/exported value
effective, which the corrected dev recipe depends on.

## Acceptance criteria

- `grep -rn "url_pentad\|url_decad" doc/` shows no passage pairing these variables with
  a scheme, and none presenting env-file assignment as ineffective.
- Every row in the re-check table above has been read post-INFRA-032 and confirmed or
  fixed — not assumed.
- The `doc/development.md` recipe, run verbatim against a local env file, serves a
  dashboard the developer can actually use **at the URL the doc tells them to open**,
  with no origin rejection in the container log. If the doc says `localhost:5006`, the
  test is `localhost:5006`.
