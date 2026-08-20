# INFRA-032: Make the dashboard WebSocket origin allow-list configurable per deployment

**Status**: Implemented — rev 5, awaiting owner review
**Module**: infra (cross-module) — `bin/utils/common_functions.sh`, dashboard launchers, config docs
**Priority**: High (blocks Kyrgyz Hydromet in-network access to the dashboards)
**Labels**: `bug`, `deployment`, `dashboard`, `configuration`

---

## Summary

Kyrgyz Hydromet runs SAPPHIRE **on their own server inside their own network**, reached
by private IP and by an internal hostname; the box is not publicly visible. The public
hostname `kyg.fc.<url>` is needed only by the AWS staging deployment. Today the origin
allow-list is hard-derived in shared shell code with an unguarded `export`, so a
deployment cannot express its own origins.

Guard the derivation so that **a deployment's env file wins when it sets
`ieasyhydroforecast_url_pentad` / `_decad`**, and derive only when it does not. Validate
the resulting values on the dashboard start paths.

## The workaround this replaces, and what it is really doing

The kghm server currently expresses its allow-list by stuffing a comma list into the
**base** variable, `ieasyhydroforecast_url=<a>:5006,<b>:5006,<c>:5006,<d>:5006`. taj and
uzb do the same (owner, 2026-08-20). Two consequences, both verified by executing the
derivation and the pinned Bokeh against a structurally identical placeholder value:

1. **The pentadal allow-list works by accident.** The derivation is a bare string
   prefix, so `kyg.fc.` attaches only to the **first** entry, producing the nonsense
   host `kyg.fc.<a>:5006` — which matches nothing — while entries two onwards pass
   through untouched as their own comma-separated entries. The deployment is reachable
   today because of where the commas happen to fall, not because anything intended it.
   **A configuration that works by accident gives no signal when the accident stops
   holding**: adding a fifth host, or reordering the list so a different entry lands
   first, silently breaks access with no error at the point of the edit. That is the
   same silence as the crash-loop in step 5 of the operator procedure and the
   never-passing healthcheck in INFRA-035 — only the layer differs (config time,
   start time, verify time).

2. **The decadal dashboard rejects every origin.** Every entry in that list carries
   `:5006`, and the decadal dashboard is served on `:5007`. Bokeh requires an exact port
   match, so **no** origin in the list can connect to it. Any deployment configured this
   way has a decadal dashboard that loads and then never responds.

There is a third leak: `apps/pipeline/pipeline_docker.py:1377` reads the same base
variable and emails it to operators as *"View the latest forecasts on the dashboard:
&lt;url&gt;"*. With a comma list stuffed in, that notification carries a blob of
host:port pairs instead of a link. The base variable was designed to hold one URL.
Filed as INFRA-036 — this issue does not change the base variable's contents, so that
notification stays wrong after this lands.

> **Scope caveat — do not close the decade thread on this evidence.** Finding 2 explains
> why decade forecasts are **unviewable** on these deployments. It does **not** explain
> the separate, open report of **missing DECADE `ENSEMBLE_MEAN` rows** for specific dates
> while `NAIVE_ENSEMBLE` was written for the same dates. That is a write-side gap in the
> database: an unreachable dashboard cannot stop rows being written, and those rows are
> absent for EM while present for NE on the same dates, so something upstream chose not
> to write them. The two are at different layers and must be investigated separately —
> see the dev-DB coverage thread. Recording this finding as "the decade problem, solved"
> would close that investigation on evidence that does not touch it.

**The migration this issue enables**: set the origins on `_pentad` / `_decad`, each with
the right port, and let `ieasyhydroforecast_url` go back to being a single base URL.

## Explicitly NOT fixed by this issue

The internal hostname is **slow to respond** while the same dashboard is fast over an
SSH tunnel to `localhost`. That is not an origin problem: that hostname is already
present in the allow-list with the correct port and Bokeh accepts it (verified). A
rejected origin makes a dashboard **unresponsive**, not **slow**. Do not expect this
change to improve it — it needs its own investigation of DNS, the network path, MTU, or
a proxy in front. Filed as a separate note, not as part of this work.

## Context — how the allow-list is produced and consumed

Line references are to the pre-change tree.

1. **Derivation.** `bin/utils/common_functions.sh:111-125` (`read_configuration`)
   derives `_pentad` / `_decad` from `ieasyhydroforecast_url`, keyed on the last four
   characters of the env-file path (`kghm` / `tjhm` / `uzhm`), with a **bare `export`**.

2. **Consumption.** `sapphire/docker-compose.yml:231` and
   `bin/docker-compose-dashboards.yml:50,96` interpolate the value into the container
   `command:`, where a `sed` pair splits it on commas into repeated
   `--allow-websocket-origin=` arguments; `${ORIGINS_ARGS}` is unquoted so word-splitting
   yields separate arguments. **Multi-origin already works** — verified end to end.

3. **The derivation currently beats the env file.** `read_configuration` exports into the
   shell, and Compose gives the **shell environment precedence over `--env-file`**
   (confirmed on Compose v5.1.4). Three scripts start a dashboard after calling it:
   `bin/restart_sapphire_stack.sh:41`, `bin/daily_update_sapphire_frontend.sh:52`,
   `bin/deploy_sapphire_forecast_tools.sh:68`.

4. **The decadal dashboard has no scripted start path.**
   `grep docker-compose-dashboards.yml bin/` yields **four `down` invocations and zero
   `up`**. It is started only by the manual command at `doc/deployment.md:718`, which
   never sources `common_functions.sh`. `bin/daily_update_sapphire_frontend.sh:73` looks
   like it deploys decad but does not — `start_docker_compose_dashboards`
   (`common_functions.sh:271`) brings up `sapphire/docker-compose.yml`, which has only
   the pentadal service.

   **This is what selects the design.** Under a guard, the manual decad command reads the
   value from `--env-file` and the scripted pentad path reads the same variable from the
   shell — one variable, one contract, both paths. A design that *appended* inside
   `read_configuration` could never reach the decadal container.

5. **`ALLOWED_WEBSOCKET_ORIGINS` is not the mechanism.** Three assignments
   (`sapphire/docker-compose.yml:226`, `bin/docker-compose-dashboards.yml:46,92`) plus
   one doc mention; read by nothing. Bokeh's own variable is `BOKEH_ALLOW_WS_ORIGIN`.
   (INFRA-034.)

6. **`apps/forecast_dashboard/Dockerfile:42`** is overridden by both Compose files, but
   is *not* universally dead — `doc/development.md:1390-1395` documents a bare
   `docker run` that uses it. Out of scope.

## Value form — what Bokeh actually accepts

Executed against the pinned Bokeh 3.8.2 (`bokeh.server.util`), cross-checked against the
upstream 3.8.2 source:

| Allow-list entry | Browser `Origin` | Result |
|---|---|---|
| `host.example` | `http://host.example` | accept |
| `host.example` | `https://host.example` | accept |
| `host.example` | `http://host.example:5006` | **reject** |
| `<IP>` | `http://<IP>:5006` | **reject** |
| `<IP>:5006` | `http://<IP>:5006` | accept |
| `<IP>:5006` | `http://<IP>:5007` | **reject** |
| `https://<host>` | any | **startup crash** — `ValueError: Invalid port in host value` |
| `` (empty) | any | **startup crash** — `ValueError: Empty host value` |

`create_hosts_allowlist` appends `:80` to any entry without a colon, and
`check_allowlist` appends `:80` to an `Origin` netloc without a colon; an `Origin` with
an explicit port matches only an entry with that same port. Hence:

- Entries for a directly-browsed dashboard **must carry the port** — `:5006` for pentad,
  `:5007` for decad. This is the single most common way to get this wrong.
- A **scheme must never appear**; existing bare hostnames are correct as-is.
- **IPv4 only.** Bokeh splits an entry on `:`, so IPv6 literals yield too many parts.
- `0.0.0.0:5006`, `localhost:5006` and `127.0.0.1:5006` are **three different origins**.
  List each form staff actually use.

## Design decision (owner, 2026-08-20, revised across two review rounds)

Guard the derivation; **the env file wins when it sets the variable**.

| Deployment | Env file | Resulting origins |
|---|---|---|
| kghm in-network | sets `_pentad` / `_decad` | its own list, correct ports, no public hostname |
| AWS staging (kghm) | leaves them unset | derived `kyg.fc.<url>` — unchanged |
| taj, uzb (AWS) | leave them unset (they set only the base var) | unchanged, byte for byte |

**Blast radius is nil only for the resolved origin *values*, not for behaviour more
broadly.** The owner confirmed taj and uzb carry their allowed hosts on the base
`ieasyhydroforecast_url`, not on `_pentad` / `_decad`, so the guard falls through to the
derivation for them and the exported values are byte-identical to today. But this
overstates it if read as "nothing changes": every `read_configuration` call — all 38 of
them under `bin/`, for every org, whether or not the deployment sets `_pentad`/`_decad` — now emits an
extra "Resolved dashboard origins: ..." echo line, and all three dashboard launchers gain
a brand-new failure condition (`validate_dashboard_origins`) that did not exist before,
even for deployments whose origins are purely derived. A malformed *derived* value (there
is currently no reason to expect one, but the guard does not know that) would now abort a
launcher that previously would have started regardless.

Two earlier designs were rejected: an additive `_extra` variable (unimplementable for
the decadal dashboard, see Context §4) and hard-coding the private IP into the `kghm`
branch (it would commit a hydromet's internal address to a public repository).

## Scope

### 1. `bin/utils/common_functions.sh` — deterministic provenance

**Immediately before sourcing the env file** (before the `set -a` block at lines 68-71),
`unset ieasyhydroforecast_url_pentad ieasyhydroforecast_url_decad`.

Without this, a guard cannot distinguish *"this deployment did not set it"* from
*"a previous run in this shell exported it"*. That matters concretely:
`doc/prod/long_term_deploy_runbook.md:364-366` instructs operators to `source
common_functions.sh` (:364) and call `read_configuration` (:366) **in their current shell**, so
without the unset, a second call for a different env file would silently inherit the
first deployment's origins — and, because shell environment beats `--env-file`, would
also poison a subsequent manual decad command in that shell.

After the unset, the only two sources are the env file and the derivation.

### 2. `bin/utils/common_functions.sh` — guard the derivation

Make each assignment in the `env_ending` block (lines 113-122) conditional, e.g.
`: "${ieasyhydroforecast_url_pentad:=kyg.fc.$ieasyhydroforecast_url}"`, keeping one
`export` of both names after the block. The `:=` form assigns when the variable is unset
**or empty**, which is required: an env file line `ieasyhydroforecast_url_pentad=` must
fall back to the derived value rather than produce an empty entry that crashes Bokeh.

The `kghm` decad prefix stays `demo.fc.decade.` — **the owner confirmed this is correct
for kghm, not a leftover.** Do not "fix" it to match taj/uzb.

### 3. New `validate_dashboard_origins()` — on the dashboard start paths only

Define it in `common_functions.sh`; **call it from the three dashboard launchers**
(`restart_sapphire_stack.sh`, `daily_update_sapphire_frontend.sh`,
`deploy_sapphire_forecast_tools.sh`), *not* from `read_configuration`.

This placement is deliberate and is the owner's decision. `read_configuration` has 38
call sites under `bin/`, and `bin/setup_historical_backfill_env.sh` is documented **"source this
file"** — an `exit 1` inside `read_configuration` would abort backfills, forecast runs
and maintenance jobs over a dashboard typo, and would close an operator's interactive
shell. Confining it to dashboard startup keeps the failure proportional to the cause.

Validate the **form** of each comma-separated entry rather than blacklisting substrings.
**Correction (review round 2): no single regex carries this alone** — that was true only
of the first cut. The validator (`_check_dashboard_origin_value`) is now a sequence of
checks, most of them added after later review rounds found something a bare regex could
not: dedicated pre-checks reject an embedded newline/CR, a blank/whitespace-only value,
and a leading/trailing/doubled comma (each with its own message, before any splitting);
the value is then split into entries; **each entry** is matched against
`^[A-Za-z0-9_.-]+(:([0-9]+))?$` (note the underscore - `host_name` is a valid hostname
form and must not be rejected) for the base `HOST[:PORT]` shape — this one regex covers
wildcard (`*`, `*:5006`), scheme (`https://…`), IPv6 literal, `host:notaport`, `a:b:c`;
then three checks that a regex alone cannot express safely run on top: the host part is
rejected if it is empty, ends in a bare trailing dot, or is all-digits-no-dot (a likely
`HOST:PORT` typo, e.g. `5006` alone); the port, if present, is rejected first on *shape*
(1-5 digits) before any arithmetic range check — an oversized port (20+ digits) would
otherwise make bash's own `-lt`/`-gt` comparisons silently evaluate false and the value
would be accepted — and only then on *range* (1-65535). A separate, bash-builtin-only
check on the split itself rejects a comma-split that produced zero entries from a
non-blank value (a bash-3.2 here-document failure mode, not a value problem), and a
separate check on the variable itself (`declare -p`) rejects an array or nameref before
`${!var_name}` is ever read. On failure print the variable name and the offending entry
and `return 1` — `validate_dashboard_origins` is a function that is also reachable if an
operator sources `common_functions.sh` directly, not just from the three scripted
launchers, so it cannot safely `exit` the caller's shell. Each of the three launcher call
sites instead does `validate_dashboard_origins || exit 1`, which is where the fail-fast
`exit` actually happens.

Wildcards are rejected on the owner's instruction: Bokeh accepts them with only a log
warning, and `*:5006` would let **any** hostname on that port open a WebSocket.

### 4. `bin/utils/common_functions.sh` — echo the resolved values

**Correction (third round): there are two origin-reporting echo lines, not one,**
and they report two different values:

- **`read_configuration`** echoes `"| Resolved dashboard origins: pentad=... decad=..."`
  immediately after the `env_ending` derivation block, **before** lowercasing/port
  normalisation — this runs on every `read_configuration` call, including the 35 (of 38)
  that never touch a dashboard.
- **`validate_dashboard_origins`** echoes `"| Dashboard WebSocket origins in use
  (validated, lowercased, port-normalised): pentad=... decad=..."` at the end of the
  function, **after** lowercasing and leading-zero stripping — this runs only on the
  three dashboard launcher paths.

**The `validate_dashboard_origins` line is authoritative**: it reports the value the
container actually receives (Compose interpolates the shell variable after
`validate_dashboard_origins` has already re-exported it). The `read_configuration` line
can differ from it — e.g. `HOST.Example:05006` prints once uppercase/zero-padded from
`read_configuration`, then again as `host.example:5006` from `validate_dashboard_origins`
— and only the second is what Bokeh's allow-list is built from. This is the value an
operator needs when diagnosing a rejected WebSocket, and it is what makes step 4 of the
operator procedure checkable.

### 5. Documentation

- **`doc/configuration.md:162-164`** — `ieasyhydroforecast_url` is required **when either
  dashboard origin must be derived** (not "unused for LAN-only deployments", and not
  unconditionally required). **Correction (third round): it is still dereferenced
  even when both `_pentad`/`_decad` are set explicitly** —
  `start_docker_compose_dashboards` (`bin/utils/common_functions.sh`) unconditionally
  echoes it (`echo "| Deploying dashboard to: ieasyhydroforecast_url: $ieasyhydroforecast_url"`,
  reproduced in the deploy-launcher test output), and `apps/pipeline/pipeline_docker.py:1377`
  reads it for the operator notification email (see "The workaround this replaces" above).
  `doc/configuration.md` already states this correctly; this plan previously
  disagreed with it and was wrong. For `_pentad`/`_decad`, replace the false "if unset"
  default with the now-true one and state the value-form rules from the table above,
  including that the two dashboards need different ports.
- **`doc/deployment.md:761` and `doc/plans/deployment_new_hydromet_aws.md:580-582`** —
  minimal edit (owner decision): correct the value form so operators are not told to
  enter a scheme that crashes Bokeh, and stop presenting `ALLOWED_WEBSOCKET_ORIGINS` as
  the knob. Remaining passages stay with INFRA-033.
- **`doc/deployment.md:726`** — replace `restart decaddashboard` with
  `up -d --force-recreate decaddashboard` (see below).

### Out of scope

Migrating the decadal dashboard into `sapphire/docker-compose.yml`; removing
`ALLOWED_WEBSOCKET_ORIGINS` (INFRA-034); remaining doc passages (INFRA-033); the legacy
healthcheck endpoint drift (INFRA-035); the `Dockerfile` CMD default; the internal
hostname's slowness.

## Operator procedure — and why `restart` is the wrong verb

`bin/restart_sapphire_stack.sh:48` runs `docker compose -f bin/docker-compose-dashboards.yml down`,
which **removes** the decadal container, and nothing brings it back.
`doc/deployment.md:726` then instructs `restart decaddashboard` — which cannot restart a
removed container and, even for a live one, reuses the container's baked environment and
**cannot pick up a changed variable**. `doc/prod/long_term_deploy_runbook.md:364-366`
already records a production crash-loop caused by exactly this.

1. In the deployment's env file set both, each with its own port — pentad entries on
   `:5006`, decad entries on `:5007` — listing every form staff actually type (private
   IP, internal hostname, `localhost` if used over an SSH tunnel).
2. `bash bin/restart_sapphire_stack.sh <env_file>` (pentad).
3. `docker compose --env-file <env_file> -f bin/docker-compose-dashboards.yml up -d --force-recreate decaddashboard` (decad).
4. Confirm the arguments actually reached each container:
   `docker inspect sapphire-dashboard --format '{{join .Config.Cmd " "}}'` and
   `docker inspect sapphire-frontend-forecast-decad --format '{{join .Config.Cmd " "}}'`
   — expect one `--allow-websocket-origin=` per configured entry, each with its port.
5. **Inspect container state; never infer health from the absence of a loud failure.**
   Both services are `restart: always` (`sapphire/docker-compose.yml:240`;
   `bin/docker-compose-dashboards.yml:59,105`), so a rejected value becomes a silent
   permanent restart loop while `docker compose up -d` still exits 0. Run
   `docker compose --env-file <env_file> -f sapphire/docker-compose.yml ps` and
   `docker compose --env-file <env_file> -f bin/docker-compose-dashboards.yml ps`,
   confirm neither container is cycling, and check `docker logs sapphire-dashboard` and
   `docker logs sapphire-frontend-forecast-decad` for `ValueError`.
   Note the legacy healthchecks report `unhealthy` even when working — see INFRA-035.
6. From a LAN machine open the pentad dashboard, interact with a control, and confirm in
   the browser network tab that the WebSocket stays open. Repeat for decad on `:5007`.
   **The decadal dashboard is expected to change from broken to working** for these
   deployments — it currently rejects every origin (see above).
7. On the **AWS staging** boxes, confirm the public hostnames still work. Those env files
   leave the variables unset and must be entirely unaffected.

## Blast radius

Only the two Compose files reference these variables — no Python reads them, and
`bin/docker-compose-luigi.yml` does not. `validate_dashboard_origins` is called from
three executed scripts only, so the 35 other `read_configuration` callers cannot be
aborted by it. The strict-mode callers that wrap the helper in `set +u`
(`bin/yearly_runoff_hydrograph_aggregation.sh:95-100`,
`bin/backfill_snow_stats_history.sh:94-98`,
`bin/backfill_discharge_aggregation.sh:90-95`) stay safe: the guard adds no new bare
dereference, and `unset` is `set -u`-safe.

The **resolved origin values** change only where an env file sets `_pentad` or `_decad`.
Per the owner, no current deployment does — they all use the base variable — so the
*values* ship as a no-op and change only once kghm's env file is edited. That is
narrower than "behaviour changes only where...": every `read_configuration` call now
emits an extra echo line regardless of org, and all three dashboard launchers gain a new
failure condition (`validate_dashboard_origins`, plus the `|| exit 1` at each call site)
that applies even to deployments whose origins are entirely derived — a malformed derived
value would now stop a launcher that previously started unconditionally.

## Validation coverage — what is and isn't checked

**Which start paths validate.** `validate_dashboard_origins` runs only in the three
scripted launchers (`bin/restart_sapphire_stack.sh`, `bin/daily_update_sapphire_frontend.sh`,
`bin/deploy_sapphire_forecast_tools.sh`), each via
`validate_dashboard_origins || exit 1` immediately after `read_configuration`, before any
`docker compose down`.

**Which start paths do NOT validate — an accepted gap, not an oversight, and wider than
just decad.** The documented direct `docker compose -f bin/docker-compose-dashboards.yml
--env-file <env_file> up -d decaddashboard` command (`doc/deployment.md:718-720`, and the
same command in the first-deploy checklist) is the **only** scripted or documented way to
(re)start the decadal dashboard after a value change — see Context §4 above — and it
bypasses `read_configuration` and `validate_dashboard_origins` entirely: Compose reads
`_pentad`/`_decad` straight from `--env-file`. **This means the only start path for the
decadal dashboard is unvalidated.**

The same gap exists for the **pentadal** dashboard, on two separate documented paths:
- `doc/prod/first_deploy_checklist.md:514` runs
  `docker compose --env-file "${ENV_FILE_PATH}" -f sapphire/docker-compose.yml up -d`
  directly — the whole `sapphire/docker-compose.yml` stack, including the pentad
  dashboard service, brought up straight from the env file with no `read_configuration`
  and no `validate_dashboard_origins` in between.
- `doc/prod/long_term_deploy_runbook.md:367` is a narrower case of the same gap: it DOES
  `source bin/utils/common_functions.sh` (:364) and call `read_configuration` (:366) first
  (so the origin is resolved/derived), but then runs
  `docker compose --env-file "<env>" -f sapphire/docker-compose.yml up -d --force-recreate --no-deps dashboard`
  directly, without the `validate_dashboard_origins || exit 1` step that only the three
  scripted launchers perform. `read_configuration` ran; validation did not.

The owner has decided to accept this gap — for both dashboards, on all of the paths
above — rather than block the issue on building a wrapper script around every direct
Compose invocation, and to document the gap plainly rather than let "Validate the
resulting values on the dashboard start paths" (Summary, above) be read as covering it.
An operator who edits the env file and runs any of these commands directly gets no
structural validation — a malformed value fails only at Bokeh startup, surfacing as the
silent restart-loop described in the Operator procedure step 5.

**What validation does NOT catch, even on the paths that run it.**
`validate_dashboard_origins` checks a narrow **structural** class of errors — that each
entry has the shape `HOST[:PORT]`, no scheme, no wildcard, is confined to a single line,
and (if a port is present) that it is in 1-65535. It cannot tell you the value will
actually **work**:
- A host that is syntactically valid but does not resolve, or is malformed in a way the
  regex does not catch (e.g. `a..b`).
- A syntactically valid port that is nonetheless the *wrong* port for how the dashboard
  is actually reached — Panel's own port supplied for a deployment behind a reverse
  proxy, or vice versa (see "Value form" table above and the corrected rule in
  `doc/configuration.md`).
- Anything about DNS, the network path, or a proxy in front of the dashboard.

Passing `validate_dashboard_origins` is necessary but not sufficient for a working
dashboard; step 6 of the Operator procedure (open the dashboard in a browser and confirm
the WebSocket stays open) remains the only real confirmation.

## Tests

Precedent: `apps/iEasyHydroForecast/tests/test_read_configuration_set_u.py` (drives
`common_functions.sh` in a child `bash` with a minimal environment). **Placeholders only
— `10.0.0.1`, `192.0.2.0/24`, `example.org`. No real IP addresses, internal hostnames or
credentials in tests, fixtures, commits, plan files or PR text.**

`apps/iEasyHydroForecast/tests/test_websocket_origin_config.py`:

1. **Unset ⇒ derived, for all three org endings** — the regression guard for taj, uzb and
   AWS staging. Assert exact strings, including that kghm decad is `demo.fc.decade.<url>`
   while kghm pentad is `kyg.fc.<url>`.
2. **Set ⇒ env file wins**; the derived hostname does not appear.
3. **Empty string ⇒ falls back to derived** (must not yield an empty origin).
4. **Pentad and decad independent** — setting one does not alter the other.
5. **Provenance: two `read_configuration` calls in one shell.** Call with an env file
   that sets the origins, then with one that does not, **in the same shell**, and assert
   the second call yields the derived values — not the first file's. This is the §1
   regression and cannot be caught by a single-call test.
6. **A parent-shell export does not leak in** — export a value, run `read_configuration`
   against an env file that omits it, assert the derived value wins.
7. **A strict caller still survives** — source under `set -euo pipefail` using the
   established `set +u` wrapper pattern, so the nounset safety is verified not asserted.

`apps/iEasyHydroForecast/tests/test_validate_dashboard_origins.py`:

8. **Accepts** valid forms: bare host, host with port, IP with port, multi-entry list.
9. **Rejects**, parametrised, each asserting non-zero exit and a message naming the
   variable and the entry: `*`, `*:5006`, `a, b` (whitespace), `" "` (whitespace-only),
   `a,,b`, `a,`, `,a`, `https://a`, `:5006`, `a:notaport`, `a:b:c`, an IPv6 literal.
10. **`read_configuration` alone does NOT abort** on an invalid value — proving
    validation is confined to the dashboard launchers and cannot take down a backfill.

`apps/iEasyHydroForecast/tests/test_dashboard_compose_origin_args.py`:

11. **The real Compose command turns a configured value into the right argv.** Read the
    `command:` for the pentad service in `sapphire/docker-compose.yml` and the decad
    service in `bin/docker-compose-dashboards.yml`, undo Compose's `$$` escaping,
    `shlex.split` to recover the actual `bash -c <script>` argv, and execute **that
    argv** with a stub on `PATH` recording its arguments — not by wrapping the script in
    another `bash -c`, which would expand `$( )` a level too early. Assert a two-entry
    list yields two separate `--allow-websocket-origin=` arguments, and that the decad
    service reads `ieasyhydroforecast_url_decad` and serves `:5007`.

*The reviewer proposed cutting test 11 as scope creep since neither Compose command
changes here. Kept, reduced from four services to two: it is the only test covering the
boundary between a shell variable and Bokeh's argv, and it fails if anyone quotes
`${ORIGINS_ARGS}` or edits the `sed`. The legacy pentad service is excluded per the
reviewer, since `bin/README.md:130` treats that file as decad-only.*

**Test 11's specific failure mode**: if the compose `command:` is wrapped in another
`bash -c` rather than `shlex.split` into its real argv, `$( )` expands a level too early
and the test passes no matter what the code does. Settle this by mutation, not by
reading: point it at a deliberately mangled command and confirm it goes red.

**Deliberately not written**: unit tests over `bokeh.server.util` internals. An earlier
revision proposed three; they re-assert behaviour owned by the pinned dependency rather
than this repository's wiring. **Correction (post-implementation review):** test 11 does
**not** pin Bokeh's semantics — it stubs `uv` on `PATH` and never invokes Bokeh at all, so
it proves nothing about how Bokeh interprets `--allow-websocket-origin`. It pins only the
shell-side wiring: that a configured value is turned into the right argv by the Compose
`command:` construction. Bokeh's actual origin-matching behaviour (the table in "Value
form" above) was checked once, manually, against the pinned Bokeh version while drafting
this issue, and is not re-verified by any test in this repository — a Bokeh upgrade could
silently change it without any test here going red.

`apps/iEasyHydroForecast/tests/test_launcher_validation_order.py`:

**Correction (review round 2): this file was missing from the enumeration above even
though later text ("the four files", "Mutation-test evidence") already assumed its
existence.** Added here to close that gap — see "A check that is never called" in
`doc/dev/testing_workflow.md` for why this file exists at all: without it, deleting all
three `validate_dashboard_origins` call sites left the rest of this suite green, because
`test_validate_dashboard_origins.py` only drives the function directly, never through a
launcher.

12. **Each of the three launchers — `restart_sapphire_stack.sh`,
    `daily_update_sapphire_frontend.sh`, `deploy_sapphire_forecast_tools.sh` — aborts
    before any `docker` call when the origin is invalid.** A stub `docker` on `PATH`
    records every invocation; with an invalid pentad value (`*`), the launcher must exit
    non-zero and the stub must have recorded nothing at all (not just "no `compose ...
    down`" — validation runs before the very first `docker` call in each script).
13. **The same three launchers proceed to `compose ... down` with a valid origin** — the
    non-vacuousness companion to test 12: proves the invalid-case assertion is not passing
    for an unrelated reason (a missing stub, a bad `PATH`, an early failure elsewhere).

Six tests total (two per launcher). This is also the file the "Deleting the
`validate_dashboard_origins` call site from `bin/deploy_sapphire_forecast_tools.sh`"
mutation result under "Mutation-test evidence" below refers to.

## Acceptance criteria

- Variables unset ⇒ exported values byte-identical to the pre-change code, all three org
  endings.
- Variables set ⇒ no derived hostname is ever added. Through `read_configuration` ALONE
  (e.g. a sourced backfill script) the env file value survives verbatim. On a scripted
  dashboard start it does not survive verbatim: `validate_dashboard_origins` lowercases
  BOTH the explicit and the derived value and strips a leading zero from any port, so
  `HOST.Example:05006` in an env file is exported as `host.example:5006`, not echoed back
  unchanged. "Wins" (the env file value is not overwritten by the derivation) and
  "verbatim" (byte-identical) are two different guarantees — only the first holds once a
  scripted launcher runs.
- Two sequential `read_configuration` calls in one shell do not leak the first file's
  origins into the second (test 5), and a parent-shell export does not leak in (test 6).
- Every malformed form in §3 exits non-zero from a dashboard launcher, naming the
  variable and the entry — and does **not** abort a plain `read_configuration` (test 10).
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected
  skips. *(A reviewer proposed cutting this gate as disproportionate to a shell change.
  Rejected: CLAUDE.md makes it a standing precondition, not a per-change judgement.
  Recorded so it is not re-litigated.)*
- `shellcheck` reports no new findings on the changed shell files versus pre-change.
- **Specific, named mutations have been shown to turn the test they target red, then
  restored.** Break the thing each test watches — mangle the `--allow-websocket-origin`
  construction for test 11, remove the `unset` for test 5, drop a rejected pattern for
  test 9, delete a launcher's call site — and confirm the intended test goes red before
  restoring it. **Correction (third round): this was previously worded as "every
  test above has been shown to FAIL once," which contradicts this same document's own
  "not individually mutation-checked" line below — the two cannot both be true.** The
  mutations actually run are the ones listed under "Mutation-test evidence" below; the
  rest of the suite was verified to pass on the current code but was not deliberately
  broken one by one. A check that cannot fail and a check that cannot pass both look
  exactly like verification and supply none, and reading the test cannot tell them apart
  — recording exactly which mutations were run, not claiming full coverage, is what lets
  the next person tell the two apart here.

  **Mutation-test evidence (recorded, not merely asserted).** This is a precise record of
  exactly which mutations were run and what went red — not every test in the suite below
  was individually mutation-checked; see the explicit "not checked" line at the end.

  *First round, post-implementation review, 2026-08-20:*
  - Mangling `--allow-websocket-origin=` in `sapphire/docker-compose.yml` turned 2 of
    test 11's cases red.
  - Removing the provenance `unset` (from `read_configuration`, §1 above) turned exactly
    the 2 provenance tests red.
  - Widening the validator regex to admit `*` turned the 2 wildcard cases plus the
    decad-variable case red.
  - Deleting the `validate_dashboard_origins` call sites from
    `bin/restart_sapphire_stack.sh` and `bin/daily_update_sapphire_frontend.sh` turned
    exactly those 2 launcher-order tests red.
  - Simulating a broken `tr` (a PATH containing `bash` but not `tr`) turned the 2
    lowercase tests red.
  - All files were restored byte-identical afterwards.

  *Second round, confirm-fixes review, 2026-08-20 — the two BLOCKING fail-open defects
  and the launcher-order coverage gap that round found:*
  - Reproduced the port-range fail-open bug directly, pre-fix: a 20-digit and a 30-digit
    port were silently ACCEPTED (`[ "$port" -lt 1 ]` / `[ "$port" -gt 65535 ]` each emit
    "integer expression expected" on a value bash cannot parse as an integer and both
    evaluate false); confirmed both are now REJECTED post-fix.
  - Reproduced the lowercase fail-open bug directly, pre-fix: with `tr` unavailable, both
    origin values were silently emptied (`RC=0 P='' D=''`) and the function still
    returned 0; confirmed the current code returns non-zero and leaves both values
    unchanged.
  - Deleting the `validate_dashboard_origins` call site from
    `bin/deploy_sapphire_forecast_tools.sh` turned
    `test_deploy_invalid_origin_aborts_before_any_docker_call` red (the script ran to
    completion, `returncode == 0`, instead of aborting). **Correction (third round):
    this was previously reported as "the 2 new deploy launcher-order tests" — re-run and
    confirmed only 1 of the 2 depends on the call site.**
    `test_deploy_valid_origin_proceeds_to_docker_compose_down` still passed with the call
    site removed, because a *valid* origin never triggers the early exit the call site
    guards — the script reaches `compose ... down` either way, so that test cannot
    distinguish "the call site is present and passed" from "the call site is absent." It
    is a companion/non-vacuousness check for the invalid-origin test, not itself a check
    on the call site's presence. Restored byte-identical, confirmed both tests green
    again.

  *Third round, 2026-08-20 — the two BLOCKING fail-open findings this round found in
  `validate_dashboard_origins`'s normalisation block (unchecked `tr` exit status,
  unchecked `printf -v` exit status, no postcondition re-validation), and the
  bare-port-as-hostname finding:*
  - A `tr` stub that exits non-zero but still echoes its (unmodified) input turned the
    new `test_tr_exits_nonzero_while_echoing_input_causes_nonzero_return` from green (on
    the pre-fix code, which checked only for an *empty* result) to correctly red-then-
    caught post-fix.
  - A `tr` stub that exits 0 but emits a different, structurally invalid value (`,,,`, or
    separately a 20-digit port) is caught **only** by the new postcondition re-validation
    call — no exit-status check can see it, since `tr` itself reports success. Confirmed
    both `test_tr_succeeds_but_emits_invalid_value_causes_nonzero_return_via_postcheck`
    and `test_tr_stub_emitting_oversized_port_causes_nonzero_return_via_postcheck` fail
    closed.
  - Declaring `ieasyhydroforecast_url_pentad` `readonly` before calling
    `validate_dashboard_origins` reproduces a `printf -v` failure that the assignment's
    own exit-status check cannot always catch: on GNU bash 5.2 `printf -v` on a readonly
    target correctly reports exit status 1, but on macOS's system bash 3.2 (verified,
    `/bin/bash`) it reports exit status **0** while leaving the variable unchanged. The
    added equality check (`"${!var_name}" != "$normalized"`, exit-status-independent)
    catches it on both; confirmed by executing the readonly-variable case directly under
    both `/bin/bash` (3.2) and the newer bash on `PATH` (5.2) — both now return non-zero.
  - `ieasyhydroforecast_url_pentad=5006` (a plausible typo for `HOST:5006`) was silently
    ACCEPTED pre-fix, producing the un-matchable allow-list entry `5006:80`; confirmed
    REJECTED post-fix under both bash versions, while `10.0.0.1:5006` and `10.0.0.1`
    (digits *and* dots) remain accepted.
  - All files were restored byte-identical afterwards.

  *Fourth round, 2026-08-20 — the two BLOCKING findings (FIX 2 canonical-form postcheck,
  FIX 4 non-scalar variable guard) and two non-blocking findings (FIX 5 trailing-dot host,
  FIX 3 empty-split guard) a second independent reviewer raised, plus FIX 6/FIX 7 test-
  integrity fixes:*
  - A `tr` stub that exits 0 and echoes its input **completely unchanged** (a true no-op,
    distinct from the third-round "exits non-zero" stub) is not empty and does not fail on
    exit status, so it passed every pre-existing check while leaving the value uppercase.
    Reproduced directly, pre-fix: `rc=0 pentad=HOST.EXAMPLE:5006 decad=OTHER.EXAMPLE:5007`.
    Confirmed post-fix (the canonical-form/idempotence postcheck: reject any post-
    normalisation value that still contains `[A-Z]`) both
    `test_tr_noop_stub_on_pentad_causes_nonzero_return_via_canonical_postcheck` and, with
    PENTAD deliberately left already-lowercase so only DECAD is corrupted,
    `test_tr_noop_stub_on_decad_causes_nonzero_return_via_canonical_postcheck` now return
    non-zero — proving the guard runs on both loop iterations, not just pentad's.
  - `declare -a ieasyhydroforecast_url_pentad=(...)` (reachable because env files are
    sourced as shell code) was silently accepted pre-fix (`${!var_name}` reads element
    zero); confirmed REJECTED post-fix by the new `declare -p`-based scalar guard, via
    `test_array_valued_pentad_var_is_rejected`.
  - With `ieasyhydroforecast_url` unset, the kghm derivation yields `kyg.fc.` (trailing
    dot, no host) and was silently accepted pre-fix, exporting a dead origin; confirmed
    REJECTED post-fix via `test_derived_origin_with_trailing_dot_and_no_host_is_rejected`,
    and the same rejection was confirmed to also apply to a syntactically legal absolute
    FQDN (`host.example.`) via `test_legal_fqdn_trailing_dot_is_also_rejected_not_just_bare_prefix`
    — a deliberate policy choice, documented next to the check, not a heuristic.
  - **FIX 6 (test integrity):** the pre-fix `VALIDATE_EXIT_MARKER=$?` in the shared test
    snippet builders was captured after `validate_dashboard_origins || exit 1`, whose own
    exit status is always 0 on any run that reaches the echo — the marker could never read
    anything but 0 on that line, a "check that cannot fail." Fixed by capturing `$?`
    immediately after the call, then `exit`-ing the script with that captured value (so the
    process return code and the marker text now agree and both carry real information).
    Confirmed by mutation on the *production* guard, not the test: temporarily inserting
    `return 0` as the first line of `_check_dashboard_origin_value` (disabling every
    structural check it performs) and re-running `test_rejects_malformed_origin_forms`
    turned red exactly the 17 rejection cases plus
    `test_rejects_multiline_value_with_wildcard_smuggled_on_second_line` (18 tests), each
    now showing the fixed marker reporting the real value - e.g. for the wildcard case,
    `stdout` now reads `...VALIDATE_EXIT_MARKER=0` (previously this line was simply never
    reached, not printed-but-wrong) alongside `pentad=host.example:5006,,*` actually being
    exported - proving the marker is no longer a check that cannot fail. Restored
    byte-identical afterwards (`diff` confirmed) and confirmed all 48 tests in this file
    pass again.
  - **FIX 7:** declaring `ieasyhydroforecast_url_pentad` `readonly` (needing a case change)
    is caught by the post-assignment CONTENT check on both bash versions — confirmed via
    `test_readonly_pentad_var_causes_nonzero_return_via_content_check`, parametrised over
    `/bin/bash` (3.2, where `printf -v` on a readonly target returns exit status 0) and
    `/opt/local/bin/bash` (5.2, where it correctly returns 1) — both now return non-zero,
    for different underlying reasons, both caught by the same content check. `declare -u`
    (bash 4.3+; forces the variable back to uppercase on every assignment, including
    `printf -v`'s own write) is caught the same way — confirmed via
    `test_declare_u_pentad_var_causes_nonzero_return_via_content_check` on bash 5.2 (bash
    3.2 does not support `declare -u`, so that parametrisation is version-gated to skip,
    not run and pass vacuously).
  - All files were restored byte-identical afterwards.

  **Not individually mutation-checked**: every other test in the four files — including
  the leading-zero-port, all-zero-port, and overlong-port-rejection cases added in the
  second round — was verified to PASS on the current code, but was not deliberately
  broken and shown red one by one.

  **Test count (counted, not the stale "32 tests" figure from the first round):**
  `pytest --collect-only -q` across the four files (`test_validate_dashboard_origins.py`,
  `test_launcher_validation_order.py`, `test_dashboard_compose_origin_args.py`,
  `test_websocket_origin_config.py`) collected **54** test cases as of the second round,
  **59** as of the third round (5 tests added: two `tr`-stub postcondition tests, one
  oversized-port-via-stub test, one bare-port-typo rejection case, and one
  bare-IPv4-with-no-port acceptance case), and **68** as of the fourth round (9 tests
  added: two no-op-`tr` canonical-postcheck tests (pentad, decad), one array-variable
  rejection test, two trailing-dot-host rejection tests, and two content-check regression
  tests each parametrised over two bash versions — `readonly` and `declare -u` — of which
  one `declare -u` parametrisation is version-gated to skip on bash < 4).
- No real IP address, internal hostname or credential in the diff, tests, plan or PR.

## Rollback

Revert the commit and restart per the Operator procedure. Values set in an env file
become inert and the derivation resumes unconditionally; no data or state to clean up.

## Related issues

- **INFRA-033** — [`high_prio_gi_draft_infra_websocket_origin_doc_consistency.md`](high_prio_gi_draft_infra_websocket_origin_doc_consistency.md).
  Remaining doc passages. Note the §1 `unset` means the `doc/development.md:1379` inline
  prefix recipe stays ineffective by design — that recipe must move to an env-file value.
- **INFRA-034** — [`mid_prio_gi_draft_infra_dead_allowed_websocket_origins_var.md`](mid_prio_gi_draft_infra_dead_allowed_websocket_origins_var.md).
- **INFRA-035** — [`mid_prio_gi_draft_infra_legacy_dashboard_healthcheck_endpoint.md`](mid_prio_gi_draft_infra_legacy_dashboard_healthcheck_endpoint.md).
