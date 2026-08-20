# INFRA-032: Make the dashboard WebSocket origin allow-list configurable per deployment

**Status**: Draft — rev 3, after two out-of-loop review rounds (codex, 2026-08-20)
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

2. **The decadal dashboard rejects every origin.** Every entry in that list carries
   `:5006`, and the decadal dashboard is served on `:5007`. Bokeh requires an exact port
   match, so **no** origin in the list can connect to it. Any deployment configured this
   way has a decadal dashboard that loads and then never responds.

There is a third leak: `apps/pipeline/pipeline_docker.py:1377` reads the same base
variable and emails it to operators as *"View the latest forecasts on the dashboard:
&lt;url&gt;"*. With a comma list stuffed in, that notification carries a blob of
host:port pairs instead of a link. The base variable was designed to hold one URL.

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

**Blast radius is nil until a deployment opts in.** The owner confirmed taj and uzb
carry their allowed hosts on the base `ieasyhydroforecast_url`, not on `_pentad` /
`_decad`, so the guard falls through to the derivation for them exactly as today.

Two earlier designs were rejected: an additive `_extra` variable (unimplementable for
the decadal dashboard, see Context §4) and hard-coding the private IP into the `kghm`
branch (it would commit a hydromet's internal address to a public repository).

## Scope

### 1. `bin/utils/common_functions.sh` — deterministic provenance

**Immediately before sourcing the env file** (before the `set -a` block at lines 68-71),
`unset ieasyhydroforecast_url_pentad ieasyhydroforecast_url_decad`.

Without this, a guard cannot distinguish *"this deployment did not set it"* from
*"a previous run in this shell exported it"*. That matters concretely:
`doc/prod/long_term_deploy_runbook.md:357-362` instructs operators to `source
common_functions.sh` and call `read_configuration` **in their current shell**, so
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

This placement is deliberate and is the owner's decision. `read_configuration` has ~45
call sites, and `bin/setup_historical_backfill_env.sh` is documented **"source this
file"** — an `exit 1` inside `read_configuration` would abort backfills, forecast runs
and maintenance jobs over a dashboard typo, and would close an operator's interactive
shell. Confining it to dashboard startup keeps the failure proportional to the cause.

Validate the **form** of each comma-separated entry rather than blacklisting substrings.
Each entry must match `HOST[:PORT]`: `^[A-Za-z0-9.-]+(:[0-9]+)?$`. One regex rejects
every case the reviews raised — wildcard (`*`, `*:5006`), whitespace, empty entry
(leading, trailing or doubled comma), scheme (`https://…`), IPv6 literal, bare `:5006`,
`host:notaport`, `a:b:c`. On failure print the variable name and the offending entry and
`exit 1`; these three scripts are executed, never sourced, so `exit` is correct there.

Wildcards are rejected on the owner's instruction: Bokeh accepts them with only a log
warning, and `*:5006` would let **any** hostname on that port open a WebSocket.

### 4. `bin/utils/common_functions.sh` — echo the resolved values

One line, in the style of the surrounding `echo "| …"`, reporting the resolved pentad and
decad origins. This is the value an operator needs when diagnosing a rejected WebSocket,
and it is what makes step 4 of the operator procedure checkable.

### 5. Documentation

- **`doc/configuration.md:162-164`** — `ieasyhydroforecast_url` is required **when either
  dashboard origin must be derived** (not "unused for LAN-only deployments", and not
  unconditionally required — after this change it is not dereferenced at all when both
  explicit origins are set). For `_pentad`/`_decad`, replace the false "if unset" default
  with the now-true one and state the value-form rules from the table above, including
  that the two dashboards need different ports.
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
**cannot pick up a changed variable**. `doc/prod/long_term_deploy_runbook.md:357-362`
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
three executed scripts only, so the ~42 other `read_configuration` callers cannot be
aborted by it. The strict-mode callers that wrap the helper in `set +u`
(`bin/yearly_runoff_hydrograph_aggregation.sh:95-100`,
`bin/backfill_snow_stats_history.sh:94-98`,
`bin/backfill_discharge_aggregation.sh:90-95`) stay safe: the guard adds no new bare
dereference, and `unset` is `set -u`-safe.

Behaviour changes only where an env file sets `_pentad` or `_decad`. Per the owner, no
current deployment does — they all use the base variable — so this ships as a no-op and
takes effect only when kghm's env file is edited.

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

**Deliberately not written**: unit tests over `bokeh.server.util` internals. An earlier
revision proposed three; they re-assert behaviour owned by the pinned dependency rather
than this repository's wiring. Those semantics are pinned by test 11 and the table above.

## Acceptance criteria

- Variables unset ⇒ exported values byte-identical to the pre-change code, all three org
  endings.
- Variables set ⇒ env file value survives verbatim, no derived hostname added.
- Two sequential `read_configuration` calls in one shell do not leak the first file's
  origins into the second (test 5), and a parent-shell export does not leak in (test 6).
- Every malformed form in §3 exits non-zero from a dashboard launcher, naming the
  variable and the entry — and does **not** abort a plain `read_configuration` (test 10).
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected
  skips. *(A reviewer proposed cutting this gate as disproportionate to a shell change.
  Rejected: CLAUDE.md makes it a standing precondition, not a per-change judgement.
  Recorded so it is not re-litigated.)*
- `shellcheck` reports no new findings on the changed shell files versus pre-change.
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
