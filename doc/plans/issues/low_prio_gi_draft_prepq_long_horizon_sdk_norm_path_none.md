## Long-horizon hydrograph sync: SDK norm lookup fails with "No path provided or the provided path is None" for 4 sites (PREPQ-014)

**Status**: Draft (2026-08-15)
**Module**: `apps/preprocessing_runoff` (`sync_long_horizon_hydrograph.py`,
`write_station_short_horizon`)
**Priority**: **Low** *(downgraded from Medium 2026-08-17)* — the long-horizon script does not run
in daily production maintenance at all; the "FAIL on every run" symptom is specific to the local
`run_locally.sh` target. Genuine production exposure is the **yearly 01 Jan** aggregation, where the
wrapper does surface a non-zero exit correctly. Root cause is an accepted **upstream SDK gap**, not
a SAPPHIRE defect. See "Open decisions for the owner" 1 and 2, both resolved.
**Labels**: `preprocessing_runoff`, `hydrograph`, `sdk`, `configuration`
**Found**: 2026-08-14, reconfirmed 2026-08-15, local kghm review on `maxat_sapphire_2` @
`8e3fc1bc`.
**Related**: PREPQ-009 (long-horizon hydrograph norm decouple — Complete). PREPG-009 (same
question of how a module should report partial sub-task failure).

> **Provenance correction (2026-08-16).** The checkout moved from `maxat_sapphire_2` to
> `fix_lr010_lr011_write_contract` at **2026-08-14 16:00** (git reflog), so every run from the
> full-history recalc onward executed on that branch (now `849c8736`), **not** on trunk as the
> line above states. That branch's diff vs trunk touches only
> `apps/linear_regression/linear_regression.py`, `apps/iEasyHydroForecast/forecast_library.py`,
> their tests, and docs — **none of the files this issue concerns** — so the finding holds
> identically on trunk. Recorded for accuracy of the audit trail, not because the conclusion changes.

---

## Observation

Two distinct failure messages appear in the short-horizon hydrograph build:

**(a) 4 sites — a path is unset (this issue):**
```
WARNING - write_station_short_horizon: SDK norm call failed for site <CODE> (pentad), skipping.
          Error: ValueError: No path provided or the provided path is None
WARNING - write_station_short_horizon: SDK norm call failed for site <CODE> (decade), skipping.
          … (8 warnings across 4 distinct sites, pentad + decade each)
```

**(b) 51 sites — norms genuinely absent (probably not a defect):**
```
48x  write_station_short_horizon: expected 72 norm values for site <CODE> (pentad), got 0 - skipping this site.
 3x  write_station_short_horizon: expected 36 norm values for site <CODE> (decade), got 0 - skipping this site.
```

55 distinct sites *did* receive hydrograph writes, so neither condition is total.

The (a) class propagates to the run's exit code:

```
ERROR - Long-horizon monthly hydrograph ingestion completed with 4 SDK norm lookup failure(s).
[ERROR] Long-horizon hydrograph sync had SDK norm lookup failure(s)
[ERROR] preprocessing_runoff maintenance failed (exit 4) after 9m 21s
```

**CORRECTION (2026-08-17).** An earlier revision of this file claimed "the exit-code contract
behaves correctly". That is **wrong at the process level** and must not be relied on.

What is true: the inner branch selects the right *diagnostic message* (`run_locally.sh:725-726`
logs "SDK norm lookup failure(s)" for `lt_rc=4`) and sets the module's own `rc=4` (`:727`).

What is false: that 4 reaches the caller. `print_summary` returns 1 on any failure
(`:1585-1588`), the caller does `print_summary ... || exit_code=1` (`:1974`), and the script
then does `exit $exit_code`. **`run_locally.sh` exits 1, not 4.** The specific code survives
only in log text.

Consequence: any CI gate, cron alert, or wrapper reasoning on `$?` cannot distinguish
"SDK norm failure" (4) from "API failure" (5) from "no records" (2). Independently found by an
out-of-loop review during PREPQ-014 investigation and re-verified here by direct code reading.

## Why (a) is likely a real defect and (b) likely is not

> **SUPERSEDED (2026-08-17).** This section's original wording — that the error is a
> "configuration/path" error "raised before any data lookup", and that the deployment difference
> points at kghm configuration — is **wrong on both counts** and is kept only so the reasoning
> trail is legible. A hydrological station-lookup request *has* already been issued and decoded by
> the time the error is raised; what never happens is the *norm-endpoint* request. And there is no
> deployment setting that could fix it. See "Root cause" below, which supersedes this section and
> the "Cross-organisation evidence" triage bullets that follow it.

`No path provided or the provided path is None` is a **path-resolution** error raised before the
norm request, whereas `expected 72 norm values … got 0` is a **data availability** statement. They
should not be conflated, and only (a) currently drives the exit code.

Open question for whoever picks this up: are those 4 sites expected to have SDK norms at all?
*(Answered 2026-08-17 — see "Open decisions for the owner" item 1.)*

## Operational consequence

`maintenance:preprocessing_runoff` reports **FAIL** on every run while this persists, even
though its primary job — the 30-day runoff gap-fill — **succeeded** (verified: 31/31
consecutive non-null days restored in the same run that exited 4). An operator watching only
module status sees a persistent red for a secondary sub-task.

> **Scope correction (2026-08-17).** This is established for the local `apps/run_locally.sh`
> target only. The Docker maintenance wrapper and the Luigi task do not invoke the long-horizon
> script at all — see "Open decisions for the owner", item 2. Do not set priority from this
> paragraph until that is resolved. Note also that the wrapper's final process status is 1, not 4.


## Cross-organisation evidence (2026-08-17) — **kghm-specific, does not reproduce on tjhm**

Same command, same trunk commit (`maxat_sapphire_2` @ `8e3fc1bc`), different org:

| | kghm | tjhm |
|---|---|---|
| `No path provided or the provided path is None` | **8 warnings / 4 sites** | **0 — does not occur** |
| `expected N norm values … got 0 - skipping this site` | 51 | 19 |
| Sites receiving hydrograph writes | 55 | 15 |
| ERROR lines | 0 | 0 |
| Exit code (logged, inner) | **4** | n/a |
| Exit code (actual process status) | **1** — see CORRECTION | **0** (PASS, 3m 25s) |

> **SUPERSEDED (2026-08-17)** — the "deployment configuration" reading below is **wrong**. There is
> no per-site or per-org *path setting* anywhere; the unresolved value is a UUID derived at call
> time. The cross-org counts themselves stand; only their interpretation was mistaken. See
> "Root cause".

**This substantially changes the diagnosis.** The path-unset failure is **not** a general code
defect — it does not occur at all for a second deployment running identical code. ~~That points
at kghm **deployment configuration** (an unresolved per-site or per-org path)~~ — it points at a
difference in kghm's *station data*, not its settings.

Consequences for triage:

- **Priority is arguably lower than Medium** as a *code* issue. ~~and higher as a *kghm config*
  issue. Recommend re-scoping to "diagnose the unresolved kghm path"~~ — there is no kghm path to
  diagnose; see "Root cause". *(Priority was subsequently set to Low for a different and better
  reason: no daily-production path runs this code at all.)*
- The **`got 0` class appears on both orgs** (51 kghm / 19 tjhm) and drives no exit code. That
  supports the original read that it is data availability, not a defect.
- The tjhm run also confirms the **exit-code contract is sound in the healthy direction**:
  with no path-unset failures it correctly exits 0 and reports PASS, while the primary 25-day
  gap-fill succeeded (verified: 34/34 consecutive non-null days restored).

## Related observation — new warning class seen only on tjhm

```
WARNING - DEGRADED: monthly discharge norms unavailable for 17/17 stations;
          observed runoff written; norm and percent-of-norm unavailable.
```

**100% of tjhm stations lack monthly discharge norms.** The module degrades gracefully and
says so explicitly — good behaviour, and a useful contrast with the silent-success defects
elsewhere in this review. But it means tjhm has no norm or percent-of-norm for any station,
which affects bulletin display and any norm-relative product. Worth its own issue if the
norms are expected to exist; not filed here because it is a different subsystem from the SDK
path failure.

---

## Root cause (2026-08-17, static analysis)

Read-only diagnosis: SDK source, module source, and org config files. No module was run and
no API/DB/tunnel was touched.

Runtime provenance of the SDK read (this matters — a lockfile does not prove installed state):
the runner executes this module from `apps/preprocessing_runoff/.venv` (`run_locally.sh:429-430`), and
that venv's `ieasyhydro_sdk-0.3.2.dist-info/direct_url.json` records
`commit_id = 2cc795306c1b…`. The three SDK files cited below are **byte-identical** (`cmp`) between
that venv and `apps/preprocessing_gateway/.venv`, which is the copy actually read. So the source
inspected is the source that runs.

> **Review note.** This section was revised on 2026-08-17 after an out-of-loop adversarial review
> (`codex exec`, read-only, fresh context) found nine factual overstatements in the first draft.
> Corrections are applied inline below; the four scope/design findings are escalated in
> "Open decisions for the owner" at the end of this section rather than decided here.

### The unresolved variable is `site_uuid`, and it is per-site — **proven from code**

The failing path variable is the `path` local in the SDK's `_call_get_norm_for_site`, which is
`None` exactly when the per-site UUID lookup returns `None`:

1. `write_station_short_horizon` calls `iehhf_sdk.get_norm_for_site(code, "discharge",
   norm_period=config["norm_period"])` — `apps/preprocessing_runoff/sync_short_horizon_hydrograph.py:631`
2. → `_call_get_discharge_norm_for_site(site_code, norm_period, station_type="M")`
   → `_call_get_norm_for_site(site_code, 'hydro', …)` — `ieasyhydro_sdk/sdk_endpoint_definitions.py:137,111`
3. → the inner `_get_path()` calls `_get_site_uuid_for_site_code(site_code, 'hydro', 'M')`, which
   queries **only** `GET stations/{organization_uuid}/hydrological?station_code=<code>`
   — `sdk_endpoint_definitions.py:90-109`
4. → `_get_path()` returns `None` because both branches are guarded by `and site_uuid`
   — `sdk_endpoint_definitions.py:118-126`
5. → `_call_api` raises `ValueError('No path provided or the provided path is None')`
   — `ieasyhydro_sdk/sdk_base.py:63-64`

So the answer to "global or per-site" is **per-site**, which is why 4 of 62 sites hit it and the
rest do not. Nothing about the deployment's env vars, host, or credentials is unset — the failing
value is a UUID derived per station code at call time.

**All four ways `_get_site_uuid_for_site_code` yields a falsy UUID** (`:98-109`) — the first draft
claimed only the first, which was wrong:

1. The station-lookup response is **non-200** — falls through to `return None`. Note this would
   normally be systemic (auth, outage), not per-site, so it is a poor fit for 4-of-62.
2. The response is 200 with an **empty** station list — the `if sites:` fallback is skipped,
   `return None`.
3. The response is 200 and a row matches `station_type='M'` but its **`uuid` key is missing or
   null** — `site.get('uuid')` returns `None`.
4. The response is 200 with rows but **none matches `station_type='M'`** — falls back to
   `sites[0].get('uuid')`, which can likewise be missing/null.

So the error is **not** strictly evidence of registry absence — cases 1, 3 and 4 all produce it
without the code being missing.

**An unresolvable question from client code alone:** `station_type='M'` is sent as a *query
parameter* to the station endpoint (`:78-84`), so the backend may filter server-side. If it does, an
automatic-only station returns an empty list (case 2) and the client-side `station_type` match loop
and `sites[0]` fallback are unreachable for it. If it does not, the fallback catches it. Nothing in
the SDK settles which. **Therefore "exists in the registry but is automatic-only" cannot be ruled
out as a trigger for the 4 sites** — an earlier draft of this section wrongly asserted that it
could. Settling it needs either a captured response or the service's filtering semantics. See also
the station-identity hazard in the open decisions below.

### What can make a code unresolvable — virtual-only codes are the leading candidate, not a finding

**Proven from code** — the work list and the norm lookup are built from *different* station
registries:

- `resolve_sdk_station_codes` (`sync_long_horizon_hydrograph.py:683-691`) delegates to
  `get_all_forecast_sites_from_HF_SDK`, which merges `get_discharge_sites()` (→
  `stations/{org}/hydrological`) **and** `get_virtual_sites()` (→ `stations/{org}/virtual`)
  — `apps/iEasyHydroForecast/setup_library.py:1534-1545`.
- Virtual sites are admitted on forecast flags alone
  (`forecast_library.py:7874+`, `virtual_all_forecast_sites_from_iEH_HF_SDK`); there is no check
  that the code also exists in the hydrological registry.
- `_call_get_norm_for_site` accepts only `'hydro'` and `'meteo'` site types. There is **no virtual
  branch**, and no fallback to `stations/{org}/virtual`.

A **virtual-only** station code — one present in `stations/{org}/virtual` and *absent* from
`stations/{org}/hydrological` — is therefore placed in the work list and cannot resolve a norm UUID.

Two boundaries on that claim, both of which the first draft got wrong by asserting a universal
("no virtual station can ever resolve"):

- **A virtual code that also exists in the hydrological registry resolves normally.** Hydrological
  sites are merged *first* and the dedup keeps the first occurrence (`setup_library.py:1538,1545`,
  dedup at `:1552-1561`), so such a code appears once and its norm lookup hits the hydrological
  registry successfully. Whether kghm has any such overlap is unknown.
- **The converse also holds:** a purely hydrological code can produce this same error via cases 1,
  3, or 4 above. "Virtual" is a *sufficient* explanation, not a necessary one.

The precise, defensible statement is: **a code whose per-code hydrological lookup returns no usable
UUID cannot resolve a norm through this SDK.** Virtual-only codes are the class the module is known
to inject that satisfies this.

**Inferred, not proven** — that the 4 failing sites *are* precisely the org's virtual-only set.
Confirming membership requires listing `stations/{org}/virtual` live, which is tunnel-gated and out
of scope here.

The live candidate explanations for the 4, in rough order of likelihood, are: **virtual-only codes**
(fits the mechanism and the org contrast); **automatic-only stations**, if the service filters on
the `station_type=M` query param (see above — not excludable from client code); and **malformed or
UUID-less rows** (cases 3/4). A systemic non-200 (case 1) fits poorly, since it would not stop at 4
of 62. Mocked cases for virtual-only, virtual/hydrological overlap, automatic-only, and
hydrological-only codes would settle the mechanism without any live access.

Config-side corroboration (counts only):

| | kghm | tjhm |
|---|---|---|
| `config_virtual_stations.json` present | yes | **no such file** |
| virtual stations defined in it | 4 | — |
| of those, present in `config_station_selection.json` | 3 | — |
| codes in `config_station_selection.json` | 62 | (not compared) |
| manual codes (`data_source != "ieh_hf"`) | 0 | (not compared) |

The kghm counts **can be arranged** to sum to 62 — **55** sites wrote records + **4** path-unset +
**3** sites where *both* pentad and decade returned zero norms (48 pentad + 3 decade `got 0`
warnings) — but this is an arrangement, not an established partition, and it is offered only to show
the counts are not mutually contradictory.

**Do not read it as causal evidence.** It holds only if *all* of the following are true, none of
which is established: the 3 decade-zero sites
are exactly a subset of the 48 pentad-zero sites; there were no other no-record or API-failure
cases; and the selection snapshot came from that same run. Even if all three hold, **any** four
unresolved codes produce the same total — virtual or not. Worse, `config_station_selection.json` is
*mutable output*, not a raw registry snapshot: it is rewritten by `get_all_forecast_sites_from_HF_SDK`
after forecast-flag filtering, dedup, and manual-code appending (`setup_library.py:1567-1579`).
Set equality from one timestamped, redacted run would settle this; counts alone cannot.

The SAPPHIRE-side `config_virtual_stations.json` holds 4 entries but only **3** appear in the
selection file, and the two registries are independent — `config_virtual_stations.json` drives
SAPPHIRE's own virtual-discharge computation, while the work list comes from the iEH HF
`stations/{org}/virtual` endpoint. The "4 virtual / 4 failures" symmetry is suggestive and nothing
more.

**No supporting evidence** (not: refuted) for the "one of the four is a repeating-digit test entry"
reading. Zero of the 4 kghm *local* virtual codes contain three consecutive identical digits, and
the 3 in the station library carry `data_source = "ieh_hf"`. But that falls short of refutation on
three counts: the failing set was never matched to the local virtual set; an arbitrary digit-shape
test does not establish production legitimacy; and `data_source` records *origin*, not whether the
upstream entry is synthetic. The linear-regression `No slope and intercept for site <CODE>` message
is generic and fires for any code lacking coefficients (`forecast_library.py:6206,6264,6322`) — it
is consistent with a virtual station having no fitted LR model, not evidence either way. Only
service metadata or owner confirmation can settle this.

### Why tjhm does not reproduce it — **structurally identical path; empty input is inferred**

**Proven:** the same code runs on tjhm and nothing skips it. `get_all_forecast_sites_from_HF_SDK`
calls `get_virtual_sites()` unconditionally (`setup_library.py:1541-1545`), so there is no
structural branch that excludes tjhm from this path.

**Inferred, not proven:** that the endpoint returned no virtual sites. tjhm having no
`config_virtual_stations.json` says nothing about what the *remote* endpoint returns — that file
drives a different subsystem. The remote could equally have returned virtual sites with no forecast
flags enabled (filtered out at `forecast_library.py:7899-7906`), or virtual codes that overlap
hydrological ones and therefore resolve. A captured count of remote virtual rows and of
forecast-enabled virtual codes would settle it.

Either way the conclusion holds that this is a **data-shape** difference between deployments, not a
missing tjhm setting and not a code branch.

### (a) and (b) reach different terminal outcomes — they share upstream code

Precisely: they share the public `get_norm_for_site` entry point and the entire station-resolution
step, and **in case (a) a hydrological station-lookup request has already been issued** — the
"no HTTP request" phrasing in the first draft was wrong; what is never issued is the request to the
*norm* endpoint. What differs is where they terminate:

- **(a) path unset** — an exception out of `get_norm_for_site`, caught at
  `sync_short_horizon_hydrograph.py:632`, raised *before* the norm-endpoint request
  (`sdk_base.py:63-64`).
- **(b) `got 0`** — a *successful* call whose length check fails at
  `sync_short_horizon_hydrograph.py:643`. The SDK's `_ensure_norm_data_has_correct_length` returns
  `[]` for a falsy payload and otherwise pads to 36/12/72 (`sdk_endpoint_definitions.py:199-219`),
  so `got 0` means the norm endpoint answered **200 with a body that yielded no entries** when
  iterated (`sdk.py:240-243`) — not necessarily an empty JSON array.

That distinction is sharp enough to support different operator handling and different exit codes.
It is *not* sharp enough to classify every exception in branch (a) as "virtual / not-applicable" —
see the exception-class caveat immediately below.

### Correction to this issue's exit-code attribution

Two corrections, one to this issue's Observation and one to its "Contract not to break".

**The 8 `write_station_short_horizon` warnings do not drive exit 4.** That function catches, warns,
and returns `[]` (`sync_short_horizon_hydrograph.py:632-641`); short-horizon `main()` exits 0 unless
*every* attempted station failed. Exit 4 originates in the **long-horizon** module:
`_exit_code_for_long_horizon_summary` returns 4 when any station is `SDK_FAILED`
(`sync_long_horizon_hydrograph.py:639-645`).

**But "the same 4 sites, for the same reason" is inference, not proof.**
`_lookup_monthly_norms` catches bare `Exception` (`:295-304`) and maps *every* failure to
`SDK_FAILED`: connection and auth failures, station-lookup non-200, malformed JSON, missing keys,
float-conversion errors, norm-endpoint non-200, and the missing-path `ValueError`. Exit 4 therefore
has many possible causes, and the equal count of 4 is suggestive only. Because long-horizon logs
that failure at `logger.debug` (`:356`), the evidence needed to distinguish them was never emitted.
A counts-only exception-class summary at WARNING would settle it without exposing station codes.

**`run_locally.sh` does not propagate exit 4 as the process status.** The inner maintenance function
receives `lt_rc=4` and logs the specific "SDK norm lookup failure(s)" diagnostic (`run_locally.sh:725-727`),
but `print_summary` returns 1 whenever any module failed (`:1585-1588`) and the caller does
`print_summary "$pipeline_elapsed" || exit_code=1` (`:1974`), overwriting 4 before `exit $exit_code`.
So the observed `failed (exit 4)` line describes the **inner** result; the wrapper's final process
status is **1**. This issue's "Contract not to break" note — that exit codes 2/4/5 "are consumed by
`run_locally.sh`" — is true only for the inner branching and logging, not for the final exit status.
Anyone changing the exit-code mapping should add a shell-level test of the *final* exit contract;
the existing test inspects only the function body
(`apps/preprocessing_runoff/test/test_run_locally_long_horizon_wiring.py`).

**Net operator-visible effect**, and a reportability defect worth filing independently of the
virtual-station question: the operator sees short-horizon WARNINGs that are harmless to the exit
code, plus an exit-4 error line whose four causing events were never printed (DEBUG-only), on a run
whose actual process status is 1. Three separate signals, none of which points at the others.

### The SDK pin is current — re-pinning is not the fix (checked 2026-08-17)

Ruling out the cheapest hypothesis first, because "we're on a stale SDK, just bump it" is the
obvious first guess and it is wrong:

- Upstream `hydrosolutions/ieasyhydro-python-sdk` **`master` HEAD is `2cc7953`** (2025-10-29,
  *"update docs, remove old SDK info"*), and **all 11 modules that depend on the SDK pin exactly
  that revision** at v0.3.2 — no inter-module drift. Verified via `git ls-remote` against every
  `apps/*/uv.lock` carrying an `ieasyhydro-sdk` entry.
- The repo publishes **no tags and no releases**, so `master` is the only meaningful "latest".
- All five non-master branches — including `fix_norm_retrieval` and `virtual_station_weights` —
  have **zero commits not already in `master`**. Everything is merged.

The decisive part for a fixer: upstream already carries a commit named *"fix norm retrieval, enable
retrieval of water level norms"* (`d3ce158`), **and** separate virtual-station work
(*"support virtual forecast"*, *"virtual stations associations"*). We run the post-fix SDK, and
`_call_get_norm_for_site` **still has no virtual branch**. The virtual-station commits extended site
*listing* and *associations*; they never touched the norm path. So the gap is current and
deliberate-looking, not an artifact of an old pin — and bumping the dependency changes nothing.

Consequence for the fix space below: option 3 ("extend the SDK to resolve norms for virtual sites")
cannot be satisfied by waiting for a release — upstream `master` has been quiet since 2025-10-29 and
publishes no tags. It would need an upstream PR, a maintained fork, a local dependency patch, or a
change on the **service** side instead of the client. Price whichever is chosen before committing to
this option; the client-side options 1 and 2 avoid the question entirely.

### Verdict: deployment data shape, surfacing a latent code gap

Not a defect in `write_station_short_horizon`'s implementation, and not a missing kghm setting.
There is **no env var or config key a deployment can set to fix this** — that framing does not
apply. The defect, such as it is, sits at the seam: the module builds its work list from
hydrological + virtual registries but its norm lookup can only address the hydrological one. kghm is
merely the deployment whose data has codes that exercise the seam; an org adding a virtual station
whose code does *not* also exist in the hydrological registry would reproduce it.

The fix space (for the owner to choose, once the open decisions below are settled) is one of:

1. Classify virtual-only codes out of the norm-requiring work list before the call.
2. Treat a path-unset result for a known virtual code as not-applicable rather than `SDK_FAILED`,
   so it stops contributing to exit 4.
3. Extend the SDK/service to resolve norms for virtual sites — note the upstream cost recorded
   above and the station-identity hazard in open decision 5.

All three are behaviour changes and none was attempted here. Options 1 and 2 differ in where the
knowledge of "this code is virtual" lives: option 1 needs the work-list builder to keep the
hydrological/virtual distinction it currently discards at `setup_library.py:1544-1545`, whereas
option 2 would infer it at the failure site — which open decision 4 argues is currently unsafe,
since the exception class alone cannot distinguish a virtual-only code from an auth failure or a
malformed row.

### Open decisions for the owner — these block the fix

**1. ~~Are virtual stations expected to carry SDK discharge norms at all?~~ — RESOLVED 2026-08-17
(owner):** **Yes, they should be able to.** Since the SDK cannot do it today, this is accepted as an
**upstream iEH HF gap**, not a SAPPHIRE defect. Action: request the capability from the SDK
developer (draft: `doc/prod/iehhf_virtual_station_norms_request.md`).

Consequence for the fix space above: **option 3 is the desired end state**, and options 1–2 are
interim measures at best. Do not implement a client-side "classify virtual out" fix as though it
were the answer — it would suppress a signal the owner wants to keep. If an interim measure is
needed before upstream lands, prefer option 2 (stop counting toward `SDK_FAILED`) over option 1
(drop from the work list), because option 2 preserves the observed rows that open decision 3 is
about.

**2. ~~Which entrypoint is authoritative for "maintenance"?~~ — RESOLVED 2026-08-17 from the
crontab documentation.** **The long-horizon script does not run in daily production maintenance at
all.** Exhaustive grep for `sync_long_horizon_hydrograph` across `apps/` and `bin/` (excluding tests
and the module itself) returns exactly three invocation sites:

| Caller | Level | Cadence | Production? |
|---|---|---|---|
| `apps/run_locally.sh:706,713` | runs the script | on demand | no — local dev runner |
| `bin/yearly_runoff_hydrograph_aggregation.sh:184` | runs the script | **yearly, 01 Jan** | **yes** |
| `apps/preprocessing_runoff/backfill_discharge_aggregation.py:109` | imports and calls `write_long_horizon_hydrograph` | on demand | no — backfill tool (`bin/backfill_discharge_aggregation.sh`) |
| `bin/dev_local_backfill.sh` | **indirect** — via `initialize_regenerate_hooks.sh` / the yearly wrapper | on demand | no — dev backfill |

**This table is not certified exhaustive**, and it deliberately distinguishes script-level from
function-level callers — an earlier draft claimed "exactly three invocation sites" and both missed
the backfill module and conflated the two levels. What the grep *does* support is the narrower and
sufficient claim: **no daily-production path reaches this code.**

The documented production cron (`doc/deployment.md:912`) runs daily maintenance at 19:00 UTC via
`bin/run_daily_maintenance.sh` → Luigi, whose `PrepRunoffMaintenance` runs only
`preprocessing_runoff.py`. `bin/daily_preprunoff_maintenance.sh` is marked **[Legacy]** and
superseded (`bin/README.md:44,173`).

**Therefore the headline symptom of this issue — "`maintenance:preprocessing_runoff` reports FAIL on
every run" — cannot occur in scheduled production.** It is specific to the local `run_locally.sh`
target. **Priority drops accordingly** (see the status block at the top).

The genuine production exposure is the **yearly 01 Jan** job, once per year. That wrapper handles
the exit code correctly: it reads the container's real status via
`docker inspect ... {{.State.ExitCode}}` rather than the `tee` pipeline's code, and branches on
non-zero (`bin/yearly_runoff_hydrograph_aggregation.sh:206-213`). So an exit 4 there *would* surface
— unlike in `run_locally.sh`.

> **New defect found while resolving this — filed separately, do not fix here.**
> `doc/deployment.md:923` schedules the 01 Jan 03:00 slot as
> `run_periodic_maintenance.sh monthly_norms`, but Luigi's task map handles only
> `long_term`/`skill_recalc`/`snow_norms` and **raises `ValueError` for `monthly_norms`**
> (`apps/pipeline/pipeline_docker.py:2049-2057`). `bin/README.md:231` names a *different* script for
> the same slot (`yearly_runoff_hydrograph_aggregation.sh`). A deployment following `deployment.md`
> would therefore never run the yearly long-horizon aggregation. See
> `doc/plans/issues/high_prio_gi_draft_infra_yearly_monthly_norms_cron_unmapped.md`.

**3. Per horizon, should a virtual site keep its observed rows with `norm=None`?** The two horizons
already disagree. Long-horizon `NORM_ABSENT` still writes the 12 monthly rows and preserves any
stored norm (`sync_long_horizon_hydrograph.py:337-417`), and the caller then also writes the derived
seasonal and quarterly rows (`:552-599`); `SDK_FAILED` skips the station entirely (`:572-579`).
Short-horizon skips without norms in both cases (`sync_short_horizon_hydrograph.py:619-652`). "Classify virtual codes out" could therefore *delete
products that are currently produced*. Acceptance criteria should protect those observed rows, not
merely remove exit 4.

**4. How should a "known virtual" code be identified?** Today it cannot be, reliably. The virtual
`Site` builder does not pass `site_type` (defaults to `"default"`) and does not set `is_virtual`,
which config serialization then defaults to false (`forecast_library.py:7931`, `:6041`,
`setup_library.py:811`); and `resolve_sdk_station_codes` discards the `Site` objects entirely,
keeping only codes (`sync_long_horizon_hydrograph.py:683-691`). Any fix must carry registry
provenance explicitly and define duplicate-code behaviour. It must **not** infer virtual status from
an exception message or from `data_source`.

**5. The SDK's `sites[0]` fallback is a station-identity hazard** (`sdk_endpoint_definitions.py:101-108`).
It accepts the first returned UUID without checking that the row's station code matches the one
requested. It was presumably meant for automatic/manual variants of the same station, but any change
that widens this lookup could silently attach **another station's norms**. If option 3 is chosen,
require a test proving that norms requested for placeholder `19999` can never be served from a
different code.

---

## What to inspect

> Items 1 and 2 are **answered** in "Root cause" above; item 3's factual half (are they
> distinguishable in code?) is answered — yes — leaving only the policy half. Item 4 is untouched.

1. ~~Which path variable is unresolved~~ — **answered**: the SDK's per-site `site_uuid`, unresolvable
   for codes absent from the org's hydrological station registry.
2. ~~Whether the 4 affected sites differ structurally~~ — **answered (mechanism proven, membership
   inferred)**: virtual stations, admitted to the work list from a registry the norm lookup cannot
   address. Open: confirm the failing set against a live `stations/{org}/virtual` listing.
3. Whether (a) and (b) should share an exit code at all — today only (a) reaches exit 4. They are
   cleanly separable in code; this is now purely a policy question. Fold in the reportability
   defect noted above: long-horizon logs its `SDK_FAILED` at DEBUG, so the events causing exit 4
   are invisible at default log level.
4. Whether a failed *secondary* sub-task should mark the whole maintenance target FAIL when
   the primary gap-fill succeeded (cross-reference PREPG-009's partial-failure question).

## Acceptance criteria

- The 4 sites either produce norms, or are explicitly classified as not-applicable and stop
  contributing to the failure count.
- Path-unset and data-absent conditions are reported distinctly and, if they warrant different
  operator responses, carry different exit codes.
- A run whose primary gap-fill succeeded is distinguishable from one where it did not.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` green.

## Contract not to break

- Exit codes 2 / 4 / 5 are already consumed by `run_locally.sh`
  (`run_maintenance_preprocessing_runoff`); do not renumber without updating that mapping.
  **Qualified 2026-08-17:** they are consumed for *inner branching and logging* only — the
  wrapper's final process status is 1 whenever any module failed, because `print_summary`'s
  return overwrites it (`run_locally.sh:1585-1588,1973`). See the exit-code correction in
  "Root cause". A change here needs a shell-level test of the final exit contract; the existing
  test inspects only the function body.
- The 30-day gap-fill must keep running to completion regardless of hydrograph norm failures —
  it did here, and that ordering is what saved the review.
