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
question of how a module should report partial sub-task failure). **PREPQ-015** (High, Draft) —
this issue explains *why* the SDK raises; PREPQ-015 is what stops that raise from discarding a
station's data. As of PREPQ-015's 2026-08-21 second revision, it does **not** stop the permanent
false alarm (exit 4 on every run) for stations this issue's cause structurally can never resolve —
three designs to reclassify that raise were reviewed and refuted, and the persistent FAIL row is now
an accepted, documented limitation, not a fix. See "Third confirmation" below.

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

The (a) class *appeared* to propagate to the run's exit code — **superseded**, see the CORRECTION
below and "Root cause": these short-horizon warnings drive **no** exit code, and the exit-4 line
below comes from the separate long-horizon module. The log excerpt is retained verbatim as the
original observation:

```
ERROR - Long-horizon monthly hydrograph ingestion completed with 4 SDK norm lookup failure(s).
[ERROR] Long-horizon hydrograph sync had SDK norm lookup failure(s)
[ERROR] preprocessing_runoff maintenance failed (exit 4) after 9m 21s
```

**CORRECTION (2026-08-17).** An earlier revision of this file claimed "the exit-code contract
behaves correctly". That is **wrong at the process level** and must not be relied on.

What was true at the time: the inner branch selected the right *diagnostic message*
(`run_locally.sh:725-726` logged "SDK norm lookup failure(s)" for `lt_rc=4`) and set the module's
own `rc=4` (`:727`).

What was false: that 4 reached the caller. `print_summary` returned 1 on any failure
(`:1585-1588`), the caller did `print_summary ... || exit_code=1` (`:1974`), and the script then
did `exit $exit_code`. **`run_locally.sh` exited 1, not 4.** The specific code survived only in
log text.

**Superseded by INFRA-037 (verified against the current file, line numbers moved).** The `lt_rc==4`
branch no longer sets `rc`; it now records a `preprocessing_runoff (long-horizon sync)` FAIL row
directly and lets the module continue (`run_locally.sh:923-931`, `record_result(...)` at `:931`) —
`rc` stays whatever it already was. The overwrite mechanism described above still exists and still
applies to whatever FAIL rows are present: `print_summary` returns 1 on any failure
(`:1823-1826`), and the caller does `print_summary "$pipeline_elapsed" || exit_code=1` (`:2270`).
So the conclusion is unchanged in spirit — the specific inner code (4, or now "FAIL row present")
does not survive as the process's exit status, only as log/summary text — but the mechanism by
which `lt_rc=4` stops setting `rc` at all is a real behavior change this correction predates.

Consequence: any CI gate, cron alert, or wrapper reasoning on `$?` cannot distinguish "SDK norm
failure" from "API failure" (5, which still sets `rc=$lt_rc` at `:920`) from "no records" (2).
Independently found by an out-of-loop review during PREPQ-014 investigation and re-verified here
by direct code reading.

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
has many possible causes, and the equal count of 4 is suggestive only. **At the time this was
written**, long-horizon logged that failure at `logger.debug` (`:356`), so the evidence needed to
distinguish them was never emitted. **Superseded — INFRA-037 lifted this to `logger.warning`**
(verified against the current file: `sync_long_horizon_hydrograph.py:356`, message
`"write_station_monthly_hydrograph: SDK call failed for site %s, skipping. Error: %s: %s"`); the
per-station exception type and message are now emitted at default log level instead of being
suppressed. A counts-only summary at WARNING keyed by
a **normalised reason/stage** (not by exception class — `ValueError` covers several distinct stages
here; see INFRA-024) would still improve on today's per-station raw-exception lines without
exposing station codes.

**`run_locally.sh` does not propagate exit 4 as the process status — and, per INFRA-037, no longer
even tries to.** Verified against the current file (line numbers moved since this was written): the
inner maintenance function logs the "SDK norm lookup failure(s)" diagnostic and records a
`preprocessing_runoff (long-horizon sync)` FAIL row directly, without ever assigning `rc=4`
(`run_locally.sh:923-931`); `print_summary` returns 1 whenever any FAIL row is present (`:1823-1826`)
and the caller does `print_summary "$pipeline_elapsed" || exit_code=1` (`:2270`), so the process
exits 1. So the observed `failed (exit 4)` line still describes only the **inner** diagnostic; the
wrapper's final process status is **1**, via a different mechanism (a FAIL row) than the one
originally described here (an `rc=4` assignment later overwritten). This issue's "Contract not to
break" note — that exit codes 2/4/5 "are consumed by `run_locally.sh`" — is true only for the inner
branching and logging, not for the final exit status.
Anyone changing the exit-code mapping should add a shell-level test of the *final* exit contract;
the existing test inspects only the function body
(`apps/preprocessing_runoff/test/test_run_locally_long_horizon_wiring.py`).

**Net operator-visible effect at the time this was written**, and a reportability defect filed
independently of the virtual-station question: the operator saw short-horizon WARNINGs that were
harmless to the exit code, plus an exit-4 error line whose four causing events were never printed
(DEBUG-only), on a run whose actual process status was 1. Three separate signals, none of which
pointed at the others. **Superseded — INFRA-037 lifted the causing events to `logger.warning`**
(verified: `sync_long_horizon_hydrograph.py:356`), so they are now printed at default log level;
the process-status split (exit-4 diagnostic vs. exit-1 actual status) is unaffected and still
stands.

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

> **SUPERSEDED (2026-08-21), then superseded again (2026-08-21, second pass).** This fix-space
> enumeration predates PREPQ-015's design entirely. A first PREPQ-015 revision implemented a refined
> form of option 2 below (grade a raise using the SDK's own `get_virtual_sites()` list); a second
> revision the same day **dropped that grading after three proposed reclassification designs — the
> exception itself, local config, and the SDK's virtual-site list — were reviewed and refuted**.
> **None of options 1-3 below is implemented.** PREPQ-015 ships only a fall-through fix (write the
> station's records, keep status `SDK_FAILED`) and documents the permanent exit-4 alarm for
> structurally normless stations as an accepted, unsolved limitation. Kept verbatim for the
> reasoning trail; see PREPQ-015's "Grading mechanisms considered and rejected" for why each
> reclassification design was rejected.

The fix space (for the owner to choose, if reclassification is ever revisited) was one of:

1. Classify virtual-only codes out of the norm-requiring work list before the call.
2. Treat a path-unset result for a known virtual code as not-applicable rather than `SDK_FAILED`,
   so it stops contributing to exit 4.
3. Extend the SDK/service to resolve norms for virtual sites — note the upstream cost recorded
   above and the station-identity hazard in open decision 5.

All three were behaviour changes and none was attempted here at the time, nor by PREPQ-015. Options
1 and 2 differ in where the knowledge of "this code is virtual" lives: option 1 needs the work-list
builder to keep the hydrological/virtual distinction it currently discards at
`setup_library.py:1544-1545`, whereas option 2 would infer it at the failure site — which open
decision 4 argues is currently unsafe, since the exception class alone cannot distinguish a
virtual-only code from an auth failure or a malformed row. A grading form of option 2 (against the
SDK's authoritative virtual-site list, not the exception) was tried and refuted for a different
reason — see PREPQ-015's "Grading mechanisms considered and rejected", design 3.

### Open decisions for the owner (historical)

Items 1-2 resolved 2026-08-17; items 3-4 were provisionally resolved by PREPQ-015's first
2026-08-21 revision, then reopened by its second 2026-08-21 revision, which dropped reclassification
entirely (see the SUPERSEDED box above); item 5 remains open. None of these still "block the fix" in
the sense the original heading implied — PREPQ-015's fall-through fix is unblocked and
implementable on its own; only reclassification, if ever revisited, would need items 3-4 answered
again.

**1. ~~Are virtual stations expected to carry SDK discharge norms at all?~~ — RESOLVED 2026-08-17
(owner):** **Yes, they should be able to.** Since the SDK cannot do it today, this is accepted as an
**upstream iEH HF gap**, not a SAPPHIRE defect. Action: request the capability from the SDK
developer — **sent and acknowledged 2026-08-17**, awaiting their answer
(`doc/prod/iehhf_virtual_station_norms_request.md`, incl. a dated addendum correcting two
overstatements in what was sent). Note the framing caveat recorded there: calling this an "upstream
gap" prejudges question 1; "not supported by design" remains a possible and acceptable answer.

Consequence for the fix space above: **option 3 is the desired end state**, and options 1-2 were
interim measures at best. This paragraph's warning against "classify virtual out" refers
specifically to **option 1** (pre-filter the code out of the work list before the call ever
happens) — that would suppress the signal the owner wants to keep (a virtual station gaining a norm
later). **PREPQ-015 avoids option 1 too**: the norm call is still always attempted for every
station, including known-virtual ones, so no signal is suppressed — but as of its second 2026-08-21
revision it performs no reclassification at all (see the SUPERSEDED box above), so this is no
longer "a grading form of option 2"; it is neither option 1 nor option 2.

**2. ~~Which entrypoint is authoritative for "maintenance"?~~ — RESOLVED 2026-08-17 from the
crontab documentation.** **The long-horizon script does not run in daily production maintenance at
all.** Exhaustive grep for `sync_long_horizon_hydrograph` across `apps/` and `bin/` (excluding tests
and the module itself) returns these invocation sites:

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
> `doc/deployment.md:922` schedules the 01 Jan 03:00 slot as
> `run_periodic_maintenance.sh monthly_norms`, but Luigi's task map handles only
> `long_term`/`skill_recalc`/`snow_norms` and **raises `ValueError` for `monthly_norms`**
> (`apps/pipeline/pipeline_docker.py:2049-2057`). `bin/README.md:231` names a *different* script for
> the same slot (`yearly_runoff_hydrograph_aggregation.sh`). A deployment following `deployment.md`
> would therefore never run the yearly long-horizon aggregation. See
> `doc/plans/issues/high_prio_gi_draft_infra_yearly_monthly_norms_cron_unmapped.md`.

**3. Per horizon, should a virtual site keep its observed rows with `norm=None`?** — **RESOLVED by
PREPQ-015's design: yes, but via fall-through, not grading (updated 2026-08-21, second pass).** At
the time this was written the two horizons disagreed: long-horizon `NORM_ABSENT` wrote the 12
monthly rows and preserved any stored norm (`sync_long_horizon_hydrograph.py:337-417`, plus derived
seasonal/quarterly rows, `:552-599`), while `SDK_FAILED` skipped the station entirely (`:572-579` at
the time — PREPQ-015 removes that skip). PREPQ-015's fall-through contract now writes the same
observed rows for `SDK_FAILED` as for `NORM_ABSENT`, without reclassifying either status — a station
keeps its observed rows regardless of which of the two it lands in. Short-horizon's separate
`skip-without-norm` behaviour (`sync_short_horizon_hydrograph.py:619-652`) is untouched by
PREPQ-015 and remains a distinct, unresolved question for that horizon.

**4. How should a "known virtual" code be identified?** — **No longer answered by PREPQ-015 (its
first 2026-08-21 revision proposed `get_virtual_sites()`; its second 2026-08-21 revision dropped
identification/grading entirely — see the SUPERSEDED box above).** At the time this item was
written it could not be identified reliably: the virtual `Site` builder does not pass `site_type`
(defaults to `"default"`) and does not set `is_virtual`, which config serialization then defaults to
false (`forecast_library.py:7931`, `:6041`, `setup_library.py:811`), and `resolve_sdk_station_codes`
discards the `Site` objects entirely, keeping only codes (`sync_long_horizon_hydrograph.py:683-691`).
That gap is unchanged today; PREPQ-015 no longer needs to close it, since it performs no
classification by virtual status at all.

**5. The SDK's `sites[0]` fallback is a station-identity hazard** (`sdk_endpoint_definitions.py:101-108`).
It accepts the first returned UUID without checking that the row's station code matches the one
requested. It was presumably meant for automatic/manual variants of the same station, but any change
that widens this lookup could silently attach **another station's norms**. If option 3 is chosen,
require a test proving that norms requested for placeholder `19999` can never be served from a
different code.

---

## Second reproduction + owner input (2026-08-18, kyg, trunk `a304ffb0`)

`bash apps/run_locally.sh maintenance:preprocessing_runoff` against the tunnelled kyg iEH HF,
11m 08s, **0 tracebacks**. The daily gap-fill half succeeded cleanly ("Preprocessing completed
successfully", 569s). The whole module FAIL came from the long-horizon sync, whose own counters
were:

```
total_attempted=62   written=53   norm_absent=5   sdk_failed=4   api_failed=0
```

| Outcome | Count | How the code treats it |
|---|---|---|
| Written OK | 53 | success |
| Monthly norm **absent** | 5 | DEGRADED → warning, exit 0 |
| SDK norm lookup **failed** | 4 | ERROR → **exit 4** → module FAIL |
| API read/write failures | 0 | — |

All 4 failures carry the identical `No path provided or the provided path is None` signature this
issue describes, so the condition reproduces on a second date, on trunk, at the same cardinality
(**4** sites), independently of the 2026-08-14/15 observations.

**Owner input, recorded (2026-08-18):** norm lookup succeeds only where the hydromet actually
stores norms in the iEH HF database, and **virtual stations do not get norms at all — that is
expected, not a fault**. Discharge, likewise, may legitimately be missing for some stations.

What that does and does not settle:

- It **confirms the class**: for a virtual station there is no norm to fetch, and no amount of
  fixing on our side produces one. So the exit-4 severity is being applied to a condition the
  owner considers normal.
- It does **not** establish membership — that these particular 4 codes are exactly the org's
  virtual-only set. Item 2 below stands as written; confirming it still needs a live
  `stations/{org}/virtual` listing.
- It sharpens **item 3 from a policy question with no premise to a policy question with one**:
  the code already draws the distinction (`norm_absent` → degraded success, `sdk_failed` → exit 4),
  and for a virtual station both mean the same thing — *there is no norm, and that is fine*.
  The consequence is that a scheduled kyg run fails every time on 4 sites while 53 sites' data
  was written successfully, and the non-zero exit masks that success.

**Recommendation (owner's call, not applied):** treat "no norm because the site is virtual" as
degraded success alongside `norm_absent`, and keep exit 4 for a genuine SDK/transport failure.
That needs a way to identify virtual sites at that call site, which is the same lookup item 2
requires — so items 2 and 3 are one piece of work, not two.

**Reportability, worse than recorded at the time this was written.** Item 3 noted that `SDK_FAILED`
was logged at DEBUG and so was invisible at default level. **INFRA-029** showed the effective level
for this very file is WARNING: `sync_long_horizon_hydrograph.py:100-104` calls
`logging.basicConfig(level=logging.INFO)` *after* importing `setup_library` (`:35`), which makes it
a no-op, so **INFO** lines are discarded too — `logger.info("Resolved %d SDK-only station(s)", …)`
at `:696` (line numbers have moved), for one. The counters quoted above survived only because the
summary block is emitted with **`print`** (`:781`, moved), bypassing logging entirely — so
bumping `SDK_FAILED` from DEBUG to INFO alone would **not** have fixed it; only WARNING (or print)
clears the effective floor. **Superseded for the `SDK_FAILED` event specifically — INFRA-037 logs
it at `logger.warning`** (verified: `sync_long_horizon_hydrograph.py:356`), which does clear that
floor, so this one event is no longer dark. The broader INFRA-029 finding is otherwise unaffected:
this module's INFO lines (e.g. the "Resolved %d SDK-only station(s)" line above) are still
discarded, and the summary counters still survive only via `print`, not the logger.

## Third confirmation — direct probe of `_lookup_monthly_norms` (2026-08-21, kyg)

A read-only probe called the module's own `_lookup_monthly_norms` directly, once per station in
the long-horizon work list, against live kyg iEH-HF — a more targeted check than the full-run
observation in "Second reproduction" above, which only observed the aggregate counters.

- **62 stations attempted: 53 `VALID`, 5 `NORM_ABSENT`, 4 `SDK_FAILED`** — this reproduces the
  2026-08-18 field report's counts exactly (62/53/5/4/0), on a different date, confirming the
  cardinality is stable, not a one-off.
- **All 4 `SDK_FAILED` stations raised the identical exception**:
  `ValueError: No path provided or the provided path is None` — consistent with the single
  mechanism this issue's "Root cause" section already traces to `_get_site_uuid_for_site_code`
  (`ieasyhydro_sdk/sdk_endpoint_definitions.py:90-109`) returning no usable UUID.
- **3 of the 4 are virtual stations**, present in the deployment's *local* virtual-stations config.
  This settled, for those 3, the membership question item 2 in "Open decisions for the owner" left
  open ("mechanism proven, membership NOT established") — for 3 of the 4, membership was established
  by config, not merely inferred from the count symmetry.
- **The 4th was not flagged virtual** in that local config, despite raising the identical exception.
  **Superseded (2026-08-21) by a stronger, direct check**: calling the SDK's own authoritative
  `get_virtual_sites()` (`ieasyhydro_sdk/sdk.py:208-213`) against the same live kyg iEH-HF returns 6
  virtual sites, and **all four** of the failing stations — including this one — are in that list.
  So the local virtual-station config and the station library are **both wrong** about this one
  station: the config omits it, and the library flags it non-virtual, while the SDK's own registry
  includes it. That is an owner data-fix for those two local artifacts, out of scope for both this
  issue and PREPQ-015. Do not name the station in any file.

**What this confirms, precisely — and what it does not.** This issue's hypothesised cause — the
two-registry seam, where `site_uuid` resolves only via the hydrological registry while the
long-horizon work list also injects virtual-registry codes — is confirmed for the failing-set
cardinality (4 of 62), the exception signature (identical across all 4), and SDK-side virtual-list
*membership*: all 4 are confirmed virtual by the SDK's own `get_virtual_sites()` list, a direct
authoritative check rather than an inference from matching counts or from local,
since-shown-unreliable config files. **Not verified**: the 4 stations' *absence* from the
hydrological registry itself. `get_virtual_sites()` establishes list membership, not registry
absence — a code could in principle appear in both registries (see "What can make a code
unresolvable" above: overlap resolves normally). No live query against
`stations/{organization_uuid}/hydrological` was made for these 4 codes, so the two-registry seam is
confirmed as the *mechanism*, not confirmed as *this station set's specific cause* beyond what the
identical exception signature already implies.

**What this changes structurally, not just evidentially.** If a virtual station indeed has no
per-site UUID in the hydrological registry — plausible, but per the caveat above not independently
verified for these 4 — its norm lookup would raise on **every future run** until the upstream
registry or the site's status changes, not intermittently. A plain fall-through fix (write the
station's rows, keep `SDK_FAILED`, keep exiting 4) is therefore insufficient on its own to clear the
alarm for a station like this: it stops the data loss but leaves a `long-horizon sync` FAIL row that
never clears, which is alarm fatigue by construction and would let a genuine future SDK outage hide
inside an alarm nobody reads anymore. **PREPQ-015's 2026-08-21 revision does not solve this.** Three
designs to reclassify a raise for a structurally normless station were proposed and each was
refuted (see PREPQ-015's "Grading mechanisms considered and rejected"); PREPQ-015 ships the
fall-through fix only and records the permanent FAIL row as an accepted, documented limitation, not
a resolved one.

**Relationship to PREPQ-015, stated plainly**: this issue (PREPQ-014) explains *why* the SDK norm
lookup raises for these stations. PREPQ-015 is what stops that raise from discarding a station's
month/quarter/season rows. As of its 2026-08-21 revision, PREPQ-015 does **not** stop the raise from
recurring as a permanent FAIL row for structurally normless stations — that outcome was considered
and explicitly accepted as a known limitation, not fixed, after the reclassification designs that
would have fixed it were reviewed and refuted.

**Disposition note, not a recommendation.** This confirmation makes the underlying condition more
precisely understood, not more fixable on its own: nothing about "confirmed cause" makes any of the
4 stations any less structurally normless, and the owner has already ruled once (2026-08-18) that a
virtual station lacking a norm is expected, not a fault. PREPQ-015 does not reclassify this
condition — it only stops the raise from discarding data. Whether this issue's own disposition
should change (e.g. from "defect to fix" to "expected behaviour, permanent FAIL row accepted") is
left to the owner; this file's Status is unchanged here.

## What to inspect

> Items 1 and 2 are **answered** in "Root cause" above; item 3's factual half (are they
> distinguishable in code?) is answered — yes — leaving only the policy half. Item 4 is untouched.

1. ~~Which path variable is unresolved~~ — **answered**: the SDK's per-site `site_uuid`. It is
   unresolvable whenever the hydrological station lookup yields no usable UUID — which is **four**
   distinct conditions, not just registry absence (non-200, empty list, missing/null UUID on the
   matched row, missing/null UUID via the `sites[0]` fallback). See "Root cause".
2. ~~Whether the 4 affected sites differ structurally~~ — **mechanism proven; virtual-list membership
   CONFIRMED for all 4** (updated 2026-08-21, see "Third confirmation" above). A direct probe first
   tied 3 of the 4 `SDK_FAILED` sites to the deployment's *local* virtual-stations config by name; a
   second, stronger check — calling the SDK's own authoritative `get_virtual_sites()` directly,
   rather than trusting the local config — confirmed the 4th as virtual too, and showed the local
   config and the station library are both wrong about it (see "Third confirmation"). No site in
   this set remains open as "automatic-only or malformed-row" by elimination against those two
   sources. Whether virtual status is *itself* what causes the raise — via absence from the
   hydrological registry specifically — was not independently verified for these 4; see the caveat
   in "Third confirmation" above.
3. Whether (a) and (b) should share an exit code at all — today only (a) reaches exit 4. They are
   cleanly separable in code; this is now purely a policy question. The reportability defect noted
   above — long-horizon logged its `SDK_FAILED` at DEBUG, so the events causing exit 4 were
   invisible at default log level — is **superseded**: INFRA-037 lifted it to `logger.warning`
   (verified: `sync_long_horizon_hydrograph.py:356`), so those events are printed at default log
   level now. Nothing left to fold in on that front; the remaining half of this item is the policy
   question alone.
4. Whether a failed *secondary* sub-task should mark the whole maintenance target FAIL when
   the primary gap-fill succeeded (cross-reference PREPG-009's partial-failure question).

## Acceptance criteria

- **NOT satisfied by PREPQ-015's design (revised 2026-08-21, second pass).** The original
  criterion — the 4 sites either produce norms, or are explicitly classified as not-applicable and
  stop contributing to the failure count — is **not** met: PREPQ-015 dropped reclassification after
  three proposed designs were reviewed and refuted (see its "Grading mechanisms considered and
  rejected"). The 4 sites keep contributing to `SDK_FAILED`'s exit-4 count on every run,
  indefinitely — an accepted, documented limitation (PREPQ-015's "Accepted cost"), not a resolution
  of this criterion. This issue's own acceptance criterion stays open.
- Path-unset and data-absent conditions are reported distinctly — already true in code
  (`NORM_ABSENT` vs `SDK_FAILED`/exit 4 vs exit 2). **True again as of PREPQ-015's 2026-08-21 second
  revision**: with reclassification dropped, PREPQ-015 keeps `SDK_FAILED` and `NORM_ABSENT` fully
  separate outcomes — it does not collapse one into the other for any station, virtual or not.
- A run whose primary gap-fill succeeded is distinguishable from one where it did not — unaffected
  by PREPQ-015, which only changes long-horizon's own status handling.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` green.

## Contract not to break

- Exit codes 2 / 4 / 5 are already consumed by `run_locally.sh`
  (`run_maintenance_preprocessing_runoff`); do not renumber without updating that mapping.
  **Qualified 2026-08-17, re-verified 2026-08-21 after INFRA-037 shipped:** they are consumed for
  *inner branching and logging* only — the wrapper's final process status is 1 whenever any FAIL row
  is present, because `print_summary`'s return overwrites it (`run_locally.sh:1823-1826,2270`; the
  `lt_rc==4` branch itself no longer assigns `rc` at all — see the exit-code correction in "Root
  cause"). A change here needs a shell-level test of the final exit contract; the existing test
  inspects only the function body.
- The 30-day gap-fill must keep running to completion regardless of hydrograph norm failures —
  it did here, and that ordering is what saved the review.
