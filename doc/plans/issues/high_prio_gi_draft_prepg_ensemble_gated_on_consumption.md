# PREPG-023: The ensemble forecast is downloaded and made fatal on every run, on deployments that never consume it

**Status**: Draft (2026-09-03, revised after out-of-loop review)
**Module**: `apps/preprocessing_gateway/Quantile_Mapping_OP.py`
**Priority**: **High** — on kghm, `preprocessing_gateway` has exited 1 on **every recorded run that
reached ensemble processing** since at least 2026-06-16. Since P-007 landed (PR #478, 2026-08-24)
that exit code is no longer discarded by Luigi, so the gateway task now fails for real — and
`RunMLModel`, `RunAllMLModels`, `RunLongTermForecast` and `ConceptualModel` all `require()` it.
**Labels**: `preprocessing_gateway`, `ensemble`, `config-gating`, `upstream-data-gap`
**Found**: 2026-09-03, investigating a `bash apps/run_locally.sh preprocessing_gateway` failure on kghm.
**Related**: **PREPG-016** (nothing counts ensemble members), **PREPG-010/015** (the three DG call
sites and the API-key redaction on their error paths), **P-007** (the exit-code propagation fix that
turns this from silent into blocking), **INFRA-038** (the env-var-parsed-many-ways defect class that
C1 must not worsen).

---

## Summary

Two independent problems, one symptom:

1. **Upstream/unknown**: the Data Gateway returns **no temperature files** for the ENS stream, so
   `merge_ensemble_forecast` aborts the module (`Quantile_Mapping_OP.py:301-303`).
2. **Ours**: the module downloads and hard-requires ensemble data on deployments where **nothing
   consumes it**. kghm runs with `ieasyhydroforecast_run_CM_models=False` — the conceptual model,
   the consumer, is switched off — yet `ieasyhydroforecast_HRU_ENSEMBLE` still names two HRUs and drives
   up to 100 Data Gateway model calls per run and a fatal exit.

This issue fixes (2) and **investigates** (1). It does not assume (1) is a defect on our side;
Phase 0 exists to find out.

## Evidence (measured 2026-09-03, kghm)

The run downloaded 51 files into `intermediate_data/data_gateway`:

```
  1 …2026-09-03.csv     <- control member (different endpoint)
 50 …_tp.csv            <- ensemble precipitation, models 1-50, first HRU only
  0 …_2t.csv            <- ensemble temperature: NONE
```

Filenames follow `ECMWFIFS_<YYYYMMDD>_ENS<n>_HRU<code>_<var>.csv` (real HRU codes are omitted
throughout this file — one of this deployment's ensemble HRUs is also a live station code; read the
real values from the env file or your own log), which the parser at
`Quantile_Mapping_OP.py:277-286` reads correctly — `elements[-1]` → `tp`, `elements[-2][3:]` → the
HRU, `elements[-3][3:]` → the member. **The parsing is not the bug**: no
`"Variable … not recognized"` warning (`:294`) appears in the log, so every file was classified, and
none was `2t`.

The abort order also proves precipitation arrived intact: `P_ensemble.empty` is checked *first*
(`:298-300`) and did not fire.

**The control member is unaffected.** It uses a different client method —
`client.operational.get_control_spinup_and_forecast` (`:794`) — and delivered both variables
(1008 P + 1008 T records written to the API) in the same run. So temperature exists for the
*operational/control* stream and is missing only from the *ENS* stream
(`client.ecmwf_ens.get_ensemble_forecast` → `api/calculations/ecmwf/template/RSMinerva/links?…&source=ENS`,
`sapphire_dg_client/ecmwf.py:42-59`).

**Only the first of the two configured HRUs was attempted.** The module exits inside the merge for
HRU 1 (`:301-303`) before reaching HRU 2, so the observed run made **50** model calls. The
configuration is 50 model calls **per HRU per date attempt** (`:910-918`); 100 is therefore the
floor for a two-HRU run that completes, not a ceiling — `_call_with_transport_retry` allows two
attempts each, and the `yesterday` fallback (`:928-947`) re-runs the loop for a date. Do not quote a
fixed call count in the fix's justification; quote the shape.

**This has never worked in the recorded history.** Every gateway run in `apps/logs/log.2026-*` that
reached ensemble processing logged exactly one `No temperature data found` per run:

| Log date | runs reaching ensembles | `No temperature data found` |
|---|---:|---:|
| 06-16, 06-19, 06-23, 06-25, 06-26, 06-29, 07-01, 07-15, 07-23, 07-24, 08-16 | 1 each | 1 each |
| 06-21 | 2 | 2 |
| 08-13 | 4 | 4 |
| 08-14, 08-24 | 3 | 3 |
| 08-18 | 6 | 6 |
| 08-20 | 22 | 22 |
| 08-21 | 5 | 5 |
| 09-03 | 1 | 1 |

Zero successful ensemble runs **in the recorded logs** — but those are historical samples, not a
census, and the live probe below shows the gap is **intermittent per date**. Do not read this table
as evidence of a permanent condition; read it as evidence that it has been happening for months. (This is a
statement about **these kghm logs under the observed missing-temperature response** — valid P/T
input reaches the writes and exits 0 at `:983-1036`.)

## Why this is blocking now, not merely wasteful

`PreprocessingGatewayQuantileMapping.execute_with_retries` writes its Luigi `output()` marker
**only** on `exit_status == 0` (`pipeline_docker.py:384-400`), and after the retries are exhausted it
**raises `RuntimeError`** (`:421-429`). So a non-zero gateway exit fails the task hard, leaves
`output()` unwritten, skips the daily marker (`:566-578`), and everything that requires it does not
run:

- `RunMLModel` (`:730`, requires at `:743`)
- `RunAllMLModels` (`:776`, requires at `:786`)
- `RunLongTermForecast` (`:2148`, requires at `:2231`) — the **operational/undated** form only; the
  dated recovery form returns no gateway dependency
- `ConceptualModel` itself (`:679`, requires at `:685`)

One escape hatch exists: if a daily marker from an earlier successful run is already present,
`get_gateway_dependency()` returns `ExternalPreprocessingGateway` and the dependents proceed
(`:147-155`).

Before P-007 (PR #478) `run_docker_container` discarded the container's `StatusCode`, so this
failure was invisible and the pipeline carried on. `pipeline_docker.py:333` now reads it. **This is
a predicted P-007 rollout-fallout instance**: a task that was failing silently every day is about to
start failing visibly, and it gates the forecast models.

> **Verify before assuming production is affected.** The log evidence above is from a developer
> machine. Any deployment with `ieasyhydroforecast_HRU_ENSEMBLE` set to real HRUs is exposed if its
> DG also returns no ENS temperature — confirm per deployment in Phase 0, do not assume.

## Immediate workaround (no code change)

Setting `ieasyhydroforecast_HRU_ENSEMBLE=None` makes the loop break on its first iteration
(`:906-907`) and the module exit 0. On a deployment with `run_CM_models=False` this loses nothing.

**Trap: the literal string `"None"` is required — leaving the variable unset crashes.**
`ENSEMBLE_HRUS.split(",")` at `:751` is called on `os.getenv`'s `None` and raises `AttributeError`.
That fragility is part of what C1 replaces.

## Who consumes the ensemble CSVs

The ensemble stage's final forcing outputs are two CSVs per HRU (`:1020-1025`):
`{code}_P_ensemble_forecast.csv` and `{code}_T_ensemble_forecast.csv` in
`ieasyhydroforecast_OUTPUT_PATH_ENS`. (The stage has other side effects too — raw DG downloads into
`OUTPUT_PATH_DG` at `:909-920`, and logging.) They are **not** written to the API: the meteo schema
has no `ensemble_member` dimension
(`doc/plans/issues/archive/high_prio_gi_draft_prepg_meteo_forecast_not_in_api.md:100`).

**The consumer is the conceptual model, and it is in THIS repository** at `apps/conceptual_model/`
— it is built and run from repo source (`apps/conceptual_model/Dockerfile:17,30`), not from an
external checkout:

- `run_operation_forecasting_CM.R:148-149` builds the filenames as
  `paste0(Code, Sys.getenv("ieasyhydroforecast_FILE_PF_P"))` and `…_FILE_PF_T`;
- `:181-184` passes them to `process_forecast_forcing` as `file_path_Ptot` / `file_path_Temp`;
- `functions/functions_operational.R:477` and `:483` `read_csv` them, and `stop()` if either is
  empty; `:451-457` stops when the forcing files are missing entirely.

kghm's env file sets `ieasyhydroforecast_FILE_PF_P=_P_ensemble_forecast.csv` and
`ieasyhydroforecast_FILE_PF_T=_T_ensemble_forecast.csv`, closing the loop.

> **Correction, recorded so nobody repeats it.** An earlier revision of this issue claimed nothing
> in the repository reads these files, based on a literal grep for `_P_ensemble_forecast.csv`. That
> grep could not find the reader **because the suffix is supplied by an env var and never appears
> as a literal in the R source.** A filename-literal search is not a proof of no-consumer for any
> config-driven path in this codebase.

## Phase 0 findings — Q1 ANSWERED by live probe (2026-09-04)

**The premise of the first revision was wrong.** It assumed the Data Gateway simply has no ENS
temperature for these HRUs — a structural, per-station gap. A read-only probe of the links endpoint
(no downloads; filenames only) shows it is **intermittent and per-date**.

One ensemble HRU, model 1, fifteen consecutive dates:

| Outcome | Days | Dates |
|---|---:|---|
| both `_2t.csv` and `_tp.csv` | 9 | 04, 02 Sep; 29, 27, 26, 24, 23, 22, 21 Aug |
| **`_tp.csv` only — no temperature** | **4** | **03 Sep, 30, 28, 25 Aug** |
| no data published at all (endpoint errors) | 2 | 01 Sep, 31 Aug |

Both configured ensemble HRUs behaved **identically** on every date checked, so the gap is driven by
the date, not the station.

Three consequences that change this issue:

1. **It is not structural.** Roughly one day in four lacks temperature; the rest are complete. A run
   today would succeed. Any wording implying a permanent per-HRU gap is wrong.
2. **It does not backfill.** Re-querying 03 September on 04 September still returns `_tp.csv` only.
   A missing day stays missing, so a retry the next day does not repair it.
3. **The consumer would be starved on ~27% of days.** When the conceptual model *is* enabled, the
   current abort-on-first-failure behaviour means those days produce **no ensemble forcing at all**
   — which is exactly what C2 exists to fix, and raises C2's value well above "tidier reporting".

**The endpoint returns HTTP 200 with a partial list** (one link instead of two) rather than
erroring, so nothing upstream of `merge_ensemble_forecast` notices. That is **PREPG-016**'s subject
(nothing counts what came back); this issue's C1/C2 mitigate the symptom, not the blindness.

**Still open, and now the right question to ask the Data Gateway team**: why does ENS temperature
fail to publish on roughly a quarter of dates, and why does it never backfill? File that as an
upstream request with the dates above — it is not a defect on our side.

**Probe method, for repeatability**: call the links endpoint directly
(`api/calculations/ecmwf/template/RSMinerva/links?hru_code=<hru>&date=<ISO>&source=ENS&models=<n>`)
via the installed client's `_call_api`, read `resp.json()` and record only `filename` suffixes.
Never download, never print links (they may carry tokens) or the key. Credentials come from
`ieasyhydroforecast_API_KEY_GATEAWAY` and `SAPPHIRE_DG_HOST` in the deployment env file — note the
spelling of that variable, it is not `..._GATEWAY`.

## Phase 0 — investigation

**Access prerequisites — the owner must supply these before P0 can start.** A live DG API key and
host for each deployment to be probed, network access to the Data Gateway, and the authority to file
an upstream request. The DG client is a private dependency tracking a **moving `@main`**
(`apps/preprocessing_gateway/pyproject.toml:30`), resolved to a commit only in
`apps/preprocessing_gateway/uv.lock:649` — so probe results are tied to whatever `main` was at lock
time, and should be recorded with that commit. Deployment evidence that cannot be
gathered from this machine (tjhm/uzhm behaviour, production env values) is an **owner input**, not
an agent task. P0's written output goes in this issue file, under a dated "Phase 0 findings" heading.

**Q1 — why is there no ENS temperature? ANSWERED above for kghm.** What remains: whether the
same intermittency appears on tjhm/uzhm, and the upstream request. Probe read-only. Reproduce the caller's real arguments —
a probe with invented parameters manufactures an outage (see the `dg_control_member_spinup_date_trap`
precedent, where exactly that produced a false alarm).

- Call the links endpoint for **each HRU named in `ieasyhydroforecast_HRU_ENSEMBLE`** (read them
  from the deployment's env file — do not hard-code them here), `source=ENS`, for today and
  several past dates, and record the `filename` values in `resp.json()` **before** any download
  (`sapphire_dg_client/client_base.py:37-46`). Is `2t` absent from the listing, or listed and
  failing to download?
- Vary one axis at a time: HRU, date, model number. All HRUs? All dates? All 50 members?
- `client_base.py:43` does `requests.get(file_resp.get("link"))` and **never checks the response
  status**, so a 404/500 body would be written to disk under the correct filename. Rule this in or
  out. It is *not* what we see today (there is no `2t.csv` at all), but it matters for any
  deployment that differs.
- Check whether `get_raster_forecast(parameter="2t", …)` (`ecmwf.py:61-84`) is the endpoint that
  carries ENS temperature — the client declares `_raster_parameters = {"2t", "tp"}`.
- Record whether this reproduces on tjhm/uzhm or is kghm-only.

Outcome: either (a) an upstream request to the Data Gateway team, filed like
`doc/prod/iehhf_virtual_station_norms_request.md`; or (b) a client/endpoint defect on our side,
filed as its own issue. **Do not fold the fix for either into this issue.**

**Q2 — is the conceptual model the only consumer?** The in-repo half is **answered above**. What
remains: confirm no *deployed* frozen `sapphire-conceptmod` image or operator script reads the CSVs
by a different route, and confirm no deployment runs the R program directly (see C1's scope caveat).

## The contract

**C1 — process ensembles only when something declares it needs them.** *(Owner decision,
2026-09-04.)*

Gate the entire ensemble block — download included — on **either** of:

1. the conceptual model being enabled (`ieasyhydroforecast_run_CM_models`), **or**
2. a **new** variable, `ieasyhydroforecast_ensemble_forcing_required`, **default off**, by which any
   other consumer declares that it needs ensemble forcing.

If the conceptual model runs, ensemble forcing is processed regardless — it is the known consumer
and must never be starved by a flag someone forgot to set. If the conceptual model is off **and**
nothing else has declared a need, the ensembles are not downloaded and not processed.

The new variable exists so the gate does not have to guess. It covers the cases the conceptual-model
flag cannot see: a deployment running the model outside Luigi (`apps/conceptual_model/README.md:97-105`
documents direct invocation), and any future consumer of `{code}_{P,T}_ensemble_forecast.csv`. Its
default is **off**, so no existing deployment starts processing ensembles because of this change,
and kghm — where the conceptual model is off — stops.

> **Parse it once, the same way as the CM flag** (case-insensitive `true`, default off), and add
> both names to the same helper. Do not introduce a fifth spelling of a boolean; see the truth table
> below.

Also `HRU_ENSEMBLE` must still name at least one HRU — see C1a for what happens when a consumer
declares a need and no HRUs are configured.

**C1a — the gate open with no ensemble HRUs is a configuration error, not a quiet skip.** This
applies when **either** gate input is on — the conceptual model enabled, *or*
`ieasyhydroforecast_ensemble_forcing_required` set — not only the former. Exiting 0
with the gate open but no HRUs merely moves the failure downstream: the conceptual model gets its
basin codes from its own JSON config (`run_operation_forecasting_CM.R:114-129`), not from
`HRU_ENSEMBLE`, and stops when the forcing files are missing
(`functions_operational.R:451-457`). So when **either gate input is on** and `HRU_ENSEMBLE` is
unset/empty/`None`, **fail loudly in the gateway** with a message naming both variables — unless P0
establishes that the conceptual configuration contains no basins.

When the gate is closed, log exactly one INFO line naming both variables and the values that closed
it, then skip to the end. Skipped is not the same as succeeded: it must be legible why no ensemble
CSVs were produced.

**C2 — when ensembles ARE required, one bad HRU must not abort the module mid-flight.**

**Scope decision (make it explicitly): C2 covers the entire per-HRU unit, download through write** —
not only the missing-variable check. Changing `merge_ensemble_forecast` alone does not deliver the
stated contract, because the download path exits independently at `:958` (yesterday-fallback `ValueError`) and
`:964` (non-matching `ValueError`) — note `:954` is the `except`, the `sys.exit` is at `:958`, and exhausted transport errors plus
parse/quantile-map/write exceptions propagate straight out of the loop.

Wrap the whole per-HRU body so that any stage failure is recorded and the loop continues.
**Preserve the existing retry and redaction behaviour exactly** — `_call_with_transport_retry` and
`dg_utils.redact_api_key` on every error path (PREPG-010/015); a rewrite that logs a raw exception
message here reintroduces the API-key leak those issues closed.

Today's failure calls `sys.exit(1)` from inside `merge_ensemble_forecast` (`:300`, `:303`), which:

- leaves a **partial write** reported as a total failure — the control member's CSVs (`:871-872`)
  and its API writes (`:875-890`) are already durable and are not rolled back;
- never attempts the remaining HRUs (kghm's second ensemble HRU has never been tried);
- gives the operator one line and no summary of what did or did not land.

**Return type and counting unit — pin both, they are what the tests assert.**
`merge_ensemble_forecast` returns `(DataFrame | None, set[str])`: the merged frame (or `None`), and
the set of variables that were missing (a subset of `{"P", "T"}`; the empty-`files_downloaded` case
returns `(None, {"P", "T"})`). The run summary counts **per HRU**, with the missing variables named
per failing HRU. For a two-HRU run where the first HRU has `tp`-only files and the second succeeds:
`ensemble_hrus_attempted=2 written=1 failed=1`, plus one line naming the failed HRU and `{T}`.

Exit non-zero **after** the loop if any required HRU failed.

**C3 — a skipped ensemble stage is exit 0.** With the gate closed there is no ensemble failure to
report, and `preprocessing_gateway` reports on the control member alone.

**C4 — do not change what the ensemble stage computes.** Quantile mapping, gap filling
(`dg_utils.fill_gaps_grouped`), the CSV schema, and the output paths stay exactly as they are. This
issue changes *whether* and *how loudly*, never *what*.

## Files that may be modified

Implementation:

- `apps/preprocessing_gateway/Quantile_Mapping_OP.py`

Tests — all four are required, not optional; the first two break in **P1** and the last two in
**P2** if left untouched:

- `apps/preprocessing_gateway/test/test_integration_preprocessing_gateway.py` — the
  `gateway_env_ensemble` fixture (`:364-379`) sets HRUs but **not** `run_CM_models`, so a
  default-disabled gate stops those tests reaching the download.

  > **Do not simply add `run_CM_models=true` to that fixture.**
  > `test_control_member_connection_error_retried_and_recovers` reuses it, sets
  > `ieasyhydroforecast_HRU_ENSEMBLE = "None"` (`:1604`) to exercise the control-member site alone,
  > and asserts **exit 0** (`:1628`). Under C1a that combination is a loud config failure, so a
  > blanket fixture change turns this passing test red. Set the flag per test (or give the fixture a
  > parameter), and leave this one with the gate closed.
- `apps/preprocessing_gateway/test/test_transport_retry.py` — the `dg_call_site_env` fixture
  (`:517-529`) omits `run_CM_models` for the same reason.
- `apps/preprocessing_gateway/test/test_ensemble_transforms.py` — **every direct caller of
  `merge_ensemble_forecast` in this file changes**, not only the failure cases. C2's tuple return
  means each success-path test must unpack `(frame, missing)` instead of using the return value as a
  DataFrame (`:157`, `:172`, `:179`, `:201`, `:212`, `:226`, `:251`), and the three `SystemExit`
  assertions must become status assertions: `test_empty_files_list_exits` (`:145`),
  `test_no_precipitation_files_exits` (`:182`), `test_no_temperature_files_exits` (`:189`).
- `apps/preprocessing_gateway/test/test_integration_preprocessing_gateway.py:1482-1508` — the
  transport-exception expectations, which C2's wider handling may change.
- new `apps/preprocessing_gateway/test/test_ensemble_consumption_gate.py`

Documentation: `apps/preprocessing_gateway/README.md`, `doc/configuration.md`.

P0 additionally writes to **this issue file** (its "Phase 0 findings" section) and may create one
new `gi_draft_*` file under `doc/plans/issues/` plus its `doc/plans/module_issues.md` row, for
whichever of Q1's two outcomes applies.

**Do not** change `dg_utils`, the DG client, `pipeline_docker.py`, `bin/setup_docker.sh`,
`apps/conceptual_model/`, or the control-member path. Fixing the four `run_CM_models` parsings is
INFRA-038's job — this issue only avoids adding a fifth.

## Tests

1. **Gate closed → no downloads.** `run_CM_models` unset / `False` / `false` / `no`: the DG client's
   `get_ensemble_forecast` is **never called**, no ensemble CSVs are written, module exits 0, and
   the skip is logged once naming both variables and their values.
2. **Gate open, case-insensitive, via EITHER input.** Assert the client is called for each of:
   `run_CM_models=True`; `run_CM_models=true`; `run_CM_models` off with
   `ieasyhydroforecast_ensemble_forcing_required=true`; and both on. Assert it is **not** called when
   both are off or unset — including when `ensemble_forcing_required` is absent entirely, which is
   its default. This is the full truth table and it pins both the OR and the default-off.
3. **Gate open, no HRUs → loud config failure (C1a).** Test via **each** gate input separately
   (`run_CM_models=true`, and `ensemble_forcing_required=true` with the CM flag off) with
   `HRU_ENSEMBLE` unset / empty / `None`: exits non-zero with a message naming the gate input that
   opened the gate and the missing HRU variable. **Not** exit 0.
4. **Gate closed with `HRU_ENSEMBLE` unset does not crash** (`:751` raises `AttributeError` today).
5. **Missing temperature, ensembles required → non-fatal per HRU.** Two ensemble HRUs, the first
   with `tp`-only files: the second is still attempted, the summary reads
   `attempted=2 written=1 failed=1` naming the failed HRU and `{T}`, and the module exits non-zero
   **after** the loop.
6. **Missing temperature, ensembles not required → exit 0** and no download attempt at all.
7. **Control-member work survives an ensemble failure.** With ensembles required and failing, assert
   the control-member API write still happened and is reported.
8. **Missing precipitation** takes the same non-fatal path (the `P` branch at `:298-300` is the
   sibling of the `T` branch and must not be left calling `sys.exit`).
9. **Empty `files_downloaded` still fails its HRU** — returns `(None, {"P","T"})` rather than
   exiting the module, and the next HRU is still attempted.
10. **A download `ValueError` fails only its HRU (C2 scope).** The `:954`/`:964` paths record the
    failure and continue; assert the redaction helper is still applied to the logged message.

## Acceptance criteria

- [ ] Phase 0 answered Q1, and Q2's deployed-image half, in writing in this file.
- [ ] `bash apps/run_locally.sh preprocessing_gateway` on kghm (`run_CM_models=False`, the new
      variable unset) exits **0**, downloads **no** ensemble files, and logs one line naming both
      gate inputs and their values.
- [ ] Setting `ieasyhydroforecast_ensemble_forcing_required=true` alone, with the conceptual model
      off, processes ensembles — the whole point of the new variable.
- [ ] The new variable is documented in `doc/configuration.md` with its default (off).
- [ ] With `run_CM_models=true` and the DG still returning no `2t`, the module attempts **both**
      ensemble HRUs, prints the summary, and exits non-zero.
- [ ] The Luigi gateway task writes its success marker on a kghm-shaped run, unblocking
      `RunMLModel` / `RunAllMLModels` / `RunLongTermForecast`.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` — zero failures,
      zero unexpected skips — **at the end of each phase, not only at the end**; then the full
      `run_tests.sh`.
- [ ] `ruff check` / `ruff format --check` clean on every changed file.

## Phases

- **P0 — investigation.** No code. Depends on: the owner supplying the access prerequisites above.
  Agents: 1 (read-only DG probe). Accept: Q1 and Q2 answered in this file; any upstream request or
  client defect filed as its **own** issue.
- **P1 — the consumption gate (C1, C1a, C3).** Files: `Quantile_Mapping_OP.py`, the new gate test
  file, **and the `gateway_env_ensemble` / `dg_call_site_env` fixtures**. Depends on: P0 (Q2 could
  invalidate the gate). Agents: 1. Accept: tests 1-4, 6 pass and the pre-existing ensemble and
  transport tests still pass.
- **P2 — non-fatal per-HRU failure (C2).** Files: `Quantile_Mapping_OP.py`,
  `test_ensemble_transforms.py` (the three `SystemExit` tests), `test_integration_…py`
  (including `:1482-1508`). Depends on: P1. Agents: 1. Accept: tests 5, 7, 8, 9, 10 pass **and** the
  rewritten `SystemExit` tests pass.
- **P3 — documentation.** Files: `README.md`, `doc/configuration.md`. Depends on: P1, P2. Agents: 1.
  Accept: the gating rule, the direct-invocation caveat, and the **new** disabled/unset semantics are
  documented. Document what the code does after this issue — **not** the retired `None`-vs-unset
  trap, which C1 removes.

```json
{
  "phases": {
    "P0": { "depends_on": [], "parallel_agents": 1 },
    "P1": { "depends_on": ["P0"], "parallel_agents": 1 },
    "P2": { "depends_on": ["P1"], "parallel_agents": 1 },
    "P3": { "depends_on": ["P1", "P2"], "parallel_agents": 1 }
  }
}
```

## Open decisions for the owner

*(Decision 1 — "skip the download entirely, or download and merely stop failing?" — was **answered
2026-09-04**: skip entirely, gated on the conceptual model **or** a new
`ieasyhydroforecast_ensemble_forcing_required`, default off. The earlier proposal of a separate
`force_ensemble_download` override is withdrawn: the new variable already serves that purpose, and a
third flag would be one more boolean to parse inconsistently.)*

*(Decision 2 — "should a failing ensemble block ML and long-term forecasting?" — was **answered
2026-09-04**: no. Filed as **INFRA-046**.)*

Nothing further is open on this issue. Phase 0's remaining work is the upstream request and the
tjhm/uzhm check, both described above.

## Corrections applied after out-of-loop review (2026-09-03)

- **Blocker.** The claim that no in-repo code reads the ensemble CSVs was **false** —
  `apps/conceptual_model/` is the reader, built from repo source. The original grep missed it
  because the filename suffix comes from an env var. Q2's in-repo half is now answered rather than
  investigated, and the "external repository" framing is gone.
- **Blocker.** C2 originally changed only `merge_ensemble_forecast`, which does not deliver
  "one bad HRU must not abort" — the download paths exit independently at `:954`/`:964`. C2 now
  states its scope explicitly and requires the retry/redaction behaviour to be preserved.
- **Blocker.** The allowed-files list and phases could not keep the suite green: two fixtures omit
  `run_CM_models` and would stop reaching the download under a default-disabled gate. Both are now
  named and assigned to P1.
- C1 now gates early — `OUTPUT_PATH_ENS` creation (`:688-694`) and `HRU_ENSEMBLE` parsing
  (`:739-751`) both precede the block a `:896` gate would cover.
- C1a added: CM enabled with no HRUs is a config error, since the conceptual model sources its
  basins from its own JSON and would fail downstream anyway.
- The `run_CM_models` truth table grew from two readers to four (`setup_library.py` and the
  dashboard's `config.py` were missing), and the safety claim is now scoped to the Luigi path.
- C2's return type and counting unit are pinned, resolving a contradiction between the contract
  (per `(HRU, variable)`) and test 5 (per HRU).
- Corrected citations: abort `:301-303`; warning `:294`; control-member call `:794`; `sys.exit`
  calls `:300`/`:303`; P branch `:298-300`; archive path prefix; `setup_docker.sh:90` for the
  default. Corrected "2×50 calls" to "configured for up to 100; the observed run made 50";
  "discards control-member work" to "leaves a partial write"; "only products" to "final forcing
  outputs"; and the run command to `bash apps/run_locally.sh …`.

## Corrections applied after the confirm-fixes pass (2026-09-03)

- **Fixture conflict that would have turned a passing test red.**
  `test_control_member_connection_error_retried_and_recovers` reuses `gateway_env_ensemble`, sets
  `HRU_ENSEMBLE="None"` and asserts exit 0 — exactly the combination C1a makes a loud failure. P1
  must set the flag per test, not on the shared fixture.
- **C2's tuple return breaks every direct `merge_ensemble_forecast` caller**, not only the three
  `SystemExit` tests; all seven success-path tests must unpack `(frame, missing)`.
- "Up to 100 DG calls" was not a true ceiling — retries and the `yesterday` fallback exceed it. The
  count is now stated as a shape, not a number.
- The DG client tracks a moving `@main` (`pyproject.toml:30`); only `uv.lock:649` pins a commit.
  Probe results must be recorded against that commit.
- P0's own output targets (this file, plus a possible new issue file and index row) are now in the
  allowed-files list.
- **One reviewer claim was checked and rejected**: the log evidence table cites
  `apps/logs/log.2026-*`, which the reviewer said should be
  `apps/preprocessing_gateway/logs/log.2026-*`. Both directories exist and both contain the error;
  `apps/logs/log.2026-08-20` holds the 22 occurrences the table reports. Citation left as written.

## Corrections applied after the decision-fold review (2026-09-04)

- D1's OR-gate was in the contract but not in C1a, the tests or the acceptance criteria; all three
  now exercise **both** gate inputs and the default-off. The stale `force_ensemble_download`
  proposal is withdrawn.
- Removed the self-contradiction between "not intermittent" in the log table and the live probe's
  one-day-in-four finding.
- Citations: the ensemble `sys.exit` is `:958` (`:954` is the `except`); operational
  `RunLongTermForecast.requires()` is `:2231`, and the dated-recovery form has no gateway dependency.

## Out of scope

- Fixing the missing ENS temperature (Q1's outcome — upstream request or a separate client issue).
- Counting ensemble members (**PREPG-016**).
- Unifying the four `run_CM_models` parsings (**INFRA-038**).
- Adding the `ensemble_member` dimension to the meteo API.
