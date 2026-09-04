## `preprocessing_gateway` reports PASS with all six snow tasks errored (PREPG-009)

**Status**: Draft (2026-08-14)
**Module**: `apps/preprocessing_gateway` (`snow_data_operational.py`), surfaced via
`apps/run_locally.sh`
**Priority**: **Medium** — silent-success on a fully failed sub-task. Not data loss, but the
operator signal is wrong, and the companion validation defect (INFRA-026) means nothing
downstream catches it either.
**Labels**: `preprocessing_gateway`, `snow`, `error-handling`, `silent-success`
**Found**: 2026-08-14, local kghm review on `maxat_sapphire_2` @ `8e3fc1bc`.
**Related**: **INFRA-026** (validation passes on the resulting norm-only rows — the two
together turn a run that wrote no snow at all into an entirely green one). Same silent-success family
as PP-051 / PP-054 / LR-010.

> **Provenance correction (2026-08-16).** The checkout moved from `maxat_sapphire_2` to
> `fix_lr010_lr011_write_contract` at **2026-08-14 16:00** (git reflog), so every run from the
> full-history recalc onward executed on that branch (now `849c8736`), **not** on trunk as the
> line above states. That branch's diff vs trunk touches only
> `apps/linear_regression/linear_regression.py`, `apps/iEasyHydroForecast/forecast_library.py`,
> their tests, and docs — **none of the files this issue concerns** — so the finding holds
> identically on trunk. Recorded for accuracy of the audit trail, not because the conclusion changes.

---

## Observation

`snow_data_operational.py` logged **six ERROR pairs** — every HRU × variable combination
(SWE, HS, RoF for two HRUs) — then finished normally:

```
ERROR - Error getting snow data from Data Gateway for HRU <X>, SWE: Failed to get data from
        api/calculations/snow-operational/… : {"message": "Operational data for HRU <X> is not
        available for date 2026-08-09 00:00:00!", "success": false}
ERROR - Failed to get snow data for HRU <X>, SWE
… x6 …
INFO  - Snow data processing complete (6 tasks)
```

Result: `preprocessing_gateway completed in 1m 42s` → **PASS**, exit 0. Zero snow values
were written; the stored SWE series still ends at its previous non-null date.

The **upstream cause is not ours** — the SAPPHIRE Data Gateway reports operational snow data
unavailable from the requested date. That is a data-availability condition to report, not a
bug to fix here. The defect is that it is reported as success.

> **Recurrence + corrected diagnosis (2026-09-04).** The condition recurred, and measuring it
> properly changed what it *is*. It is **not** a blanket outage: a live sweep of the last 26 days
> (`snow-forecast` endpoint, one HRU per org) found **24 of 26 days present**, with exactly two
> absent — **2026-09-01** and the current day, identically for kghm and tjhm. So there is a
> **one-day hole at 2026-09-01**, plus today not yet published.
>
> **Why one missing day costs the entire fetch.** `get_operational` accepts only a `start_date` and
> returns all-or-nothing from there through today+forecast, so an interior hole voids the whole
> response. The window cannot be shrunk past it, because a separate spin-up precondition rejects
> recent start dates. Measured on kghm/SWE:
>
> ```
> start <= 2026-08-28  ->  blocked by the 2026-09-01 hole
> start >= 2026-08-29  ->  blocked by spin-up ("No reanalysis data available for the given HRU code and date!")
> ```
>
> **No start date works** — no viable window remains between the two constraints.
>
> **Measurement scope, so the generalization is auditable.** Measured directly: (i) the start-date
> squeeze above, on **kghm/SWE only**; (ii) `get_operational` failing on 2026-09-04 for **all four
> HRUs x three variables across both orgs**, every one naming 2026-09-01 as the first missing day;
> (iii) the 26-day availability sweep, on **one HRU per org**. From (ii) the hole is org- and
> variable-independent, and the endpoint contract in (i) is not parameterised by HRU or variable —
> so the conclusion that operational snow is currently unfetchable for every HRU, variable and
> deployment is a **well-supported inference, not a per-HRU measurement**. Treat it as an upstream
> escalation; worth telling the operators that one absent run has that blast radius.
>
> **The 2026-09-01 hole is real, and the evidence is status-code level.** Re-probed capturing HTTP
> status and body rather than "the client raised" (kghm, snow-forecast, SWE):
>
> ```
> 2026-08-29/30/31   HTTP 200  data returned
> 2026-09-01         HTTP 400  {"message": "No data found for the given HRU code, date and parameter!"}
> 2026-09-02/03      HTTP 200  data returned
> 2026-09-04 (today) HTTP 400  same "No data found" body
> ```
>
> The 400 carries an explicit semantic message, so this is the **server asserting absence**, not an
> unexplained failure.
>
> **Beware the collapsed-exception trap when re-measuring** — it bit both sessions that looked at
> this. `sapphire_dg_client`'s `_call_api` raises a bare `ValueError` on **any** non-200
> (`client_base.py:59-60`), so a probe that records only the exception cannot tell "no data
> published" from a 4xx/5xx. The 26-day availability sweep quoted above was written that way and
> establishes only *"did not return 200"* for its two absent days; the status/body probe here is what
> upgrades 09-01 to demonstrated absence. **Identical in shape to the iEasyHydro SDK trap documented
> in PREPQ-014** — different SDK, same failure to distinguish error from absence.
>
> **Cross-product corroboration, stated at its true strength.** A parallel session probed the Data
> Gateway **ensemble links** endpoint and found 2026-09-01 also failing there — evidence that 09-01
> is a gap in the gateway's own production for that day rather than a snow-product fault. That probe
> recorded exception type only, so it establishes *non-200*, not absence. Their 2026-08-31 ensemble
> failure is an **unexplained non-200 and must not be read as absence** — snow on 08-31 returned
> **HTTP 200 with data** (above), so whatever happened there is ensemble-specific. Escalate 09-01 as
> the day both products lost; do not claim a two-day outage.
>
> **Not a total data absence.** The same days are retrievable through other endpoints: the
> 2026-09-03 forecast returned all three variables for both orgs with real non-zero values covering
> 2026-09-03→2026-09-12. `get_snow_reanalysis` additionally supports `end_date`. A fallback able to
> ride out a one-day hole is therefore *possible* — recorded as a known recovery path if this
> recurs, **not** proposed as work here.
>
> **Operational exposure**: tjhm ingested a forecast tail on 2026-09-01 reaching 2026-09-09, so it
> coasts until then; kghm was already stale at 2026-08-29.
>
> The defect this issue describes is unchanged and was re-verified the same day: a direct run
> against kghm logged 6/6 `Error getting snow data` pairs and **exited 0**. The 2026-08-09 instance
> resolving on its own by 2026-08-17 is evidence about one occurrence, not about the class.
>
> **Probe trap, if anyone re-measures this.** `get_operational` takes only `start_date`. A start date
> within roughly five days of today returns the *spin-up* error above, which is easily misread as
> the outage. Only a production-shaped call (start ~365 days back) reproduces the real
> `not available for date` message.

## Why "6 tasks complete" is the wrong summary

`Snow data processing complete (6 tasks)` counts tasks *attempted*, not *succeeded*. An
operator reading the tail of the log sees a completion line and a PASS. The six ERROR lines
are 15 lines above it and are not reflected in any exit code, summary, or downstream check.

## Proposed fix

1. Track succeeded/failed counts and emit a summary that states both
   (`Snow data processing complete: 0/6 succeeded`).
2. Exit non-zero when any task failed — **one aggregate status, not a graded code**
   (decided 2026-09-04; see the correction below for why graded codes were rejected).
3. No caller changes. `run_locally.sh` and `pipeline_docker.py` already surface a non-zero
   status correctly; nothing needs to learn a new code.

> **Why graded codes were rejected (2026-09-04, out-of-loop review).**
> No current caller can act on a graded code. `pipeline_docker.py:384-428` treats every non-zero
> status except `124` identically — retry, notify, raise, write no marker — and
> `run_locally.sh:1831-1904,2345-2350` reduces any recorded failure to a final exit 1. A
> "warn-level" code for *upstream has no data yet* would therefore still page and still fail
> Luigi, buying nothing over a plain non-zero. The original text proposing that distinct codes
> are "the important one" has been removed as unimplementable without a separate, justified
> orchestration change. **DECIDED (owner, 2026-09-04): single non-zero aggregate** — emit one
> non-zero status and name the failed stages and succeeded/failed counts in the log
> (`Snow data processing complete: 0/6 succeeded`). Luigi behaviour and the marker contract stay
> as they are. Changing caller semantics to distinguish partial from total failure is explicitly
> **out of scope**.

> **Scope of "succeeded/failed" (2026-09-04).** Define a task failure as the existing `False`
> return from the fetch/read/local-CSV path. `get_snow_data_operational()` currently returns
> `True` even when `write_snow_to_api()` returns `False`, when the consistency check fails, or
> after a caught `SapphireAPIError` (`snow_data_operational.py:381-400`). Counting API delivery
> as failure is a **larger** change that would alter what a green run means — out of scope here;
> file separately if wanted.

## Acceptance criteria

- A run in which all snow tasks fail does **not** report PASS.
- A partial failure is distinguishable from full success and from total failure **in the log**
  (named stages plus succeeded/failed counts) — **not** in the exit status, which is a single
  aggregate by decision. Do not add a criterion requiring distinct exit codes.
- Data outputs and the exit behaviour of a fully-successful run are unchanged — **except** the
  completion summary line, which necessarily changes (so "byte-identical" cannot be the bar).
- A test at `main()` level proving all-tasks-failed exits non-zero.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.

## Contract not to break

- Do not make a snow outage abort the whole gateway run: `Quantile_Mapping_OP.py` and
  `extend_era5_reanalysis.py` succeeded in this same invocation and their output is needed
  downstream. The exit signal must communicate partial failure without discarding good work.

## Incidental hygiene finding — **ACTIONED, do not re-file**

The Data Gateway `api_key` was logged in cleartext. The caught path now redacts (measured
2026-09-04: cleartext stops at `log.2026-08-15`; the six errors from that day's failed kghm run
are `api_key=***`). Related: **PREPG-015** (shipped), **PREPG-017** (residual uncaught path),
**PREPG-014** (upstream cure).
