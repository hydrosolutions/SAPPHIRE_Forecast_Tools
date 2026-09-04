# Report to the SAPPHIRE Data Gateway team: three data gaps observed 2026-08/09

**Status**: **Draft — not yet sent.**
**Reported by**: SAPPHIRE Forecast Tools
**Concerns**: `data-gateway.ieasyhydro.org` — snow-operational, snow-forecast and ensemble
endpoints; and the `sapphire-dg-client` Python client
**Origin**: PREPG-009 and PREPG-024 — see
[`doc/plans/issues/mid_prio_gi_draft_prepg_snow_task_failures_exit_zero.md`](../plans/issues/mid_prio_gi_draft_prepg_snow_task_failures_exit_zero.md)

Three independent items, most urgent first. Item 1 is currently blocking snow ingestion on every
SAPPHIRE deployment.

---

## 1. One missing day — 2026-09-01 — makes operational snow unfetchable entirely

**The ask: please backfill the 2026-09-01 run.**

A single absent day is not a small gap here, because `snow-operational` accepts only a
`start_date` and returns all-or-nothing from there through the forecast horizon. One missing
interior day voids the whole response, and the window cannot be shortened past it because a
spin-up precondition rejects recent start dates. Measured on HRU `00003`, SWE, on 2026-09-04:

```
start_date <= 2026-08-28   ->  "Operational data for HRU 00003 is not available for date 2026-09-01 00:00:00!"
start_date >= 2026-08-29   ->  "No reanalysis data available for the given HRU code and date!"   (spin-up)
```

**No start date works.** Until 2026-09-01 is restored, no SAPPHIRE deployment can fetch
operational snow from this endpoint at all — for any HRU or variable.

The day is genuinely absent rather than erroring. Probing `snow-forecast` directly and recording
HTTP status and body (HRU `00003`, SWE):

```
2026-08-29 / 30 / 31   HTTP 200   data returned
2026-09-01             HTTP 400   {"message": "No data found for the given HRU code, date and parameter!"}
2026-09-02 / 03        HTTP 200   data returned
```

Everything on both sides of 09-01 is present and healthy, so this looks like one lost or skipped
run rather than an outage.

**It is not snow-specific.** A probe of the **ensemble links** endpoint over 15 dates also failed
on 2026-09-01, which suggests the gap is in that day's production generally. That probe recorded
only the client exception, not the HTTP status, so for the ensemble endpoint we can state
"did not return 200" rather than confirmed absence.

**Worth considering on your side** (not a request, an observation): if `snow-operational` could
skip an interior missing day instead of refusing the range, a single lost run would degrade one
day of output rather than stop ingestion entirely.

## 2. Ensemble forecasts published without temperature on some dates

**The ask: is precipitation-without-temperature expected, and can those days be completed?**

Ensemble forecast files are published for some dates with **precipitation but no temperature**.
The August dates were **still incomplete when re-probed on 2026-09-04**, days after publication, so
this is not simply a publication delay — but we have not watched a single date long enough to say
temperature is *never* added, and we are not claiming that.

Dates observed: **2026-09-03, 2026-08-30, 2026-08-28, 2026-08-25**. We are not putting a rate on
it — those are the dates we have checked, not a measured frequency over a defined period.

This is a positive observation, not an inference from a failure. On every request the endpoint
returned **HTTP 200 with a well-formed body**; the returned file list simply contained only a
`_tp.csv` entry and no `_2t.csv`. Re-probed capturing HTTP status across **6 ensemble members
(1, 5, 13, 27, 44, 50) x both configured HRUs x the four dates — 48 requests**:

```
2026-09-03   both HRUs, 6 members each  ->  ['tp.csv']   HTTP 200
2026-08-30   both HRUs, 6 members each  ->  ['tp.csv']   HTTP 200
2026-08-28   both HRUs, 6 members each  ->  ['tp.csv']   HTTP 200
2026-08-25   both HRUs, 6 members each  ->  ['tp.csv']   HTTP 200
```

Not one `_2t.csv` on any member, on either HRU, on any of the four dates.

For **2026-09-03 the observation is exhaustive**: that day's gateway run downloaded the full set
and left **50 `_tp.csv` files and zero `_2t.csv`**, covering all 50 members directly. For the three
August dates the coverage is a **6-of-50 member sample per HRU** — enough to establish that
temperature is absent rather than sparse, but not a per-member census. We can extend it to all 50
members on request; we have not, because it would not change the ask.

For us this is a hard stop, not a degradation: our quantile-mapping step needs both variables and
exits on the missing one, which is what an operator sees:

```
ERROR - No temperature data found in the ensemble forecast files.
```

If temperature is genuinely unavailable for a given run, we would rather the endpoint said so
explicitly than return a precipitation-only bundle that looks complete.

## 3. Client bug: `get_snow_forecast` silently returns HS for every variable

**The ask: a one-line fix in `sapphire-dg-client`.**

`snow_model.get_snow_forecast()` builds its query as `param={parameter.lower()}`, but the
`snow-forecast` endpoint expects `parameter=<UPPERCASE>`. The server accepts the unrecognised
`param=`, returns **HTTP 200**, and serves **HS regardless of the variable requested**:

```
param=hs / param=swe / param=rof   -> HTTP 200, filename HS-Forecast-..., identical sha1 for all three
parameter=HS / SWE / ROF           -> HTTP 200, correct per-variable files, distinct sha1
parameter=hs / swe / rof           -> error (lowercase not accepted)
```

So any caller asking for SWE or RoF receives snow-depth data with a success status. Identical
checksums across three different variables is what exposed it.

Note the two neighbouring methods use the **opposite** convention and are correct as written:
`get_snow_reanalysis` sends `param=` and that endpoint honours it (and ignores `parameter=`);
`get_operational` sends `parameter=<UPPER>`. Only `get_snow_forecast` is mismatched. **Nothing in
SAPPHIRE calls it today**, so there is no bad data in our systems — we found it while
investigating item 1. A server-side alternative would be to reject an unrecognised parameter name
rather than fall back to a default.

---

## Provenance

Stated so you can weigh each claim:

- Items 1 and 3, and the status/body probes: measured directly by SAPPHIRE Forecast Tools on
  2026-09-04 against `data-gateway.ieasyhydro.org`, both the kghm and tjhm HRU sets.
- Item 2, and the ensemble-endpoint result in item 1: measured by a parallel SAPPHIRE session on
  2026-09-04. Item 2's four dates are **verified by direct HTTP probe** (48 requests, all HTTP 200,
  bodies listing precipitation only), not inferred from a failure. The exhaustive 2026-09-03
  on-disk observation was made live during that day's run; it is **not re-checkable now**, because
  the download directory is cleared at the start of every gateway run. Our own module logs
  independently show the temperature error on 2026-09-03. Our local runs are sporadic rather than
  daily, so they neither confirm nor contradict the three August dates; those rest on the probe
  above, which is reproducible.
- One correction we made before sending: an earlier reading of this incident described a blanket
  outage from 2026-09-01 onward. That was wrong — 24 of the 26 preceding days are present. Only
  2026-09-01 is missing (plus the current day, which is normal publication timing). A second
  ensemble date, 2026-08-31, was initially reported as absent and is **withdrawn**: it was an
  uncaptured non-200, and snow returned 200 with data that day.
