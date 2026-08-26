# PREPG-020: The snow anti-clobber guard fails open — one failed API read nulls a year of norms

**Status**: **Complete** (2026-08-26) — fixed in **PR #483**, plan in **#481**, both merged
**Module**: `apps/preprocessing_gateway` (`dg_utils.py`) — with a service-side half that is
**colleague-owned**, see § Where the fix goes
**Priority**: **High** — a *transient* API read failure during a maintenance run silently replaces
stored `norm`, statistics and elevation bands with `NULL`, across the maintenance selection — an inclusive window of normally **366 dates** —, for
every snow code. The run reports success.
**Labels**: `preprocessing_gateway`, `snow-data`, `data-integrity`, `silent-failure`, `api`
**Found**: 2026-08-21, by the (late) post-implementation review gate for **PREPG-007**.
**Related**: **PREPG-007** — widened this window from 30 to 365 days and claimed an anti-clobber
guarantee. **PREPQ-011/012** — the same defect shape on *runoff*, already fixed gateway-side; its
service-side follow-up is the precedent for this one.

---

> **Completed 2026-08-26 (PR #483).** Preservation reads now fail closed in every mode; the same
> fix applied to three destructive reads in the yearly recalculation; and
> `bin/yearly_snow_norm_recalculation.sh` now exits with its container's status.
>
> **Regression evidence — this broke nothing that worked.** Baseline 386 → **401 passed**, with
> **exactly one** pre-existing test changed (the one that existed to assert the fail-open being
> removed). The other 385 pass untouched.
>
> **Mutation matrix** — each production file reverted individually, to prove the tests can fail:
> `dg_utils.py` → 10 failed; `recalculate_snow_norms.py` → 2; `snow_data_renalysis.py` → 1;
> `yearly_snow_norm_recalculation.sh` → 3.
>
> **KNOWN AND ACCEPTED, owner decision:** the explicit
> `except SnowPreservationReadError: raise` in `snow_data_operational.py` is **not pinned by a
> test** — reverting it fails nothing. It is deliberately redundant today (the exception escapes
> `except SapphireAPIError` on ancestry) and exists as defence-in-depth against the SDK's class
> hierarchy changing under a pinned `@main`. Pinning it needs a monkeypatched ancestry test; the
> owner reviewed and declined. **Do not delete the clause as "dead code" — that is the finding, not
> a discovery.**
>
> **Accepted coupling:** the roundtrip test imports colleague-owned
> `sapphire/services/preprocessing/app` read-only and adds sqlalchemy/pydantic/pydantic-settings to
> the gateway's **dev** extras. If the service changes its schema or `crud.create_snow`, this
> gateway test breaks. Accepted because asserting the payload instead of the stored state is what
> let the original defect through.
>
> **OPERATIONAL — expect noise, not breakage.** Runs that previously completed while quietly
> writing nulls now abort and exit non-zero. Recovery is a **manual maintenance-mode re-run**:
> there is no durable API replay, and operational's `date >= yesterday` filter means a missed date
> can fall permanently outside tomorrow's window. **Do not assume it catches up on the next run.**
>
> **Carried forward, not fixed:** `bin/yearly_runoff_hydrograph_aggregation.sh:214-220` has the
> identical `| tee` → `EXIT_CODE=$?` fallback this issue fixed in its snow sibling. It *does* exit
> with the status (`:241`), so it is less exposed, but its fallback when `docker inspect` fails is
> still tee's zero. Out of scope here; worth its own fix.

## The chain, verified end to end on trunk 2026-08-21

**1. The preservation read fails open** (`dg_utils.py:909-916`):

```python
except Exception as e:
    logger.warning(
        "Could not read existing snow metadata from API (%s): %s. "
        "Proceeding without metadata preservation.", snow_type, e,
    )
    return {}
```

Any exception — a timeout, a 5xx, a transport reset — discards the **entire** accumulated
existing-values map for the request and returns `{}`. The write then proceeds with nothing to
preserve.

**2. The record is built with `None` in the unpreserved fields** (`dg_utils.py:1127-1155`).

**3. Omitting a field does not protect it.** `SnowBase` declares every optional field with a
default of `None` (`sapphire/services/preprocessing/app/schemas.py:100-131`), so an absent key
arrives at the service as an explicit `None`, not as "unset".

**4. The upsert assigns every field unconditionally**
(`sapphire/services/preprocessing/app/crud.py:238-257`):

```python
incoming = [item.model_dump() for item in bulk_data.data]
...
if _has_changes(existing, data):
    for k, v in data.items():
        setattr(existing, k, v)
```

A stored `norm` of `42.0` becoming `None` *is* a change, so it is written.

**Net effect:** one transient read failure → `norm`, `q05..q95`, `previous` and the elevation
bands are nulled for every row in the window. The dashboard bands disappear. Nothing errors.

## Why the existing guard does not stop it

The guard at `dg_utils.py:1112-1115` is a **per-row check on incoming data only**: it skips a row
when the incoming main value *and* every incoming band are null. It never compares against what is
already stored. So:

- incoming main value non-null + preservation read failed → row passes the guard, `value`/`current`
  update, and everything else is nulled.
- one incoming band non-null, main value null, read failed → passes the guard; `value`, `current`,
  the other bands, norms and statistics can all be nulled.

## This behaviour is currently pinned by a test

`test_api_read_failure_does_not_block_write`
(`apps/preprocessing_gateway/test/test_api_integration.py:2283-2311`), docstring:
*"read_snow raises → write proceeds with norm=None."*

So the fail-open is **enshrined as intended**, not an oversight. **Any fix must replace that test,
not work around it** — and whoever changes it should read this issue first, because the test looks
deliberate and is.

## Provenance — this is not PREPG-007's bug, but PREPG-007 made it matter

The fail-open predates PREPG-007. What PREPG-007 changed is the blast radius: **30 days → 365
days**, while its own text claims an anti-clobber guarantee that this path does not provide. That
combination is why this is filed High rather than as a latent nit.

## Where the fix goes

**Gateway-side (this repo, the actionable half):** make preservation **fail closed in EVERY
mode**. If the existing-row read fails, do not write that batch.

### FIRST, the distinction the whole fix depends on: a FAILED read is not an EMPTY read

**Both currently produce `{}`, and conflating them breaks cold-database seeding.** Verified: a
successful-but-empty read `continue`s and returns the accumulated map (`dg_utils.py:893`, `:917`);
an exception returns `{}` (`dg_utils.py:909`, `:916`). The caller cannot tell them apart.

So **do not implement fail-closed as `if not existing_snow_fields:`.** That check would pass the
"read raised" test and simultaneously block the legitimate first write into an empty database —
behaviour already pinned by `test_api_integration.py:2252` and `:2274`.

**Required:** `_read_existing_snow_fields` must signal failure out-of-band — raise a dedicated
preservation-read error — and return the dict on every *successful* read, including `{}`.
A successful empty result proves no rows exist for that scope; a failed read proves nothing.

### Operational mode fails closed too — DECIDED 2026-08-26, correcting this issue's own earlier text

*An earlier revision said "operational mode is narrow and may keep current behaviour". That was
wrong, and the code contradicts it.* The preservation read is **not mode-gated** — it runs
unconditionally at `dg_utils.py:1062`, and the comment immediately above it states its purpose:

```python
# Read existing metadata so operational writes don't clobber full-year
# norms/statistics produced by recalculate_snow_norms.py.
```

So operational is not a lesser case; it is **the** case the preservation was written for. Two
consequences the "narrow blast radius" argument missed:

- **The norms are a whole-year artifact by design.** `recalculate_snow_norms.py` writes 365
  records per code per snow type for the calendar year (observed in the 2026-08-21 P3 rehearsal)
  **specifically so the dashboard never has to source norms from a different year.** A hole
  anywhere in that year defeats the reason it is filled.
- **Operational writes land on `yesterday`, `today` and the forecast dates** — few rows, but the
  most-viewed part of the band, and the part with no fallback.

Row count is the wrong measure of blast radius here.

### The mode matrix — state it explicitly, do not infer it

| mode | read RAISES | read succeeds but returns `{}` |
|---|---|---|
| `operational` | **do not write** | write normally |
| `maintenance` | **do not write** | write normally |
| `initial` | **do not write** | **write normally** — this is the cold-seed path (`README.md:73`) |

`initial` is an upsert mode, not create-only, and may be re-run against populated data — so mode
alone never licenses writing nulls. The exemption that matters is *successful-empty*, not *initial*.

**Service-side (colleague-owned — do NOT edit `sapphire/services/**`):** the deeper fix is an
omit-aware update so an absent key means "leave unchanged" rather than "set NULL". **PREPQ-011/012
set the precedent**: fixed gateway-side first, with the service-side change raised separately.
Follow that, and note the precedent's explicit caution — an omit-aware PATCH, **not** a blanket
`COALESCE`.

## Acceptance criteria

- A maintenance write whose preservation read raises does **not** call `write_snow` — assert the
  write is skipped, not merely that an exception was logged.
- **The run reports failure END TO END — raising from the helper is NOT sufficient.** The two
  callers swallow differently, and the distinction decides how much work this is:
  - `snow_data_renalysis.py:369` — `except Exception:` then `return True`. **Unconditional**: it
    swallows the new error whatever its ancestry, so **this call site must change.**
  - `snow_data_operational.py:387` — `except SapphireAPIError:`, commented *"Continue - CSV write
    succeeded, API failure is not fatal"*. **Conditional**: it swallows only if the new error
    derives from `SapphireAPIError`. Defining the error as a plain local exception avoids it
    without editing this site — but **verify that**, do not assume it.

  Pin the outcome with a **script-level** test asserting a non-zero exit — not "an error was logged"
  and not a helper returning `False`. This module has documented form for exit-0-on-failure
  (**PREPG-009**).
- A partial row (main value missing, one band present) **both** preserves the stored main value
  **and** writes the valid incoming band. *Asserting only the first permits an implementation that
  skips the row entirely and silently drops a good band update.* Prefer asserting final stored
  state over asserting that `write_snow` was called.
- A successful preservation read still writes exactly what it writes today — the happy path is
  byte-identical.
- **A successful EMPTY read still writes** (cold database) — the paired test to the one above, and
  the one that catches a falsey-check implementation.
- **Multi-code:** a read failure after one code has already been read successfully produces **no**
  POST for the whole invocation — one `write_snow_to_api` call may issue several GETs, and the
  helper discards everything accumulated on failure.
- At least one test asserts **final stored state** through `SnowBulkCreate` and the service upsert,
  not merely that `write_snow` was or was not called.
- **Operational mode also fails closed** — a failed preservation read on an operational write does
  not write, pinned by its own test. *(Not "unchanged"; see the decision above.)*
- `test_api_read_failure_does_not_block_write` is **replaced** by a test asserting the new contract,
  with a comment naming this issue so the reversal is not mistaken for a regression.
**Recalc — one criterion per destructive read, none of which the write-path tests cover:**

- **Target-year read fails** → no POST for that code/type, and the process exits non-zero.
- **Prior-year read fails** → no POST for that code/type, and the process exits non-zero.
- **Statistics-history read fails** → no POST for that code/type, and the process exits non-zero.
  *This is the one that would otherwise write null percentile bands across a full year.*
- Each asserts **no POST**, not merely "an error was logged" — and asserts the **end-to-end** exit,
  since `recalculate_norms` has only a `finally` and `main` calls it directly, so a correctly
  propagated error does exit non-zero **only if nothing above swallows it**.

**Wrapper:**

- `bin/yearly_snow_norm_recalculation.sh` **exits with the container's status**. Pin it with a test
  driving the script against a failing stub container and asserting a non-zero script exit. Today
  it captures `CONTAINER_EXIT_CODE` (`:136`), logs a WARNING (`:141`) and returns 0 — copy the last
  line of its sibling `bin/yearly_runoff_hydrograph_aggregation.sh:241`.
  *This matters beyond this issue: that wrapper is the recommended command for PREPG-007's P3.*

**Regression gate — the evidence that nothing working is broken:**

- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.
- **Baseline measured on trunk 2026-08-26: `386 passed`.** After the fix the count must be
  **386 + (new tests) − 0**, i.e. no pre-existing test is deleted to make the change pass.
- **EXACTLY ONE pre-existing test may change:**
  `test_api_read_failure_does_not_block_write` (`test_api_integration.py:2283`), which asserts the
  fail-open being removed. **The other 385 must pass untouched.** That is the concrete form of
  "the happy path is byte-identical" — if any other pre-existing test needs editing, the change has
  altered behaviour it was not supposed to touch, and that is a finding to report, not to fix by
  editing the test.
- The replaced test must assert the **new** contract (read raises → no write), not merely be
  deleted.

## The SAME defect exists in `recalculate_snow_norms.py` — in scope, and worse

Found by the 2026-08-26 review; verified. `recalculate_snow_norms.py:180-196` repeats the pattern
exactly:

```python
existing = {}
try:
    api_df = client.read_snow(...)
    ...
except Exception as e:
    logger.warning("Could not read existing snow records for %s/%s: %s", ...)
```

It then builds records with `value=None`, omits bands, and **posts the whole calendar year**
(`:240`, `:299`). Its prior-year read turns a failure into `previous=None` the same way, and the
statistics reads can fail and still yield explicit null statistics.

**Blast radius here is larger than the write path this issue was filed about** — a full year per
code per snow type, in the one job whose entire purpose is producing that year.

**Kept in this issue rather than split**, deliberately: it is the same defect, the same fix shape,
and the same file family. Splitting invites the partial fix — someone repairs the write path, the
recalc keeps nulling a full year, and the issue closes looking done.

### THREE destructive reads in the recalc, not two

| read | failure produces | site |
|---|---|---|
| target-year preservation | `value`, `current`, bands nulled | call `recalculate_snow_norms.py:182`, catch `:193` |
| prior-year | `previous` nulled | call `recalculate_snow_norms.py:207`, catch `:218` |
| **statistics history** | `count/mean/std/min/max/q*` written as **null over the whole year** | call `dg_utils.py:737`, catch `:748` — recalc then "proceeds with NaN stat fields" (`:128`, `:137`) |

*An earlier revision of this issue named only the first two. The stats-history read is the one that
nulls the percentile bands the dashboard actually plots.*

**Rule for all three:** no POST for a code/type may happen when a read needed to preserve or compute
its outgoing fields failed.

**The norm-history read (call `dg_utils.py:597`, catch `:608`) is deliberately NOT in that list.** Its failure drops
the affected variable from `norms_df`, and recalc iterates only what is present — so it produces a
*silently missed update*, not a destructive write. **Out of scope here**; it is a real defect of a
different shape and should be filed separately rather than folded in.

### Every `except Exception` that would swallow a new dedicated error

A dedicated exception only helps if nothing upstream eats it. These all currently would:

**Two different jobs — do not conflate them.**

*(a) ORIGIN handlers — these convert a raw read error into silence today and must instead raise:*

| site | what it currently enables |
|---|---|
| `dg_utils.py:909` | the handler this fix replaces — must raise, not return `{}` |
| `dg_utils.py:748` (stats history) | the null-stat write over the whole year |
| `recalculate_snow_norms.py:193` | null `value`/`current`/bands |
| `recalculate_snow_norms.py:218` | null `previous` |

*(b) UPSTREAM handlers — these would swallow the new error after it is raised:*

| site | behaviour |
|---|---|
| `snow_data_renalysis.py:369` | `except Exception` then `return True` — **unconditional**, will swallow it whatever its ancestry |
| `snow_data_operational.py:387` | `except SapphireAPIError` — **conditional**: it swallows only if the new error derives from that type |

**Therefore define the error as a plain local exception NOT derived from `SapphireAPIError`**, which
neutralises (b)'s second row, and change the reanalysis catch, which the ancestry choice cannot
save you from.

**Therefore: define the error as a plain local exception NOT derived from `SapphireAPIError`**, and
audit each site above rather than assuming propagation.

### The yearly wrapper also masks the failure — one line, with a working sibling to copy

`bin/yearly_snow_norm_recalculation.sh` captures `CONTAINER_EXIT_CODE` (`:136`) and logs a WARNING
when it is non-zero (`:141`) — **but never exits with it**, so it always returns 0. Its sibling
`bin/yearly_runoff_hydrograph_aggregation.sh` does the identical capture and then
`exit "$CONTAINER_EXIT_CODE"` (`:241`).

This matters beyond this issue: **that wrapper is the recommended command for PREPG-007's P3
remediation.** A failed recalc there currently reports *"Recalculation complete."* Copy the sibling's
last line.

## Failure policy — DECIDED 2026-08-26: ABORT the run

**Resolved. Do not re-open it silently; the reasoning is below.**

Failing closed prevents the clobber either way, but the two shapes fail very differently:

| | Behaviour | Risk it accepts |
|---|---|---|
| **Skip the affected write, continue** | that batch is not written; other codes/types still sync; run reports non-zero at the end | the snow data for that batch is stale until the next run — a *freshness* gap |

**The freshness cost is larger than "until the next run", and the issue must say so.** The source
data survives — both callers write the CSV *before* the API call (`snow_data_operational.py:374`,
`snow_data_renalysis.py:351`) — but **there is no durable API retry**. Operational filters to
`date >= yesterday`, so a date missed today may fall outside tomorrow's window and never be written.
Recovery therefore requires an explicit **re-run in maintenance mode**, and the fix must say so in
the docstring; otherwise "it'll catch up next run" is assumed and is false.
| **Abort the whole run** | nothing further is written | one transient API blip stops the entire daily snow sync — an *availability* gap, and the gateway target `break`s on first failure, so ERA5 extension and snow downstream of it do not run either |

The second is how PREPG-010's original incident escalated: a single transient fault took out the
whole module *and* everything sequenced after it. That argues for skip-and-continue with a loud,
non-zero outcome. **But it is an owner call**, because skip-and-continue means the operator sees a
non-zero exit on a run that partially succeeded, and that must not be mistaken for "nothing
happened".

**The 2026-08-26 reviewer argued for abort**, on evidence worth weighing: the SDK already retries
transport errors before raising (`sapphire_api_client/client.py:110`, `:167`), so an exception that
reaches us has already survived retry and is more likely systemic than transient; the container
chains its scripts with `&&` (`Dockerfile:43`); and a non-zero container is retried and eventually
made loud by the Luigi runner — which now actually reports it, since **P-007** merged.

The counter-argument remains that the gateway target `break`s on first failure, so aborting also
costs ERA5 extension and snow downstream — which is exactly how PREPG-010's incident escalated.

**Owner decision 2026-08-26: abort.** The deciding argument is that the SDK has *already retried*
before the exception reaches us, so what we see is more likely systemic than transient — and
continuing through every HRU and variable just repeats a systemic failure while accumulating
inconsistent partial progress. P-007 (merged) means a non-zero container is now actually visible to
Luigi, which was not true when this trade-off was last considered.

**The cost is smaller than an earlier revision of this issue claimed — corrected 2026-08-26.**
That revision said aborting snow also costs ERA5 extension. **It does not.** The container CMD
chains `Quantile_Mapping_OP.py && extend_era5_reanalysis.py && snow_data_operational.py`
(`Dockerfile:43`), and `run_locally.sh:681` orders them the same way — **snow runs last**, so
quantile mapping and ERA5 have already completed by the time snow can abort.

What aborting actually costs is the remainder of the *snow* step: HRUs and variables not yet
processed in this run. That is materially cheaper than the PREPG-010 escalation shape it was
mistakenly compared to, and it strengthens this decision rather than weakening it.

**Recovery is MANUAL and must be documented in the docstring — decided 2026-08-26.** The source
data survives in CSV (`snow_data_operational.py:374`, `snow_data_renalysis.py:351`), but **there is
no durable API-write replay**. Operational filters to `date >= yesterday`, so a date missed today
can fall outside tomorrow's window permanently. Recovery is an explicit **re-run in maintenance
mode**, whose 365-day window picks the missed date back up. **Do not write "it will catch up on the
next run" anywhere — it will not.** A follow-up issue for durable replay is out of scope here.

## Contract not to break

- **Do not make a failed read silently skip the whole run.** Failing closed must be *loud* — this
  module already has a documented history of exit-0-on-failure (PREPG-009), and trading a
  data-integrity bug for a silent no-op would be no better.
- Do not add a `COALESCE`-style blanket fill service-side; PREPQ-011/012 explicitly rejected that.
- The 365-day maintenance window itself is correct and must stay — this issue is about what the
  write does when preservation is unavailable, not about the window.

## Not covered

Whether any deployment has already been affected. That is **forensics, not implementation** — it
does not guide the fix and should not gate it. If someone does look: rows where `value` is present
but `norm`/bands are NULL inside the maintenance window, per environment — and note that empty
bands on a server can also be the pre-existing one-time-backfill gap, a *different* cause with the
same symptom.
