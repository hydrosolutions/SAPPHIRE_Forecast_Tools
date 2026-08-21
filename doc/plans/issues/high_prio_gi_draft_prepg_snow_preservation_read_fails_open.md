# PREPG-020: The snow anti-clobber guard fails open — one failed API read nulls a year of norms

**Status**: Draft (2026-08-21)
**Module**: `apps/preprocessing_gateway` (`dg_utils.py`) — with a service-side half that is
**colleague-owned**, see § Where the fix goes
**Priority**: **High** — a *transient* API read failure during a maintenance run silently replaces
stored `norm`, statistics and elevation bands with `NULL`, across the **full 365-day** window, for
every snow code. The run reports success.
**Labels**: `preprocessing_gateway`, `snow-data`, `data-integrity`, `silent-failure`, `api`
**Found**: 2026-08-21, by the (late) post-implementation review gate for **PREPG-007**.
**Related**: **PREPG-007** — widened this window from 30 to 365 days and claimed an anti-clobber
guarantee. **PREPQ-011/012** — the same defect shape on *runoff*, already fixed gateway-side; its
service-side follow-up is the precedent for this one.

---

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
- there is no quality or provenance ranking, so any non-null but *weaker* value also passes.

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

**Gateway-side (this repo, the actionable half):** make preservation **fail closed** for any
overwrite-capable mode. If the existing-row read fails, abort that write request rather than
proceeding with nulls. Operational mode (`date >= yesterday`) is narrow and may keep current
behaviour — decide deliberately and state it.

**Service-side (colleague-owned — do NOT edit `sapphire/services/**`):** the deeper fix is an
omit-aware update so an absent key means "leave unchanged" rather than "set NULL". **PREPQ-011/012
set the precedent**: fixed gateway-side first, with the service-side change raised separately.
Follow that, and note the precedent's explicit caution — an omit-aware PATCH, **not** a blanket
`COALESCE`.

## Acceptance criteria

- A maintenance write whose preservation read raises does **not** call `write_snow` — assert the
  write is skipped, not merely that an exception was logged.
- A partial row (main value missing, one band present) does **not** null the stored main value.
- A successful preservation read still writes exactly what it writes today — the happy path is
  byte-identical.
- Operational mode's behaviour is unchanged, whichever way the decision goes, and a test pins it.
- `test_api_read_failure_does_not_block_write` is **replaced** by a test asserting the new contract,
  with a comment naming this issue so the reversal is not mistaken for a regression.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green, zero skips.

## Contract not to break

- **Do not make a failed read silently skip the whole run.** Failing closed must be *loud* — this
  module already has a documented history of exit-0-on-failure (PREPG-009), and trading a
  data-integrity bug for a silent no-op would be no better.
- Do not add a `COALESCE`-style blanket fill service-side; PREPQ-011/012 explicitly rejected that.
- The 365-day maintenance window itself is correct and must stay — this issue is about what the
  write does when preservation is unavailable, not about the window.

## Not covered

Whether any deployment has already been affected. That needs a check for rows where `value` is
present but `norm`/bands are NULL inside the last 365 days, per environment — and note that empty
bands on a server can also be the pre-existing one-time-backfill gap, which is a *different* cause
with the same symptom.
