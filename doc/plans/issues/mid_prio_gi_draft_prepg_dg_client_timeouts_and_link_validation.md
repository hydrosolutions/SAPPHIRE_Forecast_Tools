## `sapphire-dg-client` has no request timeouts and never validates the link response (PREPG-014)

**Status**: Draft — **NOT ready to implement** (reviewed 2026-08-20). Split out of PREPG-010,
which is local-only by design. An out-of-loop pass found three HIGH gaps: the timeout values and
policy are unspecified, the link-error exception type is unspecified, and the new timeout
exceptions do not line up with the retry that has since shipped. **Three owner decisions are
required before this can be implemented — see § Owner decisions.**
**Module**: the **`sapphire-dg-client`** dependency (separate hydrosolutions repo), consumed by
`apps/preprocessing_gateway`
**Priority**: **Medium** — *raised from Low 2026-08-18.* No observed incident, which is what kept
it Low, but the reasoning was wrong on two counts:

1. **A hang is worse than a crash.** PREPG-010 makes the gateway survive a *reset*. A peer that
   hangs instead still blocks indefinitely — and unlike a crash it never reports, so cron cannot
   retry and the gateway target's ERA5 and snow scripts never run either.
2. **External lead time.** Separate repo; consumers move only on a relock.

The missing timeout is the half most likely to cause a real operational stall; the unvalidated
link response remains a latent correctness gap.
**Labels**: `preprocessing_gateway`, `dependency`, `network`, `upstream`
**Found**: 2026-08-18, while reviewing PREPG-010.
**Related**: **PREPG-010** — the local bounded retry. That fix stands alone and does **not** depend
on this one, but it explicitly does **not** cover hangs; this issue is where that gets fixed.
**PREPG-015** (shipped, PR #460) — the same client puts the API key in its exception message,
which we then log. **PREPG-017** — an *uncaught* transport exception carries the key-bearing URL to
stderr; this issue makes new exception classes reachable, so it feeds directly into that path.

---

## Why this is separate from PREPG-010

PREPG-010 retries a fault that already crosses the call boundary as a `ConnectionError`. Everything
in *this* issue is invisible from the calling code and can only be fixed inside the client:

- The two `requests` calls are internal (`client_base.py:43`, `:56`).
- A non-200 **link** response is never raised, only written to disk — the caller cannot catch what
  is never thrown.

Keep them apart: filing them together put items in PREPG-010's acceptance criteria that its own
local-only phase could not satisfy.

## Two gaps

### 1. No timeouts anywhere

Only two `requests` calls exist in the whole client (`client_base.py:43` and `:56`); **neither
passes a `timeout`**. A hung peer blocks the process indefinitely rather than failing. This is the
gap most likely to cause a real operational stall.

### 2. The link response is written without an HTTP-status check

```python
def _call_api_and_save_file(self, endpoint: str, directory: str):
    resp = self._call_api(method="GET", endpoint=endpoint)
    for file_resp in resp.json():
        resp = requests.get(file_resp.get("link"))      # no status check
        local_file_path = self._save_file(resp, directory, file_resp["filename"])
```

Note the asymmetry: the sibling `_call_api` **does** validate (`client_base.py:59`, raising
`ValueError` on non-200), but the *link* request does not. `_save_file` writes
`response.content` with no content-type, size, checksum or schema check (`client_base.py:26`).

**Scoped honestly — this is NOT established as silent corruption.** Downstream parsing calls `pd.read_csv` and then assumes a two-column structure and parses dates, so a typical
HTML or JSON error body would most likely raise. The accurate claim is narrower:

> Non-200 link bodies are written as raw forecast-named files before validation. Typical error
> bodies probably fail during parsing, but **no explicit status, content, size or schema check
> guarantees rejection**, so a sufficiently CSV-shaped bad body could pass.

## Where the fix goes

`sapphire-dg-client` is a hydrosolutions-owned git dependency, so upstream is possible. Consumers
move when they **relock**, not when `@main` advances — `uv.lock` pins a specific commit and the
Docker build runs `uv sync --frozen --no-dev`, so the dependency set is reproducible.

The two requests live in **different methods**, so a fix confined to one cannot cover both:

- the **link** request is in `_call_api_and_save_file` (`client_base.py:43`) — add the status check
  here, before `_save_file`;
- the request inside `_call_api` (`client_base.py:56`) — the timeout must be added here (or to its
  interface) as well.

**`_call_api` is NOT "the metadata request" — correcting this issue's own earlier wording.** It
returns link *metadata* only for the ensemble endpoint. For control, ERA5 and snow it returns the
**complete file body**, which is then handed straight to `_save_file`
(`operational.py:14`→`:26`, `era5_land.py:20`→`:28`). This matters directly for the timeout value:
a read timeout tuned for a small JSON reply would be applied to multi-megabyte downloads.

## The exception seam — this issue changes WHICH exception the caller sees

Added 2026-08-20. PREPG-010 shipped after this issue was drafted, so "retry ownership is settled,
do not add retry logic here" is still true but **no longer sufficient**: adding timeouts makes
fault classes reachable that did not previously cross the boundary, and the shipped retry does not
catch all of them. Verified in the module venv:

| New failure | Class crossing the boundary | Retried by shipped PREPG-010? | Caller outcome today |
|---|---|---|---|
| connect stalls | `ConnectTimeout` | **yes** — subclasses `ConnectionError` | retried, then handled |
| **peer accepts then never replies** | **`ReadTimeout`** | **NO** — subclasses `Timeout` only | **uncaught in the ensemble path → default excepthook → PREPG-017** |
| stall while buffering the body | `ConnectionError` (Requests rewraps urllib3's body read timeout, `models.py:827`) | yes | retried |
| non-200 link, if `raise_for_status()` | `HTTPError` | **no** | neither `ConnectionError` nor `ValueError` → **uncaught** |

**The middle row is the one that matters: it is exactly the hang this issue exists to fix.** Left
as-is, PREPG-014 converts "blocks forever" into "dies uncaught, printing a key-bearing URL to
stderr" — an improvement over hanging, but not what a reader would assume, and it lands on
PREPG-017's open path.

## Owner decisions required before implementing

These are genuine forks, not details. **Do not start without them.**

1. **Does `ReadTimeout` join the local retry set?** It is the read-phase hang. Adding it means
   editing shipped PREPG-010 code (`_call_with_transport_retry`'s retryable tuple) — cheap, but it
   changes merged behaviour and needs its own test. Declining it means a read timeout is a loud
   one-shot failure. Either is defensible; silence is not.
2. **What exception does a failed link raise?** `raise_for_status()` gives `HTTPError`, which
   **bypasses both ensemble `ValueError` handlers**. A `ValueError` subclass preserves handler
   coverage but must be guaranteed *not* to contain the sentinel
   `"Couldn't find any files for the given HRU code, date and models!"`, or a link failure would
   silently trigger the today→yesterday fallback. Note the control path catches `Exception`
   broadly (`Quantile_Mapping_OP.py:792-821`), so it is unaffected either way.
3. **What timeout values, and on what evidence?** "Every request carries an explicit timeout" is
   satisfied by `timeout=None`. Values must be positive and finite, and **connect and read must be
   set separately**, because a read timeout is an *idle interval between socket reads*, not a total
   deadline (`urllib3/util/timeout.py:76-103`) — a trickling response never trips it. Since
   `_call_api` also downloads whole files, a single small number is unsafe. Needs operational
   evidence: largest file, observed latency, and the acceptable worst case for a 50-member loop
   that may now attempt each member twice.

## Acceptance criteria

- Every request carries an explicit, **positive and finite** connect and read timeout, asserted as
  an exact tuple on **both** `requests` call sites. *"Has a timeout" is not testable;
  `timeout=None` satisfies it.*
- A non-200 **link** response is surfaced as the exception type chosen in § Owner decisions, and
  **never written to disk** — assert the target file does not exist afterwards, not merely that an
  exception was raised.
- Direct-file `_call_api` calls (control, ERA5, snow) are covered separately from the ensemble
  link-list call — they are different payload sizes through the same method.
- Each newly-reachable class is tested at the **caller**: `ConnectTimeout`, `ReadTimeout`, the link
  error, and retry exhaustion — including that nothing reaches stdout/stderr carrying `api_key`
  (PREPG-015/PREPG-017).
- The today→yesterday fallback still fires on the real sentinel, and a **link** failure does
  **not** trigger it.
- **`uv.lock` is relocked** to the reviewed upstream commit, and the full suite passes on a frozen
  install. *Without this the fix ships nowhere.*

## Contract not to break

- `_call_api`'s existing `ValueError` on non-200 metadata is load-bearing: `Quantile_Mapping_OP.py`
  matches on its message text to drive the today→yesterday date fallback. Changing that message or
  exception type breaks a working daily path.
- **Retry ownership stays with PREPG-010, locally.** Do not add retry logic *to the client*. But
  see § The exception seam — whether `ReadTimeout` joins that local retry set is an owner decision
  this issue must resolve, not one it can ignore.
- **Status-only, deliberately.** This issue validates the link response's HTTP **status**. It does
  **not** validate a 200 body's schema, size or checksum, the link's origin, or the filename —
  the last of which is **PREPG-018**. Do not describe the result as "validated" without that
  qualifier.

## Note on verification — the earlier claim here was WRONG

The `preprocessing_gateway` suite does mock `sapphire_dg_client` entirely today
(`test_integration_preprocessing_gateway.py:75-80`, `test_transport_retry.py:35-40`), but the
conclusion drawn from that — "verification has to live in the client's own repo" — **does not
follow, and it hid a delivery gap.**

The real client is a declared, locked dependency **present in this module's venv**
(`pyproject.toml:30`, `uv.lock:438-440`). So this repo **can**:

- add an **isolated** contract test that imports `client_base` and monkeypatches its two
  `requests` calls, asserting the exact timeout tuple and the non-200 link behaviour. Isolated is
  load-bearing: other test modules poison `sys.modules` with `MagicMock` at collection.
- test the **caller** side of every newly-reachable exception, which it already does for transport
  faults (`test_transport_retry.py`).

**The delivery gap this exposed:** no acceptance criterion required a **relock**. Upstream could be
fixed, tested and merged while forecast-tools keeps shipping the pinned commit `bd9cc905…`
(`Dockerfile:25-29` runs `uv sync --frozen`), leaving the fix live nowhere. **A relock to the
reviewed upstream commit is part of this issue, not a follow-up.**
