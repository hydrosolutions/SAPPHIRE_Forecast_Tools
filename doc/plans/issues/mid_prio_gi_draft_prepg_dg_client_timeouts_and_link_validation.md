## `sapphire-dg-client` has no request timeouts and never validates the link response (PREPG-014)

**Status**: **Ready** (2026-08-20) — split out of PREPG-010, which is local-only by design. The
three owner decisions are RESOLVED (§ Decisions) and the time-to-first-byte measurement they were
waiting on is **done** (§ Decisions #3). Implementation is upstream in `sapphire-dg-client`; the
local half (adding `Timeout` to the retry set) and the relock are in this repo.
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

## Decisions — RESOLVED 2026-08-20 by the owner

Recorded with their reasoning, because each rules out an alternative an implementer would otherwise
reach for.

1. **`ReadTimeout` IS retried. Add `requests.exceptions.Timeout` to the retryable tuple** in the
   shipped `_call_with_transport_retry` — that single class covers both `ReadTimeout` and
   `ConnectTimeout`, rather than naming subclasses. **Reasoning:** a read timeout is exactly the
   transient fault the retry exists for, and without it PREPG-014 would convert a hang into an
   *uncaught* exception on PREPG-017's key-bearing stderr path — trading one bad outcome for
   another. This edits merged code, so it needs its own test asserting a `ReadTimeout` produces
   exactly 2 calls.
   **Consequence to state in the PR:** retrying a timeout doubles the worst-case wait per member to
   `2 x (connect + read)`.
2. **A failed link raises a dedicated `ValueError` SUBCLASS.** Not `raise_for_status()`.
   **Reasoning:** `HTTPError` is neither `ConnectionError` nor `ValueError`, so it bypasses both
   ensemble handlers *and* the retry and propagates uncaught — the idiomatic choice is the worse
   one here. A `ValueError` subclass keeps the existing handlers working, so the failure exits
   loudly through the path that already exists.
   **Hard requirement:** its message must be guaranteed **not** to contain
   `"Couldn't find any files for the given HRU code, date and models!"`. A link failure is not
   "no data for this date", and if its text ever matched that sentinel it would silently trigger
   the today→yesterday fallback and download the wrong day's forecast. **Pin that with a test.**
   The control path catches `Exception` broadly (`Quantile_Mapping_OP.py:792-821`) and is
   unaffected either way.
3. **`timeout=(10, 60)` — connect 10s, read 60s — with ONE measurement required before merge.**

   **Evidence for 60s:** 121 observed `run_locally` gateway runs give whole-module
   min 92s / median 329s / max 584s. That total covers 50 ensemble members per HRU *plus* control,
   ERA5, snow and all processing — so a **single request** idling 60s is already far outside
   anything normal. It cannot plausibly break a download that completes today.

   **MEASURED 2026-08-20 against the live gateway** — this was the one number that could have
   broken a working download, and it does not:

   | request (the calls the pipeline actually makes) | TTFB | payload |
   |---|---|---|
   | control member, `start_date = today − 365d` | **4.92s** | 313 KB |
   | ensemble link-list metadata | 0.54s | 1 KB |
   | ensemble link download | 0.08–0.17s | ~325 B |

   So `read=60s` carries roughly **12x headroom** over the slowest observed first byte, and the
   "large downloads might idle" concern is unfounded at these payload sizes.

   **A trap for whoever re-measures:** the control member is a **spin-up** request —
   `Quantile_Mapping_OP.py:754` sets `start_date = today − 365 days`. Probing that endpoint with
   *today's* date returns HTTP 500 and looks exactly like a gateway outage. It is not; it is a
   meaningless request. **Reproduce the caller's real arguments.**

   **A per-request timeout does NOT bound the job.** Worst case is now
   50 members x 2 attempts x 70s = **~1.9 hours per HRU** of pure waiting. That is finite, unlike
   today, but it is long enough to overlap the next cron run. **The implementer must confirm the
   deployed cron spacing tolerates it, or an overall job deadline is needed — a Requests timeout
   cannot provide one.**

## Acceptance criteria

- Both `requests` call sites pass **`timeout=(10, 60)`** exactly — assert the tuple, not merely
  that some timeout is present. *"Has a timeout" is not testable; `timeout=None` satisfies it.*
- **`requests.exceptions.Timeout` is in `_call_with_transport_retry`'s retryable tuple**, pinned by
  a test asserting a `ReadTimeout` produces exactly **2** calls — the same shape as the existing
  `SSLError` test, so a later "tightening" that drops it fails visibly.
- A non-200 **link** response raises the dedicated `ValueError` subclass and is **never written to
  disk** — assert the target file does not exist afterwards, not merely that an exception was
  raised.
- **The link error's message does not contain the fallback sentinel.** Assert it directly, and
  assert at `main()` level that a link failure does **not** trigger the today→yesterday path. This
  is the criterion that stops a link failure silently fetching the wrong day's forecast.
- Direct-file `_call_api` calls (control, ERA5, snow) are covered separately from the ensemble
  link-list call — they are different payload sizes through the same method.
- Each newly-reachable class is tested at the **caller**: `ConnectTimeout`, `ReadTimeout`, the link
  error, and retry exhaustion — including that nothing reaches stdout/stderr carrying `api_key`
  (PREPG-015/PREPG-017).
- The today→yesterday fallback still fires on the real sentinel.
- **Recorded before merge, not after:** the measured Data Gateway time-to-first-byte, and a
  statement that the deployed cron spacing tolerates the ~1.9h worst case (or the overall deadline
  that replaces it). See § Decisions #3.
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
