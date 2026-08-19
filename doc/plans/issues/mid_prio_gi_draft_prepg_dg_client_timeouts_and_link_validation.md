## `sapphire-dg-client` has no request timeouts and never validates the link response (PREPG-014)

**Status**: Draft (2026-08-18) — split out of PREPG-010, which is local-only by design
**Module**: the **`sapphire-dg-client`** dependency (separate hydrosolutions repo), consumed by
`apps/preprocessing_gateway`
**Priority**: **Medium** — *raised from Low 2026-08-18.* No observed incident, which is what kept
it Low, but the reasoning was wrong on two counts:

1. **A hang is worse than a crash.** PREPG-010 makes the gateway survive a *reset*. A peer that
   hangs instead still blocks indefinitely — and unlike a crash it never reports, so cron cannot
   retry and the gateway target's ERA5 and snow scripts never run either.
2. **External lead time.** This lives in a separate repo and consumers only move on a relock, so
   it must start *earlier* than a local fix of equal value, not later.

The missing timeout is the half most likely to cause a real operational stall; the unvalidated
link response remains a latent correctness gap.
**Labels**: `preprocessing_gateway`, `dependency`, `network`, `upstream`
**Found**: 2026-08-18, while reviewing PREPG-010.
**Related**: **PREPG-010** — the local bounded retry. That fix stands alone and does **not** depend
on this one, but it explicitly does **not** cover hangs; this issue is where that gets fixed.
**PREPG-015** — the same client puts the API key in its exception message, which we then log.

---

## Why this is separate from PREPG-010

PREPG-010 retries a fault that already crosses the call boundary as a `ConnectionError`. Everything
in *this* issue is invisible from the calling code and can only be fixed inside the client:

- The two `requests` calls are internal (`client_base.py:43`, `:56`).
- A non-200 **link** response is never raised, only written to disk — the caller cannot catch what
  is never thrown.

Filing them together caused PREPG-010's acceptance criteria to include items its own phase could
not satisfy. Keep them apart.

## Two gaps

### 1. No timeouts anywhere

Only two `requests` calls exist in the whole client (`client_base.py:43` and `:56`); **neither
passes a `timeout`**. A hung peer blocks the process indefinitely rather than failing. This is the
gap most likely to cause a real operational stall.

### 2. The link response is written without validation

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

**Scoped honestly:** an earlier draft called this silent corruption. Not established. Downstream
parsing calls `pd.read_csv` and then assumes a two-column structure and parses dates, so a typical
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
- the **metadata** request is inside `_call_api` (`client_base.py:56`) — the timeout must be added
  here (or to its interface) as well.

*An earlier revision said to add timeouts "to `_call_api_and_save_file`". That cannot reach the
metadata request.*

## Acceptance criteria

- Every request carries an explicit timeout.
- A non-200 **link** response is surfaced as an error and never written to disk as a forecast
  file. (No retry behaviour is specified here — see the ownership note below.)
- Metadata-response and link-response handling are tested **separately** — they differ today, and
  that asymmetry is the bug.

## Contract not to break

- `_call_api`'s existing `ValueError` on non-200 metadata is load-bearing: `Quantile_Mapping_OP.py`
  matches on its message text to drive the today→yesterday date fallback. Changing that message or
  exception type breaks a working daily path.
- **Retry ownership is already settled: PREPG-010 owns it, locally.** Do not add retry logic here.
  This issue is timeouts and link-status validation only.

## Note on verification

The `preprocessing_gateway` test suite **mocks `sapphire_dg_client` entirely**, so it cannot prove
anything about this issue. Verification has to live in the client's own repo — which also means
this issue's status cannot be assessed from the forecast-tools repository alone.
