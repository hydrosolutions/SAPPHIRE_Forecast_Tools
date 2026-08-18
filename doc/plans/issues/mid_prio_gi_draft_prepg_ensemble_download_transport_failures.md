## ECMWF ensemble download: a transient transport fault aborts `preprocessing_gateway` because the only handler catches `ValueError` (PREPG-010)

**Status**: Draft (2026-08-18)
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`), plus the
`sapphire-dg-client` dependency
**Priority**: **Medium** — a single transient network fault fails the whole module, and because
the gateway target runs its three scripts with `break`-on-first-failure, ERA5 extension **and**
snow do not run either. The failure **is** reported (`run_locally.sh:501` logs ERROR/FAIL) — it
is loud, not silent.

> **Scope note.** This issue is now **narrowly about transport-fault handling**. Two
> data-correctness defects found on the same code path during review are filed separately as
> **PREPG-011** (cross-member forward-fill) and **PREPG-012** (wrong HRU quantile-mapping
> parameters). Those are more serious than this one.
**Labels**: `preprocessing_gateway`, `error-handling`, `network`, `dependency`
**Found**: 2026-08-14, reported by a colleague running `run_locally.sh daily` on a dev machine;
code confirmed on trunk 2026-08-18.
**Related**: PREPG-009 (same module, opposite direction — that one reports success on failure;
this one fails the whole run on a recoverable fault).

---

## What happened

```
Processing HRU Ensemble: <code>
Traceback (most recent call last):
  …
  File "Quantile_Mapping_OP.py", line 811, in main
    files = client.ecmwf_ens.get_ensemble_forecast(
  File "sapphire_dg_client/client_base.py", line 43, in _call_api_and_save_file
    resp = requests.get(file_resp.get("link"))
  …
requests.exceptions.ConnectionError: ('Connection aborted.',
    ConnectionResetError(54, 'Connection reset by peer'))
[ERROR] preprocessing_gateway failed (exit 1) after 2m 4s
```

The TLS connection for one ensemble-member file download was reset by the peer. The module
died. ERA5 reanalysis extension and snow never ran.

## Root cause — the error handling covers the *data* failure mode, not the *transport* one

`Quantile_Mapping_OP.py:808-846`:

```python
try:
    files_downloaded = []
    for model in range(1, 51):                       # 50 client calls; each = 1 metadata req + N body reqs
        files = client.ecmwf_ens.get_ensemble_forecast(
            hru_code=code_ens, date=today, models=[str(model)], directory=OUTPUT_PATH_DG)
        files_downloaded.append(files)
except ValueError as e:                              # <-- ValueError ONLY
    if "Couldn't find any files for the given HRU code, date and models!" in str(e):
        …retry the same 50 calls with `yesterday`…
```

Four compounding problems:

1. **The handler catches `ValueError` only.** A `requests.exceptions.ConnectionError` is not a
   `ValueError`, so it bypasses this block entirely and propagates out of `main()`.
2. **The existing retry is a date fallback, not a transport retry.** It exists for "today's data
   isn't published yet, try yesterday" — a legitimate and different concern. It was never meant
   to cover network faults, and doesn't.
3. **The loop makes 50 sequential client calls per ensemble HRU — and more HTTP requests than
   that.** Each `get_ensemble_forecast` performs one metadata request (`client_base.py:38`) plus
   one body request per returned link (`client_base.py:42`), so the true exposure is
   `50 metadata requests + sum(returned links)` per HRU, plus the fallback's calls if it is entered (the first pass
   can fail after any member, so the total is not simply doubled). *The original draft said "50 sequential HTTP downloads", which
   understated it.* The inline comment explains the split — the batched form "may cause timeout
   errors from gateway server side" — but it multiplied exposure to per-call transport faults
   **without adding per-call resilience**. A fault also aborts the enclosing HRU loop
   (`:799`): earlier HRUs have **completed**, the **currently failing** HRU is left with partial
   raw downloads, and later HRUs are never attempted.
4. **A failure at model N loses the accumulated in-memory path list, not the files.**
   *Corrected 2026-08-18 after out-of-loop review — the original wording ("discards up to 49
   downloaded files") was wrong.* `_save_file` writes each response to disk immediately
   (`client_base.py:26`), so files from members `1..N-1` remain. What is lost is the
   `files_downloaded` list, so no merge or final ensemble CSV is produced for that HRU. **No
   code resumes from the partial files**, and the next normal run deletes everything under
   `OUTPUT_PATH_DG` before downloading (`Quantile_Mapping_OP.py:625`, skipped only at DEBUG
   level), then re-requests every member. There is also no sound "49 files" bound — each member
   call may return multiple links, so models are not files.

### The client itself has no timeouts, retries, or status checks

`sapphire_dg_client/client_base.py`:

```python
def _call_api_and_save_file(self, endpoint: str, directory: str):
    resp = self._call_api(method="GET", endpoint=endpoint)
    for file_resp in resp.json():
        resp = requests.get(file_resp.get("link"))          # no timeout, no retry, no status check
        local_file_path = self._save_file(resp, directory, file_resp["filename"])
```

Compare its sibling `_call_api`, which **does** validate:

```python
    if response.status_code != 200:
        raise ValueError(f"Failed to get data from {endpoint}: {response.text}")
```

**A non-200 body is written to disk before any validation** — `_save_file` writes
`response.content` with no content-type, size, checksum or schema check (`client_base.py:26`).
Note the asymmetry: the *metadata* request is validated (`client_base.py:59`), the *link* request
is not.

*Corrected 2026-08-18:* the original draft called this "the more dangerous half" and asserted
silent corruption. **That is not established.** Downstream parsing calls `pd.read_csv`
(`Quantile_Mapping_OP.py:204`) and then assumes a non-empty two-column structure (`:147`) and
parses dates (`:153`), so a typical HTML/JSON/plain-text error body would most likely raise
loudly. The accurate statement is narrower:

> Non-200 link bodies are written as raw forecast-named files before validation. Typical error
> bodies probably fail during parsing, but **no explicit status, content, size or schema check
> guarantees rejection**, so a sufficiently CSV-shaped bad body could pass.

The missing **timeout** is arguably the more practically dangerous gap: with none set, a hung
peer blocks the process indefinitely rather than failing.

Only two `requests` calls exist in the whole client (`client_base.py:43` and `:56`); neither
passes a `timeout`, so a hung peer blocks indefinitely rather than failing.

## Where to fix it

`sapphire-dg-client` is a **hydrosolutions-owned git dependency**, so upstream is possible:

```toml
"sapphire-dg-client @ git+https://github.com/hydrosolutions/sapphire-dg-client.git@main"
```

| Option | Pros | Cons |
|---|---|---|
| **(a) Local, in `Quantile_Mapping_OP.py`** — retry the individual member on *transient* faults only (see breadth note below), with backoff | No cross-repo change; fixes the observed failure; can resume mid-loop | Leaves the client unguarded for other callers |
| **(b) Upstream in `sapphire_dg_client`** — add `timeout`, bounded retry, and a status check to `_call_api_and_save_file` | Fixes it for every caller; the status check is the real prize | Separate repo and release; consumers move when they **relock**, not when `@main` moves |

**Recommend (a) now and (b) as a follow-up** — but note an inconsistency the review caught:
**local-only work cannot satisfy every acceptance criterion below.** Request timeouts, a status
check before `_save_file`, and any guarantee that a non-200 body is never written all live
*inside the client*. So either (b) lands too, or the acceptance criteria must be split by phase.
They are split accordingly below.

**Retry breadth — do not catch all of `RequestException`.** Retry only genuinely transient
transport faults and retryable statuses. Two traps:

- **`requests.exceptions.SSLError` subclasses `ConnectionError`**, so retrying `ConnectionError`
  also retries a permanent TLS misconfiguration. Exclude `SSLError` explicitly, or the
  "fail immediately on permanent TLS failure" intent is not implemented.
- The sibling `sapphire_api_client` treats **429** as retryable alongside 502/503/504. Either
  include it deliberately or state why this API differs.

**Reuse an existing pattern rather than inventing one.** There is no shared retry utility in
`preprocessing_gateway`, but the installed `sapphire_api_client` implements exactly this shape —
bounded exponential backoff for `ConnectionError`/`Timeout`, an explicit timeout, and retryable
status handling. Follow it. Note that `requests` and `tenacity` are currently **transitive**
dependencies of `preprocessing_gateway`, not declared ones (`pyproject.toml:18`); if local code
imports either, declare it.

**Not the only retry in the system.** The Luigi/Docker path already retries the whole container
(`pipeline/pipeline_docker.py:359`, used at `:550`). That does not help the reported
`run_locally.sh` invocation and is far too coarse — it re-runs every HRU and every member — but
it means "no transport-failure handling" would be too broad a claim.

## Withdrawn — the `@main` reproducibility concern

*An earlier revision claimed that `@main` means "any upstream commit lands on the next
`uv sync` with no lockfile-visible intent". **That is wrong** and has been removed.*
`uv.lock` resolves the dependency to a specific commit (`apps/preprocessing_gateway/uv.lock`),
the installed metadata records the same commit, and the Docker build runs
`uv sync --frozen --no-dev`. **The Python dependency set is reproducible.** (Not the whole
image — the base image tag is mutable, which is a separate concern and not this issue's.)

What remains is an *upgrade-policy* question only: a deliberate relock advances `main`. Worth a
sentence in a dependency-policy discussion, not an issue.

## Acceptance criteria

**Phase (a) — local, achievable without the client:**
- A simulated `requests.exceptions.ConnectionError` (chained from `ConnectionResetError`, as
  Requests actually raises it — **not** a bare built-in) on one ensemble member does **not** fail the module; the
  member is retried with bounded backoff and the run continues.
- After a bounded number of retries, a genuinely unreachable gateway still fails **loudly** —
  this must not become a silent skip (cf. PREPG-009, the opposite defect in the same module).
- A failure in one ensemble HRU does not silently abandon the remaining HRUs.

**Phase (b) — requires the client change** (local code *cannot* satisfy these: a link-level 404
is never raised, only written, because only the metadata request's status is checked):
- Every request carries an explicit timeout.
- A non-200 **link** response is never written to disk as a forecast file.
- A non-retryable status fails immediately without burning retries.
- Metadata-response and link-response handling are tested **separately**.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Contract not to break

- **The `yesterday` date-fallback must keep working.** It handles "data not published yet",
  which is a normal daily condition — do not collapse it into the transport retry.
- **Do not widen the `except` to bare `Exception`.** A genuine `ValueError` for "no files for
  this HRU/date" must remain distinguishable from a transport fault; conflating them would
  re-create PREPG-009's problem of an unexplained condition reported as routine.
- The one-model-at-a-time loop is deliberate (batched requests time out server-side). Keep it.
