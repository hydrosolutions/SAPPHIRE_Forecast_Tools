## A transient transport fault kills `preprocessing_gateway`: the only handler catches `ValueError` (PREPG-010)

**Status**: Draft (2026-08-18) — **scope-cut after pre-implementation review**; ready to implement
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`)
**Priority**: **Medium** — one transient network fault fails the whole module, and because the
gateway target runs its three scripts with `break`-on-first-failure, ERA5 extension **and** snow
do not run either. The failure **is** reported (`run_locally.sh:506-507` logs ERROR/FAIL) — loud,
not silent.
**Labels**: `preprocessing_gateway`, `error-handling`, `network`
**Found**: 2026-08-14, reported by a colleague running `run_locally.sh` on a dev machine; code
confirmed on trunk 2026-08-18.
**Related**: PREPG-014 (client-side timeouts and link-status validation — split out of this issue,
requires the separate `sapphire-dg-client` repo). PREPG-009 (same module, opposite direction —
that one reports success on failure).

---

## What happened

```
Processing HRU Ensemble: <code>
  File "Quantile_Mapping_OP.py", line 811, in main
    files = client.ecmwf_ens.get_ensemble_forecast(
  File "sapphire_dg_client/client_base.py", line 43, in _call_api_and_save_file
    resp = requests.get(file_resp.get("link"))
requests.exceptions.ConnectionError: ('Connection aborted.',
    ConnectionResetError(54, 'Connection reset by peer'))
[ERROR] preprocessing_gateway failed (exit 1) after 2m 4s
```

One member's TLS connection was reset. The module died; ERA5 extension and snow never ran.

## Root cause

`Quantile_Mapping_OP.py:808-846` wraps the download in `try / except ValueError`. That handler
exists for a **different** condition — "today's data isn't published yet, fall back to yesterday" —
and a `requests.exceptions.ConnectionError` is not a `ValueError`, so it bypasses the block and
propagates out of `main()`.

## There are TWO download loops, not one

This is the detail an implementation is most likely to get wrong:

| Site | Date | Context |
|---|---|---|
| `Quantile_Mapping_OP.py:811` | `today` | the normal path |
| `Quantile_Mapping_OP.py:830` | `yesterday` | **inside** the `ValueError` fallback |

They are near-identical 50-iteration loops. **A fix applied to only the first one leaves the
defect live on the fallback path** — and the fallback is a normal daily occurrence, not an edge
case. Both must use the same retry.

## The fix

**One small bounded retry helper, applied at both call sites.** Nothing more.

- **A fixed number of attempts — three — hard-coded. No env var, no config surface, no CLI flag.**
- **Retry the individual member call**, not the enclosing 50-member loop. Re-running all 50 after
  a fault on member 37 is a different (and worse) behaviour.
- **Do not use `tenacity` or copy `sapphire_api_client`'s retry machinery.** That client does use
  Tenacity with exponential backoff (`sapphire_api_client/client.py:110`), but importing a
  framework for one call site is disproportionate. A small local loop is the right size.

### The `SSLError` decision

`requests.exceptions.SSLError` **subclasses** `ConnectionError` (`requests/exceptions.py:60`,
`:68`) — verified, not assumed. So a bare `except ConnectionError` also retries a permanent TLS
misconfiguration.

Pick one and pin it with a test:

1. **Exclude `SSLError`** explicitly, so a permanent TLS failure fails immediately; or
2. **Retry all `ConnectionError`** briefly, accepting three wasted attempts on a permanent fault.

Either is defensible. **Do not claim the class hierarchy cleanly separates transient from
permanent faults** — it does not.

### Retryable HTTP statuses are out of reach here

Do **not** add 429/502/503/504 handling. It cannot work from this layer: the client turns a
non-200 *metadata* response into an undifferentiated `ValueError` (`client_base.py:59`), and a
non-200 *link* response is not raised at all. Status-aware retry requires the client — see
PREPG-014.

## Coverage today is ZERO — build the harness first

Measured 2026-08-18. **Nothing exercises the code this issue changes**, so there is no safety net
and no characterization of current behaviour:

- **No test references `get_ensemble_forecast` or `ecmwf_ens`** anywhere in `test/`.
- `test_dg_download_failure_exits` does call `qm.main()`, but it makes the **control** download
  raise, so execution dies long before the ensemble block at `:808`.
- `test_qm_writes_both_P_and_T_csvs` runs `main()` all the way through successfully — but the
  `gateway_env` fixture sets `ieasyhydroforecast_HRU_ENSEMBLE: "None"`
  (`test_integration_preprocessing_gateway.py:318`), and the loop does
  `if ENSEMBLE_HRUS == "None": break` (`Quantile_Mapping_OP.py:806`) **before** the download. The
  ensemble path is never entered.
- **The today→yesterday date fallback has no test at all** — and it is the contract this fix most
  must not break.

**So the first task is not the retry.** It is a fixture that actually reaches the ensemble block:
a `gateway_env` variant with a real `HRU_ENSEMBLE` value and a mocked `client.ecmwf_ens`. The
pattern already exists (`patch("Quantile_Mapping_OP.sapphire_dg_client.client.SapphireDGClient")`
+ `qm.main()`), so this is extension, not invention.

**Pin current behaviour before changing it.** Two characterization tests, written against the
*unmodified* code and expected to pass immediately:

1. `today` raises the "Couldn't find any files…" `ValueError` → the `yesterday` loop runs and the
   HRU completes. *This is the regression guard.*
2. A `ConnectionError` on one member propagates out of `main()` today. This test **inverts** when
   the fix lands — which is the point, and makes the behaviour change explicit in the diff.

Only then add the retry. This is not extra scope: criteria below are unverifiable without it.

## Acceptance criteria

- A simulated `requests.exceptions.ConnectionError` (chained from `ConnectionResetError`, as
  Requests actually raises it — **not** a bare built-in) on one member is retried and the run
  continues.
- **The `yesterday` fallback path is covered too**: `today` raises the fallback `ValueError` →
  `yesterday` hits a `ConnectionError` → retry succeeds. *Without this case, wrapping only the
  first loop would pass.*
- **Call counts are asserted exactly**, proving only the failing member is retried and the other
  49 are not re-requested.
- After the attempt limit is exhausted, the module still fails **loudly** — this must not become a
  silent skip (cf. PREPG-009, the opposite defect in this module).
- The chosen `SSLError` policy is pinned by a test.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Contract not to break

- **The `yesterday` date-fallback must keep working.** It handles "data not published yet", a
  normal daily condition. Do not collapse it into the transport retry.
- **Do not widen the `except` to bare `Exception`.** The `ValueError` for "no files for this
  HRU/date" must stay distinguishable from a transport fault; conflating them re-creates
  PREPG-009's problem of an unexplained condition reported as routine.
- **Preserve fail-fast across HRUs.** Today an escaping exception ends the HRU loop
  (`Quantile_Mapping_OP.py:799`). Keep that. Switching to "continue" would change
  operator-visible partial output, since results are written per HRU (`:911`) — a separate
  decision, not this fix.
- The one-model-at-a-time loop is deliberate (batched requests time out server-side). Keep it.

## State on failure — no new cleanup needed

`_save_file` writes each response to disk immediately (`client_base.py:26`) with mode `wb`, so a
retry overwrites the same path harmlessly. Files from earlier members persist, but the in-memory
`files_downloaded` list is lost, so no merge or final ensemble CSV is produced for that HRU.
**No code resumes from partial files**, and the next run *attempts* to delete everything under
`OUTPUT_PATH_DG` before downloading (`Quantile_Mapping_OP.py:625`) — deletion failures are caught
and suppressed (`:637`), so it is an attempt, not a guarantee.
