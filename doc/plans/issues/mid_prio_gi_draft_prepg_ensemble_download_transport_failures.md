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

`Quantile_Mapping_OP.py:808-856` wraps the download in `try / except ValueError`. That handler
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
### The `SSLError` decision — DECIDED 2026-08-20: exclude it

`requests.exceptions.SSLError` **subclasses** `ConnectionError` (`requests/exceptions.py:60`,
`:68`) — verified, not assumed. So a bare `except ConnectionError` also retries a permanent TLS
misconfiguration.

**Owner decision (2026-08-20): exclude `SSLError`.** A permanent TLS failure must fail
immediately and loudly rather than burn three attempts on a fault that cannot succeed — and,
per the hang section below, every avoided attempt is one fewer opportunity to meet a hung peer.
Implement as an explicit re-raise, not a narrower catch:

```python
except requests.exceptions.SSLError:
    raise
except requests.exceptions.ConnectionError:
    ...  # retry
```

**Do not claim the class hierarchy cleanly separates transient from permanent faults** — it does
not, and this decision does not assert otherwise. A connection reset *during* a TLS handshake can
surface as either class depending on where it lands; excluding `SSLError` therefore accepts that
some genuinely transient faults will not be retried. That is the accepted trade, not an oversight.
The observed failure (`ConnectionResetError(54)`) reached us as a plain `ConnectionError`, so it
is retried under this policy.

## Coverage today is ZERO, and the harness is NOT a small fixture tweak

Measured 2026-08-18. **Nothing exercises the code this issue changes:**

- **No test references `get_ensemble_forecast` or `ecmwf_ens`.**
- `test_dg_download_failure_exits` calls `qm.main()`, but makes the **control** download raise, so
  it dies long before `:808`.
- `test_qm_writes_both_P_and_T_csvs` runs `main()` successfully — but `gateway_env` sets
  `ieasyhydroforecast_HRU_ENSEMBLE: "None"` (`test_integration_preprocessing_gateway.py:318`) and
  the loop breaks on that value (`Quantile_Mapping_OP.py:806`) **before** the download.
- **The today→yesterday fallback has no test at all** — the contract this fix most must not break.

### Budget the harness properly

*An earlier revision called this "extension, not invention". That was wrong.* Reaching `:808`
requires **all** of the following to succeed first:

- `load_environment()` neutralised; API key present.
- Every path env var non-`None`; CM/ENS/DG directories writable.
- Models/QM path env vars present. **If the QM directory exists, both control-member parameter
  CSVs must be valid** — the current fixture sidesteps this by leaving it absent.
- Both HRU env vars present, ensemble **not** `"None"`.
- Mapping path env vars present; any existing mapping file must be valid JSON containing
  `gateway_name_twins`.
- The control-member download must return a truthy, existing, valid DG-format CSV, and its
  transform and CM output writes must complete.

### The harder part: characterization test (a) needs real files

A plain `MagicMock` return **will not work**. `merge_ensemble_forecast` exits the process when the
list is empty (`Quantile_Mapping_OP.py:189-191`, `sys.exit(1)`), and a default mock flattens to
`[]`. So the fallback test needs **all 50 `yesterday` calls to return iterable lists of files that
actually exist on disk**, created *after* the `:625` cleanup, with parseable
`..._EMnnn_HRU..._{tp,2t}.csv` names and contents.

An ensemble-file factory already exists but is **module-local to `test/test_ensemble_transforms.py`**,
so it must be shared or rebuilt.

**Characterization test (b) is cheap by contrast** — a `ConnectionError` propagates directly out of
`main()`, so it needs no file scaffolding, and it inverts when the fix lands.

**Implication for sequencing:** (b) and the retry can land first and cheaply. (a) — the date-fallback
regression guard — is the expensive part, and it is the one that protects a normal daily path. Do
not let its cost silently drop it.

## This fix creates a small NEW hang exposure — know it before implementing

`requests` has **no default timeout**, and the client sets none (`client_base.py:43`, `:56`), so a
peer that accepts a connection and never responds blocks forever. That is **PREPG-014**, not this
issue — but the retry interacts with it, and not neutrally:

| Scenario | Today | After this fix |
|---|---|---|
| Attempt 1 **hangs** | blocks forever | blocks forever — **unchanged**; the retry never fires because the call never returns |
| Attempt 1 **resets** | module dies in seconds — loud, and cron can retry | attempt 2 runs, and **attempt 2 can hang** |

So the fix does not make a hang worse; it creates **up to 3× the opportunities to meet one**, on a
path that previously terminated immediately. The bad trade it can produce is converting a fast,
loud, cron-retryable failure into an indefinite silent block.

**This is not a reason to skip the fix** — a reset is the observed failure and hangs are
unobserved — but two things follow:

1. **Keep the attempt count genuinely small (3).** Every extra attempt is another chance to hang.
   This is a second, independent reason not to make it configurable: an operator raising it to 10
   would multiply the exposure without seeing the connection.
2. **PREPG-014 stops being merely "the other half" and becomes this fix's safety net.** Land it, or
   accept the exposure knowingly.

**A local option, if the exposure is unacceptable before PREPG-014 lands:** `socket.setdefaulttimeout(N)`
at process start bounds *any* blocking socket operation without touching the client. It is a blunt
instrument — process-global, affects the SAPPHIRE API client too — but it is per-socket-operation,
not per-request, so a large ensemble download that streams steadily is unaffected. **Owner
decision; do not add it unprompted.**

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
- **A `requests.exceptions.SSLError` is NOT retried** — it propagates on the first
  attempt, pinned by a test asserting exactly one call.
- `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Contract not to break

- **The `yesterday` date-fallback must keep working.** It handles "data not published yet", a
  normal daily condition. Do not collapse it into the transport retry.
- **Do not widen the `except` to bare `Exception`.** The `ValueError` for "no files for this
  HRU/date" must stay distinguishable from a transport fault; conflating them re-creates
  PREPG-009's problem of an unexplained condition reported as routine.
- **Preserve fail-fast across HRUs.** Today an escaping exception ends the HRU loop
  (`Quantile_Mapping_OP.py:799`). Keep that. Switching to "continue" would change
  operator-visible partial output, since results are written per HRU (`:912-917`) — a separate
  decision, not this fix.
- The one-model-at-a-time loop is deliberate (batched requests time out server-side). Keep it.
- **A small local loop, not `tenacity`** — a retry framework for one call site is disproportionate.
- **No HTTP-status retry (429/502/503/504).** It cannot work from here: the client turns a non-200
  *metadata* response into an undifferentiated `ValueError` and never raises on a bad *link*. That
  is PREPG-014's territory.

## State on failure — no new cleanup needed

`_save_file` writes each response to disk immediately (`client_base.py:26`) with mode `wb`, so a
retry overwrites the same path harmlessly. Files from earlier members persist, but the in-memory
`files_downloaded` list is lost, so no merge or final ensemble CSV is produced for that HRU.
**No code resumes from partial files**, and the next run *attempts* to delete everything under
`OUTPUT_PATH_DG` before downloading (`Quantile_Mapping_OP.py:625`) — deletion failures are caught
and suppressed (`:637`), so it is an attempt, not a guarantee.
